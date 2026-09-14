use bytes::Bytes;
use data_plane_gateway::client::{connect_edge, ConnectClientConfig, EdgeTlsConfig};
use data_plane_gateway::common::protocol::ConnectTarget;
use data_plane_gateway::common::resource::raise_nofile_soft_limit_from_env;
use data_plane_gateway::edge::connector::H2ConnectStream;
use data_plane_gateway::edge::{AccessKind, DataPlaneL4Connector, H2PoolConfig};
use http::{header, Method, Request, Uri};
use http_body_util::{BodyExt, Empty, Full};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use std::env;
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Barrier, Semaphore};
use tokio::task::JoinSet;
use tokio_rustls::TlsConnector;

const BUFFER_SIZE: usize = 64 * 1024;

trait BenchIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> BenchIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}
type BoxIo = Box<dyn BenchIo>;

#[derive(Clone)]
enum Path {
    Direct {
        target: String,
    },
    Node {
        connector: Box<DataPlaneL4Connector>,
        node: String,
        target: ConnectTarget,
    },
    Edge {
        edge: String,
        instance: String,
        target_port: u16,
        tls: Option<EdgeTlsConfig>,
    },
}

#[derive(Clone, Copy)]
enum Direction {
    Upload,
    Download,
}

struct NodeStream {
    inner: H2ConnectStream,
    _cancel: watch::Sender<bool>,
}

impl AsyncRead for NodeStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for NodeStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    data_plane_gateway::common::install_crypto_provider();
    raise_nofile_soft_limit_from_env()?;
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("server") => {
            let bind = required(&mut args, "server bind address")?;
            ensure_done(&mut args)?;
            run_server(&bind).await?;
        }
        Some("http-server") => {
            let bind = required(&mut args, "HTTP server bind address")?;
            ensure_done(&mut args)?;
            run_http_server(&bind).await?;
        }
        Some("bench-direct") => {
            let target = required(&mut args, "target address")?;
            let options = BenchOptions::parse(&mut args)?;
            run_benchmark(Path::Direct { target }, options).await?;
        }
        Some("bench-node") => {
            let node = required(&mut args, "Node Proxy address")?;
            let target_ip = required(&mut args, "target IP")?.parse()?;
            let target_port = required(&mut args, "target port")?.parse()?;
            let options = BenchOptions::parse(&mut args)?;
            run_benchmark(
                Path::Node {
                    connector: Box::new(DataPlaneL4Connector::new(H2PoolConfig::default())),
                    node,
                    target: ConnectTarget {
                        instance_id: "perf-instance".into(),
                        workload_id: "perf-workload".into(),
                        target_ip,
                        target_port,
                        request_id: "perf-request".into(),
                    },
                },
                options,
            )
            .await?;
        }
        Some("bench-edge") => {
            let edge = required(&mut args, "Edge Frontend address")?;
            let instance = required(&mut args, "instance ID")?;
            let target_port = required(&mut args, "target port")?.parse()?;
            let options = BenchOptions::parse(&mut args)?;
            run_benchmark(
                Path::Edge {
                    edge,
                    instance,
                    target_port,
                    tls: None,
                },
                options,
            )
            .await?;
        }
        Some("bench-edge-tls") => {
            let edge = required(&mut args, "Edge Frontend address")?;
            let instance = required(&mut args, "instance ID")?;
            let target_port = required(&mut args, "target port")?.parse()?;
            let ca_path = required(&mut args, "CA certificate path")?;
            let server_name = required(&mut args, "TLS server name")?;
            let options = BenchOptions::parse(&mut args)?;
            run_benchmark(
                Path::Edge {
                    edge,
                    instance,
                    target_port,
                    tls: Some(load_tls(&ca_path, server_name)?),
                },
                options,
            )
            .await?;
        }
        Some("bench-http-tls") => {
            let edge = required(&mut args, "Edge address")?;
            let ca_path = required(&mut args, "CA certificate path")?;
            let server_name = required(&mut args, "TLS server name")?;
            let token = required(&mut args, "bearer token")?;
            let path_template = required(&mut args, "path template")?;
            let targets = required(&mut args, "logical target count")?.parse()?;
            let requests = required(&mut args, "request count")?.parse()?;
            let concurrency = required(&mut args, "concurrency")?.parse()?;
            let warmup = required(&mut args, "warmup requests per connection")?.parse()?;
            ensure_done(&mut args)?;
            run_http_benchmark(
                edge,
                load_tls(&ca_path, server_name)?,
                token,
                path_template,
                targets,
                requests,
                concurrency,
                warmup,
            )
            .await?;
        }
        _ => return Err(usage().into()),
    }
    Ok(())
}

struct BenchOptions {
    direction: Direction,
    bytes: u64,
    iterations: usize,
    concurrency: usize,
}

impl BenchOptions {
    fn parse(args: &mut impl Iterator<Item = String>) -> Result<Self, Box<dyn std::error::Error>> {
        let direction = match required(args, "upload or download")?.as_str() {
            "upload" => Direction::Upload,
            "download" => Direction::Download,
            _ => return Err(usage().into()),
        };
        let bytes = required(args, "bytes per stream")?.parse()?;
        let iterations = required(args, "iterations")?.parse::<usize>()?;
        let concurrency = required(args, "concurrency")?.parse::<usize>()?;
        ensure_done(args)?;
        if iterations == 0 || concurrency == 0 {
            return Err("iterations and concurrency must be non-zero".into());
        }
        Ok(Self {
            direction,
            bytes,
            iterations,
            concurrency,
        })
    }
}

async fn run_server(bind: &str) -> io::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    println!("READY {}", listener.local_addr()?);
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let _ = handle_server_stream(stream).await;
        });
    }
}

async fn run_http_server(bind: &str) -> io::Result<()> {
    let listener = TcpListener::bind(bind).await?;
    let bodies = Arc::new([
        (0usize, Bytes::new()),
        (128, Bytes::from(vec![b'x'; 128])),
        (4096, Bytes::from(vec![b'x'; 4096])),
        (65_536, Bytes::from(vec![b'x'; 65_536])),
    ]);
    println!("READY {}", listener.local_addr()?);
    loop {
        let (stream, _) = listener.accept().await?;
        let bodies = bodies.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request: http::Request<hyper::body::Incoming>| {
                let bodies = bodies.clone();
                async move {
                    let requested = request
                        .uri()
                        .path()
                        .rsplit('/')
                        .next()
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(128);
                    let body = bodies
                        .iter()
                        .find(|(size, _)| *size == requested)
                        .map(|(_, body)| body.clone())
                        .unwrap_or_else(|| Bytes::from_static(b"unsupported body size"));
                    Ok::<_, std::convert::Infallible>(http::Response::new(Full::new(body)))
                }
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .keep_alive(true)
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_http_benchmark(
    edge: String,
    tls: EdgeTlsConfig,
    token: String,
    path_template: String,
    targets: usize,
    requests: usize,
    concurrency: usize,
    warmup: usize,
) -> io::Result<()> {
    if targets == 0 || requests == 0 || concurrency == 0 || requests < concurrency {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "targets/concurrency must be non-zero and requests must be at least concurrency",
        ));
    }
    if !path_template.contains("{target}") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path template must contain {target}",
        ));
    }

    let barrier = Arc::new(Barrier::new(concurrency + 1));
    // Admission bursts are a separate concern from steady-state request QPS.
    // Ramp persistent TLS connections in bounded batches, then release every
    // worker through one barrier so the measured interval still starts at the
    // requested concurrency.
    let warmup_admission = Arc::new(Semaphore::new(concurrency.min(64)));
    let mut tasks = JoinSet::new();
    let base = requests / concurrency;
    let remainder = requests % concurrency;
    for worker_index in 0..concurrency {
        let edge = edge.clone();
        let tls = tls.clone();
        let token = token.clone();
        let path_template = path_template.clone();
        let barrier = barrier.clone();
        let warmup_admission = warmup_admission.clone();
        let count = base + usize::from(worker_index < remainder);
        tasks.spawn(async move {
            let warmup_permit = warmup_admission.acquire_owned().await.unwrap();
            let (mut sender, connection) = open_http_connection(&edge, &tls).await?;
            tokio::spawn(async move {
                if let Err(error) = connection.await {
                    tracing::debug!(%error, "HTTP load-generator connection closed");
                }
            });
            for _ in 0..warmup {
                send_http_request(&mut sender, &token, &path_template, worker_index % targets)
                    .await?;
            }
            drop(warmup_permit);
            barrier.wait().await;

            let mut samples = Vec::with_capacity(count);
            let mut bytes_received = 0u64;
            for request_index in 0..count {
                // When concurrency is an integer multiple of targets this
                // pins every connection to one sandbox, so total concurrency
                // is exactly targets * per-sandbox concurrency.
                let target = (worker_index + request_index * concurrency) % targets;
                let started = Instant::now();
                bytes_received +=
                    send_http_request(&mut sender, &token, &path_template, target).await? as u64;
                samples.push(started.elapsed().as_secs_f64() * 1000.0);
            }
            Ok::<_, io::Error>((samples, bytes_received))
        });
    }

    tokio::time::timeout(std::time::Duration::from_secs(180), barrier.wait())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "HTTP workers did not warm up"))?;
    let usage_before = process_usage();
    let overall = Instant::now();
    let mut samples = Vec::with_capacity(requests);
    let mut bytes_received = 0u64;
    while let Some(result) = tasks.join_next().await {
        let (mut task_samples, task_bytes) = result.map_err(io::Error::other)??;
        samples.append(&mut task_samples);
        bytes_received += task_bytes;
    }
    let elapsed = overall.elapsed().as_secs_f64();
    let usage_after = process_usage();
    samples.sort_by(f64::total_cmp);
    let cpu_seconds = (usage_after.cpu_seconds - usage_before.cpu_seconds).max(0.0);
    let per_sandbox_concurrency = concurrency as f64 / targets as f64;
    println!(
        "{}",
        serde_json::json!({
            "path": "edge-http-tls",
            "requests": samples.len(),
            "concurrency": concurrency,
            "connection_count": concurrency,
            "logical_targets": targets,
            "per_sandbox_concurrency": per_sandbox_concurrency,
            "errors": 0,
            "elapsed_ms": elapsed * 1000.0,
            "requests_per_second": samples.len() as f64 / elapsed,
            "bytes_received": bytes_received,
            "throughput_mib_s": bytes_received as f64 / 1024.0 / 1024.0 / elapsed,
            "p50_ms": percentile(&samples, 0.50),
            "p95_ms": percentile(&samples, 0.95),
            "p99_ms": percentile(&samples, 0.99),
            "generator_cpu_seconds": cpu_seconds,
            "generator_cpu_cores_avg": cpu_seconds / elapsed,
            "generator_rss_peak_kib": usage_after.max_rss_kib,
        })
    );
    Ok(())
}

type HttpSender = hyper::client::conn::http1::SendRequest<Empty<Bytes>>;
type HttpConnection = hyper::client::conn::http1::Connection<
    TokioIo<tokio_rustls::client::TlsStream<TcpStream>>,
    Empty<Bytes>,
>;

async fn open_http_connection(
    edge: &str,
    tls: &EdgeTlsConfig,
) -> io::Result<(HttpSender, HttpConnection)> {
    let tcp = TcpStream::connect(edge).await?;
    tcp.set_nodelay(true)?;
    let server_name = ServerName::try_from(tls.server_name.clone())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let stream = TlsConnector::from(tls.client.clone())
        .connect(server_name, tcp)
        .await
        .map_err(io::Error::other)?;
    hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .map_err(io::Error::other)
}

async fn send_http_request(
    sender: &mut HttpSender,
    token: &str,
    path_template: &str,
    target: usize,
) -> io::Result<usize> {
    let path = path_template.replace("{target}", &format!("{target:04}"));
    let uri = path
        .parse::<Uri>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let request = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Empty::<Bytes>::new())
        .map_err(io::Error::other)?;
    let response = sender
        .send_request(request)
        .await
        .map_err(io::Error::other)?;
    if response.status() != http::StatusCode::OK {
        return Err(io::Error::other(format!(
            "unexpected HTTP status {}",
            response.status()
        )));
    }
    response
        .into_body()
        .collect()
        .await
        .map(|body| body.to_bytes().len())
        .map_err(io::Error::other)
}

struct ProcessUsage {
    cpu_seconds: f64,
    max_rss_kib: i64,
}

fn process_usage() -> ProcessUsage {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage initializes the provided rusage structure and does not
    // retain its address.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return ProcessUsage {
            cpu_seconds: 0.0,
            max_rss_kib: 0,
        };
    }
    // SAFETY: a successful getrusage call initialized the structure.
    let usage = unsafe { usage.assume_init() };
    let seconds = |time: libc::timeval| time.tv_sec as f64 + time.tv_usec as f64 / 1_000_000.0;
    let max_rss_kib = if cfg!(target_os = "macos") {
        usage.ru_maxrss / 1024
    } else {
        usage.ru_maxrss
    };
    ProcessUsage {
        cpu_seconds: seconds(usage.ru_utime) + seconds(usage.ru_stime),
        max_rss_kib,
    }
}

async fn handle_server_stream(mut stream: TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut header = [0u8; 9];
    stream.read_exact(&mut header).await?;
    let bytes = u64::from_be_bytes(header[1..].try_into().unwrap());
    let mut buffer = vec![0u8; BUFFER_SIZE];
    match header[0] {
        b'U' => {
            let mut remaining = bytes;
            while remaining != 0 {
                let amount = remaining.min(buffer.len() as u64) as usize;
                stream.read_exact(&mut buffer[..amount]).await?;
                remaining -= amount as u64;
            }
            stream.write_all(&[1]).await?;
        }
        b'D' => {
            let mut remaining = bytes;
            while remaining != 0 {
                let amount = remaining.min(buffer.len() as u64) as usize;
                stream.write_all(&buffer[..amount]).await?;
                remaining -= amount as u64;
            }
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unknown operation",
            ))
        }
    }
    stream.shutdown().await
}

async fn run_benchmark(path: Path, options: BenchOptions) -> io::Result<()> {
    // Create pools and physical H2 connections before the measured warm run.
    run_one(&path, options.direction, 0).await?;

    let semaphore = Arc::new(Semaphore::new(options.concurrency));
    let overall = Instant::now();
    let mut tasks = JoinSet::new();
    for _ in 0..options.iterations {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let path = path.clone();
        let direction = options.direction;
        let bytes = options.bytes;
        tasks.spawn(async move {
            let started = Instant::now();
            let result = run_one(&path, direction, bytes).await;
            drop(permit);
            result.map(|_| started.elapsed())
        });
    }
    let mut samples = Vec::with_capacity(options.iterations);
    while let Some(result) = tasks.join_next().await {
        samples.push(result.map_err(io::Error::other)??.as_secs_f64() * 1000.0);
    }
    let elapsed = overall.elapsed().as_secs_f64();
    samples.sort_by(f64::total_cmp);
    let p50 = percentile(&samples, 0.50);
    let p95 = percentile(&samples, 0.95);
    let p99 = percentile(&samples, 0.99);
    let total_bytes = options.bytes.saturating_mul(options.iterations as u64);
    let throughput = if elapsed == 0.0 {
        0.0
    } else {
        total_bytes as f64 / 1024.0 / 1024.0 / elapsed
    };
    println!(
        "{{\"path\":\"{}\",\"direction\":\"{}\",\"bytes_per_stream\":{},\"iterations\":{},\"concurrency\":{},\"errors\":0,\"elapsed_ms\":{:.3},\"throughput_mib_s\":{:.3},\"p50_ms\":{:.3},\"p95_ms\":{:.3},\"p99_ms\":{:.3}}}",
        path_name(&path),
        direction_name(options.direction),
        options.bytes,
        options.iterations,
        options.concurrency,
        elapsed * 1000.0,
        throughput,
        p50,
        p95,
        p99,
    );
    Ok(())
}

async fn run_one(path: &Path, direction: Direction, bytes: u64) -> io::Result<()> {
    let mut stream = open(path).await?;
    let mut header = [0u8; 9];
    header[0] = match direction {
        Direction::Upload => b'U',
        Direction::Download => b'D',
    };
    header[1..].copy_from_slice(&bytes.to_be_bytes());
    stream.write_all(&header).await?;
    let buffer = vec![0u8; BUFFER_SIZE];
    match direction {
        Direction::Upload => {
            let mut remaining = bytes;
            while remaining != 0 {
                let amount = remaining.min(buffer.len() as u64) as usize;
                stream.write_all(&buffer[..amount]).await?;
                remaining -= amount as u64;
            }
            stream.shutdown().await?;
            let mut ack = [0u8; 1];
            stream.read_exact(&mut ack).await?;
        }
        Direction::Download => {
            let mut remaining = bytes;
            let mut buffer = buffer;
            while remaining != 0 {
                let amount = remaining.min(buffer.len() as u64) as usize;
                stream.read_exact(&mut buffer[..amount]).await?;
                remaining -= amount as u64;
            }
            let mut trailing = [0u8; 1];
            if stream.read(&mut trailing).await? != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "download server sent more bytes than requested",
                ));
            }
        }
    }
    Ok(())
}

async fn open(path: &Path) -> io::Result<BoxIo> {
    match path {
        Path::Direct { target } => {
            let stream = TcpStream::connect(target).await?;
            stream.set_nodelay(true)?;
            Ok(Box::new(stream))
        }
        Path::Node {
            connector,
            node,
            target,
        } => {
            let (cancel, cancelled) = watch::channel(false);
            let stream = connector.connect_stream(node, target, cancelled).await?;
            Ok(Box::new(NodeStream {
                inner: stream,
                _cancel: cancel,
            }))
        }
        Path::Edge {
            edge,
            instance,
            target_port,
            tls,
        } => Ok(Box::new(
            connect_edge(ConnectClientConfig {
                edge_address: edge.clone(),
                instance_id: instance.clone(),
                target_port: *target_port,
                access_kind: AccessKind::PortForwarding,
                bearer_token: String::new(),
                request_id: uuid::Uuid::new_v4().to_string(),
                tls: tls.clone(),
            })
            .await?,
        )),
    }
}

fn percentile(samples: &[f64], percentile: f64) -> f64 {
    let index = ((samples.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(samples.len() - 1);
    samples[index]
}

fn path_name(path: &Path) -> &'static str {
    match path {
        Path::Direct { .. } => "direct-tcp",
        Path::Node { .. } => "node-h2",
        Path::Edge { tls: None, .. } => "edge-node",
        Path::Edge { tls: Some(_), .. } => "edge-tls-node",
    }
}

fn direction_name(direction: Direction) -> &'static str {
    match direction {
        Direction::Upload => "upload",
        Direction::Download => "download",
    }
}

fn required(
    args: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    args.next()
        .ok_or_else(|| format!("missing {name}\n{}", usage()).into())
}

fn ensure_done(args: &mut impl Iterator<Item = String>) -> Result<(), Box<dyn std::error::Error>> {
    if args.next().is_some() {
        return Err(usage().into());
    }
    Ok(())
}

fn usage() -> &'static str {
    "usage:\n  relay_perf server <bind>\n  relay_perf http-server <bind>\n  relay_perf bench-direct <target> <upload|download> <bytes> <iterations> <concurrency>\n  relay_perf bench-node <node> <target-ip> <target-port> <upload|download> <bytes> <iterations> <concurrency>\n  relay_perf bench-edge <edge> <instance> <target-port> <upload|download> <bytes> <iterations> <concurrency>\n  relay_perf bench-edge-tls <edge> <instance> <target-port> <ca-path> <server-name> <upload|download> <bytes> <iterations> <concurrency>\n  relay_perf bench-http-tls <edge> <ca-path> <server-name> <token> <path-template-with-{target}> <targets> <requests> <concurrency> <warmup>"
}

fn load_tls(ca_path: &str, server_name: String) -> io::Result<EdgeTlsConfig> {
    let mut roots = rustls::RootCertStore::empty();
    let mut reader = BufReader::new(File::open(ca_path)?);
    let certificates = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "TLS CA contains no certificates",
        ));
    }
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    }
    Ok(EdgeTlsConfig {
        client: Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        ),
        server_name,
    })
}
