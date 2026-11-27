use super::*;
use crate::ingress::{H2PoolConfig, RouteStore};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, timeout};

type Sender = hyper::client::conn::http1::SendRequest<Full<Bytes>>;

struct Fixture {
    address: SocketAddr,
    accepted: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    release: Arc<Notify>,
    tasks: Vec<JoinHandle<()>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

struct ActiveConnection(Arc<AtomicUsize>);
impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

async fn backend_connection(
    mut stream: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    requests: Arc<AtomicUsize>,
    release: Arc<Notify>,
) {
    loop {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            if stream.read_exact(&mut byte).await.is_err() {
                return;
            }
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap();
        requests.fetch_add(1, Ordering::SeqCst);
        let content_length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0; content_length];
        if stream.read_exact(&mut body).await.is_err() {
            return;
        }
        if head
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked")
        {
            loop {
                let mut line = Vec::new();
                while !line.ends_with(b"\r\n") {
                    let Ok(byte) = stream.read_u8().await else {
                        return;
                    };
                    line.push(byte);
                }
                let line = String::from_utf8(line).unwrap();
                let length =
                    usize::from_str_radix(line.trim().split(';').next().unwrap(), 16).unwrap();
                if length == 0 {
                    loop {
                        let mut line = Vec::new();
                        while !line.ends_with(b"\r\n") {
                            let Ok(byte) = stream.read_u8().await else {
                                return;
                            };
                            line.push(byte);
                        }
                        if line == b"\r\n" {
                            break;
                        }
                    }
                    break;
                }
                let offset = body.len();
                body.resize(offset + length, 0);
                stream.read_exact(&mut body[offset..]).await.unwrap();
                assert_eq!(stream.read_u16().await.unwrap(), 0x0d0a);
            }
        }
        let path = head.split_whitespace().nth(1).unwrap();
        match path {
            "/api/progress" => {
                stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
                for byte in [b"a", b"b", b"c", b"d"] {
                    if stream.write_all(b"1\r\n").await.is_err()
                        || stream.write_all(byte).await.is_err()
                        || stream.write_all(b"\r\n").await.is_err()
                    {
                        return;
                    }
                    sleep(Duration::from_millis(120)).await;
                }
                let _ = stream.write_all(b"0\r\n\r\n").await;
            }
            "/login" => {
                if stream.write_all(b"HTTP/1.1 302 Found\r\nLocation: /grafana/login?next=%2Fgrafana%2F\r\nSet-Cookie: session=one; Path=/grafana/; HttpOnly\r\nSet-Cookie: csrf=two; Path=/grafana/\r\nContent-Length: 0\r\n\r\n").await.is_err() { return; }
            }
            "/api/fail" => return,
            "/api/stream" => {
                if stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n3\r\none\r\n").await.is_err() { return; }
                let mut byte = [0];
                tokio::select! {
                    _ = release.notified() => {},
                    _ = stream.read(&mut byte) => return,
                }
                if stream.write_all(b"3\r\ntwo\r\n0\r\n\r\n").await.is_err() {
                    return;
                }
            }
            "/api/upgrade" => {
                if stream.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n").await.is_err() { return; }
                let (mut reader, mut writer) = tokio::io::split(stream);
                let _ = tokio::io::copy(&mut reader, &mut writer).await;
                return;
            }
            "/api/legacy-ws" => {
                if !head.starts_with("GET /api/legacy-ws HTTP/1.1\r\n") {
                    stream.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n").await.unwrap();
                    continue;
                }
                let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(
                    b"dGhlIHNhbXBsZSBub25jZQ==",
                );
                stream.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
                let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
                    stream, tokio_tungstenite::tungstenite::protocol::Role::Server, None,
                ).await;
                ws.send(tokio_tungstenite::tungstenite::Message::Text("tunnel hello".into())).await.unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    if message.is_close() { break; }
                    if ws.send(message).await.is_err() { break; }
                }
                return;
            }
            _ => {
                let close = path == "/api/close";
                let mut reply = head.into_bytes();
                reply.extend(body);
                let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nSet-Cookie: a=1\r\nSet-Cookie: b=2\r\n{}\r\n", reply.len(), if close { "Connection: close\r\n" } else { "" });
                if stream.write_all(response.as_bytes()).await.is_err()
                    || stream.write_all(&reply).await.is_err()
                {
                    return;
                }
                if close {
                    return;
                }
            }
        }
    }
}

#[tokio::test]
async fn transfer_reverse_proxy_sse_survives_processing_deadline() {
    let mut fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut sender = fixture.sender().await;
    let mut req = request("/api/progress");
    req.headers_mut().insert(
        "X-ADX-Processing-Timeout-Ms",
        http::HeaderValue::from_static("240"),
    );
    let response = sender.send_request(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = timeout(Duration::from_secs(2), response.into_body().collect())
        .await
        .unwrap()
        .expect("SSE must keep delivering while data is arriving")
        .to_bytes();
    assert_eq!(&body[..], b"abcd");
}

impl Fixture {
    async fn new(config: ReverseProxyConfig) -> Self {
        Self::with_mount(config, None).await
    }

    async fn with_mount(config: ReverseProxyConfig, mount: Option<&str>) -> Self {
        Self::with_mount_and_security(config, mount, IngressSecurity::Tls).await
    }

    async fn with_mount_and_security(
        config: ReverseProxyConfig,
        mount: Option<&str>,
        security: IngressSecurity,
    ) -> Self {
        Self::start(config, mount, security, None).await
    }

    async fn start(
        config: ReverseProxyConfig,
        mount: Option<&str>,
        security: IngressSecurity,
        upstream_tls: Option<(tokio_rustls::TlsAcceptor, PathBuf, &str)>,
    ) -> Self {
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let upstream_acceptor = upstream_tls
            .as_ref()
            .map(|(acceptor, _, _)| acceptor.clone());
        let backend_task = {
            let (accepted, active, requests, release) = (
                accepted.clone(),
                active.clone(),
                requests.clone(),
                release.clone(),
            );
            tokio::spawn(async move {
                let mut tasks = JoinSet::new();
                loop {
                    tokio::select! {
                        result = backend.accept() => {
                            let (stream, _) = result.unwrap();
                            accepted.fetch_add(1, Ordering::SeqCst);
                            active.fetch_add(1, Ordering::SeqCst);
                            let guard = ActiveConnection(active.clone());
                            let (requests, release) = (requests.clone(), release.clone());
                            let upstream_acceptor = upstream_acceptor.clone();
                            tasks.spawn(async move {
                                let _guard = guard;
                                if let Some(acceptor) = upstream_acceptor {
                                    if let Ok(stream) = acceptor.accept(stream).await {
                                        backend_connection(stream, requests, release).await;
                                    }
                                } else {
                                    backend_connection(stream, requests, release).await;
                                }
                            });
                        }
                        _ = tasks.join_next(), if !tasks.is_empty() => {},
                    }
                }
            })
        };
        let upstream = match &upstream_tls {
            Some((_, _, host)) => format!("https://{host}:{}", backend_address.port()),
            None => backend_address.to_string(),
        };
        let ingress = Ingress::new(
            Arc::new(IngressRouteResolver::new(Arc::new(RouteStore::new()))),
            DataPlaneL4Connector::new(H2PoolConfig::default()),
            IngressAuthenticator::new(true, false, "", Duration::from_secs(30)).unwrap(),
            8888,
            8765,
            upstream,
            crate::ingress::parse_static_routes("prefix:/api").unwrap(),
        )
        .with_reverse_proxy_config(config, upstream_tls.as_ref().map(|(_, ca, _)| ca.as_path()))
        .unwrap()
        .with_proxy_routes(
            mount
                .map(|prefix| {
                    vec![ProxyRoute {
                        name: "grafana".into(),
                        path_prefix: prefix.into(),
                        upstream: format!("http://{backend_address}"),
                        strip_prefix: true,
                        host: None,
                    }]
                })
                .unwrap_or_default(),
        );
        let ingress = Arc::new(ingress);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let ingress_task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (stream, peer) = result.unwrap();
                        let ingress = ingress.clone();
                        tasks.spawn(async move {
                            ingress.serve_http_connection(stream, peer, security).await;
                        });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {},
                }
            }
        });
        Self {
            address,
            accepted,
            active,
            requests,
            release,
            tasks: vec![backend_task, ingress_task],
        }
    }

    async fn sender(&mut self) -> Sender {
        let stream = TcpStream::connect(self.address).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        self.tasks.push(tokio::spawn(async move {
            let _ = connection.with_upgrades().await;
        }));
        sender
    }
}

#[tokio::test]
async fn control_plane_https_verifies_ca_and_name_and_preserves_environment_requests() {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose};

    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let mut ca = CertificateParams::default();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let ca_key = KeyPair::generate().unwrap();
    for (name, key) in [
        ("ca.pem", &ca_key),
        ("other.pem", &KeyPair::generate().unwrap()),
    ] {
        std::fs::write(dir.join(name), ca.self_signed(key).unwrap().pem()).unwrap();
    }
    let server_key = KeyPair::generate().unwrap();
    let certificate = CertificateParams::new(vec!["localhost".into()])
        .unwrap()
        .signed_by(&server_key, &Issuer::new(ca, ca_key))
        .unwrap();
    std::fs::write(dir.join("server.pem"), certificate.pem()).unwrap();
    std::fs::write(dir.join("server.key"), server_key.serialize_pem()).unwrap();
    let acceptor = adx_transport::tls::http_server_acceptor(
        dir.join("server.pem"),
        dir.join("server.key"),
        None,
        vec![b"http/1.1".to_vec()],
    )
    .unwrap();
    for (ca, host, trusted) in [
        ("ca.pem", "localhost", true),
        ("other.pem", "localhost", false),
        ("ca.pem", "127.0.0.1", false),
    ] {
        let mut fixture = Fixture::start(
            ReverseProxyConfig::default(),
            None,
            IngressSecurity::Tls,
            Some((acceptor.clone(), dir.join(ca), host)),
        )
        .await;
        let mut sender = fixture.sender().await;
        let get = Request::builder()
            .uri("/api/environment/v1/resources?label=a%2Fb")
            .header("host", "public.example:8443")
            .header("authorization", "Bearer test-tenant")
            .body(Full::new(Bytes::new()))
            .unwrap();
        let response = timeout(Duration::from_secs(3), sender.send_request(get))
            .await
            .unwrap()
            .unwrap();
        if trusted {
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let body = std::str::from_utf8(&body).unwrap();
            assert!(body.starts_with("GET /api/environment/v1/resources?label=a%2Fb HTTP/1.1\r\n"));
            assert!(body.contains("authorization: Bearer test-tenant\r\n"));
            let body = complete(&mut sender, "/api/environment/v1/environments").await;
            assert!(body.starts_with(b"POST /api/environment/v1/environments HTTP/1.1\r\n"));
            assert!(body.ends_with(b"hello"));
            assert_eq!(fixture.requests.load(Ordering::SeqCst), 2);
            assert_eq!(fixture.accepted.load(Ordering::SeqCst), 1);
        } else {
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY, "{ca} {host}");
            assert_eq!(fixture.requests.load(Ordering::SeqCst), 0);
        }
    }
}

fn request(path: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header("host", "public.example:8443")
        .header("x-request-id", "stable-id")
        .header("content-length", "5")
        .body(Full::new(Bytes::from_static(b"hello")))
        .unwrap()
}

async fn complete(sender: &mut Sender, path: &str) -> Bytes {
    let response = timeout(Duration::from_secs(3), sender.send_request(request(path)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
    response.into_body().collect().await.unwrap().to_bytes()
}

async fn wait_active(fixture: &Fixture, expected: usize) {
    timeout(Duration::from_secs(3), async {
        while fixture.active.load(Ordering::SeqCst) != expected {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn reverse_proxy_reuses_connections_and_preserves_request() {
    for idle_limit in [0, 4] {
        let mut fixture = Fixture::new(ReverseProxyConfig {
            max_idle_connections: idle_limit,
            ..Default::default()
        })
        .await;
        let mut sender = fixture.sender().await;
        let start = Instant::now();
        for _ in 0..100 {
            let response = complete(&mut sender, "/api/echo?q=a%2Fb").await;
            let response = String::from_utf8(response.to_vec()).unwrap().to_lowercase();
            for value in [
                "post /api/echo?q=a%2fb http/1.1",
                "host: public.example:8443",
                "x-forwarded-host: public.example:8443",
                "x-forwarded-proto: https",
                "x-real-ip: 127.0.0.1",
                "x-request-id: stable-id",
            ] {
                assert!(response.contains(value), "missing {value}: {response}");
            }
            assert!(response.ends_with("hello"));
        }
        let connections = fixture.accepted.load(Ordering::SeqCst);
        println!("frontend keep-alive: idle_limit={idle_limit}, requests=100, connections={connections}, elapsed_ms={}", start.elapsed().as_millis());
        assert_eq!(connections, if idle_limit == 0 { 100 } else { 1 });
    }
}

#[tokio::test]
async fn reverse_proxy_stream_does_not_block_other_requests() {
    let mut fixture = Fixture::new(ReverseProxyConfig {
        max_idle_connections: 1,
        ..Default::default()
    })
    .await;
    let mut streaming = fixture.sender().await;
    let mut body = streaming
        .send_request(request("/api/stream"))
        .await
        .unwrap()
        .into_body();
    let first = timeout(Duration::from_secs(3), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert_eq!(first, "one");
    let mut other = fixture.sender().await;
    complete(&mut other, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
    fixture.release.notify_one();
    assert_eq!(body.collect().await.unwrap().to_bytes(), "two");
    wait_active(&fixture, 1).await;
    complete(&mut other, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_incomplete_response_is_discarded() {
    let mut fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut sender = fixture.sender().await;
    let mut body = sender
        .send_request(request("/api/stream"))
        .await
        .unwrap()
        .into_body();
    body.frame().await.unwrap().unwrap();
    drop(body);
    drop(sender);
    wait_active(&fixture, 0).await;
    let mut next = fixture.sender().await;
    complete(&mut next, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_idle_connections_expire() {
    let mut fixture = Fixture::new(ReverseProxyConfig {
        idle_timeout: Duration::from_millis(50),
        ..Default::default()
    })
    .await;
    let mut sender = fixture.sender().await;
    complete(&mut sender, "/api/normal").await;
    wait_active(&fixture, 0).await;
    complete(&mut sender, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_post_failure_is_not_replayed() {
    let mut fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut sender = fixture.sender().await;
    complete(&mut sender, "/api/normal").await;
    let response = sender.send_request(request("/api/fail")).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    response.into_body().collect().await.unwrap();
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 2);
    complete(&mut sender, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_connection_close_is_not_reused() {
    let mut fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut sender = fixture.sender().await;
    complete(&mut sender, "/api/close").await;
    let mut sender = fixture.sender().await;
    complete(&mut sender, "/api/normal").await;
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_upgrade_keeps_its_own_connection() {
    let mut fixture = Fixture::with_mount(ReverseProxyConfig::default(), Some("/grafana")).await;
    let mut sender = fixture.sender().await;
    let mut req = request("/grafana/api/upgrade");
    req.headers_mut().insert(
        header::CONNECTION,
        http::HeaderValue::from_static("upgrade"),
    );
    req.headers_mut()
        .insert(header::UPGRADE, http::HeaderValue::from_static("websocket"));
    let response = sender.send_request(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    let mut upgraded = TokioIo::new(hyper::upgrade::on(response).await.unwrap());
    upgraded.write_all(b"ping").await.unwrap();
    let mut echoed = [0; 4];
    timeout(Duration::from_secs(3), upgraded.read_exact(&mut echoed))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&echoed, b"ping");
    let mut other = fixture.sender().await;
    for _ in 0..3 {
        complete(&mut other, "/api/normal").await;
    }
    assert_eq!(fixture.accepted.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn reverse_proxy_configured_prefix_rewrites_path_and_keeps_frontend_routes() {
    let mut fixture = Fixture::with_mount(ReverseProxyConfig::default(), Some("/grafana")).await;
    let mut sender = fixture.sender().await;
    let response = complete(&mut sender, "/grafana/api/echo?q=a%2Fb").await;
    let text = String::from_utf8(response.to_vec()).unwrap().to_lowercase();
    assert!(text.contains("post /api/echo?q=a%2fb http/1.1"));
    assert!(text.contains("x-forwarded-prefix: /grafana"));
    assert!(text.contains("host: public.example:8443"));
    let direct = complete(&mut sender, "/api/normal").await;
    assert!(!String::from_utf8(direct.to_vec())
        .unwrap()
        .contains("x-forwarded-prefix"));
    let boundary = sender
        .send_request(request("/grafana2/api/echo"))
        .await
        .unwrap();
    assert_ne!(boundary.status(), StatusCode::OK);
}

#[tokio::test]
async fn reverse_proxy_preserves_app_redirects_and_cookie_paths() {
    let mut fixture = Fixture::with_mount(ReverseProxyConfig::default(), Some("/grafana")).await;
    let mut sender = fixture.sender().await;
    let response = sender
        .send_request(request("/grafana/login"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(
        response.headers()[header::LOCATION],
        "/grafana/login?next=%2Fgrafana%2F"
    );
    let cookies: Vec<_> = response
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .collect();
    assert_eq!(cookies.len(), 2);
    assert!(cookies
        .iter()
        .all(|value| value.to_str().unwrap().contains("Path=/grafana/")));
    response.into_body().collect().await.unwrap();
}

#[tokio::test]
async fn reverse_proxy_removes_connection_nominated_headers() {
    let mut fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut sender = fixture.sender().await;
    let mut req = request("/api/normal");
    req.headers_mut().insert(
        header::CONNECTION,
        http::HeaderValue::from_static("x-hop-secret, keep-alive"),
    );
    req.headers_mut()
        .insert("x-hop-secret", http::HeaderValue::from_static("private"));
    req.headers_mut()
        .insert("forwarded", http::HeaderValue::from_static("for=attacker"));
    let response = sender.send_request(req).await.unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap().to_lowercase();
    assert!(!text.contains("x-hop-secret"));
    assert!(!text.contains("for=attacker"));
}

#[tokio::test]
async fn explicit_authenticated_http_reports_actual_proxy_transport() {
    let mut fixture = Fixture::with_mount_and_security(
        ReverseProxyConfig::default(),
        None,
        IngressSecurity::AuthenticatedHttp,
    )
    .await;
    let mut sender = fixture.sender().await;
    let response = complete(&mut sender, "/api/echo").await;
    let response = String::from_utf8(response.to_vec()).unwrap().to_lowercase();
    assert!(response.contains("x-forwarded-proto: http"), "{response}");
    assert!(!response.contains("x-forwarded-proto: https"));
}

async fn raw_http_reply(stream: &mut TcpStream) -> (String, Vec<u8>) {
    timeout(Duration::from_secs(5), async {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(stream.read_u8().await.expect("HTTP response head"));
        }
        let head = String::from_utf8(head).unwrap();
        let length = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await.unwrap();
        (head, body)
    })
    .await
    .expect("HTTP reply timeout")
}

fn legacy_ws_head(path: &str, extra: &str) -> String {
    format!("GET {path} HTTP/1.0\r\nHost: public.example\r\nConnection: keep-alive, UpGrAdE\r\nUpgrade: WebSocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n{extra}\r\n")
}

async fn check_legacy_ws(mut stream: TcpStream, path: &str, extra: &str) {
    // Split the request line and header terminator across writes.
    let head = legacy_ws_head(path, extra);
    for part in head.as_bytes().chunks(7) {
        stream.write_all(part).await.unwrap();
        tokio::task::yield_now().await;
    }
    check_ws_connected(stream).await;
}

async fn check_ws_connected(mut stream: TcpStream) {
    let (reply, body) = raw_http_reply(&mut stream).await;
    assert!(
        reply.starts_with("HTTP/1.1 101 "),
        "{reply} {}",
        String::from_utf8_lossy(&body)
    );
    assert!(reply
        .to_ascii_lowercase()
        .contains("sec-websocket-accept: s3pplmbitxaq9kygzzhzrbk+xoo="));
    let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
        stream,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    assert_eq!(
        timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap(),
        "tunnel hello"
    );
    // HTTP-looking frame data must stay opaque after the upgrade.
    let payload = legacy_ws_head("/inside-frame", "");
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        payload.clone(),
    ))
    .await
    .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_text()
            .unwrap(),
        payload
    );
    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn http10_websocket_new_connection() {
    let fixture = Fixture::new(ReverseProxyConfig::default()).await;
    check_legacy_ws(
        TcpStream::connect(fixture.address).await.unwrap(),
        "/api/legacy-ws",
        "",
    )
    .await;
}

#[tokio::test]
async fn http10_websocket_reused_connection_after_fixed_body() {
    let fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut stream = TcpStream::connect(fixture.address).await.unwrap();
    // A complete fake handshake in the POST body must not be rewritten.
    let body = legacy_ws_head("/inside-body", "");
    stream.write_all(format!("POST /api/normal HTTP/1.1\r\nHost: public.example\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    let (reply, echoed) = raw_http_reply(&mut stream).await;
    assert!(reply.starts_with("HTTP/1.1 200 "));
    assert!(echoed.ends_with(body.as_bytes()));
    check_legacy_ws(stream, "/api/legacy-ws", "Content-Length: 0\r\n").await;
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture.accepted.load(Ordering::SeqCst),
        2,
        "WebSocket must use a fresh backend connection after ordinary HTTP"
    );
}

#[tokio::test]
async fn http10_websocket_reused_connection_after_chunked_body() {
    let fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut stream = TcpStream::connect(fixture.address).await.unwrap();
    let body = legacy_ws_head("/inside-chunk", "");
    let wire = format!("POST /api/normal HTTP/1.1\r\nHost: public.example\r\nTransfer-Encoding: chunked\r\nTrailer: X-End\r\n\r\n{:x};tag=yes\r\n{body}\r\n0\r\nX-End: done\r\n\r\n", body.len());
    for part in wire.as_bytes().chunks(11) {
        stream.write_all(part).await.unwrap();
    }
    let (reply, echoed) = raw_http_reply(&mut stream).await;
    assert!(reply.starts_with("HTTP/1.1 200 "));
    assert!(echoed.ends_with(body.as_bytes()));
    check_legacy_ws(stream, "/api/legacy-ws", "").await;
}

#[tokio::test]
async fn http10_websocket_application_proxy() {
    let fixture = Fixture::with_mount(ReverseProxyConfig::default(), Some("/grafana")).await;
    check_legacy_ws(
        TcpStream::connect(fixture.address).await.unwrap(),
        "/grafana/api/legacy-ws",
        "",
    )
    .await;
}

#[tokio::test]
async fn http10_websocket_retry_after_non_upgrade_response() {
    let fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut stream = TcpStream::connect(fixture.address).await.unwrap();
    stream
        .write_all(legacy_ws_head("/api/normal", "").as_bytes())
        .await
        .unwrap();
    let (reply, _) = raw_http_reply(&mut stream).await;
    assert!(reply.starts_with("HTTP/1.1 200 "));
    check_legacy_ws(stream, "/api/legacy-ws", "").await;
}

#[tokio::test]
async fn http10_websocket_pipelined_after_http10_keep_alive() {
    let fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut stream = TcpStream::connect(fixture.address).await.unwrap();
    let first =
        "GET /api/normal HTTP/1.0\r\nHost: public.example\r\nConnection: keep-alive\r\n\r\n";
    stream
        .write_all(format!("{first}{}", legacy_ws_head("/api/legacy-ws", "")).as_bytes())
        .await
        .unwrap();
    let (reply, echoed) = raw_http_reply(&mut stream).await;
    assert!(reply.starts_with("HTTP/1.0 200 "));
    assert!(echoed.starts_with(b"GET /api/normal HTTP/1.1\r\n"));
    check_ws_connected(stream).await;
}

#[tokio::test]
async fn http10_websocket_reused_connection_after_lf_headers() {
    let fixture = Fixture::new(ReverseProxyConfig::default()).await;
    let mut stream = TcpStream::connect(fixture.address).await.unwrap();
    stream
        .write_all(b"GET /api/normal HTTP/1.1\nHost: public.example\n\n")
        .await
        .unwrap();
    let (reply, _) = raw_http_reply(&mut stream).await;
    assert!(reply.starts_with("HTTP/1.1 200 "));
    check_legacy_ws(stream, "/api/legacy-ws", "").await;
}
