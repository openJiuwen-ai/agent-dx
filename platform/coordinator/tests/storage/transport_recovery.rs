//! Real Redis behind a connection-scoped proxy: a fresh TCP stays usable while
//! the old connection loses responses or carries a not-yet-delivered mutation.
use super::*;
use adx_coordinator::routes::RoutePublisher;
use adx_coordinator::storage::RedisStore;
use adx_protocol::{
    auth::Peers,
    control::{self as pb, environment_directory_service_server::EnvironmentDirectoryService},
};
use std::sync::{
    atomic::{AtomicBool, AtomicU8, Ordering},
    Arc, Mutex,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, UnixStream},
    sync::Notify,
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tonic::Request;

#[derive(Default)]
struct Gate {
    // 1 stalls responses after Redis has executed; 2 stalls outgoing commands.
    mode: AtomicU8,
    mutation_seen: AtomicBool,
    reached: Notify,
}
struct Proxy {
    url: String,
    gate: Arc<Gate>,
    listener: JoinHandle<()>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}
async fn forward<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut reader: R,
    mut writer: W,
    gate: Arc<Gate>,
    direction: u8,
) {
    let mut buffer = [0; 8192];
    loop {
        let size = match reader.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(size) => size,
        };
        if gate.mode.load(Ordering::Acquire) == direction
            && gate.mutation_seen.load(Ordering::Acquire)
        {
            gate.reached.notify_one();
            while gate.mode.load(Ordering::Acquire) == direction {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        if writer.write_all(&buffer[..size]).await.is_err() {
            break;
        }
    }
}
async fn forward_commands<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    reader: R,
    mut writer: W,
    gate: Arc<Gate>,
) {
    let mut reader = BufReader::new(reader);
    loop {
        let mut header = String::new();
        if !matches!(reader.read_line(&mut header).await, Ok(n) if n > 0) {
            break;
        }
        let count: usize = header.strip_prefix('*').unwrap().trim().parse().unwrap();
        let mut frame = header.into_bytes();
        let mut eval = false;
        for index in 0..count {
            let mut header = String::new();
            if reader.read_line(&mut header).await.is_err() {
                return;
            }
            let size: usize = header.strip_prefix('$').unwrap().trim().parse().unwrap();
            assert!(size < 1024 * 1024);
            let mut bytes = vec![0; size + 2];
            if reader.read_exact(&mut bytes).await.is_err() {
                return;
            }
            if index == 0 {
                eval = &bytes[..size] == b"EVAL";
            }
            frame.extend_from_slice(header.as_bytes());
            frame.extend_from_slice(&bytes);
        }
        if eval && gate.mode.load(Ordering::Acquire) != 0 {
            gate.mutation_seen.store(true, Ordering::Release);
            if gate.mode.load(Ordering::Acquire) == 2 {
                gate.reached.notify_one();
                while gate.mode.load(Ordering::Acquire) == 2 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
        if writer.write_all(&frame).await.is_err() {
            break;
        }
    }
}
impl Proxy {
    async fn new(upstream: &str) -> Self {
        let socket = upstream.strip_prefix("redis+unix://").unwrap().to_owned();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("redis://{}/", listener.local_addr().unwrap());
        let gate = Arc::new(Gate::default());
        let first_gate = gate.clone();
        let tasks = Arc::new(Mutex::new(Vec::new()));
        let connections = tasks.clone();
        let listener = tokio::spawn(async move {
            let mut first = true;
            loop {
                let (client, _) = listener.accept().await.unwrap();
                let server = UnixStream::connect(&socket).await.unwrap();
                let gate = if first {
                    first_gate.clone()
                } else {
                    Arc::new(Gate::default())
                };
                first = false;
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();
                let mut tasks = connections.lock().unwrap();
                tasks.push(tokio::spawn(forward_commands(
                    client_read,
                    server_write,
                    gate.clone(),
                )));
                tasks.push(tokio::spawn(forward(server_read, client_write, gate, 1)));
            }
        });
        Self {
            url,
            gate,
            listener,
            tasks,
        }
    }
    fn stall(&self, direction: u8) {
        self.gate
            .mutation_seen
            .store(direction == 1, Ordering::Release);
        self.gate.mode.store(direction, Ordering::Release);
    }
    fn stall_mutation(&self, direction: u8) {
        self.gate.mutation_seen.store(false, Ordering::Release);
        self.gate.mode.store(direction, Ordering::Release);
    }
    fn release(&self) {
        self.gate.mode.store(0, Ordering::Release);
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.release();
        self.listener.abort();
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}
fn record(id: &str) -> EnvironmentRecord {
    running(
        spec(id),
        Assignment {
            environment_id: id.into(),
            node_id: "node".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        },
    )
}
async fn directory(publisher: &RoutePublisher) -> pb::EnvironmentDirectoryFrame {
    let mut request = Request::new(pb::WatchEnvironmentsRequest {});
    request
        .metadata_mut()
        .insert("adx-component", "apiserver".parse().unwrap());
    publisher
        .watch_environments(request)
        .await
        .unwrap()
        .into_inner()
        .next()
        .await
        .unwrap()
        .unwrap()
}
async fn fixture() -> (
    common::Redis,
    Proxy,
    adx_coordinator::storage::Session,
    RoutePublisher,
) {
    let redis = common::Redis::new().await;
    let proxy = Proxy::new(&redis.url).await;
    let store = RedisStore::connect(&proxy.url, "test", Duration::from_millis(250))
        .await
        .unwrap();
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    let publisher = RoutePublisher::new(session.clone(), Peers::network());
    publisher.refresh().await.unwrap();
    (redis, proxy, session, publisher)
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn transport_recovery_replaces_a_response_stalled_connection() {
    let (_redis, proxy, session, _publisher) = fixture().await;
    proxy.stall(1);
    assert!(matches!(
        session.get("absent").await,
        Err(Error::Unavailable(_))
    ));
    assert!(
        matches!(session.get("absent").await, Err(Error::NotFound)),
        "the next bounded call must recover through a fresh connection"
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn transport_recovery_repairs_an_applied_write_with_a_lost_response() {
    let (_redis, proxy, session, publisher) = fixture().await;
    let record = record("applied-before-timeout");
    proxy.stall_mutation(1);
    assert!(matches!(
        session
            .publish_initial(record.clone(), "applied-request", "")
            .await,
        Err(Error::Unavailable(_))
    ));
    publisher.refresh().await.unwrap();
    assert_eq!(directory(&publisher).await.upserts.len(), 1);
    assert_eq!(
        session
            .published_request("tenant", "applied-request")
            .await
            .unwrap(),
        Some(record.clone())
    );
    proxy.release();
    assert_eq!(
        session
            .publish_initial(record.clone(), "applied-request", "")
            .await
            .unwrap(),
        record
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn transport_recovery_fences_a_delayed_write_before_publishing_absence() {
    let (_redis, proxy, session, publisher) = fixture().await;
    let record = record("delayed-before-redis");
    proxy.stall_mutation(2);
    assert!(matches!(
        session
            .publish_initial(record.clone(), "delayed-request", "")
            .await,
        Err(Error::Unavailable(_))
    ));
    publisher.refresh().await.unwrap();
    assert!(directory(&publisher).await.upserts.is_empty());
    proxy.release();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        matches!(session.get(&record.spec.id).await, Err(Error::NotFound)),
        "a delayed old-connection command must not execute after repair"
    );
    assert_eq!(
        session
            .publish_initial(record.clone(), "delayed-request", "")
            .await
            .unwrap(),
        record
    );
    publisher.refresh().await.unwrap();
    assert_eq!(directory(&publisher).await.upserts.len(), 1);
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn transport_recovery_fences_a_cancelled_write_without_waiting_for_its_timeout() {
    let (_redis, proxy, session, publisher) = fixture().await;
    let record = record("cancelled-before-redis");
    proxy.stall_mutation(2);
    let writer = tokio::spawn({
        let session = session.clone();
        let record = record.clone();
        async move {
            session
                .publish_initial(record, "cancelled-request", "")
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), proxy.gate.reached.notified())
        .await
        .unwrap();
    writer.abort();
    assert!(writer.await.unwrap_err().is_cancelled());
    publisher.refresh().await.unwrap();
    assert!(directory(&publisher).await.upserts.is_empty());
    proxy.release();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(
        session.get(&record.spec.id).await,
        Err(Error::NotFound)
    ));
    assert_eq!(
        session
            .publish_initial(record.clone(), "cancelled-request", "")
            .await
            .unwrap(),
        record
    );
}
