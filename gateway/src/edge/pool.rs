use crate::common::protocol::{
    ConnectTarget, H_INSTANCE_ID, H_REQUEST_ID, H_TARGET_IP, H_TARGET_PORT, H_WORKLOAD_ID,
};
use bytes::Bytes;
use h2::client::{self, SendRequest};
use http::{Request, StatusCode, Uri};
use rustls::{pki_types::ServerName, ClientConfig};
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tokio_rustls::TlsConnector;

// The h2 defaults (64 KiB windows and 16 KiB frames) are sized for ordinary
// HTTP messages. CONNECT streams carry bulk TCP traffic, so those defaults
// make window updates part of the steady-state hot path even on a low-latency
// node network. These values cover a realistic node-link bandwidth-delay
// product without allocating the advertised window eagerly.
const H2_STREAM_WINDOW: u32 = 4 * 1024 * 1024;
const H2_CONNECTION_WINDOW: u32 = 32 * 1024 * 1024;
const H2_MAX_FRAME_SIZE: u32 = 64 * 1024;

#[derive(Clone)]
pub struct H2PoolConfig {
    pub connections_per_node: usize,
    pub max_connections_per_node: usize,
    pub connect_timeout: Duration,
    pub tls_config: Option<Arc<ClientConfig>>,
    pub tls_server_name: Option<String>,
    pub keepalive_interval: Duration,
    pub keepalive_timeout: Duration,
    pub physical_idle_timeout: Duration,
}

impl Default for H2PoolConfig {
    fn default() -> Self {
        Self {
            connections_per_node: 2,
            max_connections_per_node: 4,
            connect_timeout: Duration::from_secs(3),
            tls_config: None,
            tls_server_name: None,
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            physical_idle_timeout: Duration::from_secs(5 * 60),
        }
    }
}

struct NodePoolState {
    senders: Vec<SendRequest<Bytes>>,
    next: usize,
}

struct NodePool {
    state: Mutex<NodePoolState>,
    connection_creation: Mutex<()>,
    active_streams: Arc<AtomicUsize>,
    last_used: Arc<StdMutex<tokio::time::Instant>>,
}

impl Default for NodePool {
    fn default() -> Self {
        Self {
            state: Mutex::new(NodePoolState {
                senders: Vec::new(),
                next: 0,
            }),
            connection_creation: Mutex::new(()),
            active_streams: Arc::new(AtomicUsize::new(0)),
            last_used: Arc::new(StdMutex::new(tokio::time::Instant::now())),
        }
    }
}

pub struct PoolStreamGuard {
    active_streams: Arc<AtomicUsize>,
    last_used: Arc<StdMutex<tokio::time::Instant>>,
}

impl Drop for PoolStreamGuard {
    fn drop(&mut self) {
        self.active_streams.fetch_sub(1, Ordering::Relaxed);
        *self.last_used.lock().unwrap() = tokio::time::Instant::now();
    }
}

#[derive(Clone)]
pub struct H2ConnectionPool {
    config: H2PoolConfig,
    nodes: Arc<Mutex<HashMap<String, Arc<NodePool>>>>,
    physical_connections: Arc<AtomicUsize>,
}

impl H2ConnectionPool {
    pub fn new(mut config: H2PoolConfig) -> Self {
        config.connections_per_node = config.connections_per_node.max(1);
        config.max_connections_per_node = config
            .max_connections_per_node
            .max(config.connections_per_node);
        let nodes = Arc::new(Mutex::new(HashMap::new()));
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let weak_nodes = Arc::downgrade(&nodes);
            let idle_timeout = config.physical_idle_timeout;
            runtime.spawn(async move {
                let interval = std::cmp::min(idle_timeout, Duration::from_secs(60))
                    .max(Duration::from_secs(1));
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(nodes) = weak_nodes.upgrade() else {
                        return;
                    };
                    prune_nodes(&nodes, idle_timeout).await;
                }
            });
        }
        Self {
            config,
            nodes,
            physical_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn physical_connection_count(&self) -> usize {
        self.physical_connections.load(Ordering::Relaxed)
    }

    pub async fn connect(
        &self,
        node: &str,
        target: &ConnectTarget,
    ) -> io::Result<(h2::SendStream<Bytes>, h2::RecvStream, PoolStreamGuard)> {
        self.connect_with_activity(node, target, false).await
    }

    pub async fn connect_with_activity(
        &self,
        node: &str,
        target: &ConnectTarget,
        passive: bool,
    ) -> io::Result<(h2::SendStream<Bytes>, h2::RecvStream, PoolStreamGuard)> {
        let mut last_error = None;
        for _ in 0..2 {
            let (sender, pool) = self.sender_for(node).await?;
            let mut sender = match sender.ready().await {
                Ok(sender) => sender,
                Err(error) => {
                    last_error = Some(io_error(error));
                    self.invalidate(node).await;
                    continue;
                }
            };
            let authority = node
                .parse::<http::uri::Authority>()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
            let uri = Uri::builder()
                .authority(authority)
                .build()
                .map_err(io_error)?;
            let mut request = Request::builder()
                .method(http::Method::CONNECT)
                .uri(uri)
                .header(H_INSTANCE_ID, &target.instance_id)
                .header(H_WORKLOAD_ID, &target.workload_id)
                .header(H_TARGET_IP, target.target_ip.to_string())
                .header(H_TARGET_PORT, target.target_port.to_string())
                .header(H_REQUEST_ID, &target.request_id);
            if passive {
                request = request.header(
                    crate::common::protocol::H_ACTIVITY_CLASS,
                    crate::common::protocol::ACTIVITY_CLASS_PASSIVE,
                );
            }
            let request = request.body(()).map_err(io_error)?;
            let (response, send) = match sender.send_request(request, false) {
                Ok(streams) => streams,
                Err(error) => {
                    last_error = Some(io_error(error));
                    self.invalidate(node).await;
                    continue;
                }
            };
            let response = response.await.map_err(io_error)?;
            if response.status() != StatusCode::OK {
                return Err(io::Error::new(
                    status_to_error_kind(response.status()),
                    format!("node proxy rejected CONNECT: {}", response.status()),
                ));
            }
            pool.active_streams.fetch_add(1, Ordering::Relaxed);
            *pool.last_used.lock().unwrap() = tokio::time::Instant::now();
            return Ok((
                send,
                response.into_body(),
                PoolStreamGuard {
                    active_streams: pool.active_streams.clone(),
                    last_used: pool.last_used.clone(),
                },
            ));
        }
        Err(last_error
            .unwrap_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "H2 pool unavailable")))
    }

    async fn sender_for(&self, node: &str) -> io::Result<(SendRequest<Bytes>, Arc<NodePool>)> {
        self.prune_idle_connections().await;
        let pool = {
            let mut nodes = self.nodes.lock().await;
            nodes
                .entry(node.to_owned())
                .or_insert_with(|| Arc::new(NodePool::default()))
                .clone()
        };

        {
            let mut state = pool.state.lock().await;
            if state.senders.len() >= self.config.connections_per_node {
                if let Some(sender) = ready_sender(&mut state) {
                    return Ok((sender, pool.clone()));
                }
            }
        }

        // Serialize physical connection creation per node without retaining the
        // sender-state lock across TCP, TLS, or H2 awaits. Existing connections
        // therefore remain selectable while a replacement is being established.
        let _creation = pool.connection_creation.lock().await;
        loop {
            let sender_count = pool.state.lock().await.senders.len();
            if sender_count >= self.config.connections_per_node {
                break;
            }
            let sender = self.open_physical(node).await?;
            pool.state.lock().await.senders.push(sender);
        }

        {
            let mut state = pool.state.lock().await;
            if let Some(sender) = ready_sender(&mut state) {
                return Ok((sender, pool.clone()));
            }
            if state.senders.len() >= self.config.max_connections_per_node {
                return Ok((next_sender(&mut state), pool.clone()));
            }
        }

        let sender = self.open_physical(node).await?;
        let mut state = pool.state.lock().await;
        state.senders.push(sender.clone());
        state.next = state.senders.len();
        Ok((sender, pool.clone()))
    }

    async fn prune_idle_connections(&self) {
        prune_nodes(&self.nodes, self.config.physical_idle_timeout).await;
    }

    async fn invalidate(&self, node: &str) {
        let pool = self.nodes.lock().await.get(node).cloned();
        if let Some(pool) = pool {
            let _creation = pool.connection_creation.lock().await;
            pool.state.lock().await.senders.clear();
        }
    }

    async fn open_physical(&self, node: &str) -> io::Result<SendRequest<Bytes>> {
        timeout(self.config.connect_timeout, self.open_physical_inner(node))
            .await
            .map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "node proxy handshake timed out")
            })?
    }

    async fn open_physical_inner(&self, node: &str) -> io::Result<SendRequest<Bytes>> {
        let tcp = TcpStream::connect(node).await?;
        tcp.set_nodelay(true)?;
        if let Some(tls_config) = &self.config.tls_config {
            let server_name = self.config.tls_server_name.clone().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "node TLS server name is required",
                )
            })?;
            let server_name = ServerName::try_from(server_name)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
            let io = TlsConnector::from(tls_config.clone())
                .connect(server_name, tcp)
                .await
                .map_err(io_error)?;
            self.handshake(io, node.to_owned()).await
        } else {
            self.handshake(tcp, node.to_owned()).await
        }
    }

    async fn handshake<I>(&self, io: I, node: String) -> io::Result<SendRequest<Bytes>>
    where
        I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut builder = client::Builder::new();
        builder
            .initial_window_size(H2_STREAM_WINDOW)
            .initial_connection_window_size(H2_CONNECTION_WINDOW)
            .max_frame_size(H2_MAX_FRAME_SIZE);
        let (sender, mut connection) = builder.handshake(io).await.map_err(io_error)?;
        let mut ping_pong = connection
            .ping_pong()
            .ok_or_else(|| io::Error::other("H2 ping handle unavailable"))?;
        // h2's client handshake only writes our preface; it does not wait for
        // the peer. Confirm protocol liveness before publishing the sender.
        // Keep the driver owned by this future until then, so the enclosing
        // connect timeout or caller cancellation also drops the socket.
        tokio::select! {
            result = &mut connection => {
                result.map_err(io_error)?;
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "node proxy closed during H2 handshake",
                ));
            }
            result = ping_pong.ping(h2::Ping::opaque()) => {
                result.map_err(io_error)?;
            }
        }
        self.physical_connections.fetch_add(1, Ordering::Relaxed);
        let physical_connections = self.physical_connections.clone();
        let keepalive_interval = self.config.keepalive_interval;
        let keepalive_timeout = self.config.keepalive_timeout;
        tokio::spawn(async move {
            let result = tokio::select! {
                result = &mut connection => result,
                result = keepalive_loop(&mut ping_pong, keepalive_interval, keepalive_timeout) => result,
            };
            if let Err(error) = result {
                tracing::debug!(%node, %error, "Edge-to-Node Proxy H2 connection closed");
            }
            physical_connections.fetch_sub(1, Ordering::Relaxed);
        });
        Ok(sender)
    }
}

fn ready_sender(state: &mut NodePoolState) -> Option<SendRequest<Bytes>> {
    let sender_count = state.senders.len();
    let mut context = Context::from_waker(futures_util::task::noop_waker_ref());
    for offset in 0..sender_count {
        let index = (state.next + offset) % sender_count;
        match state.senders[index].poll_ready(&mut context) {
            Poll::Ready(Ok(())) => {
                state.next = index.wrapping_add(1);
                return Some(state.senders[index].clone());
            }
            Poll::Ready(Err(_)) | Poll::Pending => {}
        }
    }
    None
}

fn next_sender(state: &mut NodePoolState) -> SendRequest<Bytes> {
    let index = state.next % state.senders.len();
    state.next = state.next.wrapping_add(1);
    state.senders[index].clone()
}

async fn prune_nodes(nodes: &Mutex<HashMap<String, Arc<NodePool>>>, idle_timeout: Duration) {
    let pools = nodes.lock().await.values().cloned().collect::<Vec<_>>();
    let now = tokio::time::Instant::now();
    for pool in pools {
        if pool.active_streams.load(Ordering::Relaxed) != 0
            || now.duration_since(*pool.last_used.lock().unwrap()) < idle_timeout
        {
            continue;
        }
        pool.state.lock().await.senders.clear();
    }
}

async fn keepalive_loop(
    ping_pong: &mut h2::PingPong,
    interval: Duration,
    response_timeout: Duration,
) -> Result<(), h2::Error> {
    loop {
        tokio::time::sleep(interval).await;
        timeout(response_timeout, ping_pong.ping(h2::Ping::opaque()))
            .await
            .map_err(|_| h2::Reason::SETTINGS_TIMEOUT)?
            .map(|_| ())?;
    }
}

fn status_to_error_kind(status: StatusCode) -> io::ErrorKind {
    match status {
        StatusCode::TOO_MANY_REQUESTS => io::ErrorKind::WouldBlock,
        StatusCode::GATEWAY_TIMEOUT => io::ErrorKind::TimedOut,
        StatusCode::BAD_GATEWAY => io::ErrorKind::ConnectionRefused,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => io::ErrorKind::PermissionDenied,
        StatusCode::CONFLICT => io::ErrorKind::InvalidData,
        StatusCode::SERVICE_UNAVAILABLE => io::ErrorKind::NotConnected,
        _ => io::ErrorKind::Other,
    }
}

fn io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;

    const TEST_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(100);
    const TEST_REQUEST_DEADLINE: Duration = Duration::from_secs(1);

    #[tokio::test]
    async fn stalled_tls_handshake_does_not_hold_sender_state_lock_and_times_out() {
        assert_stalled_handshake_times_out(true).await;
    }

    #[tokio::test]
    async fn stalled_h2_handshake_does_not_hold_sender_state_lock_and_times_out() {
        assert_stalled_handshake_times_out(false).await;
    }

    async fn assert_stalled_handshake_times_out(use_tls: bool) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            received
        });

        let tls_config = use_tls.then(|| {
            Arc::new(
                ClientConfig::builder()
                    .with_root_certificates(rustls::RootCertStore::empty())
                    .with_no_client_auth(),
            )
        });
        let pool = H2ConnectionPool::new(H2PoolConfig {
            connections_per_node: 1,
            max_connections_per_node: 1,
            connect_timeout: TEST_HANDSHAKE_TIMEOUT,
            tls_config,
            tls_server_name: use_tls.then(|| "localhost".to_owned()),
            ..H2PoolConfig::default()
        });
        let request_pool = pool.clone();
        let request_address = address.clone();
        let request = tokio::spawn(async move { request_pool.sender_for(&request_address).await });

        accepted_rx.await.unwrap();
        let node_pool = pool.nodes.lock().await.get(&address).cloned().unwrap();
        assert!(node_pool.state.try_lock().is_ok());

        let result = timeout(TEST_REQUEST_DEADLINE, request)
            .await
            .expect("stalled handshake request did not finish")
            .unwrap();
        let error = match result {
            Ok(_) => panic!("stalled handshake unexpectedly succeeded (TLS: {use_tls})"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(node_pool.state.lock().await.senders.is_empty());
        assert_eq!(pool.physical_connection_count(), 0);
        let received = timeout(TEST_REQUEST_DEADLINE, server)
            .await
            .expect("timed out handshake left its TCP connection open")
            .unwrap();
        if use_tls {
            assert_eq!(received.first(), Some(&0x16)); // TLS handshake record
        } else {
            assert!(received.starts_with(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"));
        }
    }

    #[tokio::test]
    async fn cancelling_pending_handshake_closes_socket_without_publishing_sender() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (preface_tx, preface_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preface = [0; 24];
            stream.read_exact(&mut preface).await.unwrap();
            preface_tx.send(()).unwrap();
            stream.read_to_end(&mut Vec::new()).await.unwrap();
        });
        let pool = H2ConnectionPool::new(H2PoolConfig {
            connections_per_node: 1,
            max_connections_per_node: 1,
            connect_timeout: Duration::from_secs(60),
            ..H2PoolConfig::default()
        });
        let request_pool = pool.clone();
        let request_address = address.clone();
        let request = tokio::spawn(async move { request_pool.sender_for(&request_address).await });
        timeout(TEST_REQUEST_DEADLINE, preface_rx)
            .await
            .unwrap()
            .unwrap();
        request.abort();
        assert!(request.await.err().unwrap().is_cancelled());
        timeout(TEST_REQUEST_DEADLINE, server)
            .await
            .expect("cancelled handshake left its TCP connection open")
            .unwrap();
        let node_pool = pool.nodes.lock().await.get(&address).cloned().unwrap();
        assert!(node_pool.state.lock().await.senders.is_empty());
        assert!(node_pool.connection_creation.try_lock().is_ok());
        assert_eq!(pool.physical_connection_count(), 0);
    }

    #[tokio::test]
    async fn responsive_h2_peer_is_pooled_and_reused() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake::<_>(stream).await.unwrap();
            // Polling accept drives the peer SETTINGS and PING/PONG exchange.
            while let Some(result) = connection.accept().await {
                let (_, mut respond) = result.unwrap();
                respond
                    .send_response(http::Response::new(()), true)
                    .unwrap();
            }
        });
        let pool = H2ConnectionPool::new(H2PoolConfig {
            connections_per_node: 1,
            max_connections_per_node: 1,
            connect_timeout: TEST_REQUEST_DEADLINE,
            ..H2PoolConfig::default()
        });
        let (sender, node_pool) = timeout(TEST_REQUEST_DEADLINE, pool.sender_for(&address))
            .await
            .unwrap()
            .unwrap();
        let (second, reused_pool) = pool.sender_for(&address).await.unwrap();
        assert!(Arc::ptr_eq(&node_pool, &reused_pool));
        assert_eq!(node_pool.state.lock().await.senders.len(), 1);
        assert_eq!(pool.physical_connection_count(), 1);
        let mut second = second.ready().await.unwrap();
        let (response, _) = second
            .send_request(
                Request::builder()
                    .uri("http://node/health")
                    .body(())
                    .unwrap(),
                true,
            )
            .unwrap();
        assert_eq!(
            timeout(TEST_REQUEST_DEADLINE, response)
                .await
                .unwrap()
                .unwrap()
                .status(),
            StatusCode::OK
        );
        drop(sender);
        drop(second);
        pool.invalidate(&address).await;
        timeout(TEST_REQUEST_DEADLINE, server)
            .await
            .unwrap()
            .unwrap();
    }
}
