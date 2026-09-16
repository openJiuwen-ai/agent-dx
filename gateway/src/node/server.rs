use super::activity::ActivityTracker;
use super::relay::{relay_h2_tcp_with_stats, RelayStats};
use crate::common::protocol::{ConnectTarget, GatewayPolicy, ProtocolError};
use bytes::Bytes;
use h2::server::SendResponse;
use http::{Request, Response, StatusCode};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io;
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::sync::Notify;
use tokio::time::timeout;

const H2_STREAM_WINDOW: u32 = 4 * 1024 * 1024;
const H2_CONNECTION_WINDOW: u32 = 32 * 1024 * 1024;
const H2_MAX_FRAME_SIZE: u32 = 64 * 1024;
const ROUTE_SHARD_COUNT: usize = 64;

#[derive(Clone)]
pub struct NodeProxy {
    pub policy: GatewayPolicy,
    pub connect_timeout: Duration,
    route_shards: Arc<Vec<RwLock<HashMap<RouteKey, RouteBinding>>>>,
    route_revision: Arc<AtomicU64>,
    route_enforcement: bool,
    bindings_ready: Arc<AtomicBool>,
    active_streams: Arc<AtomicUsize>,
    max_active_streams: usize,
    activity: Option<Arc<ActivityTracker>>,
    draining: Arc<std::sync::atomic::AtomicBool>,
    drain_notify: Arc<Notify>,
    metrics: Arc<NodeMetrics>,
}

#[derive(Default)]
struct NodeMetrics {
    connect_total: AtomicU64,
    connect_errors: AtomicU64,
    completed_streams: AtomicU64,
    bytes_up: AtomicU64,
    bytes_down: AtomicU64,
    route_mismatch: AtomicU64,
    forbidden_target: AtomicU64,
    overload_rejections: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub struct NodeMetricSnapshot {
    pub connect_total: u64,
    pub connect_errors: u64,
    pub completed_streams: u64,
    pub bytes_up: u64,
    pub bytes_down: u64,
    pub route_mismatch: u64,
    pub forbidden_target: u64,
    pub overload_rejections: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RouteKey {
    instance_id: String,
    workload_id: String,
}

struct RouteBinding {
    sandbox_ip: IpAddr,
    revision_tx: watch::Sender<u64>,
}

impl NodeProxy {
    pub fn new(policy: GatewayPolicy) -> Self {
        Self {
            policy,
            connect_timeout: Duration::from_secs(3),
            route_shards: Arc::new(
                (0..ROUTE_SHARD_COUNT)
                    .map(|_| RwLock::new(HashMap::new()))
                    .collect(),
            ),
            route_revision: Arc::new(AtomicU64::new(0)),
            route_enforcement: false,
            bindings_ready: Arc::new(AtomicBool::new(true)),
            active_streams: Arc::new(AtomicUsize::new(0)),
            max_active_streams: 0,
            activity: None,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            drain_notify: Arc::new(Notify::new()),
            metrics: Arc::new(NodeMetrics::default()),
        }
    }

    pub fn active_streams(&self) -> usize {
        self.active_streams.load(Ordering::Relaxed)
    }

    pub fn max_active_streams(&self) -> usize {
        self.max_active_streams
    }

    pub fn metrics(&self) -> NodeMetricSnapshot {
        NodeMetricSnapshot {
            connect_total: self.metrics.connect_total.load(Ordering::Relaxed),
            connect_errors: self.metrics.connect_errors.load(Ordering::Relaxed),
            completed_streams: self.metrics.completed_streams.load(Ordering::Relaxed),
            bytes_up: self.metrics.bytes_up.load(Ordering::Relaxed),
            bytes_down: self.metrics.bytes_down.load(Ordering::Relaxed),
            route_mismatch: self.metrics.route_mismatch.load(Ordering::Relaxed),
            forbidden_target: self.metrics.forbidden_target.load(Ordering::Relaxed),
            overload_rejections: self.metrics.overload_rejections.load(Ordering::Relaxed),
        }
    }

    pub fn ready(&self) -> bool {
        !self.draining.load(Ordering::Acquire) && self.bindings_ready.load(Ordering::Acquire)
    }

    pub fn start_drain(&self) {
        self.draining.store(true, Ordering::Release);
        self.drain_notify.notify_waiters();
    }

    pub fn with_max_active_streams(mut self, max: usize) -> Self {
        self.max_active_streams = max;
        self
    }

    pub fn instance_activity(&self, instance_id: &str) -> Option<(u64, u64)> {
        self.activity
            .as_ref()
            .map(|tracker| tracker.observe(instance_id))
    }

    pub fn with_activity_tracker(mut self, tracker: Arc<ActivityTracker>) -> Self {
        self.activity = Some(tracker);
        self
    }

    pub fn with_route_enforcement(mut self) -> Self {
        self.route_enforcement = true;
        self
    }

    pub fn set_bindings_ready(&self, ready: bool) {
        self.bindings_ready.store(ready, Ordering::Release);
    }
    pub fn clear_bindings(&self) {
        self.set_bindings_ready(false);
        let revision = self.route_revision.fetch_add(1, Ordering::AcqRel) + 1;
        for shard in self.route_shards.iter() {
            let mut routes = shard.write().unwrap_or_else(|p| p.into_inner());
            for (key, binding) in routes.drain() {
                if let Some(tracker) = &self.activity {
                    tracker.retire(&key.instance_id);
                }
                let _ = binding.revision_tx.send(revision);
            }
        }
    }

    fn route_shard(&self, key: &RouteKey) -> &RwLock<HashMap<RouteKey, RouteBinding>> {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        &self.route_shards[hasher.finish() as usize % self.route_shards.len()]
    }

    /// Publish one authoritative sandbox route before FunctionProxy exposes it.
    pub async fn activate_route(
        &self,
        instance_id: String,
        workload_id: String,
        sandbox_ip: IpAddr,
    ) {
        if let Some(tracker) = &self.activity {
            tracker.register(&instance_id);
        }
        let key = RouteKey {
            instance_id,
            workload_id,
        };
        let revision = self.route_revision.fetch_add(1, Ordering::AcqRel) + 1;
        let mut routes = self
            .route_shard(&key)
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(previous) = routes.remove(&key) {
            let _ = previous.revision_tx.send(revision);
        }
        let (revision_tx, _) = watch::channel(revision);
        routes.insert(
            key,
            RouteBinding {
                sandbox_ip,
                revision_tx,
            },
        );
    }

    /// Retire one route before sandboxd reclaims its IP. New streams are
    /// rejected immediately and existing relays observe the route revision.
    pub async fn retire_route(&self, instance_id: String, workload_id: String) {
        if let Some(tracker) = &self.activity {
            tracker.retire(&instance_id);
        }
        let key = RouteKey {
            instance_id,
            workload_id,
        };
        let revision = self.route_revision.fetch_add(1, Ordering::AcqRel) + 1;
        let mut routes = self
            .route_shard(&key)
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(binding) = routes.remove(&key) {
            let _ = binding.revision_tx.send(revision);
        }
    }

    /// Serve one HTTP/2 connection admitted by the listener's network ACL or mTLS boundary.
    pub async fn serve_h2<T>(&self, io: T) -> Result<(), h2::Error>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.serve_h2_with_keepalive(io, Duration::from_secs(30), Duration::from_secs(10))
            .await
    }

    async fn serve_h2_with_keepalive<T>(
        &self,
        io: T,
        keepalive_interval: Duration,
        keepalive_timeout: Duration,
    ) -> Result<(), h2::Error>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let mut builder = h2::server::Builder::new();
        builder
            .initial_window_size(H2_STREAM_WINDOW)
            .initial_connection_window_size(H2_CONNECTION_WINDOW)
            .max_frame_size(H2_MAX_FRAME_SIZE);
        if self.max_active_streams != 0 {
            builder
                .max_concurrent_streams(u32::try_from(self.max_active_streams).unwrap_or(u32::MAX));
        }
        let mut connection = builder.handshake(io).await?;
        let ping = connection
            .ping_pong()
            .expect("H2 ping handle already taken");
        let keepalive = probe_h2_connection(ping, keepalive_interval, keepalive_timeout);
        tokio::pin!(keepalive);
        // Dropping the last sender ends every relay belonging to this physical
        // connection, including when this serving future itself is cancelled.
        let (_connection_lifetime, connection_closed) = watch::channel(());
        let gateway = Arc::new(self.clone());
        let mut drain_started = self.draining.load(Ordering::Acquire);
        if drain_started {
            connection.graceful_shutdown();
        }
        loop {
            tokio::select! {
                result = &mut keepalive => return result,
                _ = self.drain_notify.notified(), if !drain_started => {
                    connection.graceful_shutdown();
                    drain_started = true;
                }
                result = connection.accept() => {
                    let Some(result) = result else { break };
                    let (request, respond) = result?;
                    let gateway = gateway.clone();
                    let connection_closed = connection_closed.clone();
                    tokio::spawn(async move {
                        gateway.handle(request, respond, connection_closed).await
                    });
                }
            }
        }
        Ok(())
    }

    async fn handle(
        &self,
        request: Request<h2::RecvStream>,
        respond: SendResponse<Bytes>,
        connection_closed: watch::Receiver<()>,
    ) {
        let trace = adx_observability::trace::Trace::remote(
            "node-proxy.connect",
            request
                .headers()
                .get("traceparent")
                .and_then(|v| v.to_str().ok()),
            request
                .headers()
                .get("tracestate")
                .and_then(|v| v.to_str().ok()),
        );
        trace
            .run(self.handle_inner(request, respond, connection_closed))
            .await;
    }
    async fn handle_inner(
        &self,
        request: Request<h2::RecvStream>,
        mut respond: SendResponse<Bytes>,
        mut connection_closed: watch::Receiver<()>,
    ) {
        if !self.ready() {
            send_error(&mut respond, StatusCode::SERVICE_UNAVAILABLE);
            return;
        }
        if request.method() != http::Method::CONNECT {
            let _ = respond.send_response(
                Response::builder()
                    .status(StatusCode::METHOD_NOT_ALLOWED)
                    .body(())
                    .unwrap(),
                true,
            );
            return;
        }
        let target = match ConnectTarget::from_headers(request.headers()) {
            Ok(target) => target,
            Err(error) => {
                send_error(&mut respond, status_for_protocol_error(&error));
                return;
            }
        };
        let passive = request
            .headers()
            .get(crate::common::protocol::H_ACTIVITY_CLASS)
            .and_then(|value| value.to_str().ok())
            == Some(crate::common::protocol::ACTIVITY_CLASS_PASSIVE);
        if let Err(error) = self.policy.validate(&target) {
            self.metrics
                .forbidden_target
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                target: "adx_audit",
                event = "target_policy",
                decision = "deny",
                instance_id = %target.instance_id,
                workload_id = %target.workload_id,
                request_id = %target.request_id,
                target_ip = %target.target_ip,
                target_port = target.target_port,
                reason = %error,
                "Node Proxy target denied"
            );
            send_error(&mut respond, status_for_protocol_error(&error));
            return;
        }
        let stream_limit = if self.max_active_streams == 0 {
            usize::MAX
        } else {
            self.max_active_streams
        };
        if !reserve_stream(&self.active_streams, stream_limit) {
            self.metrics
                .overload_rejections
                .fetch_add(1, Ordering::Relaxed);
            send_error(&mut respond, StatusCode::TOO_MANY_REQUESTS);
            return;
        }
        let _active = ActiveStreamGuard(self.active_streams.clone());
        let route_rx = if self.route_enforcement {
            let key = RouteKey {
                instance_id: target.instance_id.clone(),
                workload_id: target.workload_id.clone(),
            };
            let routes = self
                .route_shard(&key)
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(binding) = routes.get(&key) else {
                self.metrics.route_mismatch.fetch_add(1, Ordering::Relaxed);
                send_error(&mut respond, StatusCode::CONFLICT);
                return;
            };
            if binding.sandbox_ip != target.target_ip {
                self.metrics.route_mismatch.fetch_add(1, Ordering::Relaxed);
                send_error(&mut respond, StatusCode::CONFLICT);
                return;
            }
            Some(binding.revision_tx.subscribe())
        } else {
            None
        };
        let connect = tokio::select! {
            result = connect_within_budget(target.socket_addr(), self.connect_timeout) => result,
            _ = connection_closed.changed() => return,
        };
        let tcp = match connect {
            Ok(stream) => stream,
            Err(ConnectFailure::Io(error)) => {
                self.metrics.connect_errors.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(address = %target.socket_addr(), %error, "sandbox TCP connect failed");
                send_error(&mut respond, StatusCode::BAD_GATEWAY);
                return;
            }
            Err(ConnectFailure::Timeout) => {
                self.metrics.connect_errors.fetch_add(1, Ordering::Relaxed);
                send_error(&mut respond, StatusCode::GATEWAY_TIMEOUT);
                return;
            }
        };
        let response = Response::builder().status(StatusCode::OK).body(()).unwrap();
        let send = match respond.send_response(response, false) {
            Ok(send) => send,
            Err(_) => return,
        };
        self.metrics.connect_total.fetch_add(1, Ordering::Relaxed);
        let _activity = (!passive)
            .then_some(self.activity.as_ref())
            .flatten()
            .map(|tracker| {
                tracker.activate(&target.instance_id);
                ActivityStreamGuard {
                    tracker: tracker.clone(),
                    instance_id: target.instance_id.clone(),
                }
            });
        let mut route_rx = route_rx;
        let relay_stats = RelayStats::default();
        let result = tokio::select! {
            result = relay_h2_tcp_with_stats(request.into_body(), send, tcp, relay_stats.clone()) => Some(result),
            _ = connection_closed.changed() => None,
            _ = async {
                if let Some(receiver) = route_rx.as_mut() {
                    let _ = receiver.changed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => None,
        };
        let (bytes_up, bytes_down) = relay_stats.snapshot();
        self.metrics.bytes_up.fetch_add(bytes_up, Ordering::Relaxed);
        self.metrics
            .bytes_down
            .fetch_add(bytes_down, Ordering::Relaxed);
        if let Some(Err(error)) = result {
            tracing::debug!(
                instance_id = %target.instance_id,
                workload_id = %target.workload_id,
                request_id = %target.request_id,
                bytes_up,
                bytes_down,
                %error,
                "Node Proxy relay closed with an error"
            );
        }
        self.metrics
            .completed_streams
            .fetch_add(1, Ordering::Relaxed);
    }
}

async fn probe_h2_connection(
    mut ping: h2::PingPong,
    interval: Duration,
    response_timeout: Duration,
) -> Result<(), h2::Error> {
    loop {
        tokio::time::sleep(interval).await;
        timeout(response_timeout, ping.ping(h2::Ping::opaque()))
            .await
            .map_err(|_| h2::Reason::SETTINGS_TIMEOUT)??;
    }
}

enum ConnectFailure {
    Io(io::Error),
    Timeout,
}

async fn connect_within_budget(
    address: std::net::SocketAddr,
    budget: Duration,
) -> Result<TcpStream, ConnectFailure> {
    let deadline = tokio::time::Instant::now() + budget;
    let mut last_error = None;
    for delay in [
        Duration::ZERO,
        Duration::from_millis(25),
        Duration::from_millis(50),
    ] {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(ConnectFailure::Timeout);
        }
        match timeout(remaining, TcpStream::connect(address)).await {
            Ok(Ok(stream)) => {
                let _ = stream.set_nodelay(true);
                return Ok(stream);
            }
            Ok(Err(error)) => last_error = Some(error),
            Err(_) => return Err(ConnectFailure::Timeout),
        }
    }
    Err(ConnectFailure::Io(last_error.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::NotConnected, "sandbox TCP connect failed")
    })))
}

struct ActiveStreamGuard(Arc<AtomicUsize>);

impl Drop for ActiveStreamGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

struct ActivityStreamGuard {
    tracker: Arc<ActivityTracker>,
    instance_id: String,
}

impl Drop for ActivityStreamGuard {
    fn drop(&mut self) {
        self.tracker.deactivate(&self.instance_id);
    }
}

fn reserve_stream(counter: &AtomicUsize, max: usize) -> bool {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        if current >= max {
            return false;
        }
        match counter.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(next) => current = next,
        }
    }
}

fn send_error(respond: &mut SendResponse<Bytes>, status: StatusCode) {
    let _ = respond.send_response(Response::builder().status(status).body(()).unwrap(), true);
}

fn status_for_protocol_error(error: &ProtocolError) -> StatusCode {
    match error {
        ProtocolError::InvalidHeader(_) => StatusCode::BAD_REQUEST,
        ProtocolError::TargetOutsideAllowedNetworks | ProtocolError::ReservedTarget => {
            StatusCode::FORBIDDEN
        }
    }
}

pub async fn serve_connection<T>(gateway: NodeProxy, io: T) -> Result<(), h2::Error>
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    gateway.serve_h2(io).await
}

pub fn io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(all(test, feature = "mock-e2e"))]
mod tests;
