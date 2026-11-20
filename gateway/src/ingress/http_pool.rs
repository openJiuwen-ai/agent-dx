use super::pool::RelayReuseGuard;
use bytes::Bytes;
use http::{header, HeaderValue, Request, Response};
use hyper::body::{Body, Frame, Incoming};
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::timeout;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendHttpPoolConfig {
    pub max_connections_per_endpoint: usize,
    pub max_idle_connections: usize,
    pub max_idle_connections_per_endpoint: usize,
    pub idle_timeout: Duration,
    /// Retire a connection after its current response completes, even when it
    /// is continuously reused. This bounds planned Relay drain latency.
    pub max_connection_lifetime: Duration,
    pub acquire_timeout: Duration,
}

impl Default for BackendHttpPoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_endpoint: 64,
            max_idle_connections: 1024,
            max_idle_connections_per_endpoint: 64,
            // An idle HTTP connection is still an open CONNECT stream at the
            // Relay. Keep this deliberately short so pooling cannot pin a
            // environment busy for an unbounded period.
            idle_timeout: Duration::from_secs(5),
            max_connection_lifetime: Duration::from_secs(30),
            acquire_timeout: Duration::from_secs(3),
        }
    }
}

#[derive(Debug, Clone, Eq)]
pub struct BackendHttpPoolKey {
    pub route_revision: u64,
    pub relay_address: String,
    pub instance_id: String,
    pub workload_id: String,
    pub target_ip: std::net::IpAddr,
    pub target_port: u16,
}

impl PartialEq for BackendHttpPoolKey {
    fn eq(&self, other: &Self) -> bool {
        self.route_revision == other.route_revision
            && self.relay_address == other.relay_address
            && self.instance_id == other.instance_id
            && self.workload_id == other.workload_id
            && self.target_ip == other.target_ip
            && self.target_port == other.target_port
    }
}

impl Hash for BackendHttpPoolKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.route_revision.hash(state);
        self.relay_address.hash(state);
        self.instance_id.hash(state);
        self.workload_id.hash(state);
        self.target_ip.hash(state);
        self.target_port.hash(state);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackendHttpPoolMetrics {
    pub idle_connections: usize,
    pub opened_total: u64,
    pub reused_total: u64,
    pub discarded_total: u64,
    pub acquire_timeouts_total: u64,
}

#[derive(Clone)]
pub struct BackendHttpPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    config: BackendHttpPoolConfig,
    endpoints: Mutex<HashMap<BackendHttpPoolKey, Arc<EndpointPool>>>,
    idle_connections: AtomicUsize,
    opened_total: AtomicU64,
    reused_total: AtomicU64,
    discarded_total: AtomicU64,
    acquire_timeouts_total: AtomicU64,
}

struct EndpointPool {
    permits: Arc<Semaphore>,
    idle: Mutex<VecDeque<IdleConnection>>,
    valid: std::sync::atomic::AtomicBool,
}

struct IdleConnection {
    sender: SendRequest<super::server::ProxyBody>,
    idle_since: Instant,
    opened_at: Instant,
    reuse_guard: Option<RelayReuseGuard>,
}

impl BackendHttpPool {
    pub fn new(config: BackendHttpPoolConfig) -> Self {
        assert!(config.max_connections_per_endpoint > 0);
        assert!(config.max_idle_connections_per_endpoint > 0);
        let inner = Arc::new(PoolInner {
            config,
            endpoints: Mutex::new(HashMap::new()),
            idle_connections: AtomicUsize::new(0),
            opened_total: AtomicU64::new(0),
            reused_total: AtomicU64::new(0),
            discarded_total: AtomicU64::new(0),
            acquire_timeouts_total: AtomicU64::new(0),
        });
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let weak = Arc::downgrade(&inner);
            let interval = (inner.config.idle_timeout / 2)
                .clamp(Duration::from_millis(100), Duration::from_secs(30));
            runtime.spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(inner) = weak.upgrade() else {
                        return;
                    };
                    inner.reap_idle();
                }
            });
        }
        Self { inner }
    }

    pub fn metrics(&self) -> BackendHttpPoolMetrics {
        BackendHttpPoolMetrics {
            idle_connections: self.inner.idle_connections.load(Ordering::Relaxed),
            opened_total: self.inner.opened_total.load(Ordering::Relaxed),
            reused_total: self.inner.reused_total.load(Ordering::Relaxed),
            discarded_total: self.inner.discarded_total.load(Ordering::Relaxed),
            acquire_timeouts_total: self.inner.acquire_timeouts_total.load(Ordering::Relaxed),
        }
    }

    pub fn invalidate_instance(&self, instance_id: &str, before_revision: u64) {
        self.inner.remove_matching(|key| {
            key.instance_id == instance_id && key.route_revision < before_revision
        });
    }

    pub fn clear(&self) {
        self.inner.remove_matching(|_| true);
    }

    pub async fn send<F, Fut, T>(
        &self,
        key: BackendHttpPoolKey,
        request: Request<Incoming>,
        open: F,
    ) -> Result<Response<PooledResponseBody>, BackendHttpPoolError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = io::Result<T>>,
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.send_recoverable(key, request, open, || Ok(()))
            .await
            .map_err(|(error, _)| error)
    }

    /// Return ownership only if no business request has been sent. Callers may safely re-resolve
    /// a target in that case, but may never replay an ambiguous send_request failure.
    /// Validate admission after all pool/connection waits, immediately before sending.
    pub async fn send_recoverable<F, Fut, T, V>(
        &self,
        key: BackendHttpPoolKey,
        request: Request<Incoming>,
        open: F,
        validate: V,
    ) -> Result<Response<PooledResponseBody>, (BackendHttpPoolError, Option<Request<Incoming>>)>
    where
        F: FnOnce() -> Fut,
        V: FnOnce() -> io::Result<()>,
        Fut: Future<Output = io::Result<T>>,
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.send_guarded_recoverable(key, request, open, validate, |_| None, None)
            .await
    }

    pub(crate) async fn send_relay_recoverable<F, Fut, V>(
        &self,
        key: BackendHttpPoolKey,
        request: Request<Incoming>,
        prepare_deadline: Option<tokio::time::Instant>,
        open: F,
        validate: V,
    ) -> Result<Response<PooledResponseBody>, (BackendHttpPoolError, Option<Request<Incoming>>)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = io::Result<super::server::IngressStream>>,
        V: FnOnce() -> io::Result<()>,
    {
        self.send_guarded_recoverable(
            key,
            request,
            open,
            validate,
            super::server::IngressStream::reuse_guard,
            prepare_deadline,
        )
        .await
    }

    async fn send_guarded_recoverable<F, Fut, T, V, G>(
        &self,
        key: BackendHttpPoolKey,
        mut request: Request<Incoming>,
        open: F,
        validate: V,
        guard: G,
        prepare_deadline: Option<tokio::time::Instant>,
    ) -> Result<Response<PooledResponseBody>, (BackendHttpPoolError, Option<Request<Incoming>>)>
    where
        F: FnOnce() -> Fut,
        V: FnOnce() -> io::Result<()>,
        Fut: Future<Output = io::Result<T>>,
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        G: FnOnce(&T) -> Option<RelayReuseGuard>,
    {
        // The pool always opens HTTP/1.1 connections to the sandbox. An
        // HTTP/2 client request therefore must not retain its inbound version
        // when Hyper serializes it on that backend connection.
        normalize_http1_request(&mut request);
        let prepared = async {
            let endpoint = self.inner.endpoint(&key);
            let permit = timeout(
                self.inner.config.acquire_timeout,
                endpoint.permits.clone().acquire_owned(),
            )
            .await
            .map_err(|_| {
                self.inner
                    .acquire_timeouts_total
                    .fetch_add(1, Ordering::Relaxed);
                BackendHttpPoolError::Saturated
            })?
            .map_err(|_| BackendHttpPoolError::Closed)?;
            if !endpoint.valid.load(Ordering::Acquire) {
                return Err(BackendHttpPoolError::Closed);
            }

            let (sender, opened_at, reused, reuse_guard) = loop {
                let Some(mut idle) = self.inner.take_idle(&endpoint) else {
                    let stream = open().await.map_err(BackendHttpPoolError::Connect)?;
                    let reuse_guard = guard(&stream);
                    let (sender, connection) =
                        hyper::client::conn::http1::handshake(TokioIo::new(stream))
                            .await
                            .map_err(BackendHttpPoolError::Handshake)?;
                    self.inner.opened_total.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(adx_observability::trace::with_current_context(async move {
                        if let Err(error) = connection.await {
                            tracing::debug!(%error, "pooled environment HTTP connection closed");
                        }
                    }));
                    break (sender, Instant::now(), false, reuse_guard);
                };
                if idle.sender.ready().await.is_ok() && relay_reusable(&idle.reuse_guard) {
                    break (idle.sender, idle.opened_at, true, idle.reuse_guard);
                }
                self.inner.discarded_total.fetch_add(1, Ordering::Relaxed);
            };

            Ok::<_, BackendHttpPoolError>((
                sender,
                opened_at,
                endpoint,
                permit,
                reused,
                reuse_guard,
            ))
        };
        // Bound preparation without cancelling a business request after it has
        // been sent. Timeouts here retain the complete original Incoming body.
        let prepared = match prepare_deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, prepared)
                .await
                .unwrap_or_else(|_| {
                    Err(BackendHttpPoolError::Connect(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "Relay connection admission timed out",
                    )))
                }),
            None => prepared.await,
        };
        let (mut sender, opened_at, endpoint, permit, reused, reuse_guard) = match prepared {
            Ok(value) => value,
            Err(error) => return Err((error, Some(request))),
        };
        if !endpoint.valid.load(Ordering::Acquire) {
            return Err((BackendHttpPoolError::Closed, Some(request)));
        }
        if let Err(error) = validate() {
            return Err((BackendHttpPoolError::Connect(error), Some(request)));
        }
        let request_allows_reuse = !has_connection_close(request.headers());
        // No business bytes or forwarded marker have been emitted. A GOAWAY
        // received during admission validation can still return the full request.
        if !relay_reusable(&reuse_guard) {
            self.inner.discarded_total.fetch_add(1, Ordering::Relaxed);
            return Err((BackendHttpPoolError::Closed, Some(request)));
        }
        if reused {
            self.inner.reused_total.fetch_add(1, Ordering::Relaxed);
        }
        if let Some(processing) = request
            .extensions()
            .get::<super::processing::ProcessingDeadline>()
        {
            processing.forwarded();
        }
        let response = sender
            .send_request(super::processing::forwarding_request(request))
            .await
            .map_err(|error| (BackendHttpPoolError::Request(error), None))?;
        let response_allows_reuse = !has_connection_close(response.headers());
        let reusable = request_allows_reuse && response_allows_reuse;
        let lease = ConnectionLease {
            sender: Some(sender),
            reuse_guard,
            opened_at,
            endpoint,
            pool: Arc::downgrade(&self.inner),
            permit: Some(permit),
            reusable,
        };
        let (parts, incoming) = response.into_parts();
        let mut body = PooledResponseBody {
            inner: incoming,
            lease: Some(lease),
            complete: false,
        };
        if body.inner.is_end_stream() {
            body.complete = true;
            body.release();
        }
        Ok(Response::from_parts(parts, body))
    }
}

impl PoolInner {
    fn endpoint(&self, key: &BackendHttpPoolKey) -> Arc<EndpointPool> {
        self.endpoints
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(EndpointPool {
                    permits: Arc::new(Semaphore::new(self.config.max_connections_per_endpoint)),
                    idle: Mutex::new(VecDeque::new()),
                    valid: std::sync::atomic::AtomicBool::new(true),
                })
            })
            .clone()
    }

    fn take_idle(&self, endpoint: &EndpointPool) -> Option<IdleConnection> {
        let now = Instant::now();
        let mut idle = endpoint.idle.lock().unwrap();
        if !endpoint.valid.load(Ordering::Acquire) {
            return None;
        }
        while let Some(connection) = idle.pop_back() {
            self.idle_connections.fetch_sub(1, Ordering::Relaxed);
            if now.duration_since(connection.idle_since) <= self.config.idle_timeout
                && now.duration_since(connection.opened_at) < self.config.max_connection_lifetime
                && !connection.sender.is_closed()
                && relay_reusable(&connection.reuse_guard)
            {
                return Some(connection);
            }
            self.discarded_total.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    fn return_idle(
        &self,
        endpoint: &EndpointPool,
        sender: SendRequest<super::server::ProxyBody>,
        opened_at: Instant,
        reuse_guard: Option<RelayReuseGuard>,
    ) {
        // Serialize the validity check and insertion with invalidation's queue drain.
        let mut idle = endpoint.idle.lock().unwrap();
        if !endpoint.valid.load(Ordering::Acquire)
            || sender.is_closed()
            || opened_at.elapsed() >= self.config.max_connection_lifetime
            || !relay_reusable(&reuse_guard)
        {
            self.discarded_total.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if idle.len() >= self.config.max_idle_connections_per_endpoint
            || self
                .idle_connections
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    (current < self.config.max_idle_connections).then_some(current + 1)
                })
                .is_err()
        {
            self.discarded_total.fetch_add(1, Ordering::Relaxed);
            return;
        }
        idle.push_back(IdleConnection {
            sender,
            reuse_guard,
            idle_since: Instant::now(),
            opened_at,
        });
    }

    fn reap_idle(&self) {
        let now = Instant::now();
        let mut endpoints = self.endpoints.lock().unwrap();
        endpoints.retain(|_, endpoint| {
            let mut idle = endpoint.idle.lock().unwrap();
            let before = idle.len();
            idle.retain(|connection| {
                now.duration_since(connection.idle_since) <= self.config.idle_timeout
                    && now.duration_since(connection.opened_at)
                        < self.config.max_connection_lifetime
                    && !connection.sender.is_closed()
                    && relay_reusable(&connection.reuse_guard)
            });
            let removed = before - idle.len();
            if removed > 0 {
                self.idle_connections.fetch_sub(removed, Ordering::Relaxed);
                self.discarded_total
                    .fetch_add(removed as u64, Ordering::Relaxed);
            }
            let keep = !(idle.is_empty()
                && endpoint.permits.available_permits()
                    == self.config.max_connections_per_endpoint);
            if !keep {
                endpoint.valid.store(false, Ordering::Release);
                endpoint.permits.close();
            }
            keep
        });
    }

    fn remove_matching(&self, predicate: impl Fn(&BackendHttpPoolKey) -> bool) {
        let mut endpoints = self.endpoints.lock().unwrap();
        endpoints.retain(|key, endpoint| {
            if !predicate(key) {
                return true;
            }
            let mut idle = endpoint.idle.lock().unwrap();
            endpoint.valid.store(false, Ordering::Release);
            endpoint.permits.close();
            let removed = idle.len();
            idle.clear();
            if removed > 0 {
                self.idle_connections.fetch_sub(removed, Ordering::Relaxed);
                self.discarded_total
                    .fetch_add(removed as u64, Ordering::Relaxed);
            }
            false
        });
    }
}

struct ConnectionLease {
    sender: Option<SendRequest<super::server::ProxyBody>>,
    reuse_guard: Option<RelayReuseGuard>,
    opened_at: Instant,
    endpoint: Arc<EndpointPool>,
    pool: std::sync::Weak<PoolInner>,
    permit: Option<OwnedSemaphorePermit>,
    reusable: bool,
}

impl ConnectionLease {
    fn finish(mut self, body_complete: bool) {
        if body_complete && self.reusable {
            if let (Some(pool), Some(sender)) = (self.pool.upgrade(), self.sender.take()) {
                pool.return_idle(
                    &self.endpoint,
                    sender,
                    self.opened_at,
                    self.reuse_guard.take(),
                );
            }
        } else if let Some(pool) = self.pool.upgrade() {
            pool.discarded_total.fetch_add(1, Ordering::Relaxed);
        }
        self.permit.take();
    }
}

pub struct PooledResponseBody {
    inner: Incoming,
    lease: Option<ConnectionLease>,
    complete: bool,
}

impl PooledResponseBody {
    fn release(&mut self) {
        if let Some(lease) = self.lease.take() {
            lease.finish(self.complete);
        }
    }
}

impl Drop for PooledResponseBody {
    fn drop(&mut self) {
        self.release();
    }
}

impl Body for PooledResponseBody {
    type Data = Bytes;
    type Error = hyper::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match Pin::new(&mut self.inner).poll_frame(context) {
            Poll::Ready(None) => {
                self.complete = true;
                self.release();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                self.release();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(frame))) => {
                if self.inner.is_end_stream() {
                    self.complete = true;
                    self.release();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BackendHttpPoolError {
    #[error("environment HTTP pool is saturated")]
    Saturated,
    #[error("environment HTTP pool is closed")]
    Closed,
    #[error("open environment stream: {0}")]
    Connect(io::Error),
    #[error("environment HTTP handshake: {0}")]
    Handshake(hyper::Error),
    #[error("environment HTTP request: {0}")]
    Request(hyper::Error),
}

fn relay_reusable(guard: &Option<RelayReuseGuard>) -> bool {
    guard.as_ref().is_none_or(RelayReuseGuard::is_reusable)
}

fn has_connection_close(headers: &http::HeaderMap) -> bool {
    headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|token| token.trim().eq_ignore_ascii_case("close"))
}

fn normalize_http1_request<B>(request: &mut Request<B>) {
    if !request.headers().contains_key(header::HOST) {
        let authority = request
            .uri()
            .authority()
            .map(|value| value.as_str().to_owned());
        if let Some(authority) = authority {
            if let Ok(host) = HeaderValue::from_str(&authority) {
                request.headers_mut().insert(header::HOST, host);
            }
        }
    }
    // A Hyper HTTP/1 client serializes an absolute URI in absolute-form. The
    // backend is an application server, not a proxy, so all non-CONNECT
    // requests must use origin-form. CONNECT deliberately retains authority-
    // form; OPTIONS `*` is already a valid path-and-query and is preserved.
    if request.method() != http::Method::CONNECT {
        let mut parts = request.uri().clone().into_parts();
        parts.scheme = None;
        parts.authority = None;
        if parts.path_and_query.is_none() {
            parts.path_and_query = Some(http::uri::PathAndQuery::from_static("/"));
        }
        // The retained path-and-query came from an already-valid URI, so
        // clearing scheme and authority cannot make the origin URI invalid.
        if let Ok(origin) = http::Uri::from_parts(parts) {
            *request.uri_mut() = origin;
        }
    }
    *request.version_mut() = http::Version::HTTP_11;
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    use http_body_util::{BodyExt, Full};
    use hyper::service::service_fn;
    use std::convert::Infallible;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // A real h2 receiver drives GOAWAY into the exact sender used by the guard.
    async fn physical_guard() -> (
        super::super::pool::RelayReuseGuard,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let (client, server) = tokio::io::duplex(8192);
        let (drain_tx, drain_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let mut connection = h2::server::handshake(server).await.unwrap();
            tokio::pin!(drain_rx);
            tokio::select! {
                _ = &mut drain_rx => connection.graceful_shutdown(),
                _ = connection.accept() => panic!("unexpected stream"),
            }
            while connection.accept().await.is_some() {}
        });
        let (sender, connection) = h2::client::handshake(client).await.unwrap();
        let client = tokio::spawn(async move {
            let _ = connection.await;
        });
        (
            super::super::pool::RelayReuseGuard::new(sender),
            drain_tx,
            server,
            client,
        )
    }

    #[tokio::test]
    async fn goaway_reselects_http_connection_and_never_replays_a_lost_response() {
        exercise_reselection(false).await;
    }

    #[tokio::test]
    async fn reaper_discards_goaway_idle_connections_without_waiting_for_idle_timeout() {
        exercise_reselection(true).await;
    }

    async fn exercise_reselection(reap: bool) {
        let (guard, drain_tx, h2_server, h2_client) = physical_guard().await;
        let observed_guard = guard.clone();
        let pool = BackendHttpPool::new(BackendHttpPoolConfig {
            idle_timeout: Duration::from_secs(600),
            max_connection_lifetime: Duration::from_secs(600),
            ..BackendHttpPoolConfig::default()
        });
        let (front, incoming) = tokio::io::duplex(8192);
        let requests = Arc::new(AtomicUsize::new(0));
        let opens = Arc::new(AtomicUsize::new(0));
        let service_pool = pool.clone();
        let service_opens = opens.clone();
        let service_requests = requests.clone();
        let task = tokio::spawn(async move {
            hyper::server::conn::http1::Builder::new().serve_connection(
                TokioIo::new(incoming), service_fn(move |request| {
                    let pool = service_pool.clone();
                    let opens = service_opens.clone();
                    let requests = service_requests.clone();
                    let guard = guard.clone();
                    async move {
                        let key = BackendHttpPoolKey {
                            route_revision: 1, relay_address: "relay".into(),
                            instance_id: "instance".into(), workload_id: "workload".into(),
                            target_ip: "127.0.0.1".parse().unwrap(), target_port: 8080,
                        };
                        let ordinal = requests.fetch_add(1, Ordering::SeqCst);
                        let result = pool.send_guarded_recoverable(key, request, || async move {
                            opens.fetch_add(1, Ordering::SeqCst);
                            let (stream, mut backend) = tokio::io::duplex(8192);
                            tokio::spawn(async move {
                                loop {
                                    let mut bytes = Vec::new();
                                    while !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                                        let mut byte = [0];
                                        if backend.read_exact(&mut byte).await.is_err() { return; }
                                        bytes.push(byte[0]);
                                    }
                                    if ordinal > 0 { return; } // Lost response, after sending.
                                    backend.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n").await.unwrap();
                                }
                            });
                            Ok(stream)
                        }, || Ok(()), move |_| (ordinal == 0).then_some(guard), None).await;
                        match (ordinal, result) {
                            (0, Ok(response)) => { response.into_body().collect().await.unwrap(); }
                            (1, Err((BackendHttpPoolError::Request(_), None))) => {}
                            _ => panic!("unexpected result or replay eligibility"),
                        }
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::new())))
                    }
                })
            ).await.unwrap();
        });
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(front))
            .await
            .unwrap();
        let connection = tokio::spawn(connection);
        sender
            .send_request(Request::new(Full::new(Bytes::new())))
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap();
        assert_eq!(pool.metrics().idle_connections, 1);
        drain_tx.send(()).unwrap();
        // Wait on actual received GOAWAY, not elapsed time or port reachability.
        timeout(Duration::from_secs(3), async {
            while observed_guard.is_reusable() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if reap {
            pool.inner.reap_idle();
            assert_eq!(pool.metrics().idle_connections, 0);
        }
        sender.ready().await.unwrap();
        sender
            .send_request(Request::new(Full::new(Bytes::new())))
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap();
        assert_eq!(opens.load(Ordering::SeqCst), 2);
        assert_eq!(requests.load(Ordering::SeqCst), 2);
        assert_eq!(pool.metrics().reused_total, 0);
        connection.abort();
        task.abort();
        h2_server.abort();
        h2_client.abort();
    }

    async fn exercise(
        fail_before_send: bool,
        reject_admission: bool,
        goaway_before_send: bool,
        prepare_timeout: bool,
    ) {
        let pool = BackendHttpPool::new(BackendHttpPoolConfig::default());
        let (guard, drain_tx, h2_server, h2_client) = physical_guard().await;
        if goaway_before_send {
            drain_tx.send(()).unwrap();
            timeout(Duration::from_secs(3), async {
                while guard.is_reusable() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        let (front, incoming) = tokio::io::duplex(8192);
        let task = tokio::spawn(async move {
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    TokioIo::new(incoming),
                    service_fn(move |request| {
                        let pool = pool.clone();
                        let guard = guard.clone();
                        async move {
                            let key = BackendHttpPoolKey {
                                route_revision: 1,
                                relay_address: "unused".into(),
                                instance_id: "instance".into(),
                                workload_id: "workload".into(),
                                target_ip: "127.0.0.1".parse().unwrap(),
                                target_port: 8080,
                            };
                            let endpoint = pool.inner.endpoint(&key);
                            let _held = if prepare_timeout {
                                Some(
                                    endpoint
                                        .permits
                                        .clone()
                                        .acquire_many_owned(
                                            u32::try_from(endpoint.permits.available_permits())
                                                .unwrap(),
                                        )
                                        .await
                                        .unwrap(),
                                )
                            } else {
                                None
                            };
                            let (stream, mut peer) = tokio::io::duplex(8192);
                            let received = tokio::spawn(async move {
                                if fail_before_send || reject_admission || prepare_timeout {
                                    return Vec::new();
                                }
                                if goaway_before_send {
                                    let mut all = Vec::new();
                                    peer.read_to_end(&mut all).await.unwrap();
                                    return all;
                                }
                                let mut all = Vec::new();
                                let mut chunk = [0u8; 512];
                                while !all
                                    .windows(b"one-business-body".len())
                                    .any(|w| w == b"one-business-body")
                                {
                                    let n = peer.read(&mut chunk).await.unwrap();
                                    assert!(n > 0);
                                    all.extend_from_slice(&chunk[..n]);
                                }
                                // Drop after consuming the body, simulating a lost response.
                                all
                            });
                            let result = pool
                                .send_guarded_recoverable(
                                    key,
                                    request,
                                    || async move {
                                        assert!(
                                            !prepare_timeout,
                                            "a saturated pool must time out before opening"
                                        );
                                        if fail_before_send {
                                            Err(io::Error::other("connect failed"))
                                        } else {
                                            Ok(stream)
                                        }
                                    },
                                    || {
                                        if reject_admission {
                                            Err(io::Error::other("admission revoked"))
                                        } else {
                                            Ok(())
                                        }
                                    },
                                    move |_| Some(guard),
                                    prepare_timeout.then(|| {
                                        tokio::time::Instant::now() + Duration::from_millis(20)
                                    }),
                                )
                                .await;
                            match result {
                                Err((BackendHttpPoolError::Connect(error), Some(request)))
                                    if fail_before_send || reject_admission || prepare_timeout =>
                                {
                                    if prepare_timeout {
                                        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
                                    }
                                    assert_eq!(
                                        request.into_body().collect().await.unwrap().to_bytes(),
                                        Bytes::from_static(b"one-business-body")
                                    );
                                }
                                Err((BackendHttpPoolError::Closed, Some(request)))
                                    if goaway_before_send =>
                                {
                                    assert_eq!(
                                        request.into_body().collect().await.unwrap().to_bytes(),
                                        Bytes::from_static(b"one-business-body")
                                    );
                                }
                                Err((BackendHttpPoolError::Request(_), None))
                                    if !fail_before_send
                                        && !reject_admission
                                        && !goaway_before_send => {}
                                _ => panic!("incorrect replay eligibility"),
                            }
                            let bytes = received.await.unwrap();
                            if goaway_before_send {
                                assert!(bytes.is_empty(), "business bytes sent after GOAWAY");
                            }
                            if !fail_before_send
                                && !reject_admission
                                && !goaway_before_send
                                && !prepare_timeout
                            {
                                assert!(bytes.ends_with(b"one-business-body"));
                            }
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::new())))
                        }
                    }),
                )
                .await
                .unwrap();
        });
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(front))
            .await
            .unwrap();
        let connection = tokio::spawn(connection);
        let request = Request::builder()
            .method("POST")
            .uri("/business")
            .body(Full::new(Bytes::from_static(b"one-business-body")))
            .unwrap();
        let response = tokio::time::timeout(Duration::from_secs(3), sender.send_request(request))
            .await
            .unwrap()
            .unwrap();
        response.into_body().collect().await.unwrap();
        drop(sender);
        connection.abort();
        task.abort();
        h2_server.abort();
        h2_client.abort();
    }
    #[tokio::test]
    async fn connection_failure_returns_the_unconsumed_business_body() {
        exercise(true, false, false, false).await;
    }
    #[tokio::test]
    async fn lost_response_never_returns_a_replayable_business_body() {
        exercise(false, false, false, false).await;
    }

    #[tokio::test]
    async fn revoked_admission_keeps_the_business_body_unsent() {
        exercise(false, true, false, false).await;
    }

    #[tokio::test]
    async fn goaway_at_final_check_returns_the_entire_unsent_body() {
        exercise(false, false, true, false).await;
    }

    #[tokio::test]
    async fn preparation_timeout_while_waiting_for_pool_permit_preserves_the_unsent_body() {
        exercise(false, false, false, true).await;
    }

    #[test]
    fn http2_authority_becomes_host_for_the_http1_backend_pool() {
        let mut request = Request::builder()
            .version(http::Version::HTTP_2)
            .uri("https://sdk.example.test:443/envd.Process/Start")
            .body(())
            .unwrap();
        normalize_http1_request(&mut request);
        assert_eq!(request.version(), http::Version::HTTP_11);
        assert_eq!(
            request.headers().get(header::HOST).unwrap(),
            "sdk.example.test:443"
        );
    }

    #[test]
    fn existing_http1_host_is_preserved() {
        let mut request = Request::builder()
            .version(http::Version::HTTP_11)
            .uri("http://upstream.example.test/path")
            .header(header::HOST, "sdk.example.test")
            .body(())
            .unwrap();
        normalize_http1_request(&mut request);
        assert_eq!(
            request.headers().get(header::HOST).unwrap(),
            "sdk.example.test"
        );
    }

    #[test]
    fn connect_retains_authority_form() {
        let authority = http::Uri::builder()
            .authority("sandbox.example.test:443")
            .build()
            .unwrap();
        let mut request = Request::builder()
            .method(http::Method::CONNECT)
            .version(http::Version::HTTP_2)
            .uri(authority)
            .body(())
            .unwrap();
        normalize_http1_request(&mut request);
        assert_eq!(request.uri(), "sandbox.example.test:443");
    }

    #[test]
    fn options_asterisk_is_preserved() {
        let mut request = Request::builder()
            .method(http::Method::OPTIONS)
            .version(http::Version::HTTP_2)
            .uri("*")
            .body(())
            .unwrap();
        normalize_http1_request(&mut request);
        assert_eq!(request.uri(), "*");
    }

    #[tokio::test]
    async fn http2_absolute_uri_serializes_as_origin_form_on_the_http1_backend() {
        let (client, mut backend) = tokio::io::duplex(4096);
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(client))
            .await
            .unwrap();
        let connection = tokio::spawn(connection);
        let received = tokio::spawn(async move {
            let mut bytes = Vec::new();
            let mut chunk = [0_u8; 512];
            while !bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                let count = backend.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&chunk[..count]);
            }
            backend
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            bytes
        });
        let mut request = Request::builder()
            .method("GET")
            .version(http::Version::HTTP_2)
            .uri("https://localhost:8443/request-form-probe?source=sdk")
            .body(http_body_util::Empty::<Bytes>::new())
            .unwrap();
        normalize_http1_request(&mut request);
        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        let request = received.await.unwrap();
        assert!(request.starts_with(b"GET /request-form-probe?source=sdk HTTP/1.1\r\n"));
        assert!(request
            .windows(b"host: localhost:8443\r\n".len())
            .any(|window| { window.eq_ignore_ascii_case(b"host: localhost:8443\r\n") }));
        drop(sender);
        connection.abort();
    }
}
