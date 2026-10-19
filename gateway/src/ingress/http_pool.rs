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
            acquire_timeout: Duration::from_secs(3),
        }
    }
}

#[derive(Debug, Clone, Eq)]
pub struct BackendHttpPoolKey {
    pub relay_address: String,
    pub instance_id: String,
    pub workload_id: String,
    pub target_ip: std::net::IpAddr,
    pub target_port: u16,
}

impl PartialEq for BackendHttpPoolKey {
    fn eq(&self, other: &Self) -> bool {
        self.relay_address == other.relay_address
            && self.instance_id == other.instance_id
            && self.workload_id == other.workload_id
            && self.target_ip == other.target_ip
            && self.target_port == other.target_port
    }
}

impl Hash for BackendHttpPoolKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
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
    sender: SendRequest<Incoming>,
    idle_since: Instant,
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
            let interval = (inner.config.idle_timeout / 2).max(Duration::from_millis(100));
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

    pub fn invalidate_instance(&self, instance_id: &str) {
        self.inner
            .remove_matching(|key| key.instance_id == instance_id);
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
        self.send_recoverable(key, request, open)
            .await
            .map_err(|(error, _)| error)
    }

    /// Return ownership only if no business request has been sent. Callers may safely re-resolve
    /// a target in that case, but may never replay an ambiguous send_request failure.
    pub async fn send_recoverable<F, Fut, T>(
        &self,
        key: BackendHttpPoolKey,
        mut request: Request<Incoming>,
        open: F,
    ) -> Result<Response<PooledResponseBody>, (BackendHttpPoolError, Option<Request<Incoming>>)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = io::Result<T>>,
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
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

            let sender = loop {
                let Some(mut idle) = self.inner.take_idle(&endpoint) else {
                    let stream = open().await.map_err(BackendHttpPoolError::Connect)?;
                    let (sender, connection) =
                        hyper::client::conn::http1::handshake(TokioIo::new(stream))
                            .await
                            .map_err(BackendHttpPoolError::Handshake)?;
                    self.inner.opened_total.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(async move {
                        if let Err(error) = connection.await {
                            tracing::debug!(%error, "pooled environment HTTP connection closed");
                        }
                    });
                    break sender;
                };
                if idle.sender.ready().await.is_ok() {
                    self.inner.reused_total.fetch_add(1, Ordering::Relaxed);
                    break idle.sender;
                }
                self.inner.discarded_total.fetch_add(1, Ordering::Relaxed);
            };

            Ok::<_, BackendHttpPoolError>((sender, endpoint, permit))
        }
        .await;
        let (mut sender, endpoint, permit) = match prepared {
            Ok(value) => value,
            Err(error) => return Err((error, Some(request))),
        };
        let request_allows_reuse = !has_connection_close(request.headers());
        if let Some(processing) = request
            .extensions()
            .get::<super::processing::ProcessingDeadline>()
        {
            processing.forwarded();
        }
        let response = sender
            .send_request(request)
            .await
            .map_err(|error| (BackendHttpPoolError::Request(error), None))?;
        let response_allows_reuse = !has_connection_close(response.headers());
        let reusable = request_allows_reuse && response_allows_reuse;
        let lease = ConnectionLease {
            sender: Some(sender),
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
        while let Some(connection) = idle.pop_back() {
            self.idle_connections.fetch_sub(1, Ordering::Relaxed);
            if now.duration_since(connection.idle_since) <= self.config.idle_timeout
                && !connection.sender.is_closed()
            {
                return Some(connection);
            }
            self.discarded_total.fetch_add(1, Ordering::Relaxed);
        }
        None
    }

    fn return_idle(&self, endpoint: &EndpointPool, sender: SendRequest<Incoming>) {
        if !endpoint.valid.load(Ordering::Acquire) || sender.is_closed() {
            self.discarded_total.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut idle = endpoint.idle.lock().unwrap();
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
            idle_since: Instant::now(),
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
                    && !connection.sender.is_closed()
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
            endpoint.valid.store(false, Ordering::Release);
            let removed = endpoint.idle.lock().unwrap().len();
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
    sender: Option<SendRequest<Incoming>>,
    endpoint: Arc<EndpointPool>,
    pool: std::sync::Weak<PoolInner>,
    permit: Option<OwnedSemaphorePermit>,
    reusable: bool,
}

impl ConnectionLease {
    fn finish(mut self, body_complete: bool) {
        if body_complete && self.reusable {
            if let (Some(pool), Some(sender)) = (self.pool.upgrade(), self.sender.take()) {
                pool.return_idle(&self.endpoint, sender);
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

    async fn exercise(fail_before_send: bool) {
        let pool = BackendHttpPool::new(BackendHttpPoolConfig::default());
        let (front, incoming) = tokio::io::duplex(8192);
        let task = tokio::spawn(async move {
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    TokioIo::new(incoming),
                    service_fn(move |request| {
                        let pool = pool.clone();
                        async move {
                            let key = BackendHttpPoolKey {
                                relay_address: "unused".into(),
                                instance_id: "instance".into(),
                                workload_id: "workload".into(),
                                target_ip: "127.0.0.1".parse().unwrap(),
                                target_port: 8080,
                            };
                            let (stream, mut peer) = tokio::io::duplex(8192);
                            let received = tokio::spawn(async move {
                                if fail_before_send {
                                    return Vec::new();
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
                                .send_recoverable(key, request, || async move {
                                    if fail_before_send {
                                        Err(io::Error::other("connect failed"))
                                    } else {
                                        Ok(stream)
                                    }
                                })
                                .await;
                            match result {
                                Err((BackendHttpPoolError::Connect(_), Some(request)))
                                    if fail_before_send =>
                                {
                                    assert_eq!(
                                        request.into_body().collect().await.unwrap().to_bytes(),
                                        Bytes::from_static(b"one-business-body")
                                    );
                                }
                                Err((BackendHttpPoolError::Request(_), None))
                                    if !fail_before_send => {}
                                _ => panic!("incorrect replay eligibility"),
                            }
                            let bytes = received.await.unwrap();
                            if !fail_before_send {
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
    }
    #[tokio::test]
    async fn connection_failure_returns_the_unconsumed_business_body() {
        exercise(true).await;
    }
    #[tokio::test]
    async fn lost_response_never_returns_a_replayable_business_body() {
        exercise(false).await;
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
