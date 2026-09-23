use bytes::Bytes;
use http::{header, Request, Response};
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
            // sandbox busy for an unbounded period.
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
        request: Request<Incoming>,
        open: F,
    ) -> Result<Response<PooledResponseBody>, (BackendHttpPoolError, Option<Request<Incoming>>)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = io::Result<T>>,
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
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
                            tracing::debug!(%error, "pooled sandbox HTTP connection closed");
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
    #[error("sandbox HTTP pool is saturated")]
    Saturated,
    #[error("sandbox HTTP pool is closed")]
    Closed,
    #[error("open sandbox stream: {0}")]
    Connect(io::Error),
    #[error("sandbox HTTP handshake: {0}")]
    Handshake(hyper::Error),
    #[error("sandbox HTTP request: {0}")]
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

#[cfg(test)]
mod retry_tests {
    use super::*;
    use http_body_util::{BodyExt, Full};
    use hyper::service::service_fn;
    use std::convert::Infallible;
    use tokio::io::AsyncReadExt;

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
}
