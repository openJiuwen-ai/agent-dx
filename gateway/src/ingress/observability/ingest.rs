//! 观测接收端点：`POST /internal/adx/observability/v1/events`。
//! - 独立私网监听（默认关），复用既有 `/metrics` 的安全模型区隔：事件能改写
//!   业务成功率的分子分母，必须服务凭据 + producer 绑定，不是无鉴权的 Pull 口。
//! - 整批原子：1–500 条、≤1 MiB，任意一条校验失败整批拒绝（不做部分成功，
//!   避免生产者无法判定重试范围）；重复事件返回 202 + `result=duplicate`。
//! - 存储不可用时先写有界内存 spool；spool 满返回 503（软依赖，不阻塞业务）。
//! - 接收路径不调用任何业务/平台 RPC。
use super::{Error, ObservationStore};
use adx_observability::business::{BusinessObservation, ReceivedObservation};
use async_trait::async_trait;
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    body::Incoming, header, server::conn::http1, service::service_fn, Method, Request, Response,
    StatusCode,
};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    convert::Infallible,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};
use time::OffsetDateTime;
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch, OwnedSemaphorePermit, Semaphore},
};

/// 唯一接收路径（§6.8）。
pub const INGEST_PATH: &str = "/internal/adx/observability/v1/events";

/// spool 队列条目：一批待落库事件 + 覆盖"排队 + 处理中"的容量许可。
pub type SpoolBatch = (Vec<ReceivedObservation>, OwnedSemaphorePermit);
/// 单请求最大事件数（§6.8）。
pub const DEFAULT_MAX_BATCH: usize = 500;
/// 单请求最大字节数（§6.8）。
pub const DEFAULT_MAX_BYTES: usize = 1024 * 1024;
/// 每个 producer 的默认速率上限（§5.5.4 规则 7）。
pub const DEFAULT_EVENTS_PER_SECOND: u64 = 2_000;
/// spool 默认容量（批数）；满即 503。
pub const DEFAULT_SPOOL_CAPACITY: usize = 64;
/// spool 重试间隔（存储不可用时的退避起点）。
const RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);
/// 关停时等待 spool 排空的上限。
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// 接收端点配置（§6.8）。
#[derive(Debug, Clone, Deserialize)]
pub struct IngestConfig {
    /// 私网监听地址（回环或私网网段，否则拒启动），示例 `127.0.0.1:19094`。
    pub listen: SocketAddr,
    /// `producer → 租户/业务类型` 绑定；空列表拒启动。
    pub bindings: Vec<ProducerBinding>,
    #[serde(default = "default_max_batch")]
    pub max_batch: usize,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
    #[serde(default = "default_events_per_second")]
    pub events_per_second: u64,
    #[serde(default = "default_spool_capacity")]
    pub spool_capacity: usize,
}

fn default_max_batch() -> usize {
    DEFAULT_MAX_BATCH
}

fn default_max_bytes() -> usize {
    DEFAULT_MAX_BYTES
}

fn default_events_per_second() -> u64 {
    DEFAULT_EVENTS_PER_SECOND
}

fn default_spool_capacity() -> usize {
    DEFAULT_SPOOL_CAPACITY
}

/// 一个生产者绑定：token 即服务凭据（producerId 不由客户端自报，D6）。
#[derive(Clone, Deserialize)]
pub struct ProducerBinding {
    pub producer_id: String,
    /// 服务 token 原文；比较时双方先做 SHA-256，避免长度/内容时序泄漏。
    pub token: String,
    /// 允许的租户引用集合（非空）。
    pub tenant_scope: Vec<String>,
    /// 允许的业务类型集合（非空）。
    pub business_types: Vec<adx_observability::business::BusinessType>,
}

/// 手写 `Debug`：`token` 是生产者凭据，配置打印不得带出明文。
impl std::fmt::Debug for ProducerBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProducerBinding")
            .field("producer_id", &self.producer_id)
            .field("token", &"<redacted>")
            .field("tenant_scope", &self.tenant_scope)
            .field("business_types", &self.business_types)
            .finish()
    }
}

impl IngestConfig {
    /// 启动前校验：只绑私网地址、绑定与上限非空。
    pub fn validate(&self) -> Result<(), Error> {
        let private = match self.listen.ip() {
            std::net::IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
            std::net::IpAddr::V6(ip) => ip.is_loopback(),
        };
        if !private {
            return Err(Error::Configuration);
        }
        if self.bindings.is_empty()
            || self.max_batch == 0
            || self.max_bytes == 0
            || self.events_per_second == 0
            || self.spool_capacity == 0
        {
            return Err(Error::Configuration);
        }
        for binding in &self.bindings {
            if binding.producer_id.trim().is_empty()
                || binding.token.is_empty()
                || binding.tenant_scope.is_empty()
                || binding.business_types.is_empty()
            {
                return Err(Error::Configuration);
            }
        }
        Ok(())
    }
}

/// 存储接口：真实实现是 [`ObservationStore`]，测试用假实现覆盖 503 路径。
#[async_trait]
pub trait EventStore: Send + Sync {
    async fn insert_batch(
        &self,
        batch: &[ReceivedObservation],
    ) -> Result<Vec<super::store::EventInsert>, Error>;

    /// 落一条拒收诊断（M-A11）。默认空实现：测试替身无需关心诊断表。
    #[allow(clippy::too_many_arguments)]
    async fn record_reject(
        &self,
        _received_at: OffsetDateTime,
        _producer_id: Option<&str>,
        _event_id: Option<&str>,
        _reason_code: &str,
        _field: Option<&str>,
        _detail_bytes: Option<i32>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

#[async_trait]
impl EventStore for ObservationStore {
    async fn insert_batch(
        &self,
        batch: &[ReceivedObservation],
    ) -> Result<Vec<super::store::EventInsert>, Error> {
        ObservationStore::insert_batch(self, batch).await
    }

    async fn record_reject(
        &self,
        received_at: OffsetDateTime,
        producer_id: Option<&str>,
        event_id: Option<&str>,
        reason_code: &str,
        field: Option<&str>,
        detail_bytes: Option<i32>,
    ) -> Result<(), Error> {
        ObservationStore::record_reject(
            self,
            received_at,
            producer_id,
            event_id,
            reason_code,
            field,
            detail_bytes,
        )
        .await
    }
}

/// 拒收原因（映射到 §6.8 响应码）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestError {
    /// 400：信封校验失败（整批拒绝）。
    Rejected(adx_observability::business::Violation),
    /// 403：`businessType`/`tenantRef` 超出 producer 绑定。
    Forbidden,
    /// 429：超过每 producer 速率。
    RateLimited,
    /// 503：存储不可用且 spool 已满。
    Unavailable,
}

struct TokenBucket {
    tokens: f64,
    last: Instant,
}

/// 每个 producer 的接收结果计数（质量族原料）。
#[derive(Default)]
struct ProducerCounters {
    accepted: std::sync::atomic::AtomicU64,
    duplicate: std::sync::atomic::AtomicU64,
    rejected: std::sync::atomic::AtomicU64,
}

/// 接收端点状态：配置、绑定、存储、spool 与限流。
pub struct IngestState {
    max_batch: usize,
    max_bytes: usize,
    events_per_second: u64,
    bindings: Vec<ProducerBinding>,
    store: Arc<dyn EventStore>,
    /// 排队中的批次连同其许可一起传递；够不着许可说明在途容量已满。
    spool: mpsc::Sender<(Vec<ReceivedObservation>, OwnedSemaphorePermit)>,
    /// 覆盖"排队 + 正在处理"的总许可：writer 取出批次后仍占一个席位，
    /// 避免实际上限变成"配置容量 + 处理中一批"（C13）。
    permits: Arc<Semaphore>,
    buckets: Mutex<HashMap<String, TokenBucket>>,
    counters: Mutex<HashMap<String, ProducerCounters>>,
    /// 最近一批事件的接收滞后（f64 位模式；`pipeline="ingest"`）。
    ingest_lag_bits: AtomicU64,
    lag_initialized: AtomicBool,
}

impl IngestState {
    /// 构造状态与 spool 接收端；调用方负责在 `serve` 中启动 writer。
    pub fn new(
        config: IngestConfig,
        store: Arc<dyn EventStore>,
    ) -> Result<(Arc<Self>, mpsc::Receiver<SpoolBatch>), Error> {
        config.validate()?;
        let capacity = config.spool_capacity.max(1);
        let (spool, spool_rx) = mpsc::channel(capacity);
        let state = Arc::new(Self {
            max_batch: config.max_batch,
            max_bytes: config.max_bytes,
            events_per_second: config.events_per_second,
            bindings: config.bindings,
            store,
            spool,
            permits: Arc::new(Semaphore::new(capacity)),
            buckets: Mutex::new(HashMap::new()),
            counters: Mutex::new(HashMap::new()),
            ingest_lag_bits: AtomicU64::new(0),
            lag_initialized: AtomicBool::new(false),
        });
        Ok((state, spool_rx))
    }

    /// 校验 + 绑定核查 + 限流 + 写库；存储不可用转入 spool。
    /// 返回每条事件的结果（`accepted`/`duplicate`/`queued`），与批顺序一致。
    pub async fn accept(
        &self,
        binding: &ProducerBinding,
        batch: Vec<BusinessObservation>,
        received_at: OffsetDateTime,
    ) -> Result<Vec<&'static str>, IngestError> {
        let events = batch.len();
        let mut received = Vec::with_capacity(batch.len());
        for event in batch {
            if !binding.business_types.contains(&event.business_type) {
                self.count_rejected(binding, events);
                return Err(IngestError::Forbidden);
            }
            let observation =
                match ReceivedObservation::new(&binding.producer_id, received_at, event) {
                    Ok(observation) => observation,
                    Err(violation) => {
                        self.count_rejected(binding, events);
                        return Err(IngestError::Rejected(violation));
                    }
                };
            if !binding
                .tenant_scope
                .iter()
                .any(|tenant| tenant == &observation.observation().tenant_ref)
            {
                self.count_rejected(binding, events);
                return Err(IngestError::Forbidden);
            }
            received.push(observation);
        }
        // 接收滞后（received − occurred）：反映生产端到接收端的通道延迟。
        let max_lag = received
            .iter()
            .map(|event| {
                (received_at - event.observation().occurred_at)
                    .as_seconds_f64()
                    .max(0.0)
            })
            .fold(0.0_f64, f64::max);
        self.ingest_lag_bits
            .store(max_lag.to_bits(), Ordering::Relaxed);
        self.lag_initialized.store(true, Ordering::Relaxed);
        if !self.consume_rate(binding, received.len()) {
            self.count_rejected(binding, events);
            return Err(IngestError::RateLimited);
        }
        match self.store.insert_batch(&received).await {
            Ok(results) => {
                {
                    let mut counters = self
                        .counters
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let entry = counters.entry(binding.producer_id.clone()).or_default();
                    for result in &results {
                        match result {
                            super::store::EventInsert::Accepted => {
                                entry
                                    .accepted
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            super::store::EventInsert::Duplicate => {
                                entry
                                    .duplicate
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                }
                Ok(results
                    .into_iter()
                    .map(|result| match result {
                        super::store::EventInsert::Accepted => "accepted",
                        super::store::EventInsert::Duplicate => "duplicate",
                    })
                    .collect())
            }
            Err(_) => {
                let count = received.len();
                // 许可覆盖排队与处理中：writer 正在重试的批次仍占席位。
                let Ok(permit) = Arc::clone(&self.permits).try_acquire_owned() else {
                    self.count_rejected(binding, count);
                    return Err(IngestError::Unavailable);
                };
                match self.spool.try_send((received, permit)) {
                    Ok(()) => Ok(vec!["queued"; count]),
                    Err(_) => {
                        self.count_rejected(binding, count);
                        Err(IngestError::Unavailable)
                    }
                }
            }
        }
    }

    /// 落拒收诊断（M-A11）：只写原因码、字段与请求体字节数，不写正文。
    /// 诊断写入失败不影响响应——拒收结论已经返回给生产者。
    async fn record_reject(
        &self,
        binding: Option<&ProducerBinding>,
        reason_code: &str,
        field: Option<&str>,
        detail_bytes: Option<usize>,
    ) {
        let detail = detail_bytes.and_then(|value| i32::try_from(value).ok());
        let producer_id = binding.map(|binding| binding.producer_id.as_str());
        if let Err(error) = self
            .store
            .record_reject(
                OffsetDateTime::now_utc(),
                producer_id,
                None,
                reason_code,
                field,
                detail,
            )
            .await
        {
            tracing::debug!(%error, "observation reject diagnostic write failed");
        }
    }

    fn count_rejected(&self, binding: &ProducerBinding, events: usize) {
        let mut counters = self
            .counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        counters
            .entry(binding.producer_id.clone())
            .or_default()
            .rejected
            .fetch_add(events as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// 接收质量族：`adx_observation_events_total{producer_class,result}`。
    pub fn render(&self, text: &mut adx_observability::metrics::Text) {
        let counters = self
            .counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (producer, entry) in counters.iter() {
            for (result, counter) in [
                ("accepted", &entry.accepted),
                ("duplicate", &entry.duplicate),
                ("rejected", &entry.rejected),
            ] {
                text.counter(
                    "adx_observation_events_total",
                    "Observation ingest results by producer",
                    &[
                        ("producer_class", producer.clone()),
                        ("result", result.to_owned()),
                    ],
                    counter.load(std::sync::atomic::Ordering::Relaxed),
                );
            }
        }
        if self.lag_initialized.load(Ordering::Relaxed) {
            text.gauge_f64(
                "adx_observation_lag_seconds",
                "Occurred-to-aggregated lag of the business-event pipeline",
                &[("pipeline", "ingest".to_owned())],
                f64::from_bits(self.ingest_lag_bits.load(Ordering::Relaxed)),
            );
        }
    }

    /// 每 producer 令牌桶（容量 = 速率，按秒补充）。
    fn consume_rate(&self, binding: &ProducerBinding, events: usize) -> bool {
        let rate = self.events_per_second as f64;
        let mut buckets = self
            .buckets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let bucket = buckets
            .entry(binding.producer_id.clone())
            .or_insert_with(|| TokenBucket {
                tokens: rate,
                last: Instant::now(),
            });
        let now = Instant::now();
        let elapsed = now.duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * rate).min(rate);
        bucket.last = now;
        let requested = events as f64;
        if bucket.tokens >= requested {
            bucket.tokens -= requested;
            true
        } else {
            false
        }
    }

    /// 按 Bearer token 找绑定；双方先 SHA-256 再比较，避免直接比较明文 token。
    fn authenticate(
        &self,
        header: Option<&hyper::header::HeaderValue>,
    ) -> Option<&ProducerBinding> {
        let token = header?.to_str().ok()?.strip_prefix("Bearer ")?;
        let digest = Sha256::digest(token.as_bytes());
        self.bindings.iter().find(|binding| {
            let expected = Sha256::digest(binding.token.as_bytes());
            expected[..] == digest[..]
        })
    }

    /// spool 消费循环：写库失败保持持有并退避重试（spool 满时新请求 503）。
    ///
    /// 关停时不直接 abort：已回 `202/queued` 的批必须尽力落库。收到信号后
    /// 当前批再试一次，然后对队列里剩余的批各尝试一次（有界排空）。
    async fn run_writer(
        self: Arc<Self>,
        mut spool: mpsc::Receiver<SpoolBatch>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        loop {
            // 关停位是粘滞的：`changed()` 可能已被 `persist` 消费，轮次开头
            // 用 `borrow()` 复查，避免漏掉排空。
            if *shutdown.borrow() {
                self.drain(spool).await;
                return;
            }
            let (batch, _permit) = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.drain(spool).await;
                        return;
                    }
                    continue;
                }
                batch = spool.recv() => match batch {
                    Some(batch) => batch,
                    None => return,
                },
            };
            if !self.persist(&batch, &mut shutdown).await {
                self.drain(spool).await;
                return;
            }
        }
    }

    /// 写一批直到成功；关停信号到达时再试一次，返回是否落库成功。
    async fn persist(
        &self,
        batch: &[ReceivedObservation],
        shutdown: &mut watch::Receiver<bool>,
    ) -> bool {
        loop {
            if self.store.insert_batch(batch).await.is_ok() {
                return true;
            }
            tokio::select! {
                _ = tokio::time::sleep(RETRY_INTERVAL) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        // 关停：最后一次尝试，失败即放弃，不能无限期拖住退出。
                        return self.store.insert_batch(batch).await.is_ok();
                    }
                }
            }
        }
    }

    /// 关停排空：对队列中剩余的批各尝试一次写库（不重试）。
    async fn drain(&self, mut spool: mpsc::Receiver<SpoolBatch>) {
        spool.close();
        while let Some((batch, _permit)) = spool.recv().await {
            let _ = self.store.insert_batch(&batch).await;
        }
    }
}

/// 服务入口：writer + HTTP 接受循环（设计签名 `serve(listener, state)`，
/// spool/shutdown 为实现细节）。
pub async fn serve(
    listener: TcpListener,
    state: Arc<IngestState>,
    spool: mpsc::Receiver<SpoolBatch>,
    mut shutdown: watch::Receiver<bool>,
) -> std::io::Result<()> {
    let writer = {
        let state = Arc::clone(&state);
        let shutdown = shutdown.clone();
        tokio::spawn(async move { state.run_writer(spool, shutdown).await })
    };
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    // 等 writer 完成有界排空（最多 DRAIN_TIMEOUT），避免把已
                    // 回 202 的批留在内存里随进程消失。
                    let _ = tokio::time::timeout(DRAIN_TIMEOUT, writer).await;
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, _peer) = accepted?;
                let state = Arc::clone(&state);
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        let state = Arc::clone(&state);
                        async move { Ok::<_, Infallible>(handle(state, request).await) }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        }
    }
}

async fn handle(state: Arc<IngestState>, request: Request<Incoming>) -> Response<Full<Bytes>> {
    if request.uri().path() != INGEST_PATH {
        return error_response(StatusCode::NOT_FOUND, "not_found");
    }
    if request.method() != Method::POST {
        return error_response(StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed");
    }
    let Some(binding) = state.authenticate(request.headers().get(header::AUTHORIZATION)) else {
        return error_response(StatusCode::UNAUTHORIZED, "unauthorized");
    };
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok());
    if !content_type.is_some_and(|value| value.starts_with("application/json")) {
        state
            .record_reject(Some(binding), "content_type", None, None)
            .await;
        return error_response(StatusCode::BAD_REQUEST, "content_type");
    }
    let body = match Limited::new(request.into_body(), state.max_bytes)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(error) => {
            // 区分"超过上限"与"读体失败"：前者是客户端体的确定性拒收，
            // 后者是传输层中断，原因码不能混为一谈。
            let too_large = error
                .downcast_ref::<http_body_util::LengthLimitError>()
                .is_some();
            let reason = if too_large {
                "body_too_large"
            } else {
                "body_read_error"
            };
            state.record_reject(Some(binding), reason, None, None).await;
            return error_response(StatusCode::BAD_REQUEST, reason);
        }
    };
    let batch: Vec<BusinessObservation> = match serde_json::from_slice(&body) {
        Ok(batch) => batch,
        Err(_) => {
            state
                .record_reject(Some(binding), "invalid_json", None, Some(body.len()))
                .await;
            return error_response(StatusCode::BAD_REQUEST, "invalid_json");
        }
    };
    if batch.is_empty() || batch.len() > state.max_batch {
        state
            .record_reject(Some(binding), "batch_size", None, Some(body.len()))
            .await;
        return error_response(StatusCode::BAD_REQUEST, "batch_size");
    }
    match state
        .accept(binding, batch, OffsetDateTime::now_utc())
        .await
    {
        Ok(results) => json_response(
            StatusCode::ACCEPTED,
            &serde_json::json!({ "results": results }),
        ),
        Err(IngestError::Rejected(violation)) => {
            state
                .record_reject(
                    Some(binding),
                    violation.class().as_str(),
                    Some(violation.field()),
                    Some(body.len()),
                )
                .await;
            json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({
                    "result": "rejected",
                    "reason": violation.class().as_str(),
                    "field": violation.field(),
                }),
            )
        }
        Err(IngestError::Forbidden) => {
            state
                .record_reject(Some(binding), "forbidden", None, Some(body.len()))
                .await;
            error_response(StatusCode::FORBIDDEN, "forbidden")
        }
        Err(IngestError::RateLimited) => {
            state
                .record_reject(Some(binding), "rate_limited", None, Some(body.len()))
                .await;
            error_response(StatusCode::TOO_MANY_REQUESTS, "rate_limited")
        }
        Err(IngestError::Unavailable) => {
            error_response(StatusCode::SERVICE_UNAVAILABLE, "unavailable")
        }
    }
}

fn error_response(status: StatusCode, reason: &str) -> Response<Full<Bytes>> {
    json_response(
        status,
        &serde_json::json!({ "result": "rejected", "reason": reason }),
    )
}

fn json_response(status: StatusCode, body: &serde_json::Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_observability::business::{BusinessType, EventType, StartMode};
    use time::Duration;

    struct FakeStore {
        fail: bool,
    }

    #[async_trait]
    impl EventStore for FakeStore {
        async fn insert_batch(
            &self,
            batch: &[ReceivedObservation],
        ) -> Result<Vec<super::super::store::EventInsert>, Error> {
            if self.fail {
                return Err(Error::Unavailable);
            }
            Ok(batch
                .iter()
                .map(|event| {
                    if event.observation().event_id == "dup" {
                        super::super::store::EventInsert::Duplicate
                    } else {
                        super::super::store::EventInsert::Accepted
                    }
                })
                .collect())
        }
    }

    fn binding() -> ProducerBinding {
        ProducerBinding {
            producer_id: "jiuwen-ingress".to_owned(),
            token: "service-token".to_owned(),
            tenant_scope: vec!["tenant-1".to_owned()],
            business_types: vec![BusinessType::Jiuwen],
        }
    }

    fn config(spool_capacity: usize) -> IngestConfig {
        IngestConfig {
            listen: "127.0.0.1:19094".parse().unwrap(),
            bindings: vec![binding()],
            max_batch: DEFAULT_MAX_BATCH,
            max_bytes: DEFAULT_MAX_BYTES,
            events_per_second: DEFAULT_EVENTS_PER_SECOND,
            spool_capacity,
        }
    }

    fn event(event_id: &str) -> BusinessObservation {
        BusinessObservation {
            schema_version: "v1".to_owned(),
            event_id: event_id.to_owned(),
            business_type: BusinessType::Jiuwen,
            action_kind: None,
            event_type: EventType::ExperienceAccepted,
            occurred_at: OffsetDateTime::now_utc() - Duration::seconds(30),
            tenant_ref: "tenant-1".to_owned(),
            user_ref: Some("user-1".to_owned()),
            session_ref: Some("session-1".to_owned()),
            experience_id: "exp-1".to_owned(),
            environment_id: None,
            template_release: Some("chat@1.0.0".to_owned()),
            start_mode: Some(StartMode::Unknown),
            trace_id: None,
            span_id: None,
            outcome: None,
            reason_class: None,
            duration_seconds: None,
        }
    }

    fn state(store: FakeStore, spool_capacity: usize) -> Arc<IngestState> {
        IngestState::new(config(spool_capacity), Arc::new(store))
            .map(|(state, _spool)| state)
            .unwrap()
    }

    /// 配置结构体的 `{:?}` 不得带出生产者 token 明文。
    #[test]
    fn config_debug_redacts_binding_tokens() {
        let rendered = format!("{:?}", config(1));
        assert!(!rendered.contains("service-token"), "{rendered}");
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn config_rejects_public_listen_and_empty_scope() {
        let mut public = config(1);
        public.listen = "0.0.0.0:19094".parse().unwrap();
        assert_eq!(public.validate(), Err(Error::Configuration));

        let mut empty_scope = config(1);
        empty_scope.bindings[0].tenant_scope.clear();
        assert_eq!(empty_scope.validate(), Err(Error::Configuration));

        let mut empty_bindings = config(1);
        empty_bindings.bindings.clear();
        assert_eq!(empty_bindings.validate(), Err(Error::Configuration));

        assert_eq!(config(1).validate(), Ok(()));
    }

    #[tokio::test]
    async fn accept_maps_accepted_duplicate_and_queue_results() {
        let (state, _spool) =
            IngestState::new(config(1), Arc::new(FakeStore { fail: false })).unwrap();
        let results = state
            .accept(
                &binding(),
                vec![event("evt-1"), event("dup")],
                OffsetDateTime::now_utc(),
            )
            .await
            .unwrap();
        assert_eq!(results, vec!["accepted", "duplicate"]);
        // 质量族：接收结果计数 + ingest 滞后（事件 occurred 为 30 秒前）。
        let mut text = adx_observability::metrics::Text::default();
        state.render(&mut text);
        let output = text.finish();
        assert!(output.contains(
            "adx_observation_events_total{producer_class=\"jiuwen-ingress\",result=\"accepted\"} 1"
        ));
        assert!(output.contains(
            "adx_observation_events_total{producer_class=\"jiuwen-ingress\",result=\"duplicate\"} 1"
        ));
        assert!(output.contains("adx_observation_lag_seconds{pipeline=\"ingest\"}"));
    }

    #[tokio::test]
    async fn accept_rejects_whole_batch_on_any_violation() {
        let state = state(FakeStore { fail: false }, 1);
        let mut invalid = event("evt-2");
        invalid.tenant_ref = "  ".to_owned();
        let error = state
            .accept(
                &binding(),
                vec![event("evt-1"), invalid],
                OffsetDateTime::now_utc(),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, IngestError::Rejected(_)));
    }

    #[tokio::test]
    async fn accept_forbids_out_of_scope_tenant_and_business_type() {
        let state = state(FakeStore { fail: false }, 1);
        let mut foreign_tenant = event("evt-1");
        foreign_tenant.tenant_ref = "tenant-2".to_owned();
        assert_eq!(
            state
                .accept(&binding(), vec![foreign_tenant], OffsetDateTime::now_utc())
                .await
                .unwrap_err(),
            IngestError::Forbidden
        );

        let narrow = ProducerBinding {
            business_types: vec![],
            ..binding()
        };
        assert_eq!(
            state
                .accept(&narrow, vec![event("evt-1")], OffsetDateTime::now_utc())
                .await
                .unwrap_err(),
            IngestError::Forbidden
        );
    }

    #[tokio::test]
    async fn accept_rate_limits_per_producer() {
        let mut narrow = config(1);
        narrow.events_per_second = 1;
        let (state, _spool) =
            IngestState::new(narrow, Arc::new(FakeStore { fail: false })).unwrap();
        assert_eq!(
            state
                .accept(
                    &binding(),
                    vec![event("evt-1"), event("evt-2")],
                    OffsetDateTime::now_utc()
                )
                .await
                .unwrap_err(),
            IngestError::RateLimited
        );
    }

    #[tokio::test]
    async fn accept_queues_when_store_down_and_returns_503_when_spool_full() {
        // 无 writer：两层 spool 容量 → 第一批入队，第二批 503。
        let (state, _spool) =
            IngestState::new(config(1), Arc::new(FakeStore { fail: true })).unwrap();
        let queued = state
            .accept(&binding(), vec![event("evt-1")], OffsetDateTime::now_utc())
            .await
            .unwrap();
        assert_eq!(queued, vec!["queued"]);
        assert_eq!(
            state
                .accept(&binding(), vec![event("evt-2")], OffsetDateTime::now_utc())
                .await
                .unwrap_err(),
            IngestError::Unavailable
        );
    }

    /// 起一个真实监听，返回地址与关停句柄。
    async fn spawn_server(
        store: FakeStore,
        spool_capacity: usize,
    ) -> (std::net::SocketAddr, watch::Sender<bool>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (state, spool) = IngestState::new(config(spool_capacity), Arc::new(store)).unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(serve(listener, state, spool, shutdown_rx));
        (addr, shutdown_tx)
    }

    async fn post(
        addr: std::net::SocketAddr,
        token: &str,
        body: String,
    ) -> (reqwest::StatusCode, serde_json::Value) {
        let response = reqwest::Client::new()
            .post(format!("http://{addr}{INGEST_PATH}"))
            .bearer_auth(token)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let body = response.json().await.unwrap_or_default();
        (status, body)
    }

    fn wire_batch(events: &[BusinessObservation]) -> String {
        serde_json::to_string(events).unwrap()
    }

    #[tokio::test]
    async fn http_returns_401_400_403_429_and_202() {
        let (addr, shutdown) = spawn_server(FakeStore { fail: false }, 4).await;

        let (status, _) = post(addr, "wrong-token", wire_batch(&[event("evt-1")])).await;
        assert_eq!(status, reqwest::StatusCode::UNAUTHORIZED);

        let (status, body) = post(addr, "service-token", "{not-json".to_owned()).await;
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(body["reason"], "invalid_json");

        let mut foreign = event("evt-1");
        foreign.tenant_ref = "tenant-2".to_owned();
        let (status, body) = post(addr, "service-token", wire_batch(&[foreign])).await;
        assert_eq!(status, reqwest::StatusCode::FORBIDDEN);
        assert_eq!(body["reason"], "forbidden");

        let (status, body) = post(addr, "service-token", wire_batch(&[event("evt-1")])).await;
        assert_eq!(status, reqwest::StatusCode::ACCEPTED);
        assert_eq!(body["results"], serde_json::json!(["accepted"]));

        // 客户端自报 producerId：deny_unknown_fields → 400。
        let mut self_reported = serde_json::to_value(event("evt-2")).unwrap();
        self_reported["producerId"] = serde_json::json!("forged");
        let (status, body) = post(addr, "service-token", self_reported.to_string()).await;
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(body["reason"], "invalid_json");

        shutdown.send(true).unwrap();
    }

    #[tokio::test]
    async fn http_returns_429_when_batch_exceeds_rate() {
        let mut narrow = config(1);
        narrow.events_per_second = 1;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (state, spool) = IngestState::new(narrow, Arc::new(FakeStore { fail: false })).unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        tokio::spawn(serve(listener, state, spool, shutdown_rx));
        let (status, body) = post(
            addr,
            "service-token",
            wire_batch(&[event("evt-1"), event("evt-2")]),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(body["reason"], "rate_limited");
        shutdown_tx.send(true).unwrap();
    }

    #[tokio::test]
    async fn http_rejects_unknown_path_and_method() {
        let (addr, shutdown) = spawn_server(FakeStore { fail: false }, 4).await;
        let response = reqwest::Client::new()
            .post(format!("http://{addr}/metrics"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
        let response = reqwest::Client::new()
            .get(format!("http://{addr}{INGEST_PATH}"))
            .bearer_auth("service-token")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
        shutdown.send(true).unwrap();
    }

    #[tokio::test]
    async fn http_rejects_non_json_content_type() {
        let (addr, shutdown) = spawn_server(FakeStore { fail: false }, 4).await;
        let response = reqwest::Client::new()
            .post(format!("http://{addr}{INGEST_PATH}"))
            .bearer_auth("service-token")
            .header("content-type", "text/plain")
            .body("[]")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        shutdown.send(true).unwrap();
    }

    /// 可切换失败、并记录写入批次与拒收诊断的存储替身。
    #[derive(Clone, Default)]
    struct RecordingStore {
        fail: Arc<AtomicBool>,
        batches: Arc<Mutex<Vec<usize>>>,
        rejects: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl EventStore for RecordingStore {
        async fn insert_batch(
            &self,
            batch: &[ReceivedObservation],
        ) -> Result<Vec<super::super::store::EventInsert>, Error> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(Error::Unavailable);
            }
            self.batches.lock().unwrap().push(batch.len());
            Ok(vec![
                super::super::store::EventInsert::Accepted;
                batch.len()
            ])
        }

        async fn record_reject(
            &self,
            _received_at: OffsetDateTime,
            producer_id: Option<&str>,
            _event_id: Option<&str>,
            reason_code: &str,
            field: Option<&str>,
            detail_bytes: Option<i32>,
        ) -> Result<(), Error> {
            self.rejects.lock().unwrap().push(format!(
                "{}/{}/{}",
                producer_id.unwrap_or("-"),
                reason_code,
                detail_bytes
                    .map(|value| value.to_string())
                    .or_else(|| field.map(str::to_owned))
                    .unwrap_or_else(|| "-".to_owned())
            ));
            Ok(())
        }
    }

    #[tokio::test]
    async fn reject_diagnostics_record_reason_field_and_size() {
        let store = RecordingStore::default();
        let (state, _spool) = IngestState::new(config(1), Arc::new(store.clone())).unwrap();
        state
            .record_reject(Some(&binding()), "invalid_json", None, Some(42))
            .await;
        state
            .record_reject(Some(&binding()), "clock_skew", Some("occurredAt"), None)
            .await;
        let rejects = store.rejects.lock().unwrap().clone();
        assert_eq!(
            rejects,
            vec![
                "jiuwen-ingress/invalid_json/42".to_owned(),
                "jiuwen-ingress/clock_skew/occurredAt".to_owned(),
            ]
        );
    }

    /// 关停不能丢已回 202/queued 的批：writer 应把 spool 剩余内容排空落库。
    #[tokio::test]
    async fn writer_drains_queued_batches_on_shutdown() {
        let store = RecordingStore {
            fail: Arc::new(AtomicBool::new(true)),
            ..Default::default()
        };
        let (state, spool) = IngestState::new(config(4), Arc::new(store.clone())).unwrap();
        let now = OffsetDateTime::now_utc();
        assert_eq!(
            state
                .accept(&binding(), vec![event("evt-1")], now)
                .await
                .unwrap(),
            vec!["queued"]
        );
        assert_eq!(
            state
                .accept(&binding(), vec![event("evt-2")], now)
                .await
                .unwrap(),
            vec!["queued"]
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let writer = tokio::spawn({
            let state = Arc::clone(&state);
            async move { state.run_writer(spool, shutdown_rx).await }
        });
        // 让 writer 进入对第一批的退避重试。
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        store.fail.store(false, Ordering::Relaxed);
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), writer)
            .await
            .expect("writer should exit after shutdown")
            .unwrap();
        let batches = store.batches.lock().unwrap().clone();
        assert_eq!(
            batches.len(),
            2,
            "queued batches must be persisted: {batches:?}"
        );
    }
}
