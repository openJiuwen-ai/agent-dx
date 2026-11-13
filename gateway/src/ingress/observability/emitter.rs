//! 业务事件发射（M2-4，详设 §6.4 M2-4b）：挂点判定 → 非阻塞 spool → 接收端点。
//!
//! - **热路径零阻塞**：`emit` 只做校验与 `try_send`；spool 满即丢弃并计
//!   `dropped`，绝不阻塞业务请求（软依赖）。
//! - **脱敏引用**：`userRef`/`sessionRef` 用 HMAC-SHA256（密钥来自
//!   `hmac_key_env` 指向的环境变量），原文不落库。
//! - **投递语义**：后台 worker 组批 POST；`202` 成功、`429`/`5xx`/网络错误按
//!   指数退避重试至 `max_retry_window`，超时/`4xx` 计 `dead_lettered`。
//! - **关机**：spool 未投递事件随进程退出丢失（软依赖，不等待持久化）。
use adx_observability::business::BusinessObservation;
use adx_observability::metrics::Text;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use time::OffsetDateTime;
use tokio::sync::{mpsc, watch};

/// 发端配置（`ADX_OBSERVABILITY_CONFIG.emit`）。
#[derive(Clone, Deserialize)]
pub struct EmitConfig {
    /// 接收端点完整 URL（如 `http://127.0.0.1:19094/internal/adx/observability/v1/events`）。
    pub endpoint: String,
    /// 服务 token（与接收端点 producer 绑定一致）。
    pub token: String,
    /// HMAC 密钥所在的环境变量名（`userRef`/`sessionRef` 脱敏用）。
    pub hmac_key_env: String,
    #[serde(default = "default_spool_capacity")]
    pub spool_capacity: usize,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_flush_millis")]
    pub flush_interval_millis: u64,
    #[serde(default = "default_retry_window")]
    pub max_retry_window_seconds: u64,
}

/// 手写 `Debug`：`token` 是服务凭据，任何 `{:?}` 打印（含启动日志与错误
/// 上下文）都不得带出明文。
impl std::fmt::Debug for EmitConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmitConfig")
            .field("endpoint", &self.endpoint)
            .field("token", &"<redacted>")
            .field("hmac_key_env", &self.hmac_key_env)
            .field("spool_capacity", &self.spool_capacity)
            .field("batch_size", &self.batch_size)
            .field("flush_interval_millis", &self.flush_interval_millis)
            .field("max_retry_window_seconds", &self.max_retry_window_seconds)
            .finish()
    }
}

fn default_spool_capacity() -> usize {
    256
}

fn default_batch_size() -> usize {
    20
}

fn default_flush_millis() -> u64 {
    1_000
}

fn default_retry_window() -> u64 {
    3_600
}

const MAX_BATCH_SIZE: usize = 500;
const RETRY_BACKOFF_START: Duration = Duration::from_millis(100);
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// 业务事件发射器：进程内单实例，挂点通过 [`ObservationSink::emit`] 写入。
pub struct ObservationSink {
    endpoint: String,
    token: String,
    hmac_key: Vec<u8>,
    batch_size: usize,
    flush_interval: Duration,
    max_retry_window: Duration,
    client: reqwest::Client,
    spool: mpsc::Sender<BusinessObservation>,
    spooled: AtomicU64,
    dropped: AtomicU64,
    sent: AtomicU64,
    dead_lettered: AtomicU64,
}

impl ObservationSink {
    /// 校验配置并创建 spool；调用方负责用 [`ObservationSink::run`] 启动 worker
    /// （测试可只保留接收端直接断言事件内容）。
    pub fn new(
        config: &EmitConfig,
    ) -> Result<(Arc<Self>, mpsc::Receiver<BusinessObservation>), super::Error> {
        if !config.endpoint.starts_with("http://") && !config.endpoint.starts_with("https://") {
            return Err(super::Error::Configuration);
        }
        if config.token.trim().is_empty()
            || config.spool_capacity == 0
            || config.batch_size == 0
            || config.batch_size > MAX_BATCH_SIZE
        {
            return Err(super::Error::Configuration);
        }
        let hmac_key = std::env::var(&config.hmac_key_env)
            .map_err(|_| super::Error::Configuration)?
            .into_bytes();
        if hmac_key.is_empty() {
            return Err(super::Error::Configuration);
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| super::Error::Configuration)?;
        let (spool, receiver) = mpsc::channel(config.spool_capacity);
        let sink = Arc::new(Self {
            endpoint: config.endpoint.clone(),
            token: config.token.clone(),
            hmac_key,
            batch_size: config.batch_size,
            flush_interval: Duration::from_millis(config.flush_interval_millis.max(10)),
            max_retry_window: Duration::from_secs(config.max_retry_window_seconds.max(1)),
            client,
            spool,
            spooled: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            dead_lettered: AtomicU64::new(0),
        });
        Ok((sink, receiver))
    }

    /// 热路径发射：非阻塞。信封校验失败或 spool 满只计数，不返回错误。
    /// 返回是否已进入 spool——覆盖率分子只认这个口径，不引用"已入库"。
    pub fn emit(&self, observation: BusinessObservation) -> bool {
        if let Err(violation) = observation.validate(OffsetDateTime::now_utc()) {
            self.dead_lettered.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                field = violation.field(),
                class = violation.class().as_str(),
                "BusinessObservation rejected before spool"
            );
            return false;
        }
        match self.spool.try_send(observation) {
            Ok(()) => {
                self.spooled.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(_) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    /// 脱敏引用：HMAC-SHA256（hex）。租户参与命名空间，避免同引用的
    /// 不同租户在活跃去重时被合并成一个（M-A04）。
    pub fn hmac_ref(&self, tenant: &str, value: &str) -> String {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, &self.hmac_key);
        let mut input = Vec::with_capacity(tenant.len() + 1 + value.len());
        input.extend_from_slice(tenant.as_bytes());
        input.push(0);
        input.extend_from_slice(value.as_bytes());
        let tag = ring::hmac::sign(&key, &input);
        hex::encode(tag)
    }

    /// 发射质量族（spooled/dropped/sent/dead_lettered）。
    pub fn render(&self, text: &mut Text) {
        for (result, counter) in [
            ("spooled", &self.spooled),
            ("dropped", &self.dropped),
            ("sent", &self.sent),
            ("dead_lettered", &self.dead_lettered),
        ] {
            text.counter(
                "adx_observation_emit_total",
                "Business observation emission outcomes",
                &[("result", result.to_owned())],
                counter.load(Ordering::Relaxed),
            );
        }
    }

    /// 后台 worker：组批投递（批大小 / 刷新间隔），失败按退避重试。
    pub async fn run(
        self: Arc<Self>,
        mut spool: mpsc::Receiver<BusinessObservation>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        loop {
            let first = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        // 关停排空：spool 里已计入 `spooled` 的事件各投递一次
                        //（不重试），不能随进程退出静默丢掉。
                        self.drain(&mut spool).await;
                        return;
                    }
                    continue;
                }
                event = spool.recv() => match event {
                    Some(event) => event,
                    None => return,
                },
            };
            let mut batch = vec![first];
            let deadline = tokio::time::Instant::now() + self.flush_interval;
            while batch.len() < self.batch_size {
                match tokio::time::timeout_at(deadline, spool.recv()).await {
                    Ok(Some(event)) => batch.push(event),
                    Ok(None) | Err(_) => break,
                }
            }
            self.deliver(batch).await;
        }
    }

    /// 关停排空：把队列里剩余事件按批各投递一次，不走退避重试。
    async fn drain(&self, spool: &mut mpsc::Receiver<BusinessObservation>) {
        let mut batch = Vec::new();
        while let Ok(event) = spool.try_recv() {
            batch.push(event);
            if batch.len() >= self.batch_size {
                self.deliver_once(std::mem::take(&mut batch)).await;
            }
        }
        if !batch.is_empty() {
            self.deliver_once(batch).await;
        }
    }

    /// 单次投递（关停路径用）：成功计 `sent`，其余一律 `dead_lettered`。
    async fn deliver_once(&self, batch: Vec<BusinessObservation>) {
        let count = batch.len() as u64;
        let Ok(body) = serde_json::to_vec(&batch) else {
            self.dead_lettered.fetch_add(count, Ordering::Relaxed);
            return;
        };
        let delivered = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.token)
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        if delivered {
            self.sent.fetch_add(count, Ordering::Relaxed);
        } else {
            self.dead_lettered.fetch_add(count, Ordering::Relaxed);
        }
    }

    /// 投递一批：202 成功；429/503/网络错误退避重试；其余 4xx 直接死信。
    async fn deliver(&self, batch: Vec<BusinessObservation>) {
        let count = batch.len() as u64;
        let body = match serde_json::to_vec(&batch) {
            Ok(body) => body,
            Err(_) => {
                self.dead_lettered.fetch_add(count, Ordering::Relaxed);
                tracing::warn!(count, "BusinessObservation batch serialization failed");
                return;
            }
        };
        let deadline = tokio::time::Instant::now() + self.max_retry_window;
        let mut backoff = RETRY_BACKOFF_START;
        loop {
            let outcome = self
                .client
                .post(&self.endpoint)
                .bearer_auth(&self.token)
                .header(CONTENT_TYPE, "application/json")
                .body(body.clone())
                .send()
                .await;
            // 429 与全部 5xx 都是瞬时错误（对端过载/网关抖动），退避重试；
            // 其余 4xx 是确定性拒收，立即死信。
            let retryable = match outcome {
                Ok(response) if response.status().is_success() => {
                    self.sent.fetch_add(count, Ordering::Relaxed);
                    return;
                }
                Ok(response)
                    if response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || response.status().is_server_error() =>
                {
                    tracing::debug!(count, status = %response.status(), "retrying batch");
                    true
                }
                Ok(response) => {
                    self.dead_lettered.fetch_add(count, Ordering::Relaxed);
                    tracing::warn!(count, status = %response.status(),
                        "BusinessObservation rejected; events discarded");
                    return;
                }
                Err(error) => {
                    tracing::debug!(count, %error, "retrying batch");
                    true
                }
            };
            debug_assert!(retryable);
            if tokio::time::Instant::now() + backoff > deadline {
                self.dead_lettered.fetch_add(count, Ordering::Relaxed);
                tracing::warn!(count, "BusinessObservation retry window exhausted");
                return;
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(RETRY_BACKOFF_MAX);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_observability::business::{BusinessType, EventType, StartMode};

    fn test_event(event_id: &str) -> BusinessObservation {
        BusinessObservation {
            schema_version: "v1".to_owned(),
            event_id: event_id.to_owned(),
            business_type: BusinessType::Jiuwen,
            action_kind: None,
            event_type: EventType::ExperienceAccepted,
            occurred_at: OffsetDateTime::now_utc(),
            tenant_ref: "tenant-1".to_owned(),
            user_ref: Some("user-ref".to_owned()),
            session_ref: Some("session-ref".to_owned()),
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

    fn config(endpoint: &str) -> EmitConfig {
        EmitConfig {
            endpoint: endpoint.to_owned(),
            token: "service-token".to_owned(),
            hmac_key_env: "ADX_TEST_HMAC_KEY".to_owned(),
            spool_capacity: 4,
            batch_size: 2,
            flush_interval_millis: 50,
            max_retry_window_seconds: 1,
        }
    }

    #[test]
    fn hmac_ref_is_deterministic_and_redacted() {
        std::env::set_var("ADX_TEST_HMAC_KEY", "test-key-material");
        let (sink, _receiver) = ObservationSink::new(&config("http://127.0.0.1:1/events")).unwrap();
        let first = sink.hmac_ref("tenant-1", "user-1");
        assert_eq!(first, sink.hmac_ref("tenant-1", "user-1"));
        assert_ne!(first, sink.hmac_ref("tenant-1", "user-2"));
        // 同引用不同租户不得合并（M-A04）。
        assert_ne!(first, sink.hmac_ref("tenant-2", "user-1"));
        assert_eq!(first.len(), 64);
        assert!(!first.contains("user-1"));
    }

    /// 配置结构体的 `{:?}` 不得带出服务 token 明文。
    #[test]
    fn config_debug_redacts_token() {
        let rendered = format!("{:?}", config("http://127.0.0.1:1/events"));
        assert!(!rendered.contains("service-token"), "{rendered}");
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn config_validation_rejects_bad_values() {
        std::env::set_var("ADX_TEST_HMAC_KEY", "test-key-material");
        let mut bad = config("http://127.0.0.1:1/events");
        bad.token = " ".to_owned();
        assert_eq!(
            ObservationSink::new(&bad).map(|_| ()).unwrap_err(),
            super::super::Error::Configuration
        );
        let mut bad = config("not-a-url");
        bad.token = "t".to_owned();
        assert_eq!(
            ObservationSink::new(&bad).map(|_| ()).unwrap_err(),
            super::super::Error::Configuration
        );
        let mut bad = config("http://127.0.0.1:1/events");
        bad.batch_size = 501;
        assert_eq!(
            ObservationSink::new(&bad).map(|_| ()).unwrap_err(),
            super::super::Error::Configuration
        );
        // 未设置的环境变量名（不与并发测试共享，避免删改变量竞态）。
        let mut missing = config("http://127.0.0.1:1/events");
        missing.hmac_key_env = "ADX_TEST_HMAC_KEY_NEVER_SET".to_owned();
        assert_eq!(
            ObservationSink::new(&missing).map(|_| ()).unwrap_err(),
            super::super::Error::Configuration
        );
    }

    #[tokio::test]
    async fn emit_rejects_invalid_events_and_counts_drops() {
        std::env::set_var("ADX_TEST_HMAC_KEY", "test-key-material");
        let (sink, mut receiver) =
            ObservationSink::new(&config("http://127.0.0.1:1/events")).unwrap();
        let mut invalid = test_event("bad");
        invalid.tenant_ref = " ".to_owned();
        sink.emit(invalid);
        assert_eq!(sink.dead_lettered.load(Ordering::Relaxed), 1);
        assert!(receiver.try_recv().is_err());
        // spool 容量 4：5 条合法事件 → 至少 1 条 dropped。
        for index in 0..5 {
            sink.emit(test_event(&format!("evt-{index}")));
        }
        assert!(sink.dropped.load(Ordering::Relaxed) >= 1);
    }

    type Captured = Arc<std::sync::Mutex<Vec<(String, Vec<BusinessObservation>)>>>;
    type Statuses = Arc<std::sync::Mutex<Vec<axum::http::StatusCode>>>;
    type ReceiverState = (Captured, Statuses);

    /// 假接收端点：记录请求头与批次，返回可编排的状态码（从尾部弹）。
    async fn spawn_receiver(
        statuses: Vec<axum::http::StatusCode>,
    ) -> (std::net::SocketAddr, Captured) {
        use axum::{extract::State, routing::post, Json, Router};
        let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state: ReceiverState = (captured.clone(), Arc::new(std::sync::Mutex::new(statuses)));
        let app = Router::new()
            .route(
                "/events",
                post(
                    |State((captured, statuses)): State<ReceiverState>,
                     headers: axum::http::HeaderMap,
                     Json(batch): Json<Vec<BusinessObservation>>| async move {
                        let authorization = headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or("")
                            .to_owned();
                        captured.lock().unwrap().push((authorization, batch));
                        statuses
                            .lock()
                            .unwrap()
                            .pop()
                            .unwrap_or(axum::http::StatusCode::ACCEPTED)
                    },
                ),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (addr, captured)
    }

    #[tokio::test]
    async fn worker_delivers_batches_with_token() {
        std::env::set_var("ADX_TEST_HMAC_KEY", "test-key-material");
        let (addr, captured) = spawn_receiver(vec![axum::http::StatusCode::ACCEPTED]).await;
        let (sink, receiver) =
            ObservationSink::new(&config(&format!("http://{addr}/events"))).unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker = {
            let sink = sink.clone();
            tokio::spawn(async move { sink.run(receiver, shutdown_rx).await })
        };
        sink.emit(test_event("evt-1"));
        tokio::time::sleep(Duration::from_millis(300)).await;
        {
            let captured = captured.lock().unwrap();
            assert_eq!(captured.len(), 1);
            assert_eq!(captured[0].0, "Bearer service-token");
            assert_eq!(captured[0].1.len(), 1);
            assert_eq!(captured[0].1[0].event_id, "evt-1");
        }
        assert_eq!(sink.sent.load(Ordering::Relaxed), 1);
        shutdown_tx.send(true).unwrap();
        worker.abort();
    }

    #[tokio::test]
    async fn retries_on_503_then_delivers() {
        std::env::set_var("ADX_TEST_HMAC_KEY", "test-key-material");
        // pop 从尾部取：第一次请求 503，重试得到 202。
        let (addr, captured) = spawn_receiver(vec![
            axum::http::StatusCode::ACCEPTED,
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        ])
        .await;
        let (sink, receiver) =
            ObservationSink::new(&config(&format!("http://{addr}/events"))).unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker = {
            let sink = sink.clone();
            tokio::spawn(async move { sink.run(receiver, shutdown_rx).await })
        };
        sink.emit(test_event("evt-retry"));
        tokio::time::sleep(Duration::from_millis(800)).await;
        {
            let captured = captured.lock().unwrap();
            assert_eq!(captured.len(), 2);
            assert_eq!(captured[1].1[0].event_id, "evt-retry");
        }
        assert_eq!(sink.sent.load(Ordering::Relaxed), 1);
        assert_eq!(sink.dead_lettered.load(Ordering::Relaxed), 0);
        shutdown_tx.send(true).unwrap();
        worker.abort();
    }

    /// 5xx 与 429 同属瞬时错误：不得立即死信。
    #[tokio::test]
    async fn retries_on_500_then_delivers() {
        std::env::set_var("ADX_TEST_HMAC_KEY", "test-key-material");
        let (addr, captured) = spawn_receiver(vec![
            axum::http::StatusCode::ACCEPTED,
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ])
        .await;
        let (sink, receiver) =
            ObservationSink::new(&config(&format!("http://{addr}/events"))).unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker = {
            let sink = sink.clone();
            tokio::spawn(async move { sink.run(receiver, shutdown_rx).await })
        };
        sink.emit(test_event("evt-5xx"));
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert_eq!(captured.lock().unwrap().len(), 2);
        assert_eq!(sink.sent.load(Ordering::Relaxed), 1);
        assert_eq!(sink.dead_lettered.load(Ordering::Relaxed), 0);
        shutdown_tx.send(true).unwrap();
        worker.abort();
    }

    #[tokio::test]
    async fn dead_letters_on_400() {
        std::env::set_var("ADX_TEST_HMAC_KEY", "test-key-material");
        let (addr, captured) = spawn_receiver(vec![axum::http::StatusCode::BAD_REQUEST]).await;
        let (sink, receiver) =
            ObservationSink::new(&config(&format!("http://{addr}/events"))).unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let worker = {
            let sink = sink.clone();
            tokio::spawn(async move { sink.run(receiver, shutdown_rx).await })
        };
        sink.emit(test_event("evt-dead"));
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(captured.lock().unwrap().len(), 1);
        assert_eq!(sink.dead_lettered.load(Ordering::Relaxed), 1);
        assert_eq!(sink.sent.load(Ordering::Relaxed), 0);
        shutdown_tx.send(true).unwrap();
        worker.abort();
    }
}
