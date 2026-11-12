//! A single E2A socket owner. No business replay or hidden reconnect loop.
use super::protocol::{Method, ProtocolError, Request, MAX_FRAME_BYTES};
use super::response::Response;
use super::session::{Session, SessionLimits};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{collections::HashMap, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
    time::Instant,
};
use tokio_tungstenite::{
    tungstenite::{protocol::WebSocketConfig, Message},
    WebSocketStream,
};

pub struct DriverConfig {
    pub limits: SessionLimits,
    pub write_timeout: Duration,
    pub output_queue_timeout: Duration,
    pub idle_timeout: Duration,
    pub ping_interval: Duration,
    /// Unary/control wait budget; chat/history streams are long-lived.
    pub request_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("AgentServer transport failed: {0}")]
    Transport(Box<tokio_tungstenite::tungstenite::Error>),
    #[error("AgentServer disconnected; in-flight results may be unknown")]
    Disconnected,
    #[error("AgentServer transport timed out; in-flight results may be unknown")]
    Timeout,
    #[error("Jiuwen frontend output queue remained full until timeout")]
    SlowConsumer,
}

impl From<tokio_tungstenite::tungstenite::Error> for DriverError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Transport(Box::new(error))
    }
}

/// Apply at WS handshake/raw-socket construction, before accepting any frames.
pub fn websocket_config() -> WebSocketConfig {
    WebSocketConfig {
        max_message_size: Some(MAX_FRAME_BYTES),
        max_frame_size: Some(MAX_FRAME_BYTES),
        max_write_buffer_size: MAX_FRAME_BYTES + 64 * 1024,
        ..WebSocketConfig::default()
    }
}

async fn emit(
    output: &mpsc::Sender<Value>,
    frame: Value,
    queue_timeout: Duration,
) -> Result<(), DriverError> {
    tokio::time::timeout(queue_timeout, output.send(frame))
        .await
        .map_err(|_| DriverError::SlowConsumer)?
        .map_err(|_| DriverError::Disconnected)
}

// Diagnostic fields only; do not format response bodies or tool arguments.
pub(super) fn log_field(value: Option<&str>) -> Option<&str> {
    value.filter(|s| {
        s.len() <= 256
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b':'))
    })
}

fn log_response_error(phase: &'static str, header: &Value, error: &ProtocolError) {
    tracing::warn!(phase, error = %error,
        backend_request_id = ?log_field(header["request_id"].as_str()),
        session_id = ?log_field(header["session_id"].as_str()),
        response_kind = ?log_field(header["response_kind"].as_str()),
        status = ?log_field(header["status"].as_str()), sequence = ?header["sequence"].as_i64(),
        is_final = ?header["is_final"].as_bool(), is_push = ?header["metadata"]["_jiuwenswarm_server_push"].as_bool(),
        result_is_object = header["body"]["result"].is_object(), "Jiuwen response rejected");
}

/// Run an already-authorized, bounded E2A socket for one frontend attachment.
/// All methods reuse this socket. Queue producers must use bounded channels;
/// backend loss, write timeout or slow frontend ends this driver, drops the socket
/// and closes output. The frontend owner must then close its WS so beegent reports
/// unknown outcomes. No request is replayed, even if the backend may not have read it.
/// Connection ACK admits Front RPCs; it is never treated as Agent Runtime readiness.
/// Returns configuration/protocol, transport, timeout or output-backpressure errors.
pub async fn run<S>(
    mut backend: WebSocketStream<S>,
    user_id: String,
    config: DriverConfig,
    mut requests: mpsc::Receiver<Request>,
    output: mpsc::Sender<Value>,
    anchor: Option<crate::ingress::experience::ExperienceAnchor>,
) -> Result<(), DriverError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let now = Instant::now();
    let durations = [
        config.write_timeout,
        config.output_queue_timeout,
        config.idle_timeout,
        config.ping_interval,
        config.request_timeout,
    ];
    let ws_config = backend.get_config();
    if durations
        .iter()
        .any(|d| d.is_zero() || now.checked_add(*d).is_none())
        || config.ping_interval >= config.idle_timeout
        || ws_config
            .max_message_size
            .is_none_or(|n| n > MAX_FRAME_BYTES)
        || ws_config.max_frame_size.is_none_or(|n| n > MAX_FRAME_BYTES)
        || ws_config.max_write_buffer_size > MAX_FRAME_BYTES + 64 * 1024
    {
        return Err(ProtocolError::Invalid("invalid Jiuwen driver limits").into());
    }
    // 体验族观察：anchor 为 None（观测关闭）时全部 no-op；
    // Drop 守卫保证 driver 返回或被取消时，遗留 open 体验按退出原因清场。
    let mut observation = crate::ingress::experience::DriverObservation::new(anchor);
    let mut session = Session::new(user_id, config.limits)?;
    let mut deadlines = HashMap::<String, Instant>::new();
    let mut ready = false;
    let mut legacy_agentserver_ready = false;
    let mut last_response = None;
    let started = now;
    let mut last_received = now;
    let mut tick = tokio::time::interval(config.ping_interval.min(config.request_timeout));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_ping = now;
    loop {
        tokio::select! {
            _ = output.closed() => return Ok(()),
            request = requests.recv(), if ready => {
                let Some(request) = request else { return Ok(()); };
                let client_id = request.id().to_owned();
                // JiuwenSwarm's legacy single-process server handled this method in its
                // own Gateway. When connected directly it otherwise falls through to
                // agent execution. New servers expose explicit readiness and own the RPC.
                if legacy_agentserver_ready && request.is_health_check_get_conf() {
                    emit(&output, json!({
                        "type": "res",
                        "id": client_id,
                        "ok": true,
                        "payload": {
                            "ok": true,
                            "state": "AGENT_READY",
                            "transport_ready": true,
                            "control_ready": true,
                            "runtime_warming": true,
                            "agent_ready": true,
                            "draining": false,
                            "failed": false,
                            "failed_reason": null,
                            "last_error": null,
                            "retrying": false,
                            "capabilities": {
                                "transport": true,
                                "control": true,
                                "execution": true
                            }
                        }
                    }), config.output_queue_timeout).await?;
                    continue;
                }
                let timed = !request.is_stream();
                let is_chat = request.method() == Method::Chat;
                let session_id = request.session_id().map(str::to_owned);
                let prepared = match session.submit(request) {
                    Ok(value) => {
                        // H3：Chat 根请求接受即开体验并发射 experience_accepted；
                        // submit 失败不开（未接受）。体验起点：连接上首个 Chat 用
                        // anchor t0（含连接建立），后续用 submit 时刻。
                        if is_chat {
                            if let Some(id) = value.envelope["request_id"].as_str() {
                                observation.task_opened(
                                    id,
                                    session_id.as_deref(),
                                    Instant::now(),
                                );
                            }
                        }
                        value
                    }
                    Err(error) => {
                        let code = if matches!(error, ProtocolError::Capacity(_)) { "TOO_MANY_REQUESTS" } else { "BAD_REQUEST" };
                        emit(&output, json!({"type":"res","id":client_id,"ok":false,"error":error.to_string(),"code":code}), config.output_queue_timeout).await?;
                        continue;
                    }
                };
                if timed {
                    let id = prepared.envelope["request_id"].as_str()
                        .ok_or(ProtocolError::Invalid("missing prepared request ID"))?;
                    let deadline = Instant::now().checked_add(config.request_timeout)
                        .ok_or(ProtocolError::Invalid("request deadline overflow"))?;
                    deadlines.insert(id.into(), deadline);
                }
                tracing::info!(client_request_id = ?log_field(Some(&client_id)),
                    backend_request_id = ?log_field(prepared.envelope["request_id"].as_str()),
                    method = ?log_field(prepared.envelope["method"].as_str()),
                    session_id = ?log_field(prepared.envelope["session_id"].as_str()), timed,
                    "Jiuwen request mapped before backend write");
                // After this point even a failed flush may have delivered the request.
                match tokio::time::timeout(
                    config.write_timeout,
                    backend.send(Message::Text(prepared.envelope.to_string())),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        observation.backend_exit();
                        return Err(error.into());
                    }
                    Err(_) => {
                        observation.backend_exit();
                        return Err(DriverError::Timeout);
                    }
                }
                if let Some(ack) = prepared.acknowledgement { emit(&output, ack, config.output_queue_timeout).await?; }
            }
            incoming = backend.next() => {
                let Some(message) = incoming else {
                    tracing::warn!(?last_response, pending = session.pending_len(), "Jiuwen backend EOF");
                    observation.backend_exit();
                    return Err(DriverError::Disconnected);
                };
                let message = match message.inspect_err(|_| {
                    tracing::warn!(?last_response, pending = session.pending_len(), "Jiuwen backend read failed");
                }) {
                    Ok(message) => message,
                    Err(error) => {
                        observation.backend_exit();
                        return Err(error.into());
                    }
                };
                last_received = Instant::now();
                match message {
                    Message::Text(raw) => {
                        if raw.len() > MAX_FRAME_BYTES {
                            observation.backend_exit();
                            return Err(ProtocolError::TooLarge.into());
                        }
                        let header: Value = match serde_json::from_str(&raw) {
                            Ok(value) => value,
                            Err(error) => {
                                tracing::warn!(json_category = ?error.classify(), line = error.line(), column = error.column(), "Jiuwen backend JSON decode failed");
                                observation.backend_exit();
                                return Err(ProtocolError::Invalid("invalid AgentServer JSON").into());
                            }
                        };
                        if header["type"] == "event" && header["event"] == "connection.ack" {
                            if !header["payload"].is_object() {
                                observation.backend_exit();
                                return Err(ProtocolError::Invalid("invalid AgentServer connection ACK").into());
                            }
                            legacy_agentserver_ready = header["payload"]["status"] == "ready"
                                && header["payload"]["heartbeat_job_owner"] == "agentserver"
                                && header["payload"].get("readiness").is_none();
                            ready = true;
                            emit(&output, json!({"type":"event","event":"connection.ack","payload":header["payload"]}), config.output_queue_timeout).await?;
                            continue;
                        }
                        if !ready {
                            observation.backend_exit();
                            return Err(ProtocolError::Invalid("AgentServer connection ACK required").into());
                        }
                        let response = match Response::parse(raw.as_bytes()) {
                            Ok(response) => response,
                            Err(error) => {
                                log_response_error("response_parse", &header, &error);
                                observation.backend_exit();
                                return Err(error.into());
                            }
                        };
                        last_response = Some((log_field(response.request_id()).unwrap_or("").to_owned(), response.sequence(), response.is_final()));
                        let frame = match session.receive(&response) {
                            Ok(frame) => frame,
                            Err(error) => {
                                log_response_error("response_projection", &header, &error);
                                observation.backend_exit();
                                return Err(error.into());
                            }
                        };
                        if !response.is_push() && response.is_final() {
                            if let Some(id) = response.request_id() { deadlines.remove(id); }
                        }
                        // H4：投影帧按 backend_id 归因分类，记录从用户发送请求到输出首字耗时埋点
                        if let Some(frame) = &frame {
                            observation.frame_seen(response.request_id(), frame, Instant::now());
                        }
                        // H5：非 push 终态、或 push 终态哨兵（frame 为 None）闭合
                        // 记录从网关受理 → 收到终态的端到端任务耗时(不包含认证、绑定/冷启动等过程)
                        // 体验并发射 task_terminal；不对应 open 体验时 no-op。
                        if response.is_final() && (!response.is_push() || frame.is_none()) {
                            observation.task_closed(
                                response.request_id(),
                                crate::ingress::experience::Close::Terminal,
                                Instant::now(),
                            );
                        }
                        let projected = frame.is_some();
                        if let Some(frame) = frame {
                            emit(&output, frame, config.output_queue_timeout).await.inspect_err(|error| {
                                tracing::warn!(error = %error, ?last_response, "Jiuwen output enqueue failed");
                            })?;
                        }
                        if response.is_final() {
                            tracing::info!(?last_response, projected, is_push = response.is_push(),
                                pending = session.pending_len(), "Jiuwen terminal response handled");
                        }
                    }
                    Message::Ping(_) => {
                        // tungstenite already queued the matching pong.
                        match tokio::time::timeout(config.write_timeout, backend.flush()).await {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                observation.backend_exit();
                                return Err(error.into());
                            }
                            Err(_) => {
                                observation.backend_exit();
                                return Err(DriverError::Timeout);
                            }
                        }
                    }
                    Message::Pong(_) => {},
                    Message::Close(frame) => {
                        tracing::info!(close_code = ?frame.as_ref().map(|f| u16::from(f.code)), ?last_response,
                            pending = session.pending_len(), "Jiuwen backend peer close received");
                        observation.backend_exit();
                        return Err(DriverError::Disconnected);
                    },
                    _ => {
                        observation.backend_exit();
                        return Err(ProtocolError::Invalid("AgentServer requires text frames").into());
                    }
                }
            }
            _ = tick.tick() => {
                let now = Instant::now();
                if (!ready && now.duration_since(started) >= config.idle_timeout) || now.duration_since(last_received) >= config.idle_timeout {
                    observation.backend_exit();
                    return Err(DriverError::Timeout);
                }
                let expired: Vec<_> = deadlines.iter().filter(|(_, deadline)| **deadline <= now).map(|(id, _)| id.clone()).collect();
                for id in expired {
                    deadlines.remove(&id);
                    // H5：deadline 到期即闭合（结果未知，不进 unresolved）；Chat 流
                    // 当前无 deadline，此路径为兜底。先闭合再尝试下发超时帧。
                    observation.task_closed(
                        Some(&id),
                        crate::ingress::experience::Close::Unknown,
                        Instant::now(),
                    );
                    if let Some(frame) = session.timeout(&id) { emit(&output, frame, config.output_queue_timeout).await?; }
                }
                if now.duration_since(last_ping) >= config.ping_interval {
                    match tokio::time::timeout(config.write_timeout, backend.send(Message::Ping(Vec::new()))).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            observation.backend_exit();
                            return Err(error.into());
                        }
                        Err(_) => {
                            observation.backend_exit();
                            return Err(DriverError::Timeout);
                        }
                    }
                    last_ping = now;
                }
            }
        }
    }
}

#[cfg(test)]
mod experience_tests {
    //! 体验族直埋的 driver 级验证（详设 §6.4 M2-4a）：duplex＋raw WebSocket
    //! 模拟后端，跑完整 driver 循环，断言 H3/H4/H5 的指标落点。
    use super::*;
    use crate::ingress::experience::{ExperienceAnchor, ExperienceMetrics};
    use adx_observability::metrics::Text;
    use serde_json::json;
    use std::sync::Arc;
    use tokio_tungstenite::tungstenite::protocol::Role;

    fn test_config() -> DriverConfig {
        DriverConfig {
            limits: SessionLimits {
                pending: 8,
                recent: 16,
                busy: 16,
            },
            write_timeout: Duration::from_secs(5),
            output_queue_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            ping_interval: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
        }
    }

    /// 一条合法的 E2A 响应帧（[response.rs](super::super::response) 的线格式）。
    fn e2a(
        request_id: &str,
        sequence: i64,
        is_final: bool,
        status: &str,
        kind: &str,
        body: Value,
    ) -> String {
        json!({
            "protocol_version": "1.0",
            "request_id": request_id,
            "sequence": sequence,
            "is_final": is_final,
            "status": status,
            "response_kind": kind,
            "body": body,
        })
        .to_string()
    }

    fn render(metrics: &ExperienceMetrics) -> String {
        let mut text = Text::default();
        metrics.render(&mut text);
        text.finish()
    }

    /// 取样本 `name{labels}` 的值；`None` 表示样本不存在。
    fn sample_value(text: &str, sample: &str) -> Option<u64> {
        text.lines()
            .find(|line| {
                line.starts_with(sample) && line.as_bytes().get(sample.len()) == Some(&b' ')
            })
            .and_then(|line| line.split(' ').nth(1))
            .and_then(|value| value.parse().ok())
    }

    struct Harness {
        requests: mpsc::Sender<Request>,
        output: mpsc::Receiver<Value>,
        backend: WebSocketStream<tokio::io::DuplexStream>,
        acked: bool,
        driver: tokio::task::JoinHandle<Result<(), DriverError>>,
        metrics: Arc<ExperienceMetrics>,
    }

    /// 起一对 raw WebSocket（无握手）模拟 AgentServer，跑完整 driver 循环。
    async fn setup() -> Harness {
        setup_inner(None).await
    }

    async fn setup_inner(
        sink: Option<Arc<crate::ingress::observability::ObservationSink>>,
    ) -> Harness {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        let socket =
            WebSocketStream::from_raw_socket(client_io, Role::Client, Some(websocket_config()))
                .await;

        let backend =
            WebSocketStream::from_raw_socket(server_io, Role::Server, Some(websocket_config()))
                .await;

        let (requests, requests_rx) = mpsc::channel(4);
        let (output_tx, output) = mpsc::channel(64);
        let metrics = Arc::new(ExperienceMetrics::default());
        let anchor = ExperienceAnchor::new(
            Arc::clone(&metrics),
            sink,
            "tpl@1",
            Instant::now(),
            "tenant-1",
            "user-1",
        );
        let driver = tokio::spawn(run(
            socket,
            "user".into(),
            test_config(),
            requests_rx,
            output_tx,
            Some(anchor),
        ));
        Harness {
            requests,
            output,
            backend,
            acked: false,
            driver,
            metrics,
        }
    }

    impl Harness {
        /// 首次调用先发 connection.ack；提交一条请求并返回映射后的后端 ID。
        async fn submit(&mut self, request: &str) -> String {
            if !self.acked {
                self.backend
                    .send(Message::Text(
                        r#"{"type":"event","event":"connection.ack","payload":{"status":"ready"}}"#
                            .into(),
                    ))
                    .await
                    .unwrap();
                self.acked = true;
            }
            self.requests
                .send(Request::parse(request.as_bytes()).unwrap())
                .await
                .unwrap();
            let envelope = match self.backend.next().await.unwrap().unwrap() {
                Message::Text(text) => serde_json::from_str::<Value>(&text).unwrap(),
                other => panic!("expected request envelope, got {other:?}"),
            };
            envelope["request_id"].as_str().unwrap().to_owned()
        }

        async fn output_frame(&mut self) -> Value {
            tokio::time::timeout(Duration::from_secs(5), self.output.recv())
                .await
                .expect("output frame within 5s")
                .unwrap()
        }
    }

    /// 正常路径：chat.send → 文本 delta（first_event/first_text/first_use）→
    /// complete 终态（闭合、inflight 归零、无 unresolved）。
    #[tokio::test]
    async fn chat_experience_records_milestones_and_closes() {
        let mut harness = setup().await;
        let backend_id = harness
            .submit(
                r#"{"type":"req","id":"c1","method":"chat.send","params":{"session_id":"s1","query":"hi"}}"#,
            )
            .await;
        harness
            .backend
            .send(Message::Text(e2a(
                &backend_id,
                1,
                false,
                "in_progress",
                "e2a.chunk",
                json!({"delta_kind":"text","delta":"hello"}),
            )))
            .await
            .unwrap();
        harness
            .backend
            .send(Message::Text(e2a(
                &backend_id,
                2,
                true,
                "succeeded",
                "e2a.complete",
                json!({"result":{"done":true}}),
            )))
            .await
            .unwrap();
        let mut seen = Vec::new();
        for _ in 0..4 {
            let frame = harness.output_frame().await;
            seen.push(frame["event"].as_str().unwrap_or("res").to_owned());
        }
        assert_eq!(seen, ["connection.ack", "res", "chat.delta", "chat.final"]);
        let Harness {
            requests,
            output,
            backend,
            driver,
            metrics,
            ..
        } = harness;
        drop(requests);
        drop(output);
        assert!(driver.await.unwrap().is_ok());
        drop(backend);
        let text = render(&metrics);
        assert_eq!(
            sample_value(
                &text,
                "adx_business_first_output_seconds_count{template_release=\"tpl@1\",\
                 output_kind=\"first_event\",start_mode=\"unknown\"}"
            ),
            Some(1)
        );
        assert_eq!(
            sample_value(
                &text,
                "adx_business_first_output_seconds_count{template_release=\"tpl@1\",\
                 output_kind=\"first_text\",start_mode=\"unknown\"}"
            ),
            Some(1)
        );
        assert_eq!(
            sample_value(
                &text,
                "adx_experience_phase_duration_seconds_count{phase=\"first_use\",\
                 outcome=\"success\",template_release=\"tpl@1\"}"
            ),
            Some(1)
        );
        assert_eq!(
            sample_value(&text, "adx_business_tasks_inflight{start_mode=\"unknown\"}"),
            Some(0)
        );
        assert_eq!(
            sample_value(
                &text,
                "adx_business_tasks_unresolved{reason=\"backend_gone\"}"
            ),
            Some(0)
        );
        assert_eq!(
            sample_value(
                &text,
                "adx_business_tasks_unresolved{reason=\"client_gone\"}"
            ),
            Some(0)
        );
    }

    /// M2-4b：正常聊天流发射 accepted → first_output → terminal 三条事件，
    /// 脱敏引用为 HMAC、终态带成功结果与时长。
    #[tokio::test]
    async fn chat_flow_emits_business_events() {
        use adx_observability::business::{EventType, Outcome};
        std::env::set_var("ADX_TEST_DRIVER_HMAC_KEY", "test-key-material");
        let emit = crate::ingress::observability::EmitConfig {
            endpoint: "http://127.0.0.1:1/events".to_owned(),
            token: "service-token".to_owned(),
            hmac_key_env: "ADX_TEST_DRIVER_HMAC_KEY".to_owned(),
            spool_capacity: 16,
            batch_size: 20,
            flush_interval_millis: 1_000,
            max_retry_window_seconds: 1,
        };
        let (sink, mut events) =
            crate::ingress::observability::ObservationSink::new(&emit).unwrap();
        let mut harness = setup_inner(Some(sink)).await;
        let backend_id = harness
            .submit(
                r#"{"type":"req","id":"c1","method":"chat.send","params":{"session_id":"s1","query":"hi"}}"#,
            )
            .await;
        harness
            .backend
            .send(Message::Text(e2a(
                &backend_id,
                1,
                false,
                "in_progress",
                "e2a.chunk",
                json!({"delta_kind":"text","delta":"hello"}),
            )))
            .await
            .unwrap();
        harness
            .backend
            .send(Message::Text(e2a(
                &backend_id,
                2,
                true,
                "succeeded",
                "e2a.complete",
                json!({"result":{"done":true}}),
            )))
            .await
            .unwrap();
        for _ in 0..4 {
            harness.output_frame().await;
        }
        let Harness {
            requests,
            output,
            backend,
            driver,
            ..
        } = harness;
        drop(requests);
        drop(output);
        assert!(driver.await.unwrap().is_ok());
        drop(backend);

        let mut emitted = Vec::new();
        while let Ok(event) = events.try_recv() {
            emitted.push(event);
        }
        assert_eq!(emitted.len(), 3);
        assert_eq!(emitted[0].event_type, EventType::ExperienceAccepted);
        assert_eq!(emitted[1].event_type, EventType::FirstOutput);
        assert_eq!(emitted[2].event_type, EventType::TaskTerminal);
        // 协议正常结束没有业务终态证据 → unknown（C01），不冒充成功。
        assert_eq!(emitted[2].outcome, Some(Outcome::Unknown));
        assert_eq!(emitted[0].experience_id, backend_id);
        assert_eq!(emitted[2].experience_id, backend_id);
        assert_eq!(emitted[0].tenant_ref, "tenant-1");
        assert_eq!(emitted[0].user_ref.as_deref().map(str::len), Some(64));
        assert_eq!(emitted[0].session_ref.as_deref().map(str::len), Some(64));
        assert_eq!(emitted[0].event_id, format!("{backend_id}:accepted"));
        assert!(emitted[2].duration_seconds.unwrap_or(-1.0) >= 0.0);
    }

    /// 后端 EOF：遗留 open 体验按 backend_gone 清场。
    #[tokio::test]
    async fn backend_eof_sweeps_open_experience_as_backend_gone() {
        let mut harness = setup().await;
        let backend_id = harness
            .submit(
                r#"{"type":"req","id":"c1","method":"chat.send","params":{"session_id":"s1","query":"hi"}}"#,
            )
            .await;
        // 首输出已到、终态未到。
        harness
            .backend
            .send(Message::Text(e2a(
                &backend_id,
                1,
                false,
                "in_progress",
                "e2a.chunk",
                json!({"delta_kind":"text","delta":"hello"}),
            )))
            .await
            .unwrap();
        let _ = harness.output_frame().await;
        let _ = harness.output_frame().await;
        // 后端主动 Close（带握手）→ driver 以 Disconnected 退出。
        harness.backend.send(Message::Close(None)).await.unwrap();
        drop(harness.backend);
        let result = harness.driver.await.unwrap();
        assert!(matches!(result, Err(DriverError::Disconnected)));
        let text = render(&harness.metrics);
        assert_eq!(
            sample_value(
                &text,
                "adx_business_tasks_unresolved{reason=\"backend_gone\"}"
            ),
            Some(1)
        );
        assert_eq!(
            sample_value(&text, "adx_business_tasks_inflight{start_mode=\"unknown\"}"),
            Some(0)
        );
    }

    /// 前端侧退出（请求与输出通道关闭）：遗留 open 体验按 client_gone 清场。
    #[tokio::test]
    async fn frontend_close_sweeps_open_experience_as_client_gone() {
        let mut harness = setup().await;
        harness
            .submit(
                r#"{"type":"req","id":"c1","method":"chat.send","params":{"session_id":"s1","query":"hi"}}"#,
            )
            .await;
        let _ = harness.output_frame().await;
        let Harness {
            requests,
            output,
            backend,
            driver,
            metrics,
            ..
        } = harness;
        drop(requests);
        drop(output);
        assert!(driver.await.unwrap().is_ok());
        drop(backend);
        let text = render(&metrics);
        assert_eq!(
            sample_value(
                &text,
                "adx_business_tasks_unresolved{reason=\"client_gone\"}"
            ),
            Some(1)
        );
        assert_eq!(
            sample_value(&text, "adx_business_tasks_inflight{start_mode=\"unknown\"}"),
            Some(0)
        );
    }

    /// 非 Chat 请求（session.list）不开体验：无首输出样本、inflight 恒 0。
    #[tokio::test]
    async fn non_chat_requests_do_not_open_experiences() {
        let mut harness = setup().await;
        let backend_id = harness
            .submit(r#"{"type":"req","id":"c1","method":"session.list","params":{}}"#)
            .await;
        harness
            .backend
            .send(Message::Text(e2a(
                &backend_id,
                1,
                true,
                "succeeded",
                "e2a.complete",
                json!({"result":{"sessions":[]}}),
            )))
            .await
            .unwrap();
        // 首帧是连接 ack 的回显，其后才是 session.list 的应答。
        let ack = harness.output_frame().await;
        assert_eq!(ack["event"], "connection.ack");
        let frame = harness.output_frame().await;
        assert_eq!(frame["type"], "res");
        assert_eq!(frame["ok"], true);
        let Harness {
            requests,
            output,
            backend,
            driver,
            metrics,
            ..
        } = harness;
        drop(requests);
        drop(output);
        assert!(driver.await.unwrap().is_ok());
        drop(backend);
        let text = render(&metrics);
        assert_eq!(
            sample_value(&text, "adx_business_tasks_inflight{start_mode=\"unknown\"}"),
            Some(0)
        );
        // 稠密导出：样本存在但计数为 0（无任何里程碑发生）。
        assert_eq!(
            sample_value(
                &text,
                "adx_business_first_output_seconds_count{template_release=\"tpl@1\",\
                 output_kind=\"first_event\",start_mode=\"unknown\"}"
            ),
            Some(0)
        );
        assert_eq!(
            sample_value(
                &text,
                "adx_experience_phase_duration_seconds_count{phase=\"first_use\",\
                 outcome=\"success\",template_release=\"tpl@1\"}"
            ),
            Some(0)
        );
    }
}
