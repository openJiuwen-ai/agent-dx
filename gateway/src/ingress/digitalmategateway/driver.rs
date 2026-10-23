//! A single E2A socket owner. No business replay or hidden reconnect loop.
use super::protocol::{ProtocolError, Request, MAX_FRAME_BYTES};
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
                let prepared = match session.submit(request) {
                    Ok(value) => value,
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
                tokio::time::timeout(config.write_timeout, backend.send(Message::Text(prepared.envelope.to_string())))
                    .await.map_err(|_| DriverError::Timeout)??;
                if let Some(ack) = prepared.acknowledgement { emit(&output, ack, config.output_queue_timeout).await?; }
            }
            incoming = backend.next() => {
                let Some(message) = incoming else {
                    tracing::warn!(?last_response, pending = session.pending_len(), "Jiuwen backend EOF");
                    return Err(DriverError::Disconnected);
                };
                let message = message.inspect_err(|_| {
                    tracing::warn!(?last_response, pending = session.pending_len(), "Jiuwen backend read failed");
                })?;
                last_received = Instant::now();
                match message {
                    Message::Text(raw) => {
                        if raw.len() > MAX_FRAME_BYTES { return Err(ProtocolError::TooLarge.into()); }
                        let header: Value = serde_json::from_str(&raw)
                            .map_err(|error| {
                                tracing::warn!(json_category = ?error.classify(), line = error.line(), column = error.column(), "Jiuwen backend JSON decode failed");
                                ProtocolError::Invalid("invalid AgentServer JSON")
                            })?;
                        if header["type"] == "event" && header["event"] == "connection.ack" {
                            if !header["payload"].is_object() {
                                return Err(ProtocolError::Invalid("invalid AgentServer connection ACK").into());
                            }
                            legacy_agentserver_ready = header["payload"]["status"] == "ready"
                                && header["payload"]["heartbeat_job_owner"] == "agentserver"
                                && header["payload"].get("readiness").is_none();
                            ready = true;
                            emit(&output, json!({"type":"event","event":"connection.ack","payload":header["payload"]}), config.output_queue_timeout).await?;
                            continue;
                        }
                        if !ready { return Err(ProtocolError::Invalid("AgentServer connection ACK required").into()); }
                        let response = Response::parse(raw.as_bytes()).inspect_err(|error| log_response_error("response_parse", &header, error))?;
                        last_response = Some((log_field(response.request_id()).unwrap_or("").to_owned(), response.sequence(), response.is_final()));
                        let frame = session.receive(&response).inspect_err(|error| log_response_error("response_projection", &header, error))?;
                        if !response.is_push() && response.is_final() {
                            if let Some(id) = response.request_id() { deadlines.remove(id); }
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
                        tokio::time::timeout(config.write_timeout, backend.flush())
                            .await.map_err(|_| DriverError::Timeout)??;
                    }
                    Message::Pong(_) => {},
                    Message::Close(frame) => {
                        tracing::info!(close_code = ?frame.as_ref().map(|f| u16::from(f.code)), ?last_response,
                            pending = session.pending_len(), "Jiuwen backend peer close received");
                        return Err(DriverError::Disconnected);
                    },
                    _ => return Err(ProtocolError::Invalid("AgentServer requires text frames").into()),
                }
            }
            _ = tick.tick() => {
                let now = Instant::now();
                if (!ready && now.duration_since(started) >= config.idle_timeout) || now.duration_since(last_received) >= config.idle_timeout { return Err(DriverError::Timeout); }
                let expired: Vec<_> = deadlines.iter().filter(|(_, deadline)| **deadline <= now).map(|(id, _)| id.clone()).collect();
                for id in expired {
                    deadlines.remove(&id);
                    if let Some(frame) = session.timeout(&id) { emit(&output, frame, config.output_queue_timeout).await?; }
                }
                if now.duration_since(last_ping) >= config.ping_interval {
                    tokio::time::timeout(config.write_timeout, backend.send(Message::Ping(Vec::new())))
                        .await.map_err(|_| DriverError::Timeout)??;
                    last_ping = now;
                }
            }
        }
    }
}
