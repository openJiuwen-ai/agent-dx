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
    #[error("Jiuwen frontend output queue is full")]
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

fn emit(output: &mpsc::Sender<Value>, frame: Value) -> Result<(), DriverError> {
    output.try_send(frame).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => DriverError::SlowConsumer,
        mpsc::error::TrySendError::Closed(_) => DriverError::Disconnected,
    })
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
                let timed = !request.is_stream();
                let prepared = match session.submit(request) {
                    Ok(value) => value,
                    Err(error) => {
                        let code = if matches!(error, ProtocolError::Capacity(_)) { "TOO_MANY_REQUESTS" } else { "BAD_REQUEST" };
                        emit(&output, json!({"type":"res","id":client_id,"ok":false,"error":error.to_string(),"code":code}))?;
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
                // After this point even a failed flush may have delivered the request.
                tokio::time::timeout(config.write_timeout, backend.send(Message::Text(prepared.envelope.to_string())))
                    .await.map_err(|_| DriverError::Timeout)??;
                if let Some(ack) = prepared.acknowledgement { emit(&output, ack)?; }
            }
            incoming = backend.next() => {
                let Some(message) = incoming else { return Err(DriverError::Disconnected); };
                let message = message?;
                last_received = Instant::now();
                match message {
                    Message::Text(raw) => {
                        if raw.len() > MAX_FRAME_BYTES { return Err(ProtocolError::TooLarge.into()); }
                        let header: Value = serde_json::from_str(&raw)
                            .map_err(|_| ProtocolError::Invalid("invalid AgentServer JSON"))?;
                        if header["type"] == "event" && header["event"] == "connection.ack" {
                            if !header["payload"].is_object() {
                                return Err(ProtocolError::Invalid("invalid AgentServer connection ACK").into());
                            }
                            ready = true;
                            emit(&output, json!({"type":"event","event":"connection.ack","payload":header["payload"]}))?;
                            continue;
                        }
                        if !ready { return Err(ProtocolError::Invalid("AgentServer connection ACK required").into()); }
                        let response = Response::parse(raw.as_bytes())?;
                        let frame = session.receive(&response)?;
                        if !response.is_push() && response.is_final() {
                            if let Some(id) = response.request_id() { deadlines.remove(id); }
                        }
                        if let Some(frame) = frame { emit(&output, frame)?; }
                    }
                    Message::Ping(_) => {
                        // tungstenite already queued the matching pong.
                        tokio::time::timeout(config.write_timeout, backend.flush())
                            .await.map_err(|_| DriverError::Timeout)??;
                    }
                    Message::Pong(_) => {},
                    Message::Close(_) => return Err(DriverError::Disconnected),
                    _ => return Err(ProtocolError::Invalid("AgentServer requires text frames").into()),
                }
            }
            _ = tick.tick() => {
                let now = Instant::now();
                if (!ready && now.duration_since(started) >= config.idle_timeout) || now.duration_since(last_received) >= config.idle_timeout { return Err(DriverError::Timeout); }
                let expired: Vec<_> = deadlines.iter().filter(|(_, deadline)| **deadline <= now).map(|(id, _)| id.clone()).collect();
                for id in expired {
                    deadlines.remove(&id);
                    if let Some(frame) = session.timeout(&id) { emit(&output, frame)?; }
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
