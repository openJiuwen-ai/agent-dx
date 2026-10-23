//! One bounded frontend socket and its owned backend driver, cancelled together.
use super::{
    connection::ConnectionError,
    driver::DriverError,
    protocol::{ProtocolError, Request, MAX_FRAME_BYTES},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{future::Future, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
    time::Instant,
};
use tokio_tungstenite::{
    tungstenite::{self, protocol::frame::coding::CloseCode, protocol::CloseFrame, Message},
    WebSocketStream,
};

#[derive(Clone, Copy)]
pub struct FrontendConfig {
    pub queue_capacity: usize,
    pub write_timeout: Duration,
    pub idle_timeout: Duration,
    pub ping_interval: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum FrontendError {
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Backend(#[from] ConnectionError),
    #[error("Jiuwen frontend transport failed")]
    Transport(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("business session ended")]
    Session,
    #[error("Jiuwen frontend transport timed out")]
    Timeout,
    #[error("Jiuwen frontend request queue is full")]
    Capacity,
    #[error("Jiuwen backend output closed")]
    OutputClosed,
}

/// Drive an upgraded, bounded frontend socket together with its owned backend.
/// The caller authenticates and selects the binding before handing over the socket;
/// `backend` normally runs `Connection::run` with that verified user's identity.
/// No tasks are detached. Peer closure, backend failure, caller cancellation or a
/// stalled frontend drops both channel ends and the backend future/connection.
/// Invalid request frames receive correlated Jiuwen errors without reaching E2A.
/// Transport/queue failures close the socket; they never acknowledge or replay work.
pub async fn run<S, F, B>(
    socket: WebSocketStream<S>,
    config: FrontendConfig,
    backend: F,
) -> Result<(), FrontendError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(mpsc::Receiver<Request>, mpsc::Sender<Value>) -> B,
    B: Future<Output = Result<(), ConnectionError>>,
{
    run_with_auth(socket, config, backend, None).await
}

pub async fn run_authenticated<S, F, B>(
    socket: WebSocketStream<S>,
    config: FrontendConfig,
    backend: F,
    guard: crate::ingress::accounts::SessionGuard,
) -> Result<(), FrontendError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(mpsc::Receiver<Request>, mpsc::Sender<Value>) -> B,
    B: Future<Output = Result<(), ConnectionError>>,
{
    run_with_auth(socket, config, backend, Some(guard)).await
}

async fn run_with_auth<S, F, B>(
    mut socket: WebSocketStream<S>,
    config: FrontendConfig,
    backend: F,
    guard: Option<crate::ingress::accounts::SessionGuard>,
) -> Result<(), FrontendError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnOnce(mpsc::Receiver<Request>, mpsc::Sender<Value>) -> B,
    B: Future<Output = Result<(), ConnectionError>>,
{
    let now = Instant::now();
    let ws = socket.get_config();
    if config.queue_capacity == 0
        || config.queue_capacity > tokio::sync::Semaphore::MAX_PERMITS
        || [
            config.write_timeout,
            config.idle_timeout,
            config.ping_interval,
        ]
        .iter()
        .any(|duration| duration.is_zero() || now.checked_add(*duration).is_none())
        || config.ping_interval >= config.idle_timeout
        || ws
            .max_message_size
            .is_none_or(|limit| limit > MAX_FRAME_BYTES)
        || ws
            .max_frame_size
            .is_none_or(|limit| limit > MAX_FRAME_BYTES)
        || ws.max_write_buffer_size > MAX_FRAME_BYTES + 64 * 1024
    {
        return Err(ProtocolError::Invalid("invalid Jiuwen frontend limits").into());
    }
    let result = {
        let (requests, incoming) = mpsc::channel(config.queue_capacity);
        let (output, outgoing) = mpsc::channel(config.queue_capacity);
        let driver = backend(incoming, output);
        let transport = forward(&mut socket, config, requests, outgoing);
        let revoked = async {
            match &guard {
                Some(guard) => guard.watch().await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(driver, transport);
        tokio::select! {
            biased;
            _ = revoked => {
                let result = Err(FrontendError::Session);
                log_exit("auth_watch", &result);
                result
            },
            result = &mut driver => {
                let result = result.map_err(FrontendError::from);
                log_exit("backend", &result);
                result
            },
            result = &mut transport => {
                log_exit("frontend", &result);
                result
            },
        }
    };
    // Drop the losing future (including the E2A connection owner) before waiting
    // for the frontend close write; an unresponsive peer cannot retain ownership.
    let frame = CloseFrame {
        code: if result.is_ok() {
            CloseCode::Normal
        } else {
            CloseCode::Error
        },
        reason: if result.is_ok() {
            "connection closed"
        } else {
            "connection closed; in-flight results may be unknown"
        }
        .into(),
    };
    let close_code = u16::from(frame.code);
    match tokio::time::timeout(config.write_timeout, socket.close(Some(frame))).await {
        Ok(Ok(())) => tracing::info!(close_code, "Jiuwen frontend close written"),
        Ok(Err(error)) => tracing::warn!(
            close_code,
            transport_kind = transport_kind(&error),
            "Jiuwen frontend close failed"
        ),
        Err(_) => tracing::warn!(close_code, "Jiuwen frontend close timed out"),
    }
    result
}

// Log inside the winning branch, before the losing future/connection is dropped.
// Protocol messages are static; transport errors may contain payloads or HTTP
// headers, so only their category and numeric I/O details are logged.
fn log_exit(winner: &'static str, result: &Result<(), FrontendError>) {
    let Err(error) = result else {
        tracing::info!(winner, "Jiuwen WebSocket exited before cleanup");
        return;
    };
    let (error_kind, protocol, transport) = match error {
        FrontendError::Protocol(e) => ("frontend_protocol", Some(e), None),
        FrontendError::Transport(e) => ("frontend_transport", None, Some(e)),
        FrontendError::Backend(ConnectionError::Driver(DriverError::Protocol(e))) => {
            ("backend_protocol", Some(e), None)
        }
        FrontendError::Backend(ConnectionError::Driver(DriverError::Transport(e))) => {
            ("backend_transport", None, Some(e.as_ref()))
        }
        FrontendError::Backend(ConnectionError::Driver(DriverError::Disconnected)) => {
            ("backend_disconnected", None, None)
        }
        FrontendError::Backend(ConnectionError::Driver(DriverError::Timeout)) => {
            ("backend_timeout", None, None)
        }
        FrontendError::Backend(ConnectionError::Driver(DriverError::SlowConsumer)) => {
            ("output_queue_timeout", None, None)
        }
        FrontendError::Backend(ConnectionError::Revoked) => ("route_revoked", None, None),
        FrontendError::Backend(_) => ("backend_connect", None, None),
        FrontendError::Session => ("auth_revoked", None, None),
        FrontendError::Timeout => ("frontend_timeout", None, None),
        FrontendError::Capacity => ("request_queue_full", None, None),
        FrontendError::OutputClosed => ("output_closed", None, None),
    };
    let io_error = transport.and_then(|e| match e {
        tungstenite::Error::Io(e) => Some(e),
        _ => None,
    });
    tracing::warn!(winner, error_kind, protocol_error = ?protocol.map(ToString::to_string),
        transport_kind = ?transport.map(transport_kind), io_kind = ?io_error.map(|e| e.kind()),
        os_code = ?io_error.and_then(|e| e.raw_os_error()), "Jiuwen WebSocket exited before cleanup");
}

fn transport_kind(error: &tungstenite::Error) -> &'static str {
    match error {
        tungstenite::Error::Protocol(
            tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ) => "reset_without_closing_handshake",
        tungstenite::Error::Protocol(_) => "websocket_protocol",
        tungstenite::Error::Io(_) => "io",
        tungstenite::Error::Http(_) | tungstenite::Error::HttpFormat(_) => "http_handshake",
        tungstenite::Error::Capacity(_) | tungstenite::Error::WriteBufferFull(_) => "capacity",
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => "closed",
        _ => "other_transport",
    }
}

async fn forward<S>(
    socket: &mut WebSocketStream<S>,
    config: FrontendConfig,
    requests: mpsc::Sender<Request>,
    mut outgoing: mpsc::Receiver<Value>,
) -> Result<(), FrontendError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut last_received = Instant::now();
    let mut heartbeat = tokio::time::interval(config.ping_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    loop {
        tokio::select! {
            incoming = socket.next() => {
                let Some(message) = incoming else { return Ok(()); };
                let message = message?;
                last_received = Instant::now();
                match message {
                    Message::Text(raw) => match Request::parse(raw.as_bytes()) {
                        Ok(request) => requests.try_send(request).map_err(|error| match error {
                            mpsc::error::TrySendError::Full(_) => FrontendError::Capacity,
                            mpsc::error::TrySendError::Closed(_) => FrontendError::OutputClosed,
                        })?,
                        Err(error) => send(socket, config, error_frame(&raw, &error)).await?,
                    },
                    Message::Ping(_) => {
                        tokio::time::timeout(config.write_timeout, socket.flush()).await
                            .map_err(|_| FrontendError::Timeout)??;
                    },
                    Message::Pong(_) => {},
                    Message::Close(_) => return Ok(()),
                    _ => return Err(ProtocolError::Invalid("Jiuwen frontend requires text frames").into()),
                }
            }
            output = outgoing.recv() => {
                let Some(output) = output else { return Err(FrontendError::OutputClosed); };
                send(socket, config, output).await?;
            }
            _ = heartbeat.tick() => {
                if last_received.elapsed() >= config.idle_timeout { return Err(FrontendError::Timeout); }
                tokio::time::timeout(config.write_timeout, socket.send(Message::Ping(Vec::new()))).await
                    .map_err(|_| FrontendError::Timeout)??;
            }
        }
    }
}

async fn send<S>(
    socket: &mut WebSocketStream<S>,
    config: FrontendConfig,
    frame: Value,
) -> Result<(), FrontendError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let text = frame.to_string();
    if text.len() > MAX_FRAME_BYTES {
        return Err(ProtocolError::TooLarge.into());
    }
    tokio::time::timeout(config.write_timeout, socket.send(Message::Text(text)))
        .await
        .map_err(|_| FrontendError::Timeout)??;
    if matches!(
        frame["event"].as_str(),
        Some("chat.final" | "chat.error" | "chat.processing_status")
    ) {
        tracing::info!(event = ?frame["event"].as_str(),
            request_id = ?super::driver::log_field(frame["payload"]["request_id"].as_str()),
            session_id = ?super::driver::log_field(frame["payload"]["session_id"].as_str()),
            is_complete = ?frame["payload"]["is_complete"].as_bool(),
            is_processing = ?frame["payload"]["is_processing"].as_bool(), "Jiuwen terminal/status frame written");
    }
    Ok(())
}

fn error_frame(raw: &str, error: &ProtocolError) -> Value {
    let parsed = (raw.len() <= MAX_FRAME_BYTES)
        .then(|| serde_json::from_str::<Value>(raw).ok())
        .flatten();
    let id = parsed
        .as_ref()
        .and_then(|value| value["id"].as_str())
        .filter(|id| id.len() <= 256 && !id.chars().any(char::is_control))
        .unwrap_or("");
    json!({"type":"res", "id":id, "ok":false, "error":error.to_string(), "code":"BAD_REQUEST"})
}
