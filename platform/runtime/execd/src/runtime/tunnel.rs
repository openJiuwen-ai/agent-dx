// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
// See the LICENSE file in this repository for the complete license text.

//! Native Rust reverse-tunnel server (replaces spawning the python tunnel_server).
//!
//! Port A (ws_port, 0.0.0.0): WebSocket endpoint the external TunnelClient connects to.
//! Port B (http_port, 127.0.0.1): HTTP/WS surface the sandbox's own code hits;
//! each request is framed and forwarded over the Port-A WS to the client, which
//! relays it to the real upstream and frames the response back.
//!
//! The paired sandbox-sdk TunnelClient keeps metadata and small bodies in JSON
//! text frames. After V2 hello negotiation, large HTTP bodies use bounded raw
//! binary envelopes with byte-credit backpressure, while WebSocket binary
//! messages use the same bounded raw envelope without base64. Headers remain
//! ordered [name, value] pairs. V1 framing stays
//! available for mixed-version peers and the replayable small-body fast path.

use base64::Engine;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited, StreamBody};
use hyper::body::{Body as _, Frame as BodyFrame, Incoming};
use hyper::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_LENGTH, UPGRADE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::error::Error as StdError;
use std::fmt::Write as _;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{
    mpsc, oneshot, watch, Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore,
};
use tokio::task::AbortHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};

const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(600);
const WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Legacy V1 in-flight HTTP requests keep the historical bounded replay TTL.
/// Resumable V2 uses the configured HTTP timeout instead.
const PENDING_REQUEST_TTL: Duration = Duration::from_secs(120);
const MAX_HTTP_HEADER_BYTES: usize = 64 * 1024;
const MAX_HTTP_HEADERS: usize = 200;
const MAX_HTTP_BODY_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_MAX_WS_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const TUNNEL_PROTOCOL_VERSION: u8 = 2;
const BINARY_ENVELOPE_VERSION: u8 = 1;
const BINARY_MAGIC: [u8; 2] = *b"YD";
const DEFAULT_STREAM_CHUNK_BYTES: usize = 64 * 1024;
const MIN_STREAM_CHUNK_BYTES: usize = 1024;
const DEFAULT_MAX_INFLIGHT: usize = 16;
const DEFAULT_STREAM_WINDOW_FRAMES: usize = 16;
const FAST_PATH_BODY_BYTES: u64 = 64 * 1024;
/// A V1 body is base64-encoded into one JSON frame. Keep the encoded frame
/// safely below the control WebSocket's fixed 8 MiB message limit.
const MAX_V1_BODY_BYTES: usize = 5 * 1024 * 1024;
const MAX_CONFIGURED_BODY_BYTES: usize = 1024 * 1024 * 1024;
const MAX_CONFIGURED_WS_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_CONFIGURED_STREAM_CHUNK_BYTES: usize = 64 * 1024;
const MAX_CONFIGURED_INFLIGHT: usize = 1024;
const MAX_CONFIGURED_WINDOW_FRAMES: usize = 1024;
const BINARY_HEADER_BYTES: usize = 26;
const BINARY_END_OF_BODY: u8 = 0x01;
const BINARY_HAS_OFFSET: u8 = 0x02;
const OUTBOUND_QUEUE_FRAMES: usize = 512;
const OUTBOUND_CONTROL_RESERVE: usize = 32;
const CONTROL_WS_MESSAGE_BYTES: usize = 8 * 1024 * 1024;
const TERMINATED_STREAM_TTL: Duration = Duration::from_secs(30);
const TERMINATED_STREAM_LIMIT: usize = 1024;

type HeaderList = Vec<(String, String)>;

fn deserialize_headers<'de, D>(deserializer: D) -> Result<HeaderList, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Headers {
        Ordered(HeaderList),
        Legacy(std::collections::BTreeMap<String, String>),
    }
    Ok(match Headers::deserialize(deserializer)? {
        Headers::Ordered(headers) => headers,
        Headers::Legacy(headers) => headers.into_iter().collect(),
    })
}
type BoxError = Box<dyn StdError + Send + Sync>;
type TunnelBody = UnsyncBoxBody<Bytes, BoxError>;

fn positive_env_usize(name: &str, default: usize, maximum: usize) -> usize {
    match std::env::var(name) {
        Ok(raw) => match raw.parse::<usize>() {
            Ok(value) if value > 0 => value.min(maximum),
            _ => {
                execd_warn!("[adx-execd] tunnel invalid_config name={name}; using default");
                default
            }
        },
        Err(_) => default,
    }
}

fn parse_positive_duration_seconds(raw: &str) -> Option<Duration> {
    let seconds = raw.parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    let duration = Duration::try_from_secs_f64(seconds).ok()?;
    (!duration.is_zero()).then_some(duration)
}

fn http_timeout_from_raw(raw: Option<&str>) -> Duration {
    raw.and_then(parse_positive_duration_seconds)
        .unwrap_or(DEFAULT_HTTP_TIMEOUT)
}

fn configured_http_timeout() -> Duration {
    static VALUE: OnceLock<Duration> = OnceLock::new();
    *VALUE.get_or_init(|| {
        let raw = std::env::var("ADX_TUNNEL_HTTP_TIMEOUT").ok();
        let timeout = http_timeout_from_raw(raw.as_deref());
        if raw
            .as_deref()
            .is_some_and(|value| parse_positive_duration_seconds(value).is_none())
        {
            execd_warn!(
                "[adx-execd] tunnel invalid_config name=ADX_TUNNEL_HTTP_TIMEOUT; using default"
            );
        }
        execd_info!(
            "[adx-execd] tunnel configured http_timeout_seconds={}",
            timeout.as_secs_f64()
        );
        timeout
    })
}

fn configured_protocol_version() -> u8 {
    static VALUE: OnceLock<u8> = OnceLock::new();
    *VALUE.get_or_init(|| {
        positive_env_usize(
            "ADX_TUNNEL_PROTOCOL_VERSION",
            TUNNEL_PROTOCOL_VERSION as usize,
            TUNNEL_PROTOCOL_VERSION as usize,
        ) as u8
    })
}

fn configured_max_body_bytes() -> usize {
    static VALUE: OnceLock<usize> = OnceLock::new();
    *VALUE.get_or_init(|| {
        positive_env_usize(
            "ADX_TUNNEL_MAX_BODY_SIZE",
            MAX_HTTP_BODY_BYTES,
            MAX_CONFIGURED_BODY_BYTES,
        )
    })
}

fn configured_stream_chunk_bytes() -> usize {
    static VALUE: OnceLock<usize> = OnceLock::new();
    *VALUE.get_or_init(|| {
        positive_env_usize(
            "ADX_TUNNEL_STREAM_CHUNK_BYTES",
            DEFAULT_STREAM_CHUNK_BYTES,
            MAX_CONFIGURED_STREAM_CHUNK_BYTES,
        )
        .max(MIN_STREAM_CHUNK_BYTES)
    })
}

fn configured_max_ws_message_bytes() -> usize {
    static VALUE: OnceLock<usize> = OnceLock::new();
    *VALUE.get_or_init(|| {
        positive_env_usize(
            "ADX_TUNNEL_MAX_WS_MESSAGE_SIZE",
            DEFAULT_MAX_WS_MESSAGE_BYTES,
            MAX_CONFIGURED_WS_MESSAGE_BYTES,
        )
    })
}

fn configured_max_inflight() -> usize {
    static VALUE: OnceLock<usize> = OnceLock::new();
    *VALUE.get_or_init(|| {
        positive_env_usize(
            "ADX_TUNNEL_MAX_INFLIGHT",
            DEFAULT_MAX_INFLIGHT,
            MAX_CONFIGURED_INFLIGHT,
        )
    })
}

fn configured_stream_window_frames() -> usize {
    static VALUE: OnceLock<usize> = OnceLock::new();
    *VALUE.get_or_init(|| {
        positive_env_usize(
            "ADX_TUNNEL_STREAM_WINDOW_FRAMES",
            DEFAULT_STREAM_WINDOW_FRAMES,
            MAX_CONFIGURED_WINDOW_FRAMES,
        )
    })
}

fn configured_fast_path_body_bytes() -> u64 {
    static VALUE: OnceLock<u64> = OnceLock::new();
    *VALUE.get_or_init(|| {
        positive_env_usize(
            "ADX_TUNNEL_FAST_PATH_BODY_BYTES",
            FAST_PATH_BODY_BYTES as usize,
            configured_max_body_bytes().min(MAX_V1_BODY_BYTES),
        ) as u64
    })
}

struct StreamingResponse {
    generation: u64,
    status: StatusCode,
    headers: HeaderList,
    content_length: Option<u64>,
    body_rx: mpsc::Receiver<Result<ResponseChunk, String>>,
    queued_frames: Arc<AtomicUsize>,
}

struct ResponseChunk {
    offset: u64,
    payload: Bytes,
}

struct StreamingHttpRequest {
    method: Method,
    path: String,
    headers: HeaderList,
    content_length: Option<u64>,
    body: Incoming,
}

enum TunnelResponse {
    Legacy(Frame),
    Streaming(StreamingResponse),
}

struct PendingHttpResponse {
    /// Non-resumable exchanges are generation-scoped; resumable V2 also carries
    /// a stable session id and is rebound to the new generation after reconnect.
    generation: Option<u64>,
    session_id: Option<String>,
    sender: oneshot::Sender<Result<TunnelResponse, String>>,
}

enum WsTunnelMessage {
    Control(Frame),
    Binary(BinaryEnvelope),
}

struct ResponseStreamSink {
    generation: u64,
    session_id: Option<String>,
    sender: mpsc::Sender<Result<ResponseChunk, String>>,
    received: usize,
    expected: Option<usize>,
    max_body_size: usize,
    queued_frames: Arc<AtomicUsize>,
    window: usize,
}

struct StreamCredits {
    generation: AtomicU64,
    session_id: Option<String>,
    connected: AtomicBool,
    closed: AtomicBool,
    semaphore: Mutex<Arc<Semaphore>>,
    window: usize,
    ack_offset: AtomicU64,
    unacked: Mutex<VecDeque<(u64, Bytes)>>,
    ended: AtomicBool,
    resuming: AtomicBool,
    resume_notify: Notify,
    send_lock: AsyncMutex<()>,
}

impl StreamCredits {
    fn new(generation: u64, session_id: Option<String>, window: usize) -> Self {
        Self {
            generation: AtomicU64::new(generation),
            session_id,
            connected: AtomicBool::new(true),
            closed: AtomicBool::new(false),
            semaphore: Mutex::new(Arc::new(Semaphore::new(0))),
            window,
            ack_offset: AtomicU64::new(0),
            unacked: Mutex::new(VecDeque::new()),
            ended: AtomicBool::new(false),
            resuming: AtomicBool::new(false),
            resume_notify: Notify::new(),
            send_lock: AsyncMutex::new(()),
        }
    }

    fn is_resumable(&self) -> bool {
        self.session_id.is_some()
    }

    fn disconnect(&self, generation: u64) {
        if self.generation.load(Ordering::Acquire) != generation {
            return;
        }
        self.connected.store(false, Ordering::Release);
        self.resuming.store(self.is_resumable(), Ordering::Release);
        self.semaphore.lock().unwrap().close();
        self.resume_notify.notify_waiters();
    }

    fn rebind(&self, generation: u64) {
        self.generation.store(generation, Ordering::Release);
        self.connected.store(true, Ordering::Release);
        self.resuming.store(true, Ordering::Release);
        *self.semaphore.lock().unwrap() = Arc::new(Semaphore::new(0));
        self.resume_notify.notify_waiters();
    }

    fn grant(&self, generation: u64, credits: usize, ack_offset: Option<u64>) -> bool {
        if self.generation.load(Ordering::Acquire) != generation {
            return false;
        }
        if let Some(ack_offset) = ack_offset {
            let previous = self.ack_offset.fetch_max(ack_offset, Ordering::AcqRel);
            if ack_offset > previous {
                self.unacked.lock().unwrap().retain(|(offset, payload)| {
                    offset.saturating_add(payload.len() as u64) > ack_offset
                });
            }
        }
        let semaphore = self.semaphore.lock().unwrap().clone();
        let available = semaphore.available_permits();
        let grant = credits.min(self.window.saturating_sub(available));
        if grant > 0 {
            semaphore.add_permits(grant);
        }
        self.resuming.load(Ordering::Acquire) && ack_offset.is_some()
    }

    async fn acquire(&self, allow_during_resume: bool) -> Result<u64, ()> {
        loop {
            let notified = self.resume_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.closed.load(Ordering::Acquire) {
                return Err(());
            }
            if !allow_during_resume && self.resuming.load(Ordering::Acquire) {
                notified.await;
                continue;
            }
            if !self.connected.load(Ordering::Acquire) {
                if !self.is_resumable() {
                    return Err(());
                }
                notified.await;
                continue;
            }
            let generation = self.generation.load(Ordering::Acquire);
            let semaphore = self.semaphore.lock().unwrap().clone();
            match semaphore.acquire_owned().await {
                Ok(permit) => {
                    permit.forget();
                    if self.connected.load(Ordering::Acquire)
                        && self.generation.load(Ordering::Acquire) == generation
                    {
                        return Ok(generation);
                    }
                }
                Err(_) if !self.is_resumable() => return Err(()),
                Err(_) => {}
            }
        }
    }

    fn record(&self, offset: u64, payload: Bytes) {
        self.unacked.lock().unwrap().push_back((offset, payload));
    }

    fn unacked(&self) -> Vec<(u64, Bytes)> {
        let ack_offset = self.ack_offset.load(Ordering::Acquire);
        self.unacked
            .lock()
            .unwrap()
            .iter()
            .filter(|(offset, payload)| offset.saturating_add(payload.len() as u64) > ack_offset)
            .cloned()
            .collect()
    }

    fn finish_resume(&self) {
        self.resuming.store(false, Ordering::Release);
        self.resume_notify.notify_waiters();
    }

    async fn wait_until_ready(&self) -> Result<u64, ()> {
        loop {
            let notified = self.resume_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.connected.load(Ordering::Acquire) && !self.resuming.load(Ordering::Acquire) {
                return Ok(self.generation.load(Ordering::Acquire));
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(());
            }
            if !self.is_resumable() {
                return Err(());
            }
            notified.await;
        }
    }

    async fn wait_until_connected(&self) -> Result<u64, ()> {
        loop {
            let notified = self.resume_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.closed.load(Ordering::Acquire) {
                return Err(());
            }
            if self.connected.load(Ordering::Acquire) {
                return Ok(self.generation.load(Ordering::Acquire));
            }
            if !self.is_resumable() {
                return Err(());
            }
            notified.await;
        }
    }
}

struct CachedRequest {
    frame: Frame,
    created_at: Instant,
    session_id: Option<String>,
    ttl: Duration,
}

#[derive(Clone)]
struct PendingWsChannel {
    generation: u64,
    sender: mpsc::Sender<WsTunnelMessage>,
}

struct ActiveClient {
    generation: u64,
    sender: mpsc::Sender<OutboundMessage>,
    data_permits: Arc<Semaphore>,
    shutdown: oneshot::Sender<()>,
}

struct OutboundMessage {
    message: Message,
    _data_permit: Option<OwnedSemaphorePermit>,
}

struct NegotiatedConnection {
    generation: u64,
    protocol: NegotiatedProtocol,
    session_id: Option<String>,
    http: Arc<Semaphore>,
    websocket: Arc<Semaphore>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConnectionProtocol {
    Disconnected,
    Legacy {
        generation: u64,
    },
    Streaming {
        generation: u64,
        protocol: NegotiatedProtocol,
        session_id: Option<String>,
    },
}

fn b64() -> base64::engine::general_purpose::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

/// Server-local unique frame id. `ADEX` makes captures recognizable, the UUID
/// version/variant bits keep standard parsers happy, and the atomic counter is
/// sufficient because ids only need to be unique within this process's bounded
/// in-flight and tombstone windows. Relaxed ordering is enough for uniqueness.
fn make_id() -> String {
    static N: AtomicU64 = AtomicU64::new(1);
    let sequence = N.fetch_add(1, Ordering::Relaxed);
    let mut bytes = [0u8; 16];
    bytes[..4].copy_from_slice(b"ADEX");
    bytes[6] = 0x40;
    bytes[8..].copy_from_slice(&sequence.to_be_bytes());
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    id_from_uuid_bytes(&bytes)
}

fn uuid_bytes_from_id(id: &str) -> Result<[u8; 16], String> {
    let compact: String = id.chars().filter(|ch| *ch != '-').collect();
    if compact.len() != 32 || !compact.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("binary envelope id must be a UUID".into());
    }
    let mut bytes = [0u8; 16];
    for (index, target) in bytes.iter_mut().enumerate() {
        *target = u8::from_str_radix(&compact[index * 2..index * 2 + 2], 16)
            .map_err(|_| "binary envelope id must be a UUID".to_string())?;
    }
    Ok(bytes)
}

fn id_from_uuid_bytes(bytes: &[u8; 16]) -> String {
    let mut hex = String::with_capacity(32);
    for byte in bytes {
        write!(&mut hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

// ───────────────────────── wire frames ─────────────────────────
// Matches tunnel_protocol.py. V1 `body` / binary WS `data` remain base64 strings.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
enum Frame {
    #[serde(rename = "hello")]
    Hello {
        protocol_version: u8,
        max_stream_chunk: usize,
        max_inflight: usize,
        stream_window_frames: usize,
        #[serde(default = "configured_max_body_bytes")]
        max_body_size: usize,
        #[serde(default = "configured_max_ws_message_bytes")]
        max_ws_message_size: usize,
        #[serde(default)]
        resume: bool,
        #[serde(default)]
        session_id: Option<String>,
    },
    #[serde(rename = "http_req")]
    HttpReq {
        id: String,
        method: String,
        path: String,
        #[serde(deserialize_with = "deserialize_headers")]
        headers: HeaderList,
        #[serde(default)]
        body: String,
    },
    #[serde(rename = "http_resp")]
    HttpResp {
        id: String,
        status: u16,
        #[serde(default, deserialize_with = "deserialize_headers")]
        headers: HeaderList,
        #[serde(default)]
        body: String,
    },
    #[serde(rename = "http_req_begin")]
    HttpReqBegin {
        id: String,
        method: String,
        path: String,
        #[serde(deserialize_with = "deserialize_headers")]
        headers: HeaderList,
        #[serde(default)]
        content_length: Option<u64>,
    },
    #[serde(rename = "http_req_end")]
    HttpReqEnd { id: String },
    #[serde(rename = "http_resp_begin")]
    HttpRespBegin {
        id: String,
        status: u16,
        #[serde(default, deserialize_with = "deserialize_headers")]
        headers: HeaderList,
        #[serde(default)]
        content_length: Option<u64>,
    },
    #[serde(rename = "http_resp_end")]
    HttpRespEnd { id: String },
    #[serde(rename = "window")]
    Window {
        id: String,
        credits: usize,
        #[serde(default)]
        ack_offset: Option<u64>,
        #[serde(default)]
        complete: bool,
    },
    #[serde(rename = "ws_connect")]
    WsConnect {
        id: String,
        path: String,
        headers: HashMap<String, String>,
    },
    #[serde(rename = "ws_connected")]
    WsConnected { id: String },
    #[serde(rename = "ws_message")]
    WsMessage {
        id: String,
        data: String,
        #[serde(default)]
        binary: bool,
    },
    #[serde(rename = "ws_close")]
    WsClose {
        id: String,
        #[serde(default = "default_close_code")]
        code: u16,
        #[serde(default)]
        reason: String,
    },
    #[serde(rename = "error")]
    Error { id: String, message: String },
    #[serde(rename = "ping")]
    Ping { id: String, timestamp: f64 },
    #[serde(rename = "pong")]
    Pong { id: String, timestamp: f64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum BinaryKind {
    HttpRequest = 0x01,
    HttpResponse = 0x02,
    WebSocket = 0x03,
}

impl TryFrom<u8> for BinaryKind {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::HttpRequest),
            0x02 => Ok(Self::HttpResponse),
            0x03 => Ok(Self::WebSocket),
            other => Err(format!("unknown binary envelope kind: {other}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BinaryEnvelope {
    id: String,
    kind: BinaryKind,
    payload: Bytes,
    end_of_body: bool,
    offset: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NegotiatedProtocol {
    max_stream_chunk: usize,
    max_inflight: usize,
    stream_window_frames: usize,
    max_body_size: usize,
    max_ws_message_size: usize,
    resumable: bool,
}

impl BinaryEnvelope {
    fn encode(&self, max_payload: usize) -> Result<Message, String> {
        if self.payload.len() > max_payload {
            return Err(format!(
                "binary payload exceeds negotiated chunk limit: {} > {max_payload}",
                self.payload.len()
            ));
        }
        let mut raw = Vec::with_capacity(BINARY_HEADER_BYTES + self.payload.len());
        raw.extend_from_slice(&BINARY_MAGIC);
        raw.push(BINARY_ENVELOPE_VERSION);
        raw.push(self.kind as u8);
        raw.push(16);
        raw.extend_from_slice(&uuid_bytes_from_id(&self.id)?);
        let mut flags = if self.end_of_body {
            BINARY_END_OF_BODY
        } else {
            0
        };
        let wire_payload_len = self
            .payload
            .len()
            .checked_add(if self.offset.is_some() { 8 } else { 0 })
            .ok_or_else(|| "binary payload size overflow".to_string())?;
        if self.offset.is_some() {
            flags |= BINARY_HAS_OFFSET;
        }
        raw.push(flags);
        raw.extend_from_slice(&(wire_payload_len as u32).to_be_bytes());
        if let Some(offset) = self.offset {
            raw.extend_from_slice(&offset.to_be_bytes());
        }
        raw.extend_from_slice(&self.payload);
        Ok(Message::Binary(raw))
    }

    fn decode(raw: &[u8], max_payload: usize) -> Result<Self, String> {
        if raw.len() < BINARY_HEADER_BYTES {
            return Err("binary envelope is shorter than its header".into());
        }
        if raw[..2] != BINARY_MAGIC {
            return Err("invalid binary envelope magic".into());
        }
        if raw[2] != BINARY_ENVELOPE_VERSION {
            return Err(format!("unsupported binary envelope version: {}", raw[2]));
        }
        let kind = BinaryKind::try_from(raw[3])?;
        if raw[4] != 16 {
            return Err(format!("invalid binary envelope UUID length: {}", raw[4]));
        }
        let raw_id: [u8; 16] = raw[5..21].try_into().unwrap();
        let id = id_from_uuid_bytes(&raw_id);
        let flags = raw[21];
        if flags & !(BINARY_END_OF_BODY | BINARY_HAS_OFFSET) != 0 {
            return Err(format!("unknown binary envelope flags: {flags:#x}"));
        }
        let payload_len = u32::from_be_bytes(raw[22..26].try_into().unwrap()) as usize;
        let wire_limit = max_payload + if flags & BINARY_HAS_OFFSET != 0 { 8 } else { 0 };
        if payload_len > wire_limit {
            return Err(format!(
                "binary payload exceeds negotiated chunk limit: {payload_len} > {wire_limit}"
            ));
        }
        if raw.len() - BINARY_HEADER_BYTES != payload_len {
            return Err(format!(
                "binary payload length mismatch: {} != {payload_len}",
                raw.len() - BINARY_HEADER_BYTES
            ));
        }
        let wire_payload = &raw[BINARY_HEADER_BYTES..];
        let (offset, payload) = if flags & BINARY_HAS_OFFSET != 0 {
            if wire_payload.len() < 8 {
                return Err("offset binary envelope payload is too short".into());
            }
            (
                Some(u64::from_be_bytes(wire_payload[..8].try_into().unwrap())),
                &wire_payload[8..],
            )
        } else {
            (None, wire_payload)
        };
        if payload.len() > max_payload {
            return Err(format!(
                "binary payload exceeds negotiated chunk limit: {} > {max_payload}",
                payload.len()
            ));
        }
        Ok(Self {
            id,
            kind,
            payload: Bytes::copy_from_slice(payload),
            end_of_body: flags & BINARY_END_OF_BODY != 0,
            offset,
        })
    }
}

fn default_close_code() -> u16 {
    1000
}

impl Frame {
    #[cfg(test)]
    fn to_msg(&self) -> Message {
        control_message(self, false).expect("test frame must fit the control channel")
    }
}

fn control_message(frame: &Frame, legacy_headers: bool) -> Result<Message, ()> {
    let raw = if let Frame::HttpReq {
        id,
        method,
        path,
        headers,
        body,
    } = frame
    {
        if legacy_headers {
            // A peer without hello negotiation expects the original map shape.
            // Keep ordered pairs internally and for every negotiated V2 peer.
            serde_json::to_string(&serde_json::json!({
                "type": "http_req", "id": id, "method": method, "path": path,
                "headers": headers.iter().map(|(name, value)| (name, value))
                    .collect::<std::collections::BTreeMap<_, _>>(),
                "body": body,
            }))
        } else {
            serde_json::to_string(frame)
        }
    } else {
        serde_json::to_string(frame)
    }
    .map_err(|_| ())?;
    (raw.len() <= CONTROL_WS_MESSAGE_BYTES)
        .then_some(Message::Text(raw))
        .ok_or(())
}

fn headers_within_limits(headers: &HeaderList) -> bool {
    headers.len() <= MAX_HTTP_HEADERS
        && headers
            .iter()
            .try_fold(0usize, |total, (name, value)| {
                total.checked_add(name.len())?.checked_add(value.len())
            })
            .is_some_and(|total| total <= MAX_HTTP_HEADER_BYTES)
}

// ───────────────────────── shared state ─────────────────────────
struct State {
    /// Maximum wait for one reverse-tunnel HTTP exchange.
    http_timeout: Duration,
    /// Outbound bounded channel and generation of the active TunnelClient WS.
    active_client: Mutex<Option<ActiveClient>>,
    /// HTTP request id -> waiter accepting either a V1 response or V2 stream metadata.
    pending_http: Mutex<HashMap<String, PendingHttpResponse>>,
    /// WS channel id -> queue of frames from the client for that channel.
    pending_ws: Mutex<HashMap<String, PendingWsChannel>>,
    /// In-flight HTTP request frames, cached for resend when a client reconnects.
    pending_requests: Mutex<HashMap<String, CachedRequest>>,
    /// V2 response id -> bounded downstream body channel.
    response_streams: Mutex<HashMap<String, ResponseStreamSink>>,
    /// V2 stream id -> sender-side byte credits granted by the peer.
    stream_credits: Mutex<HashMap<String, Arc<StreamCredits>>>,
    /// Recently closed response ids absorb the peer's already-granted window
    /// without turning a single downstream cancellation into a tunnel reset.
    terminated_streams: Mutex<HashMap<(u64, String), Instant>>,
    /// Monotonic identity of the active TunnelClient connection.
    active_generation: AtomicU64,
    /// Generation that already timed out negotiation and selected V1 fallback.
    legacy_generation: AtomicU64,
    /// V2 limits and permits negotiated for the active connection.
    negotiated: Mutex<Option<NegotiatedConnection>>,
    protocol_notify: Notify,
    http_permits: Arc<Semaphore>,
    ws_permits: Arc<Semaphore>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            http_timeout: configured_http_timeout(),
            active_client: Mutex::new(None),
            pending_http: Mutex::new(HashMap::new()),
            pending_ws: Mutex::new(HashMap::new()),
            pending_requests: Mutex::new(HashMap::new()),
            response_streams: Mutex::new(HashMap::new()),
            stream_credits: Mutex::new(HashMap::new()),
            terminated_streams: Mutex::new(HashMap::new()),
            active_generation: AtomicU64::new(0),
            legacy_generation: AtomicU64::new(0),
            negotiated: Mutex::new(None),
            protocol_notify: Notify::new(),
            http_permits: Arc::new(Semaphore::new(configured_max_inflight())),
            ws_permits: Arc::new(Semaphore::new(configured_max_inflight())),
        }
    }
}

impl State {
    #[cfg(test)]
    fn with_http_timeout(http_timeout: Duration) -> Self {
        Self {
            http_timeout,
            ..Self::default()
        }
    }

    fn send_message_for_generation(&self, generation: u64, message: Message) -> Result<(), ()> {
        let guard = self.active_client.lock().unwrap();
        match guard
            .as_ref()
            .filter(|client| client.generation == generation)
        {
            Some(client) => client
                .sender
                .try_send(OutboundMessage {
                    message,
                    _data_permit: None,
                })
                .map_err(|_| ()),
            None => Err(()),
        }
    }

    async fn send_data_message_for_generation_wait(
        &self,
        generation: u64,
        message: Message,
    ) -> Result<(), ()> {
        let (sender, data_permits) = self
            .active_client
            .lock()
            .unwrap()
            .as_ref()
            .filter(|client| client.generation == generation)
            .map(|client| (client.sender.clone(), client.data_permits.clone()))
            .ok_or(())?;
        let permit = tokio::time::timeout(WS_CONNECT_TIMEOUT, data_permits.acquire_owned())
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?;
        tokio::time::timeout(
            WS_CONNECT_TIMEOUT,
            sender.send(OutboundMessage {
                message,
                _data_permit: Some(permit),
            }),
        )
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
    }

    fn send_to_generation(&self, generation: u64, frame: &Frame) -> Result<(), ()> {
        self.send_message_for_generation(
            generation,
            control_message(
                frame,
                self.legacy_generation.load(Ordering::Acquire) == generation,
            )?,
        )
    }

    async fn send_to_generation_wait(&self, generation: u64, frame: Frame) -> Result<(), ()> {
        let message = control_message(
            &frame,
            self.legacy_generation.load(Ordering::Acquire) == generation,
        )?;
        let sender = self
            .active_client
            .lock()
            .unwrap()
            .as_ref()
            .filter(|client| client.generation == generation)
            .map(|client| client.sender.clone())
            .ok_or(())?;
        sender
            .send(OutboundMessage {
                message,
                _data_permit: None,
            })
            .await
            .map_err(|_| ())
    }

    fn send_error_eventually(self: &Arc<Self>, generation: u64, id: String, message: &str) {
        let frame = Frame::Error {
            id,
            message: message.to_string(),
        };
        if self.send_to_generation(generation, &frame).is_ok() {
            return;
        }
        let state = self.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = tokio::time::timeout(
                    Duration::from_secs(1),
                    state.send_to_generation_wait(generation, frame),
                )
                .await;
            });
        }
    }

    fn active_client_generation(&self) -> Option<u64> {
        self.active_client
            .lock()
            .unwrap()
            .as_ref()
            .map(|client| client.generation)
    }

    fn active_protocol(&self) -> Option<(u64, NegotiatedProtocol)> {
        let generation = self.active_generation.load(Ordering::Acquire);
        self.negotiated
            .lock()
            .unwrap()
            .as_ref()
            .filter(|connection| connection.generation == generation)
            .map(|connection| (generation, connection.protocol))
    }

    fn active_session_id(&self) -> Option<String> {
        let generation = self.active_generation.load(Ordering::Acquire);
        self.negotiated
            .lock()
            .unwrap()
            .as_ref()
            .filter(|connection| connection.generation == generation)
            .and_then(|connection| connection.session_id.clone())
    }

    fn select_legacy_if_unnegotiated(&self, generation: u64) -> bool {
        if self.active_client_generation() != Some(generation) {
            return false;
        }
        let negotiated = self.negotiated.lock().unwrap();
        if negotiated
            .as_ref()
            .is_some_and(|connection| connection.generation == generation)
        {
            return false;
        }
        self.legacy_generation.store(generation, Ordering::Release);
        true
    }

    async fn wait_for_protocol(&self) -> ConnectionProtocol {
        // Register before inspecting state so a concurrent hello cannot notify
        // between the last check and creation of the waiter.
        let notified = self.protocol_notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let Some(generation) = self.active_client_generation() else {
            return ConnectionProtocol::Disconnected;
        };
        if let Some((candidate, protocol)) = self.active_protocol() {
            return ConnectionProtocol::Streaming {
                generation: candidate,
                protocol,
                session_id: self.active_session_id(),
            };
        }
        if self.legacy_generation.load(Ordering::Acquire) == generation {
            return ConnectionProtocol::Legacy { generation };
        }
        let _ = tokio::time::timeout(Duration::from_millis(250), notified.as_mut()).await;
        if self.active_client_generation() != Some(generation) {
            return ConnectionProtocol::Disconnected;
        }
        if let Some((candidate, protocol)) = self.active_protocol() {
            return ConnectionProtocol::Streaming {
                generation: candidate,
                protocol,
                session_id: self.active_session_id(),
            };
        }
        if self.select_legacy_if_unnegotiated(generation) {
            self.protocol_notify.notify_waiters();
            ConnectionProtocol::Legacy { generation }
        } else if let Some((candidate, protocol)) = self.active_protocol() {
            ConnectionProtocol::Streaming {
                generation: candidate,
                protocol,
                session_id: self.active_session_id(),
            }
        } else {
            ConnectionProtocol::Disconnected
        }
    }

    fn register_stream_credits(
        &self,
        id: &str,
        generation: u64,
        session_id: Option<String>,
        window: usize,
    ) -> Arc<StreamCredits> {
        let credits = Arc::new(StreamCredits::new(generation, session_id, window));
        self.stream_credits
            .lock()
            .unwrap()
            .insert(id.to_string(), credits.clone());
        credits
    }

    fn grant_stream_credits(
        &self,
        id: &str,
        generation: u64,
        credits: usize,
        ack_offset: Option<u64>,
    ) -> Option<Arc<StreamCredits>> {
        let guard = self.stream_credits.lock().unwrap();
        let entry = guard.get(id)?;
        entry
            .grant(generation, credits, ack_offset)
            .then(|| entry.clone())
    }

    fn remove_stream_credits(&self, id: &str) -> bool {
        if let Some(entry) = self.stream_credits.lock().unwrap().remove(id) {
            entry.closed.store(true, Ordering::Release);
            entry.connected.store(false, Ordering::Release);
            entry.semaphore.lock().unwrap().close();
            entry.resume_notify.notify_waiters();
            true
        } else {
            false
        }
    }

    fn try_acquire_negotiated_permit(
        &self,
        generation: u64,
        websocket: bool,
    ) -> Result<OwnedSemaphorePermit, ()> {
        let semaphore = self
            .negotiated
            .lock()
            .unwrap()
            .as_ref()
            .filter(|connection| connection.generation == generation)
            .map(|connection| {
                if websocket {
                    connection.websocket.clone()
                } else {
                    connection.http.clone()
                }
            })
            .ok_or(())?;
        semaphore.try_acquire_owned().map_err(|_| ())
    }

    fn deactivate_generation(&self, generation: u64, message: &str) {
        let was_active = {
            let mut active = self.active_client.lock().unwrap();
            if active
                .as_ref()
                .is_some_and(|client| client.generation == generation)
            {
                *active = None;
                true
            } else {
                false
            }
        };
        if !was_active {
            return;
        }
        *self.negotiated.lock().unwrap() = None;
        self.disconnect_streams_for_generation(generation, message);
        self.close_ws_for_generation(generation, message);
        self.protocol_notify.notify_waiters();
    }

    fn disconnect_streams_for_generation(&self, generation: u64, message: &str) {
        let pending_ids: Vec<String> = self
            .pending_http
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, pending)| {
                pending.generation == Some(generation) && pending.session_id.is_none()
            })
            .map(|(id, _)| id.clone())
            .collect();
        let mut pending = self.pending_http.lock().unwrap();
        for id in pending_ids {
            if let Some(pending) = pending.remove(&id) {
                let _ = pending.sender.send(Err(message.to_string()));
            }
        }
        drop(pending);

        let response_ids: Vec<String> = self
            .response_streams
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, sink)| sink.generation == generation && sink.session_id.is_none())
            .map(|(id, _)| id.clone())
            .collect();
        let mut responses = self.response_streams.lock().unwrap();
        for id in response_ids {
            if let Some(sink) = responses.remove(&id) {
                let _ = sink.sender.try_send(Err(message.to_string()));
            }
        }
        drop(responses);

        let credit_entries: Vec<(String, Arc<StreamCredits>)> = self
            .stream_credits
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, entry)| entry.generation.load(Ordering::Acquire) == generation)
            .map(|(id, entry)| (id.clone(), entry.clone()))
            .collect();
        for (id, entry) in credit_entries {
            entry.disconnect(generation);
            if !entry.is_resumable() {
                self.remove_stream_credits(&id);
            }
        }
    }

    fn fail_resumable_session(&self, session_id: &str, message: &str) {
        let pending_ids: Vec<String> = self
            .pending_http
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, pending)| pending.session_id.as_deref() == Some(session_id))
            .map(|(id, _)| id.clone())
            .collect();
        let mut pending = self.pending_http.lock().unwrap();
        for id in &pending_ids {
            if let Some(pending) = pending.remove(id) {
                let _ = pending.sender.send(Err(message.to_string()));
            }
        }
        drop(pending);

        let response_ids: Vec<String> = self
            .response_streams
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, sink)| sink.session_id.as_deref() == Some(session_id))
            .map(|(id, _)| id.clone())
            .collect();
        let mut responses = self.response_streams.lock().unwrap();
        for id in &response_ids {
            if let Some(sink) = responses.remove(id) {
                let _ = sink.sender.try_send(Err(message.to_string()));
            }
        }
        drop(responses);

        let credit_ids: Vec<String> = self
            .stream_credits
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, entry)| entry.session_id.as_deref() == Some(session_id))
            .map(|(id, _)| id.clone())
            .collect();
        for id in credit_ids {
            self.remove_stream_credits(&id);
        }
        self.pending_requests
            .lock()
            .unwrap()
            .retain(|_, cached| cached.session_id.as_deref() != Some(session_id));
    }

    fn resumable_session_ids(&self) -> HashSet<String> {
        let mut sessions = HashSet::new();
        sessions.extend(
            self.pending_http
                .lock()
                .unwrap()
                .values()
                .filter_map(|pending| pending.session_id.clone()),
        );
        sessions.extend(
            self.response_streams
                .lock()
                .unwrap()
                .values()
                .filter_map(|sink| sink.session_id.clone()),
        );
        sessions.extend(
            self.stream_credits
                .lock()
                .unwrap()
                .values()
                .filter_map(|credits| credits.session_id.clone()),
        );
        sessions
    }

    fn rebind_resumable_session(
        &self,
        session_id: &str,
        generation: u64,
    ) -> Vec<(String, usize, u64)> {
        for pending in self.pending_http.lock().unwrap().values_mut() {
            if pending.session_id.as_deref() == Some(session_id) {
                pending.generation = Some(generation);
            }
        }

        let response_windows = {
            let mut streams = self.response_streams.lock().unwrap();
            streams
                .iter_mut()
                .filter_map(|(id, sink)| {
                    if sink.session_id.as_deref() != Some(session_id) {
                        return None;
                    }
                    sink.generation = generation;
                    let queued = sink.queued_frames.load(Ordering::Acquire);
                    Some((
                        id.clone(),
                        sink.window.saturating_sub(queued),
                        sink.received as u64,
                    ))
                })
                .collect()
        };

        for credits in self.stream_credits.lock().unwrap().values() {
            if credits.session_id.as_deref() == Some(session_id) {
                credits.rebind(generation);
            }
        }
        response_windows
    }

    fn send_response_window(&self, id: &str, credits: usize, ack_offset: u64) {
        let generation = self
            .response_streams
            .lock()
            .unwrap()
            .get(id)
            .map(|sink| sink.generation);
        if let Some(generation) = generation {
            let _ = self.send_to_generation(
                generation,
                &Frame::Window {
                    id: id.to_string(),
                    credits,
                    ack_offset: Some(ack_offset),
                    complete: false,
                },
            );
        }
    }

    fn close_ws_for_generation(&self, generation: u64, message: &str) {
        let ids: Vec<String> = self
            .pending_ws
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, channel)| channel.generation == generation)
            .map(|(id, _)| id.clone())
            .collect();
        let mut channels = self.pending_ws.lock().unwrap();
        for id in ids {
            if let Some(channel) = channels.remove(&id) {
                let _ = channel
                    .sender
                    .try_send(WsTunnelMessage::Control(Frame::WsClose {
                        id,
                        code: 1001,
                        reason: message.to_string(),
                    }));
            }
        }
    }

    fn remember_terminated_stream(&self, generation: u64, id: &str) {
        let now = Instant::now();
        let mut terminated = self.terminated_streams.lock().unwrap();
        terminated.retain(|_, timestamp| now.duration_since(*timestamp) <= TERMINATED_STREAM_TTL);
        if terminated.len() >= TERMINATED_STREAM_LIMIT {
            if let Some(oldest) = terminated
                .iter()
                .min_by_key(|(_, timestamp)| **timestamp)
                .map(|(key, _)| key.clone())
            {
                terminated.remove(&oldest);
            }
        }
        terminated.insert((generation, id.to_string()), now);
    }

    fn is_terminated_stream(&self, generation: u64, id: &str) -> bool {
        self.terminated_streams
            .lock()
            .unwrap()
            .get(&(generation, id.to_string()))
            .is_some_and(|timestamp| timestamp.elapsed() <= TERMINATED_STREAM_TTL)
    }
}

struct ActiveConnectionGuard {
    state: Arc<State>,
    generation: u64,
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.state
            .deactivate_generation(self.generation, "tunnel client disconnected");
    }
}

struct AbortTaskOnDrop(AbortHandle);

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct PendingRequestGuard {
    state: Arc<State>,
    generation: u64,
    id: String,
    armed: bool,
}

impl PendingRequestGuard {
    fn new(state: Arc<State>, generation: u64, id: String) -> Self {
        Self {
            state,
            generation,
            id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingRequestGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let removed_pending = self
            .state
            .pending_http
            .lock()
            .unwrap()
            .remove(&self.id)
            .is_some();
        let removed_cached = self
            .state
            .pending_requests
            .lock()
            .unwrap()
            .remove(&self.id)
            .is_some();
        let removed_credits = self.state.remove_stream_credits(&self.id);
        let removed_response = self
            .state
            .response_streams
            .lock()
            .unwrap()
            .remove(&self.id)
            .is_some();
        let canceled = removed_pending || removed_cached || removed_credits || removed_response;
        if canceled {
            self.state
                .remember_terminated_stream(self.generation, &self.id);
            self.state.send_error_eventually(
                self.generation,
                self.id.clone(),
                "downstream request closed",
            );
        }
    }
}

struct PendingWsGuard {
    state: Arc<State>,
    generation: u64,
    id: String,
}

impl Drop for PendingWsGuard {
    fn drop(&mut self) {
        let mut pending = self.state.pending_ws.lock().unwrap();
        let removed = if pending
            .get(&self.id)
            .is_some_and(|channel| channel.generation == self.generation)
        {
            pending.remove(&self.id);
            true
        } else {
            false
        };
        drop(pending);
        if removed {
            self.state
                .remember_terminated_stream(self.generation, &self.id);
        }
    }
}

struct DownstreamStreamGuard {
    state: Arc<State>,
    generation: u64,
    id: String,
}

impl Drop for DownstreamStreamGuard {
    fn drop(&mut self) {
        if self
            .state
            .response_streams
            .lock()
            .unwrap()
            .remove(&self.id)
            .is_some()
        {
            self.state
                .remember_terminated_stream(self.generation, &self.id);
            self.state.send_error_eventually(
                self.generation,
                self.id.clone(),
                "downstream response closed",
            );
        }
    }
}

pub async fn run_standalone(ws_port: u16, http_port: u16) {
    run_servers(ws_port, http_port, Arc::new(State::default())).await;
}

/// Both tunnel listeners reserved as one startup unit. Binding happens before
/// the runtime reports readiness, so a configured tunnel cannot be
/// advertised ready with only one of its two ports available.
pub(super) struct BoundTunnelServers {
    porta: TcpListener,
    portb: TcpListener,
    ws_port: u16,
    http_port: u16,
}

#[derive(Clone)]
pub(super) struct TunnelServerControl {
    inner: Arc<TunnelServerControlInner>,
}

impl std::fmt::Debug for TunnelServerControl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TunnelServerControl")
            .field("generation", &self.inner.generation.load(Ordering::Acquire))
            .finish()
    }
}

struct TunnelServerControlInner {
    porta: std::net::TcpListener,
    portb: std::net::TcpListener,
    accept_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    generation: AtomicU64,
    ready_tx: watch::Sender<super::RuntimeReadyState>,
}

impl TunnelServerControl {
    pub(super) fn start(
        bound: BoundTunnelServers,
        ready_tx: watch::Sender<super::RuntimeReadyState>,
    ) -> Result<Self, String> {
        let BoundTunnelServers {
            porta,
            portb,
            ws_port,
            http_port,
        } = bound;
        let porta = porta
            .into_std()
            .map_err(|error| format!("failed to preserve tunnel WS listener: {error}"))?;
        let portb = portb
            .into_std()
            .map_err(|error| format!("failed to preserve tunnel HTTP listener: {error}"))?;
        porta
            .set_nonblocking(true)
            .map_err(|error| format!("failed to configure tunnel WS listener: {error}"))?;
        portb
            .set_nonblocking(true)
            .map_err(|error| format!("failed to configure tunnel HTTP listener: {error}"))?;
        let control = Self {
            inner: Arc::new(TunnelServerControlInner {
                porta,
                portb,
                accept_task: Mutex::new(None),
                generation: AtomicU64::new(0),
                ready_tx,
            }),
        };
        control.rearm().map_err(|error| {
            format!(
                "failed to install tunnel listeners on WS port {} and HTTP port {}: {error}",
                ws_port, http_port
            )
        })?;
        Ok(control)
    }

    pub(super) fn validate_restored_ports(
        &self,
        environment: &HashMap<String, String>,
    ) -> std::io::Result<()> {
        for (name, expected) in [
            (
                "EXECD_TUNNEL_WS_PORT",
                self.inner.porta.local_addr()?.port(),
            ),
            (
                "EXECD_TUNNEL_HTTP_PORT",
                self.inner.portb.local_addr()?.port(),
            ),
        ] {
            if environment
                .get(name)
                .is_some_and(|value| value.parse::<u16>().ok() != Some(expected))
            {
                return Err(std::io::Error::other(
                    "restore cannot change inherited tunnel ports",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn rearm(&self) -> std::io::Result<u64> {
        let mut accept_task =
            self.inner.accept_task.lock().map_err(|_| {
                std::io::Error::other("EXECD tunnel listener control lock is poisoned")
            })?;
        let porta = TcpListener::from_std(self.inner.porta.try_clone()?)?;
        let portb = TcpListener::from_std(self.inner.portb.try_clone()?)?;
        let state = Arc::new(State::default());
        let generation = self.inner.generation.load(Ordering::Relaxed) + 1;
        self.inner.generation.store(generation, Ordering::Release);
        let _ = self.inner.ready_tx.send(super::RuntimeReadyState::Ready);
        let task = tokio::spawn(async move {
            serve(porta, portb, state).await;
        });
        if let Some(previous) = accept_task.replace(task) {
            previous.abort();
        }
        execd_info!("[execd-tunnel] listener generation installed generation={generation}");
        Ok(generation)
    }
}

impl BoundTunnelServers {
    pub(super) async fn bind(ws_port: u16, http_port: u16) -> Result<Self, String> {
        let (porta, portb) = tokio::try_join!(
            async {
                TcpListener::bind(("0.0.0.0", ws_port))
                    .await
                    .map_err(|e| format!("failed to bind tunnel WS port {ws_port}: {e}"))
            },
            async {
                TcpListener::bind(("127.0.0.1", http_port))
                    .await
                    .map_err(|e| format!("failed to bind tunnel HTTP port {http_port}: {e}"))
            }
        )?;
        Ok(Self {
            porta,
            portb,
            ws_port,
            http_port,
        })
    }
}

async fn run_servers(ws_port: u16, http_port: u16, state: Arc<State>) {
    let bound = match BoundTunnelServers::bind(ws_port, http_port).await {
        Ok(bound) => bound,
        Err(e) => {
            execd_error!("[adx-execd] tunnel readiness failed: {e}");
            return;
        }
    };
    execd_info!("[adx-execd] tunnel listening ws=0.0.0.0:{ws_port} http=127.0.0.1:{http_port}");
    serve(bound.porta, bound.portb, state).await;
}

/// Drive both accept loops over pre-bound listeners (split out for tests).
async fn serve(porta: TcpListener, portb: TcpListener, state: Arc<State>) {
    let s2 = state.clone();
    tokio::join!(accept_port_a(porta, state), accept_port_b(portb, s2));
}

// ───────────────────────── Port A: TunnelClient WS ─────────────────────────
async fn accept_port_a(listener: TcpListener, state: Arc<State>) {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = connections.join_next(), if !connections.is_empty() => continue,
        };
        match accepted {
            Ok((stream, _)) => {
                let st = state.clone();
                connections.spawn(async move {
                    if let Err(e) = handle_client(stream, st).await {
                        execd_warn!("[adx-execd] tunnel client_conn_ended error={e}");
                    }
                });
            }
            Err(e) => {
                execd_error!("[adx-execd] tunnel port_a_accept_error error={e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn handle_client(stream: TcpStream, state: Arc<State>) -> Result<(), String> {
    if super::control::current().is_some_and(|controller| {
        controller.status().phase != adx_core::runtime::RuntimePhase::Running
    }) {
        return Err("runtime checkpoint transition in progress".into());
    }
    let ws = tokio_tungstenite::accept_async(stream)
        .await
        .map_err(|e| format!("ws accept: {e}"))?;
    let (mut sink, mut rx_ws) = ws.split();
    let _active = super::activity::enter(super::activity::ActivitySource::Tunnel);
    let (tx, mut rx) = mpsc::channel::<OutboundMessage>(OUTBOUND_QUEUE_FRAMES);
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let generation = state.active_generation.fetch_add(1, Ordering::AcqRel) + 1;
    // Installing a new generation atomically redirects future sends. Streams
    // from the replaced connection are failed immediately instead of being
    // orphaned until the stale socket eventually notices its disconnect.
    let replaced = state.active_client.lock().unwrap().replace(ActiveClient {
        generation,
        sender: tx,
        data_permits: Arc::new(Semaphore::new(
            OUTBOUND_QUEUE_FRAMES - OUTBOUND_CONTROL_RESERVE,
        )),
        shutdown: shutdown_tx,
    });
    *state.negotiated.lock().unwrap() = None;
    state.legacy_generation.store(0, Ordering::Release);
    if let Some(previous) = replaced {
        previous.data_permits.close();
        let _ = previous.shutdown.send(());
        state.disconnect_streams_for_generation(previous.generation, "tunnel client replaced");
        state.close_ws_for_generation(previous.generation, "tunnel client replaced");
    }
    let _connection_guard = ActiveConnectionGuard {
        state: state.clone(),
        generation,
    };
    execd_info!("[adx-execd] tunnel client connected");

    state
        .send_to_generation(
            generation,
            &Frame::Hello {
                protocol_version: configured_protocol_version(),
                max_stream_chunk: configured_stream_chunk_bytes(),
                max_inflight: configured_max_inflight(),
                stream_window_frames: configured_stream_window_frames(),
                max_body_size: configured_max_body_bytes(),
                max_ws_message_size: configured_max_ws_message_bytes(),
                resume: true,
                session_id: None,
            },
        )
        .map_err(|_| "failed to queue tunnel hello".to_string())?;
    // V1 has no hello response. Select it only after the negotiation window;
    // cached small requests are replayed at that point, never while mode is
    // unknown and never as oversized single JSON frames.
    let fallback_state = state.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if fallback_state.select_legacy_if_unnegotiated(generation) {
            fallback_state.protocol_notify.notify_waiters();
            resend_pending_requests(&fallback_state, generation, None);
        }
    });

    // Outbound pump: frames queued by Port B -> client WS.
    let out = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if sink.send(m.message).await.is_err() {
                break;
            }
        }
    });
    let _out_guard = AbortTaskOnDrop(out.abort_handle());

    // Inbound: client frames -> dispatch.
    let mut connection_error = None;
    loop {
        let msg = tokio::select! {
            message = rx_ws.next() => message,
            _ = &mut shutdown_rx => break,
        };
        let Some(msg) = msg else {
            break;
        };
        match msg {
            Ok(Message::Text(t)) => match serde_json::from_str::<Frame>(&t) {
                Ok(frame) => dispatch_from_client(frame, &state, generation).await,
                Err(e) => execd_warn!("[adx-execd] tunnel drop_malformed_frame error={e}"),
            },
            Ok(Message::Binary(raw)) => {
                let max_payload = state
                    .active_protocol()
                    .map(|(_, protocol)| protocol.max_stream_chunk)
                    .unwrap_or_else(configured_stream_chunk_bytes);
                let envelope = match BinaryEnvelope::decode(&raw, max_payload) {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        connection_error = Some(error);
                        break;
                    }
                };
                if let Err(error) = dispatch_binary_from_client(envelope, &state, generation).await
                {
                    connection_error = Some(error);
                    break;
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }

    out.abort();
    let _ = out.await;
    execd_info!("[adx-execd] tunnel client disconnected");
    connection_error.map_or(Ok(()), Err)
}

/// Drop cached requests older than the TTL (and unblock their waiters).
fn cleanup_expired_requests(state: &Arc<State>) {
    let now = Instant::now();
    let expired: Vec<String> = {
        let mut pr = state.pending_requests.lock().unwrap();
        let ex: Vec<String> = pr
            .iter()
            .filter(|(_, cached)| now.duration_since(cached.created_at) > cached.ttl)
            .map(|(k, _)| k.clone())
            .collect();
        for k in &ex {
            pr.remove(k);
        }
        ex
    };
    // Dropping the oneshot sender unblocks the waiting HTTP handler (-> closes conn).
    let mut ph = state.pending_http.lock().unwrap();
    for k in &expired {
        ph.remove(k);
    }
}

/// On client (re)connect, resend any HTTP requests still in flight.
fn resend_pending_requests(state: &Arc<State>, generation: u64, session_id: Option<&str>) {
    cleanup_expired_requests(state);
    let frames: Vec<Frame> = state
        .pending_requests
        .lock()
        .unwrap()
        .values()
        .filter(|cached| cached.session_id.as_deref() == session_id)
        .map(|cached| cached.frame.clone())
        .collect();
    if !frames.is_empty() {
        execd_info!(
            "[adx-execd] tunnel resending_pending_requests count={}",
            frames.len()
        );
        for f in &frames {
            let _ = state.send_to_generation(generation, f);
        }
    }
}

async fn dispatch_from_client(frame: Frame, state: &Arc<State>, generation: u64) {
    if state.active_generation.load(Ordering::Acquire) != generation {
        return;
    }
    match &frame {
        Frame::Hello {
            protocol_version,
            max_stream_chunk,
            max_inflight,
            stream_window_frames,
            max_body_size,
            max_ws_message_size,
            resume,
            session_id,
        } if configured_protocol_version() >= TUNNEL_PROTOCOL_VERSION
            && *protocol_version >= TUNNEL_PROTOCOL_VERSION
            && *max_stream_chunk >= MIN_STREAM_CHUNK_BYTES
            && *max_inflight > 0
            && *stream_window_frames > 0
            && *max_body_size > 0
            && *max_ws_message_size > 0 =>
        {
            let negotiated_session = if *resume {
                session_id
                    .as_ref()
                    .filter(|candidate| uuid_bytes_from_id(candidate).is_ok())
                    .cloned()
            } else {
                None
            };
            if *resume && negotiated_session.is_none() {
                execd_warn!(
                    "[adx-execd] tunnel ignored_invalid_resume_session generation={generation}"
                );
            }
            let negotiated_max_inflight = (*max_inflight).min(configured_max_inflight());
            let protocol = NegotiatedProtocol {
                max_stream_chunk: (*max_stream_chunk).min(configured_stream_chunk_bytes()),
                max_inflight: negotiated_max_inflight,
                stream_window_frames: (*stream_window_frames)
                    .min(configured_stream_window_frames())
                    .min(
                        OUTBOUND_QUEUE_FRAMES
                            .saturating_sub(OUTBOUND_CONTROL_RESERVE)
                            .checked_div(negotiated_max_inflight)
                            .unwrap_or(0)
                            .max(1),
                    ),
                max_body_size: (*max_body_size).min(configured_max_body_bytes()),
                max_ws_message_size: (*max_ws_message_size).min(configured_max_ws_message_bytes()),
                resumable: negotiated_session.is_some(),
            };
            let mut negotiated = state.negotiated.lock().unwrap();
            if let Some(existing) = negotiated.as_ref() {
                if existing.generation == generation {
                    if existing.protocol != protocol {
                        execd_warn!(
                            "[adx-execd] tunnel ignored_changed_duplicate_hello generation={generation}"
                        );
                    }
                    return;
                }
            }
            let was_legacy = state.legacy_generation.load(Ordering::Acquire) == generation;
            *negotiated = Some(NegotiatedConnection {
                generation,
                protocol,
                session_id: negotiated_session.clone(),
                http: Arc::new(Semaphore::new(protocol.max_inflight)),
                websocket: Arc::new(Semaphore::new(protocol.max_inflight)),
            });
            drop(negotiated);
            state.protocol_notify.notify_waiters();
            if let Some(session_id) = negotiated_session.as_deref() {
                for stale in state.resumable_session_ids() {
                    if stale != session_id {
                        state.fail_resumable_session(&stale, "tunnel client session was replaced");
                    }
                }
                let response_windows = state.rebind_resumable_session(session_id, generation);
                resend_pending_requests(state, generation, Some(session_id));
                for (id, credits, ack_offset) in response_windows {
                    let _ = state.send_to_generation(
                        generation,
                        &Frame::Window {
                            id,
                            credits,
                            ack_offset: Some(ack_offset),
                            complete: false,
                        },
                    );
                }
            } else if !was_legacy {
                // A request already sent after the V1 fallback remains valid
                // when a late hello upgrades subsequent traffic.
                resend_pending_requests(state, generation, None);
            }
            execd_info!(
                "[adx-execd] tunnel protocol_v2 negotiated chunk={} inflight={} window={} body={} ws_message={} resume={}",
                protocol.max_stream_chunk,
                protocol.max_inflight,
                protocol.stream_window_frames,
                protocol.max_body_size,
                protocol.max_ws_message_size,
                protocol.resumable
            );
        }
        Frame::Ping { id, timestamp } => {
            let _ = state.send_to_generation(
                generation,
                &Frame::Pong {
                    id: id.clone(),
                    timestamp: *timestamp,
                },
            );
        }
        Frame::HttpResp { id, headers, .. } => {
            if let Some(pending) = state.pending_http.lock().unwrap().remove(id) {
                if !headers_within_limits(headers) {
                    let _ = pending
                        .sender
                        .send(Err("response headers exceed tunnel limit".into()));
                } else if pending.generation.is_none() || pending.generation == Some(generation) {
                    let _ = pending.sender.send(Ok(TunnelResponse::Legacy(frame)));
                } else {
                    let _ = pending
                        .sender
                        .send(Err("response belongs to a stale connection".into()));
                }
            }
        }
        Frame::HttpRespBegin {
            id,
            status,
            headers,
            content_length,
        } => {
            let pending = state.pending_http.lock().unwrap().remove(id);
            if let Some(pending) = pending {
                if state.active_protocol().is_none()
                    || pending
                        .generation
                        .is_some_and(|entry_generation| entry_generation != generation)
                {
                    let _ = pending.sender.send(Err(
                        "streaming response belongs to a stale connection".into(),
                    ));
                    return;
                }
                if !headers_within_limits(headers) {
                    let _ = pending
                        .sender
                        .send(Err("response headers exceed tunnel limit".into()));
                    let _ = state.send_to_generation(
                        generation,
                        &Frame::Error {
                            id: id.clone(),
                            message: "response headers exceed tunnel limit".into(),
                        },
                    );
                    return;
                }
                // The request frame is no longer needed once response metadata
                // arrives. Response-body resume uses its own offset/window state.
                state.pending_requests.lock().unwrap().remove(id);
                state.remove_stream_credits(id);
                let protocol = state
                    .active_protocol()
                    .map(|(_, protocol)| protocol)
                    .unwrap_or(NegotiatedProtocol {
                        max_stream_chunk: configured_stream_chunk_bytes(),
                        max_inflight: configured_max_inflight(),
                        stream_window_frames: configured_stream_window_frames(),
                        max_body_size: configured_max_body_bytes(),
                        max_ws_message_size: configured_max_ws_message_bytes(),
                        resumable: false,
                    });
                if content_length.is_some_and(|length| length > protocol.max_body_size as u64) {
                    let _ = pending
                        .sender
                        .send(Err("response body exceeds negotiated limit".into()));
                    let _ = state.send_to_generation(
                        generation,
                        &Frame::Error {
                            id: id.clone(),
                            message: "response body exceeds negotiated limit".into(),
                        },
                    );
                    state.remember_terminated_stream(generation, id);
                    return;
                }
                let (body_tx, body_rx) = mpsc::channel(protocol.stream_window_frames);
                let queued_frames = Arc::new(AtomicUsize::new(0));
                state.response_streams.lock().unwrap().insert(
                    id.clone(),
                    ResponseStreamSink {
                        generation,
                        session_id: pending.session_id.clone(),
                        sender: body_tx,
                        received: 0,
                        expected: content_length.map(|length| length as usize),
                        max_body_size: protocol.max_body_size,
                        queued_frames: queued_frames.clone(),
                        window: protocol.stream_window_frames,
                    },
                );
                let status = StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_GATEWAY);
                let response = StreamingResponse {
                    generation,
                    status,
                    headers: headers.clone(),
                    content_length: *content_length,
                    body_rx,
                    queued_frames,
                };
                if pending
                    .sender
                    .send(Ok(TunnelResponse::Streaming(response)))
                    .is_ok()
                {
                    let _ = state.send_to_generation(
                        generation,
                        &Frame::Window {
                            id: id.clone(),
                            credits: protocol.stream_window_frames,
                            ack_offset: Some(0),
                            complete: false,
                        },
                    );
                } else {
                    state.response_streams.lock().unwrap().remove(id);
                    state.remember_terminated_stream(generation, id);
                    let _ = state.send_to_generation(
                        generation,
                        &Frame::Error {
                            id: id.clone(),
                            message: "downstream request closed".into(),
                        },
                    );
                }
            }
        }
        Frame::HttpRespEnd { id } => {
            if let Some(sink) = state.response_streams.lock().unwrap().remove(id) {
                let length_mismatch = sink
                    .expected
                    .is_some_and(|expected| expected != sink.received);
                if length_mismatch {
                    let message = "response content length mismatch";
                    let _ = sink.sender.try_send(Err(message.into()));
                    let _ = state.send_to_generation(
                        generation,
                        &Frame::Error {
                            id: id.clone(),
                            message: message.into(),
                        },
                    );
                } else if sink.session_id.is_some() {
                    let _ = state.send_to_generation(
                        generation,
                        &Frame::Window {
                            id: id.clone(),
                            credits: 0,
                            ack_offset: Some(sink.received as u64),
                            complete: true,
                        },
                    );
                }
                state.remember_terminated_stream(generation, id);
            }
        }
        Frame::Window {
            id,
            credits,
            ack_offset,
            ..
        } => {
            if let Some(stream) = state.grant_stream_credits(id, generation, *credits, *ack_offset)
            {
                let state = state.clone();
                let id = id.clone();
                tokio::spawn(async move {
                    resume_upload(state, id, stream).await;
                });
            }
        }
        Frame::Error { id, message } => {
            state.remove_stream_credits(id);
            let pending = state.pending_http.lock().unwrap().remove(id);
            if let Some(pending) = pending {
                let _ = pending.sender.send(Err(message.clone()));
                return;
            }
            let sink = state.response_streams.lock().unwrap().remove(id);
            if let Some(sink) = sink {
                let _ = sink.sender.try_send(Err(message.clone()));
                state.remember_terminated_stream(generation, id);
                return;
            }
            let channel = state.pending_ws.lock().unwrap().get(id).cloned();
            if let Some(channel) = channel.filter(|channel| channel.generation == generation) {
                if channel
                    .sender
                    .try_send(WsTunnelMessage::Control(frame.clone()))
                    .is_err()
                {
                    state.pending_ws.lock().unwrap().remove(id);
                    state.remember_terminated_stream(generation, id);
                }
            }
        }
        Frame::WsConnected { id } | Frame::WsMessage { id, .. } | Frame::WsClose { id, .. } => {
            let channel = state.pending_ws.lock().unwrap().get(id).cloned();
            if let Some(channel) = channel.filter(|channel| channel.generation == generation) {
                if channel
                    .sender
                    .try_send(WsTunnelMessage::Control(frame.clone()))
                    .is_err()
                {
                    state.pending_ws.lock().unwrap().remove(id);
                    state.remember_terminated_stream(generation, id);
                    let _ = state.send_to_generation(
                        generation,
                        &Frame::Error {
                            id: id.clone(),
                            message: "WebSocket channel queue limit reached".into(),
                        },
                    );
                }
            }
        }
        _ => {}
    }
}

async fn dispatch_binary_from_client(
    envelope: BinaryEnvelope,
    state: &Arc<State>,
    generation: u64,
) -> Result<(), String> {
    if state.active_client_generation() != Some(generation) {
        return Ok(());
    }
    match envelope.kind {
        BinaryKind::HttpResponse => {
            let id = envelope.id.to_string();
            let mut duplicate_ack = None;
            let mut queued_frames = None;
            let (sender, chunk_offset) = {
                let mut streams = state.response_streams.lock().unwrap();
                let Some(sink) = streams.get_mut(&id) else {
                    if state.is_terminated_stream(generation, &id) {
                        return Ok(());
                    }
                    return Err(format!("response data for unknown stream: {id}"));
                };
                if sink.generation != generation {
                    return Ok(());
                }
                let chunk_offset = if sink.session_id.is_some() {
                    envelope.offset.ok_or_else(|| {
                        format!("resumable response chunk is missing offset: {id}")
                    })?
                } else {
                    sink.received as u64
                };
                let chunk_end = chunk_offset
                    .checked_add(envelope.payload.len() as u64)
                    .ok_or_else(|| format!("response offset overflow for stream: {id}"))?;
                if chunk_offset < sink.received as u64 {
                    if chunk_end <= sink.received as u64 {
                        duplicate_ack = Some(sink.received as u64);
                        (sink.sender.clone(), chunk_offset)
                    } else {
                        return Err(format!("overlapping response chunk for stream: {id}"));
                    }
                } else if chunk_offset > sink.received as u64 {
                    return Err(format!("response chunk gap for stream: {id}"));
                } else {
                    let total = usize::try_from(chunk_end)
                        .map_err(|_| format!("response size overflow for stream: {id}"))?;
                    if total > sink.max_body_size
                        || sink.expected.is_some_and(|expected| total > expected)
                    {
                        let message = "response body exceeds advertised or negotiated limit";
                        let sink = streams.remove(&id).expect("stream exists");
                        let _ = sink.sender.try_send(Err(message.into()));
                        drop(streams);
                        state.remember_terminated_stream(generation, &id);
                        let _ = state.send_to_generation(
                            generation,
                            &Frame::Error {
                                id,
                                message: message.into(),
                            },
                        );
                        return Ok(());
                    }
                    sink.received = total;
                    sink.queued_frames.fetch_add(1, Ordering::AcqRel);
                    queued_frames = Some(sink.queued_frames.clone());
                    (sink.sender.clone(), chunk_offset)
                }
            };
            if let Some(ack_offset) = duplicate_ack {
                state.send_response_window(&id, 0, ack_offset);
                return Ok(());
            }
            let queued_frames = queued_frames.expect("new response chunks track queue depth");
            if sender
                .try_send(Ok(ResponseChunk {
                    offset: chunk_offset,
                    payload: envelope.payload,
                }))
                .is_err()
            {
                queued_frames.fetch_sub(1, Ordering::AcqRel);
                state.response_streams.lock().unwrap().remove(&id);
                state.remember_terminated_stream(generation, &id);
                let _ = state.send_to_generation(
                    generation,
                    &Frame::Error {
                        id,
                        message: "downstream response closed".into(),
                    },
                );
                return Ok(());
            }
            if envelope.end_of_body {
                if let Some(sink) = state.response_streams.lock().unwrap().remove(&id) {
                    if sink
                        .expected
                        .is_some_and(|expected| expected != sink.received)
                    {
                        let _ = sink
                            .sender
                            .try_send(Err("response content length mismatch".into()));
                        let _ = state.send_to_generation(
                            generation,
                            &Frame::Error {
                                id: id.clone(),
                                message: "response content length mismatch".into(),
                            },
                        );
                    }
                    state.remember_terminated_stream(generation, &id);
                }
            }
            Ok(())
        }
        BinaryKind::HttpRequest => {
            Err("TunnelClient sent request-body data in the wrong direction".into())
        }
        BinaryKind::WebSocket => {
            let id = envelope.id.clone();
            let channel = state.pending_ws.lock().unwrap().get(&id).cloned();
            let Some(channel) = channel else {
                return if state.is_terminated_stream(generation, &id) {
                    Ok(())
                } else {
                    Err(format!("WebSocket data for unknown channel: {id}"))
                };
            };
            if channel.generation != generation {
                return Ok(());
            }
            if channel
                .sender
                .try_send(WsTunnelMessage::Binary(envelope))
                .is_err()
            {
                state.pending_ws.lock().unwrap().remove(&id);
                state.remember_terminated_stream(generation, &id);
                let _ = state.send_to_generation(
                    generation,
                    &Frame::Error {
                        id,
                        message: "WebSocket channel queue limit reached".into(),
                    },
                );
            }
            Ok(())
        }
    }
}

// ───────────────────────── Port B: sandbox HTTP ─────────────────────────
async fn accept_port_b(listener: TcpListener, state: Arc<State>) {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = connections.join_next(), if !connections.is_empty() => continue,
        };
        match accepted {
            Ok((stream, _)) => {
                let st = state.clone();
                connections.spawn(async move {
                    if let Err(error) = handle_port_b(stream, st).await {
                        execd_warn!("[adx-execd] tunnel port_b_connection_ended error={error}");
                    }
                });
            }
            Err(e) => {
                execd_error!("[adx-execd] tunnel port_b_accept_error error={e}");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn handle_port_b(stream: TcpStream, state: Arc<State>) -> Result<(), String> {
    if super::control::current().is_some_and(|controller| {
        controller.status().phase != adx_core::runtime::RuntimePhase::Running
    }) {
        return Err("runtime checkpoint transition in progress".into());
    }
    match request_is_websocket_upgrade(&stream).await? {
        Some(true) => handle_port_b_ws(stream, state).await,
        Some(false) => handle_port_b_http(stream, state).await,
        None => Ok(()),
    }
}

async fn request_is_websocket_upgrade(stream: &TcpStream) -> Result<Option<bool>, String> {
    // Peek until the complete request head is present, so a fragmented Upgrade
    // header cannot be mistaken for ordinary HTTP. Peeking preserves the bytes
    // for tungstenite or Hyper to parse afterwards.
    let deadline = tokio::time::Instant::now() + WS_CONNECT_TIMEOUT;
    let mut head = vec![0u8; 8192];
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err("timed out reading tunnel request headers".into());
        }
        let count = tokio::time::timeout(remaining, stream.peek(&mut head))
            .await
            .map_err(|_| "timed out reading tunnel request headers".to_string())?
            .map_err(|error| error.to_string())?;
        if count == 0 {
            return Ok(None);
        }
        if let Some(end) = head[..count]
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
        {
            let request_head = String::from_utf8_lossy(&head[..end + 4]);
            return Ok(Some(request_head.lines().skip(1).any(|line| {
                line.split_once(':').is_some_and(|(name, value)| {
                    name.eq_ignore_ascii_case("upgrade")
                        && value
                            .split(',')
                            .any(|token| token.trim().eq_ignore_ascii_case("websocket"))
                })
            })));
        }
        if count == head.len() {
            if head.len() >= MAX_HTTP_HEADER_BYTES {
                return Err("tunnel request headers exceed limit".into());
            }
            head.resize((head.len() * 2).min(MAX_HTTP_HEADER_BYTES), 0);
        } else {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}

fn is_fixed_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn connection_tokens(headers: &HeaderList) -> HashSet<String> {
    headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("connection"))
        .flat_map(|(_, value)| value.split(','))
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| !token.is_empty())
        .collect()
}

fn header_list(headers: &HeaderMap) -> HeaderList {
    headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect()
}

fn request_headers_for_frame(headers: &HeaderMap) -> HeaderList {
    let headers = header_list(headers);
    let dynamic = connection_tokens(&headers);
    headers
        .into_iter()
        .filter(|(name, _)| {
            let lower = name.to_ascii_lowercase();
            !is_fixed_hop_by_hop(&lower)
                && !dynamic.contains(&lower)
                && !matches!(lower.as_str(), "host" | "content-length" | "expect")
        })
        .collect()
}

fn response_headers_for_downstream(
    headers: HeaderList,
    method: &Method,
    status: StatusCode,
    body_len: usize,
) -> HeaderList {
    let dynamic = connection_tokens(&headers);
    let representation_length = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .filter_map(|(_, value)| value.parse::<usize>().ok())
        .next();
    let mut result: HeaderList = headers
        .into_iter()
        .filter(|(name, _)| {
            let lower = name.to_ascii_lowercase();
            !is_fixed_hop_by_hop(&lower) && !dynamic.contains(&lower) && lower != "content-length"
        })
        .collect();

    if method == Method::HEAD {
        if let Some(length) = representation_length {
            result.push(("content-length".into(), length.to_string()));
        }
    } else if !status.is_informational()
        && status != StatusCode::NO_CONTENT
        && status != StatusCode::NOT_MODIFIED
    {
        result.push(("content-length".into(), body_len.to_string()));
    }
    result
}

fn response_headers_for_stream(
    headers: HeaderList,
    method: &Method,
    status: StatusCode,
    content_length: Option<u64>,
) -> HeaderList {
    let dynamic = connection_tokens(&headers);
    let representation_length = content_length.or_else(|| {
        headers
            .iter()
            .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .find_map(|(_, value)| value.parse::<u64>().ok())
    });
    let mut result: HeaderList = headers
        .into_iter()
        .filter(|(name, _)| {
            let lower = name.to_ascii_lowercase();
            !is_fixed_hop_by_hop(&lower) && !dynamic.contains(&lower) && lower != "content-length"
        })
        .collect();
    if method == Method::HEAD {
        if let Some(length) = representation_length {
            result.push(("content-length".into(), length.to_string()));
        }
    } else if !status.is_informational()
        && status != StatusCode::NO_CONTENT
        && status != StatusCode::NOT_MODIFIED
    {
        if let Some(length) = content_length {
            result.push(("content-length".into(), length.to_string()));
        }
    }
    result
}

fn boxed_full(body: Bytes) -> TunnelBody {
    Full::new(body)
        .map_err(|never: Infallible| match never {})
        .boxed_unsync()
}

fn plain_response(status: StatusCode, message: &str) -> Response<TunnelBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_LENGTH, message.len())
        .body(boxed_full(Bytes::copy_from_slice(message.as_bytes())))
        .expect("static HTTP response is valid")
}

fn build_response(
    status: StatusCode,
    headers: HeaderList,
    body: TunnelBody,
) -> Response<TunnelBody> {
    let mut response = Response::builder().status(status);
    for (name, value) in headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            response
                .headers_mut()
                .expect("response builder")
                .append(name, value);
        }
    }
    response
        .body(body)
        .unwrap_or_else(|_| plain_response(StatusCode::BAD_GATEWAY, "Invalid response headers"))
}

async fn resume_upload(state: Arc<State>, id: String, credits: Arc<StreamCredits>) {
    let _send_guard = credits.send_lock.lock().await;
    if !credits.resuming.load(Ordering::Acquire) || credits.closed.load(Ordering::Acquire) {
        return;
    }

    'resume: loop {
        for (offset, payload) in credits.unacked() {
            let generation = match credits.acquire(true).await {
                Ok(generation) => generation,
                Err(()) => return,
            };
            let message = match (BinaryEnvelope {
                id: id.clone(),
                kind: BinaryKind::HttpRequest,
                payload,
                end_of_body: false,
                offset: Some(offset),
            })
            .encode(configured_stream_chunk_bytes())
            {
                Ok(message) => message,
                Err(_) => {
                    state.remove_stream_credits(&id);
                    return;
                }
            };
            if state
                .send_data_message_for_generation_wait(generation, message)
                .await
                .is_err()
            {
                credits.disconnect(generation);
                continue 'resume;
            }
        }

        if credits.ended.load(Ordering::Acquire) {
            let generation = match credits.wait_until_connected().await {
                Ok(generation) => generation,
                Err(()) => return,
            };
            if state
                .send_to_generation(generation, &Frame::HttpReqEnd { id: id.clone() })
                .is_err()
            {
                credits.disconnect(generation);
                continue 'resume;
            }
        }
        credits.finish_resume();
        return;
    }
}

async fn proxy_streaming_http_request(
    request: StreamingHttpRequest,
    state: Arc<State>,
    generation: u64,
    protocol: NegotiatedProtocol,
    session_id: Option<String>,
    permit: OwnedSemaphorePermit,
) -> Response<TunnelBody> {
    let StreamingHttpRequest {
        method,
        path,
        headers,
        content_length,
        mut body,
    } = request;
    if content_length.is_some_and(|length| length > protocol.max_body_size as u64) {
        return plain_response(StatusCode::PAYLOAD_TOO_LARGE, "request body exceeds limit");
    }
    let session_id = protocol.resumable.then_some(session_id).flatten();
    let id = make_id();
    let credits = state.register_stream_credits(
        &id,
        generation,
        session_id.clone(),
        protocol.stream_window_frames,
    );
    let (response_tx, response_rx) = oneshot::channel();
    state.pending_http.lock().unwrap().insert(
        id.clone(),
        PendingHttpResponse {
            generation: Some(generation),
            session_id: session_id.clone(),
            sender: response_tx,
        },
    );
    let mut request_guard = PendingRequestGuard::new(state.clone(), generation, id.clone());
    let begin = Frame::HttpReqBegin {
        id: id.clone(),
        method: method.to_string(),
        path,
        headers,
        content_length,
    };
    if let Some(session_id) = session_id.clone() {
        state.pending_requests.lock().unwrap().insert(
            id.clone(),
            CachedRequest {
                frame: begin.clone(),
                created_at: Instant::now(),
                session_id: Some(session_id),
                ttl: state.http_timeout,
            },
        );
    }
    if state.send_to_generation(generation, &begin).is_err() && session_id.is_none() {
        return plain_response(StatusCode::BAD_GATEWAY, "Tunnel client is not connected");
    }

    let mut received = 0usize;
    let mut sent_offset = 0u64;
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => {
                return plain_response(StatusCode::BAD_REQUEST, &error.to_string());
            }
        };
        let Ok(data) = frame.into_data() else {
            continue;
        };
        received = match received.checked_add(data.len()) {
            Some(total) if total <= protocol.max_body_size => total,
            _ => {
                return plain_response(StatusCode::PAYLOAD_TOO_LARGE, "request body exceeds limit");
            }
        };
        for chunk in data.chunks(protocol.max_stream_chunk) {
            let chunk = Bytes::copy_from_slice(chunk);
            let chunk_offset = sent_offset;
            let stream_generation =
                match tokio::time::timeout(state.http_timeout, credits.acquire(false)).await {
                    Ok(Ok(generation)) => generation,
                    _ => {
                        return plain_response(
                            StatusCode::BAD_GATEWAY,
                            "Tunnel request stream lost",
                        );
                    }
                };
            if session_id.is_some() {
                credits.record(chunk_offset, chunk.clone());
            }
            let message = BinaryEnvelope {
                id: id.clone(),
                kind: BinaryKind::HttpRequest,
                payload: chunk.clone(),
                end_of_body: false,
                offset: session_id.as_ref().map(|_| chunk_offset),
            }
            .encode(protocol.max_stream_chunk)
            .expect("request chunk is bounded by the negotiated size");
            if state
                .send_data_message_for_generation_wait(stream_generation, message)
                .await
                .is_err()
            {
                credits.disconnect(stream_generation);
                if session_id.is_none() {
                    return plain_response(
                        StatusCode::BAD_GATEWAY,
                        "Tunnel request stream disconnected",
                    );
                }
            }
            sent_offset = sent_offset.saturating_add(chunk.len() as u64);
        }
    }
    if content_length.is_some_and(|expected| expected != received as u64) {
        return plain_response(StatusCode::BAD_REQUEST, "Request content length mismatch");
    }
    credits.ended.store(true, Ordering::Release);
    let end_generation =
        match tokio::time::timeout(state.http_timeout, credits.wait_until_ready()).await {
            Ok(Ok(generation)) => generation,
            _ => return plain_response(StatusCode::BAD_GATEWAY, "Tunnel request stream lost"),
        };
    if state
        .send_to_generation(end_generation, &Frame::HttpReqEnd { id: id.clone() })
        .is_err()
    {
        credits.disconnect(end_generation);
        if session_id.is_none()
            || tokio::time::timeout(state.http_timeout, credits.wait_until_ready())
                .await
                .is_err()
        {
            return plain_response(
                StatusCode::BAD_GATEWAY,
                "Tunnel request stream disconnected",
            );
        }
    }
    if session_id.is_none() {
        state.remove_stream_credits(&id);
    }

    let streaming_response = match tokio::time::timeout(state.http_timeout, response_rx).await {
        Ok(Ok(Ok(TunnelResponse::Streaming(response)))) => response,
        Ok(Ok(Ok(TunnelResponse::Legacy(_)))) => {
            return plain_response(StatusCode::BAD_GATEWAY, "Invalid legacy tunnel response");
        }
        Ok(Ok(Err(message))) => {
            return plain_response(StatusCode::BAD_GATEWAY, &message);
        }
        Ok(Err(_)) => {
            return plain_response(StatusCode::BAD_GATEWAY, "Tunnel response stream closed");
        }
        Err(_) => {
            return plain_response(StatusCode::GATEWAY_TIMEOUT, "Tunnel timeout");
        }
    };

    request_guard.disarm();
    streaming_response_to_downstream(method, id, state, streaming_response, permit)
}

fn streaming_response_to_downstream(
    method: Method,
    id: String,
    state: Arc<State>,
    response: StreamingResponse,
    permit: OwnedSemaphorePermit,
) -> Response<TunnelBody> {
    let StreamingResponse {
        generation,
        status,
        headers,
        content_length,
        body_rx,
        queued_frames,
        ..
    } = response;
    let downstream_headers = response_headers_for_stream(headers, &method, status, content_length);
    let suppress_body = method == Method::HEAD
        || status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED;
    if suppress_body {
        if state.response_streams.lock().unwrap().remove(&id).is_some() {
            state.remember_terminated_stream(generation, &id);
        }
        return build_response(status, downstream_headers, boxed_full(Bytes::new()));
    }

    let ack_state = state.clone();
    let ack_id = id.clone();
    let permit = permit;
    let stream_guard = DownstreamStreamGuard {
        state: state.clone(),
        generation,
        id: id.clone(),
    };
    let stream = ReceiverStream::new(body_rx).map(move |item| {
        let _hold_permit = &permit;
        let _hold_stream_guard = &stream_guard;
        match item {
            Ok(chunk) => {
                queued_frames.fetch_sub(1, Ordering::AcqRel);
                let ack_offset = chunk.offset.saturating_add(chunk.payload.len() as u64);
                ack_state.send_response_window(&ack_id, 1, ack_offset);
                Ok(BodyFrame::data(chunk.payload))
            }
            Err(message) => Err::<BodyFrame<Bytes>, BoxError>(Box::new(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                message,
            ))),
        }
    });
    build_response(
        status,
        downstream_headers,
        StreamBody::new(stream).boxed_unsync(),
    )
}

async fn proxy_http_request(request: Request<Incoming>, state: Arc<State>) -> Response<TunnelBody> {
    let (parts, body) = request.into_parts();
    let method = parts.method;
    let is_websocket_upgrade = parts
        .headers
        .get_all(UPGRADE)
        .iter()
        .any(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"));
    if is_websocket_upgrade {
        return plain_response(
            StatusCode::NOT_IMPLEMENTED,
            "Reverse WebSocket tunnel is not implemented",
        );
    }
    let path = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| "/".into());
    let headers = request_headers_for_frame(&parts.headers);
    let content_length = parts
        .headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or_else(|| body.size_hint().exact());
    let local_permit = match state.http_permits.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            return plain_response(
                StatusCode::TOO_MANY_REQUESTS,
                "Tunnel max_inflight limit reached",
            );
        }
    };
    let connection = state.wait_for_protocol().await;
    if connection == ConnectionProtocol::Disconnected {
        return plain_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Tunnel client is not connected",
        );
    }
    let permit = if let ConnectionProtocol::Streaming {
        generation,
        protocol,
        session_id,
    } = &connection
    {
        let negotiated_permit = match state.try_acquire_negotiated_permit(*generation, false) {
            Ok(permit) => permit,
            Err(_) => {
                return plain_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    "Tunnel negotiated max_inflight limit reached",
                );
            }
        };
        drop(local_permit);
        if content_length.is_none_or(|length| length > configured_fast_path_body_bytes()) {
            return proxy_streaming_http_request(
                StreamingHttpRequest {
                    method,
                    path,
                    headers,
                    content_length,
                    body,
                },
                state,
                *generation,
                *protocol,
                session_id.clone(),
                negotiated_permit,
            )
            .await;
        }
        negotiated_permit
    } else {
        local_permit
    };
    let body_limit = match &connection {
        ConnectionProtocol::Legacy { .. } => MAX_V1_BODY_BYTES,
        ConnectionProtocol::Streaming { protocol, .. } => {
            protocol.max_body_size.min(MAX_V1_BODY_BYTES)
        }
        ConnectionProtocol::Disconnected => unreachable!(),
    };
    if content_length.is_some_and(|length| length > body_limit as u64) {
        return plain_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body requires tunnel protocol V2 streaming",
        );
    }
    let body = match Limited::new(body, body_limit).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => {
            let message = error.to_string();
            let status = if error.downcast_ref::<LengthLimitError>().is_some() {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            };
            return plain_response(status, &message);
        }
    };
    let id = make_id();
    let frame = Frame::HttpReq {
        id: id.clone(),
        method: method.to_string(),
        path,
        headers,
        body: b64().encode(&body),
    };
    let (tx, rx) = oneshot::channel();
    state.pending_http.lock().unwrap().insert(
        id.clone(),
        PendingHttpResponse {
            generation: match &connection {
                ConnectionProtocol::Streaming { generation, .. } => Some(*generation),
                _ => None,
            },
            session_id: match &connection {
                ConnectionProtocol::Streaming {
                    protocol,
                    session_id,
                    ..
                } if protocol.resumable => session_id.clone(),
                _ => None,
            },
            sender: tx,
        },
    );
    // V1 keeps its historical replay behavior. Resumable V2 additionally
    // relies on the client session to deduplicate this stable request id.
    let cached_session = match &connection {
        ConnectionProtocol::Streaming {
            protocol,
            session_id,
            ..
        } if protocol.resumable => session_id.clone(),
        _ => None,
    };
    if matches!(connection, ConnectionProtocol::Legacy { .. }) || cached_session.is_some() {
        state.pending_requests.lock().unwrap().insert(
            id.clone(),
            CachedRequest {
                frame: frame.clone(),
                created_at: Instant::now(),
                session_id: cached_session.clone(),
                ttl: if cached_session.is_some() {
                    state.http_timeout
                } else {
                    PENDING_REQUEST_TTL
                },
            },
        );
    }
    let generation = match &connection {
        ConnectionProtocol::Legacy { generation }
        | ConnectionProtocol::Streaming { generation, .. } => *generation,
        ConnectionProtocol::Disconnected => unreachable!(),
    };
    let mut request_guard = PendingRequestGuard::new(state.clone(), generation, id.clone());
    if state.send_to_generation(generation, &frame).is_err() && cached_session.is_none() {
        return plain_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Tunnel client is not connected",
        );
    }

    let result = tokio::time::timeout(state.http_timeout, rx).await;

    let (status, headers, response_body) = match result {
        Ok(Ok(Ok(TunnelResponse::Legacy(Frame::HttpResp {
            status,
            headers,
            body,
            ..
        })))) => {
            let response_body = match b64().decode(body.as_bytes()) {
                Ok(body) => body,
                Err(_) => {
                    return plain_response(StatusCode::BAD_GATEWAY, "Invalid tunnel response body");
                }
            };
            if response_body.len() > body_limit {
                return plain_response(
                    StatusCode::BAD_GATEWAY,
                    "Tunnel response body exceeds limit",
                );
            }
            (
                StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
                headers,
                response_body,
            )
        }
        Ok(Ok(Ok(TunnelResponse::Streaming(response)))) => {
            state.pending_requests.lock().unwrap().remove(&id);
            request_guard.disarm();
            return streaming_response_to_downstream(method, id, state, response, permit);
        }
        Ok(Ok(Err(message))) => {
            return plain_response(StatusCode::BAD_GATEWAY, &message);
        }
        Ok(Ok(Ok(TunnelResponse::Legacy(_)))) | Ok(Err(_)) => {
            return plain_response(StatusCode::BAD_GATEWAY, "Invalid tunnel response");
        }
        Err(_) => {
            return plain_response(StatusCode::GATEWAY_TIMEOUT, "Tunnel timeout");
        }
    };
    state.pending_requests.lock().unwrap().remove(&id);
    request_guard.disarm();

    let downstream_headers =
        response_headers_for_downstream(headers, &method, status, response_body.len());
    let suppress_body = method == Method::HEAD
        || status.is_informational()
        || status == StatusCode::NO_CONTENT
        || status == StatusCode::NOT_MODIFIED;
    build_response(
        status,
        downstream_headers,
        boxed_full(if suppress_body {
            Bytes::new()
        } else {
            Bytes::from(response_body)
        }),
    )
}

async fn handle_port_b_http(stream: TcpStream, state: Arc<State>) -> Result<(), String> {
    let service = service_fn(move |request| {
        let state = state.clone();
        async move { Ok::<Response<TunnelBody>, Infallible>(proxy_http_request(request, state).await) }
    });
    match http1::Builder::new()
        .max_headers(MAX_HTTP_HEADERS)
        .max_buf_size(MAX_HTTP_HEADER_BYTES)
        .serve_connection(TokioIo::new(stream), service)
        .await
    {
        Ok(()) => Ok(()),
        // A downstream that closes mid-request is already handled by the
        // request guard. It is a cancellation, not a tunnel health failure.
        Err(error) if error.is_incomplete_message() => Ok(()),
        Err(error) => Err(format!("port B HTTP connection: {error}")),
    }
}

async fn send_ws_binary_to_client(
    state: &State,
    generation: u64,
    id: &str,
    data: &[u8],
    max_stream_chunk: usize,
) -> Result<(), ()> {
    if data.is_empty() {
        return state
            .send_data_message_for_generation_wait(
                generation,
                BinaryEnvelope {
                    id: id.to_string(),
                    kind: BinaryKind::WebSocket,
                    payload: Bytes::new(),
                    end_of_body: true,
                    offset: None,
                }
                .encode(max_stream_chunk)
                .map_err(|_| ())?,
            )
            .await;
    }
    let chunk_count = data.len().div_ceil(max_stream_chunk);
    for (index, chunk) in data.chunks(max_stream_chunk).enumerate() {
        state
            .send_data_message_for_generation_wait(
                generation,
                BinaryEnvelope {
                    id: id.to_string(),
                    kind: BinaryKind::WebSocket,
                    payload: Bytes::copy_from_slice(chunk),
                    end_of_body: index + 1 == chunk_count,
                    offset: None,
                }
                .encode(max_stream_chunk)
                .map_err(|_| ())?,
            )
            .await?;
    }
    Ok(())
}

#[allow(clippy::result_large_err)] // Tungstenite fixes the handshake callback error type.
async fn handle_port_b_ws(stream: TcpStream, state: Arc<State>) -> Result<(), String> {
    let local_permit = state
        .ws_permits
        .clone()
        .try_acquire_owned()
        .map_err(|_| "tunnel max WebSocket channel limit reached".to_string())?;
    let connection = state.wait_for_protocol().await;
    let generation = match connection {
        ConnectionProtocol::Legacy { generation }
        | ConnectionProtocol::Streaming { generation, .. } => generation,
        ConnectionProtocol::Disconnected => return Err("tunnel client is not connected".into()),
    };
    let _permit = match connection {
        ConnectionProtocol::Streaming { .. } => {
            let permit = state
                .try_acquire_negotiated_permit(generation, true)
                .map_err(|_| "tunnel negotiated WebSocket channel limit reached".to_string())?;
            drop(local_permit);
            permit
        }
        _ => local_permit,
    };
    let (max_ws_message_size, max_stream_chunk) = match connection {
        ConnectionProtocol::Streaming { protocol, .. } => {
            (protocol.max_ws_message_size, protocol.max_stream_chunk)
        }
        _ => (
            configured_max_body_bytes().min(MAX_V1_BODY_BYTES),
            configured_stream_chunk_bytes(),
        ),
    };
    // Capture the request path and end-to-end handshake headers before
    // tungstenite writes the downstream 101 response.
    let captured: Arc<Mutex<(String, HashMap<String, String>)>> =
        Arc::new(Mutex::new((String::from("/"), HashMap::new())));
    let capture = captured.clone();
    let ws = tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
         response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            let mut captured = capture.lock().unwrap();
            captured.0 = request
                .uri()
                .path_and_query()
                .map(|path| path.as_str().to_string())
                .unwrap_or_else(|| "/".into());
            for (name, value) in request.headers() {
                if !name.as_str().eq_ignore_ascii_case("host") {
                    captured.1.insert(
                        name.as_str().to_string(),
                        value.to_str().unwrap_or("").to_string(),
                    );
                }
            }
            Ok(response)
        },
        Some(WebSocketConfig {
            max_message_size: Some(max_ws_message_size),
            max_frame_size: Some(max_ws_message_size),
            ..WebSocketConfig::default()
        }),
    )
    .await
    .map_err(|error| format!("port B ws accept: {error}"))?;
    let (path, headers) = {
        let captured = captured.lock().unwrap();
        (captured.0.clone(), captured.1.clone())
    };

    let (mut sink, mut source) = ws.split();
    let id = make_id();
    let ws_message_frames = max_ws_message_size.div_ceil(max_stream_chunk);
    let (queue_tx, mut queue_rx) = mpsc::channel::<WsTunnelMessage>(
        configured_stream_window_frames()
            .max(DEFAULT_STREAM_WINDOW_FRAMES)
            .max(ws_message_frames + 2),
    );
    state.pending_ws.lock().unwrap().insert(
        id.clone(),
        PendingWsChannel {
            generation,
            sender: queue_tx,
        },
    );
    let _pending_guard = PendingWsGuard {
        state: state.clone(),
        generation,
        id: id.clone(),
    };

    if state
        .send_to_generation(
            generation,
            &Frame::WsConnect {
                id: id.clone(),
                path,
                headers,
            },
        )
        .is_err()
    {
        return Ok(());
    }

    match tokio::time::timeout(WS_CONNECT_TIMEOUT, queue_rx.recv()).await {
        Ok(Some(WsTunnelMessage::Control(Frame::WsConnected { .. }))) => {}
        _ => return Ok(()),
    }

    let mut incoming_binary = Vec::new();
    loop {
        tokio::select! {
            biased;
            message = source.next() => match message {
                Some(Ok(Message::Text(data))) => {
                    let forwarded = state.send_to_generation(generation, &Frame::WsMessage {
                        id: id.clone(),
                        data,
                        binary: false,
                    });
                    if forwarded.is_err() {
                        let _ = state.send_to_generation(generation, &Frame::Error {
                            id: id.clone(),
                            message: "WebSocket text message exceeds control-frame limit".into(),
                        });
                        break;
                    }
                }
                Some(Ok(Message::Binary(data))) => {
                    if let Some((active_generation, protocol)) = state.active_protocol() {
                        if active_generation != generation {
                            break;
                        }
                        if data.len() > protocol.max_ws_message_size {
                            let _ = state.send_to_generation(generation, &Frame::Error {
                                id: id.clone(),
                                message: "WebSocket binary message exceeds limit".into(),
                            });
                            break;
                        }
                        if send_ws_binary_to_client(
                            &state,
                            generation,
                            &id,
                            &data,
                            protocol.max_stream_chunk,
                        ).await.is_err() {
                            break;
                        }
                    } else {
                        let _ = state.send_to_generation(generation, &Frame::WsMessage {
                            id: id.clone(),
                            data: b64().encode(&data),
                            binary: true,
                        });
                    }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => {
                    let _ = state.send_to_generation(generation, &Frame::WsClose {
                        id: id.clone(),
                        code: 1000,
                        reason: String::new(),
                    });
                    break;
                }
                _ => {}
            },
            frame = queue_rx.recv() => match frame {
                Some(WsTunnelMessage::Control(Frame::WsMessage { data, binary, .. })) => {
                    let message = if binary {
                        Message::Binary(
                            b64().decode(data.as_bytes()).unwrap_or_default(),
                        )
                    } else {
                        Message::Text(data)
                    };
                    if sink.send(message).await.is_err() {
                        break;
                    }
                }
                Some(WsTunnelMessage::Binary(envelope)) => {
                    let total = incoming_binary
                        .len()
                        .checked_add(envelope.payload.len())
                        .ok_or_else(|| "WebSocket binary message size overflow".to_string())?;
                    if total > max_ws_message_size {
                        let _ = state.send_to_generation(generation, &Frame::Error {
                            id: id.clone(),
                            message: "WebSocket binary message exceeds limit".into(),
                        });
                        let _ = sink.send(Message::Close(None)).await;
                        break;
                    }
                    incoming_binary.extend_from_slice(&envelope.payload);
                    if envelope.end_of_body
                        && sink
                            .send(Message::Binary(std::mem::take(&mut incoming_binary)))
                            .await
                            .is_err()
                    {
                        break;
                    }
                }
                Some(WsTunnelMessage::Control(Frame::WsClose { .. }))
                | Some(WsTunnelMessage::Control(Frame::Error { .. }))
                | None => {
                    let _ = sink.send(Message::Close(None)).await;
                    break;
                }
                _ => {}
            },
        }
    }
    Ok(())
}

// ───────────────────────── E2E regression tests ─────────────────────────
// Drive the real server over localhost: a fake (Rust) TunnelClient on Port A
// and raw HTTP / a WS client on Port B, exercising the actual frame protocol
// the python TunnelClient also speaks.
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::connect_async;

    #[test]
    fn http_headers_decode_legacy_maps_and_preserve_ordered_duplicates() {
        for kind in ["http_resp", "http_resp_begin"] {
            for headers in [
                serde_json::json!({"X-Legacy": "value"}),
                serde_json::json!([["Set-Cookie", "a=1"], ["Set-Cookie", "b=2"]]),
            ] {
                let frame: Frame = serde_json::from_value(serde_json::json!({
                    "type": kind, "id": "request", "status": 200, "headers": headers,
                }))
                .unwrap();
                let decoded = match frame {
                    Frame::HttpResp { headers, .. } | Frame::HttpRespBegin { headers, .. } => {
                        headers
                    }
                    _ => panic!("unexpected frame"),
                };
                if headers.is_object() {
                    assert_eq!(decoded, vec![("X-Legacy".into(), "value".into())]);
                } else {
                    assert_eq!(
                        decoded,
                        vec![
                            ("Set-Cookie".into(), "a=1".into()),
                            ("Set-Cookie".into(), "b=2".into())
                        ]
                    );
                }
            }
            assert!(serde_json::from_value::<Frame>(serde_json::json!({
                "type": kind, "id": "bad", "status": 200, "headers": {"X-Bad": 42},
            }))
            .is_err());
        }
    }

    #[test]
    fn legacy_request_encoding_is_selected_per_connection() {
        let frame = Frame::HttpReq {
            id: "request".into(),
            method: "GET".into(),
            path: "/".into(),
            body: String::new(),
            headers: vec![
                ("X-Tag".into(), "one".into()),
                ("X-Tag".into(), "two".into()),
            ],
        };
        let legacy: serde_json::Value =
            serde_json::from_str(control_message(&frame, true).unwrap().to_text().unwrap())
                .unwrap();
        assert_eq!(legacy["headers"], serde_json::json!({"X-Tag": "two"}));
        let current: serde_json::Value =
            serde_json::from_str(control_message(&frame, false).unwrap().to_text().unwrap())
                .unwrap();
        assert_eq!(
            current["headers"],
            serde_json::json!([["X-Tag", "one"], ["X-Tag", "two"]])
        );
    }

    #[test]
    fn tunnel_http_timeout_accepts_positive_integer_and_fractional_seconds() {
        assert_eq!(
            parse_positive_duration_seconds("5"),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            parse_positive_duration_seconds("12.5"),
            Some(Duration::from_millis(12_500))
        );
        assert_eq!(
            parse_positive_duration_seconds("0.125"),
            Some(Duration::from_millis(125))
        );
    }

    #[test]
    fn tunnel_http_timeout_falls_back_for_missing_or_invalid_values() {
        assert_eq!(http_timeout_from_raw(None), DEFAULT_HTTP_TIMEOUT);
        for raw in ["", "invalid", "0", "-1", "NaN", "inf", "1e500", "1e-20"] {
            assert_eq!(
                http_timeout_from_raw(Some(raw)),
                DEFAULT_HTTP_TIMEOUT,
                "raw={raw:?}"
            );
        }
    }

    async fn spawn_test_server() -> (u16, u16) {
        let porta = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let portb = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let wp = porta.local_addr().unwrap().port();
        let hp = portb.local_addr().unwrap().port();
        tokio::spawn(serve(porta, portb, Arc::new(State::default())));
        (wp, hp)
    }

    async fn spawn_test_server_with_http_timeout(http_timeout: Duration) -> (u16, u16) {
        let porta = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let portb = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_port = porta.local_addr().unwrap().port();
        let http_port = portb.local_addr().unwrap().port();
        tokio::spawn(serve(
            porta,
            portb,
            Arc::new(State::with_http_timeout(http_timeout)),
        ));
        (ws_port, http_port)
    }

    async fn connect_client(
        ws_port: u16,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>> {
        let (c, _) = connect_async(format!("ws://127.0.0.1:{ws_port}/"))
            .await
            .unwrap();
        let mut c = c;
        match next_frame(&mut c).await {
            Frame::Hello {
                protocol_version,
                max_stream_chunk,
                ..
            } => {
                assert_eq!(protocol_version, TUNNEL_PROTOCOL_VERSION);
                assert_eq!(max_stream_chunk, DEFAULT_STREAM_CHUNK_BYTES);
            }
            other => panic!("expected server hello, got {other:?}"),
        }
        // Let the server register this as the active client.
        tokio::time::sleep(Duration::from_millis(100)).await;
        c
    }

    async fn connect_v2_client(
        ws_port: u16,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>> {
        connect_v2_client_with_max_inflight(ws_port, DEFAULT_MAX_INFLIGHT).await
    }

    async fn connect_resumable_v2_client(
        ws_port: u16,
        session_id: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>> {
        let mut client = connect_client(ws_port).await;
        client
            .send(
                Frame::Hello {
                    protocol_version: TUNNEL_PROTOCOL_VERSION,
                    max_stream_chunk: DEFAULT_STREAM_CHUNK_BYTES,
                    max_inflight: DEFAULT_MAX_INFLIGHT,
                    stream_window_frames: DEFAULT_STREAM_WINDOW_FRAMES,
                    max_body_size: MAX_HTTP_BODY_BYTES,
                    max_ws_message_size: DEFAULT_MAX_WS_MESSAGE_BYTES,
                    resume: true,
                    session_id: Some(session_id.to_string()),
                }
                .to_msg(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        client
    }

    async fn connect_v2_client_with_max_inflight(
        ws_port: u16,
        max_inflight: usize,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>> {
        let mut client = connect_client(ws_port).await;
        client
            .send(
                Frame::Hello {
                    protocol_version: TUNNEL_PROTOCOL_VERSION,
                    max_stream_chunk: DEFAULT_STREAM_CHUNK_BYTES,
                    max_inflight,
                    stream_window_frames: DEFAULT_STREAM_WINDOW_FRAMES,
                    max_body_size: MAX_HTTP_BODY_BYTES,
                    max_ws_message_size: DEFAULT_MAX_WS_MESSAGE_BYTES,
                    resume: false,
                    session_id: None,
                }
                .to_msg(),
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        client
    }

    async fn next_frame<S>(ws: &mut S) -> Frame
    where
        S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(t))) => return serde_json::from_str(&t).unwrap(),
                Some(Ok(_)) => continue,
                other => panic!("expected text frame, got {other:?}"),
            }
        }
    }

    async fn http_get(port: u16, path: &str) -> String {
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        let _ = s.read_to_end(&mut buf).await;
        String::from_utf8_lossy(&buf).to_string()
    }

    async fn next_message<S>(ws: &mut S) -> Message
    where
        S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        loop {
            match ws.next().await {
                Some(Ok(message)) if !message.is_ping() && !message.is_pong() => return message,
                Some(Ok(_)) => continue,
                other => panic!("expected tunnel message, got {other:?}"),
            }
        }
    }

    async fn raw_http(port: u16, request: &[u8]) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream.write_all(request).await.unwrap();
        let mut response = Vec::new();
        let read_result =
            tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut response))
                .await
                .expect("HTTP response timed out");
        if let Err(error) = read_result {
            assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        }
        String::from_utf8_lossy(&response).into_owned()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disconnected_http_fails_fast_without_caching_request() {
        let (_ws_port, http_port) = spawn_test_server().await;
        let started = Instant::now();
        let response = http_get(http_port, "/no-client").await;
        assert!(
            response.starts_with("HTTP/1.1 503"),
            "response={response:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn configured_http_timeout_controls_slow_response_wait() {
        let (ws_port, http_port) =
            spawn_test_server_with_http_timeout(Duration::from_millis(100)).await;
        let mut client = connect_v2_client(ws_port).await;
        let request = tokio::spawn(async move { http_get(http_port, "/slow-timeout").await });
        let timed_out_id = match next_frame(&mut client).await {
            Frame::HttpReq { id, path, .. } => {
                assert_eq!(path, "/slow-timeout");
                id
            }
            other => panic!("expected http_req, got {other:?}"),
        };
        let response = tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .expect("configured timeout should finish the downstream request")
            .unwrap();
        assert!(
            response.starts_with("HTTP/1.1 504"),
            "response={response:?}"
        );
        assert!(
            response.ends_with("Tunnel timeout"),
            "response={response:?}"
        );
        match tokio::time::timeout(Duration::from_secs(1), next_frame(&mut client))
            .await
            .expect("timeout should notify the upstream tunnel client")
        {
            Frame::Error { id, message } => {
                assert_eq!(id, timed_out_id);
                assert!(message.contains("downstream request closed"));
            }
            other => panic!("expected upstream cancellation, got {other:?}"),
        }

        let (ws_port, http_port) =
            spawn_test_server_with_http_timeout(Duration::from_millis(500)).await;
        let mut client = connect_v2_client(ws_port).await;
        let request = tokio::spawn(async move { http_get(http_port, "/slow-success").await });
        let id = match next_frame(&mut client).await {
            Frame::HttpReq { id, path, .. } => {
                assert_eq!(path, "/slow-success");
                id
            }
            other => panic!("expected http_req, got {other:?}"),
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        client
            .send(
                Frame::HttpResp {
                    id,
                    status: 200,
                    headers: Vec::new(),
                    body: b64().encode(b"slow-ok"),
                }
                .to_msg(),
            )
            .await
            .unwrap();
        let response = request.await.unwrap();
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "response={response:?}"
        );
        assert!(response.ends_with("slow-ok"), "response={response:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn legacy_peer_rejects_oversized_single_frame_request() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_client(ws_port).await;
        let request = format!(
            "POST /large-v1 HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            MAX_V1_BODY_BYTES + 1
        );
        let response = raw_http(http_port, request.as_bytes()).await;
        assert!(
            response.starts_with("HTTP/1.1 413"),
            "response={response:?}"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(200), next_message(&mut client))
                .await
                .is_err(),
            "oversized V1 requests must not be queued or sent"
        );
    }

    #[tokio::test]
    async fn downstream_stream_cancellation_isolated_to_one_request() {
        let state = Arc::new(State::default());
        let generation = 1;
        state.active_generation.store(generation, Ordering::Release);
        let (outbound, mut outbound_rx) = mpsc::channel(OUTBOUND_QUEUE_FRAMES);
        *state.active_client.lock().unwrap() = Some(ActiveClient {
            generation,
            sender: outbound,
            data_permits: Arc::new(Semaphore::new(
                OUTBOUND_QUEUE_FRAMES - OUTBOUND_CONTROL_RESERVE,
            )),
            shutdown: oneshot::channel().0,
        });
        let (body_tx, body_rx) = mpsc::channel(1);
        drop(body_rx);
        let id = make_id();
        state.response_streams.lock().unwrap().insert(
            id.clone(),
            ResponseStreamSink {
                generation,
                session_id: None,
                sender: body_tx,
                received: 0,
                expected: None,
                max_body_size: MAX_HTTP_BODY_BYTES,
                queued_frames: Arc::new(AtomicUsize::new(0)),
                window: DEFAULT_STREAM_WINDOW_FRAMES,
            },
        );

        dispatch_binary_from_client(
            BinaryEnvelope {
                id: id.clone(),
                kind: BinaryKind::HttpResponse,
                payload: Bytes::from_static(b"late"),
                end_of_body: false,
                offset: None,
            },
            &state,
            generation,
        )
        .await
        .unwrap();
        assert!(state.is_terminated_stream(generation, &id));
        match outbound_rx.recv().await.unwrap().message {
            Message::Text(raw) => match serde_json::from_str::<Frame>(&raw).unwrap() {
                Frame::Error { id: error_id, .. } => assert_eq!(error_id, id),
                other => panic!("expected stream error, got {other:?}"),
            },
            other => panic!("expected control frame, got {other:?}"),
        }

        // Remaining data from the already-granted window is absorbed without
        // closing the tunnel connection.
        dispatch_binary_from_client(
            BinaryEnvelope {
                id,
                kind: BinaryKind::HttpResponse,
                payload: Bytes::from_static(b"later"),
                end_of_body: true,
                offset: None,
            },
            &state,
            generation,
        )
        .await
        .unwrap();
    }

    #[test]
    fn binary_envelope_matches_python_protocol_layout() {
        let id = "00112233-4455-6677-8899-aabbccddeeff".to_string();
        let envelope = BinaryEnvelope {
            id: id.clone(),
            kind: BinaryKind::HttpRequest,
            payload: Bytes::from_static(b"payload"),
            end_of_body: false,
            offset: None,
        };
        let Message::Binary(raw) = envelope.encode(DEFAULT_STREAM_CHUNK_BYTES).unwrap() else {
            panic!("binary envelope must produce a binary WebSocket message")
        };
        assert_eq!(&raw[..2], b"YD");
        assert_eq!(&raw[2..5], &[1, 1, 16]);
        assert_eq!(&raw[5..21], &uuid_bytes_from_id(&id).unwrap());
        assert_eq!(raw[21], 0);
        assert_eq!(u32::from_be_bytes(raw[22..26].try_into().unwrap()), 7);
        assert_eq!(&raw[26..], b"payload");
        assert_eq!(
            BinaryEnvelope::decode(&raw, DEFAULT_STREAM_CHUNK_BYTES).unwrap(),
            envelope
        );
    }

    #[test]
    fn binary_envelope_rejects_malformed_and_oversized_payloads() {
        let id = "00112233-4455-6677-8899-aabbccddeeff".to_string();
        let envelope = BinaryEnvelope {
            id,
            kind: BinaryKind::HttpResponse,
            payload: Bytes::from_static(b"last"),
            end_of_body: true,
            offset: None,
        };
        let Message::Binary(raw) = envelope.encode(DEFAULT_STREAM_CHUNK_BYTES).unwrap() else {
            unreachable!()
        };
        assert!(BinaryEnvelope::decode(&raw[..25], DEFAULT_STREAM_CHUNK_BYTES).is_err());
        let mut bad_magic = raw.clone();
        bad_magic[..2].copy_from_slice(b"NO");
        assert!(BinaryEnvelope::decode(&bad_magic, DEFAULT_STREAM_CHUNK_BYTES).is_err());
        let mut bad_kind = raw.clone();
        bad_kind[3] = 0xff;
        assert!(BinaryEnvelope::decode(&bad_kind, DEFAULT_STREAM_CHUNK_BYTES).is_err());
        assert!(BinaryEnvelope::decode(&raw, 3).is_err());
        assert!(envelope.encode(3).is_err());
    }

    #[test]
    fn hello_frame_advertises_v2_limits() {
        let frame = Frame::Hello {
            protocol_version: TUNNEL_PROTOCOL_VERSION,
            max_stream_chunk: DEFAULT_STREAM_CHUNK_BYTES,
            max_inflight: DEFAULT_MAX_INFLIGHT,
            stream_window_frames: DEFAULT_STREAM_WINDOW_FRAMES,
            max_body_size: MAX_HTTP_BODY_BYTES,
            max_ws_message_size: DEFAULT_MAX_WS_MESSAGE_BYTES,
            resume: false,
            session_id: None,
        };
        let value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&frame).unwrap()).unwrap();
        assert_eq!(value["type"], "hello");
        assert_eq!(value["protocol_version"], 2);
        assert_eq!(value["max_stream_chunk"], 65536);
        assert_eq!(value["max_inflight"], 16);
        assert_eq!(value["stream_window_frames"], 16);
        assert_eq!(value["max_body_size"], 536870912);
        assert_eq!(value["max_ws_message_size"], 8388608);
    }

    #[test]
    fn frame_json_matches_python_protocol() {
        // Header pairs retain duplicate fields and their original order.
        let f = Frame::HttpReq {
            id: "x".into(),
            method: "GET".into(),
            path: "/p".into(),
            headers: Vec::new(),
            body: b64().encode(b"hi"),
        };
        let j: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&f).unwrap()).unwrap();
        assert_eq!(j["type"], "http_req");
        assert_eq!(j["id"], "x");
        assert_eq!(j["body"], "aGk="); // base64("hi")
        assert!(j["headers"].is_array(), "headers={}", j["headers"]);

        let raw = r#"{"type":"http_resp","id":"x","status":201,"headers":[["Set-Cookie","a=1"],["Set-Cookie","b=2"]],"body":"cG9uZw=="}"#;
        match serde_json::from_str::<Frame>(raw).unwrap() {
            Frame::HttpResp {
                status,
                headers,
                body,
                ..
            } => {
                assert_eq!(status, 201);
                assert_eq!(headers.len(), 2);
                assert_eq!(b64().decode(body.as_bytes()).unwrap(), b"pong");
            }
            o => panic!("{o:?}"),
        }
    }

    #[test]
    fn response_content_length_obeys_method_and_status_semantics() {
        let representation_headers = vec![
            ("Content-Length".into(), "123".into()),
            ("Set-Cookie".into(), "a=1".into()),
            ("Set-Cookie".into(), "b=2".into()),
        ];

        let head = response_headers_for_downstream(
            representation_headers.clone(),
            &Method::HEAD,
            StatusCode::OK,
            0,
        );
        assert_eq!(
            head.iter()
                .filter(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                .map(|(_, value)| value.as_str())
                .collect::<Vec<_>>(),
            vec!["123"]
        );

        let no_content = response_headers_for_downstream(
            representation_headers.clone(),
            &Method::GET,
            StatusCode::NO_CONTENT,
            0,
        );
        assert!(
            no_content
                .iter()
                .all(|(name, _)| !name.eq_ignore_ascii_case("content-length")),
            "headers={no_content:?}"
        );

        let not_modified = response_headers_for_downstream(
            representation_headers,
            &Method::GET,
            StatusCode::NOT_MODIFIED,
            0,
        );
        assert!(
            not_modified
                .iter()
                .all(|(name, _)| !name.eq_ignore_ascii_case("content-length")),
            "headers={not_modified:?}"
        );
        assert_eq!(
            not_modified
                .iter()
                .filter(|(name, _)| name.eq_ignore_ascii_case("set-cookie"))
                .count(),
            2
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ambiguous_request_framing_is_handled_safely() {
        let (_ws_port, http_port) = spawn_test_server().await;
        let response = raw_http(
            http_port,
            b"POST /ambiguous HTTP/1.1\r\nHost: local\r\nContent-Length: 1\r\nContent-Length: 2\r\nConnection: close\r\n\r\nx",
        )
        .await;
        assert!(
            response.starts_with("HTTP/1.1 400"),
            "response={response:?}"
        );

        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_client(ws_port).await;
        let request = tokio::spawn(async move {
            raw_http(
                http_port,
                b"POST /canonical HTTP/1.1\r\nHost: local\r\nContent-Length: 99\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1\r\nx\r\n0\r\n\r\n",
            )
            .await
        });
        let id = match next_frame(&mut client).await {
            Frame::HttpReq {
                id, headers, body, ..
            } => {
                assert_eq!(b64().decode(body.as_bytes()).unwrap(), b"x");
                assert!(headers.iter().all(|(name, _)| {
                    !name.eq_ignore_ascii_case("content-length")
                        && !name.eq_ignore_ascii_case("transfer-encoding")
                }));
                id
            }
            other => panic!("expected canonical http_req, got {other:?}"),
        };
        client
            .send(
                Frame::HttpResp {
                    id,
                    status: 200,
                    headers: Vec::new(),
                    body: b64().encode(b"ok"),
                }
                .to_msg(),
            )
            .await
            .unwrap();
        assert!(request.await.unwrap().starts_with("HTTP/1.1 200"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn excessive_header_count_is_rejected() {
        let (_ws_port, http_port) = spawn_test_server().await;
        let mut request = String::from("GET /headers HTTP/1.1\r\nHost: local\r\n");
        for index in 0..=MAX_HTTP_HEADERS {
            request.push_str(&format!("X-Test-{index}: value\r\n"));
        }
        request.push_str("Connection: close\r\n\r\n");
        let response = raw_http(http_port, request.as_bytes()).await;
        assert!(
            response.starts_with("HTTP/1.1 431"),
            "response={response:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn chunked_request_is_decoded_before_framing() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_client(ws_port).await;
        let request_task = tokio::spawn(async move {
            let mut stream = TcpStream::connect(("127.0.0.1", http_port)).await.unwrap();
            stream
                .write_all(
                    b"POST /chunked HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
                )
                .await
                .unwrap();
            let mut response = Vec::new();
            let _ = stream.read_to_end(&mut response).await;
            response
        });

        let frame = tokio::time::timeout(Duration::from_secs(2), next_frame(&mut client))
            .await
            .expect("EXECD did not frame the chunked request");
        let id = match frame {
            Frame::HttpReq {
                id, headers, body, ..
            } => {
                assert_eq!(b64().decode(body.as_bytes()).unwrap(), b"hello world");
                assert!(
                    headers
                        .iter()
                        .all(|(name, _)| { !name.eq_ignore_ascii_case("transfer-encoding") }),
                    "headers={headers:?}"
                );
                id
            }
            other => panic!("expected http_req, got {other:?}"),
        };
        client
            .send(
                Frame::HttpResp {
                    id,
                    status: 200,
                    headers: Vec::new(),
                    body: b64().encode(b"ok"),
                }
                .to_msg(),
            )
            .await
            .unwrap();
        let response = request_task.await.unwrap();
        assert!(
            String::from_utf8_lossy(&response).contains("200"),
            "response={response:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_tunnel_roundtrip() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_client(ws_port).await;
        let task = tokio::spawn(async move {
            let f = next_frame(&mut client).await;
            let id = match f {
                Frame::HttpReq {
                    id, path, method, ..
                } => {
                    assert_eq!(path, "/hello");
                    assert_eq!(method, "GET");
                    id
                }
                o => panic!("expected http_req, got {o:?}"),
            };
            client
                .send(
                    Frame::HttpResp {
                        id,
                        status: 200,
                        headers: Vec::new(),
                        body: b64().encode(b"pong"),
                    }
                    .to_msg(),
                )
                .await
                .unwrap();
        });
        let resp = http_get(http_port, "/hello").await;
        assert!(resp.contains("200"), "resp={resp}");
        assert!(resp.ends_with("pong"), "resp={resp}");
        task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn v2_streaming_http_roundtrip_uses_bounded_binary_chunks() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_v2_client(ws_port).await;
        let payload = vec![b'a'; 100_000];
        let request_payload = payload.clone();
        let request = tokio::spawn(async move {
            let mut raw = format!(
                "POST /stream HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                request_payload.len()
            )
            .into_bytes();
            raw.extend_from_slice(&request_payload);
            raw_http(http_port, &raw).await
        });

        let id = match next_frame(&mut client).await {
            Frame::HttpReqBegin {
                id,
                method,
                path,
                content_length,
                ..
            } => {
                assert_eq!(method, "POST");
                assert_eq!(path, "/stream");
                assert_eq!(content_length, Some(payload.len() as u64));
                uuid_bytes_from_id(&id).unwrap();
                id
            }
            other => panic!("expected http_req_begin, got {other:?}"),
        };
        client
            .send(
                Frame::Window {
                    id: id.clone(),
                    credits: DEFAULT_STREAM_WINDOW_FRAMES,
                    ack_offset: None,
                    complete: false,
                }
                .to_msg(),
            )
            .await
            .unwrap();

        let mut streamed = Vec::new();
        loop {
            match next_message(&mut client).await {
                Message::Binary(raw) => {
                    let envelope =
                        BinaryEnvelope::decode(&raw, DEFAULT_STREAM_CHUNK_BYTES).unwrap();
                    assert_eq!(envelope.id, id);
                    assert_eq!(envelope.kind, BinaryKind::HttpRequest);
                    assert!(envelope.payload.len() <= DEFAULT_STREAM_CHUNK_BYTES);
                    streamed.extend_from_slice(&envelope.payload);
                }
                Message::Text(raw) => match serde_json::from_str::<Frame>(&raw).unwrap() {
                    Frame::HttpReqEnd { id: end_id } => {
                        assert_eq!(end_id, id);
                        break;
                    }
                    other => panic!("expected request data/end, got {other:?}"),
                },
                other => panic!("expected request data/end, got {other:?}"),
            }
        }
        assert_eq!(streamed, payload);

        client
            .send(
                Frame::HttpRespBegin {
                    id: id.clone(),
                    status: 200,
                    headers: vec![("content-type".into(), "text/plain".into())],
                    content_length: Some(2),
                }
                .to_msg(),
            )
            .await
            .unwrap();
        match next_frame(&mut client).await {
            Frame::Window {
                id: window_id,
                credits,
                ..
            } => {
                assert_eq!(window_id, id);
                assert_eq!(credits, DEFAULT_STREAM_WINDOW_FRAMES);
            }
            other => panic!("expected response window, got {other:?}"),
        }
        client
            .send(
                BinaryEnvelope {
                    id: id.clone(),
                    kind: BinaryKind::HttpResponse,
                    payload: Bytes::from_static(b"ok"),
                    end_of_body: false,
                    offset: None,
                }
                .encode(DEFAULT_STREAM_CHUNK_BYTES)
                .unwrap(),
            )
            .await
            .unwrap();
        client
            .send(Frame::HttpRespEnd { id: id.clone() }.to_msg())
            .await
            .unwrap();

        let response = request.await.unwrap();
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "response={response:?}"
        );
        assert!(response.ends_with("ok"), "response={response:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn v2_disconnect_fails_stream_without_replay() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_v2_client(ws_port).await;
        let request = tokio::spawn(async move {
            let payload = vec![b'x'; 100_000];
            let mut raw = format!(
                "POST /disconnect HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
                payload.len()
            )
            .into_bytes();
            raw.extend_from_slice(&payload);
            raw_http(http_port, &raw).await
        });
        match next_frame(&mut client).await {
            Frame::HttpReqBegin { path, .. } => assert_eq!(path, "/disconnect"),
            other => panic!("expected streaming request begin, got {other:?}"),
        }
        drop(client);

        let response = request.await.unwrap();
        assert!(
            response.starts_with("HTTP/1.1 502"),
            "response={response:?}"
        );
        let mut replacement = connect_client(ws_port).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(200), next_message(&mut replacement))
                .await
                .is_err(),
            "V2 streamed requests must not be replayed after disconnect"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn malformed_binary_disconnects_generation_and_http_fails_fast() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_v2_client(ws_port).await;
        client
            .send(Message::Binary(vec![0x01, 0x02, 0x03]))
            .await
            .unwrap();

        let response = tokio::time::timeout(
            Duration::from_secs(2),
            http_get(http_port, "/after-malformed-frame"),
        )
        .await
        .expect("HTTP must not hang on a half-dead generation");
        assert!(response.contains("503"), "response={response:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replacement_actively_closes_the_previous_client_task() {
        let (ws_port, _http_port) = spawn_test_server().await;
        let mut previous = connect_v2_client(ws_port).await;
        let mut active = connect_v2_client(ws_port).await;

        let old_result = tokio::time::timeout(Duration::from_secs(2), previous.next())
            .await
            .expect("replaced client must be closed promptly");
        assert!(
            matches!(
                old_result,
                None | Some(Ok(Message::Close(_))) | Some(Err(_))
            ),
            "unexpected old client result: {old_result:?}"
        );
        active
            .send(
                Frame::Ping {
                    id: "replacement-healthy".into(),
                    timestamp: 1.0,
                }
                .to_msg(),
            )
            .await
            .unwrap();
        match next_frame(&mut active).await {
            Frame::Pong { id, .. } => assert_eq!(id, "replacement-healthy"),
            other => panic!("expected pong from replacement, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn downstream_request_cancellation_notifies_upstream_and_keeps_tunnel_healthy() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_v2_client(ws_port).await;
        let payload = vec![b'c'; 128 * 1024];
        let mut downstream = TcpStream::connect(("127.0.0.1", http_port)).await.unwrap();
        let mut raw = format!(
            "POST /cancel-upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
            payload.len()
        )
        .into_bytes();
        raw.extend_from_slice(&payload);
        downstream.write_all(&raw).await.unwrap();

        let id = match next_frame(&mut client).await {
            Frame::HttpReqBegin { id, path, .. } => {
                assert_eq!(path, "/cancel-upload");
                id
            }
            other => panic!("expected streaming request begin, got {other:?}"),
        };
        client
            .send(
                Frame::Window {
                    id: id.clone(),
                    credits: DEFAULT_STREAM_WINDOW_FRAMES,
                    ack_offset: None,
                    complete: false,
                }
                .to_msg(),
            )
            .await
            .unwrap();
        loop {
            match next_message(&mut client).await {
                Message::Binary(_) => {}
                Message::Text(raw) => match serde_json::from_str::<Frame>(&raw).unwrap() {
                    Frame::HttpReqEnd { id: end_id } => {
                        assert_eq!(end_id, id);
                        break;
                    }
                    other => panic!("expected request data/end, got {other:?}"),
                },
                other => panic!("expected request data/end, got {other:?}"),
            }
        }
        drop(downstream);

        match tokio::time::timeout(Duration::from_secs(2), next_frame(&mut client))
            .await
            .expect("upstream cancellation notification")
        {
            Frame::Error {
                id: error_id,
                message,
            } => {
                assert_eq!(error_id, id);
                assert!(message.contains("downstream request closed"));
            }
            other => panic!("expected stream error, got {other:?}"),
        }
        client
            .send(
                Frame::Ping {
                    id: "still-healthy".into(),
                    timestamp: 1.0,
                }
                .to_msg(),
            )
            .await
            .unwrap();
        match next_frame(&mut client).await {
            Frame::Pong { id, .. } => assert_eq!(id, "still-healthy"),
            other => panic!("expected pong after cancellation, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn negotiated_max_inflight_is_enforced_by_execd() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_v2_client_with_max_inflight(ws_port, 1).await;
        let first = tokio::spawn(async move { http_get(http_port, "/held").await });
        let first_id = match next_frame(&mut client).await {
            Frame::HttpReq { id, path, .. } => {
                assert_eq!(path, "/held");
                id
            }
            other => panic!("expected first request, got {other:?}"),
        };
        client
            .send(
                Frame::Hello {
                    protocol_version: TUNNEL_PROTOCOL_VERSION,
                    max_stream_chunk: DEFAULT_STREAM_CHUNK_BYTES,
                    max_inflight: DEFAULT_MAX_INFLIGHT,
                    stream_window_frames: DEFAULT_STREAM_WINDOW_FRAMES,
                    max_body_size: MAX_HTTP_BODY_BYTES,
                    max_ws_message_size: DEFAULT_MAX_WS_MESSAGE_BYTES,
                    resume: false,
                    session_id: None,
                }
                .to_msg(),
            )
            .await
            .unwrap();
        client
            .send(
                Frame::Ping {
                    id: "after-duplicate-hello".into(),
                    timestamp: 2.0,
                }
                .to_msg(),
            )
            .await
            .unwrap();
        match next_frame(&mut client).await {
            Frame::Pong { id, .. } => assert_eq!(id, "after-duplicate-hello"),
            other => panic!("expected duplicate-hello barrier pong, got {other:?}"),
        }

        let second = tokio::time::timeout(
            Duration::from_secs(2),
            http_get(http_port, "/must-backpressure"),
        )
        .await
        .expect("negotiated limit must reject promptly");
        assert!(second.contains("429"), "response={second:?}");

        client
            .send(
                Frame::HttpResp {
                    id: first_id,
                    status: 200,
                    headers: Vec::new(),
                    body: b64().encode(b"ok"),
                }
                .to_msg(),
            )
            .await
            .unwrap();
        assert!(first.await.unwrap().contains("200"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn v2_small_request_accepts_streaming_response() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_v2_client(ws_port).await;
        let request = tokio::spawn(async move { http_get(http_port, "/events").await });

        let id = match next_frame(&mut client).await {
            Frame::HttpReq {
                id, method, path, ..
            } => {
                assert_eq!(method, "GET");
                assert_eq!(path, "/events");
                id
            }
            other => panic!("expected fast-path http_req, got {other:?}"),
        };
        client
            .send(
                Frame::HttpRespBegin {
                    id: id.clone(),
                    status: 200,
                    headers: vec![("content-type".into(), "text/event-stream".into())],
                    content_length: None,
                }
                .to_msg(),
            )
            .await
            .unwrap();
        match next_frame(&mut client).await {
            Frame::Window {
                id: window_id,
                credits,
                ..
            } => {
                assert_eq!(window_id, id);
                assert_eq!(credits, DEFAULT_STREAM_WINDOW_FRAMES);
            }
            other => panic!("expected response window, got {other:?}"),
        }
        for payload in [
            b"data: first\n\n".as_slice(),
            b"data: second\n\n".as_slice(),
        ] {
            client
                .send(
                    BinaryEnvelope {
                        id: id.clone(),
                        kind: BinaryKind::HttpResponse,
                        payload: Bytes::copy_from_slice(payload),
                        end_of_body: false,
                        offset: None,
                    }
                    .encode(DEFAULT_STREAM_CHUNK_BYTES)
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        client
            .send(Frame::HttpRespEnd { id }.to_msg())
            .await
            .unwrap();

        let response = request.await.unwrap();
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "response={response:?}"
        );
        let first = response.find("data: first").expect("first SSE event");
        let second = response.find("data: second").expect("second SSE event");
        assert!(first < second, "response={response:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn response_length_mismatch_fails_only_that_stream() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_v2_client(ws_port).await;
        let request = tokio::spawn(async move { http_get(http_port, "/mismatch").await });
        let id = match next_frame(&mut client).await {
            Frame::HttpReq { id, .. } => id,
            other => panic!("expected fast-path request, got {other:?}"),
        };
        client
            .send(
                Frame::HttpRespBegin {
                    id: id.clone(),
                    status: 200,
                    headers: Vec::new(),
                    content_length: Some(3),
                }
                .to_msg(),
            )
            .await
            .unwrap();
        assert!(matches!(
            next_frame(&mut client).await,
            Frame::Window { .. }
        ));
        client
            .send(
                BinaryEnvelope {
                    id: id.clone(),
                    kind: BinaryKind::HttpResponse,
                    payload: Bytes::from_static(b"ok"),
                    end_of_body: false,
                    offset: None,
                }
                .encode(DEFAULT_STREAM_CHUNK_BYTES)
                .unwrap(),
            )
            .await
            .unwrap();
        client
            .send(Frame::HttpRespEnd { id: id.clone() }.to_msg())
            .await
            .unwrap();
        loop {
            match next_frame(&mut client).await {
                Frame::Window { .. } => continue,
                Frame::Error {
                    id: error_id,
                    message,
                } => {
                    assert_eq!(error_id, id);
                    assert!(message.contains("content length mismatch"));
                    break;
                }
                other => panic!("expected stream-local error, got {other:?}"),
            }
        }
        let _ = request.await.unwrap();

        client
            .send(
                Frame::Ping {
                    id: "still-alive".into(),
                    timestamp: 1.0,
                }
                .to_msg(),
            )
            .await
            .unwrap();
        loop {
            match next_frame(&mut client).await {
                Frame::Pong { id, .. } if id == "still-alive" => break,
                Frame::Window { .. } | Frame::Error { .. } => continue,
                other => panic!("expected tunnel to remain alive, got {other:?}"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ping_gets_pong() {
        let (ws_port, _hp) = spawn_test_server().await;
        let mut client = connect_client(ws_port).await;
        client
            .send(
                Frame::Ping {
                    id: "p1".into(),
                    timestamp: 1.5,
                }
                .to_msg(),
            )
            .await
            .unwrap();
        match next_frame(&mut client).await {
            Frame::Pong { id, timestamp } => {
                assert_eq!(id, "p1");
                assert_eq!(timestamp, 1.5);
            }
            o => panic!("expected pong, got {o:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn http_error_returns_bad_gateway() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_client(ws_port).await;
        let task = tokio::spawn(async move {
            let id = match next_frame(&mut client).await {
                Frame::HttpReq { id, .. } => id,
                o => panic!("{o:?}"),
            };
            client
                .send(
                    Frame::Error {
                        id,
                        message: "upstream unreachable".into(),
                    }
                    .to_msg(),
                )
                .await
                .unwrap();
        });
        let resp = http_get(http_port, "/boom").await;
        assert!(resp.starts_with("HTTP/1.1 502"), "resp={resp}");
        assert!(resp.ends_with("upstream unreachable"), "resp={resp}");
        task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fragmented_websocket_upgrade_is_not_routed_as_http() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_v2_client(ws_port).await;
        let upstream = tokio::spawn(async move {
            let id = match next_frame(&mut client).await {
                Frame::WsConnect { id, path, .. } => {
                    assert_eq!(path, "/fragmented");
                    id
                }
                other => panic!("expected ws_connect, got {other:?}"),
            };
            client
                .send(Frame::WsConnected { id }.to_msg())
                .await
                .unwrap();
        });

        let mut stream = TcpStream::connect(("127.0.0.1", http_port)).await.unwrap();
        stream
            .write_all(
                b"GET /fragmented HTTP/1.1\r\nHost: localhost\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        stream
            .write_all(
                b"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\n\r\n",
            )
            .await
            .unwrap();

        let mut response = [0u8; 512];
        let count = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut response))
            .await
            .expect("WebSocket handshake timed out")
            .unwrap();
        assert!(
            response[..count].starts_with(b"HTTP/1.1 101"),
            "response={:?}",
            String::from_utf8_lossy(&response[..count])
        );
        upstream.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ws_tunnel_roundtrip() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_client(ws_port).await;
        let task = tokio::spawn(async move {
            let id = match next_frame(&mut client).await {
                Frame::WsConnect { id, path, .. } => {
                    assert_eq!(path, "/chat");
                    id
                }
                other => panic!("expected ws_connect, got {other:?}"),
            };
            client
                .send(Frame::WsConnected { id: id.clone() }.to_msg())
                .await
                .unwrap();
            match next_frame(&mut client).await {
                Frame::WsMessage { data, binary, .. } => {
                    assert!(!binary);
                    assert_eq!(data, "hi");
                }
                other => panic!("expected ws_message, got {other:?}"),
            }
            client
                .send(
                    Frame::WsMessage {
                        id,
                        data: "hi-echo".into(),
                        binary: false,
                    }
                    .to_msg(),
                )
                .await
                .unwrap();
        });
        let (mut browser_ws, _) = connect_async(format!("ws://127.0.0.1:{http_port}/chat"))
            .await
            .unwrap();
        browser_ws.send(Message::Text("hi".into())).await.unwrap();
        match browser_ws.next().await {
            Some(Ok(Message::Text(text))) => assert_eq!(text, "hi-echo"),
            other => panic!("expected echo, got {other:?}"),
        }
        task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn v2_ws_binary_roundtrip_uses_raw_bounded_chunks() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut client = connect_v2_client(ws_port).await;
        let payload = vec![0x5a; 100_000];
        let expected = payload.clone();
        let task = tokio::spawn(async move {
            let id = match next_frame(&mut client).await {
                Frame::WsConnect { id, path, .. } => {
                    assert_eq!(path, "/binary");
                    id
                }
                other => panic!("expected ws_connect, got {other:?}"),
            };
            client
                .send(Frame::WsConnected { id: id.clone() }.to_msg())
                .await
                .unwrap();
            let mut received = Vec::new();
            loop {
                let Message::Binary(raw) = next_message(&mut client).await else {
                    panic!("expected raw binary tunnel frame")
                };
                let envelope = BinaryEnvelope::decode(&raw, DEFAULT_STREAM_CHUNK_BYTES).unwrap();
                assert_eq!(envelope.id, id);
                assert_eq!(envelope.kind, BinaryKind::WebSocket);
                assert!(envelope.payload.len() <= DEFAULT_STREAM_CHUNK_BYTES);
                received.extend_from_slice(&envelope.payload);
                if envelope.end_of_body {
                    break;
                }
            }
            assert_eq!(received, expected);
            for (index, chunk) in received.chunks(DEFAULT_STREAM_CHUNK_BYTES).enumerate() {
                client
                    .send(
                        BinaryEnvelope {
                            id: id.clone(),
                            kind: BinaryKind::WebSocket,
                            payload: Bytes::copy_from_slice(chunk),
                            end_of_body: (index + 1) * DEFAULT_STREAM_CHUNK_BYTES >= received.len(),
                            offset: None,
                        }
                        .encode(DEFAULT_STREAM_CHUNK_BYTES)
                        .unwrap(),
                    )
                    .await
                    .unwrap();
            }
            match next_frame(&mut client).await {
                Frame::WsClose { id: close_id, .. } => assert_eq!(close_id, id),
                other => panic!("expected ws_close, got {other:?}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            client
                .send(
                    BinaryEnvelope {
                        id: id.clone(),
                        kind: BinaryKind::WebSocket,
                        payload: Bytes::from_static(b"late"),
                        end_of_body: true,
                        offset: None,
                    }
                    .encode(DEFAULT_STREAM_CHUNK_BYTES)
                    .unwrap(),
                )
                .await
                .unwrap();
            client
                .send(
                    Frame::Ping {
                        id: "after-late-ws".into(),
                        timestamp: 1.0,
                    }
                    .to_msg(),
                )
                .await
                .unwrap();
            match next_frame(&mut client).await {
                Frame::Pong { id, .. } => assert_eq!(id, "after-late-ws"),
                other => panic!("expected pong after late WS data, got {other:?}"),
            }
        });
        let (mut browser_ws, _) = connect_async(format!("ws://127.0.0.1:{http_port}/binary"))
            .await
            .unwrap();
        browser_ws
            .send(Message::Binary(payload.clone()))
            .await
            .unwrap();
        match browser_ws.next().await {
            Some(Ok(Message::Binary(echo))) => assert_eq!(echo, payload),
            other => panic!("expected binary echo, got {other:?}"),
        }
        browser_ws.close(None).await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_resends_pending_http() {
        let (ws_port, http_port) = spawn_test_server().await;
        // client1 connects, receives the request, then drops WITHOUT responding.
        let mut c1 = connect_client(ws_port).await;
        let http_task = tokio::spawn(async move { http_get(http_port, "/persist").await });
        match next_frame(&mut c1).await {
            Frame::HttpReq { path, .. } => assert_eq!(path, "/persist"),
            o => panic!("expected http_req, got {o:?}"),
        }
        drop(c1); // simulate tunnel client disconnect mid-request
        tokio::time::sleep(Duration::from_millis(150)).await;
        // client2 reconnects -> server must resend the still-pending request.
        let mut c2 = connect_client(ws_port).await;
        let id = match next_frame(&mut c2).await {
            Frame::HttpReq { id, path, .. } => {
                assert_eq!(path, "/persist");
                id
            }
            o => panic!("reconnect should resend http_req, got {o:?}"),
        };
        c2.send(
            Frame::HttpResp {
                id,
                status: 200,
                headers: Vec::new(),
                body: b64().encode(b"resent-ok"),
            }
            .to_msg(),
        )
        .await
        .unwrap();
        let resp = http_task.await.unwrap();
        assert!(
            resp.contains("200") && resp.ends_with("resent-ok"),
            "resp={resp}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn v2_small_fast_path_disconnect_fails_without_replay() {
        let (ws_port, http_port) = spawn_test_server().await;
        let mut first = connect_v2_client(ws_port).await;
        let request = tokio::spawn(async move { http_get(http_port, "/v2-replay").await });
        match next_frame(&mut first).await {
            Frame::HttpReq { path, .. } => assert_eq!(path, "/v2-replay"),
            other => panic!("expected fast-path request, got {other:?}"),
        }
        drop(first);
        let response = tokio::time::timeout(Duration::from_secs(2), request)
            .await
            .expect("downstream must fail promptly")
            .unwrap();
        assert!(response.contains("502"), "response={response:?}");

        let mut replacement = connect_v2_client(ws_port).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(350), next_message(&mut replacement))
                .await
                .is_err(),
            "V2 requests must not be replayed across connection generations"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resumable_v2_fast_request_survives_control_ws_reconnect() {
        let (ws_port, http_port) = spawn_test_server().await;
        let session_id = "00112233-4455-4677-8899-aabbccddeeff";
        let mut first = connect_resumable_v2_client(ws_port, session_id).await;
        let request = tokio::spawn(async move { http_get(http_port, "/resume-fast").await });
        let request_id = match next_frame(&mut first).await {
            Frame::HttpReq { id, path, .. } => {
                assert_eq!(path, "/resume-fast");
                id
            }
            other => panic!("expected fast-path request, got {other:?}"),
        };
        first.close(None).await.unwrap();

        let mut resumed = connect_resumable_v2_client(ws_port, session_id).await;
        match next_frame(&mut resumed).await {
            Frame::HttpReq { id, path, .. } => {
                assert_eq!(id, request_id);
                assert_eq!(path, "/resume-fast");
            }
            other => panic!("expected stable-id replay after reconnect, got {other:?}"),
        }
        resumed
            .send(
                Frame::HttpResp {
                    id: request_id,
                    status: 200,
                    headers: Vec::new(),
                    body: b64().encode(b"resume-ok"),
                }
                .to_msg(),
            )
            .await
            .unwrap();
        let response = request.await.unwrap();
        assert!(response.contains("200") && response.ends_with("resume-ok"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resumable_v2_response_chunks_are_deduplicated_after_reconnect() {
        let (ws_port, http_port) = spawn_test_server().await;
        let session_id = "10112233-4455-4677-8899-aabbccddeeff";
        let mut first = connect_resumable_v2_client(ws_port, session_id).await;
        let request = tokio::spawn(async move { http_get(http_port, "/resume-sse").await });
        let id = match next_frame(&mut first).await {
            Frame::HttpReq { id, .. } => id,
            other => panic!("expected request, got {other:?}"),
        };
        first
            .send(
                Frame::HttpRespBegin {
                    id: id.clone(),
                    status: 200,
                    headers: vec![("content-type".into(), "text/event-stream".into())],
                    content_length: None,
                }
                .to_msg(),
            )
            .await
            .unwrap();
        assert!(matches!(next_frame(&mut first).await, Frame::Window { .. }));
        first
            .send(
                BinaryEnvelope {
                    id: id.clone(),
                    kind: BinaryKind::HttpResponse,
                    payload: Bytes::from_static(b"first"),
                    end_of_body: false,
                    offset: Some(0),
                }
                .encode(DEFAULT_STREAM_CHUNK_BYTES)
                .unwrap(),
            )
            .await
            .unwrap();
        first.close(None).await.unwrap();

        let mut resumed = connect_resumable_v2_client(ws_port, session_id).await;
        match next_frame(&mut resumed).await {
            Frame::Window {
                id: window_id,
                ack_offset,
                ..
            } => {
                assert_eq!(window_id, id);
                assert_eq!(ack_offset, Some(5));
            }
            other => panic!("expected resumed response window, got {other:?}"),
        }
        for (offset, payload) in [(0, b"first".as_slice()), (5, b"second".as_slice())] {
            resumed
                .send(
                    BinaryEnvelope {
                        id: id.clone(),
                        kind: BinaryKind::HttpResponse,
                        payload: Bytes::copy_from_slice(payload),
                        end_of_body: false,
                        offset: Some(offset),
                    }
                    .encode(DEFAULT_STREAM_CHUNK_BYTES)
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        resumed
            .send(Frame::HttpRespEnd { id }.to_msg())
            .await
            .unwrap();
        let response = request.await.unwrap();
        assert!(
            response.contains("200")
                && response.matches("first").count() == 1
                && response.matches("second").count() == 1,
            "response={response:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resumable_v2_upload_restarts_from_acknowledged_offset() {
        let (ws_port, http_port) = spawn_test_server().await;
        let session_id = "20112233-4455-4677-8899-aabbccddeeff";
        let payload = vec![b'u'; 100_000];
        let mut raw = format!(
            "POST /resume-upload HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
            payload.len()
        )
        .into_bytes();
        raw.extend_from_slice(&payload);

        let mut first = connect_resumable_v2_client(ws_port, session_id).await;
        let request = tokio::spawn(async move { raw_http(http_port, &raw).await });
        let id = match next_frame(&mut first).await {
            Frame::HttpReqBegin { id, path, .. } => {
                assert_eq!(path, "/resume-upload");
                id
            }
            other => panic!("expected streaming request begin, got {other:?}"),
        };
        first
            .send(
                Frame::Window {
                    id: id.clone(),
                    credits: 1,
                    ack_offset: Some(0),
                    complete: false,
                }
                .to_msg(),
            )
            .await
            .unwrap();
        let first_chunk = match next_message(&mut first).await {
            Message::Binary(raw) => {
                BinaryEnvelope::decode(&raw, DEFAULT_STREAM_CHUNK_BYTES).unwrap()
            }
            other => panic!("expected first upload chunk, got {other:?}"),
        };
        assert_eq!(first_chunk.offset, Some(0));
        first.close(None).await.unwrap();

        let mut resumed = connect_resumable_v2_client(ws_port, session_id).await;
        match next_frame(&mut resumed).await {
            Frame::HttpReqBegin { id: resumed_id, .. } => assert_eq!(resumed_id, id),
            other => panic!("expected replayed request begin, got {other:?}"),
        }
        resumed
            .send(
                Frame::Window {
                    id: id.clone(),
                    credits: DEFAULT_STREAM_WINDOW_FRAMES,
                    ack_offset: Some(0),
                    complete: false,
                }
                .to_msg(),
            )
            .await
            .unwrap();

        let mut received = Vec::new();
        loop {
            match next_message(&mut resumed).await {
                Message::Binary(raw) => {
                    let envelope =
                        BinaryEnvelope::decode(&raw, DEFAULT_STREAM_CHUNK_BYTES).unwrap();
                    assert_eq!(envelope.offset, Some(received.len() as u64));
                    received.extend_from_slice(&envelope.payload);
                }
                Message::Text(raw) => match serde_json::from_str::<Frame>(&raw).unwrap() {
                    Frame::HttpReqEnd { id: end_id } => {
                        assert_eq!(end_id, id);
                        break;
                    }
                    other => panic!("expected upload data/end, got {other:?}"),
                },
                other => panic!("expected upload data/end, got {other:?}"),
            }
        }
        assert_eq!(received, payload);
        resumed
            .send(
                Frame::HttpRespBegin {
                    id: id.clone(),
                    status: 200,
                    headers: Vec::new(),
                    content_length: Some(0),
                }
                .to_msg(),
            )
            .await
            .unwrap();
        assert!(matches!(
            next_frame(&mut resumed).await,
            Frame::Window { .. }
        ));
        resumed
            .send(Frame::HttpRespEnd { id }.to_msg())
            .await
            .unwrap();
        let response = request.await.unwrap();
        assert!(response.contains("200"), "response={response:?}");
    }
}
