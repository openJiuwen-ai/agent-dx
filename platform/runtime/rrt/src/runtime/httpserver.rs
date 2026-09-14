// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
// See the LICENSE file in this repository for the complete license text.

//! RRT atomic-operation HTTP/1.1 server.
//!
//! Purpose: expose sandbox-embedded RRT atomic operations over HTTP through sandboxRouter as an L7 reverse proxy,
//! removing the frontend invoke -> libruntime RuntimeRPC -> msgpack hop.
//!
//! Protocol:
//! - `POST /invoke`, body = `{"action": "...", "args": {...}}`, shaped like sandbox_invoke,
//!   reuse dispatch::normalize_sandbox_action + dispatch_runtime_action and return action result JSON.
//! - `POST /upload?path=/abs/file&type=file|tar`, body is raw binary or a tar stream.
//! - `GET /download?path=/abs/file&type=file|tar`, response body is raw binary or a tar stream.
//! - `GET /healthz` → `{"status":"ok"}`。
//!
//! Auth model: RRT is privileged, so token auth is required when RRT_HTTP_TOKEN is set. Requests must carry
//! `X-Auth: <token>` or they receive 401. Production should use JWT validation; this path uses a static token.
//!
//! No new dependencies: use raw tokio TcpListener plus handwritten HTTP/1.1.
//! JSON/control responses support sequential keep-alive requests so the Edge
//! backend pool can reuse a connection through the Edge proxy. Streaming responses
//! that require EOF framing still advertise `Connection: close`.

use futures_util::{SinkExt, StreamExt};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsStr;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener, UnixStream};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot, watch};

use crate::posix::runtime_rpc::StreamingMessage;

const IO_BUFFER_SIZE: usize = 256 * 1024;
static COMMAND_WATCHERS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
struct CachedResponse {
    status: u16,
    body: String,
}

#[derive(Clone, Copy)]
enum RequestBody {
    ContentLength(usize),
    Chunked,
}

impl RequestBody {
    fn label(self) -> &'static str {
        match self {
            RequestBody::ContentLength(_) => "content-length",
            RequestBody::Chunked => "chunked",
        }
    }

    fn expected_len(self) -> Option<usize> {
        match self {
            RequestBody::ContentLength(n) => Some(n),
            RequestBody::Chunked => None,
        }
    }
}

#[derive(Default)]
struct CopyStats {
    bytes: usize,
    reads: usize,
    writes: usize,
    read_ms: u128,
    write_ms: u128,
    first_byte_ms: Option<u128>,
}

impl CopyStats {
    fn record_initial_write(&mut self, bytes: usize, write_ms: u128) {
        if bytes == 0 {
            return;
        }
        self.bytes += bytes;
        self.writes += 1;
        self.write_ms += write_ms;
        self.first_byte_ms.get_or_insert(0);
    }

    fn record_read(&mut self, bytes: usize, read_ms: u128, elapsed_ms: u128) {
        if bytes == 0 {
            return;
        }
        self.reads += 1;
        self.read_ms += read_ms;
        self.first_byte_ms.get_or_insert(elapsed_ms);
    }

    fn record_write(&mut self, bytes: usize, write_ms: u128) {
        if bytes == 0 {
            return;
        }
        self.bytes += bytes;
        self.writes += 1;
        self.write_ms += write_ms;
    }
}

/// Serve RRT atomic operations on 0.0.0.0:port. token=None disables auth for isolated/internal use only.
pub async fn serve(port: u16, token: Option<String>) -> Result<(), Box<dyn std::error::Error>> {
    let listener = bind(port).await?;
    serve_listener(listener, token).await
}

pub(crate) async fn bind(port: u16) -> std::io::Result<TcpListener> {
    TcpListener::bind(("0.0.0.0", port)).await
}

#[derive(Clone, Debug)]
pub(crate) struct HttpServerControl {
    inner: Arc<HttpServerControlInner>,
}

#[derive(Debug)]
struct HttpServerControlInner {
    // This descriptor owns the kernel socket but is never registered with a
    // Tokio reactor. Each generation clones it and installs that clone in the
    // current reactor, so restored epoll state is never the only accept path.
    listener: std::net::TcpListener,
    token: Option<String>,
    accept_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    generation: AtomicU64,
    ready_tx: watch::Sender<super::RuntimeReadyState>,
}

impl HttpServerControl {
    pub(crate) fn start(
        listener: TcpListener,
        token: Option<String>,
        ready_tx: watch::Sender<super::RuntimeReadyState>,
    ) -> std::io::Result<Self> {
        let listener = listener.into_std()?;
        listener.set_nonblocking(true)?;
        let control = Self {
            inner: Arc::new(HttpServerControlInner {
                listener,
                token,
                accept_task: Mutex::new(None),
                generation: AtomicU64::new(0),
                ready_tx,
            }),
        };
        control.rearm()?;
        Ok(control)
    }

    pub(crate) fn rearm(&self) -> std::io::Result<u64> {
        let mut accept_task = self
            .inner
            .accept_task
            .lock()
            .map_err(|_| std::io::Error::other("RRT HTTP listener control lock is poisoned"))?;
        let listener = self.inner.listener.try_clone()?;
        listener.set_nonblocking(true)?;
        let listener = TcpListener::from_std(listener)?;
        let generation = self.inner.generation.load(Ordering::Relaxed) + 1;
        let token = self.inner.token.clone();
        self.inner.generation.store(generation, Ordering::Release);
        // Publish the replacement generation before it can report a failure;
        // a fast accept error must win over Ready rather than be overwritten.
        let _ = self.inner.ready_tx.send(super::RuntimeReadyState::Ready);
        let inner = Arc::downgrade(&self.inner);
        let task = tokio::spawn(async move {
            if let Err(error) = serve_listener(listener, token).await {
                if let Some(inner) = inner.upgrade() {
                    if inner.generation.load(Ordering::Acquire) == generation {
                        let message =
                            format!("RRT HTTP accept generation {generation} stopped: {error}");
                        rrt_error!("[rrt-http] {message}");
                        let _ = inner
                            .ready_tx
                            .send(super::RuntimeReadyState::Failed(message));
                    }
                }
            }
        });
        if let Some(previous) = accept_task.replace(task) {
            previous.abort();
        }
        rrt_info!("[rrt-http] listener generation installed generation={generation}");
        Ok(generation)
    }
}

pub(crate) async fn serve_listener(
    listener: TcpListener,
    token: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let address = listener.local_addr()?;
    rrt_info!("[rrt-http] atomic-ops server listening on {address}");
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        let token = token.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_conn(&mut sock, token).await {
                rrt_error!("[rrt-http] conn error: {error}");
            }
        });
    }
}

async fn handle_conn(
    sock: &mut tokio::net::TcpStream,
    token: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        // Distinguish an orderly peer close from the next request without
        // consuming its first byte. The Edge proxy does not pipeline requests on
        // this connection, so one iteration owns one complete exchange.
        let mut first = [0u8; 1];
        if sock.peek(&mut first).await? == 0 {
            return Ok(());
        }
        handle_one_request(sock, token.clone()).await?;
    }
}

async fn handle_one_request(
    sock: &mut tokio::net::TcpStream,
    token: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let _active = super::activity::enter(super::activity::ActivitySource::DirectHttp);
    // Read until the header terminator (\r\n\r\n). Bodies support Content-Length or chunked encoding.
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; IO_BUFFER_SIZE];
    let header_end = loop {
        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            return Ok(()); // Connection closed.
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > 1 << 20 {
            return write_resp(sock, 431, "{\"error\":\"headers too large\"}").await;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let (method, path) = parse_request_line(&head);
    let content_len = parse_content_length(&head);
    let body_mode = if header_has_token(&head, "transfer-encoding", "chunked") {
        RequestBody::Chunked
    } else {
        RequestBody::ContentLength(content_len)
    };
    let auth = parse_header(&head, "x-auth");
    let trace_id = parse_header(&head, "x-trace-id").unwrap_or_default();
    let route = request_path(&path);
    rrt_info!(
        "[rrt-http] request method={} path={} body_mode={} content_len={} initial_body={} trace={}",
        method,
        route,
        body_mode.label(),
        body_mode
            .expected_len()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "-".to_string()),
        buf[header_end..].len(),
        trace_id
    );

    // Health checks do not need a body.
    if method == "GET" && route == "/healthz" {
        return write_resp(sock, 200, "{\"status\":\"ok\"}").await;
    }
    if method == "GET" && route == "/metrics" {
        let metrics = super::cmd::command_metrics();
        let body = format!(
            "command_records {}\ncommand_running {}\ncommand_completed {}\ncommand_result_bytes {}\ncommand_result_truncated_total {}\ncommand_record_expired_total {}\ncommand_id_conflict_total {}\ncommand_watch_connections {}\n",
            metrics.records,
            metrics.running,
            metrics.completed,
            metrics.result_bytes,
            metrics.result_truncated_total,
            metrics.record_expired_total,
            metrics.id_conflict_total,
            COMMAND_WATCHERS.load(std::sync::atomic::Ordering::Relaxed),
        );
        return write_resp(sock, 200, &body).await;
    }
    // Auth: RRT requires token authentication. /invoke, /upload, and /download are all control-plane capabilities.
    if let Some(expect) = token.as_deref() {
        if auth.as_deref() != Some(expect) {
            return write_resp(sock, 401, "{\"error\":\"unauthorized\"}").await;
        }
    }

    if method == "GET"
        && route == "/commands/watch"
        && header_has_token(&head, "upgrade", "websocket")
    {
        return handle_command_watch(sock, &head).await;
    }

    // Ordinary atomic operations are data activity. The long-lived command
    // watch above is deliberately a passive observer and must not prevent idle.
    let _active = super::activity::enter(super::activity::ActivitySource::DirectHttp);

    if method == "GET" && route == "/upload/status" {
        return handle_upload_status(sock, &path).await;
    }
    if method == "POST" && route == "/upload/commit" {
        return handle_upload_commit(sock, &path).await;
    }
    if method == "POST" && route == "/upload" {
        return handle_upload(
            sock,
            &path,
            body_mode,
            &buf[header_end..],
            &mut tmp,
            trace_id.as_str(),
        )
        .await;
    }
    if method == "GET" && route == "/download" {
        return handle_download(sock, &path, &head, &mut tmp, trace_id.as_str()).await;
    }

    if !(method == "POST" && route == "/invoke") {
        return write_resp(sock, 404, "{\"error\":\"not found\"}").await;
    }

    // /invoke needs a JSON body. Read the full body only on this path to avoid buffering large /upload payloads in memory.
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_len {
        let n = sock.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }

    let parsed: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return write_resp(sock, 400, &err_json(&format!("bad json: {e}"))).await,
    };
    let action = parsed
        .get("action")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let kw = json_args_to_kwargs(parsed.get("args"));
    let request_id = request_id_from(&head, &parsed);

    let resp = execute_invoke(request_id, action, kw, trace_id).await;
    write_resp(sock, resp.status, &resp.body).await
}

async fn handle_command_watch(
    sock: &mut tokio::net::TcpStream,
    head: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    struct WatchGuard;
    impl Drop for WatchGuard {
        fn drop(&mut self) {
            COMMAND_WATCHERS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    COMMAND_WATCHERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let _guard = WatchGuard;
    let key = parse_header(head, "sec-websocket-key").ok_or("missing websocket key")?;
    let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(key.as_bytes());
    sock.write_all(
        format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        )
        .as_bytes(),
    )
    .await?;
    let mut websocket = tokio_tungstenite::WebSocketStream::from_raw_socket(
        sock,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let mut subscriptions = HashSet::<String>::new();
    let mut versions = HashMap::<String, u64>::new();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(200));
    let max_subscriptions = std::env::var("RRT_COMMAND_WATCH_MAX_SUBSCRIPTIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4096);
    let max_frame_bytes = std::env::var("RRT_COMMAND_WATCH_MAX_FRAME_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1024 * 1024);

    loop {
        tokio::select! {
            message = websocket.next() => {
                let Some(message) = message else { break; };
                match message? {
                    tokio_tungstenite::tungstenite::Message::Text(text) => {
                        if text.len() > max_frame_bytes {
                            websocket.close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Size,
                                reason: "command watch frame exceeds configured limit".into(),
                            })).await?;
                            break;
                        }
                        let request: serde_json::Value = serde_json::from_str(&text)?;
                        let op = request.get("op").and_then(|value| value.as_str()).unwrap_or("");
                        let ids = request.get("commandIds").and_then(|value| value.as_array())
                            .map(|items| items.iter().filter_map(|item| item.as_str().map(str::to_owned)).collect::<Vec<_>>())
                            .unwrap_or_default();
                        match op {
                            "subscribe" => {
                                let new_count = ids.iter().filter(|id| !subscriptions.contains(*id)).count();
                                if subscriptions.len().saturating_add(new_count) > max_subscriptions {
                                    websocket.close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                        code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Policy,
                                        reason: "command watch subscription limit reached".into(),
                                    })).await?;
                                    break;
                                }
                                for id in ids {
                                    subscriptions.insert(id.clone());
                                    // Every subscribe operation promises an
                                    // immediate current-state snapshot, even
                                    // when this connection already watches the
                                    // same command for another Edge consumer.
                                    versions.remove(&id);
                                }
                            }
                            "unsubscribe" => {
                                for id in ids { subscriptions.remove(&id); versions.remove(&id); }
                            }
                            _ => {
                                websocket.close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                    code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Protocol,
                                    reason: "unsupported command watch operation".into(),
                                })).await?;
                                break;
                            }
                        }
                    }
                    tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                        websocket.send(tokio_tungstenite::tungstenite::Message::Pong(payload)).await?;
                    }
                    tokio_tungstenite::tungstenite::Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = tick.tick() => {
                if subscriptions.is_empty() { continue; }
                let ids: Vec<String> = subscriptions.iter().cloned().collect();
                for state in super::cmd::watch_snapshot(&ids) {
                    let json = rmpv_to_json(&state);
                    let command_id = json.get("command_id").and_then(|value| value.as_str()).unwrap_or("");
                    let version = json.get("state_version").and_then(|value| value.as_u64()).unwrap_or(0);
                    if versions.get(command_id).copied().unwrap_or(u64::MAX) == version { continue; }
                    versions.insert(command_id.to_owned(), version);
                    let response = serde_json::json!({
                        "op": "state",
                        "commandId": command_id,
                        "status": json.get("status").cloned().unwrap_or(serde_json::Value::Null),
                        "stateVersion": version,
                    });
                    websocket.send(tokio_tungstenite::tungstenite::Message::Text(response.to_string())).await?;
                }
            }
        }
    }
    Ok(())
}

async fn execute_invoke(
    request_id: Option<String>,
    action: String,
    kw: BTreeMap<String, rmpv::Value>,
    trace_id: String,
) -> CachedResponse {
    let normalized_action = super::dispatch::normalize_sandbox_action(&action);
    let result = tokio::task::spawn_blocking(move || {
        super::dispatch::execute_sandbox_action_once(request_id.as_deref(), &action, &kw, &trace_id)
    })
    .await;

    match result {
        Ok(Ok(result)) => {
            let json = rmpv_to_json(&result);
            let error = json
                .get("error")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let error_code = json
                .get("error_code")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let status = if error.is_empty() {
                200
            } else {
                match error_code {
                    "COMMAND_NOT_FOUND" => 404,
                    "COMMAND_CONFLICT" => 409,
                    "RESOURCE_EXHAUSTED" => 429,
                    "UNSUPPORTED_FEATURE" => 501,
                    _ if normalized_action == Some("cmd_start") => 400,
                    _ => 400,
                }
            };
            CachedResponse {
                status,
                body: serde_json::to_string(&json).unwrap_or_else(|_| "{}".into()),
            }
        }
        Ok(Err(message)) => CachedResponse {
            status: 400,
            body: err_json(&message),
        },
        Err(e) => CachedResponse {
            status: 500,
            body: err_json(&format!("dispatch failed: {e}")),
        },
    }
}

async fn handle_upload(
    sock: &mut tokio::net::TcpStream,
    raw_path: &str,
    body_mode: RequestBody,
    initial_body: &[u8],
    tmp: &mut [u8],
    trace_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = match query_param(raw_path, "path").and_then(|p| percent_decode(&p)) {
        Some(p) if !p.is_empty() => p,
        _ => return write_resp(sock, 400, "{\"error\":\"missing path\"}").await,
    };
    let started = Instant::now();
    rrt_info!(
        "[rrt-http] upload start type={} path={} body_mode={} content_len={} initial_body={} trace={}",
        upload_type(raw_path),
        path,
        body_mode.label(),
        body_mode
            .expected_len()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "-".to_string()),
        initial_body.len(),
        trace_id
    );
    match upload_type(raw_path).as_str() {
        "tar" => {
            handle_tar_upload(sock, &path, body_mode, initial_body, tmp, started, trace_id).await
        }
        "file" | "" => {
            handle_file_upload(
                sock,
                raw_path,
                &path,
                body_mode,
                initial_body,
                tmp,
                started,
                trace_id,
            )
            .await
        }
        other => {
            write_resp(
                sock,
                400,
                &err_json(&format!("unsupported upload type: {other}")),
            )
            .await
        }
    }
}

async fn handle_file_upload(
    sock: &mut tokio::net::TcpStream,
    raw_path: &str,
    path: &str,
    body_mode: RequestBody,
    initial_body: &[u8],
    tmp: &mut [u8],
    started: Instant,
    trace_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(upload_id) = query_param(raw_path, "uploadId").and_then(|p| percent_decode(&p)) {
        return handle_file_upload_chunk(
            sock,
            raw_path,
            path,
            &upload_id,
            body_mode,
            initial_body,
            tmp,
            started,
            trace_id,
        )
        .await;
    }
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let open_started = Instant::now();
    let mut file = tokio::fs::File::create(path).await?;
    let open_ms = open_started.elapsed().as_millis();
    let copy_started = Instant::now();
    let stats = copy_request_body(sock, body_mode, initial_body, tmp, &mut file).await?;
    let copy_ms = copy_started.elapsed().as_millis();
    let flush_started = Instant::now();
    file.flush().await?;
    let flush_ms = flush_started.elapsed().as_millis();
    rrt_info!(
        "[rrt-http] upload type=file path={} bytes={} body_mode={} content_len={} initial_body={} open_ms={} copy_ms={} read_ms={} write_ms={} flush_ms={} reads={} writes={} first_byte_ms={} total_ms={} trace={}",
        path,
        stats.bytes,
        body_mode.label(),
        body_mode
            .expected_len()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "-".to_string()),
        initial_body.len(),
        open_ms,
        copy_ms,
        stats.read_ms,
        stats.write_ms,
        flush_ms,
        stats.reads,
        stats.writes,
        stats
            .first_byte_ms
            .map(|n| n.to_string())
            .unwrap_or_else(|| "-".to_string()),
        started.elapsed().as_millis(),
        trace_id
    );

    let meta = std::fs::metadata(path).ok();
    let name = Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    let body = serde_json::json!({
        "error": null,
        "name": name,
        "path": path,
        "type": "file",
        "size": meta.map(|m| m.len()).unwrap_or(stats.bytes as u64),
        "bytes_written": stats.bytes,
    })
    .to_string();
    let resp_started = Instant::now();
    let r = write_resp(sock, 200, &body).await;
    rrt_info!(
        "[rrt-http] upload response type=file path={} resp_ms={} trace={}",
        path,
        resp_started.elapsed().as_millis(),
        trace_id
    );
    r
}

async fn handle_file_upload_chunk(
    sock: &mut tokio::net::TcpStream,
    raw_path: &str,
    path: &str,
    upload_id: &str,
    body_mode: RequestBody,
    initial_body: &[u8],
    tmp: &mut [u8],
    started: Instant,
    trace_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let part = upload_part_path(path, upload_id);
    let offset = query_param(raw_path, "offset")
        .and_then(|v| percent_decode(&v))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let current = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
    if offset != current {
        let body = serde_json::json!({
            "error": "offset mismatch",
            "path": path,
            "uploadId": upload_id,
            "offset": current,
            "expected_offset": current,
        })
        .to_string();
        return write_resp(sock, 409, &body).await;
    }

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&part)
        .await?;
    let stats = copy_request_body(sock, body_mode, initial_body, tmp, &mut file).await?;
    file.flush().await?;
    let new_offset = current + stats.bytes as u64;
    rrt_info!(
        "[rrt-http] upload chunk path={} upload_id={} offset={} bytes={} new_offset={} total_ms={} trace={}",
        path,
        upload_id,
        offset,
        stats.bytes,
        new_offset,
        started.elapsed().as_millis(),
        trace_id
    );
    let body = serde_json::json!({
        "error": null,
        "path": path,
        "type": "file",
        "uploadId": upload_id,
        "offset": new_offset,
        "bytes_written": stats.bytes,
        "committed": false,
    })
    .to_string();
    write_resp(sock, 200, &body).await
}

async fn handle_upload_status(
    sock: &mut tokio::net::TcpStream,
    raw_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = match query_param(raw_path, "path").and_then(|p| percent_decode(&p)) {
        Some(p) if !p.is_empty() => p,
        _ => return write_resp(sock, 400, "{\"error\":\"missing path\"}").await,
    };
    let upload_id = match query_param(raw_path, "uploadId").and_then(|p| percent_decode(&p)) {
        Some(p) if !p.is_empty() => p,
        _ => return write_resp(sock, 400, "{\"error\":\"missing uploadId\"}").await,
    };
    let part = upload_part_path(&path, &upload_id);
    let offset = std::fs::metadata(&part).map(|m| m.len()).unwrap_or(0);
    let body = serde_json::json!({
        "error": null,
        "path": path,
        "uploadId": upload_id,
        "offset": offset,
        "exists": offset > 0,
    })
    .to_string();
    write_resp(sock, 200, &body).await
}

async fn handle_upload_commit(
    sock: &mut tokio::net::TcpStream,
    raw_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = match query_param(raw_path, "path").and_then(|p| percent_decode(&p)) {
        Some(p) if !p.is_empty() => p,
        _ => return write_resp(sock, 400, "{\"error\":\"missing path\"}").await,
    };
    let upload_id = match query_param(raw_path, "uploadId").and_then(|p| percent_decode(&p)) {
        Some(p) if !p.is_empty() => p,
        _ => return write_resp(sock, 400, "{\"error\":\"missing uploadId\"}").await,
    };
    let total_size = query_param(raw_path, "totalSize")
        .and_then(|v| percent_decode(&v))
        .and_then(|v| v.parse::<u64>().ok());
    let part = upload_part_path(&path, &upload_id);
    let meta = match std::fs::metadata(&part) {
        Ok(m) => m,
        Err(_) => return write_resp(sock, 404, &err_json("upload part not found")).await,
    };
    if let Some(total) = total_size {
        if meta.len() != total {
            let body = serde_json::json!({
                "error": "size mismatch",
                "path": path,
                "uploadId": upload_id,
                "offset": meta.len(),
                "expected_size": total,
            })
            .to_string();
            return write_resp(sock, 409, &body).await;
        }
    }
    if let Some(parent) = Path::new(&path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    std::fs::rename(&part, &path)?;
    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    let body = serde_json::json!({
        "error": null,
        "name": Path::new(&path).file_name().and_then(|n| n.to_str()).unwrap_or(""),
        "path": path,
        "type": "file",
        "size": size,
        "bytes_written": size,
        "uploadId": upload_id,
        "committed": true,
    })
    .to_string();
    write_resp(sock, 200, &body).await
}

async fn handle_tar_upload(
    sock: &mut tokio::net::TcpStream,
    path: &str,
    body_mode: RequestBody,
    initial_body: &[u8],
    tmp: &mut [u8],
    started: Instant,
    trace_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(path)?;
    let mut child = Command::new("tar");
    child
        .arg("--no-same-owner")
        .arg("--no-same-permissions")
        .arg("-x")
        .arg("-C")
        .arg(path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    super::child_env::apply_tokio(&mut child);
    let mut child = child.spawn()?;
    let mut stdin = child.stdin.take().ok_or("tar stdin unavailable")?;
    let copy_started = Instant::now();
    let stats = copy_request_body(sock, body_mode, initial_body, tmp, &mut stdin).await?;
    let copy_ms = copy_started.elapsed().as_millis();
    let shutdown_started = Instant::now();
    stdin.shutdown().await?;
    let stdin_shutdown_ms = shutdown_started.elapsed().as_millis();
    drop(stdin);
    let wait_started = Instant::now();
    let status = child.wait().await?;
    let tar_wait_ms = wait_started.elapsed().as_millis();
    if !status.success() {
        return write_resp(sock, 400, &err_json("tar extract failed")).await;
    }
    rrt_info!(
        "[rrt-http] upload type=tar path={} bytes={} body_mode={} content_len={} initial_body={} copy_ms={} read_ms={} write_ms={} stdin_shutdown_ms={} tar_wait_ms={} reads={} writes={} first_byte_ms={} total_ms={} trace={}",
        path,
        stats.bytes,
        body_mode.label(),
        body_mode
            .expected_len()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "-".to_string()),
        initial_body.len(),
        copy_ms,
        stats.read_ms,
        stats.write_ms,
        stdin_shutdown_ms,
        tar_wait_ms,
        stats.reads,
        stats.writes,
        stats
            .first_byte_ms
            .map(|n| n.to_string())
            .unwrap_or_else(|| "-".to_string()),
        started.elapsed().as_millis(),
        trace_id
    );
    let body = serde_json::json!({
        "error": null,
        "name": Path::new(path).file_name().and_then(|n| n.to_str()).unwrap_or(""),
        "path": path,
        "type": "dir",
        "size": stats.bytes,
        "bytes_written": stats.bytes,
    })
    .to_string();
    let resp_started = Instant::now();
    let r = write_resp(sock, 200, &body).await;
    rrt_info!(
        "[rrt-http] upload response type=tar path={} resp_ms={} trace={}",
        path,
        resp_started.elapsed().as_millis(),
        trace_id
    );
    r
}

async fn copy_request_body<W: AsyncWrite + Unpin>(
    sock: &mut tokio::net::TcpStream,
    body_mode: RequestBody,
    initial_body: &[u8],
    tmp: &mut [u8],
    writer: &mut W,
) -> Result<CopyStats, Box<dyn std::error::Error>> {
    match body_mode {
        RequestBody::ContentLength(content_len) => {
            copy_content_length_body(sock, content_len, initial_body, tmp, writer).await
        }
        RequestBody::Chunked => copy_chunked_body(sock, initial_body, tmp, writer).await,
    }
}

async fn copy_content_length_body<W: AsyncWrite + Unpin>(
    sock: &mut tokio::net::TcpStream,
    content_len: usize,
    initial_body: &[u8],
    tmp: &mut [u8],
    writer: &mut W,
) -> Result<CopyStats, Box<dyn std::error::Error>> {
    let mut remaining = content_len;
    let started = Instant::now();
    let mut stats = CopyStats::default();
    let first = initial_body.len().min(remaining);
    if first > 0 {
        let write_started = Instant::now();
        writer.write_all(&initial_body[..first]).await?;
        stats.record_initial_write(first, write_started.elapsed().as_millis());
        remaining -= first;
    }
    while remaining > 0 {
        let read_started = Instant::now();
        let n = sock.read(tmp).await?;
        let read_ms = read_started.elapsed().as_millis();
        if n == 0 {
            break;
        }
        stats.record_read(n, read_ms, started.elapsed().as_millis());
        let take = n.min(remaining);
        let write_started = Instant::now();
        writer.write_all(&tmp[..take]).await?;
        stats.record_write(take, write_started.elapsed().as_millis());
        remaining -= take;
    }
    if remaining != 0 {
        return Err("short body".into());
    }
    Ok(stats)
}

async fn copy_chunked_body<W: AsyncWrite + Unpin>(
    sock: &mut tokio::net::TcpStream,
    initial_body: &[u8],
    tmp: &mut [u8],
    writer: &mut W,
) -> Result<CopyStats, Box<dyn std::error::Error>> {
    let mut reader = ChunkedReader {
        sock,
        pending: initial_body.to_vec(),
        tmp,
    };
    let started = Instant::now();
    let mut stats = CopyStats::default();
    loop {
        let line = reader.read_line().await?;
        let size_token = line
            .trim()
            .split_once(';')
            .map(|(size, _)| size)
            .unwrap_or_else(|| line.trim());
        let chunk_size = usize::from_str_radix(size_token, 16)?;
        if chunk_size == 0 {
            // Consume optional trailer headers through the terminating blank line.
            loop {
                let trailer = reader.read_line().await?;
                if trailer.trim().is_empty() {
                    break;
                }
            }
            break;
        }
        let chunk = reader.read_exact_vec(chunk_size).await?;
        stats.record_read(chunk.len(), 0, started.elapsed().as_millis());
        let write_started = Instant::now();
        writer.write_all(&chunk).await?;
        stats.record_write(chunk.len(), write_started.elapsed().as_millis());
        let crlf = reader.read_exact_vec(2).await?;
        if crlf.as_slice() != b"\r\n" {
            return Err("invalid chunk terminator".into());
        }
    }
    Ok(stats)
}

struct ChunkedReader<'a> {
    sock: &'a mut tokio::net::TcpStream,
    pending: Vec<u8>,
    tmp: &'a mut [u8],
}

impl<'a> ChunkedReader<'a> {
    async fn read_more(&mut self) -> Result<bool, Box<dyn std::error::Error>> {
        let n = self.sock.read(self.tmp).await?;
        if n == 0 {
            return Ok(false);
        }
        self.pending.extend_from_slice(&self.tmp[..n]);
        Ok(true)
    }

    async fn read_line(&mut self) -> Result<String, Box<dyn std::error::Error>> {
        loop {
            if let Some(pos) = find_subslice(&self.pending, b"\r\n") {
                let line = self.pending.drain(..pos + 2).collect::<Vec<_>>();
                return Ok(String::from_utf8_lossy(&line[..line.len() - 2]).to_string());
            }
            if !self.read_more().await? {
                return Err("unexpected EOF while reading chunk line".into());
            }
        }
    }

    async fn read_exact_vec(&mut self, len: usize) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        while self.pending.len() < len {
            if !self.read_more().await? {
                return Err("unexpected EOF while reading chunk body".into());
            }
        }
        Ok(self.pending.drain(..len).collect())
    }
}

async fn handle_download(
    sock: &mut tokio::net::TcpStream,
    raw_path: &str,
    head: &str,
    tmp: &mut [u8],
    trace_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = match query_param(raw_path, "path").and_then(|p| percent_decode(&p)) {
        Some(p) if !p.is_empty() => p,
        _ => return write_resp(sock, 400, "{\"error\":\"missing path\"}").await,
    };
    match upload_type(raw_path).as_str() {
        "tar" => handle_tar_download(sock, &path, tmp, trace_id).await,
        "file" | "" => handle_file_download(sock, &path, head, tmp, trace_id).await,
        other => {
            write_resp(
                sock,
                400,
                &err_json(&format!("unsupported download type: {other}")),
            )
            .await
        }
    }
}

async fn handle_file_download(
    sock: &mut tokio::net::TcpStream,
    path: &str,
    head: &str,
    tmp: &mut [u8],
    trace_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    let meta = match std::fs::metadata(path) {
        Ok(m) if m.is_file() => m,
        _ => return write_resp(sock, 404, &err_json("file not found")).await,
    };
    let total = meta.len();
    let range = parse_range_header(head, total);
    let (status, start, end) = match range {
        Some((start, end)) => (206, start, end),
        None => (200, 0, total.saturating_sub(1)),
    };
    let content_len = if total == 0 { 0 } else { end - start + 1 };
    write_binary_headers_with_range(
        sock,
        status,
        "application/octet-stream",
        Some(content_len),
        range.map(|_| (start, end, total)),
    )
    .await?;
    let mut file = tokio::fs::File::open(path).await?;
    if start > 0 {
        file.seek(SeekFrom::Start(start)).await?;
    }
    let mut bytes_sent = 0u64;
    let mut remaining = content_len;
    while remaining > 0 {
        let limit = remaining.min(tmp.len() as u64) as usize;
        let n = file.read(&mut tmp[..limit]).await?;
        if n == 0 {
            break;
        }
        sock.write_all(&tmp[..n]).await?;
        bytes_sent += n as u64;
        remaining -= n as u64;
    }
    sock.flush().await?;
    rrt_info!(
        "[rrt-http] download type=file bytes={} range_start={} total_ms={} trace={}",
        bytes_sent,
        start,
        started.elapsed().as_millis(),
        trace_id
    );
    Ok(())
}

async fn handle_tar_download(
    sock: &mut tokio::net::TcpStream,
    path: &str,
    tmp: &mut [u8],
    trace_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let started = Instant::now();
    if !Path::new(path).is_dir() {
        return write_resp(sock, 404, &err_json("directory not found")).await;
    }
    let mut child = Command::new("tar");
    child
        .arg("-C")
        .arg(path)
        .arg("-cf")
        .arg("-")
        .arg(".")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    super::child_env::apply_tokio(&mut child);
    let mut child = child.spawn()?;
    let mut stdout = child.stdout.take().ok_or("tar stdout unavailable")?;
    write_binary_headers(sock, 200, "application/x-tar", None).await?;
    let mut bytes_sent = 0u64;
    loop {
        let n = stdout.read(tmp).await?;
        if n == 0 {
            break;
        }
        sock.write_all(&tmp[..n]).await?;
        bytes_sent += n as u64;
    }
    sock.flush().await?;
    let status = child.wait().await?;
    if !status.success() {
        rrt_error!("[rrt-http] tar create failed path={path} status={status}");
    }
    rrt_info!(
        "[rrt-http] download type=tar bytes={} total_ms={} trace={}",
        bytes_sent,
        started.elapsed().as_millis(),
        trace_id
    );
    Ok(())
}

async fn write_binary_headers(
    sock: &mut tokio::net::TcpStream,
    status: u16,
    content_type: &str,
    content_len: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    write_binary_headers_with_range(sock, status, content_type, content_len, None).await
}

async fn write_binary_headers_with_range(
    sock: &mut tokio::net::TcpStream,
    status: u16,
    content_type: &str,
    content_len: Option<u64>,
    content_range: Option<(u64, u64, u64)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let reason = match status {
        200 => "OK",
        206 => "Partial Content",
        404 => "Not Found",
        _ => "Error",
    };
    let len_header = content_len
        .map(|n| format!("Content-Length: {n}\r\n"))
        .unwrap_or_default();
    let range_header = content_range
        .map(|(start, end, total)| format!("Content-Range: bytes {start}-{end}/{total}\r\n"))
        .unwrap_or_default();
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\n{len_header}{range_header}Connection: close\r\n\r\n"
    );
    sock.write_all(resp.as_bytes()).await?;
    Ok(())
}

fn err_json(msg: &str) -> String {
    serde_json::json!({ "error": msg }).to_string()
}

async fn write_resp(
    sock: &mut tokio::net::TcpStream,
    status: u16,
    body: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        429 => "Too Many Requests",
        501 => "Not Implemented",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        _ => "Error",
    };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",
        body.as_bytes().len()
    );
    sock.write_all(resp.as_bytes()).await?;
    sock.flush().await?;
    Ok(())
}

// ── HTTP parsing helpers ────────────────────────────────────────────

fn request_path(path: &str) -> &str {
    path.split_once('?').map(|(p, _)| p).unwrap_or(path)
}

fn query_param(raw_path: &str, name: &str) -> Option<String> {
    let query = raw_path.split_once('?')?.1;
    for pair in query.split('&') {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        if k == name {
            return Some(v.to_string());
        }
    }
    None
}

fn upload_type(raw_path: &str) -> String {
    query_param(raw_path, "type")
        .and_then(|v| percent_decode(&v))
        .unwrap_or_else(|| "file".to_string())
}

fn percent_decode(input: &str) -> Option<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = hex_val(bytes[i + 1])?;
                let lo = hex_val(bytes[i + 2])?;
                out.push((hi << 4) | lo);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn parse_request_line(head: &str) -> (String, String) {
    let line = head.lines().next().unwrap_or("");
    let mut it = line.split_whitespace();
    (
        it.next().unwrap_or("").to_string(),
        it.next().unwrap_or("").to_string(),
    )
}

fn parse_content_length(head: &str) -> usize {
    parse_header(head, "content-length")
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0)
}

fn parse_header(head: &str, name: &str) -> Option<String> {
    head.lines()
        .skip(1)
        .find_map(|l| {
            l.split_once(':')
                .filter(|(k, _)| k.trim().eq_ignore_ascii_case(name))
        })
        .map(|(_, v)| v.trim().to_string())
}

fn header_has_token(head: &str, name: &str, token: &str) -> bool {
    parse_header(head, name)
        .map(|value| {
            value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case(token))
        })
        .unwrap_or(false)
}

fn request_id_from(head: &str, parsed: &serde_json::Value) -> Option<String> {
    let raw = parse_header(head, "x-adx-request-id")
        .or_else(|| parse_header(head, "x-request-id"))
        .or_else(|| {
            parsed
                .get("requestId")
                .and_then(|v| v.as_str())
                .map(str::to_string)
        });
    raw.map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty() && v.len() <= 128)
}

fn upload_part_path(path: &str, upload_id: &str) -> PathBuf {
    let p = Path::new(path);
    let parent = p.parent().unwrap_or_else(|| Path::new(""));
    let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("upload");
    let safe_id: String = upload_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect();
    parent.join(format!(".{name}.adx-upload.{safe_id}.part"))
}

fn parse_range_header(head: &str, total: u64) -> Option<(u64, u64)> {
    let value = parse_header(head, "range")?;
    let spec = value.strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    if start.is_empty() || total == 0 {
        return None;
    }
    let start = start.parse::<u64>().ok()?;
    if start >= total {
        return None;
    }
    let end = if end.is_empty() {
        total - 1
    } else {
        end.parse::<u64>().ok()?.min(total - 1)
    };
    if end < start {
        return None;
    }
    Some((start, end))
}

// ── JSON ↔ rmpv conversion ──────────────────────────────────────────

fn json_args_to_kwargs(args: Option<&serde_json::Value>) -> BTreeMap<String, rmpv::Value> {
    let mut out = BTreeMap::new();
    if let Some(serde_json::Value::Object(o)) = args {
        for (k, v) in o {
            out.insert(k.clone(), json_to_rmpv(v));
        }
    }
    out
}

fn json_to_rmpv(j: &serde_json::Value) -> rmpv::Value {
    use rmpv::Value as V;
    match j {
        serde_json::Value::Null => V::Nil,
        serde_json::Value::Bool(b) => V::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                V::from(i)
            } else if let Some(u) = n.as_u64() {
                V::from(u)
            } else {
                V::from(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::String(s) => V::from(s.clone()),
        serde_json::Value::Array(a) => V::Array(a.iter().map(json_to_rmpv).collect()),
        serde_json::Value::Object(o) => V::Map(
            o.iter()
                .map(|(k, v)| (V::from(k.clone()), json_to_rmpv(v)))
                .collect(),
        ),
    }
}

fn rmpv_to_json(v: &rmpv::Value) -> serde_json::Value {
    use rmpv::Value as V;
    match v {
        V::Nil => serde_json::Value::Null,
        V::Boolean(b) => serde_json::Value::Bool(*b),
        V::Integer(i) => i
            .as_i64()
            .map(|x| serde_json::json!(x))
            .or_else(|| i.as_u64().map(|x| serde_json::json!(x)))
            .unwrap_or(serde_json::Value::Null),
        V::F32(f) => serde_json::json!(*f),
        V::F64(f) => serde_json::json!(*f),
        V::String(s) => serde_json::Value::String(s.as_str().unwrap_or("").to_string()),
        V::Binary(b) => serde_json::Value::String(hex_encode(b)),
        V::Array(a) => serde_json::Value::Array(a.iter().map(rmpv_to_json).collect()),
        V::Map(m) => {
            let mut o = serde_json::Map::new();
            for (k, val) in m {
                let key = k
                    .as_str()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| k.to_string());
                o.insert(key, rmpv_to_json(val));
            }
            serde_json::Value::Object(o)
        }
        V::Ext(_, _) => serde_json::Value::Null,
    }
}

fn hex_encode(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for byte in b {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

pub(crate) const RRT_CONTROL_SOCKET_PATH_ENV: &str = "ADX_RRT_CONTROL_SOCKET_PATH";
const CHECKPOINT_SOCKET_NAME: &str = "rrt.sock";

pub(crate) fn checkpoint_socket_path_from_control_directory(
    configured_directory: Option<&OsStr>,
) -> Option<PathBuf> {
    configured_directory
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|directory| directory.join(CHECKPOINT_SOCKET_NAME))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CheckpointHttpResponse {
    pub status: u16,
    pub body: String,
}

#[derive(Debug)]
struct PendingCheckpointRequest {
    request_id: String,
    handoff_complete: bool,
    snap_started: bool,
    completion: Option<oneshot::Sender<Result<(), String>>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CheckpointRequestCoordinator {
    pending: Arc<Mutex<Option<PendingCheckpointRequest>>>,
}

impl CheckpointRequestCoordinator {
    fn begin(&self, request_id: String) -> Result<oneshot::Receiver<Result<(), String>>, String> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| "checkpoint coordinator lock is poisoned".to_string())?;
        if pending.is_some() {
            return Err("checkpoint is already in progress".to_string());
        }
        let (completion, receiver) = oneshot::channel();
        *pending = Some(PendingCheckpointRequest {
            request_id,
            handoff_complete: false,
            snap_started: false,
            completion: Some(completion),
        });
        Ok(receiver)
    }

    fn fail(&self, request_id: &str, message: String) {
        let Ok(mut pending) = self.pending.lock() else {
            return;
        };
        if pending.as_ref().map(|value| value.request_id.as_str()) != Some(request_id) {
            return;
        }
        if let Some(mut request) = pending.take() {
            if let Some(completion) = request.completion.take() {
                let _ = completion.send(Err(message));
            }
        }
    }

    pub(crate) fn reject_unstarted_checkpoint(&self, request_id: &str, message: String) -> bool {
        let matches = self.pending.lock().ok().is_some_and(|pending| {
            pending
                .as_ref()
                .is_some_and(|request| request.request_id == request_id)
        });
        if matches {
            self.fail(request_id, format!("proxy rejected checkpoint: {message}"));
        }
        matches
    }

    pub(crate) fn record_proxy_ack(&self, request_id: &str, code: i32, message: String) {
        if code != crate::posix::common::ErrorCode::ErrNone as i32 {
            self.fail(request_id, format!("proxy rejected checkpoint: {message}"));
        }
    }

    pub(crate) fn record_handoff(&self, outcome: crate::startup::CheckpointOutcome) {
        if outcome == crate::startup::CheckpointOutcome::Error {
            self.record_handoff_error("checkpoint handoff reported error".to_string());
            return;
        }
        let Ok(mut pending) = self.pending.lock() else {
            return;
        };
        // A restored runtime inherits the request that initiated the source
        // checkpoint. The target must still complete its own SnapStarted
        // handshake. Drop the copied source request so it cannot complete
        // against the target's lifecycle events or leave the coordinator busy.
        if outcome == crate::startup::CheckpointOutcome::Restore {
            pending.take();
            return;
        }
        let should_complete = if let Some(request) = pending.as_mut() {
            request.handoff_complete = true;
            request.snap_started
        } else {
            false
        };
        if should_complete {
            complete_pending_checkpoint(&mut pending);
        }
    }

    pub(crate) fn record_handoff_error(&self, message: String) {
        let request_id = self
            .pending
            .lock()
            .ok()
            .and_then(|pending| pending.as_ref().map(|request| request.request_id.clone()));
        if let Some(request_id) = request_id {
            self.fail(&request_id, message);
        }
    }

    pub(crate) fn record_snap_started(&self, code: i32, message: String) {
        if code != crate::posix::common::ErrorCode::ErrNone as i32 {
            self.record_handoff_error(format!("Proxy failed to finalize checkpoint: {message}"));
            return;
        }
        let Ok(mut pending) = self.pending.lock() else {
            return;
        };
        let should_complete = if let Some(request) = pending.as_mut() {
            request.snap_started = true;
            request.handoff_complete
        } else {
            false
        };
        if should_complete {
            complete_pending_checkpoint(&mut pending);
        }
    }
}

fn complete_pending_checkpoint(pending: &mut Option<PendingCheckpointRequest>) {
    if let Some(mut request) = pending.take() {
        if let Some(completion) = request.completion.take() {
            let _ = completion.send(Ok(()));
        }
    }
}

pub(crate) async fn invoke_checkpoint_handler(
    instance_id: &str,
    tx: mpsc::Sender<StreamingMessage>,
    coordinator: CheckpointRequestCoordinator,
) -> CheckpointHttpResponse {
    let _active = super::activity::enter(super::activity::ActivitySource::Checkpoint);
    let message = super::checkpoint_request_msg(instance_id);
    let request_id = message.message_id.clone();
    let completion = match coordinator.begin(request_id.clone()) {
        Ok(completion) => completion,
        Err(error) => {
            return CheckpointHttpResponse {
                status: 409,
                body: err_json(&error),
            };
        }
    };
    if let Err(error) = tx.send(message).await {
        coordinator.fail(
            &request_id,
            format!("checkpoint channel unavailable: {error}"),
        );
        return CheckpointHttpResponse {
            status: 503,
            body: err_json(&format!("checkpoint channel unavailable: {error}")),
        };
    }
    match completion.await {
        Ok(Ok(())) => CheckpointHttpResponse {
            status: 200,
            body: r#"{"status":"completed"}"#.to_string(),
        },
        Ok(Err(error)) => CheckpointHttpResponse {
            status: 500,
            body: err_json(&error),
        },
        Err(error) => CheckpointHttpResponse {
            status: 500,
            body: err_json(&format!("checkpoint completion was canceled: {error}")),
        },
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CheckpointServerControl {
    inner: Arc<CheckpointServerControlInner>,
}

#[derive(Debug)]
struct CheckpointServerControlInner {
    listener: std::os::unix::net::UnixListener,
    instance_id: RwLock<String>,
    tx: mpsc::Sender<StreamingMessage>,
    accept_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    generation: AtomicU64,
    ready_tx: watch::Sender<super::RuntimeReadyState>,
    coordinator: CheckpointRequestCoordinator,
}

impl CheckpointServerControl {
    pub(crate) fn start(
        listener: UnixListener,
        instance_id: String,
        tx: mpsc::Sender<StreamingMessage>,
        ready_tx: watch::Sender<super::RuntimeReadyState>,
    ) -> std::io::Result<Self> {
        let listener = listener.into_std()?;
        listener.set_nonblocking(true)?;
        let control = Self {
            inner: Arc::new(CheckpointServerControlInner {
                listener,
                instance_id: RwLock::new(instance_id),
                tx,
                accept_task: Mutex::new(None),
                generation: AtomicU64::new(0),
                ready_tx,
                coordinator: CheckpointRequestCoordinator::default(),
            }),
        };
        control.rearm()?;
        Ok(control)
    }

    pub(crate) fn rebind_instance_id(&self, instance_id: &str) {
        *self
            .inner
            .instance_id
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = instance_id.to_string();
    }

    pub(crate) fn reject_unstarted_checkpoint(&self, request_id: &str, message: String) -> bool {
        self.inner
            .coordinator
            .reject_unstarted_checkpoint(request_id, message)
    }

    pub(crate) fn record_proxy_ack(&self, request_id: &str, code: i32, message: String) {
        self.inner
            .coordinator
            .record_proxy_ack(request_id, code, message);
    }

    pub(crate) fn record_handoff(&self, outcome: crate::startup::CheckpointOutcome) {
        self.inner.coordinator.record_handoff(outcome);
    }

    pub(crate) fn record_handoff_error(&self, message: String) {
        self.inner.coordinator.record_handoff_error(message);
    }

    pub(crate) fn record_snap_started(&self, code: i32, message: String) {
        self.inner.coordinator.record_snap_started(code, message);
    }

    pub(crate) fn rearm(&self) -> std::io::Result<u64> {
        let mut accept_task = self
            .inner
            .accept_task
            .lock()
            .map_err(|_| std::io::Error::other("checkpoint listener lock is poisoned"))?;
        let listener = self.inner.listener.try_clone()?;
        listener.set_nonblocking(true)?;
        let listener = UnixListener::from_std(listener)?;
        let generation = self.inner.generation.load(Ordering::Relaxed) + 1;
        self.inner.generation.store(generation, Ordering::Release);
        let _ = self.inner.ready_tx.send(super::RuntimeReadyState::Ready);
        let inner = Arc::downgrade(&self.inner);
        let task = tokio::spawn(async move {
            if let Err(error) = serve_checkpoint_listener(listener, inner.clone()).await {
                if let Some(inner) = inner.upgrade() {
                    if inner.generation.load(Ordering::Acquire) == generation {
                        let _ = inner
                            .ready_tx
                            .send(super::RuntimeReadyState::Failed(format!(
                                "checkpoint listener generation {generation} stopped: {error}"
                            )));
                    }
                }
            }
        });
        if let Some(previous) = accept_task.replace(task) {
            previous.abort();
        }
        Ok(generation)
    }
}

pub(crate) async fn bind_checkpoint_socket(path: &Path) -> std::io::Result<UnixListener> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let listener = UnixListener::bind(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o666))?;
    }
    Ok(listener)
}

async fn serve_checkpoint_listener(
    listener: UnixListener,
    inner: std::sync::Weak<CheckpointServerControlInner>,
) -> std::io::Result<()> {
    loop {
        let (mut stream, _) = listener.accept().await?;
        let Some(inner) = inner.upgrade() else {
            return Ok(());
        };
        let instance_id = inner
            .instance_id
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let tx = inner.tx.clone();
        let coordinator = inner.coordinator.clone();
        tokio::spawn(async move {
            let _ = handle_checkpoint_connection(&mut stream, &instance_id, tx, coordinator).await;
        });
    }
}

async fn handle_checkpoint_connection(
    stream: &mut UnixStream,
    instance_id: &str,
    tx: mpsc::Sender<StreamingMessage>,
    coordinator: CheckpointRequestCoordinator,
) -> std::io::Result<()> {
    let mut request = Vec::with_capacity(512);
    let mut buffer = [0u8; 512];
    loop {
        let count = stream.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        request.extend_from_slice(&buffer[..count]);
        if find_subslice(&request, b"\r\n\r\n").is_some() {
            break;
        }
        if request.len() > 8192 {
            return write_checkpoint_response(stream, 431, r#"{"error":"headers too large"}"#)
                .await;
        }
    }
    let head = String::from_utf8_lossy(&request);
    let (method, path) = parse_request_line(&head);
    let response = if method != "POST" {
        CheckpointHttpResponse {
            status: 405,
            body: r#"{"error":"method not allowed"}"#.to_string(),
        }
    } else if request_path(&path) != "/checkpoint" {
        CheckpointHttpResponse {
            status: 404,
            body: r#"{"error":"not found"}"#.to_string(),
        }
    } else {
        invoke_checkpoint_handler(instance_id, tx, coordinator).await
    };
    write_checkpoint_response(stream, response.status, &response.body).await
}

async fn write_checkpoint_response(
    stream: &mut UnixStream,
    status: u16,
    body: &str,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        409 => "Conflict",
        500 => "Internal Server Error",
        404 => "Not Found",
        405 => "Method Not Allowed",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

#[cfg(test)]
mod tests {
    use super::{
        bind, checkpoint_socket_path_from_control_directory, handle_conn,
        invoke_checkpoint_handler, parse_content_length, parse_range_header, percent_decode,
        query_param, request_path, serve_listener, upload_part_path, upload_type,
        CheckpointRequestCoordinator, CheckpointServerControl,
    };
    use crate::posix::runtime_rpc::StreamingMessage;
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;
    use tokio::sync::{mpsc, watch};

    async fn read_http_response(stream: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
        let mut bytes = Vec::new();
        let mut byte = [0u8; 1];
        let header_end = loop {
            stream.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
            if bytes.ends_with(b"\r\n\r\n") {
                break bytes.len();
            }
        };
        let head = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
        let content_length = parse_content_length(&head);
        let mut body = vec![0u8; content_length];
        stream.read_exact(&mut body).await.unwrap();
        (head, body)
    }

    #[tokio::test]
    async fn json_responses_reuse_one_http_connection() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_conn(&mut stream, None).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();

        for _ in 0..2 {
            client
                .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
                .await
                .unwrap();
            let (head, body) = read_http_response(&mut client).await;
            assert!(head.starts_with("HTTP/1.1 200 OK\r\n"));
            assert!(head.contains("Connection: keep-alive\r\n"));
            assert_eq!(body, br#"{"status":"ok"}"#);
        }

        drop(client);
        server.await.unwrap();
    }

    use std::ffi::OsStr;
    use std::path::PathBuf;
    use std::process::Command;
    use std::time::Duration;
    use tokio::sync::oneshot;

    use crate::posix::runtime_rpc::streaming_message;
    use crate::startup::CheckpointOutcome;

    #[test]
    fn parses_binary_stream_request_targets() {
        let raw = "/upload?path=%2Ftmp%2Fdir%2Fblob.bin&ignored=1";
        assert_eq!(request_path(raw), "/upload");
        assert_eq!(
            query_param(raw, "path").and_then(|p| percent_decode(&p)),
            Some("/tmp/dir/blob.bin".to_string())
        );
        assert_eq!(upload_type(raw), "file");

        let raw = "/download?path=%2Ftmp%2Fdir&type=tar";
        assert_eq!(request_path(raw), "/download");
        assert_eq!(upload_type(raw), "tar");
    }

    #[test]
    fn upload_part_path_is_stable_and_sanitized() {
        let got = upload_part_path("/tmp/blob.bin", "abc/123");
        assert_eq!(
            got,
            std::path::PathBuf::from("/tmp/.blob.bin.adx-upload.abc-123.part")
        );
    }

    #[test]
    fn parses_http_range_header() {
        let head = "GET /download HTTP/1.1\r\nRange: bytes=5-9\r\n\r\n";
        assert_eq!(parse_range_header(head, 20), Some((5, 9)));
        let head = "GET /download HTTP/1.1\r\nRange: bytes=5-\r\n\r\n";
        assert_eq!(parse_range_header(head, 20), Some((5, 19)));
    }

    #[tokio::test]
    async fn checkpoint_pre_rpc_rejections_release_wait_and_reuse_one_handoff_reader() {
        const ISOLATED: &str = "ADX_RRT_REJECTED_HANDOFF_TEST_ISOLATED";
        if std::env::var_os(ISOLATED).is_none() {
            let status = Command::new(std::env::current_exe().unwrap())
                .arg("runtime::httpserver::tests::checkpoint_pre_rpc_rejections_release_wait_and_reuse_one_handoff_reader")
                .arg("--exact").arg("--test-threads=1").env(ISOLATED, "1")
                .status().unwrap();
            assert!(status.success());
            return;
        }
        use super::super::{handle_prepare_snap_on_stream, CheckpointHandoffFuture};
        use crate::posix::core_service::KillResponse;
        use std::io::Write;
        use std::os::unix::ffi::OsStrExt;
        use tokio_stream::wrappers::ReceiverStream;

        let directory = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(directory.path().join("checkpoint.sock")).unwrap();
        let (tx, _rx) = mpsc::channel(8);
        let (ready_tx, _ready_rx) = watch::channel(super::super::RuntimeReadyState::Ready);
        let control =
            CheckpointServerControl::start(listener, "sandbox".into(), tx, ready_tx).unwrap();
        let barrier_path = directory.path().join("handoff");
        let path = std::ffi::CString::new(barrier_path.as_os_str().as_bytes()).unwrap();
        // A real blocking descriptor models the backend barrier. Keeping its
        // writer open ensures rejected attempts receive no handoff event.
        assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
        let mut writer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&barrier_path)
            .unwrap();
        std::env::set_var("ADX_CHECKPOINT_HANDOFF_FILE", &barrier_path);
        let mut handoff: Option<CheckpointHandoffFuture> = None;
        let (in_tx, in_rx) = mpsc::channel(8);
        let mut inbound = ReceiverStream::new(in_rx);
        let (response_tx, mut response_rx) = mpsc::channel(8);
        let mut buffered = std::collections::VecDeque::new();

        for attempt in 0..3 {
            let request_id = format!("checkpoint-{attempt}");
            let completion = control.inner.coordinator.begin(request_id.clone()).unwrap();
            in_tx
                .send(Ok(StreamingMessage {
                    message_id: request_id,
                    body: Some(streaming_message::Body::KillRsp(KillResponse {
                        code: 80034,
                        message: "sandbox runtime does not advertise checkpoint/restore capability"
                            .into(),
                        checkpoint_not_started: true,
                        ..Default::default()
                    })),
                    ..Default::default()
                }))
                .await
                .unwrap();
            let keep_stream = tokio::time::timeout(
                Duration::from_secs(1),
                handle_prepare_snap_on_stream(
                    format!("prepare-{attempt}"),
                    &response_tx,
                    &mut inbound,
                    &mut buffered,
                    &mut handoff,
                    Some(&control),
                ),
            )
            .await
            .expect("pre-RPC failure must not wait for handoff");
            assert!(keep_stream);
            assert!(
                handoff.is_some(),
                "reuse the next backend checkpoint generation"
            );
            assert!(buffered.is_empty());
            assert!(completion
                .await
                .unwrap()
                .unwrap_err()
                .contains("does not advertise"));
            assert!(matches!(
                response_rx.recv().await.unwrap().body,
                Some(streaming_message::Body::PrepareSnapRsp(_))
            ));
        }

        let completion = control
            .inner
            .coordinator
            .begin("checkpoint-success".into())
            .unwrap();
        writer.write_all(b"resume").unwrap();
        drop(writer);
        assert!(tokio::time::timeout(
            Duration::from_secs(1),
            handle_prepare_snap_on_stream(
                "prepare-success".into(),
                &response_tx,
                &mut inbound,
                &mut buffered,
                &mut handoff,
                Some(&control),
            )
        )
        .await
        .unwrap());
        assert!(handoff.is_none());
        control.record_snap_started(0, String::new());
        assert_eq!(completion.await.unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn checkpoint_handoff_ignores_unrelated_and_uncertain_rejections() {
        use super::super::{handle_prepare_snap_on_stream, CheckpointHandoffFuture};
        use crate::posix::core_service::KillResponse;
        use tokio_stream::wrappers::ReceiverStream;

        let directory = tempfile::tempdir().unwrap();
        let listener = UnixListener::bind(directory.path().join("checkpoint.sock")).unwrap();
        let (tx, _rx) = mpsc::channel(8);
        let (ready_tx, _ready_rx) = watch::channel(super::super::RuntimeReadyState::Ready);
        let control =
            CheckpointServerControl::start(listener, "sandbox".into(), tx, ready_tx).unwrap();
        let mut completion = control
            .inner
            .coordinator
            .begin("checkpoint".into())
            .unwrap();
        let mut handoff: Option<CheckpointHandoffFuture> = Some(Box::pin(std::future::pending()));
        let (in_tx, in_rx) = mpsc::channel(8);
        let mut inbound = ReceiverStream::new(in_rx);
        for (id, not_started) in [("unrelated", true), ("checkpoint", false)] {
            in_tx
                .send(Ok(StreamingMessage {
                    message_id: id.into(),
                    body: Some(streaming_message::Body::KillRsp(KillResponse {
                        code: 80034,
                        checkpoint_not_started: not_started,
                        ..Default::default()
                    })),
                    ..Default::default()
                }))
                .await
                .unwrap();
        }
        let (response_tx, _response_rx) = mpsc::channel(8);
        let mut buffered = std::collections::VecDeque::new();
        assert!(tokio::time::timeout(
            Duration::from_millis(50),
            handle_prepare_snap_on_stream(
                "prepare".into(),
                &response_tx,
                &mut inbound,
                &mut buffered,
                &mut handoff,
                Some(&control),
            )
        )
        .await
        .is_err());
        assert_eq!(buffered.len(), 2);
        assert!(handoff.is_some());
        assert!(matches!(
            completion.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        control.record_handoff(CheckpointOutcome::Restore);
    }

    #[tokio::test]
    async fn checkpoint_endpoint_waits_for_handoff_and_snap_started() {
        const ISOLATED_ENV: &str = "ADX_RRT_CHECKPOINT_ACTIVITY_TEST_ISOLATED";
        if std::env::var_os(ISOLATED_ENV).is_none() {
            let status = Command::new(std::env::current_exe().expect("current test executable"))
                .arg(
                    "runtime::httpserver::tests::checkpoint_endpoint_waits_for_handoff_and_snap_started",
                )
                .arg("--exact")
                .arg("--test-threads=1")
                .env(ISOLATED_ENV, "1")
                .status()
                .expect("run isolated checkpoint activity test");
            assert!(status.success(), "isolated checkpoint activity test failed");
            return;
        }

        let baseline = super::super::activity::active_count();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let coordinator = CheckpointRequestCoordinator::default();

        let request = tokio::spawn(invoke_checkpoint_handler(
            "sandbox-a",
            tx,
            coordinator.clone(),
        ));

        let message = rx.recv().await.expect("checkpoint signal");
        assert_eq!(super::super::activity::active_count(), baseline + 1);
        assert!(
            !request.is_finished(),
            "HTTP completed before checkpoint handoff"
        );

        coordinator.record_handoff(CheckpointOutcome::Resume);
        tokio::task::yield_now().await;
        assert!(!request.is_finished(), "HTTP completed before SnapStarted");

        coordinator.record_snap_started(
            crate::posix::common::ErrorCode::ErrNone as i32,
            String::new(),
        );
        let response = tokio::time::timeout(std::time::Duration::from_secs(1), request)
            .await
            .expect("checkpoint HTTP completion timed out")
            .expect("checkpoint HTTP task");
        assert_eq!(super::super::activity::active_count(), baseline);

        assert_eq!(response.status, 200);
        assert_eq!(response.body, r#"{"status":"completed"}"#);
        let Some(streaming_message::Body::KillReq(kill)) = message.body else {
            panic!("unexpected checkpoint message body")
        };
        assert_eq!(kill.instance_id, "sandbox-a");
        assert_eq!(kill.signal, 24);
        assert!(!kill.request_id.is_empty());
    }

    #[test]
    fn checkpoint_completion_accepts_both_event_orders() {
        for snap_started_first in [false, true] {
            for proxy_ack_received in [false, true] {
                let coordinator = CheckpointRequestCoordinator::default();
                let mut completion = coordinator.begin("checkpoint-a".to_string()).unwrap();
                if proxy_ack_received {
                    coordinator.record_proxy_ack("checkpoint-a", 0, String::new());
                }
                assert!(matches!(
                    completion.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                if snap_started_first {
                    coordinator.record_snap_started(0, String::new());
                } else {
                    coordinator.record_handoff(CheckpointOutcome::Resume);
                }
                assert!(matches!(
                    completion.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                if snap_started_first {
                    coordinator.record_handoff(CheckpointOutcome::Resume);
                } else {
                    coordinator.record_snap_started(0, String::new());
                }
                assert_eq!(completion.try_recv(), Ok(Ok(())));

                let mut next = coordinator.begin("checkpoint-b".to_string()).unwrap();
                coordinator.record_proxy_ack("checkpoint-a", 1, "late reply".to_string());
                assert!(matches!(
                    next.try_recv(),
                    Err(oneshot::error::TryRecvError::Empty)
                ));
                coordinator.record_handoff(CheckpointOutcome::Resume);
                coordinator.record_snap_started(0, String::new());
                assert_eq!(next.try_recv(), Ok(Ok(())));
            }
        }
    }

    #[test]
    fn checkpoint_completion_preserves_errors() {
        for failure in ["proxy", "handoff", "snap_started"] {
            let coordinator = CheckpointRequestCoordinator::default();
            let mut completion = coordinator.begin("checkpoint-a".to_string()).unwrap();
            match failure {
                "proxy" => coordinator.record_proxy_ack("checkpoint-a", 1, "rejected".to_string()),
                "handoff" => coordinator.record_handoff(CheckpointOutcome::Error),
                "snap_started" => coordinator.record_snap_started(1, "rearm failed".to_string()),
                _ => unreachable!(),
            }
            assert!(matches!(completion.try_recv(), Ok(Err(_))), "{failure}");
            assert!(coordinator.begin("checkpoint-b".to_string()).is_ok());
        }
    }

    #[tokio::test]
    async fn restored_runtime_discards_source_checkpoint_request() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let coordinator = CheckpointRequestCoordinator::default();
        let source_request = tokio::spawn(invoke_checkpoint_handler(
            "sandbox-source",
            tx.clone(),
            coordinator.clone(),
        ));

        rx.recv().await.expect("source checkpoint signal");
        coordinator.record_handoff(CheckpointOutcome::Restore);
        let source_response = source_request.await.expect("source checkpoint HTTP task");
        assert_eq!(source_response.status, 500);
        coordinator.record_snap_started(
            crate::posix::common::ErrorCode::ErrNone as i32,
            "restored runtime SnapStarted completed".to_string(),
        );

        let restored_request = tokio::spawn(invoke_checkpoint_handler(
            "sandbox-restored",
            tx,
            coordinator.clone(),
        ));
        rx.recv().await.expect("restored checkpoint signal");
        coordinator.record_handoff(CheckpointOutcome::Resume);
        coordinator.record_snap_started(
            crate::posix::common::ErrorCode::ErrNone as i32,
            String::new(),
        );
        let restored_response =
            tokio::time::timeout(std::time::Duration::from_secs(1), restored_request)
                .await
                .expect("restored checkpoint HTTP completion timed out")
                .expect("restored checkpoint HTTP task");
        assert_eq!(restored_response.status, 200);
        assert_eq!(restored_response.body, r#"{"status":"completed"}"#);
    }

    #[test]
    fn checkpoint_socket_uses_configured_control_directory() {
        assert_eq!(
            checkpoint_socket_path_from_control_directory(Some(OsStr::new("/run/adx-control"))),
            Some(PathBuf::from("/run/adx-control/rrt.sock")),
        );
        assert_eq!(checkpoint_socket_path_from_control_directory(None), None);
        assert_eq!(
            checkpoint_socket_path_from_control_directory(Some(OsStr::new(""))),
            None,
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_poll_counts_as_request_activity() {
        const ISOLATED_ENV: &str = "ADX_RRT_HTTP_POLL_ACTIVITY_TEST_ISOLATED";
        if std::env::var_os(ISOLATED_ENV).is_none() {
            let status = Command::new(std::env::current_exe().expect("current test executable"))
                .arg("runtime::httpserver::tests::process_poll_counts_as_request_activity")
                .arg("--exact")
                .arg("--test-threads=1")
                .env(ISOLATED_ENV, "1")
                .status()
                .expect("run isolated process poll activity test");
            assert!(
                status.success(),
                "isolated process poll activity test failed"
            );
            return;
        }

        let baseline = super::super::activity::active_command_count();
        let mut start = std::collections::BTreeMap::new();
        start.insert(
            "command_id".to_string(),
            rmpv::Value::from("poll-activity-command"),
        );
        start.insert("cmd".to_string(), rmpv::Value::from("sleep 2"));
        let started = super::super::cmd::cmd_start(&start);
        let pid = started
            .as_map()
            .and_then(|entries| {
                entries.iter().find_map(|(key, value)| {
                    (key.as_str() == Some("pid"))
                        .then(|| value.as_i64())
                        .flatten()
                })
            })
            .expect("started process pid");
        assert_eq!(super::super::activity::active_command_count(), baseline + 1);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test HTTP listener");
        let address = listener.local_addr().expect("test listener address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept test request");
            super::handle_conn(&mut stream, None)
                .await
                .expect("serve process.poll request");
        });

        let body = format!(
            r#"{{"action":"process.poll","args":{{"pid":{pid},"wait_timeout":1}},"requestId":"poll-activity-test"}}"#
        );
        let request = format!(
            "POST /invoke HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let mut client = tokio::net::TcpStream::connect(address)
            .await
            .expect("connect test HTTP listener");
        tokio::io::AsyncWriteExt::write_all(&mut client, request.as_bytes())
            .await
            .expect("send process.poll request");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert_eq!(
            super::super::activity::active_command_count(),
            baseline + 1,
            "process.poll observes the launched process and must not add another busy unit"
        );

        let (response_head, _) = read_http_response(&mut client).await;
        drop(client);
        server.await.expect("process.poll server task");
        assert!(response_head.starts_with("HTTP/1.1 200"));
    }

    #[tokio::test]
    async fn command_watch_returns_initial_and_terminal_state() {
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = serve_listener(listener, None).await;
        });
        let command_id = format!(
            "cmd-watch-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let args = std::collections::BTreeMap::from([
            (
                "command_id".to_string(),
                rmpv::Value::from(command_id.clone()),
            ),
            ("command".to_string(), rmpv::Value::from("sleep 0.1")),
        ]);
        super::super::cmd::cmd_start(&args);

        let (mut websocket, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/commands/watch"))
                .await
                .unwrap();
        websocket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                serde_json::json!({"op": "subscribe", "commandIds": [command_id.clone()]})
                    .to_string(),
            ))
            .await
            .unwrap();
        let mut observed_terminal = false;
        for _ in 0..5 {
            let message = tokio::time::timeout(std::time::Duration::from_secs(1), websocket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let value: serde_json::Value =
                serde_json::from_str(message.to_text().unwrap()).unwrap();
            if value["status"] == "SUCCEEDED" {
                observed_terminal = true;
                break;
            }
        }
        assert!(observed_terminal);

        websocket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                serde_json::json!({"op": "subscribe", "commandIds": [command_id.clone()]})
                    .to_string(),
            ))
            .await
            .unwrap();
        let replay = tokio::time::timeout(std::time::Duration::from_secs(1), websocket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let replay: serde_json::Value = serde_json::from_str(replay.to_text().unwrap()).unwrap();
        assert_eq!(replay["status"], "SUCCEEDED");
        server.abort();
    }
}
