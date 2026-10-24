// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0.
// See the LICENSE file in this repository for the complete license text.

//! EXECD atomic-operation HTTP/1.1 server.
//!
//! Purpose: expose environment-embedded EXECD atomic operations over HTTP through environmentRouter as an L7 reverse proxy,
//! serving atomic operations directly over HTTP.
//!
//! Protocol:
//! - `POST /invoke`, body = `{"action": "...", "args": {...}}`, shaped like environment_invoke,
//!   reuse dispatch::normalize_environment_action + dispatch_runtime_action and return action result JSON.
//! - `POST /upload?path=/abs/file&type=file|tar`, body is raw binary or a tar stream.
//! - `GET /download?path=/abs/file&type=file|tar`, response body is raw binary or a tar stream.
//! - `GET /pty?command=...`, WebSocket carrying PTY bytes and lifecycle frames.
//! - `GET /healthz` → `{"status":"ok"}`。
//!
//! Auth model: EXECD is privileged, so token auth is required when EXECD_HTTP_TOKEN is set. Requests must carry
//! `X-Auth: <token>` or they receive 401. The deployment supplies the token.
//!
//! No new dependencies: use raw tokio TcpListener plus handwritten HTTP/1.1.
//! JSON/control responses support sequential keep-alive requests so the Ingress
//! backend pool can reuse a connection through the Ingress proxy. Streaming responses
//! that require EOF framing still advertise `Connection: close`.

use futures_util::{SinkExt, StreamExt};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::watch;

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

pub(crate) async fn bind(port: u16) -> std::io::Result<TcpListener> {
    TcpListener::bind(("0.0.0.0", port)).await
}

#[derive(Clone, Debug)]
pub(crate) struct HttpServerControl {
    inner: Arc<HttpServerControlInner>,
}

#[derive(Debug)]
struct HttpServerControlInner {
    // Restore binds a fresh kernel socket after the previous accept task exits.
    // Cloning a checkpoint-restored descriptor retains its backlog and readiness
    // state, even when the clone gets a new Tokio registration.
    address: std::net::SocketAddr,
    token: RwLock<Option<String>>,
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
        let address = listener.local_addr()?;
        let control = Self {
            inner: Arc::new(HttpServerControlInner {
                address,
                token: RwLock::new(token),
                accept_task: Mutex::new(None),
                generation: AtomicU64::new(0),
                ready_tx,
            }),
        };
        control.install(Some(listener))?;
        Ok(control)
    }

    pub(crate) fn update_token(&self, token: String) -> std::io::Result<()> {
        *self
            .inner
            .token
            .write()
            .map_err(|_| std::io::Error::other("token lock poisoned"))? = Some(token);
        Ok(())
    }

    pub(crate) fn rearm(&self) -> std::io::Result<u64> {
        self.install(None)
    }

    fn install(&self, listener: Option<TcpListener>) -> std::io::Result<u64> {
        let mut accept_task =
            self.inner.accept_task.lock().map_err(|_| {
                std::io::Error::other("EXECD HTTP listener control lock is poisoned")
            })?;
        let generation = self
            .inner
            .generation
            .load(Ordering::Relaxed)
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("EXECD HTTP listener generation exhausted"))?;
        let token = self
            .inner
            .token
            .read()
            .map_err(|_| std::io::Error::other("token lock poisoned"))?
            .clone();
        let previous = accept_task.take();
        if let Some(previous) = &previous {
            previous.abort();
        }
        self.inner.generation.store(generation, Ordering::Release);
        self.inner
            .ready_tx
            .send_replace(super::RuntimeReadyState::Starting);
        let address = self.inner.address;
        let inner = Arc::downgrade(&self.inner);
        *accept_task = Some(tokio::spawn(async move {
            let result = async {
                // Await cancellation so the inherited listener is closed before
                // binding the same address. Pending source connections are retired
                // with the socket instead of surviving through a duplicated FD.
                if let Some(previous) = previous {
                    if let Err(error) = previous.await {
                        if !error.is_cancelled() {
                            return Err(std::io::Error::other(error).into());
                        }
                    }
                }
                let listener = match listener {
                    Some(listener) => listener,
                    None => TcpListener::bind(address).await?,
                };
                if let Some(inner) = inner.upgrade() {
                    if inner.generation.load(Ordering::Acquire) == generation {
                        inner.ready_tx.send_replace(super::RuntimeReadyState::Ready);
                    }
                }
                execd_info!("[execd-http] listener generation installed generation={generation}");
                serve_listener(listener, token).await
            }
            .await;
            if let Err(error) = result {
                if let Some(inner) = inner.upgrade() {
                    if inner.generation.load(Ordering::Acquire) == generation {
                        let message =
                            format!("EXECD HTTP accept generation {generation} stopped: {error}");
                        execd_error!("[execd-http] {message}");
                        inner
                            .ready_tx
                            .send_replace(super::RuntimeReadyState::Failed(message));
                    }
                }
            }
        }));
        Ok(generation)
    }
}

pub(crate) async fn serve_listener(
    listener: TcpListener,
    token: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let address = listener.local_addr()?;
    execd_info!("[execd-http] atomic-ops server listening on {address}");
    let mut connections = tokio::task::JoinSet::new();
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = connections.join_next(), if !connections.is_empty() => continue,
        };
        let (mut sock, _peer) = accepted?;
        let token = token.clone();
        connections.spawn(async move {
            if let Err(error) = handle_conn(&mut sock, token).await {
                execd_error!("[execd-http] conn error: {error}");
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
        // consuming its first byte. The Ingress proxy does not pipeline requests on
        // this connection, so one iteration owns one complete exchange.
        let mut first = [0u8; 1];
        if sock.peek(&mut first).await? == 0 {
            return Ok(());
        }
        let mut close_after_response = false;
        handle_one_request(sock, token.clone(), &mut close_after_response).await?;
        if close_after_response {
            return Ok(());
        }
    }
}

async fn handle_one_request(
    sock: &mut tokio::net::TcpStream,
    token: Option<String>,
    close_after_response: &mut bool,
) -> Result<(), Box<dyn std::error::Error>> {
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
    let (trace_method, trace_path) = parse_request_line(&head);
    let parent = parse_header(&head, "traceparent");
    let passive = trace_method == "GET"
        && matches!(
            request_path(&trace_path),
            "/healthz" | "/metrics" | "/control/v1/status"
        )
        && parent.is_none();
    // Ordinary observations are silent; explicit diagnostic parents and all
    // mutations retain their HTTP context and lifecycle spans.
    let trace = (!passive).then(|| {
        adx_observability::trace::Trace::http(
            &trace_method,
            adx_observability::trace::http_route(&trace_path),
            parent.as_deref(),
            parse_header(&head, "tracestate").as_deref(),
        )
    });
    let request = async {
        let (method, path) = parse_request_line(&head);
        adx_observability::trace::attribute(
            "environment.id",
            std::env::var("ADX_ENVIRONMENT_ID").unwrap_or_default(),
        );
        let content_len = parse_content_length(&head);
        let body_mode = if header_has_token(&head, "transfer-encoding", "chunked") {
            RequestBody::Chunked
        } else {
            RequestBody::ContentLength(content_len)
        };
        let auth = parse_header(&head, "x-auth");
        let trace_id = parse_header(&head, "x-trace-id").unwrap_or_default();
        let route = request_path(&path);
        execd_info!(
        "[execd-http] request method={} path={} body_mode={} content_len={} initial_body={} trace={}",
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
            if super::control::current().is_some_and(|controller| {
                controller.status().phase != adx_core::runtime::RuntimePhase::Running
            }) {
                return write_resp(sock, 503, "{\"status\":\"unavailable\"}").await;
            }
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
        // Auth: EXECD requires token authentication. /invoke, /upload, and /download are all control-plane capabilities.
        if let Some(expect) = token.as_deref() {
            if auth.as_deref() != Some(expect) {
                return write_resp(sock, 401, "{\"error\":\"unauthorized\"}").await;
            }
        }

        if route.starts_with("/control/v1/") {
            if content_len > 65536 || header_has_token(&head, "transfer-encoding", "chunked") {
                return write_resp(
                    sock,
                    400,
                    "{\"error\":\"control body requires bounded content-length\"}",
                )
                .await;
            }
            let mut body = buf[header_end..].to_vec();
            while body.len() < content_len {
                let n = sock.read(&mut tmp).await?;
                if n == 0 {
                    return Ok(());
                }
                body.extend_from_slice(&tmp[..n]);
            }
            let response = control_response(&method, route, &body[..content_len]).await;
            let checkpoint_control = route.starts_with("/control/v1/checkpoint/");
            if checkpoint_control {
                execd_info!(
                    "[checkpoint-control] http_response_ready path={} status={}",
                    route,
                    response.status
                );
            }
            let result = write_resp(sock, response.status, &response.body).await;
            if checkpoint_control {
                execd_info!(
                    "[checkpoint-control] http_response_written path={} status={} success={}",
                    route,
                    response.status,
                    result.is_ok()
                );
            }
            return result;
        }
        if super::control::current().is_some_and(|controller| {
            controller.status().phase != adx_core::runtime::RuntimePhase::Running
        }) {
            return write_resp(
                sock,
                503,
                "{\"error\":\"runtime checkpoint transition in progress\"}",
            )
            .await;
        }

        if method == "GET"
            && route == "/commands/watch"
            && header_has_token(&head, "upgrade", "websocket")
        {
            // An upgraded stream owns this connection until it ends. Returning to
            // the HTTP keep-alive loop would keep TCP open after its Close frame.
            *close_after_response = true;
            return handle_command_watch(sock, &head).await;
        }
        if method == "GET" && route == "/pty" && header_has_token(&head, "upgrade", "websocket") {
            *close_after_response = true;
            return handle_pty(sock, &head, &path).await;
        }

        // Ordinary atomic operations are data activity. The long-lived command
        // watch above is deliberately a passive observer and must not prevent idle.
        let _active = match super::activity::try_enter(super::activity::ActivitySource::DirectHttp)
        {
            Ok(active) => active,
            Err(_) => return write_resp(sock, 503, "{\"error\":\"runtime is quiescing\"}").await,
        };

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
            // Binary file responses advertise Connection: close. Tar downloads
            // and ordinary error responses remain reusable.
            *close_after_response =
                handle_download(sock, &path, &head, &mut tmp, trace_id.as_str()).await?;
            return Ok(());
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
        if let Some(operation) = super::dispatch::normalize_environment_action(&action) {
            adx_observability::trace::attribute("rpc.method", operation.to_owned());
        }
        let kw = json_args_to_kwargs(parsed.get("args"));
        let request_id = request_id_from(&head, &parsed);

        let resp = execute_invoke(request_id, action, kw, trace_id).await;
        write_resp(sock, resp.status, &resp.body).await
    };
    match trace {
        Some(trace) => trace.run_result(request).await,
        None => request.await,
    }
}

async fn control_response(method: &str, path: &str, body: &[u8]) -> CachedResponse {
    use adx_core::{
        runtime::{AbortCheckpoint, FinishWorkloadCheckpoint, PrepareCheckpoint},
        Error,
    };
    let Some(controller) = super::control::current() else {
        return CachedResponse {
            status: 503,
            body: err_json("runtime identity is not configured"),
        };
    };
    let result = match (method, path) {
        ("GET", "/control/v1/status") => Ok(controller.status()),
        ("POST", "/control/v1/checkpoint/finish") => {
            match serde_json::from_slice::<FinishWorkloadCheckpoint>(body) {
                Ok(request) => controller.finish_checkpoint(request),
                Err(error) => Err(Error::Invalid(error.to_string())),
            }
        }
        ("POST", "/control/v1/checkpoint/prepare") => {
            match serde_json::from_slice::<PrepareCheckpoint>(body) {
                Ok(request) => controller.prepare(request).await,
                Err(error) => Err(Error::Invalid(error.to_string())),
            }
        }
        ("POST", "/control/v1/checkpoint/abort-unstarted") => {
            match serde_json::from_slice::<AbortCheckpoint>(body) {
                Ok(request) => controller.abort_unstarted(
                    &request.operation_id,
                    &request.identity,
                    request.expected_revision,
                ),
                Err(error) => Err(Error::Invalid(error.to_string())),
            }
        }
        _ => {
            return CachedResponse {
                status: 404,
                body: err_json("unknown control operation"),
            }
        }
    };
    match result {
        Ok(status) => CachedResponse {
            status: 200,
            body: serde_json::to_string(&status).expect("serializable status"),
        },
        Err(error) => CachedResponse {
            status: if matches!(error, Error::Conflict) {
                409
            } else if matches!(error, Error::Invalid(_)) {
                400
            } else {
                503
            },
            body: err_json(&error.to_string()),
        },
    }
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
    let max_subscriptions = std::env::var("EXECD_COMMAND_WATCH_MAX_SUBSCRIPTIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4096);
    let max_frame_bytes = std::env::var("EXECD_COMMAND_WATCH_MAX_FRAME_BYTES")
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
                                    // same command for another Ingress consumer.
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

async fn handle_pty(
    sock: &mut tokio::net::TcpStream,
    head: &str,
    raw_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let _active = match super::activity::try_enter(super::activity::ActivitySource::DirectHttp) {
        Ok(active) => active,
        Err(_) => return write_resp(sock, 503, "{\"error\":\"runtime is quiescing\"}").await,
    };
    let command: Vec<String> = query_params(raw_path, "command")
        .into_iter()
        .filter_map(|value| percent_decode(&value))
        .collect();
    if command.is_empty()
        || command.len() > 256
        || command
            .iter()
            .any(|argument| argument.is_empty() || argument.len() > 64 * 1024)
    {
        return write_resp(sock, 400, &err_json("invalid PTY command")).await;
    }
    let dimension = |name: &str, default: u16| {
        query_param(raw_path, name)
            .and_then(|value| value.parse::<u16>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(default)
    };
    let rows = dimension("rows", 24);
    let cols = dimension("cols", 80);
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

    let pty = portable_pty::native_pty_system();
    let pair = pty.openpty(portable_pty::PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    })?;
    let mut process = portable_pty::CommandBuilder::new(&command[0]);
    for argument in &command[1..] {
        process.arg(argument);
    }
    super::child_env::apply_pty(&mut process);
    let mut child = pair.slave.spawn_command(process)?;
    let mut killer = child.clone_killer();
    let mut writer = Some(pair.master.take_writer()?);
    let mut reader = pair.master.try_clone_reader()?;
    let master = pair.master;
    let (output_tx, mut output_rx) = tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(move || {
        let mut buffer = vec![0u8; 16 * 1024];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(size) if output_tx.send(buffer[..size].to_vec()).is_err() => break,
                Ok(_) => {}
            }
        }
    });
    static NEXT_PTY: AtomicU64 = AtomicU64::new(1);
    let session_id = format!(
        "pty-{}-{}",
        std::process::id(),
        NEXT_PTY.fetch_add(1, Ordering::Relaxed)
    );
    websocket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::json!({"version":1,"type":"started","session_id":session_id}).to_string(),
        ))
        .await?;
    let exit = tokio::task::spawn_blocking(move || child.wait());
    tokio::pin!(exit);
    loop {
        tokio::select! {
            message = websocket.next() => {
                let Some(message) = message else { let _ = killer.kill(); break; };
                match message? {
                    tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
                        if let Some(writer) = writer.as_mut() {
                            writer.write_all(&bytes)?;
                            writer.flush()?;
                        }
                    }
                    tokio_tungstenite::tungstenite::Message::Text(text) if text == "STDIN_EOF" => {
                        writer.take();
                    }
                    tokio_tungstenite::tungstenite::Message::Text(text) if text.starts_with("RESIZE:") => {
                        let values: Vec<_> = text.split(':').collect();
                        if values.len() == 3 {
                            if let (Ok(cols), Ok(rows)) = (values[1].parse::<u16>(), values[2].parse::<u16>()) {
                                if cols > 0 && rows > 0 {
                                    master.resize(portable_pty::PtySize { rows, cols, pixel_width: 0, pixel_height: 0 })?;
                                }
                            }
                        }
                    }
                    tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                        websocket.send(tokio_tungstenite::tungstenite::Message::Pong(payload)).await?;
                    }
                    tokio_tungstenite::tungstenite::Message::Close(_) => {
                        let _ = killer.kill();
                        websocket.flush().await?;
                        break;
                    }
                    _ => {}
                }
            }
            Some(output) = output_rx.recv() => {
                websocket.send(tokio_tungstenite::tungstenite::Message::Binary(output)).await?;
            }
            status = &mut exit => {
                let status = status??;
                while let Ok(Some(output)) = tokio::time::timeout(
                        std::time::Duration::from_millis(250),
                        output_rx.recv(),
                    )
                    .await
                {
                    websocket.send(tokio_tungstenite::tungstenite::Message::Binary(output)).await?;
                }
                websocket.send(tokio_tungstenite::tungstenite::Message::Text(
                    serde_json::json!({"version":1,"type":"exited","session_id":session_id,"exit_code":status.exit_code()}).to_string()
                )).await?;
                websocket.close(None).await?;
                // A short-lived child can exit before the SDK's initial resize
                // reaches this task. Dropping TCP with that input unread sends
                // RST, which can discard the terminal frames through a relay.
                // Read through the peer's Close reply before releasing TCP. The
                // terminal status is already sent; an absent/disconnected peer
                // must not keep this completed session alive indefinitely.
                let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    while let Some(Ok(message)) = websocket.next().await {
                        if matches!(message, tokio_tungstenite::tungstenite::Message::Close(_)) {
                            break;
                        }
                    }
                }).await;
                break;
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
    let normalized_action = super::dispatch::normalize_environment_action(&action);
    let result = tokio::task::spawn_blocking(move || {
        super::dispatch::execute_environment_action_once(
            request_id.as_deref(),
            &action,
            &kw,
            &trace_id,
        )
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
                    "COMMAND_EXPIRED" => 410,
                    "COMMAND_CONFLICT" => 409,
                    "DEADLINE_EXCEEDED" => 408,
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
    execd_info!(
        "[execd-http] upload start type={} path={} body_mode={} content_len={} initial_body={} trace={}",
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

#[allow(clippy::too_many_arguments)] // Streaming upload passes borrowed buffers without copying.
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
    execd_info!(
        "[execd-http] upload type=file path={} bytes={} body_mode={} content_len={} initial_body={} open_ms={} copy_ms={} read_ms={} write_ms={} flush_ms={} reads={} writes={} first_byte_ms={} total_ms={} trace={}",
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

    let name = Path::new(path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    let body = serde_json::json!({
        "error": null,
        "name": name,
        "path": path,
        "type": "file",
        "size": stats.bytes,
        "bytes_written": stats.bytes,
    })
    .to_string();
    let resp_started = Instant::now();
    let r = write_resp(sock, 200, &body).await;
    execd_info!(
        "[execd-http] upload response type=file path={} resp_ms={} trace={}",
        path,
        resp_started.elapsed().as_millis(),
        trace_id
    );
    r
}

#[allow(clippy::too_many_arguments)] // Shares the streaming upload request context.
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
    execd_info!(
        "[execd-http] upload chunk path={} upload_id={} offset={} bytes={} new_offset={} total_ms={} trace={}",
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
    execd_info!(
        "[execd-http] upload type=tar path={} bytes={} body_mode={} content_len={} initial_body={} copy_ms={} read_ms={} write_ms={} stdin_shutdown_ms={} tar_wait_ms={} reads={} writes={} first_byte_ms={} total_ms={} trace={}",
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
    execd_info!(
        "[execd-http] upload response type=tar path={} resp_ms={} trace={}",
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
) -> Result<bool, Box<dyn std::error::Error>> {
    let path = match query_param(raw_path, "path").and_then(|p| percent_decode(&p)) {
        Some(p) if !p.is_empty() => p,
        _ => {
            return write_resp(sock, 400, "{\"error\":\"missing path\"}")
                .await
                .map(|_| false)
        }
    };
    match upload_type(raw_path).as_str() {
        "tar" => handle_tar_download(sock, &path, tmp, trace_id)
            .await
            .map(|_| false),
        "file" | "" => handle_file_download(sock, &path, head, tmp, trace_id).await,
        other => write_resp(
            sock,
            400,
            &err_json(&format!("unsupported download type: {other}")),
        )
        .await
        .map(|_| false),
    }
}

#[cfg(target_os = "linux")]
async fn open_regular_download(path: &str) -> std::io::Result<tokio::fs::File> {
    use std::os::fd::{AsRawFd, FromRawFd};

    // O_PATH pins the inode without opening a FIFO reader or touching a device.
    // Reopen that inode only after checking its type, even if the pathname changes.
    let path = std::ffi::CString::new(path)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let file = tokio::task::spawn_blocking(move || {
        // musl includes O_PATH in O_ACCMODE, which Rust's OpenOptions masks out
        // of custom_flags. Call open directly to preserve the path-only access.
        // SAFETY: path is NUL-terminated and valid throughout this call; the
        // flags do not create a file, so open does not need a variadic mode.
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_PATH | libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: open returned a valid, uniquely owned descriptor. File closes
        // it on every path, including metadata errors and failed reopen calls.
        let pinned = unsafe { std::fs::File::from_raw_fd(fd) };
        if !pinned.metadata()?.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "file not found",
            ));
        }
        std::fs::File::open(format!("/proc/self/fd/{}", pinned.as_raw_fd()))
    })
    .await
    .map_err(std::io::Error::other)??;
    Ok(tokio::fs::File::from_std(file))
}

#[cfg(not(target_os = "linux"))]
async fn open_regular_download(path: &str) -> std::io::Result<tokio::fs::File> {
    // Reject known special files before opening them on development hosts too.
    if !tokio::fs::metadata(path).await?.is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file not found",
        ));
    }
    let mut options = tokio::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NONBLOCK);
    options.open(path).await
}

async fn handle_file_download(
    sock: &mut tokio::net::TcpStream,
    path: &str,
    head: &str,
    tmp: &mut [u8],
    trace_id: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let started = Instant::now();
    let mut file = match open_regular_download(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return write_resp(sock, 404, &err_json("file not found"))
                .await
                .map(|_| false)
        }
        Err(error) => return Err(error.into()),
    };
    // Use the same descriptor for metadata and bytes, including when another
    // process replaces the pathname after open.
    let meta = file.metadata().await?;
    if !meta.is_file() {
        return write_resp(sock, 404, &err_json("file not found"))
            .await
            .map(|_| false);
    }
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
        // Full reads may reach EOF early after an in-place truncate. Let EOF
        // finish the chunked response instead of advertising a stale file length.
        // Range responses retain their fixed-length resume contract.
        range.map(|_| content_len),
        range.map(|_| (start, end, total)),
    )
    .await?;
    if start > 0 {
        file.seek(SeekFrom::Start(start)).await?;
    }
    let mut bytes_sent = 0u64;
    // Bound growth during a concurrent append to the initial extent.
    let mut remaining = content_len;
    while remaining > 0 {
        let limit = remaining.min(tmp.len() as u64) as usize;
        let n = file.read(&mut tmp[..limit]).await?;
        if n == 0 {
            break;
        }
        if range.is_some() {
            sock.write_all(&tmp[..n]).await?;
        } else {
            write_chunk(sock, &tmp[..n]).await?;
        }
        bytes_sent += n as u64;
        remaining -= n as u64;
    }
    if range.is_none() {
        // A file EOF is a valid completion, even after concurrent truncation.
        // I/O failures return above without this terminator, so clients can
        // distinguish an interrupted transfer and retry/resume it.
        write_chunk(sock, &[]).await?;
    }
    sock.flush().await?;
    execd_info!(
        "[execd-http] download type=file bytes={} range_start={} total_ms={} trace={}",
        bytes_sent,
        start,
        started.elapsed().as_millis(),
        trace_id
    );
    Ok(true)
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
    write_chunked_headers(sock, 200, "application/x-tar").await?;
    let mut bytes_sent = 0u64;
    loop {
        let n = stdout.read(tmp).await?;
        if n == 0 {
            break;
        }
        write_chunk(sock, &tmp[..n]).await?;
        bytes_sent += n as u64;
    }
    sock.write_all(b"0\r\n\r\n").await?;
    sock.flush().await?;
    let status = child.wait().await?;
    if !status.success() {
        execd_error!("[execd-http] tar create failed path={path} status={status}");
    }
    execd_info!(
        "[execd-http] download type=tar bytes={} total_ms={} trace={}",
        bytes_sent,
        started.elapsed().as_millis(),
        trace_id
    );
    Ok(())
}

async fn write_chunked_headers(
    sock: &mut tokio::net::TcpStream,
    status: u16,
    content_type: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let resp = format!(
        "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n"
    );
    sock.write_all(resp.as_bytes()).await?;
    Ok(())
}

async fn write_chunk(
    sock: &mut tokio::net::TcpStream,
    bytes: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    sock.write_all(format!("{:X}\r\n", bytes.len()).as_bytes())
        .await?;
    sock.write_all(bytes).await?;
    sock.write_all(b"\r\n").await?;
    Ok(())
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
        .unwrap_or_else(|| "Transfer-Encoding: chunked\r\n".to_string());
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
        410 => "Gone",
        429 => "Too Many Requests",
        501 => "Not Implemented",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        _ => "Error",
    };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n{body}",
        body.len()
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

fn query_params(raw_path: &str, name: &str) -> Vec<String> {
    raw_path
        .split_once('?')
        .map(|(_, query)| {
            query
                .split('&')
                .filter_map(|pair| {
                    let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                    (key == name).then(|| value.to_string())
                })
                .collect()
        })
        .unwrap_or_default()
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

#[cfg(test)]
mod trace_tests;

#[cfg(test)]
mod tests {
    use super::{
        bind, handle_conn, parse_content_length, parse_range_header, percent_decode, query_param,
        request_path, serve_listener, upload_part_path, upload_type,
    };
    use futures_util::{SinkExt, StreamExt};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn restore_discards_inherited_listener_backlog() {
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        // Establish a source-generation connection before any accept task runs.
        let mut inherited = tokio::net::TcpStream::connect(address).await.unwrap();
        let (ready, mut receiver) =
            tokio::sync::watch::channel(crate::runtime::RuntimeReadyState::Starting);
        let control = super::HttpServerControl::start(listener, None, ready).unwrap();
        control.rearm().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if *receiver.borrow_and_update() == crate::runtime::RuntimeReadyState::Ready {
                    break;
                }
                receiver.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        // A restored listener must not accept a connection inherited from the
        // checkpoint's backlog, even when its request arrives after restore.
        let request = b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        if inherited.write_all(request).await.is_ok() {
            let mut response = [0; 256];
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                inherited.read(&mut response),
            )
            .await
            .unwrap();
            assert!(
                matches!(result, Ok(0) | Err(_)),
                "inherited connection was served: {result:?}"
            );
        }
        let mut fresh = tokio::net::TcpStream::connect(address).await.unwrap();
        fresh.write_all(request).await.unwrap();
        let (head, _) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            read_http_response(&mut fresh),
        )
        .await
        .unwrap();
        assert!(head.starts_with("HTTP/1.1 200"));
    }

    async fn wait_for_file_contents(path: &std::path::Path, expected: &[u8]) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while std::fs::read(path).unwrap() != expected {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("unexpected file contents: {:?}", std::fs::read(path)));
    }

    async fn read_http_head(stream: &mut tokio::net::TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            stream.read_exact(&mut byte).await.unwrap();
            bytes.push(byte[0]);
            if bytes.ends_with(b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8(bytes).unwrap()
    }

    async fn read_http_response(stream: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
        let head = read_http_head(stream).await;
        if head.contains("Transfer-Encoding: chunked\r\n") {
            return (head, read_chunked_body(stream).await);
        }
        let content_length = parse_content_length(&head);
        let mut body = vec![0u8; content_length];
        stream.read_exact(&mut body).await.unwrap();
        (head, body)
    }

    async fn read_chunked_response(stream: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
        let head = read_http_head(stream).await;
        (head, read_chunked_body(stream).await)
    }

    async fn read_chunked_body(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut body = Vec::new();
        loop {
            let mut line = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                stream.read_exact(&mut byte).await.unwrap();
                line.push(byte[0]);
                if line.ends_with(b"\r\n") {
                    break;
                }
            }
            let size =
                usize::from_str_radix(std::str::from_utf8(&line[..line.len() - 2]).unwrap(), 16)
                    .unwrap();
            if size == 0 {
                stream.read_exact(&mut [0u8; 2]).await.unwrap();
                break;
            }
            let offset = body.len();
            body.resize(offset + size, 0);
            stream.read_exact(&mut body[offset..]).await.unwrap();
            let mut delimiter = [0u8; 2];
            stream.read_exact(&mut delimiter).await.unwrap();
            assert_eq!(&delimiter, b"\r\n");
        }
        body
    }

    async fn invoke_over_http(action: &str, args: serde_json::Value) -> serde_json::Value {
        invoke_over_http_status(action, args, 200).await
    }

    async fn invoke_over_http_status(
        action: &str,
        args: serde_json::Value,
        status: u16,
    ) -> serde_json::Value {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            handle_conn(&mut socket, Some("test-token".into()))
                .await
                .unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let body = serde_json::json!({"action": action, "args": args}).to_string();
        let request = format!("POST /invoke HTTP/1.1\r\nHost: localhost\r\nX-Auth: test-token\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}", body.len(), body);
        client.write_all(request.as_bytes()).await.unwrap();
        let (head, body) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            read_http_response(&mut client),
        )
        .await
        .unwrap();
        assert!(
            head.starts_with(&format!("HTTP/1.1 {status}")),
            "{head}: {}",
            String::from_utf8_lossy(&body)
        );
        drop(client);
        server.await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn http_command_returns_actual_stdout_stderr_and_exit_status() {
        let response = invoke_over_http(
            "process.exec",
            serde_json::json!({"command":"printf hello; printf error >&2; exit 7"}),
        )
        .await;
        assert_eq!(response["stdout"], "hello");
        assert_eq!(response["stderr"], "error");
        assert_eq!(response["exit_code"], 7);
    }

    #[tokio::test]
    async fn http_filesystem_roundtrip_preserves_binary_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sub/file.bin");
        let response = invoke_over_http(
            "file.write",
            serde_json::json!({"path":path,"binary":true,"data":"000aff80"}),
        )
        .await;
        assert!(response["error"].is_null(), "{response}");
        assert_eq!(std::fs::read(&path).unwrap(), vec![0, 10, 255, 128]);
        let response =
            invoke_over_http("file.read", serde_json::json!({"path":path,"binary":true})).await;
        assert_eq!(response["data"], "000aff80");
    }

    #[tokio::test]
    async fn http_native_file_search_matches_names_paths_and_exclusions() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        std::fs::create_dir_all(root.join("sub/deep")).unwrap();
        for name in [
            "a.py",
            "secret.py",
            "case.PY",
            "sub/b.py",
            "sub/deep/中文.py",
            "content.txt",
        ] {
            std::fs::write(root.join(name), b"needle-in-content").unwrap();
        }
        #[cfg(unix)]
        std::os::unix::fs::symlink(root, root.join("sub/loop")).unwrap();
        let result = invoke_over_http(
            "file.search",
            serde_json::json!({
                "path":root,"pattern":"*.py","exclude_patterns":["secret*"],"depth":64
            }),
        )
        .await;
        let names = result["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(names, ["a.py", "b.py", "中文.py"]);
        let result = invoke_over_http(
            "file.search",
            serde_json::json!({
                "path":root,"pattern":format!("{}/sub/[b].py", root.display())
            }),
        )
        .await;
        assert_eq!(result["entries"].as_array().unwrap().len(), 1);
        let result = invoke_over_http(
            "file.search",
            serde_json::json!({"path":root,"pattern":"needle*"}),
        )
        .await;
        assert!(result["entries"].as_array().unwrap().is_empty());
        let shallow = invoke_over_http(
            "file.search",
            serde_json::json!({"path":root,"pattern":"*.py","depth":1}),
        )
        .await;
        assert_eq!(shallow["entries"].as_array().unwrap().len(), 2);
        let missing = invoke_over_http(
            "file.search",
            serde_json::json!({"path":root.join("missing"),"pattern":"*"}),
        )
        .await;
        assert!(missing["entries"].as_array().unwrap().is_empty());
        let file = invoke_over_http(
            "file.search",
            serde_json::json!({"path":root.join("a.py"),"pattern":"*"}),
        )
        .await;
        assert!(file["entries"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn http_native_file_search_limits_reject_partial_results_and_invalid_input() {
        let directory = tempfile::tempdir().unwrap();
        for name in ["one", "two"] {
            std::fs::write(directory.path().join(name), b"data").unwrap();
        }
        for field in ["max_results", "max_entries"] {
            let mut args = serde_json::json!({"path":directory.path(),"pattern":"*"});
            args[field] = serde_json::json!(1);
            let result = invoke_over_http_status("file.search", args, 429).await;
            assert_eq!(result["error_code"], "RESOURCE_EXHAUSTED");
            assert!(result["entries"].as_array().unwrap().is_empty());
        }
        for (field, value) in [
            ("depth", serde_json::json!(0)),
            ("max_results", serde_json::json!(true)),
            ("timeout_ms", serde_json::json!(30001)),
            ("pattern", serde_json::json!("[z-a]")),
            ("exclude_patterns", serde_json::json!("*.tmp")),
        ] {
            let mut args = serde_json::json!({"path":directory.path(),"pattern":"*"});
            args[field] = value;
            let result = invoke_over_http_status("file.search", args, 400).await;
            assert_eq!(result["error_code"], "INVALID_ARGUMENT");
        }
    }

    #[tokio::test]
    async fn http_filesystem_list_preserves_recursive_depth() {
        let directory = tempfile::tempdir().unwrap();
        let nested = directory.path().join("nested");
        std::fs::create_dir(&nested).unwrap();
        let payload = nested.join("payload.bin");
        std::fs::write(&payload, b"payload").unwrap();

        let response = invoke_over_http(
            "file.list",
            serde_json::json!({"path":directory.path(),"depth":2}),
        )
        .await;
        let paths = response["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["path"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert!(paths.contains(&payload.to_str().unwrap()), "{response}");
    }

    #[tokio::test]
    async fn command_wait_deadline_and_missing_kill_are_http_successes() {
        let command_id = format!("http-wait-deadline-{}", std::process::id());
        let start = std::collections::BTreeMap::from([
            (
                "command_id".to_string(),
                rmpv::Value::from(command_id.clone()),
            ),
            ("cmd".to_string(), rmpv::Value::from("sleep 5")),
        ]);
        super::super::cmd::cmd_start(&start);
        let lookup = std::collections::BTreeMap::from([
            (
                "command_id".to_string(),
                rmpv::Value::from(command_id.clone()),
            ),
            ("timeout".to_string(), rmpv::Value::from(0)),
        ]);
        let wait = invoke_over_http(
            "process.wait",
            serde_json::json!({"command_id": command_id, "timeout": 0}),
        )
        .await;
        let _ = super::super::cmd::cmd_kill(&lookup);
        assert_eq!(wait["status"], "running");
        assert_eq!(wait["error_code"], "WAIT_TIMEOUT");

        let kill = invoke_over_http(
            "process.kill",
            serde_json::json!({"command_id": format!("http-missing-kill-{}", std::process::id())}),
        )
        .await;
        assert_eq!(kill["status"], "NOT_FOUND");
        assert_eq!(kill["killed"], false);
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

    #[tokio::test]
    async fn streaming_upload_preserves_in_place_write_semantics() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shared.txt");
        std::fs::write(&path, b"old-complete").unwrap();
        let alias = directory.path().join("alias.txt");
        std::fs::hard_link(&path, &alias).unwrap();
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_conn(&mut stream, None).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client.write_all(format!(
            "POST /upload?path={}&type=file HTTP/1.1\r\nHost: localhost\r\nContent-Length: 12\r\n\r\nnew-",
            path.display()
        ).as_bytes()).await.unwrap();
        // Observe the partial body while the upload remains open.
        wait_for_file_contents(&path, b"new-").await;
        client.write_all(b"complete").await.unwrap();
        let (head, _) = read_http_response(&mut client).await;
        assert!(head.starts_with("HTTP/1.1 200"));
        assert_eq!(std::fs::read(&path).unwrap(), b"new-complete");
        assert_eq!(std::fs::read(&alias).unwrap(), b"new-complete");
        drop(client);
        server.await.unwrap();
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 2);
    }

    #[tokio::test]
    async fn aborted_upload_retains_the_partial_in_place_write() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shared.txt");
        std::fs::write(&path, b"original").unwrap();
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            assert!(handle_conn(&mut stream, None).await.is_err());
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client.write_all(format!(
            "POST /upload?path={}&type=file HTTP/1.1\r\nHost: localhost\r\nContent-Length: 12\r\n\r\nshort",
            path.display()
        ).as_bytes()).await.unwrap();
        client.shutdown().await.unwrap();
        server.await.unwrap();
        // An aborted upload may still have a Tokio blocking write in flight.
        wait_for_file_contents(&path, b"short").await;
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upload_preserves_target_mode_and_symlink() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target");
        let link = directory.path().join("link");
        std::fs::write(&target, b"original").unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640)).unwrap();
        symlink(&target, &link).unwrap();
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_conn(&mut stream, None).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client.write_all(format!(
            "POST /upload?path={}&type=file HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\nnew",
            link.display()
        ).as_bytes()).await.unwrap();
        let (head, _) = read_http_response(&mut client).await;
        assert!(head.starts_with("HTTP/1.1 200"));
        drop(client);
        server.await.unwrap();
        assert!(std::fs::symlink_metadata(&link).unwrap().is_symlink());
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }

    #[tokio::test]
    async fn http_list_preserves_typed_filesystem_errors_and_path() {
        let directory = tempfile::tempdir().unwrap();
        let missing = directory.path().join("missing");
        let response =
            invoke_over_http_status("file.list", serde_json::json!({"path":missing}), 400).await;
        assert_eq!(response["error_code"], "FILE_NOT_FOUND");
        assert!(response["error"]
            .as_str()
            .unwrap()
            .contains(missing.to_str().unwrap()));
        let file = directory.path().join("file");
        std::fs::write(&file, b"data").unwrap();
        let response =
            invoke_over_http_status("file.list", serde_json::json!({"path":file}), 400).await;
        assert_eq!(response["error_code"], "NOT_A_DIRECTORY");
    }

    #[tokio::test]
    async fn binary_file_download_closes_connection_after_body() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("payload.bin");
        std::fs::write(&file, [0, 1, 255, 0]).unwrap();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_conn(&mut stream, None).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let path = file.to_string_lossy().replace('/', "%2F");
        client
            .write_all(
                format!("GET /download?path={path}&type=file HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let (head, body) = read_http_response(&mut client).await;
        assert!(head.contains("Connection: close\r\n"));
        assert!(!head.to_ascii_lowercase().contains("\r\ncontent-length:"));
        assert!(head.contains("Transfer-Encoding: chunked\r\n"));
        assert_eq!(body, [0, 1, 255, 0]);
        let mut byte = [0u8; 1];
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), client.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        server.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn upload_follows_a_symlink_to_a_missing_target() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target");
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_conn(&mut stream, None).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client.write_all(format!(
            "POST /upload?path={}&type=file HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\nnew",
            link.display()
        ).as_bytes()).await.unwrap();
        let (head, _) = read_http_response(&mut client).await;
        assert!(head.starts_with("HTTP/1.1 200"));
        assert!(link.is_symlink());
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
        drop(client);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn full_download_accepts_concurrent_in_place_truncation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data");
        std::fs::write(&path, vec![b'x'; 16 * 1024 * 1024]).unwrap();
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_conn(&mut stream, None).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(
                format!(
                    "GET /download?path={}&type=file HTTP/1.1\r\nHost: localhost\r\n\r\n",
                    path.display()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let head = read_http_head(&mut client).await;
        std::fs::write(&path, []).unwrap();
        let body = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            read_chunked_body(&mut client),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(head.starts_with("HTTP/1.1 200"));
        assert!(!head.to_ascii_lowercase().contains("\r\ncontent-length:"));
        assert!(head.contains("Transfer-Encoding: chunked\r\n"));
        assert!(body.len() < 16 * 1024 * 1024);
    }

    #[tokio::test]
    async fn range_download_retains_fixed_length_and_selected_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data");
        std::fs::write(&path, b"0123456789").unwrap();
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_conn(&mut stream, None).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client.write_all(format!(
            "GET /download?path={}&type=file HTTP/1.1\r\nHost: localhost\r\nRange: bytes=3-6\r\n\r\n",
            path.display()
        ).as_bytes()).await.unwrap();
        let (head, body) = read_http_response(&mut client).await;
        assert!(head.starts_with("HTTP/1.1 206 Partial Content"));
        assert!(head.contains("Content-Length: 4\r\n"));
        assert!(head.contains("Content-Range: bytes 3-6/10\r\n"));
        assert_eq!(body, b"3456");
        server.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fifo_download_does_not_wake_a_waiting_writer() {
        use std::os::unix::fs::OpenOptionsExt;
        use std::sync::mpsc;
        use std::time::Duration;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fifo");
        assert!(std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success());
        let writer_path = path.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (opened_tx, opened_rx) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(writer_path)
                .unwrap();
            opened_tx.send(()).unwrap();
            file
        });
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let blocked_before_request = opened_rx.recv_timeout(Duration::from_millis(50)).is_err();

        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_conn(&mut stream, None).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(
                format!(
                    "GET /download?path={}&type=file HTTP/1.1\r\nHost: localhost\r\n\r\n",
                    path.display()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let response =
            tokio::time::timeout(Duration::from_secs(2), read_http_response(&mut client)).await;
        let writer_woken = opened_rx.recv_timeout(Duration::from_millis(100)).is_ok();
        // Always release the producer before asserting, including on regressions.
        let peer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&path)
            .unwrap();
        drop(writer.join().unwrap());
        drop(peer);
        let (head, _) = response.unwrap();
        drop(client);
        server.await.unwrap();
        assert!(blocked_before_request);
        assert!(head.starts_with("HTTP/1.1 404"));
        assert!(
            !writer_woken,
            "rejected download opened a FIFO read endpoint"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn fifo_download_is_rejected_without_waiting_for_a_writer() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fifo");
        assert!(std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success());
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_conn(&mut stream, None).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        client
            .write_all(
                format!(
                    "GET /download?path={}&type=file HTTP/1.1\r\nHost: localhost\r\n\r\n",
                    path.display()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            read_http_response(&mut client),
        )
        .await;
        // Release any blocked open even on failure so the test runtime can exit.
        use std::os::unix::fs::OpenOptionsExt;
        let _peer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(&path)
            .unwrap();
        let (head, _) = response.unwrap();
        assert!(head.starts_with("HTTP/1.1 404"));
        drop(client);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn tar_download_is_chunked_and_connection_remains_reusable() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("payload.txt"), b"tar-roundtrip").unwrap();
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            handle_conn(&mut stream, None).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(address).await.unwrap();
        let path = directory.path().to_string_lossy().replace('/', "%2F");
        client
            .write_all(
                format!("GET /download?path={path}&type=tar HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let (head, tar) = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            read_chunked_response(&mut client),
        )
        .await
        .unwrap();
        assert!(head.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(head.contains("Transfer-Encoding: chunked\r\n"));
        assert!(tar
            .windows(b"payload.txt".len())
            .any(|v| v == b"payload.txt"));
        assert!(tar
            .windows(b"tar-roundtrip".len())
            .any(|v| v == b"tar-roundtrip"));

        client
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let (_, body) = read_http_response(&mut client).await;
        assert_eq!(body, br#"{"status":"ok"}"#);
        drop(client);
        server.await.unwrap();
    }

    use std::process::Command;

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_poll_counts_as_request_activity() {
        const ISOLATED_ENV: &str = "ADX_EXECD_HTTP_POLL_ACTIVITY_TEST_ISOLATED";
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

    #[tokio::test]
    async fn pty_terminal_status_and_close_frame_are_followed_by_tcp_eof() {
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = serve_listener(listener, None).await;
        });
        let uri = format!(
            "ws://{address}/pty?protocol=environment.pty.v1&command=%2Fbin%2Fsh&command=-c&command=exit%200"
        );
        let (mut websocket, _) = tokio_tungstenite::connect_async(uri).await.unwrap();
        let mut exited = false;
        loop {
            let message = tokio::time::timeout(std::time::Duration::from_secs(5), websocket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            match message {
                tokio_tungstenite::tungstenite::Message::Text(text) => {
                    let event: serde_json::Value = serde_json::from_str(&text).unwrap();
                    if event["type"] == "exited" {
                        assert_eq!(event["exit_code"], 0);
                        exited = true;
                    }
                }
                tokio_tungstenite::tungstenite::Message::Close(_) => break,
                _ => {}
            }
        }
        assert!(exited);
        // Acknowledge the Close frame, then inspect TCP without another
        // WebSocket poll: the session must not return to HTTP keep-alive.
        websocket.flush().await.unwrap();
        let socket = websocket.get_mut();
        let mut buffer = [0; 1024];
        let eof =
            tokio::time::timeout(std::time::Duration::from_secs(1), socket.read(&mut buffer)).await;
        server.abort();
        assert_eq!(
            eof.expect("Execd must close TCP after the PTY closes")
                .unwrap(),
            0
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn pty_late_resize_preserves_terminal_frames_and_orderly_tcp_close() {
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = serve_listener(listener, None).await;
        });
        let uri = format!("ws://{address}/pty?command=%2Fbin%2Fsh&command=-c&command=exit%207");
        let (mut websocket, _) = tokio_tungstenite::connect_async(uri).await.unwrap();
        let started = websocket.next().await.unwrap().unwrap();
        let event: serde_json::Value = serde_json::from_str(started.to_text().unwrap()).unwrap();
        assert_eq!(event["type"], "started");
        // The SDK's initial resize can reach a short-lived process after it exits,
        // while the server is draining the last output. It must not reset TCP and
        // discard the terminal frames when that client input remains unread.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        websocket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                "RESIZE:80:24".into(),
            ))
            .await
            .unwrap();
        let mut exited = false;
        loop {
            let frame = tokio::time::timeout(std::time::Duration::from_secs(2), websocket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            match frame {
                tokio_tungstenite::tungstenite::Message::Text(text) => {
                    let event: serde_json::Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(event["type"], "exited");
                    assert_eq!(event["exit_code"], 7);
                    exited = true;
                }
                tokio_tungstenite::tungstenite::Message::Close(_) => break,
                _ => {}
            }
        }
        assert!(exited);
        websocket.flush().await.unwrap();
        let mut byte = [0];
        let eof = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            websocket.get_mut().read(&mut byte),
        )
        .await;
        server.abort();
        assert_eq!(eof.expect("PTY close must finish TCP shutdown").unwrap(), 0);
    }

    #[tokio::test]
    async fn pty_peer_without_close_reply_does_not_hold_tcp_forever() {
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = serve_listener(listener, None).await;
        });
        let (mut websocket, _) = tokio_tungstenite::connect_async(format!(
            "ws://{address}/pty?command=%2Fbin%2Fsh&command=-c&command=exit%200"
        ))
        .await
        .unwrap();
        loop {
            let frame = tokio::time::timeout(std::time::Duration::from_secs(2), websocket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if matches!(frame, tokio_tungstenite::tungstenite::Message::Close(_)) {
                break;
            }
        }
        // Do not flush tungstenite's automatic Close reply. A peer that vanishes
        // after receiving the terminal frames cannot retain this socket forever.
        let mut byte = [0];
        let eof = tokio::time::timeout(
            std::time::Duration::from_secs(6),
            websocket.get_mut().read(&mut byte),
        )
        .await;
        server.abort();
        assert_eq!(
            eof.expect("PTY close handshake must have a deadline")
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn command_watch_client_close_does_not_return_to_http_keepalive() {
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = serve_listener(listener, None).await;
        });
        let (mut websocket, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/commands/watch"))
                .await
                .unwrap();
        websocket
            .send(tokio_tungstenite::tungstenite::Message::Close(None))
            .await
            .unwrap();
        let socket = websocket.get_mut();
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            let mut buffer = [0; 1024];
            while socket.read(&mut buffer).await.unwrap() != 0 {}
        })
        .await;
        server.abort();
        result.expect("Execd must close TCP after the command watch closes");
    }

    #[tokio::test]
    async fn pty_websocket_streams_output_and_terminal_status() {
        let listener = bind(0).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _ = serve_listener(listener, None).await;
        });
        let uri = format!(
            "ws://{address}/pty?rows=24&cols=80&protocol=environment.pty.v1&command=%2Fbin%2Fsh&command=-c&command=sleep%200.2%3B%20printf%20pty-ok"
        );
        let (mut websocket, _) = tokio_tungstenite::connect_async(uri).await.unwrap();

        let started = tokio::time::timeout(std::time::Duration::from_secs(5), websocket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let started: serde_json::Value = serde_json::from_str(started.to_text().unwrap()).unwrap();
        assert_eq!(started["type"], "started");
        assert_eq!(started["version"], 1);
        websocket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                "RESIZE:100:40".into(),
            ))
            .await
            .unwrap();

        let mut output = Vec::new();
        let mut exit_code = None;
        while exit_code.is_none() {
            let message = tokio::time::timeout(std::time::Duration::from_secs(5), websocket.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            match message {
                tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
                    output.extend_from_slice(&bytes);
                }
                tokio_tungstenite::tungstenite::Message::Text(text) => {
                    let event: serde_json::Value = serde_json::from_str(&text).unwrap();
                    if event["type"] == "exited" {
                        exit_code = event["exit_code"].as_i64();
                    }
                }
                _ => {}
            }
        }
        assert_eq!(
            exit_code,
            Some(0),
            "PTY output: {}",
            String::from_utf8_lossy(&output)
        );
        assert!(String::from_utf8_lossy(&output).contains("pty-ok"));
        server.abort();
    }
}
