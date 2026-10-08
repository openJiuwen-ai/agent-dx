//! Legacy exec/file HTTP contract over the shared, tenant-authorized Relay connection.
use super::{
    agent_response::{error_response, json_response},
    server::{Ingress, ProxyBody},
    AccessKind,
};
use adx_agent_api::{inline_runtime as adapt, management::InlineService, Error};
use adx_agent_core::{limits, sandbox::SandboxRuntime, transport::RequestProgress};
use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

type Result<T> = std::result::Result<T, Error>;
const MAX_FILE: u64 = 512 * 1024 * 1024;

pub(super) fn matches(path: &str) -> bool {
    path.strip_prefix("/api/agent/")
        .and_then(|p| p.split_once('/'))
        .is_some_and(|(_, operation)| {
            matches!(
                operation,
                "exec" | "files/upload" | "files/download" | "files/list" | "files/mkdir"
            )
        })
}
pub(super) async fn handle(
    request: Request<Incoming>,
    tenant: &str,
    service: &InlineService,
    gateway: &Ingress,
) -> Response<ProxyBody> {
    match dispatch(request, tenant, service, gateway).await {
        Ok(response) => response,
        Err(error) => error_response(error, None),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Execute {
    command: Value,
    #[serde(default)]
    working_dir: Option<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    timeout: Option<f64>,
}
struct RuntimeClient<'a> {
    gateway: &'a Ingress,
    tenant: &'a str,
    id: &'a str,
    runtime: SandboxRuntime,
    trace: String,
}
impl RuntimeClient<'_> {
    async fn send(
        &self,
        method: &str,
        path: &str,
        body: Bytes,
        range: Option<&str>,
        timeout: Duration,
    ) -> Result<Response<Incoming>> {
        self.send_body(method, path, Full::new(body), range, timeout)
            .await
    }
    async fn send_body<B>(
        &self,
        method: &str,
        path: &str,
        body: B,
        range: Option<&str>,
        timeout: Duration,
    ) -> Result<Response<Incoming>>
    where
        B: hyper::body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        let progress = RequestProgress::default();
        let operation = async {
            let route = self
                .gateway
                .resolve_route(
                    self.id,
                    self.runtime.port,
                    AccessKind::Tunnel,
                    self.trace.clone(),
                )
                .await
                .map_err(|_| Error::Unavailable("Sandbox runtime route unavailable".into()))?;
            self.gateway
                .authorize(&route, self.tenant)
                .map_err(|_| Error::NotFound)?;
            let stream = self
                .gateway
                .open_authorized_stream(route)
                .await
                .map_err(|_| Error::Unavailable("Sandbox runtime connection unavailable".into()))?;
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake::<_, B>(TokioIo::new(stream))
                    .await
                    .map_err(|_| {
                        Error::Unavailable("Sandbox runtime HTTP handshake failed".into())
                    })?;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let length = body.size_hint().exact();
            let mut request = Request::builder()
                .method(method)
                .uri(path)
                .header("host", "execd.internal")
                .header("x-auth", &self.runtime.token)
                .header("x-trace-id", &self.trace)
                .header("connection", "close");
            if let Some(length) = length {
                request = request.header("content-length", length);
            }
            if let Some(range) = range {
                request = request.header("range", range);
            }
            let request = request
                .body(body)
                .map_err(|_| Error::Invalid("invalid runtime request".into()))?;
            if method != "GET" {
                progress.start_write();
            }
            let mut response = sender
                .send_request(request)
                .await
                .map_err(|_| transport_error(progress.may_have_written()))?;
            response.extensions_mut().insert(deadline);
            Ok(response)
        };
        tokio::time::timeout_at(deadline, operation)
            .await
            .map_err(|_| transport_error(progress.may_have_written()))?
    }

    async fn json(
        &self,
        method: &str,
        path: &str,
        body: Value,
        timeout: Duration,
    ) -> Result<Value> {
        let bytes = if body.is_null() {
            Bytes::new()
        } else {
            Bytes::from(
                serde_json::to_vec(&body)
                    .map_err(|_| Error::Invalid("invalid JSON request".into()))?,
            )
        };
        self.read_json(self.send(method, path, bytes, None, timeout).await?)
            .await
    }
    async fn read_json(&self, response: Response<Incoming>) -> Result<Value> {
        let status = response.status();
        let deadline = response
            .extensions()
            .get::<tokio::time::Instant>()
            .copied()
            .ok_or_else(|| Error::Unavailable("missing internal response deadline".into()))?;
        let raw = tokio::time::timeout_at(
            deadline,
            http_body_util::Limited::new(response.into_body(), limits::HTTP_JSON_BYTES).collect(),
        )
        .await
        .map_err(|_| transport_error(true))?
        .map_err(|_| transport_error(true))?
        .to_bytes();
        let value: Value = serde_json::from_slice(&raw)
            .map_err(|_| Error::Unavailable("invalid Sandbox response".into()))?;
        if !status.is_success() {
            let message = value.to_string().replace(&self.runtime.token, "[redacted]");
            return Err(match status {
                StatusCode::NOT_FOUND => Error::NotFound,
                StatusCode::BAD_REQUEST => Error::Invalid(message),
                StatusCode::CONFLICT => Error::Conflict(message),
                _ => Error::Unavailable(format!("Sandbox HTTP {}: {message}", status.as_u16())),
            });
        }
        adapt::checked(value)
    }
    async fn invoke(&self, action: &str, args: Value, timeout: Duration) -> Result<Value> {
        let result = self
            .json(
                "POST",
                "/invoke",
                json!({"action":action,"args":args}),
                timeout,
            )
            .await;
        if matches!(action, "fs_list" | "fs_get_info") {
            result.map_err(read_error)
        } else {
            result
        }
    }
}
fn transport_error(write: bool) -> Error {
    if write {
        Error::OutcomeUnknown(
            "Sandbox response interrupted or deadline exceeded; do not replay automatically".into(),
        )
    } else {
        Error::Unavailable("Sandbox read or connection unavailable".into())
    }
}
fn read_error(error: Error) -> Error {
    match error {
        Error::OutcomeUnknown(_) => {
            Error::Unavailable("Sandbox read response interrupted or deadline exceeded".into())
        }
        other => other,
    }
}
fn query_path(path: &str, pairs: &[(&str, &str)]) -> String {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(pairs.iter().copied())
        .finish();
    format!("{path}?{query}")
}
fn field<'a>(query: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str> {
    query
        .get(name)
        .map(String::as_str)
        .filter(|v| !v.trim().is_empty() && !v.contains('\0'))
        .ok_or_else(|| Error::Invalid(format!("{name} is required")))
}
fn boolean(query: &BTreeMap<String, String>, key: &str) -> Result<bool> {
    match query.get(key).map(String::as_str) {
        None | Some("false" | "0" | "") => Ok(false),
        Some("true" | "1") => Ok(true),
        _ => Err(Error::Invalid(format!("invalid {key}"))),
    }
}
async fn dispatch(
    request: Request<Incoming>,
    tenant: &str,
    service: &InlineService,
    gateway: &Ingress,
) -> Result<Response<ProxyBody>> {
    let (parts, body) = request.into_parts();
    let (id, operation) = parts
        .uri
        .path()
        .strip_prefix("/api/agent/")
        .and_then(|path| path.split_once('/'))
        .ok_or(Error::NotFound)?;
    uuid::Uuid::parse_str(id).map_err(|_| Error::Invalid("invalid instance ID".into()))?;
    let expected_method = if matches!(operation, "files/list" | "files/download") {
        http::Method::GET
    } else {
        http::Method::POST
    };
    if parts.method != expected_method {
        return Err(Error::Invalid(
            "method does not match inline operation".into(),
        ));
    }
    let mut query = BTreeMap::new();
    for (key, value) in url::form_urlencoded::parse(parts.uri.query().unwrap_or("").as_bytes()) {
        let allowed = matches!(key.as_ref(), "token" | "tenant_id")
            || match operation {
                "files/list" => matches!(key.as_ref(), "path" | "recursive" | "max_depth"),
                "files/mkdir" => matches!(key.as_ref(), "path" | "mode" | "recursive"),
                "files/download" => key == "path",
                "files/upload" => key == "mode",
                _ => false,
            };
        if !allowed || query.insert(key.into_owned(), value.into_owned()).is_some() {
            return Err(Error::Invalid(
                "unknown or duplicate inline parameter".into(),
            ));
        }
    }
    let trace = match parts.headers.get("x-trace-id") {
        Some(value) => value
            .to_str()
            .ok()
            .filter(|v| {
                !v.trim().is_empty()
                    && v.len() <= limits::IDENTIFIER_BYTES
                    && !v.chars().any(char::is_control)
            })
            .ok_or_else(|| Error::Invalid("invalid X-Trace-ID".into()))?
            .to_owned(),
        None => uuid::Uuid::new_v4().to_string(),
    };
    let runtime = service.runtime(tenant, id).await?;
    let client = RuntimeClient {
        gateway,
        tenant,
        id,
        runtime,
        trace,
    };
    let timeout = limits::AGENT_REQUEST_TIMEOUT;
    let value = match operation {
        "exec" => {
            let raw = http_body_util::Limited::new(body, limits::HTTP_JSON_BYTES)
                .collect()
                .await
                .map_err(|_| Error::Invalid("invalid or oversized exec request".into()))?
                .to_bytes();
            let input: Execute = serde_json::from_slice(&raw)
                .map_err(|_| Error::Invalid("invalid exec JSON".into()))?;
            let seconds = input.timeout.unwrap_or(60.0);
            if !seconds.is_finite() || seconds <= 0.0 || seconds > 3600.0 {
                return Err(Error::Invalid(
                    "timeout must be in (0, 3600] seconds".into(),
                ));
            }
            if input.working_dir.as_ref().is_some_and(|s| s.contains('\0'))
                || input
                    .env
                    .iter()
                    .any(|(k, v)| k.is_empty() || k.contains(['=', '\0']) || v.contains('\0'))
            {
                return Err(Error::Invalid(
                    "invalid command environment or working_dir".into(),
                ));
            }
            let result = client.invoke("cmd_run", json!({"cmd":adapt::command(&input.command)?,"cwd":input.working_dir,"env":input.env,"timeout":seconds}), Duration::from_secs_f64(seconds) + Duration::from_secs(5)).await?;
            adapt::exec_result(result)?
        }
        "files/list" => {
            let path = field(&query, "path")?;
            let recursive = boolean(&query, "recursive")?;
            let depth = query
                .get("max_depth")
                .map(|s| {
                    s.parse::<u32>().map_err(|_| {
                        Error::Invalid("max_depth must be a nonnegative integer".into())
                    })
                })
                .transpose()?
                .unwrap_or(0);
            if depth > 64 {
                return Err(Error::Unsupported(
                    "Execd directory depth is limited to 64".into(),
                ));
            }
            let depth = if !recursive {
                1
            } else if depth == 0 {
                20
            } else {
                depth
            };
            adapt::list_result(
                client
                    .invoke("fs_list", json!({"path":path,"depth":depth}), timeout)
                    .await?,
            )?
        }
        "files/mkdir" => {
            let path = field(&query, "path")?;
            if query.get("mode").is_some_and(|v| !v.is_empty()) || !boolean(&query, "recursive")? {
                return Err(Error::Unsupported("Execd mkdir currently supports recursive=true without mode; nonrecursive and permission policies are pending Platform support".into()));
            }
            let result = client
                .invoke("fs_make_dir", json!({"path":path}), timeout)
                .await?;
            json!({"success":true,"path":path,"created":result["created"]})
        }
        "files/download" => {
            let path = field(&query, "path")?;
            let range = parts
                .headers
                .get("range")
                .map(|v| {
                    v.to_str()
                        .map_err(|_| Error::Invalid("invalid Range".into()))
                })
                .transpose()?;
            let range = if let Some(range) = range {
                let info = client
                    .invoke("fs_get_info", json!({"path":path}), timeout)
                    .await?;
                let size = info["size"]
                    .as_u64()
                    .ok_or_else(|| Error::Unavailable("invalid file size".into()))?;
                match download_range(range, size) {
                    Some(range) => Some(range),
                    None => {
                        let mut response = json_response(
                            StatusCode::RANGE_NOT_SATISFIABLE,
                            &json!({"code":416,"message":"requested range is not satisfiable"}),
                            Some(&client.trace),
                        );
                        response.headers_mut().insert(
                            "content-range",
                            format!("bytes */{size}").parse().map_err(|_| {
                                Error::Unavailable("invalid file size header".into())
                            })?,
                        );
                        return Ok(response);
                    }
                }
            } else {
                None
            };
            let response = client
                .send(
                    "GET",
                    &query_path("/download", &[("path", path), ("type", "file")]),
                    Bytes::new(),
                    range.as_deref(),
                    timeout,
                )
                .await?;
            if !response.status().is_success() {
                client.read_json(response).await.map_err(read_error)?;
                return Err(Error::Unavailable("unexpected download response".into()));
            }
            let (mut parts, body) = response.into_parts();
            parts.headers.remove("connection");
            parts
                .headers
                .insert("accept-ranges", http::HeaderValue::from_static("bytes"));
            parts.headers.insert(
                "x-trace-id",
                client
                    .trace
                    .parse()
                    .map_err(|_| Error::Invalid("invalid trace".into()))?,
            );
            return Ok(Response::from_parts(
                parts,
                body.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
                    .boxed_unsync(),
            ));
        }
        "files/upload" => upload(&client, &parts.headers, &query, body).await?,
        _ => return Err(Error::NotFound),
    };
    Ok(json_response(StatusCode::OK, &value, Some(&client.trace)))
}

async fn upload(
    client: &RuntimeClient<'_>,
    headers: &http::HeaderMap,
    query: &BTreeMap<String, String>,
    body: Incoming,
) -> Result<Value> {
    if query.get("mode").is_some_and(|v| !v.is_empty()) {
        return Err(Error::Unsupported(
            "Execd upload permission mode is pending Platform support".into(),
        ));
    }
    let boundary = headers
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .and_then(|value| multer::parse_boundary(value).ok())
        .ok_or_else(|| Error::Invalid("expected multipart upload".into()))?;
    let constraints = multer::Constraints::new().size_limit(
        multer::SizeLimit::new()
            .whole_stream(MAX_FILE + 1024 * 1024)
            .per_field(MAX_FILE)
            .for_field("path", 4096),
    );
    let mut multipart =
        multer::Multipart::with_constraints(body.into_data_stream(), boundary, constraints);
    let mut path = None;
    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|_| Error::Invalid("invalid or oversized multipart upload".into()))?
    {
        match field.name() {
            Some("path") => {
                let value = field
                    .text()
                    .await
                    .map_err(|_| Error::Invalid("invalid upload path".into()))?;
                if value.trim().is_empty() || value.len() > 4096 || value.contains('\0') {
                    return Err(Error::Invalid("invalid upload path".into()));
                }
                path = Some(value.trim().to_owned());
            }
            Some("file") => {
                let path = path
                    .as_deref()
                    .ok_or_else(|| Error::Invalid("path field must precede file".into()))?;
                let upload = uuid::Uuid::new_v4().to_string();
                // Stream this multipart field in one Execd request. The runtime owns the part
                // and only exposes the target after the separate commit succeeds.
                let url = query_path(
                    "/upload",
                    &[
                        ("path", path),
                        ("type", "file"),
                        ("uploadId", &upload),
                        ("offset", "0"),
                    ],
                );
                let stream = futures_util::stream::try_unfold(field, |mut field| async move {
                    match field.chunk().await {
                        Ok(Some(bytes)) => Ok(Some((hyper::body::Frame::data(bytes), field))),
                        Ok(None) => Ok(None),
                        Err(error) => Err(std::io::Error::other(error)),
                    }
                });
                let response = client
                    .send_body(
                        "POST",
                        &url,
                        StreamBody::new(stream),
                        None,
                        limits::AGENT_REQUEST_TIMEOUT,
                    )
                    .await?;
                let result = client.read_json(response).await?;
                let offset = result["bytes_written"]
                    .as_u64()
                    .filter(|size| *size <= MAX_FILE)
                    .ok_or_else(|| {
                        Error::OutcomeUnknown(
                            "invalid upload byte count; target not committed".into(),
                        )
                    })?;
                let size = offset.to_string();
                let url = query_path(
                    "/upload/commit",
                    &[("path", path), ("uploadId", &upload), ("totalSize", &size)],
                );
                client
                    .json("POST", &url, Value::Null, limits::AGENT_REQUEST_TIMEOUT)
                    .await?;
                return Ok(json!({"success":true,"path":path,"size":offset}));
            }
            _ => {
                while field
                    .chunk()
                    .await
                    .map_err(|_| Error::Invalid("invalid upload part".into()))?
                    .is_some()
                {}
            }
        }
    }
    Err(Error::Invalid(
        "multipart form must include a file field".into(),
    ))
}

fn download_range(value: &str, size: u64) -> Option<String> {
    let (start, end) = value.strip_prefix("bytes=")?.split_once('-')?;
    if size == 0 || end.contains(',') {
        return None;
    }
    let (start, end) = if start.is_empty() {
        let suffix = end.parse::<u64>().ok().filter(|value| *value > 0)?;
        (size.saturating_sub(suffix), size - 1)
    } else {
        let start = start.parse::<u64>().ok().filter(|value| *value < size)?;
        let end = if end.is_empty() {
            size - 1
        } else {
            end.parse::<u64>().ok()?.min(size - 1)
        };
        if start > end {
            return None;
        }
        (start, end)
    };
    Some(format!("bytes={start}-{end}"))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn download_ranges_bound_the_requested_file_segment() {
        for (range, expected) in [
            ("bytes=1-3", Some("bytes=1-3")),
            ("bytes=2-", Some("bytes=2-4")),
            ("bytes=-2", Some("bytes=3-4")),
            ("bytes=3-99", Some("bytes=3-4")),
            ("bytes=5-", None),
            ("bytes=3-1", None),
            ("bytes=0-1,3-4", None),
        ] {
            assert_eq!(download_range(range, 5).as_deref(), expected);
        }
        assert_eq!(download_range("bytes=0-", 0), None);
    }
}
