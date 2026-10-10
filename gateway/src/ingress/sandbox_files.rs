//! Shared Sandbox file operations and Execd transport authorization.
//! Business adapters supply an authorized tenant/Sandbox pair, never Execd credentials.
use crate::ingress::{
    resolver::RouteHandle,
    route_store::RouteChange,
    server::{wait_for_route_cancellation, Ingress},
    AccessKind,
};
use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper_util::rt::TokioIo;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{broadcast, OnceCell};
use tokio::time::Instant;

const MAX_READ_BYTES: usize = 64 * 1024;

/// Deliberately excludes upstream text, file contents and runtime credentials.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ReadError {
    #[error("a verified tenant and Sandbox identity are required")]
    Identity,
    #[error("invalid Sandbox file path")]
    InvalidPath,
    #[error("invalid Sandbox byte range")]
    InvalidRange,
    #[error("invalid bounded read limit")]
    InvalidLimit,
    #[error("Sandbox or file not found in the authorized scope")]
    NotFound,
    #[error("Sandbox runtime unavailable")]
    Runtime,
    #[error("Sandbox runtime route unavailable")]
    Route,
    #[error("Sandbox route revoked")]
    Revoked,
    #[error("system clock unavailable")]
    Clock,
    #[error("Sandbox read deadline exceeded")]
    Timeout,
    #[error("invalid or unsuccessful Execd response")]
    Protocol,
    #[error("Sandbox file exceeds the read limit")]
    TooLarge,
}

#[derive(Clone, Copy)]
pub struct FileMetadata {
    pub regular_file: bool,
    pub size: u64,
}

/// Read-only management boundary. Implementations must enforce tenant ownership
/// and Running state, and must not create or activate a Sandbox.
#[async_trait::async_trait]
pub trait SandboxDirectory: Send + Sync {
    async fn authorize(&self, tenant: &str, sandbox_id: &str) -> Result<(), ReadError>;
}

/// Deployment-owned credentials, private to the shared data plane.
/// Neither this type nor its credentials implement Debug/Serialize.
pub struct ExecdAccess {
    directory: Arc<dyn SandboxDirectory>,
    pub(crate) port: u16,
    token: http::HeaderValue,
}
impl ExecdAccess {
    pub fn new(
        directory: Arc<dyn SandboxDirectory>,
        port: u16,
        token: String,
    ) -> Result<Self, ReadError> {
        if port == 0
            || token.is_empty()
            || token.len() > 4096
            || token
                .bytes()
                .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        {
            return Err(ReadError::Runtime);
        }
        let mut token = http::HeaderValue::from_str(&token).map_err(|_| ReadError::Runtime)?;
        token.set_sensitive(true);
        Ok(Self {
            directory,
            port,
            token,
        })
    }
    pub(crate) async fn apply<B>(
        &self,
        tenant: &str,
        id: &str,
        request: &mut Request<B>,
    ) -> Result<(), ReadError> {
        self.directory.authorize(tenant, id).await?;
        request.headers_mut().insert("x-auth", self.token.clone());
        Ok(())
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DirectoryConfig {
    pub url: String,
    #[serde(default)]
    pub allow_http: bool,
    pub ca_file: Option<String>,
}

/// Configuration contains environment variable names, never inline secrets.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileAccessConfig {
    pub port: u16,
    pub token_env: String,
    pub directory: Option<DirectoryConfig>,
    #[serde(default)]
    pub api_key_envs: BTreeMap<String, String>,
}
impl FileAccessConfig {
    pub fn build(
        self,
        local: Option<Arc<dyn SandboxDirectory>>,
    ) -> Result<Arc<ExecdAccess>, ReadError> {
        let directory = if let Some(local) = local {
            if self.directory.is_some() || !self.api_key_envs.is_empty() {
                return Err(ReadError::Runtime);
            }
            local
        } else {
            let keys = self
                .api_key_envs
                .into_iter()
                .map(|(tenant, name)| {
                    std::env::var(name)
                        .map(|key| (tenant, key))
                        .map_err(|_| ReadError::Runtime)
                })
                .collect::<Result<BTreeMap<_, _>, _>>()?;
            Arc::new(HttpDirectory::new(
                self.directory.ok_or(ReadError::Runtime)?,
                keys,
            )?)
        };
        let token = std::env::var(self.token_env).map_err(|_| ReadError::Runtime)?;
        Ok(Arc::new(ExecdAccess::new(directory, self.port, token)?))
    }
}

/// Existing API Server GET /api/instances. Keys must be tenant-scoped, never admin keys.
pub struct HttpDirectory {
    client: reqwest::Client,
    base: url::Url,
    keys: BTreeMap<String, String>,
}
impl HttpDirectory {
    pub fn new(config: DirectoryConfig, keys: BTreeMap<String, String>) -> Result<Self, ReadError> {
        let base = adx_agent_core::transport::service_origin(&config.url, config.allow_http)
            .map_err(|_| ReadError::Runtime)?;
        if keys.is_empty()
            || keys.iter().any(|(tenant, key)| {
                tenant.trim().is_empty()
                    || key.is_empty()
                    || key.len() > 4096
                    || key
                        .bytes()
                        .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
                    || http::HeaderValue::from_str(key).is_err()
            })
        {
            return Err(ReadError::Runtime);
        }
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none());
        if let Some(path) = config.ca_file {
            let bytes = std::fs::read(path).map_err(|_| ReadError::Runtime)?;
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(&bytes).map_err(|_| ReadError::Runtime)?,
            );
        }
        Ok(Self {
            client: builder.build().map_err(|_| ReadError::Runtime)?,
            base,
            keys,
        })
    }
}
#[async_trait::async_trait]
impl SandboxDirectory for HttpDirectory {
    async fn authorize(&self, tenant: &str, sandbox_id: &str) -> Result<(), ReadError> {
        if tenant.trim().is_empty() || sandbox_id.trim().is_empty() {
            return Err(ReadError::Identity);
        }
        let key = self.keys.get(tenant).ok_or(ReadError::NotFound)?;
        let mut url = self
            .base
            .join("/api/instances")
            .map_err(|_| ReadError::Runtime)?;
        url.query_pairs_mut().append_pair("instance_id", sandbox_id);
        let mut response = self
            .client
            .get(url)
            .bearer_auth(key)
            .send()
            .await
            .map_err(|_| ReadError::Runtime)?;
        match response.status().as_u16() {
            200 => (),
            403 | 404 => return Err(ReadError::NotFound),
            _ => return Err(ReadError::Runtime),
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| ReadError::Runtime)? {
            if chunk.len() > MAX_READ_BYTES.saturating_sub(bytes.len()) {
                return Err(ReadError::TooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        // The existing filtered API returns a one-element array, not an object.
        let instances: Vec<Value> =
            serde_json::from_slice(&bytes).map_err(|_| ReadError::Protocol)?;
        let [value] = instances.as_slice() else {
            return Err(ReadError::Runtime);
        };
        if value["id"].as_str() != Some(sandbox_id) || value["status"].as_str() != Some("running") {
            return Err(ReadError::Runtime);
        }
        Ok(())
    }
}

/// One request's authorized target. Retain route events across chunk boundaries,
/// including while no Relay stream exists. Missing events fail closed.
struct Admission {
    route: RouteHandle,
    changes: Mutex<broadcast::Receiver<RouteChange>>,
    revoked: AtomicBool,
}
impl Admission {
    fn check(&self, ingress: &Ingress) -> Result<(), ReadError> {
        if self.revoked.load(Ordering::Acquire) {
            return Err(ReadError::Revoked);
        }
        let mut changes = self.changes.lock().map_err(|_| ReadError::Revoked)?;
        loop {
            match changes.try_recv() {
                Ok(change) if change.instance_id != self.route.target.instance_id => continue,
                Err(broadcast::error::TryRecvError::Empty) => break,
                _ => {
                    self.revoked.store(true, Ordering::Release);
                    return Err(ReadError::Revoked);
                }
            }
        }
        if !ingress.route_is_current(&self.route) {
            self.revoked.store(true, Ordering::Release);
            return Err(ReadError::Revoked);
        }
        Ok(())
    }
}

/// Bounded file operations for one HTTP transfer. Directory admission is shared
/// by all reads/uploads/commits in this object, never across frontend requests.
/// The execution stays pinned; a revoked transfer cannot resume on another route.
pub struct Files {
    ingress: Arc<Ingress>,
    deadline: Instant,
    tenant: String,
    sandbox_id: String,
    admission: OnceCell<Result<Admission, ReadError>>,
}
impl Files {
    pub fn new(
        ingress: &Arc<Ingress>,
        deadline: Instant,
        tenant: &str,
        sandbox_id: &str,
    ) -> Result<Self, ReadError> {
        if tenant.trim().is_empty() || sandbox_id.trim().is_empty() {
            return Err(ReadError::Identity);
        }
        Ok(Self {
            ingress: ingress.clone(),
            deadline,
            tenant: tenant.to_owned(),
            sandbox_id: sandbox_id.to_owned(),
            admission: OnceCell::new(),
        })
    }

    async fn admit(&self, access: &ExecdAccess) -> Result<&Admission, ReadError> {
        self.admission
            .get_or_init(|| async {
                access
                    .directory
                    .authorize(&self.tenant, &self.sandbox_id)
                    .await?;
                let route = self
                    .ingress
                    .resolve_route(
                        &self.sandbox_id,
                        access.port,
                        AccessKind::Direct,
                        String::new(),
                    )
                    .await
                    .map_err(|_| ReadError::Route)?;
                self.ingress
                    .authorize(&route, &self.tenant)
                    .map_err(|_| ReadError::NotFound)?;
                let admission = Admission {
                    route,
                    changes: Mutex::new(self.ingress.subscribe_route_changes()),
                    revoked: AtomicBool::new(false),
                };
                admission.check(&self.ingress)?;
                Ok(admission)
            })
            .await
            .as_ref()
            .map_err(Clone::clone)
    }
    /// Read an existing key or registration file, never creating or replacing it.
    /// Returns identity/route, deadline/revocation, response or size-limit errors.
    /// The limit is enforced on received bytes, even without Content-Length.
    pub async fn read_file(&self, path: &str, limit: usize) -> Result<Bytes, ReadError> {
        if limit == 0 || limit > MAX_READ_BYTES {
            return Err(ReadError::InvalidLimit);
        }
        let path = absolute_path(path)?;
        let query = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs([("path", path.as_str()), ("type", "file")])
            .finish();
        self.read(
            "GET",
            &format!("/download?{query}"),
            Bytes::new(),
            limit,
            None,
        )
        .await
    }

    /// Store a complete bounded body in this authorized Sandbox. Execd first
    /// writes a private part and only replaces the target after a checked commit.
    pub async fn write_file(&self, path: &str, bytes: Bytes) -> Result<usize, ReadError> {
        let path = absolute_path(path)?;
        let size = bytes.len();
        let upload_id = uuid::Uuid::new_v4().to_string();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs([
                ("path", path.as_str()),
                ("type", "file"),
                ("uploadId", upload_id.as_str()),
                ("offset", "0"),
            ])
            .finish();
        let response = self
            .read(
                "POST",
                &format!("/upload?{query}"),
                bytes,
                MAX_READ_BYTES,
                None,
            )
            .await?;
        let value: Value = serde_json::from_slice(&response).map_err(|_| ReadError::Protocol)?;
        if value.get("error") != Some(&Value::Null)
            || value.get("path").and_then(Value::as_str) != Some(path.as_str())
            || value.get("offset").and_then(Value::as_u64) != Some(size as u64)
            || value.get("committed").and_then(Value::as_bool) != Some(false)
        {
            return Err(ReadError::Protocol);
        }
        let total_size = size.to_string();
        let query = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs([
                ("path", path.as_str()),
                ("uploadId", upload_id.as_str()),
                ("totalSize", total_size.as_str()),
            ])
            .finish();
        let response = self
            .read(
                "POST",
                &format!("/upload/commit?{query}"),
                Bytes::new(),
                MAX_READ_BYTES,
                None,
            )
            .await?;
        let value: Value = serde_json::from_slice(&response).map_err(|_| ReadError::Protocol)?;
        if value.get("error") != Some(&Value::Null)
            || value.get("path").and_then(Value::as_str) != Some(path.as_str())
            || value.get("bytes_written").and_then(Value::as_u64) != Some(size as u64)
            || value.get("committed").and_then(Value::as_bool) != Some(true)
        {
            return Err(ReadError::Protocol);
        }
        Ok(size)
    }

    /// Read Execd's path metadata. This is not an open-file/fstat guarantee:
    /// fs_get_info labels directories and final symlinks separately, while the
    /// subsequent /download checks the followed path again before opening it.
    /// Returns read errors or Protocol for malformed/failed metadata responses.
    pub async fn metadata(&self, path: &str) -> Result<FileMetadata, ReadError> {
        let path = absolute_path(path)?;
        let body = Bytes::from(json!({"action":"fs_get_info", "args":{"path":path}}).to_string());
        let bytes = self
            .read("POST", "/invoke", body, MAX_READ_BYTES, None)
            .await?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| ReadError::Protocol)?;
        if value.get("error") != Some(&Value::Null) {
            return Err(ReadError::Protocol);
        }
        let regular_file = match value.get("type").and_then(Value::as_str) {
            Some("file") => true,
            Some("dir" | "symlink") => false,
            _ => return Err(ReadError::Protocol),
        };
        let size = value
            .get("size")
            .and_then(Value::as_u64)
            .ok_or(ReadError::Protocol)?;
        Ok(FileMetadata { regular_file, size })
    }

    pub(crate) async fn read(
        &self,
        method: &str,
        uri: &str,
        body: Bytes,
        limit: usize,
        range: Option<(u64, u64, u64)>,
    ) -> Result<Bytes, ReadError> {
        let deadline = self.deadline;
        if Instant::now() >= deadline {
            return Err(ReadError::Timeout);
        }
        let operation = async {
            let access = self
                .ingress
                .execd_access
                .as_ref()
                .ok_or(ReadError::Runtime)?;
            let admission = self.admit(access).await?;
            admission.check(&self.ingress)?;
            let trace = uuid::Uuid::new_v4().to_string();
            let mut route = admission.route.clone();
            route.target.request_id = trace.clone();
            self.ingress
                .authorize(&route, &self.tenant)
                .map_err(|_| ReadError::NotFound)?;
            let stream = self
                .ingress
                .open_pinned_stream(route)
                .await
                .map_err(|_| ReadError::Route)?;
            admission.check(&self.ingress)?;
            let cancelled = stream.cancellation();
            let exchange = async {
                let (mut sender, connection) =
                    hyper::client::conn::http1::handshake::<_, Full<Bytes>>(TokioIo::new(stream))
                        .await
                        .map_err(|_| ReadError::Protocol)?;
                let request = Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("host", "execd.internal")
                    .header("x-auth", &access.token)
                    .header("x-trace-id", trace)
                    .header("connection", "close")
                    .header("content-type", "application/json")
                    .header("content-length", body.len());
                let request = if let Some((start, end, _)) = range {
                    request.header("range", format!("bytes={start}-{end}"))
                } else {
                    request
                };
                let request = request
                    .body(Full::new(body))
                    .map_err(|_| ReadError::Runtime)?;
                let response = async {
                    let response = sender
                        .send_request(request)
                        .await
                        .map_err(|_| ReadError::Protocol)?;
                    match response.status() {
                        StatusCode::OK if range.is_none() => (),
                        StatusCode::PARTIAL_CONTENT if range.is_some() => (),
                        StatusCode::NOT_FOUND => return Err(ReadError::NotFound),
                        _ => return Err(ReadError::Protocol),
                    }
                    if let Some((start, end, size)) = range {
                        let expected = format!("bytes {start}-{end}/{size}");
                        if response
                            .headers()
                            .get(http::header::CONTENT_RANGE)
                            .and_then(|h| h.to_str().ok())
                            != Some(expected.as_str())
                        {
                            return Err(ReadError::Protocol);
                        }
                    }
                    if response
                        .headers()
                        .get(http::header::CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .is_some_and(|length| length > limit as u64)
                    {
                        return Err(if range.is_some() {
                            ReadError::Protocol
                        } else {
                            ReadError::TooLarge
                        });
                    }
                    Limited::new(response.into_body(), limit)
                        .collect()
                        .await
                        .map(|body| body.to_bytes())
                        .map_err(|error| {
                            if range.is_none() && error.is::<http_body_util::LengthLimitError>() {
                                ReadError::TooLarge
                            } else {
                                ReadError::Protocol
                            }
                        })
                };
                tokio::pin!(response);
                // Drive HTTP in this future: cancellation drops the connection and
                // its SessionGuard immediately, with no detached connection task.
                tokio::select! {
                    result = &mut response => result,
                    result = connection => {
                        result.map_err(|_| ReadError::Protocol)?;
                        response.await
                    }
                }
            };
            let result = tokio::select! {
                biased;
                _ = wait_for_route_cancellation(Some(cancelled)) => Err(ReadError::Revoked),
                result = exchange => result,
            };
            admission.check(&self.ingress)?;
            result
        };
        tokio::time::timeout_at(deadline, operation)
            .await
            .map_err(|_| ReadError::Timeout)?
    }
}

fn absolute_path(path: &str) -> Result<String, ReadError> {
    if !path.starts_with('/') || path.contains('\0') || path.split('/').any(|p| p == "..") {
        return Err(ReadError::InvalidPath);
    }
    Ok(format!(
        "/{}",
        path.split('/')
            .filter(|p| !p.is_empty() && *p != ".")
            .collect::<Vec<_>>()
            .join("/")
    ))
}
