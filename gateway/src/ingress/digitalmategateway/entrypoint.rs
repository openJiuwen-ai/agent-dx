//! Authenticated Jiuwen business entrypoints. ADX management credentials are separate.
use super::{
    connection::{Connection, ConnectionError},
    download_config::{absolute_directory, DownloadConfig},
    download_http::Download,
    driver::{self, DriverConfig},
    frontend::{self, FrontendConfig},
    session::SessionLimits,
    upload_http::Upload,
};
use crate::ingress::accounts::{self, BusinessAuth, Principal};
use crate::ingress::{
    agent_service::{AgentV2Access, ServiceAccessError, ServiceSelector},
    server::{Ingress, ProxyBody},
};
use adx_agent_api::{managed::ManagedService, request::RequestContext, Error};
use adx_agent_core::{Protocol, Scope};
use bytes::Bytes;
use http::{header, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited, StreamBody};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio_tungstenite::{
    tungstenite::{handshake::server::create_response, protocol::Role},
    WebSocketStream,
};
use tracing::Instrument;

/// Select how a trusted business identity maps to a pre-mounted workspace.
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceIdentity {
    #[default]
    Scoped,
    /// Existing AgentOS user directory; template deployment selects the tenant's root.
    User,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DigitalMateGatewayConfig {
    pub tenant: String,
    pub jiuwen: JiuwenTarget,
    pub orbit: Option<super::orbit_ws::OrbitWsConfig>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JiuwenTarget {
    pub agent_type: String,
    pub template: String,
    pub version: String,
    #[serde(default)]
    pub workspace_identity: WorkspaceIdentity,
    pub service_port: Option<u16>,
    #[serde(default = "queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "request_timeout")]
    pub request_timeout_seconds: u64,
    #[serde(default = "download_timeout")]
    pub download_timeout_seconds: u64,
    #[serde(default = "write_timeout")]
    pub write_timeout_seconds: u64,
    #[serde(default = "output_queue_timeout")]
    pub output_queue_timeout_seconds: u64,
    #[serde(default = "idle_timeout")]
    pub idle_timeout_seconds: u64,
    #[serde(default = "ping_interval")]
    pub ping_interval_seconds: u64,
}
fn queue_capacity() -> usize {
    64
}
/// Shared business workspace identity, independent of template version.
pub(super) fn workspace_parameters(
    tenant: &str,
    template: &str,
    user_id: &str,
) -> Result<adx_agent_core::mounts::Parameters, Error> {
    let tuple = serde_json::to_vec(&[tenant, template, user_id]).map_err(|_| invalid())?;
    Ok(std::collections::BTreeMap::from([(
        "workspace".into(),
        format!("w-{}", hex::encode(Sha256::digest(tuple))),
    )]))
}

pub(super) fn request_timeout() -> u64 {
    60
}
fn download_timeout() -> u64 {
    300
}
fn write_timeout() -> u64 {
    15
}
fn output_queue_timeout() -> u64 {
    5
}
fn idle_timeout() -> u64 {
    90
}
fn ping_interval() -> u64 {
    20
}
fn invalid() -> Error {
    Error::Invalid("invalid DigitalMateGateway configuration".into())
}
impl DigitalMateGatewayConfig {
    /// Identity comes only from the authenticated business session.
    /// Template version stays outside binding_id, inside the complete binding scope.
    pub fn scope(&self, user_id: &str) -> Result<Scope, Error> {
        for value in [
            &self.tenant,
            user_id,
            &self.jiuwen.agent_type,
            &self.jiuwen.template,
            &self.jiuwen.version,
        ] {
            adx_agent_core::identifier(value, "Jiuwen identity").map_err(Error::Invalid)?;
        }
        if user_id.len() > 256 {
            return Err(invalid());
        }
        let tuple = serde_json::to_vec(&[&self.tenant, user_id, &self.jiuwen.agent_type])
            .map_err(|_| invalid())?;
        Ok(Scope {
            tenant: self.tenant.clone(),
            template: self.jiuwen.template.clone(),
            version: self.jiuwen.version.clone(),
            binding_id: format!("jiuwen-{}", hex::encode(Sha256::digest(tuple))),
        })
    }
    /// Stable workspace identity across template versions and agent types.
    pub fn parameters(&self, user_id: &str) -> Result<adx_agent_core::mounts::Parameters, Error> {
        self.scope(user_id)?;
        if matches!(self.jiuwen.workspace_identity, WorkspaceIdentity::User) {
            // Do not silently sanitize collisions or traverse outside the configured root.
            if matches!(user_id, "." | "..")
                || !user_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            {
                return Err(Error::Invalid(
                    "workspace user requires a safe directory component".into(),
                ));
            }
            return Ok(std::collections::BTreeMap::from([(
                "workspace".into(),
                user_id.into(),
            )]));
        }
        workspace_parameters(&self.tenant, &self.jiuwen.template, user_id)
    }

    pub fn validate(&self) -> Result<(), Error> {
        if let Some(orbit) = &self.orbit {
            orbit.validate(&self.tenant)?;
        }
        self.scope("validation")?
            .validate()
            .map_err(Error::Invalid)?;
        if self.jiuwen.service_port == Some(0)
            || !(1..=1024).contains(&self.jiuwen.queue_capacity)
            || self.jiuwen.ping_interval_seconds >= self.jiuwen.idle_timeout_seconds
        {
            return Err(invalid());
        }
        for seconds in [
            self.jiuwen.request_timeout_seconds,
            self.jiuwen.download_timeout_seconds,
            self.jiuwen.write_timeout_seconds,
            self.jiuwen.output_queue_timeout_seconds,
            self.jiuwen.idle_timeout_seconds,
            self.jiuwen.ping_interval_seconds,
        ] {
            if seconds == 0 || seconds > 86400 {
                return Err(invalid());
            }
        }
        Ok(())
    }
    fn frontend(&self) -> FrontendConfig {
        FrontendConfig {
            queue_capacity: self.jiuwen.queue_capacity,
            write_timeout: Duration::from_secs(self.jiuwen.write_timeout_seconds),
            idle_timeout: Duration::from_secs(self.jiuwen.idle_timeout_seconds),
            ping_interval: Duration::from_secs(self.jiuwen.ping_interval_seconds),
        }
    }
    fn driver(&self) -> DriverConfig {
        DriverConfig {
            limits: SessionLimits {
                pending: self.jiuwen.queue_capacity,
                recent: 1024,
                busy: 1024,
            },
            write_timeout: Duration::from_secs(self.jiuwen.write_timeout_seconds),
            output_queue_timeout: Duration::from_secs(self.jiuwen.output_queue_timeout_seconds),
            idle_timeout: Duration::from_secs(self.jiuwen.idle_timeout_seconds),
            ping_interval: Duration::from_secs(self.jiuwen.ping_interval_seconds),
            request_timeout: Duration::from_secs(self.jiuwen.request_timeout_seconds),
        }
    }
}
pub struct DigitalMateGatewayApi {
    config: DigitalMateGatewayConfig,
    auth: Arc<dyn BusinessAuth>,
    managed: Arc<ManagedService>,
}
impl DigitalMateGatewayApi {
    pub fn new(
        config: DigitalMateGatewayConfig,
        managed: Arc<ManagedService>,
        auth: Arc<dyn BusinessAuth>,
    ) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self {
            auth,
            config,
            managed,
        })
    }
    pub fn matches(path: &str) -> bool {
        matches!(
            path,
            "/ws"
                | "/orbit/ws"
                | "/file-api/download"
                | "/file-api/upload"
                | "/api/v1/auth/huawei/login"
                | "/api/v1/auth/agc/login"
                | "/api/v1/auth/session"
                | "/api/v1/auth/logout"
                | "/api/v1/auth/quota"
        )
    }
    pub(crate) async fn handle(
        &self,
        mut request: Request<Incoming>,
        ingress: Arc<Ingress>,
    ) -> Response<ProxyBody> {
        if request.uri().path() == "/orbit/ws" && self.config.orbit.is_none() {
            return error(StatusCode::NOT_FOUND, "NOT_FOUND");
        }
        if matches!(
            request.uri().path(),
            "/api/v1/auth/huawei/login" | "/api/v1/auth/agc/login" | "/api/v1/auth/logout"
        ) {
            return self.auth_request(request).await;
        }
        let token = match accounts::bearer(request.headers()) {
            Ok(v) => v,
            Err(e) => return auth_error(e),
        };
        let principal = match tokio::time::timeout(
            Duration::from_secs(15),
            self.auth.authenticate(token),
        )
        .await
        {
            Ok(Ok(p)) => p,
            Ok(Err(e)) => return auth_error(e),
            _ => return auth_error(accounts::Error::Unavailable),
        };
        if let Some(orbit) = self
            .config
            .orbit
            .as_ref()
            .filter(|_| request.uri().path() == "/orbit/ws")
        {
            return orbit
                .handle(
                    request,
                    ingress,
                    self.managed.clone(),
                    self.auth.clone(),
                    principal,
                    &self.config.tenant,
                )
                .await;
        }
        if request.uri().path() == "/api/v1/auth/session" {
            if request.method() != http::Method::GET || request.uri().query().is_some() {
                return error(StatusCode::BAD_REQUEST, "INVALID_REQUEST");
            }
            return Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .header("cache-control", "no-store")
                .body(body(Bytes::from(
                    serde_json::json!({"userId": principal.user_id}).to_string(),
                )))
                .unwrap_or_else(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "RESPONSE_ERROR"));
        }
        if request.uri().path() == "/api/v1/auth/quota" {
            if request.method() != http::Method::GET {
                return error(StatusCode::METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED");
            }
            if request.uri().query().is_some() {
                return error(StatusCode::BAD_REQUEST, "INVALID_QUERY");
            }
            return match tokio::time::timeout(Duration::from_secs(30), self.auth.quota(&principal))
                .await
            {
                Ok(Ok(quota)) => match serde_json::to_vec(&quota) {
                    Ok(bytes) => Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/json")
                        .header("cache-control", "no-store")
                        .body(body(Bytes::from(bytes)))
                        .unwrap_or_else(|_| {
                            error(StatusCode::INTERNAL_SERVER_ERROR, "RESPONSE_ERROR")
                        }),
                    Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "RESPONSE_ERROR"),
                },
                Ok(Err(e)) => auth_error(e),
                Err(_) => auth_error(accounts::Error::Unavailable),
            };
        }
        let scope = match self.config.scope(&principal.user_id) {
            Ok(s) => s,
            Err(e) => return agent_error(e),
        };
        if request.uri().path() == "/file-api/download" {
            return self.download(request, ingress, scope, principal).await;
        }
        if request.uri().path() == "/file-api/upload" {
            return self.upload(request, ingress, scope, principal).await;
        }
        // Client user_id is only an ignored hint. The authenticated session owns
        // both binding selection and the user identity sent to AgentServer.
        if request.uri().query().is_some_and(|query| {
            query.len() > 1024
                || url::form_urlencoded::parse(query.as_bytes()).any(|(name, _)| name != "user_id")
        }) {
            return error(StatusCode::BAD_REQUEST, "INVALID_QUERY");
        }
        if request.method() != http::Method::GET {
            return error(StatusCode::METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED");
        }
        // Validate HTTP upgrade before activating any Environment.
        let mut handshake = Request::builder()
            .method(request.method())
            .uri(request.uri())
            .version(request.version())
            .body(())
            .expect("request parts are valid");
        *handshake.headers_mut() = request.headers().clone();
        let response = match create_response(&handshake) {
            Ok(response) => response,
            Err(_) => return error(StatusCode::BAD_REQUEST, "INVALID_WEBSOCKET_HANDSHAKE"),
        };
        let context = Arc::new(RequestContext::new(Duration::from_secs(
            self.config.jiuwen.request_timeout_seconds,
        )));
        let access = AgentV2Access::new(self.managed.clone());
        let initialization = async {
            let mut parameters = self.auth.model_parameters(&principal).await?;
            parameters.extend(
                self.config
                    .parameters(&principal.user_id)
                    .map_err(|_| accounts::Error::Invalid)?,
            );
            Ok::<_, accounts::Error>(parameters)
        };
        let selection = match access
            .select_with_initializer(
                context.clone(),
                scope.clone(),
                ServiceSelector {
                    protocol: Protocol::Ws,
                    port: self.config.jiuwen.service_port,
                },
                initialization,
            )
            .await
        {
            Ok(value) => value,
            Err(crate::ingress::agent_service::InitializationError::Agent(error)) => {
                return agent_error(error)
            }
            Err(crate::ingress::agent_service::InitializationError::Source(error)) => {
                return auth_error(error)
            }
        };
        let connection = match Connection::connect(&ingress, &access, selection).await {
            Ok(value) => value,
            Err(ConnectionError::Access(ServiceAccessError::Agent(e))) => return agent_error(e),
            Err(_) => return error(StatusCode::BAD_GATEWAY, "AGENT_SERVER_UNAVAILABLE"),
        };
        if let Err(e) = self.auth.check(&principal).await {
            return auth_error(e);
        }
        let auth = self.auth.clone();
        let upgrade = hyper::upgrade::on(&mut request);
        let config = self.config.clone();
        let span = tracing::info_span!("jiuwen_ws", connection_id = %uuid::Uuid::new_v4());
        let task = async move {
            let Ok(Ok(upgraded)) = tokio::time::timeout_at(context.deadline(), upgrade).await
            else {
                return;
            };
            let socket = WebSocketStream::from_raw_socket(
                TokioIo::new(upgraded),
                Role::Server,
                Some(driver::websocket_config()),
            )
            .await;
            let front = config.frontend();
            let driver = config.driver();
            let user_id = principal.user_id.clone();
            let guard = accounts::SessionGuard { auth, principal };
            let _ = frontend::run_authenticated(
                socket,
                front,
                |requests, output| connection.run(user_id, driver, requests, output),
                guard,
            )
            .await;
        };
        tokio::spawn(task.instrument(span));
        let (parts, _) = response.into_parts();
        Response::from_parts(parts, body(Bytes::new()))
    }
    async fn auth_request(&self, request: Request<Incoming>) -> Response<ProxyBody> {
        if request.method() != http::Method::POST {
            return error(StatusCode::METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED");
        }
        if request.uri().query().is_some() {
            return error(StatusCode::BAD_REQUEST, "INVALID_QUERY");
        }
        if request.uri().path() == "/api/v1/auth/logout" {
            let token = match accounts::bearer(request.headers()) {
                Ok(v) => v,
                Err(e) => return auth_error(e),
            };
            return match tokio::time::timeout(Duration::from_secs(15), self.auth.logout(token))
                .await
            {
                Ok(Ok(())) => Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .header("cache-control", "no-store")
                    .body(body(Bytes::new()))
                    .unwrap_or_else(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "RESPONSE_ERROR")),
                Ok(Err(e)) => auth_error(e),
                Err(_) => auth_error(accounts::Error::Unavailable),
            };
        }
        if request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_none_or(|v| v.split(';').next() != Some("application/json"))
        {
            return error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "JSON_REQUIRED");
        }
        let agc = request.uri().path() == "/api/v1/auth/agc/login";
        let bytes = match tokio::time::timeout(
            Duration::from_secs(10),
            Limited::new(request.into_body(), 16384).collect(),
        )
        .await
        {
            Ok(Ok(v)) => v.to_bytes(),
            _ => return error(StatusCode::BAD_REQUEST, "INVALID_LOGIN_REQUEST"),
        };
        let result = if agc {
            let input: accounts::AgcLoginRequest = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(_) => return auth_error(accounts::Error::Invalid),
            };
            tokio::time::timeout(Duration::from_secs(45), self.auth.agc_login(input)).await
        } else {
            let input: accounts::LoginRequest = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(_) => return auth_error(accounts::Error::Invalid),
            };
            tokio::time::timeout(Duration::from_secs(45), self.auth.login(input)).await
        };
        match result {
            Ok(Ok(value)) => match serde_json::to_vec(&value) {
                Ok(bytes) => Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/json")
                    .header("cache-control", "no-store")
                    .body(body(Bytes::from(bytes)))
                    .unwrap_or_else(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "RESPONSE_ERROR")),
                Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "RESPONSE_ERROR"),
            },
            Ok(Err(e)) => auth_error(e),
            Err(_) => auth_error(accounts::Error::Unavailable),
        }
    }
    async fn upload(
        &self,
        request: Request<Incoming>,
        ingress: Arc<Ingress>,
        scope: Scope,
        principal: Principal,
    ) -> Response<ProxyBody> {
        if request.method() != http::Method::POST {
            return error(StatusCode::METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED");
        }
        let content_type = match request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
        {
            Some(value)
                if value.split(';').next().is_some_and(|kind| {
                    kind.trim().eq_ignore_ascii_case("multipart/form-data")
                }) =>
            {
                value.to_owned()
            }
            _ => return error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "MULTIPART_REQUIRED"),
        };
        if request
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
            .is_some_and(|size| size > super::upload_http::MAX_UPLOAD_BYTES)
        {
            return error(StatusCode::PAYLOAD_TOO_LARGE, "PAYLOAD_TOO_LARGE");
        }
        let header_user_id = request
            .headers()
            .get("x-user-id")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if header_user_id
            .as_deref()
            .is_some_and(|id| !id.is_empty() && id != principal.user_id)
        {
            return error(StatusCode::FORBIDDEN, "USER_MISMATCH");
        }
        let context = Arc::new(RequestContext::new(Duration::from_secs(
            self.config.jiuwen.download_timeout_seconds,
        )));
        let template = match self
            .managed
            .template(&context, &scope.tenant, &scope.template, &scope.version)
            .await
        {
            Ok(value) => value,
            Err(e) => return agent_error(e),
        };
        let workspace = match absolute_directory(
            template
                .env
                .get("JIUWENSWARM_WORKSPACE")
                .map(String::as_str),
            "JIUWENSWARM_WORKSPACE",
        ) {
            Ok(value) => value,
            Err(e) => return agent_error(e),
        };
        let target = match context.run(self.managed.binding(&context, &scope)).await {
            Ok(target) if target.phase == adx_agent_core::BindingPhase::Active => target,
            Ok(_) => return error(StatusCode::CONFLICT, "BINDING_DELETING"),
            Err(e) => return agent_error(e),
        };
        let user_id = principal.user_id.clone();
        let guard = accounts::SessionGuard {
            auth: self.auth.clone(),
            principal,
        };
        let upload = Upload {
            ingress,
            context,
            tenant: scope.tenant,
            environment_id: target.environment_id,
            workspace,
            user_id,
            agent_type: self.config.jiuwen.agent_type.clone(),
        };
        tokio::select! {
            response = upload.respond(request.into_body(), &content_type) => response,
            _ = guard.watch() => error(StatusCode::UNAUTHORIZED, "SESSION_ENDED"),
        }
    }
    async fn download(
        &self,
        request: Request<Incoming>,
        ingress: Arc<Ingress>,
        scope: Scope,
        principal: Principal,
    ) -> Response<ProxyBody> {
        if !matches!(*request.method(), http::Method::GET | http::Method::HEAD) {
            return error(StatusCode::METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED");
        }
        let mut token = None;
        let mut inline = false;
        let query = request.uri().query().unwrap_or("");
        if query.len() > super::download_token::MAX_TOKEN_BYTES * 3 + 128 {
            return error(StatusCode::BAD_REQUEST, "INVALID_QUERY");
        }
        for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match name.as_ref() {
                "token" if token.is_none() => token = Some(value.into_owned()),
                "inline" => inline = matches!(value.as_ref(), "1" | "true"),
                // Original AgentServer links can carry this routing hint. It is
                // never an identity source and cannot select another binding.
                "user_id" if value == principal.user_id => {}
                "user_id" => return error(StatusCode::FORBIDDEN, "USER_MISMATCH"),
                _ => return error(StatusCode::BAD_REQUEST, "INVALID_QUERY"),
            }
        }
        let Some(token) = token.filter(|v| !v.is_empty()) else {
            return error(StatusCode::BAD_REQUEST, "MISSING_TOKEN");
        };
        let context = Arc::new(RequestContext::new(Duration::from_secs(
            self.config.jiuwen.download_timeout_seconds,
        )));
        let config = match DownloadConfig::load(&self.managed, &context, &scope).await {
            Ok(v) => v,
            Err(e) => return agent_error(e),
        };
        let target = match context.run(self.managed.binding(&context, &scope)).await {
            Ok(target) if target.phase == adx_agent_core::BindingPhase::Active => target,
            Ok(_) => return error(StatusCode::CONFLICT, "BINDING_DELETING"),
            Err(e) => return agent_error(e),
        };
        let guard = accounts::SessionGuard {
            auth: self.auth.clone(),
            principal,
        };
        let response = Download {
            ingress,
            context,
            tenant: scope.tenant.clone(),
            environment_id: target.environment_id,
            config,
            token,
            expected_session: None,
        }
        .respond(request.method().clone(), request.headers(), inline)
        .await;
        let (parts, response_body) = response.into_parts();
        // Keep checking expiry/revocation even when the download is waiting on upstream I/O.
        let watch = Box::pin(async move { guard.watch().await });
        let frames = futures_util::stream::unfold(
            Some((response_body, watch)),
            |state| async move {
                let (mut response_body, mut watch) = state?;
                tokio::select! {
                    _ = &mut watch => Some((Err(std::io::Error::other("business session ended")),None)),
                    frame = response_body.frame() => match frame {
                        Some(Ok(frame))=>Some((Ok(frame),Some((response_body,watch)))),
                        Some(Err(_))=>Some((Err(std::io::Error::other("download interrupted")),None)),
                        None=>None,
                    }
                }
            },
        );
        Response::from_parts(
            parts,
            BodyExt::boxed_unsync(
                StreamBody::new(frames)
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) }),
            ),
        )
    }
}
pub(super) fn auth_error(error: accounts::Error) -> Response<ProxyBody> {
    self::error(error.status(), error.code())
}
pub(super) fn agent_error(e: Error) -> Response<ProxyBody> {
    let (status, code) = match e {
        Error::Invalid(_) => (StatusCode::BAD_REQUEST, "INVALID_REQUEST"),
        Error::NotFound => (StatusCode::NOT_FOUND, "NOT_FOUND"),
        Error::Conflict(_) => (StatusCode::CONFLICT, "CONFLICT"),
        Error::Unsupported(_) => (StatusCode::NOT_IMPLEMENTED, "UNSUPPORTED"),
        _ => (StatusCode::SERVICE_UNAVAILABLE, "AGENT_UNAVAILABLE"),
    };
    error(status, code)
}
fn body(bytes: Bytes) -> ProxyBody {
    Full::new(bytes)
        .map_err(|never| match never {})
        .boxed_unsync()
}
pub(super) fn error(status: StatusCode, code: &str) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(body(Bytes::from(
            serde_json::json!({"error":code,"code":code}).to_string(),
        )))
        .expect("fixed error response headers are valid")
}
