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

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JiuwenConfig {
    pub tenant: String,
    pub agent_type: String,
    pub template: String,
    pub version: String,
    pub service_port: Option<u16>,
    pub allowed_hosts: Vec<String>,
    #[serde(default)]
    pub allowed_origins: Vec<String>,
    #[serde(default = "queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "request_timeout")]
    pub request_timeout_seconds: u64,
    #[serde(default = "download_timeout")]
    pub download_timeout_seconds: u64,
    #[serde(default = "write_timeout")]
    pub write_timeout_seconds: u64,
    #[serde(default = "idle_timeout")]
    pub idle_timeout_seconds: u64,
    #[serde(default = "ping_interval")]
    pub ping_interval_seconds: u64,
}
fn queue_capacity() -> usize {
    64
}
fn request_timeout() -> u64 {
    60
}
fn download_timeout() -> u64 {
    300
}
fn write_timeout() -> u64 {
    15
}
fn idle_timeout() -> u64 {
    90
}
fn ping_interval() -> u64 {
    20
}
fn invalid() -> Error {
    Error::Invalid("invalid Jiuwen configuration".into())
}
impl JiuwenConfig {
    /// Identity comes only from the authenticated business session.
    /// Template version stays outside binding_id, inside the complete binding scope.
    pub fn scope(&self, user_id: &str) -> Result<Scope, Error> {
        for value in [
            &self.tenant,
            user_id,
            &self.agent_type,
            &self.template,
            &self.version,
        ] {
            adx_agent_core::identifier(value, "Jiuwen identity").map_err(Error::Invalid)?;
        }
        if user_id.len() > 256 {
            return Err(invalid());
        }
        let tuple = serde_json::to_vec(&[&self.tenant, user_id, &self.agent_type])
            .map_err(|_| invalid())?;
        Ok(Scope {
            tenant: self.tenant.clone(),
            template: self.template.clone(),
            version: self.version.clone(),
            binding_id: format!("jiuwen-{}", hex::encode(Sha256::digest(tuple))),
        })
    }
    pub fn validate(&self) -> Result<(), Error> {
        self.scope("validation")?
            .validate()
            .map_err(Error::Invalid)?;
        if self.allowed_hosts.is_empty()
            || self.service_port == Some(0)
            || !(1..=1024).contains(&self.queue_capacity)
            || self.ping_interval_seconds >= self.idle_timeout_seconds
        {
            return Err(invalid());
        }
        for seconds in [
            self.request_timeout_seconds,
            self.download_timeout_seconds,
            self.write_timeout_seconds,
            self.idle_timeout_seconds,
            self.ping_interval_seconds,
        ] {
            if seconds == 0 || seconds > 86400 {
                return Err(invalid());
            }
        }
        for host in &self.allowed_hosts {
            let authority: http::uri::Authority = host.parse().map_err(|_| invalid())?;
            if authority.port().is_some() || authority.host() != host || host.contains('@') {
                return Err(invalid());
            }
        }
        for origin in &self.allowed_origins {
            let url = url::Url::parse(origin).map_err(|_| invalid())?;
            if !matches!(url.scheme(), "http" | "https")
                || url.origin().ascii_serialization() != *origin
            {
                return Err(invalid());
            }
        }
        Ok(())
    }
    fn frontend(&self) -> FrontendConfig {
        FrontendConfig {
            queue_capacity: self.queue_capacity,
            write_timeout: Duration::from_secs(self.write_timeout_seconds),
            idle_timeout: Duration::from_secs(self.idle_timeout_seconds),
            ping_interval: Duration::from_secs(self.ping_interval_seconds),
        }
    }
    fn driver(&self) -> DriverConfig {
        DriverConfig {
            limits: SessionLimits {
                pending: self.queue_capacity,
                recent: 1024,
                busy: 1024,
            },
            write_timeout: Duration::from_secs(self.write_timeout_seconds),
            idle_timeout: Duration::from_secs(self.idle_timeout_seconds),
            ping_interval: Duration::from_secs(self.ping_interval_seconds),
            request_timeout: Duration::from_secs(self.request_timeout_seconds),
        }
    }
}
pub struct JiuwenApi {
    config: JiuwenConfig,
    auth: Arc<dyn BusinessAuth>,
    managed: Arc<ManagedService>,
}
impl JiuwenApi {
    pub fn new(
        config: JiuwenConfig,
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
                | "/file-api/download"
                | "/file-api/upload"
                | "/auth/huawei/login"
                | "/auth/logout"
        )
    }
    fn admitted(&self, request: &Request<Incoming>) -> bool {
        if request.headers().get_all(header::HOST).iter().count() != 1
            || request.headers().get_all(header::ORIGIN).iter().count() > 1
        {
            return false;
        }
        let host = request
            .headers()
            .get(header::HOST)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.parse::<http::uri::Authority>().ok());
        if !host.is_some_and(|h| {
            self.config
                .allowed_hosts
                .iter()
                .any(|allowed| h.host().eq_ignore_ascii_case(allowed))
        }) {
            return false;
        }
        if let Some(origin) = request.headers().get(header::ORIGIN) {
            if !origin.to_str().ok().is_some_and(|value| {
                self.config
                    .allowed_origins
                    .iter()
                    .any(|allowed| allowed == value)
            }) {
                return false;
            }
        }
        true
    }
    pub(crate) async fn handle(
        &self,
        mut request: Request<Incoming>,
        ingress: Arc<Ingress>,
    ) -> Response<ProxyBody> {
        if !self.admitted(&request) {
            return error(StatusCode::FORBIDDEN, "ACCESS_DENIED");
        }
        if matches!(request.uri().path(), "/auth/huawei/login" | "/auth/logout") {
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
            Ok(Ok(p)) if p.tenant == self.config.tenant => p,
            Ok(Err(e)) => return auth_error(e),
            _ => return auth_error(accounts::Error::Unavailable),
        };
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
        // Validate HTTP upgrade before activating any Sandbox.
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
            self.config.request_timeout_seconds,
        )));
        // Only the first binding creation provisions a credential. Warm reuse and
        // downloads are independent of the LiteLLM management API.
        match context.run(self.managed.binding(&context, &scope)).await {
            Ok(_) => {}
            Err(Error::NotFound) => {
                let launch = match tokio::time::timeout_at(
                    context.deadline(),
                    self.auth.launch_config(&principal),
                )
                .await
                {
                    Ok(Ok(v)) => v,
                    Ok(Err(e)) => return auth_error(e),
                    Err(_) => return auth_error(accounts::Error::Unavailable),
                };
                if let Err(e) = context
                    .run(self.managed.prepare_binding(&context, &scope, &launch))
                    .await
                {
                    return agent_error(e);
                }
            }
            Err(e) => return agent_error(e),
        }
        let access = AgentV2Access::new(self.managed.clone());
        let selection = match access
            .select(
                context.clone(),
                scope.clone(),
                ServiceSelector {
                    protocol: Protocol::Ws,
                    port: self.config.service_port,
                },
            )
            .await
        {
            Ok(value) => value,
            Err(e) => return agent_error(e),
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
        tokio::spawn(async move {
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
        });
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
        if request.uri().path() == "/auth/logout" {
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
        let bytes = match tokio::time::timeout(
            Duration::from_secs(10),
            Limited::new(request.into_body(), 16384).collect(),
        )
        .await
        {
            Ok(Ok(v)) => v.to_bytes(),
            _ => return error(StatusCode::BAD_REQUEST, "INVALID_LOGIN_REQUEST"),
        };
        let input = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(_) => return auth_error(accounts::Error::Invalid),
        };
        match tokio::time::timeout(Duration::from_secs(45), self.auth.login(input)).await {
            Ok(Ok(v)) => match serde_json::to_vec(&v) {
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
            self.config.download_timeout_seconds,
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
            sandbox_id: target.sandbox_id,
            workspace,
            user_id,
            agent_type: self.config.agent_type.clone(),
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
            self.config.download_timeout_seconds,
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
            sandbox_id: target.sandbox_id,
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
fn auth_error(error: accounts::Error) -> Response<ProxyBody> {
    self::error(error.status(), error.code())
}
fn agent_error(e: Error) -> Response<ProxyBody> {
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
fn error(status: StatusCode, code: &str) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .header("cache-control", "no-store")
        .body(body(Bytes::from(
            serde_json::json!({"error":code,"code":code}).to_string(),
        )))
        .expect("fixed error response headers are valid")
}
