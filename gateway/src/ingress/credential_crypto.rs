//! Workspace-authorized access to one operator-selected, ADX-managed crypto service.
use super::{
    agent_service::{AgentV2Access, ServiceSelector},
    server::{wait_for_route_cancellation, Ingress, IngressSecurity, ProxyBody},
};
use adx_agent_api::{managed::ManagedService, request::RequestContext};
use adx_agent_core::{
    service_grants::{credential_digest, ServiceAccess as Context, VerifyServiceGrant},
    Protocol, Scope,
};
use adx_credential_crypto::storage::{read_private, StorageError};
use bytes::Bytes;
use http::{header, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use rustls::pki_types::ServerName;
use serde::Deserialize;
use std::{convert::Infallible, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio_rustls::TlsConnector;

const BODY_LIMIT: usize = 524_288;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialCryptoConfig {
    /// Operator-selected service identity, independent of the caller's workspace grant.
    pub scope: Scope,
    pub port: u16,
    pub server_name: String,
    pub ca_file: PathBuf,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}
fn default_timeout() -> u64 {
    60
}

pub struct CredentialCryptoApi {
    config: CredentialCryptoConfig,
    access: AgentV2Access,
    managed: Arc<ManagedService>,
    tls: TlsConnector,
    server_name: ServerName<'static>,
    host: HeaderValue,
    slots: Semaphore,
}

struct ApiError(StatusCode, &'static str);
impl ApiError {
    fn unavailable() -> Self {
        Self(StatusCode::SERVICE_UNAVAILABLE, "CRYPTO_UNAVAILABLE")
    }
    fn response(self) -> Response<ProxyBody> {
        response(
            self.0,
            Bytes::from(serde_json::json!({"code": self.1}).to_string()),
        )
    }
}
fn response(status: StatusCode, body: Bytes) -> Response<ProxyBody> {
    let mut response = Response::new(
        Full::new(body)
            .map_err(
                |never: Infallible| -> Box<dyn std::error::Error + Send + Sync> { match never {} },
            )
            .boxed_unsync(),
    );
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}
impl CredentialCryptoApi {
    /// Rejects invalid scope, TLS trust or timeout configuration.
    pub fn new(
        config: CredentialCryptoConfig,
        managed: Arc<ManagedService>,
    ) -> Result<Self, StorageError> {
        config.scope.validate().map_err(|_| StorageError::Invalid)?;
        if config.port == 0 || !(1..=300).contains(&config.timeout_seconds) {
            return Err(StorageError::Invalid);
        }
        let server_name =
            ServerName::try_from(config.server_name.clone()).map_err(|_| StorageError::Invalid)?;
        let host_name = if config.server_name.contains(':') {
            format!("[{}]:{}", config.server_name, config.port)
        } else {
            format!("{}:{}", config.server_name, config.port)
        };
        let host = HeaderValue::from_str(&host_name).map_err(|_| StorageError::Invalid)?;
        let tls = adx_transport::tls::http_client_config(
            &read_private(&config.ca_file, 65_536)?,
            None,
            vec![b"http/1.1".to_vec()],
        )
        .map_err(|_| StorageError::Invalid)?;
        Ok(Self {
            config,
            access: AgentV2Access::new(managed.clone()),
            managed,
            tls: TlsConnector::from(Arc::new(tls)),
            server_name,
            host,
            slots: Semaphore::new(128),
        })
    }
    pub fn matches(path: &str) -> bool {
        matches!(path, "/v1/seal" | "/v1/open")
    }
    pub(super) async fn handle(
        &self,
        request: Request<Incoming>,
        ingress: Arc<Ingress>,
        security: IngressSecurity,
    ) -> Response<ProxyBody> {
        let context = Arc::new(RequestContext::new(Duration::from_secs(
            self.config.timeout_seconds,
        )));
        match tokio::time::timeout_at(
            context.deadline(),
            self.forward(request, ingress, security, context.clone()),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => error.response(),
            Err(_) => ApiError::unavailable().response(),
        }
    }
    async fn forward(
        &self,
        request: Request<Incoming>,
        ingress: Arc<Ingress>,
        security: IngressSecurity,
        context: Arc<RequestContext>,
    ) -> Result<Response<ProxyBody>, ApiError> {
        if security != IngressSecurity::Tls {
            return Err(ApiError(StatusCode::UPGRADE_REQUIRED, "TLS_REQUIRED"));
        }
        if request.method() != Method::POST {
            return Err(ApiError(
                StatusCode::METHOD_NOT_ALLOWED,
                "METHOD_NOT_ALLOWED",
            ));
        }
        if request.uri().query().is_some() || request.headers().contains_key(header::UPGRADE) {
            return Err(ApiError(StatusCode::BAD_REQUEST, "INVALID_REQUEST"));
        }
        let mut authorization = request.headers().get_all(header::AUTHORIZATION).iter();
        let bearer = authorization
            .next()
            .cloned()
            .filter(|_| authorization.next().is_none())
            .ok_or(ApiError(StatusCode::UNAUTHORIZED, "UNAUTHORIZED"))?;
        let credential = bearer
            .to_str()
            .ok()
            .and_then(|value| value.strip_prefix("Bearer "))
            .filter(|value| credential_digest(value).is_ok())
            .ok_or(ApiError(StatusCode::UNAUTHORIZED, "UNAUTHORIZED"))?;
        let _permit = self
            .slots
            .try_acquire()
            .map_err(|_| ApiError::unavailable())?;
        let path = request.uri().path().to_owned();
        let body = Limited::new(request.into_body(), BODY_LIMIT)
            .collect()
            .await
            .map_err(|_| ApiError(StatusCode::PAYLOAD_TOO_LARGE, "INVALID_REQUEST"))?
            .to_bytes();
        // Read only the context for admission. The crypto service validates the full request.
        #[derive(Deserialize)]
        struct Admission {
            context: Context,
        }
        let admission: Admission = serde_json::from_slice(&body)
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "INVALID_REQUEST"))?;
        admission
            .context
            .validate()
            .map_err(|_| ApiError(StatusCode::BAD_REQUEST, "INVALID_REQUEST"))?;
        if !self
            .managed
            .verify_service_grant(
                &context,
                &VerifyServiceGrant {
                    credential: credential.into(),
                    expected: admission.context,
                },
            )
            .await
            .map_err(|_| ApiError(StatusCode::SERVICE_UNAVAILABLE, "AUTHORITY_UNAVAILABLE"))?
        {
            return Err(ApiError(StatusCode::FORBIDDEN, "FORBIDDEN"));
        }
        // Authority verification precedes any activation. Callers cannot select a service target.
        let selection = self
            .access
            .select(
                context,
                self.config.scope.clone(),
                ServiceSelector {
                    protocol: Protocol::Http,
                    port: Some(self.config.port),
                },
            )
            .await
            .map_err(|_| ApiError::unavailable())?;
        let stream = self
            .access
            .connect_service(&ingress, selection)
            .await
            .map_err(|_| ApiError::unavailable())?;
        let key = stream
            .http_pool_key()
            .map_err(|_| ApiError::unavailable())?;
        let cancelled = Some(stream.cancellation());
        let exchange = async {
            let tls = self
                .tls
                .connect(self.server_name.clone(), stream)
                .await
                .map_err(|_| ApiError::unavailable())?;
            let mut upstream = Request::new(Full::new(body));
            *upstream.method_mut() = Method::POST;
            *upstream.uri_mut() = path.parse().map_err(|_| ApiError::unavailable())?;
            upstream
                .headers_mut()
                .insert(header::HOST, self.host.clone());
            upstream.headers_mut().insert(header::AUTHORIZATION, bearer);
            upstream.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            upstream
                .headers_mut()
                .insert(header::CONNECTION, HeaderValue::from_static("close"));
            let reply = ingress
                .http_pool
                .send(key, upstream, || async { Ok(tls) })
                .await
                .map_err(|_| ApiError::unavailable())?;
            let status = reply.status();
            let body = Limited::new(reply.into_body(), BODY_LIMIT)
                .collect()
                .await
                .map_err(|_| ApiError::unavailable())?
                .to_bytes();
            Ok(response(status, body))
        };
        tokio::select! {
            _ = wait_for_route_cancellation(cancelled) => Err(ApiError::unavailable()),
            result = exchange => result,
        }
    }
}
