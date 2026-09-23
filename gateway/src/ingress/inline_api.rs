//! Yuanrong Frontend inline compatibility entrypoint, independent of the v2 Agent API.
use super::{agent_response::json_response, inline_auth::InlineAuth};
use adx_agent_api::request::RequestContext;
use adx_agent_api::{
    management::{InlineProfile, InlineService, Options},
    Error,
};
use adx_agent_core::{inline::CreateRequest, limits, sandbox::Sandbox};
use http::{Request, Response, StatusCode};
use http_body_util::BodyExt;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InlineConfig {
    pub inline_profiles: Vec<InlineProfile>,
    pub backend_timeout_seconds: u64,
    pub max_inflight: usize,
    pub iam_address: String,
}

pub struct InlineApi {
    pub service: Arc<InlineService>,
    auth: InlineAuth,
}
impl InlineApi {
    /// Configure the compatibility adapter and its independent IAM verifier.
    /// Returns an error for invalid profiles or an invalid IAM address.
    pub fn new(
        config: InlineConfig,
        sandbox: Arc<dyn Sandbox>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let auth = InlineAuth::new(config.iam_address)?;
        let service = Arc::new(InlineService::new(
            sandbox,
            Options {
                profiles: config.inline_profiles,
                backend_timeout: Duration::from_secs(config.backend_timeout_seconds),
                max_inflight: config.max_inflight,
            },
        )?);
        Ok(Self { service, auth })
    }
    pub(super) async fn authenticate_data<B>(
        &self,
        request: &Request<B>,
    ) -> Result<super::auth::AuthenticatedIdentity, super::auth::AuthError> {
        self.auth.authenticate_data(request).await
    }
    pub fn matches(path: &str) -> bool {
        path == "/api/agent"
            || (path.starts_with("/api/agent/")
                && path != "/api/agent/v2"
                && !path.starts_with("/api/agent/v2/"))
    }
    pub async fn management(
        &self,
        request: Request<hyper::body::Incoming>,
    ) -> Response<super::server::ProxyBody> {
        let tenant = match self.auth.authenticate_management(&request).await {
            Ok(tenant) => tenant,
            Err(error) => {
                return json_response(
                    StatusCode::UNAUTHORIZED,
                    &serde_json::json!({"error": format!("Authentication failed: {error}")}),
                    None,
                )
            }
        };
        super::agent_response::management(
            request,
            limits::AGENT_REQUEST_TIMEOUT,
            |request, ctx| async move { self.management_inner(request, &tenant, &ctx).await },
        )
        .await
    }
    async fn management_inner(
        &self,
        request: Request<hyper::body::Incoming>,
        tenant: &str,
        ctx: &RequestContext,
    ) -> Result<serde_json::Value, Error> {
        let (parts, body) = request.into_parts();
        let suffix = parts
            .uri
            .path()
            .strip_prefix("/api/agent")
            .ok_or(Error::NotFound)?;
        let id = if suffix.is_empty() {
            None
        } else {
            Some(
                suffix
                    .strip_prefix('/')
                    .filter(|s| uuid::Uuid::parse_str(s).is_ok())
                    .ok_or(Error::NotFound)?,
            )
        };
        if url::form_urlencoded::parse(parts.uri.query().unwrap_or("").as_bytes())
            .any(|(key, _)| key != "token" && key != "tenant_id")
        {
            return Err(Error::Invalid("unknown inline management parameter".into()));
        }
        match (parts.method, id) {
            (http::Method::POST, None) => {
                let raw = http_body_util::Limited::new(body, limits::HTTP_JSON_BYTES)
                    .collect()
                    .await
                    .map_err(|_| {
                        Error::Invalid("invalid or oversized Agent create request".into())
                    })?
                    .to_bytes();
                let request: CreateRequest = serde_json::from_slice(&raw).map_err(|_| {
                    Error::Invalid("invalid report-backed inline create JSON".into())
                })?;
                ctx.start_write();
                Ok(
                    serde_json::to_value(self.service.create(tenant, request).await?).map_err(
                        |_| Error::Unavailable("inline response serialization failed".into()),
                    )?,
                )
            }
            (http::Method::GET, Some(id)) => {
                Ok(serde_json::json!({"code":200,"instance":self.service.get(tenant,id).await?}))
            }
            (http::Method::DELETE, Some(id)) => {
                ctx.start_write();
                self.service.kill(tenant, id).await?;
                Ok(serde_json::json!({"code":200,"status":"deleted"}))
            }
            _ => Err(Error::NotFound),
        }
    }
}
