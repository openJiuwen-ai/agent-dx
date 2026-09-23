//! Agent transport composition. Management JSON is separate from unchanged data forwarding.
use adx_agent_api::request::RequestContext;
use adx_agent_api::{activator::ActivatorClient, managed::ManagedService, Error};
use adx_agent_core::limits;
use adx_agent_core::{Protocol, Scope, TemplateVersion};
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::BodyExt;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivatorConfig {
    pub urls: Vec<String>,
    pub token_env: String,
    pub ca_path: Option<String>,
    #[serde(default)]
    pub allow_plaintext: bool,
}

fn default_timeout_seconds() -> u64 {
    limits::AGENT_REQUEST_TIMEOUT.as_secs()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentConfig {
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    pub activator: ActivatorConfig,
}
pub struct AgentApi {
    pub managed: Arc<ManagedService>,
    pub request_timeout: Duration,
}
impl AgentApi {
    /// Configure the independent Activator client. Invalid credentials, TLS or deadlines
    /// fail startup; service availability is reported by individual requests.
    pub fn new(config: AgentConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let request_timeout = Duration::from_secs(config.timeout_seconds);
        if request_timeout.is_zero()
            || tokio::time::Instant::now()
                .checked_add(request_timeout)
                .is_none()
        {
            return Err(
                Error::Invalid("Agent timeout must be positive and representable".into()).into(),
            );
        }
        let settings = config.activator;
        let token = std::env::var(&settings.token_env)
            .map_err(|_| Error::Invalid("Activator token environment variable missing".into()))?;
        let ca = settings.ca_path.map(std::fs::read).transpose()?;
        let control = Arc::new(ActivatorClient::new(
            settings.urls,
            token,
            request_timeout,
            ca.as_deref(),
            settings.allow_plaintext,
        )?);
        Ok(Self {
            managed: Arc::new(ManagedService::new(control)),
            request_timeout,
        })
    }
    pub fn matches(path: &str) -> bool {
        path == "/api/agent/v2" || path.starts_with("/api/agent/v2/")
    }
    pub async fn management(
        &self,
        request: Request<hyper::body::Incoming>,
        tenant: &str,
    ) -> Response<super::server::ProxyBody> {
        super::agent_response::management(
            request,
            self.request_timeout,
            |request, ctx| async move { self.managed_management(request, tenant, &ctx).await },
        )
        .await
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResolveInput {
    protocol: Protocol,
    port: Option<u16>,
}
fn segment(value: &str) -> Result<String, Error> {
    let value = percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map_err(|_| Error::Invalid("invalid UTF-8 path identifier".into()))?
        .into_owned();
    if value.is_empty()
        || value.len() > limits::IDENTIFIER_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(Error::Invalid("invalid path identifier".into()));
    }
    Ok(value)
}
impl AgentApi {
    async fn managed_management(
        &self,
        request: Request<hyper::body::Incoming>,
        tenant: &str,
        ctx: &RequestContext,
    ) -> Result<serde_json::Value, Error> {
        let (parts, body) = request.into_parts();
        let path: Vec<_> = parts
            .uri
            .path()
            .strip_prefix("/api/agent/v2/")
            .ok_or(Error::NotFound)?
            .split('/')
            .collect();
        let listing = parts.method == http::Method::GET
            && matches!(
                path.as_slice(),
                ["templates", _, "versions", _, "environments"]
            );
        if !listing && parts.uri.query().is_some() {
            return Err(Error::Invalid(
                "this managed API does not accept query parameters".into(),
            ));
        }
        let managed = &self.managed;
        if path == ["templates"] && parts.method == http::Method::POST {
            let input: TemplateVersion = decode_body(body).await?;

            managed.publish(ctx, tenant, &input).await?;
            return Ok(serde_json::json!({"template":input}));
        }
        let ["templates", name, "versions", version, rest @ ..] = path.as_slice() else {
            return Err(Error::NotFound);
        };
        let name = segment(name)?;
        let version = segment(version)?;
        if rest == ["environments"] && parts.method == http::Method::GET {
            let mut query = adx_agent_core::activator::EnvironmentList {
                tenant: tenant.into(),
                template: name,
                version,
                page_size: adx_agent_core::activator::default_page_size(),
                page_token: None,
            };
            let mut seen = std::collections::BTreeSet::new();
            for (key, value) in
                url::form_urlencoded::parse(parts.uri.query().unwrap_or("").as_bytes())
            {
                if !seen.insert(key.to_string()) {
                    return Err(Error::Invalid("duplicate pagination parameter".into()));
                }
                match key.as_ref() {
                    "page_size" => {
                        query.page_size = value
                            .parse()
                            .map_err(|_| Error::Invalid("invalid page_size".into()))?
                    }
                    "page_token" => query.page_token = Some(value.into_owned()),
                    _ => return Err(Error::Invalid("unknown pagination parameter".into())),
                }
            }
            return serde_json::to_value(managed.list_environments(ctx, &query).await?)
                .map_err(|_| Error::Unavailable("Environment page serialization failed".into()));
        }
        if rest.is_empty() && parts.method == http::Method::GET {
            return Ok(
                serde_json::json!({"template":managed.template(ctx, tenant,&name,&version).await?}),
            );
        }
        let ["environments", environment, action @ ..] = rest else {
            return Err(Error::NotFound);
        };
        let scope = Scope {
            tenant: tenant.into(),
            template: name,
            version,
            environment_id: segment(environment)?,
        };
        scope.validate().map_err(Error::Invalid)?;
        match (parts.method, action) {
            (http::Method::GET, []) => {
                Ok(serde_json::json!({"environment":managed.environment(ctx, &scope).await?}))
            }
            (http::Method::DELETE, []) => {
                managed.delete_environment(ctx, &scope).await?;
                Ok(serde_json::json!({"status":"deleted"}))
            }
            (http::Method::POST, ["resolve"]) => {
                let input: ResolveInput = decode_body(body).await?;

                let (target, port) = managed
                    .resolve(ctx, &scope, input.protocol, input.port)
                    .await?;
                Ok(
                    serde_json::json!({"sandbox_id":target.environment.sandbox_id,"port":port,"protocol":input.protocol}),
                )
            }
            _ => Err(Error::NotFound),
        }
    }
    /// Rewrite a managed HTTP/WS request into the existing fixed Sandbox forwarding path.
    /// SSH clients use the resolve endpoint then the existing Sandbox-ID CONNECT/tunnel interface.
    pub fn can_retry_data<B>(&self, request: &Request<B>) -> bool {
        request.extensions().get::<SelectionRetry>().is_some()
    }
    /// Consume retry metadata: each incoming business request can repeat activation once before sending.
    pub async fn retry_data<B>(&self, request: &mut Request<B>) -> Result<bool, Error> {
        let Some(retry) = request.extensions_mut().remove::<SelectionRetry>() else {
            return Ok(false);
        };
        let (target, port) = retry
            .context
            .run(self.managed.retry_resolve(
                &retry.context,
                &retry.scope,
                retry.protocol,
                retry.port,
                &retry.generation,
            ))
            .await?;
        let uri = format!(
            "/{}/{port}/{}",
            target.environment.sandbox_id, retry.tail_and_query
        );
        *request.uri_mut() = uri
            .parse()
            .map_err(|_| Error::Unavailable("invalid resolved forwarding path".into()))?;
        Ok(true)
    }
    pub(super) async fn prepare_data<B>(
        &self,
        request: &mut Request<B>,
        tenant: &str,
        access: super::agent_access::AccessRequest,
    ) -> Result<(), Error> {
        let scope = ManagedService::environment_scope(tenant, &access.target)?;
        let notice = super::agent_access::EnvironmentNotice::new(&scope)?;
        request.extensions_mut().insert(notice);
        let context = Arc::new(RequestContext::new(self.request_timeout));
        let (target, port) = context
            .run(
                self.managed
                    .resolve(&context, &scope, access.protocol, access.port),
            )
            .await?;
        let backend_uri = access.backend_uri.to_string();
        let tail_and_query = backend_uri
            .strip_prefix('/')
            .unwrap_or(&backend_uri)
            .to_owned();
        *request.uri_mut() = format!(
            "/{}/{port}/{}",
            target.environment.sandbox_id, tail_and_query
        )
        .parse()
        .map_err(|_| Error::Unavailable("invalid resolved forwarding path".into()))?;
        request.extensions_mut().insert(SelectionRetry {
            context,
            generation: target.environment.generation,
            scope,
            protocol: access.protocol,
            port,
            tail_and_query,
        });
        Ok(())
    }
}

async fn decode_body<T: serde::de::DeserializeOwned>(
    body: hyper::body::Incoming,
) -> Result<T, Error> {
    let bytes = read_body(body).await?;
    serde_json::from_slice(&bytes)
        .map_err(|_| Error::Invalid("invalid managed request JSON".into()))
}
async fn read_body(body: hyper::body::Incoming) -> Result<Bytes, Error> {
    let bytes = http_body_util::Limited::new(body, limits::HTTP_JSON_BYTES)
        .collect()
        .await
        .map_err(|_| Error::Invalid("invalid or oversized managed request".into()))?
        .to_bytes();
    Ok(bytes)
}

#[derive(Clone)]
struct SelectionRetry {
    context: Arc<RequestContext>,
    generation: String,
    scope: Scope,
    protocol: Protocol,
    port: u16,
    tail_and_query: String,
}
