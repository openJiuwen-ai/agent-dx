//! Agent transport composition. Management JSON is separate from unchanged data forwarding.
use adx_agent_api::request::RequestContext;
use adx_agent_api::{
    activator::{ActivatorClient, Control},
    local::LocalControl,
    managed::ManagedService,
    Error,
};
use adx_agent_core::environment::Environment;
use adx_agent_core::limits;
use adx_agent_core::{Protocol, Scope};
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::BodyExt;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivatorConfig {
    #[serde(default)]
    pub urls: Vec<String>,
    pub discovery: Option<adx_agent_api::discovery::DiscoveryConfig>,
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
    pub activator: Option<ActivatorConfig>,
    pub embedded: Option<EmbeddedActivatorConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddedActivatorConfig {
    pub sandbox_network: adx_agent_core::network::SandboxNetworkPolicy,
    pub redis_url: String,
    pub namespace: String,
    pub registration: Option<adx_agent_api::local::RegistrationConfig>,
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
        let settings = config
            .activator
            .ok_or_else(|| Error::Invalid("independent Activator configuration required".into()))?;
        if config.embedded.is_some() {
            return Err(Error::Invalid("configure exactly one Activator mode".into()).into());
        }
        let token = std::env::var(&settings.token_env)
            .map_err(|_| Error::Invalid("Activator token environment variable missing".into()))?;
        let ca = settings.ca_path.map(std::fs::read).transpose()?;
        let control = Arc::new(match settings.discovery {
            Some(discovery) => {
                if !settings.urls.is_empty() {
                    return Err(Error::Invalid(
                        "configure either activator.urls or discovery".into(),
                    )
                    .into());
                }
                ActivatorClient::with_discovery(
                    discovery,
                    token,
                    request_timeout,
                    ca.as_deref(),
                    settings.allow_plaintext,
                )?
            }
            None => ActivatorClient::new(
                settings.urls,
                token,
                request_timeout,
                ca.as_deref(),
                settings.allow_plaintext,
            )?,
        });
        Ok(Self {
            managed: Arc::new(ManagedService::new(control)),
            request_timeout,
        })
    }

    /// Bind an embedded Activator directly to the API Server's Environment service.
    pub async fn new_embedded(
        config: AgentConfig,
        environment: Arc<dyn Environment>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if config.activator.is_some() {
            return Err(Error::Invalid("configure exactly one Activator mode".into()).into());
        }
        let settings = config
            .embedded
            .ok_or_else(|| Error::Invalid("embedded Activator configuration required".into()))?;
        let timeout = Duration::from_secs(config.timeout_seconds);
        if timeout.is_zero() || tokio::time::Instant::now().checked_add(timeout).is_none() {
            return Err(
                Error::Invalid("Agent timeout must be positive and representable".into()).into(),
            );
        }
        let control: Arc<dyn Control> = Arc::new(match settings.registration {
            Some(registration) => {
                LocalControl::connect_reconciled(
                    &settings.redis_url,
                    &settings.namespace,
                    environment,
                    registration,
                )
                .await?
            }
            None => {
                LocalControl::connect(&settings.redis_url, &settings.namespace, environment).await?
            }
        });
        Ok(Self {
            managed: Arc::new(ManagedService::new(control)),
            request_timeout: timeout,
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
    #[serde(default, rename = "bypasscache")]
    bypass_cache: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AliasInput {
    seed: String,
    versions: Vec<adx_agent_core::alias::VersionWeight>,
    expected_revision: Option<String>,
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
                ["templates", _, "versions" | "aliases", _, "bindings"]
            );
        if !listing && parts.uri.query().is_some() {
            return Err(Error::Invalid(
                "this managed API does not accept query parameters".into(),
            ));
        }
        let managed = &self.managed;
        if path == ["templates"] && parts.method == http::Method::POST {
            let input: adx_agent_core::TemplateDraft = decode_body(body).await?;

            let published = managed.publish(ctx, tenant, &input).await?;
            return Ok(serde_json::json!({"template":published}));
        }
        let ["templates", name, kind, reference, rest @ ..] = path.as_slice() else {
            return Err(Error::NotFound);
        };
        let name = segment(name)?;
        let reference = segment(reference)?;
        let template_ref = match *kind {
            "versions" => adx_agent_core::TemplateRef::Version(reference),
            "aliases" => adx_agent_core::TemplateRef::Alias(reference),
            _ => return Err(Error::NotFound),
        };
        template_ref.validate().map_err(Error::Invalid)?;
        if rest == ["bindings"] && parts.method == http::Method::GET {
            let mut query = adx_agent_core::activator::BindingList {
                tenant: tenant.into(),
                template: name,
                template_ref: template_ref.clone(),
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
            return serde_json::to_value(managed.list_bindings(ctx, &query).await?)
                .map_err(|_| Error::Unavailable("AgentBinding page serialization failed".into()));
        }
        if rest.is_empty() && parts.method == http::Method::PUT {
            let adx_agent_core::TemplateRef::Alias(alias) = &template_ref else {
                return Err(Error::NotFound);
            };
            let input: AliasInput = decode_body(body).await?;
            let request = adx_agent_core::activator::AliasUpdate {
                config: adx_agent_core::alias::AliasConfig {
                    tenant: tenant.into(),
                    template: name,
                    alias: alias.clone(),
                    seed: input.seed,
                    versions: input.versions,
                },
                expected_revision: input.expected_revision,
            };
            return Ok(serde_json::json!({"alias":managed.update_alias(ctx, &request).await?}));
        }
        if rest.is_empty() && parts.method == http::Method::GET {
            return match &template_ref {
                adx_agent_core::TemplateRef::Version(version) => Ok(
                    serde_json::json!({"template":managed.template(ctx, tenant,&name,version).await?}),
                ),
                adx_agent_core::TemplateRef::Alias(alias) => Ok(
                    serde_json::json!({"alias":managed.alias_config(ctx,tenant,&name,alias).await?}),
                ),
            };
        }
        let ["bindings", binding, action @ ..] = rest else {
            return Err(Error::NotFound);
        };
        let scope = Scope {
            tenant: tenant.into(),
            template: name,
            template_ref: template_ref.clone(),
            binding_id: segment(binding)?,
        };
        scope.validate().map_err(Error::Invalid)?;
        match (parts.method, action) {
            (http::Method::GET, []) => {
                Ok(serde_json::json!({"binding":managed.binding(ctx, &scope).await?}))
            }
            (http::Method::DELETE, []) => {
                managed.delete_binding(ctx, &scope).await?;
                Ok(serde_json::json!({"status":"deleted"}))
            }
            (http::Method::POST, ["resolve"]) => {
                let input: ResolveInput = decode_body(body).await?;

                let (target, port) = managed
                    .resolve_with_cache(ctx, &scope, input.protocol, input.port, input.bypass_cache)
                    .await?;
                Ok(
                    serde_json::json!({"environment_id":target.binding.current.environment_id,"port":port,"protocol":input.protocol}),
                )
            }
            _ => Err(Error::NotFound),
        }
    }
    pub fn service_access(&self) -> super::agent_service::AgentV2Access {
        super::agent_service::AgentV2Access::new(self.managed.clone())
    }

    pub(super) fn take_service_selection<B>(
        &self,
        request: &mut Request<B>,
    ) -> Option<(super::agent_service::ServiceSelection, http::Uri)> {
        request
            .extensions_mut()
            .remove::<SelectionRetry>()
            .map(|retry| (retry.selection, retry.backend_uri))
    }

    /// Rewrite a managed HTTP/WS request into the existing fixed Environment forwarding path.
    /// SSH clients use the resolve endpoint then the existing Environment-ID CONNECT/tunnel interface.
    pub fn can_retry_data<B>(&self, request: &Request<B>) -> bool {
        request.extensions().get::<SelectionRetry>().is_some()
    }
    /// Consume retry metadata: each incoming business request can repeat activation once before sending.
    pub async fn retry_data<B>(&self, request: &mut Request<B>) -> Result<bool, Error> {
        let Some(retry) = request.extensions_mut().remove::<SelectionRetry>() else {
            return Ok(false);
        };
        let mut selection = retry.selection;
        if !self.service_access().retry(&mut selection).await? {
            return Ok(false);
        }
        *request.uri_mut() = selection.forwarding_uri(&retry.backend_uri)?;
        Ok(true)
    }
    pub(super) async fn prepare_data<B>(
        &self,
        request: &mut Request<B>,
        tenant: &str,
        access: super::agent_access::AccessRequest,
    ) -> Result<(), Error> {
        // Keep bytes opaque until the authoritative Activator confirms a missing Binding.
        let mut parameters = Vec::new();
        let mut present = false;
        for value in request
            .headers()
            .get_all(adx_agent_core::parameters::HEADER)
        {
            if present {
                parameters.push(b'&');
            }
            parameters.extend_from_slice(value.as_bytes());
            present = true;
        }
        request
            .headers_mut()
            .remove(adx_agent_core::parameters::HEADER);
        let scope = ManagedService::binding_scope(tenant, &access.target)?;
        let notice = super::agent_access::BindingNotice::new(&scope)?;
        request.extensions_mut().insert(notice);
        let context = Arc::new(RequestContext::new(self.request_timeout));
        let selection = self
            .service_access()
            .select_with_parameters(
                context,
                scope,
                super::agent_service::ServiceSelector {
                    protocol: access.protocol,
                    port: access.port,
                },
                if present {
                    Some(parameters.as_slice())
                } else {
                    None
                },
            )
            .await?;
        *request.uri_mut() = selection.forwarding_uri(&access.backend_uri)?;
        request.extensions_mut().insert(SelectionRetry {
            selection,
            backend_uri: access.backend_uri,
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
    selection: super::agent_service::ServiceSelection,
    backend_uri: http::Uri,
}
