//! HTTP client for the independent Activator service.
use crate::discovery::{validate_members, DiscoveryConfig};
use crate::request::RequestContext;
use crate::{Error, Result};
use adx_agent_core::discovery::{ranked_endpoints, ActivatorEndpoint};
use adx_agent_core::{activator::*, transport, Environment, Scope, TemplateVersion};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

/// Product operations shared by the independent HTTP client and embedded Activator.
#[async_trait]
pub trait Control: Send + Sync {
    async fn publish(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        template: &TemplateVersion,
    ) -> Result<()>;
    async fn template(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<TemplateVersion>;
    async fn environment(&self, ctx: &RequestContext, scope: &Scope) -> Result<Environment>;
    async fn list_environments(
        &self,
        ctx: &RequestContext,
        query: &EnvironmentList,
    ) -> Result<EnvironmentPage>;
    async fn delete_environment(&self, ctx: &RequestContext, scope: &Scope) -> Result<()>;
    async fn activate(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        expected_generation: Option<&str>,
    ) -> Result<Target>;
    /// Refresh a cached binding when requested; immutable template caching is unchanged.
    async fn activate_with_cache(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        expected_generation: Option<&str>,
        bypass_cache: bool,
    ) -> Result<Target>;
}

pub struct ActivatorClient {
    endpoints: Arc<RwLock<Vec<ActivatorEndpoint>>>,
    discovery_task: Option<tokio::task::JoinHandle<()>>,
    token: String,
    client: reqwest::Client,
    timeout: Duration,
    next: AtomicUsize,
}
impl Drop for ActivatorClient {
    fn drop(&mut self) {
        if let Some(task) = &self.discovery_task {
            task.abort();
        }
    }
}
impl ActivatorClient {
    pub fn new(
        urls: Vec<String>,
        token: String,
        timeout: Duration,
        ca: Option<&[u8]>,
        allow_plaintext: bool,
    ) -> Result<Self> {
        Self::build(urls, token, timeout, ca, allow_plaintext, None)
    }
    /// Start asynchronous discovery; requests return Unavailable until a nonempty snapshot exists.
    pub fn with_discovery(
        config: DiscoveryConfig,
        token: String,
        timeout: Duration,
        ca: Option<&[u8]>,
        allow_plaintext: bool,
    ) -> Result<Self> {
        Self::build(
            Vec::new(),
            token,
            timeout,
            ca,
            allow_plaintext,
            Some(config),
        )
    }
    fn build(
        urls: Vec<String>,
        token: String,
        timeout: Duration,
        ca: Option<&[u8]>,
        allow_plaintext: bool,
        discovery: Option<DiscoveryConfig>,
    ) -> Result<Self> {
        transport::validate_service_token(&token).map_err(Error::Invalid)?;
        if (urls.is_empty() && discovery.is_none()) || timeout.is_zero() {
            return Err(Error::Invalid(
                "Activator addresses and positive timeout required".into(),
            ));
        }
        let urls = urls
            .iter()
            .map(|url| {
                transport::service_origin(url, allow_plaintext)
                    .map(|url| url.as_str().trim_end_matches('/').to_owned())
                    .map_err(Error::Invalid)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(adx_agent_core::limits::CONNECT_TIMEOUT);
        if let Some(ca) = ca {
            builder = builder.add_root_certificate(
                reqwest::Certificate::from_pem(ca).map_err(|e| Error::Invalid(e.to_string()))?,
            );
        }
        let client = builder.build().map_err(|e| Error::Invalid(e.to_string()))?;
        let endpoints = Arc::new(RwLock::new(validate_members(
            urls.into_iter()
                .map(|url| ActivatorEndpoint {
                    id: url.clone(),
                    url,
                })
                .collect(),
            allow_plaintext,
        )?));
        let discovery_task = discovery
            .map(|config| crate::discovery::start(config, endpoints.clone(), allow_plaintext))
            .transpose()?;
        Ok(Self {
            endpoints,
            discovery_task,
            token,
            client,
            timeout,
            next: AtomicUsize::new(0),
        })
    }
    async fn request<T: Serialize + Sync, R: DeserializeOwned>(
        &self,
        ctx: &RequestContext,
        path: &str,
        body: &T,
        writes: bool,
        scope: Option<&Scope>,
    ) -> Result<R> {
        let endpoints = self
            .endpoints
            .read()
            .map_err(|_| Error::Unavailable("Activator membership lock unavailable".into()))?
            .clone();
        let endpoints = if let Some(scope) = scope {
            scope.validate().map_err(Error::Invalid)?;
            ranked_endpoints(scope, &endpoints)
        } else {
            let mut endpoints = endpoints;
            if !endpoints.is_empty() {
                let offset = self.next.fetch_add(1, Ordering::Relaxed) % endpoints.len();
                endpoints.rotate_left(offset);
            }
            endpoints
        };
        let deadline = ctx
            .deadline()
            .min(tokio::time::Instant::now() + self.timeout);
        let wall_deadline = adx_agent_core::unix_time_millis().saturating_add(
            deadline
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis()
                .min(u64::MAX as u128) as u64,
        );
        // Only a connection failure proves the write was not sent. All attempts share one budget.
        for (offset, endpoint) in endpoints.iter().take(2).enumerate() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::Unavailable(
                    "Activator connection deadline expired".into(),
                ));
            }
            let url = &endpoint.url;
            if writes {
                ctx.start_write();
            }
            let response = self
                .client
                .post(format!("{url}/internal/adx/v1/{path}"))
                .bearer_auth(&self.token)
                .header(DEADLINE_HEADER, wall_deadline.to_string())
                .timeout(remaining)
                .json(body)
                .send()
                .await;
            let mut response = match response {
                Ok(response) => response,
                Err(e) if e.is_connect() => {
                    if offset + 1 < endpoints.len().min(2) {
                        continue;
                    }
                    return Err(Error::Unavailable("Activator connection failed".into()));
                }
                Err(_) => return Err(uncertain(writes, "Activator response unavailable")),
            };
            let status = response.status();
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| uncertain(writes, "Activator response interrupted"))?
            {
                if bytes.len() + chunk.len() > adx_agent_core::limits::HTTP_JSON_BYTES {
                    return Err(uncertain(writes, "Activator response exceeds size limit"));
                }
                bytes.extend_from_slice(&chunk);
            }
            if !status.is_success() {
                return Err(serde_json::from_slice(&bytes)
                    .unwrap_or_else(|_| uncertain(writes, "invalid Activator error response")));
            }
            return serde_json::from_slice(&bytes)
                .map_err(|_| uncertain(writes, "invalid Activator response"));
        }
        Err(Error::Unavailable("Activator unavailable".into()))
    }
}

#[async_trait]
impl Control for ActivatorClient {
    async fn publish(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        template: &TemplateVersion,
    ) -> Result<()> {
        ActivatorClient::publish(self, ctx, tenant, template).await
    }
    async fn template(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<TemplateVersion> {
        ActivatorClient::template(self, ctx, tenant, name, version).await
    }
    async fn environment(&self, ctx: &RequestContext, scope: &Scope) -> Result<Environment> {
        ActivatorClient::environment(self, ctx, scope).await
    }
    async fn list_environments(
        &self,
        ctx: &RequestContext,
        query: &EnvironmentList,
    ) -> Result<EnvironmentPage> {
        ActivatorClient::list_environments(self, ctx, query).await
    }
    async fn delete_environment(&self, ctx: &RequestContext, scope: &Scope) -> Result<()> {
        ActivatorClient::delete_environment(self, ctx, scope).await
    }
    async fn activate(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        expected_generation: Option<&str>,
    ) -> Result<Target> {
        ActivatorClient::activate(self, ctx, scope, expected_generation).await
    }
    async fn activate_with_cache(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        expected_generation: Option<&str>,
        bypass_cache: bool,
    ) -> Result<Target> {
        ActivatorClient::activate_with_cache(self, ctx, scope, expected_generation, bypass_cache)
            .await
    }
}
pub(crate) fn uncertain(writes: bool, message: &str) -> Error {
    if writes {
        Error::OutcomeUnknown(message.into())
    } else {
        Error::Unavailable(message.into())
    }
}
impl ActivatorClient {
    pub async fn publish(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        template: &TemplateVersion,
    ) -> Result<()> {
        self.request(
            ctx,
            "templates/publish",
            &PublishRequest {
                tenant: tenant.into(),
                template: template.clone(),
            },
            true,
            None,
        )
        .await
    }
    pub async fn template(
        &self,
        ctx: &RequestContext,
        tenant: &str,
        name: &str,
        version: &str,
    ) -> Result<TemplateVersion> {
        self.request(
            ctx,
            "templates/get",
            &TemplateRequest {
                tenant: tenant.into(),
                name: name.into(),
                version: version.into(),
            },
            false,
            None,
        )
        .await
    }
    pub async fn environment(&self, ctx: &RequestContext, scope: &Scope) -> Result<Environment> {
        self.request(
            ctx,
            "environments/get",
            &ScopeRequest {
                scope: scope.clone(),
            },
            false,
            Some(scope),
        )
        .await
    }
    pub async fn list_environments(
        &self,
        ctx: &RequestContext,
        query: &EnvironmentList,
    ) -> Result<EnvironmentPage> {
        self.request(ctx, "environments/list", query, false, None)
            .await
    }
    pub async fn delete_environment(&self, ctx: &RequestContext, scope: &Scope) -> Result<()> {
        self.request(
            ctx,
            "environments/delete",
            &ScopeRequest {
                scope: scope.clone(),
            },
            true,
            Some(scope),
        )
        .await
    }
    pub async fn activate(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        expected_generation: Option<&str>,
    ) -> Result<Target> {
        self.activate_with_cache(ctx, scope, expected_generation, false)
            .await
    }
    /// Bypass successful activation bindings while retaining immutable template reuse.
    pub async fn activate_with_cache(
        &self,
        ctx: &RequestContext,
        scope: &Scope,
        expected_generation: Option<&str>,
        bypass_cache: bool,
    ) -> Result<Target> {
        self.request(
            ctx,
            "environments/activate",
            &ActivationRequest {
                scope: scope.clone(),
                expected_generation: expected_generation.map(str::to_owned),
                bypass_cache,
            },
            true,
            Some(scope),
        )
        .await
    }
}
