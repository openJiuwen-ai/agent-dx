//! Configured service addresses; no registry, owner or Redis dependency.
use crate::{Error, Result};
use adx_agent_core::{activator::*, transport, Environment, Scope, TemplateVersion};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[async_trait]
pub trait Control: Send + Sync {
    async fn publish(&self, tenant: &str, template: &TemplateVersion) -> Result<()>;
    async fn template(&self, tenant: &str, name: &str, version: &str) -> Result<TemplateVersion>;
    async fn create_environment(&self, scope: &Scope) -> Result<Environment>;
    async fn environment(&self, scope: &Scope) -> Result<Environment>;
    async fn delete_environment(&self, scope: &Scope) -> Result<()>;
    async fn activate(&self, scope: &Scope) -> Result<Target>;
}

pub struct ActivatorClient {
    urls: Vec<String>,
    token: String,
    client: reqwest::Client,
    timeout: Duration,
    next: AtomicUsize,
}
impl ActivatorClient {
    pub fn new(
        urls: Vec<String>,
        token: String,
        timeout: Duration,
        ca: Option<&[u8]>,
        allow_plaintext: bool,
    ) -> Result<Self> {
        transport::validate_service_token(&token).map_err(Error::Invalid)?;
        if urls.is_empty() || timeout.is_zero() {
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
        Ok(Self {
            urls,
            token,
            client,
            timeout,
            next: AtomicUsize::new(0),
        })
    }
    pub fn request_budget(&self) -> Duration {
        self.timeout
    }
    async fn request<T: Serialize + Sync, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &T,
        writes: bool,
    ) -> Result<R> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        let wall_deadline = adx_agent_core::unix_time_millis()
            .saturating_add(self.timeout.as_millis().min(u64::MAX as u128) as u64);
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        // Only a connection failure proves the write was not sent. All attempts share one budget.
        for offset in 0..self.urls.len().min(2) {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::Unavailable(
                    "Activator connection deadline expired".into(),
                ));
            }
            let url = &self.urls[start.wrapping_add(offset) % self.urls.len()];
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
                    if offset + 1 < self.urls.len().min(2) {
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
fn uncertain(writes: bool, message: &str) -> Error {
    if writes {
        Error::OutcomeUnknown(message.into())
    } else {
        Error::Unavailable(message.into())
    }
}
#[async_trait]
impl Control for ActivatorClient {
    async fn publish(&self, tenant: &str, template: &TemplateVersion) -> Result<()> {
        self.request(
            "templates/publish",
            &PublishRequest {
                tenant: tenant.into(),
                template: template.clone(),
            },
            true,
        )
        .await
    }
    async fn template(&self, tenant: &str, name: &str, version: &str) -> Result<TemplateVersion> {
        self.request(
            "templates/get",
            &TemplateRequest {
                tenant: tenant.into(),
                name: name.into(),
                version: version.into(),
            },
            false,
        )
        .await
    }
    async fn create_environment(&self, scope: &Scope) -> Result<Environment> {
        self.request(
            "environments/create",
            &ScopeRequest {
                scope: scope.clone(),
            },
            true,
        )
        .await
    }
    async fn environment(&self, scope: &Scope) -> Result<Environment> {
        self.request(
            "environments/get",
            &ScopeRequest {
                scope: scope.clone(),
            },
            false,
        )
        .await
    }
    async fn delete_environment(&self, scope: &Scope) -> Result<()> {
        self.request(
            "environments/delete",
            &ScopeRequest {
                scope: scope.clone(),
            },
            true,
        )
        .await
    }
    async fn activate(&self, scope: &Scope) -> Result<Target> {
        self.request(
            "environments/activate",
            &ScopeRequest {
                scope: scope.clone(),
            },
            true,
        )
        .await
    }
}
