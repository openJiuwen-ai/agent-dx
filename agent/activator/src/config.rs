//! Configuration shared by the embedded and standalone Activator.
use crate::{
    image_resolution::ImageResolver,
    reconciliation::{ReconcileSettings, Reconciliation},
    registration::RegistrationConfig,
    Activator, CacheSettings, Result,
};
use adx_agent_core::{environment::Environment, limits, network::PlatformEgress};
use adx_agent_store::{discovery::RedisRegistry, AgentState, RedisRepository};
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc, time::Duration};

const REDIS_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivatorConfig {
    pub redis_url: String,
    pub namespace: String,
    pub egress_isolation: Option<PlatformEgress>,
    #[serde(default = "default_request_timeout_seconds")]
    request_timeout_seconds: u64,
    #[serde(default)]
    pub binding_cache: CacheSettings,
    pub registration: Option<RegistrationConfig>,
    /// Docker config for publish-time OCI tag resolution. Only listed registries are queried.
    pub image_registry_auth_file: Option<PathBuf>,
}

fn default_request_timeout_seconds() -> u64 {
    limits::ENVIRONMENT_REQUEST_TIMEOUT.as_secs()
}

impl ActivatorConfig {
    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_seconds)
    }

    /// Returns an error if Redis is unavailable or the cache settings are invalid.
    pub async fn connect(
        &self,
        environment: Arc<dyn Environment>,
    ) -> Result<(Arc<Activator>, RedisRepository)> {
        let store =
            RedisRepository::connect(&self.redis_url, &self.namespace, REDIS_TIMEOUT).await?;
        let mut activator = Activator::with_cache(
            AgentState::new(Arc::new(store.clone())),
            environment,
            self.binding_cache,
        )?;
        activator.image_resolver = self
            .image_registry_auth_file
            .as_deref()
            .map(ImageResolver::from_file)
            .transpose()?;
        Ok((Arc::new(activator), store))
    }

    /// Starts reconciliation after any transport listener is bound. Propagates
    /// membership, Redis notification and registration errors.
    pub async fn start_reconciliation(
        self,
        activator: Arc<Activator>,
        store: RedisRepository,
    ) -> Result<Option<Reconciliation>> {
        let Some(registration) = self.registration else {
            return Ok(None);
        };
        let registry = RedisRegistry::new(&self.redis_url, &self.namespace, REDIS_TIMEOUT)?;
        let worker = Reconciliation::start(
            activator,
            store,
            registry,
            registration,
            ReconcileSettings::default(),
        )
        .await?;
        Ok(Some(worker))
    }
}
