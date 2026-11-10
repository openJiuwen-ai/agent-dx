//! Nydus HTTP backend connector. Registry semantics stay inside this package.
mod http;
mod lazy_fill;
mod registry;
mod service;

use super::{ConnectorContext, ConnectorModule};
use crate::node::vfs::lazy_fill::LazyFillRegistry;
use afs_error::{Error, Result};
use axum::Router;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct NydusConfig {
    pub enabled: bool,
    pub root_name: String,
    pub uid: u32,
    pub gid: u32,
    pub timeout_ms: u64,
    pub max_requests: usize,
    /// Optional bearer token for non-loopback callers of the Nydus routes.
    pub http_token_file: Option<PathBuf>,
    pub sources: BTreeMap<String, RegistryConfig>,
}
impl Default for NydusConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            root_name: ".afs-nydus".into(),
            uid: 0,
            gid: 0,
            timeout_ms: 30_000,
            max_requests: 16,
            http_token_file: None,
            sources: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryConfig {
    pub registry: String,
    /// Only enable for a trusted development Registry on an isolated network.
    #[serde(default)]
    pub allow_insecure_http: bool,
    pub repository: String,
    pub username: Option<String>,
    pub password_file: Option<PathBuf>,
    pub bearer_token_file: Option<PathBuf>,
    /// Optional trusted cross-origin token endpoint allowed to receive Basic credentials.
    pub auth_realm: Option<String>,
}

pub struct NydusConnector {
    config: NydusConfig,
    provider: Arc<registry::RegistryProvider>,
    http_token_hash: Option<[u8; 32]>,
    service: OnceLock<Arc<service::NydusService>>,
}
impl NydusConnector {
    pub fn new(config: NydusConfig, node_id: &str) -> Result<Self> {
        registry::validate_component(&config.root_name)?;
        if config.sources.is_empty()
            || !(1..=128).contains(&config.max_requests)
            || !(100..=120_000).contains(&config.timeout_ms)
        {
            return Err(Error::coded(
                afs_error::CONFIG_INVALID,
                "invalid Nydus sources/request limits",
            ));
        }
        let http_token_hash = config
            .http_token_file
            .as_deref()
            .map(http::load_token_hash)
            .transpose()?;
        let provider = registry::RegistryProvider::new(&config.sources, node_id)?;
        Ok(Self {
            config,
            provider,
            http_token_hash,
            service: OnceLock::new(),
        })
    }
}
impl ConnectorModule for NydusConnector {
    fn id(&self) -> &'static str {
        "nydus"
    }
    fn register_lazy_handlers(&self, registry: &mut LazyFillRegistry) -> Result<()> {
        let handler = Arc::new(lazy_fill::NydusLazyFillHandler::new(self.provider.clone()));
        registry.register(lazy_fill::KIND, handler.clone())?;
        registry.register(lazy_fill::PLAN_KIND, handler)
    }
    fn http_routes(&self, context: ConnectorContext) -> Result<Router> {
        let service = Arc::new(service::NydusService::new(
            context,
            self.config.clone(),
            self.provider.clone(),
        ));
        self.service.set(service.clone()).map_err(|_| {
            Error::coded(afs_error::CONFIG_INVALID, "Nydus routes already assembled")
        })?;
        Ok(http::routes(service, self.http_token_hash))
    }
    fn stop(&self) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            if let Some(service) = self.service.get() {
                service.stop().await;
            }
            Ok(())
        })
    }
}
