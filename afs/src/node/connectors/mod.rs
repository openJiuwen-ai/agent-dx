//! Optional third-party connectors. Node only assembles hooks and HTTP routes.
pub mod nydus;
#[cfg(all(target_os = "linux", feature = "overlaybd-connector"))]
mod overlaybd;

use super::vfs::{Backend, lazy_fill::LazyFillRegistry, types::BackendInode};
use afs_error::Result;
use axum::Router;
use serde::{Deserialize, Serialize};
use std::{future::Future, pin::Pin, sync::Arc};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConnectorsConfig {
    pub nydus: nydus::NydusConfig,
    #[cfg(all(target_os = "linux", feature = "overlaybd-connector"))]
    pub overlaybd: overlaybd::OverlaybdConfig,
}

#[derive(Clone)]
pub struct ConnectorContext {
    pub backend: Arc<dyn Backend>,
    pub root: BackendInode,
}

pub trait ConnectorModule: Send + Sync {
    fn id(&self) -> &'static str;
    fn register_lazy_handlers(&self, registry: &mut LazyFillRegistry) -> Result<()>;
    fn http_routes(&self, context: ConnectorContext) -> Result<Router>;
    /// Stop admission and wait for file workers, including cancelled HTTP work.
    fn stop(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>;
}

pub struct ConnectorRuntime {
    connectors: Vec<Box<dyn ConnectorModule>>,
}

impl ConnectorRuntime {
    pub fn build(config: &ConnectorsConfig, node_id: &str) -> Result<Self> {
        let mut connectors: Vec<Box<dyn ConnectorModule>> = Vec::new();
        if config.nydus.enabled {
            connectors.push(Box::new(nydus::NydusConnector::new(
                config.nydus.clone(),
                node_id,
            )?));
        }
        #[cfg(all(target_os = "linux", feature = "overlaybd-connector"))]
        if config.overlaybd.enabled {
            if !config.nydus.enabled {
                return Err(afs_error::Error::coded(
                    afs_error::CONFIG_INVALID,
                    "OverlayBD requires the AFS Registry blob routes",
                ));
            }
            connectors.push(Box::new(overlaybd::OverlaybdConnector::new(
                config.overlaybd.clone(),
            )?));
        }
        Ok(Self { connectors })
    }
    pub fn register_lazy_handlers(&self) -> Result<Arc<LazyFillRegistry>> {
        let mut registry = LazyFillRegistry::default();
        for connector in &self.connectors {
            connector.register_lazy_handlers(&mut registry)?;
        }
        Ok(Arc::new(registry))
    }
    pub fn http_routes(&self, context: ConnectorContext) -> Result<Router> {
        let mut router = Router::new();
        for connector in &self.connectors {
            router = router.merge(connector.http_routes(context.clone())?);
        }
        Ok(router)
    }
    pub async fn stop(&self) -> Result<()> {
        for connector in self.connectors.iter().rev() {
            connector.stop().await?;
        }
        Ok(())
    }
}
