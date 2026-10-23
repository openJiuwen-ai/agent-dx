//! Runtime class dispatch and execution ownership recovered from node authority.
use crate::{
    jiuwenbox::Jiuwenbox, sandboxd::Sandboxd, RestoreSource, RuntimeDriver, RuntimeObservation,
    RuntimeStatus,
};
use adx_core::{
    execution::NetworkPolicy, scheduling::DeviceAllocation, EnvironmentRecord, EnvironmentSpec,
    Error, Result,
};
use async_trait::async_trait;
use std::{net::IpAddr, path::Path, time::Duration};
use tokio::time::Instant;

pub struct RuntimeRouter {
    sandboxd: Option<Sandboxd>,
    jiuwenbox: Option<Jiuwenbox>,
}
impl RuntimeRouter {
    pub fn new(sandboxd: Option<Sandboxd>, jiuwenbox: Option<Jiuwenbox>) -> Result<Self> {
        if sandboxd.is_none() && jiuwenbox.is_none() {
            return Err(Error::Invalid(
                "at least one runtime backend required".into(),
            ));
        }
        Ok(Self {
            sandboxd,
            jiuwenbox,
        })
    }
    pub async fn check_health(&self) -> Result<()> {
        self.sandboxd
            .as_ref()
            .ok_or_else(|| {
                Error::Invalid("Sandboxd capacity source needs a configured backend".into())
            })?
            .check_health()
            .await
    }
    pub async fn available_runtime_classes(&self) -> Result<Vec<String>> {
        let mut classes = vec![];
        if let Some(runtime) = &self.sandboxd {
            classes.extend(runtime.available_runtime_classes().await?);
        }
        if let Some(runtime) = &self.jiuwenbox {
            classes.extend(runtime.available_runtime_classes().await?);
        }
        classes.sort();
        classes.dedup();
        Ok(classes)
    }
    fn class(&self, class: &str) -> Result<&dyn RuntimeDriver> {
        if matches!(class, "supervisor" | "conch") {
            Ok(self
                .jiuwenbox
                .as_ref()
                .ok_or_else(|| Error::Invalid("Jiuwenbox backend is not configured".into()))?)
        } else {
            Ok(self
                .sandboxd
                .as_ref()
                .ok_or_else(|| Error::Invalid("sandboxd backend is not configured".into()))?)
        }
    }
    async fn owner(&self, id: &str) -> Result<&dyn RuntimeDriver> {
        if let Some(runtime) = &self.jiuwenbox {
            if runtime.owns(id).await? {
                return Ok(runtime);
            }
        }
        self.sandboxd
            .as_ref()
            .map(|r| r as &dyn RuntimeDriver)
            .ok_or(Error::NotFound)
    }
}

#[async_trait]
impl RuntimeDriver for RuntimeRouter {
    fn retains_checkpoint_files(&self, class: &str) -> bool {
        self.class(class)
            .is_ok_and(|r| r.retains_checkpoint_files(class))
    }
    fn retains_capture_files(&self, class: &str) -> bool {
        self.class(class)
            .is_ok_and(|r| r.retains_capture_files(class))
    }
    async fn start(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
    ) -> Result<IpAddr> {
        self.class(&spec.runtime_class)?
            .start(spec, id, generation, devices)
            .await
    }
    async fn start_with_deadline(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        deadline: Instant,
    ) -> Result<IpAddr> {
        self.class(&spec.runtime_class)?
            .start_with_deadline(spec, id, generation, devices, deadline)
            .await
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        match self.owner(id).await {
            Ok(r) => r.is_running(id).await,
            Err(Error::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }
    async fn observe(&self, id: &str) -> Result<RuntimeStatus> {
        match self.owner(id).await {
            Ok(r) => r.observe(id).await,
            Err(Error::NotFound) => Ok(RuntimeStatus {
                running: false,
                failure: None,
            }),
            Err(e) => Err(e),
        }
    }
    async fn remove(&self, id: &str) -> Result<()> {
        if let Some(runtime) = &self.jiuwenbox {
            if runtime.owns(id).await? {
                return runtime.remove(id).await;
            }
        }
        if let Some(runtime) = &self.sandboxd {
            runtime.remove(id).await?;
        }
        // Unknown execution IDs have no class yet. Fence a late Jiuwenbox
        // start even when the fallback sandboxd backend was also configured.
        if let Some(runtime) = &self.jiuwenbox {
            runtime.remove(id).await?;
        }
        Ok(())
    }
    async fn reconcile_inventory(
        &self,
        catalog: &[EnvironmentRecord],
    ) -> Result<Vec<RuntimeObservation>> {
        let mut inventory = vec![];
        if let Some(r) = &self.sandboxd {
            inventory.extend(r.reconcile_inventory(catalog).await?);
        }
        if let Some(r) = &self.jiuwenbox {
            inventory.extend(r.reconcile_inventory(catalog).await?);
        }
        let mut ids = std::collections::BTreeSet::new();
        if inventory.iter().any(|r| !ids.insert(&r.runtime_id)) {
            return Err(Error::Conflict);
        }
        Ok(inventory)
    }
    async fn reconcile_unowned_executions(&self) -> Result<()> {
        if let Some(r) = &self.sandboxd {
            r.reconcile_unowned_executions().await?;
        }
        if let Some(r) = &self.jiuwenbox {
            r.reconcile_unowned_executions().await?;
        }
        Ok(())
    }
    async fn inventory(&self) -> Result<Vec<RuntimeObservation>> {
        let mut inventory = vec![];
        if let Some(r) = &self.sandboxd {
            inventory.extend(r.inventory().await?);
        }
        if let Some(r) = &self.jiuwenbox {
            inventory.extend(r.inventory().await?);
        }
        let mut ids = std::collections::BTreeSet::new();
        if inventory.iter().any(|r| !ids.insert(&r.runtime_id)) {
            return Err(Error::Conflict);
        }
        Ok(inventory)
    }
    async fn set_network_policy(
        &self,
        id: &str,
        policy: Option<&NetworkPolicy>,
        ports: &[u16],
        tunnel_http_port: Option<u16>,
    ) -> Result<()> {
        self.owner(id)
            .await?
            .set_network_policy(id, policy, ports, tunnel_http_port)
            .await
    }
    async fn stats(&self, id: &str) -> Result<crate::metrics::RuntimeUsage> {
        self.owner(id).await?.stats(id).await
    }
    async fn checkpoint_supported(&self, class: &str) -> Result<()> {
        self.class(class)?.checkpoint_supported(class).await
    }
    async fn checkpoint(&self, id: &str, path: &Path, timeout: Duration) -> Result<()> {
        self.owner(id).await?.checkpoint(id, path, timeout).await
    }
    async fn checkpoint_running(&self, id: &str, path: &Path, timeout: Duration) -> Result<()> {
        self.owner(id)
            .await?
            .checkpoint_running(id, path, timeout)
            .await
    }
    async fn restore(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        path: &Path,
    ) -> Result<IpAddr> {
        self.class(&spec.runtime_class)?
            .restore(spec, id, generation, devices, path)
            .await
    }
    async fn restore_from(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        path: &Path,
        origin: Option<&adx_core::runtime::RuntimeIdentity>,
    ) -> Result<IpAddr> {
        self.class(&spec.runtime_class)?
            .restore_from(spec, id, generation, devices, path, origin)
            .await
    }
    async fn restore_from_with_deadline(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        source: RestoreSource<'_>,
        deadline: Instant,
    ) -> Result<IpAddr> {
        self.class(&spec.runtime_class)?
            .restore_from_with_deadline(spec, id, generation, devices, source, deadline)
            .await
    }
}
