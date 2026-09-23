//! Node-local admission and serial Environment lifecycle ownership.

pub mod activity;
pub mod admin;
pub mod checkpoint;
mod controller;
mod local;
pub use local::LocalReservation;
pub mod journal;
pub mod metrics;
pub mod proxy;
pub mod readiness;
mod reconciliation;
pub mod resources;
pub mod routes;
pub mod rpc;
pub mod runtime_control;
pub mod sandboxd;

use adx_core::scheduling::{validate_device_assignment, Device, DeviceAllocation, DeviceLedger};
use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, Error, ResourceLedger, Resources, Result,
};
use async_trait::async_trait;
pub use controller::EnvironmentHandle;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Coordinator has committed the record to cluster storage.
    Published,
    /// A local durable degradation journal accepted it; cluster routing is stale.
    Journaled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationResult {
    pub record: EnvironmentRecord,
    pub durability: Durability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeObservation {
    pub environment_id: String,
    pub runtime_id: String,
    pub generation: u64,
    pub tenant_id: String,
    pub running: bool,
}

#[async_trait]
pub trait RuntimeDriver: Send + Sync {
    async fn stats(&self, _runtime_id: &str) -> Result<metrics::RuntimeUsage> {
        Err(Error::Unavailable("runtime usage is unavailable".into()))
    }
    async fn checkpoint_supported(&self, _runtime: &str) -> Result<()> {
        Err(Error::Invalid(
            "runtime does not support checkpoint/restore".into(),
        ))
    }
    async fn checkpoint(
        &self,
        _runtime_id: &str,
        _path: &std::path::Path,
        _timeout: Duration,
    ) -> Result<()> {
        Err(Error::Invalid("runtime does not support checkpoint".into()))
    }
    /// Capture a local recovery point while keeping the same execution alive.
    async fn checkpoint_running(
        &self,
        _runtime_id: &str,
        _path: &std::path::Path,
        _timeout: Duration,
    ) -> Result<()> {
        Err(Error::Invalid(
            "runtime does not support running checkpoint".into(),
        ))
    }
    async fn restore(
        &self,
        _spec: &EnvironmentSpec,
        _runtime_id: &str,
        _generation: u64,
        _devices: &[DeviceAllocation],
        _path: &std::path::Path,
    ) -> Result<std::net::IpAddr> {
        Err(Error::Invalid("runtime does not support restore".into()))
    }
    async fn restore_from(
        &self,
        spec: &EnvironmentSpec,
        runtime_id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        path: &std::path::Path,
        origin: Option<&adx_core::runtime::RuntimeIdentity>,
    ) -> Result<std::net::IpAddr> {
        if origin.is_some() {
            return Err(Error::Invalid(
                "runtime does not support snapshot cloning".into(),
            ));
        }
        self.restore(spec, runtime_id, generation, devices, path)
            .await
    }
    /// Complete inventory of managed runtimes. Failure is never an empty list.
    async fn inventory(&self) -> Result<Vec<RuntimeObservation>> {
        Err(Error::Unavailable(
            "runtime inventory is unsupported".into(),
        ))
    }
    /// Correlate the supplied platform execution identity with a backend-generated
    /// ID. Do not start a second runtime while a previous Start is uncertain.
    async fn start(
        &self,
        spec: &EnvironmentSpec,
        runtime_id: &str,
        ownership_generation: u64,
        devices: &[DeviceAllocation],
    ) -> Result<std::net::IpAddr>;
    async fn is_running(&self, runtime_id: &str) -> Result<bool>;
    /// Atomically replace the complete runtime network policy. `None` clears it.
    async fn set_network_policy(
        &self,
        _runtime_id: &str,
        _policy: Option<&adx_core::sandbox::NetworkPolicy>,
        _ports: &[u16],
    ) -> Result<()> {
        Err(Error::Invalid(
            "runtime does not support network policy updates".into(),
        ))
    }
    /// Idempotent. Success means this runtime is confirmed absent and an older
    /// in-flight start cannot materialize it later. Uncertain cleanup is an error.
    async fn remove(&self, runtime_id: &str) -> Result<()>;
}

#[async_trait]
pub trait Readiness: Send + Sync {
    async fn activity(&self, _record: &EnvironmentRecord) -> Result<(u64, u64)> {
        Err(Error::Unavailable("runtime activity is unavailable".into()))
    }
    /// Runtime execution and the platform runtime service must both be ready.
    async fn wait_ready(&self, record: &EnvironmentRecord) -> Result<()>;
}

#[async_trait]
pub trait Routes: Send + Sync {
    /// (process session, cumulative activity revision, active streams).
    async fn activity(&self, _record: &EnvironmentRecord) -> Result<(String, u64, u64)> {
        Err(Error::Unavailable("proxy activity is unavailable".into()))
    }
    async fn begin_reconcile(&self) -> Result<()> {
        Ok(())
    }
    async fn finish_reconcile(&self) -> Result<()> {
        Ok(())
    }
    async fn ensure_synced(&self) -> Result<()> {
        Ok(())
    }

    async fn retire_orphan(&self, _runtime: &RuntimeObservation) -> Result<()> {
        Err(Error::Unavailable(
            "orphan binding retirement is unsupported".into(),
        ))
    }
    /// Return after the local binding is applied, not merely enqueued.
    async fn activate(&self, record: &EnvironmentRecord) -> Result<()>;
    /// Close local admission and retire sessions for this assignment identity.
    async fn retire(&self, record: &EnvironmentRecord) -> Result<()>;
}

#[async_trait]
pub trait StateSink: Send + Sync {
    /// Normal path: Coordinator -> cluster store. On outage, return Journaled only
    /// after a durable local append. Reject stale assignment generations and
    /// revisions. Repeated commits of an identical record are idempotent.
    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability>;
}

pub(crate) struct Admission {
    ledger: ResourceLedger,
    devices: DeviceLedger,
    devices_valid_until: Option<Instant>,
    valid_until: Option<Instant>,
    maintenance: bool,
    pressure: bool,
}

impl Admission {
    fn reserve(&mut self, id: &str, spec: &EnvironmentSpec, assignment: &Assignment) -> Result<()> {
        if self.maintenance
            || self.pressure
            || self.valid_until.is_none_or(|until| Instant::now() >= until)
            || (!spec.scheduling.devices.is_empty()
                && self
                    .devices_valid_until
                    .is_none_or(|until| Instant::now() >= until))
        {
            return Err(Error::NoCapacity);
        }
        let mut scalar = self.ledger.clone();
        let mut devices = self.devices.clone();
        scalar.reserve(id, spec.resources)?;
        devices.reserve(id, &spec.scheduling.devices, &assignment.devices)?;
        self.ledger = scalar;
        self.devices = devices;
        Ok(())
    }
    fn release(&mut self, id: &str) -> Result<()> {
        let mut scalar = self.ledger.clone();
        let mut devices = self.devices.clone();
        scalar.release(id)?;
        devices.release(id)?;
        self.ledger = scalar;
        self.devices = devices;
        Ok(())
    }
}

pub(crate) struct Services {
    runtime: Arc<dyn RuntimeDriver>,
    readiness: Arc<dyn Readiness>,
    routes: Arc<dyn Routes>,
    sink: Arc<dyn StateSink>,
    admission: Arc<Mutex<Admission>>,
    operation_timeout: Duration,
    checkpoint: Option<Arc<checkpoint::CheckpointServices>>,
    snapshots: Option<Arc<dyn checkpoint::SnapshotCatalog>>,
    metrics: metrics::Metrics,
    health_failure_threshold: Option<u32>,
}

pub struct Adxlet {
    node_id: String,
    local_holds: Mutex<BTreeMap<String, local::LocalHold>>,
    draining: std::sync::atomic::AtomicBool,
    lifecycle_ready: tokio::sync::RwLock<bool>,
    retired_generations: Mutex<BTreeMap<String, u64>>,
    services: Arc<Services>,
    environments: Mutex<BTreeMap<String, (EnvironmentSpec, Assignment, EnvironmentHandle)>>,
}

impl Adxlet {
    pub fn new(
        node_id: String,
        runtime: Arc<dyn RuntimeDriver>,
        readiness: Arc<dyn Readiness>,
        routes: Arc<dyn Routes>,
        sink: Arc<dyn StateSink>,
    ) -> Self {
        Self {
            node_id,
            local_holds: Mutex::default(),
            draining: std::sync::atomic::AtomicBool::new(false),
            lifecycle_ready: tokio::sync::RwLock::new(true),
            retired_generations: Mutex::default(),
            services: Arc::new(Services {
                runtime,
                readiness,
                routes,
                sink,
                admission: Arc::new(Mutex::new(Admission {
                    ledger: ResourceLedger::new(Resources::default()),
                    devices: DeviceLedger::default(),
                    devices_valid_until: None,
                    valid_until: None,
                    maintenance: false,
                    pressure: false,
                })),
                operation_timeout: Duration::from_secs(30),
                checkpoint: None,
                snapshots: None,
                metrics: metrics::Metrics::default(),
                health_failure_threshold: None,
            }),
            environments: Mutex::default(),
        }
    }

    pub fn with_snapshot_catalog(
        mut self,
        catalog: Arc<dyn checkpoint::SnapshotCatalog>,
    ) -> Result<Self> {
        Arc::get_mut(&mut self.services)
            .ok_or(Error::Conflict)?
            .snapshots = Some(catalog);
        Ok(self)
    }

    pub fn with_checkpointing(
        mut self,
        store: Arc<dyn checkpoint::CheckpointStore>,
        cooperation: Arc<dyn checkpoint::CheckpointCooperation>,
    ) -> Result<Self> {
        Arc::get_mut(&mut self.services)
            .ok_or(Error::Conflict)?
            .checkpoint = Some(Arc::new(checkpoint::CheckpointServices {
            store,
            cooperation,
        }));
        Ok(self)
    }

    pub async fn sync_proxy(&self) -> Result<()> {
        self.services.routes.ensure_synced().await
    }

    pub fn with_health_check(mut self, failure_threshold: Option<u32>) -> Result<Self> {
        if failure_threshold == Some(0) {
            return Err(Error::Invalid(
                "health failure threshold must be positive".into(),
            ));
        }
        Arc::get_mut(&mut self.services)
            .ok_or(Error::Conflict)?
            .health_failure_threshold = failure_threshold;
        Ok(self)
    }

    pub async fn pause_lifecycle(&self) {
        *self.lifecycle_ready.write().await = false;
    }

    pub fn update_capacity(&self, resources: Resources, valid_for: Duration) -> Result<()> {
        resources.validate()?;
        if valid_for.is_zero() {
            return Err(Error::Invalid("capacity lifetime must be positive".into()));
        }
        let until = Instant::now()
            .checked_add(valid_for)
            .ok_or_else(|| Error::Invalid("capacity lifetime overflow".into()))?;
        let mut admission = self
            .services
            .admission
            .lock()
            .expect("shared state lock poisoned");
        admission.ledger.set_capacity(resources);
        admission.valid_until = Some(until);
        Ok(())
    }

    /// Refresh physical inventory without erasing allocations, including cards
    /// that disappeared or became unhealthy while an execution still holds them.
    pub fn update_devices(&self, devices: Vec<Device>, valid_for: Duration) -> Result<()> {
        if valid_for.is_zero() {
            return Err(Error::Invalid("device lifetime must be positive".into()));
        }
        let until = Instant::now()
            .checked_add(valid_for)
            .ok_or_else(|| Error::Invalid("device lifetime overflow".into()))?;
        let mut admission = self
            .services
            .admission
            .lock()
            .expect("shared state lock poisoned");
        admission.devices.update(devices)?;
        admission.devices_valid_until = Some(until);
        Ok(())
    }

    /// Configure before obtaining any Environment handles.
    pub fn with_operation_timeout(mut self, duration: Duration) -> Result<Self> {
        if duration.is_zero() {
            return Err(Error::Invalid("operation timeout must be positive".into()));
        }
        Arc::get_mut(&mut self.services)
            .ok_or(Error::Conflict)?
            .operation_timeout = duration;
        Ok(self)
    }

    pub fn set_maintenance(&self, maintenance: bool) {
        self.services
            .admission
            .lock()
            .expect("shared state lock poisoned")
            .maintenance = maintenance;
    }

    pub fn set_pressure(&self, under_pressure: bool) {
        self.services
            .admission
            .lock()
            .expect("shared state lock poisoned")
            .pressure = under_pressure;
    }

    pub fn accepting_allocations(&self) -> bool {
        if !self.lifecycle_ready.try_read().is_ok_and(|ready| *ready) {
            return false;
        }
        let admission = self
            .services
            .admission
            .lock()
            .expect("shared state lock poisoned");
        !admission.maintenance
            && !admission.pressure
            && admission
                .valid_until
                .is_some_and(|until| Instant::now() < until)
    }

    pub fn used(&self) -> Resources {
        self.services
            .admission
            .lock()
            .expect("shared state lock poisoned")
            .ledger
            .used()
    }

    pub fn environment(
        &self,
        spec: EnvironmentSpec,
        assignment: Assignment,
    ) -> Result<EnvironmentHandle> {
        if self.is_draining() {
            return Err(Error::Unavailable("node is draining".into()));
        }
        spec.validate()?;
        validate_device_assignment(&spec.scheduling.devices, &assignment.devices)?;
        if assignment.node_id != self.node_id
            || assignment.environment_id != spec.id
            || assignment.generation == 0
        {
            return Err(Error::Conflict);
        }
        if self
            .retired_generations
            .lock()
            .expect("shared state lock poisoned")
            .get(&spec.id)
            .is_some_and(|g| assignment.generation <= *g)
        {
            return Err(Error::Conflict);
        }
        let mut environments = self
            .environments
            .lock()
            .expect("shared state lock poisoned");
        if let Some((existing, owner, handle)) = environments.get(&spec.id) {
            return if *existing == spec && *owner == assignment {
                Ok(handle.clone())
            } else {
                Err(Error::Conflict)
            };
        }
        // The registry lock makes controller creation atomic across concurrent callers.
        let held = self.adopt_local(&spec, &assignment)?;
        let handle = controller::spawn(
            spec.clone(),
            assignment.clone(),
            self.services.clone(),
            held,
        );
        environments.insert(spec.id.clone(), (spec, assignment, handle.clone()));
        Ok(handle)
    }
}

impl Adxlet {
    /// Best effort across independent environments; each expiration is serialized
    /// with that environment's accepted lifecycle operations.
    pub async fn expire_checkpoints(&self) -> Result<()> {
        let gate = self.lifecycle_ready.read().await;
        if !*gate || self.is_draining() {
            return Ok(());
        }
        let handles: Vec<_> = self
            .environments
            .lock()
            .expect("shared state lock poisoned")
            .values()
            .map(|(_, _, h)| h.clone())
            .collect();
        let now = checkpoint::now()?;
        let mut failure = None;
        for h in handles {
            if let Err(e) = h.expire_checkpoint(now).await {
                failure = Some(e);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

impl Adxlet {
    /// Each check enters the Environment's serial controller. Bound fan-out so a
    /// slow backend cannot create an unbounded number of node monitoring tasks.
    pub async fn monitor_environments(&self) -> Result<()> {
        let gate = self.lifecycle_ready.read().await;
        if !*gate || self.is_draining() {
            return Ok(());
        }
        let handles: Vec<_> = self
            .environments
            .lock()
            .expect("shared state lock poisoned")
            .values()
            .map(|(_, _, h)| h.clone())
            .collect();
        let mut error = None;
        for chunk in handles.chunks(32) {
            let mut tasks = tokio::task::JoinSet::new();
            for handle in chunk {
                let handle = handle.clone();
                tasks.spawn(async move { handle.tick().await });
            }
            while let Some(result) = tasks.join_next().await {
                match result {
                    Ok(Ok(())) => (),
                    Ok(Err(e)) => error = Some(e),
                    Err(e) => error = Some(Error::Unavailable(format!("lifecycle monitor: {e}"))),
                }
            }
        }
        error.map_or(Ok(()), Err)
    }
}
