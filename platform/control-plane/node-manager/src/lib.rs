//! Node-local admission and serial Instance lifecycle ownership.

pub mod activity;
pub mod admin;
mod controller;
pub mod readiness;
mod reconciliation;
pub mod routes;
pub mod rpc;
pub mod runtime_control;
pub mod sandboxd;

use adx_core::scheduling::{validate_device_assignment, Device, DeviceAllocation, DeviceLedger};
use adx_core::{
    Assignment, Error, InstanceRecord, InstanceSpec, ResourceLedger, Resources, Result,
};
use async_trait::async_trait;
pub use controller::InstanceHandle;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Master has committed the record to cluster storage.
    Published,
    /// A local durable degradation journal accepted it; cluster routing is stale.
    Journaled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationResult {
    pub record: InstanceRecord,
    pub durability: Durability,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeObservation {
    pub instance_id: String,
    pub runtime_id: String,
    pub generation: u64,
    pub tenant_id: String,
    pub running: bool,
}

#[async_trait]
pub trait RuntimeBackend: Send + Sync {
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
        spec: &InstanceSpec,
        runtime_id: &str,
        ownership_generation: u64,
        devices: &[DeviceAllocation],
    ) -> Result<std::net::IpAddr>;
    async fn is_running(&self, runtime_id: &str) -> Result<bool>;
    /// Idempotent. Success means this runtime is confirmed absent and an older
    /// in-flight start cannot materialize it later. Uncertain cleanup is an error.
    async fn remove(&self, runtime_id: &str) -> Result<()>;
}

#[async_trait]
pub trait Readiness: Send + Sync {
    /// Runtime execution and the platform runtime service must both be ready.
    async fn wait_ready(&self, record: &InstanceRecord) -> Result<()>;
}

#[async_trait]
pub trait Routes: Send + Sync {
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
    async fn activate(&self, record: &InstanceRecord) -> Result<()>;
    /// Close local admission and retire sessions for this assignment identity.
    async fn retire(&self, record: &InstanceRecord) -> Result<()>;
}

#[async_trait]
pub trait StateSink: Send + Sync {
    /// Normal path: Master -> cluster store. On outage, return Journaled only
    /// after a durable local append. Reject stale assignment generations and
    /// revisions. Repeated commits of an identical record are idempotent.
    async fn commit(&self, record: &InstanceRecord) -> Result<Durability>;
}

pub(crate) struct Admission {
    ledger: ResourceLedger,
    devices: DeviceLedger,
    devices_valid_until: Option<Instant>,
    valid_until: Option<Instant>,
    maintenance: bool,
}

impl Admission {
    fn reserve(&mut self, id: &str, spec: &InstanceSpec, assignment: &Assignment) -> Result<()> {
        if self.maintenance
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
    runtime: Arc<dyn RuntimeBackend>,
    readiness: Arc<dyn Readiness>,
    routes: Arc<dyn Routes>,
    sink: Arc<dyn StateSink>,
    admission: Arc<Mutex<Admission>>,
    operation_timeout: Duration,
}

pub struct NodeManager {
    node_id: String,
    draining: std::sync::atomic::AtomicBool,
    lifecycle_ready: tokio::sync::RwLock<bool>,
    retired_generations: Mutex<BTreeMap<String, u64>>,
    services: Arc<Services>,
    instances: Mutex<BTreeMap<String, (InstanceSpec, Assignment, InstanceHandle)>>,
}

impl NodeManager {
    pub fn new(
        node_id: String,
        runtime: Arc<dyn RuntimeBackend>,
        readiness: Arc<dyn Readiness>,
        routes: Arc<dyn Routes>,
        sink: Arc<dyn StateSink>,
    ) -> Self {
        Self {
            node_id,
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
                })),
                operation_timeout: Duration::from_secs(30),
            }),
            instances: Mutex::default(),
        }
    }

    pub async fn sync_proxy(&self) -> Result<()> {
        self.services.routes.ensure_synced().await
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
        let mut admission = self.services.admission.lock().unwrap();
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
        let mut admission = self.services.admission.lock().unwrap();
        admission.devices.update(devices)?;
        admission.devices_valid_until = Some(until);
        Ok(())
    }

    /// Configure before obtaining any Instance handles.
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
        self.services.admission.lock().unwrap().maintenance = maintenance;
    }

    pub fn used(&self) -> Resources {
        self.services.admission.lock().unwrap().ledger.used()
    }

    pub fn instance(&self, spec: InstanceSpec, assignment: Assignment) -> Result<InstanceHandle> {
        if self.is_draining() {
            return Err(Error::Unavailable("node is draining".into()));
        }
        spec.validate()?;
        validate_device_assignment(&spec.scheduling.devices, &assignment.devices)?;
        if assignment.node_id != self.node_id
            || assignment.instance_id != spec.id
            || assignment.generation == 0
        {
            return Err(Error::Conflict);
        }
        if self
            .retired_generations
            .lock()
            .unwrap()
            .get(&spec.id)
            .is_some_and(|g| assignment.generation <= *g)
        {
            return Err(Error::Conflict);
        }
        let mut instances = self.instances.lock().unwrap();
        if let Some((existing, owner, handle)) = instances.get(&spec.id) {
            return if *existing == spec && *owner == assignment {
                Ok(handle.clone())
            } else {
                Err(Error::Conflict)
            };
        }
        // The registry lock makes controller creation atomic across concurrent callers.
        let handle = controller::spawn(spec.clone(), assignment.clone(), self.services.clone());
        instances.insert(spec.id.clone(), (spec, assignment, handle.clone()));
        Ok(handle)
    }
}
