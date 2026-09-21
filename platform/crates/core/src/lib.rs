//! Instance and resource contracts shared by the control plane.

pub mod checkpoint;
pub mod environment;
pub mod lifecycle;
pub mod snapshots;
pub use checkpoint::{
    valid_runtime_id, CheckpointArtifact, CompletedOperation, LifecycleKind, RestorePoint,
};
pub mod runtime;
pub mod sandbox;
pub mod scheduling;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("request conflicts with existing state")]
    Conflict,
    #[error("insufficient resources or node admission closed")]
    NoCapacity,
    #[error("record not found")]
    NotFound,
    #[error("dependency unavailable: {0}")]
    Unavailable(String),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resources {
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub disk_bytes: u64,
}

impl Resources {
    pub fn validate(&self) -> Result<()> {
        if self.cpu_millis == 0 || self.memory_bytes == 0 {
            return Err(Error::Invalid("CPU and memory must be positive".into()));
        }
        Ok(())
    }

    pub fn fits(&self, available: &Self) -> bool {
        self.cpu_millis <= available.cpu_millis
            && self.memory_bytes <= available.memory_bytes
            && self.disk_bytes <= available.disk_bytes
    }

    pub fn saturating_sub(self, other: Self) -> Self {
        Self {
            cpu_millis: self.cpu_millis.saturating_sub(other.cpu_millis),
            memory_bytes: self.memory_bytes.saturating_sub(other.memory_bytes),
            disk_bytes: self.disk_bytes.saturating_sub(other.disk_bytes),
        }
    }

    fn checked_add(self, other: Self) -> Result<Self> {
        let add = |a: u64, b: u64| a.checked_add(b).ok_or(Error::NoCapacity);
        Ok(Self {
            cpu_millis: add(self.cpu_millis, other.cpu_millis)?,
            memory_bytes: add(self.memory_bytes, other.memory_bytes)?,
            disk_bytes: add(self.disk_bytes, other.disk_bytes)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceSpec {
    #[serde(default)]
    pub runtime_environment: Option<environment::RuntimeEnvironment>,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub lifecycle: lifecycle::LifecyclePolicy,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub id: String,
    pub tenant_id: String,
    pub image: String,
    pub runtime: String,
    pub resources: Resources,
    pub priority: i32,
    #[serde(default)]
    pub scheduling: scheduling::SchedulingPolicy,
    #[serde(default)]
    pub sandbox: sandbox::SandboxOptions,
}

impl InstanceSpec {
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("id", &self.id),
            ("tenant", &self.tenant_id),
            ("runtime", &self.runtime),
        ] {
            if value.trim().is_empty() {
                return Err(Error::Invalid(format!("{name} is required")));
            }
        }
        if let Some(environment) = &self.runtime_environment {
            environment.validate()?;
        } else if self.image.trim().is_empty() && self.sandbox.rootfs.is_none() {
            return Err(Error::Invalid(
                "image or runtime environment is required".into(),
            ));
        }
        self.lifecycle.validate()?;
        self.resources.validate()?;
        self.scheduling.validate()?;
        self.sandbox.validate(&self.resources)
    }
}

/// In-memory accounting. A resource observation updates capacity, never usage.
#[derive(Debug, Clone)]
pub struct ResourceLedger {
    capacity: Resources,
    used: Resources,
    reservations: BTreeMap<String, Resources>,
}

impl ResourceLedger {
    pub fn new(capacity: Resources) -> Self {
        Self {
            capacity,
            used: Resources::default(),
            reservations: BTreeMap::new(),
        }
    }
    pub fn set_capacity(&mut self, capacity: Resources) {
        self.capacity = capacity;
    }
    pub fn capacity(&self) -> Resources {
        self.capacity
    }
    pub fn used(&self) -> Resources {
        self.used
    }
    pub fn available(&self) -> Resources {
        self.capacity.saturating_sub(self.used)
    }
    pub fn reserve(&mut self, id: &str, resources: Resources) -> Result<()> {
        resources.validate()?;
        if let Some(existing) = self.reservations.get(id) {
            return if *existing == resources {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        if !resources.fits(&self.available()) {
            return Err(Error::NoCapacity);
        }
        self.used = self.used.checked_add(resources)?;
        self.reservations.insert(id.into(), resources);
        Ok(())
    }
    pub fn release(&mut self, id: &str) -> Result<()> {
        let resources = self.reservations.remove(id).ok_or(Error::NotFound)?;
        self.used = self.used.saturating_sub(resources);
        Ok(())
    }
    /// Rebuild authoritative usage after restart. Existing usage may exceed a
    /// newly reduced capacity; retain it and make new admission fail closed.
    pub fn restore(&mut self, id: &str, resources: Resources) -> Result<()> {
        resources.validate()?;
        if let Some(old) = self.reservations.get(id) {
            return if *old == resources {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        self.used = self.used.checked_add(resources)?;
        self.reservations.insert(id.into(), resources);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceState {
    Pending,
    Starting,
    Running,
    Pausing,
    Paused,
    Resuming,
    Deleting,
    Deleted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Start,
    Ready,
    Pause,
    Checkpointed,
    Resume,
    Rollback,
    Delete,
    Removed,
    Fail,
}

impl InstanceState {
    /// Pure transition rules; only the node controller applies these events.
    pub fn apply(self, event: Event) -> Result<Self> {
        use Event::*;
        use InstanceState::*;
        match (self, event) {
            (Pending | Failed, Start) => Ok(Starting),
            (Starting | Resuming, Ready) => Ok(Running),
            (Running, Pause) => Ok(Pausing),
            (Pausing, Checkpointed) => Ok(Paused),
            (Paused, Resume) => Ok(Resuming),
            (Pausing, Rollback) => Ok(Running),
            (Resuming, Rollback) => Ok(Paused),
            (Pending | Starting | Running | Pausing | Paused | Resuming | Failed, Delete) => {
                Ok(Deleting)
            }
            (Deleting, Removed) => Ok(Deleted),
            (Starting | Running | Pausing | Paused | Resuming | Deleting, Fail) => Ok(Failed),
            _ => Err(Error::Conflict),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    pub instance_id: String,
    pub node_id: String,
    #[serde(alias = "domain_id")]
    pub shard_id: usize,
    pub generation: u64,
    #[serde(default)]
    pub devices: Vec<scheduling::DeviceAllocation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceRecord {
    #[serde(default)]
    pub restart_attempts: u32,
    #[serde(default)]
    pub restart_pending: bool,
    pub spec: InstanceSpec,
    pub assignment: Assignment,
    pub state: InstanceState,
    pub revision: u64,
    pub runtime_id: String,
    /// Failed does not imply cleanup. Keep capacity reserved while this is true.
    pub resources_held: bool,
    pub runtime_ip: Option<std::net::IpAddr>,
    #[serde(default)]
    pub checkpoint: Option<RestorePoint>,
    #[serde(default)]
    pub last_operation: Option<CompletedOperation>,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn r(n: u64) -> Resources {
        Resources {
            cpu_millis: n,
            memory_bytes: n,
            disk_bytes: n,
        }
    }

    #[test]
    fn capacity_refresh_does_not_erase_allocations() {
        let mut ledger = ResourceLedger::new(r(10));
        ledger.reserve("a", r(8)).unwrap();
        ledger.set_capacity(r(4));
        assert_eq!(ledger.available(), r(0));
        assert_eq!(ledger.reserve("b", r(1)), Err(Error::NoCapacity));
        ledger.release("a").unwrap();
        assert_eq!(ledger.available(), r(4));
    }

    #[test]
    fn reservation_is_atomic_at_numeric_limits() {
        let mut ledger = ResourceLedger::new(r(u64::MAX));
        ledger.reserve("a", r(u64::MAX)).unwrap();
        assert_eq!(ledger.reserve("b", r(1)), Err(Error::NoCapacity));
        assert_eq!(ledger.used(), r(u64::MAX));
        assert_eq!(ledger.reserve("a", r(1)), Err(Error::Conflict));
    }

    #[test]
    fn lifecycle_cannot_publish_running_before_start_or_revive_deleted() {
        assert_eq!(
            InstanceState::Pending.apply(Event::Ready),
            Err(Error::Conflict)
        );
        assert_eq!(
            InstanceState::Deleted.apply(Event::Start),
            Err(Error::Conflict)
        );
        assert_eq!(
            InstanceState::Starting.apply(Event::Ready),
            Ok(InstanceState::Running)
        );
    }
}
