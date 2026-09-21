//! Generated transport contracts and validated conversions, without services.
mod environment;
mod snapshots;
pub mod control {
    tonic::include_proto!("adx.control.v1");
}

pub use adx_core::valid_runtime_id;
use adx_core::{Error, Result};
mod sandbox;
mod scheduling;

impl From<adx_core::Resources> for control::Resources {
    fn from(value: adx_core::Resources) -> Self {
        Self {
            cpu_millis: value.cpu_millis,
            memory_bytes: value.memory_bytes,
            disk_bytes: value.disk_bytes,
        }
    }
}

impl TryFrom<control::CapsuleSpec> for adx_core::CapsuleSpec {
    type Error = Error;
    fn try_from(value: control::CapsuleSpec) -> Result<Self> {
        let resources = value
            .resources
            .ok_or_else(|| Error::Invalid("resources are required".into()))?;
        let spec = Self {
            environment: value.environment.map(TryInto::try_into).transpose()?,
            snapshot_id: value.snapshot_id,
            lifecycle: value.lifecycle.map(Into::into).unwrap_or_default(),
            env: value.env.into_iter().collect(),
            id: value.id,
            tenant_id: value.tenant_id,
            image: value.image,
            runtime_class: value.runtime_class,
            resources: adx_core::Resources {
                cpu_millis: resources.cpu_millis,
                memory_bytes: resources.memory_bytes,
                disk_bytes: resources.disk_bytes,
            },
            priority: value.priority,
            scheduling: value
                .scheduling
                .map(TryInto::try_into)
                .transpose()?
                .unwrap_or_default(),
            sandbox: value
                .sandbox
                .map(TryInto::try_into)
                .transpose()?
                .unwrap_or_default(),
        };
        spec.validate()?;
        Ok(spec)
    }
}

impl From<adx_core::CapsuleSpec> for control::CapsuleSpec {
    fn from(value: adx_core::CapsuleSpec) -> Self {
        Self {
            environment: value.environment.map(Into::into),
            snapshot_id: value.snapshot_id,
            lifecycle: Some(value.lifecycle.into()),
            env: value.env.into_iter().collect(),
            id: value.id,
            tenant_id: value.tenant_id,
            image: value.image,
            runtime_class: value.runtime_class,
            resources: Some(value.resources.into()),
            priority: value.priority,
            scheduling: Some(value.scheduling.into()),
            sandbox: Some(value.sandbox.into()),
        }
    }
}

impl TryFrom<control::Assignment> for adx_core::Assignment {
    type Error = Error;
    fn try_from(value: control::Assignment) -> Result<Self> {
        if value.capsule_id.trim().is_empty()
            || value.node_id.trim().is_empty()
            || value.generation == 0
        {
            return Err(Error::Invalid(
                "assignment requires capsule, node and generation".into(),
            ));
        }
        let devices: Vec<adx_core::scheduling::DeviceAllocation> = value
            .devices
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_>>()?;
        adx_core::scheduling::validate_allocations(&devices)?;
        Ok(Self {
            devices,
            capsule_id: value.capsule_id,
            node_id: value.node_id,
            shard_id: value.shard_id as usize,
            generation: value.generation,
        })
    }
}

impl TryFrom<adx_core::Assignment> for control::Assignment {
    type Error = Error;
    fn try_from(value: adx_core::Assignment) -> Result<Self> {
        adx_core::scheduling::validate_allocations(&value.devices)?;
        Ok(Self {
            devices: value.devices.into_iter().map(Into::into).collect(),
            capsule_id: value.capsule_id,
            node_id: value.node_id,
            shard_id: u32::try_from(value.shard_id)
                .map_err(|_| Error::Invalid("shard id overflow".into()))?,
            generation: value.generation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn missing_resources_and_zero_generation_are_rejected() {
        assert!(adx_core::CapsuleSpec::try_from(control::CapsuleSpec::default()).is_err());
        assert!(adx_core::Assignment::try_from(control::Assignment::default()).is_err());
    }

    #[test]
    fn capsule_spec_round_trip_preserves_units_and_priority() {
        let value = adx_core::CapsuleSpec {
            environment: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            id: "a".into(),
            tenant_id: "t".into(),
            image: "image".into(),
            runtime_class: "runtime".into(),
            resources: adx_core::Resources {
                cpu_millis: 1500,
                memory_bytes: 2 * 1024 * 1024 * 1024,
                disk_bytes: u64::MAX,
            },
            priority: -10,
            sandbox: Default::default(),
        };
        let bytes = control::CapsuleSpec::from(value.clone()).encode_to_vec();
        let decoded = control::CapsuleSpec::decode(bytes.as_slice()).unwrap();
        assert_eq!(adx_core::CapsuleSpec::try_from(decoded).unwrap(), value);
    }
}

/// Node-local binding and activity contracts.
pub mod node_proxy {
    tonic::include_proto!("adx.node.v1");
}

pub mod auth;

impl TryFrom<control::CapsuleRecord> for adx_core::CapsuleRecord {
    type Error = Error;
    fn try_from(v: control::CapsuleRecord) -> Result<Self> {
        let spec: adx_core::CapsuleSpec = v
            .spec
            .ok_or_else(|| Error::Invalid("spec required".into()))?
            .try_into()?;
        let assignment: adx_core::Assignment = v
            .assignment
            .ok_or_else(|| Error::Invalid("assignment required".into()))?
            .try_into()?;
        if spec.id != assignment.capsule_id {
            return Err(Error::Conflict);
        }
        adx_core::scheduling::validate_device_assignment(
            &spec.scheduling.devices,
            &assignment.devices,
        )?;
        let state = match control::CapsuleState::try_from(v.state) {
            Ok(control::CapsuleState::Pending) => adx_core::CapsuleState::Pending,
            Ok(control::CapsuleState::Starting) => adx_core::CapsuleState::Starting,
            Ok(control::CapsuleState::Running) => adx_core::CapsuleState::Running,
            Ok(control::CapsuleState::Deleting) => adx_core::CapsuleState::Deleting,
            Ok(control::CapsuleState::Deleted) => adx_core::CapsuleState::Deleted,
            Ok(control::CapsuleState::Pausing) => adx_core::CapsuleState::Pausing,
            Ok(control::CapsuleState::Paused) => adx_core::CapsuleState::Paused,
            Ok(control::CapsuleState::Resuming) => adx_core::CapsuleState::Resuming,
            Ok(control::CapsuleState::Failed) => adx_core::CapsuleState::Failed,
            _ => return Err(Error::Invalid("unknown capsule state".into())),
        };
        let runtime = v
            .runtime
            .ok_or_else(|| Error::Invalid("runtime required".into()))?;
        let runtime_ip = if runtime.ip.is_empty() {
            None
        } else {
            Some(
                runtime
                    .ip
                    .parse()
                    .map_err(|_| Error::Invalid("invalid runtime IP".into()))?,
            )
        };
        Ok(Self {
            spec,
            assignment,
            state,
            revision: v.revision,
            restart_attempts: v.restart_attempts,
            restart_pending: v.restart_pending,
            runtime: adx_core::Runtime {
                id: runtime.id,
                ip: runtime_ip,
            },
            resources_held: v.resources_held,
            checkpoint: v.checkpoint.map(TryInto::try_into).transpose()?,
            last_operation: v.last_operation.map(TryInto::try_into).transpose()?,
        })
    }
}
impl TryFrom<adx_core::CapsuleRecord> for control::CapsuleRecord {
    type Error = Error;
    fn try_from(v: adx_core::CapsuleRecord) -> Result<Self> {
        let state = match v.state {
            adx_core::CapsuleState::Pending => control::CapsuleState::Pending,
            adx_core::CapsuleState::Starting => control::CapsuleState::Starting,
            adx_core::CapsuleState::Running => control::CapsuleState::Running,
            adx_core::CapsuleState::Deleting => control::CapsuleState::Deleting,
            adx_core::CapsuleState::Deleted => control::CapsuleState::Deleted,
            adx_core::CapsuleState::Pausing => control::CapsuleState::Pausing,
            adx_core::CapsuleState::Paused => control::CapsuleState::Paused,
            adx_core::CapsuleState::Resuming => control::CapsuleState::Resuming,
            adx_core::CapsuleState::Failed => control::CapsuleState::Failed,
        };
        Ok(Self {
            spec: Some(v.spec.into()),
            assignment: Some(v.assignment.try_into()?),
            state: state as i32,
            revision: v.revision,
            restart_attempts: v.restart_attempts,
            restart_pending: v.restart_pending,
            runtime: Some(control::Runtime {
                id: v.runtime.id,
                ip: v.runtime.ip.map(|ip| ip.to_string()).unwrap_or_default(),
            }),
            resources_held: v.resources_held,
            checkpoint: v.checkpoint.map(Into::into),
            last_operation: v.last_operation.map(Into::into),
        })
    }
}
pub fn status(error: adx_core::Error) -> tonic::Status {
    match error {
        Error::Invalid(message) => tonic::Status::invalid_argument(message),
        Error::Conflict => tonic::Status::failed_precondition("identity or version conflict"),
        Error::NotFound => tonic::Status::not_found("capsule or node not found"),
        Error::NoCapacity => tonic::Status::resource_exhausted("capacity unavailable"),
        Error::Unavailable(message) => tonic::Status::unavailable(message),
    }
}
pub fn dependency_status(status: tonic::Status) -> Error {
    match status.code() {
        tonic::Code::FailedPrecondition => Error::Conflict,
        tonic::Code::NotFound => Error::NotFound,
        tonic::Code::ResourceExhausted => Error::NoCapacity,
        tonic::Code::InvalidArgument => Error::Invalid("remote request rejected".into()),
        _ => Error::Unavailable("control RPC unavailable".into()),
    }
}

impl From<adx_core::RestorePoint> for control::RestorePoint {
    fn from(v: adx_core::RestorePoint) -> Self {
        Self {
            id: v.id,
            expires_at_unix_seconds: v.expires_at_unix_seconds,
            origin: v.origin.map(Into::into),
            source_runtime_id: v.source_runtime_id,
            artifact: Some(control::CheckpointArtifact {
                storage: v.artifact.storage,
                location: v.artifact.location,
                size_bytes: v.artifact.size_bytes,
            }),
        }
    }
}
impl TryFrom<control::RestorePoint> for adx_core::RestorePoint {
    type Error = Error;
    fn try_from(v: control::RestorePoint) -> Result<Self> {
        let a = v
            .artifact
            .ok_or_else(|| Error::Invalid("checkpoint artifact required".into()))?;
        if v.id.is_empty()
            || v.source_runtime_id.is_empty()
            || v.expires_at_unix_seconds == 0
            || a.storage.is_empty()
            || a.location.is_empty()
            || a.size_bytes == 0
        {
            return Err(Error::Invalid("invalid checkpoint metadata".into()));
        }
        Ok(Self {
            id: v.id,
            expires_at_unix_seconds: v.expires_at_unix_seconds,
            origin: v.origin.map(TryInto::try_into).transpose()?,
            source_runtime_id: v.source_runtime_id,
            artifact: adx_core::CheckpointArtifact {
                storage: a.storage,
                location: a.location,
                size_bytes: a.size_bytes,
            },
        })
    }
}
impl From<adx_core::CompletedOperation> for control::CompletedOperation {
    fn from(v: adx_core::CompletedOperation) -> Self {
        Self {
            id: v.id,
            expected_revision: v.expected_revision,
            kind: match v.kind {
                adx_core::LifecycleKind::Pause => 1,
                adx_core::LifecycleKind::Resume => 2,
                adx_core::LifecycleKind::Snapshot => 3,
                adx_core::LifecycleKind::Network => 4,
                adx_core::LifecycleKind::Reload => 5,
            },
        }
    }
}
impl TryFrom<control::CompletedOperation> for adx_core::CompletedOperation {
    type Error = Error;
    fn try_from(v: control::CompletedOperation) -> Result<Self> {
        if v.id.trim().is_empty() || v.id.len() > 128 || v.expected_revision == 0 {
            return Err(Error::Invalid("invalid completed operation".into()));
        }
        Ok(Self {
            id: v.id,
            expected_revision: v.expected_revision,
            kind: match v.kind {
                1 => adx_core::LifecycleKind::Pause,
                2 => adx_core::LifecycleKind::Resume,
                3 => adx_core::LifecycleKind::Snapshot,
                4 => adx_core::LifecycleKind::Network,
                5 => adx_core::LifecycleKind::Reload,
                _ => return Err(Error::Invalid("invalid lifecycle kind".into())),
            },
        })
    }
}

impl From<control::LifecyclePolicy> for adx_core::lifecycle::LifecyclePolicy {
    fn from(v: control::LifecyclePolicy) -> Self {
        Self {
            idle_timeout_seconds: v.idle_timeout_seconds,
            restart: v.restart.map(|r| adx_core::lifecycle::RestartPolicy {
                max_attempts: r.max_attempts,
                initial_backoff_seconds: r.initial_backoff_seconds,
                max_backoff_seconds: r.max_backoff_seconds,
            }),
        }
    }
}
impl From<adx_core::lifecycle::LifecyclePolicy> for control::LifecyclePolicy {
    fn from(v: adx_core::lifecycle::LifecyclePolicy) -> Self {
        Self {
            idle_timeout_seconds: v.idle_timeout_seconds,
            restart: v.restart.map(|r| control::RestartPolicy {
                max_attempts: r.max_attempts,
                initial_backoff_seconds: r.initial_backoff_seconds,
                max_backoff_seconds: r.max_backoff_seconds,
            }),
        }
    }
}

impl From<adx_core::runtime::RuntimeIdentity> for control::RuntimeIdentity {
    fn from(v: adx_core::runtime::RuntimeIdentity) -> Self {
        Self {
            capsule_id: v.capsule_id,
            runtime_id: v.runtime_id,
            ownership_generation: v.ownership_generation,
        }
    }
}
impl TryFrom<control::RuntimeIdentity> for adx_core::runtime::RuntimeIdentity {
    type Error = Error;
    fn try_from(v: control::RuntimeIdentity) -> Result<Self> {
        let identity = Self {
            capsule_id: v.capsule_id,
            runtime_id: v.runtime_id,
            ownership_generation: v.ownership_generation,
        };
        identity.validate()?;
        Ok(identity)
    }
}
