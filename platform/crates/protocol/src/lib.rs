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

impl TryFrom<control::InstanceSpec> for adx_core::InstanceSpec {
    type Error = Error;
    fn try_from(value: control::InstanceSpec) -> Result<Self> {
        let resources = value
            .resources
            .ok_or_else(|| Error::Invalid("resources are required".into()))?;
        let spec = Self {
            runtime_environment: value
                .runtime_environment
                .map(TryInto::try_into)
                .transpose()?,
            snapshot_id: value.snapshot_id,
            lifecycle: value.lifecycle.map(Into::into).unwrap_or_default(),
            env: value.env.into_iter().collect(),
            id: value.id,
            tenant_id: value.tenant_id,
            image: value.image,
            runtime: value.runtime,
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

impl From<adx_core::InstanceSpec> for control::InstanceSpec {
    fn from(value: adx_core::InstanceSpec) -> Self {
        Self {
            runtime_environment: value.runtime_environment.map(Into::into),
            snapshot_id: value.snapshot_id,
            lifecycle: Some(value.lifecycle.into()),
            env: value.env.into_iter().collect(),
            id: value.id,
            tenant_id: value.tenant_id,
            image: value.image,
            runtime: value.runtime,
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
        if value.instance_id.trim().is_empty()
            || value.node_id.trim().is_empty()
            || value.generation == 0
        {
            return Err(Error::Invalid(
                "assignment requires instance, node and generation".into(),
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
            instance_id: value.instance_id,
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
            instance_id: value.instance_id,
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
        assert!(adx_core::InstanceSpec::try_from(control::InstanceSpec::default()).is_err());
        assert!(adx_core::Assignment::try_from(control::Assignment::default()).is_err());
    }

    #[test]
    fn instance_spec_round_trip_preserves_units_and_priority() {
        let value = adx_core::InstanceSpec {
            runtime_environment: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            id: "a".into(),
            tenant_id: "t".into(),
            image: "image".into(),
            runtime: "runtime".into(),
            resources: adx_core::Resources {
                cpu_millis: 1500,
                memory_bytes: 2 * 1024 * 1024 * 1024,
                disk_bytes: u64::MAX,
            },
            priority: -10,
            sandbox: Default::default(),
        };
        let bytes = control::InstanceSpec::from(value.clone()).encode_to_vec();
        let decoded = control::InstanceSpec::decode(bytes.as_slice()).unwrap();
        assert_eq!(adx_core::InstanceSpec::try_from(decoded).unwrap(), value);
    }
}

/// Node-local binding and activity contracts.
pub mod node_proxy {
    tonic::include_proto!("adx.node.v1");
}

pub mod auth;

impl TryFrom<control::InstanceRecord> for adx_core::InstanceRecord {
    type Error = Error;
    fn try_from(v: control::InstanceRecord) -> Result<Self> {
        let spec: adx_core::InstanceSpec = v
            .spec
            .ok_or_else(|| Error::Invalid("spec required".into()))?
            .try_into()?;
        let assignment: adx_core::Assignment = v
            .assignment
            .ok_or_else(|| Error::Invalid("assignment required".into()))?
            .try_into()?;
        if spec.id != assignment.instance_id {
            return Err(Error::Conflict);
        }
        adx_core::scheduling::validate_device_assignment(
            &spec.scheduling.devices,
            &assignment.devices,
        )?;
        let state = match control::InstanceState::try_from(v.state) {
            Ok(control::InstanceState::Pending) => adx_core::InstanceState::Pending,
            Ok(control::InstanceState::Starting) => adx_core::InstanceState::Starting,
            Ok(control::InstanceState::Running) => adx_core::InstanceState::Running,
            Ok(control::InstanceState::Deleting) => adx_core::InstanceState::Deleting,
            Ok(control::InstanceState::Deleted) => adx_core::InstanceState::Deleted,
            Ok(control::InstanceState::Pausing) => adx_core::InstanceState::Pausing,
            Ok(control::InstanceState::Paused) => adx_core::InstanceState::Paused,
            Ok(control::InstanceState::Resuming) => adx_core::InstanceState::Resuming,
            Ok(control::InstanceState::Failed) => adx_core::InstanceState::Failed,
            _ => return Err(Error::Invalid("unknown instance state".into())),
        };
        let runtime_ip = if v.runtime_ip.is_empty() {
            None
        } else {
            Some(
                v.runtime_ip
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
            runtime_id: v.runtime_id,
            resources_held: v.resources_held,
            runtime_ip,
            checkpoint: v.checkpoint.map(TryInto::try_into).transpose()?,
            last_operation: v.last_operation.map(TryInto::try_into).transpose()?,
        })
    }
}
impl TryFrom<adx_core::InstanceRecord> for control::InstanceRecord {
    type Error = Error;
    fn try_from(v: adx_core::InstanceRecord) -> Result<Self> {
        let state = match v.state {
            adx_core::InstanceState::Pending => control::InstanceState::Pending,
            adx_core::InstanceState::Starting => control::InstanceState::Starting,
            adx_core::InstanceState::Running => control::InstanceState::Running,
            adx_core::InstanceState::Deleting => control::InstanceState::Deleting,
            adx_core::InstanceState::Deleted => control::InstanceState::Deleted,
            adx_core::InstanceState::Pausing => control::InstanceState::Pausing,
            adx_core::InstanceState::Paused => control::InstanceState::Paused,
            adx_core::InstanceState::Resuming => control::InstanceState::Resuming,
            adx_core::InstanceState::Failed => control::InstanceState::Failed,
        };
        Ok(Self {
            spec: Some(v.spec.into()),
            assignment: Some(v.assignment.try_into()?),
            state: state as i32,
            revision: v.revision,
            restart_attempts: v.restart_attempts,
            restart_pending: v.restart_pending,
            runtime_id: v.runtime_id,
            resources_held: v.resources_held,
            checkpoint: v.checkpoint.map(Into::into),
            last_operation: v.last_operation.map(Into::into),
            runtime_ip: v.runtime_ip.map(|ip| ip.to_string()).unwrap_or_default(),
        })
    }
}
pub fn status(error: adx_core::Error) -> tonic::Status {
    match error {
        Error::Invalid(message) => tonic::Status::invalid_argument(message),
        Error::Conflict => tonic::Status::failed_precondition("identity or version conflict"),
        Error::NotFound => tonic::Status::not_found("instance or node not found"),
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

pub mod tls;

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
            instance_id: v.instance_id,
            runtime_id: v.runtime_id,
            ownership_generation: v.ownership_generation,
        }
    }
}
impl TryFrom<control::RuntimeIdentity> for adx_core::runtime::RuntimeIdentity {
    type Error = Error;
    fn try_from(v: control::RuntimeIdentity) -> Result<Self> {
        let identity = Self {
            instance_id: v.instance_id,
            runtime_id: v.runtime_id,
            ownership_generation: v.ownership_generation,
        };
        identity.validate()?;
        Ok(identity)
    }
}
