//! Generated transport contracts and validated conversions, without services.
mod admission;
pub use admission::{scheduling_deadline, scheduling_failure};
mod runtime_profile;
mod snapshots;
mod volumes;
pub mod control {
    tonic::include_proto!("adx.control.v1");
}

pub use adx_core::valid_runtime_id;
use adx_core::{Error, Result};
mod execution;
mod observation;
mod probes;
pub use observation::create_status;
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

impl TryFrom<control::EnvironmentSpec> for adx_core::EnvironmentSpec {
    type Error = Error;
    fn try_from(value: control::EnvironmentSpec) -> Result<Self> {
        let resources = value
            .resources
            .ok_or_else(|| Error::Invalid("resources are required".into()))?;
        let spec = Self {
            runtime_profile: value.runtime_profile.map(TryInto::try_into).transpose()?,
            snapshot_id: value.snapshot_id,
            lifecycle: value
                .lifecycle
                .map(TryInto::try_into)
                .transpose()?
                .unwrap_or_default(),
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
            execution: value
                .execution
                .map(TryInto::try_into)
                .transpose()?
                .unwrap_or_default(),
        };
        spec.validate()?;
        Ok(spec)
    }
}

impl From<adx_core::EnvironmentSpec> for control::EnvironmentSpec {
    fn from(value: adx_core::EnvironmentSpec) -> Self {
        Self {
            runtime_profile: value.runtime_profile.map(Into::into),
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
            execution: Some(value.execution.into()),
        }
    }
}

impl TryFrom<control::Assignment> for adx_core::Assignment {
    type Error = Error;
    fn try_from(value: control::Assignment) -> Result<Self> {
        if value.environment_id.trim().is_empty()
            || value.node_id.trim().is_empty()
            || value.generation == 0
        {
            return Err(Error::Invalid(
                "assignment requires environment, node and generation".into(),
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
            environment_id: value.environment_id,
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
            environment_id: value.environment_id,
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
        assert!(adx_core::EnvironmentSpec::try_from(control::EnvironmentSpec::default()).is_err());
        assert!(adx_core::Assignment::try_from(control::Assignment::default()).is_err());
    }

    #[test]
    fn environment_spec_round_trip_preserves_units_and_priority() {
        let value = adx_core::EnvironmentSpec {
            runtime_profile: None,
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
            execution: Default::default(),
        };
        let bytes = control::EnvironmentSpec::from(value.clone()).encode_to_vec();
        let decoded = control::EnvironmentSpec::decode(bytes.as_slice()).unwrap();
        assert_eq!(adx_core::EnvironmentSpec::try_from(decoded).unwrap(), value);
    }
}

/// Node-local binding and activity contracts.
pub mod relay {
    tonic::include_proto!("adx.node.v1");
}

pub mod auth;

impl TryFrom<control::EnvironmentRecord> for adx_core::EnvironmentRecord {
    type Error = Error;
    fn try_from(v: control::EnvironmentRecord) -> Result<Self> {
        let spec: adx_core::EnvironmentSpec = v
            .spec
            .ok_or_else(|| Error::Invalid("spec required".into()))?
            .try_into()?;
        let assignment: adx_core::Assignment = v
            .assignment
            .ok_or_else(|| Error::Invalid("assignment required".into()))?
            .try_into()?;
        if spec.id != assignment.environment_id {
            return Err(Error::Conflict);
        }
        adx_core::scheduling::validate_device_assignment(
            &spec.scheduling.devices,
            &assignment.devices,
        )?;
        let state = match control::EnvironmentState::try_from(v.state) {
            Ok(control::EnvironmentState::Pending) => adx_core::EnvironmentState::Pending,
            Ok(control::EnvironmentState::Starting) => adx_core::EnvironmentState::Starting,
            Ok(control::EnvironmentState::Running) => adx_core::EnvironmentState::Running,
            Ok(control::EnvironmentState::Deleting) => adx_core::EnvironmentState::Deleting,
            Ok(control::EnvironmentState::Deleted) => adx_core::EnvironmentState::Deleted,
            Ok(control::EnvironmentState::Pausing) => adx_core::EnvironmentState::Pausing,
            Ok(control::EnvironmentState::Paused) => adx_core::EnvironmentState::Paused,
            Ok(control::EnvironmentState::Resuming) => adx_core::EnvironmentState::Resuming,
            Ok(control::EnvironmentState::Failed) => adx_core::EnvironmentState::Failed,
            _ => return Err(Error::Invalid("unknown environment state".into())),
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
            snapshot_source: v.snapshot_source.map(TryInto::try_into).transpose()?,
            checkpoint: v.checkpoint.map(TryInto::try_into).transpose()?,
            execution_artifacts: v
                .execution_artifacts
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>()?,
            last_operation: v.last_operation.map(TryInto::try_into).transpose()?,
            failure: v.failure.map(TryInto::try_into).transpose()?,
            observation: v.observation.map(TryInto::try_into).transpose()?,
        })
    }
}
impl TryFrom<adx_core::EnvironmentRecord> for control::EnvironmentRecord {
    type Error = Error;
    fn try_from(v: adx_core::EnvironmentRecord) -> Result<Self> {
        let state = match v.state {
            adx_core::EnvironmentState::Pending => control::EnvironmentState::Pending,
            adx_core::EnvironmentState::Starting => control::EnvironmentState::Starting,
            adx_core::EnvironmentState::Running => control::EnvironmentState::Running,
            adx_core::EnvironmentState::Deleting => control::EnvironmentState::Deleting,
            adx_core::EnvironmentState::Deleted => control::EnvironmentState::Deleted,
            adx_core::EnvironmentState::Pausing => control::EnvironmentState::Pausing,
            adx_core::EnvironmentState::Paused => control::EnvironmentState::Paused,
            adx_core::EnvironmentState::Resuming => control::EnvironmentState::Resuming,
            adx_core::EnvironmentState::Failed => control::EnvironmentState::Failed,
        };
        Ok(Self {
            spec: Some(v.spec.into()),
            assignment: Some(v.assignment.try_into()?),
            state: state as i32,
            revision: v.revision,
            restart_attempts: v.restart_attempts,
            restart_pending: v.restart_pending,
            failure: v.failure.map(|failure| Box::new(failure.into())),
            observation: v.observation.map(Into::into),
            runtime: Some(control::Runtime {
                id: v.runtime.id,
                ip: v.runtime.ip.map(|ip| ip.to_string()).unwrap_or_default(),
            }),
            resources_held: v.resources_held,
            snapshot_source: v.snapshot_source.map(Into::into),
            checkpoint: v.checkpoint.map(Into::into),
            execution_artifacts: v
                .execution_artifacts
                .into_iter()
                .map(|a| control::CheckpointArtifact {
                    storage: a.storage,
                    location: a.location,
                    size_bytes: a.size_bytes,
                })
                .collect(),
            last_operation: v.last_operation.map(Into::into),
        })
    }
}
pub fn status(error: adx_core::Error) -> tonic::Status {
    match error {
        adx_core::Error::SnapshotInUse { sandbox_ids, template_ids } => tonic::Status::with_details(
            tonic::Code::FailedPrecondition, "snapshot is in use",
            serde_json::json!({"code":"SNAPSHOT_IN_USE", "sandbox_ids":sandbox_ids, "template_ids":template_ids}).to_string().into_bytes().into(),
        ),
        Error::BackendFailure { code, message } => {
            let mut status = tonic::Status::new(backend_wire_code(code), message);
            status.metadata_mut().insert(
                "adx-backend-failure",
                tonic::metadata::MetadataValue::from_static("true"),
            );
            status
        }
        Error::Invalid(message) => tonic::Status::invalid_argument(message),
        Error::Conflict => tonic::Status::failed_precondition("identity or version conflict"),
        Error::NotFound => tonic::Status::not_found("environment or node not found"),
        Error::NoCapacity => tonic::Status::resource_exhausted("capacity unavailable"),
        Error::VolumeBusy(uid) => tonic::Status::with_details(
            tonic::Code::ResourceExhausted, "waiting for Volume",
            serde_json::json!({"code":"WAITING_FOR_VOLUME", "volume_uid":uid}).to_string().into_bytes().into(),
        ),
        Error::Unavailable(message) => tonic::Status::unavailable(message),
        Error::ResponseTimeout(message) => tonic::Status::deadline_exceeded(message),
        Error::DeadlineExceeded(message) => {
            let failure = adx_core::RuntimeFailure {
                reason: adx_core::RuntimeFailureReason::DeadlineExceeded,
                message: message.clone(),
                exit_code: None,
                probe_failure: None,
            };
            match serde_json::to_vec(&failure) {
                Ok(details) => tonic::Status::with_details(
                    tonic::Code::DeadlineExceeded,
                    message,
                    details.into(),
                ),
                Err(_) => tonic::Status::internal("deadline failure encoding failed"),
            }
        }
        Error::ProbeFailed(failure) => match serde_json::to_vec(&failure) {
            Ok(details) => tonic::Status::with_details(
                tonic::Code::FailedPrecondition,
                failure.message.clone(),
                details.into(),
            ),
            Err(_) => tonic::Status::internal("probe failure encoding failed"),
        },
    }
}
pub fn dependency_status(status: tonic::Status) -> Error {
    if status.code() == tonic::Code::ResourceExhausted {
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(status.details()) {
            if value["code"] == "WAITING_FOR_VOLUME" {
                if let Some(uid) = value["volume_uid"].as_str() {
                    let reference = adx_core::volumes::VolumeRef {
                        id: uid.into(),
                        uid: None,
                    };
                    if reference.validate().is_ok() {
                        return Error::VolumeBusy(uid.into());
                    }
                }
            }
        }
    }
    if let Some(failure) = deadline_failure(&status) {
        return Error::DeadlineExceeded(failure.message);
    }
    if let Ok(failure) = serde_json::from_slice(status.details()) {
        return Error::ProbeFailed(failure);
    }
    match status.code() {
        tonic::Code::FailedPrecondition => Error::Conflict,
        tonic::Code::NotFound => Error::NotFound,
        tonic::Code::ResourceExhausted => Error::NoCapacity,
        tonic::Code::InvalidArgument => Error::Invalid("remote request rejected".into()),
        _ => Error::Unavailable("control RPC unavailable".into()),
    }
}

/// A node-confirmed failed create, distinct from a transport wait expiring.
pub fn deadline_failure(status: &tonic::Status) -> Option<adx_core::RuntimeFailure> {
    if status.code() != tonic::Code::DeadlineExceeded {
        return None;
    }
    serde_json::from_slice::<adx_core::RuntimeFailure>(status.details())
        .ok()
        .filter(|failure| failure.reason == adx_core::RuntimeFailureReason::DeadlineExceeded)
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
            || v.expires_at_unix_seconds == Some(0)
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

impl TryFrom<control::LifecyclePolicy> for adx_core::lifecycle::LifecyclePolicy {
    type Error = Error;
    fn try_from(v: control::LifecyclePolicy) -> Result<Self> {
        use adx_core::lifecycle::{CheckpointStorage, IdleAction, PausePolicy};
        let policy = Self {
            idle_action: match v.idle_action {
                0 => IdleAction::Delete,
                1 => IdleAction::Pause,
                _ => return Err(Error::Invalid("invalid idle action".into())),
            },
            pause: v
                .pause
                .map(|p| {
                    Ok(PausePolicy {
                        retention: p
                            .retention
                            .ok_or_else(|| Error::Invalid("pause retention required".into()))?
                            .try_into()?,
                        timeout_seconds: p.timeout_seconds,
                        storage: match p.storage {
                            0 => CheckpointStorage::Configured,
                            1 => CheckpointStorage::Local,
                            2 => CheckpointStorage::Shared,
                            _ => return Err(Error::Invalid("invalid checkpoint storage".into())),
                        },
                    })
                })
                .transpose()?,
            idle_timeout_seconds: v.idle_timeout_seconds,
            restart: v.restart.map(|r| adx_core::lifecycle::RestartPolicy {
                max_attempts: r.max_attempts,
                initial_backoff_seconds: r.initial_backoff_seconds,
                max_backoff_seconds: r.max_backoff_seconds,
            }),
        };
        policy.validate()?;
        Ok(policy)
    }
}
impl From<adx_core::lifecycle::LifecyclePolicy> for control::LifecyclePolicy {
    fn from(v: adx_core::lifecycle::LifecyclePolicy) -> Self {
        Self {
            idle_action: match v.idle_action {
                adx_core::lifecycle::IdleAction::Delete => 0,
                adx_core::lifecycle::IdleAction::Pause => 1,
            },
            pause: v.pause.map(|p| control::PausePolicy {
                retention: Some(p.retention.into()),
                timeout_seconds: p.timeout_seconds,
                storage: match p.storage {
                    adx_core::lifecycle::CheckpointStorage::Configured => 0,
                    adx_core::lifecycle::CheckpointStorage::Local => 1,
                    adx_core::lifecycle::CheckpointStorage::Shared => 2,
                },
            }),
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
            environment_id: v.environment_id,
            runtime_id: v.runtime_id,
            ownership_generation: v.ownership_generation,
        }
    }
}
impl TryFrom<control::RuntimeIdentity> for adx_core::runtime::RuntimeIdentity {
    type Error = Error;
    fn try_from(v: control::RuntimeIdentity) -> Result<Self> {
        let identity = Self {
            environment_id: v.environment_id,
            runtime_id: v.runtime_id,
            ownership_generation: v.ownership_generation,
        };
        identity.validate()?;
        Ok(identity)
    }
}

/// Execd-initiated node-local control streams.
pub mod runtime {
    tonic::include_proto!("adx.runtime.v1");
}
pub mod runtime_stream;

impl TryFrom<control::RetentionPolicy> for adx_core::lifecycle::RetentionPolicy {
    type Error = Error;
    fn try_from(value: control::RetentionPolicy) -> Result<Self> {
        use control::retention_policy::Policy;
        let retention = match value.policy {
            Some(Policy::TtlSeconds(ttl_seconds)) => Self::Ttl { ttl_seconds },
            Some(Policy::Forever(true)) => Self::Forever,
            _ => return Err(Error::Invalid("explicit retention policy required".into())),
        };
        retention.validate()?;
        Ok(retention)
    }
}
impl From<adx_core::lifecycle::RetentionPolicy> for control::RetentionPolicy {
    fn from(value: adx_core::lifecycle::RetentionPolicy) -> Self {
        use control::retention_policy::Policy;
        Self {
            policy: Some(match value {
                adx_core::lifecycle::RetentionPolicy::Ttl { ttl_seconds } => {
                    Policy::TtlSeconds(ttl_seconds)
                }
                adx_core::lifecycle::RetentionPolicy::Forever => Policy::Forever(true),
            }),
        }
    }
}

impl TryFrom<control::RuntimeFailure> for adx_core::RuntimeFailure {
    type Error = Error;
    fn try_from(value: control::RuntimeFailure) -> Result<Self> {
        use adx_core::RuntimeFailureReason as Reason;
        use control::runtime_failure::Reason as Wire;
        let reason = match Wire::try_from(value.reason) {
            Ok(Wire::OomKilled) => Reason::OomKilled,
            Ok(Wire::Exited) => Reason::Exited,
            Ok(Wire::Missing) => Reason::Missing,
            Ok(Wire::ProbeFailed) => Reason::ProbeFailed,
            Ok(Wire::DeadlineExceeded) => Reason::DeadlineExceeded,
            _ => return Err(Error::Invalid("invalid runtime failure reason".into())),
        };
        let probe_failure = value.probe_failure.map(TryInto::try_into).transpose()?;
        if (reason == Reason::ProbeFailed) != probe_failure.is_some() {
            return Err(Error::Invalid(
                "runtime probe failure metadata does not match reason".into(),
            ));
        }
        Ok(Self {
            reason,
            exit_code: value.exit_code,
            message: value.message,
            probe_failure,
        })
    }
}
impl From<adx_core::RuntimeFailure> for control::RuntimeFailure {
    fn from(value: adx_core::RuntimeFailure) -> Self {
        use adx_core::RuntimeFailureReason as Reason;
        use control::runtime_failure::Reason as Wire;
        let reason = match value.reason {
            Reason::OomKilled => Wire::OomKilled,
            Reason::Exited => Wire::Exited,
            Reason::Missing => Wire::Missing,
            Reason::ProbeFailed => Wire::ProbeFailed,
            Reason::DeadlineExceeded => Wire::DeadlineExceeded,
        };
        Self {
            reason: reason as i32,
            exit_code: value.exit_code,
            message: value.message,
            probe_failure: value.probe_failure.map(Into::into),
        }
    }
}

impl TryFrom<Box<control::RuntimeFailure>> for adx_core::RuntimeFailure {
    type Error = Error;
    fn try_from(value: Box<control::RuntimeFailure>) -> Result<Self> {
        (*value).try_into()
    }
}

/// Preserve a backend application status without inferring a confirmed timeout.
/// Confirmed Start deadlines use the separate DeadlineExceeded failure contract.
pub fn backend_error(status: tonic::Status) -> adx_core::Error {
    let mut source = std::error::Error::source(&status);
    while let Some(error) = source {
        if error.is::<tonic::transport::Error>() {
            return Error::Unavailable(format!("sandboxd transport: {}", status.message()));
        }
        source = error.source();
    }
    let code = match status.code() {
        tonic::Code::Cancelled => adx_core::BackendFailureCode::Cancelled,
        tonic::Code::Unknown => adx_core::BackendFailureCode::Unknown,
        tonic::Code::InvalidArgument => adx_core::BackendFailureCode::InvalidArgument,
        tonic::Code::DeadlineExceeded => adx_core::BackendFailureCode::DeadlineExceeded,
        tonic::Code::NotFound => adx_core::BackendFailureCode::NotFound,
        tonic::Code::AlreadyExists => adx_core::BackendFailureCode::AlreadyExists,
        tonic::Code::PermissionDenied => adx_core::BackendFailureCode::PermissionDenied,
        tonic::Code::ResourceExhausted => adx_core::BackendFailureCode::ResourceExhausted,
        tonic::Code::FailedPrecondition => adx_core::BackendFailureCode::FailedPrecondition,
        tonic::Code::Aborted => adx_core::BackendFailureCode::Aborted,
        tonic::Code::OutOfRange => adx_core::BackendFailureCode::OutOfRange,
        tonic::Code::Unimplemented => adx_core::BackendFailureCode::Unimplemented,
        tonic::Code::Internal => adx_core::BackendFailureCode::Internal,
        tonic::Code::Unavailable => adx_core::BackendFailureCode::Unavailable,
        tonic::Code::DataLoss => adx_core::BackendFailureCode::DataLoss,
        tonic::Code::Unauthenticated => adx_core::BackendFailureCode::Unauthenticated,
        tonic::Code::Ok => adx_core::BackendFailureCode::Internal,
    };
    Error::BackendFailure {
        code,
        message: format!("sandboxd: {}", status.message()),
    }
}

fn backend_wire_code(code: adx_core::BackendFailureCode) -> tonic::Code {
    match code {
        adx_core::BackendFailureCode::Cancelled => tonic::Code::Cancelled,
        adx_core::BackendFailureCode::Unknown => tonic::Code::Unknown,
        adx_core::BackendFailureCode::InvalidArgument => tonic::Code::InvalidArgument,
        adx_core::BackendFailureCode::DeadlineExceeded => tonic::Code::DeadlineExceeded,
        adx_core::BackendFailureCode::NotFound => tonic::Code::NotFound,
        adx_core::BackendFailureCode::AlreadyExists => tonic::Code::AlreadyExists,
        adx_core::BackendFailureCode::PermissionDenied => tonic::Code::PermissionDenied,
        adx_core::BackendFailureCode::ResourceExhausted => tonic::Code::ResourceExhausted,
        adx_core::BackendFailureCode::FailedPrecondition => tonic::Code::FailedPrecondition,
        adx_core::BackendFailureCode::Aborted => tonic::Code::Aborted,
        adx_core::BackendFailureCode::OutOfRange => tonic::Code::OutOfRange,
        adx_core::BackendFailureCode::Unimplemented => tonic::Code::Unimplemented,
        adx_core::BackendFailureCode::Internal => tonic::Code::Internal,
        adx_core::BackendFailureCode::Unavailable => tonic::Code::Unavailable,
        adx_core::BackendFailureCode::DataLoss => tonic::Code::DataLoss,
        adx_core::BackendFailureCode::Unauthenticated => tonic::Code::Unauthenticated,
    }
}

/// Artifact references may reserve an incomplete capture with size zero. RestorePoint rejects it.
impl TryFrom<control::CheckpointArtifact> for adx_core::CheckpointArtifact {
    type Error = Error;
    fn try_from(a: control::CheckpointArtifact) -> Result<Self> {
        if a.storage.is_empty() || a.location.is_empty() {
            return Err(Error::Invalid("invalid artifact identity".into()));
        }
        Ok(Self {
            storage: a.storage,
            location: a.location,
            size_bytes: a.size_bytes,
        })
    }
}
impl From<adx_core::CheckpointArtifact> for control::CheckpointArtifact {
    fn from(a: adx_core::CheckpointArtifact) -> Self {
        Self {
            storage: a.storage,
            location: a.location,
            size_bytes: a.size_bytes,
        }
    }
}
