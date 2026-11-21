//! Shared Execd HTTP/stream control payloads; lifecycle decisions remain in Adxlet.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIdentity {
    pub environment_id: String,
    pub runtime_id: String,
    pub ownership_generation: u64,
}
impl RuntimeIdentity {
    pub fn validate_restore_from(&self, previous: &Self) -> Result<()> {
        self.validate()?;
        if self.environment_id != previous.environment_id
            || self.ownership_generation < previous.ownership_generation
        {
            return Err(Error::Conflict);
        }
        if self.ownership_generation > previous.ownership_generation {
            return Ok(());
        }
        if self == previous {
            return Ok(());
        }
        let version = |identity: &Self| -> Option<u64> {
            crate::runtime_revision(
                &identity.environment_id,
                identity.ownership_generation,
                &identity.runtime_id,
            )
        };
        match (version(previous), version(self)) {
            (Some(a), Some(b)) if b > a => Ok(()),
            _ => Err(Error::Conflict),
        }
    }
    pub fn validate(&self) -> Result<()> {
        if self.environment_id.trim().is_empty()
            || self.runtime_id.trim().is_empty()
            || self.ownership_generation == 0
        {
            return Err(Error::Invalid(
                "runtime requires explicit environment, execution and ownership generation".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePhase {
    Initializing,
    Running,
    Preparing,
    Prepared,
    Restoring,
    Failed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointPhase {
    Preparing,
    Prepared,
    Aborted,
    Resumed,
    Restored,
    Failed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointStatus {
    pub operation_id: String,
    pub phase: CheckpointPhase,
    pub error: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<crate::probes::ProbeFailure>,
    pub identity: RuntimeIdentity,
    pub revision: u64,
    pub phase: RuntimePhase,
    pub checkpoint: Option<CheckpointStatus>,
    /// Workload-originated request, consumed only by the owning Adxlet.
    #[serde(default)]
    pub requested_checkpoint: Option<String>,
    /// Absolute deadline for the workload-originated checkpoint operation.
    /// Older runtimes omit it and retain the legacy default timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_checkpoint_deadline_unix_millis: Option<u64>,
    pub active_requests: u64,
    pub active_commands: u64,
    /// Changes on request/command entry and exit, including bursts between polls.
    #[serde(default)]
    pub activity_revision: u64,
    /// Deployment-owned services required by the selected runtime profile.
    /// An omitted map is the legacy/native profile and carries no extra
    /// readiness requirements.
    #[serde(default)]
    pub required_services: BTreeMap<String, RequiredServiceStatus>,
}

impl RuntimeStatus {
    /// Every reported mandatory service and every configured expectation must be ready.
    /// A legacy runtime with no configured or reported services remains compatible.
    pub fn validate_required_services<'a>(
        &self,
        expected: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        if self
            .required_services
            .values()
            .any(|service| service.phase != RequiredServicePhase::Ready)
            || expected
                .into_iter()
                .any(|name| !self.required_services.contains_key(name))
        {
            return Err(Error::Unavailable(
                "required runtime service is not ready".into(),
            ));
        }
        Ok(())
    }
}

/// State reported by a guest service which participates in runtime readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequiredServicePhase {
    Disabled,
    Starting,
    Ready,
    Failed,
}

/// Version and failure information for one deployment-owned guest service.
/// The error text is diagnostic only and must never contain credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequiredServiceStatus {
    pub phase: RequiredServicePhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareCheckpoint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_revision: Option<u64>,
    pub identity: RuntimeIdentity,
    pub operation_id: String,
    pub expected_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbortCheckpoint {
    pub identity: RuntimeIdentity,
    pub operation_id: String,
    pub expected_revision: u64,
}

/// Backend-supplied restore context, never accepted from a public HTTP request.
/// Cloning is allowed only when the embedded identity matches the exact source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRestore {
    pub target: RuntimeIdentity,
    pub origin: Option<RuntimeIdentity>,
}
impl RuntimeRestore {
    pub fn validate(&self, previous: &RuntimeIdentity) -> Result<()> {
        previous.validate()?;
        self.target.validate()?;
        match &self.origin {
            None => self.target.validate_restore_from(previous),
            Some(origin)
                if origin == previous
                    && (self.target.environment_id != previous.environment_id
                        || self.target.ownership_generation > previous.ownership_generation)
                    && crate::valid_runtime_id(
                        &self.target.environment_id,
                        self.target.ownership_generation,
                        &self.target.runtime_id,
                    )
                    && crate::valid_runtime_id(
                        &origin.environment_id,
                        origin.ownership_generation,
                        &origin.runtime_id,
                    ) =>
            {
                Ok(())
            }
            Some(_) => Err(Error::Conflict),
        }
    }
}

/// Node acknowledgement after the local recovery point is committed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinishWorkloadCheckpoint {
    pub identity: RuntimeIdentity,
    pub operation_id: String,
    /// A failure wakes the workload without asserting that a checkpoint exists.
    pub error: Option<String>,
}

#[cfg(test)]
mod required_service_tests {
    use super::*;

    #[test]
    fn validates_aggregate_status_and_configured_expectations() {
        let mut status: RuntimeStatus = serde_json::from_value(serde_json::json!({
            "identity": {"environment_id":"i", "runtime_id":"i-1", "ownership_generation":1},
            "revision":1, "phase":"running", "checkpoint":null,
            "active_requests":0, "active_commands":0,
        }))
        .unwrap();
        assert!(status.validate_required_services([]).is_ok());
        assert!(status.validate_required_services(["helper"]).is_err());
        status.required_services.insert(
            "helper".into(),
            RequiredServiceStatus {
                phase: RequiredServicePhase::Ready,
                version: None,
                error: None,
            },
        );
        assert!(status.validate_required_services(["helper"]).is_ok());
        for phase in [
            RequiredServicePhase::Starting,
            RequiredServicePhase::Failed,
            RequiredServicePhase::Disabled,
        ] {
            status.required_services.get_mut("helper").unwrap().phase = phase;
            assert!(status.validate_required_services([]).is_err());
        }
    }
}
