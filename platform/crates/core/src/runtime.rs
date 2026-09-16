//! RRT HTTP control payloads; lifecycle decisions remain in Node Manager.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIdentity {
    pub instance_id: String,
    pub runtime_id: String,
    pub ownership_generation: u64,
}
impl RuntimeIdentity {
    pub fn validate_restore_from(&self, previous: &Self) -> Result<()> {
        self.validate()?;
        if self.instance_id != previous.instance_id
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
            let base = format!("{}-{}", identity.instance_id, identity.ownership_generation);
            if identity.runtime_id == base {
                Some(0)
            } else {
                identity
                    .runtime_id
                    .strip_prefix(&format!("{base}-r"))
                    .and_then(|s| s.parse::<u64>().ok())
                    .filter(|n| *n > 0)
            }
        };
        match (version(previous), version(self)) {
            (Some(a), Some(b)) if b > a => Ok(()),
            _ => Err(Error::Conflict),
        }
    }
    pub fn validate(&self) -> Result<()> {
        if self.instance_id.trim().is_empty()
            || self.runtime_id.trim().is_empty()
            || self.ownership_generation == 0
        {
            return Err(Error::Invalid(
                "runtime requires explicit instance, execution and ownership generation".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePhase {
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
    pub identity: RuntimeIdentity,
    pub revision: u64,
    pub phase: RuntimePhase,
    pub checkpoint: Option<CheckpointStatus>,
    pub active_requests: u64,
    pub active_commands: u64,
    /// Changes on request/command entry and exit, including bursts between polls.
    #[serde(default)]
    pub activity_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareCheckpoint {
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
                    && (self.target.instance_id != previous.instance_id
                        || self.target.ownership_generation > previous.ownership_generation)
                    && crate::valid_runtime_id(
                        &self.target.instance_id,
                        self.target.ownership_generation,
                        &self.target.runtime_id,
                    )
                    && crate::valid_runtime_id(
                        &origin.instance_id,
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
