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
