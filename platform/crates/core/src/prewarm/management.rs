//! Shared typed management contract; persistence remains in Platform.
use super::*;
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PoolError {
    #[error("pool not found")]
    NotFound,
    #[error("precondition required")]
    PreconditionRequired,
    #[error("pool revision precondition failed")]
    PreconditionFailed,
    #[error("logical request ID has different arguments")]
    IdempotencyConflict,
    #[error("pool profile is immutable")]
    ImmutableProfile,
    #[error("no qualified preparation backend is installed")]
    Unsupported,
    #[error(transparent)]
    Control(#[from] Error),
}
pub type PoolResult<T> = std::result::Result<T, PoolError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "revision",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Condition {
    Absent,
    Revision(u64),
    Missing,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Reconcile,
    Suspend,
    Resume,
    Drain,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Put {
        profile: Box<PreparationProfile>,
        policy: PoolPolicy,
    },
    Action {
        action: Action,
    },
    Delete,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mutation {
    pub pool_id: String,
    pub request_id: String,
    pub condition: Condition,
    pub command: Command,
    #[serde(default)]
    pub request_path: String,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolPhase {
    Suspended,
    Active,
    Draining,
    Deleting,
    Deleted,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolRecord {
    pub pool_id: String,
    pub tenant: String,
    pub revision: u64,
    pub profile_digest: String,
    #[serde(default)]
    pub artifact_recipe_digest: String,
    pub profile: PreparationProfile,
    pub policy: PoolPolicy,
    pub phase: PoolPhase,
    pub created_at: u64,
    pub updated_at: u64,
    #[serde(default)]
    pub artifacts: Vec<PreparationArtifact>,
    #[serde(default)]
    pub observed_revision: u64,
    #[serde(default)]
    pub observed_at: u64,
    #[serde(default)]
    pub refill_failures: u32,
    #[serde(default)]
    pub refill_retry_at: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagementOperation {
    pub operation_id: String,
    pub pool_id: String,
    pub tenant: String,
    pub accepted_revision: u64,
    pub state: String,
    pub outcome: String,
    pub created_at: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Accepted {
    pub pool: PoolRecord,
    pub operation: ManagementOperation,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPage {
    pub items: Vec<PreparationArtifact>,
    pub next_page_token: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Selection {
    Cold,
    Artifact,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationAttempt {
    pub tenant: String,
    pub environment_id: String,
    pub context: PreparationContext,
    pub request_digest: String,
    pub selection: Selection,
    pub reason: String,
    pub created_at: u64,
    #[serde(default)]
    pub assignment: Option<crate::Assignment>,
    #[serde(default)]
    pub artifact_id: Option<String>,
}
