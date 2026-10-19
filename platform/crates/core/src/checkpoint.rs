//! Portable restore-point metadata. Artifacts stay behind a storage backend.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointArtifact {
    pub storage: String,
    #[serde(default)]
    pub scope: String,
    pub location: String,
    pub size_bytes: u64,
}

/// Immutable deletion authority retained after an Environment leaves the catalog.
/// A task is acknowledged only after every artifact has been removed idempotently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointCleanup {
    pub assignment: crate::Assignment,
    pub revision: u64,
    pub artifacts: Vec<CheckpointArtifact>,
}

impl CheckpointArtifact {
    /// Conservatively match physical bytes, including legacy unscoped references.
    pub fn aliases(&self, other: &Self) -> bool {
        self.storage == other.storage
            && self.location == other.location
            && (self.scope.is_empty() || other.scope.is_empty() || self.scope == other.scope)
    }
}

impl CheckpointCleanup {
    pub fn validate(&self) -> crate::Result<()> {
        if self.assignment.environment_id.is_empty()
            || self.assignment.node_id.is_empty()
            || self.assignment.generation == 0
            || self.revision == 0
            || self.artifacts.is_empty()
            || self.artifacts.len() > 1024
            || self
                .artifacts
                .iter()
                .any(|a| a.storage.is_empty() || a.location.is_empty() || a.size_bytes == 0)
        {
            return Err(crate::Error::Invalid(
                "invalid checkpoint cleanup intent".into(),
            ));
        }
        Ok(())
    }

    pub fn id(&self) -> String {
        // A byte-length prefix keeps arbitrary Environment IDs unambiguous.
        format!(
            "{}:{}:{}:{}",
            self.assignment.environment_id.len(),
            self.assignment.environment_id,
            self.assignment.generation,
            self.revision
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestorePoint {
    #[serde(default)]
    pub origin: Option<crate::runtime::RuntimeIdentity>,
    pub id: String,
    pub artifact: CheckpointArtifact,
    /// None retains the restore point until explicit deletion or replacement.
    pub expires_at_unix_seconds: Option<u64>,
    pub source_runtime_id: String,
}
impl RestorePoint {
    pub fn expired(&self, now: u64) -> bool {
        self.expires_at_unix_seconds
            .is_some_and(|expiry| expiry <= now)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleKind {
    Pause,
    Resume,
    Snapshot,
    Network,
    Reload,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedOperation {
    pub id: String,
    pub kind: LifecycleKind,
    pub expected_revision: u64,
}
/// Ownership and an execution lifetime are distinct: a same-node restore starts
/// a new execution under the existing ownership, fenced by binding revision.
pub fn valid_runtime_id(environment: &str, generation: u64, runtime_id: &str) -> bool {
    let base = format!("{environment}-{generation}");
    runtime_id == base
        || runtime_id
            .strip_prefix(&format!("{base}-r"))
            .is_some_and(|s| {
                !s.is_empty()
                    && s.bytes().all(|b| b.is_ascii_digit())
                    && s.parse::<u64>().is_ok_and(|n| n > 0)
            })
}
