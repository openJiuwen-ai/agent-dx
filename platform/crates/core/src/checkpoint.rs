//! Portable restore-point metadata. Artifacts stay behind a storage backend.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointArtifact {
    pub storage: String,
    pub location: String,
    pub size_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestorePoint {
    #[serde(default)]
    pub origin: Option<crate::runtime::RuntimeIdentity>,
    pub id: String,
    pub artifact: CheckpointArtifact,
    pub expires_at_unix_seconds: u64,
    pub source_runtime_id: String,
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
pub fn valid_runtime_id(capsule: &str, generation: u64, runtime_id: &str) -> bool {
    let base = format!("{capsule}-{generation}");
    runtime_id == base
        || runtime_id
            .strip_prefix(&format!("{base}-r"))
            .is_some_and(|s| {
                !s.is_empty()
                    && s.bytes().all(|b| b.is_ascii_digit())
                    && s.parse::<u64>().is_ok_and(|n| n > 0)
            })
}
