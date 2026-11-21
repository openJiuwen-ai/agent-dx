//! Portable restore-point metadata. Artifacts stay behind a storage backend.
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Checkpoint evidence for an OCI rootfs, scoped to one execution.
pub const RECOVERY_OCI_ROOTFS: &str = "ADX_RECOVERY_OCI_ROOTFS";
/// Node advertises support for verifying OCI rootfs evidence before restoring.
pub const RECOVERY_OCI_NODE: &str = "ADX_RECOVERY_OCI";

/// Validate the SHA-256 digest spelling used in immutable OCI references.
pub fn is_oci_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

/// Effective OCI image supplied to the runtime, if content-addressed.
/// Explicit execution rootfs takes precedence over the top-level image.
pub fn immutable_oci_rootfs(spec: &crate::EnvironmentSpec) -> Option<&str> {
    let image = match spec.execution.rootfs.as_ref().map(|root| &root.source) {
        Some(crate::execution::StorageSource::Image(image)) => image,
        None => &spec.image,
        Some(_) => return None,
    };
    let (repository, digest) = image.split_once('@')?;
    (!repository.is_empty() && is_oci_digest(digest)).then_some(image)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointArtifact {
    pub storage: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
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
    // An incomplete capture may have size zero; deletion still needs its identity.
    // RestorePoint validation separately rejects incomplete captures.
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
                .any(|a| a.storage.is_empty() || a.location.is_empty())
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
    /// Measured capture-time runtime and rootfs identity for cross-node recovery.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub recovery_profile: std::collections::BTreeMap<String, String>,
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
/// Maximum execution ID length supported by host runtime log filenames.
pub const MAX_RUNTIME_ID_BYTES: usize = 240;

/// Initial execution identity, bounded for native runtime log filenames.
/// Long Environment IDs remain unchanged in the public API and record.
pub fn runtime_id(environment: &str, generation: u64) -> String {
    let readable = format!("{environment}-{generation}");
    if readable.len() <= MAX_RUNTIME_ID_BYTES {
        readable
    } else {
        compact_runtime_id(environment, generation)
    }
}

/// Restart or restore identity, including its revision in the filename bound.
pub fn runtime_id_for_revision(environment: &str, generation: u64, revision: u64) -> String {
    let readable = format!("{environment}-{generation}-r{revision}");
    if readable.len() <= MAX_RUNTIME_ID_BYTES {
        readable
    } else {
        format!(
            "{}-r{revision}",
            compact_runtime_id(environment, generation)
        )
    }
}

fn compact_runtime_id(environment: &str, generation: u64) -> String {
    // A compact base contains no hyphens, so it cannot alias a readable
    // <environment>-<generation> identity, including a caller-chosen name.
    format!(
        "rt_{:x}_{generation}",
        Sha256::digest(environment.as_bytes())
    )
}

/// Execution revision for this exact Environment and ownership generation.
/// Existing records and checkpoints can retain their original readable IDs.
pub fn runtime_revision(environment: &str, generation: u64, id: &str) -> Option<u64> {
    [
        compact_runtime_id(environment, generation),
        format!("{environment}-{generation}"),
    ]
    .iter()
    .find_map(|base| {
        if id == base {
            return Some(0);
        }
        let suffix = id.strip_prefix(base.as_str())?.strip_prefix("-r")?;
        if suffix.is_empty() || !suffix.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        suffix.parse::<u64>().ok().filter(|revision| *revision > 0)
    })
}

pub fn valid_runtime_id(environment: &str, generation: u64, runtime_id: &str) -> bool {
    runtime_revision(environment, generation, runtime_id).is_some()
}
