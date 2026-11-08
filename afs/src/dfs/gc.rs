//! Durable references and generation-fenced physical reclamation.
use super::*;
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcReference {
    pub node_id: String,
    pub session_id: String,
    pub reference_id: String,
    pub inode_id: Option<InodeId>,
    /// Set on admitted `write:` generation references, which protect every
    /// chunk of their generation from collection.
    pub publication: bool,
    /// Decode only: always empty on new references. Per-chunk `publish:`
    /// reservations were removed (R33), but this field is part of the postcard
    /// record, so dropping it changes the WAL and checkpoint encoding and needs
    /// a format version bump (see the format golden tests).
    pub chunks: Vec<ChunkObject>,
    pub retired_at_unix_ms: Option<u64>,
    /// Decode only: always `None` on new references. Open-read authorization
    /// was removed, but this field is part of the postcard record, so dropping
    /// it changes the WAL and checkpoint encoding and needs a format version
    /// bump (see the format golden tests).
    #[serde(default)]
    pub open_read: Option<OpenReadIdentity>,
    /// The one version this reference keeps readable besides the head: a
    /// reader's opened version, or the version a write generation started
    /// from (R81 F2). `None` keeps the inode and its head only. Added with
    /// `META_RECORD_DECISION` 2 / `META_STATE_IMAGE` 3; older stored references
    /// carried no version and are refused with their format, never read as
    /// "protects nothing".
    pub pinned_version: Option<FileVersionId>,
}
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcState {
    pub generation: u64,
    pub chunks: Vec<ChunkId>,
    pub waiting_nodes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct UpdateGcReference {
    pub node_id: String,
    pub session_id: String,
    pub reference_id: String,
    pub inode_id: Option<InodeId>,
    pub version_id: Option<FileVersionId>,
    pub publication: bool,
    pub admission_deadline_unix_ms: u64,
}

/// The `open:` reference Meta admits inside the Create transaction, for the
/// handle the same call opens. Create retries reuse the operation id, so a replay
/// names the same reference.
pub fn created_handle_reference_id(operation: &crate::dfs::OperationId) -> String {
    format!("open:create-{}", operation.0)
}

/// How a native write generation is protected from GC while it produces receipts.
/// Never written to the Meta log. The variant name still reaches disk through the
/// JSON namespace request digest, so renaming it breaks replay of recorded commits.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum GcFence {
    /// The caller's admitted write reference, independent of unrelated GC work.
    WriteGeneration,
}
impl GcFence {
    pub(crate) fn wire(self) -> afs_protocol::meta::DfsGcFence {
        use afs_protocol::meta::dfs_gc_fence::Kind;
        afs_protocol::meta::DfsGcFence {
            kind: Some(match self {
                Self::WriteGeneration => Kind::WriteGeneration(true),
            }),
        }
    }
    pub(crate) fn from_wire(value: afs_protocol::meta::DfsGcFence) -> afs_error::Result<Self> {
        use afs_protocol::meta::dfs_gc_fence::Kind;
        match value.kind {
            Some(Kind::WriteGeneration(true)) => Ok(Self::WriteGeneration),
            _ => Err(afs_error::Error::coded(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "invalid GC fence",
            )),
        }
    }
}

/// The persisted shape of `GcReference::open_read`. Nothing creates it any more;
/// it stays so that existing records keep decoding (see that field).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OpenReadIdentity {
    pub version_id: FileVersionId,
    pub layout_root_id: LayoutRootId,
    pub closed: bool,
}

#[cfg(test)]
mod open_authority_tests {
    use super::*;
    #[test]
    fn open_authority_reference_journal_codec_roundtrip() {
        for identity in [
            None,
            Some(OpenReadIdentity {
                version_id: FileVersionId::new("v"),
                layout_root_id: LayoutRootId::new("l"),
                closed: false,
            }),
            Some(OpenReadIdentity {
                version_id: FileVersionId::new("v"),
                layout_root_id: LayoutRootId::new("l"),
                closed: true,
            }),
        ] {
            let reference = GcReference {
                node_id: "n".into(),
                session_id: "s".into(),
                reference_id: "read-open:r".into(),
                inode_id: Some(InodeId::new("i")),
                publication: false,
                chunks: vec![],
                retired_at_unix_ms: None,
                open_read: identity,
                pinned_version: None,
            };
            let expected = (reference, true);
            let bytes = postcard::to_allocvec(&expected).unwrap();
            let restored: (GcReference, bool) = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(
                restored, expected,
                "optional context fields must not shift the following journal record"
            );
        }
    }
}
