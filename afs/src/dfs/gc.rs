//! Durable references and generation-fenced physical reclamation.
use super::*;
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcReference {
    pub node_id: String,
    pub session_id: String,
    pub reference_id: String,
    pub inode_id: Option<InodeId>,
    pub publication: bool,
    pub chunks: Vec<ChunkObject>,
    pub retired_at_unix_ms: Option<u64>,
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
    pub chunks: Vec<ChunkObject>,
    pub release: bool,
    pub admission_deadline_unix_ms: u64,
}

/// GC state observed before a native write generation produces any receipts.
/// Absent is explicit; an absent wire marker never means an absent GC record.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum GcFence {
    Absent,
    Revision(u64),
    /// The caller's admitted write reference, independent of unrelated GC work.
    WriteGeneration,
}
impl GcFence {
    pub(crate) fn wire(self) -> afs_protocol::meta::DfsGcFence {
        use afs_protocol::meta::dfs_gc_fence::Kind;
        afs_protocol::meta::DfsGcFence {
            kind: Some(match self {
                Self::Absent => Kind::Absent(true),
                Self::Revision(value) => Kind::StateRevision(value),
                Self::WriteGeneration => Kind::WriteGeneration(true),
            }),
        }
    }
    pub(crate) fn from_wire(value: afs_protocol::meta::DfsGcFence) -> afs_error::Result<Self> {
        use afs_protocol::meta::dfs_gc_fence::Kind;
        match value.kind {
            Some(Kind::Absent(true)) => Ok(Self::Absent),
            Some(Kind::StateRevision(value)) if value > 0 => Ok(Self::Revision(value)),
            Some(Kind::WriteGeneration(true)) => Ok(Self::WriteGeneration),
            _ => Err(afs_error::Error::coded(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "invalid GC fence",
            )),
        }
    }
    pub(crate) fn condition(self) -> Option<crate::meta::store::TxnCondition> {
        use crate::meta::store::{MetaKey, StoreRevision, TxnCondition};
        match self {
            Self::Absent => Some(TxnCondition::Missing(MetaKey::DfsGc)),
            Self::Revision(value) => Some(TxnCondition::RevisionEquals {
                key: MetaKey::DfsGc,
                revision: StoreRevision(value),
            }),
            Self::WriteGeneration => None,
        }
    }
}
