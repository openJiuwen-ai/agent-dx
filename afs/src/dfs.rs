//! Shared DistributedFs identities and immutable metadata records.
//!
//! Mutable POSIX state is represented by an `InodeRecord` whose head points at
//! an immutable `FileVersion`. A version owns an immutable extent layout; every
//! extent refers to an immutable chunk. Physical copies are tracked separately.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
        pub struct $name(pub String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }
        }
    };
}

mod bytes_key_map {
    use std::{collections::BTreeMap, fmt};

    use serde::{Deserializer, Serialize, Serializer, de::Visitor};

    pub fn serialize<S>(map: &BTreeMap<Vec<u8>, Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = map
            .iter()
            .map(|(key, value)| (hex_encode(key), value))
            .collect::<BTreeMap<_, _>>();
        encoded.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MapVisitor;

        impl<'de> Visitor<'de> for MapVisitor {
            type Value = BTreeMap<Vec<u8>, Vec<u8>>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a map with hex-encoded byte keys")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut out = BTreeMap::new();
                while let Some((key, value)) = access.next_entry::<String, Vec<u8>>()? {
                    let key = hex_decode(&key).map_err(serde::de::Error::custom)?;
                    out.insert(key, value);
                }
                Ok(out)
            }
        }

        deserializer.deserialize_map(MapVisitor)
    }

    fn hex_encode(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
        out
    }

    fn hex_decode(input: &str) -> Result<Vec<u8>, String> {
        if !input.len().is_multiple_of(2) {
            return Err("hex key length must be even".into());
        }
        input
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let high = hex_value(pair[0])?;
                let low = hex_value(pair[1])?;
                Ok((high << 4) | low)
            })
            .collect()
    }

    fn hex_value(byte: u8) -> Result<u8, String> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            b'A'..=b'F' => Ok(byte - b'A' + 10),
            _ => Err("hex key contains a non-hex character".into()),
        }
    }
}

string_id!(NamespaceId);
string_id!(InodeId);
string_id!(FileVersionId);
string_id!(LayoutRootId);
string_id!(ChunkId);
string_id!(CopyId);
string_id!(OperationId);
string_id!(DfsWriteSessionId);
string_id!(ReplicaGroupId);
string_id!(ReplicationTaskId);
string_id!(ReadBatchId);
string_id!(ReadAttemptId);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SpecialNodeKind {
    Fifo,
    Socket,
    BlockDevice { rdev: u64 },
    CharDevice { rdev: u64 },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum InodeKind {
    Regular,
    Directory,
    Symlink,
    Special(SpecialNodeKind),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InodeAttributes {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub atime_unix_ms: u64,
    pub mtime_unix_ms: u64,
    pub ctime_unix_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InodeRecord {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
    pub kind: InodeKind,
    pub attributes: InodeAttributes,
    pub head_version: Option<FileVersionId>,
    #[serde(default)]
    pub symlink_target: Option<Vec<u8>>,
    #[serde(default, with = "bytes_key_map")]
    pub xattrs: BTreeMap<Vec<u8>, Vec<u8>>,
    pub revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct DentryKey {
    pub namespace_id: NamespaceId,
    pub parent_inode_id: InodeId,
    pub name: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Dentry {
    pub key: DentryKey,
    pub inode_id: InodeId,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DentryRecord {
    pub name: Vec<u8>,
    pub inode: InodeRecord,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MkdirRequest {
    pub caller_id: String,
    pub operation_id: OperationId,
    pub namespace_id: NamespaceId,
    pub parent_inode_id: InodeId,
    pub name: Vec<u8>,
    pub attributes: InodeAttributes,
    pub caller: CallerContext,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LinkRequest {
    pub caller_id: String,
    pub operation_id: OperationId,
    pub namespace_id: NamespaceId,
    pub existing_inode_id: InodeId,
    pub expected_inode_revision: u64,
    pub parent_inode_id: InodeId,
    pub name: Vec<u8>,
    pub caller: CallerContext,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SymlinkRequest {
    pub caller_id: String,
    pub operation_id: OperationId,
    pub namespace_id: NamespaceId,
    pub parent_inode_id: InodeId,
    pub name: Vec<u8>,
    pub target: Vec<u8>,
    pub attributes: InodeAttributes,
    pub caller: CallerContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadLinkRequest {
    pub namespace_id: NamespaceId,
    pub inode_id: InodeId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CallerContext {
    pub uid: u32,
    pub gid: u32,
    pub supplementary_gids: Vec<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MknodRequest {
    pub caller_id: String,
    pub operation_id: OperationId,
    pub namespace_id: NamespaceId,
    pub parent_inode_id: InodeId,
    pub name: Vec<u8>,
    pub kind: SpecialNodeKind,
    pub attributes: InodeAttributes,
    pub caller: CallerContext,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InodeAttrUpdate {
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub atime_unix_ms: Option<u64>,
    pub mtime_unix_ms: Option<u64>,
    pub ctime_unix_ms: Option<u64>,
    /// Kernel NOW intent; explicit timestamps require owner/root authority.
    pub timestamps_now: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetInodeAttrRequest {
    pub caller_id: String,
    pub operation_id: OperationId,
    pub caller: CallerContext,
    pub inode_id: InodeId,
    pub expected_inode_revision: u64,
    pub update: InodeAttrUpdate,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum XattrSetMode {
    Upsert,
    Create,
    Replace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetXattrRequest {
    pub caller: CallerContext,
    pub inode_id: InodeId,
    pub name: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListXattrRequest {
    pub caller: CallerContext,
    pub inode_id: InodeId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetXattrRequest {
    pub caller_id: String,
    pub operation_id: OperationId,
    pub caller: CallerContext,
    pub inode_id: InodeId,
    pub expected_inode_revision: u64,
    pub name: Vec<u8>,
    pub value: Vec<u8>,
    pub mode: XattrSetMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoveXattrRequest {
    pub caller_id: String,
    pub operation_id: OperationId,
    pub caller: CallerContext,
    pub inode_id: InodeId,
    pub expected_inode_revision: u64,
    pub name: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct UnlinkRequest {
    pub caller_id: String,
    pub operation_id: OperationId,
    pub namespace_id: NamespaceId,
    pub parent_inode_id: InodeId,
    pub name: Vec<u8>,
    pub caller: CallerContext,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RmdirRequest {
    pub caller_id: String,
    pub operation_id: OperationId,
    pub namespace_id: NamespaceId,
    pub parent_inode_id: InodeId,
    pub name: Vec<u8>,
    pub caller: CallerContext,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RenameMode {
    NoReplace,
    Replace,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RenameRequest {
    pub caller_id: String,
    pub operation_id: OperationId,
    pub namespace_id: NamespaceId,
    pub old_parent_inode_id: InodeId,
    pub old_name: Vec<u8>,
    pub new_parent_inode_id: InodeId,
    pub new_name: Vec<u8>,
    pub mode: RenameMode,
    pub caller: CallerContext,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RenameOutcome {
    pub inode: InodeRecord,
    pub replaced_inode: Option<InodeRecord>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileVersion {
    pub id: FileVersionId,
    pub inode_id: InodeId,
    pub parent_version: Option<FileVersionId>,
    pub length: u64,
    pub layout_root: LayoutRootId,
    pub created_at_unix_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Extent {
    pub file_offset: u64,
    pub length: u64,
    pub chunk_id: ChunkId,
    pub chunk_offset: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LayoutRoot {
    pub id: LayoutRootId,
    pub file_length: u64,
    /// The R=1 vertical slice keeps extents inline. A later accepted extent-tree
    /// format can replace this field without changing FileVersion or Chunk IDs.
    pub inline_extents: Vec<Extent>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DigestAlgorithm {
    Blake3,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ContentDigest {
    pub algorithm: DigestAlgorithm,
    pub bytes: [u8; 32],
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ChunkEncoding {
    Raw,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChunkObject {
    pub id: ChunkId,
    pub length: u64,
    pub content_digest: ContentDigest,
    pub encoding: ChunkEncoding,
}

/// One immutable filesystem-wide replication contract.
///
/// The first implementation writes this record when the filesystem is
/// initialized and rejects a different value on later starts. Changing it
/// requires creating a new filesystem rather than migrating individual inodes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicationConfig {
    pub desired_copies: u16,
    pub sync_required_copies: u16,
    pub min_distinct_nodes: u16,
    pub min_distinct_failure_domains: u16,
    pub local_copy: LocalCopyPolicy,
}

impl ReplicationConfig {
    pub fn local_single_copy() -> Self {
        Self {
            desired_copies: 1,
            sync_required_copies: 1,
            min_distinct_nodes: 1,
            min_distinct_failure_domains: 1,
            local_copy: LocalCopyPolicy::Required,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.sync_required_copies > 0
            && self.sync_required_copies <= self.desired_copies
            && self.min_distinct_nodes > 0
            && self.min_distinct_nodes <= self.desired_copies
            && self.min_distinct_failure_domains > 0
            && self.min_distinct_failure_domains <= self.desired_copies
    }

    pub fn is_local_fast_path(&self) -> bool {
        self.desired_copies == 1
            && self.sync_required_copies == 1
            && self.local_copy == LocalCopyPolicy::Required
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LocalCopyPolicy {
    Required,
    Preferred,
    NotRequired,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StorageDeviceDescriptor {
    pub device_id: String,
    pub device_epoch: u64,
    pub catalog_revision: u64,
    pub failure_domain: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaTarget {
    pub node_id: String,
    pub node_epoch: u64,
    pub data_endpoint: String,
    pub device: StorageDeviceDescriptor,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaGroup {
    pub id: ReplicaGroupId,
    pub placement_epoch: u64,
    pub targets: Vec<ReplicaTarget>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlacementSnapshot {
    pub revision: u64,
    pub replication: ReplicationConfig,
    pub replica_groups: Vec<ReplicaGroup>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ValidateReplicaWriteRequest {
    pub requester_node_id: String,
    pub requester_node_epoch: u64,
    pub initiator_node_id: String,
    pub initiator_node_epoch: u64,
    pub operation_id: OperationId,
    pub chunk_id: ChunkId,
    pub chunk_length: u64,
    pub content_digest: ContentDigest,
    pub placement_revision: u64,
    pub placement_epoch: u64,
    pub replica_group_id: ReplicaGroupId,
    pub target_index: u32,
    pub ordered_targets: Vec<ReplicaTarget>,
    #[serde(default)]
    pub repair_claim: Option<ReplicationClaim>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaWriteGrant {
    pub requester_node_id: String,
    pub requester_node_epoch: u64,
    pub initiator_node_id: String,
    pub initiator_node_epoch: u64,
    pub operation_id: OperationId,
    pub chunk_id: ChunkId,
    pub chunk_length: u64,
    pub content_digest: ContentDigest,
    pub placement_revision: u64,
    pub placement_epoch: u64,
    pub replica_group_id: ReplicaGroupId,
    pub target_index: u32,
    pub replication: ReplicationConfig,
    pub replica_group: ReplicaGroup,
    pub expires_at_unix_ms: u64,
    pub fence: u64,
    pub token: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicaAck {
    pub operation_id: OperationId,
    pub chunk_id: ChunkId,
    pub placement_revision: u64,
    pub placement_epoch: u64,
    pub node_id: String,
    pub node_epoch: u64,
    pub device_id: String,
    pub device_epoch: u64,
    pub catalog_revision: u64,
    pub persisted_bytes: u64,
    pub verified_digest: ContentDigest,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum CopyRole {
    #[serde(alias = "Durable", alias = "DurableReplica")]
    #[default]
    DurableReplica,
    VerifiedCache,
    ExternalCommitted,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum CopyState {
    /// Decode-only compatibility for snapshots written before staging was
    /// removed from the Meta catalog. New code never creates or serves this
    /// state, so old staging records can never become readable Ready copies.
    #[serde(rename = "Staging")]
    LegacyStaging,
    #[serde(alias = "Durable", alias = "DurableReplica")]
    #[default]
    Ready,
    Corrupt,
    Deleting,
}

impl CopyState {
    pub fn is_readable(self) -> bool {
        self == Self::Ready
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CopyLocation {
    Node {
        node_id: String,
        node_epoch: u64,
        device_id: String,
        device_epoch: u64,
        catalog_revision: u64,
    },
    External {
        store_id: String,
        object_key: String,
        object_revision: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CopyRecord {
    pub id: CopyId,
    pub chunk_id: ChunkId,
    #[serde(default)]
    pub role: CopyRole,
    pub location: CopyLocation,
    pub state: CopyState,
    pub persisted_bytes: u64,
    pub verified_digest: ContentDigest,
}

impl CopyRecord {
    pub fn node_location(&self) -> Option<CopyLocation> {
        match &self.location {
            CopyLocation::Node {
                node_id,
                node_epoch,
                device_id,
                device_epoch,
                catalog_revision,
            } => Some(CopyLocation::Node {
                node_id: node_id.clone(),
                node_epoch: *node_epoch,
                device_id: device_id.clone(),
                device_epoch: *device_epoch,
                catalog_revision: *catalog_revision,
            }),
            CopyLocation::External { .. } => None,
        }
    }

    pub fn is_ready_durable(&self) -> bool {
        self.role == CopyRole::DurableReplica && self.state.is_readable()
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum PlacementHealth {
    #[default]
    Satisfied,
    UnderReplicated,
    BlockedNoSource,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlacementRecord {
    pub chunk_id: ChunkId,
    pub replica_group_id: ReplicaGroupId,
    #[serde(alias = "epoch")]
    pub placement_epoch: u64,
    #[serde(default = "one_copy")]
    pub desired_copies: u16,
    pub copies: Vec<CopyId>,
    #[serde(default)]
    pub health: PlacementHealth,
}

const fn one_copy() -> u16 {
    1
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChunkReceipt {
    pub operation_id: OperationId,
    pub chunk: ChunkObject,
    pub placement_revision: u64,
    pub placement_epoch: u64,
    pub replica_group_id: ReplicaGroupId,
    pub durable_acks: Vec<ReplicaAck>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ReplicationTaskState {
    Pending,
    Running,
    RetryWaiting,
    Completed,
    BlockedNoSource,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicationClaim {
    pub task_id: ReplicationTaskId,
    pub operation_id: OperationId,
    pub worker_node_id: String,
    pub worker_node_epoch: u64,
    pub worker_session_id: String,
    pub expires_at_unix_ms: u64,
    pub fence: u64,
    pub chunk: ChunkObject,
    pub source_copy_id: CopyId,
    pub placement_revision: u64,
    pub replica_group: ReplicaGroup,
    pub replication: ReplicationConfig,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReplicationTask {
    pub id: ReplicationTaskId,
    pub chunk_id: ChunkId,
    pub placement_epoch: u64,
    pub desired_copies: u16,
    pub existing_copies: Vec<CopyId>,
    pub state: ReplicationTaskState,
    pub attempt: u32,
    pub next_retry_unix_ms: u64,
    pub last_error: Option<String>,
    #[serde(default)]
    pub claim: Option<Box<ReplicationClaim>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ClaimReplicationTask {
    pub caller_id: String,
    pub caller_session_id: String,
    pub caller_node_epoch: u64,
    pub operation_id: OperationId,
    pub lease_seconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReportReplicationTask {
    pub caller_id: String,
    pub caller_session_id: String,
    pub caller_node_epoch: u64,
    pub operation_id: OperationId,
    pub claim: ReplicationClaim,
    pub durable_acks: Vec<ReplicaAck>,
    pub error: Option<String>,
    pub source_invalid: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReportChunkCorruption {
    pub caller_id: String,
    pub caller_session_id: String,
    pub caller_node_epoch: u64,
    pub operation_id: OperationId,
    pub chunk_id: ChunkId,
    pub device_id: String,
    pub device_epoch: u64,
    pub catalog_revision: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WriteLease {
    pub inode_id: InodeId,
    pub owner_node_id: String,
    pub owner_session_id: String,
    pub lease_epoch: u64,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DfsReadGrant {
    pub namespace_id: NamespaceId,
    pub file_version_id: FileVersionId,
    pub layout_root_id: LayoutRootId,
    pub caller_node_id: String,
    pub caller_node_epoch: u64,
    pub expires_at_unix_ms: u64,
    pub fence: u64,
    pub token: String,
}

/// One authenticated, immutable source context presented by a receiver to Meta.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DfsReadValidation {
    pub grant: DfsReadGrant,
    pub chunk_id: ChunkId,
    pub copy_id: CopyId,
    pub chunk_offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DfsAuthorizedRead {
    pub validation: DfsReadValidation,
    pub allowed_ranges: Vec<(u64, u64)>,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ValidateDfsReadGrants {
    pub receiver_node_id: String,
    pub receiver_node_epoch: u64,
    pub peer_node_id: String,
    pub validations: Vec<DfsReadValidation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SourceCandidate {
    pub copy_id: CopyId,
    pub chunk_id: ChunkId,
    pub role: CopyRole,
    pub state: CopyState,
    pub location: CopyLocation,
    pub data_endpoint: Option<String>,
    pub load_hint: u32,
    pub read_grant: DfsReadGrant,
}

impl SourceCandidate {
    pub fn node_data_endpoint(&self) -> Option<&str> {
        self.data_endpoint.as_deref()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChunkSources {
    pub chunk_id: ChunkId,
    pub sources: Vec<SourceCandidate>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DfsChunkSourcesRequest {
    pub caller_id: String,
    pub namespace_id: NamespaceId,
    pub file_version_id: FileVersionId,
    pub layout_root_id: LayoutRootId,
    pub chunk_ids: Vec<ChunkId>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct DfsChunkSourcesReply {
    pub revision: u64,
    pub chunks: Vec<ChunkSources>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CommitMetadataMode {
    DataOnly,
    Full,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitMetadataDelta {
    pub mode: CommitMetadataMode,
    pub mtime_unix_ms: Option<u64>,
    pub ctime_unix_ms: Option<u64>,
    /// Kernel killpriv v2 side effect, accepted only with a valid write lease.
    #[serde(default)]
    pub kill_suidgid: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitFileVersion {
    pub operation_id: OperationId,
    pub inode_id: InodeId,
    pub write_lease: WriteLease,
    pub expected_inode_revision: u64,
    pub expected_head_version: Option<FileVersionId>,
    pub file_version: FileVersion,
    pub layout_root: LayoutRoot,
    pub chunk_receipts: Vec<ChunkReceipt>,
    pub metadata_delta: CommitMetadataDelta,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SyncInodeMetadata {
    pub operation_id: OperationId,
    pub inode_id: InodeId,
    pub write_lease: WriteLease,
    pub expected_inode_revision: u64,
    pub expected_head_version: Option<FileVersionId>,
    pub metadata_delta: CommitMetadataDelta,
}
