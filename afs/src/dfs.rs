//! Shared DistributedFs identities and immutable metadata records.
//!
//! Mutable POSIX state is represented by an `InodeRecord` whose head points at
//! an immutable `FileVersion`. A version owns an immutable extent layout; every
//! extent refers to an immutable chunk. Physical copies are tracked separately.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

mod cow;
mod gc;
pub use cow::clone_file_layout;
pub use gc::{GcFence, GcReference, GcState, UpdateGcReference};

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
    /// BLAKE3 binding of a versioned 32KiB CRC32C table, never a body BLAKE3 root.
    Crc32cSegmentsV1,
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

impl ChunkObject {
    /// Birth generation of an independently named CRC object. Repairs retain it.
    pub fn write_generation(&self) -> Option<&str> {
        let value = self.id.0.strip_prefix("crc32c-v2-")?;
        let (generation, object) = value.split_once('-')?;
        let hex = |s: &str| {
            s.len() == 32
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        (hex(generation) && hex(object)).then_some(generation)
    }

    pub fn generation_id(operation: &OperationId) -> String {
        blake3::hash(operation.0.as_bytes()).to_hex()[..32].to_string()
    }

    pub fn is_segment_crc32c(&self) -> bool {
        self.content_digest.algorithm == DigestAlgorithm::Crc32cSegmentsV1
    }
    pub fn valid_immutable_identity(&self) -> bool {
        if !self.is_segment_crc32c() {
            return Self::from_content_addressed_id(&self.id).as_ref() == Some(self);
        }
        self.length > 0
            && self.length <= 4 * 1024 * 1024
            && self.encoding == ChunkEncoding::Raw
            && self.write_generation().is_some()
    }

    /// Recover the standard root and length committed by a canonical ChunkId.
    /// Other historical IDs have no content-addressed authority; callers must
    /// retain their complete verification fallback rather than infer a root.
    pub fn from_content_addressed_id(chunk_id: &ChunkId) -> Option<Self> {
        let (hex, length) = content_addressed_parts(chunk_id)?;
        let mut bytes = [0; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).ok()?;
        }
        Some(Self {
            id: chunk_id.clone(),
            length,
            content_digest: ContentDigest {
                algorithm: DigestAlgorithm::Blake3,
                bytes,
            },
            encoding: ChunkEncoding::Raw,
        })
    }
}

fn content_addressed_parts(chunk_id: &ChunkId) -> Option<(&str, u64)> {
    let content = chunk_id.0.strip_prefix("blake3-")?;
    let (digest, length) = content.split_once('-')?;
    let parsed_length = length.parse::<u64>().ok()?;
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || parsed_length.to_string() != length
    {
        return None;
    }
    Some((digest, parsed_length))
}

/// Immutable durability contract; external storage never counts as a Node replica.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub enum DfsDurabilityPolicy {
    #[default]
    NodeReplicated,
    ExternalBackend {
        store_id: String,
        generation: String,
    },
}

/// Stable backend acknowledgement, bound to the enclosing receipt's operation and chunk.
/// ETag and backend version IDs are deliberately excluded from commit identity.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExternalCommitReceipt {
    pub store_id: String,
    pub generation: String,
    pub object_key: String,
    pub persisted_bytes: u64,
    pub verified_digest: ContentDigest,
}

/// Reject ambiguous or unsafe object-namespace path segments.
pub fn validate_external_store_identity(store_id: &str, generation: &str) -> afs_error::Result<()> {
    let safe_segment = |value: &str| {
        !value.is_empty()
            && value != "."
            && value != ".."
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
    };
    if !safe_segment(store_id) || !safe_segment(generation) {
        return Err(afs_error::Error::coded(
            afs_error::CLIENT_ARGUMENT_INVALID,
            "invalid external store identity",
        ));
    }
    Ok(())
}

/// Canonical immutable object key. Store ID is a filesystem/store namespace.
/// Invalid path segments or noncanonical chunk identities return an argument error.
pub fn external_object_key(
    store_id: &str,
    generation: &str,
    chunk_id: &ChunkId,
) -> afs_error::Result<String> {
    validate_external_store_identity(store_id, generation)?;
    let invalid = || {
        afs_error::Error::coded(
            afs_error::CLIENT_ARGUMENT_INVALID,
            "invalid external chunk identity",
        )
    };
    let (digest, _) = content_addressed_parts(chunk_id).ok_or_else(invalid)?;
    Ok(format!(
        "afs/v1/{store_id}/{generation}/chunks/{}/{}",
        &digest[..2],
        chunk_id.0
    ))
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
        /// Fixed store generation; backend ETag/version observations are not identity.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_commit: Option<ExternalCommitReceipt>,
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

/// Read range whose SourceCandidate is intentionally absent from the Node cache.
/// Meta resolves it and authorizes it in the same current-head read view.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DfsMissingReadSource {
    pub chunk_id: ChunkId,
    pub chunk_offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DfsAuthorizedRead {
    pub validation: DfsReadValidation,
    pub allowed_ranges: Vec<(u64, u64)>,
    pub expires_at_unix_ms: u64,
}

/// Fresh authorization for a trusted Node fetching an external object directly.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DfsExternalAuthorizedRead {
    pub authorized: DfsAuthorizedRead,
    pub chunk: ChunkObject,
    pub location: CopyLocation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ValidateExternalReadGrants {
    pub caller_id: String,
    pub validations: Vec<DfsReadValidation>,
}

/// Speculative fixed-version read plan requiring a fresh head and authorization
/// from one Meta authority view. No file bytes are returned by this operation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorizeCurrentExternalRead {
    pub caller_id: String,
    pub inode_id: InodeId,
    pub namespace_id: NamespaceId,
    pub expected_head_version: FileVersionId,
    pub layout_root_id: LayoutRootId,
    /// Existing grants that must be revalidated exactly as supplied.
    pub validations: Vec<DfsReadValidation>,
    /// Ranges whose source was missing from the Node cache and must be resolved
    /// in the same Meta view before authorization.
    pub missing_sources: Vec<DfsMissingReadSource>,
}

/// A changed head never carries authority to consume the speculative plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AuthorizeCurrentExternalReadReply {
    Authorized {
        inode: InodeRecord,
        authorizations: Vec<DfsExternalAuthorizedRead>,
    },
    HeadChanged(InodeRecord),
}

/// Re-admits a trusted reader against one immutable FileVersion and layout.
/// This does not accept old MAC input and does not require the version to still
/// be the inode head; open-time freshness is a separate caller contract.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorizeExternalVersionRead {
    pub caller_id: String,
    pub inode_id: InodeId,
    pub namespace_id: NamespaceId,
    pub file_version_id: FileVersionId,
    pub layout_root_id: LayoutRootId,
    pub requests: Vec<DfsMissingReadSource>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorizeExternalVersionReadReply {
    pub authorizations: Vec<DfsExternalAuthorizedRead>,
    pub remaining_validity_ms: u64,
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
    #[serde(default)]
    pub chunk: Option<ChunkObject>,
    pub sources: Vec<SourceCandidate>,
    /// Committed chunk-relative ranges covered by these existing grants.
    /// Missing or overly fragmented hints stay empty and cannot authorize
    /// fetching the whole object instead of the requested range.
    #[serde(default)]
    pub allowed_ranges: Vec<(u64, u64)>,
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
pub struct CloneFileRange {
    pub version: FileVersionId,
    pub source_offset: u64,
    pub destination_offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommitFileVersion {
    /// Frozen before uploads start; never refreshed when retrying a commit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gc_fence: Option<GcFence>,
    pub operation_id: OperationId,
    pub inode_id: InodeId,
    pub write_lease: WriteLease,
    pub expected_inode_revision: u64,
    pub expected_head_version: Option<FileVersionId>,
    pub file_version: FileVersion,
    pub layout_root: LayoutRoot,
    pub chunk_receipts: Vec<ChunkReceipt>,
    /// COW inherits only the specified range of an immutable source version.
    #[serde(default)]
    pub clone_source: Option<CloneFileRange>,
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

#[cfg(test)]
mod external_identity_tests {
    use super::*;

    #[test]
    fn committed_content_addressed_id_anchors_standard_root_and_length() {
        for body in [Vec::new(), vec![19; 32_769]] {
            let root = blake3::hash(&body);
            let id = ChunkId::new(format!("blake3-{}-{}", root.to_hex(), body.len()));
            let chunk = ChunkObject::from_content_addressed_id(&id).unwrap();
            assert_eq!(chunk.id, id);
            assert_eq!(chunk.length, body.len() as u64);
            assert_eq!(chunk.content_digest.bytes, *root.as_bytes());
            assert_eq!(chunk.encoding, ChunkEncoding::Raw);
        }
        for id in [
            "legacy-object".to_owned(),
            format!("blake3-{}-01", "a".repeat(64)),
            format!("blake3-{}-1", "A".repeat(64)),
            format!("blake3-{}-18446744073709551616", "a".repeat(64)),
            format!("blake3-{}-1", "é".repeat(32)),
        ] {
            assert!(ChunkObject::from_content_addressed_id(&ChunkId::new(id)).is_none());
        }
    }

    #[test]
    fn node_receipt_preserves_pre_external_exact_replay_digest() {
        // This is the previous wire-independent commit receipt shape. Persisted
        // namespace outcomes hash these exact serialized bytes, including order.
        #[derive(Serialize)]
        struct LegacyReceipt<'a> {
            operation_id: &'a OperationId,
            chunk: &'a ChunkObject,
            placement_revision: u64,
            placement_epoch: u64,
            replica_group_id: &'a ReplicaGroupId,
            durable_acks: &'a [ReplicaAck],
        }
        let receipt = ChunkReceipt {
            operation_id: OperationId::new("commit-original"),
            chunk: ChunkObject {
                id: ChunkId::new(format!("blake3-{}-42", "a".repeat(64))),
                length: 42,
                content_digest: ContentDigest {
                    algorithm: DigestAlgorithm::Blake3,
                    bytes: [0xaa; 32],
                },
                encoding: ChunkEncoding::Raw,
            },
            placement_revision: 7,
            placement_epoch: 3,
            replica_group_id: ReplicaGroupId::new("group-original"),
            durable_acks: Vec::new(),
            external_commit: None,
        };
        let legacy = LegacyReceipt {
            operation_id: &receipt.operation_id,
            chunk: &receipt.chunk,
            placement_revision: receipt.placement_revision,
            placement_epoch: receipt.placement_epoch,
            replica_group_id: &receipt.replica_group_id,
            durable_acks: &receipt.durable_acks,
        };
        let old_bytes = serde_json::to_vec(&legacy).unwrap();
        let new_bytes = serde_json::to_vec(&receipt).unwrap();
        assert_eq!(new_bytes, old_bytes);
        assert_eq!(blake3::hash(&new_bytes), blake3::hash(&old_bytes));
        let recovered: ChunkReceipt = serde_json::from_slice(&old_bytes).unwrap();
        assert_eq!(recovered, receipt);
        assert_eq!(serde_json::to_vec(&recovered).unwrap(), old_bytes);
    }

    #[test]
    fn external_key_is_stable_and_rejects_path_or_chunk_aliases() {
        let chunk = ChunkId::new(format!("blake3-{}-42", "a".repeat(64)));
        assert_eq!(
            external_object_key("filesystem", "generation-1", &chunk).unwrap(),
            format!("afs/v1/filesystem/generation-1/chunks/aa/{}", chunk.0)
        );
        for segment in ["", "..", "a/b", "a b", "a%2fb"] {
            assert!(external_object_key(segment, "generation", &chunk).is_err());
            assert!(external_object_key("store", segment, &chunk).is_err());
        }
        for identity in [
            format!("blake3-{}-042", "a".repeat(64)),
            format!("blake3-{}-42", "A".repeat(64)),
            "short".into(),
        ] {
            assert!(external_object_key("store", "generation", &ChunkId::new(identity)).is_err());
        }
    }
}

#[cfg(test)]
mod crc32c_format_tests {
    #[test]
    fn crc32c_descriptor_has_distinct_persisted_algorithm() {
        let descriptor: super::ContentDigest = serde_json::from_value(serde_json::json!({
            "algorithm":"Crc32cSegmentsV1", "bytes":vec![7_u8;32]
        }))
        .unwrap();
        assert_ne!(descriptor.algorithm, super::DigestAlgorithm::Blake3);
        assert_eq!(
            serde_json::to_value(descriptor).unwrap()["algorithm"],
            "Crc32cSegmentsV1"
        );
    }
}
