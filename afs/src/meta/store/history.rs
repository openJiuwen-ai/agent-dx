//! Deterministic history retention for the canonical single writer.
//! The index owns metadata only; outcomes/events and their bytes are charged
//! by the owner. A selected delta is not effective until durable publication.
use std::{collections::BTreeSet, time::Duration};

use afs_error::Result;
use serde::{Deserialize, Serialize};

use super::{RequestKey, StoreRevision, invalid_contract, state::RequestCompletion};

const MAX_CLEANUP: usize = 65_536;

/// Logical retained capacities, not allocator statistics or RSS. The fixed
/// tree-root reserve is separate from the per-history payload/entry limits.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Retained {
    pub requests: usize,
    pub request_bytes: u64,
    pub events: usize,
    pub event_bytes: u64,
    pub index_base_bytes: u64,
}
impl Retained {
    pub(super) fn total_bytes(self) -> Option<u64> {
        self.request_bytes
            .checked_add(self.event_bytes)?
            .checked_add(self.index_base_bytes)
    }
}
// im15.1 uses64-entry B-tree nodes, with32-entry non-root occupancy. Entry
// charges include amortized structural slack; this separate fixed reserve
// covers partially occupied roots of the history/completion/order trees.
const INDEX_BASE_BYTES: u64 = 64 * 1024;
const REQUEST_ENTRY_BYTES: usize = 1024;
const EVENT_ENTRY_BYTES: usize = 64;
const EVENT_GROUP_BYTES: u64 = 512;

/// Visits owned capacities without encoding, allocating or deduplicating away
/// references retained by distinct owners. Shared historical values are charged
/// in full even when the current authority also retains equivalent content.
struct Charge(u64);
impl Charge {
    fn add(&mut self, bytes: usize) -> Result<()> {
        let bytes = u64::try_from(bytes)
            .map_err(|_| invalid_contract("history capacity conversion overflow"))?;
        self.0 = self
            .0
            .checked_add(bytes)
            .ok_or_else(|| invalid_contract("history owned byte overflow"))?;
        Ok(())
    }
    fn value<T: Heap>(&mut self, value: &T) -> Result<()> {
        value.heap(self)
    }
}
trait Heap {
    fn heap(&self, charge: &mut Charge) -> Result<()>;
}
macro_rules! copy_heap {
    ($($ty:ty),* $(,)?)=>{$(impl Heap for $ty {
        fn heap(&self, _charge: &mut Charge) -> Result<()> {
            fn require_copy<T: Copy>(_: &T) {} require_copy(self);Ok(())
        }
    })*};
}
macro_rules! fields_heap {
    ($ty:ty { $($field:ident),* $(,)? })=>{impl Heap for $ty {
        fn heap(&self, charge: &mut Charge) -> Result<()> {
            // Exhaustive destructuring makes a new model field require review.
            let Self {$($field),*}=self;$(charge.value($field)?;)* Ok(())
        }
    }};
}
impl Heap for String {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        c.add(self.capacity())
    }
}
impl<T: Heap> Heap for Vec<T> {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        c.add(
            self.capacity()
                .checked_mul(std::mem::size_of::<T>())
                .ok_or_else(|| invalid_contract("history vector capacity overflow"))?,
        )?;
        for value in self {
            c.value(value)?;
        }
        Ok(())
    }
}
impl<T: Heap> Heap for Option<T> {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        if let Some(value) = self {
            c.value(value)?;
        }
        Ok(())
    }
}
impl<T: Heap> Heap for Box<T> {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        c.add(std::mem::size_of::<T>())?;
        c.value(self.as_ref())
    }
}
impl<T: Heap, const N: usize> Heap for [T; N] {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        for value in self {
            c.value(value)?;
        }
        Ok(())
    }
}
impl<K: Ord + Heap, V: Heap> Heap for std::collections::BTreeMap<K, V> {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        // std's nodes are private. Count all key/value capacities exactly and use
        // a conservative per-entry node reserve (not an allocator-byte claim).
        let node = std::mem::size_of::<(K, V)>()
            .checked_add(std::mem::size_of::<usize>())
            .and_then(|n| n.checked_mul(16))
            .and_then(|n| n.checked_add(256))
            .and_then(|n| n.checked_mul(self.len()))
            .ok_or_else(|| invalid_contract("history map capacity overflow"))?;
        c.add(node)?;
        for (key, value) in self {
            c.value(key)?;
            c.value(value)?;
        }
        Ok(())
    }
}
use crate::dfs;
copy_heap!(
    std::time::Duration,
    u8,
    u16,
    u32,
    u64,
    usize,
    bool,
    StoreRevision,
    super::StoreOperation,
    super::RootRight,
    super::RootCommandType,
    dfs::InodeKind,
    dfs::DigestAlgorithm,
    dfs::ChunkEncoding,
    dfs::LocalCopyPolicy,
    dfs::CopyRole,
    dfs::CopyState,
    dfs::PlacementHealth,
    dfs::ReplicationTaskState
);
macro_rules! id_heap {($($ty:ty),* $(,)?)=>{$(impl Heap for $ty {fn heap(&self,c:&mut Charge)->Result<()>{c.value(&self.0)}})*};}
id_heap!(
    dfs::NamespaceId,
    dfs::InodeId,
    dfs::FileVersionId,
    dfs::LayoutRootId,
    dfs::ChunkId,
    dfs::CopyId,
    dfs::OperationId,
    dfs::ReplicaGroupId,
    dfs::ReplicationTaskId
);
fields_heap!(RequestKey {
    caller_id,
    request_id
});
fields_heap!(super::MetaTxn {
    request,
    operation,
    conditions,
    mutations
});
fields_heap!(super::NodeSessionLease {
    node_id,
    session_id,
    grpc_addr,
    data_addr,
    rest_addr,
    storage_devices,
    lease_ttl
});
impl Heap for super::TxnMutation {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        match self {
            Self::Put(value) => c.value(value),
            Self::Delete(value) => c.value(value),
            Self::RecordRequestOutcome(value) => c.value(value),
        }
    }
}
impl Heap for super::TxnCondition {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        match self {
            Self::RootEpochWatermarkEquals(value) => c.value(value),
            Self::StoreRevisionEquals(value) => c.value(value),
            Self::Missing(value) => c.value(value),
            Self::RevisionEquals { key, revision } => {
                c.value(key)?;
                c.value(revision)
            }
            Self::EntityEquals(value) => c.value(value),
            Self::NodeSessionCurrent {
                node_id,
                session_id,
            } => {
                c.value(node_id)?;
                c.value(session_id)
            }
            Self::RootEpochEquals {
                root_id,
                root_epoch,
            } => {
                c.value(root_id)?;
                c.value(root_epoch)
            }
            Self::RootCommandAcked {
                command_id,
                node_id,
                session_id,
            } => {
                c.value(command_id)?;
                c.value(node_id)?;
                c.value(session_id)
            }
            Self::RootCommandMatches {
                command_id,
                home_node_id,
                home_session_id,
                root_id,
                root_epoch,
                old_access_generation,
                command_type,
            } => {
                c.value(command_id)?;
                c.value(home_node_id)?;
                c.value(home_session_id)?;
                c.value(root_id)?;
                c.value(root_epoch)?;
                c.value(old_access_generation)?;
                c.value(command_type)
            }
            Self::RootCommandAckedExact {
                command_id,
                node_id,
                session_id,
                root_id,
                root_epoch,
                access_generation,
                command_type,
            } => {
                c.value(command_id)?;
                c.value(node_id)?;
                c.value(session_id)?;
                c.value(root_id)?;
                c.value(root_epoch)?;
                c.value(access_generation)?;
                c.value(command_type)
            }
            Self::RequestAbsent(value) => c.value(value),
            Self::DfsDirectoryEmpty {
                namespace_id,
                parent_inode_id,
            } => {
                c.value(namespace_id)?;
                c.value(parent_inode_id)
            }
            Self::DfsNotDescendant {
                namespace_id,
                ancestor_inode_id,
                child_inode_id,
            } => {
                c.value(namespace_id)?;
                c.value(ancestor_inode_id)?;
                c.value(child_inode_id)
            }
        }
    }
}
pub(super) fn transaction_charge(value: &super::MetaTxn) -> Result<u64> {
    let mut c = owned(value)?;
    c.add(2048)?;
    Ok(c.0)
}
pub(super) fn registration_charge(
    key: &RequestKey,
    value: &super::NodeSessionLease,
) -> Result<u64> {
    let mut c = owned(value)?;
    c.add(std::mem::size_of::<RequestKey>())?;
    c.value(key)?;
    c.add(2048)?;
    Ok(c.0)
}
pub(super) fn protection_charge(key: &RequestKey) -> Result<u64> {
    let mut c = owned(key)?;
    c.add(1024)?;
    Ok(c.0)
}

fields_heap!(super::RequestOutcome {
    request,
    operation,
    result
});
fields_heap!(super::WatchEvent {
    revision,
    event_index,
    key,
    change
});
fields_heap!(super::NodeSession {
    node_id,
    session_id,
    grpc_addr,
    data_addr,
    rest_addr,
    storage_devices,
    lease_epoch,
    expires_at_unix_ms
});
fields_heap!(super::RootRecord {
    root_id,
    root_epoch,
    home_node_id,
    home_session_id,
    local_prepare_id,
    created_at_revision,
    updated_at_revision
});
fields_heap!(super::RootReservationRecord {
    root_id,
    root_epoch,
    home_node_id,
    home_session_id,
    create_intent_id,
    prepare_token,
    created_at_revision
});
fields_heap!(super::RootAccessGrant {
    root_id,
    root_epoch,
    home_node_id,
    home_session_id,
    holder_node_id,
    holder_session_id,
    access_generation,
    rights,
    fencing_token,
    issued_at_revision
});
fields_heap!(super::RootCommandRecord {
    command_id,
    home_node_id,
    home_session_id,
    root_id,
    root_epoch,
    old_access_generation,
    command_type
});
fields_heap!(super::RootCommandAck {
    command_id,
    node_id,
    session_id,
    root_id,
    root_epoch,
    access_generation,
    success,
    message
});
fields_heap!(dfs::InodeAttributes {
    mode,
    uid,
    gid,
    nlink,
    atime_unix_ms,
    mtime_unix_ms,
    ctime_unix_ms
});
fields_heap!(dfs::InodeRecord {
    namespace_id,
    inode_id,
    kind,
    attributes,
    head_version,
    symlink_target,
    xattrs,
    revision
});
fields_heap!(dfs::DentryKey {
    namespace_id,
    parent_inode_id,
    name
});
fields_heap!(dfs::Dentry { key, inode_id });
fields_heap!(dfs::RenameOutcome {
    inode,
    replaced_inode
});
fields_heap!(dfs::FileVersion {
    id,
    inode_id,
    parent_version,
    length,
    layout_root,
    created_at_unix_ms
});
fields_heap!(dfs::Extent {
    file_offset,
    length,
    chunk_id,
    chunk_offset
});
fields_heap!(dfs::LayoutRoot {
    id,
    file_length,
    inline_extents
});
fields_heap!(dfs::ContentDigest { algorithm, bytes });
fields_heap!(dfs::ChunkObject {
    id,
    length,
    content_digest,
    encoding
});
fields_heap!(dfs::ReplicationConfig {
    desired_copies,
    sync_required_copies,
    min_distinct_nodes,
    min_distinct_failure_domains,
    local_copy
});
fields_heap!(dfs::StorageDeviceDescriptor {
    device_id,
    device_epoch,
    catalog_revision,
    failure_domain
});
fields_heap!(dfs::ReplicaTarget {
    node_id,
    node_epoch,
    data_endpoint,
    device
});
fields_heap!(dfs::ReplicaGroup {
    id,
    placement_epoch,
    targets
});
fields_heap!(dfs::CopyRecord {
    id,
    chunk_id,
    role,
    location,
    state,
    persisted_bytes,
    verified_digest
});
fields_heap!(dfs::PlacementRecord {
    chunk_id,
    replica_group_id,
    placement_epoch,
    desired_copies,
    copies,
    health
});
fields_heap!(dfs::ReplicationClaim {
    task_id,
    operation_id,
    worker_node_id,
    worker_node_epoch,
    worker_session_id,
    expires_at_unix_ms,
    fence,
    chunk,
    source_copy_id,
    placement_revision,
    replica_group,
    replication
});
fields_heap!(dfs::ReplicationTask {
    id,
    chunk_id,
    placement_epoch,
    desired_copies,
    existing_copies,
    state,
    attempt,
    next_retry_unix_ms,
    last_error,
    claim
});
fields_heap!(dfs::WriteLease {
    inode_id,
    owner_node_id,
    owner_session_id,
    lease_epoch,
    expires_at_unix_ms
});
impl Heap for dfs::DfsDurabilityPolicy {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        match self {
            Self::NodeReplicated => Ok(()),
            Self::ExternalBackend {
                store_id,
                generation,
            } => {
                c.value(store_id)?;
                c.value(generation)
            }
        }
    }
}
impl Heap for dfs::CopyLocation {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        match self {
            Self::Node {
                node_id,
                node_epoch,
                device_id,
                device_epoch,
                catalog_revision,
            } => {
                c.value(node_id)?;
                c.value(node_epoch)?;
                c.value(device_id)?;
                c.value(device_epoch)?;
                c.value(catalog_revision)
            }
            Self::External {
                store_id,
                object_key,
                object_revision,
            } => {
                c.value(store_id)?;
                c.value(object_key)?;
                c.value(object_revision)
            }
        }
    }
}
impl Heap for super::MetaKey {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        match self {
            Self::NodeSession {
                node_id,
                session_id,
            } => {
                c.value(node_id)?;
                c.value(session_id)
            }
            Self::CurrentNodeSession { node_id } => c.value(node_id),
            Self::CurrentNodeSessions | Self::RootEpochWatermark | Self::DfsGc => Ok(()),
            Self::Root { root_id } | Self::RootReservation { root_id } => c.value(root_id),
            Self::RootGrant {
                root_id,
                holder_node_id,
                holder_session_id,
            } => {
                c.value(root_id)?;
                c.value(holder_node_id)?;
                c.value(holder_session_id)
            }
            Self::RootCommand { command_id } => c.value(command_id),
            Self::RootCommandAck {
                command_id,
                home_node_id,
                home_session_id,
            } => {
                c.value(command_id)?;
                c.value(home_node_id)?;
                c.value(home_session_id)
            }
            Self::DfsReference {
                node_id,
                session_id,
                reference_id,
            } => {
                c.value(node_id)?;
                c.value(session_id)?;
                c.value(reference_id)
            }
            Self::RequestOutcome(value) => c.value(value),
            Self::DfsDentry(value) => c.value(value),
            Self::DfsInode(value) | Self::DfsWriteLease(value) => c.value(value),
            Self::DfsFileVersion(value) => c.value(value),
            Self::DfsLayoutRoot(value) => c.value(value),
            Self::DfsChunk(value) | Self::DfsPlacement(value) => c.value(value),
            Self::DfsCopy(value) => c.value(value),
            Self::DfsReplicationTask(value) => c.value(value),
            Self::DfsReplicationConfig | Self::DfsDurabilityPolicy => Ok(()),
        }
    }
}
fields_heap!(crate::dfs::GcReference {
    node_id,
    session_id,
    reference_id,
    inode_id,
    publication,
    chunks,
    retired_at_unix_ms
});
fields_heap!(crate::dfs::GcState {
    generation,
    chunks,
    waiting_nodes
});
macro_rules! entity_heap {($($variant:ident),* $(,)?)=>{impl Heap for super::MetaEntity {fn heap(&self,c:&mut Charge)->Result<()>{match self {$(Self::$variant(value)=>c.value(value),)*}}}};}
entity_heap!(
    NodeSession,
    RootReservation,
    Root,
    RootGrant,
    RootCommand,
    RootCommandAck,
    DfsDentry,
    DfsInode,
    DfsFileVersion,
    DfsLayoutRoot,
    DfsChunk,
    DfsReplicationConfig,
    DfsDurabilityPolicy,
    DfsPlacement,
    DfsCopy,
    DfsReplicationTask,
    DfsWriteLease,
    RootEpochWatermark,
    DfsReference,
    DfsGc
);
impl Heap for super::WatchChange {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        match self {
            Self::Put(value) => c.value(value),
            Self::Delete => Ok(()),
        }
    }
}
impl Heap for super::OperationResult {
    fn heap(&self, c: &mut Charge) -> Result<()> {
        match self {
            Self::DfsNamespace {
                request_digest,
                result,
            } => {
                c.value(request_digest)?;
                c.value(result)
            }
            Self::NodeSession(value) => c.value(value),
            Self::RootReservation(value) => c.value(value),
            Self::RootRecord(value) => c.value(value),
            Self::RootGrant(value) => c.value(value),
            Self::RootCommand(value) => c.value(value),
            Self::RootCommandAck(value) => c.value(value),
            Self::DfsInode(value) => c.value(value),
            Self::DfsInodeWithLease { inode, lease } => {
                c.value(inode)?;
                c.value(lease)
            }
            Self::DfsWriteLease(value) => c.value(value),
            Self::DfsReplicationClaim {
                request_digest,
                claim,
            } => {
                c.value(request_digest)?;
                c.value(claim)
            }
            Self::DfsReplicationTask(value) => c.value(value),
            Self::DfsRename(value) => c.value(value),
            Self::Empty => Ok(()),
        }
    }
}
fn owned<T: Heap>(value: &T) -> Result<Charge> {
    let mut c = Charge(0);
    c.add(std::mem::size_of::<T>())?;
    c.value(value)?;
    Ok(c)
}
pub(super) fn request_charge(value: &super::RequestOutcome) -> Result<u64> {
    let mut c = owned(value)?;
    c.add(REQUEST_ENTRY_BYTES)?;
    // Separate State outcome/completion and derived index/order keys retain
    // four additional RequestKeys. Count owned capacities, not Arc sizes only.
    for _ in 0..4 {
        c.add(std::mem::size_of::<RequestKey>())?;
        c.value(&value.request)?;
    }
    Ok(c.0)
}
pub(super) fn event_charge(value: &super::WatchEvent) -> Result<u64> {
    let mut c = owned(value)?;
    c.add(EVENT_ENTRY_BYTES)?;
    Ok(c.0)
}

/// Full primary authority ownership; separate from retained outcome/event bytes.
pub(super) fn authority_charge(key: &super::MetaKey, entity: &super::MetaEntity) -> Result<u64> {
    let mut charge = Charge(1024);
    charge.add(std::mem::size_of::<super::MetaKey>())?;
    charge.add(std::mem::size_of::<super::state::VersionedEntity>())?;
    charge.value(key)?;
    charge.value(entity)?;
    Ok(charge.0)
}
pub(super) fn epoch_charge(node: &String) -> Result<u64> {
    let mut charge = Charge(1024);
    charge.add(std::mem::size_of::<String>() + std::mem::size_of::<u64>())?;
    charge.value(node)?;
    Ok(charge.0)
}

pub(super) fn events_charge<'a>(
    events: impl Iterator<Item = &'a super::WatchEvent>,
) -> Result<(usize, u64)> {
    let mut count = 0usize;
    let mut bytes = EVENT_GROUP_BYTES;
    for event in events {
        count = count
            .checked_add(1)
            .ok_or_else(|| invalid_contract("history group event count overflow"))?;
        bytes = bytes
            .checked_add(event_charge(event)?)
            .ok_or_else(|| invalid_contract("history group event bytes overflow"))?;
    }
    if count == 0 {
        bytes = 0;
    }
    Ok((count, bytes))
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Limits {
    pub request_count: usize,
    pub request_bytes: u64,
    pub request_retention: Duration,
    pub event_count: usize,
    pub event_bytes: u64,
    pub event_retention: Duration,
    pub cleanup_items: usize,
    pub protected_requests: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            request_count: 1_000_000,
            request_bytes: 256 * 1024 * 1024,
            request_retention: Duration::from_secs(24 * 60 * 60),
            event_count: 1_000_000,
            event_bytes: 64 * 1024 * 1024,
            event_retention: Duration::from_secs(60 * 60),
            cleanup_items: 4096,
            protected_requests: 1088,
        }
    }
}
impl Limits {
    pub fn poll(self) -> Duration {
        Duration::from_secs(1)
            .min(self.request_retention / 2)
            .min(self.event_retention / 2)
            .max(Duration::from_millis(1))
    }
    pub fn validate(self) -> Result<Self> {
        if self.request_count == 0
            || self.request_bytes == 0
            || self.event_count == 0
            || self.event_bytes == 0
            || self.request_retention.is_zero()
            || self.event_retention.is_zero()
            || u64::try_from(self.request_retention.as_millis()).is_err()
            || u64::try_from(self.event_retention.as_millis()).is_err()
            || !(1..=MAX_CLEANUP).contains(&self.cleanup_items)
            || self.protected_requests == 0
        {
            return Err(invalid_contract("invalid finite history retention limits"));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct RemovedRequest {
    pub key: RequestKey,
    pub completion: RequestCompletion,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Delta {
    pub(super) revision: StoreRevision,
    pub(super) selected_at_unix_ms: u64,
    pub(super) compacted_from: StoreRevision,
    pub(super) compacted_to: StoreRevision,
    #[serde(deserialize_with = "super::log::bounded_items")]
    pub(super) requests: Vec<RemovedRequest>,
}
impl Delta {
    pub fn validate(&self) -> Result<()> {
        if self.compacted_from > self.compacted_to
            || self.compacted_to > self.revision
            || self.requests.len() > MAX_CLEANUP
            || (self.requests.is_empty() && self.compacted_from == self.compacted_to)
        {
            return Err(invalid_contract("invalid history cleanup revision/count"));
        }
        let mut previous = None;
        for item in &self.requests {
            if !item.key.is_valid()
                || item.completion.revision == StoreRevision::ZERO
                || item.completion.revision > self.revision
                || previous.is_some_and(|key| key >= &item.key)
            {
                return Err(invalid_contract("invalid history cleanup identity/order"));
            }
            previous = Some(&item.key);
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RequestMeta {
    completion: RequestCompletion,
    held_bytes: u64,
}
#[derive(Clone)]
struct EventMeta {
    completed_at: Option<u64>,
    count: usize,
    held_bytes: u64,
}

#[derive(Clone, Default)]
pub(super) struct Index {
    requests: im::OrdMap<RequestKey, RequestMeta>,
    request_order: im::OrdMap<(u64, RequestKey), ()>,
    request_bytes: u64,
    events: im::OrdMap<StoreRevision, EventMeta>,
    event_expiry: im::OrdMap<(u64, StoreRevision), ()>,
    event_count: usize,
    event_bytes: u64,
}
impl Index {
    pub fn stats(&self) -> Retained {
        Retained {
            requests: self.requests.len(),
            request_bytes: self.request_bytes,
            events: self.event_count,
            event_bytes: self.event_bytes,
            index_base_bytes: INDEX_BASE_BYTES,
        }
    }
    pub fn track_request(
        &mut self,
        key: RequestKey,
        completion: RequestCompletion,
        held_bytes: u64,
    ) -> Result<()> {
        if !key.is_valid() || completion.revision == StoreRevision::ZERO || held_bytes == 0 {
            return Err(invalid_contract("invalid retained request metadata"));
        }
        let old = self.requests.get(&key);
        let bytes = self
            .request_bytes
            .checked_sub(old.map_or(0, |old| old.held_bytes))
            .and_then(|n| n.checked_add(held_bytes))
            .ok_or_else(|| invalid_contract("request history byte overflow"))?;
        if let Some(old) = old {
            self.request_order
                .remove(&(old.completion.completed_at_unix_ms, key.clone()));
        }
        self.request_order
            .insert((completion.completed_at_unix_ms, key.clone()), ());
        self.requests.insert(
            key,
            RequestMeta {
                completion,
                held_bytes,
            },
        );
        self.request_bytes = bytes;
        Ok(())
    }
    pub fn track_events(
        &mut self,
        revision: StoreRevision,
        completed_at: Option<u64>,
        count: usize,
        held_bytes: u64,
    ) -> Result<()> {
        if revision == StoreRevision::ZERO
            || count == 0
            || held_bytes == 0
            || self.events.contains_key(&revision)
        {
            return Err(invalid_contract(
                "invalid or duplicate event revision metadata",
            ));
        }
        let total = self
            .event_count
            .checked_add(count)
            .ok_or_else(|| invalid_contract("event history count overflow"))?;
        let bytes = self
            .event_bytes
            .checked_add(held_bytes)
            .ok_or_else(|| invalid_contract("event history byte overflow"))?;
        self.events.insert(
            revision,
            EventMeta {
                completed_at,
                count,
                held_bytes,
            },
        );
        if let Some(time) = completed_at {
            self.event_expiry.insert((time, revision), ());
        }
        self.event_count = total;
        self.event_bytes = bytes;
        Ok(())
    }
    pub fn fits(&self, limits: Limits) -> bool {
        let stats = self.stats();
        stats.total_bytes().is_some()
            && stats.requests <= limits.request_count
            && stats.request_bytes <= limits.request_bytes
            && stats.events <= limits.event_count
            && stats.event_bytes <= limits.event_bytes
    }
    pub fn select(
        &self,
        revision: StoreRevision,
        compacted_to: StoreRevision,
        now: u64,
        limits: Limits,
        protected: &BTreeSet<RequestKey>,
    ) -> Result<Option<Delta>> {
        let limits = limits.validate()?;
        if compacted_to > revision
            || self
                .events
                .iter()
                .next()
                .is_some_and(|(oldest, _)| *oldest <= compacted_to)
        {
            return Err(invalid_contract(
                "history index and compaction boundary disagree",
            ));
        }
        if protected.len() > limits.protected_requests {
            return Err(super::journal::unavailable(
                "in-flight history protection capacity exhausted",
            ));
        }
        let request_ttl = u64::try_from(limits.request_retention.as_millis())
            .map_err(|_| invalid_contract("request retention duration overflow"))?;
        let event_ttl = u64::try_from(limits.event_retention.as_millis())
            .map_err(|_| invalid_contract("event retention duration overflow"))?;
        let expired = |time: u64, ttl: u64| now.checked_sub(time).is_some_and(|age| age >= ttl);
        let mut requests = Vec::new();
        let mut count = self.requests.len();
        let mut bytes = self.request_bytes;
        for ((time, key), _) in self.request_order.iter() {
            if requests.len() == limits.cleanup_items {
                break;
            }
            if protected.contains(key) {
                continue;
            }
            if count <= limits.request_count
                && bytes <= limits.request_bytes
                && !expired(*time, request_ttl)
            {
                break;
            }
            let entry = self
                .requests
                .get(key)
                .ok_or_else(|| invalid_contract("request retention order is orphaned"))?;
            requests.push(RemovedRequest {
                key: key.clone(),
                completion: entry.completion.clone(),
            });
            count = count
                .checked_sub(1)
                .ok_or_else(|| invalid_contract("request retention count underflow"))?;
            bytes = bytes
                .checked_sub(entry.held_bytes)
                .ok_or_else(|| invalid_contract("request retention byte underflow"))?;
        }
        // Records use stable identity order, independent of HashMap iteration
        // and of the wall clock at replay time.
        requests.sort_by(|a, b| a.key.cmp(&b.key));
        let remaining = limits
            .cleanup_items
            .checked_sub(requests.len())
            .ok_or_else(|| invalid_contract("cleanup item accounting underflow"))?;
        let expired_revision = self
            .event_expiry
            .iter()
            .next()
            .filter(|((time, _), _)| expired(*time, event_ttl))
            .map(|((_, revision), _)| *revision);
        let mut event_count = self.event_count;
        let mut event_bytes = self.event_bytes;
        let mut through = compacted_to;
        for (revision, entry) in self.events.iter().take(remaining) {
            if event_count <= limits.event_count
                && event_bytes <= limits.event_bytes
                && expired_revision.is_none_or(|target| *revision > target)
            {
                break;
            }
            through = *revision;
            event_count = event_count
                .checked_sub(entry.count)
                .ok_or_else(|| invalid_contract("event retention count underflow"))?;
            event_bytes = event_bytes
                .checked_sub(entry.held_bytes)
                .ok_or_else(|| invalid_contract("event retention byte underflow"))?;
        }
        if requests.is_empty() && through == compacted_to {
            return Ok(None);
        }
        let delta = Delta {
            revision,
            selected_at_unix_ms: now,
            compacted_from: compacted_to,
            compacted_to: through,
            requests,
        };
        delta.validate()?;
        Ok(Some(delta))
    }
    pub fn apply(
        &self,
        delta: &Delta,
        revision: StoreRevision,
        compacted_to: StoreRevision,
    ) -> Result<Self> {
        delta.validate()?;
        if delta.revision != revision || delta.compacted_from != compacted_to {
            return Err(invalid_contract(
                "history cleanup does not follow current state",
            ));
        }
        for item in &delta.requests {
            if self
                .requests
                .get(&item.key)
                .is_none_or(|old| old.completion != item.completion)
            {
                return Err(invalid_contract(
                    "history cleanup request generation mismatch",
                ));
            }
        }
        if delta.compacted_to > compacted_to && !self.events.contains_key(&delta.compacted_to) {
            return Err(invalid_contract("history cleanup event boundary missing"));
        }
        let mut next = self.clone();
        for item in &delta.requests {
            let old = next
                .requests
                .remove(&item.key)
                .ok_or_else(|| invalid_contract("history cleanup missing request"))?;
            next.request_order
                .remove(&(old.completion.completed_at_unix_ms, item.key.clone()));
            next.request_bytes = next
                .request_bytes
                .checked_sub(old.held_bytes)
                .ok_or_else(|| invalid_contract("request history byte underflow"))?;
        }
        while let Some((revision, _)) = next.events.iter().next() {
            if *revision > delta.compacted_to {
                break;
            }
            let revision = *revision;
            let old = next
                .events
                .remove(&revision)
                .ok_or_else(|| invalid_contract("history cleanup missing event revision"))?;
            next.event_count = next
                .event_count
                .checked_sub(old.count)
                .ok_or_else(|| invalid_contract("event history count underflow"))?;
            next.event_bytes = next
                .event_bytes
                .checked_sub(old.held_bytes)
                .ok_or_else(|| invalid_contract("event history byte underflow"))?;
            if let Some(time) = old.completed_at {
                next.event_expiry.remove(&(time, revision));
            }
        }
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn authority() -> super::super::state::State {
        let mut state = super::super::state::State::default();
        for record in super::super::journal::test_records(3) {
            state = state.replay_record(&record).unwrap();
        }
        state
    }
    fn removal(state: &super::super::state::State) -> Delta {
        let key = RequestKey::new("log-test", "request-0");
        Delta {
            revision: state.revision,
            selected_at_unix_ms: 12346,
            compacted_from: StoreRevision::ZERO,
            compacted_to: StoreRevision(1),
            requests: vec![RemovedRequest {
                completion: state.request_completion.get(&key).unwrap().clone(),
                key,
            }],
        }
    }
    #[test]
    fn p3_history_state_checkpoint_does_not_resurrect_and_preserves_event_ordinal() {
        use super::super::{
            MetaRead, NodeSessionLease, TxnOutcome,
            log::{Record, StateImage},
        };
        let state = authority();
        let before = state.snapshot_bytes_with_batch(0).unwrap();
        let delta = removal(&state);
        let record = Record::History(delta.clone());
        let selected = state
            .replay_record(&record)
            .expect("durable decided cleanup must apply without a user operation");
        assert_eq!(selected.revision, StoreRevision(3));
        assert!(
            selected
                .read(MetaRead::RequestOutcome(delta.requests[0].key.clone()))
                .unwrap()
                .request_outcome
                .is_none()
        );
        assert_eq!(selected.requests.len(), 2);
        assert_eq!(selected.events.len(), 4);
        assert_eq!(selected.events_removed, 2);
        assert_eq!(selected.compacted_to, StoreRevision(1));
        assert_eq!(selected.entities.len(), state.entities.len());
        assert_eq!(
            state.snapshot_bytes_with_batch(0).unwrap(),
            before,
            "private replay cannot change old published state"
        );
        let bytes = StateImage::encode(&selected, 1024 * 1024).unwrap();
        let restored = StateImage::decode(&bytes, 1024 * 1024).unwrap();
        assert_eq!(
            restored.snapshot_bytes_with_batch(0).unwrap(),
            selected.snapshot_bytes_with_batch(0).unwrap()
        );
        assert_eq!(
            restored.event_completion.get(&StoreRevision(2)),
            Some(&12345)
        );
        assert!(restored.event_completion.get(&StoreRevision(1)).is_none());
        let (_, reply, new_record) = restored
            .prepare_node_record(
                RequestKey::new("log-test", "request-3"),
                NodeSessionLease {
                    node_id: "node".into(),
                    session_id: "new-session".into(),
                    grpc_addr: "g".into(),
                    data_addr: "d".into(),
                    rest_addr: "r".into(),
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(30),
                },
                12347,
                1024 * 1024,
            )
            .unwrap();
        assert!(matches!(
            reply,
            TxnOutcome::Committed {
                revision: StoreRevision(4),
                ..
            }
        ));
        assert_eq!(
            new_record.unwrap().facts().unwrap().events[0].event_index,
            6,
            "event index cannot be reset to retained length4"
        );
        assert!(
            restored.replay_record(&record).is_err(),
            "same cleanup cannot be applied twice"
        );
        let mut wrong = delta;
        wrong.requests[0].completion.completed_at_unix_ms += 1;
        assert!(state.replay_record(&Record::History(wrong)).is_err());
        assert_eq!(state.snapshot_bytes_with_batch(0).unwrap(), before);
    }
    #[test]
    fn p3_history_state_watch_compaction_uses_recovery_boundary_and_full_revision() {
        use super::super::{RecoveryReason, WatchBatch};
        let mut state = authority();
        // State fixture mirrors an already-decided complete prefix. No live
        // path may publish this without the canonical management record.
        state.compacted_to = StoreRevision(1);
        state.events_removed = 2;
        state.events = state
            .events
            .iter()
            .filter(|event| event.revision > StoreRevision(1))
            .cloned()
            .collect();
        state.event_completion.remove(&StoreRevision(1));
        match state.watch(StoreRevision::ZERO, 1).unwrap() {
            WatchBatch::Compacted {
                requested_after,
                compacted_to,
                recovery,
            } => {
                assert_eq!(requested_after, StoreRevision::ZERO);
                assert_eq!(compacted_to, StoreRevision(1));
                assert_eq!(recovery.resume_after, StoreRevision(3));
                assert_eq!(recovery.reason, RecoveryReason::WatchCompacted);
            }
            other => panic!("missing history must not report success: {other:?}"),
        }
        match state.watch(StoreRevision(1), 1).unwrap() {
            WatchBatch::Events {
                events,
                next_revision,
                ..
            } => {
                assert_eq!(events.len(), 2);
                assert!(events.iter().all(|e| e.revision == StoreRevision(2)));
                assert_eq!(next_revision, StoreRevision(3));
            }
            other => panic!("retained revision must be available: {other:?}"),
        }
    }
    #[tokio::test]
    async fn p3_history_state_log_management_advances_only_physical_sequence() {
        use super::super::{
            journal::{LogLimits, LogTip, MemoryLog, MetaLog},
            log::Record,
            state::State,
        };
        let log = MemoryLog::new(LogLimits::default()).unwrap();
        let records = super::super::journal::test_records(3);
        let tip = log.append_and_sync(LogTip::EMPTY, records).await.unwrap();
        let state = authority();
        let delta = removal(&state);
        let next = log
            .append_and_sync(tip, vec![Record::History(delta)])
            .await
            .unwrap();
        assert_eq!(next.sequence, 4);
        let recovered = log.recover().await.unwrap();
        let mut restored = State::default();
        for record in recovered.records {
            restored = restored
                .replay_record(&record)
                .expect("recovery must replay explicit decided cleanup");
        }
        assert_eq!(restored.revision, StoreRevision(3));
        assert_eq!(restored.requests.len(), 2);
        assert_eq!(restored.compacted_to, StoreRevision(1));
    }
    #[tokio::test]
    async fn p3_history_state_local_checkpoint_suffix_preserves_exact_cleanup() {
        use super::super::{
            NodeSessionLease,
            journal::{LogLimits, LogTip, MetaLog},
            local_log::LocalLog,
            log::{Record, StateImage},
            state::State,
        };
        let dir = tempfile::tempdir().unwrap();
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        let identity = log.identity();
        let records = super::super::journal::test_records(3);
        let tip = log.append_and_sync(LogTip::EMPTY, records).await.unwrap();
        let state = authority();
        let delta = removal(&state);
        let covered = log
            .append_and_sync(tip, vec![Record::History(delta.clone())])
            .await
            .unwrap();
        drop(log);
        // First reopen must replay the management record from the real WAL.
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        assert_eq!(log.identity(), identity);
        let recovered = log.recover().await.unwrap();
        assert!(recovered.checkpoint.is_none());
        let mut restored = State::default();
        for record in recovered.records {
            restored = restored.replay_record(&record).unwrap();
        }
        assert_eq!(restored.revision, StoreRevision(3));
        assert_eq!(restored.requests.len(), 2);
        assert!(!restored.requests.contains_key(&delta.requests[0].key));
        let image = StateImage::encode(&restored, 1024 * 1024).unwrap();
        log.install_checkpoint(covered, image).await.unwrap();
        let (expected, _, record) = restored
            .prepare_node_record(
                RequestKey::new("log-test", "request-3"),
                NodeSessionLease {
                    node_id: "node".into(),
                    session_id: "session-3".into(),
                    grpc_addr: "grpc".into(),
                    data_addr: "data".into(),
                    rest_addr: "rest".into(),
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(30),
                },
                12347,
                1024 * 1024,
            )
            .unwrap();
        let final_tip = log
            .append_and_sync(covered, vec![Record::from(record.unwrap())])
            .await
            .unwrap();
        drop(log);
        // Second reopen restores the cleared set from the image, then only the
        // suffix. It must not resurrect the old request or rebase event IDs.
        let log = LocalLog::open(dir.path(), LogLimits::default()).unwrap();
        assert_eq!(log.identity(), identity);
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.tip, final_tip);
        assert_eq!(final_tip.sequence, 5);
        assert_eq!(recovered.records.len(), 1);
        let image = recovered.checkpoint.unwrap();
        assert_eq!(image.covered, covered);
        let mut actual = StateImage::decode(&image.bytes, 1024 * 1024).unwrap();
        for record in recovered.records {
            actual = actual.replay_record(&record).unwrap();
        }
        assert_eq!(
            actual.snapshot_bytes_with_batch(0).unwrap(),
            expected.snapshot_bytes_with_batch(0).unwrap()
        );
        assert!(!actual.requests.contains_key(&delta.requests[0].key));
        assert_eq!(actual.compacted_to, StoreRevision(1));
        assert_eq!(actual.events_removed, 2);
        assert_eq!(actual.events.iter().last().unwrap().event_index, 7);
    }
    #[test]
    fn p3_history_state_associated_record_is_atomic_on_invalid_cleanup() {
        use super::super::{NodeSessionLease, log::Record};
        let state = authority();
        let before = state.snapshot_bytes_with_batch(0).unwrap();
        let (prepared, _, decision) = state
            .prepare_node_record(
                RequestKey::new("log-test", "request-3"),
                NodeSessionLease {
                    node_id: "node".into(),
                    session_id: "session-3".into(),
                    grpc_addr: "g".into(),
                    data_addr: "d".into(),
                    rest_addr: "r".into(),
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(30),
                },
                12347,
                1024 * 1024,
            )
            .unwrap();
        let delta = removal(&prepared);
        let decision = decision.unwrap();
        let expected = prepared.apply_history(&delta).unwrap();
        let record = Record::Decision {
            decision: decision.clone(),
            history: Some(delta.clone()),
        };
        assert_eq!(
            state
                .replay_record(&record)
                .unwrap()
                .snapshot_bytes_with_batch(0)
                .unwrap(),
            expected.snapshot_bytes_with_batch(0).unwrap()
        );
        let mut wrong = delta;
        wrong.requests[0].completion.revision = StoreRevision(2);
        assert!(
            state
                .replay_record(&Record::Decision {
                    decision,
                    history: Some(wrong)
                })
                .is_err()
        );
        assert_eq!(
            state.snapshot_bytes_with_batch(0).unwrap(),
            before,
            "failed associated cleanup cannot publish even its valid business prefix"
        );
    }
    #[test]
    fn p3_retention_request_charge_includes_owned_spare_capacity_and_nested_box() {
        use super::super::{NodeSession, OperationResult, RequestOutcome, StoreOperation};
        let mut address = String::with_capacity(64 * 1024);
        address.push('g');
        let devices = Vec::with_capacity(32);
        let minimum = address.capacity()
            + devices.capacity() * std::mem::size_of::<crate::dfs::StorageDeviceDescriptor>();
        let session = NodeSession {
            node_id: "node".into(),
            session_id: "session".into(),
            grpc_addr: address,
            data_addr: "d".into(),
            rest_addr: "r".into(),
            storage_devices: devices,
            lease_epoch: 1,
            expires_at_unix_ms: 100,
        };
        let result = OperationResult::DfsNamespace {
            request_digest: [1; 32],
            result: Box::new(OperationResult::NodeSession(session)),
        };
        let outcome = RequestOutcome {
            request: RequestKey::new("caller", "r"),
            operation: StoreOperation::RegisterNode,
            result,
        };
        let charge = request_charge(&outcome).unwrap();
        eprintln!(
            "P3_RETENTION_REQUEST capacity_min={} charge={} encoded={}",
            minimum,
            charge,
            super::super::codec::encoded_size(&outcome).unwrap()
        );
        assert!(
            charge >= minimum as u64,
            "owned history must not charge only encoded bytes: {charge} < {minimum}"
        );
    }
    #[test]
    fn p3_retention_event_charge_includes_key_and_owned_entity_capacity() {
        use super::super::{MetaEntity, MetaKey, RootRecord, WatchChange, WatchEvent};
        let mut payload = String::with_capacity(128 * 1024);
        payload.push('x');
        let minimum = payload.capacity();
        let entity = MetaEntity::Root(RootRecord {
            root_id: "root".into(),
            root_epoch: 1,
            home_node_id: "node".into(),
            home_session_id: "session".into(),
            local_prepare_id: payload,
            created_at_revision: StoreRevision(1),
            updated_at_revision: StoreRevision(1),
        });
        let event = WatchEvent {
            revision: StoreRevision(1),
            event_index: 0,
            key: MetaKey::Root {
                root_id: "root".into(),
            },
            change: WatchChange::Put(entity),
        };
        let charge = event_charge(&event).unwrap();
        eprintln!(
            "P3_RETENTION_EVENT capacity_min={} charge={} encoded={}",
            minimum,
            charge,
            super::super::codec::encoded_size(&event).unwrap()
        );
        assert!(
            charge >= minimum as u64,
            "a reference or encoded length is not retained entity bytes"
        );
    }
    fn completion(time: u64, revision: u64) -> RequestCompletion {
        RequestCompletion {
            completed_at_unix_ms: time,
            revision: StoreRevision(revision),
        }
    }
    fn limits() -> Limits {
        Limits {
            request_count: 2,
            request_bytes: 20,
            event_count: 3,
            event_bytes: 30,
            request_retention: Duration::from_millis(100),
            event_retention: Duration::from_millis(100),
            cleanup_items: 4,
            protected_requests: 2,
        }
    }
    fn seeded() -> Index {
        let mut index = Index::default();
        for n in 1..=3 {
            index
                .track_request(
                    RequestKey::new("a", format!("r{n}")),
                    completion(10 * n, n),
                    10,
                )
                .unwrap();
        }
        for n in 1..=3 {
            index
                .track_events(StoreRevision(n), Some(10 * n), 2, 20)
                .unwrap();
        }
        index
    }
    #[test]
    fn p3_history_capacity_keeps_whole_revisions_and_original_index_private() {
        let original = seeded();
        let delta = original
            .select(
                StoreRevision(3),
                StoreRevision::ZERO,
                31,
                limits(),
                &BTreeSet::new(),
            )
            .unwrap()
            .expect("capacity cleanup must be selected");
        assert_eq!(delta.requests.len(), 1);
        assert_eq!(delta.requests[0].key, RequestKey::new("a", "r1"));
        assert_eq!(delta.compacted_to, StoreRevision(2));
        assert_eq!(original.requests.len(), 3);
        assert_eq!(original.event_count, 6);
        let next = original
            .apply(&delta, StoreRevision(3), StoreRevision::ZERO)
            .unwrap();
        assert!(next.fits(limits()));
        assert_eq!(next.requests.len(), 2);
        assert_eq!(next.event_count, 2);
        assert!(next.events.contains_key(&StoreRevision(3)));
    }
    #[test]
    fn p3_history_time_byte_and_original_generation_are_independent() {
        let mut index = Index::default();
        let key = RequestKey::new("a", "replace");
        index
            .track_request(key.clone(), completion(1, 1), 18)
            .unwrap();
        index
            .track_request(key.clone(), completion(200, 2), 8)
            .unwrap();
        assert_eq!(index.request_bytes, 8);
        assert_eq!(index.request_order.len(), 1);
        assert!(
            index
                .select(
                    StoreRevision(2),
                    StoreRevision::ZERO,
                    201,
                    limits(),
                    &BTreeSet::new()
                )
                .unwrap()
                .is_none()
        );
        let delta = index
            .select(
                StoreRevision(2),
                StoreRevision::ZERO,
                300,
                limits(),
                &BTreeSet::new(),
            )
            .unwrap()
            .expect("time expiry must use replacement original completion");
        assert_eq!(delta.requests[0].completion, completion(200, 2));
        let mut changed = index.clone();
        changed.track_request(key, completion(301, 3), 8).unwrap();
        assert!(
            changed
                .apply(&delta, StoreRevision(2), StoreRevision::ZERO)
                .is_err()
        );
        assert_eq!(changed.request_bytes, 8);
        let mut bytes = limits();
        bytes.request_bytes = 7;
        assert!(
            index
                .select(
                    StoreRevision(2),
                    StoreRevision::ZERO,
                    201,
                    bytes,
                    &BTreeSet::new()
                )
                .unwrap()
                .is_some()
        );
    }
    #[test]
    fn p3_history_protected_and_partial_cleanup_stay_bounded() {
        let index = seeded();
        let mut cap = limits();
        cap.cleanup_items = 1;
        let protected = BTreeSet::from([RequestKey::new("a", "r1")]);
        let delta = index
            .select(StoreRevision(3), StoreRevision::ZERO, 31, cap, &protected)
            .unwrap()
            .expect("bounded cleanup must make progress");
        assert_eq!(delta.requests.len(), 1);
        assert_eq!(delta.requests[0].key, RequestKey::new("a", "r2"));
        assert_eq!(delta.compacted_to, StoreRevision::ZERO);
        let next = index
            .apply(&delta, StoreRevision(3), StoreRevision::ZERO)
            .unwrap();
        assert!(!next.fits(cap));
        assert!(next.requests.contains_key(&RequestKey::new("a", "r1")));
        assert_eq!(next.event_count, 6);
    }
    #[test]
    fn p3_history_unknown_time_not_reinvented_and_clock_reversal_is_a_prefix() {
        let mut index = Index::default();
        index.track_events(StoreRevision(1), None, 1, 10).unwrap();
        index
            .track_events(StoreRevision(2), Some(300), 1, 10)
            .unwrap();
        index
            .track_events(StoreRevision(3), Some(1), 1, 10)
            .unwrap();
        let delta = index
            .select(
                StoreRevision(3),
                StoreRevision::ZERO,
                101,
                limits(),
                &BTreeSet::new(),
            )
            .unwrap()
            .expect("expired later revision requires a complete prefix");
        assert_eq!(delta.compacted_to, StoreRevision(3));
        assert_eq!(
            index
                .apply(&delta, StoreRevision(3), StoreRevision::ZERO)
                .unwrap()
                .event_count,
            0
        );
        let mut unknown = Index::default();
        unknown.track_events(StoreRevision(1), None, 1, 10).unwrap();
        assert!(
            unknown
                .select(
                    StoreRevision(1),
                    StoreRevision::ZERO,
                    u64::MAX,
                    limits(),
                    &BTreeSet::new()
                )
                .unwrap()
                .is_none()
        );
    }
    #[test]
    fn p3_history_delta_is_exact_and_invalid_replay_cannot_mutate_index() {
        let index = seeded();
        let delta = index
            .select(
                StoreRevision(3),
                StoreRevision::ZERO,
                31,
                limits(),
                &BTreeSet::new(),
            )
            .unwrap()
            .unwrap();
        let bytes = super::super::codec::encode_bounded(&delta, 1024).unwrap();
        let (decoded, tail): (Delta, _) = postcard::take_from_bytes(&bytes).unwrap();
        assert!(tail.is_empty());
        assert_eq!(decoded, delta);
        decoded.validate().unwrap();
        assert!(
            index
                .apply(&delta, StoreRevision(4), StoreRevision::ZERO)
                .is_err()
        );
        let mut altered = delta.clone();
        altered.requests[0].completion.completed_at_unix_ms += 1;
        assert!(
            index
                .apply(&altered, StoreRevision(3), StoreRevision::ZERO)
                .is_err()
        );
        assert_eq!(index.requests.len(), 3);
        assert_eq!(index.event_bytes, 60);
        let next = index
            .apply(&delta, StoreRevision(3), StoreRevision::ZERO)
            .unwrap();
        assert!(
            next.apply(&delta, StoreRevision(3), StoreRevision(2))
                .is_err()
        );
        assert_eq!(next.event_count, 2);
    }
    #[test]
    fn p3_history_invalid_limits_and_excess_protection_do_not_bypass_caps() {
        let index = seeded();
        let mut cap = limits();
        cap.request_bytes = 0;
        assert!(
            index
                .select(
                    StoreRevision(3),
                    StoreRevision::ZERO,
                    31,
                    cap,
                    &BTreeSet::new()
                )
                .is_err()
        );
        let protected = BTreeSet::from([
            RequestKey::new("a", "r1"),
            RequestKey::new("a", "r2"),
            RequestKey::new("a", "r3"),
        ]);
        assert!(
            index
                .select(
                    StoreRevision(3),
                    StoreRevision::ZERO,
                    31,
                    limits(),
                    &protected
                )
                .is_err()
        );
        assert_eq!(index.requests.len(), 3);
        assert_eq!(index.request_bytes, 30);
        let mut all_protected = limits();
        all_protected.protected_requests = 3;
        all_protected.event_count = 10;
        all_protected.event_bytes = 100;
        assert!(
            index
                .select(
                    StoreRevision(3),
                    StoreRevision::ZERO,
                    31,
                    all_protected,
                    &protected
                )
                .unwrap()
                .is_none()
        );
        assert!(
            !index.fits(all_protected),
            "protected history must backpressure its owner, not silently fit"
        );
    }
}
