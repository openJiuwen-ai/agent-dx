//! DistributedFs POSIX backend for the first local R=1 vertical slice.
//!
//! Open handles are process-local identities. Mutable file contents live in an
//! inode-level dirty view shared by every handle in this mount for that inode. A
//! normal `write` changes that dirty view and the local visibility sequence;
//! explicit `fdatasync`/`fsync` turns the dirty view into immutable Chunk data
//! and asks Meta to publish a new FileVersion. Writable `flush` commits prior
//! writes before reporting close success; `release` only drops the handle.

mod read_pipeline;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    ffi::{OsStr, OsString},
    hash::{Hash, Hasher},
    os::unix::ffi::OsStringExt,
    sync::{
        Arc, Condvar, Mutex, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use afs_error::{Error, Result};
use afs_tracing::tracing;

mod pending_trace;
use pending_trace::PendingTrace;

use super::{
    Backend, PageCacheIdentity, WriteAdmissionMode,
    locks::{LockError, LockRequest, LockTable, LockWaiterId, LockWaiterOutcome},
    types::{
        AttributeChange, BackendInode, CreatedFile, DirectoryEntry, DirectoryHandle, Entry,
        FileAttributes, FileHandle, FileKind, FileLockConflict, FileLockKind, FileLockOwner,
        FileLockRange, FileLockType, FilesystemCapacity, OpenOptions, ReleaseKind, RenameFlags,
        RequestContext, SetAttrOptions, SpecialFileKind, SyncMode, WriteOptions,
    },
};
use crate::{
    dfs::{
        CallerContext, ChunkObject, ChunkReceipt, CommitFileVersion, CommitMetadataDelta,
        CommitMetadataMode, DentryRecord, DfsWriteSessionId, Extent, FileVersion, FileVersionId,
        GetXattrRequest, InodeAttrUpdate, InodeAttributes, InodeId, InodeKind, InodeRecord,
        LayoutRoot, LayoutRootId, ListXattrRequest, MknodRequest, NamespaceId, OperationId,
        ReadLinkRequest, RemoveXattrRequest, RenameMode, RenameOutcome, SetInodeAttrRequest,
        SetXattrRequest, SpecialNodeKind, SymlinkRequest, SyncInodeMetadata, WriteLease,
        XattrSetMode,
    },
    node::chunk::{
        ChunkBuilder, ChunkStore, NativeChunkReceipt, NativeWriteAdmission, NativeWriteBudget,
        NativeWriteOwner, SharedChunkBytes, StagedChunk, WriteBufferPool,
    },
    node::dfs_read::{
        BoundedLru, ChunkReadOp, CurrentExternalReadOutcome, DfsReadEngine,
        NativeReadAuthoritySnapshot, ReadBatch,
    },
    node::rpc::peer::RemoteDfsOwner,
};

pub const DFS_WRITE_LEASE_SECONDS: u64 = 30;
pub const DEFAULT_DIRTY_DATA_BUDGET_BYTES: u64 = 16 * 1024 * 1024;
const ROOT_INODE: u64 = 1;
const DEFAULT_VERSION_CACHE_ENTRIES: usize = 4096;
const DEFAULT_VERSION_CACHE_BYTES: usize = 32 * 1024 * 1024;
// How long a captured Node read-authority snapshot is reused before it is
// captured again. Renewal is local and never moves a handle's version.
const NATIVE_READ_VIEW_TTL: Duration = Duration::from_secs(5);
const COMMIT_CHUNK_BYTES: u64 = crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64;
const MAX_PENDING_REMOTE_RELEASES: usize = 1024;
const REMOTE_RELEASE_MAINTENANCE_BUDGET: std::time::Duration =
    std::time::Duration::from_millis(250);
const REMOTE_RELEASE_SHUTDOWN_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);
const REMOTE_PROVIDER_IDLE_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);
const REMOTE_RELEASE_RPC_BUDGET: std::time::Duration = std::time::Duration::from_millis(100);
const REMOTE_RELEASE_RETRY_MAX_OPS: usize = 64;
const LOCK_SESSION_REAP_BUDGET: std::time::Duration = std::time::Duration::from_millis(250);
const LOCK_SESSION_REAP_MAX_OPS: usize = 64;
const OWNER_HANDLE_REAP_BUDGET: std::time::Duration = std::time::Duration::from_millis(250);
const OWNER_HANDLE_REAP_MAX_PEERS: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsNodeLocation {
    pub node_id: String,
    pub node_epoch: u64,
    pub data_endpoint: String,
}

pub trait DfsRemoteOwnerFactory: Send + Sync {
    fn connect(&self, location: DfsNodeLocation) -> Result<Arc<dyn RemoteDfsOwner>>;
}

pub trait DfsMeta: Send + Sync {
    fn lookup(&self, parent: &InodeId, name: &[u8]) -> Result<Option<InodeRecord>>;
    fn create(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        attributes: InodeAttributes,
    ) -> Result<(InodeRecord, WriteLease)>;
    fn get_inode(&self, inode_id: &InodeId) -> Result<InodeRecord>;
    fn get_inode_async(
        &self,
        inode_id: InodeId,
    ) -> crate::node::read_pipeline::ReadFuture<'_, InodeRecord> {
        Box::pin(async move { self.get_inode(&inode_id) })
    }

    fn get_file_version(&self, version_id: &FileVersionId) -> Result<(FileVersion, LayoutRoot)>;
    fn get_file_version_async(
        &self,
        version_id: FileVersionId,
    ) -> crate::node::read_pipeline::ReadFuture<'_, (FileVersion, LayoutRoot)> {
        Box::pin(async move { self.get_file_version(&version_id) })
    }

    fn open_write(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)>;
    fn resolve_lock_authority(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        self.open_write(inode_id)
    }
    fn resolve_write_authority(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
        self.open_write(inode_id)
    }
    fn renew_write_lease(&self, lease: WriteLease) -> Result<WriteLease>;
    fn renew_write_lease_with_timeout(
        &self,
        lease: WriteLease,
        _timeout: Duration,
    ) -> Result<WriteLease> {
        self.renew_write_lease(lease)
    }
    fn sync_inode_metadata(&self, sync: SyncInodeMetadata) -> Result<InodeRecord>;
    fn sync_inode_metadata_with_timeout(
        &self,
        sync: SyncInodeMetadata,
        _timeout: Duration,
    ) -> Result<InodeRecord> {
        self.sync_inode_metadata(sync)
    }
    fn commit_file_version(&self, commit: CommitFileVersion) -> Result<InodeRecord>;
    fn commit_file_version_with_timeout(
        &self,
        commit: CommitFileVersion,
        _timeout: Duration,
    ) -> Result<InodeRecord> {
        self.commit_file_version(commit)
    }
    fn lookup_node_location(&self, node_id: &str) -> Result<Option<DfsNodeLocation>>;
    fn current_node_session(&self, node_id: &str) -> Result<Option<String>>;
    fn current_node_session_with_timeout(
        &self,
        node_id: &str,
        _timeout: Duration,
    ) -> Result<Option<String>> {
        self.current_node_session(node_id)
    }

    fn mkdir(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        attributes: InodeAttributes,
        caller: CallerContext,
    ) -> Result<InodeRecord>;
    fn read_dir(&self, parent: &InodeId) -> Result<Vec<DentryRecord>>;
    fn link(
        &self,
        operation_id: &OperationId,
        inode_id: &InodeId,
        expected_inode_revision: u64,
        parent: &InodeId,
        name: &[u8],
        caller: CallerContext,
    ) -> Result<InodeRecord>;
    fn symlink(&self, request: SymlinkRequest) -> Result<InodeRecord>;
    fn mknod(&self, request: MknodRequest) -> Result<InodeRecord>;
    fn read_link(&self, request: ReadLinkRequest) -> Result<Vec<u8>>;
    fn unlink(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        caller: CallerContext,
    ) -> Result<InodeRecord>;
    fn rmdir(
        &self,
        operation_id: &OperationId,
        parent: &InodeId,
        name: &[u8],
        caller: CallerContext,
    ) -> Result<InodeRecord>;
    #[allow(clippy::too_many_arguments)] // Mirrors the authenticated Meta rename contract.
    fn rename(
        &self,
        operation_id: &OperationId,
        old_parent: &InodeId,
        old_name: &[u8],
        new_parent: &InodeId,
        new_name: &[u8],
        mode: RenameMode,
        caller: CallerContext,
    ) -> Result<RenameOutcome>;

    fn set_inode_attributes(&self, request: SetInodeAttrRequest) -> Result<InodeRecord>;
    fn get_xattr(&self, request: GetXattrRequest) -> Result<Vec<u8>>;
    fn list_xattr(&self, request: ListXattrRequest) -> Result<Vec<Vec<u8>>>;
    fn set_xattr(&self, request: SetXattrRequest) -> Result<InodeRecord>;
    fn remove_xattr(&self, request: RemoveXattrRequest) -> Result<InodeRecord>;
}

type SharedInodeWriteState = Arc<InodeWriteStateCell>;

struct InodeWriteStateCell {
    state: Mutex<InodeWriteState>,
    cv: Condvar,
    lock_wait: Option<Arc<InodeLockWait>>,
}

/// Structural-cost counters for inode write-state lock contention. Only an
/// acquisition that has to wait is timed; an uncontended one costs a `try_lock`.
struct InodeLockWait {
    contended: afs_metrics::IntCounter,
    wait_nanoseconds: afs_metrics::IntCounter,
}

impl InodeLockWait {
    fn register(
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        let contended = afs_metrics::IntCounter::new(
            "afs_dfs_inode_lock_contended_total",
            "Inode write-state lock acquisitions that had to wait for another holder.",
        )?;
        let wait_nanoseconds = afs_metrics::IntCounter::new(
            "afs_dfs_inode_lock_wait_nanoseconds_total",
            "Time spent waiting in contended inode write-state lock acquisitions.",
        )?;
        afs_metrics::register_collector(registry, &contended)?;
        afs_metrics::register_collector(registry, &wait_nanoseconds)?;
        Ok(Self {
            contended,
            wait_nanoseconds,
        })
    }

    fn record(&self, waited: Duration) {
        self.contended.inc();
        self.wait_nanoseconds
            .inc_by(u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX));
    }
}

pub struct DistributedFs {
    inode_lock_wait: Option<Arc<InodeLockWait>>,
    namespace_id: NamespaceId,
    node_id: String,
    session_id: String,
    pending_trace: Option<PendingTrace>,
    meta: Arc<dyn DfsMeta>,
    chunk_store: Arc<dyn ChunkStore>,
    read_engine: Arc<DfsReadEngine>,
    remote_owner_factory: Option<Arc<dyn DfsRemoteOwnerFactory>>,
    handles: DfsHandleTable,
    gc_references: Option<Arc<crate::node::gc::GcReferences>>,
    gc_gate: Mutex<()>,
    dir_handles: Mutex<HashMap<u64, DfsDirectoryHandle>>,
    inode_writes: ShardedMap<InodeId, SharedInodeWriteState>,
    remote_inode_providers: ShardedMap<InodeId, RemoteDfsWriteSession>,
    pending_remote_releases: Mutex<PendingRemoteReleaseState>,
    lock_authorities: ShardedMap<InodeId, Arc<DfsLockAuthority>>,
    lock_authority_admission: Mutex<()>,
    lock_renewal: Arc<DfsLockRenewal>,
    remote_lock_authorities: Mutex<HashMap<InodeId, DfsRemoteLockAuthority>>,
    remote_lock_authority_sessions: Mutex<HashMap<InodeId, HashMap<String, usize>>>,
    remote_lock_waiters: Mutex<HashMap<LockWaiterId, DfsRemoteLockAuthority>>,
    cancelled_remote_lock_waiters: Mutex<HashSet<LockWaiterId>>,
    owner_open_routes: Mutex<HashMap<DfsOwnerOpenRouteKey, DfsOwnerOpenRouteState>>,
    remote_open_admission_locks:
        Mutex<HashMap<DfsOwnerOpenRouteKey, Arc<DfsRemoteOpenAdmissionLock>>>,
    #[cfg(test)]
    owner_open_finish_pause: Mutex<Option<Arc<OwnerOpenFinishPause>>>,
    #[cfg(test)]
    native_read_before_delivery: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    native_read_after_committed: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    backend_inode_after_missing: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    handle_registration_after_route_check: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    readonly_version_before_install: Mutex<Option<Arc<std::sync::Barrier>>>,
    lock_session_reap_cursor: AtomicU64,
    owner_handle_reap_cursor: AtomicU64,
    write_lease_cursor: AtomicU64,
    #[cfg(test)]
    writeback_after_prepare_pause: Mutex<Option<Duration>>,
    closed_lock_sessions: Mutex<HashSet<String>>,
    closed_lock_session_admission_closed: AtomicBool,
    remote_operation_results: Mutex<HashMap<RemoteOperationKey, RemoteOperationResult>>,
    readonly_version_cache: Arc<Mutex<ReadOnlyVersionCache>>,
    readonly_version_gates: Arc<crate::node::read_pipeline::ReadKeyGates>,
    readonly_empty_version: Arc<ReadOnlyVersion>,
    write_buffer_pool: Arc<WriteBufferPool>,
    native_pipeline: Option<Arc<NativeWritePipeline>>,
    inode_to_backend: ShardedMap<InodeId, u64>,
    backend_to_inode: ShardedMap<u64, InodeId>,
    inode_mapping_creation: Mutex<()>,
    next_inode: AtomicU64,
    next_handle: AtomicU64,
    next_operation: AtomicU64,
    next_owner_open_seq: AtomicU64,
}

/// Immutable metadata only. An Arc never grants authority or extends the
/// separately captured inode/session view deadline.
struct ReadOnlyVersion {
    version: Option<FileVersion>,
    layout: LayoutRoot,
    index: LayoutReadIndex,
}

struct CachedReadOnlyVersion {
    value: Arc<ReadOnlyVersion>,
    retained_bytes: usize,
}

/// Bounds this cache's retained metadata estimate, not total Node RSS. Evicted
/// Arc values stay live until their active readers release them. Cache-owned
/// containers are charged conservatively at their configured maximum, so a
/// HashMap's retained capacity after eviction cannot escape the byte budget.
struct ReadOnlyVersionCache {
    entries: BoundedLru<FileVersionId, CachedReadOnlyVersion>,
    capacity: usize,
    byte_limit: usize,
    table_bytes: usize,
    value_bytes: usize,
    entry_count: usize,
}

fn version_cache_table_bytes(capacity: usize) -> Option<usize> {
    // Preserve the runtime estimate: permit hash-table growth/control storage
    // and sparsely occupied BTree nodes; key buffers are charged per value.
    let table_slot = std::mem::size_of::<(FileVersionId, CachedReadOnlyVersion, u64)>()
        .checked_add(std::mem::size_of::<(u64, FileVersionId)>())?
        .checked_mul(4)?
        .checked_add(64)?;
    capacity.checked_mul(table_slot)?.checked_add(2048)
}

/// Startup floor for the table and one value's fixed fields/Arc header. This
/// makes no assumption about identifier lengths or layout cardinality: actual
/// String/Vec capacities are charged on admission and can still exceed it.
pub(crate) fn version_cache_minimum_bytes(capacity: usize) -> Option<usize> {
    version_cache_table_bytes(capacity)?.checked_add(ReadOnlyVersion::fixed_retained_bytes())
}

impl ReadOnlyVersionCache {
    fn new(capacity: usize, byte_limit: usize) -> Self {
        let table_bytes = version_cache_table_bytes(capacity).unwrap_or(usize::MAX);
        Self {
            entries: BoundedLru::new(capacity),
            capacity,
            byte_limit,
            table_bytes,
            value_bytes: 0,
            entry_count: 0,
        }
    }

    fn get(&mut self, key: &FileVersionId) -> Option<&Arc<ReadOnlyVersion>> {
        self.entries.get(key).map(|entry| &entry.value)
    }

    fn insert(&mut self, key: FileVersionId, value: Arc<ReadOnlyVersion>) {
        if let Some(previous) = self.entries.remove(&key) {
            self.value_bytes = self.value_bytes.saturating_sub(previous.retained_bytes);
            self.entry_count -= 1;
        }
        let retained_bytes = value.cache_retained_bytes(&key);
        if self.capacity == 0 || self.table_bytes.saturating_add(retained_bytes) > self.byte_limit {
            if self.entry_count == 0 {
                // A rejected replacement must also release any old empty table
                // allocation; zero retained entries otherwise have zero charge.
                self.entries = BoundedLru::new(self.capacity);
            }
            return;
        }
        while self.entry_count >= self.capacity
            || self
                .table_bytes
                .saturating_add(self.value_bytes)
                .saturating_add(retained_bytes)
                > self.byte_limit
        {
            let Some((_, evicted)) = self.entries.pop_oldest() else {
                return;
            };
            self.value_bytes = self.value_bytes.saturating_sub(evicted.retained_bytes);
            self.entry_count -= 1;
        }
        self.entries.insert(
            key,
            CachedReadOnlyVersion {
                value,
                retained_bytes,
            },
        );
        self.value_bytes = self.value_bytes.saturating_add(retained_bytes);
        self.entry_count += 1;
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entry_count
    }
}

impl ReadOnlyVersion {
    fn fixed_retained_bytes() -> usize {
        std::mem::size_of::<Self>().saturating_add(2 * std::mem::size_of::<usize>())
    }

    fn cache_retained_bytes(&self, key: &FileVersionId) -> usize {
        // The hash table and recency tree each own a key. Counting the source
        // capacity twice conservatively covers the cloned key's allocation.
        // Heap allocations also include a two-word bookkeeping allowance;
        // this remains a portable retained-size estimate, not allocator RSS.
        let heap = |capacity: usize| {
            if capacity == 0 {
                0
            } else {
                capacity.saturating_add(2 * std::mem::size_of::<usize>())
            }
        };
        let mut bytes = Self::fixed_retained_bytes()
            .saturating_add(heap(key.0.capacity()).saturating_mul(2))
            .saturating_add(heap(self.layout.id.0.capacity()))
            .saturating_add(heap(
                self.layout
                    .inline_extents
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Extent>()),
            ))
            .saturating_add(heap(
                self.index
                    .entries
                    .capacity()
                    .saturating_mul(std::mem::size_of::<IndexedExtent>()),
            ));
        if let Some(version) = &self.version {
            for capacity in [
                version.id.0.capacity(),
                version.inode_id.0.capacity(),
                version
                    .parent_version
                    .as_ref()
                    .map_or(0, |id| id.0.capacity()),
                version.layout_root.0.capacity(),
            ] {
                bytes = bytes.saturating_add(heap(capacity));
            }
        }
        for extent in &self.layout.inline_extents {
            bytes = bytes.saturating_add(heap(extent.chunk_id.0.capacity()));
        }
        bytes
    }

    fn empty() -> Self {
        Self {
            version: None,
            layout: LayoutRoot {
                id: LayoutRootId::new("empty"),
                file_length: 0,
                inline_extents: Vec::new(),
            },
            index: LayoutReadIndex {
                entries: Vec::new(),
                serialization_ordered: true,
            },
        }
    }

    fn checked(expected: &FileVersionId, version: FileVersion, layout: LayoutRoot) -> Result<Self> {
        if version.id != *expected
            || version.layout_root != layout.id
            || version.length != layout.file_length
        {
            return Err(invalid(
                "DFS readonly version/layout identity or length differs",
            ));
        }
        let index = LayoutReadIndex::new(&layout)?;
        Ok(Self {
            version: Some(version),
            layout,
            index,
        })
    }

    fn ops(&self, offset: u64, length: usize) -> Result<Vec<ChunkReadOp>> {
        self.index.ops(&self.layout, offset, length)
    }

    fn native_read_batch(&self, offset: u64, length: usize, lookahead: usize) -> Result<ReadBatch> {
        self.read_batch_with_source_window(offset, length, lookahead, true)
    }

    fn read_batch(&self, offset: u64, length: usize, lookahead: usize) -> Result<ReadBatch> {
        self.read_batch_with_source_window(offset, length, lookahead, false)
    }

    fn read_batch_with_source_window(
        &self,
        offset: u64,
        length: usize,
        lookahead: usize,
        native: bool,
    ) -> Result<ReadBatch> {
        let ops = self.ops(offset, length)?;
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| invalid("read range overflow"))?;
        let source_window = if native && ops.len() == 1 && lookahead > 0 {
            // Use the existing advisory quota in a stable immutable-layout
            // window. A later concurrent offset must not omit earlier peers
            // and force an extra lookup. This never changes body operations.
            let width = lookahead
                .saturating_add(1)
                .min(crate::node::dfs_read::MAX_DFS_READ_OPS);
            let first = self
                .index
                .entries
                .partition_point(|entry| entry.max_end <= offset);
            let start = (first / width) * width;
            start..start.saturating_add(width).min(self.index.entries.len())
        } else {
            let next = self.index.entries.partition_point(|entry| {
                self.layout.inline_extents[entry.original].file_offset < end
            });
            next..self.index.entries.len()
        };
        Ok(ReadBatch {
            lookahead: source_lookahead_ids(
                self.index.entries[source_window]
                    .iter()
                    .map(|entry| &self.layout.inline_extents[entry.original]),
                &ops,
                lookahead,
            ),
            file_version_id: self.version.as_ref().map(|version| version.id.clone()),
            layout_root_id: self.layout.id.clone(),
            ops,
        })
    }
}

/// Metadata only. These IDs never enter body operations or expand ranges.
fn source_lookahead_ids<'a>(
    extents: impl Iterator<Item = &'a Extent>,
    ops: &[ChunkReadOp],
    limit: usize,
) -> Vec<crate::dfs::ChunkId> {
    let mut ids = Vec::new();
    let limit = limit.min(crate::node::dfs_read::MAX_DFS_READ_OPS);
    if limit == 0 || ops.is_empty() {
        return ids;
    }
    for extent in extents {
        if extent.length == 0
            || extent.chunk_id.0.is_empty()
            || extent.chunk_id.0.len() > crate::node::dfs_read::MAX_SOURCE_LOOKAHEAD_ID_BYTES
            || ops.iter().any(|op| op.chunk_id == extent.chunk_id)
            || ids.contains(&extent.chunk_id)
        {
            continue;
        }
        ids.push(extent.chunk_id.clone());
        if ids.len() == limit {
            break;
        }
    }
    ids
}

struct IndexedExtent {
    original: usize,
    max_end: u64,
}

/// The prefix maximum handles overlapping/unsorted legacy layouts without
/// dropping an extent that starts before the request. Read operations retain
/// original serialization order, including its overlap precedence.
struct LayoutReadIndex {
    entries: Vec<IndexedExtent>,
    serialization_ordered: bool,
}

impl LayoutReadIndex {
    fn new(layout: &LayoutRoot) -> Result<Self> {
        let mut entries = layout
            .inline_extents
            .iter()
            .enumerate()
            .map(|(original, extent)| {
                let max_end = extent
                    .file_offset
                    .checked_add(extent.length)
                    .ok_or_else(|| invalid("extent range overflow"))?;
                Ok(IndexedExtent { original, max_end })
            })
            .collect::<Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| {
            (
                layout.inline_extents[entry.original].file_offset,
                entry.original,
            )
        });
        let mut max_end = 0;
        for entry in &mut entries {
            max_end = max_end.max(entry.max_end);
            entry.max_end = max_end;
        }
        let serialization_ordered = entries
            .windows(2)
            .all(|pair| pair[0].original < pair[1].original);
        Ok(Self {
            entries,
            serialization_ordered,
        })
    }

    fn ops(&self, layout: &LayoutRoot, offset: u64, length: usize) -> Result<Vec<ChunkReadOp>> {
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| invalid("read range overflow"))?;
        let first = self
            .entries
            .partition_point(|entry| entry.max_end <= offset);
        let last = self
            .entries
            .partition_point(|entry| layout.inline_extents[entry.original].file_offset < end);
        if first >= last {
            return Ok(Vec::new());
        }
        let mut ops = Vec::new();
        let mut append = |original: usize| -> Result<()> {
            if let Some(op) = extent_read_op(&layout.inline_extents[original], offset, end)? {
                ops.push(op);
            }
            Ok(())
        };
        if self.serialization_ordered {
            // Canonical layouts need neither a candidate allocation nor a
            // per-read sort. Legacy unordered layouts retain exact precedence.
            for entry in &self.entries[first..last] {
                append(entry.original)?;
            }
        } else {
            let mut candidates = self.entries[first..last]
                .iter()
                .map(|entry| entry.original)
                .collect::<Vec<_>>();
            candidates.sort_unstable();
            for original in candidates {
                append(original)?;
            }
        }
        Ok(ops)
    }
}

fn extent_read_op(extent: &Extent, offset: u64, end: u64) -> Result<Option<ChunkReadOp>> {
    let extent_end = extent
        .file_offset
        .checked_add(extent.length)
        .ok_or_else(|| invalid("extent range overflow"))?;
    let start = offset.max(extent.file_offset);
    let stop = end.min(extent_end);
    if start >= stop {
        return Ok(None);
    }
    let output_offset =
        usize::try_from(start - offset).map_err(|_| invalid("read output offset is too large"))?;
    let length = usize::try_from(stop - start).map_err(|_| invalid("read length is too large"))?;
    let chunk_offset = extent
        .chunk_offset
        .checked_add(start - extent.file_offset)
        .ok_or_else(|| invalid("chunk read offset overflow"))?;
    Ok(Some(ChunkReadOp {
        chunk_id: extent.chunk_id.clone(),
        chunk_offset,
        length: length as u64,
        output_offset,
    }))
}

#[derive(Clone)]
struct NativeReadView {
    generation: u64,
    authority: NativeReadAuthoritySnapshot,
    deadline: Instant,
}

struct DfsFileHandle {
    gc_pin: Option<Arc<crate::node::gc::GcPin>>,
    inode_id: InodeId,
    opened_inode: InodeRecord,
    opened_page_identity: Option<PageCacheIdentity>,
    native_read_view: Option<NativeReadView>,
    native_read_generation: u64,
    native_denial_serial: u64,
    write_session: Option<DfsRemoteWriteSession>,
    flags: i32,
    owner_scope: Option<DfsOwnerHandleScope>,
}

struct DfsHandle {
    native_prefetch: std::sync::OnceLock<Arc<crate::node::native_prefetch::Controller>>,
    _gc_pin: Option<Arc<crate::node::gc::GcPin>>,
    inode_id: InodeId,
    flags: i32,
    owner_scope: Option<DfsOwnerHandleScope>,
    state: Mutex<HandleMut>,
    closed: AtomicBool,
    // Set once this Node had write activity or a proxy provider on the inode
    // while the handle was open. Reads then check that state; with no
    // activity left they fall back to the handle's pinned version.
    dynamic_read_route: AtomicBool,
}

struct HandleMut {
    opened_inode: Arc<InodeRecord>,
    opened_page_identity: Option<PageCacheIdentity>,
    native_read_view: Option<NativeReadView>,
    native_read_generation: u64,
    native_denial_serial: u64,
    // Accepted/synced sequences and the observed error cursor change after open.
    write_session: Option<DfsRemoteWriteSession>,
    read_sequence: crate::node::dfs_fetch::ReadSequence,
}

type PinnedReadView = Result<(Arc<InodeRecord>, NativeReadView)>;

impl From<DfsFileHandle> for DfsHandle {
    fn from(opened: DfsFileHandle) -> Self {
        Self {
            native_prefetch: std::sync::OnceLock::new(),
            _gc_pin: opened.gc_pin,
            inode_id: opened.inode_id,
            flags: opened.flags,
            owner_scope: opened.owner_scope,
            state: Mutex::new(HandleMut {
                opened_inode: Arc::new(opened.opened_inode),
                opened_page_identity: opened.opened_page_identity,
                native_read_view: opened.native_read_view,
                native_read_generation: opened.native_read_generation,
                native_denial_serial: opened.native_denial_serial,
                write_session: opened.write_session,
                read_sequence: crate::node::dfs_fetch::ReadSequence::default(),
            }),
            closed: AtomicBool::new(false),
            dynamic_read_route: AtomicBool::new(false),
        }
    }
}

const DFS_HANDLE_SHARDS: usize = 64;

#[cfg(test)]
std::thread_local! {
    static DFS_METADATA_TABLE_LOCKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn record_metadata_table_lock() {
    #[cfg(test)]
    DFS_METADATA_TABLE_LOCKS.with(|count| count.set(count.get() + 1));
}

/// Shared metadata utility; guards never cross RPCs or inode state waits.
struct ShardedMap<K, V> {
    shards: [RwLock<HashMap<K, V>>; DFS_HANDLE_SHARDS],
}

impl<K: Eq + Hash, V> Default for ShardedMap<K, V> {
    fn default() -> Self {
        Self::from_entries(std::iter::empty())
    }
}

impl<K: Eq + Hash, V> ShardedMap<K, V> {
    fn shard(key: &K) -> usize {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hash);
        hash.finish() as usize % DFS_HANDLE_SHARDS
    }

    fn from_entries(entries: impl IntoIterator<Item = (K, V)>) -> Self {
        let mut maps = std::array::from_fn::<_, DFS_HANDLE_SHARDS, _>(|_| HashMap::new());
        for (key, value) in entries {
            maps[Self::shard(&key)].insert(key, value);
        }
        Self {
            shards: maps.map(RwLock::new),
        }
    }

    fn read(
        &self,
        key: &K,
    ) -> std::sync::LockResult<std::sync::RwLockReadGuard<'_, HashMap<K, V>>> {
        record_metadata_table_lock();
        self.shards[Self::shard(key)].read()
    }

    fn write(
        &self,
        key: &K,
    ) -> std::sync::LockResult<std::sync::RwLockWriteGuard<'_, HashMap<K, V>>> {
        record_metadata_table_lock();
        self.shards[Self::shard(key)].write()
    }

    fn snapshot(&self) -> Result<Vec<(K, V)>>
    where
        K: Clone,
        V: Clone,
    {
        let mut entries = Vec::new();
        for shard in &self.shards {
            record_metadata_table_lock();
            let shard = shard
                .read()
                .map_err(|_| unavailable("DFS metadata shard is poisoned"))?;
            entries.extend(
                shard
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
        }
        Ok(entries)
    }

    fn len(&self) -> Result<usize> {
        let mut count = 0;
        for shard in &self.shards {
            count += shard
                .read()
                .map_err(|_| unavailable("DFS metadata shard is poisoned"))?
                .len();
        }
        Ok(count)
    }

    fn insert(&self, key: K, value: V) -> Result<Option<V>> {
        Ok(self
            .write(&key)
            .map_err(|_| unavailable("DFS metadata shard is poisoned"))?
            .insert(key, value))
    }

    fn values_snapshot(&self) -> Result<Vec<V>>
    where
        K: Clone,
        V: Clone,
    {
        Ok(self
            .snapshot()?
            .into_iter()
            .map(|(_, value)| value)
            .collect())
    }

    fn keys_snapshot(&self) -> Result<Vec<K>>
    where
        K: Clone,
        V: Clone,
    {
        Ok(self.snapshot()?.into_iter().map(|(key, _)| key).collect())
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.len().unwrap() == 0
    }
}
type HandleShard = RwLock<HashMap<u64, Arc<DfsHandle>>>;
type InodeHandleIndex = Mutex<HashMap<InodeId, HashMap<u64, Weak<DfsHandle>>>>;

/// Structural locks protect Arc registration only. Registration/removal takes
/// the inode index before the handle shard; callers drop both before HandleMut,
/// inode write state, or RPCs. No HandleMut guard may cross an RPC or acquire an
/// inode write state guard. Lifecycle snapshots contain metadata owners, not data.
struct DfsHandleTable {
    shards: [HandleShard; DFS_HANDLE_SHARDS],
    by_inode: [InodeHandleIndex; DFS_HANDLE_SHARDS],
}

impl Default for DfsHandleTable {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            by_inode: std::array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }
}

impl DfsHandleTable {
    fn inode_shard(inode_id: &InodeId) -> usize {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        inode_id.hash(&mut hash);
        (hash.finish() as usize) % DFS_HANDLE_SHARDS
    }

    fn get(&self, id: u64) -> Result<Option<Arc<DfsHandle>>> {
        record_metadata_table_lock();
        Ok(self.shards[id as usize % DFS_HANDLE_SHARDS]
            .read()
            .map_err(|_| unavailable("DFS handle shard is poisoned"))?
            .get(&id)
            .cloned())
    }

    fn insert(
        &self,
        id: u64,
        opened: DfsFileHandle,
        dynamic: impl FnOnce() -> Result<bool>,
    ) -> Result<()> {
        let handle = Arc::new(DfsHandle::from(opened));
        let mut index = self.by_inode[Self::inode_shard(&handle.inode_id)]
            .lock()
            .map_err(|_| unavailable("DFS inode handle index is poisoned"))?;
        handle
            .dynamic_read_route
            .store(dynamic()?, Ordering::Release);
        let mut shard = self.shards[id as usize % DFS_HANDLE_SHARDS]
            .write()
            .map_err(|_| unavailable("DFS handle shard is poisoned"))?;
        if shard.contains_key(&id) {
            return Err(unavailable("DFS file handle identity exhausted"));
        }
        index
            .entry(handle.inode_id.clone())
            .or_default()
            .insert(id, Arc::downgrade(&handle));
        shard.insert(id, handle);
        Ok(())
    }

    fn remove(&self, id: u64) -> Result<Option<Arc<DfsHandle>>> {
        let Some(handle) = self.get(id)? else {
            return Ok(None);
        };
        let mut index = self.by_inode[Self::inode_shard(&handle.inode_id)]
            .lock()
            .map_err(|_| unavailable("DFS inode handle index is poisoned"))?;
        let removed = self.shards[id as usize % DFS_HANDLE_SHARDS]
            .write()
            .map_err(|_| unavailable("DFS handle shard is poisoned"))?
            .remove(&id);
        if let Some(handle) = &removed {
            handle.closed.store(true, Ordering::Release);
            if let Some(prefetch) = handle.native_prefetch.get() {
                prefetch.retire();
            }
        }
        if let Some(handles) = index.get_mut(&handle.inode_id) {
            handles.remove(&id);
            if handles.is_empty() {
                index.remove(&handle.inode_id);
            }
        }
        Ok(removed)
    }

    fn for_inode(&self, inode_id: &InodeId) -> Result<Vec<Arc<DfsHandle>>> {
        let index = self.by_inode[Self::inode_shard(inode_id)]
            .lock()
            .map_err(|_| unavailable("DFS inode handle index is poisoned"))?;
        Ok(index
            .get(inode_id)
            .into_iter()
            .flat_map(|handles| handles.values())
            .filter_map(Weak::upgrade)
            .collect())
    }

    /// Move every open handle on the inode to `inode`, this Node's newest view
    /// of it. Only this Node's own write activity calls this; other nodes'
    /// commits reach a handle only through a new open (decision 4a).
    fn advance_to_local_view(&self, inode: &InodeRecord) -> Result<()> {
        let opened = Arc::new(inode.clone());
        for handle in self.for_inode(&inode.inode_id)? {
            handle.dynamic_read_route.store(true, Ordering::Release);
            let mut handle = handle
                .state
                .lock()
                .map_err(|_| unavailable("DFS handle state is poisoned"))?;
            handle.native_read_generation = handle
                .native_read_generation
                .checked_add(1)
                .ok_or_else(|| unavailable("DFS native read generation exhausted"))?;
            handle.native_read_view = None;
            handle.opened_page_identity = None;
            handle.opened_inode = opened.clone();
        }
        Ok(())
    }

    fn mark_dynamic(&self, inode_id: &InodeId) -> Result<()> {
        // Allocate holds the same index lock while checking the published
        // maps. Publishers must release their map guard before entering here.
        let index = self.by_inode[Self::inode_shard(inode_id)]
            .lock()
            .map_err(|_| unavailable("DFS inode handle index is poisoned"))?;
        if let Some(handles) = index.get(inode_id) {
            for handle in handles.values().filter_map(Weak::upgrade) {
                handle.dynamic_read_route.store(true, Ordering::Release);
            }
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<Vec<(u64, Arc<DfsHandle>)>> {
        let mut entries = Vec::new();
        for shard in &self.shards {
            let shard = shard
                .read()
                .map_err(|_| unavailable("DFS handle shard is poisoned"))?;
            entries.extend(shard.iter().map(|(id, handle)| (*id, handle.clone())));
        }
        Ok(entries)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read().unwrap().len())
            .sum()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    fn clear(&self) {
        for (id, _) in self.snapshot().unwrap() {
            self.remove(id).unwrap();
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DfsOwnerHandleScope {
    caller_node_id: String,
    caller_session_id: String,
    lease_epoch: u64,
    open_seq: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DfsOwnerOpenRouteKey {
    caller_node_id: String,
    caller_session_id: String,
    owner_node_id: String,
    owner_session_id: String,
}

struct DfsRemoteOpenAdmissionLock {
    gate: Mutex<()>,
    users: AtomicUsize,
}

#[derive(Clone)]
struct DfsOwnerOpenAdmission {
    fingerprint: [u8; 32],
    handle: afs_protocol::node_control::DfsOwnerHandle,
}

struct DfsOwnerOpenInflight {
    fingerprint: [u8; 32],
    cleanup_scope: afs_protocol::node_control::DfsOwnerHandle,
}

#[derive(Default)]
struct DfsOwnerOpenRouteState {
    highwater: u64,
    active: HashMap<u64, DfsOwnerOpenAdmission>,
    inflight: HashMap<u64, DfsOwnerOpenInflight>,
    cancelled: HashSet<u64>,
}

#[cfg(test)]
struct OwnerOpenFinishPause {
    reached: (Mutex<bool>, Condvar),
    release: (Mutex<bool>, Condvar),
}

#[cfg(test)]
impl OwnerOpenFinishPause {
    fn new() -> Self {
        Self {
            reached: (Mutex::new(false), Condvar::new()),
            release: (Mutex::new(false), Condvar::new()),
        }
    }

    fn pause(&self) {
        let (reached_lock, reached_cv) = &self.reached;
        *reached_lock.lock().unwrap() = true;
        reached_cv.notify_all();
        let (release_lock, release_cv) = &self.release;
        let mut released = release_lock.lock().unwrap();
        while !*released {
            released = release_cv.wait(released).unwrap();
        }
    }

    fn wait_until_reached(&self) {
        let (lock, cv) = &self.reached;
        let mut reached = lock.lock().unwrap();
        while !*reached {
            reached = cv.wait(reached).unwrap();
        }
    }

    fn resume(&self) {
        let (lock, cv) = &self.release;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
}

struct DfsDirectoryHandle {
    _gc_pin: Option<Arc<crate::node::gc::GcPin>>,
    entries: Vec<DirectoryEntry>,
}

struct DfsLockAuthority {
    table: LockTable,
    state: Mutex<DfsLockAuthorityState>,
    renewal: Mutex<()>,
}

struct DfsLockAuthorityState {
    lease: WriteLease,
    pinned_owners: HashSet<FileLockOwner>,
    waiters: HashSet<LockWaiterId>,
    last_error: Option<Error>,
}

struct DfsLockRenewal {
    authorities: Arc<Mutex<HashMap<InodeId, Arc<DfsLockAuthority>>>>,
    running: Arc<AtomicBool>,
}

#[derive(Clone)]
struct DfsRemoteLockAuthority {
    owner: Arc<dyn RemoteDfsOwner>,
    authority: afs_protocol::node_control::DfsOwnerLockAuthority,
}

enum DfsLockTarget {
    Local(Arc<DfsLockAuthority>),
    Remote(DfsRemoteLockAuthority),
}

impl DfsLockAuthority {
    fn new(lease: WriteLease) -> Self {
        Self {
            table: LockTable::default(),
            state: Mutex::new(DfsLockAuthorityState {
                lease,
                pinned_owners: HashSet::new(),
                waiters: HashSet::new(),
                last_error: None,
            }),
            renewal: Mutex::new(()),
        }
    }
}

fn same_lease_identity(left: &WriteLease, right: &WriteLease) -> bool {
    left.inode_id == right.inode_id
        && left.owner_node_id == right.owner_node_id
        && left.owner_session_id == right.owner_session_id
        && left.lease_epoch == right.lease_epoch
}

fn merge_same_identity_lease(current: &WriteLease, incoming: WriteLease) -> WriteLease {
    let mut merged = incoming;
    merged.expires_at_unix_ms = merged.expires_at_unix_ms.max(current.expires_at_unix_ms);
    merged
}

fn lock_authority_renewal_guard(
    authority: &Arc<DfsLockAuthority>,
) -> Result<std::sync::MutexGuard<'_, ()>> {
    authority
        .renewal
        .lock()
        .map_err(|_| unavailable("DFS lock authority renewal guard is poisoned"))
}

fn refresh_same_epoch_lock_authority(
    authority: &Arc<DfsLockAuthority>,
    lease: WriteLease,
) -> Result<()> {
    let mut state = authority
        .state
        .lock()
        .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
    if let Some(error) = state.last_error.clone() {
        return Err(error);
    }
    if same_lease_identity(&state.lease, &lease) {
        state.lease = merge_same_identity_lease(&state.lease, lease);
    }
    Ok(())
}

fn apply_lock_renewal_success(
    authority: &Arc<DfsLockAuthority>,
    renewed: WriteLease,
) -> Result<Option<Error>> {
    let terminal_error = {
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        if let Some(error) = state.last_error.clone() {
            return Ok(Some(error));
        }
        let terminal_error = if !same_lease_identity(&state.lease, &renewed) {
            Some(stale(
                "DFS lock authority was renewed with a different lease identity",
            ))
        } else if state
            .lease
            .expires_at_unix_ms
            .max(renewed.expires_at_unix_ms)
            <= now_unix_ms()
        {
            Some(stale("DFS lock authority lease expired"))
        } else {
            None
        };
        if let Some(error) = terminal_error.clone() {
            state.last_error = Some(error);
            state.pinned_owners.clear();
            state.waiters.clear();
        } else {
            state.lease = merge_same_identity_lease(&state.lease, renewed);
        }
        terminal_error
    };
    if terminal_error.is_some() {
        let _ = authority.table.invalidate();
    }
    Ok(terminal_error)
}

fn apply_lock_renewal_failure(
    authority: &Arc<DfsLockAuthority>,
    attempted: &WriteLease,
    error: Error,
) -> Result<Option<Error>> {
    let terminal_error = {
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        if let Some(error) = state.last_error.clone() {
            return Ok(Some(error));
        }
        if !same_lease_identity(&state.lease, attempted) {
            return Ok(None);
        }
        let terminal_error = if is_confirmed_lock_renewal_fence(&error) {
            Some(error)
        } else if state.lease.expires_at_unix_ms <= now_unix_ms() {
            Some(stale("DFS lock authority lease expired"))
        } else {
            None
        };
        if let Some(error) = terminal_error.clone() {
            state.last_error = Some(error.clone());
            state.pinned_owners.clear();
            state.waiters.clear();
            Some(error)
        } else {
            None
        }
    };
    if terminal_error.is_some() {
        let _ = authority.table.invalidate();
    }
    Ok(terminal_error)
}

fn expire_lock_authority_if_due_locked(authority: &Arc<DfsLockAuthority>) -> Result<bool> {
    let expired = {
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        let expired = state.last_error.is_none() && state.lease.expires_at_unix_ms <= now_unix_ms();
        if expired {
            state.last_error = Some(stale("DFS lock authority lease expired"));
            state.pinned_owners.clear();
            state.waiters.clear();
        }
        expired
    };
    if expired {
        let _ = authority.table.invalidate();
        return Ok(true);
    }
    Ok(false)
}

fn is_confirmed_lock_renewal_fence(error: &Error) -> bool {
    matches!(
        error.code(),
        afs_error::META_DFS_CONFLICT | afs_error::NODE_DFS_STALE_HANDLE
    )
}

impl DfsLockRenewal {
    fn new() -> Self {
        Self {
            authorities: Arc::new(Mutex::new(HashMap::new())),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    fn track(&self, inode_id: InodeId, authority: Arc<DfsLockAuthority>) -> Result<()> {
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| unavailable("DFS lock renewal registry is poisoned"))?;
        authorities.insert(inode_id, authority);
        Ok(())
    }

    fn untrack_if_current(
        &self,
        inode_id: &InodeId,
        authority: &Arc<DfsLockAuthority>,
    ) -> Result<bool> {
        let mut authorities = self
            .authorities
            .lock()
            .map_err(|_| unavailable("DFS lock renewal registry is poisoned"))?;
        let should_remove = authorities
            .get(inode_id)
            .is_some_and(|current| Arc::ptr_eq(current, authority));
        if should_remove {
            authorities.remove(inode_id);
        }
        Ok(should_remove)
    }

    fn stop(&self) {
        self.running.store(false, Ordering::Release);
        if let Ok(mut authorities) = self.authorities.lock() {
            for authority in authorities.values() {
                let _ = authority.table.invalidate();
                if let Ok(mut state) = authority.state.lock() {
                    state.last_error = Some(stale("DFS lock renewal stopped"));
                    state.pinned_owners.clear();
                    state.waiters.clear();
                }
            }
            authorities.clear();
        }
    }

    fn start(&self, meta: Arc<dyn DfsMeta>) {
        if self.running.swap(true, Ordering::AcqRel) {
            return;
        }
        let authorities = self.authorities.clone();
        let running = self.running.clone();
        // The thread is intentionally one per DistributedFs instance, not one per inode.
        std::thread::spawn(move || {
            while running.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_secs(1));
                if !running.load(Ordering::Acquire) {
                    break;
                }
                let entries = match authorities.lock() {
                    Ok(map) => map
                        .iter()
                        .map(|(inode_id, authority)| (inode_id.clone(), authority.clone()))
                        .collect::<Vec<_>>(),
                    Err(_) => return,
                };
                let mut idle = Vec::new();
                for (inode_id, authority) in entries {
                    let Ok(_renewal) = lock_authority_renewal_guard(&authority) else {
                        return;
                    };
                    let (active, lease) = match authority.state.lock() {
                        Ok(state) => {
                            let active =
                                !state.pinned_owners.is_empty() || !state.waiters.is_empty();
                            (active, state.lease.clone())
                        }
                        Err(_) => return,
                    };
                    if expire_lock_authority_if_due_locked(&authority).unwrap_or(true) {
                        continue;
                    }
                    if !active && authority.table.is_idle().unwrap_or(false) {
                        drop(_renewal);
                        idle.push((inode_id, authority));
                        continue;
                    }
                    if !should_background_renew(&lease) {
                        continue;
                    }
                    match meta.renew_write_lease(lease.clone()) {
                        Ok(renewed) => {
                            if apply_lock_renewal_success(&authority, renewed).is_err() {
                                return;
                            }
                        }
                        Err(error) => {
                            if apply_lock_renewal_failure(&authority, &lease, error).is_err() {
                                return;
                            }
                        }
                    }
                }
                if !idle.is_empty() {
                    if let Ok(mut map) = authorities.lock() {
                        for (inode_id, authority) in idle {
                            let current_is_idle = map.get(&inode_id).is_some_and(|current| {
                                Arc::ptr_eq(current, &authority)
                                    && Arc::strong_count(current) <= 2
                                    && current.table.is_idle().unwrap_or(false)
                            });
                            if current_is_idle {
                                map.remove(&inode_id);
                            }
                        }
                    } else {
                        running.store(false, Ordering::Release);
                        return;
                    }
                }
            }
            running.store(false, Ordering::Release);
        });
    }
}

#[derive(Default)]
struct RemoteProviderLifecycle {
    state: Mutex<RemoteProviderLifecycleState>,
    cv: Condvar,
}

#[derive(Default)]
struct RemoteProviderLifecycleState {
    live: bool,
    inflight: usize,
}

struct RemoteProviderIoGuard {
    lifecycle: Arc<RemoteProviderLifecycle>,
}

impl RemoteProviderLifecycle {
    fn live() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(RemoteProviderLifecycleState {
                live: true,
                inflight: 0,
            }),
            cv: Condvar::new(),
        })
    }

    fn begin_io(self: &Arc<Self>) -> Result<Option<RemoteProviderIoGuard>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?;
        if !state.live {
            return Ok(None);
        }
        state.inflight = state.inflight.saturating_add(1);
        Ok(Some(RemoteProviderIoGuard {
            lifecycle: self.clone(),
        }))
    }

    fn retire_new_io(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?;
        state.live = false;
        Ok(())
    }

    fn wait_idle(&self, budget: Duration) -> Result<()> {
        let deadline = Instant::now() + budget;
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?;
        while state.inflight != 0 {
            let now = Instant::now();
            if now >= deadline {
                return Err(unavailable("DFS remote provider idle wait timed out"));
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, timeout) = self
                .cv
                .wait_timeout(state, remaining)
                .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?;
            state = next_state;
            if timeout.timed_out() && state.inflight != 0 {
                return Err(unavailable("DFS remote provider idle wait timed out"));
            }
        }
        Ok(())
    }

    fn is_live(&self) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?
            .live)
    }

    fn is_idle(&self) -> Result<bool> {
        Ok(self
            .state
            .lock()
            .map_err(|_| unavailable("DFS remote provider lifecycle is poisoned"))?
            .inflight
            == 0)
    }
}

impl Drop for RemoteProviderIoGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.lifecycle.state.lock() {
            state.inflight = state.inflight.saturating_sub(1);
            self.lifecycle.cv.notify_all();
        }
    }
}

#[derive(Clone)]
struct RemoteDfsWriteSession {
    owner: Arc<dyn RemoteDfsOwner>,
    handle: afs_protocol::node_control::DfsOwnerHandle,
    read_handle: Option<afs_protocol::node_control::DfsOwnerHandle>,
    release_slots: usize,
    lifecycle: Arc<RemoteProviderLifecycle>,
}

struct RemoteDfsProviderIo {
    remote: RemoteDfsWriteSession,
    _guard: RemoteProviderIoGuard,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PendingRemoteReleaseKey {
    namespace_id: String,
    inode_id: String,
    owner_node_id: String,
    owner_session_id: String,
    lease_epoch: u64,
    caller_node_id: String,
    caller_session_id: String,
    open_seq: u64,
    opaque_handle: Vec<u8>,
}

#[derive(Clone)]
struct PendingRemoteRelease {
    owner: Arc<dyn RemoteDfsOwner>,
    handle: afs_protocol::node_control::DfsOwnerHandle,
    lifecycle: Option<Arc<RemoteProviderLifecycle>>,
}

#[derive(Default)]
struct PendingRemoteReleaseState {
    entries: HashMap<PendingRemoteReleaseKey, PendingRemoteRelease>,
    order: VecDeque<PendingRemoteReleaseKey>,
    reserved: usize,
}

#[derive(Clone)]
struct DfsRemoteWriteSession {
    local: DfsWriteSession,
    remote: Option<RemoteDfsWriteSession>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RemoteOperationKey {
    peer: String,
    caller_session_id: String,
    operation_id: OperationId,
}

#[derive(Clone)]
enum RemoteOperationResult {
    Write {
        fingerprint: [u8; 32],
        reply: afs_protocol::node_data::DfsOwnerWriteReply,
    },
    Resize {
        fingerprint: [u8; 32],
        reply: afs_protocol::node_data::DfsOwnerResizeReply,
    },
    Sync {
        fingerprint: [u8; 32],
        reply: afs_protocol::node_data::DfsOwnerSyncReply,
    },
}

const MAX_REMOTE_OPERATION_RESULTS: usize = 4096;
const MAX_LOCK_AUTHORITIES: usize = 8192;
const MAX_REMOTE_LOCK_AUTHORITIES: usize = 8192;
const MAX_REMOTE_LOCK_WAITERS: usize = 4096;
const MAX_CLOSED_LOCK_SESSIONS: usize = 4096;
const REMOTE_LOCK_SESSION_PENDING_CLOSE: usize = 0;
const REMOTE_LOCK_SESSION_ACTIVE: usize = 1;

#[derive(Clone)]
pub struct DfsWriteSession {
    pub id: DfsWriteSessionId,
    pub inode_id: InodeId,
    pub open_flags: i32,
    pub lease_epoch: u64,
    pub last_accepted_seq: u64,
    pub last_synced_seq: u64,
    pub error_cursor: u64,
}

struct ResolvedReadView {
    committed: Option<FileVersionId>,
    length: u64,
    layout: LayoutRoot,
    frozen: Option<DirtyExtentMap>,
    active: DirtyExtentMap,
}

struct InodeWriteState {
    inode: InodeRecord,
    write_lease: WriteLease,
    base_version: Option<FileVersion>,
    base_layout: LayoutRoot,
    logical_length: u64,
    metadata_dirty: bool,
    kill_suidgid_dirty: bool,
    dirty_extents: DirtyExtentMap,
    in_flight: Option<InFlightCommit>,
    commit_busy: bool,
    operation_busy: bool,
    native_queue: NativeTicketQueue,
    native_thread: Option<std::thread::ThreadId>,
    native_owner: Arc<NativeWriteOwner>,
    native_due: bool,
    native_due_through: u64,
    dirty: bool,
    next_write_seq: u64,
    visible_write_seq: u64,
    durable_write_seq: u64,
    committed_write_seq: u64,
    open_writers: u64,
    last_writer_background_requested: bool,
    background_error: Option<ObservedWriteError>,
    terminal_error: Option<Error>,
}

struct PrepareCommitError {
    error: Error,
}

#[derive(Clone)]
struct ObservedWriteError {
    cursor: u64,
    error: Error,
}

const NATIVE_METADATA_BYTES: usize = 4 * 1024 * 1024;
const NATIVE_CONTROL_BYTES: usize = 64 * 1024;
const NATIVE_DESCRIPTOR_BYTES: usize = NATIVE_METADATA_BYTES - NATIVE_CONTROL_BYTES;

#[derive(Clone, Copy)]
pub(crate) struct NativeWriteConfig {
    pub unpublished_descriptors: usize,
    pub upload_inflight_chunks: usize,
}

impl Default for NativeWriteConfig {
    fn default() -> Self {
        Self {
            unpublished_descriptors: 1024,
            upload_inflight_chunks: 8,
        }
    }
}

#[derive(Default)]
struct NativeMetadataState {
    control_bytes: usize,
    control_peak: usize,
    descriptor_bytes: usize,
    descriptor_reserved_bytes: usize,
    scratch_reserved_bytes: usize,
    descriptor_peak: usize,
    descriptors: usize,
    descriptor_peak_count: usize,
    inflight: usize,
    inflight_peak: usize,
    completed: u64,
    failed: u64,
    pressure_descriptor: u64,
    pressure_live: u64,
}

struct NativeMetadataBudget {
    state: Mutex<NativeMetadataState>,
    descriptor_limit: usize,
    upload_limit: usize,
    notify: Arc<tokio::sync::Notify>,
}

struct NativeMetadataLease {
    budget: Arc<NativeMetadataBudget>,
    bytes: usize,
    descriptor: bool,
    counted_descriptor: bool,
    actual_bytes: usize,
}

impl NativeMetadataBudget {
    fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        descriptor: bool,
    ) -> Result<Option<NativeMetadataLease>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("native metadata budget is poisoned"))?;
        if descriptor {
            if state.descriptors >= self.descriptor_limit
                || bytes > NATIVE_DESCRIPTOR_BYTES.saturating_sub(state.descriptor_reserved_bytes)
            {
                return Ok(None);
            }
            state.descriptors += 1;
            state.descriptor_reserved_bytes += bytes;
            state.descriptor_peak_count = state.descriptor_peak_count.max(state.descriptors);
        } else {
            if bytes > NATIVE_CONTROL_BYTES.saturating_sub(state.control_bytes) {
                return Ok(None);
            }
            state.control_bytes += bytes;
            state.control_peak = state.control_peak.max(state.control_bytes);
        }
        Ok(Some(NativeMetadataLease {
            budget: self.clone(),
            bytes,
            descriptor,
            counted_descriptor: descriptor,
            actual_bytes: 0,
        }))
    }
}

impl NativeMetadataLease {
    fn set_actual(&mut self, actual: usize) -> Result<()> {
        let budget = self.budget.clone();
        let mut state = budget
            .state
            .lock()
            .map_err(|_| unavailable("native metadata budget is poisoned"))?;
        self.set_actual_locked(actual, &mut state)
    }

    // Caller has already separated a structural lock failure from an
    // owner-scoped capacity refusal. The arithmetic and limits stay shared.
    fn set_actual_locked(&mut self, actual: usize, state: &mut NativeMetadataState) -> Result<()> {
        if self.descriptor {
            let reserved = self.bytes.max(actual.saturating_mul(4));
            if reserved - self.bytes
                > NATIVE_DESCRIPTOR_BYTES.saturating_sub(state.descriptor_reserved_bytes)
            {
                return Err(unavailable(
                    "native metadata actual capacity exceeds its reservation",
                ));
            }
            let next_reserved = state.descriptor_reserved_bytes + reserved - self.bytes;
            let next_actual = state.descriptor_bytes - self.actual_bytes + actual;
            if next_actual.saturating_add(state.scratch_reserved_bytes) > next_reserved {
                return Err(unavailable(
                    "native receipt cannot consume reserved publication scratch",
                ));
            }
            state.descriptor_reserved_bytes = next_reserved;
            state.descriptor_bytes = next_actual;
            state.descriptor_peak = state.descriptor_peak.max(state.descriptor_bytes);
            self.bytes = reserved;
            self.actual_bytes = actual;
        }
        Ok(())
    }
}

fn native_receipt_heap(receipt: &ChunkReceipt) -> usize {
    receipt.operation_id.0.capacity()
        + receipt.chunk.id.0.capacity()
        + receipt.replica_group_id.0.capacity()
        + receipt.durable_acks.capacity() * std::mem::size_of::<crate::dfs::ReplicaAck>()
        + receipt
            .durable_acks
            .iter()
            .map(|ack| {
                ack.operation_id.0.capacity()
                    + ack.chunk_id.0.capacity()
                    + ack.node_id.capacity()
                    + ack.device_id.capacity()
            })
            .sum::<usize>()
        + receipt.external_commit.as_ref().map_or(0, |external| {
            external.store_id.capacity()
                + external.generation.capacity()
                + external.object_key.capacity()
        })
}

impl Drop for NativeMetadataLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.budget.state.lock() {
            if self.descriptor {
                state.descriptor_reserved_bytes =
                    state.descriptor_reserved_bytes.saturating_sub(self.bytes);
                state.descriptor_bytes = state.descriptor_bytes.saturating_sub(self.actual_bytes);
                if self.counted_descriptor {
                    state.descriptors = state.descriptors.saturating_sub(1);
                }
            } else {
                state.control_bytes = state.control_bytes.saturating_sub(self.bytes);
            }
        }
        self.budget.notify.notify_one();
    }
}

#[derive(Default)]
struct NativeTicketQueue {
    values: VecDeque<u64>,
    allocation: Option<NativeMetadataLease>,
}

impl NativeTicketQueue {
    fn push(&mut self, id: u64, budget: &Arc<NativeMetadataBudget>) -> Result<()> {
        if self.values.len() == self.values.capacity() {
            let target = self.values.capacity().saturating_mul(2).max(4);
            let bytes = target
                .checked_mul(std::mem::size_of::<u64>())
                .ok_or_else(file_too_large)?;
            // Charge the replacement while the old capacity is still owned.
            let Some(mut allocation) = budget.reserve(bytes, false)? else {
                return Err(unavailable("native admission control capacity is full"));
            };
            let mut replacement = VecDeque::new();
            replacement.try_reserve_exact(target).map_err(|_| {
                Error::coded(
                    afs_error::IO_OUT_OF_MEMORY,
                    "native admission queue allocation refused",
                )
            })?;
            let actual = replacement.capacity() * std::mem::size_of::<u64>();
            if actual > allocation.bytes {
                let Some(mut extra) = budget.reserve(actual - allocation.bytes, false)? else {
                    return Err(unavailable(
                        "native admission queue actual capacity exceeds its control budget",
                    ));
                };
                // Transfer the two reservations into one without refunding the
                // extra capacity while the replacement remains live.
                allocation.bytes += extra.bytes;
                extra.bytes = 0;
            }
            replacement.extend(self.values.drain(..));
            self.values = replacement;
            self.allocation = Some(allocation);
        }
        self.values.push_back(id);
        Ok(())
    }

    fn remove(&mut self, id: u64) {
        if let Some(index) = self.values.iter().position(|value| *value == id) {
            self.values.remove(index);
        }
        if self.values.is_empty() {
            self.values = VecDeque::new();
            self.allocation = None;
        }
    }
}

struct NativeWritePipeline {
    budget: Arc<NativeWriteBudget>,
    metadata: Arc<NativeMetadataBudget>,
    runtime: tokio::runtime::Handle,
    notify: Arc<tokio::sync::Notify>,
    metrics: Option<crate::node::read_work::ReadWorkMetrics>,
    closed: AtomicBool,
    next_ticket: AtomicU64,
    allocation_queue: Mutex<NativeTicketQueue>,
    tasks: Mutex<Vec<Arc<NativeUpload>>>,
    _task_capacity: Option<NativeMetadataLease>,
}

impl NativeWritePipeline {
    fn new(
        budget: Arc<NativeWriteBudget>,
        config: NativeWriteConfig,
        runtime: tokio::runtime::Handle,
        metrics: Option<crate::node::read_work::ReadWorkMetrics>,
    ) -> Self {
        let notify = Arc::new(tokio::sync::Notify::new());
        let metadata = Arc::new(NativeMetadataBudget {
            state: Mutex::new(NativeMetadataState::default()),
            descriptor_limit: config.unpublished_descriptors,
            upload_limit: config.upload_inflight_chunks,
            notify: notify.clone(),
        });
        let tasks = Vec::with_capacity(config.upload_inflight_chunks);
        let task_capacity = metadata
            .reserve(
                tasks.capacity() * std::mem::size_of::<Arc<NativeUpload>>(),
                false,
            )
            .ok()
            .flatten();
        Self {
            budget,
            metadata,
            runtime,
            notify,
            metrics,
            closed: AtomicBool::new(false),
            next_ticket: AtomicU64::new(1),
            allocation_queue: Mutex::new(NativeTicketQueue::default()),
            tasks: Mutex::new(tasks),
            _task_capacity: task_capacity,
        }
    }

    fn observe(&self) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let live = self.budget.snapshot();
        metrics.native_write_live(&live);
        if let Ok(state) = self.metadata.state.lock() {
            metrics.native_write_resources(&[
                (
                    "descriptor_reserved_bytes",
                    state.descriptor_reserved_bytes as u64,
                ),
                (
                    "publication_scratch_reserved_bytes",
                    state.scratch_reserved_bytes as u64,
                ),
                ("descriptor_bytes", state.descriptor_bytes as u64),
                ("descriptor_peak_bytes", state.descriptor_peak as u64),
                ("descriptors", state.descriptors as u64),
                ("descriptor_peak_count", state.descriptor_peak_count as u64),
                ("control_bytes", state.control_bytes as u64),
                ("control_peak_bytes", state.control_peak as u64),
                ("control_reserve_bytes", NATIVE_CONTROL_BYTES as u64),
                ("metadata_limit_bytes", NATIVE_METADATA_BYTES as u64),
                ("descriptor_limit_bytes", NATIVE_DESCRIPTOR_BYTES as u64),
                ("upload_inflight", state.inflight as u64),
                ("upload_peak", state.inflight_peak as u64),
                ("upload_limit", self.metadata.upload_limit as u64),
                ("pressure_descriptor", state.pressure_descriptor),
                ("pressure_live", state.pressure_live),
                ("completed_uploads", state.completed),
                ("failed_uploads", state.failed),
            ]);
        }
    }
}

struct NativeMutationTicket {
    id: u64,
    cell: SharedInodeWriteState,
    pipeline: Arc<NativeWritePipeline>,
}

impl Drop for NativeMutationTicket {
    fn drop(&mut self) {
        if let Ok(mut state) = self.cell.lock() {
            if state.native_queue.values.front() == Some(&self.id) {
                state.native_thread = None;
            }
            state.native_queue.remove(self.id);
        }
        self.cell.notify_all();
        self.pipeline.notify.notify_one();
    }
}

struct NativeAllocationTurn {
    id: u64,
    pipeline: Arc<NativeWritePipeline>,
}

impl Drop for NativeAllocationTurn {
    fn drop(&mut self) {
        if let Ok(mut queue) = self.pipeline.allocation_queue.lock() {
            queue.remove(self.id);
        }
        self.pipeline.notify.notify_one();
    }
}

struct NativeCommitAccounting {
    budget: Arc<NativeMetadataBudget>,
    reserved: usize,
    actual: AtomicUsize,
    _owners: Vec<Arc<Mutex<NativeMetadataLease>>>,
}

impl NativeCommitAccounting {
    fn set_actual(&self, bytes: usize) -> Result<()> {
        if bytes > self.reserved {
            return Err(unavailable(
                "native commit metadata exceeds its publication scratch",
            ));
        }
        let mut state = self
            .budget
            .state
            .lock()
            .map_err(|_| unavailable("native metadata budget is poisoned"))?;
        let previous = self.actual.swap(bytes, Ordering::Relaxed);
        state.descriptor_bytes = state
            .descriptor_bytes
            .saturating_sub(previous)
            .saturating_add(bytes);
        state.descriptor_peak = state.descriptor_peak.max(state.descriptor_bytes);
        Ok(())
    }
}

impl Drop for NativeCommitAccounting {
    fn drop(&mut self) {
        if let Ok(mut state) = self.budget.state.lock() {
            state.scratch_reserved_bytes =
                state.scratch_reserved_bytes.saturating_sub(self.reserved);
            state.descriptor_bytes = state
                .descriptor_bytes
                .saturating_sub(self.actual.load(Ordering::Relaxed));
        }
        self.budget.notify.notify_one();
    }
}

struct NativeRpcAccounting {
    accounting: Arc<NativeCommitAccounting>,
    base: usize,
}
impl Drop for NativeRpcAccounting {
    fn drop(&mut self) {
        let _ = self.accounting.set_actual(self.base);
    }
}

// P1M already retains complete base/layout/version and owned Meta RPC values.
// Do not impose a new file-size limit on those existing layers. This ledger
// covers W2's additional retained receipts and incremental publication owners;
// full-layout/domain/wire capacity remains a separately reported RSS cost.
fn native_commit_heap(commit: &CommitFileVersion) -> usize {
    std::mem::size_of::<CommitFileVersion>()
        + commit.operation_id.0.capacity()
        + commit.inode_id.0.capacity()
        + commit
            .expected_head_version
            .as_ref()
            .map_or(0, |id| id.0.capacity())
        + commit.write_lease.inode_id.0.capacity()
        + commit.write_lease.owner_node_id.capacity()
        + commit.write_lease.owner_session_id.capacity()
        + commit.chunk_receipts.capacity() * std::mem::size_of::<ChunkReceipt>()
        + commit
            .chunk_receipts
            .iter()
            .map(native_receipt_heap)
            .sum::<usize>()
}

struct NativeUploadPermit(Arc<NativeMetadataBudget>);
impl Drop for NativeUploadPermit {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.inflight = state.inflight.saturating_sub(1);
        }
        self.0.notify.notify_one();
    }
}

struct NativeUpload {
    _gc_pin: Option<Arc<crate::node::gc::GcPin>>,
    operation_id: OperationId,
    chunk: ChunkObject,
    owner: Weak<InodeWriteStateCell>,
    state: Mutex<NativeUploadState>,
    cv: Condvar,
    _descriptor: Arc<Mutex<NativeMetadataLease>>,
}

struct NativeUploadState {
    body: Option<SharedChunkBytes>,
    staged: Option<StagedChunk>,
    task: Option<tokio::task::JoinHandle<Result<NativeChunkReceipt>>>,
    receipt: Option<NativeChunkReceipt>,
    error: Option<Error>,
    terminal_error: Option<Error>,
    joining: bool,
}

impl NativeUpload {
    fn read_at(&self, store: &dyn ChunkStore, offset: u64, out: &mut [u8]) -> Result<usize> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("native upload state is poisoned"))?;
        if let Some(error) = &state.terminal_error {
            return Err(error.clone());
        }
        if let Some(body) = &state.body {
            let start = usize::try_from(offset)
                .map_err(|_| file_too_large())?
                .min(body.len());
            let size = out.len().min(body.len() - start);
            out[..size].copy_from_slice(&body[start..start + size]);
            return Ok(size);
        }
        let receipt = state
            .receipt
            .as_ref()
            .ok_or_else(|| unavailable("native uploaded body has no durable receipt"))?;
        if !receipt.local_required_and_durable {
            return Err(unavailable(
                "native nonlocal body was released before publication",
            ));
        }
        store.read_native_uploaded_at(&receipt.receipt, offset, out)
    }
}

struct NativeInodeMutationGuard<'a> {
    state: std::sync::MutexGuard<'a, InodeWriteState>,
    _ticket: Option<NativeMutationTicket>,
}
impl std::ops::Deref for NativeInodeMutationGuard<'_> {
    type Target = InodeWriteState;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl std::ops::DerefMut for NativeInodeMutationGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

struct InodeOperationGuard {
    cell: SharedInodeWriteState,
    native_notify: Option<Arc<tokio::sync::Notify>>,
}

impl Drop for InodeOperationGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.cell.lock() {
            state.operation_busy = false;
            self.cell.notify_all();
        }
        if let Some(notify) = &self.native_notify {
            notify.notify_one();
        }
    }
}

#[derive(Clone)]
struct DirtyExtent {
    file_offset: u64,
    length: u64,
    write_seq: u64,
    data: Option<SharedChunkBytes>,
    builder: Option<usize>,
    source_offset: usize,
    // The last write's original coverage boundary, independent of visible
    // interval splits. Reusing this owner must not turn one input copy into
    // multiple copies merely because later writes split the visible map.
    native_reuse_end: u64,
    native_upload: Option<Arc<NativeUpload>>,
    native_descriptor: Option<Arc<Mutex<NativeMetadataLease>>>,
}

#[derive(Default)]
struct DirtyExtentMap {
    // Native writes and resize holes retain only the final nonoverlapping
    // visible intervals.
    extents: BTreeMap<u64, DirtyExtent>,
    builders: Vec<NativeDirtyBuilder>,
    append_builders: BTreeMap<u64, BTreeSet<(u64, usize)>>,
    next_builder_order: u64,
    native_admitted_bytes: Option<u64>,
    native_operation_id: Option<OperationId>,
    native_gc_pin: Option<Arc<crate::node::gc::GcPin>>,
}

struct NativeDirtyBuilder {
    file_offset: u64,
    builder: ChunkBuilder,
    descriptor: Option<Arc<Mutex<NativeMetadataLease>>>,
    creation_order: u64,
}

// Frozen maps and S3 maps only clone immutable owners. The sole mutable-map
// clone fallback is a read snapshot; production reads use snapshot_range instead.
impl Clone for DirtyExtentMap {
    fn clone(&self) -> Self {
        let bodies = self
            .builders
            .iter()
            .map(|entry| SharedChunkBytes::from_vec(entry.builder.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        let freeze = |extent: &DirtyExtent| {
            let mut extent = extent.clone();
            if let Some(index) = extent.builder.take() {
                extent.data = Some(bodies[index].clone());
            }
            extent
        };
        Self {
            extents: self
                .extents
                .iter()
                .map(|(key, extent)| (*key, freeze(extent)))
                .collect(),
            builders: Vec::new(),
            append_builders: BTreeMap::new(),
            next_builder_order: self.next_builder_order,
            native_admitted_bytes: self.native_admitted_bytes,
            native_operation_id: self.native_operation_id.clone(),
            native_gc_pin: self.native_gc_pin.clone(),
        }
    }
}

#[derive(Clone)]
struct FrozenCommit {
    through_seq: u64,
    logical_length: u64,
    inode: InodeRecord,
    write_lease: WriteLease,
    base_version: Option<FileVersion>,
    base_layout: LayoutRoot,
    dirty_extents: DirtyExtentMap,
    kill_suidgid_dirty: bool,
}

enum InFlightCommit {
    Preparing(Box<FrozenCommit>),
    File(Box<PendingFileCommit>),
    Metadata(Box<SyncInodeMetadata>, u64),
}

#[derive(Clone)]
struct PendingFileCommit {
    frozen: FrozenCommit,
    batch: CommitBatch,
    // Diagnostic only, shared by clones of this exact prepared request. Never
    // serialized into the domain request or consulted by filesystem policy.
    trace_send_attempt: Option<Arc<AtomicU64>>,
}

struct CommitPlan {
    layout_root: LayoutRoot,
    staged_chunks: Vec<StagedChunk>,
}

#[derive(Clone)]
struct OverlaySegment {
    file_offset: u64,
    length: u64,
    data: Option<(SharedChunkBytes, usize)>,
}

#[derive(Clone)]
struct CommitBatch {
    _gc_pin: Option<Arc<crate::node::gc::GcPin>>,
    through_seq: u64,
    commit: Arc<CommitFileVersion>,
    _native_accounting: Option<Arc<NativeCommitAccounting>>,
    _native_uploads: Vec<Arc<NativeUpload>>,
}

enum AppliedCommit {
    File {
        through_seq: u64,
        committed_full_metadata: bool,
        version: FileVersion,
        layout: LayoutRoot,
        updated: InodeRecord,
    },
    Metadata {
        through_seq: u64,
        updated: InodeRecord,
    },
}

/// Why a commit runs. A foreground commit is part of a caller's barrier and
/// waits its turn behind the inode's admitted work; a background commit is
/// best effort and never waits. Each policy difference is one method below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitReason {
    Foreground(ForegroundCommit),
    Background(BackgroundCommit),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ForegroundCommit {
    /// fdatasync, and data barriers inside other operations (resize, setattr).
    DataSync,
    /// fsync: data plus a full metadata barrier.
    FullSync,
    /// FUSE flush on close(2); data only (decision 5).
    Close,
    /// Node shutdown.
    NodeDrain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackgroundCommit {
    Periodic,
}

/// How a commit step orders itself against operations already admitted on the
/// inode.
#[derive(Clone, Copy, Debug)]
enum CommitWait {
    /// Wait for admitted operations (the normal entry from outside).
    AfterOperations,
    /// Called from inside an admitted operation on the same inode.
    InsideOperation,
}

impl CommitWait {
    fn waits_for_operations(self) -> bool {
        !matches!(self, Self::InsideOperation)
    }
}

impl InodeWriteState {
    /// This Node has write activity on the inode: an open local writer,
    /// accepted writes not yet committed, or a commit in flight. While it
    /// lasts every handle on this Node reads the Node's own view (4a).
    fn has_local_write_activity(&self) -> bool {
        self.open_writers > 0
            || self.dirty
            || self.in_flight.is_some()
            || self.visible_write_seq > self.committed_write_seq
    }
}

impl InFlightCommit {
    fn dirty_extents(&self) -> Option<DirtyExtentMap> {
        match self {
            InFlightCommit::Preparing(frozen) => Some(frozen.dirty_extents.clone()),
            InFlightCommit::File(pending) => Some(pending.frozen.dirty_extents.clone()),
            InFlightCommit::Metadata(_, _) => None,
        }
    }
}

impl InodeWriteStateCell {
    fn new(state: InodeWriteState) -> Self {
        Self {
            state: Mutex::new(state),
            cv: Condvar::new(),
            lock_wait: None,
        }
    }

    fn with_lock_wait(mut self, lock_wait: Option<Arc<InodeLockWait>>) -> Self {
        self.lock_wait = lock_wait;
        self
    }

    fn lock(&self) -> std::sync::LockResult<std::sync::MutexGuard<'_, InodeWriteState>> {
        let Some(lock_wait) = &self.lock_wait else {
            return self.state.lock();
        };
        match self.state.try_lock() {
            Ok(guard) => Ok(guard),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => Err(poisoned),
            Err(std::sync::TryLockError::WouldBlock) => {
                let started = Instant::now();
                let result = self.state.lock();
                lock_wait.record(started.elapsed());
                result
            }
        }
    }

    fn wait<'a>(
        &self,
        guard: std::sync::MutexGuard<'a, InodeWriteState>,
    ) -> std::sync::LockResult<std::sync::MutexGuard<'a, InodeWriteState>> {
        self.cv.wait(guard)
    }

    fn notify_all(&self) {
        self.cv.notify_all();
    }
}

impl DistributedFs {
    pub fn new(
        namespace_id: NamespaceId,
        node_id: impl Into<String>,
        session_id: impl Into<String>,
        meta: Arc<dyn DfsMeta>,
        chunk_store: Arc<dyn ChunkStore>,
        read_engine: Arc<DfsReadEngine>,
    ) -> Self {
        let node_id = node_id.into();
        let session_id = session_id.into();
        // 原生存储的写流水线由装配方用 `with_native_write_pipeline` 装上（需要 Node runtime）；
        // 构造时不装，避免出现只在测试里存在的"无 runtime 流水线"。
        let native_pipeline = None;
        #[cfg(not(test))]
        let pending_trace = PendingTrace::from_env(&namespace_id.0, &node_id, &session_id);
        #[cfg(test)]
        let pending_trace = None;
        Self {
            inode_lock_wait: None,
            namespace_id,
            node_id,
            session_id,
            pending_trace,
            meta,
            chunk_store,
            read_engine,
            remote_owner_factory: None,
            handles: DfsHandleTable::default(),
            gc_references: None,
            gc_gate: Mutex::new(()),
            dir_handles: Mutex::new(HashMap::new()),
            inode_writes: ShardedMap::default(),
            remote_inode_providers: ShardedMap::default(),
            pending_remote_releases: Mutex::new(PendingRemoteReleaseState::default()),
            lock_authorities: ShardedMap::default(),
            lock_authority_admission: Mutex::new(()),
            lock_renewal: Arc::new(DfsLockRenewal::new()),
            remote_lock_authorities: Mutex::new(HashMap::new()),
            remote_lock_authority_sessions: Mutex::new(HashMap::new()),
            remote_lock_waiters: Mutex::new(HashMap::new()),
            cancelled_remote_lock_waiters: Mutex::new(HashSet::new()),
            owner_open_routes: Mutex::new(HashMap::new()),
            remote_open_admission_locks: Mutex::new(HashMap::new()),
            #[cfg(test)]
            owner_open_finish_pause: Mutex::new(None),
            #[cfg(test)]
            native_read_before_delivery: Mutex::new(None),
            #[cfg(test)]
            native_read_after_committed: Mutex::new(None),
            #[cfg(test)]
            backend_inode_after_missing: Mutex::new(None),
            #[cfg(test)]
            handle_registration_after_route_check: Mutex::new(None),
            #[cfg(test)]
            readonly_version_before_install: Mutex::new(None),
            lock_session_reap_cursor: AtomicU64::new(0),
            owner_handle_reap_cursor: AtomicU64::new(0),
            write_lease_cursor: AtomicU64::new(0),
            #[cfg(test)]
            writeback_after_prepare_pause: Mutex::new(None),
            closed_lock_sessions: Mutex::new(HashSet::new()),
            closed_lock_session_admission_closed: AtomicBool::new(false),
            remote_operation_results: Mutex::new(HashMap::new()),
            readonly_version_gates: Arc::new(Default::default()),
            readonly_version_cache: Arc::new(Mutex::new(ReadOnlyVersionCache::new(
                DEFAULT_VERSION_CACHE_ENTRIES,
                DEFAULT_VERSION_CACHE_BYTES,
            ))),
            readonly_empty_version: Arc::new(ReadOnlyVersion::empty()),
            write_buffer_pool: WriteBufferPool::new(128 * 1024 * 1024),
            native_pipeline,
            inode_to_backend: ShardedMap::from_entries([(InodeId::new("1"), ROOT_INODE)]),
            backend_to_inode: ShardedMap::from_entries([(ROOT_INODE, InodeId::new("1"))]),
            inode_mapping_creation: Mutex::new(()),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
            next_handle: AtomicU64::new(1),
            next_operation: AtomicU64::new(1),
            next_owner_open_seq: AtomicU64::new(1),
        }
    }

    pub fn with_gc_references(
        mut self,
        references: Option<Arc<crate::node::gc::GcReferences>>,
    ) -> Self {
        self.gc_references = references;
        self
    }
    /// Native storage writes segmented-CRC32C objects; S3 stores only
    /// canonical BLAKE3 objects (decision recorded in S1a-7, F6).
    fn crc32c_writes(&self) -> bool {
        !self.read_engine.has_external_backend()
    }
    fn stage_native_chunk(&self, op: OperationId, bytes: SharedChunkBytes) -> Result<StagedChunk> {
        if self.crc32c_writes() {
            StagedChunk::from_shared_crc32c(op, bytes)
        } else {
            Ok(StagedChunk::from_shared(op, bytes))
        }
    }
    pub fn flush_gc_releases(&self) -> Result<()> {
        if let Some(references) = &self.gc_references {
            references.flush()?;
        }
        Ok(())
    }
    pub fn collect_garbage(&self) -> Result<usize> {
        let Some(references) = &self.gc_references else {
            return Ok(0);
        };
        let _gate = self
            .gc_gate
            .lock()
            .map_err(|_| unavailable("GC worker poisoned"))?;
        references.flush()?;
        let completed = self.read_engine.gc_generation()?;
        let batch = references
            .control
            .collect(completed, self.read_engine.gc_inventory()?)?;
        if batch.waiting_nodes.contains(&self.node_id) && batch.generation > completed {
            self.read_engine
                .collect_generation(batch.generation, &batch.chunks)?;
            references.control.collect(batch.generation, Vec::new())?;
            return Ok(batch.chunks.len());
        }
        Ok(0)
    }

    /// Register inode write-state lock contention counters with the Node registry.
    pub(crate) fn with_inode_lock_metrics(
        mut self,
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        self.inode_lock_wait = Some(Arc::new(InodeLockWait::register(registry)?));
        Ok(self)
    }

    pub(crate) fn with_native_write_pipeline(
        mut self,
        budget: Arc<NativeWriteBudget>,
        config: NativeWriteConfig,
        runtime: tokio::runtime::Handle,
        metrics: Option<crate::node::read_work::ReadWorkMetrics>,
    ) -> Result<Self> {
        if config.unpublished_descriptors == 0
            || config.unpublished_descriptors > 1024
            || config.upload_inflight_chunks == 0
            || config.upload_inflight_chunks > 8
        {
            return Err(invalid("invalid native write pipeline configuration"));
        }
        self.native_pipeline = Some(Arc::new(NativeWritePipeline::new(
            budget, config, runtime, metrics,
        )));
        self.observe_native_resources();
        Ok(self)
    }

    pub(crate) fn native_writeback_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.native_pipeline
            .as_ref()
            .map(|pipeline| pipeline.notify.clone())
    }

    pub(crate) fn close_native_write_admission(&self) {
        if let Some(pipeline) = &self.native_pipeline {
            pipeline.closed.store(true, Ordering::Release);
            pipeline.budget.stop();
            pipeline.notify.notify_waiters();
            if let Ok(states) = self.inode_writes.snapshot() {
                for (_, state) in states {
                    state.notify_all();
                }
            }
        }
    }

    fn native_ticket(&self, cell: &SharedInodeWriteState) -> Result<Option<NativeMutationTicket>> {
        self.native_ticket_with_cleanup(cell, false)
    }

    fn native_ticket_with_cleanup(
        &self,
        cell: &SharedInodeWriteState,
        cleanup: bool,
    ) -> Result<Option<NativeMutationTicket>> {
        let Some(pipeline) = &self.native_pipeline else {
            return Ok(None);
        };
        if !cleanup && pipeline.closed.load(Ordering::Acquire) {
            return Err(unavailable("native write admission is closed"));
        }
        let id = pipeline.next_ticket.fetch_add(1, Ordering::Relaxed);
        let mut state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        if state.native_thread == Some(std::thread::current().id()) {
            return Ok(None);
        }
        state.native_queue.push(id, &pipeline.metadata)?;
        let ticket = NativeMutationTicket {
            id,
            cell: cell.clone(),
            pipeline: pipeline.clone(),
        };
        loop {
            if !cleanup && pipeline.closed.load(Ordering::Acquire) {
                drop(state);
                drop(ticket);
                return Err(unavailable("native write admission is closed"));
            }
            if state.native_queue.values.front() == Some(&id) {
                state.native_thread = Some(std::thread::current().id());
                break;
            }
            state = cell
                .cv
                .wait_timeout(state, Duration::from_millis(10))
                .map_err(|_| unavailable("native mutation queue is poisoned"))?
                .0;
        }
        drop(state);
        Ok(Some(ticket))
    }

    fn observe_native_resources(&self) {
        let Some(pipeline) = &self.native_pipeline else {
            return;
        };
        pipeline.observe();
        if let Some(metrics) = &pipeline.metrics
            && let Ok(states) = self.inode_writes.snapshot()
        {
            let mut pending = 0_u64;
            for (_, cell) in states {
                if let Ok(state) = cell.lock() {
                    pending += u64::from(state.dirty_extents.native_operation_id.is_some());
                    pending += u64::from(state.in_flight.is_some());
                }
            }
            metrics.native_write_resources(&[("pending_generations", pending)]);
        }
    }

    fn native_wait_mutation_ready(
        &self,
        inode: &InodeId,
        cell: &SharedInodeWriteState,
    ) -> Result<()> {
        loop {
            let state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            if let Some(error) = &state.terminal_error {
                return Err(error.clone());
            }
            if !Self::in_flight_blocks_mutation(&state.in_flight) {
                return Ok(());
            }
            if state.commit_busy {
                drop(
                    cell.cv
                        .wait_timeout(state, Duration::from_millis(10))
                        .map_err(|_| unavailable("DFS inode write state is poisoned"))?,
                );
            } else {
                drop(state);
                if let Some(seq) =
                    self.commit_inode(inode, CommitReason::BACKGROUND, CommitWait::AfterOperations)?
                {
                    self.update_handles_after_commit(inode, seq)?;
                }
            }
        }
    }

    fn native_owner_limit(&self, pipeline: &NativeWritePipeline) -> Result<usize> {
        let mut writers = 0;
        for (_, cell) in self.inode_writes.snapshot()? {
            let state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            if state.open_writers != 0 || state.dirty {
                writers += 1;
            }
        }
        Ok((16 * 1024 * 1024).max(pipeline.budget.snapshot().limit / writers.max(1)))
    }

    fn native_ordinary_write(
        &self,
        handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
        cell: SharedInodeWriteState,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> Result<usize> {
        let mut accepted = 0;
        let result = self.native_ordinary_write_inner(
            handle,
            snapshot,
            cell,
            (offset, data, options),
            &mut accepted,
        );
        match result {
            Err(error)
                if accepted > 0
                    && snapshot.flags & libc::O_SYNC != libc::O_SYNC
                    && !has_o_dsync(snapshot.flags) =>
            {
                // A prefix is already visible. Report its real length, retain
                // the failure for the next barrier, and never repeat input.
                self.record_background_error(&snapshot.inode_id, error)?;
                Ok(accepted)
            }
            result => result,
        }
    }

    fn native_ordinary_write_inner(
        &self,
        handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
        cell: SharedInodeWriteState,
        input: (u64, &[u8], WriteOptions),
        accepted: &mut usize,
    ) -> Result<usize> {
        let (offset, data, options) = input;
        let pipeline = self
            .native_pipeline
            .as_ref()
            .ok_or_else(|| unavailable("native write pipeline is missing"))?
            .clone();
        let ticket = self
            .native_ticket(&cell)?
            .ok_or_else(|| unavailable("native mutation ticket is missing"))?;
        'accept: while *accepted < data.len() {
            self.native_wait_mutation_ready(&snapshot.inode_id, &cell)?;
            if pipeline.closed.load(Ordering::Acquire) {
                return Err(unavailable("native write admission is closed"));
            }
            let _ = self.poll_native_uploads()?;
            self.observe_handle_error(handle)?;
            let (position, selection, capacity, generation, seq, owner) = {
                let state = cell
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                if let Some(error) = &state.terminal_error {
                    return Err(error.clone());
                }
                let position = if snapshot.flags & libc::O_APPEND != 0 {
                    state.logical_length
                } else {
                    offset
                        .checked_add(*accepted as u64)
                        .ok_or_else(file_too_large)?
                };
                position
                    .checked_add((data.len() - *accepted) as u64)
                    .ok_or_else(file_too_large)?;
                let selection = state
                    .dirty_extents
                    .native_selection(position, state.logical_length);
                let capacity = DirtyExtentMap::native_capacity(
                    position,
                    data.len() - *accepted,
                    state.logical_length,
                );
                (
                    position,
                    selection,
                    capacity,
                    state.dirty_extents.native_operation_id.clone(),
                    state.next_write_seq,
                    state.native_owner.clone(),
                )
            };
            let operation_id = generation
                .clone()
                .unwrap_or_else(|| self.operation_id("native-generation"));
            let mut prepared = None;
            let mut descriptor = None;
            if selection.is_none() {
                let turn_id = ticket.id;
                {
                    let mut queue = pipeline
                        .allocation_queue
                        .lock()
                        .map_err(|_| unavailable("native allocation queue is poisoned"))?;
                    queue.push(turn_id, &pipeline.metadata)?;
                }
                let _turn = NativeAllocationTurn {
                    id: turn_id,
                    pipeline: pipeline.clone(),
                };
                loop {
                    if pipeline.closed.load(Ordering::Acquire) {
                        return Err(unavailable("native write admission is closed"));
                    }
                    let is_front = pipeline
                        .allocation_queue
                        .lock()
                        .map_err(|_| unavailable("native allocation queue is poisoned"))?
                        .values
                        .front()
                        == Some(&turn_id);
                    if is_front {
                        if descriptor.is_none() {
                            let reservation = self
                                .chunk_store
                                .native_descriptor_reservation_bytes(&operation_id)?;
                            if reservation > NATIVE_DESCRIPTOR_BYTES {
                                return Err(Error::coded(
                                    afs_error::IO_CAPACITY,
                                    "native descriptor and publication scratch exceed their fixed metadata budget",
                                ));
                            }
                            descriptor = pipeline
                                .metadata
                                .reserve(reservation, true)?
                                .map(|lease| Arc::new(Mutex::new(lease)));
                            if let Some(owner) = &descriptor {
                                owner
                                    .lock()
                                    .map_err(|_| {
                                        unavailable("native descriptor reservation is poisoned")
                                    })?
                                    .set_actual(
                                        std::mem::size_of::<Mutex<NativeMetadataLease>>()
                                            + 2 * std::mem::size_of::<usize>(),
                                    )?;
                            }
                            if descriptor.is_none() {
                                self.request_native_pressure(&cell, true)?;
                                self.progress_native_writeback()?;
                                let epoch = pipeline.budget.snapshot().epoch;
                                pipeline
                                    .budget
                                    .wait_for_change(epoch, Duration::from_millis(10));
                                continue;
                            }
                        }
                        let limit = self.native_owner_limit(&pipeline)?;
                        match pipeline
                            .budget
                            .try_reserve_for(owner.clone(), capacity, limit)?
                        {
                            NativeWriteAdmission::Reserved(lease) => {
                                match ChunkBuilder::with_native_lease(
                                    self.write_buffer_pool.clone(),
                                    capacity,
                                    lease,
                                ) {
                                    Ok(builder) => {
                                        prepared = Some(builder);
                                        break;
                                    }
                                    Err(error)
                                        if error.code() == afs_error::IO_OUT_OF_MEMORY
                                            && *accepted > 0 =>
                                    {
                                        // The rejected tail was never accepted. Complete
                                        // any sync barrier for the visible prefix below.
                                        break 'accept;
                                    }
                                    Err(error) if error.code() == afs_error::IO_WOULD_BLOCK => {}
                                    Err(error) => return Err(error),
                                }
                            }
                            NativeWriteAdmission::Wait { epoch } => {
                                self.request_native_pressure(&cell, false)?;
                                self.progress_native_writeback()?;
                                pipeline
                                    .budget
                                    .wait_for_change(epoch, Duration::from_millis(10));
                            }
                        }
                    } else {
                        let epoch = pipeline.budget.snapshot().epoch;
                        pipeline
                            .budget
                            .wait_for_change(epoch, Duration::from_millis(10));
                    }
                    let _ = self.poll_native_uploads()?;
                    self.observe_handle_error(handle)?;
                }
            }
            let gc_pin = if generation.is_none() {
                self.gc_references
                    .as_ref()
                    .map(|references| {
                        references.pin_generation(&snapshot.inode_id, None, &operation_id)
                    })
                    .transpose()?
            } else {
                None
            };
            let operation = self.begin_inode_operation(&snapshot.inode_id, cell.clone())?;
            let (written, accepted_seq) = {
                let mut state = cell
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                let actual_position = if snapshot.flags & libc::O_APPEND != 0 {
                    state.logical_length
                } else {
                    offset + *accepted as u64
                };
                if state.next_write_seq != seq
                    || state.dirty_extents.native_operation_id != generation
                    || actual_position != position
                    || state
                        .dirty_extents
                        .native_selection(position, state.logical_length)
                        != selection
                {
                    drop(state);
                    drop(operation);
                    drop(prepared);
                    continue;
                }
                let next_seq = state.next_write_seq.saturating_add(1);
                if state.dirty_extents.native_operation_id.is_none() {
                    state.dirty_extents.native_operation_id = Some(operation_id.clone());
                    state.dirty_extents.native_gc_pin = gc_pin;
                }
                let written = state.dirty_extents.write_native_prepared(
                    position,
                    &data[*accepted..],
                    next_seq,
                    selection,
                    prepared,
                    descriptor,
                )?;
                state.logical_length = state.logical_length.max(position + written as u64);
                state.next_write_seq = next_seq;
                state.visible_write_seq = next_seq;
                state.dirty = true;
                state.metadata_dirty = true;
                let now = now_unix_ms();
                state.inode.attributes.mtime_unix_ms = now;
                state.inode.attributes.ctime_unix_ms = now;
                if options.kill_suidgid {
                    Self::apply_kernel_killpriv_to_state(&mut state);
                }
                (written, next_seq)
            };
            *accepted += written;
            drop(operation);
            self.update_handle_write_progress(handle, accepted_seq)?;
            self.seal_native_full_builders(&cell)?;
            self.start_native_uploads()?;
            self.observe_native_resources();
        }
        // Keep the FIFO ticket, but release every short operation guard before
        // waiting for the persistent barrier. Preparing may finish meanwhile.
        if snapshot.flags & libc::O_SYNC == libc::O_SYNC {
            self.sync_handle(handle, CommitReason::FULL_SYNC, CommitWait::InsideOperation)?;
        } else if has_o_dsync(snapshot.flags) {
            self.sync_handle(handle, CommitReason::DATA_SYNC, CommitWait::InsideOperation)?;
        }
        drop(ticket);
        Ok(*accepted)
    }

    fn request_native_pressure(
        &self,
        cell: &SharedInodeWriteState,
        descriptor: bool,
    ) -> Result<()> {
        let Some(pipeline) = &self.native_pipeline else {
            return Ok(());
        };
        let mut state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        let retained_nonlocal = state.dirty_extents.iter_extents().any(|extent| {
            extent.native_upload.as_ref().is_some_and(|upload| {
                upload.state.lock().is_ok_and(|state| {
                    state.terminal_error.is_none()
                        && state
                            .receipt
                            .as_ref()
                            .is_some_and(|receipt| !receipt.local_required_and_durable)
                })
            })
        });
        if descriptor || retained_nonlocal {
            state.native_due = true;
            state.native_due_through = state.native_due_through.max(state.visible_write_seq);
            let mut metadata = pipeline
                .metadata
                .state
                .lock()
                .map_err(|_| unavailable("native metadata budget is poisoned"))?;
            if descriptor {
                metadata.pressure_descriptor += 1;
            } else {
                metadata.pressure_live += 1;
            }
        }
        pipeline.notify.notify_one();
        Ok(())
    }

    fn seal_native_full_builders(&self, cell: &SharedInodeWriteState) -> Result<()> {
        let Some(pipeline) = &self.native_pipeline else {
            return Ok(());
        };
        let mut state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        let Some(operation_id) = state.dirty_extents.native_operation_id.clone() else {
            return Ok(());
        };
        while let Some(index) = state.dirty_extents.builders.iter().position(|entry| {
            !entry.builder.is_empty() && entry.builder.len() == entry.builder.writable_capacity()
        }) {
            let entry = &mut state.dirty_extents.builders[index];
            let descriptor = entry
                .descriptor
                .take()
                .ok_or_else(|| unavailable("native full builder has no descriptor reservation"))?;
            let end = entry.file_offset + entry.builder.len() as u64;
            let creation_order = entry.creation_order;
            let body = std::mem::take(&mut entry.builder).freeze();
            for extent in state.dirty_extents.extents.values_mut() {
                if extent.builder == Some(index) {
                    extent.builder = None;
                    extent.data = Some(body.clone());
                }
            }
            if let Some(indices) = state.dirty_extents.append_builders.get_mut(&end) {
                indices.remove(&(creation_order, index));
            }
            // The immutable allocation stays the same. Staging performs the
            // standard BLAKE3 scan; no body concatenation or clone is involved.
            let staged = self.stage_native_chunk(operation_id.clone(), body.clone())?;
            let upload = Arc::new(NativeUpload {
                _gc_pin: state.dirty_extents.native_gc_pin.clone(),
                operation_id: operation_id.clone(),
                chunk: staged.chunk.clone(),
                owner: Arc::downgrade(cell),
                state: Mutex::new(NativeUploadState {
                    body: Some(body),
                    staged: Some(staged),
                    task: None,
                    receipt: None,
                    error: None,
                    terminal_error: None,
                    joining: false,
                }),
                cv: Condvar::new(),
                _descriptor: descriptor,
            });
            let actual = std::mem::size_of::<NativeUpload>()
                + std::mem::size_of::<Mutex<NativeMetadataLease>>()
                + 4 * std::mem::size_of::<usize>()
                + upload.operation_id.0.capacity()
                + upload.chunk.id.0.capacity()
                + upload
                    .state
                    .lock()
                    .map_err(|_| unavailable("native upload state is poisoned"))?
                    .staged
                    .as_ref()
                    .map_or(0, StagedChunk::native_retained_metadata_bytes);
            upload
                ._descriptor
                .lock()
                .map_err(|_| unavailable("native descriptor reservation is poisoned"))?
                .set_actual(actual)?;
            let allocation = upload
                .state
                .lock()
                .map_err(|_| unavailable("native upload state is poisoned"))?
                .body
                .as_ref()
                .map(|body| body.allocation_id());
            for extent in state.dirty_extents.extents.values_mut() {
                if extent
                    .data
                    .as_ref()
                    .is_some_and(|body| Some(body.allocation_id()) == allocation)
                {
                    extent.data = None;
                    extent.native_upload = Some(upload.clone());
                }
            }
        }
        drop(state);
        pipeline.notify.notify_one();
        Ok(())
    }

    fn next_native_upload(&self) -> Result<Option<Arc<NativeUpload>>> {
        // Existing inode registry snapshots are shared lifecycle metadata, not
        // a second W2 payload or per-object queue. Hold no registry lock while
        // examining an inode or waiting for an upload.
        for (_, cell) in self.inode_writes.snapshot()? {
            if let Some(upload) = Self::next_native_upload_for_cell(&cell)? {
                return Ok(Some(upload));
            }
        }
        Ok(None)
    }

    fn next_native_upload_for_cell(
        cell: &SharedInodeWriteState,
    ) -> Result<Option<Arc<NativeUpload>>> {
        let state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        if state.terminal_error.is_some() {
            return Ok(None);
        }
        for extent in state.dirty_extents.iter_extents() {
            if let Some(upload) = &extent.native_upload {
                let upload_state = upload
                    .state
                    .lock()
                    .map_err(|_| unavailable("native upload state is poisoned"))?;
                if upload_state.receipt.is_none()
                    && upload_state.task.is_none()
                    && !upload_state.joining
                    && upload_state.error.is_none()
                    && upload_state.terminal_error.is_none()
                {
                    return Ok(Some(upload.clone()));
                }
            }
        }
        Ok(None)
    }

    fn start_native_upload(&self, upload: &Arc<NativeUpload>) -> Result<bool> {
        let pipeline = self
            .native_pipeline
            .as_ref()
            .ok_or_else(|| unavailable("native write pipeline is missing"))?;
        let mut state = upload
            .state
            .lock()
            .map_err(|_| unavailable("native upload state is poisoned"))?;
        if let Some(error) = &state.terminal_error {
            return Err(error.clone());
        }
        if state.receipt.is_some() || state.task.is_some() || state.joining || state.error.is_some()
        {
            return Ok(false);
        }
        let Some(body) = state.body.clone() else {
            return Err(unavailable("native upload lost its body"));
        };
        let mut task_table = pipeline
            .tasks
            .lock()
            .map_err(|_| unavailable("native upload task table is poisoned"))?;
        if task_table.len() >= pipeline.metadata.upload_limit {
            return Ok(false);
        }
        {
            let mut metadata = pipeline
                .metadata
                .state
                .lock()
                .map_err(|_| unavailable("native metadata budget is poisoned"))?;
            if metadata.inflight >= pipeline.metadata.upload_limit {
                return Ok(false);
            }
            metadata.inflight += 1;
            metadata.inflight_peak = metadata.inflight_peak.max(metadata.inflight);
        }
        let permit = NativeUploadPermit(pipeline.metadata.clone());
        let store = self.chunk_store.clone();
        let staged = match state.staged.take() {
            Some(staged) => staged,
            None if upload.chunk.is_segment_crc32c() => StagedChunk::from_shared_crc32c_id(
                upload.operation_id.clone(),
                body,
                upload.chunk.id.clone(),
            )?,
            None => StagedChunk::from_shared(upload.operation_id.clone(), body),
        };
        if staged.chunk != upload.chunk {
            return Err(invalid("native upload changed its immutable object"));
        }
        let notify = pipeline.notify.clone();
        state.task = Some(pipeline.runtime.spawn_blocking(move || {
            let _permit = permit;
            let result = store
                .put_native_batch(vec![staged])
                .and_then(|mut receipts| {
                    if receipts.len() != 1 {
                        return Err(invalid("native upload returned the wrong receipt count"));
                    }
                    Ok(receipts.remove(0))
                });
            notify.notify_one();
            result
        }));
        task_table.push(upload.clone());
        Ok(true)
    }

    fn install_native_receipt(
        &self,
        upload: &Arc<NativeUpload>,
        result: Result<NativeChunkReceipt>,
    ) -> Result<Option<Error>> {
        let pipeline = self
            .native_pipeline
            .as_ref()
            .ok_or_else(|| unavailable("native write pipeline is missing"))?;
        // Do not move a successful receipt until every fallible structural lock
        // has been acquired. A broken installer must retain that identity and
        // the original body/reservation without advertising a healthy install.
        let mut result = Some(result);
        let installed: Result<Option<Error>> = (|| {
            let retained = result
                .as_ref()
                .ok_or_else(|| unavailable("native installer lost its retained result"))?;
            let failure = match retained {
                Ok(receipt)
                    if receipt.receipt.operation_id != upload.operation_id
                        || receipt.receipt.chunk != upload.chunk =>
                {
                    Some(invalid("native receipt differs from its immutable upload"))
                }
                Ok(receipt) => {
                    let actual = std::mem::size_of::<NativeUpload>()
                        + std::mem::size_of::<Mutex<NativeMetadataLease>>()
                        + 4 * std::mem::size_of::<usize>()
                        + upload.operation_id.0.capacity()
                        + upload.chunk.id.0.capacity()
                        + native_receipt_heap(&receipt.receipt);
                    let mut descriptor = upload
                        ._descriptor
                        .lock()
                        .map_err(|_| unavailable("native descriptor reservation is poisoned"))?;
                    let budget = descriptor.budget.clone();
                    let mut metadata = budget
                        .state
                        .lock()
                        .map_err(|_| unavailable("native metadata budget is poisoned"))?;
                    // Capacity refusal remains an owner-scoped business error.
                    descriptor.set_actual_locked(actual, &mut metadata).err()
                }
                Err(error) => Some(error.clone()),
            };
            let mut state = upload
                .state
                .lock()
                .map_err(|_| unavailable("native upload state is poisoned"))?;
            let mut metadata = pipeline
                .metadata
                .state
                .lock()
                .map_err(|_| unavailable("native metadata budget is poisoned"))?;
            state.joining = false;
            if let Some(error) = &failure {
                state.error = Some(error.clone());
                metadata.failed += 1;
            } else {
                let Some(Ok(receipt)) = result.take() else {
                    return Err(unavailable("native installer lost its validated receipt"));
                };
                if receipt.local_required_and_durable {
                    state.body = None;
                }
                state.receipt = Some(receipt);
                metadata.completed += 1;
            }
            upload.cv.notify_all();
            if failure.is_none() {
                pipeline.notify.notify_one();
            }
            Ok(failure)
        })();
        if let Err(error) = &installed {
            // Poison recovery is cleanup-only: never clear poison, release a
            // body/charge, or permit this upload to retry as healthy. A valid
            // receipt is diagnostic retention, not an installed success.
            let mut state = upload
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.joining = false;
            state.terminal_error = Some(error.clone());
            if let Some(Ok(receipt)) = result.take()
                && receipt.receipt.operation_id == upload.operation_id
                && receipt.receipt.chunk == upload.chunk
            {
                state.receipt = Some(receipt);
            }
            drop(state);
            if let Some(cell) = upload.owner.upgrade() {
                cell.lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .terminal_error = Some(error.clone());
                cell.notify_all();
            }
            upload.cv.notify_all();
            pipeline.notify.notify_one();
        }
        installed
    }

    fn poll_native_uploads(&self) -> Result<Option<Error>> {
        let Some(pipeline) = &self.native_pipeline else {
            return Ok(None);
        };
        let tasks: [Option<Arc<NativeUpload>>; 8] = {
            let tasks = pipeline
                .tasks
                .lock()
                .map_err(|_| unavailable("native upload task table is poisoned"))?;
            std::array::from_fn(|index| tasks.get(index).cloned())
        };
        let mut first_error = None;
        for upload in tasks.into_iter().flatten() {
            let task = {
                let mut state = upload
                    .state
                    .lock()
                    .map_err(|_| unavailable("native upload state is poisoned"))?;
                if state.task.as_ref().is_some_and(|task| task.is_finished()) {
                    state.joining = true;
                    state.task.take()
                } else {
                    None
                }
            };
            if let Some(task) = task {
                let result = pipeline
                    .runtime
                    .block_on(task)
                    .map_err(|error| unavailable(format!("native upload worker failed: {error}")))
                    .and_then(|result| result);
                let installed = self.install_native_receipt(&upload, result);
                // A consumed JoinHandle must leave the table even when receipt
                // installation encounters a structural error. This guard only
                // removes the joined slot; a poisoned table remains poisoned.
                match pipeline.tasks.lock() {
                    Ok(mut table) => table.retain(|other| !Arc::ptr_eq(other, &upload)),
                    Err(poison) => {
                        poison
                            .into_inner()
                            .retain(|other| !Arc::ptr_eq(other, &upload));
                        return Err(unavailable("native upload task table is poisoned"));
                    }
                }
                if let Some(error) = installed? {
                    if let Some(cell) = upload.owner.upgrade() {
                        let mut state = cell
                            .lock()
                            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                        let cursor = state
                            .background_error
                            .as_ref()
                            .map_or(1, |previous| previous.cursor.saturating_add(1));
                        state.background_error = Some(ObservedWriteError {
                            cursor,
                            error: error.clone(),
                        });
                        cell.notify_all();
                    }
                    first_error.get_or_insert(error);
                }
            }
        }
        self.observe_native_resources();
        // Business failures belong to their inode. A caller may have harvested
        // another inode's task; only drain consumes this Node-wide summary.
        Ok(first_error)
    }

    fn start_native_uploads(&self) -> Result<()> {
        while let Some(upload) = self.next_native_upload()? {
            if !self.start_native_upload(&upload)? {
                break;
            }
        }
        Ok(())
    }

    fn wait_native_upload(&self, upload: &Arc<NativeUpload>) -> Result<ChunkReceipt> {
        loop {
            let _ = self.poll_native_uploads()?;
            {
                let mut state = upload
                    .state
                    .lock()
                    .map_err(|_| unavailable("native upload state is poisoned"))?;
                if let Some(error) = &state.terminal_error {
                    return Err(error.clone());
                }
                if let Some(receipt) = &state.receipt {
                    return Ok(receipt.receipt.clone());
                }
                if let Some(error) = state.error.take() {
                    return Err(error);
                }
            }
            self.start_native_upload(upload)?;
            let state = upload
                .state
                .lock()
                .map_err(|_| unavailable("native upload state is poisoned"))?;
            if state.receipt.is_none() {
                drop(
                    upload
                        .cv
                        .wait_timeout(state, Duration::from_millis(10))
                        .map_err(|_| unavailable("native upload state is poisoned"))?,
                );
            }
        }
    }

    pub(crate) fn progress_native_writeback(&self) -> Result<usize> {
        if self.native_pipeline.is_none() {
            return Ok(0);
        }
        let _ = self.poll_native_uploads()?;
        self.start_native_uploads()?;
        let mut committed = 0;
        for (inode, cell) in self.inode_writes.snapshot()? {
            let due = {
                let state = cell
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                state.native_due || state.last_writer_background_requested
            };
            if due {
                match self.commit_inode(
                    &inode,
                    CommitReason::BACKGROUND,
                    CommitWait::AfterOperations,
                ) {
                    Ok(Some(seq)) => {
                        self.update_handles_after_commit(&inode, seq)?;
                        committed += 1;
                    }
                    Ok(None) => {}
                    Err(error) => self.record_background_error(&inode, error)?,
                }
            }
        }
        self.observe_native_resources();
        Ok(committed)
    }

    pub(crate) fn with_version_cache_capacity(self, capacity: usize) -> Self {
        let mut cache = self
            .readonly_version_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *cache = ReadOnlyVersionCache::new(capacity, cache.byte_limit);
        drop(cache);
        self
    }

    pub(crate) fn with_version_cache_bytes(self, bytes: u64) -> Self {
        let mut cache = self
            .readonly_version_cache
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *cache = ReadOnlyVersionCache::new(cache.capacity, usize::try_from(bytes).unwrap_or(0));
        drop(cache);
        self
    }

    pub fn with_write_buffer_pool(mut self, pool: Arc<WriteBufferPool>) -> Self {
        self.write_buffer_pool = pool;
        self
    }

    fn operation_id(&self, prefix: &str) -> OperationId {
        OperationId::new(crate::meta::store::timed_request_id(format!(
            "{}-{prefix}-{}",
            self.session_id,
            self.next_operation.fetch_add(1, Ordering::Relaxed)
        )))
    }

    fn validate_inode(&self, inode: InodeRecord) -> Result<InodeRecord> {
        if inode.namespace_id != self.namespace_id {
            return Err(invalid("Meta returned an inode from another DFS namespace"));
        }
        Ok(inode)
    }

    fn inode_size_and_blocks(&self, inode: &InodeRecord) -> Result<(u64, u64)> {
        match inode.head_version.as_ref() {
            Some(_) => {
                let cached = self.readonly_version_for_inode(inode)?;
                let length = cached.layout.file_length;
                Ok((
                    length,
                    allocated_blocks(&cached.layout, length, None, &DirtyExtentMap::default())?,
                ))
            }
            None if inode.kind == InodeKind::Symlink => Ok((
                inode
                    .symlink_target
                    .as_ref()
                    .map_or(0, |target| target.len() as u64),
                0,
            )),
            None => Ok((0, 0)),
        }
    }

    fn inode_id(&self, inode: BackendInode) -> Result<InodeId> {
        self.backend_to_inode
            .read(&inode.value)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .get(&inode.value)
            .cloned()
            .ok_or_else(|| stale("DFS mount inode is no longer known"))
    }

    fn backend_inode(&self, inode: &InodeId) -> Result<BackendInode> {
        if let Some(value) = self
            .inode_to_backend
            .read(inode)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .get(inode)
            .copied()
        {
            return Ok(BackendInode { value });
        }
        #[cfg(test)]
        {
            let barrier = self.backend_inode_after_missing.lock().unwrap().clone();
            if let Some(barrier) = barrier {
                barrier.wait();
            }
        }
        let _creation = self
            .inode_mapping_creation
            .lock()
            .map_err(|_| unavailable("DFS inode mapping creation lock is poisoned"))?;
        if let Some(value) = self
            .inode_to_backend
            .read(inode)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .get(inode)
            .copied()
        {
            return Ok(BackendInode { value });
        }
        let value = self.next_inode.fetch_add(1, Ordering::Relaxed);
        let mut forward = self
            .inode_to_backend
            .write(inode)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?;
        let mut reverse = self
            .backend_to_inode
            .write(&value)
            .map_err(|_| unavailable("DFS inode table is poisoned"))?;
        reverse.insert(value, inode.clone());
        forward.insert(inode.clone(), value);
        Ok(BackendInode { value })
    }

    fn directory_entry(&self, entry: DentryRecord) -> Result<DirectoryEntry> {
        let inode = self.validate_inode(entry.inode)?;
        let kind = file_kind(inode.kind);
        Ok(DirectoryEntry {
            name: OsString::from_vec(entry.name),
            inode: self.backend_inode(&inode.inode_id)?,
            kind,
            next_cookie: 0,
        })
    }

    fn allocate_handle(&self, mut handle: DfsFileHandle) -> Result<FileHandle> {
        let id = self.next_handle.fetch_add(1, Ordering::Relaxed);
        let inode_id = handle.inode_id.clone();
        if let Some(references) = &self.gc_references {
            // An inode-level reference protects every version of the inode,
            // including the one this handle pins until close.
            handle.gc_pin =
                Some(references.pin(&inode_id, handle.opened_inode.head_version.as_ref(), None)?);
        }
        let active = self.local_write_activity(&inode_id)?;
        self.handles.insert(id, handle, || {
            let dynamic = (active && self.write_state(&inode_id)?.is_some())
                || self.current_remote_provider(&inode_id)?.is_some();
            #[cfg(test)]
            {
                let hook = self
                    .handle_registration_after_route_check
                    .lock()
                    .unwrap()
                    .take();
                if let Some(hook) = hook {
                    hook();
                }
            }
            Ok(dynamic)
        })?;
        Ok(FileHandle(id))
    }

    fn load_version(
        &self,
        version_id: Option<&FileVersionId>,
    ) -> Result<(Option<FileVersion>, LayoutRoot)> {
        let Some(version_id) = version_id else {
            return Ok((
                None,
                LayoutRoot {
                    id: LayoutRootId::new("empty"),
                    file_length: 0,
                    inline_extents: Vec::new(),
                },
            ));
        };
        self.meta
            .get_file_version(version_id)
            .map(|(version, layout)| (Some(version), layout))
    }

    fn load_readonly_version(
        &self,
        version_id: Option<&FileVersionId>,
    ) -> Result<Arc<ReadOnlyVersion>> {
        let Some(version_id) = version_id else {
            return Ok(self.readonly_empty_version.clone());
        };
        let span = tracing::info_span!("node.dfs.read.head", version_id = %version_id.0,
            branch = "readonly_version", cached = tracing::field::Empty);
        record_metadata_table_lock();
        if let Some(cached) = self
            .readonly_version_cache
            .lock()
            .map_err(|_| unavailable("DFS readonly version cache is poisoned"))?
            .get(version_id)
            .cloned()
        {
            span.record("cached", true);
            return Ok(cached);
        }
        span.record("cached", false);
        // No cache guard crosses Meta RPC, validation or index construction.
        let (version, layout) = span.in_scope(|| self.meta.get_file_version(version_id))?;
        let cached = Arc::new(ReadOnlyVersion::checked(version_id, version, layout)?);
        #[cfg(test)]
        {
            let barrier = self.readonly_version_before_install.lock().unwrap().clone();
            if let Some(barrier) = barrier {
                barrier.wait();
            }
        }
        record_metadata_table_lock();
        let mut cache = self
            .readonly_version_cache
            .lock()
            .map_err(|_| unavailable("DFS readonly version cache is poisoned"))?;
        if let Some(existing) = cache.get(version_id) {
            if existing.version != cached.version || existing.layout != cached.layout {
                return Err(invalid(
                    "DFS immutable version changed during concurrent cache admission",
                ));
            }
            return Ok(existing.clone());
        }
        cache.insert(version_id.clone(), cached.clone());
        Ok(cached)
    }

    /// Admit this Node's committed write base into the readonly version cache
    /// when it is the version a reader is about to pin, so reading back a file
    /// this Node just wrote costs no extra GetFileVersion.
    fn seed_readonly_version_from_write_state(&self, inode: &InodeRecord) -> Result<()> {
        let Some(version_id) = inode.head_version.as_ref() else {
            return Ok(());
        };
        let Some(state) = self.write_state(&inode.inode_id)? else {
            return Ok(());
        };
        let (version, layout) = {
            let state = state
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            match state.base_version.as_ref() {
                Some(version) if &version.id == version_id => {
                    (version.clone(), state.base_layout.clone())
                }
                _ => return Ok(()),
            }
        };
        record_metadata_table_lock();
        let mut cache = self
            .readonly_version_cache
            .lock()
            .map_err(|_| unavailable("DFS readonly version cache is poisoned"))?;
        if cache.get(version_id).is_none() {
            let cached = Arc::new(ReadOnlyVersion::checked(version_id, version, layout)?);
            cache.insert(version_id.clone(), cached);
        }
        Ok(())
    }

    fn readonly_version_for_inode(&self, inode: &InodeRecord) -> Result<Arc<ReadOnlyVersion>> {
        self.seed_readonly_version_from_write_state(inode)?;
        let cached = self.load_readonly_version(inode.head_version.as_ref())?;
        if cached
            .version
            .as_ref()
            .is_some_and(|version| version.inode_id != inode.inode_id)
        {
            return Err(invalid("DFS readonly version belongs to a different inode"));
        }
        Ok(cached)
    }

    fn write_state(&self, inode_id: &InodeId) -> Result<Option<SharedInodeWriteState>> {
        Ok(self
            .inode_writes
            .read(inode_id)
            .map_err(|_| unavailable("DFS inode write table is poisoned"))?
            .get(inode_id)
            .cloned())
    }

    fn write_state_can_adopt_fresh_lease(state: &InodeWriteState) -> bool {
        state.open_writers == 0
            && !state.dirty
            && !state.metadata_dirty
            && !state.kill_suidgid_dirty
            && state.dirty_extents.is_empty()
            && state.in_flight.is_none()
            && !state.commit_busy
            && !state.operation_busy
            && !state.last_writer_background_requested
            && state.background_error.is_none()
            && state.terminal_error.is_none()
    }

    fn write_state_can_rebind_fresh_lease_preserving_metadata(state: &InodeWriteState) -> bool {
        state.open_writers == 0
            && !state.dirty
            && state.metadata_dirty
            && !state.kill_suidgid_dirty
            && state.dirty_extents.is_empty()
            && state.in_flight.is_none()
            && !state.commit_busy
            && !state.operation_busy
            && state.background_error.is_none()
            && state.terminal_error.is_none()
    }

    fn write_state_can_refresh_committed_view(state: &InodeWriteState) -> bool {
        state.open_writers == 0
            && !state.dirty
            && !state.kill_suidgid_dirty
            && state.dirty_extents.is_empty()
            && state.in_flight.is_none()
            && !state.commit_busy
            && !state.operation_busy
            && !state.last_writer_background_requested
            && state.background_error.is_none()
            && state.terminal_error.is_none()
    }

    fn local_write_state_protects_remote_provider(&self, inode_id: &InodeId) -> Result<bool> {
        let Some(state) = self.write_state(inode_id)? else {
            return Ok(false);
        };
        let state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        Ok(!Self::write_state_can_adopt_fresh_lease(&state))
    }

    fn remote_provider_visible_for_reads(
        &self,
        inode_id: &InodeId,
    ) -> Result<Option<RemoteDfsProviderIo>> {
        if self.local_write_state_protects_remote_provider(inode_id)? {
            return Ok(None);
        }
        for _ in 0..2 {
            let Some(remote) = self.current_remote_provider(inode_id)? else {
                return Ok(None);
            };
            if let Some(guard) = remote.lifecycle.begin_io()? {
                return Ok(Some(RemoteDfsProviderIo {
                    remote,
                    _guard: guard,
                }));
            }
        }
        Ok(None)
    }

    fn write_state_should_reacquire_clean_lease(cell: &SharedInodeWriteState) -> Result<bool> {
        let state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        Ok((Self::write_state_can_adopt_fresh_lease(&state)
            || Self::write_state_can_rebind_fresh_lease_preserving_metadata(&state))
            && should_renew(&state.write_lease))
    }

    fn incoming_write_lease_is_older(state: &InodeWriteState, incoming: &WriteLease) -> bool {
        state.write_lease.inode_id == incoming.inode_id
            && state.write_lease.owner_node_id == incoming.owner_node_id
            && state.write_lease.owner_session_id == incoming.owner_session_id
            && incoming.lease_epoch < state.write_lease.lease_epoch
    }

    fn apply_clean_fresh_write_state(
        state: &mut InodeWriteState,
        inode: InodeRecord,
        write_lease: WriteLease,
        base_version: Option<FileVersion>,
        base_layout: LayoutRoot,
        logical_length: u64,
    ) {
        state.inode = inode;
        state.write_lease = write_lease;
        state.base_version = base_version;
        state.base_layout = base_layout;
        state.logical_length = logical_length;
    }

    fn apply_metadata_dirty_fresh_write_lease(
        state: &mut InodeWriteState,
        inode: &InodeRecord,
        write_lease: WriteLease,
        base_version: Option<FileVersion>,
        base_layout: LayoutRoot,
        logical_length: u64,
    ) -> Result<()> {
        if state.inode.revision != inode.revision || state.inode.head_version != inode.head_version
        {
            return Err(stale(
                "DFS metadata-dirty write state cannot adopt a changed file head",
            ));
        }
        state.write_lease = write_lease;
        state.base_version = base_version;
        state.base_layout = base_layout;
        state.logical_length = logical_length;
        Ok(())
    }

    fn apply_fresh_committed_view(
        state: &mut InodeWriteState,
        inode: InodeRecord,
        base_version: Option<FileVersion>,
        base_layout: LayoutRoot,
        logical_length: u64,
    ) {
        state.inode = inode;
        state.base_version = base_version;
        state.base_layout = base_layout;
        state.logical_length = logical_length;
        state.metadata_dirty = false;
    }

    fn refresh_retained_committed_view(&self, inode: &InodeRecord) -> Result<()> {
        let Some(existing) = self.write_state(&inode.inode_id)? else {
            return Ok(());
        };
        {
            let state = existing
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            if inode.revision < state.inode.revision {
                return Ok(());
            }
            if state.inode.revision == inode.revision
                && state.inode.head_version == inode.head_version
            {
                return Ok(());
            }
            if !Self::write_state_can_refresh_committed_view(&state) {
                return Ok(());
            }
        }
        let (base_version, base_layout) = self.load_version(inode.head_version.as_ref())?;
        let logical_length = base_version.as_ref().map_or(0, |version| version.length);
        let mut state = existing
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        if inode.revision < state.inode.revision {
            return Ok(());
        }
        if state.inode.revision == inode.revision && state.inode.head_version == inode.head_version
        {
            return Ok(());
        }
        if !Self::write_state_can_refresh_committed_view(&state) {
            return Ok(());
        }
        Self::apply_fresh_committed_view(
            &mut state,
            inode.clone(),
            base_version,
            base_layout,
            logical_length,
        );
        existing.notify_all();
        Ok(())
    }

    fn install_write_state(&self, inode: InodeRecord, write_lease: WriteLease) -> Result<()> {
        self.ensure_local_write_owner(&write_lease)?;
        if let Some(existing) = self.write_state(&inode.inode_id)? {
            {
                let mut state = existing
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                if same_lease_identity(&state.write_lease, &write_lease) {
                    state.write_lease = merge_same_identity_lease(&state.write_lease, write_lease);
                    return Ok(());
                }
                if Self::incoming_write_lease_is_older(&state, &write_lease) {
                    return Err(stale("DFS write state already holds a newer lease epoch"));
                }
                if !(Self::write_state_can_adopt_fresh_lease(&state)
                    || Self::write_state_can_rebind_fresh_lease_preserving_metadata(&state))
                {
                    return Err(stale(
                        "DFS write state has pending local changes under an older lease",
                    ));
                }
            }
            let (base_version, base_layout) = self.load_version(inode.head_version.as_ref())?;
            let logical_length = base_version.as_ref().map_or(0, |version| version.length);
            let mut state = existing
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            if same_lease_identity(&state.write_lease, &write_lease) {
                state.write_lease = merge_same_identity_lease(&state.write_lease, write_lease);
                return Ok(());
            }
            if Self::incoming_write_lease_is_older(&state, &write_lease) {
                return Err(stale("DFS write state already holds a newer lease epoch"));
            }
            if Self::write_state_can_adopt_fresh_lease(&state) {
                Self::apply_clean_fresh_write_state(
                    &mut state,
                    inode,
                    write_lease,
                    base_version,
                    base_layout,
                    logical_length,
                );
            } else if Self::write_state_can_rebind_fresh_lease_preserving_metadata(&state) {
                Self::apply_metadata_dirty_fresh_write_lease(
                    &mut state,
                    &inode,
                    write_lease,
                    base_version,
                    base_layout,
                    logical_length,
                )?;
            } else {
                return Err(stale(
                    "DFS write state became dirty before adopting a fresh lease",
                ));
            }
            existing.notify_all();
            return Ok(());
        }
        let (base_version, base_layout) = self.load_version(inode.head_version.as_ref())?;
        let logical_length = base_version.as_ref().map_or(0, |version| version.length);
        let state = Arc::new(
            InodeWriteStateCell::new(InodeWriteState {
                write_lease,
                base_version,
                base_layout,
                logical_length,
                metadata_dirty: false,
                kill_suidgid_dirty: false,
                dirty_extents: DirtyExtentMap::default(),
                in_flight: None,
                commit_busy: false,
                operation_busy: false,
                native_queue: NativeTicketQueue::default(),
                native_thread: None,
                native_owner: NativeWriteOwner::new(),
                native_due: false,
                native_due_through: 0,
                inode: inode.clone(),
                dirty: false,
                next_write_seq: 0,
                visible_write_seq: 0,
                durable_write_seq: 0,
                committed_write_seq: 0,
                open_writers: 0,
                last_writer_background_requested: false,
                background_error: None,
                terminal_error: None,
            })
            .with_lock_wait(self.inode_lock_wait.clone()),
        );
        self.inode_writes
            .write(&inode.inode_id)
            .map_err(|_| unavailable("DFS inode write table is poisoned"))?
            .entry(inode.inode_id.clone())
            .or_insert(state);
        self.handles.mark_dynamic(&inode.inode_id)?;
        Ok(())
    }

    fn ensure_inode_write_state(&self, inode: &InodeRecord) -> Result<SharedInodeWriteState> {
        let state = match self.write_state(&inode.inode_id)? {
            Some(state) => {
                if Self::write_state_should_reacquire_clean_lease(&state)? {
                    let (fresh_inode, write_lease) = self.meta.open_write(&inode.inode_id)?;
                    let fresh_inode = self.validate_inode(fresh_inode)?;
                    self.install_write_state(fresh_inode, write_lease)?;
                }
                state
            }
            None => {
                let (fresh_inode, write_lease) = self.meta.open_write(&inode.inode_id)?;
                let fresh_inode = self.validate_inode(fresh_inode)?;
                self.install_write_state(fresh_inode, write_lease)?;
                self.write_state(&inode.inode_id)?
                    .ok_or_else(|| unavailable("DFS write state was not installed"))?
            }
        };
        Ok(state)
    }

    pub fn with_remote_owner_factory(mut self, factory: Arc<dyn DfsRemoteOwnerFactory>) -> Self {
        self.remote_owner_factory = Some(factory);
        self
    }

    fn open_write_session(&self, inode: &InodeRecord, open_flags: i32) -> Result<DfsWriteSession> {
        let state = self.ensure_inode_write_state(inode)?;
        let mut state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        if should_renew(&state.write_lease) {
            state.write_lease = self.meta.renew_write_lease(state.write_lease.clone())?;
        }
        state.open_writers = state.open_writers.saturating_add(1);
        state.last_writer_background_requested = false;
        let session_id = DfsWriteSessionId::new(format!(
            "{}-write-session-{}",
            self.session_id,
            self.next_operation.fetch_add(1, Ordering::Relaxed)
        ));
        let session = write_session(&state, open_flags, session_id);
        let inode = state.inode.clone();
        drop(state);
        // A local writer starts write activity: open readers move to the view
        // its writes build on, so they never mix an older pinned version with
        // this Node's new data (kernel page cache included).
        self.handles.advance_to_local_view(&inode)?;
        Ok(session)
    }

    fn visible_attributes(&self, inode: &InodeRecord) -> Result<FileAttributes> {
        self.visible_attributes_with_observed_inode(inode, inode)
    }

    fn visible_attributes_with_observed_inode(
        &self,
        inode: &InodeRecord,
        observed: &InodeRecord,
    ) -> Result<FileAttributes> {
        let Some(state) = self.write_state(&inode.inode_id)? else {
            let (size, blocks) = self.inode_size_and_blocks(observed)?;
            return Ok(attributes(observed, size, blocks));
        };
        let state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        let frozen = state
            .in_flight
            .as_ref()
            .and_then(InFlightCommit::dirty_extents);
        let blocks = allocated_blocks(
            &state.base_layout,
            state.logical_length,
            frozen.as_ref(),
            &state.dirty_extents,
        )?;
        let mut view = observed.clone();
        if state.metadata_dirty {
            view.attributes.atime_unix_ms = state.inode.attributes.atime_unix_ms;
            view.attributes.mtime_unix_ms = state.inode.attributes.mtime_unix_ms;
            view.attributes.ctime_unix_ms = view
                .attributes
                .ctime_unix_ms
                .max(state.inode.attributes.ctime_unix_ms);
        }
        if state.kill_suidgid_dirty {
            view.attributes.mode = state.inode.attributes.mode;
        }
        Ok(attributes(&view, state.logical_length, blocks))
    }

    fn clear_kernel_write_privileges(mode: u32) -> u32 {
        let mut cleared = mode & !0o4000;
        if mode & 0o010 != 0 {
            cleared &= !0o2000;
        }
        cleared
    }

    fn apply_kernel_killpriv_to_state(state: &mut InodeWriteState) {
        let cleared = Self::clear_kernel_write_privileges(state.inode.attributes.mode);
        if cleared != state.inode.attributes.mode {
            state.inode.attributes.mode = cleared;
            state.inode.attributes.ctime_unix_ms = now_unix_ms();
            state.metadata_dirty = true;
            state.kill_suidgid_dirty = true;
        }
    }

    fn has_pending_killpriv(&self, inode_id: &InodeId) -> Result<bool> {
        let Some(state) = self.write_state(inode_id)? else {
            return Ok(false);
        };
        let state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        Ok(state.kill_suidgid_dirty)
    }

    fn observe_inode_record(&self, inode: InodeRecord) -> Result<InodeRecord> {
        let inode = self.validate_inode(inode)?;
        // Updates only the base new opens and writers start from; open
        // handles keep their pinned version (decision 4a).
        self.refresh_retained_committed_view(&inode)?;
        Ok(inode)
    }

    fn refresh_inode_record(
        &self,
        inode: InodeRecord,
        preserve_dirty_atime: bool,
        preserve_dirty_mtime: bool,
    ) -> Result<InodeRecord> {
        let mut inode = self.validate_inode(inode)?;
        self.refresh_retained_committed_view(&inode)?;
        if let Some(state) = self.write_state(&inode.inode_id)? {
            let mut state = state
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            if state.metadata_dirty {
                if preserve_dirty_atime {
                    inode.attributes.atime_unix_ms = state.inode.attributes.atime_unix_ms;
                }
                if preserve_dirty_mtime {
                    inode.attributes.mtime_unix_ms = state.inode.attributes.mtime_unix_ms;
                }
                inode.attributes.ctime_unix_ms = inode
                    .attributes
                    .ctime_unix_ms
                    .max(state.inode.attributes.ctime_unix_ms);
            }
            if state.kill_suidgid_dirty {
                inode.attributes.mode = state.inode.attributes.mode;
            }
            state.inode = inode.clone();
        }
        // Open handles keep their pinned version (decision 4a).
        Ok(inode)
    }

    fn capture_native_read_view(&self) -> Result<NativeReadView> {
        let started = Instant::now();
        let authority = self.read_engine.capture_native_read_authority()?;
        let deadline = authority
            .expires_at()
            .map_or(started + NATIVE_READ_VIEW_TTL, |expiry| {
                expiry.min(started + NATIVE_READ_VIEW_TTL)
            });
        Ok(NativeReadView {
            generation: 0,
            authority,
            deadline,
        })
    }

    fn check_native_read_view(&self, view: &NativeReadView) -> Result<()> {
        self.read_engine
            .check_native_read_authority(&view.authority)?;
        if Instant::now() >= view.deadline {
            return Err(unavailable("DFS native read view expired before delivery"));
        }
        Ok(())
    }

    fn invalidate_native_inode_views_on_error(&self, inode_id: &InodeId, error: &Error) {
        if matches!(
            error.kind(),
            afs_error::ErrorKind::PermissionDenied | afs_error::ErrorKind::Unauthenticated
        ) {
            // Preserve the originating failure. A poisoned handle table also
            // prevents any later read from reusing the rejected context.
            if let Ok(handles) = self.handles.for_inode(inode_id) {
                for handle in handles {
                    if let Ok(mut current) = handle.state.lock() {
                        current.native_read_view = None;
                        current.opened_page_identity = None;
                        current.native_read_generation =
                            current.native_read_generation.saturating_add(1);
                        current.native_denial_serial =
                            current.native_denial_serial.saturating_add(1);
                    }
                }
            }
        }
    }

    fn invalidate_native_handle_view_on_error(&self, handle: FileHandle, error: &Error) {
        if matches!(
            error.kind(),
            afs_error::ErrorKind::PermissionDenied | afs_error::ErrorKind::Unauthenticated
        ) && let Ok(Some(handle)) = self.handles.get(handle.0)
            && let Ok(mut current) = handle.state.lock()
        {
            current.native_read_view = None;
            current.opened_page_identity = None;
            // An exhausted generation can never be used by a new view.
            current.native_read_generation = current.native_read_generation.saturating_add(1);
            current.native_denial_serial = current.native_denial_serial.saturating_add(1);
        }
    }

    #[cfg(test)]
    fn native_inode_for_read(
        &self,
        handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
    ) -> PinnedReadView {
        let mut result = None;
        self.read_engine.run_sync_boundary(Box::pin(async {
            result = Some(self.native_inode_for_read_async(handle, snapshot).await?);
            Ok(())
        }))?;
        result.ok_or_else(|| unavailable("read view produced no result"))
    }

    /// The handle's pinned version and a current Node read-authority view.
    /// Other nodes' commits reach a handle only through a new open (decision
    /// 4a), so renewing an expired view is local and never refetches the inode.
    async fn native_inode_for_read_async(
        &self,
        _handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
    ) -> PinnedReadView {
        if snapshot.native_read_generation == u64::MAX {
            return Err(unavailable("DFS native read generation exhausted"));
        }
        if let Some(view) = snapshot.native_read_view.as_ref()
            && view.deadline > Instant::now()
        {
            self.check_native_read_view(view)?;
            return Ok((snapshot.opened_inode.clone(), view.clone()));
        }
        let opened = snapshot.handle.clone();
        if opened.closed.load(Ordering::Acquire) {
            return Err(stale("DFS file handle is no longer open"));
        }
        let mut view = self.capture_native_read_view()?;
        let mut current = opened
            .state
            .lock()
            .map_err(|_| unavailable("DFS handle state is poisoned"))?;
        if current.native_denial_serial != snapshot.native_denial_serial {
            return Err(unavailable(
                "DFS native read authorization changed since the read started",
            ));
        }
        if let Some(installed) = current.native_read_view.as_ref()
            && installed.deadline > Instant::now()
        {
            let result = (current.opened_inode.clone(), installed.clone());
            drop(current);
            self.check_native_read_view(&result.1)?;
            return Ok(result);
        }
        current.native_read_generation = current
            .native_read_generation
            .checked_add(1)
            .ok_or_else(|| unavailable("DFS native read generation exhausted"))?;
        view.generation = current.native_read_generation;
        current.native_read_view = Some(view.clone());
        Ok((current.opened_inode.clone(), view))
    }

    fn read_native_owned(
        &self,
        handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
        offset: u64,
        size: usize,
    ) -> Result<prost::bytes::Bytes> {
        let mut result = None;
        let deadline = Instant::now() + self.read_engine.read_pipeline_timeout();
        self.read_engine.run_sync_boundary(Box::pin(async {
            result = Some(
                self.read_native_bytes_async(
                    handle,
                    snapshot,
                    offset,
                    size,
                    deadline,
                    Arc::new(()),
                )
                .await?,
            );
            Ok(())
        }))?;
        result.ok_or_else(|| unavailable("synchronous read boundary produced no result"))
    }

    fn read_visible_from_handle_inode(
        &self,
        inode_id: &InodeId,
        opened_inode: Option<&InodeRecord>,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        if let Some(state) = self.write_state(inode_id)? {
            let view = {
                let state = state
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                // With no local activity left, a pinned handle reads its own
                // version below (4a).
                if !state.has_local_write_activity() && opened_inode.is_some() {
                    None
                } else {
                    Some(ResolvedReadView {
                        committed: state
                            .base_version
                            .as_ref()
                            .map(|version| version.id.clone()),
                        length: state.logical_length,
                        layout: state.base_layout.clone(),
                        frozen: state
                            .in_flight
                            .as_ref()
                            .and_then(InFlightCommit::dirty_extents),
                        active: state.dirty_extents.snapshot_range(offset, out.len())?,
                    })
                }
            };
            if let Some(view) = view {
                return self.read_visible_resolved(view, offset, out);
            }
        }

        if let Some(opened_inode) = opened_inode
            && let Some(count) = self.try_read_external_version(opened_inode, offset, out)?
        {
            return Ok(count);
        }

        let inode = self.validate_inode(
            tracing::info_span!(
                "node.dfs.read.head",
                inode_id = %inode_id.0,
                branch = "inode",
                cached = false,
            )
            .in_scope(|| self.meta.get_inode(inode_id))?,
        )?;
        self.read_committed_inode(&inode, offset, out)
    }

    fn read_committed_inode(
        &self,
        inode: &InodeRecord,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        let cached = self.readonly_version_for_inode(inode)?;
        let length = cached.layout.file_length;
        if offset >= length || out.is_empty() {
            return Ok(0);
        }
        let count = usize::try_from((length - offset).min(out.len() as u64))
            .map_err(|_| invalid("read length is too large"))?;
        out[..count].fill(0);
        let batch = cached.read_batch(offset, count, self.read_engine.source_lookahead_chunks())?;
        if self.read_engine.has_external_backend() {
            self.read_engine.read_batch(&batch, &mut out[..count])?;
        } else {
            self.read_engine
                .read_batch_native_into_private(&batch, &mut out[..count])?;
        }
        Ok(count)
    }

    #[cfg(test)]
    fn read_committed_inode_owned(
        &self,
        inode: &InodeRecord,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        self.read_committed_inode_owned_hint(inode, offset, size, &mut None, None)
            .map(crate::node::read_buffer::ReadBuffer::into_vec)
    }

    #[cfg(test)]
    fn read_committed_inode_owned_hint(
        &self,
        inode: &InodeRecord,
        offset: u64,
        size: usize,
        hint: &mut Option<ReadBatch>,
        deadline: Option<Instant>,
    ) -> Result<crate::node::read_buffer::ReadBuffer> {
        let cached = self.readonly_version_for_inode(inode)?;
        let length = cached.layout.file_length;
        if offset >= length || size == 0 {
            return Ok(Vec::new().into());
        }
        let count = usize::try_from((length - offset).min(size as u64))
            .map_err(|_| invalid("read length is too large"))?;
        let batch = cached.read_batch(offset, count, self.read_engine.source_lookahead_chunks())?;
        if let Some(result) = self
            .read_engine
            .read_batch_native_open_owned_before(&batch, count, deadline)?
        {
            if self.read_engine.readahead_object_limit() != 0 {
                *hint = Some(batch);
            }
            return Ok(result);
        }
        let mut result = vec![0; count];
        self.read_engine
            .read_batch_native_open_into_private_before(&batch, &mut result, deadline)?;
        if self.read_engine.readahead_object_limit() != 0 {
            *hint = Some(batch);
        }
        Ok(result.into())
    }

    fn read_visible_resolved(
        &self,
        view: ResolvedReadView,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        if offset >= view.length || out.is_empty() {
            return Ok(0);
        }
        let count = usize::try_from((view.length - offset).min(out.len() as u64))
            .map_err(|_| invalid("read length is too large"))?;
        out[..count].fill(0);
        self.read_layout_range(
            view.committed.as_ref(),
            &view.layout,
            offset,
            &mut out[..count],
        )?;
        if let Some(frozen) = view.frozen {
            frozen.overlay_native(offset, &mut out[..count], Some(self.chunk_store.as_ref()))?;
        }
        view.active
            .overlay_native(offset, &mut out[..count], Some(self.chunk_store.as_ref()))?;
        Ok(count)
    }

    fn try_read_external_version(
        &self,
        opened_inode: &InodeRecord,
        offset: u64,
        out: &mut [u8],
    ) -> Result<Option<usize>> {
        let opened_inode = self.validate_inode(opened_inode.clone())?;
        if !self.read_engine.has_external_backend() {
            return Ok(None);
        }
        if opened_inode.head_version.is_none() {
            return Ok(None);
        }
        let cached = self.readonly_version_for_inode(&opened_inode)?;
        let Some(version) = cached.version.as_ref() else {
            return Ok(None);
        };
        let length = version.length;
        if offset >= length || out.is_empty() {
            return Ok(Some(0));
        }
        let count = usize::try_from((length - offset).min(out.len() as u64))
            .map_err(|_| invalid("read length is too large"))?;
        match self.read_layout_range_external_version(
            &opened_inode.inode_id,
            &version.id,
            &cached,
            offset,
            &mut out[..count],
        )? {
            CurrentExternalReadOutcome::Read => Ok(Some(count)),
            CurrentExternalReadOutcome::Unsupported => Ok(None),
            CurrentExternalReadOutcome::HeadChanged(_) => Err(invalid(
                "external version read must not use current-head refresh replies",
            )),
        }
    }

    fn try_read_external_version_owned(
        &self,
        opened_inode: &InodeRecord,
        offset: u64,
        size: usize,
    ) -> Result<Option<Vec<u8>>> {
        let opened_inode = self.validate_inode(opened_inode.clone())?;
        if !self.read_engine.has_external_backend() {
            return Ok(None);
        }
        if opened_inode.head_version.is_none() {
            return Ok(None);
        }
        let cached = self.readonly_version_for_inode(&opened_inode)?;
        let Some(version) = cached.version.as_ref() else {
            return Ok(None);
        };
        let length = version.length;
        if offset >= length || size == 0 {
            return Ok(Some(Vec::new()));
        }
        let count = usize::try_from((length - offset).min(size as u64))
            .map_err(|_| invalid("read length is too large"))?;
        self.read_layout_range_external_version_owned(
            &opened_inode.inode_id,
            &version.id,
            &cached,
            offset,
            count,
        )
    }

    fn layout_read_ops(
        &self,
        layout: &LayoutRoot,
        offset: u64,
        length: usize,
    ) -> Result<Vec<ChunkReadOp>> {
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| invalid("read range overflow"))?;
        let mut ops = Vec::new();
        for extent in &layout.inline_extents {
            if let Some(op) = extent_read_op(extent, offset, end)? {
                ops.push(op);
            }
        }
        Ok(ops)
    }

    fn read_layout_range_external_version(
        &self,
        inode_id: &InodeId,
        committed: &FileVersionId,
        cached: &ReadOnlyVersion,
        offset: u64,
        out: &mut [u8],
    ) -> Result<CurrentExternalReadOutcome> {
        let ops = cached.ops(offset, out.len())?;
        let covered = ops
            .iter()
            .fold(0_u64, |sum, op| sum.saturating_add(op.length));
        if covered != out.len() as u64 || ops.is_empty() {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        }
        self.read_engine.read_batch_external_version(
            inode_id,
            &ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(committed.clone()),
                layout_root_id: cached.layout.id.clone(),
                ops,
            },
            out,
        )
    }

    fn read_layout_range_external_version_owned(
        &self,
        inode_id: &InodeId,
        committed: &FileVersionId,
        cached: &ReadOnlyVersion,
        offset: u64,
        length: usize,
    ) -> Result<Option<Vec<u8>>> {
        if length == 0 {
            return Ok(Some(Vec::new()));
        }
        let ops = cached.ops(offset, length)?;
        self.read_engine.read_batch_external_version_owned(
            inode_id,
            &ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(committed.clone()),
                layout_root_id: cached.layout.id.clone(),
                ops,
            },
            length,
        )
    }

    fn read_layout_range(
        &self,
        committed: Option<&FileVersionId>,
        layout: &LayoutRoot,
        offset: u64,
        out: &mut [u8],
    ) -> Result<()> {
        let ops = self.layout_read_ops(layout, offset, out.len())?;
        let end = offset
            .checked_add(out.len() as u64)
            .ok_or_else(|| invalid("read range overflow"))?;
        let batch = ReadBatch {
            lookahead: if committed.is_some() {
                source_lookahead_ids(
                    layout
                        .inline_extents
                        .iter()
                        .filter(|extent| extent.file_offset >= end),
                    &ops,
                    self.read_engine.source_lookahead_chunks(),
                )
            } else {
                Vec::new()
            },
            file_version_id: committed.cloned(),
            layout_root_id: layout.id.clone(),
            ops,
        };
        if self.read_engine.has_external_backend() {
            self.read_engine.read_batch(&batch, out)?;
        } else {
            // Every native caller supplies its unpublished final Vec. Avoid
            // allocating another aggregate stage before the outer checks.
            self.read_engine
                .read_batch_native_into_private(&batch, out)?;
        }
        Ok(())
    }

    fn native_read_capability(&self, handle: FileHandle) -> Option<Arc<DfsHandle>> {
        if self.read_engine.has_external_backend() {
            return None;
        }
        let opened = self.handles.get(handle.0).ok()??;
        if opened.flags & libc::O_ACCMODE != libc::O_RDONLY
            || opened.flags & libc::O_PATH != 0
            || opened.owner_scope.is_some()
            || opened.closed.load(Ordering::Acquire)
            || opened.dynamic_read_route.load(Ordering::Acquire)
        {
            return None;
        }
        {
            let state = opened.state.lock().ok()?;
            if state.write_session.is_some() || state.opened_inode.kind != InodeKind::Regular {
                return None;
            }
            // A stale optimization view must be refreshed in the async route,
            // never select a worker-blocking network implementation.
            self.check_native_read_view(state.native_read_view.as_ref()?)
                .ok()?;
        }
        Some(opened)
    }

    fn opened_page_identity(&self, inode: &InodeRecord) -> Result<PageCacheIdentity> {
        let cached = self.readonly_version_for_inode(inode)?;
        Ok(PageCacheIdentity {
            version_id: cached.version.as_ref().map(|version| version.id.0.clone()),
            length: cached.layout.file_length,
        })
    }

    fn handle_snapshot(&self, handle: FileHandle) -> Result<DfsFileHandleSnapshot> {
        let handle = self
            .handles
            .get(handle.0)?
            .ok_or_else(|| stale("DFS file handle is no longer open"))?;
        DfsFileHandleSnapshot::from_handle(&handle)
    }

    /// Barrier: commit until every write this handle's inode had accepted when
    /// the call started is committed (and, for fsync, its metadata synced).
    /// Built from repeated `commit_inode` steps.
    fn sync_handle(
        &self,
        handle: FileHandle,
        reason: CommitReason,
        wait: CommitWait,
    ) -> Result<()> {
        let wait_for_operation = wait.waits_for_operations();
        self.observe_handle_error(handle)?;
        let snapshot = self.handle_snapshot(handle)?;
        let Some(state) = self.write_state(&snapshot.inode_id)? else {
            // A read-only open without a local writer has no accepted writes
            // to publish. With a local writer, fsync is an inode-wide barrier.
            return if snapshot.write_session.is_none() {
                Ok(())
            } else {
                Err(stale("DFS write state is no longer open"))
            };
        };
        let _native_ticket = if wait_for_operation {
            self.native_ticket_with_cleanup(&state, true)?
        } else {
            None
        };
        let _operation = if wait_for_operation && self.native_pipeline.is_none() {
            Some(self.begin_inode_operation(&snapshot.inode_id, state.clone())?)
        } else {
            None
        };
        let target_seq = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?
            .visible_write_seq;
        loop {
            let synced = state
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?
                .committed_write_seq;
            let metadata_pending =
                reason.full_metadata_barrier() && self.needs_full_metadata_sync(&state)?;
            if synced >= target_seq && !metadata_pending {
                break;
            }
            let committed_seq =
                self.commit_inode(&snapshot.inode_id, reason, CommitWait::InsideOperation)?;
            if let Some(committed_seq) = committed_seq {
                self.update_handles_after_commit(&snapshot.inode_id, committed_seq)?;
                continue;
            }
            let committed_seq = state
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?
                .committed_write_seq;
            if committed_seq >= target_seq {
                self.update_handles_after_commit(&snapshot.inode_id, committed_seq)?;
                continue;
            }
            if target_seq == 0 {
                break;
            }
            return Err(unavailable(
                "DFS sync made no progress toward the accepted write sequence",
            ));
        }
        Ok(())
    }

    /// The single commit step: send at most one pending commit for this inode
    /// and return the committed write sequence, or `None` when there was nothing
    /// to do (or a background commit found the inode busy).
    fn commit_inode(
        &self,
        inode_id: &InodeId,
        reason: CommitReason,
        wait: CommitWait,
    ) -> Result<Option<u64>> {
        let wait_for_operation = wait.waits_for_operations();
        let commit_span = tracing::info_span!(
            "node.dfs.commit",
            reason = reason.label(),
            inode_id = %inode_id.0,
            wait_for_operation,
            branch = tracing::field::Empty,
        );
        let _entered = commit_span.enter();
        let Some(cell) = self.write_state(inode_id)? else {
            return Ok(None);
        };
        enum CommitAction {
            Prepare(Box<FrozenCommit>),
            Send(InFlightCommit),
        }

        loop {
            let action = 'action: {
                let maintenance = reason.is_background();
                let mut state = if maintenance {
                    match cell.state.try_lock() {
                        Ok(state) => state,
                        Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
                        Err(std::sync::TryLockError::Poisoned(_)) => {
                            return Err(unavailable("DFS inode write state is poisoned"));
                        }
                    }
                } else {
                    cell.lock()
                        .map_err(|_| unavailable("DFS inode write state is poisoned"))?
                };
                // Background policy never queues behind an already admitted
                // operation. Foreground sync/drain still observes inode order.
                if maintenance && (state.commit_busy || state.operation_busy) {
                    return Ok(None);
                }
                while state.commit_busy || wait_for_operation && state.operation_busy {
                    state = cell
                        .wait(state)
                        .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                }
                if let Some(error) = state.terminal_error.clone() {
                    return Err(error);
                }
                if state.in_flight.is_some() {
                    let pending = match state.in_flight.as_ref() {
                        Some(InFlightCommit::Preparing(_)) => {
                            let taken = state.in_flight.take().ok_or_else(|| {
                                unavailable(
                                    "DFS preparing commit disappeared before ownership transfer",
                                )
                            })?;
                            let InFlightCommit::Preparing(frozen) = taken else {
                                return Err(unavailable(
                                    "DFS in-flight commit changed before preparing ownership transfer",
                                ));
                            };
                            state.in_flight = Some(InFlightCommit::Preparing(frozen.clone()));
                            state.commit_busy = true;
                            break 'action CommitAction::Prepare(frozen);
                        }
                        Some(InFlightCommit::File(pending)) => {
                            InFlightCommit::File(pending.clone())
                        }
                        Some(InFlightCommit::Metadata(sync, through_seq)) => {
                            InFlightCommit::Metadata(sync.clone(), *through_seq)
                        }
                        None => {
                            return Err(unavailable(
                                "DFS in-flight commit disappeared before send ownership transfer",
                            ));
                        }
                    };
                    state.commit_busy = true;
                    break 'action CommitAction::Send(pending);
                } else {
                    // See `CommitReason::flushes_pending_metadata`.
                    let sync_metadata_only = state.metadata_dirty
                        && reason.flushes_pending_metadata(
                            state.visible_write_seq > state.committed_write_seq,
                            state.open_writers,
                        );
                    if !(state.dirty
                        || sync_metadata_only
                        || reason.full_metadata_barrier() && state.metadata_dirty
                        || state.kill_suidgid_dirty)
                    {
                        return Ok(None);
                    }
                    if should_renew(&state.write_lease) {
                        state.write_lease =
                            self.meta.renew_write_lease(state.write_lease.clone())?;
                    }
                    if !state.dirty {
                        let sync = SyncInodeMetadata {
                            operation_id: self.operation_id("fsync-metadata"),
                            inode_id: state.inode.inode_id.clone(),
                            write_lease: state.write_lease.clone(),
                            expected_inode_revision: state.inode.revision,
                            expected_head_version: state
                                .base_version
                                .as_ref()
                                .map(|version| version.id.clone()),
                            metadata_delta: CommitMetadataDelta {
                                mode: CommitMetadataMode::Full,
                                mtime_unix_ms: Some(state.inode.attributes.mtime_unix_ms),
                                ctime_unix_ms: Some(state.inode.attributes.ctime_unix_ms),
                                kill_suidgid: state.kill_suidgid_dirty,
                            },
                        };
                        let through_seq = state.visible_write_seq;
                        let sync = Box::new(sync);
                        let pending = InFlightCommit::Metadata(sync.clone(), through_seq);
                        state.in_flight = Some(InFlightCommit::Metadata(sync, through_seq));
                        state.commit_busy = true;
                        break 'action CommitAction::Send(pending);
                    } else {
                        let frozen = Self::freeze_state_for_commit(&mut state);
                        state.in_flight = Some(InFlightCommit::Preparing(Box::new(frozen.clone())));
                        state.commit_busy = true;
                        state.dirty = false;
                        break 'action CommitAction::Prepare(Box::new(frozen));
                    }
                }
            };

            let pending_reuse = matches!(&action, CommitAction::Send(_));
            let pending = match action {
                CommitAction::Send(pending) => pending,
                CommitAction::Prepare(mut frozen) => {
                    match self.prepare_commit(&mut frozen, reason) {
                        Ok(batch) => {
                            let pending_file = PendingFileCommit {
                                frozen: *frozen,
                                batch,
                                trace_send_attempt: self
                                    .pending_trace
                                    .as_ref()
                                    .map(|_| Arc::new(AtomicU64::new(0))),
                            };
                            let pending = InFlightCommit::File(Box::new(pending_file.clone()));
                            let mut state = cell
                                .lock()
                                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                            while self.native_pipeline.is_some()
                                && wait_for_operation
                                && state.operation_busy
                            {
                                state = cell.wait(state).map_err(|_| {
                                    unavailable("DFS inode write state is poisoned")
                                })?;
                            }
                            state.in_flight = Some(InFlightCommit::File(Box::new(pending_file)));
                            #[cfg(test)]
                            self.pause_after_writeback_prepare();
                            pending
                        }
                        Err(prepare_error) => {
                            let mut state = cell
                                .lock()
                                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                            let failed = *frozen;
                            if state.dirty_extents.is_empty() {
                                state.dirty_extents.restore_before(failed.dirty_extents);
                                state.in_flight = None;
                            } else {
                                state.in_flight = Some(InFlightCommit::Preparing(Box::new(failed)));
                            }
                            state.dirty = !state.dirty_extents.is_empty();
                            state.commit_busy = false;
                            cell.notify_all();
                            return Err(prepare_error.error);
                        }
                    }
                }
            };

            let branch = if pending_reuse {
                "pending_reuse"
            } else {
                "new_prepare"
            };
            commit_span.record("branch", branch);
            let committed = self.finish_commit(cell.clone(), pending, None, branch)?;
            if reason.full_metadata_barrier() && self.needs_full_metadata_sync(&cell)? {
                continue;
            }
            return Ok(committed);
        }
    }

    fn finish_commit(
        &self,
        cell: SharedInodeWriteState,
        pending: InFlightCommit,
        timeout: Option<Duration>,
        branch: &str,
    ) -> Result<Option<u64>> {
        let committed_kill_suidgid = matches!(&pending, InFlightCommit::File(pending) if pending.frozen.kill_suidgid_dirty)
            || matches!(&pending, InFlightCommit::Metadata(sync, _) if sync.metadata_delta.kill_suidgid);
        let result = (|| match &pending {
            InFlightCommit::Preparing(_) => Err(unavailable("DFS inode commit is still preparing")),
            InFlightCommit::File(pending) => {
                let committed_full_metadata =
                    pending.batch.commit.metadata_delta.mode == CommitMetadataMode::Full;
                let version = pending.batch.commit.file_version.clone();
                let layout = pending.batch.commit.layout_root.clone();
                let _native_rpc_accounting =
                    if let Some(accounting) = &pending.batch._native_accounting {
                        let base = accounting.actual.load(Ordering::Relaxed);
                        accounting
                            .set_actual(base + native_commit_heap(pending.batch.commit.as_ref()))?;
                        Some(NativeRpcAccounting {
                            accounting: accounting.clone(),
                            base,
                        })
                    } else {
                        None
                    };
                if let Some(trace) = &self.pending_trace
                    && let Ok(state) = cell.lock()
                {
                    trace.observe("dfs_pending_commit_send", branch, &state, pending, None);
                }
                let publish_span = tracing::info_span!(
                    "node.dfs.commit.publish",
                    branch,
                    inode_id = %pending.batch.commit.inode_id.0,
                    operation_id = %pending.batch.commit.operation_id.0,
                    through_seq = pending.batch.through_seq,
                    chunks = pending.batch.commit.chunk_receipts.len(),
                    file_length = pending.batch.commit.file_version.length,
                    metadata_mode = ?pending.batch.commit.metadata_delta.mode,
                    has_timeout = timeout.is_some(),
                );
                let rpc_started = Instant::now();
                let updated = self.validate_inode(
                    publish_span
                        .in_scope(|| {
                            let publication_diagnostic = self.read_engine.publication_diagnostic();
                            let _coarse = self.read_engine.publication_coarse_diagnostic();
                            let result = if let Some(timeout) = timeout {
                                self.meta.commit_file_version_with_timeout(
                                    pending.batch.commit.as_ref().clone(),
                                    timeout,
                                )
                            } else {
                                self.meta
                                    .commit_file_version(pending.batch.commit.as_ref().clone())
                            };
                            if let Some(diagnostic) = publication_diagnostic {
                                diagnostic.finish(result.is_ok());
                            }
                            result
                        })
                        .inspect_err(|error| {
                            afs_logging::warn!("dfs.commit_rpc_failed";
                                "method" => "commit_file_version",
                                "inode_id" => pending.batch.commit.inode_id.0.as_str(),
                                "operation_id" => pending.batch.commit.operation_id.0.as_str(),
                                "branch" => branch,
                                "timeout" => timeout.map(|value| format!("{}ms", value.as_millis()))
                                    .unwrap_or_else(|| "foreground_default".to_owned()),
                                "elapsed_ms" => rpc_started.elapsed().as_secs_f64() * 1000.0,
                                "error" => error.to_string()
                            );
                        })?,
                )?;
                Ok(AppliedCommit::File {
                    through_seq: pending.batch.through_seq,
                    committed_full_metadata,
                    version,
                    layout,
                    updated,
                })
            }
            InFlightCommit::Metadata(sync, through_seq) => {
                let rpc_started = Instant::now();
                let updated = self.validate_inode(
                    (if let Some(timeout) = timeout {
                        self.meta
                            .sync_inode_metadata_with_timeout(sync.as_ref().clone(), timeout)
                    } else {
                        self.meta.sync_inode_metadata(sync.as_ref().clone())
                    })
                    .inspect_err(|error| {
                        afs_logging::warn!("dfs.commit_rpc_failed";
                            "method" => "sync_inode_metadata",
                            "inode_id" => sync.inode_id.0.as_str(),
                            "operation_id" => sync.operation_id.0.as_str(),
                            "branch" => branch,
                            "timeout" => timeout.map(|value| format!("{}ms", value.as_millis()))
                                .unwrap_or_else(|| "foreground_default".to_owned()),
                            "elapsed_ms" => rpc_started.elapsed().as_secs_f64() * 1000.0,
                            "error" => error.to_string()
                        );
                    })?,
                )?;
                Ok(AppliedCommit::Metadata {
                    through_seq: *through_seq,
                    updated,
                })
            }
        })();

        let mut state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        state.commit_busy = false;
        let outcome = match result {
            Ok(AppliedCommit::File {
                through_seq,
                committed_full_metadata,
                version,
                layout,
                updated,
            }) => {
                state.in_flight = None;
                let mut updated = updated;
                if !committed_full_metadata {
                    updated.attributes.mtime_unix_ms = state.inode.attributes.mtime_unix_ms;
                    updated.attributes.ctime_unix_ms = state.inode.attributes.ctime_unix_ms;
                }
                let has_later_dirty = state.visible_write_seq > through_seq;
                if has_later_dirty {
                    updated.attributes.mtime_unix_ms = state.inode.attributes.mtime_unix_ms;
                    updated.attributes.ctime_unix_ms = state.inode.attributes.ctime_unix_ms;
                    if state.kill_suidgid_dirty {
                        updated.attributes.mode =
                            Self::clear_kernel_write_privileges(updated.attributes.mode);
                    }
                }
                state.inode = updated;
                state.base_version = Some(version);
                state.base_layout = layout;
                state.dirty = !state.dirty_extents.is_empty();
                state.durable_write_seq = through_seq;
                state.committed_write_seq = through_seq;
                state.last_writer_background_requested = false;
                state.native_due = state.native_due_through > through_seq;
                if committed_full_metadata
                    && state.dirty_extents.is_empty()
                    && state.visible_write_seq <= through_seq
                {
                    state.metadata_dirty = false;
                }
                if committed_kill_suidgid && !has_later_dirty {
                    state.kill_suidgid_dirty = false;
                }
                if let Some(trace) = &self.pending_trace
                    && let InFlightCommit::File(sent) = &pending
                {
                    trace.observe(
                        "dfs_pending_commit_success_cleared",
                        branch,
                        &state,
                        sent,
                        None,
                    );
                }
                Ok(Some(through_seq))
            }
            Ok(AppliedCommit::Metadata {
                through_seq,
                updated,
            }) => {
                state.in_flight = None;
                state.inode = updated;
                state.metadata_dirty = false;
                state.kill_suidgid_dirty = false;
                state.last_writer_background_requested = false;
                state.durable_write_seq = state.durable_write_seq.max(through_seq);
                state.committed_write_seq = state.committed_write_seq.max(through_seq);
                Ok(Some(state.committed_write_seq))
            }
            Err(error) => {
                if is_definite_commit_rejection(&error) {
                    state.in_flight = None;
                    state.terminal_error = Some(error.clone());
                    if let Some(trace) = &self.pending_trace
                        && let InFlightCommit::File(sent) = &pending
                    {
                        trace.observe(
                            "dfs_pending_commit_definite_rejection_cleared",
                            branch,
                            &state,
                            sent,
                            Some(&error),
                        );
                    }
                    if let InFlightCommit::File(pending) = pending {
                        state
                            .dirty_extents
                            .restore_before(pending.frozen.dirty_extents);
                        state.dirty = !state.dirty_extents.is_empty();
                    }
                } else if let Some(trace) = &self.pending_trace
                    && let InFlightCommit::File(sent) = &pending
                {
                    trace.observe(
                        "dfs_pending_commit_unknown_retained",
                        branch,
                        &state,
                        sent,
                        Some(&error),
                    );
                }
                Err(error)
            }
        };
        cell.notify_all();
        outcome
    }

    fn needs_full_metadata_sync(&self, cell: &SharedInodeWriteState) -> Result<bool> {
        let state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        Ok(state.metadata_dirty)
    }

    fn begin_inode_operation(
        &self,
        inode_id: &InodeId,
        cell: SharedInodeWriteState,
    ) -> Result<InodeOperationGuard> {
        loop {
            let mut state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            while state.operation_busy
                || state.commit_busy
                    && (self.native_pipeline.is_none()
                        || Self::in_flight_blocks_mutation(&state.in_flight))
            {
                state = cell
                    .wait(state)
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            }
            if let Some(error) = state.terminal_error.clone() {
                return Err(error);
            }
            if !Self::in_flight_blocks_mutation(&state.in_flight) {
                state.operation_busy = true;
                drop(state);
                return Ok(InodeOperationGuard {
                    cell,
                    native_notify: self.native_writeback_notify(),
                });
            }
            drop(state);
            if let Some(seq) = self.commit_inode(
                inode_id,
                CommitReason::BACKGROUND,
                CommitWait::AfterOperations,
            )? {
                self.update_handles_after_commit(inode_id, seq)?;
            }
        }
    }

    fn lock_for_mutation<'a>(
        &self,
        inode_id: &InodeId,
        cell: &'a SharedInodeWriteState,
    ) -> Result<NativeInodeMutationGuard<'a>> {
        let ticket = self.native_ticket(cell)?;
        loop {
            let mut state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            while state.operation_busy
                || state.commit_busy
                    && (self.native_pipeline.is_none()
                        || Self::in_flight_blocks_mutation(&state.in_flight))
            {
                state = cell
                    .wait(state)
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            }
            if let Some(error) = state.terminal_error.clone() {
                return Err(error);
            }
            if !Self::in_flight_blocks_mutation(&state.in_flight) {
                return Ok(NativeInodeMutationGuard {
                    state,
                    _ticket: ticket,
                });
            }
            drop(state);
            self.commit_inode(
                inode_id,
                CommitReason::BACKGROUND,
                CommitWait::AfterOperations,
            )?;
        }
    }

    fn in_flight_blocks_mutation(in_flight: &Option<InFlightCommit>) -> bool {
        matches!(
            in_flight,
            Some(InFlightCommit::File(_) | InFlightCommit::Metadata(_, _))
        )
    }

    fn prepare_native_generation(
        &self,
        frozen: &mut FrozenCommit,
        operation_id: &OperationId,
        layout_id: LayoutRootId,
        retained_uploads: &mut Vec<Arc<NativeUpload>>,
    ) -> Result<(LayoutRoot, Vec<ChunkReceipt>)> {
        let mut plan = CommitPlanner.plan_with_integrity(
            frozen,
            operation_id.clone(),
            layout_id,
            self.crc32c_writes(),
        )?;
        let mut receipts: Vec<ChunkReceipt> = Vec::new();
        for extent in frozen.dirty_extents.extents.values() {
            if let Some(upload) = &extent.native_upload {
                let receipt = self.wait_native_upload(upload)?;
                if extent.file_offset < frozen.logical_length {
                    plan.layout_root.inline_extents.push(Extent {
                        file_offset: extent.file_offset,
                        length: extent
                            .length
                            .min(frozen.logical_length - extent.file_offset),
                        chunk_id: receipt.chunk.id.clone(),
                        chunk_offset: extent.source_offset as u64,
                    });
                    if !receipts
                        .iter()
                        .any(|previous| previous.chunk.id == receipt.chunk.id)
                    {
                        receipts.push(receipt);
                    }
                }
            }
        }
        for staged in unique_staged_chunks(plan.staged_chunks)? {
            let body = staged.shared_bytes();
            let descriptor = frozen
                .dirty_extents
                .iter_extents()
                .find_map(|extent| {
                    extent
                        .data
                        .as_ref()
                        .filter(|bytes| bytes.allocation_id() == body.allocation_id())
                        .and(extent.native_descriptor.as_ref())
                        .cloned()
                })
                .ok_or_else(|| {
                    unavailable("native publication body has no descriptor reservation")
                })?;
            let upload = Arc::new(NativeUpload {
                _gc_pin: frozen.dirty_extents.native_gc_pin.clone(),
                operation_id: operation_id.clone(),
                chunk: staged.chunk.clone(),
                owner: self
                    .write_state(&frozen.inode.inode_id)?
                    .as_ref()
                    .map_or_else(Weak::new, Arc::downgrade),
                state: Mutex::new(NativeUploadState {
                    body: Some(body),
                    staged: Some(staged),
                    task: None,
                    receipt: None,
                    error: None,
                    terminal_error: None,
                    joining: false,
                }),
                cv: Condvar::new(),
                _descriptor: descriptor,
            });
            if upload.chunk.is_segment_crc32c() {
                // Preserve the immutable tail identity before any fallible I/O.
                // Failed preparation restores this map, including successful
                // receipts. Dispatch keeps the original sequential order.
                for extent in plan
                    .layout_root
                    .inline_extents
                    .iter()
                    .filter(|extent| extent.chunk_id == upload.chunk.id)
                {
                    frozen.dirty_extents.insert_native(DirtyExtent {
                        file_offset: extent.file_offset,
                        length: extent.length,
                        write_seq: frozen.through_seq,
                        data: None,
                        builder: None,
                        source_offset: usize::try_from(extent.chunk_offset)
                            .map_err(|_| invalid("native tail offset is too large"))?,
                        native_reuse_end: extent.file_offset + extent.length,
                        native_upload: Some(upload.clone()),
                        native_descriptor: Some(upload._descriptor.clone()),
                    });
                }
            }
            retained_uploads.push(upload.clone());
            let receipt = self.wait_native_upload(&upload)?;
            if !receipts
                .iter()
                .any(|previous| previous.chunk.id == receipt.chunk.id)
            {
                receipts.push(receipt);
            }
        }
        plan.layout_root
            .inline_extents
            .sort_by_key(|extent| extent.file_offset);
        plan.layout_root
            .inline_extents
            .dedup_by(|extent, previous| {
                if previous.file_offset + previous.length == extent.file_offset
                    && previous.chunk_id == extent.chunk_id
                    && previous.chunk_offset + previous.length == extent.chunk_offset
                {
                    previous.length += extent.length;
                    true
                } else {
                    false
                }
            });
        receipts.sort_by(|left, right| left.chunk.id.0.cmp(&right.chunk.id.0));
        Ok((plan.layout_root, receipts))
    }

    fn prepare_commit(
        &self,
        frozen: &mut FrozenCommit,
        reason: CommitReason,
    ) -> std::result::Result<CommitBatch, PrepareCommitError> {
        let dirty_bytes = frozen.dirty_extents.dirty_data_bytes();
        let span = tracing::info_span!(
            "node.dfs.commit.prepare",
            reason = reason.label(),
            inode_id = %frozen.inode.inode_id.0,
            logical_length = frozen.logical_length,
            dirty_bytes,
            staged_chunks = tracing::field::Empty,
            staged_bytes = tracing::field::Empty,
            unique_chunks = tracing::field::Empty,
            unique_bytes = tracing::field::Empty,
            receipts = tracing::field::Empty,
        );
        let _entered = span.enter();
        let operation_id = frozen
            .dirty_extents
            .native_operation_id
            .clone()
            .or_else(|| {
                // Length-only preparation can fail after admission. The pin
                // already owns its exact operation; never mint a replacement.
                frozen
                    .dirty_extents
                    .native_gc_pin
                    .as_ref()
                    .and_then(|pin| pin.write_operation_id())
            })
            .unwrap_or_else(|| self.operation_id(reason.label()));
        let gc_pin = match (&frozen.dirty_extents.native_gc_pin, &self.gc_references) {
            (Some(pin), _) => Some(pin.clone()),
            (None, Some(references)) => Some(
                references
                    .pin_generation(
                        &frozen.inode.inode_id,
                        frozen.inode.head_version.as_ref(),
                        &operation_id,
                    )
                    .map_err(|error| PrepareCommitError { error })?,
            ),
            (None, None) => None,
        };
        if self.gc_references.is_some() {
            // Length-only generations have no write-time pin. Freeze their
            // captured fence as well; preparation retries never refresh it.
            frozen.dirty_extents.native_gc_pin = gc_pin.clone();
        }
        let generation = self.next_operation.fetch_add(1, Ordering::Relaxed);
        let layout_id = LayoutRootId::new(format!("{}-layout-{generation}", self.session_id));

        let mut native_uploads = Vec::new();
        let (layout_root, receipts) = if frozen.dirty_extents.native_operation_id.is_some() {
            self.prepare_native_generation(frozen, &operation_id, layout_id, &mut native_uploads)
                .map_err(|error| PrepareCommitError { error })?
        } else {
            let plan = CommitPlanner
                .plan(frozen, operation_id.clone(), layout_id)
                .map_err(|error| PrepareCommitError { error })?;
            let staged_chunks = plan.staged_chunks.len();
            let staged_bytes = plan
                .staged_chunks
                .iter()
                .fold(0_u64, |sum, chunk| sum.saturating_add(chunk.chunk.length));
            span.record("staged_chunks", staged_chunks as u64);
            span.record("staged_bytes", staged_bytes);
            let unique = unique_staged_chunks(plan.staged_chunks)
                .map_err(|error| PrepareCommitError { error })?;
            let unique_bytes = unique
                .iter()
                .fold(0_u64, |sum, chunk| sum.saturating_add(chunk.chunk.length));
            span.record("unique_chunks", unique.len() as u64);
            span.record("unique_bytes", unique_bytes);
            let receipts = tracing::info_span!(
                "node.dfs.commit.put_batch",
                reason = reason.label(),
                chunks = unique.len(),
                bytes = unique_bytes,
                reused_receipts = 0_u64,
            )
            .in_scope(|| self.chunk_store.put_batch(unique))
            .map_err(|error| PrepareCommitError { error })?;
            (plan.layout_root, receipts)
        };
        span.record("receipts", receipts.len() as u64);
        let now = now_unix_ms();
        let version = FileVersion {
            id: FileVersionId::new(format!("{}-version-{generation}", self.session_id)),
            inode_id: frozen.inode.inode_id.clone(),
            parent_version: frozen
                .base_version
                .as_ref()
                .map(|version| version.id.clone()),
            length: frozen.logical_length,
            layout_root: layout_root.id.clone(),
            created_at_unix_ms: now,
        };
        let gc_fence = gc_pin.as_ref().and_then(|pin| pin.fence());
        if self.gc_references.is_some() && gc_fence.is_none() {
            return Err(PrepareCommitError {
                error: unavailable("optimistic commit has no frozen GC fence"),
            });
        }
        let commit = CommitFileVersion {
            gc_fence,
            operation_id,
            inode_id: frozen.inode.inode_id.clone(),
            write_lease: frozen.write_lease.clone(),
            expected_inode_revision: frozen.inode.revision,
            expected_head_version: frozen
                .base_version
                .as_ref()
                .map(|version| version.id.clone()),
            file_version: version,
            layout_root,
            chunk_receipts: receipts,
            clone_source: None,
            metadata_delta: reason.metadata_delta(
                frozen.inode.attributes.mtime_unix_ms,
                frozen.inode.attributes.ctime_unix_ms,
                frozen.kill_suidgid_dirty,
            ),
        };
        let native_accounting = if frozen.dirty_extents.native_operation_id.is_some()
            && (!commit.chunk_receipts.is_empty()
                || frozen
                    .dirty_extents
                    .iter_extents()
                    .any(|extent| extent.native_descriptor.is_some()))
        {
            let pipeline = self
                .native_pipeline
                .as_ref()
                .ok_or_else(|| PrepareCommitError {
                    error: unavailable("native write pipeline is missing"),
                })?;
            let mut owners: Vec<Arc<Mutex<NativeMetadataLease>>> = Vec::new();
            for extent in frozen.dirty_extents.iter_extents() {
                if let Some(owner) = &extent.native_descriptor
                    && !owners.iter().any(|other| Arc::ptr_eq(other, owner))
                {
                    owners.push(owner.clone());
                }
            }
            let mut owned_headroom = 0usize;
            for owner in &owners {
                let owner = owner.lock().map_err(|_| PrepareCommitError {
                    error: unavailable("native descriptor reservation is poisoned"),
                })?;
                owned_headroom =
                    owned_headroom.saturating_add(owner.bytes.saturating_sub(owner.actual_bytes));
            }
            let base = native_commit_heap(&commit)
                + owners.capacity() * std::mem::size_of::<Arc<Mutex<NativeMetadataLease>>>()
                + native_uploads.capacity() * std::mem::size_of::<Arc<NativeUpload>>();
            let reserved = base.saturating_mul(3);
            let mut state = pipeline
                .metadata
                .state
                .lock()
                .map_err(|_| PrepareCommitError {
                    error: unavailable("native metadata budget is poisoned"),
                })?;
            if reserved > owned_headroom
                || reserved
                    > state
                        .descriptor_reserved_bytes
                        .saturating_sub(state.descriptor_bytes)
                        .saturating_sub(state.scratch_reserved_bytes)
            {
                return Err(PrepareCommitError {
                    error: unavailable(
                        "native commit cannot fit its pre-reserved publication scratch",
                    ),
                });
            }
            state.scratch_reserved_bytes += reserved;
            state.descriptor_bytes += base;
            state.descriptor_peak = state.descriptor_peak.max(state.descriptor_bytes);
            drop(state);
            Some(Arc::new(NativeCommitAccounting {
                budget: pipeline.metadata.clone(),
                reserved,
                actual: AtomicUsize::new(base),
                _owners: owners,
            }))
        } else {
            None
        };
        Ok(CommitBatch {
            _gc_pin: gc_pin,
            through_seq: frozen.through_seq,
            _native_accounting: native_accounting,
            _native_uploads: native_uploads,
            commit: Arc::new(commit),
        })
    }

    fn observe_handle_error(&self, handle: FileHandle) -> Result<()> {
        let snapshot = self.handle_snapshot(handle)?;
        let Some(session) = snapshot.write_session else {
            return Ok(());
        };
        let Some(state) = self.write_state(&snapshot.inode_id)? else {
            return Ok(());
        };
        let state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        if let Some(error) = state.terminal_error.clone() {
            return Err(error);
        }
        let observed = state.background_error.clone();
        let Some(observed) = observed.filter(|error| error.cursor > session.local.error_cursor)
        else {
            return Ok(());
        };
        self.update_handle_error_cursor(handle, observed.cursor)?;
        afs_logging::warn!("dfs.background_error_consumed";
            "inode_id" => snapshot.inode_id.0.as_str(),
            "handle" => handle.0,
            "previous_cursor" => session.local.error_cursor,
            "cursor" => observed.cursor,
            "error" => observed.error.to_string()
        );
        Err(observed.error)
    }

    fn resize_dirty_inode(
        &self,
        inode_id: &InodeId,
        length: u64,
        kill_suidgid: bool,
    ) -> Result<u64> {
        let state = self
            .write_state(inode_id)?
            .ok_or_else(|| stale("DFS write state is no longer open"))?;
        let mut state = self.lock_for_mutation(inode_id, &state)?;
        state.next_write_seq = state.next_write_seq.saturating_add(1);
        let write_seq = state.next_write_seq;
        let old_length = state.logical_length;
        if old_length != length {
            state.dirty_extents.resize(old_length, length, write_seq)?;
            state.logical_length = length;
            state.dirty = true;
        }
        state.visible_write_seq = write_seq;
        let now = now_unix_ms();
        state.inode.attributes.mtime_unix_ms = now;
        state.inode.attributes.ctime_unix_ms = now;
        state.metadata_dirty = true;
        if kill_suidgid {
            Self::apply_kernel_killpriv_to_state(&mut state);
        }
        if state.open_writers == 0 && (state.dirty || state.metadata_dirty) {
            state.last_writer_background_requested = true;
        }
        Ok(state.visible_write_seq)
    }

    fn release_writer(&self, session: &DfsWriteSession) -> Result<()> {
        let Some(state) = self.write_state(&session.inode_id)? else {
            return Ok(());
        };
        let _native_ticket = self.native_ticket_with_cleanup(&state, true)?;
        let mut state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        state.open_writers = state.open_writers.saturating_sub(1);
        if state.open_writers == 0 && (state.dirty || state.metadata_dirty) {
            state.last_writer_background_requested = true;
        }
        Ok(())
    }

    fn ensure_local_write_owner(&self, lease: &WriteLease) -> Result<()> {
        if lease.owner_node_id != self.node_id || lease.owner_session_id != self.session_id {
            return Err(stale("DFS write lease is owned by another node session"));
        }
        Ok(())
    }

    fn remote_owner(&self, lease: &WriteLease) -> Result<Arc<dyn RemoteDfsOwner>> {
        let factory = self.remote_owner_factory.as_ref().ok_or_else(|| {
            Error::coded(
                afs_error::NODE_VFS_UNIMPLEMENTED,
                "DFS remote write owner connector is not configured",
            )
        })?;
        let location = self
            .meta
            .lookup_node_location(&lease.owner_node_id)?
            .ok_or_else(|| stale("DFS write owner node is not registered"))?;
        if location.node_id != lease.owner_node_id {
            return Err(stale("DFS owner lookup returned another node"));
        }
        factory.connect(location)
    }

    fn open_remote_write_session(
        &self,
        inode: &InodeRecord,
        lease: &WriteLease,
        open_flags: i32,
        options: OpenOptions,
    ) -> Result<DfsRemoteWriteSession> {
        let release_slots = Self::remote_release_slot_count(open_flags);
        let owner = self.remote_owner(lease)?;
        self.reserve_remote_release_capacity(release_slots)?;
        let open_request = afs_protocol::node_control::DfsOwnerOpenRequest {
            namespace_id: self.namespace_id.0.clone(),
            inode_id: inode.inode_id.0.clone(),
            owner_node_id: lease.owner_node_id.clone(),
            owner_session_id: lease.owner_session_id.clone(),
            lease_epoch: lease.lease_epoch,
            caller_session_id: self.session_id.clone(),
            open_flags,
            kill_suidgid: options.kill_suidgid,
            open_seq: 0,
        };
        let (_open_request, handle) = self.open_remote_owner_admitted(
            &owner,
            open_request,
            release_slots,
            "DFS owner open returned no handle",
        )?;
        let read_handle = if open_flags & libc::O_ACCMODE == libc::O_WRONLY {
            let read_open_request = afs_protocol::node_control::DfsOwnerOpenRequest {
                namespace_id: self.namespace_id.0.clone(),
                inode_id: inode.inode_id.0.clone(),
                owner_node_id: lease.owner_node_id.clone(),
                owner_session_id: lease.owner_session_id.clone(),
                lease_epoch: lease.lease_epoch,
                caller_session_id: self.session_id.clone(),
                open_flags: libc::O_RDONLY,
                kill_suidgid: false,
                open_seq: 0,
            };
            match self.open_remote_owner_admitted(
                &owner,
                read_open_request,
                1,
                "DFS owner read companion open returned no handle",
            ) {
                Ok((_request, read_handle)) => Some(read_handle),
                Err(error) => {
                    let release =
                        self.remote_release_handle_or_queue_reserved(&owner, handle, true);
                    release?;
                    return Err(error);
                }
            }
        } else {
            None
        };
        let remote = RemoteDfsWriteSession {
            owner,
            handle,
            read_handle,
            release_slots,
            lifecycle: RemoteProviderLifecycle::live(),
        };
        self.remote_inode_providers
            .insert(inode.inode_id.clone(), remote.clone())?;
        self.handles.mark_dynamic(&inode.inode_id)?;
        Ok(DfsRemoteWriteSession {
            local: DfsWriteSession {
                id: DfsWriteSessionId::new(format!(
                    "{}-remote-write-session-{}",
                    self.session_id,
                    self.next_operation.fetch_add(1, Ordering::Relaxed)
                )),
                inode_id: inode.inode_id.clone(),
                open_flags,
                lease_epoch: lease.lease_epoch,
                last_accepted_seq: 0,
                last_synced_seq: 0,
                error_cursor: 0,
            },
            remote: Some(remote),
        })
    }

    fn open_remote_owner_admitted(
        &self,
        owner: &Arc<dyn RemoteDfsOwner>,
        request_template: afs_protocol::node_control::DfsOwnerOpenRequest,
        reserved_slots: usize,
        missing_handle_message: &'static str,
    ) -> Result<(
        afs_protocol::node_control::DfsOwnerOpenRequest,
        afs_protocol::node_control::DfsOwnerHandle,
    )> {
        let entered = std::cell::Cell::new(false);
        let result = self.with_remote_open_admission(
            &request_template.owner_node_id,
            &request_template.owner_session_id,
            || {
                entered.set(true);
                let open_seq = match self.next_remote_open_seq() {
                    Ok(open_seq) => open_seq,
                    Err(error) => {
                        self.release_remote_release_capacity(reserved_slots)?;
                        return Err(error);
                    }
                };
                let mut request = request_template.clone();
                request.open_seq = open_seq;
                let reply = match owner.open(request.clone()) {
                    Ok(reply) => reply,
                    Err(error) => {
                        if Self::remote_release_error_is_retryable(&error) {
                            self.queue_pending_remote_release(
                                owner.clone(),
                                Self::cleanup_handle_for_open_request(&self.node_id, &request)?,
                                true,
                            )?;
                            if reserved_slots > 1 {
                                self.release_remote_release_capacity(reserved_slots - 1)?;
                            }
                        } else {
                            self.release_remote_release_capacity(reserved_slots)?;
                        }
                        return Err(error);
                    }
                };
                let Some(handle) = reply.handle else {
                    self.queue_pending_remote_release(
                        owner.clone(),
                        Self::cleanup_handle_for_open_request(&self.node_id, &request)?,
                        true,
                    )?;
                    if reserved_slots > 1 {
                        self.release_remote_release_capacity(reserved_slots - 1)?;
                    }
                    return Err(unavailable(missing_handle_message));
                };
                if let Err(error) = self.validate_remote_open_reply_handle(&request, &handle) {
                    self.queue_pending_remote_release(
                        owner.clone(),
                        Self::cleanup_handle_for_open_request(&self.node_id, &request)?,
                        true,
                    )?;
                    if reserved_slots > 1 {
                        self.release_remote_release_capacity(reserved_slots - 1)?;
                    }
                    return Err(error);
                }
                Ok((request, handle))
            },
        );
        if result.is_err() && !entered.get() {
            self.release_remote_release_capacity(reserved_slots)?;
        }
        result
    }

    fn with_remote_open_admission<T>(
        &self,
        owner_node_id: &str,
        owner_session_id: &str,
        action: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let route = DfsOwnerOpenRouteKey {
            caller_node_id: self.node_id.clone(),
            caller_session_id: self.session_id.clone(),
            owner_node_id: owner_node_id.to_owned(),
            owner_session_id: owner_session_id.to_owned(),
        };
        let lock = {
            let mut locks = self
                .remote_open_admission_locks
                .lock()
                .map_err(|_| unavailable("DFS remote open admission lock table is poisoned"))?;
            if !locks.contains_key(&route) && locks.len() >= MAX_PENDING_REMOTE_RELEASES {
                return Err(Error::coded(
                    afs_error::NODE_VFS_UNAVAILABLE,
                    "DFS remote open admission lock table is full",
                ));
            }
            let lock = locks
                .entry(route.clone())
                .or_insert_with(|| {
                    Arc::new(DfsRemoteOpenAdmissionLock {
                        gate: Mutex::new(()),
                        users: AtomicUsize::new(0),
                    })
                })
                .clone();
            lock.users.fetch_add(1, Ordering::AcqRel);
            lock
        };
        let result = match lock.gate.lock() {
            Ok(_guard) => action(),
            Err(_) => Err(unavailable("DFS remote open admission lock is poisoned")),
        };
        if lock.users.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.cleanup_remote_open_admission_lock(&route, &lock)?;
        }
        result
    }

    fn cleanup_remote_open_admission_lock(
        &self,
        route: &DfsOwnerOpenRouteKey,
        lock: &Arc<DfsRemoteOpenAdmissionLock>,
    ) -> Result<()> {
        let mut locks = self
            .remote_open_admission_locks
            .lock()
            .map_err(|_| unavailable("DFS remote open admission lock table is poisoned"))?;
        if locks
            .get(route)
            .is_some_and(|current| Arc::ptr_eq(current, lock))
            && lock.users.load(Ordering::Acquire) == 0
        {
            locks.remove(route);
        }
        Ok(())
    }

    fn validate_remote_open_reply_handle(
        &self,
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        if handle.namespace_id != request.namespace_id
            || handle.inode_id != request.inode_id
            || handle.owner_node_id != request.owner_node_id
            || handle.owner_session_id != request.owner_session_id
            || handle.lease_epoch != request.lease_epoch
            || handle.caller_node_id != self.node_id
            || handle.caller_session_id != self.session_id
            || handle.open_seq != request.open_seq
            || handle.opaque_handle.is_empty()
        {
            return Err(stale("DFS owner open returned a mismatched handle"));
        }
        Ok(())
    }

    fn next_remote_open_seq(&self) -> Result<u64> {
        let seq = self
            .next_owner_open_seq
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| {
                Error::coded(
                    afs_error::NODE_VFS_UNAVAILABLE,
                    "DFS owner open sequence counter overflowed",
                )
            })?;
        if seq == 0 {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS owner open sequence counter overflowed",
            ));
        }
        Ok(seq)
    }

    fn make_local_write_session(session: DfsWriteSession) -> DfsRemoteWriteSession {
        DfsRemoteWriteSession {
            local: session,
            remote: None,
        }
    }

    fn lock_authority_for(&self, inode_id: &InodeId) -> Result<DfsLockTarget> {
        let (inode, lease) = self.meta.resolve_lock_authority(inode_id)?;
        let inode = self.validate_inode(inode)?;
        if inode.inode_id != *inode_id {
            return Err(stale("DFS lock authority returned another inode"));
        }
        if lease.inode_id != *inode_id
            || lease.lease_epoch == 0
            || lease.owner_node_id.is_empty()
            || lease.owner_session_id.is_empty()
        {
            return Err(stale(
                "DFS lock authority returned an invalid lease identity",
            ));
        }
        if lease.owner_node_id == self.node_id && lease.owner_session_id == self.session_id {
            self.ensure_local_write_owner(&lease)?;
            let authority = self.local_lock_authority(&inode.inode_id, lease)?;
            Ok(DfsLockTarget::Local(authority))
        } else {
            Ok(DfsLockTarget::Remote(
                self.remote_lock_authority(&inode.inode_id, &lease)?,
            ))
        }
    }

    fn existing_lock_authority_for(&self, inode_id: &InodeId) -> Result<Option<DfsLockTarget>> {
        if let Some(authority) = self
            .lock_authorities
            .read(inode_id)
            .map_err(|_| unavailable("DFS lock authority table is poisoned"))?
            .get(inode_id)
            .cloned()
        {
            return Ok(Some(DfsLockTarget::Local(authority)));
        }
        Ok(self
            .remote_lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .get(inode_id)
            .cloned()
            .map(DfsLockTarget::Remote))
    }

    fn reclaim_idle_lock_authorities(&self) -> Result<()> {
        let idle = self
            .lock_authorities
            .snapshot()?
            .into_iter()
            .filter(|(_, authority)| {
                // The snapshot adds one metadata reference to table + renewal.
                authority.table.is_idle().unwrap_or(false) && Arc::strong_count(authority) <= 3
            })
            .collect::<Vec<_>>();
        for (inode_id, authority) in idle {
            let mut authorities = self
                .lock_authorities
                .write(&inode_id)
                .map_err(|_| unavailable("DFS lock authority table is poisoned"))?;
            let current_is_idle = authorities.get(&inode_id).is_some_and(|current| {
                Arc::ptr_eq(current, &authority)
                    && Arc::strong_count(current) <= 3
                    && current.table.is_idle().unwrap_or(false)
            });
            if current_is_idle {
                authorities.remove(&inode_id);
            }
            drop(authorities);
            if current_is_idle {
                let _ = self.lock_renewal.untrack_if_current(&inode_id, &authority);
            }
        }
        Ok(())
    }

    fn replace_stale_lock_authority(
        &self,
        inode_id: &InodeId,
        old: &Arc<DfsLockAuthority>,
        lease: WriteLease,
    ) -> Result<Arc<DfsLockAuthority>> {
        let _ = old.table.invalidate();
        if let Ok(mut state) = old.state.lock() {
            state.last_error = Some(stale(
                "DFS lock authority was fenced by a newer lease epoch",
            ));
            state.pinned_owners.clear();
            state.waiters.clear();
        }
        let authority = Arc::new(DfsLockAuthority::new(lease));
        self.lock_renewal
            .track(inode_id.clone(), authority.clone())?;
        Ok(authority)
    }

    fn local_lock_authority(
        &self,
        inode_id: &InodeId,
        lease: WriteLease,
    ) -> Result<Arc<DfsLockAuthority>> {
        let admission = self
            .lock_authority_admission
            .lock()
            .map_err(|_| unavailable("DFS lock authority admission is poisoned"))?;
        self.reclaim_idle_lock_authorities()?;
        let count = self.lock_authorities.len()?;
        let mut authorities = self
            .lock_authorities
            .write(inode_id)
            .map_err(|_| unavailable("DFS lock authority table is poisoned"))?;
        if let Some(authority) = authorities.get(inode_id).cloned() {
            let epoch = authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
                .lease
                .lease_epoch;
            if epoch == lease.lease_epoch {
                drop(authorities);
                drop(admission);
                let _renewal = lock_authority_renewal_guard(&authority)?;
                refresh_same_epoch_lock_authority(&authority, lease)?;
                self.check_lock_renewal_locked(&authority)?;
                drop(_renewal);
                return Ok(authority);
            }
            let replacement = self.replace_stale_lock_authority(inode_id, &authority, lease)?;
            authorities.insert(inode_id.clone(), replacement.clone());
            return Ok(replacement);
        }
        if count >= MAX_LOCK_AUTHORITIES {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        let authority = Arc::new(DfsLockAuthority::new(lease));
        self.lock_renewal
            .track(inode_id.clone(), authority.clone())?;
        authorities.insert(inode_id.clone(), authority.clone());
        drop(authorities);
        Ok(authority)
    }

    fn reclaim_idle_remote_lock_authorities(&self) -> Result<()> {
        let active_session_inodes = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?
            .iter()
            .filter(|&(_inode_id, sessions)| !sessions.is_empty())
            .map(|(inode_id, _sessions)| inode_id.clone())
            .collect::<HashSet<_>>();
        let active_waiter_inodes = self
            .remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .values()
            .map(|remote| InodeId::new(remote.authority.inode_id.clone()))
            .collect::<HashSet<_>>();
        self.remote_lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .retain(|inode_id, _| {
                active_session_inodes.contains(inode_id) || active_waiter_inodes.contains(inode_id)
            });
        Ok(())
    }

    fn remote_lock_authority(
        &self,
        inode_id: &InodeId,
        lease: &WriteLease,
    ) -> Result<DfsRemoteLockAuthority> {
        self.reclaim_idle_remote_lock_authorities()?;
        let owner = self.remote_owner(lease)?;
        let remote = DfsRemoteLockAuthority {
            owner,
            authority: self.lock_authority_wire(inode_id, lease),
        };
        let mut authorities = self
            .remote_lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?;
        if !authorities.contains_key(inode_id) && authorities.len() >= MAX_REMOTE_LOCK_AUTHORITIES {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        authorities.insert(inode_id.clone(), remote.clone());
        Ok(remote)
    }

    fn lock_authority_wire(
        &self,
        inode_id: &InodeId,
        lease: &WriteLease,
    ) -> afs_protocol::node_control::DfsOwnerLockAuthority {
        afs_protocol::node_control::DfsOwnerLockAuthority {
            namespace_id: self.namespace_id.0.clone(),
            inode_id: inode_id.0.clone(),
            owner_node_id: lease.owner_node_id.clone(),
            owner_session_id: lease.owner_session_id.clone(),
            lease_epoch: lease.lease_epoch,
            lease_expires_at_unix_ms: lease.expires_at_unix_ms,
            caller_node_id: self.node_id.clone(),
            caller_session_id: self.session_id.clone(),
        }
    }

    fn lock_scope(
        node_id: &str,
        caller_session_id: &str,
        ingress_session_id: &str,
    ) -> Result<String> {
        if node_id.is_empty() || caller_session_id.is_empty() || ingress_session_id.is_empty() {
            return Err(invalid("DFS lock scope is incomplete"));
        }
        if node_id.contains('\0')
            || caller_session_id.contains('\0')
            || ingress_session_id.contains('\0')
        {
            return Err(invalid("DFS lock scope contains an invalid NUL byte"));
        }
        Ok(format!(
            "{node_id}\0{caller_session_id}\0{ingress_session_id}"
        ))
    }

    fn raw_lock_session(scoped: &str) -> &str {
        scoped.rsplit('\0').next().unwrap_or(scoped)
    }

    fn scoped_lock_process_session(scoped: &str) -> Option<(String, String)> {
        let mut parts = scoped.splitn(3, '\0');
        let node_id = parts.next()?;
        let session_id = parts.next()?;
        let raw_session = parts.next()?;
        (!node_id.is_empty() && !session_id.is_empty() && !raw_session.is_empty())
            .then(|| (node_id.to_owned(), session_id.to_owned()))
    }

    fn scoped_local_lock_owner(&self, owner: FileLockOwner) -> Result<FileLockOwner> {
        Ok(FileLockOwner {
            ingress_session_id: Self::lock_scope(
                &self.node_id,
                &self.session_id,
                &owner.ingress_session_id,
            )?,
            kernel_owner: owner.kernel_owner,
        })
    }

    fn scoped_local_lock_waiter(&self, waiter: LockWaiterId) -> Result<LockWaiterId> {
        Ok(LockWaiterId {
            ingress_session_id: Self::lock_scope(
                &self.node_id,
                &self.session_id,
                &waiter.ingress_session_id,
            )?,
            request_id: waiter.request_id,
        })
    }

    fn scoped_remote_lock_owner(
        peer: &str,
        caller_session_id: &str,
        owner: afs_protocol::node_control::DfsOwnerLockOwner,
    ) -> Result<FileLockOwner> {
        if owner.ingress_session_id.is_empty() {
            return Err(invalid("DFS lock owner session is empty"));
        }
        Ok(FileLockOwner {
            ingress_session_id: Self::lock_scope(
                peer,
                caller_session_id,
                &owner.ingress_session_id,
            )?,
            kernel_owner: owner.kernel_owner,
        })
    }

    fn scoped_remote_lock_waiter(
        peer: &str,
        caller_session_id: &str,
        waiter: afs_protocol::node_control::DfsOwnerLockWaiter,
    ) -> Result<LockWaiterId> {
        if waiter.ingress_session_id.is_empty() {
            return Err(invalid("DFS lock waiter session is empty"));
        }
        Ok(LockWaiterId {
            ingress_session_id: Self::lock_scope(
                peer,
                caller_session_id,
                &waiter.ingress_session_id,
            )?,
            request_id: waiter.request_id,
        })
    }

    fn scoped_local_lock_request(&self, mut request: LockRequest) -> Result<LockRequest> {
        request.owner = self.scoped_local_lock_owner(request.owner)?;
        Ok(request)
    }

    fn scoped_remote_lock_request(
        peer: &str,
        caller_session_id: &str,
        request: afs_protocol::node_control::DfsOwnerLockRequest,
    ) -> Result<LockRequest> {
        Ok(LockRequest {
            kind: Self::file_lock_kind_from_wire(request.kind)?,
            owner: Self::scoped_remote_lock_owner(
                peer,
                caller_session_id,
                request
                    .owner
                    .ok_or_else(|| invalid("DFS lock owner is missing"))?,
            )?,
            pid: request.pid,
            range: Self::file_lock_range_from_wire(
                request
                    .range
                    .ok_or_else(|| invalid("DFS lock range is missing"))?,
            )?,
            lock_type: Self::file_lock_type_from_wire(request.lock_type)?,
        })
    }

    fn validate_lock_authority(
        &self,
        peer: &str,
        authority: &afs_protocol::node_control::DfsOwnerLockAuthority,
    ) -> Result<WriteLease> {
        if authority.namespace_id != self.namespace_id.0
            || authority.owner_node_id != self.node_id
            || authority.owner_session_id != self.session_id
            || authority.caller_node_id != peer
            || authority.caller_session_id.is_empty()
            || authority.lease_epoch == 0
        {
            return Err(stale(
                "DFS lock authority identity does not match this owner",
            ));
        }
        if peer.contains('\0') || authority.caller_session_id.contains('\0') {
            return Err(invalid("DFS lock authority contains an invalid NUL byte"));
        }
        match self.meta.current_node_session(peer)? {
            Some(session_id) if session_id == authority.caller_session_id => {}
            _ => {
                return Err(stale(
                    "DFS lock authority caller session is no longer current",
                ));
            }
        }
        let lease = WriteLease {
            inode_id: InodeId::new(authority.inode_id.clone()),
            owner_node_id: authority.owner_node_id.clone(),
            owner_session_id: authority.owner_session_id.clone(),
            lease_epoch: authority.lease_epoch,
            expires_at_unix_ms: authority.lease_expires_at_unix_ms,
        };
        let renewed = self.meta.renew_write_lease(lease)?;
        if renewed.owner_node_id != self.node_id
            || renewed.owner_session_id != self.session_id
            || renewed.lease_epoch != authority.lease_epoch
        {
            return Err(stale("DFS lock authority lease is no longer current"));
        }
        Ok(renewed)
    }

    fn cached_local_authority_from_wire(
        &self,
        peer: &str,
        authority: afs_protocol::node_control::DfsOwnerLockAuthority,
    ) -> Result<Option<(InodeId, Arc<DfsLockAuthority>)>> {
        if authority.namespace_id != self.namespace_id.0
            || authority.owner_node_id != self.node_id
            || authority.owner_session_id != self.session_id
            || authority.caller_node_id != peer
            || authority.caller_session_id.is_empty()
            || authority.lease_epoch == 0
        {
            return Err(stale(
                "DFS lock authority identity does not match this owner",
            ));
        }
        if peer.contains('\0') || authority.caller_session_id.contains('\0') {
            return Err(invalid("DFS lock authority contains an invalid NUL byte"));
        }
        let inode_id = InodeId::new(authority.inode_id);
        let existing = self
            .lock_authorities
            .read(&inode_id)
            .map_err(|_| unavailable("DFS lock authority table is poisoned"))?
            .get(&inode_id)
            .cloned();
        let Some(existing) = existing else {
            return Ok(None);
        };
        let epoch = existing
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
            .lease
            .lease_epoch;
        if epoch != authority.lease_epoch {
            return Ok(None);
        }
        Ok(Some((inode_id, existing)))
    }

    fn local_authority_from_wire(
        &self,
        peer: &str,
        authority: afs_protocol::node_control::DfsOwnerLockAuthority,
    ) -> Result<(InodeId, Arc<DfsLockAuthority>)> {
        let lease = self.validate_lock_authority(peer, &authority)?;
        let inode_id = InodeId::new(authority.inode_id);
        Ok((
            inode_id.clone(),
            self.local_lock_authority(&inode_id, lease)?,
        ))
    }

    fn collect_peer_lock_scopes(&self) -> Result<HashMap<(String, String), HashSet<String>>> {
        let authorities = self.lock_authorities.values_snapshot()?;
        let mut scopes: HashMap<(String, String), HashSet<String>> = HashMap::new();
        for authority in authorities {
            let state = authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
            for scope in state
                .pinned_owners
                .iter()
                .map(|owner| owner.ingress_session_id.as_str())
                .chain(
                    state
                        .waiters
                        .iter()
                        .map(|waiter| waiter.ingress_session_id.as_str()),
                )
            {
                if let Some(peer_session) = Self::scoped_lock_process_session(scope) {
                    scopes
                        .entry(peer_session)
                        .or_default()
                        .insert(scope.to_owned());
                }
            }
        }
        Ok(scopes)
    }

    fn release_scoped_lock_sessions(&self, scopes: HashSet<String>) -> Result<usize> {
        if scopes.is_empty() {
            return Ok(0);
        }
        for scope in &scopes {
            self.note_closed_lock_session(scope)?;
        }
        let authorities = self.lock_authorities.values_snapshot()?;
        let mut first_error = None;
        for authority in authorities {
            for scope in &scopes {
                if let Err(error) = authority.table.release_session(scope).map_err(lock_error) {
                    first_error.get_or_insert(error);
                }
            }
            let mut state = authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
            state
                .pinned_owners
                .retain(|owner| !scopes.contains(&owner.ingress_session_id));
            state
                .waiters
                .retain(|waiter| !scopes.contains(&waiter.ingress_session_id));
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(scopes.len())
    }

    pub fn reap_expired_peer_lock_sessions(&self) -> Result<usize> {
        self.reclaim_idle_lock_authorities()?;
        let deadline = std::time::Instant::now() + LOCK_SESSION_REAP_BUDGET;
        let mut scoped_by_peer = self
            .collect_peer_lock_scopes()?
            .into_iter()
            .collect::<Vec<_>>();
        scoped_by_peer.sort_by(|left, right| left.0.cmp(&right.0));
        if !scoped_by_peer.is_empty() {
            let start = (self
                .lock_session_reap_cursor
                .fetch_add(1, Ordering::Relaxed) as usize)
                % scoped_by_peer.len();
            scoped_by_peer.rotate_left(start);
        }
        let mut reclaimed = 0usize;
        let mut first_error = None;
        let mut checked = 0usize;
        for ((node_id, session_id), scopes) in scoped_by_peer {
            if checked >= LOCK_SESSION_REAP_MAX_OPS || std::time::Instant::now() >= deadline {
                break;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let timeout = remaining.min(REMOTE_RELEASE_RPC_BUDGET);
            if timeout.is_zero() {
                break;
            }
            checked = checked.saturating_add(1);
            match self
                .meta
                .current_node_session_with_timeout(&node_id, timeout)
            {
                Ok(current) if current.as_deref() != Some(session_id.as_str()) => {
                    match self.release_scoped_lock_sessions(scopes) {
                        Ok(count) => reclaimed = reclaimed.saturating_add(count),
                        Err(error) => {
                            first_error.get_or_insert(error);
                        }
                    }
                }
                Ok(_) => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        match self.retry_closed_remote_lock_sessions_until(
            deadline,
            LOCK_SESSION_REAP_MAX_OPS.saturating_sub(checked),
        ) {
            Ok(count) => reclaimed = reclaimed.saturating_add(count),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
        match self
            .retry_cancelled_remote_lock_waiter_acks_until(deadline, LOCK_SESSION_REAP_MAX_OPS)
        {
            Ok(count) => reclaimed = reclaimed.saturating_add(count),
            Err(error) => {
                first_error.get_or_insert(error);
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(reclaimed)
    }

    fn start_lock_renewal_if_needed(&self) {
        self.lock_renewal.start(self.meta.clone());
    }

    fn renewal_lease_snapshot(
        &self,
        authority: &Arc<DfsLockAuthority>,
    ) -> Result<Option<WriteLease>> {
        let state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        if let Some(error) = state.last_error.clone() {
            return Err(error);
        }
        if !should_renew(&state.lease) {
            return Ok(None);
        }
        Ok(Some(state.lease.clone()))
    }

    fn check_lock_renewal_locked(&self, authority: &Arc<DfsLockAuthority>) -> Result<()> {
        let Some(lease) = self.renewal_lease_snapshot(authority)? else {
            return Ok(());
        };
        match self.meta.renew_write_lease(lease.clone()) {
            Ok(renewed) => match apply_lock_renewal_success(authority, renewed)? {
                Some(error) => Err(error),
                None => Ok(()),
            },
            Err(error) => match apply_lock_renewal_failure(authority, &lease, error.clone())? {
                Some(terminal) => Err(terminal),
                None => Err(error),
            },
        }
    }

    fn check_lock_renewal(&self, authority: &Arc<DfsLockAuthority>) -> Result<()> {
        let _renewal = lock_authority_renewal_guard(authority)?;
        self.check_lock_renewal_locked(authority)
    }

    fn check_lock_renewal_after_mutation(&self, authority: &Arc<DfsLockAuthority>) -> Result<()> {
        let _renewal = lock_authority_renewal_guard(authority)?;
        let Some(lease) = self.renewal_lease_snapshot(authority)? else {
            return Ok(());
        };
        match self.meta.renew_write_lease(lease.clone()) {
            Ok(renewed) => match apply_lock_renewal_success(authority, renewed)? {
                Some(error) => Err(error),
                None => Ok(()),
            },
            Err(error) => match apply_lock_renewal_failure(authority, &lease, error)? {
                Some(terminal) => Err(terminal),
                None => Ok(()),
            },
        }
    }

    fn register_local_lock_success(
        &self,
        authority: &Arc<DfsLockAuthority>,
        request: &LockRequest,
    ) -> Result<()> {
        if request.lock_type == FileLockType::Unlock {
            self.unpin_owner_if_inactive(authority, &request.owner, Some(request.kind))?;
            return Ok(());
        }
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        if let Some(error) = state.last_error.clone() {
            return Err(error);
        }
        state.pinned_owners.insert(request.owner.clone());
        drop(state);
        self.start_lock_renewal_if_needed();
        Ok(())
    }

    fn register_lock_waiter(
        &self,
        authority: &Arc<DfsLockAuthority>,
        waiter: &LockWaiterId,
    ) -> Result<()> {
        if self.consume_precancelled_lock_waiter(waiter)? {
            return Err(lock_error(LockError::Interrupted));
        }
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        if let Some(error) = state.last_error.clone() {
            return Err(error);
        }
        state.waiters.insert(waiter.clone());
        drop(state);
        self.start_lock_renewal_if_needed();
        Ok(())
    }

    fn unpin_lock_waiter(
        &self,
        authority: &Arc<DfsLockAuthority>,
        waiter: Option<&LockWaiterId>,
    ) -> Result<()> {
        let Some(waiter) = waiter else {
            return Ok(());
        };
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        state.waiters.remove(waiter);
        Ok(())
    }

    fn unpin_owner_if_inactive(
        &self,
        authority: &Arc<DfsLockAuthority>,
        owner: &FileLockOwner,
        kind: Option<FileLockKind>,
    ) -> Result<()> {
        if authority
            .table
            .has_owner_activity(owner, kind)
            .map_err(lock_error)?
        {
            return Ok(());
        }
        authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
            .pinned_owners
            .remove(owner);
        Ok(())
    }

    fn note_closed_lock_session(&self, ingress_session_id: &str) -> Result<()> {
        let mut sessions = self
            .closed_lock_sessions
            .lock()
            .map_err(|_| unavailable("DFS closed lock session table is poisoned"))?;
        // Closing a concrete session is cleanup, not new lock admission. Keep
        // this admission tombstone table bounded: when it is full, cleanup still
        // proceeds through the concrete lock tables and remote pending-close
        // state, but this global fast-path set is not expanded.
        if sessions.len() < MAX_CLOSED_LOCK_SESSIONS || sessions.contains(ingress_session_id) {
            sessions.insert(ingress_session_id.to_owned());
        } else {
            self.closed_lock_session_admission_closed
                .store(true, Ordering::Release);
        }
        Ok(())
    }

    fn ensure_lock_session_open(&self, ingress_session_id: &str) -> Result<()> {
        if self
            .closed_lock_sessions
            .lock()
            .map_err(|_| unavailable("DFS closed lock session table is poisoned"))?
            .contains(ingress_session_id)
        {
            return Err(lock_error(LockError::Interrupted));
        }
        if self
            .closed_lock_session_admission_closed
            .load(Ordering::Acquire)
        {
            return Err(lock_error(LockError::Capacity));
        }
        Ok(())
    }

    fn register_remote_lock_waiter_route(
        &self,
        waiter_id: &LockWaiterId,
        remote: DfsRemoteLockAuthority,
    ) -> Result<()> {
        let cancelled = self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?;
        if cancelled.contains(waiter_id) {
            return Err(lock_error(LockError::Interrupted));
        }
        let mut waiters = self
            .remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?;
        if waiters.contains_key(waiter_id) {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::EBUSY)));
        }
        if waiters.len() >= MAX_REMOTE_LOCK_WAITERS {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        waiters.insert(waiter_id.clone(), remote);
        Ok(())
    }

    fn remove_remote_lock_waiter_route(&self, waiter_id: &LockWaiterId) -> Result<()> {
        self.remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .remove(waiter_id);
        Ok(())
    }

    fn mark_cancelled_remote_lock_waiter(&self, waiter: &LockWaiterId) -> Result<()> {
        let mut cancelled = self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?;
        if cancelled.len() >= MAX_REMOTE_LOCK_WAITERS && !cancelled.contains(waiter) {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        cancelled.insert(waiter.clone());
        Ok(())
    }

    fn clear_cancelled_remote_lock_waiter(&self, waiter: &LockWaiterId) -> Result<()> {
        self.cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?
            .remove(waiter);
        Ok(())
    }

    fn cleanup_acknowledged_remote_lock_waiter(&self, waiter: &LockWaiterId) -> Result<()> {
        self.remove_remote_lock_waiter_route(waiter)?;
        self.clear_cancelled_remote_lock_waiter(waiter)
    }

    fn retry_cancelled_remote_lock_waiter_acks_until(
        &self,
        deadline: std::time::Instant,
        max_ops: usize,
    ) -> Result<usize> {
        let mut cancelled = self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        cancelled.sort_by(|left, right| {
            left.ingress_session_id
                .cmp(&right.ingress_session_id)
                .then_with(|| left.request_id.cmp(&right.request_id))
        });
        if !cancelled.is_empty() {
            let start = (self
                .lock_session_reap_cursor
                .fetch_add(1, Ordering::Relaxed) as usize)
                % cancelled.len();
            cancelled.rotate_left(start);
        }
        let waiters = self
            .remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .clone();
        let mut acknowledged = 0usize;
        let mut first_error = None;
        let mut attempted = 0usize;
        for waiter in cancelled {
            if attempted >= max_ops || std::time::Instant::now() >= deadline {
                break;
            }
            attempted = attempted.saturating_add(1);
            let Some(remote) = waiters.get(&waiter) else {
                continue;
            };
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let timeout = remaining.min(REMOTE_RELEASE_RPC_BUDGET);
            if timeout.is_zero() {
                break;
            }
            match remote.owner.acknowledge_lock_wait_with_timeout(
                afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                    authority: Some(remote.authority.clone()),
                    waiter: Some(Self::wire_lock_waiter(&waiter)),
                },
                timeout,
            ) {
                Ok(_) => {
                    let inode_id = InodeId::new(remote.authority.inode_id.clone());
                    self.cleanup_acknowledged_remote_lock_waiter(&waiter)?;
                    self.cleanup_remote_lock_authority_if_idle(&inode_id)?;
                    acknowledged = acknowledged.saturating_add(1);
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(acknowledged)
    }

    fn consume_precancelled_lock_waiter(&self, waiter: &LockWaiterId) -> Result<bool> {
        Ok(self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS lock cancellation table is poisoned"))?
            .contains(waiter))
    }

    fn note_precancelled_lock_waiter(&self, waiter: LockWaiterId) -> Result<()> {
        let mut cancelled = self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS lock cancellation table is poisoned"))?;
        if cancelled.len() >= MAX_REMOTE_LOCK_WAITERS && !cancelled.contains(&waiter) {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        cancelled.insert(waiter);
        Ok(())
    }

    fn note_remote_lock_session(&self, inode_id: &InodeId, session_id: &str) -> Result<bool> {
        let mut sessions = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?;
        if let Some(active_sessions) = sessions.get_mut(inode_id) {
            if let Some(state) = active_sessions.get(session_id) {
                if *state == REMOTE_LOCK_SESSION_PENDING_CLOSE {
                    return Err(lock_error(LockError::Interrupted));
                }
                return Ok(false);
            }
            if self
                .closed_lock_session_admission_closed
                .load(Ordering::Acquire)
            {
                return Err(lock_error(LockError::Capacity));
            }
            if active_sessions.len() >= MAX_REMOTE_LOCK_WAITERS {
                return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
            }
            active_sessions.insert(session_id.to_owned(), REMOTE_LOCK_SESSION_ACTIVE);
            return Ok(true);
        }
        if self
            .closed_lock_session_admission_closed
            .load(Ordering::Acquire)
        {
            return Err(lock_error(LockError::Capacity));
        }
        if sessions.len() >= MAX_REMOTE_LOCK_AUTHORITIES {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        sessions.insert(
            inode_id.clone(),
            HashMap::from([(session_id.to_owned(), REMOTE_LOCK_SESSION_ACTIVE)]),
        );
        Ok(true)
    }

    fn mark_remote_lock_session_pending_close(
        &self,
        inode_id: &InodeId,
        session_id: &str,
    ) -> Result<bool> {
        let mut sessions = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?;
        let Some(active_sessions) = sessions.get_mut(inode_id) else {
            return Ok(false);
        };
        let Some(state) = active_sessions.get_mut(session_id) else {
            return Ok(false);
        };
        *state = REMOTE_LOCK_SESSION_PENDING_CLOSE;
        Ok(true)
    }

    fn forget_remote_lock_session(&self, inode_id: &InodeId, session_id: &str) -> Result<()> {
        let mut sessions = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?;
        if let Some(active_sessions) = sessions.get_mut(inode_id) {
            active_sessions.remove(session_id);
            if active_sessions.is_empty() {
                sessions.remove(inode_id);
            }
        }
        Ok(())
    }

    fn remote_waiter_inodes(&self) -> Result<HashSet<InodeId>> {
        Ok(self
            .remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .values()
            .map(|remote| InodeId::new(remote.authority.inode_id.clone()))
            .collect())
    }

    fn cleanup_remote_lock_authority_if_idle(&self, inode_id: &InodeId) -> Result<()> {
        let has_sessions = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?
            .get(inode_id)
            .is_some_and(|sessions| !sessions.is_empty());
        if has_sessions {
            return Ok(());
        }
        if self.remote_waiter_inodes()?.contains(inode_id) {
            return Ok(());
        }
        self.remote_lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .remove(inode_id);
        Ok(())
    }

    fn cleanup_successful_remote_lock_session(
        &self,
        inode_id: &InodeId,
        scoped_session: &str,
    ) -> Result<()> {
        self.forget_remote_lock_session(inode_id, scoped_session)?;
        self.remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?
            .retain(|waiter, remote| {
                waiter.ingress_session_id != scoped_session
                    || remote.authority.inode_id != inode_id.0
            });
        self.cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?
            .retain(|waiter| waiter.ingress_session_id != scoped_session);
        self.cleanup_remote_lock_authority_if_idle(inode_id)
    }

    fn release_remote_lock_session_once(
        &self,
        inode_id: &InodeId,
        remote: &DfsRemoteLockAuthority,
        scoped_session: &str,
        timeout: Duration,
    ) -> Result<()> {
        remote.owner.release_lock_session_with_timeout(
            afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest {
                authority: Some(remote.authority.clone()),
                ingress_session_id: Self::raw_lock_session(scoped_session).to_owned(),
            },
            timeout,
        )?;
        self.cleanup_successful_remote_lock_session(inode_id, scoped_session)
    }

    fn retry_closed_remote_lock_sessions_until(
        &self,
        deadline: std::time::Instant,
        max_ops: usize,
    ) -> Result<usize> {
        let closed = self
            .closed_lock_sessions
            .lock()
            .map_err(|_| unavailable("DFS closed lock session table is poisoned"))?
            .clone();
        let mut pending = self
            .remote_lock_authority_sessions
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?
            .iter()
            .flat_map(|(inode_id, sessions)| {
                sessions
                    .iter()
                    .filter(|&(session, state)| {
                        *state == REMOTE_LOCK_SESSION_PENDING_CLOSE || closed.contains(session)
                    })
                    .map(|(session, _state)| (inode_id.clone(), session.clone()))
            })
            .collect::<Vec<_>>();
        pending.sort_by(|left, right| left.0.0.cmp(&right.0.0).then_with(|| left.1.cmp(&right.1)));
        if !pending.is_empty() {
            let start = (self
                .lock_session_reap_cursor
                .fetch_add(1, Ordering::Relaxed) as usize)
                % pending.len();
            pending.rotate_left(start);
        }
        let remotes = self
            .remote_lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .clone();
        let mut released = 0usize;
        let mut first_error = None;
        let mut attempted = 0usize;
        for (inode_id, scoped_session) in pending {
            if attempted >= max_ops || std::time::Instant::now() >= deadline {
                break;
            }
            attempted = attempted.saturating_add(1);
            let Some(remote) = remotes.get(&inode_id) else {
                if let Err(error) =
                    self.cleanup_successful_remote_lock_session(&inode_id, &scoped_session)
                {
                    first_error.get_or_insert(error);
                } else {
                    released = released.saturating_add(1);
                }
                continue;
            };
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let timeout = remaining.min(REMOTE_RELEASE_RPC_BUDGET);
            if timeout.is_zero() {
                break;
            }
            match self.release_remote_lock_session_once(&inode_id, remote, &scoped_session, timeout)
            {
                Ok(()) => released = released.saturating_add(1),
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(released)
    }

    fn take_remote_lock_waiter_route_or_precancel(
        &self,
        waiter: LockWaiterId,
    ) -> Result<Option<DfsRemoteLockAuthority>> {
        let mut cancelled = self
            .cancelled_remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock cancellation table is poisoned"))?;
        let waiters = self
            .remote_lock_waiters
            .lock()
            .map_err(|_| unavailable("DFS remote lock waiter table is poisoned"))?;
        if let Some(remote) = waiters.get(&waiter).cloned() {
            return Ok(Some(remote));
        }
        if cancelled.len() >= MAX_REMOTE_LOCK_WAITERS && !cancelled.contains(&waiter) {
            return Err(Error::from(std::io::Error::from_raw_os_error(libc::ENOLCK)));
        }
        cancelled.insert(waiter);
        Ok(None)
    }

    fn validate_lock_handle(
        &self,
        inode: BackendInode,
        handle: FileHandle,
        request: Option<&LockRequest>,
    ) -> Result<(InodeId, DfsFileHandleSnapshot)> {
        let inode_id = self.inode_id(inode)?;
        let snapshot = self.handle_snapshot(handle)?;
        if snapshot.inode_id != inode_id {
            return Err(bad_file_descriptor(
                "DFS lock handle belongs to another inode",
            ));
        }
        if let Some(request) = request
            && request.kind == FileLockKind::Posix
        {
            match request.lock_type {
                FileLockType::Read if snapshot.flags & libc::O_ACCMODE == libc::O_WRONLY => {
                    return Err(bad_file_descriptor(
                        "POSIX read lock requires a readable handle",
                    ));
                }
                FileLockType::Write if snapshot.flags & libc::O_ACCMODE == libc::O_RDONLY => {
                    return Err(bad_file_descriptor(
                        "POSIX write lock requires a writable handle",
                    ));
                }
                _ => {}
            }
        }
        Ok((inode_id, snapshot))
    }

    /// Execute pending background commits without changing the user-visible
    /// close contract. Failures remain attached to the inode and are reported
    /// once per open writer by a later write/flush/sync operation.
    pub fn writeback_pending(&self) -> Result<usize> {
        if self.native_pipeline.is_none() {
            return Ok(0);
        }
        for (_, cell) in self.inode_writes.snapshot()? {
            let mut state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            if state.dirty || state.in_flight.is_some() {
                state.native_due = true;
                state.native_due_through = state.native_due_through.max(state.visible_write_seq);
            }
        }
        self.progress_native_writeback()
    }

    pub fn renew_active_write_leases(&self) -> Result<usize> {
        // Open idle writers and unknown commits still depend on their lease.
        // Renew independently of writeback, without changing a frozen request.
        let mut entries = self.inode_writes.snapshot()?;
        entries.sort_by(|left, right| left.0.0.cmp(&right.0.0));
        if !entries.is_empty() {
            let start = self.write_lease_cursor.load(Ordering::Relaxed) as usize % entries.len();
            entries.rotate_left(start);
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut examined = 0;
        let mut renewed_count = 0;
        let mut first_error = None;
        for (_, cell) in entries {
            if Instant::now() >= deadline {
                break;
            }
            examined += 1;
            let lease = {
                let Ok(state) = cell.state.try_lock() else {
                    continue;
                };
                if state.terminal_error.is_some()
                    || !(state.open_writers > 0
                        || state.dirty
                        || state.metadata_dirty
                        || state.kill_suidgid_dirty
                        || state.in_flight.is_some())
                    || !should_background_renew(&state.write_lease)
                {
                    continue;
                }
                state.write_lease.clone()
            };
            match self.meta.renew_write_lease_with_timeout(
                lease.clone(),
                deadline.saturating_duration_since(Instant::now()),
            ) {
                Ok(renewed) if same_lease_identity(&lease, &renewed) => {
                    let mut state = cell
                        .lock()
                        .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                    // A late reply cannot replace a newer authority or shorten TTL.
                    if state.terminal_error.is_none()
                        && same_lease_identity(&state.write_lease, &lease)
                    {
                        state.write_lease = merge_same_identity_lease(&state.write_lease, renewed);
                        renewed_count += 1;
                    }
                }
                Ok(_) => {
                    first_error
                        .get_or_insert_with(|| stale("DFS writer renewal changed lease identity"));
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        self.write_lease_cursor
            .fetch_add(examined, Ordering::Relaxed);
        first_error.map_or(Ok(renewed_count), Err)
    }

    #[cfg(test)]
    fn pause_after_writeback_prepare(&self) {
        if let Some(duration) = self.writeback_after_prepare_pause.lock().unwrap().take() {
            std::thread::sleep(duration);
        }
    }

    fn freeze_state_for_commit(state: &mut InodeWriteState) -> FrozenCommit {
        let mut dirty_extents = std::mem::take(&mut state.dirty_extents);
        dirty_extents.freeze_builders();
        FrozenCommit {
            through_seq: state.visible_write_seq,
            logical_length: state.logical_length,
            inode: state.inode.clone(),
            write_lease: state.write_lease.clone(),
            base_version: state.base_version.clone(),
            base_layout: state.base_layout.clone(),
            dirty_extents,
            kill_suidgid_dirty: state.kill_suidgid_dirty,
        }
    }

    fn join_native_uploads_for_drain(&self) -> Result<Option<Error>> {
        let Some(pipeline) = &self.native_pipeline else {
            return Ok(None);
        };
        let mut first_error = None;
        loop {
            if let Some(error) = self.poll_native_uploads()? {
                first_error.get_or_insert(error);
            }
            if pipeline
                .tasks
                .lock()
                .map_err(|_| unavailable("native upload task table is poisoned"))?
                .is_empty()
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(first_error)
    }

    /// Best-effort graceful drain. It gives a clean Node shutdown a chance to
    /// publish dirty state, while an abrupt crash is still allowed to recover
    /// only the last committed FileVersion.
    pub fn drain(&self) -> Result<usize> {
        // Harvest existing Node-wide tasks before per-inode preparation can
        // consume a different owner's error. Node shutdown has closed admission.
        let mut join_error = self.join_native_uploads_for_drain()?;
        let result = (|| -> Result<usize> {
            let inode_ids = self.inode_writes.keys_snapshot()?;
            let mut committed = 0;
            for inode_id in inode_ids {
                loop {
                    let target_seq = self
                        .write_state(&inode_id)?
                        .ok_or_else(|| stale("DFS write state is no longer open"))?
                        .lock()
                        .map_err(|_| unavailable("DFS inode write state is poisoned"))?
                        .visible_write_seq;
                    let synced_seq = self
                        .write_state(&inode_id)?
                        .ok_or_else(|| stale("DFS write state is no longer open"))?
                        .lock()
                        .map_err(|_| unavailable("DFS inode write state is poisoned"))?
                        .committed_write_seq;
                    if synced_seq >= target_seq {
                        break;
                    }
                    if let Some(seq) = self.commit_inode(
                        &inode_id,
                        CommitReason::NODE_DRAIN,
                        CommitWait::AfterOperations,
                    )? {
                        self.update_handles_after_commit(&inode_id, seq)?;
                        committed += 1;
                    } else {
                        return Err(unavailable(
                            "DFS drain made no progress toward the accepted write sequence",
                        ));
                    }
                }
            }
            Ok(committed)
        })();
        if let Some(error) = self.join_native_uploads_for_drain()? {
            join_error.get_or_insert(error);
        }
        match result {
            Err(error) => Err(error),
            Ok(value) => match join_error {
                Some(error) => Err(error),
                None => Ok(value),
            },
        }
    }

    fn record_background_error(&self, inode_id: &InodeId, error: Error) -> Result<()> {
        let Some(state) = self.write_state(inode_id)? else {
            return Ok(());
        };
        let mut state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        let cursor = state
            .background_error
            .as_ref()
            .map_or(1, |previous| previous.cursor.saturating_add(1));
        afs_logging::warn!("dfs.background_error_recorded";
            "inode_id" => inode_id.0.as_str(),
            "cursor" => cursor,
            "error" => error.to_string()
        );
        state.background_error = Some(ObservedWriteError { cursor, error });
        Ok(())
    }
}

#[derive(Clone)]
struct DfsFileHandleSnapshot {
    handle: Arc<DfsHandle>,
    inode_id: InodeId,
    opened_inode: Arc<InodeRecord>,
    native_read_view: Option<NativeReadView>,
    native_read_generation: u64,
    native_denial_serial: u64,
    write_session: Option<DfsRemoteWriteSession>,
    flags: i32,
    owner_scope: Option<DfsOwnerHandleScope>,
}

impl DfsFileHandleSnapshot {
    fn from_handle(handle: &Arc<DfsHandle>) -> Result<Self> {
        let state = handle
            .state
            .lock()
            .map_err(|_| unavailable("DFS handle state is poisoned"))?;
        if handle.closed.load(Ordering::Acquire) {
            return Err(stale("DFS file handle is no longer open"));
        }
        Ok(Self {
            handle: handle.clone(),
            inode_id: handle.inode_id.clone(),
            opened_inode: state.opened_inode.clone(),
            native_read_view: state.native_read_view.clone(),
            native_read_generation: state.native_read_generation,
            native_denial_serial: state.native_denial_serial,
            write_session: state.write_session.clone(),
            flags: handle.flags,
            owner_scope: handle.owner_scope.clone(),
        })
    }
}

impl DirtyExtentMap {
    fn native_selection(
        &self,
        position: u64,
        logical_length: u64,
    ) -> Option<(usize, usize, usize)> {
        let latest = self
            .extents
            .range(..=position)
            .next_back()
            .map(|(_, extent)| extent)
            .filter(|extent| position < extent.file_offset + extent.length);
        if let Some(extent) = latest
            && let Some(index) = extent.builder
        {
            return Some((
                index,
                extent.source_offset + (position - extent.file_offset) as usize,
                (extent.native_reuse_end - position) as usize,
            ));
        }
        if position >= logical_length {
            return self
                .append_builders
                .get(&position)
                .and_then(|indices| indices.last())
                .map(|(_, index)| {
                    let entry = &self.builders[*index];
                    (
                        *index,
                        entry.builder.len(),
                        entry.builder.writable_capacity() - entry.builder.len(),
                    )
                });
        }
        None
    }

    fn native_capacity(position: u64, remaining: usize, logical_length: u64) -> usize {
        if position >= logical_length {
            COMMIT_CHUNK_BYTES as usize
        } else {
            remaining
                .min(COMMIT_CHUNK_BYTES as usize)
                .max(4096)
                .next_power_of_two()
        }
    }

    fn write_native_prepared(
        &mut self,
        position: u64,
        data: &[u8],
        write_seq: u64,
        selection: Option<(usize, usize, usize)>,
        prepared: Option<ChunkBuilder>,
        descriptor: Option<Arc<Mutex<NativeMetadataLease>>>,
    ) -> Result<usize> {
        self.promote_native();
        let (index, source_offset, available) = if let Some(selection) = selection {
            selection
        } else {
            let builder =
                prepared.ok_or_else(|| unavailable("native write has no admitted builder"))?;
            let capacity = builder.writable_capacity();
            let index = self
                .builders
                .iter()
                .position(|entry| {
                    entry.builder.writable_capacity() == 0 && entry.descriptor.is_none()
                })
                .unwrap_or(self.builders.len());
            let creation_order = self.next_builder_order;
            self.next_builder_order = self
                .next_builder_order
                .checked_add(1)
                .ok_or_else(file_too_large)?;
            let entry = NativeDirtyBuilder {
                file_offset: position,
                builder,
                descriptor,
                creation_order,
            };
            if index == self.builders.len() {
                self.builders.push(entry);
            } else {
                self.builders[index] = entry;
            }
            (index, 0, capacity)
        };
        let take = available.min(data.len());
        let previous_end =
            self.builders[index].file_offset + self.builders[index].builder.len() as u64;
        self.builders[index]
            .builder
            .write_at(source_offset as u64, &data[..take])?;
        self.update_append_builder(index, Some(previous_end));
        self.insert_native(DirtyExtent {
            file_offset: position,
            length: take as u64,
            write_seq,
            data: None,
            builder: Some(index),
            source_offset,
            native_reuse_end: position + take as u64,
            native_upload: None,
            native_descriptor: self.builders[index].descriptor.clone(),
        });
        let admitted = self.native_admitted_bytes.get_or_insert(0);
        *admitted = admitted.saturating_add(take as u64);
        Ok(take)
    }

    fn iter_extents(&self) -> impl DoubleEndedIterator<Item = &DirtyExtent> {
        self.extents.values()
    }

    fn iter_extents_mut(&mut self) -> impl Iterator<Item = &mut DirtyExtent> {
        self.extents.values_mut()
    }

    fn range_start(&self, offset: u64) -> u64 {
        self.extents
            .range(..=offset)
            .next_back()
            .map_or(offset, |(start, extent)| {
                if extent.file_offset + extent.length > offset {
                    *start
                } else {
                    offset
                }
            })
    }

    fn intersecting_extents(&self, offset: u64, end: u64) -> impl Iterator<Item = &DirtyExtent> {
        self.extents
            .range(self.range_start(offset)..end)
            .map(|(_, extent)| extent)
    }

    // Caller has already checked the new extent's endpoint. Only the two
    // boundary fragments survive; masked owners are dropped without touching
    // immutable snapshots, frozen batches or asynchronous holders.
    fn insert_native(&mut self, extent: DirtyExtent) {
        let start = extent.file_offset;
        let end = start + extent.length;
        if start == end {
            return;
        }
        let keys = self
            .extents
            .range(self.range_start(start)..end)
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        for key in keys {
            let Some(previous) = self.extents.remove(&key) else {
                continue;
            };
            let previous_end = previous.file_offset + previous.length;
            if previous.file_offset < start {
                let mut left = previous.clone();
                left.length = start - previous.file_offset;
                self.extents.insert(left.file_offset, left);
            }
            if previous_end > end {
                let mut right = previous;
                if right.data.is_some() || right.builder.is_some() || right.native_upload.is_some()
                {
                    right.source_offset += (end - right.file_offset) as usize;
                }
                right.file_offset = end;
                right.length = previous_end - end;
                self.extents.insert(end, right);
            }
        }
        self.extents.insert(start, extent);
    }

    fn update_append_builder(&mut self, index: usize, previous_end: Option<u64>) {
        if let Some(end) = previous_end
            && let Some(indices) = self.append_builders.get_mut(&end)
        {
            indices.remove(&(self.builders[index].creation_order, index));
            if indices.is_empty() {
                self.append_builders.remove(&end);
            }
        }
        let entry = &self.builders[index];
        if entry.builder.len() < entry.builder.writable_capacity() {
            self.append_builders
                .entry(entry.file_offset + entry.builder.len() as u64)
                .or_default()
                .insert((entry.creation_order, index));
        }
    }

    fn promote_native(&mut self) {
        self.native_admitted_bytes.get_or_insert(0);
    }

    /// Test driver for map-level index and builder tests. Production native writes
    /// go through `write_native_prepared`; the pipeline-less native write path that
    /// used this was removed in S1a-1 (R53), and R33 decides its fate in S1b.
    #[cfg(test)]
    fn write_native_at(
        &mut self,
        offset: u64,
        data: &[u8],
        write_seq: u64,
        pool: &Arc<WriteBufferPool>,
        logical_length: u64,
    ) -> Result<usize> {
        offset
            .checked_add(data.len() as u64)
            .ok_or_else(file_too_large)?;
        self.promote_native();
        let mut accepted = 0;
        while accepted < data.len() {
            let position = offset + accepted as u64;
            let latest = self
                .extents
                .range(..=position)
                .next_back()
                .map(|(_, extent)| extent)
                .filter(|extent| position < extent.file_offset + extent.length);
            let existing = latest.and_then(|extent| {
                extent.builder.map(|index| {
                    let source = extent.source_offset + (position - extent.file_offset) as usize;
                    (index, source, (extent.native_reuse_end - position) as usize)
                })
            });
            let append = if existing.is_none() && position >= logical_length {
                self.append_builders
                    .get(&position)
                    .and_then(|indices| indices.last())
                    .map(|(_, index)| {
                        let entry = &self.builders[*index];
                        let length = entry.builder.len();
                        (*index, length, entry.builder.writable_capacity() - length)
                    })
            } else {
                None
            };
            let (index, source_offset, available) = if let Some(existing) = existing.or(append) {
                existing
            } else {
                let remaining = data.len() - accepted;
                let capacity = if position >= logical_length {
                    COMMIT_CHUNK_BYTES as usize
                } else {
                    remaining
                        .min(COMMIT_CHUNK_BYTES as usize)
                        .max(4096)
                        .next_power_of_two()
                };
                let builder = match ChunkBuilder::with_pool(pool.clone(), capacity) {
                    Ok(builder) => builder,
                    Err(_error) if accepted > 0 => return Ok(accepted),
                    Err(error) => return Err(error),
                };
                let index = self.builders.len();
                self.builders.push(NativeDirtyBuilder {
                    file_offset: position,
                    builder,
                    descriptor: None,
                    creation_order: index as u64,
                });
                (index, 0, capacity)
            };
            let take = available.min(data.len() - accepted);
            let previous_end =
                self.builders[index].file_offset + self.builders[index].builder.len() as u64;
            self.builders[index]
                .builder
                .write_at(source_offset as u64, &data[accepted..accepted + take])?;
            self.update_append_builder(index, Some(previous_end));
            self.insert_native(DirtyExtent {
                file_offset: position,
                length: take as u64,
                write_seq,
                data: None,
                builder: Some(index),
                source_offset,
                native_reuse_end: position + take as u64,
                native_upload: None,
                native_descriptor: None,
            });
            let admitted = self.native_admitted_bytes.get_or_insert(0);
            *admitted = admitted.saturating_add(take as u64);
            accepted += take;
        }
        Ok(accepted)
    }

    // Consuming builders is the only path that exposes their allocation to
    // asynchronous holders. No writable alias survives this transition.
    fn freeze_builders(&mut self) {
        let bodies = std::mem::take(&mut self.builders)
            .into_iter()
            .map(|entry| entry.builder.freeze())
            .collect::<Vec<_>>();
        self.append_builders.clear();
        for extent in self.iter_extents_mut() {
            if let Some(index) = extent.builder.take() {
                extent.data = Some(bodies[index].clone());
            }
        }
    }

    // Called while the inode mutex is held. Copy only bytes intersecting this
    // read; a later write may mutate its builder while network reads run.
    fn snapshot_range(&self, offset: u64, length: usize) -> Result<Self> {
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| invalid("dirty snapshot range overflow"))?;
        let mut snapshot = Self {
            native_admitted_bytes: self.native_admitted_bytes.map(|_| 0),
            ..Self::default()
        };
        for extent in self.intersecting_extents(offset, end) {
            let start = offset.max(extent.file_offset);
            let stop = end.min(
                extent
                    .file_offset
                    .checked_add(extent.length)
                    .ok_or_else(|| invalid("dirty snapshot extent overflow"))?,
            );
            if start >= stop {
                continue;
            }
            let mut copied = extent.clone();
            copied.file_offset = start;
            copied.length = stop - start;
            let source = extent.source_offset
                + usize::try_from(start - extent.file_offset)
                    .map_err(|_| invalid("dirty snapshot source overflow"))?;
            if let Some(index) = extent.builder {
                let bytes = &self.builders[index].builder.as_bytes();
                // Holes installed by truncate can mask an obsolete tail. Do not
                // even read that tail into the snapshot before applying holes.
                let available = bytes
                    .len()
                    .saturating_sub(source)
                    .min(copied.length as usize);
                let mut body = vec![0; copied.length as usize];
                if available > 0 {
                    body[..available].copy_from_slice(&bytes[source..source + available]);
                }
                tracing::info_span!(
                    "node.dfs.write_buffer.read_snapshot_copy",
                    bytes = available as u64
                )
                .in_scope(|| {});
                copied.data = Some(SharedChunkBytes::from_vec(body));
                copied.source_offset = 0;
                copied.builder = None;
            } else {
                copied.source_offset = source;
            }
            snapshot.extents.insert(copied.file_offset, copied);
        }
        Ok(snapshot)
    }

    fn resize(&mut self, old_length: u64, new_length: u64, write_seq: u64) -> Result<()> {
        if new_length < old_length {
            for entry in &mut self.builders {
                let keep = new_length
                    .saturating_sub(entry.file_offset)
                    .min(entry.builder.len() as u64) as usize;
                entry.builder.truncate(keep);
            }
            self.append_builders.clear();
            for index in 0..self.builders.len() {
                self.update_append_builder(index, None);
            }
            for extent in self.iter_extents_mut() {
                if extent.builder.is_some() {
                    extent.length = extent
                        .length
                        .min(new_length.saturating_sub(extent.file_offset));
                    extent.native_reuse_end = extent.native_reuse_end.min(new_length);
                }
            }
            self.extents.retain(|_, extent| extent.length > 0);
        }
        let (start, stop) = if new_length < old_length {
            (new_length, old_length)
        } else {
            (old_length, new_length)
        };
        if start < stop {
            let hole = DirtyExtent {
                file_offset: start,
                length: stop - start,
                write_seq,
                data: None,
                builder: None,
                source_offset: 0,
                native_reuse_end: 0,
                native_upload: None,
                native_descriptor: None,
            };
            self.insert_native(hole);
        }
        Ok(())
    }

    #[cfg(test)]
    fn overlay(&self, offset: u64, out: &mut [u8]) -> Result<()> {
        self.overlay_native(offset, out, None)
    }

    fn overlay_native(
        &self,
        offset: u64,
        out: &mut [u8],
        store: Option<&dyn ChunkStore>,
    ) -> Result<()> {
        let end = offset
            .checked_add(out.len() as u64)
            .ok_or_else(|| invalid("dirty read range overflow"))?;
        for extent in self.intersecting_extents(offset, end) {
            let extent_end = extent
                .file_offset
                .checked_add(extent.length)
                .ok_or_else(|| invalid("dirty extent range overflow"))?;
            let start = offset.max(extent.file_offset);
            let stop = end.min(extent_end);
            if start >= stop {
                continue;
            }
            let output_start = usize::try_from(start - offset)
                .map_err(|_| invalid("dirty output offset is too large"))?;
            let length = usize::try_from(stop - start)
                .map_err(|_| invalid("dirty overlay length is too large"))?;
            if let Some(upload) = &extent.native_upload {
                let store = store
                    .ok_or_else(|| unavailable("native uploaded read requires its local store"))?;
                let source = extent.source_offset as u64 + start - extent.file_offset;
                if upload.read_at(store, source, &mut out[output_start..output_start + length])?
                    != length
                {
                    return Err(unavailable("native uploaded local read ended early"));
                }
                continue;
            }
            match &extent.data {
                Some(data) => {
                    let data_start = extent.source_offset
                        + usize::try_from(start - extent.file_offset)
                            .map_err(|_| invalid("dirty data offset is too large"))?;
                    out[output_start..output_start + length]
                        .copy_from_slice(&data[data_start..data_start + length]);
                }
                None => out[output_start..output_start + length].fill(0),
            }
        }
        Ok(())
    }

    fn restore_before(&mut self, mut older: DirtyExtentMap) {
        if self.native_operation_id.is_none() {
            self.native_operation_id = older.native_operation_id.take();
            self.native_gc_pin = older.native_gc_pin.take();
        }
        // The restored batch has already frozen its owners. Mutable builder
        // indices belong only to the active map and must never be renumbered.
        let mut ordered = std::mem::take(&mut older.extents)
            .into_values()
            .chain(std::mem::take(&mut self.extents).into_values())
            .collect::<Vec<_>>();
        ordered.sort_by_key(|extent| extent.write_seq);
        for extent in ordered {
            self.insert_native(extent);
        }
        self.native_admitted_bytes = match (older.native_admitted_bytes, self.native_admitted_bytes)
        {
            (None, None) => None,
            (left, right) => Some(left.unwrap_or(0).saturating_add(right.unwrap_or(0))),
        };
    }

    fn dirty_data_bytes(&self) -> u64 {
        if let Some(accepted) = self.native_admitted_bytes {
            return accepted;
        }
        self.iter_extents()
            .filter(|extent| extent.data.is_some())
            .fold(0u64, |total, extent| total.saturating_add(extent.length))
    }

    fn is_empty(&self) -> bool {
        self.extents.is_empty()
    }
}

struct CommitPlanner;

impl CommitPlanner {
    fn plan(
        &self,
        frozen: &FrozenCommit,
        operation_id: OperationId,
        layout_id: LayoutRootId,
    ) -> Result<CommitPlan> {
        self.plan_with_integrity(frozen, operation_id, layout_id, false)
    }

    fn plan_with_integrity(
        &self,
        frozen: &FrozenCommit,
        operation_id: OperationId,
        layout_id: LayoutRootId,
        crc32c_writes: bool,
    ) -> Result<CommitPlan> {
        let overlay = normalized_overlay(&frozen.dirty_extents, frozen.logical_length)?;
        let mut extents = frozen
            .base_layout
            .inline_extents
            .iter()
            .filter_map(|extent| clip_extent(extent, frozen.logical_length))
            .collect::<Vec<_>>();
        for segment in &overlay {
            extents = subtract_range(extents, segment.file_offset, segment.length)?;
        }

        let mut visible_sources: HashMap<(u64, usize, usize), Vec<(usize, usize)>> = HashMap::new();
        for segment in &overlay {
            let Some((bytes, source_offset)) = &segment.data else {
                continue;
            };
            let length = usize::try_from(segment.length)
                .map_err(|_| invalid("native source range is too large"))?;
            let end = source_offset
                .checked_add(length)
                .filter(|end| *end <= bytes.len())
                .ok_or_else(|| invalid("native source range exceeds immutable body"))?;
            let identity = (bytes.allocation_id(), bytes.as_ptr() as usize, bytes.len());
            visible_sources
                .entry(identity)
                .or_default()
                .push((*source_offset, end));
        }
        for ranges in visible_sources.values_mut() {
            ranges.sort_unstable();
            let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
            for (start, end) in ranges.drain(..) {
                if let Some(previous) = merged.last_mut()
                    && start <= previous.1
                {
                    previous.1 = previous.1.max(end);
                } else {
                    merged.push((start, end));
                }
            }
            *ranges = merged;
        }
        let mut staged_chunks = Vec::new();
        let mut staged_by_source = HashMap::new();
        for segment in overlay {
            let Some((bytes, source_offset)) = segment.data else {
                continue;
            };
            let identity = (bytes.allocation_id(), bytes.as_ptr() as usize, bytes.len());
            let source_end = source_offset + segment.length as usize;
            let (start, end) = visible_sources
                .get(&identity)
                .and_then(|ranges| {
                    ranges
                        .iter()
                        .copied()
                        .find(|(start, end)| *start <= source_offset && source_end <= *end)
                })
                .ok_or_else(|| invalid("native source has no final visible interval"))?;
            // Only final visible contiguous source ranges are hashed and
            // persisted. Restored frozen tails and overwritten interiors
            // stay owned but never become new submitted payload bytes.
            let key = (identity, start, end);
            let staged_index = match staged_by_source.get(&key) {
                Some(index) => *index,
                None => {
                    let index = staged_chunks.len();
                    let body = bytes.slice(start..end);
                    staged_chunks.push(if crc32c_writes {
                        StagedChunk::from_shared_crc32c(operation_id.clone(), body)?
                    } else {
                        StagedChunk::from_shared(operation_id.clone(), body)
                    });
                    staged_by_source.insert(key, index);
                    index
                }
            };
            let staged: &StagedChunk = &staged_chunks[staged_index];
            extents.push(Extent {
                file_offset: segment.file_offset,
                length: segment.length,
                chunk_id: staged.chunk.id.clone(),
                chunk_offset: (source_offset - start) as u64,
            });
        }
        extents.sort_by_key(|extent| extent.file_offset);
        let mut coalesced: Vec<Extent> = Vec::with_capacity(extents.len());
        for extent in extents {
            if let Some(previous) = coalesced.last_mut()
                && previous.file_offset + previous.length == extent.file_offset
                && previous.chunk_id == extent.chunk_id
                && previous.chunk_offset + previous.length == extent.chunk_offset
            {
                previous.length += extent.length;
            } else {
                coalesced.push(extent);
            }
        }
        Ok(CommitPlan {
            layout_root: LayoutRoot {
                id: layout_id,
                file_length: frozen.logical_length,
                inline_extents: coalesced,
            },
            staged_chunks,
        })
    }
}

fn unique_staged_chunks(staged_chunks: Vec<StagedChunk>) -> Result<Vec<StagedChunk>> {
    let mut unique = Vec::with_capacity(staged_chunks.len());
    let mut chunks = HashMap::with_capacity(staged_chunks.len());
    for staged in staged_chunks {
        match chunks.get(&staged.chunk.id) {
            Some(existing) => {
                if existing != &staged.chunk {
                    return Err(invalid(
                        "content-addressed staged chunk id has inconsistent chunk metadata",
                    ));
                }
            }
            None => {
                chunks.insert(staged.chunk.id.clone(), staged.chunk.clone());
                unique.push(staged);
            }
        }
    }
    Ok(unique)
}

fn normalized_overlay(map: &DirtyExtentMap, file_length: u64) -> Result<Vec<OverlaySegment>> {
    map.extents
        .values()
        .filter(|extent| extent.file_offset < file_length && extent.length > 0)
        .map(|extent| {
            Ok(OverlaySegment {
                file_offset: extent.file_offset,
                length: extent.length.min(file_length - extent.file_offset),
                data: extent
                    .data
                    .as_ref()
                    .map(|bytes| (bytes.clone(), extent.source_offset)),
            })
        })
        .collect()
}

fn clip_extent(extent: &Extent, file_length: u64) -> Option<Extent> {
    if extent.file_offset >= file_length {
        return None;
    }
    let length = extent.length.min(file_length - extent.file_offset);
    (length > 0).then(|| Extent {
        file_offset: extent.file_offset,
        length,
        chunk_id: extent.chunk_id.clone(),
        chunk_offset: extent.chunk_offset,
    })
}

fn subtract_range(extents: Vec<Extent>, offset: u64, length: u64) -> Result<Vec<Extent>> {
    if length == 0 {
        return Ok(extents);
    }
    let end = offset
        .checked_add(length)
        .ok_or_else(|| invalid("subtracted extent range overflow"))?;
    let mut output = Vec::with_capacity(extents.len().saturating_add(1));
    for extent in extents {
        let extent_end = extent
            .file_offset
            .checked_add(extent.length)
            .ok_or_else(|| invalid("base extent range overflow"))?;
        if end <= extent.file_offset || offset >= extent_end {
            output.push(extent);
            continue;
        }
        if offset > extent.file_offset {
            output.push(Extent {
                file_offset: extent.file_offset,
                length: offset - extent.file_offset,
                chunk_id: extent.chunk_id.clone(),
                chunk_offset: extent.chunk_offset,
            });
        }
        if end < extent_end {
            output.push(Extent {
                file_offset: end,
                length: extent_end - end,
                chunk_id: extent.chunk_id,
                chunk_offset: extent
                    .chunk_offset
                    .checked_add(end - extent.file_offset)
                    .ok_or_else(|| invalid("base extent chunk offset overflow"))?,
            });
        }
    }
    Ok(output)
}

impl DistributedFs {
    fn remote_read(
        &self,
        provider: &RemoteDfsProviderIo,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let remote = &provider.remote;
        let handle = remote.read_handle.as_ref().unwrap_or(&remote.handle);
        let reply = remote
            .owner
            .read(afs_protocol::node_data::DfsOwnerReadRequest {
                handle: Some(Self::data_owner_handle(handle)),
                offset,
                length: out.len() as u64,
            })?;
        let count = reply.data.len().min(out.len());
        out[..count].copy_from_slice(&reply.data[..count]);
        Ok(count)
    }

    fn remote_getattr(&self, provider: &RemoteDfsProviderIo) -> Result<FileAttributes> {
        let remote = &provider.remote;
        let reply = remote
            .owner
            .getattr(afs_protocol::node_control::DfsOwnerGetAttrRequest {
                handle: Some(remote.handle.clone()),
            })?;
        let attr = reply
            .attr
            .ok_or_else(|| unavailable("DFS owner getattr returned no attributes"))?;
        file_attributes_from_owner_attr(attr)
    }

    fn remote_write(
        &self,
        session: &DfsRemoteWriteSession,
        flags: i32,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> Result<(usize, u64)> {
        let remote = session
            .remote
            .as_ref()
            .ok_or_else(|| invalid("DFS write session is local"))?;
        let operation_id = self.operation_id("remote-write");
        let reply = remote
            .owner
            .write(afs_protocol::node_data::DfsOwnerWriteRequest {
                handle: Some(Self::data_owner_handle(&remote.handle)),
                operation_id: operation_id.0,
                offset,
                data: data.to_vec(),
                append: flags & libc::O_APPEND != 0,
                kill_suidgid: options.kill_suidgid,
            })?;
        Ok((reply.written as usize, reply.accepted_write_seq))
    }

    fn remote_resize(
        &self,
        session: &DfsRemoteWriteSession,
        length: u64,
        options: SetAttrOptions,
    ) -> Result<u64> {
        let remote = session
            .remote
            .as_ref()
            .ok_or_else(|| invalid("DFS write session is local"))?;
        let reply = remote
            .owner
            .resize(afs_protocol::node_data::DfsOwnerResizeRequest {
                handle: Some(Self::data_owner_handle(&remote.handle)),
                operation_id: self.operation_id("remote-resize").0,
                length,
                kill_suidgid: options.kill_suidgid,
            })?;
        Ok(reply.accepted_write_seq)
    }

    fn remote_sync(
        &self,
        session: &DfsRemoteWriteSession,
        mode: SyncMode,
    ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
        let remote = session
            .remote
            .as_ref()
            .ok_or_else(|| invalid("DFS write session is local"))?;
        remote
            .owner
            .sync(afs_protocol::node_data::DfsOwnerSyncRequest {
                handle: Some(Self::data_owner_handle(&remote.handle)),
                operation_id: self.operation_id("remote-sync").0,
                through_write_seq: session.local.last_accepted_seq,
                data_only: matches!(mode, SyncMode::DataOnly),
            })
    }

    fn remote_release(&self, session: &DfsRemoteWriteSession) -> Result<()> {
        let remote = session
            .remote
            .as_ref()
            .ok_or_else(|| invalid("DFS write session is local"))?;
        let mut first_error = None;
        let mut released = 0usize;
        if let Some(read_handle) = remote.read_handle.clone() {
            released = released.saturating_add(1);
            if let Err(error) =
                self.remote_release_handle_or_queue_reserved(&remote.owner, read_handle, true)
            {
                first_error = Some(error);
            }
        }
        released = released.saturating_add(1);
        if let Err(error) =
            self.remote_release_handle_or_queue_reserved(&remote.owner, remote.handle.clone(), true)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
        if remote.release_slots > released {
            self.release_remote_release_capacity(remote.release_slots - released)?;
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }

    fn remote_release_slot_count(open_flags: i32) -> usize {
        if open_flags & libc::O_ACCMODE == libc::O_WRONLY {
            2
        } else {
            1
        }
    }

    fn remote_release_key(
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> PendingRemoteReleaseKey {
        PendingRemoteReleaseKey {
            namespace_id: handle.namespace_id.clone(),
            inode_id: handle.inode_id.clone(),
            owner_node_id: handle.owner_node_id.clone(),
            owner_session_id: handle.owner_session_id.clone(),
            lease_epoch: handle.lease_epoch,
            caller_node_id: handle.caller_node_id.clone(),
            caller_session_id: handle.caller_session_id.clone(),
            open_seq: handle.open_seq,
            opaque_handle: handle.opaque_handle.clone(),
        }
    }

    fn cleanup_handle_for_open_request(
        caller_node_id: &str,
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerHandle> {
        let open_seq = Self::validate_owner_open_seq(request)?;
        Ok(afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: request.namespace_id.clone(),
            inode_id: request.inode_id.clone(),
            owner_node_id: request.owner_node_id.clone(),
            owner_session_id: request.owner_session_id.clone(),
            lease_epoch: request.lease_epoch,
            caller_node_id: caller_node_id.to_owned(),
            caller_session_id: request.caller_session_id.clone(),
            opaque_handle: Vec::new(),
            open_seq,
        })
    }

    fn reserve_remote_release_capacity(&self, slots: usize) -> Result<()> {
        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        if state
            .entries
            .len()
            .saturating_add(state.reserved)
            .saturating_add(slots)
            > MAX_PENDING_REMOTE_RELEASES
        {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS pending remote release table is full",
            ));
        }
        state.reserved = state.reserved.saturating_add(slots);
        Ok(())
    }

    fn release_remote_release_capacity(&self, slots: usize) -> Result<()> {
        if slots == 0 {
            return Ok(());
        }
        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        state.reserved = state.reserved.saturating_sub(slots);
        Ok(())
    }

    fn remote_release_handle_or_queue_reserved(
        &self,
        owner: &Arc<dyn RemoteDfsOwner>,
        handle: afs_protocol::node_control::DfsOwnerHandle,
        consume_reserved: bool,
    ) -> Result<()> {
        match Self::remote_release_handle(owner, &handle) {
            Ok(()) => {
                if consume_reserved {
                    self.release_remote_release_capacity(1)?;
                }
                Ok(())
            }
            Err(error) if Self::remote_release_error_is_retryable(&error) => {
                self.queue_pending_remote_release(owner.clone(), handle, consume_reserved)?;
                Err(error)
            }
            Err(error) => {
                if consume_reserved {
                    self.release_remote_release_capacity(1)?;
                }
                Err(error)
            }
        }
    }

    fn queue_pending_remote_release(
        &self,
        owner: Arc<dyn RemoteDfsOwner>,
        handle: afs_protocol::node_control::DfsOwnerHandle,
        consume_reserved: bool,
    ) -> Result<()> {
        self.queue_pending_remote_release_with_lifecycle(owner, handle, consume_reserved, None)
    }

    fn queue_pending_remote_release_with_lifecycle(
        &self,
        owner: Arc<dyn RemoteDfsOwner>,
        handle: afs_protocol::node_control::DfsOwnerHandle,
        consume_reserved: bool,
        lifecycle: Option<Arc<RemoteProviderLifecycle>>,
    ) -> Result<()> {
        let key = Self::remote_release_key(&handle);
        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        if consume_reserved {
            state.reserved = state.reserved.saturating_sub(1);
        } else if !state.entries.contains_key(&key)
            && state.entries.len().saturating_add(state.reserved) >= MAX_PENDING_REMOTE_RELEASES
        {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS pending remote release table is full",
            ));
        }
        if !state.entries.contains_key(&key) {
            state.order.push_back(key.clone());
        }
        state.entries.entry(key).or_insert(PendingRemoteRelease {
            owner,
            handle,
            lifecycle,
        });
        Ok(())
    }

    fn queue_remote_session_release_with_lifecycle(
        &self,
        remote: &RemoteDfsWriteSession,
    ) -> Result<()> {
        let mut handles = Vec::with_capacity(remote.release_slots);
        if let Some(read_handle) = remote.read_handle.clone() {
            handles.push(read_handle);
        }
        handles.push(remote.handle.clone());

        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        let new_entries = handles
            .iter()
            .filter(|handle| {
                let key = Self::remote_release_key(handle);
                !state.entries.contains_key(&key)
            })
            .count();
        let remaining_reserved = state.reserved.saturating_sub(handles.len());
        if state
            .entries
            .len()
            .saturating_add(new_entries)
            .saturating_add(remaining_reserved)
            > MAX_PENDING_REMOTE_RELEASES
        {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS pending remote release table is full",
            ));
        }
        state.reserved = state.reserved.saturating_sub(handles.len());
        for handle in handles {
            let key = Self::remote_release_key(&handle);
            if !state.entries.contains_key(&key) {
                state.order.push_back(key.clone());
            }
            state
                .entries
                .entry(key)
                .or_insert_with(|| PendingRemoteRelease {
                    owner: remote.owner.clone(),
                    handle,
                    lifecycle: Some(remote.lifecycle.clone()),
                });
        }
        Ok(())
    }

    pub fn retry_pending_remote_releases(&self) -> Result<usize> {
        self.retry_pending_remote_releases_with_budget(REMOTE_RELEASE_MAINTENANCE_BUDGET)
    }

    pub fn drain_pending_remote_releases(&self) -> Result<usize> {
        self.retry_pending_remote_releases_with_budget(REMOTE_RELEASE_SHUTDOWN_BUDGET)
    }

    fn retry_pending_remote_releases_with_budget(
        &self,
        budget: std::time::Duration,
    ) -> Result<usize> {
        let deadline = std::time::Instant::now() + budget;
        let mut released = 0usize;
        let mut first_error = None;
        let mut session_cache = HashMap::new();
        for (attempted, (key, pending)) in self
            .pending_remote_release_snapshot()?
            .into_iter()
            .enumerate()
        {
            if attempted >= REMOTE_RELEASE_RETRY_MAX_OPS || std::time::Instant::now() >= deadline {
                break;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let session_key = key.owner_node_id.clone();
            let current_session = if let Some(cached) = session_cache.get(&session_key).cloned() {
                cached
            } else {
                let timeout = remaining.min(REMOTE_RELEASE_RPC_BUDGET);
                let result = self
                    .meta
                    .current_node_session_with_timeout(&key.owner_node_id, timeout);
                session_cache.insert(session_key, result.clone());
                result
            };
            match current_session {
                Ok(Some(current)) if current == key.owner_session_id => {}
                Ok(_) => {
                    if self.remove_pending_remote_release_if_current(&key, &pending)? {
                        released = released.saturating_add(1);
                    }
                    continue;
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                    self.rotate_pending_remote_release(&key)?;
                    continue;
                }
            }
            if let Some(lifecycle) = pending.lifecycle.as_ref()
                && !lifecycle.is_idle()?
            {
                self.rotate_pending_remote_release(&key)?;
                first_error.get_or_insert_with(|| {
                    unavailable("DFS remote provider release is waiting for admitted IO")
                });
                continue;
            }

            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let timeout = remaining.min(REMOTE_RELEASE_RPC_BUDGET);
            match Self::remote_release_handle_with_timeout(&pending.owner, &pending.handle, timeout)
            {
                Ok(()) => {
                    if self.remove_pending_remote_release_if_current(&key, &pending)? {
                        released = released.saturating_add(1);
                    }
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                    self.rotate_pending_remote_release(&key)?;
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        let remaining = self.pending_remote_release_count()?;
        if remaining != 0 {
            return Err(unavailable(format!(
                "DFS pending remote release drain left {remaining} handle(s) queued"
            )));
        }
        Ok(released)
    }

    fn pending_remote_release_snapshot(
        &self,
    ) -> Result<Vec<(PendingRemoteReleaseKey, PendingRemoteRelease)>> {
        let state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        Ok(state
            .order
            .iter()
            .filter_map(|key| {
                state
                    .entries
                    .get(key)
                    .map(|pending| (key.clone(), pending.clone()))
            })
            .take(REMOTE_RELEASE_RETRY_MAX_OPS)
            .collect())
    }

    fn rotate_pending_remote_release(&self, key: &PendingRemoteReleaseKey) -> Result<()> {
        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        if let Some(position) = state.order.iter().position(|candidate| candidate == key)
            && let Some(key) = state.order.remove(position)
        {
            state.order.push_back(key);
        }
        Ok(())
    }

    fn remove_pending_remote_release_if_current(
        &self,
        key: &PendingRemoteReleaseKey,
        pending: &PendingRemoteRelease,
    ) -> Result<bool> {
        let mut state = self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?;
        let current_matches = state.entries.get(key).is_some_and(|current| {
            current.handle == pending.handle
                && Arc::ptr_eq(&current.owner, &pending.owner)
                && Self::same_pending_release_lifecycle(&current.lifecycle, &pending.lifecycle)
        });
        if current_matches {
            state.entries.remove(key);
            state.order.retain(|candidate| candidate != key);
        }
        Ok(current_matches)
    }

    fn same_pending_release_lifecycle(
        left: &Option<Arc<RemoteProviderLifecycle>>,
        right: &Option<Arc<RemoteProviderLifecycle>>,
    ) -> bool {
        match (left, right) {
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            (None, None) => true,
            _ => false,
        }
    }

    pub fn pending_remote_release_count(&self) -> Result<usize> {
        Ok(self
            .pending_remote_releases
            .lock()
            .map_err(|_| unavailable("DFS pending remote release table is poisoned"))?
            .entries
            .len())
    }

    fn remote_release_error_is_retryable(error: &Error) -> bool {
        matches!(
            error.kind(),
            afs_error::ErrorKind::Unavailable
                | afs_error::ErrorKind::DeadlineExceeded
                | afs_error::ErrorKind::ResourceExhausted
                | afs_error::ErrorKind::Aborted
        )
    }

    fn remote_release_handle(
        owner: &Arc<dyn RemoteDfsOwner>,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        owner.release(afs_protocol::node_control::DfsOwnerReleaseRequest {
            handle: Some(handle.clone()),
        })?;
        Ok(())
    }

    fn remote_release_handle_with_timeout(
        owner: &Arc<dyn RemoteDfsOwner>,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        timeout: std::time::Duration,
    ) -> Result<()> {
        owner.release_with_timeout(
            afs_protocol::node_control::DfsOwnerReleaseRequest {
                handle: Some(handle.clone()),
            },
            timeout,
        )?;
        Ok(())
    }

    fn same_remote_provider(left: &RemoteDfsWriteSession, right: &RemoteDfsWriteSession) -> bool {
        let left = &left.handle;
        let right = &right.handle;
        left.namespace_id == right.namespace_id
            && left.inode_id == right.inode_id
            && left.owner_node_id == right.owner_node_id
            && left.owner_session_id == right.owner_session_id
            && left.lease_epoch == right.lease_epoch
            && left.caller_node_id == right.caller_node_id
            && left.caller_session_id == right.caller_session_id
            && left.open_seq == right.open_seq
            && left.opaque_handle == right.opaque_handle
    }

    fn current_remote_provider(&self, inode_id: &InodeId) -> Result<Option<RemoteDfsWriteSession>> {
        self.remote_inode_providers
            .read(inode_id)
            .map_err(|_| unavailable("DFS remote owner table is poisoned"))
            .map(|providers| providers.get(inode_id).cloned())
    }

    fn replacement_remote_provider(
        &self,
        inode_id: &InodeId,
        released: &RemoteDfsWriteSession,
        preferred: Option<&RemoteDfsWriteSession>,
    ) -> Result<Option<RemoteDfsWriteSession>> {
        let handles = self.handles.for_inode(inode_id)?;
        let snapshots = handles
            .iter()
            .filter_map(|handle| match DfsFileHandleSnapshot::from_handle(handle) {
                Ok(snapshot) => Some(Ok(snapshot)),
                Err(error) if error.code() == afs_error::NODE_DFS_STALE_HANDLE => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<Vec<_>>>()?;
        if let Some(preferred) = preferred
            && !Self::same_remote_provider(preferred, released)
            && preferred.lifecycle.is_live()?
            && snapshots.iter().any(|handle| {
                handle.inode_id == *inode_id
                    && handle
                        .write_session
                        .as_ref()
                        .and_then(|session| session.remote.as_ref())
                        .is_some_and(|remote| Self::same_remote_provider(remote, preferred))
            })
        {
            return Ok(Some(preferred.clone()));
        }
        for handle in snapshots
            .iter()
            .filter(|handle| handle.inode_id == *inode_id)
        {
            let Some(remote) = handle
                .write_session
                .as_ref()
                .and_then(|session| session.remote.as_ref())
            else {
                continue;
            };
            if !Self::same_remote_provider(remote, released) && remote.lifecycle.is_live()? {
                return Ok(Some(remote.clone()));
            }
        }
        Ok(None)
    }

    fn replace_released_remote_provider(
        &self,
        inode_id: &InodeId,
        released: &RemoteDfsWriteSession,
        preferred: Option<&RemoteDfsWriteSession>,
    ) -> Result<()> {
        let replacement = self.replacement_remote_provider(inode_id, released, preferred)?;
        let mut providers = self
            .remote_inode_providers
            .write(inode_id)
            .map_err(|_| unavailable("DFS remote owner table is poisoned"))?;
        if providers
            .get(inode_id)
            .is_some_and(|current| Self::same_remote_provider(current, released))
        {
            if let Some(replacement) = replacement.filter(|candidate| {
                candidate.lifecycle.is_live().unwrap_or(false)
                    && !Self::same_remote_provider(candidate, released)
            }) {
                providers.insert(inode_id.clone(), replacement);
            } else {
                providers.remove(inode_id);
            }
        }
        let last_provider = !providers.contains_key(inode_id);
        drop(providers);
        self.handles.mark_dynamic(inode_id)?;
        if last_provider && !self.handles.for_inode(inode_id)?.is_empty() {
            // Writes this Node proxied are visible to its handles (4a): once
            // the last proxy session ends they pin the version it produced.
            let inode = self.validate_inode(self.meta.get_inode(inode_id)?)?;
            self.handles.advance_to_local_view(&inode)?;
        }
        Ok(())
    }

    fn retire_released_remote_provider(
        &self,
        inode_id: &InodeId,
        released: &RemoteDfsWriteSession,
        preferred: Option<&RemoteDfsWriteSession>,
    ) -> Result<()> {
        released.lifecycle.retire_new_io()?;
        let replace_result = self.replace_released_remote_provider(inode_id, released, preferred);
        let wait_result = released
            .lifecycle
            .wait_idle(REMOTE_PROVIDER_IDLE_WAIT_BUDGET);
        replace_result.and(wait_result)
    }

    fn close_remote_write_session(
        &self,
        inode_id: &InodeId,
        session: &DfsRemoteWriteSession,
    ) -> Result<()> {
        self.close_remote_write_session_with_provider(inode_id, session, None)
    }

    fn close_temporary_remote_write_session(
        &self,
        inode_id: &InodeId,
        session: &DfsRemoteWriteSession,
        previous_provider: Option<RemoteDfsWriteSession>,
    ) -> Result<()> {
        self.close_remote_write_session_with_provider(inode_id, session, previous_provider.as_ref())
    }

    fn close_remote_write_session_with_provider(
        &self,
        inode_id: &InodeId,
        session: &DfsRemoteWriteSession,
        previous_provider: Option<&RemoteDfsWriteSession>,
    ) -> Result<()> {
        let Some(remote) = session.remote.as_ref() else {
            return Err(invalid("DFS write session is local"));
        };
        if let Err(error) =
            self.retire_released_remote_provider(inode_id, remote, previous_provider)
        {
            self.queue_remote_session_release_with_lifecycle(remote)?;
            return Err(error);
        }
        self.remote_release(session)
    }

    fn remote_operation_cached(
        &self,
        key: &RemoteOperationKey,
        fingerprint: [u8; 32],
    ) -> Result<Option<RemoteOperationResult>> {
        let cached = self
            .remote_operation_results
            .lock()
            .map_err(|_| unavailable("DFS remote operation table is poisoned"))?
            .get(key)
            .cloned();
        let Some(result) = cached else {
            return Ok(None);
        };
        let cached_fingerprint = match &result {
            RemoteOperationResult::Write { fingerprint, .. }
            | RemoteOperationResult::Resize { fingerprint, .. }
            | RemoteOperationResult::Sync { fingerprint, .. } => *fingerprint,
        };
        if cached_fingerprint != fingerprint {
            return Err(invalid(
                "DFS remote operation id was reused with a different request body",
            ));
        }
        Ok(Some(result))
    }

    fn remember_remote_operation(
        &self,
        key: RemoteOperationKey,
        result: RemoteOperationResult,
    ) -> Result<()> {
        let mut operations = self
            .remote_operation_results
            .lock()
            .map_err(|_| unavailable("DFS remote operation table is poisoned"))?;
        if operations.len() >= MAX_REMOTE_OPERATION_RESULTS && !operations.contains_key(&key) {
            return Err(unavailable(
                "DFS remote operation table is full; retry later",
            ));
        }
        operations.insert(key, result);
        Ok(())
    }

    fn remote_operation_key(
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        operation_id: OperationId,
    ) -> RemoteOperationKey {
        RemoteOperationKey {
            peer: peer.to_owned(),
            caller_session_id: handle.caller_session_id.clone(),
            operation_id,
        }
    }

    fn fingerprint_remote_write(
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        offset: u64,
        append: bool,
        data: &[u8],
        kill_suidgid: bool,
    ) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        Self::fingerprint_handle(&mut hasher, handle);
        hasher.update(b"write");
        hasher.update(&offset.to_be_bytes());
        hasher.update(&[u8::from(append)]);
        hasher.update(&[u8::from(kill_suidgid)]);
        hasher.update(&(data.len() as u64).to_be_bytes());
        hasher.update(data);
        *hasher.finalize().as_bytes()
    }

    fn fingerprint_remote_resize(
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        length: u64,
        kill_suidgid: bool,
    ) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        Self::fingerprint_handle(&mut hasher, handle);
        hasher.update(b"resize");
        hasher.update(&length.to_be_bytes());
        hasher.update(&[u8::from(kill_suidgid)]);
        *hasher.finalize().as_bytes()
    }

    fn fingerprint_remote_sync(
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        through_write_seq: u64,
        data_only: bool,
    ) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        Self::fingerprint_handle(&mut hasher, handle);
        hasher.update(b"sync");
        hasher.update(&through_write_seq.to_be_bytes());
        hasher.update(&[u8::from(data_only)]);
        *hasher.finalize().as_bytes()
    }

    fn fingerprint_handle(
        hasher: &mut blake3::Hasher,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) {
        for part in [
            handle.namespace_id.as_bytes(),
            handle.inode_id.as_bytes(),
            handle.owner_node_id.as_bytes(),
            handle.owner_session_id.as_bytes(),
            handle.caller_node_id.as_bytes(),
            handle.caller_session_id.as_bytes(),
            handle.opaque_handle.as_slice(),
        ] {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(part);
        }
        hasher.update(&handle.lease_epoch.to_be_bytes());
    }

    fn file_handle_from_owner(
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<FileHandle> {
        let bytes: [u8; 8] = handle
            .opaque_handle
            .as_slice()
            .try_into()
            .map_err(|_| stale("DFS owner handle is malformed"))?;
        Ok(FileHandle(u64::from_be_bytes(bytes)))
    }

    fn owner_handle_for(
        &self,
        peer: &str,
        caller_session_id: String,
        inode: &InodeRecord,
        lease: &WriteLease,
        handle: FileHandle,
        open_seq: u64,
    ) -> afs_protocol::node_control::DfsOwnerHandle {
        afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: self.namespace_id.0.clone(),
            inode_id: inode.inode_id.0.clone(),
            owner_node_id: self.node_id.clone(),
            owner_session_id: self.session_id.clone(),
            lease_epoch: lease.lease_epoch,
            caller_node_id: peer.to_owned(),
            caller_session_id,
            opaque_handle: handle.0.to_be_bytes().to_vec(),
            open_seq,
        }
    }

    fn control_owner_handle(
        handle: &afs_protocol::node_data::DfsOwnerHandle,
    ) -> afs_protocol::node_control::DfsOwnerHandle {
        afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: handle.namespace_id.clone(),
            inode_id: handle.inode_id.clone(),
            owner_node_id: handle.owner_node_id.clone(),
            owner_session_id: handle.owner_session_id.clone(),
            lease_epoch: handle.lease_epoch,
            caller_node_id: handle.caller_node_id.clone(),
            caller_session_id: handle.caller_session_id.clone(),
            opaque_handle: handle.opaque_handle.clone(),
            open_seq: handle.open_seq,
        }
    }

    fn data_owner_handle(
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> afs_protocol::node_data::DfsOwnerHandle {
        afs_protocol::node_data::DfsOwnerHandle {
            namespace_id: handle.namespace_id.clone(),
            inode_id: handle.inode_id.clone(),
            owner_node_id: handle.owner_node_id.clone(),
            owner_session_id: handle.owner_session_id.clone(),
            lease_epoch: handle.lease_epoch,
            caller_node_id: handle.caller_node_id.clone(),
            caller_session_id: handle.caller_session_id.clone(),
            opaque_handle: handle.opaque_handle.clone(),
            open_seq: handle.open_seq,
        }
    }

    fn wire_lock_owner(owner: &FileLockOwner) -> afs_protocol::node_control::DfsOwnerLockOwner {
        afs_protocol::node_control::DfsOwnerLockOwner {
            ingress_session_id: Self::raw_lock_session(&owner.ingress_session_id).to_owned(),
            kernel_owner: owner.kernel_owner,
        }
    }

    fn file_lock_owner_from_wire(
        owner: afs_protocol::node_control::DfsOwnerLockOwner,
    ) -> Result<FileLockOwner> {
        if owner.ingress_session_id.is_empty() {
            return Err(invalid("DFS lock owner session is empty"));
        }
        Ok(FileLockOwner {
            ingress_session_id: owner.ingress_session_id,
            kernel_owner: owner.kernel_owner,
        })
    }

    fn wire_lock_range(range: FileLockRange) -> afs_protocol::node_control::DfsOwnerLockRange {
        afs_protocol::node_control::DfsOwnerLockRange {
            start: range.start,
            end: range.end,
        }
    }

    fn file_lock_range_from_wire(
        range: afs_protocol::node_control::DfsOwnerLockRange,
    ) -> Result<FileLockRange> {
        let range = FileLockRange {
            start: range.start,
            end: range.end,
        };
        if !range.is_valid() {
            return Err(invalid("DFS lock range is invalid"));
        }
        Ok(range)
    }

    fn wire_lock_kind(kind: FileLockKind) -> afs_protocol::node_control::DfsOwnerLockKind {
        match kind {
            FileLockKind::Posix => afs_protocol::node_control::DfsOwnerLockKind::Posix,
            FileLockKind::Flock => afs_protocol::node_control::DfsOwnerLockKind::Flock,
        }
    }

    fn file_lock_kind_from_wire(kind: i32) -> Result<FileLockKind> {
        match afs_protocol::node_control::DfsOwnerLockKind::try_from(kind) {
            Ok(afs_protocol::node_control::DfsOwnerLockKind::Posix) => Ok(FileLockKind::Posix),
            Ok(afs_protocol::node_control::DfsOwnerLockKind::Flock) => Ok(FileLockKind::Flock),
            _ => Err(invalid("DFS lock kind is invalid")),
        }
    }

    fn wire_lock_type(lock_type: FileLockType) -> afs_protocol::node_control::DfsOwnerLockType {
        match lock_type {
            FileLockType::Read => afs_protocol::node_control::DfsOwnerLockType::Read,
            FileLockType::Write => afs_protocol::node_control::DfsOwnerLockType::Write,
            FileLockType::Unlock => afs_protocol::node_control::DfsOwnerLockType::Unlock,
        }
    }

    fn file_lock_type_from_wire(lock_type: i32) -> Result<FileLockType> {
        match afs_protocol::node_control::DfsOwnerLockType::try_from(lock_type) {
            Ok(afs_protocol::node_control::DfsOwnerLockType::Read) => Ok(FileLockType::Read),
            Ok(afs_protocol::node_control::DfsOwnerLockType::Write) => Ok(FileLockType::Write),
            Ok(afs_protocol::node_control::DfsOwnerLockType::Unlock) => Ok(FileLockType::Unlock),
            _ => Err(invalid("DFS lock type is invalid")),
        }
    }

    fn wire_lock_request(request: &LockRequest) -> afs_protocol::node_control::DfsOwnerLockRequest {
        afs_protocol::node_control::DfsOwnerLockRequest {
            kind: Self::wire_lock_kind(request.kind) as i32,
            owner: Some(Self::wire_lock_owner(&request.owner)),
            pid: request.pid,
            range: Some(Self::wire_lock_range(request.range)),
            lock_type: Self::wire_lock_type(request.lock_type) as i32,
        }
    }

    fn wire_lock_conflict(
        conflict: FileLockConflict,
    ) -> afs_protocol::node_control::DfsOwnerLockConflict {
        afs_protocol::node_control::DfsOwnerLockConflict {
            kind: Self::wire_lock_kind(conflict.kind) as i32,
            owner: Some(Self::wire_lock_owner(&conflict.owner)),
            pid: conflict.pid,
            range: Some(Self::wire_lock_range(conflict.range)),
            lock_type: Self::wire_lock_type(conflict.lock_type) as i32,
        }
    }

    fn file_lock_conflict_from_wire(
        conflict: afs_protocol::node_control::DfsOwnerLockConflict,
    ) -> Result<FileLockConflict> {
        Ok(FileLockConflict {
            kind: Self::file_lock_kind_from_wire(conflict.kind)?,
            owner: Self::file_lock_owner_from_wire(
                conflict
                    .owner
                    .ok_or_else(|| invalid("DFS lock conflict owner is missing"))?,
            )?,
            pid: conflict.pid,
            range: Self::file_lock_range_from_wire(
                conflict
                    .range
                    .ok_or_else(|| invalid("DFS lock conflict range is missing"))?,
            )?,
            lock_type: Self::file_lock_type_from_wire(conflict.lock_type)?,
        })
    }

    fn raw_lock_conflict(mut conflict: FileLockConflict) -> FileLockConflict {
        conflict.owner.ingress_session_id =
            Self::raw_lock_session(&conflict.owner.ingress_session_id).to_owned();
        conflict
    }

    fn wire_lock_waiter(waiter: &LockWaiterId) -> afs_protocol::node_control::DfsOwnerLockWaiter {
        afs_protocol::node_control::DfsOwnerLockWaiter {
            ingress_session_id: Self::raw_lock_session(&waiter.ingress_session_id).to_owned(),
            request_id: waiter.request_id,
        }
    }

    fn lock_release_kind_from_wire(release_kind: i32) -> Result<ReleaseKind> {
        match afs_protocol::node_control::DfsOwnerLockReleaseKind::try_from(release_kind) {
            Ok(afs_protocol::node_control::DfsOwnerLockReleaseKind::PosixOwner) => {
                Ok(ReleaseKind::PosixOwner)
            }
            Ok(afs_protocol::node_control::DfsOwnerLockReleaseKind::FlockOwner) => {
                Ok(ReleaseKind::FlockOwner)
            }
            _ => Err(invalid("DFS lock release kind is invalid")),
        }
    }

    fn wire_lock_release_kind(
        release_kind: ReleaseKind,
    ) -> afs_protocol::node_control::DfsOwnerLockReleaseKind {
        match release_kind {
            ReleaseKind::PosixOwner => {
                afs_protocol::node_control::DfsOwnerLockReleaseKind::PosixOwner
            }
            ReleaseKind::FlockOwner => {
                afs_protocol::node_control::DfsOwnerLockReleaseKind::FlockOwner
            }
        }
    }

    fn wire_attr(attr: FileAttributes) -> afs_protocol::node_control::DfsOwnerFileAttr {
        let (kind, special_node) = match attr.kind {
            FileKind::Regular => (1, None),
            FileKind::Directory => (2, None),
            FileKind::Symlink => (3, None),
            FileKind::Special(kind) => {
                let special = dfs_owner_special_node(kind);
                (special.kind as u32, Some(special))
            }
        };
        afs_protocol::node_control::DfsOwnerFileAttr {
            size: attr.size,
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            nlink: attr.nlink,
            atime_unix_ms: system_time_ms(attr.atime),
            mtime_unix_ms: system_time_ms(attr.mtime),
            ctime_unix_ms: system_time_ms(attr.ctime),
            kind,
            special_node,
        }
    }
}

impl Drop for DistributedFs {
    fn drop(&mut self) {
        self.lock_renewal.stop();
    }
}

impl CommitReason {
    const DATA_SYNC: Self = Self::Foreground(ForegroundCommit::DataSync);
    const FULL_SYNC: Self = Self::Foreground(ForegroundCommit::FullSync);
    const CLOSE: Self = Self::Foreground(ForegroundCommit::Close);
    const NODE_DRAIN: Self = Self::Foreground(ForegroundCommit::NodeDrain);
    const BACKGROUND: Self = Self::Background(BackgroundCommit::Periodic);

    /// Label for operation ids, spans and logs.
    fn label(self) -> &'static str {
        match self {
            Self::Foreground(ForegroundCommit::DataSync) => "fdatasync",
            Self::Foreground(ForegroundCommit::FullSync) => "fsync",
            Self::Foreground(ForegroundCommit::Close) => "close",
            Self::Foreground(ForegroundCommit::NodeDrain) => "node-drain",
            Self::Background(BackgroundCommit::Periodic) => "background",
        }
    }

    /// Best effort: never waits for the inode lock or for in-progress work.
    fn is_background(self) -> bool {
        matches!(self, Self::Background(_))
    }

    /// Whether a commit with no dirty data still flushes pending metadata.
    /// DataOnly commits advance the data sequence but leave timestamps pending.
    /// Foreground barriers flush them once the data sequence moved past the last
    /// commit; background commits flush them once no writer is left open, even
    /// when both sequences are equal (otherwise lease renewal keeps unlinked
    /// versions pinned forever).
    fn flushes_pending_metadata(self, data_advanced: bool, open_writers: u64) -> bool {
        match self {
            Self::Foreground(_) => data_advanced,
            Self::Background(_) => open_writers == 0,
        }
    }

    /// fsync also commits dirty metadata on its own and repeats until the
    /// inode's metadata is fully synchronized.
    fn full_metadata_barrier(self) -> bool {
        self == Self::FULL_SYNC
    }

    fn metadata_mode(self) -> CommitMetadataMode {
        match self {
            Self::Foreground(ForegroundCommit::DataSync | ForegroundCommit::Close) => {
                CommitMetadataMode::DataOnly
            }
            Self::Foreground(ForegroundCommit::FullSync | ForegroundCommit::NodeDrain)
            | Self::Background(_) => CommitMetadataMode::Full,
        }
    }

    fn metadata_delta(
        self,
        mtime_unix_ms: u64,
        ctime_unix_ms: u64,
        kill_suidgid: bool,
    ) -> CommitMetadataDelta {
        if self.metadata_mode() == CommitMetadataMode::Full {
            CommitMetadataDelta {
                mode: CommitMetadataMode::Full,
                mtime_unix_ms: Some(mtime_unix_ms),
                ctime_unix_ms: Some(ctime_unix_ms),
                kill_suidgid,
            }
        } else {
            CommitMetadataDelta {
                mode: CommitMetadataMode::DataOnly,
                mtime_unix_ms: None,
                ctime_unix_ms: kill_suidgid.then_some(ctime_unix_ms),
                kill_suidgid,
            }
        }
    }
}

impl Backend for DistributedFs {
    /// A read-only handle opened with no local write activity pins its version
    /// until close (decision 4a); FUSE must not let it share page-cache bytes
    /// with a handle pinned to another version.
    fn fixed_read_view(&self, handle: FileHandle) -> bool {
        self.handles
            .get(handle.0)
            .ok()
            .flatten()
            .is_some_and(|opened| {
                opened.flags & libc::O_ACCMODE == libc::O_RDONLY
                    && opened.owner_scope.is_none()
                    && !opened.dynamic_read_route.load(Ordering::Acquire)
                    && opened
                        .state
                        .lock()
                        .is_ok_and(|state| state.opened_page_identity.is_some())
            })
    }

    fn page_cache_identity(&self, handle: FileHandle) -> Option<PageCacheIdentity> {
        if self.read_engine.has_external_backend() {
            let opened = self.handles.get(handle.0).ok()??;
            if opened.flags & libc::O_ACCMODE != libc::O_RDONLY
                || opened.owner_scope.is_some()
                || opened.closed.load(Ordering::Acquire)
                || opened.dynamic_read_route.load(Ordering::Acquire)
            {
                return None;
            }
            let state = opened.state.lock().ok()?;
            if state.write_session.is_some() {
                return None;
            }
            return state.opened_page_identity.clone();
        }
        let opened = self.native_read_capability(handle)?;
        let state = opened.state.lock().ok()?;
        self.check_native_read_view(state.native_read_view.as_ref()?)
            .ok()?;
        state.opened_page_identity.clone()
    }

    fn supports_parallel_reads(&self, handle: FileHandle) -> bool {
        self.native_read_capability(handle).is_some()
    }

    fn root_inode(&self) -> BackendInode {
        BackendInode { value: ROOT_INODE }
    }

    fn supports_killpriv_v2(&self) -> bool {
        true
    }

    fn supports_advisory_locks(&self) -> bool {
        true
    }

    fn statfs(&self, _: &RequestContext, inode: BackendInode) -> Result<FilesystemCapacity> {
        let inode_id = self.inode_id(inode)?;
        let record = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
        if record.inode_id != inode_id {
            return Err(invalid("Meta returned a different DFS inode for statfs"));
        }
        self.chunk_store.capacity()
    }

    fn getlk(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        handle: FileHandle,
        request: LockRequest,
    ) -> Result<Option<FileLockConflict>> {
        let (inode_id, _) = self.validate_lock_handle(inode, handle, Some(&request))?;
        let request = self.scoped_local_lock_request(request)?;
        self.ensure_lock_session_open(&request.owner.ingress_session_id)?;
        match self.lock_authority_for(&inode_id)? {
            DfsLockTarget::Local(authority) => {
                self.check_lock_renewal(&authority)?;
                authority
                    .table
                    .getlk(&request)
                    .map_err(lock_error)
                    .map(|conflict| conflict.map(Self::raw_lock_conflict))
            }
            DfsLockTarget::Remote(remote) => {
                let reply =
                    remote
                        .owner
                        .get_lock(afs_protocol::node_control::DfsOwnerGetLockRequest {
                            authority: Some(remote.authority),
                            lock: Some(Self::wire_lock_request(&request)),
                        })?;
                reply
                    .conflict
                    .map(Self::file_lock_conflict_from_wire)
                    .transpose()
            }
        }
    }

    fn setlk(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        handle: FileHandle,
        request: LockRequest,
        waiter: Option<LockWaiterId>,
    ) -> Result<()> {
        let (inode_id, _) = self.validate_lock_handle(inode, handle, Some(&request))?;
        let request = self.scoped_local_lock_request(request)?;
        let waiter = waiter
            .map(|waiter| self.scoped_local_lock_waiter(waiter))
            .transpose()?;
        if request.lock_type != FileLockType::Unlock {
            self.ensure_lock_session_open(&request.owner.ingress_session_id)?;
        }
        if let Some(waiter) = waiter.as_ref() {
            if waiter.ingress_session_id != request.owner.ingress_session_id {
                return Err(invalid("DFS lock waiter scope does not match owner scope"));
            }
            if request.lock_type != FileLockType::Unlock {
                self.ensure_lock_session_open(&waiter.ingress_session_id)?;
            }
        }
        match self.lock_authority_for(&inode_id)? {
            DfsLockTarget::Local(authority) => {
                self.check_lock_renewal(&authority)?;
                if let Some(waiter_id) = waiter.clone() {
                    self.register_lock_waiter(&authority, &waiter_id)?;
                    let result = authority
                        .table
                        .setlk_blocking(request.clone(), waiter_id.clone())
                        .map_err(lock_error);
                    self.unpin_lock_waiter(&authority, waiter.as_ref())?;
                    if result.is_ok() {
                        self.register_local_lock_success(&authority, &request)?;
                        authority
                            .table
                            .acknowledge_waiter(&waiter_id)
                            .map_err(lock_error)?;
                        self.check_lock_renewal_after_mutation(&authority)?;
                    } else if result
                        .as_ref()
                        .err()
                        .is_some_and(|error| error.code() == afs_error::IO_INTERRUPTED)
                    {
                        authority
                            .table
                            .acknowledge_waiter(&waiter_id)
                            .map_err(lock_error)?;
                    }
                    result
                } else {
                    let result = authority
                        .table
                        .setlk_nonblocking(request.clone())
                        .map_err(lock_error);
                    if result.is_ok() {
                        self.register_local_lock_success(&authority, &request)?;
                        self.check_lock_renewal_after_mutation(&authority)?;
                    }
                    result
                }
            }
            DfsLockTarget::Remote(remote) => {
                let pins_remote_session = request.lock_type != FileLockType::Unlock;
                let remote_session_was_new = if pins_remote_session {
                    self.note_remote_lock_session(&inode_id, &request.owner.ingress_session_id)?
                } else {
                    false
                };
                if let Some(waiter_id) = waiter.as_ref()
                    && let Err(error) =
                        self.register_remote_lock_waiter_route(waiter_id, remote.clone())
                {
                    if remote_session_was_new {
                        self.forget_remote_lock_session(
                            &inode_id,
                            &request.owner.ingress_session_id,
                        )?;
                    }
                    return Err(error);
                }
                let result =
                    remote
                        .owner
                        .set_lock(afs_protocol::node_control::DfsOwnerSetLockRequest {
                            authority: Some(remote.authority.clone()),
                            lock: Some(Self::wire_lock_request(&request)),
                            waiter: waiter.as_ref().map(Self::wire_lock_waiter),
                        });
                if result
                    .as_ref()
                    .err()
                    .is_some_and(|error| error.code() == afs_error::IO_INTERRUPTED)
                    && let Some(waiter_id) = waiter.as_ref()
                {
                    self.mark_cancelled_remote_lock_waiter(waiter_id)?;
                    if remote_session_was_new {
                        self.forget_remote_lock_session(
                            &inode_id,
                            &request.owner.ingress_session_id,
                        )?;
                    }
                    return Err(lock_error(LockError::Interrupted));
                }
                if result.is_err()
                    && waiter.is_some()
                    && let Some(waiter_id) = waiter.as_ref()
                {
                    match remote.owner.cancel_lock_wait(
                        afs_protocol::node_control::DfsOwnerCancelLockWaitRequest {
                            authority: Some(remote.authority.clone()),
                            waiter: Some(Self::wire_lock_waiter(waiter_id)),
                        },
                    ) {
                        Ok(reply) if reply.outcome == 1 => {
                            let ack = remote.owner.acknowledge_lock_wait(
                                afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                                    authority: Some(remote.authority.clone()),
                                    waiter: Some(Self::wire_lock_waiter(waiter_id)),
                                },
                            );
                            if ack.is_err() {
                                self.mark_cancelled_remote_lock_waiter(waiter_id)?;
                                return Err(unavailable(
                                    "DFS remote lock cancellation could not be acknowledged; waiter identity retained",
                                ));
                            }
                            self.cleanup_acknowledged_remote_lock_waiter(waiter_id)?;
                            if remote_session_was_new {
                                self.forget_remote_lock_session(
                                    &inode_id,
                                    &request.owner.ingress_session_id,
                                )?;
                            }
                            return Err(lock_error(LockError::Interrupted));
                        }
                        Ok(reply) if reply.outcome == 2 => {
                            let ack = remote.owner.acknowledge_lock_wait(
                                afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                                    authority: Some(remote.authority.clone()),
                                    waiter: Some(Self::wire_lock_waiter(waiter_id)),
                                },
                            );
                            if ack.is_ok() {
                                self.cleanup_acknowledged_remote_lock_waiter(waiter_id)?;
                            } else {
                                self.mark_cancelled_remote_lock_waiter(waiter_id)?;
                            }
                            return Ok(());
                        }
                        _ => {
                            return Err(unavailable(
                                "DFS remote lock cancellation outcome is unknown; waiter identity retained",
                            ));
                        }
                    }
                }
                if result.is_ok()
                    && let Some(waiter_id) = waiter.as_ref()
                {
                    let ack = remote.owner.acknowledge_lock_wait(
                        afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                            authority: Some(remote.authority.clone()),
                            waiter: Some(Self::wire_lock_waiter(waiter_id)),
                        },
                    );
                    if ack.is_ok() {
                        self.cleanup_acknowledged_remote_lock_waiter(waiter_id)?;
                    } else {
                        self.mark_cancelled_remote_lock_waiter(waiter_id)?;
                    }
                }
                result.map(|_| ())
            }
        }
    }

    fn cancel_lock_wait(&self, waiter: LockWaiterId) -> Result<()> {
        let waiter = self.scoped_local_lock_waiter(waiter)?;
        if let Some(remote) = self.take_remote_lock_waiter_route_or_precancel(waiter.clone())? {
            let authority = remote.authority.clone();
            let reply = remote.owner.cancel_lock_wait(
                afs_protocol::node_control::DfsOwnerCancelLockWaitRequest {
                    authority: Some(authority.clone()),
                    waiter: Some(Self::wire_lock_waiter(&waiter)),
                },
            )?;
            match reply.outcome {
                1 => {
                    match remote.owner.acknowledge_lock_wait(
                        afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                            authority: Some(authority.clone()),
                            waiter: Some(Self::wire_lock_waiter(&waiter)),
                        },
                    ) {
                        Ok(_) => {
                            self.cleanup_acknowledged_remote_lock_waiter(&waiter)?;
                        }
                        Err(error) => {
                            self.mark_cancelled_remote_lock_waiter(&waiter)?;
                            return Err(error);
                        }
                    }
                }
                2 => {
                    let ack = remote.owner.acknowledge_lock_wait(
                        afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest {
                            authority: Some(authority),
                            waiter: Some(Self::wire_lock_waiter(&waiter)),
                        },
                    );
                    if ack.is_ok() {
                        self.cleanup_acknowledged_remote_lock_waiter(&waiter)?;
                    } else {
                        self.mark_cancelled_remote_lock_waiter(&waiter)?;
                    }
                }
                _ => {
                    return Err(Error::coded(
                        afs_error::IO_OTHER,
                        "DFS remote lock cancellation outcome is unknown",
                    ));
                }
            }
            return Ok(());
        }
        let authorities = self.lock_authorities.values_snapshot()?;
        let mut last_error = None;
        for authority in authorities {
            match authority
                .table
                .cancel_waiter_with_outcome(waiter.clone())
                .map_err(lock_error)
            {
                Ok(LockWaiterOutcome::Unknown) => {
                    last_error = Some(Error::coded(
                        afs_error::IO_OTHER,
                        "DFS local lock cancellation outcome is unknown",
                    ));
                }
                Ok(LockWaiterOutcome::Cancelled | LockWaiterOutcome::Granted) => {}
                Err(error) => last_error = Some(error),
            }
            authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
                .waiters
                .remove(&waiter);
        }
        if let Some(error) = last_error {
            return Err(error);
        }
        Ok(())
    }

    fn release_locks(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        handle: FileHandle,
        owner: FileLockOwner,
        kind: ReleaseKind,
    ) -> Result<()> {
        let (inode_id, _) = self.validate_lock_handle(inode, handle, None)?;
        let owner = self.scoped_local_lock_owner(owner)?;
        match self.existing_lock_authority_for(&inode_id)? {
            Some(DfsLockTarget::Local(authority)) => {
                match kind {
                    ReleaseKind::PosixOwner => authority.table.release_posix_owner(&owner),
                    ReleaseKind::FlockOwner => authority.table.release_flock_owner(&owner),
                }
                .map_err(lock_error)?;
                self.unpin_owner_if_inactive(
                    &authority,
                    &owner,
                    Some(match kind {
                        ReleaseKind::PosixOwner => FileLockKind::Posix,
                        ReleaseKind::FlockOwner => FileLockKind::Flock,
                    }),
                )?;
                Ok(())
            }
            Some(DfsLockTarget::Remote(remote)) => {
                remote.owner.release_locks(
                    afs_protocol::node_control::DfsOwnerReleaseLocksRequest {
                        authority: Some(remote.authority),
                        owner: Some(Self::wire_lock_owner(&owner)),
                        release_kind: Self::wire_lock_release_kind(kind) as i32,
                    },
                )?;
                // Keep remote session presence until explicit session close. Releasing one
                // kernel owner does not prove that this mount has no remaining locks.
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn release_lock_session(&self, ingress_session_id: &str) -> Result<()> {
        let scoped_session = Self::lock_scope(&self.node_id, &self.session_id, ingress_session_id)?;
        let ingress_session_id = scoped_session.as_str();
        self.note_closed_lock_session(ingress_session_id)?;
        let authorities = self.lock_authorities.values_snapshot()?;
        let mut errors = Vec::new();
        for authority in authorities {
            if let Err(error) = authority
                .table
                .release_session(ingress_session_id)
                .map_err(lock_error)
            {
                errors.push(error);
                continue;
            }
            let mut state = authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
            state
                .pinned_owners
                .retain(|owner| owner.ingress_session_id != ingress_session_id);
            state
                .waiters
                .retain(|waiter| waiter.ingress_session_id != ingress_session_id);
        }
        let remotes = self
            .remote_lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS remote lock authority table is poisoned"))?
            .iter()
            .map(|(inode_id, remote)| (inode_id.clone(), remote.clone()))
            .collect::<Vec<_>>();
        for (inode_id, remote) in &remotes {
            let has_session = self
                .remote_lock_authority_sessions
                .lock()
                .map_err(|_| unavailable("DFS remote lock authority session table is poisoned"))?
                .get(inode_id)
                .is_some_and(|sessions| sessions.contains_key(ingress_session_id));
            if !has_session {
                continue;
            }
            self.mark_remote_lock_session_pending_close(inode_id, ingress_session_id)?;
            if let Err(error) = self.release_remote_lock_session_once(
                inode_id,
                remote,
                ingress_session_id,
                REMOTE_RELEASE_RPC_BUDGET,
            ) {
                errors.push(error);
            }
        }
        if let Some(error) = errors.into_iter().next() {
            return Err(error);
        }
        Ok(())
    }

    fn lookup(&self, _: &RequestContext, parent: BackendInode, name: &OsStr) -> Result<Entry> {
        let inode = self.validate_inode(
            self.meta
                .lookup(&self.inode_id(parent)?, name.as_encoded_bytes())?
                .ok_or_else(|| {
                    Error::coded(afs_error::NODE_VFS_NOT_FOUND, "DFS dentry not found")
                })?,
        )?;
        Ok(Entry {
            inode: self.backend_inode(&inode.inode_id)?,
            attributes: self.visible_attributes(&inode)?,
        })
    }

    fn getattr(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        handle: Option<FileHandle>,
    ) -> Result<FileAttributes> {
        if let Some(handle) = handle {
            let snapshot = self.handle_snapshot(handle)?;
            // A pinned reader reports the version it reads (decision 4a).
            if snapshot.flags & libc::O_ACCMODE == libc::O_RDONLY
                && snapshot.write_session.is_none()
                && snapshot.opened_inode.kind == InodeKind::Regular
                && self
                    .remote_provider_visible_for_reads(&snapshot.inode_id)?
                    .is_none()
                && !self.local_write_activity(&snapshot.inode_id)?
            {
                let opened = snapshot.opened_inode.as_ref();
                let (size, blocks) = self.inode_size_and_blocks(opened)?;
                return Ok(attributes(opened, size, blocks));
            }
            if let Some(remote) = self.remote_provider_visible_for_reads(&snapshot.inode_id)? {
                return self.remote_getattr(&remote).inspect_err(|error| {
                    self.invalidate_native_inode_views_on_error(&snapshot.inode_id, error);
                });
            }
            let observed = self
                .meta
                .get_inode(&snapshot.inode_id)
                .inspect_err(|error| {
                    self.invalidate_native_inode_views_on_error(&snapshot.inode_id, error);
                })?;
            if self.read_engine.has_external_backend() {
                let record = self.refresh_inode_record(observed, true, true)?;
                return self.visible_attributes(&record);
            }
            let record = self.observe_inode_record(observed)?;
            return self.visible_attributes_with_observed_inode(&snapshot.opened_inode, &record);
        }
        let inode_id = self.inode_id(inode)?;
        if let Some(remote) = self.remote_provider_visible_for_reads(&inode_id)? {
            return self.remote_getattr(&remote).inspect_err(|error| {
                self.invalidate_native_inode_views_on_error(&inode_id, error);
            });
        }
        let observed = self.meta.get_inode(&inode_id).inspect_err(|error| {
            self.invalidate_native_inode_views_on_error(&inode_id, error);
        })?;
        let record = if self.read_engine.has_external_backend() {
            self.refresh_inode_record(observed, true, true)?
        } else {
            self.observe_inode_record(observed)?
        };
        self.visible_attributes_with_observed_inode(&record, &record)
    }

    fn setattr(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        handle: Option<FileHandle>,
        change: &AttributeChange,
    ) -> Result<FileAttributes> {
        self.setattr_with_options(ctx, inode, handle, change, SetAttrOptions::default())
    }

    fn setattr_with_options(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        handle: Option<FileHandle>,
        change: &AttributeChange,
        options: SetAttrOptions,
    ) -> Result<FileAttributes> {
        let inode_id = self.inode_id(inode)?;
        let mut change = change.clone();
        let mut killpriv_setattr = false;
        let _native_ticket = if let Some(cell) = self.write_state(&inode_id)? {
            let ticket = self.native_ticket(&cell)?;
            if self.native_pipeline.is_some() {
                drop(self.begin_inode_operation(&inode_id, cell)?);
            }
            ticket
        } else {
            None
        };
        if options.kill_suidgid {
            let current = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
            if current.kind == InodeKind::Regular {
                killpriv_setattr = true;
                let state = self.ensure_inode_write_state(&current)?;
                let mut state = self.lock_for_mutation(&inode_id, &state)?;
                if change.mode.is_some()
                    && !super::types::is_legacy_privilege_clear(
                        &attributes(&state.inode, state.logical_length, 0),
                        &change,
                    )
                {
                    return Err(Error::from(std::io::Error::from(
                        std::io::ErrorKind::PermissionDenied,
                    )));
                }
                Self::apply_kernel_killpriv_to_state(&mut state);
                change.mode = None;
            }
        } else if change.mode.is_some() && self.has_pending_killpriv(&inode_id)? {
            self.commit_inode(
                &inode_id,
                CommitReason::DATA_SYNC,
                CommitWait::AfterOperations,
            )?;
        }
        if change.mode.is_some()
            || change.uid.is_some()
            || change.gid.is_some()
            || change.atime.is_some()
            || change.mtime.is_some()
            || options.timestamps_now
        {
            let current = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
            let updated = self.refresh_inode_record(
                self.meta.set_inode_attributes(SetInodeAttrRequest {
                    caller_id: self.node_id.clone(),
                    operation_id: self.operation_id("setattr"),
                    caller: caller_context(ctx),
                    inode_id: inode_id.clone(),
                    expected_inode_revision: current.revision,
                    update: InodeAttrUpdate {
                        mode: change.mode,
                        uid: change.uid,
                        gid: change.gid,
                        atime_unix_ms: change.atime.map(system_time_ms),
                        mtime_unix_ms: change.mtime.map(system_time_ms),
                        ctime_unix_ms: Some(now_unix_ms()),
                        timestamps_now: options.timestamps_now,
                    },
                })?,
                change.atime.is_none() && !options.timestamps_now,
                change.mtime.is_none() && !options.timestamps_now,
            )?;
            if change.size.is_none() {
                if killpriv_setattr {
                    self.commit_inode(
                        &inode_id,
                        CommitReason::DATA_SYNC,
                        CommitWait::AfterOperations,
                    )?;
                    let updated =
                        self.refresh_inode_record(self.meta.get_inode(&inode_id)?, true, true)?;
                    return self.visible_attributes(&updated);
                }
                return self.visible_attributes(&updated);
            }
        }

        let Some(length) = change.size else {
            if killpriv_setattr {
                self.commit_inode(
                    &inode_id,
                    CommitReason::DATA_SYNC,
                    CommitWait::AfterOperations,
                )?;
            }
            return self.getattr(ctx, inode, handle);
        };
        let (record, accepted_seq) = if let Some(handle) = handle {
            self.observe_handle_error(handle)?;
            let snapshot = self.handle_snapshot(handle)?;
            if snapshot.inode_id != inode_id {
                return Err(stale("DFS file handle does not name the requested inode"));
            }
            if snapshot.flags & libc::O_ACCMODE == libc::O_RDONLY {
                return Err(bad_file_descriptor("DFS handle was not opened for writing"));
            }
            let Some(session) = snapshot.write_session.as_ref() else {
                return Err(invalid("writable DFS handle has no write session"));
            };
            if snapshot.opened_inode.kind != InodeKind::Regular {
                return Err(Error::from(std::io::Error::from_raw_os_error(libc::EISDIR)));
            }
            let accepted_seq = if session.remote.is_some() {
                self.remote_resize(session, length, options)?
            } else {
                self.resize_dirty_inode(&snapshot.inode_id, length, options.kill_suidgid)?
            };
            (
                snapshot.opened_inode.as_ref().clone(),
                Some((handle, accepted_seq)),
            )
        } else {
            let (record, write_lease) = self.meta.open_write(&inode_id)?;
            let mut record = self.validate_inode(record)?;
            if record.kind != InodeKind::Regular {
                return Err(Error::from(std::io::Error::from_raw_os_error(libc::EISDIR)));
            }
            if write_lease.owner_node_id == self.node_id
                && write_lease.owner_session_id == self.session_id
            {
                self.install_write_state(record.clone(), write_lease)?;
                self.resize_dirty_inode(&record.inode_id, length, options.kill_suidgid)?;
            } else {
                let previous_provider = self.current_remote_provider(&record.inode_id)?;
                let mut session = self.open_remote_write_session(
                    &record,
                    &write_lease,
                    libc::O_WRONLY,
                    OpenOptions {
                        kill_suidgid: options.kill_suidgid,
                    },
                )?;
                match self.remote_resize(&session, length, options) {
                    Ok(accepted_seq) => {
                        session.local.last_accepted_seq = accepted_seq;
                    }
                    Err(error) => {
                        let _ = self.close_temporary_remote_write_session(
                            &record.inode_id,
                            &session,
                            previous_provider.clone(),
                        );
                        return Err(error);
                    }
                }
                if let Err(error) = self.remote_sync(&session, SyncMode::Full) {
                    let _ = self.close_temporary_remote_write_session(
                        &record.inode_id,
                        &session,
                        previous_provider.clone(),
                    );
                    return Err(error);
                }
                self.close_temporary_remote_write_session(
                    &record.inode_id,
                    &session,
                    previous_provider,
                )?;
                record = self.observe_inode_record(self.meta.get_inode(&inode_id)?)?;
            }
            (record, None)
        };
        if let Some((handle, accepted_seq)) = accepted_seq {
            self.update_handle_write_progress(handle, accepted_seq)?;
        }
        self.visible_attributes(&record)
    }

    fn create(
        &self,
        ctx: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        mode: u32,
        flags: i32,
    ) -> Result<CreatedFile> {
        self.create_with_options(ctx, parent, name, mode, flags, OpenOptions::default())
    }

    fn create_with_options(
        &self,
        ctx: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        mode: u32,
        flags: i32,
        options: OpenOptions,
    ) -> Result<CreatedFile> {
        let now = now_unix_ms();
        let mode = mode & !ctx.umask;
        let mode = if options.kill_suidgid {
            Self::clear_kernel_write_privileges(mode)
        } else {
            mode
        };
        let (inode, write_lease) = self.meta.create(
            &self.operation_id("create"),
            &self.inode_id(parent)?,
            name.as_encoded_bytes(),
            InodeAttributes {
                mode,
                uid: ctx.uid,
                gid: ctx.gid,
                nlink: 1,
                atime_unix_ms: now,
                mtime_unix_ms: now,
                ctime_unix_ms: now,
            },
        )?;
        let inode = self.validate_inode(inode)?;
        self.install_write_state(inode.clone(), write_lease)?;
        let session = self.open_write_session(&inode, flags)?;
        let handle = self.allocate_handle(DfsFileHandle {
            gc_pin: None,
            inode_id: inode.inode_id.clone(),
            opened_inode: inode.clone(),
            opened_page_identity: None,
            native_read_view: None,
            native_read_generation: 0,
            native_denial_serial: 0,
            write_session: Some(Self::make_local_write_session(session)),
            flags,
            owner_scope: None,
        })?;
        Ok(CreatedFile {
            entry: Entry {
                inode: self.backend_inode(&inode.inode_id)?,
                attributes: attributes(&inode, 0, 0),
            },
            handle,
        })
    }

    fn mknod(
        &self,
        ctx: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        kind: SpecialFileKind,
        mode: u32,
    ) -> Result<Entry> {
        let now = now_unix_ms();
        let inode = self.validate_inode(self.meta.mknod(MknodRequest {
            caller_id: self.node_id.clone(),
            operation_id: self.operation_id("mknod"),
            namespace_id: self.namespace_id.clone(),
            parent_inode_id: self.inode_id(parent)?,
            name: name.as_encoded_bytes().to_vec(),
            kind: dfs_special_kind(kind),
            attributes: InodeAttributes {
                mode: mode & !ctx.umask,
                uid: ctx.uid,
                gid: ctx.gid,
                nlink: 1,
                atime_unix_ms: now,
                mtime_unix_ms: now,
                ctime_unix_ms: now,
            },
            caller: caller_context(ctx),
        })?)?;
        Ok(Entry {
            inode: self.backend_inode(&inode.inode_id)?,
            attributes: attributes(&inode, 0, 0),
        })
    }

    fn open(&self, ctx: &RequestContext, inode: BackendInode, flags: i32) -> Result<FileHandle> {
        self.open_with_options(ctx, inode, flags, OpenOptions::default())
    }

    fn open_with_options(
        &self,
        _: &RequestContext,
        inode: BackendInode,
        flags: i32,
        options: OpenOptions,
    ) -> Result<FileHandle> {
        let inode_id = self.inode_id(inode)?;
        let native_read_view = if !self.read_engine.has_external_backend()
            && flags & libc::O_ACCMODE == libc::O_RDONLY
        {
            Some(self.capture_native_read_view()?)
        } else {
            None
        };
        let preflight =
            self.validate_inode(self.meta.get_inode(&inode_id).inspect_err(|error| {
                self.invalidate_native_inode_views_on_error(&inode_id, error);
            })?)?;
        if matches!(preflight.kind, InodeKind::Special(_)) {
            return Err(Error::coded(
                afs_error::IO_NOT_SUPPORTED,
                "DFS special inode data path is handled by kernel/local device semantics",
            ));
        }
        let writable = flags & libc::O_ACCMODE != libc::O_RDONLY;
        let (opened_inode, write_session) = if writable {
            let (inode, write_lease) = self.meta.resolve_write_authority(&inode_id)?;
            let inode = self.validate_inode(inode)?;
            let session = if write_lease.owner_node_id == self.node_id
                && write_lease.owner_session_id == self.session_id
            {
                self.install_write_state(inode.clone(), write_lease.clone())?;
                let session = self.open_write_session(&inode, flags)?;
                let session = Self::make_local_write_session(session);
                if options.kill_suidgid {
                    let state = self
                        .write_state(&inode.inode_id)?
                        .ok_or_else(|| stale("DFS write state is no longer open"))?;
                    let mut state = self.lock_for_mutation(&inode.inode_id, &state)?;
                    Self::apply_kernel_killpriv_to_state(&mut state);
                }
                if flags & libc::O_TRUNC != 0
                    && let Err(error) =
                        self.resize_dirty_inode(&inode.inode_id, 0, options.kill_suidgid)
                {
                    self.release_writer(&session.local)?;
                    return Err(error);
                }
                session
            } else {
                let previous_provider = self.current_remote_provider(&inode.inode_id)?;
                let session =
                    self.open_remote_write_session(&inode, &write_lease, flags, options)?;
                if flags & libc::O_TRUNC != 0
                    && let Err(error) = self.remote_resize(
                        &session,
                        0,
                        SetAttrOptions {
                            kill_suidgid: options.kill_suidgid,
                            timestamps_now: false,
                        },
                    )
                {
                    let _ = self.close_temporary_remote_write_session(
                        &inode.inode_id,
                        &session,
                        previous_provider,
                    );
                    return Err(error);
                }
                session
            };
            (inode, Some(session))
        } else {
            let inode = if self.read_engine.has_external_backend() {
                self.refresh_inode_record(preflight, true, true)?
            } else {
                self.observe_inode_record(preflight)?
            };
            (inode, None)
        };
        // Pinned at open: a read-only open with no local write activity and no
        // proxy provider reads this committed version until close (4a).
        let committed_read_only = !writable
            && opened_inode.kind == InodeKind::Regular
            && !self.local_write_activity(&inode_id)?
            && self.current_remote_provider(&inode_id)?.is_none();
        let opened_page_identity = if committed_read_only {
            // The first version/layout cache miss costs one Meta RPC, outside
            // all handle locks. Authority still starts before the inode RPC.
            Some(self.opened_page_identity(&opened_inode)?)
        } else {
            None
        };
        if let Some(view) = native_read_view.as_ref() {
            self.check_native_read_view(view)?;
        }
        self.allocate_handle(DfsFileHandle {
            gc_pin: None,
            inode_id: opened_inode.inode_id.clone(),
            opened_inode,
            opened_page_identity,
            native_read_view,
            native_read_generation: 0,
            native_denial_serial: 0,
            write_session,
            flags,
            owner_scope: None,
        })
    }

    fn read(
        &self,
        _: &RequestContext,
        handle: FileHandle,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        let snapshot = self.handle_snapshot(handle)?;
        if snapshot.flags & libc::O_ACCMODE == libc::O_WRONLY {
            return Err(bad_file_descriptor("DFS handle was not opened for reading"));
        }
        if !self.read_engine.has_external_backend() {
            let result = self.read_native_owned(handle, &snapshot, offset, out.len())?;
            out[..result.len()].copy_from_slice(&result);
            return Ok(result.len());
        }
        if let Some(remote) = self.remote_provider_visible_for_reads(&snapshot.inode_id)? {
            return self.remote_read(&remote, offset, out);
        }
        self.read_visible_from_handle_inode(
            &snapshot.inode_id,
            Some(&snapshot.opened_inode),
            offset,
            out,
        )
    }

    fn supports_async_reads(&self) -> bool {
        self.read_engine.async_read_enabled()
    }

    fn read_reply_async(
        self: Arc<Self>,
        _ctx: RequestContext,
        handle: FileHandle,
        offset: u64,
        size: usize,
        deadline: Instant,
        lifetime: crate::node::read_pipeline::ReadLifetime,
    ) -> Result<Option<crate::node::read_pipeline::ReadReplyFuture>> {
        self.prepare_native_async_read(handle, offset, size, deadline, lifetime)
    }

    fn read_reply(
        &self,
        ctx: &RequestContext,
        handle: FileHandle,
        offset: u64,
        size: usize,
    ) -> Result<prost::bytes::Bytes> {
        if self.read_engine.has_external_backend() {
            return self.read_owned(ctx, handle, offset, size).map(Into::into);
        }
        let snapshot = self.handle_snapshot(handle)?;
        if snapshot.flags & libc::O_ACCMODE == libc::O_WRONLY {
            return Err(bad_file_descriptor("DFS handle was not opened for reading"));
        }
        self.read_native_owned(handle, &snapshot, offset, size)
    }

    fn read_owned(
        &self,
        ctx: &RequestContext,
        handle: FileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        let snapshot = self.handle_snapshot(handle)?;
        if snapshot.flags & libc::O_ACCMODE == libc::O_WRONLY {
            return Err(bad_file_descriptor("DFS handle was not opened for reading"));
        }
        if !self.read_engine.has_external_backend() {
            return self
                .read_native_owned(handle, &snapshot, offset, size)
                .map(|bytes| bytes.to_vec());
        }
        let fallback = || {
            let mut out = vec![0; size];
            let n = <Self as Backend>::read(self, ctx, handle, offset, &mut out)?;
            out.truncate(n);
            Ok(out)
        };
        if self
            .remote_provider_visible_for_reads(&snapshot.inode_id)?
            .is_some()
        {
            return fallback();
        }
        // Local writes not yet committed are served from this Node's write
        // state (dirty overlay); reading never commits them.
        if self.local_write_activity(&snapshot.inode_id)? {
            return fallback();
        }
        if let Some(data) =
            self.try_read_external_version_owned(&snapshot.opened_inode, offset, size)?
        {
            return Ok(data);
        }
        fallback()
    }

    fn write(
        &self,
        ctx: &RequestContext,
        handle: FileHandle,
        offset: u64,
        data: &[u8],
    ) -> Result<usize> {
        self.write_with_options(ctx, handle, offset, data, WriteOptions::default())
    }

    fn accepts_queued_writes(&self, handle: FileHandle) -> bool {
        self.native_pipeline.is_some()
            && self.handle_snapshot(handle).ok().is_some_and(|snapshot| {
                snapshot.flags & libc::O_ACCMODE != libc::O_RDONLY
                    && snapshot.flags & libc::O_APPEND == 0
                    && snapshot.flags & libc::O_SYNC == 0
                    && !has_o_dsync(snapshot.flags)
                    && snapshot
                        .write_session
                        .as_ref()
                        .is_some_and(|session| session.remote.is_none())
            })
    }

    fn write_admission_mode(&self, handle: FileHandle) -> WriteAdmissionMode {
        // Native admission may wait for mutation/publication or shared body
        // capacity. Keep those waits on a worker so the FUSE receiver can
        // admit unrelated reads and writeback needed to make progress.
        if self.native_pipeline.is_some() {
            return WriteAdmissionMode::Queued;
        }
        if self
            .handle_snapshot(handle)
            .ok()
            .and_then(|snapshot| snapshot.write_session)
            .is_some_and(|session| session.remote.is_none())
        {
            WriteAdmissionMode::Borrowed
        } else {
            WriteAdmissionMode::Queued
        }
    }

    fn write_with_options(
        &self,
        _: &RequestContext,
        handle: FileHandle,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> Result<usize> {
        self.observe_handle_error(handle)?;
        let snapshot = self.handle_snapshot(handle)?;
        if snapshot.flags & libc::O_ACCMODE == libc::O_RDONLY {
            return Err(bad_file_descriptor("DFS handle was not opened for writing"));
        }
        let Some(write_session) = snapshot.write_session.as_ref() else {
            return Err(invalid("writable DFS handle has no write session"));
        };
        if write_session.remote.is_some() {
            let written =
                self.remote_write(write_session, snapshot.flags, offset, data, options)?;
            if snapshot.flags & libc::O_SYNC == libc::O_SYNC {
                self.remote_sync(write_session, SyncMode::Full)?;
            } else if has_o_dsync(snapshot.flags) {
                self.remote_sync(write_session, SyncMode::DataOnly)?;
            }
            self.update_handle_write_progress(handle, written.1)?;
            return Ok(written.0);
        }
        let state = self
            .write_state(&snapshot.inode_id)?
            .ok_or_else(|| stale("DFS write state is no longer open"))?;
        if data.is_empty() {
            return Ok(0);
        }
        if self.native_pipeline.is_none() {
            return Err(unavailable(
                "DFS store has no native write pipeline configured",
            ));
        }
        self.native_ordinary_write(handle, &snapshot, state, offset, data, options)
    }

    fn copy_file_range(
        &self,
        _: &RequestContext,
        range: super::types::CopyFileRange,
    ) -> Result<u64> {
        let source = self.handle_snapshot(range.source)?;
        let destination = self.handle_snapshot(range.destination)?;
        if source.flags & libc::O_ACCMODE == libc::O_WRONLY
            || destination.flags & libc::O_ACCMODE == libc::O_RDONLY
        {
            return Err(bad_file_descriptor(
                "COW copy requires readable source and writable destination",
            ));
        }
        if source.inode_id == destination.inode_id {
            return Err(Error::coded(
                afs_error::IO_NOT_SUPPORTED,
                "same-inode COW range is unsupported",
            ));
        }
        let session = destination
            .write_session
            .as_ref()
            .ok_or_else(|| bad_file_descriptor("missing destination write session"))?;
        if session.remote.is_some()
            || destination.flags & libc::O_APPEND != 0
            || self
                .remote_provider_visible_for_reads(&source.inode_id)?
                .is_some()
        {
            return Err(Error::coded(
                afs_error::IO_NOT_SUPPORTED,
                "COW requires local destination authority and committed source",
            ));
        }
        // Snapshot the source before acquiring destination admission, avoiding
        // cross-copy deadlock. Concurrent later writes create another version.
        self.commit_inode(
            &source.inode_id,
            CommitReason::FULL_SYNC,
            CommitWait::AfterOperations,
        )?;
        let inode = self.validate_inode(self.meta.get_inode(&source.inode_id)?)?;
        let Some(version_id) = inode.head_version else {
            return Ok(0);
        };
        let (source_version, source_layout) = self.meta.get_file_version(&version_id)?;
        if range.source_offset >= source_version.length || range.length == 0 {
            return Ok(0);
        }
        let length = range
            .length
            .min(source_version.length - range.source_offset)
            .min(u64::from(u32::MAX));
        let cell = self
            .write_state(&destination.inode_id)?
            .ok_or_else(|| stale("destination write state is missing"))?;
        let _operation = self.begin_inode_operation(&destination.inode_id, cell.clone())?;
        self.commit_inode(
            &destination.inode_id,
            CommitReason::FULL_SYNC,
            CommitWait::InsideOperation,
        )?;
        let pending = {
            let mut state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            if should_renew(&state.write_lease) {
                state.write_lease = self.meta.renew_write_lease(state.write_lease.clone())?;
            }
            let clone_source = crate::dfs::CloneFileRange {
                version: version_id,
                source_offset: range.source_offset,
                destination_offset: range.destination_offset,
                length,
            };
            let generation = self.next_operation.fetch_add(1, Ordering::Relaxed);
            let layout = crate::dfs::clone_file_layout(
                &state.base_layout,
                &source_layout,
                &clone_source,
                LayoutRootId::new(format!("{}-cow-layout-{generation}", self.session_id)),
            )?;
            let now = now_unix_ms();
            state.next_write_seq = state.next_write_seq.saturating_add(1);
            state.visible_write_seq = state.next_write_seq;
            state.logical_length = layout.file_length;
            state.metadata_dirty = true;
            state.inode.attributes.mtime_unix_ms = now;
            state.inode.attributes.ctime_unix_ms = now;
            Self::apply_kernel_killpriv_to_state(&mut state);
            let frozen = FrozenCommit {
                through_seq: state.visible_write_seq,
                logical_length: layout.file_length,
                inode: state.inode.clone(),
                write_lease: state.write_lease.clone(),
                base_version: state.base_version.clone(),
                base_layout: state.base_layout.clone(),
                dirty_extents: DirtyExtentMap::default(),
                kill_suidgid_dirty: state.kill_suidgid_dirty,
            };
            let commit = CommitFileVersion {
                gc_fence: None,
                operation_id: self.operation_id("cow-copy"),
                inode_id: destination.inode_id.clone(),
                write_lease: state.write_lease.clone(),
                expected_inode_revision: state.inode.revision,
                expected_head_version: state.inode.head_version.clone(),
                file_version: FileVersion {
                    id: FileVersionId::new(format!("{}-cow-version-{generation}", self.session_id)),
                    inode_id: destination.inode_id.clone(),
                    parent_version: state.inode.head_version.clone(),
                    length: layout.file_length,
                    layout_root: layout.id.clone(),
                    created_at_unix_ms: now,
                },
                layout_root: layout,
                chunk_receipts: Vec::new(),
                clone_source: Some(clone_source),
                metadata_delta: CommitReason::FULL_SYNC.metadata_delta(
                    now,
                    now,
                    state.kill_suidgid_dirty,
                ),
            };
            let pending = PendingFileCommit {
                frozen,
                batch: CommitBatch {
                    _gc_pin: None,
                    through_seq: state.visible_write_seq,
                    commit: Arc::new(commit),
                    _native_accounting: None,
                    _native_uploads: Vec::new(),
                },
                trace_send_attempt: self
                    .pending_trace
                    .as_ref()
                    .map(|_| Arc::new(AtomicU64::new(0))),
            };
            state.in_flight = Some(InFlightCommit::File(Box::new(pending.clone())));
            state.commit_busy = true;
            InFlightCommit::File(Box::new(pending))
        };
        if let Some(seq) = self.finish_commit(cell, pending, None, "cow-copy")? {
            self.update_handles_after_commit(&destination.inode_id, seq)?;
            self.update_handle_write_progress(range.destination, seq)?;
        }
        afs_logging::info!("dfs.cow_range_committed";
            "source_inode" => source.inode_id.0,
            "source_version" => source_version.id.0,
            "destination_inode" => destination.inode_id.0,
            "source_offset" => range.source_offset,
            "destination_offset" => range.destination_offset,
            "length" => length);
        Ok(length)
    }

    fn flush(&self, _: &RequestContext, handle: FileHandle) -> Result<()> {
        let snapshot = self.handle_snapshot(handle)?;
        if let Some(session) = snapshot.write_session.as_ref() {
            if session.remote.is_some() {
                self.remote_sync(session, SyncMode::DataOnly).map(|_| ())
            } else {
                self.sync_handle(handle, CommitReason::CLOSE, CommitWait::AfterOperations)
            }
        } else {
            self.observe_handle_error(handle)
        }
    }

    fn fsync(&self, _: &RequestContext, handle: FileHandle, mode: SyncMode) -> Result<()> {
        let snapshot = self.handle_snapshot(handle)?;
        if let Some(session) = snapshot.write_session.as_ref()
            && session.remote.is_some()
        {
            self.remote_sync(session, mode).map(|_| ())
        } else {
            self.sync_handle(
                handle,
                match mode {
                    SyncMode::DataOnly => CommitReason::DATA_SYNC,
                    SyncMode::Full => CommitReason::FULL_SYNC,
                },
                CommitWait::AfterOperations,
            )
        }
    }

    fn release(&self, _: &RequestContext, handle: FileHandle) -> Result<()> {
        let removed = self
            .handles
            .remove(handle.0)?
            .ok_or_else(|| stale("DFS file handle is no longer open"))?;
        let session = removed
            .state
            .lock()
            .map_err(|_| unavailable("DFS handle state is poisoned"))?
            .write_session
            .clone();
        if let Some(session) = session.as_ref() {
            if session.remote.is_some() {
                self.close_remote_write_session(&removed.inode_id, session)?;
            } else {
                self.release_writer(&session.local)?;
            }
        }
        Ok(())
    }

    fn opendir(&self, _: &RequestContext, inode: BackendInode) -> Result<DirectoryHandle> {
        let inode_id = self.inode_id(inode)?;
        let record = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
        if record.kind != InodeKind::Directory {
            return Err(Error::from(std::io::Error::from(
                std::io::ErrorKind::NotADirectory,
            )));
        }
        let entries = self
            .meta
            .read_dir(&inode_id)?
            .into_iter()
            .map(|entry| self.directory_entry(entry))
            .collect::<Result<Vec<_>>>()?;
        let id = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.dir_handles
            .lock()
            .map_err(|_| unavailable("DFS directory handle table is poisoned"))?
            .insert(
                id,
                DfsDirectoryHandle {
                    entries,
                    _gc_pin: self
                        .gc_references
                        .as_ref()
                        .map(|references| references.pin(&inode_id, None, None))
                        .transpose()?,
                },
            );
        Ok(DirectoryHandle(id))
    }

    fn readdir(
        &self,
        _: &RequestContext,
        handle: DirectoryHandle,
        cookie: u64,
        max_entries: usize,
    ) -> Result<Vec<DirectoryEntry>> {
        let handles = self
            .dir_handles
            .lock()
            .map_err(|_| unavailable("DFS directory handle table is poisoned"))?;
        let directory = handles
            .get(&handle.0)
            .ok_or_else(|| stale("DFS directory handle is no longer open"))?;
        slice_directory_entries(directory.entries.clone(), cookie, max_entries)
    }

    fn fsyncdir(&self, _: &RequestContext, handle: DirectoryHandle, _: SyncMode) -> Result<()> {
        let handles = self
            .dir_handles
            .lock()
            .map_err(|_| unavailable("DFS directory handle table is poisoned"))?;
        handles
            .get(&handle.0)
            .ok_or_else(|| stale("DFS directory handle is no longer open"))?;
        Ok(())
    }

    fn releasedir(&self, _: &RequestContext, handle: DirectoryHandle) -> Result<()> {
        self.dir_handles
            .lock()
            .map_err(|_| unavailable("DFS directory handle table is poisoned"))?
            .remove(&handle.0)
            .ok_or_else(|| stale("DFS directory handle is no longer open"))?;
        Ok(())
    }

    fn mkdir(
        &self,
        ctx: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        mode: u32,
    ) -> Result<Entry> {
        let now = now_unix_ms();
        let inode = self.validate_inode(self.meta.mkdir(
            &self.operation_id("mkdir"),
            &self.inode_id(parent)?,
            name.as_encoded_bytes(),
            InodeAttributes {
                mode: mode & !ctx.umask,
                uid: ctx.uid,
                gid: ctx.gid,
                nlink: 2,
                atime_unix_ms: now,
                mtime_unix_ms: now,
                ctime_unix_ms: now,
            },
            caller_context(ctx),
        )?)?;
        Ok(Entry {
            inode: self.backend_inode(&inode.inode_id)?,
            attributes: attributes(&inode, 0, 0),
        })
    }

    fn unlink(&self, ctx: &RequestContext, parent: BackendInode, name: &OsStr) -> Result<()> {
        self.refresh_inode_record(
            self.meta.unlink(
                &self.operation_id("unlink"),
                &self.inode_id(parent)?,
                name.as_encoded_bytes(),
                caller_context(ctx),
            )?,
            true,
            true,
        )?;
        Ok(())
    }

    fn rmdir(&self, ctx: &RequestContext, parent: BackendInode, name: &OsStr) -> Result<()> {
        self.refresh_inode_record(
            self.meta.rmdir(
                &self.operation_id("rmdir"),
                &self.inode_id(parent)?,
                name.as_encoded_bytes(),
                caller_context(ctx),
            )?,
            true,
            true,
        )?;
        Ok(())
    }

    fn rename(
        &self,
        ctx: &RequestContext,
        from_parent: BackendInode,
        from_name: &OsStr,
        to_parent: BackendInode,
        to_name: &OsStr,
        flags: RenameFlags,
    ) -> Result<()> {
        let mode = rename_mode(flags)?;
        let outcome = self.meta.rename(
            &self.operation_id("rename"),
            &self.inode_id(from_parent)?,
            from_name.as_encoded_bytes(),
            &self.inode_id(to_parent)?,
            to_name.as_encoded_bytes(),
            mode,
            caller_context(ctx),
        )?;
        self.refresh_inode_record(outcome.inode, true, true)?;
        if let Some(replaced) = outcome.replaced_inode {
            self.refresh_inode_record(replaced, true, true)?;
        }
        Ok(())
    }

    fn symlink(
        &self,
        ctx: &RequestContext,
        parent: BackendInode,
        name: &OsStr,
        target: &OsStr,
    ) -> Result<Entry> {
        let now = now_unix_ms();
        let inode = self.validate_inode(self.meta.symlink(SymlinkRequest {
            caller_id: self.node_id.clone(),
            operation_id: self.operation_id("symlink"),
            namespace_id: self.namespace_id.clone(),
            parent_inode_id: self.inode_id(parent)?,
            name: name.as_encoded_bytes().to_vec(),
            target: target.as_encoded_bytes().to_vec(),
            attributes: InodeAttributes {
                mode: libc::S_IFLNK | 0o777,
                uid: ctx.uid,
                gid: ctx.gid,
                nlink: 1,
                atime_unix_ms: now,
                mtime_unix_ms: now,
                ctime_unix_ms: now,
            },
            caller: caller_context(ctx),
        })?)?;
        Ok(Entry {
            inode: self.backend_inode(&inode.inode_id)?,
            attributes: attributes(&inode, 0, 0),
        })
    }

    fn readlink(&self, _: &RequestContext, inode: BackendInode) -> Result<OsString> {
        let target = self.meta.read_link(ReadLinkRequest {
            namespace_id: self.namespace_id.clone(),
            inode_id: self.inode_id(inode)?,
        })?;
        Ok(OsString::from_vec(target))
    }

    fn link(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        new_parent: BackendInode,
        new_name: &OsStr,
    ) -> Result<Entry> {
        let inode_id = self.inode_id(inode)?;
        let current = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
        let linked = self.refresh_inode_record(
            self.meta.link(
                &self.operation_id("link"),
                &inode_id,
                current.revision,
                &self.inode_id(new_parent)?,
                new_name.as_encoded_bytes(),
                caller_context(ctx),
            )?,
            true,
            true,
        )?;
        Ok(Entry {
            inode: self.backend_inode(&linked.inode_id)?,
            attributes: self.visible_attributes(&linked)?,
        })
    }

    fn getxattr_is_fixed_local(&self, name: &OsStr) -> bool {
        name.as_encoded_bytes() == b"security.capability"
    }

    fn getxattr(&self, ctx: &RequestContext, inode: BackendInode, name: &OsStr) -> Result<Vec<u8>> {
        let inode_id = self.inode_id(inode)?;
        if name.as_encoded_bytes() == b"security.capability" {
            return Err(Error::coded(
                afs_error::IO_NOT_SUPPORTED,
                "DFS currently supports only user.* xattrs",
            ));
        }
        self.meta.get_xattr(GetXattrRequest {
            caller: caller_context(ctx),
            inode_id,
            name: name.as_encoded_bytes().to_vec(),
        })
    }

    fn listxattr(&self, ctx: &RequestContext, inode: BackendInode) -> Result<Vec<u8>> {
        let names = self.meta.list_xattr(ListXattrRequest {
            caller: caller_context(ctx),
            inode_id: self.inode_id(inode)?,
        })?;
        let total = names.iter().map(|name| name.len() + 1).sum();
        let mut encoded = Vec::with_capacity(total);
        for name in names {
            encoded.extend_from_slice(&name);
            encoded.push(0);
        }
        Ok(encoded)
    }

    fn setxattr(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        name: &OsStr,
        value: &[u8],
        flags: i32,
    ) -> Result<()> {
        let inode_id = self.inode_id(inode)?;
        let current = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
        self.refresh_inode_record(
            self.meta.set_xattr(SetXattrRequest {
                caller_id: self.node_id.clone(),
                operation_id: self.operation_id("setxattr"),
                caller: caller_context(ctx),
                inode_id,
                expected_inode_revision: current.revision,
                name: name.as_encoded_bytes().to_vec(),
                value: value.to_vec(),
                mode: xattr_set_mode(flags)?,
            })?,
            true,
            true,
        )?;
        Ok(())
    }

    fn removexattr(&self, ctx: &RequestContext, inode: BackendInode, name: &OsStr) -> Result<()> {
        let inode_id = self.inode_id(inode)?;
        let current = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
        self.refresh_inode_record(
            self.meta.remove_xattr(RemoveXattrRequest {
                caller_id: self.node_id.clone(),
                operation_id: self.operation_id("removexattr"),
                caller: caller_context(ctx),
                inode_id,
                expected_inode_revision: current.revision,
                name: name.as_encoded_bytes().to_vec(),
            })?,
            true,
            true,
        )?;
        Ok(())
    }
}

impl DistributedFs {
    fn update_handle_write_progress(&self, handle: FileHandle, accepted_seq: u64) -> Result<()> {
        let opened = self
            .handles
            .get(handle.0)?
            .ok_or_else(|| stale("DFS file handle is no longer open"))?;
        let mut file = opened
            .state
            .lock()
            .map_err(|_| unavailable("DFS handle state is poisoned"))?;
        if let Some(session) = file.write_session.as_mut() {
            session.local.last_accepted_seq = accepted_seq;
        }
        Ok(())
    }

    fn update_handle_error_cursor(&self, handle: FileHandle, cursor: u64) -> Result<()> {
        let opened = self
            .handles
            .get(handle.0)?
            .ok_or_else(|| stale("DFS file handle is no longer open"))?;
        let mut file = opened
            .state
            .lock()
            .map_err(|_| unavailable("DFS handle state is poisoned"))?;
        if let Some(session) = file.write_session.as_mut() {
            session.local.error_cursor = cursor;
        }
        Ok(())
    }

    fn local_write_activity(&self, inode_id: &InodeId) -> Result<bool> {
        let Some(state) = self.write_state(inode_id)? else {
            return Ok(false);
        };
        let state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        Ok(state.has_local_write_activity())
    }

    fn update_handles_after_commit(&self, inode_id: &InodeId, committed_seq: u64) -> Result<()> {
        let inode = self
            .write_state(inode_id)?
            .ok_or_else(|| stale("DFS write state is no longer open"))?
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?
            .inode
            .clone();
        for handle in self.handles.for_inode(inode_id)? {
            let mut handle = handle
                .state
                .lock()
                .map_err(|_| unavailable("DFS handle state is poisoned"))?;
            handle.native_read_generation = handle
                .native_read_generation
                .checked_add(1)
                .ok_or_else(|| unavailable("DFS native read generation exhausted"))?;
            handle.native_read_view = None;
            handle.opened_page_identity = None;
            // This Node's commits are visible to every handle on it (4a).
            handle.opened_inode = Arc::new(inode.clone());
            if let Some(session) = handle.write_session.as_mut() {
                session.local.last_synced_seq = session.local.last_synced_seq.max(committed_seq);
            }
        }
        Ok(())
    }
}

fn write_session(
    state: &InodeWriteState,
    open_flags: i32,
    id: DfsWriteSessionId,
) -> DfsWriteSession {
    DfsWriteSession {
        id,
        inode_id: state.inode.inode_id.clone(),
        open_flags,
        lease_epoch: state.write_lease.lease_epoch,
        last_accepted_seq: state.visible_write_seq,
        last_synced_seq: state.committed_write_seq,
        error_cursor: state
            .background_error
            .as_ref()
            .map_or(0, |error| error.cursor),
    }
}

enum DfsOwnerOpenAdmissionStart {
    Replay(afs_protocol::node_control::DfsOwnerHandle),
    Cancelled,
    Apply {
        route: DfsOwnerOpenRouteKey,
        open_seq: u64,
        fingerprint: [u8; 32],
    },
}

impl crate::node::rpc::control::DfsOwnerLifecycleHandler for DistributedFs {
    fn open(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
        if request.namespace_id != self.namespace_id.0
            || request.owner_node_id != self.node_id
            || request.owner_session_id != self.session_id
        {
            return Err(stale("DFS owner open targets another owner session"));
        }
        let inode_id = InodeId::new(request.inode_id.clone());
        if self.meta.current_node_session(peer)?.as_deref()
            != Some(request.caller_session_id.as_str())
        {
            return Err(stale("DFS owner open caller session is stale"));
        }
        let admission = self.start_owner_open_admission(peer, &request)?;
        let (route, open_seq, fingerprint) = match admission {
            DfsOwnerOpenAdmissionStart::Replay(handle) => {
                return Ok(afs_protocol::node_control::DfsOwnerOpenReply {
                    handle: Some(handle),
                });
            }
            DfsOwnerOpenAdmissionStart::Cancelled => {
                return Err(stale("DFS owner open admission was already retired"));
            }
            DfsOwnerOpenAdmissionStart::Apply {
                route,
                open_seq,
                fingerprint,
            } => (route, open_seq, fingerprint),
        };
        let (inode, lease) = match self.meta.resolve_write_authority(&inode_id) {
            Ok(opened) => opened,
            Err(error) => {
                self.finish_failed_owner_open_admission(&route, open_seq)?;
                return Err(error);
            }
        };
        let inode = match self.validate_inode(inode) {
            Ok(inode) => inode,
            Err(error) => {
                self.finish_failed_owner_open_admission(&route, open_seq)?;
                return Err(error);
            }
        };
        if lease.owner_node_id != self.node_id
            || lease.owner_session_id != self.session_id
            || lease.lease_epoch != request.lease_epoch
        {
            self.finish_failed_owner_open_admission(&route, open_seq)?;
            return Err(stale("DFS owner lease is no longer current"));
        }
        let apply = (|| {
            self.install_write_state(inode.clone(), lease.clone())?;
            let session = self.open_write_session(&inode, request.open_flags)?;
            if session.lease_epoch != request.lease_epoch {
                self.release_writer(&session)?;
                return Err(stale("DFS owner lease changed during open admission"));
            }
            if request.kill_suidgid {
                let state = self
                    .write_state(&inode.inode_id)?
                    .ok_or_else(|| stale("DFS write state is no longer open"))?;
                let mut state = self.lock_for_mutation(&inode.inode_id, &state)?;
                Self::apply_kernel_killpriv_to_state(&mut state);
            }
            self.allocate_handle(DfsFileHandle {
                gc_pin: None,
                inode_id: inode.inode_id.clone(),
                opened_inode: inode.clone(),
                opened_page_identity: None,
                native_read_view: None,
                native_read_generation: 0,
                native_denial_serial: 0,
                write_session: Some(Self::make_local_write_session(session)),
                flags: request.open_flags,
                owner_scope: Some(DfsOwnerHandleScope {
                    caller_node_id: peer.to_owned(),
                    caller_session_id: request.caller_session_id.clone(),
                    lease_epoch: lease.lease_epoch,
                    open_seq,
                }),
            })
        })();
        let handle = match apply {
            Ok(handle) => handle,
            Err(error) => {
                self.finish_failed_owner_open_admission(&route, open_seq)?;
                return Err(error);
            }
        };
        let wire = self.owner_handle_for(
            peer,
            request.caller_session_id,
            &inode,
            &lease,
            handle,
            open_seq,
        );
        #[cfg(test)]
        self.pause_before_owner_open_finish();
        let cancelled =
            match self.finish_owner_open_admission(&route, open_seq, fingerprint, wire.clone()) {
                Ok(cancelled) => cancelled,
                Err(error) => {
                    match <Self as Backend>::release(self, &owner_request_context(), handle) {
                        Ok(()) => {}
                        Err(release_error)
                            if release_error.code() == afs_error::NODE_DFS_STALE_HANDLE => {}
                        Err(release_error) => return Err(release_error),
                    }
                    return Err(error);
                }
            };
        if cancelled {
            <Self as Backend>::release(self, &owner_request_context(), handle)?;
            self.finish_owner_open_identity_retirement(peer, &wire)?;
            return Err(stale("DFS owner open admission was cancelled"));
        }
        Ok(afs_protocol::node_control::DfsOwnerOpenReply { handle: Some(wire) })
    }

    fn getattr(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
        let handle = request
            .handle
            .ok_or_else(|| stale("DFS owner getattr missing handle"))?;
        let file = self.validate_owner_handle(peer, &handle)?;
        let snapshot = self.handle_snapshot(file)?;
        let attr = self.visible_attributes(&snapshot.opened_inode)?;
        Ok(afs_protocol::node_control::DfsOwnerGetAttrReply {
            attr: Some(Self::wire_attr(attr)),
        })
    }

    fn get_lock(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerGetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
        let authority = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        let lock = request
            .lock
            .ok_or_else(|| invalid("DFS owner lock request is missing"))?;
        let caller_session_id = authority.caller_session_id.clone();
        let (_, authority) = self.local_authority_from_wire(peer, authority)?;
        let lock = Self::scoped_remote_lock_request(peer, &caller_session_id, lock)?;
        self.check_lock_renewal(&authority)?;
        let conflict = authority
            .table
            .getlk(&lock)
            .map_err(lock_error)?
            .map(Self::wire_lock_conflict);
        Ok(afs_protocol::node_control::DfsOwnerGetLockReply { conflict })
    }

    fn set_lock(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerSetLockRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
        let authority_wire = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        let lock = request
            .lock
            .ok_or_else(|| invalid("DFS owner lock request is missing"))?;
        let caller_session_id = authority_wire.caller_session_id.clone();
        let waiter = request
            .waiter
            .map(|waiter| Self::scoped_remote_lock_waiter(peer, &caller_session_id, waiter))
            .transpose()?;
        let (_, authority) = self.local_authority_from_wire(peer, authority_wire)?;
        let lock = Self::scoped_remote_lock_request(peer, &caller_session_id, lock)?;
        self.ensure_lock_session_open(&lock.owner.ingress_session_id)?;
        if let Some(waiter_id) = waiter.as_ref() {
            if waiter_id.ingress_session_id != lock.owner.ingress_session_id {
                return Err(invalid("DFS lock waiter scope does not match owner scope"));
            }
            self.ensure_lock_session_open(&waiter_id.ingress_session_id)?;
        }
        self.check_lock_renewal(&authority)?;
        let result = if let Some(waiter_id) = waiter.clone() {
            self.register_lock_waiter(&authority, &waiter_id)?;
            let result = authority
                .table
                .setlk_blocking(lock.clone(), waiter_id.clone())
                .map_err(lock_error);
            self.unpin_lock_waiter(&authority, waiter.as_ref())?;
            result
        } else {
            authority
                .table
                .setlk_nonblocking(lock.clone())
                .map_err(lock_error)
        };
        if result.is_ok() {
            self.register_local_lock_success(&authority, &lock)?;
            self.check_lock_renewal_after_mutation(&authority)?;
        }
        result?;
        Ok(afs_protocol::node_control::DfsOwnerSetLockReply {})
    }

    fn cancel_lock_wait(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
        let authority_wire = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        let caller_session_id = authority_wire.caller_session_id.clone();
        let waiter = Self::scoped_remote_lock_waiter(
            peer,
            &caller_session_id,
            request
                .waiter
                .ok_or_else(|| invalid("DFS owner lock waiter is missing"))?,
        )?;
        let Some((_, authority)) = self.cached_local_authority_from_wire(peer, authority_wire)?
        else {
            self.note_precancelled_lock_waiter(waiter)?;
            return Ok(afs_protocol::node_control::DfsOwnerCancelLockWaitReply { outcome: 1 });
        };
        let outcome = authority
            .table
            .cancel_waiter_with_outcome(waiter.clone())
            .map_err(lock_error)?;
        authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
            .waiters
            .remove(&waiter);
        Ok(afs_protocol::node_control::DfsOwnerCancelLockWaitReply {
            outcome: match outcome {
                LockWaiterOutcome::Cancelled => 1,
                LockWaiterOutcome::Granted => 2,
                LockWaiterOutcome::Unknown => 3,
            },
        })
    }

    fn acknowledge_lock_wait(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
        let authority_wire = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        let caller_session_id = authority_wire.caller_session_id.clone();
        let waiter = Self::scoped_remote_lock_waiter(
            peer,
            &caller_session_id,
            request
                .waiter
                .ok_or_else(|| invalid("DFS owner lock waiter is missing"))?,
        )?;
        let Some((_, authority)) = self.cached_local_authority_from_wire(peer, authority_wire)?
        else {
            return Ok(afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply {});
        };
        authority
            .table
            .acknowledge_waiter(&waiter)
            .map_err(lock_error)?;
        authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?
            .waiters
            .remove(&waiter);
        Ok(afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply {})
    }

    fn release_locks(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
        let authority_wire = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        let caller_session_id = authority_wire.caller_session_id.clone();
        let owner = Self::scoped_remote_lock_owner(
            peer,
            &caller_session_id,
            request
                .owner
                .ok_or_else(|| invalid("DFS owner lock owner is missing"))?,
        )?;
        let kind = Self::lock_release_kind_from_wire(request.release_kind)?;
        let Some((_, authority)) = self.cached_local_authority_from_wire(peer, authority_wire)?
        else {
            return Ok(afs_protocol::node_control::DfsOwnerReleaseLocksReply {});
        };
        match kind {
            ReleaseKind::PosixOwner => authority.table.release_posix_owner(&owner),
            ReleaseKind::FlockOwner => authority.table.release_flock_owner(&owner),
        }
        .map_err(lock_error)?;
        self.unpin_owner_if_inactive(
            &authority,
            &owner,
            Some(match kind {
                ReleaseKind::PosixOwner => FileLockKind::Posix,
                ReleaseKind::FlockOwner => FileLockKind::Flock,
            }),
        )?;
        Ok(afs_protocol::node_control::DfsOwnerReleaseLocksReply {})
    }

    fn release_lock_session(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
        let authority_wire = request
            .authority
            .ok_or_else(|| stale("DFS owner lock authority is missing"))?;
        if request.ingress_session_id.is_empty() {
            return Err(invalid("DFS owner release lock session is empty"));
        }
        let caller_session_id = authority_wire.caller_session_id.clone();
        let scoped_session =
            Self::lock_scope(peer, &caller_session_id, &request.ingress_session_id)?;
        self.note_closed_lock_session(&scoped_session)?;
        let Some((_, authority)) = self.cached_local_authority_from_wire(peer, authority_wire)?
        else {
            return Ok(afs_protocol::node_control::DfsOwnerReleaseLockSessionReply {});
        };
        let known_session = {
            let state = authority
                .state
                .lock()
                .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
            state
                .pinned_owners
                .iter()
                .any(|owner| owner.ingress_session_id == scoped_session)
                || state
                    .waiters
                    .iter()
                    .any(|waiter| waiter.ingress_session_id == scoped_session)
        };
        if !known_session {
            return Ok(afs_protocol::node_control::DfsOwnerReleaseLockSessionReply {});
        }
        authority
            .table
            .release_session(&scoped_session)
            .map_err(lock_error)?;
        let mut state = authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS lock authority state is poisoned"))?;
        state
            .pinned_owners
            .retain(|owner| owner.ingress_session_id != scoped_session);
        state
            .waiters
            .retain(|waiter| waiter.ingress_session_id != scoped_session);
        Ok(afs_protocol::node_control::DfsOwnerReleaseLockSessionReply {})
    }

    fn release(
        &self,
        peer: &str,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
    ) -> Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
        let handle = request
            .handle
            .ok_or_else(|| stale("DFS owner release missing handle"))?;
        if handle.opaque_handle.is_empty() {
            if let Some(active) =
                self.prepare_owner_open_identity_retirement(peer, &handle, true)?
            {
                let file = self.owner_release_file_handle(peer, &active)?;
                let file = file.ok_or_else(|| stale("DFS owner release handle is not active"))?;
                <Self as Backend>::release(self, &owner_request_context(), file)?;
                self.finish_owner_open_identity_retirement(peer, &active)?;
            }
            return Ok(afs_protocol::node_control::DfsOwnerReleaseReply {});
        }
        self.validate_owner_release_session_current(peer, &handle)?;
        if let Some(file) = self.owner_release_file_handle(peer, &handle)? {
            <Self as Backend>::release(self, &owner_request_context(), file)?;
        }
        self.finish_owner_open_identity_retirement(peer, &handle)?;
        Ok(afs_protocol::node_control::DfsOwnerReleaseReply {})
    }
}

impl crate::node::rpc::data::DfsOwnerFilesHandler for DistributedFs {
    fn read(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
        let handle = request
            .handle
            .ok_or_else(|| stale("DFS owner read missing handle"))?;
        let handle = Self::control_owner_handle(&handle);
        let file = self.validate_owner_handle(peer, &handle)?;
        let length = usize::try_from(request.length)
            .map_err(|_| invalid("DFS owner read length exceeds platform limits"))?;
        let data = <Self as Backend>::read_owned(
            self,
            &owner_request_context(),
            file,
            request.offset,
            length,
        )?;
        let visible_write_seq = self
            .write_state(&InodeId::new(handle.inode_id))?
            .and_then(|state| state.lock().ok().map(|state| state.visible_write_seq))
            .unwrap_or(0);
        Ok(afs_protocol::node_data::DfsOwnerReadReply {
            data,
            visible_write_seq,
        })
    }

    fn write(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerWriteRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
        let handle = request
            .handle
            .clone()
            .ok_or_else(|| stale("DFS owner write missing handle"))?;
        let handle = Self::control_owner_handle(&handle);
        let file = self.validate_owner_handle(peer, &handle)?;
        let operation_id = OperationId::new(request.operation_id.clone());
        let key = Self::remote_operation_key(peer, &handle, operation_id);
        let fingerprint = Self::fingerprint_remote_write(
            &handle,
            request.offset,
            request.append,
            &request.data,
            request.kill_suidgid,
        );
        if let Some(RemoteOperationResult::Write { reply, .. }) =
            self.remote_operation_cached(&key, fingerprint)?
        {
            return Ok(reply);
        }
        let offset = if request.append {
            let snapshot = self.handle_snapshot(file)?;
            self.visible_attributes(&snapshot.opened_inode)?.size
        } else {
            request.offset
        };
        let written = <Self as Backend>::write_with_options(
            self,
            &owner_request_context(),
            file,
            offset,
            &request.data,
            WriteOptions {
                kill_suidgid: request.kill_suidgid,
            },
        )?;
        let snapshot = self.handle_snapshot(file)?;
        let accepted = snapshot
            .write_session
            .as_ref()
            .map_or(0, |session| session.local.last_accepted_seq);
        let reply = afs_protocol::node_data::DfsOwnerWriteReply {
            written: written as u64,
            accepted_write_seq: accepted,
            effective_offset: offset,
        };
        self.remember_remote_operation(key, RemoteOperationResult::Write { fingerprint, reply })?;
        Ok(reply)
    }

    fn resize(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerResizeRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
        let handle = request
            .handle
            .clone()
            .ok_or_else(|| stale("DFS owner resize missing handle"))?;
        let handle = Self::control_owner_handle(&handle);
        let file = self.validate_owner_handle(peer, &handle)?;
        let operation_id = OperationId::new(request.operation_id.clone());
        let key = Self::remote_operation_key(peer, &handle, operation_id);
        let fingerprint =
            Self::fingerprint_remote_resize(&handle, request.length, request.kill_suidgid);
        if let Some(RemoteOperationResult::Resize { reply, .. }) =
            self.remote_operation_cached(&key, fingerprint)?
        {
            return Ok(reply);
        }
        let snapshot = self.handle_snapshot(file)?;
        let accepted =
            self.resize_dirty_inode(&snapshot.inode_id, request.length, request.kill_suidgid)?;
        self.update_handle_write_progress(file, accepted)?;
        let reply = afs_protocol::node_data::DfsOwnerResizeReply {
            accepted_write_seq: accepted,
        };
        self.remember_remote_operation(key, RemoteOperationResult::Resize { fingerprint, reply })?;
        Ok(reply)
    }

    fn sync(
        &self,
        peer: &str,
        request: afs_protocol::node_data::DfsOwnerSyncRequest,
    ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
        let handle = request
            .handle
            .clone()
            .ok_or_else(|| stale("DFS owner sync missing handle"))?;
        let handle = Self::control_owner_handle(&handle);
        let file = self.validate_owner_handle(peer, &handle)?;
        let operation_id = OperationId::new(request.operation_id.clone());
        let key = Self::remote_operation_key(peer, &handle, operation_id);
        let fingerprint =
            Self::fingerprint_remote_sync(&handle, request.through_write_seq, request.data_only);
        if let Some(RemoteOperationResult::Sync { reply, .. }) =
            self.remote_operation_cached(&key, fingerprint)?
        {
            return Ok(reply);
        }
        <Self as Backend>::fsync(
            self,
            &owner_request_context(),
            file,
            if request.data_only {
                SyncMode::DataOnly
            } else {
                SyncMode::Full
            },
        )?;
        let snapshot = self.handle_snapshot(file)?;
        let reply = afs_protocol::node_data::DfsOwnerSyncReply {
            durable_write_seq: snapshot
                .write_session
                .as_ref()
                .map_or(0, |session| session.local.last_synced_seq),
            file_version_id: self
                .write_state(&snapshot.inode_id)?
                .and_then(|state| {
                    state.lock().ok().and_then(|state| {
                        state
                            .base_version
                            .as_ref()
                            .map(|version| version.id.0.clone())
                    })
                })
                .unwrap_or_default(),
        };
        self.remember_remote_operation(
            key,
            RemoteOperationResult::Sync {
                fingerprint,
                reply: reply.clone(),
            },
        )?;
        Ok(reply)
    }
}

impl DistributedFs {
    #[cfg(test)]
    fn set_owner_open_finish_pause(&self, pause: Option<Arc<OwnerOpenFinishPause>>) {
        *self.owner_open_finish_pause.lock().unwrap() = pause;
    }

    #[cfg(test)]
    fn pause_before_owner_open_finish(&self) {
        let pause = self.owner_open_finish_pause.lock().unwrap().clone();
        if let Some(pause) = pause {
            pause.pause();
        }
    }

    fn validate_owner_open_seq(
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<u64> {
        if request.open_seq == 0 {
            return Err(stale("DFS owner open sequence is zero"));
        }
        Ok(request.open_seq)
    }

    fn owner_open_route(
        peer: &str,
        caller_session_id: &str,
        owner_node_id: &str,
        owner_session_id: &str,
    ) -> DfsOwnerOpenRouteKey {
        DfsOwnerOpenRouteKey {
            caller_node_id: peer.to_owned(),
            caller_session_id: caller_session_id.to_owned(),
            owner_node_id: owner_node_id.to_owned(),
            owner_session_id: owner_session_id.to_owned(),
        }
    }

    fn owner_open_fingerprint(
        peer: &str,
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<[u8; 32]> {
        let open_seq = Self::validate_owner_open_seq(request)?;
        let mut hasher = blake3::Hasher::new();
        for part in [
            peer.as_bytes(),
            request.namespace_id.as_bytes(),
            request.inode_id.as_bytes(),
            request.owner_node_id.as_bytes(),
            request.owner_session_id.as_bytes(),
            request.caller_session_id.as_bytes(),
        ] {
            hasher.update(&(part.len() as u64).to_be_bytes());
            hasher.update(part);
        }
        hasher.update(&request.lease_epoch.to_be_bytes());
        hasher.update(&request.open_flags.to_be_bytes());
        hasher.update(&[u8::from(request.kill_suidgid)]);
        hasher.update(&open_seq.to_be_bytes());
        Ok(*hasher.finalize().as_bytes())
    }

    fn start_owner_open_admission(
        &self,
        peer: &str,
        request: &afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> Result<DfsOwnerOpenAdmissionStart> {
        let open_seq = Self::validate_owner_open_seq(request)?;
        let route = Self::owner_open_route(
            peer,
            &request.caller_session_id,
            &request.owner_node_id,
            &request.owner_session_id,
        );
        let fingerprint = Self::owner_open_fingerprint(peer, request)?;
        let mut routes = self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?;
        if !routes.contains_key(&route) && routes.len() >= MAX_PENDING_REMOTE_RELEASES {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS owner open route table is full",
            ));
        }
        let state = routes.entry(route.clone()).or_default();
        if let Some(active) = state.active.get(&open_seq) {
            if active.fingerprint == fingerprint {
                return Ok(DfsOwnerOpenAdmissionStart::Replay(active.handle.clone()));
            }
            return Err(stale("DFS owner open replay fingerprint changed"));
        }
        if state.cancelled.contains(&open_seq) {
            return Ok(DfsOwnerOpenAdmissionStart::Cancelled);
        }
        if let Some(existing) = state.inflight.get(&open_seq) {
            if existing.fingerprint == fingerprint {
                return Err(unavailable("DFS owner open admission is still pending"));
            }
            return Err(stale("DFS owner open replay fingerprint changed"));
        }
        if open_seq <= state.highwater {
            return Err(stale("DFS owner open sequence is already retired"));
        }
        if state.cancelled.len().saturating_add(state.inflight.len()) >= MAX_PENDING_REMOTE_RELEASES
        {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS owner open admission table is full",
            ));
        }
        state.highwater = open_seq;
        state.inflight.insert(
            open_seq,
            DfsOwnerOpenInflight {
                fingerprint,
                cleanup_scope: Self::cleanup_handle_for_open_request(peer, request)?,
            },
        );
        Ok(DfsOwnerOpenAdmissionStart::Apply {
            route,
            open_seq,
            fingerprint,
        })
    }

    fn finish_failed_owner_open_admission(
        &self,
        route: &DfsOwnerOpenRouteKey,
        open_seq: u64,
    ) -> Result<()> {
        let mut routes = self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?;
        if let Some(state) = routes.get_mut(route) {
            state.inflight.remove(&open_seq);
            state.cancelled.remove(&open_seq);
        }
        Ok(())
    }

    fn finish_owner_open_admission(
        &self,
        route: &DfsOwnerOpenRouteKey,
        open_seq: u64,
        fingerprint: [u8; 32],
        handle: afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<bool> {
        let mut routes = self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?;
        let state = routes
            .get_mut(route)
            .ok_or_else(|| stale("DFS owner open route was retired"))?;
        state.inflight.remove(&open_seq);
        let cancelled = state.cancelled.remove(&open_seq);
        state.active.insert(
            open_seq,
            DfsOwnerOpenAdmission {
                fingerprint,
                handle,
            },
        );
        Ok(cancelled)
    }

    fn validate_owner_open_release_identity(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<DfsOwnerOpenRouteKey> {
        if handle.open_seq == 0 {
            return Err(stale("DFS owner open sequence is missing"));
        }
        if handle.namespace_id != self.namespace_id.0
            || handle.owner_node_id != self.node_id
            || handle.owner_session_id != self.session_id
            || handle.caller_node_id != peer
            || handle.caller_session_id.is_empty()
            || handle.lease_epoch == 0
        {
            return Err(stale("DFS owner handle identity does not match this owner"));
        }
        let current = self.meta.current_node_session(peer)?;
        if current.as_deref() != Some(handle.caller_session_id.as_str()) {
            return Err(stale("DFS owner release caller session is stale"));
        }
        Ok(Self::owner_open_route(
            peer,
            &handle.caller_session_id,
            &handle.owner_node_id,
            &handle.owner_session_id,
        ))
    }

    fn owner_open_active_scope_matches(
        active: &afs_protocol::node_control::DfsOwnerHandle,
        requested: &afs_protocol::node_control::DfsOwnerHandle,
        cleanup_only: bool,
    ) -> bool {
        active.namespace_id == requested.namespace_id
            && active.inode_id == requested.inode_id
            && active.owner_node_id == requested.owner_node_id
            && active.owner_session_id == requested.owner_session_id
            && active.lease_epoch == requested.lease_epoch
            && active.caller_node_id == requested.caller_node_id
            && active.caller_session_id == requested.caller_session_id
            && active.open_seq == requested.open_seq
            && (cleanup_only || active.opaque_handle == requested.opaque_handle)
    }

    fn prepare_owner_open_identity_retirement(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        cleanup_only: bool,
    ) -> Result<Option<afs_protocol::node_control::DfsOwnerHandle>> {
        let route = self.validate_owner_open_release_identity(peer, handle)?;
        let mut routes = self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?;
        if !routes.contains_key(&route) && routes.len() >= MAX_PENDING_REMOTE_RELEASES {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS owner open route table is full",
            ));
        }
        let state = routes.entry(route).or_default();
        if let Some(active) = state.active.get(&handle.open_seq) {
            if !Self::owner_open_active_scope_matches(&active.handle, handle, cleanup_only) {
                return Err(stale(
                    "DFS owner open release scope does not match active admission",
                ));
            }
            return Ok(Some(active.handle.clone()));
        }
        if let Some(inflight) = state.inflight.get(&handle.open_seq) {
            if !Self::owner_open_active_scope_matches(&inflight.cleanup_scope, handle, true) {
                return Err(stale(
                    "DFS owner open release scope does not match inflight admission",
                ));
            }
            state.cancelled.insert(handle.open_seq);
            return Ok(None);
        }
        if handle.open_seq > state.highwater {
            state.highwater = handle.open_seq;
        }
        Ok(None)
    }

    fn finish_owner_open_identity_retirement(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        let route = self.validate_owner_open_release_identity(peer, handle)?;
        let mut routes = self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?;
        if let Some(state) = routes.get_mut(&route) {
            if let Some(active) = state.active.get(&handle.open_seq)
                && !Self::owner_open_active_scope_matches(&active.handle, handle, false)
            {
                return Err(stale(
                    "DFS owner open release scope does not match active admission",
                ));
            }
            state.active.remove(&handle.open_seq);
            state.cancelled.remove(&handle.open_seq);
            if handle.open_seq > state.highwater {
                state.highwater = handle.open_seq;
            }
        }
        Ok(())
    }

    fn validate_owner_release_session_current(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<()> {
        if handle.namespace_id != self.namespace_id.0
            || handle.owner_node_id != self.node_id
            || handle.owner_session_id != self.session_id
            || handle.caller_node_id != peer
            || handle.caller_session_id.is_empty()
            || handle.lease_epoch == 0
            || handle.open_seq == 0
        {
            return Err(stale("DFS owner handle identity does not match this owner"));
        }
        let current = self.meta.current_node_session(peer)?;
        if current.as_deref() != Some(handle.caller_session_id.as_str()) {
            return Err(stale("DFS owner release caller session is stale"));
        }
        Ok(())
    }

    fn validate_owner_handle(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<FileHandle> {
        let (inode_id, file) = self.validate_owner_handle_basic(peer, handle)?;
        let snapshot = self.handle_snapshot(file)?;
        self.validate_owner_handle_scope(peer, handle, &inode_id, &snapshot)?;
        let current = self.meta.current_node_session(peer)?;
        if current.as_deref() != Some(handle.caller_session_id.as_str()) {
            return Err(stale("DFS owner caller session is stale"));
        }
        let state = self
            .write_state(&inode_id)?
            .ok_or_else(|| stale("DFS owner write state is not open"))?;
        let state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        if state.write_lease.lease_epoch != handle.lease_epoch
            || state.write_lease.owner_node_id != self.node_id
            || state.write_lease.owner_session_id != self.session_id
        {
            return Err(stale("DFS owner lease epoch is stale"));
        }
        Ok(file)
    }

    fn validate_owner_handle_basic(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<(InodeId, FileHandle)> {
        if handle.namespace_id != self.namespace_id.0
            || handle.owner_node_id != self.node_id
            || handle.owner_session_id != self.session_id
            || handle.caller_node_id != peer
            || handle.lease_epoch == 0
            || handle.open_seq == 0
        {
            return Err(stale("DFS owner handle identity does not match this owner"));
        }
        Ok((
            InodeId::new(handle.inode_id.clone()),
            Self::file_handle_from_owner(handle)?,
        ))
    }

    fn validate_owner_handle_scope(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
        inode_id: &InodeId,
        snapshot: &DfsFileHandleSnapshot,
    ) -> Result<()> {
        if snapshot.inode_id != *inode_id {
            return Err(stale("DFS owner handle names another inode"));
        }
        let Some(scope) = snapshot.owner_scope.as_ref() else {
            return Err(stale("DFS owner handle is not a remote caller handle"));
        };
        if scope.caller_node_id != peer
            || scope.caller_session_id != handle.caller_session_id
            || scope.lease_epoch != handle.lease_epoch
            || scope.open_seq != handle.open_seq
        {
            return Err(stale("DFS owner handle caller scope does not match"));
        }
        Ok(())
    }

    fn owner_release_file_handle(
        &self,
        peer: &str,
        handle: &afs_protocol::node_control::DfsOwnerHandle,
    ) -> Result<Option<FileHandle>> {
        let (inode_id, file) = self.validate_owner_handle_basic(peer, handle)?;
        let Some(opened) = self.handles.get(file.0)? else {
            return Ok(None);
        };
        let snapshot = DfsFileHandleSnapshot::from_handle(&opened)?;
        self.validate_owner_handle_scope(peer, handle, &inode_id, &snapshot)?;
        Ok(Some(file))
    }

    fn peer_owner_handle_sessions(&self) -> Result<HashMap<String, HashSet<String>>> {
        let mut sessions: HashMap<String, HashSet<String>> = HashMap::new();
        for (_, handle) in self.handles.snapshot()? {
            if let Some(scope) = handle.owner_scope.as_ref() {
                sessions
                    .entry(scope.caller_node_id.clone())
                    .or_default()
                    .insert(scope.caller_session_id.clone());
            }
        }
        for route in self
            .owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?
            .keys()
        {
            sessions
                .entry(route.caller_node_id.clone())
                .or_default()
                .insert(route.caller_session_id.clone());
        }
        Ok(sessions)
    }

    pub fn reap_expired_peer_owner_handles(&self) -> Result<usize> {
        let sessions_by_node = self.peer_owner_handle_sessions()?;
        let mut peers = sessions_by_node.into_iter().collect::<Vec<_>>();
        peers.sort_by(|left, right| left.0.cmp(&right.0));
        if peers.is_empty() {
            return Ok(0);
        }
        let start =
            (self.owner_handle_reap_cursor.fetch_add(1, Ordering::AcqRel) as usize) % peers.len();
        peers.rotate_left(start);
        let deadline = std::time::Instant::now() + OWNER_HANDLE_REAP_BUDGET;
        let mut reaped = 0usize;
        let mut first_error = None;
        let mut checked = 0usize;
        for (node_id, sessions) in peers {
            if checked >= OWNER_HANDLE_REAP_MAX_PEERS || std::time::Instant::now() >= deadline {
                break;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let timeout = remaining.min(REMOTE_RELEASE_RPC_BUDGET);
            if timeout.is_zero() {
                break;
            }
            checked = checked.saturating_add(1);
            match self
                .meta
                .current_node_session_with_timeout(&node_id, timeout)
            {
                Ok(current) => {
                    for session_id in sessions {
                        if current.as_deref() != Some(session_id.as_str()) {
                            match self.reap_peer_owner_session(&node_id, &session_id) {
                                Ok(count) => reaped = reaped.saturating_add(count),
                                Err(error) => {
                                    first_error.get_or_insert(error);
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(reaped)
    }

    fn reap_peer_owner_session(&self, node_id: &str, session_id: &str) -> Result<usize> {
        let handles = self
            .handles
            .snapshot()?
            .into_iter()
            .filter_map(|(id, handle)| {
                handle.owner_scope.as_ref().and_then(|scope| {
                    (scope.caller_node_id == node_id && scope.caller_session_id == session_id)
                        .then_some(FileHandle(id))
                })
            })
            .collect::<Vec<_>>();
        let mut reaped = 0usize;
        let mut first_error = None;
        for file in handles {
            match <Self as Backend>::release(self, &owner_request_context(), file) {
                Ok(()) => reaped = reaped.saturating_add(1),
                Err(error) if error.code() == afs_error::NODE_DFS_STALE_HANDLE => {}
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        self.owner_open_routes
            .lock()
            .map_err(|_| unavailable("DFS owner open route table is poisoned"))?
            .retain(|route, _| {
                !(route.caller_node_id == node_id
                    && route.caller_session_id == session_id
                    && route.owner_session_id == self.session_id)
            });
        Ok(reaped)
    }
}

fn owner_request_context() -> RequestContext {
    RequestContext {
        uid: 0,
        gid: 0,
        pid: 0,
        umask: 0,
        supplementary_gids: Vec::new(),
    }
}

fn should_renew(lease: &WriteLease) -> bool {
    lease.expires_at_unix_ms <= now_unix_ms().saturating_add(5_000)
}

fn should_background_renew(lease: &WriteLease) -> bool {
    let renew_before_expiry_ms = DFS_WRITE_LEASE_SECONDS.saturating_mul(1_000) * 2 / 3;
    lease.expires_at_unix_ms <= now_unix_ms().saturating_add(renew_before_expiry_ms)
}

fn has_o_dsync(flags: i32) -> bool {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        flags & libc::O_DSYNC != 0
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        let _ = flags;
        false
    }
}

fn file_kind(kind: InodeKind) -> FileKind {
    match kind {
        InodeKind::Regular => FileKind::Regular,
        InodeKind::Directory => FileKind::Directory,
        InodeKind::Symlink => FileKind::Symlink,
        InodeKind::Special(kind) => FileKind::Special(vfs_special_kind(kind)),
    }
}

fn dfs_special_kind(kind: SpecialFileKind) -> SpecialNodeKind {
    match kind {
        SpecialFileKind::Fifo => SpecialNodeKind::Fifo,
        SpecialFileKind::Socket => SpecialNodeKind::Socket,
        SpecialFileKind::BlockDevice { rdev } => SpecialNodeKind::BlockDevice { rdev },
        SpecialFileKind::CharDevice { rdev } => SpecialNodeKind::CharDevice { rdev },
    }
}

fn vfs_special_kind(kind: SpecialNodeKind) -> SpecialFileKind {
    match kind {
        SpecialNodeKind::Fifo => SpecialFileKind::Fifo,
        SpecialNodeKind::Socket => SpecialFileKind::Socket,
        SpecialNodeKind::BlockDevice { rdev } => SpecialFileKind::BlockDevice { rdev },
        SpecialNodeKind::CharDevice { rdev } => SpecialFileKind::CharDevice { rdev },
    }
}

fn dfs_owner_special_node(
    kind: SpecialFileKind,
) -> afs_protocol::node_control::DfsOwnerSpecialNode {
    let (kind, rdev) = match kind {
        SpecialFileKind::Fifo => (afs_protocol::node_control::DfsOwnerSpecialNodeKind::Fifo, 0),
        SpecialFileKind::Socket => (
            afs_protocol::node_control::DfsOwnerSpecialNodeKind::Socket,
            0,
        ),
        SpecialFileKind::BlockDevice { rdev } => (
            afs_protocol::node_control::DfsOwnerSpecialNodeKind::BlockDevice,
            rdev,
        ),
        SpecialFileKind::CharDevice { rdev } => (
            afs_protocol::node_control::DfsOwnerSpecialNodeKind::CharDevice,
            rdev,
        ),
    };
    afs_protocol::node_control::DfsOwnerSpecialNode {
        kind: kind.into(),
        rdev,
    }
}

fn attributes(inode: &InodeRecord, size: u64, blocks: u64) -> FileAttributes {
    let attrs = &inode.attributes;
    FileAttributes {
        kind: file_kind(inode.kind),
        size,
        blocks,
        mode: attrs.mode,
        uid: attrs.uid,
        gid: attrs.gid,
        nlink: attrs.nlink,
        atime: unix_ms(attrs.atime_unix_ms),
        mtime: unix_ms(attrs.mtime_unix_ms),
        ctime: unix_ms(attrs.ctime_unix_ms),
    }
}

fn file_attributes_from_owner_attr(
    attr: afs_protocol::node_control::DfsOwnerFileAttr,
) -> Result<FileAttributes> {
    let kind = match attr.kind {
        1 => FileKind::Regular,
        2 => FileKind::Directory,
        3 => FileKind::Symlink,
        _ => {
            return Err(invalid("DFS owner getattr returned unsupported file kind"));
        }
    };
    Ok(FileAttributes {
        kind,
        size: attr.size,
        blocks: attr.size.div_ceil(512),
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        nlink: attr.nlink,
        atime: unix_ms(attr.atime_unix_ms),
        mtime: unix_ms(attr.mtime_unix_ms),
        ctime: unix_ms(attr.ctime_unix_ms),
    })
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn unix_ms(value: u64) -> SystemTime {
    UNIX_EPOCH + std::time::Duration::from_millis(value)
}

fn system_time_ms(value: SystemTime) -> u64 {
    value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn allocated_blocks(
    layout: &LayoutRoot,
    logical_length: u64,
    frozen: Option<&DirtyExtentMap>,
    active: &DirtyExtentMap,
) -> Result<u64> {
    let mut ranges = Vec::new();
    for extent in &layout.inline_extents {
        push_allocated_range(
            &mut ranges,
            extent.file_offset,
            extent.length,
            logical_length,
        )?;
    }
    if let Some(frozen) = frozen {
        push_dirty_allocated_ranges(&mut ranges, frozen, logical_length)?;
    }
    push_dirty_allocated_ranges(&mut ranges, active, logical_length)?;
    ranges_to_blocks(ranges)
}

fn push_dirty_allocated_ranges(
    ranges: &mut Vec<(u64, u64)>,
    map: &DirtyExtentMap,
    logical_length: u64,
) -> Result<()> {
    for extent in map.iter_extents() {
        if extent.data.is_some() || extent.builder.is_some() || extent.native_upload.is_some() {
            push_allocated_range(ranges, extent.file_offset, extent.length, logical_length)?;
        }
    }
    Ok(())
}

fn push_allocated_range(
    ranges: &mut Vec<(u64, u64)>,
    offset: u64,
    length: u64,
    logical_length: u64,
) -> Result<()> {
    if offset >= logical_length || length == 0 {
        return Ok(());
    }
    let end = offset
        .checked_add(length)
        .ok_or_else(|| invalid("allocated range overflow"))?
        .min(logical_length);
    if end > offset {
        ranges.push((offset, end));
    }
    Ok(())
}

fn ranges_to_blocks(mut ranges: Vec<(u64, u64)>) -> Result<u64> {
    if ranges.is_empty() {
        return Ok(0);
    }
    ranges.sort_by_key(|range| range.0);
    let mut total = 0u64;
    let (mut start, mut end) = ranges[0];
    for (next_start, next_end) in ranges.into_iter().skip(1) {
        if next_start <= end {
            end = end.max(next_end);
        } else {
            total = total
                .checked_add(end - start)
                .ok_or_else(|| invalid("allocated byte count overflow"))?;
            start = next_start;
            end = next_end;
        }
    }
    total = total
        .checked_add(end - start)
        .ok_or_else(|| invalid("allocated byte count overflow"))?;
    Ok(total.div_ceil(512))
}

fn caller_context(ctx: &RequestContext) -> CallerContext {
    CallerContext {
        uid: ctx.uid,
        gid: ctx.gid,
        supplementary_gids: ctx.supplementary_gids.clone(),
    }
}

fn slice_directory_entries(
    mut rows: Vec<DirectoryEntry>,
    cookie: u64,
    max_entries: usize,
) -> Result<Vec<DirectoryEntry>> {
    let start = usize::try_from(cookie).map_err(|_| invalid("directory cookie is too large"))?;
    rows.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(rows
        .into_iter()
        .enumerate()
        .skip(start)
        .take(max_entries)
        .map(|(index, mut entry)| {
            entry.next_cookie = u64::try_from(index + 1).unwrap_or(u64::MAX);
            entry
        })
        .collect())
}

fn rename_mode(flags: RenameFlags) -> Result<RenameMode> {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        if flags.0 == 0 {
            return Ok(RenameMode::Replace);
        }
        if flags.0 == libc::RENAME_NOREPLACE {
            return Ok(RenameMode::NoReplace);
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        if flags.0 == 0 {
            return Ok(RenameMode::Replace);
        }
    }
    Err(Error::coded(
        afs_error::NODE_VFS_UNIMPLEMENTED,
        "DFS rename supports only default replace and RENAME_NOREPLACE",
    ))
}

fn xattr_set_mode(flags: i32) -> Result<XattrSetMode> {
    if flags == 0 {
        return Ok(XattrSetMode::Upsert);
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        if flags == libc::XATTR_CREATE {
            return Ok(XattrSetMode::Create);
        }
        if flags == libc::XATTR_REPLACE {
            return Ok(XattrSetMode::Replace);
        }
    }
    Err(invalid("invalid DFS xattr set flags"))
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_VFS_INVALID, message)
}

fn lock_error(error: LockError) -> Error {
    Error::from(std::io::Error::from_raw_os_error(error.errno()))
}

fn file_too_large() -> Error {
    Error::from(std::io::Error::from_raw_os_error(libc::EFBIG))
}

fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_VFS_UNAVAILABLE, message)
}

fn is_definite_commit_rejection(error: &Error) -> bool {
    matches!(
        error.code(),
        afs_error::META_CATALOG_INVALID_REQUEST
            | afs_error::META_DFS_CONFLICT
            | afs_error::META_DFS_GC_CHANGED
    )
}

fn stale(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_DFS_STALE_HANDLE, message)
}

fn bad_file_descriptor(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_BAD_FILE_DESCRIPTOR, message)
}

#[cfg(test)]
mod tests;
