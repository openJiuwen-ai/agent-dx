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

mod backend;
mod commit;
mod commit_prepare;
mod dirty_extents;
mod gc;
mod handles;
mod inode_map;
mod lock_authority;
mod lock_sessions;
mod lock_wire;
mod meta_port;
mod native_pipeline;
mod native_write;
mod owner_gateway;
mod read_view;
mod remote_client;
mod sharded;
mod util;
mod version_cache;
mod write_state;
mod writeback;

use commit::*;
#[cfg(test)]
use commit_prepare::CommitPlanner;
use dirty_extents::*;
use handles::*;
use lock_authority::*;
pub use meta_port::{DfsMeta, DfsNodeLocation, DfsRemoteOwnerFactory};
pub(crate) use native_pipeline::NativeWriteConfig;
use native_pipeline::*;
use owner_gateway::*;
use remote_client::*;
use sharded::*;
use util::*;
pub(crate) use version_cache::version_cache_minimum_bytes;
use version_cache::*;
pub use write_state::DfsWriteSession;
use write_state::*;

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

const DFS_HANDLE_SHARDS: usize = 64;

const MAX_REMOTE_OPERATION_RESULTS: usize = 4096;
const MAX_LOCK_AUTHORITIES: usize = 8192;
const MAX_REMOTE_LOCK_AUTHORITIES: usize = 8192;
const MAX_REMOTE_LOCK_WAITERS: usize = 4096;
const MAX_CLOSED_LOCK_SESSIONS: usize = 4096;
const REMOTE_LOCK_SESSION_PENDING_CLOSE: usize = 0;
const REMOTE_LOCK_SESSION_ACTIVE: usize = 1;

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

    pub fn with_remote_owner_factory(mut self, factory: Arc<dyn DfsRemoteOwnerFactory>) -> Self {
        self.remote_owner_factory = Some(factory);
        self
    }
}

impl Drop for DistributedFs {
    fn drop(&mut self) {
        self.lock_renewal.stop();
    }
}

#[cfg(test)]
mod tests;
