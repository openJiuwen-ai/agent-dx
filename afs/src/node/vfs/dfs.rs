//! DistributedFs POSIX backend for the first local R=1 vertical slice.
//!
//! Open handles are process-local identities. Mutable file contents live in an
//! inode-level dirty view shared by every handle in this mount for that inode. A
//! normal `write` changes that dirty view and the local visibility sequence;
//! explicit `fdatasync`/`fsync` turns the dirty view into immutable Chunk data
//! and asks Meta to publish a new FileVersion. Writable `flush` commits prior
//! writes before reporting close success; `release` only drops the handle.

mod async_read;

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

mod backend;
mod commit;
mod commit_prepare;
mod dirty_extents;
mod gc;
mod handles;
mod identity;
mod inode_map;
mod lock_authority;
mod lock_entry;
mod lock_proxy_client;
mod lock_proxy_owner;
mod lock_sessions;
mod lock_wire;
mod meta_port;
pub mod owner_api;
mod owner_gateway;
mod owner_op_table;
mod read_view;
mod remote_client;
mod sharded;
#[cfg(test)]
mod test_hooks;
mod util;
mod version_cache;
mod write_pipeline;
mod write_pipeline_ops;
mod write_state;
mod writeback;

use commit::*;
#[cfg(test)]
use commit_prepare::CommitPlanner;
use dirty_extents::*;
use gc::*;
use handles::*;
use identity::*;
use inode_map::*;
use lock_authority::*;
use lock_proxy_client::*;
pub use meta_port::{DfsMeta, DfsNodeLocation, DfsRemoteOwnerFactory};
use owner_api::RemoteDfsOwner;
use owner_gateway::*;
use owner_op_table::*;
use remote_client::*;
use sharded::*;
#[cfg(test)]
use test_hooks::*;
use util::*;
pub(crate) use version_cache::version_cache_minimum_bytes;
use version_cache::*;
pub(crate) use write_pipeline::WritePipelineConfig;
use write_pipeline::*;
pub use write_state::DfsWriteSession;
use write_state::*;

use super::{
    Backend, PageCacheIdentity, WriteAdmissionMode,
    locks::{LockError, LockRequest, LockTable, LockWaiterId, LockWaiterOutcome},
    types::{
        AttributeChange, BackendInode, CreatedFile, DirectoryEntry, DirectoryHandle, Entry,
        FileAttributes, FileHandle, FileKind, FileLockConflict, FileLockKind, FileLockOwner,
        FileLockRange, FileLockType, FilesystemCapacity, OpenOptions, ReleaseKind, RenameFlags,
        RequestContext, SetAttrOptions, SpecialFileKind, SyncMode, SyncReceipt, WriteOptions,
    },
};
use crate::{
    dfs::{
        CallerContext, ChunkObject, ChunkReceipt, CommitFileVersion, CommitMetadataDelta,
        CommitMetadataMode, DfsWriteSessionId, DirEntryRecord, DirPage, Extent, FileVersion,
        FileVersionId, GetXattrRequest, InodeAttrUpdate, InodeAttributes, InodeId, InodeKind,
        InodeRecord, LayoutRoot, LayoutRootId, ListXattrRequest, MknodRequest, NamespaceId,
        OperationId, ReadLinkRequest, RemoveXattrRequest, RenameMode, RenameOutcome,
        SetInodeAttrRequest, SetXattrRequest, SpecialNodeKind, SymlinkRequest, SyncInodeMetadata,
        WriteLease, XattrSetMode,
    },
    node::chunk::{
        BodyRelease, ChunkBuilder, ChunkSink, DirtyAdmission, DirtyBudget, DirtyOwner, PutIntent,
        SharedChunkBytes, StagedChunk, StoredChunk, WriteBufferPool,
    },
    node::read::engine::{
        BoundedLru, ChunkReadOp, CurrentExternalReadOutcome, DfsReadEngine,
        NativeReadAuthoritySnapshot, ReadBatch,
    },
};

pub const DEFAULT_DIRTY_DATA_BUDGET_BYTES: u64 = 16 * 1024 * 1024;
const ROOT_INODE: u64 = 1;
const DEFAULT_VERSION_CACHE_ENTRIES: usize = 4096;
const DEFAULT_VERSION_CACHE_BYTES: usize = 32 * 1024 * 1024;
// How long a captured Node read-authority snapshot is reused before it is
// captured again. Renewal is local and never moves a handle's version.
const NATIVE_READ_VIEW_TTL: Duration = Duration::from_secs(5);
/// Entries the Node asks Meta for per ReadDir page; Meta caps it too.
const READ_DIR_PAGE_ENTRIES: u32 = 1024;
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
    identity: Arc<NodeIdentity>,
    inode_map: InodeMap,
    versions: VersionCache,
    gc: GcCoordinator,
    writes: InodeWriteTable,
    write_resources: WriteResources,
    locks: LockService,
    #[cfg(test)]
    hooks: TestHooks,
    meta: Arc<dyn DfsMeta>,
    chunk_store: Arc<dyn ChunkSink>,
    read_engine: Arc<DfsReadEngine>,
    handles: DfsHandleTable,
    /// Serves proxied RPCs for inodes this node session owns.
    owner_gateway: OwnerGateway,
    /// Proxies writes for inodes another node session owns.
    write_coordinator: WriteCoordinator,
}

const DFS_HANDLE_SHARDS: usize = 64;

/// Process-wide cap on owner replay slots, checked before an operation
/// applies: room for 1024 owner handles at the per-handle limit of 64.
#[cfg(not(test))]
const MAX_REMOTE_OPERATION_RESULTS: usize = 64 * 1024;
/// Unit tests reach the cap with 64 full handles instead of 1024.
#[cfg(test)]
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
        chunk_store: Arc<dyn ChunkSink>,
        read_engine: Arc<DfsReadEngine>,
    ) -> Self {
        let node_id = node_id.into();
        let session_id = session_id.into();
        let identity = Arc::new(NodeIdentity::new(namespace_id, node_id, session_id));
        Self {
            owner_gateway: OwnerGateway::new(identity.clone(), meta.clone()),
            write_coordinator: WriteCoordinator::new(identity.clone(), meta.clone()),
            identity,
            inode_map: InodeMap::default(),
            versions: VersionCache::default(),
            gc: GcCoordinator::default(),
            writes: InodeWriteTable::default(),
            write_resources: WriteResources::default(),
            locks: LockService::default(),
            #[cfg(test)]
            hooks: TestHooks::default(),
            meta,
            chunk_store,
            read_engine,
            handles: DfsHandleTable::default(),
        }
    }

    pub fn with_gc_references(
        mut self,
        references: Option<Arc<crate::node::gc::GcReferences>>,
    ) -> Self {
        self.gc.references = references;
        self
    }

    /// Register inode write-state lock contention counters with the Node registry.
    pub(crate) fn with_inode_lock_metrics(
        mut self,
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        self.writes.lock_wait = Some(Arc::new(InodeLockWait::register(registry)?));
        Ok(self)
    }

    pub(crate) fn with_write_pipeline(
        mut self,
        budget: Arc<DirtyBudget>,
        config: WritePipelineConfig,
        runtime: tokio::runtime::Handle,
        metrics: Option<crate::node::metrics::work::ReadWorkMetrics>,
    ) -> Result<Self> {
        if config.unpublished_descriptors == 0
            || config.unpublished_descriptors > 1024
            || config.upload_inflight_chunks == 0
            || config.upload_inflight_chunks > 8
        {
            return Err(invalid("invalid native write pipeline configuration"));
        }
        self.write_resources.pipeline = Some(Arc::new(WritePipeline::new(
            budget, config, runtime, metrics,
        )));
        self.observe_pipeline_resources();
        Ok(self)
    }

    pub(crate) fn with_version_cache_capacity(self, capacity: usize) -> Self {
        self.versions.set_capacity(capacity);
        self
    }

    pub(crate) fn with_version_cache_bytes(self, bytes: u64) -> Self {
        self.versions.set_byte_limit(bytes);
        self
    }

    pub fn with_write_buffer_pool(mut self, pool: Arc<WriteBufferPool>) -> Self {
        self.write_resources.buffer_pool = pool;
        self
    }

    pub fn with_remote_owner_factory(mut self, factory: Arc<dyn DfsRemoteOwnerFactory>) -> Self {
        self.write_coordinator.channel.remote_owner_factory = Some(factory);
        self
    }
}

impl Drop for DistributedFs {
    fn drop(&mut self) {
        self.locks.renewal.stop();
    }
}

#[cfg(test)]
mod tests;
