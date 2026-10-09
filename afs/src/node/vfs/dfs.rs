//! DistributedFs POSIX backend for the first local R=1 vertical slice.
//!
//! Open handles are process-local identities. Mutable file contents live in an
//! inode-level dirty view shared by every handle in this mount for that inode. A
//! normal `write` changes that dirty view and the local visibility sequence;
//! explicit `fdatasync`/`fsync` turns the dirty view into immutable Chunk data
//! and asks Meta to publish a new FileVersion. Writable `flush` commits prior
//! writes before reporting close success; `release` only drops the handle.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    ffi::{OsStr, OsString},
    os::unix::ffi::OsStringExt,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use afs_error::{Error, Result};

mod pending_trace;
use pending_trace::PendingTrace;

use super::{
    Backend,
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
        CallerContext, CommitFileVersion, CommitMetadataDelta, CommitMetadataMode, DentryRecord,
        DfsWriteSessionId, Extent, FileVersion, FileVersionId, GetXattrRequest, InodeAttrUpdate,
        InodeAttributes, InodeId, InodeKind, InodeRecord, LayoutRoot, LayoutRootId,
        ListXattrRequest, MknodRequest, NamespaceId, OperationId, ReadLinkRequest,
        RemoveXattrRequest, RenameMode, RenameOutcome, SetInodeAttrRequest, SetXattrRequest,
        SpecialNodeKind, SymlinkRequest, SyncInodeMetadata, WriteLease, XattrSetMode,
    },
    node::chunk::{ChunkBuilder, ChunkStore, StagedChunk},
    node::dfs_read::{ChunkReadOp, DfsReadEngine, ReadBatch},
    node::rpc::peer::RemoteDfsOwner,
};

pub const DFS_WRITE_LEASE_SECONDS: u64 = 30;
pub const DEFAULT_DIRTY_DATA_BUDGET_BYTES: u64 = 16 * 1024 * 1024;
const ROOT_INODE: u64 = 1;
const COMMIT_CHUNK_BYTES: u64 = crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64;
const MAX_PENDING_REMOTE_RELEASES: usize = 1024;
const REMOTE_RELEASE_MAINTENANCE_BUDGET: std::time::Duration =
    std::time::Duration::from_millis(250);
const REMOTE_RELEASE_SHUTDOWN_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);
const REMOTE_PROVIDER_IDLE_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);
const REMOTE_RELEASE_RPC_BUDGET: std::time::Duration = std::time::Duration::from_millis(100);
const REMOTE_RELEASE_RETRY_MAX_OPS: usize = 64;
const WRITEBACK_MAINTENANCE_BUDGET: std::time::Duration = std::time::Duration::from_millis(250);
const WRITEBACK_MAINTENANCE_MAX_OPS: usize = 64;
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
    fn get_file_version(&self, version_id: &FileVersionId) -> Result<(FileVersion, LayoutRoot)>;
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
}

pub struct DistributedFs {
    namespace_id: NamespaceId,
    node_id: String,
    session_id: String,
    pending_trace: Option<PendingTrace>,
    meta: Arc<dyn DfsMeta>,
    chunk_store: Arc<dyn ChunkStore>,
    read_engine: Arc<DfsReadEngine>,
    remote_owner_factory: Option<Arc<dyn DfsRemoteOwnerFactory>>,
    handles: Mutex<HashMap<u64, DfsFileHandle>>,
    dir_handles: Mutex<HashMap<u64, DfsDirectoryHandle>>,
    inode_writes: Mutex<HashMap<InodeId, SharedInodeWriteState>>,
    remote_inode_providers: Mutex<HashMap<InodeId, RemoteDfsWriteSession>>,
    pending_remote_releases: Mutex<PendingRemoteReleaseState>,
    lock_authorities: Mutex<HashMap<InodeId, Arc<DfsLockAuthority>>>,
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
    lock_session_reap_cursor: AtomicU64,
    owner_handle_reap_cursor: AtomicU64,
    writeback_cursor: AtomicU64,
    writeback_maintenance_gate: Mutex<()>,
    #[cfg(test)]
    writeback_after_examine_pause: Mutex<Option<Duration>>,
    #[cfg(test)]
    writeback_after_prepare_pause: Mutex<Option<Duration>>,
    closed_lock_sessions: Mutex<HashSet<String>>,
    closed_lock_session_admission_closed: AtomicBool,
    remote_operation_results: Mutex<HashMap<RemoteOperationKey, RemoteOperationResult>>,
    readonly_version_cache: Mutex<Option<ReadOnlyVersionCache>>,
    dirty_budget_bytes: u64,
    inode_to_backend: Mutex<HashMap<InodeId, u64>>,
    backend_to_inode: Mutex<HashMap<u64, InodeId>>,
    next_inode: AtomicU64,
    next_handle: AtomicU64,
    next_operation: AtomicU64,
    next_owner_open_seq: AtomicU64,
}

#[derive(Clone)]
struct ReadOnlyVersionCache {
    version_id: FileVersionId,
    version: FileVersion,
    layout: LayoutRoot,
}

struct DfsFileHandle {
    inode_id: InodeId,
    opened_inode: InodeRecord,
    write_session: Option<DfsRemoteWriteSession>,
    flags: i32,
    owner_scope: Option<DfsOwnerHandleScope>,
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

#[derive(Clone)]
struct ObservedWriteError {
    cursor: u64,
    error: Error,
}

struct InodeOperationGuard {
    cell: SharedInodeWriteState,
}

impl Drop for InodeOperationGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.cell.lock() {
            state.operation_busy = false;
            self.cell.notify_all();
        }
    }
}

#[derive(Clone)]
struct DirtyExtent {
    file_offset: u64,
    length: u64,
    write_seq: u64,
    data: Option<Arc<[u8]>>,
}

#[derive(Clone, Default)]
struct DirtyExtentMap {
    extents: Vec<DirtyExtent>,
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

#[derive(Clone)]
enum InFlightCommit {
    Preparing(Box<FrozenCommit>),
    File(Box<PendingFileCommit>),
    Metadata(SyncInodeMetadata),
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
    data: Option<(Arc<[u8]>, usize)>,
}

#[derive(Clone)]
struct CommitBatch {
    through_seq: u64,
    commit: CommitFileVersion,
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
        updated: InodeRecord,
    },
}

#[derive(Clone, Copy)]
enum CommitReason {
    DataSync,
    DirtyBudget,
    FullSync,
    Close,
    Background,
    LastWriter,
    NodeDrain,
}

impl InFlightCommit {
    fn dirty_extents(&self) -> Option<DirtyExtentMap> {
        match self {
            InFlightCommit::Preparing(frozen) => Some(frozen.dirty_extents.clone()),
            InFlightCommit::File(pending) => Some(pending.frozen.dirty_extents.clone()),
            InFlightCommit::Metadata(_) => None,
        }
    }
}

impl InodeWriteStateCell {
    fn new(state: InodeWriteState) -> Self {
        Self {
            state: Mutex::new(state),
            cv: Condvar::new(),
        }
    }

    fn lock(&self) -> std::sync::LockResult<std::sync::MutexGuard<'_, InodeWriteState>> {
        self.state.lock()
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
        #[cfg(not(test))]
        let pending_trace = PendingTrace::from_env(&namespace_id.0, &node_id, &session_id);
        #[cfg(test)]
        let pending_trace = None;
        Self {
            namespace_id,
            node_id,
            session_id,
            pending_trace,
            meta,
            chunk_store,
            read_engine,
            remote_owner_factory: None,
            handles: Mutex::new(HashMap::new()),
            dir_handles: Mutex::new(HashMap::new()),
            inode_writes: Mutex::new(HashMap::new()),
            remote_inode_providers: Mutex::new(HashMap::new()),
            pending_remote_releases: Mutex::new(PendingRemoteReleaseState::default()),
            lock_authorities: Mutex::new(HashMap::new()),
            lock_renewal: Arc::new(DfsLockRenewal::new()),
            remote_lock_authorities: Mutex::new(HashMap::new()),
            remote_lock_authority_sessions: Mutex::new(HashMap::new()),
            remote_lock_waiters: Mutex::new(HashMap::new()),
            cancelled_remote_lock_waiters: Mutex::new(HashSet::new()),
            owner_open_routes: Mutex::new(HashMap::new()),
            remote_open_admission_locks: Mutex::new(HashMap::new()),
            #[cfg(test)]
            owner_open_finish_pause: Mutex::new(None),
            lock_session_reap_cursor: AtomicU64::new(0),
            owner_handle_reap_cursor: AtomicU64::new(0),
            writeback_cursor: AtomicU64::new(0),
            writeback_maintenance_gate: Mutex::new(()),
            #[cfg(test)]
            writeback_after_examine_pause: Mutex::new(None),
            #[cfg(test)]
            writeback_after_prepare_pause: Mutex::new(None),
            closed_lock_sessions: Mutex::new(HashSet::new()),
            closed_lock_session_admission_closed: AtomicBool::new(false),
            remote_operation_results: Mutex::new(HashMap::new()),
            readonly_version_cache: Mutex::new(None),
            dirty_budget_bytes: DEFAULT_DIRTY_DATA_BUDGET_BYTES,
            inode_to_backend: Mutex::new(HashMap::from([(InodeId::new("1"), ROOT_INODE)])),
            backend_to_inode: Mutex::new(HashMap::from([(ROOT_INODE, InodeId::new("1"))])),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
            next_handle: AtomicU64::new(1),
            next_operation: AtomicU64::new(1),
            next_owner_open_seq: AtomicU64::new(1),
        }
    }

    pub fn with_dirty_budget_bytes(mut self, budget: u64) -> Self {
        self.dirty_budget_bytes = budget.max(1);
        self
    }

    fn operation_id(&self, prefix: &str) -> OperationId {
        OperationId::new(format!(
            "{}-{prefix}-{}",
            self.session_id,
            self.next_operation.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn validate_inode(&self, inode: InodeRecord) -> Result<InodeRecord> {
        if inode.namespace_id != self.namespace_id {
            return Err(invalid("Meta returned an inode from another DFS namespace"));
        }
        Ok(inode)
    }

    fn inode_size_and_blocks(&self, inode: &InodeRecord) -> Result<(u64, u64)> {
        match inode.head_version.as_ref() {
            Some(version_id) => {
                let (version, layout) = self.meta.get_file_version(version_id)?;
                Ok((
                    version.length,
                    allocated_blocks(&layout, version.length, None, &DirtyExtentMap::default())?,
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
            .lock()
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .get(&inode.value)
            .cloned()
            .ok_or_else(|| stale("DFS mount inode is no longer known"))
    }

    fn backend_inode(&self, inode: &InodeId) -> Result<BackendInode> {
        if let Some(value) = self
            .inode_to_backend
            .lock()
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .get(inode)
            .copied()
        {
            return Ok(BackendInode { value });
        }
        let value = self.next_inode.fetch_add(1, Ordering::Relaxed);
        self.inode_to_backend
            .lock()
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .insert(inode.clone(), value);
        self.backend_to_inode
            .lock()
            .map_err(|_| unavailable("DFS inode table is poisoned"))?
            .insert(value, inode.clone());
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

    fn allocate_handle(&self, handle: DfsFileHandle) -> Result<FileHandle> {
        let id = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.handles
            .lock()
            .map_err(|_| unavailable("DFS handle table is poisoned"))?
            .insert(id, handle);
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
    ) -> Result<(Option<FileVersion>, LayoutRoot)> {
        // Safe only for the read-only no-write-state path: callers must first
        // fetch and validate the current inode head on every read. The cached
        // payload is immutable FileVersion/LayoutRoot data keyed by the exact
        // head ID, and the cache lock is not held across Meta RPCs.
        let Some(version_id) = version_id else {
            return self.load_version(None);
        };
        if let Some(cached) = self
            .readonly_version_cache
            .lock()
            .map_err(|_| unavailable("DFS readonly version cache is poisoned"))?
            .as_ref()
            .filter(|cached| &cached.version_id == version_id)
            .cloned()
        {
            return Ok((Some(cached.version), cached.layout));
        }

        let (version, layout) = self.meta.get_file_version(version_id)?;
        *self
            .readonly_version_cache
            .lock()
            .map_err(|_| unavailable("DFS readonly version cache is poisoned"))? =
            Some(ReadOnlyVersionCache {
                version_id: version_id.clone(),
                version: version.clone(),
                layout: layout.clone(),
            });
        Ok((Some(version), layout))
    }

    fn write_state(&self, inode_id: &InodeId) -> Result<Option<SharedInodeWriteState>> {
        Ok(self
            .inode_writes
            .lock()
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
            && !state.last_writer_background_requested
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
        let state = Arc::new(InodeWriteStateCell::new(InodeWriteState {
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
        }));
        self.inode_writes
            .lock()
            .map_err(|_| unavailable("DFS inode write table is poisoned"))?
            .entry(inode.inode_id)
            .or_insert(state);
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
        Ok(write_session(&state, open_flags, session_id))
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
        let mut handles = self
            .handles
            .lock()
            .map_err(|_| unavailable("DFS handle table is poisoned"))?;
        for handle in handles
            .values_mut()
            .filter(|handle| handle.inode_id == inode.inode_id)
        {
            handle.opened_inode = inode.clone();
        }
        Ok(inode)
    }

    fn read_visible(&self, inode_id: &InodeId, offset: u64, out: &mut [u8]) -> Result<usize> {
        // The base version and its layout must come from the same owner-state
        // snapshot. An open handle identifies an inode, not a lifetime version.
        let (committed, length, layout, frozen, active) =
            if let Some(state) = self.write_state(inode_id)? {
                let state = state
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                (
                    state
                        .base_version
                        .as_ref()
                        .map(|version| version.id.clone()),
                    state.logical_length,
                    state.base_layout.clone(),
                    state
                        .in_flight
                        .as_ref()
                        .and_then(InFlightCommit::dirty_extents),
                    state.dirty_extents.clone(),
                )
            } else {
                let inode = self.validate_inode(self.meta.get_inode(inode_id)?)?;
                let (version, layout) = self.load_readonly_version(inode.head_version.as_ref())?;
                (
                    version.as_ref().map(|version| version.id.clone()),
                    version.as_ref().map_or(0, |value| value.length),
                    layout,
                    None,
                    DirtyExtentMap::default(),
                )
            };
        if offset >= length || out.is_empty() {
            return Ok(0);
        }
        let count = usize::try_from((length - offset).min(out.len() as u64))
            .map_err(|_| invalid("read length is too large"))?;
        out[..count].fill(0);
        self.read_layout_range(committed.as_ref(), &layout, offset, &mut out[..count])?;
        if let Some(frozen) = frozen {
            frozen.overlay(offset, &mut out[..count])?;
        }
        active.overlay(offset, &mut out[..count])?;
        Ok(count)
    }

    fn read_layout_range(
        &self,
        committed: Option<&FileVersionId>,
        layout: &LayoutRoot,
        offset: u64,
        out: &mut [u8],
    ) -> Result<()> {
        let end = offset
            .checked_add(out.len() as u64)
            .ok_or_else(|| invalid("read range overflow"))?;
        let mut ops = Vec::new();
        for extent in &layout.inline_extents {
            let extent_end = extent
                .file_offset
                .checked_add(extent.length)
                .ok_or_else(|| invalid("extent range overflow"))?;
            let start = offset.max(extent.file_offset);
            let stop = end.min(extent_end);
            if start >= stop {
                continue;
            }
            let output_offset = usize::try_from(start - offset)
                .map_err(|_| invalid("read output offset is too large"))?;
            let length =
                usize::try_from(stop - start).map_err(|_| invalid("read length is too large"))?;
            let chunk_offset = extent
                .chunk_offset
                .checked_add(start - extent.file_offset)
                .ok_or_else(|| invalid("chunk read offset overflow"))?;
            ops.push(ChunkReadOp {
                chunk_id: extent.chunk_id.clone(),
                chunk_offset,
                length: length as u64,
                output_offset,
            });
        }
        self.read_engine.read_batch(
            &ReadBatch {
                file_version_id: committed.cloned(),
                layout_root_id: layout.id.clone(),
                ops,
            },
            out,
        )?;
        Ok(())
    }

    fn handle_snapshot(&self, handle: FileHandle) -> Result<DfsFileHandleSnapshot> {
        self.handles
            .lock()
            .map_err(|_| unavailable("DFS handle table is poisoned"))?
            .get(&handle.0)
            .map(DfsFileHandleSnapshot::from)
            .ok_or_else(|| stale("DFS file handle is no longer open"))
    }

    fn commit_handle(&self, handle: FileHandle, reason: CommitReason) -> Result<()> {
        self.observe_handle_error(handle)?;
        let snapshot = self.handle_snapshot(handle)?;
        let committed_seq = self.commit_inode(&snapshot.inode_id, reason)?;
        if let Some(committed_seq) = committed_seq {
            self.update_handles_after_commit(&snapshot.inode_id, committed_seq)?;
        }
        Ok(())
    }

    fn commit_inode(&self, inode_id: &InodeId, reason: CommitReason) -> Result<Option<u64>> {
        self.commit_inode_with_operation_gate(inode_id, reason, true, None)
    }

    fn commit_inode_inside_operation(
        &self,
        inode_id: &InodeId,
        reason: CommitReason,
    ) -> Result<Option<u64>> {
        self.commit_inode_with_operation_gate(inode_id, reason, false, None)
    }

    fn commit_inode_until(
        &self,
        inode_id: &InodeId,
        reason: CommitReason,
        deadline: Instant,
    ) -> Result<Option<u64>> {
        self.commit_inode_with_operation_gate(inode_id, reason, true, Some(deadline))
    }

    fn commit_inode_with_operation_gate(
        &self,
        inode_id: &InodeId,
        reason: CommitReason,
        wait_for_operation: bool,
        deadline: Option<Instant>,
    ) -> Result<Option<u64>> {
        let Some(cell) = self.write_state(inode_id)? else {
            return Ok(None);
        };
        enum CommitAction {
            Prepare(Box<FrozenCommit>),
            Send(InFlightCommit),
        }

        loop {
            let action = loop {
                let maintenance =
                    matches!(reason, CommitReason::Background | CommitReason::LastWriter);
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
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    return Ok(None);
                }
                if let Some(in_flight) = state.in_flight.clone() {
                    let pending = match in_flight {
                        InFlightCommit::Preparing(_) => {
                            if maintenance {
                                return Ok(None);
                            }
                            state = cell
                                .wait(state)
                                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                            drop(state);
                            continue;
                        }
                        InFlightCommit::File(pending) => InFlightCommit::File(pending),
                        InFlightCommit::Metadata(sync) => InFlightCommit::Metadata(sync),
                    };
                    state.commit_busy = true;
                    break CommitAction::Send(pending);
                } else {
                    if !(state.dirty
                        || matches!(reason, CommitReason::FullSync) && state.metadata_dirty
                        || state.kill_suidgid_dirty)
                    {
                        return Ok(None);
                    }
                    if should_renew(&state.write_lease) {
                        state.write_lease = if let Some(timeout) = remaining_until(deadline) {
                            if timeout.is_zero() {
                                return Ok(None);
                            }
                            self.meta.renew_write_lease_with_timeout(
                                state.write_lease.clone(),
                                timeout,
                            )?
                        } else {
                            self.meta.renew_write_lease(state.write_lease.clone())?
                        };
                    }
                    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                        return Ok(None);
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
                        let pending = InFlightCommit::Metadata(sync);
                        state.in_flight = Some(pending.clone());
                        state.commit_busy = true;
                        break CommitAction::Send(pending);
                    } else {
                        let frozen = FrozenCommit {
                            through_seq: state.visible_write_seq,
                            logical_length: state.logical_length,
                            inode: state.inode.clone(),
                            write_lease: state.write_lease.clone(),
                            base_version: state.base_version.clone(),
                            base_layout: state.base_layout.clone(),
                            dirty_extents: std::mem::take(&mut state.dirty_extents),
                            kill_suidgid_dirty: state.kill_suidgid_dirty,
                        };
                        state.in_flight = Some(InFlightCommit::Preparing(Box::new(frozen.clone())));
                        state.commit_busy = true;
                        state.dirty = false;
                        break CommitAction::Prepare(Box::new(frozen));
                    }
                }
            };

            let pending_reuse = matches!(&action, CommitAction::Send(_));
            let pending = match action {
                CommitAction::Send(pending) => pending,
                CommitAction::Prepare(frozen) => match self.prepare_commit(&frozen, reason) {
                    Ok(batch) => {
                        let pending = InFlightCommit::File(Box::new(PendingFileCommit {
                            frozen: *frozen,
                            batch,
                            trace_send_attempt: self
                                .pending_trace
                                .as_ref()
                                .map(|_| Arc::new(AtomicU64::new(0))),
                        }));
                        let mut state = cell
                            .lock()
                            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                        state.in_flight = Some(pending.clone());
                        #[cfg(test)]
                        self.pause_after_writeback_prepare();
                        pending
                    }
                    Err(error) => {
                        let mut state = cell
                            .lock()
                            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                        if let Some(InFlightCommit::Preparing(failed)) = state.in_flight.take() {
                            state.dirty_extents.restore_before(failed.dirty_extents);
                        }
                        state.dirty = !state.dirty_extents.is_empty();
                        state.commit_busy = false;
                        cell.notify_all();
                        return Err(error);
                    }
                },
            };

            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                let mut state = cell
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                state.commit_busy = false;
                cell.notify_all();
                return Ok(None);
            }
            let timeout = remaining_until(deadline);
            if timeout.is_some_and(|timeout| timeout.is_zero()) {
                let mut state = cell
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                state.commit_busy = false;
                cell.notify_all();
                return Ok(None);
            }
            let branch = if pending_reuse {
                "pending_reuse"
            } else {
                "new_prepare"
            };
            let committed = self.finish_commit(cell.clone(), pending, timeout, branch)?;
            if matches!(reason, CommitReason::FullSync) && self.needs_full_metadata_sync(&cell)? {
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
            || matches!(&pending, InFlightCommit::Metadata(sync) if sync.metadata_delta.kill_suidgid);
        let result = (|| match &pending {
            InFlightCommit::Preparing(_) => Err(unavailable("DFS inode commit is still preparing")),
            InFlightCommit::File(pending) => {
                let committed_full_metadata =
                    pending.batch.commit.metadata_delta.mode == CommitMetadataMode::Full;
                let version = pending.batch.commit.file_version.clone();
                let layout = pending.batch.commit.layout_root.clone();
                if let Some(trace) = &self.pending_trace
                    && let Ok(state) = cell.lock()
                {
                    trace.observe("dfs_pending_commit_send", branch, &state, pending, None);
                }
                let updated = self.validate_inode(if let Some(timeout) = timeout {
                    self.meta
                        .commit_file_version_with_timeout(pending.batch.commit.clone(), timeout)?
                } else {
                    self.meta
                        .commit_file_version(pending.batch.commit.clone())?
                })?;
                Ok(AppliedCommit::File {
                    through_seq: pending.batch.through_seq,
                    committed_full_metadata,
                    version,
                    layout,
                    updated,
                })
            }
            InFlightCommit::Metadata(sync) => {
                let updated = self.validate_inode(if let Some(timeout) = timeout {
                    self.meta
                        .sync_inode_metadata_with_timeout(sync.clone(), timeout)?
                } else {
                    self.meta.sync_inode_metadata(sync.clone())?
                })?;
                Ok(AppliedCommit::Metadata { updated })
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
                state.inode = updated;
                state.base_version = Some(version);
                state.base_layout = layout;
                state.dirty = !state.dirty_extents.is_empty();
                state.durable_write_seq = through_seq;
                state.committed_write_seq = through_seq;
                state.last_writer_background_requested = false;
                if committed_full_metadata
                    && state.dirty_extents.is_empty()
                    && state.visible_write_seq <= through_seq
                {
                    state.metadata_dirty = false;
                }
                if committed_kill_suidgid {
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
            Ok(AppliedCommit::Metadata { updated }) => {
                state.in_flight = None;
                state.inode = updated;
                state.metadata_dirty = false;
                state.kill_suidgid_dirty = false;
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
            while state.commit_busy || state.operation_busy {
                state = cell
                    .wait(state)
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            }
            if let Some(error) = state.terminal_error.clone() {
                return Err(error);
            }
            if state.in_flight.is_none() {
                state.operation_busy = true;
                drop(state);
                return Ok(InodeOperationGuard { cell });
            }
            drop(state);
            self.commit_inode(inode_id, CommitReason::Background)?;
        }
    }

    fn lock_for_mutation<'a>(
        &self,
        inode_id: &InodeId,
        cell: &'a SharedInodeWriteState,
    ) -> Result<std::sync::MutexGuard<'a, InodeWriteState>> {
        loop {
            let mut state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            while state.commit_busy || state.operation_busy {
                state = cell
                    .wait(state)
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            }
            if let Some(error) = state.terminal_error.clone() {
                return Err(error);
            }
            if state.in_flight.is_none() {
                return Ok(state);
            }
            drop(state);
            self.commit_inode(inode_id, CommitReason::Background)?;
        }
    }

    fn prepare_commit(&self, frozen: &FrozenCommit, reason: CommitReason) -> Result<CommitBatch> {
        let operation_id = self.operation_id(reason.operation_prefix());
        let generation = self.next_operation.fetch_add(1, Ordering::Relaxed);
        let plan = CommitPlanner.plan(
            frozen,
            operation_id.clone(),
            LayoutRootId::new(format!("{}-layout-{generation}", self.session_id)),
        )?;
        let receipts = self
            .chunk_store
            .put_batch(unique_staged_chunks(plan.staged_chunks)?)?;
        let now = now_unix_ms();
        let version = FileVersion {
            id: FileVersionId::new(format!("{}-version-{generation}", self.session_id)),
            inode_id: frozen.inode.inode_id.clone(),
            parent_version: frozen
                .base_version
                .as_ref()
                .map(|version| version.id.clone()),
            length: frozen.logical_length,
            layout_root: plan.layout_root.id.clone(),
            created_at_unix_ms: now,
        };
        Ok(CommitBatch {
            through_seq: frozen.through_seq,
            commit: CommitFileVersion {
                operation_id,
                inode_id: frozen.inode.inode_id.clone(),
                write_lease: frozen.write_lease.clone(),
                expected_inode_revision: frozen.inode.revision,
                expected_head_version: frozen
                    .base_version
                    .as_ref()
                    .map(|version| version.id.clone()),
                file_version: version,
                layout_root: plan.layout_root,
                chunk_receipts: receipts,
                metadata_delta: reason.metadata_delta(
                    frozen.inode.attributes.mtime_unix_ms,
                    frozen.inode.attributes.ctime_unix_ms,
                    frozen.kill_suidgid_dirty,
                ),
            },
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
        if state.open_writers == 0 && state.dirty {
            state.last_writer_background_requested = true;
        }
        Ok(state.visible_write_seq)
    }

    fn release_writer(&self, session: &DfsWriteSession) -> Result<()> {
        let Some(state) = self.write_state(&session.inode_id)? else {
            return Ok(());
        };
        let mut state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        state.open_writers = state.open_writers.saturating_sub(1);
        if state.open_writers == 0 && state.dirty {
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
            .lock()
            .map_err(|_| unavailable("DFS remote owner table is poisoned"))?
            .insert(inode.inode_id.clone(), remote.clone());
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
            .lock()
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
        let mut authorities = self
            .lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS lock authority table is poisoned"))?;
        let idle = authorities
            .iter()
            .filter_map(|(inode_id, authority)| {
                let idle = authority.table.is_idle().ok()?;
                (idle && Arc::strong_count(authority) <= 2)
                    .then(|| (inode_id.clone(), authority.clone()))
            })
            .collect::<Vec<_>>();
        for (inode_id, authority) in idle {
            let current_is_idle = authorities.get(&inode_id).is_some_and(|current| {
                Arc::ptr_eq(current, &authority)
                    && Arc::strong_count(current) <= 3
                    && current.table.is_idle().unwrap_or(false)
            });
            if current_is_idle {
                authorities.remove(&inode_id);
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
        self.reclaim_idle_lock_authorities()?;
        let mut authorities = self
            .lock_authorities
            .lock()
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
        if authorities.len() >= MAX_LOCK_AUTHORITIES {
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
            .lock()
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
        let authorities = self
            .lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS lock authority table is poisoned"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
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
        let authorities = self
            .lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS lock authority table is poisoned"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
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
        self.writeback_pending_with_budget(
            WRITEBACK_MAINTENANCE_BUDGET,
            WRITEBACK_MAINTENANCE_MAX_OPS,
        )
    }

    fn writeback_pending_with_budget(&self, budget: Duration, max_ops: usize) -> Result<usize> {
        let _guard = match self.writeback_maintenance_gate.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(0),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(unavailable("DFS writeback maintenance gate is poisoned"));
            }
        };
        let deadline = Instant::now() + budget;
        // Snapshot references only. No inode lock or network wait may be held
        // under the shared table: that would stall unrelated inode lookups.
        let mut states = self
            .inode_writes
            .lock()
            .map_err(|_| unavailable("DFS inode write table is poisoned"))?
            .iter()
            .map(|(inode_id, state)| (inode_id.clone(), state.clone()))
            .collect::<Vec<_>>();
        states.sort_by(|left, right| left.0.0.cmp(&right.0.0));
        if !states.is_empty() {
            let start = (self.writeback_cursor.load(Ordering::Relaxed) as usize) % states.len();
            states.rotate_left(start);
        }

        let mut examined = 0usize;
        let mut attempted = 0usize;
        let mut committed = 0usize;
        for (inode_id, cell) in states {
            if examined >= max_ops || Instant::now() >= deadline {
                break;
            }
            examined = examined.saturating_add(1);
            #[cfg(test)]
            self.pause_after_writeback_examine();
            let state = match cell.state.try_lock() {
                Ok(state) => state,
                Err(std::sync::TryLockError::WouldBlock) => continue,
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(unavailable("DFS inode write state is poisoned"));
                }
            };
            if !state.commit_busy
                && !state.operation_busy
                && (state.dirty
                    || state.metadata_dirty
                    || state.kill_suidgid_dirty
                    || state.in_flight.is_some())
            {
                let reason = if state.last_writer_background_requested {
                    CommitReason::LastWriter
                } else {
                    CommitReason::Background
                };
                drop(state);
                if attempted >= max_ops || Instant::now() >= deadline {
                    break;
                }
                attempted = attempted.saturating_add(1);
                match self.commit_inode_until(&inode_id, reason, deadline) {
                    Ok(Some(seq)) => {
                        self.update_handles_after_commit(&inode_id, seq)?;
                        committed += 1;
                    }
                    Ok(None) => {}
                    Err(error) => self.record_background_error(&inode_id, error)?,
                }
            }
        }
        if examined != 0 {
            self.writeback_cursor
                .fetch_add(examined as u64, Ordering::Relaxed);
        }
        Ok(committed)
    }

    #[cfg(test)]
    fn pause_after_writeback_examine(&self) {
        if let Some(duration) = *self.writeback_after_examine_pause.lock().unwrap() {
            std::thread::sleep(duration);
        }
    }

    #[cfg(test)]
    fn pause_after_writeback_prepare(&self) {
        if let Some(duration) = self.writeback_after_prepare_pause.lock().unwrap().take() {
            std::thread::sleep(duration);
        }
    }

    /// Best-effort graceful drain. It gives a clean Node shutdown a chance to
    /// publish dirty state, while an abrupt crash is still allowed to recover
    /// only the last committed FileVersion.
    pub fn drain(&self) -> Result<usize> {
        let inode_ids = self
            .inode_writes
            .lock()
            .map_err(|_| unavailable("DFS inode write table is poisoned"))?
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut committed = 0;
        for inode_id in inode_ids {
            if let Some(seq) = self.commit_inode(&inode_id, CommitReason::NodeDrain)? {
                self.update_handles_after_commit(&inode_id, seq)?;
                committed += 1;
            }
        }
        Ok(committed)
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
        state.background_error = Some(ObservedWriteError { cursor, error });
        Ok(())
    }
}

#[derive(Clone)]
struct DfsFileHandleSnapshot {
    inode_id: InodeId,
    opened_inode: InodeRecord,
    write_session: Option<DfsRemoteWriteSession>,
    flags: i32,
    owner_scope: Option<DfsOwnerHandleScope>,
}

impl From<&DfsFileHandle> for DfsFileHandleSnapshot {
    fn from(handle: &DfsFileHandle) -> Self {
        Self {
            inode_id: handle.inode_id.clone(),
            opened_inode: handle.opened_inode.clone(),
            write_session: handle.write_session.clone(),
            flags: handle.flags,
            owner_scope: handle.owner_scope.clone(),
        }
    }
}

impl DirtyExtentMap {
    fn write_at(&mut self, offset: u64, data: &[u8], write_seq: u64) -> Result<usize> {
        let _ = usize::try_from(offset).map_err(|_| invalid("write offset is too large"))?;
        if !data.is_empty() {
            self.extents.push(DirtyExtent {
                file_offset: offset,
                length: data.len() as u64,
                write_seq,
                data: Some(Arc::from(data)),
            });
        }
        Ok(data.len())
    }

    fn resize(&mut self, old_length: u64, new_length: u64, write_seq: u64) -> Result<()> {
        let (start, stop) = if new_length < old_length {
            (new_length, old_length)
        } else {
            (old_length, new_length)
        };
        if start < stop {
            self.extents.push(DirtyExtent {
                file_offset: start,
                length: stop - start,
                write_seq,
                data: None,
            });
        }
        Ok(())
    }

    fn overlay(&self, offset: u64, out: &mut [u8]) -> Result<()> {
        let end = offset
            .checked_add(out.len() as u64)
            .ok_or_else(|| invalid("dirty read range overflow"))?;
        for extent in &self.extents {
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
            match &extent.data {
                Some(data) => {
                    let data_start = usize::try_from(start - extent.file_offset)
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
        older.extents.append(&mut self.extents);
        older.extents.sort_by_key(|extent| extent.write_seq);
        self.extents = older.extents;
    }

    fn dirty_data_bytes(&self) -> u64 {
        self.extents
            .iter()
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

        let mut staged_chunks = Vec::new();
        let mut pending_start = None;
        let mut pending = Vec::new();
        for segment in overlay {
            let Some((bytes, source_offset)) = segment.data else {
                flush_pending(
                    &mut pending_start,
                    &mut pending,
                    &operation_id,
                    &mut extents,
                    &mut staged_chunks,
                );
                continue;
            };
            let mut source_offset = source_offset;
            let mut file_offset = segment.file_offset;
            let mut remaining = usize::try_from(segment.length)
                .map_err(|_| invalid("dirty data length is too large"))?;
            while remaining > 0 {
                let expected = pending_start.map(|start| start + pending.len() as u64);
                if expected.is_some_and(|expected| expected != file_offset)
                    || pending.len() == COMMIT_CHUNK_BYTES as usize
                {
                    flush_pending(
                        &mut pending_start,
                        &mut pending,
                        &operation_id,
                        &mut extents,
                        &mut staged_chunks,
                    );
                }
                let capacity = COMMIT_CHUNK_BYTES as usize - pending.len();
                let take = capacity.min(remaining);
                pending_start.get_or_insert(file_offset);
                pending.extend_from_slice(&bytes[source_offset..source_offset + take]);
                source_offset += take;
                file_offset += take as u64;
                remaining -= take;
            }
        }
        flush_pending(
            &mut pending_start,
            &mut pending,
            &operation_id,
            &mut extents,
            &mut staged_chunks,
        );
        extents.sort_by_key(|extent| extent.file_offset);
        Ok(CommitPlan {
            layout_root: LayoutRoot {
                id: layout_id,
                file_length: frozen.logical_length,
                inline_extents: extents,
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
    let mut output: Vec<OverlaySegment> = Vec::new();
    for dirty in &map.extents {
        if dirty.file_offset >= file_length || dirty.length == 0 {
            continue;
        }
        let dirty_end = dirty
            .file_offset
            .checked_add(dirty.length)
            .ok_or_else(|| invalid("dirty extent range overflow"))?
            .min(file_length);
        let mut next = Vec::with_capacity(output.len().saturating_add(1));
        for segment in output {
            let segment_end = segment.file_offset + segment.length;
            if dirty_end <= segment.file_offset || dirty.file_offset >= segment_end {
                next.push(segment);
                continue;
            }
            if dirty.file_offset > segment.file_offset {
                next.push(OverlaySegment {
                    file_offset: segment.file_offset,
                    length: dirty.file_offset - segment.file_offset,
                    data: segment.data.clone(),
                });
            }
            if dirty_end < segment_end {
                let data = match segment.data {
                    Some((bytes, source_offset)) => {
                        let delta = usize::try_from(dirty_end - segment.file_offset)
                            .map_err(|_| invalid("overlay source offset is too large"))?;
                        let source_offset = source_offset
                            .checked_add(delta)
                            .ok_or_else(|| invalid("overlay source offset overflow"))?;
                        Some((bytes, source_offset))
                    }
                    None => None,
                };
                next.push(OverlaySegment {
                    file_offset: dirty_end,
                    length: segment_end - dirty_end,
                    data,
                });
            }
        }
        next.push(OverlaySegment {
            file_offset: dirty.file_offset,
            length: dirty_end - dirty.file_offset,
            data: dirty.data.clone().map(|bytes| (bytes, 0)),
        });
        next.sort_by_key(|segment| segment.file_offset);
        output = next;
    }
    Ok(output)
}

fn flush_pending(
    pending_start: &mut Option<u64>,
    pending: &mut Vec<u8>,
    operation_id: &OperationId,
    extents: &mut Vec<Extent>,
    staged_chunks: &mut Vec<StagedChunk>,
) {
    let Some(file_offset) = pending_start.take() else {
        return;
    };
    if pending.is_empty() {
        return;
    }
    let mut builder = ChunkBuilder::default();
    builder.replace(std::mem::take(pending));
    let staged = builder.stage(operation_id.clone());
    extents.push(Extent {
        file_offset,
        length: staged.chunk.length,
        chunk_id: staged.chunk.id.clone(),
        chunk_offset: 0,
    });
    staged_chunks.push(staged);
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
            .lock()
            .map_err(|_| unavailable("DFS remote owner table is poisoned"))
            .map(|providers| providers.get(inode_id).cloned())
    }

    fn replacement_remote_provider(
        &self,
        inode_id: &InodeId,
        released: &RemoteDfsWriteSession,
        preferred: Option<&RemoteDfsWriteSession>,
    ) -> Result<Option<RemoteDfsWriteSession>> {
        let handles = self
            .handles
            .lock()
            .map_err(|_| unavailable("DFS handle table is poisoned"))?;
        if let Some(preferred) = preferred
            && !Self::same_remote_provider(preferred, released)
            && preferred.lifecycle.is_live()?
            && handles.values().any(|handle| {
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
        for handle in handles
            .values()
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
            .lock()
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
    fn operation_prefix(self) -> &'static str {
        match self {
            Self::DataSync => "fdatasync",
            Self::DirtyBudget => "dirty-budget",
            Self::FullSync => "fsync",
            Self::Close => "close",
            Self::Background => "background",
            Self::LastWriter => "last-writer",
            Self::NodeDrain => "node-drain",
        }
    }

    fn metadata_delta(
        self,
        mtime_unix_ms: u64,
        ctime_unix_ms: u64,
        kill_suidgid: bool,
    ) -> CommitMetadataDelta {
        if !matches!(self, Self::DataSync | Self::DirtyBudget | Self::Close) {
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
        let authorities = self
            .lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS lock authority table is poisoned"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
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
        let authorities = self
            .lock_authorities
            .lock()
            .map_err(|_| unavailable("DFS lock authority table is poisoned"))?
            .values()
            .cloned()
            .collect::<Vec<_>>();
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
            if let Some(remote) = self.remote_provider_visible_for_reads(&snapshot.inode_id)? {
                return self.remote_getattr(&remote);
            }
            let record = self.observe_inode_record(self.meta.get_inode(&snapshot.inode_id)?)?;
            return self.visible_attributes_with_observed_inode(&snapshot.opened_inode, &record);
        }
        let inode_id = self.inode_id(inode)?;
        if let Some(remote) = self.remote_provider_visible_for_reads(&inode_id)? {
            return self.remote_getattr(&remote);
        }
        let record = self.observe_inode_record(self.meta.get_inode(&inode_id)?)?;
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
            self.commit_inode(&inode_id, CommitReason::DataSync)?;
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
                    self.commit_inode(&inode_id, CommitReason::DataSync)?;
                    let updated =
                        self.refresh_inode_record(self.meta.get_inode(&inode_id)?, true, true)?;
                    return self.visible_attributes(&updated);
                }
                return self.visible_attributes(&updated);
            }
        }

        let Some(length) = change.size else {
            if killpriv_setattr {
                self.commit_inode(&inode_id, CommitReason::DataSync)?;
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
            (snapshot.opened_inode, Some((handle, accepted_seq)))
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
            inode_id: inode.inode_id.clone(),
            opened_inode: inode.clone(),
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
        let preflight = self.validate_inode(self.meta.get_inode(&inode_id)?)?;
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
            let inode = self.observe_inode_record(self.meta.get_inode(&inode_id)?)?;
            (inode, None)
        };
        self.allocate_handle(DfsFileHandle {
            inode_id: opened_inode.inode_id.clone(),
            opened_inode,
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
        if let Some(remote) = self.remote_provider_visible_for_reads(&snapshot.inode_id)? {
            return self.remote_read(&remote, offset, out);
        }
        self.read_visible(&snapshot.inode_id, offset, out)
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
        let _operation = self.begin_inode_operation(&snapshot.inode_id, state.clone())?;
        let mut total_written = 0usize;
        let mut accepted_seq = None;
        while total_written < data.len() {
            let (written, seq, over_budget) = {
                let mut state = state
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                if let Some(error) = state.terminal_error.clone() {
                    return Err(error);
                }
                let dirty_bytes = state.dirty_extents.dirty_data_bytes();
                let budget_remaining = self.dirty_budget_bytes.saturating_sub(dirty_bytes);
                let take = (data.len() - total_written)
                    .min(usize::try_from(budget_remaining.max(1)).unwrap_or(usize::MAX));
                let chunk = &data[total_written..total_written + take];
                let file_offset = if snapshot.flags & libc::O_APPEND != 0 {
                    state.logical_length
                } else {
                    offset
                        .checked_add(total_written as u64)
                        .ok_or_else(file_too_large)?
                };
                let next_seq = state.next_write_seq.saturating_add(1);
                let written = state.dirty_extents.write_at(file_offset, chunk, next_seq)?;
                let end_offset = file_offset
                    .checked_add(written as u64)
                    .ok_or_else(file_too_large)?;
                state.logical_length = state.logical_length.max(end_offset);
                if written > 0 {
                    state.next_write_seq = next_seq;
                    state.visible_write_seq = state.next_write_seq;
                    state.dirty = true;
                    state.metadata_dirty = true;
                    let now = now_unix_ms();
                    state.inode.attributes.mtime_unix_ms = now;
                    state.inode.attributes.ctime_unix_ms = now;
                    if options.kill_suidgid {
                        Self::apply_kernel_killpriv_to_state(&mut state);
                    }
                }
                let over_budget = written > 0
                    && state.dirty_extents.dirty_data_bytes() >= self.dirty_budget_bytes;
                (written, state.visible_write_seq, over_budget)
            };
            if written == 0 {
                break;
            }
            total_written += written;
            accepted_seq = Some(seq);
            if over_budget {
                match self
                    .commit_inode_inside_operation(&snapshot.inode_id, CommitReason::DirtyBudget)
                {
                    Ok(Some(committed_seq)) => {
                        self.update_handles_after_commit(&snapshot.inode_id, committed_seq)?;
                        self.update_handle_write_progress(handle, seq)?;
                    }
                    Ok(None) => {
                        self.update_handle_write_progress(handle, seq)?;
                    }
                    Err(error)
                        if snapshot.flags & libc::O_SYNC == libc::O_SYNC
                            || has_o_dsync(snapshot.flags) =>
                    {
                        return Err(error);
                    }
                    Err(error) => {
                        self.update_handle_write_progress(handle, seq)?;
                        self.record_background_error(&snapshot.inode_id, error)?;
                        return Ok(total_written);
                    }
                }
            }
        }
        if let Some(seq) = accepted_seq {
            self.update_handle_write_progress(handle, seq)?;
        }
        if snapshot.flags & libc::O_SYNC == libc::O_SYNC {
            let committed_seq =
                self.commit_inode_inside_operation(&snapshot.inode_id, CommitReason::FullSync)?;
            if let Some(committed_seq) = committed_seq {
                self.update_handles_after_commit(&snapshot.inode_id, committed_seq)?;
            }
        } else if has_o_dsync(snapshot.flags) {
            let committed_seq =
                self.commit_inode_inside_operation(&snapshot.inode_id, CommitReason::DataSync)?;
            if let Some(committed_seq) = committed_seq {
                self.update_handles_after_commit(&snapshot.inode_id, committed_seq)?;
            }
        }
        Ok(total_written)
    }

    fn flush(&self, _: &RequestContext, handle: FileHandle) -> Result<()> {
        let snapshot = self.handle_snapshot(handle)?;
        if let Some(session) = snapshot.write_session.as_ref() {
            if session.remote.is_some() {
                self.remote_sync(session, SyncMode::DataOnly).map(|_| ())
            } else {
                self.commit_handle(handle, CommitReason::Close)
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
            self.commit_handle(
                handle,
                match mode {
                    SyncMode::DataOnly => CommitReason::DataSync,
                    SyncMode::Full => CommitReason::FullSync,
                },
            )
        }
    }

    fn release(&self, _: &RequestContext, handle: FileHandle) -> Result<()> {
        let removed = self
            .handles
            .lock()
            .map_err(|_| unavailable("DFS handle table is poisoned"))?
            .remove(&handle.0)
            .ok_or_else(|| stale("DFS file handle is no longer open"))?;
        if let Some(session) = removed.write_session.as_ref() {
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
            .insert(id, DfsDirectoryHandle { entries });
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

    fn getxattr(&self, ctx: &RequestContext, inode: BackendInode, name: &OsStr) -> Result<Vec<u8>> {
        self.meta.get_xattr(GetXattrRequest {
            caller: caller_context(ctx),
            inode_id: self.inode_id(inode)?,
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
        let mut handles = self
            .handles
            .lock()
            .map_err(|_| unavailable("DFS handle table is poisoned"))?;
        let file = handles
            .get_mut(&handle.0)
            .ok_or_else(|| stale("DFS file handle is no longer open"))?;
        if let Some(session) = file.write_session.as_mut() {
            session.local.last_accepted_seq = accepted_seq;
        }
        Ok(())
    }

    fn update_handle_error_cursor(&self, handle: FileHandle, cursor: u64) -> Result<()> {
        let mut handles = self
            .handles
            .lock()
            .map_err(|_| unavailable("DFS handle table is poisoned"))?;
        let file = handles
            .get_mut(&handle.0)
            .ok_or_else(|| stale("DFS file handle is no longer open"))?;
        if let Some(session) = file.write_session.as_mut() {
            session.local.error_cursor = cursor;
        }
        Ok(())
    }

    fn update_handles_after_commit(&self, inode_id: &InodeId, committed_seq: u64) -> Result<()> {
        let inode = self
            .write_state(inode_id)?
            .ok_or_else(|| stale("DFS write state is no longer open"))?
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?
            .inode
            .clone();
        let mut handles = self
            .handles
            .lock()
            .map_err(|_| unavailable("DFS handle table is poisoned"))?;
        for handle in handles
            .values_mut()
            .filter(|handle| handle.inode_id == *inode_id)
        {
            if let Some(session) = handle.write_session.as_mut() {
                handle.opened_inode = inode.clone();
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
                inode_id: inode.inode_id.clone(),
                opened_inode: inode.clone(),
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
        let mut data = vec![0; request.length as usize];
        let read = <Self as Backend>::read(
            self,
            &owner_request_context(),
            file,
            request.offset,
            &mut data,
        )?;
        data.truncate(read);
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
        let Some(snapshot) = self
            .handles
            .lock()
            .map_err(|_| unavailable("DFS handle table is poisoned"))?
            .get(&file.0)
            .map(DfsFileHandleSnapshot::from)
        else {
            return Ok(None);
        };
        self.validate_owner_handle_scope(peer, handle, &inode_id, &snapshot)?;
        Ok(Some(file))
    }

    fn peer_owner_handle_sessions(&self) -> Result<HashMap<String, HashSet<String>>> {
        let mut sessions: HashMap<String, HashSet<String>> = HashMap::new();
        for handle in self
            .handles
            .lock()
            .map_err(|_| unavailable("DFS handle table is poisoned"))?
            .values()
        {
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
            .lock()
            .map_err(|_| unavailable("DFS handle table is poisoned"))?
            .iter()
            .filter_map(|(id, handle)| {
                handle.owner_scope.as_ref().and_then(|scope| {
                    (scope.caller_node_id == node_id && scope.caller_session_id == session_id)
                        .then_some(FileHandle(*id))
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

fn remaining_until(deadline: Option<Instant>) -> Option<Duration> {
    deadline.map(|deadline| deadline.saturating_duration_since(Instant::now()))
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
    for extent in &map.extents {
        if extent.data.is_some() {
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
        afs_error::META_CATALOG_INVALID_REQUEST | afs_error::META_DFS_CONFLICT
    )
}

fn stale(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_DFS_STALE_HANDLE, message)
}

fn bad_file_descriptor(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_BAD_FILE_DESCRIPTOR, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::chunk::LocalChunkStore;
    use std::sync::atomic::AtomicUsize;

    struct CommitPause {
        after_successes: usize,
        reached: (Mutex<bool>, std::sync::Condvar),
        release: (Mutex<bool>, std::sync::Condvar),
    }

    impl CommitPause {
        fn new(after_successes: usize) -> Self {
            Self {
                after_successes,
                reached: (Mutex::new(false), std::sync::Condvar::new()),
                release: (Mutex::new(false), std::sync::Condvar::new()),
            }
        }

        fn wait_until_reached(&self) {
            let (lock, cv) = &self.reached;
            let mut reached = lock.lock().unwrap();
            while !*reached {
                reached = cv.wait(reached).unwrap();
            }
        }

        fn release(&self) {
            let (lock, cv) = &self.release;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }

        fn block(&self) {
            let (reached_lock, reached_cv) = &self.reached;
            *reached_lock.lock().unwrap() = true;
            reached_cv.notify_all();

            let (release_lock, release_cv) = &self.release;
            let mut released = release_lock.lock().unwrap();
            while !*released {
                released = release_cv.wait(released).unwrap();
            }
        }
    }

    struct RenewPause {
        reached: (Mutex<bool>, std::sync::Condvar),
        release: (Mutex<bool>, std::sync::Condvar),
    }

    impl RenewPause {
        fn new() -> Self {
            Self {
                reached: (Mutex::new(false), std::sync::Condvar::new()),
                release: (Mutex::new(false), std::sync::Condvar::new()),
            }
        }

        fn block(&self) {
            let (reached_lock, reached_cv) = &self.reached;
            *reached_lock.lock().unwrap() = true;
            reached_cv.notify_all();
            let (release_lock, release_cv) = &self.release;
            let mut release = release_lock.lock().unwrap();
            while !*release {
                release = release_cv.wait(release).unwrap();
            }
        }

        fn release(&self) {
            let (lock, cv) = &self.release;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }

        fn was_reached(&self) -> bool {
            *self.reached.0.lock().unwrap()
        }
    }

    #[cfg(feature = "dfs")]
    struct CountingStoreBackend {
        inner: Arc<dyn crate::meta::store::StoreBackend>,
        commits: AtomicU64,
    }

    #[cfg(feature = "dfs")]
    impl CountingStoreBackend {
        fn new(inner: Arc<dyn crate::meta::store::StoreBackend>) -> Self {
            Self {
                inner,
                commits: AtomicU64::new(0),
            }
        }

        fn commit_count(&self) -> u64 {
            self.commits.load(Ordering::SeqCst)
        }
    }

    #[cfg(feature = "dfs")]
    impl crate::meta::store::StoreBackend for CountingStoreBackend {
        fn health(&self) -> crate::meta::store::MetaFuture<'_, ()> {
            self.inner.health()
        }

        fn persistence(&self) -> crate::meta::store::BackendPersistence {
            self.inner.persistence()
        }

        fn load(&self) -> crate::meta::store::MetaFuture<'_, Option<(u64, Vec<u8>)>> {
            self.inner.load()
        }

        fn commit(
            &self,
            expected_version: u64,
            bytes: Vec<u8>,
        ) -> crate::meta::store::MetaFuture<'_, u64> {
            Box::pin(async move {
                let version = self.inner.commit(expected_version, bytes).await?;
                self.commits.fetch_add(1, Ordering::SeqCst);
                Ok(version)
            })
        }
    }

    struct RecordingMeta {
        inode: Mutex<InodeRecord>,
        lease: Mutex<WriteLease>,
        commits: Mutex<Vec<CommitFileVersion>>,
        failed_commits: Mutex<Vec<CommitFileVersion>>,
        cas_failed_commits: Mutex<Vec<CommitFileVersion>>,
        metadata_syncs: Mutex<Vec<SyncInodeMetadata>>,
        failed_metadata_syncs: Mutex<Vec<SyncInodeMetadata>>,
        next_commit_error: Mutex<Option<Error>>,
        commit_error_after_successes: Mutex<Option<(usize, Error)>>,
        commit_pause: Mutex<Option<Arc<CommitPause>>>,
        renew_pause: Mutex<Option<Arc<RenewPause>>>,
        next_metadata_sync_error: Mutex<Option<Error>>,
        next_renew_error: Mutex<Option<Error>>,
        next_open_write_error: Mutex<Option<Error>>,
        next_get_inode_error: Mutex<Option<Error>>,
        next_get_file_version_error: Mutex<Option<Error>>,
        next_get_inode_bad_id: Mutex<bool>,
        next_open_write_bad_namespace: Mutex<bool>,
        open_write_results: Mutex<std::collections::VecDeque<Result<(InodeRecord, WriteLease)>>>,
        renew_results: Mutex<std::collections::VecDeque<Result<WriteLease>>>,
        renew_timeouts: Mutex<Vec<Duration>>,
        metadata_timeouts: Mutex<Vec<Duration>>,
        commit_timeouts: Mutex<Vec<Duration>>,
        node_sessions: Mutex<HashMap<String, Option<String>>>,
        next_current_session_error: Mutex<Option<Error>>,
        current_session_calls: AtomicUsize,
        file_version_loads: AtomicUsize,
        renew_calls: AtomicUsize,
        open_write_calls: AtomicUsize,
        resolve_lock_calls: AtomicUsize,
        resolve_write_calls: AtomicUsize,
    }

    impl RecordingMeta {
        fn new() -> Self {
            Self {
                inode: Mutex::new(InodeRecord {
                    namespace_id: NamespaceId::new("default"),
                    inode_id: InodeId::new("inode:test"),
                    kind: InodeKind::Regular,
                    attributes: InodeAttributes {
                        mode: 0o640,
                        uid: 1000,
                        gid: 1000,
                        nlink: 1,
                        atime_unix_ms: 1,
                        mtime_unix_ms: 1,
                        ctime_unix_ms: 1,
                    },
                    head_version: None,
                    symlink_target: None,
                    xattrs: std::collections::BTreeMap::new(),
                    revision: 1,
                }),
                lease: Mutex::new(WriteLease {
                    inode_id: InodeId::new("inode:test"),
                    owner_node_id: "node-a".into(),
                    owner_session_id: "session-a".into(),
                    lease_epoch: 1,
                    expires_at_unix_ms: u64::MAX,
                }),
                commits: Mutex::new(Vec::new()),
                failed_commits: Mutex::new(Vec::new()),
                cas_failed_commits: Mutex::new(Vec::new()),
                metadata_syncs: Mutex::new(Vec::new()),
                failed_metadata_syncs: Mutex::new(Vec::new()),
                next_commit_error: Mutex::new(None),
                commit_error_after_successes: Mutex::new(None),
                commit_pause: Mutex::new(None),
                renew_pause: Mutex::new(None),
                next_metadata_sync_error: Mutex::new(None),
                next_renew_error: Mutex::new(None),
                next_open_write_error: Mutex::new(None),
                next_get_inode_error: Mutex::new(None),
                next_get_file_version_error: Mutex::new(None),
                next_get_inode_bad_id: Mutex::new(false),
                next_open_write_bad_namespace: Mutex::new(false),
                open_write_results: Mutex::new(std::collections::VecDeque::new()),
                renew_results: Mutex::new(std::collections::VecDeque::new()),
                renew_timeouts: Mutex::new(Vec::new()),
                metadata_timeouts: Mutex::new(Vec::new()),
                commit_timeouts: Mutex::new(Vec::new()),
                node_sessions: Mutex::new(HashMap::new()),
                next_current_session_error: Mutex::new(None),
                current_session_calls: AtomicUsize::new(0),
                file_version_loads: AtomicUsize::new(0),
                renew_calls: AtomicUsize::new(0),
                open_write_calls: AtomicUsize::new(0),
                resolve_lock_calls: AtomicUsize::new(0),
                resolve_write_calls: AtomicUsize::new(0),
            }
        }

        fn commit_count(&self) -> usize {
            self.commits.lock().unwrap().len()
        }

        fn advance_namespace_revision(&self) {
            let mut inode = self.inode.lock().unwrap();
            inode.revision = inode.revision.saturating_add(1);
        }

        fn fail_next_commit(&self) {
            self.fail_next_commit_with(unavailable("injected test commit failure"));
        }

        fn fail_next_metadata_sync(&self) {
            self.fail_next_metadata_sync_with(unavailable("injected test metadata sync failure"));
        }

        fn fail_next_commit_with(&self, error: Error) {
            *self.next_commit_error.lock().unwrap() = Some(error);
        }

        fn fail_commit_after_successes(&self, successes: usize, error: Error) {
            *self.commit_error_after_successes.lock().unwrap() = Some((successes, error));
        }

        fn pause_commit_after_successes(&self, successes: usize) -> Arc<CommitPause> {
            let pause = Arc::new(CommitPause::new(successes));
            *self.commit_pause.lock().unwrap() = Some(pause.clone());
            pause
        }

        fn pause_next_renew(&self) -> Arc<RenewPause> {
            let pause = Arc::new(RenewPause::new());
            *self.renew_pause.lock().unwrap() = Some(pause.clone());
            pause
        }

        fn fail_next_metadata_sync_with(&self, error: Error) {
            *self.next_metadata_sync_error.lock().unwrap() = Some(error);
        }

        fn fail_next_renew_with(&self, error: Error) {
            *self.next_renew_error.lock().unwrap() = Some(error);
        }

        fn fail_next_open_write_with(&self, error: Error) {
            *self.next_open_write_error.lock().unwrap() = Some(error);
        }

        fn fail_next_get_inode_with(&self, error: Error) {
            *self.next_get_inode_error.lock().unwrap() = Some(error);
        }

        fn fail_next_get_file_version_with(&self, error: Error) {
            *self.next_get_file_version_error.lock().unwrap() = Some(error);
        }

        fn file_version_load_count(&self) -> usize {
            self.file_version_loads.load(Ordering::SeqCst)
        }

        fn return_bad_inode_on_next_get_inode(&self) {
            *self.next_get_inode_bad_id.lock().unwrap() = true;
        }

        fn return_bad_namespace_on_next_open_write(&self) {
            *self.next_open_write_bad_namespace.lock().unwrap() = true;
        }

        fn queue_renew_result(&self, result: Result<WriteLease>) {
            self.renew_results.lock().unwrap().push_back(result);
        }

        fn queue_open_write_result(&self, result: Result<(InodeRecord, WriteLease)>) {
            self.open_write_results.lock().unwrap().push_back(result);
        }

        fn lease_with_expiry(&self, expires_at_unix_ms: u64) -> WriteLease {
            let mut lease = self.lease.lock().unwrap().clone();
            lease.expires_at_unix_ms = expires_at_unix_ms;
            lease
        }

        fn observed_write_authority(&self) -> Result<(InodeRecord, WriteLease)> {
            if let Some(error) = self.next_open_write_error.lock().unwrap().take() {
                return Err(error);
            }
            let mut inode = self.inode.lock().unwrap().clone();
            if *self.next_open_write_bad_namespace.lock().unwrap() {
                *self.next_open_write_bad_namespace.lock().unwrap() = false;
                inode.namespace_id = NamespaceId::new("wrong");
            }
            Ok((inode, self.lease.lock().unwrap().clone()))
        }

        fn renew_call_count(&self) -> usize {
            self.renew_calls.load(Ordering::SeqCst)
        }

        fn commit_timeouts(&self) -> Vec<Duration> {
            self.commit_timeouts.lock().unwrap().clone()
        }

        fn metadata_timeouts(&self) -> Vec<Duration> {
            self.metadata_timeouts.lock().unwrap().clone()
        }

        fn renew_timeouts(&self) -> Vec<Duration> {
            self.renew_timeouts.lock().unwrap().clone()
        }

        fn set_node_session(&self, node_id: &str, session_id: Option<&str>) {
            self.node_sessions
                .lock()
                .unwrap()
                .insert(node_id.to_owned(), session_id.map(ToOwned::to_owned));
        }

        fn fail_next_current_session_with(&self, error: Error) {
            *self.next_current_session_error.lock().unwrap() = Some(error);
        }

        fn current_session_call_count(&self) -> usize {
            self.current_session_calls.load(Ordering::SeqCst)
        }

        fn publish_external_version(&self, length: u64) -> FileVersionId {
            self.publish_external_version_with_receipts(length, Vec::new(), Vec::new())
        }

        fn publish_external_version_with_receipts(
            &self,
            length: u64,
            inline_extents: Vec<Extent>,
            chunk_receipts: Vec<crate::dfs::ChunkReceipt>,
        ) -> FileVersionId {
            let mut inode = self.inode.lock().unwrap();
            let generation = self.commits.lock().unwrap().len() + 1;
            let layout_id = LayoutRootId::new(format!("external-layout-{generation}"));
            let version_id = FileVersionId::new(format!("external-version-{generation}"));
            let now = now_unix_ms();
            let version = FileVersion {
                id: version_id.clone(),
                inode_id: inode.inode_id.clone(),
                parent_version: inode.head_version.clone(),
                length,
                layout_root: layout_id.clone(),
                created_at_unix_ms: now,
            };
            let commit = CommitFileVersion {
                operation_id: OperationId::new(format!("external-commit-{generation}")),
                inode_id: inode.inode_id.clone(),
                write_lease: self.lease.lock().unwrap().clone(),
                expected_inode_revision: inode.revision,
                expected_head_version: inode.head_version.clone(),
                file_version: version,
                layout_root: LayoutRoot {
                    id: layout_id,
                    file_length: length,
                    inline_extents,
                },
                chunk_receipts,
                metadata_delta: CommitMetadataDelta {
                    mode: CommitMetadataMode::Full,
                    mtime_unix_ms: Some(now),
                    ctime_unix_ms: Some(now),
                    kill_suidgid: false,
                },
            };
            inode.revision = inode.revision.saturating_add(1);
            inode.head_version = Some(version_id.clone());
            inode.attributes.mtime_unix_ms = now;
            inode.attributes.ctime_unix_ms = now;
            drop(inode);
            self.commits.lock().unwrap().push(commit);
            version_id
        }
    }

    impl DfsMeta for RecordingMeta {
        fn lookup(&self, _: &InodeId, _: &[u8]) -> Result<Option<InodeRecord>> {
            Ok(Some(self.inode.lock().unwrap().clone()))
        }

        fn create(
            &self,
            _: &OperationId,
            _: &InodeId,
            _: &[u8],
            _: InodeAttributes,
        ) -> Result<(InodeRecord, WriteLease)> {
            Ok((
                self.inode.lock().unwrap().clone(),
                self.lease.lock().unwrap().clone(),
            ))
        }

        fn get_inode(&self, _: &InodeId) -> Result<InodeRecord> {
            if let Some(error) = self.next_get_inode_error.lock().unwrap().take() {
                return Err(error);
            }
            let mut inode = self.inode.lock().unwrap().clone();
            if *self.next_get_inode_bad_id.lock().unwrap() {
                *self.next_get_inode_bad_id.lock().unwrap() = false;
                inode.inode_id = InodeId::new("inode:wrong");
            }
            Ok(inode)
        }

        fn get_file_version(
            &self,
            version_id: &FileVersionId,
        ) -> Result<(FileVersion, LayoutRoot)> {
            self.file_version_loads.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.next_get_file_version_error.lock().unwrap().take() {
                return Err(error);
            }
            self.commits
                .lock()
                .unwrap()
                .iter()
                .find(|commit| commit.file_version.id == *version_id)
                .map(|commit| (commit.file_version.clone(), commit.layout_root.clone()))
                .ok_or_else(|| invalid("test FileVersion not found"))
        }

        fn open_write(&self, _: &InodeId) -> Result<(InodeRecord, WriteLease)> {
            self.open_write_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(result) = self.open_write_results.lock().unwrap().pop_front() {
                return result;
            }
            self.observed_write_authority()
        }

        fn resolve_lock_authority(&self, _: &InodeId) -> Result<(InodeRecord, WriteLease)> {
            self.resolve_lock_calls.fetch_add(1, Ordering::SeqCst);
            self.observed_write_authority()
        }

        fn resolve_write_authority(&self, _: &InodeId) -> Result<(InodeRecord, WriteLease)> {
            self.resolve_write_calls.fetch_add(1, Ordering::SeqCst);
            self.observed_write_authority()
        }

        fn renew_write_lease(&self, lease: WriteLease) -> Result<WriteLease> {
            self.renew_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(pause) = self.renew_pause.lock().unwrap().take() {
                pause.block();
            }
            if let Some(result) = self.renew_results.lock().unwrap().pop_front() {
                return result;
            }
            if let Some(error) = self.next_renew_error.lock().unwrap().take() {
                return Err(error);
            }
            let mut stored = self.lease.lock().unwrap();
            stored.expires_at_unix_ms = stored.expires_at_unix_ms.max(lease.expires_at_unix_ms);
            Ok(stored.clone())
        }

        fn renew_write_lease_with_timeout(
            &self,
            lease: WriteLease,
            timeout: Duration,
        ) -> Result<WriteLease> {
            self.renew_timeouts.lock().unwrap().push(timeout);
            self.renew_write_lease(lease)
        }

        fn sync_inode_metadata(&self, sync: SyncInodeMetadata) -> Result<InodeRecord> {
            let next_error = self.next_metadata_sync_error.lock().unwrap().take();
            if let Some(error) = next_error {
                self.failed_metadata_syncs.lock().unwrap().push(sync);
                return Err(error);
            }
            let mut inode = self.inode.lock().unwrap();
            if inode.revision != sync.expected_inode_revision
                || inode.head_version != sync.expected_head_version
            {
                return Err(unavailable("test inode metadata CAS failed"));
            }
            inode.revision = inode.revision.saturating_add(1);
            inode.attributes.mtime_unix_ms = sync.metadata_delta.mtime_unix_ms.unwrap();
            inode.attributes.ctime_unix_ms = sync.metadata_delta.ctime_unix_ms.unwrap();
            if sync.metadata_delta.kill_suidgid {
                inode.attributes.mode =
                    DistributedFs::clear_kernel_write_privileges(inode.attributes.mode);
            }
            self.metadata_syncs.lock().unwrap().push(sync);
            Ok(inode.clone())
        }

        fn sync_inode_metadata_with_timeout(
            &self,
            sync: SyncInodeMetadata,
            timeout: Duration,
        ) -> Result<InodeRecord> {
            self.metadata_timeouts.lock().unwrap().push(timeout);
            self.sync_inode_metadata(sync)
        }

        fn commit_file_version(&self, commit: CommitFileVersion) -> Result<InodeRecord> {
            let next_error = self.next_commit_error.lock().unwrap().take();
            if let Some(error) = next_error {
                self.failed_commits.lock().unwrap().push(commit);
                return Err(error);
            }
            {
                let mut scheduled = self.commit_error_after_successes.lock().unwrap();
                let should_fail = scheduled
                    .as_ref()
                    .is_some_and(|(successes, _)| self.commits.lock().unwrap().len() >= *successes);
                if should_fail {
                    let (_, error) = scheduled.take().unwrap();
                    self.failed_commits.lock().unwrap().push(commit);
                    return Err(error);
                }
            }
            let pause = self.commit_pause.lock().unwrap().clone();
            if let Some(pause) = pause
                && self.commits.lock().unwrap().len() >= pause.after_successes
            {
                *self.commit_pause.lock().unwrap() = None;
                pause.block();
            }
            let mut inode = self.inode.lock().unwrap();
            if inode.revision != commit.expected_inode_revision
                || inode.head_version != commit.expected_head_version
            {
                self.cas_failed_commits.lock().unwrap().push(commit);
                return Err(unavailable("test inode CAS failed"));
            }
            inode.revision = inode.revision.saturating_add(1);
            inode.head_version = Some(commit.file_version.id.clone());
            if commit.metadata_delta.mode == CommitMetadataMode::Full {
                inode.attributes.mtime_unix_ms = commit.metadata_delta.mtime_unix_ms.unwrap();
                inode.attributes.ctime_unix_ms = commit.metadata_delta.ctime_unix_ms.unwrap();
            }
            if commit.metadata_delta.kill_suidgid {
                inode.attributes.mode =
                    DistributedFs::clear_kernel_write_privileges(inode.attributes.mode);
            }
            self.commits.lock().unwrap().push(commit);
            Ok(inode.clone())
        }

        fn commit_file_version_with_timeout(
            &self,
            commit: CommitFileVersion,
            timeout: Duration,
        ) -> Result<InodeRecord> {
            self.commit_timeouts.lock().unwrap().push(timeout);
            self.commit_file_version(commit)
        }

        fn mkdir(
            &self,
            _: &OperationId,
            _: &InodeId,
            _: &[u8],
            attributes: InodeAttributes,
            _: CallerContext,
        ) -> Result<InodeRecord> {
            let mut inode = self.inode.lock().unwrap().clone();
            inode.kind = InodeKind::Directory;
            inode.attributes = attributes;
            Ok(inode)
        }

        fn read_dir(&self, _: &InodeId) -> Result<Vec<DentryRecord>> {
            Ok(Vec::new())
        }

        fn unlink(
            &self,
            _: &OperationId,
            _: &InodeId,
            _: &[u8],
            _: CallerContext,
        ) -> Result<InodeRecord> {
            Ok(self.inode.lock().unwrap().clone())
        }

        fn rmdir(
            &self,
            _: &OperationId,
            _: &InodeId,
            _: &[u8],
            _: CallerContext,
        ) -> Result<InodeRecord> {
            Ok(self.inode.lock().unwrap().clone())
        }

        fn rename(
            &self,
            _: &OperationId,
            _: &InodeId,
            _: &[u8],
            _: &InodeId,
            _: &[u8],
            _: RenameMode,
            _: CallerContext,
        ) -> Result<RenameOutcome> {
            Ok(RenameOutcome {
                inode: self.inode.lock().unwrap().clone(),
                replaced_inode: None,
            })
        }

        fn link(
            &self,
            _: &OperationId,
            _: &InodeId,
            expected_inode_revision: u64,
            _: &InodeId,
            _: &[u8],
            _: CallerContext,
        ) -> Result<InodeRecord> {
            let mut inode = self.inode.lock().unwrap();
            if inode.revision != expected_inode_revision {
                return Err(unavailable("test link CAS failed"));
            }
            inode.attributes.nlink = inode.attributes.nlink.saturating_add(1);
            inode.revision = inode.revision.saturating_add(1);
            Ok(inode.clone())
        }

        fn symlink(&self, _: SymlinkRequest) -> Result<InodeRecord> {
            Err(unavailable("test meta symlink is not implemented"))
        }

        fn mknod(&self, request: MknodRequest) -> Result<InodeRecord> {
            let mut inode = self.inode.lock().unwrap().clone();
            inode.kind = InodeKind::Special(request.kind);
            inode.attributes = request.attributes;
            inode.head_version = None;
            inode.revision = inode.revision.saturating_add(1);
            *self.inode.lock().unwrap() = inode.clone();
            Ok(inode)
        }

        fn read_link(&self, _: ReadLinkRequest) -> Result<Vec<u8>> {
            Err(unavailable("test meta readlink is not implemented"))
        }

        fn set_inode_attributes(&self, request: SetInodeAttrRequest) -> Result<InodeRecord> {
            let mut inode = self.inode.lock().unwrap();
            if inode.revision != request.expected_inode_revision {
                return Err(unavailable("test setattr CAS failed"));
            }
            if let Some(mode) = request.update.mode {
                inode.attributes.mode = mode;
            }
            if let Some(uid) = request.update.uid {
                inode.attributes.uid = uid;
            }
            if let Some(gid) = request.update.gid {
                inode.attributes.gid = gid;
            }
            if let Some(atime) = request.update.atime_unix_ms {
                inode.attributes.atime_unix_ms = atime;
            }
            if let Some(mtime) = request.update.mtime_unix_ms {
                inode.attributes.mtime_unix_ms = mtime;
            }
            if let Some(ctime) = request.update.ctime_unix_ms {
                inode.attributes.ctime_unix_ms = ctime;
            }
            inode.revision = inode.revision.saturating_add(1);
            Ok(inode.clone())
        }

        fn get_xattr(&self, request: GetXattrRequest) -> Result<Vec<u8>> {
            self.inode
                .lock()
                .unwrap()
                .xattrs
                .get(&request.name)
                .cloned()
                .ok_or_else(|| Error::coded(afs_error::NODE_VFS_NOT_FOUND, "test xattr not found"))
        }

        fn list_xattr(&self, _: ListXattrRequest) -> Result<Vec<Vec<u8>>> {
            Ok(self.inode.lock().unwrap().xattrs.keys().cloned().collect())
        }

        fn set_xattr(&self, request: SetXattrRequest) -> Result<InodeRecord> {
            let mut inode = self.inode.lock().unwrap();
            if inode.revision != request.expected_inode_revision {
                return Err(unavailable("test setxattr CAS failed"));
            }
            let exists = inode.xattrs.contains_key(&request.name);
            match request.mode {
                XattrSetMode::Create if exists => return Err(invalid("test xattr exists")),
                XattrSetMode::Replace if !exists => return Err(invalid("test xattr missing")),
                _ => {}
            }
            inode.xattrs.insert(request.name, request.value);
            inode.revision = inode.revision.saturating_add(1);
            Ok(inode.clone())
        }

        fn remove_xattr(&self, request: RemoveXattrRequest) -> Result<InodeRecord> {
            let mut inode = self.inode.lock().unwrap();
            if inode.revision != request.expected_inode_revision {
                return Err(unavailable("test removexattr CAS failed"));
            }
            inode.xattrs.remove(&request.name);
            inode.revision = inode.revision.saturating_add(1);
            Ok(inode.clone())
        }

        fn lookup_node_location(&self, node_id: &str) -> Result<Option<DfsNodeLocation>> {
            Ok(Some(DfsNodeLocation {
                node_id: node_id.into(),
                node_epoch: 1,
                data_endpoint: "http://127.0.0.1:9".into(),
            }))
        }

        fn current_node_session(&self, node_id: &str) -> Result<Option<String>> {
            self.current_session_calls.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.next_current_session_error.lock().unwrap().take() {
                return Err(error);
            }
            if let Some(session) = self.node_sessions.lock().unwrap().get(node_id).cloned() {
                return Ok(session);
            }
            let suffix = node_id.strip_prefix("node-").unwrap_or("a");
            Ok(Some(format!("session-{suffix}")))
        }
    }

    fn context() -> RequestContext {
        RequestContext {
            uid: 1000,
            gid: 1000,
            pid: 42,
            umask: 0,
            supplementary_gids: Vec::new(),
        }
    }

    fn test_fs() -> (tempfile::TempDir, Arc<RecordingMeta>, DistributedFs) {
        test_fs_with_dirty_budget(DEFAULT_DIRTY_DATA_BUDGET_BYTES)
    }

    fn test_fs_with_dirty_budget(
        budget: u64,
    ) -> (tempfile::TempDir, Arc<RecordingMeta>, DistributedFs) {
        let meta = Arc::new(RecordingMeta::new());
        let temp = tempfile::tempdir().unwrap();
        let chunks = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let fs = distributed_fs_with_chunk_store(meta.clone(), chunks.clone(), chunks, budget);
        (temp, meta, fs)
    }

    fn distributed_fs_with_chunk_store(
        meta: Arc<RecordingMeta>,
        local: Arc<LocalChunkStore>,
        chunk_store: Arc<dyn ChunkStore>,
        budget: u64,
    ) -> DistributedFs {
        let read_engine = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-a".into(),
            local,
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        DistributedFs::new(
            NamespaceId::new("default"),
            "node-a",
            "session-a",
            meta,
            chunk_store,
            read_engine,
        )
        .with_dirty_budget_bytes(budget)
    }

    fn publish_external_version_with_bytes(
        meta: &RecordingMeta,
        fs: &DistributedFs,
        operation_id: &str,
        bytes: &[u8],
    ) -> FileVersionId {
        let receipt = fs
            .chunk_store
            .put(StagedChunk::new(
                OperationId::new(operation_id),
                bytes.to_vec(),
            ))
            .unwrap();
        meta.publish_external_version_with_receipts(
            bytes.len() as u64,
            vec![Extent {
                file_offset: 0,
                length: bytes.len() as u64,
                chunk_id: receipt.chunk.id.clone(),
                chunk_offset: 0,
            }],
            vec![receipt],
        )
    }

    #[cfg(feature = "dfs")]
    struct CountingChunkStore {
        inner: Arc<dyn ChunkStore>,
        put_batches: AtomicUsize,
        capacity_calls: AtomicUsize,
    }

    #[cfg(feature = "dfs")]
    impl CountingChunkStore {
        fn new(inner: Arc<dyn ChunkStore>) -> Self {
            Self {
                inner,
                put_batches: AtomicUsize::new(0),
                capacity_calls: AtomicUsize::new(0),
            }
        }

        fn put_batch_count(&self) -> usize {
            self.put_batches.load(Ordering::SeqCst)
        }

        fn capacity_call_count(&self) -> usize {
            self.capacity_calls.load(Ordering::SeqCst)
        }
    }

    #[cfg(feature = "dfs")]
    impl ChunkStore for CountingChunkStore {
        fn put_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<crate::dfs::ChunkReceipt>> {
            self.put_batches.fetch_add(1, Ordering::SeqCst);
            self.inner.put_batch(staged)
        }

        fn read_at(
            &self,
            chunk_id: &crate::dfs::ChunkId,
            offset: u64,
            out: &mut [u8],
        ) -> Result<usize> {
            self.inner.read_at(chunk_id, offset, out)
        }

        fn capacity(&self) -> Result<FilesystemCapacity> {
            self.capacity_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.capacity()
        }
    }

    #[cfg(feature = "dfs")]
    struct UnsupportedCapacityChunkStore {
        inner: Arc<dyn ChunkStore>,
    }

    #[cfg(feature = "dfs")]
    impl ChunkStore for UnsupportedCapacityChunkStore {
        fn put_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<crate::dfs::ChunkReceipt>> {
            self.inner.put_batch(staged)
        }

        fn read_at(
            &self,
            chunk_id: &crate::dfs::ChunkId,
            offset: u64,
            out: &mut [u8],
        ) -> Result<usize> {
            self.inner.read_at(chunk_id, offset, out)
        }
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn statfs_validates_meta_inode_and_reads_chunk_store_capacity_without_writer() {
        let meta = Arc::new(RecordingMeta::new());
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        local
            .put(StagedChunk::new(
                OperationId::new("statfs-capacity"),
                b"bytes".to_vec(),
            ))
            .unwrap();
        let counting = Arc::new(CountingChunkStore::new(local.clone()));
        let fs = distributed_fs_with_chunk_store(
            meta.clone(),
            local,
            counting.clone(),
            DEFAULT_DIRTY_DATA_BUDGET_BYTES,
        );
        let inode = fs.backend_inode(&InodeId::new("inode:test")).unwrap();
        let open_writes_before = meta.open_write_calls.load(Ordering::SeqCst);

        let capacity = fs.statfs(&context(), inode).unwrap();

        assert!(capacity.bsize > 0);
        assert!(capacity.frsize > 0);
        assert!(capacity.blocks >= capacity.bfree);
        assert_eq!(counting.capacity_call_count(), 1);
        assert_eq!(
            meta.open_write_calls.load(Ordering::SeqCst),
            open_writes_before
        );
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn statfs_preserves_meta_failure_and_rejects_unknown_or_mismatched_inode() {
        let (_temp, meta, fs) = test_fs();
        let inode = fs.backend_inode(&InodeId::new("inode:test")).unwrap();

        meta.fail_next_get_inode_with(unavailable("injected statfs get_inode failure"));
        assert_eq!(
            fs.statfs(&context(), inode).unwrap_err().code(),
            afs_error::NODE_VFS_UNAVAILABLE
        );

        meta.return_bad_inode_on_next_get_inode();
        assert_eq!(
            fs.statfs(&context(), inode).unwrap_err().code(),
            afs_error::NODE_VFS_INVALID
        );

        meta.inode.lock().unwrap().namespace_id = NamespaceId::new("wrong");
        assert_eq!(
            fs.statfs(&context(), inode).unwrap_err().code(),
            afs_error::NODE_VFS_INVALID
        );
        meta.inode.lock().unwrap().namespace_id = NamespaceId::new("default");

        assert_eq!(
            fs.statfs(&context(), BackendInode { value: 999 })
                .unwrap_err()
                .code(),
            afs_error::NODE_DFS_STALE_HANDLE
        );
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn statfs_returns_unsupported_when_chunk_backend_has_no_capacity_capability() {
        let meta = Arc::new(RecordingMeta::new());
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let unsupported = Arc::new(UnsupportedCapacityChunkStore {
            inner: local.clone(),
        });
        let fs = distributed_fs_with_chunk_store(
            meta.clone(),
            local,
            unsupported,
            DEFAULT_DIRTY_DATA_BUDGET_BYTES,
        );
        let inode = fs.backend_inode(&InodeId::new("inode:test")).unwrap();
        let open_writes_before = meta.open_write_calls.load(Ordering::SeqCst);

        let error = fs.statfs(&context(), inode).unwrap_err();

        assert_eq!(error.code(), afs_error::NODE_VFS_UNIMPLEMENTED);
        assert_eq!(
            meta.open_write_calls.load(Ordering::SeqCst),
            open_writes_before
        );
    }

    #[cfg(feature = "dfs")]
    #[derive(Clone)]
    struct HoldCommitRepliesLayer {
        gate: Arc<HoldCommitRepliesGate>,
    }

    #[cfg(feature = "dfs")]
    struct HoldCommitRepliesGate {
        first_delay: Duration,
        hits: AtomicUsize,
        first_done: AtomicBool,
        first_done_notify: tokio::sync::Notify,
        second_reached: AtomicBool,
        second_reached_notify: tokio::sync::Notify,
        release_second: AtomicBool,
        release_second_notify: tokio::sync::Notify,
    }

    #[cfg(feature = "dfs")]
    impl HoldCommitRepliesLayer {
        fn new(first_delay: Duration) -> Self {
            Self {
                gate: Arc::new(HoldCommitRepliesGate {
                    first_delay,
                    hits: AtomicUsize::new(0),
                    first_done: AtomicBool::new(false),
                    first_done_notify: tokio::sync::Notify::new(),
                    second_reached: AtomicBool::new(false),
                    second_reached_notify: tokio::sync::Notify::new(),
                    release_second: AtomicBool::new(false),
                    release_second_notify: tokio::sync::Notify::new(),
                }),
            }
        }

        async fn wait_first_committed(&self, timeout: Duration) {
            let notified = self.gate.first_done_notify.notified();
            if self.gate.first_done.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::timeout(timeout, notified)
                .await
                .expect("first CommitFileVersion did not reach post-handler delay");
        }

        async fn wait_second_reached(&self, timeout: Duration) {
            let notified = self.gate.second_reached_notify.notified();
            if self.gate.second_reached.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::timeout(timeout, notified)
                .await
                .expect("second CommitFileVersion replay did not reach explicit response gate");
        }

        fn release_second(&self) {
            self.gate.release_second.store(true, Ordering::SeqCst);
            self.gate.release_second_notify.notify_waiters();
        }
    }

    #[cfg(feature = "dfs")]
    impl<S> tower::Layer<S> for HoldCommitRepliesLayer {
        type Service = HoldCommitRepliesService<S>;

        fn layer(&self, inner: S) -> Self::Service {
            HoldCommitRepliesService {
                inner,
                gate: self.gate.clone(),
            }
        }
    }

    #[cfg(feature = "dfs")]
    #[derive(Clone)]
    struct HoldCommitRepliesService<S> {
        inner: S,
        gate: Arc<HoldCommitRepliesGate>,
    }

    #[cfg(feature = "dfs")]
    impl<S, B> tower::Service<tonic::codegen::http::Request<B>> for HoldCommitRepliesService<S>
    where
        S: tower::Service<tonic::codegen::http::Request<B>> + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Response: Send + 'static,
        S::Error: Send + 'static,
        B: Send + 'static,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = std::pin::Pin<
            Box<
                dyn std::future::Future<Output = std::result::Result<Self::Response, Self::Error>>
                    + Send,
            >,
        >;

        fn poll_ready(
            &mut self,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
            let path = request.uri().path().to_owned();
            let future = self.inner.call(request);
            let gate = self.gate.clone();
            Box::pin(async move {
                let response = future.await?;
                if path == "/afs.meta.v1.DfsMeta/CommitFileVersion" {
                    match gate.hits.fetch_add(1, Ordering::SeqCst) {
                        0 => {
                            gate.first_done.store(true, Ordering::SeqCst);
                            gate.first_done_notify.notify_waiters();
                            tokio::time::sleep(gate.first_delay).await;
                        }
                        1 => {
                            gate.second_reached.store(true, Ordering::SeqCst);
                            gate.second_reached_notify.notify_waiters();
                            while !gate.release_second.load(Ordering::SeqCst) {
                                let notified = gate.release_second_notify.notified();
                                if gate.release_second.load(Ordering::SeqCst) {
                                    break;
                                }
                                notified.await;
                            }
                        }
                        _ => {}
                    }
                }
                Ok(response)
            })
        }
    }

    #[cfg(feature = "dfs")]
    struct CapturingDfsMeta {
        inner: Arc<crate::node::rpc::meta::GrpcDfsMeta>,
        commits: Mutex<Vec<CommitFileVersion>>,
    }

    #[cfg(feature = "dfs")]
    impl CapturingDfsMeta {
        fn new(inner: crate::node::rpc::meta::GrpcDfsMeta) -> Self {
            Self {
                inner: Arc::new(inner),
                commits: Mutex::new(Vec::new()),
            }
        }

        fn commits(&self) -> Vec<CommitFileVersion> {
            self.commits.lock().unwrap().clone()
        }
    }

    #[cfg(feature = "dfs")]
    impl DfsMeta for CapturingDfsMeta {
        fn lookup(&self, parent: &InodeId, name: &[u8]) -> Result<Option<InodeRecord>> {
            DfsMeta::lookup(self.inner.as_ref(), parent, name)
        }

        fn create(
            &self,
            operation_id: &OperationId,
            parent: &InodeId,
            name: &[u8],
            attributes: InodeAttributes,
        ) -> Result<(InodeRecord, WriteLease)> {
            DfsMeta::create(self.inner.as_ref(), operation_id, parent, name, attributes)
        }

        fn get_inode(&self, inode_id: &InodeId) -> Result<InodeRecord> {
            DfsMeta::get_inode(self.inner.as_ref(), inode_id)
        }

        fn get_file_version(
            &self,
            version_id: &FileVersionId,
        ) -> Result<(FileVersion, LayoutRoot)> {
            DfsMeta::get_file_version(self.inner.as_ref(), version_id)
        }

        fn open_write(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
            DfsMeta::open_write(self.inner.as_ref(), inode_id)
        }

        fn resolve_lock_authority(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
            DfsMeta::resolve_lock_authority(self.inner.as_ref(), inode_id)
        }

        fn resolve_write_authority(&self, inode_id: &InodeId) -> Result<(InodeRecord, WriteLease)> {
            DfsMeta::resolve_write_authority(self.inner.as_ref(), inode_id)
        }

        fn renew_write_lease(&self, lease: WriteLease) -> Result<WriteLease> {
            DfsMeta::renew_write_lease(self.inner.as_ref(), lease)
        }

        fn renew_write_lease_with_timeout(
            &self,
            lease: WriteLease,
            timeout: Duration,
        ) -> Result<WriteLease> {
            DfsMeta::renew_write_lease_with_timeout(self.inner.as_ref(), lease, timeout)
        }

        fn sync_inode_metadata(&self, sync: SyncInodeMetadata) -> Result<InodeRecord> {
            DfsMeta::sync_inode_metadata(self.inner.as_ref(), sync)
        }

        fn sync_inode_metadata_with_timeout(
            &self,
            sync: SyncInodeMetadata,
            timeout: Duration,
        ) -> Result<InodeRecord> {
            DfsMeta::sync_inode_metadata_with_timeout(self.inner.as_ref(), sync, timeout)
        }

        fn commit_file_version(&self, commit: CommitFileVersion) -> Result<InodeRecord> {
            self.commits.lock().unwrap().push(commit.clone());
            DfsMeta::commit_file_version(self.inner.as_ref(), commit)
        }

        fn commit_file_version_with_timeout(
            &self,
            commit: CommitFileVersion,
            timeout: Duration,
        ) -> Result<InodeRecord> {
            self.commits.lock().unwrap().push(commit.clone());
            DfsMeta::commit_file_version_with_timeout(self.inner.as_ref(), commit, timeout)
        }

        fn lookup_node_location(&self, node_id: &str) -> Result<Option<DfsNodeLocation>> {
            DfsMeta::lookup_node_location(self.inner.as_ref(), node_id)
        }

        fn current_node_session(&self, node_id: &str) -> Result<Option<String>> {
            DfsMeta::current_node_session(self.inner.as_ref(), node_id)
        }

        fn current_node_session_with_timeout(
            &self,
            node_id: &str,
            timeout: Duration,
        ) -> Result<Option<String>> {
            DfsMeta::current_node_session_with_timeout(self.inner.as_ref(), node_id, timeout)
        }

        fn mkdir(
            &self,
            operation_id: &OperationId,
            parent: &InodeId,
            name: &[u8],
            attributes: InodeAttributes,
            caller: CallerContext,
        ) -> Result<InodeRecord> {
            DfsMeta::mkdir(
                self.inner.as_ref(),
                operation_id,
                parent,
                name,
                attributes,
                caller,
            )
        }

        fn read_dir(&self, parent: &InodeId) -> Result<Vec<DentryRecord>> {
            DfsMeta::read_dir(self.inner.as_ref(), parent)
        }

        fn link(
            &self,
            operation_id: &OperationId,
            inode_id: &InodeId,
            expected_inode_revision: u64,
            parent: &InodeId,
            name: &[u8],
            caller: CallerContext,
        ) -> Result<InodeRecord> {
            DfsMeta::link(
                self.inner.as_ref(),
                operation_id,
                inode_id,
                expected_inode_revision,
                parent,
                name,
                caller,
            )
        }

        fn symlink(&self, request: SymlinkRequest) -> Result<InodeRecord> {
            DfsMeta::symlink(self.inner.as_ref(), request)
        }

        fn mknod(&self, request: MknodRequest) -> Result<InodeRecord> {
            DfsMeta::mknod(self.inner.as_ref(), request)
        }

        fn read_link(&self, request: ReadLinkRequest) -> Result<Vec<u8>> {
            DfsMeta::read_link(self.inner.as_ref(), request)
        }

        fn set_inode_attributes(&self, request: SetInodeAttrRequest) -> Result<InodeRecord> {
            DfsMeta::set_inode_attributes(self.inner.as_ref(), request)
        }

        fn get_xattr(&self, request: GetXattrRequest) -> Result<Vec<u8>> {
            DfsMeta::get_xattr(self.inner.as_ref(), request)
        }

        fn list_xattr(&self, request: ListXattrRequest) -> Result<Vec<Vec<u8>>> {
            DfsMeta::list_xattr(self.inner.as_ref(), request)
        }

        fn set_xattr(&self, request: SetXattrRequest) -> Result<InodeRecord> {
            DfsMeta::set_xattr(self.inner.as_ref(), request)
        }

        fn remove_xattr(&self, request: RemoveXattrRequest) -> Result<InodeRecord> {
            DfsMeta::remove_xattr(self.inner.as_ref(), request)
        }

        fn unlink(
            &self,
            operation_id: &OperationId,
            parent: &InodeId,
            name: &[u8],
            caller: CallerContext,
        ) -> Result<InodeRecord> {
            DfsMeta::unlink(self.inner.as_ref(), operation_id, parent, name, caller)
        }

        fn rmdir(
            &self,
            operation_id: &OperationId,
            parent: &InodeId,
            name: &[u8],
            caller: CallerContext,
        ) -> Result<InodeRecord> {
            DfsMeta::rmdir(self.inner.as_ref(), operation_id, parent, name, caller)
        }

        fn rename(
            &self,
            operation_id: &OperationId,
            old_parent: &InodeId,
            old_name: &[u8],
            new_parent: &InodeId,
            new_name: &[u8],
            mode: RenameMode,
            caller: CallerContext,
        ) -> Result<RenameOutcome> {
            DfsMeta::rename(
                self.inner.as_ref(),
                operation_id,
                old_parent,
                old_name,
                new_parent,
                new_name,
                mode,
                caller,
            )
        }
    }

    #[cfg(feature = "dfs")]
    async fn grpc_dfs_test_fs(
        node_id: &str,
        session_id: &str,
        meta_endpoint: &str,
        local: Arc<LocalChunkStore>,
        timeout: Duration,
    ) -> (
        Arc<CapturingDfsMeta>,
        Arc<CountingChunkStore>,
        DistributedFs,
    ) {
        let meta = Arc::new(CapturingDfsMeta::new(
            crate::node::rpc::meta::GrpcDfsMeta::new(
                meta_endpoint,
                node_id.to_owned(),
                session_id.to_owned(),
                NamespaceId::new("default"),
                timeout,
                afs_transport::TlsConfig::Disabled,
            )
            .unwrap(),
        ));
        let peers = Arc::new(
            crate::node::rpc::peer::PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::Disabled,
                4,
            )
            .unwrap(),
        );
        let data_plane =
            Arc::new(crate::node::rpc::peer::GrpcReplicaDataPlane::new(peers, timeout).unwrap());
        // Use the product R1 path and Meta placement authority. The test-only
        // LocalChunkStore trait adapter fabricates a group for mock Meta tests.
        let chunks = Arc::new(CountingChunkStore::new(Arc::new(
            crate::node::replication::DfsChunkStore::new_with_epoch(
                node_id.to_owned(),
                1,
                local.clone(),
                meta.inner.clone(),
                data_plane,
            ),
        )));
        let read_engine = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            node_id.to_owned(),
            local,
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        (
            meta.clone(),
            chunks.clone(),
            DistributedFs::new(
                NamespaceId::new("default"),
                node_id,
                session_id,
                meta,
                chunks,
                read_engine,
            ),
        )
    }

    #[cfg(feature = "dfs")]
    async fn real_grpc_meta_fixture() -> (
        tempfile::TempDir,
        String,
        HoldCommitRepliesLayer,
        tokio::task::JoinHandle<()>,
        Arc<dyn crate::meta::store::MetaStore>,
        Arc<LocalChunkStore>,
        Arc<CountingStoreBackend>,
    ) {
        use crate::meta::{
            Meta,
            store::{MetaRead, NodeSessionLease, RequestKey, Store, memory::MemoryBackend},
        };
        use tokio::net::TcpListener;
        use tokio_stream::wrappers::TcpListenerStream;
        use tonic::transport::Server;

        let temp = tempfile::tempdir().unwrap();
        let local_a =
            Arc::new(LocalChunkStore::open(temp.path().join("node-a"), "node-a").unwrap());
        let backend = Arc::new(CountingStoreBackend::new(
            Arc::new(MemoryBackend::default()),
        ));
        let store: Arc<dyn crate::meta::store::MetaStore> =
            Arc::new(Store::open(backend.clone()).await.unwrap());
        store
            .register_node_session(
                RequestKey::new("node-a", "register-node-a"),
                NodeSessionLease {
                    node_id: "node-a".into(),
                    session_id: "session-a".into(),
                    grpc_addr: "http://127.0.0.1:1".into(),
                    data_addr: "http://127.0.0.1:1".into(),
                    rest_addr: "http://127.0.0.1:1".into(),
                    storage_devices: vec![local_a.device_descriptor().unwrap()],
                    lease_ttl: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        let meta = Meta::with_store(
            "meta-unknown-file-ack".into(),
            crate::runtime::Observability::new().unwrap(),
            store.clone(),
        );
        meta.dfs
            .as_ref()
            .unwrap()
            .initialize_replication_config()
            .await
            .unwrap();
        assert!(
            store
                .read(MetaRead::DfsReplicationConfig)
                .await
                .unwrap()
                .entity
                .is_some()
        );
        let meta = Arc::new(meta);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let meta_endpoint = format!("http://{}", listener.local_addr().unwrap());
        let gate = HoldCommitRepliesLayer::new(Duration::from_secs(3));
        let server_gate = gate.clone();
        let server = tokio::spawn(async move {
            Server::builder()
                .layer(server_gate)
                .add_service(afs_protocol::meta::dfs_meta_server::DfsMetaServer::new(
                    crate::meta::rpc::DfsMetaRpc(meta),
                ))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (temp, meta_endpoint, gate, server, store, local_a, backend)
    }

    #[cfg(feature = "dfs")]
    struct ReleaseCommitGateOnDrop(HoldCommitRepliesLayer);

    #[cfg(feature = "dfs")]
    impl Drop for ReleaseCommitGateOnDrop {
        fn drop(&mut self) {
            self.0.release_second();
        }
    }

    #[cfg(feature = "dfs")]
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct InodeMutationSnapshot {
        logical_length: u64,
        dirty: bool,
        dirty_extent_count: usize,
        visible_write_seq: u64,
        durable_write_seq: u64,
        committed_write_seq: u64,
        pending_operation_id: OperationId,
        pending_version_id: FileVersionId,
        pending_layout_id: LayoutRootId,
    }

    #[cfg(feature = "dfs")]
    fn pending_file_commit_snapshot(
        fs: &DistributedFs,
        inode_id: &InodeId,
    ) -> (PendingFileCommit, InodeMutationSnapshot) {
        let cell = fs.write_state(inode_id).unwrap().unwrap();
        let state = cell.lock().unwrap();
        let pending = match state.in_flight.as_ref().unwrap() {
            InFlightCommit::File(pending) => pending.as_ref().clone(),
            _ => panic!("expected pending file commit"),
        };
        let snapshot = InodeMutationSnapshot {
            logical_length: state.logical_length,
            dirty: state.dirty,
            dirty_extent_count: state.dirty_extents.extents.len(),
            visible_write_seq: state.visible_write_seq,
            durable_write_seq: state.durable_write_seq,
            committed_write_seq: state.committed_write_seq,
            pending_operation_id: pending.batch.commit.operation_id.clone(),
            pending_version_id: pending.batch.commit.file_version.id.clone(),
            pending_layout_id: pending.batch.commit.layout_root.id.clone(),
        };
        (pending, snapshot)
    }

    #[cfg(feature = "dfs")]
    fn read_exact_from(fs: &DistributedFs, handle: FileHandle, offset: u64, len: usize) -> Vec<u8> {
        let mut bytes = vec![0; len];
        let read = fs.read(&context(), handle, offset, &mut bytes).unwrap();
        bytes.truncate(read);
        bytes
    }

    #[cfg(feature = "dfs")]
    async fn run_blocking<T: Send + 'static>(action: impl FnOnce() -> T + Send + 'static) -> T {
        tokio::time::timeout(Duration::from_secs(5), tokio::task::spawn_blocking(action))
            .await
            .expect("blocking DFS test operation timed out")
            .unwrap()
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_grpc_lock_resolver_preserves_locks_without_meta_commits() {
        use crate::meta::store::{MetaEntity, MetaRead};
        let (_temp, endpoint, _gate, server, store, local, _backend) =
            real_grpc_meta_fixture().await;
        let (_meta, _chunks, fs) = grpc_dfs_test_fs(
            "node-a",
            "session-a",
            &endpoint,
            local,
            Duration::from_secs(2),
        )
        .await;
        let fs = Arc::new(fs);
        let creating = fs.clone();
        let created = run_blocking(move || {
            creating.create(
                &context(),
                creating.root_inode(),
                OsStr::new("grpc-lock-resolver.bin"),
                0o640,
                libc::O_RDWR,
            )
        })
        .await
        .unwrap();
        let inode_id = fs.inode_id(created.entry.inode).unwrap();
        let before = store
            .read(MetaRead::DfsWriteLease(inode_id.clone()))
            .await
            .unwrap();
        let locking = fs.clone();
        run_blocking(move || {
            locking.setlk(
                &context(),
                created.entry.inode,
                created.handle,
                LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-a", 71),
                    71,
                    lock_range(0, 99),
                ),
                None,
            )?;
            for _ in 0..32 {
                let conflict = locking.getlk(
                    &context(),
                    created.entry.inode,
                    created.handle,
                    LockRequest::write(
                        FileLockKind::Posix,
                        lock_owner("mount-b", 72),
                        72,
                        lock_range(0, 99),
                    ),
                )?;
                assert!(conflict.is_some());
            }
            locking.release_locks(
                &context(),
                created.entry.inode,
                created.handle,
                lock_owner("mount-a", 71),
                ReleaseKind::PosixOwner,
            )?;
            assert!(
                locking
                    .getlk(
                        &context(),
                        created.entry.inode,
                        created.handle,
                        LockRequest::write(
                            FileLockKind::Posix,
                            lock_owner("mount-b", 72),
                            72,
                            lock_range(0, 99)
                        )
                    )?
                    .is_none()
            );
            Ok::<(), Error>(())
        })
        .await
        .unwrap();
        let after = store.read(MetaRead::DfsWriteLease(inode_id)).await.unwrap();
        assert_eq!(
            after.revision, before.revision,
            "actual Node/Meta RPC lock path must not commit"
        );
        assert_eq!(after.entity, before.entity);
        assert!(matches!(after.entity, Some(MetaEntity::DfsWriteLease(_))));
        server.abort();
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_grpc_writable_reopen_resolves_write_authority_without_meta_mutation() {
        use crate::meta::store::{MetaEntity, MetaRead, RequestKey};
        let (_temp, endpoint, _gate, server, store, local, backend) =
            real_grpc_meta_fixture().await;
        let (_meta, _chunks, fs_raw) = grpc_dfs_test_fs(
            "node-a",
            "session-a",
            &endpoint,
            local,
            Duration::from_secs(2),
        )
        .await;
        let fs = Arc::new(fs_raw);
        let created = {
            let fs = fs.clone();
            run_blocking(move || {
                fs.create(
                    &context(),
                    fs.root_inode(),
                    OsStr::new("grpc-write-authority-reopen.bin"),
                    0o640,
                    libc::O_RDWR,
                )
            })
            .await
            .unwrap()
        };
        let inode = created.entry.inode;
        let inode_id = fs.inode_id(inode).unwrap();
        {
            let fs = fs.clone();
            run_blocking(move || fs.release(&context(), created.handle))
                .await
                .unwrap();
        }
        let before = store
            .read(MetaRead::DfsWriteLease(inode_id.clone()))
            .await
            .unwrap();
        let before_commits = backend.commit_count();
        for _ in 0..4 {
            let opening = fs.clone();
            let handle = run_blocking(move || opening.open(&context(), inode, libc::O_RDWR))
                .await
                .unwrap();
            let releasing = fs.clone();
            run_blocking(move || releasing.release(&context(), handle))
                .await
                .unwrap();
        }
        let after = store.read(MetaRead::DfsWriteLease(inode_id)).await.unwrap();
        assert_eq!(after.revision, before.revision);
        assert_eq!(after.entity, before.entity);
        assert!(matches!(after.entity, Some(MetaEntity::DfsWriteLease(_))));
        assert_eq!(
            backend.commit_count(),
            before_commits,
            "production GrpcDfsMeta writable reopen must be a fresh read, not CAS/outcome mutation"
        );
        for request_id in [
            "session-a-dfs-2",
            "session-a-dfs-3",
            "session-a-dfs-4",
            "session-a-dfs-5",
        ] {
            assert!(
                store
                    .read(MetaRead::RequestOutcome(RequestKey::new(
                        "node-a", request_id
                    )))
                    .await
                    .unwrap()
                    .request_outcome
                    .is_none(),
                "live write authority request {request_id} must not persist an outcome"
            );
        }
        server.abort();
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_grpc_write_authority_foreign_owner_does_not_take_over_or_mutate() {
        use crate::meta::store::{MetaEntity, MetaRead, RequestKey};
        let (_temp, endpoint, _gate, server, store, local, backend) =
            real_grpc_meta_fixture().await;
        let (_meta_a, _chunks_a, fs_a_raw) = grpc_dfs_test_fs(
            "node-a",
            "session-a",
            &endpoint,
            local,
            Duration::from_secs(2),
        )
        .await;
        let fs_a = Arc::new(fs_a_raw);
        let created = {
            let fs = fs_a.clone();
            run_blocking(move || {
                fs.create(
                    &context(),
                    fs.root_inode(),
                    OsStr::new("grpc-write-authority-foreign.bin"),
                    0o640,
                    libc::O_RDWR,
                )
            })
            .await
            .unwrap()
        };
        let inode_id = fs_a.inode_id(created.entry.inode).unwrap();
        let before = store
            .read(MetaRead::DfsWriteLease(inode_id.clone()))
            .await
            .unwrap();
        let before_commits = backend.commit_count();
        let (inode, lease) = {
            let endpoint = endpoint.clone();
            let inode_id = inode_id.clone();
            run_blocking(move || {
                let foreign = crate::node::rpc::meta::GrpcDfsMeta::new(
                    &endpoint,
                    "node-b".into(),
                    "session-b".into(),
                    NamespaceId::new("default"),
                    Duration::from_secs(2),
                    afs_transport::TlsConfig::Disabled,
                )
                .unwrap();
                DfsMeta::resolve_write_authority(&foreign, &inode_id)
            })
            .await
            .unwrap()
        };

        assert_eq!(inode.inode_id, inode_id);
        assert_eq!(lease.owner_node_id, "node-a");
        assert_eq!(lease.owner_session_id, "session-a");
        assert_eq!(
            backend.commit_count(),
            before_commits,
            "foreign live write authority resolution must not acquire or mutate Meta"
        );
        let after = store.read(MetaRead::DfsWriteLease(inode_id)).await.unwrap();
        assert_eq!(after.revision, before.revision);
        assert_eq!(after.entity, before.entity);
        assert!(matches!(after.entity, Some(MetaEntity::DfsWriteLease(_))));
        assert!(
            store
                .read(MetaRead::RequestOutcome(RequestKey::new(
                    "node-b",
                    "session-b-dfs-1",
                )))
                .await
                .unwrap()
                .request_outcome
                .is_none(),
            "foreign live write authority read must not persist an outcome"
        );
        server.abort();
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_grpc_unknown_file_commit_ack_blocks_inode_and_replays_exact_request() {
        use crate::meta::store::{
            MetaEntity, MetaRead, OperationResult, RequestKey, StoreOperation,
        };

        let (_temp, meta_endpoint, gate, server, store, local_a, _backend) =
            real_grpc_meta_fixture().await;
        let (meta_a, chunks_a, fs_a_raw) = grpc_dfs_test_fs(
            "node-a",
            "session-a",
            &meta_endpoint,
            local_a,
            Duration::from_secs(2),
        )
        .await;
        let mut fs_a = Arc::new(fs_a_raw);

        let created_a = {
            let fs = fs_a.clone();
            run_blocking(move || {
                fs.create(
                    &context(),
                    fs.root_inode(),
                    OsStr::new("unknown-file-ack-a.bin"),
                    0o640,
                    libc::O_RDWR,
                )
            })
            .await
            .unwrap()
        };
        let inode_backend_a = created_a.entry.inode;
        let handle_a = created_a.handle;
        let inode_a = fs_a.inode_id(inode_backend_a).unwrap();
        let trace_output = _temp.path().join("real-meta-pending-trace.jsonl");
        Arc::get_mut(&mut fs_a).unwrap().pending_trace = Some(PendingTrace::test_gate(
            &trace_output,
            "default",
            "node-a",
            "session-a",
            &inode_a.0,
        ));
        {
            let fs = fs_a.clone();
            run_blocking(move || fs.write(&context(), handle_a, 0, b"alpha"))
                .await
                .unwrap();
        }

        let created_b = {
            let fs = fs_a.clone();
            run_blocking(move || {
                fs.create(
                    &context(),
                    fs.root_inode(),
                    OsStr::new("unknown-file-ack-b.bin"),
                    0o640,
                    libc::O_RDWR,
                )
            })
            .await
            .unwrap()
        };
        let handle_b = created_b.handle;

        let first_error = {
            let fs = fs_a.clone();
            run_blocking(move || fs.fsync(&context(), handle_a, SyncMode::Full))
                .await
                .unwrap_err()
        };
        assert_eq!(
            first_error.code(),
            afs_error::CLIENT_DEADLINE_EXCEEDED,
            "unexpected first commit error: {first_error:?}"
        );
        gate.wait_first_committed(Duration::from_secs(1)).await;

        let (pending, blocked_before_replay) = pending_file_commit_snapshot(&fs_a, &inode_a);
        let trace_rows: Vec<serde_json::Value> = std::fs::read_to_string(&trace_output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(trace_rows[1]["event"], "dfs_pending_commit_first_send");
        assert_eq!(
            trace_rows[2]["event"],
            "dfs_pending_commit_unknown_retained"
        );
        assert_eq!(
            trace_rows[1]["request_digest"],
            trace_rows[2]["request_digest"]
        );
        assert_eq!(chunks_a.put_batch_count(), 1);
        assert_eq!(pending.frozen.through_seq, 1);
        assert_eq!(pending.frozen.logical_length, 5);
        assert_eq!(pending.batch.through_seq, 1);
        assert_eq!(pending.batch.commit.layout_root.file_length, 5);
        assert_eq!(pending.batch.commit.file_version.length, 5);
        assert_eq!(pending.batch.commit.chunk_receipts.len(), 1);

        let stored_inode = store
            .read(MetaRead::DfsInode(inode_a.clone()))
            .await
            .unwrap();
        let stored_inode = match stored_inode.entity.unwrap() {
            MetaEntity::DfsInode(inode) => inode,
            _ => panic!("expected stored inode"),
        };
        assert_eq!(
            stored_inode.head_version,
            Some(pending.batch.commit.file_version.id.clone())
        );
        let stored_version = store
            .read(MetaRead::DfsFileVersion(
                pending.batch.commit.file_version.id.clone(),
            ))
            .await
            .unwrap();
        let stored_version = match stored_version.entity.unwrap() {
            MetaEntity::DfsFileVersion(version) => version,
            _ => panic!("expected stored file version"),
        };
        assert_eq!(stored_version, pending.batch.commit.file_version);
        let stored_layout = store
            .read(MetaRead::DfsLayoutRoot(
                pending.batch.commit.layout_root.id.clone(),
            ))
            .await
            .unwrap();
        let stored_layout = match stored_layout.entity.unwrap() {
            MetaEntity::DfsLayoutRoot(layout) => layout,
            _ => panic!("expected stored layout root"),
        };
        assert_eq!(stored_layout, pending.batch.commit.layout_root);
        let outcome = store
            .read(MetaRead::RequestOutcome(RequestKey::new(
                "node-a",
                pending.batch.commit.operation_id.0.clone(),
            )))
            .await
            .unwrap()
            .request_outcome
            .expect("commit outcome must be durable before reply release");
        assert_eq!(outcome.operation, StoreOperation::DfsCommitFileVersion);
        match outcome.result.clone() {
            OperationResult::DfsNamespace {
                request_digest,
                result,
            } if trace_rows[2]["request_digest"]
                == request_digest
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
                && matches!(
                    result.as_ref(),
                    OperationResult::DfsInode(inode)
                        if inode.head_version == Some(pending.batch.commit.file_version.id.clone())
                ) => {}
            other => panic!("unexpected commit outcome: {other:?}"),
        }
        // Optional immutable test export. This fixture deliberately has no
        // installed-process identity and cannot qualify installed acceptance.
        let trace_fixture =
            std::env::var_os("AFS_DFS_PENDING_TRACE_FIXTURE_DIR").map(std::path::PathBuf::from);
        if let Some(directory) = &trace_fixture {
            use crate::meta::store::StoreBackend;
            std::fs::create_dir(directory).unwrap();
            std::fs::copy(&trace_output, directory.join("trace-unknown.jsonl")).unwrap();
            std::fs::write(
                directory.join("commit-request.json"),
                serde_json::to_vec(&pending.batch.commit).unwrap(),
            )
            .unwrap();
            std::fs::write(
                directory.join("meta-outcome.json"),
                serde_json::to_vec(&outcome).unwrap(),
            )
            .unwrap();
            let (revision, bytes) = _backend.load().await.unwrap().unwrap();
            std::fs::write(directory.join("meta-store-state.raw.json"), bytes).unwrap();
            std::fs::write(directory.join("scope.json"), serde_json::to_vec(&serde_json::json!({"status":"HARNESS_ONLY",
                "test_fixture":true,"backend":"memory","backend_revision":revision,"installed_fault":"NOT_RUN",
                "reason":"actual Rust emit and Meta outcome; header lacks installed process identity"})).unwrap()).unwrap();
        }

        let replay = {
            let fs = fs_a.clone();
            tokio::task::spawn_blocking(move || fs.fsync(&context(), handle_a, SyncMode::Full))
        };
        gate.wait_second_reached(Duration::from_secs(1)).await;
        let release_gate_on_drop = ReleaseCommitGateOnDrop(gate.clone());

        let replayed_inode = store
            .read(MetaRead::DfsInode(inode_a.clone()))
            .await
            .unwrap();
        assert_eq!(
            replayed_inode.entity,
            Some(MetaEntity::DfsInode(stored_inode))
        );
        let replayed_version = store
            .read(MetaRead::DfsFileVersion(
                pending.batch.commit.file_version.id.clone(),
            ))
            .await
            .unwrap();
        assert_eq!(
            replayed_version.entity,
            Some(MetaEntity::DfsFileVersion(stored_version))
        );
        let replayed_layout = store
            .read(MetaRead::DfsLayoutRoot(
                pending.batch.commit.layout_root.id.clone(),
            ))
            .await
            .unwrap();
        assert_eq!(
            replayed_layout.entity,
            Some(MetaEntity::DfsLayoutRoot(stored_layout))
        );
        let replayed_outcome = store
            .read(MetaRead::RequestOutcome(RequestKey::new(
                "node-a",
                pending.batch.commit.operation_id.0.clone(),
            )))
            .await
            .unwrap();
        assert_eq!(replayed_outcome.request_outcome, Some(outcome));

        let mut blocked_write = {
            let fs = fs_a.clone();
            tokio::task::spawn_blocking(move || fs.write(&context(), handle_a, 5, b"!"))
        };
        let mut blocked_resize = {
            let fs = fs_a.clone();
            tokio::task::spawn_blocking(move || {
                fs.setattr(
                    &context(),
                    inode_backend_a,
                    Some(handle_a),
                    &AttributeChange {
                        size: Some(7),
                        ..AttributeChange::default()
                    },
                )
            })
        };
        let mut blocked_sync = {
            let fs = fs_a.clone();
            tokio::task::spawn_blocking(move || fs.fsync(&context(), handle_a, SyncMode::Full))
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut blocked_write)
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut blocked_resize)
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut blocked_sync)
                .await
                .is_err()
        );
        let (still_pending, blocked_after_wait) = pending_file_commit_snapshot(&fs_a, &inode_a);
        assert_eq!(still_pending.batch.commit, pending.batch.commit);
        assert_eq!(blocked_after_wait, blocked_before_replay);
        assert_eq!(chunks_a.put_batch_count(), 1);
        let captured_commits = meta_a.commits();
        assert_eq!(captured_commits.len(), 2);
        assert_eq!(captured_commits[0], pending.batch.commit);
        assert_eq!(captured_commits[1], pending.batch.commit);

        {
            let fs = fs_a.clone();
            run_blocking(move || fs.write(&context(), handle_b, 0, b"bravo"))
                .await
                .unwrap();
        }
        {
            let fs = fs_a.clone();
            run_blocking(move || fs.fsync(&context(), handle_b, SyncMode::Full))
                .await
                .unwrap();
        }
        assert_eq!(chunks_a.put_batch_count(), 2);
        let b_bytes = {
            let fs = fs_a.clone();
            run_blocking(move || read_exact_from(&fs, handle_b, 0, 8)).await
        };
        assert_eq!(b_bytes, b"bravo".to_vec());
        let a_bytes = {
            let fs = fs_a.clone();
            run_blocking(move || read_exact_from(&fs, handle_a, 0, 8)).await
        };
        assert_eq!(a_bytes, b"alpha".to_vec());

        gate.release_second();
        drop(release_gate_on_drop);
        tokio::time::timeout(Duration::from_secs(2), replay)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Some(directory) = &trace_fixture {
            std::fs::copy(&trace_output, directory.join("trace-after-replay.jsonl")).unwrap();
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), blocked_write)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            1
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), blocked_resize)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
                .size,
            7
        );
        tokio::time::timeout(Duration::from_secs(2), blocked_sync)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        {
            let fs = fs_a.clone();
            run_blocking(move || fs.fsync(&context(), handle_a, SyncMode::Full))
                .await
                .unwrap();
        }
        {
            let fs = fs_a.clone();
            run_blocking(move || fs.flush(&context(), handle_a))
                .await
                .unwrap();
        }
        {
            let fs = fs_a.clone();
            run_blocking(move || fs.release(&context(), handle_a))
                .await
                .unwrap();
        }

        let reopened = {
            let fs = fs_a.clone();
            run_blocking(move || fs.open(&context(), inode_backend_a, libc::O_RDONLY))
                .await
                .unwrap()
        };
        let final_read = {
            let fs = fs_a.clone();
            run_blocking(move || {
                let mut final_bytes = vec![0; 8];
                let read = fs.read(&context(), reopened, 0, &mut final_bytes).unwrap();
                final_bytes.truncate(read);
                let eof = fs
                    .read(&context(), reopened, read as u64, &mut [0; 8])
                    .unwrap();
                fs.release(&context(), reopened).unwrap();
                (final_bytes, eof)
            })
            .await
        };
        assert_eq!(final_read.0, b"alpha!\0".to_vec());
        assert_eq!(final_read.1, 0);
        {
            let fs = fs_a.clone();
            run_blocking(move || fs.release(&context(), handle_b))
                .await
                .unwrap();
        }
        server.abort();
    }

    #[test]
    fn killpriv_positive_write_clears_suid_and_executable_sgid_until_commit() {
        let (_temp, meta, fs) = test_fs();
        meta.inode.lock().unwrap().attributes.mode = 0o6777;
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("killpriv-write.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();

        assert_eq!(
            fs.write_with_options(
                &context(),
                created.handle,
                0,
                b"x",
                WriteOptions { kill_suidgid: true },
            )
            .unwrap(),
            1
        );
        assert_eq!(
            fs.getattr(&context(), created.entry.inode, Some(created.handle))
                .unwrap()
                .mode,
            0o0777
        );

        let killpriv_ctime = system_time_ms(
            fs.getattr(&context(), created.entry.inode, Some(created.handle))
                .unwrap()
                .ctime,
        );
        fs.flush(&context(), created.handle).unwrap();
        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert!(commits[0].metadata_delta.kill_suidgid);
        assert_eq!(
            commits[0].metadata_delta.ctime_unix_ms,
            Some(killpriv_ctime)
        );
        assert_eq!(meta.inode.lock().unwrap().attributes.mode, 0o0777);
    }

    #[test]
    fn killpriv_zero_write_does_not_clear_mode() {
        let (_temp, meta, fs) = test_fs();
        meta.inode.lock().unwrap().attributes.mode = 0o6777;
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("killpriv-zero.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();

        assert_eq!(
            fs.write_with_options(
                &context(),
                created.handle,
                0,
                b"",
                WriteOptions { kill_suidgid: true },
            )
            .unwrap(),
            0
        );
        assert_eq!(
            fs.getattr(&context(), created.entry.inode, Some(created.handle))
                .unwrap()
                .mode,
            0o6777
        );
        assert_eq!(meta.commit_count(), 0);
    }

    #[test]
    fn killpriv_pending_clear_commits_before_plain_chmod() {
        let (_temp, meta, fs) = test_fs();
        meta.inode.lock().unwrap().attributes.mode = 0o6777;
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("killpriv-chmod.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();

        fs.write_with_options(
            &context(),
            created.handle,
            0,
            b"x",
            WriteOptions { kill_suidgid: true },
        )
        .unwrap();
        let attrs = fs
            .setattr_with_options(
                &context(),
                created.entry.inode,
                Some(created.handle),
                &AttributeChange {
                    mode: Some(0o6755),
                    ..AttributeChange::default()
                },
                SetAttrOptions::default(),
            )
            .unwrap();
        assert_eq!(attrs.mode, 0o6755);
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();

        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert!(commits[0].metadata_delta.kill_suidgid);
        assert_eq!(meta.inode.lock().unwrap().attributes.mode, 0o6755);
    }

    #[test]
    fn killpriv_resize_clears_mode_on_explicit_cause() {
        let (_temp, meta, fs) = test_fs();
        meta.inode.lock().unwrap().attributes.mode = 0o6777;
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("killpriv-resize.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();

        let attrs = fs
            .setattr_with_options(
                &context(),
                created.entry.inode,
                Some(created.handle),
                &AttributeChange {
                    size: Some(4),
                    ..AttributeChange::default()
                },
                SetAttrOptions {
                    kill_suidgid: true,
                    timestamps_now: false,
                },
            )
            .unwrap();
        assert_eq!(attrs.mode, 0o0777);
        fs.flush(&context(), created.handle).unwrap();
        assert!(meta.commits.lock().unwrap()[0].metadata_delta.kill_suidgid);
    }

    #[test]
    fn legacy_kernel_mode_clear_rechecks_current_dfs_state() {
        let (_temp, meta, fs) = test_fs();
        meta.inode.lock().unwrap().attributes.mode = 0o6777;
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("legacy-clear.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let options = SetAttrOptions {
            kill_suidgid: true,
            timestamps_now: false,
        };
        let invalid = AttributeChange {
            mode: Some(0o666),
            ..Default::default()
        };
        assert!(
            fs.setattr_with_options(
                &context(),
                created.entry.inode,
                Some(created.handle),
                &invalid,
                options
            )
            .is_err()
        );
        assert_eq!(
            fs.getattr(&context(), created.entry.inode, Some(created.handle))
                .unwrap()
                .mode,
            0o6777
        );
        let clear = AttributeChange {
            mode: Some(0o777),
            ..Default::default()
        };
        let attrs = fs
            .setattr_with_options(
                &context(),
                created.entry.inode,
                Some(created.handle),
                &clear,
                options,
            )
            .unwrap();
        assert_eq!(attrs.mode, 0o777);
        assert!(
            fs.setattr_with_options(
                &context(),
                created.entry.inode,
                Some(created.handle),
                &clear,
                options
            )
            .is_err()
        );
        fs.flush(&context(), created.handle).unwrap();
        assert!(
            meta.metadata_syncs
                .lock()
                .unwrap()
                .iter()
                .any(|sync| sync.metadata_delta.kill_suidgid)
        );
    }

    #[test]
    fn killpriv_setattr_on_special_inode_uses_normal_meta_attribute_path() {
        let (_temp, meta, fs) = test_fs();
        let entry = fs
            .mknod(
                &context(),
                fs.root_inode(),
                OsStr::new("killpriv-fifo"),
                SpecialFileKind::Fifo,
                0o666,
            )
            .unwrap();

        let attrs = fs
            .setattr_with_options(
                &context(),
                entry.inode,
                None,
                &AttributeChange {
                    mode: Some(0o600),
                    uid: Some(4242),
                    gid: Some(4343),
                    ..AttributeChange::default()
                },
                SetAttrOptions {
                    kill_suidgid: true,
                    timestamps_now: false,
                },
            )
            .unwrap();

        assert!(matches!(
            attrs.kind,
            FileKind::Special(SpecialFileKind::Fifo)
        ));
        assert_eq!(attrs.mode & 0o777, 0o600);
        assert_eq!(attrs.uid, 4242);
        assert_eq!(attrs.gid, 4343);
        assert_eq!(
            meta.commit_count(),
            0,
            "non-regular setattr killpriv cause must not enter DFS chunk/write-lease commit path"
        );
    }

    #[test]
    fn dirty_budget_writeback_splits_streaming_writes_without_per_write_meta_rpc() {
        let (_temp, meta, fs) = test_fs_with_dirty_budget(8);
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("budget-stream.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();

        assert_eq!(
            fs.write(&context(), created.handle, 0, b"abcdefghijklmnop")
                .unwrap(),
            16
        );
        assert_eq!(
            meta.commit_count(),
            2,
            "one large write is staged and committed in budget-sized windows"
        );

        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits[0].metadata_delta.mode, CommitMetadataMode::DataOnly);
        assert_eq!(commits[0].file_version.length, 8);
        assert_eq!(commits[1].metadata_delta.mode, CommitMetadataMode::DataOnly);
        assert_eq!(commits[1].file_version.length, 16);
        assert_eq!(
            commits[1].file_version.parent_version,
            Some(commits[0].file_version.id.clone())
        );
        drop(commits);

        let mut out = [0; 16];
        assert_eq!(
            fs.read(&context(), created.handle, 0, &mut out).unwrap(),
            16
        );
        assert_eq!(&out, b"abcdefghijklmnop");
    }

    #[test]
    fn dirty_budget_repeated_identical_chunks_share_receipt_but_keep_layout_references() {
        let (_temp, meta, fs) = test_fs_with_dirty_budget(DEFAULT_DIRTY_DATA_BUDGET_BYTES);
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("budget-repeated-chunks.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let payload = vec![b'w'; 8192];
        let mut offset = 0u64;

        for size in (1..=8192).rev() {
            assert_eq!(
                fs.write(&context(), created.handle, offset, &payload[..size])
                    .unwrap(),
                size
            );
            offset += size as u64;
            if meta.commit_count() > 0 {
                break;
            }
        }

        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        let commit = &commits[0];
        assert_eq!(commit.file_version.length, DEFAULT_DIRTY_DATA_BUDGET_BYTES);
        assert_eq!(
            commit.layout_root.file_length,
            DEFAULT_DIRTY_DATA_BUDGET_BYTES
        );
        assert_eq!(
            commit.layout_root.inline_extents.len(),
            (DEFAULT_DIRTY_DATA_BUDGET_BYTES / COMMIT_CHUNK_BYTES) as usize
        );
        assert_eq!(
            commit.chunk_receipts.len(),
            1,
            "identical content-addressed chunks need one proof receipt per chunk id"
        );
        let repeated_chunk = commit.layout_root.inline_extents[0].chunk_id.clone();
        assert_eq!(commit.chunk_receipts[0].chunk.id, repeated_chunk);
        for (index, extent) in commit.layout_root.inline_extents.iter().enumerate() {
            assert_eq!(extent.file_offset, index as u64 * COMMIT_CHUNK_BYTES);
            assert_eq!(extent.length, COMMIT_CHUNK_BYTES);
            assert_eq!(extent.chunk_offset, 0);
            assert_eq!(extent.chunk_id, repeated_chunk);
        }
        drop(commits);

        let mut checked = 0u64;
        let mut out = vec![0; 64 * 1024];
        while checked < DEFAULT_DIRTY_DATA_BUDGET_BYTES {
            let read = fs
                .read(&context(), created.handle, checked, &mut out)
                .unwrap();
            assert_eq!(read, out.len());
            assert!(out[..read].iter().all(|byte| *byte == b'w'));
            checked += read as u64;
        }
    }

    #[test]
    fn dirty_budget_counts_payload_not_sparse_holes() {
        let (_temp, meta, fs) = test_fs_with_dirty_budget(8);
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("budget-sparse.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let offset = 8 * 1024 * 1024 * 1024u64;

        assert_eq!(
            fs.write(&context(), created.handle, offset, b"tail")
                .unwrap(),
            4
        );
        assert_eq!(
            meta.commit_count(),
            0,
            "large logical hole must not count as dirty payload memory"
        );
        fs.flush(&context(), created.handle).unwrap();
        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].file_version.length, offset + 4);
        assert_eq!(commits[0].layout_root.inline_extents.len(), 1);
        assert_eq!(commits[0].layout_root.inline_extents[0].file_offset, offset);
        assert_eq!(commits[0].layout_root.inline_extents[0].length, 4);
        assert_eq!(commits[0].chunk_receipts.len(), 1);
    }

    #[test]
    fn dirty_budget_unknown_commit_ack_preserves_exact_request_and_backpressures_next_write() {
        let (_temp, meta, fs) = test_fs_with_dirty_budget(4);
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("budget-retry.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();

        meta.fail_next_commit();
        assert_eq!(
            fs.write(&context(), created.handle, 0, b"data").unwrap(),
            4,
            "buffered write reports bytes admitted into the exact pending request"
        );
        let failed = meta.failed_commits.lock().unwrap()[0].clone();

        assert_eq!(
            fs.write(&context(), created.handle, 4, b"!")
                .unwrap_err()
                .code(),
            afs_error::NODE_VFS_UNAVAILABLE,
            "the unknown commit is reported before admitting new mutation"
        );
        assert_eq!(fs.write(&context(), created.handle, 4, b"!").unwrap(), 1);
        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(failed.operation_id, commits[0].operation_id);
        assert_eq!(failed.file_version.id, commits[0].file_version.id);
        assert_eq!(failed.layout_root.id, commits[0].layout_root.id);
        assert_eq!(commits[0].file_version.length, 4);
        drop(commits);

        fs.flush(&context(), created.handle).unwrap();
        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[1].file_version.length, 5);
    }

    #[test]
    fn dirty_budget_returns_admitted_bytes_and_latches_later_error() {
        let (_temp, meta, fs) = test_fs_with_dirty_budget(4);
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("budget-partial.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();

        meta.fail_commit_after_successes(1, unavailable("injected second commit failure"));
        assert_eq!(
            fs.write(&context(), created.handle, 0, b"abcdefgh")
                .unwrap(),
            8,
            "buffered write reports every byte admitted into dirty/pending state"
        );
        assert_eq!(meta.commit_count(), 1);
        assert_eq!(meta.failed_commits.lock().unwrap().len(), 1);
        assert_eq!(
            fs.getattr(&context(), created.entry.inode, Some(created.handle))
                .unwrap()
                .size,
            8,
            "visible length follows the returned admitted byte count"
        );
        assert_eq!(
            fs.write(&context(), created.handle, 8, b"!")
                .unwrap_err()
                .code(),
            afs_error::NODE_VFS_UNAVAILABLE,
            "the uncertain later window is reported on the next operation"
        );
    }

    #[test]
    fn dirty_budget_sync_write_error_does_not_report_unconfirmed_bytes() {
        let (_temp, meta, fs) = test_fs_with_dirty_budget(4);
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("budget-sync-error.bin"),
                0o640,
                libc::O_RDWR | libc::O_SYNC,
            )
            .unwrap();

        meta.fail_commit_after_successes(1, unavailable("injected second commit failure"));
        assert_eq!(
            fs.write(&context(), created.handle, 0, b"abcdefgh")
                .unwrap_err()
                .code(),
            afs_error::NODE_VFS_UNAVAILABLE,
            "O_SYNC cannot report bytes whose dirty-budget commit is unknown"
        );
        assert_eq!(meta.commit_count(), 1);
        assert_eq!(meta.failed_commits.lock().unwrap().len(), 1);
    }

    #[test]
    fn dirty_budget_append_write_serializes_concurrent_resize() {
        let (_temp, meta, fs) = test_fs_with_dirty_budget(4);
        let fs = Arc::new(fs);
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("append-resize.bin"),
                0o640,
                libc::O_RDWR | libc::O_APPEND,
            )
            .unwrap();
        let pause = meta.pause_commit_after_successes(0);

        let writer_fs = fs.clone();
        let writer_handle = created.handle;
        let inode = created.entry.inode;
        let handle = created.handle;
        let writer =
            std::thread::spawn(move || writer_fs.write(&context(), writer_handle, 0, b"abcdefgh"));
        pause.wait_until_reached();

        let resizer_fs = fs.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let resizer = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            resizer_fs.setattr(
                &context(),
                inode,
                Some(handle),
                &AttributeChange {
                    size: Some(1),
                    ..AttributeChange::default()
                },
            )
        });
        started_rx.recv().unwrap();
        pause.release();

        assert_eq!(writer.join().unwrap().unwrap(), 8);
        assert_eq!(resizer.join().unwrap().unwrap().size, 1);
        assert_eq!(
            fs.getattr(&context(), inode, Some(handle)).unwrap().size,
            1,
            "resize runs after the complete append write, not between budget windows"
        );
        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].file_version.length, 4);
        assert_eq!(commits[1].file_version.length, 8);
    }

    #[test]
    fn dirty_budget_append_write_serializes_concurrent_full_sync() {
        let (_temp, meta, fs) = test_fs_with_dirty_budget(4);
        let fs = Arc::new(fs);
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("append-sync.bin"),
                0o640,
                libc::O_RDWR | libc::O_APPEND,
            )
            .unwrap();
        let pause = meta.pause_commit_after_successes(0);

        let writer_fs = fs.clone();
        let writer_handle = created.handle;
        let sync_handle = created.handle;
        let writer =
            std::thread::spawn(move || writer_fs.write(&context(), writer_handle, 0, b"abcdefgh"));
        pause.wait_until_reached();

        let sync_fs = fs.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let syncer = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            sync_fs.fsync(&context(), sync_handle, SyncMode::Full)
        });
        started_rx.recv().unwrap();
        pause.release();

        assert_eq!(writer.join().unwrap().unwrap(), 8);
        syncer.join().unwrap().unwrap();

        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].file_version.length, 4);
        assert_eq!(commits[1].file_version.length, 8);
        let final_head = Some(commits[1].file_version.id.clone());
        drop(commits);

        let metadata_syncs = meta.metadata_syncs.lock().unwrap();
        assert_eq!(metadata_syncs.len(), 1);
        assert_eq!(
            metadata_syncs[0].expected_head_version, final_head,
            "full fsync must synchronize the completed append version, not a mid-write budget window"
        );
    }

    #[test]
    fn mknod_creates_special_inode_without_write_session() {
        let (_temp, _meta, fs) = test_fs();
        let entry = fs
            .mknod(
                &context(),
                fs.root_inode(),
                OsStr::new("pipe"),
                SpecialFileKind::Fifo,
                0o666,
            )
            .unwrap();
        assert_eq!(
            entry.attributes.kind,
            FileKind::Special(SpecialFileKind::Fifo)
        );
        assert_eq!(entry.attributes.size, 0);
        assert_eq!(entry.attributes.blocks, 0);
        assert_eq!(entry.attributes.mode, 0o666);
        assert_eq!(
            fs.open(&context(), entry.inode, libc::O_RDONLY)
                .unwrap_err()
                .code(),
            afs_error::IO_NOT_SUPPORTED
        );
    }

    #[test]
    fn close_flush_commits_dirty_data_without_an_explicit_sync() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("close.txt"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"closed").unwrap();
        assert_eq!(meta.commit_count(), 0, "write does not create a version");
        fs.flush(&context(), created.handle).unwrap();
        assert_eq!(meta.commit_count(), 1, "successful close flush must commit");
        assert_eq!(meta.commits.lock().unwrap()[0].file_version.length, 6);
        fs.flush(&context(), created.handle).unwrap();
        assert_eq!(
            meta.commit_count(),
            1,
            "unchanged flush must not create a version"
        );
        fs.release(&context(), created.handle).unwrap();
        let reader = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();
        let mut out = [0; 6];
        assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 6);
        assert_eq!(&out, b"closed");
        fs.release(&context(), reader).unwrap();
    }

    #[test]
    fn close_flush_reports_a_commit_error_instead_of_success() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("failed-close.txt"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"pending").unwrap();
        *meta.next_commit_error.lock().unwrap() = Some(unavailable("injected ACK loss"));
        assert!(fs.flush(&context(), created.handle).is_err());
        assert_eq!(meta.commit_count(), 0);
        fs.flush(&context(), created.handle).unwrap();
        assert_eq!(meta.commit_count(), 1);
        let failed = meta.failed_commits.lock().unwrap();
        let applied = meta.commits.lock().unwrap();
        assert_eq!(failed[0].operation_id, applied[0].operation_id);
        assert_eq!(failed[0].file_version, applied[0].file_version);
        assert_eq!(failed[0].layout_root, applied[0].layout_root);
    }

    #[test]
    fn readonly_close_does_not_commit_another_handles_dirty_writes() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("read-close.txt"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let reader = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();
        fs.write(&context(), created.handle, 0, b"dirty").unwrap();
        fs.flush(&context(), reader).unwrap();
        fs.release(&context(), reader).unwrap();
        assert_eq!(meta.commit_count(), 0);
        fs.flush(&context(), created.handle).unwrap();
        assert_eq!(meta.commit_count(), 1);
        assert_eq!(
            meta.commits.lock().unwrap()[0].metadata_delta.mode,
            CommitMetadataMode::DataOnly
        );
    }

    #[test]
    fn dsync_write_commits_data_without_promoting_to_full_inode_sync() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("dsync.txt"),
                0o640,
                libc::O_RDWR | libc::O_DSYNC,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"data").unwrap();
        assert_eq!(meta.commit_count(), 1);
        assert_eq!(
            meta.commits.lock().unwrap()[0].metadata_delta.mode,
            CommitMetadataMode::DataOnly
        );
        fs.flush(&context(), created.handle).unwrap();
        assert_eq!(meta.commit_count(), 1);
    }

    #[test]
    fn readonly_handles_share_the_current_mount_dirty_and_committed_view() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("hello.txt"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();

        assert_eq!(
            fs.write(&context(), created.handle, 0, b"hello").unwrap(),
            5
        );
        assert_eq!(
            meta.commit_count(),
            0,
            "ordinary write does not create FileVersion"
        );

        let reader = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();
        let mut out = [0; 5];
        assert_eq!(
            fs.read(&context(), reader, 0, &mut out).unwrap(),
            5,
            "ordinary readonly handles share the mount's accepted dirty view"
        );
        assert_eq!(&out, b"hello");
        assert_eq!(
            fs.getattr(&context(), created.entry.inode, Some(reader))
                .unwrap()
                .size,
            5
        );

        let writer = fs
            .open(&context(), created.entry.inode, libc::O_RDWR)
            .unwrap();
        let mut writer_view = [0; 5];
        assert_eq!(fs.read(&context(), writer, 0, &mut writer_view).unwrap(), 5);
        assert_eq!(
            &writer_view, b"hello",
            "writer must keep seeing owner dirty"
        );
        fs.release(&context(), created.handle).unwrap();
        assert_eq!(
            meta.commit_count(),
            0,
            "release must not synchronously commit"
        );

        fs.fsync(&context(), writer, SyncMode::DataOnly).unwrap();
        assert_eq!(meta.commit_count(), 1);
        assert_eq!(
            meta.commits.lock().unwrap()[0].metadata_delta.mode,
            CommitMetadataMode::DataOnly
        );
        let data_version = meta.inode.lock().unwrap().head_version.clone();
        fs.fsync(&context(), writer, SyncMode::Full).unwrap();
        assert_eq!(
            meta.commit_count(),
            1,
            "fsync after fdatasync must sync inode metadata without inventing another FileVersion"
        );
        assert_eq!(meta.inode.lock().unwrap().head_version, data_version);
        let committed_reader = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();
        let mut committed = [0; 5];
        assert_eq!(
            fs.read(&context(), committed_reader, 0, &mut committed)
                .unwrap(),
            5
        );
        assert_eq!(&committed, b"hello");

        fs.write(&context(), writer, 0, b"H").unwrap();
        assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 5);
        assert_eq!(
            &out, b"Hello",
            "accepted overwrites are visible before sync"
        );
        fs.fsync(&context(), writer, SyncMode::Full).unwrap();
        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[1].metadata_delta.mode, CommitMetadataMode::Full);
        assert_eq!(
            commits[1].file_version.parent_version,
            Some(commits[0].file_version.id.clone())
        );
        drop(commits);

        let mut latest = [0; 5];
        assert_eq!(fs.read(&context(), reader, 0, &mut latest).unwrap(), 5);
        assert_eq!(&latest, b"Hello");
        assert_eq!(
            fs.read(&context(), committed_reader, 0, &mut latest)
                .unwrap(),
            5
        );
        assert_eq!(&latest, b"Hello");
        let fresh_reader = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();
        assert_eq!(
            fs.read(&context(), fresh_reader, 0, &mut latest).unwrap(),
            5
        );
        assert_eq!(&latest, b"Hello");
        fs.release(&context(), writer).unwrap();
        fs.release(&context(), reader).unwrap();
        fs.release(&context(), committed_reader).unwrap();
        fs.release(&context(), fresh_reader).unwrap();
        assert_eq!(fs.writeback_pending().unwrap(), 0);
    }

    #[test]
    fn previously_open_readonly_handle_tracks_append_and_shrink_grow() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("view.bin"),
                0o600,
                libc::O_RDWR,
            )
            .unwrap();
        let reader = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();
        let mut out = [0; 8];
        assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 0);
        fs.write(&context(), created.handle, 0, b"abcd").unwrap();
        fs.write(&context(), created.handle, 4, b"efgh").unwrap();
        assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 8);
        assert_eq!(&out, b"abcdefgh");
        for length in [3, 8] {
            fs.setattr(
                &context(),
                created.entry.inode,
                Some(created.handle),
                &AttributeChange {
                    size: Some(length),
                    ..AttributeChange::default()
                },
            )
            .unwrap();
        }
        assert_eq!(meta.commit_count(), 0);
        assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 8);
        assert_eq!(&out, b"abc\0\0\0\0\0");
        assert_eq!(
            fs.getattr(&context(), created.entry.inode, Some(reader))
                .unwrap()
                .size,
            8
        );
        fs.flush(&context(), created.handle).unwrap();
        assert_eq!(fs.read(&context(), reader, 0, &mut out).unwrap(), 8);
        assert_eq!(&out, b"abc\0\0\0\0\0");
    }

    #[test]
    fn writeonly_handle_cannot_read_the_inode_view() {
        let (_temp, _meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("writeonly.bin"),
                0o600,
                libc::O_WRONLY,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"secret").unwrap();
        assert_eq!(
            fs.read(&context(), created.handle, 0, &mut [0; 8])
                .unwrap_err()
                .code(),
            afs_error::IO_BAD_FILE_DESCRIPTOR
        );
    }

    #[test]
    fn metadata_changes_refresh_active_write_state_after_close() {
        let (_temp, _meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("attrs.bin"),
                0o644,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"mode").unwrap();
        fs.flush(&context(), created.handle).unwrap();
        fs.release(&context(), created.handle).unwrap();

        let changed = AttributeChange {
            mode: Some(libc::S_IFREG | 0o640),
            uid: Some(4242),
            gid: Some(60002),
            ..AttributeChange::default()
        };
        fs.setattr(&context(), created.entry.inode, None, &changed)
            .unwrap();
        let attrs = fs.getattr(&context(), created.entry.inode, None).unwrap();
        assert_eq!(attrs.mode & 0o777, 0o640);
        assert_eq!(attrs.uid, 4242);
        assert_eq!(attrs.gid, 60002);
    }

    #[test]
    fn hardlink_refreshes_active_write_state_nlink() {
        let (_temp, _meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("link-a"),
                0o644,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"hardlink-data")
            .unwrap();
        fs.flush(&context(), created.handle).unwrap();

        let linked = fs
            .link(
                &context(),
                created.entry.inode,
                fs.root_inode(),
                OsStr::new("link-b"),
            )
            .unwrap();
        assert_eq!(linked.attributes.nlink, 2);
        assert_eq!(
            fs.getattr(&context(), created.entry.inode, Some(created.handle))
                .unwrap()
                .nlink,
            2
        );
        assert_eq!(
            fs.getattr(&context(), created.entry.inode, None)
                .unwrap()
                .nlink,
            2
        );
    }

    #[test]
    fn namespace_and_xattr_refresh_preserve_dirty_write_mtime_for_full_sync() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("dirty-mtime.bin"),
                0o644,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"mtime").unwrap();
        let dirty_mtime = {
            let state = fs
                .write_state(&meta.inode.lock().unwrap().inode_id)
                .unwrap()
                .unwrap();
            state.lock().unwrap().inode.attributes.mtime_unix_ms
        };

        fs.link(
            &context(),
            created.entry.inode,
            fs.root_inode(),
            OsStr::new("dirty-mtime-link"),
        )
        .unwrap();
        fs.setxattr(
            &context(),
            created.entry.inode,
            OsStr::new("user.dirty_mtime"),
            b"value",
            0,
        )
        .unwrap();
        fs.setattr(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &AttributeChange {
                mode: Some(libc::S_IFREG | 0o640),
                ..AttributeChange::default()
            },
        )
        .unwrap();
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();

        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].metadata_delta.mode, CommitMetadataMode::Full);
        assert_eq!(commits[0].metadata_delta.mtime_unix_ms, Some(dirty_mtime));
    }

    #[test]
    fn explicit_mtime_refresh_overrides_dirty_write_mtime_for_full_sync() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("explicit-mtime.bin"),
                0o644,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"mtime").unwrap();
        let explicit = UNIX_EPOCH + std::time::Duration::from_millis(42);
        fs.setattr(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &AttributeChange {
                mtime: Some(explicit),
                ..AttributeChange::default()
            },
        )
        .unwrap();
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();

        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0].metadata_delta.mode, CommitMetadataMode::Full);
        assert_eq!(commits[0].metadata_delta.mtime_unix_ms, Some(42));
    }

    #[test]
    fn getattr_refreshes_active_write_state_from_meta_without_losing_dirty_mtime() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("remote-fresh.bin"),
                0o644,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"fresh").unwrap();
        let dirty_mtime = {
            let state = fs
                .write_state(&meta.inode.lock().unwrap().inode_id)
                .unwrap()
                .unwrap();
            state.lock().unwrap().inode.attributes.mtime_unix_ms
        };
        {
            let mut inode = meta.inode.lock().unwrap();
            inode.attributes.mode = libc::S_IFREG | 0o660;
            inode.attributes.gid = 60002;
            inode.attributes.nlink = 2;
            inode.attributes.mtime_unix_ms = 7;
            inode.attributes.ctime_unix_ms = inode.attributes.ctime_unix_ms.saturating_add(100);
            inode.revision = inode.revision.saturating_add(1);
        }

        let attrs = fs
            .getattr(&context(), created.entry.inode, Some(created.handle))
            .unwrap();
        assert_eq!(attrs.mode & 0o777, 0o660);
        assert_eq!(attrs.gid, 60002);
        assert_eq!(attrs.nlink, 2);
        assert_eq!(system_time_ms(attrs.mtime), dirty_mtime);
    }

    #[test]
    fn sparse_write_materializes_only_payload_chunk() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("sparse.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let offset = 1024 * 1024;
        assert_eq!(
            fs.write(&context(), created.handle, offset, b"tail")
                .unwrap(),
            4
        );

        let attrs = fs
            .getattr(&context(), created.entry.inode, Some(created.handle))
            .unwrap();
        assert_eq!(attrs.size, offset + 4);
        let mut head = [1; 16];
        assert_eq!(
            fs.read(&context(), created.handle, 0, &mut head).unwrap(),
            head.len()
        );
        assert_eq!(head, [0; 16]);

        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        let commits = meta.commits.lock().unwrap();
        let commit = commits.last().unwrap();
        assert_eq!(commit.file_version.length, offset + 4);
        assert_eq!(commit.layout_root.file_length, offset + 4);
        assert_eq!(commit.layout_root.inline_extents.len(), 1);
        assert_eq!(commit.layout_root.inline_extents[0].file_offset, offset);
        assert_eq!(commit.layout_root.inline_extents[0].length, 4);
        assert_eq!(commit.chunk_receipts.len(), 1);
    }

    #[test]
    fn handle_resize_shrink_then_grow_masks_old_tail_without_new_chunk() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("resize.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"abcdefgh")
            .unwrap();
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        let before_seq = fs
            .handle_snapshot(created.handle)
            .unwrap()
            .write_session
            .unwrap()
            .local
            .last_accepted_seq;

        let shrink = AttributeChange {
            size: Some(4),
            ..AttributeChange::default()
        };
        let grow = AttributeChange {
            size: Some(8),
            ..AttributeChange::default()
        };
        fs.setattr(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &shrink,
        )
        .unwrap();
        let attrs = fs
            .setattr(&context(), created.entry.inode, Some(created.handle), &grow)
            .unwrap();
        assert_eq!(attrs.size, 8);
        let after_seq = fs
            .handle_snapshot(created.handle)
            .unwrap()
            .write_session
            .unwrap()
            .local
            .last_accepted_seq;
        assert!(after_seq > before_seq);

        let mut visible = [1; 8];
        assert_eq!(
            fs.read(&context(), created.handle, 0, &mut visible)
                .unwrap(),
            visible.len()
        );
        assert_eq!(&visible, b"abcd\0\0\0\0");
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();

        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 2);
        let resized = commits.last().unwrap();
        assert_eq!(resized.file_version.length, 8);
        assert!(resized.chunk_receipts.is_empty());
        assert_eq!(resized.layout_root.inline_extents.len(), 1);
        assert_eq!(resized.layout_root.inline_extents[0].length, 4);
        drop(commits);

        fs.setattr(&context(), created.entry.inode, Some(created.handle), &grow)
            .unwrap();
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        assert_eq!(
            meta.commit_count(),
            2,
            "same-length resize must not create another FileVersion"
        );
    }

    #[test]
    fn path_resize_uses_inode_state_without_opening_a_writer() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("path-resize.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"abcdefgh")
            .unwrap();
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs.release(&context(), created.handle).unwrap();

        let resize = AttributeChange {
            size: Some(3),
            ..AttributeChange::default()
        };
        let attrs = fs
            .setattr(&context(), created.entry.inode, None, &resize)
            .unwrap();
        assert_eq!(attrs.size, 3);
        let state = fs
            .write_state(&meta.inode.lock().unwrap().inode_id)
            .unwrap()
            .unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.open_writers, 0);
        assert!(state.last_writer_background_requested);
        drop(state);

        let reader = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();
        let readonly_error = fs
            .setattr(
                &context(),
                created.entry.inode,
                Some(reader),
                &AttributeChange {
                    size: Some(2),
                    ..AttributeChange::default()
                },
            )
            .unwrap_err();
        assert_eq!(readonly_error.code(), afs_error::IO_BAD_FILE_DESCRIPTOR);
        let mut visible = [0; 8];
        assert_eq!(fs.read(&context(), reader, 0, &mut visible).unwrap(), 3);
        assert_eq!(&visible[..3], b"abc");
        assert_eq!(
            fs.getattr(&context(), created.entry.inode, Some(reader))
                .unwrap()
                .size,
            3
        );

        assert_eq!(fs.writeback_pending().unwrap(), 1);
        assert_eq!(meta.commit_count(), 2);
        let commits = meta.commits.lock().unwrap();
        let resized = commits.last().unwrap();
        assert_eq!(resized.file_version.length, 3);
        assert!(resized.chunk_receipts.is_empty());
    }

    #[test]
    fn closed_metadata_dirty_write_state_reopen_adopts_fresh_epoch_without_old_renewal() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("clean-reopen-fresh-lease.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        assert_eq!(fs.write(&context(), created.handle, 0, b"base").unwrap(), 4);
        fs.flush(&context(), created.handle).unwrap();
        fs.release(&context(), created.handle).unwrap();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        {
            let mut state = state.lock().unwrap();
            state.write_lease.expires_at_unix_ms = now_unix_ms().saturating_sub(1);
            assert!(!DistributedFs::write_state_can_adopt_fresh_lease(&state));
            assert!(DistributedFs::write_state_can_rebind_fresh_lease_preserving_metadata(&state));
        }
        let fresh_epoch = {
            let mut lease = meta.lease.lock().unwrap();
            lease.lease_epoch = lease.lease_epoch.saturating_add(1);
            lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
            lease.lease_epoch
        };
        meta.fail_next_renew_with(Error::coded(
            afs_error::META_DFS_CONFLICT,
            "old clean lease must be replaced by fresh Meta open before renewal",
        ));
        let renews_before = meta.renew_call_count();

        let handle = fs
            .open(&context(), created.entry.inode, libc::O_RDWR)
            .unwrap();

        assert_eq!(meta.renew_call_count(), renews_before);
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.write_lease.lease_epoch, fresh_epoch);
        assert!(state.write_lease.expires_at_unix_ms > now_unix_ms());
        drop(state);
        assert_eq!(fs.write(&context(), handle, 4, b"x").unwrap(), 1);
    }

    #[test]
    fn clean_retained_write_state_path_resize_reacquires_fresh_epoch() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("clean-resize-fresh-lease.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.release(&context(), created.handle).unwrap();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        {
            let mut state = state.lock().unwrap();
            state.write_lease.expires_at_unix_ms = now_unix_ms().saturating_sub(1);
            assert!(DistributedFs::write_state_can_adopt_fresh_lease(&state));
        }
        let fresh_epoch = {
            let mut lease = meta.lease.lock().unwrap();
            lease.lease_epoch = lease.lease_epoch.saturating_add(1);
            lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
            lease.lease_epoch
        };
        meta.fail_next_renew_with(Error::coded(
            afs_error::META_DFS_CONFLICT,
            "path resize must not renew the old clean lease",
        ));
        let renews_before = meta.renew_call_count();

        let attrs = fs
            .setattr(
                &context(),
                created.entry.inode,
                None,
                &AttributeChange {
                    size: Some(5),
                    ..AttributeChange::default()
                },
            )
            .unwrap();

        assert_eq!(attrs.size, 5);
        assert_eq!(meta.renew_call_count(), renews_before);
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.write_lease.lease_epoch, fresh_epoch);
        assert_eq!(state.logical_length, 5);
        assert!(state.dirty);
    }

    #[test]
    fn metadata_dirty_write_state_rejects_fresh_epoch_after_head_change() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("metadata-dirty-reject-changed-head.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        assert_eq!(fs.write(&context(), created.handle, 0, b"base").unwrap(), 4);
        fs.flush(&context(), created.handle).unwrap();
        fs.release(&context(), created.handle).unwrap();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        {
            let mut state = state.lock().unwrap();
            state.write_lease.expires_at_unix_ms = now_unix_ms().saturating_sub(1);
            assert!(DistributedFs::write_state_can_rebind_fresh_lease_preserving_metadata(&state));
        }
        {
            let mut lease = meta.lease.lock().unwrap();
            lease.lease_epoch = lease.lease_epoch.saturating_add(1);
            lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
        }
        meta.advance_namespace_revision();

        let error = fs
            .open(&context(), created.entry.inode, libc::O_RDWR)
            .expect_err("metadata-dirty state must not rebind across a changed Meta inode");

        assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
        let state = state.lock().unwrap();
        assert!(state.metadata_dirty);
        assert_eq!(state.write_lease.lease_epoch, 1);
    }

    #[test]
    fn clean_retained_write_state_rejects_delayed_older_epoch() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("clean-reject-old-epoch.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.release(&context(), created.handle).unwrap();
        let inode = meta.inode.lock().unwrap().clone();
        let older_lease = meta.lease.lock().unwrap().clone();
        let mut newer_lease = older_lease.clone();
        newer_lease.lease_epoch = newer_lease.lease_epoch.saturating_add(1);
        newer_lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
        fs.install_write_state(inode.clone(), newer_lease.clone())
            .unwrap();

        let error = fs
            .install_write_state(inode, older_lease)
            .expect_err("a delayed old open response must not downgrade a newer clean state");

        assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
        let state = fs.write_state(&newer_lease.inode_id).unwrap().unwrap();
        assert_eq!(
            state.lock().unwrap().write_lease.lease_epoch,
            newer_lease.lease_epoch
        );
    }

    #[test]
    fn dirty_write_state_rejects_fresh_epoch_without_losing_dirty_data() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("dirty-reject-fresh-epoch.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        assert_eq!(
            fs.write(&context(), created.handle, 0, b"dirty").unwrap(),
            5
        );
        let inode = meta.inode.lock().unwrap().clone();
        let inode_id = inode.inode_id.clone();
        let mut fresh_lease = meta.lease.lock().unwrap().clone();
        fresh_lease.lease_epoch = fresh_lease.lease_epoch.saturating_add(1);
        fresh_lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);

        let error = fs
            .install_write_state(inode, fresh_lease)
            .expect_err("dirty state must not be silently reattached to a new lease epoch");

        assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.write_lease.lease_epoch, 1);
        assert!(state.dirty);
        assert_eq!(state.dirty_extents.dirty_data_bytes(), 5);
    }

    #[test]
    fn remote_owner_write_after_local_close_rebinds_metadata_dirty_state() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("remote-after-close.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        assert_eq!(
            fs.write(&context(), created.handle, 0, b"base-data")
                .unwrap(),
            9
        );
        fs.flush(&context(), created.handle).unwrap();
        fs.release(&context(), created.handle).unwrap();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        {
            let mut state = state.lock().unwrap();
            state.write_lease.expires_at_unix_ms = now_unix_ms().saturating_sub(1);
            assert!(state.metadata_dirty);
            assert!(DistributedFs::write_state_can_rebind_fresh_lease_preserving_metadata(&state));
        }
        let fresh_epoch = {
            let mut lease = meta.lease.lock().unwrap();
            lease.lease_epoch = lease.lease_epoch.saturating_add(1);
            lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
            lease.lease_epoch
        };

        let open = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerOpenRequest {
                namespace_id: "default".into(),
                inode_id: "inode:test".into(),
                owner_node_id: "node-a".into(),
                owner_session_id: "session-a".into(),
                lease_epoch: fresh_epoch,
                caller_session_id: "session-b".into(),
                open_flags: libc::O_RDWR,
                kill_suidgid: false,
                open_seq: 1,
            },
        )
        .unwrap();
        let handle = open.handle.expect("owner open returns handle");
        let data_handle = DistributedFs::data_owner_handle(&handle);
        let write = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
            &fs,
            "node-b",
            afs_protocol::node_data::DfsOwnerWriteRequest {
                handle: Some(data_handle.clone()),
                operation_id: "remote-after-close-write".into(),
                offset: 0,
                data: b"remote-final".to_vec(),
                append: false,
                kill_suidgid: false,
            },
        )
        .unwrap();
        assert_eq!(write.written, 12);
        <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::resize(
            &fs,
            "node-b",
            afs_protocol::node_data::DfsOwnerResizeRequest {
                handle: Some(data_handle.clone()),
                operation_id: "remote-after-close-resize".into(),
                length: 12,
                kill_suidgid: false,
            },
        )
        .unwrap();
        <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::sync(
            &fs,
            "node-b",
            afs_protocol::node_data::DfsOwnerSyncRequest {
                handle: Some(data_handle),
                operation_id: "remote-after-close-sync".into(),
                through_write_seq: write.accepted_write_seq,
                data_only: false,
            },
        )
        .unwrap();
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseRequest {
                handle: Some(handle),
            },
        )
        .unwrap();

        let state = state.lock().unwrap();
        assert_eq!(state.write_lease.lease_epoch, fresh_epoch);
        assert_eq!(state.logical_length, 12);
        assert!(!state.metadata_dirty);
    }

    #[test]
    fn path_resize_to_remote_owner_uses_owner_rpc_not_local_lease() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("remote-path-resize.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        assert_eq!(
            fs_a.write(&context(), created.handle, 0, b"abcdef")
                .unwrap(),
            6
        );
        fs_a.flush(&context(), created.handle).unwrap();
        fs_a.release(&context(), created.handle).unwrap();
        assert_eq!(meta.commit_count(), 1);

        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        }));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();

        let attrs = fs_b
            .setattr(
                &context(),
                inode_b,
                None,
                &AttributeChange {
                    size: Some(3),
                    ..AttributeChange::default()
                },
            )
            .unwrap();

        assert_eq!(attrs.size, 3);
        assert_eq!(meta.commit_count(), 2);
        let state = fs_a.write_state(&inode_id).unwrap().unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.logical_length, 3);
        assert!(!state.dirty);
    }

    #[test]
    fn temporary_remote_resize_preserves_existing_remote_provider() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("remote-provider-preserve.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        assert_eq!(
            fs_a.write(&context(), created.handle, 0, b"abcdef")
                .unwrap(),
            6
        );
        fs_a.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();

        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        }));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let handle = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap();
        let provider_before = fs_b
            .current_remote_provider(&inode_id)
            .unwrap()
            .expect("remote open installs provider");

        let attrs = fs_b
            .setattr(
                &context(),
                inode_b,
                None,
                &AttributeChange {
                    size: Some(3),
                    ..AttributeChange::default()
                },
            )
            .unwrap();
        assert_eq!(attrs.size, 3);
        let provider_after = fs_b
            .current_remote_provider(&inode_id)
            .unwrap()
            .expect("temporary resize restores existing provider");
        assert!(DistributedFs::same_remote_provider(
            &provider_after,
            &provider_before
        ));

        assert_eq!(fs_b.write(&context(), handle, 0, b"XYZ").unwrap(), 3);
        assert_eq!(fs_b.getattr(&context(), inode_b, None).unwrap().size, 3);
        let reader = fs_b.open(&context(), inode_b, libc::O_RDONLY).unwrap();
        assert_eq!(
            fs_b.getattr(&context(), inode_b, Some(reader))
                .unwrap()
                .size,
            3
        );
        let mut path_data = [0; 3];
        assert_eq!(fs_b.read(&context(), reader, 0, &mut path_data).unwrap(), 3);
        assert_eq!(&path_data, b"XYZ");
        fs_b.release(&context(), reader).unwrap();

        let mut data = [0; 3];
        assert_eq!(fs_b.read(&context(), handle, 0, &mut data).unwrap(), 3);
        assert_eq!(&data, b"XYZ");

        fs_b.release(&context(), handle).unwrap();
        assert!(fs_b.current_remote_provider(&inode_id).unwrap().is_none());
    }

    #[test]
    fn remote_release_failure_after_companion_close_still_removes_provider() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("remote-release-failure-cleans-provider.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.write(&context(), created.handle, 0, b"abcdef")
            .unwrap();
        fs_a.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();

        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        }));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let writer = fs_b.open(&context(), inode_b, libc::O_WRONLY).unwrap();
        let provider = fs_b
            .current_remote_provider(&inode_id)
            .unwrap()
            .expect("remote open installs provider");
        assert!(provider.read_handle.is_some());
        let owner_writer = DistributedFs::file_handle_from_owner(&provider.handle).unwrap();
        fs_a.handles
            .lock()
            .unwrap()
            .remove(&owner_writer.0)
            .unwrap();

        fs_b.release(&context(), writer).unwrap();
        assert!(fs_b.current_remote_provider(&inode_id).unwrap().is_none());
        assert!(fs_a.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn paused_companion_release_retires_provider_before_fresh_read() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("remote-release-race.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.write(&context(), created.handle, 0, b"abcdef")
            .unwrap();
        fs_a.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();

        let chunks_b = fs_a.chunk_store.clone();
        let read_engine_b = fs_a.read_engine.clone();
        let pause = Arc::new(OwnerOpenFinishPause::new());
        let loopback = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
            loopback,
            vec![ScriptedReleaseAction::DelegateThenPause(pause.clone())],
        ));
        let remote_owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
        let fs_b = Arc::new(
            DistributedFs::new(
                NamespaceId::new("default"),
                "node-b",
                "session-b",
                meta.clone(),
                chunks_b,
                read_engine_b,
            )
            .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
                owner: remote_owner,
            })),
        );
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let writer = fs_b
            .open(&context(), inode_b, libc::O_WRONLY | libc::O_TRUNC)
            .unwrap();

        assert_eq!(fs_b.write(&context(), writer, 0, b"XYZ").unwrap(), 3);
        fs_b.fsync(&context(), writer, SyncMode::Full).unwrap();

        let release_fs = fs_b.clone();
        let releaser = std::thread::spawn(move || release_fs.release(&context(), writer));
        pause.wait_until_reached();

        assert!(
            fs_b.current_remote_provider(&inode_id).unwrap().is_none(),
            "provider must be retired before the owner release RPC can expose a closed handle"
        );
        let reader = fs_b.open(&context(), inode_b, libc::O_RDONLY).unwrap();
        let mut data = [0; 3];
        assert_eq!(fs_b.read(&context(), reader, 0, &mut data).unwrap(), 3);
        assert_eq!(&data, b"XYZ");
        fs_b.release(&context(), reader).unwrap();

        pause.resume();
        releaser.join().unwrap().unwrap();
        assert_eq!(scripted.release_call_count(), 2);
        assert!(fs_a.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn retiring_provider_waits_for_inflight_fresh_read_before_owner_release() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("remote-provider-inflight-read.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.write(&context(), created.handle, 0, b"abcdef")
            .unwrap();
        fs_a.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();

        let chunks_b = fs_a.chunk_store.clone();
        let read_engine_b = fs_a.read_engine.clone();
        let loopback = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(loopback, Vec::new()));
        let read_pause = Arc::new(OwnerOpenFinishPause::new());
        scripted.set_read_pause(read_pause.clone());
        let remote_owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
        let fs_b = Arc::new(
            DistributedFs::new(
                NamespaceId::new("default"),
                "node-b",
                "session-b",
                meta.clone(),
                chunks_b,
                read_engine_b,
            )
            .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
                owner: remote_owner,
            })),
        );
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let writer = fs_b
            .open(&context(), inode_b, libc::O_WRONLY | libc::O_TRUNC)
            .unwrap();
        fs_b.write(&context(), writer, 0, b"XYZ").unwrap();
        fs_b.fsync(&context(), writer, SyncMode::Full).unwrap();

        let reader = fs_b.open(&context(), inode_b, libc::O_RDONLY).unwrap();
        let reader_fs = fs_b.clone();
        let reader_thread = std::thread::spawn(move || {
            let mut data = [0; 3];
            let read = reader_fs.read(&context(), reader, 0, &mut data);
            (read, data)
        });
        read_pause.wait_until_reached();

        let release_fs = fs_b.clone();
        let (release_done_tx, release_done_rx) = std::sync::mpsc::channel();
        let releaser = std::thread::spawn(move || {
            let result = release_fs.release(&context(), writer);
            release_done_tx.send(()).unwrap();
            result
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while fs_b.current_remote_provider(&inode_id).unwrap().is_some() {
            assert!(
                std::time::Instant::now() < deadline,
                "provider was not retired while an admitted read was paused"
            );
            std::thread::yield_now();
        }
        assert_eq!(
            release_done_rx.recv_timeout(std::time::Duration::from_millis(100)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout),
            "provider release must wait for an already admitted provider read"
        );

        read_pause.resume();
        let (read, data) = reader_thread.join().unwrap();
        assert_eq!(read.unwrap(), 3);
        assert_eq!(&data, b"XYZ");
        releaser.join().unwrap().unwrap();
        assert!(release_done_rx.recv().is_ok());
        assert_eq!(scripted.release_call_count(), 2);
        assert!(fs_a.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn remote_provider_lifecycle_wait_idle_times_out_with_admitted_io() {
        let lifecycle = RemoteProviderLifecycle::live();
        let guard = lifecycle
            .begin_io()
            .unwrap()
            .expect("live lifecycle admits provider IO");
        lifecycle.retire_new_io().unwrap();
        assert!(
            lifecycle.begin_io().unwrap().is_none(),
            "retired provider must reject new IO while admitted IO drains"
        );

        let started = Instant::now();
        let result = lifecycle.wait_idle(Duration::from_millis(50));
        assert!(result.is_err());
        assert!(
            started.elapsed() >= Duration::from_millis(40),
            "wait_idle should wait for its budget before timing out"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "wait_idle timeout must be bounded"
        );

        drop(guard);
        lifecycle.wait_idle(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn temporary_remote_release_failure_still_restores_previous_provider() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("remote-temp-release-failure-restores.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.write(&context(), created.handle, 0, b"abcdef")
            .unwrap();
        fs_a.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();

        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        }));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let writer = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap();
        let previous = fs_b
            .current_remote_provider(&inode_id)
            .unwrap()
            .expect("remote open installs provider");
        let (record, lease) = meta.open_write(&inode_id).unwrap();
        let record = fs_b.validate_inode(record).unwrap();
        let temp = fs_b
            .open_remote_write_session(&record, &lease, libc::O_WRONLY, OpenOptions::default())
            .unwrap();
        let temp_remote = temp.remote.as_ref().unwrap().clone();
        let owner_writer = DistributedFs::file_handle_from_owner(&temp_remote.handle).unwrap();
        fs_a.handles
            .lock()
            .unwrap()
            .remove(&owner_writer.0)
            .unwrap();

        fs_b.close_temporary_remote_write_session(&inode_id, &temp, Some(previous.clone()))
            .unwrap();
        let current = fs_b
            .current_remote_provider(&inode_id)
            .unwrap()
            .expect("previous provider is restored after failed temp close");
        assert!(DistributedFs::same_remote_provider(&current, &previous));
        fs_b.release(&context(), writer).unwrap();
    }

    #[test]
    fn remote_release_ack_loss_is_retained_and_drained_idempotently() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("release-ack-loss.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();
        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let loopback = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
            loopback,
            vec![ScriptedReleaseAction::DelegateThenErr(unavailable(
                "injected ACK loss",
            ))],
        ));
        let remote_owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        }));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let writer = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap();

        fs_b.release(&context(), writer)
            .expect_err("lost release ACK is reported once");
        assert_eq!(fs_b.pending_remote_release_count().unwrap(), 1);
        assert!(fs_a.handles.lock().unwrap().is_empty());
        assert_eq!(fs_b.retry_pending_remote_releases().unwrap(), 1);
        assert_eq!(fs_b.pending_remote_release_count().unwrap(), 0);
        assert_eq!(scripted.release_call_count(), 2);
    }

    #[test]
    fn remote_release_before_apply_failure_is_retained_and_later_drained() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("release-before-apply.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();
        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let loopback = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
            loopback,
            vec![ScriptedReleaseAction::ErrWithoutDelegate(unavailable(
                "injected before apply",
            ))],
        ));
        let remote_owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        }));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let writer = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap();

        fs_b.release(&context(), writer)
            .expect_err("pre-apply release failure is reported");
        assert_eq!(fs_b.pending_remote_release_count().unwrap(), 1);
        assert_eq!(fs_a.handles.lock().unwrap().len(), 1);
        assert_eq!(fs_b.retry_pending_remote_releases().unwrap(), 1);
        assert_eq!(fs_b.pending_remote_release_count().unwrap(), 0);
        assert!(fs_a.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn pending_remote_release_retry_pass_attempts_each_existing_handle_once() {
        let (_temp, meta, fs) = test_fs();
        meta.set_node_session("node-b", Some("owner-session-b"));
        let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
            Arc::new(NoopRemoteDfsOwner),
            vec![
                ScriptedReleaseAction::ErrWithoutDelegate(unavailable("release one fails")),
                ScriptedReleaseAction::ErrWithoutDelegate(unavailable("release two fails")),
            ],
        ));
        let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
        let first = afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: "default".into(),
            inode_id: "inode:pending-release-one".into(),
            owner_node_id: "node-b".into(),
            owner_session_id: "owner-session-b".into(),
            lease_epoch: 7,
            caller_node_id: "node-a".into(),
            caller_session_id: "caller-session-a".into(),
            open_seq: 1,
            opaque_handle: b"release-one".to_vec(),
        };
        let second = afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: "default".into(),
            inode_id: "inode:pending-release-two".into(),
            owner_node_id: "node-b".into(),
            owner_session_id: "owner-session-b".into(),
            lease_epoch: 7,
            caller_node_id: "node-a".into(),
            caller_session_id: "caller-session-a".into(),
            open_seq: 2,
            opaque_handle: b"release-two".to_vec(),
        };
        fs.queue_pending_remote_release(owner.clone(), first.clone(), false)
            .unwrap();
        fs.queue_pending_remote_release(owner, second.clone(), false)
            .unwrap();

        fs.retry_pending_remote_releases()
            .expect_err("retryable release failures remain queued");

        assert_eq!(
            scripted.release_call_count(),
            2,
            "one maintenance pass must not retry the same queued release more than once"
        );
        assert_eq!(fs.pending_remote_release_count().unwrap(), 2);
        let state = fs.pending_remote_releases.lock().unwrap();
        assert!(
            state
                .entries
                .contains_key(&DistributedFs::remote_release_key(&first))
        );
        assert!(
            state
                .entries
                .contains_key(&DistributedFs::remote_release_key(&second))
        );
    }

    #[test]
    fn pending_remote_release_retry_pass_caps_attempts_and_rotates_fairly() {
        let (_temp, meta, fs) = test_fs();
        meta.set_node_session("node-b", Some("owner-session-b"));
        let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
            Arc::new(NoopRemoteDfsOwner),
            (0..65)
                .map(|index| {
                    ScriptedReleaseAction::ErrWithoutDelegate(unavailable(format!(
                        "release {index} fails"
                    )))
                })
                .collect(),
        ));
        let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
        let handles: Vec<_> = (0..65)
            .map(|index| afs_protocol::node_control::DfsOwnerHandle {
                namespace_id: "default".into(),
                inode_id: format!("inode:pending-release-{index:02}"),
                owner_node_id: "node-b".into(),
                owner_session_id: "owner-session-b".into(),
                lease_epoch: 7,
                caller_node_id: "node-a".into(),
                caller_session_id: "caller-session-a".into(),
                open_seq: index + 1,
                opaque_handle: vec![index as u8],
            })
            .collect();
        for handle in &handles {
            fs.queue_pending_remote_release(owner.clone(), handle.clone(), false)
                .unwrap();
        }

        fs.retry_pending_remote_releases()
            .expect_err("retryable release failures remain queued");

        let first_pass = scripted.release_opaque_handles();
        assert_eq!(
            first_pass,
            (0..64).map(|index| vec![index as u8]).collect::<Vec<_>>(),
            "one maintenance pass tries only the first 64 queued handles once"
        );
        assert_eq!(fs.pending_remote_release_count().unwrap(), 65);

        fs.retry_pending_remote_releases()
            .expect_err("retryable release failures remain queued");

        let calls = scripted.release_opaque_handles();
        assert_eq!(
            calls.get(64),
            Some(&vec![64]),
            "the next pass starts with the previous pass's unattempted tail handle"
        );
        assert_eq!(fs.pending_remote_release_count().unwrap(), 65);
    }

    #[test]
    fn pending_remote_release_retires_replaced_or_absent_owner_session() {
        let (_temp, meta, fs) = test_fs();
        meta.set_node_session("node-b", Some("session-new"));
        meta.set_node_session("node-c", None);
        let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
            Arc::new(NoopRemoteDfsOwner),
            Vec::new(),
        ));
        let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
        let replaced = afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: "default".into(),
            inode_id: "inode:pending-retired-replaced".into(),
            owner_node_id: "node-b".into(),
            owner_session_id: "session-old".into(),
            lease_epoch: 7,
            caller_node_id: "node-a".into(),
            caller_session_id: "caller-session-a".into(),
            open_seq: 1,
            opaque_handle: b"retired-replaced".to_vec(),
        };
        let absent = afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: "default".into(),
            inode_id: "inode:pending-retired-absent".into(),
            owner_node_id: "node-c".into(),
            owner_session_id: "session-old".into(),
            lease_epoch: 7,
            caller_node_id: "node-a".into(),
            caller_session_id: "caller-session-a".into(),
            open_seq: 2,
            opaque_handle: b"retired-absent".to_vec(),
        };
        fs.queue_pending_remote_release(owner.clone(), replaced, false)
            .unwrap();
        fs.queue_pending_remote_release(owner, absent, false)
            .unwrap();

        assert_eq!(fs.retry_pending_remote_releases().unwrap(), 2);

        assert_eq!(fs.pending_remote_release_count().unwrap(), 0);
        assert_eq!(scripted.release_call_count(), 0);
        assert_eq!(meta.current_session_call_count(), 2);
    }

    #[test]
    fn pending_remote_release_unknown_owner_session_retains_debt() {
        let (_temp, meta, fs) = test_fs();
        meta.fail_next_current_session_with(unavailable("injected session lookup failure"));
        let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
            Arc::new(NoopRemoteDfsOwner),
            Vec::new(),
        ));
        let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
        let handle = afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: "default".into(),
            inode_id: "inode:pending-unknown-session".into(),
            owner_node_id: "node-b".into(),
            owner_session_id: "session-b".into(),
            lease_epoch: 7,
            caller_node_id: "node-a".into(),
            caller_session_id: "caller-session-a".into(),
            open_seq: 1,
            opaque_handle: b"unknown-session".to_vec(),
        };
        fs.queue_pending_remote_release(owner, handle.clone(), false)
            .unwrap();

        fs.retry_pending_remote_releases()
            .expect_err("unknown owner process session keeps exact cleanup debt");

        assert_eq!(fs.pending_remote_release_count().unwrap(), 1);
        assert_eq!(scripted.release_call_count(), 0);
        assert_eq!(meta.current_session_call_count(), 1);
        let state = fs.pending_remote_releases.lock().unwrap();
        assert!(
            state
                .entries
                .contains_key(&DistributedFs::remote_release_key(&handle))
        );
    }

    #[test]
    fn pending_remote_release_mixed_sessions_retire_old_and_release_same_session() {
        let (_temp, meta, fs) = test_fs();
        meta.set_node_session("node-b", Some("session-live"));
        let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
            Arc::new(NoopRemoteDfsOwner),
            vec![ScriptedReleaseAction::OkWithoutDelegate],
        ));
        let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
        let old = afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: "default".into(),
            inode_id: "inode:pending-old-session".into(),
            owner_node_id: "node-b".into(),
            owner_session_id: "session-old".into(),
            lease_epoch: 7,
            caller_node_id: "node-a".into(),
            caller_session_id: "caller-session-a".into(),
            open_seq: 1,
            opaque_handle: b"old-session".to_vec(),
        };
        let live = afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: "default".into(),
            inode_id: "inode:pending-live-session".into(),
            owner_node_id: "node-b".into(),
            owner_session_id: "session-live".into(),
            lease_epoch: 7,
            caller_node_id: "node-a".into(),
            caller_session_id: "caller-session-a".into(),
            open_seq: 2,
            opaque_handle: b"live-session".to_vec(),
        };
        fs.queue_pending_remote_release(owner.clone(), old, false)
            .unwrap();
        fs.queue_pending_remote_release(owner, live, false).unwrap();

        assert_eq!(fs.retry_pending_remote_releases().unwrap(), 2);

        assert_eq!(fs.pending_remote_release_count().unwrap(), 0);
        assert_eq!(scripted.release_call_count(), 1);
        assert_eq!(
            scripted.release_opaque_handles(),
            vec![b"live-session".to_vec()]
        );
        assert_eq!(meta.current_session_call_count(), 1);
    }

    #[test]
    fn pending_remote_release_groups_retired_session_lookup_and_preserves_fair_cap() {
        let (_temp, meta, fs) = test_fs();
        meta.set_node_session("node-b", Some("session-new"));
        let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(
            Arc::new(NoopRemoteDfsOwner),
            Vec::new(),
        ));
        let owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
        for index in 0..65 {
            let handle = afs_protocol::node_control::DfsOwnerHandle {
                namespace_id: "default".into(),
                inode_id: format!("inode:pending-retired-group-{index:02}"),
                owner_node_id: "node-b".into(),
                owner_session_id: "session-old".into(),
                lease_epoch: 7,
                caller_node_id: "node-a".into(),
                caller_session_id: "caller-session-a".into(),
                open_seq: index + 1,
                opaque_handle: vec![index as u8],
            };
            fs.queue_pending_remote_release(owner.clone(), handle, false)
                .unwrap();
        }

        fs.retry_pending_remote_releases()
            .expect_err("one fair pass retires only the first 64 queued entries");

        assert_eq!(fs.pending_remote_release_count().unwrap(), 1);
        assert_eq!(scripted.release_call_count(), 0);
        assert_eq!(meta.current_session_call_count(), 1);

        assert_eq!(fs.retry_pending_remote_releases().unwrap(), 1);
        assert_eq!(fs.pending_remote_release_count().unwrap(), 0);
        assert_eq!(scripted.release_call_count(), 0);
        assert_eq!(meta.current_session_call_count(), 2);
    }

    #[test]
    fn remote_release_capacity_rejects_new_admission_without_owner_open() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("release-capacity.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();
        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        }));
        fs_b.pending_remote_releases.lock().unwrap().reserved = MAX_PENDING_REMOTE_RELEASES;
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let error = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
        assert!(fs_a.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn owner_open_open_write_failure_clears_inflight_admission() {
        let (_temp, meta, fs) = test_fs();
        meta.fail_next_open_write_with(unavailable("injected open_write failure"));
        let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(1, libc::O_RDWR),
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
        let routes = fs.owner_open_routes.lock().unwrap();
        let state = routes.values().next().unwrap();
        assert_eq!(state.highwater, 1);
        assert!(state.inflight.is_empty());
        assert!(state.cancelled.is_empty());
        assert!(state.active.is_empty());
    }

    #[test]
    fn owner_open_resolves_fresh_authority_without_legacy_open_write() {
        let (_temp, meta, fs) = test_fs();
        let opens_before = meta.open_write_calls.load(Ordering::SeqCst);
        let resolve_before = meta.resolve_write_calls.load(Ordering::SeqCst);

        let reply = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(1, libc::O_RDWR),
        )
        .unwrap();
        assert!(reply.handle.is_some());

        assert_eq!(
            meta.open_write_calls.load(Ordering::SeqCst),
            opens_before,
            "remote owner admission must not invoke legacy OpenWrite"
        );
        assert_eq!(
            meta.resolve_write_calls.load(Ordering::SeqCst),
            resolve_before + 1,
            "remote owner admission must resolve fresh write authority"
        );
    }

    #[test]
    fn owner_open_rejects_stale_request_epoch_after_fresh_write_authority_reacquire() {
        let (_temp, meta, fs) = test_fs();
        {
            let mut lease = meta.lease.lock().unwrap();
            lease.lease_epoch = lease.lease_epoch.saturating_add(1);
            lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
        }

        let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(1, libc::O_RDWR),
        )
        .expect_err("stale remote owner open request must not succeed after Meta reacquires a newer lease epoch");

        assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
        let routes = fs.owner_open_routes.lock().unwrap();
        let state = routes.values().next().unwrap();
        assert_eq!(state.highwater, 1);
        assert!(state.inflight.is_empty());
        assert!(state.cancelled.is_empty());
        assert!(state.active.is_empty());
        assert!(fs.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn owner_open_rejects_epoch_changed_by_legacy_reacquire_after_resolver_reply() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let near_expiry = now_unix_ms().saturating_add(1_000);
        let inode = meta.inode.lock().unwrap().clone();
        let mut epoch_two = {
            let mut lease = meta.lease.lock().unwrap();
            lease.lease_epoch = 1;
            lease.expires_at_unix_ms = near_expiry;
            lease.clone()
        };
        epoch_two.lease_epoch = 2;
        epoch_two.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
        meta.queue_open_write_result(Ok((inode, epoch_two)));

        let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(1, libc::O_RDWR),
        )
        .expect_err("owner open must not succeed after legacy OpenWrite changes the lease epoch behind the resolver reply");

        assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
        assert_eq!(meta.resolve_write_calls.load(Ordering::SeqCst), 1);
        assert_eq!(meta.open_write_calls.load(Ordering::SeqCst), 1);
        let routes = fs.owner_open_routes.lock().unwrap();
        let state = routes.values().next().unwrap();
        assert_eq!(state.highwater, 1);
        assert!(state.inflight.is_empty());
        assert!(state.cancelled.is_empty());
        assert!(state.active.is_empty());
        drop(routes);
        assert!(fs.handles.lock().unwrap().is_empty());
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        assert_eq!(state.lock().unwrap().open_writers, 0);

        let replay = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(1, libc::O_RDWR),
        )
        .expect_err("failed owner admission sequence must be retired");
        assert_eq!(replay.code(), afs_error::NODE_DFS_STALE_HANDLE);
        assert!(fs.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn owner_open_validate_inode_failure_clears_inflight_admission() {
        let (_temp, meta, fs) = test_fs();
        meta.return_bad_namespace_on_next_open_write();
        let error = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(1, libc::O_RDWR),
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_VFS_INVALID);
        let routes = fs.owner_open_routes.lock().unwrap();
        let state = routes.values().next().unwrap();
        assert_eq!(state.highwater, 1);
        assert!(state.inflight.is_empty());
        assert!(state.cancelled.is_empty());
        assert!(state.active.is_empty());
    }

    #[test]
    fn owner_open_replay_returns_same_handle_and_changed_fingerprint_is_rejected() {
        let (_temp, _meta, fs) = test_fs();
        let request = dfs_owner_open_request(1, libc::O_RDWR);
        let first = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            request.clone(),
        )
        .unwrap()
        .handle
        .unwrap();
        let replay = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs, "node-b", request,
        )
        .unwrap()
        .handle
        .unwrap();
        assert_eq!(replay.opaque_handle, first.opaque_handle);
        assert_eq!(fs.handles.lock().unwrap().len(), 1);

        let changed = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(1, libc::O_WRONLY),
        )
        .unwrap_err();
        assert_eq!(changed.code(), afs_error::NODE_DFS_STALE_HANDLE);
        assert_eq!(fs.handles.lock().unwrap().len(), 1);
    }

    #[test]
    fn owner_open_cleanup_identity_cancels_late_open_without_allocation() {
        let (_temp, _meta, fs) = test_fs();
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseRequest {
                handle: Some(dfs_owner_cleanup_handle(1)),
            },
        )
        .unwrap();

        let late = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(1, libc::O_RDWR),
        )
        .unwrap_err();
        assert_eq!(late.code(), afs_error::NODE_DFS_STALE_HANDLE);
        assert!(fs.handles.lock().unwrap().is_empty());

        let phantom = DistributedFs::data_owner_handle(&dfs_owner_cleanup_handle(2));
        let read = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::read(
            &fs,
            "node-b",
            afs_protocol::node_data::DfsOwnerReadRequest {
                handle: Some(phantom),
                offset: 0,
                length: 1,
            },
        )
        .unwrap_err();
        assert_eq!(read.code(), afs_error::NODE_DFS_STALE_HANDLE);
    }

    #[test]
    fn long_lived_low_open_sequence_does_not_block_later_admissions() {
        let (_temp, _meta, fs) = test_fs();
        for seq in 1..=5000 {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
                &fs,
                "node-b",
                dfs_owner_open_request(seq, libc::O_RDWR),
            )
            .unwrap();
        }
        assert_eq!(fs.handles.lock().unwrap().len(), 5000);
    }

    #[test]
    fn owner_open_unknown_cleanup_advances_highwater_without_tombstone_growth() {
        let (_temp, _meta, fs) = test_fs();
        for seq in 1..=MAX_PENDING_REMOTE_RELEASES as u64 + 10 {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
                &fs,
                "node-b",
                afs_protocol::node_control::DfsOwnerReleaseRequest {
                    handle: Some(dfs_owner_cleanup_handle(seq)),
                },
            )
            .unwrap();
        }
        let routes = fs.owner_open_routes.lock().unwrap();
        let state = routes.values().next().unwrap();
        assert_eq!(state.cancelled.len(), 0);
        assert_eq!(state.inflight.len(), 0);
        assert_eq!(state.highwater, MAX_PENDING_REMOTE_RELEASES as u64 + 10);
        drop(routes);
        let old = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(1, libc::O_RDWR),
        )
        .unwrap_err();
        assert_eq!(old.code(), afs_error::NODE_DFS_STALE_HANDLE);
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            dfs_owner_open_request(MAX_PENDING_REMOTE_RELEASES as u64 + 11, libc::O_RDWR),
        )
        .unwrap();
    }

    #[test]
    fn owner_open_inflight_cleanup_wrong_scope_does_not_cancel() {
        let (_temp, _meta, fs) = test_fs();
        let request = dfs_owner_open_request(1, libc::O_RDWR);
        let admission = fs.start_owner_open_admission("node-b", &request).unwrap();
        assert!(matches!(
            admission,
            DfsOwnerOpenAdmissionStart::Apply { .. }
        ));
        let mut wrong = dfs_owner_cleanup_handle(1);
        wrong.inode_id = "inode:wrong".into();
        let error =
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
                &fs,
                "node-b",
                afs_protocol::node_control::DfsOwnerReleaseRequest {
                    handle: Some(wrong),
                },
            )
            .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
        let routes = fs.owner_open_routes.lock().unwrap();
        let state = routes.values().next().unwrap();
        assert_eq!(state.cancelled.len(), 0);
        assert_eq!(state.inflight.len(), 1);
        drop(routes);
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseRequest {
                handle: Some(dfs_owner_cleanup_handle(1)),
            },
        )
        .unwrap();
        let routes = fs.owner_open_routes.lock().unwrap();
        let state = routes.values().next().unwrap();
        assert_eq!(state.cancelled.len(), 1);
    }

    #[test]
    fn owner_open_cleanup_wrong_scope_does_not_close_active_handle() {
        let (_temp, _meta, fs) = test_fs();
        let request = dfs_owner_open_request(1, libc::O_RDWR);
        let handle = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            request.clone(),
        )
        .unwrap()
        .handle
        .unwrap();
        let mut wrong = dfs_owner_cleanup_handle(1);
        wrong.inode_id = "inode:wrong".into();
        let error =
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
                &fs,
                "node-b",
                afs_protocol::node_control::DfsOwnerReleaseRequest {
                    handle: Some(wrong),
                },
            )
            .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
        assert_eq!(fs.handles.lock().unwrap().len(), 1);
        let replay = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs, "node-b", request,
        )
        .unwrap()
        .handle
        .unwrap();
        assert_eq!(replay.opaque_handle, handle.opaque_handle);
    }

    #[test]
    fn owner_open_cleanup_release_failure_retains_active_identity() {
        let (_temp, _meta, fs) = test_fs();
        let request = dfs_owner_open_request(1, libc::O_RDWR);
        let handle = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            request.clone(),
        )
        .unwrap()
        .handle
        .unwrap();
        fs.handles.lock().unwrap().clear();
        let error =
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
                &fs,
                "node-b",
                afs_protocol::node_control::DfsOwnerReleaseRequest {
                    handle: Some(dfs_owner_cleanup_handle(1)),
                },
            )
            .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
        let replay = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs, "node-b", request,
        )
        .unwrap()
        .handle
        .unwrap();
        assert_eq!(replay.opaque_handle, handle.opaque_handle);
    }

    #[test]
    fn owner_open_replay_state_retires_with_authoritative_caller_session() {
        let (_temp, meta, fs) = test_fs();
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseRequest {
                handle: Some(dfs_owner_cleanup_handle(1)),
            },
        )
        .unwrap();
        assert_eq!(fs.owner_open_routes.lock().unwrap().len(), 1);
        meta.set_node_session("node-b", Some("session-b2"));
        assert_eq!(fs.reap_expired_peer_owner_handles().unwrap(), 0);
        assert!(fs.owner_open_routes.lock().unwrap().is_empty());
        let stale =
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
                &fs,
                "node-b",
                afs_protocol::node_control::DfsOwnerReleaseRequest {
                    handle: Some(dfs_owner_cleanup_handle(2)),
                },
            )
            .unwrap_err();
        assert_eq!(stale.code(), afs_error::NODE_DFS_STALE_HANDLE);
        assert!(fs.owner_open_routes.lock().unwrap().is_empty());
    }

    #[test]
    fn remote_open_admission_lock_cleanup_keeps_reused_lock_with_users() {
        let (_temp, _meta, fs) = test_fs();
        let route = DfsOwnerOpenRouteKey {
            caller_node_id: "node-b".into(),
            caller_session_id: "session-b".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
        };
        let lock = Arc::new(DfsRemoteOpenAdmissionLock {
            gate: Mutex::new(()),
            users: AtomicUsize::new(1),
        });
        fs.remote_open_admission_locks
            .lock()
            .unwrap()
            .insert(route.clone(), lock.clone());
        fs.cleanup_remote_open_admission_lock(&route, &lock)
            .unwrap();
        assert!(
            fs.remote_open_admission_locks
                .lock()
                .unwrap()
                .contains_key(&route)
        );
        lock.users.store(0, Ordering::Release);
        fs.cleanup_remote_open_admission_lock(&route, &lock)
            .unwrap();
        assert!(fs.remote_open_admission_locks.lock().unwrap().is_empty());
    }

    #[test]
    fn caller_serializes_same_route_open_admission_until_ack() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("serialized-open-admission.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();
        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let loopback = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let (first_entered_tx, first_entered_rx) = std::sync::mpsc::channel();
        let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();
        let (second_entered_tx, second_entered_rx) = std::sync::mpsc::channel();
        let blocking = Arc::new(BlockingFirstOpenRemoteDfsOwner::new(
            loopback,
            first_entered_tx,
            release_first_rx,
            second_entered_tx,
        ));
        let fs_b = Arc::new(
            DistributedFs::new(
                NamespaceId::new("default"),
                "node-b",
                "session-b",
                meta.clone(),
                chunks_b,
                read_engine_b,
            )
            .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
                owner: blocking.clone() as Arc<dyn RemoteDfsOwner>,
            })),
        );
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let fs_first = fs_b.clone();
        let first_inode = inode_b;
        let first =
            std::thread::spawn(move || fs_first.open(&context(), first_inode, libc::O_RDWR));
        first_entered_rx.recv().unwrap();
        let fs_second = fs_b.clone();
        let second_inode = inode_b;
        let second =
            std::thread::spawn(move || fs_second.open(&context(), second_inode, libc::O_RDWR));
        assert!(
            second_entered_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err()
        );
        release_first_tx.send(()).unwrap();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        let requests = blocking.open_requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].open_seq, 1);
        assert_eq!(requests[1].open_seq, 2);
        assert!(fs_b.remote_open_admission_locks.lock().unwrap().is_empty());
    }

    #[test]
    fn owner_open_finish_after_session_reaper_cleans_allocated_handle() {
        let (_temp, meta, fs_raw) = test_fs();
        let fs = Arc::new(fs_raw);
        let pause = Arc::new(OwnerOpenFinishPause::new());
        fs.set_owner_open_finish_pause(Some(pause.clone()));
        let fs_open = fs.clone();
        let open = std::thread::spawn(move || {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
                fs_open.as_ref(),
                "node-b",
                dfs_owner_open_request(1, libc::O_RDWR),
            )
        });
        pause.wait_until_reached();
        assert_eq!(fs.handles.lock().unwrap().len(), 1);
        meta.set_node_session("node-b", Some("session-b2"));
        assert_eq!(fs.reap_expired_peer_owner_handles().unwrap(), 1);
        assert!(fs.handles.lock().unwrap().is_empty());
        pause.resume();
        let error = open.join().unwrap().unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
        assert!(fs.handles.lock().unwrap().is_empty());
        assert!(fs.owner_open_routes.lock().unwrap().is_empty());
        fs.set_owner_open_finish_pause(None);
    }

    #[test]
    fn remote_open_ack_loss_queues_cleanup_identity_and_late_replay_does_not_reopen() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("open-ack-loss.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();
        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let loopback = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let scripted = Arc::new(ScriptedOpenRemoteDfsOwner::new(
            loopback,
            vec![ScriptedOpenAction::DelegateThenErr(unavailable(
                "injected open ACK loss",
            ))],
        ));
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: scripted.clone() as Arc<dyn RemoteDfsOwner>,
        }));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();

        let error = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
        assert_eq!(fs_b.pending_remote_release_count().unwrap(), 1);
        assert_eq!(fs_a.handles.lock().unwrap().len(), 1);
        assert_eq!(fs_b.retry_pending_remote_releases().unwrap(), 1);
        assert!(fs_a.handles.lock().unwrap().is_empty());

        let late = scripted.open_requests()[0].clone();
        let late = scripted.open(late).unwrap_err();
        assert_eq!(late.code(), afs_error::NODE_DFS_STALE_HANDLE);
        assert!(fs_a.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn writeonly_read_companion_ack_loss_has_independent_cleanup_identity() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("writeonly-companion-ack-loss.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();
        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let loopback = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let scripted = Arc::new(ScriptedOpenRemoteDfsOwner::new(
            loopback,
            vec![
                ScriptedOpenAction::Delegate,
                ScriptedOpenAction::DelegateThenErr(unavailable("injected companion ACK loss")),
            ],
        ));
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: scripted.clone() as Arc<dyn RemoteDfsOwner>,
        }));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();

        let error = fs_b.open(&context(), inode_b, libc::O_WRONLY).unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
        assert_eq!(scripted.open_requests().len(), 2);
        assert_eq!(fs_b.pending_remote_release_count().unwrap(), 1);
        assert_eq!(fs_a.handles.lock().unwrap().len(), 1);
        assert_eq!(fs_b.retry_pending_remote_releases().unwrap(), 1);
        assert!(fs_a.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn old_owner_lease_data_ops_are_denied_but_release_cleans_resource() {
        let (_temp, meta, fs) = test_fs();
        let open = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerOpenRequest {
                namespace_id: "default".into(),
                inode_id: "inode:test".into(),
                owner_node_id: "node-a".into(),
                owner_session_id: "session-a".into(),
                lease_epoch: 1,
                caller_session_id: "session-b".into(),
                open_flags: libc::O_RDWR,
                kill_suidgid: false,
                open_seq: 1,
            },
        )
        .unwrap();
        let handle = open.handle.unwrap();
        let inode_id = InodeId::new("inode:test");
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        state.lock().unwrap().write_lease.lease_epoch = 2;
        meta.lease.lock().unwrap().lease_epoch = 2;
        let data_handle = DistributedFs::data_owner_handle(&handle);
        let error = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
            &fs,
            "node-b",
            afs_protocol::node_data::DfsOwnerWriteRequest {
                handle: Some(data_handle),
                operation_id: "old-lease-write".into(),
                offset: 0,
                data: b"x".to_vec(),
                append: false,
                kill_suidgid: false,
            },
        )
        .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseRequest {
                handle: Some(handle),
            },
        )
        .unwrap();
        assert!(fs.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn owner_handle_reap_requires_definite_caller_session_replacement() {
        let (_temp, meta, fs) = test_fs();
        let open = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerOpenRequest {
                namespace_id: "default".into(),
                inode_id: "inode:test".into(),
                owner_node_id: "node-a".into(),
                owner_session_id: "session-a".into(),
                lease_epoch: 1,
                caller_session_id: "session-b".into(),
                open_flags: libc::O_RDWR,
                kill_suidgid: false,
                open_seq: 1,
            },
        )
        .unwrap();
        assert!(open.handle.is_some());
        meta.fail_next_current_session_with(unavailable("injected Meta session lookup failure"));
        assert!(fs.reap_expired_peer_owner_handles().is_err());
        assert_eq!(fs.handles.lock().unwrap().len(), 1);
        meta.set_node_session("node-b", Some("session-b2"));
        assert_eq!(fs.reap_expired_peer_owner_handles().unwrap(), 1);
        assert!(fs.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn wrong_peer_cannot_release_live_owner_handle() {
        let (_temp, _meta, fs) = test_fs();
        let handle = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerOpenRequest {
                namespace_id: "default".into(),
                inode_id: "inode:test".into(),
                owner_node_id: "node-a".into(),
                owner_session_id: "session-a".into(),
                lease_epoch: 1,
                caller_session_id: "session-b".into(),
                open_flags: libc::O_RDWR,
                kill_suidgid: false,
                open_seq: 1,
            },
        )
        .unwrap()
        .handle
        .unwrap();
        let error =
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
                &fs,
                "node-c",
                afs_protocol::node_control::DfsOwnerReleaseRequest {
                    handle: Some(handle.clone()),
                },
            )
            .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_DFS_STALE_HANDLE);
        assert_eq!(fs.handles.lock().unwrap().len(), 1);
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseRequest {
                handle: Some(handle),
            },
        )
        .unwrap();
    }

    #[test]
    fn writeonly_remote_provider_supports_fresh_readonly_reader() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("remote-provider-readonly-reader.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.write(&context(), created.handle, 0, b"abcdef")
            .unwrap();
        fs_a.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();

        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        }));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let writer = fs_b
            .open(&context(), inode_b, libc::O_WRONLY | libc::O_TRUNC)
            .unwrap();

        assert_eq!(fs_b.write(&context(), writer, 0, b"XYZ").unwrap(), 3);
        let reader = fs_b.open(&context(), inode_b, libc::O_RDONLY).unwrap();
        assert_eq!(
            fs_b.getattr(&context(), inode_b, Some(reader))
                .unwrap()
                .size,
            3
        );
        let mut data = [0; 3];
        assert_eq!(fs_b.read(&context(), reader, 0, &mut data).unwrap(), 3);
        assert_eq!(&data, b"XYZ");

        assert!(fs_b.read(&context(), writer, 0, &mut data).is_err());
        fs_b.release(&context(), reader).unwrap();
        fs_b.release(&context(), writer).unwrap();
        assert!(fs_b.current_remote_provider(&inode_id).unwrap().is_none());
        assert!(fs_a.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn remote_provider_retire_timeout_keeps_release_debt_until_io_guard_drops() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        meta.set_node_session("node-a", Some("session-a"));
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("remote-provider-retire-timeout.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.write(&context(), created.handle, 0, b"abcdef")
            .unwrap();
        fs_a.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();

        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let loopback = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let scripted = Arc::new(ScriptedReleaseRemoteDfsOwner::new(loopback, Vec::new()));
        let remote_owner = scripted.clone() as Arc<dyn RemoteDfsOwner>;
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        }));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let writer = fs_b.open(&context(), inode_b, libc::O_WRONLY).unwrap();
        let snapshot = fs_b.handle_snapshot(writer).unwrap();
        let remote = snapshot
            .write_session
            .as_ref()
            .and_then(|session| session.remote.as_ref())
            .expect("remote write-only open installs a provider")
            .clone();
        let guard = remote
            .lifecycle
            .begin_io()
            .unwrap()
            .expect("live provider admits read IO");

        let error = fs_b
            .release(&context(), writer)
            .expect_err("retire waits for admitted IO and reports timeout");
        assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
        assert_eq!(scripted.release_call_count(), 0);
        assert_eq!(fs_b.pending_remote_release_count().unwrap(), 2);
        assert_eq!(fs_a.handles.lock().unwrap().len(), 2);
        assert_eq!(fs_b.pending_remote_releases.lock().unwrap().reserved, 0);

        fs_b.retry_pending_remote_releases()
            .expect_err("maintenance must not release while provider IO is admitted");
        assert_eq!(scripted.release_call_count(), 0);
        assert_eq!(fs_b.pending_remote_release_count().unwrap(), 2);
        assert_eq!(fs_a.handles.lock().unwrap().len(), 2);

        drop(guard);

        assert_eq!(fs_b.retry_pending_remote_releases().unwrap(), 2);
        assert_eq!(scripted.release_call_count(), 2);
        assert_eq!(fs_b.pending_remote_release_count().unwrap(), 0);
        assert_eq!(fs_b.pending_remote_releases.lock().unwrap().reserved, 0);
        assert!(fs_a.handles.lock().unwrap().is_empty());
    }

    #[test]
    fn former_local_clean_state_does_not_shadow_remote_provider() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let created = fs_a_raw
            .create(
                &context(),
                fs_a_raw.root_inode(),
                OsStr::new("former-local-remote-provider.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a_raw
            .write(&context(), created.handle, 0, b"abcdef")
            .unwrap();
        fs_a_raw
            .fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs_a_raw.release(&context(), created.handle).unwrap();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let retained = fs_a_raw
            .write_state(&inode_id)
            .unwrap()
            .expect("local owner keeps a clean retained write state");
        assert!(DistributedFs::write_state_can_refresh_committed_view(
            &retained.lock().unwrap()
        ));

        {
            let mut lease = meta.lease.lock().unwrap();
            lease.owner_node_id = "node-b".into();
            lease.owner_session_id = "session-b".into();
            lease.lease_epoch = lease.lease_epoch.saturating_add(1);
            lease.expires_at_unix_ms = u64::MAX;
        }

        // The routing fixture gives the new owner a recovered committed base.
        let chunks_b = Arc::new(LocalChunkStore::open(_temp_a.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let fs_b = Arc::new(DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        ));
        let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_b,
            peer: "node-a".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let fs_a = fs_a_raw.with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        }));
        let inode_a = fs_a.backend_inode(&inode_id).unwrap();
        let writer = fs_a
            .open(&context(), inode_a, libc::O_WRONLY | libc::O_TRUNC)
            .unwrap();

        assert_eq!(fs_a.write(&context(), writer, 0, b"XYZ").unwrap(), 3);
        let reader = fs_a.open(&context(), inode_a, libc::O_RDONLY).unwrap();
        assert_eq!(
            fs_a.getattr(&context(), inode_a, Some(reader))
                .unwrap()
                .size,
            3
        );
        let mut data = [0; 3];
        assert_eq!(fs_a.read(&context(), reader, 0, &mut data).unwrap(), 3);
        assert_eq!(&data, b"XYZ");
        fs_a.release(&context(), reader).unwrap();
        fs_a.release(&context(), writer).unwrap();
        assert!(fs_a.current_remote_provider(&inode_id).unwrap().is_none());
    }

    #[test]
    fn temporary_remote_close_does_not_restore_released_previous_provider() {
        let (_temp_a, meta, fs_a_raw) = test_fs();
        let fs_a = Arc::new(fs_a_raw);
        let created = fs_a
            .create(
                &context(),
                fs_a.root_inode(),
                OsStr::new("remote-provider-stale-restore.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs_a.write(&context(), created.handle, 0, b"abcdef")
            .unwrap();
        fs_a.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs_a.release(&context(), created.handle).unwrap();

        let temp_b = tempfile::tempdir().unwrap();
        let chunks_b = Arc::new(LocalChunkStore::open(temp_b.path(), "node-b").unwrap());
        let read_engine_b = Arc::new(crate::node::dfs_read::DfsReadEngine::new(
            NamespaceId::new("default"),
            "node-b".into(),
            chunks_b.clone(),
            Arc::new(crate::node::dfs_read::UnimplementedReadSourceProvider),
            Arc::new(crate::node::dfs_read::UnimplementedChunkTransfer),
            crate::node::dfs_read::DfsReadConfig::default(),
        ));
        let remote_owner = Arc::new(LoopbackRemoteDfsOwner {
            owner: fs_a.clone(),
            peer: "node-b".into(),
        }) as Arc<dyn RemoteDfsOwner>;
        let fs_b = DistributedFs::new(
            NamespaceId::new("default"),
            "node-b",
            "session-b",
            meta.clone(),
            chunks_b,
            read_engine_b,
        )
        .with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote_owner,
        }));
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let inode_b = fs_b.backend_inode(&inode_id).unwrap();
        let handle = fs_b.open(&context(), inode_b, libc::O_RDWR).unwrap();
        let previous = fs_b
            .current_remote_provider(&inode_id)
            .unwrap()
            .expect("remote open installs provider");
        let (record, lease) = meta.open_write(&inode_id).unwrap();
        let record = fs_b.validate_inode(record).unwrap();
        let temp = fs_b
            .open_remote_write_session(&record, &lease, libc::O_WRONLY, OpenOptions::default())
            .unwrap();
        let current = fs_b
            .current_remote_provider(&inode_id)
            .unwrap()
            .expect("temporary session becomes provider");
        assert!(!DistributedFs::same_remote_provider(&current, &previous));

        fs_b.release(&context(), handle).unwrap();
        let current = fs_b
            .current_remote_provider(&inode_id)
            .unwrap()
            .expect("ordinary release must not remove temporary provider");
        assert!(!DistributedFs::same_remote_provider(&current, &previous));

        fs_b.close_temporary_remote_write_session(&inode_id, &temp, Some(previous))
            .unwrap();
        assert!(fs_b.current_remote_provider(&inode_id).unwrap().is_none());
    }

    #[test]
    fn readonly_read_reuses_cached_file_version_for_same_head() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        publish_external_version_with_bytes(&meta, &fs, "cache-same-head", b"cached-view");
        let inode = fs.backend_inode(&inode_id).unwrap();
        let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();

        let mut first = [0; 16];
        assert_eq!(fs.read(&context(), reader, 0, &mut first).unwrap(), 11);
        assert_eq!(&first[..11], b"cached-view");
        assert_eq!(meta.file_version_load_count(), 1);

        let mut second = [0; 16];
        assert_eq!(fs.read(&context(), reader, 0, &mut second).unwrap(), 11);
        assert_eq!(&second[..11], b"cached-view");
        assert_eq!(
            meta.file_version_load_count(),
            1,
            "same validated head should reuse the cached immutable version/layout"
        );

        fs.release(&context(), reader).unwrap();
    }

    #[test]
    fn readonly_read_loads_changed_head_and_returns_new_content_and_length() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        publish_external_version_with_bytes(&meta, &fs, "cache-old-head", b"old");
        let inode = fs.backend_inode(&inode_id).unwrap();
        let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
        let mut old = [0; 8];
        assert_eq!(fs.read(&context(), reader, 0, &mut old).unwrap(), 3);
        assert_eq!(&old[..3], b"old");
        assert_eq!(meta.file_version_load_count(), 1);

        publish_external_version_with_bytes(&meta, &fs, "cache-new-head", b"new-content");
        let mut new = [0; 16];
        assert_eq!(fs.read(&context(), reader, 0, &mut new).unwrap(), 11);
        assert_eq!(&new[..11], b"new-content");
        assert_eq!(meta.file_version_load_count(), 2);

        fs.release(&context(), reader).unwrap();
    }

    #[test]
    fn readonly_cached_read_still_propagates_get_inode_error() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        publish_external_version_with_bytes(&meta, &fs, "cache-inode-error", b"cached-view");
        let inode = fs.backend_inode(&inode_id).unwrap();
        let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
        let mut first = [0; 16];
        assert_eq!(fs.read(&context(), reader, 0, &mut first).unwrap(), 11);
        assert_eq!(meta.file_version_load_count(), 1);

        meta.fail_next_get_inode_with(unavailable("injected readonly get_inode failure"));
        let mut second = [7; 16];
        assert_eq!(
            fs.read(&context(), reader, 0, &mut second)
                .expect_err("validated inode failure must be returned")
                .code(),
            afs_error::NODE_VFS_UNAVAILABLE
        );
        assert_eq!(second, [7; 16]);
        assert_eq!(
            meta.file_version_load_count(),
            1,
            "cache lookup must not bypass the per-read inode validation"
        );

        fs.release(&context(), reader).unwrap();
    }

    #[test]
    fn readonly_changed_version_load_error_does_not_return_cached_old_content() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        publish_external_version_with_bytes(&meta, &fs, "cache-old-before-error", b"old-data");
        let inode = fs.backend_inode(&inode_id).unwrap();
        let reader = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
        let mut old = [0; 16];
        assert_eq!(fs.read(&context(), reader, 0, &mut old).unwrap(), 8);
        assert_eq!(&old[..8], b"old-data");

        publish_external_version_with_bytes(&meta, &fs, "cache-new-error", b"new-data");
        meta.fail_next_get_file_version_with(unavailable(
            "injected readonly GetFileVersion failure",
        ));
        let mut new = [9; 16];
        assert_eq!(
            fs.read(&context(), reader, 0, &mut new)
                .expect_err("changed head load failure must not fall back to old cached version")
                .code(),
            afs_error::NODE_VFS_UNAVAILABLE
        );
        assert_eq!(new, [9; 16]);

        let mut retry = [0; 16];
        assert_eq!(fs.read(&context(), reader, 0, &mut retry).unwrap(), 8);
        assert_eq!(&retry[..8], b"new-data");

        fs.release(&context(), reader).unwrap();
    }

    #[test]
    fn fresh_readonly_open_refreshes_retained_committed_view_after_external_commit() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("fresh-open-external-commit.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        assert_eq!(
            fs.write(&context(), created.handle, 0, b"dfs-handover-initial")
                .unwrap(),
            20
        );
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs.release(&context(), created.handle).unwrap();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        {
            let state = state.lock().unwrap();
            assert_eq!(state.logical_length, 20);
            assert!(!state.dirty);
            assert!(!state.metadata_dirty);
        }

        let external_version = meta.publish_external_version(18);
        let reader = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();

        assert_eq!(
            fs.getattr(&context(), created.entry.inode, Some(reader))
                .unwrap()
                .size,
            18
        );
        let state = state.lock().unwrap();
        assert_eq!(
            state.base_version.as_ref().map(|version| &version.id),
            Some(&external_version)
        );
        assert_eq!(state.logical_length, 18);
        assert_eq!(state.inode.head_version.as_ref(), Some(&external_version));
        drop(state);
        fs.release(&context(), reader).unwrap();
    }

    #[test]
    fn delayed_older_readonly_observation_does_not_downgrade_clean_committed_view() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("delayed-readonly-observation.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        assert_eq!(fs.write(&context(), created.handle, 0, b"old").unwrap(), 3);
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs.release(&context(), created.handle).unwrap();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        let older_inode = meta.inode.lock().unwrap().clone();
        let newer_version = meta.publish_external_version(8);
        let newer_inode = meta.inode.lock().unwrap().clone();

        fs.observe_inode_record(newer_inode).unwrap();
        {
            let state = state.lock().unwrap();
            assert_eq!(state.inode.head_version.as_ref(), Some(&newer_version));
            assert_eq!(state.logical_length, 8);
        }

        fs.observe_inode_record(older_inode).unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.inode.head_version.as_ref(), Some(&newer_version));
        assert_eq!(state.logical_length, 8);
    }

    #[test]
    fn fresh_readonly_open_preserves_dirty_open_writer_after_external_commit() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("dirty-open-reader-external.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        assert_eq!(
            fs.write(&context(), created.handle, 0, b"local-dirty")
                .unwrap(),
            11
        );
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        let (old_revision, old_head, old_base, dirty_bytes) = {
            let state = state.lock().unwrap();
            assert_eq!(state.open_writers, 1);
            assert!(state.dirty);
            (
                state.inode.revision,
                state.inode.head_version.clone(),
                state
                    .base_version
                    .as_ref()
                    .map(|version| version.id.clone()),
                state.dirty_extents.dirty_data_bytes(),
            )
        };

        let external_version = meta.publish_external_version(18);
        let reader = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();
        assert_eq!(
            fs.getattr(&context(), created.entry.inode, Some(reader))
                .unwrap()
                .size,
            11
        );
        let mut data = [0; 16];
        assert_eq!(fs.read(&context(), reader, 0, &mut data).unwrap(), 11);
        assert_eq!(&data[..11], b"local-dirty");

        let state = state.lock().unwrap();
        assert_eq!(state.inode.revision, old_revision);
        assert_eq!(state.inode.head_version.as_ref(), old_head.as_ref());
        assert_ne!(state.inode.head_version.as_ref(), Some(&external_version));
        assert_eq!(
            state
                .base_version
                .as_ref()
                .map(|version| version.id.clone()),
            old_base
        );
        assert!(state.dirty);
        assert_eq!(state.dirty_extents.dirty_data_bytes(), dirty_bytes);
        drop(state);
        fs.release(&context(), reader).unwrap();
        fs.release(&context(), created.handle).unwrap();
    }

    #[test]
    fn fresh_readonly_open_preserves_inflight_commit_after_external_commit() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("inflight-reader-external.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        assert_eq!(
            fs.write(&context(), created.handle, 0, b"pending").unwrap(),
            7
        );
        meta.fail_next_commit_with(unavailable("injected pending commit failure"));
        assert_eq!(
            fs.fsync(&context(), created.handle, SyncMode::DataOnly)
                .expect_err("injected commit must fail")
                .code(),
            afs_error::NODE_VFS_UNAVAILABLE
        );
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        let (old_revision, old_head, pending_expected_head, pending_version) = {
            let state = state.lock().unwrap();
            let pending = match state.in_flight.as_ref().expect("commit remains in flight") {
                InFlightCommit::File(pending) => pending,
                InFlightCommit::Preparing(_) => panic!("commit should have reached Meta"),
                InFlightCommit::Metadata(_) => panic!("test expects data commit"),
            };
            (
                state.inode.revision,
                state.inode.head_version.clone(),
                pending.batch.commit.expected_head_version.clone(),
                pending.batch.commit.file_version.id.clone(),
            )
        };

        let external_version = meta.publish_external_version(18);
        let reader = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();

        let state = state.lock().unwrap();
        assert_eq!(state.inode.revision, old_revision);
        assert_eq!(state.inode.head_version.as_ref(), old_head.as_ref());
        assert_ne!(state.inode.head_version.as_ref(), Some(&external_version));
        match state
            .in_flight
            .as_ref()
            .expect("pending commit must be preserved")
        {
            InFlightCommit::File(pending) => {
                assert_eq!(
                    pending.batch.commit.expected_head_version.as_ref(),
                    pending_expected_head.as_ref()
                );
                assert_eq!(&pending.batch.commit.file_version.id, &pending_version);
            }
            InFlightCommit::Preparing(_) => {
                panic!("commit should still be exact pending file commit")
            }
            InFlightCommit::Metadata(_) => panic!("test expects data commit"),
        }
        drop(state);
        fs.release(&context(), reader).unwrap();
        fs.release(&context(), created.handle).unwrap();
    }

    #[test]
    fn failed_metadata_only_sync_keeps_request_identity_and_blocks_writes() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("metadata-retry.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"data").unwrap();
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap();
        assert_eq!(meta.commit_count(), 1);
        assert!(meta.metadata_syncs.lock().unwrap().is_empty());

        meta.fail_next_metadata_sync();
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap_err();
        let failed = meta.failed_metadata_syncs.lock().unwrap()[0].clone();
        assert_eq!(
            fs.write(&context(), created.handle, 4, b"!").unwrap(),
            1,
            "write must wait for the pending metadata sync retry before creating new dirty state"
        );
        let metadata_syncs = meta.metadata_syncs.lock().unwrap();
        assert_eq!(metadata_syncs.len(), 1);
        assert_eq!(failed.operation_id, metadata_syncs[0].operation_id);
        assert_eq!(
            failed.expected_inode_revision,
            metadata_syncs[0].expected_inode_revision
        );
        assert_eq!(
            failed.expected_head_version,
            metadata_syncs[0].expected_head_version
        );
    }

    #[test]
    fn full_sync_after_pending_data_only_commit_also_syncs_metadata() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("full-after-data-pending.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"data").unwrap();
        let dirty_mtime = {
            let state = fs
                .write_state(&meta.inode.lock().unwrap().inode_id)
                .unwrap()
                .unwrap();
            state.lock().unwrap().inode.attributes.mtime_unix_ms
        };

        meta.fail_next_commit();
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap_err();
        let failed = meta.failed_commits.lock().unwrap()[0].clone();

        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(failed.operation_id, commits[0].operation_id);
        assert_eq!(failed.file_version.id, commits[0].file_version.id);
        assert_eq!(commits[0].metadata_delta.mode, CommitMetadataMode::DataOnly);
        drop(commits);
        let metadata_syncs = meta.metadata_syncs.lock().unwrap();
        assert_eq!(metadata_syncs.len(), 1);
        assert_eq!(
            metadata_syncs[0].metadata_delta.mtime_unix_ms,
            Some(dirty_mtime)
        );
        assert_eq!(
            metadata_syncs[0].expected_head_version,
            meta.inode.lock().unwrap().head_version
        );
    }

    #[test]
    fn background_writeback_does_not_hold_table_behind_locked_inode() {
        let (_temp, meta, fs) = test_fs();
        let fs = Arc::new(fs);
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("busy-table.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"data").unwrap();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let cell = fs.write_state(&inode_id).unwrap().unwrap();
        let held = cell.lock().unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let writer_fs = fs.clone();
        let writer = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            writer_fs.writeback_pending()
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        // Give the original blocking scan time to acquire the table mutex.
        std::thread::sleep(Duration::from_millis(50));
        let (lookup_tx, lookup_rx) = std::sync::mpsc::channel();
        let lookup_fs = fs.clone();
        let lookup = std::thread::spawn(move || {
            lookup_tx
                .send(lookup_fs.write_state(&inode_id).unwrap().is_some())
                .unwrap();
        });
        let result = lookup_rx.recv_timeout(Duration::from_millis(150));
        drop(held);
        writer.join().unwrap().unwrap();
        lookup.join().unwrap();
        assert!(
            result.unwrap(),
            "busy inode must not block the shared table"
        );
    }

    #[test]
    fn background_writeback_skips_active_commit_and_preserves_exact_request() {
        let (_temp, meta, fs) = test_fs();
        let fs = Arc::new(fs);
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("busy-commit.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"data").unwrap();
        let pause = meta.pause_commit_after_successes(0);
        let commit_fs = fs.clone();
        let commit = std::thread::spawn(move || {
            commit_fs.fsync(&context(), created.handle, SyncMode::DataOnly)
        });
        pause.wait_until_reached();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let cell = fs.write_state(&inode_id).unwrap().unwrap();
        let exact = match cell.lock().unwrap().in_flight.clone().unwrap() {
            InFlightCommit::File(pending) => pending.batch.commit,
            _ => panic!("expected exact prepared file request"),
        };
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let maintenance_fs = fs.clone();
        let maintenance = std::thread::spawn(move || {
            done_tx.send(maintenance_fs.writeback_pending()).unwrap();
        });
        let result = done_rx.recv_timeout(Duration::from_millis(150));
        // Release both workers even on the original regression failure.
        pause.release();
        commit.join().unwrap().unwrap();
        maintenance.join().unwrap();
        assert_eq!(result.unwrap().unwrap(), 0);
        let committed = meta.commits.lock().unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(
            serde_json::to_value(&exact).unwrap(),
            serde_json::to_value(&committed[0]).unwrap()
        );
    }

    fn synthetic_write_state(
        inode_id: InodeId,
        lease: WriteLease,
        dirty: bool,
        metadata_dirty: bool,
        kill_suidgid_dirty: bool,
        operation_busy: bool,
    ) -> SharedInodeWriteState {
        let mut inode = InodeRecord {
            namespace_id: NamespaceId::new("default"),
            inode_id: inode_id.clone(),
            kind: InodeKind::Regular,
            attributes: InodeAttributes {
                mode: 0o640,
                uid: 1000,
                gid: 1000,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            },
            head_version: None,
            symlink_target: None,
            xattrs: std::collections::BTreeMap::new(),
            revision: 1,
        };
        let mut write_lease = lease;
        write_lease.inode_id = inode_id.clone();
        inode.inode_id = inode_id;
        Arc::new(InodeWriteStateCell::new(InodeWriteState {
            inode,
            write_lease,
            base_version: None,
            base_layout: LayoutRoot {
                id: LayoutRootId::new("synthetic-empty-layout"),
                file_length: 0,
                inline_extents: Vec::new(),
            },
            logical_length: 0,
            metadata_dirty,
            kill_suidgid_dirty,
            dirty_extents: DirtyExtentMap::default(),
            in_flight: None,
            commit_busy: false,
            operation_busy,
            dirty,
            next_write_seq: 0,
            visible_write_seq: 0,
            durable_write_seq: 0,
            committed_write_seq: 0,
            open_writers: 0,
            last_writer_background_requested: false,
            background_error: None,
            terminal_error: None,
        }))
    }

    #[test]
    fn background_writeback_rotates_bounded_scan_across_clean_and_busy_entries() {
        let (_temp, meta, fs) = test_fs();
        let lease = meta.lease.lock().unwrap().clone();
        {
            let mut states = fs.inode_writes.lock().unwrap();
            for index in 0..64 {
                let inode_id = InodeId::new(format!("inode:fair-{index:03}"));
                states.insert(
                    inode_id.clone(),
                    synthetic_write_state(
                        inode_id,
                        lease.clone(),
                        index == 63,
                        false,
                        false,
                        index == 63,
                    ),
                );
            }
            let inode_id = InodeId::new("inode:fair-064");
            states.insert(
                inode_id.clone(),
                synthetic_write_state(inode_id, lease, false, false, true, false),
            );
        }

        assert_eq!(
            fs.writeback_pending_with_budget(Duration::from_secs(1), 64)
                .unwrap(),
            0,
            "the first bounded pass should examine only the clean/busy prefix"
        );
        assert!(meta.metadata_syncs.lock().unwrap().is_empty());

        assert_eq!(
            fs.writeback_pending_with_budget(Duration::from_secs(1), 64)
                .unwrap(),
            1,
            "the next pass should rotate to the deferred candidate"
        );
        assert_eq!(meta.metadata_syncs.lock().unwrap().len(), 1);
    }

    #[test]
    fn background_writeback_cursor_advances_after_partial_timed_passes() {
        let (_temp, meta, fs) = test_fs();
        let lease = meta.lease.lock().unwrap().clone();
        {
            let mut states = fs.inode_writes.lock().unwrap();
            for index in 0..4 {
                let inode_id = InodeId::new(format!("inode:partial-{index:03}"));
                states.insert(
                    inode_id.clone(),
                    synthetic_write_state(inode_id, lease.clone(), false, false, index == 3, false),
                );
            }
        }
        *fs.writeback_after_examine_pause.lock().unwrap() = Some(Duration::from_millis(60));
        for pass in 0..3 {
            assert_eq!(
                fs.writeback_pending_with_budget(Duration::from_millis(50), 64)
                    .unwrap(),
                0
            );
            assert_eq!(fs.writeback_cursor.load(Ordering::Relaxed), pass + 1);
        }
        assert!(meta.metadata_syncs.lock().unwrap().is_empty());

        *fs.writeback_after_examine_pause.lock().unwrap() = None;
        assert_eq!(
            fs.writeback_pending_with_budget(Duration::from_secs(1), 1)
                .unwrap(),
            1
        );
        let metadata_syncs = meta.metadata_syncs.lock().unwrap();
        assert_eq!(metadata_syncs.len(), 1);
        assert_eq!(
            metadata_syncs[0].inode_id,
            InodeId::new("inode:partial-003")
        );
    }

    #[test]
    fn background_writeback_passes_remaining_timeout_to_each_candidate() {
        let (_temp, meta, fs) = test_fs();
        let lease = meta.lease.lock().unwrap().clone();
        {
            let mut states = fs.inode_writes.lock().unwrap();
            for index in 0..2 {
                let inode_id = InodeId::new(format!("inode:timeout-{index:03}"));
                let cell = synthetic_write_state(
                    inode_id.clone(),
                    lease.clone(),
                    false,
                    false,
                    true,
                    false,
                );
                cell.lock().unwrap().inode.revision = (index + 1) as u64;
                states.insert(inode_id, cell);
            }
        }

        assert_eq!(
            fs.writeback_pending_with_budget(Duration::from_millis(250), 64)
                .unwrap(),
            2
        );
        let timeouts = meta.metadata_timeouts();
        assert_eq!(timeouts.len(), 2);
        assert!(
            timeouts
                .iter()
                .all(|timeout| *timeout <= Duration::from_millis(250))
        );
        assert!(timeouts.iter().all(|timeout| !timeout.is_zero()));
        assert!(
            timeouts.windows(2).all(|window| window[1] <= window[0]),
            "remaining timeout should not increase across candidates"
        );
    }

    #[test]
    fn background_writeback_expired_before_send_retains_exact_pending_for_replay() {
        let (temp, meta, mut fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("prepared-replay.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"prepared")
            .unwrap();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let cell = fs.write_state(&inode_id).unwrap().unwrap();
        let output = temp.path().join("prepared-trace.jsonl");
        fs.pending_trace = Some(PendingTrace::test_gate(
            &output,
            "default",
            "node-a",
            "session-a",
            &inode_id.0,
        ));

        *fs.writeback_after_prepare_pause.lock().unwrap() = Some(Duration::from_secs(2));
        assert_eq!(
            fs.writeback_pending_with_budget(Duration::from_secs(1), 64)
                .unwrap(),
            0
        );
        assert!(meta.commits.lock().unwrap().is_empty());
        assert!(meta.failed_commits.lock().unwrap().is_empty());
        assert_eq!(std::fs::read_to_string(&output).unwrap().lines().count(), 1);
        let pending_commit = {
            let state = cell.lock().unwrap();
            assert!(!state.commit_busy);
            assert!(state.background_error.is_none());
            match state.in_flight.as_ref().expect("pending commit retained") {
                InFlightCommit::File(pending) => {
                    assert_eq!(
                        pending.batch.commit.metadata_delta.mode,
                        CommitMetadataMode::Full
                    );
                    pending.batch.commit.clone()
                }
                InFlightCommit::Preparing(_) | InFlightCommit::Metadata(_) => {
                    panic!("expected exact prepared file commit")
                }
            }
        };

        meta.fail_next_commit();
        assert!(
            fs.fsync(&context(), created.handle, SyncMode::DataOnly)
                .is_err()
        );
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows[1]["event"], "dfs_pending_commit_first_send");
        assert_eq!(rows[1]["branch"], "pending_reuse");
        assert_eq!(rows[1]["send_attempt"], 1);
        assert_eq!(rows[2]["event"], "dfs_pending_commit_unknown_retained");
        assert_eq!(rows[2]["send_attempt"], 1);
        assert_eq!(meta.failed_commits.lock().unwrap()[0], pending_commit);
        assert_eq!(
            fs.writeback_pending_with_budget(Duration::from_secs(1), 64)
                .unwrap(),
            1
        );
        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(
            commits[0], pending_commit,
            "exact request, lease, CAS and receipts must replay"
        );
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows[3]["event"], "dfs_pending_commit_retry_send");
        assert_eq!(rows[3]["branch"], "pending_reuse");
        assert_eq!(rows[3]["send_attempt"], 2);
        assert_eq!(rows[4]["event"], "dfs_pending_commit_success_cleared");
        assert_eq!(rows[4]["send_attempt"], 2);
        for row in &rows[2..] {
            assert_eq!(row["request_digest"], rows[1]["request_digest"]);
            assert_eq!(row["operation_id"], rows[1]["operation_id"]);
        }
        assert!(cell.lock().unwrap().in_flight.is_none());
    }

    #[test]
    fn background_writeback_expired_before_lease_renewal_defers_cleanly() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:lease-defer");
        let mut lease = meta.lease.lock().unwrap().clone();
        lease.expires_at_unix_ms = 0;
        let cell = synthetic_write_state(inode_id.clone(), lease, true, false, false, false);
        cell.lock()
            .unwrap()
            .dirty_extents
            .write_at(0, b"x", 1)
            .unwrap();
        fs.inode_writes
            .lock()
            .unwrap()
            .insert(inode_id.clone(), cell.clone());

        *fs.writeback_after_examine_pause.lock().unwrap() = Some(Duration::from_millis(2));
        assert_eq!(
            fs.writeback_pending_with_budget(Duration::from_millis(1), 64)
                .unwrap(),
            0
        );
        assert_eq!(meta.renew_call_count(), 0);
        let state = cell.lock().unwrap();
        assert!(state.background_error.is_none());
        assert!(state.dirty);
        assert!(state.in_flight.is_none());
    }

    #[test]
    fn foreground_sync_does_not_use_writeback_meta_timeout_slice() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("foreground-no-budget.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"data").unwrap();
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap();
        assert!(meta.commit_timeouts().is_empty());
        assert!(meta.metadata_timeouts().is_empty());
        assert!(meta.renew_timeouts().is_empty());
    }

    #[test]
    fn deadline_commit_error_preserves_exact_batch_for_retry() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("deadline-retry.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"data").unwrap();

        meta.fail_next_commit_with(Error::coded(
            afs_error::CLIENT_DEADLINE_EXCEEDED,
            "deadline while committing file version",
        ));
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap_err();
        let failed = meta.failed_commits.lock().unwrap()[0].clone();

        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        let commits = meta.commits.lock().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(failed.operation_id, commits[0].operation_id);
        assert_eq!(failed.file_version.id, commits[0].file_version.id);
        assert_eq!(failed.layout_root.id, commits[0].layout_root.id);
    }

    #[test]
    fn remote_status_metadata_error_preserves_exact_request_for_retry() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("remote-status-metadata-retry.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"data").unwrap();
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap();

        meta.fail_next_metadata_sync_with(Error::new(
            afs_error::CLIENT_REMOTE_STATUS,
            afs_error::ErrorKind::Unavailable,
            "remote status without AFS detail",
        ));
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap_err();
        let failed = meta.failed_metadata_syncs.lock().unwrap()[0].clone();

        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        let metadata_syncs = meta.metadata_syncs.lock().unwrap();
        assert_eq!(metadata_syncs.len(), 1);
        assert_eq!(failed.operation_id, metadata_syncs[0].operation_id);
        assert_eq!(
            failed.expected_head_version,
            metadata_syncs[0].expected_head_version
        );
    }

    #[test]
    fn definite_meta_invalid_blocks_subsequent_mutation() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("terminal-invalid.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"data").unwrap();

        meta.fail_next_commit_with(Error::coded(
            afs_error::META_CATALOG_INVALID_REQUEST,
            "definite invalid commit",
        ));
        let error = fs
            .fsync(&context(), created.handle, SyncMode::Full)
            .unwrap_err();
        assert_eq!(error.code(), afs_error::META_CATALOG_INVALID_REQUEST);

        let mut visible = [0; 4];
        assert_eq!(
            fs.read(&context(), created.handle, 0, &mut visible)
                .unwrap(),
            4
        );
        assert_eq!(&visible, b"data");
        assert_eq!(
            fs.write(&context(), created.handle, 4, b"!")
                .unwrap_err()
                .code(),
            afs_error::META_CATALOG_INVALID_REQUEST
        );
        assert_eq!(
            fs.fsync(&context(), created.handle, SyncMode::Full)
                .unwrap_err()
                .code(),
            afs_error::META_CATALOG_INVALID_REQUEST
        );
        assert_eq!(meta.commit_count(), 0);
    }

    #[test]
    fn failed_resize_commit_keeps_batch_identity_and_blocks_new_dirty_for_retry() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("retry-resize.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"abcdefgh")
            .unwrap();
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap();
        fs.setattr(
            &context(),
            created.entry.inode,
            Some(created.handle),
            &AttributeChange {
                size: Some(4),
                ..AttributeChange::default()
            },
        )
        .unwrap();

        meta.fail_next_commit();
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap_err();
        let failed = meta.failed_commits.lock().unwrap()[0].clone();
        let attrs = fs
            .getattr(&context(), created.entry.inode, Some(created.handle))
            .unwrap();
        assert_eq!(attrs.size, 4);
        let mut visible = [0; 8];
        assert_eq!(
            fs.read(&context(), created.handle, 0, &mut visible)
                .unwrap(),
            4
        );
        assert_eq!(&visible[..4], b"abcd");
        assert_eq!(
            fs.write(&context(), created.handle, 4, b"Z").unwrap(),
            1,
            "write must wait for the pending FileVersion retry before appending new dirty state"
        );
        assert_eq!(meta.commit_count(), 2);
        let commits = meta.commits.lock().unwrap();
        let retried = commits.last().unwrap();
        assert_eq!(failed.operation_id, retried.operation_id);
        assert_eq!(failed.file_version.id, retried.file_version.id);
        assert_eq!(failed.layout_root.id, retried.layout_root.id);
        assert_eq!(retried.file_version.length, 4);
        assert_eq!(retried.layout_root.file_length, 4);
        assert!(retried.chunk_receipts.is_empty());
        drop(commits);
        let attrs = fs
            .setattr(
                &context(),
                created.entry.inode,
                Some(created.handle),
                &AttributeChange {
                    size: Some(6),
                    ..AttributeChange::default()
                },
            )
            .unwrap();
        assert_eq!(attrs.size, 6);
    }

    #[test]
    fn pending_trace_unknown_retention_retry_and_actual_clear() {
        let (temp, meta, mut fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("pending-trace.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let inode = fs.inode_id(created.entry.inode).unwrap();
        let output = temp.path().join("trace.jsonl");
        fs.pending_trace = Some(PendingTrace::test_gate(
            &output,
            "default",
            "node-a",
            "session-a",
            &inode.0,
        ));
        fs.write(
            &context(),
            created.handle,
            0,
            b"private-payload-never-traced",
        )
        .unwrap();
        meta.fail_next_commit();
        let error = fs
            .fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
        let cell = fs.write_state(&inode).unwrap().unwrap();
        let pending = match cell.lock().unwrap().in_flight.as_ref().unwrap() {
            InFlightCommit::File(pending) => pending.batch.commit.clone(),
            _ => panic!("expected retained file request"),
        };
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows[1]["event"], "dfs_pending_commit_first_send");
        assert_eq!(rows[2]["event"], "dfs_pending_commit_unknown_retained");
        assert_eq!(rows[1]["send_attempt"], 1);
        assert_eq!(rows[2]["send_attempt"], 1);
        assert_eq!(
            rows[2]["operation_id"],
            serde_json::json!(pending.operation_id)
        );
        assert_eq!(rows[1]["request_digest"], rows[2]["request_digest"]);
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap();
        let text = std::fs::read_to_string(&output).unwrap();
        assert!(!text.contains("private-payload-never-traced"));
        let rows: Vec<serde_json::Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows[3]["event"], "dfs_pending_commit_retry_send");
        assert_eq!(rows[3]["branch"], "pending_reuse");
        assert_eq!(rows[3]["send_attempt"], 2);
        assert_eq!(rows[4]["event"], "dfs_pending_commit_success_cleared");
        assert_eq!(rows[4]["send_attempt"], 2);
        assert_eq!(rows[4]["in_flight"], "None");
        assert_eq!(rows[4]["committed_write_seq"], 1);
        assert!(cell.lock().unwrap().in_flight.is_none());
        assert_eq!(
            meta.failed_commits.lock().unwrap()[0],
            meta.commits.lock().unwrap()[0]
        );
    }

    #[test]
    fn pending_trace_late_arm_does_not_fabricate_first_send() {
        let (temp, meta, mut fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("late-arm.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let inode = fs.inode_id(created.entry.inode).unwrap();
        let output = temp.path().join("late-arm.jsonl");
        let trace = PendingTrace::test_gate(&output, "default", "node-a", "session-a", &inode.0);
        trace.test_unarm();
        fs.pending_trace = Some(trace);
        fs.write(&context(), created.handle, 0, b"data").unwrap();
        meta.fail_next_commit();
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap_err();
        assert_eq!(std::fs::read_to_string(&output).unwrap().lines().count(), 1);
        let cell = fs.write_state(&inode).unwrap().unwrap();
        let pending = match cell.lock().unwrap().in_flight.as_ref().unwrap() {
            InFlightCommit::File(pending) => pending.as_ref().clone(),
            _ => unreachable!(),
        };
        assert_eq!(
            pending
                .trace_send_attempt
                .as_ref()
                .unwrap()
                .load(Ordering::Relaxed),
            1
        );
        fs.pending_trace.as_ref().unwrap().test_arm_target(&inode.0);
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap();
        let rows: Vec<serde_json::Value> = std::fs::read_to_string(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1]["event"], "dfs_pending_commit_retry_send");
        assert_eq!(rows[1]["branch"], "pending_reuse");
        assert_eq!(rows[1]["send_attempt"], 2);
        assert_eq!(rows[2]["event"], "dfs_pending_commit_success_cleared");
        assert_eq!(rows[2]["send_attempt"], 2);
        assert_eq!(
            meta.failed_commits.lock().unwrap()[0],
            meta.commits.lock().unwrap()[0]
        );
        assert!(cell.lock().unwrap().in_flight.is_none());
    }

    #[test]
    fn pending_trace_send_counter_overflow_disables_only_diagnostics() {
        let (temp, meta, mut fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("counter-overflow.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let inode = fs.inode_id(created.entry.inode).unwrap();
        let output = temp.path().join("counter-overflow.jsonl");
        fs.pending_trace = Some(PendingTrace::test_gate(
            &output,
            "default",
            "node-a",
            "session-a",
            &inode.0,
        ));
        fs.write(&context(), created.handle, 0, b"data").unwrap();
        meta.fail_next_commit();
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap_err();
        let before = std::fs::read(&output).unwrap();
        let cell = fs.write_state(&inode).unwrap().unwrap();
        match cell.lock().unwrap().in_flight.as_ref().unwrap() {
            InFlightCommit::File(pending) => pending
                .trace_send_attempt
                .as_ref()
                .unwrap()
                .store(u64::MAX, Ordering::Relaxed),
            _ => unreachable!(),
        }
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap();
        assert!(cell.lock().unwrap().in_flight.is_none());
        assert_eq!(std::fs::read(&output).unwrap(), before);
        assert_eq!(
            meta.failed_commits.lock().unwrap()[0],
            meta.commits.lock().unwrap()[0]
        );
        let mut bytes = [0; 4];
        fs.read(&context(), created.handle, 0, &mut bytes).unwrap();
        assert_eq!(&bytes, b"data");
    }

    #[test]
    fn pending_trace_definite_rejection_and_diagnostic_failure_preserve_fs_results() {
        for bounded in [false, true] {
            let (temp, meta, mut fs) = test_fs();
            let created = fs
                .create(
                    &context(),
                    fs.root_inode(),
                    OsStr::new("trace-rejection.bin"),
                    0o640,
                    libc::O_RDWR,
                )
                .unwrap();
            let inode = fs.inode_id(created.entry.inode).unwrap();
            let output = temp.path().join("trace.jsonl");
            let trace =
                PendingTrace::test_gate(&output, "default", "node-a", "session-a", &inode.0);
            if bounded {
                trace.test_exhaust_records();
            }
            fs.pending_trace = Some(trace);
            fs.write(&context(), created.handle, 0, b"data").unwrap();
            meta.fail_next_commit_with(Error::coded(
                afs_error::META_DFS_CONFLICT,
                "definite conflict",
            ));
            assert_eq!(
                fs.fsync(&context(), created.handle, SyncMode::DataOnly)
                    .unwrap_err()
                    .code(),
                afs_error::META_DFS_CONFLICT
            );
            let cell = fs.write_state(&inode).unwrap().unwrap();
            let state = cell.lock().unwrap();
            assert!(state.in_flight.is_none());
            assert!(state.terminal_error.is_some());
            assert!(!state.dirty_extents.is_empty());
            drop(state);
            let text = std::fs::read_to_string(&output).unwrap();
            assert!(!text.contains("unknown_retained"));
            if !bounded {
                assert!(text.contains("definite_rejection_cleared"));
            }
            let mut bytes = [0; 4];
            fs.read(&context(), created.handle, 0, &mut bytes).unwrap();
            assert_eq!(&bytes, b"data");
        }
    }

    #[test]
    fn pending_trace_rejects_stale_send_clone_without_changing_pending_or_rpc_result() {
        let (temp, meta, mut fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("trace-mismatch.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let inode = fs.inode_id(created.entry.inode).unwrap();
        let output = temp.path().join("trace.jsonl");
        fs.pending_trace = Some(PendingTrace::test_gate(
            &output,
            "default",
            "node-a",
            "session-a",
            &inode.0,
        ));
        fs.write(&context(), created.handle, 0, b"data").unwrap();
        meta.fail_next_commit();
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap_err();
        let before = std::fs::read(&output).unwrap();
        let cell = fs.write_state(&inode).unwrap().unwrap();
        {
            let state = cell.lock().unwrap();
            let mut stale = match state.in_flight.as_ref().unwrap() {
                InFlightCommit::File(p) => p.as_ref().clone(),
                _ => unreachable!(),
            };
            stale.batch.commit.operation_id = OperationId::new("wrong-stale-operation");
            fs.pending_trace.as_ref().unwrap().observe(
                "dfs_pending_commit_unknown_retained",
                "pending_reuse",
                &state,
                &stale,
                Some(&unavailable("unknown")),
            );
        }
        assert_eq!(std::fs::read(&output).unwrap(), before);
        fs.fsync(&context(), created.handle, SyncMode::DataOnly)
            .unwrap();
        assert_eq!(meta.commit_count(), 1);
        assert!(cell.lock().unwrap().in_flight.is_none());
        assert_eq!(
            std::fs::read(&output).unwrap(),
            before,
            "unusable trace remains disabled"
        );
    }

    #[test]
    fn unknown_commit_ack_replays_original_request_after_namespace_revision_change() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("unknown-ack-revision.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.write(&context(), created.handle, 0, b"data").unwrap();

        meta.fail_next_commit();
        fs.fsync(&context(), created.handle, SyncMode::Full)
            .unwrap_err();
        let failed = meta.failed_commits.lock().unwrap()[0].clone();

        meta.advance_namespace_revision();
        let error = fs.write(&context(), created.handle, 4, b"!").unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);

        let cas_failed = meta.cas_failed_commits.lock().unwrap();
        assert_eq!(cas_failed.len(), 1);
        assert_eq!(failed.operation_id, cas_failed[0].operation_id);
        assert_eq!(
            failed.expected_inode_revision,
            cas_failed[0].expected_inode_revision
        );
        assert_eq!(
            failed.expected_head_version,
            cas_failed[0].expected_head_version
        );
        assert_eq!(failed.file_version.id, cas_failed[0].file_version.id);
        assert_eq!(failed.layout_root.id, cas_failed[0].layout_root.id);
    }

    fn lock_owner(session: &str, kernel_owner: u64) -> FileLockOwner {
        FileLockOwner {
            ingress_session_id: session.into(),
            kernel_owner,
        }
    }

    fn lock_range(start: u64, end: u64) -> FileLockRange {
        FileLockRange { start, end }
    }

    fn dfs_lock_authority(
        caller_session_id: &str,
    ) -> afs_protocol::node_control::DfsOwnerLockAuthority {
        afs_protocol::node_control::DfsOwnerLockAuthority {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            lease_expires_at_unix_ms: u64::MAX,
            caller_node_id: "node-b".into(),
            caller_session_id: caller_session_id.into(),
        }
    }

    fn dfs_owner_open_request(
        open_seq: u64,
        open_flags: i32,
    ) -> afs_protocol::node_control::DfsOwnerOpenRequest {
        afs_protocol::node_control::DfsOwnerOpenRequest {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            caller_session_id: "session-b".into(),
            open_flags,
            kill_suidgid: false,
            open_seq,
        }
    }

    fn dfs_owner_cleanup_handle(open_seq: u64) -> afs_protocol::node_control::DfsOwnerHandle {
        afs_protocol::node_control::DfsOwnerHandle {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            caller_node_id: "node-b".into(),
            caller_session_id: "session-b".into(),
            opaque_handle: Vec::new(),
            open_seq,
        }
    }

    struct NoopRemoteDfsOwner;

    macro_rules! noop_remote_owner_method {
        ($method:ident, $request:ty, $reply:ty) => {
            fn $method(&self, _: $request) -> afs_error::Result<$reply> {
                Err(unavailable("noop remote owner should not be called"))
            }
        };
    }

    impl RemoteDfsOwner for NoopRemoteDfsOwner {
        noop_remote_owner_method!(
            open,
            afs_protocol::node_control::DfsOwnerOpenRequest,
            afs_protocol::node_control::DfsOwnerOpenReply
        );
        noop_remote_owner_method!(
            getattr,
            afs_protocol::node_control::DfsOwnerGetAttrRequest,
            afs_protocol::node_control::DfsOwnerGetAttrReply
        );
        noop_remote_owner_method!(
            release,
            afs_protocol::node_control::DfsOwnerReleaseRequest,
            afs_protocol::node_control::DfsOwnerReleaseReply
        );
        noop_remote_owner_method!(
            get_lock,
            afs_protocol::node_control::DfsOwnerGetLockRequest,
            afs_protocol::node_control::DfsOwnerGetLockReply
        );
        noop_remote_owner_method!(
            set_lock,
            afs_protocol::node_control::DfsOwnerSetLockRequest,
            afs_protocol::node_control::DfsOwnerSetLockReply
        );
        noop_remote_owner_method!(
            cancel_lock_wait,
            afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
            afs_protocol::node_control::DfsOwnerCancelLockWaitReply
        );
        noop_remote_owner_method!(
            acknowledge_lock_wait,
            afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
            afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply
        );
        noop_remote_owner_method!(
            release_locks,
            afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
            afs_protocol::node_control::DfsOwnerReleaseLocksReply
        );
        noop_remote_owner_method!(
            release_lock_session,
            afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
            afs_protocol::node_control::DfsOwnerReleaseLockSessionReply
        );
        noop_remote_owner_method!(
            read,
            afs_protocol::node_data::DfsOwnerReadRequest,
            afs_protocol::node_data::DfsOwnerReadReply
        );
        noop_remote_owner_method!(
            write,
            afs_protocol::node_data::DfsOwnerWriteRequest,
            afs_protocol::node_data::DfsOwnerWriteReply
        );
        noop_remote_owner_method!(
            resize,
            afs_protocol::node_data::DfsOwnerResizeRequest,
            afs_protocol::node_data::DfsOwnerResizeReply
        );
        noop_remote_owner_method!(
            sync,
            afs_protocol::node_data::DfsOwnerSyncRequest,
            afs_protocol::node_data::DfsOwnerSyncReply
        );
    }

    struct StaticRemoteOwnerFactory {
        owner: Arc<dyn RemoteDfsOwner>,
    }

    impl DfsRemoteOwnerFactory for StaticRemoteOwnerFactory {
        fn connect(&self, _: DfsNodeLocation) -> Result<Arc<dyn RemoteDfsOwner>> {
            Ok(self.owner.clone())
        }
    }

    struct LoopbackRemoteDfsOwner {
        owner: Arc<DistributedFs>,
        peer: String,
    }

    impl RemoteDfsOwner for LoopbackRemoteDfsOwner {
        fn open(
            &self,
            request: afs_protocol::node_control::DfsOwnerOpenRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn getattr(
            &self,
            request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::getattr(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn release(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn get_lock(
            &self,
            request: afs_protocol::node_control::DfsOwnerGetLockRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::get_lock(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn set_lock(
            &self,
            request: afs_protocol::node_control::DfsOwnerSetLockRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::set_lock(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn cancel_lock_wait(
            &self,
            request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::cancel_lock_wait(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn acknowledge_lock_wait(
            &self,
            request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::acknowledge_lock_wait(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn release_locks(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release_locks(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn release_lock_session(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release_lock_session(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn read(
            &self,
            request: afs_protocol::node_data::DfsOwnerReadRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
            <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::read(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn write(
            &self,
            request: afs_protocol::node_data::DfsOwnerWriteRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
            <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn resize(
            &self,
            request: afs_protocol::node_data::DfsOwnerResizeRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
            <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::resize(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }

        fn sync(
            &self,
            request: afs_protocol::node_data::DfsOwnerSyncRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
            <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::sync(
                self.owner.as_ref(),
                &self.peer,
                request,
            )
        }
    }

    enum ScriptedReleaseAction {
        DelegateThenErr(Error),
        DelegateThenPause(Arc<OwnerOpenFinishPause>),
        ErrWithoutDelegate(Error),
        OkWithoutDelegate,
    }

    struct ScriptedReleaseRemoteDfsOwner {
        inner: Arc<dyn RemoteDfsOwner>,
        releases: Mutex<std::collections::VecDeque<ScriptedReleaseAction>>,
        calls: Mutex<Vec<afs_protocol::node_control::DfsOwnerHandle>>,
        read_pause: Mutex<Option<Arc<OwnerOpenFinishPause>>>,
    }

    impl ScriptedReleaseRemoteDfsOwner {
        fn new(inner: Arc<dyn RemoteDfsOwner>, actions: Vec<ScriptedReleaseAction>) -> Self {
            Self {
                inner,
                releases: Mutex::new(actions.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
                read_pause: Mutex::new(None),
            }
        }

        fn release_call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }

        fn release_opaque_handles(&self) -> Vec<Vec<u8>> {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .map(|handle| handle.opaque_handle.clone())
                .collect()
        }

        fn set_read_pause(&self, pause: Arc<OwnerOpenFinishPause>) {
            *self.read_pause.lock().unwrap() = Some(pause);
        }
    }

    impl RemoteDfsOwner for ScriptedReleaseRemoteDfsOwner {
        fn open(
            &self,
            request: afs_protocol::node_control::DfsOwnerOpenRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
            self.inner.open(request)
        }

        fn getattr(
            &self,
            request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
            self.inner.getattr(request)
        }

        fn release(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
            if let Some(handle) = request.handle.as_ref() {
                self.calls.lock().unwrap().push(handle.clone());
            }
            match self.releases.lock().unwrap().pop_front() {
                None => self.inner.release(request),
                Some(ScriptedReleaseAction::DelegateThenErr(error)) => {
                    self.inner.release(request)?;
                    Err(error)
                }
                Some(ScriptedReleaseAction::DelegateThenPause(pause)) => {
                    let reply = self.inner.release(request)?;
                    pause.pause();
                    Ok(reply)
                }
                Some(ScriptedReleaseAction::ErrWithoutDelegate(error)) => Err(error),
                Some(ScriptedReleaseAction::OkWithoutDelegate) => {
                    Ok(afs_protocol::node_control::DfsOwnerReleaseReply {})
                }
            }
        }

        fn get_lock(
            &self,
            request: afs_protocol::node_control::DfsOwnerGetLockRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
            self.inner.get_lock(request)
        }

        fn set_lock(
            &self,
            request: afs_protocol::node_control::DfsOwnerSetLockRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
            self.inner.set_lock(request)
        }

        fn cancel_lock_wait(
            &self,
            request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
            self.inner.cancel_lock_wait(request)
        }

        fn acknowledge_lock_wait(
            &self,
            request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
            self.inner.acknowledge_lock_wait(request)
        }

        fn release_locks(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
            self.inner.release_locks(request)
        }

        fn release_lock_session(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
            self.inner.release_lock_session(request)
        }

        fn read(
            &self,
            request: afs_protocol::node_data::DfsOwnerReadRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
            let reply = self.inner.read(request)?;
            if let Some(pause) = self.read_pause.lock().unwrap().take() {
                pause.pause();
            }
            Ok(reply)
        }

        fn write(
            &self,
            request: afs_protocol::node_data::DfsOwnerWriteRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
            self.inner.write(request)
        }

        fn resize(
            &self,
            request: afs_protocol::node_data::DfsOwnerResizeRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
            self.inner.resize(request)
        }

        fn sync(
            &self,
            request: afs_protocol::node_data::DfsOwnerSyncRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
            self.inner.sync(request)
        }
    }

    struct BlockingFirstOpenRemoteDfsOwner {
        inner: Arc<dyn RemoteDfsOwner>,
        calls: Mutex<Vec<afs_protocol::node_control::DfsOwnerOpenRequest>>,
        first_entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
        release_first: Mutex<std::sync::mpsc::Receiver<()>>,
        second_entered: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    }

    impl BlockingFirstOpenRemoteDfsOwner {
        fn new(
            inner: Arc<dyn RemoteDfsOwner>,
            first_entered: std::sync::mpsc::Sender<()>,
            release_first: std::sync::mpsc::Receiver<()>,
            second_entered: std::sync::mpsc::Sender<()>,
        ) -> Self {
            Self {
                inner,
                calls: Mutex::new(Vec::new()),
                first_entered: Mutex::new(Some(first_entered)),
                release_first: Mutex::new(release_first),
                second_entered: Mutex::new(Some(second_entered)),
            }
        }

        fn open_requests(&self) -> Vec<afs_protocol::node_control::DfsOwnerOpenRequest> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl RemoteDfsOwner for BlockingFirstOpenRemoteDfsOwner {
        fn open(
            &self,
            request: afs_protocol::node_control::DfsOwnerOpenRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
            let call_index = {
                let mut calls = self.calls.lock().unwrap();
                let call_index = calls.len();
                calls.push(request.clone());
                call_index
            };
            if call_index == 0 {
                if let Some(sender) = self.first_entered.lock().unwrap().take() {
                    sender.send(()).unwrap();
                }
                self.release_first.lock().unwrap().recv().unwrap();
            } else if call_index == 1
                && let Some(sender) = self.second_entered.lock().unwrap().take()
            {
                sender.send(()).unwrap();
            }
            self.inner.open(request)
        }

        fn getattr(
            &self,
            request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
            self.inner.getattr(request)
        }

        fn release(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
            self.inner.release(request)
        }

        fn get_lock(
            &self,
            request: afs_protocol::node_control::DfsOwnerGetLockRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
            self.inner.get_lock(request)
        }

        fn set_lock(
            &self,
            request: afs_protocol::node_control::DfsOwnerSetLockRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
            self.inner.set_lock(request)
        }

        fn cancel_lock_wait(
            &self,
            request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
            self.inner.cancel_lock_wait(request)
        }

        fn acknowledge_lock_wait(
            &self,
            request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
            self.inner.acknowledge_lock_wait(request)
        }

        fn release_locks(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
            self.inner.release_locks(request)
        }

        fn release_lock_session(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
            self.inner.release_lock_session(request)
        }

        fn read(
            &self,
            request: afs_protocol::node_data::DfsOwnerReadRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
            self.inner.read(request)
        }

        fn write(
            &self,
            request: afs_protocol::node_data::DfsOwnerWriteRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
            self.inner.write(request)
        }

        fn resize(
            &self,
            request: afs_protocol::node_data::DfsOwnerResizeRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
            self.inner.resize(request)
        }

        fn sync(
            &self,
            request: afs_protocol::node_data::DfsOwnerSyncRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
            self.inner.sync(request)
        }
    }

    enum ScriptedOpenAction {
        Delegate,
        DelegateThenErr(Error),
    }

    struct ScriptedOpenRemoteDfsOwner {
        inner: Arc<dyn RemoteDfsOwner>,
        opens: Mutex<std::collections::VecDeque<ScriptedOpenAction>>,
        calls: Mutex<Vec<afs_protocol::node_control::DfsOwnerOpenRequest>>,
    }

    impl ScriptedOpenRemoteDfsOwner {
        fn new(inner: Arc<dyn RemoteDfsOwner>, actions: Vec<ScriptedOpenAction>) -> Self {
            Self {
                inner,
                opens: Mutex::new(actions.into_iter().collect()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn open_requests(&self) -> Vec<afs_protocol::node_control::DfsOwnerOpenRequest> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl RemoteDfsOwner for ScriptedOpenRemoteDfsOwner {
        fn open(
            &self,
            request: afs_protocol::node_control::DfsOwnerOpenRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerOpenReply> {
            self.calls.lock().unwrap().push(request.clone());
            match self.opens.lock().unwrap().pop_front() {
                None | Some(ScriptedOpenAction::Delegate) => self.inner.open(request),
                Some(ScriptedOpenAction::DelegateThenErr(error)) => {
                    self.inner.open(request)?;
                    Err(error)
                }
            }
        }

        fn getattr(
            &self,
            request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
            self.inner.getattr(request)
        }

        fn release(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
            self.inner.release(request)
        }

        fn get_lock(
            &self,
            request: afs_protocol::node_control::DfsOwnerGetLockRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
            self.inner.get_lock(request)
        }

        fn set_lock(
            &self,
            request: afs_protocol::node_control::DfsOwnerSetLockRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
            self.inner.set_lock(request)
        }

        fn cancel_lock_wait(
            &self,
            request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
            self.inner.cancel_lock_wait(request)
        }

        fn acknowledge_lock_wait(
            &self,
            request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
            self.inner.acknowledge_lock_wait(request)
        }

        fn release_locks(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
            self.inner.release_locks(request)
        }

        fn release_lock_session(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
            self.inner.release_lock_session(request)
        }

        fn read(
            &self,
            request: afs_protocol::node_data::DfsOwnerReadRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerReadReply> {
            self.inner.read(request)
        }

        fn write(
            &self,
            request: afs_protocol::node_data::DfsOwnerWriteRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerWriteReply> {
            self.inner.write(request)
        }

        fn resize(
            &self,
            request: afs_protocol::node_data::DfsOwnerResizeRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerResizeReply> {
            self.inner.resize(request)
        }

        fn sync(
            &self,
            request: afs_protocol::node_data::DfsOwnerSyncRequest,
        ) -> Result<afs_protocol::node_data::DfsOwnerSyncReply> {
            self.inner.sync(request)
        }
    }

    struct InterruptedSetLockRemoteDfsOwner {
        cancel_calls: AtomicUsize,
        acknowledge_calls: AtomicUsize,
    }

    impl InterruptedSetLockRemoteDfsOwner {
        fn cancel_call_count(&self) -> usize {
            self.cancel_calls.load(Ordering::SeqCst)
        }

        fn acknowledge_call_count(&self) -> usize {
            self.acknowledge_calls.load(Ordering::SeqCst)
        }
    }

    struct CancelAckFailsOnceRemoteDfsOwner {
        cancel_calls: AtomicUsize,
        acknowledge_calls: AtomicUsize,
    }

    impl CancelAckFailsOnceRemoteDfsOwner {
        fn cancel_call_count(&self) -> usize {
            self.cancel_calls.load(Ordering::SeqCst)
        }

        fn acknowledge_call_count(&self) -> usize {
            self.acknowledge_calls.load(Ordering::SeqCst)
        }
    }

    impl RemoteDfsOwner for CancelAckFailsOnceRemoteDfsOwner {
        noop_remote_owner_method!(
            open,
            afs_protocol::node_control::DfsOwnerOpenRequest,
            afs_protocol::node_control::DfsOwnerOpenReply
        );
        noop_remote_owner_method!(
            getattr,
            afs_protocol::node_control::DfsOwnerGetAttrRequest,
            afs_protocol::node_control::DfsOwnerGetAttrReply
        );
        noop_remote_owner_method!(
            release,
            afs_protocol::node_control::DfsOwnerReleaseRequest,
            afs_protocol::node_control::DfsOwnerReleaseReply
        );
        noop_remote_owner_method!(
            get_lock,
            afs_protocol::node_control::DfsOwnerGetLockRequest,
            afs_protocol::node_control::DfsOwnerGetLockReply
        );
        fn set_lock(
            &self,
            _: afs_protocol::node_control::DfsOwnerSetLockRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
            Err(unavailable("injected remote lock failure"))
        }
        fn cancel_lock_wait(
            &self,
            _: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            Ok(afs_protocol::node_control::DfsOwnerCancelLockWaitReply { outcome: 1 })
        }
        fn acknowledge_lock_wait(
            &self,
            _: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
            if self.acknowledge_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(unavailable("injected remote lock ack failure"));
            }
            Ok(afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply {})
        }
        noop_remote_owner_method!(
            release_locks,
            afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
            afs_protocol::node_control::DfsOwnerReleaseLocksReply
        );
        noop_remote_owner_method!(
            release_lock_session,
            afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
            afs_protocol::node_control::DfsOwnerReleaseLockSessionReply
        );
        noop_remote_owner_method!(
            read,
            afs_protocol::node_data::DfsOwnerReadRequest,
            afs_protocol::node_data::DfsOwnerReadReply
        );
        noop_remote_owner_method!(
            write,
            afs_protocol::node_data::DfsOwnerWriteRequest,
            afs_protocol::node_data::DfsOwnerWriteReply
        );
        noop_remote_owner_method!(
            resize,
            afs_protocol::node_data::DfsOwnerResizeRequest,
            afs_protocol::node_data::DfsOwnerResizeReply
        );
        noop_remote_owner_method!(
            sync,
            afs_protocol::node_data::DfsOwnerSyncRequest,
            afs_protocol::node_data::DfsOwnerSyncReply
        );
    }

    impl RemoteDfsOwner for InterruptedSetLockRemoteDfsOwner {
        noop_remote_owner_method!(
            open,
            afs_protocol::node_control::DfsOwnerOpenRequest,
            afs_protocol::node_control::DfsOwnerOpenReply
        );
        noop_remote_owner_method!(
            getattr,
            afs_protocol::node_control::DfsOwnerGetAttrRequest,
            afs_protocol::node_control::DfsOwnerGetAttrReply
        );
        noop_remote_owner_method!(
            release,
            afs_protocol::node_control::DfsOwnerReleaseRequest,
            afs_protocol::node_control::DfsOwnerReleaseReply
        );
        noop_remote_owner_method!(
            get_lock,
            afs_protocol::node_control::DfsOwnerGetLockRequest,
            afs_protocol::node_control::DfsOwnerGetLockReply
        );
        fn set_lock(
            &self,
            _: afs_protocol::node_control::DfsOwnerSetLockRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
            Err(lock_error(LockError::Interrupted))
        }
        fn cancel_lock_wait(
            &self,
            _: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
            self.cancel_calls.fetch_add(1, Ordering::SeqCst);
            Ok(afs_protocol::node_control::DfsOwnerCancelLockWaitReply { outcome: 1 })
        }
        fn acknowledge_lock_wait(
            &self,
            _: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
        ) -> Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
            self.acknowledge_calls.fetch_add(1, Ordering::SeqCst);
            Ok(afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply {})
        }
        noop_remote_owner_method!(
            release_locks,
            afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
            afs_protocol::node_control::DfsOwnerReleaseLocksReply
        );
        noop_remote_owner_method!(
            release_lock_session,
            afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
            afs_protocol::node_control::DfsOwnerReleaseLockSessionReply
        );
        noop_remote_owner_method!(
            read,
            afs_protocol::node_data::DfsOwnerReadRequest,
            afs_protocol::node_data::DfsOwnerReadReply
        );
        noop_remote_owner_method!(
            write,
            afs_protocol::node_data::DfsOwnerWriteRequest,
            afs_protocol::node_data::DfsOwnerWriteReply
        );
        noop_remote_owner_method!(
            resize,
            afs_protocol::node_data::DfsOwnerResizeRequest,
            afs_protocol::node_data::DfsOwnerResizeReply
        );
        noop_remote_owner_method!(
            sync,
            afs_protocol::node_data::DfsOwnerSyncRequest,
            afs_protocol::node_data::DfsOwnerSyncReply
        );
    }

    struct FailOnceReleaseRemoteDfsOwner {
        calls: Mutex<Vec<String>>,
    }

    impl FailOnceReleaseRemoteDfsOwner {
        fn new() -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    impl RemoteDfsOwner for FailOnceReleaseRemoteDfsOwner {
        noop_remote_owner_method!(
            open,
            afs_protocol::node_control::DfsOwnerOpenRequest,
            afs_protocol::node_control::DfsOwnerOpenReply
        );
        noop_remote_owner_method!(
            getattr,
            afs_protocol::node_control::DfsOwnerGetAttrRequest,
            afs_protocol::node_control::DfsOwnerGetAttrReply
        );
        noop_remote_owner_method!(
            release,
            afs_protocol::node_control::DfsOwnerReleaseRequest,
            afs_protocol::node_control::DfsOwnerReleaseReply
        );
        noop_remote_owner_method!(
            get_lock,
            afs_protocol::node_control::DfsOwnerGetLockRequest,
            afs_protocol::node_control::DfsOwnerGetLockReply
        );
        noop_remote_owner_method!(
            set_lock,
            afs_protocol::node_control::DfsOwnerSetLockRequest,
            afs_protocol::node_control::DfsOwnerSetLockReply
        );
        noop_remote_owner_method!(
            cancel_lock_wait,
            afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
            afs_protocol::node_control::DfsOwnerCancelLockWaitReply
        );
        noop_remote_owner_method!(
            acknowledge_lock_wait,
            afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
            afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply
        );
        noop_remote_owner_method!(
            release_locks,
            afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
            afs_protocol::node_control::DfsOwnerReleaseLocksReply
        );

        fn release_lock_session(
            &self,
            request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
        ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply>
        {
            let mut calls = self.calls.lock().unwrap();
            calls.push(request.ingress_session_id);
            if calls.len() == 1 {
                return Err(unavailable("injected remote release failure"));
            }
            Ok(afs_protocol::node_control::DfsOwnerReleaseLockSessionReply {})
        }

        noop_remote_owner_method!(
            read,
            afs_protocol::node_data::DfsOwnerReadRequest,
            afs_protocol::node_data::DfsOwnerReadReply
        );
        noop_remote_owner_method!(
            write,
            afs_protocol::node_data::DfsOwnerWriteRequest,
            afs_protocol::node_data::DfsOwnerWriteReply
        );
        noop_remote_owner_method!(
            resize,
            afs_protocol::node_data::DfsOwnerResizeRequest,
            afs_protocol::node_data::DfsOwnerResizeReply
        );
        noop_remote_owner_method!(
            sync,
            afs_protocol::node_data::DfsOwnerSyncRequest,
            afs_protocol::node_data::DfsOwnerSyncReply
        );
    }

    #[test]
    fn dfs_reaps_lock_scopes_for_expired_peer_process_session() {
        let (_temp, _meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:reap-peer-locks");
        let authority = fs
            .local_lock_authority(
                &inode_id,
                WriteLease {
                    inode_id: inode_id.clone(),
                    owner_node_id: "node-a".into(),
                    owner_session_id: "session-a".into(),
                    lease_epoch: 1,
                    expires_at_unix_ms: u64::MAX,
                },
            )
            .unwrap();
        let owner = FileLockOwner {
            ingress_session_id: DistributedFs::lock_scope("node-z", "old-session", "mount-z")
                .unwrap(),
            kernel_owner: 77,
        };
        let lock = LockRequest::write(FileLockKind::Posix, owner.clone(), 77, lock_range(0, 99));
        authority.table.setlk_nonblocking(lock.clone()).unwrap();
        fs.register_local_lock_success(&authority, &lock).unwrap();
        assert!(
            authority
                .table
                .getlk(&LockRequest::write(
                    FileLockKind::Posix,
                    FileLockOwner {
                        ingress_session_id: DistributedFs::lock_scope(
                            "node-a",
                            "session-a",
                            "mount-a"
                        )
                        .unwrap(),
                        kernel_owner: 78,
                    },
                    78,
                    lock_range(0, 99),
                ))
                .unwrap()
                .is_some()
        );

        assert_eq!(fs.reap_expired_peer_lock_sessions().unwrap(), 1);
        assert!(
            authority
                .table
                .getlk(&LockRequest::write(
                    FileLockKind::Posix,
                    FileLockOwner {
                        ingress_session_id: DistributedFs::lock_scope(
                            "node-a",
                            "session-a",
                            "mount-a"
                        )
                        .unwrap(),
                        kernel_owner: 78,
                    },
                    78,
                    lock_range(0, 99),
                ))
                .unwrap()
                .is_none()
        );
        assert!(authority.state.lock().unwrap().pinned_owners.is_empty());
    }

    #[test]
    fn dfs_release_scoped_session_cleans_scope_when_closed_tombstones_are_full() {
        let (_temp, _meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:closed-capacity");
        let authority = fs
            .local_lock_authority(
                &inode_id,
                WriteLease {
                    inode_id: inode_id.clone(),
                    owner_node_id: "node-a".into(),
                    owner_session_id: "session-a".into(),
                    lease_epoch: 1,
                    expires_at_unix_ms: u64::MAX,
                },
            )
            .unwrap();
        for index in 0..MAX_CLOSED_LOCK_SESSIONS {
            fs.closed_lock_sessions
                .lock()
                .unwrap()
                .insert(format!("closed-{index}"));
        }
        let released_scope = DistributedFs::lock_scope("node-z", "old-session", "mount-z").unwrap();
        let retained_scope =
            DistributedFs::lock_scope("node-y", "live-session", "mount-y").unwrap();
        let released = FileLockOwner {
            ingress_session_id: released_scope.clone(),
            kernel_owner: 10,
        };
        let retained = FileLockOwner {
            ingress_session_id: retained_scope.clone(),
            kernel_owner: 11,
        };
        let released_lock =
            LockRequest::write(FileLockKind::Posix, released.clone(), 10, lock_range(0, 9));
        let retained_lock = LockRequest::write(
            FileLockKind::Posix,
            retained.clone(),
            11,
            lock_range(100, 109),
        );
        authority
            .table
            .setlk_nonblocking(released_lock.clone())
            .unwrap();
        fs.register_local_lock_success(&authority, &released_lock)
            .unwrap();
        authority
            .table
            .setlk_nonblocking(retained_lock.clone())
            .unwrap();
        fs.register_local_lock_success(&authority, &retained_lock)
            .unwrap();

        let mut scopes = HashSet::new();
        scopes.insert(released_scope.clone());
        assert_eq!(fs.release_scoped_lock_sessions(scopes).unwrap(), 1);

        let closed = fs.closed_lock_sessions.lock().unwrap();
        assert_eq!(closed.len(), MAX_CLOSED_LOCK_SESSIONS);
        assert!(!closed.contains(&released_scope));
        drop(closed);
        assert!(
            fs.closed_lock_session_admission_closed
                .load(Ordering::Acquire)
        );
        assert!(fs.ensure_lock_session_open("new-mount").is_err());
        assert!(
            authority
                .table
                .getlk(&LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("probe-a", 20),
                    20,
                    lock_range(0, 9),
                ))
                .unwrap()
                .is_none()
        );
        assert!(
            authority
                .table
                .getlk(&LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("probe-b", 21),
                    21,
                    lock_range(100, 109),
                ))
                .unwrap()
                .is_some()
        );
        assert!(matches!(
            authority.table.setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                released,
                10,
                lock_range(0, 9),
            )),
            Err(LockError::Interrupted)
        ));
        assert!(
            authority
                .state
                .lock()
                .unwrap()
                .pinned_owners
                .contains(&retained)
        );
    }

    #[test]
    fn dfs_remote_lock_session_release_failure_retains_identity_for_retry() {
        let (_temp, _meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:remote-release-retry");
        let owner = Arc::new(FailOnceReleaseRemoteDfsOwner::new());
        let remote = DfsRemoteLockAuthority {
            owner: owner.clone(),
            authority: afs_protocol::node_control::DfsOwnerLockAuthority {
                namespace_id: "default".into(),
                inode_id: inode_id.0.clone(),
                owner_node_id: "node-b".into(),
                owner_session_id: "session-b".into(),
                lease_epoch: 1,
                lease_expires_at_unix_ms: u64::MAX,
                caller_node_id: "node-a".into(),
                caller_session_id: "session-a".into(),
            },
        };
        let scoped_session = DistributedFs::lock_scope("node-a", "session-a", "mount-r").unwrap();
        let waiter = LockWaiterId {
            ingress_session_id: scoped_session.clone(),
            request_id: 7,
        };
        fs.remote_lock_authorities
            .lock()
            .unwrap()
            .insert(inode_id.clone(), remote.clone());
        fs.note_remote_lock_session(&inode_id, &scoped_session)
            .unwrap();
        for index in 0..MAX_CLOSED_LOCK_SESSIONS {
            fs.closed_lock_sessions
                .lock()
                .unwrap()
                .insert(format!("closed-remote-{index}"));
        }
        fs.register_remote_lock_waiter_route(&waiter, remote)
            .unwrap();

        assert_eq!(
            fs.release_lock_session("mount-r").unwrap_err().code(),
            afs_error::NODE_VFS_UNAVAILABLE
        );
        assert_eq!(owner.call_count(), 1);
        let closed = fs.closed_lock_sessions.lock().unwrap();
        assert_eq!(closed.len(), MAX_CLOSED_LOCK_SESSIONS);
        assert!(!closed.contains(&scoped_session));
        drop(closed);
        assert_eq!(
            fs.remote_lock_authority_sessions
                .lock()
                .unwrap()
                .get(&inode_id)
                .unwrap()
                .get(&scoped_session),
            Some(&REMOTE_LOCK_SESSION_PENDING_CLOSE)
        );
        assert_eq!(
            fs.note_remote_lock_session(&inode_id, &scoped_session)
                .unwrap_err()
                .code(),
            afs_error::IO_INTERRUPTED
        );
        assert!(fs.remote_lock_waiters.lock().unwrap().contains_key(&waiter));
        assert!(
            fs.remote_lock_authorities
                .lock()
                .unwrap()
                .contains_key(&inode_id)
        );

        assert_eq!(fs.reap_expired_peer_lock_sessions().unwrap(), 1);
        assert_eq!(owner.call_count(), 2);
        assert!(
            !fs.remote_lock_authority_sessions
                .lock()
                .unwrap()
                .contains_key(&inode_id)
        );
        assert!(!fs.remote_lock_waiters.lock().unwrap().contains_key(&waiter));
        assert!(
            !fs.remote_lock_authorities
                .lock()
                .unwrap()
                .contains_key(&inode_id)
        );
    }

    #[test]
    fn dfs_remote_lock_precancel_survives_replayed_waiter_identity() {
        let (_temp, _meta, fs) = test_fs();
        let remote = DfsRemoteLockAuthority {
            owner: Arc::new(NoopRemoteDfsOwner),
            authority: afs_protocol::node_control::DfsOwnerLockAuthority {
                namespace_id: "default".into(),
                inode_id: "inode:test".into(),
                owner_node_id: "node-b".into(),
                owner_session_id: "session-b".into(),
                lease_epoch: 1,
                lease_expires_at_unix_ms: u64::MAX,
                caller_node_id: "node-a".into(),
                caller_session_id: "session-a".into(),
            },
        };
        let waiter = LockWaiterId {
            ingress_session_id: "mount-a".into(),
            request_id: 43,
        };

        assert!(
            fs.take_remote_lock_waiter_route_or_precancel(waiter.clone())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            fs.register_remote_lock_waiter_route(&waiter, remote.clone())
                .unwrap_err()
                .code(),
            afs_error::IO_INTERRUPTED
        );
        assert_eq!(
            fs.register_remote_lock_waiter_route(&waiter, remote)
                .unwrap_err()
                .code(),
            afs_error::IO_INTERRUPTED
        );
        assert!(fs.remote_lock_waiters.lock().unwrap().is_empty());
    }

    #[test]
    fn dfs_remote_lock_waiter_route_rejects_duplicate_identity() {
        let (_temp, _meta, fs) = test_fs();
        let remote = DfsRemoteLockAuthority {
            owner: Arc::new(NoopRemoteDfsOwner),
            authority: afs_protocol::node_control::DfsOwnerLockAuthority {
                namespace_id: "default".into(),
                inode_id: "inode:test".into(),
                owner_node_id: "node-b".into(),
                owner_session_id: "session-b".into(),
                lease_epoch: 1,
                lease_expires_at_unix_ms: u64::MAX,
                caller_node_id: "node-a".into(),
                caller_session_id: "session-a".into(),
            },
        };
        let waiter = LockWaiterId {
            ingress_session_id: "mount-a".into(),
            request_id: 42,
        };

        fs.register_remote_lock_waiter_route(&waiter, remote.clone())
            .unwrap();
        let error = fs
            .register_remote_lock_waiter_route(&waiter, remote)
            .unwrap_err();

        assert_eq!(error.code(), afs_error::IO_OTHER);
        assert_eq!(fs.remote_lock_waiters.lock().unwrap().len(), 1);
    }

    #[test]
    fn dfs_local_posix_locks_conflict_and_release_by_owner() {
        let (_temp, _meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("locks-local.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let second = fs
            .open(&context(), created.entry.inode, libc::O_RDWR)
            .unwrap();
        let owner_a = lock_owner("mount-a", 10);
        let owner_b = lock_owner("mount-a", 11);

        fs.setlk(
            &context(),
            created.entry.inode,
            created.handle,
            LockRequest::read(FileLockKind::Posix, owner_a.clone(), 100, lock_range(0, 9)),
            None,
        )
        .unwrap();
        let conflict = fs
            .getlk(
                &context(),
                created.entry.inode,
                second,
                LockRequest::write(FileLockKind::Posix, owner_b.clone(), 200, lock_range(0, 9)),
            )
            .unwrap()
            .expect("write lock must observe read-lock conflict");
        assert_eq!(conflict.owner, owner_a);
        assert_eq!(conflict.lock_type, FileLockType::Read);

        fs.setlk(
            &context(),
            created.entry.inode,
            second,
            LockRequest::write(FileLockKind::Posix, owner_b.clone(), 200, lock_range(0, 9)),
            None,
        )
        .unwrap_err();
        let authority = fs
            .lock_authorities
            .lock()
            .unwrap()
            .get(&InodeId::new("inode:test"))
            .cloned()
            .expect("local lock authority exists");
        assert!(
            !authority
                .state
                .lock()
                .unwrap()
                .pinned_owners
                .contains(&owner_b),
            "failed nonblocking lock must not pin owner or renew activity"
        );
        fs.release_locks(
            &context(),
            created.entry.inode,
            created.handle,
            owner_a,
            ReleaseKind::PosixOwner,
        )
        .unwrap();
        fs.setlk(
            &context(),
            created.entry.inode,
            second,
            LockRequest::write(FileLockKind::Posix, owner_b, 200, lock_range(0, 9)),
            None,
        )
        .unwrap();
    }

    #[test]
    fn dfs_owner_lock_authority_is_bound_to_authenticated_peer() {
        let (_temp, _meta, fs) = test_fs();
        let authority = afs_protocol::node_control::DfsOwnerLockAuthority {
            namespace_id: "default".into(),
            inode_id: "inode:test".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            lease_expires_at_unix_ms: u64::MAX,
            caller_node_id: "node-b".into(),
            caller_session_id: "session-b".into(),
        };
        let request = afs_protocol::node_control::DfsOwnerGetLockRequest {
            authority: Some(authority.clone()),
            lock: Some(DistributedFs::wire_lock_request(&LockRequest::read(
                FileLockKind::Posix,
                lock_owner("mount-b", 7),
                77,
                lock_range(0, 0),
            ))),
        };
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::get_lock(
            &fs,
            "node-b",
            request.clone(),
        )
        .unwrap();
        assert_eq!(
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::get_lock(
                &fs, "node-c", request,
            )
            .unwrap_err()
            .code(),
            afs_error::NODE_DFS_STALE_HANDLE
        );
    }

    #[test]
    fn dfs_owner_lock_release_is_scoped_by_authenticated_process_session() {
        let (_temp, _meta, fs) = test_fs();
        let authority = dfs_lock_authority("session-b");
        let lock = LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-b", 70),
            70,
            lock_range(0, 99),
        );
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::set_lock(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerSetLockRequest {
                authority: Some(authority.clone()),
                lock: Some(DistributedFs::wire_lock_request(&lock)),
                waiter: None,
            },
        )
        .unwrap();

        let mut forged_authority = authority.clone();
        forged_authority.caller_session_id = "session-other".into();
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release_locks(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseLocksRequest {
                authority: Some(forged_authority),
                owner: Some(DistributedFs::wire_lock_owner(&lock.owner)),
                release_kind: DistributedFs::wire_lock_release_kind(ReleaseKind::PosixOwner) as i32,
            },
        )
        .unwrap();

        let conflict =
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::get_lock(
                &fs,
                "node-b",
                afs_protocol::node_control::DfsOwnerGetLockRequest {
                    authority: Some(authority.clone()),
                    lock: Some(DistributedFs::wire_lock_request(&LockRequest::write(
                        FileLockKind::Posix,
                        lock_owner("mount-c", 71),
                        71,
                        lock_range(0, 99),
                    ))),
                },
            )
            .unwrap()
            .conflict
            .map(DistributedFs::file_lock_conflict_from_wire)
            .transpose()
            .unwrap()
            .expect("forged release must not drop another process session's lock");
        assert_eq!(conflict.owner, lock.owner);

        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release_locks(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseLocksRequest {
                authority: Some(authority.clone()),
                owner: Some(DistributedFs::wire_lock_owner(&lock.owner)),
                release_kind: DistributedFs::wire_lock_release_kind(ReleaseKind::PosixOwner) as i32,
            },
        )
        .unwrap();
        assert!(
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::get_lock(
                &fs,
                "node-b",
                afs_protocol::node_control::DfsOwnerGetLockRequest {
                    authority: Some(authority),
                    lock: Some(DistributedFs::wire_lock_request(&LockRequest::write(
                        FileLockKind::Posix,
                        lock_owner("mount-c", 71),
                        71,
                        lock_range(0, 99),
                    ))),
                },
            )
            .unwrap()
            .conflict
            .is_none()
        );
    }

    #[test]
    fn dfs_remote_lock_session_presence_is_not_refcounted_by_lock_count() {
        let (_temp, _meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:remote-presence");
        assert!(fs.note_remote_lock_session(&inode_id, "mount-a").unwrap());
        assert!(!fs.note_remote_lock_session(&inode_id, "mount-a").unwrap());
        let sessions = fs.remote_lock_authority_sessions.lock().unwrap();
        assert_eq!(sessions.get(&inode_id).unwrap().get("mount-a"), Some(&1));
        drop(sessions);
        fs.forget_remote_lock_session(&inode_id, "mount-a").unwrap();
        assert!(
            !fs.remote_lock_authority_sessions
                .lock()
                .unwrap()
                .contains_key(&inode_id)
        );
    }

    #[test]
    fn remote_interrupted_lock_wait_does_not_issue_redundant_cancel_ack() {
        let (_temp, meta, fs_raw) = test_fs();
        {
            let mut lease = meta.lease.lock().unwrap();
            lease.owner_node_id = "node-c".into();
            lease.owner_session_id = "session-c".into();
            lease.expires_at_unix_ms = u64::MAX;
        }
        let remote = Arc::new(InterruptedSetLockRemoteDfsOwner {
            cancel_calls: AtomicUsize::new(0),
            acknowledge_calls: AtomicUsize::new(0),
        });
        let fs = fs_raw.with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote.clone() as Arc<dyn RemoteDfsOwner>,
        }));
        let inode = fs.backend_inode(&InodeId::new("inode:test")).unwrap();
        let handle = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
        let error = fs
            .setlk(
                &context(),
                inode,
                handle,
                LockRequest::read(
                    FileLockKind::Posix,
                    lock_owner("mount-b", 94),
                    94,
                    lock_range(0, 99),
                ),
                Some(LockWaiterId {
                    ingress_session_id: "mount-b".into(),
                    request_id: 94,
                }),
            )
            .unwrap_err();
        assert_eq!(error.code(), afs_error::IO_INTERRUPTED);
        assert_eq!(remote.cancel_call_count(), 0);
        assert_eq!(remote.acknowledge_call_count(), 0);
        let scoped_waiter = LockWaiterId {
            ingress_session_id: DistributedFs::lock_scope("node-a", "session-a", "mount-b")
                .unwrap(),
            request_id: 94,
        };
        assert!(
            fs.remote_lock_waiters
                .lock()
                .unwrap()
                .contains_key(&scoped_waiter)
        );
        assert!(
            fs.cancelled_remote_lock_waiters
                .lock()
                .unwrap()
                .contains(&scoped_waiter)
        );

        assert_eq!(fs.reap_expired_peer_lock_sessions().unwrap(), 1);
        assert_eq!(remote.acknowledge_call_count(), 1);
        assert!(
            !fs.remote_lock_waiters
                .lock()
                .unwrap()
                .contains_key(&scoped_waiter)
        );
        assert!(
            !fs.cancelled_remote_lock_waiters
                .lock()
                .unwrap()
                .contains(&scoped_waiter)
        );
    }

    #[test]
    fn remote_cancel_ack_failure_retains_waiter_for_background_retry() {
        let (_temp, meta, fs_raw) = test_fs();
        {
            let mut lease = meta.lease.lock().unwrap();
            lease.owner_node_id = "node-c".into();
            lease.owner_session_id = "session-c".into();
            lease.expires_at_unix_ms = u64::MAX;
        }
        let remote = Arc::new(CancelAckFailsOnceRemoteDfsOwner {
            cancel_calls: AtomicUsize::new(0),
            acknowledge_calls: AtomicUsize::new(0),
        });
        let fs = fs_raw.with_remote_owner_factory(Arc::new(StaticRemoteOwnerFactory {
            owner: remote.clone() as Arc<dyn RemoteDfsOwner>,
        }));
        let inode = fs.backend_inode(&InodeId::new("inode:test")).unwrap();
        let handle = fs.open(&context(), inode, libc::O_RDONLY).unwrap();
        let error = fs
            .setlk(
                &context(),
                inode,
                handle,
                LockRequest::read(
                    FileLockKind::Posix,
                    lock_owner("mount-b", 95),
                    95,
                    lock_range(0, 99),
                ),
                Some(LockWaiterId {
                    ingress_session_id: "mount-b".into(),
                    request_id: 95,
                }),
            )
            .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_VFS_UNAVAILABLE);
        assert_eq!(remote.cancel_call_count(), 1);
        assert_eq!(remote.acknowledge_call_count(), 1);
        let scoped_waiter = LockWaiterId {
            ingress_session_id: DistributedFs::lock_scope("node-a", "session-a", "mount-b")
                .unwrap(),
            request_id: 95,
        };
        assert!(
            fs.remote_lock_waiters
                .lock()
                .unwrap()
                .contains_key(&scoped_waiter)
        );
        assert!(
            fs.cancelled_remote_lock_waiters
                .lock()
                .unwrap()
                .contains(&scoped_waiter)
        );

        assert_eq!(fs.reap_expired_peer_lock_sessions().unwrap(), 1);
        assert_eq!(remote.acknowledge_call_count(), 2);
        assert!(
            !fs.remote_lock_waiters
                .lock()
                .unwrap()
                .contains_key(&scoped_waiter)
        );
        assert!(
            !fs.cancelled_remote_lock_waiters
                .lock()
                .unwrap()
                .contains(&scoped_waiter)
        );
    }

    #[test]
    fn dfs_owner_lock_session_release_is_scoped_by_authenticated_process_session() {
        let (_temp, _meta, fs) = test_fs();
        let authority = dfs_lock_authority("session-b");
        let lock = LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-b", 80),
            80,
            lock_range(0, 99),
        );
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::set_lock(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerSetLockRequest {
                authority: Some(authority.clone()),
                lock: Some(DistributedFs::wire_lock_request(&lock)),
                waiter: None,
            },
        )
        .unwrap();

        let mut forged_authority = authority.clone();
        forged_authority.caller_session_id = "session-other".into();
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release_lock_session(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest {
                authority: Some(forged_authority),
                ingress_session_id: "mount-b".into(),
            },
        )
        .unwrap();
        assert!(
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::get_lock(
                &fs,
                "node-b",
                afs_protocol::node_control::DfsOwnerGetLockRequest {
                    authority: Some(authority.clone()),
                    lock: Some(DistributedFs::wire_lock_request(&LockRequest::write(
                        FileLockKind::Posix,
                        lock_owner("mount-c", 81),
                        81,
                        lock_range(0, 99),
                    ))),
                },
            )
            .unwrap()
            .conflict
            .is_some()
        );

        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release_lock_session(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest {
                authority: Some(authority.clone()),
                ingress_session_id: "mount-b".into(),
            },
        )
        .unwrap();
        assert!(
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::get_lock(
                &fs,
                "node-b",
                afs_protocol::node_control::DfsOwnerGetLockRequest {
                    authority: Some(authority),
                    lock: Some(DistributedFs::wire_lock_request(&LockRequest::write(
                        FileLockKind::Posix,
                        lock_owner("mount-c", 81),
                        81,
                        lock_range(0, 99),
                    ))),
                },
            )
            .unwrap()
            .conflict
            .is_none()
        );
    }

    #[test]
    fn dfs_owner_cancel_before_authority_precancels_replayed_waiter() {
        let (_temp, _meta, fs) = test_fs();
        let authority = dfs_lock_authority("session-b");
        let waiter = afs_protocol::node_control::DfsOwnerLockWaiter {
            ingress_session_id: "mount-b".into(),
            request_id: 91,
        };
        let cancel = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::cancel_lock_wait(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerCancelLockWaitRequest {
                authority: Some(authority.clone()),
                waiter: Some(waiter.clone()),
            },
        )
        .unwrap();
        assert_eq!(cancel.outcome, 1);

        let lock = LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-b", 91),
            91,
            lock_range(0, 99),
        );
        let error =
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::set_lock(
                &fs,
                "node-b",
                afs_protocol::node_control::DfsOwnerSetLockRequest {
                    authority: Some(authority),
                    lock: Some(DistributedFs::wire_lock_request(&lock)),
                    waiter: Some(waiter),
                },
            )
            .unwrap_err();
        assert_eq!(error.code(), afs_error::IO_INTERRUPTED);
    }

    #[test]
    fn interrupted_owner_lock_wait_does_not_run_post_mutation_renewal() {
        let (_temp, meta, fs_raw) = test_fs();
        let fs = Arc::new(fs_raw);
        let authority = dfs_lock_authority("session-b");
        let inode_id = InodeId::new("inode:test");
        let held = LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-b", 92),
            92,
            lock_range(0, 99),
        );
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::set_lock(
            fs.as_ref(),
            "node-b",
            afs_protocol::node_control::DfsOwnerSetLockRequest {
                authority: Some(authority.clone()),
                lock: Some(DistributedFs::wire_lock_request(&held)),
                waiter: None,
            },
        )
        .unwrap();
        let renews_after_grant = meta.renew_call_count();
        let waiter = afs_protocol::node_control::DfsOwnerLockWaiter {
            ingress_session_id: "mount-b".into(),
            request_id: 93,
        };
        let blocked = LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-b", 93),
            93,
            lock_range(0, 99),
        );
        let fs_for_waiter = fs.clone();
        let authority_for_waiter = authority.clone();
        let waiter_for_thread = waiter.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            let result =
                <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::set_lock(
                    fs_for_waiter.as_ref(),
                    "node-b",
                    afs_protocol::node_control::DfsOwnerSetLockRequest {
                        authority: Some(authority_for_waiter),
                        lock: Some(DistributedFs::wire_lock_request(&blocked)),
                        waiter: Some(waiter_for_thread),
                    },
                )
                .map(|_| ());
            let _ = done_tx.send(result);
        });
        let mut registered = false;
        for _ in 0..100 {
            registered = fs
                .lock_authorities
                .lock()
                .unwrap()
                .values()
                .any(|authority| !authority.state.lock().unwrap().waiters.is_empty());
            if registered {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            registered,
            "waiter should be registered before cancellation"
        );
        let local_authority = fs
            .lock_authorities
            .lock()
            .unwrap()
            .get(&inode_id)
            .cloned()
            .expect("owner lock authority must exist");
        local_authority
            .state
            .lock()
            .unwrap()
            .lease
            .expires_at_unix_ms = now_unix_ms().saturating_add(4_000);
        let renews_after_waiter_registered = meta.renew_call_count();
        let pause = meta.pause_next_renew();
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::cancel_lock_wait(
            fs.as_ref(),
            "node-b",
            afs_protocol::node_control::DfsOwnerCancelLockWaitRequest {
                authority: Some(authority),
                waiter: Some(waiter),
            },
        )
        .unwrap();
        let result = match done_rx.recv_timeout(Duration::from_millis(100)) {
            Ok(result) => result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                pause.release();
                join.join().unwrap();
                panic!("interrupted waiter was blocked behind post-mutation lock renewal");
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("interrupted waiter thread exited without reporting a result");
            }
        };
        join.join().unwrap();
        let error = result.expect_err("interrupted waiter must return an error");
        assert_eq!(error.code(), afs_error::IO_INTERRUPTED);
        assert_eq!(meta.renew_call_count(), renews_after_waiter_registered);
        assert!(!pause.was_reached());
        assert!(meta.renew_call_count() >= renews_after_grant);
    }

    #[test]
    fn dfs_owner_lock_waiter_must_share_owner_ingress_scope() {
        let (_temp, _meta, fs) = test_fs();
        let authority = dfs_lock_authority("session-b");
        let lock = LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-b", 90),
            90,
            lock_range(0, 99),
        );
        let error =
            <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::set_lock(
                &fs,
                "node-b",
                afs_protocol::node_control::DfsOwnerSetLockRequest {
                    authority: Some(authority),
                    lock: Some(DistributedFs::wire_lock_request(&lock)),
                    waiter: Some(afs_protocol::node_control::DfsOwnerLockWaiter {
                        ingress_session_id: "mount-other".into(),
                        request_id: 90,
                    }),
                },
            )
            .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_VFS_INVALID);
    }

    #[test]
    fn dfs_writable_reopen_uses_fresh_authority_without_legacy_open_write() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("write-authority-reopen.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.release(&context(), created.handle).unwrap();

        let opens_before = meta.open_write_calls.load(Ordering::SeqCst);
        let reopened = fs
            .open(&context(), created.entry.inode, libc::O_RDWR)
            .unwrap();
        fs.release(&context(), reopened).unwrap();

        assert_eq!(
            meta.open_write_calls.load(Ordering::SeqCst),
            opens_before,
            "new writable opens must resolve fresh write authority without invoking legacy OpenWrite"
        );
        assert_eq!(meta.resolve_write_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn dfs_writable_open_near_expiry_clean_state_reacquires_and_renews() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("near-expiry-clean-reacquire.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.release(&context(), created.handle).unwrap();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let near_expiry = now_unix_ms().saturating_add(1_000);
        {
            let mut lease = meta.lease.lock().unwrap();
            lease.expires_at_unix_ms = near_expiry;
        }
        {
            let state = fs.write_state(&inode_id).unwrap().unwrap();
            {
                let mut locked = state.lock().unwrap();
                locked.write_lease.expires_at_unix_ms = near_expiry;
                locked.last_writer_background_requested = false;
                assert!(DistributedFs::write_state_can_adopt_fresh_lease(&locked));
            }
            assert!(DistributedFs::write_state_should_reacquire_clean_lease(&state).unwrap());
        }
        let renewed_expiry = now_unix_ms().saturating_add(30_000);
        meta.queue_renew_result(Ok(meta.lease_with_expiry(renewed_expiry)));
        let opens_before = meta.open_write_calls.load(Ordering::SeqCst);
        let resolves_before = meta.resolve_write_calls.load(Ordering::SeqCst);
        let renews_before = meta.renew_call_count();

        let reopened = fs
            .open(&context(), created.entry.inode, libc::O_RDWR)
            .unwrap();

        assert_eq!(
            meta.resolve_write_calls.load(Ordering::SeqCst),
            resolves_before + 1
        );
        assert_eq!(
            meta.open_write_calls.load(Ordering::SeqCst),
            opens_before + 1,
            "clean near-expiry writable open must preserve existing ensure_inode_write_state reacquire"
        );
        assert_eq!(meta.renew_call_count(), renews_before + 1);
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        assert!(state.lock().unwrap().write_lease.expires_at_unix_ms >= renewed_expiry);
        fs.release(&context(), reopened).unwrap();
    }

    #[test]
    fn dfs_writable_open_near_expiry_protected_state_renews_without_reacquire() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("near-expiry-protected-renew.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let inode_id = meta.inode.lock().unwrap().inode_id.clone();
        let near_expiry = now_unix_ms().saturating_add(1_000);
        {
            let mut lease = meta.lease.lock().unwrap();
            lease.expires_at_unix_ms = near_expiry;
        }
        {
            let state = fs.write_state(&inode_id).unwrap().unwrap();
            state.lock().unwrap().write_lease.expires_at_unix_ms = near_expiry;
        }
        let renewed_expiry = now_unix_ms().saturating_add(30_000);
        meta.queue_renew_result(Ok(meta.lease_with_expiry(renewed_expiry)));
        let opens_before = meta.open_write_calls.load(Ordering::SeqCst);
        let resolves_before = meta.resolve_write_calls.load(Ordering::SeqCst);
        let renews_before = meta.renew_call_count();

        let reopened = fs
            .open(&context(), created.entry.inode, libc::O_RDWR)
            .unwrap();

        assert_eq!(
            meta.resolve_write_calls.load(Ordering::SeqCst),
            resolves_before + 1
        );
        assert_eq!(
            meta.open_write_calls.load(Ordering::SeqCst),
            opens_before,
            "protected near-expiry writable state must renew current state instead of reacquiring"
        );
        assert_eq!(meta.renew_call_count(), renews_before + 1);
        let state = fs.write_state(&inode_id).unwrap().unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.write_lease.lease_epoch, 1);
        assert!(state.write_lease.expires_at_unix_ms >= renewed_expiry);
        drop(state);
        fs.release(&context(), reopened).unwrap();
        fs.release(&context(), created.handle).unwrap();
    }

    #[test]
    fn dfs_readonly_lock_handle_resolves_fresh_authority_without_writable_open() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("readonly-lock.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let readonly = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();
        let opens_before = meta.open_write_calls.load(Ordering::SeqCst);
        fs.setlk(
            &context(),
            created.entry.inode,
            readonly,
            LockRequest::read(
                FileLockKind::Posix,
                lock_owner("mount-a", 201),
                201,
                lock_range(0, 99),
            ),
            None,
        )
        .unwrap();
        for _ in 0..64 {
            assert!(
                fs.getlk(
                    &context(),
                    created.entry.inode,
                    created.handle,
                    LockRequest::write(
                        FileLockKind::Posix,
                        lock_owner("mount-b", 202),
                        202,
                        lock_range(0, 99)
                    )
                )
                .unwrap()
                .is_some()
            );
        }
        assert_eq!(meta.open_write_calls.load(Ordering::SeqCst), opens_before);
        assert_eq!(
            meta.resolve_lock_calls.load(Ordering::SeqCst),
            65,
            "every lock operation must still resolve current Meta authority"
        );
    }

    #[test]
    fn dfs_lock_authority_epoch_replaces_old_table() {
        let (_temp, meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("lock-epoch.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let owner_a = lock_owner("mount-a", 30);
        fs.setlk(
            &context(),
            created.entry.inode,
            created.handle,
            LockRequest::write(FileLockKind::Posix, owner_a, 30, lock_range(0, 99)),
            None,
        )
        .unwrap();
        {
            let mut lease = meta.lease.lock().unwrap();
            lease.lease_epoch = lease.lease_epoch.saturating_add(1);
        }
        let conflict = fs
            .getlk(
                &context(),
                created.entry.inode,
                created.handle,
                LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-b", 31),
                    31,
                    lock_range(0, 99),
                ),
            )
            .unwrap();
        assert!(
            conflict.is_none(),
            "old epoch locks must not survive fencing"
        );
    }

    #[test]
    fn dfs_lock_renewal_transient_error_preserves_locks_and_waiters() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:test");
        {
            let mut lease = meta.lease.lock().unwrap();
            lease.expires_at_unix_ms = now_unix_ms().saturating_add(4_000);
        }
        let authority = fs
            .local_lock_authority(&inode_id, meta.lease.lock().unwrap().clone())
            .unwrap();
        let owner = lock_owner("mount-a", 40);
        let lock = LockRequest::write(FileLockKind::Posix, owner.clone(), 40, lock_range(0, 99));
        let waiter = LockWaiterId {
            ingress_session_id: "mount-b".into(),
            request_id: 41,
        };
        authority.table.setlk_nonblocking(lock).unwrap();
        {
            let mut state = authority.state.lock().unwrap();
            state.pinned_owners.insert(owner.clone());
            state.waiters.insert(waiter.clone());
        }

        meta.fail_next_renew_with(Error::coded(
            afs_error::META_DFS_LEASE_RETRY,
            "injected retryable renewal contention",
        ));
        assert_eq!(
            fs.check_lock_renewal(&authority).unwrap_err().code(),
            afs_error::META_DFS_LEASE_RETRY
        );

        let state = authority.state.lock().unwrap();
        assert!(state.last_error.is_none());
        assert!(state.pinned_owners.contains(&owner));
        assert!(state.waiters.contains(&waiter));
        drop(state);
        assert!(
            authority
                .table
                .getlk(&LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-c", 42),
                    42,
                    lock_range(0, 99),
                ))
                .unwrap()
                .is_some(),
            "retryable renewal errors must not clear the active lock table"
        );
    }

    #[test]
    fn dfs_lock_authority_refresh_waits_for_renewal_guard_and_preserves_terminal_state() {
        let (_temp, meta, fs) = test_fs();
        let fs = Arc::new(fs);
        let inode_id = InodeId::new("inode:test");
        let authority = fs
            .local_lock_authority(&inode_id, meta.lease.lock().unwrap().clone())
            .unwrap();
        let renewal = lock_authority_renewal_guard(&authority).unwrap();
        let worker_fs = fs.clone();
        let worker_inode = inode_id.clone();
        let worker_lease = meta.lease_with_expiry(now_unix_ms().saturating_add(30_000));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            done_tx
                .send(worker_fs.local_lock_authority(&worker_inode, worker_lease))
                .unwrap();
        });
        started_rx.recv().unwrap();
        assert!(
            done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "same-epoch authority refresh must serialize behind an in-flight renewal decision"
        );
        {
            let mut state = authority.state.lock().unwrap();
            state.last_error = Some(stale("foreground renewal fenced authority"));
        }
        let _ = authority.table.invalidate();
        drop(renewal);
        assert_eq!(
            done_rx
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
                .err()
                .expect("same-epoch refresh must remain fenced")
                .code(),
            afs_error::NODE_DFS_STALE_HANDLE,
            "same-epoch refresh must not revive a terminal authority once the serialized renewal finishes"
        );
        worker.join().unwrap();
        assert!(authority.state.lock().unwrap().last_error.is_some());
        assert!(matches!(
            authority.table.getlk(&LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-b", 39),
                39,
                lock_range(0, 99),
            )),
            Err(LockError::Interrupted)
        ));
    }

    #[test]
    fn dfs_lock_renewal_retry_after_expiry_records_local_stale_error() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:test");
        let attempted = meta.lease.lock().unwrap().clone();
        let authority = fs
            .local_lock_authority(&inode_id, attempted.clone())
            .unwrap();
        let expired_at = now_unix_ms().saturating_sub(1);
        {
            let mut state = authority.state.lock().unwrap();
            state.lease.expires_at_unix_ms = expired_at;
            assert_eq!(state.lease.expires_at_unix_ms, expired_at);
        }
        let mut attempted = attempted;
        attempted.expires_at_unix_ms = expired_at;

        assert_eq!(
            apply_lock_renewal_failure(
                &authority,
                &attempted,
                Error::coded(
                    afs_error::META_DFS_LEASE_RETRY,
                    "retry exhausted after expiry"
                ),
            )
            .unwrap()
            .unwrap()
            .code(),
            afs_error::NODE_DFS_STALE_HANDLE
        );
        assert_eq!(
            authority
                .state
                .lock()
                .unwrap()
                .last_error
                .as_ref()
                .unwrap()
                .code(),
            afs_error::NODE_DFS_STALE_HANDLE,
            "local expiry is the terminal cause; a retryable Meta error must not become the terminal lock state"
        );
    }

    #[test]
    fn dfs_lock_expiry_waits_for_inflight_renewal_completion() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:test");
        let authority = fs
            .local_lock_authority(&inode_id, meta.lease.lock().unwrap().clone())
            .unwrap();
        let expired_at = now_unix_ms().saturating_sub(1);
        {
            let mut state = authority.state.lock().unwrap();
            state.lease.expires_at_unix_ms = expired_at;
            assert_eq!(state.lease.expires_at_unix_ms, expired_at);
        }
        let renewal = lock_authority_renewal_guard(&authority).unwrap();
        let worker_authority = authority.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let _renewal = lock_authority_renewal_guard(&worker_authority).unwrap();
            done_tx
                .send(expire_lock_authority_if_due_locked(&worker_authority).unwrap())
                .unwrap();
        });
        started_rx.recv().unwrap();
        assert!(
            done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "background expiry must not complete while a foreground renewal owns the authority guard"
        );
        authority.state.lock().unwrap().lease.expires_at_unix_ms =
            now_unix_ms().saturating_add(30_000);
        drop(renewal);
        assert!(
            !done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "a foreground renewal that extends the same authority before releasing the guard prevents stale expiry"
        );
        worker.join().unwrap();
        assert!(authority.state.lock().unwrap().last_error.is_none());
    }

    #[test]
    fn dfs_lock_renewal_stale_retry_error_does_not_fence_extended_state() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:test");
        let attempted = {
            let mut lease = meta.lease.lock().unwrap();
            lease.expires_at_unix_ms = now_unix_ms().saturating_add(1_000);
            lease.clone()
        };
        let authority = fs
            .local_lock_authority(&inode_id, attempted.clone())
            .unwrap();
        let owner = lock_owner("mount-a", 45);
        let lock = LockRequest::write(FileLockKind::Posix, owner.clone(), 45, lock_range(0, 99));
        authority.table.setlk_nonblocking(lock).unwrap();
        {
            let mut state = authority.state.lock().unwrap();
            state.pinned_owners.insert(owner.clone());
            state.lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
        }

        assert!(
            apply_lock_renewal_failure(
                &authority,
                &attempted,
                Error::coded(afs_error::META_DFS_LEASE_RETRY, "stale renewal retry"),
            )
            .unwrap()
            .is_none()
        );

        let state = authority.state.lock().unwrap();
        assert!(state.last_error.is_none());
        assert!(state.pinned_owners.contains(&owner));
        drop(state);
        assert!(
            authority
                .table
                .getlk(&LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-c", 46),
                    46,
                    lock_range(0, 99),
                ))
                .unwrap()
                .is_some(),
            "a stale renewal response must not invalidate a fresher local authority"
        );
    }

    #[test]
    fn dfs_lock_renewal_confirmed_fence_invalidates_same_identity_extended_state() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:test");
        let attempted = {
            let mut lease = meta.lease.lock().unwrap();
            lease.expires_at_unix_ms = now_unix_ms().saturating_add(1_000);
            lease.clone()
        };
        let authority = fs
            .local_lock_authority(&inode_id, attempted.clone())
            .unwrap();
        {
            let mut state = authority.state.lock().unwrap();
            state.lease.expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
        }

        assert_eq!(
            apply_lock_renewal_failure(
                &authority,
                &attempted,
                Error::coded(afs_error::META_DFS_CONFLICT, "confirmed lease fencing"),
            )
            .unwrap()
            .unwrap()
            .code(),
            afs_error::META_DFS_CONFLICT
        );

        assert_eq!(
            authority
                .state
                .lock()
                .unwrap()
                .last_error
                .as_ref()
                .unwrap()
                .code(),
            afs_error::META_DFS_CONFLICT
        );
        assert!(matches!(
            authority.table.getlk(&LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-c", 48),
                48,
                lock_range(0, 99),
            )),
            Err(LockError::Interrupted)
        ));
    }

    #[test]
    fn dfs_local_setlk_postgrant_transient_renewal_keeps_lock() {
        let (_temp, meta, fs) = test_fs();
        let near_expiry = now_unix_ms().saturating_add(1_000);
        {
            let mut lease = meta.lease.lock().unwrap();
            lease.expires_at_unix_ms = near_expiry;
        }
        meta.queue_renew_result(Ok(meta.lease_with_expiry(near_expiry)));
        meta.queue_renew_result(Ok(meta.lease_with_expiry(near_expiry)));
        meta.queue_renew_result(Err(Error::coded(
            afs_error::META_DFS_LEASE_RETRY,
            "post-grant renewal retry",
        )));

        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("postgrant-local-lock.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.setlk(
            &context(),
            created.entry.inode,
            created.handle,
            LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-a", 48),
                48,
                lock_range(0, 99),
            ),
            None,
        )
        .unwrap();

        let authority = fs
            .lock_authorities
            .lock()
            .unwrap()
            .get(&InodeId::new("inode:test"))
            .cloned()
            .expect("successful lock must keep a local authority");
        assert_eq!(
            meta.renew_call_count(),
            3,
            "local lock regression must inject the retryable error after the lock table mutation"
        );
        assert!(authority.state.lock().unwrap().last_error.is_none());
        assert!(
            authority
                .table
                .getlk(&LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-b", 49),
                    49,
                    lock_range(0, 99),
                ))
                .unwrap()
                .is_some(),
            "post-grant transient renewal errors must not leave a granted lock reported as failed"
        );
    }

    #[test]
    fn dfs_owner_set_lock_postgrant_transient_renewal_keeps_lock() {
        let (_temp, meta, fs) = test_fs();
        let near_expiry = now_unix_ms().saturating_add(1_000);
        {
            let mut lease = meta.lease.lock().unwrap();
            lease.expires_at_unix_ms = near_expiry;
        }
        meta.queue_renew_result(Ok(meta.lease_with_expiry(near_expiry)));
        meta.queue_renew_result(Ok(meta.lease_with_expiry(near_expiry)));
        meta.queue_renew_result(Err(Error::coded(
            afs_error::META_DFS_LEASE_RETRY,
            "remote post-grant renewal retry",
        )));

        let authority = dfs_lock_authority("session-b");
        let lock = LockRequest::write(
            FileLockKind::Posix,
            lock_owner("mount-b", 50),
            50,
            lock_range(0, 99),
        );
        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::set_lock(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerSetLockRequest {
                authority: Some(authority),
                lock: Some(DistributedFs::wire_lock_request(&lock)),
                waiter: None,
            },
        )
        .unwrap();

        let authority = fs
            .lock_authorities
            .lock()
            .unwrap()
            .get(&InodeId::new("inode:test"))
            .cloned()
            .expect("successful owner lock must keep a local authority");
        assert_eq!(
            meta.renew_call_count(),
            3,
            "remote owner lock regression must inject the retryable error after the owner table mutation"
        );
        assert!(authority.state.lock().unwrap().last_error.is_none());
        assert!(
            authority
                .table
                .getlk(&LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-c", 51),
                    51,
                    lock_range(0, 99),
                ))
                .unwrap()
                .is_some(),
            "remote post-grant transient renewal errors must preserve the installed owner lock"
        );
    }

    #[test]
    fn dfs_lock_renewal_success_accepts_live_response_after_old_expiry() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:test");
        let expired = {
            let mut lease = meta.lease.lock().unwrap();
            lease.expires_at_unix_ms = now_unix_ms().saturating_sub(1);
            lease.clone()
        };
        let authority = fs.local_lock_authority(&inode_id, expired).unwrap();
        let renewed_expiry = now_unix_ms().saturating_add(30_000);

        assert!(
            apply_lock_renewal_success(&authority, meta.lease_with_expiry(renewed_expiry))
                .unwrap()
                .is_none(),
            "a same-identity live renewal proves continuous authority even if the old local snapshot expired"
        );
        let state = authority.state.lock().unwrap();
        assert!(state.last_error.is_none());
        assert!(state.lease.expires_at_unix_ms >= renewed_expiry);
    }

    #[test]
    fn dfs_lock_renewal_old_success_hint_does_not_shorten_newer_state() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:test");
        let current_expiry = now_unix_ms().saturating_add(30_000);
        let authority = fs
            .local_lock_authority(&inode_id, meta.lease_with_expiry(current_expiry))
            .unwrap();
        let stale_success_expiry = now_unix_ms().saturating_sub(1);

        assert!(
            apply_lock_renewal_success(&authority, meta.lease_with_expiry(stale_success_expiry))
                .unwrap()
                .is_none()
        );
        let state = authority.state.lock().unwrap();
        assert!(state.last_error.is_none());
        assert_eq!(state.lease.expires_at_unix_ms, current_expiry);
    }

    #[test]
    fn dfs_lock_renewal_success_does_not_resurrect_terminal_authority() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:test");
        let attempted = meta.lease_with_expiry(now_unix_ms().saturating_add(1_000));
        let authority = fs
            .local_lock_authority(&inode_id, attempted.clone())
            .unwrap();
        authority
            .table
            .setlk_nonblocking(LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-a", 52),
                52,
                lock_range(0, 99),
            ))
            .unwrap();
        apply_lock_renewal_failure(
            &authority,
            &attempted,
            Error::coded(afs_error::META_DFS_CONFLICT, "previous confirmed fencing"),
        )
        .unwrap()
        .unwrap();
        let original_expiry = authority.state.lock().unwrap().lease.expires_at_unix_ms;
        let renewed_expiry = now_unix_ms().saturating_add(30_000);

        assert_eq!(
            apply_lock_renewal_success(&authority, meta.lease_with_expiry(renewed_expiry))
                .unwrap()
                .unwrap()
                .code(),
            afs_error::META_DFS_CONFLICT
        );
        let state = authority.state.lock().unwrap();
        assert_eq!(state.lease.expires_at_unix_ms, original_expiry);
        assert_eq!(
            state.last_error.as_ref().unwrap().code(),
            afs_error::META_DFS_CONFLICT
        );
        assert!(state.pinned_owners.is_empty());
        drop(state);
        assert!(matches!(
            authority.table.getlk(&LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-b", 53),
                53,
                lock_range(0, 99),
            )),
            Err(LockError::Interrupted)
        ));
    }

    #[test]
    fn dfs_lock_renewal_terminal_fence_invalidates_locks_and_waiters() {
        let (_temp, meta, fs) = test_fs();
        let inode_id = InodeId::new("inode:test");
        {
            let mut lease = meta.lease.lock().unwrap();
            lease.expires_at_unix_ms = now_unix_ms().saturating_add(4_000);
        }
        let authority = fs
            .local_lock_authority(&inode_id, meta.lease.lock().unwrap().clone())
            .unwrap();
        let owner = lock_owner("mount-a", 50);
        let lock = LockRequest::write(FileLockKind::Posix, owner.clone(), 50, lock_range(0, 99));
        let waiter = LockWaiterId {
            ingress_session_id: "mount-b".into(),
            request_id: 51,
        };
        authority.table.setlk_nonblocking(lock).unwrap();
        {
            let mut state = authority.state.lock().unwrap();
            state.pinned_owners.insert(owner);
            state.waiters.insert(waiter);
        }

        meta.fail_next_renew_with(Error::coded(
            afs_error::META_DFS_CONFLICT,
            "injected lease fencing",
        ));
        assert_eq!(
            fs.check_lock_renewal(&authority).unwrap_err().code(),
            afs_error::META_DFS_CONFLICT
        );

        let state = authority.state.lock().unwrap();
        assert!(state.last_error.is_some());
        assert!(state.pinned_owners.is_empty());
        assert!(state.waiters.is_empty());
        drop(state);
        assert!(matches!(
            authority.table.getlk(&LockRequest::write(
                FileLockKind::Posix,
                lock_owner("mount-c", 52),
                52,
                lock_range(0, 99),
            )),
            Err(LockError::Interrupted)
        ));
    }

    #[test]
    fn dfs_lock_background_renewal_starts_with_retry_budget() {
        let mut lease = WriteLease {
            inode_id: InodeId::new("inode:test"),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            expires_at_unix_ms: now_unix_ms().saturating_add(21_000),
        };
        assert!(!should_background_renew(&lease));
        assert!(!should_renew(&lease));

        lease.expires_at_unix_ms = now_unix_ms().saturating_add(19_000);
        assert!(should_background_renew(&lease));
        assert!(!should_renew(&lease));
    }

    #[test]
    fn dfs_release_locks_without_authority_does_not_open_write() {
        let (_temp, _meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("release-no-lock.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.release_locks(
            &context(),
            created.entry.inode,
            created.handle,
            lock_owner("mount-a", 20),
            ReleaseKind::PosixOwner,
        )
        .unwrap();
        assert!(fs.lock_authorities.lock().unwrap().is_empty());
        assert!(fs.remote_lock_authorities.lock().unwrap().is_empty());
    }

    #[test]
    fn dfs_posix_lock_checks_file_access() {
        let (_temp, _meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("lock-access.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        let readonly = fs
            .open(&context(), created.entry.inode, libc::O_RDONLY)
            .unwrap();
        let writeonly = fs
            .open(&context(), created.entry.inode, libc::O_WRONLY)
            .unwrap();
        assert_eq!(
            fs.setlk(
                &context(),
                created.entry.inode,
                readonly,
                LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-a", 21),
                    21,
                    lock_range(0, 0),
                ),
                None,
            )
            .unwrap_err()
            .code(),
            afs_error::IO_BAD_FILE_DESCRIPTOR
        );
        assert_eq!(
            fs.getlk(
                &context(),
                created.entry.inode,
                writeonly,
                LockRequest::read(
                    FileLockKind::Posix,
                    lock_owner("mount-a", 22),
                    22,
                    lock_range(0, 0),
                ),
            )
            .unwrap_err()
            .code(),
            afs_error::IO_BAD_FILE_DESCRIPTOR
        );
    }

    #[test]
    fn dfs_closed_lock_session_rejects_late_waiter() {
        let (_temp, _meta, fs) = test_fs();
        let created = fs
            .create(
                &context(),
                fs.root_inode(),
                OsStr::new("closed-lock-session.bin"),
                0o640,
                libc::O_RDWR,
            )
            .unwrap();
        fs.release_lock_session("mount-closed").unwrap();
        assert_eq!(
            fs.setlk(
                &context(),
                created.entry.inode,
                created.handle,
                LockRequest::write(
                    FileLockKind::Posix,
                    lock_owner("mount-closed", 23),
                    23,
                    lock_range(0, 0),
                ),
                Some(LockWaiterId {
                    ingress_session_id: "mount-closed".into(),
                    request_id: 1,
                }),
            )
            .unwrap_err()
            .code(),
            afs_error::IO_INTERRUPTED
        );
    }

    #[test]
    fn remote_owner_append_retry_is_idempotent_and_dirty_read_is_visible() {
        let (_temp, _meta, fs) = test_fs();
        let open = <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::open(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerOpenRequest {
                namespace_id: "default".into(),
                inode_id: "inode:test".into(),
                owner_node_id: "node-a".into(),
                owner_session_id: "session-a".into(),
                lease_epoch: 1,
                caller_session_id: "session-b".into(),
                open_flags: libc::O_RDWR | libc::O_APPEND,
                kill_suidgid: false,
                open_seq: 1,
            },
        )
        .unwrap();
        let handle = open.handle.expect("owner open returns handle");
        let data_handle = DistributedFs::data_owner_handle(&handle);

        let write = afs_protocol::node_data::DfsOwnerWriteRequest {
            handle: Some(data_handle.clone()),
            operation_id: "node-b-op-1".into(),
            offset: 0,
            data: b"hi".to_vec(),
            append: true,
            kill_suidgid: false,
        };
        let first = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
            &fs,
            "node-b",
            write.clone(),
        )
        .unwrap();
        let retry = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
            &fs,
            "node-b",
            write.clone(),
        )
        .unwrap();
        assert_eq!(first, retry);
        assert_eq!(first.written, 2);
        let mut changed = afs_protocol::node_data::DfsOwnerWriteRequest {
            data: b"changed".to_vec(),
            ..write.clone()
        };
        changed.operation_id = "node-b-op-1".into();
        assert!(
            <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::write(
                &fs, "node-b", changed
            )
            .unwrap_err()
            .message()
            .contains("reused with a different request body")
        );

        let reply = <DistributedFs as crate::node::rpc::data::DfsOwnerFilesHandler>::read(
            &fs,
            "node-b",
            afs_protocol::node_data::DfsOwnerReadRequest {
                handle: Some(data_handle.clone()),
                offset: 0,
                length: 8,
            },
        )
        .unwrap();
        assert_eq!(reply.data, b"hi");

        <DistributedFs as crate::node::rpc::control::DfsOwnerLifecycleHandler>::release(
            &fs,
            "node-b",
            afs_protocol::node_control::DfsOwnerReleaseRequest {
                handle: Some(handle),
            },
        )
        .unwrap();
    }
}
