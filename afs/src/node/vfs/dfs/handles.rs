use super::*;

#[derive(Clone)]
pub(super) struct NativeReadView {
    pub(super) generation: u64,
    pub(super) authority: NativeReadAuthoritySnapshot,
    pub(super) deadline: Instant,
}

/// The inode version a handle reads, together with the GC reference that keeps
/// that version (R94). Every holder of the `Arc` keeps the reference: the handle
/// while the view is current, and each in-flight read that captured it until
/// the read ends. Publishing a newer view therefore never releases a version a
/// read still uses, and the last holder's drop queues the release.
pub(super) struct OpenedView {
    inode: InodeRecord,
    gc_pin: Option<Arc<crate::node::gc::GcPin>>,
}

impl OpenedView {
    pub(super) fn new(inode: InodeRecord, gc_pin: Option<Arc<crate::node::gc::GcPin>>) -> Self {
        Self { inode, gc_pin }
    }

    /// Whether the handle protects its version with a GC reference. A handle
    /// opened without one (no GC references, an owner-side handle, the
    /// diagnostics unpinned reader) keeps moving without one.
    pub(super) fn is_pinned(&self) -> bool {
        self.gc_pin.is_some()
    }
}

impl std::ops::Deref for OpenedView {
    type Target = InodeRecord;

    fn deref(&self) -> &InodeRecord {
        &self.inode
    }
}

/// A local view a handle should move to but could not protect yet: building
/// its GC reference failed. The handle keeps reading its current, protected
/// view; the next read or local commit tries again (R94).
pub(super) struct PendingView {
    pub(super) inode: Arc<InodeRecord>,
    pub(super) retry_after: Instant,
}

/// Earliest retry after a failed GC reference for a pending view, so a Meta
/// outage does not add a Meta round trip to every read.
#[cfg(not(test))]
const PENDING_VIEW_RETRY_INTERVAL: Duration = Duration::from_secs(1);
#[cfg(test)]
const PENDING_VIEW_RETRY_INTERVAL: Duration = Duration::from_millis(0);

pub(super) struct DfsFileHandle {
    pub(super) gc_pin: Option<Arc<crate::node::gc::GcPin>>,
    pub(super) inode_id: InodeId,
    pub(super) opened_inode: InodeRecord,
    pub(super) opened_page_identity: Option<PageCacheIdentity>,
    pub(super) native_read_view: Option<NativeReadView>,
    pub(super) native_read_generation: u64,
    pub(super) native_denial_serial: u64,
    pub(super) write_session: Option<DfsRemoteWriteSession>,
    pub(super) flags: i32,
    pub(super) owner_scope: Option<DfsOwnerHandleScope>,
}

pub(super) struct DfsHandle {
    pub(super) direct_prefetch:
        std::sync::OnceLock<Arc<crate::node::read::direct_prefetch::Controller>>,
    pub(super) inode_id: InodeId,
    pub(super) flags: i32,
    pub(super) owner_scope: Option<DfsOwnerHandleScope>,
    pub(super) state: Mutex<HandleMut>,
    pub(super) closed: AtomicBool,
    // Set once this Node had write activity or a proxy provider on the inode
    // while the handle was open. Reads then check that state; with no
    // activity left they fall back to the handle's pinned version.
    pub(super) dynamic_read_route: AtomicBool,
}

pub(super) struct HandleMut {
    /// The version this handle reads and its GC reference. Replaced only as a
    /// whole: a new version is published only together with its reference.
    pub(super) opened_inode: Arc<OpenedView>,
    pub(super) pending_view: Option<PendingView>,
    /// The version this Node's proxied writes produced, when the inode could
    /// not be looked up as they ended: the next read moves the handle to it if
    /// it is still the head, or gives up and keeps the current view.
    pub(super) refresh_view: Option<ProxiedCommit>,
    pub(super) opened_page_identity: Option<PageCacheIdentity>,
    pub(super) native_read_view: Option<NativeReadView>,
    pub(super) native_read_generation: u64,
    pub(super) native_denial_serial: u64,
    // Accepted/synced sequences and the observed error cursor change after open.
    pub(super) write_session: Option<DfsRemoteWriteSession>,
    pub(super) read_sequence: crate::node::read::object_fetch::ReadSequence,
}

pub(super) type PinnedReadView = Result<(Arc<OpenedView>, NativeReadView)>;

impl From<DfsFileHandle> for DfsHandle {
    fn from(opened: DfsFileHandle) -> Self {
        Self {
            direct_prefetch: std::sync::OnceLock::new(),
            inode_id: opened.inode_id,
            flags: opened.flags,
            owner_scope: opened.owner_scope,
            state: Mutex::new(HandleMut {
                opened_inode: Arc::new(OpenedView::new(opened.opened_inode, opened.gc_pin)),
                pending_view: None,
                refresh_view: None,
                opened_page_identity: opened.opened_page_identity,
                native_read_view: opened.native_read_view,
                native_read_generation: opened.native_read_generation,
                native_denial_serial: opened.native_denial_serial,
                write_session: opened.write_session,
                read_sequence: crate::node::read::object_fetch::ReadSequence::default(),
            }),
            closed: AtomicBool::new(false),
            dynamic_read_route: AtomicBool::new(false),
        }
    }
}

type HandleShard = RwLock<HashMap<u64, Arc<DfsHandle>>>;
type InodeHandleIndex = Mutex<HashMap<InodeId, HashMap<u64, Weak<DfsHandle>>>>;

/// Structural locks protect Arc registration only. Registration/removal takes
/// the inode index before the handle shard; callers drop both before HandleMut,
/// inode write state, or RPCs. No HandleMut guard may cross an RPC or acquire an
/// inode write state guard. Lifecycle snapshots contain metadata owners, not data.
///
/// The table also owns the id counter shared by file and directory handles
/// and the directory-handle map (a separate lock, never nested with the file
/// shards).
pub(super) struct DfsHandleTable {
    shards: [HandleShard; DFS_HANDLE_SHARDS],
    by_inode: [InodeHandleIndex; DFS_HANDLE_SHARDS],
    next_handle: AtomicU64,
    pub(super) directories: Mutex<HashMap<u64, Arc<Mutex<DfsDirectoryHandle>>>>,
}

impl Default for DfsHandleTable {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(HashMap::new())),
            by_inode: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            next_handle: AtomicU64::new(1),
            directories: Mutex::new(HashMap::new()),
        }
    }
}

impl DfsHandleTable {
    /// Next file or directory handle id; ids are never reused.
    pub(super) fn next_id(&self) -> u64 {
        self.next_handle.fetch_add(1, Ordering::Relaxed)
    }

    fn inode_shard(inode_id: &InodeId) -> usize {
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        inode_id.hash(&mut hash);
        (hash.finish() as usize) % DFS_HANDLE_SHARDS
    }

    pub(super) fn get(&self, id: u64) -> Result<Option<Arc<DfsHandle>>> {
        record_metadata_table_lock();
        Ok(self.shards[id as usize % DFS_HANDLE_SHARDS]
            .read()
            .map_err(|_| unavailable("DFS handle shard is poisoned"))?
            .get(&id)
            .cloned())
    }

    pub(super) fn insert(
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

    pub(super) fn remove(&self, id: u64) -> Result<Option<Arc<DfsHandle>>> {
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
            if let Some(prefetch) = handle.direct_prefetch.get() {
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

    pub(super) fn for_inode(&self, inode_id: &InodeId) -> Result<Vec<Arc<DfsHandle>>> {
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

    pub(super) fn mark_dynamic(&self, inode_id: &InodeId) -> Result<()> {
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

    pub(super) fn snapshot(&self) -> Result<Vec<(u64, Arc<DfsHandle>)>> {
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
    pub(super) fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read().unwrap().len())
            .sum()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[cfg(test)]
    pub(super) fn clear(&self) {
        for (id, _) in self.snapshot().unwrap() {
            self.remove(id).unwrap();
        }
    }
}

/// An open directory lists Meta pages on demand (R72). Entries fetched so far
/// stay in name order; a FUSE cookie is the index after an entry, so seekdir
/// within the fetched range is exact. The next page starts strictly after the
/// last fetched name, so an entry not changed while open appears exactly once.
pub(super) struct DfsDirectoryHandle {
    pub(super) _gc_pin: Option<Arc<crate::node::gc::GcPin>>,
    pub(super) inode_id: InodeId,
    pub(super) entries: Vec<DirectoryEntry>,
    /// Raw name of the last fetched entry: the next page's cursor.
    pub(super) last_name: Vec<u8>,
    /// Meta reported no entry after `last_name`.
    pub(super) eof: bool,
}

impl DistributedFs {
    pub(super) fn allocate_handle(&self, mut handle: DfsFileHandle) -> Result<FileHandle> {
        let id = self.handles.next_id();
        let inode_id = handle.inode_id.clone();
        #[cfg(feature = "diagnostics")]
        let unpinned_reader = handle.flags & libc::O_ACCMODE == libc::O_RDONLY
            && crate::node::fault::experiment::reader_pin_off();
        #[cfg(not(feature = "diagnostics"))]
        let unpinned_reader = false;
        if handle.gc_pin.is_none()
            && !unpinned_reader
            && let Some(references) = &self.gc.references
        {
            // The reference protects the inode, its head and the version
            // this handle opened (decision 4a: readers are pinned to it until
            // close); other historical versions are collected (R81 F2).
            handle.gc_pin =
                Some(references.pin(&inode_id, handle.opened_inode.head_version.as_ref())?);
        }
        let active = self.local_write_activity(&inode_id)?;
        self.handles.insert(id, handle, || {
            let dynamic = (active && self.write_state(&inode_id)?.is_some())
                || self
                    .write_coordinator
                    .current_remote_provider(&inode_id)?
                    .is_some();
            #[cfg(test)]
            {
                let hook = self
                    .hooks
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
}

#[derive(Clone)]
pub(super) struct DfsFileHandleSnapshot {
    pub(super) handle: Arc<DfsHandle>,
    pub(super) inode_id: InodeId,
    pub(super) opened_inode: Arc<OpenedView>,
    pub(super) native_read_view: Option<NativeReadView>,
    pub(super) native_read_generation: u64,
    pub(super) native_denial_serial: u64,
    pub(super) write_session: Option<DfsRemoteWriteSession>,
    pub(super) flags: i32,
    pub(super) owner_scope: Option<DfsOwnerHandleScope>,
}

impl DfsFileHandleSnapshot {
    pub(super) fn from_handle(handle: &Arc<DfsHandle>) -> Result<Self> {
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

impl DistributedFs {
    pub(super) fn update_handle_write_progress(
        &self,
        handle: FileHandle,
        accepted_seq: u64,
    ) -> Result<()> {
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

    /// Removes the handle and its writer. `Ok(Some(_))`: the handle is gone and
    /// this write error had not reached any caller yet.
    pub(super) fn release_file_handle(&self, handle: FileHandle) -> Result<Option<Error>> {
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
        let Some(session) = session else {
            return Ok(None);
        };
        if session.remote.is_some() {
            self.close_remote_write_session(&removed.inode_id, &session)?;
            return Ok(None);
        }
        let undelivered = self.release_writer(&session.local)?;
        self.publish_deferred_views(&removed.inode_id)?;
        self.retire_base_reference(&removed.inode_id)?;
        Ok(undelivered)
    }

    /// What a sync barrier on this local handle has reached: this handle's
    /// acknowledged sequence and the inode's committed version.
    pub(super) fn local_sync_receipt(&self, handle: FileHandle) -> Result<SyncReceipt> {
        self.sync_receipt_with_proxied_commit(handle)
            .map(|(receipt, _)| receipt)
    }

    pub(super) fn sync_receipt_with_proxied_commit(
        &self,
        handle: FileHandle,
    ) -> Result<(SyncReceipt, Option<ProxiedCommit>)> {
        let snapshot = self.handle_snapshot(handle)?;
        let durable_write_seq = snapshot
            .write_session
            .as_ref()
            .map_or(0, |session| session.local.last_synced_seq);
        let (file_version_id, proxied_commit) = self
            .write_state(&snapshot.inode_id)?
            .and_then(|state| {
                let state = state.lock().ok()?;
                let version = state
                    .base_version
                    .as_ref()
                    .map_or_else(String::new, |version| version.id.0.clone());
                let proxied_commit = snapshot
                    .owner_scope
                    .as_ref()
                    .and(snapshot.write_session.as_ref())
                    .and_then(|session| state.proxied_commit(&session.local.id));
                Some((version, proxied_commit))
            })
            .unwrap_or_default();
        Ok((
            SyncReceipt {
                file_version_id,
                durable_write_seq,
            },
            proxied_commit,
        ))
    }

    pub(super) fn update_handle_error_cursor(&self, handle: FileHandle, cursor: u64) -> Result<()> {
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

    pub(super) fn local_write_activity(&self, inode_id: &InodeId) -> Result<bool> {
        let Some(state) = self.write_state(inode_id)? else {
            return Ok(false);
        };
        let state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        Ok(state.has_local_write_activity())
    }

    pub(super) fn update_handles_after_commit(
        &self,
        inode_id: &InodeId,
        committed_seq: u64,
    ) -> Result<()> {
        let inode = self
            .write_state(inode_id)?
            .ok_or_else(|| stale("DFS write state is no longer open"))?
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?
            .inode
            .clone();
        // This Node's commits are visible to every handle on it (4a).
        self.advance_handles(&inode, Some(committed_seq), false)
    }

    /// Move every open handle on the inode to `inode`, this Node's newest view
    /// of it. Only this Node's own write activity calls this; other nodes'
    /// commits reach a handle only through a new open (decision 4a).
    pub(super) fn advance_to_local_view(&self, inode: &InodeRecord) -> Result<()> {
        self.advance_handles(inode, None, true)
    }

    /// Publish `inode` as the read view of every open handle on it.
    ///
    /// A handle that protects its version with a GC reference moves only
    /// together with a reference for the new version (R94): the reference is
    /// built first, outside every handle lock, and handles of one Node share it
    /// per (inode, version), so one local commit costs at most one admission.
    /// If the admission fails the handle keeps its current, still protected
    /// view and records the new one as pending; the next read or commit tries
    /// again. `committed_seq` is already durable and is recorded either way:
    /// a failed reference never turns a committed write into an uncommitted one.
    ///
    /// While a local writer is open every read of the inode on this Node goes
    /// through its write state, not a handle's view; such a read protects the
    /// base it selects itself (`base_read_reference`), whether or not the write
    /// lease is still valid. Publication then waits as a pending view, without
    /// an admission, until the last local writer closes
    /// (`publish_deferred_views`) or a read or commit after that. A commit no
    /// read follows thus costs no admission.
    fn advance_handles(
        &self,
        inode: &InodeRecord,
        committed_seq: Option<u64>,
        dynamic: bool,
    ) -> Result<()> {
        let handles = self.handles.for_inode(&inode.inode_id)?;
        if handles.is_empty() {
            return Ok(());
        }
        let target = Arc::new(inode.clone());
        let deferred = self.local_writers_open(&inode.inode_id)?;
        let mut needs_reference = false;
        for handle in &handles {
            let state = handle
                .state
                .lock()
                .map_err(|_| unavailable("DFS handle state is poisoned"))?;
            needs_reference |= state.opened_inode.is_pinned();
        }
        let protected = if needs_reference && !deferred {
            match self.protected_view(&target) {
                Ok(view) => Some(view),
                Err(error) => {
                    afs_logging::warn!("dfs.handle_view_reference_failed";
                        "inode" => inode.inode_id.0.as_str(),
                        "version" => inode.head_version.as_ref().map_or("", |version| version.0.as_str()),
                        "handles" => handles.len(),
                        "error" => error.to_string());
                    None
                }
            }
        } else {
            None
        };
        let unprotected = Arc::new(OpenedView::new(inode.clone(), None));
        let retry_after = if deferred {
            Instant::now()
        } else {
            Instant::now() + PENDING_VIEW_RETRY_INTERVAL
        };
        for handle in handles {
            if dynamic {
                handle.dynamic_read_route.store(true, Ordering::Release);
            }
            let mut state = handle
                .state
                .lock()
                .map_err(|_| unavailable("DFS handle state is poisoned"))?;
            if let Some(seq) = committed_seq
                && let Some(session) = state.write_session.as_mut()
            {
                session.local.last_synced_seq = session.local.last_synced_seq.max(seq);
            }
            let view = if !state.opened_inode.is_pinned() {
                unprotected.clone()
            } else if let Some(view) = &protected {
                view.clone()
            } else {
                let newer = target.revision >= state.opened_inode.revision
                    && state
                        .pending_view
                        .as_ref()
                        .is_none_or(|pending| pending.inode.revision <= target.revision);
                if newer {
                    state.pending_view = Some(PendingView {
                        inode: target.clone(),
                        retry_after,
                    });
                }
                continue;
            };
            let replaced = Self::publish_view(&mut state, view)?;
            drop(state);
            drop(replaced);
        }
        Ok(())
    }

    fn local_writers_open(&self, inode_id: &InodeId) -> Result<bool> {
        let Some(state) = self.write_state(inode_id)? else {
            return Ok(false);
        };
        let state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        Ok(state.open_writers > 0)
    }

    /// After the last local writer of the inode closed: publish the views its
    /// commits left pending, with one admission for the newest local version
    /// (shared with the write state's base reference when a read took one). If
    /// that version is gone already the admission fails and the handles keep
    /// their own, protected versions.
    pub(super) fn publish_deferred_views(&self, inode_id: &InodeId) -> Result<()> {
        if self.local_writers_open(inode_id)? {
            return Ok(());
        }
        let mut pending = false;
        for handle in self.handles.for_inode(inode_id)? {
            pending |= handle
                .state
                .lock()
                .map_err(|_| unavailable("DFS handle state is poisoned"))?
                .pending_view
                .is_some();
        }
        if !pending {
            return Ok(());
        }
        let Some(state) = self.write_state(inode_id)? else {
            return Ok(());
        };
        let inode = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?
            .inode
            .clone();
        self.advance_handles(&inode, None, false)
    }

    /// A view of `inode` protected by this Node's GC reference for its version.
    fn protected_view(&self, inode: &InodeRecord) -> Result<Arc<OpenedView>> {
        let pin = match &self.gc.references {
            Some(references) => Some(references.pin(&inode.inode_id, inode.head_version.as_ref())?),
            None => None,
        };
        Ok(Arc::new(OpenedView::new(inode.clone(), pin)))
    }

    /// Install `view` as the handle's read view; returns the replaced view so
    /// the caller drops it (and maybe its reference) outside the handle lock.
    /// A handle never moves back to an older inode revision: two publishers
    /// racing on one inode leave the newer view in place.
    fn publish_view(
        state: &mut HandleMut,
        view: Arc<OpenedView>,
    ) -> Result<Option<Arc<OpenedView>>> {
        if view.revision < state.opened_inode.revision {
            return Ok(None);
        }
        state.native_read_generation = state
            .native_read_generation
            .checked_add(1)
            .ok_or_else(|| unavailable("DFS native read generation exhausted"))?;
        state.native_read_view = None;
        state.opened_page_identity = None;
        if state
            .pending_view
            .as_ref()
            .is_some_and(|pending| pending.inode.revision <= view.revision)
        {
            state.pending_view = None;
        }
        Ok(Some(std::mem::replace(&mut state.opened_inode, view)))
    }

    /// The inode record of `version` when it is still the head: the only record
    /// that describes it. `None` when another commit replaced it since.
    pub(super) fn proxied_view(
        &self,
        inode_id: &InodeId,
        version: &FileVersionId,
    ) -> Result<Option<InodeRecord>> {
        let inode = self.validate_inode(self.meta.get_inode(inode_id)?)?;
        Ok((inode.head_version.as_ref() == Some(version)).then_some(inode))
    }

    /// Whether the handle has a local view a read should try to publish now:
    /// one waiting for its GC reference, with no local writer open (reads go
    /// through the write state while one is).
    pub(super) fn handle_view_pending(&self, handle: FileHandle) -> Result<bool> {
        let Some(opened) = self.handles.get(handle.0)? else {
            return Ok(false);
        };
        let pending = {
            let state = opened
                .state
                .lock()
                .map_err(|_| unavailable("DFS handle state is poisoned"))?;
            state.pending_view.is_some() || state.refresh_view.is_some()
        };
        Ok(pending && !self.local_writers_open(&opened.inode_id)?)
    }

    /// Try again to protect and publish the handle's pending view (R94). A
    /// failure leaves the handle on its current, protected view; it is not an
    /// error of the read that triggered the retry.
    pub(super) fn retry_pending_view(&self, handle: FileHandle) -> Result<()> {
        let Some(opened) = self.handles.get(handle.0)? else {
            return Ok(());
        };
        let refresh = opened
            .state
            .lock()
            .map_err(|_| unavailable("DFS handle state is poisoned"))?
            .refresh_view
            .clone();
        if let Some(commit) = refresh
            && !self.local_writers_open(&opened.inode_id)?
        {
            match self.proxied_view(&opened.inode_id, &commit.version) {
                Ok(found) => {
                    // Settled either way: moved to the proxied version, or
                    // that version is no longer the head and the handle keeps
                    // its current view (never another Node's later commit).
                    if let Some(inode) = found {
                        self.advance_to_local_view(&inode)?;
                    }
                    for handle in self.handles.for_inode(&opened.inode_id)? {
                        let mut state = handle
                            .state
                            .lock()
                            .map_err(|_| unavailable("DFS handle state is poisoned"))?;
                        if state.refresh_view.as_ref() == Some(&commit) {
                            state.refresh_view = None;
                        }
                    }
                }
                Err(error) => {
                    afs_logging::debug!("dfs.handle_view_refresh_retry_failed";
                        "inode" => opened.inode_id.0.as_str(),
                        "error" => error.to_string());
                    return Ok(());
                }
            }
        }
        let target = {
            let mut state = opened
                .state
                .lock()
                .map_err(|_| unavailable("DFS handle state is poisoned"))?;
            let Some(pending) = state.pending_view.as_ref() else {
                return Ok(());
            };
            if pending.inode.revision < state.opened_inode.revision {
                state.pending_view = None;
                return Ok(());
            }
            if pending.retry_after > Instant::now() {
                return Ok(());
            }
            pending.inode.clone()
        };
        // Reads go through the write state while a local writer is open.
        if self.local_writers_open(&opened.inode_id)? {
            return Ok(());
        }
        let view = match self.protected_view(&target) {
            Ok(view) => view,
            Err(error) => {
                afs_logging::debug!("dfs.handle_view_reference_retry_failed";
                    "inode" => target.inode_id.0.as_str(),
                    "error" => error.to_string());
                let mut state = opened
                    .state
                    .lock()
                    .map_err(|_| unavailable("DFS handle state is poisoned"))?;
                if let Some(pending) = state.pending_view.as_mut()
                    && Arc::ptr_eq(&pending.inode, &target)
                {
                    pending.retry_after = Instant::now() + PENDING_VIEW_RETRY_INTERVAL;
                }
                return Ok(());
            }
        };
        if opened.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut state = opened
            .state
            .lock()
            .map_err(|_| unavailable("DFS handle state is poisoned"))?;
        // A newer publication or pending view replaced this one meanwhile;
        // the reference just built drops with `view`.
        if !state
            .pending_view
            .as_ref()
            .is_some_and(|pending| Arc::ptr_eq(&pending.inode, &target))
        {
            return Ok(());
        }
        let replaced = Self::publish_view(&mut state, view)?;
        drop(state);
        drop(replaced);
        Ok(())
    }
}

/// Outcome of protecting the committed base a write-state read selected.
pub(super) enum BaseReference {
    /// Already protected, nothing committed, or GC references are off.
    NotNeeded,
    Taken(Arc<crate::node::gc::GcPin>),
    Failed(Error),
}

impl DistributedFs {
    /// The base reference reads through this write state already share.
    pub(super) fn cached_base_reference(
        state: &InodeWriteState,
    ) -> Option<Arc<crate::node::gc::GcPin>> {
        let base = state.base_version.as_ref()?;
        state
            .base_read_reference
            .as_ref()
            .filter(|(version, _)| *version == base.id)
            .map(|(_, pin)| pin.clone())
    }

    fn base_reference_needed(&self, view: &ResolvedReadView) -> Option<FileVersionId> {
        if view.base_reference.is_some() || self.gc.references.is_none() {
            return None;
        }
        view.committed.clone()
    }

    /// Synchronous reads take the base reference on the calling thread.
    pub(super) fn base_reference_sync(
        &self,
        inode_id: &InodeId,
        view: &ResolvedReadView,
    ) -> BaseReference {
        let (Some(version), Some(references)) = (
            self.base_reference_needed(view),
            self.gc.references.as_ref(),
        ) else {
            return BaseReference::NotNeeded;
        };
        match references.pin(inode_id, Some(&version)) {
            Ok(pin) => BaseReference::Taken(pin),
            Err(error) => BaseReference::Failed(error),
        }
    }

    /// Asynchronous reads take it on the blocking pool: the admission is a
    /// synchronous Meta call a runtime worker must not block on.
    pub(super) async fn base_reference_async(
        &self,
        inode_id: &InodeId,
        view: &ResolvedReadView,
    ) -> BaseReference {
        let (Some(version), Some(references)) =
            (self.base_reference_needed(view), self.gc.references.clone())
        else {
            return BaseReference::NotNeeded;
        };
        let inode_id = inode_id.clone();
        match crate::node::bridge::spawn_blocking(move || references.pin(&inode_id, Some(&version)))
            .await
        {
            Ok(Ok(pin)) => BaseReference::Taken(pin),
            Ok(Err(error)) => BaseReference::Failed(error),
            Err(_) => BaseReference::Failed(unavailable(
                "DFS base read reference admission was cancelled",
            )),
        }
    }

    /// Apply `base` to `view`. `Ok(true)`: read `view`, its base protected for
    /// the read. `Ok(false)`: the base could not be protected and nothing local
    /// overlays it, so the caller reads the handle's own, protected version
    /// instead (as when a handle's view cannot advance). With local data over
    /// the base, a read without protection could lose that data, so the
    /// admission error is returned.
    pub(super) fn settle_base_reference(
        &self,
        cell: &SharedInodeWriteState,
        view: &mut ResolvedReadView,
        base: BaseReference,
    ) -> Result<bool> {
        match base {
            BaseReference::NotNeeded => Ok(true),
            BaseReference::Taken(pin) => {
                let version = view.committed.clone();
                let replaced = {
                    let mut state = cell
                        .lock()
                        .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                    // Shared only while local write activity can still route
                    // reads here: once the last writer closed nothing would
                    // retire it, and it would keep this version for ever. The
                    // read itself keeps `pin` either way until it ends.
                    match (&version, state.base_version.as_ref()) {
                        (Some(version), Some(base))
                            if base.id == *version
                                && state.has_local_write_activity()
                                && Self::cached_base_reference(&state).is_none() =>
                        {
                            state
                                .base_read_reference
                                .replace((version.clone(), pin.clone()))
                        }
                        _ => None,
                    }
                };
                drop(replaced);
                view.base_reference = Some(pin);
                Ok(true)
            }
            BaseReference::Failed(error) => {
                afs_logging::warn!("dfs.write_state_read_reference_failed";
                    "version" => view.committed.as_ref().map_or("", |version| version.0.as_str()),
                    "error" => error.to_string());
                let overlaid = view
                    .frozen
                    .as_ref()
                    .is_some_and(|frozen| !frozen.is_empty())
                    || !view.active.is_empty();
                if overlaid { Err(error) } else { Ok(false) }
            }
        }
    }

    /// Drop the shared base reference once no local writer and no pending
    /// write work is left: later reads use their handle's own version.
    pub(super) fn retire_base_reference(&self, inode_id: &InodeId) -> Result<()> {
        let Some(cell) = self.write_state(inode_id)? else {
            return Ok(());
        };
        let retired = {
            let mut state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            if state.has_local_write_activity() {
                return Ok(());
            }
            state.base_read_reference.take()
        };
        drop(retired);
        Ok(())
    }
}
