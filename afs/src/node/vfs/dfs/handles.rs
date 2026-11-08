use super::*;

#[derive(Clone)]
pub(super) struct NativeReadView {
    pub(super) generation: u64,
    pub(super) authority: NativeReadAuthoritySnapshot,
    pub(super) deadline: Instant,
}

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
    _gc_pin: Option<Arc<crate::node::gc::GcPin>>,
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
    pub(super) opened_inode: Arc<InodeRecord>,
    pub(super) opened_page_identity: Option<PageCacheIdentity>,
    pub(super) native_read_view: Option<NativeReadView>,
    pub(super) native_read_generation: u64,
    pub(super) native_denial_serial: u64,
    // Accepted/synced sequences and the observed error cursor change after open.
    pub(super) write_session: Option<DfsRemoteWriteSession>,
    pub(super) read_sequence: crate::node::read::object_fetch::ReadSequence,
}

pub(super) type PinnedReadView = Result<(Arc<InodeRecord>, NativeReadView)>;

impl From<DfsFileHandle> for DfsHandle {
    fn from(opened: DfsFileHandle) -> Self {
        Self {
            direct_prefetch: std::sync::OnceLock::new(),
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

    /// Move every open handle on the inode to `inode`, this Node's newest view
    /// of it. Only this Node's own write activity calls this; other nodes'
    /// commits reach a handle only through a new open (decision 4a).
    pub(super) fn advance_to_local_view(&self, inode: &InodeRecord) -> Result<()> {
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
            // An inode-level reference protects every version of the inode,
            // including the one this handle pins until close.
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
    pub(super) opened_inode: Arc<InodeRecord>,
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
        self.release_writer(&session.local)
    }

    /// What a sync barrier on this local handle has reached: this handle's
    /// acknowledged sequence and the inode's committed version.
    pub(super) fn local_sync_receipt(&self, handle: FileHandle) -> Result<SyncReceipt> {
        let snapshot = self.handle_snapshot(handle)?;
        let durable_write_seq = snapshot
            .write_session
            .as_ref()
            .map_or(0, |session| session.local.last_synced_seq);
        let file_version_id = self
            .write_state(&snapshot.inode_id)?
            .and_then(|state| {
                state.lock().ok().and_then(|state| {
                    state
                        .base_version
                        .as_ref()
                        .map(|version| version.id.0.clone())
                })
            })
            .unwrap_or_default();
        Ok(SyncReceipt {
            file_version_id,
            durable_write_seq,
        })
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
