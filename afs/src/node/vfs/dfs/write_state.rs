use super::*;

pub(super) type SharedInodeWriteState = Arc<InodeWriteStateCell>;

pub(super) struct InodeWriteStateCell {
    pub(super) state: Mutex<InodeWriteState>,
    pub(super) cv: Condvar,
    lock_wait: Option<Arc<InodeLockWait>>,
}

/// Structural-cost counters for inode write-state lock contention. Only an
/// acquisition that has to wait is timed; an uncontended one costs a `try_lock`.
pub(super) struct InodeLockWait {
    pub(super) contended: afs_metrics::IntCounter,
    pub(super) wait_nanoseconds: afs_metrics::IntCounter,
}

impl InodeLockWait {
    pub(super) fn register(
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

pub(super) struct ResolvedReadView {
    pub(super) committed: Option<FileVersionId>,
    pub(super) length: u64,
    pub(super) layout: LayoutRoot,
    pub(super) frozen: Option<DirtyExtentMap>,
    pub(super) active: DirtyExtentMap,
}

pub(super) struct InodeWriteState {
    pub(super) inode: InodeRecord,
    pub(super) write_lease: WriteLease,
    pub(super) base_version: Option<FileVersion>,
    pub(super) base_layout: LayoutRoot,
    pub(super) logical_length: u64,
    pub(super) metadata_dirty: bool,
    pub(super) kill_suidgid_dirty: bool,
    pub(super) dirty_extents: DirtyExtentMap,
    pub(super) in_flight: Option<InFlightCommit>,
    pub(super) commit_busy: bool,
    pub(super) operation_busy: bool,
    pub(super) native_queue: MutationTicketQueue,
    pub(super) native_thread: Option<std::thread::ThreadId>,
    pub(super) native_owner: Arc<DirtyOwner>,
    pub(super) native_due: bool,
    pub(super) native_due_through: u64,
    pub(super) dirty: bool,
    pub(super) next_write_seq: u64,
    pub(super) visible_write_seq: u64,
    pub(super) durable_write_seq: u64,
    pub(super) committed_write_seq: u64,
    pub(super) open_writers: u64,
    pub(super) last_writer_background_requested: bool,
    pub(super) background_error: Option<ObservedWriteError>,
    pub(super) terminal_error: Option<Error>,
}

pub(super) struct PrepareCommitError {
    pub(super) error: Error,
}

#[derive(Clone)]
pub(super) struct ObservedWriteError {
    pub(super) cursor: u64,
    pub(super) error: Error,
}

pub(super) struct InodeOperationGuard {
    pub(super) cell: SharedInodeWriteState,
    pub(super) native_notify: Option<Arc<tokio::sync::Notify>>,
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

impl InodeWriteState {
    /// This Node has write activity on the inode: an open local writer,
    /// accepted writes not yet committed, or a commit in flight. While it
    /// lasts every handle on this Node reads the Node's own view (4a).
    pub(super) fn has_local_write_activity(&self) -> bool {
        self.open_writers > 0
            || self.dirty
            || self.in_flight.is_some()
            || self.visible_write_seq > self.committed_write_seq
    }
}

impl InodeWriteStateCell {
    pub(super) fn new(state: InodeWriteState) -> Self {
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

    pub(super) fn lock(&self) -> std::sync::LockResult<std::sync::MutexGuard<'_, InodeWriteState>> {
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

    pub(super) fn wait<'a>(
        &self,
        guard: std::sync::MutexGuard<'a, InodeWriteState>,
    ) -> std::sync::LockResult<std::sync::MutexGuard<'a, InodeWriteState>> {
        self.cv.wait(guard)
    }

    pub(super) fn notify_all(&self) {
        self.cv.notify_all();
    }
}

/// Per-inode write state cells. Each cell carries its own `Mutex`+`Condvar`;
/// the sharded table lock only guards membership and is never held while a
/// cell is locked by this table.
#[derive(Default)]
pub(super) struct InodeWriteTable {
    pub(super) states: ShardedMap<InodeId, SharedInodeWriteState>,
    /// Contention counters attached to every newly installed cell.
    pub(super) lock_wait: Option<Arc<InodeLockWait>>,
    /// Rotation cursor so lease renewal is fair across passes.
    pub(super) lease_cursor: AtomicU64,
}

impl InodeWriteTable {
    pub(super) fn get(&self, inode_id: &InodeId) -> Result<Option<SharedInodeWriteState>> {
        Ok(self
            .states
            .read(inode_id)
            .map_err(|_| unavailable("DFS inode write table is poisoned"))?
            .get(inode_id)
            .cloned())
    }
}

impl DistributedFs {
    pub(super) fn write_state(&self, inode_id: &InodeId) -> Result<Option<SharedInodeWriteState>> {
        self.writes.get(inode_id)
    }

    pub(super) fn write_state_can_adopt_fresh_lease(state: &InodeWriteState) -> bool {
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

    pub(super) fn write_state_can_rebind_fresh_lease_preserving_metadata(
        state: &InodeWriteState,
    ) -> bool {
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

    pub(super) fn write_state_can_refresh_committed_view(state: &InodeWriteState) -> bool {
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

    pub(super) fn remote_provider_visible_for_reads(
        &self,
        inode_id: &InodeId,
    ) -> Result<Option<RemoteDfsProviderIo>> {
        self.write_coordinator.remote_provider_visible_for_reads(
            inode_id,
            self.local_write_state_protects_remote_provider(inode_id)?,
        )
    }

    pub(super) fn write_state_should_reacquire_clean_lease(
        cell: &SharedInodeWriteState,
    ) -> Result<bool> {
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

    pub(super) fn install_write_state(
        &self,
        inode: InodeRecord,
        write_lease: WriteLease,
    ) -> Result<()> {
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
                native_queue: MutationTicketQueue::default(),
                native_thread: None,
                native_owner: DirtyOwner::new(),
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
            .with_lock_wait(self.writes.lock_wait.clone()),
        );
        self.writes
            .states
            .write(&inode.inode_id)
            .map_err(|_| unavailable("DFS inode write table is poisoned"))?
            .entry(inode.inode_id.clone())
            .or_insert(state);
        self.handles.mark_dynamic(&inode.inode_id)?;
        Ok(())
    }

    pub(super) fn ensure_inode_write_state(
        &self,
        inode: &InodeRecord,
    ) -> Result<SharedInodeWriteState> {
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

    pub(super) fn open_write_session(
        &self,
        inode: &InodeRecord,
        open_flags: i32,
    ) -> Result<DfsWriteSession> {
        let state = self.ensure_inode_write_state(inode)?;
        let mut state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        // J4: a fresh lease is part of this call's contract (see fault::lease_keeper).
        ensure_fresh(&mut state.write_lease, |lease| {
            self.meta.renew_write_lease(lease)
        })?;
        state.open_writers = state.open_writers.saturating_add(1);
        state.last_writer_background_requested = false;
        let session_id = DfsWriteSessionId::new(format!(
            "{}-write-session-{}",
            self.identity.session_id,
            self.identity.next_operation.fetch_add(1, Ordering::Relaxed)
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

    pub(super) fn visible_attributes(&self, inode: &InodeRecord) -> Result<FileAttributes> {
        self.visible_attributes_with_observed_inode(inode, inode)
    }

    pub(super) fn visible_attributes_with_observed_inode(
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

    pub(super) fn clear_kernel_write_privileges(mode: u32) -> u32 {
        let mut cleared = mode & !0o4000;
        if mode & 0o010 != 0 {
            cleared &= !0o2000;
        }
        cleared
    }

    pub(super) fn apply_kernel_killpriv_to_state(state: &mut InodeWriteState) {
        let cleared = Self::clear_kernel_write_privileges(state.inode.attributes.mode);
        if cleared != state.inode.attributes.mode {
            state.inode.attributes.mode = cleared;
            state.inode.attributes.ctime_unix_ms = now_unix_ms();
            state.metadata_dirty = true;
            state.kill_suidgid_dirty = true;
        }
    }

    pub(super) fn has_pending_killpriv(&self, inode_id: &InodeId) -> Result<bool> {
        let Some(state) = self.write_state(inode_id)? else {
            return Ok(false);
        };
        let state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        Ok(state.kill_suidgid_dirty)
    }

    pub(super) fn observe_inode_record(&self, inode: InodeRecord) -> Result<InodeRecord> {
        let inode = self.validate_inode(inode)?;
        // Updates only the base new opens and writers start from; open
        // handles keep their pinned version (decision 4a).
        self.refresh_retained_committed_view(&inode)?;
        Ok(inode)
    }

    pub(super) fn refresh_inode_record(
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
