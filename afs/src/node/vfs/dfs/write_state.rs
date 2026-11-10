use super::*;

pub(super) type SharedInodeWriteState = Arc<InodeWriteStateCell>;

#[cfg(test)]
std::thread_local! {
    /// Inode write-state lock acquisitions on this thread (test probe).
    pub(super) static DFS_WRITE_STATE_LOCKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(super) struct InodeWriteStateCell {
    state: Mutex<InodeWriteState>,
    pub(super) cv: Condvar,
    lock_wait: Option<Arc<InodeLockWait>>,
    /// Set once, when the cell joins an `InodeWriteTable`.
    pending: std::sync::OnceLock<PendingWriteEntry>,
    /// Whether `pending` currently lists this inode. Changed only while
    /// `state` is held, so listing and unlisting follow the inode's order.
    listed: AtomicBool,
}

/// Inodes that may have pending write work, the only ones a Node-wide
/// writeback pass has to examine. Closed and published write states stay in
/// `InodeWriteTable::states` for reopen; they are not listed here.
pub(super) type PendingWriteIndex = ShardedMap<InodeId, ()>;

struct PendingWriteEntry {
    inode_id: InodeId,
    index: Arc<PendingWriteIndex>,
}

/// A held inode write-state lock. Releasing it (drop or a condvar wait) lists
/// the inode in the pending-write index when the state has pending write work,
/// so every transition out of idle is visible to the next writeback pass.
pub(super) struct InodeWriteStateGuard<'a> {
    cell: &'a InodeWriteStateCell,
    guard: Option<std::sync::MutexGuard<'a, InodeWriteState>>,
}

impl<'a> InodeWriteStateGuard<'a> {
    fn new(
        cell: &'a InodeWriteStateCell,
        guard: std::sync::MutexGuard<'a, InodeWriteState>,
    ) -> Self {
        Self {
            cell,
            guard: Some(guard),
        }
    }

    fn wrap(
        cell: &'a InodeWriteStateCell,
        result: std::sync::LockResult<std::sync::MutexGuard<'a, InodeWriteState>>,
    ) -> std::sync::LockResult<Self> {
        match result {
            Ok(guard) => Ok(Self::new(cell, guard)),
            Err(poisoned) => Err(std::sync::PoisonError::new(Self::new(
                cell,
                poisoned.into_inner(),
            ))),
        }
    }

    /// Publish pending work, then hand the raw guard to a condvar wait.
    fn release_for_wait(mut self) -> std::sync::MutexGuard<'a, InodeWriteState> {
        let guard = self
            .guard
            .take()
            .expect("inode write-state guard is held until released");
        self.cell.publish_pending(&guard);
        guard
    }
}

impl std::ops::Deref for InodeWriteStateGuard<'_> {
    type Target = InodeWriteState;
    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("inode write-state guard is held until released")
    }
}

impl std::ops::DerefMut for InodeWriteStateGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard
            .as_mut()
            .expect("inode write-state guard is held until released")
    }
}

impl Drop for InodeWriteStateGuard<'_> {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            self.cell.publish_pending(&guard);
        }
    }
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
    /// The GC reference that keeps `committed` while this read uses it (R94).
    pub(super) base_reference: Option<Arc<crate::node::gc::GcPin>>,
    pub(super) length: u64,
    pub(super) layout: LayoutRoot,
    pub(super) frozen: Option<DirtyExtentMap>,
    pub(super) active: DirtyExtentMap,
}

/// A file version that durably incorporated a proxied handle's writes.
#[derive(Clone, Eq, PartialEq)]
pub(super) struct ProxiedCommit {
    pub(super) version: FileVersionId,
    pub(super) revision: u64,
}

#[derive(Default)]
pub(super) struct ProxiedWriteProgress {
    pending: bool,
    frozen: bool,
    committed: Option<ProxiedCommit>,
}

pub(super) struct InodeWriteState {
    pub(super) inode: InodeRecord,
    pub(super) write_lease: WriteLease,
    pub(super) base_version: Option<FileVersion>,
    pub(super) base_layout: LayoutRoot,
    /// The GC reference of `base_version` that reads through this state share,
    /// taken by the first such read (R94). A write lease, or a writer still
    /// open, does not keep the base: the lease can lapse while a writer is open
    /// and this Node's next commit retires the base as head.
    pub(super) base_read_reference: Option<(FileVersionId, Arc<crate::node::gc::GcPin>)>,
    /// Only open proxy writers that have changed file data have an entry.
    /// Pending mutations and the frozen commit are recorded under this lock.
    pub(super) proxied_writes: HashMap<DfsWriteSessionId, ProxiedWriteProgress>,
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
    /// Some caller has received this error. A new writer's session starts past
    /// a delivered error and before an undelivered one (Linux errseq).
    pub(super) delivered: bool,
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
    pub(super) fn note_proxied_write(&mut self, writer: &DfsWriteSessionId) {
        self.proxied_writes
            .entry(writer.clone())
            .or_default()
            .pending = true;
    }

    pub(super) fn freeze_proxied_writes(&mut self) {
        for write in self.proxied_writes.values_mut() {
            write.frozen = std::mem::take(&mut write.pending);
        }
    }

    pub(super) fn restore_proxied_writes(&mut self) {
        for write in self.proxied_writes.values_mut() {
            write.pending |= std::mem::take(&mut write.frozen);
        }
    }

    pub(super) fn commit_proxied_writes(&mut self, version: &FileVersionId, revision: u64) {
        for write in self.proxied_writes.values_mut() {
            if std::mem::take(&mut write.frozen) {
                write.committed = Some(ProxiedCommit {
                    version: version.clone(),
                    revision,
                });
            }
        }
    }

    pub(super) fn proxied_commit(&self, writer: &DfsWriteSessionId) -> Option<ProxiedCommit> {
        self.proxied_writes.get(writer)?.committed.clone()
    }

    /// Anything a Node-wide writeback pass (progress, timer, upload start,
    /// lease renewal, resource accounting) may act on or count. A state
    /// without any of these is closed and published: no pass touches it.
    pub(super) fn has_pending_write_work(&self) -> bool {
        self.open_writers > 0
            || self.dirty
            || self.metadata_dirty
            || self.kill_suidgid_dirty
            || !self.dirty_extents.is_empty()
            || !self.dirty_extents.builders.is_empty()
            || self.dirty_extents.native_operation_id.is_some()
            || self.in_flight.is_some()
            || self.commit_busy
            || self.operation_busy
            || self.native_due
            || self.last_writer_background_requested
            || !self.native_queue.values.is_empty()
            || self.native_thread.is_some()
            || self.visible_write_seq > self.committed_write_seq
    }

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
            pending: std::sync::OnceLock::new(),
            listed: AtomicBool::new(false),
        }
    }

    fn with_lock_wait(mut self, lock_wait: Option<Arc<InodeLockWait>>) -> Self {
        self.lock_wait = lock_wait;
        self
    }

    pub(super) fn lock(&self) -> std::sync::LockResult<InodeWriteStateGuard<'_>> {
        #[cfg(test)]
        DFS_WRITE_STATE_LOCKS.with(|count| count.set(count.get() + 1));
        let Some(lock_wait) = &self.lock_wait else {
            return InodeWriteStateGuard::wrap(self, self.state.lock());
        };
        match self.state.try_lock() {
            Ok(guard) => Ok(InodeWriteStateGuard::new(self, guard)),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                InodeWriteStateGuard::wrap(self, Err(poisoned))
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                let started = Instant::now();
                let result = self.state.lock();
                lock_wait.record(started.elapsed());
                InodeWriteStateGuard::wrap(self, result)
            }
        }
    }

    pub(super) fn try_lock(&self) -> std::sync::TryLockResult<InodeWriteStateGuard<'_>> {
        #[cfg(test)]
        DFS_WRITE_STATE_LOCKS.with(|count| count.set(count.get() + 1));
        match self.state.try_lock() {
            Ok(guard) => Ok(InodeWriteStateGuard::new(self, guard)),
            Err(std::sync::TryLockError::WouldBlock) => Err(std::sync::TryLockError::WouldBlock),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => Err(
                std::sync::TryLockError::Poisoned(std::sync::PoisonError::new(
                    InodeWriteStateGuard::new(self, poisoned.into_inner()),
                )),
            ),
        }
    }

    pub(super) fn wait<'a>(
        &self,
        guard: InodeWriteStateGuard<'a>,
    ) -> std::sync::LockResult<InodeWriteStateGuard<'a>> {
        let cell = guard.cell;
        InodeWriteStateGuard::wrap(cell, self.cv.wait(guard.release_for_wait()))
    }

    pub(super) fn wait_timeout<'a>(
        &self,
        guard: InodeWriteStateGuard<'a>,
        timeout: Duration,
    ) -> std::sync::LockResult<InodeWriteStateGuard<'a>> {
        let cell = guard.cell;
        let result = match self.cv.wait_timeout(guard.release_for_wait(), timeout) {
            Ok((guard, _)) => Ok(guard),
            Err(poisoned) => Err(std::sync::PoisonError::new(poisoned.into_inner().0)),
        };
        InodeWriteStateGuard::wrap(cell, result)
    }

    /// List this inode for writeback passes once its state has pending write
    /// work. Called with the state lock held.
    fn publish_pending(&self, state: &InodeWriteState) {
        let Some(entry) = self.pending.get() else {
            return;
        };
        if self.listed.load(Ordering::Relaxed) || !state.has_pending_write_work() {
            return;
        }
        // A poisoned shard leaves the inode unlisted; that shard's other
        // operations already fail, and a later release retries the listing.
        if let Ok(mut shard) = entry.index.write(&entry.inode_id) {
            shard.insert(entry.inode_id.clone(), ());
            self.listed.store(true, Ordering::Relaxed);
        }
    }

    /// Drop this inode from the pending-write index if its state has no
    /// pending write work. A busy state is kept listed and checked next pass.
    fn unlist_if_idle(&self) -> Result<bool> {
        let Some(entry) = self.pending.get() else {
            return Ok(false);
        };
        let state = match self.state.try_lock() {
            Ok(state) => state,
            // Busy or poisoned: keep it listed; the pass reports poison itself.
            Err(_) => return Ok(false),
        };
        if state.has_pending_write_work() {
            return Ok(false);
        }
        entry
            .index
            .write(&entry.inode_id)
            .map_err(|_| unavailable("DFS pending write index is poisoned"))?
            .remove(&entry.inode_id);
        self.listed.store(false, Ordering::Relaxed);
        Ok(true)
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
    /// The subset of `states` that may have pending write work.
    pub(super) pending: Arc<PendingWriteIndex>,
    /// Contention counters attached to every newly installed cell.
    pub(super) lock_wait: Option<Arc<InodeLockWait>>,
    /// Rotation cursor so lease renewal is fair across passes.
    pub(super) lease_cursor: AtomicU64,
}

impl InodeWriteTable {
    /// Install `cell` for `inode_id` unless a cell is already there; the
    /// installed cell is listed for writeback passes whenever it has pending
    /// write work.
    pub(super) fn insert_cell(&self, inode_id: InodeId, cell: SharedInodeWriteState) -> Result<()> {
        let _ = cell.pending.set(PendingWriteEntry {
            inode_id: inode_id.clone(),
            index: self.pending.clone(),
        });
        let installed = self
            .states
            .write(&inode_id)
            .map_err(|_| unavailable("DFS inode write table is poisoned"))?
            .entry(inode_id)
            .or_insert(cell)
            .clone();
        // A cell built with pending work is listed now, not at its next release.
        let state = installed
            .state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        installed.publish_pending(&state);
        Ok(())
    }

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
    /// Write states a Node-wide writeback pass has to examine: those listed
    /// with pending write work. Listed states found idle are unlisted here, so
    /// a pass costs the inodes with work, not every inode this Node retains.
    pub(super) fn pending_write_states(&self) -> Result<Vec<(InodeId, SharedInodeWriteState)>> {
        let mut pending = Vec::new();
        for inode_id in self.writes.pending.keys_snapshot()? {
            let Some(cell) = self.write_state(&inode_id)? else {
                continue;
            };
            if !cell.unlist_if_idle()? {
                pending.push((inode_id, cell));
            }
        }
        Ok(pending)
    }

    pub(super) fn write_state(&self, inode_id: &InodeId) -> Result<Option<SharedInodeWriteState>> {
        self.writes.get(inode_id)
    }

    // The three predicates below deliberately ignore `background_error`: they
    // all require no open writer and no pending work, so an undelivered error
    // has no receiver yet and must not stop the next writer that will get it.
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
        Ok(Self::rejected_and_idle(&state)
            || (Self::write_state_can_adopt_fresh_lease(&state)
                || Self::write_state_can_rebind_fresh_lease_preserving_metadata(&state))
                && should_renew(&state.write_lease))
    }

    /// Meta definitively rejected this state's writes and every writer has left:
    /// they can never commit, and the failure was already returned or is retained
    /// for the next writer. A state in this condition is discarded at the next
    /// open for write (as Linux drops dirty pages after a reported writeback
    /// failure); until then readers keep the Node's view.
    fn rejected_and_idle(state: &InodeWriteState) -> bool {
        state.terminal_error.is_some()
            && state.open_writers == 0
            && !state.commit_busy
            && !state.operation_busy
            && state.in_flight.is_none()
            && state.native_queue.values.is_empty()
            && state.native_thread.is_none()
    }

    fn write_state_is_rejected_and_idle(cell: &SharedInodeWriteState) -> Result<bool> {
        let state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        Ok(Self::rejected_and_idle(&state))
    }

    fn discard_rejected_writes(state: &mut InodeWriteState) {
        state.terminal_error = None;
        // Dropping the map releases its builders, uploads, budget and GC pin.
        state.dirty_extents = DirtyExtentMap::default();
        state.dirty = false;
        state.metadata_dirty = false;
        state.kill_suidgid_dirty = false;
        state.native_due = false;
        state.native_due_through = 0;
        state.last_writer_background_requested = false;
        state.visible_write_seq = state.next_write_seq;
        state.durable_write_seq = state.next_write_seq;
        state.committed_write_seq = state.next_write_seq;
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
            if Self::write_state_is_rejected_and_idle(&existing)? {
                let (base_version, base_layout) = self.load_version(inode.head_version.as_ref())?;
                let logical_length = base_version.as_ref().map_or(0, |version| version.length);
                let mut state = existing
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                if Self::rejected_and_idle(&state) {
                    Self::discard_rejected_writes(&mut state);
                    Self::apply_clean_fresh_write_state(
                        &mut state,
                        inode,
                        write_lease,
                        base_version,
                        base_layout,
                        logical_length,
                    );
                    existing.notify_all();
                    return Ok(());
                }
            }
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
                base_read_reference: None,
                proxied_writes: HashMap::new(),
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
        self.writes.insert_cell(inode.inode_id.clone(), state)?;
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
        self.advance_to_local_view(&inode)?;
        Ok(session)
    }

    /// This node's write state for a privilege clear, or `None` when the write
    /// lease belongs to another node.
    pub(super) fn local_write_state_for_killpriv(
        &self,
        inode: &InodeRecord,
    ) -> Result<Option<SharedInodeWriteState>> {
        if self.write_state(&inode.inode_id)?.is_some() {
            return self.ensure_inode_write_state(inode).map(Some);
        }
        let (fresh_inode, write_lease) = self.meta.open_write(&inode.inode_id)?;
        if self.ensure_local_write_owner(&write_lease).is_err() {
            return Ok(None);
        }
        self.install_write_state(self.validate_inode(fresh_inode)?, write_lease)?;
        self.write_state(&inode.inode_id)
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
            .filter(|error| error.delivered)
            .map_or(0, |error| error.cursor),
    }
}
