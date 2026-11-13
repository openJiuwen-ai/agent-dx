use super::*;

#[derive(Clone)]
pub(super) struct FrozenCommit {
    pub(super) through_seq: u64,
    pub(super) logical_length: u64,
    pub(super) inode: InodeRecord,
    pub(super) write_lease: WriteLease,
    pub(super) base_version: Option<FileVersion>,
    pub(super) base_layout: LayoutRoot,
    pub(super) dirty_extents: DirtyExtentMap,
    pub(super) kill_suidgid_dirty: bool,
}

pub(super) enum InFlightCommit {
    Preparing(Box<FrozenCommit>),
    File(Box<PendingFileCommit>),
    Metadata(Box<SyncInodeMetadata>, u64),
}

#[derive(Clone)]
pub(super) struct PendingFileCommit {
    pub(super) frozen: FrozenCommit,
    pub(super) batch: CommitBatch,
}

pub(super) struct CommitPlan {
    pub(super) layout_root: LayoutRoot,
    pub(super) staged_chunks: Vec<StagedChunk>,
}

#[derive(Clone)]
pub(super) struct OverlaySegment {
    pub(super) file_offset: u64,
    pub(super) length: u64,
    pub(super) data: Option<(SharedChunkBytes, usize)>,
}

#[derive(Clone)]
pub(super) struct CommitBatch {
    pub(super) _gc_pin: Option<Arc<crate::node::gc::GcPin>>,
    pub(super) through_seq: u64,
    pub(super) commit: Arc<CommitFileVersion>,
    pub(super) _pipeline_accounting: Option<Arc<CommitAccounting>>,
    pub(super) _pipeline_uploads: Vec<Arc<PipelineUpload>>,
}

enum CommitAction {
    Prepare(Box<FrozenCommit>),
    Send(InFlightCommit),
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
pub(super) enum CommitReason {
    Foreground(ForegroundCommit),
    Background(BackgroundCommit),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ForegroundCommit {
    /// fdatasync, and data barriers inside other operations (resize, setattr).
    DataSync,
    /// fsync: data plus a full metadata barrier.
    FullSync,
    /// FUSE flush on close(2) and SDK close; data only (decision 5).
    Close,
    /// Node shutdown.
    NodeDrain,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BackgroundCommit {
    Periodic,
}

/// How a commit step orders itself against operations already admitted on the
/// inode.
#[derive(Clone, Copy, Debug)]
pub(super) enum CommitWait {
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

impl InFlightCommit {
    pub(super) fn dirty_extents(&self) -> Option<DirtyExtentMap> {
        match self {
            InFlightCommit::Preparing(frozen) => Some(frozen.dirty_extents.clone()),
            InFlightCommit::File(pending) => Some(pending.frozen.dirty_extents.clone()),
            InFlightCommit::Metadata(_, _) => None,
        }
    }
}

/// Metadata-only syncs one inode rebuilds in a row after Meta refused them on
/// a changed inode (B5) before the refusal becomes a terminal error.
const METADATA_SYNC_REBUILD_LIMIT: u32 = 4;

impl DistributedFs {
    /// Barrier: commit until every write this handle's inode had accepted when
    /// the call started is committed (and, for fsync, its metadata synced).
    /// Built from repeated `commit_inode` steps.
    pub(super) fn sync_handle(
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
            self.mutation_ticket_with_cleanup(&state, true)?
        } else {
            None
        };
        let _operation = if wait_for_operation && self.write_resources.pipeline.is_none() {
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
            // A failed step may be one the inode already retained (an upload
            // failure): report it from there, so this handle sees it only once.
            let committed_seq = self
                .commit_inode(&snapshot.inode_id, reason, CommitWait::InsideOperation)
                .map_err(|error| self.observe_handle_error(handle).err().unwrap_or(error))?;
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
    pub(super) fn commit_inode(
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

        loop {
            let Some(action) = self.acquire_commit_action(&cell, reason, wait_for_operation)?
            else {
                return Ok(None);
            };
            let (pending, branch) = match action {
                CommitAction::Send(pending) => (pending, "pending_reuse"),
                CommitAction::Prepare(frozen) => (
                    self.prepare_in_flight_commit(&cell, frozen, reason, wait_for_operation)?,
                    "new_prepare",
                ),
            };
            commit_span.record("branch", branch);
            let committed = self.finish_commit(cell.clone(), pending, branch)?;
            if reason.full_metadata_barrier() && self.needs_full_metadata_sync(&cell)? {
                continue;
            }
            return Ok(committed);
        }
    }

    /// Wait for this inode's turn and claim `commit_busy` for what it needs
    /// next. `None`: nothing to commit, or a background commit found it busy.
    fn acquire_commit_action(
        &self,
        cell: &SharedInodeWriteState,
        reason: CommitReason,
        wait_for_operation: bool,
    ) -> Result<Option<CommitAction>> {
        let maintenance = reason.is_background();
        let mut state = if maintenance {
            match cell.try_lock() {
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
            let wait = crate::node::call_deadline::remaining();
            if wait.is_some_and(|time| time.is_zero()) {
                return Err(Error::coded(
                    afs_error::IO_TIMEOUT,
                    "DFS commit turn deadline elapsed",
                ));
            }
            state = match wait {
                Some(wait) => cell.wait_timeout(state, wait),
                None => cell.wait(state),
            }
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        }
        if let Some(error) = state.terminal_error.clone() {
            return Err(error);
        }
        if state.in_flight.is_some() {
            return Self::resume_in_flight_commit(&mut state).map(Some);
        }
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
        // J4: a fresh lease is part of this call's contract (see fault::lease_keeper).
        ensure_fresh(&mut state.write_lease, |lease| {
            self.meta.renew_write_lease(lease)
        })?;
        if !state.dirty {
            let sync = Box::new(self.full_metadata_sync(&state));
            let through_seq = state.visible_write_seq;
            let pending = InFlightCommit::Metadata(sync.clone(), through_seq);
            state.in_flight = Some(InFlightCommit::Metadata(sync, through_seq));
            state.commit_busy = true;
            Ok(Some(CommitAction::Send(pending)))
        } else {
            let frozen = Self::freeze_state_for_commit(&mut state);
            state.freeze_proxied_writes();
            state.in_flight = Some(InFlightCommit::Preparing(Box::new(frozen.clone())));
            state.commit_busy = true;
            state.dirty = false;
            Ok(Some(CommitAction::Prepare(Box::new(frozen))))
        }
    }

    /// Continue the commit a previous attempt left in flight. The caller has
    /// checked that there is one.
    fn resume_in_flight_commit(state: &mut InodeWriteState) -> Result<CommitAction> {
        let pending = match state.in_flight.as_ref() {
            Some(InFlightCommit::Preparing(_)) => {
                let taken = state.in_flight.take().ok_or_else(|| {
                    unavailable("DFS preparing commit disappeared before ownership transfer")
                })?;
                let InFlightCommit::Preparing(frozen) = taken else {
                    return Err(unavailable(
                        "DFS in-flight commit changed before preparing ownership transfer",
                    ));
                };
                state.in_flight = Some(InFlightCommit::Preparing(frozen.clone()));
                state.commit_busy = true;
                return Ok(CommitAction::Prepare(frozen));
            }
            Some(InFlightCommit::File(pending)) => InFlightCommit::File(pending.clone()),
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
        Ok(CommitAction::Send(pending))
    }

    fn full_metadata_sync(&self, state: &InodeWriteState) -> SyncInodeMetadata {
        SyncInodeMetadata {
            operation_id: self.identity.operation_id("fsync-metadata"),
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
        }
    }

    /// Prepare the frozen generation and record it as the in-flight file
    /// commit, or roll the freeze back and release `commit_busy` on failure.
    fn prepare_in_flight_commit(
        &self,
        cell: &SharedInodeWriteState,
        mut frozen: Box<FrozenCommit>,
        reason: CommitReason,
        wait_for_operation: bool,
    ) -> Result<InFlightCommit> {
        match self.prepare_commit(&mut frozen, reason) {
            Ok(batch) => {
                let pending_file = PendingFileCommit {
                    frozen: *frozen,
                    batch,
                };
                let pending = InFlightCommit::File(Box::new(pending_file.clone()));
                let mut state = cell
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                while self.write_resources.pipeline.is_some()
                    && wait_for_operation
                    && state.operation_busy
                {
                    let remaining = crate::node::call_deadline::remaining();
                    if remaining.is_some_and(|time| time.is_zero()) {
                        state.in_flight = Some(InFlightCommit::File(Box::new(pending_file)));
                        state.commit_busy = false;
                        cell.notify_all();
                        return Err(Error::coded(
                            afs_error::IO_TIMEOUT,
                            "DFS prepared commit deadline elapsed before send",
                        ));
                    }
                    state = match remaining {
                        Some(time) => cell.wait_timeout(state, time),
                        None => cell.wait(state),
                    }
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                }
                state.in_flight = Some(InFlightCommit::File(Box::new(pending_file)));
                #[cfg(test)]
                self.pause_after_writeback_prepare();
                Ok(pending)
            }
            Err(prepare_error) => {
                let mut state = cell
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                if crate::node::gc::is_permanent_admission_failure(&prepare_error.error) {
                    // Meta will never admit this generation's reference: its
                    // writes can never publish. Report them like any definite
                    // rejection; the state is discarded (releasing the pin) at
                    // the next open for write.
                    state.terminal_error = Some(prepare_error.error.clone());
                }
                let failed = *frozen;
                if state.dirty_extents.is_empty() {
                    state.dirty_extents.restore_before(failed.dirty_extents);
                    state.restore_proxied_writes();
                    state.in_flight = None;
                } else {
                    state.in_flight = Some(InFlightCommit::Preparing(Box::new(failed)));
                }
                state.dirty = !state.dirty_extents.is_empty();
                state.commit_busy = false;
                cell.notify_all();
                Err(prepare_error.error)
            }
        }
    }

    pub(super) fn finish_commit(
        &self,
        cell: SharedInodeWriteState,
        pending: InFlightCommit,
        branch: &str,
    ) -> Result<Option<u64>> {
        let committed_kill_suidgid = matches!(&pending, InFlightCommit::File(pending) if pending.frozen.kill_suidgid_dirty)
            || matches!(&pending, InFlightCommit::Metadata(sync, _) if sync.metadata_delta.kill_suidgid);
        // Admission expiry is safe before sending. Once sent, keep the
        // publication RPC's usual result and exact-replay semantics.
        let result = if crate::node::call_deadline::remaining().is_some_and(|time| time.is_zero()) {
            Err(Error::coded(
                afs_error::IO_TIMEOUT,
                "DFS publication deadline elapsed before send",
            ))
        } else {
            crate::node::call_deadline::without(|| match &pending {
                InFlightCommit::Preparing(_) => {
                    Err(unavailable("DFS inode commit is still preparing"))
                }
                InFlightCommit::File(pending) => self.send_file_commit(pending, branch),
                InFlightCommit::Metadata(sync, through_seq) => {
                    self.send_metadata_sync(sync, *through_seq, branch)
                }
            })
        };

        // B5: Meta refuses a metadata-only sync whose inode changed after it
        // was frozen. Read the inode again (outside the state lock) and let
        // `settle_metadata_sync_conflict` drop or rebuild the sync.
        let conflict_current = match (&result, &pending) {
            (Err(error), InFlightCommit::Metadata(sync, _))
                if error.code() == afs_error::META_DFS_CONFLICT =>
            {
                Some(self.validate_inode(self.meta.get_inode(&sync.inode_id)?)?)
            }
            _ => None,
        };
        let mut state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        state.commit_busy = false;
        if let (Some(current), InFlightCommit::Metadata(sync, through_seq)) =
            (conflict_current, &pending)
            && state.metadata_sync_rebuilds < METADATA_SYNC_REBUILD_LIMIT
        {
            Self::settle_metadata_sync_conflict(&mut state, sync, *through_seq, current);
            cell.notify_all();
            return Ok(None);
        }
        let outcome = match result {
            Ok(AppliedCommit::File {
                through_seq,
                committed_full_metadata,
                version,
                layout,
                updated,
            }) => {
                Self::apply_file_commit_locked(
                    &mut state,
                    through_seq,
                    committed_full_metadata,
                    committed_kill_suidgid,
                    version,
                    layout,
                    updated,
                );
                Ok(Some(through_seq))
            }
            Ok(AppliedCommit::Metadata {
                through_seq,
                updated,
            }) => {
                state.in_flight = None;
                state.meta_mtime_unix_ms = updated.attributes.mtime_unix_ms;
                state.metadata_sync_rebuilds = 0;
                state.inode = updated;
                state.metadata_dirty = false;
                state.kill_suidgid_dirty = false;
                state.last_writer_background_requested = false;
                state.durable_write_seq = state.durable_write_seq.max(through_seq);
                state.committed_write_seq = state.committed_write_seq.max(through_seq);
                Ok(Some(state.committed_write_seq))
            }
            Err(error) => {
                let definite = is_definite_commit_rejection(&error);
                Self::record_commit_error_locked(&mut state, &error, definite);
                if definite && let InFlightCommit::File(pending) = pending {
                    state.restore_proxied_writes();
                    state
                        .dirty_extents
                        .restore_before(pending.frozen.dirty_extents);
                    state.dirty = !state.dirty_extents.is_empty();
                }
                Err(error)
            }
        };
        cell.notify_all();
        outcome
    }

    fn send_file_commit(&self, pending: &PendingFileCommit, branch: &str) -> Result<AppliedCommit> {
        let committed_full_metadata =
            pending.batch.commit.metadata_delta.mode == CommitMetadataMode::Full;
        let version = pending.batch.commit.file_version.clone();
        let layout = pending.batch.commit.layout_root.clone();
        let _pipeline_rpc_accounting = if let Some(accounting) = &pending.batch._pipeline_accounting
        {
            let base = accounting.actual.load(Ordering::Relaxed);
            accounting.set_actual(base + commit_heap(pending.batch.commit.as_ref()))?;
            Some(CommitRpcAccounting {
                accounting: accounting.clone(),
                base,
            })
        } else {
            None
        };
        let publish_span = tracing::info_span!(
            "node.dfs.commit.publish",
            branch,
            inode_id = %pending.batch.commit.inode_id.0,
            operation_id = %pending.batch.commit.operation_id.0,
            through_seq = pending.batch.through_seq,
            chunks = pending.batch.commit.chunk_receipts.len(),
            file_length = pending.batch.commit.file_version.length,
            metadata_mode = ?pending.batch.commit.metadata_delta.mode,
        );
        let rpc_started = Instant::now();
        let updated = self.validate_inode(
            publish_span
                .in_scope(|| {
                    let publication_diagnostic = self.read_engine.publication_diagnostic();
                    let _coarse = self.read_engine.publication_coarse_diagnostic();
                    let result = self
                        .meta
                        .commit_file_version(pending.batch.commit.as_ref().clone());
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

    fn send_metadata_sync(
        &self,
        sync: &SyncInodeMetadata,
        through_seq: u64,
        branch: &str,
    ) -> Result<AppliedCommit> {
        let rpc_started = Instant::now();
        let updated =
            self.validate_inode(self.meta.sync_inode_metadata(sync.clone()).inspect_err(
                |error| {
                    afs_logging::warn!("dfs.commit_rpc_failed";
                        "method" => "sync_inode_metadata",
                        "inode_id" => sync.inode_id.0.as_str(),
                        "operation_id" => sync.operation_id.0.as_str(),
                        "branch" => branch,
                        "elapsed_ms" => rpc_started.elapsed().as_secs_f64() * 1000.0,
                        "error" => error.to_string()
                    );
                },
            )?)?;
        Ok(AppliedCommit::Metadata {
            through_seq,
            updated,
        })
    }

    fn apply_file_commit_locked(
        state: &mut InodeWriteState,
        through_seq: u64,
        committed_full_metadata: bool,
        committed_kill_suidgid: bool,
        version: FileVersion,
        layout: LayoutRoot,
        mut updated: InodeRecord,
    ) {
        state.in_flight = None;
        state.meta_mtime_unix_ms = updated.attributes.mtime_unix_ms;
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
        state.commit_proxied_writes(&version.id, updated.revision);
        state.inode = updated;
        state.base_version = Some(version);
        state.base_layout = layout;
        // In-flight reads keep their own reference of the old base (R94).
        let _retired_base_reference = state.base_read_reference.take();
        state.dirty = !state.dirty_extents.is_empty();
        state.durable_write_seq = through_seq;
        state.committed_write_seq = through_seq;
        state.last_writer_background_requested = false;
        state.writeback_retry_at = None;
        state.writeback_failures = 0;
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
    }

    /// A metadata-only sync lost to a newer inode on Meta (B5). When Meta's
    /// mtime moved since this state last took it, the newer update set the
    /// times explicitly (utimens on any node): it supersedes the write times
    /// owed for writes up to `through_seq`, so they are dropped, unless the
    /// sync also carries a privilege clear or a write came later. Otherwise
    /// the update was unrelated (a chmod): the owed times are kept and the
    /// next sync is built on the newer revision.
    ///
    /// Known limit, accepted: Meta's refusal does not say which update won,
    /// so an explicit utimens that sets exactly the mtime this state last took
    /// from Meta reads as "unrelated", and the rebuilt sync then writes the
    /// write time over it. Rebuilds in a row are bounded by
    /// `METADATA_SYNC_REBUILD_LIMIT`; past it the refusal takes the terminal
    /// error path like any definite commit rejection.
    fn settle_metadata_sync_conflict(
        state: &mut InodeWriteState,
        sync: &SyncInodeMetadata,
        through_seq: u64,
        current: InodeRecord,
    ) {
        state.in_flight = None;
        let times_set_elsewhere = current.attributes.mtime_unix_ms != state.meta_mtime_unix_ms;
        let meta_mtime = current.attributes.mtime_unix_ms;
        if times_set_elsewhere
            && !sync.metadata_delta.kill_suidgid
            && state.visible_write_seq <= through_seq
        {
            state.inode = current;
            state.metadata_dirty = false;
            state.last_writer_background_requested = false;
        } else {
            state.metadata_sync_rebuilds = state.metadata_sync_rebuilds.saturating_add(1);
            let mut rebuilt = current;
            rebuilt.attributes.mtime_unix_ms = state.inode.attributes.mtime_unix_ms;
            rebuilt.attributes.ctime_unix_ms = rebuilt
                .attributes
                .ctime_unix_ms
                .max(state.inode.attributes.ctime_unix_ms);
            if state.kill_suidgid_dirty {
                rebuilt.attributes.mode = state.inode.attributes.mode;
            }
            state.inode = rebuilt;
        }
        state.meta_mtime_unix_ms = meta_mtime;
    }

    /// A definite rejection is terminal for the inode; anything else leaves the
    /// outcome unknown, so the commit stays in flight for an exact retry.
    fn record_commit_error_locked(state: &mut InodeWriteState, error: &Error, definite: bool) {
        if definite {
            state.in_flight = None;
            state.terminal_error = Some(error.clone());
        }
    }

    fn needs_full_metadata_sync(&self, cell: &SharedInodeWriteState) -> Result<bool> {
        let state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        Ok(state.metadata_dirty)
    }
}

impl CommitReason {
    pub(super) const DATA_SYNC: Self = Self::Foreground(ForegroundCommit::DataSync);
    pub(super) const FULL_SYNC: Self = Self::Foreground(ForegroundCommit::FullSync);
    pub(super) const CLOSE: Self = Self::Foreground(ForegroundCommit::Close);
    pub(super) const NODE_DRAIN: Self = Self::Foreground(ForegroundCommit::NodeDrain);
    pub(super) const BACKGROUND: Self = Self::Background(BackgroundCommit::Periodic);

    /// Label for operation ids, spans and logs.
    pub(super) fn label(self) -> &'static str {
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

    pub(super) fn metadata_delta(
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
