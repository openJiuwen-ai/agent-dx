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
    // Diagnostic only, shared by clones of this exact prepared request. Never
    // serialized into the domain request or consulted by filesystem policy.
    pub(super) trace_send_attempt: Option<Arc<AtomicU64>>,
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
    pub(super) _native_accounting: Option<Arc<NativeCommitAccounting>>,
    pub(super) _native_uploads: Vec<Arc<NativeUpload>>,
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
    /// FUSE flush on close(2); data only (decision 5).
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

    pub(super) fn finish_commit(
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
