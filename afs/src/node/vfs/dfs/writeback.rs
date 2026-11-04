use super::*;

impl DistributedFs {
    /// Execute pending background commits without changing the user-visible
    /// close contract. Failures remain attached to the inode and are reported
    /// once per open writer by a later write/flush/sync operation.
    pub fn writeback_pending(&self) -> Result<usize> {
        if self.native.pipeline.is_none() {
            return Ok(0);
        }
        for (_, cell) in self.writes.states.snapshot()? {
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
        let mut entries = self.writes.states.snapshot()?;
        entries.sort_by(|left, right| left.0.0.cmp(&right.0.0));
        if !entries.is_empty() {
            let start = self.writes.lease_cursor.load(Ordering::Relaxed) as usize % entries.len();
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
            let renewal = crate::node::call_deadline::within(deadline, || {
                self.meta.renew_write_lease(lease.clone())
            });
            match renewal {
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
        self.writes
            .lease_cursor
            .fetch_add(examined, Ordering::Relaxed);
        first_error.map_or(Ok(renewed_count), Err)
    }

    #[cfg(test)]
    pub(super) fn pause_after_writeback_prepare(&self) {
        if let Some(duration) = self
            .hooks
            .writeback_after_prepare_pause
            .lock()
            .unwrap()
            .take()
        {
            std::thread::sleep(duration);
        }
    }

    pub(super) fn freeze_state_for_commit(state: &mut InodeWriteState) -> FrozenCommit {
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
        let Some(pipeline) = &self.native.pipeline else {
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
            let inode_ids = self.writes.states.keys_snapshot()?;
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

    pub(super) fn record_background_error(&self, inode_id: &InodeId, error: Error) -> Result<()> {
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
