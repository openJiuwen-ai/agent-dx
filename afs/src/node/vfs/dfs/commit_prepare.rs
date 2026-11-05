use super::*;

impl DistributedFs {
    pub(super) fn begin_inode_operation(
        &self,
        inode_id: &InodeId,
        cell: SharedInodeWriteState,
    ) -> Result<InodeOperationGuard> {
        loop {
            let mut state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            while state.operation_busy
                || state.commit_busy
                    && (self.write_resources.pipeline.is_none()
                        || Self::in_flight_blocks_mutation(&state.in_flight))
            {
                state = cell
                    .wait(state)
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            }
            if let Some(error) = state.terminal_error.clone() {
                return Err(error);
            }
            if !Self::in_flight_blocks_mutation(&state.in_flight) {
                state.operation_busy = true;
                drop(state);
                return Ok(InodeOperationGuard {
                    cell,
                    native_notify: self.writeback_notify(),
                });
            }
            drop(state);
            if let Some(seq) = self.commit_inode(
                inode_id,
                CommitReason::BACKGROUND,
                CommitWait::AfterOperations,
            )? {
                self.update_handles_after_commit(inode_id, seq)?;
            }
        }
    }

    pub(super) fn lock_for_mutation<'a>(
        &self,
        inode_id: &InodeId,
        cell: &'a SharedInodeWriteState,
    ) -> Result<InodeMutationGuard<'a>> {
        let ticket = self.mutation_ticket(cell)?;
        loop {
            let mut state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            while state.operation_busy
                || state.commit_busy
                    && (self.write_resources.pipeline.is_none()
                        || Self::in_flight_blocks_mutation(&state.in_flight))
            {
                state = cell
                    .wait(state)
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            }
            if let Some(error) = state.terminal_error.clone() {
                return Err(error);
            }
            if !Self::in_flight_blocks_mutation(&state.in_flight) {
                return Ok(InodeMutationGuard {
                    state,
                    _ticket: ticket,
                });
            }
            drop(state);
            self.commit_inode(
                inode_id,
                CommitReason::BACKGROUND,
                CommitWait::AfterOperations,
            )?;
        }
    }

    pub(super) fn in_flight_blocks_mutation(in_flight: &Option<InFlightCommit>) -> bool {
        matches!(
            in_flight,
            Some(InFlightCommit::File(_) | InFlightCommit::Metadata(_, _))
        )
    }

    fn prepare_native_generation(
        &self,
        frozen: &mut FrozenCommit,
        operation_id: &OperationId,
        layout_id: LayoutRootId,
        retained_uploads: &mut Vec<Arc<PipelineUpload>>,
    ) -> Result<(LayoutRoot, Vec<ChunkReceipt>)> {
        let mut plan = CommitPlanner.plan_with_integrity(
            frozen,
            operation_id.clone(),
            layout_id,
            self.crc32c_writes(),
        )?;
        let mut receipts: Vec<ChunkReceipt> = Vec::new();
        for extent in frozen.dirty_extents.extents.values() {
            if let Some(upload) = &extent.native_upload {
                let receipt = self.wait_upload(upload)?;
                if extent.file_offset < frozen.logical_length {
                    plan.layout_root.inline_extents.push(Extent {
                        file_offset: extent.file_offset,
                        length: extent
                            .length
                            .min(frozen.logical_length - extent.file_offset),
                        chunk_id: receipt.chunk.id.clone(),
                        chunk_offset: extent.source_offset as u64,
                    });
                    if !receipts
                        .iter()
                        .any(|previous| previous.chunk.id == receipt.chunk.id)
                    {
                        receipts.push(receipt);
                    }
                }
            }
        }
        for staged in unique_staged_chunks(plan.staged_chunks)? {
            let body = staged.shared_bytes();
            let descriptor = frozen
                .dirty_extents
                .iter_extents()
                .find_map(|extent| {
                    extent
                        .data
                        .as_ref()
                        .filter(|bytes| bytes.allocation_id() == body.allocation_id())
                        .and(extent.native_descriptor.as_ref())
                        .cloned()
                })
                .ok_or_else(|| {
                    unavailable("native publication body has no descriptor reservation")
                })?;
            let upload = Arc::new(PipelineUpload {
                _gc_pin: frozen.dirty_extents.native_gc_pin.clone(),
                operation_id: operation_id.clone(),
                chunk: staged.chunk.clone(),
                owner: self
                    .write_state(&frozen.inode.inode_id)?
                    .as_ref()
                    .map_or_else(Weak::new, Arc::downgrade),
                state: Mutex::new(PipelineUploadState {
                    body: Some(body),
                    staged: Some(staged),
                    task: None,
                    receipt: None,
                    error: None,
                    terminal_error: None,
                    joining: false,
                }),
                cv: Condvar::new(),
                _descriptor: descriptor,
            });
            if upload.chunk.is_segment_crc32c() {
                // Preserve the immutable tail identity before any fallible I/O.
                // Failed preparation restores this map, including successful
                // receipts. Dispatch keeps the original sequential order.
                for extent in plan
                    .layout_root
                    .inline_extents
                    .iter()
                    .filter(|extent| extent.chunk_id == upload.chunk.id)
                {
                    frozen.dirty_extents.insert_native(DirtyExtent {
                        file_offset: extent.file_offset,
                        length: extent.length,
                        write_seq: frozen.through_seq,
                        data: None,
                        builder: None,
                        source_offset: usize::try_from(extent.chunk_offset)
                            .map_err(|_| invalid("native tail offset is too large"))?,
                        native_reuse_end: extent.file_offset + extent.length,
                        native_upload: Some(upload.clone()),
                        native_descriptor: Some(upload._descriptor.clone()),
                    });
                }
            }
            retained_uploads.push(upload.clone());
            let receipt = self.wait_upload(&upload)?;
            if !receipts
                .iter()
                .any(|previous| previous.chunk.id == receipt.chunk.id)
            {
                receipts.push(receipt);
            }
        }
        plan.layout_root
            .inline_extents
            .sort_by_key(|extent| extent.file_offset);
        plan.layout_root
            .inline_extents
            .dedup_by(|extent, previous| {
                if previous.file_offset + previous.length == extent.file_offset
                    && previous.chunk_id == extent.chunk_id
                    && previous.chunk_offset + previous.length == extent.chunk_offset
                {
                    previous.length += extent.length;
                    true
                } else {
                    false
                }
            });
        receipts.sort_by(|left, right| left.chunk.id.0.cmp(&right.chunk.id.0));
        Ok((plan.layout_root, receipts))
    }

    pub(super) fn prepare_commit(
        &self,
        frozen: &mut FrozenCommit,
        reason: CommitReason,
    ) -> std::result::Result<CommitBatch, PrepareCommitError> {
        let dirty_bytes = frozen.dirty_extents.dirty_data_bytes();
        let span = tracing::info_span!(
            "node.dfs.commit.prepare",
            reason = reason.label(),
            inode_id = %frozen.inode.inode_id.0,
            logical_length = frozen.logical_length,
            dirty_bytes,
            staged_chunks = tracing::field::Empty,
            staged_bytes = tracing::field::Empty,
            unique_chunks = tracing::field::Empty,
            unique_bytes = tracing::field::Empty,
            receipts = tracing::field::Empty,
        );
        let _entered = span.enter();
        let (operation_id, gc_pin) = self.commit_operation_and_gc_pin(frozen, reason)?;
        let generation = self.identity.next_operation.fetch_add(1, Ordering::Relaxed);
        let layout_id =
            LayoutRootId::new(format!("{}-layout-{generation}", self.identity.session_id));

        let mut native_uploads = Vec::new();
        let (layout_root, receipts) = if frozen.dirty_extents.native_operation_id.is_some() {
            self.prepare_native_generation(frozen, &operation_id, layout_id, &mut native_uploads)
                .map_err(|error| PrepareCommitError { error })?
        } else {
            self.prepare_staged_generation(frozen, &operation_id, layout_id, reason, &span)
                .map_err(|error| PrepareCommitError { error })?
        };
        span.record("receipts", receipts.len() as u64);
        let gc_fence = gc_pin.as_ref().and_then(|pin| pin.fence());
        if self.gc.references.is_some() && gc_fence.is_none() {
            return Err(PrepareCommitError {
                error: unavailable("optimistic commit has no frozen GC fence"),
            });
        }
        let commit = self.commit_record(
            frozen,
            reason,
            generation,
            (operation_id, gc_fence),
            layout_root,
            receipts,
        );
        let native_accounting = self.reserve_commit_accounting(frozen, &commit, &native_uploads)?;
        Ok(CommitBatch {
            _gc_pin: gc_pin,
            through_seq: frozen.through_seq,
            _pipeline_accounting: native_accounting,
            _pipeline_uploads: native_uploads,
            commit: Arc::new(commit),
        })
    }

    fn commit_operation_and_gc_pin(
        &self,
        frozen: &mut FrozenCommit,
        reason: CommitReason,
    ) -> std::result::Result<(OperationId, Option<Arc<crate::node::gc::GcPin>>), PrepareCommitError>
    {
        let operation_id = frozen
            .dirty_extents
            .native_operation_id
            .clone()
            .or_else(|| {
                // Length-only preparation can fail after admission. The pin
                // already owns its exact operation; never mint a replacement.
                frozen
                    .dirty_extents
                    .native_gc_pin
                    .as_ref()
                    .and_then(|pin| pin.write_operation_id())
            })
            .unwrap_or_else(|| self.identity.operation_id(reason.label()));
        let gc_pin = match (&frozen.dirty_extents.native_gc_pin, &self.gc.references) {
            (Some(pin), _) => Some(pin.clone()),
            (None, Some(references)) => Some(
                references
                    .pin_generation(
                        &frozen.inode.inode_id,
                        frozen.inode.head_version.as_ref(),
                        &operation_id,
                    )
                    .map_err(|error| PrepareCommitError { error })?,
            ),
            (None, None) => None,
        };
        if self.gc.references.is_some() {
            // Length-only generations have no write-time pin. Freeze their
            // captured fence as well; preparation retries never refresh it.
            frozen.dirty_extents.native_gc_pin = gc_pin.clone();
        }
        Ok((operation_id, gc_pin))
    }

    /// A generation without native uploads: plan it and store its chunks here.
    fn prepare_staged_generation(
        &self,
        frozen: &FrozenCommit,
        operation_id: &OperationId,
        layout_id: LayoutRootId,
        reason: CommitReason,
        span: &tracing::Span,
    ) -> Result<(LayoutRoot, Vec<ChunkReceipt>)> {
        let plan = CommitPlanner.plan(frozen, operation_id.clone(), layout_id)?;
        let staged_chunks = plan.staged_chunks.len();
        let staged_bytes = plan
            .staged_chunks
            .iter()
            .fold(0_u64, |sum, chunk| sum.saturating_add(chunk.chunk.length));
        span.record("staged_chunks", staged_chunks as u64);
        span.record("staged_bytes", staged_bytes);
        let unique = unique_staged_chunks(plan.staged_chunks)?;
        let unique_bytes = unique
            .iter()
            .fold(0_u64, |sum, chunk| sum.saturating_add(chunk.chunk.length));
        span.record("unique_chunks", unique.len() as u64);
        span.record("unique_bytes", unique_bytes);
        let receipts = tracing::info_span!(
            "node.dfs.commit.put_batch",
            reason = reason.label(),
            chunks = unique.len(),
            bytes = unique_bytes,
            reused_receipts = 0_u64,
        )
        .in_scope(|| self.chunk_store.put_chunks(unique, PutIntent::Commit))?
        .into_iter()
        .map(|stored| stored.receipt)
        .collect();
        Ok((plan.layout_root, receipts))
    }

    fn commit_record(
        &self,
        frozen: &FrozenCommit,
        reason: CommitReason,
        generation: u64,
        (operation_id, gc_fence): (OperationId, Option<crate::dfs::GcFence>),
        layout_root: LayoutRoot,
        receipts: Vec<ChunkReceipt>,
    ) -> CommitFileVersion {
        let now = now_unix_ms();
        let version = FileVersion {
            id: FileVersionId::new(format!("{}-version-{generation}", self.identity.session_id)),
            inode_id: frozen.inode.inode_id.clone(),
            parent_version: frozen
                .base_version
                .as_ref()
                .map(|version| version.id.clone()),
            length: frozen.logical_length,
            layout_root: layout_root.id.clone(),
            created_at_unix_ms: now,
        };
        CommitFileVersion {
            gc_fence,
            operation_id,
            inode_id: frozen.inode.inode_id.clone(),
            write_lease: frozen.write_lease.clone(),
            expected_inode_revision: frozen.inode.revision,
            expected_head_version: frozen
                .base_version
                .as_ref()
                .map(|version| version.id.clone()),
            file_version: version,
            layout_root,
            chunk_receipts: receipts,
            clone_source: None,
            metadata_delta: reason.metadata_delta(
                frozen.inode.attributes.mtime_unix_ms,
                frozen.inode.attributes.ctime_unix_ms,
                frozen.kill_suidgid_dirty,
            ),
        }
    }

    /// Charge the commit's metadata against scratch its descriptors already
    /// reserved. Admission happened at write time, so this fails, never waits.
    fn reserve_commit_accounting(
        &self,
        frozen: &FrozenCommit,
        commit: &CommitFileVersion,
        native_uploads: &Vec<Arc<PipelineUpload>>,
    ) -> std::result::Result<Option<Arc<CommitAccounting>>, PrepareCommitError> {
        if frozen.dirty_extents.native_operation_id.is_none()
            || commit.chunk_receipts.is_empty()
                && !frozen
                    .dirty_extents
                    .iter_extents()
                    .any(|extent| extent.native_descriptor.is_some())
        {
            return Ok(None);
        }
        let pipeline =
            self.write_resources
                .pipeline
                .as_ref()
                .ok_or_else(|| PrepareCommitError {
                    error: unavailable("native write pipeline is missing"),
                })?;
        let mut owners: Vec<Arc<Mutex<PipelineMetadataLease>>> = Vec::new();
        for extent in frozen.dirty_extents.iter_extents() {
            if let Some(owner) = &extent.native_descriptor
                && !owners.iter().any(|other| Arc::ptr_eq(other, owner))
            {
                owners.push(owner.clone());
            }
        }
        let mut owned_headroom = 0usize;
        for owner in &owners {
            let owner = owner.lock().map_err(|_| PrepareCommitError {
                error: unavailable("native descriptor reservation is poisoned"),
            })?;
            owned_headroom =
                owned_headroom.saturating_add(owner.bytes.saturating_sub(owner.actual_bytes));
        }
        let base = commit_heap(commit)
            + owners.capacity() * std::mem::size_of::<Arc<Mutex<PipelineMetadataLease>>>()
            + native_uploads.capacity() * std::mem::size_of::<Arc<PipelineUpload>>();
        let reserved = base.saturating_mul(3);
        let mut state = pipeline
            .metadata
            .state
            .lock()
            .map_err(|_| PrepareCommitError {
                error: unavailable("native metadata budget is poisoned"),
            })?;
        if reserved > owned_headroom
            || reserved
                > state
                    .descriptor_reserved_bytes
                    .saturating_sub(state.descriptor_bytes)
                    .saturating_sub(state.scratch_reserved_bytes)
        {
            return Err(PrepareCommitError {
                error: unavailable("native commit cannot fit its pre-reserved publication scratch"),
            });
        }
        state.scratch_reserved_bytes += reserved;
        state.descriptor_bytes += base;
        state.descriptor_peak = state.descriptor_peak.max(state.descriptor_bytes);
        drop(state);
        Ok(Some(Arc::new(CommitAccounting {
            budget: pipeline.metadata.clone(),
            reserved,
            actual: AtomicUsize::new(base),
            _owners: owners,
        })))
    }

    pub(super) fn observe_handle_error(&self, handle: FileHandle) -> Result<()> {
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
        afs_logging::warn!("dfs.background_error_consumed";
            "inode_id" => snapshot.inode_id.0.as_str(),
            "handle" => handle.0,
            "previous_cursor" => session.local.error_cursor,
            "cursor" => observed.cursor,
            "error" => observed.error.to_string()
        );
        Err(observed.error)
    }

    pub(super) fn resize_dirty_inode(
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
        if state.open_writers == 0 && (state.dirty || state.metadata_dirty) {
            state.last_writer_background_requested = true;
        }
        Ok(state.visible_write_seq)
    }

    pub(super) fn release_writer(&self, session: &DfsWriteSession) -> Result<()> {
        let Some(state) = self.write_state(&session.inode_id)? else {
            return Ok(());
        };
        let _native_ticket = self.mutation_ticket_with_cleanup(&state, true)?;
        let mut state = state
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        state.open_writers = state.open_writers.saturating_sub(1);
        if state.open_writers == 0 && (state.dirty || state.metadata_dirty) {
            state.last_writer_background_requested = true;
        }
        Ok(())
    }
}

pub(super) struct CommitPlanner;

impl CommitPlanner {
    pub(super) fn plan(
        &self,
        frozen: &FrozenCommit,
        operation_id: OperationId,
        layout_id: LayoutRootId,
    ) -> Result<CommitPlan> {
        self.plan_with_integrity(frozen, operation_id, layout_id, false)
    }

    fn plan_with_integrity(
        &self,
        frozen: &FrozenCommit,
        operation_id: OperationId,
        layout_id: LayoutRootId,
        crc32c_writes: bool,
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

        let mut visible_sources: HashMap<(u64, usize, usize), Vec<(usize, usize)>> = HashMap::new();
        for segment in &overlay {
            let Some((bytes, source_offset)) = &segment.data else {
                continue;
            };
            let length = usize::try_from(segment.length)
                .map_err(|_| invalid("native source range is too large"))?;
            let end = source_offset
                .checked_add(length)
                .filter(|end| *end <= bytes.len())
                .ok_or_else(|| invalid("native source range exceeds immutable body"))?;
            let identity = (bytes.allocation_id(), bytes.as_ptr() as usize, bytes.len());
            visible_sources
                .entry(identity)
                .or_default()
                .push((*source_offset, end));
        }
        for ranges in visible_sources.values_mut() {
            ranges.sort_unstable();
            let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
            for (start, end) in ranges.drain(..) {
                if let Some(previous) = merged.last_mut()
                    && start <= previous.1
                {
                    previous.1 = previous.1.max(end);
                } else {
                    merged.push((start, end));
                }
            }
            *ranges = merged;
        }
        let mut staged_chunks = Vec::new();
        let mut staged_by_source = HashMap::new();
        for segment in overlay {
            let Some((bytes, source_offset)) = segment.data else {
                continue;
            };
            let identity = (bytes.allocation_id(), bytes.as_ptr() as usize, bytes.len());
            let source_end = source_offset + segment.length as usize;
            let (start, end) = visible_sources
                .get(&identity)
                .and_then(|ranges| {
                    ranges
                        .iter()
                        .copied()
                        .find(|(start, end)| *start <= source_offset && source_end <= *end)
                })
                .ok_or_else(|| invalid("native source has no final visible interval"))?;
            // Only final visible contiguous source ranges are hashed and
            // persisted. Restored frozen tails and overwritten interiors
            // stay owned but never become new submitted payload bytes.
            let key = (identity, start, end);
            let staged_index = match staged_by_source.get(&key) {
                Some(index) => *index,
                None => {
                    let index = staged_chunks.len();
                    let body = bytes.slice(start..end);
                    staged_chunks.push(if crc32c_writes {
                        StagedChunk::from_shared_crc32c(operation_id.clone(), body)?
                    } else {
                        StagedChunk::from_shared(operation_id.clone(), body)
                    });
                    staged_by_source.insert(key, index);
                    index
                }
            };
            let staged: &StagedChunk = &staged_chunks[staged_index];
            extents.push(Extent {
                file_offset: segment.file_offset,
                length: segment.length,
                chunk_id: staged.chunk.id.clone(),
                chunk_offset: (source_offset - start) as u64,
            });
        }
        extents.sort_by_key(|extent| extent.file_offset);
        let mut coalesced: Vec<Extent> = Vec::with_capacity(extents.len());
        for extent in extents {
            if let Some(previous) = coalesced.last_mut()
                && previous.file_offset + previous.length == extent.file_offset
                && previous.chunk_id == extent.chunk_id
                && previous.chunk_offset + previous.length == extent.chunk_offset
            {
                previous.length += extent.length;
            } else {
                coalesced.push(extent);
            }
        }
        Ok(CommitPlan {
            layout_root: LayoutRoot {
                id: layout_id,
                file_length: frozen.logical_length,
                inline_extents: coalesced,
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
    map.extents
        .values()
        .filter(|extent| extent.file_offset < file_length && extent.length > 0)
        .map(|extent| {
            Ok(OverlaySegment {
                file_offset: extent.file_offset,
                length: extent.length.min(file_length - extent.file_offset),
                data: extent
                    .data
                    .as_ref()
                    .map(|bytes| (bytes.clone(), extent.source_offset)),
            })
        })
        .collect()
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
