use super::*;

/// Where a native write's next bytes go, as read before unlocked admission.
struct WriteTarget {
    position: u64,
    selection: Option<(usize, usize, usize)>,
    capacity: usize,
    generation: Option<OperationId>,
    /// The committed version this write builds on; a new generation keeps it
    /// from collection until the generation's commit is applied (R81 F2).
    base: Option<FileVersionId>,
    seq: u64,
    owner: Arc<DirtyOwner>,
}

struct ReservationRequest<'a> {
    turn_id: u64,
    operation_id: &'a OperationId,
    owner: &'a Arc<DirtyOwner>,
    capacity: usize,
    has_accepted_prefix: bool,
}

enum Reservation {
    Reserved {
        builder: ChunkBuilder,
        descriptor: Option<Arc<Mutex<PipelineMetadataLease>>>,
    },
    /// Out of memory with a prefix already visible: end the write short.
    StopAccepting,
}

struct WriteApply<'a> {
    append: bool,
    offset: u64,
    data: &'a [u8],
    accepted: usize,
    kill_suidgid: bool,
    target: WriteTarget,
    operation_id: OperationId,
    proxied_writer: Option<&'a DfsWriteSessionId>,
}

impl DistributedFs {
    /// Native storage writes segmented-CRC32C objects; S3 stores only
    /// generational BLAKE3 objects (S1a-7 F6, R05).
    pub(super) fn crc32c_writes(&self) -> bool {
        !self.read_engine.has_external_backend()
    }

    /// The one place a write's payload gets its immutable identity.
    pub(super) fn stage_write_chunk(
        &self,
        op: OperationId,
        bytes: SharedChunkBytes,
    ) -> Result<StagedChunk> {
        if self.crc32c_writes() {
            StagedChunk::from_shared_crc32c(op, bytes)
        } else {
            Ok(StagedChunk::from_shared_generational(op, bytes))
        }
    }

    pub(crate) fn writeback_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.write_resources
            .pipeline
            .as_ref()
            .map(|pipeline| pipeline.notify.clone())
    }

    pub(crate) fn close_write_admission(&self) {
        if let Some(pipeline) = &self.write_resources.pipeline {
            pipeline.closed.store(true, Ordering::Release);
            pipeline.budget.stop();
            pipeline.notify.notify_waiters();
            if let Ok(states) = self.writes.states.snapshot() {
                for (_, state) in states {
                    state.notify_all();
                }
            }
        }
    }

    pub(super) fn mutation_ticket(
        &self,
        cell: &SharedInodeWriteState,
    ) -> Result<Option<MutationTicket>> {
        self.mutation_ticket_with_cleanup(cell, false)
    }

    pub(super) fn mutation_ticket_with_cleanup(
        &self,
        cell: &SharedInodeWriteState,
        cleanup: bool,
    ) -> Result<Option<MutationTicket>> {
        let Some(pipeline) = &self.write_resources.pipeline else {
            return Ok(None);
        };
        if !cleanup && pipeline.closed.load(Ordering::Acquire) {
            return Err(unavailable("native write admission is closed"));
        }
        let id = pipeline.next_ticket.fetch_add(1, Ordering::Relaxed);
        let mut state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        if state.native_thread == Some(std::thread::current().id()) {
            return Ok(None);
        }
        state.native_queue.push(id, &pipeline.metadata)?;
        let ticket = MutationTicket {
            id,
            cell: cell.clone(),
            pipeline: pipeline.clone(),
        };
        loop {
            if !cleanup && pipeline.closed.load(Ordering::Acquire) {
                drop(state);
                drop(ticket);
                return Err(unavailable("native write admission is closed"));
            }
            if crate::node::call_deadline::remaining().is_some_and(|time| time.is_zero()) {
                drop(state);
                drop(ticket);
                return Err(Error::coded(
                    afs_error::IO_TIMEOUT,
                    "DFS write turn deadline elapsed",
                ));
            }
            if state.native_queue.values.front() == Some(&id) {
                state.native_thread = Some(std::thread::current().id());
                break;
            }
            state = cell
                .wait_timeout(
                    state,
                    crate::node::call_deadline::bounded(Duration::from_millis(10)),
                )
                .map_err(|_| unavailable("native mutation queue is poisoned"))?;
        }
        drop(state);
        Ok(Some(ticket))
    }

    pub(super) fn observe_pipeline_resources(&self) {
        let Some(pipeline) = &self.write_resources.pipeline else {
            return;
        };
        pipeline.observe();
        if let Some(metrics) = &pipeline.metrics
            && let Ok(states) = self.pending_write_states()
        {
            let mut pending = 0_u64;
            for (_, cell) in states {
                if let Ok(state) = cell.lock() {
                    pending += u64::from(state.dirty_extents.native_operation_id.is_some());
                    pending += u64::from(state.in_flight.is_some());
                }
            }
            metrics.native_write_resources(&[("pending_generations", pending)]);
        }
    }

    fn wait_mutation_ready(&self, inode: &InodeId, cell: &SharedInodeWriteState) -> Result<()> {
        loop {
            let state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            if let Some(error) = &state.terminal_error {
                return Err(error.clone());
            }
            if !Self::in_flight_blocks_mutation(&state.in_flight) {
                return Ok(());
            }
            let remaining = crate::node::call_deadline::bounded(Duration::from_millis(10));
            if remaining.is_zero() {
                return Err(Error::coded(
                    afs_error::IO_TIMEOUT,
                    "DFS write mutation deadline elapsed",
                ));
            }
            if state.commit_busy {
                drop(
                    cell.wait_timeout(state, remaining)
                        .map_err(|_| unavailable("DFS inode write state is poisoned"))?,
                );
            } else {
                drop(state);
                if let Some(seq) =
                    self.commit_inode(inode, CommitReason::BACKGROUND, CommitWait::AfterOperations)?
                {
                    self.update_handles_after_commit(inode, seq)?;
                }
            }
        }
    }

    fn dirty_owner_limit(&self, pipeline: &WritePipeline) -> Result<usize> {
        let mut writers = 0;
        for (_, cell) in self.pending_write_states()? {
            let state = cell
                .lock()
                .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
            if state.open_writers != 0 || state.dirty {
                writers += 1;
            }
        }
        Ok((16 * 1024 * 1024).max(pipeline.budget.snapshot().limit / writers.max(1)))
    }

    pub(super) fn pipeline_ordinary_write(
        &self,
        handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
        cell: SharedInodeWriteState,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> Result<usize> {
        let mut accepted = 0;
        let result = crate::node::call_deadline::within_timeout(self.write_timeout, || {
            self.pipeline_ordinary_write_inner(
                handle,
                snapshot,
                cell,
                (offset, data, options),
                &mut accepted,
            )
        });
        match result {
            Err(error)
                if accepted > 0
                    && snapshot.flags & libc::O_SYNC != libc::O_SYNC
                    && !has_o_dsync(snapshot.flags) =>
            {
                // A prefix is already visible. Report its real length, retain
                // the failure for the next barrier, and never repeat input.
                self.record_background_error(&snapshot.inode_id, error)?;
                Ok(accepted)
            }
            result => result,
        }
    }

    fn pipeline_ordinary_write_inner(
        &self,
        handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
        cell: SharedInodeWriteState,
        input: (u64, &[u8], WriteOptions),
        accepted: &mut usize,
    ) -> Result<usize> {
        let (offset, data, options) = input;
        let pipeline = self
            .write_resources
            .pipeline
            .as_ref()
            .ok_or_else(|| unavailable("native write pipeline is missing"))?
            .clone();
        let ticket = self
            .mutation_ticket(&cell)?
            .ok_or_else(|| unavailable("native mutation ticket is missing"))?;
        let append = snapshot.flags & libc::O_APPEND != 0;
        let proxied_writer = snapshot
            .owner_scope
            .as_ref()
            .and(snapshot.write_session.as_ref())
            .map(|session| &session.local.id);
        while *accepted < data.len() {
            self.wait_mutation_ready(&snapshot.inode_id, &cell)?;
            if pipeline.closed.load(Ordering::Acquire) {
                return Err(unavailable("native write admission is closed"));
            }
            let _ = self.poll_uploads()?;
            self.observe_handle_error(handle)?;
            let target = Self::write_target(&cell, append, offset, data, *accepted)?;
            let operation_id = target
                .generation
                .clone()
                .unwrap_or_else(|| self.identity.operation_id("native-generation"));
            let (prepared, descriptor) = if target.selection.is_none() {
                match self.reserve_dirty_builder(
                    handle,
                    &cell,
                    &pipeline,
                    ReservationRequest {
                        turn_id: ticket.id,
                        operation_id: &operation_id,
                        owner: &target.owner,
                        capacity: target.capacity,
                        has_accepted_prefix: *accepted > 0,
                    },
                )? {
                    Reservation::Reserved {
                        builder,
                        descriptor,
                    } => (Some(builder), descriptor),
                    Reservation::StopAccepting => break,
                }
            } else {
                (None, None)
            };
            let gc_pin = if target.generation.is_none() {
                self.gc
                    .references
                    .as_ref()
                    .map(|references| {
                        references.pin_generation(
                            &snapshot.inode_id,
                            target.base.as_ref(),
                            &operation_id,
                        )
                    })
                    .transpose()?
            } else {
                None
            };
            let operation = self.begin_inode_operation(&snapshot.inode_id, cell.clone())?;
            let Some((written, accepted_seq)) = Self::apply_pipeline_write(
                &cell,
                WriteApply {
                    append,
                    offset,
                    data,
                    accepted: *accepted,
                    kill_suidgid: options.kill_suidgid,
                    target,
                    operation_id,
                    proxied_writer,
                },
                prepared,
                descriptor,
                gc_pin,
                operation,
            )?
            else {
                continue;
            };
            *accepted += written;
            self.update_handle_write_progress(handle, accepted_seq)?;
            self.seal_full_builders(&cell)?;
            self.start_uploads()?;
            self.observe_pipeline_resources();
        }
        // Keep the FIFO ticket, but release every short operation guard before
        // waiting for the persistent barrier. Preparing may finish meanwhile.
        if snapshot.flags & libc::O_SYNC == libc::O_SYNC {
            self.sync_handle(handle, CommitReason::FULL_SYNC, CommitWait::InsideOperation)?;
        } else if has_o_dsync(snapshot.flags) {
            self.sync_handle(handle, CommitReason::DATA_SYNC, CommitWait::InsideOperation)?;
        }
        drop(ticket);
        Ok(*accepted)
    }

    /// Read where the next unaccepted bytes land. Admission runs unlocked after
    /// this, so `apply_pipeline_write` revalidates the result under the same lock.
    fn write_target(
        cell: &SharedInodeWriteState,
        append: bool,
        offset: u64,
        data: &[u8],
        accepted: usize,
    ) -> Result<WriteTarget> {
        let state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        if let Some(error) = &state.terminal_error {
            return Err(error.clone());
        }
        let position = if append {
            state.logical_length
        } else {
            offset
                .checked_add(accepted as u64)
                .ok_or_else(file_too_large)?
        };
        position
            .checked_add((data.len() - accepted) as u64)
            .ok_or_else(file_too_large)?;
        let selection = state.dirty_extents.native_selection(position);
        let capacity = state.dirty_extents.native_capacity(
            position,
            data.len() - accepted,
            state.logical_length,
        );
        Ok(WriteTarget {
            position,
            selection,
            capacity,
            generation: state.dirty_extents.native_operation_id.clone(),
            base: state
                .base_version
                .as_ref()
                .map(|version| version.id.clone()),
            seq: state.next_write_seq,
            owner: state.native_owner.clone(),
        })
    }

    /// Take this writer's allocation turn and wait for its metadata reservation
    /// and body capacity. The turn is released on every return.
    fn reserve_dirty_builder(
        &self,
        handle: FileHandle,
        cell: &SharedInodeWriteState,
        pipeline: &Arc<WritePipeline>,
        request: ReservationRequest<'_>,
    ) -> Result<Reservation> {
        let turn_id = request.turn_id;
        let deadline = Instant::now() + crate::node::call_deadline::bounded(self.write_timeout);
        let mut descriptor = None;
        {
            let mut queue = pipeline
                .allocation_queue
                .lock()
                .map_err(|_| unavailable("native allocation queue is poisoned"))?;
            queue.push(turn_id, &pipeline.metadata)?;
        }
        let _turn = AllocationTurn {
            id: turn_id,
            pipeline: pipeline.clone(),
        };
        loop {
            if pipeline.closed.load(Ordering::Acquire) {
                return Err(unavailable("native write admission is closed"));
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                self.observe_handle_error(handle)?;
                if request.has_accepted_prefix {
                    return Ok(Reservation::StopAccepting);
                }
                return Err(Error::coded(
                    afs_error::IO_TIMEOUT,
                    "DFS write admission deadline elapsed",
                ));
            }
            let retry_after = remaining.min(Duration::from_millis(10));
            let is_front = pipeline
                .allocation_queue
                .lock()
                .map_err(|_| unavailable("native allocation queue is poisoned"))?
                .values
                .front()
                == Some(&turn_id);
            if is_front {
                if descriptor.is_none() {
                    descriptor = self.reserve_descriptor(pipeline, request.operation_id)?;
                    if descriptor.is_none() {
                        self.request_pipeline_pressure(cell, true)?;
                        crate::node::call_deadline::within(deadline, || self.progress_writeback())?;
                        let epoch = pipeline.budget.snapshot().epoch;
                        pipeline.budget.wait_for_change(epoch, retry_after);
                        continue;
                    }
                }
                let limit = self.dirty_owner_limit(pipeline)?;
                match pipeline.budget.try_reserve_for(
                    request.owner.clone(),
                    request.capacity,
                    limit,
                )? {
                    DirtyAdmission::Reserved(lease) => {
                        match ChunkBuilder::with_native_lease(
                            self.write_resources.buffer_pool.clone(),
                            request.capacity,
                            lease,
                        ) {
                            Ok(builder) => {
                                return Ok(Reservation::Reserved {
                                    builder,
                                    descriptor,
                                });
                            }
                            Err(error)
                                if error.code() == afs_error::IO_OUT_OF_MEMORY
                                    && request.has_accepted_prefix =>
                            {
                                // The rejected tail was never accepted. Complete
                                // any sync barrier for the visible prefix below.
                                return Ok(Reservation::StopAccepting);
                            }
                            Err(error) if error.code() == afs_error::IO_WOULD_BLOCK => {}
                            Err(error) => return Err(error),
                        }
                    }
                    DirtyAdmission::Wait { epoch } => {
                        self.request_pipeline_pressure(cell, false)?;
                        crate::node::call_deadline::within(deadline, || self.progress_writeback())?;
                        pipeline.budget.wait_for_change(epoch, retry_after);
                    }
                }
            } else {
                let epoch = pipeline.budget.snapshot().epoch;
                pipeline.budget.wait_for_change(epoch, retry_after);
            }
            let _ = self.poll_uploads()?;
            self.observe_handle_error(handle)?;
        }
    }

    /// `None` means the metadata budget is full and the caller must apply
    /// pressure and retry.
    fn reserve_descriptor(
        &self,
        pipeline: &WritePipeline,
        operation_id: &OperationId,
    ) -> Result<Option<Arc<Mutex<PipelineMetadataLease>>>> {
        let reservation = self
            .chunk_store
            .descriptor_reservation_bytes(operation_id)?;
        if reservation > PIPELINE_DESCRIPTOR_BYTES {
            return Err(Error::coded(
                afs_error::IO_CAPACITY,
                "native descriptor and publication scratch exceed their fixed metadata budget",
            ));
        }
        let descriptor = pipeline
            .metadata
            .reserve(reservation, true)?
            .map(|lease| Arc::new(Mutex::new(lease)));
        if let Some(owner) = &descriptor {
            owner
                .lock()
                .map_err(|_| unavailable("native descriptor reservation is poisoned"))?
                .set_actual(
                    std::mem::size_of::<Mutex<PipelineMetadataLease>>()
                        + 2 * std::mem::size_of::<usize>(),
                )?;
        }
        Ok(descriptor)
    }

    /// Publish one prefix under the inode state lock, returning its length and
    /// write sequence. `None` means the target moved during unlocked admission
    /// and the caller must retry. Consumes the operation guard so that it is
    /// released after the state lock, whichever way this returns.
    fn apply_pipeline_write(
        cell: &SharedInodeWriteState,
        write: WriteApply<'_>,
        prepared: Option<ChunkBuilder>,
        descriptor: Option<Arc<Mutex<PipelineMetadataLease>>>,
        gc_pin: Option<Arc<crate::node::gc::GcPin>>,
        operation: InodeOperationGuard,
    ) -> Result<Option<(usize, u64)>> {
        let WriteApply {
            append,
            offset,
            data,
            accepted,
            kill_suidgid,
            target,
            operation_id,
            proxied_writer,
        } = write;
        let position = target.position;
        let mut state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        let actual_position = if append {
            state.logical_length
        } else {
            offset + accepted as u64
        };
        if state.next_write_seq != target.seq
            || state.dirty_extents.native_operation_id != target.generation
            || actual_position != position
            || state.dirty_extents.native_selection(position) != target.selection
        {
            drop(state);
            drop(operation);
            drop(prepared);
            return Ok(None);
        }
        let next_seq = state.next_write_seq.saturating_add(1);
        if state.dirty_extents.native_operation_id.is_none() {
            state.dirty_extents.native_operation_id = Some(operation_id);
            state.dirty_extents.native_gc_pin = gc_pin;
        }
        let written = state.dirty_extents.write_native_prepared(
            position,
            &data[accepted..],
            next_seq,
            target.selection,
            prepared,
            descriptor,
        )?;
        state.logical_length = state.logical_length.max(position + written as u64);
        state.next_write_seq = next_seq;
        state.visible_write_seq = next_seq;
        if written != 0
            && let Some(writer) = proxied_writer
        {
            state.note_proxied_write(writer);
        }
        state.dirty = true;
        state.metadata_dirty = true;
        let now = now_unix_ms();
        state.inode.attributes.mtime_unix_ms = now;
        state.inode.attributes.ctime_unix_ms = now;
        if kill_suidgid {
            Self::apply_kernel_killpriv_to_state(&mut state);
        }
        drop(state);
        drop(operation);
        Ok(Some((written, next_seq)))
    }

    fn request_pipeline_pressure(
        &self,
        cell: &SharedInodeWriteState,
        descriptor: bool,
    ) -> Result<()> {
        let Some(pipeline) = &self.write_resources.pipeline else {
            return Ok(());
        };
        let mut state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        let retained_nonlocal = state.dirty_extents.iter_extents().any(|extent| {
            extent.native_upload.as_ref().is_some_and(|upload| {
                upload.state.lock().is_ok_and(|state| {
                    state.terminal_error.is_none()
                        && state.receipt.as_ref().is_some_and(|receipt| {
                            receipt.body_release == BodyRelease::RetainUntilPublish
                        })
                })
            })
        });
        if descriptor || retained_nonlocal {
            state.native_due = true;
            state.native_due_through = state.native_due_through.max(state.visible_write_seq);
            let mut metadata = pipeline
                .metadata
                .state
                .lock()
                .map_err(|_| unavailable("native metadata budget is poisoned"))?;
            if descriptor {
                metadata.pressure_descriptor += 1;
            } else {
                metadata.pressure_live += 1;
            }
        }
        pipeline.notify.notify_one();
        Ok(())
    }

    fn seal_full_builders(&self, cell: &SharedInodeWriteState) -> Result<()> {
        let Some(pipeline) = &self.write_resources.pipeline else {
            return Ok(());
        };
        let mut state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        let Some(operation_id) = state.dirty_extents.native_operation_id.clone() else {
            return Ok(());
        };
        while let Some(index) = state.dirty_extents.builders.iter().position(|entry| {
            !entry.builder.is_empty() && entry.builder.len() == entry.builder.writable_capacity()
        }) {
            let entry = &mut state.dirty_extents.builders[index];
            let descriptor = entry
                .descriptor
                .take()
                .ok_or_else(|| unavailable("native full builder has no descriptor reservation"))?;
            let end = entry.file_offset + entry.builder.len() as u64;
            let creation_order = entry.creation_order;
            let body = std::mem::take(&mut entry.builder).freeze();
            for extent in state.dirty_extents.extents.values_mut() {
                if extent.builder == Some(index) {
                    extent.builder = None;
                    extent.data = Some(body.clone());
                }
            }
            if let Some(indices) = state.dirty_extents.append_builders.get_mut(&end) {
                indices.remove(&(creation_order, index));
            }
            // The immutable allocation stays the same. Staging performs the
            // standard BLAKE3 scan; no body concatenation or clone is involved.
            let staged = self.stage_write_chunk(operation_id.clone(), body.clone())?;
            let upload = Arc::new(PipelineUpload {
                _gc_pin: state.dirty_extents.native_gc_pin.clone(),
                operation_id: operation_id.clone(),
                chunk: staged.chunk.clone(),
                owner: Arc::downgrade(cell),
                state: Mutex::new(PipelineUploadState {
                    body: Some(body),
                    staged: Some(staged),
                    task: None,
                    finished: None,
                    receipt: None,
                    error: None,
                    terminal_error: None,
                    joining: false,
                }),
                cv: Condvar::new(),
                _descriptor: descriptor,
            });
            let actual = std::mem::size_of::<PipelineUpload>()
                + std::mem::size_of::<Mutex<PipelineMetadataLease>>()
                + 4 * std::mem::size_of::<usize>()
                + upload.operation_id.0.capacity()
                + upload.chunk.id.0.capacity()
                + upload
                    .state
                    .lock()
                    .map_err(|_| unavailable("native upload state is poisoned"))?
                    .staged
                    .as_ref()
                    .map_or(0, StagedChunk::native_retained_metadata_bytes);
            upload
                ._descriptor
                .lock()
                .map_err(|_| unavailable("native descriptor reservation is poisoned"))?
                .set_actual(actual)?;
            let allocation = upload
                .state
                .lock()
                .map_err(|_| unavailable("native upload state is poisoned"))?
                .body
                .as_ref()
                .map(|body| body.allocation_id());
            for extent in state.dirty_extents.extents.values_mut() {
                if extent
                    .data
                    .as_ref()
                    .is_some_and(|body| Some(body.allocation_id()) == allocation)
                {
                    extent.data = None;
                    extent.native_upload = Some(upload.clone());
                }
            }
        }
        drop(state);
        pipeline.notify.notify_one();
        Ok(())
    }

    fn next_upload(&self) -> Result<Option<Arc<PipelineUpload>>> {
        // Existing inode registry snapshots are shared lifecycle metadata, not
        // a second W2 payload or per-object queue. Hold no registry lock while
        // examining an inode or waiting for an upload.
        for (_, cell) in self.pending_write_states()? {
            if let Some(upload) = Self::next_upload_for_cell(&cell)? {
                return Ok(Some(upload));
            }
        }
        Ok(None)
    }

    fn next_upload_for_cell(cell: &SharedInodeWriteState) -> Result<Option<Arc<PipelineUpload>>> {
        let state = cell
            .lock()
            .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
        if state.terminal_error.is_some()
            || state
                .writeback_retry_at
                .is_some_and(|at| at > Instant::now())
        {
            return Ok(None);
        }
        for extent in state.dirty_extents.iter_extents() {
            if let Some(upload) = &extent.native_upload {
                let upload_state = upload
                    .state
                    .lock()
                    .map_err(|_| unavailable("native upload state is poisoned"))?;
                if upload_state.receipt.is_none()
                    && upload_state.task.is_none()
                    && !upload_state.joining
                    && upload_state.error.is_none()
                    && upload_state.terminal_error.is_none()
                {
                    return Ok(Some(upload.clone()));
                }
            }
        }
        Ok(None)
    }

    pub(super) fn start_upload(&self, upload: &Arc<PipelineUpload>) -> Result<bool> {
        let pipeline = self
            .write_resources
            .pipeline
            .as_ref()
            .ok_or_else(|| unavailable("native write pipeline is missing"))?;
        let mut state = upload
            .state
            .lock()
            .map_err(|_| unavailable("native upload state is poisoned"))?;
        if let Some(error) = &state.terminal_error {
            return Err(error.clone());
        }
        if state.receipt.is_some() || state.task.is_some() || state.joining || state.error.is_some()
        {
            return Ok(false);
        }
        let Some(body) = state.body.clone() else {
            return Err(unavailable("native upload lost its body"));
        };
        let mut task_table = pipeline
            .tasks
            .lock()
            .map_err(|_| unavailable("native upload task table is poisoned"))?;
        if task_table.len() >= pipeline.metadata.upload_limit {
            return Ok(false);
        }
        {
            let mut metadata = pipeline
                .metadata
                .state
                .lock()
                .map_err(|_| unavailable("native metadata budget is poisoned"))?;
            if metadata.inflight >= pipeline.metadata.upload_limit {
                return Ok(false);
            }
            metadata.inflight += 1;
            metadata.inflight_peak = metadata.inflight_peak.max(metadata.inflight);
        }
        let permit = UploadPermit(pipeline.metadata.clone());
        let store = self.chunk_store.clone();
        let staged = match state.staged.take() {
            Some(staged) => staged,
            None if upload.chunk.is_segment_crc32c() => StagedChunk::from_shared_crc32c_id(
                upload.operation_id.clone(),
                body,
                upload.chunk.id.clone(),
            )?,
            None => self.stage_write_chunk(upload.operation_id.clone(), body)?,
        };
        if staged.chunk != upload.chunk {
            return Err(invalid("native upload changed its immutable object"));
        }
        let notify = pipeline.notify.clone();
        let finished = Arc::downgrade(upload);
        state.task = Some(crate::node::bridge::spawn_blocking_on(
            &pipeline.runtime,
            move || {
                let _permit = permit;
                let result = store
                    .put_chunks(vec![staged], PutIntent::PipelineUpload)
                    .and_then(|mut receipts| {
                        if receipts.len() != 1 {
                            return Err(invalid("native upload returned the wrong receipt count"));
                        }
                        Ok(receipts.remove(0))
                    });
                // Hand the result to whoever harvests first: a barrier waiting
                // on `cv`, or the next background pass (notified below).
                if let Some(upload) = finished.upgrade() {
                    if let Ok(mut state) = upload.state.lock() {
                        state.finished = Some(result);
                    }
                    upload.cv.notify_all();
                }
                notify.notify_one();
            },
        ));
        task_table.push(upload.clone());
        Ok(true)
    }

    fn install_upload_receipt(
        &self,
        upload: &Arc<PipelineUpload>,
        result: Result<StoredChunk>,
    ) -> Result<Option<Error>> {
        let pipeline = self
            .write_resources
            .pipeline
            .as_ref()
            .ok_or_else(|| unavailable("native write pipeline is missing"))?;
        // Do not move a successful receipt until every fallible structural lock
        // has been acquired. A broken installer must retain that identity and
        // the original body/reservation without advertising a healthy install.
        let mut result = Some(result);
        let installed: Result<Option<Error>> = (|| {
            let retained = result
                .as_ref()
                .ok_or_else(|| unavailable("native installer lost its retained result"))?;
            let failure = match retained {
                Ok(receipt)
                    if receipt.receipt.operation_id != upload.operation_id
                        || receipt.receipt.chunk != upload.chunk =>
                {
                    Some(invalid("native receipt differs from its immutable upload"))
                }
                Ok(receipt) => {
                    let actual = std::mem::size_of::<PipelineUpload>()
                        + std::mem::size_of::<Mutex<PipelineMetadataLease>>()
                        + 4 * std::mem::size_of::<usize>()
                        + upload.operation_id.0.capacity()
                        + upload.chunk.id.0.capacity()
                        + receipt_heap(&receipt.receipt);
                    let mut descriptor = upload
                        ._descriptor
                        .lock()
                        .map_err(|_| unavailable("native descriptor reservation is poisoned"))?;
                    let budget = descriptor.budget.clone();
                    let mut metadata = budget
                        .state
                        .lock()
                        .map_err(|_| unavailable("native metadata budget is poisoned"))?;
                    // Capacity refusal remains an owner-scoped business error.
                    descriptor.set_actual_locked(actual, &mut metadata).err()
                }
                Err(error) => Some(error.clone()),
            };
            // Retain the failure on its inode before a waiter can see it on the
            // upload: the waiter's barrier reports it from there, exactly once.
            if let (Some(error), Some(cell)) = (&failure, upload.owner.upgrade()) {
                let mut state = cell
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                Self::record_background_error_locked(&mut state, error.clone());
                if let Some(metrics) = &pipeline.metrics {
                    metrics.native_writeback_error();
                }
                cell.notify_all();
            }
            let mut state = upload
                .state
                .lock()
                .map_err(|_| unavailable("native upload state is poisoned"))?;
            let mut metadata = pipeline
                .metadata
                .state
                .lock()
                .map_err(|_| unavailable("native metadata budget is poisoned"))?;
            state.joining = false;
            if let Some(error) = &failure {
                state.error = Some(error.clone());
                metadata.failed += 1;
            } else {
                let Some(Ok(receipt)) = result.take() else {
                    return Err(unavailable("native installer lost its validated receipt"));
                };
                if receipt.body_release == BodyRelease::ReleaseAfterUpload {
                    state.body = None;
                }
                state.receipt = Some(receipt);
                metadata.completed += 1;
            }
            upload.cv.notify_all();
            if failure.is_none() {
                pipeline.notify.notify_one();
            }
            Ok(failure)
        })();
        if let Err(error) = &installed {
            // Poison recovery is cleanup-only: never clear poison, release a
            // body/charge, or permit this upload to retry as healthy. A valid
            // receipt is diagnostic retention, not an installed success.
            let mut state = upload
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            state.joining = false;
            state.terminal_error = Some(error.clone());
            if let Some(Ok(receipt)) = result.take()
                && receipt.receipt.operation_id == upload.operation_id
                && receipt.receipt.chunk == upload.chunk
            {
                state.receipt = Some(receipt);
            }
            drop(state);
            if let Some(cell) = upload.owner.upgrade() {
                cell.lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .terminal_error = Some(error.clone());
                cell.notify_all();
            }
            upload.cv.notify_all();
            pipeline.notify.notify_one();
        }
        installed
    }

    pub(super) fn poll_uploads(&self) -> Result<Option<Error>> {
        let Some(pipeline) = &self.write_resources.pipeline else {
            return Ok(None);
        };
        let tasks: [Option<Arc<PipelineUpload>>; 8] = {
            let tasks = pipeline
                .tasks
                .lock()
                .map_err(|_| unavailable("native upload task table is poisoned"))?;
            std::array::from_fn(|index| tasks.get(index).cloned())
        };
        let mut first_error = None;
        for upload in tasks.into_iter().flatten() {
            let harvest = {
                let mut state = upload
                    .state
                    .lock()
                    .map_err(|_| unavailable("native upload state is poisoned"))?;
                if let Some(result) = state.finished.take() {
                    // The worker is past its last use of the upload; detach it.
                    state.joining = true;
                    state.task = None;
                    Some(Ok(result))
                } else if state.task.as_ref().is_some_and(|task| task.is_finished()) {
                    // Finished without leaving a result: it panicked.
                    state.joining = true;
                    state.task.take().map(Err)
                } else {
                    None
                }
            };
            if let Some(harvest) = harvest {
                let result = match harvest {
                    Ok(result) => result,
                    Err(task) => crate::node::bridge::block_on(&pipeline.runtime, async {
                        match task.await {
                            Ok(()) => Err(unavailable("native upload worker left no result")),
                            Err(error) => {
                                Err(unavailable(format!("native upload worker failed: {error}")))
                            }
                        }
                    }),
                };
                let installed = self.install_upload_receipt(&upload, result);
                // A consumed JoinHandle must leave the table even when receipt
                // installation encounters a structural error. This guard only
                // removes the joined slot; a poisoned table remains poisoned.
                match pipeline.tasks.lock() {
                    Ok(mut table) => table.retain(|other| !Arc::ptr_eq(other, &upload)),
                    Err(poison) => {
                        poison
                            .into_inner()
                            .retain(|other| !Arc::ptr_eq(other, &upload));
                        return Err(unavailable("native upload task table is poisoned"));
                    }
                }
                if let Some(error) = installed? {
                    first_error.get_or_insert(error);
                }
            }
        }
        self.observe_pipeline_resources();
        // Business failures belong to their inode. A caller may have harvested
        // another inode's task; only drain consumes this Node-wide summary.
        Ok(first_error)
    }

    fn start_uploads(&self) -> Result<()> {
        while let Some(upload) = self.next_upload()? {
            if !self.start_upload(&upload)? {
                break;
            }
        }
        Ok(())
    }

    /// A barrier's wait for one upload. An earlier failed attempt was reported
    /// through the inode's error cursor, so this barrier retries it; a failure
    /// during this wait aborts the barrier, which reports it from that cursor.
    pub(super) fn wait_upload(&self, upload: &Arc<PipelineUpload>) -> Result<ChunkReceipt> {
        upload
            .state
            .lock()
            .map_err(|_| unavailable("native upload state is poisoned"))?
            .error = None;
        loop {
            let _ = self.poll_uploads()?;
            {
                let state = upload
                    .state
                    .lock()
                    .map_err(|_| unavailable("native upload state is poisoned"))?;
                if let Some(error) = &state.terminal_error {
                    return Err(error.clone());
                }
                if let Some(receipt) = &state.receipt {
                    return Ok(receipt.receipt.clone());
                }
                if let Some(error) = &state.error {
                    return Err(error.clone());
                }
            }
            if crate::node::call_deadline::remaining().is_some_and(|time| time.is_zero()) {
                return Err(Error::coded(
                    afs_error::IO_TIMEOUT,
                    "DFS upload deadline elapsed",
                ));
            }
            self.start_upload(upload)?;
            let state = upload
                .state
                .lock()
                .map_err(|_| unavailable("native upload state is poisoned"))?;
            if state.receipt.is_none() {
                drop(
                    upload
                        .cv
                        .wait_timeout(
                            state,
                            crate::node::call_deadline::bounded(Duration::from_millis(10)),
                        )
                        .map_err(|_| unavailable("native upload state is poisoned"))?,
                );
            }
        }
    }

    /// Whether `progress_writeback` could act now: a finished upload to
    /// harvest, an upload it may start, or a due inode. Conservative, cheap and
    /// non-blocking, so the background tick skips its blocking hop for the
    /// many wakeups that find nothing to do (R66).
    pub(crate) fn writeback_ready(&self) -> Result<bool> {
        let Some(pipeline) = &self.write_resources.pipeline else {
            return Ok(false);
        };
        let can_start = {
            let tasks = pipeline
                .tasks
                .lock()
                .map_err(|_| unavailable("native upload task table is poisoned"))?;
            for upload in tasks.iter() {
                let state = upload
                    .state
                    .lock()
                    .map_err(|_| unavailable("native upload state is poisoned"))?;
                if state.finished.is_some()
                    || state.task.as_ref().is_some_and(|task| task.is_finished())
                {
                    return Ok(true);
                }
            }
            tasks.len() < pipeline.metadata.upload_limit
                && pipeline
                    .metadata
                    .state
                    .lock()
                    .map_err(|_| unavailable("native metadata budget is poisoned"))?
                    .inflight
                    < pipeline.metadata.upload_limit
        };
        for (_, cell) in self.pending_write_states()? {
            {
                let state = cell
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                if state.terminal_error.is_none()
                    && state
                        .writeback_retry_at
                        .is_none_or(|at| at <= Instant::now())
                    && (state.native_due || state.last_writer_background_requested)
                {
                    return Ok(true);
                }
            }
            if can_start && Self::next_upload_for_cell(&cell)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn progress_writeback(&self) -> Result<usize> {
        if self.write_resources.pipeline.is_none() {
            return Ok(0);
        }
        let _ = self.poll_uploads()?;
        self.start_uploads()?;
        let mut committed = 0;
        for (inode, cell) in self.pending_write_states()? {
            let due = {
                let state = cell
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                state.terminal_error.is_none()
                    && state
                        .writeback_retry_at
                        .is_none_or(|at| at <= Instant::now())
                    && (state.native_due || state.last_writer_background_requested)
            };
            if due {
                let retained = Self::retained_error_cursor(&cell)?;
                match self.commit_inode(
                    &inode,
                    CommitReason::BACKGROUND,
                    CommitWait::AfterOperations,
                ) {
                    Ok(Some(seq)) => {
                        self.update_handles_after_commit(&inode, seq)?;
                        committed += 1;
                    }
                    Ok(None) => {}
                    // An upload failure was retained when it was harvested.
                    Err(_) if Self::retained_error_cursor(&cell)? != retained => {}
                    Err(error) => self.record_background_error(&inode, error)?,
                }
            }
        }
        self.observe_pipeline_resources();
        Ok(committed)
    }
}
