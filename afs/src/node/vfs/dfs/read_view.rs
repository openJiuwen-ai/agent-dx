use super::*;

impl DistributedFs {
    pub(super) fn capture_native_read_view(&self) -> Result<NativeReadView> {
        let started = Instant::now();
        let authority = self.read_engine.capture_native_read_authority()?;
        let deadline = authority
            .expires_at()
            .map_or(started + NATIVE_READ_VIEW_TTL, |expiry| {
                expiry.min(started + NATIVE_READ_VIEW_TTL)
            });
        Ok(NativeReadView {
            generation: 0,
            authority,
            deadline,
        })
    }

    pub(super) fn check_native_read_view(&self, view: &NativeReadView) -> Result<()> {
        self.read_engine
            .check_native_read_authority(&view.authority)?;
        if Instant::now() >= view.deadline {
            return Err(unavailable("DFS native read view expired before delivery"));
        }
        Ok(())
    }

    pub(super) fn invalidate_native_inode_views_on_error(&self, inode_id: &InodeId, error: &Error) {
        if matches!(
            error.kind(),
            afs_error::ErrorKind::PermissionDenied | afs_error::ErrorKind::Unauthenticated
        ) {
            // Preserve the originating failure. A poisoned handle table also
            // prevents any later read from reusing the rejected context.
            if let Ok(handles) = self.handles.for_inode(inode_id) {
                for handle in handles {
                    if let Ok(mut current) = handle.state.lock() {
                        current.native_read_view = None;
                        current.opened_page_identity = None;
                        current.native_read_generation =
                            current.native_read_generation.saturating_add(1);
                        current.native_denial_serial =
                            current.native_denial_serial.saturating_add(1);
                    }
                }
            }
        }
    }

    pub(super) fn invalidate_native_handle_view_on_error(&self, handle: FileHandle, error: &Error) {
        if matches!(
            error.kind(),
            afs_error::ErrorKind::PermissionDenied | afs_error::ErrorKind::Unauthenticated
        ) && let Ok(Some(handle)) = self.handles.get(handle.0)
            && let Ok(mut current) = handle.state.lock()
        {
            current.native_read_view = None;
            current.opened_page_identity = None;
            // An exhausted generation can never be used by a new view.
            current.native_read_generation = current.native_read_generation.saturating_add(1);
            current.native_denial_serial = current.native_denial_serial.saturating_add(1);
        }
    }

    #[cfg(test)]
    pub(super) fn native_inode_for_read(
        &self,
        handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
    ) -> PinnedReadView {
        let mut result = None;
        self.read_engine.run_sync_boundary(Box::pin(async {
            result = Some(self.native_inode_for_read_async(handle, snapshot).await?);
            Ok(())
        }))?;
        result.ok_or_else(|| unavailable("read view produced no result"))
    }

    /// The handle's pinned version and a current Node read-authority view.
    /// Other nodes' commits reach a handle only through a new open (decision
    /// 4a), so renewing an expired view is local and never refetches the inode.
    pub(super) async fn native_inode_for_read_async(
        &self,
        _handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
    ) -> PinnedReadView {
        if snapshot.native_read_generation == u64::MAX {
            return Err(unavailable("DFS native read generation exhausted"));
        }
        if let Some(view) = snapshot.native_read_view.as_ref()
            && view.deadline > Instant::now()
        {
            self.check_native_read_view(view)?;
            return Ok((snapshot.opened_inode.clone(), view.clone()));
        }
        let opened = snapshot.handle.clone();
        if opened.closed.load(Ordering::Acquire) {
            return Err(stale("DFS file handle is no longer open"));
        }
        let mut view = self.capture_native_read_view()?;
        let mut current = opened
            .state
            .lock()
            .map_err(|_| unavailable("DFS handle state is poisoned"))?;
        if current.native_denial_serial != snapshot.native_denial_serial {
            return Err(unavailable(
                "DFS native read authorization changed since the read started",
            ));
        }
        if let Some(installed) = current.native_read_view.as_ref()
            && installed.deadline > Instant::now()
        {
            let result = (current.opened_inode.clone(), installed.clone());
            drop(current);
            self.check_native_read_view(&result.1)?;
            return Ok(result);
        }
        current.native_read_generation = current
            .native_read_generation
            .checked_add(1)
            .ok_or_else(|| unavailable("DFS native read generation exhausted"))?;
        view.generation = current.native_read_generation;
        current.native_read_view = Some(view.clone());
        Ok((current.opened_inode.clone(), view))
    }

    pub(super) fn read_native_owned(
        &self,
        handle: FileHandle,
        snapshot: &DfsFileHandleSnapshot,
        offset: u64,
        size: usize,
    ) -> Result<prost::bytes::Bytes> {
        let mut result = None;
        let deadline = Instant::now() + self.read_engine.read_pipeline_timeout();
        self.read_engine.run_sync_boundary(Box::pin(async {
            result = Some(
                self.read_replicated_bytes_async(
                    handle,
                    snapshot,
                    offset,
                    size,
                    deadline,
                    Arc::new(()),
                )
                .await?,
            );
            Ok(())
        }))?;
        result.ok_or_else(|| unavailable("synchronous read boundary produced no result"))
    }

    pub(super) fn read_visible_from_handle_inode(
        &self,
        inode_id: &InodeId,
        opened_inode: Option<&InodeRecord>,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        if let Some(state) = self.write_state(inode_id)? {
            let view = {
                let state = state
                    .lock()
                    .map_err(|_| unavailable("DFS inode write state is poisoned"))?;
                // With no local activity left, a pinned handle reads its own
                // version below (4a).
                if !state.has_local_write_activity() && opened_inode.is_some() {
                    None
                } else {
                    Some(ResolvedReadView {
                        committed: state
                            .base_version
                            .as_ref()
                            .map(|version| version.id.clone()),
                        length: state.logical_length,
                        layout: state.base_layout.clone(),
                        frozen: state
                            .in_flight
                            .as_ref()
                            .and_then(InFlightCommit::dirty_extents),
                        active: state.dirty_extents.snapshot_range(offset, out.len())?,
                    })
                }
            };
            if let Some(view) = view {
                return self.read_visible_resolved(view, offset, out);
            }
        }

        if let Some(opened_inode) = opened_inode
            && let Some(count) = self.try_read_external_version(opened_inode, offset, out)?
        {
            return Ok(count);
        }

        let inode = self.validate_inode(
            tracing::info_span!(
                "node.dfs.read.head",
                inode_id = %inode_id.0,
                branch = "inode",
                cached = false,
            )
            .in_scope(|| self.meta.get_inode(inode_id))?,
        )?;
        self.read_committed_inode(&inode, offset, out)
    }

    fn read_committed_inode(
        &self,
        inode: &InodeRecord,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        let cached = self.readonly_version_for_inode(inode)?;
        let length = cached.layout.file_length;
        if offset >= length || out.is_empty() {
            return Ok(0);
        }
        let count = usize::try_from((length - offset).min(out.len() as u64))
            .map_err(|_| invalid("read length is too large"))?;
        out[..count].fill(0);
        let batch = cached.read_batch(offset, count, self.read_engine.source_lookahead_chunks())?;
        if self.read_engine.has_external_backend() {
            self.read_engine.read_batch(&batch, &mut out[..count])?;
        } else {
            self.read_engine
                .read_batch_native_into_private(&batch, &mut out[..count])?;
        }
        Ok(count)
    }

    #[cfg(test)]
    pub(super) fn read_committed_inode_owned(
        &self,
        inode: &InodeRecord,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        self.read_committed_inode_owned_hint(inode, offset, size, &mut None, None)
            .map(crate::node::read::buffer::ReadBuffer::into_vec)
    }

    #[cfg(test)]
    fn read_committed_inode_owned_hint(
        &self,
        inode: &InodeRecord,
        offset: u64,
        size: usize,
        hint: &mut Option<ReadBatch>,
        deadline: Option<Instant>,
    ) -> Result<crate::node::read::buffer::ReadBuffer> {
        let cached = self.readonly_version_for_inode(inode)?;
        let length = cached.layout.file_length;
        if offset >= length || size == 0 {
            return Ok(Vec::new().into());
        }
        let count = usize::try_from((length - offset).min(size as u64))
            .map_err(|_| invalid("read length is too large"))?;
        let batch = cached.read_batch(offset, count, self.read_engine.source_lookahead_chunks())?;
        if let Some(result) = self
            .read_engine
            .read_batch_native_open_owned_before(&batch, count, deadline)?
        {
            if self.read_engine.readahead_object_limit() != 0 {
                *hint = Some(batch);
            }
            return Ok(result);
        }
        let mut result = vec![0; count];
        self.read_engine
            .read_batch_native_open_into_private_before(&batch, &mut result, deadline)?;
        if self.read_engine.readahead_object_limit() != 0 {
            *hint = Some(batch);
        }
        Ok(result.into())
    }

    fn read_visible_resolved(
        &self,
        view: ResolvedReadView,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        if offset >= view.length || out.is_empty() {
            return Ok(0);
        }
        let count = usize::try_from((view.length - offset).min(out.len() as u64))
            .map_err(|_| invalid("read length is too large"))?;
        out[..count].fill(0);
        self.read_layout_range(
            view.committed.as_ref(),
            &view.layout,
            offset,
            &mut out[..count],
        )?;
        if let Some(frozen) = view.frozen {
            frozen.overlay_native(offset, &mut out[..count], Some(self.chunk_store.as_ref()))?;
        }
        view.active
            .overlay_native(offset, &mut out[..count], Some(self.chunk_store.as_ref()))?;
        Ok(count)
    }

    fn try_read_external_version(
        &self,
        opened_inode: &InodeRecord,
        offset: u64,
        out: &mut [u8],
    ) -> Result<Option<usize>> {
        let opened_inode = self.validate_inode(opened_inode.clone())?;
        if !self.read_engine.has_external_backend() {
            return Ok(None);
        }
        if opened_inode.head_version.is_none() {
            return Ok(None);
        }
        let cached = self.readonly_version_for_inode(&opened_inode)?;
        let Some(version) = cached.version.as_ref() else {
            return Ok(None);
        };
        let length = version.length;
        if offset >= length || out.is_empty() {
            return Ok(Some(0));
        }
        let count = usize::try_from((length - offset).min(out.len() as u64))
            .map_err(|_| invalid("read length is too large"))?;
        match self.read_layout_range_external_version(
            &opened_inode.inode_id,
            &version.id,
            &cached,
            offset,
            &mut out[..count],
        )? {
            CurrentExternalReadOutcome::Read => Ok(Some(count)),
            CurrentExternalReadOutcome::Unsupported => Ok(None),
            CurrentExternalReadOutcome::HeadChanged(_) => Err(invalid(
                "external version read must not use current-head refresh replies",
            )),
        }
    }

    pub(super) fn try_read_external_version_owned(
        &self,
        opened_inode: &InodeRecord,
        offset: u64,
        size: usize,
    ) -> Result<Option<Vec<u8>>> {
        let opened_inode = self.validate_inode(opened_inode.clone())?;
        if !self.read_engine.has_external_backend() {
            return Ok(None);
        }
        if opened_inode.head_version.is_none() {
            return Ok(None);
        }
        let cached = self.readonly_version_for_inode(&opened_inode)?;
        let Some(version) = cached.version.as_ref() else {
            return Ok(None);
        };
        let length = version.length;
        if offset >= length || size == 0 {
            return Ok(Some(Vec::new()));
        }
        let count = usize::try_from((length - offset).min(size as u64))
            .map_err(|_| invalid("read length is too large"))?;
        self.read_layout_range_external_version_owned(
            &opened_inode.inode_id,
            &version.id,
            &cached,
            offset,
            count,
        )
    }

    pub(super) fn layout_read_ops(
        &self,
        layout: &LayoutRoot,
        offset: u64,
        length: usize,
    ) -> Result<Vec<ChunkReadOp>> {
        let end = offset
            .checked_add(length as u64)
            .ok_or_else(|| invalid("read range overflow"))?;
        let mut ops = Vec::new();
        for extent in &layout.inline_extents {
            if let Some(op) = extent_read_op(extent, offset, end)? {
                ops.push(op);
            }
        }
        Ok(ops)
    }

    fn read_layout_range_external_version(
        &self,
        inode_id: &InodeId,
        committed: &FileVersionId,
        cached: &ReadOnlyVersion,
        offset: u64,
        out: &mut [u8],
    ) -> Result<CurrentExternalReadOutcome> {
        let ops = cached.ops(offset, out.len())?;
        let covered = ops
            .iter()
            .fold(0_u64, |sum, op| sum.saturating_add(op.length));
        if covered != out.len() as u64 || ops.is_empty() {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        }
        self.read_engine.read_batch_external_version(
            inode_id,
            &ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(committed.clone()),
                layout_root_id: cached.layout.id.clone(),
                ops,
            },
            out,
        )
    }

    fn read_layout_range_external_version_owned(
        &self,
        inode_id: &InodeId,
        committed: &FileVersionId,
        cached: &ReadOnlyVersion,
        offset: u64,
        length: usize,
    ) -> Result<Option<Vec<u8>>> {
        if length == 0 {
            return Ok(Some(Vec::new()));
        }
        let ops = cached.ops(offset, length)?;
        self.read_engine.read_batch_external_version_owned(
            inode_id,
            &ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(committed.clone()),
                layout_root_id: cached.layout.id.clone(),
                ops,
            },
            length,
        )
    }

    fn read_layout_range(
        &self,
        committed: Option<&FileVersionId>,
        layout: &LayoutRoot,
        offset: u64,
        out: &mut [u8],
    ) -> Result<()> {
        let ops = self.layout_read_ops(layout, offset, out.len())?;
        let end = offset
            .checked_add(out.len() as u64)
            .ok_or_else(|| invalid("read range overflow"))?;
        let batch = ReadBatch {
            lookahead: if committed.is_some() {
                source_lookahead_ids(
                    layout
                        .inline_extents
                        .iter()
                        .filter(|extent| extent.file_offset >= end),
                    &ops,
                    self.read_engine.source_lookahead_chunks(),
                )
            } else {
                Vec::new()
            },
            file_version_id: committed.cloned(),
            layout_root_id: layout.id.clone(),
            ops,
        };
        if self.read_engine.has_external_backend() {
            self.read_engine.read_batch(&batch, out)?;
        } else {
            // Every native caller supplies its unpublished final Vec. Avoid
            // allocating another aggregate stage before the outer checks.
            self.read_engine
                .read_batch_native_into_private(&batch, out)?;
        }
        Ok(())
    }

    pub(super) fn native_read_capability(&self, handle: FileHandle) -> Option<Arc<DfsHandle>> {
        if self.read_engine.has_external_backend() {
            return None;
        }
        let opened = self.handles.get(handle.0).ok()??;
        if opened.flags & libc::O_ACCMODE != libc::O_RDONLY
            || opened.flags & libc::O_PATH != 0
            || opened.owner_scope.is_some()
            || opened.closed.load(Ordering::Acquire)
            || opened.dynamic_read_route.load(Ordering::Acquire)
        {
            return None;
        }
        {
            let state = opened.state.lock().ok()?;
            if state.write_session.is_some() || state.opened_inode.kind != InodeKind::Regular {
                return None;
            }
            // A stale optimization view must be refreshed in the async route,
            // never select a worker-blocking network implementation.
            self.check_native_read_view(state.native_read_view.as_ref()?)
                .ok()?;
        }
        Some(opened)
    }

    pub(super) fn opened_page_identity(&self, inode: &InodeRecord) -> Result<PageCacheIdentity> {
        let cached = self.readonly_version_for_inode(inode)?;
        Ok(PageCacheIdentity {
            version_id: cached.version.as_ref().map(|version| version.id.0.clone()),
            length: cached.layout.file_length,
        })
    }

    pub(super) fn handle_snapshot(&self, handle: FileHandle) -> Result<DfsFileHandleSnapshot> {
        let handle = self
            .handles
            .get(handle.0)?
            .ok_or_else(|| stale("DFS file handle is no longer open"))?;
        DfsFileHandleSnapshot::from_handle(&handle)
    }
}
