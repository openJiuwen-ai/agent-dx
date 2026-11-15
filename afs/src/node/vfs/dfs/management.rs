//! File-level management operations using the ordinary committed DFS view.
use super::version_cache::ReadOnlyVersion;
use super::*;
use crate::dfs::EnsureReplicaPlacement;
use crate::node::read::engine::PrefetchChunk;
use std::ops::Range;

const RETRY_INTERVAL: Duration = Duration::from_millis(50);

impl DistributedFs {
    pub(crate) fn ensure_replicas(
        &self,
        handle: FileHandle,
        target_nodes: &[String],
        deadline: Instant,
    ) -> Result<()> {
        let snapshot = self.handle_snapshot(handle)?;
        let view = self.readonly_version_for_inode(&snapshot.opened_inode)?;
        if let Some(source) = lazy_fill::descriptor(&snapshot.opened_inode)?
            && !lazy_fill::covered(&view.layout, 0, source.length)
        {
            return Err(unavailable("lazy file has uncommitted replica ranges"));
        }
        let Some(version) = view.version.as_ref() else {
            return Ok(());
        };
        let chunks: BTreeSet<_> = view
            .layout
            .inline_extents
            .iter()
            .map(|extent| &extent.chunk_id)
            .collect();
        let mut pending = chunks
            .into_iter()
            .flat_map(|chunk| target_nodes.iter().map(move |target| (chunk, target)))
            .collect::<Vec<_>>();
        while !pending.is_empty() {
            let epoch = self.read_engine.capture_native_read_authority()?.epoch();
            let mut retry = Vec::new();
            for (chunk, target) in pending {
                check_management_deadline(deadline)?;
                let ready = crate::node::call_deadline::within(deadline, || {
                    self.meta.ensure_replica_placement(EnsureReplicaPlacement {
                        caller_id: self.identity.node_id.clone(),
                        caller_session_id: self.identity.session_id.clone(),
                        caller_node_epoch: epoch,
                        operation_id: self.identity.operation_id("place"),
                        file_version_id: version.id.clone(),
                        chunk_id: chunk.clone(),
                        target_node_id: target.clone(),
                    })
                })?;
                if !ready {
                    retry.push((chunk, target));
                }
            }
            pending = retry;
            if !pending.is_empty() {
                std::thread::sleep(
                    RETRY_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
                );
            }
        }
        Ok(())
    }

    /// Warm file ranges at the current node using the native DFS read cache.
    /// Every requested Chunk must be locally readable when the job completes.
    pub(crate) fn prefetch_ranges(
        &self,
        ctx: &RequestContext,
        files: &[(BackendInode, Vec<Range<u64>>)],
        deadline: Instant,
    ) -> Result<()> {
        let mut chunks = BTreeMap::new();
        for (inode, ranges) in files {
            check_management_deadline(deadline)?;
            crate::node::call_deadline::within(deadline, || {
                self.with_prefetch_handle(ctx, *inode, |handle| {
                    let snapshot = self.read_handle_snapshot(handle)?;
                    let (opened, view) = self.prefetch_view(&snapshot, ranges, deadline)?;
                    for range in ranges {
                        check_management_deadline(deadline)?;
                        let length = usize::try_from(range.end - range.start)
                            .map_err(|_| invalid("DFS prefetch range length overflow"))?;
                        for op in view.ops(range.start, length)? {
                            chunks
                                .entry(op.chunk_id.clone())
                                .or_insert_with(|| PrefetchChunk {
                                    batch: ReadBatch {
                                        file_version_id: view
                                            .version
                                            .as_ref()
                                            .map(|v| v.id.clone()),
                                        layout_root_id: view.layout.id.clone(),
                                        ops: vec![op],
                                        lookahead: Vec::new(),
                                    },
                                    pin: opened.clone(),
                                });
                        }
                    }
                    Ok(())
                })
            })?;
        }
        self.read_engine.run_sync_boundary(Box::pin(
            self.read_engine.prefetch_committed(chunks, deadline),
        ))
    }

    fn prefetch_view(
        &self,
        snapshot: &DfsFileHandleSnapshot,
        ranges: &[Range<u64>],
        deadline: Instant,
    ) -> Result<(Arc<OpenedView>, Arc<ReadOnlyVersion>)> {
        let mut opened = snapshot.opened_inode.clone();
        let mut view = self.readonly_version_for_inode(&opened)?;
        let source = lazy_fill::descriptor(&opened)?;
        for range in ranges {
            let length = source
                .as_ref()
                .map_or(view.layout.file_length, |source| source.length);
            if range.start > range.end || range.end > length {
                return Err(invalid("DFS prefetch range falls outside the file"));
            }
            if source.is_none() {
                continue; // Ordinary sparse ranges without extents need no cache.
            }
            while let Some((offset, count)) =
                lazy_fill::missing_range(&view.layout, range.start, range.end)
            {
                check_management_deadline(deadline)?;
                // Only genuinely uncommitted ranges use the legacy owner fill.
                // Committed data goes directly into the acknowledged Chunk path.
                self.lazy_read(snapshot, offset, count, deadline)?;
                let inode = self.validate_inode(self.meta.get_inode(&snapshot.inode_id)?)?;
                if lazy_fill::descriptor(&inode)? != source {
                    return Err(stale("lazy file source changed during prefetch"));
                }
                opened = self.protected_view(&inode)?;
                view = self.readonly_version_for_inode(&opened)?;
                if !lazy_fill::covered(&view.layout, offset, count as u64) {
                    return Err(unavailable("lazy prefetch range was not committed"));
                }
            }
        }
        Ok((opened, view))
    }

    fn with_prefetch_handle<T>(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        action: impl FnOnce(FileHandle) -> Result<T>,
    ) -> Result<T> {
        let handle = self.open(ctx, inode, libc::O_RDONLY)?;
        let outcome = action(handle);
        let released = self.release(ctx, handle);
        outcome.and_then(|result| released.map(|()| result))
    }
}

fn check_management_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(unavailable("DFS management operation expired"))
    } else {
        Ok(())
    }
}
