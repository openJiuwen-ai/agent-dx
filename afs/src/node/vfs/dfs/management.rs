//! File-level management operations using the ordinary committed DFS view.
use super::*;
use crate::dfs::{ChunkId, EnsureReplicaPlacement};
use std::ops::Range;

const READ_SLICE: u64 = 256 * 1024;
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
        let Some(version) = view.version.as_ref() else { return Ok(()); };
        let chunks: BTreeSet<_> = view.layout.inline_extents.iter()
            .map(|extent| &extent.chunk_id).collect();
        let mut pending = chunks.into_iter().flat_map(|chunk| {
            target_nodes.iter().map(move |target| (chunk, target))
        }).collect::<Vec<_>>();
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
                std::thread::sleep(RETRY_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
            }
        }
        Ok(())
    }

    pub(crate) fn prefetch_ranges(
        &self,
        ctx: &RequestContext,
        inode: BackendInode,
        ranges: &[Range<u64>],
        deadline: Instant,
    ) -> Result<BTreeSet<ChunkId>> {
        // Ordinary reads first fill missing lazy ranges, keeping Registry
        // interpretation in the connector and existing owner hook.
        self.with_prefetch_handle(ctx, inode, |handle| {
            for range in ranges {
                self.read_management_range(ctx, handle, range.clone(), deadline)?;
            }
            Ok(())
        })?;
        self.with_prefetch_handle(ctx, inode, |handle| {
            let snapshot = self.handle_snapshot(handle)?;
            let view = self.readonly_version_for_inode(&snapshot.opened_inode)?;
            let chunks = view.layout.inline_extents.iter()
                .filter(|extent| {
                    ranges.iter().any(|range| overlaps(extent, range))
                })
                .map(|extent| extent.chunk_id.clone()).collect::<BTreeSet<_>>();

            // The native disk cache installs a verified Chunk. Read its mapped
            // extents to complete the object when a requested range was partial.
            let missing = chunks.iter().filter(|chunk| !self.read_engine.has_local_chunk(chunk))
                .cloned().collect::<BTreeSet<_>>();
            for extent in &view.layout.inline_extents {
                if missing.contains(&extent.chunk_id) {
                    self.read_management_range(
                        ctx, handle, extent.file_offset..extent.file_offset + extent.length, deadline,
                    )?;
                }
            }
            loop {
                check_management_deadline(deadline)?;
                if self.verify_prefetched_chunks(&chunks) {
                    return Ok(chunks);
                }
                std::thread::sleep(RETRY_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
            }
        })
    }

    pub(crate) fn verify_prefetched_chunks(&self, chunks: &BTreeSet<ChunkId>) -> bool {
        chunks.iter().all(|chunk| self.read_engine.has_local_chunk(chunk))
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

    fn read_management_range(
        &self,
        ctx: &RequestContext,
        handle: FileHandle,
        range: Range<u64>,
        deadline: Instant,
    ) -> Result<()> {
        let mut offset = range.start;
        while offset < range.end {
            check_management_deadline(deadline)?;
            let count = (range.end - offset).min(READ_SLICE) as usize;
            let data = crate::node::call_deadline::within(deadline, || {
                self.read_reply(ctx, handle, offset, count)
            })?;
            if data.len() != count {
                return Err(invalid("short read during DFS range prefetch"));
            }
            offset += count as u64;
        }
        Ok(())
    }
}

fn overlaps(extent: &Extent, range: &Range<u64>) -> bool {
    extent.file_offset < range.end && range.start < extent.file_offset.saturating_add(extent.length)
}

fn check_management_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(unavailable("DFS management operation expired"))
    } else {
        Ok(())
    }
}
