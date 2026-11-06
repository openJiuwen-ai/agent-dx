//! Native immutable metadata and reader invalidation shared by DFS recovery.
use super::*;

impl DfsReadEngine {
    pub(crate) fn recovery_sources(
        &self,
        version: crate::dfs::FileVersionId,
        layout: crate::dfs::LayoutRootId,
        chunk: ChunkId,
        deadline: Instant,
    ) -> Result<ChunkSources> {
        let request = DfsChunkSourcesRequest {
            caller_id: self.local_node_id.clone(),
            namespace_id: self.namespace_id.clone(),
            file_version_id: version,
            layout_root_id: layout,
            chunk_ids: vec![chunk.clone()],
        };
        let mut result = None;
        self.run_sync_boundary(Box::pin(async {
            let reply = tokio::time::timeout_at(
                deadline.into(),
                self.source_provider.sources_for_async(request),
            )
            .await
            .map_err(|_| unavailable("recovery metadata deadline exceeded"))??;
            let entry = reply
                .chunks
                .into_iter()
                .find(|entry| entry.chunk_id == chunk)
                .ok_or_else(|| invalid("recovery metadata omitted requested chunk"))?;
            let object = entry
                .chunk
                .as_ref()
                .ok_or_else(|| invalid("recovery requires immutable chunk descriptor"))?;
            if object.id != chunk
                || object.length == 0
                || object.length > super::super::chunk::MAX_STAGED_CHUNK_BYTES as u64
                || !range_covered(&entry.allowed_ranges, 0, object.length)
            {
                return Err(invalid(
                    "recovery requires bounded, fully referenced immutable bytes",
                ));
            }
            result = Some(entry);
            Ok(())
        }))?;
        result.ok_or_else(|| unavailable("recovery metadata missing"))
    }

    pub(crate) fn invalidate_recovered_chunk(&self, id: &ChunkId) -> Result<()> {
        self.readers
            .lock()
            .map_err(|_| unavailable("reader cache poisoned"))?
            .remove(id);
        self.source_cache
            .lock()
            .map_err(|_| unavailable("source cache poisoned"))?
            .retain(|_, _| false);
        Ok(())
    }
}

impl DfsReadEngine {
    /// A verified physical restore can outlive a lost Meta acknowledgement.
    /// Derive retry work from the ordinary durable catalog and current file
    /// mapping, including after restart; do not create another recovery journal.
    pub(crate) fn has_unpublished_local_copy(
        &self,
        batch: &ReadBatch,
        deadline: Instant,
    ) -> Result<bool> {
        for id in batch
            .ops
            .iter()
            .map(|op| &op.chunk_id)
            .collect::<BTreeSet<_>>()
        {
            if !self.local.has_durable_record(id)? {
                continue;
            }
            let sources = self.recovery_sources(
                batch
                    .file_version_id
                    .clone()
                    .ok_or_else(|| invalid("restore requires fixed version"))?,
                batch.layout_root_id.clone(),
                id.clone(),
                deadline,
            )?;
            if !sources.sources.iter().any(|s| matches!(&s.location, CopyLocation::Node { node_id, .. } if node_id == &self.local_node_id)) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
