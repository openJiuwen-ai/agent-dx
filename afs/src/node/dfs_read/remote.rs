//! Remote read routing and candidate rotation.

use super::*;

impl DfsReadEngine {
    pub(super) async fn read_remote_batch_async(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        final_owned: bool,
        deadline: Option<Instant>,
    ) -> Result<()> {
        if final_owned
            && self
                .fanout_private_objects_async(batch, sources, out, deadline)
                .await?
        {
            return Ok(());
        }
        if self
            .read_native_peer_cache_mode_async(batch, sources, out, final_owned, false)
            .await?
        {
            return Ok(());
        }
        self.read_remote_ranges_async(batch, sources, out, final_owned)
            .await
    }

    async fn read_remote_ranges_async(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        final_owned: bool,
    ) -> Result<()> {
        let version = batch
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("DFS peer read requires a fixed FileVersion"))?;
        let candidates: Vec<Vec<SourceCandidate>> = batch.ops.iter().map(|op| {
            sources.iter().find(|set| set.chunk_id == op.chunk_id)
                .map(|set| set.sources.iter().filter(|source| {
                    source.state.is_readable()
                        && matches!(source.role, CopyRole::DurableReplica | CopyRole::VerifiedCache)
                        && matches!(&source.location, CopyLocation::Node { node_id, .. } if node_id != &self.local_node_id)
                }).cloned().collect()).unwrap_or_default()
        }).collect();
        let mut next = vec![0; batch.ops.len()];
        let mut pending: Vec<usize> = (0..batch.ops.len()).collect();
        let mut last_error = None;
        while !pending.is_empty() {
            let mut groups: Vec<Vec<usize>> = Vec::new();
            for &index in &pending {
                let Some(source) = candidates[index].get(next[index]) else {
                    return Err(last_error
                        .unwrap_or_else(|| corrupt("committed Chunk has no readable copy")));
                };
                if let Some(group) = groups.iter_mut().find(|group| {
                    let first = group[0];
                    !sources.iter().any(|set| {
                        (set.chunk_id == batch.ops[index].chunk_id
                            || set.chunk_id == batch.ops[first].chunk_id)
                            && set
                                .chunk
                                .as_ref()
                                .is_some_and(ChunkObject::is_segment_crc32c)
                    }) && same_peer_context(&candidates[first][next[first]], source)
                }) {
                    group.push(index);
                } else {
                    groups.push(vec![index]);
                }
            }
            pending.clear();
            for group in groups {
                let mut length = 0;
                let operations = group
                    .iter()
                    .map(|&index| {
                        let mut op = batch.ops[index].clone();
                        op.output_offset = length;
                        length += op.length as usize; // validated by ReadBatch
                        ResolvedReadOp {
                            op,
                            source: candidates[index][next[index]].clone(),
                        }
                    })
                    .collect();
                let attempt = PeerReadBatch {
                    file_version_id: version.clone(),
                    layout_root_id: batch.layout_root_id.clone(),
                    operations,
                };
                // Only the private, single contiguous final response may be
                // used directly. Borrowed output and complex batches retain
                // per-attempt isolation and the existing retry scheduler.
                let direct = final_owned
                    && batch.ops.len() == 1
                    && group.len() == 1
                    && batch.ops[0].output_offset == 0
                    && length == out.len();
                let mut scratch = if direct { Vec::new() } else { vec![0; length] };
                let integrity = sources
                    .iter()
                    .find(|set| set.chunk_id == batch.ops[group[0]].chunk_id)
                    .and_then(|set| set.chunk.as_ref())
                    .filter(|chunk| chunk.is_segment_crc32c());
                let target = if direct { &mut *out } else { &mut scratch };
                let result = if let Some(chunk) = integrity {
                    self.transfer
                        .read_crc32c_range_async(&attempt, chunk, target)
                        .await
                } else {
                    self.transfer.read_ranges_async(&attempt, target).await
                };
                match result {
                    Ok(()) => {
                        if attempt
                            .operations
                            .iter()
                            .any(|item| !self.source_matches(batch, &item.source))
                        {
                            return Err(external_denied(
                                "native peer read authority expired before publication",
                            ));
                        }
                        if direct {
                            continue;
                        }
                        for (&index, item) in group.iter().zip(&attempt.operations) {
                            let original = &batch.ops[index];
                            let len = original.length as usize;
                            self.local
                                .record_read_copy("attempt_to_staging", original.length);
                            out[original.output_offset..original.output_offset + len]
                                .copy_from_slice(
                                    &scratch[item.op.output_offset..item.op.output_offset + len],
                                );
                        }
                    }
                    Err(error) => {
                        self.invalidate_native_batch_on_error(batch, &error);
                        if crate::node::fault::classify::read_authority_rejected(&error) {
                            return Err(error);
                        }
                        self.capture_native_read_authority()?;
                        last_error = Some(error);
                        // A batch error does not identify which copy failed. Before
                        // discarding a valid source for its neighbours, retry each
                        // operation on that same source once with a fresh scratch.
                        // Each candidate gets at most one batch and one single-op
                        // attempt; failed singles advance exactly once.
                        for (index, item) in group.iter().copied().zip(&attempt.operations) {
                            if group.len() > 1 {
                                let mut single = item.clone();
                                single.op.output_offset = 0;
                                let single_batch = PeerReadBatch {
                                    file_version_id: version.clone(),
                                    layout_root_id: batch.layout_root_id.clone(),
                                    operations: vec![single],
                                };
                                let mut single_scratch = vec![0; item.op.length as usize];
                                match self
                                    .transfer
                                    .read_ranges_async(&single_batch, &mut single_scratch)
                                    .await
                                {
                                    Ok(()) => {
                                        if !self.source_matches(batch, &item.source) {
                                            return Err(external_denied(
                                                "native peer read authority expired before publication",
                                            ));
                                        }
                                        let original = &batch.ops[index];
                                        self.local.record_read_copy(
                                            "attempt_to_staging",
                                            original.length,
                                        );
                                        out[original.output_offset
                                            ..original.output_offset + single_scratch.len()]
                                            .copy_from_slice(&single_scratch);
                                        continue;
                                    }
                                    Err(error) => {
                                        self.invalidate_native_batch_on_error(batch, &error);
                                        if crate::node::fault::classify::read_authority_rejected(
                                            &error,
                                        ) {
                                            return Err(error);
                                        }
                                        self.capture_native_read_authority()?;
                                        last_error = Some(error);
                                    }
                                }
                            }
                            next[index] += 1;
                            pending.push(index);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
