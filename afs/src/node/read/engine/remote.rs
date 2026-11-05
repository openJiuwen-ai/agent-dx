//! `PeerRangeSource`: verified peer ranges with candidate rotation.
//!
//! Each operation walks its own candidate list in Meta order. Operations on
//! the same peer context share one batch attempt; a failed batch retries each
//! operation once alone on the same copy before that operation advances.

use super::*;

/// The fixed inputs of one rotation over a batch.
struct PeerRangeRound<'r> {
    batch: &'r ReadBatch,
    sources: &'r [ChunkSources],
    candidates: &'r [Vec<SourceCandidate>],
    version: &'r FileVersionId,
    final_owned: bool,
}

/// Rotation state: next candidate per operation, operations still to read
/// in the next pass, and the error returned once a list is exhausted.
struct PeerRangeProgress {
    next: Vec<usize>,
    pending: Vec<usize>,
    last_error: Option<Error>,
}

impl DfsReadEngine {
    pub(super) async fn read_remote_ranges_async(
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
        let candidates: Vec<Vec<SourceCandidate>> = batch
            .ops
            .iter()
            .map(|op| {
                sources
                    .iter()
                    .find(|set| set.chunk_id == op.chunk_id)
                    .map(|set| {
                        set.sources
                            .iter()
                            .filter(|source| self.is_remote_candidate(source))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default()
            })
            .collect();
        let round = PeerRangeRound {
            batch,
            sources,
            candidates: &candidates,
            version: &version,
            final_owned,
        };
        let mut progress = PeerRangeProgress {
            next: vec![0; batch.ops.len()],
            pending: (0..batch.ops.len()).collect(),
            last_error: None,
        };
        while !progress.pending.is_empty() {
            let groups = Self::group_pending_ranges(&round, &mut progress)?;
            progress.pending.clear();
            for group in groups {
                self.read_range_group(&round, &group, out, &mut progress)
                    .await?;
            }
        }
        Ok(())
    }

    /// Group pending operations by peer context. CRC-segment objects are
    /// always read alone. An operation without a next candidate ends the read.
    fn group_pending_ranges(
        round: &PeerRangeRound<'_>,
        progress: &mut PeerRangeProgress,
    ) -> Result<Vec<Vec<usize>>> {
        let (batch, candidates, next) = (round.batch, round.candidates, &progress.next);
        let mut groups: Vec<Vec<usize>> = Vec::new();
        for &index in &progress.pending {
            let Some(source) = candidates[index].get(next[index]) else {
                return Err(progress
                    .last_error
                    .take()
                    .unwrap_or_else(|| corrupt("committed Chunk has no readable copy")));
            };
            if let Some(group) = groups.iter_mut().find(|group| {
                let first = group[0];
                !round.sources.iter().any(|set| {
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
        Ok(groups)
    }

    /// One batch attempt for a group on each operation's current candidate.
    async fn read_range_group(
        &self,
        round: &PeerRangeRound<'_>,
        group: &[usize],
        out: &mut [u8],
        progress: &mut PeerRangeProgress,
    ) -> Result<()> {
        let batch = round.batch;
        let mut length = 0;
        let operations = group
            .iter()
            .map(|&index| {
                let mut op = batch.ops[index].clone();
                op.output_offset = length;
                length += op.length as usize; // validated by ReadBatch
                ResolvedReadOp {
                    op,
                    source: round.candidates[index][progress.next[index]].clone(),
                }
            })
            .collect();
        let attempt = PeerReadBatch {
            file_version_id: round.version.clone(),
            layout_root_id: batch.layout_root_id.clone(),
            operations,
        };
        // Only the private, single contiguous final response may be
        // used directly. Borrowed output and complex batches retain
        // per-attempt isolation and the existing retry scheduler.
        let direct = round.final_owned
            && batch.ops.len() == 1
            && group.len() == 1
            && batch.ops[0].output_offset == 0
            && length == out.len();
        let mut scratch = if direct { Vec::new() } else { vec![0; length] };
        let integrity = round
            .sources
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
                    return Ok(());
                }
                for (&index, item) in group.iter().zip(&attempt.operations) {
                    let original = &batch.ops[index];
                    let len = original.length as usize;
                    self.local
                        .record_read_copy("attempt_to_staging", original.length);
                    out[original.output_offset..original.output_offset + len].copy_from_slice(
                        &scratch[item.op.output_offset..item.op.output_offset + len],
                    );
                }
                Ok(())
            }
            Err(error) => {
                self.invalidate_native_batch_on_error(batch, &error);
                if crate::node::fault::classify::read_authority_rejected(&error) {
                    return Err(error);
                }
                self.capture_native_read_authority()?;
                progress.last_error = Some(error);
                self.retry_group_singly(round, group, &attempt, out, progress)
                    .await
            }
        }
    }

    /// A batch error does not identify which copy failed. Before discarding
    /// a valid source for its neighbours, retry each operation on that same
    /// source once with a fresh scratch. Each candidate gets at most one
    /// batch and one single-op attempt; failed singles advance exactly once.
    async fn retry_group_singly(
        &self,
        round: &PeerRangeRound<'_>,
        group: &[usize],
        attempt: &PeerReadBatch,
        out: &mut [u8],
        progress: &mut PeerRangeProgress,
    ) -> Result<()> {
        let batch = round.batch;
        for (index, item) in group.iter().copied().zip(&attempt.operations) {
            if group.len() > 1 {
                let mut single = item.clone();
                single.op.output_offset = 0;
                let single_batch = PeerReadBatch {
                    file_version_id: round.version.clone(),
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
                        self.local
                            .record_read_copy("attempt_to_staging", original.length);
                        out[original.output_offset..original.output_offset + single_scratch.len()]
                            .copy_from_slice(&single_scratch);
                        continue;
                    }
                    Err(error) => {
                        self.invalidate_native_batch_on_error(batch, &error);
                        if crate::node::fault::classify::read_authority_rejected(&error) {
                            return Err(error);
                        }
                        self.capture_native_read_authority()?;
                        progress.last_error = Some(error);
                    }
                }
            }
            progress.next[index] += 1;
            progress.pending.push(index);
        }
        Ok(())
    }
}
