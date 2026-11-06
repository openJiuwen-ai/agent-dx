//! Bounded range scheduling for ordinary DFS files. Plans retain one immutable
//! version and its existing grants; no source or application-specific IO lives here.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReadDistributionConfig {
    /// One preserves sequential reads; 2..=8 permits independent source reads.
    pub parallelism: usize,
    /// Split large requested ranges, never fetch unrequested neighbours.
    pub stripe_bytes: usize,
}

impl Default for ReadDistributionConfig {
    fn default() -> Self {
        Self {
            parallelism: 1,
            stripe_bytes: 256 * 1024,
        }
    }
}

impl ReadDistributionConfig {
    pub fn validate(&self) -> Result<()> {
        if !(1..=8).contains(&self.parallelism)
            || !(64 * 1024..=MAX_DFS_READ_BYTES as usize).contains(&self.stripe_bytes)
            || !self.stripe_bytes.is_multiple_of(64 * 1024)
        {
            return Err(invalid(
                "read distribution requires parallelism 1..=8 and stripe_bytes 64KiB..=8MiB in 64KiB units",
            ));
        }
        Ok(())
    }
}

impl DfsReadEngine {
    /// Rotate equal-load peers, spreading requests and stripes without retaining
    /// an unbounded per-peer history. Node salt avoids identical reader schedules.
    pub(super) fn distributed_sources(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
    ) -> Vec<ChunkSources> {
        let mut result = sources.to_vec();
        if self.config.distribution.parallelism == 1 {
            return result;
        }
        let turn = self.source_turn.fetch_add(1, Ordering::Relaxed);
        let salt = self
            .local_node_id
            .bytes()
            .fold(0usize, |a, b| a.wrapping_mul(31).wrapping_add(b as usize));
        for (index, set) in result.iter_mut().enumerate() {
            set.sources.retain(|s| self.source_matches(batch, s));
            if !set.sources.is_empty() {
                let offset = turn.wrapping_add(salt).wrapping_add(index) % set.sources.len();
                set.sources.rotate_left(offset);
                set.sources.sort_by_key(|s| s.load_hint);
            }
        }
        result
    }

    /// The caller owns unpublished output. Disjoint slices avoid another body
    /// allocation; all children finish before returning, including after failure.
    pub(super) async fn read_distributed_ranges(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        deadline: Option<Instant>,
    ) -> Result<bool> {
        use futures_util::{StreamExt, stream};
        if self.config.distribution.parallelism <= 1 || !self.async_read_enabled() {
            return Ok(false);
        }
        let Some(version) = batch.file_version_id.as_ref() else {
            return Ok(false);
        };
        let mut ops = Vec::new();
        for op in &batch.ops {
            let Some(set) = sources.iter().find(|s| s.chunk_id == op.chunk_id) else {
                return Ok(false);
            };
            let peers: BTreeSet<_> = set
                .sources
                .iter()
                .filter(|s| self.source_matches(batch, s))
                .filter_map(|s| match &s.location {
                    CopyLocation::Node { node_id, .. }
                        if node_id != &self.local_node_id && s.state.is_readable() =>
                    {
                        Some(node_id)
                    }
                    _ => None,
                })
                .collect();
            let stripe = if peers.len() > 1 {
                self.config.distribution.stripe_bytes as u64
            } else {
                op.length.max(1)
            };
            let mut done = 0u64;
            while done < op.length {
                if ops.len() == MAX_DFS_READ_OPS {
                    return Ok(false);
                }
                let length = (op.length - done).min(stripe);
                ops.push(ChunkReadOp {
                    chunk_id: op.chunk_id.clone(),
                    chunk_offset: op
                        .chunk_offset
                        .checked_add(done)
                        .ok_or_else(|| invalid("distributed chunk offset overflow"))?,
                    output_offset: op
                        .output_offset
                        .checked_add(
                            usize::try_from(done)
                                .map_err(|_| invalid("distributed output overflow"))?,
                        )
                        .ok_or_else(|| invalid("distributed output overflow"))?,
                    length,
                });
                done += length;
            }
        }
        if ops.len() < 2 {
            return Ok(false);
        }
        ops.sort_by_key(|op| op.output_offset);
        let mut end = 0usize;
        for op in &ops {
            // Preserve the old scheduler's semantics for overlapping scatter.
            if op.output_offset < end {
                return Ok(false);
            }
            end = op
                .output_offset
                .checked_add(
                    usize::try_from(op.length)
                        .map_err(|_| invalid("distributed length overflow"))?,
                )
                .ok_or_else(|| invalid("distributed range overflow"))?;
            if end > out.len() {
                return Err(invalid("distributed range exceeds output"));
            }
        }
        let deadline = deadline.unwrap_or_else(|| Instant::now() + self.read_pipeline_timeout());
        let authority = self.capture_native_read_authority()?;
        let ordered = self.distributed_sources(batch, sources);
        let mut tail = out;
        let mut cursor = 0usize;
        let mut children = Vec::with_capacity(ops.len());
        for (index, mut op) in ops.into_iter().enumerate() {
            let (_, rest) = tail.split_at_mut(op.output_offset - cursor);
            let (target, rest) = rest.split_at_mut(op.length as usize);
            cursor = op.output_offset + op.length as usize;
            tail = rest;
            op.output_offset = 0;
            let candidates = ordered
                .iter()
                .find(|s| s.chunk_id == op.chunk_id)
                .ok_or_else(|| invalid("distributed source disappeared"))?;
            let child = ReadBatch {
                file_version_id: Some(version.clone()),
                layout_root_id: batch.layout_root_id.clone(),
                ops: vec![op],
                lookahead: Vec::new(),
            };
            children.push(async move {
                // Clone only an admitted child's metadata, not every queued stripe.
                let mut candidates = candidates.clone();
                if !candidates.sources.is_empty() {
                    let n = candidates.sources.len();
                    candidates.sources.rotate_left(index % n);
                    candidates.sources.sort_by_key(|s| s.load_hint);
                }
                tokio::time::timeout_at(
                    tokio::time::Instant::from_std(deadline),
                    self.read_remote_ranges_async(
                        &child,
                        &[candidates],
                        target,
                        true,
                        Some(deadline),
                    ),
                )
                .await
                .map_err(|_| unavailable("distributed read deadline exceeded"))?
            });
        }
        let results: Vec<Result<()>> = stream::iter(children)
            .buffer_unordered(self.config.distribution.parallelism)
            .collect()
            .await;
        for result in results {
            result?;
        }
        self.check_native_read_authority(&authority)?;
        Ok(true)
    }
}
