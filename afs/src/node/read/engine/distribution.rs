//! Bounded range scheduling for ordinary DFS files. Plans retain one immutable
//! version and its existing grants; no source or application-specific IO lives here.
use super::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

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
    /// D3: the one candidate order of every native remote read, at any
    /// `parallelism`. Cache holders first and durable copies as the fallback
    /// when a verified cache copy is offered (the writer's egress is the only
    /// source nobody can replace); then the advertised load; then a
    /// rendezvous hash of (version, Chunk, holder) so each Chunk has one
    /// stable first choice and different Chunks spread over the holders.
    /// Only the order changes: candidates, grants and versions do not.
    pub(super) fn distributed_sources(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
    ) -> Vec<ChunkSources> {
        let mut result = sources.to_vec();
        for set in &mut result {
            // Keep an all-stale list as it was: the holder's refusal then takes
            // the usual reauthorization path instead of "no readable copy".
            if set.sources.iter().any(|s| self.source_matches(batch, s)) {
                set.sources.retain(|s| self.source_matches(batch, s));
            }
            self.order_candidates(batch, set);
        }
        result
    }

    /// Orders one Chunk's candidates in place; see [`Self::distributed_sources`].
    pub(super) fn order_candidates(&self, batch: &ReadBatch, set: &mut ChunkSources) {
        let demoted = self.demoted_now();
        let is_demoted = |s: &SourceCandidate| demoted.contains(&s.copy_id);
        let cached = set.sources.iter().any(|s| {
            self.is_remote_candidate(s) && s.role == CopyRole::VerifiedCache && !is_demoted(s)
        });
        let version = batch
            .file_version_id
            .as_ref()
            .map_or("", |version| version.0.as_str());
        set.sources.sort_by_cached_key(|source| {
            let remote = self.is_remote_candidate(source);
            let fallback = cached && source.role != CopyRole::VerifiedCache;
            // H2 step 2: version holders after the published copies, before
            // the durable ones.
            let holder = source.grant.is_holder();
            let node = match &source.location {
                CopyLocation::Node { node_id, .. } => node_id.as_str(),
                CopyLocation::External { .. } => "",
            };
            (
                !remote,
                is_demoted(source),
                fallback,
                holder,
                source.load_hint,
                std::cmp::Reverse(rendezvous_weight(version, &set.chunk_id, node)),
            )
        });
    }

    /// Remote Nodes that striping may use for one Chunk: the cache holders
    /// when any is offered, otherwise every remote holder (D3-1).
    fn striping_nodes<'s>(&self, batch: &ReadBatch, set: &'s ChunkSources) -> BTreeSet<&'s String> {
        let demoted = self.demoted_now();
        // A version holder may answer NOT_READY: never a stripe.
        let usable = |s: &&SourceCandidate| {
            self.is_remote_candidate(s)
                && !s.grant.is_holder()
                && self.source_matches(batch, s)
                && !demoted.contains(&s.copy_id)
        };
        let cached = set
            .sources
            .iter()
            .filter(usable)
            .any(|s| s.role == CopyRole::VerifiedCache);
        set.sources
            .iter()
            .filter(usable)
            .filter(|s| !cached || s.role == CopyRole::VerifiedCache)
            .filter_map(|s| match &s.location {
                CopyLocation::Node { node_id, .. } => Some(node_id),
                CopyLocation::External { .. } => None,
            })
            .collect()
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
        if self.distribution.parallelism <= 1 || !self.async_read_enabled() {
            return Ok(false);
        }
        let Some(version) = batch.file_version_id.as_ref() else {
            return Ok(false);
        };
        let mut ops = Vec::new();
        let mut available_peers = BTreeSet::new();
        for op in &batch.ops {
            let Some(set) = sources.iter().find(|s| s.chunk_id == op.chunk_id) else {
                return Ok(false);
            };
            let peers = self.striping_nodes(batch, set);
            let stripe = if peers.len() > 1 {
                self.distribution.stripe_bytes as u64
            } else {
                op.length.max(1)
            };
            available_peers.extend(peers);
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
        if ops.len() < 2 || available_peers.len() < 2 {
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
                let nodes: BTreeSet<String> = self
                    .striping_nodes(&child, &candidates)
                    .into_iter()
                    .cloned()
                    .collect();
                if nodes.len() > 1
                    && let Some(node) = nodes.iter().nth(index % nodes.len())
                {
                    candidates.sources.sort_by_key(|source| {
                        !matches!(&source.location, CopyLocation::Node { node_id, .. } if node_id == node)
                    });
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
            .buffer_unordered(self.distribution.parallelism)
            .collect()
            .await;
        for result in results {
            result?;
        }
        self.check_native_read_authority(&authority)?;
        Ok(true)
    }
}

/// How long a cache copy that failed a read here is ordered last.
pub(super) const DEMOTE_FOR: Duration = Duration::from_secs(5);
const MAX_DEMOTED: usize = 1024;

impl DfsReadEngine {
    /// Cache copies currently demoted; expired ones are dropped.
    pub(super) fn demoted_now(&self) -> BTreeSet<crate::dfs::CopyId> {
        let Ok(mut demoted) = self.demoted_copies.lock() else {
            return BTreeSet::new();
        };
        let now = Instant::now();
        demoted.retain(|_, until| *until > now);
        demoted.keys().cloned().collect()
    }

    /// A cache copy failed a read (not backpressure, not a denial): order
    /// it last for [`DEMOTE_FOR`]. Durable copies are never demoted.
    pub(super) fn demote_cache_copy(&self, source: &SourceCandidate) {
        if source.role != CopyRole::VerifiedCache {
            return;
        }
        let Ok(mut demoted) = self.demoted_copies.lock() else {
            return;
        };
        let now = Instant::now();
        demoted.retain(|_, until| *until > now);
        if demoted.len() < MAX_DEMOTED || demoted.contains_key(&source.copy_id) {
            demoted.insert(source.copy_id.clone(), now + DEMOTE_FOR);
            self.local.record_read_cache_event("holder_demoted");
        }
    }
}

/// Highest-random-weight score of one holder for one Chunk of one version.
pub(super) fn rendezvous_weight(version: &str, chunk: &ChunkId, node: &str) -> u64 {
    let mut hasher = blake3::Hasher::new();
    for part in [version.as_bytes(), chunk.0.as_bytes(), node.as_bytes()] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    let mut weight = [0_u8; 8];
    weight.copy_from_slice(&hasher.finalize().as_bytes()[..8]);
    u64::from_le_bytes(weight)
}
