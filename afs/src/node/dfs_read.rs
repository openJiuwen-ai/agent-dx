//! DFS fixed-version Chunk read path.
//!
//! DistributedFs resolves a file range into immutable Chunk ranges, then this
//! module chooses local or peer sources, or an explicitly configured external
//! backend. All paths publish into the caller buffer only after the whole
//! requested batch succeeds. External fixed-version reads reuse bounded
//! authenticated contexts; full immutable-object verification remains required.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    hash::Hash,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub use super::dfs_fetch::PrefetchControl;

use afs_error::{Error, Result};
use afs_tracing::tracing;

use crate::{
    dfs::{
        AuthorizeCurrentExternalRead, AuthorizeCurrentExternalReadReply,
        AuthorizeExternalVersionRead, AuthorizeExternalVersionReadReply, ChunkId, ChunkObject,
        ChunkSources, CopyLocation, CopyRole, CopyState, DfsChunkSourcesReply,
        DfsChunkSourcesRequest, DfsExternalAuthorizedRead, DfsMissingReadSource, DfsReadGrant,
        DfsReadValidation, FileVersionId, LayoutRootId, SourceCandidate,
    },
    node::{
        chunk::{LocalChunkStore, PinnedChunkReader, VerifiedRangeCopy, outboard::OutboardBudget},
        peer_cache::{CompletedPeerDiskRead, DiskPeerCache, PeerStreamSink, VerifiedPeerChunk},
        read_work::PeerFillClock,
    },
};

/// Hard protocol budgets shared by the scheduler, peer adapter and service.
pub const MAX_DFS_READ_OPS: usize = 128;
pub const MAX_DFS_READ_BYTES: u64 = 8 * 1024 * 1024;
/// External objects are fetched and verified one at a time, independently of
/// the requested range budget. This matches the producer's staged chunk limit.
pub const MAX_DFS_EXTERNAL_CHUNK_BYTES: u64 = crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64;
const MAX_EXTERNAL_AUTH_CACHE_ENTRIES: usize = 128;
const MAX_EXTERNAL_AUTH_CACHE_BYTES: usize = 1024 * 1024;
const MAX_SOURCE_CACHE_BYTES: usize = 4 * 1024 * 1024;
const SOURCE_GRANT_SAFETY_MS: u64 = 1_000;
pub(super) const MAX_SOURCE_LOOKAHEAD_ID_BYTES: usize = 512;

#[derive(Clone, Debug)]
pub struct DfsReadConfig {
    pub max_ops_per_batch: usize,
    pub max_inflight_bytes: u64,
    pub source_cache_ttl: Duration,
    pub source_lookahead_chunks: usize,
    pub source_cache_bytes: usize,
    /// Object scheduling distance, not additional resident or transport bytes.
    pub readahead_max_bytes: u64,
}

impl Default for DfsReadConfig {
    fn default() -> Self {
        Self {
            max_ops_per_batch: MAX_DFS_READ_OPS,
            max_inflight_bytes: MAX_DFS_READ_BYTES,
            source_cache_ttl: Duration::from_millis(5_000),
            source_lookahead_chunks: 64,
            source_cache_bytes: MAX_SOURCE_CACHE_BYTES,
            readahead_max_bytes: 0,
        }
    }
}

impl DfsReadConfig {
    /// Reject unsupported configuration before it becomes a peer-only failure.
    pub fn validate(&self) -> Result<()> {
        if self.max_ops_per_batch == 0
            || self.max_ops_per_batch > MAX_DFS_READ_OPS
            || self.max_inflight_bytes == 0
            || self.max_inflight_bytes > MAX_DFS_READ_BYTES
            || self.source_lookahead_chunks > MAX_DFS_READ_OPS
            || self.source_cache_bytes > MAX_SOURCE_CACHE_BYTES
            || self.readahead_max_bytes > 32 * 1024 * 1024
            || self.source_cache_ttl > Duration::from_millis(60_000)
        {
            return Err(invalid(format!(
                "DFS read configuration requires 1..={MAX_DFS_READ_OPS} operations, 1..={MAX_DFS_READ_BYTES} bytes, lookahead <= {MAX_DFS_READ_OPS}, source cache <= {MAX_SOURCE_CACHE_BYTES} bytes and maximum TTL <= 60000 ms"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkReadOp {
    pub chunk_id: ChunkId,
    pub chunk_offset: u64,
    pub length: u64,
    pub output_offset: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadBatch {
    pub file_version_id: Option<FileVersionId>,
    pub layout_root_id: LayoutRootId,
    pub ops: Vec<ChunkReadOp>,
    /// Advisory source hints, excluded from foreground output operations.
    /// Optional prefetch independently validates cached full-object grants.
    pub lookahead: Vec<ChunkId>,
}

impl ReadBatch {
    pub fn validate_for_output(&self, output_len: usize, config: &DfsReadConfig) -> Result<()> {
        config.validate()?;
        self.validate_lookahead()?;
        if self.layout_root_id.0.is_empty() {
            return Err(invalid("DFS read batch has no LayoutRoot"));
        }
        if self.ops.len() > config.max_ops_per_batch {
            return Err(invalid("DFS read batch contains too many ranges"));
        }
        let mut total = 0_u64;
        for op in &self.ops {
            if op.length == 0 {
                continue;
            }
            total = total
                .checked_add(op.length)
                .ok_or_else(|| invalid("DFS read batch byte count overflow"))?;
            let length = usize::try_from(op.length)
                .map_err(|_| invalid("DFS read range length is too large"))?;
            let end = op
                .output_offset
                .checked_add(length)
                .ok_or_else(|| invalid("DFS read output range overflow"))?;
            if end > output_len {
                return Err(invalid("DFS read output range exceeds buffer"));
            }
        }
        if total > config.max_inflight_bytes {
            return Err(invalid("DFS read batch exceeds inflight byte budget"));
        }
        Ok(())
    }

    fn validate_lookahead(&self) -> Result<()> {
        if self.lookahead.len() > MAX_DFS_READ_OPS
            || self.lookahead.iter().any(|chunk| chunk.0.is_empty())
        {
            return Err(invalid(
                "DFS source lookahead must contain at most 128 non-empty Chunk IDs",
            ));
        }
        Ok(())
    }

    fn source_request(
        &self,
        caller_id: String,
        namespace_id: crate::dfs::NamespaceId,
    ) -> Result<DfsChunkSourcesRequest> {
        let file_version_id = self
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("DFS peer read requires a fixed FileVersion"))?;
        let chunk_ids = self
            .ops
            .iter()
            .map(|op| op.chunk_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(DfsChunkSourcesRequest {
            caller_id,
            namespace_id,
            file_version_id,
            layout_root_id: self.layout_root_id.clone(),
            chunk_ids,
        })
    }
}

pub trait ReadSourceProvider: Send + Sync {
    fn sources_for(&self, request: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply>;
}

/// Revalidates committed external copy identity and allowed ranges on every
/// read batch. A source-cache hit is never an authorization-cache hit.
pub trait ExternalReadAuthorizer: Send + Sync {
    fn authorize_current_external_read(
        &self,
        _request: AuthorizeCurrentExternalRead,
    ) -> Result<Option<AuthorizeCurrentExternalReadReply>> {
        Ok(None)
    }

    fn authorize_external_version_read(
        &self,
        _request: AuthorizeExternalVersionRead,
    ) -> Result<Option<AuthorizeExternalVersionReadReply>> {
        Ok(None)
    }

    fn validate_external_read_grants(
        &self,
        caller_id: &str,
        validations: Vec<DfsReadValidation>,
    ) -> Result<Vec<DfsExternalAuthorizedRead>>;
}

/// Immutable full-object content whose length and BLAKE3 match its chunk identity.
/// Private fields prevent a reader from claiming verification of arbitrary bytes.
#[derive(Clone, Debug)]
pub struct VerifiedExternalChunk {
    chunk: ChunkObject,
    bytes: Arc<Vec<u8>>,
}

impl VerifiedExternalChunk {
    /// Verify the entire object before any requested range can become visible.
    /// Returns corrupt-data for a size/digest mismatch or an oversized object.
    pub fn verify(chunk: &ChunkObject, bytes: Vec<u8>) -> Result<Self> {
        Self::verify_with_digest(chunk, bytes, "serial", 1, 0, blake3::hash)
    }

    pub(super) fn verify_with_rayon_pool(
        chunk: &ChunkObject,
        bytes: Vec<u8>,
        pool: &rayon_core::ThreadPool,
        workers: usize,
        gate_wait_us: u64,
    ) -> Result<Self> {
        Self::verify_with_digest(
            chunk,
            bytes,
            "rayon",
            workers as u64,
            gate_wait_us,
            |bytes| {
                pool.install(|| {
                    let mut hasher = blake3::Hasher::new();
                    hasher.update_rayon(bytes);
                    hasher.finalize()
                })
            },
        )
    }

    fn verify_with_digest(
        chunk: &ChunkObject,
        bytes: Vec<u8>,
        execution: &'static str,
        workers: u64,
        gate_wait_us: u64,
        digest: impl FnOnce(&[u8]) -> blake3::Hash,
    ) -> Result<Self> {
        let span = tracing::info_span!(
            "node.dfs.external.verify",
            chunk_id = %chunk.id.0,
            chunk_bytes = chunk.length,
            hash_bytes = bytes.len() as u64,
            whole_hash_bytes = bytes.len() as u64,
            hash_execution = tracing::field::Empty,
            hash_workers = tracing::field::Empty,
            gate_wait_us = tracing::field::Empty,
        );
        span.in_scope(|| {
            if chunk.length > MAX_DFS_EXTERNAL_CHUNK_BYTES || bytes.len() as u64 != chunk.length {
                return Err(corrupt("external Chunk length or content digest mismatch"));
            }
            tracing::Span::current().record("hash_execution", execution);
            tracing::Span::current().record("hash_workers", workers);
            tracing::Span::current().record("gate_wait_us", gate_wait_us);
            if digest(&bytes).as_bytes() != &chunk.content_digest.bytes {
                return Err(corrupt("external Chunk length or content digest mismatch"));
            }
            Ok(())
        })?;
        Ok(Self {
            chunk: chunk.clone(),
            bytes: Arc::new(bytes),
        })
    }

    pub(super) fn load_with_rayon_pool(
        chunk: &ChunkObject,
        mut bytes: Vec<u8>,
        pool: &rayon_core::ThreadPool,
        gate_wait_us: u64,
        mut load_segment: impl FnMut(u64, &mut [u8], bool) -> Result<()> + Send,
        finalize_reader: impl FnOnce() -> Result<()>,
        mut before_segment_hash: impl FnMut(u64) + Send,
    ) -> Result<Self> {
        const READY_SEGMENT_BYTES: usize = 1024 * 1024;
        let span = tracing::info_span!(
            "node.dfs.external.verify",
            chunk_id = %chunk.id.0,
            chunk_bytes = chunk.length,
            hash_bytes = bytes.len() as u64,
            whole_hash_bytes = bytes.len() as u64,
            hash_execution = "ready_pipeline",
            hash_workers = pool.current_num_threads() as u64,
            gate_wait_us,
        );
        span.in_scope(|| {
            if chunk.length > MAX_DFS_EXTERNAL_CHUNK_BYTES || bytes.len() as u64 != chunk.length {
                return Err(corrupt("external Chunk length or content digest mismatch"));
            }
            let total_len = bytes.len();

            let mut hasher = blake3::Hasher::new();
            let segment_count = total_len.div_ceil(READY_SEGMENT_BYTES);
            if segment_count > 0 {
                let first_len = total_len.min(READY_SEGMENT_BYTES);
                load_segment(0, &mut bytes[..first_len], segment_count == 1)?;
            }

            for segment_index in 0..segment_count {
                let segment_offset = segment_index * READY_SEGMENT_BYTES;
                let segment_end = (segment_offset + READY_SEGMENT_BYTES).min(total_len);
                let next_offset = segment_end;
                let (loaded, remaining) = bytes.split_at_mut(next_offset);
                let segment = &loaded[segment_offset..segment_end];
                let next_len = remaining.len().min(READY_SEGMENT_BYTES);
                let pipeline_span = tracing::info_span!(
                    "node.dfs.s3.cache.load.pipeline_scope",
                    segment_offset = segment_offset as u64,
                    segment_bytes = segment.len() as u64,
                    next_segment_offset = tracing::field::Empty,
                    next_segment_bytes = next_len as u64,
                );
                if next_len != 0 {
                    pipeline_span.record("next_segment_offset", next_offset as u64);
                }
                let pipeline_parent = pipeline_span.clone();
                pipeline_span.in_scope(|| {
                    pool.in_place_scope(|scope| {
                        let hash_parent = pipeline_parent.clone();
                        let hasher = &mut hasher;
                        let before_segment_hash = &mut before_segment_hash;
                        scope.spawn(move |_| {
                            let hash_span = tracing::info_span!(
                                parent: &hash_parent,
                                "node.dfs.s3.cache.load.segment_hash",
                                chunk_id = %chunk.id.0,
                                segment_offset = segment_offset as u64,
                                segment_bytes = segment.len() as u64,
                                hash_bytes = segment.len() as u64,
                            );
                            hash_span.in_scope(|| {
                                before_segment_hash(segment_offset as u64);
                                hasher.update_rayon(segment);
                            });
                        });
                        if next_len == 0 {
                            Ok(())
                        } else {
                            let tail = next_offset + next_len == total_len;
                            load_segment(next_offset as u64, &mut remaining[..next_len], tail)
                        }
                    })
                })?;
            }

            tracing::info_span!("node.dfs.s3.cache.load.finalize_reader")
                .in_scope(finalize_reader)?;
            let digest_matches = tracing::info_span!(
                "node.dfs.s3.cache.load.hash_finalize",
                hash_bytes = total_len as u64,
            )
            .in_scope(|| hasher.finalize().as_bytes() == &chunk.content_digest.bytes);
            if !digest_matches {
                return Err(corrupt("external Chunk length or content digest mismatch"));
            }
            Ok(Self {
                chunk: chunk.clone(),
                bytes: Arc::new(bytes),
            })
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn matches(&self, chunk: &ChunkObject) -> bool {
        self.chunk == *chunk
    }

    /// Strip verified identity only when no reader or fill still owns the payload.
    /// The caller must retain capacity accounting and fully reverify before reuse.
    pub(super) fn try_into_bytes(self) -> std::result::Result<Vec<u8>, Self> {
        match Arc::try_unwrap(self.bytes) {
            Ok(bytes) => Ok(bytes),
            Err(bytes) => Err(Self {
                chunk: self.chunk,
                bytes,
            }),
        }
    }

    pub(super) fn allocation_weak(&self) -> Weak<Vec<u8>> {
        Arc::downgrade(&self.bytes)
    }

    pub(super) fn allocation_bytes(&self) -> u64 {
        self.bytes.capacity() as u64
    }

    pub(super) fn same_allocation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.bytes, &other.bytes)
    }
}

/// Fetch one complete immutable external chunk. Implementations must bound
/// response allocation and reject another store, key or generation. Content
/// crosses this boundary only through the validating immutable buffer above;
/// the engine still checks its full identity and authorizes every read batch.
pub trait ExternalChunkReader: Send + Sync {
    fn read_chunk(
        &self,
        chunk: &ChunkObject,
        source: &SourceCandidate,
    ) -> Result<VerifiedExternalChunk>;
}

struct ExternalReadBackend {
    reader: Arc<dyn ExternalChunkReader>,
    authorizer: Arc<dyn ExternalReadAuthorizer>,
}

/// Every operation keeps its own physical copy. A batch shares only the peer
/// connection and the same version and authenticated caller context; each copy retains its grant.
#[derive(Clone, Debug)]
pub struct ResolvedReadOp {
    pub op: ChunkReadOp,
    pub source: SourceCandidate,
}

#[derive(Clone, Debug)]
pub struct PeerReadBatch {
    pub file_version_id: FileVersionId,
    pub layout_root_id: LayoutRootId,
    pub operations: Vec<ResolvedReadOp>,
}

impl PeerReadBatch {
    pub fn validate(&self, output_len: usize) -> Result<()> {
        let first = self
            .operations
            .first()
            .ok_or_else(|| invalid("empty peer read batch"))?;
        ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(self.file_version_id.clone()),
            layout_root_id: self.layout_root_id.clone(),
            ops: self.operations.iter().map(|item| item.op.clone()).collect(),
        }
        .validate_for_output(output_len, &DfsReadConfig::default())?;
        for item in &self.operations {
            let grant = &item.source.read_grant;
            if item.op.length == 0
                || item
                    .source
                    .data_endpoint
                    .as_deref()
                    .is_none_or(str::is_empty)
                || item.source.chunk_id != item.op.chunk_id
                || grant.file_version_id != self.file_version_id
                || grant.layout_root_id != self.layout_root_id
                || grant.expires_at_unix_ms <= now_unix_ms()
                || !same_peer_context(&first.source, &item.source)
            {
                return Err(invalid(
                    "peer read source does not match the original batch identity",
                ));
            }
        }
        Ok(())
    }
}

fn validate_local_object_identity(expected: &ChunkObject, local: &ChunkObject) -> Result<()> {
    if expected != local {
        return Err(corrupt(
            "local Chunk catalog differs from the trusted layout content address",
        ));
    }
    Ok(())
}

pub trait ChunkTransfer: Send + Sync {
    /// `out` belongs to this attempt; the scheduler publishes it only after
    /// every operation completes. An adapter must drain or retain any DMA
    /// resources before returning, including on cancellation or timeout.
    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()>;

    /// One exact authorized CRC range. Server verifies covering storage segments;
    /// the consumer verifies its descriptor and authenticated range checksum.
    fn read_crc32c_range(
        &self,
        _batch: &PeerReadBatch,
        _chunk: &ChunkObject,
        _out: &mut [u8],
    ) -> Result<()> {
        Err(Error::coded(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "CRC range transport is unavailable",
        ))
    }

    /// Capability check before creating or admitting a private cache file.
    /// Unsupported runtimes/transports preserve the original cache path.
    fn supports_verified_object_sink(&self) -> bool {
        false
    }

    /// Optional complete-object stream into a private disk sink and the
    /// currently requested private range. `None` means unsupported before
    /// starting an RPC or changing either destination. A successful sealed
    /// completion is still subject to the engine's final authority checks.
    fn read_verified_object_to_sink<'output>(
        &self,
        _batch: &PeerReadBatch,
        _chunk: &ChunkObject,
        _requested_offset: u64,
        _private_out: &'output mut [u8],
        _sink: &mut PeerStreamSink<'_>,
    ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
        Ok(None)
    }

    /// Speculative reads never wait for transport admission and never use the
    /// legacy complete-body fallback. The default starts no RPC.
    fn try_read_verified_object_to_sink<'output>(
        &self,
        _batch: &PeerReadBatch,
        _chunk: &ChunkObject,
        _requested_offset: u64,
        _private_out: &'output mut [u8],
        _sink: &mut PeerStreamSink<'_>,
        _control: &PrefetchControl,
    ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
        Ok(None)
    }

    /// Optional observation only; never grants permission or memory admission.
    fn verification_metrics(&self) -> Option<&crate::node::read_work::ReadWorkMetrics> {
        None
    }

    /// Return one complete immutable object bound to trusted Meta identity.
    /// Adapters without a completed-stream proof retain full root verification.
    fn read_verified_object<'a>(
        &self,
        batch: &PeerReadBatch,
        chunk: &'a ChunkObject,
        out: &'a mut [u8],
        proof_budget: Arc<OutboardBudget>,
    ) -> Result<VerifiedPeerChunk<'a>> {
        validate_verified_peer_object_request(batch, chunk, out.len())?;
        self.read_ranges(batch, out)?;
        if let Some(metrics) = self.verification_metrics() {
            metrics.hash("root_fallback", chunk.length);
        }
        let verified = VerifiedPeerChunk::verify(chunk, out, proof_budget)?;
        Ok(verified)
    }
}

pub(super) fn validate_verified_peer_object_request(
    batch: &PeerReadBatch,
    chunk: &ChunkObject,
    output_len: usize,
) -> Result<()> {
    batch.validate(output_len)?;
    if batch.operations.len() != 1
        || chunk.id.0.is_empty()
        || chunk.id.0.len() > 512
        || chunk.encoding != crate::dfs::ChunkEncoding::Raw
        || chunk.content_digest.algorithm != crate::dfs::DigestAlgorithm::Blake3
        || chunk.length == 0
        || chunk.length > crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
        || output_len as u64 != chunk.length
    {
        return Err(invalid(
            "verified Peer read requires one bounded complete object",
        ));
    }
    let item = &batch.operations[0];
    if item.op.chunk_id != chunk.id
        || item.source.chunk_id != chunk.id
        || item.op.chunk_offset != 0
        || item.op.output_offset != 0
        || item.op.length != chunk.length
    {
        return Err(invalid(
            "verified Peer read differs from the full object identity",
        ));
    }
    Ok(())
}

fn same_peer_context(left: &SourceCandidate, right: &SourceCandidate) -> bool {
    let same_node = matches!(
        (&left.location, &right.location),
        (CopyLocation::Node { node_id: a, node_epoch: ae, .. },
         CopyLocation::Node { node_id: b, node_epoch: be, .. }) if a == b && ae == be
    );
    let a = &left.read_grant;
    let b = &right.read_grant;
    same_node
        && left.data_endpoint == right.data_endpoint
        && a.namespace_id == b.namespace_id
        && a.file_version_id == b.file_version_id
        && a.layout_root_id == b.layout_root_id
        && a.caller_node_id == b.caller_node_id
        && a.caller_node_epoch == b.caller_node_epoch
}

fn range_covered(ranges: &[(u64, u64)], requested_start: u64, requested_len: u64) -> bool {
    ranges.iter().any(|(start, length)| {
        requested_start >= *start
            && start.checked_add(*length).is_some_and(|end| {
                requested_start
                    .checked_add(requested_len)
                    .is_some_and(|requested_end| requested_end <= end)
            })
    })
}

fn external_authority_entry_bytes(
    key: &ExternalAuthorityKey,
    chunk: &ChunkObject,
    grant: &DfsReadGrant,
    location: &CopyLocation,
    allowed_ranges: &[(u64, u64)],
) -> usize {
    let mut bytes = std::mem::size_of::<ExternalAuthorityEntry>()
        + key.namespace_id.0.len()
        + key.inode_id.0.len()
        + key.file_version_id.0.len()
        + key.layout_root_id.0.len()
        + key.chunk_id.0.len()
        + chunk.id.0.len()
        + grant.namespace_id.0.len()
        + grant.file_version_id.0.len()
        + grant.layout_root_id.0.len()
        + grant.caller_node_id.len()
        + grant.token.len()
        + std::mem::size_of_val(allowed_ranges);
    match location {
        CopyLocation::External {
            store_id,
            object_key,
            object_revision,
        } => {
            bytes = bytes
                .saturating_add(store_id.len())
                .saturating_add(object_key.len())
                .saturating_add(object_revision.len());
        }
        CopyLocation::Node {
            node_id, device_id, ..
        } => {
            bytes = bytes
                .saturating_add(node_id.len())
                .saturating_add(device_id.len());
        }
    }
    bytes
}

type SourceCacheKey = (FileVersionId, LayoutRootId, ChunkId);
const MAX_CACHED_SOURCES: usize = 1024;
const MAX_PINNED_READERS: usize = 256;

/// Entry-bounded metadata/pin ownership, not a bound on live payloads or all
/// open descriptors. Eviction drops only this cache's ownership; in-flight
/// clones keep their reader alive until their final holder releases them.
pub(super) struct BoundedLru<K, V> {
    capacity: usize,
    entries: HashMap<K, LruEntry<V>>,
    order: BTreeMap<u64, K>,
    clock: u64,
}

struct LruEntry<V> {
    value: V,
    used: u64,
}

impl<K: Clone + Eq + Hash, V> BoundedLru<K, V> {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: HashMap::new(),
            order: BTreeMap::new(),
            clock: 0,
        }
    }

    fn next_tick(&mut self) -> u64 {
        if self.clock == u64::MAX {
            // Preserve relative recency rather than wrapping into an existing
            // rank. All callers bound their resident metadata entry counts.
            let old_order = std::mem::take(&mut self.order);
            self.clock = 0;
            for (_, key) in old_order {
                if let Some(entry) = self.entries.get_mut(&key) {
                    entry.used = self.clock;
                    self.order.insert(self.clock, key);
                    self.clock = self.clock.saturating_add(1);
                }
            }
        }
        let tick = self.clock;
        self.clock = self.clock.saturating_add(1);
        tick
    }

    pub(super) fn get(&mut self, key: &K) -> Option<&V> {
        let old_tick = self.entries.get(key)?.used;
        self.order.remove(&old_tick);
        let tick = self.next_tick();
        self.order.insert(tick, key.clone());
        self.entries.get_mut(key).map(|entry| {
            entry.used = tick;
            &entry.value
        })
    }

    pub(super) fn insert(&mut self, key: K, value: V) {
        self.remove(&key);
        if self.capacity == 0 {
            return;
        }
        if self.entries.len() >= self.capacity
            && let Some((_, oldest)) = self.order.pop_first()
        {
            self.entries.remove(&oldest);
        }
        let tick = self.next_tick();
        self.order.insert(tick, key.clone());
        self.entries.insert(key, LruEntry { value, used: tick });
    }

    pub(super) fn remove(&mut self, key: &K) -> Option<V> {
        let entry = self.entries.remove(key)?;
        self.order.remove(&entry.used);
        Some(entry.value)
    }

    pub(super) fn pop_oldest(&mut self) -> Option<(K, V)> {
        let (_, key) = self.order.pop_first()?;
        let entry = self.entries.remove(&key)?;
        Some((key, entry.value))
    }

    fn retain(&mut self, mut keep: impl FnMut(&K, &mut V) -> bool) {
        let order = &mut self.order;
        self.entries.retain(|key, entry| {
            if keep(key, &mut entry.value) {
                true
            } else {
                order.remove(&entry.used);
                false
            }
        });
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct NativeReadAuthoritySnapshot {
    epoch: u64,
    expires_at: Option<Instant>,
}

impl NativeReadAuthoritySnapshot {
    pub(super) fn expires_at(&self) -> Option<Instant> {
        self.expires_at
    }
}

#[derive(Clone, Debug)]
struct LocalReadAuthorityState {
    epoch: u64,
    expires_at: Option<Instant>,
    stopped: bool,
}

/// Shared local reader authority state supplied by the Node lifecycle.
///
/// Product wiring should refresh it from registration/heartbeat and stop it
/// before FUSE output can be committed. Unit-only engines use an unbounded
/// unknown-epoch authority so legacy tests do not accidentally depend on Node
/// lifecycle plumbing.
pub(super) struct LocalReadAuthority {
    state: Mutex<LocalReadAuthorityState>,
}

impl LocalReadAuthority {
    pub(super) fn new(epoch: u64, expires_at: Instant) -> Self {
        Self {
            state: Mutex::new(LocalReadAuthorityState {
                epoch,
                expires_at: Some(expires_at),
                stopped: false,
            }),
        }
    }

    fn unbounded_for_tests_and_legacy() -> Self {
        Self {
            state: Mutex::new(LocalReadAuthorityState {
                epoch: 0,
                expires_at: None,
                stopped: false,
            }),
        }
    }

    pub(super) fn refresh(&self, epoch: u64, expires_at: Instant) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS read authority state is poisoned"))?;
        if state.stopped {
            return Err(external_denied("DFS read authority has stopped"));
        }
        if expires_at <= Instant::now()
            || state
                .expires_at
                .is_some_and(|deadline| deadline <= Instant::now())
        {
            state.stopped = true;
            return Err(external_denied("DFS read authority expired"));
        }
        if state.epoch != epoch {
            state.stopped = true;
            return Err(external_denied("DFS read authority epoch changed"));
        }
        state.expires_at = Some(expires_at);
        Ok(())
    }

    pub(super) fn stop(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.stopped = true;
        }
    }

    fn check_snapshot(&self, snapshot: &NativeReadAuthoritySnapshot) -> Result<()> {
        // Same-epoch heartbeats may extend future admissions, never an already
        // captured operation or opened view. Known stop/epoch changes win.
        let current = self.snapshot()?;
        if current.epoch != snapshot.epoch
            || snapshot
                .expires_at
                .is_some_and(|deadline| deadline <= Instant::now())
        {
            return Err(external_denied(
                "DFS read authority changed or expired before publication",
            ));
        }
        Ok(())
    }

    fn snapshot(&self) -> Result<NativeReadAuthoritySnapshot> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS read authority state is poisoned"))?;
        if state.stopped {
            return Err(external_denied("DFS read authority has stopped"));
        }
        if state
            .expires_at
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            return Err(external_denied("DFS read authority expired"));
        }
        Ok(NativeReadAuthoritySnapshot {
            epoch: state.epoch,
            expires_at: state.expires_at,
        })
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct ExternalAuthorityKey {
    namespace_id: crate::dfs::NamespaceId,
    inode_id: crate::dfs::InodeId,
    file_version_id: FileVersionId,
    layout_root_id: LayoutRootId,
    chunk_id: ChunkId,
}

struct ExternalAuthorityEntry {
    key: ExternalAuthorityKey,
    chunk: ChunkObject,
    copy_id: crate::dfs::CopyId,
    location: CopyLocation,
    grant: DfsReadGrant,
    allowed_ranges: Vec<(u64, u64)>,
    expires_at_unix_ms: u64,
    deadline: Instant,
    node_epoch: u64,
    bytes: usize,
    invalidated: AtomicBool,
    _reservation: Option<Arc<ExternalAuthorityReservation>>,
}

struct ExternalAuthorityReservation {
    bytes: usize,
    budget: Weak<Mutex<ExternalAuthorityBudget>>,
}

impl Drop for ExternalAuthorityReservation {
    fn drop(&mut self) {
        if let Some(budget) = self.budget.upgrade()
            && let Ok(mut budget) = budget.lock()
        {
            budget.bytes = budget.bytes.saturating_sub(self.bytes);
            budget.entries = budget.entries.saturating_sub(1);
        }
    }
}

#[derive(Default)]
struct ExternalAuthorityBudget {
    bytes: usize,
    entries: usize,
}

#[derive(Default)]
struct ExternalAuthorityCache {
    // Local rejection serializes in-flight output, including a fresh context
    // that has not yet been retained. It does not stop the Node/session.
    invalidation_generation: u64,
    entries: HashMap<ExternalAuthorityKey, Arc<ExternalAuthorityEntry>>,
    budget: Arc<Mutex<ExternalAuthorityBudget>>,
}

impl ExternalAuthorityCache {
    fn lookup(
        &mut self,
        key: &ExternalAuthorityKey,
        requested_start: u64,
        requested_len: u64,
        local: &NativeReadAuthoritySnapshot,
    ) -> Option<Arc<ExternalAuthorityEntry>> {
        let now = Instant::now();
        self.entries
            .retain(|_, entry| entry.deadline > now || Arc::strong_count(entry) > 1);
        let entry = self.entries.get(key)?;
        if entry.invalidated.load(Ordering::Acquire)
            || entry.deadline <= now
            || (local.epoch != 0 && entry.node_epoch != local.epoch)
            || local.expires_at.is_some_and(|deadline| deadline <= now)
            || !range_covered(&entry.allowed_ranges, requested_start, requested_len)
        {
            return None;
        }
        Some(entry.clone())
    }

    fn insert(&mut self, mut entry: ExternalAuthorityEntry) -> Option<Arc<ExternalAuthorityEntry>> {
        // Never replace an in-flight context with a new one under the same key.
        // Expired in-flight entries remain reachable for known invalidation.
        if self.entries.contains_key(&entry.key) {
            return None;
        }
        if entry.bytes > MAX_EXTERNAL_AUTH_CACHE_BYTES {
            return None;
        }
        let full = self
            .budget
            .lock()
            .map(|budget| {
                budget.entries >= MAX_EXTERNAL_AUTH_CACHE_ENTRIES
                    || budget.bytes.saturating_add(entry.bytes) > MAX_EXTERNAL_AUTH_CACHE_BYTES
            })
            .unwrap_or(true);
        if full {
            self.entries.retain(|_, value| Arc::strong_count(value) > 1);
        }
        let mut budget = self.budget.lock().ok()?;
        if budget.entries >= MAX_EXTERNAL_AUTH_CACHE_ENTRIES
            || budget.bytes.saturating_add(entry.bytes) > MAX_EXTERNAL_AUTH_CACHE_BYTES
        {
            return None;
        }
        budget.bytes += entry.bytes;
        budget.entries += 1;
        entry._reservation = Some(Arc::new(ExternalAuthorityReservation {
            bytes: entry.bytes,
            budget: Arc::downgrade(&self.budget),
        }));
        drop(budget);
        let entry = Arc::new(entry);
        self.entries.insert(entry.key.clone(), entry.clone());
        Some(entry)
    }

    fn invalidate_inode_version(
        &mut self,
        namespace_id: &crate::dfs::NamespaceId,
        inode_id: &crate::dfs::InodeId,
        file_version_id: &FileVersionId,
    ) {
        self.invalidation_generation = self.invalidation_generation.wrapping_add(1);
        self.entries.retain(|key, entry| {
            let keep = &key.namespace_id != namespace_id
                || &key.inode_id != inode_id
                || &key.file_version_id != file_version_id;
            if !keep {
                entry.invalidated.store(true, Ordering::Release);
            }
            keep
        });
    }
}

/// One low-frequency fill stage. Total encloses the whole fill. A streaming
/// receive also includes private disk writes; store then covers only the Ready
/// commit. Nested producer/write observations must not be added to this parent.
struct PeerFillStage<'a> {
    local: &'a LocalChunkStore,
    stage: &'static str,
    clock: PeerFillClock,
    active: bool,
}

impl<'a> PeerFillStage<'a> {
    fn start(local: &'a LocalChunkStore, stage: &'static str) -> Self {
        Self::since(local, stage, PeerFillClock::start())
    }

    fn since(local: &'a LocalChunkStore, stage: &'static str, clock: PeerFillClock) -> Self {
        Self {
            local,
            stage,
            clock,
            active: true,
        }
    }
}

impl Drop for PeerFillStage<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let (wall_ns, cpu_ns) = self.clock.elapsed();
        self.local
            .record_peer_cache_fill_stage(self.stage, wall_ns, cpu_ns);
    }
}

pub struct DfsReadEngine {
    namespace_id: crate::dfs::NamespaceId,
    local_node_id: String,
    local: Arc<LocalChunkStore>,
    source_provider: Arc<dyn ReadSourceProvider>,
    transfer: Arc<dyn ChunkTransfer>,
    config: DfsReadConfig,
    external_backend: Option<ExternalReadBackend>,
    peer_cache: Option<Arc<DiskPeerCache>>,
    read_authority: Arc<LocalReadAuthority>,
    readers: Mutex<BoundedLru<ChunkId, Arc<PinnedChunkReader>>>,
    #[cfg(test)]
    local_reader_install_barrier: Mutex<Option<Arc<std::sync::Barrier>>>,
    #[cfg(test)]
    peer_cache_recheck_hook: Mutex<Option<PeerCacheRecheckHook>>,
    #[cfg(test)]
    prefetch_attempt_hook: Mutex<Option<PrefetchAttemptHook>>,
    source_cache: Arc<Mutex<SourceCache>>,
    fetcher: Option<Arc<super::dfs_fetch::ObjectFetcher>>,
    external_authority_cache: Mutex<ExternalAuthorityCache>,
}

#[cfg(test)]
type PrefetchAttemptHook = Arc<dyn Fn(usize, &ChunkId) + Send + Sync>;

#[cfg(test)]
struct PeerCacheRecheckHook {
    stage: &'static str,
    entered: std::sync::mpsc::Sender<()>,
    release: std::sync::mpsc::Receiver<()>,
}

#[derive(Clone)]
struct CachedSources {
    expires_at: Instant,
    sources: ChunkSources,
}

struct SourceCacheEntry {
    value: CachedSources,
    retained_bytes: usize,
}

/// Source metadata only. Charge owned capacities and a conservative table
/// envelope; active read/RPC clones are separate from this resident cache.
struct SourceCache {
    entries: BoundedLru<SourceCacheKey, SourceCacheEntry>,
    capacity: usize,
    byte_limit: usize,
    table_bytes: usize,
    value_bytes: usize,
    count: usize,
    peak_bytes: usize,
    // Global only for in-flight admission, not a grant fence. Invalidation
    // leaves other immutable scopes resident but prevents any older reply
    // from reinstating data. Overflow permanently disables admission.
    generation: Option<u64>,
}

impl SourceCache {
    fn new(byte_limit: usize, enabled: bool) -> Self {
        let slot = std::mem::size_of::<(SourceCacheKey, SourceCacheEntry, u64)>()
            .saturating_add(std::mem::size_of::<(u64, SourceCacheKey)>())
            .saturating_mul(4)
            .saturating_add(64);
        let capacity = if enabled {
            byte_limit
                .saturating_sub(2048)
                .checked_div(slot)
                .unwrap_or(0)
                .min(MAX_CACHED_SOURCES)
        } else {
            0
        };
        Self {
            entries: BoundedLru::new(capacity),
            capacity,
            byte_limit,
            table_bytes: if capacity == 0 {
                0
            } else {
                capacity.saturating_mul(slot).saturating_add(2048)
            },
            value_bytes: 0,
            count: 0,
            peak_bytes: 0,
            generation: Some(0),
        }
    }

    fn retained_bytes(key: &SourceCacheKey, sources: &ChunkSources) -> usize {
        let heap = |capacity: usize| {
            if capacity == 0 {
                0
            } else {
                capacity.saturating_add(2 * std::mem::size_of::<usize>())
            }
        };
        // Both the hash table and recency tree own an independent key.
        let mut bytes = [key.0.0.capacity(), key.1.0.capacity(), key.2.0.capacity()]
            .into_iter()
            .fold(0_usize, |bytes, size| {
                bytes.saturating_add(heap(size).saturating_mul(2))
            });
        bytes = bytes
            .saturating_add(heap(sources.chunk_id.0.capacity()))
            .saturating_add(heap(
                sources
                    .sources
                    .capacity()
                    .saturating_mul(std::mem::size_of::<SourceCandidate>()),
            ))
            .saturating_add(heap(
                sources
                    .allowed_ranges
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(u64, u64)>()),
            ));
        if let Some(chunk) = &sources.chunk {
            bytes = bytes.saturating_add(heap(chunk.id.0.capacity()));
        }
        for source in &sources.sources {
            let grant = &source.read_grant;
            for size in [
                source.copy_id.0.capacity(),
                source.chunk_id.0.capacity(),
                source.data_endpoint.as_ref().map_or(0, String::capacity),
                grant.namespace_id.0.capacity(),
                grant.file_version_id.0.capacity(),
                grant.layout_root_id.0.capacity(),
                grant.caller_node_id.capacity(),
                grant.token.capacity(),
            ] {
                bytes = bytes.saturating_add(heap(size));
            }
            match &source.location {
                CopyLocation::Node {
                    node_id, device_id, ..
                } => {
                    bytes = bytes
                        .saturating_add(heap(node_id.capacity()))
                        .saturating_add(heap(device_id.capacity()));
                }
                CopyLocation::External {
                    store_id,
                    object_key,
                    object_revision,
                } => {
                    bytes = bytes
                        .saturating_add(heap(store_id.capacity()))
                        .saturating_add(heap(object_key.capacity()))
                        .saturating_add(heap(object_revision.capacity()));
                }
            }
        }
        bytes
    }

    fn bytes(&self) -> usize {
        if self.count == 0 {
            0
        } else {
            self.table_bytes.saturating_add(self.value_bytes)
        }
    }

    fn record(&self, local: &LocalChunkStore) {
        local.record_source_cache_resources(
            self.count,
            self.bytes(),
            self.peak_bytes,
            self.byte_limit,
        );
    }

    fn get(&mut self, key: &SourceCacheKey) -> Option<&CachedSources> {
        self.entries.get(key).map(|entry| &entry.value)
    }

    fn insert(&mut self, key: SourceCacheKey, value: CachedSources) {
        if let Some(previous) = self.entries.remove(&key) {
            self.count -= 1;
            self.value_bytes = self.value_bytes.saturating_sub(previous.retained_bytes);
        }
        let retained_bytes = Self::retained_bytes(&key, &value.sources);
        if self.capacity == 0
            || self.generation.is_none()
            || self.table_bytes.saturating_add(retained_bytes) > self.byte_limit
        {
            self.release_empty_table();
            return;
        }
        while self.count >= self.capacity
            || self
                .table_bytes
                .saturating_add(self.value_bytes)
                .saturating_add(retained_bytes)
                > self.byte_limit
        {
            let Some((_, oldest)) = self.entries.pop_oldest() else {
                return;
            };
            self.value_bytes = self.value_bytes.saturating_sub(oldest.retained_bytes);
            self.count -= 1;
        }
        self.entries.insert(
            key,
            SourceCacheEntry {
                value,
                retained_bytes,
            },
        );
        self.value_bytes = self.value_bytes.saturating_add(retained_bytes);
        self.count += 1;
        self.peak_bytes = self.peak_bytes.max(self.bytes());
    }

    fn retain(&mut self, mut keep: impl FnMut(&SourceCacheKey, &mut CachedSources) -> bool) {
        self.entries.retain(|key, entry| {
            if keep(key, &mut entry.value) {
                true
            } else {
                self.value_bytes = self.value_bytes.saturating_sub(entry.retained_bytes);
                self.count -= 1;
                false
            }
        });
        self.release_empty_table();
    }

    fn release_empty_table(&mut self) {
        if self.count == 0 {
            self.entries = BoundedLru::new(self.capacity);
        }
    }

    fn invalidate_scope(&mut self, version: &FileVersionId, layout: &LayoutRootId) {
        self.generation = self
            .generation
            .and_then(|generation| generation.checked_add(1));
        self.retain(|key, _| &key.0 != version || &key.1 != layout);
    }

    fn invalidate_observed_remote_identities(&mut self, fresh: &[ChunkSources]) -> bool {
        let changed = |old: &CachedSources| {
            old.sources.sources.iter().any(|old| {
                let CopyLocation::Node {
                    node_id,
                    node_epoch,
                    device_id,
                    device_epoch,
                    ..
                } = &old.location
                else {
                    return false;
                };
                fresh.iter().flat_map(|set| &set.sources).any(|new| {
                    matches!(&new.location, CopyLocation::Node {
                    node_id: new_node, node_epoch: new_epoch, device_id: new_device,
                    device_epoch: new_device_epoch, ..
                } if new_node == node_id && (new_epoch != node_epoch
                    || (new_device == device_id && new_device_epoch != device_epoch)))
                })
            })
        };
        if self
            .entries
            .entries
            .values()
            .any(|entry| changed(&entry.value.value))
        {
            self.generation = self
                .generation
                .and_then(|generation| generation.checked_add(1));
            self.retain(|_, entry| !changed(entry));
            true
        } else {
            false
        }
    }

    fn clear(&mut self) {
        self.entries = BoundedLru::new(self.capacity);
        self.count = 0;
        self.value_bytes = 0;
        self.generation = self
            .generation
            .and_then(|generation| generation.checked_add(1));
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.count
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.count == 0
    }

    #[cfg(test)]
    fn contains_key(&self, key: &SourceCacheKey) -> bool {
        self.entries.contains_key(key)
    }
}

impl Drop for DfsReadEngine {
    fn drop(&mut self) {
        // Task readers share this cache. Dropping one reader must not revoke
        // another active task's scope or clear its resident grants.
        let Some(cache) = Arc::get_mut(&mut self.source_cache) else {
            return;
        };
        let cache = match cache.get_mut() {
            Ok(cache) => cache,
            Err(poisoned) => poisoned.into_inner(),
        };
        cache.clear();
        cache.record(&self.local);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CurrentExternalReadOutcome {
    Read,
    Unsupported,
    HeadChanged(crate::dfs::InodeRecord),
}

#[derive(Clone, Copy)]
struct SourceCacheTiming {
    sent: Instant,
    sent_wall: u64,
    generation: Option<u64>,
    authority_deadline: Option<Instant>,
}

struct ExternalReadSelection {
    batch: ReadBatch,
    selected: Vec<ExternalReadSelected>,
    validations: Vec<DfsReadValidation>,
    missing_sources: Vec<DfsMissingReadSource>,
}

struct ExternalReadSelected {
    chunk_id: ChunkId,
    chunk_offset: u64,
    length: u64,
    output_offset: usize,
    chunk: Option<ChunkObject>,
    source: Option<SourceCandidate>,
}

struct AuthorizedExternalRead {
    chunk: ChunkObject,
    source: SourceCandidate,
    op: ChunkReadOp,
}

impl ExternalReadSelection {
    fn unique_chunk_count(&self) -> usize {
        self.selected
            .iter()
            .map(|entry| entry.chunk_id.clone())
            .collect::<BTreeSet<_>>()
            .len()
    }
}

struct CachedFetchContext {
    batch: ReadBatch,
    sources: ChunkSources,
    generation: Option<u64>,
    expires_at: Instant,
    authority: NativeReadAuthoritySnapshot,
    scope_check: Arc<dyn Fn() -> Result<()> + Send + Sync>,
}

impl DfsReadEngine {
    pub(super) fn publication_diagnostic(&self) -> Option<super::read_work::ReadDiagnosticSpan> {
        self.local
            .read_diagnostic(super::read_work::ReadDiagnosticStage::MetaCommitFileVersion)
    }

    pub(super) fn publication_coarse_diagnostic(
        &self,
    ) -> Option<super::read_work::ReadCoarseDiagnosticSpan> {
        self.local.coarse_diagnostic("meta_publish_rpc", 0)
    }

    pub(crate) fn gc_generation(&self) -> Result<u64> {
        self.local.gc_generation()
    }
    pub(crate) fn gc_inventory(&self) -> Result<Vec<crate::dfs::ChunkObject>> {
        self.local.gc_inventory()
    }
    pub(crate) fn collect_generation(&self, generation: u64, chunks: &[ChunkId]) -> Result<usize> {
        self.readers
            .lock()
            .map_err(|_| unavailable("reader cache poisoned"))?
            .retain(|id, _| !chunks.contains(id));
        self.source_cache
            .lock()
            .map_err(|_| unavailable("source cache poisoned"))?
            .retain(|_, _| false);
        self.local.collect_generation(generation, chunks)
    }

    pub fn new(
        namespace_id: crate::dfs::NamespaceId,
        local_node_id: String,
        local: Arc<LocalChunkStore>,
        source_provider: Arc<dyn ReadSourceProvider>,
        transfer: Arc<dyn ChunkTransfer>,
        config: DfsReadConfig,
    ) -> Self {
        let source_cache = SourceCache::new(
            config.source_cache_bytes,
            !config.source_cache_ttl.is_zero(),
        );
        source_cache.record(&local);
        Self {
            namespace_id,
            local_node_id,
            local,
            source_provider,
            transfer,
            config,
            external_backend: None,
            peer_cache: None,
            read_authority: Arc::new(LocalReadAuthority::unbounded_for_tests_and_legacy()),
            readers: Mutex::new(BoundedLru::new(MAX_PINNED_READERS)),
            #[cfg(test)]
            local_reader_install_barrier: Mutex::new(None),
            #[cfg(test)]
            peer_cache_recheck_hook: Mutex::new(None),
            #[cfg(test)]
            prefetch_attempt_hook: Mutex::new(None),
            source_cache: Arc::new(Mutex::new(source_cache)),
            fetcher: None,
            external_authority_cache: Mutex::new(ExternalAuthorityCache::default()),
        }
    }

    /// Select external-backend-only reads. Healthy local chunks and peer
    /// candidates are deliberately bypassed; errors never fall back to them.
    pub fn with_external_backend(
        mut self,
        reader: Arc<dyn ExternalChunkReader>,
        authorizer: Arc<dyn ExternalReadAuthorizer>,
    ) -> Self {
        self.external_backend = Some(ExternalReadBackend { reader, authorizer });
        self
    }

    /// Disposable local storage for already verified native Peer objects.
    /// Cache contents never supply grant, Node/session or file-view authority.
    pub(super) fn with_peer_cache(mut self, cache: Arc<DiskPeerCache>) -> Self {
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            self.fetcher = match super::dfs_fetch::ObjectFetcher::new(
                runtime,
                cache.proof_budget(),
                cache.max_inflight_objects(),
                self.transfer.verification_metrics().cloned(),
            ) {
                Ok(fetcher) => fetcher,
                Err(error) => {
                    afs_logging::warn!("dfs.fetcher_unavailable"; "error" => error.to_string());
                    None
                }
            };
        }
        self.peer_cache = Some(cache);
        self
    }

    pub(super) fn object_fetcher(&self) -> Option<Arc<super::dfs_fetch::ObjectFetcher>> {
        self.fetcher.clone()
    }

    #[cfg(test)]
    pub(super) fn set_prefetch_attempt_hook_for_test(&self, hook: PrefetchAttemptHook) {
        *self.prefetch_attempt_hook.lock().unwrap() = Some(hook);
    }

    fn task_reader(&self) -> Self {
        Self {
            namespace_id: self.namespace_id.clone(),
            local_node_id: self.local_node_id.clone(),
            local: self.local.clone(),
            source_provider: self.source_provider.clone(),
            transfer: self.transfer.clone(),
            config: self.config.clone(),
            external_backend: None,
            peer_cache: self.peer_cache.clone(),
            read_authority: self.read_authority.clone(),
            readers: Mutex::new(BoundedLru::new(0)),
            #[cfg(test)]
            local_reader_install_barrier: Mutex::new(None),
            #[cfg(test)]
            peer_cache_recheck_hook: Mutex::new(None),
            #[cfg(test)]
            prefetch_attempt_hook: Mutex::new(None),
            source_cache: self.source_cache.clone(),
            fetcher: None,
            external_authority_cache: Mutex::new(ExternalAuthorityCache::default()),
        }
    }

    pub(super) fn with_read_authority(mut self, authority: Arc<LocalReadAuthority>) -> Self {
        self.read_authority = authority;
        self
    }

    pub(super) fn capture_native_read_authority(&self) -> Result<NativeReadAuthoritySnapshot> {
        self.read_authority.snapshot()
    }

    pub(super) fn check_native_read_authority(
        &self,
        snapshot: &NativeReadAuthoritySnapshot,
    ) -> Result<()> {
        self.read_authority.check_snapshot(snapshot)
    }

    fn invalidate_native_batch_authority(&self, batch: &ReadBatch) {
        // A read/grant denial belongs to this immutable read context, not the
        // Node registration lease. Drop its sources so a later attempt must
        // return to Meta; only Node lifecycle events stop global authority.
        if let Some(version) = &batch.file_version_id
            && let Ok(mut cache) = self.source_cache.lock()
        {
            cache.invalidate_scope(version, &batch.layout_root_id);
            cache.record(&self.local);
        }
    }

    fn invalidate_native_batch_on_error(&self, batch: &ReadBatch, error: &Error) {
        if matches!(
            error.kind(),
            afs_error::ErrorKind::PermissionDenied | afs_error::ErrorKind::Unauthenticated
        ) {
            self.invalidate_native_batch_authority(batch);
        }
    }

    pub(super) fn validate_native_read_size(&self, size: usize) -> Result<()> {
        self.config.validate()?;
        if size as u64 > self.config.max_inflight_bytes {
            return Err(invalid("DFS private read exceeds inflight byte budget"));
        }
        Ok(())
    }

    /// Fill a caller-owned, unpublished native result without an additional
    /// aggregate staging allocation. On error this private buffer may change
    /// and must be discarded. Borrowed public output must use `read_batch`.
    pub(super) fn read_batch_native_into_private(
        &self,
        batch: &ReadBatch,
        out: &mut [u8],
    ) -> Result<()> {
        self.read_batch_native_into_private_before(batch, out, None)
    }

    pub(super) fn read_batch_native_into_private_before(
        &self,
        batch: &ReadBatch,
        out: &mut [u8],
        deadline: Option<Instant>,
    ) -> Result<()> {
        self.validate_native_read_size(out.len())?;
        batch.validate_for_output(out.len(), &self.config)?;
        if self.external_backend.is_some() {
            return Err(Error::coded(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                "private native read cannot use an external backend",
            ));
        }
        // Native layout operations are disjoint. Reject overlaps explicitly:
        // direct local grouping must not change the old scatter precedence.
        for (index, left) in batch.ops.iter().enumerate() {
            if left.length == 0 {
                continue;
            }
            let left_end = left.output_offset + left.length as usize;
            if batch.ops[index + 1..].iter().any(|right| {
                right.length != 0
                    && left.output_offset < right.output_offset + right.length as usize
                    && right.output_offset < left_end
            }) {
                return Err(invalid("private native read output ranges overlap"));
            }
        }
        let authority = self.capture_native_read_authority()?;
        self.read_batch_into_private(batch, out, true, deadline)
            .inspect_err(|error| {
                self.invalidate_native_batch_on_error(batch, error);
            })?;
        self.check_native_read_authority(&authority)
    }

    pub fn has_external_backend(&self) -> bool {
        self.external_backend.is_some()
    }

    pub fn read_batch(&self, batch: &ReadBatch, out: &mut [u8]) -> Result<()> {
        let requested_bytes = batch
            .ops
            .iter()
            .fold(0_u64, |sum, op| sum.saturating_add(op.length));
        let span = tracing::info_span!(
            "node.dfs.read.batch",
            ops = batch.ops.len(),
            requested_bytes,
            output_bytes = out.len() as u64,
            external = self.external_backend.is_some(),
        );
        let _entered = span.enter();
        batch.validate_for_output(out.len(), &self.config)?;
        let native_authority = if self.external_backend.is_none() {
            Some(self.capture_native_read_authority()?)
        } else {
            None
        };
        // Stage only requested bytes, bounded by the validated batch budget.
        // Local successes and peer retries become visible together after every
        // range succeeds; a failed read never alters the caller's output.
        let mut staged_batch = batch.clone();
        let mut length = 0;
        for op in &mut staged_batch.ops {
            op.output_offset = length;
            length += op.length as usize;
        }
        let mut staged = vec![0; length];
        let expires_at = if let Some(backend) = &self.external_backend {
            self.read_external_batch(backend, &staged_batch, &mut staged)?
        } else {
            self.read_batch_into(&staged_batch, &mut staged)
                .inspect_err(|error| {
                    self.invalidate_native_batch_on_error(batch, error);
                })?;
            None
        };
        // Full-object fetches may outlive the authorization that allowed them.
        // Check again at the publication boundary, including cached sources.
        if expires_at.is_some_and(|deadline| deadline <= now_unix_ms()) {
            return Err(external_denied(
                "external read authority expired before publication",
            ));
        }
        if let Some(authority) = &native_authority {
            self.check_native_read_authority(authority)?;
        }
        for (original, packed) in batch.ops.iter().zip(&staged_batch.ops) {
            if original.length == 0 {
                continue;
            }
            let length = original.length as usize;
            self.local
                .record_read_copy("staging_to_output", original.length);
            tracing::info_span!(
                "node.dfs.read.copy",
                copy_stage = "staged_to_output",
                requested_bytes = original.length,
                copied_bytes = length as u64,
            )
            .in_scope(|| {
                out[original.output_offset..original.output_offset + length]
                    .copy_from_slice(&staged[packed.output_offset..packed.output_offset + length]);
            });
        }
        Ok(())
    }

    /// Read one contiguous native range into its private final response.
    /// Unsupported layouts retain the borrowed scheduler. Validate before
    /// allocation; a failed transfer can modify only this unpublished Vec.
    pub fn read_batch_native_owned(
        &self,
        batch: &ReadBatch,
        output_len: usize,
    ) -> Result<Option<Vec<u8>>> {
        self.read_batch_native_owned_before(batch, output_len, None)
    }

    pub(super) fn read_batch_native_owned_before(
        &self,
        batch: &ReadBatch,
        output_len: usize,
        deadline: Option<Instant>,
    ) -> Result<Option<Vec<u8>>> {
        if output_len as u64 > self.config.max_inflight_bytes {
            return Err(invalid("DFS owned read exceeds inflight byte budget"));
        }
        batch.validate_for_output(output_len, &self.config)?;
        if self.external_backend.is_some()
            || batch.ops.len() != 1
            || batch.ops[0].output_offset != 0
            || batch.ops[0].length != output_len as u64
        {
            return Ok(None);
        }
        let span = tracing::info_span!(
            "node.dfs.read.batch",
            ops = batch.ops.len(),
            requested_bytes = output_len as u64,
            output_bytes = output_len as u64,
            external = false,
            owned = true,
        );
        let _entered = span.enter();
        let authority = self.capture_native_read_authority()?;
        let mut result = vec![0; output_len];
        self.read_batch_into_private(batch, &mut result, true, deadline)
            .inspect_err(|error| {
                self.invalidate_native_batch_on_error(batch, error);
            })?;
        self.check_native_read_authority(&authority)?;
        Ok(Some(result))
    }

    pub fn read_batch_current_external(
        &self,
        inode_id: &crate::dfs::InodeId,
        batch: &ReadBatch,
        out: &mut [u8],
    ) -> Result<CurrentExternalReadOutcome> {
        let Some(backend) = &self.external_backend else {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        };
        let requested_bytes = batch
            .ops
            .iter()
            .fold(0_u64, |sum, op| sum.saturating_add(op.length));
        let span = tracing::info_span!(
            "node.dfs.read.current_external",
            inode_id = %inode_id.0,
            ops = batch.ops.len(),
            requested_bytes,
            output_bytes = out.len() as u64,
            selected_chunks = tracing::field::Empty,
            authorized_ranges = tracing::field::Empty,
            fetched_chunks = tracing::field::Empty,
            copied_bytes = tracing::field::Empty,
        );
        let _entered = span.enter();
        batch.validate_for_output(out.len(), &self.config)?;
        let mut staged_batch = batch.clone();
        let mut length = 0;
        for op in &mut staged_batch.ops {
            op.output_offset = length;
            length += op.length as usize;
        }
        let mut staged = vec![0; length];
        if staged_batch.file_version_id.is_none() {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        }
        let Some(selection) = self.select_current_external_reads(&staged_batch)? else {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        };
        let Some(expected_head_version) = selection.batch.file_version_id.clone() else {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        };
        span.record("selected_chunks", selection.unique_chunk_count() as u64);
        let source_timing = self.source_cache_timing()?;
        let reply = tracing::info_span!(
            "node.dfs.read.current_auth_rpc",
            validations = selection.validations.len(),
            missing_sources = selection.missing_sources.len(),
        )
        .in_scope(|| {
            backend
                .authorizer
                .authorize_current_external_read(AuthorizeCurrentExternalRead {
                    caller_id: self.local_node_id.clone(),
                    inode_id: inode_id.clone(),
                    namespace_id: self.namespace_id.clone(),
                    expected_head_version: expected_head_version.clone(),
                    layout_root_id: selection.batch.layout_root_id.clone(),
                    validations: selection.validations.clone(),
                    missing_sources: selection.missing_sources.clone(),
                })
        })?;
        let Some(reply) = reply else {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        };
        let expires_at = match reply {
            AuthorizeCurrentExternalReadReply::Authorized {
                inode,
                authorizations,
            } => {
                self.validate_current_authorized_inode(&inode, inode_id, &expected_head_version)?;
                span.record("authorized_ranges", authorizations.len() as u64);
                self.read_external_authorized_selection(
                    backend,
                    &selection,
                    authorizations,
                    &mut staged,
                    source_timing,
                )?
            }
            AuthorizeCurrentExternalReadReply::HeadChanged(inode) => {
                self.validate_current_head_changed_inode(&inode, inode_id, &expected_head_version)?;
                return Ok(CurrentExternalReadOutcome::HeadChanged(inode));
            }
        };
        if expires_at.is_some_and(|deadline| deadline <= now_unix_ms()) {
            return Err(external_denied(
                "external read authority expired before publication",
            ));
        }
        for (original, packed) in batch.ops.iter().zip(&staged_batch.ops) {
            if original.length == 0 {
                continue;
            }
            let length = original.length as usize;
            tracing::info_span!(
                "node.dfs.read.copy",
                copy_stage = "staged_to_output",
                requested_bytes = original.length,
                copied_bytes = length as u64,
            )
            .in_scope(|| {
                out[original.output_offset..original.output_offset + length]
                    .copy_from_slice(&staged[packed.output_offset..packed.output_offset + length]);
            });
        }
        Ok(CurrentExternalReadOutcome::Read)
    }

    /// Read one immutable version using bounded, authenticated object contexts.
    /// Missing or expired contexts require Meta; transport/authorization errors
    /// propagate without falling back to old tokens or a different version.
    pub fn read_batch_external_version(
        &self,
        inode_id: &crate::dfs::InodeId,
        batch: &ReadBatch,
        out: &mut [u8],
    ) -> Result<CurrentExternalReadOutcome> {
        batch.validate_for_output(out.len(), &self.config)?;
        let mut packed = batch.clone();
        let mut size = 0;
        for op in &mut packed.ops {
            op.output_offset = size;
            size += op.length as usize;
        }
        let published = self.with_external_version_result(inode_id, &packed, size, |staged| {
            for (original, op) in batch.ops.iter().zip(&packed.ops) {
                let length = original.length as usize;
                if length == 0 {
                    continue;
                }
                tracing::info_span!(
                    "node.dfs.read.copy",
                    copy_stage = "staged_to_output",
                    requested_bytes = original.length,
                    copied_bytes = length as u64,
                )
                .in_scope(|| {
                    out[original.output_offset..original.output_offset + length]
                        .copy_from_slice(&staged[op.output_offset..op.output_offset + length]);
                });
            }
        })?;
        Ok(if published.is_some() {
            CurrentExternalReadOutcome::Read
        } else {
            CurrentExternalReadOutcome::Unsupported
        })
    }

    /// Return the private final response after the same final authority checks
    /// as borrowed reads. Holes are zeroed; unsupported authorizers return None.
    /// Reject response allocations beyond the existing read budget before I/O.
    pub fn read_batch_external_version_owned(
        &self,
        inode_id: &crate::dfs::InodeId,
        batch: &ReadBatch,
        output_len: usize,
    ) -> Result<Option<Vec<u8>>> {
        if output_len as u64 > self.config.max_inflight_bytes {
            return Err(invalid("DFS owned read exceeds inflight byte budget"));
        }
        batch.validate_for_output(output_len, &self.config)?;
        // Object-grouped fetches may reorder overlapping writes. Keep the
        // borrowed packed/scatter path for those uncommon layouts.
        for (index, op) in batch
            .ops
            .iter()
            .enumerate()
            .filter(|(_, op)| op.length != 0)
        {
            let end = op.output_offset + op.length as usize;
            if batch.ops[..index].iter().any(|previous| {
                previous.length != 0
                    && previous.output_offset < end
                    && op.output_offset < previous.output_offset + previous.length as usize
            }) {
                return Ok(None);
            }
        }
        self.with_external_version_result(inode_id, batch, output_len, |bytes| bytes)
    }

    fn with_external_version_result<T>(
        &self,
        inode_id: &crate::dfs::InodeId,
        batch: &ReadBatch,
        output_len: usize,
        publish: impl FnOnce(Vec<u8>) -> T,
    ) -> Result<Option<T>> {
        let Some(backend) = &self.external_backend else {
            return Ok(None);
        };
        batch.validate_for_output(output_len, &self.config)?;
        let local = self.read_authority.snapshot()?;
        let version = batch
            .file_version_id
            .as_ref()
            .ok_or_else(|| invalid("external read requires a version"))?;
        let span = tracing::info_span!(
            "node.dfs.read.external_version",
            ops = batch.ops.len(),
            auth_hits = tracing::field::Empty,
            auth_misses = tracing::field::Empty
        );
        let _entered = span.enter();
        let control = tracing::info_span!("node.dfs.read.authority_control").entered();
        let mut packed = batch.clone();
        packed.ops.retain(|op| op.length != 0);
        let selection = ExternalReadSelection {
            batch: packed.clone(),
            selected: packed
                .ops
                .iter()
                .map(|op| ExternalReadSelected {
                    chunk_id: op.chunk_id.clone(),
                    chunk_offset: op.chunk_offset,
                    length: op.length,
                    output_offset: op.output_offset,
                    chunk: None,
                    source: None,
                })
                .collect(),
            validations: Vec::new(),
            missing_sources: Vec::new(),
        };
        let mut permits = Vec::with_capacity(packed.ops.len());
        let mut authorizations = Vec::with_capacity(packed.ops.len());
        let mut misses = Vec::new();
        let invalidation_generation;
        {
            let mut cache = self
                .external_authority_cache
                .lock()
                .map_err(|_| unavailable("DFS external authority cache is poisoned"))?;
            invalidation_generation = cache.invalidation_generation;
            for op in &packed.ops {
                let key = self.external_authority_key(inode_id, &packed, &op.chunk_id)?;
                let permit = cache.lookup(&key, op.chunk_offset, op.length, &local);
                authorizations.push(
                    permit
                        .as_ref()
                        .map(|entry| Self::authorization_from_cache(entry, op)),
                );
                if permit.is_none() {
                    misses.push(DfsMissingReadSource {
                        chunk_id: op.chunk_id.clone(),
                        chunk_offset: op.chunk_offset,
                        length: op.length,
                    });
                }
                permits.push(permit);
            }
        }
        span.record("auth_hits", (packed.ops.len() - misses.len()) as u64);
        span.record("auth_misses", misses.len() as u64);
        let source_timing = self.source_cache_timing()?;
        let rpc_started = source_timing.sent;
        let mut new_deadline = None;
        if !misses.is_empty() {
            let count = misses.len();
            let reply = tracing::info_span!("node.dfs.read.version_auth_rpc", requests = count)
                .in_scope(|| {
                    backend.authorizer.authorize_external_version_read(
                        AuthorizeExternalVersionRead {
                            caller_id: self.local_node_id.clone(),
                            inode_id: inode_id.clone(),
                            namespace_id: self.namespace_id.clone(),
                            file_version_id: version.clone(),
                            layout_root_id: packed.layout_root_id.clone(),
                            requests: misses,
                        },
                    )
                });
            let reply = match reply {
                Ok(Some(reply)) => reply,
                Ok(None) => return Ok(None),
                Err(error) => {
                    if matches!(
                        error.kind(),
                        afs_error::ErrorKind::PermissionDenied
                            | afs_error::ErrorKind::Unauthenticated
                    ) {
                        self.invalidate_external_authority(inode_id, version)?;
                    }
                    return Err(error);
                }
            };
            if reply.authorizations.len() != count
                || reply.remaining_validity_ms == 0
                || reply.remaining_validity_ms > 5_000
            {
                self.invalidate_external_authority(inode_id, version)?;
                return Err(external_denied(
                    "external version authority reply is incomplete or expired",
                ));
            }
            // SEND-time anchoring already subtracts the complete RPC duration.
            // Do not grant a fresh five seconds to a delayed response.
            let deadline = rpc_started + Duration::from_millis(reply.remaining_validity_ms);
            if deadline <= Instant::now() {
                return Err(external_denied(
                    "external version authority expired during RPC",
                ));
            }
            new_deadline = Some(deadline);
            let mut fresh = reply.authorizations.into_iter();
            for authorization in &mut authorizations {
                if authorization.is_none() {
                    *authorization = fresh.next();
                }
            }
        }
        let mut deadline = new_deadline;
        if let Some(session_deadline) = local.expires_at {
            deadline = Some(deadline.map_or(session_deadline, |value| value.min(session_deadline)));
        }
        for permit in permits.iter().flatten() {
            deadline = Some(deadline.map_or(permit.deadline, |value| value.min(permit.deadline)));
        }
        let authorized = authorizations
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| external_denied("external version authority reply omitted a range"))?;
        if local.epoch != 0
            && authorized
                .iter()
                .any(|entry| entry.authorized.validation.grant.caller_node_epoch != local.epoch)
        {
            self.invalidate_external_authority(inode_id, version)?;
            return Err(external_denied(
                "external version authority has a different Node epoch",
            ));
        }
        drop(control);
        let mut staged = vec![0; output_len];
        // Existing batch checks validate every object/range before any GET and
        // retain complete BLAKE3 and caller-output atomicity.
        let expires = self.read_external_authorized_selection(
            backend,
            &selection,
            authorized.clone(),
            &mut staged,
            SourceCacheTiming {
                authority_deadline: deadline,
                ..source_timing
            },
        )?;
        if expires.is_some_and(|value| value <= now_unix_ms())
            || deadline.is_some_and(|value| value <= Instant::now())
        {
            return Err(external_denied(
                "external version authority expired before publication",
            ));
        }
        let mut cache = self
            .external_authority_cache
            .lock()
            .map_err(|_| unavailable("DFS external authority cache is poisoned"))?;
        let state = self
            .read_authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS read authority state is poisoned"))?;
        if cache.invalidation_generation != invalidation_generation
            || state.stopped
            || state.epoch != local.epoch
            || state
                .expires_at
                .is_some_and(|value| value <= Instant::now())
            || local
                .expires_at
                .is_some_and(|value| value <= Instant::now())
            || permits.iter().flatten().any(|entry| {
                entry.invalidated.load(Ordering::Acquire) || entry.deadline <= Instant::now()
            })
        {
            return Err(external_denied(
                "external version authority invalidated before publication",
            ));
        }
        // Cache only freshly admitted contexts. Hits never renew deadlines.
        for (index, authorization) in authorized.iter().enumerate() {
            if permits[index].is_none()
                && let Some(fresh_deadline) = new_deadline
            {
                let key =
                    self.external_authority_key(inode_id, &packed, &packed.ops[index].chunk_id)?;
                let mut cutoff = fresh_deadline;
                if let Some(session_deadline) = local.expires_at {
                    cutoff = cutoff.min(session_deadline);
                }
                let wall_remaining = authorization
                    .authorized
                    .expires_at_unix_ms
                    .saturating_sub(now_unix_ms());
                cutoff = cutoff.min(Instant::now() + Duration::from_millis(wall_remaining));
                let mut bytes = external_authority_entry_bytes(
                    &key,
                    &authorization.chunk,
                    &authorization.authorized.validation.grant,
                    &authorization.location,
                    &authorization.authorized.allowed_ranges,
                );
                // Conservatively reserve the key copy, Arc/map overhead, copy ID
                // and bounded projections held by the current batch as well.
                bytes = bytes
                    .saturating_add(authorization.authorized.validation.copy_id.0.capacity())
                    .saturating_mul(4)
                    .saturating_add(256);
                permits[index] = cache.insert(ExternalAuthorityEntry {
                    key,
                    chunk: authorization.chunk.clone(),
                    copy_id: authorization.authorized.validation.copy_id.clone(),
                    location: authorization.location.clone(),
                    grant: authorization.authorized.validation.grant.clone(),
                    allowed_ranges: authorization.authorized.allowed_ranges.clone(),
                    expires_at_unix_ms: authorization.authorized.expires_at_unix_ms,
                    deadline: cutoff,
                    node_epoch: authorization.authorized.validation.grant.caller_node_epoch,
                    bytes,
                    invalidated: AtomicBool::new(false),
                    _reservation: None,
                });
            }
        }
        // Invalidation takes the same locks: transferring this private result
        // or copying into a borrowed output is the publication boundary.
        Ok(Some(publish(staged)))
    }

    fn external_authority_key(
        &self,
        inode: &crate::dfs::InodeId,
        batch: &ReadBatch,
        chunk: &ChunkId,
    ) -> Result<ExternalAuthorityKey> {
        Ok(ExternalAuthorityKey {
            namespace_id: self.namespace_id.clone(),
            inode_id: inode.clone(),
            file_version_id: batch
                .file_version_id
                .clone()
                .ok_or_else(|| invalid("external read requires a version"))?,
            layout_root_id: batch.layout_root_id.clone(),
            chunk_id: chunk.clone(),
        })
    }

    fn authorization_from_cache(
        entry: &ExternalAuthorityEntry,
        op: &ChunkReadOp,
    ) -> DfsExternalAuthorizedRead {
        DfsExternalAuthorizedRead {
            authorized: crate::dfs::DfsAuthorizedRead {
                validation: DfsReadValidation {
                    grant: entry.grant.clone(),
                    chunk_id: entry.chunk.id.clone(),
                    copy_id: entry.copy_id.clone(),
                    chunk_offset: op.chunk_offset,
                    length: op.length,
                },
                allowed_ranges: entry.allowed_ranges.clone(),
                expires_at_unix_ms: entry.expires_at_unix_ms,
            },
            chunk: entry.chunk.clone(),
            location: entry.location.clone(),
        }
    }

    fn invalidate_external_authority(
        &self,
        inode: &crate::dfs::InodeId,
        version: &FileVersionId,
    ) -> Result<()> {
        self.external_authority_cache
            .lock()
            .map_err(|_| unavailable("DFS external authority cache is poisoned"))?
            .invalidate_inode_version(&self.namespace_id, inode, version);
        let mut sources = self
            .source_cache
            .lock()
            .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
        sources.generation = sources
            .generation
            .and_then(|generation| generation.checked_add(1));
        sources.retain(|key, _| &key.0 != version);
        sources.record(&self.local);
        Ok(())
    }

    fn read_external_batch(
        &self,
        backend: &ExternalReadBackend,
        batch: &ReadBatch,
        out: &mut [u8],
    ) -> Result<Option<u64>> {
        let input_ops = batch.ops.len();
        let input_bytes = batch
            .ops
            .iter()
            .fold(0_u64, |sum, op| sum.saturating_add(op.length));
        let span = tracing::info_span!(
            "node.dfs.read.external",
            ops = input_ops,
            requested_bytes = input_bytes,
            selected_chunks = tracing::field::Empty,
            authorized_ranges = tracing::field::Empty,
            fetched_chunks = tracing::field::Empty,
            copied_bytes = tracing::field::Empty,
        );
        let _entered = span.enter();
        let Some(selection) = self.select_external_reads(batch)? else {
            // Holes and EOF do not require fetching or authorizing a chunk.
            return Ok(None);
        };
        span.record("selected_chunks", selection.unique_chunk_count() as u64);
        let source_timing = self.source_cache_timing()?;
        let authorized = tracing::info_span!(
            "node.dfs.read.auth_rpc",
            validations = selection.validations.len(),
        )
        .in_scope(|| {
            backend
                .authorizer
                .validate_external_read_grants(&self.local_node_id, selection.validations.clone())
        })?;
        span.record("authorized_ranges", authorized.len() as u64);
        self.read_external_authorized_selection(backend, &selection, authorized, out, source_timing)
    }

    fn select_external_reads(&self, batch: &ReadBatch) -> Result<Option<ExternalReadSelection>> {
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: batch.file_version_id.clone(),
            layout_root_id: batch.layout_root_id.clone(),
            ops: batch
                .ops
                .iter()
                .filter(|op| op.length != 0)
                .cloned()
                .collect(),
        };
        if batch.ops.is_empty() {
            return Ok(None);
        }
        let sources = self.sources_for(&batch)?;
        self.select_external_reads_from_sources(batch, &sources, false)
    }

    fn select_current_external_reads(
        &self,
        batch: &ReadBatch,
    ) -> Result<Option<ExternalReadSelection>> {
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: batch.file_version_id.clone(),
            layout_root_id: batch.layout_root_id.clone(),
            ops: batch
                .ops
                .iter()
                .filter(|op| op.length != 0)
                .cloned()
                .collect(),
        };
        if batch.ops.is_empty() || batch.file_version_id.is_none() {
            return Ok(None);
        }
        let (sources, missing) = self.cached_sources_for(&batch)?;
        let mut selection = self
            .select_external_reads_from_sources(batch.clone(), &sources, true)?
            .unwrap_or(ExternalReadSelection {
                batch,
                selected: Vec::new(),
                validations: Vec::new(),
                missing_sources: Vec::new(),
            });
        for op in missing {
            selection.missing_sources.push(DfsMissingReadSource {
                chunk_id: op.chunk_id.clone(),
                chunk_offset: op.chunk_offset,
                length: op.length,
            });
            selection.selected.push(ExternalReadSelected {
                chunk_id: op.chunk_id,
                chunk_offset: op.chunk_offset,
                length: op.length,
                output_offset: op.output_offset,
                chunk: None,
                source: None,
            });
        }
        if selection.selected.is_empty() {
            return Ok(None);
        }
        Ok(Some(selection))
    }

    fn select_external_reads_from_sources(
        &self,
        batch: ReadBatch,
        sources: &[ChunkSources],
        allow_missing: bool,
    ) -> Result<Option<ExternalReadSelection>> {
        let mut selected = Vec::with_capacity(batch.ops.len());
        let mut validations = Vec::with_capacity(batch.ops.len());
        for op in &batch.ops {
            let Some(set) = sources.iter().find(|set| set.chunk_id == op.chunk_id) else {
                if allow_missing {
                    continue;
                }
                return Err(unavailable(
                    "external source lookup omitted a requested Chunk",
                ));
            };
            let (chunk, source, validation) = self.select_external_read_source(set, op)?;
            validations.push(validation);
            selected.push(ExternalReadSelected {
                chunk_id: op.chunk_id.clone(),
                chunk_offset: op.chunk_offset,
                length: op.length,
                output_offset: op.output_offset,
                chunk: Some(chunk),
                source: Some(source),
            });
        }
        if selected.is_empty() {
            return Ok(None);
        }
        Ok(Some(ExternalReadSelection {
            batch,
            selected,
            validations,
            missing_sources: Vec::new(),
        }))
    }

    fn select_external_read_source(
        &self,
        set: &ChunkSources,
        op: &ChunkReadOp,
    ) -> Result<(ChunkObject, SourceCandidate, DfsReadValidation)> {
        let Some(chunk) = set.chunk.as_ref() else {
            return Err(invalid("external source has no trusted Chunk identity"));
        };
        if chunk.id != op.chunk_id
            || chunk.length > MAX_DFS_EXTERNAL_CHUNK_BYTES
            || op
                .chunk_offset
                .checked_add(op.length)
                .is_none_or(|end| end > chunk.length)
        {
            return Err(invalid(
                "external Chunk identity, size or requested range is invalid",
            ));
        }
        let Some(source) = set.sources.iter().find(|source| {
            source.role == CopyRole::ExternalCommitted
                && source.state.is_readable()
                && source.data_endpoint.is_none()
                && matches!(source.location, CopyLocation::External { .. })
        }) else {
            return Err(unavailable("committed Chunk has no readable external copy"));
        };
        let validation = DfsReadValidation {
            grant: source.read_grant.clone(),
            chunk_id: op.chunk_id.clone(),
            copy_id: source.copy_id.clone(),
            chunk_offset: op.chunk_offset,
            length: op.length,
        };
        Ok((chunk.clone(), source.clone(), validation))
    }

    fn cached_sources_for(
        &self,
        batch: &ReadBatch,
    ) -> Result<(Vec<ChunkSources>, Vec<ChunkReadOp>)> {
        let version = batch
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("DFS peer read requires a fixed FileVersion"))?;
        let now = Instant::now();
        let mut resolved = Vec::new();
        let mut misses = Vec::new();
        let mut seen = BTreeSet::new();
        let mut cache = self
            .source_cache
            .lock()
            .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
        cache.retain(|_, entry| entry.expires_at > now);
        cache.record(&self.local);
        let mut missing_chunks = BTreeSet::new();
        for op in &batch.ops {
            let key = (
                version.clone(),
                batch.layout_root_id.clone(),
                op.chunk_id.clone(),
            );
            match cache.get(&key) {
                Some(entry) => {
                    if seen.insert(op.chunk_id.clone()) {
                        resolved.push(entry.sources.clone());
                    }
                }
                None => {
                    missing_chunks.insert(op.chunk_id.clone());
                    misses.push(op.clone());
                }
            }
        }
        resolved.retain(|set| !missing_chunks.contains(&set.chunk_id));
        Ok((resolved, misses))
    }

    fn read_external_authorized_selection(
        &self,
        backend: &ExternalReadBackend,
        selection: &ExternalReadSelection,
        authorized: Vec<DfsExternalAuthorizedRead>,
        out: &mut [u8],
        source_timing: SourceCacheTiming,
    ) -> Result<Option<u64>> {
        let authority_deadline = source_timing.authority_deadline;
        if authorized.len() != selection.selected.len() {
            return Err(external_denied(
                "external read authorization reply is incomplete",
            ));
        }
        let now = now_unix_ms();
        let mut expires_at = u64::MAX;
        let mut reads = Vec::with_capacity(authorized.len());
        for (entry, selected) in authorized.into_iter().zip(&selection.selected) {
            let authorization = &entry.authorized;
            let validation = &authorization.validation;
            let covered = authorization.allowed_ranges.iter().any(|(start, length)| {
                validation.chunk_offset >= *start
                    && start.checked_add(*length).is_some_and(|end| {
                        validation
                            .chunk_offset
                            .checked_add(validation.length)
                            .is_some_and(|requested_end| requested_end <= end)
                    })
            });
            if validation.chunk_id != selected.chunk_id
                || validation.chunk_offset != selected.chunk_offset
                || validation.length != selected.length
                || authorization.expires_at_unix_ms <= now
                || authorization.expires_at_unix_ms > validation.grant.expires_at_unix_ms
                || authorization.allowed_ranges.len() > MAX_DFS_READ_OPS
                || authorization.allowed_ranges.iter().any(|(start, length)| {
                    *length == 0
                        || start
                            .checked_add(*length)
                            .is_none_or(|end| end > entry.chunk.length)
                })
                || !covered
                || entry.chunk.id != selected.chunk_id
                || entry.chunk.length > MAX_DFS_EXTERNAL_CHUNK_BYTES
                || validation
                    .chunk_offset
                    .checked_add(validation.length)
                    .is_none_or(|end| end > entry.chunk.length)
            {
                return Err(external_denied(
                    "external read authorization changed identity, range or expiry",
                ));
            }
            let source = SourceCandidate {
                copy_id: validation.copy_id.clone(),
                chunk_id: validation.chunk_id.clone(),
                role: CopyRole::ExternalCommitted,
                state: CopyState::Ready,
                location: entry.location.clone(),
                data_endpoint: None,
                load_hint: 0,
                read_grant: validation.grant.clone(),
            };
            if !self.source_matches(&selection.batch, &source)
                || !matches!(source.location, CopyLocation::External { .. })
            {
                return Err(external_denied(
                    "external read authorization returned an unusable source",
                ));
            }
            if let CopyLocation::External {
                store_id,
                object_key,
                object_revision,
            } = &source.location
            {
                let expected_key =
                    crate::dfs::external_object_key(store_id, object_revision, &entry.chunk.id)
                        .map_err(|error| external_denied(error.to_string()))?;
                if object_key != &expected_key {
                    return Err(external_denied(
                        "external read authorization returned a different object key",
                    ));
                }
            }
            if let (Some(expected_chunk), Some(expected_source)) =
                (&selected.chunk, &selected.source)
            {
                if entry.chunk != *expected_chunk || source.location != expected_source.location {
                    return Err(external_denied(
                        "external read authorization changed cached source identity",
                    ));
                }
                let expected_validation = DfsReadValidation {
                    grant: expected_source.read_grant.clone(),
                    chunk_id: expected_source.chunk_id.clone(),
                    copy_id: expected_source.copy_id.clone(),
                    chunk_offset: selected.chunk_offset,
                    length: selected.length,
                };
                if authorization.validation != expected_validation {
                    return Err(external_denied(
                        "external read authorization changed cached grant identity",
                    ));
                }
            }
            expires_at = expires_at.min(authorization.expires_at_unix_ms);
            reads.push(AuthorizedExternalRead {
                chunk: entry.chunk,
                source,
                op: ChunkReadOp {
                    chunk_id: selected.chunk_id.clone(),
                    chunk_offset: selected.chunk_offset,
                    length: selected.length,
                    output_offset: selected.output_offset,
                },
            });
        }
        self.cache_authorized_external_sources(selection, &reads, source_timing)?;
        // Retain at most one full object plus the requested-byte staging buffer.
        // Several ranges of one chunk share this fetch and full-content digest.
        let mut fetched = BTreeSet::new();
        let mut fetched_chunks = 0_u64;
        let mut copied_bytes = 0_u64;
        for read in &reads {
            let chunk = &read.chunk;
            if !fetched.insert(chunk.id.clone()) {
                continue;
            }
            if expires_at <= now_unix_ms()
                || authority_deadline.is_some_and(|deadline| deadline <= Instant::now())
            {
                return Err(external_denied(
                    "external read authority expired before the next fetch",
                ));
            }
            let bytes = tracing::info_span!(
                "node.dfs.read.external_fetch",
                chunk_id = %chunk.id.0,
                chunk_bytes = chunk.length,
            )
            .in_scope(|| backend.reader.read_chunk(chunk, &read.source))?;
            fetched_chunks = fetched_chunks.saturating_add(1);
            if !bytes.matches(chunk) {
                return Err(corrupt(
                    "verified external Chunk identity does not match the request",
                ));
            }
            let bytes = bytes.as_slice();
            for op in reads
                .iter()
                .filter(|read| read.chunk.id == chunk.id)
                .map(|read| &read.op)
            {
                let start = usize::try_from(op.chunk_offset)
                    .map_err(|_| invalid("external Chunk offset is too large"))?;
                let length = usize::try_from(op.length)
                    .map_err(|_| invalid("external Chunk range is too large"))?;
                copied_bytes = copied_bytes.saturating_add(length as u64);
                tracing::info_span!(
                    "node.dfs.read.external_copy",
                    copy_stage = "external_to_staged",
                    chunk_id = %chunk.id.0,
                    chunk_offset = op.chunk_offset,
                    requested_bytes = op.length,
                    copied_bytes = length as u64,
                )
                .in_scope(|| {
                    out[op.output_offset..op.output_offset + length]
                        .copy_from_slice(&bytes[start..start + length]);
                });
            }
        }
        tracing::Span::current().record("fetched_chunks", fetched_chunks);
        tracing::Span::current().record("copied_bytes", copied_bytes);
        Ok(Some(expires_at))
    }

    fn cache_authorized_external_sources(
        &self,
        selection: &ExternalReadSelection,
        reads: &[AuthorizedExternalRead],
        timing: SourceCacheTiming,
    ) -> Result<()> {
        let Some(version) = selection.batch.file_version_id.clone() else {
            return Ok(());
        };
        if reads.is_empty() {
            return Ok(());
        }
        let mut cache = self
            .source_cache
            .lock()
            .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
        if timing.generation.is_none() || cache.generation != timing.generation {
            return Ok(());
        }
        let now = Instant::now();
        cache.retain(|_, entry| entry.expires_at > now);
        for read in reads {
            let sources = ChunkSources {
                allowed_ranges: Vec::new(),
                chunk_id: read.chunk.id.clone(),
                chunk: Some(read.chunk.clone()),
                sources: vec![read.source.clone()],
            };
            if let Some(deadline) = self.source_deadline(&sources, timing.sent, timing.sent_wall) {
                let expires_at = timing
                    .authority_deadline
                    .map_or(deadline, |limit| deadline.min(limit));
                let key = (
                    version.clone(),
                    selection.batch.layout_root_id.clone(),
                    read.chunk.id.clone(),
                );
                let expires_at = cache
                    .get(&key)
                    .map_or(expires_at, |previous| expires_at.min(previous.expires_at));
                if expires_at <= now {
                    continue;
                }
                cache.insert(
                    key,
                    CachedSources {
                        expires_at,
                        sources,
                    },
                );
            }
        }
        cache.record(&self.local);
        Ok(())
    }

    fn validate_current_authorized_inode(
        &self,
        inode: &crate::dfs::InodeRecord,
        expected_inode_id: &crate::dfs::InodeId,
        expected_head_version: &FileVersionId,
    ) -> Result<()> {
        if inode.namespace_id != self.namespace_id
            || inode.inode_id != *expected_inode_id
            || inode.kind != crate::dfs::InodeKind::Regular
            || inode.head_version.as_ref() != Some(expected_head_version)
        {
            return Err(external_denied(
                "current external read authorization returned a different inode head",
            ));
        }
        Ok(())
    }

    fn validate_current_head_changed_inode(
        &self,
        inode: &crate::dfs::InodeRecord,
        expected_inode_id: &crate::dfs::InodeId,
        expected_head_version: &FileVersionId,
    ) -> Result<()> {
        if inode.namespace_id != self.namespace_id
            || inode.inode_id != *expected_inode_id
            || inode.kind != crate::dfs::InodeKind::Regular
            || inode.head_version.as_ref() == Some(expected_head_version)
        {
            return Err(external_denied(
                "current external read head-changed reply returned an invalid inode",
            ));
        }
        Ok(())
    }

    fn read_batch_into(&self, batch: &ReadBatch, out: &mut [u8]) -> Result<()> {
        self.read_batch_into_private(batch, out, false, None)
    }

    fn read_batch_into_private(
        &self,
        batch: &ReadBatch,
        out: &mut [u8],
        final_owned: bool,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let mut groups: Vec<(ChunkId, Vec<usize>)> = Vec::new();
        let mut positions: HashMap<ChunkId, usize> = HashMap::new();
        for (index, op) in batch.ops.iter().enumerate() {
            if op.length == 0 {
                continue;
            }
            if let Some(group_index) = positions.get(&op.chunk_id).copied() {
                groups[group_index].1.push(index);
            } else {
                positions.insert(op.chunk_id.clone(), groups.len());
                groups.push((op.chunk_id.clone(), vec![index]));
            }
        }
        let mut remote = vec![false; batch.ops.len()];
        for (chunk_id, indices) in groups {
            let reader = match self.local_reader(&chunk_id) {
                Ok(reader) => reader,
                Err(error) => {
                    self.handle_local_read_failure(&chunk_id, &error)?;
                    for index in indices {
                        remote[index] = true;
                    }
                    continue;
                }
            };
            // Published layout ChunkIds are trusted content addresses. The
            // local catalog/pinned descriptor cannot substitute a different
            // standard root or object shape for that Meta layout identity.
            let expected = if batch.file_version_id.is_some() {
                ChunkObject::from_content_addressed_id(&chunk_id)
            } else {
                None
            };
            if let Some(expected) = &expected {
                validate_local_object_identity(expected, reader.chunk())?;
            }
            let mut ranges = Vec::with_capacity(indices.len());
            for &index in &indices {
                let op = &batch.ops[index];
                let length = usize::try_from(op.length)
                    .map_err(|_| invalid("DFS read length is too large"))?;
                ranges.push(VerifiedRangeCopy {
                    chunk_offset: op.chunk_offset,
                    length,
                    output_offset: op.output_offset,
                });
            }
            let read = if expected.is_some() {
                reader.read_ranges_at_uncommitted(&ranges, out)
            } else {
                // Legacy IDs and unpublished local reads retain the previous
                // complete-root path; they do not claim a Meta-anchored proof.
                reader.read_ranges_at_uncommitted_fullscan(&ranges, out)
            };
            match read {
                Ok(counts) => {
                    let mut short = false;
                    for (&index, count) in indices.iter().zip(counts) {
                        let expected = usize::try_from(batch.ops[index].length)
                            .map_err(|_| invalid("DFS read length is too large"))?;
                        if count != expected {
                            remote[index] = true;
                            short = true;
                        }
                    }
                    if short {
                        let error = corrupt("local Chunk ended before the requested range");
                        self.handle_local_read_failure(&chunk_id, &error)?;
                    }
                }
                Err(error) => {
                    self.handle_local_read_failure(&chunk_id, &error)?;
                    for index in indices {
                        remote[index] = true;
                    }
                }
            }
        }
        let remote_ops = batch
            .ops
            .iter()
            .enumerate()
            .filter(|(index, _op)| remote[*index])
            .map(|(_, op)| op.clone())
            .collect::<Vec<_>>();
        if remote_ops.is_empty() {
            return Ok(());
        }
        let remote_batch = ReadBatch {
            lookahead: batch.lookahead.clone(),
            file_version_id: batch.file_version_id.clone(),
            layout_root_id: batch.layout_root_id.clone(),
            ops: remote_ops,
        };
        let sources = self.sources_for(&remote_batch)?;
        let authority = self.capture_native_read_authority()?;
        if authority.epoch != 0
            && sources
                .iter()
                .flat_map(|set| &set.sources)
                .any(|source| source.read_grant.caller_node_epoch != authority.epoch)
        {
            self.invalidate_native_batch_authority(batch);
            return Err(external_denied(
                "DFS source caller epoch differs from the registered session",
            ));
        }
        self.read_remote_batch(&remote_batch, &sources, out, final_owned, deadline)?;
        Ok(())
    }

    fn handle_local_read_failure(&self, chunk_id: &ChunkId, error: &Error) -> Result<()> {
        if matches!(
            error.kind(),
            afs_error::ErrorKind::DataLoss | afs_error::ErrorKind::NotFound
        ) {
            self.local.try_quarantine(chunk_id);
        }
        // A cached pin can outlive a corrupt file's replacement. Drop it on
        // failure so the next attempt opens the current physical copy instead
        // of retrying the retired descriptor.
        self.readers
            .lock()
            .map_err(|_| unavailable("DFS local reader cache is poisoned"))?
            .remove(chunk_id);
        Ok(())
    }

    fn local_reader(&self, chunk_id: &ChunkId) -> Result<Arc<PinnedChunkReader>> {
        if let Some(reader) = self
            .readers
            .lock()
            .map_err(|_| unavailable("DFS local reader cache is poisoned"))?
            .get(chunk_id)
            .cloned()
        {
            return Ok(reader);
        }
        let reader = Arc::new(self.local.open_verified(chunk_id)?);
        #[cfg(test)]
        {
            // Deterministically hold concurrent cold opens before installation.
            // No cache or hook mutex is held while the test barrier waits.
            let barrier = self.local_reader_install_barrier.lock().unwrap().clone();
            if let Some(barrier) = barrier {
                barrier.wait();
            }
        }
        let mut readers = self
            .readers
            .lock()
            .map_err(|_| unavailable("DFS local reader cache is poisoned"))?;
        // Another cold reader may have installed the same identity while this
        // descriptor was opening/verifying outside the cache lock. Share its
        // canonical pin; the redundant temporary descriptor is released here.
        if let Some(cached) = readers.get(chunk_id) {
            return Ok(cached.clone());
        }
        readers.insert(chunk_id.clone(), reader.clone());
        Ok(reader)
    }

    /// Maximum metadata lookahead for this engine; external source
    /// authorization stays on its independent current-version path.
    pub(super) fn source_lookahead_chunks(&self) -> usize {
        if self.external_backend.is_some()
            || self.config.source_cache_ttl.is_zero()
            || self.config.source_cache_bytes == 0
        {
            0
        } else {
            self.config.source_lookahead_chunks.min(MAX_DFS_READ_OPS)
        }
    }

    fn source_cache_timing(&self) -> Result<SourceCacheTiming> {
        let cache = self
            .source_cache
            .lock()
            .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
        Ok(SourceCacheTiming {
            sent: Instant::now(),
            sent_wall: now_unix_ms(),
            generation: cache.generation,
            authority_deadline: None,
        })
    }

    fn source_deadline(
        &self,
        sources: &ChunkSources,
        sent: Instant,
        sent_wall: u64,
    ) -> Option<Instant> {
        let earliest = sources
            .sources
            .iter()
            .map(|source| source.read_grant.expires_at_unix_ms)
            .min()?;
        let remaining = earliest
            .saturating_sub(SOURCE_GRANT_SAFETY_MS)
            .saturating_sub(sent_wall);
        let duration = self
            .config
            .source_cache_ttl
            .min(Duration::from_millis(remaining));
        if duration.is_zero() {
            None
        } else {
            sent.checked_add(duration)
        }
    }

    fn sources_for(&self, batch: &ReadBatch) -> Result<Vec<ChunkSources>> {
        self.sources_for_attempt(batch, true)
    }

    fn sources_for_attempt(
        &self,
        batch: &ReadBatch,
        allow_generation_retry: bool,
    ) -> Result<Vec<ChunkSources>> {
        batch.validate_lookahead()?;
        let version = batch
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("DFS peer read requires a fixed FileVersion"))?;
        let authority = if self.external_backend.is_none() {
            match self.capture_native_read_authority() {
                Ok(authority) => Some(authority),
                Err(error) => {
                    self.invalidate_native_batch_authority(batch);
                    return Err(error);
                }
            }
        } else {
            None
        };
        let matches = |source: &SourceCandidate| {
            self.source_matches(batch, source)
                && authority.as_ref().is_none_or(|view| {
                    view.epoch == 0 || source.read_grant.caller_node_epoch == view.epoch
                })
        };
        let now = Instant::now();
        let mut resolved = Vec::new();
        let mut misses = Vec::new();
        let mut seen = BTreeSet::new();
        let mut advisory = Vec::new();
        let span = tracing::info_span!(
            "node.dfs.read.sources",
            ops = batch.ops.len(),
            cache_hits = tracing::field::Empty,
            cache_misses = tracing::field::Empty,
            fresh_chunks = tracing::field::Empty
        );
        let _entered = span.enter();
        let mut cache_hits = 0_u64;
        let generation;
        {
            let mut cache = self
                .source_cache
                .lock()
                .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
            cache.retain(|key, entry| {
                entry.expires_at > now
                    && (key.0 != version
                        || key.1 != batch.layout_root_id
                        || entry.sources.sources.iter().all(&matches))
            });
            for op in &batch.ops {
                if !seen.insert(op.chunk_id.clone()) {
                    continue;
                }
                let key = (
                    version.clone(),
                    batch.layout_root_id.clone(),
                    op.chunk_id.clone(),
                );
                let cached = if allow_generation_retry {
                    cache.get(&key)
                } else {
                    None
                };
                match cached {
                    Some(entry) => {
                        cache_hits += 1;
                        resolved.push(entry.sources.clone());
                    }
                    None => misses.push(op.chunk_id.clone()),
                }
            }
            if seen.len() > MAX_DFS_READ_OPS {
                return Err(invalid("DFS source lookup exceeds 128 mandatory Chunk IDs"));
            }
            if !misses.is_empty() {
                for chunk_id in batch.lookahead.iter().take(self.source_lookahead_chunks()) {
                    if chunk_id.0.len() > MAX_SOURCE_LOOKAHEAD_ID_BYTES {
                        continue;
                    }
                    if misses.len() + advisory.len() >= MAX_DFS_READ_OPS {
                        break;
                    }
                    if !seen.insert(chunk_id.clone()) {
                        continue;
                    }
                    let key = (
                        version.clone(),
                        batch.layout_root_id.clone(),
                        chunk_id.clone(),
                    );
                    if cache.get(&key).is_none() {
                        advisory.push(chunk_id.clone());
                    }
                }
            }
            generation = cache.generation;
            cache.record(&self.local);
        }
        span.record("cache_hits", cache_hits);
        span.record("cache_misses", misses.len() as u64);
        if misses.is_empty() {
            span.record("fresh_chunks", 0_u64);
            return Ok(resolved);
        }
        let mandatory: BTreeSet<_> = misses.iter().cloned().collect();
        let mut request =
            batch.source_request(self.local_node_id.clone(), self.namespace_id.clone())?;
        request.chunk_ids.retain(|chunk| mandatory.contains(chunk));
        request.chunk_ids.extend(advisory.iter().cloned());
        let mut sent = Instant::now();
        let mut sent_wall = now_unix_ms();
        let first = self.source_provider.sources_for(request.clone());
        let reply = match first {
            Err(error)
                if !advisory.is_empty()
                    && error.code() == afs_error::META_CATALOG_INVALID_REQUEST =>
            {
                // The code does not identify the failing chunk. Revalidate
                // mandatory IDs once; never call the first error advisory-only.
                request.chunk_ids.retain(|chunk| mandatory.contains(chunk));
                sent = Instant::now();
                sent_wall = now_unix_ms();
                self.source_provider.sources_for(request.clone())
            }
            reply => reply,
        };
        let fresh = match reply {
            Ok(reply) => reply.chunks,
            Err(error) => {
                self.invalidate_native_batch_authority(batch);
                return Err(error);
            }
        };
        span.record("fresh_chunks", fresh.len() as u64);
        let expected: BTreeSet<_> = request.chunk_ids.into_iter().collect();
        let mut received = BTreeSet::new();
        for set in &fresh {
            if !expected.contains(&set.chunk_id)
                || !received.insert(set.chunk_id.clone())
                || set
                    .chunk
                    .as_ref()
                    .is_some_and(|chunk| chunk.id != set.chunk_id)
                || set
                    .sources
                    .iter()
                    .any(|source| source.chunk_id != set.chunk_id || !matches(source))
            {
                self.invalidate_native_batch_authority(batch);
                return Err(invalid(
                    "DFS source reply changed the requested content or authorization identity",
                ));
            }
        }
        if !mandatory.is_subset(&received) {
            self.invalidate_native_batch_authority(batch);
            return Err(unavailable("DFS source lookup omitted a mandatory Chunk"));
        }
        if let Some(authority) = &authority
            && let Err(error) = self.check_native_read_authority(authority)
        {
            self.invalidate_native_batch_authority(batch);
            return Err(error);
        }
        {
            let mut cache = self
                .source_cache
                .lock()
                .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
            if generation.is_none() || cache.generation != generation {
                drop(cache);
                if allow_generation_retry {
                    // Discard ALL earlier cache hits and the old reply. The
                    // second attempt bypasses cache and reauthorizes every
                    // current mandatory ID, without speculative metadata.
                    let mut current = batch.clone();
                    current.lookahead.clear();
                    return self.sources_for_attempt(&current, false);
                }
                return Err(unavailable(
                    "DFS source context changed again during reauthorization",
                ));
            }
            // A newly observed remote process/device identity retires cached
            // grants for that identity across layouts, but not unrelated peers.
            // No global equality assumption is made about per-copy fences.
            if cache.invalidate_observed_remote_identities(&fresh) && !resolved.is_empty() {
                // Cache-hit clones were captured before this reply revealed
                // the new identity. Retiring resident entries alone cannot
                // revoke those clones: discard them and authorize the entire
                // current request through the same bounded retry path.
                cache.record(&self.local);
                drop(cache);
                if allow_generation_retry {
                    let mut current = batch.clone();
                    current.lookahead.clear();
                    return self.sources_for_attempt(&current, false);
                }
                return Err(unavailable(
                    "DFS remote identity changed again during reauthorization",
                ));
            }
            let now = Instant::now();
            for set in &fresh {
                if let Some(expires_at) = self.source_deadline(set, sent, sent_wall)
                    && expires_at > now
                {
                    cache.insert(
                        (
                            version.clone(),
                            batch.layout_root_id.clone(),
                            set.chunk_id.clone(),
                        ),
                        CachedSources {
                            expires_at,
                            sources: set.clone(),
                        },
                    );
                }
            }
            cache.record(&self.local);
        }
        resolved.extend(
            fresh
                .into_iter()
                .filter(|set| mandatory.contains(&set.chunk_id)),
        );
        Ok(resolved)
    }

    fn source_matches(&self, batch: &ReadBatch, source: &SourceCandidate) -> bool {
        let grant = &source.read_grant;
        Some(&grant.file_version_id) == batch.file_version_id.as_ref()
            && grant.layout_root_id == batch.layout_root_id
            && grant.namespace_id == self.namespace_id
            && grant.caller_node_id == self.local_node_id
            && grant.caller_node_epoch != 0
            && grant.expires_at_unix_ms > now_unix_ms()
            && !grant.token.is_empty()
    }

    fn peer_cache_source_eligible(
        &self,
        batch: &ReadBatch,
        chunk: &ChunkObject,
        source: &SourceCandidate,
    ) -> bool {
        self.source_matches(batch, source)
            && source.state.is_readable()
            && matches!(
                source.role,
                CopyRole::DurableReplica | CopyRole::VerifiedCache
            )
            && source.chunk_id == chunk.id
            && !source.copy_id.0.is_empty()
            && source
                .data_endpoint
                .as_deref()
                .is_some_and(|endpoint| !endpoint.is_empty())
            && matches!(&source.location, CopyLocation::Node {
                node_id, node_epoch, device_id, device_epoch, catalog_revision,
            } if node_id != &self.local_node_id && !node_id.is_empty() && *node_epoch != 0
                && !device_id.is_empty() && *device_epoch != 0 && *catalog_revision != 0)
    }

    fn check_peer_cache_authority(
        &self,
        batch: &ReadBatch,
        source: &SourceCandidate,
        authority: &NativeReadAuthoritySnapshot,
    ) -> Result<()> {
        self.check_native_read_authority(authority)?;
        if !self.source_matches(batch, source)
            || (authority.epoch != 0 && source.read_grant.caller_node_epoch != authority.epoch)
        {
            return Err(external_denied(
                "Peer cache read grant expired or changed before publication",
            ));
        }
        Ok(())
    }

    /// Disposable cache I/O is never a source/authorization result. The
    /// caller keeps its own final grant check and the normal verified Peer
    /// fallback; this also covers rechecks after waiting for another fill.
    fn read_optional_peer_cache(
        &self,
        operation: &mut super::peer_cache::PeerCacheOperation<'_>,
        chunk: &ChunkObject,
        offset: u64,
        output: &mut [u8],
    ) -> bool {
        match operation.read_range(chunk, offset, output) {
            Ok(hit) => hit,
            Err(error) => {
                self.local.record_peer_disk_cache_event("corrupt_or_io");
                afs_logging::warn!("dfs.peer_cache_read_failed"; "error" => error.to_string());
                false
            }
        }
    }

    #[cfg(test)]
    fn wait_peer_cache_recheck_for_test(&self, stage: &'static str) {
        let hook = {
            let mut slot = self.peer_cache_recheck_hook.lock().unwrap();
            if slot.as_ref().is_some_and(|hook| hook.stage == stage) {
                slot.take()
            } else {
                None
            }
        };
        if let Some(hook) = hook {
            hook.entered.send(()).unwrap();
            hook.release.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }

    fn read_native_peer_cache(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        final_owned: bool,
    ) -> Result<bool> {
        self.read_native_peer_cache_mode(batch, sources, out, final_owned, false)
    }

    fn read_native_peer_cache_mode(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        final_owned: bool,
        stream_only: bool,
    ) -> Result<bool> {
        let Some(cache) = &self.peer_cache else {
            return Ok(false);
        };
        // The cache never changes external-backend routing or complex batch
        // scatter/retry semantics. Only the currently requested object is read.
        if self.external_backend.is_some() || batch.ops.len() != 1 {
            return Ok(false);
        }
        if cache.namespace_id() != &self.namespace_id {
            return Err(invalid("Peer cache namespace differs from the read engine"));
        }
        let op = &batch.ops[0];
        let Some(set) = sources.iter().find(|set| set.chunk_id == op.chunk_id) else {
            return Ok(false);
        };
        let Some(chunk) = &set.chunk else {
            return Ok(false);
        };
        if chunk.is_segment_crc32c() {
            return Ok(false);
        }
        if chunk.id != op.chunk_id
            || chunk.length == 0
            || set.allowed_ranges.len() > MAX_DFS_READ_OPS
            || set.allowed_ranges.iter().any(|(start, length)| {
                *length == 0
                    || start
                        .checked_add(*length)
                        .is_none_or(|end| end > chunk.length)
            })
        {
            self.invalidate_native_batch_authority(batch);
            return Err(invalid(
                "Peer cache source has an invalid immutable object or grant range",
            ));
        }
        if chunk.length > crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
            || set.allowed_ranges.is_empty()
        {
            return Ok(false);
        }
        if !range_covered(&set.allowed_ranges, op.chunk_offset, op.length) {
            return Err(external_denied(
                "Peer cache requested range is outside the committed grant",
            ));
        }
        let source = set
            .sources
            .iter()
            .find(|source| self.peer_cache_source_eligible(batch, chunk, source));
        let Some(source) = source else {
            return Ok(false);
        };
        let authority = self.capture_native_read_authority()?;
        self.check_peer_cache_authority(batch, source, &authority)?;
        // Acquire the one synchronous cache operation before any full-object
        // allocation. No retained payload, additional fill or prefetch queue.
        let mut total = PeerFillStage {
            local: &self.local,
            stage: "total",
            clock: PeerFillClock::start(),
            active: false,
        };
        let mut operation = match cache.operation() {
            Ok(operation) => operation,
            Err(error) => {
                self.local.record_peer_disk_cache_event("fallback");
                afs_logging::warn!("dfs.peer_cache_unavailable"; "error" => error.to_string());
                return Ok(false);
            }
        };
        let lock_wait_elapsed = total.clock.elapsed();
        let lookup_clock = PeerFillClock::start();
        self.check_peer_cache_authority(batch, source, &authority)?;
        let start = op.output_offset;
        let end = start + op.length as usize; // batch validated before scheduling
        if self.read_optional_peer_cache(
            &mut operation,
            chunk,
            op.chunk_offset,
            &mut out[start..end],
        ) {
            self.local.record_peer_disk_cache_event("hit");
            self.check_peer_cache_authority(batch, source, &authority)?;
            return Ok(true);
        }
        self.local.record_peer_disk_cache_event("miss");
        // Partial layout grants permit their requested ranges, never an
        // expanded full-object Peer fetch. Unknown old-server grants fall back.
        if !final_owned
            || !range_covered(&set.allowed_ranges, 0, chunk.length)
            || chunk
                .length
                .checked_add(out.len() as u64)
                .is_none_or(|bytes| bytes > self.config.max_inflight_bytes)
        {
            self.local.record_peer_disk_cache_event("fallback");
            return Ok(false);
        }
        if !operation.has_control_capacity() {
            self.local.record_peer_disk_cache_event("fallback");
            return Ok(false);
        }
        // Share only immutable data work. Every caller retains its own grant,
        // deadline and final delivery check; a stopped leader cannot authorize
        // another request. Flight wait and the one body permit never hold the
        // cache metadata lock.
        let mut leader = match cache.fill_flight(chunk, || {
            self.check_peer_cache_authority(batch, source, &authority)
        })? {
            crate::node::peer_cache::FillRole::Leader(leader) => leader,
            crate::node::peer_cache::FillRole::Finished(outcome) => {
                self.check_peer_cache_authority(batch, source, &authority)?;
                match outcome {
                    crate::node::peer_cache::FillOutcome::Ready => {
                        #[cfg(test)]
                        self.wait_peer_cache_recheck_for_test("follower_ready");
                        let hit = self.read_optional_peer_cache(
                            &mut operation,
                            chunk,
                            op.chunk_offset,
                            &mut out[start..end],
                        );
                        self.check_peer_cache_authority(batch, source, &authority)?;
                        return Ok(hit);
                    }
                    crate::node::peer_cache::FillOutcome::Uncached => return Ok(false),
                    crate::node::peer_cache::FillOutcome::Failed(_) => {
                        // A flight is shared by object, not by source, grant or
                        // deadline. Its leader's failure cannot decide another
                        // caller's read, including source-specific corruption.
                        // Continue once through the existing bounded range path;
                        // do not rejoin this still-retained terminal flight.
                        self.local.record_peer_disk_cache_event("fallback");
                        return Ok(false);
                    }
                }
            }
        };
        let result = (|| {
            let mut use_stream = self.transfer.supports_verified_object_sink()
                && (crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64)
                    .checked_add(out.len() as u64)
                    .is_some_and(|bytes| bytes <= self.config.max_inflight_bytes);
            if stream_only && !use_stream {
                leader.finish(crate::node::peer_cache::FillOutcome::Uncached);
                return Ok(false);
            }
            if use_stream {
                operation.acquire_stream_fill(|| {
                    self.check_peer_cache_authority(batch, source, &authority)
                })?;
            } else {
                operation
                    .acquire_fill(|| self.check_peer_cache_authority(batch, source, &authority))?;
            }
            // A completed earlier fill may have become Ready while this leader
            // waited for the global body allowance. Recheck before any allocation.
            #[cfg(test)]
            self.wait_peer_cache_recheck_for_test("leader_recheck");
            if self.read_optional_peer_cache(
                &mut operation,
                chunk,
                op.chunk_offset,
                &mut out[start..end],
            ) {
                self.check_peer_cache_authority(batch, source, &authority)?;
                leader.finish(crate::node::peer_cache::FillOutcome::Ready);
                return Ok(true);
            }
            // Emit fill timings only after an actual complete-object fill has been
            // admitted. A cache hit or range fallback never increments fill counts.
            let lookup_elapsed = lookup_clock.elapsed();
            // The guard was declared before `operation`, so total also includes
            // its drain/observation drop and the private body's release.
            total.active = true;
            self.local.record_peer_cache_fill_stage(
                "lock_wait",
                lock_wait_elapsed.0,
                lock_wait_elapsed.1,
            );
            self.local
                .record_peer_cache_fill_stage("lookup", lookup_elapsed.0, lookup_elapsed.1);
            let mut preparation = Some(PeerFillStage::start(&self.local, "prepare"));
            self.check_peer_cache_authority(batch, source, &authority)?;
            let length = usize::try_from(chunk.length)
                .map_err(|_| invalid("Peer cache object length is too large"))?;
            // Only an unsupported streaming adapter allocates the legacy body.
            // After that decision, retries keep the old mechanism rather than hold
            // a full Vec alongside a new streaming pipeline reservation.
            let mut _legacy_fill_work = None;
            let mut body = Vec::new();
            let version = batch
                .file_version_id
                .clone()
                .ok_or_else(|| invalid("verified Peer cache fill requires a fixed FileVersion"))?;
            let mut last_error = None;
            // Each distinct eligible source gets one full-object attempt. A proof
            // is consumed entirely in its success branch, never mixed with another
            // attempt or reconstructed by rehashing an already completed stream.
            let mut attempted_copies = BTreeSet::new();
            for source in set
                .sources
                .iter()
                .filter(|source| self.peer_cache_source_eligible(batch, chunk, source))
                .take(MAX_DFS_READ_OPS)
            {
                if !attempted_copies.insert(&source.copy_id) {
                    continue;
                }
                self.check_peer_cache_authority(batch, source, &authority)?;
                let attempt = PeerReadBatch {
                    file_version_id: version.clone(),
                    layout_root_id: batch.layout_root_id.clone(),
                    operations: vec![ResolvedReadOp {
                        op: ChunkReadOp {
                            chunk_id: chunk.id.clone(),
                            chunk_offset: 0,
                            length: chunk.length,
                            output_offset: 0,
                        },
                        source: source.clone(),
                    }],
                };
                if use_stream {
                    // The adapter's Node-shared session owns transport work until
                    // its producers and every handed-off frame have drained.
                    let mut sink = operation.begin_stream(chunk)?;
                    if stream_only && !sink.is_caching() {
                        leader.finish(crate::node::peer_cache::FillOutcome::Uncached);
                        return Ok(false);
                    }
                    let output_pointer = out[start..end].as_ptr() as usize;
                    let output_length = end - start;
                    let prepare_elapsed = preparation.as_ref().map(|stage| stage.clock.elapsed());
                    let receive_clock = PeerFillClock::start();
                    let received = self.transfer.read_verified_object_to_sink(
                        &attempt,
                        chunk,
                        op.chunk_offset,
                        &mut out[start..end],
                        &mut sink,
                    );
                    if !received.as_ref().is_ok_and(Option::is_none) {
                        // An unsupported probe starts no RPC. Retain the legacy
                        // prepare clock in that case instead of inventing an
                        // extra receive observation or overlapping prepare work.
                        if let Some(mut stage) = preparation.take() {
                            stage.active = false;
                        }
                        if let Some(elapsed) = prepare_elapsed {
                            self.local
                                .record_peer_cache_fill_stage("prepare", elapsed.0, elapsed.1);
                        }
                        let elapsed = receive_clock.elapsed();
                        self.local.record_peer_cache_fill_stage(
                            "receive_verify",
                            elapsed.0,
                            elapsed.1,
                        );
                        preparation = Some(PeerFillStage::start(&self.local, "prepare"));
                    }
                    match received {
                        Ok(Some(completed)) => {
                            if completed.chunk() != chunk
                                || completed.requested_offset() != op.chunk_offset
                                || completed.requested_length() != output_length
                                || completed.root_digest() != &chunk.content_digest.bytes
                                || !completed
                                    .matches_private_output_identity(output_pointer, output_length)
                            {
                                return Err(corrupt(
                                    "streamed Peer completion changed trusted identity or private output",
                                ));
                            }
                            self.check_peer_cache_authority(batch, source, &authority)?;
                            drop(preparation.take());
                            let stored = {
                                let _store = PeerFillStage::start(&self.local, "store");
                                sink.commit(completed)
                            };
                            let _delivery = PeerFillStage::start(&self.local, "deliver");
                            leader.finish(if stored.as_ref().is_ok_and(|stored| *stored) {
                                crate::node::peer_cache::FillOutcome::Ready
                            } else {
                                crate::node::peer_cache::FillOutcome::Uncached
                            });
                            match stored {
                                Ok(true) => self.local.record_peer_disk_cache_event("fill"),
                                Ok(false) => self.local.record_peer_disk_cache_event("fill_failed"),
                                Err(error) => {
                                    self.local.record_peer_disk_cache_event("fill_failed");
                                    afs_logging::warn!("dfs.peer_cache_stream_commit_failed"; "error" => error.to_string());
                                }
                            }
                            // The adapter already extracted the range into the
                            // unpublished final output and counted actual copies.
                            // Cache I/O/quota failure does not cause another RPC.
                            self.check_peer_cache_authority(batch, source, &authority)?;
                            return Ok(true);
                        }
                        Ok(None) => use_stream = false,
                        Err(error) => {
                            self.local.record_peer_disk_cache_event("fill_failed");
                            self.invalidate_native_batch_on_error(batch, &error);
                            if matches!(
                                error.kind(),
                                afs_error::ErrorKind::PermissionDenied
                                    | afs_error::ErrorKind::Unauthenticated
                            ) {
                                return Err(error);
                            }
                            self.check_native_read_authority(&authority)?;
                            last_error = Some(error);
                            continue;
                        }
                    }
                }
                // A parallel child may never switch to the old full-body or
                // range transport. Its batch owner joins every child and drops
                // all fragment scratch before the original serial fallback.
                if stream_only {
                    leader.finish(crate::node::peer_cache::FillOutcome::Uncached);
                    return Ok(false);
                }
                // The unsupported sink and its pipeline allowance have dropped
                // before the original complete-object Vec is admitted.
                if body.is_empty() {
                    // The streaming adapter and sink have drained before changing
                    // admission modes. Legacy owns the original exclusive body
                    // permit, so its full Vec never coexists with another stream.
                    operation.release_fill();
                    operation.acquire_fill(|| {
                        self.check_peer_cache_authority(batch, source, &authority)
                    })?;
                    _legacy_fill_work = self.local.peer_disk_cache_fill_work(length, out.len());
                    body.resize(length, 0);
                }
                drop(preparation.take());
                let received = {
                    let _receive = PeerFillStage::start(&self.local, "receive_verify");
                    self.transfer.read_verified_object(
                        &attempt,
                        chunk,
                        &mut body,
                        cache.proof_budget(),
                    )
                };
                // Preparation resumes for proof identity/authorization checks, or
                // for error bookkeeping and the next bounded copy attempt.
                preparation = Some(PeerFillStage::start(&self.local, "prepare"));
                match received {
                    Ok(verified) => {
                        if verified.chunk() != chunk
                            || verified.bytes().len() as u64 != chunk.length
                        {
                            return Err(corrupt(
                                "verified Peer object proof changed trusted identity",
                            ));
                        }
                        if !verified.uses_completed_stream_proof()
                            && self.transfer.verification_metrics().is_none()
                        {
                            self.local.record_read_hash("root_fallback", chunk.length);
                        }
                        self.check_peer_cache_authority(batch, source, &authority)?;
                        drop(preparation.take());
                        let stored = {
                            let _store = PeerFillStage::start(&self.local, "store");
                            operation.store_verified(&verified)
                        };
                        let _delivery = PeerFillStage::start(&self.local, "deliver");
                        leader.finish(if stored.as_ref().is_ok_and(|stored| *stored) {
                            crate::node::peer_cache::FillOutcome::Ready
                        } else {
                            crate::node::peer_cache::FillOutcome::Uncached
                        });
                        match stored {
                            Ok(true) => self.local.record_peer_disk_cache_event("fill"),
                            Ok(false) => self.local.record_peer_disk_cache_event("fill_failed"),
                            Err(error) => {
                                self.local.record_peer_disk_cache_event("fill_failed");
                                afs_logging::warn!("dfs.peer_cache_write_failed"; "error" => error.to_string());
                            }
                        }
                        self.check_peer_cache_authority(batch, source, &authority)?;
                        let chunk_start = op.chunk_offset as usize;
                        out[start..end].copy_from_slice(
                            &verified.bytes()[chunk_start..chunk_start + op.length as usize],
                        );
                        self.local.record_read_copy("fill_to_output", op.length);
                        self.check_peer_cache_authority(batch, source, &authority)?;
                        return Ok(true);
                    }
                    Err(error) => {
                        self.local.record_peer_disk_cache_event("fill_failed");
                        self.invalidate_native_batch_on_error(batch, &error);
                        if matches!(
                            error.kind(),
                            afs_error::ErrorKind::PermissionDenied
                                | afs_error::ErrorKind::Unauthenticated
                        ) {
                            return Err(error);
                        }
                        self.check_native_read_authority(&authority)?;
                        last_error = Some(error);
                    }
                }
            }
            Err(last_error
                .unwrap_or_else(|| corrupt("Peer cache fill has no readable authorized source")))
        })();
        if let Err(error) = &result {
            // An individual leader's grant/session decision cannot revoke an
            // independently authorized follower. No Ready is installed when
            // the precommit check fails; followers take their own safe path.
            let outcome = if matches!(
                error.kind(),
                afs_error::ErrorKind::PermissionDenied | afs_error::ErrorKind::Unauthenticated
            ) {
                crate::node::peer_cache::FillOutcome::Uncached
            } else {
                crate::node::peer_cache::FillOutcome::Failed(error.clone())
            };
            leader.finish(outcome);
        }
        result
    }

    pub(super) fn readahead_object_limit(&self) -> usize {
        if self.external_backend.is_some() {
            return 0;
        }
        (self.config.readahead_max_bytes / crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64).min(8)
            as usize
    }

    fn check_cached_fetch(&self, context: &CachedFetchContext) -> Result<()> {
        (context.scope_check)()?;
        self.check_native_read_authority(&context.authority)?;
        if context.expires_at <= Instant::now() {
            return Err(external_denied("DFS prefetch grant expired"));
        }
        let version = context
            .batch
            .file_version_id
            .as_ref()
            .ok_or_else(|| invalid("DFS prefetch requires a fixed version"))?;
        let key = (
            version.clone(),
            context.batch.layout_root_id.clone(),
            context.sources.chunk_id.clone(),
        );
        let mut cache = self
            .source_cache
            .lock()
            .map_err(|_| unavailable("DFS source cache poisoned"))?;
        if context.generation.is_none() || cache.generation != context.generation {
            return Err(external_denied("DFS prefetch source generation changed"));
        }
        let Some(entry) = cache.get(&key) else {
            return Err(external_denied("DFS prefetch grant is no longer cached"));
        };
        if entry.expires_at <= Instant::now() || entry.sources != context.sources {
            return Err(external_denied("DFS prefetch cached grant changed"));
        }
        if !context.sources.sources.iter().any(|source| {
            self.source_matches(&context.batch, source)
                && (context.authority.epoch == 0
                    || source.read_grant.caller_node_epoch == context.authority.epoch)
        }) {
            return Err(external_denied("DFS prefetch has no valid caller grant"));
        }
        Ok(())
    }

    /// Scheduling consumes only already-resident R-1 grants. It never resolves
    /// metadata, queues for body quota or makes prefetch failure a read failure.
    pub(super) fn prefetch_cached(
        &self,
        batch: &ReadBatch,
        window: usize,
        scope_check: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) {
        let limit = self.readahead_object_limit().min(window);
        if limit == 0 {
            return;
        }
        let (Some(fetcher), Some(cache), Some(version)) =
            (&self.fetcher, &self.peer_cache, &batch.file_version_id)
        else {
            return;
        };
        if !self.transfer.supports_verified_object_sink() {
            fetcher.event("skip_busy");
            return;
        }
        for chunk_id in batch.lookahead.iter().take(limit) {
            let result: Result<()> = (|| {
                (scope_check)()?;
                // Bound and charge lookup-key clones before constructing them.
                let key_bytes = version
                    .0
                    .capacity()
                    .saturating_add(batch.layout_root_id.0.capacity())
                    .saturating_add(chunk_id.0.capacity());
                if key_bytes > 16 * 1024 {
                    fetcher.event("skip_budget");
                    return Ok(());
                }
                let Some(_probe_metadata) = cache.proof_budget().try_reserve(key_bytes * 4 + 1024)
                else {
                    fetcher.event("skip_budget");
                    return Ok(());
                };
                let key = (
                    version.clone(),
                    batch.layout_root_id.clone(),
                    chunk_id.clone(),
                );
                let (generation, expires_at, charge, chunk) = {
                    let mut sources = match self.source_cache.try_lock() {
                        Ok(sources) => sources,
                        Err(std::sync::TryLockError::WouldBlock) => {
                            fetcher.event("skip_busy");
                            return Ok(());
                        }
                        Err(std::sync::TryLockError::Poisoned(_)) => {
                            return Err(unavailable("DFS source cache poisoned"));
                        }
                    };
                    let generation = sources.generation;
                    if generation.is_none() {
                        fetcher.event("skip_grant");
                        return Ok(());
                    }
                    let Some(entry) = sources.get(&key) else {
                        fetcher.event("skip_grant");
                        return Ok(());
                    };
                    let Some(chunk) = &entry.sources.chunk else {
                        fetcher.event("skip_grant");
                        return Ok(());
                    };
                    if chunk.id != entry.sources.chunk_id
                        || chunk.id.0.is_empty()
                        || chunk.id.0.len() > 512
                        || chunk.length == 0
                        || chunk.length > crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
                        || chunk.encoding != crate::dfs::ChunkEncoding::Raw
                        || chunk.content_digest.algorithm != crate::dfs::DigestAlgorithm::Blake3
                        || entry.expires_at <= Instant::now()
                        || !range_covered(&entry.sources.allowed_ranges, 0, chunk.length)
                        || !entry
                            .sources
                            .sources
                            .iter()
                            .any(|source| self.peer_cache_source_eligible(batch, chunk, source))
                    {
                        fetcher.event("skip_grant");
                        return Ok(());
                    }
                    let charge = SourceCache::retained_bytes(&key, &entry.sources)
                        .saturating_mul(2)
                        .saturating_add(
                            std::mem::size_of::<Self>()
                                + std::mem::size_of::<CachedFetchContext>()
                                + 64 * 1024,
                        )
                        .saturating_add(
                            key_bytes * 4
                                + self.namespace_id.0.capacity()
                                + self.local_node_id.capacity(),
                        );
                    (generation, entry.expires_at, charge, chunk.clone())
                };
                if cache.try_contains_ready(&chunk)? != Some(false) {
                    return Ok(());
                }
                let Some(permit) = fetcher.try_reserve_background(charge)? else {
                    fetcher.event("skip_budget");
                    return Ok(());
                };
                let Some(leader) = cache.try_fill_flight(&chunk)? else {
                    fetcher.event("skip_busy");
                    return Ok(());
                };
                let authority = self.capture_native_read_authority()?;
                let selected = {
                    let mut sources = match self.source_cache.try_lock() {
                        Ok(sources) => sources,
                        Err(std::sync::TryLockError::WouldBlock) => {
                            fetcher.event("skip_busy");
                            return Ok(());
                        }
                        Err(std::sync::TryLockError::Poisoned(_)) => {
                            return Err(unavailable("DFS source cache poisoned"));
                        }
                    };
                    if sources.generation != generation {
                        fetcher.event("skip_grant");
                        return Ok(());
                    }
                    let Some(entry) = sources.get(&key) else {
                        fetcher.event("skip_grant");
                        return Ok(());
                    };
                    if entry.expires_at != expires_at || entry.expires_at <= Instant::now() {
                        fetcher.event("skip_grant");
                        return Ok(());
                    }
                    entry.sources.clone()
                };
                let context = Arc::new(CachedFetchContext {
                    batch: ReadBatch {
                        file_version_id: Some(version.clone()),
                        layout_root_id: batch.layout_root_id.clone(),
                        ops: Vec::new(),
                        lookahead: Vec::new(),
                    },
                    sources: selected,
                    generation,
                    expires_at,
                    authority,
                    scope_check: scope_check.clone(),
                });
                let reader = Arc::new(self.task_reader());
                let control = {
                    let reader = reader.clone();
                    let context = context.clone();
                    PrefetchControl::new(fetcher.cancellation(), move || {
                        reader.check_cached_fetch(&context)
                    })
                };
                fetcher.spawn_background(permit, move || {
                    reader.prefetch_one(&context, &control, leader)
                })?;
                fetcher.event("scheduled");
                Ok(())
            })();
            if let Err(error) = result {
                fetcher.event("failed");
                afs_logging::warn!("dfs.prefetch_skipped"; "error" => error.to_string());
            }
            #[cfg(test)]
            {
                let hook = self.prefetch_attempt_hook.lock().unwrap().clone();
                if let Some(hook) = hook {
                    hook(window, chunk_id);
                }
            }
        }
    }

    fn prefetch_one(
        &self,
        context: &CachedFetchContext,
        control: &PrefetchControl,
        mut leader: super::peer_cache::OwnedFillLeader,
    ) -> Result<()> {
        control.check()?;
        let cache = self
            .peer_cache
            .as_ref()
            .ok_or_else(|| unavailable("DFS prefetch cache missing"))?;
        let chunk = context
            .sources
            .chunk
            .as_ref()
            .ok_or_else(|| invalid("DFS prefetch object missing"))?;
        match cache.try_contains_ready(chunk)? {
            Some(false) => {}
            ready => {
                leader.finish(if ready == Some(true) {
                    super::peer_cache::FillOutcome::Ready
                } else {
                    super::peer_cache::FillOutcome::Uncached
                });
                return Ok(());
            }
        }
        let Some(mut operation) = cache.try_operation()? else {
            leader.finish(super::peer_cache::FillOutcome::Uncached);
            return Ok(());
        };
        let Some(mut sink) = operation.try_begin_stream(chunk)? else {
            leader.finish(super::peer_cache::FillOutcome::Uncached);
            return Ok(());
        };
        if !sink.is_caching() {
            leader.finish(super::peer_cache::FillOutcome::Uncached);
            return Ok(());
        }
        let Some(source) = context
            .sources
            .sources
            .iter()
            .find(|source| self.peer_cache_source_eligible(&context.batch, chunk, source))
        else {
            return Err(external_denied("DFS prefetch source expired"));
        };
        control.check()?;
        let batch = PeerReadBatch {
            file_version_id: context
                .batch
                .file_version_id
                .clone()
                .ok_or_else(|| invalid("DFS prefetch version missing"))?,
            layout_root_id: context.batch.layout_root_id.clone(),
            operations: vec![ResolvedReadOp {
                op: ChunkReadOp {
                    chunk_id: chunk.id.clone(),
                    chunk_offset: 0,
                    length: chunk.length,
                    output_offset: 0,
                },
                source: source.clone(),
            }],
        };
        let mut empty = [];
        let Some(completed) = self
            .transfer
            .try_read_verified_object_to_sink(&batch, chunk, 0, &mut empty, &mut sink, control)?
        else {
            leader.finish(super::peer_cache::FillOutcome::Uncached);
            return Ok(());
        };
        control.check()?;
        let stored = sink.commit(completed)?;
        leader.finish(if stored {
            super::peer_cache::FillOutcome::Ready
        } else {
            super::peer_cache::FillOutcome::Uncached
        });
        Ok(())
    }

    fn fanout_private_objects(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        context_deadline: Option<Instant>,
    ) -> Result<bool> {
        let (Some(fetcher), Some(cache)) = (&self.fetcher, &self.peer_cache) else {
            return Ok(false);
        };
        if batch.ops.len() < 2 || batch.ops.iter().any(|op| op.length == 0) {
            return Ok(false);
        }
        // Static rejection avoids dispatching obviously unsupported objects;
        // the child also enforces stream-only execution for dynamic decline.
        if !self.transfer.supports_verified_object_sink()
            || batch.ops.iter().any(|op| {
                let Some(set) = sources.iter().find(|set| set.chunk_id == op.chunk_id) else {
                    return true;
                };
                let Some(chunk) = &set.chunk else {
                    return true;
                };
                chunk.is_segment_crc32c()
                    || chunk.id != op.chunk_id
                    || chunk.length == 0
                    || chunk.length > crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
                    || !range_covered(&set.allowed_ranges, 0, chunk.length)
                    || (crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64)
                        .checked_add(op.length)
                        .is_none_or(|bytes| bytes > self.config.max_inflight_bytes)
                    || !set
                        .sources
                        .iter()
                        .any(|source| self.peer_cache_source_eligible(batch, chunk, source))
            })
        {
            return Ok(false);
        }
        // Completed handles retain only request scratch or a bounded 512-byte
        // failure until this group joins them. Charge their vector/control too.
        let Some(_group_metadata) = cache
            .proof_budget()
            .try_reserve(batch.ops.len() * 1024 + 4096)
        else {
            fetcher.event("skip_budget");
            return Ok(false);
        };
        let authority = self.capture_native_read_authority()?;
        // Convert the original grants once; waking or advancing to another
        // object must never restart this batch's admission lifetime.
        let admitted_at = Instant::now();
        let admitted_wall = now_unix_ms();
        let deadline = sources
            .iter()
            .flat_map(|set| &set.sources)
            .filter(|source| self.source_matches(batch, source))
            .filter_map(|source| {
                admitted_at.checked_add(Duration::from_millis(
                    source
                        .read_grant
                        .expires_at_unix_ms
                        .saturating_sub(admitted_wall),
                ))
            })
            .chain(authority.expires_at())
            .chain(context_deadline)
            .min();
        let generation = self
            .source_cache
            .lock()
            .map_err(|_| unavailable("DFS source cache poisoned"))?
            .generation;
        let mut tasks = Vec::with_capacity(batch.ops.len());
        let mut fallback = false;
        let mut admission_failure = None;
        for op in &batch.ops {
            let Some(set) = sources.iter().find(|set| set.chunk_id == op.chunk_id) else {
                fallback = true;
                break;
            };
            let Some(version) = batch.file_version_id.as_ref() else {
                fallback = true;
                break;
            };
            let empty_key = (
                FileVersionId::new(""),
                LayoutRootId::new(""),
                ChunkId::new(""),
            );
            let charge = SourceCache::retained_bytes(&empty_key, set)
                .saturating_mul(2)
                .saturating_add(
                    4 * (version.0.capacity()
                        + batch.layout_root_id.0.capacity()
                        + op.chunk_id.0.capacity()),
                )
                .saturating_add(
                    std::mem::size_of::<Self>() + std::mem::size_of::<ReadBatch>() + 64 * 1024,
                )
                .saturating_add(self.namespace_id.0.capacity() + self.local_node_id.capacity());
            let permit = match fetcher.reserve_foreground(charge, deadline, || {
                self.check_native_read_authority(&authority)?;
                if sources.iter().any(|set| {
                    !set.sources.iter().any(|source| {
                        self.source_matches(batch, source)
                            && (authority.epoch == 0
                                || source.read_grant.caller_node_epoch == authority.epoch)
                    })
                }) {
                    return Err(external_denied("DFS queued fetch grant expired"));
                }
                Ok(())
            }) {
                Ok(Some(permit)) => permit,
                Ok(None) => {
                    fetcher.event("skip_budget");
                    fallback = true;
                    break;
                }
                Err(error) => {
                    admission_failure = Some(error);
                    break;
                }
            };
            let reader = self.task_reader();
            let selected = set.clone();
            let mut operation = op.clone();
            operation.output_offset = 0;
            let batch = ReadBatch {
                file_version_id: Some(version.clone()),
                layout_root_id: batch.layout_root_id.clone(),
                ops: vec![operation],
                lookahead: Vec::new(),
            };
            // Same two representations as the existing complex-batch fallback:
            // private final response plus total requested bytes of attempt scratch.
            let length = op.length as usize; // ReadBatch validated against usize and the 8MiB limit.
            tasks.push(fetcher.spawn_foreground(permit, move || {
                let mut scratch = vec![0; length];
                if reader.read_native_peer_cache_mode(
                    &batch,
                    &[selected],
                    &mut scratch,
                    true,
                    true,
                )? {
                    Ok(Some(scratch))
                } else {
                    Ok(None)
                }
            }));
        }
        // Join every task, including after the first failure. No partial result
        // reaches the caller's final buffer until all joins and checks succeed.
        let mut results = Vec::with_capacity(tasks.len());
        let mut failure = None;
        for task in tasks {
            match fetcher.block_on(task) {
                Ok(Ok(Some(bytes))) => results.push(bytes),
                Ok(Ok(None)) => fallback = true,
                Ok(Err(error)) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(unavailable(format!("DFS fetch task failed: {error}")));
                    }
                }
            }
        }
        if let Some(error) = failure.or(admission_failure) {
            return Err(error);
        }
        if fallback {
            drop(results);
            return Ok(false);
        }
        self.check_native_read_authority(&authority)?;
        if self
            .source_cache
            .lock()
            .map_err(|_| unavailable("DFS source cache poisoned"))?
            .generation
            != generation
        {
            return Err(unavailable(
                "DFS fetch source context changed before delivery",
            ));
        }
        for set in sources {
            if !set
                .sources
                .iter()
                .any(|source| self.source_matches(batch, source))
            {
                return Err(external_denied("DFS fetch grant expired before delivery"));
            }
        }
        for (op, bytes) in batch.ops.iter().zip(results) {
            let end = op.output_offset + bytes.len();
            out[op.output_offset..end].copy_from_slice(&bytes);
            self.local.record_read_copy("attempt_to_staging", op.length);
        }
        fetcher.event("joined");
        Ok(true)
    }

    fn read_remote_batch(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        final_owned: bool,
        deadline: Option<Instant>,
    ) -> Result<()> {
        if final_owned && self.fanout_private_objects(batch, sources, out, deadline)? {
            return Ok(());
        }
        if self.read_native_peer_cache(batch, sources, out, final_owned)? {
            return Ok(());
        }
        self.read_remote_ranges(batch, sources, out, final_owned)
    }

    fn read_remote_ranges(
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
                    self.transfer.read_crc32c_range(&attempt, chunk, target)
                } else {
                    self.transfer.read_ranges(&attempt, target)
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
                        if matches!(
                            error.kind(),
                            afs_error::ErrorKind::PermissionDenied
                                | afs_error::ErrorKind::Unauthenticated
                        ) {
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
                                    .read_ranges(&single_batch, &mut single_scratch)
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
                                        if matches!(
                                            error.kind(),
                                            afs_error::ErrorKind::PermissionDenied
                                                | afs_error::ErrorKind::Unauthenticated
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

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub struct UnimplementedReadSourceProvider;

impl ReadSourceProvider for UnimplementedReadSourceProvider {
    fn sources_for(&self, _request: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
        Err(Error::coded(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "DFS read source lookup is not wired",
        ))
    }
}

pub struct UnimplementedChunkTransfer;

impl ChunkTransfer for UnimplementedChunkTransfer {
    fn read_ranges(&self, _batch: &PeerReadBatch, _out: &mut [u8]) -> Result<()> {
        Err(Error::coded(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "DFS peer range read is not wired",
        ))
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_INVALID, message)
}

fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_UNAVAILABLE, message)
}

fn external_denied(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_PERMISSION_DENIED, message)
}

fn corrupt(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_CORRUPT_DATA, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dfs::{CopyId, CopyState, DfsReadGrant, OperationId},
        node::chunk::{ChunkBuilder, ChunkStore, StagedChunk},
    };
    use std::{
        sync::{Arc, Condvar},
        time::Duration,
    };

    #[derive(Default)]
    struct StaticSources {
        reply: Mutex<DfsChunkSourcesReply>,
        calls: Mutex<u64>,
    }

    impl StaticSources {
        fn new(reply: DfsChunkSourcesReply) -> Self {
            Self {
                reply: Mutex::new(reply),
                calls: Mutex::new(0),
            }
        }
    }

    impl ReadSourceProvider for StaticSources {
        fn sources_for(&self, _request: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
            *self.calls.lock().unwrap() += 1;
            Ok(self.reply.lock().unwrap().clone())
        }
    }

    struct StaticTransfer {
        bytes: Vec<u8>,
        fail_first: Mutex<bool>,
    }

    impl ChunkTransfer for StaticTransfer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            let mut fail_first = self.fail_first.lock().unwrap();
            if *fail_first {
                *fail_first = false;
                return Err(unavailable("injected peer failure"));
            }
            for item in &batch.operations {
                let op = &item.op;
                let length = usize::try_from(op.length).unwrap();
                out[op.output_offset..op.output_offset + length]
                    .copy_from_slice(&self.bytes[..length]);
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct PipelineProbe {
        state: Mutex<PipelineProbeState>,
        wake: Condvar,
    }

    #[derive(Default)]
    struct PipelineProbeState {
        head_hash_started: bool,
        head_hash_released: bool,
        tail_read_before_head_release: bool,
        hash_finished: bool,
    }

    impl PipelineProbe {
        fn mark_head_hash_started_and_wait(&self) {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut state = self.state.lock().unwrap();
            state.head_hash_started = true;
            self.wake.notify_all();
            while !state.head_hash_released {
                let now = Instant::now();
                assert!(now < deadline, "tail read did not release head hash");
                let timeout = deadline.saturating_duration_since(now);
                let (next, _) = self.wake.wait_timeout(state, timeout).unwrap();
                state = next;
            }
        }

        fn mark_tail_read_and_release_head(&self) {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut state = self.state.lock().unwrap();
            while !state.head_hash_started {
                let now = Instant::now();
                assert!(now < deadline, "head hash did not start before tail read");
                let timeout = deadline.saturating_duration_since(now);
                let (next, _) = self.wake.wait_timeout(state, timeout).unwrap();
                state = next;
            }
            state.tail_read_before_head_release = !state.head_hash_released;
            state.head_hash_released = true;
            self.wake.notify_all();
        }

        fn release_head_hash(&self) {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut state = self.state.lock().unwrap();
            while !state.head_hash_started {
                let now = Instant::now();
                assert!(now < deadline, "head hash did not start before release");
                let timeout = deadline.saturating_duration_since(now);
                let (next, _) = self.wake.wait_timeout(state, timeout).unwrap();
                state = next;
            }
            state.head_hash_released = true;
            self.wake.notify_all();
        }

        fn mark_hash_finished(&self) {
            let mut state = self.state.lock().unwrap();
            state.hash_finished = true;
            self.wake.notify_all();
        }

        fn tail_read_before_head_release(&self) -> bool {
            self.state.lock().unwrap().tail_read_before_head_release
        }

        fn hash_finished(&self) -> bool {
            self.state.lock().unwrap().hash_finished
        }
    }

    fn two_worker_pool() -> rayon_core::ThreadPool {
        rayon_core::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap()
    }

    #[test]
    fn ready_pipeline_reads_tail_while_head_hash_is_in_flight() {
        let payload = (0..(2 * 1024 * 1024 + 1024))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-overlap"), payload.clone());
        let pool = two_worker_pool();
        let probe = Arc::new(PipelineProbe::default());
        let loader_probe = probe.clone();
        let hash_probe = probe.clone();

        let verified = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, _tail| {
                if offset == 1024 * 1024 {
                    loader_probe.mark_tail_read_and_release_head();
                }
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Ok(()),
            move |offset| {
                if offset == 0 {
                    hash_probe.mark_head_hash_started_and_wait();
                }
            },
        )
        .unwrap();

        assert_eq!(verified.as_slice(), staged.bytes());
        assert!(
            probe.tail_read_before_head_release(),
            "tail read must start while the head hash task is still in flight"
        );
    }

    #[test]
    fn ready_pipeline_overlaps_each_hash_with_next_segment_read() {
        const MIB: usize = 1024 * 1024;
        let payload = (0..(4 * MIB))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let pool = two_worker_pool();

        for target_offset in [0, MIB, 2 * MIB] {
            let staged = StagedChunk::new(
                OperationId::new(format!("ready-overlap-{target_offset}")),
                payload.clone(),
            );
            let probe = Arc::new(PipelineProbe::default());
            let loader_probe = probe.clone();
            let hash_probe = probe.clone();

            let verified = VerifiedExternalChunk::load_with_rayon_pool(
                &staged.chunk,
                vec![0; payload.len()],
                &pool,
                0,
                |offset, segment, _tail| {
                    if offset == (target_offset + MIB) as u64 {
                        loader_probe.mark_tail_read_and_release_head();
                    }
                    let offset = usize::try_from(offset).unwrap();
                    segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                    Ok(())
                },
                || Ok(()),
                move |offset| {
                    if offset == target_offset as u64 {
                        hash_probe.mark_head_hash_started_and_wait();
                    }
                },
            )
            .unwrap();

            assert_eq!(verified.as_slice(), staged.bytes());
            assert!(
                probe.tail_read_before_head_release(),
                "read of the next segment must overlap blocked hash at offset {target_offset}"
            );
        }
    }

    #[test]
    fn ready_pipeline_waits_for_head_hash_after_next_read_error() {
        let payload = (0..(2 * 1024 * 1024 + 1024))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-tail-error"), payload.clone());
        let pool = two_worker_pool();
        let probe = Arc::new(PipelineProbe::default());
        let loader_probe = probe.clone();
        let hash_probe = probe.clone();

        let error = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, _tail| {
                if offset == 1024 * 1024 {
                    loader_probe.release_head_hash();
                    return Err(unavailable("injected next read failure"));
                }
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Ok(()),
            move |offset| {
                if offset == 0 {
                    hash_probe.mark_head_hash_started_and_wait();
                    hash_probe.mark_hash_finished();
                }
            },
        )
        .unwrap_err();

        assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
        assert!(
            probe.hash_finished(),
            "blocked head hash worker must be released before the next read error returns"
        );
    }

    #[test]
    fn ready_pipeline_loads_four_one_mib_segments_and_keeps_allocation() {
        const MIB: usize = 1024 * 1024;
        let payload = (0..(4 * MIB))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-four-segments"), payload.clone());
        let pool = two_worker_pool();
        let buffer = vec![0; payload.len()];
        let buffer_ptr = buffer.as_ptr();
        let buffer_capacity = buffer.capacity();
        let loads = Arc::new(Mutex::new(Vec::new()));
        let observed_loads = loads.clone();

        let verified = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            buffer,
            &pool,
            0,
            |offset, segment, tail| {
                observed_loads
                    .lock()
                    .unwrap()
                    .push((offset, segment.len(), tail));
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Ok(()),
            |_| {},
        )
        .unwrap();

        assert_eq!(verified.as_slice(), staged.bytes());
        assert_eq!(verified.as_slice().as_ptr(), buffer_ptr);
        assert_eq!(verified.allocation_bytes(), buffer_capacity as u64);
        assert_eq!(
            loads.lock().unwrap().as_slice(),
            &[
                (0, MIB, false),
                (MIB as u64, MIB, false),
                ((2 * MIB) as u64, MIB, false),
                ((3 * MIB) as u64, MIB, true),
            ]
        );
    }

    #[test]
    fn ready_pipeline_accepts_empty_chunk_without_segment_loads() {
        let staged = StagedChunk::new(OperationId::new("ready-empty"), Vec::new());
        let pool = two_worker_pool();
        let finalized = Arc::new(Mutex::new(false));
        let finalized_probe = finalized.clone();

        let verified = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            Vec::new(),
            &pool,
            0,
            |_offset, _segment, _tail| panic!("empty ready chunk must not load a segment"),
            || {
                *finalized_probe.lock().unwrap() = true;
                Ok(())
            },
            |_| {},
        )
        .unwrap();

        assert!(verified.as_slice().is_empty());
        assert!(*finalized.lock().unwrap());
    }

    #[test]
    fn ready_pipeline_loads_short_tail_after_full_segment() {
        const MIB: usize = 1024 * 1024;
        let payload = (0..(MIB + 123))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-short-tail"), payload.clone());
        let pool = two_worker_pool();
        let loads = Arc::new(Mutex::new(Vec::new()));
        let observed_loads = loads.clone();

        let verified = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, tail| {
                observed_loads
                    .lock()
                    .unwrap()
                    .push((offset, segment.len(), tail));
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Ok(()),
            |_| {},
        )
        .unwrap();

        assert_eq!(verified.as_slice(), staged.bytes());
        assert_eq!(
            loads.lock().unwrap().as_slice(),
            &[(0, MIB, false), (MIB as u64, 123, true)]
        );
    }

    #[test]
    fn ready_pipeline_rejects_corrupt_middle_and_final_segments() {
        const MIB: usize = 1024 * 1024;
        let payload = (0..(4 * MIB))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let pool = two_worker_pool();

        for corrupt_offset in [MIB, 3 * MIB] {
            let staged = StagedChunk::new(
                OperationId::new(format!("ready-corrupt-{corrupt_offset}")),
                payload.clone(),
            );
            let error = VerifiedExternalChunk::load_with_rayon_pool(
                &staged.chunk,
                vec![0; payload.len()],
                &pool,
                0,
                |offset, segment, _tail| {
                    let offset = usize::try_from(offset).unwrap();
                    segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                    if offset == corrupt_offset {
                        segment[0] ^= 0x55;
                    }
                    Ok(())
                },
                || Ok(()),
                |_| {},
            )
            .unwrap_err();

            assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        }
    }

    #[test]
    fn ready_pipeline_joins_accepted_hash_before_late_read_failure_returns() {
        const MIB: usize = 1024 * 1024;
        let payload = (0..(4 * MIB))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-late-read-failure"), payload.clone());
        let pool = two_worker_pool();
        let probe = Arc::new(PipelineProbe::default());
        let loader_probe = probe.clone();
        let hash_probe = probe.clone();

        let error = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, _tail| {
                if offset == (3 * MIB) as u64 {
                    loader_probe.mark_tail_read_and_release_head();
                    return Err(unavailable("injected late ready read failure"));
                }
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Ok(()),
            move |offset| {
                if offset == (2 * MIB) as u64 {
                    hash_probe.mark_head_hash_started_and_wait();
                    hash_probe.mark_hash_finished();
                }
            },
        )
        .unwrap_err();

        assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
        assert!(
            probe.tail_read_before_head_release(),
            "failing read must wait for the middle hash worker to start before releasing it"
        );
        assert!(
            probe.hash_finished(),
            "accepted hash worker must be released and joined before the late read error returns"
        );
    }

    #[test]
    fn ready_pipeline_rejects_reader_growth_after_exact_length() {
        let payload = (0..(2 * 1024 * 1024 + 1024))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-eof-probe"), payload.clone());
        let pool = two_worker_pool();

        let error = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, _tail| {
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Err(corrupt("S3 cache entry exceeds expected length")),
            |_| {},
        )
        .unwrap_err();

        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    }

    fn source(copy: &str, chunk_id: &ChunkId, node_id: &str) -> SourceCandidate {
        SourceCandidate {
            copy_id: CopyId::new(copy),
            chunk_id: chunk_id.clone(),
            role: CopyRole::DurableReplica,
            state: CopyState::Ready,
            location: CopyLocation::Node {
                node_id: node_id.into(),
                node_epoch: 1,
                device_id: "local-0".into(),
                device_epoch: 1,
                catalog_revision: 1,
            },
            data_endpoint: Some(format!("http://{node_id}")),
            load_hint: 0,
            read_grant: DfsReadGrant {
                namespace_id: crate::dfs::NamespaceId::new("default"),
                file_version_id: FileVersionId::new("version"),
                layout_root_id: LayoutRootId::new("layout"),
                caller_node_id: "node-a".into(),
                caller_node_epoch: 1,
                expires_at_unix_ms: u64::MAX,
                fence: 1,
                token: "test-token".into(),
            },
        }
    }

    fn put_local_chunk(local: &LocalChunkStore, operation: &str, bytes: &[u8]) -> ChunkId {
        let mut builder = ChunkBuilder::default();
        builder.replace(bytes.to_vec());
        let staged = builder.stage(OperationId::new(operation));
        let chunk_id = staged.chunk.id.clone();
        local.put(staged).unwrap();
        chunk_id
    }

    #[test]
    fn bounded_read_cache_replacement_expiry_and_clock_rollover_preserve_lru() {
        let mut cache = BoundedLru::new(2);
        cache.insert("a", 1);
        cache.insert("b", 2);
        assert_eq!(cache.get(&"a"), Some(&1));
        cache.insert("a", 3);
        assert_eq!(
            cache.len(),
            2,
            "replacement must not evict an unrelated entry"
        );
        assert!(cache.contains_key(&"b"));
        cache.clock = u64::MAX;
        assert_eq!(cache.get(&"a"), Some(&3));
        cache.insert("c", 4);
        assert_eq!(cache.len(), 2);
        assert!(!cache.contains_key(&"b"));
        assert_eq!(cache.remove(&"a"), Some(3));
        cache.retain(|_, value| *value != 4);
        assert!(cache.is_empty());
        assert!(
            cache.order.is_empty(),
            "retired entries must not leave recency keys"
        );
        cache.insert("d", 5);
        assert_eq!(cache.get(&"d"), Some(&5));
    }

    fn source_cache_lru_batch(chunk_id: ChunkId) -> ReadBatch {
        ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id,
                chunk_offset: 0,
                length: 1,
                output_offset: 0,
            }],
        }
    }

    fn source_cache_lru_set(index: usize) -> ChunkSources {
        let chunk_id = ChunkId::new(format!("source-cache-lru-{index}"));
        ChunkSources {
            allowed_ranges: Vec::new(),
            chunk: None,
            sources: vec![source("source-cache-lru-copy", &chunk_id, "node-b")],
            chunk_id,
        }
    }

    #[test]
    fn p2_r1_source_cache_default_maximum_is_five_seconds() {
        assert_eq!(
            DfsReadConfig::default().source_cache_ttl,
            Duration::from_millis(5_000),
            "the source cache maximum must use the approved R-1 default"
        );
    }

    #[test]
    fn p2_r1_source_cache_short_grant_is_usable_but_not_retained() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let mut sources = source_cache_lru_set(0);
        // The current source is still authorized, but cannot cover the fixed
        // 1000 ms safety margin for a future cached read.
        sources.sources[0].read_grant.expires_at_unix_ms = now_unix_ms().saturating_add(500);
        let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![sources.clone()],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            provider.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );

        let resolved = engine
            .sources_for(&source_cache_lru_batch(sources.chunk_id.clone()))
            .unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].chunk_id, sources.chunk_id);
        assert_eq!(*provider.calls.lock().unwrap(), 1);
        assert!(
            engine.source_cache.lock().unwrap().is_empty(),
            "a grant with less than 1000 ms remaining must not enter the source cache"
        );
    }

    #[test]
    fn p2_r1_source_cache_oversized_metadata_is_usable_but_not_retained() {
        let temp = tempfile::tempdir().unwrap();
        let mut sources = source_cache_lru_set(0);
        // Use actual content length so cloning cannot shrink the oversized
        // allocation below the approved 4 MiB resident metadata limit.
        sources.sources[0].read_grant.token = "t".repeat(4 * 1024 * 1024 + 1);
        let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![sources.clone()],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            provider.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );

        let resolved = engine
            .sources_for(&source_cache_lru_batch(sources.chunk_id.clone()))
            .unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].chunk_id, sources.chunk_id);
        assert_eq!(*provider.calls.lock().unwrap(), 1);
        assert!(
            engine.source_cache.lock().unwrap().is_empty(),
            "current authorized metadata may be used, but an oversized item must not be cached"
        );
    }

    #[derive(Default)]
    struct R1Sources {
        requests: Mutex<Vec<DfsChunkSourcesRequest>>,
        errors: Mutex<std::collections::VecDeque<Error>>,
        hooks: Mutex<std::collections::VecDeque<Box<dyn FnOnce() + Send>>>,
        node_epoch: std::sync::atomic::AtomicU64,
        device_epoch: std::sync::atomic::AtomicU64,
    }

    impl ReadSourceProvider for R1Sources {
        fn sources_for(&self, request: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
            let call = {
                let mut requests = self.requests.lock().unwrap();
                requests.push(request.clone());
                requests.len()
            };
            let hook = self.hooks.lock().unwrap().pop_front();
            if let Some(hook) = hook {
                hook();
            }
            if let Some(error) = self.errors.lock().unwrap().pop_front() {
                return Err(error);
            }
            Ok(DfsChunkSourcesReply {
                revision: call as u64,
                chunks: request
                    .chunk_ids
                    .into_iter()
                    .enumerate()
                    .map(|(index, chunk_id)| {
                        let mut candidate = source("r1-copy", &chunk_id, "node-b");
                        if let CopyLocation::Node {
                            node_epoch,
                            device_epoch,
                            ..
                        } = &mut candidate.location
                        {
                            *node_epoch = self.node_epoch.load(Ordering::Relaxed).max(1);
                            *device_epoch = self.device_epoch.load(Ordering::Relaxed).max(1);
                        }
                        candidate.read_grant.file_version_id = request.file_version_id.clone();
                        candidate.read_grant.layout_root_id = request.layout_root_id.clone();
                        candidate.read_grant.namespace_id = request.namespace_id.clone();
                        candidate.read_grant.caller_node_id = request.caller_id.clone();
                        // Different signed copy fences within one reply are legal.
                        candidate.read_grant.fence = index as u64 + 1;
                        candidate.read_grant.token = format!("r1-call-{call}");
                        ChunkSources {
                            chunk_id,
                            chunk: None,
                            sources: vec![candidate],
                            allowed_ranges: vec![(7, 1)],
                        }
                    })
                    .collect(),
            })
        }
    }

    fn r1_engine(
        temp: &tempfile::TempDir,
        provider: Arc<R1Sources>,
        config: DfsReadConfig,
    ) -> Arc<DfsReadEngine> {
        Arc::new(DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            provider,
            Arc::new(UnimplementedChunkTransfer),
            config,
        ))
    }

    #[test]
    fn p2_r1_metadata_lookahead_resolves_sixteen_objects_without_body_operations() {
        let temp = tempfile::tempdir().unwrap();
        let provider = Arc::new(R1Sources::default());
        let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
        let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
        batch.lookahead = (1..16)
            .map(|index| source_cache_lru_set(index).chunk_id)
            .collect();
        let before = Instant::now();
        let result = engine.sources_for(&batch).unwrap();
        assert_eq!(batch.ops.len(), 1);
        assert_eq!(
            result.len(),
            1,
            "advisory metadata is not returned as current body work"
        );
        assert_eq!(
            result[0].allowed_ranges,
            vec![(7, 1)],
            "batching must not widen grants"
        );
        assert_eq!(provider.requests.lock().unwrap()[0].chunk_ids.len(), 16);
        for index in 0..16 {
            let single = source_cache_lru_batch(source_cache_lru_set(index).chunk_id);
            assert_eq!(engine.sources_for(&single).unwrap().len(), 1);
        }
        assert_eq!(provider.requests.lock().unwrap().len(), 1);
        let mut cache = engine.source_cache.lock().unwrap();
        assert_eq!(cache.len(), 16);
        assert!(
            cache
                .get(&source_cache_lru_key(&batch.ops[0].chunk_id))
                .unwrap()
                .expires_at
                >= before + Duration::from_secs(4)
        );
    }

    #[test]
    fn p2_r1_zero_options_and_bounded_ids_do_not_add_speculative_work() {
        for mode in 0..3 {
            let temp = tempfile::tempdir().unwrap();
            let provider = Arc::new(R1Sources::default());
            let mut config = DfsReadConfig::default();
            match mode {
                0 => config.source_cache_ttl = Duration::ZERO,
                1 => config.source_cache_bytes = 0,
                _ => config.source_lookahead_chunks = 0,
            }
            let engine = r1_engine(&temp, provider.clone(), config);
            let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
            batch.lookahead = vec![source_cache_lru_set(1).chunk_id];
            engine.sources_for(&batch).unwrap();
            assert_eq!(provider.requests.lock().unwrap()[0].chunk_ids.len(), 1);
            let cache = engine.source_cache.lock().unwrap();
            if mode < 2 {
                assert!(cache.is_empty());
                assert_eq!(
                    cache.entries.entries.capacity(),
                    0,
                    "disabled must not retain a hash table allocation"
                );
                assert_eq!(cache.bytes(), 0);
            } else {
                assert_eq!(
                    cache.len(),
                    1,
                    "lookahead=0 must not disable mandatory source caching"
                );
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let provider = Arc::new(R1Sources::default());
        let engine = r1_engine(
            &temp,
            provider.clone(),
            DfsReadConfig {
                source_lookahead_chunks: MAX_DFS_READ_OPS,
                ..DfsReadConfig::default()
            },
        );
        let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
        batch.lookahead = (0..MAX_DFS_READ_OPS)
            .map(|index| source_cache_lru_set(index).chunk_id)
            .collect();
        engine.sources_for(&batch).unwrap();
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests[0].chunk_ids.len(), MAX_DFS_READ_OPS);
        assert_eq!(
            requests[0].chunk_ids[0], batch.ops[0].chunk_id,
            "mandatory first"
        );
        drop(requests);
        batch.lookahead.push(ChunkId::new("overflow"));
        assert!(engine.sources_for(&batch).is_err());
        batch.lookahead = vec![ChunkId::new("")];
        assert!(engine.sources_for(&batch).is_err());
        assert_eq!(provider.requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn p2_r1_only_precise_batch_invalidity_revalidates_mandatory_once() {
        for code in [
            afs_error::META_CATALOG_INVALID_REQUEST,
            afs_error::META_DFS_CONFLICT,
            afs_error::IO_PERMISSION_DENIED,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let provider = Arc::new(R1Sources::default());
            provider
                .errors
                .lock()
                .unwrap()
                .push_back(Error::coded(code, "batch failure"));
            let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
            let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
            batch.lookahead = vec![source_cache_lru_set(1).chunk_id];
            let result = engine.sources_for(&batch);
            let requests = provider.requests.lock().unwrap();
            if code == afs_error::META_CATALOG_INVALID_REQUEST {
                assert_eq!(result.unwrap().len(), 1);
                assert_eq!(requests.len(), 2);
                assert_eq!(requests[1].chunk_ids, vec![batch.ops[0].chunk_id.clone()]);
            } else {
                assert_eq!(result.unwrap_err().code(), code);
                assert_eq!(
                    requests.len(),
                    1,
                    "authorization/session failure cannot be retried as advisory"
                );
            }
        }
        for advisory in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let provider = Arc::new(R1Sources::default());
            for _ in 0..2 {
                provider.errors.lock().unwrap().push_back(Error::coded(
                    afs_error::META_CATALOG_INVALID_REQUEST,
                    "mandatory missing",
                ));
            }
            let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
            let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
            if advisory {
                batch.lookahead.push(source_cache_lru_set(1).chunk_id);
            }
            assert_eq!(
                engine.sources_for(&batch).unwrap_err().code(),
                afs_error::META_CATALOG_INVALID_REQUEST
            );
            assert_eq!(
                provider.requests.lock().unwrap().len(),
                if advisory { 2 } else { 1 }
            );
            assert!(engine.source_cache.lock().unwrap().is_empty());
        }
    }

    #[test]
    fn p2_r1_oversized_advisory_is_skipped_without_rejecting_current_source() {
        let temp = tempfile::tempdir().unwrap();
        let provider = Arc::new(R1Sources::default());
        let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
        let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
        batch
            .lookahead
            .push(ChunkId::new("x".repeat(MAX_SOURCE_LOOKAHEAD_ID_BYTES + 1)));
        let result = engine.sources_for(&batch).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(batch.ops.len(), 1);
        assert_eq!(
            provider.requests.lock().unwrap()[0].chunk_ids,
            vec![batch.ops[0].chunk_id.clone()]
        );
    }

    #[test]
    fn p2_r1_invalidation_race_discards_old_hits_and_revalidates_all_current_ids() {
        for same_scope in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let provider = Arc::new(R1Sources::default());
            let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
            let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
            engine.sources_for(&batch).unwrap();
            batch.ops.push(ChunkReadOp {
                chunk_id: source_cache_lru_set(1).chunk_id,
                chunk_offset: 0,
                length: 1,
                output_offset: 1,
            });
            let mut invalidated = batch.clone();
            if !same_scope {
                invalidated.file_version_id = Some(FileVersionId::new("other-version"));
            }
            let weak = Arc::downgrade(&engine);
            provider.hooks.lock().unwrap().push_back(Box::new(move || {
                weak.upgrade()
                    .unwrap()
                    .invalidate_native_batch_authority(&invalidated)
            }));
            let result = engine.sources_for(&batch).unwrap();
            let requests = provider.requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            assert_eq!(
                requests[1].chunk_ids.len(),
                1,
                "first attempt only had one miss"
            );
            assert_eq!(
                requests[2].chunk_ids.len(),
                2,
                "retry must reauthorize the old cache hit too"
            );
            assert_eq!(result.len(), 2);
            assert!(
                result
                    .iter()
                    .all(|set| set.sources[0].read_grant.token == "r1-call-3")
            );
        }
    }

    #[test]
    fn p2_r1_repeated_invalidation_is_bounded_unavailable_not_permission_denied() {
        let temp = tempfile::tempdir().unwrap();
        let provider = Arc::new(R1Sources::default());
        let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
        let batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
        for _ in 0..2 {
            let weak = Arc::downgrade(&engine);
            let batch = batch.clone();
            provider.hooks.lock().unwrap().push_back(Box::new(move || {
                weak.upgrade()
                    .unwrap()
                    .invalidate_native_batch_authority(&batch)
            }));
        }
        assert_eq!(
            engine.sources_for(&batch).unwrap_err().kind(),
            afs_error::ErrorKind::Unavailable
        );
        assert_eq!(provider.requests.lock().unwrap().len(), 2);
        assert!(engine.source_cache.lock().unwrap().is_empty());
    }

    #[test]
    fn p2_r1_observed_remote_epochs_retire_related_sources_only() {
        for node_restart in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let first = source_cache_lru_set(0);
            let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
                revision: 1,
                chunks: vec![first.clone()],
            }));
            let engine = DfsReadEngine::new(
                crate::dfs::NamespaceId::new("default"),
                "node-a".into(),
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                provider.clone(),
                Arc::new(UnimplementedChunkTransfer),
                DfsReadConfig::default(),
            );
            engine
                .sources_for(&source_cache_lru_batch(first.chunk_id.clone()))
                .unwrap();
            let mut unrelated = source_cache_lru_set(80);
            unrelated.sources[0] = source("other-copy", &unrelated.chunk_id, "node-c");
            unrelated.sources[0].read_grant.file_version_id = FileVersionId::new("other-version");
            let unrelated_key = (
                FileVersionId::new("other-version"),
                LayoutRootId::new("layout"),
                unrelated.chunk_id.clone(),
            );
            engine.source_cache.lock().unwrap().insert(
                unrelated_key.clone(),
                CachedSources {
                    expires_at: Instant::now() + Duration::from_secs(5),
                    sources: unrelated,
                },
            );
            let mut fresh = source_cache_lru_set(1);
            if let CopyLocation::Node {
                node_epoch,
                device_epoch,
                ..
            } = &mut fresh.sources[0].location
            {
                if node_restart {
                    *node_epoch = 2;
                } else {
                    *device_epoch = 2;
                }
            }
            *provider.reply.lock().unwrap() = DfsChunkSourcesReply {
                revision: 2,
                chunks: vec![fresh.clone()],
            };
            engine
                .sources_for(&source_cache_lru_batch(fresh.chunk_id.clone()))
                .unwrap();
            let cache = engine.source_cache.lock().unwrap();
            assert!(!cache.contains_key(&source_cache_lru_key(&first.chunk_id)));
            assert!(cache.contains_key(&source_cache_lru_key(&fresh.chunk_id)));
            assert!(
                cache.contains_key(&unrelated_key),
                "other Node and version must remain cached"
            );
        }
    }

    #[test]
    fn p2_r1_new_remote_epoch_discards_already_resolved_old_cache_hits() {
        for node_restart in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let provider = Arc::new(R1Sources::default());
            let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
            let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
            engine.sources_for(&batch).unwrap();
            if node_restart {
                provider.node_epoch.store(2, Ordering::Relaxed);
            } else {
                provider.device_epoch.store(2, Ordering::Relaxed);
            }
            batch.ops.push(ChunkReadOp {
                chunk_id: source_cache_lru_set(1).chunk_id,
                chunk_offset: 0,
                length: 1,
                output_offset: 1,
            });
            let result = engine.sources_for(&batch).unwrap();
            assert_eq!(result.len(), 2);
            assert!(
                result.iter().flat_map(|set| &set.sources).all(|source| {
                    matches!(&source.location, CopyLocation::Node { node_epoch, device_epoch, .. }
                    if if node_restart { *node_epoch == 2 } else { *device_epoch == 2 })
                }),
                "a newly observed remote identity must also discard earlier cache-hit clones"
            );
            let requests = provider.requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            assert_eq!(requests[1].chunk_ids.len(), 1);
            assert_eq!(requests[2].chunk_ids.len(), 2);
            assert!(
                result
                    .iter()
                    .all(|set| set.sources[0].read_grant.token == "r1-call-3")
            );
        }
    }

    #[test]
    fn p2_r1_source_deadline_consumes_rpc_time_and_hits_do_not_renew_it() {
        let temp = tempfile::tempdir().unwrap();
        let engine = r1_engine(
            &temp,
            Arc::new(R1Sources::default()),
            DfsReadConfig::default(),
        );
        let sent = Instant::now() - Duration::from_secs(2);
        let wall = now_unix_ms().saturating_sub(2_000);
        let mut set = source_cache_lru_set(0);
        set.sources[0].read_grant.expires_at_unix_ms = wall + 4_000;
        assert_eq!(
            engine.source_deadline(&set, sent, wall),
            Some(sent + Duration::from_secs(3))
        );
        set.sources[0].read_grant.expires_at_unix_ms = u64::MAX;
        assert_eq!(
            engine.source_deadline(&set, sent, wall),
            Some(sent + Duration::from_secs(5))
        );
        set.sources[0].read_grant.expires_at_unix_ms = wall + 1_000;
        assert!(engine.source_deadline(&set, sent, wall).is_none());
    }

    #[test]
    fn p2_r1_source_cache_capacity_and_metrics_follow_real_retained_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::read_work::ReadWorkMetrics::register(&registry).unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_read_work_metrics(metrics),
        );
        let provider = Arc::new(R1Sources::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            provider,
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let gauge = |resource: &str| -> usize {
            registry
                .gather()
                .into_iter()
                .find(|family| family.name() == "afs_dfs_source_cache_resources")
                .unwrap()
                .get_metric()
                .iter()
                .find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "resource" && label.value() == resource)
                })
                .unwrap()
                .get_gauge()
                .get_value() as usize
        };
        assert_eq!(gauge("entries"), 0);
        let batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
        engine.sources_for(&batch).unwrap();
        let bytes = engine.source_cache.lock().unwrap().bytes();
        assert_eq!(gauge("entries"), 1);
        assert_eq!(gauge("bytes"), bytes);
        assert_eq!(gauge("peak_bytes"), bytes);
        assert_eq!(gauge("limit_bytes"), MAX_SOURCE_CACHE_BYTES);
        engine.invalidate_native_batch_authority(&batch);
        assert_eq!(gauge("entries"), 0);
        assert_eq!(gauge("bytes"), 0);
        assert_eq!(gauge("peak_bytes"), bytes);
        {
            let mut cache = engine.source_cache.lock().unwrap();
            for index in 0..8 {
                let mut set = source_cache_lru_set(index);
                set.sources[0].read_grant.token = String::with_capacity(700 * 1024);
                set.sources[0].read_grant.token.push('t');
                cache.insert(
                    source_cache_lru_key(&set.chunk_id),
                    CachedSources {
                        expires_at: Instant::now() + Duration::from_secs(5),
                        sources: set,
                    },
                );
                assert!(cache.bytes() <= MAX_SOURCE_CACHE_BYTES);
            }
            assert!(
                cache.len() < 8,
                "spare String capacity must force bounded LRU eviction"
            );
            let mut huge = source_cache_lru_set(50);
            huge.allowed_ranges =
                Vec::with_capacity(MAX_SOURCE_CACHE_BYTES / std::mem::size_of::<(u64, u64)>() + 1);
            cache.insert(
                source_cache_lru_key(&huge.chunk_id),
                CachedSources {
                    expires_at: Instant::now() + Duration::from_secs(5),
                    sources: huge,
                },
            );
            assert!(!cache.contains_key(&source_cache_lru_key(&source_cache_lru_set(50).chunk_id)));
            cache.record(&engine.local);
        }
        let peak = gauge("peak_bytes");
        let generation = engine.source_cache.lock().unwrap().generation;
        let retained_entries = gauge("entries");
        drop(engine.task_reader());
        assert_eq!(engine.source_cache.lock().unwrap().generation, generation);
        assert_eq!(gauge("entries"), retained_entries);
        drop(engine);
        assert_eq!(gauge("entries"), 0);
        assert_eq!(gauge("bytes"), 0);
        assert_eq!(gauge("peak_bytes"), peak);
    }

    #[test]
    fn p2_r1_external_metadata_uses_original_send_and_authority_deadlines() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"r1 external"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let batch = source_cache_lru_batch(fixture.chunk.id.clone());
        let selection = ExternalReadSelection {
            batch,
            selected: Vec::new(),
            validations: Vec::new(),
            missing_sources: Vec::new(),
        };
        let reads = [AuthorizedExternalRead {
            chunk: fixture.chunk.clone(),
            source: fixture.candidate.clone(),
            op: selection.batch.ops[0].clone(),
        }];
        let now = Instant::now();
        let mut timing = engine.source_cache_timing().unwrap();
        timing.sent = now - Duration::from_secs(61);
        timing.sent_wall = now_unix_ms().saturating_sub(61_000);
        engine
            .cache_authorized_external_sources(&selection, &reads, timing)
            .unwrap();
        assert!(
            engine.source_cache.lock().unwrap().is_empty(),
            "delayed installation cannot grant a fresh maximum TTL"
        );
        let mut timing = engine.source_cache_timing().unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        timing.authority_deadline = Some(deadline);
        engine
            .cache_authorized_external_sources(&selection, &reads, timing)
            .unwrap();
        let key = source_cache_lru_key(&fixture.chunk.id);
        assert!(
            engine
                .source_cache
                .lock()
                .unwrap()
                .get(&key)
                .unwrap()
                .expires_at
                <= deadline
        );
    }

    fn source_cache_lru_key(chunk_id: &ChunkId) -> SourceCacheKey {
        (
            FileVersionId::new("version"),
            LayoutRootId::new("layout"),
            chunk_id.clone(),
        )
    }

    fn fill_source_cache_lru_fixture(engine: &DfsReadEngine) {
        let expires_at = Instant::now() + Duration::from_secs(60);
        let mut cache = engine.source_cache.lock().unwrap();
        for index in 0..MAX_CACHED_SOURCES {
            let sources = source_cache_lru_set(index);
            cache.insert(
                source_cache_lru_key(&sources.chunk_id),
                CachedSources {
                    expires_at,
                    sources,
                },
            );
        }
    }

    #[test]
    fn source_cache_lru_native_overflow_preserves_hot_entries_and_evicts_one() {
        let temp = tempfile::tempdir().unwrap();
        let fresh = source_cache_lru_set(MAX_CACHED_SOURCES);
        let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![fresh.clone()],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            provider.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        fill_source_cache_lru_fixture(&engine);
        let hot = source_cache_lru_set(0).chunk_id;
        engine
            .sources_for(&source_cache_lru_batch(hot.clone()))
            .unwrap();
        assert_eq!(*provider.calls.lock().unwrap(), 0);
        engine
            .sources_for(&source_cache_lru_batch(fresh.chunk_id.clone()))
            .unwrap();
        let cache = engine.source_cache.lock().unwrap();
        assert_eq!(
            cache.len(),
            MAX_CACHED_SOURCES,
            "overflow must not clear the cache"
        );
        assert!(cache.contains_key(&source_cache_lru_key(&hot)));
        assert!(!cache.contains_key(&source_cache_lru_key(&source_cache_lru_set(1).chunk_id)));
        assert!(cache.contains_key(&source_cache_lru_key(&fresh.chunk_id)));
        drop(cache);
        engine.sources_for(&source_cache_lru_batch(hot)).unwrap();
        assert_eq!(
            *provider.calls.lock().unwrap(),
            1,
            "hot hit must not requery Meta"
        );
    }

    #[test]
    fn source_cache_lru_external_admission_preserves_hot_entries_and_evicts_one() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = ExternalFixture::new(b"new external source");
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            Arc::new(StaticSources::default()),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        fill_source_cache_lru_fixture(&engine);
        let hot = source_cache_lru_set(0).chunk_id;
        let (cached, misses) = engine
            .cached_sources_for(&source_cache_lru_batch(hot.clone()))
            .unwrap();
        assert_eq!(cached.len(), 1);
        assert!(misses.is_empty());
        let selection = ExternalReadSelection {
            batch: source_cache_lru_batch(fixture.chunk.id.clone()),
            selected: Vec::new(),
            validations: Vec::new(),
            missing_sources: Vec::new(),
        };
        // This helper is reached only after external authorization. Its cached
        // metadata still cannot replace the authorizer or final grant checks.
        engine
            .cache_authorized_external_sources(
                &selection,
                &[AuthorizedExternalRead {
                    chunk: fixture.chunk.clone(),
                    source: fixture.candidate,
                    op: selection.batch.ops[0].clone(),
                }],
                engine.source_cache_timing().unwrap(),
            )
            .unwrap();
        let cache = engine.source_cache.lock().unwrap();
        assert_eq!(
            cache.len(),
            MAX_CACHED_SOURCES,
            "external admission must evict incrementally"
        );
        assert!(cache.contains_key(&source_cache_lru_key(&hot)));
        assert!(!cache.contains_key(&source_cache_lru_key(&source_cache_lru_set(1).chunk_id)));
        assert!(cache.contains_key(&source_cache_lru_key(&fixture.chunk.id)));
    }

    #[test]
    fn source_cache_lru_hits_do_not_extend_ttl_or_keep_expired_entries() {
        let temp = tempfile::tempdir().unwrap();
        let sources = source_cache_lru_set(0);
        let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![sources.clone()],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            provider.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let key = source_cache_lru_key(&sources.chunk_id);
        let batch = source_cache_lru_batch(sources.chunk_id.clone());
        let deadline = Instant::now() + Duration::from_secs(30);
        engine.source_cache.lock().unwrap().insert(
            key.clone(),
            CachedSources {
                expires_at: deadline,
                sources: sources.clone(),
            },
        );
        engine.sources_for(&batch).unwrap();
        assert_eq!(
            engine
                .source_cache
                .lock()
                .unwrap()
                .get(&key)
                .unwrap()
                .expires_at,
            deadline
        );
        assert_eq!(*provider.calls.lock().unwrap(), 0);
        engine.source_cache.lock().unwrap().insert(
            key,
            CachedSources {
                expires_at: Instant::now() - Duration::from_secs(1),
                sources,
            },
        );
        engine.sources_for(&batch).unwrap();
        assert_eq!(
            *provider.calls.lock().unwrap(),
            1,
            "expired metadata must return to Meta"
        );
    }

    #[test]
    fn reader_cache_lru_overflow_preserves_hot_pin_and_retired_arc_lifetime() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            Arc::new(StaticSources::default()),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let mut chunks = Vec::new();
        let mut retired = None;
        for index in 0..MAX_PINNED_READERS {
            let bytes = format!("reader-cache-lru-{index}");
            let chunk = put_local_chunk(&local, &bytes, bytes.as_bytes());
            let pin = engine.local_reader(&chunk).unwrap();
            if index == 1 {
                retired = Some(pin);
            }
            chunks.push(chunk);
        }
        let hot = engine.local_reader(&chunks[0]).unwrap();
        let retired = retired.unwrap();
        assert_eq!(Arc::strong_count(&retired), 2);
        let weak = Arc::downgrade(&retired);
        let fresh = put_local_chunk(&local, "reader-cache-lru-extra", b"extra");
        engine.local_reader(&fresh).unwrap();
        let cache = engine.readers.lock().unwrap();
        assert_eq!(
            cache.len(),
            MAX_PINNED_READERS,
            "overflow must retire only the coldest pin"
        );
        assert!(cache.contains_key(&chunks[0]));
        assert!(!cache.contains_key(&chunks[1]));
        assert!(cache.contains_key(&fresh));
        drop(cache);
        assert!(Arc::ptr_eq(&hot, &engine.local_reader(&chunks[0]).unwrap()));
        assert_eq!(
            Arc::strong_count(&retired),
            1,
            "cache ownership must be released on eviction"
        );
        let mut output = [0; 32];
        let length = retired.read_at(0, &mut output).unwrap();
        assert_eq!(&output[..length], b"reader-cache-lru-1");
        drop(retired);
        assert!(
            weak.upgrade().is_none(),
            "last in-flight pin owns its descriptor lifetime"
        );
    }

    #[test]
    fn reader_cache_concurrent_misses_return_one_canonical_pin() {
        const READERS: usize = 16;
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk = put_local_chunk(
            &local,
            "reader-cache-concurrent",
            &vec![b'x'; 4 * 1024 * 1024],
        );
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            Arc::new(StaticSources::default()),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        *engine.local_reader_install_barrier.lock().unwrap() =
            Some(Arc::new(std::sync::Barrier::new(READERS)));
        let pins = std::thread::scope(|scope| {
            let threads = (0..READERS)
                .map(|_| scope.spawn(|| engine.local_reader(&chunk).unwrap()))
                .collect::<Vec<_>>();
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(
            pins.iter().all(|pin| Arc::ptr_eq(pin, &pins[0])),
            "concurrent misses must reuse the installed pin"
        );
        assert_eq!(engine.readers.lock().unwrap().len(), 1);
    }

    #[test]
    fn local_object_validation_rejects_same_id_with_different_standard_root() {
        let staged = StagedChunk::new(OperationId::new("local-root-anchor"), vec![71; 64 * 1024]);
        let mut wrong = staged.chunk.clone();
        wrong.content_digest.bytes[0] ^= 1;
        assert_eq!(wrong.id, staged.chunk.id);
        assert!(validate_local_object_identity(&staged.chunk, &wrong).is_err());
        wrong = staged.chunk.clone();
        wrong.length -= 1;
        assert!(validate_local_object_identity(&staged.chunk, &wrong).is_err());
    }

    #[test]
    fn local_pinned_object_mismatch_never_publishes_output_or_adds_meta_rpc() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let first = put_local_chunk(&local, "trusted-layout-chunk", b"abcdef");
        let other = put_local_chunk(&local, "wrong-pinned-chunk", b"uvwxyz");
        let wrong_pin = Arc::new(local.open_verified(&other).unwrap());
        let sources = Arc::new(StaticSources::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            sources.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        // Fault injection: an index hit must still bind the pin to the layout's
        // independently trusted canonical root/length, never just the map key.
        engine
            .readers
            .lock()
            .unwrap()
            .insert(first.clone(), wrong_pin);
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id: first,
                chunk_offset: 0,
                length: 6,
                output_offset: 0,
            }],
        };
        let mut output = [b'!'; 6];
        assert!(engine.read_batch(&batch, &mut output).is_err());
        assert_eq!(output, [b'!'; 6]);
        assert_eq!(*sources.calls.lock().unwrap(), 0);
    }

    #[test]
    fn local_read_uses_verified_reader_without_source_lookup() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk_id = put_local_chunk(local.as_ref(), "op", b"abcdef");
        let sources = Arc::new(StaticSources::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            sources.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let mut out = [0; 3];
        engine
            .read_batch(
                &ReadBatch {
                    lookahead: Vec::new(),
                    file_version_id: Some(FileVersionId::new("version")),
                    layout_root_id: LayoutRootId::new("layout"),
                    ops: vec![ChunkReadOp {
                        chunk_id,
                        chunk_offset: 2,
                        length: 3,
                        output_offset: 0,
                    }],
                },
                &mut out,
            )
            .unwrap();
        assert_eq!(&out, b"cde");
        assert_eq!(*sources.calls.lock().unwrap(), 0);
    }

    #[test]
    fn same_chunk_group_preserves_overlapping_output_order() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk_id = put_local_chunk(local.as_ref(), "overlap", b"abcdef");
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            Arc::new(StaticSources::default()),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 0,
                    length: 3,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id,
                    chunk_offset: 3,
                    length: 3,
                    output_offset: 0,
                },
            ],
        };
        let mut out = [b'!'; 3];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"def");
    }

    #[test]
    fn same_chunk_damage_outside_selected_ranges_falls_back_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let mut bytes = vec![b'a'; 128 * 1024 + 1];
        bytes[4096] = b'b';
        let chunk_id = put_local_chunk(local.as_ref(), "outside-damage", &bytes);
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            Arc::new(StaticSources::default()),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 0,
                    length: 1,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 4096,
                    length: 1,
                    output_offset: 1,
                },
            ],
        };
        let mut out = [b'!'; 2];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"ab");
        let path = temp.path().join("chunks").join(&chunk_id.0);
        bytes[128 * 1024] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        out = [b'!'; 2];
        assert!(engine.read_batch(&batch, &mut out).is_err());
        assert_eq!(&out, b"!!");
        assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
        assert!(engine.readers.lock().unwrap().is_empty());
    }

    #[test]
    fn same_chunk_short_read_falls_back_only_for_short_ranges_in_original_order() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk_id = put_local_chunk(local.as_ref(), "short", b"abcdef");
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id: chunk_id.clone(),
                sources: vec![source("remote-short", &chunk_id, "node-b")],
            }],
        }));
        let transfer = Arc::new(RecordingTransfer::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            sources,
            transfer.clone(),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 0,
                    length: 1,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: u64::MAX,
                    length: 1,
                    output_offset: 1,
                },
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 1,
                    length: 1,
                    output_offset: 2,
                },
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 5,
                    length: 2,
                    output_offset: 3,
                },
                ChunkReadOp {
                    chunk_id,
                    chunk_offset: 0,
                    length: 0,
                    output_offset: 0,
                },
            ],
        };
        let mut out = [b'!'; 5];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"arbrr");
        assert!(local.quarantined_chunks().unwrap().is_empty());
        assert!(engine.readers.lock().unwrap().is_empty());
        let calls = transfer.batches.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operations.len(), 2);
        assert_eq!(calls[0].operations[0].op.chunk_offset, u64::MAX);
        assert_eq!(calls[0].operations[1].op.chunk_offset, 5);
        assert_eq!(calls[0].operations[1].op.length, 2);
    }

    #[test]
    fn same_chunk_cached_pin_survives_path_replacement_for_grouped_reads() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk_id = put_local_chunk(local.as_ref(), "pin", b"abcdef");
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            Arc::new(StaticSources::default()),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 0,
                    length: 3,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 3,
                    length: 3,
                    output_offset: 3,
                },
            ],
        };
        let mut out = [0; 6];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"abcdef");
        let replacement = temp.path().join("replacement");
        std::fs::write(&replacement, b"abcXef").unwrap();
        std::fs::rename(replacement, temp.path().join("chunks").join(&chunk_id.0)).unwrap();
        out.fill(b'!');
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"abcdef");
        assert!(local.open_verified(&chunk_id).is_err());
    }

    #[test]
    fn diagnostic_cold_corrupted_local_copy_uses_healthy_peer() {
        diagnostic_corrupted_local_copy(false);
    }

    #[test]
    fn diagnostic_warm_corrupted_local_copy_uses_healthy_peer() {
        diagnostic_corrupted_local_copy(true);
    }

    fn diagnostic_corrupted_local_copy(warm: bool) {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let mut builder = ChunkBuilder::default();
        builder.replace(b"abcdef".to_vec());
        let staged = builder.stage(OperationId::new("diagnostic"));
        let chunk_id = staged.chunk.id.clone();
        local.put(staged).unwrap();
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id: chunk_id.clone(),
                sources: vec![source("healthy-b", &chunk_id, "node-b")],
            }],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            sources.clone(),
            Arc::new(StaticTransfer {
                bytes: b"abcdef".to_vec(),
                fail_first: Mutex::new(false),
            }),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id: chunk_id.clone(),
                chunk_offset: 0,
                length: 6,
                output_offset: 0,
            }],
        };
        let mut out = [0; 6];
        if warm {
            engine.read_batch(&batch, &mut out).unwrap();
            assert_eq!(&out, b"abcdef");
            assert_eq!(*sources.calls.lock().unwrap(), 0);
        }
        let path = temp.path().join("chunks").join(&chunk_id.0);
        std::fs::write(&path, b"abXdef").unwrap();
        std::fs::File::open(&path).unwrap().sync_all().unwrap();
        out.fill(9);
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(
            &out, b"abcdef",
            "corrupted cached local bytes must not reach the caller"
        );
        assert_eq!(*sources.calls.lock().unwrap(), 1);
        assert!(engine.readers.lock().unwrap().is_empty());
        // Replacement keeps the identity but creates a new physical inode.
        // The failed pin must not trap later reads on the retired corrupt fd.
        local
            .put(crate::node::chunk::StagedChunk::new(
                OperationId::new("restore"),
                b"abcdef".to_vec(),
            ))
            .unwrap();
        out.fill(9);
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"abcdef");
        assert_eq!(*sources.calls.lock().unwrap(), 1);
        assert_eq!(engine.readers.lock().unwrap().len(), 1);
        assert!(local.quarantined_chunks().unwrap().is_empty());
    }

    #[test]
    fn quarantined_local_copy_and_no_readable_peer_return_eio_without_output() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let staged =
            crate::node::chunk::StagedChunk::new(OperationId::new("all-bad"), b"abcdef".to_vec());
        let chunk_id = staged.chunk.id.clone();
        local.put(staged).unwrap();
        std::fs::write(temp.path().join("chunks").join(&chunk_id.0), b"abXdef").unwrap();
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id: chunk_id.clone(),
                sources: Vec::new(),
            }],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            sources,
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id,
                chunk_offset: 0,
                length: 6,
                output_offset: 0,
            }],
        };
        let mut out = [9; 6];
        for _ in 0..2 {
            let error = engine.read_batch(&batch, &mut out).unwrap_err();
            assert_eq!(crate::error::errno(&error), libc::EIO);
            assert_eq!(out, [9; 6]);
        }
        assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
    }

    #[test]
    fn remote_read_uses_scratch_and_falls_back_between_sources() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk_id = ChunkId::new("missing");
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id: chunk_id.clone(),
                sources: vec![
                    source("copy-b", &chunk_id, "node-b"),
                    source("copy-c", &chunk_id, "node-c"),
                ],
            }],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            sources,
            Arc::new(StaticTransfer {
                bytes: b"remote".to_vec(),
                fail_first: Mutex::new(true),
            }),
            DfsReadConfig::default(),
        );
        let mut out = [9; 8];
        engine
            .read_batch(
                &ReadBatch {
                    lookahead: Vec::new(),
                    file_version_id: Some(FileVersionId::new("version")),
                    layout_root_id: LayoutRootId::new("layout"),
                    ops: vec![ChunkReadOp {
                        chunk_id,
                        chunk_offset: 0,
                        length: 6,
                        output_offset: 1,
                    }],
                },
                &mut out,
            )
            .unwrap();
        assert_eq!(&out, &[9, b'r', b'e', b'm', b'o', b't', b'e', 9]);
    }

    #[test]
    fn native_authority_snapshot_keeps_original_deadline_and_stops_on_epoch_change() {
        let authority = LocalReadAuthority::new(7, Instant::now() + Duration::from_secs(10));
        let original = authority.snapshot().unwrap();
        authority
            .refresh(7, Instant::now() + Duration::from_secs(30))
            .unwrap();
        assert!(authority.snapshot().unwrap().expires_at() > original.expires_at());
        assert!(authority.check_snapshot(&original).is_ok());
        let expired = NativeReadAuthoritySnapshot {
            epoch: 7,
            expires_at: Some(Instant::now()),
        };
        assert!(authority.check_snapshot(&expired).is_err());
        assert!(
            authority
                .refresh(8, Instant::now() + Duration::from_secs(30))
                .is_err()
        );
        assert!(authority.check_snapshot(&original).is_err());
        assert!(
            authority
                .refresh(7, Instant::now() + Duration::from_secs(30))
                .is_err()
        );
    }

    #[test]
    fn native_authority_refresh_cannot_revive_expired_or_stopped_session() {
        for stopped in [false, true] {
            let authority = LocalReadAuthority::new(
                1,
                if stopped {
                    Instant::now() + Duration::from_secs(30)
                } else {
                    Instant::now()
                },
            );
            if stopped {
                authority.stop();
            }
            assert!(
                authority
                    .refresh(1, Instant::now() + Duration::from_secs(30))
                    .is_err()
            );
            assert!(authority.snapshot().is_err());
        }
    }

    #[test]
    fn native_authority_stopped_session_rejects_local_and_sparse_reads_without_output() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk_id = put_local_chunk(&local, "native-stop", b"abcdef");
        let authority = Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        ));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            Arc::new(UnimplementedReadSourceProvider),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        )
        .with_read_authority(authority.clone());
        let mut batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id,
                chunk_offset: 0,
                length: 6,
                output_offset: 0,
            }],
        };
        assert_eq!(
            engine.read_batch_native_owned(&batch, 6).unwrap().unwrap(),
            b"abcdef"
        );
        authority.stop();
        for sparse in [false, true] {
            if sparse {
                batch.ops.clear();
            }
            let mut output = [b'!'; 6];
            assert!(engine.read_batch(&batch, &mut output).is_err());
            assert_eq!(output, [b'!'; 6]);
            if !sparse {
                assert!(engine.read_batch_native_owned(&batch, 6).is_err());
            }
        }
    }

    struct NativeAuthorityTransfer {
        authority: Arc<LocalReadAuthority>,
        action: &'static str,
    }

    impl ChunkTransfer for NativeAuthorityTransfer {
        fn read_ranges(&self, _batch: &PeerReadBatch, output: &mut [u8]) -> Result<()> {
            output.fill(b'x');
            match self.action {
                "stop" => self.authority.stop(),
                "renew_then_expire" => {
                    let deadline = self.authority.snapshot()?.expires_at().unwrap();
                    self.authority
                        .refresh(1, Instant::now() + Duration::from_secs(30))?;
                    std::thread::sleep(
                        deadline.saturating_duration_since(Instant::now())
                            + Duration::from_millis(1),
                    );
                }
                "expire_then_refresh" => {
                    let deadline = self.authority.snapshot()?.expires_at().unwrap();
                    std::thread::sleep(
                        deadline.saturating_duration_since(Instant::now())
                            + Duration::from_millis(1),
                    );
                    let _ = self
                        .authority
                        .refresh(1, Instant::now() + Duration::from_secs(30));
                }
                "epoch" => {
                    let _ = self
                        .authority
                        .refresh(2, Instant::now() + Duration::from_secs(30));
                }
                "denied" => return Err(external_denied("injected known denial")),
                "unavailable" => return Err(unavailable("injected ordinary transport failure")),
                _ => {}
            }
            Ok(())
        }
    }

    #[test]
    fn native_authority_late_failure_never_commits_borrowed_or_owned_output() {
        for action in [
            "stop",
            "expire_then_refresh",
            "renew_then_expire",
            "epoch",
            "denied",
            "unavailable",
        ] {
            for owned in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let chunk_id = ChunkId::new("missing-authority");
                let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
                    revision: 1,
                    chunks: vec![ChunkSources {
                        allowed_ranges: Vec::new(),
                        chunk: None,
                        chunk_id: chunk_id.clone(),
                        sources: vec![source("b", &chunk_id, "node-b")],
                    }],
                }));
                // Opening the durable store can fsync; do it before starting
                // the short expiry interval exercised inside the transfer.
                let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
                let authority = Arc::new(LocalReadAuthority::new(
                    1,
                    Instant::now()
                        + if matches!(action, "expire_then_refresh" | "renew_then_expire") {
                            Duration::from_millis(250)
                        } else {
                            Duration::from_secs(30)
                        },
                ));
                let engine = DfsReadEngine::new(
                    crate::dfs::NamespaceId::new("default"),
                    "node-a".into(),
                    local,
                    sources,
                    Arc::new(NativeAuthorityTransfer {
                        authority: authority.clone(),
                        action,
                    }),
                    DfsReadConfig::default(),
                )
                .with_read_authority(authority.clone());
                let batch = ReadBatch {
                    lookahead: Vec::new(),
                    file_version_id: Some(FileVersionId::new("version")),
                    layout_root_id: LayoutRootId::new("layout"),
                    ops: vec![ChunkReadOp {
                        chunk_id,
                        chunk_offset: 0,
                        length: 6,
                        output_offset: 0,
                    }],
                };
                if owned {
                    assert!(
                        engine.read_batch_native_owned(&batch, 6).is_err(),
                        "{action}"
                    );
                } else {
                    let mut output = [b'!'; 6];
                    assert!(engine.read_batch(&batch, &mut output).is_err(), "{action}");
                    assert_eq!(output, [b'!'; 6], "{action}");
                }
                assert_eq!(
                    authority.snapshot().is_ok(),
                    matches!(action, "unavailable" | "renew_then_expire" | "denied"),
                    "{action}"
                );
            }
        }
    }

    struct NativeScopedDenialTransfer {
        calls: std::sync::atomic::AtomicUsize,
        unauthenticated: bool,
    }

    impl ChunkTransfer for NativeScopedDenialTransfer {
        fn read_ranges(&self, _batch: &PeerReadBatch, output: &mut [u8]) -> Result<()> {
            output.fill(b'x');
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                return Err(if self.unauthenticated {
                    Error::new(
                        afs_error::IO_PERMISSION_DENIED,
                        afs_error::ErrorKind::Unauthenticated,
                        "injected scoped grant denial",
                    )
                } else {
                    external_denied("injected scoped grant denial")
                });
            }
            Ok(())
        }
    }

    #[test]
    fn native_authority_read_denial_clears_only_batch_sources_and_allows_fresh_authorization() {
        for unauthenticated in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let chunk_id = ChunkId::new("scoped-denial");
            let reply = DfsChunkSourcesReply {
                revision: 1,
                chunks: vec![ChunkSources {
                    allowed_ranges: Vec::new(),
                    chunk: None,
                    chunk_id: chunk_id.clone(),
                    sources: vec![source("b", &chunk_id, "node-b")],
                }],
            };
            let sources = Arc::new(StaticSources::new(reply.clone()));
            let authority = Arc::new(LocalReadAuthority::new(
                1,
                Instant::now() + Duration::from_secs(30),
            ));
            let engine = DfsReadEngine::new(
                crate::dfs::NamespaceId::new("default"),
                "node-a".into(),
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources.clone(),
                Arc::new(NativeScopedDenialTransfer {
                    calls: std::sync::atomic::AtomicUsize::new(0),
                    unauthenticated,
                }),
                DfsReadConfig::default(),
            )
            .with_read_authority(authority);
            let unrelated = (
                FileVersionId::new("other-version"),
                LayoutRootId::new("other-layout"),
                chunk_id.clone(),
            );
            engine.source_cache.lock().unwrap().insert(
                unrelated.clone(),
                CachedSources {
                    expires_at: Instant::now() + Duration::from_secs(30),
                    sources: reply.chunks[0].clone(),
                },
            );
            let batch = ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(FileVersionId::new("version")),
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![ChunkReadOp {
                    chunk_id,
                    chunk_offset: 0,
                    length: 6,
                    output_offset: 0,
                }],
            };
            let mut output = [b'!'; 6];
            let error = engine.read_batch(&batch, &mut output).unwrap_err();
            assert_eq!(
                error.kind(),
                if unauthenticated {
                    afs_error::ErrorKind::Unauthenticated
                } else {
                    afs_error::ErrorKind::PermissionDenied
                }
            );
            assert_eq!(output, [b'!'; 6]);
            assert!(engine.capture_native_read_authority().is_ok());
            let cache = engine.source_cache.lock().unwrap();
            assert_eq!(cache.len(), 1);
            assert!(cache.contains_key(&unrelated));
            drop(cache);
            engine.read_batch(&batch, &mut output).unwrap();
            assert_eq!(output, [b'x'; 6]);
            assert_eq!(
                *sources.calls.lock().unwrap(),
                2,
                "rejected context must return to Meta"
            );
        }
    }

    #[test]
    fn native_private_read_uses_existing_final_buffer_and_preserves_sparse_local_ranges() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk = put_local_chunk(&local, "native-private", b"abcdef");
        let transfer = Arc::new(OwnedTransferProbe {
            destination: std::sync::atomic::AtomicUsize::new(0),
            fail_first: AtomicBool::new(false),
            fail_all: false,
            expire_before_return: false,
        });
        let missing = ChunkId::new("missing-private");
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id: missing.clone(),
                sources: vec![source("b", &missing, "node-b")],
            }],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            sources,
            transfer.clone(),
            DfsReadConfig::default(),
        );
        let mut batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id: missing,
                chunk_offset: 0,
                length: 6,
                output_offset: 0,
            }],
        };
        let mut private = [0; 6];
        engine
            .read_batch_native_into_private(&batch, &mut private)
            .unwrap();
        assert_eq!(private, [b'x'; 6]);
        assert_eq!(
            transfer.destination.load(Ordering::Relaxed),
            private.as_ptr() as usize,
            "private helper must not allocate another aggregate staging destination"
        );
        batch.ops = vec![
            ChunkReadOp {
                chunk_id: chunk.clone(),
                chunk_offset: 4,
                length: 2,
                output_offset: 4,
            },
            ChunkReadOp {
                chunk_id: chunk,
                chunk_offset: 0,
                length: 2,
                output_offset: 1,
            },
        ];
        private.fill(0);
        engine
            .read_batch_native_into_private(&batch, &mut private)
            .unwrap();
        assert_eq!(&private, b"\0ab\0ef");
    }

    #[test]
    fn native_private_read_rejects_overlap_and_budget_before_work() {
        let temp = tempfile::tempdir().unwrap();
        let sources = Arc::new(StaticSources::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig {
                max_inflight_bytes: 6,
                ..DfsReadConfig::default()
            },
        );
        assert!(engine.validate_native_read_size(7).is_err());
        assert!(engine.validate_native_read_size(6).is_ok());
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: ChunkId::new("one"),
                    chunk_offset: 0,
                    length: 2,
                    output_offset: 1,
                },
                ChunkReadOp {
                    chunk_id: ChunkId::new("two"),
                    chunk_offset: 0,
                    length: 2,
                    output_offset: 2,
                },
            ],
        };
        let mut private = [b'!'; 6];
        assert!(
            engine
                .read_batch_native_into_private(&batch, &mut private)
                .is_err()
        );
        assert_eq!(private, [b'!'; 6]);
        assert_eq!(*sources.calls.lock().unwrap(), 0);
    }

    #[test]
    fn native_private_read_late_stop_returns_error_before_private_output_publication() {
        let temp = tempfile::tempdir().unwrap();
        let chunk_id = ChunkId::new("late-private");
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id: chunk_id.clone(),
                sources: vec![source("b", &chunk_id, "node-b")],
            }],
        }));
        let authority = Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        ));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources,
            Arc::new(NativeAuthorityTransfer {
                authority: authority.clone(),
                action: "stop",
            }),
            DfsReadConfig::default(),
        )
        .with_read_authority(authority);
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id,
                chunk_offset: 0,
                length: 6,
                output_offset: 0,
            }],
        };
        let mut private = [0; 6];
        assert!(
            engine
                .read_batch_native_into_private(&batch, &mut private)
                .is_err()
        );
        assert_eq!(
            private, [b'x'; 6],
            "unpublished private payload is discarded by its caller"
        );
    }

    struct NativeCacheTransfer {
        bytes: Mutex<Vec<u8>>,
        batches: Mutex<Vec<PeerReadBatch>>,
        corrupt: AtomicBool,
        delay: Duration,
    }

    impl ChunkTransfer for NativeCacheTransfer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            batch.validate(out.len())?;
            self.batches.lock().unwrap().push(batch.clone());
            let bytes = self.bytes.lock().unwrap();
            for item in &batch.operations {
                let start = item.op.chunk_offset as usize;
                let length = item.op.length as usize;
                out[item.op.output_offset..item.op.output_offset + length]
                    .copy_from_slice(&bytes[start..start + length]);
            }
            if self.corrupt.load(Ordering::Relaxed) {
                let last = out.len() - 1;
                out[last] ^= 1;
            }
            std::thread::sleep(self.delay);
            Ok(())
        }
    }

    struct NativeCacheFixture {
        _temp: tempfile::TempDir,
        engine: DfsReadEngine,
        cache: Arc<crate::node::peer_cache::DiskPeerCache>,
        sources: Arc<StaticSources>,
        transfer: Arc<NativeCacheTransfer>,
        batch: ReadBatch,
        chunk: ChunkObject,
    }

    impl NativeCacheFixture {
        fn new(max_inflight_bytes: u64, ranges: Vec<(u64, u64)>, delay: Duration) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let bytes = (0..64 * 1024)
                .map(|index| (index % 251) as u8)
                .collect::<Vec<_>>();
            let staged = StagedChunk::new(OperationId::new("native-disk-cache"), bytes.clone());
            let chunk = staged.chunk.clone();
            let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
                revision: 1,
                chunks: vec![ChunkSources {
                    allowed_ranges: ranges,
                    chunk: Some(chunk.clone()),
                    chunk_id: chunk.id.clone(),
                    sources: vec![source("b", &chunk.id, "node-b")],
                }],
            }));
            let cache = Arc::new(
                crate::node::peer_cache::DiskPeerCache::open(
                    crate::node::peer_cache::PeerCacheOptions {
                        directory: temp.path().join("cache"),
                        capacity_bytes: 2 * 1024 * 1024,
                        namespace_id: crate::dfs::NamespaceId::new("default"),
                    },
                )
                .unwrap(),
            );
            let transfer = Arc::new(NativeCacheTransfer {
                bytes: Mutex::new(bytes),
                batches: Mutex::new(Vec::new()),
                corrupt: AtomicBool::new(false),
                delay,
            });
            let engine = DfsReadEngine::new(
                crate::dfs::NamespaceId::new("default"),
                "node-a".into(),
                Arc::new(LocalChunkStore::open(temp.path().join("local"), "node-a").unwrap()),
                sources.clone(),
                transfer.clone(),
                DfsReadConfig {
                    max_inflight_bytes,
                    ..DfsReadConfig::default()
                },
            )
            .with_read_authority(Arc::new(LocalReadAuthority::new(
                1,
                Instant::now() + Duration::from_secs(30),
            )))
            .with_peer_cache(cache.clone());
            let batch = ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(FileVersionId::new("version")),
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![ChunkReadOp {
                    chunk_id: chunk.id.clone(),
                    chunk_offset: 1,
                    length: 2,
                    output_offset: 0,
                }],
            };
            Self {
                _temp: temp,
                engine,
                cache,
                sources,
                transfer,
                batch,
                chunk,
            }
        }
    }

    fn replace_native_ready_with_foreign(fixture: &NativeCacheFixture) -> std::path::PathBuf {
        use std::{io::Write, os::unix::fs::OpenOptionsExt};
        let directory = fixture._temp.path().join("cache");
        let ready = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "ready")
            })
            .unwrap();
        std::fs::rename(
            &ready,
            fixture._temp.path().join("original-ready-preserved"),
        )
        .unwrap();
        let mut foreign = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&ready)
            .unwrap();
        foreign
            .write_all(b"foreign-private-cache-replacement")
            .unwrap();
        ready
    }

    #[test]
    fn native_peer_disk_cache_follower_ready_io_failure_falls_back_without_deleting_foreign() {
        struct PausedFirstTransfer {
            inner: Arc<NativeCacheTransfer>,
            entered: std::sync::mpsc::Sender<()>,
            release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        }
        impl ChunkTransfer for PausedFirstTransfer {
            fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
                self.inner.read_ranges(batch, out)?;
                let release = self.release.lock().unwrap().take();
                if let Some(release) = release {
                    self.entered.send(()).unwrap();
                    release.recv_timeout(Duration::from_secs(5)).unwrap();
                }
                Ok(())
            }
        }
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let (transfer_entered_tx, transfer_entered_rx) = std::sync::mpsc::channel();
        let (transfer_release_tx, transfer_release_rx) = std::sync::mpsc::channel();
        fixture.engine.transfer = Arc::new(PausedFirstTransfer {
            inner: fixture.transfer.clone(),
            entered: transfer_entered_tx,
            release: Mutex::new(Some(transfer_release_rx)),
        });
        let (recheck_entered_tx, recheck_entered_rx) = std::sync::mpsc::channel();
        let (recheck_release_tx, recheck_release_rx) = std::sync::mpsc::channel();
        *fixture.engine.peer_cache_recheck_hook.lock().unwrap() = Some(PeerCacheRecheckHook {
            stage: "follower_ready",
            entered: recheck_entered_tx,
            release: recheck_release_rx,
        });
        std::thread::scope(|scope| {
            let leader = scope.spawn(|| fixture.engine.read_batch_native_owned(&fixture.batch, 2));
            transfer_entered_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            let follower =
                scope.spawn(|| fixture.engine.read_batch_native_owned(&fixture.batch, 2));
            let deadline = Instant::now() + Duration::from_secs(3);
            while fixture.cache.waiting_fill_readers_for_test() == 0 {
                assert!(
                    Instant::now() < deadline,
                    "follower did not join the actual fill"
                );
                std::thread::yield_now();
            }
            transfer_release_tx.send(()).unwrap();
            recheck_entered_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            assert_eq!(leader.join().unwrap().unwrap().unwrap(), [1, 2]);
            let foreign = replace_native_ready_with_foreign(&fixture);
            recheck_release_tx.send(()).unwrap();
            let result = follower.join().unwrap();
            assert_eq!(
                std::fs::read(&foreign).unwrap(),
                b"foreign-private-cache-replacement"
            );
            assert_eq!(
                result.unwrap().unwrap(),
                [1, 2],
                "disposable Ready I/O must fall back to Peer"
            );
        });
        let calls = fixture.transfer.batches.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].operations[0].op.length, 64 * 1024);
        assert_eq!(calls[1].operations[0].op.length, 2);
    }

    #[test]
    fn native_peer_disk_cache_leader_recheck_io_failure_falls_back_without_deleting_foreign() {
        let fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let mut blocker = fixture.cache.operation().unwrap();
        blocker.acquire_fill(|| Ok(())).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *fixture.engine.peer_cache_recheck_hook.lock().unwrap() = Some(PeerCacheRecheckHook {
            stage: "leader_recheck",
            entered: entered_tx,
            release: release_rx,
        });
        std::thread::scope(|scope| {
            let reader = scope.spawn(|| fixture.engine.read_batch_native_owned(&fixture.batch, 2));
            let deadline = Instant::now() + Duration::from_secs(3);
            while !fixture.cache.has_fill_flight_for_test() {
                assert!(
                    Instant::now() < deadline,
                    "reader did not reach its initial miss/fill flight"
                );
                std::thread::yield_now();
            }
            {
                let bytes = fixture.transfer.bytes.lock().unwrap();
                let verified = crate::node::peer_cache::VerifiedPeerChunk::verify(
                    &fixture.chunk,
                    &bytes,
                    fixture.cache.proof_budget(),
                )
                .unwrap();
                assert!(blocker.store_verified(&verified).unwrap());
            }
            drop(blocker);
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let foreign = replace_native_ready_with_foreign(&fixture);
            release_tx.send(()).unwrap();
            let result = reader.join().unwrap();
            assert_eq!(
                std::fs::read(&foreign).unwrap(),
                b"foreign-private-cache-replacement"
            );
            assert_eq!(
                result.unwrap().unwrap(),
                [1, 2],
                "recheck cache I/O must not become a flight data error"
            );
        });
        let calls = fixture.transfer.batches.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operations[0].op.length, 64 * 1024);
    }

    #[test]
    fn native_peer_disk_cache_failed_leader_does_not_fail_valid_follower() {
        for failure in [
            None,
            Some(unavailable(
                "another caller exhausted its transfer deadline",
            )),
            Some(corrupt("another caller's selected source was corrupt")),
        ] {
            let fixture =
                NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
            let crate::node::peer_cache::FillRole::Leader(mut leader) = fixture
                .cache
                .fill_flight(&fixture.chunk, || Ok(()))
                .unwrap()
            else {
                panic!("manual flight must be the first leader");
            };
            std::thread::scope(|scope| {
                let following =
                    scope.spawn(|| fixture.engine.read_batch_native_owned(&fixture.batch, 2));
                let deadline = Instant::now() + Duration::from_secs(3);
                while fixture.cache.waiting_fill_readers_for_test() == 0 {
                    assert!(Instant::now() < deadline, "reader did not join the flight");
                    std::thread::yield_now();
                }
                if let Some(error) = failure {
                    // Keep the completed leader alive: the follower must not
                    // rejoin this terminal flight or inherit its source error.
                    leader.finish(crate::node::peer_cache::FillOutcome::Failed(error));
                } else {
                    drop(leader);
                }
                assert_eq!(following.join().unwrap().unwrap().unwrap(), [1, 2]);
            });
            let calls = fixture.transfer.batches.lock().unwrap();
            assert_eq!(
                calls.len(),
                1,
                "one bounded independently verified fallback"
            );
            assert_eq!(calls[0].operations[0].op.chunk_offset, 1);
            assert_eq!(calls[0].operations[0].op.length, 2);
            assert_eq!(fixture.cache.stats().unwrap().entries, 0);
        }
    }

    #[test]
    fn native_peer_disk_cache_failed_leader_cannot_bypass_follower_stop() {
        let fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let crate::node::peer_cache::FillRole::Leader(mut leader) = fixture
            .cache
            .fill_flight(&fixture.chunk, || Ok(()))
            .unwrap()
        else {
            panic!("manual flight must be the first leader");
        };
        std::thread::scope(|scope| {
            let following =
                scope.spawn(|| fixture.engine.read_batch_native_owned(&fixture.batch, 2));
            let deadline = Instant::now() + Duration::from_secs(3);
            while fixture.cache.waiting_fill_readers_for_test() == 0 {
                assert!(Instant::now() < deadline, "reader did not join the flight");
                std::thread::yield_now();
            }
            fixture.engine.read_authority.stop();
            leader.finish(crate::node::peer_cache::FillOutcome::Failed(unavailable(
                "leader timed out",
            )));
            assert!(following.join().unwrap().is_err());
        });
        assert!(fixture.transfer.batches.lock().unwrap().is_empty());
        assert_eq!(fixture.cache.stats().unwrap().entries, 0);
    }

    #[test]
    fn native_peer_disk_cache_leader_revocation_does_not_revoke_valid_follower() {
        struct PausedTransfer {
            inner: Arc<NativeCacheTransfer>,
            entered: std::sync::mpsc::Sender<()>,
            release: Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl ChunkTransfer for PausedTransfer {
            fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
                self.inner.read_ranges(batch, out)?;
                self.entered.send(()).unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
                Ok(())
            }
        }
        let mut leader =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let mut follower =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        follower.engine.peer_cache = Some(leader.cache.clone());
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        leader.engine.transfer = Arc::new(PausedTransfer {
            inner: leader.transfer.clone(),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        std::thread::scope(|scope| {
            let reading = scope.spawn(|| leader.engine.read_batch_native_owned(&leader.batch, 2));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let following =
                scope.spawn(|| follower.engine.read_batch_native_owned(&follower.batch, 2));
            let deadline = Instant::now() + Duration::from_secs(3);
            while leader.cache.waiting_fill_readers_for_test() == 0 {
                assert!(
                    Instant::now() < deadline,
                    "follower did not join the paused leader flight"
                );
                std::thread::yield_now();
            }
            leader.engine.read_authority.stop();
            release_tx.send(()).unwrap();
            assert!(reading.join().unwrap().is_err());
            assert_eq!(following.join().unwrap().unwrap().unwrap(), [1, 2]);
        });
        assert_eq!(
            leader.cache.stats().unwrap().entries,
            0,
            "revoked precommit leader must not publish a Ready object"
        );
    }

    #[test]
    fn native_peer_disk_cache_four_same_object_readers_share_one_verified_fill() {
        let fixture = NativeCacheFixture::new(
            MAX_DFS_READ_BYTES,
            vec![(0, 64 * 1024)],
            Duration::from_millis(50),
        );
        let start = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            let mut readers = Vec::new();
            for _ in 0..4 {
                readers.push(scope.spawn(|| {
                    start.wait();
                    assert_eq!(
                        fixture
                            .engine
                            .read_batch_native_owned(&fixture.batch, 2)
                            .unwrap()
                            .unwrap(),
                        [1, 2]
                    );
                }));
            }
            for reader in readers {
                reader.join().unwrap();
            }
        });
        assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 1);
        assert_eq!(fixture.cache.stats().unwrap().entries, 1);
    }

    #[test]
    fn native_peer_disk_cache_fetches_current_object_once_and_reuses_without_peer_rpc() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [1, 2]
        );
        fixture.batch.ops[0].chunk_offset = 8;
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [8, 9]
        );
        let calls = fixture.transfer.batches.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operations[0].op.chunk_offset, 0);
        assert_eq!(calls[0].operations[0].op.length, 64 * 1024);
        assert_eq!(*fixture.sources.calls.lock().unwrap(), 1);
    }

    struct NativeStreamingCacheTransfer {
        inner: Arc<NativeCacheTransfer>,
        stream_supported: AtomicBool,
        streamed_calls: std::sync::atomic::AtomicUsize,
        fail_first: AtomicBool,
        deny_first: bool,
        stop_after_completion: Option<Arc<LocalReadAuthority>>,
    }

    impl ChunkTransfer for NativeStreamingCacheTransfer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            self.inner.read_ranges(batch, out)
        }

        fn supports_verified_object_sink(&self) -> bool {
            self.stream_supported.load(Ordering::Relaxed)
        }

        fn read_verified_object_to_sink<'output>(
            &self,
            batch: &PeerReadBatch,
            chunk: &ChunkObject,
            requested_offset: u64,
            out: &'output mut [u8],
            sink: &mut PeerStreamSink<'_>,
        ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
            validate_verified_peer_object_request(batch, chunk, chunk.length as usize)?;
            self.streamed_calls.fetch_add(1, Ordering::Relaxed);
            if self.fail_first.swap(false, Ordering::Relaxed) {
                out.fill(13);
                return Err(if self.deny_first {
                    external_denied("streamed grant rejected")
                } else {
                    corrupt("streamed completion failed")
                });
            }
            let bytes = self.inner.bytes.lock().unwrap();
            let completed = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                chunk,
                &bytes,
                requested_offset,
                out,
                sink,
            )?;
            if let Some(authority) = &self.stop_after_completion {
                authority.stop();
            }
            Ok(Some(completed))
        }
    }

    fn streaming_cache_transfer(fixture: &NativeCacheFixture) -> Arc<NativeStreamingCacheTransfer> {
        Arc::new(NativeStreamingCacheTransfer {
            inner: fixture.transfer.clone(),
            stream_supported: AtomicBool::new(true),
            streamed_calls: std::sync::atomic::AtomicUsize::new(0),
            fail_first: AtomicBool::new(false),
            deny_first: false,
            stop_after_completion: None,
        })
    }

    #[test]
    fn r7_prefetch_busy_source_lock_skips_without_queue_or_rpc() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let mut fixture = {
            let _entered = runtime.enter();
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO)
        };
        fixture.engine.config.readahead_max_bytes = 4 * 1024 * 1024;
        fixture.engine.sources_for(&fixture.batch).unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::read_work::ReadWorkMetrics::register(&registry).unwrap();
        fixture.engine.fetcher = super::super::dfs_fetch::ObjectFetcher::new(
            runtime.handle().clone(),
            fixture.cache.proof_budget(),
            8,
            Some(metrics),
        )
        .unwrap();
        let transfer = streaming_cache_transfer(&fixture);
        fixture.engine.transfer = transfer.clone();
        fixture.batch.lookahead = vec![fixture.chunk.id.clone()];
        fixture.batch.ops.clear();
        // The same thread owns the source lock. A blocking background lookup
        // would deadlock; try-only admission must finish with a busy event.
        let sources = fixture.engine.source_cache.lock().unwrap();
        fixture
            .engine
            .prefetch_cached(&fixture.batch, 1, Arc::new(|| Ok(())));
        drop(sources);
        assert!(!fixture.cache.has_fill_flight_for_test());
        assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.cache.stats().unwrap().entries, 0);
        assert_eq!(*fixture.sources.calls.lock().unwrap(), 1);
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(
            text.contains("afs_dfs_fetch_events_total{event=\"skip_busy\"} 1\n"),
            "{text}"
        );
        assert!(
            text.contains("afs_dfs_fetch_events_total{event=\"scheduled\"} 0\n"),
            "{text}"
        );
        runtime
            .block_on(fixture.engine.object_fetcher().unwrap().shutdown())
            .unwrap();
    }

    #[test]
    fn r7_dropping_a_task_reader_keeps_other_readers_source_scope_live() {
        let fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        fixture.engine.sources_for(&fixture.batch).unwrap();
        let generation = fixture.engine.source_cache.lock().unwrap().generation;
        drop(fixture.engine.task_reader());
        let cache = fixture.engine.source_cache.lock().unwrap();
        assert_eq!(cache.generation, generation);
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn r7_prefetch_missing_partial_and_expired_cached_grants_do_no_work() {
        for mode in ["missing", "partial", "expired"] {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let ranges = if mode == "partial" {
                vec![(1, 2)]
            } else {
                vec![(0, 64 * 1024)]
            };
            let mut fixture = {
                let _entered = runtime.enter();
                NativeCacheFixture::new(MAX_DFS_READ_BYTES, ranges, Duration::ZERO)
            };
            fixture.engine.config.readahead_max_bytes = 4 * 1024 * 1024;
            if mode != "missing" {
                fixture.engine.sources_for(&fixture.batch).unwrap();
            }
            if mode == "expired" {
                fixture
                    .engine
                    .source_cache
                    .lock()
                    .unwrap()
                    .entries
                    .retain(|_, entry| {
                        entry.value.expires_at = Instant::now();
                        true
                    });
            }
            let transfer = streaming_cache_transfer(&fixture);
            fixture.engine.transfer = transfer.clone();
            fixture.batch.lookahead = vec![fixture.chunk.id.clone()];
            fixture.batch.ops.clear();
            fixture
                .engine
                .prefetch_cached(&fixture.batch, 1, Arc::new(|| Ok(())));
            assert!(
                !fixture.cache.has_fill_flight_for_test(),
                "{mode}: no optional task may be admitted"
            );
            runtime
                .block_on(fixture.engine.object_fetcher().unwrap().shutdown())
                .unwrap();
            assert!(!fixture.cache.has_fill_flight_for_test());
            assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 0);
            assert_eq!(fixture.cache.stats().unwrap().entries, 0);
            assert_eq!(
                *fixture.sources.calls.lock().unwrap(),
                u64::from(mode != "missing")
            );
        }
    }

    #[test]
    fn r7_prefetch_rechecks_generation_scope_and_stop_after_verified_completion() {
        struct BeforeCommitTransfer {
            inner: Arc<NativeCacheTransfer>,
            invalidate: Arc<dyn Fn() + Send + Sync>,
            completed: Arc<AtomicBool>,
        }
        impl ChunkTransfer for BeforeCommitTransfer {
            fn read_ranges(&self, _: &PeerReadBatch, _: &mut [u8]) -> Result<()> {
                panic!("prefetch cannot fall back to a body Vec")
            }
            fn supports_verified_object_sink(&self) -> bool {
                true
            }
            fn try_read_verified_object_to_sink<'output>(
                &self,
                _: &PeerReadBatch,
                chunk: &ChunkObject,
                offset: u64,
                out: &'output mut [u8],
                sink: &mut PeerStreamSink<'_>,
                control: &PrefetchControl,
            ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
                control.check()?;
                assert!(out.is_empty());
                let bytes = self.inner.bytes.lock().unwrap();
                let receipt = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                    chunk, &bytes, offset, out, sink,
                )?;
                (self.invalidate)();
                self.completed.store(true, Ordering::Release);
                Ok(Some(receipt))
            }
        }
        for action in ["generation", "scope", "stop"] {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let mut fixture = {
                let _entered = runtime.enter();
                NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO)
            };
            fixture.engine.config.readahead_max_bytes = 4 * 1024 * 1024;
            fixture.engine.sources_for(&fixture.batch).unwrap();
            let valid = Arc::new(AtomicBool::new(true));
            let completed = Arc::new(AtomicBool::new(false));
            let cache = fixture.engine.source_cache.clone();
            let batch = fixture.batch.clone();
            let authority = fixture.engine.read_authority.clone();
            let invalid_scope = valid.clone();
            let invalidate = Arc::new(move || match action {
                "generation" => cache.lock().unwrap().invalidate_scope(
                    batch.file_version_id.as_ref().unwrap(),
                    &batch.layout_root_id,
                ),
                "scope" => invalid_scope.store(false, Ordering::Release),
                "stop" => authority.stop(),
                _ => unreachable!(),
            });
            fixture.engine.transfer = Arc::new(BeforeCommitTransfer {
                inner: fixture.transfer.clone(),
                invalidate,
                completed: completed.clone(),
            });
            fixture.batch.lookahead = vec![fixture.chunk.id.clone()];
            fixture.batch.ops.clear();
            fixture.engine.prefetch_cached(
                &fixture.batch,
                1,
                Arc::new(move || {
                    if valid.load(Ordering::Acquire) {
                        Ok(())
                    } else {
                        Err(unavailable("known handle denial"))
                    }
                }),
            );
            let deadline = Instant::now() + Duration::from_secs(3);
            while (!completed.load(Ordering::Acquire) || fixture.cache.has_fill_flight_for_test())
                && Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(
                completed.load(Ordering::Acquire),
                "{action}: verified completion must reach the invalidation barrier"
            );
            assert!(
                !fixture.cache.has_fill_flight_for_test(),
                "{action}: task must finish before close"
            );
            assert_eq!(
                fixture.cache.stats().unwrap().entries,
                0,
                "{action}: last authorization check must prevent Ready"
            );
            runtime
                .block_on(fixture.engine.object_fetcher().unwrap().shutdown())
                .unwrap();
            assert_eq!(*fixture.sources.calls.lock().unwrap(), 1);
        }
    }

    #[test]
    fn r7_expired_queue_joins_an_already_started_object_before_returning() {
        use std::sync::mpsc;
        struct GatedTransfer {
            inner: Arc<NativeCacheTransfer>,
            entered: mpsc::Sender<()>,
            release: Mutex<mpsc::Receiver<()>>,
        }
        impl ChunkTransfer for GatedTransfer {
            fn read_ranges(&self, _: &PeerReadBatch, _: &mut [u8]) -> Result<()> {
                panic!("the queued fanout object must use the controlled stream");
            }
            fn supports_verified_object_sink(&self) -> bool {
                true
            }
            fn read_verified_object_to_sink<'output>(
                &self,
                _: &PeerReadBatch,
                chunk: &ChunkObject,
                offset: u64,
                out: &'output mut [u8],
                sink: &mut PeerStreamSink<'_>,
            ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
                let bytes = self.inner.bytes.lock().unwrap();
                let completed = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                    chunk, &bytes, offset, out, sink,
                )?;
                self.entered.send(()).unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
                Ok(Some(completed))
            }
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let mut fixture = {
            let _entered = runtime.enter();
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO)
        };
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        fixture.engine.transfer = Arc::new(GatedTransfer {
            inner: fixture.transfer.clone(),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let engine = Arc::new(fixture.engine);
        let fetcher = engine.object_fetcher().unwrap();
        let permits: Vec<_> = (0..7)
            .map(|_| fetcher.try_reserve_background(1).unwrap().unwrap())
            .collect();
        let mut batch = fixture.batch;
        let mut second = batch.ops[0].clone();
        second.output_offset = 2;
        batch.ops.push(second);
        let sources = fixture.sources.reply.lock().unwrap().chunks.clone();
        let deadline = Instant::now() + Duration::from_millis(200);
        let (finished_tx, finished_rx) = mpsc::channel();
        let reading = std::thread::spawn(move || {
            let mut out = [91; 4];
            let result = engine.fanout_private_objects(&batch, &sources, &mut out, Some(deadline));
            finished_tx.send((result.is_err(), out)).unwrap();
        });
        let entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        std::thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(50),
        );
        let returned_before_join = finished_rx.try_recv().ok();
        let _ = release_tx.send(());
        drop(permits);
        reading.join().unwrap();
        let finished =
            returned_before_join.or_else(|| finished_rx.recv_timeout(Duration::from_secs(2)).ok());
        runtime.block_on(fetcher.shutdown()).unwrap();
        assert!(
            entered,
            "one object must actually begin before queue expiry"
        );
        assert!(
            returned_before_join.is_none(),
            "deadline failure cannot detach the first object"
        );
        assert_eq!(finished, Some((true, [91; 4])));
    }

    #[test]
    fn r7_saturated_foreground_queue_stops_at_the_original_grant_deadline() {
        use std::sync::mpsc;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let mut fixture = {
            let _entered = runtime.enter();
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO)
        };
        fixture.engine.transfer = streaming_cache_transfer(&fixture);
        let engine = Arc::new(fixture.engine);
        let fetcher = engine.object_fetcher().unwrap();
        let permits: Vec<_> = (0..8)
            .map(|_| fetcher.try_reserve_background(1).unwrap().unwrap())
            .collect();
        let mut batch = fixture.batch;
        let mut second = batch.ops[0].clone();
        second.chunk_offset = 8;
        second.output_offset = 2;
        batch.ops.push(second);
        let mut sources = fixture.sources.reply.lock().unwrap().chunks.clone();
        sources[0].sources[0].read_grant.expires_at_unix_ms = now_unix_ms().saturating_add(75);
        let (finished_tx, finished_rx) = mpsc::channel();
        let reading = std::thread::spawn(move || {
            let mut out = [91; 4];
            let result = engine.fanout_private_objects(&batch, &sources, &mut out, None);
            finished_tx.send((result.is_err(), out)).unwrap();
        });
        // Always release the original eight owners and join, even on RED.
        let expired_while_saturated = finished_rx.recv_timeout(Duration::from_secs(2)).ok();
        drop(permits);
        reading.join().unwrap();
        runtime.block_on(fetcher.shutdown()).unwrap();
        assert_eq!(
            expired_while_saturated,
            Some((true, [91; 4])),
            "an expired queued grant must fail without waiting for a fetch slot or changing private output"
        );
    }

    #[test]
    fn r7_fanout_degradation_uses_only_foreground_serial_range_fallback() {
        use std::sync::{
            Condvar,
            atomic::{AtomicU64, AtomicUsize},
            mpsc,
        };
        struct FallbackTransfer {
            mode: &'static str,
            bodies: BTreeMap<ChunkId, Vec<u8>>,
            foreground: Mutex<Option<std::thread::ThreadId>>,
            child_range: AtomicBool,
            range_calls: AtomicUsize,
            range_bytes: AtomicU64,
            active: AtomicUsize,
            peak_active: AtomicUsize,
            entered: mpsc::Sender<()>,
            stream_entered: mpsc::Sender<()>,
            release: Arc<(Mutex<bool>, Condvar)>,
        }
        impl ChunkTransfer for FallbackTransfer {
            fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
                batch.validate(out.len())?;
                if self.foreground.lock().unwrap().as_ref() != Some(&std::thread::current().id()) {
                    self.child_range.store(true, Ordering::SeqCst);
                }
                self.range_calls.fetch_add(1, Ordering::SeqCst);
                self.range_bytes.fetch_add(
                    batch.operations.iter().map(|item| item.op.length).sum(),
                    Ordering::SeqCst,
                );
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak_active.fetch_max(active, Ordering::SeqCst);
                let _ = self.entered.send(());
                let (lock, changed) = &*self.release;
                let released = lock.lock().unwrap();
                let _released = changed
                    .wait_timeout_while(released, Duration::from_secs(5), |released| !*released)
                    .unwrap();
                for item in &batch.operations {
                    let body = &self.bodies[&item.op.chunk_id];
                    let start = item.op.chunk_offset as usize;
                    let length = item.op.length as usize;
                    out[item.op.output_offset..item.op.output_offset + length]
                        .copy_from_slice(&body[start..start + length]);
                }
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(())
            }
            fn supports_verified_object_sink(&self) -> bool {
                self.mode != "unsupported"
            }
            fn read_verified_object_to_sink<'output>(
                &self,
                _: &PeerReadBatch,
                chunk: &ChunkObject,
                offset: u64,
                out: &'output mut [u8],
                sink: &mut PeerStreamSink<'_>,
            ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
                if self.mode == "sink_none"
                    || (self.mode == "sink_mixed" && self.bodies[&chunk.id][0] == 29)
                {
                    return Ok(None);
                }
                let completed = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                    chunk,
                    &self.bodies[&chunk.id],
                    offset,
                    out,
                    sink,
                )?;
                if matches!(self.mode, "sink_mixed" | "commit_failure") {
                    let _ = self.stream_entered.send(());
                    let (lock, changed) = &*self.release;
                    let released = lock.lock().unwrap();
                    let _released = changed
                        .wait_timeout_while(released, Duration::from_secs(5), |released| !*released)
                        .unwrap();
                }
                Ok(Some(completed))
            }
        }
        for mode in [
            "partial",
            "chunk_none",
            "unsupported",
            "sink_none",
            "capacity",
            "sink_mixed",
            "commit_failure",
        ] {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let temp = tempfile::tempdir().unwrap();
            let chunks = [
                StagedChunk::new(OperationId::new("r7-fallback-first"), vec![17; 64 * 1024]),
                StagedChunk::new(OperationId::new("r7-fallback-second"), vec![29; 64 * 1024]),
            ];
            let cache = Arc::new(
                crate::node::peer_cache::DiskPeerCache::open(
                    crate::node::peer_cache::PeerCacheOptions {
                        directory: temp.path().join("cache"),
                        capacity_bytes: if mode == "capacity" {
                            8192
                        } else {
                            1024 * 1024
                        },
                        namespace_id: crate::dfs::NamespaceId::new("default"),
                    },
                )
                .unwrap(),
            );
            let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
                revision: 1,
                chunks: chunks
                    .iter()
                    .enumerate()
                    .map(|(index, staged)| ChunkSources {
                        chunk_id: staged.chunk.id.clone(),
                        chunk: if mode == "chunk_none" {
                            None
                        } else {
                            Some(staged.chunk.clone())
                        },
                        allowed_ranges: vec![(
                            0,
                            if mode == "partial" {
                                8
                            } else {
                                staged.chunk.length
                            },
                        )],
                        sources: vec![source(
                            if index == 0 { "copy-b" } else { "copy-c" },
                            &staged.chunk.id,
                            if index == 0 { "node-b" } else { "node-c" },
                        )],
                    })
                    .collect(),
            }));
            let (entered_tx, entered_rx) = mpsc::channel();
            let (stream_tx, stream_rx) = mpsc::channel();
            let release = Arc::new((Mutex::new(false), Condvar::new()));
            let transfer = Arc::new(FallbackTransfer {
                mode,
                bodies: chunks
                    .iter()
                    .map(|staged| (staged.chunk.id.clone(), staged.bytes().to_vec()))
                    .collect(),
                foreground: Mutex::new(None),
                child_range: AtomicBool::new(false),
                range_calls: AtomicUsize::new(0),
                range_bytes: AtomicU64::new(0),
                active: AtomicUsize::new(0),
                peak_active: AtomicUsize::new(0),
                entered: entered_tx,
                stream_entered: stream_tx,
                release: release.clone(),
            });
            let engine = {
                let _entered = runtime.enter();
                Arc::new(
                    DfsReadEngine::new(
                        crate::dfs::NamespaceId::new("default"),
                        "node-a".into(),
                        Arc::new(
                            LocalChunkStore::open(temp.path().join("local"), "node-a").unwrap(),
                        ),
                        provider,
                        transfer.clone(),
                        DfsReadConfig::default(),
                    )
                    .with_read_authority(Arc::new(LocalReadAuthority::new(
                        1,
                        Instant::now() + Duration::from_secs(30),
                    )))
                    .with_peer_cache(cache),
                )
            };
            let fetcher = engine.object_fetcher().unwrap();
            let batch = ReadBatch {
                file_version_id: Some(FileVersionId::new("version")),
                layout_root_id: LayoutRootId::new("layout"),
                lookahead: Vec::new(),
                ops: chunks
                    .iter()
                    .enumerate()
                    .map(|(index, staged)| ChunkReadOp {
                        chunk_id: staged.chunk.id.clone(),
                        chunk_offset: 0,
                        length: 8,
                        output_offset: index * 8,
                    })
                    .collect(),
            };
            let driver_transfer = transfer.clone();
            let reading = std::thread::spawn(move || {
                *driver_transfer.foreground.lock().unwrap() = Some(std::thread::current().id());
                let mut out = [91; 16];
                let result = engine.read_batch_native_into_private(&batch, &mut out);
                (result, out)
            });
            let (first_entered, second_before_release) =
                if matches!(mode, "sink_mixed" | "commit_failure") {
                    let needed = if mode == "commit_failure" { 2 } else { 1 };
                    let entered =
                        (0..needed).all(|_| stream_rx.recv_timeout(Duration::from_secs(2)).is_ok());
                    if mode == "commit_failure" && entered {
                        // Both bodies/proofs are complete. Remove only this test's
                        // owned pending paths before commit, leaving the pinned
                        // verified body valid while forcing cache installation Err.
                        for entry in std::fs::read_dir(temp.path().join("cache")).unwrap() {
                            let entry = entry.unwrap();
                            if entry
                                .file_name()
                                .to_str()
                                .is_some_and(|name| name.starts_with(".afs-peer-cache.pending-"))
                            {
                                std::fs::remove_file(entry.path()).unwrap();
                            }
                        }
                    }
                    (
                        entered,
                        entered_rx.recv_timeout(Duration::from_millis(100)).is_ok(),
                    )
                } else {
                    (
                        entered_rx.recv_timeout(Duration::from_secs(2)).is_ok(),
                        entered_rx.recv_timeout(Duration::from_millis(100)).is_ok(),
                    )
                };
            *release.0.lock().unwrap() = true;
            release.1.notify_all();
            let (result, out) = reading.join().unwrap();
            runtime.block_on(fetcher.shutdown()).unwrap();
            result.unwrap();
            assert_eq!(&out[..8], &[17; 8]);
            assert_eq!(&out[8..], &[29; 8]);
            assert!(
                first_entered,
                "{mode}: a controlled operation must actually begin"
            );
            assert!(
                !second_before_release,
                "{mode}: different peer ranges cannot bypass the shared stream boundary in parallel"
            );
            assert!(
                !transfer.child_range.load(Ordering::SeqCst),
                "{mode}: no fanout child may issue a range or legacy-body RPC"
            );
            assert_eq!(
                transfer.peak_active.load(Ordering::SeqCst),
                usize::from(mode != "commit_failure"),
                "{mode}"
            );
            assert_eq!(
                transfer.range_calls.load(Ordering::SeqCst),
                if mode == "commit_failure" { 0 } else { 2 },
                "{mode}"
            );
            assert_eq!(
                transfer.range_bytes.load(Ordering::SeqCst),
                if mode == "commit_failure" { 0 } else { 16 },
                "{mode}: fallback cannot fetch complete legacy bodies in children"
            );
            assert_eq!(
                transfer.active.load(Ordering::SeqCst),
                0,
                "{mode}: every range owner must drain"
            );
        }
    }

    #[test]
    fn r7_cross_object_private_read_overlaps_two_sink_fills_and_joins_before_delivery() {
        use std::sync::{Condvar, mpsc};
        struct CrossObjectTransfer {
            bodies: BTreeMap<ChunkId, Vec<u8>>,
            entered: mpsc::Sender<ChunkId>,
            released: Arc<(Mutex<bool>, Condvar)>,
        }
        impl ChunkTransfer for CrossObjectTransfer {
            fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
                for item in &batch.operations {
                    let body = &self.bodies[&item.op.chunk_id];
                    let start = item.op.chunk_offset as usize;
                    let length = item.op.length as usize;
                    out[item.op.output_offset..item.op.output_offset + length]
                        .copy_from_slice(&body[start..start + length]);
                }
                Ok(())
            }
            fn supports_verified_object_sink(&self) -> bool {
                true
            }
            fn read_verified_object_to_sink<'output>(
                &self,
                _batch: &PeerReadBatch,
                chunk: &ChunkObject,
                offset: u64,
                out: &'output mut [u8],
                sink: &mut PeerStreamSink<'_>,
            ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
                self.entered.send(chunk.id.clone()).unwrap();
                let (lock, changed) = &*self.released;
                let release = lock.lock().unwrap();
                let _release = changed
                    .wait_timeout_while(release, Duration::from_secs(5), |release| !*release)
                    .unwrap();
                crate::node::rpc::peer::complete_peer_disk_read_for_test(
                    chunk,
                    &self.bodies[&chunk.id],
                    offset,
                    out,
                    sink,
                )
                .map(Some)
            }
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let chunks = [
            StagedChunk::new(OperationId::new("r7-cross-first"), vec![17; 64 * 1024]),
            StagedChunk::new(OperationId::new("r7-cross-second"), vec![29; 64 * 1024]),
        ];
        let (entered_tx, entered_rx) = mpsc::channel();
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        let cache = Arc::new(
            crate::node::peer_cache::DiskPeerCache::open(
                crate::node::peer_cache::PeerCacheOptions {
                    directory: temp.path().join("cache"),
                    capacity_bytes: 1024 * 1024,
                    namespace_id: crate::dfs::NamespaceId::new("default"),
                },
            )
            .unwrap(),
        );
        let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: chunks
                .iter()
                .map(|chunk| ChunkSources {
                    chunk: Some(chunk.chunk.clone()),
                    chunk_id: chunk.chunk.id.clone(),
                    allowed_ranges: vec![(0, chunk.chunk.length)],
                    sources: vec![source("b", &chunk.chunk.id, "node-b")],
                })
                .collect(),
        }));
        let transfer = Arc::new(CrossObjectTransfer {
            bodies: chunks
                .iter()
                .map(|chunk| (chunk.chunk.id.clone(), chunk.bytes().to_vec()))
                .collect(),
            entered: entered_tx,
            released: released.clone(),
        });
        let engine = {
            let _entered = runtime.enter();
            DfsReadEngine::new(
                crate::dfs::NamespaceId::new("default"),
                "node-a".into(),
                Arc::new(LocalChunkStore::open(temp.path().join("local"), "node-a").unwrap()),
                provider,
                transfer,
                DfsReadConfig::default(),
            )
            .with_read_authority(Arc::new(LocalReadAuthority::new(
                1,
                Instant::now() + Duration::from_secs(30),
            )))
            .with_peer_cache(cache.clone())
        };
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: chunks
                .iter()
                .enumerate()
                .map(|(index, chunk)| ChunkReadOp {
                    chunk_id: chunk.chunk.id.clone(),
                    chunk_offset: if index == 0 {
                        chunk.chunk.length - 8
                    } else {
                        0
                    },
                    length: 8,
                    output_offset: index * 8,
                })
                .collect(),
        };
        let reading = std::thread::spawn(move || {
            let mut out = [0; 16];
            engine
                .read_batch_native_into_private(&batch, &mut out)
                .unwrap();
            out
        });
        let first = entered_rx.recv_timeout(Duration::from_secs(2)).ok();
        let second = entered_rx.recv_timeout(Duration::from_secs(2)).ok();
        *released.0.lock().unwrap() = true;
        released.1.notify_all();
        let out = reading.join().unwrap();
        assert!(
            first.is_some() && second.is_some() && first != second,
            "two objects in one private read must overlap their sink fills"
        );
        assert_eq!(&out[..8], &[17; 8]);
        assert_eq!(&out[8..], &[29; 8]);
        assert_eq!(cache.stats().unwrap().entries, 2);
    }

    #[test]
    fn native_stream_fill_success_and_hit_never_invoke_legacy_full_body_transfer() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let transfer = streaming_cache_transfer(&fixture);
        fixture.engine.transfer = transfer.clone();
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [1, 2]
        );
        assert_eq!(fixture.cache.stats().unwrap().entries, 1);
        fixture.batch.ops[0].chunk_offset = 8;
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [8, 9]
        );
        assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 1);
        assert!(fixture.transfer.batches.lock().unwrap().is_empty());
    }

    #[test]
    fn native_stream_fill_revocation_before_commit_returns_no_output_or_ready_object() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let transfer = Arc::new(NativeStreamingCacheTransfer {
            inner: fixture.transfer.clone(),
            stream_supported: AtomicBool::new(true),
            streamed_calls: std::sync::atomic::AtomicUsize::new(0),
            fail_first: AtomicBool::new(false),
            deny_first: false,
            stop_after_completion: Some(fixture.engine.read_authority.clone()),
        });
        fixture.engine.transfer = transfer.clone();
        assert!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .is_err()
        );
        assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.cache.stats().unwrap().entries, 0);
        assert!(fixture.transfer.batches.lock().unwrap().is_empty());
    }

    #[test]
    fn native_stream_fill_transfer_failure_never_publishes_or_retries_legacy_path() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let transfer = streaming_cache_transfer(&fixture);
        transfer.fail_first.store(true, Ordering::Relaxed);
        fixture.engine.transfer = transfer.clone();
        assert!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .is_err()
        );
        assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.cache.stats().unwrap().entries, 0);
        assert!(fixture.transfer.batches.lock().unwrap().is_empty());
    }

    #[test]
    fn native_stream_fill_cache_quota_failure_keeps_same_rpc_verified_range_success() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let cache = Arc::new(
            DiskPeerCache::open(crate::node::peer_cache::PeerCacheOptions {
                directory: fixture._temp.path().join("stream-tiny-cache"),
                capacity_bytes: 16,
                namespace_id: crate::dfs::NamespaceId::new("default"),
            })
            .unwrap(),
        );
        fixture.engine.peer_cache = Some(cache.clone());
        let transfer = streaming_cache_transfer(&fixture);
        fixture.engine.transfer = transfer.clone();
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [1, 2]
        );
        assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 1);
        assert_eq!(cache.stats().unwrap().entries, 0);
        assert!(fixture.transfer.batches.lock().unwrap().is_empty());
    }

    #[test]
    fn native_stream_fill_retry_overwrites_failed_private_range_and_does_not_mix_proofs() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        fixture.sources.reply.lock().unwrap().chunks[0]
            .sources
            .push(source("c", &fixture.chunk.id, "node-c"));
        let transfer = streaming_cache_transfer(&fixture);
        transfer.fail_first.store(true, Ordering::Relaxed);
        fixture.engine.transfer = transfer.clone();
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [1, 2]
        );
        assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 2);
        assert_eq!(fixture.cache.stats().unwrap().entries, 1);
        assert!(fixture.transfer.batches.lock().unwrap().is_empty());
    }

    #[test]
    fn native_stream_fill_known_denial_stops_other_copy_retry_and_clears_sources() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        fixture.sources.reply.lock().unwrap().chunks[0]
            .sources
            .push(source("c", &fixture.chunk.id, "node-c"));
        let transfer = Arc::new(NativeStreamingCacheTransfer {
            inner: fixture.transfer.clone(),
            stream_supported: AtomicBool::new(true),
            streamed_calls: std::sync::atomic::AtomicUsize::new(0),
            fail_first: AtomicBool::new(true),
            deny_first: true,
            stop_after_completion: None,
        });
        fixture.engine.transfer = transfer.clone();
        assert!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .is_err()
        );
        assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.cache.stats().unwrap().entries, 0);
        assert!(fixture.engine.source_cache.lock().unwrap().is_empty());
        assert!(fixture.engine.capture_native_read_authority().is_ok());
        assert!(fixture.transfer.batches.lock().unwrap().is_empty());
    }

    #[test]
    fn native_stream_fill_unsupported_or_smaller_budget_preserves_legacy_transfer() {
        for (supported, budget) in [(false, MAX_DFS_READ_BYTES), (true, 128 * 1024)] {
            let mut fixture = NativeCacheFixture::new(budget, vec![(0, 64 * 1024)], Duration::ZERO);
            let transfer = streaming_cache_transfer(&fixture);
            transfer
                .stream_supported
                .store(supported, Ordering::Relaxed);
            fixture.engine.transfer = transfer.clone();
            assert_eq!(
                fixture
                    .engine
                    .read_batch_native_owned(&fixture.batch, 2)
                    .unwrap()
                    .unwrap(),
                [1, 2]
            );
            assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 0);
            assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 1);
            assert_eq!(fixture.cache.stats().unwrap().entries, 1);
        }
    }

    #[test]
    fn native_peer_disk_cache_partial_grant_or_fill_budget_preserves_range_fallback() {
        for (budget, ranges) in [
            (MAX_DFS_READ_BYTES, vec![(1, 2)]),
            (6, vec![(0, 64 * 1024)]),
            (MAX_DFS_READ_BYTES, Vec::new()),
        ] {
            let fixture = NativeCacheFixture::new(budget, ranges, Duration::ZERO);
            assert_eq!(
                fixture
                    .engine
                    .read_batch_native_owned(&fixture.batch, 2)
                    .unwrap()
                    .unwrap(),
                [1, 2]
            );
            let calls = fixture.transfer.batches.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].operations[0].op.chunk_offset, 1);
            assert_eq!(calls[0].operations[0].op.length, 2);
            assert!(
                !fixture
                    .cache
                    .operation()
                    .unwrap()
                    .read_range(&fixture.chunk, 1, &mut [0; 2])
                    .unwrap()
            );
        }
    }

    #[test]
    fn native_peer_disk_cache_denies_outside_grant_without_cache_or_peer_success() {
        let fixture = NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 1)], Duration::ZERO);
        let mut output = [b'!'; 2];
        assert_eq!(
            fixture
                .engine
                .read_batch(&fixture.batch, &mut output)
                .unwrap_err()
                .kind(),
            afs_error::ErrorKind::PermissionDenied
        );
        assert_eq!(output, [b'!'; 2]);
        assert!(fixture.transfer.batches.lock().unwrap().is_empty());
        assert!(fixture.engine.source_cache.lock().unwrap().is_empty());
        assert!(fixture.engine.capture_native_read_authority().is_ok());
    }

    #[test]
    fn native_peer_disk_cache_rejects_complete_hash_mismatch_before_admission_or_output() {
        let fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        fixture.transfer.corrupt.store(true, Ordering::Relaxed);
        assert!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .is_err()
        );
        assert!(
            !fixture
                .cache
                .operation()
                .unwrap()
                .read_range(&fixture.chunk, 1, &mut [0; 2])
                .unwrap()
        );
    }

    #[test]
    fn native_peer_disk_cache_final_grant_expiry_does_not_admit_or_publish() {
        let fixture = NativeCacheFixture::new(
            MAX_DFS_READ_BYTES,
            vec![(0, 64 * 1024)],
            Duration::from_millis(100),
        );
        fixture.sources.reply.lock().unwrap().chunks[0].sources[0]
            .read_grant
            .expires_at_unix_ms = now_unix_ms() + 50;
        assert!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .is_err()
        );
        assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 1);
        assert!(fixture.engine.source_cache.lock().unwrap().is_empty());
        assert!(fixture.engine.capture_native_read_authority().is_ok());
        assert!(
            !fixture
                .cache
                .operation()
                .unwrap()
                .read_range(&fixture.chunk, 1, &mut [0; 2])
                .unwrap()
        );
    }

    #[test]
    fn native_peer_disk_cache_new_version_cannot_reuse_old_object_content() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [1, 2]
        );
        let bytes = vec![97; 64 * 1024];
        let next = StagedChunk::new(OperationId::new("next-native-cache"), bytes.clone());
        let mut next_source = source("next-copy", &next.chunk.id, "node-b");
        next_source.read_grant.file_version_id = FileVersionId::new("next-version");
        fixture.sources.reply.lock().unwrap().chunks = vec![ChunkSources {
            allowed_ranges: vec![(0, next.chunk.length)],
            chunk: Some(next.chunk.clone()),
            chunk_id: next.chunk.id.clone(),
            sources: vec![next_source],
        }];
        *fixture.transfer.bytes.lock().unwrap() = bytes;
        fixture.batch.file_version_id = Some(FileVersionId::new("next-version"));
        fixture.batch.ops[0].chunk_id = next.chunk.id;
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [97, 97]
        );
        assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 2);
    }

    #[test]
    fn native_peer_disk_cache_hit_still_requires_current_grant() {
        let fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        assert!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .is_some()
        );
        fixture.engine.source_cache.lock().unwrap().clear();
        fixture.sources.reply.lock().unwrap().chunks[0].sources[0]
            .read_grant
            .expires_at_unix_ms = 0;
        let mut output = [b'!'; 2];
        assert!(
            fixture
                .engine
                .read_batch(&fixture.batch, &mut output)
                .is_err()
        );
        assert_eq!(output, [b'!'; 2]);
        assert_eq!(
            fixture.transfer.batches.lock().unwrap().len(),
            1,
            "neither disk cache nor an expired grant may bypass fresh Meta authority"
        );
        assert!(fixture.engine.capture_native_read_authority().is_ok());
    }

    #[test]
    fn native_peer_disk_cache_corruption_outside_requested_range_refetches_peer() {
        use std::os::unix::fs::MetadataExt;

        let identity = |metadata: &std::fs::Metadata| {
            (
                metadata.dev(),
                metadata.ino(),
                metadata.len(),
                metadata.mtime(),
                metadata.mtime_nsec(),
                metadata.ctime(),
                metadata.ctime_nsec(),
            )
        };
        let fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [1, 2]
        );
        let mut directories = vec![fixture._temp.path().join("cache")];
        let mut damaged = false;
        while let Some(directory) = directories.pop() {
            for entry in std::fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    directories.push(path);
                } else if path.metadata().unwrap().len() >= fixture.chunk.length {
                    let before = identity(&path.metadata().unwrap());
                    let mut bytes = std::fs::read(&path).unwrap();
                    assert_eq!(&bytes[..8], b"AFSPC002");
                    // v2 places the body after a fixed 4096-byte header, with
                    // proof metadata afterwards. Damage the body, not the proof.
                    let last = 4096 + usize::try_from(fixture.chunk.length).unwrap() - 1;
                    assert!(last < bytes.len());
                    bytes[last] ^= 1; // Deliberately outside the requested range.
                    std::fs::write(&path, bytes).unwrap();
                    // Force an observable identity change even when both writes
                    // fall in one filesystem timestamp tick. The full-root path
                    // must reject this damage before falling back to Peer.
                    std::fs::File::open(&path)
                        .unwrap()
                        .set_modified(std::time::UNIX_EPOCH)
                        .unwrap();
                    assert_ne!(before, identity(&path.metadata().unwrap()));
                    damaged = true;
                }
            }
        }
        assert!(damaged, "verified object must exist on disk");
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [1, 2]
        );
        assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 2);
        assert_eq!(fixture.cache.stats().unwrap().corruptions, 1);
    }

    #[test]
    fn native_peer_disk_cache_admission_failure_does_not_fail_verified_read() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let cache = Arc::new(
            DiskPeerCache::open(crate::node::peer_cache::PeerCacheOptions {
                directory: fixture._temp.path().join("small-cache"),
                capacity_bytes: 4096,
                namespace_id: crate::dfs::NamespaceId::new("default"),
            })
            .unwrap(),
        );
        fixture.engine.peer_cache = Some(cache.clone());
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [1, 2]
        );
        assert_eq!(cache.stats().unwrap().entries, 0);
        assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 1);
    }

    #[test]
    fn native_peer_disk_cache_borrowed_miss_does_not_expand_fill_but_can_use_verified_hit() {
        let fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let mut output = [b'!'; 2];
        fixture
            .engine
            .read_batch(&fixture.batch, &mut output)
            .unwrap();
        assert_eq!(output, [1, 2]);
        assert_eq!(
            fixture.transfer.batches.lock().unwrap()[0].operations[0]
                .op
                .length,
            2,
            "borrowed caller plus aggregate staging must not allocate a third full payload"
        );
        assert_eq!(fixture.cache.stats().unwrap().entries, 0);
        assert!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .is_some()
        );
        let calls_before_hit = fixture.transfer.batches.lock().unwrap().len();
        fixture
            .engine
            .read_batch(&fixture.batch, &mut output)
            .unwrap();
        assert_eq!(output, [1, 2]);
        assert_eq!(
            fixture.transfer.batches.lock().unwrap().len(),
            calls_before_hit
        );
    }

    struct NativeVerifiedTransfer {
        inner: Arc<NativeCacheTransfer>,
        verified_calls: std::sync::atomic::AtomicUsize,
        fail_first: AtomicBool,
        deny_first: bool,
        stop_after_proof: Option<Arc<LocalReadAuthority>>,
    }

    impl ChunkTransfer for NativeVerifiedTransfer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            self.inner.read_ranges(batch, out)
        }

        fn read_verified_object<'a>(
            &self,
            batch: &PeerReadBatch,
            chunk: &'a ChunkObject,
            out: &'a mut [u8],
            proof_budget: Arc<OutboardBudget>,
        ) -> Result<VerifiedPeerChunk<'a>> {
            validate_verified_peer_object_request(batch, chunk, out.len())?;
            self.verified_calls.fetch_add(1, Ordering::Relaxed);
            if self.fail_first.swap(false, Ordering::Relaxed) {
                out.fill(13);
                return Err(if self.deny_first {
                    external_denied("known grant rejection")
                } else {
                    corrupt("failed full-object completion")
                });
            }
            self.inner.read_ranges(batch, out)?;
            let verified = VerifiedPeerChunk::verify(chunk, out, proof_budget)?;
            if let Some(authority) = &self.stop_after_proof {
                authority.stop();
            }
            Ok(verified)
        }
    }

    #[test]
    fn native_verified_object_default_rejects_partial_or_changed_identity_before_transfer() {
        let fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let candidate = fixture.sources.reply.lock().unwrap().chunks[0].sources[0].clone();
        let mut full = PeerReadBatch {
            file_version_id: FileVersionId::new("version"),
            layout_root_id: LayoutRootId::new("layout"),
            operations: vec![ResolvedReadOp {
                op: ChunkReadOp {
                    chunk_id: fixture.chunk.id.clone(),
                    chunk_offset: 0,
                    length: fixture.chunk.length,
                    output_offset: 0,
                },
                source: candidate,
            }],
        };
        let mut body = vec![0; fixture.chunk.length as usize];
        full.operations[0].op.length -= 1;
        assert!(
            fixture
                .transfer
                .read_verified_object(
                    &full,
                    &fixture.chunk,
                    &mut body,
                    fixture.cache.proof_budget()
                )
                .is_err()
        );
        full.operations[0].op.length += 1;
        full.operations[0].op.chunk_offset = 1;
        assert!(
            fixture
                .transfer
                .read_verified_object(
                    &full,
                    &fixture.chunk,
                    &mut body,
                    fixture.cache.proof_budget()
                )
                .is_err()
        );
        full.operations[0].op.chunk_offset = 0;
        let mut other = fixture.chunk.clone();
        other.id = ChunkId::new("different-root-object");
        assert!(
            fixture
                .transfer
                .read_verified_object(&full, &other, &mut body, fixture.cache.proof_budget())
                .is_err()
        );
        assert!(fixture.transfer.batches.lock().unwrap().is_empty());
        let verified = fixture
            .transfer
            .read_verified_object(
                &full,
                &fixture.chunk,
                &mut body,
                fixture.cache.proof_budget(),
            )
            .unwrap();
        assert_eq!(verified.chunk(), &fixture.chunk);
        assert_eq!(verified.bytes().len(), fixture.chunk.length as usize);
    }

    #[test]
    fn native_verified_object_retry_consumes_only_successful_full_object_proof() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        fixture.sources.reply.lock().unwrap().chunks[0]
            .sources
            .push(source("c", &fixture.chunk.id, "node-c"));
        let transfer = Arc::new(NativeVerifiedTransfer {
            inner: fixture.transfer.clone(),
            verified_calls: std::sync::atomic::AtomicUsize::new(0),
            fail_first: AtomicBool::new(true),
            deny_first: false,
            stop_after_proof: None,
        });
        fixture.engine.transfer = transfer.clone();
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [1, 2]
        );
        assert_eq!(transfer.verified_calls.load(Ordering::Relaxed), 2);
        assert_eq!(
            fixture.transfer.batches.lock().unwrap().len(),
            1,
            "engine must not repeat ordinary range transfer after obtaining proof"
        );
        assert_eq!(fixture.cache.stats().unwrap().entries, 1);
    }

    #[test]
    fn native_verified_object_known_denial_does_not_retry_another_copy() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        fixture.sources.reply.lock().unwrap().chunks[0]
            .sources
            .push(source("c", &fixture.chunk.id, "node-c"));
        let transfer = Arc::new(NativeVerifiedTransfer {
            inner: fixture.transfer.clone(),
            verified_calls: std::sync::atomic::AtomicUsize::new(0),
            fail_first: AtomicBool::new(true),
            deny_first: true,
            stop_after_proof: None,
        });
        fixture.engine.transfer = transfer.clone();
        assert!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .is_err()
        );
        assert_eq!(transfer.verified_calls.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.cache.stats().unwrap().entries, 0);
        assert!(fixture.engine.source_cache.lock().unwrap().is_empty());
        assert!(fixture.engine.capture_native_read_authority().is_ok());
    }

    #[test]
    fn native_verified_object_late_node_stop_rejects_proof_before_admission() {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let transfer = Arc::new(NativeVerifiedTransfer {
            inner: fixture.transfer.clone(),
            verified_calls: std::sync::atomic::AtomicUsize::new(0),
            fail_first: AtomicBool::new(false),
            deny_first: false,
            stop_after_proof: Some(fixture.engine.read_authority.clone()),
        });
        fixture.engine.transfer = transfer;
        assert!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .is_err()
        );
        assert_eq!(fixture.cache.stats().unwrap().entries, 0);
    }

    struct NativeObservedVerificationTransfer {
        inner: Arc<NativeCacheTransfer>,
        metrics: crate::node::read_work::ReadWorkMetrics,
    }

    impl ChunkTransfer for NativeObservedVerificationTransfer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            self.inner.read_ranges(batch, out)
        }

        fn verification_metrics(&self) -> Option<&crate::node::read_work::ReadWorkMetrics> {
            Some(&self.metrics)
        }
    }

    #[test]
    fn native_verified_object_fallback_counts_root_failure_without_unperformed_segment_hash() {
        for corrupt_root in [false, true] {
            let fixture =
                NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
            fixture
                .transfer
                .corrupt
                .store(corrupt_root, Ordering::Relaxed);
            let registry = afs_metrics::Registry::new();
            let transfer = NativeObservedVerificationTransfer {
                inner: fixture.transfer.clone(),
                metrics: crate::node::read_work::ReadWorkMetrics::register(&registry).unwrap(),
            };
            let batch = PeerReadBatch {
                file_version_id: FileVersionId::new("version"),
                layout_root_id: LayoutRootId::new("layout"),
                operations: vec![ResolvedReadOp {
                    op: ChunkReadOp {
                        chunk_id: fixture.chunk.id.clone(),
                        chunk_offset: 0,
                        length: fixture.chunk.length,
                        output_offset: 0,
                    },
                    source: fixture.sources.reply.lock().unwrap().chunks[0].sources[0].clone(),
                }],
            };
            let mut body = vec![0; fixture.chunk.length as usize];
            assert_eq!(
                transfer
                    .read_verified_object(
                        &batch,
                        &fixture.chunk,
                        &mut body,
                        fixture.cache.proof_budget()
                    )
                    .is_err(),
                corrupt_root
            );
            let family = registry
                .gather()
                .into_iter()
                .find(|family| family.name() == "afs_dfs_read_hash_bytes_total")
                .unwrap();
            let count = |stage: &str| {
                family
                    .get_metric()
                    .iter()
                    .find(|metric| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == "stage" && label.value() == stage)
                    })
                    .unwrap()
                    .get_counter()
                    .get_value() as u64
            };
            assert_eq!(count("root_fallback"), fixture.chunk.length);
            assert_eq!(count("segment_index_bytes"), 0);
            assert_eq!(count("root_in_receive"), 0);
        }
    }

    #[test]
    fn native_peer_cache_fill_stage_metrics_exclude_hits_and_retain_failed_receive() {
        for receive_failure in [false, true] {
            let mut fixture =
                NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
            let registry = afs_metrics::Registry::new();
            let metrics = crate::node::read_work::ReadWorkMetrics::register(&registry).unwrap();
            fixture.engine.local = Arc::new(
                LocalChunkStore::open(fixture._temp.path().join("timing-local"), "node-a")
                    .unwrap()
                    .with_read_work_metrics(metrics),
            );
            if receive_failure {
                fixture.engine.transfer = Arc::new(NativeVerifiedTransfer {
                    inner: fixture.transfer.clone(),
                    verified_calls: std::sync::atomic::AtomicUsize::new(0),
                    fail_first: AtomicBool::new(true),
                    deny_first: true,
                    stop_after_proof: None,
                });
            }
            assert_eq!(
                fixture
                    .engine
                    .read_batch_native_owned(&fixture.batch, 2)
                    .is_err(),
                receive_failure
            );
            if !receive_failure {
                assert_eq!(
                    fixture
                        .engine
                        .read_batch_native_owned(&fixture.batch, 2)
                        .unwrap()
                        .unwrap(),
                    [1, 2]
                );
                assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 1);
            }
            let family = registry
                .gather()
                .into_iter()
                .find(|family| {
                    family.name() == "afs_dfs_peer_disk_cache_fill_stage_observations_total"
                })
                .unwrap();
            let count = |stage: &str| {
                family
                    .get_metric()
                    .iter()
                    .find(|metric| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == "stage" && label.value() == stage)
                    })
                    .unwrap()
                    .get_counter()
                    .get_value() as u64
            };
            for stage in ["lock_wait", "lookup", "receive_verify", "total"] {
                assert_eq!(count(stage), 1, "{stage}: failure={receive_failure}");
            }
            assert_eq!(count("prepare"), 2);
            for stage in ["store", "deliver"] {
                assert_eq!(
                    count(stage),
                    u64::from(!receive_failure),
                    "{stage}: failure={receive_failure}"
                );
            }
        }
    }

    struct OwnedTransferProbe {
        destination: std::sync::atomic::AtomicUsize,
        fail_first: AtomicBool,
        fail_all: bool,
        expire_before_return: bool,
    }

    impl ChunkTransfer for OwnedTransferProbe {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            batch.validate(out.len())?;
            self.destination
                .store(out.as_ptr() as usize, Ordering::Relaxed);
            out.fill(b'?');
            if self.fail_all || self.fail_first.swap(false, Ordering::Relaxed) {
                return Err(corrupt("injected late stream completion failure"));
            }
            out.fill(b'x');
            if self.expire_before_return {
                let deadline = batch.operations[0].source.read_grant.expires_at_unix_ms;
                while now_unix_ms() < deadline {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            Ok(())
        }
    }

    #[test]
    fn native_owned_peer_returns_the_transfer_destination_and_discards_failed_attempts() {
        for fail_first in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let chunk = ChunkId::new("missing-owned");
            let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
                revision: 1,
                chunks: vec![ChunkSources {
                    allowed_ranges: Vec::new(),
                    chunk: None,
                    chunk_id: chunk.clone(),
                    sources: vec![source("b", &chunk, "node-b"), source("c", &chunk, "node-c")],
                }],
            }));
            let transfer = Arc::new(OwnedTransferProbe {
                destination: std::sync::atomic::AtomicUsize::new(0),
                fail_first: AtomicBool::new(fail_first),
                fail_all: false,
                expire_before_return: false,
            });
            let engine = DfsReadEngine::new(
                crate::dfs::NamespaceId::new("default"),
                "node-a".into(),
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources,
                transfer.clone(),
                DfsReadConfig::default(),
            );
            let batch = ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(FileVersionId::new("version")),
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![ChunkReadOp {
                    chunk_id: chunk,
                    chunk_offset: 0,
                    length: 6,
                    output_offset: 0,
                }],
            };
            let result = engine.read_batch_native_owned(&batch, 6).unwrap().unwrap();
            assert_eq!(result, b"xxxxxx");
            assert_eq!(
                result.as_ptr() as usize,
                transfer.destination.load(Ordering::Relaxed)
            );
        }
    }

    #[test]
    fn native_owned_peer_rejects_late_failure_expiry_and_excess_budget() {
        for expire_before_return in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let chunk = ChunkId::new("missing-owned");
            let mut candidate = source("b", &chunk, "node-b");
            if expire_before_return {
                candidate.read_grant.expires_at_unix_ms = now_unix_ms() + 100;
            }
            let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
                revision: 1,
                chunks: vec![ChunkSources {
                    allowed_ranges: Vec::new(),
                    chunk: None,
                    chunk_id: chunk.clone(),
                    sources: vec![candidate],
                }],
            }));
            let engine = DfsReadEngine::new(
                crate::dfs::NamespaceId::new("default"),
                "node-a".into(),
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources.clone(),
                Arc::new(OwnedTransferProbe {
                    destination: std::sync::atomic::AtomicUsize::new(0),
                    fail_first: AtomicBool::new(false),
                    fail_all: !expire_before_return,
                    expire_before_return,
                }),
                DfsReadConfig::default(),
            );
            let batch = ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(FileVersionId::new("version")),
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![ChunkReadOp {
                    chunk_id: chunk,
                    chunk_offset: 0,
                    length: 6,
                    output_offset: 0,
                }],
            };
            assert!(
                engine
                    .read_batch_native_owned(&batch, MAX_DFS_READ_BYTES as usize + 1)
                    .is_err()
            );
            assert_eq!(
                *sources.calls.lock().unwrap(),
                0,
                "budget rejection must precede source lookup"
            );
            assert!(engine.read_batch_native_owned(&batch, 6).is_err());
            assert!(engine.capture_native_read_authority().is_ok());
            if expire_before_return {
                assert!(
                    engine.source_cache.lock().unwrap().is_empty(),
                    "expired grant context must not be reused"
                );
            }
        }
    }

    #[test]
    fn native_owned_complex_batch_uses_existing_borrowed_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            Arc::new(UnimplementedReadSourceProvider),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id: ChunkId::new("missing"),
                chunk_offset: 0,
                length: 1,
                output_offset: 1,
            }],
        };
        assert!(engine.read_batch_native_owned(&batch, 2).unwrap().is_none());
    }
    #[derive(Default)]
    struct RecordingTransfer {
        batches: Mutex<Vec<PeerReadBatch>>,
    }

    impl ChunkTransfer for RecordingTransfer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            batch.validate(out.len())?;
            self.batches.lock().unwrap().push(batch.clone());
            for item in &batch.operations {
                let start = item.op.output_offset;
                out[start..start + item.op.length as usize]
                    .fill(item.source.copy_id.0.as_bytes()[0]);
            }
            Ok(())
        }
    }

    #[test]
    fn peer_batch_keeps_each_copy_and_grant_while_coalescing_one_peer() {
        let temp = tempfile::tempdir().unwrap();
        let chunks = [ChunkId::new("one"), ChunkId::new("two")];
        let mut second = source("b-copy", &chunks[1], "node-b");
        second.read_grant.token = "another-copy-grant".into();
        second.read_grant.fence = 2;
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![
                ChunkSources {
                    allowed_ranges: Vec::new(),
                    chunk: None,
                    chunk_id: chunks[0].clone(),
                    sources: vec![source("a-copy", &chunks[0], "node-b")],
                },
                ChunkSources {
                    allowed_ranges: Vec::new(),
                    chunk: None,
                    chunk_id: chunks[1].clone(),
                    sources: vec![second],
                },
            ],
        }));
        let transfer = Arc::new(RecordingTransfer::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources,
            transfer.clone(),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: chunks
                .into_iter()
                .enumerate()
                .map(|(index, chunk_id)| ChunkReadOp {
                    chunk_id,
                    chunk_offset: 0,
                    length: 2,
                    output_offset: index * 2,
                })
                .collect(),
        };
        let mut out = [0; 4];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"aabb");
        let calls = transfer.batches.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operations.len(), 2);
        assert_ne!(
            calls[0].operations[0].source.read_grant.token,
            calls[0].operations[1].source.read_grant.token
        );
    }

    #[test]
    fn same_chunk_in_another_version_refreshes_grant_without_rewriting_identity() {
        let temp = tempfile::tempdir().unwrap();
        let chunk = ChunkId::new("shared");
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id: chunk.clone(),
                sources: vec![source("a-copy", &chunk, "node-b")],
            }],
        }));
        let transfer = Arc::new(RecordingTransfer::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
            transfer.clone(),
            DfsReadConfig::default(),
        );
        let mut batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id: chunk,
                chunk_offset: 0,
                length: 1,
                output_offset: 0,
            }],
        };
        engine.read_batch(&batch, &mut [0]).unwrap();
        batch.file_version_id = Some(FileVersionId::new("next-version"));
        // A source lookup that returns the old grant must fail rather than rewrite the batch.
        assert!(engine.read_batch(&batch, &mut [0]).is_err());
        sources.reply.lock().unwrap().chunks[0].sources[0]
            .read_grant
            .file_version_id = FileVersionId::new("next-version");
        engine.read_batch(&batch, &mut [0]).unwrap();
        assert_eq!(*sources.calls.lock().unwrap(), 3);
        let calls = transfer.batches.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[1].file_version_id, FileVersionId::new("next-version"));
    }

    struct PartlyStalePeer {
        all_fail: bool,
        fallback_fails: bool,
        attempts: Mutex<Vec<Vec<String>>>,
    }

    impl ChunkTransfer for PartlyStalePeer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            self.attempts.lock().unwrap().push(
                batch
                    .operations
                    .iter()
                    .map(|item| item.source.copy_id.0.clone())
                    .collect(),
            );
            // A real stream may have produced bytes before another range fails.
            out.fill(b'?');
            if self.all_fail
                || batch.operations.len() > 1
                || batch.operations[0].source.copy_id.0 == "b-stale"
                || (self.fallback_fails && batch.operations[0].source.copy_id.0 == "b-good")
            {
                return Err(unavailable("selected copy is stale"));
            }
            out.fill(batch.operations[0].source.copy_id.0.as_bytes()[0]);
            Ok(())
        }
    }

    #[test]
    fn failed_batch_retries_each_original_copy_before_advancing_candidates() {
        for (all_fail, fallback_fails) in [(false, false), (true, false), (false, true)] {
            let temp = tempfile::tempdir().unwrap();
            let a = ChunkId::new("a");
            let b = ChunkId::new("b");
            let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
                revision: 1,
                chunks: vec![
                    ChunkSources {
                        allowed_ranges: Vec::new(),
                        chunk: None,
                        chunk_id: a.clone(),
                        sources: vec![source("a-only", &a, "node-x")],
                    },
                    ChunkSources {
                        allowed_ranges: Vec::new(),
                        chunk: None,
                        chunk_id: b.clone(),
                        sources: vec![
                            source("b-stale", &b, "node-x"),
                            source("b-good", &b, "node-y"),
                        ],
                    },
                ],
            }));
            let transfer = Arc::new(PartlyStalePeer {
                all_fail,
                fallback_fails,
                attempts: Mutex::new(Vec::new()),
            });
            let engine = DfsReadEngine::new(
                crate::dfs::NamespaceId::new("default"),
                "node-a".into(),
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources,
                transfer.clone(),
                DfsReadConfig::default(),
            );
            let batch = ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(FileVersionId::new("version")),
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![
                    ChunkReadOp {
                        chunk_id: a,
                        chunk_offset: 0,
                        length: 1,
                        output_offset: 0,
                    },
                    ChunkReadOp {
                        chunk_id: b,
                        chunk_offset: 0,
                        length: 1,
                        output_offset: 1,
                    },
                ],
            };
            let mut out = [b'!'; 2];
            let result = engine.read_batch(&batch, &mut out);
            let attempts = transfer.attempts.lock().unwrap();
            assert_eq!(attempts[0], vec!["a-only", "b-stale"]);
            assert_eq!(attempts[1], vec!["a-only"]);
            assert_eq!(attempts[2], vec!["b-stale"]);
            if all_fail {
                assert!(result.is_err());
                assert_eq!(attempts.len(), 3);
                assert_eq!(&out, b"!!");
            } else {
                assert_eq!(attempts.len(), 4);
                assert_eq!(attempts[3], vec!["b-good"]);
                if fallback_fails {
                    assert!(result.is_err());
                    assert_eq!(&out, b"!!", "A succeeded but B exhausted its candidates");
                } else {
                    result.unwrap();
                    assert_eq!(&out, b"ab");
                }
            }
        }
    }

    #[test]
    fn read_configuration_is_explicitly_bounded_by_the_wire_contract() {
        let maximum = DfsReadConfig::default();
        maximum.validate().unwrap();
        DfsReadConfig {
            max_ops_per_batch: 1,
            max_inflight_bytes: 1,
            ..maximum.clone()
        }
        .validate()
        .unwrap();
        for config in [
            DfsReadConfig {
                source_lookahead_chunks: MAX_DFS_READ_OPS + 1,
                ..maximum.clone()
            },
            DfsReadConfig {
                source_cache_bytes: MAX_SOURCE_CACHE_BYTES + 1,
                ..maximum.clone()
            },
            DfsReadConfig {
                source_cache_ttl: Duration::from_millis(60_001),
                ..maximum.clone()
            },
            DfsReadConfig {
                max_ops_per_batch: 0,
                ..maximum.clone()
            },
            DfsReadConfig {
                max_ops_per_batch: MAX_DFS_READ_OPS + 1,
                ..maximum.clone()
            },
            DfsReadConfig {
                max_inflight_bytes: 0,
                ..maximum.clone()
            },
            DfsReadConfig {
                max_inflight_bytes: MAX_DFS_READ_BYTES + 1,
                ..maximum.clone()
            },
        ] {
            assert!(config.validate().is_err());
            let empty = ReadBatch {
                lookahead: Vec::new(),
                file_version_id: None,
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![],
            };
            assert!(empty.validate_for_output(0, &config).is_err());
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum ExternalAuthorityFault {
        Chunk,
        Location,
        Validation,
        Range,
        GrantVersion,
        MissingReply,
        DuplicateReply,
    }

    struct ExternalFixture {
        chunk: crate::dfs::ChunkObject,
        candidate: SourceCandidate,
        bytes: Mutex<Vec<u8>>,
        authorizations: Mutex<u64>,
        fetches: Mutex<u64>,
        denial: Mutex<bool>,
        expires_at: Mutex<u64>,
        expires_after: Option<Duration>,
        authority_fault: Option<ExternalAuthorityFault>,
        delay: Duration,
    }

    impl ExternalFixture {
        fn new(bytes: &[u8]) -> Self {
            let staged = crate::node::chunk::StagedChunk::new(
                OperationId::new("external-fixture"),
                bytes.to_vec(),
            );
            let mut candidate = source("external-copy", &staged.chunk.id, "node-b");
            candidate.role = CopyRole::ExternalCommitted;
            candidate.data_endpoint = None;
            let object_revision = "immutable-v1".to_owned();
            candidate.location = CopyLocation::External {
                store_id: "store".into(),
                object_key: crate::dfs::external_object_key(
                    "store",
                    &object_revision,
                    &staged.chunk.id,
                )
                .unwrap(),
                object_revision,
            };
            Self {
                chunk: staged.chunk,
                candidate,
                bytes: Mutex::new(bytes.to_vec()),
                authorizations: Mutex::new(0),
                fetches: Mutex::new(0),
                denial: Mutex::new(false),
                expires_at: Mutex::new(u64::MAX),
                expires_after: None,
                authority_fault: None,
                delay: Duration::ZERO,
            }
        }

        fn sources(&self) -> Arc<StaticSources> {
            Arc::new(StaticSources::new(DfsChunkSourcesReply {
                revision: 1,
                chunks: vec![ChunkSources {
                    allowed_ranges: Vec::new(),
                    chunk: Some(self.chunk.clone()),
                    chunk_id: self.chunk.id.clone(),
                    sources: vec![self.candidate.clone()],
                }],
            }))
        }

        fn empty_sources(&self) -> Arc<StaticSources> {
            Arc::new(StaticSources::default())
        }

        fn current_authorizations(
            &self,
            request: crate::dfs::AuthorizeCurrentExternalRead,
        ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
            let mut validations = request.validations;
            validations.extend(request.missing_sources.into_iter().map(|missing| {
                crate::dfs::DfsReadValidation {
                    grant: self.candidate.read_grant.clone(),
                    chunk_id: missing.chunk_id,
                    copy_id: self.candidate.copy_id.clone(),
                    chunk_offset: missing.chunk_offset,
                    length: missing.length,
                }
            }));
            self.validate_external_read_grants(&request.caller_id, validations)
        }

        fn batch(&self) -> ReadBatch {
            ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(FileVersionId::new("version")),
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![ChunkReadOp {
                    chunk_id: self.chunk.id.clone(),
                    chunk_offset: 1,
                    length: 2,
                    output_offset: 0,
                }],
            }
        }

        fn engine(
            self: &Arc<Self>,
            local: Arc<LocalChunkStore>,
            sources: Arc<StaticSources>,
        ) -> DfsReadEngine {
            DfsReadEngine::new(
                crate::dfs::NamespaceId::new("default"),
                "node-a".into(),
                local,
                sources,
                Arc::new(UnimplementedChunkTransfer),
                DfsReadConfig {
                    source_cache_ttl: Duration::from_secs(60),
                    ..DfsReadConfig::default()
                },
            )
            .with_external_backend(self.clone(), self.clone())
        }
    }

    impl ExternalReadAuthorizer for ExternalFixture {
        fn authorize_external_version_read(
            &self,
            request: AuthorizeExternalVersionRead,
        ) -> Result<Option<AuthorizeExternalVersionReadReply>> {
            let validations = request
                .requests
                .into_iter()
                .map(|op| {
                    let mut grant = self.candidate.read_grant.clone();
                    grant.namespace_id = request.namespace_id.clone();
                    grant.file_version_id = request.file_version_id.clone();
                    grant.layout_root_id = request.layout_root_id.clone();
                    DfsReadValidation {
                        grant,
                        chunk_id: op.chunk_id,
                        copy_id: self.candidate.copy_id.clone(),
                        chunk_offset: op.chunk_offset,
                        length: op.length,
                    }
                })
                .collect();
            let entries = self.validate_external_read_grants(&request.caller_id, validations)?;
            let remaining = self
                .expires_after
                .map_or(5_000, |value| value.as_millis() as u64);
            Ok(Some(AuthorizeExternalVersionReadReply {
                authorizations: entries,
                remaining_validity_ms: remaining,
            }))
        }

        fn validate_external_read_grants(
            &self,
            caller_id: &str,
            validations: Vec<crate::dfs::DfsReadValidation>,
        ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
            assert_eq!(caller_id, "node-a");
            *self.authorizations.lock().unwrap() += 1;
            if *self.denial.lock().unwrap() {
                return Err(unavailable("external authorization denied"));
            }
            if matches!(
                self.authority_fault,
                Some(ExternalAuthorityFault::MissingReply)
            ) {
                return Ok(Vec::new());
            }
            let expires_at = self.expires_after.map_or_else(
                || *self.expires_at.lock().unwrap(),
                |duration| now_unix_ms() + duration.as_millis() as u64,
            );
            let mut replies = validations
                .into_iter()
                .map(|validation| {
                    let mut entry = crate::dfs::DfsExternalAuthorizedRead {
                        authorized: crate::dfs::DfsAuthorizedRead {
                            validation,
                            allowed_ranges: vec![(0, self.chunk.length)],
                            expires_at_unix_ms: expires_at,
                        },
                        chunk: self.chunk.clone(),
                        location: self.candidate.location.clone(),
                    };
                    match self.authority_fault {
                        Some(ExternalAuthorityFault::Chunk) => {
                            entry.chunk.content_digest.bytes[0] ^= 1
                        }
                        Some(ExternalAuthorityFault::Location) => {
                            entry.location = CopyLocation::External {
                                store_id: "another-store".into(),
                                object_key: "generation/chunk".into(),
                                object_revision: "immutable-v1".into(),
                            }
                        }
                        Some(ExternalAuthorityFault::Validation) => {
                            entry.authorized.validation.copy_id = CopyId::new("another-copy")
                        }
                        Some(ExternalAuthorityFault::Range) => {
                            entry.authorized.allowed_ranges = vec![(0, 1)]
                        }
                        Some(ExternalAuthorityFault::GrantVersion) => {
                            entry.authorized.validation.grant.file_version_id =
                                FileVersionId::new("next-version")
                        }
                        _ => {}
                    }
                    entry
                })
                .collect::<Vec<_>>();
            if matches!(
                self.authority_fault,
                Some(ExternalAuthorityFault::DuplicateReply)
            ) && let Some(first) = replies.first().cloned()
            {
                replies.push(first);
            }
            Ok(replies)
        }
    }

    impl ExternalChunkReader for ExternalFixture {
        fn read_chunk(
            &self,
            chunk: &crate::dfs::ChunkObject,
            source: &SourceCandidate,
        ) -> Result<VerifiedExternalChunk> {
            assert_eq!(chunk, &self.chunk);
            assert_eq!(source.location, self.candidate.location);
            *self.fetches.lock().unwrap() += 1;
            std::thread::sleep(self.delay);
            VerifiedExternalChunk::verify(chunk, self.bytes.lock().unwrap().clone())
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum CurrentReadMode {
        Authorized,
        AuthorizedWrongInode,
        Denied,
        HeadChanged,
        HeadChangedSameHead,
        DeadlineExceeded,
    }

    struct CurrentReadAuthorizer {
        fixture: Arc<ExternalFixture>,
        mode: CurrentReadMode,
        legacy_calls: Mutex<u64>,
        current_calls: Mutex<u64>,
        last_validations: Mutex<usize>,
        last_missing_sources: Mutex<usize>,
        legacy_fallback_allowed: bool,
    }

    impl CurrentReadAuthorizer {
        fn new(
            fixture: Arc<ExternalFixture>,
            mode: CurrentReadMode,
            legacy_fallback_allowed: bool,
        ) -> Self {
            Self {
                fixture,
                mode,
                legacy_calls: Mutex::new(0),
                current_calls: Mutex::new(0),
                last_validations: Mutex::new(0),
                last_missing_sources: Mutex::new(0),
                legacy_fallback_allowed,
            }
        }

        fn legacy_calls(&self) -> u64 {
            *self.legacy_calls.lock().unwrap()
        }

        fn current_calls(&self) -> u64 {
            *self.current_calls.lock().unwrap()
        }

        fn last_request_shape(&self) -> (usize, usize) {
            (
                *self.last_validations.lock().unwrap(),
                *self.last_missing_sources.lock().unwrap(),
            )
        }
    }

    impl ExternalReadAuthorizer for CurrentReadAuthorizer {
        fn authorize_current_external_read(
            &self,
            request: crate::dfs::AuthorizeCurrentExternalRead,
        ) -> Result<Option<crate::dfs::AuthorizeCurrentExternalReadReply>> {
            *self.current_calls.lock().unwrap() += 1;
            assert_eq!(request.caller_id, "node-a");
            assert_eq!(
                request.namespace_id,
                crate::dfs::NamespaceId::new("default")
            );
            assert_eq!(request.expected_head_version, FileVersionId::new("version"));
            assert_eq!(request.layout_root_id, LayoutRootId::new("layout"));
            *self.last_validations.lock().unwrap() = request.validations.len();
            *self.last_missing_sources.lock().unwrap() = request.missing_sources.len();
            assert!(request.validations.len() + request.missing_sources.len() > 0);
            for validation in &request.validations {
                assert_eq!(validation.chunk_id, self.fixture.chunk.id);
            }
            for missing in &request.missing_sources {
                assert_eq!(missing.chunk_id, self.fixture.chunk.id);
            }
            match self.mode {
                CurrentReadMode::Authorized => {
                    let authorizations = self.fixture.current_authorizations(request)?;
                    Ok(Some(
                        crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
                            inode: inode_with_head("version"),
                            authorizations,
                        },
                    ))
                }
                CurrentReadMode::AuthorizedWrongInode => {
                    let authorizations = self.fixture.current_authorizations(request)?;
                    let mut inode = inode_with_head("version");
                    inode.inode_id = crate::dfs::InodeId::new("inode:other");
                    Ok(Some(
                        crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
                            inode,
                            authorizations,
                        },
                    ))
                }
                CurrentReadMode::Denied => Err(external_denied(
                    "current external read authorization denied",
                )),
                CurrentReadMode::DeadlineExceeded => Err(Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "current external read authorization timed out",
                )),
                CurrentReadMode::HeadChanged => Ok(Some(
                    crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(inode_with_head(
                        "next-version",
                    )),
                )),
                CurrentReadMode::HeadChangedSameHead => Ok(Some(
                    crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(inode_with_head(
                        "version",
                    )),
                )),
            }
        }

        fn validate_external_read_grants(
            &self,
            caller_id: &str,
            validations: Vec<crate::dfs::DfsReadValidation>,
        ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
            *self.legacy_calls.lock().unwrap() += 1;
            if self.legacy_fallback_allowed {
                self.fixture
                    .validate_external_read_grants(caller_id, validations)
            } else {
                Err(unavailable(
                    "legacy external read authorization must not be used",
                ))
            }
        }
    }

    fn prime_external_source_cache(engine: &DfsReadEngine, fixture: &ExternalFixture) {
        prime_external_source_cache_with(
            engine,
            fixture,
            fixture.candidate.clone(),
            Duration::from_secs(60),
        );
    }

    fn prime_external_source_cache_with(
        engine: &DfsReadEngine,
        fixture: &ExternalFixture,
        source: SourceCandidate,
        ttl: Duration,
    ) {
        engine.source_cache.lock().unwrap().insert(
            (
                FileVersionId::new("version"),
                LayoutRootId::new("layout"),
                fixture.chunk.id.clone(),
            ),
            CachedSources {
                expires_at: Instant::now() + ttl,
                sources: ChunkSources {
                    allowed_ranges: Vec::new(),
                    chunk_id: fixture.chunk.id.clone(),
                    chunk: Some(fixture.chunk.clone()),
                    sources: vec![source],
                },
            },
        );
    }

    struct CurrentExternalChunks {
        fixtures: Vec<Arc<ExternalFixture>>,
        current_calls: Mutex<u64>,
        last_validations: Mutex<usize>,
        last_missing_sources: Mutex<usize>,
    }

    impl CurrentExternalChunks {
        fn new(fixtures: Vec<Arc<ExternalFixture>>) -> Self {
            Self {
                fixtures,
                current_calls: Mutex::new(0),
                last_validations: Mutex::new(0),
                last_missing_sources: Mutex::new(0),
            }
        }

        fn current_calls(&self) -> u64 {
            *self.current_calls.lock().unwrap()
        }

        fn last_request_shape(&self) -> (usize, usize) {
            (
                *self.last_validations.lock().unwrap(),
                *self.last_missing_sources.lock().unwrap(),
            )
        }
    }

    impl ExternalReadAuthorizer for CurrentExternalChunks {
        fn authorize_current_external_read(
            &self,
            request: crate::dfs::AuthorizeCurrentExternalRead,
        ) -> Result<Option<crate::dfs::AuthorizeCurrentExternalReadReply>> {
            *self.current_calls.lock().unwrap() += 1;
            *self.last_validations.lock().unwrap() = request.validations.len();
            *self.last_missing_sources.lock().unwrap() = request.missing_sources.len();
            let mut replies = Vec::new();
            let mut validations = request.validations;
            validations.extend(request.missing_sources.into_iter().map(|missing| {
                let fixture = self
                    .fixtures
                    .iter()
                    .find(|fixture| fixture.chunk.id == missing.chunk_id)
                    .unwrap();
                crate::dfs::DfsReadValidation {
                    grant: fixture.candidate.read_grant.clone(),
                    chunk_id: missing.chunk_id,
                    copy_id: fixture.candidate.copy_id.clone(),
                    chunk_offset: missing.chunk_offset,
                    length: missing.length,
                }
            }));
            for validation in validations {
                let fixture = self
                    .fixtures
                    .iter()
                    .find(|fixture| fixture.chunk.id == validation.chunk_id)
                    .unwrap();
                replies.extend(
                    fixture.validate_external_read_grants(&request.caller_id, vec![validation])?,
                );
            }
            Ok(Some(
                crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
                    inode: inode_with_head("version"),
                    authorizations: replies,
                },
            ))
        }

        fn validate_external_read_grants(
            &self,
            _caller_id: &str,
            _validations: Vec<crate::dfs::DfsReadValidation>,
        ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
            Err(unavailable(
                "legacy external read authorization must not be used",
            ))
        }
    }

    impl ExternalChunkReader for CurrentExternalChunks {
        fn read_chunk(
            &self,
            chunk: &ChunkObject,
            source: &SourceCandidate,
        ) -> Result<VerifiedExternalChunk> {
            self.fixtures
                .iter()
                .find(|fixture| fixture.chunk.id == chunk.id)
                .unwrap()
                .read_chunk(chunk, source)
        }
    }

    fn inode_with_head(version_id: &str) -> crate::dfs::InodeRecord {
        crate::dfs::InodeRecord {
            namespace_id: crate::dfs::NamespaceId::new("default"),
            inode_id: crate::dfs::InodeId::new("inode:test"),
            kind: crate::dfs::InodeKind::Regular,
            attributes: crate::dfs::InodeAttributes {
                mode: 0o640,
                uid: 1000,
                gid: 1000,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            },
            head_version: Some(FileVersionId::new(version_id)),
            symlink_target: None,
            xattrs: Default::default(),
            revision: 1,
        }
    }

    #[test]
    fn external_backend_uses_current_head_authorization_when_available() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::Authorized,
            false,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());
        prime_external_source_cache(&engine, &fixture);

        let mut out = [b'!'; 2];
        let outcome = engine
            .read_batch_current_external(
                &crate::dfs::InodeId::new("inode:test"),
                &fixture.batch(),
                &mut out,
            )
            .unwrap();

        assert_eq!(outcome, CurrentExternalReadOutcome::Read);
        assert_eq!(&out, b"bc");
        assert_eq!(authorizer.current_calls(), 1);
        assert_eq!(authorizer.legacy_calls(), 0);
        assert_eq!(authorizer.last_request_shape(), (1, 0));
        assert_eq!(*fixture.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_current_read_resolves_missing_source_without_source_lookup() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let sources = fixture.empty_sources();
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::Authorized,
            false,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources.clone(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        let outcome = engine
            .read_batch_current_external(
                &crate::dfs::InodeId::new("inode:test"),
                &fixture.batch(),
                &mut out,
            )
            .unwrap();

        assert_eq!(outcome, CurrentExternalReadOutcome::Read);
        assert_eq!(&out, b"bc");
        assert_eq!(authorizer.last_request_shape(), (0, 1));
        assert_eq!(*sources.calls.lock().unwrap(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_current_read_resolves_each_missing_range_on_same_chunk() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let sources = fixture.empty_sources();
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::Authorized,
            false,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources.clone(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: fixture.chunk.id.clone(),
                    chunk_offset: 1,
                    length: 2,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id: fixture.chunk.id.clone(),
                    chunk_offset: 3,
                    length: 2,
                    output_offset: 2,
                },
            ],
        };

        let mut out = [b'!'; 4];
        let outcome = engine
            .read_batch_current_external(&crate::dfs::InodeId::new("inode:test"), &batch, &mut out)
            .unwrap();

        assert_eq!(outcome, CurrentExternalReadOutcome::Read);
        assert_eq!(&out, b"bcde");
        assert_eq!(authorizer.last_request_shape(), (0, 2));
        assert_eq!(*sources.calls.lock().unwrap(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_current_read_mixes_cached_and_missing_sources_in_original_output_order() {
        let temp = tempfile::tempdir().unwrap();
        let cached = Arc::new(ExternalFixture::new(b"abcdef"));
        let missing = Arc::new(ExternalFixture::new(b"uvwxyz"));
        let chunks = Arc::new(CurrentExternalChunks::new(vec![
            cached.clone(),
            missing.clone(),
        ]));
        let sources = Arc::new(StaticSources::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig {
                source_cache_ttl: Duration::from_secs(60),
                ..DfsReadConfig::default()
            },
        )
        .with_external_backend(chunks.clone(), chunks.clone());
        prime_external_source_cache(&engine, &cached);
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: missing.chunk.id.clone(),
                    chunk_offset: 1,
                    length: 2,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id: cached.chunk.id.clone(),
                    chunk_offset: 2,
                    length: 2,
                    output_offset: 2,
                },
                ChunkReadOp {
                    chunk_id: missing.chunk.id.clone(),
                    chunk_offset: 3,
                    length: 2,
                    output_offset: 4,
                },
            ],
        };

        let mut out = [b'!'; 6];
        let outcome = engine
            .read_batch_current_external(&crate::dfs::InodeId::new("inode:test"), &batch, &mut out)
            .unwrap();

        assert_eq!(outcome, CurrentExternalReadOutcome::Read);
        assert_eq!(&out, b"vwcdxy");
        assert_eq!(chunks.current_calls(), 1);
        assert_eq!(chunks.last_request_shape(), (1, 2));
        assert_eq!(*sources.calls.lock().unwrap(), 0);
        assert_eq!(*cached.fetches.lock().unwrap(), 1);
        assert_eq!(*missing.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_current_missing_authority_faults_do_not_fetch_or_fallback() {
        for fault in [
            ExternalAuthorityFault::Location,
            ExternalAuthorityFault::Range,
            ExternalAuthorityFault::GrantVersion,
            ExternalAuthorityFault::MissingReply,
            ExternalAuthorityFault::DuplicateReply,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let fixture = Arc::new(ExternalFixture {
                authority_fault: Some(fault),
                ..ExternalFixture::new(b"abcdef")
            });
            let sources = fixture.empty_sources();
            let authorizer = Arc::new(CurrentReadAuthorizer::new(
                fixture.clone(),
                CurrentReadMode::Authorized,
                true,
            ));
            let engine = fixture
                .engine(
                    Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                    sources.clone(),
                )
                .with_external_backend(fixture.clone(), authorizer.clone());

            let mut out = [b'!'; 2];
            assert!(
                engine
                    .read_batch_current_external(
                        &crate::dfs::InodeId::new("inode:test"),
                        &fixture.batch(),
                        &mut out,
                    )
                    .is_err(),
                "fault {fault:?}"
            );

            assert_eq!(&out, b"!!", "fault {fault:?}");
            assert_eq!(authorizer.current_calls(), 1, "fault {fault:?}");
            assert_eq!(authorizer.legacy_calls(), 0, "fault {fault:?}");
            assert_eq!(authorizer.last_request_shape(), (0, 1), "fault {fault:?}");
            assert_eq!(*sources.calls.lock().unwrap(), 0, "fault {fault:?}");
            assert_eq!(*fixture.fetches.lock().unwrap(), 0, "fault {fault:?}");
        }
    }

    #[test]
    fn external_backend_current_cached_invalid_grant_is_not_resolved_as_missing() {
        for case in 0..4 {
            let temp = tempfile::tempdir().unwrap();
            let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
            let sources = fixture.sources();
            let authorizer = Arc::new(CurrentReadAuthorizer::new(
                fixture.clone(),
                CurrentReadMode::Denied,
                true,
            ));
            let engine = fixture
                .engine(
                    Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                    sources.clone(),
                )
                .with_external_backend(fixture.clone(), authorizer.clone());
            let mut source = fixture.candidate.clone();
            match case {
                0 => source.read_grant.namespace_id = crate::dfs::NamespaceId::new("other"),
                1 => source.read_grant.caller_node_epoch = 0,
                2 => source.read_grant.expires_at_unix_ms = 0,
                _ => source.read_grant.token.clear(),
            }
            prime_external_source_cache_with(&engine, &fixture, source, Duration::from_secs(60));

            let mut out = [b'!'; 2];
            assert!(
                engine
                    .read_batch_current_external(
                        &crate::dfs::InodeId::new("inode:test"),
                        &fixture.batch(),
                        &mut out,
                    )
                    .is_err(),
                "case {case}"
            );

            assert_eq!(&out, b"!!", "case {case}");
            assert_eq!(authorizer.current_calls(), 1, "case {case}");
            assert_eq!(authorizer.legacy_calls(), 0, "case {case}");
            assert_eq!(authorizer.last_request_shape(), (1, 0), "case {case}");
            assert_eq!(*sources.calls.lock().unwrap(), 0, "case {case}");
            assert_eq!(*fixture.fetches.lock().unwrap(), 0, "case {case}");
        }
    }

    #[test]
    fn external_backend_current_auth_errors_do_not_fallback_or_fetch() {
        for mode in [CurrentReadMode::Denied, CurrentReadMode::DeadlineExceeded] {
            let temp = tempfile::tempdir().unwrap();
            let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
            let sources = fixture.empty_sources();
            let authorizer = Arc::new(CurrentReadAuthorizer::new(fixture.clone(), mode, true));
            let engine = fixture
                .engine(
                    Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                    sources.clone(),
                )
                .with_external_backend(fixture.clone(), authorizer.clone());

            let mut out = [b'!'; 2];
            assert!(
                engine
                    .read_batch_current_external(
                        &crate::dfs::InodeId::new("inode:test"),
                        &fixture.batch(),
                        &mut out,
                    )
                    .is_err(),
                "mode {mode:?}"
            );

            assert_eq!(&out, b"!!", "mode {mode:?}");
            assert_eq!(authorizer.current_calls(), 1, "mode {mode:?}");
            assert_eq!(authorizer.legacy_calls(), 0, "mode {mode:?}");
            assert_eq!(authorizer.last_request_shape(), (0, 1), "mode {mode:?}");
            assert_eq!(*sources.calls.lock().unwrap(), 0, "mode {mode:?}");
            assert_eq!(*fixture.fetches.lock().unwrap(), 0, "mode {mode:?}");
        }
    }

    #[test]
    fn external_backend_current_authorization_denial_is_not_legacy_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::Denied,
            true,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_current_external(
                    &crate::dfs::InodeId::new("inode:test"),
                    &fixture.batch(),
                    &mut out,
                )
                .is_err()
        );

        assert_eq!(&out, b"!!");
        assert_eq!(authorizer.current_calls(), 1);
        assert_eq!(authorizer.legacy_calls(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_current_head_changed_does_not_consume_speculative_plan() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::HeadChanged,
            true,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        let outcome = engine
            .read_batch_current_external(
                &crate::dfs::InodeId::new("inode:test"),
                &fixture.batch(),
                &mut out,
            )
            .unwrap();

        assert!(matches!(
            outcome,
            CurrentExternalReadOutcome::HeadChanged(inode)
                if inode.head_version == Some(FileVersionId::new("next-version"))
        ));
        assert_eq!(&out, b"!!");
        assert_eq!(authorizer.current_calls(), 1);
        assert_eq!(authorizer.legacy_calls(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_current_authorized_wrong_inode_is_rejected_without_output() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::AuthorizedWrongInode,
            true,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_current_external(
                    &crate::dfs::InodeId::new("inode:test"),
                    &fixture.batch(),
                    &mut out,
                )
                .is_err()
        );

        assert_eq!(&out, b"!!");
        assert_eq!(authorizer.current_calls(), 1);
        assert_eq!(authorizer.legacy_calls(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_current_fake_head_changed_is_rejected_without_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::HeadChangedSameHead,
            true,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_current_external(
                    &crate::dfs::InodeId::new("inode:test"),
                    &fixture.batch(),
                    &mut out,
                )
                .is_err()
        );

        assert_eq!(&out, b"!!");
        assert_eq!(authorizer.current_calls(), 1);
        assert_eq!(authorizer.legacy_calls(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_current_read_reports_unsupported_without_legacy_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );

        let mut out = [b'!'; 2];
        let outcome = engine
            .read_batch_current_external(
                &crate::dfs::InodeId::new("inode:test"),
                &fixture.batch(),
                &mut out,
            )
            .unwrap();

        assert_eq!(outcome, CurrentExternalReadOutcome::Unsupported);
        assert_eq!(&out, b"!!");
        assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_rejects_verified_content_for_another_chunk() {
        struct WrongChunk(VerifiedExternalChunk);
        impl ExternalChunkReader for WrongChunk {
            fn read_chunk(
                &self,
                _: &ChunkObject,
                _: &SourceCandidate,
            ) -> Result<VerifiedExternalChunk> {
                Ok(self.0.clone())
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let other = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("other"),
            b"ghijkl".to_vec(),
        );
        let verified = VerifiedExternalChunk::verify(&other.chunk, other.bytes().to_vec()).unwrap();
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(Arc::new(WrongChunk(verified)), fixture.clone());
        let mut out = [b'!'; 2];
        let error = engine.read_batch(&fixture.batch(), &mut out).unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        assert_eq!(&out, b"!!");
        assert_eq!(*fixture.authorizations.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_refuses_invalid_authority_before_fetch() {
        for failure in 0..4 {
            let temp = tempfile::tempdir().unwrap();
            let mut fixture = ExternalFixture::new(b"abcdef");
            match failure {
                0 => *fixture.expires_at.lock().unwrap() = 1,
                1 => fixture.chunk.length = MAX_DFS_EXTERNAL_CHUNK_BYTES + 1,
                2 => fixture.chunk.id = ChunkId::new("unexpected-chunk"),
                _ => {
                    fixture.candidate.location = CopyLocation::Node {
                        node_id: "node-b".into(),
                        node_epoch: 1,
                        device_id: "local-0".into(),
                        device_epoch: 1,
                        catalog_revision: 1,
                    }
                }
            }
            let fixture = Arc::new(fixture);
            let mut batch = fixture.batch();
            batch.ops[0].chunk_id = fixture.candidate.chunk_id.clone();
            let sources = fixture.sources();
            sources.reply.lock().unwrap().chunks[0].chunk_id = fixture.candidate.chunk_id.clone();
            let engine = fixture.engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources,
            );
            let mut out = [b'!'; 2];
            assert!(engine.read_batch(&batch, &mut out).is_err());
            assert_eq!(&out, b"!!");
            assert_eq!(*fixture.fetches.lock().unwrap(), 0);
        }
    }

    #[test]
    fn external_backend_rechecks_authority_after_fetch() {
        let temp = tempfile::tempdir().unwrap();
        let mut fixture = ExternalFixture::new(b"abcdef");
        fixture.delay = Duration::from_millis(150);
        fixture.expires_after = Some(Duration::from_millis(100));
        let fixture = Arc::new(fixture);
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let mut out = [b'!'; 2];
        assert!(engine.read_batch(&fixture.batch(), &mut out).is_err());
        assert_eq!(&out, b"!!");
        assert_eq!(*fixture.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_rejects_changed_authorization_identity_and_range() {
        for fault in [
            ExternalAuthorityFault::Chunk,
            ExternalAuthorityFault::Location,
            ExternalAuthorityFault::Validation,
            ExternalAuthorityFault::Range,
            ExternalAuthorityFault::MissingReply,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let mut fixture = ExternalFixture::new(b"abcdef");
            fixture.authority_fault = Some(fault);
            let fixture = Arc::new(fixture);
            let engine = fixture.engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            );
            let mut out = [b'!'; 2];
            assert!(engine.read_batch(&fixture.batch(), &mut out).is_err());
            assert_eq!(&out, b"!!");
            assert_eq!(*fixture.fetches.lock().unwrap(), 0);
        }
    }

    struct ExternalChunks(Vec<Arc<ExternalFixture>>);

    impl ExternalReadAuthorizer for ExternalChunks {
        fn validate_external_read_grants(
            &self,
            caller_id: &str,
            validations: Vec<DfsReadValidation>,
        ) -> Result<Vec<DfsExternalAuthorizedRead>> {
            let mut replies = Vec::new();
            for validation in validations {
                let fixture = self
                    .0
                    .iter()
                    .find(|fixture| fixture.chunk.id == validation.chunk_id)
                    .unwrap();
                replies.extend(fixture.validate_external_read_grants(caller_id, vec![validation])?);
            }
            Ok(replies)
        }
    }

    impl ExternalChunkReader for ExternalChunks {
        fn read_chunk(
            &self,
            chunk: &ChunkObject,
            source: &SourceCandidate,
        ) -> Result<VerifiedExternalChunk> {
            self.0
                .iter()
                .find(|fixture| fixture.chunk.id == chunk.id)
                .unwrap()
                .read_chunk(chunk, source)
        }
    }

    #[test]
    fn external_backend_late_chunk_failure_never_publishes_earlier_success() {
        let temp = tempfile::tempdir().unwrap();
        let first = Arc::new(ExternalFixture::new(b"abcdef"));
        let second = Arc::new(ExternalFixture::new(b"uvwxyz"));
        second.bytes.lock().unwrap()[5] ^= 1;
        let chunks = Arc::new(ExternalChunks(vec![first.clone(), second.clone()]));
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: [&first, &second]
                .iter()
                .map(|fixture| ChunkSources {
                    allowed_ranges: Vec::new(),
                    chunk_id: fixture.chunk.id.clone(),
                    chunk: Some(fixture.chunk.clone()),
                    sources: vec![fixture.candidate.clone()],
                })
                .collect(),
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources,
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        )
        .with_external_backend(chunks.clone(), chunks);
        let mut batch = first.batch();
        let mut last_op = second.batch().ops.remove(0);
        last_op.output_offset = 2;
        batch.ops.push(last_op);
        let mut out = [b'!'; 4];
        assert!(engine.read_batch(&batch, &mut out).is_err());
        assert_eq!(&out, b"!!!!");
        assert_eq!(*first.fetches.lock().unwrap(), 1);
        assert_eq!(*second.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_does_not_start_another_get_after_authority_expires() {
        let temp = tempfile::tempdir().unwrap();
        let mut first = ExternalFixture::new(b"abcdef");
        first.delay = Duration::from_millis(150);
        first.expires_after = Some(Duration::from_millis(100));
        let first = Arc::new(first);
        let second = Arc::new(ExternalFixture::new(b"uvwxyz"));
        let chunks = Arc::new(ExternalChunks(vec![first.clone(), second.clone()]));
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: [&first, &second]
                .iter()
                .map(|fixture| ChunkSources {
                    allowed_ranges: Vec::new(),
                    chunk_id: fixture.chunk.id.clone(),
                    chunk: Some(fixture.chunk.clone()),
                    sources: vec![fixture.candidate.clone()],
                })
                .collect(),
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources,
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        )
        .with_external_backend(chunks.clone(), chunks);
        let mut batch = first.batch();
        let mut last_op = second.batch().ops.remove(0);
        last_op.output_offset = 2;
        batch.ops.push(last_op);
        let mut out = [b'!'; 4];
        assert!(engine.read_batch(&batch, &mut out).is_err());
        assert_eq!(&out, b"!!!!");
        assert_eq!(*first.fetches.lock().unwrap(), 1);
        assert_eq!(*second.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_new_version_requires_its_original_grant() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let sources = fixture.sources();
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
        );
        let mut batch = fixture.batch();
        let mut out = [b'!'; 2];
        engine.read_batch(&batch, &mut out).unwrap();
        batch.file_version_id = Some(FileVersionId::new("next-version"));
        out.fill(b'!');
        assert!(engine.read_batch(&batch, &mut out).is_err());
        assert_eq!(&out, b"!!");
        sources.reply.lock().unwrap().chunks[0].sources[0]
            .read_grant
            .file_version_id = FileVersionId::new("next-version");
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"bc");
        assert_eq!(*sources.calls.lock().unwrap(), 3);
        assert_eq!(*fixture.authorizations.lock().unwrap(), 2);
        assert_eq!(
            *fixture.fetches.lock().unwrap(),
            2,
            "S1 has no content cache"
        );
    }

    #[test]
    fn external_version_owned_result_overlapping_groups_use_borrowed_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdefgh"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let mut batch = fixture.batch();
        let mut middle = batch.ops[0].clone();
        middle.chunk_id = ChunkId::new("other-object");
        batch.ops.push(middle);
        batch.ops.push(batch.ops[0].clone());
        assert!(
            engine
                .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 2,)
                .unwrap()
                .is_none(),
            "A/B/A overlap must retain original scatter ordering"
        );
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_version_owned_result_holes_still_require_live_authority() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authority = Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_read_authority(authority.clone());
        let mut batch = fixture.batch();
        batch.ops.clear();
        assert_eq!(
            engine
                .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 4,)
                .unwrap()
                .unwrap(),
            [0; 4]
        );
        authority.stop();
        assert!(
            engine
                .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 4,)
                .is_err()
        );
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
        assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
    }

    #[test]
    fn external_version_owned_result_preserves_sparse_offsets_and_tail() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdefgh"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let mut batch = fixture.batch();
        batch.ops[0].output_offset = 1;
        let mut tail = batch.ops[0].clone();
        tail.chunk_offset = 7;
        tail.length = 1;
        tail.output_offset = 5;
        batch.ops.push(tail);
        let result = engine
            .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 7)
            .unwrap()
            .unwrap();
        assert_eq!(result, b"\0bc\0\0h\0");
        let mut borrowed = [b'!'; 7];
        engine
            .read_batch_external_version(&crate::dfs::InodeId::new("inode"), &batch, &mut borrowed)
            .unwrap();
        assert_eq!(&borrowed, b"!bc!!h!");
    }

    #[test]
    fn external_version_owned_result_rejects_oversized_allocation_before_fetch() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        assert!(
            engine
                .read_batch_external_version_owned(
                    &crate::dfs::InodeId::new("inode"),
                    &fixture.batch(),
                    MAX_DFS_READ_BYTES as usize + 1,
                )
                .is_err()
        );
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
        assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
    }

    #[test]
    fn external_version_owned_result_final_checks_do_not_expose_bytes() {
        for corrupt_bytes in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut fixture = ExternalFixture::new(b"abcdef");
            if corrupt_bytes {
                fixture.bytes.lock().unwrap()[5] ^= 1; // Outside requested range.
            } else {
                fixture.expires_after = Some(Duration::from_millis(20));
                fixture.delay = Duration::from_millis(60);
            }
            let fixture = Arc::new(fixture);
            let engine = fixture.engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            );
            assert!(
                engine
                    .read_batch_external_version_owned(
                        &crate::dfs::InodeId::new("inode"),
                        &fixture.batch(),
                        2,
                    )
                    .is_err()
            );
            assert_eq!(*fixture.fetches.lock().unwrap(), 1);
        }
    }

    #[test]
    fn external_version_owned_result_stop_during_get_stays_private() {
        let temp = tempfile::tempdir().unwrap();
        let mut fixture = ExternalFixture::new(b"abcdef");
        fixture.delay = Duration::from_millis(100);
        let fixture = Arc::new(fixture);
        let authority = Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_read_authority(authority.clone());
        std::thread::scope(|scope| {
            scope.spawn(|| {
                while *fixture.fetches.lock().unwrap() == 0 {
                    std::thread::yield_now();
                }
                authority.stop();
            });
            assert!(
                engine
                    .read_batch_external_version_owned(
                        &crate::dfs::InodeId::new("inode"),
                        &fixture.batch(),
                        2,
                    )
                    .is_err()
            );
        });
    }

    #[test]
    fn external_version_reuses_ranges_and_separates_inode_version_layout() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdefgh"));
        let sources = fixture.sources();
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
        );
        let inode = crate::dfs::InodeId::new("inode");
        let mut batch = fixture.batch();
        let mut out = [0; 2];
        engine
            .read_batch_external_version(&inode, &batch, &mut out)
            .unwrap();
        assert_eq!(&out, b"bc");
        batch.ops[0].chunk_offset = 4;
        *fixture.denial.lock().unwrap() = true; // Meta disconnected after valid admission.
        engine
            .read_batch_external_version(&inode, &batch, &mut out)
            .unwrap();
        assert_eq!(&out, b"ef");
        assert_eq!(*fixture.authorizations.lock().unwrap(), 1);
        assert_eq!(
            *sources.calls.lock().unwrap(),
            0,
            "source TTL must not force an RPC"
        );
        for change in 0..3 {
            let mut other = batch.clone();
            let key = if change == 0 {
                crate::dfs::InodeId::new("other")
            } else {
                inode.clone()
            };
            if change == 1 {
                other.file_version_id = Some(FileVersionId::new("other"));
            }
            if change == 2 {
                other.layout_root_id = LayoutRootId::new("other");
            }
            out.fill(b'!');
            assert!(
                engine
                    .read_batch_external_version(&key, &other, &mut out)
                    .is_err()
            );
            assert_eq!(out, [b'!'; 2]);
        }
    }

    #[test]
    fn external_version_monotonic_expiry_never_renews_on_hit() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let inode = crate::dfs::InodeId::new("inode");
        let batch = fixture.batch();
        let mut out = [0; 2];
        engine
            .read_batch_external_version(&inode, &batch, &mut out)
            .unwrap();
        let original = engine
            .external_authority_cache
            .lock()
            .unwrap()
            .entries
            .values()
            .next()
            .unwrap()
            .deadline;
        engine
            .read_batch_external_version(&inode, &batch, &mut out)
            .unwrap();
        assert_eq!(
            engine
                .external_authority_cache
                .lock()
                .unwrap()
                .entries
                .values()
                .next()
                .unwrap()
                .deadline,
            original
        );
        {
            let mut cache = engine.external_authority_cache.lock().unwrap();
            let entry = Arc::get_mut(cache.entries.values_mut().next().unwrap()).unwrap();
            entry.deadline = Instant::now() - Duration::from_millis(1);
            entry.expires_at_unix_ms = u64::MAX; // A backwards wall clock cannot extend monotonic authority.
        }
        *fixture.denial.lock().unwrap() = true;
        out.fill(b'!');
        assert!(
            engine
                .read_batch_external_version(&inode, &batch, &mut out)
                .is_err()
        );
        assert_eq!(out, [b'!'; 2]);
        *fixture.denial.lock().unwrap() = false;
        engine
            .read_batch_external_version(&inode, &batch, &mut out)
            .unwrap();
        assert_eq!(*fixture.authorizations.lock().unwrap(), 3);
    }

    #[test]
    fn external_version_stop_during_get_prevents_output_and_cannot_resurrect() {
        let temp = tempfile::tempdir().unwrap();
        let mut fixture = ExternalFixture::new(b"abcdef");
        fixture.delay = Duration::from_millis(100);
        let fixture = Arc::new(fixture);
        let authority = Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_read_authority(authority.clone());
        std::thread::scope(|scope| {
            scope.spawn(|| {
                while *fixture.fetches.lock().unwrap() == 0 {
                    std::thread::yield_now();
                }
                authority.stop();
            });
            let mut out = [b'!'; 2];
            assert!(
                engine
                    .read_batch_external_version(
                        &crate::dfs::InodeId::new("inode"),
                        &fixture.batch(),
                        &mut out
                    )
                    .is_err()
            );
            assert_eq!(out, [b'!'; 2]);
        });
        assert!(
            authority
                .refresh(1, Instant::now() + Duration::from_secs(30))
                .is_err()
        );
    }

    #[test]
    fn external_version_expiring_fetch_and_bad_range_do_not_publish() {
        for fault in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut fixture = ExternalFixture::new(b"abcdef");
            if fault {
                fixture.authority_fault = Some(ExternalAuthorityFault::Range);
            } else {
                fixture.expires_after = Some(Duration::from_millis(20));
                fixture.delay = Duration::from_millis(60);
            }
            let fixture = Arc::new(fixture);
            let engine = fixture.engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            );
            let mut out = [b'!'; 2];
            assert!(
                engine
                    .read_batch_external_version(
                        &crate::dfs::InodeId::new("inode"),
                        &fixture.batch(),
                        &mut out
                    )
                    .is_err()
            );
            assert_eq!(out, [b'!'; 2]);
            assert_eq!(*fixture.fetches.lock().unwrap(), if fault { 0 } else { 1 });
        }
    }

    #[test]
    fn external_version_metadata_budget_includes_pinned_entries_and_reclaims_on_drop() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        engine
            .read_batch_external_version(
                &crate::dfs::InodeId::new("inode"),
                &fixture.batch(),
                &mut [0; 2],
            )
            .unwrap();
        let mut cache = engine.external_authority_cache.lock().unwrap();
        let pinned = cache.entries.values().next().unwrap().clone();
        let budget = cache.budget.clone();
        let charged = budget.lock().unwrap().bytes;
        assert!(charged > 0 && charged <= MAX_EXTERNAL_AUTH_CACHE_BYTES);
        cache.invalidate_inode_version(
            &crate::dfs::NamespaceId::new("default"),
            &crate::dfs::InodeId::new("inode"),
            &FileVersionId::new("version"),
        );
        assert!(pinned.invalidated.load(Ordering::Acquire));
        assert_eq!(
            budget.lock().unwrap().bytes,
            charged,
            "eviction cannot free a pinned allocation"
        );
        drop(pinned);
        assert_eq!(budget.lock().unwrap().bytes, 0);
        assert_eq!(budget.lock().unwrap().entries, 0);
    }

    #[test]
    fn external_version_cache_caps_entries_and_bytes_while_pinned() {
        for large_metadata in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut fixture = ExternalFixture::new(b"abcdef");
            // Exercise budget accounting independently of trusted transport decoding.
            if large_metadata {
                fixture.candidate.read_grant.token = "x".repeat(16_384);
            }
            let fixture = Arc::new(fixture);
            let engine = fixture.engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            );
            let mut pins = Vec::new();
            for i in 0..=MAX_EXTERNAL_AUTH_CACHE_ENTRIES {
                let inode = crate::dfs::InodeId::new(format!("inode-{i}"));
                engine
                    .read_batch_external_version(&inode, &fixture.batch(), &mut [0; 2])
                    .unwrap();
                let cache = engine.external_authority_cache.lock().unwrap();
                if let Some(entry) = cache
                    .entries
                    .values()
                    .find(|entry| entry.key.inode_id == inode)
                {
                    pins.push(entry.clone());
                }
                let budget = cache.budget.lock().unwrap();
                assert!(budget.entries <= MAX_EXTERNAL_AUTH_CACHE_ENTRIES);
                assert!(budget.bytes <= MAX_EXTERNAL_AUTH_CACHE_BYTES);
            }
            assert_eq!(
                *fixture.authorizations.lock().unwrap(),
                (MAX_EXTERNAL_AUTH_CACHE_ENTRIES + 1) as u64
            );
            let mut cache = engine.external_authority_cache.lock().unwrap();
            if large_metadata {
                assert!(cache.entries.len() < MAX_EXTERNAL_AUTH_CACHE_ENTRIES);
            } else {
                assert_eq!(cache.entries.len(), MAX_EXTERNAL_AUTH_CACHE_ENTRIES);
            }
            cache.entries.clear();
            assert!(cache.budget.lock().unwrap().bytes > 0);
            drop(pins);
            assert_eq!(cache.budget.lock().unwrap().bytes, 0);
        }
    }

    #[test]
    fn external_version_delayed_authorization_reply_cannot_reset_deadline() {
        struct Delayed(Arc<ExternalFixture>);
        impl ExternalReadAuthorizer for Delayed {
            fn authorize_external_version_read(
                &self,
                request: AuthorizeExternalVersionRead,
            ) -> Result<Option<AuthorizeExternalVersionReadReply>> {
                let mut reply = self.0.authorize_external_version_read(request)?.unwrap();
                reply.remaining_validity_ms = 20;
                std::thread::sleep(Duration::from_millis(50));
                Ok(Some(reply))
            }
            fn validate_external_read_grants(
                &self,
                caller: &str,
                validations: Vec<DfsReadValidation>,
            ) -> Result<Vec<DfsExternalAuthorizedRead>> {
                self.0.validate_external_read_grants(caller, validations)
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), Arc::new(Delayed(fixture.clone())));
        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_external_version(
                    &crate::dfs::InodeId::new("inode"),
                    &fixture.batch(),
                    &mut out
                )
                .is_err()
        );
        assert_eq!(out, [b'!'; 2]);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_preserves_holes_without_requesting_a_source() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let sources = fixture.sources();
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
        );
        let mut batch = fixture.batch();
        batch.ops.clear();
        let mut out = [0; 4];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(out, [0; 4]);
        assert_eq!(*sources.calls.lock().unwrap(), 0);
        assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }
}
