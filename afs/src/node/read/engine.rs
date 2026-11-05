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

mod async_prepare;

pub use super::object_fetch::PrefetchControl;

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
        read::peer_cache::{
            CompletedPeerDiskRead, DiskPeerCache, PeerStreamSink, VerifiedPeerChunk,
        },
        read_work::PeerFillClock,
    },
};

mod authority;
mod external;
mod fanout;
mod local;
mod peer_cache_source;
mod prefetch;
mod remote;
mod route;
mod sources;
pub(in crate::node) use authority::{LocalReadAuthority, NativeReadAuthoritySnapshot};
use external::*;
pub use external::{CurrentExternalReadOutcome, VerifiedExternalChunk};
#[cfg(test)]
use peer_cache_source::*;
use route::*;
pub(in crate::node) use sources::BoundedLru;
use sources::*;

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
pub(in crate::node) const MAX_SOURCE_LOOKAHEAD_ID_BYTES: usize = 512;

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

    /// Local/test providers may return immediately. Network adapters override
    /// this entry so async read preparation never hides a network block_on.
    fn sources_for_async(
        &self,
        request: DfsChunkSourcesRequest,
    ) -> super::admission::ReadFuture<'_, DfsChunkSourcesReply> {
        Box::pin(async move { crate::node::bridge::run_sync_backend(|| self.sources_for(request)) })
    }
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
    /// Synchronous callers are compatibility boundaries; all scheduling logic
    /// lives in the same async core used by FUSE. Real gRPC overrides this.
    fn block_on_read(&self, future: super::admission::ReadFuture<'_, ()>) -> Result<()> {
        crate::node::bridge::block_on_scoped_runtime(future)
    }

    fn read_ranges_async<'a>(
        &'a self,
        batch: &'a PeerReadBatch,
        out: &'a mut [u8],
    ) -> super::admission::ReadFuture<'a, ()> {
        Box::pin(
            async move { crate::node::bridge::run_sync_backend(|| self.read_ranges(batch, out)) },
        )
    }
    fn read_crc32c_range_async<'a>(
        &'a self,
        batch: &'a PeerReadBatch,
        chunk: &'a ChunkObject,
        out: &'a mut [u8],
    ) -> super::admission::ReadFuture<'a, ()> {
        Box::pin(async move {
            crate::node::bridge::run_sync_backend(|| self.read_crc32c_range(batch, chunk, out))
        })
    }

    fn supports_async_native_read(&self) -> bool {
        false
    }

    fn admit_native_read_async(
        &self,
    ) -> super::admission::ReadFuture<'_, super::admission::ReadLifetime> {
        Box::pin(async { Ok(Arc::new(()) as super::admission::ReadLifetime) })
    }
    fn configure_read_pipeline(&self, _limits: super::admission::ReadPipelineLimits) -> Result<()> {
        Ok(())
    }

    fn read_verified_object_async<'a>(
        &'a self,
        batch: &'a PeerReadBatch,
        chunk: &'a ChunkObject,
        out: &'a mut [u8],
        proof_budget: Arc<OutboardBudget>,
    ) -> super::admission::ReadFuture<'a, VerifiedPeerChunk<'a>> {
        Box::pin(async move {
            crate::node::bridge::run_sync_backend(|| {
                self.read_verified_object(batch, chunk, out, proof_budget)
            })
        })
    }
    fn read_verified_object_to_sink_async<'a, 'output: 'a>(
        &'a self,
        batch: &'a PeerReadBatch,
        chunk: &'a ChunkObject,
        requested_offset: u64,
        out: &'output mut [u8],
        sink: &'a mut PeerStreamSink<'_>,
        prefetch: Option<&'a PrefetchControl>,
    ) -> super::admission::ReadFuture<'a, Option<CompletedPeerDiskRead<'output>>> {
        Box::pin(async move {
            crate::node::bridge::run_sync_backend(|| {
                if let Some(control) = prefetch {
                    self.try_read_verified_object_to_sink(
                        batch,
                        chunk,
                        requested_offset,
                        out,
                        sink,
                        control,
                    )
                } else {
                    self.read_verified_object_to_sink(batch, chunk, requested_offset, out, sink)
                }
            })
        })
    }

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

pub(in crate::node) fn validate_verified_peer_object_request(
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

pub struct DfsReadEngine {
    namespace_id: crate::dfs::NamespaceId,
    reply_buffer_pool: Arc<super::buffer::ReadBufferPool>,
    read_pipeline_limits: super::admission::ReadPipelineLimits,
    reply_admission: Arc<tokio::sync::Semaphore>,
    native_prefetch_budget: Arc<std::sync::OnceLock<Arc<super::native_prefetch::Budget>>>,
    source_query_gates: Arc<super::admission::ReadKeyGates>,
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
    fetcher: Option<Arc<super::object_fetch::ObjectFetcher>>,
    external_authority_cache: Mutex<ExternalAuthorityCache>,
}

#[cfg(test)]
type PrefetchAttemptHook = Arc<dyn Fn(usize, &ChunkId) + Send + Sync>;

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

impl DfsReadEngine {
    pub(in crate::node) fn publication_diagnostic(
        &self,
    ) -> Option<super::super::read_work::ReadDiagnosticSpan> {
        self.local
            .read_diagnostic(super::super::read_work::ReadDiagnosticStage::MetaCommitFileVersion)
    }

    pub(in crate::node) fn publication_coarse_diagnostic(
        &self,
    ) -> Option<super::super::read_work::ReadCoarseDiagnosticSpan> {
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
        self.local.lifecycle().retire_generation(generation, chunks)
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
            reply_buffer_pool: super::buffer::ReadBufferPool::new("reply"),
            read_pipeline_limits: Default::default(),
            reply_admission: Arc::new(tokio::sync::Semaphore::new(8 * 1024 * 1024)),
            native_prefetch_budget: Arc::new(std::sync::OnceLock::new()),
            source_query_gates: Arc::new(Default::default()),
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
    pub(in crate::node) fn with_peer_cache(mut self, cache: Arc<DiskPeerCache>) -> Self {
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            self.fetcher = match super::object_fetch::ObjectFetcher::new(
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

    pub(in crate::node) fn object_fetcher(
        &self,
    ) -> Option<Arc<super::object_fetch::ObjectFetcher>> {
        self.fetcher.clone()
    }

    #[cfg(test)]
    pub(in crate::node) fn set_prefetch_attempt_hook_for_test(&self, hook: PrefetchAttemptHook) {
        *self.prefetch_attempt_hook.lock().unwrap() = Some(hook);
    }

    fn task_reader(&self) -> Self {
        Self {
            reply_buffer_pool: self.reply_buffer_pool.clone(),
            read_pipeline_limits: self.read_pipeline_limits,
            reply_admission: self.reply_admission.clone(),
            native_prefetch_budget: self.native_prefetch_budget.clone(),
            source_query_gates: self.source_query_gates.clone(),
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

    pub(in crate::node) fn with_read_authority(
        mut self,
        authority: Arc<LocalReadAuthority>,
    ) -> Self {
        self.read_authority = authority;
        self
    }

    pub(in crate::node) fn validate_native_read_size(&self, size: usize) -> Result<()> {
        self.config.validate()?;
        if size as u64 > self.config.max_inflight_bytes {
            return Err(invalid("DFS private read exceeds inflight byte budget"));
        }
        Ok(())
    }

    /// Fill a caller-owned, unpublished native result without an additional
    /// aggregate staging allocation. On error this private buffer may change
    /// and must be discarded. Borrowed public output must use `read_batch`.
    pub(in crate::node) fn read_batch_native_into_private(
        &self,
        batch: &ReadBatch,
        out: &mut [u8],
    ) -> Result<()> {
        self.read_batch_native_into_private_before(batch, out, None)
    }

    pub(in crate::node) fn read_batch_native_into_private_before(
        &self,
        batch: &ReadBatch,
        out: &mut [u8],
        deadline: Option<Instant>,
    ) -> Result<()> {
        self.validate_native_read_size(out.len())?;
        batch.validate_for_output(out.len(), &self.config)?;
        let ChunkSource::Native(native) = self.chunk_source() else {
            return Err(Error::coded(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                "private native read cannot use an external backend",
            ));
        };
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
        native
            .read(batch, out, true, deadline)
            .inspect_err(|error| {
                self.invalidate_native_batch_on_error(batch, error);
            })?;
        self.check_native_read_authority(&authority)
    }

    #[cfg(test)]
    pub(in crate::node) fn read_batch_native_open_into_private_before(
        &self,
        batch: &ReadBatch,
        out: &mut [u8],
        deadline: Option<Instant>,
    ) -> Result<()> {
        self.validate_native_read_size(out.len())?;
        batch.validate_for_output(out.len(), &self.config)?;
        let ChunkSource::Native(native) = self.chunk_source() else {
            return Err(Error::coded(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                "private native read cannot use an external backend",
            ));
        };
        self.validate_private_native_ranges(batch)?;
        let authority = self.capture_native_read_authority()?;
        native
            .read(batch, out, true, deadline)
            .inspect_err(|error| {
                self.invalidate_native_batch_on_error(batch, error);
            })?;
        self.check_native_read_authority(&authority)
    }
    fn validate_private_native_ranges(&self, batch: &ReadBatch) -> Result<()> {
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
        Ok(())
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
        let route = self.chunk_source();
        let native_authority = if matches!(route, ChunkSource::Native(_)) {
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
        let expires_at = match route {
            ChunkSource::External(source) => source.read(&staged_batch, &mut staged)?,
            ChunkSource::Native(source) => {
                source
                    .read(&staged_batch, &mut staged, false, None)
                    .inspect_err(|error| {
                        self.invalidate_native_batch_on_error(batch, error);
                    })?;
                None
            }
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

    pub(in crate::node) fn read_batch_native_owned_before(
        &self,
        batch: &ReadBatch,
        output_len: usize,
        deadline: Option<Instant>,
    ) -> Result<Option<Vec<u8>>> {
        self.read_batch_native_open_owned_before(batch, output_len, deadline)
            .map(|result| result.map(super::buffer::ReadBuffer::into_vec))
    }
    pub(in crate::node) fn read_batch_native_open_owned_before(
        &self,
        batch: &ReadBatch,
        output_len: usize,
        deadline: Option<Instant>,
    ) -> Result<Option<super::buffer::ReadBuffer>> {
        if output_len as u64 > self.config.max_inflight_bytes {
            return Err(invalid("DFS owned read exceeds inflight byte budget"));
        }
        batch.validate_for_output(output_len, &self.config)?;
        let ChunkSource::Native(native) = self.chunk_source() else {
            return Ok(None);
        };
        if batch.ops.len() != 1
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
        let mut result = self
            .reply_buffer_pool
            .take(output_len, self.local.read_work_metrics().cloned())?;
        native
            .read(batch, &mut result, true, deadline)
            .inspect_err(|error| {
                self.invalidate_native_batch_on_error(batch, error);
            })?;
        self.check_native_read_authority(&authority)?;
        Ok(Some(result))
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
mod tests;
