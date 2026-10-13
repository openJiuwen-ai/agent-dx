//! DFS fixed-version Chunk read path.
//!
//! DistributedFs resolves a file range into immutable Chunk ranges, then this
//! module chooses local or peer sources, or an explicitly configured external
//! backend. All paths publish into the caller buffer only after the whole
//! requested batch succeeds. External reads authorize every batch and verify
//! complete immutable chunks, without a local content cache.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use afs_error::{Error, Result};
use afs_tracing::tracing;

use crate::{
    dfs::{
        AuthorizeCurrentExternalRead, AuthorizeCurrentExternalReadReply, ChunkId, ChunkObject,
        ChunkSources, CopyLocation, CopyRole, CopyState, DfsChunkSourcesReply,
        DfsChunkSourcesRequest, DfsExternalAuthorizedRead, DfsMissingReadSource, DfsReadValidation,
        FileVersionId, LayoutRootId, SourceCandidate,
    },
    node::chunk::{LocalChunkStore, PinnedChunkReader, VerifiedRangeCopy},
};

/// Hard protocol budgets shared by the scheduler, peer adapter and service.
pub const MAX_DFS_READ_OPS: usize = 128;
pub const MAX_DFS_READ_BYTES: u64 = 8 * 1024 * 1024;
/// External objects are fetched and verified one at a time, independently of
/// the requested range budget. This matches the producer's staged chunk limit.
pub const MAX_DFS_EXTERNAL_CHUNK_BYTES: u64 = crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64;

#[derive(Clone, Debug)]
pub struct DfsReadConfig {
    pub max_ops_per_batch: usize,
    pub max_inflight_bytes: u64,
    pub source_cache_ttl: Duration,
}

impl Default for DfsReadConfig {
    fn default() -> Self {
        Self {
            max_ops_per_batch: MAX_DFS_READ_OPS,
            max_inflight_bytes: MAX_DFS_READ_BYTES,
            source_cache_ttl: Duration::from_millis(500),
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
        {
            return Err(invalid(format!(
                "DFS read configuration requires 1..={MAX_DFS_READ_OPS} operations and 1..={MAX_DFS_READ_BYTES} bytes"
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
}

impl ReadBatch {
    pub fn validate_for_output(&self, output_len: usize, config: &DfsReadConfig) -> Result<()> {
        config.validate()?;
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

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn matches(&self, chunk: &ChunkObject) -> bool {
        self.chunk == *chunk
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

pub trait ChunkTransfer: Send + Sync {
    /// `out` belongs to this attempt; the scheduler publishes it only after
    /// every operation completes. An adapter must drain or retain any DMA
    /// resources before returning, including on cancellation or timeout.
    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()>;
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

type SourceCacheKey = (FileVersionId, LayoutRootId, ChunkId);
const MAX_CACHED_SOURCES: usize = 1024;
const MAX_PINNED_READERS: usize = 256;

pub struct DfsReadEngine {
    namespace_id: crate::dfs::NamespaceId,
    local_node_id: String,
    local: Arc<LocalChunkStore>,
    source_provider: Arc<dyn ReadSourceProvider>,
    transfer: Arc<dyn ChunkTransfer>,
    config: DfsReadConfig,
    external_backend: Option<ExternalReadBackend>,
    readers: Mutex<HashMap<ChunkId, Arc<PinnedChunkReader>>>,
    source_cache: Mutex<HashMap<SourceCacheKey, CachedSources>>,
}

#[derive(Clone)]
struct CachedSources {
    expires_at: Instant,
    sources: ChunkSources,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CurrentExternalReadOutcome {
    Read,
    Unsupported,
    HeadChanged(crate::dfs::InodeRecord),
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

impl DfsReadEngine {
    pub fn new(
        namespace_id: crate::dfs::NamespaceId,
        local_node_id: String,
        local: Arc<LocalChunkStore>,
        source_provider: Arc<dyn ReadSourceProvider>,
        transfer: Arc<dyn ChunkTransfer>,
        config: DfsReadConfig,
    ) -> Self {
        Self {
            namespace_id,
            local_node_id,
            local,
            source_provider,
            transfer,
            config,
            external_backend: None,
            readers: Mutex::new(HashMap::new()),
            source_cache: Mutex::new(HashMap::new()),
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
            self.read_batch_into(&staged_batch, &mut staged)?;
            None
        };
        // Full-object fetches may outlive the authorization that allowed them.
        // Check again at the publication boundary, including cached sources.
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
        Ok(())
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
        self.read_external_authorized_selection(backend, &selection, authorized, out)
    }

    fn select_external_reads(&self, batch: &ReadBatch) -> Result<Option<ExternalReadSelection>> {
        let batch = ReadBatch {
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
    ) -> Result<Option<u64>> {
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
        self.cache_authorized_external_sources(selection, &reads)?;
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
            if expires_at <= now_unix_ms() {
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
        if cache.len().saturating_add(reads.len()) > MAX_CACHED_SOURCES {
            cache.clear();
        }
        let expires_at = Instant::now() + self.config.source_cache_ttl;
        for read in reads {
            cache.insert(
                (
                    version.clone(),
                    selection.batch.layout_root_id.clone(),
                    read.chunk.id.clone(),
                ),
                CachedSources {
                    expires_at,
                    sources: ChunkSources {
                        chunk_id: read.chunk.id.clone(),
                        chunk: Some(read.chunk.clone()),
                        sources: vec![read.source.clone()],
                    },
                },
            );
        }
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
            match reader.read_ranges_at_uncommitted(&ranges, out) {
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
            file_version_id: batch.file_version_id.clone(),
            layout_root_id: batch.layout_root_id.clone(),
            ops: remote_ops,
        };
        let sources = self.sources_for(&remote_batch)?;
        self.read_remote_batch(&remote_batch, &sources, out)?;
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
        let mut readers = self
            .readers
            .lock()
            .map_err(|_| unavailable("DFS local reader cache is poisoned"))?;
        if readers.len() >= MAX_PINNED_READERS {
            // Dropping the cached Arc does not invalidate an active read pin.
            readers.clear();
        }
        readers.insert(chunk_id.clone(), reader.clone());
        Ok(reader)
    }

    fn sources_for(&self, batch: &ReadBatch) -> Result<Vec<ChunkSources>> {
        let version = batch
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("DFS peer read requires a fixed FileVersion"))?;
        let now = Instant::now();
        let mut resolved = Vec::new();
        let mut misses = Vec::new();
        let mut seen = BTreeSet::new();
        let span = tracing::info_span!(
            "node.dfs.read.sources",
            ops = batch.ops.len(),
            cache_hits = tracing::field::Empty,
            cache_misses = tracing::field::Empty,
            fresh_chunks = tracing::field::Empty,
        );
        let _entered = span.enter();
        let mut cache_hits = 0_u64;
        {
            let mut cache = self
                .source_cache
                .lock()
                .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
            cache.retain(|_, entry| entry.expires_at > now);
            for op in &batch.ops {
                if !seen.insert(op.chunk_id.clone()) {
                    continue;
                }
                let key = (
                    version.clone(),
                    batch.layout_root_id.clone(),
                    op.chunk_id.clone(),
                );
                match cache.get(&key).filter(|entry| {
                    entry
                        .sources
                        .sources
                        .iter()
                        .all(|source| self.source_matches(batch, source))
                }) {
                    Some(entry) => {
                        cache_hits = cache_hits.saturating_add(1);
                        resolved.push(entry.sources.clone())
                    }
                    None => misses.push(op.clone()),
                }
            }
        }
        span.record("cache_hits", cache_hits);
        span.record("cache_misses", misses.len() as u64);
        if misses.is_empty() {
            span.record("fresh_chunks", 0_u64);
            return Ok(resolved);
        }
        let request = ReadBatch {
            file_version_id: Some(version.clone()),
            layout_root_id: batch.layout_root_id.clone(),
            ops: misses,
        }
        .source_request(self.local_node_id.clone(), self.namespace_id.clone())?;
        let expected: BTreeSet<_> = request.chunk_ids.iter().cloned().collect();
        let fresh = self.source_provider.sources_for(request)?.chunks;
        span.record("fresh_chunks", fresh.len() as u64);
        let mut received = BTreeSet::new();
        for set in &fresh {
            if !expected.contains(&set.chunk_id)
                || !received.insert(set.chunk_id.clone())
                || set.sources.iter().any(|source| {
                    source.chunk_id != set.chunk_id || !self.source_matches(batch, source)
                })
            {
                return Err(invalid(
                    "DFS source reply changed the requested content or authorization identity",
                ));
            }
        }
        if expected != received {
            return Err(unavailable("DFS source lookup omitted a requested Chunk"));
        }
        {
            let mut cache = self
                .source_cache
                .lock()
                .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
            if cache.len().saturating_add(fresh.len()) > MAX_CACHED_SOURCES {
                cache.clear();
            }
            for set in &fresh {
                cache.insert(
                    (
                        version.clone(),
                        batch.layout_root_id.clone(),
                        set.chunk_id.clone(),
                    ),
                    CachedSources {
                        expires_at: now + self.config.source_cache_ttl,
                        sources: set.clone(),
                    },
                );
            }
        }
        resolved.extend(fresh);
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

    fn read_remote_batch(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
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
                    same_peer_context(&candidates[first][next[first]], source)
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
                let mut scratch = vec![0; length];
                match self.transfer.read_ranges(&attempt, &mut scratch) {
                    Ok(()) => {
                        for (&index, item) in group.iter().zip(&attempt.operations) {
                            let original = &batch.ops[index];
                            let len = original.length as usize;
                            out[original.output_offset..original.output_offset + len]
                                .copy_from_slice(
                                    &scratch[item.op.output_offset..item.op.output_offset + len],
                                );
                        }
                    }
                    Err(error) => {
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
                                        let original = &batch.ops[index];
                                        out[original.output_offset
                                            ..original.output_offset + single_scratch.len()]
                                            .copy_from_slice(&single_scratch);
                                        continue;
                                    }
                                    Err(error) => last_error = Some(error),
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
        node::chunk::{ChunkBuilder, ChunkStore},
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
                    chunk: None,
                    chunk_id: chunks[0].clone(),
                    sources: vec![source("a-copy", &chunks[0], "node-b")],
                },
                ChunkSources {
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
                        chunk: None,
                        chunk_id: a.clone(),
                        sources: vec![source("a-only", &a, "node-x")],
                    },
                    ChunkSources {
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
