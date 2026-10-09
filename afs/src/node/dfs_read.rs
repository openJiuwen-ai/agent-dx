//! DFS fixed-version Chunk read path.
//!
//! DistributedFs resolves a file range into immutable Chunk ranges, then this
//! module chooses local or peer sources. Local reads use verified pinned
//! readers. Peer reads land in a scratch buffer first and publish into the
//! caller buffer only after the whole requested range succeeds.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use afs_error::{Error, Result};

use crate::{
    dfs::{
        ChunkId, ChunkSources, CopyLocation, CopyRole, DfsChunkSourcesReply,
        DfsChunkSourcesRequest, FileVersionId, LayoutRootId, SourceCandidate,
    },
    node::chunk::{LocalChunkStore, PinnedChunkReader, VerifiedRangeCopy},
};

/// Hard protocol budgets shared by the scheduler, peer adapter and service.
pub const MAX_DFS_READ_OPS: usize = 128;
pub const MAX_DFS_READ_BYTES: u64 = 8 * 1024 * 1024;

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
    readers: Mutex<HashMap<ChunkId, Arc<PinnedChunkReader>>>,
    source_cache: Mutex<HashMap<SourceCacheKey, CachedSources>>,
}

#[derive(Clone)]
struct CachedSources {
    expires_at: Instant,
    sources: ChunkSources,
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
            readers: Mutex::new(HashMap::new()),
            source_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn read_batch(&self, batch: &ReadBatch, out: &mut [u8]) -> Result<()> {
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
        self.read_batch_into(&staged_batch, &mut staged)?;
        for (original, packed) in batch.ops.iter().zip(&staged_batch.ops) {
            if original.length == 0 {
                continue;
            }
            let length = original.length as usize;
            out[original.output_offset..original.output_offset + length]
                .copy_from_slice(&staged[packed.output_offset..packed.output_offset + length]);
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
                    Some(entry) => resolved.push(entry.sources.clone()),
                    None => misses.push(op.clone()),
                }
            }
        }
        if misses.is_empty() {
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
                    chunk_id: chunks[0].clone(),
                    sources: vec![source("a-copy", &chunks[0], "node-b")],
                },
                ChunkSources {
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
                        chunk_id: a.clone(),
                        sources: vec![source("a-only", &a, "node-x")],
                    },
                    ChunkSources {
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
}
