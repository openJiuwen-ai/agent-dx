//! The exclusive read route for externally committed (S3) Chunks.

use super::*;

/// Immutable full-object content whose length and BLAKE3 match its chunk identity.
/// Private fields prevent a reader from claiming verification of arbitrary bytes.
#[derive(Clone, Debug)]
pub struct VerifiedExternalChunk {
    pub(super) chunk: ChunkObject,
    pub(super) bytes: Arc<Vec<u8>>,
}

impl VerifiedExternalChunk {
    /// Verify the entire object before any requested range can become visible.
    /// Returns corrupt-data for a size/digest mismatch or an oversized object.
    pub fn verify(chunk: &ChunkObject, bytes: Vec<u8>) -> Result<Self> {
        Self::verify_with_digest(chunk, bytes, "serial", 1, 0, blake3::hash)
    }

    pub(in crate::node) fn verify_with_rayon_pool(
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

    pub(in crate::node) fn load_with_rayon_pool(
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

    pub(in crate::node) fn matches(&self, chunk: &ChunkObject) -> bool {
        self.chunk == *chunk
    }

    /// Strip verified identity only when no reader or fill still owns the payload.
    /// The caller must retain capacity accounting and fully reverify before reuse.
    pub(in crate::node) fn try_into_bytes(self) -> std::result::Result<Vec<u8>, Self> {
        match Arc::try_unwrap(self.bytes) {
            Ok(bytes) => Ok(bytes),
            Err(bytes) => Err(Self {
                chunk: self.chunk,
                bytes,
            }),
        }
    }

    pub(in crate::node) fn allocation_weak(&self) -> Weak<Vec<u8>> {
        Arc::downgrade(&self.bytes)
    }

    pub(in crate::node) fn allocation_bytes(&self) -> u64 {
        self.bytes.capacity() as u64
    }

    pub(in crate::node) fn same_allocation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.bytes, &other.bytes)
    }
}

pub(super) struct ExternalReadBackend {
    pub(super) reader: Arc<dyn ExternalChunkReader>,
    pub(super) authorizer: Arc<dyn ExternalReadAuthorizer>,
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

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
pub(super) struct ExternalAuthorityKey {
    namespace_id: crate::dfs::NamespaceId,
    pub(super) inode_id: crate::dfs::InodeId,
    file_version_id: FileVersionId,
    layout_root_id: LayoutRootId,
    chunk_id: ChunkId,
}

pub(super) struct ExternalAuthorityEntry {
    pub(super) key: ExternalAuthorityKey,
    pub(super) chunk: ChunkObject,
    copy_id: crate::dfs::CopyId,
    location: CopyLocation,
    grant: DfsReadGrant,
    allowed_ranges: Vec<(u64, u64)>,
    pub(super) expires_at_unix_ms: u64,
    pub(super) deadline: Instant,
    node_epoch: u64,
    pub(super) bytes: usize,
    pub(super) invalidated: AtomicBool,
    _reservation: Option<Arc<ExternalAuthorityReservation>>,
}

pub(super) struct ExternalAuthorityReservation {
    pub(super) bytes: usize,
    pub(super) budget: Weak<Mutex<ExternalAuthorityBudget>>,
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
pub(super) struct ExternalAuthorityBudget {
    pub(super) bytes: usize,
    pub(super) entries: usize,
}

#[derive(Default)]
pub(super) struct ExternalAuthorityCache {
    // Local rejection serializes in-flight output, including a fresh context
    // that has not yet been retained. It does not stop the Node/session.
    invalidation_generation: u64,
    pub(super) entries: HashMap<ExternalAuthorityKey, Arc<ExternalAuthorityEntry>>,
    pub(super) budget: Arc<Mutex<ExternalAuthorityBudget>>,
}

impl ExternalAuthorityCache {
    pub(super) fn lookup(
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

    pub(super) fn insert(
        &mut self,
        mut entry: ExternalAuthorityEntry,
    ) -> Option<Arc<ExternalAuthorityEntry>> {
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

    pub(super) fn invalidate_inode_version(
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CurrentExternalReadOutcome {
    Read,
    Unsupported,
    HeadChanged(crate::dfs::InodeRecord),
}

pub(super) struct ExternalReadSelection {
    pub(super) batch: ReadBatch,
    pub(super) selected: Vec<ExternalReadSelected>,
    pub(super) validations: Vec<DfsReadValidation>,
    /// Only the test driver `read_batch_current_external` resolves these.
    #[cfg(test)]
    pub(super) missing_sources: Vec<DfsMissingReadSource>,
}

pub(super) struct ExternalReadSelected {
    chunk_id: ChunkId,
    chunk_offset: u64,
    length: u64,
    output_offset: usize,
    pub(super) chunk: Option<ChunkObject>,
    pub(super) source: Option<SourceCandidate>,
}

pub(super) struct AuthorizedExternalRead {
    pub(super) chunk: ChunkObject,
    pub(super) source: SourceCandidate,
    pub(super) op: ChunkReadOp,
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
    /// Test driver; the product S3 read enters through
    /// `read_external_authorized_selection`.
    #[cfg(test)]
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
        batch.validate_output(out.len(), &self.config, false)?;
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
        self.read_batch_external_version_ahead(inode_id, batch, out, &Vec::new)
    }

    /// [`Self::read_batch_external_version`] with authority read-ahead: when
    /// this read has to ask Meta, the same request also covers the ranges
    /// `ahead` returns (chunks that follow in the same version and layout),
    /// so a sequential reader asks once per window, not once per chunk.
    /// `ahead` is called only on a cache miss. Ranges it names are never
    /// fetched here, and a refusal of them never fails this read.
    pub fn read_batch_external_version_ahead(
        &self,
        inode_id: &crate::dfs::InodeId,
        batch: &ReadBatch,
        out: &mut [u8],
        ahead: &dyn Fn() -> Vec<DfsMissingReadSource>,
    ) -> Result<CurrentExternalReadOutcome> {
        batch.validate_output(out.len(), &self.config, false)?;
        let mut packed = batch.clone();
        let mut size = 0;
        for op in &mut packed.ops {
            op.output_offset = size;
            size += op.length as usize;
        }
        let published =
            self.with_external_version_result(inode_id, &packed, size, ahead, |staged| {
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
        self.read_batch_external_version_owned_ahead(inode_id, batch, output_len, &Vec::new)
    }

    /// [`Self::read_batch_external_version_owned`] with authority read-ahead;
    /// see [`Self::read_batch_external_version_ahead`].
    pub fn read_batch_external_version_owned_ahead(
        &self,
        inode_id: &crate::dfs::InodeId,
        batch: &ReadBatch,
        output_len: usize,
        ahead: &dyn Fn() -> Vec<DfsMissingReadSource>,
    ) -> Result<Option<Vec<u8>>> {
        if output_len as u64 > self.config.max_inflight_bytes {
            return Err(invalid("DFS owned read exceeds inflight byte budget"));
        }
        batch.validate_output(output_len, &self.config, false)?;
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
        self.with_external_version_result(inode_id, batch, output_len, ahead, |bytes| bytes)
    }

    fn with_external_version_result<T>(
        &self,
        inode_id: &crate::dfs::InodeId,
        batch: &ReadBatch,
        output_len: usize,
        ahead: &dyn Fn() -> Vec<DfsMissingReadSource>,
        publish: impl FnOnce(Vec<u8>) -> T,
    ) -> Result<Option<T>> {
        let Some(backend) = &self.external_backend else {
            return Ok(None);
        };
        batch.validate_output(output_len, &self.config, false)?;
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
            #[cfg(test)]
            missing_sources: Vec::new(),
        };
        let ExternalVersionPermits {
            mut permits,
            mut authorizations,
            misses,
            invalidation_generation,
        } = self.collect_cached_version_permits(inode_id, &packed, &local)?;
        span.record("auth_hits", (packed.ops.len() - misses.len()) as u64);
        span.record("auth_misses", misses.len() as u64);
        let ahead = if misses.is_empty() {
            Vec::new()
        } else {
            self.uncached_ahead(inode_id, &packed, &local, ahead())?
        };
        let mut source_timing = self.source_cache_timing()?;
        let mut authorized_ahead = Vec::new();
        let first = self.authorize_version_misses(
            inode_id,
            &packed,
            (misses.clone(), ahead.clone()),
            &mut authorizations,
            &mut authorized_ahead,
            source_timing.sent,
        );
        let outcome = match first {
            // Read-ahead is advisory: whatever Meta refused, ask again for
            // exactly what this read needs and let that answer decide.
            Err(_) if !ahead.is_empty() => {
                source_timing = self.source_cache_timing()?;
                authorized_ahead.clear();
                self.authorize_version_misses(
                    inode_id,
                    &packed,
                    (misses, Vec::new()),
                    &mut authorizations,
                    &mut authorized_ahead,
                    source_timing.sent,
                )?
            }
            other => other?,
        };
        let new_deadline = match outcome {
            VersionMissAuthorization::Unsupported => return Ok(None),
            VersionMissAuthorization::Authorized(deadline) => deadline,
        };
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
        if let Some(fresh_deadline) = new_deadline {
            let mut cutoff = fresh_deadline;
            if let Some(session_deadline) = local.expires_at {
                cutoff = cutoff.min(session_deadline);
            }
            self.install_version_authorities(
                &mut cache,
                inode_id,
                &packed,
                &authorized,
                &mut permits,
                cutoff,
            )?;
            for (requested, authorization) in ahead.iter().zip(&authorized_ahead) {
                self.install_ahead_authority(
                    &mut cache,
                    inode_id,
                    &packed,
                    &local,
                    (requested, authorization),
                    cutoff,
                )?;
            }
        }
        // Invalidation takes the same locks: transferring this private result
        // or copying into a borrowed output is the publication boundary.
        Ok(Some(publish(staged)))
    }

    /// Cached authority per nonempty op under one cache lock; misses are the
    /// ranges that need a Meta authorization.
    fn collect_cached_version_permits(
        &self,
        inode_id: &crate::dfs::InodeId,
        packed: &ReadBatch,
        local: &NativeReadAuthoritySnapshot,
    ) -> Result<ExternalVersionPermits> {
        let mut permits = Vec::with_capacity(packed.ops.len());
        let mut authorizations = Vec::with_capacity(packed.ops.len());
        let mut misses = Vec::new();
        let mut cache = self
            .external_authority_cache
            .lock()
            .map_err(|_| unavailable("DFS external authority cache is poisoned"))?;
        let invalidation_generation = cache.invalidation_generation;
        for op in &packed.ops {
            let key = self.external_authority_key(inode_id, packed, &op.chunk_id)?;
            let permit = cache.lookup(&key, op.chunk_offset, op.length, local);
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
        Ok(ExternalVersionPermits {
            permits,
            authorizations,
            misses,
            invalidation_generation,
        })
    }

    /// One Meta authorization for every cache miss, anchored at send time.
    /// Fills the missing `authorizations` in order.
    fn authorize_version_misses(
        &self,
        inode_id: &crate::dfs::InodeId,
        packed: &ReadBatch,
        (mut misses, ahead): (Vec<DfsMissingReadSource>, Vec<DfsMissingReadSource>),
        authorizations: &mut [Option<DfsExternalAuthorizedRead>],
        authorized_ahead: &mut Vec<DfsExternalAuthorizedRead>,
        rpc_started: Instant,
    ) -> Result<VersionMissAuthorization> {
        let backend = self
            .external_backend
            .as_ref()
            .ok_or_else(|| unavailable("external read backend is not configured"))?;
        if misses.is_empty() {
            return Ok(VersionMissAuthorization::Authorized(None));
        }
        let needed = misses.len();
        // A request widened by read-ahead proves nothing about this read when
        // it fails: the caller repeats it without read-ahead, and only that
        // answer may invalidate cached authority.
        let advisory = !ahead.is_empty();
        misses.extend(ahead);
        let version = packed
            .file_version_id
            .as_ref()
            .ok_or_else(|| invalid("external read requires a version"))?;
        let count = misses.len();
        let reply = tracing::info_span!("node.dfs.read.version_auth_rpc", requests = count)
            .in_scope(|| {
                backend
                    .authorizer
                    .authorize_external_version_read(AuthorizeExternalVersionRead {
                        caller_id: self.local_node_id.clone(),
                        inode_id: inode_id.clone(),
                        namespace_id: self.namespace_id.clone(),
                        file_version_id: version.clone(),
                        layout_root_id: packed.layout_root_id.clone(),
                        requests: misses,
                    })
            });
        let reply = match reply {
            Ok(Some(reply)) => reply,
            Ok(None) => return Ok(VersionMissAuthorization::Unsupported),
            Err(error) => {
                if !advisory && crate::node::fault::classify::read_authority_rejected(&error) {
                    self.invalidate_external_authority(inode_id, version)?;
                }
                return Err(error);
            }
        };
        if reply.authorizations.len() != count
            || reply.remaining_validity_ms == 0
            || reply.remaining_validity_ms > 5_000
        {
            if !advisory {
                self.invalidate_external_authority(inode_id, version)?;
            }
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
        // The reply is in request order: this read's misses, then read-ahead.
        let mut fresh = reply.authorizations.into_iter();
        let mut filled = 0;
        for authorization in authorizations.iter_mut() {
            if authorization.is_none() && filled < needed {
                *authorization = fresh.next();
                filled += 1;
            }
        }
        authorized_ahead.extend(fresh);
        Ok(VersionMissAuthorization::Authorized(Some(deadline)))
    }

    /// The read-ahead ranges worth asking Meta for: bounded, one per chunk,
    /// not part of this read and without a usable cached authority.
    fn uncached_ahead(
        &self,
        inode_id: &crate::dfs::InodeId,
        packed: &ReadBatch,
        local: &NativeReadAuthoritySnapshot,
        ahead: Vec<DfsMissingReadSource>,
    ) -> Result<Vec<DfsMissingReadSource>> {
        if ahead.is_empty() {
            return Ok(ahead);
        }
        let mut cache = self
            .external_authority_cache
            .lock()
            .map_err(|_| unavailable("DFS external authority cache is poisoned"))?;
        let mut wanted: Vec<DfsMissingReadSource> = Vec::new();
        for range in ahead {
            if wanted.len() == MAX_EXTERNAL_AUTHORITY_AHEAD {
                break;
            }
            if range.length == 0
                || packed.ops.iter().any(|op| op.chunk_id == range.chunk_id)
                || wanted.iter().any(|entry| entry.chunk_id == range.chunk_id)
            {
                continue;
            }
            let key = self.external_authority_key(inode_id, packed, &range.chunk_id)?;
            if cache
                .lookup(&key, range.chunk_offset, range.length, local)
                .is_none()
            {
                wanted.push(range);
            }
        }
        Ok(wanted)
    }

    /// Retains one read-ahead authority. It was never used for a fetch, so
    /// only what the cache key and deadline rest on is checked here; the first
    /// read that takes it from the cache runs the full per-read validation.
    /// An answer that does not match the request is dropped, not an error.
    fn install_ahead_authority(
        &self,
        cache: &mut ExternalAuthorityCache,
        inode_id: &crate::dfs::InodeId,
        packed: &ReadBatch,
        local: &NativeReadAuthoritySnapshot,
        (requested, authorization): (&DfsMissingReadSource, &DfsExternalAuthorizedRead),
        fresh_cutoff: Instant,
    ) -> Result<()> {
        let validation = &authorization.authorized.validation;
        if authorization.chunk.id != requested.chunk_id
            || validation.chunk_id != requested.chunk_id
            || !matches!(authorization.location, CopyLocation::External { .. })
            || (local.epoch != 0 && validation.grant.caller_node_epoch != local.epoch)
            || !range_covered(
                &authorization.authorized.allowed_ranges,
                requested.chunk_offset,
                requested.length,
            )
        {
            return Ok(());
        }
        let mut permit = [None];
        let mut one = packed.clone();
        one.ops = vec![ChunkReadOp {
            chunk_id: requested.chunk_id.clone(),
            chunk_offset: requested.chunk_offset,
            length: requested.length,
            output_offset: 0,
        }];
        self.install_version_authorities(
            cache,
            inode_id,
            &one,
            std::slice::from_ref(authorization),
            &mut permit,
            fresh_cutoff,
        )
    }

    /// Retain the freshly authorized contexts, none past `fresh_cutoff` (the
    /// fresh and session deadlines). Runs under the publication locks the
    /// caller holds.
    fn install_version_authorities(
        &self,
        cache: &mut ExternalAuthorityCache,
        inode_id: &crate::dfs::InodeId,
        packed: &ReadBatch,
        authorized: &[DfsExternalAuthorizedRead],
        permits: &mut [Option<Arc<ExternalAuthorityEntry>>],
        fresh_cutoff: Instant,
    ) -> Result<()> {
        for (index, authorization) in authorized.iter().enumerate() {
            if permits[index].is_some() {
                continue;
            }
            let key = self.external_authority_key(inode_id, packed, &packed.ops[index].chunk_id)?;
            let mut cutoff = fresh_cutoff;
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
        Ok(())
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

    pub(super) fn read_external_batch(
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

    #[cfg(test)]
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
            #[cfg(test)]
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

    #[cfg(test)]
    pub(super) fn cached_sources_for(
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
            let mut ranges = Vec::new();
            for op in reads
                .iter()
                .filter(|read| read.chunk.id == chunk.id)
                .map(|read| &read.op)
            {
                let length = usize::try_from(op.length)
                    .map_err(|_| invalid("external Chunk range is too large"))?;
                copied_bytes = copied_bytes.saturating_add(length as u64);
                ranges.push(ExternalRangeCopy {
                    chunk_offset: op.chunk_offset,
                    length,
                    output_offset: op.output_offset,
                });
            }
            // `out` is this read's private staging buffer: nothing in it is
            // published unless every chunk of the batch verifies.
            tracing::info_span!(
                "node.dfs.read.external_fetch",
                chunk_id = %chunk.id.0,
                chunk_bytes = chunk.length,
                ranges = ranges.len(),
            )
            .in_scope(|| {
                backend
                    .reader
                    .read_chunk_ranges(chunk, &read.source, &ranges, out)
            })?;
            fetched_chunks = fetched_chunks.saturating_add(1);
        }
        tracing::Span::current().record("fetched_chunks", fetched_chunks);
        tracing::Span::current().record("copied_bytes", copied_bytes);
        Ok(Some(expires_at))
    }

    pub(super) fn cache_authorized_external_sources(
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

    #[cfg(test)]
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

    #[cfg(test)]
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
}

/// Cached version authority looked up under one cache lock.
struct ExternalVersionPermits {
    permits: Vec<Option<Arc<ExternalAuthorityEntry>>>,
    authorizations: Vec<Option<DfsExternalAuthorizedRead>>,
    misses: Vec<DfsMissingReadSource>,
    invalidation_generation: u64,
}

enum VersionMissAuthorization {
    /// The authorizer does not support fixed-version authorization.
    Unsupported,
    /// Every miss is authorized; the fresh deadline when there were misses.
    Authorized(Option<Instant>),
}
