//! Optional full-chunk read copies. File versions, placement and Peer grants
//! remain authoritative; this path never commits a new file version.
use super::*;

impl DfsReadEngine {
    /// Complete only already authorized immutable chunks. Bounds include the
    /// expanded bytes, rather than only the application's smaller request.
    pub(super) async fn read_cache_full_chunks(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        deadline: Option<Instant>,
    ) -> Result<bool> {
        if self.config.read_cache_bytes == 0
            || self.cache_session.is_none()
            || !self.async_read_enabled()
        {
            return Ok(false);
        }
        let mut full = ReadBatch {
            file_version_id: batch.file_version_id.clone(),
            layout_root_id: batch.layout_root_id.clone(),
            ops: Vec::new(),
            lookahead: Vec::new(),
        };
        let mut bytes = 0u64;
        for op in &batch.ops {
            let Some(set) = sources.iter().find(|s| s.chunk_id == op.chunk_id) else {
                return Ok(false);
            };
            let Some(chunk) = &set.chunk else {
                return Ok(false);
            };
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
                return Err(invalid(
                    "read cache source has invalid chunk or grant ranges",
                ));
            }
            if !range_covered(&set.allowed_ranges, 0, chunk.length)
                || chunk.length > super::super::chunk::MAX_STAGED_CHUNK_BYTES as u64
                || chunk.length.saturating_add(4096) > self.config.read_cache_bytes
                || !set
                    .sources
                    .iter()
                    .any(|source| self.peer_cache_source_eligible(batch, chunk, source))
            {
                return Ok(false);
            }
            if op
                .chunk_offset
                .checked_add(op.length)
                .is_none_or(|end| end > chunk.length)
            {
                return Err(invalid("read cache request exceeds immutable chunk"));
            }
            if full.ops.iter().any(|old| old.chunk_id == op.chunk_id) {
                continue;
            }
            let next = bytes
                .checked_add(chunk.length)
                .ok_or_else(|| invalid("read cache fill overflow"))?;
            if next > self.config.max_inflight_bytes {
                return Ok(false);
            }
            full.ops.push(ChunkReadOp {
                chunk_id: op.chunk_id.clone(),
                chunk_offset: 0,
                length: chunk.length,
                output_offset: bytes as usize,
            });
            bytes = next;
        }
        if full.ops.is_empty() {
            return Ok(false);
        }
        let deadline = deadline.unwrap_or_else(|| Instant::now() + self.read_pipeline_timeout());
        // Same immutable chunks share existing bounded coordination primitives.
        // Global key order prevents cross-batch lock cycles; no body is allocated while waiting.
        let keys = full
            .ops
            .iter()
            .map(|op| op.chunk_id.clone())
            .collect::<BTreeSet<_>>();
        let mut held = Vec::with_capacity(keys.len());
        for id in keys {
            let gate = match self
                .source_query_gates
                .entry(format!("read-cache:{}:{}", self.namespace_id.0, id.0))
            {
                Ok(gate) => gate,
                Err(_) => return Ok(false),
            };
            held.push(
                tokio::time::timeout_at(deadline.into(), gate.lock_owned())
                    .await
                    .map_err(|_| {
                        Error::coded(
                            afs_error::CLIENT_DEADLINE_EXCEEDED,
                            "read cache coordination deadline exceeded",
                        )
                    })?,
            );
        }
        let Ok(_permit) = self.cache_fetch_admission.clone().try_acquire_owned() else {
            return Ok(false);
        };
        let authority = self.capture_native_read_authority()?;
        let mut body = vec![0; bytes as usize];
        full.validate_for_output(body.len(), &self.config)?;
        // The previous holder may have installed the bytes while this request waited.
        let missing = super::super::read_pipeline::sync_read_backend(|| {
            let mut missing = Vec::new();
            for op in &full.ops {
                let target = &mut body[op.output_offset..op.output_offset + op.length as usize];
                match self
                    .local
                    .open_verified(&op.chunk_id)
                    .and_then(|reader| reader.read_at(0, target))
                {
                    Ok(count) if count == target.len() => {}
                    Ok(_) => missing.push(op.clone()),
                    Err(error) => {
                        self.handle_local_read_failure(&op.chunk_id, &error)?;
                        missing.push(op.clone());
                    }
                }
            }
            Ok(missing)
        })?;
        if !missing.is_empty() {
            let remote = ReadBatch {
                ops: missing,
                ..full.clone()
            };
            self.read_remote_batch_uncached(&remote, sources, &mut body, true, Some(deadline))
                .await?;
            if let Err(error) = self
                .cache_completed_ranges(&remote, sources, &body, Some(deadline))
                .await
            {
                afs_logging::warn!("dfs.read_cache_skipped"; "error" => error.to_string());
            }
        }
        self.check_native_read_authority(&authority)?;
        for op in &batch.ops {
            let original = full
                .ops
                .iter()
                .find(|old| old.chunk_id == op.chunk_id)
                .ok_or_else(|| invalid("read cache mapping disappeared"))?;
            let offset = original.output_offset + op.chunk_offset as usize;
            let length = op.length as usize;
            let end = op
                .output_offset
                .checked_add(length)
                .ok_or_else(|| invalid("read cache scatter overflow"))?;
            let target = out
                .get_mut(op.output_offset..end)
                .ok_or_else(|| invalid("read cache scatter out of bounds"))?;
            target.copy_from_slice(&body[offset..offset + length]);
        }
        Ok(true)
    }

    pub(super) async fn cache_completed_ranges(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &[u8],
        deadline: Option<Instant>,
    ) -> Result<()> {
        let Some(session) = &self.cache_session else {
            return Ok(());
        };
        let Some(version) = &batch.file_version_id else {
            return Ok(());
        };
        let authority = self.capture_native_read_authority()?;
        let deadline = deadline.unwrap_or_else(|| Instant::now() + self.read_pipeline_timeout());
        if authority.epoch == 0 || Instant::now() >= deadline {
            return Ok(());
        }
        let mut seen = BTreeSet::new();
        for op in &batch.ops {
            let Some(set) = sources.iter().find(|s| s.chunk_id == op.chunk_id) else {
                continue;
            };
            let Some(chunk) = set.chunk.as_ref() else {
                continue;
            };
            // No speculative neighbour IO. Only a complete requested immutable
            // chunk can be advertised; partial replies never become full copies.
            if op.chunk_offset != 0
                || op.length != chunk.length
                || chunk.length > self.config.max_inflight_bytes
                || chunk.length.saturating_add(4096) > self.config.read_cache_bytes
                || !seen.insert(chunk.id.clone())
            {
                continue;
            }
            let Ok(permit) = self.cache_admission.clone().try_acquire_owned() else {
                continue;
            };
            let length =
                usize::try_from(chunk.length).map_err(|_| invalid("read cache length overflow"))?;
            let end = op
                .output_offset
                .checked_add(length)
                .ok_or_else(|| invalid("read cache output overflow"))?;
            let body = out
                .get(op.output_offset..end)
                .ok_or_else(|| invalid("read cache output is incomplete"))?
                .to_vec();
            let chunk = chunk.clone();
            let store = self.local.clone();
            let capacity = self.config.read_cache_bytes;
            let operation_id = crate::dfs::OperationId::new(format!(
                "read-cache:{session}:{}",
                self.source_turn.fetch_add(1, Ordering::Relaxed)
            ));
            let binding = (
                self.namespace_id.clone(),
                version.clone(),
                batch.layout_root_id.clone(),
            );
            tokio::task::spawn_blocking(move || {
                // The permit lives with the job even if its caller is cancelled.
                let _permit = permit;
                let staged =
                    super::super::chunk::StagedChunk::from_replica(operation_id, &chunk, body)?;
                let write = store.persist_read_cache(&staged, authority.epoch, capacity)?;
                if let Some(receipt) = &write.receipt {
                    store.track_read_cache(super::super::chunk::ReadCacheIntent {
                        namespace_id: binding.0,
                        file_version_id: binding.1,
                        layout_root_id: binding.2,
                        copy: crate::dfs::read_cache_copy(receipt)?,
                    })?;
                }
                Ok::<_, Error>(())
            })
            .await
            .map_err(|e| unavailable(format!("read cache worker failed: {e}")))??;
            self.check_native_read_authority(&authority)?;
            self.reconcile_read_caches(deadline).await?;
        }
        Ok(())
    }
}
