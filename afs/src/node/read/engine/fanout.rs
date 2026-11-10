//! `FanoutSource`: parallel verified-object fills for private multi-object reads.

use super::*;

/// Children spawned for one fan-out batch, plus why spawning stopped early.
struct FanoutChildren {
    tasks: Vec<tokio::task::JoinHandle<Result<Option<Vec<u8>>>>>,
    fallback: bool,
    admission_failure: Option<Error>,
}

impl DfsReadEngine {
    #[cfg(test)]
    pub(super) fn fanout_private_objects(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        context_deadline: Option<Instant>,
    ) -> Result<bool> {
        let mut result = false;
        self.transfer.block_on_read(Box::pin(async {
            result = self
                .fanout_private_objects_async(batch, sources, out, context_deadline)
                .await?
                .is_done();
            Ok(())
        }))?;
        Ok(result)
    }

    pub(super) async fn fanout_private_objects_async(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
        context_deadline: Option<Instant>,
    ) -> Result<SourceOutcome> {
        let (Some(fetcher), Some(cache)) = (&self.fetcher, &self.peer_cache) else {
            return Ok(SourceOutcome::NotApplicable);
        };
        if !self.fanout_applies(batch, sources) {
            return Ok(SourceOutcome::NotApplicable);
        }
        // Completed handles retain only request scratch or a bounded 512-byte
        // failure until this group joins them. Charge their vector/control too.
        let Some(_group_metadata) = cache
            .proof_budget()
            .try_reserve(batch.ops.len() * 1024 + 4096)
        else {
            fetcher.event("skip_budget");
            return Ok(SourceOutcome::NotApplicable);
        };
        let authority = self.capture_native_read_authority()?;
        let deadline = self.fanout_deadline(batch, sources, &authority, context_deadline);
        let generation = self
            .source_cache
            .lock()
            .map_err(|_| unavailable("DFS source cache poisoned"))?
            .generation;
        let children = self
            .spawn_fanout_children(fetcher, batch, sources, &authority, deadline)
            .await;
        let FanoutChildren {
            tasks,
            mut fallback,
            admission_failure,
        } = children;
        // Join every task, including after the first failure. No partial result
        // reaches the caller's final buffer until all joins and checks succeed.
        let mut results = Vec::with_capacity(tasks.len());
        let mut failure = None;
        for task in tasks {
            match task.await {
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
            return Ok(SourceOutcome::NotApplicable);
        }
        self.publish_fanout(batch, sources, &authority, generation, results, out)?;
        fetcher.event("joined");
        Ok(SourceOutcome::Done)
    }

    /// Static rejection avoids dispatching obviously unsupported objects;
    /// the child also enforces stream-only execution for dynamic decline.
    fn fanout_applies(&self, batch: &ReadBatch, sources: &[ChunkSources]) -> bool {
        if batch.ops.len() < 2 || batch.ops.iter().any(|op| op.length == 0) {
            return false;
        }
        self.transfer.supports_verified_object_sink()
            && !batch.ops.iter().any(|op| {
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
    }

    /// Convert the original grants once; waking or advancing to another
    /// object must never restart this batch's admission lifetime.
    fn fanout_deadline(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        authority: &NativeReadAuthoritySnapshot,
        context_deadline: Option<Instant>,
    ) -> Option<Instant> {
        let admitted_at = Instant::now();
        let admitted_wall = now_unix_ms();
        sources
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
            .min()
    }

    /// Admit and spawn one stream-only peer-cache child per object, in order.
    /// Admission stops at the first budget refusal or failed recheck.
    async fn spawn_fanout_children(
        &self,
        fetcher: &Arc<super::super::object_fetch::ObjectFetcher>,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        authority: &NativeReadAuthoritySnapshot,
        deadline: Option<Instant>,
    ) -> FanoutChildren {
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
            let permit = match fetcher
                .reserve_foreground_async(charge, deadline, || {
                    self.check_native_read_authority(authority)?;
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
                })
                .await
            {
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
            tasks.push(fetcher.spawn_foreground_async(permit, async move {
                let mut scratch = vec![0; length];
                if PeerCacheSource(&reader)
                    .read(&batch, &[selected], &mut scratch, true, true)
                    .await?
                    .is_done()
                {
                    Ok(Some(scratch))
                } else {
                    Ok(None)
                }
            }));
        }
        FanoutChildren {
            tasks,
            fallback,
            admission_failure,
        }
    }

    /// Deliver joined children only while authority, source context and
    /// every grant still hold.
    fn publish_fanout(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        authority: &NativeReadAuthoritySnapshot,
        generation: Option<u64>,
        results: Vec<Vec<u8>>,
        out: &mut [u8],
    ) -> Result<()> {
        self.check_native_read_authority(authority)?;
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
        Ok(())
    }
}
