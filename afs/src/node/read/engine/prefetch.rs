//! Cached whole-object prefetch.

use super::*;

struct CachedFetchContext {
    pub(super) batch: ReadBatch,
    pub(super) sources: ChunkSources,
    pub(super) generation: Option<u64>,
    pub(super) expires_at: Instant,
    authority: NativeReadAuthoritySnapshot,
    scope_check: Arc<dyn Fn() -> Result<()> + Send + Sync>,
}

impl DfsReadEngine {
    pub(in crate::node) fn readahead_object_limit(&self) -> usize {
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
    pub(in crate::node) fn prefetch_cached(
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
                fetcher.spawn_background_async(permit, context.expires_at, async move {
                    reader.prefetch_one_async(&context, &control, leader).await
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
                let hook = self
                    .test_hooks
                    .prefetch_attempt_hook
                    .lock()
                    .unwrap()
                    .clone();
                if let Some(hook) = hook {
                    hook(window, chunk_id);
                }
            }
        }
    }

    async fn prefetch_one_async(
        &self,
        context: &CachedFetchContext,
        control: &PrefetchControl,
        mut leader: super::super::peer_cache::OwnedFillLeader,
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
        match crate::node::bridge::local_io(|| cache.try_contains_ready(chunk))? {
            Some(false) => {}
            ready => {
                leader.finish(if ready == Some(true) {
                    super::super::peer_cache::FillOutcome::Ready
                } else {
                    super::super::peer_cache::FillOutcome::Uncached
                });
                return Ok(());
            }
        }
        let Some(mut operation) = crate::node::bridge::local_io(|| cache.try_operation())? else {
            leader.finish(super::super::peer_cache::FillOutcome::Uncached);
            return Ok(());
        };
        let Some(mut sink) = crate::node::bridge::local_io(|| operation.try_begin_stream(chunk))?
        else {
            leader.finish(super::super::peer_cache::FillOutcome::Uncached);
            return Ok(());
        };
        if !sink.is_caching() {
            leader.finish(super::super::peer_cache::FillOutcome::Uncached);
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
            .read_verified_object_to_sink_async(
                &batch,
                chunk,
                0,
                &mut empty,
                &mut sink,
                Some(control),
            )
            .await?
        else {
            leader.finish(super::super::peer_cache::FillOutcome::Uncached);
            return Ok(());
        };
        control.check()?;
        let stored = crate::node::bridge::local_io(|| sink.commit(completed))?;
        leader.finish(if stored {
            super::super::peer_cache::FillOutcome::Ready
        } else {
            super::super::peer_cache::FillOutcome::Uncached
        });
        Ok(())
    }
}
