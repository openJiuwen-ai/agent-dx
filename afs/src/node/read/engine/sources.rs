//! Resolving and caching readable copy candidates from Meta.

use super::*;

pub(super) type SourceCacheKey = (FileVersionId, LayoutRootId, ChunkId);

pub(super) const MAX_CACHED_SOURCES: usize = 1024;

pub(super) const MAX_PINNED_READERS: usize = 256;

/// Entry-bounded metadata/pin ownership, not a bound on live payloads or all
/// open descriptors. Eviction drops only this cache's ownership; in-flight
/// clones keep their reader alive until their final holder releases them.
pub(in crate::node) struct BoundedLru<K, V> {
    capacity: usize,
    pub(super) entries: HashMap<K, LruEntry<V>>,
    pub(super) order: BTreeMap<u64, K>,
    pub(super) clock: u64,
}

pub(super) struct LruEntry<V> {
    pub(super) value: V,
    used: u64,
}

impl<K: Clone + Eq + Hash, V> BoundedLru<K, V> {
    pub(in crate::node) fn new(capacity: usize) -> Self {
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

    pub(in crate::node) fn get(&mut self, key: &K) -> Option<&V> {
        let old_tick = self.entries.get(key)?.used;
        self.order.remove(&old_tick);
        let tick = self.next_tick();
        self.order.insert(tick, key.clone());
        self.entries.get_mut(key).map(|entry| {
            entry.used = tick;
            &entry.value
        })
    }

    pub(in crate::node) fn insert(&mut self, key: K, value: V) {
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

    pub(in crate::node) fn remove(&mut self, key: &K) -> Option<V> {
        let entry = self.entries.remove(key)?;
        self.order.remove(&entry.used);
        Some(entry.value)
    }

    pub(in crate::node) fn pop_oldest(&mut self) -> Option<(K, V)> {
        let (_, key) = self.order.pop_first()?;
        let entry = self.entries.remove(&key)?;
        Some((key, entry.value))
    }

    pub(super) fn retain(&mut self, mut keep: impl FnMut(&K, &mut V) -> bool) {
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
    pub(in crate::node) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }
}

enum SourceLookup {
    Ready(Vec<ChunkSources>),
    Fetch(Box<SourceLookupPlan>),
}

struct SourceLookupPlan {
    version: FileVersionId,
    authority: Option<NativeReadAuthoritySnapshot>,
    resolved: Vec<ChunkSources>,
    mandatory: BTreeSet<ChunkId>,
    request: DfsChunkSourcesRequest,
    has_advisory: bool,
    pub(super) generation: Option<u64>,
    span: tracing::Span,
}

enum SourceLookupFinish {
    Ready(Vec<ChunkSources>),
    Retry(ReadBatch),
}

#[derive(Clone)]
pub(super) struct CachedSources {
    pub(super) expires_at: Instant,
    pub(super) sources: ChunkSources,
}

pub(super) struct SourceCacheEntry {
    pub(super) value: CachedSources,
    pub(super) retained_bytes: usize,
}

/// Source metadata only. Charge owned capacities and a conservative table
/// envelope; active read/RPC clones are separate from this resident cache.
pub(super) struct SourceCache {
    pub(super) entries: BoundedLru<SourceCacheKey, SourceCacheEntry>,
    capacity: usize,
    byte_limit: usize,
    table_bytes: usize,
    value_bytes: usize,
    count: usize,
    peak_bytes: usize,
    // Global only for in-flight admission, not a grant fence. Invalidation
    // leaves other immutable scopes resident but prevents any older reply
    // from reinstating data. Overflow permanently disables admission.
    pub(super) generation: Option<u64>,
}

impl SourceCache {
    pub(super) fn new(byte_limit: usize, enabled: bool) -> Self {
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

    pub(super) fn retained_bytes(key: &SourceCacheKey, sources: &ChunkSources) -> usize {
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

    pub(super) fn bytes(&self) -> usize {
        if self.count == 0 {
            0
        } else {
            self.table_bytes.saturating_add(self.value_bytes)
        }
    }

    pub(super) fn record(&self, local: &LocalChunkStore) {
        local.record_source_cache_resources(
            self.count,
            self.bytes(),
            self.peak_bytes,
            self.byte_limit,
        );
    }

    pub(super) fn get(&mut self, key: &SourceCacheKey) -> Option<&CachedSources> {
        self.entries.get(key).map(|entry| &entry.value)
    }

    pub(super) fn insert(&mut self, key: SourceCacheKey, value: CachedSources) {
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

    pub(super) fn retain(
        &mut self,
        mut keep: impl FnMut(&SourceCacheKey, &mut CachedSources) -> bool,
    ) {
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

    pub(super) fn invalidate_scope(&mut self, version: &FileVersionId, layout: &LayoutRootId) {
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

    pub(super) fn clear(&mut self) {
        self.entries = BoundedLru::new(self.capacity);
        self.count = 0;
        self.value_bytes = 0;
        self.generation = self
            .generation
            .and_then(|generation| generation.checked_add(1));
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.count
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.count == 0
    }

    #[cfg(test)]
    pub(super) fn contains_key(&self, key: &SourceCacheKey) -> bool {
        self.entries.contains_key(key)
    }
}

#[derive(Clone, Copy)]
pub(super) struct SourceCacheTiming {
    pub(super) sent: Instant,
    pub(super) sent_wall: u64,
    pub(super) generation: Option<u64>,
    pub(super) authority_deadline: Option<Instant>,
}

impl DfsReadEngine {
    /// Maximum metadata lookahead for this engine; external source
    /// authorization stays on its independent current-version path.
    pub(in crate::node) fn source_lookahead_chunks(&self) -> usize {
        if self.external_backend.is_some()
            || self.config.source_cache_ttl.is_zero()
            || self.config.source_cache_bytes == 0
        {
            0
        } else {
            self.config.source_lookahead_chunks.min(MAX_DFS_READ_OPS)
        }
    }

    pub(super) fn source_cache_timing(&self) -> Result<SourceCacheTiming> {
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

    pub(super) fn source_deadline(
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

    pub(super) fn sources_for(&self, batch: &ReadBatch) -> Result<Vec<ChunkSources>> {
        self.sources_for_attempt(batch, true)
    }

    fn sources_for_attempt(
        &self,
        batch: &ReadBatch,
        allow_generation_retry: bool,
    ) -> Result<Vec<ChunkSources>> {
        let mut current = batch.clone();
        for attempt in 0..2 {
            let retry = allow_generation_retry && attempt == 0;
            let mut plan = match self.prepare_source_lookup(&current, retry)? {
                SourceLookup::Ready(value) => return Ok(value),
                SourceLookup::Fetch(plan) => plan,
            };
            let mut sent = Instant::now();
            let mut sent_wall = now_unix_ms();
            let first = plan
                .span
                .in_scope(|| self.source_provider.sources_for(plan.request.clone()));
            let reply = match first {
                Err(error)
                    if plan.has_advisory
                        && error.code() == afs_error::META_CATALOG_INVALID_REQUEST =>
                {
                    plan.request
                        .chunk_ids
                        .retain(|chunk| plan.mandatory.contains(chunk));
                    sent = Instant::now();
                    sent_wall = now_unix_ms();
                    plan.span
                        .in_scope(|| self.source_provider.sources_for(plan.request.clone()))
                }
                reply => reply,
            };
            match self.finish_source_lookup(&current, plan, reply, sent, sent_wall, retry)? {
                SourceLookupFinish::Ready(value) => return Ok(value),
                SourceLookupFinish::Retry(batch) => current = batch,
            }
        }
        Err(unavailable("DFS source context retry exhausted"))
    }

    pub(super) async fn sources_for_attempt_async(
        &self,
        batch: &ReadBatch,
        allow_generation_retry: bool,
        deadline: Instant,
        lifetime: super::super::admission::ReadLifetime,
    ) -> Result<Vec<ChunkSources>> {
        if let SourceLookup::Ready(value) =
            self.prepare_source_lookup(batch, allow_generation_retry)?
        {
            return Ok(value);
        }
        let authority = self.capture_native_read_authority()?;
        let key = serde_json::to_string(&(
            self.namespace_id.clone(),
            self.local_node_id.clone(),
            batch.file_version_id.clone(),
            batch.layout_root_id.clone(),
            authority.epoch,
        ))
        .map_err(|_| invalid("source coordination key"))?;
        let gate = self.source_query_gates.entry(key)?;
        let guard =
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), gate.lock_owned())
                .await
                .map_err(|_| unavailable("source coordination deadline exceeded"))?;
        if let SourceLookup::Ready(value) =
            self.prepare_source_lookup(batch, allow_generation_retry)?
        {
            return Ok(value);
        }
        let engine = self.task_reader();
        let batch = batch.clone();
        let query_deadline =
            Instant::now() + Duration::from_millis(self.read_pipeline_limits.task_timeout_ms);
        // The real query owns its gate and the originating open/task lifetime.
        // Cancelling one waiter cannot detach an unaccounted query or cancel peers.
        let query = tokio::spawn(async move {
            let _guard = guard;
            let _lifetime = lifetime;
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(query_deadline),
                engine.sources_for_attempt_async_unshared(&batch, allow_generation_retry),
            )
            .await
            .map_err(|_| unavailable("shared source lookup deadline exceeded"))?
        });
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), query)
            .await
            .map_err(|_| unavailable("source lookup waiter deadline exceeded"))?
            .map_err(|e| unavailable(e.to_string()))?
    }

    async fn sources_for_attempt_async_unshared(
        &self,
        batch: &ReadBatch,
        allow_generation_retry: bool,
    ) -> Result<Vec<ChunkSources>> {
        let mut current = batch.clone();
        for attempt in 0..2 {
            let retry = allow_generation_retry && attempt == 0;
            let mut plan = match self.prepare_source_lookup(&current, retry)? {
                SourceLookup::Ready(value) => return Ok(value),
                SourceLookup::Fetch(plan) => plan,
            };
            let mut sent = Instant::now();
            let mut sent_wall = now_unix_ms();
            let first = tracing::Instrument::instrument(
                self.source_provider.sources_for_async(plan.request.clone()),
                plan.span.clone(),
            )
            .await;
            let reply = match first {
                Err(error)
                    if plan.has_advisory
                        && error.code() == afs_error::META_CATALOG_INVALID_REQUEST =>
                {
                    plan.request
                        .chunk_ids
                        .retain(|chunk| plan.mandatory.contains(chunk));
                    sent = Instant::now();
                    sent_wall = now_unix_ms();
                    tracing::Instrument::instrument(
                        self.source_provider.sources_for_async(plan.request.clone()),
                        plan.span.clone(),
                    )
                    .await
                }
                reply => reply,
            };
            match self.finish_source_lookup(&current, plan, reply, sent, sent_wall, retry)? {
                SourceLookupFinish::Ready(value) => return Ok(value),
                SourceLookupFinish::Retry(batch) => current = batch,
            }
        }
        Err(unavailable("DFS source context retry exhausted"))
    }

    fn prepare_source_lookup(
        &self,
        batch: &ReadBatch,
        allow_generation_retry: bool,
    ) -> Result<SourceLookup> {
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
            return Ok(SourceLookup::Ready(resolved));
        }
        let mandatory: BTreeSet<_> = misses.iter().cloned().collect();
        let mut request =
            batch.source_request(self.local_node_id.clone(), self.namespace_id.clone())?;
        request.chunk_ids.retain(|chunk| mandatory.contains(chunk));
        request.chunk_ids.extend(advisory.iter().cloned());
        Ok(SourceLookup::Fetch(Box::new(SourceLookupPlan {
            version,
            authority,
            resolved,
            mandatory,
            request,
            has_advisory: !advisory.is_empty(),
            generation,
            span: span.clone(),
        })))
    }

    fn finish_source_lookup(
        &self,
        batch: &ReadBatch,
        plan: Box<SourceLookupPlan>,
        reply: Result<DfsChunkSourcesReply>,
        sent: Instant,
        sent_wall: u64,
        allow_generation_retry: bool,
    ) -> Result<SourceLookupFinish> {
        let SourceLookupPlan {
            version,
            authority,
            mut resolved,
            mandatory,
            request,
            generation,
            span,
            ..
        } = *plan;
        let matches = |source: &SourceCandidate| {
            self.source_matches(batch, source)
                && authority.as_ref().is_none_or(|view| {
                    view.epoch == 0 || source.read_grant.caller_node_epoch == view.epoch
                })
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
                    return Ok(SourceLookupFinish::Retry(current));
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
                    return Ok(SourceLookupFinish::Retry(current));
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
        Ok(SourceLookupFinish::Ready(resolved))
    }
}
