//! The verified local cache of S3 Chunks and its eviction.

use super::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct CacheKey {
    pub(super) store_id: String,
    pub(super) generation: String,
    chunk_id: ChunkId,
}

impl CacheKey {
    pub(super) fn new(store_id: &str, generation: &str, chunk: &ChunkObject) -> Self {
        Self {
            store_id: store_id.into(),
            generation: generation.into(),
            chunk_id: chunk.id.clone(),
        }
    }
}

pub(super) enum CacheEntry {
    Pending { bytes: VerifiedExternalChunk },
    Ready { length: u64, access_bucket: u64 },
}

pub(super) struct CacheResident {
    pub(super) key: CacheKey,
    pub(super) bytes: VerifiedExternalChunk,
}

pub(super) struct TrackedAllocation {
    pub(super) bytes: Weak<Vec<u8>>,
    allocation_bytes: u64,
}

pub(super) struct RawReadyReservation<'a> {
    pub(super) shared: &'a CacheShared,
    allocation_bytes: u64,
    active: bool,
}

impl Drop for RawReadyReservation<'_> {
    fn drop(&mut self) {
        if self.active {
            self.shared.release_raw_ready_load(self.allocation_bytes);
        }
    }
}

#[derive(Default)]
pub(super) struct CacheInner {
    pub(super) stopped: bool,
    pub(super) active_jobs: usize,
    pub(super) pending_bytes: u64,
    pub(super) raw_ready_bytes: u64,
    pub(super) temp_bytes: u64,
    pub(super) ready_bytes: u64,
    pub(super) entries: HashMap<CacheKey, CacheEntry>,
    // Bounded sampling index; precise positions do not break access-bucket ties.
    pub(super) lru: VecDeque<CacheKey>,
    pub(super) jobs: VecDeque<CacheJob>,
    pub(super) resident: Option<CacheResident>,
    pub(super) tracked_allocations: Vec<TrackedAllocation>,
    pub(super) temp_sequence: u64,
    pub(super) sampler_seed: u64,
    pub(super) activity: CacheActivity,
    #[cfg(test)]
    pub(super) test_hooks: CacheTestHooks,
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct CacheTestHooks {
    pub(super) eviction_samples: VecDeque<(usize, usize)>,
    pub(super) forced_access_bucket: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct CacheActivity {
    pub(super) pending_budget_rejects: u64,
    pub(super) capacity_rejects: u64,
    pub(super) task_rejects: u64,
    pub(super) admissions: u64,
    pub(super) evictions: u64,
    pub(super) eviction_ns_total: u64,
    pub(super) eviction_ns_max: u64,
}

impl CacheActivity {
    pub(super) fn max_fields(self, other: Self) -> Self {
        Self {
            pending_budget_rejects: self
                .pending_budget_rejects
                .max(other.pending_budget_rejects),
            capacity_rejects: self.capacity_rejects.max(other.capacity_rejects),
            task_rejects: self.task_rejects.max(other.task_rejects),
            admissions: self.admissions.max(other.admissions),
            evictions: self.evictions.max(other.evictions),
            eviction_ns_total: self.eviction_ns_total.max(other.eviction_ns_total),
            eviction_ns_max: self.eviction_ns_max.max(other.eviction_ns_max),
        }
    }
}

pub(super) struct EvictionVictim {
    pub(super) key: CacheKey,
    lru_position: usize,
    policy: &'static str,
    first_bucket: u64,
    second_bucket: u64,
    selected_bucket: u64,
    selection: &'static str,
}

pub(super) struct S3ChunkCache {
    pub(super) shared: Arc<CacheShared>,
    pub(super) worker: Mutex<Option<thread::JoinHandle<()>>>,
    pub(super) _lock: File,
}

pub(super) struct CacheShared {
    pub(super) options: S3ChunkCacheOptions,
    pub(super) store_id: String,
    pub(super) generation: String,
    pub(super) verifier: Arc<S3ChunkVerifier>,
    pub(super) inner: Mutex<CacheInner>,
    pub(super) wake: Condvar,
}

impl S3ChunkCache {
    pub(super) fn new(
        options: S3ChunkCacheOptions,
        store_id: &str,
        generation: &str,
        verifier: Arc<S3ChunkVerifier>,
    ) -> Result<Arc<Self>> {
        validate_cache_options(&options)?;
        let shared = Arc::new(CacheShared {
            options,
            store_id: store_id.into(),
            generation: generation.into(),
            verifier,
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        });
        shared.initialize_directory()?;
        let lock = shared.acquire_namespace_lock()?;
        shared.recover_ready_entries()?;
        let worker_shared = shared.clone();
        let worker = thread::Builder::new()
            .name("afs-s3-cache".into())
            .spawn(move || worker_shared.cache_worker())
            .map_err(Error::from)?;
        let cache = Arc::new(Self {
            shared,
            worker: Mutex::new(None),
            _lock: lock,
        });
        *cache
            .worker
            .lock()
            .map_err(|_| unavailable("S3 cache worker lock poisoned"))? = Some(worker);
        Ok(cache)
    }

    #[cfg(test)]
    pub(super) fn lookup(&self, chunk: &ChunkObject) -> Option<VerifiedExternalChunk> {
        self.shared.lookup(chunk)
    }

    pub(super) fn lookup_result(
        &self,
        chunk: &ChunkObject,
    ) -> Result<Option<VerifiedExternalChunk>> {
        self.shared.lookup_result(chunk)
    }

    pub(super) fn schedule_from_read(&self, chunk: &ChunkObject, bytes: VerifiedExternalChunk) {
        self.shared.schedule_from_read(chunk, bytes);
    }

    pub(super) fn schedule_from_write(&self, chunk: &ChunkObject, bytes: &[u8]) {
        self.shared.schedule_from_write(chunk, bytes);
    }

    pub(super) fn activity_snapshot(&self) -> CacheActivity {
        self.shared.activity_snapshot()
    }
}

impl CacheShared {
    fn reserve_raw_ready_load(&self, allocation_bytes: u64) -> Result<RawReadyReservation<'_>> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| unavailable("S3 cache lock poisoned"))?;
        if inner.stopped {
            return Err(unavailable("S3 cache is stopped"));
        }
        prune_tracked_allocations(&mut inner);
        let mut current = pending_allocation_bytes(&inner);
        if current.saturating_add(allocation_bytes) > self.options.pending_bytes {
            inner.resident = None;
            prune_tracked_allocations(&mut inner);
            current = pending_allocation_bytes(&inner);
        }
        if current.saturating_add(allocation_bytes) > self.options.pending_bytes {
            inner.pending_bytes = current;
            return Err(capacity("S3 cache Ready load budget is exhausted"));
        }
        inner.raw_ready_bytes = inner.raw_ready_bytes.saturating_add(allocation_bytes);
        inner.pending_bytes = current + allocation_bytes;
        Ok(RawReadyReservation {
            shared: self,
            allocation_bytes,
            active: true,
        })
    }

    fn release_raw_ready_load(&self, allocation_bytes: u64) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.raw_ready_bytes = inner.raw_ready_bytes.saturating_sub(allocation_bytes);
            prune_tracked_allocations(&mut inner);
            self.wake.notify_all();
        }
    }

    fn transfer_raw_ready_load(
        &self,
        mut reservation: RawReadyReservation<'_>,
        bytes: &VerifiedExternalChunk,
    ) -> bool {
        let mut inner = match self.inner.lock() {
            Ok(inner) => inner,
            Err(_) => return false,
        };
        inner.raw_ready_bytes = inner
            .raw_ready_bytes
            .saturating_sub(reservation.allocation_bytes);
        prune_tracked_allocations(&mut inner);
        let accepted = self.register_allocation_locked(&mut inner, bytes);
        reservation.active = false;
        self.wake.notify_all();
        accepted
    }

    pub(super) fn reserve_ready_load_buffer(
        &self,
        required_len: u64,
    ) -> Result<(Vec<u8>, RawReadyReservation<'_>)> {
        {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| unavailable("S3 cache lock poisoned"))?;
            if inner.stopped {
                return Err(unavailable("S3 cache is stopped"));
            }
            if let Some(resident) = inner.resident.take() {
                match resident.bytes.try_into_bytes() {
                    Ok(mut bytes) if bytes.capacity() as u64 >= required_len => {
                        let allocation_bytes = bytes.capacity() as u64;
                        inner.raw_ready_bytes =
                            inner.raw_ready_bytes.saturating_add(allocation_bytes);
                        prune_tracked_allocations(&mut inner);
                        inner.pending_bytes = pending_allocation_bytes(&inner);
                        bytes.resize(required_len as usize, 0);
                        return Ok((
                            bytes,
                            RawReadyReservation {
                                shared: self,
                                allocation_bytes,
                                active: true,
                            },
                        ));
                    }
                    Ok(_too_small) => {
                        prune_tracked_allocations(&mut inner);
                        inner.pending_bytes = pending_allocation_bytes(&inner);
                    }
                    Err(bytes) => {
                        inner.resident = Some(CacheResident {
                            key: resident.key,
                            bytes,
                        });
                    }
                }
            }
        }

        let reservation = self.reserve_raw_ready_load(required_len)?;
        let bytes = tracing::info_span!("node.dfs.s3.cache.load.allocate_buffer", required_len,)
            .in_scope(|| vec![0_u8; required_len as usize]);
        Ok((bytes, reservation))
    }

    #[cfg(test)]
    pub(super) fn lookup(&self, chunk: &ChunkObject) -> Option<VerifiedExternalChunk> {
        self.lookup_result(chunk).ok().flatten()
    }

    pub(super) fn lookup_result(
        &self,
        chunk: &ChunkObject,
    ) -> Result<Option<VerifiedExternalChunk>> {
        let span = tracing::info_span!(
            "node.dfs.s3.cache.lookup",
            chunk_id = %chunk.id.0,
            chunk_bytes = chunk.length,
            branch = tracing::field::Empty,
            copied_bytes = 0_u64,
        );
        span.in_scope(|| {
            let key = CacheKey::new(&self.store_id, &self.generation, chunk);
            let pending_or_ready = {
                let mut inner = self
                    .inner
                    .lock()
                    .map_err(|_| unavailable("S3 cache lock poisoned"))?;
                if inner.stopped {
                    span.record("branch", "stopped");
                    return Ok(None);
                }
                if let Some(resident) = &inner.resident
                    && resident.key == key
                    && resident.bytes.matches(chunk)
                    && inner.entries.contains_key(&key)
                {
                    let bytes = resident.bytes.clone();
                    touch_cache_access(&mut inner, &key);
                    span.record("branch", "resident");
                    return Ok(Some(bytes));
                }
                match inner.entries.get(&key) {
                    Some(CacheEntry::Pending { bytes }) => {
                        let bytes = bytes.clone();
                        self.install_resident_locked(&mut inner, key.clone(), chunk, bytes.clone());
                        span.record("branch", "pending");
                        Some(bytes.clone())
                    }
                    Some(CacheEntry::Ready { .. }) => {
                        span.record("branch", "ready");
                        touch_cache_access(&mut inner, &key);
                        None
                    }
                    None => {
                        span.record("branch", "miss");
                        return Ok(None);
                    }
                }
            };
            if let Some(bytes) = pending_or_ready {
                return Ok(Some(bytes));
            }
            match self.read_ready_file(chunk) {
                Ok(bytes) => {
                    if let Ok(mut inner) = self.inner.lock()
                        && !inner.stopped
                        && matches!(inner.entries.get(&key), Some(CacheEntry::Ready { .. }))
                    {
                        self.install_resident_locked(&mut inner, key.clone(), chunk, bytes.clone());
                    }
                    Ok(Some(bytes))
                }
                Err(error) if error.code() == afs_error::IO_CAPACITY => {
                    span.record("branch", "budget_reject");
                    Err(error)
                }
                Err(_) => {
                    span.record("branch", "stale_ready");
                    self.remove_ready(&key);
                    Ok(None)
                }
            }
        })
    }

    pub(super) fn schedule_from_read(&self, chunk: &ChunkObject, bytes: VerifiedExternalChunk) {
        self.schedule_verified(chunk, bytes, true);
    }

    pub(super) fn schedule_from_write(&self, chunk: &ChunkObject, bytes: &[u8]) {
        if self.options.cache_large_write || chunk.length < MAX_STAGED_CHUNK_BYTES as u64 {
            self.schedule_raw(chunk, bytes, false);
        }
    }

    pub(super) fn activity_snapshot(&self) -> CacheActivity {
        self.inner
            .lock()
            .map(|inner| inner.activity)
            .unwrap_or_default()
    }

    fn record_capacity_reject_best_effort(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            record_capacity_reject(&mut inner);
        }
    }

    fn schedule_raw(&self, chunk: &ChunkObject, bytes: &[u8], from_read: bool) {
        let length = bytes.len() as u64;
        let span = tracing::info_span!(
            "node.dfs.s3.cache.admit",
            chunk_id = %chunk.id.0,
            chunk_bytes = chunk.length,
            copied_bytes = tracing::field::Empty,
            from_read,
            branch = tracing::field::Empty,
            pending_bytes = tracing::field::Empty,
            ready_bytes = tracing::field::Empty,
            eviction_policy = tracing::field::Empty,
            eviction_victim = tracing::field::Empty,
            eviction_sequence = tracing::field::Empty,
            eviction_diagnostic_dropped = tracing::field::Empty,
            eviction_selection = tracing::field::Empty,
            eviction_first_bucket = tracing::field::Empty,
            eviction_second_bucket = tracing::field::Empty,
            eviction_selected_bucket = tracing::field::Empty,
            eviction_ns = tracing::field::Empty,
        );
        span.in_scope(|| {
            if length == 0
                || length > self.options.pending_bytes
                || length > self.options.capacity_bytes
            {
                self.record_capacity_reject_best_effort();
                span.record("branch", "budget_reject");
                return;
            }
            if verify_bytes(chunk, bytes).is_err() {
                span.record("branch", "verify_reject");
                return;
            }
            let key = CacheKey::new(&self.store_id, &self.generation, chunk);
            let mut inner = match self.inner.lock() {
                Ok(inner) => inner,
                Err(_) => {
                    span.record("branch", "lock_error");
                    return;
                }
            };
            if inner.stopped || inner.entries.contains_key(&key) {
                span.record("branch", "duplicate_or_stopped");
                return;
            }
            if inner.entries.len() >= MAX_CACHE_ENTRIES
                || inner.jobs.len() + inner.active_jobs >= MAX_CACHE_JOBS
            {
                record_task_reject(&mut inner);
                span.record("branch", "job_reject");
                return;
            }
            prune_tracked_allocations(&mut inner);
            if !can_reserve_allocation_locked(&mut inner, self.options.pending_bytes, length) {
                record_pending_budget_reject(&mut inner);
                span.record("branch", "budget_reject");
                return;
            }
            if !self.evict_for_locked(&mut inner, length) {
                span.record("branch", "capacity_reject");
                return;
            }
            // Admission precedes allocation: rejected/duplicate write fills
            // must not allocate full chunks outside the aggregate pending-byte
            // budget. Read fills already carry an immutable verified buffer.
            let Ok(bytes) = VerifiedExternalChunk::verify(chunk, bytes.to_vec()) else {
                span.record("branch", "copy_verify_reject");
                return;
            };
            span.record("copied_bytes", length);
            if !self.register_allocation_locked(&mut inner, &bytes) {
                record_pending_budget_reject(&mut inner);
                span.record("branch", "budget_reject");
                return;
            }
            self.accept_locked(&mut inner, key, chunk, bytes, length, from_read);
            span.record("branch", "accepted");
            span.record("pending_bytes", inner.pending_bytes);
            span.record("ready_bytes", inner.ready_bytes);
        });
    }

    fn schedule_verified(
        &self,
        chunk: &ChunkObject,
        bytes: VerifiedExternalChunk,
        from_read: bool,
    ) {
        let length = bytes.as_slice().len() as u64;
        let allocation_bytes = bytes.allocation_bytes();
        let span = tracing::info_span!(
            "node.dfs.s3.cache.admit",
            chunk_id = %chunk.id.0,
            chunk_bytes = chunk.length,
            copied_bytes = 0_u64,
            from_read,
            branch = tracing::field::Empty,
            pending_bytes = tracing::field::Empty,
            ready_bytes = tracing::field::Empty,
            eviction_policy = tracing::field::Empty,
            eviction_victim = tracing::field::Empty,
            eviction_sequence = tracing::field::Empty,
            eviction_diagnostic_dropped = tracing::field::Empty,
            eviction_selection = tracing::field::Empty,
            eviction_first_bucket = tracing::field::Empty,
            eviction_second_bucket = tracing::field::Empty,
            eviction_selected_bucket = tracing::field::Empty,
            eviction_ns = tracing::field::Empty,
        );
        span.in_scope(|| {
            if length == 0
                || allocation_bytes > self.options.pending_bytes
                || length > self.options.capacity_bytes
            {
                self.record_capacity_reject_best_effort();
                span.record("branch", "budget_reject");
                return;
            }
            if !bytes.matches(chunk) {
                span.record("branch", "identity_reject");
                return;
            }
            let key = CacheKey::new(&self.store_id, &self.generation, chunk);
            let mut inner = match self.inner.lock() {
                Ok(inner) => inner,
                Err(_) => {
                    span.record("branch", "lock_error");
                    return;
                }
            };
            if inner.stopped || inner.entries.contains_key(&key) {
                span.record("branch", "duplicate_or_stopped");
                return;
            }
            if inner.entries.len() >= MAX_CACHE_ENTRIES
                || inner.jobs.len() + inner.active_jobs >= MAX_CACHE_JOBS
            {
                record_task_reject(&mut inner);
                span.record("branch", "job_reject");
                return;
            }
            prune_tracked_allocations(&mut inner);
            if !can_register_allocation_locked(&mut inner, self.options.pending_bytes, &bytes) {
                record_pending_budget_reject(&mut inner);
                span.record("branch", "budget_reject");
                return;
            }
            if !self.evict_for_locked(&mut inner, length) {
                span.record("branch", "capacity_reject");
                return;
            }
            if !self.register_allocation_locked(&mut inner, &bytes) {
                record_pending_budget_reject(&mut inner);
                span.record("branch", "budget_reject");
                return;
            }
            self.accept_locked(&mut inner, key, chunk, bytes, length, from_read);
            span.record("branch", "accepted");
            span.record("pending_bytes", inner.pending_bytes);
            span.record("ready_bytes", inner.ready_bytes);
        });
    }

    fn accept_locked(
        &self,
        inner: &mut CacheInner,
        key: CacheKey,
        chunk: &ChunkObject,
        bytes: VerifiedExternalChunk,
        length: u64,
        from_read: bool,
    ) {
        inner.temp_bytes += length;
        inner.entries.insert(
            key.clone(),
            CacheEntry::Pending {
                bytes: bytes.clone(),
            },
        );
        inner.jobs.push_back(CacheJob {
            key: key.clone(),
            chunk: chunk.clone(),
            bytes: bytes.clone(),
            length,
        });
        if from_read {
            self.install_resident_locked(inner, key.clone(), chunk, bytes);
            touch_cache_access(inner, &key);
        }
        inner.activity.admissions = inner.activity.admissions.saturating_add(1);
        self.wake.notify_one();
    }

    pub(super) fn evict_for_locked(&self, inner: &mut CacheInner, needed: u64) -> bool {
        loop {
            if inner
                .ready_bytes
                .saturating_add(inner.temp_bytes)
                .saturating_add(needed)
                <= self.options.capacity_bytes
            {
                return true;
            }
            let Some(victim) = select_eviction_victim(inner) else {
                return false;
            };
            let Some(entry) = inner.entries.remove(&victim.key) else {
                continue;
            };
            let CacheEntry::Ready { length, .. } = entry else {
                restore_lru_position(inner, victim.lru_position, victim.key.clone());
                inner.entries.insert(victim.key, entry);
                continue;
            };
            let remove_start = std::time::Instant::now();
            if remove_cache_file(&self.path_for(&victim.key)).is_ok() {
                let elapsed_ns = remove_start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                inner.ready_bytes = inner.ready_bytes.saturating_sub(length);
                clear_resident_for_key(inner, &victim.key);
                record_eviction(inner, elapsed_ns);
                tracing::Span::current().record("eviction_policy", victim.policy);
                tracing::Span::current().record("eviction_victim", victim.key.chunk_id.0.as_str());
                let sequence = inner.activity.evictions;
                let span = tracing::Span::current();
                span.record("eviction_sequence", sequence);
                span.record(
                    "eviction_diagnostic_dropped",
                    sequence.saturating_sub(MAX_CACHE_EVICTION_DIAGNOSTICS),
                );
                // Reuse the existing counter: no event buffer or per-key diagnostic state.
                if sequence <= MAX_CACHE_EVICTION_DIAGNOSTICS {
                    span.record("eviction_selection", victim.selection);
                    span.record("eviction_first_bucket", victim.first_bucket);
                    span.record("eviction_second_bucket", victim.second_bucket);
                    span.record("eviction_selected_bucket", victim.selected_bucket);
                }
                tracing::Span::current().record("eviction_ns", elapsed_ns);
            } else {
                restore_lru_position(inner, victim.lru_position, victim.key.clone());
                inner.entries.insert(
                    victim.key,
                    CacheEntry::Ready {
                        length,
                        access_bucket: victim.selected_bucket,
                    },
                );
                return false;
            }
        }
    }

    pub(super) fn register_allocation_locked(
        &self,
        inner: &mut CacheInner,
        bytes: &VerifiedExternalChunk,
    ) -> bool {
        prune_tracked_allocations(inner);
        let weak = bytes.allocation_weak();
        if inner
            .tracked_allocations
            .iter()
            .any(|tracked| tracked.bytes.ptr_eq(&weak))
        {
            refresh_pending_bytes(inner);
            return true;
        }
        let current = pending_allocation_bytes(inner);
        let allocation_bytes = bytes.allocation_bytes();
        if current.saturating_add(allocation_bytes) > self.options.pending_bytes {
            inner.pending_bytes = current;
            return false;
        }
        inner.tracked_allocations.push(TrackedAllocation {
            bytes: weak,
            allocation_bytes,
        });
        inner.pending_bytes = current + allocation_bytes;
        true
    }

    pub(super) fn install_resident_locked(
        &self,
        inner: &mut CacheInner,
        key: CacheKey,
        chunk: &ChunkObject,
        bytes: VerifiedExternalChunk,
    ) -> bool {
        if inner.stopped || !bytes.matches(chunk) {
            return false;
        }
        if inner
            .resident
            .as_ref()
            .is_some_and(|resident| resident.key == key && resident.bytes.same_allocation(&bytes))
        {
            return true;
        }
        let previous = inner.resident.take();
        drop(previous);
        if !self.register_allocation_locked(inner, &bytes) {
            return false;
        }
        inner.resident = Some(CacheResident { key, bytes });
        true
    }

    fn cache_worker(&self) {
        loop {
            let job = {
                let mut inner = match self.inner.lock() {
                    Ok(inner) => inner,
                    Err(_) => return,
                };
                loop {
                    if let Some(job) = inner.jobs.pop_front() {
                        inner.active_jobs += 1;
                        break job;
                    }
                    if inner.stopped {
                        return;
                    }
                    inner = match self.wake.wait(inner) {
                        Ok(inner) => inner,
                        Err(_) => return,
                    };
                }
            };
            let result = tracing::info_span!(
                "node.dfs.s3.cache.fill",
                chunk_id = %job.chunk.id.0,
                chunk_bytes = job.length,
                written_bytes = 0_u64,
                outcome = tracing::field::Empty,
            )
            .in_scope(|| {
                let result = self.write_ready_file(&job);
                let span = tracing::Span::current();
                match &result {
                    CacheFillResult::Installed => {
                        span.record("written_bytes", job.length);
                        span.record("outcome", "file_installed");
                    }
                    CacheFillResult::Failed { temp_removed } => {
                        span.record(
                            "outcome",
                            if *temp_removed {
                                "failed_temp_removed"
                            } else {
                                "failed_temp_retained"
                            },
                        );
                    }
                }
                result
            });
            let key = job.key.clone();
            let length = job.length;
            drop(job);
            let mut inner = match self.inner.lock() {
                Ok(inner) => inner,
                Err(_) => return,
            };
            prune_tracked_allocations(&mut inner);
            refresh_pending_bytes(&mut inner);
            inner.active_jobs = inner.active_jobs.saturating_sub(1);
            match result {
                CacheFillResult::Installed => {
                    inner.temp_bytes = inner.temp_bytes.saturating_sub(length);
                    inner.ready_bytes += length;
                    let access_bucket = current_cache_access_bucket(&inner);
                    inner.entries.insert(
                        key.clone(),
                        CacheEntry::Ready {
                            length,
                            access_bucket,
                        },
                    );
                    touch_cache_access(&mut inner, &key);
                }
                CacheFillResult::Failed { temp_removed } => {
                    if temp_removed {
                        inner.temp_bytes = inner.temp_bytes.saturating_sub(length);
                    }
                    inner.entries.remove(&key);
                    clear_resident_for_key(&mut inner, &key);
                    remove_cache_tracking(&mut inner, &key);
                }
            }
            prune_tracked_allocations(&mut inner);
        }
    }

    pub(super) fn initialize_directory(&self) -> Result<()> {
        ensure_private_dir(&self.options.directory)?;
        ensure_private_dir(&self.options.directory.join(&self.store_id))?;
        ensure_private_dir(&self.namespace_dir())?;
        Ok(())
    }

    fn acquire_namespace_lock(&self) -> Result<File> {
        let path = self.namespace_dir().join(".owner-lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .map_err(|_| invalid("cannot open S3 cache owner lock"))?;
        validate_private_file(&file, 0)?;
        // SAFETY: flock only observes the valid file descriptor and does not
        // take ownership of it.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                return Err(invalid(format!(
                    "S3 cache namespace is already owned by another process: {error}",
                )));
            }
            return Err(Error::from(error));
        }
        Ok(file)
    }

    pub(super) fn recover_ready_entries(&self) -> Result<()> {
        for entry in fs::read_dir(self.namespace_dir())
            .map_err(|_| invalid("cannot read S3 cache directory"))?
        {
            let entry = entry.map_err(|_| invalid("cannot inspect S3 cache entry"))?;
            let path = entry.path();
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.starts_with(".tmp-") {
                remove_cache_file(&path)
                    .map_err(|_| invalid("cannot remove stale S3 cache temp file"))?;
                continue;
            }
            let chunk = match chunk_from_id(&ChunkId::new(name)) {
                Ok(chunk) => chunk,
                Err(_) => {
                    continue;
                }
            };
            match self.read_file_verified(&path, &chunk) {
                Ok(_) => {
                    let key = CacheKey::new(&self.store_id, &self.generation, &chunk);
                    let length = chunk.length;
                    let mut inner = self
                        .inner
                        .lock()
                        .map_err(|_| unavailable("S3 cache lock poisoned"))?;
                    if inner.entries.len() < MAX_CACHE_ENTRIES
                        && inner.ready_bytes + length <= self.options.capacity_bytes
                    {
                        let access_bucket = current_cache_access_bucket(&inner);
                        inner.entries.insert(
                            key.clone(),
                            CacheEntry::Ready {
                                length,
                                access_bucket,
                            },
                        );
                        inner.ready_bytes += length;
                        touch_cache_access(&mut inner, &key);
                    } else {
                        drop(inner);
                        remove_cache_file(&path)
                            .map_err(|_| invalid("cannot evict excess S3 cache entry"))?;
                    }
                }
                Err(error) if error.code() == afs_error::IO_CAPACITY => return Err(error),
                Err(_) => {
                    remove_cache_file(&path)
                        .map_err(|_| invalid("cannot remove invalid S3 cache entry"))?;
                }
            }
        }
        Ok(())
    }

    fn read_ready_file(&self, chunk: &ChunkObject) -> Result<VerifiedExternalChunk> {
        self.read_file_verified(
            &self.path_for(&CacheKey::new(&self.store_id, &self.generation, chunk)),
            chunk,
        )
    }

    pub(super) fn read_file_verified(
        &self,
        path: &Path,
        chunk: &ChunkObject,
    ) -> Result<VerifiedExternalChunk> {
        let span = tracing::info_span!(
            "node.dfs.s3.cache.load",
            chunk_id = %chunk.id.0,
            chunk_bytes = chunk.length,
            loaded_bytes = tracing::field::Empty,
        );
        span.in_scope(|| {
            let file =
                tracing::info_span!("node.dfs.s3.cache.load.open_private_file").in_scope(|| {
                    OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
                        .open(path)
                        .map_err(|_| unavailable("cannot open S3 cache entry"))
                })?;
            tracing::info_span!(
                "node.dfs.s3.cache.load.validate_private_file",
                expected_bytes = chunk.length,
            )
            .in_scope(|| validate_private_file(&file, chunk.length))?;
            let allocation_bytes = chunk.length;
            let (bytes, reservation) = tracing::info_span!(
                "node.dfs.s3.cache.load.reserve_ready_bytes",
                allocation_bytes,
            )
            .in_scope(|| self.reserve_ready_load_buffer(allocation_bytes))?;
            let verified = self.verifier.verify_ready_segments(chunk, &file, bytes)?;
            span.record("loaded_bytes", verified.as_slice().len() as u64);
            if !tracing::info_span!(
                "node.dfs.s3.cache.load.transfer_ready_bytes",
                loaded_bytes = verified.as_slice().len() as u64,
            )
            .in_scope(|| self.transfer_raw_ready_load(reservation, &verified))
            {
                return Err(capacity("S3 cache Ready load budget is exhausted"));
            }
            Ok(verified)
        })
    }

    fn write_ready_file(&self, job: &CacheJob) -> CacheFillResult {
        let ready = self.path_for(&job.key);
        if ensure_private_dir(&self.namespace_dir()).is_err() {
            return CacheFillResult::Failed { temp_removed: true };
        }
        let Ok(temp) = self.temp_path() else {
            return CacheFillResult::Failed { temp_removed: true };
        };
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&temp)
            .map_err(|_| ())
            .ok();
        let Some(mut file) = file else {
            return CacheFillResult::Failed { temp_removed: true };
        };
        if file.write_all(job.bytes.as_slice()).is_err() {
            return CacheFillResult::Failed {
                temp_removed: remove_cache_file(&temp).is_ok(),
            };
        }
        drop(file);
        debug_assert!(job.bytes.matches(&job.chunk));
        if fs::rename(&temp, &ready).is_err() {
            return CacheFillResult::Failed {
                temp_removed: remove_cache_file(&temp).is_ok(),
            };
        }
        CacheFillResult::Installed
    }

    fn temp_path(&self) -> Result<PathBuf> {
        let sequence = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| unavailable("S3 cache lock poisoned"))?;
            inner.temp_sequence += 1;
            inner.temp_sequence
        };
        Ok(self.namespace_dir().join(format!(".tmp-{sequence}")))
    }

    fn remove_ready(&self, key: &CacheKey) {
        if let Ok(mut inner) = self.inner.lock() {
            let length = match inner.entries.get(key) {
                Some(CacheEntry::Ready { length, .. }) => *length,
                _ => return,
            };
            if remove_cache_file(&self.path_for(key)).is_ok() {
                inner.entries.remove(key);
                inner.ready_bytes = inner.ready_bytes.saturating_sub(length);
                clear_resident_for_key(&mut inner, key);
                remove_cache_tracking(&mut inner, key);
            }
        }
    }

    pub(super) fn namespace_dir(&self) -> PathBuf {
        self.options
            .directory
            .join(&self.store_id)
            .join(&self.generation)
    }

    pub(super) fn path_for(&self, key: &CacheKey) -> PathBuf {
        self.options
            .directory
            .join(&key.store_id)
            .join(&key.generation)
            .join(&key.chunk_id.0)
    }

    #[cfg(test)]
    pub(super) fn clear_resident_for_test(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.resident = None;
            prune_tracked_allocations(&mut inner);
        }
    }
}

impl Drop for S3ChunkCache {
    fn drop(&mut self) {
        // Recover poisoned guards only to stop and join, never to resume cache
        // operations. No worker may remain active when its namespace unlocks.
        let mut inner = self
            .shared
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        inner.stopped = true;
        self.shared.wake.notify_all();
        drop(inner);
        if let Some(worker) = self
            .worker
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
            && worker.join().is_err()
        {
            tracing::warn!("S3 cache worker panicked during shutdown");
        }
        // SAFETY: this live File owns the namespace lock. All cache users and
        // the worker have drained. Explicit unlock also retires the lock when
        // a fork temporarily retains its open-file description before exec.
        if unsafe { libc::flock(self._lock.as_raw_fd(), libc::LOCK_UN) } != 0 {
            tracing::warn!(message = %std::io::Error::last_os_error(), "S3 cache namespace unlock failed");
        }
    }
}

fn remove_cache_file(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

pub(super) fn touch_cache_access(inner: &mut CacheInner, key: &CacheKey) {
    let bucket = current_cache_access_bucket(inner);
    let previous = access_bucket_for(inner, key);
    match previous {
        Some(previous)
            if previous == bucket && inner.lru.iter().any(|candidate| candidate == key) => {}
        Some(previous) if previous == bucket => {
            inner.lru.push_back(key.clone());
        }
        _ => {
            retain_lru_without(&mut inner.lru, key);
            inner.lru.push_back(key.clone());
        }
    }
    if let Some(CacheEntry::Ready { access_bucket, .. }) = inner.entries.get_mut(key) {
        *access_bucket = bucket;
    }
}

fn remove_cache_tracking(inner: &mut CacheInner, key: &CacheKey) {
    retain_lru_without(&mut inner.lru, key);
}

fn retain_lru_without(lru: &mut VecDeque<CacheKey>, key: &CacheKey) {
    lru.retain(|candidate| candidate != key);
}

fn restore_lru_position(inner: &mut CacheInner, position: usize, key: CacheKey) {
    retain_lru_without(&mut inner.lru, &key);
    if position >= inner.lru.len() {
        inner.lru.push_back(key);
    } else {
        inner.lru.insert(position, key);
    }
}

fn current_cache_access_bucket(_inner: &CacheInner) -> u64 {
    #[cfg(test)]
    if let Some(bucket) = _inner.test_hooks.forced_access_bucket {
        return bucket;
    }
    static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
    EPOCH
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_secs()
}

pub(super) fn select_eviction_victim(inner: &mut CacheInner) -> Option<EvictionVictim> {
    let candidate_count = ready_lru_count(inner);
    match candidate_count {
        0 => None,
        1 => {
            let (position, key) = ready_lru_at(inner, 0)?;
            let removed = inner.lru.remove(position);
            debug_assert_eq!(removed.as_ref(), Some(&key));
            let selected_bucket = access_bucket_for(inner, &key).unwrap_or(0);
            Some(EvictionVictim {
                key,
                lru_position: position,
                policy: "single_ready",
                first_bucket: 0,
                second_bucket: 0,
                selected_bucket,
                selection: "single",
            })
        }
        len => {
            let (first, second) = sample_eviction_candidates(inner, len);
            let first = first % len;
            let mut second = second % len;
            if second == first {
                second = (second + 1) % len;
            }
            let first_candidate = ready_lru_at(inner, first)?;
            let second_candidate = ready_lru_at(inner, second)?;
            let first_bucket = access_bucket_for(inner, &first_candidate.1).unwrap_or(0);
            let second_bucket = access_bucket_for(inner, &second_candidate.1).unwrap_or(0);
            let (position, key, selection) = if first_bucket < second_bucket {
                (first_candidate.0, first_candidate.1, "first_older_bucket")
            } else if second_bucket < first_bucket {
                (
                    second_candidate.0,
                    second_candidate.1,
                    "second_older_bucket",
                )
            } else {
                (
                    first_candidate.0,
                    first_candidate.1,
                    "same_bucket_first_sample",
                )
            };
            let removed = inner.lru.remove(position);
            debug_assert_eq!(removed.as_ref(), Some(&key));
            Some(EvictionVictim {
                key,
                lru_position: position,
                policy: "two_ready_older",
                first_bucket,
                second_bucket,
                selected_bucket: first_bucket.min(second_bucket),
                selection,
            })
        }
    }
}

pub(super) fn access_bucket_for(inner: &CacheInner, key: &CacheKey) -> Option<u64> {
    match inner.entries.get(key) {
        Some(CacheEntry::Ready { access_bucket, .. }) => Some(*access_bucket),
        _ => None,
    }
}

fn sample_eviction_candidates(inner: &mut CacheInner, len: usize) -> (usize, usize) {
    #[cfg(test)]
    if let Some(pair) = inner.test_hooks.eviction_samples.pop_front() {
        return pair;
    }
    let first = sample_index(&mut inner.sampler_seed, len);
    let mut second = sample_index(&mut inner.sampler_seed, len);
    if second == first {
        second = (second + 1) % len;
    }
    (first, second)
}

fn ready_lru_count(inner: &CacheInner) -> usize {
    inner
        .lru
        .iter()
        .filter(|key| matches!(inner.entries.get(key), Some(CacheEntry::Ready { .. })))
        .count()
}

fn ready_lru_at(inner: &CacheInner, target: usize) -> Option<(usize, CacheKey)> {
    inner
        .lru
        .iter()
        .enumerate()
        .filter(|(_, key)| matches!(inner.entries.get(key), Some(CacheEntry::Ready { .. })))
        .nth(target)
        .map(|(position, key)| (position, key.clone()))
}

fn sample_index(seed: &mut u64, len: usize) -> usize {
    if *seed == 0 {
        *seed = initial_cache_sample_seed();
    }
    *seed = mix_cache_sample_seed(seed.wrapping_add(0x9e37_79b9_7f4a_7c15));
    (*seed as usize) % len
}

fn mix_cache_sample_seed(seed: u64) -> u64 {
    let mut value = seed;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn initial_cache_sample_seed() -> u64 {
    let time_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0);
    let seed = time_seed ^ u64::from(std::process::id()) ^ 0xa076_1d64_78bd_642f;
    if seed == 0 {
        0xe703_7ed1_a0b4_28db
    } else {
        seed
    }
}

fn clear_resident_for_key(inner: &mut CacheInner, key: &CacheKey) {
    if inner
        .resident
        .as_ref()
        .is_some_and(|resident| resident.key == *key)
    {
        inner.resident = None;
        prune_tracked_allocations(inner);
    }
}

pub(super) fn prune_tracked_allocations(inner: &mut CacheInner) {
    inner
        .tracked_allocations
        .retain(|tracked| tracked.bytes.strong_count() > 0);
    refresh_pending_bytes(inner);
}

fn tracked_allocation_bytes(inner: &CacheInner) -> u64 {
    inner
        .tracked_allocations
        .iter()
        .filter(|tracked| tracked.bytes.strong_count() > 0)
        .fold(0_u64, |sum, tracked| {
            sum.saturating_add(tracked.allocation_bytes)
        })
}

fn pending_allocation_bytes(inner: &CacheInner) -> u64 {
    tracked_allocation_bytes(inner).saturating_add(inner.raw_ready_bytes)
}

fn can_reserve_allocation_locked(
    inner: &mut CacheInner,
    pending_limit: u64,
    allocation_bytes: u64,
) -> bool {
    prune_tracked_allocations(inner);
    let mut current = pending_allocation_bytes(inner);
    if current.saturating_add(allocation_bytes) > pending_limit {
        inner.resident = None;
        prune_tracked_allocations(inner);
        current = pending_allocation_bytes(inner);
    }
    inner.pending_bytes = current;
    current.saturating_add(allocation_bytes) <= pending_limit
}

fn can_register_allocation_locked(
    inner: &mut CacheInner,
    pending_limit: u64,
    bytes: &VerifiedExternalChunk,
) -> bool {
    prune_tracked_allocations(inner);
    let weak = bytes.allocation_weak();
    if inner
        .tracked_allocations
        .iter()
        .any(|tracked| tracked.bytes.ptr_eq(&weak))
    {
        refresh_pending_bytes(inner);
        return true;
    }
    can_reserve_allocation_locked(inner, pending_limit, bytes.allocation_bytes())
}

fn record_pending_budget_reject(inner: &mut CacheInner) {
    inner.activity.pending_budget_rejects = inner.activity.pending_budget_rejects.saturating_add(1);
}

fn record_capacity_reject(inner: &mut CacheInner) {
    inner.activity.capacity_rejects = inner.activity.capacity_rejects.saturating_add(1);
}

fn record_task_reject(inner: &mut CacheInner) {
    inner.activity.task_rejects = inner.activity.task_rejects.saturating_add(1);
}

fn record_eviction(inner: &mut CacheInner, elapsed_ns: u64) {
    inner.activity.evictions = inner.activity.evictions.saturating_add(1);
    inner.activity.eviction_ns_total = inner.activity.eviction_ns_total.saturating_add(elapsed_ns);
    inner.activity.eviction_ns_max = inner.activity.eviction_ns_max.max(elapsed_ns);
}

fn refresh_pending_bytes(inner: &mut CacheInner) {
    inner.pending_bytes = pending_allocation_bytes(inner);
}
