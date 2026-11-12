//! Bounded hash verification for S3 objects.

use super::*;

#[cfg(test)]
pub(super) struct HashBlocker {
    state: Mutex<HashBlockerState>,
    pub(super) wake: Condvar,
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct HashBlockerState {
    started: bool,
    release: bool,
}

#[cfg(test)]
impl HashBlocker {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(HashBlockerState::default()),
            wake: Condvar::new(),
        })
    }

    fn block_until_released(&self) {
        let mut state = self.state.lock().unwrap();
        state.started = true;
        self.wake.notify_all();
        while !state.release {
            state = self.wake.wait(state).unwrap();
        }
    }

    pub(super) fn wait_started(&self) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut state = self.state.lock().unwrap();
        while !state.started {
            let now = std::time::Instant::now();
            assert!(now < deadline, "hash worker did not start before timeout");
            let timeout = deadline.saturating_duration_since(now);
            let (next, _) = self.wake.wait_timeout(state, timeout).unwrap();
            state = next;
        }
    }

    pub(super) fn release(&self) {
        let mut state = self.state.lock().unwrap();
        state.release = true;
        self.wake.notify_all();
    }
}

pub(super) struct CacheJob {
    pub(super) key: CacheKey,
    pub(super) chunk: ChunkObject,
    pub(super) bytes: VerifiedExternalChunk,
    pub(super) length: u64,
}

pub(super) enum CacheFillResult {
    Installed,
    Failed { temp_removed: bool },
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct S3ChunkVerifierStats {
    active_hashes: usize,
    max_active_hashes: usize,
    get_hashes: usize,
    ready_hashes: usize,
    ready_segment_read_events: usize,
    ready_segment_hash_events: usize,
    shutdowns: usize,
    worker_count: usize,
    joined_workers: usize,
    panic_next_hash: bool,
    hash_blocker: Option<Arc<HashBlocker>>,
}

pub(super) struct S3ChunkVerifierInner {
    pool: Option<rayon_core::ThreadPool>,
    handles: Vec<thread::JoinHandle<()>>,
    failure: Option<&'static str>,
    #[cfg(test)]
    stats: S3ChunkVerifierStats,
}

pub(super) struct S3ChunkVerifier {
    pub(super) inner: Mutex<S3ChunkVerifierInner>,
    gate: Mutex<()>,
    closed: AtomicBool,
    metrics: Mutex<Option<S3ChunkStoreMetrics>>,
}

impl S3ChunkVerifier {
    pub(super) fn new() -> Result<Arc<Self>> {
        Self::new_with_builder(S3_HASH_WORKERS, |builder| builder)
    }

    fn new_with_builder<F>(workers: usize, configure: F) -> Result<Arc<Self>>
    where
        F: FnOnce(rayon_core::ThreadPoolBuilder) -> rayon_core::ThreadPoolBuilder,
    {
        let handles = Arc::new(Mutex::new(Vec::with_capacity(workers)));
        let spawned = handles.clone();
        let builder = configure(
            rayon_core::ThreadPoolBuilder::new()
                .num_threads(workers)
                .thread_name(|index| format!("afs-s3-hash-{index}")),
        )
        .spawn_handler(move |thread| {
            let mut builder = thread::Builder::new();
            if let Some(name) = thread.name() {
                builder = builder.name(name.to_owned());
            }
            if let Some(stack_size) = thread.stack_size() {
                builder = builder.stack_size(stack_size);
            }
            let handle = builder.spawn(move || thread.run())?;
            spawned
                .lock()
                .map_err(|_| std::io::Error::other("S3 verifier handle lock poisoned"))?
                .push(handle);
            Ok(())
        });
        let pool = match builder.build() {
            Ok(pool) => pool,
            Err(error) => {
                let partial = drain_spawned_handles(&handles);
                let (_, join_failure) = join_verifier_handles(partial);
                let suffix = join_failure
                    .map(|failure| format!("; {failure}"))
                    .unwrap_or_default();
                return Err(unavailable(format!(
                    "cannot start S3 hash verifier worker pool: {error}{suffix}"
                )));
            }
        };
        let handles = drain_spawned_handles(&handles);
        #[cfg(test)]
        let worker_count = handles.len();
        Ok(Arc::new(Self {
            inner: Mutex::new(S3ChunkVerifierInner {
                pool: Some(pool),
                handles,
                failure: None,
                #[cfg(test)]
                stats: S3ChunkVerifierStats {
                    worker_count,
                    ..S3ChunkVerifierStats::default()
                },
            }),
            gate: Mutex::new(()),
            closed: AtomicBool::new(false),
            metrics: Mutex::new(None),
        }))
    }

    pub(super) fn set_metrics(&self, metrics: Option<S3ChunkStoreMetrics>) -> Result<()> {
        let mut current = self
            .metrics
            .lock()
            .map_err(|_| unavailable("S3 verifier metrics lock poisoned"))?;
        *current = metrics;
        Ok(())
    }

    pub(super) fn verify_get(
        &self,
        chunk: &ChunkObject,
        bytes: Vec<u8>,
    ) -> Result<VerifiedExternalChunk> {
        self.digest_verified(chunk, bytes)
    }

    pub(super) fn verify_ready_segments(
        &self,
        chunk: &ChunkObject,
        file: &File,
        bytes: Vec<u8>,
    ) -> Result<VerifiedExternalChunk> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(unavailable("S3 verifier is shut down"));
        }
        let gate_start = std::time::Instant::now();
        let (_gate, gate_wait_us) = {
            let gate_span = tracing::info_span!(
                "node.dfs.s3.cache.load.verifier_gate",
                gate_wait_us = tracing::field::Empty,
            );
            let gate = gate_span.in_scope(|| {
                self.gate
                    .lock()
                    .map_err(|_| unavailable("S3 verifier gate poisoned"))
            })?;
            let gate_wait_us = gate_start.elapsed().as_micros() as u64;
            gate_span.record("gate_wait_us", gate_wait_us);
            (gate, gate_wait_us)
        };
        if self.closed.load(Ordering::SeqCst) {
            return Err(unavailable("S3 verifier is shut down"));
        }
        let mut inner = tracing::info_span!("node.dfs.s3.cache.load.verifier_state_lock")
            .in_scope(|| {
                self.inner
                    .lock()
                    .map_err(|_| unavailable("S3 verifier lock poisoned"))
            })?;
        if let Some(failure) = inner.failure {
            return Err(unavailable(failure));
        }
        #[cfg(test)]
        let (hash_blocker, panic_next_hash) = {
            inner.stats.active_hashes += 1;
            inner.stats.max_active_hashes =
                inner.stats.max_active_hashes.max(inner.stats.active_hashes);
            inner.stats.ready_hashes += 1;
            let panic_next_hash = inner.stats.panic_next_hash;
            inner.stats.panic_next_hash = false;
            (inner.stats.hash_blocker.take(), panic_next_hash)
        };
        let Some(pool) = inner.pool.as_ref() else {
            return Err(unavailable("S3 verifier is shut down"));
        };
        #[cfg(test)]
        let read_segments = std::sync::atomic::AtomicUsize::new(0);
        #[cfg(test)]
        let hash_segments = std::sync::atomic::AtomicUsize::new(0);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            VerifiedExternalChunk::load_with_rayon_pool(
                chunk,
                bytes,
                pool,
                gate_wait_us,
                |offset, segment, tail| {
                    read_file_segment(file, chunk, offset, segment, tail)?;
                    #[cfg(test)]
                    read_segments.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                || probe_ready_eof(file, chunk),
                |_offset| {
                    #[cfg(test)]
                    {
                        hash_segments.fetch_add(1, Ordering::SeqCst);
                        if _offset == 0 {
                            if panic_next_hash {
                                panic!("injected S3 verifier panic");
                            }
                            if let Some(blocker) = &hash_blocker {
                                blocker.block_until_released();
                            }
                        }
                    }
                },
            )
        }));
        #[cfg(test)]
        {
            inner.stats.ready_segment_read_events += read_segments.load(Ordering::SeqCst);
            inner.stats.ready_segment_hash_events += hash_segments.load(Ordering::SeqCst);
        }
        #[cfg(test)]
        {
            inner.stats.active_hashes = inner.stats.active_hashes.saturating_sub(1);
        }
        if let Ok(Ok(_)) = &result
            && let Ok(metrics) = self.metrics.lock()
            && let Some(metrics) = &*metrics
        {
            metrics.observe_ready_verify(chunk.length, gate_start.elapsed());
        }
        match result {
            Ok(result) => result,
            Err(payload) => {
                self.closed.store(true, Ordering::SeqCst);
                inner.failure = Some("S3 verifier worker panicked");
                Err(panic_error("S3 verifier worker panicked", payload))
            }
        }
    }

    /// Close verification admission, wait for any active hash, join private
    /// worker threads and report any stored verifier failure.
    pub fn shutdown(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        let _gate = self
            .gate
            .lock()
            .map_err(|_| unavailable("S3 verifier gate poisoned"))?;
        let (pool, handles) = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| unavailable("S3 verifier lock poisoned"))?;
            #[cfg(test)]
            if inner.pool.is_some() || !inner.handles.is_empty() {
                inner.stats.shutdowns += 1;
            }
            (inner.pool.take(), std::mem::take(&mut inner.handles))
        };
        drop(pool);
        let (joined, join_failure) = join_verifier_handles(handles);
        #[cfg(not(test))]
        let _ = joined;
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| unavailable("S3 verifier lock poisoned"))?;
        #[cfg(test)]
        {
            inner.stats.joined_workers += joined;
        }
        if inner.failure.is_none() {
            inner.failure = join_failure;
        }
        if let Some(failure) = inner.failure {
            return Err(unavailable(failure));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn worker_count_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.worker_count
    }

    #[cfg(test)]
    pub(super) fn joined_workers_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.joined_workers
    }

    #[cfg(test)]
    pub(super) fn shutdowns_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.shutdowns
    }

    #[cfg(test)]
    pub(super) fn max_active_hashes_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.max_active_hashes
    }

    #[cfg(test)]
    pub(super) fn get_hashes_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.get_hashes
    }

    #[cfg(test)]
    pub(super) fn ready_hashes_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.ready_hashes
    }

    #[cfg(test)]
    pub(super) fn panic_next_hash_for_test(&self) {
        self.inner.lock().unwrap().stats.panic_next_hash = true;
    }

    #[cfg(test)]
    pub(super) fn block_next_hash_for_test(&self) -> Arc<HashBlocker> {
        let blocker = HashBlocker::new();
        self.inner.lock().unwrap().stats.hash_blocker = Some(blocker.clone());
        blocker
    }

    #[cfg(test)]
    pub(super) fn ready_segment_events_for_test(&self) -> (usize, usize, usize) {
        let stats = &self.inner.lock().unwrap().stats;
        (
            stats.ready_segment_read_events,
            stats.ready_segment_hash_events,
            usize::from(stats.ready_segment_read_events > 1 && stats.ready_segment_hash_events > 1),
        )
    }
}

impl Drop for S3ChunkVerifier {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown() {
            tracing::warn!(message = %error.message(), "S3 verifier shutdown failed");
        }
    }
}

impl S3ChunkVerifier {
    fn digest_verified(
        &self,
        chunk: &ChunkObject,
        bytes: Vec<u8>,
    ) -> Result<VerifiedExternalChunk> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(unavailable("S3 verifier is shut down"));
        }
        let gate_start = std::time::Instant::now();
        let _gate = self
            .gate
            .lock()
            .map_err(|_| unavailable("S3 verifier gate poisoned"))?;
        if self.closed.load(Ordering::SeqCst) {
            return Err(unavailable("S3 verifier is shut down"));
        }
        let gate_wait_us = gate_start.elapsed().as_micros() as u64;
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| unavailable("S3 verifier lock poisoned"))?;
        if let Some(failure) = inner.failure {
            return Err(unavailable(failure));
        }
        #[cfg(test)]
        let panic_next_hash = {
            inner.stats.active_hashes += 1;
            inner.stats.max_active_hashes =
                inner.stats.max_active_hashes.max(inner.stats.active_hashes);
            inner.stats.get_hashes += 1;
            let panic_next_hash = inner.stats.panic_next_hash;
            inner.stats.panic_next_hash = false;
            panic_next_hash
        };
        #[cfg(test)]
        let hash_blocker = inner.stats.hash_blocker.take();
        let Some(pool) = inner.pool.as_ref() else {
            return Err(unavailable("S3 verifier is shut down"));
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            #[cfg(test)]
            if panic_next_hash {
                return pool.install(|| -> Result<VerifiedExternalChunk> {
                    panic!("injected S3 verifier panic")
                });
            }
            #[cfg(test)]
            if let Some(blocker) = hash_blocker {
                return pool.install(|| -> Result<VerifiedExternalChunk> {
                    blocker.block_until_released();
                    VerifiedExternalChunk::verify_with_rayon_pool(
                        chunk,
                        bytes,
                        pool,
                        S3_HASH_WORKERS,
                        gate_wait_us,
                    )
                });
            }
            VerifiedExternalChunk::verify_with_rayon_pool(
                chunk,
                bytes,
                pool,
                S3_HASH_WORKERS,
                gate_wait_us,
            )
        }));
        #[cfg(test)]
        {
            inner.stats.active_hashes = inner.stats.active_hashes.saturating_sub(1);
        }
        match result {
            Ok(result) => result,
            Err(payload) => {
                self.closed.store(true, Ordering::SeqCst);
                inner.failure = Some("S3 verifier worker panicked");
                Err(panic_error("S3 verifier worker panicked", payload))
            }
        }
    }
}
