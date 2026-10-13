//! Bounded synchronous access to immutable S3 chunks. External receipts never
//! manufacture a Node replica acknowledgement or a placement request.

use std::{
    collections::{HashMap, VecDeque},
    fs::{self, DirBuilder, File, OpenOptions},
    future::Future,
    future::poll_fn,
    io::{Read, Write},
    os::fd::AsRawFd,
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::Poll,
    thread,
    time::Duration,
};

use afs_error::{Error, Result};
use afs_tracing::{Instrument, tracing};
use axum::body::Bytes;
use object_store::{
    ClientOptions, ObjectStore, ObjectStoreExt, RetryConfig, aws::AmazonS3Builder,
    path::Path as ObjectPath,
};
use serde::Deserialize;

use crate::{
    config::MAX_DFS_EXTERNAL_CACHE_CAPACITY_BYTES,
    dfs::{
        ChunkEncoding, ChunkId, ChunkObject, ChunkReceipt, ContentDigest, CopyLocation, CopyRole,
        CopyState, DigestAlgorithm, ExternalCommitReceipt, OperationId, ReplicaGroupId,
        SourceCandidate, external_object_key,
    },
    node::{
        chunk::{ChunkStore, DeferredChunkPut, MAX_STAGED_CHUNK_BYTES, StagedChunk},
        dfs_read::{ExternalChunkReader, VerifiedExternalChunk},
    },
};

const MAX_BATCH: usize = 128;
const MAX_UPLOADS: usize = 4;
const MAX_ACTIVE_REQUESTS: usize = 32;
const MAX_CREDENTIAL_BYTES: u64 = 16 * 1024;
const MAX_CACHE_JOBS: usize = 128;
const MAX_CACHE_ENTRIES: usize = 4096;
const S3_HASH_WORKERS: usize = 2;
#[cfg(test)]
const MAX_ACTIVE_HASHES: usize = 1;

#[derive(Clone, Debug)]
pub enum S3CredentialsSource {
    Environment,
    /// JSON containing access_key_id, secret_access_key and optional session_token.
    /// The file must be owned by this process's effective UID, with mode 0600 or stricter.
    File(PathBuf),
}

#[derive(Clone, Debug)]
pub struct S3ChunkStoreOptions {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub store_id: String,
    pub generation: String,
    pub allow_http: bool,
    pub credentials: S3CredentialsSource,
    /// Deadline for a whole batch, including retry and response-body collection.
    pub request_timeout: Duration,
    pub max_retries: usize,
    pub cache: Option<S3ChunkCacheOptions>,
}

#[derive(Clone, Debug)]
pub struct S3ChunkCacheOptions {
    pub directory: PathBuf,
    pub capacity_bytes: u64,
    pub pending_bytes: u64,
    /// Matches the JuiceFS default boundary: large write fill is opt-in; reads
    /// always fill after full-object verification.
    pub cache_large_write: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Credentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

enum Request {
    Put(Vec<StagedChunk>),
    Read(ChunkObject),
}

enum Reply {
    Put(Vec<ChunkReceipt>),
    Read(VerifiedExternalChunk),
}

struct Command {
    request: Request,
    reply: mpsc::SyncSender<Result<Reply>>,
    span: tracing::Span,
    enqueued_at: std::time::Instant,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CacheKey {
    store_id: String,
    generation: String,
    chunk_id: ChunkId,
}

impl CacheKey {
    fn new(store_id: &str, generation: &str, chunk: &ChunkObject) -> Self {
        Self {
            store_id: store_id.into(),
            generation: generation.into(),
            chunk_id: chunk.id.clone(),
        }
    }
}

enum CacheEntry {
    Pending { bytes: VerifiedExternalChunk },
    Ready { length: u64 },
}

struct CacheResident {
    key: CacheKey,
    bytes: VerifiedExternalChunk,
}

struct TrackedAllocation {
    bytes: Weak<Vec<u8>>,
    allocation_bytes: u64,
}

struct CacheJob {
    key: CacheKey,
    chunk: ChunkObject,
    bytes: VerifiedExternalChunk,
    length: u64,
}

enum CacheFillResult {
    Installed,
    Failed { temp_removed: bool },
}

#[cfg(test)]
#[derive(Default)]
struct S3ChunkVerifierStats {
    active_hashes: usize,
    max_active_hashes: usize,
    get_hashes: usize,
    ready_hashes: usize,
    shutdowns: usize,
    worker_count: usize,
    joined_workers: usize,
    panic_next_hash: bool,
}

struct S3ChunkVerifierInner {
    pool: Option<rayon_core::ThreadPool>,
    handles: Vec<thread::JoinHandle<()>>,
    failure: Option<&'static str>,
    #[cfg(test)]
    stats: S3ChunkVerifierStats,
}

struct S3ChunkVerifier {
    inner: Mutex<S3ChunkVerifierInner>,
    gate: Mutex<()>,
    closed: AtomicBool,
}

impl S3ChunkVerifier {
    fn new() -> Result<Arc<Self>> {
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
        }))
    }

    fn verify_get(&self, chunk: &ChunkObject, bytes: Vec<u8>) -> Result<VerifiedExternalChunk> {
        self.verify(VerifySource::Get, chunk, bytes)
    }

    fn verify_ready(&self, chunk: &ChunkObject, bytes: Vec<u8>) -> Result<VerifiedExternalChunk> {
        self.verify(VerifySource::Ready, chunk, bytes)
    }

    fn verify(
        &self,
        source: VerifySource,
        chunk: &ChunkObject,
        bytes: Vec<u8>,
    ) -> Result<VerifiedExternalChunk> {
        self.digest_verified(source, chunk, bytes)
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
    fn worker_count_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.worker_count
    }

    #[cfg(test)]
    fn joined_workers_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.joined_workers
    }

    #[cfg(test)]
    fn shutdowns_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.shutdowns
    }

    #[cfg(test)]
    fn max_active_hashes_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.max_active_hashes
    }

    #[cfg(test)]
    fn get_hashes_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.get_hashes
    }

    #[cfg(test)]
    fn ready_hashes_for_test(&self) -> usize {
        self.inner.lock().unwrap().stats.ready_hashes
    }

    #[cfg(test)]
    fn panic_next_hash_for_test(&self) {
        self.inner.lock().unwrap().stats.panic_next_hash = true;
    }
}

#[derive(Clone, Copy)]
enum VerifySource {
    Get,
    Ready,
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
        source: VerifySource,
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
            match source {
                VerifySource::Get => inner.stats.get_hashes += 1,
                VerifySource::Ready => inner.stats.ready_hashes += 1,
            }
            let panic_next_hash = inner.stats.panic_next_hash;
            inner.stats.panic_next_hash = false;
            panic_next_hash
        };
        #[cfg(not(test))]
        let _ = source;
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

#[derive(Default)]
struct CacheInner {
    stopped: bool,
    active_jobs: usize,
    pending_bytes: u64,
    temp_bytes: u64,
    ready_bytes: u64,
    entries: HashMap<CacheKey, CacheEntry>,
    lru: VecDeque<CacheKey>,
    jobs: VecDeque<CacheJob>,
    resident: Option<CacheResident>,
    tracked_allocations: Vec<TrackedAllocation>,
    temp_sequence: u64,
}

struct S3ChunkCache {
    shared: Arc<CacheShared>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    _lock: File,
}

struct S3DeferredPut {
    receiver: Option<mpsc::Receiver<Result<Reply>>>,
    timeout: Duration,
}

struct CacheShared {
    options: S3ChunkCacheOptions,
    store_id: String,
    generation: String,
    verifier: Arc<S3ChunkVerifier>,
    inner: Mutex<CacheInner>,
    wake: Condvar,
}

impl S3ChunkCache {
    fn new(
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

    fn lookup(&self, chunk: &ChunkObject) -> Option<VerifiedExternalChunk> {
        self.shared.lookup(chunk)
    }

    fn schedule_from_read(&self, chunk: &ChunkObject, bytes: VerifiedExternalChunk) {
        self.shared.schedule_from_read(chunk, bytes);
    }

    fn schedule_from_write(&self, chunk: &ChunkObject, bytes: &[u8]) {
        self.shared.schedule_from_write(chunk, bytes);
    }
}

impl CacheShared {
    fn lookup(&self, chunk: &ChunkObject) -> Option<VerifiedExternalChunk> {
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
                let mut inner = self.inner.lock().ok()?;
                if inner.stopped {
                    span.record("branch", "stopped");
                    return None;
                }
                if let Some(resident) = &inner.resident
                    && resident.key == key
                    && resident.bytes.matches(chunk)
                    && inner.entries.contains_key(&key)
                {
                    let bytes = resident.bytes.clone();
                    touch_lru(&mut inner.lru, &key);
                    span.record("branch", "resident");
                    return Some(bytes);
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
                        touch_lru(&mut inner.lru, &key);
                        None
                    }
                    None => {
                        span.record("branch", "miss");
                        return None;
                    }
                }
            };
            if let Some(bytes) = pending_or_ready {
                return Some(bytes);
            }
            match self.read_ready_file(chunk) {
                Ok(bytes) => {
                    if let Ok(mut inner) = self.inner.lock()
                        && !inner.stopped
                        && matches!(inner.entries.get(&key), Some(CacheEntry::Ready { .. }))
                    {
                        self.install_resident_locked(&mut inner, key.clone(), chunk, bytes.clone());
                    }
                    Some(bytes)
                }
                Err(_) => {
                    span.record("branch", "stale_ready");
                    self.remove_ready(&key);
                    None
                }
            }
        })
    }

    fn schedule_from_read(&self, chunk: &ChunkObject, bytes: VerifiedExternalChunk) {
        self.schedule_verified(chunk, bytes, true);
    }

    fn schedule_from_write(&self, chunk: &ChunkObject, bytes: &[u8]) {
        if self.options.cache_large_write || chunk.length < MAX_STAGED_CHUNK_BYTES as u64 {
            self.schedule_raw(chunk, bytes, false);
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
        );
        span.in_scope(|| {
            if length == 0
                || length > self.options.pending_bytes
                || length > self.options.capacity_bytes
            {
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
                span.record("branch", "job_reject");
                return;
            }
            prune_tracked_allocations(&mut inner);
            if !self.evict_for_locked(&mut inner, length) {
                span.record("branch", "capacity_reject");
                return;
            }
            if tracked_allocation_bytes(&inner).saturating_add(length) > self.options.pending_bytes
            {
                span.record("branch", "budget_reject");
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
        );
        span.in_scope(|| {
            if length == 0
                || allocation_bytes > self.options.pending_bytes
                || length > self.options.capacity_bytes
            {
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
                span.record("branch", "job_reject");
                return;
            }
            prune_tracked_allocations(&mut inner);
            if !self.evict_for_locked(&mut inner, length) {
                span.record("branch", "capacity_reject");
                return;
            }
            if !self.register_allocation_locked(&mut inner, &bytes) {
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
            touch_lru(&mut inner.lru, &key);
        }
        self.wake.notify_one();
    }

    fn evict_for_locked(&self, inner: &mut CacheInner, needed: u64) -> bool {
        loop {
            if inner
                .ready_bytes
                .saturating_add(inner.temp_bytes)
                .saturating_add(needed)
                <= self.options.capacity_bytes
            {
                return true;
            }
            let Some(candidate) = inner.lru.pop_front() else {
                return false;
            };
            let Some(entry) = inner.entries.remove(&candidate) else {
                continue;
            };
            let CacheEntry::Ready { length } = entry else {
                inner.entries.insert(candidate, entry);
                continue;
            };
            if remove_cache_file(&self.path_for(&candidate)).is_ok() {
                inner.ready_bytes = inner.ready_bytes.saturating_sub(length);
                clear_resident_for_key(inner, &candidate);
            } else {
                inner
                    .entries
                    .insert(candidate, CacheEntry::Ready { length });
                return false;
            }
        }
    }

    fn register_allocation_locked(
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
            inner.pending_bytes = tracked_allocation_bytes(inner);
            return true;
        }
        let current = tracked_allocation_bytes(inner);
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

    fn install_resident_locked(
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
                written_bytes = job.length,
            )
            .in_scope(|| self.write_ready_file(&job));
            let key = job.key.clone();
            let length = job.length;
            drop(job);
            let mut inner = match self.inner.lock() {
                Ok(inner) => inner,
                Err(_) => return,
            };
            prune_tracked_allocations(&mut inner);
            inner.pending_bytes = tracked_allocation_bytes(&inner);
            inner.active_jobs = inner.active_jobs.saturating_sub(1);
            match result {
                CacheFillResult::Installed => {
                    inner.temp_bytes = inner.temp_bytes.saturating_sub(length);
                    inner.ready_bytes += length;
                    inner
                        .entries
                        .insert(key.clone(), CacheEntry::Ready { length });
                    touch_lru(&mut inner.lru, &key);
                }
                CacheFillResult::Failed { temp_removed } => {
                    if temp_removed {
                        inner.temp_bytes = inner.temp_bytes.saturating_sub(length);
                    }
                    inner.entries.remove(&key);
                    clear_resident_for_key(&mut inner, &key);
                }
            }
            prune_tracked_allocations(&mut inner);
        }
    }

    fn initialize_directory(&self) -> Result<()> {
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
            return Err(invalid(
                "S3 cache namespace is already owned by another process",
            ));
        }
        Ok(file)
    }

    fn recover_ready_entries(&self) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| unavailable("S3 cache lock poisoned"))?;
        for entry in fs::read_dir(self.namespace_dir())
            .map_err(|_| invalid("cannot read S3 cache directory"))?
        {
            let entry = entry.map_err(|_| invalid("cannot inspect S3 cache entry"))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.starts_with(".tmp-") {
                remove_cache_file(&entry.path())
                    .map_err(|_| invalid("cannot remove stale S3 cache temp file"))?;
                continue;
            }
            let chunk = match chunk_from_id(&ChunkId::new(name)) {
                Ok(chunk) => chunk,
                Err(_) => {
                    continue;
                }
            };
            match self.read_file_verified(&entry.path(), &chunk) {
                Ok(_) => {
                    let key = CacheKey::new(&self.store_id, &self.generation, &chunk);
                    let length = chunk.length;
                    if inner.entries.len() < MAX_CACHE_ENTRIES
                        && inner.ready_bytes + length <= self.options.capacity_bytes
                    {
                        inner
                            .entries
                            .insert(key.clone(), CacheEntry::Ready { length });
                        inner.ready_bytes += length;
                        touch_lru(&mut inner.lru, &key);
                    } else {
                        remove_cache_file(&entry.path())
                            .map_err(|_| invalid("cannot evict excess S3 cache entry"))?;
                    }
                }
                Err(_) => {
                    remove_cache_file(&entry.path())
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

    fn read_file_verified(
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
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
                .open(path)
                .map_err(|_| unavailable("cannot open S3 cache entry"))?;
            validate_private_file(&file, chunk.length)?;
            let mut bytes = Vec::with_capacity(chunk.length as usize);
            file.take(chunk.length + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| unavailable("cannot read S3 cache entry"))?;
            span.record("loaded_bytes", bytes.len() as u64);
            self.verifier.verify_ready(chunk, bytes)
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
                Some(CacheEntry::Ready { length }) => *length,
                _ => return,
            };
            if remove_cache_file(&self.path_for(key)).is_ok() {
                inner.entries.remove(key);
                inner.ready_bytes = inner.ready_bytes.saturating_sub(length);
                clear_resident_for_key(&mut inner, key);
                retain_lru_without(&mut inner.lru, key);
            }
        }
    }

    fn namespace_dir(&self) -> PathBuf {
        self.options
            .directory
            .join(&self.store_id)
            .join(&self.generation)
    }

    fn path_for(&self, key: &CacheKey) -> PathBuf {
        self.options
            .directory
            .join(&key.store_id)
            .join(&key.generation)
            .join(&key.chunk_id.0)
    }

    #[cfg(test)]
    fn clear_resident_for_test(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.resident = None;
            prune_tracked_allocations(&mut inner);
        }
    }
}

impl Drop for S3ChunkCache {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.shared.inner.lock() {
            inner.stopped = true;
            self.shared.wake.notify_all();
        }
        if let Ok(mut worker) = self.worker.lock()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

/// One owned asynchronous worker serves synchronous and deferred requests.
/// All requests share four upload permits; completing one PUT immediately
/// makes its permit available to the next chunk, including another batch.
/// The bounded queue applies backpressure rather than reporting a storage
/// failure. HTTP payload copies are limited to four 4MiB objects; producer
/// payload and descriptor reservations belong to the DFS write pipeline.
/// Cache entries remain disposable, never durable replicas. Drop drains and
/// joins the worker; timed-out PUTs cannot produce durability receipts.
pub struct S3ChunkStore {
    options: S3ChunkStoreOptions,
    sender: Option<mpsc::SyncSender<Command>>,
    wake: Arc<tokio::sync::Notify>,
    worker: Option<thread::JoinHandle<()>>,
    cache: Option<Arc<S3ChunkCache>>,
    verifier: Arc<S3ChunkVerifier>,
}

impl S3ChunkStore {
    /// Rejects invalid identity, credentials, endpoint or resource budgets.
    pub fn new(options: S3ChunkStoreOptions) -> Result<Self> {
        validate_options(&options)?;
        let credentials = load_credentials(&options.credentials)?;
        adx_transport::install_crypto_provider();
        let retry = RetryConfig {
            max_retries: options.max_retries,
            retry_timeout: options.request_timeout,
            ..Default::default()
        };
        let client = ClientOptions::new()
            .with_timeout(options.request_timeout)
            .with_connect_timeout(options.request_timeout.min(Duration::from_secs(10)))
            .with_pool_max_idle_per_host(MAX_UPLOADS);
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(&options.endpoint)
            .with_bucket_name(&options.bucket)
            .with_region(&options.region)
            .with_virtual_hosted_style_request(false)
            .with_access_key_id(credentials.access_key_id)
            .with_secret_access_key(credentials.secret_access_key)
            .with_retry(retry)
            .with_client_options(client)
            .with_allow_http(options.allow_http);
        if let Some(token) = credentials.session_token {
            builder = builder.with_token(token);
        }
        let backend = builder
            .build()
            .map_err(|_| invalid("cannot construct S3 client from configured settings"))?;
        Self::with_backend(options, Arc::new(backend))
    }

    fn with_backend(options: S3ChunkStoreOptions, backend: Arc<dyn ObjectStore>) -> Result<Self> {
        validate_options(&options)?;
        let verifier = S3ChunkVerifier::new()?;
        let cache = options
            .cache
            .clone()
            .map(|cache_options| {
                S3ChunkCache::new(
                    cache_options,
                    &options.store_id,
                    &options.generation,
                    verifier.clone(),
                )
            })
            .transpose()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(Error::from)?;
        let (sender, receiver) = mpsc::sync_channel::<Command>(1);
        let identity = options.clone();
        let wake = Arc::new(tokio::sync::Notify::new());
        let worker_wake = wake.clone();
        let worker_verifier = verifier.clone();
        let worker = thread::Builder::new()
            .name("afs-s3-chunks".into())
            .spawn(move || {
                runtime.block_on(run_requests(
                    backend,
                    identity,
                    receiver,
                    worker_wake,
                    worker_verifier,
                ));
            })
            .map_err(Error::from)?;
        Ok(Self {
            options,
            sender: Some(sender),
            wake,
            worker: Some(worker),
            cache,
            verifier,
        })
    }

    fn enqueue(
        &self,
        request: Request,
    ) -> Result<(mpsc::Receiver<Result<Reply>>, &'static str, usize, u64)> {
        let (reply, receiver) = mpsc::sync_channel(1);
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| unavailable("S3 worker stopped"))?;
        let (operation, chunks, bytes) = request_summary(&request);
        let span = tracing::info_span!(
            "node.dfs.s3.request",
            operation,
            chunks,
            bytes,
            max_uploads = MAX_UPLOADS,
            queue_wait_us = tracing::field::Empty,
        );
        let enqueued_at = std::time::Instant::now();
        // std::sync::mpsc supplies bounded synchronous backpressure even when
        // the caller is a Tokio business thread (blocking_send would panic).
        sender
            .send(Command {
                request,
                reply,
                span: span.clone(),
                enqueued_at,
            })
            .map_err(|_| unavailable("S3 worker stopped"))?;
        self.wake.notify_one();
        Ok((receiver, operation, chunks, bytes))
    }

    fn request(&self, request: Request) -> Result<Reply> {
        let (receiver, operation, chunks, bytes) = self.enqueue(request)?;
        // The worker owns a bounded number of requests and enforces a deadline
        // for each, including waiting for the shared upload permits.
        let wait_span = tracing::info_span!("node.dfs.s3.reply_wait", operation, chunks, bytes,);
        wait_span.in_scope(|| {
            receiver
                .recv_timeout(
                    self.options.request_timeout * (MAX_ACTIVE_REQUESTS as u32 + 2)
                        + Duration::from_secs(1),
                )
                .map_err(|error| match error {
                    mpsc::RecvTimeoutError::Timeout => {
                        Error::coded(afs_error::IO_TIMEOUT, "S3 worker response timed out")
                    }
                    mpsc::RecvTimeoutError::Disconnected => unavailable("S3 worker stopped"),
                })?
        })
    }

    fn read_verified(&self, chunk: &ChunkObject) -> Result<VerifiedExternalChunk> {
        validate_chunk(chunk)?;
        if let Some(cache) = &self.cache
            && let Some(bytes) = cache.lookup(chunk)
        {
            tracing::info_span!(
                "node.dfs.s3.read_verified",
                branch = "cache_hit",
                chunk_id = %chunk.id.0,
                chunk_bytes = chunk.length,
                copied_bytes = 0_u64,
            )
            .in_scope(|| {});
            return Ok(bytes);
        }
        match self.request(Request::Read(chunk.clone()))? {
            Reply::Read(bytes) => {
                if let Some(cache) = &self.cache {
                    cache.schedule_from_read(chunk, bytes.clone());
                }
                Ok(bytes)
            }
            Reply::Put(_) => Err(invalid("S3 worker returned the wrong response")),
        }
    }

    /// Close the private verifier pool. Any stored verifier failure is reported
    /// after all verifier worker threads have been joined.
    pub fn shutdown_verifier(&self) -> Result<()> {
        self.verifier.shutdown()
    }
}

impl DeferredChunkPut for S3DeferredPut {
    fn wait(mut self: Box<Self>) -> Result<Vec<ChunkReceipt>> {
        let wait_span = tracing::info_span!("node.dfs.s3.deferred_put_wait");
        let receiver = self
            .receiver
            .take()
            .ok_or_else(|| invalid("deferred S3 PUT was already consumed"))?;
        let response = wait_span.in_scope(|| {
            receiver
                .recv_timeout(self.timeout)
                .map_err(|error| match error {
                    mpsc::RecvTimeoutError::Timeout => {
                        Error::coded(afs_error::IO_TIMEOUT, "S3 deferred PUT timed out")
                    }
                    mpsc::RecvTimeoutError::Disconnected => unavailable("S3 worker stopped"),
                })?
        })?;
        match response {
            Reply::Put(receipts) => Ok(receipts),
            Reply::Read(_) => Err(invalid("S3 worker returned the wrong response")),
        }
    }
}

impl Drop for S3ChunkStore {
    fn drop(&mut self) {
        drop(self.cache.take());
        drop(self.sender.take());
        self.wake.notify_one();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        if let Err(error) = self.verifier.shutdown() {
            tracing::warn!(message = %error.message(), "S3 verifier shutdown failed");
        }
    }
}

impl ChunkStore for S3ChunkStore {
    fn supports_deferred_put(&self) -> bool {
        true
    }

    fn put_batch_deferred(&self, staged: Vec<StagedChunk>) -> Result<Box<dyn DeferredChunkPut>> {
        if staged.is_empty() {
            return Ok(Box::new(ReadyExternalDeferredPut {
                receipts: Some(Vec::new()),
            }));
        }
        if staged.len() > MAX_BATCH {
            return Err(invalid("S3 batch allows at most 128 chunks"));
        }
        for value in &staged {
            validate_chunk(&value.chunk)?;
            if !value.has_sealed_identity() {
                return Err(invalid("staged Chunk metadata changed after sealing"));
            }
            if let Some(cache) = &self.cache {
                cache.schedule_from_write(&value.chunk, value.bytes());
            }
        }
        let (receiver, _, _, _) = self.enqueue(Request::Put(staged))?;
        Ok(Box::new(S3DeferredPut {
            receiver: Some(receiver),
            timeout: self.options.request_timeout * (MAX_ACTIVE_REQUESTS as u32 + 2)
                + Duration::from_secs(1),
        }))
    }

    fn put_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<ChunkReceipt>> {
        if staged.is_empty() {
            return Ok(Vec::new());
        }
        if staged.len() > MAX_BATCH {
            return Err(invalid("S3 batch allows at most 128 chunks"));
        }
        for value in &staged {
            validate_chunk(&value.chunk)?;
            if !value.has_sealed_identity() {
                return Err(invalid("staged Chunk metadata changed after sealing"));
            }
            if let Some(cache) = &self.cache {
                cache.schedule_from_write(&value.chunk, value.bytes());
            }
        }
        match self.request(Request::Put(staged))? {
            Reply::Put(receipts) => Ok(receipts),
            Reply::Read(_) => Err(invalid("S3 worker returned the wrong response")),
        }
    }

    fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize> {
        let chunk = chunk_from_id(chunk_id)?;
        if offset >= chunk.length || out.is_empty() {
            return Ok(0);
        }
        let verified = self.read_verified(&chunk)?;
        let bytes = verified.as_slice();
        let start = usize::try_from(offset).map_err(|_| invalid("chunk offset is too large"))?;
        let length = out.len().min(bytes.len() - start);
        out[..length].copy_from_slice(&bytes[start..start + length]);
        Ok(length)
    }
}

struct ReadyExternalDeferredPut {
    receipts: Option<Vec<ChunkReceipt>>,
}

impl DeferredChunkPut for ReadyExternalDeferredPut {
    fn wait(mut self: Box<Self>) -> Result<Vec<ChunkReceipt>> {
        self.receipts
            .take()
            .ok_or_else(|| invalid("deferred S3 PUT was already consumed"))
    }
}

impl ExternalChunkReader for S3ChunkStore {
    fn read_chunk(
        &self,
        chunk: &ChunkObject,
        source: &SourceCandidate,
    ) -> Result<VerifiedExternalChunk> {
        let expected =
            external_object_key(&self.options.store_id, &self.options.generation, &chunk.id)?;
        if source.chunk_id != chunk.id
            || source.role != CopyRole::ExternalCommitted
            || source.state != CopyState::Ready
            || !matches!(&source.location, CopyLocation::External { store_id, object_key, object_revision }
                if store_id == &self.options.store_id && object_key == &expected
                    && object_revision == &self.options.generation)
        {
            return Err(invalid(
                "external source does not match the configured immutable object",
            ));
        }
        self.read_verified(chunk)
    }
}

async fn run_requests(
    backend: Arc<dyn ObjectStore>,
    options: S3ChunkStoreOptions,
    receiver: mpsc::Receiver<Command>,
    wake: Arc<tokio::sync::Notify>,
    verifier: Arc<S3ChunkVerifier>,
) {
    let upload_slots = Arc::new(tokio::sync::Semaphore::new(MAX_UPLOADS));
    let mut active: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
    let mut closed = false;
    loop {
        while !closed && active.len() < MAX_ACTIVE_REQUESTS {
            match receiver.try_recv() {
                Ok(command) => {
                    let backend = backend.clone();
                    let options = options.clone();
                    let upload_slots = upload_slots.clone();
                    let verifier = verifier.clone();
                    active.push(Box::pin(async move {
                        let queue_wait_us = command.enqueued_at.elapsed().as_micros() as u64;
                        command.span.record("queue_wait_us", queue_wait_us);
                        let result = async {
                            tokio::time::timeout(
                                options.request_timeout,
                                execute(
                                    &*backend,
                                    &options,
                                    command.request,
                                    queue_wait_us,
                                    &upload_slots,
                                    &verifier,
                                ),
                            )
                            .await
                            .map_err(|_| {
                                Error::coded(afs_error::IO_TIMEOUT, "S3 operation timed out")
                            })?
                        }
                        .instrument(command.span)
                        .await;
                        let _ = command.reply.send(result);
                    }));
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => closed = true,
            }
        }
        if active.is_empty() {
            if closed {
                break;
            }
            wake.notified().await;
            continue;
        }
        let can_receive = !closed && active.len() < MAX_ACTIVE_REQUESTS;
        let progress = poll_fn(|cx| {
            for index in 0..active.len() {
                if active[index].as_mut().poll(cx).is_ready() {
                    drop(active.swap_remove(index));
                    return Poll::Ready(());
                }
            }
            Poll::Pending
        });
        if can_receive {
            tokio::select! {
                _ = progress => {},
                _ = wake.notified() => {},
            }
        } else {
            progress.await;
        }
    }
}

async fn execute(
    backend: &dyn ObjectStore,
    options: &S3ChunkStoreOptions,
    request: Request,
    queue_wait_us: u64,
    upload_slots: &tokio::sync::Semaphore,
    verifier: &S3ChunkVerifier,
) -> Result<Reply> {
    let (operation, chunks, bytes) = request_summary(&request);
    let span = tracing::info_span!(
        "node.dfs.s3.execute",
        operation,
        chunks,
        bytes,
        queue_wait_us,
        max_uploads = MAX_UPLOADS,
    );
    async move {
        match request {
            Request::Put(staged) => {
                let mut receipts = vec![None; staged.len()];
                let mut staged = staged.into_iter().enumerate();
                type UploadFuture<'a> =
                    Pin<Box<dyn Future<Output = Result<(usize, ChunkReceipt)>> + 'a>>;
                let mut active: Vec<UploadFuture<'_>> = Vec::new();
                loop {
                    while active.len() < MAX_UPLOADS {
                        let Some((index, value)) = staged.next() else {
                            break;
                        };
                        active.push(Box::pin(async move {
                            let _permit = upload_slots
                                .acquire()
                                .await
                                .map_err(|_| unavailable("S3 upload scheduler stopped"))?;
                            put_one(backend, options, value)
                                .await
                                .map(|receipt| (index, receipt))
                        }));
                    }
                    if active.is_empty() {
                        break;
                    }
                    let (index, receipt) = poll_fn(|cx| {
                        let mut current = 0usize;
                        while current < active.len() {
                            match active[current].as_mut().poll(cx) {
                                Poll::Ready(result) => {
                                    drop(active.swap_remove(current));
                                    return Poll::Ready(result);
                                }
                                Poll::Pending => {
                                    current += 1;
                                }
                            }
                        }
                        Poll::Pending
                    })
                    .await?;
                    receipts[index] = Some(receipt);
                }
                let receipts = receipts
                    .into_iter()
                    .map(|receipt| {
                        receipt.ok_or_else(|| unavailable("S3 PUT receipt was not produced"))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Reply::Put(receipts))
            }
            Request::Read(chunk) => {
                let key = external_object_key(&options.store_id, &options.generation, &chunk.id)?;
                // No HEAD, versionId or conditional GET in the normal data path.
                let response = async {
                    backend
                        .get(&ObjectPath::from(key.as_str()))
                        .await
                        .map_err(storage_error)
                }
                .instrument(tracing::info_span!(
                    "node.dfs.s3.get",
                    chunk_id = %chunk.id.0,
                    chunk_bytes = chunk.length,
                ))
                .await?;
                if response.meta.size != chunk.length || response.range != (0..chunk.length) {
                    return Err(corrupt(
                        "S3 object response length does not match Chunk metadata",
                    ));
                }
                let limit =
                    usize::try_from(chunk.length).map_err(|_| invalid("chunk is too large"))?;
                let bytes = async {
                    let mut bytes = Vec::with_capacity(limit);
                    let mut stream = response.into_stream();
                    while let Some(part) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
                        let part = part.map_err(storage_error)?;
                        if part.len() > limit.saturating_sub(bytes.len()) {
                            return Err(corrupt(
                                "S3 response body exceeds the immutable Chunk length",
                            ));
                        }
                        bytes.extend_from_slice(&part);
                    }
                    tracing::Span::current().record("copied_bytes", bytes.len() as u64);
                    Ok::<_, Error>(bytes)
                }
                .instrument(tracing::info_span!(
                    "node.dfs.s3.get_body",
                    chunk_id = %chunk.id.0,
                    chunk_bytes = chunk.length,
                    copied_bytes = tracing::field::Empty,
                ))
                .await?;
                Ok(Reply::Read(verifier.verify_get(&chunk, bytes)?))
            }
        }
    }
    .instrument(span)
    .await
}

async fn put_one(
    backend: &dyn ObjectStore,
    options: &S3ChunkStoreOptions,
    value: StagedChunk,
) -> Result<ChunkReceipt> {
    let key = external_object_key(&options.store_id, &options.generation, &value.chunk.id)?;
    let payload = upload_payload(&value)?;
    // Ordinary PUT only: key identity is content-bound and immutable.
    async {
        backend
            .put(&ObjectPath::from(key.as_str()), payload)
            .await
            .map_err(storage_error)
    }
    .instrument(tracing::info_span!(
        "node.dfs.s3.put",
        chunk_id = %value.chunk.id.0,
        chunk_bytes = value.chunk.length,
        copied_bytes = 0_u64,
    ))
    .await?;
    Ok(ChunkReceipt {
        operation_id: value.operation_id,
        chunk: value.chunk.clone(),
        placement_revision: 0,
        placement_epoch: 0,
        replica_group_id: ReplicaGroupId::new(""),
        durable_acks: Vec::new(),
        external_commit: Some(ExternalCommitReceipt {
            store_id: options.store_id.clone(),
            generation: options.generation.clone(),
            object_key: key,
            persisted_bytes: value.chunk.length,
            verified_digest: value.chunk.content_digest,
        }),
    })
}

fn upload_payload(value: &StagedChunk) -> Result<object_store::PutPayload> {
    if !value.has_sealed_identity() {
        return Err(invalid("staged Chunk metadata changed after sealing"));
    }
    // Bytes pins both the immutable allocation and its typed write leases.
    // Retry/transport clones may outlive the request future or caller ticket;
    // the budget releases only when their last owner is actually dropped.
    Ok(object_store::PutPayload::from_bytes(Bytes::from_owner(
        value.clone(),
    )))
}

fn request_summary(request: &Request) -> (&'static str, usize, u64) {
    match request {
        Request::Put(staged) => (
            "put",
            staged.len(),
            staged
                .iter()
                .fold(0_u64, |sum, value| sum.saturating_add(value.chunk.length)),
        ),
        Request::Read(chunk) => ("read", 1, chunk.length),
    }
}

fn validate_options(options: &S3ChunkStoreOptions) -> Result<()> {
    let probe = StagedChunk::new(OperationId::new("identity-probe"), Vec::new());
    external_object_key(&options.store_id, &options.generation, &probe.chunk.id)?;
    if options.bucket.is_empty()
        || options.region.is_empty()
        || options.request_timeout.is_zero()
        || options.request_timeout > Duration::from_secs(300)
        || options.max_retries > 10
        || !(options.endpoint.starts_with("https://") || options.endpoint.starts_with("http://"))
        || options.endpoint.contains(['@', '?', '#'])
    {
        return Err(invalid(
            "invalid S3 endpoint, bucket, region or bounded request settings",
        ));
    }
    if let Some(cache) = &options.cache {
        validate_cache_options(cache)?;
    }
    Ok(())
}

fn validate_cache_options(options: &S3ChunkCacheOptions) -> Result<()> {
    if !options.directory.is_absolute()
        || options.capacity_bytes == 0
        || options.pending_bytes == 0
        || options.pending_bytes > options.capacity_bytes
        || options.capacity_bytes > MAX_DFS_EXTERNAL_CACHE_CAPACITY_BYTES
    {
        return Err(invalid(
            "invalid S3 cache directory or bounded byte budgets",
        ));
    }
    Ok(())
}

fn load_credentials(source: &S3CredentialsSource) -> Result<Credentials> {
    let credentials = match source {
        S3CredentialsSource::Environment => Credentials {
            access_key_id: std::env::var("AWS_ACCESS_KEY_ID")
                .map_err(|_| invalid("AWS_ACCESS_KEY_ID is required"))?,
            secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")
                .map_err(|_| invalid("AWS_SECRET_ACCESS_KEY is required"))?,
            session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        },
        S3CredentialsSource::File(path) => {
            let file = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(path)
                .map_err(|_| invalid("cannot open S3 credentials file"))?;
            let metadata = file
                .metadata()
                .map_err(|_| invalid("cannot inspect S3 credentials file"))?;
            // SAFETY: geteuid has no arguments or memory preconditions.
            let uid = unsafe { libc::geteuid() };
            if !metadata.is_file() || metadata.mode() & 0o077 != 0 || metadata.uid() != uid {
                return Err(invalid(
                    "S3 credentials require a private file owned by the current UID",
                ));
            }
            let mut bytes = Vec::new();
            file.take(MAX_CREDENTIAL_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| invalid("cannot read S3 credentials file"))?;
            if bytes.len() as u64 > MAX_CREDENTIAL_BYTES {
                return Err(invalid("S3 credentials file is too large"));
            }
            serde_json::from_slice::<Credentials>(&bytes)
                .map_err(|_| invalid("invalid S3 credentials file JSON"))?
        }
    };
    if credentials.access_key_id.is_empty() || credentials.secret_access_key.is_empty() {
        return Err(invalid(
            "S3 credentials must contain nonempty access and secret keys",
        ));
    }
    Ok(credentials)
}

fn chunk_from_id(id: &ChunkId) -> Result<ChunkObject> {
    let remainder =
        id.0.strip_prefix("blake3-")
            .ok_or_else(|| invalid("noncanonical immutable Chunk ID"))?;
    let (hex, length) = remainder
        .split_once('-')
        .ok_or_else(|| invalid("noncanonical immutable Chunk ID"))?;
    let length = length
        .parse::<u64>()
        .map_err(|_| invalid("invalid immutable Chunk length"))?;
    if hex.len() != 64
        || length > MAX_STAGED_CHUNK_BYTES as u64
        || !hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || id.0 != format!("blake3-{hex}-{length}")
    {
        return Err(invalid(
            "noncanonical immutable Chunk ID or oversized Chunk",
        ));
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16)
            .map_err(|_| invalid("invalid immutable Chunk digest"))?;
    }
    Ok(ChunkObject {
        id: id.clone(),
        length,
        encoding: ChunkEncoding::Raw,
        content_digest: ContentDigest {
            algorithm: DigestAlgorithm::Blake3,
            bytes,
        },
    })
}

fn validate_chunk(chunk: &ChunkObject) -> Result<()> {
    if &chunk_from_id(&chunk.id)? != chunk {
        return Err(invalid(
            "Chunk metadata does not match its immutable identity",
        ));
    }
    Ok(())
}

fn verify_bytes(chunk: &ChunkObject, bytes: &[u8]) -> Result<()> {
    tracing::info_span!(
        "node.dfs.s3.verify",
        chunk_id = %chunk.id.0,
        chunk_bytes = chunk.length,
        hash_bytes = bytes.len() as u64,
    )
    .in_scope(|| {
        if bytes.len() as u64 != chunk.length
            || blake3::hash(bytes).as_bytes() != &chunk.content_digest.bytes
        {
            return Err(corrupt("immutable Chunk length or BLAKE3 mismatch"));
        }
        Ok(())
    })
}

fn storage_error(error: object_store::Error) -> Error {
    let code = match error {
        object_store::Error::NotFound { .. } => afs_error::IO_NOT_FOUND,
        object_store::Error::PermissionDenied { .. }
        | object_store::Error::Unauthenticated { .. } => afs_error::IO_PERMISSION_DENIED,
        _ => afs_error::IO_UNAVAILABLE,
    };
    // SDK error text can contain request URLs or credential provider details.
    Error::coded(code, "external S3 operation failed")
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_INVALID, message)
}
fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}
fn corrupt(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_CORRUPT_DATA, message)
}

fn drain_spawned_handles(
    handles: &Arc<Mutex<Vec<thread::JoinHandle<()>>>>,
) -> Vec<thread::JoinHandle<()>> {
    match handles.lock() {
        Ok(mut handles) => std::mem::take(&mut *handles),
        Err(_) => Vec::new(),
    }
}

fn join_verifier_handles(handles: Vec<thread::JoinHandle<()>>) -> (usize, Option<&'static str>) {
    let mut joined = 0;
    let mut failure = None;
    for handle in handles {
        joined += 1;
        if handle.join().is_err() && failure.is_none() {
            failure = Some("S3 verifier worker panicked during shutdown");
        }
    }
    (joined, failure)
}

fn panic_error(message: &'static str, payload: Box<dyn std::any::Any + Send + 'static>) -> Error {
    drop(payload);
    unavailable(message)
}

fn ensure_private_dir(path: &Path) -> Result<()> {
    if path.exists() {
        validate_private_dir(path)?;
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| invalid("S3 cache directory requires a parent"))?;
    let parent_metadata = fs::symlink_metadata(parent)
        .map_err(|_| invalid("cannot inspect S3 cache parent directory"))?;
    if parent_metadata.file_type().is_symlink() || !parent_metadata.is_dir() {
        return Err(invalid("S3 cache parent must be a real directory"));
    }
    DirBuilder::new()
        .mode(0o700)
        .create(path)
        .or_else(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(error)
            }
        })
        .map_err(|_| invalid("cannot create S3 cache directory"))?;
    validate_private_dir(path)
}

fn validate_private_dir(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| invalid("cannot inspect S3 cache directory"))?;
    // SAFETY: geteuid has no arguments or memory preconditions.
    let uid = unsafe { libc::geteuid() };
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
    {
        return Err(invalid(
            "S3 cache directory must be private and owned by this UID",
        ));
    }
    Ok(())
}

fn validate_private_file(file: &File, expected_len: u64) -> Result<()> {
    let metadata = file
        .metadata()
        .map_err(|_| unavailable("cannot inspect S3 cache file"))?;
    // SAFETY: geteuid has no arguments or memory preconditions.
    let uid = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
        || (expected_len != 0 && metadata.len() != expected_len)
    {
        return Err(corrupt("S3 cache file identity does not match"));
    }
    Ok(())
}

fn remove_cache_file(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn touch_lru(lru: &mut VecDeque<CacheKey>, key: &CacheKey) {
    retain_lru_without(lru, key);
    lru.push_back(key.clone());
}

fn retain_lru_without(lru: &mut VecDeque<CacheKey>, key: &CacheKey) {
    lru.retain(|candidate| candidate != key);
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

fn prune_tracked_allocations(inner: &mut CacheInner) {
    inner
        .tracked_allocations
        .retain(|tracked| tracked.bytes.strong_count() > 0);
    inner.pending_bytes = tracked_allocation_bytes(inner);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dfs::{CopyId, DfsReadGrant, FileVersionId, LayoutRootId, NamespaceId};
    use std::{io::Write, net::TcpListener, os::unix::fs::PermissionsExt};

    fn options() -> S3ChunkStoreOptions {
        S3ChunkStoreOptions {
            endpoint: "http://127.0.0.1:1".into(),
            bucket: "chunks".into(),
            region: "us-east-1".into(),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            allow_http: true,
            credentials: S3CredentialsSource::Environment,
            request_timeout: Duration::from_secs(2),
            max_retries: 0,
            cache: None,
        }
    }

    fn cache_options(directory: &Path, capacity_bytes: u64) -> S3ChunkCacheOptions {
        S3ChunkCacheOptions {
            directory: directory.join("cache"),
            capacity_bytes,
            pending_bytes: capacity_bytes,
            cache_large_write: false,
        }
    }

    fn cache_path(directory: &Path, generation: &str, chunk: &ChunkObject) -> PathBuf {
        directory
            .join("cache")
            .join("filesystem-a")
            .join(generation)
            .join(&chunk.id.0)
    }

    fn verified(staged: &StagedChunk) -> VerifiedExternalChunk {
        VerifiedExternalChunk::verify(&staged.chunk, staged.bytes().to_vec()).unwrap()
    }

    fn verified_with_capacity(staged: &StagedChunk, capacity: usize) -> VerifiedExternalChunk {
        let mut bytes = Vec::with_capacity(capacity);
        bytes.extend_from_slice(staged.bytes());
        assert!(bytes.capacity() >= capacity);
        VerifiedExternalChunk::verify(&staged.chunk, bytes).unwrap()
    }

    fn wait_for_file(path: &Path) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !path.exists() && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(path.exists(), "cache file was not installed before timeout");
    }

    fn wait_for_ready_entry(cache: &S3ChunkCache, key: &CacheKey) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if cache
                .shared
                .inner
                .lock()
                .unwrap()
                .entries
                .get(key)
                .is_some_and(|entry| matches!(entry, CacheEntry::Ready { .. }))
            {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("cache entry was not marked ready before timeout");
    }

    fn wait_for_file_bytes(path: &Path, expected: &[u8]) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if std::fs::read(path).is_ok_and(|bytes| bytes == expected) {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            std::fs::read(path).unwrap_or_default(),
            expected,
            "cache file did not reach expected content before timeout"
        );
    }

    fn source(chunk: &ChunkObject, key: String) -> SourceCandidate {
        SourceCandidate {
            copy_id: CopyId::new("external"),
            chunk_id: chunk.id.clone(),
            role: CopyRole::ExternalCommitted,
            state: CopyState::Ready,
            location: CopyLocation::External {
                store_id: "filesystem-a".into(),
                object_key: key,
                object_revision: "generation-1".into(),
            },
            data_endpoint: None,
            load_hint: 0,
            read_grant: DfsReadGrant {
                namespace_id: NamespaceId::new("namespace"),
                file_version_id: FileVersionId::new("version"),
                layout_root_id: LayoutRootId::new("layout"),
                caller_node_id: "reader".into(),
                caller_node_epoch: 1,
                expires_at_unix_ms: u64::MAX,
                fence: 1,
                token: "validated-by-engine".into(),
            },
        }
    }

    type TestStream<T> = std::pin::Pin<Box<dyn tokio_stream::Stream<Item = T> + Send>>;

    #[derive(Debug, Default)]
    struct PutActivity {
        active: std::sync::atomic::AtomicUsize,
        maximum: std::sync::atomic::AtomicUsize,
        started: std::sync::atomic::AtomicUsize,
        finished: Mutex<Vec<u8>>,
        payload_addresses: Mutex<Vec<usize>>,
        retain_payloads: std::sync::atomic::AtomicBool,
        retained_payloads: Mutex<Vec<object_store::PutPayload>>,
        hold: std::sync::atomic::AtomicBool,
        hold_first: std::sync::atomic::AtomicBool,
    }

    struct ActivePut<'a>(&'a PutActivity);

    impl Drop for ActivePut<'_> {
        fn drop(&mut self) {
            self.0
                .active
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[derive(Debug)]
    struct ObservedStore {
        inner: object_store::memory::InMemory,
        activity: PutActivity,
        failure: Option<u8>,
        delay: Duration,
    }

    impl ObservedStore {
        fn new(failure: Option<u8>, delay: Duration) -> Arc<Self> {
            Arc::new(Self {
                inner: object_store::memory::InMemory::new(),
                activity: PutActivity::default(),
                failure,
                delay,
            })
        }
    }

    impl std::fmt::Display for ObservedStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "observed test store")
        }
    }

    #[tonic::async_trait]
    impl ObjectStore for ObservedStore {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            use std::sync::atomic::Ordering::SeqCst;
            let tag = payload.iter().next().unwrap()[0];
            self.activity
                .payload_addresses
                .lock()
                .unwrap()
                .push(payload.iter().next().unwrap().as_ptr() as usize);
            if self.activity.retain_payloads.load(SeqCst) {
                self.activity
                    .retained_payloads
                    .lock()
                    .unwrap()
                    .push(payload.clone());
            }
            let active = self.activity.active.fetch_add(1, SeqCst) + 1;
            let _owned = ActivePut(&self.activity);
            self.activity.maximum.fetch_max(active, SeqCst);
            self.activity.started.fetch_add(1, SeqCst);
            // The first four uploads must overlap; a serial implementation
            // times out instead of silently satisfying a wall-clock threshold.
            while self.activity.started.load(SeqCst) < 4 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            while self.activity.hold.load(SeqCst)
                || (tag == 0 && self.activity.hold_first.load(SeqCst))
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            // In the failure fixture, all other PUTs remain owned and pending
            // until the failing PUT cancels the batch. A timed successful PUT
            // could legitimately free a slot before the failure is observed.
            if self.failure.is_some_and(|failure| failure != tag) {
                std::future::pending::<()>().await;
            }
            tokio::time::sleep(if tag == 0 { self.delay * 2 } else { self.delay }).await;
            if self.failure == Some(tag) {
                return Err(object_store::Error::Generic {
                    store: "observed test store",
                    source: std::io::Error::other("injected upload failure").into(),
                });
            }
            let result = self.inner.put_opts(location, payload, opts).await?;
            self.activity.finished.lock().unwrap().push(tag);
            Ok(result)
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, opts).await
        }

        fn delete_stream(
            &self,
            locations: TestStream<object_store::Result<ObjectPath>>,
        ) -> TestStream<object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> TestStream<object_store::Result<object_store::ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            opts: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    fn upload_batch(count: u8, length: usize) -> Vec<StagedChunk> {
        (0..count)
            .map(|tag| {
                StagedChunk::new(OperationId::new(format!("upload-{tag}")), vec![tag; length])
            })
            .collect()
    }

    #[test]
    fn s3_verifier_is_store_owned_fixed_gate_and_shutdown_joins_workers() {
        let store =
            S3ChunkStore::with_backend(options(), Arc::new(object_store::memory::InMemory::new()))
                .unwrap();
        let verifier = store.verifier.clone();
        let staged = StagedChunk::new(OperationId::new("closed-verifier"), b"closed".to_vec());

        assert_eq!(
            verifier.worker_count_for_test(),
            S3_HASH_WORKERS,
            "S3 verification must use the store-owned fixed worker pool"
        );
        assert!(
            verifier.max_active_hashes_for_test() <= MAX_ACTIVE_HASHES,
            "the verifier gate admits at most one full-object hash"
        );

        store.shutdown_verifier().unwrap();

        assert_eq!(
            verifier.joined_workers_for_test(),
            S3_HASH_WORKERS,
            "shutdown must explicitly join verifier worker threads"
        );
        assert_eq!(verifier.shutdowns_for_test(), 1);
        drop(store);
        assert_eq!(
            verifier
                .verify_get(&staged.chunk, staged.bytes().to_vec())
                .unwrap_err()
                .code(),
            afs_error::IO_UNAVAILABLE,
            "a closed verifier must not accept a new hash"
        );
    }

    #[test]
    fn verifier_gate_limits_concurrent_hashes_to_one_object() {
        let store =
            S3ChunkStore::with_backend(options(), Arc::new(object_store::memory::InMemory::new()))
                .unwrap();
        let verifier = store.verifier.clone();
        let first = StagedChunk::new(OperationId::new("gate-a"), vec![1; MAX_STAGED_CHUNK_BYTES]);
        let second = StagedChunk::new(OperationId::new("gate-b"), vec![2; MAX_STAGED_CHUNK_BYTES]);
        let barrier = Arc::new(std::sync::Barrier::new(3));

        let first_thread = {
            let verifier = verifier.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                verifier
                    .verify_get(&first.chunk, first.bytes().to_vec())
                    .unwrap();
            })
        };
        let second_thread = {
            let verifier = verifier.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                verifier
                    .verify_get(&second.chunk, second.bytes().to_vec())
                    .unwrap();
            })
        };
        barrier.wait();
        first_thread.join().unwrap();
        second_thread.join().unwrap();

        assert_eq!(verifier.get_hashes_for_test(), 2);
        assert_eq!(verifier.max_active_hashes_for_test(), MAX_ACTIVE_HASHES);
    }

    #[test]
    fn verifier_panic_is_reported_and_closes_future_hashes() {
        let store =
            S3ChunkStore::with_backend(options(), Arc::new(object_store::memory::InMemory::new()))
                .unwrap();
        let verifier = store.verifier.clone();
        let staged = StagedChunk::new(OperationId::new("panic"), b"panic".to_vec());
        verifier.panic_next_hash_for_test();

        assert_eq!(
            verifier
                .verify_get(&staged.chunk, staged.bytes().to_vec())
                .unwrap_err()
                .code(),
            afs_error::IO_UNAVAILABLE
        );
        assert_eq!(
            verifier
                .verify_get(&staged.chunk, staged.bytes().to_vec())
                .unwrap_err()
                .code(),
            afs_error::IO_UNAVAILABLE,
            "panic closes the verifier instead of reusing a poisoned pool"
        );
        assert_eq!(
            store.shutdown_verifier().unwrap_err().code(),
            afs_error::IO_UNAVAILABLE
        );
        assert_eq!(
            store.shutdown_verifier().unwrap_err().code(),
            afs_error::IO_UNAVAILABLE
        );
        assert_eq!(verifier.joined_workers_for_test(), S3_HASH_WORKERS);
        drop(store);
    }

    #[test]
    fn full_object_digest_covers_bytes_outside_requested_read_range() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let store = S3ChunkStore::with_backend(options(), backend.clone()).unwrap();
        let staged = StagedChunk::new(OperationId::new("tail-corruption"), b"01234567".to_vec());
        store.put(staged.clone()).unwrap();
        let key = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(backend.put(&ObjectPath::from(key), b"0123456x".to_vec().into()))
            .unwrap();
        let mut out = [0xa5; 3];

        assert_eq!(
            store
                .read_at(&staged.chunk.id, 0, &mut out)
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(out, [0xa5; 3]);
    }

    #[test]
    fn get_and_ready_load_share_the_store_verifier() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = options();
        config.cache = Some(S3ChunkCacheOptions {
            directory: dir.path().join("cache"),
            capacity_bytes: 8 * 1024 * 1024,
            pending_bytes: 4 * 1024 * 1024,
            cache_large_write: false,
        });
        let backend = Arc::new(object_store::memory::InMemory::new());
        let store = S3ChunkStore::with_backend(config, backend).unwrap();
        let verifier = store.verifier.clone();
        let staged = StagedChunk::new(
            OperationId::new("shared-verifier"),
            vec![7; MAX_STAGED_CHUNK_BYTES],
        );
        store.put(staged.clone()).unwrap();
        let key = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();

        assert_eq!(
            store
                .read_chunk(&staged.chunk, &source(&staged.chunk, key))
                .unwrap()
                .as_slice(),
            staged.bytes()
        );
        let cache = store.cache.as_ref().unwrap();
        let cache_key = CacheKey::new("filesystem-a", "generation-1", &staged.chunk);
        wait_for_ready_entry(cache, &cache_key);
        cache.shared.clear_resident_for_test();
        assert_eq!(
            cache.lookup(&staged.chunk).unwrap().as_slice(),
            staged.bytes()
        );

        assert_eq!(verifier.get_hashes_for_test(), 1);
        assert_eq!(verifier.ready_hashes_for_test(), 1);
    }

    #[test]
    fn upload_payload_keeps_staged_allocation_through_backend() {
        let backend = ObservedStore::new(None, Duration::from_millis(1));
        let store = S3ChunkStore::with_backend(options(), backend.clone()).unwrap();
        let staged = upload_batch(4, 1024);
        let mut expected = staged
            .iter()
            .map(|chunk| chunk.bytes().as_ptr() as usize)
            .collect::<Vec<_>>();
        assert_eq!(store.put_batch(staged).unwrap().len(), 4);
        let mut observed = backend.activity.payload_addresses.lock().unwrap().clone();
        expected.sort_unstable();
        observed.sort_unstable();
        assert_eq!(
            observed, expected,
            "PUT must retain the immutable producer allocation"
        );
    }

    #[test]
    fn upload_body_retry_clone_keeps_write_budget_until_last_owner() {
        use crate::node::chunk::DeferredWriteBudget;

        let budget = Arc::new(DeferredWriteBudget::default());
        let reservation = budget.reserve_normal(2048).unwrap();
        let mut staged = StagedChunk::new(OperationId::new("body-pin"), vec![7; 1024]);
        staged.retain_write_budget(std::slice::from_ref(&reservation));
        let body = upload_payload(&staged).unwrap();
        let retry_body = body.clone();
        assert_eq!(
            body.iter().next().unwrap().as_ptr(),
            staged.bytes().as_ptr()
        );
        drop(staged);
        drop(reservation);
        assert_eq!(budget.state.lock().unwrap().normal_bytes, 2048);
        drop(body);
        assert_eq!(budget.state.lock().unwrap().normal_bytes, 2048);
        drop(retry_body);
        assert_eq!(budget.state.lock().unwrap().normal_bytes, 0);
    }

    #[test]
    fn timed_out_upload_keeps_budget_while_transport_retains_body() {
        use crate::node::chunk::DeferredWriteBudget;
        use std::sync::atomic::Ordering::SeqCst;

        let budget = Arc::new(DeferredWriteBudget::default());
        let backend = ObservedStore::new(None, Duration::from_secs(5));
        backend.activity.retain_payloads.store(true, SeqCst);
        let mut config = options();
        config.request_timeout = Duration::from_millis(30);
        let store = S3ChunkStore::with_backend(config, backend.clone()).unwrap();
        let mut staged = upload_batch(4, 1024);
        for chunk in &mut staged {
            let reservation = budget.reserve_normal(2048).unwrap();
            chunk.retain_write_budget(std::slice::from_ref(&reservation));
        }
        assert_eq!(
            store.put_batch(staged).unwrap_err().code(),
            afs_error::IO_TIMEOUT
        );
        assert_eq!(backend.activity.active.load(SeqCst), 0);
        assert_eq!(backend.activity.retained_payloads.lock().unwrap().len(), 4);
        drop(store);
        assert_eq!(budget.state.lock().unwrap().normal_bytes, 8192);
        backend.activity.retained_payloads.lock().unwrap().clear();
        assert_eq!(budget.state.lock().unwrap().normal_bytes, 0);
    }

    #[test]
    fn abandoned_ticket_keeps_budget_while_worker_owns_payload() {
        use crate::node::chunk::DeferredWriteBudget;
        use std::sync::atomic::Ordering::SeqCst;

        let budget = Arc::new(DeferredWriteBudget::default());
        let backend = ObservedStore::new(None, Duration::from_millis(1));
        backend.activity.hold.store(true, SeqCst);
        let store = S3ChunkStore::with_backend(options(), backend.clone()).unwrap();
        let mut staged = upload_batch(4, 1024);
        for chunk in &mut staged {
            let reservation = budget.reserve_normal(2048).unwrap();
            chunk.retain_write_budget(std::slice::from_ref(&reservation));
        }
        drop(store.put_batch_deferred(staged).unwrap());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while backend.activity.started.load(SeqCst) < 4 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(backend.activity.started.load(SeqCst), 4);
        assert_eq!(budget.state.lock().unwrap().normal_bytes, 8192);
        backend.activity.hold.store(false, SeqCst);
        drop(store);
        // The in-memory fixture may itself retain the supplied immutable Bytes.
        // Release that final backend owner as well, rather than treating drain
        // or ticket cancellation as proof that no body remains alive.
        drop(backend);
        assert_eq!(budget.state.lock().unwrap().normal_bytes, 0);
    }

    #[test]
    fn valid_but_replaced_public_identity_does_not_replace_content_proof() {
        let store = S3ChunkStore {
            options: options(),
            sender: None,
            wake: Arc::new(tokio::sync::Notify::new()),
            worker: None,
            cache: None,
            verifier: S3ChunkVerifier::new().unwrap(),
        };
        let mut original = StagedChunk::new(OperationId::new("producer"), b"original".to_vec());
        let different = StagedChunk::new(OperationId::new("producer"), b"different".to_vec());
        original.chunk = different.chunk;
        validate_chunk(&original.chunk).unwrap();
        assert_eq!(
            store.put_batch(vec![original.clone()]).unwrap_err().code(),
            afs_error::IO_INVALID
        );
        assert_eq!(
            store
                .put_batch_deferred(vec![original])
                .err()
                .unwrap()
                .code(),
            afs_error::IO_INVALID
        );
    }

    #[test]
    fn parallel_uploads_overlap_and_preserve_receipt_identity() {
        use std::sync::atomic::Ordering::SeqCst;
        let backend = ObservedStore::new(None, Duration::from_millis(30));
        let store = S3ChunkStore::with_backend(options(), backend.clone()).unwrap();
        let staged = upload_batch(4, MAX_STAGED_CHUNK_BYTES);
        let receipts = store.put_batch(staged.clone()).unwrap();
        assert_eq!(backend.activity.maximum.load(SeqCst), 4);
        assert_eq!(backend.activity.active.load(SeqCst), 0);
        assert_ne!(backend.activity.finished.lock().unwrap()[0], 0);
        assert_eq!(receipts.len(), staged.len());
        for (receipt, input) in receipts.iter().zip(&staged) {
            assert_eq!(receipt.operation_id, input.operation_id);
            assert_eq!(receipt.chunk, input.chunk);
            assert!(receipt.durable_acks.is_empty());
            let external = receipt.external_commit.as_ref().unwrap();
            assert_eq!(
                external.object_key,
                external_object_key("filesystem-a", "generation-1", &input.chunk.id).unwrap()
            );
            assert_eq!(external.persisted_bytes, input.chunk.length);
            assert_eq!(external.verified_digest, input.chunk.content_digest);
            let mut out = vec![0; MAX_STAGED_CHUNK_BYTES];
            assert_eq!(
                store.read_at(&input.chunk.id, 0, &mut out).unwrap(),
                out.len()
            );
            assert_eq!(out, input.bytes());
        }
        drop(store);
        assert_eq!(backend.activity.active.load(SeqCst), 0);
    }

    #[test]
    fn upload_window_never_exceeds_four_with_more_than_one_window() {
        use std::sync::atomic::Ordering::SeqCst;
        let backend = ObservedStore::new(None, Duration::from_millis(20));
        let store = S3ChunkStore::with_backend(options(), backend.clone()).unwrap();
        let receipts = store.put_batch(upload_batch(9, 1024)).unwrap();
        assert_eq!(receipts.len(), 9);
        assert_eq!(backend.activity.started.load(SeqCst), 9);
        assert_eq!(backend.activity.maximum.load(SeqCst), 4);
        assert_eq!(backend.activity.active.load(SeqCst), 0);
        let mut finished = backend.activity.finished.lock().unwrap().clone();
        finished.sort_unstable();
        assert_eq!(finished, (0..9).collect::<Vec<_>>());
    }

    #[test]
    fn failed_parallel_batch_returns_no_receipts_and_drops_owned_uploads() {
        use std::sync::atomic::Ordering::SeqCst;
        let backend = ObservedStore::new(Some(1), Duration::from_millis(20));
        let store = S3ChunkStore::with_backend(options(), backend.clone()).unwrap();
        assert!(store.put_batch(upload_batch(9, 1024)).is_err());
        assert_eq!(backend.activity.active.load(SeqCst), 0);
        assert!(backend.activity.started.load(SeqCst) <= 4);
        drop(store);
        assert_eq!(backend.activity.active.load(SeqCst), 0);
    }

    #[test]
    fn deferred_batches_share_four_slots_and_replace_completed_uploads() {
        use std::sync::atomic::Ordering::SeqCst;
        let backend = ObservedStore::new(None, Duration::from_millis(10));
        backend.activity.hold_first.store(true, SeqCst);
        let store = S3ChunkStore::with_backend(options(), backend.clone()).unwrap();
        let mut tickets = Vec::new();
        for staged in upload_batch(12, 1024) {
            tickets.push(store.put_batch_deferred(vec![staged]).unwrap());
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while backend.activity.started.load(SeqCst) < 5 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            backend.activity.started.load(SeqCst) >= 5,
            "completed PUT must admit another chunk while the first PUT is held"
        );
        assert!(!backend.activity.finished.lock().unwrap().contains(&0));
        assert_eq!(backend.activity.maximum.load(SeqCst), MAX_UPLOADS);
        backend.activity.hold_first.store(false, SeqCst);
        // Drop must finish all submitted work, including uncollected tickets.
        drop(store);
        for (index, ticket) in tickets.into_iter().enumerate() {
            let receipts = ticket.wait().unwrap();
            assert_eq!(receipts.len(), 1);
            assert_eq!(
                receipts[0].operation_id,
                OperationId::new(format!("upload-{index}"))
            );
        }
        assert_eq!(backend.activity.started.load(SeqCst), 12);
        assert_eq!(backend.activity.active.load(SeqCst), 0);
    }

    #[test]
    fn abandoned_deferred_ticket_does_not_prevent_store_drain() {
        use std::sync::atomic::Ordering::SeqCst;
        let backend = ObservedStore::new(None, Duration::from_millis(5));
        let store = S3ChunkStore::with_backend(options(), backend.clone()).unwrap();
        for staged in upload_batch(8, 1024) {
            drop(store.put_batch_deferred(vec![staged]).unwrap());
        }
        drop(store);
        assert_eq!(backend.activity.started.load(SeqCst), 8);
        assert_eq!(backend.activity.active.load(SeqCst), 0);
        assert_eq!(backend.activity.maximum.load(SeqCst), MAX_UPLOADS);
    }

    #[test]
    fn upload_queue_backpressure_waits_without_a_false_storage_error() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let backend = ObservedStore::new(None, Duration::from_millis(1));
        backend.activity.hold.store(true, SeqCst);
        let store = Arc::new(S3ChunkStore::with_backend(options(), backend.clone()).unwrap());
        let submitted = Arc::new(AtomicUsize::new(0));
        let producer_store = store.clone();
        let producer_submitted = submitted.clone();
        let producer = thread::spawn(move || {
            let mut tickets = Vec::new();
            for staged in upload_batch((MAX_ACTIVE_REQUESTS + 2) as u8, 1024) {
                tickets.push(producer_store.put_batch_deferred(vec![staged]).unwrap());
                producer_submitted.fetch_add(1, SeqCst);
            }
            tickets
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while submitted.load(SeqCst) < MAX_ACTIVE_REQUESTS + 1
            && std::time::Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(submitted.load(SeqCst), MAX_ACTIVE_REQUESTS + 1);
        assert_eq!(backend.activity.maximum.load(SeqCst), MAX_UPLOADS);
        backend.activity.hold.store(false, SeqCst);
        for ticket in producer.join().unwrap() {
            assert_eq!(ticket.wait().unwrap().len(), 1);
        }
        drop(store);
        assert_eq!(backend.activity.active.load(SeqCst), 0);
    }

    #[test]
    fn whole_batch_timeout_cancels_all_parallel_uploads_before_join() {
        use std::sync::atomic::Ordering::SeqCst;
        let backend = ObservedStore::new(None, Duration::from_secs(5));
        let mut config = options();
        config.request_timeout = Duration::from_millis(30);
        let store = S3ChunkStore::with_backend(config, backend.clone()).unwrap();
        let start = std::time::Instant::now();
        assert_eq!(
            store.put_batch(upload_batch(9, 1024)).unwrap_err().code(),
            afs_error::IO_TIMEOUT
        );
        assert_eq!(backend.activity.started.load(SeqCst), 4);
        assert_eq!(backend.activity.active.load(SeqCst), 0);
        drop(store);
        assert!(start.elapsed() < Duration::from_secs(2));
        assert!(backend.activity.finished.lock().unwrap().is_empty());
    }

    #[test]
    fn immutable_id_binds_both_digest_and_length() {
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        assert_eq!(chunk_from_id(&staged.chunk.id).unwrap(), staged.chunk);
        for id in [
            "",
            "../key",
            "blake3-ff-8",
            "blake3-0000000000000000000000000000000000000000000000000000000000000000-4194305",
        ] {
            assert!(chunk_from_id(&ChunkId::new(id)).is_err());
        }
    }

    #[test]
    fn changed_content_never_passes_integrity_validation() {
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        assert!(verify_bytes(&staged.chunk, b"mutated!").is_err());
        assert!(verify_bytes(&staged.chunk, b"short").is_err());
        verify_bytes(&staged.chunk, staged.bytes()).unwrap();
    }

    #[test]
    fn empty_batch_needs_no_worker_and_nonempty_batches_keep_validation() {
        // Any bridge request would fail because this adapter has no worker.
        let store = S3ChunkStore {
            options: options(),
            sender: None,
            wake: Arc::new(tokio::sync::Notify::new()),
            worker: None,
            cache: None,
            verifier: S3ChunkVerifier::new().unwrap(),
        };
        assert!(store.put_batch(Vec::new()).unwrap().is_empty());

        let valid = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        assert_eq!(
            store
                .put_batch(vec![valid.clone(); MAX_BATCH + 1])
                .unwrap_err()
                .code(),
            afs_error::IO_INVALID
        );
        let mut invalid_chunk = valid;
        invalid_chunk.chunk.id = ChunkId::new("../invalid");
        assert_eq!(
            store.put_batch(vec![invalid_chunk]).unwrap_err().code(),
            afs_error::IO_INVALID
        );
    }

    #[test]
    fn external_receipt_and_partial_rmw_are_bound_to_the_same_object() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let store = S3ChunkStore::with_backend(options(), backend).unwrap();
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        let receipt = store.put(staged.clone()).unwrap();
        assert!(receipt.durable_acks.is_empty());
        assert_eq!(receipt.placement_revision, 0);
        let external = receipt.external_commit.unwrap();
        let expected =
            external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        assert_eq!(external.object_key, expected);
        assert_eq!(external.persisted_bytes, 8);
        let mut out = [0; 3];
        assert_eq!(store.read_at(&staged.chunk.id, 2, &mut out).unwrap(), 3);
        assert_eq!(&out, b"igi");
        assert_eq!(
            store
                .read_chunk(&staged.chunk, &source(&staged.chunk, expected))
                .unwrap()
                .as_slice(),
            b"original"
        );
        assert_eq!(
            store.capacity().unwrap_err().code(),
            afs_error::NODE_VFS_UNIMPLEMENTED
        );
    }

    #[test]
    fn untrusted_store_generation_or_key_is_rejected_before_get() {
        let store =
            S3ChunkStore::with_backend(options(), Arc::new(object_store::memory::InMemory::new()))
                .unwrap();
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        let key = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        let authorized = source(&staged.chunk, key);
        for variant in 0..3 {
            let mut candidate = authorized.clone();
            if let CopyLocation::External {
                store_id,
                object_key,
                object_revision,
            } = &mut candidate.location
            {
                match variant {
                    0 => *store_id = "another-filesystem".into(),
                    1 => *object_revision = "another-generation".into(),
                    _ => object_key.push_str("-forged"),
                }
            }
            // Empty backend would yield NotFound if the binding check were bypassed.
            assert_eq!(
                store
                    .read_chunk(&staged.chunk, &candidate)
                    .unwrap_err()
                    .code(),
                afs_error::IO_INVALID
            );
        }
    }

    #[test]
    fn damaged_s3_bytes_do_not_publish_partial_rmw_output() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let store = S3ChunkStore::with_backend(options(), backend.clone()).unwrap();
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        store.put(staged.clone()).unwrap();
        let key = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(backend.put(&ObjectPath::from(key), b"mutated!".to_vec().into()))
            .unwrap();
        let mut out = [0xa5; 3];
        assert_eq!(
            store
                .read_at(&staged.chunk.id, 2, &mut out)
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(out, [0xa5; 3]);
    }

    #[test]
    fn cache_off_remains_the_default() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let store = S3ChunkStore::with_backend(options(), backend.clone()).unwrap();
        assert!(store.cache.is_none());
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        store.put(staged.clone()).unwrap();
        let key = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(backend.put(&ObjectPath::from(key), b"mutated!".to_vec().into()))
            .unwrap();
        let mut out = [0; 8];
        assert_eq!(
            store
                .read_at(&staged.chunk.id, 0, &mut out)
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
    }

    #[test]
    fn read_through_cache_serves_verified_pending_or_ready_bytes() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let mut config = options();
        config.cache = Some(cache_options(dir.path(), 1024 * 1024));
        let store = S3ChunkStore::with_backend(config, backend.clone()).unwrap();
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        store.put(staged.clone()).unwrap();
        let mut out = [0; 8];
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 8);
        assert_eq!(&out, b"original");

        let key = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(backend.put(&ObjectPath::from(key), b"mutated!".to_vec().into()))
            .unwrap();
        out.fill(0);
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 8);
        assert_eq!(&out, b"original");
    }

    #[test]
    fn ready_cache_file_recovers_after_store_restart() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        {
            let mut config = options();
            config.cache = Some(cache_options(dir.path(), 1024 * 1024));
            let store = S3ChunkStore::with_backend(config, backend.clone()).unwrap();
            store.put(staged.clone()).unwrap();
            let mut out = [0; 8];
            assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 8);
            wait_for_file(&cache_path(dir.path(), "generation-1", &staged.chunk));
        }

        let key = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(backend.put(&ObjectPath::from(key), b"mutated!".to_vec().into()))
            .unwrap();
        let mut config = options();
        config.cache = Some(cache_options(dir.path(), 1024 * 1024));
        let store = S3ChunkStore::with_backend(config, backend).unwrap();
        let mut out = [0; 8];
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 8);
        assert_eq!(&out, b"original");
    }

    #[test]
    fn corrupt_ready_cache_is_removed_and_refetched() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let mut config = options();
        config.cache = Some(cache_options(dir.path(), 1024 * 1024));
        let store = S3ChunkStore::with_backend(config, backend).unwrap();
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        store.put(staged.clone()).unwrap();
        let mut out = [0; 8];
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 8);
        let path = cache_path(dir.path(), "generation-1", &staged.chunk);
        wait_for_file(&path);
        store
            .cache
            .as_ref()
            .unwrap()
            .shared
            .clear_resident_for_test();
        std::fs::write(&path, b"mutated!").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        out.fill(0);
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 8);
        assert_eq!(&out, b"original");
        wait_for_file_bytes(&path, b"original");
    }

    #[test]
    fn failed_ready_removal_keeps_conservative_cache_budget() {
        let dir = tempfile::tempdir().unwrap();
        let blocked = StagedChunk::new(OperationId::new("blocked"), b"blocked".to_vec());
        let extra = StagedChunk::new(OperationId::new("extra"), b"extra".to_vec());
        let shared = CacheShared {
            options: cache_options(dir.path(), blocked.chunk.length),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        shared.initialize_directory().unwrap();
        let key = CacheKey::new("filesystem-a", "generation-1", &blocked.chunk);
        std::fs::create_dir(shared.path_for(&key)).unwrap();
        {
            let mut inner = shared.inner.lock().unwrap();
            inner.entries.insert(
                key.clone(),
                CacheEntry::Ready {
                    length: blocked.chunk.length,
                },
            );
            inner.ready_bytes = blocked.chunk.length;
            touch_lru(&mut inner.lru, &key);
        }

        assert!(shared.lookup(&blocked.chunk).is_none());
        {
            let inner = shared.inner.lock().unwrap();
            assert!(matches!(
                inner.entries.get(&key),
                Some(CacheEntry::Ready { .. })
            ));
            assert_eq!(inner.ready_bytes, blocked.chunk.length);
        }
        shared.schedule_from_read(&extra.chunk, verified(&extra));
        assert!(shared.lookup(&extra.chunk).is_none());
    }

    #[test]
    fn recovery_rejects_uncleanable_cache_files_that_would_break_budget() {
        let dir = tempfile::tempdir().unwrap();
        let shared = CacheShared {
            options: cache_options(dir.path(), 1024),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        shared.initialize_directory().unwrap();
        std::fs::create_dir(shared.namespace_dir().join(".tmp-leftover")).unwrap();
        assert!(shared.recover_ready_entries().is_err());

        let dir = tempfile::tempdir().unwrap();
        let staged = StagedChunk::new(OperationId::new("damaged"), b"damaged".to_vec());
        let shared = CacheShared {
            options: cache_options(dir.path(), 1024),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        shared.initialize_directory().unwrap();
        std::fs::create_dir(shared.path_for(&CacheKey::new(
            "filesystem-a",
            "generation-1",
            &staged.chunk,
        )))
        .unwrap();
        assert!(shared.recover_ready_entries().is_err());
    }

    #[test]
    fn cache_capacity_evicts_ready_entries_without_failing_reads() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let first = StagedChunk::new(OperationId::new("write-1"), b"first".to_vec());
        let second = StagedChunk::new(OperationId::new("write-2"), b"second".to_vec());
        let mut config = options();
        config.cache = Some(cache_options(dir.path(), 8));
        let store = S3ChunkStore::with_backend(config, backend).unwrap();
        store.put(first.clone()).unwrap();
        let mut out = [0; 6];
        assert_eq!(store.read_at(&first.chunk.id, 0, &mut out[..5]).unwrap(), 5);
        wait_for_file(&cache_path(dir.path(), "generation-1", &first.chunk));
        store.put(second.clone()).unwrap();
        assert_eq!(store.read_at(&second.chunk.id, 0, &mut out).unwrap(), 6);
        wait_for_file(&cache_path(dir.path(), "generation-1", &second.chunk));
        assert!(!cache_path(dir.path(), "generation-1", &first.chunk).exists());
    }

    #[test]
    fn store_generation_namespaces_never_share_cached_content() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        {
            let mut config = options();
            config.cache = Some(cache_options(dir.path(), 1024 * 1024));
            let store = S3ChunkStore::with_backend(config, backend.clone()).unwrap();
            store.put(staged.clone()).unwrap();
            let mut out = [0; 8];
            assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 8);
            wait_for_file(&cache_path(dir.path(), "generation-1", &staged.chunk));
        }
        let key = external_object_key("filesystem-a", "generation-2", &staged.chunk.id).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(backend.put(&ObjectPath::from(key), b"mutated!".to_vec().into()))
            .unwrap();
        let mut config = options();
        config.generation = "generation-2".into();
        config.cache = Some(cache_options(dir.path(), 1024 * 1024));
        let store = S3ChunkStore::with_backend(config, backend).unwrap();
        let mut out = [0; 8];
        assert_eq!(
            store
                .read_at(&staged.chunk.id, 0, &mut out)
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
    }

    #[test]
    fn write_fill_is_partial_by_default_and_large_write_opt_in() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let mut config = options();
        config.cache = Some(cache_options(dir.path(), 1024 * 1024));
        let store = S3ChunkStore::with_backend(config, backend.clone()).unwrap();
        let partial = StagedChunk::new(OperationId::new("partial-write"), b"small".to_vec());
        store.put(partial.clone()).unwrap();
        wait_for_file(&cache_path(dir.path(), "generation-1", &partial.chunk));
        let staged = StagedChunk::new(OperationId::new("write"), vec![7; MAX_STAGED_CHUNK_BYTES]);
        store.put(staged.clone()).unwrap();
        thread::sleep(Duration::from_millis(50));
        assert!(!cache_path(dir.path(), "generation-1", &staged.chunk).exists());
        drop(store);

        let mut config = options();
        let mut cache = cache_options(dir.path(), MAX_STAGED_CHUNK_BYTES as u64 + 1024);
        cache.cache_large_write = true;
        config.cache = Some(cache);
        let store = S3ChunkStore::with_backend(config, backend).unwrap();
        store.put(staged.clone()).unwrap();
        wait_for_file(&cache_path(dir.path(), "generation-1", &staged.chunk));
    }

    #[test]
    fn pending_cache_entry_is_served_before_disk_fill_runs() {
        let staged = StagedChunk::new(OperationId::new("pending"), b"pending-bytes".to_vec());
        let dir = tempfile::tempdir().unwrap();
        let shared = CacheShared {
            options: cache_options(dir.path(), 1024 * 1024),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        shared.schedule_from_read(&staged.chunk, verified(&staged));

        let first = shared.lookup(&staged.chunk).unwrap();
        let second = shared.lookup(&staged.chunk).unwrap();
        assert_eq!(
            first.as_slice().as_ptr(),
            second.as_slice().as_ptr(),
            "pending hits share immutable verified bytes"
        );
        assert_eq!(first.as_slice(), staged.bytes());
        let inner = shared.inner.lock().unwrap();
        assert_eq!(inner.jobs.len(), 1);
        assert_eq!(inner.pending_bytes, staged.chunk.length);
        assert_eq!(inner.temp_bytes, staged.chunk.length);
    }

    #[test]
    fn read_fill_keeps_the_verified_buffer_identity() {
        let staged = StagedChunk::new(OperationId::new("read-fill"), b"read-fill-bytes".to_vec());
        let verified =
            VerifiedExternalChunk::verify(&staged.chunk, staged.bytes().to_vec()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let shared = CacheShared {
            options: cache_options(dir.path(), 1024 * 1024),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };

        shared.schedule_from_read(&staged.chunk, verified.clone());

        let pending = shared.lookup(&staged.chunk).unwrap();
        assert_eq!(
            pending.as_slice().as_ptr(),
            verified.as_slice().as_ptr(),
            "read cache admission must reuse the already verified immutable buffer"
        );
        let inner = shared.inner.lock().unwrap();
        let job = inner.jobs.front().expect("accepted read fill job");
        assert_eq!(
            job.bytes.as_slice().as_ptr(),
            verified.as_slice().as_ptr(),
            "cache worker must receive the same verified buffer"
        );
    }

    #[test]
    fn read_fill_rejects_a_verified_buffer_for_another_chunk_identity() {
        let accepted = StagedChunk::new(OperationId::new("accepted"), b"same-length-a".to_vec());
        let mut mutated_identity = accepted.chunk.clone();
        mutated_identity.length += 1;
        let other =
            VerifiedExternalChunk::verify(&accepted.chunk, accepted.bytes().to_vec()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let shared = CacheShared {
            options: cache_options(dir.path(), 1024 * 1024),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };

        shared.schedule_from_read(&mutated_identity, other);

        let inner = shared.inner.lock().unwrap();
        assert!(inner.entries.is_empty());
        assert!(inner.jobs.is_empty());
        assert_eq!(inner.pending_bytes, 0);
        assert_eq!(inner.temp_bytes, 0);
    }

    #[test]
    fn stopped_read_fill_does_not_retain_the_verified_buffer() {
        let staged = StagedChunk::new(OperationId::new("stopped-read"), b"stopped".to_vec());
        let dir = tempfile::tempdir().unwrap();
        let shared = CacheShared {
            options: cache_options(dir.path(), 1024 * 1024),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner {
                stopped: true,
                ..CacheInner::default()
            }),
            wake: Condvar::new(),
        };

        shared.schedule_from_read(&staged.chunk, verified(&staged));

        let inner = shared.inner.lock().unwrap();
        assert!(inner.entries.is_empty());
        assert!(inner.jobs.is_empty());
        assert_eq!(inner.pending_bytes, 0);
        assert_eq!(inner.temp_bytes, 0);
    }

    #[test]
    fn stopped_ready_lookup_does_not_repopulate_resident_slot() {
        let staged = StagedChunk::new(OperationId::new("stopped-ready"), b"stopped".to_vec());
        let dir = tempfile::tempdir().unwrap();
        let shared = CacheShared {
            options: cache_options(dir.path(), 1024 * 1024),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        shared.initialize_directory().unwrap();
        let key = CacheKey::new("filesystem-a", "generation-1", &staged.chunk);
        std::fs::write(shared.path_for(&key), staged.bytes()).unwrap();
        {
            let mut inner = shared.inner.lock().unwrap();
            inner.stopped = true;
            inner.entries.insert(
                key,
                CacheEntry::Ready {
                    length: staged.chunk.length,
                },
            );
            inner.ready_bytes = staged.chunk.length;
        }

        assert!(shared.lookup(&staged.chunk).is_none());
        let inner = shared.inner.lock().unwrap();
        assert!(inner.resident.is_none());
        assert_eq!(inner.pending_bytes, 0);
    }

    #[test]
    fn resident_slot_reuses_verified_object_until_explicit_eviction() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let mut config = options();
        config.cache = Some(cache_options(dir.path(), 1024 * 1024));
        let store = S3ChunkStore::with_backend(config, backend.clone()).unwrap();
        let staged = StagedChunk::new(OperationId::new("resident"), b"original".to_vec());
        store.put(staged.clone()).unwrap();
        let mut out = [0; 8];
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 8);
        assert_eq!(&out, b"original");
        let path = cache_path(dir.path(), "generation-1", &staged.chunk);
        wait_for_file(&path);
        let resident = store
            .cache
            .as_ref()
            .unwrap()
            .shared
            .lookup(&staged.chunk)
            .unwrap();
        assert_eq!(resident.as_slice(), b"original");
        std::fs::write(&path, b"mutated!").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

        out.fill(0);
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 8);
        assert_eq!(
            &out, b"original",
            "trusted resident memory may satisfy an adjacent callback"
        );

        store
            .cache
            .as_ref()
            .unwrap()
            .shared
            .clear_resident_for_test();
        out.fill(0);
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 8);
        assert_eq!(&out, b"original");
        wait_for_file_bytes(&path, b"original");
    }

    #[test]
    fn resident_slot_requires_full_chunk_identity_not_only_cache_key() {
        let staged = StagedChunk::new(OperationId::new("identity"), b"identity".to_vec());
        let mut wrong_chunk = staged.chunk.clone();
        wrong_chunk.length += 1;
        let dir = tempfile::tempdir().unwrap();
        let shared = CacheShared {
            options: cache_options(dir.path(), 1024 * 1024),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        let key = CacheKey::new("filesystem-a", "generation-1", &staged.chunk);
        {
            let mut inner = shared.inner.lock().unwrap();
            inner.entries.insert(
                key.clone(),
                CacheEntry::Ready {
                    length: staged.chunk.length,
                },
            );
            assert!(shared.install_resident_locked(
                &mut inner,
                key,
                &staged.chunk,
                verified(&staged),
            ));
        }

        assert!(shared.lookup(&wrong_chunk).is_none());
        let inner = shared.inner.lock().unwrap();
        assert!(inner.resident.is_none());
    }

    #[test]
    fn resident_slot_counts_external_pins_before_replacement() {
        let first = StagedChunk::new(OperationId::new("first-pin"), b"first".to_vec());
        let second = StagedChunk::new(OperationId::new("second-pin"), b"other".to_vec());
        let dir = tempfile::tempdir().unwrap();
        let mut options = cache_options(dir.path(), first.chunk.length * 2);
        options.pending_bytes = first.chunk.length;
        let shared = CacheShared {
            options,
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        let first_key = CacheKey::new("filesystem-a", "generation-1", &first.chunk);
        let second_key = CacheKey::new("filesystem-a", "generation-1", &second.chunk);
        {
            let mut inner = shared.inner.lock().unwrap();
            inner.entries.insert(
                first_key.clone(),
                CacheEntry::Ready {
                    length: first.chunk.length,
                },
            );
            assert!(shared.install_resident_locked(
                &mut inner,
                first_key,
                &first.chunk,
                verified(&first),
            ));
        }
        let first_pin = shared.lookup(&first.chunk).unwrap();
        {
            let mut inner = shared.inner.lock().unwrap();
            assert!(!shared.install_resident_locked(
                &mut inner,
                second_key.clone(),
                &second.chunk,
                verified(&second),
            ));
            assert_eq!(inner.pending_bytes, first.chunk.length);
        }
        drop(first_pin);
        {
            let mut inner = shared.inner.lock().unwrap();
            assert!(shared.install_resident_locked(
                &mut inner,
                second_key,
                &second.chunk,
                verified(&second),
            ));
            assert_eq!(inner.pending_bytes, second.chunk.length);
        }
    }

    #[test]
    fn verified_allocation_budget_counts_vec_capacity_not_object_length() {
        let staged = StagedChunk::new(OperationId::new("capacity"), b"bytes".to_vec());
        let dir = tempfile::tempdir().unwrap();
        let mut options = cache_options(dir.path(), 1024 * 1024);
        options.pending_bytes = staged.chunk.length;
        let shared = CacheShared {
            options,
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        let oversized = verified_with_capacity(&staged, staged.chunk.length as usize + 16);
        assert!(oversized.allocation_bytes() > staged.chunk.length);

        shared.schedule_from_read(&staged.chunk, oversized);

        let inner = shared.inner.lock().unwrap();
        assert!(inner.entries.is_empty());
        assert!(inner.jobs.is_empty());
        assert!(inner.resident.is_none());
        assert_eq!(inner.pending_bytes, 0);
        assert_eq!(inner.temp_bytes, 0);
    }

    #[test]
    fn worker_completion_keeps_caller_pin_charged_until_release() {
        let staged = StagedChunk::new(OperationId::new("worker-pin"), b"worker-pin".to_vec());
        let dir = tempfile::tempdir().unwrap();
        let cache = S3ChunkCache::new(
            cache_options(dir.path(), 1024 * 1024),
            "filesystem-a",
            "generation-1",
            S3ChunkVerifier::new().unwrap(),
        )
        .unwrap();
        let key = CacheKey::new("filesystem-a", "generation-1", &staged.chunk);

        cache.schedule_from_read(&staged.chunk, verified(&staged));
        let held = cache.lookup(&staged.chunk).unwrap();
        wait_for_file(&cache.shared.path_for(&key));
        wait_for_ready_entry(&cache, &key);

        {
            let inner = cache.shared.inner.lock().unwrap();
            assert!(matches!(
                inner.entries.get(&key),
                Some(CacheEntry::Ready { .. })
            ));
            assert_eq!(inner.tracked_allocations.len(), 1);
            assert!(inner.pending_bytes >= held.allocation_bytes());
        }
        cache.shared.clear_resident_for_test();
        {
            let inner = cache.shared.inner.lock().unwrap();
            assert!(inner.pending_bytes >= held.allocation_bytes());
        }
        drop(held);
        {
            let mut inner = cache.shared.inner.lock().unwrap();
            prune_tracked_allocations(&mut inner);
            assert_eq!(inner.pending_bytes, 0);
            assert!(inner.tracked_allocations.is_empty());
        }
    }

    #[test]
    fn resident_slot_counts_a_concurrent_reader_pin_before_replacement() {
        let first = StagedChunk::new(OperationId::new("first-concurrent"), b"first".to_vec());
        let second = StagedChunk::new(OperationId::new("second-concurrent"), b"other".to_vec());
        let dir = tempfile::tempdir().unwrap();
        let mut options = cache_options(dir.path(), first.chunk.length * 2);
        options.pending_bytes = first.chunk.length;
        let shared = Arc::new(CacheShared {
            options,
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        });
        let first_key = CacheKey::new("filesystem-a", "generation-1", &first.chunk);
        let second_key = CacheKey::new("filesystem-a", "generation-1", &second.chunk);
        {
            let mut inner = shared.inner.lock().unwrap();
            inner.entries.insert(
                first_key.clone(),
                CacheEntry::Ready {
                    length: first.chunk.length,
                },
            );
            assert!(shared.install_resident_locked(
                &mut inner,
                first_key,
                &first.chunk,
                verified(&first),
            ));
        }
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let reader_shared = shared.clone();
        let reader_chunk = first.chunk.clone();
        let reader_barrier = barrier.clone();
        let reader = thread::spawn(move || {
            let pin = reader_shared.lookup(&reader_chunk).unwrap();
            reader_barrier.wait();
            reader_barrier.wait();
            drop(pin);
        });
        barrier.wait();
        {
            let mut inner = shared.inner.lock().unwrap();
            assert!(!shared.install_resident_locked(
                &mut inner,
                second_key.clone(),
                &second.chunk,
                verified(&second),
            ));
            assert_eq!(inner.pending_bytes, first.chunk.length);
        }
        barrier.wait();
        reader.join().unwrap();
        {
            let mut inner = shared.inner.lock().unwrap();
            assert!(shared.install_resident_locked(
                &mut inner,
                second_key,
                &second.chunk,
                verified(&second),
            ));
            assert_eq!(inner.pending_bytes, second.chunk.length);
        }
    }

    #[test]
    fn pending_budget_and_queue_bounds_drop_extra_fills() {
        let first = StagedChunk::new(OperationId::new("first"), b"12345".to_vec());
        let second = StagedChunk::new(OperationId::new("second"), b"abcde".to_vec());
        let dir = tempfile::tempdir().unwrap();
        let mut options = cache_options(dir.path(), first.chunk.length * 2);
        options.pending_bytes = first.chunk.length;
        let shared = CacheShared {
            options,
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        shared.schedule_from_read(&first.chunk, verified(&first));
        shared.schedule_from_read(&second.chunk, verified(&second));
        assert!(shared.lookup(&first.chunk).is_some());
        assert!(shared.lookup(&second.chunk).is_none());

        let queued = StagedChunk::new(OperationId::new("queued"), b"q".to_vec());
        let dropped = StagedChunk::new(OperationId::new("dropped"), b"d".to_vec());
        let dir = tempfile::tempdir().unwrap();
        let shared = CacheShared {
            options: cache_options(dir.path(), 1024 * 1024),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner {
                active_jobs: 1,
                ..CacheInner::default()
            }),
            wake: Condvar::new(),
        };
        {
            let mut inner = shared.inner.lock().unwrap();
            for index in 1..MAX_CACHE_JOBS {
                let staged = StagedChunk::new(
                    OperationId::new(format!("queued-{index}")),
                    vec![index as u8],
                );
                inner.jobs.push_back(CacheJob {
                    key: CacheKey::new("filesystem-a", "generation-1", &staged.chunk),
                    chunk: staged.chunk.clone(),
                    bytes: VerifiedExternalChunk::verify(&staged.chunk, staged.bytes().to_vec())
                        .unwrap(),
                    length: 1,
                });
            }
        }
        shared.schedule_from_read(&queued.chunk, verified(&queued));
        assert!(shared.lookup(&queued.chunk).is_none());
        shared.schedule_from_read(&dropped.chunk, verified(&dropped));
        assert!(shared.lookup(&dropped.chunk).is_none());

        let entry = StagedChunk::new(OperationId::new("entry"), b"entry".to_vec());
        let overflow = StagedChunk::new(OperationId::new("overflow"), b"overflow".to_vec());
        let dir = tempfile::tempdir().unwrap();
        let shared = CacheShared {
            options: cache_options(dir.path(), 1024 * 1024),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        {
            let mut inner = shared.inner.lock().unwrap();
            for index in 0..MAX_CACHE_ENTRIES {
                let staged = StagedChunk::new(
                    OperationId::new(format!("entry-{index}")),
                    index.to_le_bytes().to_vec(),
                );
                inner.entries.insert(
                    CacheKey::new("filesystem-a", "generation-1", &staged.chunk),
                    CacheEntry::Pending {
                        bytes: VerifiedExternalChunk::verify(
                            &staged.chunk,
                            staged.bytes().to_vec(),
                        )
                        .unwrap(),
                    },
                );
            }
        }
        shared.schedule_from_read(&entry.chunk, verified(&entry));
        shared.schedule_from_read(&overflow.chunk, verified(&overflow));
        assert!(shared.lookup(&entry.chunk).is_none());
        assert!(shared.lookup(&overflow.chunk).is_none());
    }

    #[test]
    fn cache_io_failure_does_not_change_s3_put_or_get_results() {
        let backend = Arc::new(object_store::memory::InMemory::new());
        let dir = tempfile::tempdir().unwrap();
        let mut config = options();
        let mut cache = cache_options(dir.path(), 1024 * 1024);
        cache.cache_large_write = true;
        config.cache = Some(cache);
        let store = S3ChunkStore::with_backend(config, backend.clone()).unwrap();
        let staged = StagedChunk::new(OperationId::new("write"), b"original".to_vec());
        let key = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        let cache_entry = cache_path(dir.path(), "generation-1", &staged.chunk);
        std::fs::create_dir(&cache_entry).unwrap();

        let receipt = store.put(staged.clone()).unwrap();
        assert!(receipt.external_commit.is_some());
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while store
            .cache
            .as_ref()
            .unwrap()
            .lookup(&staged.chunk)
            .is_some()
            && std::time::Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            store
                .cache
                .as_ref()
                .unwrap()
                .lookup(&staged.chunk)
                .is_none()
        );
        let mut out = [0; 8];
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 8);
        assert_eq!(&out, b"original");

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(backend.put(&ObjectPath::from(key.clone()), b"original".to_vec().into()))
            .unwrap();
        assert_eq!(
            store
                .read_chunk(&staged.chunk, &source(&staged.chunk, key))
                .unwrap()
                .as_slice(),
            staged.bytes()
        );
    }

    #[test]
    fn cache_on_failed_put_still_returns_no_external_receipt() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut config = options();
        config.endpoint = format!("http://{}", listener.local_addr().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials.json");
        std::fs::write(
            &credentials,
            br#"{"access_key_id":"test","secret_access_key":"test-secret"}"#,
        )
        .unwrap();
        std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600)).unwrap();
        config.credentials = S3CredentialsSource::File(credentials);
        let mut cache = cache_options(dir.path(), 1024 * 1024);
        cache.cache_large_write = true;
        config.cache = Some(cache);
        let server = thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            let mut connection = loop {
                match listener.accept() {
                    Ok((connection, _)) => break connection,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(error) => panic!("S3 fixture accept failed: {error}"),
                }
            };
            connection
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                connection.read_exact(&mut byte).unwrap();
                header.push(byte[0]);
                assert!(header.len() < 16 * 1024);
            }
            connection
                .write_all(
                    b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        });
        let store = S3ChunkStore::new(config).unwrap();
        let staged = StagedChunk::new(OperationId::new("failure"), b"original".to_vec());
        let result = store.put(staged.clone());
        assert!(result.is_err());
        assert!(
            store
                .cache
                .as_ref()
                .unwrap()
                .lookup(&staged.chunk)
                .is_some()
        );
        server.join().unwrap();
    }

    #[test]
    fn s3_client_reuses_connection_between_batches_and_closes_it_on_drop() {
        use std::io::{BufRead, BufReader};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut config = options();
        config.endpoint = format!("http://{}", listener.local_addr().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials.json");
        std::fs::write(
            &credentials,
            br#"{"access_key_id":"test","secret_access_key":"test-secret"}"#,
        )
        .unwrap();
        std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600)).unwrap();
        config.credentials = S3CredentialsSource::File(credentials);
        let server = thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            let connection = loop {
                match listener.accept() {
                    Ok((connection, _)) => break connection,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(1))
                    }
                    Err(error) => panic!("S3 fixture accept failed: {error}"),
                }
            };
            connection
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut connection = BufReader::new(connection);
            for _ in 0..2 {
                let mut header = String::new();
                connection.read_line(&mut header).unwrap();
                assert!(header.starts_with("PUT /chunks/afs/v1/"));
                let mut length = None;
                loop {
                    header.clear();
                    connection.read_line(&mut header).unwrap();
                    if header == "\r\n" {
                        break;
                    }
                    if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:")
                    {
                        length = Some(value.trim().parse::<usize>().unwrap());
                    }
                    assert!(!header.is_empty(), "client closed a reusable connection");
                }
                let mut body = vec![0; length.unwrap()];
                connection.read_exact(&mut body).unwrap();
                assert_eq!(&body, b"original");
                connection.get_mut().write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"immutable\"\r\nConnection: keep-alive\r\n\r\n").unwrap();
            }
            // The same socket carried both requests; dropping the adapter must
            // also release the worker-owned client and its idle connection.
            assert_eq!(connection.read(&mut [0]).unwrap(), 0);
            assert!(
                matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
            );
        });
        let store = S3ChunkStore::new(config).unwrap();
        for index in 0..2 {
            store
                .put(StagedChunk::new(
                    OperationId::new(format!("reuse-{index}")),
                    b"original".to_vec(),
                ))
                .unwrap();
        }
        drop(store);
        server.join().unwrap();
    }

    #[test]
    fn https_s3_client_constructs_with_explicit_tls_provider() {
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials.json");
        std::fs::write(
            &credentials,
            br#"{"access_key_id":"test","secret_access_key":"test-secret"}"#,
        )
        .unwrap();
        std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut config = options();
        config.endpoint = "https://127.0.0.1:1".into();
        config.allow_http = false;
        config.credentials = S3CredentialsSource::File(credentials);

        // Client construction initializes rustls; no endpoint connection is made.
        let store = S3ChunkStore::new(config).unwrap();
        drop(store);
    }

    #[test]
    fn real_s3_put_failure_returns_no_external_receipt() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut config = options();
        config.endpoint = format!("http://{}", listener.local_addr().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let credentials = dir.path().join("credentials.json");
        std::fs::write(
            &credentials,
            br#"{"access_key_id":"test","secret_access_key":"test-secret"}"#,
        )
        .unwrap();
        std::fs::set_permissions(&credentials, std::fs::Permissions::from_mode(0o600)).unwrap();
        config.credentials = S3CredentialsSource::File(credentials);
        let server = thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            let mut connection = loop {
                match listener.accept() {
                    Ok((connection, _)) => break connection,
                    Err(error)
                        if error.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(5))
                    }
                    Err(error) => panic!("S3 fixture accept failed: {error}"),
                }
            };
            connection
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                connection.read_exact(&mut byte).unwrap();
                header.push(byte[0]);
                assert!(header.len() < 16 * 1024);
            }
            let header = String::from_utf8(header).unwrap();
            assert!(header.starts_with("PUT /chunks/afs/v1/filesystem-a/generation-1/chunks/"));
            assert!(!header.contains("If-Match") && !header.contains("If-None-Match"));
            connection.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
        });
        let store = S3ChunkStore::new(config).unwrap();
        let result = store.put(StagedChunk::new(
            OperationId::new("failure"),
            b"original".to_vec(),
        ));
        assert!(result.is_err());
        server.join().unwrap();
    }

    #[test]
    fn credentials_file_requires_private_permissions_and_hides_secret_parse_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        std::fs::write(&path, b"sensitive-invalid-json").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(load_credentials(&S3CredentialsSource::File(path.clone())).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        match load_credentials(&S3CredentialsSource::File(path)) {
            Err(error) => assert!(!error.message().contains("sensitive")),
            Ok(_) => panic!("malformed credentials accepted"),
        }
    }

    #[test]
    fn bounded_batch_accepts_a_64mib_file_without_node_acknowledgements() {
        let mut config = options();
        config.request_timeout = Duration::from_secs(30);
        let store =
            S3ChunkStore::with_backend(config, Arc::new(object_store::memory::InMemory::new()))
                .unwrap();
        let staged = (0..16)
            .map(|index| {
                StagedChunk::new(
                    OperationId::new(format!("write-{index}")),
                    vec![index; MAX_STAGED_CHUNK_BYTES],
                )
            })
            .collect();
        let receipts = store.put_batch(staged).unwrap();
        assert_eq!(receipts.len(), 16);
        assert!(receipts.iter().all(|receipt| receipt.durable_acks.is_empty() && receipt.external_commit.is_some()));
        assert_eq!(
            receipts
                .iter()
                .map(|receipt| receipt.chunk.length)
                .sum::<u64>(),
            64 * 1024 * 1024
        );
    }

    #[test]
    fn verified_full_object_read_fills_disposable_cache_without_new_authority() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = options();
        config.cache = Some(S3ChunkCacheOptions {
            directory: dir.path().join("cache"),
            capacity_bytes: 8 * 1024 * 1024,
            pending_bytes: 4 * 1024 * 1024,
            cache_large_write: false,
        });
        let backend = Arc::new(object_store::memory::InMemory::new());
        let store = S3ChunkStore::with_backend(config, backend.clone()).unwrap();
        let staged = StagedChunk::new(
            OperationId::new("cache-fill"),
            vec![7; MAX_STAGED_CHUNK_BYTES],
        );
        store.put(staged.clone()).unwrap();
        let key = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
        assert!(
            store
                .cache
                .as_ref()
                .unwrap()
                .lookup(&staged.chunk)
                .is_none()
        );

        assert_eq!(
            store
                .read_chunk(&staged.chunk, &source(&staged.chunk, key.clone()))
                .unwrap()
                .as_slice(),
            staged.bytes()
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while store
            .cache
            .as_ref()
            .unwrap()
            .lookup(&staged.chunk)
            .is_none()
            && std::time::Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            store
                .cache
                .as_ref()
                .unwrap()
                .lookup(&staged.chunk)
                .is_some(),
            "verified read should asynchronously install a cache entry"
        );

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime
            .block_on(backend.put(
                &ObjectPath::from(key.clone()),
                vec![3; MAX_STAGED_CHUNK_BYTES].into(),
            ))
            .unwrap();
        assert_eq!(
            store
                .read_chunk(&staged.chunk, &source(&staged.chunk, key))
                .unwrap()
                .as_slice(),
            staged.bytes()
        );
    }

    #[test]
    fn timeout_cancels_the_owned_operation_before_worker_join() {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        let mut config = options();
        config.request_timeout = Duration::from_millis(20);
        let backend = ThrottledStore::new(
            object_store::memory::InMemory::new(),
            ThrottleConfig {
                wait_put_per_call: Duration::from_secs(5),
                ..Default::default()
            },
        );
        let store = S3ChunkStore::with_backend(config, Arc::new(backend)).unwrap();
        let start = std::time::Instant::now();
        let result = store.put(StagedChunk::new(
            OperationId::new("timeout"),
            b"bytes".to_vec(),
        ));
        assert_eq!(result.unwrap_err().code(), afs_error::IO_TIMEOUT);
        drop(store);
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}
