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
        unsigned_payload: false,
        credentials: S3CredentialsSource::Environment,
        request_timeout: Duration::from_secs(2),
        max_retries: 0,
        cache: None,
        metrics: None,
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

struct CapturedPut {
    header: String,
    body: Vec<u8>,
    chunk: ChunkObject,
    result: Result<ChunkReceipt>,
}

fn put_against_http_fixture(unsigned_payload: bool, status: &'static [u8]) -> CapturedPut {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut config = options();
    config.unsigned_payload = unsigned_payload;
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
        let length = header
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|value| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        let mut body = vec![0; length];
        connection.read_exact(&mut body).unwrap();
        connection.write_all(status).unwrap();
        (header, body)
    });
    let store = S3ChunkStore::new(config).unwrap();
    let staged = StagedChunk::new(OperationId::new("fixture-put"), b"payload".to_vec());
    let chunk = staged.chunk.clone();
    let result = store.put(staged);
    let (header, body) = server.join().unwrap();
    CapturedPut {
        header,
        body,
        chunk,
        result,
    }
}

fn header_value<'a>(header: &'a str, name: &str) -> &'a str {
    let prefix = format!("{}:", name.to_ascii_lowercase());
    header
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix(&prefix)
                .map(|_| line.split_once(':').unwrap().1.trim())
        })
        .unwrap_or_else(|| panic!("missing header {name}: {header}"))
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

fn write_ready_cache_file(shared: &CacheShared, staged: &StagedChunk) -> CacheKey {
    shared.initialize_directory().unwrap();
    let key = CacheKey::new("filesystem-a", "generation-1", &staged.chunk);
    let path = shared.path_for(&key);
    std::fs::write(&path, staged.bytes()).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    {
        let mut inner = shared.inner.lock().unwrap();
        inner.entries.insert(
            key.clone(),
            CacheEntry::Ready {
                length: staged.chunk.length,
                access_bucket: 0,
            },
        );
        inner.ready_bytes = inner.ready_bytes.saturating_add(staged.chunk.length);
        touch_cache_access(&mut inner, &key);
    }
    key
}

fn force_cache_access_bucket(shared: &CacheShared, bucket: u64) {
    shared.inner.lock().unwrap().test_hooks.forced_access_bucket = Some(bucket);
}

fn write_ready_payload(shared: &CacheShared, staged: &StagedChunk, payload: &[u8]) -> PathBuf {
    shared.initialize_directory().unwrap();
    let key = CacheKey::new("filesystem-a", "generation-1", &staged.chunk);
    let path = shared.path_for(&key);
    std::fs::write(&path, payload).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    path
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

enum BodyStep {
    Bytes(Bytes),
    Error,
}

struct ScriptedBody {
    steps: VecDeque<BodyStep>,
    eof_delay: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl ScriptedBody {
    fn new(steps: Vec<BodyStep>, eof_delay: Duration) -> Self {
        Self {
            steps: VecDeque::from(steps),
            eof_delay: Some(Box::pin(tokio::time::sleep(eof_delay))),
        }
    }
}

impl tokio_stream::Stream for ScriptedBody {
    type Item = object_store::Result<Bytes>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(step) = self.steps.pop_front() {
            return Poll::Ready(Some(match step {
                BodyStep::Bytes(bytes) => Ok(bytes),
                BodyStep::Error => Err(object_store::Error::Generic {
                    store: "scripted body",
                    source: std::io::Error::other("injected body failure").into(),
                }),
            }));
        }
        if let Some(delay) = self.eof_delay.as_mut()
            && std::future::Future::poll(delay.as_mut(), cx).is_pending()
        {
            return Poll::Pending;
        }
        self.eof_delay = None;
        Poll::Ready(None)
    }
}

async fn scripted_get_result(
    expected_len: u64,
    steps: Vec<BodyStep>,
    eof_delay: Duration,
) -> object_store::GetResult {
    let backend = object_store::memory::InMemory::new();
    let location = ObjectPath::from("scripted");
    backend
        .put(
            &location,
            vec![0_u8; usize::try_from(expected_len).unwrap()].into(),
        )
        .await
        .unwrap();
    let base = backend.get(&location).await.unwrap();
    object_store::GetResult {
        payload: object_store::GetResultPayload::Stream(Box::pin(ScriptedBody::new(
            steps, eof_delay,
        ))),
        meta: base.meta,
        range: 0..expected_len,
        attributes: base.attributes,
        extensions: base.extensions,
    }
}

#[test]
fn collect_s3_body_records_delayed_terminal_eof_without_extra_bytes() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let result = scripted_get_result(
            2,
            vec![BodyStep::Bytes(Bytes::from_static(b"ok"))],
            Duration::from_millis(5),
        )
        .await;
        let (bytes, timing) = collect_s3_body(result, 2).await.unwrap();
        assert_eq!(bytes, b"ok");
        assert_eq!(timing.copied_bytes, 2);
        assert!(timing.final_eof_wait_us > 0);
        assert!(
            timing.poll_wait_total_us >= timing.first_poll_wait_us,
            "body wait total covers data polls but not the separate terminal EOF poll"
        );
    });
}

#[test]
fn collect_s3_body_accepts_empty_tail_before_delayed_eof() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let result = scripted_get_result(
            2,
            vec![
                BodyStep::Bytes(Bytes::from_static(b"ok")),
                BodyStep::Bytes(Bytes::new()),
            ],
            Duration::from_millis(5),
        )
        .await;
        let (bytes, timing) = collect_s3_body(result, 2).await.unwrap();
        assert_eq!(bytes, b"ok");
        assert_eq!(timing.copied_bytes, 2);
        assert!(timing.final_eof_wait_us > 0);
    });
}

#[test]
fn collect_s3_body_rejects_short_oversize_and_stream_errors() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let short = scripted_get_result(
            3,
            vec![BodyStep::Bytes(Bytes::from_static(b"no"))],
            Duration::ZERO,
        )
        .await;
        assert_eq!(
            collect_s3_body(short, 3).await.unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );

        let oversize = scripted_get_result(
            1,
            vec![BodyStep::Bytes(Bytes::from_static(b"xx"))],
            Duration::ZERO,
        )
        .await;
        assert_eq!(
            collect_s3_body(oversize, 1).await.unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );

        let errored = scripted_get_result(1, vec![BodyStep::Error], Duration::ZERO).await;
        assert_eq!(
            collect_s3_body(errored, 1).await.unwrap_err().code(),
            afs_error::IO_UNAVAILABLE
        );
    });
}

#[test]
fn slow_get_limiter_caps_and_resets_per_window() {
    let limiter = S3SlowGetLimiter::default();
    for _ in 0..SLOW_GET_LIMIT_PER_WINDOW {
        assert!(limiter.allow());
    }
    assert!(!limiter.allow());

    let expired = std::time::Instant::now()
        .checked_sub(SLOW_GET_WINDOW + Duration::from_secs(1))
        .unwrap();
    limiter.set_window_for_test(expired, SLOW_GET_LIMIT_PER_WINDOW);
    assert!(limiter.allow());
}

#[test]
fn cache_activity_observer_ignores_out_of_order_snapshots() {
    let registry = afs_metrics::registry();
    let metrics = S3ChunkStoreMetrics::register(&registry).unwrap();
    let observer = S3GetObserver::new(Some(metrics));
    observer.observe_cache_activity(CacheActivity {
        admissions: 6,
        ..CacheActivity::default()
    });
    observer.observe_cache_activity(CacheActivity {
        admissions: 5,
        ..CacheActivity::default()
    });
    observer.observe_cache_activity(CacheActivity {
        admissions: 6,
        ..CacheActivity::default()
    });
    let text = afs_metrics::encode_text(&registry).unwrap();
    assert!(
        text.contains("afs_dfs_s3_cache_activity_total{event=\"admissions\"} 6"),
        "{text}"
    );
}

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
        while self.activity.hold.load(SeqCst) || (tag == 0 && self.activity.hold_first.load(SeqCst))
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
        .map(|tag| StagedChunk::new(OperationId::new(format!("upload-{tag}")), vec![tag; length]))
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
fn valid_but_replaced_public_identity_does_not_replace_content_proof() {
    let store = S3ChunkStore {
        options: options(),
        sender: None,
        wake: Arc::new(tokio::sync::Notify::new()),
        worker: None,
        cache: None,
        verifier: S3ChunkVerifier::new().unwrap(),
        observer: S3GetObserver::new(None),
    };
    let mut original = StagedChunk::new(OperationId::new("producer"), b"original".to_vec());
    let different = StagedChunk::new(OperationId::new("producer"), b"different".to_vec());
    original.chunk = different.chunk;
    validate_chunk(&original.chunk).unwrap();
    assert_eq!(
        store.put_batch(vec![original]).unwrap_err().code(),
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
fn concurrent_puts_share_four_slots_and_replace_completed_uploads() {
    // Native S3 writes call put_batch concurrently from blocking-pool threads.
    use std::sync::atomic::Ordering::SeqCst;
    let backend = ObservedStore::new(None, Duration::from_millis(10));
    backend.activity.hold_first.store(true, SeqCst);
    let store = Arc::new(S3ChunkStore::with_backend(options(), backend.clone()).unwrap());
    let writers: Vec<_> = upload_batch(12, 1024)
        .into_iter()
        .map(|staged| {
            let store = store.clone();
            thread::spawn(move || store.put_batch(vec![staged]))
        })
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while backend.activity.started.load(SeqCst) < 5 && std::time::Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        backend.activity.started.load(SeqCst) >= 5,
        "completed PUT must admit another chunk while the first PUT is held"
    );
    assert_eq!(backend.activity.maximum.load(SeqCst), MAX_UPLOADS);
    backend.activity.hold_first.store(false, SeqCst);
    let mut operations: Vec<_> = writers
        .into_iter()
        .map(|writer| {
            let receipts = writer.join().unwrap().unwrap();
            assert_eq!(receipts.len(), 1);
            receipts[0].operation_id.0.clone()
        })
        .collect();
    operations.sort();
    let mut expected: Vec<_> = (0..12).map(|index| format!("upload-{index}")).collect();
    expected.sort();
    assert_eq!(operations, expected);
    drop(store);
    assert_eq!(backend.activity.started.load(SeqCst), 12);
    assert_eq!(backend.activity.active.load(SeqCst), 0);
}

#[test]
fn upload_queue_backpressure_waits_without_a_false_storage_error() {
    // More concurrent writers than active request slots: the surplus waits
    // instead of failing, and only MAX_UPLOADS PUTs ever run at once.
    use std::sync::atomic::Ordering::SeqCst;
    let backend = ObservedStore::new(None, Duration::from_millis(1));
    backend.activity.hold.store(true, SeqCst);
    let store = Arc::new(S3ChunkStore::with_backend(options(), backend.clone()).unwrap());
    let writers: Vec<_> = upload_batch((MAX_ACTIVE_REQUESTS + 2) as u8, 1024)
        .into_iter()
        .map(|staged| {
            let store = store.clone();
            thread::spawn(move || store.put_batch(vec![staged]))
        })
        .collect();
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while backend.activity.started.load(SeqCst) < MAX_UPLOADS
        && std::time::Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(50));
    assert_eq!(backend.activity.maximum.load(SeqCst), MAX_UPLOADS);
    assert!(writers.iter().all(|writer| !writer.is_finished()));
    backend.activity.hold.store(false, SeqCst);
    for writer in writers {
        assert_eq!(writer.join().unwrap().unwrap().len(), 1);
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
        observer: S3GetObserver::new(None),
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
    let expected = external_object_key("filesystem-a", "generation-1", &staged.chunk.id).unwrap();
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
fn ready_load_reserves_unverified_payload_until_verified_handoff() {
    let dir = tempfile::tempdir().unwrap();
    let verifier = S3ChunkVerifier::new().unwrap();
    let shared = Arc::new(CacheShared {
        options: cache_options(dir.path(), MAX_STAGED_CHUNK_BYTES as u64),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: verifier.clone(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    });
    let staged = StagedChunk::new(
        OperationId::new("ready-budget"),
        vec![7; MAX_STAGED_CHUNK_BYTES],
    );
    write_ready_cache_file(&shared, &staged);
    let blocker = verifier.block_next_hash_for_test();
    let reader = {
        let shared = shared.clone();
        let chunk = staged.chunk.clone();
        thread::spawn(move || shared.lookup(&chunk).unwrap())
    };

    blocker.wait_started();
    let pending_bytes_while_hashing = {
        let inner = shared.inner.lock().unwrap();
        inner.pending_bytes
    };
    blocker.release();
    let verified = reader.join().unwrap();
    assert_eq!(verified.as_slice(), staged.bytes());
    assert!(
        pending_bytes_while_hashing >= MAX_STAGED_CHUNK_BYTES as u64,
        "unverified Ready load must be charged while hash is still in flight"
    );
    {
        let inner = shared.inner.lock().unwrap();
        assert_eq!(inner.raw_ready_bytes, 0);
        assert_eq!(
            inner.pending_bytes, MAX_STAGED_CHUNK_BYTES as u64,
            "raw reservation must transfer to the tracked verified payload without double charge"
        );
    }
    shared.clear_resident_for_test();
    {
        let inner = shared.inner.lock().unwrap();
        assert_eq!(
            inner.pending_bytes, MAX_STAGED_CHUNK_BYTES as u64,
            "caller-held verified payload must stay charged after resident is cleared"
        );
    }
    drop(verified);
    {
        let mut inner = shared.inner.lock().unwrap();
        prune_tracked_allocations(&mut inner);
        assert_eq!(inner.pending_bytes, 0);
    }
}

#[test]
fn ready_load_budget_evicts_unpinned_resident_before_rejecting_new_load() {
    let dir = tempfile::tempdir().unwrap();
    let verifier = S3ChunkVerifier::new().unwrap();
    let shared = Arc::new(CacheShared {
        options: cache_options(dir.path(), MAX_STAGED_CHUNK_BYTES as u64),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier,
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    });
    let first = StagedChunk::new(
        OperationId::new("ready-first"),
        vec![1; MAX_STAGED_CHUNK_BYTES],
    );
    let second = StagedChunk::new(
        OperationId::new("ready-second"),
        vec![2; MAX_STAGED_CHUNK_BYTES],
    );
    let third = StagedChunk::new(
        OperationId::new("ready-third"),
        vec![3; MAX_STAGED_CHUNK_BYTES],
    );
    write_ready_cache_file(&shared, &first);
    write_ready_cache_file(&shared, &second);
    write_ready_cache_file(&shared, &third);

    let first_loaded = shared.lookup(&first.chunk).unwrap();
    assert_eq!(first_loaded.as_slice(), first.bytes());
    drop(first_loaded);
    {
        let inner = shared.inner.lock().unwrap();
        assert_eq!(inner.pending_bytes, MAX_STAGED_CHUNK_BYTES as u64);
        assert!(inner.resident.is_some());
    }

    let second_loaded = shared.lookup_result(&second.chunk).unwrap().unwrap();
    assert_eq!(second_loaded.as_slice(), second.bytes());
    {
        let inner = shared.inner.lock().unwrap();
        assert_eq!(inner.pending_bytes, MAX_STAGED_CHUNK_BYTES as u64);
        assert_eq!(inner.raw_ready_bytes, 0);
        assert!(inner.resident.as_ref().is_some_and(|resident| {
            resident.key == CacheKey::new("filesystem-a", "generation-1", &second.chunk)
        }));
    }

    shared.clear_resident_for_test();
    let third_result = shared.lookup_result(&third.chunk).unwrap_err();
    assert_eq!(third_result.code(), afs_error::IO_CAPACITY);
    assert!(
        shared
            .path_for(&CacheKey::new("filesystem-a", "generation-1", &third.chunk))
            .exists(),
        "capacity exhaustion must not remove Ready data or bypass budget through S3"
    );
    drop(second_loaded);
    {
        let mut inner = shared.inner.lock().unwrap();
        prune_tracked_allocations(&mut inner);
        assert_eq!(inner.pending_bytes, 0);
    }
}

#[test]
fn ready_load_reuses_unique_resident_allocation_inside_existing_budget() {
    let first = StagedChunk::new(OperationId::new("resident-first"), b"old-data".to_vec());
    let second = StagedChunk::new(OperationId::new("resident-second"), b"new-data".to_vec());
    let retained_capacity = 64;
    let dir = tempfile::tempdir().unwrap();
    let mut options = cache_options(dir.path(), retained_capacity as u64);
    options.pending_bytes = retained_capacity as u64;
    let shared = CacheShared {
        options,
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    let first_key = CacheKey::new("filesystem-a", "generation-1", &first.chunk);
    let second_key = write_ready_cache_file(&shared, &second);
    {
        let mut inner = shared.inner.lock().unwrap();
        inner.entries.insert(
            first_key.clone(),
            CacheEntry::Ready {
                length: first.chunk.length,
                access_bucket: 0,
            },
        );
        assert!(shared.install_resident_locked(
            &mut inner,
            first_key,
            &first.chunk,
            verified_with_capacity(&first, retained_capacity),
        ));
        assert_eq!(inner.pending_bytes, retained_capacity as u64);
    }

    let loaded = shared.lookup(&second.chunk).unwrap();

    assert_eq!(loaded.as_slice(), second.bytes());
    assert_eq!(
        loaded.allocation_bytes(),
        retained_capacity as u64,
        "unique resident buffer should be demoted to raw Ready storage instead of allocating a fresh length-sized Vec"
    );
    let inner = shared.inner.lock().unwrap();
    assert_eq!(inner.raw_ready_bytes, 0);
    assert_eq!(inner.pending_bytes, retained_capacity as u64);
    assert!(inner.resident.as_ref().is_some_and(|resident| {
        resident.key == second_key && resident.bytes.same_allocation(&loaded)
    }));
}

#[test]
fn ready_load_recycled_capacity_stays_charged_for_short_tail() {
    let first = StagedChunk::new(OperationId::new("full-resident"), vec![1; 64]);
    let dir = tempfile::tempdir().unwrap();
    let mut options = cache_options(dir.path(), 64);
    options.pending_bytes = 64;
    let shared = CacheShared {
        options,
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    let bytes = verified_with_capacity(&first, 64);
    let pointer = bytes.as_slice().as_ptr();
    {
        let mut inner = shared.inner.lock().unwrap();
        assert!(shared.install_resident_locked(
            &mut inner,
            CacheKey::new("filesystem-a", "generation-1", &first.chunk),
            &first.chunk,
            bytes
        ));
    }
    let (raw, reservation) = shared.reserve_ready_load_buffer(7).unwrap();
    assert_eq!(raw.as_ptr(), pointer);
    assert_eq!(raw.len(), 7);
    assert_eq!(raw.capacity(), 64);
    {
        let inner = shared.inner.lock().unwrap();
        assert_eq!(inner.raw_ready_bytes, 64);
        assert_eq!(inner.pending_bytes, 64);
        assert!(inner.resident.is_none());
    }
    drop(raw);
    drop(reservation);
    assert_eq!(shared.inner.lock().unwrap().pending_bytes, 0);
}

#[test]
fn ready_load_larger_object_falls_back_without_exceeding_budget() {
    let first = StagedChunk::new(OperationId::new("small-resident"), vec![1; 8]);
    let dir = tempfile::tempdir().unwrap();
    let mut options = cache_options(dir.path(), 64);
    options.pending_bytes = 64;
    let shared = CacheShared {
        options,
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    {
        let mut inner = shared.inner.lock().unwrap();
        assert!(shared.install_resident_locked(
            &mut inner,
            CacheKey::new("filesystem-a", "generation-1", &first.chunk),
            &first.chunk,
            VerifiedExternalChunk::verify(&first.chunk, first.bytes().to_vec()).unwrap()
        ));
    }
    let (raw, reservation) = shared.reserve_ready_load_buffer(64).unwrap();
    assert_eq!(raw.len(), 64);
    assert_eq!(shared.inner.lock().unwrap().pending_bytes, 64);
    drop(raw);
    drop(reservation);
    assert_eq!(shared.inner.lock().unwrap().pending_bytes, 0);
}

#[test]
fn ready_load_does_not_recycle_pinned_resident_allocation() {
    let first = StagedChunk::new(OperationId::new("resident-pinned"), b"old-data".to_vec());
    let second = StagedChunk::new(OperationId::new("resident-next"), b"new-data".to_vec());
    let retained_capacity = 64;
    let dir = tempfile::tempdir().unwrap();
    let mut options = cache_options(dir.path(), retained_capacity as u64);
    options.pending_bytes = retained_capacity as u64;
    let shared = CacheShared {
        options,
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    let first_key = CacheKey::new("filesystem-a", "generation-1", &first.chunk);
    write_ready_cache_file(&shared, &second);
    {
        let mut inner = shared.inner.lock().unwrap();
        inner.entries.insert(
            first_key.clone(),
            CacheEntry::Ready {
                length: first.chunk.length,
                access_bucket: 0,
            },
        );
        assert!(shared.install_resident_locked(
            &mut inner,
            first_key,
            &first.chunk,
            verified_with_capacity(&first, retained_capacity),
        ));
    }
    let held_pin = shared.lookup(&first.chunk).unwrap();

    let error = shared.lookup_result(&second.chunk).unwrap_err();

    assert_eq!(error.code(), afs_error::IO_CAPACITY);
    assert_eq!(held_pin.as_slice(), first.bytes());
    let mut inner = shared.inner.lock().unwrap();
    prune_tracked_allocations(&mut inner);
    assert_eq!(inner.raw_ready_bytes, 0);
    assert_eq!(inner.pending_bytes, retained_capacity as u64);
}

#[test]
fn ready_load_recycled_buffer_failure_does_not_expose_old_identity() {
    let first = StagedChunk::new(OperationId::new("resident-stale"), b"old-data".to_vec());
    let second = StagedChunk::new(OperationId::new("resident-corrupt"), b"new-data".to_vec());
    let retained_capacity = 64;
    let dir = tempfile::tempdir().unwrap();
    let mut options = cache_options(dir.path(), retained_capacity as u64);
    options.pending_bytes = retained_capacity as u64;
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
    write_ready_payload(&shared, &second, b"bad-data");
    {
        let mut inner = shared.inner.lock().unwrap();
        inner.entries.insert(
            first_key.clone(),
            CacheEntry::Ready {
                length: first.chunk.length,
                access_bucket: 0,
            },
        );
        assert!(shared.install_resident_locked(
            &mut inner,
            first_key,
            &first.chunk,
            verified_with_capacity(&first, retained_capacity),
        ));
        inner.entries.insert(
            second_key.clone(),
            CacheEntry::Ready {
                length: second.chunk.length,
                access_bucket: 0,
            },
        );
        inner.ready_bytes = inner.ready_bytes.saturating_add(second.chunk.length);
    }

    assert!(shared.lookup(&second.chunk).is_none());

    let mut inner = shared.inner.lock().unwrap();
    prune_tracked_allocations(&mut inner);
    assert_eq!(inner.raw_ready_bytes, 0);
    assert_eq!(inner.pending_bytes, 0);
    assert!(inner.resident.is_none());
    assert!(!inner.entries.contains_key(&second_key));
}

#[test]
fn ready_load_worker_panic_releases_raw_budget() {
    let dir = tempfile::tempdir().unwrap();
    let verifier = S3ChunkVerifier::new().unwrap();
    let shared = CacheShared {
        options: cache_options(dir.path(), MAX_STAGED_CHUNK_BYTES as u64),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: verifier.clone(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    let staged = StagedChunk::new(
        OperationId::new("ready-panic"),
        vec![9; MAX_STAGED_CHUNK_BYTES],
    );
    let path = write_ready_payload(&shared, &staged, staged.bytes());
    verifier.panic_next_hash_for_test();

    let error = shared.read_file_verified(&path, &staged.chunk).unwrap_err();
    assert_eq!(error.code(), afs_error::IO_UNAVAILABLE);
    let inner = shared.inner.lock().unwrap();
    assert_eq!(inner.raw_ready_bytes, 0);
    assert_eq!(inner.pending_bytes, 0);
    assert!(inner.resident.is_none());
}

#[test]
fn ready_load_shutdown_waits_for_accepted_pipeline_and_keeps_raw_budget_charged() {
    let dir = tempfile::tempdir().unwrap();
    let verifier = S3ChunkVerifier::new().unwrap();
    let shared = Arc::new(CacheShared {
        options: cache_options(dir.path(), MAX_STAGED_CHUNK_BYTES as u64),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: verifier.clone(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    });
    let staged = StagedChunk::new(
        OperationId::new("ready-shutdown"),
        vec![5; MAX_STAGED_CHUNK_BYTES],
    );
    write_ready_cache_file(&shared, &staged);
    let blocker = verifier.block_next_hash_for_test();
    let reader = {
        let shared = shared.clone();
        let chunk = staged.chunk.clone();
        thread::spawn(move || shared.lookup(&chunk).unwrap())
    };

    blocker.wait_started();
    let shutdown_finished = Arc::new(AtomicBool::new(false));
    let shutdowner = {
        let verifier = verifier.clone();
        let shutdown_finished = shutdown_finished.clone();
        thread::spawn(move || {
            let result = verifier.shutdown();
            shutdown_finished.store(true, Ordering::SeqCst);
            result
        })
    };
    thread::sleep(Duration::from_millis(50));
    let pending_bytes_while_shutdown_waits = {
        let inner = shared.inner.lock().unwrap();
        inner.pending_bytes
    };
    assert!(
        !shutdown_finished.load(Ordering::SeqCst),
        "shutdown must wait for the accepted Ready pipeline gate"
    );
    assert!(
        pending_bytes_while_shutdown_waits >= MAX_STAGED_CHUNK_BYTES as u64,
        "accepted Ready load must remain charged while shutdown waits"
    );

    blocker.release();
    let verified = reader.join().unwrap();
    shutdowner.join().unwrap().unwrap();
    assert_eq!(verified.as_slice(), staged.bytes());
    assert_eq!(verifier.joined_workers_for_test(), S3_HASH_WORKERS);
    {
        let inner = shared.inner.lock().unwrap();
        assert_eq!(inner.raw_ready_bytes, 0);
        assert_eq!(
            inner.pending_bytes, MAX_STAGED_CHUNK_BYTES as u64,
            "verified payload pin must remain charged after shutdown joins workers"
        );
    }
    shared.clear_resident_for_test();
    {
        let inner = shared.inner.lock().unwrap();
        assert_eq!(
            inner.pending_bytes, MAX_STAGED_CHUNK_BYTES as u64,
            "caller pin must keep the verified payload charged after resident clear"
        );
    }
    drop(verified);
    {
        let mut inner = shared.inner.lock().unwrap();
        prune_tracked_allocations(&mut inner);
        assert_eq!(inner.pending_bytes, 0);
    }
}

#[test]
fn ready_load_pipeline_keeps_full_digest_and_segment_events() {
    let sizes = [
        1024 * 1024 - 1,
        1024 * 1024,
        1024 * 1024 + 1,
        2 * 1024 * 1024 - 1,
        2 * 1024 * 1024,
        2 * 1024 * 1024 + 1,
        MAX_STAGED_CHUNK_BYTES,
    ];
    for size in sizes {
        let dir = tempfile::tempdir().unwrap();
        let verifier = S3ChunkVerifier::new().unwrap();
        let shared = CacheShared {
            options: cache_options(dir.path(), MAX_STAGED_CHUNK_BYTES as u64),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: verifier.clone(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        let payload = (0..size)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new(format!("ready-{size}")), payload);
        write_ready_cache_file(&shared, &staged);

        let verified = shared.lookup(&staged.chunk).unwrap();
        assert_eq!(verified.as_slice(), staged.bytes());
        let (reads, hashes, split) = verifier.ready_segment_events_for_test();
        let expected_segments = size.div_ceil(1024 * 1024);
        assert_eq!(reads, expected_segments, "Ready load read segment count");
        assert_eq!(hashes, expected_segments, "Ready load hash segment count");
        assert_eq!(
            split,
            usize::from(expected_segments > 1),
            "large Ready loads must expose split segment events"
        );
    }
}

#[test]
fn ready_load_rejects_truncated_oversized_and_tail_corrupt_files() {
    let cases = [
        ("short", b"0123456".to_vec()),
        ("long", b"012345678".to_vec()),
        ("tail", b"0123456x".to_vec()),
    ];
    for (name, payload) in cases {
        let dir = tempfile::tempdir().unwrap();
        let staged = StagedChunk::new(
            OperationId::new(format!("ready-{name}")),
            b"01234567".to_vec(),
        );
        let shared = CacheShared {
            options: cache_options(dir.path(), 1024 * 1024),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        let path = write_ready_payload(&shared, &staged, &payload);

        assert!(shared.read_file_verified(&path, &staged.chunk).is_err());
        let inner = shared.inner.lock().unwrap();
        assert_eq!(inner.raw_ready_bytes, 0);
        assert_eq!(inner.pending_bytes, 0);
        assert!(inner.resident.is_none());
    }
}

#[test]
fn recovery_preserves_ready_files_when_raw_load_budget_is_exhausted() {
    let dir = tempfile::tempdir().unwrap();
    let staged = StagedChunk::new(OperationId::new("recovery-budget"), b"payload".to_vec());
    let mut options = cache_options(dir.path(), 1024 * 1024);
    options.pending_bytes = staged.chunk.length - 1;
    let shared = CacheShared {
        options,
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    let path = write_ready_payload(&shared, &staged, staged.bytes());

    assert_eq!(
        shared.recover_ready_entries().unwrap_err().code(),
        afs_error::IO_CAPACITY
    );
    assert!(
        path.exists(),
        "budget exhaustion must not delete valid Ready data"
    );
    let inner = shared.inner.lock().unwrap();
    assert!(inner.entries.is_empty());
    assert_eq!(inner.raw_ready_bytes, 0);
    assert_eq!(inner.pending_bytes, 0);
}

#[test]
fn failed_ready_removal_keeps_conservative_cache_budget() {
    let dir = tempfile::tempdir().unwrap();
    let blocked = StagedChunk::new(OperationId::new("blocked"), b"blocked".to_vec());
    let extra = StagedChunk::new(OperationId::new("extra"), b"extra".to_vec());
    let later = StagedChunk::new(OperationId::new("later"), b"later".to_vec());
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
                access_bucket: 0,
            },
        );
        inner.ready_bytes = blocked.chunk.length;
        touch_cache_access(&mut inner, &key);
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

    let blocked_path = shared.path_for(&key);
    std::fs::remove_dir(&blocked_path).unwrap();
    std::fs::write(&blocked_path, blocked.bytes()).unwrap();
    std::fs::set_permissions(&blocked_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    shared.schedule_from_read(&later.chunk, verified(&later));
    let later_key = CacheKey::new("filesystem-a", "generation-1", &later.chunk);
    {
        let inner = shared.inner.lock().unwrap();
        assert!(
            !inner.entries.contains_key(&key),
            "failed removal must keep the Ready entry eligible for a later successful eviction"
        );
        assert!(matches!(
            inner.entries.get(&later_key),
            Some(CacheEntry::Pending { .. })
        ));
        assert_eq!(inner.ready_bytes, 0);
        assert_eq!(inner.temp_bytes, later.chunk.length);
        assert_eq!(inner.activity.evictions, 1);
    }
    assert!(!blocked_path.exists());
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
fn eviction_sampler_selects_older_of_two_ready_candidates() {
    let dir = tempfile::tempdir().unwrap();
    let shared = CacheShared {
        options: cache_options(dir.path(), 3),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    let oldest = StagedChunk::new(OperationId::new("oldest"), b"a".to_vec());
    let middle = StagedChunk::new(OperationId::new("middle"), b"b".to_vec());
    let newest = StagedChunk::new(OperationId::new("newest"), b"c".to_vec());
    force_cache_access_bucket(&shared, 1);
    let oldest_key = write_ready_cache_file(&shared, &oldest);
    force_cache_access_bucket(&shared, 2);
    let middle_key = write_ready_cache_file(&shared, &middle);
    force_cache_access_bucket(&shared, 3);
    let newest_key = write_ready_cache_file(&shared, &newest);
    {
        let mut inner = shared.inner.lock().unwrap();
        inner.test_hooks.eviction_samples.push_back((2, 0));
        assert!(shared.evict_for_locked(&mut inner, 1));
        assert!(!inner.entries.contains_key(&oldest_key));
        assert!(matches!(
            inner.entries.get(&middle_key),
            Some(CacheEntry::Ready { .. })
        ));
        assert!(matches!(
            inner.entries.get(&newest_key),
            Some(CacheEntry::Ready { .. })
        ));
        assert_eq!(inner.ready_bytes, 2);
        assert_eq!(inner.activity.evictions, 1);
        assert_eq!(
            inner.activity.eviction_ns_total,
            inner.activity.eviction_ns_max
        );
    }
    assert!(!shared.path_for(&oldest_key).exists());
    assert!(shared.path_for(&middle_key).exists());
    assert!(shared.path_for(&newest_key).exists());
}

#[test]
fn eviction_sampler_keeps_first_random_candidate_inside_same_access_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let shared = CacheShared {
        options: cache_options(dir.path(), 3),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    force_cache_access_bucket(&shared, 7);
    let first = StagedChunk::new(OperationId::new("same-first"), b"a".to_vec());
    let second = StagedChunk::new(OperationId::new("same-second"), b"b".to_vec());
    let third = StagedChunk::new(OperationId::new("same-third"), b"c".to_vec());
    let first_key = write_ready_cache_file(&shared, &first);
    let second_key = write_ready_cache_file(&shared, &second);
    let third_key = write_ready_cache_file(&shared, &third);

    {
        let mut inner = shared.inner.lock().unwrap();
        inner.test_hooks.eviction_samples.push_back((2, 0));
        assert!(shared.evict_for_locked(&mut inner, 1));
        assert!(
            inner.entries.contains_key(&first_key),
            "same-bucket comparison must not fall back to exact LRU position"
        );
        assert!(inner.entries.contains_key(&second_key));
        assert!(!inner.entries.contains_key(&third_key));
        assert_eq!(inner.activity.evictions, 1);
    }
    assert!(shared.path_for(&first_key).exists());
    assert!(shared.path_for(&second_key).exists());
    assert!(!shared.path_for(&third_key).exists());
}

#[test]
fn ready_lookup_refreshes_access_bucket_without_expanding_budget() {
    let dir = tempfile::tempdir().unwrap();
    let shared = CacheShared {
        options: cache_options(dir.path(), 2),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    let refreshed = StagedChunk::new(OperationId::new("refreshed"), b"a".to_vec());
    let stale = StagedChunk::new(OperationId::new("stale"), b"b".to_vec());
    let incoming = StagedChunk::new(OperationId::new("incoming"), b"c".to_vec());
    force_cache_access_bucket(&shared, 1);
    let refreshed_key = write_ready_cache_file(&shared, &refreshed);
    force_cache_access_bucket(&shared, 2);
    let stale_key = write_ready_cache_file(&shared, &stale);

    force_cache_access_bucket(&shared, 3);
    assert_eq!(shared.lookup(&refreshed.chunk).unwrap().as_slice(), b"a");
    shared.clear_resident_for_test();
    {
        let mut inner = shared.inner.lock().unwrap();
        inner.test_hooks.eviction_samples.push_back((0, 1));
        assert!(shared.evict_for_locked(&mut inner, incoming.chunk.length));
        inner.entries.insert(
            CacheKey::new("filesystem-a", "generation-1", &incoming.chunk),
            CacheEntry::Ready {
                length: incoming.chunk.length,
                access_bucket: 0,
            },
        );
        inner.ready_bytes = inner.ready_bytes.saturating_add(incoming.chunk.length);
        assert!(
            inner.entries.contains_key(&refreshed_key),
            "Ready hit in a newer bucket must protect the refreshed object"
        );
        assert!(!inner.entries.contains_key(&stale_key));
        assert_eq!(inner.pending_bytes, 0);
    }
}

#[test]
fn first_ready_and_recovered_index_use_current_access_bucket() {
    let dir = tempfile::tempdir().unwrap();
    let config = cache_options(dir.path(), 8);
    let cache = S3ChunkCache::new(
        config.clone(),
        "filesystem-a",
        "generation-1",
        S3ChunkVerifier::new().unwrap(),
    )
    .unwrap();
    let staged = StagedChunk::new(OperationId::new("first-ready-bucket"), b"original".to_vec());
    let key = CacheKey::new("filesystem-a", "generation-1", &staged.chunk);
    force_cache_access_bucket(&cache.shared, 41);
    cache.schedule_from_read(&staged.chunk, verified(&staged));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if access_bucket_for(&cache.shared.inner.lock().unwrap(), &key).is_some() {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        access_bucket_for(&cache.shared.inner.lock().unwrap(), &key),
        Some(41)
    );
    drop(cache);
    let recovered = CacheShared {
        options: config,
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    force_cache_access_bucket(&recovered, 3);
    recovered.recover_ready_entries().unwrap();
    let inner = recovered.inner.lock().unwrap();
    assert_eq!(access_bucket_for(&inner, &key), Some(3));
    assert_eq!(inner.ready_bytes, 8);
    assert_eq!(inner.lru.len(), 1);
}

#[test]
fn ready_bucket_fits_existing_entry_metadata() {
    // The old Pending variant already determines the enum allocation size.
    enum PreviousEntry {
        Pending { bytes: VerifiedExternalChunk },
        Ready { length: u64 },
    }
    let staged = StagedChunk::new(OperationId::new("entry-layout"), b"a".to_vec());
    let previous = [
        PreviousEntry::Pending {
            bytes: verified(&staged),
        },
        PreviousEntry::Ready {
            length: staged.chunk.length,
        },
    ];
    for entry in previous {
        let length = match entry {
            PreviousEntry::Pending { bytes } => bytes.as_slice().len() as u64,
            PreviousEntry::Ready { length } => length,
        };
        assert_eq!(length, staged.chunk.length);
    }
    assert_eq!(
        std::mem::size_of::<CacheEntry>(),
        std::mem::size_of::<PreviousEntry>()
    );
}

#[test]
fn failed_eviction_preserves_ready_access_bucket_and_accounting() {
    let dir = tempfile::tempdir().unwrap();
    let shared = CacheShared {
        options: cache_options(dir.path(), 1),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    force_cache_access_bucket(&shared, 9);
    let staged = StagedChunk::new(OperationId::new("rollback-bucket"), b"a".to_vec());
    let key = write_ready_cache_file(&shared, &staged);
    let path = shared.path_for(&key);
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap(); // unlink must fail, preserving the indexed Ready.
    let mut inner = shared.inner.lock().unwrap();
    assert!(!shared.evict_for_locked(&mut inner, 1));
    assert_eq!(access_bucket_for(&inner, &key), Some(9));
    assert_eq!(inner.ready_bytes, 1);
    assert_eq!(inner.temp_bytes, 0);
    assert_eq!(inner.activity.evictions, 0);
    assert_eq!(inner.lru.len(), 1);
}

#[test]
fn coarse_eviction_128_objects_64_capacity_keeps_scan_hits_and_hot_entries() {
    for seed in [1_u64, 2, 3, 4, 17, 91, 255, 1024] {
        let mut inner = CacheInner {
            sampler_seed: seed,
            test_hooks: CacheTestHooks {
                forced_access_bucket: Some(0),
                ..CacheTestHooks::default()
            },
            ..CacheInner::default()
        };
        let keys = (0_u64..128)
            .map(|i| {
                let staged = StagedChunk::new(
                    OperationId::new(format!("scan-{i}")),
                    i.to_le_bytes().to_vec(),
                );
                CacheKey::new("filesystem-a", "generation-1", &staged.chunk)
            })
            .collect::<Vec<_>>();
        // Replay index-only operations at the real 128/64 scale, no sleeps or payload pool.
        for key in &keys[..64] {
            inner.entries.insert(
                key.clone(),
                CacheEntry::Ready {
                    length: 1,
                    access_bucket: 0,
                },
            );
            touch_cache_access(&mut inner, key);
        }
        for key in &keys[64..] {
            let victim = select_eviction_victim(&mut inner).unwrap();
            inner.entries.remove(&victim.key);
            inner.entries.insert(
                key.clone(),
                CacheEntry::Ready {
                    length: 1,
                    access_bucket: 0,
                },
            );
            touch_cache_access(&mut inner, key);
        }
        let mut hits = 0;
        for key in &keys {
            if inner.entries.contains_key(key) {
                hits += 1;
                touch_cache_access(&mut inner, key);
            } else {
                let victim = select_eviction_victim(&mut inner).unwrap();
                inner.entries.remove(&victim.key);
                inner.entries.insert(
                    key.clone(),
                    CacheEntry::Ready {
                        length: 1,
                        access_bucket: 0,
                    },
                );
                touch_cache_access(&mut inner, key);
            }
            assert_eq!(inner.entries.len(), 64);
            assert_eq!(inner.lru.len(), 64);
        }
        assert!(
            hits >= 8,
            "seed {seed}: sequential scan unexpectedly collapsed to {hits} hits"
        );
        let hot = inner.lru.iter().take(8).cloned().collect::<Vec<_>>();
        inner.test_hooks.forced_access_bucket = Some(2);
        for key in &hot {
            touch_cache_access(&mut inner, key);
        }
        let cold_key = inner
            .lru
            .iter()
            .find(|key| !hot.contains(key))
            .unwrap()
            .clone();
        let hot_key = hot[0].clone();
        let hot_index = inner.lru.iter().position(|key| *key == hot_key).unwrap();
        let cold_index = inner.lru.iter().position(|key| *key == cold_key).unwrap();
        inner
            .test_hooks
            .eviction_samples
            .push_back((hot_index, cold_index));
        let victim = select_eviction_victim(&mut inner).unwrap();
        assert_eq!(
            victim.key, cold_key,
            "newer hot bucket must survive a stale competitor"
        );
    }
}

#[test]
fn eviction_sampler_handles_one_none_and_pending_exclusion() {
    let dir = tempfile::tempdir().unwrap();
    let shared = CacheShared {
        options: cache_options(dir.path(), 1),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    let only = StagedChunk::new(OperationId::new("only"), b"a".to_vec());
    let only_key = write_ready_cache_file(&shared, &only);
    {
        let mut inner = shared.inner.lock().unwrap();
        assert!(shared.evict_for_locked(&mut inner, 1));
        assert!(!inner.entries.contains_key(&only_key));
        assert_eq!(inner.ready_bytes, 0);
        assert_eq!(inner.activity.evictions, 1);
    }

    let pending = StagedChunk::new(OperationId::new("pending"), b"b".to_vec());
    let pending_key = CacheKey::new("filesystem-a", "generation-1", &pending.chunk);
    {
        let mut inner = shared.inner.lock().unwrap();
        inner.entries.insert(
            pending_key.clone(),
            CacheEntry::Pending {
                bytes: verified(&pending),
            },
        );
        inner.temp_bytes = pending.chunk.length;
        touch_cache_access(&mut inner, &pending_key);
        assert!(!shared.evict_for_locked(&mut inner, 1));
        assert!(matches!(
            inner.entries.get(&pending_key),
            Some(CacheEntry::Pending { .. })
        ));
        assert_eq!(inner.temp_bytes, pending.chunk.length);
    }
}

#[test]
fn eviction_sampler_multi_seed_cyclic_scan_avoids_strict_lru_all_miss() {
    for seed in [1_u64, 2, 3, 4] {
        let dir = tempfile::tempdir().unwrap();
        let shared = CacheShared {
            options: cache_options(dir.path(), 3),
            store_id: "filesystem-a".into(),
            generation: "generation-1".into(),
            verifier: S3ChunkVerifier::new().unwrap(),
            inner: Mutex::new(CacheInner::default()),
            wake: Condvar::new(),
        };
        let chunks = [
            StagedChunk::new(OperationId::new(format!("scan-a-{seed}")), b"a".to_vec()),
            StagedChunk::new(OperationId::new(format!("scan-b-{seed}")), b"b".to_vec()),
            StagedChunk::new(OperationId::new(format!("scan-c-{seed}")), b"c".to_vec()),
            StagedChunk::new(OperationId::new(format!("scan-d-{seed}")), b"d".to_vec()),
        ];
        for staged in &chunks[..3] {
            write_ready_cache_file(&shared, staged);
        }
        shared.inner.lock().unwrap().sampler_seed = seed;

        let mut hits = 0;
        for index in [3_usize, 0, 1, 2, 3, 0, 1, 2] {
            let staged = &chunks[index];
            let key = CacheKey::new("filesystem-a", "generation-1", &staged.chunk);
            let mut inner = shared.inner.lock().unwrap();
            if matches!(inner.entries.get(&key), Some(CacheEntry::Ready { .. })) {
                hits += 1;
                touch_cache_access(&mut inner, &key);
                continue;
            }
            assert!(shared.evict_for_locked(&mut inner, staged.chunk.length));
            inner.entries.insert(
                key.clone(),
                CacheEntry::Ready {
                    length: staged.chunk.length,
                    access_bucket: 0,
                },
            );
            inner.ready_bytes = inner.ready_bytes.saturating_add(staged.chunk.length);
            touch_cache_access(&mut inner, &key);
            drop(inner);
            let path = shared.path_for(&key);
            std::fs::write(&path, staged.bytes()).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert!(
            hits > 0,
            "seed {seed} should preserve at least one object in a cyclic scan"
        );
    }
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
fn cache_shutdown_releases_namespace_despite_inherited_lock_descriptor() {
    let backend = Arc::new(object_store::memory::InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let mut config = options();
    config.cache = Some(cache_options(dir.path(), 1024 * 1024));
    let store = S3ChunkStore::with_backend(config.clone(), backend.clone()).unwrap();
    // A concurrent fork can retain the same open-file description before
    // exec closes CLOEXEC descriptors. Duplicate it to make that lifetime
    // deterministic without forking a multithreaded Rust test process.
    let inherited_lock = store.cache.as_ref().unwrap()._lock.try_clone().unwrap();
    assert!(S3ChunkStore::with_backend(config.clone(), backend.clone()).is_err());
    drop(store);
    let reopened = S3ChunkStore::with_backend(config.clone(), backend.clone()).unwrap();
    assert!(S3ChunkStore::with_backend(config.clone(), backend.clone()).is_err());
    drop(inherited_lock);
    assert!(S3ChunkStore::with_backend(config, backend).is_err());
    drop(reopened);
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
fn read_fill_pending_reject_preserves_ready_disk_entry() {
    let dir = tempfile::tempdir().unwrap();
    let ready = StagedChunk::new(OperationId::new("ready"), b"ready-01".to_vec());
    let held = StagedChunk::new(OperationId::new("held"), b"held-001".to_vec());
    let incoming = StagedChunk::new(OperationId::new("incoming"), b"incoming".to_vec());
    let shared = CacheShared {
        options: S3ChunkCacheOptions {
            directory: dir.path().join("cache"),
            capacity_bytes: ready.chunk.length,
            pending_bytes: ready.chunk.length + incoming.chunk.length - 1,
            cache_large_write: false,
        },
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    let ready_key = write_ready_cache_file(&shared, &ready);
    let ready_path = shared.path_for(&ready_key);
    let held_pin = verified(&held);
    {
        let mut inner = shared.inner.lock().unwrap();
        let held_key = CacheKey::new("filesystem-a", "generation-1", &held.chunk);
        assert!(shared.install_resident_locked(
            &mut inner,
            held_key,
            &held.chunk,
            held_pin.clone(),
        ));
        assert_eq!(inner.pending_bytes, held.chunk.length);
    }

    shared.schedule_from_read(&incoming.chunk, verified(&incoming));

    let inner = shared.inner.lock().unwrap();
    assert!(
        ready_path.exists(),
        "pending admission rejection must not delete unrelated Ready cache data"
    );
    assert!(matches!(
        inner.entries.get(&ready_key),
        Some(CacheEntry::Ready { .. })
    ));
    assert_eq!(inner.ready_bytes, ready.chunk.length);
    assert!(!inner.entries.contains_key(&CacheKey::new(
        "filesystem-a",
        "generation-1",
        &incoming.chunk,
    )));
    assert_eq!(inner.activity.pending_budget_rejects, 1);
}

#[test]
fn raw_fill_pending_reject_preserves_ready_disk_entry() {
    let dir = tempfile::tempdir().unwrap();
    let ready = StagedChunk::new(OperationId::new("ready"), b"ready-01".to_vec());
    let held = StagedChunk::new(OperationId::new("held"), b"held-001".to_vec());
    let incoming = StagedChunk::new(OperationId::new("incoming"), b"incoming".to_vec());
    let shared = CacheShared {
        options: S3ChunkCacheOptions {
            directory: dir.path().join("cache"),
            capacity_bytes: ready.chunk.length,
            pending_bytes: ready.chunk.length + incoming.chunk.length - 1,
            cache_large_write: false,
        },
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    let ready_key = write_ready_cache_file(&shared, &ready);
    let ready_path = shared.path_for(&ready_key);
    let held_pin = verified(&held);
    {
        let mut inner = shared.inner.lock().unwrap();
        assert!(shared.register_allocation_locked(&mut inner, &held_pin));
        assert_eq!(inner.pending_bytes, held.chunk.length);
    }

    shared.schedule_from_write(&incoming.chunk, incoming.bytes());

    let inner = shared.inner.lock().unwrap();
    assert!(
        ready_path.exists(),
        "raw admission rejection must not delete unrelated Ready cache data"
    );
    assert!(matches!(
        inner.entries.get(&ready_key),
        Some(CacheEntry::Ready { .. })
    ));
    assert_eq!(inner.ready_bytes, ready.chunk.length);
    assert!(!inner.entries.contains_key(&CacheKey::new(
        "filesystem-a",
        "generation-1",
        &incoming.chunk,
    )));
    assert_eq!(inner.activity.pending_budget_rejects, 1);
}

#[test]
fn read_fill_drops_unpinned_resident_before_pending_reject() {
    let dir = tempfile::tempdir().unwrap();
    let resident = StagedChunk::new(OperationId::new("resident"), b"resident".to_vec());
    let incoming = StagedChunk::new(OperationId::new("incoming"), b"incoming".to_vec());
    let shared = CacheShared {
        options: cache_options(dir.path(), incoming.chunk.length),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    {
        let mut inner = shared.inner.lock().unwrap();
        let resident_key = CacheKey::new("filesystem-a", "generation-1", &resident.chunk);
        assert!(shared.install_resident_locked(
            &mut inner,
            resident_key,
            &resident.chunk,
            verified(&resident),
        ));
        assert_eq!(inner.pending_bytes, resident.chunk.length);
    }

    shared.schedule_from_read(&incoming.chunk, verified(&incoming));

    let inner = shared.inner.lock().unwrap();
    assert!(matches!(
        inner.entries.get(&CacheKey::new(
            "filesystem-a",
            "generation-1",
            &incoming.chunk,
        )),
        Some(CacheEntry::Pending { .. })
    ));
    assert!(inner.resident.as_ref().is_some_and(|resident| {
        resident.key == CacheKey::new("filesystem-a", "generation-1", &incoming.chunk)
    }));
    assert_eq!(inner.pending_bytes, incoming.chunk.length);
    assert_eq!(inner.activity.admissions, 1);
    assert_eq!(inner.activity.pending_budget_rejects, 0);
}

#[test]
fn cache_activity_counts_task_rejections() {
    let dir = tempfile::tempdir().unwrap();
    let ready = StagedChunk::new(OperationId::new("ready"), b"ready".to_vec());
    let incoming = StagedChunk::new(OperationId::new("incoming"), b"incoming".to_vec());
    let shared = CacheShared {
        options: cache_options(dir.path(), 1024 * 1024),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };
    let ready_key = write_ready_cache_file(&shared, &ready);
    let ready_path = shared.path_for(&ready_key);
    let ready_before = std::fs::read(&ready_path).unwrap();
    {
        let mut inner = shared.inner.lock().unwrap();
        inner.active_jobs = MAX_CACHE_JOBS;
    }

    shared.schedule_from_read(&incoming.chunk, verified(&incoming));

    let inner = shared.inner.lock().unwrap();
    assert_eq!(inner.activity.task_rejects, 1);
    assert_eq!(inner.activity.admissions, 0);
    assert!(matches!(
        inner.entries.get(&ready_key),
        Some(CacheEntry::Ready { .. })
    ));
    assert_eq!(inner.ready_bytes, ready.chunk.length);
    drop(inner);
    assert_eq!(
        std::fs::read(&ready_path).unwrap(),
        ready_before,
        "task rejection must not delete or rewrite existing Ready bytes"
    );
}

#[test]
fn cache_activity_splits_capacity_from_pending_budget_rejections() {
    let dir = tempfile::tempdir().unwrap();
    let zero = StagedChunk::new(OperationId::new("zero"), Vec::new());
    let too_large = StagedChunk::new(OperationId::new("too-large"), vec![1; 8]);
    let shared = CacheShared {
        options: cache_options(dir.path(), 4),
        store_id: "filesystem-a".into(),
        generation: "generation-1".into(),
        verifier: S3ChunkVerifier::new().unwrap(),
        inner: Mutex::new(CacheInner::default()),
        wake: Condvar::new(),
    };

    shared.schedule_from_write(&zero.chunk, zero.bytes());
    shared.schedule_from_read(&too_large.chunk, verified(&too_large));

    let activity = shared.activity_snapshot();
    assert_eq!(activity.capacity_rejects, 2);
    assert_eq!(activity.pending_budget_rejects, 0);
    assert_eq!(activity.admissions, 0);
}

#[test]
fn read_fill_keeps_the_verified_buffer_identity() {
    let staged = StagedChunk::new(OperationId::new("read-fill"), b"read-fill-bytes".to_vec());
    let verified = VerifiedExternalChunk::verify(&staged.chunk, staged.bytes().to_vec()).unwrap();
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
    let other = VerifiedExternalChunk::verify(&accepted.chunk, accepted.bytes().to_vec()).unwrap();
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
                access_bucket: 0,
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
                access_bucket: 0,
            },
        );
        assert!(shared.install_resident_locked(&mut inner, key, &staged.chunk, verified(&staged),));
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
                access_bucket: 0,
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
                access_bucket: 0,
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
                    bytes: VerifiedExternalChunk::verify(&staged.chunk, staged.bytes().to_vec())
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
fn s3_put_signs_payload_by_default() {
    let captured = put_against_http_fixture(
        false,
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"immutable\"\r\nConnection: close\r\n\r\n",
    );
    assert!(
        captured
            .header
            .starts_with("PUT /chunks/afs/v1/filesystem-a/generation-1/chunks/")
    );
    assert_eq!(captured.body, b"payload");
    assert!(captured.result.unwrap().external_commit.is_some());
    let content_sha = header_value(&captured.header, "x-amz-content-sha256");
    assert_eq!(
        content_sha,
        "239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5"
    );
    assert!(
        header_value(&captured.header, "authorization").contains("AWS4-HMAC-SHA256"),
        "SigV4 Authorization must still be present"
    );
}

#[test]
fn s3_put_can_use_unsigned_payload_without_dropping_sigv4_or_receipt_checks() {
    let captured = put_against_http_fixture(
        true,
        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"immutable\"\r\nConnection: close\r\n\r\n",
    );
    assert!(
        captured
            .header
            .starts_with("PUT /chunks/afs/v1/filesystem-a/generation-1/chunks/")
    );
    assert_eq!(captured.body, b"payload");
    assert_eq!(
        header_value(&captured.header, "x-amz-content-sha256"),
        "UNSIGNED-PAYLOAD"
    );
    assert!(
        header_value(&captured.header, "authorization").contains("AWS4-HMAC-SHA256"),
        "unsigned payload only changes the canonical payload hash, not request signing"
    );
    let receipt = captured.result.unwrap();
    assert_eq!(receipt.chunk, captured.chunk);
    let external = receipt.external_commit.unwrap();
    assert_eq!(external.persisted_bytes, 7);
    assert_eq!(external.verified_digest, captured.chunk.content_digest);
    assert_eq!(
        external.object_key,
        external_object_key("filesystem-a", "generation-1", &captured.chunk.id).unwrap()
    );
}

#[test]
fn unsigned_payload_put_failure_returns_no_external_receipt() {
    let captured = put_against_http_fixture(
        true,
        b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    assert_eq!(captured.body, b"payload");
    assert_eq!(
        header_value(&captured.header, "x-amz-content-sha256"),
        "UNSIGNED-PAYLOAD"
    );
    assert!(captured.result.is_err());
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
                if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
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
    let store = S3ChunkStore::with_backend(config, Arc::new(object_store::memory::InMemory::new()))
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
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.durable_acks.is_empty() && receipt.external_commit.is_some())
    );
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
