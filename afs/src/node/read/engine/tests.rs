use super::*;
use crate::{
    dfs::{CopyId, CopyState, DfsReadGrant, OperationId},
    node::chunk::{ChunkBuilder, ChunkSink, StagedChunk},
};
use std::{
    sync::{Arc, Condvar},
    time::Duration,
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
            out[op.output_offset..op.output_offset + length].copy_from_slice(&self.bytes[..length]);
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_equivalence_cold_source_lookup_is_shared() {
    struct SlowSources(R1Sources);
    impl ReadSourceProvider for SlowSources {
        fn sources_for(&self, request: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
            self.0.sources_for(request)
        }
        fn sources_for_async(
            &self,
            request: DfsChunkSourcesRequest,
        ) -> super::super::admission::ReadFuture<'_, DfsChunkSourcesReply> {
            Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(80)).await;
                self.sources_for(request)
            })
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let provider = Arc::new(SlowSources(R1Sources::default()));
    let engine = Arc::new(DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        provider.clone(),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    ));
    let batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let engine = engine.clone();
        let batch = batch.clone();
        tasks.push(tokio::spawn(async move {
            engine
                .sources_for_attempt_async(
                    &batch,
                    true,
                    Instant::now() + Duration::from_secs(2),
                    Arc::new(()),
                )
                .await
        }));
    }
    for task in tasks {
        assert!(task.await.unwrap().is_ok());
    }
    assert_eq!(
        provider.0.requests.lock().unwrap().len(),
        1,
        "concurrent cold source lookup must issue one Meta query"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_equivalence_source_query_survives_one_waiter_timeout() {
    struct Delayed {
        inner: R1Sources,
        started: std::sync::atomic::AtomicBool,
    }
    impl ReadSourceProvider for Delayed {
        fn sources_for(&self, r: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
            self.inner.sources_for(r)
        }
        fn sources_for_async(
            &self,
            r: DfsChunkSourcesRequest,
        ) -> super::super::admission::ReadFuture<'_, DfsChunkSourcesReply> {
            Box::pin(async move {
                self.started
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                self.sources_for(r)
            })
        }
    }
    struct Held(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Held {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let provider = Arc::new(Delayed {
        inner: R1Sources::default(),
        started: std::sync::atomic::AtomicBool::new(false),
    });
    let engine = Arc::new(DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        provider.clone(),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    ));
    let batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
    let retired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let e = engine.clone();
    let b = batch.clone();
    let hold = Arc::new(Held(retired.clone()));
    let first = tokio::spawn(async move {
        e.sources_for_attempt_async(&b, true, Instant::now() + Duration::from_millis(25), hold)
            .await
    });
    while !provider.started.load(std::sync::atomic::Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    assert!(first.await.unwrap().is_err());
    assert!(!retired.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        engine
            .sources_for_attempt_async(
                &batch,
                true,
                Instant::now() + Duration::from_secs(2),
                Arc::new(())
            )
            .await
            .is_ok()
    );
    assert_eq!(provider.inner.requests.lock().unwrap().len(), 1);
    assert!(retired.load(std::sync::atomic::Ordering::SeqCst));
    let mut other = batch.clone();
    other.file_version_id = Some(FileVersionId::new("other-version"));
    assert!(
        engine
            .sources_for_attempt_async(
                &other,
                true,
                Instant::now() + Duration::from_secs(2),
                Arc::new(())
            )
            .await
            .is_ok()
    );
    assert_eq!(
        provider.inner.requests.lock().unwrap().len(),
        2,
        "different immutable views must not share source replies"
    );
}

#[test]
fn read_equivalence_bounded_reply_pool_reuses_a_concurrent_working_set() {
    struct AsyncTransfer;
    impl ChunkTransfer for AsyncTransfer {
        fn supports_async_native_read(&self) -> bool {
            true
        }
        fn read_ranges(&self, _: &PeerReadBatch, _: &mut [u8]) -> Result<()> {
            Ok(())
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::metrics::work::ReadWorkMetrics::register(&registry).unwrap();
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        Arc::new(StaticSources::default()),
        Arc::new(AsyncTransfer),
        DfsReadConfig::default(),
    )
    .with_read_pipeline(crate::node::read::admission::ReadPipelineLimits {
        ..Default::default()
    })
    .unwrap();
    for _ in 0..32 {
        let buffers: Vec<_> = (0..4)
            .map(|_| {
                engine
                    .reply_buffer_pool
                    .take(1024 * 1024, Some(metrics.clone()))
                    .unwrap()
            })
            .collect();
        drop(buffers);
    }
    let text = afs_metrics::encode_text(&registry).unwrap();
    let allocated: f64 = text
        .lines()
        .find(|line| {
            line.starts_with("afs_dfs_read_buffer_bytes_total{event=\"allocate\",role=\"reply\"}")
        })
        .unwrap()
        .rsplit(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        allocated,
        4.0 * 1024.0 * 1024.0,
        "concurrent working set must not be reallocated per batch"
    );
}

#[derive(Default)]
struct PipelineProbe {
    state: Mutex<PipelineProbeState>,
    wake: Condvar,
}

#[derive(Default)]
struct PipelineProbeState {
    head_hash_started: bool,
    head_hash_released: bool,
    tail_read_before_head_release: bool,
    hash_finished: bool,
}

impl PipelineProbe {
    fn mark_head_hash_started_and_wait(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut state = self.state.lock().unwrap();
        state.head_hash_started = true;
        self.wake.notify_all();
        while !state.head_hash_released {
            let now = Instant::now();
            assert!(now < deadline, "tail read did not release head hash");
            let timeout = deadline.saturating_duration_since(now);
            let (next, _) = self.wake.wait_timeout(state, timeout).unwrap();
            state = next;
        }
    }

    fn mark_tail_read_and_release_head(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut state = self.state.lock().unwrap();
        while !state.head_hash_started {
            let now = Instant::now();
            assert!(now < deadline, "head hash did not start before tail read");
            let timeout = deadline.saturating_duration_since(now);
            let (next, _) = self.wake.wait_timeout(state, timeout).unwrap();
            state = next;
        }
        state.tail_read_before_head_release = !state.head_hash_released;
        state.head_hash_released = true;
        self.wake.notify_all();
    }

    fn release_head_hash(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut state = self.state.lock().unwrap();
        while !state.head_hash_started {
            let now = Instant::now();
            assert!(now < deadline, "head hash did not start before release");
            let timeout = deadline.saturating_duration_since(now);
            let (next, _) = self.wake.wait_timeout(state, timeout).unwrap();
            state = next;
        }
        state.head_hash_released = true;
        self.wake.notify_all();
    }

    fn mark_hash_finished(&self) {
        let mut state = self.state.lock().unwrap();
        state.hash_finished = true;
        self.wake.notify_all();
    }

    fn tail_read_before_head_release(&self) -> bool {
        self.state.lock().unwrap().tail_read_before_head_release
    }

    fn hash_finished(&self) -> bool {
        self.state.lock().unwrap().hash_finished
    }
}

fn two_worker_pool() -> rayon_core::ThreadPool {
    rayon_core::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap()
}

#[test]
fn ready_pipeline_reads_tail_while_head_hash_is_in_flight() {
    let payload = (0..(2 * 1024 * 1024 + 1024))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let staged = StagedChunk::new(OperationId::new("ready-overlap"), payload.clone());
    let pool = two_worker_pool();
    let probe = Arc::new(PipelineProbe::default());
    let loader_probe = probe.clone();
    let hash_probe = probe.clone();

    let verified = VerifiedExternalChunk::load_with_rayon_pool(
        &staged.chunk,
        vec![0; payload.len()],
        &pool,
        0,
        |offset, segment, _tail| {
            if offset == 1024 * 1024 {
                loader_probe.mark_tail_read_and_release_head();
            }
            let offset = usize::try_from(offset).unwrap();
            segment.copy_from_slice(&payload[offset..offset + segment.len()]);
            Ok(())
        },
        || Ok(()),
        move |offset| {
            if offset == 0 {
                hash_probe.mark_head_hash_started_and_wait();
            }
        },
    )
    .unwrap();

    assert_eq!(verified.as_slice(), staged.bytes());
    assert!(
        probe.tail_read_before_head_release(),
        "tail read must start while the head hash task is still in flight"
    );
}

#[test]
fn ready_pipeline_overlaps_each_hash_with_next_segment_read() {
    const MIB: usize = 1024 * 1024;
    let payload = (0..(4 * MIB))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let pool = two_worker_pool();

    for target_offset in [0, MIB, 2 * MIB] {
        let staged = StagedChunk::new(
            OperationId::new(format!("ready-overlap-{target_offset}")),
            payload.clone(),
        );
        let probe = Arc::new(PipelineProbe::default());
        let loader_probe = probe.clone();
        let hash_probe = probe.clone();

        let verified = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, _tail| {
                if offset == (target_offset + MIB) as u64 {
                    loader_probe.mark_tail_read_and_release_head();
                }
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Ok(()),
            move |offset| {
                if offset == target_offset as u64 {
                    hash_probe.mark_head_hash_started_and_wait();
                }
            },
        )
        .unwrap();

        assert_eq!(verified.as_slice(), staged.bytes());
        assert!(
            probe.tail_read_before_head_release(),
            "read of the next segment must overlap blocked hash at offset {target_offset}"
        );
    }
}

#[test]
fn ready_pipeline_waits_for_head_hash_after_next_read_error() {
    let payload = (0..(2 * 1024 * 1024 + 1024))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let staged = StagedChunk::new(OperationId::new("ready-tail-error"), payload.clone());
    let pool = two_worker_pool();
    let probe = Arc::new(PipelineProbe::default());
    let loader_probe = probe.clone();
    let hash_probe = probe.clone();

    let error = VerifiedExternalChunk::load_with_rayon_pool(
        &staged.chunk,
        vec![0; payload.len()],
        &pool,
        0,
        |offset, segment, _tail| {
            if offset == 1024 * 1024 {
                loader_probe.release_head_hash();
                return Err(unavailable("injected next read failure"));
            }
            let offset = usize::try_from(offset).unwrap();
            segment.copy_from_slice(&payload[offset..offset + segment.len()]);
            Ok(())
        },
        || Ok(()),
        move |offset| {
            if offset == 0 {
                hash_probe.mark_head_hash_started_and_wait();
                hash_probe.mark_hash_finished();
            }
        },
    )
    .unwrap_err();

    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
    assert!(
        probe.hash_finished(),
        "blocked head hash worker must be released before the next read error returns"
    );
}

#[test]
fn ready_pipeline_loads_four_one_mib_segments_and_keeps_allocation() {
    const MIB: usize = 1024 * 1024;
    let payload = (0..(4 * MIB))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let staged = StagedChunk::new(OperationId::new("ready-four-segments"), payload.clone());
    let pool = two_worker_pool();
    let buffer = vec![0; payload.len()];
    let buffer_ptr = buffer.as_ptr();
    let buffer_capacity = buffer.capacity();
    let loads = Arc::new(Mutex::new(Vec::new()));
    let observed_loads = loads.clone();

    let verified = VerifiedExternalChunk::load_with_rayon_pool(
        &staged.chunk,
        buffer,
        &pool,
        0,
        |offset, segment, tail| {
            observed_loads
                .lock()
                .unwrap()
                .push((offset, segment.len(), tail));
            let offset = usize::try_from(offset).unwrap();
            segment.copy_from_slice(&payload[offset..offset + segment.len()]);
            Ok(())
        },
        || Ok(()),
        |_| {},
    )
    .unwrap();

    assert_eq!(verified.as_slice(), staged.bytes());
    assert_eq!(verified.as_slice().as_ptr(), buffer_ptr);
    assert_eq!(verified.allocation_bytes(), buffer_capacity as u64);
    assert_eq!(
        loads.lock().unwrap().as_slice(),
        &[
            (0, MIB, false),
            (MIB as u64, MIB, false),
            ((2 * MIB) as u64, MIB, false),
            ((3 * MIB) as u64, MIB, true),
        ]
    );
}

#[test]
fn ready_pipeline_accepts_empty_chunk_without_segment_loads() {
    let staged = StagedChunk::new(OperationId::new("ready-empty"), Vec::new());
    let pool = two_worker_pool();
    let finalized = Arc::new(Mutex::new(false));
    let finalized_probe = finalized.clone();

    let verified = VerifiedExternalChunk::load_with_rayon_pool(
        &staged.chunk,
        Vec::new(),
        &pool,
        0,
        |_offset, _segment, _tail| panic!("empty ready chunk must not load a segment"),
        || {
            *finalized_probe.lock().unwrap() = true;
            Ok(())
        },
        |_| {},
    )
    .unwrap();

    assert!(verified.as_slice().is_empty());
    assert!(*finalized.lock().unwrap());
}

#[test]
fn ready_pipeline_loads_short_tail_after_full_segment() {
    const MIB: usize = 1024 * 1024;
    let payload = (0..(MIB + 123))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let staged = StagedChunk::new(OperationId::new("ready-short-tail"), payload.clone());
    let pool = two_worker_pool();
    let loads = Arc::new(Mutex::new(Vec::new()));
    let observed_loads = loads.clone();

    let verified = VerifiedExternalChunk::load_with_rayon_pool(
        &staged.chunk,
        vec![0; payload.len()],
        &pool,
        0,
        |offset, segment, tail| {
            observed_loads
                .lock()
                .unwrap()
                .push((offset, segment.len(), tail));
            let offset = usize::try_from(offset).unwrap();
            segment.copy_from_slice(&payload[offset..offset + segment.len()]);
            Ok(())
        },
        || Ok(()),
        |_| {},
    )
    .unwrap();

    assert_eq!(verified.as_slice(), staged.bytes());
    assert_eq!(
        loads.lock().unwrap().as_slice(),
        &[(0, MIB, false), (MIB as u64, 123, true)]
    );
}

#[test]
fn ready_pipeline_rejects_corrupt_middle_and_final_segments() {
    const MIB: usize = 1024 * 1024;
    let payload = (0..(4 * MIB))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let pool = two_worker_pool();

    for corrupt_offset in [MIB, 3 * MIB] {
        let staged = StagedChunk::new(
            OperationId::new(format!("ready-corrupt-{corrupt_offset}")),
            payload.clone(),
        );
        let error = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, _tail| {
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                if offset == corrupt_offset {
                    segment[0] ^= 0x55;
                }
                Ok(())
            },
            || Ok(()),
            |_| {},
        )
        .unwrap_err();

        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    }
}

#[test]
fn ready_pipeline_joins_accepted_hash_before_late_read_failure_returns() {
    const MIB: usize = 1024 * 1024;
    let payload = (0..(4 * MIB))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let staged = StagedChunk::new(OperationId::new("ready-late-read-failure"), payload.clone());
    let pool = two_worker_pool();
    let probe = Arc::new(PipelineProbe::default());
    let loader_probe = probe.clone();
    let hash_probe = probe.clone();

    let error = VerifiedExternalChunk::load_with_rayon_pool(
        &staged.chunk,
        vec![0; payload.len()],
        &pool,
        0,
        |offset, segment, _tail| {
            if offset == (3 * MIB) as u64 {
                loader_probe.mark_tail_read_and_release_head();
                return Err(unavailable("injected late ready read failure"));
            }
            let offset = usize::try_from(offset).unwrap();
            segment.copy_from_slice(&payload[offset..offset + segment.len()]);
            Ok(())
        },
        || Ok(()),
        move |offset| {
            if offset == (2 * MIB) as u64 {
                hash_probe.mark_head_hash_started_and_wait();
                hash_probe.mark_hash_finished();
            }
        },
    )
    .unwrap_err();

    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
    assert!(
        probe.tail_read_before_head_release(),
        "failing read must wait for the middle hash worker to start before releasing it"
    );
    assert!(
        probe.hash_finished(),
        "accepted hash worker must be released and joined before the late read error returns"
    );
}

#[test]
fn ready_pipeline_rejects_reader_growth_after_exact_length() {
    let payload = (0..(2 * 1024 * 1024 + 1024))
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let staged = StagedChunk::new(OperationId::new("ready-eof-probe"), payload.clone());
    let pool = two_worker_pool();

    let error = VerifiedExternalChunk::load_with_rayon_pool(
        &staged.chunk,
        vec![0; payload.len()],
        &pool,
        0,
        |offset, segment, _tail| {
            let offset = usize::try_from(offset).unwrap();
            segment.copy_from_slice(&payload[offset..offset + segment.len()]);
            Ok(())
        },
        || Err(corrupt("S3 cache entry exceeds expected length")),
        |_| {},
    )
    .unwrap_err();

    assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
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
fn bounded_read_cache_replacement_expiry_and_clock_rollover_preserve_lru() {
    let mut cache = BoundedLru::new(2);
    cache.insert("a", 1);
    cache.insert("b", 2);
    assert_eq!(cache.get(&"a"), Some(&1));
    cache.insert("a", 3);
    assert_eq!(
        cache.len(),
        2,
        "replacement must not evict an unrelated entry"
    );
    assert!(cache.contains_key(&"b"));
    cache.clock = u64::MAX;
    assert_eq!(cache.get(&"a"), Some(&3));
    cache.insert("c", 4);
    assert_eq!(cache.len(), 2);
    assert!(!cache.contains_key(&"b"));
    assert_eq!(cache.remove(&"a"), Some(3));
    cache.retain(|_, value| *value != 4);
    assert!(cache.is_empty());
    assert!(
        cache.order.is_empty(),
        "retired entries must not leave recency keys"
    );
    cache.insert("d", 5);
    assert_eq!(cache.get(&"d"), Some(&5));
}

fn source_cache_lru_batch(chunk_id: ChunkId) -> ReadBatch {
    ReadBatch {
        lookahead: Vec::new(),
        file_version_id: Some(FileVersionId::new("version")),
        layout_root_id: LayoutRootId::new("layout"),
        ops: vec![ChunkReadOp {
            chunk_id,
            chunk_offset: 0,
            length: 1,
            output_offset: 0,
        }],
    }
}

fn source_cache_lru_set(index: usize) -> ChunkSources {
    let chunk_id = ChunkId::new(format!("source-cache-lru-{index}"));
    ChunkSources {
        allowed_ranges: Vec::new(),
        chunk: None,
        sources: vec![source("source-cache-lru-copy", &chunk_id, "node-b")],
        chunk_id,
    }
}

#[test]
fn p2_r1_source_cache_default_maximum_is_five_seconds() {
    assert_eq!(
        DfsReadConfig::default().source_cache_ttl,
        Duration::from_millis(5_000),
        "the source cache maximum must use the approved R-1 default"
    );
}

#[test]
fn p2_r1_source_cache_short_grant_is_usable_but_not_retained() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let mut sources = source_cache_lru_set(0);
    // The current source is still authorized, but cannot cover the fixed
    // 1000 ms safety margin for a future cached read.
    sources.sources[0].read_grant.expires_at_unix_ms = now_unix_ms().saturating_add(500);
    let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: vec![sources.clone()],
    }));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        local,
        provider.clone(),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    );

    let resolved = engine
        .sources_for(&source_cache_lru_batch(sources.chunk_id.clone()))
        .unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].chunk_id, sources.chunk_id);
    assert_eq!(*provider.calls.lock().unwrap(), 1);
    assert!(
        engine.source_cache.lock().unwrap().is_empty(),
        "a grant with less than 1000 ms remaining must not enter the source cache"
    );
}

#[test]
fn p2_r1_source_cache_oversized_metadata_is_usable_but_not_retained() {
    let temp = tempfile::tempdir().unwrap();
    let mut sources = source_cache_lru_set(0);
    // Use actual content length so cloning cannot shrink the oversized
    // allocation below the approved 4 MiB resident metadata limit.
    sources.sources[0].read_grant.token = "t".repeat(4 * 1024 * 1024 + 1);
    let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: vec![sources.clone()],
    }));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        provider.clone(),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    );

    let resolved = engine
        .sources_for(&source_cache_lru_batch(sources.chunk_id.clone()))
        .unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].chunk_id, sources.chunk_id);
    assert_eq!(*provider.calls.lock().unwrap(), 1);
    assert!(
        engine.source_cache.lock().unwrap().is_empty(),
        "current authorized metadata may be used, but an oversized item must not be cached"
    );
}

#[derive(Default)]
struct R1Sources {
    requests: Mutex<Vec<DfsChunkSourcesRequest>>,
    errors: Mutex<std::collections::VecDeque<Error>>,
    hooks: Mutex<std::collections::VecDeque<Box<dyn FnOnce() + Send>>>,
    node_epoch: std::sync::atomic::AtomicU64,
    device_epoch: std::sync::atomic::AtomicU64,
}

impl ReadSourceProvider for R1Sources {
    fn sources_for(&self, request: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
        let call = {
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.clone());
            requests.len()
        };
        let hook = self.hooks.lock().unwrap().pop_front();
        if let Some(hook) = hook {
            hook();
        }
        if let Some(error) = self.errors.lock().unwrap().pop_front() {
            return Err(error);
        }
        Ok(DfsChunkSourcesReply {
            revision: call as u64,
            chunks: request
                .chunk_ids
                .into_iter()
                .enumerate()
                .map(|(index, chunk_id)| {
                    let mut candidate = source("r1-copy", &chunk_id, "node-b");
                    if let CopyLocation::Node {
                        node_epoch,
                        device_epoch,
                        ..
                    } = &mut candidate.location
                    {
                        *node_epoch = self.node_epoch.load(Ordering::Relaxed).max(1);
                        *device_epoch = self.device_epoch.load(Ordering::Relaxed).max(1);
                    }
                    candidate.read_grant.file_version_id = request.file_version_id.clone();
                    candidate.read_grant.layout_root_id = request.layout_root_id.clone();
                    candidate.read_grant.namespace_id = request.namespace_id.clone();
                    candidate.read_grant.caller_node_id = request.caller_id.clone();
                    // Different signed copy fences within one reply are legal.
                    candidate.read_grant.fence = index as u64 + 1;
                    candidate.read_grant.token = format!("r1-call-{call}");
                    ChunkSources {
                        chunk_id,
                        chunk: None,
                        sources: vec![candidate],
                        allowed_ranges: vec![(7, 1)],
                    }
                })
                .collect(),
        })
    }
}

fn r1_engine(
    temp: &tempfile::TempDir,
    provider: Arc<R1Sources>,
    config: DfsReadConfig,
) -> Arc<DfsReadEngine> {
    Arc::new(DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        provider,
        Arc::new(UnimplementedChunkTransfer),
        config,
    ))
}

#[test]
fn p2_r1_metadata_lookahead_resolves_sixteen_objects_without_body_operations() {
    let temp = tempfile::tempdir().unwrap();
    let provider = Arc::new(R1Sources::default());
    let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
    let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
    batch.lookahead = (1..16)
        .map(|index| source_cache_lru_set(index).chunk_id)
        .collect();
    let before = Instant::now();
    let result = engine.sources_for(&batch).unwrap();
    assert_eq!(batch.ops.len(), 1);
    assert_eq!(
        result.len(),
        1,
        "advisory metadata is not returned as current body work"
    );
    assert_eq!(
        result[0].allowed_ranges,
        vec![(7, 1)],
        "batching must not widen grants"
    );
    assert_eq!(provider.requests.lock().unwrap()[0].chunk_ids.len(), 16);
    for index in 0..16 {
        let single = source_cache_lru_batch(source_cache_lru_set(index).chunk_id);
        assert_eq!(engine.sources_for(&single).unwrap().len(), 1);
    }
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
    let mut cache = engine.source_cache.lock().unwrap();
    assert_eq!(cache.len(), 16);
    assert!(
        cache
            .get(&source_cache_lru_key(&batch.ops[0].chunk_id))
            .unwrap()
            .expires_at
            >= before + Duration::from_secs(4)
    );
}

#[test]
fn p2_r1_zero_options_and_bounded_ids_do_not_add_speculative_work() {
    for mode in 0..3 {
        let temp = tempfile::tempdir().unwrap();
        let provider = Arc::new(R1Sources::default());
        let mut config = DfsReadConfig::default();
        match mode {
            0 => config.source_cache_ttl = Duration::ZERO,
            1 => config.source_cache_bytes = 0,
            _ => config.source_lookahead_chunks = 0,
        }
        let engine = r1_engine(&temp, provider.clone(), config);
        let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
        batch.lookahead = vec![source_cache_lru_set(1).chunk_id];
        engine.sources_for(&batch).unwrap();
        assert_eq!(provider.requests.lock().unwrap()[0].chunk_ids.len(), 1);
        let cache = engine.source_cache.lock().unwrap();
        if mode < 2 {
            assert!(cache.is_empty());
            assert_eq!(
                cache.entries.entries.capacity(),
                0,
                "disabled must not retain a hash table allocation"
            );
            assert_eq!(cache.bytes(), 0);
        } else {
            assert_eq!(
                cache.len(),
                1,
                "lookahead=0 must not disable mandatory source caching"
            );
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let provider = Arc::new(R1Sources::default());
    let engine = r1_engine(
        &temp,
        provider.clone(),
        DfsReadConfig {
            source_lookahead_chunks: MAX_DFS_READ_OPS,
            ..DfsReadConfig::default()
        },
    );
    let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
    batch.lookahead = (0..MAX_DFS_READ_OPS)
        .map(|index| source_cache_lru_set(index).chunk_id)
        .collect();
    engine.sources_for(&batch).unwrap();
    let requests = provider.requests.lock().unwrap();
    assert_eq!(requests[0].chunk_ids.len(), MAX_DFS_READ_OPS);
    assert_eq!(
        requests[0].chunk_ids[0], batch.ops[0].chunk_id,
        "mandatory first"
    );
    drop(requests);
    batch.lookahead.push(ChunkId::new("overflow"));
    assert!(engine.sources_for(&batch).is_err());
    batch.lookahead = vec![ChunkId::new("")];
    assert!(engine.sources_for(&batch).is_err());
    assert_eq!(provider.requests.lock().unwrap().len(), 1);
}

#[test]
fn p2_r1_only_precise_batch_invalidity_revalidates_mandatory_once() {
    for code in [
        afs_error::META_CATALOG_INVALID_REQUEST,
        afs_error::META_DFS_CONFLICT,
        afs_error::IO_PERMISSION_DENIED,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let provider = Arc::new(R1Sources::default());
        provider
            .errors
            .lock()
            .unwrap()
            .push_back(Error::coded(code, "batch failure"));
        let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
        let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
        batch.lookahead = vec![source_cache_lru_set(1).chunk_id];
        let result = engine.sources_for(&batch);
        let requests = provider.requests.lock().unwrap();
        if code == afs_error::META_CATALOG_INVALID_REQUEST {
            assert_eq!(result.unwrap().len(), 1);
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[1].chunk_ids, vec![batch.ops[0].chunk_id.clone()]);
        } else {
            assert_eq!(result.unwrap_err().code(), code);
            assert_eq!(
                requests.len(),
                1,
                "authorization/session failure cannot be retried as advisory"
            );
        }
    }
    for advisory in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let provider = Arc::new(R1Sources::default());
        for _ in 0..2 {
            provider.errors.lock().unwrap().push_back(Error::coded(
                afs_error::META_CATALOG_INVALID_REQUEST,
                "mandatory missing",
            ));
        }
        let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
        let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
        if advisory {
            batch.lookahead.push(source_cache_lru_set(1).chunk_id);
        }
        assert_eq!(
            engine.sources_for(&batch).unwrap_err().code(),
            afs_error::META_CATALOG_INVALID_REQUEST
        );
        assert_eq!(
            provider.requests.lock().unwrap().len(),
            if advisory { 2 } else { 1 }
        );
        assert!(engine.source_cache.lock().unwrap().is_empty());
    }
}

#[test]
fn p2_r1_oversized_advisory_is_skipped_without_rejecting_current_source() {
    let temp = tempfile::tempdir().unwrap();
    let provider = Arc::new(R1Sources::default());
    let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
    let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
    batch
        .lookahead
        .push(ChunkId::new("x".repeat(MAX_SOURCE_LOOKAHEAD_ID_BYTES + 1)));
    let result = engine.sources_for(&batch).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(batch.ops.len(), 1);
    assert_eq!(
        provider.requests.lock().unwrap()[0].chunk_ids,
        vec![batch.ops[0].chunk_id.clone()]
    );
}

#[test]
fn p2_r1_invalidation_race_discards_old_hits_and_revalidates_all_current_ids() {
    for same_scope in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let provider = Arc::new(R1Sources::default());
        let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
        let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
        engine.sources_for(&batch).unwrap();
        batch.ops.push(ChunkReadOp {
            chunk_id: source_cache_lru_set(1).chunk_id,
            chunk_offset: 0,
            length: 1,
            output_offset: 1,
        });
        let mut invalidated = batch.clone();
        if !same_scope {
            invalidated.file_version_id = Some(FileVersionId::new("other-version"));
        }
        let weak = Arc::downgrade(&engine);
        provider.hooks.lock().unwrap().push_back(Box::new(move || {
            weak.upgrade()
                .unwrap()
                .invalidate_native_batch_authority(&invalidated)
        }));
        let result = engine.sources_for(&batch).unwrap();
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[1].chunk_ids.len(),
            1,
            "first attempt only had one miss"
        );
        assert_eq!(
            requests[2].chunk_ids.len(),
            2,
            "retry must reauthorize the old cache hit too"
        );
        assert_eq!(result.len(), 2);
        assert!(
            result
                .iter()
                .all(|set| set.sources[0].read_grant.token == "r1-call-3")
        );
    }
}

#[test]
fn p2_r1_repeated_invalidation_is_bounded_unavailable_not_permission_denied() {
    let temp = tempfile::tempdir().unwrap();
    let provider = Arc::new(R1Sources::default());
    let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
    let batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
    for _ in 0..2 {
        let weak = Arc::downgrade(&engine);
        let batch = batch.clone();
        provider.hooks.lock().unwrap().push_back(Box::new(move || {
            weak.upgrade()
                .unwrap()
                .invalidate_native_batch_authority(&batch)
        }));
    }
    assert_eq!(
        engine.sources_for(&batch).unwrap_err().kind(),
        afs_error::ErrorKind::Unavailable
    );
    assert_eq!(provider.requests.lock().unwrap().len(), 2);
    assert!(engine.source_cache.lock().unwrap().is_empty());
}

#[test]
fn p2_r1_observed_remote_epochs_retire_related_sources_only() {
    for node_restart in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let first = source_cache_lru_set(0);
        let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![first.clone()],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            provider.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        engine
            .sources_for(&source_cache_lru_batch(first.chunk_id.clone()))
            .unwrap();
        let mut unrelated = source_cache_lru_set(80);
        unrelated.sources[0] = source("other-copy", &unrelated.chunk_id, "node-c");
        unrelated.sources[0].read_grant.file_version_id = FileVersionId::new("other-version");
        let unrelated_key = (
            FileVersionId::new("other-version"),
            LayoutRootId::new("layout"),
            unrelated.chunk_id.clone(),
        );
        engine.source_cache.lock().unwrap().insert(
            unrelated_key.clone(),
            CachedSources {
                expires_at: Instant::now() + Duration::from_secs(5),
                sources: unrelated,
            },
        );
        let mut fresh = source_cache_lru_set(1);
        if let CopyLocation::Node {
            node_epoch,
            device_epoch,
            ..
        } = &mut fresh.sources[0].location
        {
            if node_restart {
                *node_epoch = 2;
            } else {
                *device_epoch = 2;
            }
        }
        *provider.reply.lock().unwrap() = DfsChunkSourcesReply {
            revision: 2,
            chunks: vec![fresh.clone()],
        };
        engine
            .sources_for(&source_cache_lru_batch(fresh.chunk_id.clone()))
            .unwrap();
        let cache = engine.source_cache.lock().unwrap();
        assert!(!cache.contains_key(&source_cache_lru_key(&first.chunk_id)));
        assert!(cache.contains_key(&source_cache_lru_key(&fresh.chunk_id)));
        assert!(
            cache.contains_key(&unrelated_key),
            "other Node and version must remain cached"
        );
    }
}

#[test]
fn p2_r1_new_remote_epoch_discards_already_resolved_old_cache_hits() {
    for node_restart in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let provider = Arc::new(R1Sources::default());
        let engine = r1_engine(&temp, provider.clone(), DfsReadConfig::default());
        let mut batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
        engine.sources_for(&batch).unwrap();
        if node_restart {
            provider.node_epoch.store(2, Ordering::Relaxed);
        } else {
            provider.device_epoch.store(2, Ordering::Relaxed);
        }
        batch.ops.push(ChunkReadOp {
            chunk_id: source_cache_lru_set(1).chunk_id,
            chunk_offset: 0,
            length: 1,
            output_offset: 1,
        });
        let result = engine.sources_for(&batch).unwrap();
        assert_eq!(result.len(), 2);
        assert!(
            result.iter().flat_map(|set| &set.sources).all(|source| {
                matches!(&source.location, CopyLocation::Node { node_epoch, device_epoch, .. }
                if if node_restart { *node_epoch == 2 } else { *device_epoch == 2 })
            }),
            "a newly observed remote identity must also discard earlier cache-hit clones"
        );
        let requests = provider.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1].chunk_ids.len(), 1);
        assert_eq!(requests[2].chunk_ids.len(), 2);
        assert!(
            result
                .iter()
                .all(|set| set.sources[0].read_grant.token == "r1-call-3")
        );
    }
}

#[test]
fn p2_r1_source_deadline_consumes_rpc_time_and_hits_do_not_renew_it() {
    let temp = tempfile::tempdir().unwrap();
    let engine = r1_engine(
        &temp,
        Arc::new(R1Sources::default()),
        DfsReadConfig::default(),
    );
    let sent = Instant::now() - Duration::from_secs(2);
    let wall = now_unix_ms().saturating_sub(2_000);
    let mut set = source_cache_lru_set(0);
    set.sources[0].read_grant.expires_at_unix_ms = wall + 4_000;
    assert_eq!(
        engine.source_deadline(&set, sent, wall),
        Some(sent + Duration::from_secs(3))
    );
    set.sources[0].read_grant.expires_at_unix_ms = u64::MAX;
    assert_eq!(
        engine.source_deadline(&set, sent, wall),
        Some(sent + Duration::from_secs(5))
    );
    set.sources[0].read_grant.expires_at_unix_ms = wall + 1_000;
    assert!(engine.source_deadline(&set, sent, wall).is_none());
}

#[test]
fn p2_r1_source_cache_capacity_and_metrics_follow_real_retained_metadata() {
    let temp = tempfile::tempdir().unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::metrics::work::ReadWorkMetrics::register(&registry).unwrap();
    let local = Arc::new(
        LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_read_work_metrics(metrics),
    );
    let provider = Arc::new(R1Sources::default());
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        local,
        provider,
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    );
    let gauge = |resource: &str| -> usize {
        registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "afs_dfs_source_cache_resources")
            .unwrap()
            .get_metric()
            .iter()
            .find(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == "resource" && label.value() == resource)
            })
            .unwrap()
            .get_gauge()
            .get_value() as usize
    };
    assert_eq!(gauge("entries"), 0);
    let batch = source_cache_lru_batch(source_cache_lru_set(0).chunk_id);
    engine.sources_for(&batch).unwrap();
    let bytes = engine.source_cache.lock().unwrap().bytes();
    assert_eq!(gauge("entries"), 1);
    assert_eq!(gauge("bytes"), bytes);
    assert_eq!(gauge("peak_bytes"), bytes);
    assert_eq!(gauge("limit_bytes"), MAX_SOURCE_CACHE_BYTES);
    engine.invalidate_native_batch_authority(&batch);
    assert_eq!(gauge("entries"), 0);
    assert_eq!(gauge("bytes"), 0);
    assert_eq!(gauge("peak_bytes"), bytes);
    {
        let mut cache = engine.source_cache.lock().unwrap();
        for index in 0..8 {
            let mut set = source_cache_lru_set(index);
            set.sources[0].read_grant.token = String::with_capacity(700 * 1024);
            set.sources[0].read_grant.token.push('t');
            cache.insert(
                source_cache_lru_key(&set.chunk_id),
                CachedSources {
                    expires_at: Instant::now() + Duration::from_secs(5),
                    sources: set,
                },
            );
            assert!(cache.bytes() <= MAX_SOURCE_CACHE_BYTES);
        }
        assert!(
            cache.len() < 8,
            "spare String capacity must force bounded LRU eviction"
        );
        let mut huge = source_cache_lru_set(50);
        huge.allowed_ranges =
            Vec::with_capacity(MAX_SOURCE_CACHE_BYTES / std::mem::size_of::<(u64, u64)>() + 1);
        cache.insert(
            source_cache_lru_key(&huge.chunk_id),
            CachedSources {
                expires_at: Instant::now() + Duration::from_secs(5),
                sources: huge,
            },
        );
        assert!(!cache.contains_key(&source_cache_lru_key(&source_cache_lru_set(50).chunk_id)));
        cache.record(&engine.local);
    }
    let peak = gauge("peak_bytes");
    let generation = engine.source_cache.lock().unwrap().generation;
    let retained_entries = gauge("entries");
    drop(engine.task_reader());
    assert_eq!(engine.source_cache.lock().unwrap().generation, generation);
    assert_eq!(gauge("entries"), retained_entries);
    drop(engine);
    assert_eq!(gauge("entries"), 0);
    assert_eq!(gauge("bytes"), 0);
    assert_eq!(gauge("peak_bytes"), peak);
}

#[test]
fn p2_r1_external_metadata_uses_original_send_and_authority_deadlines() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"r1 external"));
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        fixture.sources(),
    );
    let batch = source_cache_lru_batch(fixture.chunk.id.clone());
    let selection = ExternalReadSelection {
        batch,
        selected: Vec::new(),
        validations: Vec::new(),
        missing_sources: Vec::new(),
    };
    let reads = [AuthorizedExternalRead {
        chunk: fixture.chunk.clone(),
        source: fixture.candidate.clone(),
        op: selection.batch.ops[0].clone(),
    }];
    let now = Instant::now();
    let mut timing = engine.source_cache_timing().unwrap();
    timing.sent = now - Duration::from_secs(61);
    timing.sent_wall = now_unix_ms().saturating_sub(61_000);
    engine
        .cache_authorized_external_sources(&selection, &reads, timing)
        .unwrap();
    assert!(
        engine.source_cache.lock().unwrap().is_empty(),
        "delayed installation cannot grant a fresh maximum TTL"
    );
    let mut timing = engine.source_cache_timing().unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    timing.authority_deadline = Some(deadline);
    engine
        .cache_authorized_external_sources(&selection, &reads, timing)
        .unwrap();
    let key = source_cache_lru_key(&fixture.chunk.id);
    assert!(
        engine
            .source_cache
            .lock()
            .unwrap()
            .get(&key)
            .unwrap()
            .expires_at
            <= deadline
    );
}

fn source_cache_lru_key(chunk_id: &ChunkId) -> SourceCacheKey {
    (
        FileVersionId::new("version"),
        LayoutRootId::new("layout"),
        chunk_id.clone(),
    )
}

fn fill_source_cache_lru_fixture(engine: &DfsReadEngine) {
    let expires_at = Instant::now() + Duration::from_secs(60);
    let mut cache = engine.source_cache.lock().unwrap();
    for index in 0..MAX_CACHED_SOURCES {
        let sources = source_cache_lru_set(index);
        cache.insert(
            source_cache_lru_key(&sources.chunk_id),
            CachedSources {
                expires_at,
                sources,
            },
        );
    }
}

#[test]
fn source_cache_lru_native_overflow_preserves_hot_entries_and_evicts_one() {
    let temp = tempfile::tempdir().unwrap();
    let fresh = source_cache_lru_set(MAX_CACHED_SOURCES);
    let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: vec![fresh.clone()],
    }));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        provider.clone(),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    );
    fill_source_cache_lru_fixture(&engine);
    let hot = source_cache_lru_set(0).chunk_id;
    engine
        .sources_for(&source_cache_lru_batch(hot.clone()))
        .unwrap();
    assert_eq!(*provider.calls.lock().unwrap(), 0);
    engine
        .sources_for(&source_cache_lru_batch(fresh.chunk_id.clone()))
        .unwrap();
    let cache = engine.source_cache.lock().unwrap();
    assert_eq!(
        cache.len(),
        MAX_CACHED_SOURCES,
        "overflow must not clear the cache"
    );
    assert!(cache.contains_key(&source_cache_lru_key(&hot)));
    assert!(!cache.contains_key(&source_cache_lru_key(&source_cache_lru_set(1).chunk_id)));
    assert!(cache.contains_key(&source_cache_lru_key(&fresh.chunk_id)));
    drop(cache);
    engine.sources_for(&source_cache_lru_batch(hot)).unwrap();
    assert_eq!(
        *provider.calls.lock().unwrap(),
        1,
        "hot hit must not requery Meta"
    );
}

#[test]
fn source_cache_lru_external_admission_preserves_hot_entries_and_evicts_one() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = ExternalFixture::new(b"new external source");
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        Arc::new(StaticSources::default()),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    );
    fill_source_cache_lru_fixture(&engine);
    let hot = source_cache_lru_set(0).chunk_id;
    let (cached, misses) = engine
        .cached_sources_for(&source_cache_lru_batch(hot.clone()))
        .unwrap();
    assert_eq!(cached.len(), 1);
    assert!(misses.is_empty());
    let selection = ExternalReadSelection {
        batch: source_cache_lru_batch(fixture.chunk.id.clone()),
        selected: Vec::new(),
        validations: Vec::new(),
        missing_sources: Vec::new(),
    };
    // This helper is reached only after external authorization. Its cached
    // metadata still cannot replace the authorizer or final grant checks.
    engine
        .cache_authorized_external_sources(
            &selection,
            &[AuthorizedExternalRead {
                chunk: fixture.chunk.clone(),
                source: fixture.candidate,
                op: selection.batch.ops[0].clone(),
            }],
            engine.source_cache_timing().unwrap(),
        )
        .unwrap();
    let cache = engine.source_cache.lock().unwrap();
    assert_eq!(
        cache.len(),
        MAX_CACHED_SOURCES,
        "external admission must evict incrementally"
    );
    assert!(cache.contains_key(&source_cache_lru_key(&hot)));
    assert!(!cache.contains_key(&source_cache_lru_key(&source_cache_lru_set(1).chunk_id)));
    assert!(cache.contains_key(&source_cache_lru_key(&fixture.chunk.id)));
}

#[test]
fn source_cache_lru_hits_do_not_extend_ttl_or_keep_expired_entries() {
    let temp = tempfile::tempdir().unwrap();
    let sources = source_cache_lru_set(0);
    let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: vec![sources.clone()],
    }));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        provider.clone(),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    );
    let key = source_cache_lru_key(&sources.chunk_id);
    let batch = source_cache_lru_batch(sources.chunk_id.clone());
    let deadline = Instant::now() + Duration::from_secs(30);
    engine.source_cache.lock().unwrap().insert(
        key.clone(),
        CachedSources {
            expires_at: deadline,
            sources: sources.clone(),
        },
    );
    engine.sources_for(&batch).unwrap();
    assert_eq!(
        engine
            .source_cache
            .lock()
            .unwrap()
            .get(&key)
            .unwrap()
            .expires_at,
        deadline
    );
    assert_eq!(*provider.calls.lock().unwrap(), 0);
    engine.source_cache.lock().unwrap().insert(
        key,
        CachedSources {
            expires_at: Instant::now() - Duration::from_secs(1),
            sources,
        },
    );
    engine.sources_for(&batch).unwrap();
    assert_eq!(
        *provider.calls.lock().unwrap(),
        1,
        "expired metadata must return to Meta"
    );
}

#[test]
fn reader_cache_lru_overflow_preserves_hot_pin_and_retired_arc_lifetime() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        local.clone(),
        Arc::new(StaticSources::default()),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    );
    let mut chunks = Vec::new();
    let mut retired = None;
    for index in 0..MAX_PINNED_READERS {
        let bytes = format!("reader-cache-lru-{index}");
        let chunk = put_local_chunk(&local, &bytes, bytes.as_bytes());
        let pin = engine.local_reader(&chunk).unwrap();
        if index == 1 {
            retired = Some(pin);
        }
        chunks.push(chunk);
    }
    let hot = engine.local_reader(&chunks[0]).unwrap();
    let retired = retired.unwrap();
    assert_eq!(Arc::strong_count(&retired), 2);
    let weak = Arc::downgrade(&retired);
    let fresh = put_local_chunk(&local, "reader-cache-lru-extra", b"extra");
    engine.local_reader(&fresh).unwrap();
    let cache = engine.readers.lock().unwrap();
    assert_eq!(
        cache.len(),
        MAX_PINNED_READERS,
        "overflow must retire only the coldest pin"
    );
    assert!(cache.contains_key(&chunks[0]));
    assert!(!cache.contains_key(&chunks[1]));
    assert!(cache.contains_key(&fresh));
    drop(cache);
    assert!(Arc::ptr_eq(&hot, &engine.local_reader(&chunks[0]).unwrap()));
    assert_eq!(
        Arc::strong_count(&retired),
        1,
        "cache ownership must be released on eviction"
    );
    let mut output = [0; 32];
    let length = retired.read_at(0, &mut output).unwrap();
    assert_eq!(&output[..length], b"reader-cache-lru-1");
    drop(retired);
    assert!(
        weak.upgrade().is_none(),
        "last in-flight pin owns its descriptor lifetime"
    );
}

#[test]
fn reader_cache_concurrent_misses_return_one_canonical_pin() {
    const READERS: usize = 16;
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let chunk = put_local_chunk(
        &local,
        "reader-cache-concurrent",
        &vec![b'x'; 4 * 1024 * 1024],
    );
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        local,
        Arc::new(StaticSources::default()),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    );
    *engine
        .test_hooks
        .local_reader_install_barrier
        .lock()
        .unwrap() = Some(Arc::new(std::sync::Barrier::new(READERS)));
    let pins = std::thread::scope(|scope| {
        let threads = (0..READERS)
            .map(|_| scope.spawn(|| engine.local_reader(&chunk).unwrap()))
            .collect::<Vec<_>>();
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(
        pins.iter().all(|pin| Arc::ptr_eq(pin, &pins[0])),
        "concurrent misses must reuse the installed pin"
    );
    assert_eq!(engine.readers.lock().unwrap().len(), 1);
}

#[test]
fn local_object_validation_rejects_same_id_with_different_standard_root() {
    let staged = StagedChunk::new(OperationId::new("local-root-anchor"), vec![71; 64 * 1024]);
    let mut wrong = staged.chunk.clone();
    wrong.content_digest.bytes[0] ^= 1;
    assert_eq!(wrong.id, staged.chunk.id);
    assert!(validate_local_object_identity(&staged.chunk, &wrong).is_err());
    wrong = staged.chunk.clone();
    wrong.length -= 1;
    assert!(validate_local_object_identity(&staged.chunk, &wrong).is_err());
}

#[test]
fn local_pinned_object_mismatch_never_publishes_output_or_adds_meta_rpc() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let first = put_local_chunk(&local, "trusted-layout-chunk", b"abcdef");
    let other = put_local_chunk(&local, "wrong-pinned-chunk", b"uvwxyz");
    let wrong_pin = Arc::new(local.open_verified(&other).unwrap());
    let sources = Arc::new(StaticSources::default());
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        local,
        sources.clone(),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    );
    // Fault injection: an index hit must still bind the pin to the layout's
    // independently trusted canonical root/length, never just the map key.
    engine
        .readers
        .lock()
        .unwrap()
        .insert(first.clone(), wrong_pin);
    let batch = ReadBatch {
        lookahead: Vec::new(),
        file_version_id: Some(FileVersionId::new("version")),
        layout_root_id: LayoutRootId::new("layout"),
        ops: vec![ChunkReadOp {
            chunk_id: first,
            chunk_offset: 0,
            length: 6,
            output_offset: 0,
        }],
    };
    let mut output = [b'!'; 6];
    assert!(engine.read_batch(&batch, &mut output).is_err());
    assert_eq!(output, [b'!'; 6]);
    assert_eq!(*sources.calls.lock().unwrap(), 0);
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
                lookahead: Vec::new(),
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
        lookahead: Vec::new(),
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
        lookahead: Vec::new(),
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
            allowed_ranges: Vec::new(),
            chunk: None,
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
        lookahead: Vec::new(),
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
        lookahead: Vec::new(),
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
            allowed_ranges: Vec::new(),
            chunk: None,
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
        lookahead: Vec::new(),
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
            allowed_ranges: Vec::new(),
            chunk: None,
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
        lookahead: Vec::new(),
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
            allowed_ranges: Vec::new(),
            chunk: None,
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
                lookahead: Vec::new(),
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

#[test]
fn characterize_quarantine_has_taken_effect_when_detecting_read_returns() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let chunk_id = put_local_chunk(&local, "quarantine-timing", b"abcdef");
    std::fs::write(temp.path().join("chunks").join(&chunk_id.0), b"abXdef").unwrap();
    let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: vec![ChunkSources {
            allowed_ranges: Vec::new(),
            chunk: None,
            chunk_id: chunk_id.clone(),
            sources: vec![source("healthy-b", &chunk_id, "node-b")],
        }],
    }));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        local.clone(),
        sources,
        Arc::new(StaticTransfer {
            bytes: b"abcdef".to_vec(),
            fail_first: Mutex::new(false),
        }),
        DfsReadConfig::default(),
    );
    assert!(local.quarantined_chunks().unwrap().is_empty());
    let mut out = [9; 6];
    engine
        .read_batch(
            &ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(FileVersionId::new("version")),
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 0,
                    length: 6,
                    output_offset: 0,
                }],
            },
            &mut out,
        )
        .unwrap();
    assert_eq!(&out, b"abcdef");
    // No wait and no background worker: the read that served the healthy
    // peer bytes has already moved the local copy to Quarantined.
    assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
    assert!(engine.readers.lock().unwrap().is_empty());
    let mut local_out = [0; 6];
    assert!(local.read_at(&chunk_id, 0, &mut local_out).is_err());
}

/// Records every peer attempt as the copy ids of its batch.
struct RotationTransfer {
    failing: Vec<&'static str>,
    denied: Vec<&'static str>,
    corrupt: Vec<&'static str>,
    attempts: Mutex<Vec<Vec<String>>>,
    reviews: Mutex<Vec<crate::node::fault::suspect::SuspectCopy>>,
}

impl RotationTransfer {
    fn new(failing: &[&'static str], denied: &[&'static str]) -> Arc<Self> {
        Self::with_corrupt(failing, denied, &[])
    }

    fn with_corrupt(
        failing: &[&'static str],
        denied: &[&'static str],
        corrupt: &[&'static str],
    ) -> Arc<Self> {
        Arc::new(Self {
            failing: failing.to_vec(),
            denied: denied.to_vec(),
            corrupt: corrupt.to_vec(),
            attempts: Mutex::new(Vec::new()),
            reviews: Mutex::new(Vec::new()),
        })
    }

    fn attempts(&self) -> Vec<Vec<String>> {
        self.attempts.lock().unwrap().clone()
    }
}

impl ChunkTransfer for RotationTransfer {
    fn review_suspect_chunk_async<'a>(
        &'a self,
        suspect: &'a crate::node::fault::suspect::SuspectCopy,
    ) -> crate::node::read::admission::ReadFuture<'a, bool> {
        self.reviews.lock().unwrap().push(suspect.clone());
        Box::pin(async { Ok(true) })
    }
    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
        let copies: Vec<String> = batch
            .operations
            .iter()
            .map(|item| item.source.copy_id.0.clone())
            .collect();
        self.attempts.lock().unwrap().push(copies.clone());
        if copies
            .iter()
            .any(|copy| self.denied.contains(&copy.as_str()))
        {
            return Err(external_denied("injected rotation denial"));
        }
        if copies
            .iter()
            .any(|copy| self.failing.contains(&copy.as_str()))
        {
            return Err(unavailable("injected rotation failure"));
        }
        if copies
            .iter()
            .any(|copy| self.corrupt.contains(&copy.as_str()))
        {
            return Err(Error::coded(
                afs_error::NODE_TRANSFER_CORRUPT_DATA,
                "injected content mismatch",
            ));
        }
        for item in &batch.operations {
            let start = item.op.output_offset;
            let end = start + usize::try_from(item.op.length).unwrap();
            out[start..end].fill(b'r');
        }
        Ok(())
    }
}

fn rotation_source(copy: &str, chunk_id: &ChunkId, node: &str, role: CopyRole) -> SourceCandidate {
    let mut candidate = source(copy, chunk_id, node);
    candidate.role = role;
    candidate
}

fn rotation_read(
    chunks: Vec<(ChunkId, Vec<SourceCandidate>)>,
    transfer: Arc<RotationTransfer>,
    out: &mut [u8],
) -> Result<()> {
    rotation_read_reviewing(chunks, transfer, out, false)
}

/// `review`: after the read, run one suspect review pass.
fn rotation_read_reviewing(
    chunks: Vec<(ChunkId, Vec<SourceCandidate>)>,
    transfer: Arc<RotationTransfer>,
    out: &mut [u8],
    review: bool,
) -> Result<()> {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let length = out.len() / chunks.len();
    let ops = chunks
        .iter()
        .enumerate()
        .map(|(index, (chunk_id, _))| ChunkReadOp {
            chunk_id: chunk_id.clone(),
            chunk_offset: 0,
            length: u64::try_from(length).unwrap(),
            output_offset: index * length,
        })
        .collect();
    let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: chunks
            .into_iter()
            .map(|(chunk_id, sources)| ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id,
                sources,
            })
            .collect(),
    }));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        local,
        sources,
        transfer,
        DfsReadConfig::default(),
    );
    let result = engine.read_batch(
        &ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops,
        },
        out,
    );
    if review {
        crate::node::bridge::block_on_scoped_runtime(engine.review_suspect_copies(8)).unwrap();
    }
    result
}

/// R59：对端字节没通过本端内容校验时，请持有方复核一次（读照常换下一个副本成功）；
/// 传输错误不请复核。
#[test]
fn a_content_mismatch_asks_the_holder_to_review_once_and_a_transport_error_does_not() {
    let chunk = ChunkId::new("suspect");
    let transfer = RotationTransfer::with_corrupt(&["flaky-b"], &[], &["bad-c"]);
    let mut out = [0; 4];
    rotation_read_reviewing(
        vec![(
            chunk.clone(),
            vec![
                source("flaky-b", &chunk, "node-b"),
                source("bad-c", &chunk, "node-c"),
                source("good-d", &chunk, "node-d"),
            ],
        )],
        transfer.clone(),
        &mut out,
        true,
    )
    .unwrap();
    assert_eq!(out, [b'r'; 4]);
    let reviews = transfer.reviews.lock().unwrap().clone();
    assert_eq!(reviews.len(), 1, "{reviews:?}");
    assert_eq!(reviews[0].node_id, "node-c");
    assert_eq!(reviews[0].chunk_id, chunk);
    assert_eq!(reviews[0].endpoint, "http://node-c");
}

/// R59：批量失败不知道是哪个副本，只在逐个单试确认后才请复核。
#[test]
fn a_batch_failure_is_attributed_only_by_the_single_retry() {
    let (first, second) = (ChunkId::new("first"), ChunkId::new("second"));
    let transfer = RotationTransfer::with_corrupt(&[], &[], &["bad-b2"]);
    let mut out = [0; 8];
    rotation_read_reviewing(
        vec![
            (first.clone(), vec![source("good-b1", &first, "node-b")]),
            (
                second.clone(),
                vec![
                    source("bad-b2", &second, "node-b"),
                    source("good-c2", &second, "node-c"),
                ],
            ),
        ],
        transfer.clone(),
        &mut out,
        true,
    )
    .unwrap();
    let reviews = transfer.reviews.lock().unwrap().clone();
    assert_eq!(reviews.len(), 1, "{reviews:?}");
    assert_eq!(reviews[0].chunk_id, second);
}

#[test]
fn characterize_read_rotation_follows_meta_order_without_preferring_durable_over_cache() {
    let chunk = ChunkId::new("rotation");
    let transfer = RotationTransfer::new(&["cache-b", "durable-c"], &[]);
    let mut out = [0; 4];
    rotation_read(
        vec![(
            chunk.clone(),
            vec![
                rotation_source("cache-b", &chunk, "node-b", CopyRole::VerifiedCache),
                rotation_source("durable-c", &chunk, "node-c", CopyRole::DurableReplica),
                rotation_source("cache-d", &chunk, "node-d", CopyRole::VerifiedCache),
                rotation_source("durable-e", &chunk, "node-e", CopyRole::DurableReplica),
            ],
        )],
        transfer.clone(),
        &mut out,
    )
    .unwrap();
    assert_eq!(out, [b'r'; 4]);
    assert_eq!(
        transfer.attempts(),
        vec![vec!["cache-b"], vec!["durable-c"], vec!["cache-d"]]
    );
}

#[test]
fn characterize_read_rotation_skips_unreadable_local_and_external_candidates() {
    let chunk = ChunkId::new("rotation-filter");
    let mut corrupt = source("corrupt-b", &chunk, "node-b");
    corrupt.state = CopyState::Corrupt;
    let transfer = RotationTransfer::new(&[], &[]);
    let mut out = [0; 4];
    rotation_read(
        vec![(
            chunk.clone(),
            vec![
                corrupt,
                source("local-a", &chunk, "node-a"),
                rotation_source("external-c", &chunk, "node-c", CopyRole::ExternalCommitted),
                source("ready-d", &chunk, "node-d"),
            ],
        )],
        transfer.clone(),
        &mut out,
    )
    .unwrap();
    assert_eq!(transfer.attempts(), vec![vec!["ready-d"]]);
}

#[test]
fn characterize_read_rotation_tries_each_copy_once_and_returns_last_error_when_exhausted() {
    let chunk = ChunkId::new("rotation-exhausted");
    let transfer = RotationTransfer::new(&["copy-b", "copy-c"], &[]);
    let mut out = [9; 4];
    let error = rotation_read(
        vec![(
            chunk.clone(),
            vec![
                source("copy-b", &chunk, "node-b"),
                source("copy-c", &chunk, "node-c"),
            ],
        )],
        transfer.clone(),
        &mut out,
    )
    .unwrap_err();
    assert!(error.to_string().contains("injected rotation failure"));
    assert_eq!(transfer.attempts(), vec![vec!["copy-b"], vec!["copy-c"]]);
    assert_eq!(out, [9; 4]);
}

#[test]
fn characterize_read_rotation_stops_at_first_denial() {
    let chunk = ChunkId::new("rotation-denied");
    let transfer = RotationTransfer::new(&[], &["copy-b"]);
    let mut out = [9; 4];
    let error = rotation_read(
        vec![(
            chunk.clone(),
            vec![
                source("copy-b", &chunk, "node-b"),
                source("copy-c", &chunk, "node-c"),
            ],
        )],
        transfer.clone(),
        &mut out,
    )
    .unwrap_err();
    assert_eq!(error.kind(), afs_error::ErrorKind::PermissionDenied);
    assert_eq!(transfer.attempts(), vec![vec!["copy-b"]]);
    assert_eq!(out, [9; 4]);
}

#[test]
fn characterize_read_rotation_retries_batch_singly_on_same_copy_before_advancing() {
    let first = ChunkId::new("rotation-first");
    let second = ChunkId::new("rotation-second");
    let transfer = RotationTransfer::new(&["b1", "b2"], &[]);
    let mut out = [0; 8];
    rotation_read(
        vec![
            (
                first.clone(),
                vec![
                    source("b1", &first, "node-b"),
                    source("c1", &first, "node-c"),
                ],
            ),
            (
                second.clone(),
                vec![
                    source("b2", &second, "node-b"),
                    source("c2", &second, "node-c"),
                ],
            ),
        ],
        transfer.clone(),
        &mut out,
    )
    .unwrap();
    assert_eq!(out, [b'r'; 8]);
    assert_eq!(
        transfer.attempts(),
        vec![vec!["b1", "b2"], vec!["b1"], vec!["b2"], vec!["c1", "c2"],]
    );
}

#[test]
fn native_authority_snapshot_keeps_original_deadline_and_stops_on_epoch_change() {
    let authority = LocalReadAuthority::new(7, Instant::now() + Duration::from_secs(10));
    let original = authority.snapshot().unwrap();
    authority
        .refresh(7, Instant::now() + Duration::from_secs(30))
        .unwrap();
    assert!(authority.snapshot().unwrap().expires_at() > original.expires_at());
    assert!(authority.check_snapshot(&original).is_ok());
    let expired = NativeReadAuthoritySnapshot {
        epoch: 7,
        expires_at: Some(Instant::now()),
    };
    assert!(authority.check_snapshot(&expired).is_err());
    assert!(
        authority
            .refresh(8, Instant::now() + Duration::from_secs(30))
            .is_err()
    );
    assert!(authority.check_snapshot(&original).is_err());
    assert!(
        authority
            .refresh(7, Instant::now() + Duration::from_secs(30))
            .is_err()
    );
}

#[test]
fn native_authority_refresh_cannot_revive_expired_or_stopped_session() {
    for stopped in [false, true] {
        let authority = LocalReadAuthority::new(
            1,
            if stopped {
                Instant::now() + Duration::from_secs(30)
            } else {
                Instant::now()
            },
        );
        if stopped {
            authority.stop();
        }
        assert!(
            authority
                .refresh(1, Instant::now() + Duration::from_secs(30))
                .is_err()
        );
        assert!(authority.snapshot().is_err());
    }
}

#[test]
fn native_authority_stopped_session_rejects_local_and_sparse_reads_without_output() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let chunk_id = put_local_chunk(&local, "native-stop", b"abcdef");
    let authority = Arc::new(LocalReadAuthority::new(
        1,
        Instant::now() + Duration::from_secs(30),
    ));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        local,
        Arc::new(UnimplementedReadSourceProvider),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    )
    .with_read_authority(authority.clone());
    let mut batch = ReadBatch {
        lookahead: Vec::new(),
        file_version_id: Some(FileVersionId::new("version")),
        layout_root_id: LayoutRootId::new("layout"),
        ops: vec![ChunkReadOp {
            chunk_id,
            chunk_offset: 0,
            length: 6,
            output_offset: 0,
        }],
    };
    assert_eq!(
        engine.read_batch_native_owned(&batch, 6).unwrap().unwrap(),
        b"abcdef"
    );
    authority.stop();
    for sparse in [false, true] {
        if sparse {
            batch.ops.clear();
        }
        let mut output = [b'!'; 6];
        assert!(engine.read_batch(&batch, &mut output).is_err());
        assert_eq!(output, [b'!'; 6]);
        if !sparse {
            assert!(engine.read_batch_native_owned(&batch, 6).is_err());
        }
    }
}

struct NativeAuthorityTransfer {
    authority: Arc<LocalReadAuthority>,
    action: &'static str,
}

impl ChunkTransfer for NativeAuthorityTransfer {
    fn read_ranges(&self, _batch: &PeerReadBatch, output: &mut [u8]) -> Result<()> {
        output.fill(b'x');
        match self.action {
            "stop" => self.authority.stop(),
            "renew_then_expire" => {
                let deadline = self.authority.snapshot()?.expires_at().unwrap();
                self.authority
                    .refresh(1, Instant::now() + Duration::from_secs(30))?;
                std::thread::sleep(
                    deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
                );
            }
            "expire_then_refresh" => {
                let deadline = self.authority.snapshot()?.expires_at().unwrap();
                std::thread::sleep(
                    deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(1),
                );
                let _ = self
                    .authority
                    .refresh(1, Instant::now() + Duration::from_secs(30));
            }
            "epoch" => {
                let _ = self
                    .authority
                    .refresh(2, Instant::now() + Duration::from_secs(30));
            }
            "denied" => return Err(external_denied("injected known denial")),
            "unavailable" => return Err(unavailable("injected ordinary transport failure")),
            _ => {}
        }
        Ok(())
    }
}

#[test]
fn native_authority_late_failure_never_commits_borrowed_or_owned_output() {
    for action in [
        "stop",
        "expire_then_refresh",
        "renew_then_expire",
        "epoch",
        "denied",
        "unavailable",
    ] {
        for owned in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let chunk_id = ChunkId::new("missing-authority");
            let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
                revision: 1,
                chunks: vec![ChunkSources {
                    allowed_ranges: Vec::new(),
                    chunk: None,
                    chunk_id: chunk_id.clone(),
                    sources: vec![source("b", &chunk_id, "node-b")],
                }],
            }));
            // Opening the durable store can fsync; do it before starting
            // the short expiry interval exercised inside the transfer.
            let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
            let authority = Arc::new(LocalReadAuthority::new(
                1,
                Instant::now()
                    + if matches!(action, "expire_then_refresh" | "renew_then_expire") {
                        Duration::from_millis(250)
                    } else {
                        Duration::from_secs(30)
                    },
            ));
            let engine = DfsReadEngine::new(
                crate::dfs::NamespaceId::new("default"),
                "node-a".into(),
                local,
                sources,
                Arc::new(NativeAuthorityTransfer {
                    authority: authority.clone(),
                    action,
                }),
                DfsReadConfig::default(),
            )
            .with_read_authority(authority.clone());
            let batch = ReadBatch {
                lookahead: Vec::new(),
                file_version_id: Some(FileVersionId::new("version")),
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![ChunkReadOp {
                    chunk_id,
                    chunk_offset: 0,
                    length: 6,
                    output_offset: 0,
                }],
            };
            if owned {
                assert!(
                    engine.read_batch_native_owned(&batch, 6).is_err(),
                    "{action}"
                );
            } else {
                let mut output = [b'!'; 6];
                assert!(engine.read_batch(&batch, &mut output).is_err(), "{action}");
                assert_eq!(output, [b'!'; 6], "{action}");
            }
            assert_eq!(
                authority.snapshot().is_ok(),
                matches!(action, "unavailable" | "renew_then_expire" | "denied"),
                "{action}"
            );
        }
    }
}

struct NativeScopedDenialTransfer {
    calls: std::sync::atomic::AtomicUsize,
    unauthenticated: bool,
}

impl ChunkTransfer for NativeScopedDenialTransfer {
    fn read_ranges(&self, _batch: &PeerReadBatch, output: &mut [u8]) -> Result<()> {
        output.fill(b'x');
        if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
            return Err(if self.unauthenticated {
                Error::new(
                    afs_error::IO_PERMISSION_DENIED,
                    afs_error::ErrorKind::Unauthenticated,
                    "injected scoped grant denial",
                )
            } else {
                external_denied("injected scoped grant denial")
            });
        }
        Ok(())
    }
}

#[test]
fn native_authority_read_denial_clears_only_batch_sources_and_allows_fresh_authorization() {
    for unauthenticated in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let chunk_id = ChunkId::new("scoped-denial");
        let reply = DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id: chunk_id.clone(),
                sources: vec![source("b", &chunk_id, "node-b")],
            }],
        };
        let sources = Arc::new(StaticSources::new(reply.clone()));
        let authority = Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        ));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
            Arc::new(NativeScopedDenialTransfer {
                calls: std::sync::atomic::AtomicUsize::new(0),
                unauthenticated,
            }),
            DfsReadConfig::default(),
        )
        .with_read_authority(authority);
        let unrelated = (
            FileVersionId::new("other-version"),
            LayoutRootId::new("other-layout"),
            chunk_id.clone(),
        );
        engine.source_cache.lock().unwrap().insert(
            unrelated.clone(),
            CachedSources {
                expires_at: Instant::now() + Duration::from_secs(30),
                sources: reply.chunks[0].clone(),
            },
        );
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id,
                chunk_offset: 0,
                length: 6,
                output_offset: 0,
            }],
        };
        let mut output = [b'!'; 6];
        let error = engine.read_batch(&batch, &mut output).unwrap_err();
        assert_eq!(
            error.kind(),
            if unauthenticated {
                afs_error::ErrorKind::Unauthenticated
            } else {
                afs_error::ErrorKind::PermissionDenied
            }
        );
        assert_eq!(output, [b'!'; 6]);
        assert!(engine.capture_native_read_authority().is_ok());
        let cache = engine.source_cache.lock().unwrap();
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&unrelated));
        drop(cache);
        engine.read_batch(&batch, &mut output).unwrap();
        assert_eq!(output, [b'x'; 6]);
        assert_eq!(
            *sources.calls.lock().unwrap(),
            2,
            "rejected context must return to Meta"
        );
    }
}

#[test]
fn native_private_read_uses_existing_final_buffer_and_preserves_sparse_local_ranges() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let chunk = put_local_chunk(&local, "native-private", b"abcdef");
    let transfer = Arc::new(OwnedTransferProbe {
        destination: std::sync::atomic::AtomicUsize::new(0),
        fail_first: AtomicBool::new(false),
        fail_all: false,
        expire_before_return: false,
    });
    let missing = ChunkId::new("missing-private");
    let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: vec![ChunkSources {
            allowed_ranges: Vec::new(),
            chunk: None,
            chunk_id: missing.clone(),
            sources: vec![source("b", &missing, "node-b")],
        }],
    }));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        local,
        sources,
        transfer.clone(),
        DfsReadConfig::default(),
    );
    let mut batch = ReadBatch {
        lookahead: Vec::new(),
        file_version_id: Some(FileVersionId::new("version")),
        layout_root_id: LayoutRootId::new("layout"),
        ops: vec![ChunkReadOp {
            chunk_id: missing,
            chunk_offset: 0,
            length: 6,
            output_offset: 0,
        }],
    };
    let mut private = [0; 6];
    engine
        .read_batch_native_into_private(&batch, &mut private)
        .unwrap();
    assert_eq!(private, [b'x'; 6]);
    assert_eq!(
        transfer.destination.load(Ordering::Relaxed),
        private.as_ptr() as usize,
        "private helper must not allocate another aggregate staging destination"
    );
    batch.ops = vec![
        ChunkReadOp {
            chunk_id: chunk.clone(),
            chunk_offset: 4,
            length: 2,
            output_offset: 4,
        },
        ChunkReadOp {
            chunk_id: chunk,
            chunk_offset: 0,
            length: 2,
            output_offset: 1,
        },
    ];
    private.fill(0);
    engine
        .read_batch_native_into_private(&batch, &mut private)
        .unwrap();
    assert_eq!(&private, b"\0ab\0ef");
}

#[test]
fn native_private_read_rejects_overlap_and_budget_before_work() {
    let temp = tempfile::tempdir().unwrap();
    let sources = Arc::new(StaticSources::default());
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        sources.clone(),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig {
            max_inflight_bytes: 6,
            ..DfsReadConfig::default()
        },
    );
    assert!(engine.validate_native_read_size(7).is_err());
    assert!(engine.validate_native_read_size(6).is_ok());
    let batch = ReadBatch {
        lookahead: Vec::new(),
        file_version_id: Some(FileVersionId::new("version")),
        layout_root_id: LayoutRootId::new("layout"),
        ops: vec![
            ChunkReadOp {
                chunk_id: ChunkId::new("one"),
                chunk_offset: 0,
                length: 2,
                output_offset: 1,
            },
            ChunkReadOp {
                chunk_id: ChunkId::new("two"),
                chunk_offset: 0,
                length: 2,
                output_offset: 2,
            },
        ],
    };
    let mut private = [b'!'; 6];
    assert!(
        engine
            .read_batch_native_into_private(&batch, &mut private)
            .is_err()
    );
    assert_eq!(private, [b'!'; 6]);
    assert_eq!(*sources.calls.lock().unwrap(), 0);
}

#[test]
fn native_private_read_late_stop_returns_error_before_private_output_publication() {
    let temp = tempfile::tempdir().unwrap();
    let chunk_id = ChunkId::new("late-private");
    let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: vec![ChunkSources {
            allowed_ranges: Vec::new(),
            chunk: None,
            chunk_id: chunk_id.clone(),
            sources: vec![source("b", &chunk_id, "node-b")],
        }],
    }));
    let authority = Arc::new(LocalReadAuthority::new(
        1,
        Instant::now() + Duration::from_secs(30),
    ));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        sources,
        Arc::new(NativeAuthorityTransfer {
            authority: authority.clone(),
            action: "stop",
        }),
        DfsReadConfig::default(),
    )
    .with_read_authority(authority);
    let batch = ReadBatch {
        lookahead: Vec::new(),
        file_version_id: Some(FileVersionId::new("version")),
        layout_root_id: LayoutRootId::new("layout"),
        ops: vec![ChunkReadOp {
            chunk_id,
            chunk_offset: 0,
            length: 6,
            output_offset: 0,
        }],
    };
    let mut private = [0; 6];
    assert!(
        engine
            .read_batch_native_into_private(&batch, &mut private)
            .is_err()
    );
    assert_eq!(
        private, [b'x'; 6],
        "unpublished private payload is discarded by its caller"
    );
}

struct NativeCacheTransfer {
    bytes: Mutex<Vec<u8>>,
    batches: Mutex<Vec<PeerReadBatch>>,
    corrupt: AtomicBool,
    delay: Duration,
}

impl ChunkTransfer for NativeCacheTransfer {
    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
        batch.validate(out.len())?;
        self.batches.lock().unwrap().push(batch.clone());
        let bytes = self.bytes.lock().unwrap();
        for item in &batch.operations {
            let start = item.op.chunk_offset as usize;
            let length = item.op.length as usize;
            out[item.op.output_offset..item.op.output_offset + length]
                .copy_from_slice(&bytes[start..start + length]);
        }
        if self.corrupt.load(Ordering::Relaxed) {
            let last = out.len() - 1;
            out[last] ^= 1;
        }
        std::thread::sleep(self.delay);
        Ok(())
    }
}

struct NativeCacheFixture {
    _temp: tempfile::TempDir,
    engine: DfsReadEngine,
    cache: Arc<crate::node::read::peer_cache::DiskPeerCache>,
    sources: Arc<StaticSources>,
    transfer: Arc<NativeCacheTransfer>,
    batch: ReadBatch,
    chunk: ChunkObject,
}

impl NativeCacheFixture {
    fn new(max_inflight_bytes: u64, ranges: Vec<(u64, u64)>, delay: Duration) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let bytes = (0..64 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("native-disk-cache"), bytes.clone());
        let chunk = staged.chunk.clone();
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: ranges,
                chunk: Some(chunk.clone()),
                chunk_id: chunk.id.clone(),
                sources: vec![source("b", &chunk.id, "node-b")],
            }],
        }));
        let cache = Arc::new(
            crate::node::read::peer_cache::DiskPeerCache::open(
                crate::node::read::peer_cache::PeerCacheOptions {
                    directory: temp.path().join("cache"),
                    capacity_bytes: 2 * 1024 * 1024,
                    namespace_id: crate::dfs::NamespaceId::new("default"),
                },
            )
            .unwrap(),
        );
        let transfer = Arc::new(NativeCacheTransfer {
            bytes: Mutex::new(bytes),
            batches: Mutex::new(Vec::new()),
            corrupt: AtomicBool::new(false),
            delay,
        });
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path().join("local"), "node-a").unwrap()),
            sources.clone(),
            transfer.clone(),
            DfsReadConfig {
                max_inflight_bytes,
                ..DfsReadConfig::default()
            },
        )
        .with_read_authority(Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        )))
        .with_peer_cache(cache.clone());
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id: chunk.id.clone(),
                chunk_offset: 1,
                length: 2,
                output_offset: 0,
            }],
        };
        Self {
            _temp: temp,
            engine,
            cache,
            sources,
            transfer,
            batch,
            chunk,
        }
    }
}

fn replace_native_ready_with_foreign(fixture: &NativeCacheFixture) -> std::path::PathBuf {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let directory = fixture._temp.path().join("cache");
    let ready = std::fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "ready")
        })
        .unwrap();
    std::fs::rename(
        &ready,
        fixture._temp.path().join("original-ready-preserved"),
    )
    .unwrap();
    let mut foreign = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&ready)
        .unwrap();
    foreign
        .write_all(b"foreign-private-cache-replacement")
        .unwrap();
    ready
}

#[test]
fn native_peer_disk_cache_follower_ready_io_failure_falls_back_without_deleting_foreign() {
    struct PausedFirstTransfer {
        inner: Arc<NativeCacheTransfer>,
        entered: std::sync::mpsc::Sender<()>,
        release: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
    }
    impl ChunkTransfer for PausedFirstTransfer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            self.inner.read_ranges(batch, out)?;
            let release = self.release.lock().unwrap().take();
            if let Some(release) = release {
                self.entered.send(()).unwrap();
                release.recv_timeout(Duration::from_secs(5)).unwrap();
            }
            Ok(())
        }
    }
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let (transfer_entered_tx, transfer_entered_rx) = std::sync::mpsc::channel();
    let (transfer_release_tx, transfer_release_rx) = std::sync::mpsc::channel();
    fixture.engine.transfer = Arc::new(PausedFirstTransfer {
        inner: fixture.transfer.clone(),
        entered: transfer_entered_tx,
        release: Mutex::new(Some(transfer_release_rx)),
    });
    let (recheck_entered_tx, recheck_entered_rx) = std::sync::mpsc::channel();
    let (recheck_release_tx, recheck_release_rx) = std::sync::mpsc::channel();
    *fixture
        .engine
        .test_hooks
        .peer_cache_recheck_hook
        .lock()
        .unwrap() = Some(PeerCacheRecheckHook {
        stage: "follower_ready",
        entered: recheck_entered_tx,
        release: recheck_release_rx,
    });
    std::thread::scope(|scope| {
        let leader = scope.spawn(|| fixture.engine.read_batch_native_owned(&fixture.batch, 2));
        transfer_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        let follower = scope.spawn(|| fixture.engine.read_batch_native_owned(&fixture.batch, 2));
        let deadline = Instant::now() + Duration::from_secs(3);
        while fixture.cache.waiting_fill_readers_for_test() == 0 {
            assert!(
                Instant::now() < deadline,
                "follower did not join the actual fill"
            );
            std::thread::yield_now();
        }
        transfer_release_tx.send(()).unwrap();
        recheck_entered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        assert_eq!(leader.join().unwrap().unwrap().unwrap(), [1, 2]);
        let foreign = replace_native_ready_with_foreign(&fixture);
        recheck_release_tx.send(()).unwrap();
        let result = follower.join().unwrap();
        assert_eq!(
            std::fs::read(&foreign).unwrap(),
            b"foreign-private-cache-replacement"
        );
        assert_eq!(
            result.unwrap().unwrap(),
            [1, 2],
            "disposable Ready I/O must fall back to Peer"
        );
    });
    let calls = fixture.transfer.batches.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0].operations[0].op.length, 64 * 1024);
    assert_eq!(calls[1].operations[0].op.length, 2);
}

#[test]
fn native_peer_disk_cache_leader_recheck_io_failure_falls_back_without_deleting_foreign() {
    let fixture = NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let mut blocker = fixture.cache.operation().unwrap();
    blocker.acquire_fill(|| Ok(())).unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    *fixture
        .engine
        .test_hooks
        .peer_cache_recheck_hook
        .lock()
        .unwrap() = Some(PeerCacheRecheckHook {
        stage: "leader_recheck",
        entered: entered_tx,
        release: release_rx,
    });
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| fixture.engine.read_batch_native_owned(&fixture.batch, 2));
        let deadline = Instant::now() + Duration::from_secs(3);
        while !fixture.cache.has_fill_flight_for_test() {
            assert!(
                Instant::now() < deadline,
                "reader did not reach its initial miss/fill flight"
            );
            std::thread::yield_now();
        }
        {
            let bytes = fixture.transfer.bytes.lock().unwrap();
            let verified = crate::node::read::peer_cache::VerifiedPeerChunk::verify(
                &fixture.chunk,
                &bytes,
                fixture.cache.proof_budget(),
            )
            .unwrap();
            assert!(blocker.store_verified(&verified).unwrap());
        }
        drop(blocker);
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let foreign = replace_native_ready_with_foreign(&fixture);
        release_tx.send(()).unwrap();
        let result = reader.join().unwrap();
        assert_eq!(
            std::fs::read(&foreign).unwrap(),
            b"foreign-private-cache-replacement"
        );
        assert_eq!(
            result.unwrap().unwrap(),
            [1, 2],
            "recheck cache I/O must not become a flight data error"
        );
    });
    let calls = fixture.transfer.batches.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].operations[0].op.length, 64 * 1024);
}

#[test]
fn native_peer_disk_cache_failed_leader_does_not_fail_valid_follower() {
    for failure in [
        None,
        Some(unavailable(
            "another caller exhausted its transfer deadline",
        )),
        Some(corrupt("another caller's selected source was corrupt")),
    ] {
        let fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let crate::node::read::peer_cache::FillRole::Leader(mut leader) = fixture
            .cache
            .fill_flight(&fixture.chunk, || Ok(()))
            .unwrap()
        else {
            panic!("manual flight must be the first leader");
        };
        std::thread::scope(|scope| {
            let following =
                scope.spawn(|| fixture.engine.read_batch_native_owned(&fixture.batch, 2));
            let deadline = Instant::now() + Duration::from_secs(3);
            while fixture.cache.waiting_fill_readers_for_test() == 0 {
                assert!(Instant::now() < deadline, "reader did not join the flight");
                std::thread::yield_now();
            }
            if let Some(error) = failure {
                // Keep the completed leader alive: the follower must not
                // rejoin this terminal flight or inherit its source error.
                leader.finish(crate::node::read::peer_cache::FillOutcome::Failed(error));
            } else {
                drop(leader);
            }
            assert_eq!(following.join().unwrap().unwrap().unwrap(), [1, 2]);
        });
        let calls = fixture.transfer.batches.lock().unwrap();
        assert_eq!(
            calls.len(),
            1,
            "one bounded independently verified fallback"
        );
        assert_eq!(calls[0].operations[0].op.chunk_offset, 1);
        assert_eq!(calls[0].operations[0].op.length, 2);
        assert_eq!(fixture.cache.stats().unwrap().entries, 0);
    }
}

#[test]
fn native_peer_disk_cache_failed_leader_cannot_bypass_follower_stop() {
    let fixture = NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let crate::node::read::peer_cache::FillRole::Leader(mut leader) = fixture
        .cache
        .fill_flight(&fixture.chunk, || Ok(()))
        .unwrap()
    else {
        panic!("manual flight must be the first leader");
    };
    std::thread::scope(|scope| {
        let following = scope.spawn(|| fixture.engine.read_batch_native_owned(&fixture.batch, 2));
        let deadline = Instant::now() + Duration::from_secs(3);
        while fixture.cache.waiting_fill_readers_for_test() == 0 {
            assert!(Instant::now() < deadline, "reader did not join the flight");
            std::thread::yield_now();
        }
        fixture.engine.read_authority.stop();
        leader.finish(crate::node::read::peer_cache::FillOutcome::Failed(
            unavailable("leader timed out"),
        ));
        assert!(following.join().unwrap().is_err());
    });
    assert!(fixture.transfer.batches.lock().unwrap().is_empty());
    assert_eq!(fixture.cache.stats().unwrap().entries, 0);
}

#[test]
fn native_peer_disk_cache_leader_revocation_does_not_revoke_valid_follower() {
    struct PausedTransfer {
        inner: Arc<NativeCacheTransfer>,
        entered: std::sync::mpsc::Sender<()>,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
    }
    impl ChunkTransfer for PausedTransfer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            self.inner.read_ranges(batch, out)?;
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            Ok(())
        }
    }
    let mut leader =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let mut follower =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    follower.engine.peer_cache = Some(leader.cache.clone());
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    leader.engine.transfer = Arc::new(PausedTransfer {
        inner: leader.transfer.clone(),
        entered: entered_tx,
        release: Mutex::new(release_rx),
    });
    std::thread::scope(|scope| {
        let reading = scope.spawn(|| leader.engine.read_batch_native_owned(&leader.batch, 2));
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let following = scope.spawn(|| follower.engine.read_batch_native_owned(&follower.batch, 2));
        let deadline = Instant::now() + Duration::from_secs(3);
        while leader.cache.waiting_fill_readers_for_test() == 0 {
            assert!(
                Instant::now() < deadline,
                "follower did not join the paused leader flight"
            );
            std::thread::yield_now();
        }
        leader.engine.read_authority.stop();
        release_tx.send(()).unwrap();
        assert!(reading.join().unwrap().is_err());
        assert_eq!(following.join().unwrap().unwrap().unwrap(), [1, 2]);
    });
    assert_eq!(
        leader.cache.stats().unwrap().entries,
        0,
        "revoked precommit leader must not publish a Ready object"
    );
}

#[test]
fn native_peer_disk_cache_four_same_object_readers_share_one_verified_fill() {
    let fixture = NativeCacheFixture::new(
        MAX_DFS_READ_BYTES,
        vec![(0, 64 * 1024)],
        Duration::from_millis(50),
    );
    let start = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        let mut readers = Vec::new();
        for _ in 0..4 {
            readers.push(scope.spawn(|| {
                start.wait();
                assert_eq!(
                    fixture
                        .engine
                        .read_batch_native_owned(&fixture.batch, 2)
                        .unwrap()
                        .unwrap(),
                    [1, 2]
                );
            }));
        }
        for reader in readers {
            reader.join().unwrap();
        }
    });
    assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 1);
    assert_eq!(fixture.cache.stats().unwrap().entries, 1);
}

#[test]
fn native_peer_disk_cache_fetches_current_object_once_and_reuses_without_peer_rpc() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [1, 2]
    );
    fixture.batch.ops[0].chunk_offset = 8;
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [8, 9]
    );
    let calls = fixture.transfer.batches.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].operations[0].op.chunk_offset, 0);
    assert_eq!(calls[0].operations[0].op.length, 64 * 1024);
    assert_eq!(*fixture.sources.calls.lock().unwrap(), 1);
}

struct NativeStreamingCacheTransfer {
    inner: Arc<NativeCacheTransfer>,
    stream_supported: AtomicBool,
    streamed_calls: std::sync::atomic::AtomicUsize,
    fail_first: AtomicBool,
    deny_first: bool,
    stop_after_completion: Option<Arc<LocalReadAuthority>>,
}

impl ChunkTransfer for NativeStreamingCacheTransfer {
    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
        self.inner.read_ranges(batch, out)
    }

    fn supports_verified_object_sink(&self) -> bool {
        self.stream_supported.load(Ordering::Relaxed)
    }

    fn read_verified_object_to_sink<'output>(
        &self,
        batch: &PeerReadBatch,
        chunk: &ChunkObject,
        requested_offset: u64,
        out: &'output mut [u8],
        sink: &mut PeerStreamSink<'_>,
    ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
        validate_verified_peer_object_request(batch, chunk, chunk.length as usize)?;
        self.streamed_calls.fetch_add(1, Ordering::Relaxed);
        if self.fail_first.swap(false, Ordering::Relaxed) {
            out.fill(13);
            return Err(if self.deny_first {
                external_denied("streamed grant rejected")
            } else {
                corrupt("streamed completion failed")
            });
        }
        let bytes = self.inner.bytes.lock().unwrap();
        let completed = crate::node::rpc::peer::complete_peer_disk_read_for_test(
            chunk,
            &bytes,
            requested_offset,
            out,
            sink,
        )?;
        if let Some(authority) = &self.stop_after_completion {
            authority.stop();
        }
        Ok(Some(completed))
    }
}

fn streaming_cache_transfer(fixture: &NativeCacheFixture) -> Arc<NativeStreamingCacheTransfer> {
    Arc::new(NativeStreamingCacheTransfer {
        inner: fixture.transfer.clone(),
        stream_supported: AtomicBool::new(true),
        streamed_calls: std::sync::atomic::AtomicUsize::new(0),
        fail_first: AtomicBool::new(false),
        deny_first: false,
        stop_after_completion: None,
    })
}

#[test]
fn r7_prefetch_busy_source_lock_skips_without_queue_or_rpc() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut fixture = {
        let _entered = runtime.enter();
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO)
    };
    fixture.engine.config.readahead_max_bytes = 4 * 1024 * 1024;
    fixture.engine.sources_for(&fixture.batch).unwrap();
    let registry = afs_metrics::Registry::new();
    let metrics = crate::node::metrics::work::ReadWorkMetrics::register(&registry).unwrap();
    fixture.engine.fetcher = super::super::object_fetch::ObjectFetcher::new(
        runtime.handle().clone(),
        fixture.cache.proof_budget(),
        8,
        Some(metrics),
    )
    .unwrap();
    let transfer = streaming_cache_transfer(&fixture);
    fixture.engine.transfer = transfer.clone();
    fixture.batch.lookahead = vec![fixture.chunk.id.clone()];
    fixture.batch.ops.clear();
    // The same thread owns the source lock. A blocking background lookup
    // would deadlock; try-only admission must finish with a busy event.
    let sources = fixture.engine.source_cache.lock().unwrap();
    fixture
        .engine
        .prefetch_cached(&fixture.batch, 1, Arc::new(|| Ok(())));
    drop(sources);
    assert!(!fixture.cache.has_fill_flight_for_test());
    assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.cache.stats().unwrap().entries, 0);
    assert_eq!(*fixture.sources.calls.lock().unwrap(), 1);
    let text = afs_metrics::encode_text(&registry).unwrap();
    assert!(
        text.contains("afs_dfs_fetch_events_total{event=\"skip_busy\"} 1\n"),
        "{text}"
    );
    assert!(
        text.contains("afs_dfs_fetch_events_total{event=\"scheduled\"} 0\n"),
        "{text}"
    );
    runtime
        .block_on(fixture.engine.object_fetcher().unwrap().shutdown())
        .unwrap();
}

#[test]
fn r7_dropping_a_task_reader_keeps_other_readers_source_scope_live() {
    let fixture = NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    fixture.engine.sources_for(&fixture.batch).unwrap();
    let generation = fixture.engine.source_cache.lock().unwrap().generation;
    drop(fixture.engine.task_reader());
    let cache = fixture.engine.source_cache.lock().unwrap();
    assert_eq!(cache.generation, generation);
    assert_eq!(cache.len(), 1);
}

#[test]
fn r7_prefetch_missing_partial_and_expired_cached_grants_do_no_work() {
    for mode in ["missing", "partial", "expired"] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let ranges = if mode == "partial" {
            vec![(1, 2)]
        } else {
            vec![(0, 64 * 1024)]
        };
        let mut fixture = {
            let _entered = runtime.enter();
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, ranges, Duration::ZERO)
        };
        fixture.engine.config.readahead_max_bytes = 4 * 1024 * 1024;
        if mode != "missing" {
            fixture.engine.sources_for(&fixture.batch).unwrap();
        }
        if mode == "expired" {
            fixture
                .engine
                .source_cache
                .lock()
                .unwrap()
                .entries
                .retain(|_, entry| {
                    entry.value.expires_at = Instant::now();
                    true
                });
        }
        let transfer = streaming_cache_transfer(&fixture);
        fixture.engine.transfer = transfer.clone();
        fixture.batch.lookahead = vec![fixture.chunk.id.clone()];
        fixture.batch.ops.clear();
        fixture
            .engine
            .prefetch_cached(&fixture.batch, 1, Arc::new(|| Ok(())));
        assert!(
            !fixture.cache.has_fill_flight_for_test(),
            "{mode}: no optional task may be admitted"
        );
        runtime
            .block_on(fixture.engine.object_fetcher().unwrap().shutdown())
            .unwrap();
        assert!(!fixture.cache.has_fill_flight_for_test());
        assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.cache.stats().unwrap().entries, 0);
        assert_eq!(
            *fixture.sources.calls.lock().unwrap(),
            u64::from(mode != "missing")
        );
    }
}

#[test]
fn r7_prefetch_rechecks_generation_scope_and_stop_after_verified_completion() {
    struct BeforeCommitTransfer {
        inner: Arc<NativeCacheTransfer>,
        invalidate: Arc<dyn Fn() + Send + Sync>,
        completed: Arc<AtomicBool>,
    }
    impl ChunkTransfer for BeforeCommitTransfer {
        fn read_ranges(&self, _: &PeerReadBatch, _: &mut [u8]) -> Result<()> {
            panic!("prefetch cannot fall back to a body Vec")
        }
        fn supports_verified_object_sink(&self) -> bool {
            true
        }
        fn try_read_verified_object_to_sink<'output>(
            &self,
            _: &PeerReadBatch,
            chunk: &ChunkObject,
            offset: u64,
            out: &'output mut [u8],
            sink: &mut PeerStreamSink<'_>,
            control: &PrefetchControl,
        ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
            control.check()?;
            assert!(out.is_empty());
            let bytes = self.inner.bytes.lock().unwrap();
            let receipt = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                chunk, &bytes, offset, out, sink,
            )?;
            (self.invalidate)();
            self.completed.store(true, Ordering::Release);
            Ok(Some(receipt))
        }
    }
    for action in ["generation", "scope", "stop"] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let mut fixture = {
            let _entered = runtime.enter();
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO)
        };
        fixture.engine.config.readahead_max_bytes = 4 * 1024 * 1024;
        fixture.engine.sources_for(&fixture.batch).unwrap();
        let valid = Arc::new(AtomicBool::new(true));
        let completed = Arc::new(AtomicBool::new(false));
        let cache = fixture.engine.source_cache.clone();
        let batch = fixture.batch.clone();
        let authority = fixture.engine.read_authority.clone();
        let invalid_scope = valid.clone();
        let invalidate = Arc::new(move || match action {
            "generation" => cache.lock().unwrap().invalidate_scope(
                batch.file_version_id.as_ref().unwrap(),
                &batch.layout_root_id,
            ),
            "scope" => invalid_scope.store(false, Ordering::Release),
            "stop" => authority.stop(),
            _ => unreachable!(),
        });
        fixture.engine.transfer = Arc::new(BeforeCommitTransfer {
            inner: fixture.transfer.clone(),
            invalidate,
            completed: completed.clone(),
        });
        fixture.batch.lookahead = vec![fixture.chunk.id.clone()];
        fixture.batch.ops.clear();
        fixture.engine.prefetch_cached(
            &fixture.batch,
            1,
            Arc::new(move || {
                if valid.load(Ordering::Acquire) {
                    Ok(())
                } else {
                    Err(unavailable("known handle denial"))
                }
            }),
        );
        let deadline = Instant::now() + Duration::from_secs(3);
        while (!completed.load(Ordering::Acquire) || fixture.cache.has_fill_flight_for_test())
            && Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            completed.load(Ordering::Acquire),
            "{action}: verified completion must reach the invalidation barrier"
        );
        assert!(
            !fixture.cache.has_fill_flight_for_test(),
            "{action}: task must finish before close"
        );
        assert_eq!(
            fixture.cache.stats().unwrap().entries,
            0,
            "{action}: last authorization check must prevent Ready"
        );
        runtime
            .block_on(fixture.engine.object_fetcher().unwrap().shutdown())
            .unwrap();
        assert_eq!(*fixture.sources.calls.lock().unwrap(), 1);
    }
}

#[test]
fn r7_expired_queue_joins_an_already_started_object_before_returning() {
    use std::sync::mpsc;
    struct GatedTransfer {
        inner: Arc<NativeCacheTransfer>,
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    impl ChunkTransfer for GatedTransfer {
        fn read_ranges(&self, _: &PeerReadBatch, _: &mut [u8]) -> Result<()> {
            panic!("the queued fanout object must use the controlled stream");
        }
        fn supports_verified_object_sink(&self) -> bool {
            true
        }
        fn read_verified_object_to_sink<'output>(
            &self,
            _: &PeerReadBatch,
            chunk: &ChunkObject,
            offset: u64,
            out: &'output mut [u8],
            sink: &mut PeerStreamSink<'_>,
        ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
            let bytes = self.inner.bytes.lock().unwrap();
            let completed = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                chunk, &bytes, offset, out, sink,
            )?;
            self.entered.send(()).unwrap();
            self.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            Ok(Some(completed))
        }
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut fixture = {
        let _entered = runtime.enter();
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO)
    };
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    fixture.engine.transfer = Arc::new(GatedTransfer {
        inner: fixture.transfer.clone(),
        entered: entered_tx,
        release: Mutex::new(release_rx),
    });
    let engine = Arc::new(fixture.engine);
    let fetcher = engine.object_fetcher().unwrap();
    let permits: Vec<_> = (0..7)
        .map(|_| fetcher.try_reserve_background(1).unwrap().unwrap())
        .collect();
    let mut batch = fixture.batch;
    let mut second = batch.ops[0].clone();
    second.output_offset = 2;
    batch.ops.push(second);
    let sources = fixture.sources.reply.lock().unwrap().chunks.clone();
    let deadline = Instant::now() + Duration::from_millis(200);
    let (finished_tx, finished_rx) = mpsc::channel();
    let reading = std::thread::spawn(move || {
        let mut out = [91; 4];
        let result = engine.fanout_private_objects(&batch, &sources, &mut out, Some(deadline));
        finished_tx.send((result.is_err(), out)).unwrap();
    });
    let entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
    std::thread::sleep(
        deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(50),
    );
    let returned_before_join = finished_rx.try_recv().ok();
    let _ = release_tx.send(());
    drop(permits);
    reading.join().unwrap();
    let finished =
        returned_before_join.or_else(|| finished_rx.recv_timeout(Duration::from_secs(2)).ok());
    runtime.block_on(fetcher.shutdown()).unwrap();
    assert!(
        entered,
        "one object must actually begin before queue expiry"
    );
    assert!(
        returned_before_join.is_none(),
        "deadline failure cannot detach the first object"
    );
    assert_eq!(finished, Some((true, [91; 4])));
}

#[test]
fn r7_saturated_foreground_queue_stops_at_the_original_grant_deadline() {
    use std::sync::mpsc;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let mut fixture = {
        let _entered = runtime.enter();
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO)
    };
    fixture.engine.transfer = streaming_cache_transfer(&fixture);
    let engine = Arc::new(fixture.engine);
    let fetcher = engine.object_fetcher().unwrap();
    let permits: Vec<_> = (0..8)
        .map(|_| fetcher.try_reserve_background(1).unwrap().unwrap())
        .collect();
    let mut batch = fixture.batch;
    let mut second = batch.ops[0].clone();
    second.chunk_offset = 8;
    second.output_offset = 2;
    batch.ops.push(second);
    let mut sources = fixture.sources.reply.lock().unwrap().chunks.clone();
    sources[0].sources[0].read_grant.expires_at_unix_ms = now_unix_ms().saturating_add(75);
    let (finished_tx, finished_rx) = mpsc::channel();
    let reading = std::thread::spawn(move || {
        let mut out = [91; 4];
        let result = engine.fanout_private_objects(&batch, &sources, &mut out, None);
        finished_tx.send((result.is_err(), out)).unwrap();
    });
    // Always release the original eight owners and join, even on RED.
    let expired_while_saturated = finished_rx.recv_timeout(Duration::from_secs(2)).ok();
    drop(permits);
    reading.join().unwrap();
    runtime.block_on(fetcher.shutdown()).unwrap();
    assert_eq!(
        expired_while_saturated,
        Some((true, [91; 4])),
        "an expired queued grant must fail without waiting for a fetch slot or changing private output"
    );
}

#[test]
fn r7_fanout_degradation_uses_only_foreground_serial_range_fallback() {
    use std::sync::{
        Condvar,
        atomic::{AtomicU64, AtomicUsize},
        mpsc,
    };
    struct FallbackTransfer {
        mode: &'static str,
        bodies: BTreeMap<ChunkId, Vec<u8>>,
        child_range: AtomicBool,
        range_calls: AtomicUsize,
        range_bytes: AtomicU64,
        active: AtomicUsize,
        peak_active: AtomicUsize,
        entered: mpsc::Sender<()>,
        stream_entered: mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
    }
    impl ChunkTransfer for FallbackTransfer {
        fn read_ranges_async<'a>(
            &'a self,
            batch: &'a PeerReadBatch,
            out: &'a mut [u8],
        ) -> super::super::admission::ReadFuture<'a, ()> {
            Box::pin(async move {
                if tokio::task::try_id().is_some() {
                    self.child_range.store(true, Ordering::SeqCst);
                }
                self.read_ranges(batch, out)
            })
        }
        fn read_verified_object_async<'a>(
            &'a self,
            batch: &'a PeerReadBatch,
            chunk: &'a ChunkObject,
            out: &'a mut [u8],
            proof_budget: Arc<OutboardBudget>,
        ) -> super::super::admission::ReadFuture<'a, VerifiedPeerChunk<'a>> {
            Box::pin(async move {
                if tokio::task::try_id().is_some() {
                    self.child_range.store(true, Ordering::SeqCst);
                }
                self.read_verified_object(batch, chunk, out, proof_budget)
            })
        }
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            batch.validate(out.len())?;
            self.range_calls.fetch_add(1, Ordering::SeqCst);
            self.range_bytes.fetch_add(
                batch.operations.iter().map(|item| item.op.length).sum(),
                Ordering::SeqCst,
            );
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_active.fetch_max(active, Ordering::SeqCst);
            let _ = self.entered.send(());
            let (lock, changed) = &*self.release;
            let released = lock.lock().unwrap();
            let _released = changed
                .wait_timeout_while(released, Duration::from_secs(5), |released| !*released)
                .unwrap();
            for item in &batch.operations {
                let body = &self.bodies[&item.op.chunk_id];
                let start = item.op.chunk_offset as usize;
                let length = item.op.length as usize;
                out[item.op.output_offset..item.op.output_offset + length]
                    .copy_from_slice(&body[start..start + length]);
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
        fn supports_verified_object_sink(&self) -> bool {
            self.mode != "unsupported"
        }
        fn read_verified_object_to_sink<'output>(
            &self,
            _: &PeerReadBatch,
            chunk: &ChunkObject,
            offset: u64,
            out: &'output mut [u8],
            sink: &mut PeerStreamSink<'_>,
        ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
            if self.mode == "sink_none"
                || (self.mode == "sink_mixed" && self.bodies[&chunk.id][0] == 29)
            {
                return Ok(None);
            }
            let completed = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                chunk,
                &self.bodies[&chunk.id],
                offset,
                out,
                sink,
            )?;
            if matches!(self.mode, "sink_mixed" | "commit_failure") {
                let _ = self.stream_entered.send(());
                let (lock, changed) = &*self.release;
                let released = lock.lock().unwrap();
                let _released = changed
                    .wait_timeout_while(released, Duration::from_secs(5), |released| !*released)
                    .unwrap();
            }
            Ok(Some(completed))
        }
    }
    for mode in [
        "partial",
        "chunk_none",
        "unsupported",
        "sink_none",
        "capacity",
        "sink_mixed",
        "commit_failure",
    ] {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let chunks = [
            StagedChunk::new(OperationId::new("r7-fallback-first"), vec![17; 64 * 1024]),
            StagedChunk::new(OperationId::new("r7-fallback-second"), vec![29; 64 * 1024]),
        ];
        let cache = Arc::new(
            crate::node::read::peer_cache::DiskPeerCache::open(
                crate::node::read::peer_cache::PeerCacheOptions {
                    directory: temp.path().join("cache"),
                    capacity_bytes: if mode == "capacity" {
                        8192
                    } else {
                        1024 * 1024
                    },
                    namespace_id: crate::dfs::NamespaceId::new("default"),
                },
            )
            .unwrap(),
        );
        let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: chunks
                .iter()
                .enumerate()
                .map(|(index, staged)| ChunkSources {
                    chunk_id: staged.chunk.id.clone(),
                    chunk: if mode == "chunk_none" {
                        None
                    } else {
                        Some(staged.chunk.clone())
                    },
                    allowed_ranges: vec![(
                        0,
                        if mode == "partial" {
                            8
                        } else {
                            staged.chunk.length
                        },
                    )],
                    sources: vec![source(
                        if index == 0 { "copy-b" } else { "copy-c" },
                        &staged.chunk.id,
                        if index == 0 { "node-b" } else { "node-c" },
                    )],
                })
                .collect(),
        }));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (stream_tx, stream_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let transfer = Arc::new(FallbackTransfer {
            mode,
            bodies: chunks
                .iter()
                .map(|staged| (staged.chunk.id.clone(), staged.bytes().to_vec()))
                .collect(),
            child_range: AtomicBool::new(false),
            range_calls: AtomicUsize::new(0),
            range_bytes: AtomicU64::new(0),
            active: AtomicUsize::new(0),
            peak_active: AtomicUsize::new(0),
            entered: entered_tx,
            stream_entered: stream_tx,
            release: release.clone(),
        });
        let engine = {
            let _entered = runtime.enter();
            Arc::new(
                DfsReadEngine::new(
                    crate::dfs::NamespaceId::new("default"),
                    "node-a".into(),
                    Arc::new(LocalChunkStore::open(temp.path().join("local"), "node-a").unwrap()),
                    provider,
                    transfer.clone(),
                    DfsReadConfig::default(),
                )
                .with_read_authority(Arc::new(LocalReadAuthority::new(
                    1,
                    Instant::now() + Duration::from_secs(30),
                )))
                .with_peer_cache(cache),
            )
        };
        let fetcher = engine.object_fetcher().unwrap();
        let batch = ReadBatch {
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            lookahead: Vec::new(),
            ops: chunks
                .iter()
                .enumerate()
                .map(|(index, staged)| ChunkReadOp {
                    chunk_id: staged.chunk.id.clone(),
                    chunk_offset: 0,
                    length: 8,
                    output_offset: index * 8,
                })
                .collect(),
        };
        let reading = std::thread::spawn(move || {
            let mut out = [91; 16];
            let result = engine.read_batch_native_into_private(&batch, &mut out);
            (result, out)
        });
        let (first_entered, second_before_release) =
            if matches!(mode, "sink_mixed" | "commit_failure") {
                let needed = if mode == "commit_failure" { 2 } else { 1 };
                let entered =
                    (0..needed).all(|_| stream_rx.recv_timeout(Duration::from_secs(2)).is_ok());
                if mode == "commit_failure" && entered {
                    // Both bodies/proofs are complete. Remove only this test's
                    // owned pending paths before commit, leaving the pinned
                    // verified body valid while forcing cache installation Err.
                    for entry in std::fs::read_dir(temp.path().join("cache")).unwrap() {
                        let entry = entry.unwrap();
                        if entry
                            .file_name()
                            .to_str()
                            .is_some_and(|name| name.starts_with(".afs-peer-cache.pending-"))
                        {
                            std::fs::remove_file(entry.path()).unwrap();
                        }
                    }
                }
                (
                    entered,
                    entered_rx.recv_timeout(Duration::from_millis(100)).is_ok(),
                )
            } else {
                (
                    entered_rx.recv_timeout(Duration::from_secs(2)).is_ok(),
                    entered_rx.recv_timeout(Duration::from_millis(100)).is_ok(),
                )
            };
        *release.0.lock().unwrap() = true;
        release.1.notify_all();
        let (result, out) = reading.join().unwrap();
        runtime.block_on(fetcher.shutdown()).unwrap();
        result.unwrap();
        assert_eq!(&out[..8], &[17; 8]);
        assert_eq!(&out[8..], &[29; 8]);
        assert!(
            first_entered,
            "{mode}: a controlled operation must actually begin"
        );
        assert!(
            !second_before_release,
            "{mode}: different peer ranges cannot bypass the shared stream boundary in parallel"
        );
        assert!(
            !transfer.child_range.load(Ordering::SeqCst),
            "{mode}: no fanout child may issue a range or legacy-body RPC"
        );
        assert_eq!(
            transfer.peak_active.load(Ordering::SeqCst),
            usize::from(mode != "commit_failure"),
            "{mode}"
        );
        assert_eq!(
            transfer.range_calls.load(Ordering::SeqCst),
            if mode == "commit_failure" { 0 } else { 2 },
            "{mode}"
        );
        assert_eq!(
            transfer.range_bytes.load(Ordering::SeqCst),
            if mode == "commit_failure" { 0 } else { 16 },
            "{mode}: fallback cannot fetch complete legacy bodies in children"
        );
        assert_eq!(
            transfer.active.load(Ordering::SeqCst),
            0,
            "{mode}: every range owner must drain"
        );
    }
}

#[test]
fn r7_cross_object_private_read_overlaps_two_sink_fills_and_joins_before_delivery() {
    use std::sync::{Condvar, mpsc};
    struct CrossObjectTransfer {
        bodies: BTreeMap<ChunkId, Vec<u8>>,
        entered: mpsc::Sender<ChunkId>,
        released: Arc<(Mutex<bool>, Condvar)>,
    }
    impl ChunkTransfer for CrossObjectTransfer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            for item in &batch.operations {
                let body = &self.bodies[&item.op.chunk_id];
                let start = item.op.chunk_offset as usize;
                let length = item.op.length as usize;
                out[item.op.output_offset..item.op.output_offset + length]
                    .copy_from_slice(&body[start..start + length]);
            }
            Ok(())
        }
        fn supports_verified_object_sink(&self) -> bool {
            true
        }
        fn read_verified_object_to_sink<'output>(
            &self,
            _batch: &PeerReadBatch,
            chunk: &ChunkObject,
            offset: u64,
            out: &'output mut [u8],
            sink: &mut PeerStreamSink<'_>,
        ) -> Result<Option<CompletedPeerDiskRead<'output>>> {
            self.entered.send(chunk.id.clone()).unwrap();
            let (lock, changed) = &*self.released;
            let release = lock.lock().unwrap();
            let _release = changed
                .wait_timeout_while(release, Duration::from_secs(5), |release| !*release)
                .unwrap();
            crate::node::rpc::peer::complete_peer_disk_read_for_test(
                chunk,
                &self.bodies[&chunk.id],
                offset,
                out,
                sink,
            )
            .map(Some)
        }
    }
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let chunks = [
        StagedChunk::new(OperationId::new("r7-cross-first"), vec![17; 64 * 1024]),
        StagedChunk::new(OperationId::new("r7-cross-second"), vec![29; 64 * 1024]),
    ];
    let (entered_tx, entered_rx) = mpsc::channel();
    let released = Arc::new((Mutex::new(false), Condvar::new()));
    let cache = Arc::new(
        crate::node::read::peer_cache::DiskPeerCache::open(
            crate::node::read::peer_cache::PeerCacheOptions {
                directory: temp.path().join("cache"),
                capacity_bytes: 1024 * 1024,
                namespace_id: crate::dfs::NamespaceId::new("default"),
            },
        )
        .unwrap(),
    );
    let provider = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: chunks
            .iter()
            .map(|chunk| ChunkSources {
                chunk: Some(chunk.chunk.clone()),
                chunk_id: chunk.chunk.id.clone(),
                allowed_ranges: vec![(0, chunk.chunk.length)],
                sources: vec![source("b", &chunk.chunk.id, "node-b")],
            })
            .collect(),
    }));
    let transfer = Arc::new(CrossObjectTransfer {
        bodies: chunks
            .iter()
            .map(|chunk| (chunk.chunk.id.clone(), chunk.bytes().to_vec()))
            .collect(),
        entered: entered_tx,
        released: released.clone(),
    });
    let engine = {
        let _entered = runtime.enter();
        DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path().join("local"), "node-a").unwrap()),
            provider,
            transfer,
            DfsReadConfig::default(),
        )
        .with_read_authority(Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        )))
        .with_peer_cache(cache.clone())
    };
    let batch = ReadBatch {
        lookahead: Vec::new(),
        file_version_id: Some(FileVersionId::new("version")),
        layout_root_id: LayoutRootId::new("layout"),
        ops: chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| ChunkReadOp {
                chunk_id: chunk.chunk.id.clone(),
                chunk_offset: if index == 0 {
                    chunk.chunk.length - 8
                } else {
                    0
                },
                length: 8,
                output_offset: index * 8,
            })
            .collect(),
    };
    let reading = std::thread::spawn(move || {
        let mut out = [0; 16];
        engine
            .read_batch_native_into_private(&batch, &mut out)
            .unwrap();
        out
    });
    let first = entered_rx.recv_timeout(Duration::from_secs(2)).ok();
    let second = entered_rx.recv_timeout(Duration::from_secs(2)).ok();
    *released.0.lock().unwrap() = true;
    released.1.notify_all();
    let out = reading.join().unwrap();
    assert!(
        first.is_some() && second.is_some() && first != second,
        "two objects in one private read must overlap their sink fills"
    );
    assert_eq!(&out[..8], &[17; 8]);
    assert_eq!(&out[8..], &[29; 8]);
    assert_eq!(cache.stats().unwrap().entries, 2);
}

#[test]
fn native_stream_fill_success_and_hit_never_invoke_legacy_full_body_transfer() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let transfer = streaming_cache_transfer(&fixture);
    fixture.engine.transfer = transfer.clone();
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [1, 2]
    );
    assert_eq!(fixture.cache.stats().unwrap().entries, 1);
    fixture.batch.ops[0].chunk_offset = 8;
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [8, 9]
    );
    assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 1);
    assert!(fixture.transfer.batches.lock().unwrap().is_empty());
}

#[test]
fn native_stream_fill_revocation_before_commit_returns_no_output_or_ready_object() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let transfer = Arc::new(NativeStreamingCacheTransfer {
        inner: fixture.transfer.clone(),
        stream_supported: AtomicBool::new(true),
        streamed_calls: std::sync::atomic::AtomicUsize::new(0),
        fail_first: AtomicBool::new(false),
        deny_first: false,
        stop_after_completion: Some(fixture.engine.read_authority.clone()),
    });
    fixture.engine.transfer = transfer.clone();
    assert!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .is_err()
    );
    assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.cache.stats().unwrap().entries, 0);
    assert!(fixture.transfer.batches.lock().unwrap().is_empty());
}

#[test]
fn native_stream_fill_transfer_failure_never_publishes_or_retries_legacy_path() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let transfer = streaming_cache_transfer(&fixture);
    transfer.fail_first.store(true, Ordering::Relaxed);
    fixture.engine.transfer = transfer.clone();
    assert!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .is_err()
    );
    assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.cache.stats().unwrap().entries, 0);
    assert!(fixture.transfer.batches.lock().unwrap().is_empty());
}

#[test]
fn native_stream_fill_cache_quota_failure_keeps_same_rpc_verified_range_success() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let cache = Arc::new(
        DiskPeerCache::open(crate::node::read::peer_cache::PeerCacheOptions {
            directory: fixture._temp.path().join("stream-tiny-cache"),
            capacity_bytes: 16,
            namespace_id: crate::dfs::NamespaceId::new("default"),
        })
        .unwrap(),
    );
    fixture.engine.peer_cache = Some(cache.clone());
    let transfer = streaming_cache_transfer(&fixture);
    fixture.engine.transfer = transfer.clone();
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [1, 2]
    );
    assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 1);
    assert_eq!(cache.stats().unwrap().entries, 0);
    assert!(fixture.transfer.batches.lock().unwrap().is_empty());
}

#[test]
fn native_stream_fill_retry_overwrites_failed_private_range_and_does_not_mix_proofs() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    fixture.sources.reply.lock().unwrap().chunks[0]
        .sources
        .push(source("c", &fixture.chunk.id, "node-c"));
    let transfer = streaming_cache_transfer(&fixture);
    transfer.fail_first.store(true, Ordering::Relaxed);
    fixture.engine.transfer = transfer.clone();
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [1, 2]
    );
    assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 2);
    assert_eq!(fixture.cache.stats().unwrap().entries, 1);
    assert!(fixture.transfer.batches.lock().unwrap().is_empty());
}

#[test]
fn native_stream_fill_known_denial_stops_other_copy_retry_and_clears_sources() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    fixture.sources.reply.lock().unwrap().chunks[0]
        .sources
        .push(source("c", &fixture.chunk.id, "node-c"));
    let transfer = Arc::new(NativeStreamingCacheTransfer {
        inner: fixture.transfer.clone(),
        stream_supported: AtomicBool::new(true),
        streamed_calls: std::sync::atomic::AtomicUsize::new(0),
        fail_first: AtomicBool::new(true),
        deny_first: true,
        stop_after_completion: None,
    });
    fixture.engine.transfer = transfer.clone();
    assert!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .is_err()
    );
    assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.cache.stats().unwrap().entries, 0);
    assert!(fixture.engine.source_cache.lock().unwrap().is_empty());
    assert!(fixture.engine.capture_native_read_authority().is_ok());
    assert!(fixture.transfer.batches.lock().unwrap().is_empty());
}

#[test]
fn native_stream_fill_unsupported_or_smaller_budget_preserves_legacy_transfer() {
    for (supported, budget) in [(false, MAX_DFS_READ_BYTES), (true, 128 * 1024)] {
        let mut fixture = NativeCacheFixture::new(budget, vec![(0, 64 * 1024)], Duration::ZERO);
        let transfer = streaming_cache_transfer(&fixture);
        transfer
            .stream_supported
            .store(supported, Ordering::Relaxed);
        fixture.engine.transfer = transfer.clone();
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [1, 2]
        );
        assert_eq!(transfer.streamed_calls.load(Ordering::Relaxed), 0);
        assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 1);
        assert_eq!(fixture.cache.stats().unwrap().entries, 1);
    }
}

#[test]
fn native_peer_disk_cache_partial_grant_or_fill_budget_preserves_range_fallback() {
    for (budget, ranges) in [
        (MAX_DFS_READ_BYTES, vec![(1, 2)]),
        (6, vec![(0, 64 * 1024)]),
        (MAX_DFS_READ_BYTES, Vec::new()),
    ] {
        let fixture = NativeCacheFixture::new(budget, ranges, Duration::ZERO);
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .unwrap()
                .unwrap(),
            [1, 2]
        );
        let calls = fixture.transfer.batches.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operations[0].op.chunk_offset, 1);
        assert_eq!(calls[0].operations[0].op.length, 2);
        assert!(
            !fixture
                .cache
                .operation()
                .unwrap()
                .read_range(&fixture.chunk, 1, &mut [0; 2])
                .unwrap()
        );
    }
}

#[test]
fn native_peer_disk_cache_denies_outside_grant_without_cache_or_peer_success() {
    let fixture = NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 1)], Duration::ZERO);
    let mut output = [b'!'; 2];
    assert_eq!(
        fixture
            .engine
            .read_batch(&fixture.batch, &mut output)
            .unwrap_err()
            .kind(),
        afs_error::ErrorKind::PermissionDenied
    );
    assert_eq!(output, [b'!'; 2]);
    assert!(fixture.transfer.batches.lock().unwrap().is_empty());
    assert!(fixture.engine.source_cache.lock().unwrap().is_empty());
    assert!(fixture.engine.capture_native_read_authority().is_ok());
}

#[test]
fn native_peer_disk_cache_rejects_complete_hash_mismatch_before_admission_or_output() {
    let fixture = NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    fixture.transfer.corrupt.store(true, Ordering::Relaxed);
    assert!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .is_err()
    );
    assert!(
        !fixture
            .cache
            .operation()
            .unwrap()
            .read_range(&fixture.chunk, 1, &mut [0; 2])
            .unwrap()
    );
}

#[test]
fn native_peer_disk_cache_final_grant_expiry_does_not_admit_or_publish() {
    let fixture = NativeCacheFixture::new(
        MAX_DFS_READ_BYTES,
        vec![(0, 64 * 1024)],
        Duration::from_millis(100),
    );
    fixture.sources.reply.lock().unwrap().chunks[0].sources[0]
        .read_grant
        .expires_at_unix_ms = now_unix_ms() + 50;
    assert!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .is_err()
    );
    assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 1);
    assert!(fixture.engine.source_cache.lock().unwrap().is_empty());
    assert!(fixture.engine.capture_native_read_authority().is_ok());
    assert!(
        !fixture
            .cache
            .operation()
            .unwrap()
            .read_range(&fixture.chunk, 1, &mut [0; 2])
            .unwrap()
    );
}

#[test]
fn native_peer_disk_cache_new_version_cannot_reuse_old_object_content() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [1, 2]
    );
    let bytes = vec![97; 64 * 1024];
    let next = StagedChunk::new(OperationId::new("next-native-cache"), bytes.clone());
    let mut next_source = source("next-copy", &next.chunk.id, "node-b");
    next_source.read_grant.file_version_id = FileVersionId::new("next-version");
    fixture.sources.reply.lock().unwrap().chunks = vec![ChunkSources {
        allowed_ranges: vec![(0, next.chunk.length)],
        chunk: Some(next.chunk.clone()),
        chunk_id: next.chunk.id.clone(),
        sources: vec![next_source],
    }];
    *fixture.transfer.bytes.lock().unwrap() = bytes;
    fixture.batch.file_version_id = Some(FileVersionId::new("next-version"));
    fixture.batch.ops[0].chunk_id = next.chunk.id;
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [97, 97]
    );
    assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 2);
}

#[test]
fn native_peer_disk_cache_hit_still_requires_current_grant() {
    let fixture = NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    assert!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .is_some()
    );
    fixture.engine.source_cache.lock().unwrap().clear();
    fixture.sources.reply.lock().unwrap().chunks[0].sources[0]
        .read_grant
        .expires_at_unix_ms = 0;
    let mut output = [b'!'; 2];
    assert!(
        fixture
            .engine
            .read_batch(&fixture.batch, &mut output)
            .is_err()
    );
    assert_eq!(output, [b'!'; 2]);
    assert_eq!(
        fixture.transfer.batches.lock().unwrap().len(),
        1,
        "neither disk cache nor an expired grant may bypass fresh Meta authority"
    );
    assert!(fixture.engine.capture_native_read_authority().is_ok());
}

#[test]
fn native_peer_disk_cache_corruption_outside_requested_range_refetches_peer() {
    use std::os::unix::fs::MetadataExt;

    let identity = |metadata: &std::fs::Metadata| {
        (
            metadata.dev(),
            metadata.ino(),
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        )
    };
    let fixture = NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [1, 2]
    );
    let mut directories = vec![fixture._temp.path().join("cache")];
    let mut damaged = false;
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                directories.push(path);
            } else if path.metadata().unwrap().len() >= fixture.chunk.length {
                let before = identity(&path.metadata().unwrap());
                let mut bytes = std::fs::read(&path).unwrap();
                assert_eq!(&bytes[..8], b"AFSPC002");
                // v2 places the body after a fixed 4096-byte header, with
                // proof metadata afterwards. Damage the body, not the proof.
                let last = 4096 + usize::try_from(fixture.chunk.length).unwrap() - 1;
                assert!(last < bytes.len());
                bytes[last] ^= 1; // Deliberately outside the requested range.
                std::fs::write(&path, bytes).unwrap();
                // Force an observable identity change even when both writes
                // fall in one filesystem timestamp tick. The full-root path
                // must reject this damage before falling back to Peer.
                std::fs::File::open(&path)
                    .unwrap()
                    .set_modified(std::time::UNIX_EPOCH)
                    .unwrap();
                assert_ne!(before, identity(&path.metadata().unwrap()));
                damaged = true;
            }
        }
    }
    assert!(damaged, "verified object must exist on disk");
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [1, 2]
    );
    assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 2);
    assert_eq!(fixture.cache.stats().unwrap().corruptions, 1);
}

#[test]
fn native_peer_disk_cache_admission_failure_does_not_fail_verified_read() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let cache = Arc::new(
        DiskPeerCache::open(crate::node::read::peer_cache::PeerCacheOptions {
            directory: fixture._temp.path().join("small-cache"),
            capacity_bytes: 4096,
            namespace_id: crate::dfs::NamespaceId::new("default"),
        })
        .unwrap(),
    );
    fixture.engine.peer_cache = Some(cache.clone());
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [1, 2]
    );
    assert_eq!(cache.stats().unwrap().entries, 0);
    assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 1);
}

#[test]
fn native_peer_disk_cache_borrowed_miss_does_not_expand_fill_but_can_use_verified_hit() {
    let fixture = NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let mut output = [b'!'; 2];
    fixture
        .engine
        .read_batch(&fixture.batch, &mut output)
        .unwrap();
    assert_eq!(output, [1, 2]);
    assert_eq!(
        fixture.transfer.batches.lock().unwrap()[0].operations[0]
            .op
            .length,
        2,
        "borrowed caller plus aggregate staging must not allocate a third full payload"
    );
    assert_eq!(fixture.cache.stats().unwrap().entries, 0);
    assert!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .is_some()
    );
    let calls_before_hit = fixture.transfer.batches.lock().unwrap().len();
    fixture
        .engine
        .read_batch(&fixture.batch, &mut output)
        .unwrap();
    assert_eq!(output, [1, 2]);
    assert_eq!(
        fixture.transfer.batches.lock().unwrap().len(),
        calls_before_hit
    );
}

struct NativeVerifiedTransfer {
    inner: Arc<NativeCacheTransfer>,
    verified_calls: std::sync::atomic::AtomicUsize,
    fail_first: AtomicBool,
    deny_first: bool,
    stop_after_proof: Option<Arc<LocalReadAuthority>>,
}

impl ChunkTransfer for NativeVerifiedTransfer {
    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
        self.inner.read_ranges(batch, out)
    }

    fn read_verified_object<'a>(
        &self,
        batch: &PeerReadBatch,
        chunk: &'a ChunkObject,
        out: &'a mut [u8],
        proof_budget: Arc<OutboardBudget>,
    ) -> Result<VerifiedPeerChunk<'a>> {
        validate_verified_peer_object_request(batch, chunk, out.len())?;
        self.verified_calls.fetch_add(1, Ordering::Relaxed);
        if self.fail_first.swap(false, Ordering::Relaxed) {
            out.fill(13);
            return Err(if self.deny_first {
                external_denied("known grant rejection")
            } else {
                corrupt("failed full-object completion")
            });
        }
        self.inner.read_ranges(batch, out)?;
        let verified = VerifiedPeerChunk::verify(chunk, out, proof_budget)?;
        if let Some(authority) = &self.stop_after_proof {
            authority.stop();
        }
        Ok(verified)
    }
}

#[test]
fn native_verified_object_default_rejects_partial_or_changed_identity_before_transfer() {
    let fixture = NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let candidate = fixture.sources.reply.lock().unwrap().chunks[0].sources[0].clone();
    let mut full = PeerReadBatch {
        file_version_id: FileVersionId::new("version"),
        layout_root_id: LayoutRootId::new("layout"),
        operations: vec![ResolvedReadOp {
            op: ChunkReadOp {
                chunk_id: fixture.chunk.id.clone(),
                chunk_offset: 0,
                length: fixture.chunk.length,
                output_offset: 0,
            },
            source: candidate,
        }],
    };
    let mut body = vec![0; fixture.chunk.length as usize];
    full.operations[0].op.length -= 1;
    assert!(
        fixture
            .transfer
            .read_verified_object(
                &full,
                &fixture.chunk,
                &mut body,
                fixture.cache.proof_budget()
            )
            .is_err()
    );
    full.operations[0].op.length += 1;
    full.operations[0].op.chunk_offset = 1;
    assert!(
        fixture
            .transfer
            .read_verified_object(
                &full,
                &fixture.chunk,
                &mut body,
                fixture.cache.proof_budget()
            )
            .is_err()
    );
    full.operations[0].op.chunk_offset = 0;
    let mut other = fixture.chunk.clone();
    other.id = ChunkId::new("different-root-object");
    assert!(
        fixture
            .transfer
            .read_verified_object(&full, &other, &mut body, fixture.cache.proof_budget())
            .is_err()
    );
    assert!(fixture.transfer.batches.lock().unwrap().is_empty());
    let verified = fixture
        .transfer
        .read_verified_object(
            &full,
            &fixture.chunk,
            &mut body,
            fixture.cache.proof_budget(),
        )
        .unwrap();
    assert_eq!(verified.chunk(), &fixture.chunk);
    assert_eq!(verified.bytes().len(), fixture.chunk.length as usize);
}

#[test]
fn native_verified_object_retry_consumes_only_successful_full_object_proof() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    fixture.sources.reply.lock().unwrap().chunks[0]
        .sources
        .push(source("c", &fixture.chunk.id, "node-c"));
    let transfer = Arc::new(NativeVerifiedTransfer {
        inner: fixture.transfer.clone(),
        verified_calls: std::sync::atomic::AtomicUsize::new(0),
        fail_first: AtomicBool::new(true),
        deny_first: false,
        stop_after_proof: None,
    });
    fixture.engine.transfer = transfer.clone();
    assert_eq!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .unwrap()
            .unwrap(),
        [1, 2]
    );
    assert_eq!(transfer.verified_calls.load(Ordering::Relaxed), 2);
    assert_eq!(
        fixture.transfer.batches.lock().unwrap().len(),
        1,
        "engine must not repeat ordinary range transfer after obtaining proof"
    );
    assert_eq!(fixture.cache.stats().unwrap().entries, 1);
}

#[test]
fn native_verified_object_known_denial_does_not_retry_another_copy() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    fixture.sources.reply.lock().unwrap().chunks[0]
        .sources
        .push(source("c", &fixture.chunk.id, "node-c"));
    let transfer = Arc::new(NativeVerifiedTransfer {
        inner: fixture.transfer.clone(),
        verified_calls: std::sync::atomic::AtomicUsize::new(0),
        fail_first: AtomicBool::new(true),
        deny_first: true,
        stop_after_proof: None,
    });
    fixture.engine.transfer = transfer.clone();
    assert!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .is_err()
    );
    assert_eq!(transfer.verified_calls.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.cache.stats().unwrap().entries, 0);
    assert!(fixture.engine.source_cache.lock().unwrap().is_empty());
    assert!(fixture.engine.capture_native_read_authority().is_ok());
}

#[test]
fn native_verified_object_late_node_stop_rejects_proof_before_admission() {
    let mut fixture =
        NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
    let transfer = Arc::new(NativeVerifiedTransfer {
        inner: fixture.transfer.clone(),
        verified_calls: std::sync::atomic::AtomicUsize::new(0),
        fail_first: AtomicBool::new(false),
        deny_first: false,
        stop_after_proof: Some(fixture.engine.read_authority.clone()),
    });
    fixture.engine.transfer = transfer;
    assert!(
        fixture
            .engine
            .read_batch_native_owned(&fixture.batch, 2)
            .is_err()
    );
    assert_eq!(fixture.cache.stats().unwrap().entries, 0);
}

struct NativeObservedVerificationTransfer {
    inner: Arc<NativeCacheTransfer>,
    metrics: crate::node::metrics::work::ReadWorkMetrics,
}

impl ChunkTransfer for NativeObservedVerificationTransfer {
    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
        self.inner.read_ranges(batch, out)
    }

    fn verification_metrics(&self) -> Option<&crate::node::metrics::work::ReadWorkMetrics> {
        Some(&self.metrics)
    }
}

#[test]
fn native_verified_object_fallback_counts_root_failure_without_unperformed_segment_hash() {
    for corrupt_root in [false, true] {
        let fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        fixture
            .transfer
            .corrupt
            .store(corrupt_root, Ordering::Relaxed);
        let registry = afs_metrics::Registry::new();
        let transfer = NativeObservedVerificationTransfer {
            inner: fixture.transfer.clone(),
            metrics: crate::node::metrics::work::ReadWorkMetrics::register(&registry).unwrap(),
        };
        let batch = PeerReadBatch {
            file_version_id: FileVersionId::new("version"),
            layout_root_id: LayoutRootId::new("layout"),
            operations: vec![ResolvedReadOp {
                op: ChunkReadOp {
                    chunk_id: fixture.chunk.id.clone(),
                    chunk_offset: 0,
                    length: fixture.chunk.length,
                    output_offset: 0,
                },
                source: fixture.sources.reply.lock().unwrap().chunks[0].sources[0].clone(),
            }],
        };
        let mut body = vec![0; fixture.chunk.length as usize];
        assert_eq!(
            transfer
                .read_verified_object(
                    &batch,
                    &fixture.chunk,
                    &mut body,
                    fixture.cache.proof_budget()
                )
                .is_err(),
            corrupt_root
        );
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "afs_dfs_read_hash_bytes_total")
            .unwrap();
        let count = |stage: &str| {
            family
                .get_metric()
                .iter()
                .find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "stage" && label.value() == stage)
                })
                .unwrap()
                .get_counter()
                .get_value() as u64
        };
        assert_eq!(count("root_fallback"), fixture.chunk.length);
        assert_eq!(count("segment_index_bytes"), 0);
        assert_eq!(count("root_in_receive"), 0);
    }
}

#[test]
fn native_peer_cache_fill_stage_metrics_exclude_hits_and_retain_failed_receive() {
    for receive_failure in [false, true] {
        let mut fixture =
            NativeCacheFixture::new(MAX_DFS_READ_BYTES, vec![(0, 64 * 1024)], Duration::ZERO);
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::metrics::work::ReadWorkMetrics::register(&registry).unwrap();
        fixture.engine.local = Arc::new(
            LocalChunkStore::open(fixture._temp.path().join("timing-local"), "node-a")
                .unwrap()
                .with_read_work_metrics(metrics),
        );
        if receive_failure {
            fixture.engine.transfer = Arc::new(NativeVerifiedTransfer {
                inner: fixture.transfer.clone(),
                verified_calls: std::sync::atomic::AtomicUsize::new(0),
                fail_first: AtomicBool::new(true),
                deny_first: true,
                stop_after_proof: None,
            });
        }
        assert_eq!(
            fixture
                .engine
                .read_batch_native_owned(&fixture.batch, 2)
                .is_err(),
            receive_failure
        );
        if !receive_failure {
            assert_eq!(
                fixture
                    .engine
                    .read_batch_native_owned(&fixture.batch, 2)
                    .unwrap()
                    .unwrap(),
                [1, 2]
            );
            assert_eq!(fixture.transfer.batches.lock().unwrap().len(), 1);
        }
        let family = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "afs_dfs_peer_disk_cache_fill_stage_observations_total")
            .unwrap();
        let count = |stage: &str| {
            family
                .get_metric()
                .iter()
                .find(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|label| label.name() == "stage" && label.value() == stage)
                })
                .unwrap()
                .get_counter()
                .get_value() as u64
        };
        for stage in ["lock_wait", "lookup", "receive_verify", "total"] {
            assert_eq!(count(stage), 1, "{stage}: failure={receive_failure}");
        }
        assert_eq!(count("prepare"), 2);
        for stage in ["store", "deliver"] {
            assert_eq!(
                count(stage),
                u64::from(!receive_failure),
                "{stage}: failure={receive_failure}"
            );
        }
    }
}

struct OwnedTransferProbe {
    destination: std::sync::atomic::AtomicUsize,
    fail_first: AtomicBool,
    fail_all: bool,
    expire_before_return: bool,
}

impl ChunkTransfer for OwnedTransferProbe {
    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
        batch.validate(out.len())?;
        self.destination
            .store(out.as_ptr() as usize, Ordering::Relaxed);
        out.fill(b'?');
        if self.fail_all || self.fail_first.swap(false, Ordering::Relaxed) {
            return Err(corrupt("injected late stream completion failure"));
        }
        out.fill(b'x');
        if self.expire_before_return {
            let deadline = batch.operations[0].source.read_grant.expires_at_unix_ms;
            while now_unix_ms() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        Ok(())
    }
}

#[test]
fn native_owned_peer_returns_the_transfer_destination_and_discards_failed_attempts() {
    for fail_first in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let chunk = ChunkId::new("missing-owned");
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id: chunk.clone(),
                sources: vec![source("b", &chunk, "node-b"), source("c", &chunk, "node-c")],
            }],
        }));
        let transfer = Arc::new(OwnedTransferProbe {
            destination: std::sync::atomic::AtomicUsize::new(0),
            fail_first: AtomicBool::new(fail_first),
            fail_all: false,
            expire_before_return: false,
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
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id: chunk,
                chunk_offset: 0,
                length: 6,
                output_offset: 0,
            }],
        };
        let result = engine.read_batch_native_owned(&batch, 6).unwrap().unwrap();
        assert_eq!(result, b"xxxxxx");
        assert_eq!(
            result.as_ptr() as usize,
            transfer.destination.load(Ordering::Relaxed)
        );
    }
}

#[test]
fn native_owned_peer_rejects_late_failure_expiry_and_excess_budget() {
    for expire_before_return in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let chunk = ChunkId::new("missing-owned");
        let mut candidate = source("b", &chunk, "node-b");
        if expire_before_return {
            candidate.read_grant.expires_at_unix_ms = now_unix_ms() + 100;
        }
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id: chunk.clone(),
                sources: vec![candidate],
            }],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
            Arc::new(OwnedTransferProbe {
                destination: std::sync::atomic::AtomicUsize::new(0),
                fail_first: AtomicBool::new(false),
                fail_all: !expire_before_return,
                expire_before_return,
            }),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id: chunk,
                chunk_offset: 0,
                length: 6,
                output_offset: 0,
            }],
        };
        assert!(
            engine
                .read_batch_native_owned(&batch, MAX_DFS_READ_BYTES as usize + 1)
                .is_err()
        );
        assert_eq!(
            *sources.calls.lock().unwrap(),
            0,
            "budget rejection must precede source lookup"
        );
        assert!(engine.read_batch_native_owned(&batch, 6).is_err());
        assert!(engine.capture_native_read_authority().is_ok());
        if expire_before_return {
            assert!(
                engine.source_cache.lock().unwrap().is_empty(),
                "expired grant context must not be reused"
            );
        }
    }
}

#[test]
fn native_owned_complex_batch_uses_existing_borrowed_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        Arc::new(UnimplementedReadSourceProvider),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    );
    let batch = ReadBatch {
        lookahead: Vec::new(),
        file_version_id: Some(FileVersionId::new("version")),
        layout_root_id: LayoutRootId::new("layout"),
        ops: vec![ChunkReadOp {
            chunk_id: ChunkId::new("missing"),
            chunk_offset: 0,
            length: 1,
            output_offset: 1,
        }],
    };
    assert!(engine.read_batch_native_owned(&batch, 2).unwrap().is_none());
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
            out[start..start + item.op.length as usize].fill(item.source.copy_id.0.as_bytes()[0]);
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
                allowed_ranges: Vec::new(),
                chunk: None,
                chunk_id: chunks[0].clone(),
                sources: vec![source("a-copy", &chunks[0], "node-b")],
            },
            ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: None,
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
        lookahead: Vec::new(),
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
            allowed_ranges: Vec::new(),
            chunk: None,
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
        lookahead: Vec::new(),
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
                    allowed_ranges: Vec::new(),
                    chunk: None,
                    chunk_id: a.clone(),
                    sources: vec![source("a-only", &a, "node-x")],
                },
                ChunkSources {
                    allowed_ranges: Vec::new(),
                    chunk: None,
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
            lookahead: Vec::new(),
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
            source_lookahead_chunks: MAX_DFS_READ_OPS + 1,
            ..maximum.clone()
        },
        DfsReadConfig {
            source_cache_bytes: MAX_SOURCE_CACHE_BYTES + 1,
            ..maximum.clone()
        },
        DfsReadConfig {
            source_cache_ttl: Duration::from_millis(60_001),
            ..maximum.clone()
        },
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
            lookahead: Vec::new(),
            file_version_id: None,
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![],
        };
        assert!(empty.validate_for_output(0, &config).is_err());
    }
}

#[derive(Clone, Copy, Debug)]
enum ExternalAuthorityFault {
    Chunk,
    Location,
    Validation,
    Range,
    GrantVersion,
    MissingReply,
    DuplicateReply,
}

struct ExternalFixture {
    chunk: crate::dfs::ChunkObject,
    candidate: SourceCandidate,
    bytes: Mutex<Vec<u8>>,
    authorizations: Mutex<u64>,
    fetches: Mutex<u64>,
    denial: Mutex<bool>,
    expires_at: Mutex<u64>,
    expires_after: Option<Duration>,
    authority_fault: Option<ExternalAuthorityFault>,
    delay: Duration,
}

impl ExternalFixture {
    fn new(bytes: &[u8]) -> Self {
        let staged = crate::node::chunk::StagedChunk::new(
            OperationId::new("external-fixture"),
            bytes.to_vec(),
        );
        let mut candidate = source("external-copy", &staged.chunk.id, "node-b");
        candidate.role = CopyRole::ExternalCommitted;
        candidate.data_endpoint = None;
        let object_revision = "immutable-v1".to_owned();
        candidate.location = CopyLocation::External {
            store_id: "store".into(),
            object_key: crate::dfs::external_object_key(
                "store",
                &object_revision,
                &staged.chunk.id,
            )
            .unwrap(),
            object_revision,
        };
        Self {
            chunk: staged.chunk,
            candidate,
            bytes: Mutex::new(bytes.to_vec()),
            authorizations: Mutex::new(0),
            fetches: Mutex::new(0),
            denial: Mutex::new(false),
            expires_at: Mutex::new(u64::MAX),
            expires_after: None,
            authority_fault: None,
            delay: Duration::ZERO,
        }
    }

    fn sources(&self) -> Arc<StaticSources> {
        Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                allowed_ranges: Vec::new(),
                chunk: Some(self.chunk.clone()),
                chunk_id: self.chunk.id.clone(),
                sources: vec![self.candidate.clone()],
            }],
        }))
    }

    fn empty_sources(&self) -> Arc<StaticSources> {
        Arc::new(StaticSources::default())
    }

    fn current_authorizations(
        &self,
        request: crate::dfs::AuthorizeCurrentExternalRead,
    ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
        let mut validations = request.validations;
        validations.extend(request.missing_sources.into_iter().map(|missing| {
            crate::dfs::DfsReadValidation {
                grant: self.candidate.read_grant.clone(),
                chunk_id: missing.chunk_id,
                copy_id: self.candidate.copy_id.clone(),
                chunk_offset: missing.chunk_offset,
                length: missing.length,
            }
        }));
        self.validate_external_read_grants(&request.caller_id, validations)
    }

    fn batch(&self) -> ReadBatch {
        ReadBatch {
            lookahead: Vec::new(),
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id: self.chunk.id.clone(),
                chunk_offset: 1,
                length: 2,
                output_offset: 0,
            }],
        }
    }

    fn engine(
        self: &Arc<Self>,
        local: Arc<LocalChunkStore>,
        sources: Arc<StaticSources>,
    ) -> DfsReadEngine {
        DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            sources,
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig {
                source_cache_ttl: Duration::from_secs(60),
                ..DfsReadConfig::default()
            },
        )
        .with_external_backend(self.clone(), self.clone())
    }
}

impl ExternalReadAuthorizer for ExternalFixture {
    fn authorize_external_version_read(
        &self,
        request: AuthorizeExternalVersionRead,
    ) -> Result<Option<AuthorizeExternalVersionReadReply>> {
        let validations = request
            .requests
            .into_iter()
            .map(|op| {
                let mut grant = self.candidate.read_grant.clone();
                grant.namespace_id = request.namespace_id.clone();
                grant.file_version_id = request.file_version_id.clone();
                grant.layout_root_id = request.layout_root_id.clone();
                DfsReadValidation {
                    grant,
                    chunk_id: op.chunk_id,
                    copy_id: self.candidate.copy_id.clone(),
                    chunk_offset: op.chunk_offset,
                    length: op.length,
                }
            })
            .collect();
        let entries = self.validate_external_read_grants(&request.caller_id, validations)?;
        let remaining = self
            .expires_after
            .map_or(5_000, |value| value.as_millis() as u64);
        Ok(Some(AuthorizeExternalVersionReadReply {
            authorizations: entries,
            remaining_validity_ms: remaining,
        }))
    }

    fn validate_external_read_grants(
        &self,
        caller_id: &str,
        validations: Vec<crate::dfs::DfsReadValidation>,
    ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
        assert_eq!(caller_id, "node-a");
        *self.authorizations.lock().unwrap() += 1;
        if *self.denial.lock().unwrap() {
            return Err(unavailable("external authorization denied"));
        }
        if matches!(
            self.authority_fault,
            Some(ExternalAuthorityFault::MissingReply)
        ) {
            return Ok(Vec::new());
        }
        let expires_at = self.expires_after.map_or_else(
            || *self.expires_at.lock().unwrap(),
            |duration| now_unix_ms() + duration.as_millis() as u64,
        );
        let mut replies = validations
            .into_iter()
            .map(|validation| {
                let mut entry = crate::dfs::DfsExternalAuthorizedRead {
                    authorized: crate::dfs::DfsAuthorizedRead {
                        validation,
                        allowed_ranges: vec![(0, self.chunk.length)],
                        expires_at_unix_ms: expires_at,
                    },
                    chunk: self.chunk.clone(),
                    location: self.candidate.location.clone(),
                };
                match self.authority_fault {
                    Some(ExternalAuthorityFault::Chunk) => entry.chunk.content_digest.bytes[0] ^= 1,
                    Some(ExternalAuthorityFault::Location) => {
                        entry.location = CopyLocation::External {
                            store_id: "another-store".into(),
                            object_key: "generation/chunk".into(),
                            object_revision: "immutable-v1".into(),
                        }
                    }
                    Some(ExternalAuthorityFault::Validation) => {
                        entry.authorized.validation.copy_id = CopyId::new("another-copy")
                    }
                    Some(ExternalAuthorityFault::Range) => {
                        entry.authorized.allowed_ranges = vec![(0, 1)]
                    }
                    Some(ExternalAuthorityFault::GrantVersion) => {
                        entry.authorized.validation.grant.file_version_id =
                            FileVersionId::new("next-version")
                    }
                    _ => {}
                }
                entry
            })
            .collect::<Vec<_>>();
        if matches!(
            self.authority_fault,
            Some(ExternalAuthorityFault::DuplicateReply)
        ) && let Some(first) = replies.first().cloned()
        {
            replies.push(first);
        }
        Ok(replies)
    }
}

impl ExternalChunkReader for ExternalFixture {
    fn read_chunk(
        &self,
        chunk: &crate::dfs::ChunkObject,
        source: &SourceCandidate,
    ) -> Result<VerifiedExternalChunk> {
        assert_eq!(chunk, &self.chunk);
        assert_eq!(source.location, self.candidate.location);
        *self.fetches.lock().unwrap() += 1;
        std::thread::sleep(self.delay);
        VerifiedExternalChunk::verify(chunk, self.bytes.lock().unwrap().clone())
    }
}

#[derive(Clone, Copy, Debug)]
enum CurrentReadMode {
    Authorized,
    AuthorizedWrongInode,
    Denied,
    HeadChanged,
    HeadChangedSameHead,
    DeadlineExceeded,
}

struct CurrentReadAuthorizer {
    fixture: Arc<ExternalFixture>,
    mode: CurrentReadMode,
    legacy_calls: Mutex<u64>,
    current_calls: Mutex<u64>,
    last_validations: Mutex<usize>,
    last_missing_sources: Mutex<usize>,
    legacy_fallback_allowed: bool,
}

impl CurrentReadAuthorizer {
    fn new(
        fixture: Arc<ExternalFixture>,
        mode: CurrentReadMode,
        legacy_fallback_allowed: bool,
    ) -> Self {
        Self {
            fixture,
            mode,
            legacy_calls: Mutex::new(0),
            current_calls: Mutex::new(0),
            last_validations: Mutex::new(0),
            last_missing_sources: Mutex::new(0),
            legacy_fallback_allowed,
        }
    }

    fn legacy_calls(&self) -> u64 {
        *self.legacy_calls.lock().unwrap()
    }

    fn current_calls(&self) -> u64 {
        *self.current_calls.lock().unwrap()
    }

    fn last_request_shape(&self) -> (usize, usize) {
        (
            *self.last_validations.lock().unwrap(),
            *self.last_missing_sources.lock().unwrap(),
        )
    }
}

impl ExternalReadAuthorizer for CurrentReadAuthorizer {
    fn authorize_current_external_read(
        &self,
        request: crate::dfs::AuthorizeCurrentExternalRead,
    ) -> Result<Option<crate::dfs::AuthorizeCurrentExternalReadReply>> {
        *self.current_calls.lock().unwrap() += 1;
        assert_eq!(request.caller_id, "node-a");
        assert_eq!(
            request.namespace_id,
            crate::dfs::NamespaceId::new("default")
        );
        assert_eq!(request.expected_head_version, FileVersionId::new("version"));
        assert_eq!(request.layout_root_id, LayoutRootId::new("layout"));
        *self.last_validations.lock().unwrap() = request.validations.len();
        *self.last_missing_sources.lock().unwrap() = request.missing_sources.len();
        assert!(request.validations.len() + request.missing_sources.len() > 0);
        for validation in &request.validations {
            assert_eq!(validation.chunk_id, self.fixture.chunk.id);
        }
        for missing in &request.missing_sources {
            assert_eq!(missing.chunk_id, self.fixture.chunk.id);
        }
        match self.mode {
            CurrentReadMode::Authorized => {
                let authorizations = self.fixture.current_authorizations(request)?;
                Ok(Some(
                    crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
                        inode: inode_with_head("version"),
                        authorizations,
                    },
                ))
            }
            CurrentReadMode::AuthorizedWrongInode => {
                let authorizations = self.fixture.current_authorizations(request)?;
                let mut inode = inode_with_head("version");
                inode.inode_id = crate::dfs::InodeId::new("inode:other");
                Ok(Some(
                    crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
                        inode,
                        authorizations,
                    },
                ))
            }
            CurrentReadMode::Denied => Err(external_denied(
                "current external read authorization denied",
            )),
            CurrentReadMode::DeadlineExceeded => Err(Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "current external read authorization timed out",
            )),
            CurrentReadMode::HeadChanged => Ok(Some(
                crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(inode_with_head(
                    "next-version",
                )),
            )),
            CurrentReadMode::HeadChangedSameHead => Ok(Some(
                crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(inode_with_head(
                    "version",
                )),
            )),
        }
    }

    fn validate_external_read_grants(
        &self,
        caller_id: &str,
        validations: Vec<crate::dfs::DfsReadValidation>,
    ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
        *self.legacy_calls.lock().unwrap() += 1;
        if self.legacy_fallback_allowed {
            self.fixture
                .validate_external_read_grants(caller_id, validations)
        } else {
            Err(unavailable(
                "legacy external read authorization must not be used",
            ))
        }
    }
}

fn prime_external_source_cache(engine: &DfsReadEngine, fixture: &ExternalFixture) {
    prime_external_source_cache_with(
        engine,
        fixture,
        fixture.candidate.clone(),
        Duration::from_secs(60),
    );
}

fn prime_external_source_cache_with(
    engine: &DfsReadEngine,
    fixture: &ExternalFixture,
    source: SourceCandidate,
    ttl: Duration,
) {
    engine.source_cache.lock().unwrap().insert(
        (
            FileVersionId::new("version"),
            LayoutRootId::new("layout"),
            fixture.chunk.id.clone(),
        ),
        CachedSources {
            expires_at: Instant::now() + ttl,
            sources: ChunkSources {
                allowed_ranges: Vec::new(),
                chunk_id: fixture.chunk.id.clone(),
                chunk: Some(fixture.chunk.clone()),
                sources: vec![source],
            },
        },
    );
}

struct CurrentExternalChunks {
    fixtures: Vec<Arc<ExternalFixture>>,
    current_calls: Mutex<u64>,
    last_validations: Mutex<usize>,
    last_missing_sources: Mutex<usize>,
}

impl CurrentExternalChunks {
    fn new(fixtures: Vec<Arc<ExternalFixture>>) -> Self {
        Self {
            fixtures,
            current_calls: Mutex::new(0),
            last_validations: Mutex::new(0),
            last_missing_sources: Mutex::new(0),
        }
    }

    fn current_calls(&self) -> u64 {
        *self.current_calls.lock().unwrap()
    }

    fn last_request_shape(&self) -> (usize, usize) {
        (
            *self.last_validations.lock().unwrap(),
            *self.last_missing_sources.lock().unwrap(),
        )
    }
}

impl ExternalReadAuthorizer for CurrentExternalChunks {
    fn authorize_current_external_read(
        &self,
        request: crate::dfs::AuthorizeCurrentExternalRead,
    ) -> Result<Option<crate::dfs::AuthorizeCurrentExternalReadReply>> {
        *self.current_calls.lock().unwrap() += 1;
        *self.last_validations.lock().unwrap() = request.validations.len();
        *self.last_missing_sources.lock().unwrap() = request.missing_sources.len();
        let mut replies = Vec::new();
        let mut validations = request.validations;
        validations.extend(request.missing_sources.into_iter().map(|missing| {
            let fixture = self
                .fixtures
                .iter()
                .find(|fixture| fixture.chunk.id == missing.chunk_id)
                .unwrap();
            crate::dfs::DfsReadValidation {
                grant: fixture.candidate.read_grant.clone(),
                chunk_id: missing.chunk_id,
                copy_id: fixture.candidate.copy_id.clone(),
                chunk_offset: missing.chunk_offset,
                length: missing.length,
            }
        }));
        for validation in validations {
            let fixture = self
                .fixtures
                .iter()
                .find(|fixture| fixture.chunk.id == validation.chunk_id)
                .unwrap();
            replies.extend(
                fixture.validate_external_read_grants(&request.caller_id, vec![validation])?,
            );
        }
        Ok(Some(
            crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
                inode: inode_with_head("version"),
                authorizations: replies,
            },
        ))
    }

    fn validate_external_read_grants(
        &self,
        _caller_id: &str,
        _validations: Vec<crate::dfs::DfsReadValidation>,
    ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
        Err(unavailable(
            "legacy external read authorization must not be used",
        ))
    }
}

impl ExternalChunkReader for CurrentExternalChunks {
    fn read_chunk(
        &self,
        chunk: &ChunkObject,
        source: &SourceCandidate,
    ) -> Result<VerifiedExternalChunk> {
        self.fixtures
            .iter()
            .find(|fixture| fixture.chunk.id == chunk.id)
            .unwrap()
            .read_chunk(chunk, source)
    }
}

fn inode_with_head(version_id: &str) -> crate::dfs::InodeRecord {
    crate::dfs::InodeRecord {
        namespace_id: crate::dfs::NamespaceId::new("default"),
        inode_id: crate::dfs::InodeId::new("inode:test"),
        kind: crate::dfs::InodeKind::Regular,
        attributes: crate::dfs::InodeAttributes {
            mode: 0o640,
            uid: 1000,
            gid: 1000,
            nlink: 1,
            atime_unix_ms: 1,
            mtime_unix_ms: 1,
            ctime_unix_ms: 1,
        },
        head_version: Some(FileVersionId::new(version_id)),
        symlink_target: None,
        xattrs: Default::default(),
        revision: 1,
    }
}

#[test]
fn external_backend_uses_current_head_authorization_when_available() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let authorizer = Arc::new(CurrentReadAuthorizer::new(
        fixture.clone(),
        CurrentReadMode::Authorized,
        false,
    ));
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        )
        .with_external_backend(fixture.clone(), authorizer.clone());
    prime_external_source_cache(&engine, &fixture);

    let mut out = [b'!'; 2];
    let outcome = engine
        .read_batch_current_external(
            &crate::dfs::InodeId::new("inode:test"),
            &fixture.batch(),
            &mut out,
        )
        .unwrap();

    assert_eq!(outcome, CurrentExternalReadOutcome::Read);
    assert_eq!(&out, b"bc");
    assert_eq!(authorizer.current_calls(), 1);
    assert_eq!(authorizer.legacy_calls(), 0);
    assert_eq!(authorizer.last_request_shape(), (1, 0));
    assert_eq!(*fixture.fetches.lock().unwrap(), 1);
}

#[test]
fn external_backend_current_read_resolves_missing_source_without_source_lookup() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let sources = fixture.empty_sources();
    let authorizer = Arc::new(CurrentReadAuthorizer::new(
        fixture.clone(),
        CurrentReadMode::Authorized,
        false,
    ));
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
        )
        .with_external_backend(fixture.clone(), authorizer.clone());

    let mut out = [b'!'; 2];
    let outcome = engine
        .read_batch_current_external(
            &crate::dfs::InodeId::new("inode:test"),
            &fixture.batch(),
            &mut out,
        )
        .unwrap();

    assert_eq!(outcome, CurrentExternalReadOutcome::Read);
    assert_eq!(&out, b"bc");
    assert_eq!(authorizer.last_request_shape(), (0, 1));
    assert_eq!(*sources.calls.lock().unwrap(), 0);
    assert_eq!(*fixture.fetches.lock().unwrap(), 1);
}

#[test]
fn external_backend_current_read_resolves_each_missing_range_on_same_chunk() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let sources = fixture.empty_sources();
    let authorizer = Arc::new(CurrentReadAuthorizer::new(
        fixture.clone(),
        CurrentReadMode::Authorized,
        false,
    ));
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
        )
        .with_external_backend(fixture.clone(), authorizer.clone());
    let batch = ReadBatch {
        lookahead: Vec::new(),
        file_version_id: Some(FileVersionId::new("version")),
        layout_root_id: LayoutRootId::new("layout"),
        ops: vec![
            ChunkReadOp {
                chunk_id: fixture.chunk.id.clone(),
                chunk_offset: 1,
                length: 2,
                output_offset: 0,
            },
            ChunkReadOp {
                chunk_id: fixture.chunk.id.clone(),
                chunk_offset: 3,
                length: 2,
                output_offset: 2,
            },
        ],
    };

    let mut out = [b'!'; 4];
    let outcome = engine
        .read_batch_current_external(&crate::dfs::InodeId::new("inode:test"), &batch, &mut out)
        .unwrap();

    assert_eq!(outcome, CurrentExternalReadOutcome::Read);
    assert_eq!(&out, b"bcde");
    assert_eq!(authorizer.last_request_shape(), (0, 2));
    assert_eq!(*sources.calls.lock().unwrap(), 0);
    assert_eq!(*fixture.fetches.lock().unwrap(), 1);
}

#[test]
fn external_backend_current_read_mixes_cached_and_missing_sources_in_original_output_order() {
    let temp = tempfile::tempdir().unwrap();
    let cached = Arc::new(ExternalFixture::new(b"abcdef"));
    let missing = Arc::new(ExternalFixture::new(b"uvwxyz"));
    let chunks = Arc::new(CurrentExternalChunks::new(vec![
        cached.clone(),
        missing.clone(),
    ]));
    let sources = Arc::new(StaticSources::default());
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        sources.clone(),
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig {
            source_cache_ttl: Duration::from_secs(60),
            ..DfsReadConfig::default()
        },
    )
    .with_external_backend(chunks.clone(), chunks.clone());
    prime_external_source_cache(&engine, &cached);
    let batch = ReadBatch {
        lookahead: Vec::new(),
        file_version_id: Some(FileVersionId::new("version")),
        layout_root_id: LayoutRootId::new("layout"),
        ops: vec![
            ChunkReadOp {
                chunk_id: missing.chunk.id.clone(),
                chunk_offset: 1,
                length: 2,
                output_offset: 0,
            },
            ChunkReadOp {
                chunk_id: cached.chunk.id.clone(),
                chunk_offset: 2,
                length: 2,
                output_offset: 2,
            },
            ChunkReadOp {
                chunk_id: missing.chunk.id.clone(),
                chunk_offset: 3,
                length: 2,
                output_offset: 4,
            },
        ],
    };

    let mut out = [b'!'; 6];
    let outcome = engine
        .read_batch_current_external(&crate::dfs::InodeId::new("inode:test"), &batch, &mut out)
        .unwrap();

    assert_eq!(outcome, CurrentExternalReadOutcome::Read);
    assert_eq!(&out, b"vwcdxy");
    assert_eq!(chunks.current_calls(), 1);
    assert_eq!(chunks.last_request_shape(), (1, 2));
    assert_eq!(*sources.calls.lock().unwrap(), 0);
    assert_eq!(*cached.fetches.lock().unwrap(), 1);
    assert_eq!(*missing.fetches.lock().unwrap(), 1);
}

#[test]
fn external_backend_current_missing_authority_faults_do_not_fetch_or_fallback() {
    for fault in [
        ExternalAuthorityFault::Location,
        ExternalAuthorityFault::Range,
        ExternalAuthorityFault::GrantVersion,
        ExternalAuthorityFault::MissingReply,
        ExternalAuthorityFault::DuplicateReply,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture {
            authority_fault: Some(fault),
            ..ExternalFixture::new(b"abcdef")
        });
        let sources = fixture.empty_sources();
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::Authorized,
            true,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources.clone(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_current_external(
                    &crate::dfs::InodeId::new("inode:test"),
                    &fixture.batch(),
                    &mut out,
                )
                .is_err(),
            "fault {fault:?}"
        );

        assert_eq!(&out, b"!!", "fault {fault:?}");
        assert_eq!(authorizer.current_calls(), 1, "fault {fault:?}");
        assert_eq!(authorizer.legacy_calls(), 0, "fault {fault:?}");
        assert_eq!(authorizer.last_request_shape(), (0, 1), "fault {fault:?}");
        assert_eq!(*sources.calls.lock().unwrap(), 0, "fault {fault:?}");
        assert_eq!(*fixture.fetches.lock().unwrap(), 0, "fault {fault:?}");
    }
}

#[test]
fn external_backend_current_cached_invalid_grant_is_not_resolved_as_missing() {
    for case in 0..4 {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let sources = fixture.sources();
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::Denied,
            true,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources.clone(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());
        let mut source = fixture.candidate.clone();
        match case {
            0 => source.read_grant.namespace_id = crate::dfs::NamespaceId::new("other"),
            1 => source.read_grant.caller_node_epoch = 0,
            2 => source.read_grant.expires_at_unix_ms = 0,
            _ => source.read_grant.token.clear(),
        }
        prime_external_source_cache_with(&engine, &fixture, source, Duration::from_secs(60));

        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_current_external(
                    &crate::dfs::InodeId::new("inode:test"),
                    &fixture.batch(),
                    &mut out,
                )
                .is_err(),
            "case {case}"
        );

        assert_eq!(&out, b"!!", "case {case}");
        assert_eq!(authorizer.current_calls(), 1, "case {case}");
        assert_eq!(authorizer.legacy_calls(), 0, "case {case}");
        assert_eq!(authorizer.last_request_shape(), (1, 0), "case {case}");
        assert_eq!(*sources.calls.lock().unwrap(), 0, "case {case}");
        assert_eq!(*fixture.fetches.lock().unwrap(), 0, "case {case}");
    }
}

#[test]
fn external_backend_current_auth_errors_do_not_fallback_or_fetch() {
    for mode in [CurrentReadMode::Denied, CurrentReadMode::DeadlineExceeded] {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let sources = fixture.empty_sources();
        let authorizer = Arc::new(CurrentReadAuthorizer::new(fixture.clone(), mode, true));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources.clone(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_current_external(
                    &crate::dfs::InodeId::new("inode:test"),
                    &fixture.batch(),
                    &mut out,
                )
                .is_err(),
            "mode {mode:?}"
        );

        assert_eq!(&out, b"!!", "mode {mode:?}");
        assert_eq!(authorizer.current_calls(), 1, "mode {mode:?}");
        assert_eq!(authorizer.legacy_calls(), 0, "mode {mode:?}");
        assert_eq!(authorizer.last_request_shape(), (0, 1), "mode {mode:?}");
        assert_eq!(*sources.calls.lock().unwrap(), 0, "mode {mode:?}");
        assert_eq!(*fixture.fetches.lock().unwrap(), 0, "mode {mode:?}");
    }
}

#[test]
fn external_backend_current_authorization_denial_is_not_legacy_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let authorizer = Arc::new(CurrentReadAuthorizer::new(
        fixture.clone(),
        CurrentReadMode::Denied,
        true,
    ));
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        )
        .with_external_backend(fixture.clone(), authorizer.clone());

    let mut out = [b'!'; 2];
    assert!(
        engine
            .read_batch_current_external(
                &crate::dfs::InodeId::new("inode:test"),
                &fixture.batch(),
                &mut out,
            )
            .is_err()
    );

    assert_eq!(&out, b"!!");
    assert_eq!(authorizer.current_calls(), 1);
    assert_eq!(authorizer.legacy_calls(), 0);
    assert_eq!(*fixture.fetches.lock().unwrap(), 0);
}

#[test]
fn external_backend_current_head_changed_does_not_consume_speculative_plan() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let authorizer = Arc::new(CurrentReadAuthorizer::new(
        fixture.clone(),
        CurrentReadMode::HeadChanged,
        true,
    ));
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        )
        .with_external_backend(fixture.clone(), authorizer.clone());

    let mut out = [b'!'; 2];
    let outcome = engine
        .read_batch_current_external(
            &crate::dfs::InodeId::new("inode:test"),
            &fixture.batch(),
            &mut out,
        )
        .unwrap();

    assert!(matches!(
        outcome,
        CurrentExternalReadOutcome::HeadChanged(inode)
            if inode.head_version == Some(FileVersionId::new("next-version"))
    ));
    assert_eq!(&out, b"!!");
    assert_eq!(authorizer.current_calls(), 1);
    assert_eq!(authorizer.legacy_calls(), 0);
    assert_eq!(*fixture.fetches.lock().unwrap(), 0);
}

#[test]
fn external_backend_current_authorized_wrong_inode_is_rejected_without_output() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let authorizer = Arc::new(CurrentReadAuthorizer::new(
        fixture.clone(),
        CurrentReadMode::AuthorizedWrongInode,
        true,
    ));
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        )
        .with_external_backend(fixture.clone(), authorizer.clone());

    let mut out = [b'!'; 2];
    assert!(
        engine
            .read_batch_current_external(
                &crate::dfs::InodeId::new("inode:test"),
                &fixture.batch(),
                &mut out,
            )
            .is_err()
    );

    assert_eq!(&out, b"!!");
    assert_eq!(authorizer.current_calls(), 1);
    assert_eq!(authorizer.legacy_calls(), 0);
    assert_eq!(*fixture.fetches.lock().unwrap(), 0);
}

#[test]
fn external_backend_current_fake_head_changed_is_rejected_without_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let authorizer = Arc::new(CurrentReadAuthorizer::new(
        fixture.clone(),
        CurrentReadMode::HeadChangedSameHead,
        true,
    ));
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        )
        .with_external_backend(fixture.clone(), authorizer.clone());

    let mut out = [b'!'; 2];
    assert!(
        engine
            .read_batch_current_external(
                &crate::dfs::InodeId::new("inode:test"),
                &fixture.batch(),
                &mut out,
            )
            .is_err()
    );

    assert_eq!(&out, b"!!");
    assert_eq!(authorizer.current_calls(), 1);
    assert_eq!(authorizer.legacy_calls(), 0);
    assert_eq!(*fixture.fetches.lock().unwrap(), 0);
}

#[test]
fn external_backend_current_read_reports_unsupported_without_legacy_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        fixture.sources(),
    );

    let mut out = [b'!'; 2];
    let outcome = engine
        .read_batch_current_external(
            &crate::dfs::InodeId::new("inode:test"),
            &fixture.batch(),
            &mut out,
        )
        .unwrap();

    assert_eq!(outcome, CurrentExternalReadOutcome::Unsupported);
    assert_eq!(&out, b"!!");
    assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
    assert_eq!(*fixture.fetches.lock().unwrap(), 0);
}

#[test]
fn external_backend_rejects_verified_content_for_another_chunk() {
    struct WrongChunk(VerifiedExternalChunk);
    impl ExternalChunkReader for WrongChunk {
        fn read_chunk(
            &self,
            _: &ChunkObject,
            _: &SourceCandidate,
        ) -> Result<VerifiedExternalChunk> {
            Ok(self.0.clone())
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let other = crate::node::chunk::StagedChunk::new(
        crate::dfs::OperationId::new("other"),
        b"ghijkl".to_vec(),
    );
    let verified = VerifiedExternalChunk::verify(&other.chunk, other.bytes().to_vec()).unwrap();
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        )
        .with_external_backend(Arc::new(WrongChunk(verified)), fixture.clone());
    let mut out = [b'!'; 2];
    let error = engine.read_batch(&fixture.batch(), &mut out).unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    assert_eq!(&out, b"!!");
    assert_eq!(*fixture.authorizations.lock().unwrap(), 1);
}

#[test]
fn external_backend_refuses_invalid_authority_before_fetch() {
    for failure in 0..4 {
        let temp = tempfile::tempdir().unwrap();
        let mut fixture = ExternalFixture::new(b"abcdef");
        match failure {
            0 => *fixture.expires_at.lock().unwrap() = 1,
            1 => fixture.chunk.length = MAX_DFS_EXTERNAL_CHUNK_BYTES + 1,
            2 => fixture.chunk.id = ChunkId::new("unexpected-chunk"),
            _ => {
                fixture.candidate.location = CopyLocation::Node {
                    node_id: "node-b".into(),
                    node_epoch: 1,
                    device_id: "local-0".into(),
                    device_epoch: 1,
                    catalog_revision: 1,
                }
            }
        }
        let fixture = Arc::new(fixture);
        let mut batch = fixture.batch();
        batch.ops[0].chunk_id = fixture.candidate.chunk_id.clone();
        let sources = fixture.sources();
        sources.reply.lock().unwrap().chunks[0].chunk_id = fixture.candidate.chunk_id.clone();
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources,
        );
        let mut out = [b'!'; 2];
        assert!(engine.read_batch(&batch, &mut out).is_err());
        assert_eq!(&out, b"!!");
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }
}

#[test]
fn external_backend_rechecks_authority_after_fetch() {
    let temp = tempfile::tempdir().unwrap();
    let mut fixture = ExternalFixture::new(b"abcdef");
    fixture.delay = Duration::from_millis(150);
    fixture.expires_after = Some(Duration::from_millis(100));
    let fixture = Arc::new(fixture);
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        fixture.sources(),
    );
    let mut out = [b'!'; 2];
    assert!(engine.read_batch(&fixture.batch(), &mut out).is_err());
    assert_eq!(&out, b"!!");
    assert_eq!(*fixture.fetches.lock().unwrap(), 1);
}

#[test]
fn external_backend_rejects_changed_authorization_identity_and_range() {
    for fault in [
        ExternalAuthorityFault::Chunk,
        ExternalAuthorityFault::Location,
        ExternalAuthorityFault::Validation,
        ExternalAuthorityFault::Range,
        ExternalAuthorityFault::MissingReply,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut fixture = ExternalFixture::new(b"abcdef");
        fixture.authority_fault = Some(fault);
        let fixture = Arc::new(fixture);
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let mut out = [b'!'; 2];
        assert!(engine.read_batch(&fixture.batch(), &mut out).is_err());
        assert_eq!(&out, b"!!");
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }
}

struct ExternalChunks(Vec<Arc<ExternalFixture>>);

impl ExternalReadAuthorizer for ExternalChunks {
    fn validate_external_read_grants(
        &self,
        caller_id: &str,
        validations: Vec<DfsReadValidation>,
    ) -> Result<Vec<DfsExternalAuthorizedRead>> {
        let mut replies = Vec::new();
        for validation in validations {
            let fixture = self
                .0
                .iter()
                .find(|fixture| fixture.chunk.id == validation.chunk_id)
                .unwrap();
            replies.extend(fixture.validate_external_read_grants(caller_id, vec![validation])?);
        }
        Ok(replies)
    }
}

impl ExternalChunkReader for ExternalChunks {
    fn read_chunk(
        &self,
        chunk: &ChunkObject,
        source: &SourceCandidate,
    ) -> Result<VerifiedExternalChunk> {
        self.0
            .iter()
            .find(|fixture| fixture.chunk.id == chunk.id)
            .unwrap()
            .read_chunk(chunk, source)
    }
}

#[test]
fn external_backend_late_chunk_failure_never_publishes_earlier_success() {
    let temp = tempfile::tempdir().unwrap();
    let first = Arc::new(ExternalFixture::new(b"abcdef"));
    let second = Arc::new(ExternalFixture::new(b"uvwxyz"));
    second.bytes.lock().unwrap()[5] ^= 1;
    let chunks = Arc::new(ExternalChunks(vec![first.clone(), second.clone()]));
    let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: [&first, &second]
            .iter()
            .map(|fixture| ChunkSources {
                allowed_ranges: Vec::new(),
                chunk_id: fixture.chunk.id.clone(),
                chunk: Some(fixture.chunk.clone()),
                sources: vec![fixture.candidate.clone()],
            })
            .collect(),
    }));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        sources,
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    )
    .with_external_backend(chunks.clone(), chunks);
    let mut batch = first.batch();
    let mut last_op = second.batch().ops.remove(0);
    last_op.output_offset = 2;
    batch.ops.push(last_op);
    let mut out = [b'!'; 4];
    assert!(engine.read_batch(&batch, &mut out).is_err());
    assert_eq!(&out, b"!!!!");
    assert_eq!(*first.fetches.lock().unwrap(), 1);
    assert_eq!(*second.fetches.lock().unwrap(), 1);
}

#[test]
fn external_backend_does_not_start_another_get_after_authority_expires() {
    let temp = tempfile::tempdir().unwrap();
    let mut first = ExternalFixture::new(b"abcdef");
    first.delay = Duration::from_millis(150);
    first.expires_after = Some(Duration::from_millis(100));
    let first = Arc::new(first);
    let second = Arc::new(ExternalFixture::new(b"uvwxyz"));
    let chunks = Arc::new(ExternalChunks(vec![first.clone(), second.clone()]));
    let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: [&first, &second]
            .iter()
            .map(|fixture| ChunkSources {
                allowed_ranges: Vec::new(),
                chunk_id: fixture.chunk.id.clone(),
                chunk: Some(fixture.chunk.clone()),
                sources: vec![fixture.candidate.clone()],
            })
            .collect(),
    }));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        sources,
        Arc::new(UnimplementedChunkTransfer),
        DfsReadConfig::default(),
    )
    .with_external_backend(chunks.clone(), chunks);
    let mut batch = first.batch();
    let mut last_op = second.batch().ops.remove(0);
    last_op.output_offset = 2;
    batch.ops.push(last_op);
    let mut out = [b'!'; 4];
    assert!(engine.read_batch(&batch, &mut out).is_err());
    assert_eq!(&out, b"!!!!");
    assert_eq!(*first.fetches.lock().unwrap(), 1);
    assert_eq!(*second.fetches.lock().unwrap(), 0);
}

#[test]
fn external_backend_new_version_requires_its_original_grant() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let sources = fixture.sources();
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        sources.clone(),
    );
    let mut batch = fixture.batch();
    let mut out = [b'!'; 2];
    engine.read_batch(&batch, &mut out).unwrap();
    batch.file_version_id = Some(FileVersionId::new("next-version"));
    out.fill(b'!');
    assert!(engine.read_batch(&batch, &mut out).is_err());
    assert_eq!(&out, b"!!");
    sources.reply.lock().unwrap().chunks[0].sources[0]
        .read_grant
        .file_version_id = FileVersionId::new("next-version");
    engine.read_batch(&batch, &mut out).unwrap();
    assert_eq!(&out, b"bc");
    assert_eq!(*sources.calls.lock().unwrap(), 3);
    assert_eq!(*fixture.authorizations.lock().unwrap(), 2);
    assert_eq!(
        *fixture.fetches.lock().unwrap(),
        2,
        "S1 has no content cache"
    );
}

#[test]
fn external_version_owned_result_overlapping_groups_use_borrowed_fallback() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdefgh"));
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        fixture.sources(),
    );
    let mut batch = fixture.batch();
    let mut middle = batch.ops[0].clone();
    middle.chunk_id = ChunkId::new("other-object");
    batch.ops.push(middle);
    batch.ops.push(batch.ops[0].clone());
    assert!(
        engine
            .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 2,)
            .unwrap()
            .is_none(),
        "A/B/A overlap must retain original scatter ordering"
    );
    assert_eq!(*fixture.fetches.lock().unwrap(), 0);
}

#[test]
fn external_version_owned_result_holes_still_require_live_authority() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let authority = Arc::new(LocalReadAuthority::new(
        1,
        Instant::now() + Duration::from_secs(30),
    ));
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        )
        .with_read_authority(authority.clone());
    let mut batch = fixture.batch();
    batch.ops.clear();
    assert_eq!(
        engine
            .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 4,)
            .unwrap()
            .unwrap(),
        [0; 4]
    );
    authority.stop();
    assert!(
        engine
            .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 4,)
            .is_err()
    );
    assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
}

#[test]
fn external_version_owned_result_preserves_sparse_offsets_and_tail() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdefgh"));
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        fixture.sources(),
    );
    let mut batch = fixture.batch();
    batch.ops[0].output_offset = 1;
    let mut tail = batch.ops[0].clone();
    tail.chunk_offset = 7;
    tail.length = 1;
    tail.output_offset = 5;
    batch.ops.push(tail);
    let result = engine
        .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 7)
        .unwrap()
        .unwrap();
    assert_eq!(result, b"\0bc\0\0h\0");
    let mut borrowed = [b'!'; 7];
    engine
        .read_batch_external_version(&crate::dfs::InodeId::new("inode"), &batch, &mut borrowed)
        .unwrap();
    assert_eq!(&borrowed, b"!bc!!h!");
}

#[test]
fn external_version_owned_result_rejects_oversized_allocation_before_fetch() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        fixture.sources(),
    );
    assert!(
        engine
            .read_batch_external_version_owned(
                &crate::dfs::InodeId::new("inode"),
                &fixture.batch(),
                MAX_DFS_READ_BYTES as usize + 1,
            )
            .is_err()
    );
    assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
}

#[test]
fn external_version_owned_result_final_checks_do_not_expose_bytes() {
    for corrupt_bytes in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let mut fixture = ExternalFixture::new(b"abcdef");
        if corrupt_bytes {
            fixture.bytes.lock().unwrap()[5] ^= 1; // Outside requested range.
        } else {
            fixture.expires_after = Some(Duration::from_millis(20));
            fixture.delay = Duration::from_millis(60);
        }
        let fixture = Arc::new(fixture);
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        assert!(
            engine
                .read_batch_external_version_owned(
                    &crate::dfs::InodeId::new("inode"),
                    &fixture.batch(),
                    2,
                )
                .is_err()
        );
        assert_eq!(*fixture.fetches.lock().unwrap(), 1);
    }
}

#[test]
fn external_version_owned_result_stop_during_get_stays_private() {
    let temp = tempfile::tempdir().unwrap();
    let mut fixture = ExternalFixture::new(b"abcdef");
    fixture.delay = Duration::from_millis(100);
    let fixture = Arc::new(fixture);
    let authority = Arc::new(LocalReadAuthority::new(
        1,
        Instant::now() + Duration::from_secs(30),
    ));
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        )
        .with_read_authority(authority.clone());
    std::thread::scope(|scope| {
        scope.spawn(|| {
            while *fixture.fetches.lock().unwrap() == 0 {
                std::thread::yield_now();
            }
            authority.stop();
        });
        assert!(
            engine
                .read_batch_external_version_owned(
                    &crate::dfs::InodeId::new("inode"),
                    &fixture.batch(),
                    2,
                )
                .is_err()
        );
    });
}

#[test]
fn external_version_reuses_ranges_and_separates_inode_version_layout() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdefgh"));
    let sources = fixture.sources();
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        sources.clone(),
    );
    let inode = crate::dfs::InodeId::new("inode");
    let mut batch = fixture.batch();
    let mut out = [0; 2];
    engine
        .read_batch_external_version(&inode, &batch, &mut out)
        .unwrap();
    assert_eq!(&out, b"bc");
    batch.ops[0].chunk_offset = 4;
    *fixture.denial.lock().unwrap() = true; // Meta disconnected after valid admission.
    engine
        .read_batch_external_version(&inode, &batch, &mut out)
        .unwrap();
    assert_eq!(&out, b"ef");
    assert_eq!(*fixture.authorizations.lock().unwrap(), 1);
    assert_eq!(
        *sources.calls.lock().unwrap(),
        0,
        "source TTL must not force an RPC"
    );
    for change in 0..3 {
        let mut other = batch.clone();
        let key = if change == 0 {
            crate::dfs::InodeId::new("other")
        } else {
            inode.clone()
        };
        if change == 1 {
            other.file_version_id = Some(FileVersionId::new("other"));
        }
        if change == 2 {
            other.layout_root_id = LayoutRootId::new("other");
        }
        out.fill(b'!');
        assert!(
            engine
                .read_batch_external_version(&key, &other, &mut out)
                .is_err()
        );
        assert_eq!(out, [b'!'; 2]);
    }
}

#[test]
fn external_version_monotonic_expiry_never_renews_on_hit() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        fixture.sources(),
    );
    let inode = crate::dfs::InodeId::new("inode");
    let batch = fixture.batch();
    let mut out = [0; 2];
    engine
        .read_batch_external_version(&inode, &batch, &mut out)
        .unwrap();
    let original = engine
        .external_authority_cache
        .lock()
        .unwrap()
        .entries
        .values()
        .next()
        .unwrap()
        .deadline;
    engine
        .read_batch_external_version(&inode, &batch, &mut out)
        .unwrap();
    assert_eq!(
        engine
            .external_authority_cache
            .lock()
            .unwrap()
            .entries
            .values()
            .next()
            .unwrap()
            .deadline,
        original
    );
    {
        let mut cache = engine.external_authority_cache.lock().unwrap();
        let entry = Arc::get_mut(cache.entries.values_mut().next().unwrap()).unwrap();
        entry.deadline = Instant::now() - Duration::from_millis(1);
        entry.expires_at_unix_ms = u64::MAX; // A backwards wall clock cannot extend monotonic authority.
    }
    *fixture.denial.lock().unwrap() = true;
    out.fill(b'!');
    assert!(
        engine
            .read_batch_external_version(&inode, &batch, &mut out)
            .is_err()
    );
    assert_eq!(out, [b'!'; 2]);
    *fixture.denial.lock().unwrap() = false;
    engine
        .read_batch_external_version(&inode, &batch, &mut out)
        .unwrap();
    assert_eq!(*fixture.authorizations.lock().unwrap(), 3);
}

#[test]
fn external_version_stop_during_get_prevents_output_and_cannot_resurrect() {
    let temp = tempfile::tempdir().unwrap();
    let mut fixture = ExternalFixture::new(b"abcdef");
    fixture.delay = Duration::from_millis(100);
    let fixture = Arc::new(fixture);
    let authority = Arc::new(LocalReadAuthority::new(
        1,
        Instant::now() + Duration::from_secs(30),
    ));
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        )
        .with_read_authority(authority.clone());
    std::thread::scope(|scope| {
        scope.spawn(|| {
            while *fixture.fetches.lock().unwrap() == 0 {
                std::thread::yield_now();
            }
            authority.stop();
        });
        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_external_version(
                    &crate::dfs::InodeId::new("inode"),
                    &fixture.batch(),
                    &mut out
                )
                .is_err()
        );
        assert_eq!(out, [b'!'; 2]);
    });
    assert!(
        authority
            .refresh(1, Instant::now() + Duration::from_secs(30))
            .is_err()
    );
}

#[test]
fn external_version_expiring_fetch_and_bad_range_do_not_publish() {
    for fault in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let mut fixture = ExternalFixture::new(b"abcdef");
        if fault {
            fixture.authority_fault = Some(ExternalAuthorityFault::Range);
        } else {
            fixture.expires_after = Some(Duration::from_millis(20));
            fixture.delay = Duration::from_millis(60);
        }
        let fixture = Arc::new(fixture);
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_external_version(
                    &crate::dfs::InodeId::new("inode"),
                    &fixture.batch(),
                    &mut out
                )
                .is_err()
        );
        assert_eq!(out, [b'!'; 2]);
        assert_eq!(*fixture.fetches.lock().unwrap(), if fault { 0 } else { 1 });
    }
}

#[test]
fn external_version_metadata_budget_includes_pinned_entries_and_reclaims_on_drop() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        fixture.sources(),
    );
    engine
        .read_batch_external_version(
            &crate::dfs::InodeId::new("inode"),
            &fixture.batch(),
            &mut [0; 2],
        )
        .unwrap();
    let mut cache = engine.external_authority_cache.lock().unwrap();
    let pinned = cache.entries.values().next().unwrap().clone();
    let budget = cache.budget.clone();
    let charged = budget.lock().unwrap().bytes;
    assert!(charged > 0 && charged <= MAX_EXTERNAL_AUTH_CACHE_BYTES);
    cache.invalidate_inode_version(
        &crate::dfs::NamespaceId::new("default"),
        &crate::dfs::InodeId::new("inode"),
        &FileVersionId::new("version"),
    );
    assert!(pinned.invalidated.load(Ordering::Acquire));
    assert_eq!(
        budget.lock().unwrap().bytes,
        charged,
        "eviction cannot free a pinned allocation"
    );
    drop(pinned);
    assert_eq!(budget.lock().unwrap().bytes, 0);
    assert_eq!(budget.lock().unwrap().entries, 0);
}

#[test]
fn external_version_cache_caps_entries_and_bytes_while_pinned() {
    for large_metadata in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let mut fixture = ExternalFixture::new(b"abcdef");
        // Exercise budget accounting independently of trusted transport decoding.
        if large_metadata {
            fixture.candidate.read_grant.token = "x".repeat(16_384);
        }
        let fixture = Arc::new(fixture);
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let mut pins = Vec::new();
        for i in 0..=MAX_EXTERNAL_AUTH_CACHE_ENTRIES {
            let inode = crate::dfs::InodeId::new(format!("inode-{i}"));
            engine
                .read_batch_external_version(&inode, &fixture.batch(), &mut [0; 2])
                .unwrap();
            let cache = engine.external_authority_cache.lock().unwrap();
            if let Some(entry) = cache
                .entries
                .values()
                .find(|entry| entry.key.inode_id == inode)
            {
                pins.push(entry.clone());
            }
            let budget = cache.budget.lock().unwrap();
            assert!(budget.entries <= MAX_EXTERNAL_AUTH_CACHE_ENTRIES);
            assert!(budget.bytes <= MAX_EXTERNAL_AUTH_CACHE_BYTES);
        }
        assert_eq!(
            *fixture.authorizations.lock().unwrap(),
            (MAX_EXTERNAL_AUTH_CACHE_ENTRIES + 1) as u64
        );
        let mut cache = engine.external_authority_cache.lock().unwrap();
        if large_metadata {
            assert!(cache.entries.len() < MAX_EXTERNAL_AUTH_CACHE_ENTRIES);
        } else {
            assert_eq!(cache.entries.len(), MAX_EXTERNAL_AUTH_CACHE_ENTRIES);
        }
        cache.entries.clear();
        assert!(cache.budget.lock().unwrap().bytes > 0);
        drop(pins);
        assert_eq!(cache.budget.lock().unwrap().bytes, 0);
    }
}

#[test]
fn external_version_delayed_authorization_reply_cannot_reset_deadline() {
    struct Delayed(Arc<ExternalFixture>);
    impl ExternalReadAuthorizer for Delayed {
        fn authorize_external_version_read(
            &self,
            request: AuthorizeExternalVersionRead,
        ) -> Result<Option<AuthorizeExternalVersionReadReply>> {
            let mut reply = self.0.authorize_external_version_read(request)?.unwrap();
            reply.remaining_validity_ms = 20;
            std::thread::sleep(Duration::from_millis(50));
            Ok(Some(reply))
        }
        fn validate_external_read_grants(
            &self,
            caller: &str,
            validations: Vec<DfsReadValidation>,
        ) -> Result<Vec<DfsExternalAuthorizedRead>> {
            self.0.validate_external_read_grants(caller, validations)
        }
    }
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let engine = fixture
        .engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        )
        .with_external_backend(fixture.clone(), Arc::new(Delayed(fixture.clone())));
    let mut out = [b'!'; 2];
    assert!(
        engine
            .read_batch_external_version(
                &crate::dfs::InodeId::new("inode"),
                &fixture.batch(),
                &mut out
            )
            .is_err()
    );
    assert_eq!(out, [b'!'; 2]);
    assert_eq!(*fixture.fetches.lock().unwrap(), 0);
}

#[test]
fn external_backend_preserves_holes_without_requesting_a_source() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let sources = fixture.sources();
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        sources.clone(),
    );
    let mut batch = fixture.batch();
    batch.ops.clear();
    let mut out = [0; 4];
    engine.read_batch(&batch, &mut out).unwrap();
    assert_eq!(out, [0; 4]);
    assert_eq!(*sources.calls.lock().unwrap(), 0);
    assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
    assert_eq!(*fixture.fetches.lock().unwrap(), 0);
}

/// S1c-6 read hot-path microbenchmark (release, ignored). Measures per-call
/// latency of a local read and a peer read through `StaticTransfer`; prints
/// p50/p90/p99 per round. Run before and after a read-path refactor.
#[test]
#[ignore = "microbenchmark; run with --ignored --nocapture in release"]
fn s1c6_read_hot_path_latency_microbench() {
    s1c6_read_hot_path_round(4 * 1024, "4k");
    s1c6_read_hot_path_round(64 * 1024, "64k");
}

type BenchCase<'a> = (&'static str, &'a dyn Fn(&mut [u8]));

fn s1c6_read_hot_path_round(len: usize, label: &str) {
    const WARMUP: usize = 2_000;
    const ROUNDS: usize = 5;
    const CALLS: usize = 20_000;
    let payload: Vec<u8> = (0..len).map(|index| (index % 251) as u8).collect();
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let local_chunk = put_local_chunk(&local, "bench-local", &payload);
    let peer_chunk = ChunkId::new("bench-peer");
    let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
        revision: 1,
        chunks: vec![ChunkSources {
            allowed_ranges: Vec::new(),
            chunk: None,
            chunk_id: peer_chunk.clone(),
            sources: vec![source("peer-b", &peer_chunk, "node-b")],
        }],
    }));
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        local,
        sources,
        Arc::new(StaticTransfer {
            bytes: payload.clone(),
            fail_first: Mutex::new(false),
        }),
        DfsReadConfig::default(),
    );
    let batch = |chunk_id: &ChunkId, version: Option<&str>| ReadBatch {
        lookahead: Vec::new(),
        file_version_id: version.map(FileVersionId::new),
        layout_root_id: LayoutRootId::new("layout"),
        ops: vec![ChunkReadOp {
            chunk_id: chunk_id.clone(),
            chunk_offset: 0,
            length: len as u64,
            output_offset: 0,
        }],
    };
    let local_batch = batch(&local_chunk, Some("version"));
    let peer_batch = batch(&peer_chunk, Some("version"));
    let mut out = vec![0; len];
    let cases: [BenchCase<'_>; 3] = [
        ("local_read_batch", &|out| {
            engine.read_batch(&local_batch, out).unwrap()
        }),
        ("peer_read_batch", &|out| {
            engine.read_batch(&peer_batch, out).unwrap()
        }),
        ("peer_private_owned", &|out| {
            engine
                .read_batch_native_into_private(&peer_batch, out)
                .unwrap()
        }),
    ];
    for (name, call) in cases {
        for _ in 0..WARMUP {
            call(&mut out);
        }
        assert_eq!(out, payload, "{name} returned wrong bytes");
        for round in 0..ROUNDS {
            let mut samples = Vec::with_capacity(CALLS);
            for _ in 0..CALLS {
                let started = Instant::now();
                call(&mut out);
                samples.push(started.elapsed().as_nanos() as u64);
            }
            samples.sort_unstable();
            let pick = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
            println!(
                "s1c6-bench case={name}_{label} round={round} calls={CALLS} p50_ns={} p90_ns={} p99_ns={}",
                pick(0.50),
                pick(0.90),
                pick(0.99)
            );
        }
    }
}

#[test]
fn external_version_read_authorizes_thousand_cold_ranges_once() {
    let temp = tempfile::tempdir().unwrap();
    let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
    let engine = fixture.engine(
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        fixture.empty_sources(),
    );
    let mut batch = fixture.batch();
    let op = batch.ops[0].clone();
    batch.ops = (0..1000)
        .map(|index| ChunkReadOp {
            output_offset: index * 2,
            ..op.clone()
        })
        .collect();
    let result = engine
        .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 2000)
        .unwrap()
        .unwrap();
    assert_eq!(result, b"bc".repeat(1000));
    assert_eq!(*fixture.authorizations.lock().unwrap(), 1);
    assert_eq!(*fixture.fetches.lock().unwrap(), 1);
}
