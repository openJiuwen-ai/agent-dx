//! Ordinary immutable-version reads: no connector or lazy-fill handler exists.
use super::*;
use std::sync::atomic::AtomicUsize;

struct ConcurrentTransfer {
    active: AtomicUsize,
    peak: AtomicUsize,
    entered: AtomicUsize,
    calls: Mutex<Vec<(String, u64, u64)>>,
    barrier: tokio::sync::Notify,
    wait_for: usize,
    fail_peer: Option<String>,
}

impl ConcurrentTransfer {
    fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            entered: AtomicUsize::new(0),
            calls: Mutex::new(Vec::new()),
            barrier: tokio::sync::Notify::new(),
            wait_for: 2,
            fail_peer: None,
        }
    }
}

impl ChunkTransfer for ConcurrentTransfer {
    fn supports_async_native_read(&self) -> bool {
        true
    }

    fn read_ranges(&self, _: &PeerReadBatch, _: &mut [u8]) -> Result<()> {
        Err(unavailable("test requires asynchronous peer transfer"))
    }

    fn read_ranges_async<'a>(
        &'a self,
        batch: &'a PeerReadBatch,
        out: &'a mut [u8],
    ) -> super::super::read_pipeline::ReadFuture<'a, ()> {
        Box::pin(async move {
            let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(current, Ordering::SeqCst);
            struct Active<'a>(&'a AtomicUsize);
            impl Drop for Active<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let _active = Active(&self.active);
            self.entered.fetch_add(1, Ordering::SeqCst);
            self.barrier.notify_waiters();
            let wait = async {
                loop {
                    let notified = self.barrier.notified();
                    if self.entered.load(Ordering::SeqCst) >= self.wait_for {
                        break;
                    }
                    notified.await;
                }
            };
            tokio::time::timeout(Duration::from_millis(200), wait)
                .await
                .map_err(|_| unavailable("independent peer reads did not overlap"))?;
            for item in &batch.operations {
                let CopyLocation::Node { node_id, .. } = &item.source.location else {
                    panic!("node expected")
                };
                self.calls.lock().unwrap().push((
                    node_id.clone(),
                    item.op.chunk_offset,
                    item.op.length,
                ));
                if self.fail_peer.as_ref() == Some(node_id) {
                    out.fill(255);
                    return Err(unavailable("selected peer exited mid-transfer"));
                }
                for index in 0..item.op.length as usize {
                    out[item.op.output_offset + index] =
                        ((item.op.chunk_offset + index as u64) % 251) as u8;
                }
            }
            Ok(())
        })
    }
}

fn striped_fixture(
    transfer: Arc<ConcurrentTransfer>,
) -> (
    tempfile::TempDir,
    DfsReadEngine,
    ReadBatch,
    Vec<ChunkSources>,
) {
    let temp = tempfile::tempdir().unwrap();
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        Arc::new(super::tests::StaticSources::default()),
        transfer,
        DfsReadConfig {
            distribution: ReadDistributionConfig {
                parallelism: 4,
                stripe_bytes: 64 * 1024,
            },
            ..Default::default()
        },
    );
    let mut batch = super::tests::source_cache_lru_batch(ChunkId::new("striped"));
    batch.ops[0].length = 1024 * 1024;
    batch.ops[0].chunk_offset = 17;
    let sources = vec![ChunkSources {
        chunk_id: batch.ops[0].chunk_id.clone(),
        chunk: None,
        allowed_ranges: vec![(17, 1024 * 1024)],
        sources: (0..3)
            .map(|i| {
                super::tests::source(
                    &format!("copy-{i}"),
                    &batch.ops[0].chunk_id,
                    &format!("peer-{i}"),
                )
            })
            .collect(),
    }];
    (temp, engine, batch, sources)
}

#[tokio::test]
async fn distribution_one_reader_stripes_one_chunk_across_distinct_nodes() {
    let transfer = Arc::new(ConcurrentTransfer::new());
    let (_temp, engine, batch, sources) = striped_fixture(transfer.clone());
    let mut out = vec![0; 1024 * 1024];
    engine
        .read_remote_batch_async(&batch, &sources, &mut out, true, None)
        .await
        .unwrap();
    assert!(transfer.peak.load(Ordering::SeqCst) >= 2);
    assert!(transfer.peak.load(Ordering::SeqCst) <= 4);
    assert_eq!(transfer.active.load(Ordering::SeqCst), 0);
    let calls = transfer.calls.lock().unwrap();
    assert_eq!(calls.iter().map(|c| &c.0).collect::<BTreeSet<_>>().len(), 3);
    assert_eq!(calls.iter().map(|c| c.2).sum::<u64>(), out.len() as u64);
    for (index, value) in out.iter().enumerate() {
        assert_eq!(*value, ((index + 17) % 251) as u8);
    }
}

#[tokio::test]
async fn distribution_failed_source_is_retried_without_publishing_partial_bytes() {
    let mut transfer = ConcurrentTransfer::new();
    transfer.fail_peer = Some("peer-1".into());
    let transfer = Arc::new(transfer);
    let (_temp, engine, batch, sources) = striped_fixture(transfer.clone());
    let mut out = vec![0; 1024 * 1024];
    engine
        .read_remote_batch_async(&batch, &sources, &mut out, true, None)
        .await
        .unwrap();
    assert!(
        transfer
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|c| c.0 == "peer-1")
    );
    assert_eq!(transfer.active.load(Ordering::SeqCst), 0);
    for (index, value) in out.iter().enumerate() {
        assert_eq!(*value, ((index + 17) % 251) as u8);
    }
}

#[tokio::test]
async fn distribution_deadline_drains_children_and_never_fetches_other_versions() {
    let mut transfer = ConcurrentTransfer::new();
    transfer.wait_for = usize::MAX;
    let transfer = Arc::new(transfer);
    let (_temp, engine, batch, mut sources) = striped_fixture(transfer.clone());
    let mut wrong = sources[0].sources[0].clone();
    wrong.read_grant.file_version_id = FileVersionId::new("different-version");
    wrong.location = CopyLocation::Node {
        node_id: "wrong-version".into(),
        node_epoch: 1,
        device_id: "disk".into(),
        device_epoch: 1,
        catalog_revision: 1,
    };
    sources[0].sources.insert(0, wrong);
    let mut out = vec![0; 1024 * 1024];
    assert!(
        engine
            .read_remote_batch_async(
                &batch,
                &sources,
                &mut out,
                true,
                Some(Instant::now() + Duration::from_millis(25))
            )
            .await
            .is_err()
    );
    assert_eq!(transfer.active.load(Ordering::SeqCst), 0);
    assert!(transfer.calls.lock().unwrap().is_empty());
    assert!(transfer.entered.load(Ordering::SeqCst) >= 2);
    assert!(
        engine.distributed_sources(&batch, &sources)[0]
            .sources
            .iter()
            .all(|s| s.read_grant.file_version_id == batch.file_version_id.clone().unwrap())
    );
}

#[test]
fn distribution_rotates_small_reads_and_bounds_configuration() {
    let (_temp, engine, batch, sources) = striped_fixture(Arc::new(ConcurrentTransfer::new()));
    let chosen: BTreeSet<_> = (0..3)
        .map(|_| {
            engine.distributed_sources(&batch, &sources)[0].sources[0]
                .copy_id
                .clone()
        })
        .collect();
    assert_eq!(chosen.len(), 3);
    for parallelism in [0, 9, usize::MAX] {
        assert!(
            ReadDistributionConfig {
                parallelism,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
    for stripe_bytes in [0, 65535, 65537, 9 * 1024 * 1024] {
        assert!(
            ReadDistributionConfig {
                stripe_bytes,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
}

#[tokio::test]
async fn distribution_reads_independent_peers_concurrently_in_one_file_version() {
    let temp = tempfile::tempdir().unwrap();
    let transfer = Arc::new(ConcurrentTransfer::new());
    let engine = DfsReadEngine::new(
        crate::dfs::NamespaceId::new("default"),
        "node-a".into(),
        Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
        Arc::new(super::tests::StaticSources::default()),
        transfer.clone(),
        DfsReadConfig {
            distribution: ReadDistributionConfig {
                parallelism: 4,
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let mut batch = super::tests::source_cache_lru_batch(ChunkId::new("first"));
    batch.ops = vec![
        ChunkReadOp {
            chunk_id: ChunkId::new("first"),
            chunk_offset: 7,
            length: 4096,
            output_offset: 0,
        },
        ChunkReadOp {
            chunk_id: ChunkId::new("second"),
            chunk_offset: 13,
            length: 4096,
            output_offset: 4096,
        },
    ];
    let sources: Vec<_> = batch
        .ops
        .iter()
        .enumerate()
        .map(|(i, op)| ChunkSources {
            chunk_id: op.chunk_id.clone(),
            chunk: None,
            allowed_ranges: vec![(op.chunk_offset, op.length)],
            sources: vec![super::tests::source(
                &format!("copy-{i}"),
                &op.chunk_id,
                &format!("peer-{i}"),
            )],
        })
        .collect();
    let mut out = vec![0; 8192];
    engine
        .read_remote_batch_async(
            &batch,
            &sources,
            &mut out,
            true,
            Some(Instant::now() + Duration::from_secs(2)),
        )
        .await
        .unwrap();
    assert!(transfer.peak.load(Ordering::SeqCst) >= 2);
    assert_eq!(transfer.active.load(Ordering::SeqCst), 0);
    for op in &batch.ops {
        for index in 0..op.length as usize {
            assert_eq!(
                out[op.output_offset + index],
                ((op.chunk_offset + index as u64) % 251) as u8
            );
        }
    }
    assert_eq!(transfer.calls.lock().unwrap().len(), 2);
}

#[derive(Default)]
struct CacheSources {
    published: Mutex<Vec<crate::dfs::PublishReadCache>>,
}
impl ReadSourceProvider for CacheSources {
    fn sources_for(&self, _: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
        Ok(Default::default())
    }
    fn publish_read_cache(
        &self,
        request: crate::dfs::PublishReadCache,
        _: Instant,
    ) -> super::super::read_pipeline::ReadFuture<'_, crate::dfs::CopyRecord> {
        Box::pin(async move {
            let copy = crate::dfs::read_cache_copy(&request.receipt)?;
            self.published.lock().unwrap().push(request);
            Ok(copy)
        })
    }
}

#[tokio::test]
async fn distribution_complete_read_registers_verified_copy_but_partial_read_does_not() {
    let mut transfer = ConcurrentTransfer::new();
    transfer.wait_for = 1;
    let (_temp, mut engine, mut batch, mut sources) = striped_fixture(Arc::new(transfer));
    let provider = Arc::new(CacheSources::default());
    engine.source_provider = provider.clone();
    engine.config.read_cache_bytes = 8 * 1024 * 1024;
    engine = engine
        .with_cache_session("session-a".into())
        .with_read_authority(Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(60),
        )));
    let length = 128 * 1024;
    let body = (0..length).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let staged = super::super::chunk::StagedChunk::new(
        crate::dfs::OperationId::new("original"),
        body.clone(),
    );
    batch.ops[0].chunk_id = staged.chunk.id.clone();
    batch.ops[0].chunk_offset = 0;
    batch.ops[0].length = length as u64;
    sources[0].chunk_id = staged.chunk.id.clone();
    sources[0].chunk = Some(staged.chunk.clone());
    sources[0].allowed_ranges = vec![(0, length as u64)];
    for source in &mut sources[0].sources {
        source.chunk_id = staged.chunk.id.clone();
    }
    let mut out = vec![0; length];
    engine
        .read_remote_batch_async(&batch, &sources, &mut out, true, None)
        .await
        .unwrap();
    assert_eq!(out, body);
    assert_eq!(
        provider.published.lock().unwrap().len(),
        1,
        "a complete verified read must become discoverable"
    );
    assert!(engine.local.open_verified(&staged.chunk.id).is_ok());
    batch.ops[0].length = 4096;
    sources[0].allowed_ranges = vec![(0, 4096)];
    engine
        .read_remote_batch_async(&batch, &sources, &mut out[..4096], true, None)
        .await
        .unwrap();
    assert_eq!(
        provider.published.lock().unwrap().len(),
        1,
        "partial reads must not claim a complete copy"
    );
}

#[tokio::test]
async fn distribution_small_authorized_read_can_fill_full_native_chunk() {
    let mut transfer = ConcurrentTransfer::new();
    transfer.wait_for = 1;
    let transfer = Arc::new(transfer);
    let (_temp, mut engine, mut batch, mut sources) = striped_fixture(transfer.clone());
    let provider = Arc::new(CacheSources::default());
    engine.source_provider = provider.clone();
    engine.config.read_cache_bytes = 8 * 1024 * 1024;
    engine = engine
        .with_cache_session("session-small".into())
        .with_read_authority(Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(60),
        )));
    let length = 128 * 1024;
    let body = (0..length).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let staged = super::super::chunk::StagedChunk::new(
        crate::dfs::OperationId::new("original-small"),
        body.clone(),
    );
    batch.ops[0].chunk_id = staged.chunk.id.clone();
    batch.ops[0].chunk_offset = 4096;
    batch.ops[0].length = 4096;
    sources[0].chunk_id = staged.chunk.id.clone();
    sources[0].chunk = Some(staged.chunk.clone());
    sources[0].allowed_ranges = vec![(0, length as u64)];
    for source in &mut sources[0].sources {
        source.chunk_id = staged.chunk.id.clone();
    }
    let mut out = vec![0; 4096];
    engine
        .read_remote_batch_async(&batch, &sources, &mut out, true, None)
        .await
        .unwrap();
    assert_eq!(out, body[4096..8192]);
    assert_eq!(
        provider.published.lock().unwrap().len(),
        1,
        "a small read can create a full copy only with a full-chunk grant"
    );
    assert_eq!(
        transfer
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.2)
            .sum::<u64>(),
        length as u64
    );
}

#[tokio::test]
async fn distribution_concurrent_small_reads_share_one_full_chunk_fill() {
    let mut transfer = ConcurrentTransfer::new();
    transfer.wait_for = 1;
    let transfer = Arc::new(transfer);
    let (_temp, mut engine, mut batch, mut sources) = striped_fixture(transfer.clone());
    let provider = Arc::new(CacheSources::default());
    engine.source_provider = provider.clone();
    engine.config.read_cache_bytes = 8 * 1024 * 1024;
    engine = engine
        .with_cache_session("session-small".into())
        .with_read_authority(Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(60),
        )));
    let length = 128 * 1024;
    let body = (0..length).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let staged = super::super::chunk::StagedChunk::new(
        crate::dfs::OperationId::new("original-small"),
        body.clone(),
    );
    batch.ops[0].chunk_id = staged.chunk.id.clone();
    batch.ops[0].chunk_offset = 4096;
    batch.ops[0].length = 4096;
    sources[0].chunk_id = staged.chunk.id.clone();
    sources[0].chunk = Some(staged.chunk.clone());
    sources[0].allowed_ranges = vec![(0, length as u64)];
    for source in &mut sources[0].sources {
        source.chunk_id = staged.chunk.id.clone();
    }
    let mut first = vec![0; 4096];
    let mut second = vec![0; 4096];
    let (a, b) = tokio::join!(
        engine.read_remote_batch_async(&batch, &sources, &mut first, true, None),
        engine.read_remote_batch_async(&batch, &sources, &mut second, true, None),
    );
    a.unwrap();
    b.unwrap();
    assert_eq!(first, body[4096..8192]);
    assert_eq!(second, first);
    assert_eq!(
        transfer
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.2)
            .sum::<u64>(),
        length as u64,
        "same chunk must not be fetched twice concurrently"
    );
}
