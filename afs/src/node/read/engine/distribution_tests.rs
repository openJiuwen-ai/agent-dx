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
    hang_peer: Option<String>,
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
            hang_peer: None,
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
    ) -> crate::node::read::admission::ReadFuture<'a, ()> {
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
                if self.hang_peer.as_ref() == Some(node_id) {
                    out.fill(255);
                    std::future::pending::<()>().await;
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
        DfsReadConfig::default(),
    )
    .with_distribution(ReadDistributionConfig {
        parallelism: 4,
        stripe_bytes: 64 * 1024,
    })
    .unwrap();
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
        .read_distributed_ranges(&batch, &sources, &mut out, None)
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
        .read_distributed_ranges(&batch, &sources, &mut out, None)
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
async fn distribution_unresponsive_source_leaves_budget_for_healthy_copy() {
    let mut transfer = ConcurrentTransfer::new();
    transfer.hang_peer = Some("peer-1".into());
    let transfer = Arc::new(transfer);
    let (_temp, engine, batch, sources) = striped_fixture(transfer.clone());
    let mut out = vec![0; 1024 * 1024];
    engine
        .read_distributed_ranges(
            &batch,
            &sources,
            &mut out,
            Some(Instant::now() + Duration::from_millis(400)),
        )
        .await
        .unwrap();
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
            .read_distributed_ranges(
                &batch,
                &sources,
                &mut out,
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
