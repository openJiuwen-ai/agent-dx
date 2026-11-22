//! H2 step 1 (reader sharing): D0 read-cache and source-role counters, D1
//! source freshness and publication, D3 candidate order. Ordinary native
//! reads with immutable versions; no connector.
use super::*;
use crate::{
    dfs::OperationId,
    node::chunk::{ChunkSink as _, ReadCacheIntent, StagedChunk},
};
use std::{
    collections::BTreeMap as Map,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

fn candidate(copy: &str, chunk: &ChunkId, node: &str, role: CopyRole) -> SourceCandidate {
    let mut source = super::tests::source(copy, chunk, node);
    source.role = role;
    source
}

fn sources_reply(chunks: Vec<(ChunkId, Vec<SourceCandidate>)>) -> DfsChunkSourcesReply {
    DfsChunkSourcesReply {
        revision: 1,
        chunks: chunks
            .into_iter()
            .map(|(chunk_id, sources)| ChunkSources {
                chunk_id,
                chunk: None,
                allowed_ranges: Vec::new(),
                sources,
            })
            .collect(),
    }
}

/// Meta stand-in: a replaceable reply, call counts, and recorded
/// publications with their peak concurrency.
#[derive(Default)]
struct MetaStub {
    reply: Mutex<DfsChunkSourcesReply>,
    lookups: AtomicUsize,
    published: Mutex<Vec<ChunkId>>,
    active: AtomicUsize,
    peak: AtomicUsize,
    publish_delay: Duration,
    /// Publications of these Chunks fail (not NOT_FOUND).
    fail_publish: Mutex<BTreeSet<ChunkId>>,
    /// Added to every source lookup from now on.
    lookup_delay_ms: std::sync::atomic::AtomicU64,
}

impl MetaStub {
    fn new(reply: DfsChunkSourcesReply) -> Self {
        Self {
            reply: Mutex::new(reply),
            ..Default::default()
        }
    }
}

impl ReadSourceProvider for MetaStub {
    fn sources_for(&self, request: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
        self.lookups.fetch_add(1, Ordering::SeqCst);
        let mut reply = self.reply.lock().unwrap().clone();
        reply
            .chunks
            .retain(|set| request.chunk_ids.contains(&set.chunk_id));
        Ok(reply)
    }

    /// Like a network adapter: an asynchronous lookup that can be slow.
    fn sources_for_async(
        &self,
        request: DfsChunkSourcesRequest,
    ) -> crate::node::read::admission::ReadFuture<'_, DfsChunkSourcesReply> {
        Box::pin(async move {
            let delay = self.lookup_delay_ms.load(Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(delay)).await;
            self.sources_for(request)
        })
    }

    fn publish_read_cache(
        &self,
        request: crate::dfs::PublishReadCache,
        _deadline: Instant,
    ) -> crate::node::read::admission::ReadFuture<'_, crate::dfs::CopyRecord> {
        Box::pin(async move {
            if self
                .fail_publish
                .lock()
                .unwrap()
                .contains(&request.receipt.chunk_id)
            {
                return Err(unavailable("injected publication failure"));
            }
            let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(self.publish_delay).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            self.published
                .lock()
                .unwrap()
                .push(request.receipt.chunk_id.clone());
            crate::dfs::read_cache_copy(&request.receipt)
        })
    }
}

/// Serves every node's bytes except the ones listed as busy, and records
/// which node served each operation.
#[derive(Default)]
struct NodeTransfer {
    busy: Vec<String>,
    /// Nodes that answer as if the copy were gone.
    missing: Vec<String>,
    attempts: Mutex<Vec<String>>,
    served: Mutex<Vec<String>>,
}

impl ChunkTransfer for NodeTransfer {
    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
        for item in &batch.operations {
            let CopyLocation::Node { node_id, .. } = &item.source.location else {
                return Err(invalid("node source expected"));
            };
            self.attempts.lock().unwrap().push(node_id.clone());
            if self.missing.contains(node_id) {
                return Err(unavailable("injected: cache copy no longer present"));
            }
            if self.busy.contains(node_id) {
                return Err(Error::new(
                    afs_error::CLIENT_REMOTE_STATUS,
                    afs_error::ErrorKind::ResourceExhausted,
                    "DFS peer read admission deadline exceeded",
                ));
            }
            self.served.lock().unwrap().push(node_id.clone());
            let start = item.op.output_offset;
            out[start..start + item.op.length as usize].fill(7);
        }
        Ok(())
    }
}

struct Fixture {
    _temp: tempfile::TempDir,
    local: Arc<LocalChunkStore>,
    registry: afs_metrics::Registry,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::metrics::work::ReadWorkMetrics::register(&registry).unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_read_work_metrics(metrics),
        );
        Self {
            _temp: temp,
            local,
            registry,
        }
    }

    fn engine(
        &self,
        meta: Arc<MetaStub>,
        transfer: Arc<dyn ChunkTransfer>,
        config: DfsReadConfig,
    ) -> DfsReadEngine {
        DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            self.local.clone(),
            meta,
            transfer,
            config,
        )
    }

    fn metric(&self, sample: &str) -> u64 {
        let text = afs_metrics::encode_text(&self.registry).unwrap();
        text.lines()
            .find_map(|line| line.strip_prefix(sample)?.trim().parse().ok())
            .unwrap_or_else(|| panic!("{sample} missing in\n{text}"))
    }
}

fn cache_event(event: &str) -> String {
    format!("afs_dfs_read_cache_events_total{{event=\"{event}\"}}")
}

fn source_bytes(role: &str) -> String {
    format!("afs_dfs_read_source_bytes_total{{role=\"{role}\"}}")
}

fn read_op(chunk_id: ChunkId, length: u64) -> ReadBatch {
    ReadBatch {
        lookahead: Vec::new(),
        file_version_id: Some(FileVersionId::new("version")),
        layout_root_id: LayoutRootId::new("layout"),
        ops: vec![ChunkReadOp {
            chunk_id,
            chunk_offset: 0,
            length,
            output_offset: 0,
        }],
    }
}

fn authority() -> Arc<LocalReadAuthority> {
    Arc::new(LocalReadAuthority::new(
        1,
        Instant::now() + Duration::from_secs(600),
    ))
}

// ---- D0 -------------------------------------------------------------------

/// A complete body that finds the install queue's byte budget full, and a
/// partial body pushed out by newer partial Chunks, are counted drops, not
/// silent (budgets of two 2 MiB bodies each).
#[test]
fn h2_d0_staging_counts_queue_full_and_evicted_partial_drops() {
    let fixture = Fixture::new();
    let engine = budget_engine(&fixture, 4 * MIB, 4 * MIB);
    let size = 2 * MIB as usize;
    // Nobody drains the queue: the third full body is dropped.
    for seed in 0..3 {
        stage_range(&engine, seed, size, 0, 2 * MIB);
    }
    assert_eq!(fixture.metric(&cache_event("staged")), 2);
    assert_eq!(fixture.metric(&cache_event("dropped_queue_full")), 1);
    // Three different partial bodies: only two fit.
    for seed in 10..13 {
        stage_range(&engine, seed, size, 0, 32);
    }
    assert_eq!(fixture.metric(&cache_event("dropped_partial")), 1);
}

/// Reader-side bytes are attributed to the role of the copy that served
/// them, and bytes served by this Node itself count as local.
#[test]
fn h2_d0_reader_bytes_are_counted_by_source_role() {
    let fixture = Fixture::new();
    let durable = ChunkId::new("durable-only");
    let cached = ChunkId::new("cached");
    let meta = Arc::new(MetaStub::new(sources_reply(vec![
        (
            durable.clone(),
            vec![candidate("d", &durable, "node-b", CopyRole::DurableReplica)],
        ),
        (
            cached.clone(),
            vec![
                candidate("d2", &cached, "node-b", CopyRole::DurableReplica),
                candidate("c", &cached, "node-c", CopyRole::VerifiedCache),
            ],
        ),
    ])));
    let engine = fixture.engine(
        meta,
        Arc::new(NodeTransfer::default()),
        DfsReadConfig::default(),
    );
    engine
        .read_batch(&read_op(durable, 6), &mut [0; 6])
        .unwrap();
    engine.read_batch(&read_op(cached, 9), &mut [0; 9]).unwrap();
    let local = fixture
        .local
        .put(StagedChunk::new(OperationId::new("l"), b"local!".to_vec()))
        .unwrap()
        .chunk
        .id;
    engine.read_batch(&read_op(local, 6), &mut [0; 6]).unwrap();
    assert_eq!(fixture.metric(&source_bytes("durable")), 6);
    assert_eq!(fixture.metric(&source_bytes("verified_cache")), 9);
    assert_eq!(fixture.metric(&source_bytes("local")), 6);
}

// ---- D1: source freshness ---------------------------------------------------

fn cached_expiry(engine: &DfsReadEngine, chunk: &ChunkId) -> Duration {
    let key = (
        FileVersionId::new("version"),
        LayoutRootId::new("layout"),
        chunk.clone(),
    );
    let mut cache = engine.source_cache.lock().unwrap();
    let expires = cache.get(&key).expect("source cached").expires_at;
    expires.saturating_duration_since(Instant::now())
}

/// With the read cache on, a Chunk whose only remote candidate is the
/// writer is re-asked after `source_single_ttl`, including lookahead
/// Chunks fetched with it; two remote holders keep the long lifetime.
/// Without the read cache, or with the short lifetime set to zero, the
/// lifetime is unchanged (`source_cache_ttl`).
#[test]
fn h2_d1_single_source_entries_use_the_short_lifetime_only_with_the_read_cache() {
    let single = ChunkId::new("single");
    let ahead = ChunkId::new("ahead");
    let shared = ChunkId::new("shared");
    let reply = sources_reply(vec![
        (
            single.clone(),
            vec![candidate("s", &single, "node-b", CopyRole::DurableReplica)],
        ),
        (
            ahead.clone(),
            vec![candidate("a", &ahead, "node-b", CopyRole::DurableReplica)],
        ),
        (
            shared.clone(),
            vec![
                candidate("h1", &shared, "node-b", CopyRole::DurableReplica),
                candidate("h2", &shared, "node-c", CopyRole::VerifiedCache),
            ],
        ),
    ]);
    let config = DfsReadConfig {
        source_cache_ttl: Duration::from_secs(5),
        source_single_ttl: Duration::from_millis(300),
        ..Default::default()
    };
    let lookup = |engine: &DfsReadEngine| {
        let mut batch = read_op(single.clone(), 1);
        batch.ops.push(ChunkReadOp {
            chunk_id: shared.clone(),
            chunk_offset: 0,
            length: 1,
            output_offset: 1,
        });
        batch.lookahead = vec![ahead.clone()];
        engine.sources_for(&batch).unwrap();
    };
    let fixture = Fixture::new();
    let meta = Arc::new(MetaStub::new(reply.clone()));
    let cached = fixture
        .engine(meta, Arc::new(NodeTransfer::default()), config.clone())
        .with_distributed_cache("session".into(), 1 << 30);
    lookup(&cached);
    assert!(cached_expiry(&cached, &single) <= Duration::from_millis(300));
    assert!(cached_expiry(&cached, &ahead) <= Duration::from_millis(300));
    assert!(cached_expiry(&cached, &shared) > Duration::from_secs(4));

    let plain = fixture.engine(
        Arc::new(MetaStub::new(reply.clone())),
        Arc::new(NodeTransfer::default()),
        config.clone(),
    );
    lookup(&plain);
    assert!(cached_expiry(&plain, &single) > Duration::from_secs(4));

    let disabled = fixture
        .engine(
            Arc::new(MetaStub::new(reply)),
            Arc::new(NodeTransfer::default()),
            DfsReadConfig {
                source_single_ttl: Duration::ZERO,
                ..config
            },
        )
        .with_distributed_cache("session".into(), 1 << 30);
    lookup(&disabled);
    assert!(cached_expiry(&disabled, &single) > Duration::from_secs(4));
}

#[test]
fn h2_d1_short_source_lifetime_is_bounded_like_the_long_one() {
    let config = DfsReadConfig {
        source_single_ttl: Duration::from_millis(60_001),
        ..Default::default()
    };
    assert!(config.validate().is_err());
}

/// The writer is the only candidate and answers busy. With the read cache on,
/// the reader asks Meta once more instead of backing off, finds a cache
/// holder published since its lookup, and reads from it.
#[test]
fn h2_d1_busy_only_holder_refreshes_sources_once_and_reads_the_new_holder() {
    let fixture = Fixture::new();
    let chunk = ChunkId::new("busy-refresh");
    let writer_only = sources_reply(vec![(
        chunk.clone(),
        vec![candidate("w", &chunk, "node-b", CopyRole::DurableReplica)],
    )]);
    let meta = Arc::new(MetaStub::new(writer_only));
    let transfer = Arc::new(NodeTransfer {
        busy: vec!["node-b".into()],
        ..Default::default()
    });
    let engine = fixture
        .engine(meta.clone(), transfer.clone(), DfsReadConfig::default())
        .with_distributed_cache("session".into(), 1 << 30);
    let sources = engine.sources_for(&read_op(chunk.clone(), 4)).unwrap();
    assert_eq!(meta.lookups.load(Ordering::SeqCst), 1);
    // A reader publishes its cache copy after this lookup.
    *meta.reply.lock().unwrap() = sources_reply(vec![(
        chunk.clone(),
        vec![
            candidate("w", &chunk, "node-b", CopyRole::DurableReplica),
            candidate("c", &chunk, "node-c", CopyRole::VerifiedCache),
        ],
    )]);
    let mut out = [0; 4];
    engine
        .transfer
        .block_on_read(Box::pin(engine.read_remote_ranges_async(
            &read_op(chunk.clone(), 4),
            &sources,
            &mut out,
            true,
            Some(Instant::now() + Duration::from_secs(5)),
        )))
        .unwrap();
    assert_eq!(out, [7; 4]);
    assert_eq!(*transfer.served.lock().unwrap(), vec!["node-c".to_owned()]);
    assert_eq!(meta.lookups.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.metric(&cache_event("busy_refreshed")), 1);
}

/// Without the read cache a busy single holder keeps the existing backoff:
/// no extra Meta lookup, and the read ends at its deadline.
#[test]
fn h2_d1_busy_refresh_is_off_without_the_read_cache() {
    let fixture = Fixture::new();
    let chunk = ChunkId::new("busy-plain");
    let meta = Arc::new(MetaStub::new(sources_reply(vec![(
        chunk.clone(),
        vec![candidate("w", &chunk, "node-b", CopyRole::DurableReplica)],
    )])));
    let transfer = Arc::new(NodeTransfer {
        busy: vec!["node-b".into()],
        ..Default::default()
    });
    let engine = fixture.engine(meta.clone(), transfer, DfsReadConfig::default());
    let sources = engine.sources_for(&read_op(chunk.clone(), 4)).unwrap();
    let error = engine
        .transfer
        .block_on_read(Box::pin(engine.read_remote_ranges_async(
            &read_op(chunk, 4),
            &sources,
            &mut [0; 4],
            true,
            Some(Instant::now() + Duration::from_millis(100)),
        )))
        .unwrap_err();
    assert_eq!(error.code(), afs_error::CLIENT_DEADLINE_EXCEEDED);
    assert_eq!(meta.lookups.load(Ordering::SeqCst), 1);
}

// ---- D1: publication ----------------------------------------------------------

fn install_intents(local: &LocalChunkStore, count: u8) -> Vec<ChunkId> {
    (0..count)
        .map(|seed| {
            let staged = StagedChunk::new(OperationId::new("cache"), vec![seed; 128]);
            let receipt = local
                .persist_read_cache(&staged, 1, 1 << 30)
                .unwrap()
                .receipt
                .unwrap();
            local
                .track_read_cache(ReadCacheIntent {
                    namespace_id: crate::dfs::NamespaceId::new("default"),
                    file_version_id: FileVersionId::new("version"),
                    layout_root_id: LayoutRootId::new("layout"),
                    copy: crate::dfs::read_cache_copy(&receipt).unwrap(),
                })
                .unwrap();
            staged.chunk.id
        })
        .collect()
}

/// One pass publishes every unconfirmed intent (the old pass took at most
/// 16, confirmed ones included), several at a time but never more than the
/// bound; the next pass only re-checks a bounded number of confirmed ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_d1_reconcile_publishes_unconfirmed_first_with_bounded_concurrency() {
    let fixture = Fixture::new();
    let installed = install_intents(&fixture.local, 40);
    let meta = Arc::new(MetaStub {
        publish_delay: Duration::from_millis(5),
        ..Default::default()
    });
    let engine = fixture
        .engine(
            meta.clone(),
            Arc::new(NodeTransfer::default()),
            DfsReadConfig::default(),
        )
        .with_read_authority(authority())
        .with_distributed_cache("session".into(), 1 << 30);
    let mut state = cache_reconcile::CacheSyncState::default();
    let deadline = Instant::now() + Duration::from_secs(30);
    let first = engine
        .reconcile_read_caches(&mut state, deadline)
        .await
        .unwrap();
    assert_eq!(first.published, 40);
    assert_eq!(first.pending, 0);
    let published: BTreeSet<_> = meta.published.lock().unwrap().iter().cloned().collect();
    assert_eq!(published, installed.into_iter().collect());
    let peak = meta.peak.load(Ordering::SeqCst);
    assert!(
        (2..=cache_reconcile::PUBLISH_CONCURRENCY).contains(&peak),
        "{peak}"
    );
    assert_eq!(fixture.metric(&cache_event("published")), 40);
    let second = engine
        .reconcile_read_caches(&mut state, deadline)
        .await
        .unwrap();
    assert_eq!(second, cache_reconcile::ReconcileSummary::default());
    assert_eq!(meta.published.lock().unwrap().len(), 40);
}

/// More unconfirmed intents than one pass takes: the pass reports what is
/// left, so the loop runs again at once instead of waiting for its tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_d1_reconcile_reports_unconfirmed_intents_left_for_the_next_pass() {
    let fixture = Fixture::new();
    let total = cache_reconcile::MAX_UNCONFIRMED_PER_PASS + 3;
    install_intents(&fixture.local, u8::try_from(total).unwrap());
    let meta = Arc::new(MetaStub::default());
    let engine = fixture
        .engine(
            meta.clone(),
            Arc::new(NodeTransfer::default()),
            DfsReadConfig::default(),
        )
        .with_read_authority(authority())
        .with_distributed_cache("session".into(), 1 << 30);
    let mut state = cache_reconcile::CacheSyncState::default();
    let deadline = Instant::now() + Duration::from_secs(30);
    let first = engine
        .reconcile_read_caches(&mut state, deadline)
        .await
        .unwrap();
    assert_eq!(first.published, cache_reconcile::MAX_UNCONFIRMED_PER_PASS);
    assert_eq!(first.pending, 3);
    let second = engine
        .reconcile_read_caches(&mut state, deadline)
        .await
        .unwrap();
    assert_eq!(second.published, 3);
    assert_eq!(second.pending, 0);
}

/// An installed copy is published without waiting for the once-per-second
/// tick, and the installer keeps draining while publication runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_d1_install_wakes_publication_before_the_periodic_tick() {
    let fixture = Fixture::new();
    let meta = Arc::new(MetaStub::default());
    // The periodic tick is an hour away: only the install wake-up can publish.
    let engine = Arc::new(
        fixture
            .engine(
                meta.clone(),
                Arc::new(NodeTransfer::default()),
                DfsReadConfig::default(),
            )
            .with_read_authority(authority())
            .with_distributed_cache_recheck("session".into(), 1 << 30, Duration::from_secs(3600)),
    );
    let mut services = crate::runtime::Services::new();
    engine.start_distributed_cache(&mut services);
    let staged = StagedChunk::new(OperationId::new("wake"), vec![3; 256]);
    let chunk = staged.chunk.clone();
    engine.cache_completed_ranges(
        &read_op(chunk.id.clone(), chunk.length),
        &[ChunkSources {
            chunk_id: chunk.id.clone(),
            chunk: Some(chunk.clone()),
            allowed_ranges: vec![(0, chunk.length)],
            sources: Vec::new(),
        }],
        &[3; 256],
    );
    let started = Instant::now();
    while meta.published.lock().unwrap().is_empty() {
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "publication waited for the periodic tick"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(*meta.published.lock().unwrap(), vec![chunk.id]);
    assert_eq!(fixture.metric(&cache_event("installed")), 1);
}

// ---- D3: candidate order --------------------------------------------------------

fn chunk_set(chunk: &ChunkId, holders: &[(&str, CopyRole)]) -> ChunkSources {
    ChunkSources {
        chunk_id: chunk.clone(),
        chunk: None,
        allowed_ranges: Vec::new(),
        sources: holders
            .iter()
            .map(|(node, role)| candidate(&format!("{node}-copy"), chunk, node, *role))
            .collect(),
    }
}

fn first_node(set: &ChunkSources) -> String {
    match &set.sources[0].location {
        CopyLocation::Node { node_id, .. } => node_id.clone(),
        CopyLocation::External { .. } => panic!("node expected"),
    }
}

/// When any cache holder is offered, every durable copy (the writer) is
/// ordered after the cache holders, at any parallelism.
#[test]
fn h2_d3_durable_copies_are_the_fallback_when_a_cache_holder_exists() {
    let fixture = Fixture::new();
    for parallelism in [1, 4] {
        let engine = fixture
            .engine(
                Arc::new(MetaStub::default()),
                Arc::new(NodeTransfer::default()),
                DfsReadConfig::default(),
            )
            .with_distribution(ReadDistributionConfig {
                parallelism,
                ..Default::default()
            })
            .unwrap();
        for index in 0..32 {
            let chunk = ChunkId::new(format!("fallback-{index}"));
            let set = chunk_set(
                &chunk,
                &[
                    ("node-a-writer", CopyRole::DurableReplica),
                    ("node-c", CopyRole::VerifiedCache),
                    ("node-d", CopyRole::VerifiedCache),
                ],
            );
            let batch = read_op(chunk, 1);
            let ordered = engine.distributed_sources(&batch, &[set]);
            let roles: Vec<_> = ordered[0].sources.iter().map(|s| s.role).collect();
            assert_eq!(
                roles,
                vec![
                    CopyRole::VerifiedCache,
                    CopyRole::VerifiedCache,
                    CopyRole::DurableReplica
                ]
            );
        }
    }
}

/// Rendezvous order: one Chunk always has the same first choice, and
/// different Chunks spread over the holders (P1: two durable replicas).
#[test]
fn h2_d3_rendezvous_order_is_stable_per_chunk_and_spreads_chunks() {
    let fixture = Fixture::new();
    let engine = fixture.engine(
        Arc::new(MetaStub::default()),
        Arc::new(NodeTransfer::default()),
        DfsReadConfig::default(),
    );
    let mut firsts: Map<String, usize> = Map::new();
    for index in 0..128 {
        let chunk = ChunkId::new(format!("spread-{index}"));
        let set = chunk_set(
            &chunk,
            &[
                ("node-b", CopyRole::DurableReplica),
                ("node-c", CopyRole::DurableReplica),
            ],
        );
        let batch = read_op(chunk, 1);
        let first = first_node(&engine.distributed_sources(&batch, std::slice::from_ref(&set))[0]);
        for _ in 0..3 {
            assert_eq!(
                first_node(&engine.distributed_sources(&batch, std::slice::from_ref(&set))[0]),
                first
            );
        }
        // Meta order does not decide the first choice.
        let mut reversed = set;
        reversed.sources.reverse();
        assert_eq!(
            first_node(&engine.distributed_sources(&batch, &[reversed])[0]),
            first
        );
        *firsts.entry(first).or_default() += 1;
    }
    assert_eq!(firsts.len(), 2);
    assert!(firsts.values().all(|count| *count >= 40), "{firsts:?}");
}

/// D3-5: with the default parallelism of 1, a reader of a file with two
/// replicas reads its Chunks from both, not only from Meta's first copy.
#[test]
fn h2_d3_parallelism_one_reads_spread_over_replicas() {
    let fixture = Fixture::new();
    let chunks: Vec<ChunkId> = (0..64)
        .map(|index| ChunkId::new(format!("p1-{index}")))
        .collect();
    let meta = Arc::new(MetaStub::new(sources_reply(
        chunks
            .iter()
            .map(|chunk| {
                (
                    chunk.clone(),
                    vec![
                        candidate("b", chunk, "node-b", CopyRole::DurableReplica),
                        candidate("c", chunk, "node-c", CopyRole::DurableReplica),
                    ],
                )
            })
            .collect(),
    )));
    let transfer = Arc::new(NodeTransfer::default());
    let engine = fixture.engine(meta, transfer.clone(), DfsReadConfig::default());
    for chunk in &chunks {
        engine
            .read_batch(&read_op(chunk.clone(), 4), &mut [0; 4])
            .unwrap();
    }
    let served = transfer.served.lock().unwrap();
    let from_b = served.iter().filter(|node| *node == "node-b").count();
    assert_eq!(served.len(), 64);
    assert!((16..=48).contains(&from_b), "{from_b} of 64 from node-b");
}

#[test]
fn h2_d0_remote_mask_counts_only_locally_served_bytes() {
    let mut batch = read_op(ChunkId::new("one"), 5);
    batch.ops.push(ChunkReadOp {
        chunk_id: ChunkId::new("two"),
        chunk_offset: 0,
        length: 7,
        output_offset: 5,
    });
    let mut mask = route::RemoteMask::new(2);
    assert_eq!(mask.local_bytes(&batch), 12);
    mask.mark(1);
    assert_eq!(mask.local_bytes(&batch), 5);
}

// ---- D1-6: byte-budgeted staging ---------------------------------------------

const MIB: u64 = 1024 * 1024;

fn budget_engine(fixture: &Fixture, queue_bytes: u64, partial_bytes: u64) -> DfsReadEngine {
    fixture
        .engine(
            Arc::new(MetaStub::default()),
            Arc::new(NodeTransfer::default()),
            DfsReadConfig::default(),
        )
        .with_read_authority(authority())
        .with_distributed_cache_budgets(
            "session".into(),
            1 << 30,
            ReadCacheBudgets {
                queue_bytes,
                partial_bytes,
            },
        )
        .unwrap()
}

/// Stages `[offset, offset + length)` of a body of `size` bytes filled with `seed`.
fn stage_range(engine: &DfsReadEngine, seed: u8, size: usize, offset: u64, length: u64) -> ChunkId {
    let chunk = StagedChunk::new(OperationId::new("budget"), vec![seed; size]).chunk;
    let mut batch = read_op(chunk.id.clone(), length);
    batch.ops[0].chunk_offset = offset;
    engine.cache_completed_ranges(
        &batch,
        &[ChunkSources {
            chunk_id: chunk.id.clone(),
            chunk: Some(chunk.clone()),
            allowed_ranges: vec![(0, chunk.length)],
            sources: Vec::new(),
        }],
        &vec![seed; length as usize],
    );
    // No assembler task runs in these tests: merge the segments inline.
    if let Some(cache) = &engine.distributed_cache {
        cache.assemble_pending(&engine.local);
    }
    chunk.id
}

fn resource(name: &str) -> String {
    format!("afs_dfs_read_cache_resources{{resource=\"{name}\"}}")
}

/// The FUSE path takes queue bytes without waiting: bodies fit until the
/// byte budget is used up, the next one is a counted drop, and the held
/// bytes are exported as reader memory.
#[test]
fn h2_d16_queue_budget_admits_by_bytes_and_drops_without_waiting() {
    let fixture = Fixture::new();
    let engine = budget_engine(&fixture, 4 * MIB, 4 * MIB);
    for seed in 0..5 {
        stage_range(&engine, seed, MIB as usize, 0, MIB);
    }
    // The dropped body was never copied: one body copy per staged body.
    let copies = engine
        .distributed_cache
        .as_ref()
        .unwrap()
        .body_copies
        .load(Ordering::SeqCst);
    assert_eq!(copies, 4);
    assert_eq!(fixture.metric(&cache_event("staged")), 4);
    assert_eq!(fixture.metric(&cache_event("dropped_queue_full")), 1);
    assert_eq!(fixture.metric(&resource("queue_bytes")), 4 * MIB);
    assert_eq!(fixture.metric(&resource("queue_peak_bytes")), 4 * MIB);
    assert_eq!(fixture.metric(&resource("queue_limit_bytes")), 4 * MIB);
}

/// Four interleaved partial bodies fit a budget of four (the old fixed cap
/// of two dropped them); a fifth pushes out the oldest, which is counted.
#[test]
fn h2_d16_partial_budget_bounds_interleaved_bodies_by_bytes() {
    let fixture = Fixture::new();
    let engine = budget_engine(&fixture, 64 * MIB, 4 * MIB);
    let half = MIB / 2;
    for seed in 0..4 {
        stage_range(&engine, seed, MIB as usize, 0, half);
    }
    assert_eq!(fixture.metric(&resource("partial_bytes")), 4 * MIB);
    for seed in 0..4 {
        stage_range(&engine, seed, MIB as usize, half, half);
    }
    assert_eq!(fixture.metric(&cache_event("staged")), 4);
    assert_eq!(fixture.metric(&cache_event("dropped_partial")), 0);
    assert_eq!(fixture.metric(&resource("partial_bytes")), 0);
    for seed in 10..15 {
        stage_range(&engine, seed, MIB as usize, 0, half);
    }
    assert_eq!(fixture.metric(&cache_event("dropped_partial")), 1);
    assert_eq!(fixture.metric(&resource("partial_peak_bytes")), 4 * MIB);
}

#[test]
fn h2_d16_zero_partial_budget_disables_assembly() {
    let fixture = Fixture::new();
    let engine = budget_engine(&fixture, 4 * MIB, 0);
    stage_range(&engine, 1, MIB as usize, 0, MIB / 2);
    assert_eq!(fixture.metric(&cache_event("dropped_partial")), 1);
    assert_eq!(fixture.metric(&resource("partial_bytes")), 0);
}

#[test]
fn h2_d16_budgets_are_validated() {
    for (queue_bytes, partial_bytes) in [
        (4 * MIB - 1, 0),
        (1025 * MIB, 0),
        (4 * MIB, 1),
        (4 * MIB, 1025 * MIB),
    ] {
        assert!(
            ReadCacheBudgets {
                queue_bytes,
                partial_bytes
            }
            .validate()
            .is_err()
        );
    }
    assert!(ReadCacheBudgets::default().validate().is_ok());
    assert_eq!(ReadCacheBudgets::default().queue_bytes, 32 * MIB);
    assert_eq!(ReadCacheBudgets::default().partial_bytes, 32 * MIB);
}

/// Installation releases the queue bytes, and queue wait and install time
/// are recorded per body, so a full queue can be told apart from a slow or
/// blocked installer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_d16_installed_bodies_release_queue_bytes_and_record_work() {
    let fixture = Fixture::new();
    let engine = Arc::new(budget_engine(&fixture, 4 * MIB, 4 * MIB));
    for seed in 0..3 {
        stage_range(&engine, seed, MIB as usize, 0, MIB);
    }
    assert_eq!(fixture.metric(&resource("queue_bytes")), 3 * MIB);
    let mut services = crate::runtime::Services::new();
    engine.start_distributed_cache(&mut services);
    let started = Instant::now();
    while fixture.metric(&cache_event("installed")) < 3 {
        assert!(started.elapsed() < Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    while fixture.metric(&resource("queue_bytes")) != 0 {
        assert!(started.elapsed() < Duration::from_secs(10));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        fixture.metric("afs_dfs_read_cache_work_total{stage=\"install\"}"),
        3
    );
    assert_eq!(
        fixture.metric("afs_dfs_read_cache_work_total{stage=\"queue_wait\"}"),
        3
    );
    assert!(fixture.metric("afs_dfs_read_cache_work_nanoseconds_total{stage=\"queue_wait\"}") > 0);
}

/// Only the background prefetch path waits for queue bytes, and only until
/// its own deadline; freed bytes wake it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_d16_prefetch_waits_for_queue_bytes_only_until_its_deadline() {
    let budget = cache_reconcile::StageBudget::new_for_test(4 * MIB);
    let held = budget.try_charge(4 * MIB).unwrap();
    let started = Instant::now();
    assert!(
        budget
            .charge_until(MIB, Instant::now() + Duration::from_millis(50))
            .await
            .is_err()
    );
    assert!(started.elapsed() >= Duration::from_millis(45));
    let release = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(held);
    });
    let charge = budget
        .charge_until(MIB, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    release.await.unwrap();
    drop(charge);
    assert!(budget.try_charge(4 * MIB).is_some());
}

// ---- Review fixes (review/11-h2-step1-code-review.md) ------------------------

/// HIGH: intents whose publication keeps failing back off; the pass still
/// reports, and the next pass publishes the intents behind them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_review_failing_intents_back_off_and_do_not_block_the_rest() {
    let fixture = Fixture::new();
    let total = cache_reconcile::MAX_UNCONFIRMED_PER_PASS + 2;
    install_intents(&fixture.local, u8::try_from(total).unwrap());
    let order: Vec<ChunkId> = fixture
        .local
        .read_cache_intents()
        .unwrap()
        .into_iter()
        .map(|intent| intent.copy.chunk_id)
        .collect();
    let failing: BTreeSet<ChunkId> = order[..cache_reconcile::MAX_UNCONFIRMED_PER_PASS]
        .iter()
        .cloned()
        .collect();
    let meta = Arc::new(MetaStub {
        fail_publish: Mutex::new(failing.clone()),
        ..Default::default()
    });
    let engine = fixture
        .engine(
            meta.clone(),
            Arc::new(NodeTransfer::default()),
            DfsReadConfig::default(),
        )
        .with_read_authority(authority())
        .with_distributed_cache("session".into(), 1 << 30);
    let mut state = cache_reconcile::CacheSyncState::default();
    let deadline = Instant::now() + Duration::from_secs(60);
    let first = engine
        .reconcile_read_caches(&mut state, deadline)
        .await
        .unwrap();
    assert_eq!(first.failed, cache_reconcile::MAX_UNCONFIRMED_PER_PASS);
    assert_eq!(first.pending, 2);
    let second = engine
        .reconcile_read_caches(&mut state, deadline)
        .await
        .unwrap();
    assert_eq!(second.published, 2);
    assert_eq!(second.backed_off, cache_reconcile::MAX_UNCONFIRMED_PER_PASS);
    let published: BTreeSet<ChunkId> = meta.published.lock().unwrap().iter().cloned().collect();
    assert!(published.is_disjoint(&failing));
    assert_eq!(published.len(), 2);
}

/// MEDIUM: an intent replaced after the pass listed it (evicted and
/// installed again) is skipped, not counted as a failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_review_replaced_intent_is_unchanged_not_failed() {
    let fixture = Fixture::new();
    install_intents(&fixture.local, 1);
    let mut stale = fixture.local.read_cache_intents().unwrap().remove(0);
    stale.file_version_id = FileVersionId::new("older-version");
    let engine = fixture
        .engine(
            Arc::new(MetaStub::default()),
            Arc::new(NodeTransfer::default()),
            DfsReadConfig::default(),
        )
        .with_read_authority(authority())
        .with_distributed_cache("session".into(), 1 << 30);
    let outcome = engine
        .reconcile_intent(
            "session",
            1,
            stale,
            cache_reconcile::IntentCheck::Full,
            Instant::now() + Duration::from_secs(10),
        )
        .await;
    assert!(
        matches!(outcome, Ok(cache_reconcile::IntentOutcome::Unchanged)),
        "{outcome:?}"
    );
}

fn busy_writer_engine(
    fixture: &Fixture,
) -> (DfsReadEngine, Arc<MetaStub>, Vec<ChunkSources>, ChunkId) {
    let chunk = ChunkId::new("busy-deadline");
    let meta = Arc::new(MetaStub::new(sources_reply(vec![(
        chunk.clone(),
        vec![candidate("w", &chunk, "node-b", CopyRole::DurableReplica)],
    )])));
    let engine = fixture
        .engine(
            meta.clone(),
            Arc::new(NodeTransfer {
                busy: vec!["node-b".into()],
                ..Default::default()
            }),
            DfsReadConfig::default(),
        )
        .with_distributed_cache("session".into(), 1 << 30);
    let sources = engine.sources_for(&read_op(chunk.clone(), 4)).unwrap();
    (engine, meta, sources, chunk)
}

/// MEDIUM: a slow Meta cannot stretch a read past its deadline through the
/// busy refresh, and with too little time left no refresh is sent at all.
#[test]
fn h2_review_busy_refresh_is_bounded_by_the_read_deadline() {
    let fixture = Fixture::new();
    let (engine, meta, sources, chunk) = busy_writer_engine(&fixture);
    meta.lookup_delay_ms.store(3_000, Ordering::SeqCst);
    let started = Instant::now();
    let error = engine
        .transfer
        .block_on_read(Box::pin(engine.read_remote_ranges_async(
            &read_op(chunk.clone(), 4),
            &sources,
            &mut [0; 4],
            true,
            Some(Instant::now() + Duration::from_millis(300)),
        )))
        .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_millis(2_000),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(error.code(), afs_error::CLIENT_DEADLINE_EXCEEDED);

    let (engine, meta, sources, chunk) = busy_writer_engine(&fixture);
    engine
        .transfer
        .block_on_read(Box::pin(engine.read_remote_ranges_async(
            &read_op(chunk, 4),
            &sources,
            &mut [0; 4],
            true,
            Some(Instant::now() + Duration::from_millis(20)),
        )))
        .unwrap_err();
    assert_eq!(
        meta.lookups.load(Ordering::SeqCst),
        1,
        "no refresh with 20 ms left"
    );
}

/// MEDIUM: a cache holder that failed a read (its copy is gone) is ordered
/// last for a while, so the next read goes to the writer first.
#[test]
fn h2_review_failed_cache_holder_is_demoted_for_later_reads() {
    let fixture = Fixture::new();
    let chunk = ChunkId::new("gone-holder");
    let meta = Arc::new(MetaStub::new(sources_reply(vec![(
        chunk.clone(),
        vec![
            candidate("w", &chunk, "node-b", CopyRole::DurableReplica),
            candidate("c", &chunk, "node-c", CopyRole::VerifiedCache),
        ],
    )])));
    let transfer = Arc::new(NodeTransfer {
        missing: vec!["node-c".into()],
        ..Default::default()
    });
    let engine = fixture.engine(meta, transfer.clone(), DfsReadConfig::default());
    engine
        .read_batch(&read_op(chunk.clone(), 4), &mut [0; 4])
        .unwrap();
    assert_eq!(
        *transfer.attempts.lock().unwrap(),
        vec!["node-c".to_owned(), "node-b".to_owned()]
    );
    assert_eq!(fixture.metric(&cache_event("holder_demoted")), 1);
    transfer.attempts.lock().unwrap().clear();
    engine.read_batch(&read_op(chunk, 4), &mut [0; 4]).unwrap();
    assert_eq!(
        *transfer.attempts.lock().unwrap(),
        vec!["node-b".to_owned()]
    );
}

/// LOW 1: when every candidate's grant has expired the list is kept as it
/// was (the holder's refusal then reauthorizes) instead of becoming empty.
#[test]
fn h2_review_all_stale_candidates_are_kept_for_reauthorization() {
    let fixture = Fixture::new();
    let engine = fixture.engine(
        Arc::new(MetaStub::default()),
        Arc::new(NodeTransfer::default()),
        DfsReadConfig::default(),
    );
    let chunk = ChunkId::new("stale");
    let mut set = chunk_set(
        &chunk,
        &[
            ("node-b", CopyRole::DurableReplica),
            ("node-c", CopyRole::DurableReplica),
        ],
    );
    for source in &mut set.sources {
        source.read_grant.expires_at_unix_ms = 1;
    }
    let ordered = engine.distributed_sources(&read_op(chunk, 1), &[set]);
    assert_eq!(ordered[0].sources.len(), 2);
}

/// The installer keeps draining while a publication is in progress
/// (installation and publication run in separate tasks).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_review_installer_keeps_draining_while_publication_is_slow() {
    let fixture = Fixture::new();
    let meta = Arc::new(MetaStub {
        publish_delay: Duration::from_secs(20),
        ..Default::default()
    });
    let engine = Arc::new(
        fixture
            .engine(
                meta.clone(),
                Arc::new(NodeTransfer::default()),
                DfsReadConfig::default(),
            )
            .with_read_authority(authority())
            .with_distributed_cache_recheck("session".into(), 1 << 30, Duration::from_secs(3600)),
    );
    let mut services = crate::runtime::Services::new();
    engine.start_distributed_cache(&mut services);
    stage_range(&engine, 1, 256, 0, 256);
    let started = Instant::now();
    while meta.active.load(Ordering::SeqCst) == 0 {
        assert!(started.elapsed() < Duration::from_secs(15));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // A publication is now parked for 20 s; three more bodies still install.
    for seed in 2..5 {
        stage_range(&engine, seed, 256, 0, 256);
    }
    while fixture.metric(&cache_event("installed")) < 4 {
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "installs waited for publication"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(meta.published.lock().unwrap().is_empty());
}

/// Bodies still queued when the read cache goes away release their bytes.
#[test]
fn h2_review_dropping_the_engine_releases_queued_bytes() {
    let fixture = Fixture::new();
    let engine = budget_engine(&fixture, 4 * MIB, 4 * MIB);
    stage_range(&engine, 1, MIB as usize, 0, MIB);
    stage_range(&engine, 2, MIB as usize, 0, MIB / 2);
    assert_eq!(fixture.metric(&resource("queue_bytes")), MIB);
    assert_eq!(fixture.metric(&resource("partial_bytes")), MIB);
    drop(engine);
    assert_eq!(fixture.metric(&resource("queue_bytes")), 0);
    assert_eq!(fixture.metric(&resource("partial_bytes")), 0);
}

// ---- Partial segment loss (acceptance 2026-11-20) -----------------------------

/// Many FUSE completions of the same Chunks at once (readahead keeps ~11 in
/// flight): every Chunk is still assembled and staged, none is lost.
#[test]
fn h2_partial_concurrent_completions_do_not_lose_segments() {
    let fixture = Fixture::new();
    let engine = Arc::new(budget_engine(&fixture, 1024 * MIB, 64 * MIB));
    let size = 4 * MIB as usize;
    let chunks: Vec<ChunkObject> = (0..8)
        .map(|seed| StagedChunk::new(OperationId::new("race"), vec![seed; size]).chunk)
        .collect();
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let threads: Vec<_> = (0..4)
        .map(|part| {
            let (engine, chunks, barrier) = (engine.clone(), chunks.clone(), barrier.clone());
            std::thread::spawn(move || {
                barrier.wait();
                for (seed, chunk) in chunks.iter().enumerate() {
                    let mut batch = read_op(chunk.id.clone(), MIB);
                    batch.ops[0].chunk_offset = part * MIB;
                    engine.cache_completed_ranges(
                        &batch,
                        &[ChunkSources {
                            chunk_id: chunk.id.clone(),
                            chunk: Some(chunk.clone()),
                            allowed_ranges: vec![(0, chunk.length)],
                            sources: Vec::new(),
                        }],
                        &vec![seed as u8; MIB as usize],
                    );
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    let cache = engine.distributed_cache.as_ref().unwrap();
    cache.assemble_pending(&engine.local);
    assert_eq!(fixture.metric(&cache_event("staged")), 8);
    assert_eq!(fixture.metric(&cache_event("dropped_partial")), 0);
    assert_eq!(fixture.metric(&cache_event("dropped_segment_full")), 0);
    assert_eq!(fixture.metric(&resource("partial_bytes")), 0);
    assert_eq!(fixture.metric(&resource("segment_bytes")), 0);
}

/// A partial body that lost a segment is released after `PARTIAL_MAX_AGE`
/// instead of holding its budget until newer Chunks push it out.
#[test]
fn h2_partial_incomplete_bodies_expire() {
    let fixture = Fixture::new();
    let engine = budget_engine(&fixture, 4 * MIB, 4 * MIB);
    stage_range(&engine, 1, MIB as usize, 0, MIB / 2);
    assert_eq!(fixture.metric(&resource("partial_bytes")), MIB);
    let cache = engine.distributed_cache.as_ref().unwrap();
    cache.sweep_partial_at(&engine.local, Instant::now());
    assert_eq!(fixture.metric(&resource("partial_bytes")), MIB);
    cache.sweep_partial_at(
        &engine.local,
        Instant::now() + cache_reconcile::PARTIAL_MAX_AGE + Duration::from_millis(1),
    );
    assert_eq!(fixture.metric(&resource("partial_bytes")), 0);
    assert_eq!(fixture.metric(&cache_event("dropped_partial_expired")), 1);
}

/// The FUSE path takes only the segment budget; a full one is a counted
/// drop and nothing waits.
#[test]
fn h2_partial_full_segment_budget_drops_without_waiting() {
    let fixture = Fixture::new();
    // Partial 4 MiB -> segments in flight up to 2 MiB.
    let engine = budget_engine(&fixture, 4 * MIB, 4 * MIB);
    for seed in 0..3 {
        let chunk = StagedChunk::new(OperationId::new("seg"), vec![seed; MIB as usize]).chunk;
        engine.cache_completed_ranges(
            &read_op(chunk.id.clone(), MIB / 2 + MIB / 4),
            &[ChunkSources {
                chunk_id: chunk.id.clone(),
                chunk: Some(chunk.clone()),
                allowed_ranges: vec![(0, chunk.length)],
                sources: Vec::new(),
            }],
            &vec![seed; (MIB / 2 + MIB / 4) as usize],
        );
    }
    assert_eq!(fixture.metric(&cache_event("dropped_segment_full")), 1);
    assert_eq!(
        fixture.metric(&resource("segment_bytes")),
        2 * (MIB / 2 + MIB / 4)
    );
}

// ---- CPU waste fixes (review/13 §3) -----------------------------------------

fn crc_job(seed: u8) -> (cache_reconcile::CacheJob, ChunkObject, Vec<u8>) {
    let body: Vec<u8> = (0..256 * 1024).map(|i| (i as u8) ^ seed).collect();
    let chunk = StagedChunk::new_crc32c(OperationId::new("crc"), body.clone())
        .unwrap()
        .chunk;
    let job = cache_reconcile::CacheJob::new(
        chunk.clone(),
        body.clone(),
        FileVersionId::new("version"),
        LayoutRootId::new("layout"),
        1,
    );
    (job, chunk, body)
}

fn cpu_engine(fixture: &Fixture) -> (Arc<DfsReadEngine>, Arc<MetaStub>) {
    let meta = Arc::new(MetaStub::default());
    let engine = Arc::new(
        fixture
            .engine(
                meta.clone(),
                Arc::new(NodeTransfer::default()),
                DfsReadConfig::default(),
            )
            .with_read_authority(authority())
            .with_distributed_cache("session".into(), 1 << 30),
    );
    (engine, meta)
}

/// Fix 1: a copy this process just installed is published without being
/// read back and CRC-verified again; a copy it did not install (as after a
/// restart) is still fully verified first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_cpu_fresh_install_is_published_without_full_verification() {
    let fixture = Fixture::new();
    let (engine, meta) = cpu_engine(&fixture);
    let (job, chunk, _) = crc_job(1);
    engine.run_install_job(job, 1 << 30).await;
    assert_eq!(fixture.metric(&cache_event("installed")), 1);
    let mut state = cache_reconcile::CacheSyncState::default();
    let deadline = Instant::now() + Duration::from_secs(30);
    let pass = engine
        .reconcile_read_caches(&mut state, deadline)
        .await
        .unwrap();
    assert_eq!(pass.published, 1);
    assert_eq!(*meta.published.lock().unwrap(), vec![chunk.id.clone()]);
    assert_eq!(fixture.metric(&cache_event("publish_verify_skipped")), 1);
    assert_eq!(fixture.metric(&cache_event("publish_verify_full")), 0);

    // A second engine over the same store has installed nothing itself:
    // the intent it finds is fully verified before it is published.
    let (restarted, _) = cpu_engine(&fixture);
    let mut state = cache_reconcile::CacheSyncState::default();
    restarted
        .reconcile_read_caches(&mut state, deadline)
        .await
        .unwrap();
    assert_eq!(fixture.metric(&cache_event("publish_verify_full")), 1);
}

/// Fix 1: an intent replaced after installation (different revision) is
/// not taken as fresh.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_cpu_changed_intent_is_fully_verified() {
    let fixture = Fixture::new();
    let (engine, _) = cpu_engine(&fixture);
    let (job, _, _) = crc_job(2);
    engine.run_install_job(job, 1 << 30).await;
    let mut intent = fixture.local.read_cache_intents().unwrap().remove(0);
    intent.file_version_id = FileVersionId::new("other-version");
    let cache = engine.distributed_cache.as_ref().unwrap();
    assert!(!cache.take_fresh_install_for_test(&intent));
}

/// Fix 2: the install reuses the segment table the bytes were verified
/// against on receive instead of computing it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_cpu_install_reuses_the_trusted_crc_table() {
    let fixture = Fixture::new();
    let (engine, _) = cpu_engine(&fixture);
    let (job, chunk, body) = crc_job(3);
    let table = Arc::new(crate::node::chunk_crc32c::CrcTable::build(&body).unwrap());
    engine.run_install_job(job.with_table(table), 1 << 30).await;
    assert_eq!(fixture.metric(&cache_event("installed")), 1);
    assert_eq!(fixture.metric(&cache_event("crc_table_reused")), 1);
    assert_eq!(fixture.metric(&cache_event("crc_table_built")), 0);
    let mut stored = vec![0; body.len()];
    assert_eq!(
        fixture.local.read_at(&chunk.id, 0, &mut stored).unwrap(),
        body.len()
    );
    assert_eq!(stored, body);
}

/// Fix 2: a table whose digest is not the Chunk's trusted identity is never
/// used; the table is computed from the bytes as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_cpu_untrusted_table_is_not_reused() {
    let fixture = Fixture::new();
    let (engine, _) = cpu_engine(&fixture);
    let (job, _, _) = crc_job(4);
    let other = Arc::new(crate::node::chunk_crc32c::CrcTable::build(&[9; 256 * 1024]).unwrap());
    engine
        .run_install_job(job.with_table(other.clone()), 1 << 30)
        .await;
    assert_eq!(fixture.metric(&cache_event("crc_table_reused")), 0);
    assert_eq!(fixture.metric(&cache_event("crc_table_built")), 1);
    // The read path only remembers tables bound to the Chunk identity.
    let (_, chunk, body) = crc_job(5);
    engine.remember_trusted_crc_table(&chunk, (*other).clone());
    assert!(engine.trusted_crc_table(&chunk.id).is_none());
    engine.remember_trusted_crc_table(
        &chunk,
        crate::node::chunk_crc32c::CrcTable::build(&body).unwrap(),
    );
    assert!(engine.trusted_crc_table(&chunk.id).is_some());
}

fn stage_segment(
    engine: &DfsReadEngine,
    chunk: &ChunkObject,
    body: &[u8],
    offset: usize,
    len: usize,
) {
    let mut batch = read_op(chunk.id.clone(), len as u64);
    batch.ops[0].chunk_offset = offset as u64;
    engine.cache_completed_ranges(
        &batch,
        &[ChunkSources {
            chunk_id: chunk.id.clone(),
            chunk: Some(chunk.clone()),
            allowed_ranges: vec![(0, chunk.length)],
            sources: Vec::new(),
        }],
        &body[offset..offset + len],
    );
}

/// Fix 3: in-order segments need no zeroed body: the first segment becomes
/// the body and later ones are appended; consumed segment buffers are pooled.
#[test]
fn h2_cpu_in_order_segments_adopt_the_first_and_never_zero_fill() {
    let fixture = Fixture::new();
    let engine = budget_engine(&fixture, 64 * MIB, 32 * MIB);
    let body: Vec<u8> = (0..4 * MIB as usize).map(|i| (i % 253) as u8).collect();
    let chunk = StagedChunk::new(OperationId::new("order"), body.clone()).chunk;
    let cache = engine.distributed_cache.as_ref().unwrap();
    for part in 0..4 {
        stage_segment(&engine, &chunk, &body, part * MIB as usize, MIB as usize);
    }
    cache.assemble_pending(&engine.local);
    assert_eq!(cache.adopted_bodies.load(Ordering::SeqCst), 1);
    assert_eq!(cache.zero_fill_bytes.load(Ordering::SeqCst), 0);
    assert_eq!(cache.pooled_segments_for_test(), 3);
    let jobs = cache.queued_jobs_for_test();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].body(), &body[..]);
}

/// Fix 3: out of order, only the gap before a segment is zero filled, and
/// the assembled body is still exact.
#[test]
fn h2_cpu_out_of_order_segments_zero_fill_only_the_gap() {
    let fixture = Fixture::new();
    let engine = budget_engine(&fixture, 64 * MIB, 32 * MIB);
    let body: Vec<u8> = (0..4 * MIB as usize).map(|i| (i % 251) as u8).collect();
    let chunk = StagedChunk::new(OperationId::new("gap"), body.clone()).chunk;
    let cache = engine.distributed_cache.as_ref().unwrap();
    for part in [1, 0, 2, 3] {
        stage_segment(&engine, &chunk, &body, part * MIB as usize, MIB as usize);
    }
    cache.assemble_pending(&engine.local);
    assert_eq!(cache.adopted_bodies.load(Ordering::SeqCst), 0);
    assert_eq!(cache.zero_fill_bytes.load(Ordering::SeqCst), MIB);
    let jobs = cache.queued_jobs_for_test();
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].body(), &body[..]);
}
