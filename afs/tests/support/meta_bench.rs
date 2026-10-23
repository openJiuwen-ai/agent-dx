//! Explicit, metadata-only measurement helper. Never included in a release package.
//!
//! Dataset construction and recovery run separately from timed requests. Store
//! measurements use the public transaction API; service-smoke checks the domain
//! create path independently. Neither path measures FUSE, data I/O, or network RPC.

use afs::meta::store::{
    journal::{CheckpointLimits, LogLimits, LogStatus, LogTip, MaintenancePolicy, RecoveredLog},
    log::Record,
};
use std::{
    collections::BTreeMap,
    error::Error,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use afs::{
    dfs::{
        ChunkEncoding, ChunkId, ChunkObject, ContentDigest, CopyId, CopyLocation, CopyRecord,
        CopyRole, CopyState, Dentry, DentryKey, DigestAlgorithm, Extent, FileVersion,
        FileVersionId, InodeAttributes, InodeId, InodeKind, InodeRecord, LayoutRoot, LayoutRootId,
        NamespaceId, OperationId, PlacementHealth, PlacementRecord, ReplicaGroupId,
    },
    meta::{
        dfs::{CreateFileRequest, DfsService},
        store::{
            BackendPersistence, MetaEntity, MetaFuture, MetaKey, MetaLog, MetaRead, MetaStore,
            MetaTxn, NodeSessionLease, OperationResult, RequestKey, RequestOutcome, Store,
            StoreOperation, StoreRevision, TxnCondition, TxnMutation, TxnOutcome, WatchBatch,
            local_log::LocalLog,
        },
    },
};
use afs_tracing::tracing;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const CHUNK_BYTES: u64 = 4 * 1024 * 1024;
const MAX_LATENCY_BINS: usize = 262_144;
const SEED_BATCH_FILES: u64 = 256;
const NAMESPACE: &str = "meta-bench";
const ROOT: &str = "meta-bench-root";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ValueEnum)]
enum Dataset {
    S,
    M,
    L10,
    L,
}

impl Dataset {
    fn dimensions(
        self,
        qualification_inodes: Option<u64>,
        copies: Option<u16>,
    ) -> BenchResult<Dimensions> {
        let files =
            qualification_inodes.unwrap_or(if self == Self::L { 10_000_000 } else { 100_000 });
        let chunks = match self {
            Self::S => 1,
            Self::M => 4,
            Self::L10 | Self::L => 0,
        };
        let copies = copies.unwrap_or(if self == Self::M {
            3
        } else {
            u16::from(chunks != 0)
        });
        if files == 0
            || !matches!(copies, 0 | 1 | 3)
            || (self == Self::M && copies != 3)
            || (chunks == 0 && copies != 0)
            || (chunks != 0 && copies == 0)
        {
            return Err("invalid dataset size or copy count".into());
        }
        Ok(Dimensions {
            files,
            chunks_per_file: chunks,
            copies_per_chunk: copies,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct Dimensions {
    files: u64,
    chunks_per_file: u64,
    copies_per_chunk: u16,
}

impl Dimensions {
    fn counts(self) -> BenchResult<Value> {
        let chunks = self
            .files
            .checked_mul(self.chunks_per_file)
            .ok_or("chunk count overflow")?;
        let copies = chunks
            .checked_mul(u64::from(self.copies_per_chunk))
            .ok_or("copy count overflow")?;
        let versions = if chunks == 0 { 0 } else { self.files };
        let entities = self
            .files
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
            .and_then(|n| n.checked_add(versions.checked_mul(2)?))
            .and_then(|n| n.checked_add(chunks.checked_mul(2)?))
            .and_then(|n| n.checked_add(copies))
            .ok_or("entity count overflow")?;
        Ok(
            json!({"file_inodes":self.files,"root_inodes":1,"dentries":self.files,
            "versions":versions,"layouts":versions,"chunks":chunks,"copies":copies,
            "placements":chunks,"total_entities":entities}),
        )
    }
}

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
enum Phase {
    Seed,
    Measure,
    Recover,
    AuditSeed,
    ServiceSmoke,
    Diagnose,
}

#[derive(Clone, Copy, Debug, Serialize, ValueEnum)]
enum Operation {
    Create,
    Commit,
    Read,
    Watch,
}

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum)]
    phase: Phase,
    #[arg(long, value_enum)]
    dataset: Dataset,
    #[arg(long)]
    store: PathBuf,
    #[arg(long)]
    output: PathBuf,
    /// Full baseline commit; the external source manifest also covers helper edits.
    #[arg(long)]
    source_commit: String,
    #[arg(long)]
    source_manifest_sha256: String,
    /// Smaller qualification only: never labels a scaled run as formal S/M/L.
    #[arg(long)]
    qualification_inodes: Option<u64>,
    #[arg(long)]
    copies: Option<u16>,
    #[arg(long, value_enum, default_value = "read")]
    operation: Operation,
    #[arg(long, default_value_t = 4)]
    concurrency: usize,
    #[arg(long, default_value_t = 30)]
    seconds: u64,
    #[arg(long, default_value_t = 1)]
    round: u16,
    /// Backend cumulative timing is diagnostic, not an additive E2E decomposition.
    #[arg(long)]
    diagnostic: bool,
    /// Admission for the entire synthetic fixture, even when seeded in bounded batches.
    #[arg(long, default_value_t = 3_000_000)]
    max_seed_entities: u64,
}

impl Args {
    fn validate(&self) -> BenchResult<Dimensions> {
        if !valid_hex(&self.source_commit, 40) || !valid_hex(&self.source_manifest_sha256, 64) {
            return Err("full commit and SHA-256 source manifest are required".into());
        }
        if matches!(self.phase, Phase::Diagnose) && !self.diagnostic {
            return Err("diagnose requires --diagnostic; it is never a formal sample".into());
        }
        if !(1..=64).contains(&self.concurrency)
            || !(1..=3).contains(&self.round)
            || self.seconds == 0
            || self.seconds > 300
            || (self.qualification_inodes.is_none() && self.seconds < 30)
        {
            return Err(
                "invalid concurrency/round/duration; unscaled runs require at least 30s".into(),
            );
        }
        if self
            .output
            .extension()
            .is_none_or(|extension| extension != "json")
        {
            return Err(
                "output must have a .json extension; the paired .csv has the same stem".into(),
            );
        }
        if self.output.exists() || self.output.with_extension("csv").exists() {
            return Err("output exists; use a new run identity, do not overwrite evidence".into());
        }
        self.dataset
            .dimensions(self.qualification_inodes, self.copies)
    }
}

fn valid_hex(input: &str, len: usize) -> bool {
    input.len() == len && input.bytes().all(|c| c.is_ascii_hexdigit())
}

#[derive(Default)]
struct BackendCounters {
    calls: AtomicU64,
    bytes: AtomicU64,
    elapsed_ns: AtomicU64,
}

struct ObservedBackend {
    inner: Arc<dyn MetaLog>,
    diagnostic: bool,
    counters: Arc<BackendCounters>,
}

impl MetaLog for ObservedBackend {
    fn persistence(&self) -> BackendPersistence {
        self.inner.persistence()
    }
    fn limits(&self) -> LogLimits {
        self.inner.limits()
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        self.inner.health()
    }
    fn recover(&self) -> MetaFuture<'_, RecoveredLog> {
        self.inner.recover()
    }
    fn maintenance_policy(&self) -> Option<MaintenancePolicy> {
        self.inner.maintenance_policy()
    }
    fn status(&self) -> MetaFuture<'_, LogStatus> {
        self.inner.status()
    }
    fn can_append(&self, expected: LogTip, bytes: usize) -> MetaFuture<'_, bool> {
        self.inner.can_append(expected, bytes)
    }
    fn checkpoint_limits(&self) -> CheckpointLimits {
        self.inner.checkpoint_limits()
    }
    fn install_checkpoint(&self, covered: LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()> {
        self.inner.install_checkpoint(covered, bytes)
    }
    fn append_and_sync(&self, expected: LogTip, records: Vec<Record>) -> MetaFuture<'_, LogTip> {
        if !self.diagnostic {
            return self.inner.append_and_sync(expected, records);
        }
        Box::pin(async move {
            // Logical decided-payload bytes, not framed disk bytes; diagnostic only.
            let size = records.iter().try_fold(0usize, |total, record| {
                let bytes = serde_json::to_vec(record).map_err(|error| {
                    afs_error::Error::coded(
                        afs_error::IO_INVALID,
                        format!("diagnostic encode: {error}"),
                    )
                })?;
                total.checked_add(bytes.len()).ok_or_else(|| {
                    afs_error::Error::coded(
                        afs_error::IO_CAPACITY,
                        "diagnostic byte counter overflow",
                    )
                })
            })?;
            let start = Instant::now();
            let result = self.inner.append_and_sync(expected, records).await;
            self.counters.calls.fetch_add(1, Ordering::Relaxed);
            self.counters
                .bytes
                .fetch_add(size as u64, Ordering::Relaxed);
            self.counters
                .elapsed_ns
                .fetch_add(elapsed_ns(start.elapsed()), Ordering::Relaxed);
            result
        })
    }
}

const MAX_DIAGNOSTIC_SPANS: usize = 128;

#[derive(Clone)]
struct StageCapture {
    origin: Instant,
    next: Arc<AtomicU64>,
    state: Arc<Mutex<StageState>>,
}

#[derive(Default)]
struct StageState {
    spans: BTreeMap<u64, StageSpan>,
    errors: Vec<String>,
}

#[derive(Serialize)]
struct StageSpan {
    name: &'static str,
    fields: BTreeMap<String, u64>,
    created_ns: u64,
    first_enter_ns: Option<u64>,
    last_exit_ns: Option<u64>,
    entered_ns: Option<u64>,
    busy_ns: u64,
    closed_ns: Option<u64>,
    refs: u64,
}

struct StageFields<'a>(&'a mut BTreeMap<String, u64>);

impl tracing::field::Visit for StageFields<'_> {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.0.insert(field.name().to_owned(), value);
    }
    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

impl StageCapture {
    fn new() -> Self {
        Self {
            origin: Instant::now(),
            next: Arc::new(AtomicU64::new(1)),
            state: Arc::new(Mutex::new(StageState::default())),
        }
    }

    fn now(&self) -> u64 {
        elapsed_ns(self.origin.elapsed())
    }

    fn report(&self) -> BenchResult<Value> {
        let state = self.state.lock().map_err(|_| "diagnostic lock poisoned")?;
        let complete = state.errors.is_empty()
            && !state.spans.is_empty()
            && state
                .spans
                .values()
                .all(|s| s.closed_ns.is_some() && s.entered_ns.is_none());
        Ok(
            json!({"complete":complete,"clock":"single process monotonic nanoseconds",
            "scope":"diagnostic only; no component cost is subtracted from OFF results",
            "boundary":"components use first_enter..last_exit; commit_batch uses created..closed; busy excludes async waits",
            "errors":state.errors,"spans":state.spans.values().collect::<Vec<_>>()}),
        )
    }
}

impl tracing::Subscriber for StageCapture {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        matches!(
            metadata.name(),
            "meta.store.commit_batch"
                | "meta.store.batch_gather"
                | "meta.store.batch_fork"
                | "meta.store.snapshot_clone_sort"
                | "meta.store.snapshot_json"
                | "meta.store.backend_commit"
        )
    }

    fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut state) = self.state.lock() {
            if state.spans.len() < MAX_DIAGNOSTIC_SPANS {
                let mut fields = BTreeMap::new();
                attrs.record(&mut StageFields(&mut fields));
                state.spans.insert(
                    id,
                    StageSpan {
                        name: attrs.metadata().name(),
                        fields,
                        created_ns: self.now(),
                        first_enter_ns: None,
                        last_exit_ns: None,
                        entered_ns: None,
                        busy_ns: 0,
                        closed_ns: None,
                        refs: 1,
                    },
                );
            } else if state.errors.is_empty() {
                state
                    .errors
                    .push("diagnostic span bound exceeded; do not use attribution".into());
            }
        }
        tracing::span::Id::from_u64(id)
    }

    fn record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>) {
        if let Ok(mut state) = self.state.lock()
            && let Some(span) = state.spans.get_mut(&id.into_u64())
        {
            values.record(&mut StageFields(&mut span.fields));
        }
    }
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, id: &tracing::span::Id) {
        if let Ok(mut state) = self.state.lock()
            && let Some(span) = state.spans.get_mut(&id.into_u64())
        {
            let now = self.now();
            span.first_enter_ns.get_or_insert(now);
            if span.entered_ns.replace(now).is_some() {
                state
                    .errors
                    .push("overlapping entry into one stage span".into());
            }
        }
    }
    fn exit(&self, id: &tracing::span::Id) {
        if let Ok(mut state) = self.state.lock()
            && let Some(span) = state.spans.get_mut(&id.into_u64())
        {
            let now = self.now();
            if let Some(start) = span.entered_ns.take() {
                span.busy_ns = span.busy_ns.saturating_add(now.saturating_sub(start));
                span.last_exit_ns = Some(now);
            } else {
                state
                    .errors
                    .push("exit without matching stage entry".into());
            }
        }
    }
    fn clone_span(&self, id: &tracing::span::Id) -> tracing::span::Id {
        if let Ok(mut state) = self.state.lock()
            && let Some(span) = state.spans.get_mut(&id.into_u64())
        {
            span.refs += 1;
        }
        id.clone()
    }
    fn try_close(&self, id: tracing::span::Id) -> bool {
        if let Ok(mut state) = self.state.lock()
            && let Some(span) = state.spans.get_mut(&id.into_u64())
        {
            span.refs = span.refs.saturating_sub(1);
            if span.refs == 0 {
                span.closed_ns = Some(self.now());
                return true;
            }
            return false;
        }
        true
    }
}

fn fixture_encoding_sizes(dimensions: Dimensions, limit: u64) -> BenchResult<Value> {
    let txn = seed_transaction(dimensions, limit)?;
    let mut counts = BTreeMap::<&str, u64>::new();
    let mut key_bytes = 0_u64;
    let mut value_bytes = 0_u64;
    let mut outcome_bytes = 0_u64;
    #[derive(Serialize)]
    struct SeedValue<'a> {
        revision: StoreRevision,
        entity: &'a MetaEntity,
    }
    for mutation in &txn.mutations {
        match mutation {
            TxnMutation::Put(entity) => {
                let key = match entity {
                    MetaEntity::DfsInode(x) => MetaKey::DfsInode(x.inode_id.clone()),
                    MetaEntity::DfsDentry(x) => MetaKey::DfsDentry(x.key.clone()),
                    MetaEntity::DfsFileVersion(x) => MetaKey::DfsFileVersion(x.id.clone()),
                    MetaEntity::DfsLayoutRoot(x) => MetaKey::DfsLayoutRoot(x.id.clone()),
                    MetaEntity::DfsChunk(x) => MetaKey::DfsChunk(x.id.clone()),
                    MetaEntity::DfsPlacement(x) => MetaKey::DfsPlacement(x.chunk_id.clone()),
                    MetaEntity::DfsCopy(x) => MetaKey::DfsCopy(x.id.clone()),
                    _ => return Err("unexpected entity in deterministic fixture".into()),
                };
                key_bytes += serde_json::to_vec(&key)?.len() as u64;
                value_bytes += serde_json::to_vec(&SeedValue {
                    revision: StoreRevision(1),
                    entity,
                })?
                .len() as u64;
                *counts.entry(entity.name()).or_default() += 1;
            }
            TxnMutation::RecordRequestOutcome(outcome) => {
                outcome_bytes += serde_json::to_vec(outcome)?.len() as u64;
            }
            _ => return Err("unexpected mutation in deterministic fixture".into()),
        }
    }
    Ok(
        json!({"seed":"deterministic ascending fixed-width hex ids, no random generator",
        "file_count":dimensions.files,"counts":counts,"key_json_bytes":key_bytes,
        "versioned_value_json_bytes":value_bytes,"request_outcome_json_bytes":outcome_bytes,
        "scope":"generated seed entities only; current JSON encoding, excludes pair separators, history events and snapshot envelope; not P3 codec bytes"}),
    )
}

async fn diagnose(
    store: Arc<Store>,
    dimensions: Dimensions,
    capture: &StageCapture,
) -> BenchResult<Value> {
    let mut calls = Vec::new();
    for wave in 0..2 {
        let barrier = Arc::new(tokio::sync::Barrier::new(5));
        let mut workers = tokio::task::JoinSet::new();
        for worker in 0..4 {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let capture = capture.clone();
            let id = format!("diagnose-wave{wave}-worker{worker}");
            let prepared = prepare_request(
                Operation::Commit,
                dimensions,
                &id,
                wave * 4 + worker,
                StoreRevision::ZERO,
            );
            workers.spawn(async move {
                barrier.wait().await;
                let start = capture.now();
                let result = tokio::time::timeout(
                    Duration::from_secs(120),
                    request(store.as_ref(), prepared),
                )
                .await;
                let end = capture.now();
                let error = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(e)) => Some(e.to_string()),
                    Err(_) => Some("diagnostic request timeout; result may be unknown".into()),
                };
                json!({"id":id,"wave":wave,"start_ns":start,"end_ns":end,"error":error})
            });
        }
        barrier.wait().await;
        while let Some(result) = workers.join_next().await {
            calls.push(result?);
        }
        if calls.iter().any(|c| !c["error"].is_null()) {
            break;
        }
    }
    // A reply may resume the caller before the commit task drops its final span.
    for _ in 0..100 {
        if capture.report()?["complete"] == true {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let stages = capture.report()?;
    let completed = calls.len() == 8
        && calls.iter().all(|c| c["error"].is_null())
        && stages["complete"] == true;
    Ok(
        json!({"completed_without_errors":completed,"fixed_waves":2,"concurrency":4,
        "calls":calls,"stages":stages,"scope":"one bounded ON diagnostic; not a formal performance round"}),
    )
}

fn elapsed_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn attributes() -> InodeAttributes {
    InodeAttributes {
        mode: 0o777,
        uid: 0,
        gid: 0,
        nlink: 1,
        atime_unix_ms: 1,
        mtime_unix_ms: 1,
        ctime_unix_ms: 1,
    }
}

fn inode(index: u64, dimensions: Dimensions) -> InodeRecord {
    InodeRecord {
        namespace_id: NamespaceId::new(NAMESPACE),
        inode_id: InodeId::new(format!("bench-inode-{index:016x}")),
        kind: InodeKind::Regular,
        attributes: attributes(),
        head_version: (dimensions.chunks_per_file != 0)
            .then(|| FileVersionId::new(format!("bench-version-{index:016x}"))),
        symlink_target: None,
        xattrs: BTreeMap::new(),
        revision: 1,
    }
}

fn dentry(index: u64) -> DentryKey {
    DentryKey {
        namespace_id: NamespaceId::new(NAMESPACE),
        parent_inode_id: InodeId::new(ROOT),
        name: format!("file-{index:016x}").into_bytes(),
    }
}

fn transaction(id: String, operation: StoreOperation) -> MetaTxn {
    let request = RequestKey::new("meta-bench", id);
    let mut txn = MetaTxn::new(request.clone(), operation);
    txn.conditions
        .push(TxnCondition::RequestAbsent(request.clone()));
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request,
            operation,
            result: OperationResult::Empty,
        }));
    txn
}

fn seed_transaction(dimensions: Dimensions, max_entities: u64) -> BenchResult<MetaTxn> {
    seed_transaction_range(dimensions, max_entities, 0, dimensions.files)
}

fn seed_transaction_range(
    dimensions: Dimensions,
    max_entities: u64,
    start: u64,
    end: u64,
) -> BenchResult<MetaTxn> {
    let counts = dimensions.counts()?;
    if counts["total_entities"]
        .as_u64()
        .ok_or("missing entity count")?
        > max_entities
    {
        return Err(
            "seed exceeds explicit entity admission; do not allocate or silently scale down".into(),
        );
    }
    if start >= end || end > dimensions.files {
        return Err("invalid seed file range".into());
    }
    let mut txn = transaction(
        format!("seed-{start:016x}"),
        StoreOperation::DfsInitializeNamespace,
    );
    if start == 0 {
        txn.conditions
            .push(TxnCondition::Missing(MetaKey::DfsInode(InodeId::new(ROOT))));
        let mut root = inode(0, dimensions);
        root.inode_id = InodeId::new(ROOT);
        root.kind = InodeKind::Directory;
        root.attributes.nlink = 2;
        root.head_version = None;
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::DfsInode(root)));
    }
    // One synthetic 4MiB body's real standard root; data itself is not stored.
    let digest = ContentDigest {
        algorithm: DigestAlgorithm::Blake3,
        bytes: *blake3::hash(&vec![0x5a; CHUNK_BYTES as usize]).as_bytes(),
    };
    for index in start..end {
        let file = inode(index, dimensions);
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::DfsDentry(Dentry {
                key: dentry(index),
                inode_id: file.inode_id.clone(),
            })));
        if let Some(version) = &file.head_version {
            let layout_id = LayoutRootId::new(format!("bench-layout-{index:016x}"));
            let mut extents = Vec::new();
            for part in 0..dimensions.chunks_per_file {
                let chunk_id = ChunkId::new(format!("bench-chunk-{index:016x}-{part}"));
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsChunk(ChunkObject {
                        id: chunk_id.clone(),
                        length: CHUNK_BYTES,
                        content_digest: digest.clone(),
                        encoding: ChunkEncoding::Raw,
                    })));
                let mut copies = Vec::new();
                for copy in 0..dimensions.copies_per_chunk {
                    let copy_id = CopyId::new(format!("bench-copy-{index:016x}-{part}-{copy}"));
                    txn.mutations
                        .push(TxnMutation::Put(MetaEntity::DfsCopy(CopyRecord {
                            id: copy_id.clone(),
                            chunk_id: chunk_id.clone(),
                            role: CopyRole::DurableReplica,
                            location: CopyLocation::Node {
                                node_id: format!("bench-node-{copy}"),
                                node_epoch: 1,
                                device_id: "bench-device".into(),
                                device_epoch: 1,
                                catalog_revision: 1,
                            },
                            state: CopyState::Ready,
                            persisted_bytes: CHUNK_BYTES,
                            verified_digest: digest.clone(),
                        })));
                    copies.push(copy_id);
                }
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsPlacement(
                        PlacementRecord {
                            chunk_id: chunk_id.clone(),
                            replica_group_id: ReplicaGroupId::new("bench-group"),
                            placement_epoch: 1,
                            desired_copies: dimensions.copies_per_chunk,
                            copies,
                            health: PlacementHealth::Satisfied,
                        },
                    )));
                extents.push(Extent {
                    file_offset: part * CHUNK_BYTES,
                    length: CHUNK_BYTES,
                    chunk_id,
                    chunk_offset: 0,
                });
            }
            let length = dimensions.chunks_per_file * CHUNK_BYTES;
            txn.mutations
                .push(TxnMutation::Put(MetaEntity::DfsLayoutRoot(LayoutRoot {
                    id: layout_id.clone(),
                    file_length: length,
                    inline_extents: extents,
                })));
            txn.mutations
                .push(TxnMutation::Put(MetaEntity::DfsFileVersion(FileVersion {
                    id: version.clone(),
                    inode_id: file.inode_id.clone(),
                    parent_version: None,
                    length,
                    layout_root: layout_id,
                    created_at_unix_ms: 1,
                })));
        }
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::DfsInode(file)));
    }
    txn.validate()?;
    Ok(txn)
}

async fn committed(store: &dyn MetaStore, txn: MetaTxn) -> BenchResult<StoreRevision> {
    match store.compare_and_commit(txn).await? {
        TxnOutcome::Committed { revision, .. } => Ok(revision),
        TxnOutcome::ConditionFailed { .. } => {
            Err("unexpected condition failure; evidence is not a successful sample".into())
        }
    }
}

// Explicit seed audit, separate from the historical last-inode recovery probe.
fn seed_entity_read(entity: &MetaEntity) -> BenchResult<MetaRead> {
    Ok(match entity {
        MetaEntity::DfsInode(value) => MetaRead::DfsInode(value.inode_id.clone()),
        MetaEntity::DfsDentry(value) => MetaRead::DfsDentry(value.key.clone()),
        MetaEntity::DfsFileVersion(value) => MetaRead::DfsFileVersion(value.id.clone()),
        MetaEntity::DfsLayoutRoot(value) => MetaRead::DfsLayoutRoot(value.id.clone()),
        MetaEntity::DfsChunk(value) => MetaRead::DfsChunk(value.id.clone()),
        MetaEntity::DfsPlacement(value) => MetaRead::DfsPlacement(value.chunk_id.clone()),
        MetaEntity::DfsCopy(value) => MetaRead::DfsCopy(value.id.clone()),
        _ => return Err("unexpected entity in deterministic seed audit".into()),
    })
}

async fn audit_seed(
    store: &dyn MetaStore,
    dimensions: Dimensions,
    max_entities: u64,
    seed_revision: StoreRevision,
) -> BenchResult<Value> {
    audit_seed_at_revision(
        store,
        dimensions,
        max_entities,
        seed_revision,
        seed_revision,
    )
    .await
}

async fn audit_seed_at_revision(
    store: &dyn MetaStore,
    dimensions: Dimensions,
    max_entities: u64,
    seed_revision: StoreRevision,
    expected_current_revision: StoreRevision,
) -> BenchResult<Value> {
    let batches = dimensions.files.div_ceil(SEED_BATCH_FILES);
    if seed_revision != StoreRevision(batches) || expected_current_revision < seed_revision {
        return Err("audit-seed requires the original bounded-batch seed revision".into());
    }
    // Every legitimate seed request starts with seed-. This absent read gives
    // the Store revision without modifying or retrying any request. Expired
    // outcomes must never accidentally turn an audit into a fresh mutation.
    let head_query = MetaRead::RequestOutcome(RequestKey::new(
        "meta-bench",
        "__audit_read_revision_only__",
    ));
    let before = store.read(head_query.clone()).await?;
    if before.request_outcome.is_some() || before.revision != expected_current_revision {
        return Err("audit-seed requires a pristine seed business revision".into());
    }
    let mut checked_entities = 0_u64;
    let mut checked_outcomes = 0_u64;
    for first in (0..dimensions.files).step_by(SEED_BATCH_FILES as usize) {
        let end = first.saturating_add(SEED_BATCH_FILES).min(dimensions.files);
        let txn = seed_transaction_range(dimensions, max_entities, first, end)?;
        let expected_revision = StoreRevision(first / SEED_BATCH_FILES + 1);
        let expected_outcome = txn
            .mutations
            .iter()
            .find_map(|mutation| match mutation {
                TxnMutation::RecordRequestOutcome(outcome) => Some(outcome),
                _ => None,
            })
            .ok_or("seed transaction lacks its original outcome")?;
        let actual = store
            .read(MetaRead::RequestOutcome(txn.request.clone()))
            .await?;
        let outcome = actual.request_outcome.as_ref().ok_or("seed acknowledgement unavailable; retained history may have expired; this alone is not evidence of data loss")?;
        if outcome != expected_outcome || actual.revision != expected_current_revision {
            return Err(format!(
                "seed original outcome or business revision mismatch: {:?}",
                txn.request
            )
            .into());
        }
        checked_outcomes += 1;
        for mutation in txn.mutations {
            if let TxnMutation::Put(expected) = mutation {
                let query = seed_entity_read(&expected)?;
                let actual = store.read(query.clone()).await?;
                if actual.entity.as_ref() != Some(&expected) {
                    return Err(format!("seed entity mismatch: {query:?}").into());
                }
                if actual.revision != expected_revision {
                    return Err(format!(
                        "seed revision mismatch: {query:?}; expected {}, got {}",
                        expected_revision.0, actual.revision.0
                    )
                    .into());
                }
                checked_entities += 1;
            }
        }
    }
    let after = store.read(head_query).await?;
    if after.request_outcome.is_some() || after.revision != expected_current_revision {
        return Err("seed business revision changed during read-only audit".into());
    }
    if checked_entities
        != dimensions.counts()?["total_entities"]
            .as_u64()
            .ok_or("missing seed entity count")?
        || checked_outcomes != batches
    {
        return Err("seed audit coverage differs from the declared fixture".into());
    }
    Ok(json!({
        "checked_expected_entities":checked_entities,"checked_seed_ack_outcomes":checked_outcomes,
        "all_expected_entity_fields_match":true,"per_entity_mod_revisions_match":true,
        "business_revision_before":before.revision.0,"business_revision_after":after.revision.0,
        "no_business_mutations_or_request_replays":true,"max_expected_batch_files":SEED_BATCH_FILES,
        "scope":"expected seed prefix at declared head only; arbitrary extra entities, all history/node epochs and suffix transactions require separate audits"
    }))
}

// Test-only continuation audit: the pristine audit remains a separate contract.
#[cfg(test)]
async fn audit_seed_at_declared_head(
    store: &dyn MetaStore,
    dimensions: Dimensions,
    max_entities: u64,
    seed_revision: StoreRevision,
    declared_head: StoreRevision,
) -> BenchResult<Value> {
    audit_seed_at_revision(
        store,
        dimensions,
        max_entities,
        seed_revision,
        declared_head,
    )
    .await
}

// Fixture setup only. A known busy rejection happens before checkpoint capture;
// it is safe to wait for the existing owner, without retrying actual I/O errors.
async fn checkpoint_seed(store: &Store) -> BenchResult<LogTip> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match store.checkpoint().await {
            Ok(tip) => return Ok(tip),
            Err(error)
                if error.to_string().contains("checkpoint already in progress")
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

enum PreparedRequest {
    Read(MetaRead),
    Watch(StoreRevision),
    Transaction(MetaTxn),
}

fn prepare_request(
    operation: Operation,
    dimensions: Dimensions,
    id: &str,
    serial: u64,
    watch_after: StoreRevision,
) -> PreparedRequest {
    let index = serial % dimensions.files;
    match operation {
        Operation::Read => {
            let read = if serial.is_multiple_of(2) {
                MetaRead::DfsInode(inode(index, dimensions).inode_id)
            } else {
                MetaRead::DfsDentry(dentry(index))
            };
            PreparedRequest::Read(read)
        }
        Operation::Watch => PreparedRequest::Watch(watch_after),
        Operation::Create | Operation::Commit => {
            let mut txn = transaction(
                id.to_owned(),
                if matches!(operation, Operation::Create) {
                    StoreOperation::DfsCreate
                } else {
                    StoreOperation::DfsSyncInodeMetadata
                },
            );
            let mut file = inode(index, dimensions);
            if matches!(operation, Operation::Create) {
                file.inode_id = InodeId::new(id);
                file.head_version = None;
                let key = DentryKey {
                    namespace_id: NamespaceId::new(NAMESPACE),
                    parent_inode_id: InodeId::new(ROOT),
                    name: id.as_bytes().to_vec(),
                };
                txn.conditions
                    .push(TxnCondition::Missing(MetaKey::DfsDentry(key.clone())));
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsDentry(Dentry {
                        key,
                        inode_id: file.inode_id.clone(),
                    })));
            } else {
                file.attributes.mtime_unix_ms = serial.saturating_add(2);
            }
            txn.mutations
                .push(TxnMutation::Put(MetaEntity::DfsInode(file)));
            PreparedRequest::Transaction(txn)
        }
    }
}

async fn request(store: &dyn MetaStore, prepared: PreparedRequest) -> BenchResult<()> {
    match prepared {
        PreparedRequest::Read(read) => {
            if store.read(read).await?.entity.is_none() {
                return Err("fixture entity missing".into());
            }
        }
        PreparedRequest::Watch(after) => {
            let batch = store.watch(after, 128).await?;
            batch.validate_order()?;
            if !matches!(&batch, WatchBatch::Events { events, .. } if events.is_empty()) {
                return Err("watch fixture differs from empty-tail polling contract".into());
            }
        }
        PreparedRequest::Transaction(txn) => {
            committed(store, txn).await?;
        }
    }
    Ok(())
}

fn record_latency(samples: &mut BTreeMap<u64, u64>, latency: u64) -> BenchResult<()> {
    let count = samples.entry(latency).or_default();
    *count = count.checked_add(1).ok_or("latency count overflow")?;
    Ok(())
}

fn quantiles(samples: BTreeMap<u64, u64>) -> BenchResult<Value> {
    let count = samples
        .values()
        .try_fold(0_u64, |total, n| total.checked_add(*n))
        .ok_or("latency count overflow")?;
    if count == 0 {
        return Err("no samples; missing measurement is not zero".into());
    }
    let rank = |percent: u64| -> BenchResult<u64> {
        let target = (u128::from(count) * u128::from(percent)).div_ceil(100);
        let mut seen = 0_u128;
        for (latency, occurrences) in &samples {
            seen += u128::from(*occurrences);
            if seen >= target {
                return Ok(*latency);
            }
        }
        Err("invalid latency histogram".into())
    };
    Ok(
        json!({"samples":count,"p50_ns":rank(50)?,"p95_ns":rank(95)?,
        "p99_ns":rank(99)?,"min_ns":samples.first_key_value().map(|x| *x.0),
        "max_ns":samples.last_key_value().map(|x| *x.0),
        "representation":"exact nanosecond frequency table; all attempts, no sampling"}),
    )
}

async fn measure(
    args: &Args,
    store: Arc<Store>,
    dimensions: Dimensions,
    watch_after: StoreRevision,
) -> BenchResult<Value> {
    // Fixed warmup outside timing, with a separate request identity namespace.
    for serial in 0..4 {
        let prepared = prepare_request(
            args.operation,
            dimensions,
            &format!("r{}-warmup-{serial}", args.round),
            serial,
            watch_after,
        );
        request(store.as_ref(), prepared).await?;
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(args.concurrency + 1));
    let stopped = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.seconds);
    let per_worker = MAX_LATENCY_BINS / args.concurrency;
    let mut workers = tokio::task::JoinSet::new();
    for worker in 0..args.concurrency {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let stopped = Arc::clone(&stopped);
        let operation = args.operation;
        let round = args.round;
        workers.spawn(async move {
            let mut samples = BTreeMap::new();
            let mut error = None;
            let mut serial = 0_u64;
            barrier.wait().await;
            while Instant::now() < deadline && !stopped.load(Ordering::Relaxed) {
                if samples.len() > per_worker {
                    stopped.store(true, Ordering::Relaxed);
                    error = Some("latency distinct-bin budget exhausted".to_owned());
                    break;
                }
                let id = format!("r{round}-worker{worker}-{serial}");
                let prepared = prepare_request(operation, dimensions, &id, serial, watch_after);
                let begin = Instant::now();
                let result = tokio::time::timeout(
                    Duration::from_secs(120),
                    request(store.as_ref(), prepared),
                )
                .await;
                record_latency(&mut samples, elapsed_ns(begin.elapsed()))?;
                serial += 1;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        error = Some(e.to_string());
                    }
                    Err(_) => {
                        error = Some("request timed out; commit result may be unknown".into());
                    }
                }
                if error.is_some() {
                    stopped.store(true, Ordering::Relaxed);
                    break;
                }
            }
            Ok::<_, Box<dyn Error + Send + Sync>>((samples, error))
        });
    }
    barrier.wait().await;
    let mut samples = BTreeMap::new();
    let mut errors = Vec::new();
    while let Some(result) = workers.join_next().await {
        let (worker_samples, error) = result??;
        for (latency, count) in worker_samples {
            let total: &mut u64 = samples.entry(latency).or_default();
            *total = total.checked_add(count).ok_or("latency count overflow")?;
        }
        if let Some(error) = error {
            errors.push(error);
        }
    }
    let elapsed = start.elapsed();
    let attempts = samples
        .values()
        .try_fold(0_u64, |total, n| total.checked_add(*n))
        .ok_or("latency count overflow")?;
    Ok(
        json!({"operation":args.operation,"request_scope":"public Store API; no FUSE/RPC/data I/O",
        "latency_boundary":"before public API invocation through complete return; all attempts",
        "elapsed_ns":elapsed_ns(elapsed),"attempts":attempts,"errors":errors,
        "completed_without_errors":errors.is_empty(),"attempts_per_second":attempts as f64/elapsed.as_secs_f64(),
        "latency":quantiles(samples)?,"warmup_operations":4,
        "distinct_latency_bin_budget":MAX_LATENCY_BINS,"concurrency":args.concurrency,
        "watch_after_revision":matches!(args.operation, Operation::Watch).then_some(watch_after.0)}),
    )
}

async fn service_smoke(store: Arc<Store>) -> BenchResult<Value> {
    let request_key = RequestKey::new("meta-bench", "service-session");
    store
        .register_node_session(
            request_key,
            NodeSessionLease {
                node_id: "bench-client".into(),
                session_id: "bench-session".into(),
                grpc_addr: "127.0.0.1:1".into(),
                data_addr: "127.0.0.1:2".into(),
                rest_addr: "127.0.0.1:3".into(),
                storage_devices: vec![],
                lease_ttl: Duration::from_secs(60),
            },
        )
        .await?;
    let service = DfsService::new(store.clone());
    let request = CreateFileRequest {
        caller_id: "bench-client".into(),
        owner_session_id: "bench-session".into(),
        operation_id: OperationId::new("bench-service-create"),
        namespace_id: NamespaceId::new(NAMESPACE),
        parent_inode_id: InodeId::new(ROOT),
        name: b"service-smoke".to_vec(),
        attributes: attributes(),
        lease_seconds: 30,
    };
    let start = Instant::now();
    let (file, lease) = service.create(request.clone()).await?;
    let elapsed = elapsed_ns(start.elapsed());
    let retry = service.create(request).await?;
    if retry != (file.clone(), lease) {
        return Err("domain create retry changed its outcome".into());
    }
    let found = service
        .lookup(
            NamespaceId::new(NAMESPACE),
            InodeId::new(ROOT),
            b"service-smoke".to_vec(),
        )
        .await?;
    if found != Some(file) {
        return Err("domain lookup disagrees with create".into());
    }
    Ok(
        json!({"scope":"DfsService create/retry/lookup, in-process, not RPC or performance acceptance",
        "create_ns":elapsed,"functional_pass":true}),
    )
}

fn rss_bytes() -> Option<u64> {
    fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            let rest = line.strip_prefix("VmRSS:")?;
            rest.split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()?
                .checked_mul(1024)
        })
}

fn storage_bytes(path: &Path) -> BenchResult<u64> {
    let mut bytes = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or("storage size overflow")?;
        }
    }
    Ok(bytes)
}

fn fixture_path(path: &Path) -> PathBuf {
    path.with_extension("fixture.json")
}

fn write_new(path: &Path, value: &Value) -> BenchResult<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn csv_field(value: Option<&Value>) -> String {
    value
        .filter(|v| !v.is_null())
        .map(ToString::to_string)
        .unwrap_or_default()
}

fn write_csv(path: &Path, output: &Value) -> BenchResult<()> {
    let result = &output["result"];
    let fields = [
        output.get("round"),
        result.get("attempts"),
        result.get("attempts_per_second"),
        result["latency"].get("p50_ns"),
        result["latency"].get("p95_ns"),
        result["latency"].get("p99_ns"),
        output.get("rss_bytes"),
        result.get("recovery_ns"),
        output.get("store_disk_bytes"),
    ];
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    writeln!(
        file,
        "round,attempts,attempts_per_second,p50_ns,p95_ns,p99_ns,rss_bytes,recovery_ns,store_disk_bytes"
    )?;
    writeln!(
        file,
        "{}",
        fields
            .into_iter()
            .map(csv_field)
            .collect::<Vec<_>>()
            .join(",")
    )?;
    file.sync_all()?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> BenchResult<()> {
    if !cfg!(target_os = "linux") {
        return Err("Meta measurements require Linux".into());
    }
    let args = Args::parse();
    let dimensions = args.validate()?;
    let seed_revision = if matches!(args.phase, Phase::Seed) {
        if args.store.exists() || fixture_path(&args.store).exists() {
            return Err("seed requires a new store path; existing data is never reset".into());
        }
        StoreRevision::ZERO
    } else {
        let manifest: Value = serde_json::from_slice(&fs::read(fixture_path(&args.store))?)?;
        if manifest["dimensions"] != serde_json::to_value(dimensions)?
            || manifest["dataset"] != serde_json::to_value(args.dataset)?
            || manifest["qualification_only"] != json!(args.qualification_inodes.is_some())
        {
            return Err("fixture dimensions differ; do not mix dataset identities".into());
        }
        if matches!(args.phase, Phase::AuditSeed) {
            let batches = dimensions.files.div_ceil(SEED_BATCH_FILES);
            if manifest["seed_batch_files"] != json!(SEED_BATCH_FILES)
                || manifest["seed_batches"] != json!(batches)
                || manifest["seed_revision"] != json!(batches)
            {
                return Err("audit-seed requires a sealed bounded-batch fixture manifest".into());
            }
        }
        StoreRevision(
            manifest["seed_revision"]
                .as_u64()
                .filter(|value| *value > 0)
                .ok_or("fixture seed revision missing")?,
        )
    };
    let capture = matches!(args.phase, Phase::Diagnose).then(StageCapture::new);
    if let Some(capture) = capture.as_ref() {
        tracing::subscriber::set_global_default(capture.clone())?;
    }
    let fixture_sizes = if capture.is_some() {
        Some(fixture_encoding_sizes(dimensions, args.max_seed_entities)?)
    } else {
        None
    };
    let total_recovery_start = Instant::now();
    let counters = Arc::new(BackendCounters::default());
    let backend = Arc::new(ObservedBackend {
        inner: Arc::new(LocalLog::open(&args.store, Default::default())?),
        diagnostic: args.diagnostic,
        counters: Arc::clone(&counters),
    });
    let backend_open_ns = elapsed_ns(total_recovery_start.elapsed());
    let start = Instant::now();
    let store = Arc::new(Store::open_log(backend).await?);
    let recovery_ns = elapsed_ns(start.elapsed());
    let total_store_init_ns = elapsed_ns(total_recovery_start.elapsed());
    let result = match args.phase {
        Phase::Seed => {
            let seed_start = Instant::now();
            let mut preparation_ns = 0_u64;
            let mut seed_commit_ns = 0_u64;
            let mut mutations = 0_usize;
            let mut revision = StoreRevision::ZERO;
            let mut batches = 0_u64;
            for first in (0..dimensions.files).step_by(SEED_BATCH_FILES as usize) {
                let end = first.saturating_add(SEED_BATCH_FILES).min(dimensions.files);
                let start = Instant::now();
                let txn = seed_transaction_range(dimensions, args.max_seed_entities, first, end)?;
                mutations += txn.mutations.len();
                preparation_ns = preparation_ns.saturating_add(elapsed_ns(start.elapsed()));
                let start = Instant::now();
                revision = committed(store.as_ref(), txn).await?;
                seed_commit_ns = seed_commit_ns.saturating_add(elapsed_ns(start.elapsed()));
                batches += 1;
            }
            let start = Instant::now();
            let checkpoint = checkpoint_seed(store.as_ref()).await?;
            let checkpoint_ns = elapsed_ns(start.elapsed());
            let result = json!({"dataset":args.dataset,"qualification_only":args.qualification_inodes.is_some(),
                "dimensions":dimensions,"counts":dimensions.counts()?,"seed_revision":revision.0,
                "mutations":mutations,"preparation_ns":preparation_ns,"seed_commit_ns":seed_commit_ns,
                "seed_wall_ns":elapsed_ns(seed_start.elapsed()),"seed_batches":batches,
                "seed_batch_files":SEED_BATCH_FILES,
                "seed_checkpoint_sequence":checkpoint.sequence,"seed_checkpoint_ns":checkpoint_ns,
                "scope":"synthetic metadata; no bodies; same business entities, bounded file batches and final checkpoint; batch-specific entity revisions/outcomes/history differ from legacy single-transaction seed"});
            write_new(&fixture_path(&args.store), &result)?;
            result
        }
        Phase::Measure => measure(&args, store, dimensions, seed_revision).await?,
        Phase::Recover => {
            let head = store
                .read(MetaRead::DfsInode(
                    inode(dimensions.files - 1, dimensions).inode_id,
                ))
                .await?;
            if head.entity.is_none() {
                return Err("recovery missing last seeded inode".into());
            }
            json!({"recovery_ns":recovery_ns,"last_seeded_inode_present":true,
                "scope":"normal Store reopen; not crash/power-loss recovery acceptance"})
        }
        Phase::AuditSeed => {
            let start = Instant::now();
            let audit = audit_seed(
                store.as_ref(),
                dimensions,
                args.max_seed_entities,
                seed_revision,
            )
            .await?;
            json!({"recovery_ns":recovery_ns,"audit_ns":elapsed_ns(start.elapsed()),"audit":audit,
                "scope":"all expected seed entities, per-entity revisions and original seed outcomes after normal Store reopen; not crash recovery, post-load ACK coverage or formal performance acceptance"})
        }
        Phase::ServiceSmoke => service_smoke(store).await?,
        Phase::Diagnose => {
            diagnose(
                store,
                dimensions,
                capture.as_ref().ok_or("diagnostic capture missing")?,
            )
            .await?
        }
    };
    let binary = Command::new("sha256sum")
        .arg(std::env::current_exe()?)
        .output()?;
    if !binary.status.success() {
        return Err("binary SHA-256 unavailable".into());
    }
    let binary_sha = String::from_utf8(binary.stdout)?
        .split_whitespace()
        .next()
        .ok_or("missing binary SHA")?
        .to_owned();
    let measurement_failed = result.get("completed_without_errors") == Some(&Value::Bool(false));
    let output = json!({"schema_version":2,"phase":args.phase,"dataset":args.dataset,"round":args.round,
        "qualification_only":args.qualification_inodes.is_some(),"diagnostic":args.diagnostic,
        "formal_target_pass":null,"source_commit":args.source_commit,"source_manifest_sha256":args.source_manifest_sha256,
        "binary_sha256":binary_sha,"architecture":std::env::consts::ARCH,"dimensions":dimensions,
        "rss_bytes":rss_bytes(),"store_disk_bytes":storage_bytes(&args.store)?,"result":result,
        "backend_open_ns":backend_open_ns,"store_open_only_ns":recovery_ns,"total_store_init_ns":total_store_init_ns,
        "fixture_encoding_sizes":fixture_sizes,
        "backend_diagnostic":if args.diagnostic {json!({"calls":counters.calls.load(Ordering::Relaxed),
            "snapshot_bytes":counters.bytes.load(Ordering::Relaxed),"cumulative_ns":counters.elapsed_ns.load(Ordering::Relaxed)})} else {Value::Null},
        "unmeasured":if capture.is_some() { vec!["independent RPC client","NVMe qualification"] }
            else { vec!["fork time","JSON encode time","independent RPC client","NVMe qualification"] }});
    write_new(&args.output, &output)?;
    write_csv(&args.output.with_extension("csv"), &output)?;
    println!("{}", serde_json::to_string(&output)?);
    if measurement_failed {
        return Err("measurement contains errors; original results saved".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use afs::meta::store::journal::MemoryLog;

    #[test]
    fn dataset_counts_do_not_turn_large_metadata_into_chunk_workloads() {
        let l = Dataset::L.dimensions(None, None).unwrap();
        assert_eq!(l.counts().unwrap()["total_entities"], 20_000_001);
        assert_eq!(l.counts().unwrap()["chunks"], 0);
        assert!(Dataset::L.dimensions(None, Some(1)).is_err());
        let m = Dataset::M.dimensions(Some(2), None).unwrap();
        assert_eq!(m.counts().unwrap()["chunks"], 8);
        assert_eq!(m.counts().unwrap()["copies"], 24);
        assert!(Dataset::M.dimensions(None, Some(1)).is_err());
        assert!(Dataset::S.dimensions(Some(0), None).is_err());
    }

    #[test]
    fn seed_budget_is_checked_before_building_the_large_transaction() {
        assert!(seed_transaction(Dataset::L.dimensions(None, None).unwrap(), 100).is_err());
    }

    #[test]
    fn bounded_seed_ranges_preserve_all_business_entities_without_whole_seed_allocation() {
        for dataset in [Dataset::S, Dataset::M, Dataset::L10] {
            let dimensions = dataset.dimensions(Some(7), None).unwrap();
            let original = seed_transaction(dimensions, 1000).unwrap();
            let puts = |txn: &MetaTxn| {
                txn.mutations
                    .iter()
                    .filter_map(|mutation| match mutation {
                        TxnMutation::Put(entity) => Some(entity.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            };
            let mut actual = Vec::new();
            for (start, end) in [(0, 3), (3, 6), (6, 7)] {
                let batch = seed_transaction_range(dimensions, 1000, start, end).unwrap();
                let entities = puts(&batch);
                let expected = Dimensions {
                    files: end - start,
                    ..dimensions
                }
                .counts()
                .unwrap()["total_entities"]
                    .as_u64()
                    .unwrap()
                    - u64::from(start != 0);
                assert_eq!(
                    entities.len() as u64,
                    expected,
                    "each batch must be bounded"
                );
                actual.extend(entities);
            }
            assert_eq!(
                actual,
                puts(&original),
                "all fields, IDs, roots and references stay identical"
            );
            assert!(seed_transaction_range(dimensions, 1, 0, 1).is_err());
            assert!(seed_transaction_range(dimensions, 1000, 4, 4).is_err());
            assert!(seed_transaction_range(dimensions, 1000, 0, 8).is_err());
        }
    }

    #[test]
    fn exact_nearest_rank_quantiles_keep_slow_samples() {
        let q = quantiles((1..=100).rev().map(|n| (n, 1)).collect()).unwrap();
        assert_eq!(q["p50_ns"], 50);
        assert_eq!(q["p95_ns"], 95);
        assert_eq!(q["p99_ns"], 99);
        assert_eq!(quantiles(BTreeMap::from([(9, 1)])).unwrap()["p99_ns"], 9);
        assert!(quantiles(BTreeMap::new()).is_err());
    }

    #[test]
    fn exact_histogram_retains_repetitions_and_rare_tail() {
        let mut samples = BTreeMap::new();
        for _ in 0..999 {
            record_latency(&mut samples, 10).unwrap();
        }
        record_latency(&mut samples, 100_000).unwrap();
        assert_eq!(samples.len(), 2);
        let q = quantiles(samples).unwrap();
        assert_eq!(q["samples"], 1000);
        assert_eq!(q["p99_ns"], 10);
        assert_eq!(q["max_ns"], 100_000);
        assert!(quantiles(BTreeMap::from([(1, u64::MAX), (2, 1)])).is_err());
    }

    #[test]
    fn stage_capture_uses_enter_exit_not_delayed_span_drop() {
        let capture = StageCapture::new();
        tracing::subscriber::with_default(capture.clone(), || {
            let span =
                tracing::info_span!("meta.store.snapshot_clone_sort", store.batch_id = 7_u64);
            {
                let _guard = span.enter();
            }
            let after_exit = capture.now();
            let held = span.clone();
            drop(span);
            assert_eq!(capture.report().unwrap()["complete"], false);
            drop(held);
            let report = capture.report().unwrap();
            let observation = &report["spans"][0];
            assert_eq!(report["complete"], true);
            assert!(observation["last_exit_ns"].as_u64().unwrap() <= after_exit);
            assert!(observation["closed_ns"].as_u64().unwrap() >= after_exit);
            assert_eq!(observation["fields"]["store.batch_id"], 7);
        });
    }

    #[test]
    fn diagnostic_async_wait_is_separate_from_busy_time() {
        use tracing::Subscriber as _;
        let capture = StageCapture::new();
        let span = tracing::subscriber::with_default(capture.clone(), || {
            tracing::info_span!("meta.store.backend_commit", store.batch_id = 8_u64)
        });
        let id = span.id().unwrap();
        capture.enter(&id);
        capture.exit(&id);
        let first_end = capture.now();
        capture.enter(&id);
        capture.exit(&id);
        drop(span);
        let report = capture.report().unwrap();
        let observation = &report["spans"][0];
        assert_eq!(report["complete"], true);
        assert!(observation["last_exit_ns"].as_u64().unwrap() >= first_end);
        let wall = observation["last_exit_ns"].as_u64().unwrap()
            - observation["first_enter_ns"].as_u64().unwrap();
        assert!(wall >= observation["busy_ns"].as_u64().unwrap());
    }

    #[test]
    fn fixture_byte_counts_are_explicit_about_json_scope() {
        let sizes =
            fixture_encoding_sizes(Dataset::S.dimensions(Some(1), None).unwrap(), 20).unwrap();
        assert_eq!(sizes["counts"]["dfs_inode"], 2);
        assert_eq!(sizes["counts"]["dfs_chunk"], 1);
        assert!(sizes["key_json_bytes"].as_u64().unwrap() > 0);
        assert!(
            sizes["versioned_value_json_bytes"].as_u64().unwrap()
                > sizes["key_json_bytes"].as_u64().unwrap()
        );
        assert!(sizes["request_outcome_json_bytes"].as_u64().unwrap() > 0);
    }

    #[test]
    fn absent_measurements_remain_empty_in_csv_not_zero() {
        assert_eq!(csv_field(None), "");
        assert_eq!(csv_field(Some(&Value::Null)), "");
        assert_eq!(csv_field(Some(&json!(0))), "0");
    }

    #[tokio::test]
    async fn synthetic_fixture_counts_and_references_survive_public_store_reopen() {
        let backend = Arc::new(MemoryLog::new(Default::default()).unwrap());
        let store = Store::open_log(backend.clone()).await.unwrap();
        let dimensions = Dataset::M.dimensions(Some(2), None).unwrap();
        let txn = seed_transaction(dimensions, 100).unwrap();
        let puts = txn
            .mutations
            .iter()
            .filter(|m| matches!(m, TxnMutation::Put(_)))
            .count();
        assert_eq!(puts as u64, dimensions.counts().unwrap()["total_entities"]);
        committed(&store, txn).await.unwrap();
        let reopened = Store::open_log(backend).await.unwrap();
        assert!(
            reopened
                .read(MetaRead::DfsInode(inode(1, dimensions).inode_id))
                .await
                .unwrap()
                .entity
                .is_some()
        );
        assert!(
            reopened
                .read(MetaRead::DfsCopy(CopyId::new(
                    "bench-copy-0000000000000001-3-2"
                )))
                .await
                .unwrap()
                .entity
                .is_some()
        );
    }

    #[tokio::test]
    async fn bounded_seed_local_checkpoint_and_suffix_reopen_preserve_every_field() {
        let directory = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalLog::open(directory.path(), Default::default()).unwrap());
        let identity = backend.identity();
        let store = Store::open_log(backend.clone()).await.unwrap();
        let dimensions = Dataset::M.dimensions(Some(7), None).unwrap();
        let expected = seed_transaction(dimensions, 1000).unwrap();
        for (number, (start, end)) in [(0, 3), (3, 6), (6, 7)].into_iter().enumerate() {
            let revision = committed(
                &store,
                seed_transaction_range(dimensions, 1000, start, end).unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(revision, StoreRevision(number as u64 + 1));
            if number == 0 {
                let (first, second) =
                    tokio::join!(checkpoint_seed(&store), checkpoint_seed(&store));
                assert_eq!(first.unwrap().sequence, 1);
                assert_eq!(second.unwrap().sequence, 1);
            }
        }
        let mut snapshots = Vec::new();
        for mutation in expected.mutations {
            if let TxnMutation::Put(entity) = mutation {
                let query = seed_entity_read(&entity).unwrap();
                let snapshot = store.read(query.clone()).await.unwrap();
                assert_eq!(snapshot.entity, Some(entity));
                snapshots.push((query, snapshot));
            }
        }
        assert_eq!(
            snapshots.len() as u64,
            dimensions.counts().unwrap()["total_entities"]
        );
        request(
            &store,
            prepare_request(Operation::Watch, dimensions, "unused", 0, StoreRevision(3)),
        )
        .await
        .unwrap();
        assert!(
            request(
                &store,
                prepare_request(Operation::Watch, dimensions, "unused", 0, StoreRevision(1)),
            )
            .await
            .is_err(),
            "a stale cursor with actual events is not an empty-tail sample"
        );
        drop(store);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&backend) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(backend);
        let backend = Arc::new(LocalLog::open(directory.path(), Default::default()).unwrap());
        assert_eq!(backend.identity(), identity);
        let recovered = backend.recover().await.unwrap();
        assert_eq!(recovered.checkpoint.unwrap().covered.sequence, 1);
        assert_eq!(recovered.records.len(), 2);
        let store = Store::open_log(backend).await.unwrap();
        for (query, expected) in snapshots {
            assert_eq!(store.read(query).await.unwrap(), expected);
        }
        for start in [0, 3, 6] {
            assert!(
                store
                    .read(MetaRead::RequestOutcome(RequestKey::new(
                        "meta-bench",
                        format!("seed-{start:016x}"),
                    )))
                    .await
                    .unwrap()
                    .request_outcome
                    .is_some()
            );
        }
    }

    #[tokio::test]
    async fn seed_audit_rejects_earlier_corruption_even_when_last_inode_exists() {
        let backend = Arc::new(MemoryLog::new(Default::default()).unwrap());
        let store = Store::open_log(backend).await.unwrap();
        let dimensions = Dataset::S.dimensions(Some(2), None).unwrap();
        let mut txn = seed_transaction(dimensions, 100).unwrap();
        let target = inode(0, dimensions).inode_id;
        for mutation in &mut txn.mutations {
            if let TxnMutation::Put(MetaEntity::DfsInode(value)) = mutation
                && value.inode_id == target
            {
                value.attributes.mode = 0o600;
            }
        }
        committed(&store, txn).await.unwrap();
        assert!(
            store
                .read(MetaRead::DfsInode(inode(1, dimensions).inode_id))
                .await
                .unwrap()
                .entity
                .is_some()
        );
        let result = audit_seed(&store, dimensions, 100, StoreRevision(1)).await;
        assert!(
            result.is_err(),
            "the last-inode probe must not certify earlier corrupted fields"
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("seed entity mismatch")
        );
    }

    // The suffix is a distinct request and entities; seed identities stay intact.
    fn recovery_probe_transaction() -> MetaTxn {
        let mut txn = transaction("recovery-after-seed".into(), StoreOperation::DfsCreate);
        let mut extra = inode(0, Dataset::L10.dimensions(Some(1), None).unwrap());
        extra.inode_id = InodeId::new("recovery-probe-inode");
        txn.conditions.push(TxnCondition::Missing(MetaKey::DfsInode(
            extra.inode_id.clone(),
        )));
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::DfsDentry(Dentry {
                key: DentryKey {
                    namespace_id: NamespaceId::new(NAMESPACE),
                    parent_inode_id: InodeId::new(ROOT),
                    name: b"recovery-probe-extra".to_vec(),
                },
                inode_id: extra.inode_id.clone(),
            })));
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::DfsInode(extra)));
        txn.validate().unwrap();
        txn
    }

    #[tokio::test]
    async fn seed_audit_preserves_seed_at_a_declared_acknowledged_suffix_head() {
        let store = Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap();
        let dimensions = Dataset::S.dimensions(Some(2), None).unwrap();
        committed(&store, seed_transaction(dimensions, 100).unwrap())
            .await
            .unwrap();
        assert_eq!(
            committed(&store, recovery_probe_transaction())
                .await
                .unwrap(),
            StoreRevision(2)
        );
        // A normal pristine audit must still refuse a post-seed state.
        assert!(
            audit_seed(&store, dimensions, 100, StoreRevision(1))
                .await
                .is_err()
        );
        let audit = audit_seed_at_declared_head(
            &store,
            dimensions,
            100,
            StoreRevision(1),
            StoreRevision(2),
        )
        .await
        .expect("declared ACK suffix must not invalidate the unchanged seed prefix");
        assert_eq!(audit["checked_expected_entities"], 15);
        assert_eq!(audit["checked_seed_ack_outcomes"], 1);
        assert_eq!(audit["business_revision_before"], 2);
        assert_eq!(audit["business_revision_after"], 2);
        assert!(
            audit_seed_at_declared_head(
                &store,
                dimensions,
                100,
                StoreRevision(1),
                StoreRevision(3),
            )
            .await
            .is_err(),
            "undeclared writes must not be accepted"
        );
    }

    #[tokio::test]
    async fn seed_audit_retried_ack_reports_saved_outcome_without_a_second_record() {
        let backend = Arc::new(MemoryLog::new(Default::default()).unwrap());
        let store = Store::open_log(backend.clone()).await.unwrap();
        let dimensions = Dataset::S.dimensions(Some(2), None).unwrap();
        committed(&store, seed_transaction(dimensions, 100).unwrap())
            .await
            .unwrap();
        let txn = recovery_probe_transaction();
        let first = store.compare_and_commit(txn.clone()).await.unwrap();
        let TxnOutcome::Committed { revision, outcome } = first else {
            panic!("first probe must receive a commit ACK");
        };
        let tip = backend.status().await.unwrap().tip;
        assert_eq!(revision, StoreRevision(2));
        assert_eq!(
            store.compare_and_commit(txn).await.unwrap(),
            TxnOutcome::ConditionFailed {
                revision,
                existing_outcome: Some(outcome)
            },
        );
        assert_eq!(backend.status().await.unwrap().tip, tip);
        assert_eq!(
            audit_seed_at_declared_head(&store, dimensions, 100, StoreRevision(1), revision)
                .await
                .unwrap()["checked_expected_entities"],
            15,
        );
    }

    fn recovery_probe_files(directory: &Path) -> BTreeMap<String, (u64, String)> {
        fs::read_dir(directory)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                let bytes = fs::read(entry.path()).unwrap();
                (
                    entry.file_name().into_string().unwrap(),
                    (
                        bytes.len() as u64,
                        blake3::hash(&bytes).to_hex().to_string(),
                    ),
                )
            })
            .collect()
    }

    struct RecoveryProbeChild(std::process::Child);
    impl Drop for RecoveryProbeChild {
        fn drop(&mut self) {
            if !matches!(self.0.try_wait(), Ok(Some(_))) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires an explicitly admitted, owned physical S seed copy"]
    async fn seed_audit_s_process_crash_preserves_ack_suffix_and_retry() {
        use std::os::unix::process::ExitStatusExt;
        use std::process::Stdio;

        const TEST: &str = "tests::seed_audit_s_process_crash_preserves_ack_suffix_and_retry";
        const DIRECTORY: &str = "AFS_P3_S_RECOVERY_DIRECTORY";
        const CHILD: &str = "AFS_P3_S_RECOVERY_CHILD";
        let root =
            PathBuf::from(std::env::var_os(DIRECTORY).expect("explicit owned test directory"));
        assert!(root.is_absolute());
        assert_eq!(
            fs::read(root.join("owned")).unwrap(),
            b"owned physical S seed copy\n"
        );
        let directory = root.join("seed-copy");
        let fixture: Value =
            serde_json::from_slice(&fs::read(fixture_path(&directory)).unwrap()).unwrap();
        let dimensions = Dataset::S.dimensions(None, None).unwrap();
        assert_eq!(
            fixture["dimensions"],
            serde_json::to_value(dimensions).unwrap()
        );
        assert_eq!(fixture["seed_revision"], 391);
        assert_eq!(fixture["seed_batches"], 391);
        assert_eq!(fixture["seed_batch_files"], SEED_BATCH_FILES);
        let txn = recovery_probe_transaction();
        let request_debug = format!("{txn:?}");
        if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1")) {
            let backend = Arc::new(LocalLog::open(&directory, Default::default()).unwrap());
            let before = backend.recover().await.unwrap();
            assert_eq!(before.checkpoint.as_ref().unwrap().covered.sequence, 391);
            assert_eq!(before.tip.sequence, 391);
            assert!(before.records.is_empty());
            let store = Store::open_log(backend.clone()).await.unwrap();
            let ack = store.compare_and_commit(txn.clone()).await.unwrap();
            let TxnOutcome::Committed { revision, outcome } = ack else {
                panic!("probe request did not receive a successful ACK");
            };
            assert_eq!(revision, StoreRevision(392));
            let status = backend.status().await.unwrap();
            assert_eq!(status.checkpoint.sequence, 391);
            assert_eq!(status.tip.sequence, 392);
            write_new(&root.join("ack.tmp"), &json!({
                "request_debug": request_debug, "revision": revision.0, "outcome": outcome,
                "checkpoint_sequence":status.checkpoint.sequence,"tip_sequence":status.tip.sequence,
                "identity":fs::read(directory.join("IDENTITY")).unwrap(),
                "scope":"caller observed committed ACK before deliberate process SIGKILL"
            })).unwrap();
            fs::rename(root.join("ack.tmp"), root.join("ack.json")).unwrap();
            fs::File::open(&root).unwrap().sync_all().unwrap();
            fs::write(root.join("ready"), b"ACK witness complete").unwrap();
            // The current-thread runtime cannot run background maintenance here.
            // Only the supervising test terminates this deliberately paused child.
            loop {
                std::thread::park_timeout(Duration::from_secs(1));
            }
        }
        let child_log = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(root.join("child.log"))
            .unwrap();
        let mut child = RecoveryProbeChild(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    TEST,
                    "--ignored",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD, "1")
                .env(DIRECTORY, &root)
                .stdout(Stdio::from(child_log.try_clone().unwrap()))
                .stderr(Stdio::from(child_log))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        while !root.join("ready").is_file() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before ACK witness"
            );
            assert!(
                Instant::now() < deadline,
                "child ACK timeout; preserve owned evidence"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let witness: Value =
            serde_json::from_slice(&fs::read(root.join("ack.json")).unwrap()).unwrap();
        assert_eq!(witness["request_debug"], request_debug);
        assert_eq!(witness["revision"], 392);
        child.0.kill().unwrap();
        let status = child.0.wait().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        let start = Instant::now();
        let backend = Arc::new(LocalLog::open(&directory, Default::default()).unwrap());
        let backend_open_ns = elapsed_ns(start.elapsed());
        let recovered = backend.recover().await.unwrap();
        assert_eq!(recovered.checkpoint.as_ref().unwrap().covered.sequence, 391);
        assert_eq!(recovered.records.len(), 1);
        assert_eq!(recovered.tip.sequence, 392);
        let start = Instant::now();
        let store = Store::open_log(backend.clone()).await.unwrap();
        let store_open_ns = elapsed_ns(start.elapsed());
        assert_eq!(
            witness["identity"],
            json!(fs::read(directory.join("IDENTITY")).unwrap())
        );
        let audit = audit_seed_at_declared_head(
            &store,
            dimensions,
            3_000_000,
            StoreRevision(391),
            StoreRevision(392),
        )
        .await
        .unwrap();
        assert_eq!(audit["checked_expected_entities"], 700001);
        assert_eq!(audit["checked_seed_ack_outcomes"], 391);
        let mut checked_suffix_entities = 0;
        for mutation in &txn.mutations {
            if let TxnMutation::Put(expected) = mutation {
                let actual = store
                    .read(seed_entity_read(expected).unwrap())
                    .await
                    .unwrap();
                assert_eq!(actual.entity.as_ref(), Some(expected));
                assert_eq!(actual.revision, StoreRevision(392));
                checked_suffix_entities += 1;
            }
        }
        assert_eq!(checked_suffix_entities, 2);
        let original = store
            .read(MetaRead::RequestOutcome(txn.request.clone()))
            .await
            .unwrap();
        assert_eq!(original.revision, StoreRevision(392));
        let expected_outcome: RequestOutcome =
            serde_json::from_value(witness["outcome"].clone()).unwrap();
        assert_eq!(original.request_outcome.as_ref(), Some(&expected_outcome));
        let before_retry = recovery_probe_files(&directory);
        let tip_before_retry = backend.status().await.unwrap().tip;
        assert_eq!(
            store.compare_and_commit(txn).await.unwrap(),
            TxnOutcome::ConditionFailed {
                revision: StoreRevision(392),
                existing_outcome: Some(expected_outcome)
            },
        );
        assert_eq!(backend.status().await.unwrap().tip, tip_before_retry);
        assert_eq!(recovery_probe_files(&directory), before_retry);
        let after = store
            .read(MetaRead::RequestOutcome(RequestKey::new(
                "meta-bench",
                "__audit_read_revision_only__",
            )))
            .await
            .unwrap();
        assert_eq!(after.revision, StoreRevision(392));
        write_new(&root.join("result.json"), &json!({
            "status":"PASS","audit":audit,"checked_suffix_entities":checked_suffix_entities,
            "checked_suffix_ack_outcomes":1,"child_signal":status.signal(),
            "checkpoint_sequence":391,"replayed_suffix_records":1,"recovered_tip_sequence":392,
            "exact_request_debug_matched":true,"retry_same_outcome":true,
            "retry_unchanged_tip_and_all_store_file_bytes":true,"business_revision_after_retry":392,
            "identity_preserved":true,"backend_open_ns":backend_open_ns,"store_open_ns":store_open_ns,
            "formal_target_pass":null,"formal_campaigns_added":0,
            "scope":"one S seed checkpoint+ACKed WAL suffix after actual process SIGKILL; not power loss, old load ACKs, M/L, formal recovery timing or network service readiness"
        })).unwrap();
    }

    #[tokio::test]
    async fn seed_audit_local_checkpoint_suffix_checks_every_field_revision_and_outcome() {
        let directory = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalLog::open(directory.path(), Default::default()).unwrap());
        let store = Store::open_log(backend.clone()).await.unwrap();
        let dimensions = Dataset::S.dimensions(Some(257), None).unwrap();
        for (number, (first, end)) in [(0, 256), (256, 257)].into_iter().enumerate() {
            assert_eq!(
                committed(
                    &store,
                    seed_transaction_range(dimensions, 2000, first, end).unwrap()
                )
                .await
                .unwrap(),
                StoreRevision(number as u64 + 1)
            );
            if number == 0 {
                assert_eq!(checkpoint_seed(&store).await.unwrap().sequence, 1);
            }
        }
        drop(store);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&backend) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(backend);
        let backend = Arc::new(LocalLog::open(directory.path(), Default::default()).unwrap());
        let recovered = backend.recover().await.unwrap();
        assert_eq!(recovered.checkpoint.unwrap().covered.sequence, 1);
        assert_eq!(recovered.records.len(), 1);
        let store = Store::open_log(backend).await.unwrap();
        let report = audit_seed(&store, dimensions, 2000, StoreRevision(2))
            .await
            .unwrap();
        assert_eq!(report["checked_expected_entities"], 1800);
        assert_eq!(report["checked_seed_ack_outcomes"], 2);
        assert_eq!(report["business_revision_before"], 2);
        assert_eq!(report["business_revision_after"], 2);
        assert_eq!(report["per_entity_mod_revisions_match"], true);
    }

    #[tokio::test]
    async fn seed_audit_rejects_wrong_mod_revisions_with_matching_tail_and_business_revision() {
        let store = Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap();
        let dimensions = Dataset::S.dimensions(Some(257), None).unwrap();
        // Same entity contents and final business revision as the fixture, but
        // the middle entities were installed in the wrong seed batch.
        for (first, end) in [(0, 128), (128, 257)] {
            committed(
                &store,
                seed_transaction_range(dimensions, 2000, first, end).unwrap(),
            )
            .await
            .unwrap();
        }
        let error = audit_seed(&store, dimensions, 2000, StoreRevision(2))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("seed revision mismatch"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn seed_audit_rejects_a_post_load_store_instead_of_reinterpreting_the_fixture() {
        let store = Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap();
        let dimensions = Dataset::S.dimensions(Some(2), None).unwrap();
        committed(&store, seed_transaction(dimensions, 100).unwrap())
            .await
            .unwrap();
        committed(
            &store,
            transaction(
                "post-seed-operation".into(),
                StoreOperation::DfsSetInodeAttributes,
            ),
        )
        .await
        .unwrap();
        let error = audit_seed(&store, dimensions, 100, StoreRevision(1))
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("pristine seed business revision"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn service_smoke_uses_real_domain_create_and_original_retry_result() {
        let store = Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        );
        committed(
            store.as_ref(),
            seed_transaction(Dataset::L10.dimensions(Some(1), None).unwrap(), 10).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(service_smoke(store).await.unwrap()["functional_pass"], true);
    }
}
