//! Characterization snapshots of the transactions built by the repair and
//! commit paths. Splitting those functions must leave every condition and
//! mutation set unchanged; each snapshot is the sorted, normalized `Debug`
//! rendering of the `MetaTxn` values the service submits to the store.
//!
//! Regenerate after an intentional change with
//! `cargo test -p afs --lib txn_snapshot_tests::print_txn_snapshots -- --ignored --nocapture`
//! and copy each `=== <name> ===` block into `txn_snapshots/<name>.txt`.

use std::sync::Mutex;

use super::*;
use crate::dfs::{
    ChunkReceipt, CommitMetadataDelta, Extent, GcFence, LayoutRootId, StorageDeviceDescriptor,
    UpdateGcReference,
};
use crate::meta::store::{
    BackendReadiness, MetaSnapshot, NodeSessionLease, OwnerRootScan, RecoveryScan, Store,
    StoreRevision, WatchBatch, journal::MemoryLog,
};
use crate::node::chunk::StagedChunk;

/// Delegates to a real store and records every submitted transaction.
struct RecordingStore {
    inner: Arc<Store>,
    txns: Mutex<Vec<MetaTxn>>,
}

impl RecordingStore {
    fn take(&self) -> Vec<MetaTxn> {
        std::mem::take(&mut *self.txns.lock().unwrap())
    }
}

impl MetaStore for RecordingStore {
    fn health(&self) -> crate::meta::store::MetaFuture<'_, ()> {
        self.inner.health()
    }
    fn backend_readiness(&self) -> crate::meta::store::MetaFuture<'_, BackendReadiness> {
        self.inner.backend_readiness()
    }
    fn read_view(&self) -> crate::meta::store::MetaFuture<'_, MetaReadView> {
        self.inner.read_view()
    }
    fn read(&self, read: MetaRead) -> crate::meta::store::MetaFuture<'_, MetaSnapshot> {
        self.inner.read(read)
    }
    fn compare_and_commit(&self, txn: MetaTxn) -> crate::meta::store::MetaFuture<'_, TxnOutcome> {
        self.txns.lock().unwrap().push(txn.clone());
        self.inner.compare_and_commit(txn)
    }
    fn watch(
        &self,
        after_revision: StoreRevision,
        limit: usize,
    ) -> crate::meta::store::MetaFuture<'_, WatchBatch> {
        self.inner.watch(after_revision, limit)
    }
    fn register_node_session(
        &self,
        request: RequestKey,
        lease: crate::meta::store::NodeSessionLease,
    ) -> crate::meta::store::MetaFuture<'_, TxnOutcome> {
        self.inner.register_node_session(request, lease)
    }
    fn scan_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> crate::meta::store::MetaFuture<'a, RecoveryScan> {
        self.inner.scan_roots_for_recovery(home_node_id)
    }
    fn scan_owner_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> crate::meta::store::MetaFuture<'a, OwnerRootScan> {
        self.inner.scan_owner_roots_for_recovery(home_node_id)
    }
}

/// Renders submitted transactions with volatile values removed: Unix-ms
/// timestamps (any run of 12 or more digits), request digests and
/// caller-supplied aliases such as fixture chunk and generation IDs.
fn render(txns: &[MetaTxn], aliases: &[(&str, &str)]) -> String {
    let mut out = String::new();
    for (index, txn) in txns.iter().enumerate() {
        let mut conditions = txn
            .conditions
            .iter()
            .map(|condition| normalize(&format!("{condition:?}"), aliases))
            .collect::<Vec<_>>();
        conditions.sort();
        let mut mutations = txn
            .mutations
            .iter()
            .map(|mutation| normalize(&format!("{mutation:?}"), aliases))
            .collect::<Vec<_>>();
        mutations.sort();
        out.push_str(&format!(
            "txn {index}: {:?} {}\n",
            txn.operation,
            normalize(&format!("{:?}", txn.request), aliases)
        ));
        out.push_str("conditions:\n");
        for condition in conditions {
            out.push_str(&format!("  {condition}\n"));
        }
        out.push_str("mutations:\n");
        for mutation in mutations {
            out.push_str(&format!("  {mutation}\n"));
        }
    }
    out
}

fn normalize(text: &str, aliases: &[(&str, &str)]) -> String {
    let mut text = text.to_owned();
    for (from, to) in aliases {
        text = text.replace(from, to);
    }
    // Request digests hash time-bearing requests (claims, leases); they are
    // pinned by `request_digest_golden_tests` instead.
    while let Some(start) = text.find("request_digest: [") {
        let open = start + "request_digest: ".len();
        let Some(close) = text[open..].find(']') else {
            break;
        };
        text.replace_range(open..=open + close, "<digest>");
    }
    let mut out = String::with_capacity(text.len());
    let mut digits = String::new();
    for character in text.chars().chain(std::iter::once('\0')) {
        if character.is_ascii_digit() {
            digits.push(character);
            continue;
        }
        if digits.len() >= 12 {
            out.push_str("<ms>");
        } else {
            out.push_str(&digits);
        }
        digits.clear();
        if character != '\0' {
            out.push(character);
        }
    }
    out
}

fn assert_snapshot(name: &str, expected: &str, actual: &str) {
    assert!(
        expected == actual,
        "transaction snapshot {name} changed; a pure refactor must keep \
         conditions and mutations identical. If the change is intentional, \
         regenerate meta/dfs/txn_snapshots/{name}.txt with print_txn_snapshots.\n\
         --- actual ---\n{actual}--- expected ---\n{expected}"
    );
}

fn device(id: &str, catalog_revision: u64) -> StorageDeviceDescriptor {
    StorageDeviceDescriptor {
        device_id: format!("{id}-dev"),
        device_epoch: 1,
        catalog_revision,
        failure_domain: format!("{id}-fd"),
    }
}

async fn register(store: &Store, node: &str, session: &str, catalog_revision: u64) {
    store
        .register_node_session(
            RequestKey::new(node, format!("register-{session}")),
            NodeSessionLease {
                node_id: node.into(),
                session_id: session.into(),
                grpc_addr: format!("http://{node}:1"),
                data_addr: format!("http://{node}:2"),
                rest_addr: format!("http://{node}:3"),
                ownerfs_enabled: false,
                storage_devices: vec![device(node, catalog_revision)],
                lease_ttl: std::time::Duration::from_secs(60),
            },
        )
        .await
        .unwrap();
}

async fn seed(store: &Store, operation: &str, entities: Vec<MetaEntity>) {
    let key = RequestKey::new("fixture", operation);
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations
        .extend(entities.into_iter().map(TxnMutation::Put));
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }));
    assert!(matches!(
        store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::Committed { .. }
    ));
}

struct RepairFixture {
    store: Arc<Store>,
    recording: Arc<RecordingStore>,
    service: DfsService,
    chunk: ChunkObject,
    copy: CopyRecord,
    placement: PlacementRecord,
}

impl RepairFixture {
    async fn new() -> Self {
        let store = Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        );
        let recording = Arc::new(RecordingStore {
            inner: store.clone(),
            txns: Mutex::new(Vec::new()),
        });
        register(&store, "source", "source-s1", 0).await;
        register(&store, "target", "target-s1", 0).await;
        let service = DfsService::with_replication_config(
            recording.clone(),
            ReplicationConfig {
                desired_copies: 2,
                sync_required_copies: 2,
                min_distinct_nodes: 1,
                min_distinct_failure_domains: 1,
                local_copy: LocalCopyPolicy::Required,
            },
        );
        service.initialize_replication_config().await.unwrap();
        let chunk = StagedChunk::new(OperationId::new("seed"), b"snapshot body".to_vec()).chunk;
        let copy = CopyRecord {
            id: CopyId::new("copy-source"),
            chunk_id: chunk.id.clone(),
            role: CopyRole::DurableReplica,
            state: CopyState::Ready,
            location: CopyLocation::Node {
                node_id: "source".into(),
                node_epoch: 1,
                device_id: "source-dev".into(),
                device_epoch: 1,
                catalog_revision: 1,
            },
            persisted_bytes: chunk.length,
            verified_digest: chunk.content_digest.clone(),
        };
        let placement = PlacementRecord {
            chunk_id: chunk.id.clone(),
            replica_group_id: ReplicaGroupId::new("repair-r2"),
            placement_epoch: 2,
            desired_copies: 2,
            copies: vec![copy.id.clone()],
            health: PlacementHealth::UnderReplicated,
        };
        seed(
            &store,
            "seed-chunk",
            vec![
                MetaEntity::DfsChunk(chunk.clone()),
                MetaEntity::DfsCopy(copy.clone()),
                MetaEntity::DfsPlacement(placement.clone()),
            ],
        )
        .await;
        recording.take();
        Self {
            store,
            recording,
            service,
            chunk,
            copy,
            placement,
        }
    }

    fn render(&self) -> String {
        render(
            &self.recording.take(),
            &[(self.chunk.id.0.as_str(), "<chunk>")],
        )
    }

    fn task(&self, state: ReplicationTaskState) -> ReplicationTask {
        ReplicationTask {
            id: repair_task_id(&self.chunk.id),
            chunk_id: self.chunk.id.clone(),
            placement_epoch: self.placement.placement_epoch,
            desired_copies: 2,
            existing_copies: vec![self.copy.id.clone()],
            state,
            attempt: 0,
            next_retry_unix_ms: 0,
            last_error: None,
            claim: None,
        }
    }

    /// An unexpired claim held by a process with no current Node session.
    fn ghost_claim(&self) -> Box<ReplicationClaim> {
        Box::new(ReplicationClaim {
            task_id: repair_task_id(&self.chunk.id),
            operation_id: OperationId::new("ghost-claim"),
            worker_node_id: "ghost".into(),
            worker_node_epoch: 1,
            worker_session_id: "ghost-s1".into(),
            expires_at_unix_ms: now_unix_ms().saturating_add(600_000),
            fence: 1,
            chunk: self.chunk.clone(),
            source_copy_id: self.copy.id.clone(),
            placement_revision: 1,
            replica_group: ReplicaGroup {
                id: ReplicaGroupId::new("ghost-group"),
                placement_epoch: self.placement.placement_epoch,
                targets: Vec::new(),
            },
            replication: ReplicationConfig::local_single_copy(),
        })
    }

    async fn corrupt_source_copy(&self) {
        let mut corrupt = self.copy.clone();
        corrupt.state = CopyState::Corrupt;
        seed(
            &self.store,
            "corrupt-source",
            vec![MetaEntity::DfsCopy(corrupt)],
        )
        .await;
    }

    async fn claim(&self, caller: &str, session: &str, epoch: u64, operation: &str) -> String {
        self.service
            .claim_replication_task(ClaimReplicationTask {
                caller_id: caller.into(),
                caller_session_id: session.into(),
                caller_node_epoch: epoch,
                operation_id: OperationId::new(operation),
                lease_seconds: 30,
            })
            .await
            .unwrap();
        self.render()
    }

    async fn claimed(&self) -> ReplicationClaim {
        let claim = self
            .service
            .claim_replication_task(ClaimReplicationTask {
                caller_id: "source".into(),
                caller_session_id: "source-s1".into(),
                caller_node_epoch: 1,
                operation_id: OperationId::new("claim-before-report"),
                lease_seconds: 30,
            })
            .await
            .unwrap()
            .expect("the source can claim its underreplicated chunk");
        self.recording.take();
        claim
    }

    async fn report(&self, claim: ReplicationClaim, outcome: ReportOutcome) -> String {
        let (durable_acks, error, source_invalid) = match outcome {
            ReportOutcome::Success => (repair_acks(&claim), None, false),
            ReportOutcome::WorkerError => (Vec::new(), Some("copy failed".into()), false),
            ReportOutcome::SourceInvalid => (Vec::new(), None, true),
        };
        self.service
            .report_replication_task(ReportReplicationTask {
                caller_id: "source".into(),
                caller_session_id: "source-s1".into(),
                caller_node_epoch: 1,
                operation_id: OperationId::new("report"),
                claim,
                durable_acks,
                error,
                source_invalid,
            })
            .await
            .unwrap();
        self.render()
    }

    async fn report_corruption(&self, operation: &str, catalog_revision: u64) -> String {
        self.service
            .report_chunk_corruption(ReportChunkCorruption {
                caller_id: "source".into(),
                caller_session_id: "source-s1".into(),
                caller_node_epoch: 1,
                operation_id: OperationId::new(operation),
                chunk_id: self.chunk.id.clone(),
                device_id: "source-dev".into(),
                device_epoch: 1,
                catalog_revision,
            })
            .await
            .unwrap();
        self.render()
    }
}

enum ReportOutcome {
    Success,
    WorkerError,
    SourceInvalid,
}

fn repair_acks(claim: &ReplicationClaim) -> Vec<ReplicaAck> {
    claim
        .replica_group
        .targets
        .iter()
        .map(|target| ReplicaAck {
            operation_id: claim.operation_id.clone(),
            chunk_id: claim.chunk.id.clone(),
            placement_revision: claim.placement_revision,
            placement_epoch: claim.replica_group.placement_epoch,
            node_id: target.node_id.clone(),
            node_epoch: target.node_epoch,
            device_id: target.device.device_id.clone(),
            device_epoch: target.device.device_epoch,
            catalog_revision: target.device.catalog_revision.max(1),
            persisted_bytes: claim.chunk.length,
            verified_digest: claim.chunk.content_digest.clone(),
        })
        .collect()
}

// Claim F: a fresh claim for an underreplicated placement without a task.
async fn claim_fresh_task() -> String {
    let f = RepairFixture::new().await;
    f.claim("source", "source-s1", 1, "claim").await
}

// Claim B/F: a completed task is reactivated and compared by its stored value.
async fn claim_reactivates_completed_task() -> String {
    let f = RepairFixture::new().await;
    let completed = f.task(ReplicationTaskState::Completed);
    seed(
        &f.store,
        "seed-completed",
        vec![MetaEntity::DfsReplicationTask(completed)],
    )
    .await;
    f.claim("source", "source-s1", 1, "claim").await
}

// Claim C/F: the previous worker process restarted, so its unexpired claim is
// reassigned with the current session pointer and contents frozen.
async fn claim_after_worker_restart() -> String {
    let f = RepairFixture::new().await;
    f.claim("source", "source-s1", 1, "claim-first").await;
    register(&f.store, "source", "source-s2", 1).await;
    let Some(MetaEntity::NodeSession(session)) = f
        .store
        .read(MetaRead::CurrentNodeSession {
            node_id: "source".into(),
        })
        .await
        .unwrap()
        .entity
    else {
        panic!("restarted source session is missing");
    };
    f.claim(
        "source",
        "source-s2",
        session.lease_epoch,
        "claim-restarted",
    )
    .await
}

// Claim C/F: the worker of an unexpired claim has no current session.
async fn claim_with_missing_worker_session() -> String {
    let f = RepairFixture::new().await;
    let mut task = f.task(ReplicationTaskState::Running);
    task.attempt = 1;
    task.claim = Some(f.ghost_claim());
    seed(
        &f.store,
        "seed-ghost",
        vec![MetaEntity::DfsReplicationTask(task)],
    )
    .await;
    f.claim("source", "source-s1", 1, "claim").await
}

// Claim D: no live source; the task is recorded as BlockedNoSource.
async fn claim_blocks_without_source() -> String {
    let f = RepairFixture::new().await;
    f.corrupt_source_copy().await;
    f.claim("source", "source-s1", 1, "claim").await
}

// Claim D with retired-worker conditions.
async fn claim_blocks_without_source_after_worker_loss() -> String {
    let f = RepairFixture::new().await;
    let mut task = f.task(ReplicationTaskState::Running);
    task.attempt = 1;
    task.claim = Some(f.ghost_claim());
    seed(
        &f.store,
        "seed-ghost",
        vec![MetaEntity::DfsReplicationTask(task)],
    )
    .await;
    f.corrupt_source_copy().await;
    f.claim("source", "source-s1", 1, "claim").await
}

// Claim E: the caller holds no source copy, so repair debt is recorded
// without issuing a claim.
async fn claim_records_debt_without_source_worker() -> String {
    let f = RepairFixture::new().await;
    f.claim("target", "target-s1", 1, "claim").await
}

// Claim E with retired-worker conditions on a stored Pending task.
async fn claim_records_debt_after_worker_loss() -> String {
    let f = RepairFixture::new().await;
    let mut task = f.task(ReplicationTaskState::Pending);
    task.claim = Some(f.ghost_claim());
    let mut placement = f.placement.clone();
    placement.health = PlacementHealth::Satisfied;
    seed(
        &f.store,
        "seed-pending-ghost",
        vec![
            MetaEntity::DfsReplicationTask(task),
            MetaEntity::DfsPlacement(placement),
        ],
    )
    .await;
    f.claim("target", "target-s1", 1, "claim").await
}

// Report C3: verified ACKs promote the repaired placement.
async fn report_repair_success() -> String {
    let f = RepairFixture::new().await;
    let claim = f.claimed().await;
    f.report(claim, ReportOutcome::Success).await
}

// Report C2: the worker failed and backs off.
async fn report_worker_error() -> String {
    let f = RepairFixture::new().await;
    let claim = f.claimed().await;
    f.report(claim, ReportOutcome::WorkerError).await
}

// Report C1: the bound source failed verification and was the last source.
async fn report_source_invalid() -> String {
    let f = RepairFixture::new().await;
    let claim = f.claimed().await;
    f.report(claim, ReportOutcome::SourceInvalid).await
}

// Corruption F2: a Ready copy below the reported catalog revision is marked
// Corrupt and repair is scheduled.
async fn corruption_marks_copy() -> String {
    let f = RepairFixture::new().await;
    f.report_corruption("corrupt-first", 2).await
}

// Corruption F1: a repeated report is acknowledged without changes.
async fn corruption_repeated_report_is_acknowledged() -> String {
    let f = RepairFixture::new().await;
    f.report_corruption("corrupt-first", 2).await;
    f.report_corruption("corrupt-again", 2).await
}

struct CommitFixture {
    recording: Arc<RecordingStore>,
    service: DfsService,
    commit: CommitFileVersion,
    body: Vec<u8>,
}

impl CommitFixture {
    async fn new(replication: ReplicationConfig) -> Self {
        let store = Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        );
        let recording = Arc::new(RecordingStore {
            inner: store.clone(),
            txns: Mutex::new(Vec::new()),
        });
        register(&store, "writer", "writer-s1", 0).await;
        register(&store, "peer", "peer-s1", 0).await;
        let service = DfsService::with_replication_config(recording.clone(), replication);
        service.initialize_replication_config().await.unwrap();
        let root = service
            .ensure_namespace_root(&NamespaceId::new("default"))
            .await
            .unwrap();
        let mut attributes = root.attributes;
        attributes.mode = 0o644;
        let (inode, write_lease) = service
            .create(CreateFileRequest {
                caller_id: "writer".into(),
                owner_session_id: "writer-s1".into(),
                operation_id: OperationId::new("create"),
                namespace_id: NamespaceId::new("default"),
                parent_inode_id: root.inode_id,
                name: b"snapshot.bin".to_vec(),
                attributes,
                lease_seconds: 60,
            })
            .await
            .unwrap();
        let body = b"commit snapshot body".to_vec();
        let operation_id = OperationId::new("commit");
        let chunk = StagedChunk::new(operation_id.clone(), body.clone()).chunk;
        let layout = LayoutRoot {
            id: LayoutRootId::new("layout"),
            file_length: chunk.length,
            inline_extents: vec![Extent {
                file_offset: 0,
                length: chunk.length,
                chunk_id: chunk.id.clone(),
                chunk_offset: 0,
            }],
        };
        let commit = CommitFileVersion {
            gc_fence: None,
            operation_id: operation_id.clone(),
            inode_id: inode.inode_id.clone(),
            write_lease,
            expected_inode_revision: inode.revision,
            expected_head_version: inode.head_version.clone(),
            file_version: FileVersion {
                id: FileVersionId::new("version"),
                inode_id: inode.inode_id,
                parent_version: inode.head_version,
                length: chunk.length,
                layout_root: layout.id.clone(),
                created_at_unix_ms: 1,
            },
            layout_root: layout,
            chunk_receipts: Vec::new(),
            clone_source: None,
            metadata_delta: CommitMetadataDelta {
                mode: CommitMetadataMode::DataOnly,
                mtime_unix_ms: None,
                ctime_unix_ms: None,
                kill_suidgid: false,
            },
        };
        let mut fixture = Self {
            recording,
            service,
            commit,
            body,
        };
        fixture.set_receipt(chunk).await;
        fixture.recording.take();
        fixture
    }

    /// Replaces the single receipt with one writer-local ACK for `chunk`.
    async fn set_receipt(&mut self, chunk: ChunkObject) {
        let placement = self
            .service
            .placement_snapshot("writer".into())
            .await
            .unwrap();
        let group = placement
            .replica_groups
            .iter()
            .find(|group| group.targets[0].node_id == "writer")
            .unwrap()
            .clone();
        let ack = ReplicaAck {
            operation_id: self.commit.operation_id.clone(),
            chunk_id: chunk.id.clone(),
            placement_revision: placement.revision,
            placement_epoch: group.placement_epoch,
            node_id: "writer".into(),
            node_epoch: 1,
            device_id: "writer-dev".into(),
            device_epoch: 1,
            catalog_revision: 1,
            persisted_bytes: chunk.length,
            verified_digest: chunk.content_digest.clone(),
        };
        self.commit.layout_root.inline_extents[0].chunk_id = chunk.id.clone();
        self.commit.chunk_receipts = vec![ChunkReceipt {
            operation_id: self.commit.operation_id.clone(),
            chunk,
            placement_revision: placement.revision,
            placement_epoch: group.placement_epoch,
            replica_group_id: group.id,
            durable_acks: vec![ack],
            external_commit: None,
        }];
    }

    async fn commit(&self, aliases: &[(&str, &str)]) -> String {
        self.service
            .commit_file_version("writer".into(), self.commit.clone())
            .await
            .unwrap();
        let chunk = self.commit.chunk_receipts[0].chunk.id.0.clone();
        // The chunk alias goes first: CRC chunk IDs embed the generation.
        let mut all = vec![(chunk.as_str(), "<chunk>")];
        all.extend_from_slice(aliases);
        render(&self.recording.take(), &all)
    }

    async fn schedule_unrelated_gc(&self) {
        let garbage = StagedChunk::new(OperationId::new("garbage"), b"unreferenced".to_vec());
        let gc = self
            .service
            .collect_garbage(
                "writer".into(),
                "writer-s1".into(),
                0,
                vec![garbage.chunk.clone()],
            )
            .await
            .unwrap();
        assert!(gc.chunks.contains(&garbage.chunk.id));
        self.recording.take();
    }
}

fn single_copy() -> ReplicationConfig {
    ReplicationConfig::local_single_copy()
}

// Commit J: no fence and no GC state; the absent GC record is a condition.
async fn commit_without_fence() -> String {
    let f = CommitFixture::new(single_copy()).await;
    f.commit(&[]).await
}

// Commit J: no fence while GC state exists; its revision is a condition.
async fn commit_without_fence_with_gc_state() -> String {
    let f = CommitFixture::new(single_copy()).await;
    f.schedule_unrelated_gc().await;
    f.commit(&[]).await
}

// Commit J: an explicit revision fence supplies the GC condition.
async fn commit_with_revision_fence() -> String {
    let mut f = CommitFixture::new(single_copy()).await;
    f.schedule_unrelated_gc().await;
    f.commit.gc_fence = Some(f.service.current_gc_fence().await.unwrap());
    assert!(matches!(f.commit.gc_fence, Some(GcFence::Revision(_))));
    f.commit(&[]).await
}

// Commit E/J: a native write generation is admitted, fenced and retired.
async fn commit_with_write_generation() -> String {
    let mut f = CommitFixture::new(single_copy()).await;
    f.commit.operation_id = OperationId::new(crate::meta::store::timed_request_id("generation"));
    f.commit.gc_fence = Some(GcFence::WriteGeneration);
    let chunk = StagedChunk::new_crc32c(f.commit.operation_id.clone(), f.body.clone())
        .unwrap()
        .chunk;
    f.set_receipt(chunk).await;
    f.service
        .update_gc_reference(UpdateGcReference {
            open_read: None,
            close_open_read: false,
            node_id: "writer".into(),
            session_id: "writer-s1".into(),
            reference_id: format!("write:{}", f.commit.operation_id.0),
            inode_id: Some(f.commit.inode_id.clone()),
            version_id: None,
            publication: true,
            chunks: vec![],
            release: false,
            admission_deadline_unix_ms: now_unix_ms().saturating_add(30_000),
        })
        .await
        .unwrap();
    f.recording.take();
    let generation = ChunkObject::generation_id(&f.commit.operation_id);
    f.commit(&[(generation.as_str(), "<generation>")]).await
}

// Commit K: fewer ACKs than desired copies registers repair debt.
async fn commit_registers_underreplication() -> String {
    let f = CommitFixture::new(ReplicationConfig {
        desired_copies: 2,
        sync_required_copies: 1,
        min_distinct_nodes: 1,
        min_distinct_failure_domains: 1,
        local_copy: LocalCopyPolicy::Required,
    })
    .await;
    f.commit(&[]).await
}

macro_rules! snapshot_cases {
    ($($name:ident),+ $(,)?) => {
        mod cases {
            $(
                #[tokio::test]
                async fn $name() {
                    let actual = super::$name().await;
                    super::assert_snapshot(
                        stringify!($name),
                        include_str!(concat!("txn_snapshots/", stringify!($name), ".txt")),
                        &actual,
                    );
                }
            )+
        }

        #[tokio::test]
        #[ignore = "prints regenerated snapshots"]
        async fn print_txn_snapshots() {
            $(
                println!("=== {} ===\n{}=== end ===", stringify!($name), $name().await);
            )+
        }
    };
}

snapshot_cases!(
    claim_fresh_task,
    claim_reactivates_completed_task,
    claim_after_worker_restart,
    claim_with_missing_worker_session,
    claim_blocks_without_source,
    claim_blocks_without_source_after_worker_loss,
    claim_records_debt_without_source_worker,
    claim_records_debt_after_worker_loss,
    report_repair_success,
    report_worker_error,
    report_source_invalid,
    corruption_marks_copy,
    corruption_repeated_report_is_acknowledged,
    commit_without_fence,
    commit_without_fence_with_gc_state,
    commit_with_revision_fence,
    commit_with_write_generation,
    commit_registers_underreplication,
);
