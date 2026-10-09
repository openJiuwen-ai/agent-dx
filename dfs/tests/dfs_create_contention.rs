use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use afs::{
    dfs::{InodeAttributes, InodeId, NamespaceId, OperationId, ReplicationConfig},
    meta::{
        dfs::{CreateFileRequest, DfsService},
        store::{
            BackendReadiness, MetaEntity, MetaFuture, MetaRead, MetaReadView, MetaStore, MetaTxn,
            OperationResult, OwnerRootScan, RecoveryScan, RequestKey, RequestOutcome, Store,
            StoreOperation, StoreRevision, TxnCondition, TxnMutation, TxnOutcome, WatchBatch,
            memory::MemoryBackend,
        },
    },
};
use tokio::sync::Barrier;

const EXPECTED_CREATE_PARENT_DRIFT_RETRY_LIMIT: usize = 64;

async fn memory_dfs() -> DfsService {
    let store = Arc::new(
        Store::open(Arc::new(MemoryBackend::default()))
            .await
            .unwrap(),
    );
    let dfs = DfsService::with_replication_config(store, ReplicationConfig::local_single_copy());
    dfs.initialize_replication_config().await.unwrap();
    dfs
}

async fn memory_store() -> Arc<Store> {
    Arc::new(
        Store::open(Arc::new(MemoryBackend::default()))
            .await
            .unwrap(),
    )
}

fn attrs() -> InodeAttributes {
    InodeAttributes {
        mode: 0o644,
        uid: 1000,
        gid: 1000,
        nlink: 1,
        atime_unix_ms: 1,
        mtime_unix_ms: 1,
        ctime_unix_ms: 1,
    }
}

struct CreateParentInterferenceStore {
    inner: Arc<Store>,
    target_prefix: &'static str,
    remaining: AtomicUsize,
    change_parent_mode: bool,
    sequence: AtomicUsize,
}

impl CreateParentInterferenceStore {
    fn new(
        inner: Arc<Store>,
        target_prefix: &'static str,
        remaining: usize,
        change_parent_mode: bool,
    ) -> Self {
        Self {
            inner,
            target_prefix,
            remaining: AtomicUsize::new(remaining),
            change_parent_mode,
            sequence: AtomicUsize::new(0),
        }
    }

    fn should_interfere(&self, txn: &MetaTxn) -> bool {
        txn.operation == StoreOperation::DfsCreate
            && txn.request.request_id.starts_with(self.target_prefix)
            && self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
    }

    async fn mutate_parent_before_create(&self) -> afs_error::Result<()> {
        let snapshot = self
            .inner
            .read(MetaRead::DfsInode(InodeId::new("1")))
            .await?;
        let Some(MetaEntity::DfsInode(mut parent)) = snapshot.entity else {
            panic!("test fixture must initialize the DFS namespace root first");
        };
        let request = RequestKey::new(
            "fixture",
            format!(
                "{}-parent-interference-{}",
                self.target_prefix,
                self.sequence.fetch_add(1, Ordering::SeqCst)
            ),
        );
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsSetInodeAttributes);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request.clone()),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(parent.clone())),
        ]);
        parent.revision = parent.revision.saturating_add(1);
        parent.attributes.mtime_unix_ms = parent.attributes.mtime_unix_ms.saturating_add(1);
        parent.attributes.ctime_unix_ms = parent.attributes.ctime_unix_ms.saturating_add(1);
        if self.change_parent_mode {
            parent.attributes.mode = 0o555;
        }
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsInode(parent.clone())),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation: StoreOperation::DfsSetInodeAttributes,
                result: OperationResult::DfsInode(parent),
            }),
        ]);
        match self.inner.compare_and_commit(txn).await? {
            TxnOutcome::Committed { .. } => Ok(()),
            TxnOutcome::ConditionFailed { .. } => {
                panic!("test interference parent mutation must not race")
            }
        }
    }
}

impl MetaStore for CreateParentInterferenceStore {
    fn health(&self) -> MetaFuture<'_, ()> {
        self.inner.health()
    }

    fn backend_readiness(&self) -> MetaFuture<'_, BackendReadiness> {
        self.inner.backend_readiness()
    }

    fn read_view(&self) -> MetaFuture<'_, MetaReadView> {
        self.inner.read_view()
    }

    fn read(&self, read: MetaRead) -> MetaFuture<'_, afs::meta::store::MetaSnapshot> {
        self.inner.read(read)
    }

    fn compare_and_commit(&self, txn: MetaTxn) -> MetaFuture<'_, TxnOutcome> {
        Box::pin(async move {
            if self.should_interfere(&txn) {
                self.mutate_parent_before_create().await?;
            }
            self.inner.compare_and_commit(txn).await
        })
    }

    fn watch(&self, after_revision: StoreRevision, limit: usize) -> MetaFuture<'_, WatchBatch> {
        self.inner.watch(after_revision, limit)
    }

    fn register_node_session(
        &self,
        request: RequestKey,
        lease: afs::meta::store::NodeSessionLease,
    ) -> MetaFuture<'_, TxnOutcome> {
        self.inner.register_node_session(request, lease)
    }

    fn scan_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> MetaFuture<'a, RecoveryScan> {
        self.inner.scan_roots_for_recovery(home_node_id)
    }

    fn scan_owner_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> MetaFuture<'a, OwnerRootScan> {
        self.inner.scan_owner_roots_for_recovery(home_node_id)
    }
}

fn create_request(operation: impl Into<String>, name: impl Into<Vec<u8>>) -> CreateFileRequest {
    CreateFileRequest {
        caller_id: "node-a".into(),
        owner_session_id: "session-a".into(),
        operation_id: OperationId::new(operation),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: InodeId::new("1"),
        name: name.into(),
        attributes: attrs(),
        lease_seconds: 30,
    }
}

async fn dfs_with_interference(
    target_prefix: &'static str,
    remaining: usize,
    change_parent_mode: bool,
) -> DfsService {
    let inner = memory_store().await;
    let dfs = DfsService::with_replication_config(
        Arc::new(CreateParentInterferenceStore::new(
            inner,
            target_prefix,
            remaining,
            change_parent_mode,
        )),
        ReplicationConfig::local_single_copy(),
    );
    dfs.initialize_replication_config().await.unwrap();
    dfs
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_file_creates_under_same_parent_survive_timestamp_contention() {
    let dfs = Arc::new(memory_dfs().await);
    let tasks = 24usize;
    let barrier = Arc::new(Barrier::new(tasks));
    let mut handles = Vec::new();

    for index in 0..tasks {
        let dfs = dfs.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            dfs.create(create_request(
                format!("distinct-{index}"),
                format!("file-{index}").into_bytes(),
            ))
            .await
        }));
    }

    let mut created = Vec::new();
    for handle in handles {
        created.push(
            handle
                .await
                .unwrap()
                .expect("distinct create must retry parent drift"),
        );
    }

    created.sort_by(|left, right| left.0.inode_id.0.cmp(&right.0.inode_id.0));
    created.dedup_by(|left, right| left.0.inode_id == right.0.inode_id);
    assert_eq!(
        created.len(),
        tasks,
        "each operation keeps a distinct inode"
    );

    for index in 0..tasks {
        let found = dfs
            .lookup(
                NamespaceId::new("default"),
                InodeId::new("1"),
                format!("file-{index}").into_bytes(),
            )
            .await
            .unwrap()
            .expect("created file must be visible by name");
        assert_eq!(found.kind, afs::dfs::InodeKind::Regular);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_name_contention_still_reports_one_real_target_conflict() {
    let dfs = Arc::new(memory_dfs().await);
    let barrier = Arc::new(Barrier::new(2));
    let mut handles = Vec::new();

    for index in 0..2 {
        let dfs = dfs.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            dfs.create(create_request(
                format!("same-name-{index}"),
                b"shared".to_vec(),
            ))
            .await
        }));
    }

    let mut successes = 0;
    let mut conflicts = 0;
    for handle in handles {
        match handle.await.unwrap() {
            Ok(_) => successes += 1,
            Err(error) if error.code() == afs_error::META_DFS_CONFLICT => conflicts += 1,
            Err(error) => panic!("unexpected create result: {error}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(conflicts, 1);
}

#[tokio::test]
async fn exact_operation_replay_survives_later_parent_updates() {
    let dfs = memory_dfs().await;
    let request = create_request("replay-create", b"stable".to_vec());
    let first = dfs.create(request.clone()).await.unwrap();
    dfs.create(create_request("advance-parent", b"other".to_vec()))
        .await
        .unwrap();

    assert_eq!(dfs.create(request.clone()).await.unwrap(), first);

    let mut changed = request;
    changed.name = b"changed".to_vec();
    let error = dfs.create(changed).await.unwrap_err();
    assert_eq!(error.code(), afs_error::META_CATALOG_INVALID_REQUEST);
}

#[tokio::test]
async fn retry_does_not_hide_non_directory_parent_failure() {
    let dfs = memory_dfs().await;
    let (file, _) = dfs
        .create(create_request("parent-is-file", b"plain-file".to_vec()))
        .await
        .unwrap();
    let mut child = create_request("under-file", b"child".to_vec());
    child.parent_inode_id = file.inode_id;

    let error = dfs.create(child).await.unwrap_err();
    assert_eq!(error.code(), afs_error::IO_NOT_DIRECTORY);
}

#[tokio::test]
async fn retry_rejects_parent_permission_or_attribute_change() {
    let dfs = dfs_with_interference("attr-change", 1, true).await;
    let request = create_request("attr-change-target", b"blocked-by-parent-attr".to_vec());

    let error = dfs.create(request).await.unwrap_err();
    assert_eq!(error.code(), afs_error::META_DFS_CONFLICT);
    assert!(
        dfs.lookup(
            NamespaceId::new("default"),
            InodeId::new("1"),
            b"blocked-by-parent-attr".to_vec(),
        )
        .await
        .unwrap()
        .is_none(),
        "attribute-changing parent interference must not create the target"
    );
}

#[tokio::test]
async fn retry_reports_exhaustion_when_parent_only_drifts_every_attempt() {
    let dfs = dfs_with_interference(
        "exhaust-create",
        EXPECTED_CREATE_PARENT_DRIFT_RETRY_LIMIT,
        false,
    )
    .await;

    let error = dfs
        .create(create_request(
            "exhaust-create-target",
            b"never-stable".to_vec(),
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), afs_error::META_DFS_CONFLICT);
    assert!(
        error
            .to_string()
            .contains("parent changed repeatedly during bounded retry"),
        "exhausted drift should report the bounded retry stop condition: {error}"
    );
    assert!(
        dfs.lookup(
            NamespaceId::new("default"),
            InodeId::new("1"),
            b"never-stable".to_vec(),
        )
        .await
        .unwrap()
        .is_none(),
        "exhausted retry must not create the target"
    );
}
