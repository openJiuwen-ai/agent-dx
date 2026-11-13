//! Acquiring a DFS write lease retries when the lease entity changes between
//! its read and its commit (GC deleting the expired lease, another Node
//! taking it), instead of failing the open with `META_DFS_CONFLICT`.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use afs::{
    dfs::{
        InodeAttributes, InodeId, InodeRecord, NamespaceId, OperationId, ReplicationConfig,
        WriteLease,
    },
    meta::{
        dfs::{CreateFileRequest, DfsService},
        store::{
            BackendReadiness, MetaEntity, MetaFuture, MetaKey, MetaRead, MetaReadView, MetaStore,
            MetaTxn, NodeSessionLease, OperationResult, OwnerRootScan, RecoveryScan, RequestKey,
            RequestOutcome, Store, StoreOperation, StoreRevision, TxnCondition, TxnMutation,
            TxnOutcome, WatchBatch, journal::MemoryLog,
        },
    },
};

type Hook = Arc<dyn Fn() -> futures_util::future::BoxFuture<'static, ()> + Send + Sync>;

/// Runs `hook` right before each of the next `remaining` lease acquire
/// commits: whatever it does to the lease lands between the acquire's read
/// and its commit.
struct LeaseRaceStore {
    inner: Arc<Store>,
    hook: Mutex<Option<Hook>>,
    remaining: AtomicUsize,
}

impl MetaStore for LeaseRaceStore {
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
            if txn.operation == StoreOperation::DfsAcquireWriteLease
                && self
                    .remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                        left.checked_sub(1)
                    })
                    .is_ok()
            {
                let hook = self.hook.lock().unwrap().clone();
                if let Some(hook) = hook {
                    hook().await;
                }
            }
            self.inner.compare_and_commit(txn).await
        })
    }

    fn compare_and_commit_background(&self, txn: MetaTxn) -> MetaFuture<'_, TxnOutcome> {
        self.inner.compare_and_commit_background(txn)
    }

    fn watch(&self, after_revision: StoreRevision, limit: usize) -> MetaFuture<'_, WatchBatch> {
        self.inner.watch(after_revision, limit)
    }

    fn register_node_session(
        &self,
        request: RequestKey,
        lease: NodeSessionLease,
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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn put(store: &Store, label: &str, mutations: Vec<TxnMutation>) {
    let request = RequestKey::new("fixture", label);
    let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsCollectGarbage);
    txn.conditions
        .push(TxnCondition::RequestAbsent(request.clone()));
    txn.mutations = mutations;
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request,
            operation: StoreOperation::DfsCollectGarbage,
            result: OperationResult::Empty,
        }));
    assert!(matches!(
        store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::Committed { .. }
    ));
}

async fn stored_lease(store: &Store, inode: &InodeId) -> Option<WriteLease> {
    match store
        .read(MetaRead::DfsWriteLease(inode.clone()))
        .await
        .unwrap()
        .entity
    {
        Some(MetaEntity::DfsWriteLease(lease)) => Some(lease),
        _ => None,
    }
}

/// A file node-a created whose write lease has just expired (node-a stopped
/// renewing), node-b registered, and the racing store.
async fn fixture() -> (Arc<LeaseRaceStore>, DfsService, InodeRecord) {
    let store = Arc::new(LeaseRaceStore {
        inner: Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        ),
        hook: Mutex::new(None),
        remaining: AtomicUsize::new(0),
    });
    let dfs =
        DfsService::with_replication_config(store.clone(), ReplicationConfig::local_single_copy())
            .with_gc_grace_ms(0);
    dfs.initialize_replication_config().await.unwrap();
    store
        .inner
        .register_node_session(
            RequestKey::new("node-b", "register"),
            NodeSessionLease {
                node_id: "node-b".into(),
                session_id: "session-b".into(),
                grpc_addr: "http://b:1".into(),
                data_addr: "http://b:2".into(),
                rest_addr: "http://b:3".into(),
                nfs_addr: String::new(),
                ownerfs_enabled: false,
                storage_devices: Vec::new(),
                lease_ttl: std::time::Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    let (inode, mut lease) = dfs
        .create(CreateFileRequest {
            caller_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            operation_id: OperationId::new("create-f"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"f".to_vec(),
            attributes: InodeAttributes {
                mode: 0o100644,
                uid: 0,
                gid: 0,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            },
            lease_seconds: 30,
            first_generation: None,
        })
        .await
        .unwrap();
    lease.expires_at_unix_ms = now_ms() - 1;
    put(
        &store.inner,
        "expire",
        vec![TxnMutation::Put(MetaEntity::DfsWriteLease(lease))],
    )
    .await;
    (store, dfs, inode)
}

async fn node_b_opens_for_write(
    dfs: &DfsService,
    inode: &InodeRecord,
    operation: &str,
) -> afs_error::Result<(InodeRecord, WriteLease)> {
    dfs.resolve_write_authority(
        "node-b".into(),
        "session-b".into(),
        OperationId::new(operation),
        inode.inode_id.clone(),
        30,
    )
    .await
}

/// The race of `dfs.fault.lapsed_lease_reader_under_gc`: a real GC pass
/// deletes the expired lease between node-b's read of it and its acquire
/// commit. The acquire is planned again and succeeds.
#[tokio::test]
async fn an_acquire_racing_gc_of_the_expired_lease_is_retried() {
    let (store, dfs, inode) = fixture().await;
    let gc = (dfs.clone(), store.inner.clone(), inode.inode_id.clone());
    *store.hook.lock().unwrap() = Some(Arc::new(move || {
        let (dfs, inner, inode_id) = gc.clone();
        Box::pin(async move {
            assert!(stored_lease(&inner, &inode_id).await.is_some());
            dfs.collect_garbage("node-b".into(), "session-b".into(), 0, vec![])
                .await
                .unwrap();
            assert!(
                stored_lease(&inner, &inode_id).await.is_none(),
                "a GC pass deletes an expired write lease"
            );
        })
    }));
    store.remaining.store(1, Ordering::SeqCst);
    let (_, lease) = node_b_opens_for_write(&dfs, &inode, "open-gc")
        .await
        .expect("the acquire is retried after GC removed the expired lease");
    assert_eq!(lease.owner_node_id, "node-b");
    assert_eq!(lease.owner_session_id, "session-b");
    assert_eq!(store.remaining.load(Ordering::SeqCst), 0, "the race ran");
    assert_eq!(
        stored_lease(&store.inner, &inode.inode_id).await,
        Some(lease)
    );
}

/// Another Node takes the lease in the same window: the retry sees its live
/// lease and returns it (the caller proxies), never overwriting it.
#[tokio::test]
async fn an_acquire_that_lost_the_lease_to_another_node_returns_that_lease() {
    let (store, dfs, inode) = fixture().await;
    let theirs = WriteLease {
        inode_id: inode.inode_id.clone(),
        owner_node_id: "node-c".into(),
        owner_session_id: "session-c".into(),
        lease_epoch: 2,
        expires_at_unix_ms: now_ms() + 30_000,
    };
    let taken = (store.inner.clone(), theirs.clone());
    *store.hook.lock().unwrap() = Some(Arc::new(move || {
        let (inner, theirs) = taken.clone();
        Box::pin(async move {
            put(
                &inner,
                "node-c-acquires",
                vec![TxnMutation::Put(MetaEntity::DfsWriteLease(theirs))],
            )
            .await;
        })
    }));
    store.remaining.store(1, Ordering::SeqCst);
    let (_, lease) = node_b_opens_for_write(&dfs, &inode, "open-lost")
        .await
        .expect("losing the race is not an error: the winner's lease is returned");
    assert_eq!(lease, theirs);
    assert_eq!(
        stored_lease(&store.inner, &inode.inode_id).await,
        Some(theirs)
    );
}

/// A lease that changes before every attempt ends the bounded retry with the
/// same conflict as before.
#[tokio::test]
async fn an_acquire_that_keeps_losing_ends_with_a_conflict() {
    let (store, dfs, inode) = fixture().await;
    let flip = (
        store.inner.clone(),
        inode.inode_id.clone(),
        Arc::new(AtomicUsize::new(0)),
    );
    *store.hook.lock().unwrap() = Some(Arc::new(move || {
        let (inner, inode_id, round) = flip.clone();
        Box::pin(async move {
            // An expired lease whose epoch moves every time: never live, so
            // every attempt plans an acquire and loses its condition.
            let n = round.fetch_add(1, Ordering::SeqCst);
            put(
                &inner,
                &format!("flip-{n}"),
                vec![TxnMutation::Put(MetaEntity::DfsWriteLease(WriteLease {
                    inode_id: inode_id.clone(),
                    owner_node_id: "node-a".into(),
                    owner_session_id: "session-a".into(),
                    lease_epoch: 10 + n as u64,
                    expires_at_unix_ms: 1,
                }))],
            )
            .await;
        })
    }));
    store.remaining.store(usize::MAX, Ordering::SeqCst);
    let error = node_b_opens_for_write(&dfs, &inode, "open-storm")
        .await
        .expect_err("contention on every attempt must end");
    assert_eq!(error.code(), afs_error::META_DFS_CONFLICT);
}

/// `OpenWrite` takes the same path.
#[tokio::test]
async fn open_write_retries_the_same_race() {
    let (store, dfs, inode) = fixture().await;
    let gone = (store.inner.clone(), inode.inode_id.clone());
    *store.hook.lock().unwrap() = Some(Arc::new(move || {
        let (inner, inode_id) = gone.clone();
        Box::pin(async move {
            put(
                &inner,
                "gc-delete",
                vec![TxnMutation::Delete(MetaKey::DfsWriteLease(inode_id))],
            )
            .await;
        })
    }));
    store.remaining.store(1, Ordering::SeqCst);
    let (_, lease) = dfs
        .open_write(
            "node-b".into(),
            "session-b".into(),
            OperationId::new("open-write"),
            inode.inode_id.clone(),
            30,
        )
        .await
        .expect("OpenWrite retries too");
    assert_eq!(lease.owner_node_id, "node-b");
}
