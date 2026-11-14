//! B4: `SetInodeAttributes` without a client revision retries benign Meta
//! drift internally, and a retried operation id is bound to its request.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use afs::{
    dfs::{
        CallerContext, InodeAttrUpdate, InodeAttributes, InodeId, InodeRecord, NamespaceId,
        OperationId, ReplicationConfig, SetInodeAttrRequest,
    },
    meta::{
        dfs::{CreateFileRequest, DfsService},
        store::{
            BackendReadiness, MetaEntity, MetaFuture, MetaRead, MetaReadView, MetaStore, MetaTxn,
            OperationResult, OwnerRootScan, RecoveryScan, RequestKey, RequestOutcome, Store,
            StoreOperation, StoreRevision, TxnCondition, TxnMutation, TxnOutcome, WatchBatch,
            journal::MemoryLog,
        },
    },
};

/// Bumps the target inode's revision right before each of the next
/// `remaining` setattr commits, as a racing background metadata sync would.
struct InodeDriftStore {
    inner: Arc<Store>,
    target: Mutex<Option<InodeId>>,
    /// The operation whose commits race the drift.
    operation: Mutex<StoreOperation>,
    remaining: AtomicUsize,
    sequence: AtomicUsize,
}

impl InodeDriftStore {
    async fn drift(&self, inode_id: &InodeId) -> afs_error::Result<()> {
        let Some(MetaEntity::DfsInode(mut inode)) = self
            .inner
            .read(MetaRead::DfsInode(inode_id.clone()))
            .await?
            .entity
        else {
            panic!("drift target must exist");
        };
        let request = RequestKey::new(
            "fixture",
            format!("drift-{}", self.sequence.fetch_add(1, Ordering::SeqCst)),
        );
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsSyncInodeMetadata);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request.clone()),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(inode.clone())),
        ]);
        inode.revision = inode.revision.saturating_add(1);
        inode.attributes.mtime_unix_ms = inode.attributes.mtime_unix_ms.saturating_add(1);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation: StoreOperation::DfsSyncInodeMetadata,
                result: OperationResult::DfsInode(inode),
            }),
        ]);
        assert!(matches!(
            self.inner.compare_and_commit(txn).await?,
            TxnOutcome::Committed { .. }
        ));
        Ok(())
    }
}

impl MetaStore for InodeDriftStore {
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
            let target = self.target.lock().unwrap().clone();
            let operation = *self.operation.lock().unwrap();
            if txn.operation == operation
                && let Some(target) = target
                && self
                    .remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                        left.checked_sub(1)
                    })
                    .is_ok()
            {
                self.drift(&target).await?;
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

fn caller() -> CallerContext {
    CallerContext {
        uid: 1000,
        gid: 1000,
        supplementary_gids: Vec::new(),
    }
}

/// The namespace root is root-owned 0755, so an ordinary user cannot create
/// in it. Fixtures that need a user-owned file directly under the root open
/// it first, as an administrator would for a shared directory. Meta creates
/// the root on the first namespace mutation, hence the probe directory.
async fn let_users_create_in_root(dfs: &DfsService) {
    dfs.mkdir(afs::dfs::MkdirRequest {
        caller_id: "node-a".into(),
        operation_id: afs::dfs::OperationId::new("fixture-root-probe"),
        namespace_id: afs::dfs::NamespaceId::new("default"),
        parent_inode_id: afs::dfs::InodeId::new("1"),
        name: b".fixture-root-probe".to_vec(),
        attributes: afs::dfs::InodeAttributes {
            mode: 0o700,
            uid: 0,
            gid: 0,
            nlink: 2,
            atime_unix_ms: 1,
            mtime_unix_ms: 1,
            ctime_unix_ms: 1,
        },
        caller: afs::dfs::CallerContext {
            uid: 0,
            gid: 0,
            supplementary_gids: Vec::new(),
        },
    })
    .await
    .unwrap();
    dfs.rmdir(afs::dfs::RmdirRequest {
        caller_id: "node-a".into(),
        operation_id: afs::dfs::OperationId::new("fixture-root-probe-remove"),
        namespace_id: afs::dfs::NamespaceId::new("default"),
        parent_inode_id: afs::dfs::InodeId::new("1"),
        name: b".fixture-root-probe".to_vec(),
        caller: afs::dfs::CallerContext {
            uid: 0,
            gid: 0,
            supplementary_gids: Vec::new(),
        },
    })
    .await
    .unwrap();
    dfs.set_inode_attributes(afs::dfs::SetInodeAttrRequest {
        caller_id: "node-a".into(),
        operation_id: afs::dfs::OperationId::new("fixture-open-root"),
        caller: afs::dfs::CallerContext {
            uid: 0,
            gid: 0,
            supplementary_gids: Vec::new(),
        },
        inode_id: afs::dfs::InodeId::new("1"),
        expected_inode_revision: None,
        update: afs::dfs::InodeAttrUpdate {
            mode: Some(0o1777),
            ..Default::default()
        },
    })
    .await
    .unwrap();
}

/// A DFS service whose store drifts the created file `drifts` times, and
/// that file.
async fn fixture(drifts: usize) -> (Arc<InodeDriftStore>, DfsService, InodeRecord) {
    let store = Arc::new(InodeDriftStore {
        inner: Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        ),
        target: Mutex::new(None),
        operation: Mutex::new(StoreOperation::DfsSetInodeAttributes),
        remaining: AtomicUsize::new(0),
        sequence: AtomicUsize::new(0),
    });
    let dfs =
        DfsService::with_replication_config(store.clone(), ReplicationConfig::local_single_copy());
    dfs.initialize_replication_config().await.unwrap();
    let_users_create_in_root(&dfs).await;
    let (inode, _) = dfs
        .create(CreateFileRequest {
            caller_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            operation_id: OperationId::new("create-f"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"f".to_vec(),
            attributes: InodeAttributes {
                mode: 0o100644,
                uid: 1000,
                gid: 1000,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            },
            lease_seconds: 30,
            first_generation: None,
            caller: CallerContext {
                uid: 1000,
                gid: 1000,
                supplementary_gids: Vec::new(),
            },
        })
        .await
        .unwrap();
    *store.target.lock().unwrap() = Some(inode.inode_id.clone());
    store.remaining.store(drifts, Ordering::SeqCst);
    (store, dfs, inode)
}

fn chmod(
    inode: &InodeRecord,
    operation: &str,
    mode: u32,
    expected: Option<u64>,
) -> SetInodeAttrRequest {
    SetInodeAttrRequest {
        caller_id: "node-a".into(),
        operation_id: OperationId::new(operation),
        caller: caller(),
        inode_id: inode.inode_id.clone(),
        expected_inode_revision: expected,
        update: InodeAttrUpdate {
            mode: Some(mode),
            ctime_unix_ms: Some(5),
            ..InodeAttrUpdate::default()
        },
    }
}

#[tokio::test]
async fn setattr_without_a_client_revision_retries_meta_drift() {
    let (store, dfs, inode) = fixture(3).await;
    let updated = dfs
        .set_inode_attributes(chmod(&inode, "chmod-drift", 0o100600, None))
        .await
        .expect("benign revision drift must be retried inside Meta");
    assert_eq!(updated.attributes.mode & 0o7777, 0o600);
    assert_eq!(
        store.remaining.load(Ordering::SeqCst),
        0,
        "every drift happened"
    );
    // The drifted mtime is kept: the retry rebuilt the update on the newer inode.
    assert_eq!(
        updated.attributes.mtime_unix_ms,
        inode.attributes.mtime_unix_ms + 3
    );
}

#[tokio::test]
async fn setattr_with_a_client_revision_still_fails_on_a_changed_inode() {
    let (_store, dfs, inode) = fixture(1).await;
    let error = dfs
        .set_inode_attributes(chmod(&inode, "chmod-cas", 0o100600, Some(inode.revision)))
        .await
        .expect_err("an explicit revision is a compare-and-set");
    assert_eq!(error.code(), afs_error::META_DFS_CONFLICT);
}

#[tokio::test]
async fn setattr_exhausting_its_retries_reports_a_bounded_retry_error() {
    let (_store, dfs, inode) = fixture(usize::MAX).await;
    let error = dfs
        .set_inode_attributes(chmod(&inode, "chmod-storm", 0o100600, None))
        .await
        .expect_err("drift on every attempt must end");
    assert_eq!(error.code(), afs_error::META_DFS_NAMESPACE_RETRY);
}

#[tokio::test]
async fn a_replayed_setattr_operation_is_bound_to_its_request() {
    let (_store, dfs, inode) = fixture(0).await;
    let first = dfs
        .set_inode_attributes(chmod(&inode, "chmod-once", 0o100600, None))
        .await
        .unwrap();
    let replay = dfs
        .set_inode_attributes(chmod(&inode, "chmod-once", 0o100600, None))
        .await
        .unwrap();
    assert_eq!(replay, first, "an exact retry returns the recorded result");
    let error = dfs
        .set_inode_attributes(chmod(&inode, "chmod-once", 0o100700, None))
        .await
        .expect_err("a reused id with another change must not replay");
    assert_eq!(error.code(), afs_error::META_CATALOG_INVALID_REQUEST);
}

/// B6: a rename whose commit lost a race with an update of the renamed file
/// (its close commit or metadata sync) is planned again inside Meta.
#[tokio::test]
async fn rename_retries_a_racing_update_of_the_source_inside_meta() {
    use afs::dfs::{RenameMode, RenameRequest};
    let (store, dfs, inode) = fixture(0).await;
    *store.operation.lock().unwrap() = StoreOperation::DfsRename;
    store.remaining.store(3, Ordering::SeqCst);
    let outcome = dfs
        .rename(RenameRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("rename-drift"),
            namespace_id: NamespaceId::new("default"),
            old_parent_inode_id: InodeId::new("1"),
            old_name: b"f".to_vec(),
            new_parent_inode_id: InodeId::new("1"),
            new_name: b"g".to_vec(),
            mode: RenameMode::Replace,
            // The namespace root is owned by root.
            caller: CallerContext {
                uid: 0,
                gid: 0,
                supplementary_gids: Vec::new(),
            },
        })
        .await
        .expect("a racing source update must be retried inside Meta");
    assert_eq!(outcome.inode.inode_id, inode.inode_id);
    assert_eq!(
        store.remaining.load(Ordering::SeqCst),
        0,
        "every drift happened"
    );
    assert!(
        dfs.lookup(
            NamespaceId::new("default"),
            InodeId::new("1"),
            b"g".to_vec()
        )
        .await
        .unwrap()
        .is_some()
    );
}

/// B5 (Meta): a metadata sync built on an older inode revision is refused,
/// even when the head is unchanged: it must not overwrite a later setattr.
#[tokio::test]
async fn a_metadata_sync_on_an_older_revision_is_refused() {
    use afs::dfs::{CommitMetadataDelta, CommitMetadataMode, SyncInodeMetadata};
    let (store, dfs, inode) = fixture(0).await;
    let Some(MetaEntity::DfsWriteLease(lease)) = store
        .inner
        .read(MetaRead::DfsWriteLease(inode.inode_id.clone()))
        .await
        .unwrap()
        .entity
    else {
        panic!("the creator holds the write lease");
    };
    let updated = dfs
        .set_inode_attributes(SetInodeAttrRequest {
            update: InodeAttrUpdate {
                mtime_unix_ms: Some(42),
                atime_unix_ms: Some(42),
                ctime_unix_ms: Some(5),
                ..InodeAttrUpdate::default()
            },
            ..chmod(&inode, "utimens", 0, None)
        })
        .await
        .unwrap();
    assert_eq!(updated.attributes.mtime_unix_ms, 42);
    let sync = SyncInodeMetadata {
        operation_id: OperationId::new("late-sync"),
        inode_id: inode.inode_id.clone(),
        write_lease: lease,
        expected_inode_revision: inode.revision,
        expected_head_version: inode.head_version.clone(),
        metadata_delta: CommitMetadataDelta {
            mode: CommitMetadataMode::Full,
            mtime_unix_ms: Some(7),
            ctime_unix_ms: Some(8),
            kill_suidgid: false,
        },
    };
    let error = dfs
        .sync_inode_metadata("node-a".into(), sync)
        .await
        .expect_err("a sync frozen before the setattr must fail");
    assert_eq!(error.code(), afs_error::META_DFS_CONFLICT);
}
