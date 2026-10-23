use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use afs::{
    dfs::{
        CommitFileVersion, CommitMetadataDelta, CommitMetadataMode, FileVersion, FileVersionId,
        InodeAttributes, InodeId, LayoutRoot, LayoutRootId, NamespaceId, OperationId,
        ReplicationConfig, SyncInodeMetadata,
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

#[derive(Clone, Copy)]
enum Interference {
    Renew,
    UnlinkMetadata,
    UnlinkSibling,
    UnlinkParentPermissions,
    UnlinkReplacement,
    UnlinkReplyLost,
    RmdirNewChild,
    Fence,
    Head,
    Metadata,
}
impl Interference {
    fn namespace(self) -> bool {
        matches!(
            self,
            Self::UnlinkMetadata
                | Self::UnlinkSibling
                | Self::UnlinkParentPermissions
                | Self::UnlinkReplacement
                | Self::UnlinkReplyLost
                | Self::RmdirNewChild
        )
    }
}
struct LeaseInterferenceStore {
    inner: Arc<Store>,
    remaining: AtomicUsize,
    mode: Interference,
}
impl LeaseInterferenceStore {
    async fn mutate_before_unlink(&self, txn: &MetaTxn) -> afs_error::Result<()> {
        let service = DfsService::with_replication_config(
            self.inner.clone(),
            ReplicationConfig::local_single_copy(),
        );
        let operation = format!(
            "interfere-{}-{}",
            txn.request.request_id,
            self.remaining.load(Ordering::SeqCst)
        );
        if matches!(self.mode, Interference::UnlinkSibling) {
            service
                .unlink(unlink_request(&operation, b"sibling"))
                .await?;
        } else if matches!(self.mode, Interference::UnlinkParentPermissions) {
            let parent = txn
                .conditions
                .iter()
                .find_map(|condition| match condition {
                    TxnCondition::EntityEquals(MetaEntity::DfsInode(inode))
                        if inode.kind == afs::dfs::InodeKind::Directory =>
                    {
                        Some(inode.clone())
                    }
                    _ => None,
                })
                .unwrap();
            service
                .set_inode_attributes(afs::dfs::SetInodeAttrRequest {
                    caller_id: "node-a".into(),
                    operation_id: OperationId::new(operation),
                    caller: root_caller(),
                    inode_id: parent.inode_id,
                    expected_inode_revision: parent.revision,
                    update: afs::dfs::InodeAttrUpdate {
                        mode: Some(0o555),
                        ..Default::default()
                    },
                })
                .await?;
        } else if matches!(self.mode, Interference::UnlinkReplacement) {
            service
                .unlink(unlink_request(&operation, b"memory.live"))
                .await?;
            service
                .create(create_request(
                    format!("{operation}-replacement"),
                    b"memory.live".to_vec(),
                ))
                .await?;
        } else if matches!(self.mode, Interference::UnlinkReplyLost) {
            assert!(matches!(
                self.inner.compare_and_commit(txn.clone()).await?,
                TxnOutcome::Committed { .. }
            ));
        } else if matches!(self.mode, Interference::RmdirNewChild) {
            let inode = txn
                .conditions
                .iter()
                .find_map(|condition| match condition {
                    TxnCondition::EntityEquals(MetaEntity::DfsDentry(entry)) => {
                        Some(entry.inode_id.clone())
                    }
                    _ => None,
                })
                .unwrap();
            let mut request = create_request(operation, b"new-child".to_vec());
            request.parent_inode_id = inode;
            service.create(request).await?;
        } else {
            let inode = txn
                .conditions
                .iter()
                .find_map(|condition| match condition {
                    TxnCondition::EntityEquals(MetaEntity::DfsInode(inode))
                        if inode.kind == afs::dfs::InodeKind::Regular =>
                    {
                        Some(inode.clone())
                    }
                    _ => None,
                })
                .expect("unlink observes a regular inode");
            let Some(MetaEntity::DfsWriteLease(lease)) = self
                .inner
                .read(MetaRead::DfsWriteLease(inode.inode_id.clone()))
                .await?
                .entity
            else {
                panic!("writer lease missing")
            };
            service
                .sync_inode_metadata(
                    "node-a".into(),
                    SyncInodeMetadata {
                        operation_id: OperationId::new(operation),
                        inode_id: inode.inode_id,
                        write_lease: lease,
                        expected_inode_revision: inode.revision,
                        expected_head_version: inode.head_version,
                        metadata_delta: CommitMetadataDelta {
                            mode: CommitMetadataMode::Full,
                            mtime_unix_ms: Some(7000),
                            ctime_unix_ms: Some(7000),
                            kill_suidgid: false,
                        },
                    },
                )
                .await?;
        }
        Ok(())
    }

    async fn renew_before_commit(&self, txn: &MetaTxn) -> afs_error::Result<()> {
        let inode = txn
            .conditions
            .iter()
            .find_map(|condition| match condition {
                TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(lease)) => {
                    Some(lease.inode_id.clone())
                }
                _ => None,
            })
            .expect("commit must fence its lease");
        let Some(MetaEntity::DfsWriteLease(mut lease)) = self
            .inner
            .read(MetaRead::DfsWriteLease(inode))
            .await?
            .entity
        else {
            panic!("lease missing");
        };
        let request = RequestKey::new(
            "interference",
            format!(
                "{}-{}",
                txn.request.request_id,
                self.remaining.load(Ordering::SeqCst)
            ),
        );
        let mut mutation = MetaTxn::new(request.clone(), StoreOperation::DfsRenewWriteLease);
        mutation.conditions.extend([
            TxnCondition::RequestAbsent(request.clone()),
            TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(lease.clone())),
        ]);
        match self.mode {
            Interference::UnlinkMetadata
            | Interference::UnlinkSibling
            | Interference::UnlinkParentPermissions
            | Interference::UnlinkReplacement
            | Interference::UnlinkReplyLost
            | Interference::RmdirNewChild => {
                unreachable!("namespace interference uses its own path")
            }
            Interference::Renew => lease.expires_at_unix_ms += 10_000,
            Interference::Fence => {
                lease.lease_epoch += 1;
                lease.owner_session_id = "replacement-session".into();
            }
            Interference::Head | Interference::Metadata => {
                let Some(MetaEntity::DfsInode(mut inode)) = self
                    .inner
                    .read(MetaRead::DfsInode(lease.inode_id.clone()))
                    .await?
                    .entity
                else {
                    panic!("inode missing");
                };
                mutation
                    .conditions
                    .push(TxnCondition::EntityEquals(MetaEntity::DfsInode(
                        inode.clone(),
                    )));
                inode.revision += 1;
                if matches!(self.mode, Interference::Head) {
                    let layout = LayoutRoot {
                        id: LayoutRootId::new("competing-layout"),
                        file_length: 0,
                        inline_extents: vec![],
                    };
                    let version = FileVersion {
                        id: FileVersionId::new("competing-version"),
                        inode_id: inode.inode_id.clone(),
                        parent_version: inode.head_version.clone(),
                        length: 0,
                        layout_root: layout.id.clone(),
                        created_at_unix_ms: 1,
                    };
                    inode.head_version = Some(version.id.clone());
                    mutation.mutations.extend([
                        TxnMutation::Put(MetaEntity::DfsLayoutRoot(layout)),
                        TxnMutation::Put(MetaEntity::DfsFileVersion(version)),
                    ]);
                } else {
                    inode.attributes.nlink = 0;
                    inode.attributes.mode = 0o600;
                }
                mutation
                    .mutations
                    .push(TxnMutation::Put(MetaEntity::DfsInode(inode)));
            }
        }
        mutation.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsWriteLease(lease)),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation: StoreOperation::DfsRenewWriteLease,
                result: OperationResult::Empty,
            }),
        ]);
        assert!(matches!(
            self.inner.compare_and_commit(mutation).await?,
            TxnOutcome::Committed { .. }
        ));
        Ok(())
    }
}
impl MetaStore for LeaseInterferenceStore {
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
            let namespace = self.mode.namespace();
            if (if namespace {
                matches!(
                    txn.operation,
                    StoreOperation::DfsUnlink | StoreOperation::DfsRmdir
                )
            } else {
                matches!(
                    txn.operation,
                    StoreOperation::DfsCommitFileVersion | StoreOperation::DfsSyncInodeMetadata
                )
            }) && self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    value.checked_sub(1)
                })
                .is_ok()
            {
                if namespace {
                    self.mutate_before_unlink(&txn).await?;
                } else {
                    self.renew_before_commit(&txn).await?;
                }
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

#[tokio::test]
async fn commit_survives_same_identity_lease_renewal_between_validation_and_cas() {
    let store = Arc::new(LeaseInterferenceStore {
        inner: Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        ),
        remaining: AtomicUsize::new(1),
        mode: Interference::Renew,
    });
    let service =
        DfsService::with_replication_config(store, ReplicationConfig::local_single_copy());
    service.initialize_replication_config().await.unwrap();
    let (inode, lease) = service
        .create(create_request("create", b"source".to_vec()))
        .await
        .unwrap();
    let layout = LayoutRoot {
        id: LayoutRootId::new("layout"),
        file_length: 0,
        inline_extents: vec![],
    };
    let commit = CommitFileVersion {
        gc_fence: None,
        operation_id: OperationId::new("commit"),
        inode_id: inode.inode_id.clone(),
        write_lease: lease,
        expected_inode_revision: inode.revision,
        expected_head_version: None,
        file_version: FileVersion {
            id: FileVersionId::new("version"),
            inode_id: inode.inode_id.clone(),
            parent_version: None,
            length: 0,
            layout_root: layout.id.clone(),
            created_at_unix_ms: 1,
        },
        layout_root: layout,
        chunk_receipts: vec![],
        clone_source: None,
        metadata_delta: CommitMetadataDelta {
            mode: CommitMetadataMode::DataOnly,
            mtime_unix_ms: None,
            ctime_unix_ms: None,
            kill_suidgid: false,
        },
    };
    let result = service
        .commit_file_version("node-a".into(), commit.clone())
        .await
        .expect("same-owner renewal is not a terminal data conflict");
    assert_eq!(result.head_version, Some(FileVersionId::new("version")));
    assert_eq!(
        service
            .commit_file_version("node-a".into(), commit)
            .await
            .unwrap(),
        result
    );
}

#[tokio::test]
async fn metadata_sync_survives_same_identity_lease_renewal_between_validation_and_cas() {
    let store = Arc::new(LeaseInterferenceStore {
        inner: Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        ),
        remaining: AtomicUsize::new(1),
        mode: Interference::Renew,
    });
    let service =
        DfsService::with_replication_config(store, ReplicationConfig::local_single_copy());
    service.initialize_replication_config().await.unwrap();
    let (inode, lease) = service
        .create(create_request("create", b"source".to_vec()))
        .await
        .unwrap();
    let sync = SyncInodeMetadata {
        operation_id: OperationId::new("sync"),
        inode_id: inode.inode_id.clone(),
        write_lease: lease,
        expected_inode_revision: inode.revision,
        expected_head_version: None,
        metadata_delta: CommitMetadataDelta {
            mode: CommitMetadataMode::Full,
            mtime_unix_ms: Some(2),
            ctime_unix_ms: Some(2),
            kill_suidgid: false,
        },
    };
    let updated = service
        .sync_inode_metadata("node-a".into(), sync)
        .await
        .expect("metadata sync must refresh its lease observation");
    assert_eq!(updated.attributes.mtime_unix_ms, 2);
}

async fn pending_commit(
    mode: Interference,
    count: usize,
) -> (DfsService, Arc<LeaseInterferenceStore>, CommitFileVersion) {
    let store = Arc::new(LeaseInterferenceStore {
        inner: Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        ),
        remaining: AtomicUsize::new(count),
        mode,
    });
    let service =
        DfsService::with_replication_config(store.clone(), ReplicationConfig::local_single_copy());
    service.initialize_replication_config().await.unwrap();
    let (inode, lease) = service
        .create(create_request("create", b"source".to_vec()))
        .await
        .unwrap();
    let layout = LayoutRoot {
        id: LayoutRootId::new("layout"),
        file_length: 0,
        inline_extents: vec![],
    };
    let commit = CommitFileVersion {
        gc_fence: None,
        operation_id: OperationId::new("commit"),
        inode_id: inode.inode_id.clone(),
        write_lease: lease,
        expected_inode_revision: inode.revision,
        expected_head_version: None,
        file_version: FileVersion {
            id: FileVersionId::new("version"),
            inode_id: inode.inode_id.clone(),
            parent_version: None,
            length: 0,
            layout_root: layout.id.clone(),
            created_at_unix_ms: 1,
        },
        layout_root: layout,
        chunk_receipts: vec![],
        clone_source: None,
        metadata_delta: CommitMetadataDelta {
            mode: CommitMetadataMode::DataOnly,
            mtime_unix_ms: None,
            ctime_unix_ms: None,
            kill_suidgid: false,
        },
    };

    (service, store, commit)
}

#[tokio::test]
async fn lease_fencing_and_concurrent_head_change_remain_terminal() {
    for mode in [Interference::Fence, Interference::Head] {
        let (service, _, commit) = pending_commit(mode, 1).await;
        let error = service
            .commit_file_version("node-a".into(), commit)
            .await
            .unwrap_err();
        assert_eq!(error.code(), afs_error::META_DFS_CONFLICT);
    }
}

#[tokio::test]
async fn retry_preserves_concurrent_unlink_and_permission_changes() {
    let (service, _, commit) = pending_commit(Interference::Metadata, 1).await;
    let result = service
        .commit_file_version("node-a".into(), commit)
        .await
        .unwrap();
    assert_eq!(result.attributes.nlink, 0);
    assert_eq!(result.attributes.mode, 0o600);
}

#[tokio::test]
async fn sustained_contention_is_retryable_with_the_same_operation() {
    let (service, store, commit) = pending_commit(Interference::Renew, 128).await;
    let error = service
        .commit_file_version("node-a".into(), commit.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code(), afs_error::IO_UNAVAILABLE);
    store.remaining.store(0, Ordering::SeqCst);
    let result = service
        .commit_file_version("node-a".into(), commit.clone())
        .await
        .unwrap();
    assert_eq!(result.head_version, Some(commit.file_version.id));
}

fn unlink_request(operation: &str, name: &[u8]) -> afs::dfs::UnlinkRequest {
    afs::dfs::UnlinkRequest {
        caller_id: "node-a".into(),
        operation_id: OperationId::new(operation),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: InodeId::new("1"),
        name: name.to_vec(),
        caller: afs::dfs::CallerContext {
            uid: 0,
            gid: 0,
            supplementary_gids: vec![],
        },
    }
}

async fn unlink_fixture(mode: Interference) -> (DfsService, Arc<LeaseInterferenceStore>, InodeId) {
    let store = Arc::new(LeaseInterferenceStore {
        inner: Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        ),
        remaining: AtomicUsize::new(1),
        mode,
    });
    let service =
        DfsService::with_replication_config(store.clone(), ReplicationConfig::local_single_copy());
    service.initialize_replication_config().await.unwrap();
    let (inode, _) = service
        .create(create_request("create-victim", b"memory.live".to_vec()))
        .await
        .unwrap();
    service
        .create(create_request("create-sibling", b"sibling".to_vec()))
        .await
        .unwrap();
    (service, store, inode.inode_id)
}

#[tokio::test]
async fn unlink_retries_metadata_writeback_without_losing_updates_or_replaying_twice() {
    let (service, store, inode_id) = unlink_fixture(Interference::UnlinkMetadata).await;
    let request = unlink_request("unlink-victim", b"memory.live");
    let result = service
        .unlink(request.clone())
        .await
        .expect("unlink must revalidate after concurrent metadata writeback");
    assert_eq!(result.attributes.nlink, 0);
    assert_eq!(result.attributes.mtime_unix_ms, 7000);
    assert_eq!(service.unlink(request).await.unwrap(), result);
    assert!(
        service
            .lookup(
                NamespaceId::new("default"),
                InodeId::new("1"),
                b"memory.live".to_vec()
            )
            .await
            .unwrap()
            .is_none()
    );
    let Some(MetaEntity::DfsInode(stored)) = store
        .inner
        .read(MetaRead::DfsInode(inode_id))
        .await
        .unwrap()
        .entity
    else {
        panic!("unlinked inode remains for readers")
    };
    assert_eq!(stored, result);
}

#[tokio::test]
async fn unlink_retries_same_parent_sibling_deletion() {
    let (service, _, _) = unlink_fixture(Interference::UnlinkSibling).await;
    service
        .unlink(unlink_request("unlink-victim", b"memory.live"))
        .await
        .expect("different names sharing a parent must not produce EBUSY");
    for name in [b"memory.live".as_slice(), b"sibling".as_slice()] {
        assert!(
            service
                .lookup(
                    NamespaceId::new("default"),
                    InodeId::new("1"),
                    name.to_vec()
                )
                .await
                .unwrap()
                .is_none()
        );
    }
}

fn root_caller() -> afs::dfs::CallerContext {
    afs::dfs::CallerContext {
        uid: 0,
        gid: 0,
        supplementary_gids: vec![],
    }
}

#[tokio::test]
async fn unlink_repeated_contention_is_bounded_and_retryable() {
    let (service, store, _) = unlink_fixture(Interference::UnlinkMetadata).await;
    store.remaining.store(128, Ordering::SeqCst);
    let error = service
        .unlink(unlink_request("contended", b"memory.live"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), afs_error::ErrorKind::Aborted);
    assert_eq!(afs::error::errno(&error), libc::EAGAIN);
    assert!(store.remaining.load(Ordering::SeqCst) > 0);
    assert!(
        service
            .lookup(
                NamespaceId::new("default"),
                InodeId::new("1"),
                b"memory.live".to_vec()
            )
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn unlink_retry_rechecks_parent_permissions() {
    let (service, _, _) = unlink_fixture(Interference::UnlinkParentPermissions).await;
    let parent = service.get_inode(InodeId::new("1")).await.unwrap();
    service
        .set_inode_attributes(afs::dfs::SetInodeAttrRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("parent-writable"),
            caller: root_caller(),
            inode_id: parent.inode_id,
            expected_inode_revision: parent.revision,
            update: afs::dfs::InodeAttrUpdate {
                mode: Some(0o777),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    let mut request = unlink_request("permission-race", b"memory.live");
    request.caller.uid = 1000;
    request.caller.gid = 1000;
    assert_eq!(
        service.unlink(request).await.unwrap_err().kind(),
        afs_error::ErrorKind::PermissionDenied
    );
    assert!(
        service
            .lookup(
                NamespaceId::new("default"),
                InodeId::new("1"),
                b"memory.live".to_vec()
            )
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn unlink_retry_does_not_remove_a_replacement_inode() {
    let (service, _, original) = unlink_fixture(Interference::UnlinkReplacement).await;
    let error = service
        .unlink(unlink_request("replacement-race", b"memory.live"))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), afs_error::ErrorKind::Aborted);
    assert_eq!(afs::error::errno(&error), libc::EAGAIN);
    let replacement = service
        .lookup(
            NamespaceId::new("default"),
            InodeId::new("1"),
            b"memory.live".to_vec(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_ne!(replacement.inode_id, original);
    assert_eq!(replacement.attributes.nlink, 1);
}

#[tokio::test]
async fn unlink_replays_committed_result_after_lost_reply_and_rejects_changed_request() {
    let (service, _, _) = unlink_fixture(Interference::UnlinkReplyLost).await;
    let request = unlink_request("lost-reply", b"memory.live");
    let removed = service.unlink(request.clone()).await.unwrap();
    assert_eq!(removed.attributes.nlink, 0);
    assert_eq!(service.unlink(request).await.unwrap(), removed);
    assert!(
        service
            .unlink(unlink_request("lost-reply", b"sibling"))
            .await
            .is_err()
    );
    assert!(
        service
            .lookup(
                NamespaceId::new("default"),
                InodeId::new("1"),
                b"sibling".to_vec()
            )
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn rmdir_retry_rechecks_new_children() {
    let (service, _, _) = unlink_fixture(Interference::RmdirNewChild).await;
    let directory = service
        .mkdir(afs::dfs::MkdirRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("mkdir"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"directory".to_vec(),
            attributes: attrs(),
            caller: root_caller(),
        })
        .await
        .unwrap();
    let request = afs::dfs::RmdirRequest {
        caller_id: "node-a".into(),
        operation_id: OperationId::new("rmdir"),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: InodeId::new("1"),
        name: b"directory".to_vec(),
        caller: root_caller(),
    };
    assert_eq!(
        service.rmdir(request).await.unwrap_err().code(),
        afs_error::IO_DIRECTORY_NOT_EMPTY
    );
    assert!(
        service
            .lookup(
                NamespaceId::new("default"),
                directory.inode_id,
                b"new-child".to_vec()
            )
            .await
            .unwrap()
            .is_some()
    );
}
