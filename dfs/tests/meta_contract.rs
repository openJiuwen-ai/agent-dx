use afs::dfs::{
    ChunkId, ContentDigest, DigestAlgorithm, LocalCopyPolicy, OperationId, ReplicationConfig,
    ValidateReplicaWriteRequest,
};
use afs::{
    meta::{
        Meta,
        dfs::DfsService,
        rpc,
        store::{
            MetaEntity, MetaRead, MetaStore, OperationResult, RequestKey,
            RootRight as StoreRootRight, Store, StoreBackend, StoreOperation,
            local_file::LocalFileBackend, memory::MemoryBackend,
        },
    },
    runtime::Observability,
};
use afs_protocol::meta::{
    AbortRootRequest, AckRevocationRequest, AcquireRootRequest, ActivateRootReply,
    ActivateRootRequest, CommitFileVersionRequest, DfsCallerContext, DfsChunkReceipt,
    DfsCommitMetadataDelta, DfsCommitMetadataMode, DfsCreateRequest, DfsExtent, DfsFileVersion,
    DfsGetXattrRequest, DfsInodeAttributeUpdate, DfsInodeAttributes, DfsLayoutRoot, DfsLinkRequest,
    DfsListXattrRequest, DfsLookupRequest, DfsMkdirRequest, DfsReadDirRequest, DfsReadLinkRequest,
    DfsRemoveXattrRequest, DfsRenameMode, DfsRenameRequest, DfsReplicaAck, DfsRmdirRequest,
    DfsSetInodeAttributesRequest, DfsSetXattrRequest, DfsStorageDevice, DfsSymlinkRequest,
    DfsUnlinkRequest, DfsWriteLease, DfsXattrSetMode, ListOwnerRootsRequest, LookupNodeRequest,
    LookupRootReply, LookupRootRequest, NodeDescriptor, NodeEndpoint, OpenDfsWriteRequest,
    PollRootCommandBatchRequest, PresentedRootAccess, RecoverRootRequest, RegisterNodeRequest,
    ReserveRootReply, ReserveRootRequest, RootAccess, RootCommand, RootCommandType, RootLocation,
    RootReservation, RootRight, ValidateRootAccessRequest, WatchRootCommandsRequest,
    dfs_meta_server::DfsMeta as DfsMetaService, meta_server::Meta as MetaService,
    owner_roots_server::OwnerRoots as OwnerRootsService,
};
use std::sync::{Arc, Mutex, Weak};
use tonic::{Code, Request};

async fn renewal_fixture(lease_seconds: u64) -> (DfsService, afs::dfs::WriteLease) {
    use afs::dfs::{InodeAttributes, InodeId, NamespaceId};
    use afs::meta::dfs::CreateFileRequest;
    let (_, dfs) = memory_meta_and_dfs_service(ReplicationConfig::local_single_copy()).await;
    let (_, lease) = dfs
        .create(CreateFileRequest {
            caller_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            operation_id: OperationId::new("create-renewal"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"renewal.txt".to_vec(),
            attributes: InodeAttributes {
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            },
            lease_seconds,
        })
        .await
        .unwrap();
    (dfs, lease)
}

async fn lock_resolver_fixture(
    lease_seconds: u64,
) -> (
    Arc<Store>,
    DfsService,
    afs::dfs::WriteLease,
    Arc<LostCommitResultBackend>,
) {
    use afs::dfs::{InodeAttributes, InodeId, NamespaceId};
    let backend = Arc::new(LostCommitResultBackend::new(Arc::new(
        MemoryBackend::default(),
    )));
    let store = Arc::new(Store::open(backend.clone()).await.unwrap());
    let dfs = DfsService::new(store.clone());
    let (_, lease) = dfs
        .create(afs::meta::dfs::CreateFileRequest {
            caller_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            operation_id: OperationId::new("create-lock-resolver"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"resolver.bin".to_vec(),
            attributes: InodeAttributes {
                mode: 0o640,
                uid: 1000,
                gid: 1000,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            },
            lease_seconds,
        })
        .await
        .unwrap();
    (store, dfs, lease, backend)
}

#[tokio::test]
async fn dfs_lock_resolver_live_local_authority_does_not_commit_or_extend_expiry() {
    let (store, dfs, original, backend) = lock_resolver_fixture(30).await;
    let before = store
        .read(MetaRead::DfsWriteLease(original.inode_id.clone()))
        .await
        .unwrap();
    let commits = backend.observed_commit().0;
    for index in 0..64 {
        let id = format!("resolve-live-{index}");
        let (inode, lease) = dfs
            .resolve_lock_authority(
                "node-a".into(),
                "session-a".into(),
                OperationId::new(id.clone()),
                original.inode_id.clone(),
                90,
            )
            .await
            .unwrap();
        assert_eq!(inode.inode_id, original.inode_id);
        assert_eq!(lease, original, "lock queries must not extend a live lease");
        assert!(
            store
                .read(MetaRead::RequestOutcome(RequestKey::new("node-a", id)))
                .await
                .unwrap()
                .request_outcome
                .is_none(),
            "live lock resolution must not persist an outcome"
        );
    }
    assert_eq!(backend.observed_commit().0, commits);
    assert_eq!(
        store
            .read(MetaRead::DfsWriteLease(original.inode_id))
            .await
            .unwrap()
            .revision,
        before.revision
    );
}

#[tokio::test]
async fn dfs_write_authority_live_local_does_not_commit_or_extend_expiry() {
    let (store, dfs, original, backend) = lock_resolver_fixture(30).await;
    let before = store
        .read(MetaRead::DfsWriteLease(original.inode_id.clone()))
        .await
        .unwrap();
    let commits = backend.observed_commit().0;
    for index in 0..16 {
        let id = format!("resolve-write-live-{index}");
        let (inode, lease) = dfs
            .resolve_write_authority(
                "node-a".into(),
                "session-a".into(),
                OperationId::new(id.clone()),
                original.inode_id.clone(),
                90,
            )
            .await
            .unwrap();
        assert_eq!(inode.inode_id, original.inode_id);
        assert_eq!(
            lease, original,
            "write-authority reads must not extend a live lease"
        );
        assert!(
            store
                .read(MetaRead::RequestOutcome(RequestKey::new("node-a", id)))
                .await
                .unwrap()
                .request_outcome
                .is_none(),
            "live write-authority resolution must not persist an outcome"
        );
    }
    assert_eq!(backend.observed_commit().0, commits);
    assert_eq!(
        store
            .read(MetaRead::DfsWriteLease(original.inode_id))
            .await
            .unwrap()
            .revision,
        before.revision
    );
}

#[tokio::test]
async fn dfs_write_authority_rpc_validates_fields_and_uses_new_endpoint() {
    use afs_protocol::meta::ResolveDfsWriteAuthorityRequest;
    let (store, _, original, _) = lock_resolver_fixture(30).await;
    let request = ResolveDfsWriteAuthorityRequest {
        caller_id: "node-b".into(),
        owner_session_id: "session-b".into(),
        operation_id: "resolve-write-rpc".into(),
        inode_id: original.inode_id.0.clone(),
        lease_seconds: 30,
    };
    let plain = rpc::DfsMetaRpc(Arc::new(Meta::with_store(
        "test".into(),
        Observability::new().unwrap(),
        store.clone(),
    )));
    let reply = plain
        .resolve_write_authority(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.write_lease.unwrap().owner_node_id, "node-a");
    for field in 0..4 {
        let mut missing = request.clone();
        match field {
            0 => missing.caller_id.clear(),
            1 => missing.owner_session_id.clear(),
            2 => missing.operation_id.clear(),
            _ => missing.inode_id.clear(),
        }
        assert_eq!(
            plain
                .resolve_write_authority(Request::new(missing))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    let secured = rpc::DfsMetaRpc(Arc::new(Meta::with_store_and_peer_identity(
        "test".into(),
        Observability::new().unwrap(),
        store,
        Default::default(),
        true,
    )));
    assert_eq!(
        secured
            .resolve_write_authority(Request::new(request))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
}

#[tokio::test]
async fn dfs_write_authority_expired_lease_acquires_and_replays_exact_result() {
    let (store, dfs, original, backend) = lock_resolver_fixture(1).await;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let operation = OperationId::new("resolve-write-acquire");
    let (_, acquired) = dfs
        .resolve_write_authority(
            "node-b".into(),
            "session-b".into(),
            operation.clone(),
            original.inode_id.clone(),
            30,
        )
        .await
        .unwrap();
    assert_eq!(acquired.owner_node_id, "node-b");
    assert_eq!(acquired.owner_session_id, "session-b");
    assert!(acquired.lease_epoch > original.lease_epoch);

    let (_, extended) = dfs
        .open_write(
            "node-b".into(),
            "session-b".into(),
            OperationId::new("extend-after-write-resolve"),
            original.inode_id.clone(),
            90,
        )
        .await
        .unwrap();
    assert!(extended.expires_at_unix_ms > acquired.expires_at_unix_ms);

    let commits = backend.observed_commit().0;
    let (_, replay) = dfs
        .resolve_write_authority(
            "node-b".into(),
            "session-b".into(),
            operation,
            original.inode_id.clone(),
            30,
        )
        .await
        .unwrap();
    assert_eq!(
        replay, acquired,
        "an acquired write authority must replay its exact original result"
    );
    assert_eq!(backend.observed_commit().0, commits);
    let outcome = store
        .read(MetaRead::RequestOutcome(RequestKey::new(
            "node-b",
            "resolve-write-acquire",
        )))
        .await
        .unwrap()
        .request_outcome
        .unwrap();
    assert_eq!(outcome.operation, StoreOperation::DfsAcquireWriteLease);
    assert!(
        dfs.resolve_write_authority(
            "node-b".into(),
            "different-session".into(),
            OperationId::new("resolve-write-acquire"),
            original.inode_id,
            30,
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn dfs_write_authority_lost_acquire_result_replays_after_store_reopen() {
    let (_store, dfs, original, backend) = lock_resolver_fixture(1).await;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let operation = OperationId::new("resolve-write-lost-acquire");
    backend.arm_once();

    let error = dfs
        .resolve_write_authority(
            "node-b".into(),
            "session-b".into(),
            operation.clone(),
            original.inode_id.clone(),
            30,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), afs_error::IO_UNAVAILABLE);
    let (commits_after_lost_result, native_version, native_digest) = backend.observed_commit();

    let reopened_backend: Arc<dyn StoreBackend> = backend.clone();
    let reopened_store = Arc::new(Store::open(reopened_backend).await.unwrap());
    let reopened = DfsService::new(reopened_store.clone());
    let (_, replay) = reopened
        .resolve_write_authority(
            "node-b".into(),
            "session-b".into(),
            operation,
            original.inode_id.clone(),
            30,
        )
        .await
        .unwrap();

    assert_eq!(replay.owner_node_id, "node-b");
    assert_eq!(replay.owner_session_id, "session-b");
    assert_eq!(replay.inode_id, original.inode_id);
    assert!(replay.lease_epoch > original.lease_epoch);
    assert_eq!(
        backend.observed_commit(),
        (commits_after_lost_result, native_version, native_digest),
        "exact lost acquire replay must not add another durable commit"
    );
    let outcome = reopened_store
        .read(MetaRead::RequestOutcome(RequestKey::new(
            "node-b",
            "resolve-write-lost-acquire",
        )))
        .await
        .unwrap()
        .request_outcome
        .unwrap();
    assert_eq!(outcome.operation, StoreOperation::DfsAcquireWriteLease);
    match outcome.result {
        OperationResult::DfsWriteLease(lease) => assert_eq!(lease, replay),
        other => panic!("unexpected replayed write authority outcome: {other:?}"),
    }
}

#[tokio::test]
async fn dfs_lock_resolver_acquire_reply_loss_replays_before_live_resolution() {
    let (store, dfs, original, backend) = lock_resolver_fixture(1).await;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let operation = OperationId::new("resolve-acquire");
    let (_, acquired) = dfs
        .resolve_lock_authority(
            "node-b".into(),
            "session-b".into(),
            operation.clone(),
            original.inode_id.clone(),
            30,
        )
        .await
        .unwrap();
    assert_eq!(acquired.owner_node_id, "node-b");
    assert!(acquired.lease_epoch > original.lease_epoch);
    let (_, extended) = dfs
        .open_write(
            "node-b".into(),
            "session-b".into(),
            OperationId::new("extend-after-resolve"),
            original.inode_id.clone(),
            90,
        )
        .await
        .unwrap();
    assert!(extended.expires_at_unix_ms > acquired.expires_at_unix_ms);
    let commits = backend.observed_commit().0;
    let (_, replay) = dfs
        .resolve_lock_authority(
            "node-b".into(),
            "session-b".into(),
            operation,
            original.inode_id.clone(),
            30,
        )
        .await
        .unwrap();
    assert_eq!(
        replay, acquired,
        "an acquired authority must replay its exact original result"
    );
    assert_eq!(backend.observed_commit().0, commits);
    let outcome = store
        .read(MetaRead::RequestOutcome(RequestKey::new(
            "node-b",
            "resolve-acquire",
        )))
        .await
        .unwrap()
        .request_outcome
        .unwrap();
    assert_eq!(outcome.operation, StoreOperation::DfsAcquireWriteLease);
    assert!(
        dfs.resolve_lock_authority(
            "node-b".into(),
            "different-session".into(),
            OperationId::new("resolve-acquire"),
            original.inode_id,
            30
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn dfs_lock_resolver_routes_live_foreign_owner_without_mutation() {
    let (store, dfs, original, backend) = lock_resolver_fixture(30).await;
    let commits = backend.observed_commit().0;
    let (_, resolved) = dfs
        .resolve_lock_authority(
            "node-b".into(),
            "session-b".into(),
            OperationId::new("resolve-foreign"),
            original.inode_id.clone(),
            90,
        )
        .await
        .unwrap();
    assert_eq!(resolved, original);
    assert_eq!(backend.observed_commit().0, commits);
    assert!(
        store
            .read(MetaRead::RequestOutcome(RequestKey::new(
                "node-b",
                "resolve-foreign"
            )))
            .await
            .unwrap()
            .request_outcome
            .is_none()
    );
}

#[tokio::test]
async fn dfs_lock_resolver_missing_lease_acquires_and_rejects_operation_reuse() {
    use afs::meta::store::{MetaKey, MetaTxn, RequestOutcome, TxnCondition, TxnMutation};
    let (store, dfs, original, backend) = lock_resolver_fixture(30).await;
    let key = RequestKey::new("fixture", "remove-lease");
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsAcquireWriteLease);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations.extend([
        TxnMutation::Delete(MetaKey::DfsWriteLease(original.inode_id.clone())),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::DfsAcquireWriteLease,
            result: OperationResult::Empty,
        }),
    ]);
    store.compare_and_commit(txn).await.unwrap();
    let commits = backend.observed_commit().0;
    let (_, acquired) = dfs
        .resolve_lock_authority(
            "node-b".into(),
            "session-b".into(),
            OperationId::new("resolve-missing"),
            original.inode_id.clone(),
            30,
        )
        .await
        .unwrap();
    assert_eq!(acquired.owner_node_id, "node-b");
    assert_eq!(acquired.owner_session_id, "session-b");
    assert_eq!(acquired.lease_epoch, 1);
    assert_eq!(backend.observed_commit().0, commits + 1);
    assert!(
        dfs.resolve_lock_authority(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("create-lock-resolver"),
            original.inode_id,
            30
        )
        .await
        .is_err()
    );
    assert_eq!(backend.observed_commit().0, commits + 1);
}

#[tokio::test]
async fn dfs_lock_resolver_poisoned_store_cannot_return_cached_authority() {
    let (_, dfs, original, backend) = lock_resolver_fixture(30).await;
    backend.arm_once();
    assert!(
        dfs.open_write(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("poison-lock-store"),
            original.inode_id.clone(),
            60
        )
        .await
        .is_err()
    );
    let commits = backend.observed_commit().0;
    let error = dfs
        .resolve_lock_authority(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("resolve-poison"),
            original.inode_id,
            30,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), afs_error::IO_UNAVAILABLE);
    assert_eq!(backend.observed_commit().0, commits);
}

#[tokio::test]
async fn dfs_lock_resolver_inode_and_lease_share_one_acknowledged_view() {
    use afs::meta::store::{MetaTxn, RequestOutcome, TxnCondition, TxnMutation};
    let (store, dfs, original, _) = lock_resolver_fixture(30).await;
    let mut inode = dfs.get_inode(original.inode_id.clone()).await.unwrap();
    let mut lease = original.clone();
    let writer_store = store.clone();
    let writer = tokio::spawn(async move {
        for index in 1..=32 {
            inode.attributes.uid = 1000 + index;
            inode.revision += 1;
            lease.lease_epoch = 1 + u64::from(index);
            let request = RequestKey::new("fixture", format!("advance-pair-{index}"));
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsSyncInodeMetadata);
            txn.conditions
                .push(TxnCondition::RequestAbsent(request.clone()));
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
                TxnMutation::Put(MetaEntity::DfsWriteLease(lease.clone())),
                TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request,
                    operation: StoreOperation::DfsSyncInodeMetadata,
                    result: OperationResult::DfsInode(inode.clone()),
                }),
            ]);
            writer_store.compare_and_commit(txn).await.unwrap();
            tokio::task::yield_now().await;
        }
    });
    for index in 0..256 {
        let (inode, lease) = dfs
            .resolve_lock_authority(
                "node-a".into(),
                "session-a".into(),
                OperationId::new(format!("resolve-pair-{index}")),
                original.inode_id.clone(),
                30,
            )
            .await
            .unwrap();
        assert_eq!(
            u64::from(inode.attributes.uid),
            lease.lease_epoch + 999,
            "inode metadata and authority must not mix acknowledged revisions"
        );
        tokio::task::yield_now().await;
    }
    writer.await.unwrap();
    let (_, lease) = dfs
        .resolve_lock_authority(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("resolve-final-pair"),
            original.inode_id,
            30,
        )
        .await
        .unwrap();
    assert_eq!(
        lease.lease_epoch, 33,
        "the next resolution must observe the new epoch"
    );
}

#[tokio::test]
async fn dfs_lock_resolver_rpc_validates_fields_and_requires_enforced_peer_identity() {
    use afs_protocol::meta::ResolveDfsLockAuthorityRequest;
    let (store, _, original, _) = lock_resolver_fixture(30).await;
    let request = ResolveDfsLockAuthorityRequest {
        caller_id: "node-b".into(),
        owner_session_id: "session-b".into(),
        operation_id: "resolve-rpc".into(),
        inode_id: original.inode_id.0.clone(),
        lease_seconds: 30,
    };
    let plain = rpc::DfsMetaRpc(Arc::new(Meta::with_store(
        "test".into(),
        Observability::new().unwrap(),
        store.clone(),
    )));
    let reply = plain
        .resolve_lock_authority(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(reply.write_lease.unwrap().owner_node_id, "node-a");
    for field in 0..4 {
        let mut missing = request.clone();
        match field {
            0 => missing.caller_id.clear(),
            1 => missing.owner_session_id.clear(),
            2 => missing.operation_id.clear(),
            _ => missing.inode_id.clear(),
        }
        assert_eq!(
            plain
                .resolve_lock_authority(Request::new(missing))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    let secured = rpc::DfsMetaRpc(Arc::new(Meta::with_store_and_peer_identity(
        "test".into(),
        Observability::new().unwrap(),
        store,
        Default::default(),
        true,
    )));
    assert_eq!(
        secured
            .resolve_lock_authority(Request::new(request))
            .await
            .unwrap_err()
            .code(),
        Code::PermissionDenied
    );
}

#[tokio::test]
async fn dfs_renewal_accepts_same_epoch_expiry_drift_and_replays_exact_result() {
    let (dfs, original) = renewal_fixture(30).await;
    let (_, reopened) = dfs
        .open_write(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("reopen"),
            original.inode_id.clone(),
            60,
        )
        .await
        .unwrap();
    assert_ne!(original.expires_at_unix_ms, reopened.expires_at_unix_ms);
    let renewed = dfs
        .renew_write_lease(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("renew-original"),
            original.clone(),
            30,
        )
        .await
        .expect("a same-epoch reopen must not fence the lock renewal snapshot");
    assert_eq!(renewed.lease_epoch, original.lease_epoch);
    assert!(renewed.expires_at_unix_ms >= reopened.expires_at_unix_ms);
    let mut stale_hint = original.clone();
    stale_hint.expires_at_unix_ms = 0;
    dfs.renew_write_lease(
        "node-a".into(),
        "session-a".into(),
        OperationId::new("renew-old-hint"),
        stale_hint,
        90,
    )
    .await
    .expect("stored live authority, rather than a copied expiry hint, controls renewal");
    let replay = dfs
        .renew_write_lease(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("renew-original"),
            original.clone(),
            30,
        )
        .await
        .unwrap();
    assert_eq!(replay, renewed);
    let mut fenced = original;
    fenced.lease_epoch += 1;
    assert!(
        dfs.renew_write_lease(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("renew-wrong-epoch"),
            fenced,
            30
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn dfs_same_owner_reopen_cannot_shorten_live_lease_expiry() {
    let (dfs, original) = renewal_fixture(90).await;
    let (_, reopened) = dfs
        .open_write(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("shorter-reopen"),
            original.inode_id.clone(),
            30,
        )
        .await
        .unwrap();
    assert_eq!(reopened.lease_epoch, original.lease_epoch);
    assert!(reopened.expires_at_unix_ms >= original.expires_at_unix_ms);
    let renewed = dfs
        .renew_write_lease(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("shorter-renew"),
            original,
            30,
        )
        .await
        .unwrap();
    assert!(renewed.expires_at_unix_ms >= reopened.expires_at_unix_ms);
}

#[tokio::test]
async fn dfs_renewal_concurrent_same_epoch_callers_preserve_authority() {
    let (dfs, original) = renewal_fixture(30).await;
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..8 {
        let dfs = dfs.clone();
        let original = original.clone();
        tasks.spawn(async move {
            dfs.renew_write_lease(
                "node-a".into(),
                "session-a".into(),
                OperationId::new(format!("renew-{index}")),
                original,
                60 + index,
            )
            .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        let lease = result
            .unwrap()
            .expect("same-epoch expiry CAS races must be retried");
        assert_eq!(lease.lease_epoch, original.lease_epoch);
        assert_eq!(lease.owner_session_id, original.owner_session_id);
    }
}

#[tokio::test]
async fn dfs_renewal_cannot_resurrect_expired_or_reassigned_authority() {
    let (dfs, original) = renewal_fixture(1).await;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(
        dfs.renew_write_lease(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("renew-expired"),
            original.clone(),
            30
        )
        .await
        .is_err()
    );
    let (_, next) = dfs
        .open_write(
            "node-b".into(),
            "session-b".into(),
            OperationId::new("takeover"),
            original.inode_id.clone(),
            30,
        )
        .await
        .unwrap();
    assert!(next.lease_epoch > original.lease_epoch);
    assert!(
        dfs.renew_write_lease(
            "node-a".into(),
            "session-a".into(),
            OperationId::new("renew-fenced"),
            original,
            30
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn node_rpc_store_local_file_recovers_committed_session_and_reply() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalFileBackend::open(dir.path()).unwrap());
    let store = Arc::new(Store::open(backend.clone()).await.unwrap());
    let meta = Arc::new(Meta::with_store(
        "meta-test".into(),
        Observability::new().unwrap(),
        store.clone(),
    ));
    let request = RegisterNodeRequest {
        request_id: "register-node-a".into(),
        node: Some(NodeDescriptor {
            node_id: "node-a".into(),
            endpoint: Some(NodeEndpoint {
                grpc_addr: "http://node-a:7400".into(),
                data_addr: "http://node-a:7500".into(),
                rest_addr: "http://node-a:7600".into(),
            }),
            labels: Default::default(),
            capabilities: Vec::new(),
            session_id: "session-a".into(),
            storage_devices: Vec::new(),
        }),
        lease_seconds: 30,
    };
    let registered = rpc::MetaRpc(meta)
        .register_node(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(registered.lease_epoch, 1);
    drop(store);

    let reopened = Arc::new(Meta::with_store(
        "meta-restarted".into(),
        Observability::new().unwrap(),
        Arc::new(Store::open(backend).await.unwrap()),
    ));
    let found = rpc::MetaRpc(reopened.clone())
        .lookup_node(Request::new(LookupNodeRequest {
            request_id: "lookup-node-a".into(),
            node_id: "node-a".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(found.found);
    assert_eq!(found.lease_epoch, 1);
    let replay = rpc::MetaRpc(reopened)
        .register_node(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replay.lease_epoch, 1);
}

#[test]
fn meta_services_share_one_package_but_remain_separate_routes() {
    use afs_protocol::meta::{
        dfs_meta_server::DfsMetaServer, meta_server::MetaServer,
        owner_roots_server::OwnerRootsServer,
    };
    use tonic::server::NamedService;

    assert_eq!(
        <MetaServer<rpc::MetaRpc> as NamedService>::NAME,
        "afs.meta.v1.Meta"
    );
    assert_eq!(
        <OwnerRootsServer<rpc::OwnerRootsRpc> as NamedService>::NAME,
        "afs.meta.v1.OwnerRoots"
    );
    assert_eq!(
        <DfsMetaServer<rpc::DfsMetaRpc> as NamedService>::NAME,
        "afs.meta.v1.DfsMeta"
    );
}

#[test]
fn both_edges_can_reuse_the_same_meta_state() {
    let meta = Meta::new("meta-test".into(), Observability::new().unwrap());
    assert_eq!(meta.ping("node-a").unwrap(), "pong from meta-test");
    assert!(meta.ping(&"x".repeat(129)).is_err());
    let text = afs_metrics::encode_text(&meta.observability.registry).unwrap();
    assert!(text.contains("result=\"ok\"} 1"));
    assert!(text.contains("result=\"error\"} 1"));
}

#[tokio::test]
async fn contract_rpc_does_not_issue_fake_grants_without_store() {
    let meta = Arc::new(Meta::new("meta-test".into(), Observability::new().unwrap()));

    let register = rpc::MetaRpc(meta.clone())
        .register_node(Request::new(RegisterNodeRequest {
            request_id: "req-register".into(),
            node: Some(NodeDescriptor {
                node_id: "node-a".into(),
                endpoint: Some(NodeEndpoint {
                    grpc_addr: "http://node-a:7400".into(),
                    data_addr: "http://node-a:7500".into(),
                    rest_addr: "http://node-a:7600".into(),
                }),
                labels: Default::default(),
                capabilities: Vec::new(),
                session_id: "session-a".into(),
                storage_devices: Vec::new(),
            }),
            lease_seconds: 30,
        }))
        .await
        .unwrap_err();
    assert_eq!(register.code(), Code::Unimplemented);

    let recover = rpc::OwnerRootsRpc(meta.clone())
        .recover_root(Request::new(RecoverRootRequest {
            request_id: "recover-a".into(),
            root_id: "workspace-a".into(),
            expected_root_epoch: 7,
            home_node_id: "node-a".into(),
            home_session_id: "session-a-new".into(),
            local_prepare_id: "local-a".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(recover.code(), Code::Unimplemented);

    let validate = rpc::OwnerRootsRpc(meta.clone())
        .validate_root_access(Request::new(ValidateRootAccessRequest {
            request_id: "validate-b".into(),
            presented_access: Some(PresentedRootAccess {
                root_id: "workspace-a".into(),
                root_epoch: 1,
                home_node_id: "node-a".into(),
                holder_node_id: "node-b".into(),
                session_id: "session-b".into(),
                access_generation: 1,
                fencing_token: "fence-b".into(),
                home_session_id: "session-a".into(),
            }),
            observed_peer_node_id: "node-b".into(),
            validator_home_node_id: "node-a".into(),
            validator_home_session_id: "session-a".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(validate.code(), Code::Unimplemented);

    let reserve_root = rpc::OwnerRootsRpc(meta.clone())
        .reserve_root(Request::new(ReserveRootRequest {
            request_id: "req-root".into(),
            root_id: "workspace-a".into(),
            preferred_home_node_id: "node-a".into(),
            session_id: "session-a".into(),
            create_intent_id: "mkdir-a".into(),
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(reserve_root.code(), Code::Unimplemented);

    let dfs_rpc = rpc::DfsMetaRpc(meta);
    let lookup = dfs_rpc
        .lookup(Request::new(DfsLookupRequest {
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"missing".to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(lookup.code(), Code::Unimplemented);
}

#[tokio::test]
async fn owner_authority_reserve_activate_acquire_validate_and_recover() {
    let store = Arc::new(
        Store::open(Arc::new(MemoryBackend::default()))
            .await
            .unwrap(),
    );
    let meta = Arc::new(Meta::with_store(
        "meta-test".into(),
        Observability::new().unwrap(),
        store.clone(),
    ));
    let meta_rpc = rpc::MetaRpc(meta.clone());
    let owner_rpc = rpc::OwnerRootsRpc(meta);

    for (node_id, session_id) in [("node-a", "session-a"), ("node-b", "session-b")] {
        let reply = meta_rpc
            .register_node(Request::new(RegisterNodeRequest {
                request_id: format!("register-{node_id}"),
                node: Some(NodeDescriptor {
                    node_id: node_id.into(),
                    endpoint: Some(NodeEndpoint {
                        grpc_addr: format!("http://{node_id}:7400"),
                        data_addr: format!("http://{node_id}:7500"),
                        rest_addr: format!("http://{node_id}:7600"),
                    }),
                    labels: Default::default(),
                    capabilities: vec!["ownerfs".into()],
                    session_id: session_id.into(),
                    storage_devices: Vec::new(),
                }),
                lease_seconds: 30,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.node_id, node_id);
        assert_eq!(reply.lease_epoch, 1);
    }

    let reservation = owner_rpc
        .reserve_root(Request::new(ReserveRootRequest {
            request_id: "reserve-a".into(),
            root_id: "workspace-a".into(),
            preferred_home_node_id: "node-a".into(),
            session_id: "session-a".into(),
            rights: vec![RootRight::Lookup.into(), RootRight::Write.into()],
            expected_root_epoch: 1,
            parent_root_id: String::new(),
            create_intent_id: "mkdir-a".into(),
            conflict_policy: 0,
        }))
        .await
        .unwrap()
        .into_inner()
        .reservation
        .unwrap();
    assert_eq!(
        reservation.prepare_token,
        "prepare:workspace-a:session-a:mkdir-a:reserve-a"
    );

    let home_access = owner_rpc
        .activate_root(Request::new(ActivateRootRequest {
            request_id: "activate-a".into(),
            reservation: Some(reservation.clone()),
            local_prepare_id: "local-prepare-a".into(),
            parent_fsync_generation: 1,
            parent_fsync_complete: true,
        }))
        .await
        .unwrap()
        .into_inner()
        .access
        .unwrap();
    assert_eq!(home_access.holder_node_id, "node-a");
    assert_eq!(home_access.access_generation, 1);

    let location = owner_rpc
        .lookup_root(Request::new(LookupRootRequest {
            request_id: "lookup-a".into(),
            root_id: "workspace-a".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(location.found);
    assert_eq!(location.location.unwrap().home_node_id, "node-a");

    let listed = owner_rpc
        .list_owner_roots(Request::new(ListOwnerRootsRequest {
            request_id: "list-node-a".into(),
            home_node_id: "node-a".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.active_roots.len(), 1);
    assert_eq!(listed.active_roots[0].root_id, "workspace-a");
    assert!(listed.pending_reservations.is_empty());
    assert!(listed.authority_revision > 0);

    let remote_access = owner_rpc
        .acquire_root(Request::new(AcquireRootRequest {
            request_id: "acquire-b".into(),
            root_id: "workspace-a".into(),
            requester_node_id: "node-b".into(),
            session_id: "session-b".into(),
            rights: vec![RootRight::Lookup.into(), RootRight::Read.into()],
            expected_root_epoch: home_access.root_epoch,
            expected_access_generation: home_access.access_generation,
        }))
        .await
        .unwrap()
        .into_inner()
        .access
        .unwrap();
    assert_eq!(remote_access.home_node_id, "node-a");
    assert_eq!(remote_access.holder_node_id, "node-b");
    assert_eq!(
        remote_access.access_generation,
        home_access.access_generation
    );

    let widened_access = owner_rpc
        .acquire_root(Request::new(AcquireRootRequest {
            request_id: "acquire-b-write".into(),
            root_id: "workspace-a".into(),
            requester_node_id: "node-b".into(),
            session_id: "session-b".into(),
            rights: vec![RootRight::Write.into()],
            expected_root_epoch: home_access.root_epoch,
            expected_access_generation: home_access.access_generation,
        }))
        .await
        .unwrap()
        .into_inner()
        .access
        .unwrap();
    let widened_rights = widened_access
        .rights
        .iter()
        .filter_map(|right| RootRight::try_from(*right).ok())
        .collect::<Vec<_>>();
    assert!(widened_rights.contains(&RootRight::Lookup));
    assert!(widened_rights.contains(&RootRight::Read));
    assert!(widened_rights.contains(&RootRight::Write));

    let persisted_access = store
        .read(MetaRead::RootGrantByHolder {
            root_id: "workspace-a".into(),
            holder_node_id: "node-b".into(),
            holder_session_id: "session-b".into(),
        })
        .await
        .unwrap();
    let Some(MetaEntity::RootGrant(persisted_grant)) = persisted_access.entity else {
        panic!("node-b grant should persist");
    };
    assert!(persisted_grant.rights.contains(&StoreRootRight::Lookup));
    assert!(persisted_grant.rights.contains(&StoreRootRight::Read));
    assert!(persisted_grant.rights.contains(&StoreRootRight::Write));

    let home_node = meta_rpc
        .lookup_node(Request::new(LookupNodeRequest {
            request_id: "lookup-node-a".into(),
            node_id: remote_access.home_node_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(home_node.found);
    assert_eq!(
        home_node.node.unwrap().endpoint.unwrap().grpc_addr,
        "http://node-a:7400"
    );

    let bad_identity = owner_rpc
        .validate_root_access(Request::new(ValidateRootAccessRequest {
            request_id: "validate-bad".into(),
            presented_access: Some(PresentedRootAccess {
                root_id: remote_access.root_id.clone(),
                root_epoch: remote_access.root_epoch,
                home_node_id: remote_access.home_node_id.clone(),
                holder_node_id: remote_access.holder_node_id.clone(),
                session_id: remote_access.session_id.clone(),
                access_generation: remote_access.access_generation,
                fencing_token: remote_access.fencing_token.clone(),
                home_session_id: remote_access.home_session_id.clone(),
            }),
            observed_peer_node_id: "node-x".into(),
            validator_home_node_id: "node-a".into(),
            validator_home_session_id: "session-a".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(bad_identity.code(), Code::InvalidArgument);

    let validated = owner_rpc
        .validate_root_access(Request::new(ValidateRootAccessRequest {
            request_id: "validate-b".into(),
            presented_access: Some(PresentedRootAccess {
                root_id: remote_access.root_id.clone(),
                root_epoch: remote_access.root_epoch,
                home_node_id: remote_access.home_node_id.clone(),
                holder_node_id: remote_access.holder_node_id.clone(),
                session_id: remote_access.session_id.clone(),
                access_generation: remote_access.access_generation,
                fencing_token: remote_access.fencing_token.clone(),
                home_session_id: remote_access.home_session_id.clone(),
            }),
            observed_peer_node_id: "node-b".into(),
            validator_home_node_id: "node-a".into(),
            validator_home_session_id: "session-a".into(),
        }))
        .await
        .unwrap()
        .into_inner()
        .access
        .unwrap();
    assert_eq!(validated.holder_node_id, "node-b");

    meta_rpc
        .register_node(Request::new(RegisterNodeRequest {
            request_id: "register-node-a-restart".into(),
            node: Some(NodeDescriptor {
                node_id: "node-a".into(),
                endpoint: Some(NodeEndpoint {
                    grpc_addr: "http://node-a:7400".into(),
                    data_addr: "http://node-a:7500".into(),
                    rest_addr: "http://node-a:7600".into(),
                }),
                labels: Default::default(),
                capabilities: vec!["ownerfs".into()],
                session_id: "session-a-2".into(),
                storage_devices: Vec::new(),
            }),
            lease_seconds: 30,
        }))
        .await
        .unwrap();

    let recovered = owner_rpc
        .recover_root(Request::new(RecoverRootRequest {
            request_id: "recover-a".into(),
            root_id: "workspace-a".into(),
            expected_root_epoch: 1,
            home_node_id: "node-a".into(),
            home_session_id: "session-a-2".into(),
            local_prepare_id: "local-prepare-a".into(),
        }))
        .await
        .unwrap()
        .into_inner()
        .access
        .unwrap();
    assert_eq!(recovered.home_session_id, "session-a-2");
    assert_eq!(recovered.access_generation, 2);

    // B keeps its Node session across A's restart. Its old grant occupies the
    // same Meta key and must be replaced with one bound to A's new generation.
    let reacquired = owner_rpc
        .acquire_root(Request::new(AcquireRootRequest {
            request_id: "acquire-b-after-home-recover".into(),
            root_id: "workspace-a".into(),
            requester_node_id: "node-b".into(),
            session_id: "session-b".into(),
            rights: vec![RootRight::Lookup.into(), RootRight::Read.into()],
            expected_root_epoch: recovered.root_epoch,
            expected_access_generation: recovered.access_generation,
        }))
        .await
        .unwrap()
        .into_inner()
        .access
        .unwrap();
    assert_eq!(reacquired.home_session_id, "session-a-2");
    assert_eq!(reacquired.access_generation, 2);

    let old_remote_after_recover = store
        .read(MetaRead::RootGrant {
            root_id: remote_access.root_id,
            root_epoch: remote_access.root_epoch,
            home_session_id: remote_access.home_session_id,
            holder_node_id: remote_access.holder_node_id,
            holder_session_id: remote_access.session_id,
            access_generation: remote_access.access_generation,
            fencing_token: remote_access.fencing_token,
        })
        .await
        .unwrap();
    assert!(old_remote_after_recover.entity.is_none());
}

#[tokio::test]
async fn owner_authority_stale_abort_cannot_delete_new_pending_reservation() {
    let store = Arc::new(
        Store::open(Arc::new(MemoryBackend::default()))
            .await
            .unwrap(),
    );
    let meta = Arc::new(Meta::with_store(
        "meta-test".into(),
        Observability::new().unwrap(),
        store,
    ));
    let meta_rpc = rpc::MetaRpc(meta.clone());
    let owner_rpc = rpc::OwnerRootsRpc(meta);

    meta_rpc
        .register_node(Request::new(RegisterNodeRequest {
            request_id: "register-node-a".into(),
            node: Some(NodeDescriptor {
                node_id: "node-a".into(),
                endpoint: Some(NodeEndpoint {
                    grpc_addr: "http://node-a:7400".into(),
                    data_addr: "http://node-a:7500".into(),
                    rest_addr: "http://node-a:7600".into(),
                }),
                labels: Default::default(),
                capabilities: vec!["ownerfs".into()],
                session_id: "session-a".into(),
                storage_devices: Vec::new(),
            }),
            lease_seconds: 30,
        }))
        .await
        .unwrap();

    let old = owner_rpc
        .reserve_root(Request::new(ReserveRootRequest {
            request_id: "reserve-old".into(),
            root_id: "workspace-race".into(),
            preferred_home_node_id: "node-a".into(),
            session_id: "session-a".into(),
            expected_root_epoch: 1,
            create_intent_id: "mkdir-old".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .reservation
        .unwrap();

    owner_rpc
        .abort_root(Request::new(AbortRootRequest {
            request_id: "abort-old".into(),
            root_id: old.root_id.clone(),
            root_epoch: old.root_epoch,
            session_id: old.session_id.clone(),
            create_intent_id: old.create_intent_id.clone(),
            prepare_token: old.prepare_token.clone(),
            reason: "local prepare failed".into(),
        }))
        .await
        .unwrap();

    let new = owner_rpc
        .reserve_root(Request::new(ReserveRootRequest {
            request_id: "reserve-new".into(),
            root_id: "workspace-race".into(),
            preferred_home_node_id: "node-a".into(),
            session_id: "session-a".into(),
            expected_root_epoch: 1,
            create_intent_id: "mkdir-new".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .reservation
        .unwrap();
    assert_ne!(old.prepare_token, new.prepare_token);

    let stale_abort = owner_rpc
        .abort_root(Request::new(AbortRootRequest {
            request_id: "abort-stale-old".into(),
            root_id: old.root_id,
            root_epoch: old.root_epoch,
            session_id: old.session_id,
            create_intent_id: old.create_intent_id,
            prepare_token: old.prepare_token,
            reason: "late retry from old prepare".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(stale_abort.code(), Code::InvalidArgument);

    let listed = owner_rpc
        .list_owner_roots(Request::new(ListOwnerRootsRequest {
            request_id: "list-after-stale-abort".into(),
            home_node_id: "node-a".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(listed.active_roots.is_empty());
    assert_eq!(listed.pending_reservations.len(), 1);
    assert_eq!(
        listed.pending_reservations[0].prepare_token,
        new.prepare_token
    );
}

#[test]
fn owner_root_messages_keep_location_reservation_and_access_separate() {
    let location = RootLocation {
        root_id: "workspace-a".into(),
        root_epoch: 7,
        home_node_id: "node-a".into(),
        home_session_id: "session-a".into(),
    };
    let lookup = LookupRootReply {
        location: Some(location),
        found: true,
    };
    assert_eq!(lookup.location.unwrap().home_node_id, "node-a");

    let reservation = RootReservation {
        root_id: "workspace-a".into(),
        root_epoch: 7,
        home_node_id: "node-a".into(),
        session_id: "session-a".into(),
        create_intent_id: "intent-a".into(),
        prepare_token: "prepare-a".into(),
    };
    let reserve = ReserveRootReply {
        reservation: Some(reservation.clone()),
        already_reserved_by_same_intent: false,
    };
    assert_eq!(reserve.reservation.unwrap().prepare_token, "prepare-a");

    let access = RootAccess {
        root_id: reservation.root_id,
        root_epoch: reservation.root_epoch,
        home_node_id: reservation.home_node_id,
        holder_node_id: "node-b".into(),
        session_id: "session-b".into(),
        access_generation: 8,
        rights: vec![RootRight::Read.into(), RootRight::Write.into()],
        fencing_token: "fence-b".into(),
        home_session_id: "session-a".into(),
    };
    let activate = ActivateRootReply {
        access: Some(access),
    };
    let access = activate.access.unwrap();
    assert_eq!(access.home_node_id, "node-a");
    assert_eq!(access.holder_node_id, "node-b");
    assert_eq!(access.session_id, "session-b");

    let presented = PresentedRootAccess {
        root_id: access.root_id.clone(),
        root_epoch: access.root_epoch,
        home_node_id: access.home_node_id.clone(),
        holder_node_id: access.holder_node_id.clone(),
        session_id: access.session_id.clone(),
        access_generation: access.access_generation,
        fencing_token: access.fencing_token.clone(),
        home_session_id: access.home_session_id.clone(),
    };
    assert_eq!(presented.holder_node_id, "node-b");
    assert_eq!(presented.fencing_token, "fence-b");

    let watch = WatchRootCommandsRequest {
        node_id: "node-b".into(),
        session_id: "session-b".into(),
        after_revision: 41,
    };
    assert_eq!(watch.after_revision, 41);

    let command = RootCommand {
        command_id: "cmd-42".into(),
        command_type: RootCommandType::RevokeAccess.into(),
        access: Some(access),
        revision: 42,
    };
    assert_eq!(command.revision, 42);
}

#[test]
fn dfs_commit_keeps_file_layout_and_durable_chunk_receipt_explicit() {
    let commit = CommitFileVersionRequest {
        caller_id: "node-a".into(),
        operation_id: "session-a-write-1".into(),
        inode_id: "inode:session-a-create-1".into(),
        expected_inode_revision: 1,
        expected_head_version_id: String::new(),
        version: Some(DfsFileVersion {
            version_id: "session-a-version-2".into(),
            inode_id: "inode:session-a-create-1".into(),
            parent_version_id: String::new(),
            length: 5,
            layout_root_id: "session-a-layout-2".into(),
            created_at_unix_ms: 123,
        }),
        layout: Some(DfsLayoutRoot {
            layout_root_id: "session-a-layout-2".into(),
            file_length: 5,
            inline_extents: vec![DfsExtent {
                file_offset: 0,
                length: 5,
                chunk_id: "digest-5".into(),
                chunk_offset: 0,
            }],
        }),
        chunk_receipts: vec![DfsChunkReceipt {
            operation_id: "session-a-write-1".into(),
            chunk_id: "digest-5".into(),
            chunk_length: 5,
            content_digest: vec![0; 32],
            content_digest_algorithm: afs_protocol::meta::DfsDigestAlgorithm::Blake3.into(),
            placement_revision: 1,
            placement_epoch: 1,
            replica_group_id: "local:node-a".into(),
            durable_acks: vec![DfsReplicaAck {
                operation_id: "session-a-write-1".into(),
                chunk_id: "digest-5".into(),
                placement_revision: 1,
                placement_epoch: 1,
                node_id: "node-a".into(),
                node_epoch: 1,
                device_id: "local-0".into(),
                device_epoch: 1,
                catalog_revision: 0,
                persisted_bytes: 5,
                verified_digest: vec![0; 32],
                verified_digest_algorithm: afs_protocol::meta::DfsDigestAlgorithm::Blake3.into(),
            }],
        }],
        write_lease: Some(DfsWriteLease {
            inode_id: "inode:session-a-create-1".into(),
            owner_node_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            lease_epoch: 1,
            expires_at_unix_ms: 456,
        }),
        metadata_delta: Some(DfsCommitMetadataDelta {
            kill_suidgid: false,
            mode: DfsCommitMetadataMode::Full.into(),
            mtime_unix_ms: 123,
            ctime_unix_ms: 123,
        }),
    };
    assert_eq!(commit.layout.as_ref().unwrap().inline_extents.len(), 1);
    assert_eq!(commit.chunk_receipts[0].durable_acks[0].persisted_bytes, 5);
}

async fn n2b1_memory_owner_fixture() -> (Arc<Store>, rpc::MetaRpc, rpc::OwnerRootsRpc) {
    let store = Arc::new(
        Store::open(Arc::new(MemoryBackend::default()))
            .await
            .unwrap(),
    );
    let meta = Arc::new(Meta::with_store(
        "meta-test".into(),
        Observability::new().unwrap(),
        store.clone(),
    ));
    (store, rpc::MetaRpc(meta.clone()), rpc::OwnerRootsRpc(meta))
}

async fn n2b1_register_node(meta_rpc: &rpc::MetaRpc) {
    meta_rpc
        .register_node(Request::new(RegisterNodeRequest {
            request_id: "register-node-a".into(),
            node: Some(NodeDescriptor {
                node_id: "node-a".into(),
                endpoint: Some(NodeEndpoint {
                    grpc_addr: "http://node-a:7400".into(),
                    data_addr: "http://node-a:7500".into(),
                    rest_addr: "http://node-a:7600".into(),
                }),
                labels: Default::default(),
                capabilities: vec!["ownerfs".into()],
                session_id: "session-a".into(),
                storage_devices: Vec::new(),
            }),
            lease_seconds: 30,
        }))
        .await
        .unwrap();
}

async fn n2b1_seed_root_command(
    store: &Store,
    request_id: impl Into<String>,
    command_id: impl Into<String>,
    home_node_id: impl Into<String>,
    home_session_id: impl Into<String>,
    root_id: impl Into<String>,
    old_access_generation: u64,
) {
    use afs::meta::store::{
        MetaTxn, RequestOutcome, RootCommandRecord, RootCommandType as StoreRootCommandType,
        TxnCondition, TxnMutation,
    };

    let command = RootCommandRecord {
        command_id: command_id.into(),
        home_node_id: home_node_id.into(),
        home_session_id: home_session_id.into(),
        root_id: root_id.into(),
        root_epoch: 7,
        old_access_generation,
        command_type: StoreRootCommandType::RevokeAccess,
    };
    let key = RequestKey::new("n2b1-seed", request_id);
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::BeginRootRevocation);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::RootCommand(command.clone())),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::BeginRootRevocation,
            result: OperationResult::RootCommand(command),
        }),
    ]);
    store.compare_and_commit(txn).await.unwrap();
}

fn n2b1_events(
    reply: afs_protocol::meta::RootCommandBatchReply,
) -> afs_protocol::meta::RootCommandBatchEvents {
    match reply.result.unwrap() {
        afs_protocol::meta::root_command_batch_reply::Result::Events(events) => events,
        _ => panic!("expected events reply"),
    }
}

async fn n2b1_wait_for_backend_release(backend: Weak<LocalFileBackend>) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while backend.upgrade().is_some() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "local file backend should be released before reopening the same directory"
        );
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn n2b1_poll_root_command_batch_without_store_returns_typed_unsupported() {
    let owner_rpc = rpc::OwnerRootsRpc(Arc::new(Meta::new(
        "meta-no-store".into(),
        Observability::new().unwrap(),
    )));
    let reply = owner_rpc
        .poll_root_command_batch(Request::new(PollRootCommandBatchRequest {
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            after_revision: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let Some(afs_protocol::meta::root_command_batch_reply::Result::Unsupported(unsupported)) =
        reply.result
    else {
        panic!("expected unsupported reply");
    };
    assert!(unsupported.message.contains("unavailable"));
}

#[tokio::test]
async fn n2b1_poll_root_command_batch_rejects_missing_current_session() {
    let (_, _, owner_rpc) = n2b1_memory_owner_fixture().await;
    let error = owner_rpc
        .poll_root_command_batch(Request::new(PollRootCommandBatchRequest {
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            after_revision: 0,
        }))
        .await
        .unwrap_err();
    assert_ne!(error.code(), Code::Ok);
}

#[tokio::test]
async fn n2b1_poll_root_command_batch_rejects_non_current_session() {
    let (_, meta_rpc, owner_rpc) = n2b1_memory_owner_fixture().await;
    n2b1_register_node(&meta_rpc).await;
    let error = owner_rpc
        .poll_root_command_batch(Request::new(PollRootCommandBatchRequest {
            node_id: "node-a".into(),
            session_id: "session-old".into(),
            after_revision: 0,
        }))
        .await
        .unwrap_err();
    assert_ne!(error.code(), Code::Ok);
}

#[tokio::test]
async fn n2b1_poll_root_command_batch_reports_empty_command_history_after_current_session() {
    let (_, meta_rpc, owner_rpc) = n2b1_memory_owner_fixture().await;
    n2b1_register_node(&meta_rpc).await;
    let events = owner_rpc
        .poll_root_command_batch(Request::new(PollRootCommandBatchRequest {
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            after_revision: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let events = n2b1_events(events);
    assert_eq!(events.start_revision, 0);
    assert_eq!(events.next_revision, 2);
    assert!(events.commands.is_empty());
}

#[tokio::test]
async fn n2b1_poll_root_command_batch_advances_past_filtered_events_without_skipping_adjacent_target()
 {
    let (store, meta_rpc, owner_rpc) = n2b1_memory_owner_fixture().await;
    n2b1_register_node(&meta_rpc).await;
    for index in 0..1022 {
        n2b1_seed_root_command(
            &store,
            format!("unrelated-{index}"),
            format!("cmd-unrelated-{index}"),
            "other-node",
            "other-session",
            format!("workspace-unrelated-{index}"),
            1,
        )
        .await;
    }
    n2b1_seed_root_command(
        &store,
        "target",
        "cmd-target",
        "node-a",
        "session-a",
        "workspace-a",
        3,
    )
    .await;

    let first = owner_rpc
        .poll_root_command_batch(Request::new(PollRootCommandBatchRequest {
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            after_revision: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let first = n2b1_events(first);
    assert_eq!(first.start_revision, 0);
    assert_eq!(first.next_revision, 1024);
    assert!(first.commands.is_empty());

    let resume_after = first.next_revision.checked_sub(1).unwrap();
    let second = owner_rpc
        .poll_root_command_batch(Request::new(PollRootCommandBatchRequest {
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            after_revision: resume_after,
        }))
        .await
        .unwrap()
        .into_inner();
    let second = n2b1_events(second);
    assert_eq!(second.start_revision, 1023);
    assert_eq!(second.next_revision, 1025);
    assert_eq!(second.commands.len(), 1);
    assert_eq!(second.commands[0].command_id, "cmd-target");

    let skipped = owner_rpc
        .poll_root_command_batch(Request::new(PollRootCommandBatchRequest {
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            after_revision: first.next_revision,
        }))
        .await
        .unwrap()
        .into_inner();
    let skipped = n2b1_events(skipped);
    assert!(skipped.commands.is_empty());
}

#[tokio::test]
async fn n2b1_poll_root_command_batch_rejects_saturated_invalid_cursor() {
    let (_, meta_rpc, owner_rpc) = n2b1_memory_owner_fixture().await;
    n2b1_register_node(&meta_rpc).await;
    let error = owner_rpc
        .poll_root_command_batch(Request::new(PollRootCommandBatchRequest {
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            after_revision: u64::MAX,
        }))
        .await
        .unwrap_err();
    assert_ne!(error.code(), Code::Ok);
}

#[tokio::test]
async fn n2b1_ack_revocation_requires_exact_stored_command_before_writing_ack() {
    let (store, meta_rpc, owner_rpc) = n2b1_memory_owner_fixture().await;
    n2b1_register_node(&meta_rpc).await;

    let missing = owner_rpc
        .ack_revocation(Request::new(AckRevocationRequest {
            request_id: "ack-missing".into(),
            command_id: "cmd-missing".into(),
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            root_id: "workspace-a".into(),
            root_epoch: 7,
            access_generation: 3,
            success: true,
            message: "installed".into(),
        }))
        .await
        .unwrap_err();
    assert_ne!(missing.code(), Code::Ok);

    n2b1_seed_root_command(
        &store,
        "seed-exact",
        "cmd-exact",
        "node-a",
        "session-a",
        "workspace-a",
        3,
    )
    .await;

    let wrong_root = owner_rpc
        .ack_revocation(Request::new(AckRevocationRequest {
            request_id: "ack-wrong-root".into(),
            command_id: "cmd-exact".into(),
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            root_id: "workspace-b".into(),
            root_epoch: 7,
            access_generation: 3,
            success: true,
            message: "wrong root".into(),
        }))
        .await
        .unwrap_err();
    assert_ne!(wrong_root.code(), Code::Ok);

    owner_rpc
        .ack_revocation(Request::new(AckRevocationRequest {
            request_id: "ack-exact".into(),
            command_id: "cmd-exact".into(),
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            root_id: "workspace-a".into(),
            root_epoch: 7,
            access_generation: 3,
            success: true,
            message: "installed".into(),
        }))
        .await
        .unwrap();

    owner_rpc
        .ack_revocation(Request::new(AckRevocationRequest {
            request_id: "ack-exact".into(),
            command_id: "cmd-exact".into(),
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            root_id: "workspace-a".into(),
            root_epoch: 7,
            access_generation: 3,
            success: true,
            message: "installed".into(),
        }))
        .await
        .unwrap();

    let replay_conflict = owner_rpc
        .ack_revocation(Request::new(AckRevocationRequest {
            request_id: "ack-exact".into(),
            command_id: "cmd-exact".into(),
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            root_id: "workspace-a".into(),
            root_epoch: 7,
            access_generation: 3,
            success: true,
            message: "changed payload".into(),
        }))
        .await
        .unwrap_err();
    assert_ne!(replay_conflict.code(), Code::Ok);

    let duplicate = owner_rpc
        .ack_revocation(Request::new(AckRevocationRequest {
            request_id: "ack-duplicate".into(),
            command_id: "cmd-exact".into(),
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            root_id: "workspace-a".into(),
            root_epoch: 7,
            access_generation: 3,
            success: true,
            message: "duplicate".into(),
        }))
        .await
        .unwrap_err();
    assert_ne!(duplicate.code(), Code::Ok);
}

#[tokio::test]
async fn n2b1_local_file_reopens_legacy_root_command_and_preserves_exact_ack_replay() {
    let source_dir = tempfile::tempdir().unwrap();
    let source_backend = Arc::new(LocalFileBackend::open(source_dir.path()).unwrap());
    let source_store = Arc::new(Store::open(source_backend.clone()).await.unwrap());
    n2b1_seed_root_command(
        &source_store,
        "seed-legacy",
        "cmd-legacy",
        "node-a",
        "session-a",
        "workspace-legacy",
        3,
    )
    .await;
    let (_, current_bytes) = LocalFileBackend::load(&source_backend).unwrap().unwrap();
    let source_backend_released = Arc::downgrade(&source_backend);
    drop(source_store);
    drop(source_backend);
    n2b1_wait_for_backend_release(source_backend_released).await;

    let mut legacy_json = String::from_utf8(current_bytes).unwrap();
    if legacy_json.contains(",\"command_type\":\"RevokeAccess\"") {
        legacy_json = legacy_json.replace(",\"command_type\":\"RevokeAccess\"", "");
    } else if legacy_json.contains("\"command_type\":\"RevokeAccess\",") {
        legacy_json = legacy_json.replace("\"command_type\":\"RevokeAccess\",", "");
    } else {
        panic!("serialized root command should contain command_type before legacy rewrite");
    }

    let dir = tempfile::tempdir().unwrap();
    let legacy_backend = LocalFileBackend::open(dir.path()).unwrap();
    LocalFileBackend::commit(&legacy_backend, 0, legacy_json.as_bytes()).unwrap();
    drop(legacy_backend);

    {
        let backend = Arc::new(LocalFileBackend::open(dir.path()).unwrap());
        let store = Arc::new(Store::open(backend.clone()).await.unwrap());
        let meta = Arc::new(Meta::with_store(
            "meta-reopened".into(),
            Observability::new().unwrap(),
            store.clone(),
        ));
        let meta_rpc = rpc::MetaRpc(meta.clone());
        let owner_rpc = rpc::OwnerRootsRpc(meta);
        n2b1_register_node(&meta_rpc).await;
        n2b1_seed_root_command(
            &store,
            "seed-current",
            "cmd-current",
            "node-a",
            "session-a",
            "workspace-current",
            5,
        )
        .await;

        let events = owner_rpc
            .poll_root_command_batch(Request::new(PollRootCommandBatchRequest {
                node_id: "node-a".into(),
                session_id: "session-a".into(),
                after_revision: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        let events = n2b1_events(events);
        let legacy = events
            .commands
            .iter()
            .find(|command| command.command_id == "cmd-legacy")
            .expect("legacy command after reopen");
        assert_eq!(legacy.command_type, RootCommandType::RevokeAccess as i32);
        let current = events
            .commands
            .iter()
            .find(|command| command.command_id == "cmd-current")
            .expect("current command after reopen");
        assert_eq!(current.command_type, RootCommandType::RevokeAccess as i32);

        owner_rpc
            .ack_revocation(Request::new(AckRevocationRequest {
                request_id: "ack-legacy".into(),
                command_id: "cmd-legacy".into(),
                node_id: "node-a".into(),
                session_id: "session-a".into(),
                root_id: "workspace-legacy".into(),
                root_epoch: 7,
                access_generation: 3,
                success: true,
                message: "legacy installed".into(),
            }))
            .await
            .unwrap();
        owner_rpc
            .ack_revocation(Request::new(AckRevocationRequest {
                request_id: "ack-current".into(),
                command_id: "cmd-current".into(),
                node_id: "node-a".into(),
                session_id: "session-a".into(),
                root_id: "workspace-current".into(),
                root_epoch: 7,
                access_generation: 5,
                success: true,
                message: "current installed".into(),
            }))
            .await
            .unwrap();
        let backend_released = Arc::downgrade(&backend);
        drop(owner_rpc);
        drop(meta_rpc);
        drop(store);
        drop(backend);
        n2b1_wait_for_backend_release(backend_released).await;
    }

    {
        let backend = Arc::new(LocalFileBackend::open(dir.path()).unwrap());
        let store = Arc::new(Store::open(backend.clone()).await.unwrap());
        let meta = Arc::new(Meta::with_store(
            "meta-reopened-again".into(),
            Observability::new().unwrap(),
            store.clone(),
        ));
        let owner_rpc = rpc::OwnerRootsRpc(meta);

        let events = owner_rpc
            .poll_root_command_batch(Request::new(PollRootCommandBatchRequest {
                node_id: "node-a".into(),
                session_id: "session-a".into(),
                after_revision: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        let events = n2b1_events(events);
        let current = events
            .commands
            .iter()
            .find(|command| command.command_id == "cmd-current")
            .expect("current command after second reopen");
        assert_eq!(current.command_type, RootCommandType::RevokeAccess as i32);

        owner_rpc
            .ack_revocation(Request::new(AckRevocationRequest {
                request_id: "ack-current".into(),
                command_id: "cmd-current".into(),
                node_id: "node-a".into(),
                session_id: "session-a".into(),
                root_id: "workspace-current".into(),
                root_epoch: 7,
                access_generation: 5,
                success: true,
                message: "current installed".into(),
            }))
            .await
            .unwrap();
        owner_rpc
            .ack_revocation(Request::new(AckRevocationRequest {
                request_id: "ack-legacy".into(),
                command_id: "cmd-legacy".into(),
                node_id: "node-a".into(),
                session_id: "session-a".into(),
                root_id: "workspace-legacy".into(),
                root_epoch: 7,
                access_generation: 3,
                success: true,
                message: "legacy installed".into(),
            }))
            .await
            .unwrap();

        let conflict = owner_rpc
            .ack_revocation(Request::new(AckRevocationRequest {
                request_id: "ack-current".into(),
                command_id: "cmd-current".into(),
                node_id: "node-a".into(),
                session_id: "session-a".into(),
                root_id: "workspace-current".into(),
                root_epoch: 7,
                access_generation: 5,
                success: true,
                message: "changed after reopen".into(),
            }))
            .await
            .unwrap_err();
        assert_ne!(conflict.code(), Code::Ok);
        let backend_released = Arc::downgrade(&backend);
        drop(owner_rpc);
        drop(store);
        drop(backend);
        n2b1_wait_for_backend_release(backend_released).await;
    }
}

fn status_errno(status: tonic::Status) -> i32 {
    let error = afs_transport::grpc::error_status::status_to_error(status);
    afs::error::errno(&error)
}

fn test_attrs(mode: u32, nlink: u32) -> DfsInodeAttributes {
    DfsInodeAttributes {
        mode,
        uid: 1000,
        gid: 1000,
        nlink,
        atime_unix_ms: 1,
        mtime_unix_ms: 1,
        ctime_unix_ms: 1,
    }
}

fn caller(uid: u32, gid: u32) -> DfsCallerContext {
    caller_with_groups(uid, gid, &[gid])
}

fn caller_with_groups(uid: u32, gid: u32, supplementary_gids: &[u32]) -> DfsCallerContext {
    DfsCallerContext {
        uid,
        gid,
        supplementary_gids: supplementary_gids.to_vec(),
    }
}

fn domain_caller(uid: u32, gid: u32) -> afs::dfs::CallerContext {
    domain_caller_with_groups(uid, gid, &[gid])
}

fn domain_caller_with_groups(
    uid: u32,
    gid: u32,
    supplementary_gids: &[u32],
) -> afs::dfs::CallerContext {
    afs::dfs::CallerContext {
        uid,
        gid,
        supplementary_gids: supplementary_gids.to_vec(),
    }
}

async fn memory_dfs() -> rpc::DfsMetaRpc {
    let store = Arc::new(
        Store::open(Arc::new(MemoryBackend::default()))
            .await
            .unwrap(),
    );
    rpc::DfsMetaRpc(Arc::new(Meta::with_store(
        "meta-test".into(),
        Observability::new().unwrap(),
        store,
    )))
}

async fn memory_meta_and_dfs_service(replication: ReplicationConfig) -> (rpc::MetaRpc, DfsService) {
    let store = Arc::new(
        Store::open(Arc::new(MemoryBackend::default()))
            .await
            .unwrap(),
    );
    let meta = rpc::MetaRpc(Arc::new(Meta::with_store(
        "meta-test".into(),
        Observability::new().unwrap(),
        store.clone(),
    )));
    let dfs = DfsService::with_replication_config(store, replication);
    dfs.initialize_replication_config().await.unwrap();
    (meta, dfs)
}

async fn register_dfs_storage_node(
    meta: &rpc::MetaRpc,
    node_id: &str,
    session_id: &str,
    request_id: &str,
    failure_domain: &str,
) {
    register_dfs_storage_node_with_catalog(
        meta,
        node_id,
        session_id,
        request_id,
        failure_domain,
        0,
    )
    .await;
}

async fn register_dfs_storage_node_with_catalog(
    meta: &rpc::MetaRpc,
    node_id: &str,
    session_id: &str,
    request_id: &str,
    failure_domain: &str,
    catalog_revision: u64,
) {
    meta.register_node(Request::new(RegisterNodeRequest {
        request_id: request_id.into(),
        node: Some(NodeDescriptor {
            node_id: node_id.into(),
            endpoint: Some(NodeEndpoint {
                grpc_addr: format!("http://{node_id}:7400"),
                data_addr: format!("http://{node_id}:7500"),
                rest_addr: format!("http://{node_id}:7600"),
            }),
            labels: Default::default(),
            capabilities: vec!["dfs".into()],
            session_id: session_id.into(),
            storage_devices: vec![DfsStorageDevice {
                device_id: format!("{node_id}-ssd0"),
                device_epoch: 1,
                catalog_revision,
                failure_domain: failure_domain.into(),
            }],
        }),
        lease_seconds: 30,
    }))
    .await
    .unwrap();
}

async fn register_dfs_compute_node(
    meta: &rpc::MetaRpc,
    node_id: &str,
    session_id: &str,
    request_id: &str,
) {
    meta.register_node(Request::new(RegisterNodeRequest {
        request_id: request_id.into(),
        node: Some(NodeDescriptor {
            node_id: node_id.into(),
            endpoint: Some(NodeEndpoint {
                grpc_addr: format!("http://{node_id}:7400"),
                data_addr: format!("http://{node_id}:7500"),
                rest_addr: format!("http://{node_id}:7600"),
            }),
            labels: Default::default(),
            capabilities: vec!["dfs".into()],
            session_id: session_id.into(),
            storage_devices: Vec::new(),
        }),
        lease_seconds: 30,
    }))
    .await
    .unwrap();
}

#[derive(Default)]
struct LostCommitResultState {
    armed: bool,
    commits: u64,
    last_version: Option<u64>,
    last_digest: Option<[u8; 32]>,
}

struct LostCommitResultBackend {
    inner: Arc<dyn StoreBackend>,
    state: Mutex<LostCommitResultState>,
}

impl LostCommitResultBackend {
    fn new(inner: Arc<dyn StoreBackend>) -> Self {
        Self {
            inner,
            state: Mutex::new(LostCommitResultState::default()),
        }
    }

    fn arm_once(&self) {
        self.state.lock().unwrap().armed = true;
    }

    fn observed_commit(&self) -> (u64, u64, [u8; 32]) {
        let state = self.state.lock().unwrap();
        (
            state.commits,
            state
                .last_version
                .expect("backend wrapper observed no successful native commit"),
            state
                .last_digest
                .expect("backend wrapper observed no committed snapshot digest"),
        )
    }

    async fn require_empty_snapshot(&self) {
        assert!(
            self.inner.load().await.unwrap().is_none(),
            "lost-result opt-in backend must start with no existing snapshot"
        );
    }
}

impl StoreBackend for LostCommitResultBackend {
    fn load(&self) -> afs::meta::store::MetaFuture<'_, Option<(u64, Vec<u8>)>> {
        self.inner.load()
    }

    fn commit(
        &self,
        expected_version: u64,
        bytes: Vec<u8>,
    ) -> afs::meta::store::MetaFuture<'_, u64> {
        Box::pin(async move {
            let digest = *blake3::hash(&bytes).as_bytes();
            let version = self.inner.commit(expected_version, bytes).await?;
            let mut state = self.state.lock().unwrap();
            state.commits += 1;
            state.last_version = Some(version);
            state.last_digest = Some(digest);
            if state.armed {
                state.armed = false;
                Err(afs_error::Error::coded(
                    afs_error::IO_UNAVAILABLE,
                    "injected lost backend result after durable Meta commit",
                ))
            } else {
                Ok(version)
            }
        })
    }
}

async fn lost_result_commit_request(
    meta: &rpc::MetaRpc,
    dfs: &rpc::DfsMetaRpc,
) -> CommitFileVersionRequest {
    register_dfs_storage_node_with_catalog(
        meta,
        "node-a",
        "session-a",
        "register-lost-result-storage",
        "rack-a",
        7,
    )
    .await;
    let dfs_service = DfsService::with_replication_config(
        meta.0.store.as_ref().unwrap().clone(),
        ReplicationConfig::local_single_copy(),
    );
    dfs_service.initialize_replication_config().await.unwrap();
    let placement = dfs_service
        .placement_snapshot("node-a".into())
        .await
        .unwrap();
    let group = placement.replica_groups[0].clone();
    let target = group.targets[0].clone();
    let created = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-lost-result".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"lost-result.bin".to_vec(),
            attributes: Some(test_attrs(0o644, 1)),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner();
    let inode = created.inode.unwrap();
    let lease = created.write_lease.unwrap();
    CommitFileVersionRequest {
        caller_id: "node-a".into(),
        operation_id: "commit-lost-result".into(),
        inode_id: inode.inode_id.clone(),
        expected_inode_revision: inode.revision,
        expected_head_version_id: String::new(),
        version: Some(DfsFileVersion {
            version_id: "version-lost-result".into(),
            inode_id: inode.inode_id.clone(),
            parent_version_id: String::new(),
            length: 4096,
            layout_root_id: "layout-lost-result".into(),
            created_at_unix_ms: 10,
        }),
        layout: Some(DfsLayoutRoot {
            layout_root_id: "layout-lost-result".into(),
            file_length: 4096,
            inline_extents: vec![DfsExtent {
                file_offset: 0,
                length: 4096,
                chunk_id: "chunk-lost-result".into(),
                chunk_offset: 0,
            }],
        }),
        chunk_receipts: vec![DfsChunkReceipt {
            operation_id: "commit-lost-result".into(),
            chunk_id: "chunk-lost-result".into(),
            chunk_length: 4096,
            content_digest: vec![11; 32],
            content_digest_algorithm: afs_protocol::meta::DfsDigestAlgorithm::Blake3.into(),
            placement_revision: placement.revision,
            placement_epoch: group.placement_epoch,
            replica_group_id: group.id.0.clone(),
            durable_acks: vec![DfsReplicaAck {
                operation_id: "commit-lost-result".into(),
                chunk_id: "chunk-lost-result".into(),
                placement_revision: placement.revision,
                placement_epoch: group.placement_epoch,
                node_id: target.node_id,
                node_epoch: target.node_epoch,
                device_id: target.device.device_id,
                device_epoch: target.device.device_epoch,
                catalog_revision: 7,
                persisted_bytes: 4096,
                verified_digest: vec![11; 32],
                verified_digest_algorithm: afs_protocol::meta::DfsDigestAlgorithm::Blake3.into(),
            }],
        }],
        write_lease: Some(lease),
        metadata_delta: Some(DfsCommitMetadataDelta {
            kill_suidgid: false,
            mode: DfsCommitMetadataMode::DataOnly.into(),
            mtime_unix_ms: 0,
            ctime_unix_ms: 0,
        }),
    }
}

async fn assert_lost_result_commit_contract(backend: Arc<LostCommitResultBackend>) {
    backend.require_empty_snapshot().await;
    let store = Arc::new(Store::open(backend.clone()).await.unwrap());
    let meta = rpc::MetaRpc(Arc::new(Meta::with_store(
        "meta-lost-result".into(),
        Observability::new().unwrap(),
        store.clone(),
    )));
    let dfs = rpc::DfsMetaRpc(meta.0.clone());
    let commit = lost_result_commit_request(&meta, &dfs).await;

    backend.arm_once();
    let lost = dfs
        .commit_file_version(Request::new(commit.clone()))
        .await
        .unwrap_err();
    assert_eq!(lost.code(), Code::Unavailable);
    assert!(store.health().await.is_err());
    assert!(
        store
            .read(MetaRead::DfsInode(afs::dfs::InodeId::new(
                commit.inode_id.clone()
            )))
            .await
            .is_err(),
        "poisoned Store must fail closed for reads after an unknown durable result"
    );
    assert_eq!(
        dfs.commit_file_version(Request::new(commit.clone()))
            .await
            .unwrap_err()
            .code(),
        Code::Unavailable
    );

    let (commits_after_lost_result, native_version, native_digest) = backend.observed_commit();
    let (loaded_version, loaded_bytes) = backend.inner.load().await.unwrap().unwrap();
    assert_eq!(loaded_version, native_version);
    assert_eq!(*blake3::hash(&loaded_bytes).as_bytes(), native_digest);

    let reopened_store = Arc::new(Store::open(backend.clone()).await.unwrap());
    let reopened = rpc::DfsMetaRpc(Arc::new(Meta::with_store(
        "meta-lost-result-restarted".into(),
        Observability::new().unwrap(),
        reopened_store.clone(),
    )));
    let recovered_inode = reopened_store
        .read(MetaRead::DfsInode(afs::dfs::InodeId::new(
            commit.inode_id.clone(),
        )))
        .await
        .unwrap();
    let recovered_version = reopened_store
        .read(MetaRead::DfsFileVersion(afs::dfs::FileVersionId::new(
            "version-lost-result",
        )))
        .await
        .unwrap();
    let Some(MetaEntity::DfsFileVersion(version)) = recovered_version.entity else {
        panic!("native snapshot must recover the committed FileVersion before replay");
    };
    assert_eq!(version.inode_id.0, commit.inode_id);
    assert_eq!(version.length, 4096);
    assert_eq!(version.layout_root.0, "layout-lost-result");
    let recovered_layout = reopened_store
        .read(MetaRead::DfsLayoutRoot(afs::dfs::LayoutRootId::new(
            "layout-lost-result",
        )))
        .await
        .unwrap();
    let Some(MetaEntity::DfsLayoutRoot(layout)) = recovered_layout.entity else {
        panic!("native snapshot must recover the committed LayoutRoot before replay");
    };
    assert_eq!(layout.file_length, 4096);
    assert_eq!(layout.inline_extents.len(), 1);
    assert_eq!(layout.inline_extents[0].chunk_id.0, "chunk-lost-result");
    assert_eq!(layout.inline_extents[0].length, 4096);
    let replay = reopened
        .commit_file_version(Request::new(commit.clone()))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(replay.head_version_id, "version-lost-result");
    let (commits_after_replay, replay_version, replay_digest) = backend.observed_commit();
    assert_eq!(
        commits_after_replay, commits_after_lost_result,
        "exact OperationId replay must use the persisted request outcome"
    );
    assert_eq!(replay_version, native_version);
    assert_eq!(replay_digest, native_digest);
    assert_eq!(
        reopened_store
            .read(MetaRead::DfsInode(afs::dfs::InodeId::new(
                commit.inode_id.clone(),
            )))
            .await
            .unwrap(),
        recovered_inode,
        "exact replay must not change the recovered inode or Store revision"
    );
    let outcome = reopened_store
        .read(MetaRead::RequestOutcome(RequestKey::new(
            "node-a",
            "commit-lost-result",
        )))
        .await
        .unwrap()
        .request_outcome
        .unwrap();
    assert_eq!(outcome.operation, StoreOperation::DfsCommitFileVersion);
    let OperationResult::DfsNamespace { result, .. } = outcome.result else {
        panic!("commit outcome must be bound to the original namespace request");
    };
    let OperationResult::DfsInode(record) = *result else {
        panic!("commit outcome must replay the committed inode");
    };
    assert_eq!(record.inode_id.0, replay.inode_id);
    assert_eq!(record.revision, replay.revision);
    assert_eq!(
        record
            .head_version
            .as_ref()
            .map(|version| version.0.as_str()),
        Some(replay.head_version_id.as_str())
    );

    let mut changed = commit.clone();
    changed.version.as_mut().unwrap().length = 4097;
    changed.layout.as_mut().unwrap().file_length = 4097;
    assert_eq!(
        reopened
            .commit_file_version(Request::new(changed))
            .await
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    let (_, after_bad_bytes) = backend.inner.load().await.unwrap().unwrap();
    assert_eq!(*blake3::hash(&after_bad_bytes).as_bytes(), native_digest);

    let mut next = commit;
    next.operation_id = "commit-lost-result-next".into();
    next.expected_inode_revision = replay.revision;
    next.expected_head_version_id = replay.head_version_id.clone();
    next.version.as_mut().unwrap().version_id = "version-lost-result-next".into();
    next.version.as_mut().unwrap().parent_version_id = replay.head_version_id.clone();
    next.version.as_mut().unwrap().layout_root_id = "layout-lost-result-next".into();
    next.layout.as_mut().unwrap().layout_root_id = "layout-lost-result-next".into();
    next.chunk_receipts.clear();
    let next_inode = reopened
        .commit_file_version(Request::new(next))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(next_inode.head_version_id, "version-lost-result-next");
    assert_eq!(backend.observed_commit().0, commits_after_lost_result + 1);
    println!(
        "LOST_RESULT_RECEIPT {}",
        serde_json::json!({
            "operation_id": "commit-lost-result",
            "version_id": "version-lost-result",
            "layout_root_id": "layout-lost-result",
            "receipt_count": 1,
            "native_version_after_lost_result": native_version,
            "native_snapshot_bytes": loaded_bytes.len(),
            "native_snapshot_blake3": blake3::Hash::from_bytes(native_digest).to_hex().to_string(),
            "successful_native_commits_before_replay": commits_after_lost_result,
            "successful_native_commits_after_replay": commits_after_replay,
            "successful_native_commits_after_next_operation": backend.observed_commit().0,
            "old_store_failed_closed": true,
            "same_identity_changed_payload_rejected": true,
            "scope": "StoreBackend result suppression after real commit; no wire loss or physical chunk proof"
        })
    );
}

#[tokio::test]
async fn dfs_commit_recovers_lost_store_result_with_exact_operation_replay() {
    let backend = Arc::new(LostCommitResultBackend::new(Arc::new(
        MemoryBackend::default(),
    )));
    assert_lost_result_commit_contract(backend).await;
}

#[tokio::test]
#[ignore = "requires a fresh dedicated etcd endpoint in AFS_TEST_ETCD_LOST_RESULT_ENDPOINT"]
async fn dfs_commit_recovers_lost_store_result_with_etcd_snapshot_backend() {
    let endpoint = std::env::var("AFS_TEST_ETCD_LOST_RESULT_ENDPOINT")
        .expect("AFS_TEST_ETCD_LOST_RESULT_ENDPOINT is required");
    let inner = Arc::new(
        afs::meta::store::etcd::EtcdBackend::connect(endpoint)
            .await
            .unwrap(),
    );
    assert_lost_result_commit_contract(Arc::new(LostCommitResultBackend::new(inner))).await;
}

#[tokio::test]
#[ignore = "requires a fresh dedicated Redis endpoint in AFS_TEST_REDIS_LOST_RESULT_ENDPOINT"]
async fn dfs_commit_recovers_lost_store_result_with_redis_snapshot_backend() {
    let endpoint = std::env::var("AFS_TEST_REDIS_LOST_RESULT_ENDPOINT")
        .expect("AFS_TEST_REDIS_LOST_RESULT_ENDPOINT is required");
    let inner = Arc::new(
        afs::meta::store::redis::RedisBackend::connect(endpoint)
            .await
            .unwrap(),
    );
    assert_lost_result_commit_contract(Arc::new(LostCommitResultBackend::new(inner))).await;
}

#[tokio::test]
async fn dfs_placement_uses_global_live_devices_for_required_local_copy() {
    let (meta, dfs) = memory_meta_and_dfs_service(ReplicationConfig {
        desired_copies: 3,
        sync_required_copies: 2,
        min_distinct_nodes: 2,
        min_distinct_failure_domains: 2,
        local_copy: LocalCopyPolicy::Required,
    })
    .await;
    register_dfs_storage_node(&meta, "node-a", "session-a", "register-a", "rack-a").await;
    register_dfs_storage_node(&meta, "node-b", "session-b", "register-b", "rack-b").await;
    register_dfs_storage_node(&meta, "node-c", "session-c", "register-c", "rack-c").await;

    let first = dfs.placement_snapshot("node-a".into()).await.unwrap();
    assert!(first.revision > 0);
    assert!(!first.replica_groups.is_empty());
    for group in &first.replica_groups {
        assert_eq!(group.targets.len(), 3);
        assert_eq!(group.targets[0].node_id, "node-a");
        let nodes = group
            .targets
            .iter()
            .map(|target| target.node_id.as_str())
            .collect::<std::collections::HashSet<_>>();
        let domains = group
            .targets
            .iter()
            .map(|target| target.device.failure_domain.as_str())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(nodes.len(), 3);
        assert_eq!(domains.len(), 3);
        assert_ne!(group.placement_epoch, 0);
    }

    register_dfs_storage_node(&meta, "node-b", "session-b", "renew-b", "rack-b").await;
    let renewed = dfs.placement_snapshot("node-a".into()).await.unwrap();
    let first_groups = first
        .replica_groups
        .iter()
        .map(|group| {
            (
                group.id.clone(),
                group.placement_epoch,
                group.targets.clone(),
            )
        })
        .collect::<Vec<_>>();
    let renewed_groups = renewed
        .replica_groups
        .iter()
        .map(|group| {
            (
                group.id.clone(),
                group.placement_epoch,
                group.targets.clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(first_groups, renewed_groups);
    assert!(renewed.revision >= first.revision);
}

#[tokio::test]
async fn dfs_placement_allows_remote_head_when_local_copy_not_required() {
    let (meta, dfs) = memory_meta_and_dfs_service(ReplicationConfig {
        desired_copies: 2,
        sync_required_copies: 2,
        min_distinct_nodes: 2,
        min_distinct_failure_domains: 2,
        local_copy: LocalCopyPolicy::NotRequired,
    })
    .await;
    register_dfs_compute_node(&meta, "node-a", "session-a", "register-a-nr").await;
    register_dfs_storage_node(&meta, "node-b", "session-b", "register-b-nr", "rack-b").await;
    register_dfs_storage_node(&meta, "node-c", "session-c", "register-c-nr", "rack-c").await;

    let snapshot = dfs.placement_snapshot("node-a".into()).await.unwrap();
    assert!(snapshot.replica_groups.iter().all(|group| {
        group
            .targets
            .first()
            .is_some_and(|target| target.node_id != "node-a")
    }));
    for group in &snapshot.replica_groups {
        assert_eq!(group.targets.len(), 2);
        let nodes = group
            .targets
            .iter()
            .map(|target| target.node_id.as_str())
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(nodes.len(), 2);
    }
}

#[tokio::test]
async fn dfs_replica_write_grant_accepts_frozen_target_catalog_floor() {
    let (meta, dfs) = memory_meta_and_dfs_service(ReplicationConfig {
        desired_copies: 2,
        sync_required_copies: 2,
        min_distinct_nodes: 2,
        min_distinct_failure_domains: 2,
        local_copy: LocalCopyPolicy::Required,
    })
    .await;
    register_dfs_storage_node_with_catalog(
        &meta,
        "node-a",
        "session-a",
        "register-a-floor",
        "rack-a",
        5,
    )
    .await;
    register_dfs_storage_node_with_catalog(
        &meta,
        "node-b",
        "session-b",
        "register-b-floor",
        "rack-b",
        5,
    )
    .await;

    let frozen = dfs.placement_snapshot("node-a".into()).await.unwrap();
    let group = frozen.replica_groups[0].clone();
    let requester = group.targets[1].clone();
    let initiator = group.targets[0].clone();

    register_dfs_storage_node_with_catalog(
        &meta,
        "node-b",
        "session-b",
        "renew-b-floor",
        "rack-b",
        6,
    )
    .await;

    let grant = dfs
        .validate_replica_write(ValidateReplicaWriteRequest {
            requester_node_id: requester.node_id.clone(),
            requester_node_epoch: requester.node_epoch,
            initiator_node_id: initiator.node_id.clone(),
            initiator_node_epoch: initiator.node_epoch,
            operation_id: OperationId::new("write-floor"),
            chunk_id: ChunkId::new("chunk-floor"),
            chunk_length: 4096,
            content_digest: ContentDigest {
                algorithm: DigestAlgorithm::Blake3,
                bytes: [7; 32],
            },
            placement_revision: frozen.revision,
            placement_epoch: group.placement_epoch,
            replica_group_id: group.id.clone(),
            target_index: 1,
            ordered_targets: group.targets.clone(),
            repair_claim: None,
        })
        .await
        .unwrap();

    assert_eq!(grant.requester_node_id, "node-b");
    assert_eq!(grant.initiator_node_id, "node-a");
    assert_eq!(grant.replica_group, group);
    assert_eq!(grant.replica_group.targets[1].device.catalog_revision, 5);
    assert_eq!(
        dfs.placement_snapshot("node-a".into())
            .await
            .unwrap()
            .replica_groups[0]
            .targets[1]
            .device
            .catalog_revision,
        6
    );
    assert!(grant.fence >= frozen.revision);
}

#[tokio::test]
async fn dfs_commit_accepts_durable_ack_after_unrelated_catalog_advance() {
    let (meta, dfs_service) =
        memory_meta_and_dfs_service(ReplicationConfig::local_single_copy()).await;
    register_dfs_storage_node_with_catalog(
        &meta,
        "node-a",
        "session-a",
        "register-a-ack",
        "rack-a",
        5,
    )
    .await;
    let dfs = rpc::DfsMetaRpc(meta.0.clone());

    let placement = dfs_service
        .placement_snapshot("node-a".into())
        .await
        .unwrap();
    let group = placement.replica_groups[0].clone();
    let target = group.targets[0].clone();

    let created = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-old-ack".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"old-ack".to_vec(),
            attributes: Some(test_attrs(0o644, 1)),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner();
    let inode = created.inode.unwrap();
    let lease = created.write_lease.unwrap();

    register_dfs_storage_node_with_catalog(
        &meta,
        "node-a",
        "session-a",
        "renew-a-ack",
        "rack-a",
        6,
    )
    .await;

    let committed = dfs
        .commit_file_version(Request::new(CommitFileVersionRequest {
            caller_id: "node-a".into(),
            operation_id: "commit-old-ack".into(),
            inode_id: inode.inode_id.clone(),
            expected_inode_revision: inode.revision,
            expected_head_version_id: String::new(),
            version: Some(DfsFileVersion {
                version_id: "version-old-ack".into(),
                inode_id: inode.inode_id.clone(),
                parent_version_id: String::new(),
                length: 4096,
                layout_root_id: "layout-old-ack".into(),
                created_at_unix_ms: 10,
            }),
            layout: Some(DfsLayoutRoot {
                layout_root_id: "layout-old-ack".into(),
                file_length: 4096,
                inline_extents: vec![DfsExtent {
                    file_offset: 0,
                    length: 4096,
                    chunk_id: "chunk-old-ack".into(),
                    chunk_offset: 0,
                }],
            }),
            chunk_receipts: vec![DfsChunkReceipt {
                operation_id: "commit-old-ack".into(),
                chunk_id: "chunk-old-ack".into(),
                chunk_length: 4096,
                content_digest: vec![9; 32],
                content_digest_algorithm: afs_protocol::meta::DfsDigestAlgorithm::Blake3.into(),
                placement_revision: placement.revision,
                placement_epoch: group.placement_epoch,
                replica_group_id: group.id.0.clone(),
                durable_acks: vec![DfsReplicaAck {
                    operation_id: "commit-old-ack".into(),
                    chunk_id: "chunk-old-ack".into(),
                    placement_revision: placement.revision,
                    placement_epoch: group.placement_epoch,
                    node_id: target.node_id,
                    node_epoch: target.node_epoch,
                    device_id: target.device.device_id,
                    device_epoch: target.device.device_epoch,
                    catalog_revision: 5,
                    persisted_bytes: 4096,
                    verified_digest: vec![9; 32],
                    verified_digest_algorithm: afs_protocol::meta::DfsDigestAlgorithm::Blake3
                        .into(),
                }],
            }],
            write_lease: Some(lease),
            metadata_delta: Some(DfsCommitMetadataDelta {
                kill_suidgid: false,
                mode: DfsCommitMetadataMode::DataOnly.into(),
                mtime_unix_ms: 0,
                ctime_unix_ms: 0,
            }),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();

    assert_eq!(committed.head_version_id, "version-old-ack");
}

#[tokio::test]
async fn dfs_placement_rejects_unsatisfied_sync_minima() {
    let (meta, dfs) = memory_meta_and_dfs_service(ReplicationConfig {
        desired_copies: 3,
        sync_required_copies: 1,
        min_distinct_nodes: 3,
        min_distinct_failure_domains: 3,
        local_copy: LocalCopyPolicy::Preferred,
    })
    .await;
    register_dfs_storage_node(&meta, "node-a", "session-a", "register-a-short", "rack-a").await;
    register_dfs_storage_node(&meta, "node-b", "session-b", "register-b-short", "rack-b").await;

    let err = dfs.placement_snapshot("node-a".into()).await.unwrap_err();
    assert_eq!(afs::error::errno(&err), libc::EBUSY);
}

#[tokio::test]
async fn dfs_namespace_rpc_requires_caller_context_for_mutations() {
    let dfs = memory_dfs().await;
    let missing = dfs
        .mkdir(Request::new(DfsMkdirRequest {
            caller_id: "node-a".into(),
            operation_id: "mkdir-missing-caller".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"missing-caller".to_vec(),
            attributes: Some(test_attrs(0o755, 2)),
            caller: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(missing.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn dfs_namespace_mkdir_readdir_and_rmdir_update_directory_links() {
    let dfs = memory_dfs().await;
    let dir = dfs
        .mkdir(Request::new(DfsMkdirRequest {
            caller_id: "node-a".into(),
            operation_id: "mkdir-dir".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"dir".to_vec(),
            attributes: Some(test_attrs(0o755, 0)),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(dir.kind, afs_protocol::meta::DfsInodeKind::Directory as i32);
    assert_eq!(dir.attributes.as_ref().unwrap().nlink, 2);

    let child = dfs
        .mkdir(Request::new(DfsMkdirRequest {
            caller_id: "node-a".into(),
            operation_id: "mkdir-child".into(),
            namespace_id: "default".into(),
            parent_inode_id: dir.inode_id.clone(),
            name: b"child".to_vec(),
            attributes: Some(test_attrs(0o755, 0)),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let entries = dfs
        .read_dir(Request::new(DfsReadDirRequest {
            namespace_id: "default".into(),
            parent_inode_id: dir.inode_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .entries;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, b"child".to_vec());

    let non_empty = dfs
        .rmdir(Request::new(DfsRmdirRequest {
            caller_id: "node-a".into(),
            operation_id: "rmdir-dir-non-empty".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"dir".to_vec(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(non_empty), libc::ENOTEMPTY);

    let removed_child = dfs
        .rmdir(Request::new(DfsRmdirRequest {
            caller_id: "node-a".into(),
            operation_id: "rmdir-child".into(),
            namespace_id: "default".into(),
            parent_inode_id: dir.inode_id.clone(),
            name: b"child".to_vec(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(removed_child.inode_id, child.inode_id);
    assert_eq!(removed_child.attributes.as_ref().unwrap().nlink, 0);
    let replay = dfs
        .rmdir(Request::new(DfsRmdirRequest {
            caller_id: "node-a".into(),
            operation_id: "rmdir-child".into(),
            namespace_id: "default".into(),
            parent_inode_id: dir.inode_id.clone(),
            name: b"child".to_vec(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(replay.inode_id, child.inode_id);
}

#[tokio::test]
async fn dfs_namespace_unlink_removes_name_but_keeps_orphan_inode_for_open_handles() {
    let dfs = memory_dfs().await;
    let created = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-unlink".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"file.txt".to_vec(),
            attributes: Some(test_attrs(0o640, 1)),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let removed = dfs
        .unlink(Request::new(DfsUnlinkRequest {
            caller_id: "node-a".into(),
            operation_id: "unlink-file".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"file.txt".to_vec(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(removed.inode_id, created.inode_id);
    assert_eq!(removed.attributes.as_ref().unwrap().nlink, 0);
    let lookup = dfs
        .lookup(Request::new(DfsLookupRequest {
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"file.txt".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!lookup.found);
    let orphan = dfs
        .get_inode(Request::new(afs_protocol::meta::GetDfsInodeRequest {
            inode_id: created.inode_id,
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(orphan.attributes.unwrap().nlink, 0);
}

#[tokio::test]
async fn dfs_namespace_rename_no_replace_and_replace_are_atomic() {
    let dfs = memory_dfs().await;
    let src = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-src".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"src".to_vec(),
            attributes: Some(test_attrs(0o640, 1)),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let dst = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-dst".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"dst".to_vec(),
            attributes: Some(test_attrs(0o640, 1)),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let exists = dfs
        .rename(Request::new(DfsRenameRequest {
            caller_id: "node-a".into(),
            operation_id: "rename-no-replace-fails".into(),
            namespace_id: "default".into(),
            old_parent_inode_id: "1".into(),
            old_name: b"src".to_vec(),
            new_parent_inode_id: "1".into(),
            new_name: b"dst".to_vec(),
            mode: DfsRenameMode::NoReplace.into(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(exists), libc::EEXIST);

    let renamed = dfs
        .rename(Request::new(DfsRenameRequest {
            caller_id: "node-a".into(),
            operation_id: "rename-replace".into(),
            namespace_id: "default".into(),
            old_parent_inode_id: "1".into(),
            old_name: b"src".to_vec(),
            new_parent_inode_id: "1".into(),
            new_name: b"dst".to_vec(),
            mode: DfsRenameMode::Replace.into(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(renamed.replaced);
    assert_eq!(renamed.inode.unwrap().inode_id, src.inode_id);
    assert_eq!(renamed.replaced_inode.unwrap().inode_id, dst.inode_id);
    let src_lookup = dfs
        .lookup(Request::new(DfsLookupRequest {
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"src".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!src_lookup.found);
    let dst_lookup = dfs
        .lookup(Request::new(DfsLookupRequest {
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"dst".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(dst_lookup.inode.unwrap().inode_id, src.inode_id);
}

#[tokio::test]
async fn dfs_namespace_rename_rejects_directory_into_its_subtree() {
    let dfs = memory_dfs().await;
    let dir = dfs
        .mkdir(Request::new(DfsMkdirRequest {
            caller_id: "node-a".into(),
            operation_id: "mkdir-parent".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"parent".to_vec(),
            attributes: Some(test_attrs(0o755, 0)),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let child = dfs
        .mkdir(Request::new(DfsMkdirRequest {
            caller_id: "node-a".into(),
            operation_id: "mkdir-grandchild".into(),
            namespace_id: "default".into(),
            parent_inode_id: dir.inode_id.clone(),
            name: b"child".to_vec(),
            attributes: Some(test_attrs(0o755, 0)),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let err = dfs
        .rename(Request::new(DfsRenameRequest {
            caller_id: "node-a".into(),
            operation_id: "rename-cycle".into(),
            namespace_id: "default".into(),
            old_parent_inode_id: "1".into(),
            old_name: b"parent".to_vec(),
            new_parent_inode_id: child.inode_id,
            new_name: b"moved".to_vec(),
            mode: DfsRenameMode::NoReplace.into(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(err), libc::EINVAL);
}

#[tokio::test]
async fn dfs_namespace_errors_preserve_posix_errno() {
    let dfs = memory_dfs().await;
    let missing = dfs
        .unlink(Request::new(DfsUnlinkRequest {
            caller_id: "node-a".into(),
            operation_id: "unlink-missing".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"missing".to_vec(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(missing), libc::ENOENT);

    let dir = dfs
        .mkdir(Request::new(DfsMkdirRequest {
            caller_id: "node-a".into(),
            operation_id: "mkdir-errors-dir".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"dir".to_vec(),
            attributes: Some(test_attrs(0o755, 0)),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let unlink_dir = dfs
        .unlink(Request::new(DfsUnlinkRequest {
            caller_id: "node-a".into(),
            operation_id: "unlink-dir".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"dir".to_vec(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(unlink_dir), libc::EISDIR);

    let file = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-errors-file".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"file".to_vec(),
            attributes: Some(test_attrs(0o640, 1)),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let rmdir_file = dfs
        .rmdir(Request::new(DfsRmdirRequest {
            caller_id: "node-a".into(),
            operation_id: "rmdir-file".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"file".to_vec(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(rmdir_file), libc::ENOTDIR);

    let rename_dir_over_file = dfs
        .rename(Request::new(DfsRenameRequest {
            caller_id: "node-a".into(),
            operation_id: "rename-dir-over-file".into(),
            namespace_id: "default".into(),
            old_parent_inode_id: "1".into(),
            old_name: b"dir".to_vec(),
            new_parent_inode_id: "1".into(),
            new_name: b"file".to_vec(),
            mode: DfsRenameMode::Replace.into(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(rename_dir_over_file), libc::ENOTDIR);

    let rename_file_over_dir = dfs
        .rename(Request::new(DfsRenameRequest {
            caller_id: "node-a".into(),
            operation_id: "rename-file-over-dir".into(),
            namespace_id: "default".into(),
            old_parent_inode_id: "1".into(),
            old_name: b"file".to_vec(),
            new_parent_inode_id: "1".into(),
            new_name: b"dir".to_vec(),
            mode: DfsRenameMode::Replace.into(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(rename_file_over_dir), libc::EISDIR);

    let non_dir_parent = dfs
        .mkdir(Request::new(DfsMkdirRequest {
            caller_id: "node-a".into(),
            operation_id: "mkdir-under-file".into(),
            namespace_id: "default".into(),
            parent_inode_id: file.inode_id,
            name: b"child".to_vec(),
            attributes: Some(test_attrs(0o755, 0)),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(non_dir_parent), libc::ENOTDIR);

    let long_name = dfs
        .mkdir(Request::new(DfsMkdirRequest {
            caller_id: "node-a".into(),
            operation_id: "mkdir-long-name".into(),
            namespace_id: "default".into(),
            parent_inode_id: dir.inode_id,
            name: vec![b'x'; 256],
            attributes: Some(test_attrs(0o755, 0)),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(long_name), libc::ENAMETOOLONG);
}

#[tokio::test]
async fn dfs_namespace_hardlink_symlink_and_readlink_are_metadata_operations() {
    let dfs = memory_dfs().await;
    let file = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-link-source".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"source".to_vec(),
            attributes: Some(test_attrs(0o640, 1)),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();

    let linked = dfs
        .link(Request::new(DfsLinkRequest {
            caller_id: "node-a".into(),
            operation_id: "link-source".into(),
            namespace_id: "default".into(),
            existing_inode_id: file.inode_id.clone(),
            expected_inode_revision: file.revision,
            parent_inode_id: "1".into(),
            name: b"linked".to_vec(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(linked.inode_id, file.inode_id);
    assert_eq!(linked.attributes.as_ref().unwrap().nlink, 2);

    let lookup = dfs
        .lookup(Request::new(DfsLookupRequest {
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"linked".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(lookup.inode_id, file.inode_id);
    assert_eq!(lookup.attributes.as_ref().unwrap().nlink, 2);

    let target = b"../real-target".to_vec();
    let symlink = dfs
        .symlink(Request::new(DfsSymlinkRequest {
            caller_id: "node-a".into(),
            operation_id: "symlink-target".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"sym".to_vec(),
            target: target.clone(),
            attributes: Some(test_attrs(0o777, 1)),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(
        symlink.kind,
        afs_protocol::meta::DfsInodeKind::Symlink as i32
    );
    assert_eq!(symlink.symlink_target, target);
    assert!(symlink.head_version_id.is_empty());

    let readlink = dfs
        .read_link(Request::new(DfsReadLinkRequest {
            namespace_id: "default".into(),
            inode_id: symlink.inode_id,
        }))
        .await
        .unwrap()
        .into_inner()
        .target;
    assert_eq!(readlink, b"../real-target".to_vec());
}

#[tokio::test]
async fn dfs_namespace_attrs_and_user_xattrs_are_revision_fenced() {
    let dfs = memory_dfs().await;
    let file = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-attrs-file".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"attrs".to_vec(),
            attributes: Some(test_attrs(0o640, 1)),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();

    let changed = dfs
        .set_inode_attributes(Request::new(DfsSetInodeAttributesRequest {
            caller_id: "node-a".into(),
            operation_id: "chmod-attrs".into(),
            caller: Some(caller(1000, 1000)),
            inode_id: file.inode_id.clone(),
            expected_inode_revision: file.revision,
            update: Some(DfsInodeAttributeUpdate {
                timestamps_now: false,
                mode: Some(0o660),
                uid: None,
                gid: None,
                atime_unix_ms: None,
                mtime_unix_ms: Some(99),
                ctime_unix_ms: Some(123_456),
            }),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let changed_attrs = changed.attributes.as_ref().unwrap();
    assert_eq!(changed_attrs.mode, 0o660);
    assert_eq!(changed_attrs.mtime_unix_ms, 99);
    assert_ne!(changed_attrs.ctime_unix_ms, 123_456);
    assert!(changed.head_version_id.is_empty());
    assert_eq!(changed.revision, file.revision + 1);

    let stale = dfs
        .set_xattr(Request::new(DfsSetXattrRequest {
            caller_id: "node-a".into(),
            operation_id: "setxattr-stale".into(),
            caller: Some(caller(1000, 1000)),
            inode_id: file.inode_id.clone(),
            expected_inode_revision: file.revision,
            name: b"user.note".to_vec(),
            value: b"v".to_vec(),
            mode: DfsXattrSetMode::Create.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(stale), libc::EBUSY);

    let with_xattr = dfs
        .set_xattr(Request::new(DfsSetXattrRequest {
            caller_id: "node-a".into(),
            operation_id: "setxattr-note".into(),
            caller: Some(caller_with_groups(2000, 2000, &[2000, 1000])),
            inode_id: file.inode_id.clone(),
            expected_inode_revision: changed.revision,
            name: b"user.note".to_vec(),
            value: b"hello".to_vec(),
            mode: DfsXattrSetMode::Create.into(),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(with_xattr.xattrs.len(), 1);

    let value = dfs
        .get_xattr(Request::new(DfsGetXattrRequest {
            caller: Some(caller_with_groups(2000, 2000, &[2000, 1000])),
            inode_id: file.inode_id.clone(),
            name: b"user.note".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .value;
    assert_eq!(value, b"hello".to_vec());

    let read_denied = dfs
        .get_xattr(Request::new(DfsGetXattrRequest {
            caller: Some(caller(2001, 2001)),
            inode_id: file.inode_id.clone(),
            name: b"user.note".to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(read_denied), libc::EACCES);

    let write_denied = dfs
        .set_xattr(Request::new(DfsSetXattrRequest {
            caller_id: "node-a".into(),
            operation_id: "setxattr-denied".into(),
            caller: Some(caller(2001, 2001)),
            inode_id: file.inode_id.clone(),
            expected_inode_revision: with_xattr.revision,
            name: b"user.denied".to_vec(),
            value: b"v".to_vec(),
            mode: DfsXattrSetMode::Create.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(write_denied), libc::EACCES);

    let names = dfs
        .list_xattr(Request::new(DfsListXattrRequest {
            caller: Some(caller(1000, 1000)),
            inode_id: file.inode_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .names;
    assert_eq!(names, vec![b"user.note".to_vec()]);

    let create_existing = dfs
        .set_xattr(Request::new(DfsSetXattrRequest {
            caller_id: "node-a".into(),
            operation_id: "setxattr-existing".into(),
            caller: Some(caller(1000, 1000)),
            inode_id: file.inode_id.clone(),
            expected_inode_revision: with_xattr.revision,
            name: b"user.note".to_vec(),
            value: b"again".to_vec(),
            mode: DfsXattrSetMode::Create.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(create_existing), libc::EEXIST);

    let unsupported = dfs
        .set_xattr(Request::new(DfsSetXattrRequest {
            caller_id: "node-a".into(),
            operation_id: "setxattr-security".into(),
            caller: Some(caller(1000, 1000)),
            inode_id: file.inode_id.clone(),
            expected_inode_revision: with_xattr.revision,
            name: b"security.note".to_vec(),
            value: b"v".to_vec(),
            mode: DfsXattrSetMode::Upsert.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(unsupported), libc::EOPNOTSUPP);

    let symlink = dfs
        .symlink(Request::new(DfsSymlinkRequest {
            caller_id: "node-a".into(),
            operation_id: "symlink-no-user-xattr".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"link-no-xattr".to_vec(),
            target: b"attrs".to_vec(),
            attributes: Some(test_attrs(0o777, 1)),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let symlink_xattr = dfs
        .set_xattr(Request::new(DfsSetXattrRequest {
            caller_id: "node-a".into(),
            operation_id: "setxattr-symlink".into(),
            caller: Some(caller(1000, 1000)),
            inode_id: symlink.inode_id,
            expected_inode_revision: symlink.revision,
            name: b"user.note".to_vec(),
            value: b"v".to_vec(),
            mode: DfsXattrSetMode::Create.into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(symlink_xattr), libc::EPERM);

    let removed = dfs
        .remove_xattr(Request::new(DfsRemoveXattrRequest {
            caller_id: "node-a".into(),
            operation_id: "removexattr-note".into(),
            caller: Some(caller(1000, 1000)),
            inode_id: file.inode_id.clone(),
            expected_inode_revision: with_xattr.revision,
            name: b"user.note".to_vec(),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let missing = dfs
        .get_xattr(Request::new(DfsGetXattrRequest {
            caller: Some(caller(1000, 1000)),
            inode_id: file.inode_id.clone(),
            name: b"user.note".to_vec(),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(missing), libc::ENODATA);

    let chgrp = dfs
        .set_inode_attributes(Request::new(DfsSetInodeAttributesRequest {
            caller_id: "node-a".into(),
            operation_id: "chgrp-owner-supplementary".into(),
            caller: Some(caller_with_groups(1000, 1000, &[1000, 2000])),
            inode_id: file.inode_id.clone(),
            expected_inode_revision: removed.revision,
            update: Some(DfsInodeAttributeUpdate {
                timestamps_now: false,
                mode: Some(0o2660),
                uid: None,
                gid: Some(2000),
                atime_unix_ms: None,
                mtime_unix_ms: None,
                ctime_unix_ms: None,
            }),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let chgrp_attrs = chgrp.attributes.as_ref().unwrap();
    assert_eq!(chgrp_attrs.gid, 2000);
    assert_eq!(chgrp_attrs.mode & 0o6000, 0);

    let unchanged_owner = dfs
        .set_inode_attributes(Request::new(DfsSetInodeAttributesRequest {
            caller_id: "node-a".into(),
            operation_id: "chown-owner-noop".into(),
            // The owner may preserve its uid and the existing gid, even
            // after its supplementary group membership changes.
            caller: Some(caller(1000, 1000)),
            inode_id: file.inode_id.clone(),
            expected_inode_revision: chgrp.revision,
            update: Some(DfsInodeAttributeUpdate {
                uid: Some(1000),
                gid: Some(2000),
                ..Default::default()
            }),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(unchanged_owner.attributes.as_ref().unwrap().uid, 1000);
    assert_eq!(unchanged_owner.attributes.as_ref().unwrap().gid, 2000);

    let chown = dfs
        .set_inode_attributes(Request::new(DfsSetInodeAttributesRequest {
            caller_id: "node-a".into(),
            operation_id: "chown-not-root".into(),
            caller: Some(caller(1000, 1000)),
            inode_id: file.inode_id,
            expected_inode_revision: unchanged_owner.revision,
            update: Some(DfsInodeAttributeUpdate {
                timestamps_now: false,
                mode: None,
                uid: Some(1001),
                gid: None,
                atime_unix_ms: None,
                mtime_unix_ms: None,
                ctime_unix_ms: None,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(status_errno(chown), libc::EPERM);
}

#[tokio::test]
async fn dfs_readonly_owner_can_set_times_and_directory_chown_preserves_special_bits() {
    let dfs = memory_dfs().await;
    let file = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-readonly-times".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"readonly-times".to_vec(),
            attributes: Some(test_attrs(0o444, 1)),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let timed = dfs
        .set_inode_attributes(Request::new(DfsSetInodeAttributesRequest {
            caller_id: "node-a".into(),
            operation_id: "times-owner-without-write".into(),
            caller: Some(caller(1000, 1000)),
            inode_id: file.inode_id,
            expected_inode_revision: file.revision,
            update: Some(DfsInodeAttributeUpdate {
                atime_unix_ms: Some(10),
                mtime_unix_ms: Some(20),
                ..Default::default()
            }),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(timed.attributes.as_ref().unwrap().atime_unix_ms, 10);
    assert_eq!(timed.attributes.as_ref().unwrap().mtime_unix_ms, 20);

    let writable = dfs
        .set_inode_attributes(Request::new(DfsSetInodeAttributesRequest {
            caller_id: "node-a".into(),
            operation_id: "times-world-writable".into(),
            caller: Some(caller(0, 0)),
            inode_id: timed.inode_id.clone(),
            expected_inode_revision: timed.revision,
            update: Some(DfsInodeAttributeUpdate {
                mode: Some(0o666),
                ..Default::default()
            }),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let specific = DfsSetInodeAttributesRequest {
        caller_id: "node-a".into(),
        operation_id: "times-nonowner-specific".into(),
        caller: Some(caller(2000, 2000)),
        inode_id: writable.inode_id.clone(),
        expected_inode_revision: writable.revision,
        update: Some(DfsInodeAttributeUpdate {
            atime_unix_ms: Some(30),
            mtime_unix_ms: Some(30),
            ..Default::default()
        }),
    };
    assert_eq!(
        status_errno(
            dfs.set_inode_attributes(Request::new(specific.clone()))
                .await
                .unwrap_err()
        ),
        libc::EPERM
    );
    let mut now = specific;
    now.operation_id = "times-nonowner-now".into();
    now.update.as_mut().unwrap().timestamps_now = true;
    let now = dfs
        .set_inode_attributes(Request::new(now))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let now_attrs = now.attributes.as_ref().unwrap();
    assert!(now_attrs.mtime_unix_ms > 30);
    assert_eq!(now_attrs.atime_unix_ms, now_attrs.mtime_unix_ms);
    assert_eq!(now_attrs.ctime_unix_ms, now_attrs.mtime_unix_ms);

    let directory = dfs
        .mkdir(Request::new(DfsMkdirRequest {
            caller_id: "node-a".into(),
            operation_id: "mkdir-chown-special".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"chown-special".to_vec(),
            attributes: Some(test_attrs(0o6777, 2)),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    let changed = dfs
        .set_inode_attributes(Request::new(DfsSetInodeAttributesRequest {
            caller_id: "node-a".into(),
            operation_id: "chown-directory-special".into(),
            caller: Some(caller(0, 0)),
            inode_id: directory.inode_id,
            expected_inode_revision: directory.revision,
            update: Some(DfsInodeAttributeUpdate {
                uid: Some(2000),
                gid: Some(2000),
                ..Default::default()
            }),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(changed.attributes.as_ref().unwrap().mode & 0o6000, 0o6000);
}

#[tokio::test]
async fn dfs_data_commit_allows_metadata_revision_drift_without_losing_attrs() {
    let dfs = memory_dfs().await;
    let created = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-cas-drift".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"cas".to_vec(),
            attributes: Some(test_attrs(0o640, 1)),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner();
    let initial = created.inode.unwrap();
    let lease = created.write_lease.unwrap();

    let with_xattr = dfs
        .set_xattr(Request::new(DfsSetXattrRequest {
            caller_id: "node-a".into(),
            operation_id: "setxattr-cas-drift".into(),
            caller: Some(caller(1000, 1000)),
            inode_id: initial.inode_id.clone(),
            expected_inode_revision: initial.revision,
            name: b"user.note".to_vec(),
            value: b"keep".to_vec(),
            mode: DfsXattrSetMode::Create.into(),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(with_xattr.revision, initial.revision + 1);

    let linked = dfs
        .link(Request::new(DfsLinkRequest {
            caller_id: "node-a".into(),
            operation_id: "link-cas-drift".into(),
            namespace_id: "default".into(),
            existing_inode_id: initial.inode_id.clone(),
            expected_inode_revision: with_xattr.revision,
            parent_inode_id: "1".into(),
            name: b"cas-linked".to_vec(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(linked.revision, with_xattr.revision + 1);
    assert_eq!(linked.attributes.as_ref().unwrap().nlink, 2);

    let renamed = dfs
        .rename(Request::new(DfsRenameRequest {
            caller_id: "node-a".into(),
            operation_id: "rename-cas-drift".into(),
            namespace_id: "default".into(),
            old_parent_inode_id: "1".into(),
            old_name: b"cas".to_vec(),
            new_parent_inode_id: "1".into(),
            new_name: b"cas-renamed".to_vec(),
            mode: DfsRenameMode::NoReplace.into(),
            caller: Some(caller(0, 0)),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(renamed.inode_id, initial.inode_id);
    assert_eq!(renamed.revision, linked.revision + 1);

    let commit_request = CommitFileVersionRequest {
        caller_id: "node-a".into(),
        operation_id: "commit-cas-drift".into(),
        inode_id: initial.inode_id.clone(),
        expected_inode_revision: initial.revision,
        expected_head_version_id: String::new(),
        version: Some(DfsFileVersion {
            version_id: "version-cas-drift".into(),
            inode_id: initial.inode_id.clone(),
            parent_version_id: String::new(),
            length: 0,
            layout_root_id: "layout-cas-drift".into(),
            created_at_unix_ms: 11,
        }),
        layout: Some(DfsLayoutRoot {
            layout_root_id: "layout-cas-drift".into(),
            file_length: 0,
            inline_extents: Vec::new(),
        }),
        chunk_receipts: Vec::new(),
        write_lease: Some(lease.clone()),
        metadata_delta: Some(DfsCommitMetadataDelta {
            kill_suidgid: false,
            mode: DfsCommitMetadataMode::DataOnly.into(),
            mtime_unix_ms: 0,
            ctime_unix_ms: 0,
        }),
    };
    let committed = dfs
        .commit_file_version(Request::new(commit_request.clone()))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(committed.revision, renamed.revision + 1);
    assert_eq!(committed.head_version_id, "version-cas-drift");
    assert_eq!(committed.attributes.as_ref().unwrap().nlink, 2);
    assert_eq!(committed.xattrs.len(), 1);
    assert_eq!(committed.xattrs[0].name, b"user.note".to_vec());
    assert_eq!(committed.xattrs[0].value, b"keep".to_vec());

    let replay = dfs
        .commit_file_version(Request::new(commit_request.clone()))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(replay, committed);

    let committed_version = dfs
        .get_file_version(Request::new(afs_protocol::meta::GetFileVersionRequest {
            version_id: "version-cas-drift".into(),
        }))
        .await
        .unwrap()
        .into_inner();
    let outcome_before_bad_replay = dfs
        .0
        .store
        .as_ref()
        .unwrap()
        .read(MetaRead::RequestOutcome(RequestKey::new(
            "node-a",
            "commit-cas-drift",
        )))
        .await
        .unwrap()
        .request_outcome;
    let mut sparse_replay = commit_request.clone();
    sparse_replay.version.as_mut().unwrap().length = 1;
    sparse_replay.layout.as_mut().unwrap().file_length = 1;
    let sparse_replay_error = dfs
        .commit_file_version(Request::new(sparse_replay))
        .await
        .unwrap_err();
    assert_eq!(sparse_replay_error.code(), Code::InvalidArgument);
    let inode_after_bad_replay = dfs
        .get_inode(Request::new(afs_protocol::meta::GetDfsInodeRequest {
            inode_id: initial.inode_id.clone(),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(inode_after_bad_replay.head_version_id, "version-cas-drift");
    assert_eq!(inode_after_bad_replay.revision, committed.revision);
    assert_eq!(
        dfs.get_file_version(Request::new(afs_protocol::meta::GetFileVersionRequest {
            version_id: "version-cas-drift".into(),
        }))
        .await
        .unwrap()
        .into_inner(),
        committed_version
    );
    assert_eq!(
        dfs.0
            .store
            .as_ref()
            .unwrap()
            .read(MetaRead::RequestOutcome(RequestKey::new(
                "node-a",
                "commit-cas-drift",
            )))
            .await
            .unwrap()
            .request_outcome,
        outcome_before_bad_replay
    );

    let old_head = dfs
        .commit_file_version(Request::new(CommitFileVersionRequest {
            caller_id: "node-a".into(),
            operation_id: "commit-cas-old-head".into(),
            inode_id: initial.inode_id.clone(),
            expected_inode_revision: initial.revision,
            expected_head_version_id: String::new(),
            version: Some(DfsFileVersion {
                version_id: "version-cas-old-head".into(),
                inode_id: initial.inode_id.clone(),
                parent_version_id: String::new(),
                length: 0,
                layout_root_id: "layout-cas-old-head".into(),
                created_at_unix_ms: 12,
            }),
            layout: Some(DfsLayoutRoot {
                layout_root_id: "layout-cas-old-head".into(),
                file_length: 0,
                inline_extents: Vec::new(),
            }),
            chunk_receipts: Vec::new(),
            write_lease: Some(lease.clone()),
            metadata_delta: Some(DfsCommitMetadataDelta {
                kill_suidgid: false,
                mode: DfsCommitMetadataMode::DataOnly.into(),
                mtime_unix_ms: 0,
                ctime_unix_ms: 0,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(old_head.code(), Code::FailedPrecondition);

    let future_revision = dfs
        .commit_file_version(Request::new(CommitFileVersionRequest {
            caller_id: "node-a".into(),
            operation_id: "commit-cas-future-revision".into(),
            inode_id: initial.inode_id,
            expected_inode_revision: committed.revision + 1,
            expected_head_version_id: "version-cas-drift".into(),
            version: Some(DfsFileVersion {
                version_id: "version-cas-future-revision".into(),
                inode_id: committed.inode_id,
                parent_version_id: "version-cas-drift".into(),
                length: 0,
                layout_root_id: "layout-cas-future-revision".into(),
                created_at_unix_ms: 13,
            }),
            layout: Some(DfsLayoutRoot {
                layout_root_id: "layout-cas-future-revision".into(),
                file_length: 0,
                inline_extents: Vec::new(),
            }),
            chunk_receipts: Vec::new(),
            write_lease: Some(lease),
            metadata_delta: Some(DfsCommitMetadataDelta {
                kill_suidgid: false,
                mode: DfsCommitMetadataMode::DataOnly.into(),
                mtime_unix_ms: 0,
                ctime_unix_ms: 0,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(future_revision.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn dfs_metadata_sync_allows_metadata_revision_drift_without_losing_xattrs() {
    let dfs = memory_dfs().await;
    let created = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-sync-drift".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"sync-drift".to_vec(),
            attributes: Some(test_attrs(0o640, 1)),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner();
    let initial = created.inode.unwrap();
    let lease = created.write_lease.unwrap();

    let with_xattr = dfs
        .set_xattr(Request::new(DfsSetXattrRequest {
            caller_id: "node-a".into(),
            operation_id: "setxattr-sync-drift".into(),
            caller: Some(caller(1000, 1000)),
            inode_id: initial.inode_id.clone(),
            expected_inode_revision: initial.revision,
            name: b"user.sync".to_vec(),
            value: b"keep-sync".to_vec(),
            mode: DfsXattrSetMode::Create.into(),
        }))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(with_xattr.revision, initial.revision + 1);

    let synced = dfs
        .sync_inode_metadata(Request::new(
            afs_protocol::meta::SyncDfsInodeMetadataRequest {
                caller_id: "node-a".into(),
                operation_id: "sync-drift".into(),
                inode_id: initial.inode_id.clone(),
                write_lease: Some(lease),
                expected_inode_revision: initial.revision,
                expected_head_version_id: String::new(),
                metadata_delta: Some(DfsCommitMetadataDelta {
                    kill_suidgid: false,
                    mode: DfsCommitMetadataMode::Full.into(),
                    mtime_unix_ms: 77,
                    ctime_unix_ms: 88,
                }),
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(synced.revision, with_xattr.revision + 1);
    assert_eq!(synced.attributes.as_ref().unwrap().mtime_unix_ms, 77);
    assert_eq!(synced.xattrs.len(), 1);
    assert_eq!(synced.xattrs[0].name, b"user.sync".to_vec());
    assert_eq!(synced.xattrs[0].value, b"keep-sync".to_vec());
}

#[tokio::test]
async fn dfs_write_lease_reuses_one_owner_and_fences_stale_commits() {
    let store = Arc::new(
        Store::open(Arc::new(MemoryBackend::default()))
            .await
            .unwrap(),
    );
    let meta = Arc::new(Meta::with_store(
        "meta-test".into(),
        Observability::new().unwrap(),
        store,
    ));
    let dfs = rpc::DfsMetaRpc(meta);
    let created = dfs
        .create(Request::new(DfsCreateRequest {
            caller_id: "node-a".into(),
            operation_id: "create-a".into(),
            namespace_id: "default".into(),
            parent_inode_id: "1".into(),
            name: b"lease.txt".to_vec(),
            attributes: Some(DfsInodeAttributes {
                mode: 0o640,
                uid: 1000,
                gid: 1000,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            }),
            owner_session_id: "session-a".into(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner();
    let inode = created.inode.unwrap();
    let lease = created.write_lease.unwrap();
    assert_eq!(lease.owner_node_id, "node-a");
    assert_eq!(lease.owner_session_id, "session-a");
    assert_eq!(lease.lease_epoch, 1);

    let same_owner = dfs
        .open_write(Request::new(OpenDfsWriteRequest {
            caller_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            operation_id: "open-a".into(),
            inode_id: inode.inode_id.clone(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner()
        .write_lease
        .unwrap();
    assert_eq!(same_owner.lease_epoch, 1);

    let routed_owner = dfs
        .open_write(Request::new(OpenDfsWriteRequest {
            caller_id: "node-b".into(),
            owner_session_id: "session-b".into(),
            operation_id: "open-b".into(),
            inode_id: inode.inode_id.clone(),
            lease_seconds: 30,
        }))
        .await
        .unwrap()
        .into_inner()
        .write_lease
        .unwrap();
    assert_eq!(routed_owner.owner_node_id, "node-a");
    assert_eq!(routed_owner.owner_session_id, "session-a");
    assert_eq!(routed_owner.lease_epoch, 1);

    let stale = dfs
        .commit_file_version(Request::new(CommitFileVersionRequest {
            caller_id: "node-a".into(),
            operation_id: "commit-stale".into(),
            inode_id: inode.inode_id.clone(),
            expected_inode_revision: inode.revision,
            expected_head_version_id: String::new(),
            version: Some(DfsFileVersion {
                version_id: "version-stale".into(),
                inode_id: inode.inode_id.clone(),
                parent_version_id: String::new(),
                length: 0,
                layout_root_id: "layout-stale".into(),
                created_at_unix_ms: 2,
            }),
            layout: Some(DfsLayoutRoot {
                layout_root_id: "layout-stale".into(),
                file_length: 0,
                inline_extents: Vec::new(),
            }),
            chunk_receipts: Vec::new(),
            write_lease: Some(DfsWriteLease {
                lease_epoch: 0,
                ..same_owner.clone()
            }),
            metadata_delta: Some(DfsCommitMetadataDelta {
                kill_suidgid: false,
                mode: DfsCommitMetadataMode::DataOnly.into(),
                mtime_unix_ms: 0,
                ctime_unix_ms: 0,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::FailedPrecondition);

    let synced = dfs
        .sync_inode_metadata(Request::new(
            afs_protocol::meta::SyncDfsInodeMetadataRequest {
                caller_id: "node-a".into(),
                operation_id: "sync-metadata-a".into(),
                inode_id: inode.inode_id,
                write_lease: Some(same_owner),
                expected_inode_revision: inode.revision,
                expected_head_version_id: String::new(),
                metadata_delta: Some(DfsCommitMetadataDelta {
                    kill_suidgid: false,
                    mode: DfsCommitMetadataMode::Full.into(),
                    mtime_unix_ms: 9,
                    ctime_unix_ms: 10,
                }),
            },
        ))
        .await
        .unwrap()
        .into_inner()
        .inode
        .unwrap();
    assert_eq!(synced.revision, 2);
    assert!(synced.head_version_id.is_empty());
    assert_eq!(synced.attributes.unwrap().mtime_unix_ms, 9);
}

#[tokio::test]
async fn dfs_namespace_mutations_update_parent_times_and_replay_is_stable() {
    use afs::dfs::{InodeAttributes, InodeId, MkdirRequest, NamespaceId, UnlinkRequest};
    use afs::meta::dfs::CreateFileRequest;
    let (_, dfs) = memory_meta_and_dfs_service(ReplicationConfig::local_single_copy()).await;
    let attrs = InodeAttributes {
        mode: 0o755,
        uid: 1000,
        gid: 1000,
        nlink: 2,
        atime_unix_ms: 1,
        mtime_unix_ms: 1,
        ctime_unix_ms: 1,
    };
    let dir = dfs
        .mkdir(MkdirRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("time-dir"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"time-dir".to_vec(),
            attributes: attrs.clone(),
            caller: domain_caller(0, 0),
        })
        .await
        .unwrap();
    let root = dfs.get_inode(InodeId::new("1")).await.unwrap();
    assert_eq!(root.attributes.nlink, 3);
    assert!(root.attributes.mtime_unix_ms > 1);
    assert_eq!(root.attributes.mtime_unix_ms, root.attributes.ctime_unix_ms);

    let request = CreateFileRequest {
        caller_id: "node-a".into(),
        owner_session_id: "session-a".into(),
        operation_id: OperationId::new("time-file"),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: dir.inode_id.clone(),
        name: b"file".to_vec(),
        attributes: InodeAttributes {
            nlink: 1,
            ..attrs.clone()
        },
        lease_seconds: 30,
    };
    dfs.create(request.clone()).await.unwrap();
    let updated = dfs.get_inode(dir.inode_id.clone()).await.unwrap();
    assert!(updated.attributes.mtime_unix_ms > 1);
    assert_eq!(
        updated.attributes.mtime_unix_ms,
        updated.attributes.ctime_unix_ms
    );
    assert_eq!(updated.attributes.nlink, 2);
    dfs.create(request).await.unwrap();
    assert_eq!(
        updated,
        dfs.get_inode(dir.inode_id.clone()).await.unwrap(),
        "replay must not retouch parent"
    );
    dfs.unlink(UnlinkRequest {
        caller_id: "node-a".into(),
        operation_id: OperationId::new("time-unlink"),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: dir.inode_id.clone(),
        name: b"file".to_vec(),
        caller: domain_caller(0, 0),
    })
    .await
    .unwrap();
    let removed = dfs.get_inode(dir.inode_id).await.unwrap();
    assert_eq!(removed.revision, updated.revision + 1);
    assert!(removed.attributes.mtime_unix_ms >= updated.attributes.mtime_unix_ms);
    assert_eq!(
        removed.attributes.mtime_unix_ms,
        removed.attributes.ctime_unix_ms
    );
}

#[tokio::test]
async fn dfs_namespace_root_rejects_aliasing_another_namespace() {
    use afs::dfs::{InodeAttributes, InodeId, MkdirRequest, NamespaceId};
    let (_, dfs) = memory_meta_and_dfs_service(ReplicationConfig::local_single_copy()).await;
    let request = MkdirRequest {
        caller_id: "node-a".into(),
        operation_id: OperationId::new("root-first"),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: InodeId::new("1"),
        name: b"dir".to_vec(),
        attributes: InodeAttributes {
            mode: 0o755,
            uid: 0,
            gid: 0,
            nlink: 2,
            atime_unix_ms: 1,
            mtime_unix_ms: 1,
            ctime_unix_ms: 1,
        },
        caller: domain_caller(0, 0),
    };
    dfs.mkdir(request.clone()).await.unwrap();
    let root = dfs.get_inode(InodeId::new("1")).await.unwrap();
    let mut other = request;
    other.namespace_id = NamespaceId::new("other");
    other.operation_id = OperationId::new("root-second");
    assert!(dfs.mkdir(other).await.is_err());
    assert_eq!(root, dfs.get_inode(InodeId::new("1")).await.unwrap());
}

#[tokio::test]
async fn dfs_rename_preserves_other_hardlinks_and_same_inode_is_noop() {
    use afs::dfs::{InodeAttributes, InodeId, LinkRequest, NamespaceId, RenameMode, RenameRequest};
    use afs::meta::dfs::CreateFileRequest;
    let (_, dfs) = memory_meta_and_dfs_service(ReplicationConfig::local_single_copy()).await;
    let mut files = Vec::new();
    for name in ["source", "target"] {
        let (inode, _) = dfs
            .create(CreateFileRequest {
                caller_id: "node-a".into(),
                owner_session_id: "session-a".into(),
                operation_id: OperationId::new(format!("hardlink-{name}")),
                namespace_id: NamespaceId::new("default"),
                parent_inode_id: InodeId::new("1"),
                name: name.as_bytes().to_vec(),
                attributes: InodeAttributes {
                    mode: 0o644,
                    uid: 0,
                    gid: 0,
                    nlink: 1,
                    atime_unix_ms: 1,
                    mtime_unix_ms: 1,
                    ctime_unix_ms: 1,
                },
                lease_seconds: 30,
            })
            .await
            .unwrap();
        files.push(inode);
    }
    let target = dfs
        .link(LinkRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("target-alias"),
            namespace_id: NamespaceId::new("default"),
            existing_inode_id: files[1].inode_id.clone(),
            expected_inode_revision: files[1].revision,
            parent_inode_id: InodeId::new("1"),
            name: b"alias".to_vec(),
            caller: domain_caller(0, 0),
        })
        .await
        .unwrap();
    assert_eq!(target.attributes.nlink, 2);
    let no_op = dfs
        .rename(RenameRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("same-inode-rename"),
            namespace_id: NamespaceId::new("default"),
            old_parent_inode_id: InodeId::new("1"),
            old_name: b"alias".to_vec(),
            new_parent_inode_id: InodeId::new("1"),
            new_name: b"target".to_vec(),
            mode: RenameMode::Replace,
            caller: domain_caller(0, 0),
        })
        .await
        .unwrap();
    assert_eq!(no_op.inode, target);
    assert!(
        dfs.lookup(
            NamespaceId::new("default"),
            InodeId::new("1"),
            b"alias".to_vec()
        )
        .await
        .unwrap()
        .is_some()
    );
    dfs.rename(RenameRequest {
        caller_id: "node-a".into(),
        operation_id: OperationId::new("replace-one-link"),
        namespace_id: NamespaceId::new("default"),
        old_parent_inode_id: InodeId::new("1"),
        old_name: b"source".to_vec(),
        new_parent_inode_id: InodeId::new("1"),
        new_name: b"target".to_vec(),
        mode: RenameMode::Replace,
        caller: domain_caller(0, 0),
    })
    .await
    .unwrap();
    let remaining = dfs
        .lookup(
            NamespaceId::new("default"),
            InodeId::new("1"),
            b"alias".to_vec(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(remaining.attributes.nlink, 1);
    assert_eq!(remaining.inode_id, target.inode_id);
}

#[tokio::test]
async fn dfs_namespace_permissions_require_parent_write_and_search() {
    use afs::dfs::{
        InodeAttributes, InodeId, LinkRequest, MkdirRequest, NamespaceId, RmdirRequest,
        SymlinkRequest, UnlinkRequest,
    };
    use afs::meta::dfs::CreateFileRequest;
    let (_, dfs) = memory_meta_and_dfs_service(ReplicationConfig::local_single_copy()).await;
    let attrs = |mode, uid, gid, nlink| InodeAttributes {
        mode,
        uid,
        gid,
        nlink,
        atime_unix_ms: 1,
        mtime_unix_ms: 1,
        ctime_unix_ms: 1,
    };
    let locked = dfs
        .mkdir(MkdirRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("perm-locked-dir"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"locked".to_vec(),
            attributes: attrs(0o700, 0, 0, 2),
            caller: domain_caller(0, 0),
        })
        .await
        .unwrap();
    let (inside, _) = dfs
        .create(CreateFileRequest {
            caller_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            operation_id: OperationId::new("perm-inside-file"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: locked.inode_id.clone(),
            name: b"inside".to_vec(),
            attributes: attrs(0o644, 0, 0, 1),
            lease_seconds: 30,
        })
        .await
        .unwrap();
    let (root_file, _) = dfs
        .create(CreateFileRequest {
            caller_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            operation_id: OperationId::new("perm-root-file"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"root-file".to_vec(),
            attributes: attrs(0o644, 0, 0, 1),
            lease_seconds: 30,
        })
        .await
        .unwrap();
    assert_eq!(
        dfs.mkdir(MkdirRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("perm-denied-mkdir"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: locked.inode_id.clone(),
            name: b"child".to_vec(),
            attributes: attrs(0o755, 1000, 1000, 2),
            caller: domain_caller(1000, 1000),
        })
        .await
        .unwrap_err()
        .code(),
        afs_error::IO_PERMISSION_DENIED
    );
    assert_eq!(
        dfs.link(LinkRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("perm-denied-link"),
            namespace_id: NamespaceId::new("default"),
            existing_inode_id: root_file.inode_id,
            expected_inode_revision: root_file.revision,
            parent_inode_id: locked.inode_id.clone(),
            name: b"linked".to_vec(),
            caller: domain_caller(1000, 1000),
        })
        .await
        .unwrap_err()
        .code(),
        afs_error::IO_PERMISSION_DENIED
    );
    assert_eq!(
        dfs.symlink(SymlinkRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("perm-denied-symlink"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: locked.inode_id.clone(),
            name: b"sym".to_vec(),
            target: b"target".to_vec(),
            attributes: attrs(libc::S_IFLNK | 0o777, 1000, 1000, 1),
            caller: domain_caller(1000, 1000),
        })
        .await
        .unwrap_err()
        .code(),
        afs_error::IO_PERMISSION_DENIED
    );
    assert_eq!(
        dfs.unlink(UnlinkRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("perm-denied-unlink"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: locked.inode_id.clone(),
            name: b"inside".to_vec(),
            caller: domain_caller(1000, 1000),
        })
        .await
        .unwrap_err()
        .code(),
        afs_error::IO_PERMISSION_DENIED
    );
    assert_eq!(
        dfs.rmdir(RmdirRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("perm-denied-rmdir"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: locked.inode_id,
            name: b"missing-dir".to_vec(),
            caller: domain_caller(1000, 1000),
        })
        .await
        .unwrap_err()
        .code(),
        afs_error::IO_PERMISSION_DENIED
    );
    assert_eq!(inside.attributes.nlink, 1);
}

#[tokio::test]
async fn dfs_namespace_sticky_directories_require_victim_or_directory_owner() {
    use afs::dfs::{
        InodeAttributes, InodeId, MkdirRequest, NamespaceId, RenameMode, RenameRequest,
        UnlinkRequest,
    };
    use afs::meta::dfs::CreateFileRequest;
    let (_, dfs) = memory_meta_and_dfs_service(ReplicationConfig::local_single_copy()).await;
    let attrs = |mode, uid, gid, nlink| InodeAttributes {
        mode,
        uid,
        gid,
        nlink,
        atime_unix_ms: 1,
        mtime_unix_ms: 1,
        ctime_unix_ms: 1,
    };
    let sticky = dfs
        .mkdir(MkdirRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("sticky-dir"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"sticky".to_vec(),
            attributes: attrs(0o1777, 0, 0, 2),
            caller: domain_caller(0, 0),
        })
        .await
        .unwrap();
    let (victim, _) = dfs
        .create(CreateFileRequest {
            caller_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            operation_id: OperationId::new("sticky-victim"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: sticky.inode_id.clone(),
            name: b"victim".to_vec(),
            attributes: attrs(0o644, 2000, 2000, 1),
            lease_seconds: 30,
        })
        .await
        .unwrap();
    assert_eq!(
        dfs.unlink(UnlinkRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("sticky-unlink-denied"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: sticky.inode_id.clone(),
            name: b"victim".to_vec(),
            caller: domain_caller(3000, 3000),
        })
        .await
        .unwrap_err()
        .code(),
        afs_error::IO_PERMISSION_DENIED
    );
    let (source, _) = dfs
        .create(CreateFileRequest {
            caller_id: "node-a".into(),
            owner_session_id: "session-a".into(),
            operation_id: OperationId::new("sticky-source"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: sticky.inode_id.clone(),
            name: b"source".to_vec(),
            attributes: attrs(0o644, 3000, 3000, 1),
            lease_seconds: 30,
        })
        .await
        .unwrap();
    assert_eq!(
        dfs.rename(RenameRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("sticky-replace-denied"),
            namespace_id: NamespaceId::new("default"),
            old_parent_inode_id: sticky.inode_id.clone(),
            old_name: b"source".to_vec(),
            new_parent_inode_id: sticky.inode_id.clone(),
            new_name: b"victim".to_vec(),
            mode: RenameMode::Replace,
            caller: domain_caller(3000, 3000),
        })
        .await
        .unwrap_err()
        .code(),
        afs_error::IO_PERMISSION_DENIED
    );
    assert_eq!(victim.attributes.uid, 2000);
    assert_eq!(source.attributes.uid, 3000);
}

#[tokio::test]
async fn dfs_namespace_sgid_parent_sets_child_group_for_directories_and_symlinks() {
    use afs::dfs::{InodeAttributes, InodeId, MkdirRequest, NamespaceId, SymlinkRequest};
    let (_, dfs) = memory_meta_and_dfs_service(ReplicationConfig::local_single_copy()).await;
    let attrs = |mode, uid, gid, nlink| InodeAttributes {
        mode,
        uid,
        gid,
        nlink,
        atime_unix_ms: 1,
        mtime_unix_ms: 1,
        ctime_unix_ms: 1,
    };
    let parent = dfs
        .mkdir(MkdirRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("sgid-parent"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"sgid".to_vec(),
            attributes: attrs(0o2770, 0, 2000, 2),
            caller: domain_caller(0, 2000),
        })
        .await
        .unwrap();
    let user = domain_caller_with_groups(1000, 1000, &[1000, 2000]);
    let child = dfs
        .mkdir(MkdirRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("sgid-child"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: parent.inode_id.clone(),
            name: b"child".to_vec(),
            attributes: attrs(0o755, 1234, 1234, 2),
            caller: user.clone(),
        })
        .await
        .unwrap();
    let symlink = dfs
        .symlink(SymlinkRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("sgid-symlink"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: parent.inode_id,
            name: b"sym".to_vec(),
            target: b"target".to_vec(),
            attributes: attrs(libc::S_IFLNK | 0o777, 1234, 1234, 1),
            caller: user,
        })
        .await
        .unwrap();
    assert_eq!(child.attributes.uid, 1000);
    assert_eq!(child.attributes.gid, 2000);
    assert_eq!(child.attributes.mode & 0o2000, 0o2000);
    assert_eq!(symlink.attributes.uid, 1000);
    assert_eq!(symlink.attributes.gid, 2000);
}

#[tokio::test]
async fn dfs_special_inode_has_metadata_without_chunks_and_enforces_parent_access() {
    use afs::dfs::{
        CallerContext, InodeAttributes, InodeId, InodeKind, MkdirRequest, MknodRequest,
        NamespaceId, SpecialNodeKind,
    };
    let (_, dfs) = memory_meta_and_dfs_service(ReplicationConfig::local_single_copy()).await;
    let attrs = InodeAttributes {
        mode: 0o2770,
        uid: 1000,
        gid: 2000,
        nlink: 2,
        atime_unix_ms: 1,
        mtime_unix_ms: 1,
        ctime_unix_ms: 1,
    };
    let parent = dfs
        .mkdir(MkdirRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new("special-parent"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: InodeId::new("1"),
            name: b"special-parent".to_vec(),
            attributes: attrs.clone(),
            caller: domain_caller(0, 2000),
        })
        .await
        .unwrap();
    for (index, kind) in [
        SpecialNodeKind::Fifo,
        SpecialNodeKind::Socket,
        SpecialNodeKind::BlockDevice { rdev: 1792 },
        SpecialNodeKind::CharDevice { rdev: 259 },
    ]
    .into_iter()
    .enumerate()
    {
        let privileged = index >= 2;
        let request = MknodRequest {
            caller_id: "node-a".into(),
            operation_id: OperationId::new(format!("special-{index}")),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: parent.inode_id.clone(),
            name: format!("special-{index}").into_bytes(),
            kind,
            attributes: InodeAttributes {
                mode: 0o644,
                ..attrs.clone()
            },
            caller: CallerContext {
                uid: if privileged { 0 } else { 1000 },
                gid: 1000,
                supplementary_gids: if privileged { vec![] } else { vec![2000] },
            },
        };
        let inode = dfs.mknod(request.clone()).await.unwrap();
        assert_eq!(inode.kind, InodeKind::Special(kind));
        assert!(inode.head_version.is_none());
        assert!(inode.symlink_target.is_none());
        assert_eq!(inode.attributes.nlink, 1);
        assert_eq!(
            inode.attributes.gid, 2000,
            "setgid parent determines new inode group"
        );
        assert_eq!(inode.attributes.uid, request.caller.uid);
        assert_eq!(
            dfs.mknod(request).await.unwrap(),
            inode,
            "retry replays exact inode"
        );
    }
    let denied = MknodRequest {
        caller_id: "node-a".into(),
        operation_id: OperationId::new("special-denied-parent"),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: parent.inode_id.clone(),
        name: b"denied".to_vec(),
        kind: SpecialNodeKind::Fifo,
        attributes: attrs.clone(),
        caller: CallerContext {
            uid: 3000,
            gid: 3000,
            supplementary_gids: vec![],
        },
    };
    assert_eq!(
        dfs.mknod(denied).await.unwrap_err().code(),
        afs_error::IO_PERMISSION_DENIED
    );
    let denied_device = MknodRequest {
        caller_id: "node-a".into(),
        operation_id: OperationId::new("special-denied-device"),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: parent.inode_id,
        name: b"device".to_vec(),
        kind: SpecialNodeKind::CharDevice { rdev: 259 },
        attributes: attrs,
        caller: CallerContext {
            uid: 1000,
            gid: 1000,
            supplementary_gids: vec![2000],
        },
    };
    assert_eq!(
        dfs.mknod(denied_device).await.unwrap_err().code(),
        afs_error::IO_OPERATION_NOT_PERMITTED
    );
}

#[tokio::test]
async fn dfs_namespace_retry_binds_semantics_and_new_inode_identity_scopes_caller() {
    use afs::dfs::{InodeAttributes, InodeId, NamespaceId};
    use afs::meta::dfs::CreateFileRequest;
    let (_, dfs) = memory_meta_and_dfs_service(ReplicationConfig::local_single_copy()).await;
    let request = CreateFileRequest {
        caller_id: "node-a".into(),
        owner_session_id: "session-a".into(),
        operation_id: OperationId::new("same-id"),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: InodeId::new("1"),
        name: b"original".to_vec(),
        attributes: InodeAttributes {
            mode: 0o644,
            uid: 0,
            gid: 0,
            nlink: 1,
            atime_unix_ms: 1,
            mtime_unix_ms: 1,
            ctime_unix_ms: 1,
        },
        lease_seconds: 30,
    };
    let first = dfs.create(request.clone()).await.unwrap();
    assert_eq!(dfs.create(request.clone()).await.unwrap(), first);
    let mut changed = request.clone();
    changed.name = b"different".to_vec();
    assert_eq!(
        dfs.create(changed).await.unwrap_err().code(),
        afs_error::META_CATALOG_INVALID_REQUEST
    );
    assert!(
        dfs.lookup(
            NamespaceId::new("default"),
            InodeId::new("1"),
            b"different".to_vec()
        )
        .await
        .unwrap()
        .is_none()
    );
    let mut another_caller = request;
    another_caller.caller_id = "node-b".into();
    another_caller.owner_session_id = "session-b".into();
    another_caller.name = b"other-node".to_vec();
    let second = dfs.create(another_caller).await.unwrap();
    assert_ne!(first.0.inode_id, second.0.inode_id);
}

#[tokio::test]
async fn dfs_removed_directory_cannot_receive_new_entries() {
    use afs::dfs::{InodeAttributes, InodeId, MkdirRequest, NamespaceId, RmdirRequest};
    let (_, dfs) = memory_meta_and_dfs_service(ReplicationConfig::local_single_copy()).await;
    let request = MkdirRequest {
        caller_id: "node-a".into(),
        operation_id: OperationId::new("orphan-parent"),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: InodeId::new("1"),
        name: b"orphan-parent".to_vec(),
        attributes: InodeAttributes {
            mode: 0o755,
            uid: 0,
            gid: 0,
            nlink: 2,
            atime_unix_ms: 1,
            mtime_unix_ms: 1,
            ctime_unix_ms: 1,
        },
        caller: domain_caller(0, 0),
    };
    let dir = dfs.mkdir(request.clone()).await.unwrap();
    dfs.rmdir(RmdirRequest {
        caller_id: "node-a".into(),
        operation_id: OperationId::new("remove-parent"),
        namespace_id: NamespaceId::new("default"),
        parent_inode_id: InodeId::new("1"),
        name: b"orphan-parent".to_vec(),
        caller: domain_caller(0, 0),
    })
    .await
    .unwrap();
    let mut orphan_create = request;
    orphan_create.parent_inode_id = dir.inode_id;
    orphan_create.operation_id = OperationId::new("create-under-orphan");
    orphan_create.name = b"unreachable".to_vec();
    assert_eq!(
        dfs.mkdir(orphan_create).await.unwrap_err().code(),
        afs_error::IO_NOT_FOUND
    );
}

#[tokio::test]
async fn dfs_kernel_killpriv_commit_requires_lease_and_preserves_nonexecutable_sgid() {
    let dfs = memory_dfs().await;
    for (index, mode, expected) in [(0, 0o6777, 0o777), (1, 0o6666, 0o2666)] {
        let created = dfs
            .create(Request::new(DfsCreateRequest {
                caller_id: "node-a".into(),
                operation_id: format!("create-killpriv-{index}"),
                namespace_id: "default".into(),
                parent_inode_id: "1".into(),
                name: format!("killpriv-{index}").into_bytes(),
                attributes: Some(test_attrs(mode, 1)),
                owner_session_id: "session-a".into(),
                lease_seconds: 30,
            }))
            .await
            .unwrap()
            .into_inner();
        let inode = created.inode.unwrap();
        let lease = created.write_lease.unwrap();
        let request = CommitFileVersionRequest {
            caller_id: "node-a".into(),
            operation_id: format!("commit-killpriv-{index}"),
            inode_id: inode.inode_id.clone(),
            expected_inode_revision: inode.revision,
            expected_head_version_id: String::new(),
            version: Some(DfsFileVersion {
                version_id: format!("killpriv-version-{index}"),
                inode_id: inode.inode_id.clone(),
                parent_version_id: String::new(),
                length: 0,
                layout_root_id: format!("killpriv-layout-{index}"),
                created_at_unix_ms: 2,
            }),
            layout: Some(DfsLayoutRoot {
                layout_root_id: format!("killpriv-layout-{index}"),
                file_length: 0,
                inline_extents: Vec::new(),
            }),
            chunk_receipts: Vec::new(),
            write_lease: Some(lease.clone()),
            metadata_delta: Some(DfsCommitMetadataDelta {
                mode: DfsCommitMetadataMode::DataOnly.into(),
                kill_suidgid: true,
                ctime_unix_ms: 1234,
                ..Default::default()
            }),
        };
        let mut missing_ctime = request.clone();
        missing_ctime.operation_id = format!("missing-ctime-killpriv-{index}");
        missing_ctime.metadata_delta.as_mut().unwrap().ctime_unix_ms = 0;
        assert_eq!(
            dfs.commit_file_version(Request::new(missing_ctime))
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        let mut stale = request.clone();
        stale.operation_id = format!("stale-killpriv-{index}");
        stale.write_lease.as_mut().unwrap().lease_epoch = 0;
        assert_eq!(
            dfs.commit_file_version(Request::new(stale))
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        let committed = dfs
            .commit_file_version(Request::new(request.clone()))
            .await
            .unwrap()
            .into_inner()
            .inode
            .unwrap();
        assert_eq!(committed.attributes.as_ref().unwrap().mode, expected);
        assert_eq!(committed.attributes.as_ref().unwrap().ctime_unix_ms, 1234);
        let replayed = dfs
            .commit_file_version(Request::new(request))
            .await
            .unwrap()
            .into_inner()
            .inode
            .unwrap();
        assert_eq!(committed, replayed);
    }
}
