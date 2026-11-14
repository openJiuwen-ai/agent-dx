use super::*;
use crate::dfs::{
    ChunkReceipt, CommitMetadataDelta, DfsDurabilityPolicy, Extent, ExternalCommitReceipt,
    LayoutRootId,
};
use crate::meta::store::{NodeSessionLease, Store, local_log::LocalLog};
use crate::node::chunk::StagedChunk;

fn external_policy() -> DfsDurabilityPolicy {
    DfsDurabilityPolicy::ExternalBackend {
        store_id: "seaweed-test".into(),
        generation: "generation-1".into(),
    }
}

async fn register(store: &Store, node_id: &str) {
    store
        .register_node_session(
            RequestKey::new(node_id, "register"),
            NodeSessionLease {
                node_id: node_id.into(),
                session_id: format!("{node_id}-session"),
                grpc_addr: "http://127.0.0.1:1".into(),
                data_addr: "http://127.0.0.1:2".into(),
                rest_addr: "http://127.0.0.1:3".into(),
                nfs_addr: String::new(),
                ownerfs_enabled: false,
                storage_devices: Vec::new(),
                lease_ttl: std::time::Duration::from_secs(60),
            },
        )
        .await
        .unwrap();
}

async fn pending_commit(service: &DfsService) -> CommitFileVersion {
    let namespace_id = NamespaceId::new("default");
    let root = service.ensure_namespace_root(&namespace_id).await.unwrap();
    let mut attributes = root.attributes;
    attributes.mode = 0o644;
    let (inode, write_lease) = service
        .create(CreateFileRequest {
            caller_id: "writer".into(),
            owner_session_id: "writer-session".into(),
            operation_id: OperationId::new("create-file"),
            namespace_id,
            parent_inode_id: root.inode_id,
            name: b"external-file".to_vec(),
            caller: crate::dfs::CallerContext {
                uid: 0,
                gid: 0,
                supplementary_gids: Vec::new(),
            },
            attributes,
            lease_seconds: 60,
            first_generation: None,
        })
        .await
        .unwrap();
    let operation_id = OperationId::new("publish-file");
    let staged = StagedChunk::new_generational(operation_id.clone(), b"data".to_vec());
    let receipt = ChunkReceipt {
        operation_id: operation_id.clone(),
        chunk: staged.chunk.clone(),
        placement_revision: 0,
        placement_epoch: 0,
        replica_group_id: ReplicaGroupId::new(""),
        durable_acks: Vec::new(),
        external_commit: Some(ExternalCommitReceipt {
            store_id: "seaweed-test".into(),
            generation: "generation-1".into(),
            object_key: crate::dfs::external_object_key(
                "seaweed-test",
                "generation-1",
                &staged.chunk.id,
            )
            .unwrap(),
            persisted_bytes: staged.chunk.length,
            verified_digest: staged.chunk.content_digest.clone(),
        }),
    };
    let layout = LayoutRoot {
        id: LayoutRootId::new("external-layout"),
        file_length: 4,
        inline_extents: vec![Extent {
            file_offset: 0,
            length: 4,
            chunk_id: staged.chunk.id,
            chunk_offset: 0,
        }],
    };
    CommitFileVersion {
        gc_fence: None,
        operation_id,
        inode_id: inode.inode_id.clone(),
        write_lease,
        expected_inode_revision: inode.revision,
        expected_head_version: inode.head_version,
        file_version: FileVersion {
            id: FileVersionId::new("external-version"),
            inode_id: inode.inode_id,
            parent_version: None,
            length: 4,
            layout_root: layout.id.clone(),
            created_at_unix_ms: 1,
        },
        layout_root: layout,
        chunk_receipts: vec![receipt],
        clone_source: None,
        metadata_delta: CommitMetadataDelta {
            mode: CommitMetadataMode::DataOnly,
            mtime_unix_ms: None,
            ctime_unix_ms: None,
            kill_suidgid: false,
        },
    }
}

struct CurrentReadFixture {
    _temp: tempfile::TempDir,
    store: Arc<Store>,
    service: DfsService,
    request: crate::dfs::AuthorizeCurrentExternalRead,
    inode: InodeRecord,
}

async fn current_read_fixture() -> CurrentReadFixture {
    let temp = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalLog::open(temp.path(), Default::default()).unwrap());
    let store = Arc::new(Store::open_log(backend).await.unwrap());
    register(&store, "writer").await;
    register(&store, "reader").await;
    let service = DfsService::with_durability_policy(
        store.clone(),
        ReplicationConfig::local_single_copy(),
        external_policy(),
    );
    service.initialize_replication_config().await.unwrap();
    let commit = pending_commit(&service).await;
    let inode = service
        .commit_file_version("writer".into(), commit.clone())
        .await
        .unwrap();
    let sources = service
        .chunk_sources(DfsChunkSourcesRequest {
            caller_id: "reader".into(),
            namespace_id: inode.namespace_id.clone(),
            file_version_id: commit.file_version.id.clone(),
            layout_root_id: commit.layout_root.id.clone(),
            chunk_ids: vec![commit.chunk_receipts[0].chunk.id.clone()],
        })
        .await
        .unwrap();
    let source = &sources.chunks[0].sources[0];
    let request = crate::dfs::AuthorizeCurrentExternalRead {
        caller_id: "reader".into(),
        inode_id: inode.inode_id.clone(),
        namespace_id: inode.namespace_id.clone(),
        expected_head_version: commit.file_version.id,
        layout_root_id: commit.layout_root.id,
        validations: vec![crate::dfs::DfsReadValidation {
            grant: source.read_grant.clone(),
            chunk_id: source.chunk_id.clone(),
            copy_id: source.copy_id.clone(),
            chunk_offset: 0,
            length: 4,
        }],
        missing_sources: Vec::new(),
    };
    CurrentReadFixture {
        _temp: temp,
        store,
        service,
        request,
        inode,
    }
}

async fn replace_current_read_inode(f: &CurrentReadFixture, mut inode: InodeRecord) {
    inode.revision += 1;
    let key = RequestKey::new("fixture", "replace-current-head");
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.conditions
        .push(TxnCondition::EntityEquals(MetaEntity::DfsInode(
            f.inode.clone(),
        )));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::DfsInode(inode),
        }),
    ]);
    assert!(matches!(
        f.store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::Committed { .. }
    ));
}

fn version_read_request(
    request: &crate::dfs::AuthorizeCurrentExternalRead,
    chunk_offset: u64,
    length: u64,
) -> crate::dfs::AuthorizeExternalVersionRead {
    crate::dfs::AuthorizeExternalVersionRead {
        caller_id: request.caller_id.clone(),
        inode_id: request.inode_id.clone(),
        namespace_id: request.namespace_id.clone(),
        file_version_id: request.expected_head_version.clone(),
        layout_root_id: request.layout_root_id.clone(),
        requests: vec![crate::dfs::DfsMissingReadSource {
            chunk_id: request.validations[0].chunk_id.clone(),
            chunk_offset,
            length,
        }],
    }
}

#[tokio::test]
async fn current_external_read_requires_exact_head_and_complete_authority() {
    let f = current_read_fixture().await;
    let reply = f
        .service
        .authorize_current_external_read(f.request.clone())
        .await
        .unwrap();
    match reply {
        crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
            inode,
            authorizations,
        } => {
            assert_eq!(inode, f.inode);
            assert_eq!(authorizations.len(), 1);
            assert_eq!(
                authorizations[0].authorized.validation,
                f.request.validations[0]
            );
            assert!(authorizations[0].authorized.expires_at_unix_ms > now_unix_ms());
        }
        _ => panic!("unchanged head did not return authorization"),
    }
    for case in 0..9 {
        let mut bad = f.request.clone();
        match case {
            0 => bad.namespace_id = NamespaceId::new("other"),
            1 => bad.inode_id = InodeId::new("1"),
            2 => bad.layout_root_id = LayoutRootId::new("other"),
            3 => bad.validations[0].grant.caller_node_epoch += 1,
            4 => bad.validations[0].grant.expires_at_unix_ms = now_unix_ms(),
            5 => bad.validations[0].grant.token = format!("dfs-read-v1:{}", "0".repeat(64)),
            6 => bad.validations[0].length = 5,
            7 => bad.validations[0].grant.file_version_id = FileVersionId::new("other"),
            _ => bad.validations.clear(),
        }
        assert!(
            f.service
                .authorize_current_external_read(bad)
                .await
                .is_err(),
            "case {case}"
        );
    }
}

#[tokio::test]
async fn current_external_read_resolves_missing_sources_in_the_same_view() {
    let f = current_read_fixture().await;
    let mut missing = f.request.clone();
    let chunk_id = missing.validations[0].chunk_id.clone();
    missing.validations.clear();
    missing
        .missing_sources
        .push(crate::dfs::DfsMissingReadSource {
            chunk_id: chunk_id.clone(),
            chunk_offset: 1,
            length: 2,
        });
    let reply = f
        .service
        .authorize_current_external_read(missing)
        .await
        .unwrap();
    match reply {
        crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
            inode,
            authorizations,
        } => {
            assert_eq!(inode, f.inode);
            assert_eq!(authorizations.len(), 1);
            assert_eq!(authorizations[0].authorized.validation.chunk_id, chunk_id);
            assert_eq!(authorizations[0].authorized.validation.chunk_offset, 1);
            assert_eq!(authorizations[0].authorized.validation.length, 2);
            assert!(authorizations[0].authorized.expires_at_unix_ms > now_unix_ms());
        }
        _ => panic!("unchanged head did not resolve missing source"),
    }
}

#[tokio::test]
async fn current_external_read_mixes_cached_grants_and_missing_sources() {
    let f = current_read_fixture().await;
    let mut mixed = f.request.clone();
    let chunk_id = mixed.validations[0].chunk_id.clone();
    mixed
        .missing_sources
        .push(crate::dfs::DfsMissingReadSource {
            chunk_id,
            chunk_offset: 2,
            length: 2,
        });
    let reply = f
        .service
        .authorize_current_external_read(mixed)
        .await
        .unwrap();
    match reply {
        crate::dfs::AuthorizeCurrentExternalReadReply::Authorized { authorizations, .. } => {
            assert_eq!(authorizations.len(), 2);
            assert_eq!(authorizations[0].authorized.validation.chunk_offset, 0);
            assert_eq!(authorizations[1].authorized.validation.chunk_offset, 2);
        }
        _ => panic!("unchanged head did not authorize the mixed request"),
    }
}

#[tokio::test]
async fn external_version_read_authorizes_old_head_version_without_old_mac() {
    let f = current_read_fixture().await;
    let mut changed = f.inode.clone();
    changed.head_version = None;
    replace_current_read_inode(&f, changed).await;

    let reply = f
        .service
        .authorize_external_version_read(version_read_request(&f.request, 0, 4))
        .await
        .unwrap();
    assert_eq!(reply.authorizations.len(), 1);
    assert!(reply.remaining_validity_ms > 0);
    assert!(reply.remaining_validity_ms <= 5_000);
    let authorized = &reply.authorizations[0].authorized;
    assert_eq!(
        authorized.validation.grant.file_version_id,
        f.request.expected_head_version
    );
    assert_eq!(
        authorized.validation.grant.layout_root_id,
        f.request.layout_root_id
    );
    assert_eq!(authorized.validation.chunk_offset, 0);
    assert_eq!(authorized.validation.length, 4);
}

#[tokio::test]
async fn external_version_read_rejects_wrong_identity_or_range() {
    let f = current_read_fixture().await;
    for case in 0..5 {
        let mut bad = version_read_request(&f.request, 0, 4);
        match case {
            0 => bad.namespace_id = NamespaceId::new("other"),
            1 => bad.inode_id = InodeId::new("other"),
            2 => bad.layout_root_id = LayoutRootId::new("other"),
            3 => bad.requests[0].length = 5,
            _ => bad.requests.clear(),
        }
        assert!(
            f.service
                .authorize_external_version_read(bad)
                .await
                .is_err(),
            "case {case}"
        );
    }
}

#[tokio::test]
async fn external_version_read_requires_live_current_session() {
    let f = current_read_fixture().await;
    let view = f.store.read_view().await.unwrap();
    assert!(
        f.service
            .authorize_external_version_read_in_view(
                &view,
                version_read_request(&f.request, 0, 4),
                u64::MAX
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn external_version_read_readmits_after_reader_restart_without_old_mac() {
    let f = current_read_fixture().await;
    let old_epoch = f.request.validations[0].grant.caller_node_epoch;
    f.store
        .register_node_session(
            RequestKey::new("reader", "restart-external-version-read"),
            NodeSessionLease {
                node_id: "reader".into(),
                session_id: "reader-restarted-for-version-read".into(),
                grpc_addr: "http://127.0.0.1:1".into(),
                data_addr: "http://127.0.0.1:2".into(),
                rest_addr: "http://127.0.0.1:3".into(),
                nfs_addr: String::new(),
                ownerfs_enabled: false,
                storage_devices: Vec::new(),
                lease_ttl: std::time::Duration::from_secs(60),
            },
        )
        .await
        .unwrap();

    assert!(
        f.service
            .authorize_current_external_read(f.request.clone())
            .await
            .is_err()
    );
    let reply = f
        .service
        .authorize_external_version_read(version_read_request(&f.request, 1, 2))
        .await
        .unwrap();
    assert_eq!(reply.authorizations.len(), 1);
    assert_ne!(
        reply.authorizations[0]
            .authorized
            .validation
            .grant
            .caller_node_epoch,
        old_epoch
    );
    assert_eq!(
        reply.authorizations[0].authorized.validation.chunk_offset,
        1
    );
}

#[tokio::test]
async fn current_external_read_changed_head_never_returns_old_authority() {
    let f = current_read_fixture().await;
    let mut changed = f.inode.clone();
    changed.head_version = None;
    replace_current_read_inode(&f, changed).await;
    assert!(
        matches!(f.service.authorize_current_external_read(f.request.clone()).await.unwrap(),
        crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(inode) if inode.head_version.is_none())
    );
    // A changed head must not hide a revoked/expired requester, even when
    // the same request also asks Meta to resolve a missing source.
    let mut expired = f.request.clone();
    expired.validations[0].grant.expires_at_unix_ms = 0;
    expired
        .missing_sources
        .push(crate::dfs::DfsMissingReadSource {
            chunk_id: expired.validations[0].chunk_id.clone(),
            chunk_offset: 1,
            length: 2,
        });
    assert!(
        f.service
            .authorize_current_external_read(expired)
            .await
            .is_err()
    );
    let mut missing_unknown_old_copy = f.request.clone();
    missing_unknown_old_copy.validations.clear();
    missing_unknown_old_copy
        .missing_sources
        .push(crate::dfs::DfsMissingReadSource {
            chunk_id: crate::dfs::ChunkId::new(
                "blake3-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-4",
            ),
            chunk_offset: 0,
            length: 4,
        });
    assert!(matches!(
        f.service
            .authorize_current_external_read(missing_unknown_old_copy)
            .await
            .unwrap(),
        crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(inode)
            if inode.head_version.is_none()
    ));
    f.store
        .register_node_session(
            RequestKey::new("reader", "restart-current-read"),
            NodeSessionLease {
                node_id: "reader".into(),
                session_id: "reader-restarted".into(),
                grpc_addr: "http://127.0.0.1:1".into(),
                data_addr: "http://127.0.0.1:2".into(),
                rest_addr: "http://127.0.0.1:3".into(),
                nfs_addr: String::new(),
                ownerfs_enabled: false,
                storage_devices: Vec::new(),
                lease_ttl: std::time::Duration::from_secs(60),
            },
        )
        .await
        .unwrap();
    let mut missing_after_restart = f.request.clone();
    missing_after_restart.validations.clear();
    missing_after_restart
        .missing_sources
        .push(crate::dfs::DfsMissingReadSource {
            chunk_id: f.request.validations[0].chunk_id.clone(),
            chunk_offset: 1,
            length: 2,
        });
    assert!(
        f.service
            .authorize_current_external_read(f.request)
            .await
            .is_err()
    );
    let current_view = f.store.read_view().await.unwrap();
    assert!(
        f.service
            .authorize_current_external_read_in_view(&current_view, missing_after_restart, u64::MAX)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn current_external_read_head_and_grants_share_the_pinned_view() {
    let f = current_read_fixture().await;
    let mut missing = f.request.clone();
    let chunk_id = missing.validations[0].chunk_id.clone();
    missing.validations.clear();
    missing
        .missing_sources
        .push(crate::dfs::DfsMissingReadSource {
            chunk_id,
            chunk_offset: 1,
            length: 2,
        });
    let view = f.store.read_view().await.unwrap();
    f.store
        .register_node_session(
            RequestKey::new("reader", "restart-after-pinned-view"),
            NodeSessionLease {
                node_id: "reader".into(),
                session_id: "reader-new-session".into(),
                grpc_addr: "http://127.0.0.1:11".into(),
                data_addr: "http://127.0.0.1:12".into(),
                rest_addr: "http://127.0.0.1:13".into(),
                nfs_addr: String::new(),
                ownerfs_enabled: false,
                storage_devices: Vec::new(),
                lease_ttl: std::time::Duration::from_secs(60),
            },
        )
        .await
        .unwrap();
    let mut changed = f.inode.clone();
    changed.head_version = None;
    replace_current_read_inode(&f, changed).await;
    // The fixed view must authorize and resolve the old head consistently,
    // while a new request observes the current head/session; no second view
    // may slip into validation or missing-source resolution.
    assert!(
        matches!(f.service.authorize_current_external_read_in_view(&view, missing.clone(), now_unix_ms()).await.unwrap(),
        crate::dfs::AuthorizeCurrentExternalReadReply::Authorized { inode, authorizations } if inode == f.inode && authorizations.len() == 1)
    );
    assert!(matches!(
        f.service
            .authorize_current_external_read(missing)
            .await
            .unwrap(),
        crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(_)
    ));
}

#[tokio::test]
async fn external_receipt_rejects_wrong_store_key_length_digest_without_publication() {
    let temp = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalLog::open(temp.path(), Default::default()).unwrap());
    let store = Arc::new(Store::open_log(backend).await.unwrap());
    register(&store, "writer").await;
    let service = DfsService::with_durability_policy(
        store.clone(),
        ReplicationConfig::local_single_copy(),
        external_policy(),
    );
    service.initialize_replication_config().await.unwrap();
    let commit = pending_commit(&service).await;
    for case in 0..12 {
        let mut invalid_commit = commit.clone();
        let receipt = invalid_commit.chunk_receipts[0]
            .external_commit
            .as_mut()
            .unwrap();
        match case {
            0 => receipt.store_id = "other-store".into(),
            1 => receipt.generation = "other-generation".into(),
            2 => receipt.object_key = "arbitrary/client/key".into(),
            3 => receipt.persisted_bytes += 1,
            4 => receipt.verified_digest.bytes[0] ^= 1,
            5 => invalid_commit.chunk_receipts[0].external_commit = None,
            6 => invalid_commit.chunk_receipts[0].placement_epoch = 1,
            7 => {
                invalid_commit.chunk_receipts[0].chunk.content_digest.bytes[0] ^= 1;
                let changed = invalid_commit.chunk_receipts[0]
                    .chunk
                    .content_digest
                    .clone();
                invalid_commit.chunk_receipts[0]
                    .external_commit
                    .as_mut()
                    .unwrap()
                    .verified_digest = changed;
            }
            8 => {
                invalid_commit.chunk_receipts[0].chunk.id =
                    crate::dfs::ChunkId::new("unbound-chunk")
            }
            // R05: an object must belong to this write generation. Plain content
            // addressing could name an object some other write owns.
            10 | 11 => {
                let chunk = &invalid_commit.chunk_receipts[0].chunk;
                let root = blake3::Hash::from(chunk.content_digest.bytes);
                let id = if case == 10 {
                    crate::dfs::ChunkId::new(format!("blake3-{}-{}", root.to_hex(), chunk.length))
                } else {
                    ChunkObject::generational_blake3_id(
                        &OperationId::new("another-write"),
                        &root,
                        chunk.length,
                    )
                };
                let key =
                    crate::dfs::external_object_key("seaweed-test", "generation-1", &id).unwrap();
                invalid_commit.chunk_receipts[0].chunk.id = id;
                invalid_commit.chunk_receipts[0]
                    .external_commit
                    .as_mut()
                    .unwrap()
                    .object_key = key;
            }
            _ => invalid_commit.chunk_receipts[0]
                .durable_acks
                .push(ReplicaAck {
                    operation_id: commit.operation_id.clone(),
                    chunk_id: commit.chunk_receipts[0].chunk.id.clone(),
                    placement_revision: 1,
                    placement_epoch: 1,
                    node_id: "fake-node".into(),
                    node_epoch: 1,
                    device_id: "fake-device".into(),
                    device_epoch: 1,
                    catalog_revision: 1,
                    persisted_bytes: 4,
                    verified_digest: commit.chunk_receipts[0].chunk.content_digest.clone(),
                }),
        }
        assert!(
            service
                .commit_file_version("writer".into(), invalid_commit)
                .await
                .is_err()
        );
        assert!(
            service
                .get_inode(commit.inode_id.clone())
                .await
                .unwrap()
                .head_version
                .is_none()
        );
        assert!(
            store
                .read(MetaRead::DfsChunk(
                    commit.chunk_receipts[0].chunk.id.clone()
                ))
                .await
                .unwrap()
                .entity
                .is_none()
        );
    }
    service
        .commit_file_version("writer".into(), commit)
        .await
        .unwrap();
}

#[tokio::test]
async fn external_policy_and_published_reference_survive_local_file_store_reload() {
    let temp = tempfile::tempdir().unwrap();
    let backend = Arc::new(LocalLog::open(temp.path(), Default::default()).unwrap());
    let store = Arc::new(Store::open_log(backend.clone()).await.unwrap());
    register(&store, "writer").await;
    register(&store, "reader").await;
    let service = DfsService::with_durability_policy(
        store.clone(),
        ReplicationConfig::local_single_copy(),
        external_policy(),
    );
    service.initialize_replication_config().await.unwrap();
    let commit = pending_commit(&service).await;
    let published = service
        .commit_file_version("writer".into(), commit.clone())
        .await
        .unwrap();
    assert_eq!(
        service
            .commit_file_version("writer".into(), commit.clone())
            .await
            .unwrap(),
        published
    );
    let request = RequestKey::new("fixture", "writer-left");
    let mut leave = MetaTxn::new(request.clone(), StoreOperation::RegisterNode);
    leave.conditions.extend([
        TxnCondition::RequestAbsent(request.clone()),
        TxnCondition::NodeSessionCurrent {
            node_id: "writer".into(),
            session_id: "writer-session".into(),
        },
    ]);
    leave.mutations.extend([
        TxnMutation::Delete(MetaKey::CurrentNodeSession {
            node_id: "writer".into(),
        }),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request,
            operation: StoreOperation::RegisterNode,
            result: OperationResult::Empty,
        }),
    ]);
    assert!(matches!(
        store.compare_and_commit(leave).await.unwrap(),
        TxnOutcome::Committed { .. }
    ));
    drop(service);
    drop(store);
    let reopened = Arc::new(Store::open_log(backend).await.unwrap());
    let restored = DfsService::with_durability_policy(
        reopened.clone(),
        ReplicationConfig::local_single_copy(),
        external_policy(),
    );
    restored.initialize_replication_config().await.unwrap();
    let sources = restored
        .chunk_sources(DfsChunkSourcesRequest {
            caller_id: "reader".into(),
            namespace_id: NamespaceId::new("default"),
            file_version_id: commit.file_version.id,
            layout_root_id: commit.layout_root.id,
            chunk_ids: vec![commit.chunk_receipts[0].chunk.id.clone()],
        })
        .await
        .unwrap();
    let source = &sources.chunks[0].sources[0];
    assert_eq!(source.role, CopyRole::ExternalCommitted);
    assert!(source.data_endpoint.is_none());
    assert!(
        matches!(&source.location, CopyLocation::External { store_id, object_key, .. }
        if store_id == "seaweed-test" && object_key == &commit.chunk_receipts[0].external_commit.as_ref().unwrap().object_key)
    );
    let validation = crate::dfs::DfsReadValidation {
        grant: source.read_grant.clone(),
        chunk_id: source.chunk_id.clone(),
        copy_id: source.copy_id.clone(),
        chunk_offset: 0,
        length: 4,
    };
    let authorized = restored
        .validate_external_read_grants("reader".into(), vec![validation.clone()])
        .await
        .unwrap();
    assert_eq!(authorized[0].chunk, commit.chunk_receipts[0].chunk);
    assert_eq!(authorized[0].location, source.location);
    assert!(authorized[0].authorized.expires_at_unix_ms > now_unix_ms());
    for case in 0..5 {
        let mut bad = validation.clone();
        match case {
            0 => bad.grant.namespace_id = NamespaceId::new("other-namespace"),
            1 => {
                bad.chunk_offset = 3;
                bad.length = 2;
            }
            2 => bad.grant.expires_at_unix_ms = now_unix_ms(),
            3 => bad.grant.token = format!("dfs-read-v1:{}", "0".repeat(64)),
            _ => bad.grant.caller_node_epoch += 1,
        }
        assert!(
            restored
                .validate_external_read_grants("reader".into(), vec![bad])
                .await
                .is_err()
        );
    }
    assert!(
        restored
            .validate_external_read_grants("other-reader".into(), vec![validation.clone()])
            .await
            .is_err()
    );
    reopened
        .register_node_session(
            RequestKey::new("reader", "restart"),
            NodeSessionLease {
                node_id: "reader".into(),
                session_id: "reader-restarted".into(),
                grpc_addr: "http://127.0.0.1:1".into(),
                data_addr: "http://127.0.0.1:2".into(),
                rest_addr: "http://127.0.0.1:3".into(),
                nfs_addr: String::new(),
                ownerfs_enabled: false,
                storage_devices: Vec::new(),
                lease_ttl: std::time::Duration::from_secs(60),
            },
        )
        .await
        .unwrap();
    assert!(
        restored
            .validate_external_read_grants("reader".into(), vec![validation])
            .await
            .is_err()
    );
    assert!(
        DfsService::with_replication_config(
            reopened.clone(),
            ReplicationConfig::local_single_copy()
        )
        .initialize_replication_config()
        .await
        .is_err()
    );
    let changed = DfsService::with_durability_policy(
        reopened,
        ReplicationConfig::local_single_copy(),
        DfsDurabilityPolicy::ExternalBackend {
            store_id: "seaweed-test".into(),
            generation: "generation-2".into(),
        },
    );
    assert!(changed.initialize_replication_config().await.is_err());
}

#[tokio::test]
async fn existing_node_replication_authority_cannot_be_reclassified_as_external() {
    let store = Arc::new(
        Store::open_log(Arc::new(
            crate::meta::store::journal::MemoryLog::new(Default::default()).unwrap(),
        ))
        .await
        .unwrap(),
    );
    let request = RequestKey::new("legacy", "replication");
    let mut txn = MetaTxn::new(
        request.clone(),
        StoreOperation::DfsInitializeReplicationConfig,
    );
    txn.conditions.extend([
        TxnCondition::RequestAbsent(request.clone()),
        TxnCondition::Missing(MetaKey::DfsReplicationConfig),
        TxnCondition::Missing(MetaKey::DfsDurabilityPolicy),
    ]);
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsReplicationConfig(
            ReplicationConfig::local_single_copy(),
        )),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request,
            operation: StoreOperation::DfsInitializeReplicationConfig,
            result: OperationResult::Empty,
        }),
    ]);
    assert!(matches!(
        store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::Committed { .. }
    ));
    let service = DfsService::with_durability_policy(
        store.clone(),
        ReplicationConfig::local_single_copy(),
        external_policy(),
    );
    assert!(service.initialize_replication_config().await.is_err());
    assert!(
        store
            .read(MetaRead::DfsDurabilityPolicy)
            .await
            .unwrap()
            .entity
            .is_none()
    );
    DfsService::new(store)
        .initialize_replication_config()
        .await
        .unwrap();
}

#[tokio::test]
async fn external_publication_has_one_external_copy_and_no_replication_debt() {
    let store = Arc::new(
        Store::open_log(Arc::new(
            crate::meta::store::journal::MemoryLog::new(Default::default()).unwrap(),
        ))
        .await
        .unwrap(),
    );
    register(&store, "writer").await;
    let service = DfsService::with_durability_policy(
        store.clone(),
        ReplicationConfig::local_single_copy(),
        external_policy(),
    );
    service.initialize_replication_config().await.unwrap();
    let commit = pending_commit(&service).await;
    let node_service = DfsService::new(store.clone());
    assert!(
        node_service
            .validate_chunk_receipt(
                "writer",
                &commit.chunk_receipts[0],
                &ReplicationConfig::local_single_copy()
            )
            .await
            .is_err()
    );
    service
        .commit_file_version("writer".into(), commit.clone())
        .await
        .unwrap();
    let placement = service
        .placement(&commit.chunk_receipts[0].chunk.id)
        .await
        .unwrap();
    assert_eq!(placement.desired_copies, 1);
    assert_eq!(placement.health, PlacementHealth::Satisfied);
    assert_eq!(placement.copies.len(), 1);
    let copy = service.copy(&placement.copies[0]).await.unwrap();
    assert_eq!(copy.role, CopyRole::ExternalCommitted);
    assert!(matches!(copy.location, CopyLocation::External { .. }));
    assert!(
        store
            .read(MetaRead::DfsReplicationTasks)
            .await
            .unwrap()
            .entities
            .is_empty()
    );
    assert!(
        service
            .claim_replication_task(ClaimReplicationTask {
                caller_id: "writer".into(),
                caller_session_id: "writer-session".into(),
                caller_node_epoch: 1,
                operation_id: OperationId::new("no-repair"),
                lease_seconds: 30,
            })
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn external_version_read_accepts_thousand_ranges_and_checks_last_range() {
    let f = current_read_fixture().await;
    let mut request = version_read_request(&f.request, 0, 4);
    request.requests = vec![request.requests[0].clone(); 1000];
    let reply = f
        .service
        .authorize_external_version_read(request.clone())
        .await
        .unwrap();
    assert_eq!(reply.authorizations.len(), 1000);

    request.requests.last_mut().unwrap().chunk_offset = 4;
    assert!(
        f.service
            .authorize_external_version_read(request)
            .await
            .is_err()
    );
}
