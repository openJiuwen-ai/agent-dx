use super::*;
use crate::dfs::{ChunkReceipt, CommitMetadataDelta, Extent, GcFence, LayoutRootId};
use crate::meta::store::{NodeSessionLease, Store, journal::MemoryLog};
use crate::node::chunk::{LocalChunkStore, StagedChunk};

struct Fixture {
    _temp: tempfile::TempDir,
    store: Arc<Store>,
    service: DfsService,
    local: LocalChunkStore,
    staged: StagedChunk,
    commit: CommitFileVersion,
}

async fn generation_fixture() -> Fixture {
    let mut f = fixture().await;
    f.commit.operation_id = OperationId::new(crate::meta::store::timed_request_id("generation"));
    f.commit.chunk_receipts[0].operation_id = f.commit.operation_id.clone();
    let staged =
        StagedChunk::new_crc32c(f.commit.operation_id.clone(), f.staged.bytes().to_vec()).unwrap();
    let placement = f.service.placement_snapshot("writer".into()).await.unwrap();
    let group = placement
        .replica_groups
        .iter()
        .find(|g| g.targets[0].node_id == "writer")
        .unwrap();
    let ack = f
        .local
        .persist(
            &staged,
            &group.targets[0],
            placement.revision,
            group.placement_epoch,
        )
        .unwrap();
    f.commit.gc_fence = Some(GcFence::WriteGeneration);
    f.commit.chunk_receipts[0].chunk = staged.chunk.clone();
    f.commit.chunk_receipts[0].durable_acks = vec![ack];
    f.commit.layout_root.inline_extents[0].chunk_id = staged.chunk.id.clone();
    f.staged = staged;
    f.service
        .update_gc_reference(generation_admission(&f, false))
        .await
        .unwrap();
    f
}

fn generation_admission(f: &Fixture, release: bool) -> crate::dfs::UpdateGcReference {
    crate::dfs::UpdateGcReference {
        node_id: "writer".into(),
        session_id: "writer-session".into(),
        reference_id: format!("write:{}", f.commit.operation_id.0),
        inode_id: Some(f.commit.inode_id.clone()),
        version_id: None,
        publication: true,
        chunks: vec![],
        release,
        admission_deadline_unix_ms: now_unix_ms().saturating_add(30_000),
    }
}

#[tokio::test]
async fn generation_commit_survives_unrelated_gc_and_reply_loss() {
    let f = generation_fixture().await;
    let garbage =
        StagedChunk::new_crc32c(OperationId::new("unrelated"), b"unreferenced".to_vec()).unwrap();
    let gc = f
        .service
        .collect_garbage(
            "writer".into(),
            "writer-session".into(),
            0,
            vec![f.staged.chunk.clone(), garbage.chunk.clone()],
        )
        .await
        .unwrap();
    assert!(gc.chunks.contains(&garbage.chunk.id));
    assert!(!gc.chunks.contains(&f.staged.chunk.id));
    let committed = f
        .service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap();
    let replay = f
        .service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap();
    assert_eq!(committed, replay);
    assert!(
        f.service
            .update_gc_reference(generation_admission(&f, false))
            .await
            .is_err(),
        "finished generations cannot be readmitted"
    );
}

#[tokio::test]
async fn generation_retirement_prevents_late_commit_and_readmission() {
    let f = generation_fixture().await;
    f.service
        .update_gc_reference(generation_admission(&f, true))
        .await
        .unwrap();
    assert_eq!(
        f.service
            .commit_file_version("writer".into(), f.commit.clone())
            .await
            .unwrap_err()
            .code(),
        afs_error::META_DFS_GC_CHANGED
    );
    assert!(
        f.service
            .update_gc_reference(generation_admission(&f, false))
            .await
            .is_err()
    );
    assert!(
        f.service
            .get_inode(f.commit.inode_id)
            .await
            .unwrap()
            .head_version
            .is_none()
    );
}

#[tokio::test]
async fn generation_retry_can_publish_retained_receipts_only_while_all_birth_generations_live() {
    for retire_old in [false, true] {
        let mut f = generation_fixture().await;
        let old_operation = f.commit.operation_id.clone();
        if retire_old {
            f.service
                .update_gc_reference(generation_admission(&f, true))
                .await
                .unwrap();
        }
        f.commit.operation_id = OperationId::new(crate::meta::store::timed_request_id(
            "retry-with-new-writes",
        ));
        f.service
            .update_gc_reference(generation_admission(&f, false))
            .await
            .unwrap();
        let result = f
            .service
            .commit_file_version("writer".into(), f.commit.clone())
            .await;
        if retire_old {
            assert_eq!(result.unwrap_err().code(), afs_error::META_DFS_GC_CHANGED);
        } else {
            result.unwrap();
            for operation in [old_operation, f.commit.operation_id] {
                let key = MetaKey::DfsReference {
                    node_id: "writer".into(),
                    session_id: "writer-session".into(),
                    reference_id: format!("write:{}", ChunkObject::generation_id(&operation)),
                };
                let Some(MetaEntity::DfsReference(reference)) = f
                    .store
                    .read_view()
                    .await
                    .unwrap()
                    .lookup(&key)
                    .unwrap()
                    .entity
                else {
                    panic!("retirement marker missing")
                };
                assert!(reference.retired_at_unix_ms.is_some());
            }
        }
    }
}

#[tokio::test]
async fn generation_expired_identity_cannot_refresh_admission_after_tombstone_cleanup() {
    let f = fixture().await;
    let mut request = generation_admission(&f, false);
    request.reference_id = format!(
        "write:afs1-{}-old-generation",
        now_unix_ms().saturating_sub(60_001)
    );
    assert_eq!(
        f.service
            .update_gc_reference(request)
            .await
            .unwrap_err()
            .code(),
        afs_error::META_DFS_REFERENCE_ADMISSION_EXPIRED
    );
}

#[tokio::test]
async fn generation_atomic_publication_condition_rejects_retirement_race() {
    let f = generation_fixture().await;
    let key = MetaKey::DfsReference {
        node_id: "writer".into(),
        session_id: "writer-session".into(),
        reference_id: format!(
            "write:{}",
            ChunkObject::generation_id(&f.commit.operation_id)
        ),
    };
    let reference = f
        .store
        .read_view()
        .await
        .unwrap()
        .lookup(&key)
        .unwrap()
        .entity
        .unwrap();
    let mut txn = MetaTxn::new(
        RequestKey::new("race", "generation-publish"),
        StoreOperation::DfsCommitFileVersion,
    );
    txn.conditions.push(TxnCondition::EntityEquals(reference));
    txn.conditions
        .push(TxnCondition::RequestAbsent(txn.request.clone()));
    txn.mutations
        .push(TxnMutation::Put(MetaEntity::DfsFileVersion(
            f.commit.file_version.clone(),
        )));
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: txn.request.clone(),
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }));
    f.service
        .update_gc_reference(generation_admission(&f, true))
        .await
        .unwrap();
    assert!(matches!(
        f.store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::ConditionFailed { .. }
    ));
    assert!(
        f.store
            .read(MetaRead::DfsFileVersion(f.commit.file_version.id))
            .await
            .unwrap()
            .entity
            .is_none()
    );
}
async fn fixture() -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let local = LocalChunkStore::open(temp.path(), "writer").unwrap();
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    store
        .register_node_session(
            RequestKey::new("writer", "register"),
            NodeSessionLease {
                node_id: "writer".into(),
                session_id: "writer-session".into(),
                grpc_addr: "http://127.0.0.1:1".into(),
                data_addr: "http://127.0.0.1:2".into(),
                rest_addr: "http://127.0.0.1:3".into(),
                storage_devices: vec![local.device_descriptor().unwrap()],
                lease_ttl: std::time::Duration::from_secs(60),
            },
        )
        .await
        .unwrap();
    let service = DfsService::new(store.clone());
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
            owner_session_id: "writer-session".into(),
            operation_id: OperationId::new("create-fenced"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: root.inode_id,
            name: b"fenced.bin".to_vec(),
            attributes,
            lease_seconds: 60,
        })
        .await
        .unwrap();
    let fence = service.current_gc_fence().await.unwrap();
    let operation_id = OperationId::new("publish-fenced");
    let staged = StagedChunk::new(operation_id.clone(), b"fenced body".to_vec());
    let placement = service.placement_snapshot("writer".into()).await.unwrap();
    let group = placement
        .replica_groups
        .iter()
        .find(|g| g.targets[0].node_id == "writer")
        .unwrap();
    let ack = local
        .persist(
            &staged,
            &group.targets[0],
            placement.revision,
            group.placement_epoch,
        )
        .unwrap();
    let layout = LayoutRoot {
        id: LayoutRootId::new("fenced-layout"),
        file_length: staged.chunk.length,
        inline_extents: vec![Extent {
            file_offset: 0,
            length: staged.chunk.length,
            chunk_id: staged.chunk.id.clone(),
            chunk_offset: 0,
        }],
    };
    let commit = CommitFileVersion {
        gc_fence: Some(fence),
        operation_id: operation_id.clone(),
        inode_id: inode.inode_id.clone(),
        write_lease,
        expected_inode_revision: inode.revision,
        expected_head_version: inode.head_version.clone(),
        file_version: FileVersion {
            id: FileVersionId::new("fenced-version"),
            inode_id: inode.inode_id,
            parent_version: inode.head_version,
            length: staged.chunk.length,
            layout_root: layout.id.clone(),
            created_at_unix_ms: 1,
        },
        layout_root: layout,
        chunk_receipts: vec![ChunkReceipt {
            operation_id,
            chunk: staged.chunk.clone(),
            placement_revision: placement.revision,
            placement_epoch: group.placement_epoch,
            replica_group_id: group.id.clone(),
            durable_acks: vec![ack],
            external_commit: None,
        }],
        clone_source: None,
        metadata_delta: CommitMetadataDelta {
            mode: CommitMetadataMode::DataOnly,
            mtime_unix_ms: None,
            ctime_unix_ms: None,
            kill_suidgid: false,
        },
    };
    Fixture {
        _temp: temp,
        store,
        service,
        local,
        staged,
        commit,
    }
}
async fn schedule_gc(f: &Fixture) -> crate::dfs::GcState {
    let batch = f
        .service
        .collect_garbage(
            "writer".into(),
            "writer-session".into(),
            0,
            vec![f.staged.chunk.clone()],
        )
        .await
        .unwrap();
    assert!(batch.chunks.contains(&f.staged.chunk.id));
    batch
}
#[tokio::test]
async fn gc_fence_normal_real_persistence_commits_without_chunk_registrations() {
    let f = fixture().await;
    let inode = f
        .service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap();
    assert_eq!(inode.head_version, Some(f.commit.file_version.id));
    let mut bytes = vec![0; f.staged.chunk.length as usize];
    assert_eq!(
        f.local.read_at(&f.staged.chunk.id, 0, &mut bytes).unwrap(),
        bytes.len()
    );
    assert_eq!(bytes, b"fenced body");
}
#[tokio::test]
async fn gc_fence_completed_delete_cannot_reuse_old_receipt_after_list_clears() {
    let f = fixture().await;
    let batch = schedule_gc(&f).await;
    f.local
        .collect_generation(batch.generation, &batch.chunks)
        .unwrap();
    let cleared = f
        .service
        .collect_garbage(
            "writer".into(),
            "writer-session".into(),
            batch.generation,
            vec![],
        )
        .await
        .unwrap();
    assert!(cleared.chunks.is_empty());
    assert_eq!(cleared.generation, batch.generation);
    let error = f
        .service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code(), afs_error::META_DFS_GC_CHANGED);
    assert!(
        f.service
            .get_inode(f.commit.inode_id.clone())
            .await
            .unwrap()
            .head_version
            .is_none()
    );
}
#[tokio::test]
async fn gc_fence_old_inflight_delete_rejected_even_with_matching_state_version() {
    let mut f = fixture().await;
    let _batch = schedule_gc(&f).await;
    // Model a deletion already in flight when this write generation began.
    f.commit.gc_fence = Some(f.service.current_gc_fence().await.unwrap());
    assert!(
        f.service
            .commit_file_version("writer".into(), f.commit.clone())
            .await
            .is_err()
    );
    assert!(
        f.service
            .get_inode(f.commit.inode_id)
            .await
            .unwrap()
            .head_version
            .is_none()
    );
}
#[tokio::test]
async fn gc_fence_atomic_condition_rejects_gc_between_check_and_publish() {
    let f = fixture().await;
    let mut txn = MetaTxn::new(
        RequestKey::new("race", "publish"),
        StoreOperation::DfsCommitFileVersion,
    );
    txn.conditions
        .push(TxnCondition::RequestAbsent(txn.request.clone()));
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: txn.request.clone(),
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }));
    txn.conditions
        .push(f.commit.gc_fence.unwrap().condition().unwrap());
    txn.mutations
        .push(TxnMutation::Put(MetaEntity::DfsFileVersion(
            f.commit.file_version.clone(),
        )));
    let _batch = schedule_gc(&f).await;
    assert!(matches!(
        f.store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::ConditionFailed { .. }
    ));
    assert!(
        f.store
            .read(MetaRead::DfsFileVersion(f.commit.file_version.id))
            .await
            .unwrap()
            .entity
            .is_none()
    );
}
#[tokio::test]
async fn gc_fence_publish_first_prevents_later_gc_selecting_chunk() {
    let f = fixture().await;
    f.service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap();
    let batch = f
        .service
        .collect_garbage(
            "writer".into(),
            "writer-session".into(),
            0,
            vec![f.staged.chunk.clone()],
        )
        .await
        .unwrap();
    assert!(!batch.chunks.contains(&f.staged.chunk.id));
}
#[tokio::test]
async fn gc_fence_lost_reply_replays_success_even_after_gc_state_changes() {
    let f = fixture().await;
    let original = f
        .service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap();
    let garbage = StagedChunk::new(OperationId::new("garbage"), b"unreferenced".to_vec());
    f.service
        .collect_garbage(
            "writer".into(),
            "writer-session".into(),
            0,
            vec![garbage.chunk],
        )
        .await
        .unwrap();
    assert_ne!(
        Some(f.service.current_gc_fence().await.unwrap()),
        f.commit.gc_fence
    );
    let replay = f
        .service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap();
    assert_eq!(original, replay);
}
#[test]
fn gc_fence_wire_is_explicit_and_rejects_missing_kind() {
    for fence in [GcFence::Absent, GcFence::Revision(9)] {
        assert_eq!(GcFence::from_wire(fence.wire()).unwrap(), fence);
    }
    assert!(GcFence::from_wire(afs_protocol::meta::DfsGcFence { kind: None }).is_err());
}

#[tokio::test]
async fn crc32c_meta_commit_and_gc_preserve_typed_live_object() {
    let mut f = fixture().await;
    let item =
        StagedChunk::new_crc32c(f.staged.operation_id.clone(), f.staged.bytes().to_vec()).unwrap();
    let snapshot = f.service.placement_snapshot("writer".into()).await.unwrap();
    let group = snapshot
        .replica_groups
        .iter()
        .find(|g| g.targets[0].node_id == "writer")
        .unwrap();
    let ack = f
        .local
        .persist(
            &item,
            &group.targets[0],
            snapshot.revision,
            group.placement_epoch,
        )
        .unwrap();
    f.commit.layout_root.inline_extents[0].chunk_id = item.chunk.id.clone();
    f.commit.chunk_receipts[0].chunk = item.chunk.clone();
    f.commit.chunk_receipts[0].durable_acks = vec![ack];
    f.service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap();
    let gc = f
        .service
        .collect_garbage(
            "writer".into(),
            "writer-session".into(),
            0,
            vec![item.chunk.clone()],
        )
        .await
        .unwrap();
    assert!(!gc.chunks.contains(&item.chunk.id));
}
