use super::*;
use crate::dfs::{ChunkEncoding, ChunkId, ContentDigest, DigestAlgorithm, Extent};
use std::collections::BTreeMap;
fn inode(id: &str, nlink: u32, head: &str) -> MetaEntity {
    MetaEntity::DfsInode(InodeRecord {
        namespace_id: NamespaceId::new("ns"),
        inode_id: InodeId::new(id),
        kind: InodeKind::Regular,
        attributes: InodeAttributes {
            mode: 0o600,
            uid: 0,
            gid: 0,
            nlink,
            atime_unix_ms: 0,
            mtime_unix_ms: 0,
            ctime_unix_ms: 0,
        },
        head_version: Some(FileVersionId::new(head)),
        symlink_target: None,
        xattrs: BTreeMap::new(),
        revision: 1,
    })
}
fn version(id: &str, owner: &str, chunk: &str) -> Vec<MetaEntity> {
    vec![
        MetaEntity::DfsFileVersion(FileVersion {
            id: FileVersionId::new(id),
            inode_id: InodeId::new(owner),
            parent_version: None,
            length: 4,
            layout_root: LayoutRootId::new(id),
            created_at_unix_ms: 0,
        }),
        MetaEntity::DfsLayoutRoot(LayoutRoot {
            id: LayoutRootId::new(id),
            file_length: 4,
            inline_extents: vec![Extent {
                file_offset: 0,
                length: 4,
                chunk_id: ChunkId::new(chunk),
                chunk_offset: 0,
            }],
        }),
    ]
}
fn chunk(id: &str) -> MetaEntity {
    MetaEntity::DfsChunk(ChunkObject {
        id: ChunkId::new(id),
        length: 4,
        encoding: ChunkEncoding::Raw,
        content_digest: ContentDigest {
            algorithm: DigestAlgorithm::Blake3,
            bytes: [0; 32],
        },
    })
}
fn pin(id: &str) -> GcReference {
    GcReference {
        open_read: None,

        node_id: "reader".into(),
        session_id: "session".into(),
        reference_id: "open".into(),
        inode_id: Some(InodeId::new(id)),
        publication: false,
        chunks: vec![],
        retired_at_unix_ms: None,
    }
}
#[test]
fn open_authority_closed_context_keeps_gc_pin_until_last_user_and_grace() {
    let mut entities = vec![inode("mapped", 0, "v2"), chunk("old"), chunk("new")];
    entities.extend(version("v1", "mapped", "old"));
    entities.extend(version("v2", "mapped", "new"));
    let mut reference = pin("mapped");
    reference.open_read = Some(crate::dfs::OpenReadIdentity {
        version_id: FileVersionId::new("v1"),
        layout_root_id: LayoutRootId::new("v1"),
        closed: true,
    });
    assert!(
        plan_collection(&entities, &[reference.clone()], 100, 10)
            .chunks
            .is_empty()
    );
    reference.retired_at_unix_ms = Some(100);
    assert!(
        plan_collection(&entities, &[reference.clone()], 109, 10)
            .chunks
            .is_empty()
    );
    assert_eq!(
        plan_collection(&entities, &[reference], 110, 10)
            .chunks
            .len(),
        2
    );
}
#[test]
fn cow_clone_retains_shared_chunks_after_source_is_unlinked() {
    let mut entities = vec![
        inode("source", 0, "v1"),
        inode("clone", 1, "v2"),
        chunk("shared"),
        chunk("garbage"),
    ];
    entities.extend(version("v1", "source", "shared"));
    entities.extend(version("v2", "clone", "shared"));
    let plan = plan_collection(&entities, &[], 100, 10);
    assert!(
        plan.keys
            .contains(&MetaKey::DfsFileVersion(FileVersionId::new("v1")))
    );
    assert!(
        !plan
            .keys
            .contains(&MetaKey::DfsFileVersion(FileVersionId::new("v2")))
    );
    assert_eq!(plan.chunks, vec![ChunkId::new("garbage")]);
}
#[test]
fn open_inode_retains_old_versions_until_last_reference_and_grace_are_gone() {
    let mut entities = vec![inode("mapped", 0, "v2"), chunk("old"), chunk("new")];
    entities.extend(version("v1", "mapped", "old"));
    entities.extend(version("v2", "mapped", "new"));
    let mut reference = pin("mapped");
    assert!(
        plan_collection(&entities, &[reference.clone()], 100, 10)
            .chunks
            .is_empty()
    );
    reference.retired_at_unix_ms = Some(95);
    assert!(
        plan_collection(&entities, &[reference.clone()], 100, 10)
            .chunks
            .is_empty()
    );
    let plan = plan_collection(&entities, &[reference], 106, 10);
    assert_eq!(
        plan.chunks.len(),
        2,
        "both versions become unreachable only after close and grace"
    );
}

fn retired_reference(id: &str, retired_at: u64) -> (MetaEntity, GcReference) {
    let mut reference = pin("mapped");
    reference.reference_id = id.into();
    reference.retired_at_unix_ms = Some(retired_at);
    (MetaEntity::DfsReference(reference.clone()), reference)
}

#[test]
fn retired_write_reference_record_outlives_grace_until_admission_window_closes() {
    let (entity, reference) = retired_reference("write:generation-op", 100);
    let entities = vec![entity];
    assert!(
        plan_collection(&entities, std::slice::from_ref(&reference), 110, 10)
            .keys
            .is_empty(),
        "data grace has passed but the record stays as a tombstone fencing late admissions"
    );
    assert!(
        plan_collection(
            &entities,
            std::slice::from_ref(&reference),
            100 + ADMISSION_HORIZON_MS,
            10
        )
        .keys
        .is_empty(),
        "the admission horizon alone is not enough while node clocks can skew"
    );
    assert_eq!(
        plan_collection(
            &entities,
            &[reference],
            100 + REFERENCE_RECORD_RETENTION_MS,
            10
        )
        .keys
        .len(),
        1,
        "record reclaims only after horizon + skew, when the deadline gate alone provably rejects every replay"
    );
}

#[test]
fn long_data_grace_extends_reference_record_life_beyond_retention() {
    let (entity, reference) = retired_reference("open:reader-1", 100);
    let entities = vec![entity];
    let long_grace = 300_000_u64;
    assert!(
        plan_collection(
            &entities,
            std::slice::from_ref(&reference),
            100 + REFERENCE_RECORD_RETENTION_MS,
            long_grace
        )
        .keys
        .is_empty(),
        "tombstone retention has elapsed but the record still protects data under the long grace"
    );
    assert_eq!(
        plan_collection(&entities, &[reference], 100 + long_grace, long_grace)
            .keys
            .len(),
        1,
        "record converges only when the configured data grace also expires"
    );
}

#[tokio::test]
async fn record_past_retention_but_still_protecting_data_survives_consecutive_collections() {
    // Discriminating window: retirement happened 150s ago, so the 120s
    // tombstone retention has already elapsed, but the configured 200s
    // data grace has not. A record deleted at the fixed retention would
    // leave the second collection blind to the still-protected data.
    let entities_ref = vec![inode("i", 1, "v2"), chunk("old"), chunk("new")];
    let mut entities = entities_ref;
    entities.extend(version("v1", "i", "old"));
    entities.extend(version("v2", "i", "new"));
    let mut reference = pin("i");
    reference.retired_at_unix_ms = Some(now_unix_ms().saturating_sub(150_000));
    entities.push(MetaEntity::DfsReference(reference));
    let (store, _) = seeded_store(entities).await;
    let service = DfsService::new(store.clone()).with_gc_grace_ms(200_000);
    for _ in 0..2 {
        let gc = service
            .collect_garbage("n".into(), "s".into(), 0, vec![])
            .await
            .unwrap();
        assert!(
            gc.chunks.is_empty(),
            "data under the unexpired 200s grace must not reclaim"
        );
        let (_, snapshot) = store.read_view().await.unwrap().snapshot().unwrap();
        assert!(
            snapshot
                .iter()
                .any(|entity| matches!(entity, MetaEntity::DfsReference(value)
                if value.retired_at_unix_ms.is_some_and(|retired| {
                    retired.saturating_add(200_000) > now_unix_ms()
                }))),
            "past tombstone retention but inside the data grace, the record must survive every collection round"
        );
    }
}

#[tokio::test]
async fn expired_replay_after_real_tombstone_deletion_is_rejected() {
    let mut tombstone = pending_reference("late-open");
    let aged = now_unix_ms().saturating_sub(1_000_000);
    let MetaEntity::DfsReference(value) = &mut tombstone else {
        unreachable!("pending_reference builds a DfsReference");
    };
    value.retired_at_unix_ms = Some(aged);
    let (store, _) = seeded_store(vec![inode("i", 1, "v"), tombstone]).await;
    let service = DfsService::new(store.clone()).with_gc_grace_ms(0);
    service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    let (_, snapshot) = store.read_view().await.unwrap().snapshot().unwrap();
    assert!(
        !snapshot
            .iter()
            .any(|entity| matches!(entity, MetaEntity::DfsReference(_))),
        "the aged tombstone must be genuinely deleted before the replay"
    );
    let mut late = update("late-open");
    late.admission_deadline_unix_ms = now_unix_ms().saturating_sub(1);
    assert_eq!(
        service.update_gc_reference(late).await.unwrap_err().code(),
        afs_error::META_DFS_REFERENCE_ADMISSION_EXPIRED,
        "with the record gone the deadline gate alone must reject the replay"
    );
}

#[test]
fn write_and_read_reference_records_die_together_after_retirement() {
    let (write_entity, write_reference) = retired_reference("write:generation-op", 100);
    let (read_entity, read_reference) = retired_reference("open:reader-1", 100);
    let entities = vec![write_entity, read_entity];
    let references = vec![write_reference, read_reference];
    assert!(
        plan_collection(
            &entities,
            &references,
            100 + REFERENCE_RECORD_RETENTION_MS - 1,
            10
        )
        .keys
        .is_empty()
    );
    assert_eq!(
        plan_collection(
            &entities,
            &references,
            100 + REFERENCE_RECORD_RETENTION_MS,
            10
        )
        .keys
        .len(),
        2,
        "write and read references retired together must be reclaimed together on the derived bound"
    );
}

fn generation_chunk(operation: &str, suffix: char) -> MetaEntity {
    let generation = blake3::hash(operation.as_bytes()).to_hex()[..32].to_string();
    let mut staged = crate::node::chunk::StagedChunk::new_crc32c(
        OperationId::new(operation),
        b"uncommitted inventory".to_vec(),
    )
    .unwrap();
    staged.chunk.id = ChunkId::new(format!(
        "crc32c-v2-{generation}-{}",
        suffix.to_string().repeat(32)
    ));
    MetaEntity::DfsChunk(staged.chunk)
}

#[test]
fn generation_reference_protects_unregistered_inventory_on_all_replicas() {
    let mut reference = pin("writer");
    reference.publication = true;
    reference.reference_id = "publish:generation-A".into();
    let entities = vec![
        generation_chunk("generation-A", 'a'),
        generation_chunk("generation-A", 'b'),
    ];
    let plan = plan_collection(&entities, &[reference], 100, 10);
    assert!(
        plan.chunks.is_empty(),
        "one generation admission must protect new replica inventories without per-chunk registration"
    );
}

#[test]
fn generation_reference_does_not_protect_another_write() {
    let mut reference = pin("writer");
    reference.publication = true;
    reference.reference_id = "publish:generation-A".into();
    let a = generation_chunk("generation-A", 'a');
    let b = generation_chunk("generation-B", 'b');
    let MetaEntity::DfsChunk(expected) = &b else {
        unreachable!()
    };
    let plan = plan_collection(&[a, b.clone()], &[reference], 100, 10);
    assert_eq!(
        plan.chunks,
        vec![expected.id.clone()],
        "unrelated orphan still must be reclaimable"
    );
}

#[test]
fn active_publication_retains_uploaded_but_uncommitted_chunks() {
    let entities = vec![chunk("pending")];
    let mut reference = pin("writer");
    reference.publication = true;
    let MetaEntity::DfsChunk(value) = chunk("pending") else {
        unreachable!()
    };
    reference.chunks.push(value);
    assert!(
        plan_collection(&entities, &[reference], 100, 10)
            .chunks
            .is_empty()
    );
    assert_eq!(
        plan_collection(&entities, &[], 100, 10).chunks,
        vec![ChunkId::new("pending")]
    );
}
async fn seeded_store(
    entities: Vec<MetaEntity>,
) -> (
    Arc<crate::meta::store::Store>,
    Arc<crate::meta::store::journal::MemoryLog>,
) {
    use crate::meta::store::{NodeSessionLease, Store, journal::MemoryLog};
    let backend = Arc::new(MemoryLog::new(Default::default()).unwrap());
    let store = Arc::new(Store::open_log(backend.clone()).await.unwrap());
    store
        .register_node_session(
            RequestKey::new("n", "session"),
            NodeSessionLease {
                node_id: "n".into(),
                session_id: "s".into(),
                grpc_addr: "http://n:1".into(),
                data_addr: "http://n:2".into(),
                rest_addr: "http://n:3".into(),
                ownerfs_enabled: false,
                storage_devices: vec![],
                lease_ttl: std::time::Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    let key = RequestKey::new("seed", "entities");
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCreate);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations = entities.into_iter().map(TxnMutation::Put).collect();
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::DfsCreate,
            result: OperationResult::Empty,
        }));
    store.compare_and_commit(txn).await.unwrap();
    (store, backend)
}
fn canonical_chunk() -> ChunkObject {
    ChunkObject::from_content_addressed_id(&ChunkId::new(format!(
        "blake3-{}-4",
        blake3::hash(b"data").to_hex()
    )))
    .unwrap()
}
fn update(id: &str) -> UpdateGcReference {
    UpdateGcReference {
        open_read: None,
        close_open_read: false,

        node_id: "n".into(),
        session_id: "s".into(),
        reference_id: id.into(),
        inode_id: Some(InodeId::new("i")),
        version_id: None,
        publication: true,
        chunks: vec![],
        release: false,
        admission_deadline_unix_ms: now_unix_ms().saturating_add(30_000),
    }
}
fn pending_reference(id: &str) -> MetaEntity {
    MetaEntity::DfsReference(GcReference {
        open_read: None,

        node_id: "n".into(),
        session_id: "s".into(),
        reference_id: id.into(),
        inode_id: Some(InodeId::new("i")),
        publication: true,
        chunks: vec![],
        retired_at_unix_ms: None,
    })
}
#[tokio::test]
async fn concurrent_publication_reservations_keep_every_chunk_without_retry_exhaustion() {
    let (store, _) = seeded_store(vec![inode("i", 1, "v")]).await;
    let service = DfsService::new(store.clone());
    service
        .update_gc_reference(update("publish:parallel"))
        .await
        .unwrap();
    let start = Arc::new(tokio::sync::Barrier::new(256));
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..256 {
        let service = service.clone();
        let start = start.clone();
        tasks.spawn(async move {
            let bytes = format!("parallel-{index}");
            let chunk = ChunkObject::from_content_addressed_id(&ChunkId::new(format!(
                "blake3-{}-{}",
                blake3::hash(bytes.as_bytes()).to_hex(),
                bytes.len()
            )))
            .unwrap();
            let mut reservation = update("publish:parallel");
            reservation.inode_id = None;
            reservation.chunks = vec![chunk];
            start.wait().await;
            service.update_gc_reference(reservation).await
        });
    }
    let mut errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result.unwrap() {
            errors.push(error.to_string());
        }
    }
    assert!(
        errors.is_empty(),
        "{} publication reservations failed; first: {:?}",
        errors.len(),
        errors.first()
    );
    let reference = store
        .read_view()
        .await
        .unwrap()
        .lookup(&ref_key("n", "s", "publish:parallel"))
        .unwrap();
    let Some(MetaEntity::DfsReference(reference)) = reference.entity else {
        panic!("publication protection missing");
    };
    assert_eq!(reference.chunks.len(), 256);
    assert_eq!(
        reference
            .chunks
            .iter()
            .map(|chunk| &chunk.id)
            .collect::<BTreeSet<_>>()
            .len(),
        256,
        "every distinct uploaded chunk must remain protected"
    );
}
fn admission_conditions(view: &crate::meta::store::MetaReadView, id: &str) -> Vec<TxnCondition> {
    let mut conditions = vec![TxnCondition::NodeSessionCurrent {
        node_id: "n".into(),
        session_id: "s".into(),
    }];
    for key in [
        ref_key("n", "s", id),
        MetaKey::DfsInode(InodeId::new("i")),
        MetaKey::DfsGc,
    ] {
        reference_entity(view, key, &mut conditions).unwrap();
    }
    conditions
}
#[tokio::test]
async fn reference_conditions_ignore_unrelated_commits_but_invalidate_stale_collection() {
    let (store, _) = seeded_store(vec![inode("i", 1, "v")]).await;
    let service = DfsService::new(store.clone());
    let view = store.read_view().await.unwrap();
    let conditions = admission_conditions(&view, "reader");
    service
        .update_gc_reference(update("unrelated-reference"))
        .await
        .unwrap();
    let collection_revision = store.read_view().await.unwrap().snapshot().unwrap().0;
    assert!(
        service
            .gc_commit_checked(
                conditions,
                StoreOperation::DfsUpdateReference,
                vec![TxnMutation::Put(pending_reference("reader"))]
            )
            .await
            .unwrap()
    );
    assert!(
        !service
            .gc_commit(
                collection_revision,
                StoreOperation::DfsCollectGarbage,
                vec![TxnMutation::Delete(MetaKey::DfsInode(InodeId::new("i")))]
            )
            .await
            .unwrap(),
        "a collector prepared before admission must retry its reachability scan"
    );
    assert!(
        store
            .read(MetaRead::DfsInode(InodeId::new("i")))
            .await
            .unwrap()
            .entity
            .is_some()
    );
}
#[tokio::test]
async fn reference_conditions_reject_release_collection_and_session_fences() {
    for fence in ["release", "collection", "session"] {
        let (store, _) = seeded_store(vec![inode("i", 0, "v")]).await;
        let service = DfsService::new(store.clone()).with_gc_grace_ms(0);
        let view = store.read_view().await.unwrap();
        let conditions = admission_conditions(&view, "late-reader");
        match fence {
            "release" => {
                let mut release = update("late-reader");
                release.release = true;
                service.update_gc_reference(release).await.unwrap();
            }
            "collection" => {
                service
                    .collect_garbage("n".into(), "s".into(), 0, vec![])
                    .await
                    .unwrap();
            }
            _ => {
                store
                    .register_node_session(
                        RequestKey::new("n", "replace"),
                        crate::meta::store::NodeSessionLease {
                            node_id: "n".into(),
                            session_id: "new-session".into(),
                            grpc_addr: "http://n:1".into(),
                            data_addr: "http://n:2".into(),
                            rest_addr: "http://n:3".into(),
                            ownerfs_enabled: false,
                            storage_devices: vec![],
                            lease_ttl: std::time::Duration::from_secs(30),
                        },
                    )
                    .await
                    .unwrap();
            }
        }
        assert!(
            !service
                .gc_commit_checked(
                    conditions,
                    StoreOperation::DfsUpdateReference,
                    vec![TxnMutation::Put(pending_reference("late-reader"))]
                )
                .await
                .unwrap(),
            "{fence}"
        );
    }
}
#[tokio::test]
async fn publication_conditions_reject_reserved_chunk_reclamation() {
    let object = canonical_chunk();
    let (store, _) = seeded_store(vec![
        inode("i", 1, "v"),
        MetaEntity::DfsChunk(object.clone()),
    ])
    .await;
    let service = DfsService::new(store.clone()).with_gc_grace_ms(0);
    let view = store.read_view().await.unwrap();
    let mut conditions = admission_conditions(&view, "publication");
    reference_entity(&view, MetaKey::DfsChunk(object.id.clone()), &mut conditions).unwrap();
    let barrier = service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    assert!(barrier.chunks.contains(&object.id));
    let mut reference = pending_reference("publication");
    if let MetaEntity::DfsReference(value) = &mut reference {
        value.chunks.push(object);
    }
    assert!(
        !service
            .gc_commit_checked(
                conditions,
                StoreOperation::DfsUpdateReference,
                vec![TxnMutation::Put(reference)]
            )
            .await
            .unwrap()
    );
}
#[tokio::test]
async fn reference_admission_requires_its_inode_version_and_current_session() {
    let mut entities = vec![inode("i", 1, "v")];
    entities.extend(version("v", "another-inode", "chunk"));
    let (store, _) = seeded_store(entities).await;
    let service = DfsService::new(store.clone());
    let mut missing = update("missing-inode");
    missing.inode_id = Some(InodeId::new("collected"));
    assert!(service.update_gc_reference(missing).await.is_err());
    let mut wrong_version = update("wrong-version");
    wrong_version.version_id = Some(FileVersionId::new("v"));
    assert!(service.update_gc_reference(wrong_version).await.is_err());
    service
        .update_gc_reference(update("valid-inode"))
        .await
        .unwrap();
    store
        .register_node_session(
            RequestKey::new("n", "replace-session"),
            crate::meta::store::NodeSessionLease {
                node_id: "n".into(),
                session_id: "s2".into(),
                grpc_addr: "http://n:1".into(),
                data_addr: "http://n:2".into(),
                rest_addr: "http://n:3".into(),
                ownerfs_enabled: false,
                storage_devices: vec![],
                lease_ttl: std::time::Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    assert!(
        service
            .update_gc_reference(update("stale-session"))
            .await
            .is_err()
    );
    let mut current = update("current-session");
    current.session_id = "s2".into();
    service.update_gc_reference(current).await.unwrap();
    let view = store.read_view().await.unwrap();
    for id in ["missing-inode", "wrong-version", "stale-session"] {
        assert!(
            view.lookup(&ref_key("n", "s", id))
                .unwrap()
                .entity
                .is_none()
        );
    }
}
#[tokio::test]
async fn publication_and_lost_gc_ack_are_fenced_across_meta_reopen() {
    use crate::meta::store::Store;
    let chunk = canonical_chunk();
    let (store, backend) = seeded_store(vec![inode("i", 1, "v")]).await;
    let service =
        DfsService::with_replication_config(store.clone(), ReplicationConfig::local_single_copy())
            .with_gc_grace_ms(0);
    service
        .update_gc_reference(update("publish:p"))
        .await
        .unwrap();
    let mut reserve = update("publish:p");
    reserve.inode_id = None;
    reserve.chunks = vec![chunk.clone()];
    service.update_gc_reference(reserve.clone()).await.unwrap();
    assert!(
        service
            .collect_garbage("n".into(), "s".into(), 0, vec![chunk.clone()])
            .await
            .unwrap()
            .chunks
            .is_empty()
    );
    let mut release = update("publish:p");
    release.release = true;
    service.update_gc_reference(release).await.unwrap();
    let ticket = service
        .collect_garbage("n".into(), "s".into(), 0, vec![chunk.clone()])
        .await
        .unwrap();
    assert_eq!(ticket.chunks, vec![chunk.id.clone()]);
    drop(service);
    drop(store);
    // The backend image includes pending deletion and reference records.
    let reopened = Arc::new(Store::open_log(backend).await.unwrap());
    let service =
        DfsService::with_replication_config(reopened, ReplicationConfig::local_single_copy())
            .with_gc_grace_ms(0);
    service
        .update_gc_reference(update("publish:new"))
        .await
        .unwrap();
    reserve.reference_id = "publish:new".into();
    assert_eq!(
        service
            .update_gc_reference(reserve.clone())
            .await
            .unwrap_err()
            .code(),
        afs_error::META_DFS_GC_BUSY
    );
    let repeated = service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    assert_eq!(repeated, ticket);
    let acknowledged = service
        .collect_garbage("n".into(), "s".into(), ticket.generation, vec![])
        .await
        .unwrap();
    assert!(acknowledged.chunks.is_empty());
    service
        .collect_garbage("n".into(), "s".into(), ticket.generation, vec![])
        .await
        .unwrap();
    service.update_gc_reference(reserve).await.unwrap();
}
#[tokio::test]
async fn session_replacement_retires_only_fenced_refs_after_the_old_authority_deadline() {
    let chunk = canonical_chunk();
    let mut reference = pin("i");
    reference.node_id = "n".into();
    reference.session_id = "old".into();
    reference.chunks = vec![chunk.clone()];
    let deadline = now_unix_ms() + 30_000;
    let old = MetaEntity::NodeSession(NodeSession {
        node_id: "n".into(),
        session_id: "old".into(),
        grpc_addr: "http://n:1".into(),
        data_addr: "http://n:2".into(),
        rest_addr: "http://n:3".into(),
        ownerfs_enabled: false,
        storage_devices: vec![],
        lease_epoch: 0,
        expires_at_unix_ms: deadline,
    });
    let (store, _) = seeded_store(vec![
        inode("i", 0, "v"),
        MetaEntity::DfsChunk(chunk.clone()),
        MetaEntity::DfsReference(reference),
        old,
    ])
    .await;
    let service = DfsService::new(store.clone());
    let ticket = service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    assert!(ticket.chunks.is_empty());
    let (_, entities) = store.read_view().await.unwrap().snapshot().unwrap();
    let retired = entities
        .iter()
        .find_map(|entity| match entity {
            MetaEntity::DfsReference(reference) => Some(reference),
            _ => None,
        })
        .unwrap();
    assert!(retired.retired_at_unix_ms.unwrap() >= deadline);
    assert!(
        plan_collection(
            &entities,
            std::slice::from_ref(retired),
            deadline + 59_999,
            60_000
        )
        .chunks
        .is_empty()
    );
    assert_eq!(
        plan_collection(
            &entities,
            std::slice::from_ref(retired),
            deadline + 60_001,
            60_000
        )
        .chunks,
        vec![chunk.id]
    );
    let mut wrong = update("open:stale");
    wrong.session_id = "old".into();
    assert!(service.update_gc_reference(wrong).await.is_err());
}
#[test]
fn heartbeat_expiry_without_session_replacement_never_retires_an_open_reader() {
    let mut reference = pin("i");
    let MetaEntity::DfsChunk(object) = chunk("live") else {
        unreachable!()
    };
    reference.chunks = vec![object];
    let entities = vec![inode("i", 0, "v"), chunk("live")];
    assert!(
        plan_collection(&entities, &[reference], u64::MAX - 60_000, 60_000)
            .chunks
            .is_empty()
    );
}
#[tokio::test]
async fn release_before_delayed_admission_is_a_durable_rejection() {
    use crate::meta::store::Store;
    let (store, backend) = seeded_store(vec![inode("i", 1, "v")]).await;
    let service = DfsService::new(store.clone());
    let mut release = update("late-open");
    release.release = true;
    release.inode_id = None;
    service.update_gc_reference(release).await.unwrap();
    drop(service);
    drop(store);
    let service = DfsService::new(Arc::new(Store::open_log(backend).await.unwrap()));
    assert!(
        service
            .update_gc_reference(update("late-open"))
            .await
            .is_err(),
        "a delayed lost-reply admission cannot resurrect an already released reference"
    );
}

#[tokio::test]
async fn expired_delayed_admission_cannot_reappear_after_tombstone_collection() {
    let (store, _) = seeded_store(vec![inode("i", 1, "v")]).await;
    let service = DfsService::new(store.clone()).with_gc_grace_ms(0);
    let mut release = update("late-open");
    release.release = true;
    release.inode_id = None;
    service.update_gc_reference(release).await.unwrap();
    service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    let (_, entities) = store.read_view().await.unwrap().snapshot().unwrap();
    assert!(
        entities
            .iter()
            .any(|entity| matches!(entity, MetaEntity::DfsReference(value)
            if value.retired_at_unix_ms.is_some_and(|retired| {
                retired.saturating_add(REFERENCE_RECORD_RETENTION_MS) > now_unix_ms()
            }))),
        "retired tombstones must outlive the admission window instead of converging at data grace"
    );
    let mut late = update("late-open");
    late.admission_deadline_unix_ms = now_unix_ms().saturating_sub(1);
    assert!(
        service.update_gc_reference(late).await.is_err(),
        "an expired replay must stay rejected while the tombstone is retained"
    );
}
#[tokio::test]
async fn active_publication_outlives_admission_but_cannot_change_identity() {
    let (store, _) = seeded_store(vec![inode("i", 1, "v")]).await;
    let service = DfsService::new(store);
    let mut invalid = update("too-far");
    invalid.admission_deadline_unix_ms = now_unix_ms() + 120_000;
    assert!(service.update_gc_reference(invalid).await.is_err());
    service.update_gc_reference(update("writer")).await.unwrap();
    let mut chunks = update("writer");
    chunks.inode_id = None;
    chunks.admission_deadline_unix_ms = 0;
    chunks.chunks = vec![canonical_chunk()];
    service.update_gc_reference(chunks).await.unwrap();
    let mut reused = update("writer");
    reused.inode_id = Some(InodeId::new("different"));
    assert!(service.update_gc_reference(reused).await.is_err());
}
