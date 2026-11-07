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

/// The persisted id of an admitted write generation (see `update_gc_reference`).
fn write_reference(operation: &str) -> String {
    format!(
        "write:{}",
        ChunkObject::generation_id(&OperationId::new(operation))
    )
}

#[test]
fn generation_reference_protects_unregistered_inventory_on_all_replicas() {
    let mut reference = pin("writer");
    reference.publication = true;
    reference.reference_id = write_reference("generation-A");
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
    reference.reference_id = write_reference("generation-A");
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

fn storage_device(node: &str) -> crate::dfs::StorageDeviceDescriptor {
    crate::dfs::StorageDeviceDescriptor {
        device_id: format!("{node}-device"),
        device_epoch: 1,
        catalog_revision: 1,
        failure_domain: format!("{node}-domain"),
    }
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
                storage_devices: vec![storage_device("n")],
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
        node_id: "n".into(),
        session_id: "s".into(),
        reference_id: id.into(),
        inode_id: Some(InodeId::new("i")),
        version_id: None,
        publication: true,
        admission_deadline_unix_ms: now_unix_ms().saturating_add(30_000),
    }
}
#[tokio::test]
async fn create_admits_its_handle_reference_atomically_and_a_replay_keeps_one() {
    use crate::dfs::created_handle_reference_id;
    use crate::meta::dfs::CreateFileRequest as Create;
    let (store, _) = seeded_store(vec![MetaEntity::DfsInode(InodeRecord {
        kind: InodeKind::Directory,
        ..match inode("parent", 2, "v") {
            MetaEntity::DfsInode(value) => value,
            _ => unreachable!(),
        }
    })])
    .await;
    let service = DfsService::new(store.clone());
    let operation = OperationId::new("create-1");
    let request = Create {
        caller_id: "n".into(),
        owner_session_id: "s".into(),
        operation_id: operation.clone(),
        namespace_id: NamespaceId::new("ns"),
        parent_inode_id: InodeId::new("parent"),
        name: b"file".to_vec(),
        attributes: InodeAttributes {
            mode: 0o600,
            uid: 0,
            gid: 0,
            nlink: 1,
            atime_unix_ms: 0,
            mtime_unix_ms: 0,
            ctime_unix_ms: 0,
        },
        lease_seconds: 30,
    };
    let (created, _) = service.create(request.clone()).await.unwrap();
    let key = ref_key("n", "s", &created_handle_reference_id(&operation));
    let stored = |entities: &[MetaEntity]| {
        entities
            .iter()
            .filter(|entity| crate::meta::store::entity_key(entity) == key)
            .count()
    };
    let (revision, entities) = store.read_view().await.unwrap().snapshot().unwrap();
    assert_eq!(stored(&entities), 1, "the reference is in the create txn");
    let reference = entities
        .iter()
        .find_map(|entity| match entity {
            MetaEntity::DfsReference(value) if crate::meta::store::entity_key(entity) == key => {
                Some(value.clone())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(reference.inode_id, Some(created.inode_id.clone()));
    assert!(reference.retired_at_unix_ms.is_none());
    // A lost-reply retry replays the outcome and writes nothing.
    service.create(request).await.unwrap();
    assert_eq!(
        store.read_view().await.unwrap().snapshot().unwrap().0,
        revision
    );
    // Until it is released GC treats the new inode like any opened one.
    let plan = plan_collection(
        &entities,
        std::slice::from_ref(&reference),
        now_unix_ms(),
        0,
    );
    assert!(!plan.keys.iter().any(|key| matches!(key,
        MetaKey::DfsInode(id) if *id == created.inode_id)));
    service
        .release_gc_references(
            "n".into(),
            "s".into(),
            vec![created_handle_reference_id(&operation)],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn one_release_call_retires_a_batch_in_one_transaction() {
    let (store, _) = seeded_store(vec![inode("i", 1, "v")]).await;
    let service = DfsService::new(store.clone());
    for id in ["a", "b"] {
        service.update_gc_reference(update(id)).await.unwrap();
    }
    let before = store.read_view().await.unwrap().snapshot().unwrap().0;
    service
        .release_gc_references(
            "n".into(),
            "s".into(),
            vec!["a".into(), "b".into(), "never-admitted".into()],
        )
        .await
        .unwrap();
    let (after, entities) = store.read_view().await.unwrap().snapshot().unwrap();
    assert_eq!(after.0, before.0 + 1, "one transaction for the whole batch");
    let retired = entities
        .iter()
        .filter_map(|entity| match entity {
            MetaEntity::DfsReference(reference) => Some(reference),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(retired.len(), 3);
    assert!(retired.iter().all(|r| r.retired_at_unix_ms.is_some()));
    // A retried batch changes nothing; the tombstone fences a late admission.
    service
        .release_gc_references(
            "n".into(),
            "s".into(),
            vec!["a".into(), "b".into(), "never-admitted".into()],
        )
        .await
        .unwrap();
    assert_eq!(
        store.read_view().await.unwrap().snapshot().unwrap().0,
        after,
        "a retry writes nothing"
    );
    assert!(
        service
            .update_gc_reference(update("never-admitted"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_release_batch_stays_bounded_and_needs_the_current_session() {
    let (store, _) = seeded_store(vec![inode("i", 1, "v")]).await;
    let service = DfsService::new(store.clone());
    let too_many = (0..=MAX_RELEASE_BATCH)
        .map(|i| format!("open:{i}"))
        .collect();
    assert!(
        service
            .release_gc_references("n".into(), "s".into(), too_many)
            .await
            .is_err()
    );
    assert!(
        service
            .release_gc_references("n".into(), "stale".into(), vec!["a".into()])
            .await
            .is_err(),
        "a replaced session releases nothing; its references retire with the session"
    );
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
                service
                    .release_gc_references("n".into(), "s".into(), vec!["late-reader".into()])
                    .await
                    .unwrap();
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
async fn lost_gc_ack_is_fenced_across_meta_reopen() {
    use crate::meta::store::Store;
    let chunk = canonical_chunk();
    let (store, backend) = seeded_store(vec![
        inode("i", 1, "v"),
        MetaEntity::DfsChunk(chunk.clone()),
    ])
    .await;
    let service =
        DfsService::with_replication_config(store.clone(), ReplicationConfig::local_single_copy())
            .with_gc_grace_ms(0);
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
}
#[tokio::test]
async fn session_replacement_retires_only_fenced_refs_after_the_old_authority_deadline() {
    let chunk = canonical_chunk();
    let mut reference = pin("i");
    reference.node_id = "n".into();
    reference.session_id = "old".into();
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
    let mut entities = vec![
        inode("i", 0, "v"),
        MetaEntity::DfsChunk(chunk.clone()),
        MetaEntity::DfsReference(reference),
        old,
    ];
    // The reference protects the unlinked inode, its head and that chunk.
    entities.extend(version("v", "i", &chunk.id.0));
    let (store, _) = seeded_store(entities).await;
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
fn an_unretired_reference_protects_its_inode_at_any_time() {
    let reference = pin("i");
    let mut entities = vec![inode("i", 0, "v"), chunk("live")];
    entities.extend(version("v", "i", "live"));
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
    service
        .release_gc_references("n".into(), "s".into(), vec!["late-open".into()])
        .await
        .unwrap();
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
    service
        .release_gc_references("n".into(), "s".into(), vec!["late-open".into()])
        .await
        .unwrap();
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
    let mut refresh = update("writer");
    refresh.inode_id = None;
    refresh.admission_deadline_unix_ms = 0;
    service.update_gc_reference(refresh).await.unwrap();
    let mut reused = update("writer");
    reused.inode_id = Some(InodeId::new("different"));
    assert!(service.update_gc_reference(reused).await.is_err());
}

async fn register(
    store: &crate::meta::store::Store,
    node: &str,
    session: &str,
    ttl: std::time::Duration,
    devices: Vec<crate::dfs::StorageDeviceDescriptor>,
) {
    store
        .register_node_session(
            RequestKey::new(node, session),
            crate::meta::store::NodeSessionLease {
                node_id: node.into(),
                session_id: session.into(),
                grpc_addr: "http://x:1".into(),
                data_addr: "http://x:2".into(),
                rest_addr: "http://x:3".into(),
                ownerfs_enabled: false,
                storage_devices: devices,
                lease_ttl: ttl,
            },
        )
        .await
        .unwrap();
}

fn distinct_chunk(body: &[u8]) -> ChunkObject {
    ChunkObject::from_content_addressed_id(&ChunkId::new(format!(
        "blake3-{}-{}",
        blake3::hash(body).to_hex(),
        body.len()
    )))
    .unwrap()
}

fn gc_service(store: Arc<crate::meta::store::Store>) -> DfsService {
    DfsService::with_replication_config(store, ReplicationConfig::local_single_copy())
        .with_gc_grace_ms(0)
}

const SHORT_LEASE: std::time::Duration = std::time::Duration::from_millis(150);
const PAST_SHORT_LEASE: std::time::Duration = std::time::Duration::from_millis(200);

/// R55（S0 Q02）：开代后一个等待节点失联，存活节点的下一次调用把它剔除，这一代结束；
/// 之后的代只等存活节点。修复前这一代永远结束不了，之后的删除永不回收。
#[tokio::test]
async fn an_expired_waiter_no_longer_stalls_gc() {
    let first = distinct_chunk(b"first");
    let second = distinct_chunk(b"second");
    let (store, _) = seeded_store(vec![]).await;
    register(&store, "m", "m1", SHORT_LEASE, vec![storage_device("m")]).await;
    let service = gc_service(store.clone());
    let ticket = service
        .collect_garbage("n".into(), "s".into(), 0, vec![first.clone()])
        .await
        .unwrap();
    assert_eq!(ticket.waiting_nodes, vec!["m".to_owned(), "n".to_owned()]);
    assert_eq!(ticket.chunks, vec![first.id.clone()]);

    tokio::time::sleep(PAST_SHORT_LEASE).await;
    let acked = service
        .collect_garbage("n".into(), "s".into(), ticket.generation, vec![])
        .await
        .unwrap();
    assert!(acked.waiting_nodes.is_empty(), "{acked:?}");
    assert!(acked.chunks.is_empty(), "the deletion barrier is lifted");

    let next = service
        .collect_garbage(
            "n".into(),
            "s".into(),
            ticket.generation,
            vec![second.clone()],
        )
        .await
        .unwrap();
    assert_eq!(next.generation, ticket.generation + 1);
    assert_eq!(next.waiting_nodes, vec!["n".to_owned()]);
    assert_eq!(next.chunks, vec![second.id]);
}

/// 只有持有存储设备的存活节点才进等待集合：OwnerFs-only 节点不运行 Collector，等它就是永久停摆。
#[tokio::test]
async fn nodes_without_storage_devices_are_not_waited_for() {
    let chunk = canonical_chunk();
    let (store, _) = seeded_store(vec![]).await;
    register(
        &store,
        "ownerfs",
        "o1",
        std::time::Duration::from_secs(30),
        vec![],
    )
    .await;
    let service = gc_service(store.clone());
    let ticket = service
        .collect_garbage("n".into(), "s".into(), 0, vec![chunk])
        .await
        .unwrap();
    assert_eq!(ticket.waiting_nodes, vec!["n".to_owned()]);
}

/// R55：失联节点持有的引用按会话过期退休（任何存活节点的调用都能做），它保护的已 unlink
/// inode 在宽限后回收。修复前只有该节点自己的新会话能退休，永不回来就永不回收。
#[tokio::test]
async fn references_of_an_expired_session_retire_and_release_their_inode() {
    let chunk = canonical_chunk();
    let mut reference = pin("i");
    reference.node_id = "m".into();
    reference.session_id = "m1".into();
    let mut entities = vec![
        inode("i", 0, "v"),
        MetaEntity::DfsChunk(chunk.clone()),
        MetaEntity::DfsReference(reference),
    ];
    entities.extend(version("v", "i", &chunk.id.0));
    let (store, _) = seeded_store(entities).await;
    register(&store, "m", "m1", SHORT_LEASE, vec![storage_device("m")]).await;
    let service = gc_service(store.clone());
    let live = service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    assert!(
        live.chunks.is_empty(),
        "a live session's reference protects"
    );

    tokio::time::sleep(PAST_SHORT_LEASE).await;
    service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let reclaimed = service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    assert_eq!(reclaimed.chunks, vec![chunk.id]);
    assert_eq!(reclaimed.waiting_nodes, vec!["n".to_owned()]);
}

/// 节点缺席若干代后以新会话回来：它盘上的块 Meta 已不认识，经 inventory 重新登记并回收，
/// 它落后的 generation 不妨碍参与新一代。
#[tokio::test]
async fn a_node_absent_for_generations_catches_up_through_inventory() {
    let orphan = distinct_chunk(b"left on the absent node");
    let (store, _) = seeded_store(vec![]).await;
    register(&store, "m", "m1", SHORT_LEASE, vec![storage_device("m")]).await;
    let service = gc_service(store.clone());
    tokio::time::sleep(PAST_SHORT_LEASE).await;
    let mut completed = 0;
    for body in [b"one".as_slice(), b"two".as_slice()] {
        let ticket = service
            .collect_garbage(
                "n".into(),
                "s".into(),
                completed,
                vec![distinct_chunk(body)],
            )
            .await
            .unwrap();
        assert_eq!(ticket.waiting_nodes, vec!["n".to_owned()]);
        completed = ticket.generation;
        service
            .collect_garbage("n".into(), "s".into(), completed, vec![])
            .await
            .unwrap();
    }

    register(
        &store,
        "m",
        "m2",
        std::time::Duration::from_secs(30),
        vec![storage_device("m")],
    )
    .await;
    let ticket = service
        .collect_garbage("m".into(), "m2".into(), 0, vec![orphan.clone()])
        .await
        .unwrap();
    assert_eq!(ticket.generation, completed + 1);
    assert_eq!(ticket.chunks, vec![orphan.id]);
    assert_eq!(ticket.waiting_nodes, vec!["m".to_owned(), "n".to_owned()]);
    let acked = service
        .collect_garbage("m".into(), "m2".into(), ticket.generation, vec![])
        .await
        .unwrap();
    assert_eq!(acked.waiting_nodes, vec!["n".to_owned()]);
}

/// 指标能看见停滞：一代等一个活着但不确认的节点时，等待数非零、代龄增长；失联节点被剔除时
/// 计数增加。
#[tokio::test]
async fn gc_stall_metrics_track_the_waiting_generation() {
    let chunk = canonical_chunk();
    let (store, _) = seeded_store(vec![]).await;
    register(
        &store,
        "stuck",
        "st1",
        std::time::Duration::from_secs(30),
        vec![storage_device("stuck")],
    )
    .await;
    register(
        &store,
        "gone",
        "g1",
        SHORT_LEASE,
        vec![storage_device("gone")],
    )
    .await;
    let service = gc_service(store.clone());
    let ticket = service
        .collect_garbage("n".into(), "s".into(), 0, vec![chunk])
        .await
        .unwrap();
    assert_eq!(
        service.gc_metrics().generation.get(),
        ticket.generation as i64
    );
    assert_eq!(service.gc_metrics().waiting_nodes.get(), 3);
    assert_eq!(service.gc_metrics().pending_chunks.get(), 1);
    assert_eq!(service.gc_metrics().dropped_nodes.get(), 0);
    assert_eq!(service.gc_metrics().passes.get(), 1);
    assert_eq!(service.gc_metrics().attempts.get(), 1);
    assert_eq!(service.gc_metrics().conflicts.get(), 0);
    assert_eq!(service.gc_metrics().exhausted_passes.get(), 0);
    assert_eq!(service.gc_metrics().pass_duration.get_sample_count(), 1);

    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    service
        .collect_garbage("n".into(), "s".into(), ticket.generation, vec![])
        .await
        .unwrap();
    assert_eq!(
        service.gc_metrics().waiting_nodes.get(),
        1,
        "only the stuck node"
    );
    assert_eq!(
        service.gc_metrics().dropped_nodes.get(),
        1,
        "gone was dropped"
    );
    assert!(service.gc_metrics().generation_age_seconds.get() >= 1);

    // The generation ends: nothing stalls any more.
    service
        .collect_garbage("stuck".into(), "st1".into(), ticket.generation, vec![])
        .await
        .unwrap();
    assert_eq!(service.gc_metrics().waiting_nodes.get(), 0);
    assert_eq!(service.gc_metrics().generation_age_seconds.get(), 0);
    assert_eq!(
        service.gc_metrics().generation_duration.get_sample_count(),
        1,
        "one closed generation"
    );
}

fn external_gc_service(store: Arc<crate::meta::store::Store>) -> DfsService {
    DfsService::with_durability_policy(
        store,
        ReplicationConfig::local_single_copy(),
        crate::dfs::DfsDurabilityPolicy::ExternalBackend {
            store_id: "s".into(),
            generation: "g".into(),
        },
    )
    .with_gc_grace_ms(0)
}

fn generational_chunk(operation: &str, body: &[u8]) -> ChunkObject {
    crate::node::chunk::StagedChunk::new_generational(OperationId::new(operation), body.to_vec())
        .chunk
}

/// R05：S3 一份对象只删一次：一代只等一个执行者（存活存储节点中 node_id 最小的），它同时负责列举。
#[tokio::test]
async fn external_collection_waits_for_one_deleter_that_also_lists() {
    let orphan = generational_chunk("crashed", b"orphan");
    let (store, _) = seeded_store(vec![]).await;
    register(
        &store,
        "a",
        "a1",
        std::time::Duration::from_secs(30),
        vec![storage_device("a")],
    )
    .await;
    let service = external_gc_service(store.clone());
    assert!(service.lists_external_inventory("a").await.unwrap());
    assert!(!service.lists_external_inventory("n").await.unwrap());
    let ticket = service
        .collect_garbage("a".into(), "a1".into(), 0, vec![orphan.clone()])
        .await
        .unwrap();
    assert_eq!(ticket.chunks, vec![orphan.id]);
    assert_eq!(ticket.waiting_nodes, vec!["a".to_owned()]);
}

/// R05：等待期间上报的列举页也登记，下一代回收；不因为碰上一代未结束而丢页。
#[tokio::test]
async fn external_inventory_reported_while_waiting_is_collected_next() {
    let first = generational_chunk("crashed-1", b"one");
    let second = generational_chunk("crashed-2", b"two");
    let (store, _) = seeded_store(vec![]).await;
    let service = external_gc_service(store.clone());
    let ticket = service
        .collect_garbage("n".into(), "s".into(), 0, vec![first])
        .await
        .unwrap();
    service
        .collect_garbage("n".into(), "s".into(), 0, vec![second.clone()])
        .await
        .unwrap();
    service
        .collect_garbage("n".into(), "s".into(), ticket.generation, vec![])
        .await
        .unwrap();
    let next = service
        .collect_garbage("n".into(), "s".into(), ticket.generation, vec![])
        .await
        .unwrap();
    assert_eq!(next.chunks, vec![second.id]);
}

/// R05：活着的写代引用保护它上传而未提交的对象，即使 Meta 从未登记过它（列举得来）。
#[tokio::test]
async fn a_live_write_generation_protects_its_listed_objects() {
    let operation = "upload-in-flight";
    let uploaded = generational_chunk(operation, b"pending");
    let (store, _) = seeded_store(vec![MetaEntity::DfsReference(GcReference {
        open_read: None,
        node_id: "n".into(),
        session_id: "s".into(),
        reference_id: format!(
            "write:{}",
            ChunkObject::generation_id(&OperationId::new(operation))
        ),
        inode_id: Some(InodeId::new("i")),
        publication: true,
        chunks: vec![],
        retired_at_unix_ms: None,
    })])
    .await;
    let service = external_gc_service(store.clone());
    let ticket = service
        .collect_garbage("n".into(), "s".into(), 0, vec![uploaded])
        .await
        .unwrap();
    assert!(ticket.chunks.is_empty(), "{ticket:?}");
}
