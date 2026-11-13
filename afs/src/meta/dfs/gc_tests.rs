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
        MetaEntity::DfsLayoutRoot(
            LayoutRoot {
                id: LayoutRootId::new(id),
                file_length: 4,
                inline_extents: vec![Extent {
                    file_offset: 0,
                    length: 4,
                    chunk_id: ChunkId::new(chunk),
                    chunk_offset: 0,
                }],
            }
            .into(),
        ),
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
        pinned_version: None,
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
/// A reader that opened v1 keeps v1 (and the head) until it closes and the
/// grace passes. Before R81 F2 any reference kept every version; now the
/// reference names the version it keeps.
#[test]
fn open_inode_retains_old_versions_until_last_reference_and_grace_are_gone() {
    let mut entities = vec![inode("mapped", 0, "v2"), chunk("old"), chunk("new")];
    entities.extend(version("v1", "mapped", "old"));
    entities.extend(version("v2", "mapped", "new"));
    let mut reference = pin("mapped");
    reference.pinned_version = Some(FileVersionId::new("v1"));
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

/// R81 F2: an open file no longer keeps every version it ever had. Versions
/// nobody pinned are collected while the file stays open; the head, the
/// versions references name and the chunks they share stay.
#[test]
fn an_open_file_keeps_its_head_and_pinned_versions_only() {
    let mut entities = vec![inode("busy", 1, "v5"), chunk("shared")];
    for (id, chunk_id) in [("v1", "c1"), ("v2", "c2"), ("v3", "shared"), ("v4", "c4")] {
        entities.extend(version(id, "busy", chunk_id));
        entities.push(chunk(chunk_id));
    }
    entities.extend(version("v5", "busy", "shared"));
    entities.push(MetaEntity::DfsWriteLease(WriteLease {
        inode_id: InodeId::new("busy"),
        owner_node_id: "writer".into(),
        owner_session_id: "session".into(),
        lease_epoch: 1,
        expires_at_unix_ms: u64::MAX,
    }));
    let created = pin("busy");
    let mut reader = pin("busy");
    reader.reference_id = "open:reader".into();
    reader.pinned_version = Some(FileVersionId::new("v2"));
    let plan = plan_collection(&entities, &[created, reader], 100, 10);
    let dead = |id: &str| {
        plan.keys
            .contains(&MetaKey::DfsFileVersion(FileVersionId::new(id)))
    };
    assert!(dead("v1") && dead("v3") && dead("v4"), "{:?}", plan.keys);
    assert!(!dead("v2") && !dead("v5"));
    let mut chunks = plan.chunks.clone();
    chunks.sort();
    assert_eq!(chunks, vec![ChunkId::new("c1"), ChunkId::new("c4")]);
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
    reference.pinned_version = Some(FileVersionId::new("v1"));
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
                nfs_addr: String::new(),
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
async fn a_plan_survives_unrelated_commits_and_fails_on_each_relevant_change() {
    use crate::dfs::{FileVersionId, GcReference, ReplicationTask, ReplicationTaskId};
    use crate::meta::store::{GcPlanGuard, entity_key};
    let dead_chunk =
        ChunkId::new("crc32c-v2-00000000000000000000000000000001-00000000000000000000000000000002");
    let plan = |since| {
        Arc::new(GcPlanGuard {
            since,
            written_keys: [
                MetaKey::DfsInode(InodeId::new("dead")),
                MetaKey::DfsChunk(dead_chunk.clone()),
            ]
            .into(),
            protected_by_reference: [InodeId::new("dead"), InodeId::new("owner")].into(),
            dead_inodes: [InodeId::new("dead")].into(),
            dead_layouts: [LayoutRootId::new("dead-layout")].into(),
            chunks: [dead_chunk.clone()].into(),
            generations: ["00000000000000000000000000000001".to_owned()].into(),
            live_sessions: [("n".to_owned(), "s".to_owned())].into(),
        })
    };
    let reference = |id: &str, inode: &str, publication: bool, retired: Option<u64>| {
        MetaEntity::DfsReference(GcReference {
            open_read: None,
            node_id: "n".into(),
            session_id: "s".into(),
            reference_id: id.into(),
            inode_id: Some(InodeId::new(inode)),
            publication,
            chunks: vec![],
            retired_at_unix_ms: retired,
            pinned_version: None,
        })
    };
    let version_on = |inode: &str, layout: &str| {
        MetaEntity::DfsFileVersion(FileVersion {
            id: FileVersionId::new("new-version"),
            inode_id: InodeId::new(inode),
            parent_version: None,
            length: 4,
            layout_root: LayoutRootId::new(layout),
            created_at_unix_ms: 0,
        })
    };
    let mut layout_using_chunk = version("new-layout", "live", dead_chunk.0.as_str());
    let layout_using_chunk = layout_using_chunk.remove(1);
    let lease_on = |inode: &str| {
        MetaEntity::DfsWriteLease(WriteLease {
            inode_id: InodeId::new(inode),
            owner_node_id: "n".into(),
            owner_session_id: "s".into(),
            lease_epoch: 1,
            expires_at_unix_ms: u64::MAX,
        })
    };
    let task = MetaEntity::DfsReplicationTask(ReplicationTask {
        id: ReplicationTaskId::new("task"),
        chunk_id: dead_chunk.clone(),
        placement_epoch: 1,
        desired_copies: 1,
        existing_copies: vec![],
        state: ReplicationTaskState::Pending,
        attempt: 0,
        next_retry_unix_ms: 0,
        last_error: None,
        claim: None,
    });
    let relevant = vec![
        (
            "new reference on the dead inode",
            reference("open:a", "dead", false, None),
        ),
        (
            "new reference on a version owner",
            reference("open:b", "owner", false, None),
        ),
        (
            "new write generation of a removed chunk",
            reference(
                "write:00000000000000000000000000000001",
                "other",
                true,
                None,
            ),
        ),
        ("lease on the dead inode", lease_on("dead")),
        ("lease on a version owner", lease_on("owner")),
        ("version on the dead inode", version_on("dead", "fresh")),
        (
            "version on a removed layout",
            version_on("live", "dead-layout"),
        ),
        ("layout using a removed chunk", layout_using_chunk),
        ("task of a removed chunk", task),
        ("key the plan writes", inode("dead", 1, "v")),
    ];
    let unrelated = vec![
        (
            "reference on another inode",
            reference("open:c", "live", false, None),
        ),
        (
            "retiring a reference on the dead inode",
            reference("open:d", "dead", false, Some(1)),
        ),
        (
            "write generation of another chunk",
            reference("write:ff", "live", true, None),
        ),
        ("lease on another inode", lease_on("live")),
        ("version on another inode", version_on("live", "fresh")),
        (
            "new head of the version owner",
            inode("owner", 1, "new-head"),
        ),
    ];
    for (expected, cases) in [(false, relevant), (true, unrelated)] {
        for (name, entity) in cases {
            let (store, _) =
                seeded_store(vec![inode("dead", 0, "v"), inode("owner", 1, "h")]).await;
            let service = DfsService::new(store.clone());
            let since = store.read_view().await.unwrap().snapshot().unwrap().0;
            let condition = TxnCondition::DfsGcPlanStillValid(plan(since));
            assert!(
                service
                    .gc_commit_checked(
                        vec![],
                        StoreOperation::DfsUpdateReference,
                        vec![TxnMutation::Put(entity)]
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(
                service
                    .gc_commit_checked(
                        vec![condition],
                        StoreOperation::DfsCollectGarbage,
                        vec![TxnMutation::Delete(entity_key(&inode("dead", 0, "v")))]
                    )
                    .await
                    .unwrap(),
                expected,
                "{name}"
            );
        }
    }
    // A change in the live sessions invalidates; a lease renewal does not.
    let (store, _) = seeded_store(vec![inode("dead", 0, "v")]).await;
    let service = DfsService::new(store.clone());
    let since = store.read_view().await.unwrap().snapshot().unwrap().0;
    let condition = TxnCondition::DfsGcPlanStillValid(plan(since));
    register(
        &store,
        "other",
        "o1",
        SHORT_LEASE,
        vec![storage_device("other")],
    )
    .await;
    assert!(
        !service
            .gc_commit_checked(
                vec![condition],
                StoreOperation::DfsCollectGarbage,
                vec![TxnMutation::Delete(entity_key(&inode("dead", 0, "v")))]
            )
            .await
            .unwrap(),
        "a new live session changes the waiting set"
    );
}

/// Whatever a commit keeps valid must still be dead when re-planned at commit
/// time: the check may refuse too much, never too little.
#[tokio::test]
async fn a_valid_plan_is_still_correct_when_replanned_at_commit_time() {
    // Seeded xorshift: a failure reproduces.
    let mut seed = 0x0005_DEEC_E66D_u64;
    let mut random = move |limit: usize| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        (seed % limit as u64) as usize
    };
    let (mut kept, mut refused) = (0, 0);
    for _round in 0..300 {
        let now = 1_000_000_u64;
        let grace = 10;
        let mut entities = Vec::new();
        for index in 0..6 {
            let id = format!("i{index}");
            let nlink = u32::from(random(2) == 0);
            entities.push(inode(&id, nlink, &format!("v{index}-{}", random(2))));
            for generation in 0..2 {
                let version_id = format!("v{index}-{generation}");
                let chunk_id = format!("c{index}-{generation}");
                entities.extend(version(&version_id, &id, &chunk_id));
                entities.push(chunk(&chunk_id));
            }
        }
        let mut references = Vec::new();
        for index in 0..random(4) {
            let mut value = pin(&format!("open:r{index}"));
            value.inode_id = Some(InodeId::new(format!("i{}", random(6))));
            references.push(value.clone());
            entities.push(MetaEntity::DfsReference(value));
        }
        let (store, _) = seeded_store(entities.clone()).await;
        let service = DfsService::new(store.clone());
        let (since, snapshot) = store.read_view().await.unwrap().snapshot().unwrap();
        let plan = plan_collection(&snapshot, &references, now, grace);
        let mutations = plan
            .keys
            .iter()
            .cloned()
            .map(TxnMutation::Delete)
            .collect::<Vec<_>>();
        let guard = plan_guard(since, &snapshot, now, &plan, &plan.keys, &mutations);
        // Foreground commits racing the plan: any of the changes a real writer
        // makes, aimed at random inodes, versions and chunks.
        for change in 0..random(4) {
            let target = random(6);
            let mutation = match random(5) {
                0 => {
                    let mut value = pin(&format!("open:late{change}"));
                    value.inode_id = Some(InodeId::new(format!("i{target}")));
                    MetaEntity::DfsReference(value)
                }
                1 => MetaEntity::DfsWriteLease(WriteLease {
                    inode_id: InodeId::new(format!("i{target}")),
                    owner_node_id: "n".into(),
                    owner_session_id: "s".into(),
                    lease_epoch: 1,
                    expires_at_unix_ms: u64::MAX,
                }),
                2 => {
                    // A commit: a fresh head version with a fresh layout.
                    let fresh = format!("fresh{change}");
                    for entity in version(&fresh, &format!("i{target}"), &format!("c{fresh}")) {
                        service
                            .gc_commit_checked(
                                vec![],
                                StoreOperation::DfsUpdateReference,
                                vec![TxnMutation::Put(entity)],
                            )
                            .await
                            .unwrap();
                    }
                    inode(&format!("i{target}"), 1, &fresh)
                }
                3 => {
                    // COW: a layout that references an existing chunk.
                    version(
                        &format!("clone{change}"),
                        &format!("i{target}"),
                        &format!("c{target}-{}", random(2)),
                    )
                    .remove(1)
                }
                _ => chunk(&format!("c{target}-{}", random(2))),
            };
            service
                .gc_commit_checked(
                    vec![],
                    StoreOperation::DfsUpdateReference,
                    vec![TxnMutation::Put(mutation)],
                )
                .await
                .unwrap();
        }
        let (_, at_commit) = store.read_view().await.unwrap().snapshot().unwrap();
        let references_now = at_commit
            .iter()
            .filter_map(|entity| match entity {
                MetaEntity::DfsReference(value) => Some(value.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let replanned = plan_collection(&at_commit, &references_now, now, grace);
        let still_dead = plan.keys.iter().all(|key| replanned.keys.contains(key))
            && plan
                .chunks
                .iter()
                .all(|chunk| replanned.chunks.contains(chunk));
        let valid = service
            .gc_commit_checked(
                vec![TxnCondition::DfsGcPlanStillValid(Arc::new(guard))],
                StoreOperation::DfsCollectGarbage,
                mutations,
            )
            .await
            .unwrap();
        if valid {
            kept += 1;
            assert!(
                still_dead,
                "a kept plan deletes something a fresh plan would keep: {:?} vs {:?}",
                plan.keys, replanned.keys
            );
        } else {
            refused += 1;
        }
    }
    assert!(
        kept > 30 && refused > 30,
        "the check must be exercised both ways: kept {kept}, refused {refused}"
    );
}

/// Everything reachable from a live root, at every step of a batched collection.
fn assert_live_files_intact(entities: &[MetaEntity], expected_live_files: usize) {
    let mut live = 0;
    for entity in entities {
        let MetaEntity::DfsInode(inode) = entity else {
            continue;
        };
        if inode.attributes.nlink == 0 {
            continue;
        }
        live += 1;
        let head = inode.head_version.as_ref().unwrap();
        let version = entities
            .iter()
            .find_map(|value| match value {
                MetaEntity::DfsFileVersion(version) if &version.id == head => Some(version),
                _ => None,
            })
            .unwrap_or_else(|| panic!("live inode lost its head version {head:?}"));
        let layout = entities
            .iter()
            .find_map(|value| match value {
                MetaEntity::DfsLayoutRoot(layout) if layout.id == version.layout_root => {
                    Some(layout)
                }
                _ => None,
            })
            .expect("a live version keeps its layout");
        for extent in &layout.inline_extents {
            assert!(
                entities.iter().any(|value| matches!(value,
                    MetaEntity::DfsChunk(chunk) if chunk.id == extent.chunk_id)),
                "a live layout keeps its chunks"
            );
        }
    }
    assert_eq!(live, expected_live_files);
}

fn bulk_delete_fixture(dead_files: usize, live_files: usize) -> Vec<MetaEntity> {
    let mut entities = Vec::new();
    for index in 0..dead_files + live_files {
        let nlink = u32::from(index >= dead_files);
        let (id, head, chunk_id) = (
            format!("i{index:06}"),
            format!("v{index:06}"),
            format!("c{index:06}"),
        );
        entities.push(inode(&id, nlink, &head));
        entities.extend(version(&head, &id, &chunk_id));
        entities.push(chunk(&chunk_id));
        entities.push(MetaEntity::DfsPlacement(crate::dfs::PlacementRecord {
            chunk_id: ChunkId::new(&chunk_id),
            replica_group_id: crate::dfs::ReplicaGroupId::new("g"),
            placement_epoch: 1,
            desired_copies: 1,
            copies: Vec::new(),
            health: PlacementHealth::Satisfied,
        }));
    }
    entities
}

/// Drives passes as a Node would until nothing is dead, reopening the store from
/// its log after each one when `restart` is set: a crash between any two batches.
async fn collect_until_clean(
    entities: Vec<MetaEntity>,
    live_files: usize,
    restart: bool,
) -> (usize, Arc<crate::meta::store::Store>, DfsService) {
    use crate::meta::store::Store;
    let (mut store, backend) = seeded_store(entities).await;
    let mut service = DfsService::new(store.clone()).with_gc_grace_ms(0);
    let mut passes = 0;
    let mut completed = 0;
    loop {
        passes += 1;
        assert!(passes < 1000, "collection must converge");
        let gc = service
            .collect_garbage("n".into(), "s".into(), completed, vec![])
            .await
            .unwrap();
        // The Node retires the chunks and acknowledges.
        if gc.waiting_nodes.iter().any(|node| node == "n") {
            service
                .collect_garbage("n".into(), "s".into(), gc.generation, vec![])
                .await
                .unwrap();
        }
        completed = gc.generation;
        let (_, now) = store.read_view().await.unwrap().snapshot().unwrap();
        assert_live_files_intact(&now, live_files);
        let references = Vec::new();
        let plan = plan_collection(&now, &references, now_unix_ms(), 0);
        if plan.keys.is_empty() {
            return (passes, store, service);
        }
        if restart {
            drop(service);
            drop(store);
            store = Arc::new(Store::open_log(backend.clone()).await.unwrap());
            service = DfsService::new(store.clone()).with_gc_grace_ms(0);
            // The session lease survives the reopen; the fixture is unchanged.
        }
    }
}

#[tokio::test]
async fn a_bulk_delete_converges_in_batches_and_leaves_live_files_intact() {
    let (passes, store, _) = collect_until_clean(bulk_delete_fixture(600, 25), 25, false).await;
    assert!(
        passes > 1,
        "chunk batches split a bulk delete across passes"
    );
    let (_, entities) = store.read_view().await.unwrap().snapshot().unwrap();
    assert_eq!(
        entities
            .iter()
            .filter(|entity| matches!(entity, MetaEntity::DfsInode(_)))
            .count(),
        25,
        "only live inodes remain"
    );
    assert_eq!(
        entities
            .iter()
            .filter(|entity| matches!(entity, MetaEntity::DfsChunk(_)))
            .count(),
        25,
        "only live chunks remain"
    );
    assert!(
        !entities.iter().any(|entity| matches!(entity,
            MetaEntity::DfsPlacement(placement)
                if !entities.iter().any(|chunk| matches!(chunk,
                    MetaEntity::DfsChunk(value) if value.id == placement.chunk_id)))),
        "a chunk's records leave together"
    );
}

#[tokio::test]
async fn a_restart_between_batches_resumes_and_finishes_the_same_collection() {
    let (_, store, _) = collect_until_clean(bulk_delete_fixture(300, 10), 10, true).await;
    let (_, entities) = store.read_view().await.unwrap().snapshot().unwrap();
    assert_eq!(
        entities
            .iter()
            .filter(|entity| matches!(entity, MetaEntity::DfsInode(_)))
            .count(),
        10
    );
}

#[test]
fn any_subset_of_a_plan_leaves_the_rest_dead_and_chunk_records_stay_together() {
    let entities = bulk_delete_fixture(20, 5);
    let plan = plan_bounded(&entities, &[], now_unix_ms(), 0, 8);
    assert_eq!(plan.chunks.len(), 8);
    // First the chunk records: 8 chunks and their placements.
    assert_eq!(plan.chunk_keys, 16);
    assert!(
        plan.keys[..plan.chunk_keys]
            .iter()
            .all(|key| matches!(key, MetaKey::DfsChunk(_) | MetaKey::DfsPlacement(_)))
    );
    let rest = plan.keys[plan.chunk_keys..].to_vec();
    // Delete a shrinking subset each time: what stays planned does not change.
    for keep in [rest.len() / 2, rest.len() / 3, 1] {
        let deleted = rest[keep..]
            .iter()
            .chain(plan.keys[..plan.chunk_keys].iter())
            .collect::<BTreeSet<_>>();
        let remaining = entities
            .iter()
            .filter(|entity| !deleted.contains(&crate::meta::store::entity_key(entity)))
            .cloned()
            .collect::<Vec<_>>();
        assert_live_files_intact(&remaining, 5);
        let again = plan_bounded(&remaining, &[], now_unix_ms(), 0, 1024);
        for key in &rest[..keep] {
            assert!(again.keys.contains(key), "{key:?} became reachable");
        }
    }
}

/// A plan larger than one batch: the reader's old version `zz-old` waits in a
/// later batch while its only chunk leaves with the first one, and the first
/// batch names no version of the reader's inode.
async fn plan_whose_first_batch_deletes_a_later_batch_versions_chunk() -> (
    Arc<crate::meta::store::Store>,
    crate::meta::store::StoreRevision,
    Vec<MetaEntity>,
    u64,
    GcPlan,
) {
    let mut seeded = vec![inode("owner", 1, "zz-head"), chunk("live-chunk")];
    seeded.extend(version("zz-head", "owner", "live-chunk"));
    // Dead versions of another inode, sorted before "zz-old", share the live
    // chunk: they add only version and layout keys.
    for index in 0..MAX_BATCH_KEYS {
        seeded.extend(version(
            &format!("aa-dead-{index:05}"),
            "filler",
            "live-chunk",
        ));
    }
    seeded.extend(version("zz-old", "owner", "old-chunk"));
    seeded.push(chunk("old-chunk"));
    let (store, _) = seeded_store(seeded).await;
    let (since, entities) = store.read_view().await.unwrap().snapshot().unwrap();
    let now = now_unix_ms();
    let plan = plan_bounded(&entities, &[], now, 0, MAX_CHUNKS.min(MAX_BATCH_KEYS / 8));
    assert!(plan.chunks.contains(&ChunkId::new("old-chunk")));
    (store, since, entities, now, plan)
}

/// A reader admitted after planning pins `zz-old` (it still exists, so
/// admission succeeds; Meta does not require it to be the head).
async fn pin_old_version_after_planning(service: &DfsService) {
    service
        .update_gc_reference(UpdateGcReference {
            node_id: "n".into(),
            session_id: "s".into(),
            reference_id: "open:late-reader".into(),
            inode_id: Some(InodeId::new("owner")),
            version_id: Some(FileVersionId::new("zz-old")),
            publication: false,
            admission_deadline_unix_ms: now_unix_ms().saturating_add(30_000),
        })
        .await
        .unwrap();
}

/// The chunk records leave with the first batch of a plan. A reader admitted
/// after planning that pins an old version whose own key waits in a later
/// batch must still invalidate the first batch: it deletes that version's
/// only chunk, and the reader would read EIO.
#[tokio::test]
async fn a_reader_admitted_after_planning_protects_a_chunk_of_a_later_batch_version() {
    let (store, since, entities, now, plan) =
        plan_whose_first_batch_deletes_a_later_batch_versions_chunk().await;
    let service = DfsService::new(store.clone());
    let first = plan.keys.len().min(plan.chunk_keys.max(MAX_BATCH_KEYS));
    let (batch, rest) = plan.keys.split_at(first);
    assert!(
        batch.contains(&MetaKey::DfsChunk(ChunkId::new("old-chunk"))),
        "precondition: the first batch deletes the old version's chunk"
    );
    assert!(
        rest.contains(&MetaKey::DfsFileVersion(FileVersionId::new("zz-old"))),
        "precondition: the old version itself waits for a later batch"
    );
    assert!(
        !batch
            .iter()
            .any(|key| matches!(key, MetaKey::DfsFileVersion(id)
            if plan.version_owners.get(id) == Some(&InodeId::new("owner")))),
        "precondition: no version of the reader's inode is in the first batch"
    );
    let mutations = batch
        .iter()
        .cloned()
        .map(TxnMutation::Delete)
        .collect::<Vec<_>>();
    let guard = plan_guard(since, &entities, now, &plan, batch, &mutations);
    pin_old_version_after_planning(&service).await;
    let committed = service
        .gc_commit_checked(
            vec![TxnCondition::DfsGcPlanStillValid(Arc::new(guard))],
            StoreOperation::DfsCollectGarbage,
            mutations,
        )
        .await
        .unwrap();
    assert!(
        !committed,
        "the first batch deleted the only chunk of a version a reader pinned after planning"
    );
    let (_, after) = store.read_view().await.unwrap().snapshot().unwrap();
    assert!(
        after.iter().any(
            |entity| matches!(entity, MetaEntity::DfsChunk(chunk) if chunk.id.0 == "old-chunk")
        ),
        "the pinned version's chunk must survive the refused batch"
    );
}

/// Every batch of a plan is rechecked against the whole plan, not only its own
/// keys: a later batch that names no version of the reader's inode is refused
/// too once a reader pins a version the plan still deletes. What is left is
/// planned again with the reader among its inputs.
#[tokio::test]
async fn a_later_batch_is_rechecked_against_the_whole_plan() {
    let (store, since, entities, now, plan) =
        plan_whose_first_batch_deletes_a_later_batch_versions_chunk().await;
    let service = DfsService::new(store.clone());
    let first = plan.keys.len().min(plan.chunk_keys.max(MAX_BATCH_KEYS));
    let rest = &plan.keys[first..];
    // A later batch, built as `commit_remaining_keys` builds it, that deletes
    // nothing of the reader's inode.
    let batch = rest
        .iter()
        .filter(|key| match key {
            MetaKey::DfsFileVersion(id) => {
                plan.version_owners.get(id) != Some(&InodeId::new("owner"))
            }
            MetaKey::DfsLayoutRoot(id) => id.0 != "zz-old",
            _ => false,
        })
        .take(8)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(batch.len(), 8, "precondition: the plan has a later batch");
    let mutations = batch
        .iter()
        .cloned()
        .map(TxnMutation::Delete)
        .collect::<Vec<_>>();
    let chunkless = GcPlan {
        version_owners: plan.version_owners.clone(),
        ..GcPlan::default()
    };
    let guard = plan_guard(since, &entities, now, &chunkless, &batch, &mutations);
    assert!(
        guard
            .protected_by_reference
            .contains(&InodeId::new("owner")),
        "a later batch must be rechecked for readers of every version the plan deletes"
    );
    pin_old_version_after_planning(&service).await;
    let committed = service
        .gc_commit_checked(
            vec![TxnCondition::DfsGcPlanStillValid(Arc::new(guard))],
            StoreOperation::DfsCollectGarbage,
            mutations,
        )
        .await
        .unwrap();
    assert!(
        !committed,
        "a batch of a plan a late reader invalidated must not commit"
    );
}

#[tokio::test]
async fn a_commit_refused_for_size_halves_the_batch_and_is_counted() {
    use crate::meta::store::{Store, journal::MemoryLog};
    // History at its limit in single-event revisions: a commit can only free the
    // oldest `cleanup_items` of them, so only a small batch can fit.
    let backend = Arc::new(MemoryLog::new(Default::default()).unwrap());
    let history = crate::meta::store::HistoryLimits {
        event_count: 1000,
        cleanup_items: 128,
        ..Default::default()
    };
    let store = Arc::new(
        Store::open_log_with_history(backend, history)
            .await
            .unwrap(),
    );
    store
        .register_node_session(
            RequestKey::new("n", "session"),
            crate::meta::store::NodeSessionLease {
                node_id: "n".into(),
                session_id: "s".into(),
                grpc_addr: "http://n:1".into(),
                data_addr: "http://n:2".into(),
                rest_addr: "http://n:3".into(),
                nfs_addr: String::new(),
                ownerfs_enabled: false,
                storage_devices: vec![storage_device("n")],
                lease_ttl: std::time::Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
    for (index, entity) in bulk_delete_fixture(200, 0).into_iter().enumerate() {
        let key = RequestKey::new("seed", format!("entity-{index}"));
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCollectGarbage);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        txn.mutations.push(TxnMutation::Put(entity));
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: StoreOperation::DfsCollectGarbage,
                result: OperationResult::Empty,
            }));
        store.compare_and_commit(txn).await.unwrap();
    }
    let service = DfsService::new(store.clone()).with_gc_grace_ms(0);
    let gc = service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    assert!(gc.generation > 0, "a smaller batch got through");
    assert!(service.gc_metrics().rejected_batches.get() > 0);
}

#[tokio::test]
async fn stalled_collection_is_visible_while_dead_keys_wait() {
    let (store, _) = seeded_store(bulk_delete_fixture(3, 0)).await;
    let service = DfsService::new(store.clone()).with_gc_grace_ms(0);
    service.gc_metrics().observe_stall();
    service.gc_metrics().dead_keys.set(10);
    *service.gc_metrics().last_collected.lock().unwrap() =
        Some(std::time::Instant::now() - std::time::Duration::from_secs(90));
    service.gc_metrics().observe_stall();
    assert!(service.gc_metrics().stalled_seconds.get() >= 90);
    // A pass that collects clears it.
    service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    assert_eq!(service.gc_metrics().stalled_seconds.get(), 0);
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
        pinned_version: None,
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
/// The commit condition of a plan that deletes inode `i`, made at `since`.
fn plan_of_dead_inode(since: crate::meta::store::StoreRevision) -> TxnCondition {
    TxnCondition::DfsGcPlanStillValid(Arc::new(crate::meta::store::GcPlanGuard {
        since,
        written_keys: [MetaKey::DfsInode(InodeId::new("i"))].into(),
        protected_by_reference: [InodeId::new("i")].into(),
        dead_inodes: [InodeId::new("i")].into(),
        live_sessions: [("n".to_owned(), "s".to_owned())].into(),
        ..Default::default()
    }))
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
            .gc_commit_checked(
                vec![plan_of_dead_inode(collection_revision)],
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
                            nfs_addr: String::new(),
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
                nfs_addr: String::new(),
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
        nfs_addr: String::new(),
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
                nfs_addr: String::new(),
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

/// S4-9: an external deleter that stays registered but never finishes (its
/// object store hangs) would hold every later generation. Past the handoff
/// deadline another live participant takes the deletion over; before it, the
/// deleter keeps it, and its late acknowledgement changes nothing.
#[tokio::test]
async fn an_overdue_external_deleter_hands_its_generation_to_another_live_node() {
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
    let service = external_gc_service(store.clone())
        .with_gc_deleter_handoff(std::time::Duration::from_millis(200));
    let ticket = service
        .collect_garbage("a".into(), "a1".into(), 0, vec![orphan.clone()])
        .await
        .unwrap();
    assert_eq!(ticket.waiting_nodes, vec!["a".to_owned()]);
    let early = service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    assert_eq!(early.waiting_nodes, vec!["a".to_owned()], "not overdue yet");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let late = service
        .collect_garbage("n".into(), "s".into(), 0, vec![])
        .await
        .unwrap();
    assert_eq!(late.generation, ticket.generation);
    assert_eq!(late.waiting_nodes, vec!["n".to_owned()]);
    assert_eq!(late.chunks, vec![orphan.id.clone()]);
    let done = service
        .collect_garbage("n".into(), "s".into(), late.generation, vec![])
        .await
        .unwrap();
    assert!(done.waiting_nodes.is_empty(), "{done:?}");
    // While a is overdue, the next generation and the listing pass it over.
    assert!(service.lists_external_inventory("n").await.unwrap());
    let second = generational_chunk("crashed-again", b"second");
    let next = service
        .collect_garbage("n".into(), "s".into(), late.generation, vec![second])
        .await
        .unwrap();
    assert_eq!(next.waiting_nodes, vec!["n".to_owned()]);
    // a reports the generation it lost: it is deleting again.
    service
        .collect_garbage("a".into(), "a1".into(), late.generation, vec![])
        .await
        .unwrap();
    assert!(service.lists_external_inventory("a").await.unwrap());
}

/// Native copies live on each Node's own disk: nobody can delete another
/// Node's copy, so a native generation never changes hands.
#[tokio::test]
async fn a_native_generation_never_hands_its_deletion_to_another_node() {
    let (store, _) = seeded_store(vec![]).await;
    register(
        &store,
        "a",
        "a1",
        std::time::Duration::from_secs(30),
        vec![storage_device("a")],
    )
    .await;
    let orphan = generational_chunk("native-orphan", b"orphan");
    let service = DfsService::new(store.clone())
        .with_gc_grace_ms(0)
        .with_gc_deleter_handoff(std::time::Duration::ZERO);
    let ticket = service
        .collect_garbage("a".into(), "a1".into(), 0, vec![orphan])
        .await
        .unwrap();
    assert_eq!(ticket.waiting_nodes, vec!["a".to_owned(), "n".to_owned()]);
    let n_done = service
        .collect_garbage("n".into(), "s".into(), ticket.generation, vec![])
        .await
        .unwrap();
    assert_eq!(n_done.waiting_nodes, vec!["a".to_owned()]);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let later = service
        .collect_garbage("n".into(), "s".into(), ticket.generation, vec![])
        .await
        .unwrap();
    assert_eq!(later.waiting_nodes, vec!["a".to_owned()]);
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
        pinned_version: None,
    })])
    .await;
    let service = external_gc_service(store.clone());
    let ticket = service
        .collect_garbage("n".into(), "s".into(), 0, vec![uploaded])
        .await
        .unwrap();
    assert!(ticket.chunks.is_empty(), "{ticket:?}");
}

/// R81 F2 guard: what a reader pinned at open survives every later commit and
/// collection until it closes. Under the old rule any reference protected
/// every version of its inode; under the version rule the reference names the
/// opened version. Both keep it.
#[tokio::test]
async fn a_readers_pinned_version_survives_later_commits_and_collection() {
    let mut entities = vec![inode("f", 1, "v4")];
    for (version_id, chunk_id) in [("v1", "c1"), ("v2", "c2"), ("v3", "c3"), ("v4", "c4")] {
        entities.extend(version(version_id, "f", chunk_id));
        entities.push(chunk(chunk_id));
    }
    let (store, _) = seeded_store(entities).await;
    let service = DfsService::new(store.clone()).with_gc_grace_ms(0);
    let mut reader = update("open:reader-of-v1");
    reader.inode_id = Some(InodeId::new("f"));
    reader.version_id = Some(FileVersionId::new("v1"));
    reader.publication = false;
    service.update_gc_reference(reader).await.unwrap();
    collect_with_acks(&service).await;
    let view = store.read_view().await.unwrap();
    for key in [
        MetaKey::DfsFileVersion(FileVersionId::new("v1")),
        MetaKey::DfsLayoutRoot(LayoutRootId::new("v1")),
        MetaKey::DfsChunk(ChunkId::new("c1")),
        MetaKey::DfsFileVersion(FileVersionId::new("v4")),
        MetaKey::DfsChunk(ChunkId::new("c4")),
    ] {
        assert!(
            view.lookup(&key).unwrap().entity.is_some(),
            "{key:?} was collected"
        );
    }
}

/// R81 F2 guard: an in-flight write generation keeps the version it started
/// from, even once a later commit moved the head past it.
#[tokio::test]
async fn an_inflight_writes_base_version_survives_collection() {
    let mut entities = vec![inode("f", 1, "v2")];
    for (version_id, chunk_id) in [("v1", "c1"), ("v2", "c2")] {
        entities.extend(version(version_id, "f", chunk_id));
        entities.push(chunk(chunk_id));
    }
    let (store, _) = seeded_store(entities).await;
    let service = DfsService::new(store.clone()).with_gc_grace_ms(0);
    let operation = crate::dfs::OperationId::new(crate::clock::timed_request_id("base-writer"));
    let mut writer = update(&format!("write:{}", operation.0));
    writer.inode_id = Some(InodeId::new("f"));
    writer.version_id = Some(FileVersionId::new("v1"));
    service.update_gc_reference(writer).await.unwrap();
    collect_with_acks(&service).await;
    let view = store.read_view().await.unwrap();
    for key in [
        MetaKey::DfsFileVersion(FileVersionId::new("v1")),
        MetaKey::DfsLayoutRoot(LayoutRootId::new("v1")),
        MetaKey::DfsChunk(ChunkId::new("c1")),
    ] {
        assert!(
            view.lookup(&key).unwrap().entity.is_some(),
            "{key:?} was collected"
        );
    }
}

/// Runs collection as the one storage Node `n` until nothing more is planned.
async fn collect_with_acks(service: &DfsService) {
    let mut completed = 0;
    for _ in 0..50 {
        let gc = service
            .collect_garbage("n".into(), "s".into(), completed, vec![])
            .await
            .unwrap();
        if gc.waiting_nodes.iter().any(|node| node == "n") {
            service
                .collect_garbage("n".into(), "s".into(), gc.generation, vec![])
                .await
                .unwrap();
        }
        if gc.generation == completed {
            return;
        }
        completed = gc.generation;
    }
}
