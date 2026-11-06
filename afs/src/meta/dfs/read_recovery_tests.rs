use super::*;
use crate::dfs::{
    DfsReadValidation, Extent, LayoutRootId, StorageDeviceDescriptor, ValidateDfsReadGrants,
};
use crate::meta::store::{NodeSessionLease, Store, journal::MemoryLog};
use crate::node::chunk::{ChunkSink, LocalChunkStore, StagedChunk};

struct Fixture {
    service: DfsService,
    store: Arc<Store>,
    temp: tempfile::TempDir,
    local: LocalChunkStore,
    request: DfsChunkSourcesRequest,
    copy: CopyRecord,
}

impl Fixture {
    async fn sources(&self) -> Vec<SourceCandidate> {
        self.service
            .chunk_sources(self.request.clone())
            .await
            .unwrap()
            .chunks
            .remove(0)
            .sources
    }

    async fn validate(&self, source: &SourceCandidate, epoch: u64) -> Result<()> {
        self.service
            .validate_read_grants(ValidateDfsReadGrants {
                receiver_node_id: "receiver".into(),
                receiver_node_epoch: epoch,
                peer_node_id: "reader".into(),
                validations: vec![DfsReadValidation {
                    grant: source.read_grant.clone(),
                    chunk_id: source.chunk_id.clone(),
                    copy_id: source.copy_id.clone(),
                    chunk_offset: 0,
                    length: 4,
                }],
            })
            .await
            .map(|_| ())
    }

    async fn restart(&self, device: StorageDeviceDescriptor) -> NodeSession {
        register(&self.store, "receiver", "restarted", vec![device]).await;
        match self
            .store
            .read(MetaRead::CurrentNodeSession {
                node_id: "receiver".into(),
            })
            .await
            .unwrap()
            .entity
        {
            Some(MetaEntity::NodeSession(session)) => session,
            _ => panic!("replacement session is missing"),
        }
    }
}

async fn register(store: &Store, id: &str, session: &str, devices: Vec<StorageDeviceDescriptor>) {
    store
        .register_node_session(
            RequestKey::new(id, format!("register-{session}")),
            NodeSessionLease {
                node_id: id.into(),
                session_id: session.into(),
                grpc_addr: "http://127.0.0.1:1".into(),
                data_addr: "http://127.0.0.1:2".into(),
                rest_addr: "http://127.0.0.1:3".into(),
                ownerfs_enabled: false,
                storage_devices: devices,
                lease_ttl: std::time::Duration::from_secs(30),
            },
        )
        .await
        .unwrap();
}

async fn legacy_direct_commit_fixture() -> (
    DfsService,
    Arc<Store>,
    InodeRecord,
    FileVersion,
    LayoutRoot,
    RequestKey,
) {
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    let service = DfsService::new(store.clone());
    let mut inode = root_inode(NamespaceId::new("default"));
    inode.inode_id = InodeId::new("legacy-file");
    inode.kind = InodeKind::Regular;
    inode.head_version = Some(FileVersionId::new("legacy-version"));
    inode.revision = 2;
    let version = FileVersion {
        id: FileVersionId::new("legacy-version"),
        inode_id: inode.inode_id.clone(),
        parent_version: None,
        length: 0,
        layout_root: LayoutRootId::new("legacy-layout"),
        created_at_unix_ms: 1,
    };
    let layout = LayoutRoot {
        id: LayoutRootId::new("legacy-layout"),
        file_length: 0,
        inline_extents: Vec::new(),
    };
    let key = RequestKey::new("legacy-node", "legacy-commit");
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
        TxnMutation::Put(MetaEntity::DfsFileVersion(version.clone())),
        TxnMutation::Put(MetaEntity::DfsLayoutRoot(layout.clone())),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key.clone(),
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::DfsInode(inode.clone()),
        }),
    ]);
    store.compare_and_commit(txn).await.unwrap();
    (service, store, inode, version, layout, key)
}

fn commit_for_stored_version(
    operation_id: OperationId,
    inode: &InodeRecord,
    version: FileVersion,
    layout: LayoutRoot,
) -> CommitFileVersion {
    CommitFileVersion {
        gc_fence: None,
        operation_id,
        inode_id: inode.inode_id.clone(),
        write_lease: WriteLease {
            inode_id: inode.inode_id.clone(),
            owner_node_id: "legacy-node".into(),
            owner_session_id: "legacy-session".into(),
            lease_epoch: 1,
            expires_at_unix_ms: 1,
        },
        expected_inode_revision: inode.revision,
        expected_head_version: None,
        file_version: version,
        layout_root: layout,
        chunk_receipts: Vec::new(),
        clone_source: None,
        metadata_delta: crate::dfs::CommitMetadataDelta {
            mode: CommitMetadataMode::DataOnly,
            mtime_unix_ms: None,
            ctime_unix_ms: None,
            kill_suidgid: false,
        },
    }
}

async fn fixture() -> Fixture {
    fixture_with_bytes(b"data").await
}

async fn fixture_with_bytes(bytes: &[u8]) -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let local = LocalChunkStore::open(temp.path(), "receiver").unwrap();
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    // Registration precedes writes: the live descriptor retains floor 0.
    register(&store, "reader", "reader-original", Vec::new()).await;
    register(
        &store,
        "receiver",
        "receiver-original",
        vec![local.device_descriptor().unwrap()],
    )
    .await;
    let staged = StagedChunk::new(OperationId::new("durable-r1"), bytes.to_vec());
    let receipt = local.put(staged.clone()).unwrap();
    let ack = &receipt.durable_acks[0];
    let copy = CopyRecord {
        id: CopyId::new("copy"),
        chunk_id: staged.chunk.id.clone(),
        role: CopyRole::DurableReplica,
        state: CopyState::Ready,
        location: CopyLocation::Node {
            node_id: ack.node_id.clone(),
            node_epoch: ack.node_epoch,
            device_id: ack.device_id.clone(),
            device_epoch: ack.device_epoch,
            catalog_revision: ack.catalog_revision,
        },
        persisted_bytes: ack.persisted_bytes,
        verified_digest: ack.verified_digest.clone(),
    };
    let service = DfsService::new(store.clone());
    let mut inode = root_inode(NamespaceId::new("default"));
    inode.inode_id = InodeId::new("file");
    inode.kind = InodeKind::Regular;
    inode.head_version = Some(FileVersionId::new("version"));
    let layout = LayoutRoot {
        id: LayoutRootId::new("layout"),
        file_length: staged.chunk.length,
        inline_extents: vec![Extent {
            file_offset: 0,
            length: staged.chunk.length,
            chunk_id: staged.chunk.id.clone(),
            chunk_offset: 0,
        }],
    };
    let key = RequestKey::new("fixture", "commit");
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations = vec![
        TxnMutation::Put(MetaEntity::DfsInode(inode)),
        TxnMutation::Put(MetaEntity::DfsFileVersion(FileVersion {
            id: FileVersionId::new("version"),
            inode_id: InodeId::new("file"),
            parent_version: None,
            length: staged.chunk.length,
            layout_root: layout.id.clone(),
            created_at_unix_ms: 1,
        })),
        TxnMutation::Put(MetaEntity::DfsLayoutRoot(layout)),
        TxnMutation::Put(MetaEntity::DfsChunk(staged.chunk.clone())),
        TxnMutation::Put(MetaEntity::DfsCopy(copy.clone())),
        TxnMutation::Put(MetaEntity::DfsPlacement(PlacementRecord {
            chunk_id: staged.chunk.id.clone(),
            replica_group_id: ReplicaGroupId::new("r1"),
            placement_epoch: 1,
            desired_copies: 1,
            copies: vec![copy.id.clone()],
            health: PlacementHealth::Satisfied,
        })),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }),
    ];
    store.compare_and_commit(txn).await.unwrap();
    Fixture {
        service,
        store,
        temp,
        local,
        request: DfsChunkSourcesRequest {
            caller_id: "reader".into(),
            namespace_id: NamespaceId::new("default"),
            file_version_id: FileVersionId::new("version"),
            layout_root_id: LayoutRootId::new("layout"),
            chunk_ids: vec![staged.chunk.id],
        },
        copy,
    }
}

async fn replace_source_layout(f: &Fixture, extents: Vec<Extent>) {
    let key = RequestKey::new("fixture", "source-range-layout");
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsLayoutRoot(LayoutRoot {
            id: f.request.layout_root_id.clone(),
            file_length: f.copy.persisted_bytes,
            inline_extents: extents,
        })),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }),
    ]);
    f.store.compare_and_commit(txn).await.unwrap();
}

#[tokio::test]
async fn chunk_sources_publish_only_committed_grant_ranges() {
    let f = fixture().await;
    replace_source_layout(
        &f,
        vec![Extent {
            file_offset: 1,
            chunk_offset: 1,
            length: 2,
            chunk_id: f.copy.chunk_id.clone(),
        }],
    )
    .await;
    let set = f
        .service
        .chunk_sources(f.request.clone())
        .await
        .unwrap()
        .chunks
        .remove(0);
    assert_eq!(set.allowed_ranges, vec![(1, 2)]);
    let mut request = ValidateDfsReadGrants {
        receiver_node_id: "receiver".into(),
        receiver_node_epoch: 1,
        peer_node_id: "reader".into(),
        validations: vec![DfsReadValidation {
            grant: set.sources[0].read_grant.clone(),
            chunk_id: set.chunk_id,
            copy_id: set.sources[0].copy_id.clone(),
            chunk_offset: 1,
            length: 2,
        }],
    };
    let authorized = f
        .service
        .validate_read_grants(request.clone())
        .await
        .unwrap();
    assert_eq!(authorized[0].allowed_ranges, set.allowed_ranges);
    request.validations[0].chunk_offset = 0;
    request.validations[0].length = 4;
    assert!(f.service.validate_read_grants(request).await.is_err());
}

#[tokio::test]
async fn chunk_sources_fragmented_ranges_keep_grants_but_omit_cache_hint() {
    let f = fixture_with_bytes(&[b'x'; 259]).await;
    replace_source_layout(
        &f,
        (0..129)
            .map(|index| Extent {
                file_offset: index * 2,
                chunk_offset: index * 2,
                length: 1,
                chunk_id: f.copy.chunk_id.clone(),
            })
            .collect(),
    )
    .await;
    let set = f
        .service
        .chunk_sources(f.request.clone())
        .await
        .unwrap()
        .chunks
        .remove(0);
    assert!(set.allowed_ranges.is_empty());
    assert_eq!(set.sources.len(), 1);
    let mut request = ValidateDfsReadGrants {
        receiver_node_id: "receiver".into(),
        receiver_node_epoch: 1,
        peer_node_id: "reader".into(),
        validations: vec![DfsReadValidation {
            grant: set.sources[0].read_grant.clone(),
            chunk_id: set.chunk_id,
            copy_id: set.sources[0].copy_id.clone(),
            chunk_offset: 0,
            length: 1,
        }],
    };
    let authorized = f
        .service
        .validate_read_grants(request.clone())
        .await
        .unwrap();
    assert_eq!(authorized[0].allowed_ranges, vec![(0, 1)]);
    request.validations[0].length = 2;
    assert!(f.service.validate_read_grants(request).await.is_err());
}

#[tokio::test]
async fn legacy_direct_commit_outcome_rejects_replay_but_keeps_state_readable() {
    let (service, _store, inode, version, layout, key) = legacy_direct_commit_fixture().await;

    assert_eq!(
        service.get_inode(inode.inode_id.clone()).await.unwrap(),
        inode
    );
    assert_eq!(
        service.get_file_version(version.id.clone()).await.unwrap(),
        (version.clone(), layout.clone())
    );

    let err = service
        .commit_file_version(
            key.caller_id.clone(),
            commit_for_stored_version(
                OperationId::new(key.request_id.clone()),
                &inode,
                version,
                layout,
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), afs_error::META_CATALOG_INVALID_REQUEST);
    assert!(err.message().contains("lacks its identity proof"));
}

#[tokio::test]
async fn namespace_outcome_validates_condition_failed_existing_digest() {
    let (_service, store, inode, _version, _layout, _key) = legacy_direct_commit_fixture().await;
    let key = RequestKey::new("digest-node", "digest-commit");
    let digest = [7; 32];
    let outcome = RequestOutcome {
        request: key.clone(),
        operation: StoreOperation::DfsCommitFileVersion,
        result: namespace_result(digest, OperationResult::DfsInode(inode.clone())),
    };
    let mut seed = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    seed.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    seed.mutations
        .push(TxnMutation::RecordRequestOutcome(outcome.clone()));
    store.compare_and_commit(seed).await.unwrap();

    let mut replay = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    replay.conditions.push(TxnCondition::RequestAbsent(key));
    replay
        .mutations
        .push(TxnMutation::RecordRequestOutcome(outcome));
    let raced = store.compare_and_commit(replay).await.unwrap();
    assert!(matches!(
        raced,
        TxnOutcome::ConditionFailed {
            existing_outcome: Some(_),
            ..
        }
    ));
    let validated = validate_namespace_outcome(raced.clone(), digest).unwrap();
    assert_eq!(
        inode_outcome(validated, StoreOperation::DfsCommitFileVersion).unwrap(),
        inode
    );

    let mismatched = validate_namespace_outcome(raced, [8; 32]).unwrap_err();
    assert_eq!(mismatched.code(), afs_error::META_CATALOG_INVALID_REQUEST);
    assert!(
        mismatched
            .message()
            .contains("differs from the recorded request")
    );
}

#[tokio::test]
async fn recovered_r1_copy_serves_new_epoch_without_mutating_durable_evidence() {
    let fixture = fixture().await;
    let old = fixture.sources().await.remove(0);
    fixture.validate(&old, 1).await.unwrap();
    let placement = fixture
        .store
        .read(MetaRead::DfsPlacement(old.chunk_id.clone()))
        .await
        .unwrap();
    let recovered = LocalChunkStore::open(fixture.temp.path(), "receiver").unwrap();
    let session = fixture
        .restart(recovered.device_descriptor().unwrap())
        .await;
    assert_eq!(session.lease_epoch, 2);
    let fresh = fixture.sources().await.remove(0);
    assert!(matches!(
        fresh.location,
        CopyLocation::Node { node_epoch: 2, .. }
    ));
    fixture.validate(&fresh, 2).await.unwrap();
    // The old token is unexpired and caller is unchanged: its receiver MAC
    // must nevertheless fail after serving authority changes.
    assert!(fixture.validate(&old, 2).await.is_err());
    assert!(fixture.validate(&fresh, 1).await.is_err());
    let mut bytes = [0; 4];
    assert_eq!(
        recovered.read_at(&fresh.chunk_id, 0, &mut bytes).unwrap(),
        4
    );
    assert_eq!(&bytes, b"data");
    assert!(matches!(
        fixture.store.read(MetaRead::DfsCopy(fixture.copy.id.clone())).await.unwrap().entity,
        Some(MetaEntity::DfsCopy(copy)) if copy == fixture.copy
    ));
    let after = fixture
        .store
        .read(MetaRead::DfsPlacement(old.chunk_id))
        .await
        .unwrap();
    assert_eq!(after.revision, placement.revision);
    assert!(
        matches!(after.entity, Some(MetaEntity::DfsPlacement(value)) if value.copies == vec![fixture.copy.id])
    );
}

#[tokio::test]
async fn recovered_read_copy_rejects_changed_device_and_insufficient_catalog() {
    for case in 0..3 {
        let fixture = fixture().await;
        let old = fixture.sources().await.remove(0);
        let mut device = fixture.local.device_descriptor().unwrap();
        match case {
            0 => device.device_id = "replacement-device".into(),
            1 => device.device_epoch = device.device_epoch.wrapping_add(1),
            _ => device.catalog_revision = 0,
        }
        fixture.restart(device).await;
        assert!(fixture.sources().await.is_empty());
        assert!(fixture.validate(&old, 2).await.is_err());
    }
}

#[tokio::test]
async fn live_read_copy_allows_new_writes_above_startup_catalog_floor() {
    let fixture = fixture().await;
    let device = fixture.local.device_descriptor().unwrap();
    assert!(device.catalog_revision > 0);
    let source = fixture.sources().await.remove(0);
    fixture.validate(&source, 1).await.unwrap();
    let session = match fixture
        .store
        .read(MetaRead::CurrentNodeSession {
            node_id: "receiver".into(),
        })
        .await
        .unwrap()
        .entity
    {
        Some(MetaEntity::NodeSession(session)) => session,
        _ => panic!("original session is missing"),
    };
    assert_eq!(session.storage_devices[0].catalog_revision, 0);
    assert!(serving_read_copy(&fixture.copy, &session, now_unix_ms()).is_some());
}

fn repair_replication() -> ReplicationConfig {
    ReplicationConfig {
        desired_copies: 2,
        sync_required_copies: 2,
        min_distinct_nodes: 1,
        min_distinct_failure_domains: 1,
        local_copy: LocalCopyPolicy::Required,
    }
}

async fn underreplicated_fixture() -> Fixture {
    let fixture = fixture().await;
    register(
        &fixture.store,
        "target",
        "target-original",
        vec![StorageDeviceDescriptor {
            device_id: "target-device".into(),
            device_epoch: 1,
            catalog_revision: 0,
            failure_domain: "target-fd".into(),
        }],
    )
    .await;
    let service = DfsService::with_replication_config(fixture.store.clone(), repair_replication());
    service.initialize_replication_config().await.unwrap();
    let key = RequestKey::new("fixture", "mark-underreplicated");
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsPlacement(PlacementRecord {
            chunk_id: fixture.copy.chunk_id.clone(),
            replica_group_id: ReplicaGroupId::new("repair-r2"),
            placement_epoch: 2,
            desired_copies: 2,
            copies: vec![fixture.copy.id.clone()],
            health: PlacementHealth::UnderReplicated,
        })),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }),
    ]);
    fixture.store.compare_and_commit(txn).await.unwrap();
    Fixture { service, ..fixture }
}

fn claim_request(operation_id: &str) -> ClaimReplicationTask {
    ClaimReplicationTask {
        caller_id: "receiver".into(),
        caller_session_id: "receiver-original".into(),
        caller_node_epoch: 1,
        operation_id: OperationId::new(operation_id),
        lease_seconds: 30,
    }
}

fn repair_acks(claim: &ReplicationClaim) -> Vec<ReplicaAck> {
    claim
        .replica_group
        .targets
        .iter()
        .take(usize::from(claim.replication.desired_copies))
        .map(|target| ReplicaAck {
            operation_id: claim.operation_id.clone(),
            chunk_id: claim.chunk.id.clone(),
            placement_revision: claim.placement_revision,
            placement_epoch: claim.replica_group.placement_epoch,
            node_id: target.node_id.clone(),
            node_epoch: target.node_epoch,
            device_id: target.device.device_id.clone(),
            device_epoch: target.device.device_epoch,
            catalog_revision: target.device.catalog_revision,
            persisted_bytes: claim.chunk.length,
            verified_digest: claim.chunk.content_digest.clone(),
        })
        .collect()
}

async fn overwrite_task(store: &Store, task: ReplicationTask, operation_id: &str) {
    let key = RequestKey::new("fixture", operation_id);
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsReplicationTask(task)),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }),
    ]);
    store.compare_and_commit(txn).await.unwrap();
}

async fn overwrite_copy(store: &Store, copy: CopyRecord, operation_id: &str) {
    let key = RequestKey::new("fixture", operation_id);
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::DfsCopy(copy)),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }),
    ]);
    store.compare_and_commit(txn).await.unwrap();
}

async fn stored_copy(fixture: &Fixture, copy_id: &CopyId) -> CopyRecord {
    match fixture
        .store
        .read(MetaRead::DfsCopy(copy_id.clone()))
        .await
        .unwrap()
        .entity
    {
        Some(MetaEntity::DfsCopy(copy)) => copy,
        _ => panic!("copy missing"),
    }
}

async fn stored_placement(fixture: &Fixture) -> PlacementRecord {
    match fixture
        .store
        .read(MetaRead::DfsPlacement(fixture.copy.chunk_id.clone()))
        .await
        .unwrap()
        .entity
    {
        Some(MetaEntity::DfsPlacement(placement)) => placement,
        _ => panic!("placement missing"),
    }
}

async fn stored_repair_task(fixture: &Fixture) -> ReplicationTask {
    match fixture
        .store
        .read(MetaRead::DfsReplicationTask(repair_task_id(
            &fixture.copy.chunk_id,
        )))
        .await
        .unwrap()
        .entity
    {
        Some(MetaEntity::DfsReplicationTask(task)) => task,
        _ => panic!("repair task missing"),
    }
}

fn node_catalog_revision(copy: &CopyRecord) -> u64 {
    match &copy.location {
        CopyLocation::Node {
            catalog_revision, ..
        } => *catalog_revision,
        CopyLocation::External { .. } => panic!("expected node copy"),
    }
}

fn corruption_report(
    operation_id: &str,
    copy: &CopyRecord,
    catalog_revision: u64,
) -> ReportChunkCorruption {
    let CopyLocation::Node {
        node_id,
        node_epoch,
        device_id,
        device_epoch,
        ..
    } = &copy.location
    else {
        panic!("expected node copy");
    };
    ReportChunkCorruption {
        caller_id: node_id.clone(),
        caller_session_id: format!("{node_id}-original"),
        caller_node_epoch: *node_epoch,
        operation_id: OperationId::new(operation_id),
        chunk_id: copy.chunk_id.clone(),
        device_id: device_id.clone(),
        device_epoch: *device_epoch,
        catalog_revision,
    }
}

async fn task_record(fixture: &Fixture, claim: &ReplicationClaim) -> ReplicationTask {
    match fixture
        .store
        .read(MetaRead::DfsReplicationTask(claim.task_id.clone()))
        .await
        .unwrap()
        .entity
    {
        Some(MetaEntity::DfsReplicationTask(task)) => task,
        _ => panic!("replication task missing"),
    }
}

#[tokio::test]
async fn replication_claim_is_idempotent_and_digest_bound() {
    let fixture = underreplicated_fixture().await;
    let request = claim_request("claim-repair");
    let claim = fixture
        .service
        .claim_replication_task(request.clone())
        .await
        .unwrap()
        .expect("underreplicated chunk should be claimable by its source");
    assert_eq!(claim.task_id, repair_task_id(&fixture.copy.chunk_id));
    assert_eq!(claim.source_copy_id, fixture.copy.id);
    assert_eq!(claim.replication.desired_copies, 2);
    assert_eq!(claim.replication.sync_required_copies, 2);
    assert_eq!(claim.replication.local_copy, LocalCopyPolicy::Required);
    assert_eq!(
        fixture
            .service
            .claim_replication_task(request.clone())
            .await
            .unwrap(),
        Some(claim.clone())
    );
    let mut changed = request;
    changed.lease_seconds = 31;
    assert!(
        fixture
            .service
            .claim_replication_task(changed)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn restored_source_device_can_claim_repair_with_new_epoch() {
    let fixture = underreplicated_fixture().await;
    let session = fixture
        .restart(fixture.local.device_descriptor().unwrap())
        .await;
    let claim = fixture
        .service
        .claim_replication_task(ClaimReplicationTask {
            caller_id: "receiver".into(),
            caller_session_id: session.session_id.clone(),
            caller_node_epoch: session.lease_epoch,
            operation_id: OperationId::new("claim-after-restart"),
            lease_seconds: 30,
        })
        .await
        .unwrap()
        .expect("recovered source device should remain repair-readable");
    assert_eq!(claim.source_copy_id, fixture.copy.id);
    assert_eq!(claim.worker_node_epoch, session.lease_epoch);
}

#[tokio::test]
async fn restarted_worker_reclaims_unexpired_repair_without_waiting_for_task_lease() {
    let fixture = underreplicated_fixture().await;
    let first = fixture
        .service
        .claim_replication_task(claim_request("claim-before-worker-restart"))
        .await
        .unwrap()
        .unwrap();
    assert!(first.expires_at_unix_ms > now_unix_ms());
    // A live matching worker keeps ownership even when another poll arrives.
    assert!(
        fixture
            .service
            .claim_replication_task(claim_request("claim-live-worker-again"))
            .await
            .unwrap()
            .is_none()
    );
    let session = fixture
        .restart(fixture.local.device_descriptor().unwrap())
        .await;
    let replacement = fixture
        .service
        .claim_replication_task(ClaimReplicationTask {
            caller_id: "receiver".into(),
            caller_session_id: session.session_id.clone(),
            caller_node_epoch: session.lease_epoch,
            operation_id: OperationId::new("claim-restarted-unexpired"),
            lease_seconds: 30,
        })
        .await
        .unwrap()
        .expect("a retired process cannot retain a repair lease on its recovered device");
    assert_ne!(replacement.operation_id, first.operation_id);
    assert_eq!(replacement.worker_node_epoch, session.lease_epoch);
    assert_eq!(replacement.source_copy_id, fixture.copy.id);
    assert!(first.expires_at_unix_ms > now_unix_ms());
    let stale = fixture
        .service
        .report_replication_task(ReportReplicationTask {
            caller_id: first.worker_node_id.clone(),
            caller_session_id: first.worker_session_id.clone(),
            caller_node_epoch: first.worker_node_epoch,
            operation_id: OperationId::new("report-retired-worker"),
            durable_acks: repair_acks(&first),
            error: None,
            source_invalid: false,
            claim: first,
        })
        .await;
    assert!(
        stale.is_err(),
        "the retired process must not promote its old report"
    );
}

#[tokio::test]
async fn expired_worker_blocks_without_source_before_repair_lease_expires() {
    let fixture = underreplicated_fixture().await;
    let claim = fixture
        .service
        .claim_replication_task(claim_request("claim-before-worker-expiry"))
        .await
        .unwrap()
        .unwrap();
    // Expire the worker through the real registration/lease path. Keep the
    // CurrentNodeSession pointer present, as happens after process death.
    fixture
        .store
        .register_node_session(
            RequestKey::new("receiver", "shorten-worker-lease"),
            NodeSessionLease {
                node_id: "receiver".into(),
                session_id: "receiver-original".into(),
                grpc_addr: "http://127.0.0.1:1".into(),
                data_addr: "http://127.0.0.1:2".into(),
                rest_addr: "http://127.0.0.1:3".into(),
                ownerfs_enabled: false,
                storage_devices: vec![fixture.local.device_descriptor().unwrap()],
                lease_ttl: std::time::Duration::from_millis(50),
            },
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(75)).await;
    assert!(claim.expires_at_unix_ms > now_unix_ms());
    let result = fixture
        .service
        .claim_replication_task(ClaimReplicationTask {
            caller_id: "target".into(),
            caller_session_id: "target-original".into(),
            caller_node_epoch: 1,
            operation_id: OperationId::new("observe-expired-worker"),
            lease_seconds: 30,
        })
        .await
        .unwrap();
    assert!(result.is_none());
    let task = task_record(&fixture, &claim).await;
    assert_eq!(task.state, ReplicationTaskState::BlockedNoSource);
    assert!(task.claim.is_none());
}

#[tokio::test]
async fn lost_target_persists_repair_debt_before_replacement_is_available() {
    for missing_task in [false, true] {
        let fixture = underreplicated_fixture().await;
        let claim = fixture
            .service
            .claim_replication_task(claim_request("claim-full-copy-set"))
            .await
            .unwrap()
            .unwrap();
        fixture
            .service
            .report_replication_task(ReportReplicationTask {
                caller_id: claim.worker_node_id.clone(),
                caller_session_id: claim.worker_session_id.clone(),
                caller_node_epoch: claim.worker_node_epoch,
                operation_id: OperationId::new("report-full-copy-set"),
                durable_acks: repair_acks(&claim),
                error: None,
                source_invalid: false,
                claim: claim.clone(),
            })
            .await
            .unwrap();
        if missing_task {
            let key = RequestKey::new("fixture", "remove-completed-task");
            let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
            txn.conditions
                .push(TxnCondition::RequestAbsent(key.clone()));
            txn.mutations.extend([
                TxnMutation::Delete(MetaKey::DfsReplicationTask(claim.task_id.clone())),
                TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request: key,
                    operation: StoreOperation::DfsCommitFileVersion,
                    result: OperationResult::Empty,
                }),
            ]);
            fixture.store.compare_and_commit(txn).await.unwrap();
        }
        fixture
            .store
            .register_node_session(
                RequestKey::new("target", "expire-target"),
                NodeSessionLease {
                    node_id: "target".into(),
                    session_id: "target-original".into(),
                    grpc_addr: "http://127.0.0.1:1".into(),
                    data_addr: "http://127.0.0.1:2".into(),
                    rest_addr: "http://127.0.0.1:3".into(),
                    ownerfs_enabled: false,
                    storage_devices: vec![StorageDeviceDescriptor {
                        device_id: "target-device".into(),
                        device_epoch: 1,
                        catalog_revision: 0,
                        failure_domain: "target-fd".into(),
                    }],
                    lease_ttl: std::time::Duration::from_millis(50),
                },
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(75)).await;
        let request = claim_request("observe-lost-target");
        assert!(
            fixture
                .service
                .claim_replication_task(request.clone())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            fixture
                .service
                .claim_replication_task(request)
                .await
                .unwrap()
                .is_none()
        );
        let task = task_record(&fixture, &claim).await;
        assert_eq!(task.state, ReplicationTaskState::Pending);
        assert!(task.claim.is_none());
        assert!(matches!(
            fixture
                .store
                .read(MetaRead::DfsPlacement(fixture.copy.chunk_id.clone()))
                .await
                .unwrap()
                .entity,
            Some(MetaEntity::DfsPlacement(PlacementRecord {
                health: PlacementHealth::UnderReplicated,
                ..
            }))
        ));
        register(
            &fixture.store,
            "target",
            "target-restored",
            vec![StorageDeviceDescriptor {
                device_id: "target-device".into(),
                device_epoch: 1,
                catalog_revision: 0,
                failure_domain: "target-fd".into(),
            }],
        )
        .await;
        assert!(
            fixture
                .service
                .claim_replication_task(claim_request("claim-after-target-restored"))
                .await
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test]
async fn repair_report_requires_full_desired_receipts() {
    let fixture = underreplicated_fixture().await;
    let claim = fixture
        .service
        .claim_replication_task(claim_request("claim-short-report"))
        .await
        .unwrap()
        .unwrap();
    let err = fixture
        .service
        .report_replication_task(ReportReplicationTask {
            caller_id: "receiver".into(),
            caller_session_id: "receiver-original".into(),
            caller_node_epoch: 1,
            operation_id: OperationId::new("report-short"),
            claim,
            durable_acks: Vec::new(),
            error: None,
            source_invalid: false,
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("fewer ACKs"));
}

#[tokio::test]
async fn source_invalid_marks_only_proven_source_and_blocks_without_live_source() {
    let fixture = underreplicated_fixture().await;
    let claim = fixture
        .service
        .claim_replication_task(claim_request("claim-bad-source"))
        .await
        .unwrap()
        .unwrap();
    let task = fixture
        .service
        .report_replication_task(ReportReplicationTask {
            caller_id: "receiver".into(),
            caller_session_id: "receiver-original".into(),
            caller_node_epoch: 1,
            operation_id: OperationId::new("report-bad-source"),
            claim: claim.clone(),
            durable_acks: Vec::new(),
            error: None,
            source_invalid: true,
        })
        .await
        .unwrap();
    assert_eq!(task.state, ReplicationTaskState::BlockedNoSource);
    assert!(task.claim.is_none());
    assert!(matches!(
        fixture
            .store
            .read(MetaRead::DfsCopy(claim.source_copy_id.clone()))
            .await
            .unwrap()
            .entity,
        Some(MetaEntity::DfsCopy(CopyRecord {
            state: CopyState::Corrupt,
            ..
        }))
    ));
    assert!(matches!(
        fixture
            .store
            .read(MetaRead::DfsPlacement(fixture.copy.chunk_id.clone()))
            .await
            .unwrap()
            .entity,
        Some(MetaEntity::DfsPlacement(PlacementRecord {
            health: PlacementHealth::BlockedNoSource,
            ..
        }))
    ));
}

#[tokio::test]
async fn corruption_report_excludes_bad_own_copy_and_persists_repair_debt() {
    let fixture = underreplicated_fixture().await;
    let claim = fixture
        .service
        .claim_replication_task(claim_request("claim-before-corruption-report"))
        .await
        .unwrap()
        .unwrap();
    fixture
        .service
        .report_replication_task(ReportReplicationTask {
            caller_id: claim.worker_node_id.clone(),
            caller_session_id: claim.worker_session_id.clone(),
            caller_node_epoch: claim.worker_node_epoch,
            operation_id: OperationId::new("complete-before-corruption-report"),
            durable_acks: repair_acks(&claim),
            error: None,
            source_invalid: false,
            claim,
        })
        .await
        .unwrap();
    let pre_report_placement = stored_placement(&fixture).await;
    let report = corruption_report(
        "report-own-corruption",
        &fixture.copy,
        node_catalog_revision(&fixture.copy).saturating_add(1),
    );
    fixture
        .service
        .report_chunk_corruption(report.clone())
        .await
        .unwrap();

    let copy = stored_copy(&fixture, &fixture.copy.id).await;
    assert_eq!(copy.state, CopyState::Corrupt);
    let placement = stored_placement(&fixture).await;
    assert_eq!(placement.health, PlacementHealth::UnderReplicated);
    assert_eq!(placement.copies, pre_report_placement.copies);
    let mut observed_copies = Vec::new();
    for copy_id in &placement.copies {
        observed_copies.push(stored_copy(&fixture, copy_id).await);
    }
    let mut excluded = std::collections::HashSet::new();
    excluded.insert(fixture.copy.id.clone());
    assert_eq!(
        live_sources_among(&observed_copies, &excluded, &fixture.service, now_unix_ms(),)
            .await
            .unwrap()
            .count,
        1
    );
    let task = stored_repair_task(&fixture).await;
    assert_eq!(task.state, ReplicationTaskState::Pending);
    assert!(task.claim.is_none());
    assert_eq!(task.existing_copies, placement.copies);
    assert_eq!(
        fixture
            .service
            .report_chunk_corruption(report)
            .await
            .unwrap(),
        ()
    );
}

#[tokio::test]
async fn corruption_report_rejects_forged_identity_device_revision_and_session() {
    let fixture = underreplicated_fixture().await;
    let mut other_node = corruption_report(
        "report-forged-node",
        &fixture.copy,
        node_catalog_revision(&fixture.copy).saturating_add(1),
    );
    other_node.caller_id = "target".into();
    other_node.caller_session_id = "target-original".into();
    assert!(
        fixture
            .service
            .report_chunk_corruption(other_node)
            .await
            .is_err()
    );

    let mut other_device = corruption_report(
        "report-forged-device",
        &fixture.copy,
        node_catalog_revision(&fixture.copy).saturating_add(1),
    );
    other_device.device_id = "other-device".into();
    assert!(
        fixture
            .service
            .report_chunk_corruption(other_device)
            .await
            .is_err()
    );

    let mut zero_revision = corruption_report("report-zero-revision", &fixture.copy, 0);
    assert!(
        fixture
            .service
            .report_chunk_corruption(zero_revision.clone())
            .await
            .is_err()
    );

    zero_revision.operation_id = OperationId::new("report-forged-session");
    zero_revision.catalog_revision = node_catalog_revision(&fixture.copy).saturating_add(1);
    zero_revision.caller_session_id = "stale-session".into();
    assert!(
        fixture
            .service
            .report_chunk_corruption(zero_revision)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn corruption_report_exact_replay_does_not_remark_newer_repaired_copy() {
    let fixture = fixture().await;
    fixture
        .service
        .initialize_replication_config()
        .await
        .unwrap();
    let report_revision = node_catalog_revision(&fixture.copy).saturating_add(1);
    let report = corruption_report("report-replay-corruption", &fixture.copy, report_revision);
    fixture
        .service
        .report_chunk_corruption(report.clone())
        .await
        .unwrap();

    let mut repaired = fixture.copy.clone();
    repaired.state = CopyState::Ready;
    if let CopyLocation::Node {
        catalog_revision, ..
    } = &mut repaired.location
    {
        *catalog_revision = report_revision.saturating_add(1);
    }
    overwrite_copy(
        &fixture.store,
        repaired.clone(),
        "overwrite-newer-repaired-copy",
    )
    .await;
    fixture
        .service
        .report_chunk_corruption(report)
        .await
        .unwrap();
    assert_eq!(stored_copy(&fixture, &fixture.copy.id).await, repaired);
}

#[tokio::test]
async fn corruption_report_fresh_stale_quarantine_ack_preserves_newer_repair() {
    let fixture = fixture().await;
    fixture
        .service
        .initialize_replication_config()
        .await
        .unwrap();
    let report_revision = node_catalog_revision(&fixture.copy).saturating_add(1);
    let report = corruption_report("report-before-fresh-stale", &fixture.copy, report_revision);
    fixture
        .service
        .report_chunk_corruption(report)
        .await
        .unwrap();

    let mut repaired = fixture.copy.clone();
    repaired.state = CopyState::Ready;
    if let CopyLocation::Node {
        catalog_revision, ..
    } = &mut repaired.location
    {
        *catalog_revision = report_revision.saturating_add(1);
    }
    overwrite_copy(
        &fixture.store,
        repaired.clone(),
        "overwrite-newer-repaired-copy-before-fresh-stale",
    )
    .await;

    let mut stale = corruption_report(
        "report-fresh-stale-quarantine",
        &fixture.copy,
        report_revision,
    );
    fixture
        .service
        .report_chunk_corruption(stale.clone())
        .await
        .unwrap();
    assert_eq!(stored_copy(&fixture, &fixture.copy.id).await, repaired);

    fixture
        .service
        .report_chunk_corruption(stale.clone())
        .await
        .unwrap();
    stale.catalog_revision = node_catalog_revision(&repaired).saturating_add(1);
    assert!(
        fixture
            .service
            .report_chunk_corruption(stale)
            .await
            .is_err()
    );
    assert_eq!(stored_copy(&fixture, &fixture.copy.id).await, repaired);
}

#[tokio::test]
async fn corruption_report_rejects_modified_request_reusing_same_operation() {
    let fixture = fixture().await;
    fixture
        .service
        .initialize_replication_config()
        .await
        .unwrap();
    let mut report = corruption_report(
        "report-digest-bound",
        &fixture.copy,
        node_catalog_revision(&fixture.copy).saturating_add(1),
    );
    fixture
        .service
        .report_chunk_corruption(report.clone())
        .await
        .unwrap();
    report.catalog_revision = report.catalog_revision.saturating_add(1);
    assert!(
        fixture
            .service
            .report_chunk_corruption(report)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn corruption_report_blocks_when_all_sources_are_bad() {
    let fixture = fixture().await;
    fixture
        .service
        .initialize_replication_config()
        .await
        .unwrap();
    let report = corruption_report(
        "report-only-source-corruption",
        &fixture.copy,
        node_catalog_revision(&fixture.copy).saturating_add(1),
    );
    fixture
        .service
        .report_chunk_corruption(report)
        .await
        .unwrap();
    assert_eq!(
        stored_copy(&fixture, &fixture.copy.id).await.state,
        CopyState::Corrupt
    );
    assert_eq!(
        stored_placement(&fixture).await.health,
        PlacementHealth::BlockedNoSource
    );
    assert_eq!(
        stored_repair_task(&fixture).await.state,
        ReplicationTaskState::BlockedNoSource
    );
}

#[tokio::test]
async fn late_exact_report_with_full_receipts_completes_and_replays() {
    let fixture = underreplicated_fixture().await;
    let mut claim = fixture
        .service
        .claim_replication_task(claim_request("claim-late-report"))
        .await
        .unwrap()
        .unwrap();
    let mut task = task_record(&fixture, &claim).await;
    claim.expires_at_unix_ms = 1;
    task.claim = Some(Box::new(claim.clone()));
    task.next_retry_unix_ms = 1;
    overwrite_task(&fixture.store, task, "expire-running-claim").await;

    let report = ReportReplicationTask {
        caller_id: "receiver".into(),
        caller_session_id: "receiver-original".into(),
        caller_node_epoch: 1,
        operation_id: OperationId::new("report-late-success"),
        durable_acks: repair_acks(&claim),
        error: None,
        source_invalid: false,
        claim: claim.clone(),
    };
    let task = fixture
        .service
        .report_replication_task(report.clone())
        .await
        .unwrap();
    assert_eq!(task.state, ReplicationTaskState::Completed);
    assert_eq!(task.placement_epoch, claim.replica_group.placement_epoch);
    assert_eq!(task.desired_copies, claim.replication.desired_copies);
    assert_eq!(
        fixture
            .service
            .report_replication_task(report)
            .await
            .unwrap(),
        task
    );
}

#[tokio::test]
async fn stale_reassigned_report_returns_superseded_code() {
    let fixture = underreplicated_fixture().await;
    let mut old_claim = fixture
        .service
        .claim_replication_task(claim_request("claim-old"))
        .await
        .unwrap()
        .unwrap();
    let mut task = task_record(&fixture, &old_claim).await;
    old_claim.expires_at_unix_ms = 1;
    task.claim = Some(Box::new(old_claim.clone()));
    task.next_retry_unix_ms = 1;
    overwrite_task(&fixture.store, task, "expire-old-claim").await;
    let new_claim = fixture
        .service
        .claim_replication_task(claim_request("claim-new"))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(new_claim.operation_id, old_claim.operation_id);

    let err = fixture
        .service
        .report_replication_task(ReportReplicationTask {
            caller_id: "receiver".into(),
            caller_session_id: "receiver-original".into(),
            caller_node_epoch: 1,
            operation_id: OperationId::new("report-stale"),
            durable_acks: repair_acks(&old_claim),
            error: None,
            source_invalid: false,
            claim: old_claim,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), afs_error::META_DFS_REPAIR_SUPERSEDED);
}

#[tokio::test]
async fn mismatched_source_copy_is_not_claimable() {
    let fixture = underreplicated_fixture().await;
    let mut bad = fixture.copy.clone();
    bad.persisted_bytes = bad.persisted_bytes.saturating_add(1);
    overwrite_copy(&fixture.store, bad, "mismatch-source-copy").await;
    let claim = fixture
        .service
        .claim_replication_task(claim_request("claim-mismatch-source"))
        .await
        .unwrap();
    assert!(claim.is_none());
}

#[tokio::test]
async fn recovered_read_copy_rejects_regressed_missing_expired_or_wrong_authority() {
    let fixture = fixture().await;
    let session = fixture
        .restart(fixture.local.device_descriptor().unwrap())
        .await;
    let now = now_unix_ms();
    let mut future_copy = fixture.copy.clone();
    if let CopyLocation::Node { node_epoch, .. } = &mut future_copy.location {
        *node_epoch = session.lease_epoch + 1;
    }
    assert!(serving_read_copy(&future_copy, &session, now).is_none());
    let mut bad = session.clone();
    bad.storage_devices.clear();
    assert!(serving_read_copy(&fixture.copy, &bad, now).is_none());
    bad = session.clone();
    bad.node_id = "other-node".into();
    assert!(serving_read_copy(&fixture.copy, &bad, now).is_none());
    bad = session.clone();
    bad.expires_at_unix_ms = now;
    assert!(serving_read_copy(&fixture.copy, &bad, now).is_none());
    let mut corrupt = fixture.copy.clone();
    corrupt.state = CopyState::Corrupt;
    assert!(serving_read_copy(&corrupt, &session, now).is_none());
    let mut zero_epoch = fixture.copy.clone();
    if let CopyLocation::Node { node_epoch, .. } = &mut zero_epoch.location {
        *node_epoch = 0;
    }
    assert!(serving_read_copy(&zero_epoch, &session, now).is_none());
}
