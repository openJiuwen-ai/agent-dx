use std::sync::Mutex;

use super::*;
use crate::dfs::{LocalCopyPolicy, OperationId, StorageDeviceDescriptor};
use crate::node::chunk::StagedChunk;
use crate::node::fault::repair::unix_ms;

struct StaticPlacement {
    snapshot: Arc<PlacementSnapshot>,
}

impl PlacementProvider for StaticPlacement {
    fn snapshot(&self) -> Result<Arc<PlacementSnapshot>> {
        Ok(self.snapshot.clone())
    }

    fn refresh(&self, _: u64) -> Result<Arc<PlacementSnapshot>> {
        Ok(self.snapshot.clone())
    }
}

struct RefreshingPlacement {
    cached: Arc<PlacementSnapshot>,
    fresh: Arc<PlacementSnapshot>,
    refreshes: std::sync::atomic::AtomicUsize,
}

impl PlacementProvider for RefreshingPlacement {
    fn snapshot(&self) -> Result<Arc<PlacementSnapshot>> {
        Ok(self.cached.clone())
    }

    fn refresh(&self, _: u64) -> Result<Arc<PlacementSnapshot>> {
        self.refreshes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(self.fresh.clone())
    }
}

#[derive(Default)]
struct FakeDataPlane {
    prepared: Mutex<Vec<(ChunkId, String)>>,
}

impl ReplicaDataPlane for FakeDataPlane {
    fn mode(&self) -> ReplicaTransferMode {
        ReplicaTransferMode::GrpcStream
    }

    fn prepare_peer(&self, op: &ReplicaPeerOp) -> Result<()> {
        self.prepared
            .lock()
            .unwrap()
            .push((op.chunk_id.clone(), op.target.node_id.clone()));
        Ok(())
    }

    fn put_peer_replica(
        &self,
        op: &ReplicaPeerOp,
        staged: &StagedChunk,
    ) -> Result<Vec<ReplicaAck>> {
        let mut acks = vec![ack_for(&op.target, op, staged)];
        acks.extend(
            op.chain_tail
                .iter()
                .map(|target| ack_for(target, op, staged)),
        );
        Ok(acks)
    }
}

fn ack_for(target: &ReplicaTarget, op: &ReplicaPeerOp, staged: &StagedChunk) -> ReplicaAck {
    ReplicaAck {
        operation_id: staged.operation_id.clone(),
        chunk_id: staged.chunk.id.clone(),
        placement_revision: op.placement_revision,
        placement_epoch: op.placement_epoch,
        node_id: target.node_id.clone(),
        node_epoch: target.node_epoch,
        device_id: target.device.device_id.clone(),
        device_epoch: target.device.device_epoch,
        catalog_revision: target.device.catalog_revision,
        persisted_bytes: staged.chunk.length,
        verified_digest: staged.chunk.content_digest.clone(),
    }
}

fn target(node_id: &str, device: StorageDeviceDescriptor) -> ReplicaTarget {
    ReplicaTarget {
        node_id: node_id.into(),
        node_epoch: 1,
        data_endpoint: format!("http://{node_id}"),
        device,
    }
}

fn target_with_epoch(
    node_id: &str,
    node_epoch: u64,
    device: StorageDeviceDescriptor,
) -> ReplicaTarget {
    let mut target = target(node_id, device);
    target.node_epoch = node_epoch;
    target
}

fn remote_device(device_id: &str) -> StorageDeviceDescriptor {
    StorageDeviceDescriptor {
        device_id: device_id.into(),
        device_epoch: 1,
        catalog_revision: 0,
        failure_domain: device_id.into(),
    }
}

fn staged(payload: &str) -> StagedChunk {
    StagedChunk::new(
        OperationId::new(format!("op-{payload}")),
        payload.as_bytes().to_vec(),
    )
}

fn staged_for_group(snapshot: &PlacementSnapshot, group_index: usize) -> StagedChunk {
    for attempt in 0..10_000 {
        let item = staged(&format!("group-{group_index}-{attempt}"));
        if stable_group_index(&item.chunk.id, snapshot.replica_groups.len()) == group_index {
            return item;
        }
    }
    panic!("could not find a staged Chunk for replica group {group_index}");
}

fn config(copies: u16) -> ReplicationConfig {
    ReplicationConfig {
        desired_copies: copies,
        sync_required_copies: copies,
        min_distinct_nodes: copies,
        min_distinct_failure_domains: 1,
        local_copy: LocalCopyPolicy::Required,
    }
}

#[test]
fn p2_w2_required_receipt_releases_only_for_exact_local_durable_identity() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let snapshot = Arc::new(PlacementSnapshot {
        revision: 11,
        local_copy: EffectiveLocalCopy::declared(&config(1)),
        replication: config(1),
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("native-local"),
            placement_epoch: 12,
            targets: vec![target_with_epoch(
                "node-a",
                1,
                local.device_descriptor().unwrap(),
            )],
        }],
    });
    let store = DfsChunkSink::new_with_epoch(
        "node-a".into(),
        1,
        local.clone(),
        Arc::new(StaticPlacement { snapshot }),
        Arc::new(FakeDataPlane::default()),
    );
    let input = staged("native-durable-body");
    let mut receipts = store
        .put_chunks(vec![input.clone()], PutIntent::PipelineUpload)
        .unwrap();
    let receipt = receipts.remove(0);
    assert_eq!(receipt.body_release, BodyRelease::ReleaseAfterUpload);
    assert_eq!(receipt.receipt.chunk, input.chunk);
    let mut output = [0; 7];
    assert_eq!(
        store
            .read_uploaded_at(&receipt.receipt, 0, &mut output)
            .unwrap(),
        output.len()
    );
    assert_eq!(&output, b"native-");
    let mut wrong = receipt.receipt.clone();
    wrong.chunk.length += 1;
    let mut untouched = [0x81; 7];
    assert!(store.read_uploaded_at(&wrong, 0, &mut untouched).is_err());
    assert_eq!(untouched, [0x81; 7]);
    assert!(
        local.open_receipt_reader(&receipt.receipt.chunk).is_ok(),
        "a caller expected-root mismatch must not quarantine a healthy local object"
    );
}

/// The local-release difference `PutIntent` expresses, on the same store.
#[test]
fn put_intent_selects_local_release_decision() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let snapshot = Arc::new(PlacementSnapshot {
        revision: 11,
        local_copy: EffectiveLocalCopy::declared(&config(1)),
        replication: config(1),
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("intent"),
            placement_epoch: 12,
            targets: vec![target_with_epoch(
                "node-a",
                1,
                local.device_descriptor().unwrap(),
            )],
        }],
    });
    let store = DfsChunkSink::new_with_epoch(
        "node-a".into(),
        1,
        local,
        Arc::new(StaticPlacement { snapshot }),
        Arc::new(FakeDataPlane::default()),
    );

    let committed = store
        .put_chunks(vec![staged("intent-commit")], PutIntent::Commit)
        .unwrap();
    assert_eq!(committed[0].body_release, BodyRelease::RetainUntilPublish);

    let uploaded = store
        .put_chunks(vec![staged("intent-upload")], PutIntent::PipelineUpload)
        .unwrap();
    assert_eq!(uploaded[0].body_release, BodyRelease::ReleaseAfterUpload);
}

#[test]
fn p2_w2_nonrequired_remote_receipt_never_claims_local_body_replacement() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "writer").unwrap());
    let mut policy = config(1);
    policy.local_copy = LocalCopyPolicy::Preferred;
    let snapshot = Arc::new(PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&policy),
        replication: policy,
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("remote-only"),
            placement_epoch: 8,
            targets: vec![target("node-b", remote_device("remote-b"))],
        }],
    });
    let store = DfsChunkSink::new_with_epoch(
        "writer".into(),
        1,
        local,
        Arc::new(StaticPlacement { snapshot }),
        Arc::new(FakeDataPlane::default()),
    );
    let receipt = store
        .put_chunks(vec![staged("remote-native")], PutIntent::PipelineUpload)
        .unwrap()
        .remove(0);
    assert_eq!(receipt.body_release, BodyRelease::RetainUntilPublish);
    assert!(
        receipt
            .receipt
            .durable_acks
            .iter()
            .all(|ack| ack.node_id != "writer")
    );
}

#[test]
fn p2_w2_descriptor_reservation_accounts_for_all_possible_authority_targets() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let mut long_node = String::with_capacity(8192);
    long_node.push_str("remote-b");
    let mut remote = target(&long_node, remote_device("remote-b"));
    remote.node_id = long_node;
    let snapshot = Arc::new(PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&config(1)),
        replication: config(1),
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("reservation"),
            placement_epoch: 8,
            targets: vec![target("node-a", local.device_descriptor().unwrap()), remote],
        }],
    });
    let store = DfsChunkSink::new_with_epoch(
        "node-a".into(),
        1,
        local,
        Arc::new(StaticPlacement { snapshot }),
        Arc::new(FakeDataPlane::default()),
    );
    let reserved = store
        .descriptor_reservation_bytes(&OperationId::new("native-reservation"))
        .unwrap();
    assert!(
        reserved >= 8192 * 8,
        "authority beyond the minimum synchronous prefix must still be reserved"
    );
}

#[test]
fn local_r1_capacity_forwards_after_catalog_advance_without_revision_equality() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let device_before_write = local.device_descriptor().unwrap();
    local.put(staged("capacity-advance")).unwrap();
    assert!(
        local.device_descriptor().unwrap().catalog_revision > device_before_write.catalog_revision
    );
    let snapshot = Arc::new(PlacementSnapshot {
        revision: 11,
        local_copy: EffectiveLocalCopy::declared(&config(1)),
        replication: config(1),
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("group"),
            placement_epoch: 12,
            targets: vec![target_with_epoch("node-a", 1, device_before_write)],
        }],
    });
    let store = DfsChunkSink::new_with_epoch(
        "node-a".into(),
        1,
        local,
        Arc::new(StaticPlacement { snapshot }),
        Arc::new(FakeDataPlane::default()),
    );

    let capacity = store.capacity().unwrap();

    assert!(capacity.bsize > 0);
    assert!(capacity.frsize > 0);
    assert!(capacity.blocks >= capacity.bfree);
}

#[test]
fn capacity_denies_replicated_or_remote_or_wrong_device_authority() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let local_device = local.device_descriptor().unwrap();
    let cases = vec![
        (
            "replicated",
            config(2),
            vec![
                target_with_epoch("node-a", 1, local_device.clone()),
                target_with_epoch("node-b", 1, remote_device("remote-b")),
            ],
            afs_error::NODE_VFS_UNIMPLEMENTED,
        ),
        (
            "remote",
            config(1),
            vec![target_with_epoch("node-b", 1, remote_device("remote-b"))],
            afs_error::NODE_VFS_UNIMPLEMENTED,
        ),
        (
            "wrong-device",
            config(1),
            vec![target_with_epoch("node-a", 1, remote_device("wrong-local"))],
            afs_error::NODE_STORAGE_INVALID,
        ),
        (
            "wrong-epoch",
            config(1),
            vec![target_with_epoch("node-a", 2, local_device.clone())],
            afs_error::NODE_VFS_UNIMPLEMENTED,
        ),
    ];
    for (name, replication, targets, code) in cases {
        let snapshot = Arc::new(PlacementSnapshot {
            revision: 11,
            local_copy: EffectiveLocalCopy::declared(&replication),
            replication,
            replica_groups: vec![ReplicaGroup {
                id: ReplicaGroupId::new(format!("group-{name}")),
                placement_epoch: 12,
                targets,
            }],
        });
        let store = DfsChunkSink::new_with_epoch(
            "node-a".into(),
            1,
            local.clone(),
            Arc::new(StaticPlacement { snapshot }),
            Arc::new(FakeDataPlane::default()),
        );

        assert_eq!(store.capacity().unwrap_err().code(), code, "case {name}");
    }
}

#[test]
fn put_batch_groups_by_stable_chunk_plan_and_preserves_receipt_order() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let local_device = local.device_descriptor().unwrap();
    let snapshot = Arc::new(PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&config(3)),
        replication: config(3),
        replica_groups: vec![
            ReplicaGroup {
                id: ReplicaGroupId::new("group-0"),
                placement_epoch: 70,
                targets: vec![
                    target("node-a", local_device.clone()),
                    target("node-b", remote_device("remote-b")),
                    target("node-c", remote_device("remote-c")),
                ],
            },
            ReplicaGroup {
                id: ReplicaGroupId::new("group-1"),
                placement_epoch: 71,
                targets: vec![
                    target("node-a", local_device),
                    target("node-d", remote_device("remote-d")),
                    target("node-e", remote_device("remote-e")),
                ],
            },
        ],
    });
    let first = staged_for_group(&snapshot, 1);
    let second = staged_for_group(&snapshot, 0);
    let data_plane = Arc::new(FakeDataPlane::default());
    let store = DfsChunkSink::new(
        "node-a".into(),
        local,
        Arc::new(StaticPlacement {
            snapshot: snapshot.clone(),
        }),
        data_plane.clone(),
    );

    let receipts = store
        .put_batch(vec![first.clone(), second.clone()])
        .unwrap();

    assert_eq!(receipts[0].chunk.id, first.chunk.id);
    assert_eq!(receipts[0].replica_group_id, ReplicaGroupId::new("group-1"));
    assert_eq!(receipts[0].placement_epoch, 71);
    assert_eq!(receipts[0].durable_acks.len(), 3);
    assert_eq!(receipts[1].chunk.id, second.chunk.id);
    assert_eq!(receipts[1].replica_group_id, ReplicaGroupId::new("group-0"));
    assert_eq!(receipts[1].placement_epoch, 70);
    assert_eq!(receipts[1].durable_acks.len(), 3);
    assert_eq!(data_plane.prepared.lock().unwrap().len(), 2);
}

#[test]
fn non_head_local_rn_fails_before_prepare_or_local_persist() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let local_device = local.device_descriptor().unwrap();
    let snapshot = Arc::new(PlacementSnapshot {
        revision: 9,
        local_copy: EffectiveLocalCopy::declared(&config(2)),
        replication: config(2),
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("group-0"),
            placement_epoch: 90,
            targets: vec![
                target("node-b", remote_device("remote-b")),
                target("node-a", local_device),
            ],
        }],
    });
    let item = staged("non-head-local");
    let data_plane = Arc::new(FakeDataPlane::default());
    let store = DfsChunkSink::new(
        "node-a".into(),
        local.clone(),
        Arc::new(StaticPlacement { snapshot }),
        data_plane.clone(),
    );

    let error = store.put_batch(vec![item.clone()]).unwrap_err();

    assert_eq!(error.code(), afs_error::NODE_STORAGE_INVALID);
    assert!(data_plane.prepared.lock().unwrap().is_empty());
    let mut out = [0; 1];
    assert!(local.read_at(&item.chunk.id, 0, &mut out).is_err());
}

#[test]
fn rn_write_batch_refreshes_placement_before_replica_receipts() {
    assert_fresh_placement_write(1, 1);
    assert_fresh_placement_write(2, 2);
    assert_fresh_placement_write(2, 3);
}

fn assert_fresh_placement_write(sync_required_copies: u16, chunk_count: usize) {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let local_device = local.device_descriptor().unwrap();
    let policy = ReplicationConfig {
        desired_copies: 2,
        sync_required_copies,
        min_distinct_nodes: sync_required_copies,
        min_distinct_failure_domains: 1,
        local_copy: LocalCopyPolicy::Required,
    };
    let cached = Arc::new(PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&policy),
        replication: policy.clone(),
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("group"),
            placement_epoch: 70,
            targets: vec![
                target("node-a", local_device.clone()),
                target_with_epoch("node-b", 1, remote_device("remote-b")),
            ],
        }],
    });
    let fresh = Arc::new(PlacementSnapshot {
        revision: 8,
        local_copy: EffectiveLocalCopy::declared(&policy),
        replication: policy,
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("group"),
            placement_epoch: 71,
            targets: vec![
                target("node-a", local_device),
                target_with_epoch("node-b", 2, remote_device("remote-b")),
            ],
        }],
    });
    let placement = Arc::new(RefreshingPlacement {
        cached,
        fresh,
        refreshes: std::sync::atomic::AtomicUsize::new(0),
    });
    let plane = Arc::new(FakeDataPlane::default());
    let store = DfsChunkSink::new_with_epoch("node-a".into(), 1, local, placement.clone(), plane);

    let chunks = (0..chunk_count)
        .map(|index| staged(&format!("fresh-placement-{sync_required_copies}-{index}")))
        .collect::<Vec<_>>();
    let receipts = store.put_batch(chunks).unwrap();

    assert_eq!(receipts.len(), chunk_count);
    for receipt in receipts {
        assert_eq!(receipt.placement_revision, 8);
        assert_eq!(receipt.placement_epoch, 71);
        assert_eq!(
            receipt.durable_acks.len(),
            usize::from(sync_required_copies)
        );
        assert!(receipt.durable_acks.iter().any(|ack| {
            ack.node_id == "node-a" && ack.node_epoch == 1 && ack.placement_epoch == 71
        }));
        if sync_required_copies == 1 {
            assert!(
                receipt
                    .durable_acks
                    .iter()
                    .all(|ack| ack.node_id == "node-a")
            );
        } else {
            assert!(receipt.durable_acks.iter().any(|ack| {
                ack.node_id == "node-b" && ack.node_epoch == 2 && ack.placement_epoch == 71
            }));
        }
    }
    assert_eq!(
        placement
            .refreshes
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[test]
fn configurable_two_and_four_copy_chains_return_distinct_durable_acks() {
    for copies in [2_u16, 4] {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let mut targets = vec![target("node-a", local.device_descriptor().unwrap())];
        targets.extend((1..copies).map(|index| {
            target(
                &format!("remote-{index}"),
                remote_device(&format!("disk-{index}")),
            )
        }));
        let snapshot = Arc::new(PlacementSnapshot {
            revision: 7,
            local_copy: EffectiveLocalCopy::declared(&config(copies)),
            replication: config(copies),
            replica_groups: vec![ReplicaGroup {
                id: ReplicaGroupId::new("group"),
                placement_epoch: 8,
                targets,
            }],
        });
        let plane = Arc::new(FakeDataPlane::default());
        let store = DfsChunkSink::new_with_epoch(
            "node-a".into(),
            1,
            local,
            Arc::new(StaticPlacement { snapshot }),
            plane,
        );
        assert_eq!(
            store.put(staged("variable-n")).unwrap().durable_acks.len(),
            usize::from(copies)
        );
    }
}

#[test]
fn preferred_remote_head_does_not_create_unassigned_local_copy() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "writer").unwrap());
    let mut policy = config(2);
    policy.local_copy = LocalCopyPolicy::Preferred;
    let snapshot = Arc::new(PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&policy),
        replication: policy,
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("group"),
            placement_epoch: 8,
            targets: vec![
                target("head", remote_device("head-disk")),
                target("tail", remote_device("tail-disk")),
            ],
        }],
    });
    let plane = Arc::new(FakeDataPlane::default());
    let store = DfsChunkSink::new_with_epoch(
        "writer".into(),
        9,
        local.clone(),
        Arc::new(StaticPlacement { snapshot }),
        plane.clone(),
    );
    let item = staged("remote-head");
    let receipt = store.put(item.clone()).unwrap();
    assert_eq!(receipt.durable_acks.len(), 2);
    assert_eq!(plane.prepared.lock().unwrap()[0].1, "head");
    assert!(local.read_at(&item.chunk.id, 0, &mut [0; 1]).is_err());
}

#[test]
fn async_desired_three_sync_one_does_not_wait_for_tail() {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let mut policy = config(3);
    policy.sync_required_copies = 1;
    policy.min_distinct_nodes = 1;
    let snapshot = Arc::new(PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&policy),
        replication: policy,
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("group"),
            placement_epoch: 8,
            targets: vec![
                target("node-a", local.device_descriptor().unwrap()),
                target("tail-1", remote_device("disk-1")),
                target("tail-2", remote_device("disk-2")),
            ],
        }],
    });
    let plane = Arc::new(FakeDataPlane::default());
    let store = DfsChunkSink::new_with_epoch(
        "node-a".into(),
        1,
        local,
        Arc::new(StaticPlacement { snapshot }),
        plane.clone(),
    );
    assert_eq!(store.put(staged("async-m1")).unwrap().durable_acks.len(), 1);
    assert!(plane.prepared.lock().unwrap().is_empty());
}

#[test]
fn failure_domain_constraint_extends_synchronous_prefix() {
    let mut policy = config(3);
    policy.sync_required_copies = 1;
    policy.min_distinct_nodes = 1;
    policy.min_distinct_failure_domains = 2;
    let mut targets = vec![
        target("a", remote_device("a")),
        target("b", remote_device("b")),
        target("c", remote_device("c")),
    ];
    targets[0].device.failure_domain = "zone-1".into();
    targets[1].device.failure_domain = "zone-1".into();
    targets[2].device.failure_domain = "zone-2".into();
    let snapshot = PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&policy),
        replication: policy,
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("g"),
            placement_epoch: 8,
            targets,
        }],
    };
    let plan = ReplicationPlan::derive(&staged("domains"), &snapshot).unwrap();
    assert_eq!(plan.sync_target_count().unwrap(), 3);
}

#[test]
fn duplicate_node_placement_is_rejected_before_any_chunk_side_effect() {
    let item = staged("duplicate-node");
    let snapshot = PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&config(2)),
        replication: config(2),
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("g"),
            placement_epoch: 8,
            targets: vec![
                target("a", remote_device("d1")),
                target("a", remote_device("d2")),
            ],
        }],
    };
    let plan = ReplicationPlan::derive(&item, &snapshot).unwrap();
    assert!(plan.validate_initiator("a").is_err());
}

#[test]
fn missing_or_duplicate_tail_ack_is_not_a_successful_receipt() {
    let item = staged("acks");
    let snapshot = PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&config(2)),
        replication: config(2),
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("g"),
            placement_epoch: 8,
            targets: vec![
                target("a", remote_device("d1")),
                target("b", remote_device("d2")),
            ],
        }],
    };
    let plan = ReplicationPlan::derive(&item, &snapshot).unwrap();
    let op = plan.selected_peer_op("a", 1).unwrap().unwrap();
    let first = ack_for(&plan.ordered_targets[0], &op, &item);
    assert!(validate_acks(&plan, &item, std::slice::from_ref(&first), "a").is_err());
    assert!(validate_acks(&plan, &item, &[first.clone(), first], "a").is_err());
}
#[test]
fn next_hop_preserves_initiator_and_stops_at_sync_prefix() {
    let mut policy = config(3);
    policy.sync_required_copies = 2;
    policy.min_distinct_nodes = 2;
    policy.local_copy = LocalCopyPolicy::NotRequired;
    let snapshot = PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&policy),
        replication: policy,
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("g"),
            placement_epoch: 8,
            targets: vec![
                target("a", remote_device("d1")),
                target("b", remote_device("d2")),
                target("c", remote_device("d3")),
            ],
        }],
    };
    let plan = ReplicationPlan::derive(&staged("hop"), &snapshot).unwrap();
    let first = plan.selected_peer_op("writer", 5).unwrap().unwrap();
    first.validate_shape().unwrap();
    let second = first.next_hop().unwrap().unwrap();
    assert_eq!(second.target.node_id, "b");
    assert_eq!(second.initiator_node_id, "writer");
    assert_eq!(second.initiator_node_epoch, 5);
    assert!(second.next_hop().unwrap().is_none());
    let mut malformed = first;
    malformed.chain_tail.clear();
    assert!(malformed.validate_shape().is_err());
}
#[test]
fn receiver_execution_requires_meta_target_and_authenticated_predecessor() {
    let item = staged("grant");
    let grant = ReplicaWriteGrant {
        requester_node_id: "b".into(),
        requester_node_epoch: 1,
        initiator_node_id: "writer".into(),
        initiator_node_epoch: 5,
        operation_id: item.operation_id.clone(),
        chunk_id: item.chunk.id.clone(),
        chunk_length: item.chunk.length,
        content_digest: item.chunk.content_digest.clone(),
        placement_revision: 7,
        placement_epoch: 8,
        replica_group_id: ReplicaGroupId::new("g"),
        target_index: 1,
        replication: ReplicationConfig {
            local_copy: LocalCopyPolicy::NotRequired,
            ..config(2)
        },
        replica_group: ReplicaGroup {
            id: ReplicaGroupId::new("g"),
            placement_epoch: 8,
            targets: vec![
                target("a", remote_device("a")),
                target("b", remote_device("b")),
            ],
        },
        expires_at_unix_ms: u64::MAX,
        fence: 7,
        token: "authenticated-meta-response".into(),
    };
    let op = ReplicaPeerOp::from_grant(&grant).unwrap();
    op.validate_sender("a").unwrap();
    assert!(op.validate_sender("writer").is_err());
    assert!(op.validate_sender("unrelated").is_err());
    let mut wrong_target = grant;
    wrong_target.requester_node_id = "other".into();
    assert!(ReplicaPeerOp::from_grant(&wrong_target).is_err());
}
struct DurableFixturePlane {
    stores: std::collections::HashMap<String, Arc<LocalChunkStore>>,
    unavailable: Mutex<Option<String>>,
}

impl ReplicaDataPlane for DurableFixturePlane {
    fn mode(&self) -> ReplicaTransferMode {
        ReplicaTransferMode::GrpcStream
    }
    fn prepare_peer(&self, op: &ReplicaPeerOp) -> Result<()> {
        op.validate_shape()
    }
    fn put_peer_replica(
        &self,
        op: &ReplicaPeerOp,
        staged: &StagedChunk,
    ) -> Result<Vec<ReplicaAck>> {
        op.validate_shape()?;
        if self.unavailable.lock().unwrap().as_deref() == Some(op.target.node_id.as_str()) {
            return Err(Error::coded(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "injected missing tail",
            ));
        }
        let local = self
            .stores
            .get(&op.target.node_id)
            .ok_or_else(|| invalid("fixture has no assigned target"))?;
        let mut result = vec![local.persist(
            staged,
            &op.target,
            op.placement_revision,
            op.placement_epoch,
        )?];
        if let Some(next) = op.next_hop()? {
            result.extend(self.put_peer_replica(&next, staged)?);
        }
        Ok(result)
    }
}

#[test]
fn durable_middle_with_missing_tail_retries_content_and_returns_complete_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let mut stores = std::collections::HashMap::new();
    let mut targets = Vec::new();
    for node in ["a", "b", "c"] {
        let local = Arc::new(LocalChunkStore::open(temp.path().join(node), node).unwrap());
        targets.push(target(node, local.device_descriptor().unwrap()));
        stores.insert(node.to_owned(), local);
    }
    let local = stores["a"].clone();
    let snapshot = Arc::new(PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&config(3)),
        replication: config(3),
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("g"),
            placement_epoch: 8,
            targets,
        }],
    });
    let plane = Arc::new(DurableFixturePlane {
        stores,
        unavailable: Mutex::new(Some("c".into())),
    });
    let store = DfsChunkSink::new_with_epoch(
        "a".into(),
        1,
        local.clone(),
        Arc::new(StaticPlacement {
            snapshot: snapshot.clone(),
        }),
        plane.clone(),
    );
    let item = staged("durable-chain");
    assert!(store.put(item.clone()).is_err());
    let mut bytes = [0; 13];
    assert_eq!(local.read_at(&item.chunk.id, 0, &mut bytes).unwrap(), 13);
    assert_eq!(
        plane.stores["b"]
            .read_at(&item.chunk.id, 0, &mut bytes)
            .unwrap(),
        13
    );
    assert!(
        plane.stores["c"]
            .read_at(&item.chunk.id, 0, &mut bytes)
            .is_err()
    );
    *plane.unavailable.lock().unwrap() = None;
    let receipt = store.put(item.clone()).unwrap();
    assert_eq!(receipt.durable_acks.len(), 3);
    for node in ["a", "b", "c"] {
        let reopened = LocalChunkStore::open(temp.path().join(node), node).unwrap();
        assert_eq!(reopened.read_at(&item.chunk.id, 0, &mut bytes).unwrap(), 13);
        assert_eq!(&bytes, b"durable-chain");
    }
}

/// Two-node chain whose hop or prepare fails for one selected Chunk.
struct SelectiveFailPlane {
    peer: Arc<LocalChunkStore>,
    fail_prepare: Option<ChunkId>,
    fail_hop: Option<ChunkId>,
    sent: Mutex<Vec<ChunkId>>,
}

impl ReplicaDataPlane for SelectiveFailPlane {
    fn mode(&self) -> ReplicaTransferMode {
        ReplicaTransferMode::GrpcStream
    }

    fn prepare_peer(&self, op: &ReplicaPeerOp) -> Result<()> {
        if self.fail_prepare.as_ref() == Some(&op.chunk_id) {
            return Err(Error::coded(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "injected prepare failure",
            ));
        }
        Ok(())
    }

    fn put_peer_replica(
        &self,
        op: &ReplicaPeerOp,
        staged: &StagedChunk,
    ) -> Result<Vec<ReplicaAck>> {
        self.sent.lock().unwrap().push(staged.chunk.id.clone());
        if self.fail_hop.as_ref() == Some(&staged.chunk.id) {
            return Err(Error::coded(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "injected hop failure",
            ));
        }
        Ok(vec![self.peer.persist(
            staged,
            &op.target,
            op.placement_revision,
            op.placement_epoch,
        )?])
    }
}

struct SelectiveFailFixture {
    _temp: tempfile::TempDir,
    local: Arc<LocalChunkStore>,
    plane: Arc<SelectiveFailPlane>,
    store: DfsChunkSink,
}

fn selective_fail_fixture(
    fail_prepare: Option<ChunkId>,
    fail_hop: Option<ChunkId>,
) -> SelectiveFailFixture {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path().join("a"), "a").unwrap());
    let peer = Arc::new(LocalChunkStore::open(temp.path().join("b"), "b").unwrap());
    let snapshot = Arc::new(PlacementSnapshot {
        revision: 7,
        local_copy: EffectiveLocalCopy::declared(&config(2)),
        replication: config(2),
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("g"),
            placement_epoch: 8,
            targets: vec![
                target("a", local.device_descriptor().unwrap()),
                target("b", peer.device_descriptor().unwrap()),
            ],
        }],
    });
    let plane = Arc::new(SelectiveFailPlane {
        peer,
        fail_prepare,
        fail_hop,
        sent: Mutex::new(Vec::new()),
    });
    let store = DfsChunkSink::new_with_epoch(
        "a".into(),
        1,
        local.clone(),
        Arc::new(StaticPlacement { snapshot }),
        plane.clone(),
    );
    SelectiveFailFixture {
        _temp: temp,
        local,
        plane,
        store,
    }
}

fn holds(store: &LocalChunkStore, item: &StagedChunk) -> bool {
    let mut bytes = vec![0; item.bytes().len()];
    store.read_at(&item.chunk.id, 0, &mut bytes).is_ok()
}

#[test]
fn characterize_write_batch_one_failed_hop_fails_whole_batch_without_rollback() {
    let items = [staged("batch-1"), staged("batch-2"), staged("batch-3")];
    let f = selective_fail_fixture(None, Some(items[1].chunk.id.clone()));

    let error = f.store.put_batch(items.to_vec()).unwrap_err();

    // One hop failure discards every receipt, including the finished Chunk.
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
    // No hop retry, and the Chunks after the failed one are never sent.
    assert_eq!(
        *f.plane.sent.lock().unwrap(),
        vec![items[0].chunk.id.clone(), items[1].chunk.id.clone()]
    );
    // Nothing rolls back: the head persisted the whole group before the
    // first hop, and the finished peer copy stays as an orphan for GC.
    for item in &items {
        assert!(holds(&f.local, item));
    }
    assert!(holds(&f.plane.peer, &items[0]));
    assert!(!holds(&f.plane.peer, &items[1]));
    assert!(!holds(&f.plane.peer, &items[2]));
}

#[test]
fn characterize_write_batch_prepare_failure_has_no_durable_side_effect() {
    let items = [staged("prepare-1"), staged("prepare-2")];
    let f = selective_fail_fixture(Some(items[1].chunk.id.clone()), None);

    let error = f.store.put_batch(items.to_vec()).unwrap_err();

    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
    assert!(f.plane.sent.lock().unwrap().is_empty());
    for item in &items {
        assert!(!holds(&f.local, item));
        assert!(!holds(&f.plane.peer, item));
    }
}

struct RepairFixtureAuthority {
    claim: crate::dfs::ReplicationClaim,
    claims: Mutex<Vec<crate::dfs::ClaimReplicationTask>>,
    reports: Mutex<Vec<crate::dfs::ReportReplicationTask>>,
    fail_claim_once: std::sync::atomic::AtomicBool,
    fail_report_once: std::sync::atomic::AtomicBool,
    wrong_report_once: std::sync::atomic::AtomicBool,
    conflict_report_once: std::sync::atomic::AtomicBool,
    superseded_report_once: std::sync::atomic::AtomicBool,
    corruption_reports: Mutex<Vec<crate::dfs::ReportChunkCorruption>>,
    fail_corruption_once: std::sync::atomic::AtomicBool,
    corruption_error_once: Mutex<Option<Error>>,
}

impl ReplicationTaskAuthority for RepairFixtureAuthority {
    fn report_corrupt_chunk(&self, request: crate::dfs::ReportChunkCorruption) -> Result<()> {
        self.corruption_reports.lock().unwrap().push(request);
        if let Some(error) = self.corruption_error_once.lock().unwrap().take() {
            return Err(error);
        }
        if self
            .fail_corruption_once
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return Err(Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "corruption ACK lost",
            ));
        }
        Ok(())
    }
    fn claim(
        &self,
        request: crate::dfs::ClaimReplicationTask,
    ) -> Result<Option<crate::dfs::ReplicationClaim>> {
        use std::sync::atomic::Ordering;
        self.claims.lock().unwrap().push(request.clone());
        if self.fail_claim_once.swap(false, Ordering::Relaxed) {
            return Err(Error::coded(
                afs_error::CLIENT_DEADLINE_EXCEEDED,
                "claim ACK lost",
            ));
        }
        let mut claim = self.claim.clone();
        claim.operation_id = request.operation_id;
        Ok(Some(claim))
    }

    fn report(
        &self,
        request: crate::dfs::ReportReplicationTask,
    ) -> Result<crate::dfs::ReplicationTask> {
        use std::sync::atomic::Ordering;
        self.reports.lock().unwrap().push(request.clone());
        if self.fail_report_once.swap(false, Ordering::Relaxed) {
            return Err(Error::coded(
                afs_error::CLIENT_CONNECTION_UNAVAILABLE,
                "report ACK lost",
            ));
        }
        if self.conflict_report_once.swap(false, Ordering::Relaxed) {
            return Err(Error::coded(
                afs_error::META_DFS_CONFLICT,
                "report CAS raced",
            ));
        }
        if self.superseded_report_once.swap(false, Ordering::Relaxed) {
            return Err(Error::coded(
                afs_error::META_DFS_REPAIR_SUPERSEDED,
                "claim replaced",
            ));
        }
        let wrong = self.wrong_report_once.swap(false, Ordering::Relaxed);
        Ok(crate::dfs::ReplicationTask {
            id: if wrong {
                crate::dfs::ReplicationTaskId::new("unrelated-task")
            } else {
                request.claim.task_id.clone()
            },
            chunk_id: request.claim.chunk.id.clone(),
            placement_epoch: request.claim.replica_group.placement_epoch,
            desired_copies: 2,
            existing_copies: Vec::new(),
            state: if request.error.is_none() {
                crate::dfs::ReplicationTaskState::Completed
            } else {
                crate::dfs::ReplicationTaskState::RetryWaiting
            },
            attempt: 1,
            next_retry_unix_ms: 0,
            last_error: request.error,
            claim: Some(Box::new(request.claim)),
        })
    }
}

fn repair_fixture(
    payload: &str,
) -> (
    tempfile::TempDir,
    Arc<LocalChunkStore>,
    Arc<RepairFixtureAuthority>,
    Arc<FakeDataPlane>,
) {
    let temp = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalChunkStore::open(temp.path(), "a").unwrap());
    let item = staged(payload);
    let source = target("a", local.device_descriptor().unwrap());
    local
        .persist_batch(std::slice::from_ref(&item), &source, 1, 1)
        .unwrap();
    let claim = crate::dfs::ReplicationClaim {
        task_id: crate::dfs::ReplicationTaskId::new("repair-task"),
        operation_id: OperationId::new("placeholder"),
        worker_node_id: "a".into(),
        worker_node_epoch: 1,
        worker_session_id: "source-session".into(),
        expires_at_unix_ms: unix_ms().unwrap() + 120_000,
        fence: 1,
        chunk: item.chunk,
        source_copy_id: crate::dfs::CopyId::new("source-copy"),
        placement_revision: 9,
        replica_group: ReplicaGroup {
            id: ReplicaGroupId::new("repair-group"),
            placement_epoch: 8,
            targets: vec![source, target("b", remote_device("disk-b"))],
        },
        // Meta's repair plan strengthens M=1 to a full N=2 transfer.
        replication: config(2),
    };
    let authority = Arc::new(RepairFixtureAuthority {
        claim,
        claims: Mutex::new(Vec::new()),
        reports: Mutex::new(Vec::new()),
        fail_claim_once: std::sync::atomic::AtomicBool::new(false),
        fail_report_once: std::sync::atomic::AtomicBool::new(false),
        wrong_report_once: std::sync::atomic::AtomicBool::new(false),
        conflict_report_once: std::sync::atomic::AtomicBool::new(false),
        superseded_report_once: std::sync::atomic::AtomicBool::new(false),
        corruption_reports: Mutex::new(Vec::new()),
        fail_corruption_once: std::sync::atomic::AtomicBool::new(false),
        corruption_error_once: Mutex::new(None),
    });
    (temp, local, authority, Arc::new(FakeDataPlane::default()))
}

fn repair_worker(
    local: Arc<LocalChunkStore>,
    authority: Arc<RepairFixtureAuthority>,
    plane: Arc<FakeDataPlane>,
) -> ReplicationWorker {
    ReplicationWorker::new(
        "a".into(),
        1,
        "source-session".into(),
        local,
        authority,
        plane,
    )
}

/// A placement whose effective rule is set apart from the declared one. No
/// production path builds this; these tests show the checks read `effective`.
fn effective_snapshot(
    copies: u16,
    declared: LocalCopyPolicy,
    effective: LocalCopyPolicy,
    targets: Vec<ReplicaTarget>,
) -> Arc<PlacementSnapshot> {
    let mut replication = config(copies);
    replication.local_copy = declared;
    Arc::new(PlacementSnapshot {
        revision: 11,
        local_copy: EffectiveLocalCopy::lowered_for_test(declared, effective),
        replication,
        replica_groups: vec![ReplicaGroup {
            id: ReplicaGroupId::new("effective"),
            placement_epoch: 12,
            targets,
        }],
    })
}

fn sink_on(
    local: Arc<LocalChunkStore>,
    snapshot: Arc<PlacementSnapshot>,
    plane: Arc<FakeDataPlane>,
) -> DfsChunkSink {
    DfsChunkSink::new_with_epoch(
        "node-a".into(),
        1,
        local,
        Arc::new(StaticPlacement { snapshot }),
        plane,
    )
}

/// N1-N4: one copy on the writer. The fast path, the capacity authority and
/// the body release all follow `effective`, whatever was declared.
#[test]
fn local_head_fast_path_capacity_and_release_follow_effective_local_copy() {
    use LocalCopyPolicy::{NotRequired, Preferred, Required};
    for (declared, effective, follows_required) in [
        (Required, Required, true),
        (Required, Preferred, false),
        (Required, NotRequired, false),
        (Preferred, Preferred, false),
        (Preferred, Required, true),
        (NotRequired, NotRequired, false),
        (NotRequired, Required, true),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let here = target_with_epoch("node-a", 1, local.device_descriptor().unwrap());
        let snapshot = effective_snapshot(1, declared, effective, vec![here]);
        assert_eq!(snapshot.is_local_fast_path(), follows_required);
        let plane = Arc::new(FakeDataPlane::default());
        let sink = sink_on(local.clone(), snapshot, plane.clone());

        let item = staged("effective-local-head");
        let stored = sink
            .put_chunks(vec![item.clone()], PutIntent::PipelineUpload)
            .unwrap();

        assert!(holds(&local, &item), "{declared:?}/{effective:?}");
        assert_eq!(
            stored[0].body_release,
            if follows_required {
                BodyRelease::ReleaseAfterUpload
            } else {
                BodyRelease::RetainUntilPublish
            },
            "{declared:?}/{effective:?}"
        );
        assert_eq!(
            sink.capacity().is_ok(),
            follows_required,
            "{declared:?}/{effective:?}"
        );
        assert!(plane.prepared.lock().unwrap().is_empty());
    }
}

/// N5 and N4: a remote head is refused exactly when `effective` is Required,
/// before any peer or local side effect.
#[test]
fn remote_head_is_judged_by_effective_local_copy() {
    use LocalCopyPolicy::{NotRequired, Preferred, Required};
    for (declared, effective, accepted) in [
        (Required, Required, false),
        (Required, Preferred, true),
        (Required, NotRequired, true),
        (Preferred, Preferred, true),
        (Preferred, Required, false),
        (NotRequired, NotRequired, true),
        (NotRequired, Required, false),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let peer = target("node-b", remote_device("remote-b"));
        let snapshot = effective_snapshot(1, declared, effective, vec![peer]);
        let plane = Arc::new(FakeDataPlane::default());
        let sink = sink_on(local.clone(), snapshot, plane.clone());

        let item = staged("effective-remote-head");
        let result = sink.put_chunks(vec![item.clone()], PutIntent::PipelineUpload);

        assert!(!holds(&local, &item), "{declared:?}/{effective:?}");
        assert_eq!(result.is_ok(), accepted, "{declared:?}/{effective:?}");
        assert_eq!(
            plane.prepared.lock().unwrap().len(),
            usize::from(accepted),
            "{declared:?}/{effective:?}"
        );
        if let Ok(stored) = result {
            assert_eq!(stored[0].body_release, BodyRelease::RetainUntilPublish);
        }
    }
}

/// N6: whether the initiator must be among the ACKs comes from the plan's
/// `AckRequirement`, which follows `effective`.
#[test]
fn ack_requirement_follows_effective_local_copy() {
    use LocalCopyPolicy::{Preferred, Required};
    let item = staged("effective-acks");
    let targets = vec![
        target("a", remote_device("d1")),
        target("b", remote_device("d2")),
    ];
    for (declared, effective, initiator_required) in
        [(Required, Preferred, false), (Preferred, Required, true)]
    {
        let snapshot = effective_snapshot(2, declared, effective, targets.clone());
        let plan = ReplicationPlan::derive(&item, &snapshot).unwrap();
        assert_eq!(plan.local_copy, snapshot.local_copy);
        assert_eq!(plan.ack_requirement.require_initiator, initiator_required);
        let op = plan.selected_peer_op("a", 1).unwrap().unwrap();
        let acks = [
            ack_for(&plan.ordered_targets[0], &op, &item),
            ack_for(&plan.ordered_targets[1], &op, &item),
        ];
        assert!(validate_acks(&plan, &item, &acks, "a").is_ok());
        assert_eq!(
            validate_acks(&plan, &item, &acks, "writer").is_ok(),
            !initiator_required
        );
    }
}

#[test]
fn plans_with_different_effective_local_copy_do_not_share_a_batch_group() {
    use LocalCopyPolicy::{Preferred, Required};
    let item = staged("effective-group");
    let targets = vec![target("a", remote_device("d1"))];
    let required = effective_snapshot(1, Required, Required, targets.clone());
    let preferred = effective_snapshot(1, Required, Preferred, targets);
    let plan = ReplicationPlan::derive(&item, &required).unwrap();
    assert!(plan.shares_batch_group(&plan.clone()));
    assert!(!plan.shares_batch_group(&ReplicationPlan::derive(&item, &preferred).unwrap()));
}

/// The only production constructor keeps effective equal to declared, so
/// strict requirements read exactly what the config declares.
#[test]
fn declared_effective_local_copy_keeps_strict_requirement_at_config_values() {
    for policy in [
        LocalCopyPolicy::Required,
        LocalCopyPolicy::Preferred,
        LocalCopyPolicy::NotRequired,
    ] {
        let mut replication = config(3);
        replication.sync_required_copies = 2;
        replication.min_distinct_failure_domains = 2;
        replication.local_copy = policy;
        let local_copy = EffectiveLocalCopy::declared(&replication);
        assert_eq!(local_copy.effective_policy(), policy);
        assert_eq!(
            AckRequirement::strict(&replication, local_copy),
            AckRequirement {
                min_acks: 2,
                min_nodes: 3,
                min_domains: 2,
                require_initiator: policy == LocalCopyPolicy::Required,
            }
        );
    }
}

/// Test-only writeback shape, proving `ChunkSink` composes: a body becomes
/// durable in a staging sink, then uploads through the inner sink, whose
/// receipt is the one published. Durable staging is what lets the composite
/// release the body even though the inner sink alone would retain it.
struct StagingSink {
    staging: Arc<dyn ChunkSink>,
    inner: Arc<dyn ChunkSink>,
}

impl ChunkSink for StagingSink {
    fn put_chunks(&self, staged: Vec<StagedChunk>, intent: PutIntent) -> Result<Vec<StoredChunk>> {
        self.staging.put_chunks(staged.clone(), intent)?;
        let mut stored = self.inner.put_chunks(staged, intent)?;
        for stored in &mut stored {
            stored.body_release = BodyRelease::ReleaseAfterUpload;
        }
        Ok(stored)
    }

    fn descriptor_reservation_bytes(&self, operation_id: &OperationId) -> Result<usize> {
        Ok(self
            .staging
            .descriptor_reservation_bytes(operation_id)?
            .max(self.inner.descriptor_reservation_bytes(operation_id)?))
    }

    fn read_uploaded_at(
        &self,
        receipt: &ChunkReceipt,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        self.staging.read_uploaded_at(receipt, offset, out)
    }

    fn capacity(&self) -> Result<FilesystemCapacity> {
        self.staging.capacity()
    }
}

struct FailingSink;

impl ChunkSink for FailingSink {
    fn put_chunks(&self, _: Vec<StagedChunk>, _: PutIntent) -> Result<Vec<StoredChunk>> {
        Err(Error::coded(
            afs_error::NODE_TRANSFER_UNAVAILABLE,
            "injected sink failure",
        ))
    }
}

/// A production local-R1 sink, usable as the staging half.
fn staging_sink(temp: &tempfile::TempDir) -> (Arc<LocalChunkStore>, Arc<dyn ChunkSink>) {
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let here = target_with_epoch("node-a", 1, local.device_descriptor().unwrap());
    let snapshot = effective_snapshot(
        1,
        LocalCopyPolicy::Required,
        LocalCopyPolicy::Required,
        vec![here],
    );
    let plane = Arc::new(FakeDataPlane::default());
    (local.clone(), Arc::new(sink_on(local, snapshot, plane)))
}

/// A production remote-only chain sink, usable as the upload half.
fn upload_sink(temp: &tempfile::TempDir) -> (Arc<FakeDataPlane>, Arc<dyn ChunkSink>) {
    let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
    let peer = target("node-b", remote_device("remote-b"));
    let snapshot = effective_snapshot(
        1,
        LocalCopyPolicy::Preferred,
        LocalCopyPolicy::Preferred,
        vec![peer],
    );
    let plane = Arc::new(FakeDataPlane::default());
    (
        plane.clone(),
        Arc::new(sink_on(local, snapshot, plane.clone())),
    )
}

#[test]
fn composite_sink_stages_then_uploads_through_the_trait() {
    let (staging_dir, upload_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (staged_on, staging) = staging_sink(&staging_dir);
    let (plane, inner) = upload_sink(&upload_dir);
    let item = staged("composite-payload");

    // On its own the upload half keeps the body until publication.
    let alone = inner
        .put_chunks(vec![staged("composite-alone")], PutIntent::PipelineUpload)
        .unwrap();
    assert_eq!(alone[0].body_release, BodyRelease::RetainUntilPublish);

    let sink: Arc<dyn ChunkSink> = Arc::new(StagingSink { staging, inner });
    let stored = sink
        .put_chunks(vec![item.clone()], PutIntent::PipelineUpload)
        .unwrap();

    // The published receipt is the upload's; the body is durable in staging.
    assert_eq!(stored[0].receipt.durable_acks.len(), 1);
    assert_eq!(stored[0].receipt.durable_acks[0].node_id, "node-b");
    assert_eq!(plane.prepared.lock().unwrap().len(), 2);
    assert_eq!(stored[0].body_release, BodyRelease::ReleaseAfterUpload);
    assert!(holds(&staged_on, &item));
    let mut output = vec![0; item.bytes().len()];
    assert_eq!(
        sink.read_uploaded_at(&stored[0].receipt, 0, &mut output)
            .unwrap(),
        output.len()
    );
    assert_eq!(output, item.bytes());
    // The rest of the trait goes through the composition too.
    assert!(sink.capacity().is_ok());
    assert!(
        sink.descriptor_reservation_bytes(&item.operation_id)
            .unwrap()
            > 0
    );
}

#[test]
fn composite_sink_failures_surface_and_keep_put_chunks_batch_semantics() {
    let (staging_dir, upload_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let item = staged("composite-failure");

    // Upload fails: the whole put fails, and the staged copy is not rolled
    // back, as with any put_chunks that fails after a durable step.
    let (staged_on, staging) = staging_sink(&staging_dir);
    let sink: Arc<dyn ChunkSink> = Arc::new(StagingSink {
        staging,
        inner: Arc::new(FailingSink),
    });
    let error = sink
        .put_chunks(vec![item.clone()], PutIntent::PipelineUpload)
        .unwrap_err();
    assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
    assert!(holds(&staged_on, &item));

    // Staging fails: nothing reaches the upload half.
    let (plane, inner) = upload_sink(&upload_dir);
    let sink: Arc<dyn ChunkSink> = Arc::new(StagingSink {
        staging: Arc::new(FailingSink),
        inner,
    });
    assert!(
        sink.put_chunks(vec![item], PutIntent::PipelineUpload)
            .is_err()
    );
    assert!(plane.prepared.lock().unwrap().is_empty());
}

#[test]
fn repair_claim_survives_common_grpc_and_rdma_replica_header() {
    let (_temp, _local, authority, _plane) = repair_fixture("repair-payload");
    let claim = authority.claim.clone();
    let staged = StagedChunk::new(claim.operation_id.clone(), b"repair-payload".to_vec());
    let plan = ReplicationPlan {
        chunk_id: claim.chunk.id.clone(),
        placement_revision: claim.placement_revision,
        placement_epoch: claim.replica_group.placement_epoch,
        replica_group_id: claim.replica_group.id.clone(),
        config: claim.replication.clone(),
        local_copy: EffectiveLocalCopy::declared(&claim.replication),
        ack_requirement: AckRequirement::strict(
            &claim.replication,
            EffectiveLocalCopy::declared(&claim.replication),
        ),
        ordered_targets: claim.replica_group.targets.clone(),
    };
    let mut op = plan.selected_peer_op("a", 1).unwrap().unwrap();
    op.repair_claim = Some(claim.clone());
    let header = crate::node::rpc::peer::replica_header(&op, &staged).unwrap();
    assert_eq!(header.operation_id, claim.operation_id.0);
    assert_eq!(
        crate::node::rpc::meta::domain_replication_claim(*header.repair_claim.unwrap()).unwrap(),
        claim
    );
    op.repair_claim.as_mut().unwrap().worker_node_epoch += 1;
    assert!(crate::node::rpc::peer::replica_header(&op, &staged).is_err());
}

#[test]
fn crc32c_replica_header_and_repair_preserve_birth_identity() {
    let (_temp, local, mut authority, plane) = repair_fixture("crc-repair");
    let item = StagedChunk::new_crc32c(OperationId::new("original-write"), b"crc-repair".to_vec())
        .unwrap();
    let claim = &mut Arc::get_mut(&mut authority).unwrap().claim;
    claim.chunk = item.chunk.clone();
    local
        .persist_batch(
            std::slice::from_ref(&item),
            &claim.replica_group.targets[0],
            claim.placement_revision,
            claim.replica_group.placement_epoch,
        )
        .unwrap();
    let plan = ReplicationPlan {
        chunk_id: item.chunk.id.clone(),
        placement_revision: claim.placement_revision,
        placement_epoch: claim.replica_group.placement_epoch,
        replica_group_id: claim.replica_group.id.clone(),
        config: claim.replication.clone(),
        local_copy: EffectiveLocalCopy::declared(&claim.replication),
        ack_requirement: AckRequirement::strict(
            &claim.replication,
            EffectiveLocalCopy::declared(&claim.replication),
        ),
        ordered_targets: claim.replica_group.targets.clone(),
    };
    let op = plan.selected_peer_op("a", 1).unwrap().unwrap();
    let header = crate::node::rpc::peer::replica_header(&op, &item).unwrap();
    assert_eq!(header.chunk_id, item.chunk.id.0);
    assert_eq!(
        header.content_digest_algorithm,
        afs_protocol::node_data::DfsDigestAlgorithm::Crc32cSegmentsV1 as i32
    );
    let worker = repair_worker(local, authority.clone(), plane);
    assert_eq!(
        worker.run_once().unwrap().unwrap().state,
        crate::dfs::ReplicationTaskState::Completed
    );
    let reports = authority.reports.lock().unwrap();
    assert_eq!(reports[0].claim.chunk, item.chunk);
    assert_eq!(reports[0].durable_acks.len(), 2);
    assert!(reports[0].durable_acks.iter().all(
        |ack| ack.chunk_id == item.chunk.id && ack.verified_digest == item.chunk.content_digest
    ));
}

#[test]
fn repair_worker_preserves_claim_identity_after_unknown_ack() {
    use std::sync::atomic::Ordering;
    let (_temp, local, authority, plane) = repair_fixture("repair-payload");
    authority.fail_claim_once.store(true, Ordering::Relaxed);
    let worker = repair_worker(local, authority.clone(), plane.clone());
    assert!(worker.run_once().is_err());
    assert!(plane.prepared.lock().unwrap().is_empty());
    assert_eq!(
        worker.run_once().unwrap().unwrap().state,
        crate::dfs::ReplicationTaskState::Completed
    );
    let claims = authority.claims.lock().unwrap();
    assert_eq!(claims.len(), 2);
    assert_eq!(claims[0], claims[1]);
    let reports = authority.reports.lock().unwrap();
    assert_eq!(reports[0].durable_acks.len(), 2);
    assert_eq!(reports[0].claim.operation_id, claims[0].operation_id);
    assert_eq!(plane.prepared.lock().unwrap().len(), 1);
}

#[test]
fn repair_worker_replays_report_without_retransferring_chunk() {
    use std::sync::atomic::Ordering;
    let (_temp, local, authority, plane) = repair_fixture("repair-payload");
    authority.fail_report_once.store(true, Ordering::Relaxed);
    let worker = repair_worker(local, authority.clone(), plane.clone());
    assert!(worker.run_once().is_err());
    assert_eq!(
        worker.run_once().unwrap().unwrap().state,
        crate::dfs::ReplicationTaskState::Completed
    );
    let reports = authority.reports.lock().unwrap();
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0], reports[1]);
    assert_eq!(authority.claims.lock().unwrap().len(), 1);
    assert_eq!(plane.prepared.lock().unwrap().len(), 1);
}

#[test]
fn repair_worker_retains_report_after_unrelated_reply() {
    use std::sync::atomic::Ordering;
    let (_temp, local, authority, plane) = repair_fixture("repair-payload");
    authority.wrong_report_once.store(true, Ordering::Relaxed);
    let worker = repair_worker(local, authority.clone(), plane.clone());
    assert!(worker.run_once().is_err());
    assert_eq!(
        worker.run_once().unwrap().unwrap().state,
        crate::dfs::ReplicationTaskState::Completed
    );
    let reports = authority.reports.lock().unwrap();
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0], reports[1]);
    assert_eq!(authority.claims.lock().unwrap().len(), 1);
    assert_eq!(plane.prepared.lock().unwrap().len(), 1);
}

#[test]
fn repair_worker_retains_exact_report_after_generic_cas_conflict() {
    use std::sync::atomic::Ordering;
    let (_temp, local, authority, plane) = repair_fixture("repair-payload");
    authority
        .conflict_report_once
        .store(true, Ordering::Relaxed);
    let worker = repair_worker(local, authority.clone(), plane.clone());
    assert!(worker.run_once().is_err());
    assert_eq!(
        worker.run_once().unwrap().unwrap().state,
        crate::dfs::ReplicationTaskState::Completed
    );
    let reports = authority.reports.lock().unwrap();
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0], reports[1]);
    assert_eq!(authority.claims.lock().unwrap().len(), 1);
    assert_eq!(plane.prepared.lock().unwrap().len(), 1);
}

#[test]
fn repair_worker_advances_only_after_specific_superseded_proof() {
    use std::sync::atomic::Ordering;
    let (_temp, local, authority, plane) = repair_fixture("repair-payload");
    authority
        .superseded_report_once
        .store(true, Ordering::Relaxed);
    let worker = repair_worker(local, authority.clone(), plane.clone());
    assert_eq!(
        worker.run_once().unwrap_err().code(),
        afs_error::META_DFS_REPAIR_SUPERSEDED
    );
    assert_eq!(
        worker.run_once().unwrap().unwrap().state,
        crate::dfs::ReplicationTaskState::Completed
    );
    let claims = authority.claims.lock().unwrap();
    assert_eq!(claims.len(), 2);
    assert_ne!(claims[0].operation_id, claims[1].operation_id);
    let reports = authority.reports.lock().unwrap();
    assert_ne!(reports[0].claim.operation_id, reports[1].claim.operation_id);
    assert_eq!(plane.prepared.lock().unwrap().len(), 2);
}

#[test]
fn repair_worker_reports_missing_source_without_peer_bytes() {
    let (temp, local, authority, plane) = repair_fixture("repair-payload");
    std::fs::remove_file(temp.path().join("chunks").join(&authority.claim.chunk.id.0)).unwrap();
    let worker = repair_worker(local, authority.clone(), plane.clone());
    assert_eq!(
        worker.run_once().unwrap().unwrap().state,
        crate::dfs::ReplicationTaskState::RetryWaiting
    );
    let reports = authority.reports.lock().unwrap();
    assert!(reports[0].source_invalid);
    assert!(reports[0].error.is_some());
    assert!(reports[0].durable_acks.is_empty());
    assert!(plane.prepared.lock().unwrap().is_empty());
}

#[test]
fn corruption_worker_retains_exact_report_and_does_not_resend_after_ack() {
    let (temp, local, authority, plane) = repair_fixture("repair-payload");
    let path = temp.path().join("chunks").join(&authority.claim.chunk.id.0);
    std::fs::write(path, b"repair-payloXd").unwrap();
    assert!(local.open_verified(&authority.claim.chunk.id).is_err());
    authority
        .fail_corruption_once
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let worker = repair_worker(local, authority.clone(), plane.clone());
    assert!(worker.run_once().is_err());
    assert!(worker.run_once().unwrap().is_none());
    let reports = authority.corruption_reports.lock().unwrap();
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0], reports[1]);
    drop(reports);
    // The next tick may claim ordinary repair but must not issue another
    // corruption operation for the acknowledged quarantine revision.
    worker.run_once().unwrap();
    assert_eq!(authority.corruption_reports.lock().unwrap().len(), 2);
    assert!(plane.prepared.lock().unwrap().is_empty());
}

#[test]
fn corruption_worker_retries_cas_with_exact_identity() {
    let (temp, local, authority, plane) = repair_fixture("repair-payload");
    std::fs::write(
        temp.path().join("chunks").join(&authority.claim.chunk.id.0),
        b"repair-payloXd",
    )
    .unwrap();
    assert!(local.open_verified(&authority.claim.chunk.id).is_err());
    *authority.corruption_error_once.lock().unwrap() = Some(Error::coded(
        afs_error::META_DFS_CONFLICT,
        "observed copy changed",
    ));
    let worker = repair_worker(local, authority.clone(), plane);
    assert_eq!(
        worker.run_once().unwrap_err().code(),
        afs_error::META_DFS_CONFLICT
    );
    assert!(worker.run_once().unwrap().is_none());
    let reports = authority.corruption_reports.lock().unwrap();
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0], reports[1]);
}

#[test]
fn corruption_worker_keeps_quarantine_but_does_not_wedge_on_definitive_rejection() {
    let (temp, local, authority, plane) = repair_fixture("repair-payload");
    std::fs::write(
        temp.path().join("chunks").join(&authority.claim.chunk.id.0),
        b"repair-payloXd",
    )
    .unwrap();
    assert!(local.open_verified(&authority.claim.chunk.id).is_err());
    *authority.corruption_error_once.lock().unwrap() = Some(invalid("unknown physical copy"));
    let worker = repair_worker(local.clone(), authority.clone(), plane);
    assert_eq!(
        worker.run_once().unwrap_err().kind(),
        afs_error::ErrorKind::InvalidArgument
    );
    assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
    assert!(worker.run_once().unwrap().is_some());
    assert_eq!(authority.corruption_reports.lock().unwrap().len(), 1);
    assert_eq!(authority.claims.lock().unwrap().len(), 1);
}

#[test]
fn repair_worker_reports_corrupt_source_without_peer_bytes() {
    let (temp, local, authority, plane) = repair_fixture("repair-payload");
    std::fs::write(
        temp.path().join("chunks").join(&authority.claim.chunk.id.0),
        b"repair-payloXd",
    )
    .unwrap();
    let worker = repair_worker(local, authority.clone(), plane.clone());
    assert_eq!(
        worker.run_once().unwrap().unwrap().state,
        crate::dfs::ReplicationTaskState::RetryWaiting
    );
    let reports = authority.reports.lock().unwrap();
    assert!(reports[0].source_invalid);
    assert!(reports[0].error.is_some());
    assert!(reports[0].durable_acks.is_empty());
    assert!(plane.prepared.lock().unwrap().is_empty());
}

#[test]
fn repair_worker_refuses_expired_claim_without_transfer_or_report() {
    let (_temp, local, mut authority, plane) = repair_fixture("repair-payload");
    Arc::get_mut(&mut authority)
        .unwrap()
        .claim
        .expires_at_unix_ms = 1;
    let worker = repair_worker(local, authority.clone(), plane.clone());
    assert!(worker.run_once().unwrap().is_none());
    assert!(authority.reports.lock().unwrap().is_empty());
    assert!(plane.prepared.lock().unwrap().is_empty());
}

#[test]
fn repair_worker_refuses_oversized_chunk_before_allocating_or_transfer() {
    let (_temp, local, mut authority, plane) = repair_fixture("repair-payload");
    Arc::get_mut(&mut authority).unwrap().claim.chunk.length =
        crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64 + 1;
    let worker = repair_worker(local, authority.clone(), plane.clone());
    assert_eq!(
        worker.run_once().unwrap().unwrap().state,
        crate::dfs::ReplicationTaskState::RetryWaiting
    );
    let reports = authority.reports.lock().unwrap();
    assert!(!reports[0].source_invalid);
    assert!(reports[0].durable_acks.is_empty());
    assert!(plane.prepared.lock().unwrap().is_empty());
}

/// Counts allocations only while a measured call runs; a relaxed load
/// otherwise, so the rest of the test binary pays nothing observable.
struct CountingAlloc;
static COUNTING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static ALLOCS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static ALLOC_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn note_alloc(bytes: usize) {
    use std::sync::atomic::Ordering::Relaxed;
    if COUNTING.load(Relaxed) {
        ALLOCS.fetch_add(1, Relaxed);
        ALLOC_BYTES.fetch_add(bytes as u64, Relaxed);
    }
}

// SAFETY: every method forwards unchanged to `System`, which upholds the
// `GlobalAlloc` contract; the counters neither allocate nor touch the block.
unsafe impl std::alloc::GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        note_alloc(layout.size());
        // SAFETY: the caller's layout contract is forwarded as is.
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn dealloc(&self, block: *mut u8, layout: std::alloc::Layout) {
        // SAFETY: `block` came from `System` with this layout.
        unsafe { std::alloc::System.dealloc(block, layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        note_alloc(layout.size());
        // SAFETY: the caller's layout contract is forwarded as is.
        unsafe { std::alloc::System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(
        &self,
        block: *mut u8,
        layout: std::alloc::Layout,
        new_size: usize,
    ) -> *mut u8 {
        note_alloc(new_size);
        // SAFETY: `block` came from `System` with this layout.
        unsafe { std::alloc::System.realloc(block, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// Elapsed ns, allocation count and allocated bytes of `call`. The result
/// is returned so that dropping it stays outside the measured region.
fn timed<R>(call: impl FnOnce() -> R) -> (R, u64, u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    ALLOCS.store(0, Relaxed);
    ALLOC_BYTES.store(0, Relaxed);
    COUNTING.store(true, Relaxed);
    let started = std::time::Instant::now();
    let result = call();
    let elapsed = started.elapsed().as_nanos() as u64;
    COUNTING.store(false, Relaxed);
    (
        result,
        elapsed,
        ALLOCS.load(Relaxed),
        ALLOC_BYTES.load(Relaxed),
    )
}

#[derive(Clone, Copy)]
enum BenchPlacement {
    /// `is_local_fast_path`: one Required copy on this node.
    LocalFastRequired,
    /// One Preferred copy on this node: replica chain, local persist.
    LocalPreferred,
    /// Two Required copies: local persist plus the fake peer.
    ReplicatedRequired,
    /// Two Preferred copies: local persist plus the fake peer.
    ReplicatedPreferred,
    /// One Preferred remote copy: no disk at all, the pure in-memory path.
    RemoteOnlyPreferred,
}

impl BenchPlacement {
    fn name(self) -> &'static str {
        match self {
            Self::LocalFastRequired => "local_fast_required",
            Self::LocalPreferred => "local_preferred",
            Self::ReplicatedRequired => "replicated_required",
            Self::ReplicatedPreferred => "replicated_preferred",
            Self::RemoteOnlyPreferred => "remote_only_preferred",
        }
    }

    fn store(self, local: Arc<LocalChunkStore>) -> DfsChunkSink {
        let device = local.device_descriptor().unwrap();
        let here = target_with_epoch("node-a", 1, device);
        let peer = target("node-b", remote_device("remote-b"));
        let (copies, policy, targets) = match self {
            Self::LocalFastRequired => (1, LocalCopyPolicy::Required, vec![here]),
            Self::LocalPreferred => (1, LocalCopyPolicy::Preferred, vec![here]),
            Self::ReplicatedRequired => (2, LocalCopyPolicy::Required, vec![here, peer]),
            Self::ReplicatedPreferred => (2, LocalCopyPolicy::Preferred, vec![here, peer]),
            Self::RemoteOnlyPreferred => (1, LocalCopyPolicy::Preferred, vec![peer]),
        };
        let mut replication = config(copies);
        replication.local_copy = policy;
        let snapshot = Arc::new(PlacementSnapshot {
            revision: 11,
            local_copy: EffectiveLocalCopy::declared(&replication),
            replication,
            replica_groups: vec![ReplicaGroup {
                id: ReplicaGroupId::new("bench"),
                placement_epoch: 12,
                targets,
            }],
        });
        DfsChunkSink::new_with_epoch(
            "node-a".into(),
            1,
            local,
            Arc::new(StaticPlacement { snapshot }),
            Arc::new(FakeDataPlane::default()),
        )
    }
}

fn percentile(sorted: &[u64], per_mille: usize) -> u64 {
    sorted[(sorted.len() * per_mille / 1000).min(sorted.len() - 1)]
}

/// Per-call cost of the production single-chunk put (`write_pipeline_ops.rs`).
/// Each call persists a distinct CRC32C object, as native uploads do, and
/// each cell gets a fresh store so the catalog does not grow across cells.
///
/// Alone in its process, so only this work is counted, and run on a native
/// filesystem (`TMPDIR=/workspace/ut`):
/// `chunk_store_put_microbench --ignored --nocapture --test-threads=1`.
#[test]
#[ignore = "性能测量，不在门禁里跑"]
fn chunk_store_put_microbench() {
    const SIZES: [(&str, usize); 3] = [
        ("4KiB", 4 * 1024),
        ("1MiB", 1024 * 1024),
        ("4MiB", 4 * 1024 * 1024),
    ];
    const WARMUP: usize = 10;
    let iterations: usize = std::env::var("AFS_BENCH_ITERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1000);
    let placements = [
        BenchPlacement::LocalFastRequired,
        BenchPlacement::LocalPreferred,
        BenchPlacement::ReplicatedRequired,
        BenchPlacement::ReplicatedPreferred,
        BenchPlacement::RemoteOnlyPreferred,
    ];
    for placement in placements {
        for (size_name, size) in SIZES {
            let body = vec![0x5a_u8; size];
            for commit in [true, false] {
                let temp = tempfile::tempdir().unwrap();
                let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
                let store = placement.store(local);
                let mut nanos = Vec::with_capacity(iterations);
                let mut allocs = Vec::with_capacity(iterations);
                let mut bytes = 0_u64;
                for iteration in 0..iterations + WARMUP {
                    let operation = OperationId::new(format!("bench-{iteration}"));
                    let staged = StagedChunk::new_crc32c(operation, body.clone()).unwrap();
                    let (count, elapsed, call_allocs, call_bytes) = if commit {
                        let (result, elapsed, call_allocs, call_bytes) =
                            timed(|| store.put_chunks(vec![staged], PutIntent::Commit));
                        (result.unwrap().len(), elapsed, call_allocs, call_bytes)
                    } else {
                        let (result, elapsed, call_allocs, call_bytes) =
                            timed(|| store.put_chunks(vec![staged], PutIntent::PipelineUpload));
                        (result.unwrap().len(), elapsed, call_allocs, call_bytes)
                    };
                    assert_eq!(count, 1);
                    if iteration >= WARMUP {
                        nanos.push(elapsed);
                        allocs.push(call_allocs);
                        bytes += call_bytes;
                    }
                }
                nanos.sort_unstable();
                allocs.sort_unstable();
                println!(
                    "BENCH placement={} size={} intent={} n={} p50_ns={} p99_ns={} \
                     allocs_p50={} allocs_max={} alloc_bytes_mean={}",
                    placement.name(),
                    size_name,
                    if commit { "commit" } else { "native_upload" },
                    iterations,
                    percentile(&nanos, 500),
                    percentile(&nanos, 990),
                    percentile(&allocs, 500),
                    allocs[allocs.len() - 1],
                    bytes / iterations as u64,
                );
            }
        }
    }
}
