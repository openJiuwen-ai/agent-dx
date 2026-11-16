//! R12 degraded writes on Meta: placement with too few live Nodes, the commit
//! of a receipt below the synchronous count, and the immutable contract.

use super::*;
use crate::dfs::{
    ChunkReceipt, CommitMetadataDelta, Extent, LayoutRootId, ReplicaFailurePolicy,
    StorageDeviceDescriptor,
};
use crate::meta::store::{NodeSessionLease, Store, journal::MemoryLog};
use crate::node::chunk::StagedChunk;

fn two_copies() -> ReplicationConfig {
    ReplicationConfig {
        desired_copies: 2,
        sync_required_copies: 2,
        min_distinct_nodes: 2,
        min_distinct_failure_domains: 1,
        local_copy: LocalCopyPolicy::Required,
    }
}

fn three_copies() -> ReplicationConfig {
    ReplicationConfig {
        desired_copies: 3,
        sync_required_copies: 3,
        min_distinct_nodes: 3,
        min_distinct_failure_domains: 1,
        local_copy: LocalCopyPolicy::Required,
    }
}

fn degrade(min_degraded_copies: u16) -> ReplicaFailureConfig {
    ReplicaFailureConfig {
        policy: ReplicaFailurePolicy::Degrade,
        min_degraded_copies,
    }
}

async fn register(store: &Store, node: &str, session: &str, ttl: std::time::Duration) {
    store
        .register_node_session(
            RequestKey::new(node, format!("register-{session}")),
            NodeSessionLease {
                node_id: node.into(),
                session_id: session.into(),
                grpc_addr: format!("http://{node}:1"),
                data_addr: format!("http://{node}:2"),
                rest_addr: format!("http://{node}:3"),
                nfs_addr: String::new(),
                ownerfs_enabled: false,
                storage_devices: vec![StorageDeviceDescriptor {
                    device_id: format!("{node}-dev"),
                    device_epoch: 1,
                    catalog_revision: 0,
                    failure_domain: format!("{node}-fd"),
                }],
                lease_ttl: ttl,
            },
        )
        .await
        .unwrap();
}

const LIVE: std::time::Duration = std::time::Duration::from_secs(60);
/// Long enough to build a group with the Node, short enough to wait out.
const BRIEF: std::time::Duration = std::time::Duration::from_millis(300);

struct Fixture {
    store: Arc<Store>,
    service: DfsService,
    commit: CommitFileVersion,
}

impl Fixture {
    /// `writer` plus `peers` (each live for `peer_ttl`), the file created.
    async fn new(
        replication: ReplicationConfig,
        failure: ReplicaFailureConfig,
        peers: &[&str],
        peer_ttl: std::time::Duration,
    ) -> Self {
        let store = Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        );
        register(&store, "writer", "writer-s1", LIVE).await;
        for peer in peers {
            register(&store, peer, &format!("{peer}-s1"), peer_ttl).await;
        }
        let service = DfsService::with_replication_config(store.clone(), replication)
            .with_replica_failure_config(failure);
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
                owner_session_id: "writer-s1".into(),
                operation_id: OperationId::new("create"),
                namespace_id: NamespaceId::new("default"),
                parent_inode_id: root.inode_id,
                name: b"degraded.bin".to_vec(),
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
        let operation_id = OperationId::new("commit");
        let chunk = StagedChunk::new(operation_id.clone(), b"degraded body".to_vec()).chunk;
        let layout = LayoutRoot {
            id: LayoutRootId::new("layout"),
            file_length: chunk.length,
            inline_extents: vec![Extent {
                file_offset: 0,
                length: chunk.length,
                chunk_id: chunk.id.clone(),
                chunk_offset: 0,
            }],
        };
        let commit = CommitFileVersion {
            gc_fence: None,
            operation_id,
            inode_id: inode.inode_id.clone(),
            write_lease,
            expected_inode_revision: inode.revision,
            expected_head_version: inode.head_version.clone(),
            file_version: FileVersion {
                id: FileVersionId::new("version"),
                inode_id: inode.inode_id,
                parent_version: inode.head_version,
                length: chunk.length,
                layout_root: layout.id.clone(),
                created_at_unix_ms: 1,
            },
            layout_root: layout,
            chunk_receipts: Vec::new(),
            clone_source: None,
            metadata_delta: CommitMetadataDelta {
                mode: CommitMetadataMode::DataOnly,
                mtime_unix_ms: None,
                ctime_unix_ms: None,
                kill_suidgid: false,
            },
        };
        Self {
            store,
            service,
            commit,
        }
    }

    async fn epoch(&self, node: &str) -> u64 {
        match self
            .store
            .read(MetaRead::CurrentNodeSession {
                node_id: node.into(),
            })
            .await
            .unwrap()
            .entity
        {
            Some(MetaEntity::NodeSession(session)) => session.lease_epoch,
            _ => panic!("{node} has no session"),
        }
    }

    /// One receipt for the writer's group, ACKed by `acked` only.
    async fn receipt_from(&mut self, acked: &[&str]) {
        let placement = self
            .service
            .placement_snapshot("writer".into())
            .await
            .unwrap();
        let group = placement.replica_groups[0].clone();
        let chunk =
            StagedChunk::new(self.commit.operation_id.clone(), b"degraded body".to_vec()).chunk;
        let mut acks = Vec::new();
        for node in acked {
            acks.push(ReplicaAck {
                operation_id: self.commit.operation_id.clone(),
                chunk_id: chunk.id.clone(),
                placement_revision: placement.revision,
                placement_epoch: group.placement_epoch,
                node_id: (*node).into(),
                node_epoch: self.epoch(node).await,
                device_id: format!("{node}-dev"),
                device_epoch: 1,
                catalog_revision: 1,
                persisted_bytes: chunk.length,
                verified_digest: chunk.content_digest.clone(),
            });
        }
        self.commit.chunk_receipts = vec![ChunkReceipt {
            operation_id: self.commit.operation_id.clone(),
            chunk,
            placement_revision: placement.revision,
            placement_epoch: group.placement_epoch,
            replica_group_id: group.id,
            durable_acks: acks,
            external_commit: None,
        }];
    }

    async fn commit(&self) -> Result<()> {
        self.service
            .commit_file_version("writer".into(), self.commit.clone())
            .await
            .map(|_| ())
    }

    async fn placement(&self) -> PlacementRecord {
        let chunk = self.commit.chunk_receipts[0].chunk.id.clone();
        match self
            .store
            .read(MetaRead::DfsPlacement(chunk))
            .await
            .unwrap()
            .entity
        {
            Some(MetaEntity::DfsPlacement(placement)) => placement,
            _ => panic!("placement is missing"),
        }
    }

    async fn debt(&self) -> Option<ReplicationTask> {
        let chunk = self.commit.chunk_receipts[0].chunk.id.clone();
        match self
            .store
            .read(MetaRead::DfsReplicationTask(repair_task_id(&chunk)))
            .await
            .unwrap()
            .entity
        {
            Some(MetaEntity::DfsReplicationTask(task)) => Some(task),
            _ => None,
        }
    }
}

#[tokio::test]
async fn degrade_places_on_the_live_writer_alone_and_strict_refuses() {
    for (failure, groups) in [
        (degrade(1), Some(1)),
        (ReplicaFailureConfig::strict(), None),
    ] {
        let f = Fixture::new(two_copies(), failure, &[], LIVE).await;
        let placement = f.service.placement_snapshot("writer".into()).await;
        match groups {
            Some(_) => {
                let placement = placement.unwrap();
                assert_eq!(placement.failure, failure);
                assert_eq!(placement.replica_groups.len(), 1);
                let targets = &placement.replica_groups[0].targets;
                assert_eq!(targets.len(), 1);
                assert_eq!(targets[0].node_id, "writer");
            }
            None => assert_eq!(placement.unwrap_err().code(), afs_error::META_DFS_CONFLICT),
        }
    }
}

#[tokio::test]
async fn a_complete_group_is_still_preferred_when_every_node_is_live() {
    let f = Fixture::new(two_copies(), degrade(1), &["peer"], LIVE).await;
    let placement = f.service.placement_snapshot("writer".into()).await.unwrap();
    assert!(
        placement
            .replica_groups
            .iter()
            .all(|group| group.targets.len() == 2)
    );
}

/// The degraded-write invariant: the commit keeps at least
/// `min_degraded_copies` reliable copies and records the missing ones as debt
/// in the same transaction, even after the failed peer's group is gone.
#[tokio::test]
async fn a_degraded_receipt_commits_with_its_debt_after_the_failed_peers_group_expires() {
    let mut f = Fixture::new(two_copies(), degrade(1), &["peer"], BRIEF).await;
    f.receipt_from(&["writer"]).await;
    tokio::time::sleep(BRIEF * 2).await;
    assert!(
        f.service
            .placement_snapshot("writer".into())
            .await
            .unwrap()
            .replica_groups
            .iter()
            .all(|group| group.id != f.commit.chunk_receipts[0].replica_group_id),
        "the receipt's group must be gone for this case"
    );

    f.commit().await.unwrap();

    let placement = f.placement().await;
    assert_eq!(placement.health, PlacementHealth::UnderReplicated);
    assert_eq!(placement.copies.len(), 1);
    let debt = f.debt().await.expect("degraded commit records its debt");
    assert_eq!(debt.state, ReplicationTaskState::Pending);
    assert_eq!(debt.desired_copies, 2);
    assert_eq!(debt.existing_copies, placement.copies);
}

#[tokio::test]
async fn strict_refuses_the_same_receipt() {
    let mut f = Fixture::new(
        two_copies(),
        ReplicaFailureConfig::strict(),
        &["peer"],
        LIVE,
    )
    .await;
    f.receipt_from(&["writer"]).await;
    let error = f.commit().await.unwrap_err();
    assert_eq!(
        error.code(),
        afs_error::META_CATALOG_INVALID_REQUEST,
        "{error}"
    );
    assert!(f.debt().await.is_none());
}

#[tokio::test]
async fn below_the_degraded_floor_or_without_the_local_copy_fails() {
    let mut f = Fixture::new(three_copies(), degrade(2), &["b", "c"], LIVE).await;
    f.receipt_from(&["writer"]).await;
    assert!(
        f.commit().await.is_err(),
        "one copy is below a floor of two"
    );
    f.receipt_from(&["b", "c"]).await;
    assert!(
        f.commit().await.is_err(),
        "the writer's local copy is required"
    );
    f.receipt_from(&["writer", "b"]).await;
    f.commit().await.unwrap();
    assert_eq!(f.placement().await.health, PlacementHealth::UnderReplicated);
    assert!(f.debt().await.is_some());
}

#[tokio::test]
async fn the_replica_failure_contract_is_immutable() {
    let f = Fixture::new(two_copies(), degrade(1), &["peer"], LIVE).await;
    for other in [ReplicaFailureConfig::strict(), degrade(2)] {
        let restarted = DfsService::with_replication_config(f.store.clone(), two_copies())
            .with_replica_failure_config(other);
        let error = restarted.initialize_replication_config().await.unwrap_err();
        assert_eq!(error.code(), afs_error::META_DFS_CONFLICT, "{other:?}");
    }
    let same = DfsService::with_replication_config(f.store.clone(), two_copies())
        .with_replica_failure_config(degrade(1));
    same.initialize_replication_config().await.unwrap();
}

#[tokio::test]
async fn a_floor_above_the_synchronous_count_is_refused() {
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    let service = DfsService::with_replication_config(store, two_copies())
        .with_replica_failure_config(degrade(3));
    assert!(service.initialize_replication_config().await.is_err());
}
