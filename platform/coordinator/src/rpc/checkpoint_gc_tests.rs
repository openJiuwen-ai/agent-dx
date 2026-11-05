//! Local destructive RPCs must exclude new references until their ACK or cancellation.
use super::*;
use adx_core::{snapshots::Snapshot, Assignment, CheckpointArtifact};

fn spec(id: &str) -> EnvironmentSpec {
    serde_json::from_value(serde_json::json!({
        "id":id, "tenant_id":"tenant", "image":"image", "runtime_class":"runsc",
        "priority":0, "resources":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0}
    }))
    .unwrap()
}

async fn running(session: &Session, id: &str) -> EnvironmentRecord {
    let spec = spec(id);
    let assignment = Assignment {
        environment_id: id.into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec.clone(), assignment.clone())
        .await
        .unwrap();
    let record: EnvironmentRecord = serde_json::from_value(serde_json::json!({
        "spec":spec, "assignment":assignment, "state":EnvironmentState::Running, "revision":2,
        "runtime":{"id":format!("{id}-1"),"ip":"10.0.0.2"}, "resources_held":true,
        "restart_attempts":0,"restart_pending":false,"execution_artifacts":[]
    }))
    .unwrap();
    session.commit(record.clone()).await.unwrap();
    record
}

async fn local_collection_excludes_new_references(snapshot: bool, duplicate_location: bool) {
    let redis = cancellation_test_redis::Redis::new().await;
    let store = crate::storage::RedisStore::connect(&redis.url, "test", Duration::from_secs(5))
        .await
        .unwrap();
    let session = store.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        Peers::network(),
        RpcClient::network(Principal::Coordinator),
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let node = Node {
        id: "node".into(),
        capacity: spec("x").resources,
        available: true,
        labels: Default::default(),
        devices: vec![],
        runtime_classes: vec!["runsc".into()],
    };
    let stored = session
        .register_session(
            node.clone(),
            address.clone(),
            "127.0.0.1:2".into(),
            Some(NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    {
        let mut state = rpc.0.state.lock().await;
        state.scheduler.register(node.clone()).unwrap();
        state.nodes.insert(node.id.clone(), stored);
        state.live.insert(
            node.id.clone(),
            LiveNode {
                expired: false,
                session: "boot".into(),
                sequence: 1,
                last_seen: tokio::time::Instant::now(),
                inspected: true,
                report: pb::RegisterNodeRequest {
                    node_id: node.id,
                    node_address: address,
                    session_id: "boot".into(),
                    heartbeat_sequence: 1,
                    accepting_allocations: true,
                    ..Default::default()
                },
            },
        );
    }
    let artifact = CheckpointArtifact {
        storage: "local".into(),
        scope: String::new(),
        location: "/checkpoints/gc-in-flight".into(),
        size_bytes: 1,
    };
    if snapshot {
        let saved = Snapshot::new(
            "local-gc".into(),
            vec![],
            spec("source"),
            "node".into(),
            "source-1".into(),
            artifact.clone(),
        )
        .unwrap();
        session.publish_snapshot(saved).await.unwrap();
        session.delete_snapshot("local-gc", "tenant").await.unwrap();
    } else {
        let mut retired = running(&session, "source").await;
        retired.state = EnvironmentState::Deleted;
        retired.resources_held = false;
        retired.runtime.ip = None;
        retired.revision += 1;
        let mut artifacts = vec![artifact.clone()];
        if duplicate_location {
            artifacts.push(CheckpointArtifact {
                size_bytes: 0,
                ..artifact.clone()
            });
        }
        session
            .commit_with_cleanup(retired, &artifacts)
            .await
            .unwrap();
    }
    let mut consumer = running(&session, "consumer").await;
    let mut unrelated = running(&session, "unrelated").await;
    let pending = tokio::spawn({
        let rpc = rpc.clone();
        async move {
            if snapshot {
                rpc.collect_snapshots().await
            } else {
                rpc.collect_checkpoint_cleanups().await
            }
        }
    });
    // Do not service this owned socket: acceptance proves the collector passed
    // metadata checks and entered outbound node I/O. No timing-based sleep or
    // replacement storage implementation is needed to control this interval.
    let (socket, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    unrelated.execution_artifacts.push(CheckpointArtifact {
        location: "unrelated".into(),
        ..artifact.clone()
    });
    unrelated.revision += 1;
    let independent = session.commit(unrelated).await;
    consumer.execution_artifacts.push(artifact.clone());
    consumer.revision += 1;
    let attached = session.commit(consumer.clone()).await;
    pending.abort();
    let _ = pending.await;
    drop(socket);
    assert!(
        independent.is_ok(),
        "unrelated references must not wait for local deletion"
    );
    assert!(
        matches!(attached, Err(Error::Conflict)),
        "local deletion RPC raced a new reference: {attached:?}"
    );
    // A lost/cancelled RPC is not a successful deletion. Release exclusion for
    // an authoritative retry; retain the durable cleanup intent.
    session.commit(consumer).await.unwrap();
    assert!(matches!(
        session
            .claim_artifact_deletion(&artifact, snapshot.then_some("local-gc"))
            .await,
        Err(Error::Conflict)
    ));
    if snapshot {
        assert_eq!(
            session.get_snapshot("local-gc").await.unwrap().state,
            adx_core::snapshots::SnapshotState::Deleting
        );
    } else {
        assert_eq!(session.checkpoint_cleanups().await.unwrap().len(), 1);
    }
}

#[tokio::test]
#[ignore = "requires isolated real Redis"]
async fn local_snapshot_gc_excludes_references_during_node_rpc() {
    local_collection_excludes_new_references(true, false).await;
}

#[tokio::test]
#[ignore = "requires isolated real Redis"]
async fn local_checkpoint_cleanup_excludes_references_during_node_rpc() {
    local_collection_excludes_new_references(false, false).await;
}

#[tokio::test]
#[ignore = "requires isolated real Redis"]
async fn local_checkpoint_cleanup_aliases_share_one_deletion_guard() {
    local_collection_excludes_new_references(false, true).await;
}
