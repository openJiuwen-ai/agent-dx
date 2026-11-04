use super::*;
use adx_core::snapshots::Snapshot;
use pb::{
    coordinator_service_server::CoordinatorService, snapshot_service_server::SnapshotService,
};

fn node_request<T>(value: T) -> Request<T> {
    let mut request = Request::new(value);
    request
        .metadata_mut()
        .insert("adx-component", "node".parse().unwrap());
    request.metadata_mut().insert_bin(
        "adx-node-id-bin",
        tonic::metadata::MetadataValue::from_bytes(b"node"),
    );
    request
}

fn api_request<T>(value: T) -> Request<T> {
    let mut request = Request::new(value);
    request
        .metadata_mut()
        .insert("adx-component", "apiserver".parse().unwrap());
    request
}

async fn fixture() -> (
    cancellation_test_redis::Redis,
    CoordinatorRpc,
    Session,
    Snapshot,
    pb::RegisterNodeRequest,
) {
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
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
        "id":"snapshot-source", "tenant_id":"tenant", "image":"image",
        "runtime_class":"runsc", "priority":0,
        "resources":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0}
    }))
    .unwrap();
    let node = Node {
        id: "node".into(),
        capacity: spec.resources,
        available: true,
        labels: Default::default(),
        devices: vec![],
        runtime_classes: vec!["runsc".into()],
    };
    let saved = session
        .register_session(
            node.clone(),
            "127.0.0.1:1".into(),
            "127.0.0.1:2".into(),
            Some(NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let report = pb::RegisterNodeRequest {
        node_id: "node".into(),
        node_address: "127.0.0.1:1".into(),
        proxy_address: "127.0.0.1:2".into(),
        capacity: Some(node.capacity.into()),
        accepting_allocations: true,
        session_id: "boot".into(),
        heartbeat_sequence: 1,
        runtime_classes: vec!["runsc".into()],
        ..Default::default()
    };
    let assignment = {
        let mut state = rpc.0.state.lock().await;
        state.scheduler.register(node).unwrap();
        state.nodes.insert("node".into(), saved);
        state.live.insert(
            "node".into(),
            LiveNode {
                expired: false,
                session: "boot".into(),
                sequence: 1,
                last_seen: tokio::time::Instant::now(),
                inspected: true,
                report: report.clone(),
            },
        );
        state.scheduler.submit(spec.clone()).unwrap();
        state.specs.insert(spec.id.clone(), spec.clone());
        state.drive().await.unwrap();
        state.environments[&spec.id].assignment.clone()
    };
    let artifact = adx_core::CheckpointArtifact {
        storage: "shared".into(),
        scope: "scope".into(),
        location: "snapshot-publication-artifact".into(),
        size_bytes: 1,
    };
    let record = EnvironmentRecord {
        observation: None,
        spec: spec.clone(),
        assignment: assignment.clone(),
        state: EnvironmentState::Running,
        revision: 2,
        runtime: adx_core::Runtime {
            id: format!("{}-{}", spec.id, assignment.generation),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: None,
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
        failure: None,
        snapshot_source: None,
        execution_artifacts: vec![artifact.clone()],
    };
    rpc.commit(record.clone(), "boot".into()).await.unwrap();
    let snapshot = Snapshot::new(
        "publication".into(),
        vec![],
        spec,
        "node".into(),
        record.runtime.id,
        artifact,
    )
    .unwrap();
    (redis, rpc, session, snapshot, report)
}

#[tokio::test]
#[ignore = "requires isolated real Redis"]
async fn slow_snapshot_publication_does_not_block_a_live_heartbeat() {
    let (redis, rpc, _session, snapshot, mut report) = fixture().await;
    let mut control = redis::Client::open(redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(3000)
        .arg("WRITE")
        .query_async(&mut control)
        .await
        .unwrap();
    let publishing = tokio::spawn({
        let rpc = rpc.clone();
        async move {
            rpc.publish_snapshot(node_request(pb::PublishSnapshotRequest {
                snapshot: Some(snapshot.try_into().unwrap()),
                node_session_id: "boot".into(),
            }))
            .await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !publishing.is_finished(),
        "snapshot write must still be stalled"
    );
    let mut observers = tokio::task::JoinSet::new();
    for operation in ["get", "list", "delete", "inspect", "gc"] {
        let rpc = rpc.clone();
        observers.spawn(async move {
            let caller = Some(pb::CallerContext {
                tenant_id: "tenant".into(),
                administrator: false,
            });
            match operation {
                "get" => {
                    let _ = rpc
                        .get_snapshot(api_request(pb::GetSnapshotRequest {
                            id: "publication".into(),
                            caller,
                            node_session_id: String::new(),
                        }))
                        .await;
                }
                "list" => {
                    let _ = rpc
                        .list_snapshots(api_request(pb::ListSnapshotsRequest {
                            caller,
                            ..Default::default()
                        }))
                        .await;
                }
                "delete" => {
                    let _ = rpc
                        .delete_snapshot(api_request(pb::DeleteSnapshotRequest {
                            id: "publication".into(),
                            caller,
                        }))
                        .await;
                }
                "inspect" => {
                    let _ = rpc
                        .inspect_node(node_request(pb::InspectNodeRequest {
                            node_id: "node".into(),
                            session_id: "boot".into(),
                        }))
                        .await;
                }
                "gc" => {
                    let _ = rpc.collect_snapshots().await;
                }
                _ => unreachable!(),
            }
        });
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    report.heartbeat_sequence += 1;
    let heartbeat = tokio::time::timeout(
        Duration::from_millis(250),
        rpc.register_node(node_request(report)),
    )
    .await;
    let _: String = redis::cmd("CLIENT")
        .arg("UNPAUSE")
        .query_async(&mut control)
        .await
        .unwrap();
    publishing.await.unwrap().unwrap();
    assert!(
        heartbeat.is_ok(),
        "snapshot storage stalled the heartbeat state lock"
    );
    heartbeat.unwrap().unwrap();
    while let Some(result) = observers.join_next().await {
        result.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires isolated real Redis"]
async fn persisted_node_session_replacement_fences_snapshot_publication() {
    let (_redis, rpc, session, snapshot, _) = fixture().await;
    let saved = session.snapshot().await.unwrap().nodes["node"].clone();
    session
        .register_session(
            saved.node,
            saved.address,
            saved.proxy_address,
            Some(NodeSession {
                id: "replacement-boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    // The durable session has advanced while the RPC's in-memory view still
    // contains the previous process. Publication must validate both boundaries.
    let result = rpc
        .publish_snapshot(node_request(pb::PublishSnapshotRequest {
            snapshot: Some(snapshot.clone().try_into().unwrap()),
            node_session_id: "boot".into(),
        }))
        .await;
    assert!(
        result.is_err(),
        "a replaced node session published a snapshot"
    );
    assert_eq!(
        session.get_snapshot(&snapshot.id).await,
        Err(Error::NotFound)
    );
}

#[tokio::test]
#[ignore = "requires isolated real Redis"]
async fn concurrent_snapshot_ids_cannot_claim_the_same_artifact() {
    let (_redis, rpc, session, first, _) = fixture().await;
    let mut second = first.clone();
    second.id = "another-publication".into();
    let (one, two) = tokio::join!(
        rpc.publish_snapshot(node_request(pb::PublishSnapshotRequest {
            snapshot: Some(first.try_into().unwrap()),
            node_session_id: "boot".into(),
        })),
        rpc.publish_snapshot(node_request(pb::PublishSnapshotRequest {
            snapshot: Some(second.try_into().unwrap()),
            node_session_id: "boot".into(),
        })),
    );
    assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1);
    assert_eq!(session.retained_snapshots().await.unwrap().len(), 1);
}
