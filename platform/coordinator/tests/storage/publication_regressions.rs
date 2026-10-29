use super::common;
use adx_coordinator::{
    storage::{NodeSession, Session},
    Node, Placement,
};
use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources, Runtime,
};
use adx_protocol::{
    auth::{Peers, Principal},
    control as pb,
};
use adx_transport::rpc::RpcClient;
use std::time::Duration;

fn spec() -> EnvironmentSpec {
    serde_json::from_value(serde_json::json!({
        "id":"review-env", "tenant_id":"tenant", "image":"image", "runtime_class":"runc",
        "priority":0, "resources":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0}
    }))
    .unwrap()
}
async fn registered(session: &Session, id: &str) {
    session
        .register_session(
            Node {
                id: id.into(),
                capacity: Resources {
                    cpu_millis: 1000,
                    memory_bytes: 1024,
                    disk_bytes: 0,
                },
                available: true,
                labels: Default::default(),
                devices: vec![],
                runtime_classes: vec!["runc".into()],
            },
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
}
fn running() -> EnvironmentRecord {
    EnvironmentRecord {
        observation: None,
        spec: spec(),
        assignment: Assignment {
            environment_id: "review-env".into(),
            node_id: "source".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        },
        runtime: Runtime {
            id: "review-env-1".into(),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        state: EnvironmentState::Running,
        revision: 2,
        resources_held: true,
        checkpoint: None,
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
        failure: None,
        snapshot_source: None,
        execution_artifacts: vec![],
    }
}
#[tokio::test]
#[ignore = "requires isolated Redis; run storage regression suite"]
async fn receipt_follows_recovered_environment() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    registered(&session, "source").await;
    registered(&session, "target").await;
    let mut first = running();
    first.checkpoint = Some(adx_core::RestorePoint {
        id: "pause".into(),
        origin: None,
        expires_at_unix_seconds: None,
        source_runtime_id: first.runtime.id.clone(),
        artifact: adx_core::CheckpointArtifact {
            storage: "shared".into(),
            location: "artifact".into(),
            size_bytes: 8,
        },
    });
    session
        .publish_initial(first.clone(), "review-request", "boot")
        .await
        .unwrap();
    session.invalidate_node("source", "boot").await.unwrap();
    let replacement = Assignment {
        node_id: "target".into(),
        generation: 2,
        ..first.assignment.clone()
    };
    let saved = session
        .reserve_recovery(&first.assignment, replacement, 1)
        .await
        .unwrap();
    let mut restored = saved.result.unwrap();
    restored.state = EnvironmentState::Running;
    restored.resources_held = true;
    restored.runtime.ip = Some("10.0.0.3".parse().unwrap());
    restored.runtime.id = "review-env-2-r2".into();
    restored.revision += 2;
    restored.last_operation = Some(adx_core::CompletedOperation {
        id: "recover-2".into(),
        kind: adx_core::LifecycleKind::Resume,
        expected_revision: 1,
    });
    session.commit(restored.clone()).await.unwrap();
    assert_eq!(
        session.get("review-env").await.unwrap().result,
        Some(restored.clone())
    );
    let replay = session
        .published_request("tenant", "review-request")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        replay, restored,
        "same Environment lifetime must replay its recovered execution"
    );
    // The atomic publication replay path must resolve the same current owner.
    assert_eq!(
        session
            .publish_initial(first.clone(), "review-request", "boot")
            .await
            .unwrap(),
        restored
    );
    registered(&session, "third").await;
    session.invalidate_node("target", "boot").await.unwrap();
    let next = Assignment {
        node_id: "third".into(),
        generation: 3,
        ..restored.assignment.clone()
    };
    let saved = session
        .reserve_recovery(&restored.assignment, next, 1)
        .await
        .unwrap();
    let mut current = saved.result.unwrap();
    current.state = EnvironmentState::Running;
    current.resources_held = true;
    current.runtime.ip = Some("10.0.0.4".parse().unwrap());
    current.runtime.id = "review-env-3-r2".into();
    current.revision += 2;
    current.last_operation = Some(adx_core::CompletedOperation {
        id: "recover-3".into(),
        kind: adx_core::LifecycleKind::Resume,
        expected_revision: 1,
    });
    session.commit(current.clone()).await.unwrap();
    rig.crash();
    rig.start().await;
    let session = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        session
            .published_request("tenant", "review-request")
            .await
            .unwrap(),
        Some(current.clone())
    );
    let mut deleted = current.clone();
    deleted.state = EnvironmentState::Deleted;
    deleted.resources_held = false;
    deleted.runtime.ip = None;
    deleted.revision += 2;
    session.commit(deleted).await.unwrap();
    let mut recreated = running();
    recreated.assignment.node_id = "third".into();
    recreated.assignment.generation = 4;
    recreated.runtime.id = "review-env-4".into();
    session
        .publish_initial(recreated.clone(), "new-lifetime", "boot")
        .await
        .unwrap();
    let old = session
        .published_request("tenant", "review-request")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.state, EnvironmentState::Deleted);
    assert_eq!(old.assignment, first.assignment);
    assert_eq!(
        session
            .published_request("tenant", "new-lifetime")
            .await
            .unwrap(),
        Some(recreated)
    );
}
#[tokio::test]
#[ignore = "requires isolated Redis; run storage regression suite"]
async fn environment_identity_precedes_new_snapshot_normalization() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    registered(&session, "source").await;
    let first = running();
    session
        .publish_initial(first.clone(), "first-request", "boot")
        .await
        .unwrap();
    let rpc = adx_coordinator::rpc::CoordinatorRpc::new(
        session,
        Placement::Pack,
        Peers::network(),
        RpcClient::network(Principal::Coordinator),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(rpc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let transport = RpcClient::network(Principal::ApiServer);
    let channel = transport.wrap(
        transport
            .endpoint(&addr.to_string())
            .unwrap()
            .connect()
            .await
            .unwrap(),
    );
    let mut api = pb::coordinator_service_client::CoordinatorServiceClient::new(channel);
    let mut retry: pb::EnvironmentSpec = spec().into();
    retry.snapshot_id = Some("unused-new-snapshot".into());
    let result = api
        .create_environment(pb::CreateEnvironmentRequest {
            request_id: "new-request".into(),
            preparation_context: None,
            spec: Some(retry),
            caller: Some(pb::CallerContext {
                tenant_id: "tenant".into(),
                administrator: false,
            }),
            schedule_timeout_seconds: 30,
            create_timeout_seconds: 90,
            create_deadline_unix_millis: 0,
            expected_assignment: None,
        })
        .await;
    server.abort();
    assert!(
        result.is_ok(),
        "confirmed Environment ID should reuse old success: {result:?}"
    );
    assert_eq!(
        result
            .unwrap()
            .into_inner()
            .record
            .unwrap()
            .spec
            .unwrap()
            .snapshot_id,
        None
    );
}
