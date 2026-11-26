//! Exercise the management HTTP boundary and real Node identity probe together.
use super::*;
use serde_json::{json, Value};

pub(super) async fn management(
    address: std::net::SocketAddr,
    path: &str,
    body: Value,
) -> (u16, Value) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let body = body.to_string();
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let response = String::from_utf8(bytes).unwrap();
    let (header, body) = response.split_once("\r\n\r\n").unwrap();
    (
        header.split_whitespace().nth(1).unwrap().parse().unwrap(),
        serde_json::from_str(body).unwrap(),
    )
}

pub(super) async fn serve_management(
    rpc: CoordinatorRpc,
    servers: &mut Servers,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    servers.0.push(tokio::spawn(async move {
        adx_coordinator::autoscaling::serve(listener, rpc)
            .await
            .unwrap();
    }));
    address
}

pub(super) async fn serve_handoff(
    rpc: CoordinatorRpc,
    servers: &mut Servers,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    servers.0.push(tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(rpc.clone()))
            .add_service(pb::node_handoff_service_server::NodeHandoffServiceServer::new(rpc))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    }));
    address
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates"]
async fn maintenance_registration_probes_even_after_heartbeat_expiry() {
    Box::pin(automatic_registration(false)).await;
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates"]
async fn maintenance_registration_recovers_after_coordinator_restart() {
    Box::pin(automatic_registration(true)).await;
}

async fn automatic_registration(restart: bool) {
    let redis = common::Redis::new().await;
    let store = redis.store().await;
    let mut session = store.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::with_heartbeat_timeout(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let mut servers = Servers(vec![]);
    let coordinator_address = serve_handoff(rpc.clone(), &mut servers).await;
    let mut http = serve_management(rpc, &mut servers).await;
    let backend = Arc::new(Backend {
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::default(),
        pause_writes_after_remove: Mutex::new(None),
    });
    let manager = Arc::new(Adxlet::new(
        "node".into(),
        backend,
        Arc::new(LocalChecks),
        Arc::new(LocalChecks),
        Arc::new(
            CoordinatorStateSink::new(
                channel(coordinator_address, "node").await,
                Duration::from_secs(2),
            )
            .unwrap()
            .with_session("a".into()),
        ),
    ));
    manager
        .update_capacity(spec("held").resources, Duration::from_secs(60))
        .unwrap();
    let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_address = node_listener.local_addr().unwrap();
    let mut node_server = serve_lifecycle_node(
        node_listener,
        NodeRpc::new(manager.clone(), peers(), "a".into()),
    );
    let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(coordinator_address, "node").await,
    );
    let mut handoff = pb::node_handoff_service_client::NodeHandoffServiceClient::new(
        channel(coordinator_address, "node").await,
    );
    let mut report = pb::RegisterNodeRequest {
        node_id: "node".into(),
        node_address: node_address.to_string(),
        proxy_address: "127.0.0.1:9999".into(),
        runtime_classes: vec!["runc".into()],
        capacity: Some(spec("held").resources.into()),
        session_id: "a".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        ..Default::default()
    };
    client.register_node(report.clone()).await.unwrap();
    client
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "a".into(),
        })
        .await
        .unwrap();
    report.heartbeat_sequence = 2;
    report.reconciling = false;
    report.accepting_allocations = true;
    client.register_node(report.clone()).await.unwrap();
    let mut frontend = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(coordinator_address, "apiserver").await,
    );
    frontend.create_environment(create("held")).await.unwrap();
    assert_eq!(session.snapshot().await.unwrap().routes().unwrap().len(), 1);
    let owner = json!({"node_id":"node","rollout_id":"r","local_task_id":"t","epoch":1});
    let base = "/v1/adxlet-process-upgrades";
    assert_eq!(
        management(
            http,
            base,
            json!({"action":"prepare","owner":owner,"node_session":"a"})
        )
        .await
        .0,
        200
    );
    assert_eq!(
        session.snapshot().await.unwrap().routes().unwrap().len(),
        1,
        "Prepare preserves the original route before Yield"
    );
    report.session_id = "b".into();
    report.heartbeat_sequence = 1;
    report.reconciling = true;
    report.accepting_allocations = false;
    assert!(
        client.register_node(report.clone()).await.is_err(),
        "Yield is mandatory"
    );
    let wire_owner = pb::MaintenanceOwner {
        node_id: "node".into(),
        rollout_id: "r".into(),
        local_task_id: "t".into(),
        epoch: 1,
    };
    let yielding = pb::YieldNodeSessionRequest {
        owner: Some(wire_owner.clone()),
        expected_session: "a".into(),
    };
    let mut untrusted = pb::node_handoff_service_client::NodeHandoffServiceClient::new(
        channel(coordinator_address, "apiserver").await,
    );
    assert_eq!(
        untrusted
            .r#yield(yielding.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        untrusted
            .query_release(pb::MaintenanceReleaseRequest {
                owner: Some(wire_owner.clone()),
                node_session: "a".into()
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let yielded = handoff
        .r#yield(yielding.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(yielded.yielded);
    assert_eq!(
        session.snapshot().await.unwrap().routes().unwrap().len(),
        1,
        "Yield retains the confirmed execution route"
    );
    assert_eq!(
        handoff.r#yield(yielding).await.unwrap().into_inner(),
        yielded,
        "lost Yield acknowledgement replays"
    );
    if restart {
        // No in-memory live predecessor remains; the durable task still fences registration.
        session = store.begin(1).await.unwrap();
        let replacement = CoordinatorRpc::new(
            session.clone(),
            Placement::Pack,
            peers(),
            client_tls("coordinator"),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let address = serve_handoff(replacement.clone(), &mut servers).await;
        http = serve_management(replacement, &mut servers).await;
        client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            channel(address, "node").await,
        );
        handoff = pb::node_handoff_service_client::NodeHandoffServiceClient::new(
            channel(address, "node").await,
        );
    } else {
        tokio::time::sleep(Duration::from_millis(1100)).await;
    }
    assert_eq!(
        session.snapshot().await.unwrap().routes().unwrap().len(),
        1,
        "maintenance preserves the route across expiry/restart"
    );
    assert!(
        client.register_node(report.clone()).await.is_err(),
        "GetSession still returns a, even when its heartbeat expired"
    );
    let query = format!("{base}/query");
    assert_eq!(
        management(http, &query, json!({"owner":owner})).await.1["current_session"],
        "a"
    );
    node_server.abort();
    let _ = node_server.await;
    let listener = TcpListener::bind(node_address).await.unwrap();
    node_server =
        serve_lifecycle_node(listener, NodeRpc::new(manager.clone(), peers(), "b".into()));
    client.register_node(report.clone()).await.unwrap();
    client.register_node(report.clone()).await.unwrap();
    assert_eq!(
        management(http, &query, json!({"owner":owner})).await.1["current_session"],
        "b"
    );
    let mut late = report.clone();
    late.session_id = "a".into();
    assert!(
        client.register_node(late).await.is_err(),
        "Yielded origin never reopens"
    );
    assert_ne!(
        management(
            http,
            base,
            json!({"action":"release","owner":owner,"node_session":"b"})
        )
        .await
        .0,
        200,
        "registration alone is not recovery"
    );
    // A failed candidate is reaped before another process registers for rollback.
    node_server.abort();
    let _ = node_server.await;
    let listener = TcpListener::bind(node_address).await.unwrap();
    node_server = serve_lifecycle_node(listener, NodeRpc::new(manager, peers(), "c".into()));
    let mut rollback = report.clone();
    rollback.session_id = "c".into();
    client.register_node(rollback.clone()).await.unwrap();
    assert!(
        client.register_node(report).await.is_err(),
        "a late failed candidate cannot displace rollback"
    );
    client
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "c".into(),
        })
        .await
        .unwrap();
    rollback.heartbeat_sequence = 2;
    rollback.reconciling = false;
    client.register_node(rollback).await.unwrap();
    assert_eq!(
        management(
            http,
            base,
            json!({"action":"release","owner":owner,"node_session":"c"})
        )
        .await
        .0,
        200
    );
    assert_eq!(
        management(http, &query, json!({"owner":owner})).await.1,
        json!({"status":"released","owner":owner,"node_session":"c"})
    );
    assert_eq!(
        handoff
            .query_release(pb::MaintenanceReleaseRequest {
                owner: Some(wire_owner),
                node_session: "c".into()
            })
            .await
            .unwrap()
            .into_inner()
            .node_session,
        "c"
    );
    node_server.abort();
}
