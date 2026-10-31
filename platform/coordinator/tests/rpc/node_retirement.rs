use super::*;
use adx_protocol::control::coordinator_service_server::CoordinatorService;
use adx_transport::rpc::RpcClient;

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py control-rpc"]
async fn coordinator_shutdown_finishes_with_live_directory_subscriptions() {
    use pb::environment_directory_service_server::EnvironmentDirectoryService;
    use pb::route_service_server::RouteService;

    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let rpc = service(session.clone()).await;
    let publisher = RoutePublisher::new(session, Peers::network());
    publisher.refresh().await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let closing = publisher.clone();
    let routes = publisher.clone();
    let nodes = rpc.clone();
    let closing_nodes = rpc.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(nodes))
            .add_service(pb::route_service_server::RouteServiceServer::new(
                routes.clone(),
            ))
            .add_service(
                pb::environment_directory_service_server::EnvironmentDirectoryServiceServer::new(
                    routes,
                ),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                stopped.await.unwrap();
                closing.close_subscriptions();
                closing_nodes.close_subscriptions();
            })
            .await
            .unwrap();
    });
    let channel = Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let route_request = || {
        let mut request = Request::new(pb::WatchRoutesRequest {});
        request
            .metadata_mut()
            .insert("adx-component", "ingress".parse().unwrap());
        request
    };
    let directory_request = || {
        let mut request = Request::new(pb::WatchEnvironmentsRequest {});
        request
            .metadata_mut()
            .insert("adx-component", "apiserver".parse().unwrap());
        request
    };
    let node_request = || {
        let mut request = Request::new(pb::WatchNodesRequest {});
        request
            .metadata_mut()
            .insert("adx-component", "apiserver".parse().unwrap());
        request
    };
    let mut node_client =
        pb::coordinator_service_client::CoordinatorServiceClient::new(channel.clone());
    let mut nodes = node_client
        .watch_nodes(node_request())
        .await
        .unwrap()
        .into_inner();
    assert!(nodes.message().await.unwrap().is_some());
    let mut route_client = pb::route_service_client::RouteServiceClient::new(channel.clone());
    let mut directory_client =
        pb::environment_directory_service_client::EnvironmentDirectoryServiceClient::new(channel);
    let mut routes = route_client
        .watch_routes(route_request())
        .await
        .unwrap()
        .into_inner();
    let mut directory = directory_client
        .watch_environments(directory_request())
        .await
        .unwrap()
        .into_inner();
    assert!(routes.message().await.unwrap().unwrap().reset);
    assert!(directory.message().await.unwrap().unwrap().reset);
    stop.send(()).unwrap();
    // Keep all three clients alive. Shutdown must end their infinite streams instead
    // of waiting for the deployment supervisor to kill/restart the old epoch.
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert!(nodes.message().await.unwrap().is_none());
    assert!(routes.message().await.unwrap().is_none());
    assert!(directory.message().await.unwrap().is_none());
    assert_eq!(
        rpc.watch_nodes(node_request()).await.unwrap_err().code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        publisher
            .watch_routes(route_request())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    assert_eq!(
        publisher
            .watch_environments(directory_request())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
}

async fn metrics(rpc: &CoordinatorRpc) -> String {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match rpc.metrics().await {
                Ok(text) => return text,
                Err(adx_core::Error::Unavailable(message))
                    if message == "Coordinator state busy" =>
                {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                Err(error) => panic!("metrics failed: {error}"),
            }
        }
    })
    .await
    .unwrap()
}

fn request<T>(value: T, node_id: &str) -> Request<T> {
    let mut r = Request::new(value);
    r.metadata_mut()
        .insert("adx-component", "node".parse().unwrap());
    r.metadata_mut().insert_bin(
        "adx-node-id-bin",
        tonic::metadata::MetadataValue::from_bytes(node_id.as_bytes()),
    );
    r
}
fn report(boot: &str) -> pb::RegisterNodeRequest {
    pb::RegisterNodeRequest {
        node_id: "old".into(),
        node_address: "127.0.0.1:9000".into(),
        proxy_address: "127.0.0.1:9001".into(),
        session_id: boot.into(),
        heartbeat_sequence: 1,
        reconciling: true,
        accepting_allocations: false,
        capacity: Some(spec("x").resources.into()),
        runtime_classes: vec!["runc".into()],
        ..Default::default()
    }
}
async fn service(session: Session) -> CoordinatorRpc {
    CoordinatorRpc::with_heartbeat_timeout(
        session,
        Placement::Pack,
        Peers::network(),
        RpcClient::network(Principal::Coordinator),
        Duration::from_secs(1),
        Duration::from_millis(50),
    )
    .await
    .unwrap()
}
async fn ready(rpc: &CoordinatorRpc, boot: &str) {
    rpc.register_node(request(report(boot), "old"))
        .await
        .unwrap();
    rpc.inspect_node(request(
        pb::InspectNodeRequest {
            node_id: "old".into(),
            session_id: boot.into(),
        },
        "old",
    ))
    .await
    .unwrap();
    let mut r = report(boot);
    r.reconciling = false;
    r.accepting_allocations = true;
    r.heartbeat_sequence = 2;
    rpc.register_node(request(r, "old")).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py control-rpc"]
async fn node_expiry_retirement_removes_metrics_without_reaping_healthy_nodes() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    let rpc = service(session.clone()).await;
    ready(&rpc, "boot-one").await;
    assert_eq!(
        rpc.collect_retired_nodes(Duration::from_nanos(1))
            .await
            .unwrap(),
        0
    );
    tokio::time::sleep(Duration::from_millis(80)).await;
    rpc.expire_nodes().await.unwrap();
    assert!(metrics(&rpc).await.contains("node_id=\"old\""));
    assert_eq!(
        rpc.collect_retired_nodes(Duration::from_secs(1800))
            .await
            .unwrap(),
        0
    );
    let since = session.snapshot().await.unwrap().nodes["old"]
        .retirement
        .as_ref()
        .unwrap()
        .since_unix_seconds;
    let session = store.begin(1).await.unwrap();
    let rpc = service(session.clone()).await;
    assert_eq!(
        session.snapshot().await.unwrap().nodes["old"]
            .retirement
            .as_ref()
            .unwrap()
            .since_unix_seconds,
        since
    );
    assert_eq!(
        rpc.collect_retired_nodes(Duration::from_nanos(1))
            .await
            .unwrap(),
        1
    );
    assert!(!metrics(&rpc).await.contains("node_id=\"old\""));
    let publisher = RoutePublisher::new(session, Peers::network());
    publisher.refresh().await.unwrap();
    let restarted = service(store.begin(1).await.unwrap()).await;
    assert!(!metrics(&restarted).await.contains("node_id=\"old\""));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py control-rpc"]
async fn unregister_node_is_authenticated_idempotent_and_cannot_remove_replacement() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    let rpc = service(session.clone()).await;
    ready(&rpc, "boot-one").await;
    let payload = pb::UnregisterNodeRequest {
        node_id: "old".into(),
        session_id: "boot-one".into(),
    };
    assert_eq!(
        rpc.unregister_node(request(payload.clone(), "other"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    rpc.unregister_node(request(payload.clone(), "old"))
        .await
        .unwrap();
    rpc.unregister_node(request(payload.clone(), "old"))
        .await
        .unwrap();
    assert!(!metrics(&rpc).await.contains("node_id=\"old\""));
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        0
    );
    drop(rpc);
    let rpc = service(store.begin(1).await.unwrap()).await;
    assert_eq!(
        rpc.register_node(request(report("boot-one"), "old"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    ready(&rpc, "boot-two").await;
    assert_eq!(
        rpc.unregister_node(request(payload, "old"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert!(metrics(&rpc).await.contains("node_id=\"old\""));
    let rpc = service(store.begin(1).await.unwrap()).await;
    assert_eq!(
        rpc.register_node(request(report("boot-one"), "old"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert!(metrics(&rpc).await.contains("node_id=\"old\""));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py control-rpc"]
async fn unregister_node_preserves_recovery_context_until_failed_environment_is_collected() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    let rpc = service(session.clone()).await;
    ready(&rpc, "boot-one").await;
    let assignment = adx_core::Assignment {
        environment_id: "held".into(),
        node_id: "old".into(),
        shard_id: 0,
        generation: session.epoch() << 32 | 1,
        devices: vec![],
    };
    session
        .reserve(spec("held"), assignment.clone())
        .await
        .unwrap();
    let record = EnvironmentRecord {
        observation: None,
        execution_artifacts: vec![],
        snapshot_source: None,
        failure: None,
        restart_attempts: 0,
        restart_pending: false,
        spec: spec("held"),
        assignment,
        state: adx_core::EnvironmentState::Running,
        revision: 2,
        runtime: adx_core::Runtime {
            id: format!("held-{}", session.epoch() << 32 | 1),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: None,
        last_operation: None,
    };
    session.commit(record).await.unwrap();
    rpc.unregister_node(request(
        pb::UnregisterNodeRequest {
            node_id: "old".into(),
            session_id: "boot-one".into(),
        },
        "old",
    ))
    .await
    .unwrap();
    let saved = session.get("held").await.unwrap();
    assert!(saved.invalidated && !saved.resources_held());
    assert!(session.snapshot().await.unwrap().nodes.contains_key("old"));
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        0
    );
    assert_eq!(
        rpc.collect_retired_nodes(Duration::from_nanos(1))
            .await
            .unwrap(),
        0
    );
    let rpc = service(store.begin(1).await.unwrap()).await;
    assert_eq!(
        rpc.register_node(request(report("boot-one"), "old"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(rpc.collect_failed(Duration::ZERO).await.unwrap(), 1);
    assert_eq!(
        rpc.collect_retired_nodes(Duration::from_secs(1800))
            .await
            .unwrap(),
        1
    );
    assert!(!metrics(&rpc).await.contains("node_id=\"old\""));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py control-rpc"]
async fn unregister_node_finishes_after_its_rpc_caller_disconnects() {
    let rig = common::Redis::new().await;
    // Keep the departure in flight long enough to cancel its RPC caller, while
    // separating that cancellation contract from the default 300ms Redis budget.
    // Delayed/applied/cancelled Redis writes have their own transport fault tests.
    let store =
        adx_coordinator::storage::RedisStore::connect(&rig.url, "test", Duration::from_secs(2))
            .await
            .unwrap();
    let session = store.begin(1).await.unwrap();
    let rpc = service(session.clone()).await;
    ready(&rpc, "boot-one").await;
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(200)
        .arg("WRITE")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let service = rpc.clone();
    let caller = tokio::spawn(async move {
        service
            .unregister_node(request(
                pb::UnregisterNodeRequest {
                    node_id: "old".into(),
                    session_id: "boot-one".into(),
                },
                "old",
            ))
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!caller.is_finished());
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    let completion = tokio::time::timeout(Duration::from_secs(2), async {
        while session.snapshot().await.unwrap().nodes.contains_key("old") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        completion.is_ok(),
        "cancelled caller abandoned node retirement: {:?}",
        session.snapshot().await.unwrap().nodes.get("old")
    );
    assert!(!metrics(&rpc).await.contains("node_id=\"old\""));
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        0
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py control-rpc"]
async fn coordinator_shutdown_cancels_backpressured_node_directory() {
    use tokio_stream::StreamExt;
    let rig = common::Redis::new().await;
    let rpc = service(rig.store().await.begin(1).await.unwrap()).await;
    let mut request = Request::new(pb::WatchNodesRequest {});
    request
        .metadata_mut()
        .insert("adx-component", "apiserver".parse().unwrap());
    let mut stream = rpc.watch_nodes(request).await.unwrap().into_inner();
    // Leave the capacity-one channel unread through its second publication.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    rpc.close_subscriptions();
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame) = stream.next().await {
            frame.unwrap();
        }
    })
    .await
    .unwrap();
}
