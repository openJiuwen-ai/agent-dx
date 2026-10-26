use super::*;
use adx_transport::rpc::{RpcChannel, RpcClient};

async fn connect(address: std::net::SocketAddr, principal: Principal) -> RpcChannel {
    let client = RpcClient::network(principal);
    client.wrap(
        client
            .endpoint(&address.to_string())
            .unwrap()
            .connect()
            .await
            .unwrap(),
    )
}

#[tokio::test]
#[ignore = "requires isolated Redis server"]
async fn network_create_preserves_tenant_session_and_api_key_checks() {
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let key = "network-test-api-key-01234567890123456789";
    session
        .bootstrap_credential(
            key,
            &adx_coordinator::auth::Credential {
                tenant_id: "tenant".into(),
                administrator: false,
                expires_at_unix_seconds: 0,
            },
        )
        .await
        .unwrap();
    let auth = adx_coordinator::auth::AuthRpc::new(session.clone(), Peers::network());
    let rpc = CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        Peers::network(),
        RpcClient::network(Principal::Coordinator),
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ma = ml.local_addr().unwrap();
    let mut servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(rpc))
            .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
            .serve_with_incoming(TcpListenerStream::new(ml))
            .await
            .unwrap();
    })]);
    let sink = Arc::new(
        CoordinatorStateSink::with_rpc_channel(
            connect(ma, Principal::Node("node".into())).await,
            Duration::from_secs(2),
        )
        .unwrap()
        .with_session("boot-1".into()),
    );
    let backend = Arc::new(Backend {
        pause_writes_after_remove: Mutex::new(None),
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::default(),
    });
    let manager = Arc::new(Adxlet::new(
        "node".into(),
        backend.clone(),
        Arc::new(LocalChecks),
        Arc::new(LocalChecks),
        sink,
    ));
    manager
        .update_capacity(spec("test").resources, Duration::from_secs(60))
        .unwrap();
    let node = NodeRpc::new(manager, Peers::network(), "boot-1".into());
    let nl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let na = nl.local_addr().unwrap();
    servers.0.push(tokio::spawn(async move {
        Server::builder()
            .add_service(pb::node_service_server::NodeServiceServer::new(node))
            .serve_with_incoming(TcpListenerStream::new(nl))
            .await
            .unwrap();
    }));
    let mut register = pb::RegisterNodeRequest {
        storage_mounts: Default::default(),
        runtime_classes: vec![
            "runsc".into(),
            "runc".into(),
            "firecracker".into(),
            "r".into(),
            "test-runtime".into(),
        ],
        session_id: "boot-1".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        node_id: "node".into(),
        node_address: na.to_string(),
        proxy_address: "127.0.0.1:9999".into(),
        capacity: Some(spec("test").resources.into()),
        accepting_allocations: false,
        labels: Default::default(),
        devices: vec![],
    };
    let mut worker = pb::coordinator_service_client::CoordinatorServiceClient::new(
        connect(ma, Principal::Node("node".into())).await,
    );
    worker.register_node(register.clone()).await.unwrap();
    worker
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "boot-1".into(),
        })
        .await
        .unwrap();
    register.heartbeat_sequence = 2;
    register.reconciling = false;
    register.accepting_allocations = true;
    worker.register_node(register.clone()).await.unwrap();
    register.node_id = "other".into();
    assert_eq!(
        worker.register_node(register).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    let api_channel = connect(ma, Principal::ApiServer).await;
    let mut auth = pb::auth_service_client::AuthServiceClient::new(api_channel.clone());
    assert_eq!(
        auth.verify_api_key(pb::VerifyApiKeyRequest {
            api_key: "invalid".into()
        })
        .await
        .unwrap_err()
        .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        auth.verify_api_key(pb::VerifyApiKeyRequest {
            api_key: key.into()
        })
        .await
        .unwrap()
        .into_inner()
        .caller
        .unwrap()
        .tenant_id,
        "tenant"
    );
    let mut api = pb::coordinator_service_client::CoordinatorServiceClient::new(api_channel);
    api.create_environment(create("network-environment"))
        .await
        .unwrap();
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    let mut get = pb::GetEnvironmentRequest {
        environment_id: "network-environment".into(),
        caller: caller(),
    };
    api.get_environment(get.clone()).await.unwrap();
    get.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        api.get_environment(get).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    // A bare plaintext RPC channel does not silently acquire an ingress role.
    let raw = tonic::transport::Endpoint::from_shared(format!("http://{ma}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut unknown = pb::coordinator_service_client::CoordinatorServiceClient::new(raw);
    assert_eq!(
        unknown
            .create_environment(create("unknown"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}

struct SlowBackend(Backend, AtomicUsize);
#[async_trait::async_trait]
impl RuntimeDriver for SlowBackend {
    async fn start(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        self.1.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(3200)).await;
        self.0.start(spec, id, generation, devices).await
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        self.0.is_running(id).await
    }
    async fn remove(&self, id: &str) -> Result<()> {
        self.0.remove(id).await
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis server"]
async fn create_retries_ready_delivery_and_lost_response_within_original_deadline() {
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        Peers::network(),
        RpcClient::network(Principal::Coordinator),
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ma = ml.local_addr().unwrap();
    let mut servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(rpc))
            .serve_with_incoming(TcpListenerStream::new(ml))
            .await
            .unwrap();
    })]);
    let sink = Arc::new(
        CoordinatorStateSink::with_rpc_channel(
            connect(ma, Principal::Node("node".into())).await,
            Duration::from_secs(2),
        )
        .unwrap()
        .with_session("boot-1".into()),
    );
    let backend = Arc::new(SlowBackend(
        Backend {
            pause_writes_after_remove: Mutex::new(None),
            session: session.clone(),
            started: AtomicUsize::new(0),
            removed: AtomicUsize::new(0),
            running: Mutex::default(),
        },
        AtomicUsize::new(0),
    ));
    let manager = Arc::new(Adxlet::new(
        "node".into(),
        backend.clone(),
        Arc::new(LocalChecks),
        Arc::new(LocalChecks),
        sink,
    ));
    manager
        .update_capacity(spec("test").resources, Duration::from_secs(60))
        .unwrap();
    let node = NodeRpc::new(manager, Peers::network(), "boot-1".into());
    // The first candidate is unreachable; its unfinished assignment stays in memory.
    let nl = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let na = nl.local_addr().unwrap();
    drop(nl);
    let mut worker = pb::coordinator_service_client::CoordinatorServiceClient::new(
        connect(ma, Principal::Node("node".into())).await,
    );
    let mut register = pb::RegisterNodeRequest {
        storage_mounts: Default::default(),
        runtime_classes: vec!["runc".into()],
        session_id: "boot-1".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        node_id: "node".into(),
        node_address: na.to_string(),
        proxy_address: "127.0.0.1:9999".into(),
        capacity: Some(spec("test").resources.into()),
        accepting_allocations: false,
        labels: Default::default(),
        devices: vec![],
    };
    worker.register_node(register.clone()).await.unwrap();
    worker
        .inspect_node(pb::InspectNodeRequest {
            node_id: "node".into(),
            session_id: "boot-1".into(),
        })
        .await
        .unwrap();
    register.heartbeat_sequence = 2;
    register.reconciling = false;
    register.accepting_allocations = true;
    worker.register_node(register.clone()).await.unwrap();
    let mut api = pb::coordinator_service_client::CoordinatorServiceClient::new(
        connect(ma, Principal::ApiServer).await,
    );
    let mut request = create("retry-cold");
    request
        .spec
        .as_mut()
        .unwrap()
        .execution
        .as_mut()
        .unwrap()
        .init_timeout_seconds = 1;
    request.create_deadline_unix_millis =
        adx_transport::deadline::Deadline::after(Duration::from_secs(10))
            .unwrap()
            .unix_millis();
    let pending_reply = tokio::spawn({
        let mut api = api.clone();
        let request = request.clone();
        async move { api.create_environment(request).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        session.get("retry-cold").await,
        Err(adx_core::Error::NotFound)
    );
    assert_eq!(backend.1.load(Ordering::SeqCst), 0);
    let nl = TcpListener::bind(na).await.unwrap();
    servers.0.push(tokio::spawn(async move {
        Server::builder()
            .add_service(pb::node_service_server::NodeServiceServer::new(node))
            .serve_with_incoming(TcpListenerStream::new(nl))
            .await
            .unwrap();
    }));
    register.heartbeat_sequence = 3;
    worker.register_node(register).await.unwrap();
    // Fresh readiness permits retry. Losing the caller's reply does not cancel
    // the Coordinator-owned create task or extend its deadline.
    tokio::time::timeout(Duration::from_secs(2), async {
        while backend.1.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    pending_reply.abort();
    assert!(pending_reply.await.unwrap_err().is_cancelled());
    let result = api
        .create_environment(request.clone())
        .await
        .unwrap()
        .into_inner();
    let record = result.record.unwrap();
    assert_eq!(record.state, pb::EnvironmentState::Running as i32);
    let confirmed = record.assignment.clone().unwrap();
    assert_eq!(backend.1.load(Ordering::SeqCst), 1);
    assert_eq!(backend.0.started.load(Ordering::SeqCst), 1);
    assert_eq!(
        session.get("retry-cold").await.unwrap().assignment,
        confirmed.clone().try_into().unwrap()
    );
    // A diagnostic assignment cannot override an already confirmed result.
    let mut observed = confirmed;
    observed.generation += 1;
    request.expected_assignment = Some(observed);
    assert_eq!(
        api.create_environment(request)
            .await
            .unwrap()
            .into_inner()
            .record,
        Some(record.clone())
    );
    let mut expired = create("expired");
    expired.create_deadline_unix_millis = 1;
    assert_eq!(
        api.create_environment(expired).await.unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    assert!(matches!(
        session.get("expired").await,
        Err(adx_core::Error::NotFound)
    ));
    assert_eq!(backend.1.load(Ordering::SeqCst), 1);
    // Release the successful control execution, then expire a different create
    // while its backend is preparing. The node's phase must survive RPC timers.
    let mut node_api =
        pb::node_service_client::NodeServiceClient::new(connect(na, Principal::ApiServer).await);
    node_api
        .delete_environment(pb::DeleteEnvironmentRequest {
            assignment: record.assignment.clone(),
            caller: caller(),
            node_session_id: String::new(),
            failed_revision: None,
        })
        .await
        .unwrap();
    let mut timed = create("timeout-cold");
    timed.create_deadline_unix_millis =
        adx_transport::deadline::Deadline::after(Duration::from_secs(1))
            .unwrap()
            .unix_millis();
    let error = api.create_environment(timed).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded, "{error:?}");
    assert!(
        error
            .message()
            .contains("backend preparation/runtime start"),
        "{error:?}"
    );
    assert!(adx_protocol::deadline_failure(&error).is_some());
    assert_eq!(
        session.get("timeout-cold").await,
        Err(adx_core::Error::NotFound)
    );
    assert_eq!(backend.1.load(Ordering::SeqCst), 2);
    assert_eq!(backend.0.started.load(Ordering::SeqCst), 1);
}
