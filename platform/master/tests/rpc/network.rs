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
            &adx_master::auth::Credential {
                tenant_id: "tenant".into(),
                administrator: false,
                expires_at_unix_seconds: 0,
            },
        )
        .await
        .unwrap();
    let auth = adx_master::auth::AuthRpc::new(session.clone(), Peers::network());
    let rpc = MasterRpc::new(
        session.clone(),
        Placement::Pack,
        Peers::network(),
        RpcClient::network(Principal::Master),
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ma = ml.local_addr().unwrap();
    let mut servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .add_service(pb::master_service_server::MasterServiceServer::new(rpc))
            .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
            .serve_with_incoming(TcpListenerStream::new(ml))
            .await
            .unwrap();
    })]);
    let sink = Arc::new(
        MasterStateSink::with_rpc_channel(
            connect(ma, Principal::Node("node".into())).await,
            Duration::from_secs(2),
        )
        .unwrap()
        .with_session("boot-1".into()),
    );
    let backend = Arc::new(Backend {
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::default(),
    });
    let manager = Arc::new(NodeManager::new(
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
    let mut worker = pb::master_service_client::MasterServiceClient::new(
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
    let mut api = pb::master_service_client::MasterServiceClient::new(api_channel);
    api.create_capsule(create("network-capsule")).await.unwrap();
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    let mut get = pb::GetCapsuleRequest {
        capsule_id: "network-capsule".into(),
        caller: caller(),
    };
    api.get_capsule(get.clone()).await.unwrap();
    get.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        api.get_capsule(get).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    // A bare plaintext RPC channel does not silently acquire an ingress role.
    let raw = tonic::transport::Endpoint::from_shared(format!("http://{ma}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut unknown = pb::master_service_client::MasterServiceClient::new(raw);
    assert_eq!(
        unknown
            .create_capsule(create("unknown"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}
