use super::*;
fn local_peers() -> Peers {
    Peers::new(
        [
            ("master", Principal::Master),
            ("api-server", Principal::ApiServer),
            ("node", Principal::Node("a".into())),
            ("edge", Principal::Node("b".into())),
        ]
        .map(|(cert, role)| (file(&format!("{cert}.der")), role)),
    )
}
struct Rig {
    _redis: common::Redis,
    _servers: Servers,
    session: Session,
    rpc: MasterRpc,
    master: pb::master_service_client::MasterServiceClient<Channel>,
    nodes: Vec<pb::node_service_client::NodeServiceClient<Channel>>,
    claimants: Vec<pb::master_service_client::MasterServiceClient<Channel>>,
    managers: Vec<Arc<NodeManager>>,
    backends: Vec<Arc<Backend>>,
    node_rpcs: Vec<NodeRpc>,
    address: std::net::SocketAddr,
}
impl Rig {
    async fn new() -> Self {
        let redis = common::Redis::new().await;
        let session = redis.store().await.begin(1).await.unwrap();
        let rpc = MasterRpc::with_heartbeat_timeout(
            session.clone(),
            Placement::Pack,
            local_peers(),
            client_tls("master"),
            Duration::from_secs(3),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ma = ml.local_addr().unwrap();
        let auth = adx_master::auth::AuthRpc::new(session.clone(), local_peers());
        let service = rpc.clone();
        let mut servers = Servers(vec![tokio::spawn(async move {
            Server::builder()
                .tls_config(server_tls("master"))
                .unwrap()
                .add_service(pb::master_service_server::MasterServiceServer::new(service))
                .add_service(pb::auth_service_server::AuthServiceServer::new(auth))
                .serve_with_incoming(TcpListenerStream::new(ml))
                .await
                .unwrap();
        })]);
        let mut node_rpcs = vec![];
        let mut managers = vec![];
        let mut backends = vec![];
        let mut nodes = vec![];
        let mut claimants = vec![];
        for (id, cert) in [("a", "node"), ("b", "edge")] {
            let client = channel(ma, cert).await;
            let sink = Arc::new(
                MasterStateSink::new(client.clone(), Duration::from_secs(2))
                    .unwrap()
                    .with_session(format!("boot-{id}")),
            );
            let backend = Arc::new(Backend {
                session: session.clone(),
                started: AtomicUsize::new(0),
                removed: AtomicUsize::new(0),
                running: Mutex::default(),
            });
            let manager = Arc::new(NodeManager::new(
                id.into(),
                backend.clone(),
                Arc::new(LocalChecks),
                Arc::new(LocalChecks),
                sink.clone(),
            ));
            manager
                .update_capacity(spec("x").resources, Duration::from_secs(60))
                .unwrap();
            let node = NodeRpc::new(manager.clone(), local_peers(), format!("boot-{id}"))
                .with_local_creation((*sink).clone());
            node_rpcs.push(node.clone());
            let nl = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let na = nl.local_addr().unwrap();
            servers.0.push(tokio::spawn(async move {
                Server::builder()
                    .tls_config(server_tls(cert))
                    .unwrap()
                    .add_service(pb::node_service_server::NodeServiceServer::new(node))
                    .serve_with_incoming(TcpListenerStream::new(nl))
                    .await
                    .unwrap();
            }));
            let mut master = pb::master_service_client::MasterServiceClient::new(client);
            let mut register = pb::RegisterNodeRequest {
                node_id: id.into(),
                node_address: na.to_string(),
                proxy_address: "127.0.0.1:9999".into(),
                capacity: Some(spec("x").resources.into()),
                accepting_allocations: false,
                labels: Default::default(),
                devices: vec![],
                session_id: format!("boot-{id}"),
                heartbeat_sequence: 1,
                reconciling: true,
            };
            master.register_node(register.clone()).await.unwrap();
            master
                .inspect_node(pb::InspectNodeRequest {
                    node_id: id.into(),
                    session_id: format!("boot-{id}"),
                })
                .await
                .unwrap();
            register.heartbeat_sequence = 2;
            register.reconciling = false;
            register.accepting_allocations = true;
            master.register_node(register).await.unwrap();
            managers.push(manager);
            backends.push(backend);
            claimants.push(master);
            nodes.push(pb::node_service_client::NodeServiceClient::new(
                channel(na, "api-server").await,
            ));
        }
        Self {
            _redis: redis,
            _servers: servers,
            session,
            rpc,
            master: pb::master_service_client::MasterServiceClient::new(
                channel(ma, "api-server").await,
            ),
            nodes,
            claimants,
            managers,
            backends,
            node_rpcs,
            address: ma,
        }
    }
    fn request(id: &str, index: usize) -> pb::LocalCreateRequest {
        pb::LocalCreateRequest {
            create: Some(create(id)),
            node_session_id: format!("boot-{}", if index == 0 { "a" } else { "b" }),
        }
    }
    async fn delete(&mut self, record: &pb::InstanceRecord) {
        let owner = record.assignment.as_ref().unwrap();
        let index = usize::from(owner.node_id == "b");
        self.nodes[index]
            .delete_instance(pb::DeleteInstanceRequest {
                assignment: Some(owner.clone()),
                caller: caller(),
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn concurrent_local_entries_converge_and_fallback_preserves_both_ledgers() {
    let mut rig = Rig::new().await;
    let mut a = rig.nodes[0].clone();
    let mut b = rig.nodes[1].clone();
    let (one, two) = tokio::join!(
        a.create_local_instance(Rig::request("same", 0)),
        b.create_local_instance(Rig::request("same", 1))
    );
    let one = one.unwrap().into_inner();
    assert_eq!(one, two.unwrap().into_inner());
    let record = one.record.unwrap();
    assert_eq!(
        rig.backends
            .iter()
            .map(|b| b.started.load(Ordering::SeqCst))
            .sum::<usize>(),
        1
    );
    assert_eq!(
        rig.managers
            .iter()
            .map(|m| m.used().cpu_millis)
            .sum::<u64>(),
        100
    );
    assert_eq!(rig.session.snapshot().await.unwrap().instances.len(), 1);
    let replay = a
        .create_local_instance(Rig::request("same", 0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(replay.record.as_ref(), Some(&record));
    let mut changed = Rig::request("same", 1);
    changed
        .create
        .as_mut()
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .image = "different".into();
    assert!(b.create_local_instance(changed).await.is_err());
    assert_eq!(
        rig.managers
            .iter()
            .map(|m| m.used().cpu_millis)
            .sum::<u64>(),
        100
    );
    // Full local node forwards to the center; center sees the confirmed first hold.
    let index = usize::from(record.assignment.as_ref().unwrap().node_id == "b");
    let second = rig.nodes[index]
        .create_local_instance(Rig::request("fallback", index))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_ne!(
        second.assignment.as_ref().unwrap().node_id,
        record.assignment.as_ref().unwrap().node_id
    );
    assert_eq!(
        rig.managers
            .iter()
            .map(|m| m.used().cpu_millis)
            .sum::<u64>(),
        200
    );
    let metrics = rig.rpc.metrics().await.unwrap();
    assert!(
        metrics.contains("adx_master_node_reserved_cpu_millis{shard_id=\"0\",node_id=\"a\"} 100")
    );
    assert!(
        metrics.contains("adx_master_node_reserved_cpu_millis{shard_id=\"0\",node_id=\"b\"} 100")
    );
    rig.delete(&record).await;
    rig.delete(&second).await;
    assert!(rig
        .managers
        .iter()
        .all(|n| n.used() == Resources::default()));
    // Hard placement rejects local a and falls back to b, regardless of Pack scoring.
    let mut placed = Rig::request("placed", 0);
    placed
        .create
        .as_mut()
        .unwrap()
        .spec
        .as_mut()
        .unwrap()
        .scheduling = Some(pb::SchedulingPolicy {
        required_node: vec![pb::LabelSelector {
            match_labels: [("NODE_ID".into(), "b".into())].into(),
            ..Default::default()
        }],
        ..Default::default()
    });
    let third = a
        .create_local_instance(placed)
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(third.assignment.as_ref().unwrap().node_id, "b");
    assert_eq!(rig.managers[0].used(), Resources::default());
    rig.delete(&third).await;
    let mut center = rig.master.clone();
    let (central, local) = tokio::join!(
        center.create_instance(create("mixed")),
        a.create_local_instance(Rig::request("mixed", 0))
    );
    let central = central.unwrap().into_inner();
    assert_eq!(central, local.unwrap().into_inner());
    assert_eq!(
        rig.managers
            .iter()
            .map(|m| m.used().cpu_millis)
            .sum::<u64>(),
        100
    );
    rig.delete(&central.record.unwrap()).await;
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn claim_rpc_auth_session_directory_and_real_delayed_redis_write() {
    let mut rig = Rig::new().await;
    let claim = pb::ClaimInstanceRequest {
        spec: Some(spec("delayed").into()),
        caller: caller(),
        node_session_id: "boot-a".into(),
        devices: vec![],
    };
    assert_eq!(
        rig.master
            .claim_instance(claim.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut stale = claim.clone();
    stale.node_session_id = "old".into();
    assert!(rig.claimants[0].claim_instance(stale).await.is_err());
    let mut forged = claim.clone();
    forged.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        rig.claimants[0]
            .claim_instance(forged)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        rig.claimants[0]
            .watch_nodes(pb::WatchNodesRequest {})
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut directory = rig
        .master
        .watch_nodes(pb::WatchNodesRequest {})
        .await
        .unwrap()
        .into_inner();
    let frame = directory.message().await.unwrap().unwrap();
    assert_eq!(frame.nodes.len(), 2);
    // Delay actual Redis writes beyond the store timeout. The first claim has an
    // unknown outcome; the retry must fence/reload, not create another owner.
    let before = rig.session.snapshot().await.unwrap().revision;
    let mut control = redis::Client::open(rig._redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(500)
        .arg("WRITE")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    let result = rig.nodes[0]
        .create_local_instance(Rig::request("delayed", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert!(rig.session.snapshot().await.unwrap().revision >= before + 3); // claim + barrier + result
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 1);
    assert_eq!(
        rig.session
            .get("delayed")
            .await
            .unwrap()
            .assignment
            .generation,
        1
    );
    assert_eq!(rig.managers[0].used().cpu_millis, 100);
    assert!(rig
        .rpc
        .metrics()
        .await
        .unwrap()
        .contains("node_id=\"a\"} 100"));
    rig.delete(&result).await;
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn abandoned_unknown_claim_is_retried_and_shared_hold_converges() {
    let mut rig = Rig::new().await;
    let mut control = redis::Client::open(rig._redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(1500)
        .arg("WRITE")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    let error = rig.nodes[0]
        .create_local_instance(Rig::request("abandoned", 0))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(rig.managers[0].used().cpu_millis, 100);
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 0);
    tokio::time::sleep(Duration::from_millis(1600)).await;
    // The periodic Master worker can heal storage even without another create.
    assert_eq!(rig.rpc.expire_nodes().await.unwrap(), 0);
    // No HTTP or Node RPC retry: the production background mechanism converges.
    rig.node_rpcs[0].retry_local_claims().await;
    let stored = rig.session.get("abandoned").await.unwrap();
    assert_eq!(stored.assignment.generation, 1);
    assert_eq!(
        stored.result.as_ref().unwrap().state,
        adx_core::InstanceState::Running
    );
    let mut one = rig.nodes[0].clone();
    let mut two = rig.nodes[0].clone();
    let (one, two) = tokio::join!(
        one.create_local_instance(Rig::request("abandoned", 0)),
        two.create_local_instance(Rig::request("abandoned", 0))
    );
    assert_eq!(one.unwrap().into_inner(), two.unwrap().into_inner());
    assert_eq!(rig.backends[0].started.load(Ordering::SeqCst), 1);
    assert_eq!(rig.managers[0].used().cpu_millis, 100);
    rig.delete(&stored.result.unwrap().try_into().unwrap())
        .await;
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn central_assignment_waits_for_unconfirmed_local_capacity_without_reassignment() {
    let mut rig = Rig::new().await;
    rig.managers[0].reserve_local(&spec("tentative")).unwrap();
    let mut request = create("central");
    request.spec.as_mut().unwrap().scheduling = Some(pb::SchedulingPolicy {
        required_node: vec![pb::LabelSelector {
            match_labels: [("NODE_ID".into(), "a".into())].into(),
            ..Default::default()
        }],
        ..Default::default()
    });
    let mut master = rig.master.clone();
    let task = tokio::spawn(async move { master.create_instance(request).await });
    let assignment = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(record) = rig.session.get("central").await {
                break record.assignment;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Claim cannot fit Master's committed allocation; it releases only the local
    // token and forwards to b. The center retries a with the original generation.
    let local = rig.nodes[0]
        .create_local_instance(Rig::request("tentative", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let center = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    assert_eq!(
        center.assignment.as_ref().unwrap().generation,
        assignment.generation
    );
    assert_eq!(center.assignment.as_ref().unwrap().node_id, "a");
    assert_eq!(local.assignment.as_ref().unwrap().node_id, "b");
    assert_eq!(rig.managers[0].used().cpu_millis, 100);
    assert_eq!(rig.managers[1].used().cpu_millis, 100);
    rig.delete(&local).await;
    rig.delete(&center).await;
}

#[tokio::test]
#[ignore = "requires real Redis, mTLS certificates and API Server binary for HTTPS"]
async fn local_first_https_directory_round_robin_and_concurrent_creation() {
    let Ok(api_binary) = std::env::var("ADX_TEST_API_SERVER") else {
        return;
    };
    let mut rig = Rig::new().await;
    for (key, tenant) in [("a", "tenant"), ("b", "other")] {
        rig.session
            .bootstrap_credential(
                &key.repeat(40),
                &adx_master::auth::Credential {
                    tenant_id: tenant.into(),
                    administrator: false,
                    expires_at_unix_seconds: 0,
                },
            )
            .await
            .unwrap();
    }
    for i in 0..2 {
        let capacity = Resources {
            cpu_millis: 1000,
            memory_bytes: 8 * 1024 * 1024,
            disk_bytes: 0,
        };
        rig.managers[i]
            .update_capacity(capacity, Duration::from_secs(60))
            .unwrap();
        let node = &rig.session.snapshot().await.unwrap().nodes[if i == 0 { "a" } else { "b" }];
        rig.claimants[i]
            .register_node(pb::RegisterNodeRequest {
                node_id: node.node.id.clone(),
                node_address: node.address.clone(),
                proxy_address: node.proxy_address.clone(),
                capacity: Some(capacity.into()),
                session_id: node.session.as_ref().unwrap().id.clone(),
                heartbeat_sequence: 3,
                accepting_allocations: true,
                reconciling: false,
                labels: Default::default(),
                devices: vec![],
            })
            .await
            .unwrap();
    }
    let directory = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let tls = PathBuf::from(std::env::var("ADX_TEST_TLS_DIR").unwrap());
    let config = serde_json::json!({
        "listen": address.to_string(), "master_address": format!("https://{}", rig.address),
        "create_mode": "local_first", "ca": tls.join("ca.pem"),
        "certificate": tls.join("api-server.pem"), "private_key": tls.join("api-server.key"),
        "server_name": "localhost", "rpc_timeout_seconds": 5,
        "cache_ttl_seconds": 30, "cache_entries": 128, "auth_cache_ttl_seconds": 1
    });
    let path = directory.path().join("api.json");
    std::fs::write(&path, serde_json::to_vec(&config).unwrap()).unwrap();
    let evidence = PathBuf::from(std::env::var("ADX_TEST_EVIDENCE").unwrap());
    let log = std::fs::File::create(evidence.join("local-first-api.log")).unwrap();
    let mut process = tokio::process::Command::new(api_binary)
        .args(["--config", path.to_str().unwrap()])
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let script =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../build/ci/local_first_http.py");
    let result = tokio::process::Command::new("python3")
        .arg(script)
        .env("ADX_TEST_API_ENDPOINT", format!("https://{address}"))
        .output()
        .await
        .unwrap();
    std::fs::write(
        evidence.join("local-first-http.log"),
        [result.stdout, result.stderr.clone()].concat(),
    )
    .unwrap();
    process.kill().await.unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        rig.session
            .get("lf-first")
            .await
            .unwrap()
            .assignment
            .node_id,
        "a"
    );
    assert_eq!(
        rig.session
            .get("lf-second")
            .await
            .unwrap()
            .assignment
            .node_id,
        "b"
    );
    assert_eq!(
        rig.backends
            .iter()
            .map(|b| b.started.load(Ordering::SeqCst))
            .sum::<usize>(),
        3
    );
    assert!(rig
        .managers
        .iter()
        .all(|m| m.used() == Resources::default()));
    assert!(rig
        .session
        .snapshot()
        .await
        .unwrap()
        .routes()
        .unwrap()
        .is_empty());
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn cancelled_local_rpc_finishes_once_and_retry_reuses_the_owner() {
    let mut rig = Rig::new().await;
    let mut control = redis::Client::open(rig._redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(150)
        .arg("WRITE")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    let mut request = Request::new(Rig::request("cancelled", 0));
    request.set_timeout(Duration::from_millis(40));
    assert!(rig.nodes[0].create_local_instance(request).await.is_err());
    let record = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Ok(stored) = rig.session.get("cancelled").await {
                if let Some(record) = stored.result {
                    break record;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let retry = rig.nodes[1]
        .create_local_instance(Rig::request("cancelled", 1))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let original: pb::InstanceRecord = record.try_into().unwrap();
    assert_eq!(retry, original);
    assert_eq!(
        rig.backends
            .iter()
            .map(|b| b.started.load(Ordering::SeqCst))
            .sum::<usize>(),
        1
    );
    assert_eq!(rig.managers[1].used(), Resources::default());
    rig.delete(&retry).await;
}

#[tokio::test]
#[ignore = "requires real Redis and generated mTLS certificates"]
async fn existing_terminal_claim_reconciles_a_late_commit_into_scheduler_accounting() {
    let mut rig = Rig::new().await;
    let running = rig.nodes[0]
        .create_local_instance(Rig::request("late-delete", 0))
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let mut terminal: InstanceRecord = running.clone().try_into().unwrap();
    terminal.state = adx_core::InstanceState::Deleted;
    terminal.resources_held = false;
    terminal.runtime_ip = None;
    terminal.revision += 2;
    let mut control = redis::Client::open(rig._redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(500)
        .arg("WRITE")
        .query_async::<()>(&mut control)
        .await
        .unwrap();
    assert!(rig.nodes[0]
        .delete_instance(pb::DeleteInstanceRequest {
            assignment: running.assignment,
            caller: caller(),
        })
        .await
        .is_err());
    assert_eq!(rig.backends[0].removed.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(600)).await;
    // Fault injection: complete the already-cleaned result directly in storage,
    // without the coordinator observing it. CLIENT PAUSE alone may discard a
    // command when its connection closes, so it does not prove late application.
    rig.session.commit(terminal).await.unwrap();
    assert_eq!(
        rig.session
            .get("late-delete")
            .await
            .unwrap()
            .result
            .unwrap()
            .state,
        adx_core::InstanceState::Deleted
    );
    assert!(rig.nodes[1]
        .create_local_instance(Rig::request("late-delete", 1))
        .await
        .is_err());
    assert!(rig
        .managers
        .iter()
        .all(|m| m.used() == Resources::default()));
    let metrics = rig.rpc.metrics().await.unwrap();
    assert!(
        metrics.contains("adx_master_node_reserved_cpu_millis{shard_id=\"0\",node_id=\"a\"} 0"),
        "{metrics}"
    );
}
