use super::*;
use adx_core::snapshots::{Reference, Snapshot, SnapshotState};
use adxlet::checkpoint::{CheckpointStore, ObjectCheckpointStore};

async fn publish_snapshot(session: &Session, store: &ObjectCheckpointStore, id: &str) -> Snapshot {
    let staged = store.allocate().await.unwrap();
    std::fs::write(staged.join("memory"), id.as_bytes()).unwrap();
    let snapshot = Snapshot::new(
        id.into(),
        vec![],
        spec("source-environment"),
        "source".into(),
        "source-environment-1".into(),
        store.publish(&staged).await.unwrap(),
    )
    .unwrap();
    session.publish_snapshot(snapshot).await.unwrap()
}

async fn register_collector(
    client: &mut pb::coordinator_service_client::CoordinatorServiceClient<Channel>,
    address: std::net::SocketAddr,
    scopes: Vec<String>,
    sequence: u64,
) {
    let mut report = pb::RegisterNodeRequest {
        node_id: "target".into(),
        node_address: address.to_string(),
        proxy_address: "target:9002".into(),
        session_id: "target-boot".into(),
        heartbeat_sequence: sequence,
        reconciling: true,
        accepting_allocations: false,
        runtime_classes: vec!["runc".into()],
        capacity: Some(spec("checkpoint").resources.into()),
        checkpoint_scopes: scopes,
        ..Default::default()
    };
    client.register_node(report.clone()).await.unwrap();
    client
        .inspect_node(pb::InspectNodeRequest {
            node_id: report.node_id.clone(),
            session_id: report.session_id.clone(),
        })
        .await
        .unwrap();
    report.heartbeat_sequence += 1;
    report.reconciling = false;
    report.accepting_allocations = true;
    client.register_node(report).await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates; run control-rpc suite"]
async fn delegated_snapshot_gc_requires_durable_source_fence_and_matching_scope() {
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(2).await.unwrap();
    let scope = "shared-s3-scope".to_owned();
    let remote = Arc::new(object_store::memory::InMemory::new());
    let source_dir = tempfile::tempdir().unwrap();
    let source_store = ObjectCheckpointStore::new(
        "shared".into(),
        scope.clone(),
        remote.clone(),
        "checkpoints".into(),
        source_dir.path().into(),
        1024,
    )
    .unwrap();
    let foreign_dir = tempfile::tempdir().unwrap();
    let foreign_store = ObjectCheckpointStore::new(
        "shared".into(),
        "foreign-scope".into(),
        remote.clone(),
        "checkpoints".into(),
        foreign_dir.path().into(),
        1024,
    )
    .unwrap();
    let unfenced = publish_snapshot(&session, &source_store, "unfenced").await;
    let referenced = publish_snapshot(&session, &source_store, "referenced").await;
    let foreign = publish_snapshot(&session, &foreign_store, "foreign").await;
    let restore_reference = Reference::Template {
        node_id: "target".into(),
        template_id: "hold-deletion".into(),
    };
    session
        .acquire_snapshot(&referenced.id, "tenant", restore_reference.clone())
        .await
        .unwrap();
    for snapshot in [&unfenced, &referenced, &foreign] {
        session
            .delete_snapshot(&snapshot.id, "tenant")
            .await
            .unwrap();
    }

    let test_peers = || {
        Peers::new([
            (file("coordinator.der"), Principal::Coordinator),
            (file("node.der"), Principal::Node("source".into())),
            (file("apiserver.der"), Principal::Node("target".into())),
        ])
    };
    let first = CoordinatorRpc::with_heartbeat_timeout(
        session.clone(),
        Placement::Pack,
        test_peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
        Duration::from_millis(500),
    )
    .await
    .unwrap();
    let first_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first_address = first_listener.local_addr().unwrap();
    let serving = first.clone();
    let mut servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(pb::coordinator_service_server::CoordinatorServiceServer::new(serving))
            .serve_with_incoming(TcpListenerStream::new(first_listener))
            .await
            .unwrap();
    })]);

    let target_dir = tempfile::tempdir().unwrap();
    let target_store = Arc::new(
        ObjectCheckpointStore::new(
            "shared".into(),
            scope.clone(),
            remote,
            "checkpoints".into(),
            target_dir.path().into(),
            1024,
        )
        .unwrap(),
    );
    let backend = Arc::new(Backend {
        session: session.clone(),
        started: AtomicUsize::new(0),
        removed: AtomicUsize::new(0),
        running: Mutex::default(),
    });
    let sink = Arc::new(
        CoordinatorStateSink::new(
            channel(first_address, "apiserver").await,
            Duration::from_secs(2),
        )
        .unwrap()
        .with_session("target-boot".into()),
    );
    let manager = Arc::new(
        Adxlet::new(
            "target".into(),
            backend,
            Arc::new(LocalChecks),
            Arc::new(LocalChecks),
            sink,
        )
        .with_checkpointing(target_store.clone(), Arc::new(LocalChecks))
        .unwrap(),
    );
    manager
        .update_capacity(spec("checkpoint").resources, Duration::from_secs(60))
        .unwrap();
    let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target_address = target_listener.local_addr().unwrap();
    let node = NodeRpc::new(manager, test_peers(), "target-boot".into());
    servers.0.push(tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("node"))
            .unwrap()
            .add_service(pb::node_service_server::NodeServiceServer::new(node))
            .serve_with_incoming(TcpListenerStream::new(target_listener))
            .await
            .unwrap();
    }));

    let mut source = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(first_address, "node").await,
    );
    let mut source_report = pb::RegisterNodeRequest {
        node_id: "source".into(),
        node_address: "127.0.0.1:9001".into(),
        proxy_address: "source:9002".into(),
        session_id: "source-boot".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        accepting_allocations: false,
        runtime_classes: vec!["runc".into()],
        capacity: Some(spec("checkpoint").resources.into()),
        checkpoint_scopes: vec![scope.clone()],
        ..Default::default()
    };
    source.register_node(source_report.clone()).await.unwrap();
    source
        .inspect_node(pb::InspectNodeRequest {
            node_id: source_report.node_id.clone(),
            session_id: source_report.session_id.clone(),
        })
        .await
        .unwrap();
    source_report.heartbeat_sequence += 1;
    source_report.reconciling = false;
    source_report.accepting_allocations = true;
    source.register_node(source_report.clone()).await.unwrap();

    let mut target = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(first_address, "apiserver").await,
    );
    register_collector(&mut target, target_address, vec![scope.clone()], 1).await;
    // Deleted cleanup has durable stop authority. A live source reconfigured
    // without the old scope must not starve an eligible delegated collector.
    let staged = source_store.allocate().await.unwrap();
    std::fs::write(staged.join("memory"), b"retired-checkpoint").unwrap();
    let artifact = source_store.publish(&staged).await.unwrap();
    let execution = spec("retired-checkpoint");
    let assignment = adx_core::Assignment {
        environment_id: execution.id.clone(),
        node_id: "source".into(),
        shard_id: 0,
        generation: session.snapshot().await.unwrap().generation + 1,
        devices: vec![],
    };
    session
        .reserve(execution.clone(), assignment.clone())
        .await
        .unwrap();
    let mut record = EnvironmentRecord {
        spec: execution,
        assignment,
        revision: 2,
        state: adx_core::EnvironmentState::Running,
        runtime: adx_core::Runtime {
            id: "retired-checkpoint-1".into(),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: None,
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
        failure: None,
    };
    record.runtime.id = format!("{}-{}", record.spec.id, record.assignment.generation);
    record.checkpoint = Some(adx_core::RestorePoint {
        id: "retired".into(),
        artifact: artifact.clone(),
        origin: None,
        expires_at_unix_seconds: Some(1),
        source_runtime_id: record.runtime.id.clone(),
    });
    session.commit(record.clone()).await.unwrap();
    record.revision += 1;
    record.state = adx_core::EnvironmentState::Deleted;
    record.resources_held = false;
    record.runtime.ip = None;
    record.checkpoint = None;
    session.commit(record).await.unwrap();
    source_report.heartbeat_sequence += 1;
    source_report.checkpoint_scopes = vec!["reconfigured-scope".into()];
    source.register_node(source_report.clone()).await.unwrap();
    assert_eq!(first.collect_checkpoint_cleanups().await.unwrap(), 1);
    assert!(session.checkpoint_cleanups().await.unwrap().is_empty());
    assert!(target_store.materialize(&artifact).await.is_err());
    source_report.checkpoint_scopes = vec![scope.clone()];
    // A source that merely re-enters reconciliation is not a durable fence.
    source_report.heartbeat_sequence += 1;
    source_report.reconciling = true;
    source_report.accepting_allocations = false;
    source.register_node(source_report).await.unwrap();
    assert_eq!(first.collect_snapshots().await.unwrap(), 0);
    assert_eq!(
        session.get_snapshot(&unfenced.id).await.unwrap().state,
        SnapshotState::Deleting
    );
    assert!(source_store.materialize(&unfenced.artifact).await.is_ok());

    tokio::time::sleep(Duration::from_millis(600)).await;
    register_collector(&mut target, target_address, vec![scope.clone()], 10).await;
    assert_eq!(first.expire_nodes().await.unwrap(), 1);
    assert!(
        session
            .snapshot()
            .await
            .unwrap()
            .nodes
            .get("source")
            .unwrap()
            .checkpoint_gc_fenced
    );

    register_collector(&mut target, target_address, vec!["wrong-scope".into()], 20).await;
    assert_eq!(first.collect_snapshots().await.unwrap(), 0);
    assert!(source_store.materialize(&unfenced.artifact).await.is_ok());
    assert!(foreign_store.materialize(&foreign.artifact).await.is_ok());

    register_collector(&mut target, target_address, vec![scope], 30).await;
    // The compatible collector removes only the unreferenced, correctly scoped artifact.
    assert_eq!(first.collect_snapshots().await.unwrap(), 1);
    assert_eq!(
        session.get_snapshot(&unfenced.id).await.unwrap().state,
        SnapshotState::Deleted
    );
    assert!(target_store.materialize(&unfenced.artifact).await.is_err());
    assert_eq!(
        session.get_snapshot(&referenced.id).await.unwrap().state,
        SnapshotState::Deleting
    );
    assert!(source_store.materialize(&referenced.artifact).await.is_ok());
    assert_eq!(
        session.get_snapshot(&foreign.id).await.unwrap().state,
        SnapshotState::Deleting
    );
    assert!(foreign_store.materialize(&foreign.artifact).await.is_ok());

    session
        .release_snapshot(&referenced.id, restore_reference)
        .await
        .unwrap();
    assert_eq!(first.collect_snapshots().await.unwrap(), 1);
    assert_eq!(
        session.get_snapshot(&referenced.id).await.unwrap().state,
        SnapshotState::Deleted
    );
    assert!(target_store
        .materialize(&referenced.artifact)
        .await
        .is_err());
    assert_eq!(first.collect_snapshots().await.unwrap(), 0);
}
