//! Verify recovery through real registration and Node upgrade RPCs while admission stays closed.
use super::{
    process_upgrade::{management, serve_handoff, serve_management},
    *,
};
use adx_coordinator::storage::{NodeSession, UpgradeOperation};
use adx_core::Assignment;
use serde_json::json;

#[derive(Default)]
struct EmptyRuntime(AtomicUsize);

#[async_trait::async_trait]
impl RuntimeDriver for EmptyRuntime {
    fn runtime_rootfs_identity(&self) -> Option<adxlet::RuntimeRootfsIdentity> {
        Some(adxlet::RuntimeRootfsIdentity {
            path: "/runtime/rootfs".into(),
            sha256: "c".repeat(64),
        })
    }

    async fn inventory(&self) -> Result<Vec<adxlet::RuntimeObservation>> {
        // Exercise actual unknown/nonempty inspection results without changing Coordinator state.
        match self.0.load(Ordering::SeqCst) {
            1 => Err(adx_core::Error::Unavailable("inventory unavailable".into())),
            2 => Ok(vec![adxlet::RuntimeObservation {
                environment_id: "orphan".into(),
                runtime_id: "orphan-1".into(),
                generation: 1,
                tenant_id: "tenant".into(),
                running: true,
            }]),
            _ => Ok(vec![]),
        }
    }

    async fn start(
        &self,
        _: &EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        Err(adx_core::Error::Unavailable(
            "empty-node fixture cannot start executions".into(),
        ))
    }

    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(false)
    }

    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

async fn manager(
    coordinator: std::net::SocketAddr,
    mirror: &std::path::Path,
    session: &str,
    runtime: Arc<EmptyRuntime>,
) -> Arc<Adxlet> {
    let node = Adxlet::new(
        "node".into(),
        runtime,
        Arc::new(LocalChecks),
        Arc::new(LocalChecks),
        Arc::new(
            CoordinatorStateSink::new(channel(coordinator, "node").await, Duration::from_secs(3))
                .unwrap()
                .with_session(session.into()),
        ),
    )
    .with_upgrade_control(mirror.into(), session.into())
    .unwrap();
    node.update_capacity(spec("unused").resources, Duration::from_secs(60))
        .unwrap();
    Arc::new(node)
}

async fn register_reconciled(
    client: &mut pb::coordinator_service_client::CoordinatorServiceClient<Channel>,
    node: &Adxlet,
    report: &mut pb::RegisterNodeRequest,
) {
    let initial = client
        .register_node(report.clone())
        .await
        .unwrap()
        .into_inner();
    if let Some(control) = initial.upgrade_control {
        node.apply_upgrade_control(control).await.unwrap();
    }
    let catalog = client
        .inspect_node(pb::InspectNodeRequest {
            node_id: report.node_id.clone(),
            session_id: report.session_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    node.reconcile_retained(
        catalog
            .records
            .into_iter()
            .map(|record| record.try_into().unwrap())
            .collect(),
        catalog
            .snapshots
            .into_iter()
            .map(|snapshot| snapshot.try_into().unwrap())
            .collect(),
        catalog
            .retained_artifacts
            .into_iter()
            .map(|artifact| artifact.try_into().unwrap())
            .collect(),
    )
    .await
    .unwrap();
    report.heartbeat_sequence += 1;
    report.reconciling = false;
    report.accepting_allocations = node.accepting_allocations();
    client.register_node(report.clone()).await.unwrap();
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates"]
async fn pod_replacement_verifies_without_reopening_allocation_admission() {
    for admin_paused in [false, true] {
        Box::pin(replacement(admin_paused, false)).await;
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates"]
async fn abort_cleanup_releases_empty_replacement_without_recovery_profile() {
    for admin_paused in [false, true] {
        Box::pin(replacement(admin_paused, true)).await;
    }
}

async fn replacement(admin_paused: bool, abort_cleanup: bool) {
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::with_heartbeat_timeout(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(3),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    let mut servers = Servers(vec![]);
    let coordinator = serve_handoff(rpc.clone(), &mut servers).await;
    let http = serve_management(rpc, &mut servers).await;
    let mirror = tempfile::tempdir().unwrap();
    let runtime = Arc::new(EmptyRuntime::default());
    let old = manager(
        coordinator,
        &mirror.path().join("upgrade.json"),
        "old",
        runtime.clone(),
    )
    .await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut node_server =
        serve_lifecycle_node(listener, NodeRpc::new(old.clone(), peers(), "old".into()));
    let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(coordinator, "node").await,
    );
    let mut labels = recovery_profile::qualify(&mut spec("unused"));
    labels.insert("ADX_CHECKPOINT_SCOPE".into(), "scope".into());
    if abort_cleanup {
        labels.retain(|key, _| !key.starts_with("ADX_RECOVERY_"));
    }
    let mut report = pb::RegisterNodeRequest {
        node_id: "node".into(),
        node_address: address.to_string(),
        proxy_address: "127.0.0.1:9999".into(),
        capacity: Some(spec("unused").resources.into()),
        runtime_classes: vec!["runc".into()],
        labels: labels.into_iter().collect(),
        checkpoint_scopes: vec!["scope".into()],
        session_id: "old".into(),
        heartbeat_sequence: 1,
        reconciling: true,
        ..Default::default()
    };
    register_reconciled(&mut client, &old, &mut report).await;
    if abort_cleanup {
        let mut peer = session.snapshot().await.unwrap().nodes["node"].node.clone();
        peer.id = "peer".into();
        peer.labels.insert("NODE_ID".into(), "peer".into());
        let peer = session
            .register_session(
                peer,
                "peer:1".into(),
                "peer:2".into(),
                Some(NodeSession {
                    id: "peer-session".into(),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
        let pending = session
            .reserve(
                spec("uncertain"),
                Assignment {
                    environment_id: "uncertain".into(),
                    node_id: "peer".into(),
                    shard_id: peer.shard_id,
                    generation: 1,
                    devices: vec![],
                },
            )
            .await
            .unwrap();
        assert!(pending.result.is_none());
    }
    client
        .set_node_scheduling(pb::SetNodeSchedulingRequest {
            node_id: "node".into(),
            accepting_allocations: !admin_paused,
        })
        .await
        .unwrap();
    let base = "/v1/node-upgrades/tasks";
    let begin = json!({"task_id":"pod","epoch":1,"namespace":"test","daemonset_uid":"workload",
        "target_image":"registry:5000/image:release-20261124-1","nodes":[{"node_id":"node","pod_uid":"old-pod","node_session_id":"old"}]});
    let result = management(http, base, begin.clone()).await;
    assert_eq!(result.0, 200, "{result:?}");
    assert_eq!(result.1["target_image"], begin["target_image"]);
    let replay = management(http, base, begin.clone()).await;
    assert_eq!(replay.0, 200, "{replay:?}");
    assert_eq!(replay.1, result.1);
    let mut changed = begin;
    changed["target_image"] = json!("registry:5000/image:release-20261124-2");
    let conflict = management(http, base, changed).await;
    assert_eq!(conflict.0, 409, "{conflict:?}");
    let mut request = json!({"task_id":"pod","epoch":1,"operation_id":"prepare", "node_id":"node",
        "pod_uid":"old-pod","node_session_id":"old"});
    let path = format!("{base}/pod");
    assert_eq!(
        management(http, &format!("{path}/prepare-node"), request.clone())
            .await
            .0,
        200
    );
    request["operation_id"] = json!("quiesce-old");
    let result = management(http, &format!("{path}/quiesce"), request.clone()).await;
    assert_eq!(result.0, 200, "{result:?}");
    assert_eq!(result.1["strict_empty"], true);
    request["operation_id"] = json!("delete");
    request["permit_id"] = result.1["permit_id"].clone();
    request["delete_intent"] = json!(true);
    request["pod_resource_version"] = json!("old-rv");
    assert_eq!(
        management(http, &format!("{path}/quiesce"), request.clone())
            .await
            .0,
        200
    );
    client
        .unregister_node(pb::UnregisterNodeRequest {
            node_id: "node".into(),
            session_id: "old".into(),
        })
        .await
        .unwrap();
    let departed = session.snapshot().await.unwrap().nodes["node"].clone();
    assert!(departed.checkpoint_gc_fenced);
    assert!(!departed.session.unwrap().routable);
    node_server.abort();
    let _ = node_server.await;

    let new = manager(
        coordinator,
        &mirror.path().join("upgrade.json"),
        "new",
        runtime.clone(),
    )
    .await;
    node_server = serve_lifecycle_node(
        TcpListener::bind(address).await.unwrap(),
        NodeRpc::new(new.clone(), peers(), "new".into()),
    );
    report.session_id = "new".into();
    report.heartbeat_sequence = 1;
    report.reconciling = true;
    report.accepting_allocations = false;
    register_reconciled(&mut client, &new, &mut report).await;
    assert!(
        !report.accepting_allocations,
        "replacement must remain locally isolated"
    );
    let recovered = session.snapshot().await.unwrap().nodes["node"].clone();
    assert!(recovered.scheduling_paused);
    assert!(!recovered.node.available);
    assert!(recovered.session.unwrap().routable);
    assert!(!recovered.checkpoint_gc_fenced);
    let owner = recovered.upgrade_pause.unwrap();
    assert_eq!(
        (owner.task_id.as_str(), owner.epoch, owner.original_paused),
        ("pod", 1, admin_paused)
    );

    request["operation_id"] = json!("quiesce-new");
    request["pod_uid"] = json!("new-pod");
    request["node_session_id"] = json!("new");
    request["delete_intent"] = json!(false);
    let result = management(http, &format!("{path}/quiesce"), request.clone()).await;
    assert_eq!(result.0, 200, "{result:?}");
    assert_eq!(result.1["strict_empty"], true);
    assert!(!new.accepting_allocations());
    request["operation_id"] = json!("verify");
    request["permit_id"] = result.1["permit_id"].clone();
    let verify = format!("{path}/verify-node");

    // Normal registration cannot bypass actual checkpoint recovery capability.
    report.heartbeat_sequence += 1;
    report.checkpoint_scopes.clear();
    client.register_node(report.clone()).await.unwrap();
    let rejected = management(http, &verify, request.clone()).await;
    assert_eq!(rejected.0, 400, "{rejected:?}");
    assert!(
        rejected
            .1
            .to_string()
            .contains("node recovery capabilities are unavailable or unverified"),
        "{rejected:?}"
    );
    assert!(session.snapshot().await.unwrap().nodes["node"]
        .upgrade_pause
        .is_some());
    assert!(!new.accepting_allocations());
    report.heartbeat_sequence += 1;
    report.checkpoint_scopes.push("scope".into());
    client.register_node(report.clone()).await.unwrap();
    if abort_cleanup {
        for rollback in [false, true] {
            request["rollback"] = json!(rollback);
            assert_eq!(management(http, &verify, request.clone()).await.0, 400);
        }
        let frozen = management(
            http,
            &format!("{path}/abort"),
            json!({
                "task_id":"pod", "epoch":1, "operation_id":"freeze", "stage":"freeze",
            }),
        )
        .await;
        assert_eq!(frozen.0, 200, "{frozen:?}");
        assert_eq!(frozen.1["phase"], "Aborting");
        let mut ordinary = request.clone();
        ordinary["rollback"] = json!(false);
        assert_eq!(management(http, &verify, ordinary).await.0, 409);
        let mut task = session.upgrade_task().await.unwrap().unwrap();
        let revision = task.revision;
        task.operations.insert(
            "uncertain".into(),
            UpgradeOperation {
                environment_id: "uncertain".into(),
                source_node_id: "peer".into(),
                target_node_id: "node".into(),
                automatic_target: false,
                source_generation: 1,
                source_revision: 0,
                source_stopped_revision: None,
                source_session_id: "peer-session".into(),
                target_session_id: "new".into(),
                was_running: true,
                phase: "Assigned".into(),
            },
        );
        session.save_upgrade(task, Some(revision)).await.unwrap();
        let unsettled = management(http, &verify, request.clone()).await;
        assert_eq!(unsettled.0, 409, "{unsettled:?}");
        assert!(session.snapshot().await.unwrap().nodes["node"]
            .upgrade_pause
            .is_some());
        let mut task = session.upgrade_task().await.unwrap().unwrap();
        let revision = task.revision;
        task.operations.get_mut("uncertain").unwrap().phase = "Cancelled".into();
        session.save_upgrade(task, Some(revision)).await.unwrap();
        for (field, value) in [("permit_id", "wrong"), ("node_session_id", "wrong")] {
            let mut wrong = request.clone();
            wrong[field] = json!(value);
            assert_ne!(management(http, &verify, wrong).await.0, 200);
        }
        for inventory in [1, 2] {
            runtime.0.store(inventory, Ordering::SeqCst);
            assert_ne!(management(http, &verify, request.clone()).await.0, 200);
            assert!(!new.accepting_allocations());
            assert!(session.snapshot().await.unwrap().nodes["node"]
                .upgrade_pause
                .is_some());
        }
        runtime.0.store(0, Ordering::SeqCst);
        report.checkpoint_scopes.clear();
        report.heartbeat_sequence += 1;
        client.register_node(report.clone()).await.unwrap();
        assert_eq!(management(http, &verify, request.clone()).await.0, 400);
        report.checkpoint_scopes.push("scope".into());
        report.heartbeat_sequence += 1;
        client.register_node(report.clone()).await.unwrap();
    }
    let verified = management(http, &verify, request.clone()).await;
    assert_eq!(verified.0, 200, "{verified:?}");
    assert_eq!(verified.1["nodes"]["node"]["phase"], "verified");
    let released = session.snapshot().await.unwrap().nodes["node"].clone();
    assert!(released.upgrade_pause.is_none());
    assert_eq!(released.scheduling_paused, admin_paused);
    if abort_cleanup {
        // Lost cleanup acknowledgement must replay after the Node is already verified.
        let replay = management(http, &verify, request).await;
        assert_eq!(replay.0, 200, "{replay:?}");
        assert_eq!(replay.1["nodes"]["node"]["phase"], "verified");
        assert_eq!(
            session.snapshot().await.unwrap().nodes["node"].scheduling_paused,
            admin_paused
        );
    }
    report.heartbeat_sequence += 1;
    report.accepting_allocations = new.accepting_allocations();
    assert!(report.accepting_allocations);
    client.register_node(report).await.unwrap();
    assert_eq!(
        session.snapshot().await.unwrap().nodes["node"]
            .node
            .available,
        !admin_paused
    );
    let (action, body, phase) = if abort_cleanup {
        (
            "abort",
            json!({"task_id":"pod","epoch":1,"operation_id":"finalize","stage":"finalize","template_restored":true}),
            "Aborted",
        )
    } else {
        (
            "complete",
            json!({"task_id":"pod","epoch":1,"operation_id":"complete"}),
            "Completed",
        )
    };
    let completed = management(http, &format!("{path}/{action}"), body).await;
    assert_eq!(completed.0, 200, "{completed:?}");
    assert_eq!(completed.1["phase"], phase);
    node_server.abort();
}
