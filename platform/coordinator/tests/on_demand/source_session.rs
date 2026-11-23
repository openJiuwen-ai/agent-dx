use super::*;
use serde_json::json;
use tokio::time::{interval_at, timeout, Instant};

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn abort_rebinds_an_empty_late_original_pod_before_release() {
    abort_rebinds_late_original(false).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn abort_replays_late_original_after_verification_and_another_restart() {
    abort_rebinds_late_original(true).await;
}

async fn abort_rebinds_late_original(restart_after_verify: bool) {
    let rig = Rig::new(Duration::from_secs(2)).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(adx_coordinator::autoscaling::serve(
        listener,
        rig.rpc.clone(),
    ));
    let abort = "tasks/planned-migration/abort";
    post_upgrade(
        address,
        "tasks",
        json!({
            "task_id": "planned-migration", "epoch": 1, "namespace": "test",
            "daemonset_uid": "daemonset", "target_image": format!("image@sha256:{}", "d".repeat(64)),
            "nodes": [{"node_id": "a", "pod_uid": "source-pod", "node_session_id": rig.source.report.session_id}]
        }),
    )
    .await;
    post_upgrade(
        address,
        abort,
        json!({"task_id": "planned-migration", "epoch": 1, "operation_id": "freeze", "stage": "freeze"}),
    )
    .await;
    let period = Duration::from_secs(60);
    let mut late = worker("c", &rig.address, rig.remote.clone(), None).await;
    report(&mut late, &rig.address, period).await.unwrap();
    let previous_session = late.report.session_id.clone();
    let inspection = late.manager.inspect_upgrade().await.unwrap();
    assert_eq!(inspection.phase, "isolated");
    assert!(inspection.strict_empty);

    // A restart between registration and the first release leaves this durable binding.
    let mut task = rig.session.upgrade_task().await.unwrap().unwrap();
    let revision = task.revision;
    task.nodes.insert(
        "c".into(),
        adx_coordinator::storage::UpgradeNode {
            node_id: "c".into(),
            pod_uid: "late-old-pod".into(),
            original_pod_uid: "late-old-pod".into(),
            node_session_id: previous_session.clone(),
            original_session_id: previous_session.clone(),
            control_revision: inspection.revision,
            phase: "pending".into(),
            ..Default::default()
        },
    );
    rig.session
        .save_upgrade(task, Some(revision))
        .await
        .unwrap();
    late.task.abort();
    let _ = (&mut late.task).await;
    let mut replacement = worker(
        "c",
        &rig.address,
        rig.remote.clone(),
        Some(&late.report.node_address),
    )
    .await;
    report(&mut replacement, &rig.address, period)
        .await
        .unwrap();

    let quiesce = "tasks/planned-migration/quiesce";
    let mut request = json!({"task_id": "planned-migration", "epoch": 1, "operation_id": "late-rebind",
        "node_id": "c", "pod_uid": "late-old-pod", "node_session_id": replacement.report.session_id,
        "rollback": true});
    let mut stale = request.clone();
    stale["node_session_id"] = json!(previous_session);
    let (status, error) = upgrade_response(address, "POST", quiesce, stale.to_string()).await;
    assert_eq!(status, 409, "{error}");
    assert!(!replacement.manager.accepting_allocations());
    let permit = post_upgrade(address, quiesce, request.clone()).await;
    assert_eq!(permit["strict_empty"], true);
    assert_eq!(permit["pod_uid"], "late-old-pod");
    assert_eq!(permit["node_session_id"], replacement.report.session_id);
    request["operation_id"] = json!("verify-late");
    request["permit_id"] = permit["permit_id"].clone();
    post_upgrade(address, "tasks/planned-migration/verify-node", request).await;
    let finalize = json!({"task_id": "planned-migration", "epoch": 1, "operation_id": "finalize",
        "stage": "finalize", "template_restored": true});
    if restart_after_verify {
        let verified_session = replacement.report.session_id.clone();
        let (assignment, mut coordinator) =
            create_paused_on_late_original(&rig, &replacement).await;
        replacement.task.abort();
        let _ = (&mut replacement.task).await;
        let mut restarted = worker(
            "c",
            &rig.address,
            rig.remote.clone(),
            Some(&replacement.report.node_address),
        )
        .await;
        // The new session cannot adopt a paused controller before release.
        assert_eq!(
            report(&mut restarted, &rig.address, period)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unavailable
        );
        assert!(!restarted.manager.accepting_allocations());
        let (status, error) = upgrade_response(address, "POST", abort, finalize.to_string()).await;
        assert_eq!(status, 409, "{error}");
        let mut restore = json!({
            "task_id": "planned-migration", "epoch": 1, "operation_id": "restore-verified-original",
            "stage": "restore-original", "rollback": true, "node_id": "c",
            "pod_uid": "late-old-pod", "node_session_id": verified_session,
        });
        let (status, error) = upgrade_response(address, "POST", abort, restore.to_string()).await;
        assert_eq!(status, 409, "{error}");
        restore["node_session_id"] = json!(restarted.report.session_id);
        let mut wrong_pod = restore.clone();
        wrong_pod["pod_uid"] = json!("recreated-pod");
        let (status, error) = upgrade_response(address, "POST", abort, wrong_pod.to_string()).await;
        assert_eq!(status, 409, "{error}");
        assert!(!restarted.manager.accepting_allocations());
        let restored = post_upgrade(address, abort, restore.clone()).await;
        assert_eq!(restored["restored_original"], true);
        assert_eq!(post_upgrade(address, abort, restore).await, restored);
        let paused = rig.session.get("late-sandbox").await.unwrap();
        assert_eq!(paused.assignment, assignment.clone().try_into().unwrap());
        assert_eq!(
            paused.result.unwrap().state,
            adx_core::EnvironmentState::Paused
        );
        assert_eq!(restarted.backend.restored.load(Ordering::SeqCst), 0);
        report(&mut restarted, &rig.address, period).await.unwrap();
        let running = coordinator
            .ensure_running(pb::EnsureRunningRequest {
                environment_id: "late-sandbox".into(),
                caller: caller(),
                timeout_seconds: 3,
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner()
            .record
            .unwrap();
        assert_eq!(running.assignment.unwrap(), assignment);
        assert_eq!(running.state, pb::EnvironmentState::Running as i32);
        assert_eq!(restarted.backend.restored.load(Ordering::SeqCst), 1);
        replacement = restarted;
    }
    let aborted = post_upgrade(address, abort, finalize).await;
    assert_eq!(aborted["phase"], "Aborted");
    assert_eq!(
        aborted["nodes"]["c"]["original_session_id"],
        previous_session
    );
    assert_eq!(
        aborted["nodes"]["c"]["node_session_id"],
        replacement.report.session_id
    );
    assert!(replacement.manager.accepting_allocations());
    server.abort();
}

async fn create_paused_on_late_original(
    rig: &Rig,
    worker: &Worker,
) -> (
    pb::Assignment,
    pb::coordinator_service_client::CoordinatorServiceClient<RpcChannel>,
) {
    use adx_core::scheduling::LabelSelector;

    let mut load = spec();
    load.id = "late-sandbox".into();
    load.scheduling.required_node.push(LabelSelector {
        match_labels: BTreeMap::from([("NODE_ID".into(), worker.report.node_id.clone())]),
        ..Default::default()
    });
    let mut coordinator = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(&rig.address, Principal::ApiServer).await,
    );
    let created = coordinator
        .create_environment(pb::CreateEnvironmentRequest {
            spec: Some(load.into()),
            caller: caller(),
            schedule_timeout_seconds: 3,
            create_timeout_seconds: 3,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let assignment = created.assignment.unwrap();
    assert_eq!(assignment.node_id, worker.report.node_id);
    let mut node = pb::node_service_client::NodeServiceClient::new(
        channel(&worker.report.node_address, Principal::ApiServer).await,
    );
    let paused = node
        .pause_environment(pb::PauseEnvironmentRequest {
            assignment: Some(assignment.clone()),
            caller: caller(),
            operation_id: "late-user-pause".into(),
            expected_revision: created.revision,
            timeout_seconds: 3,
            retention: Some(adx_core::lifecycle::RetentionPolicy::Forever.into()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(paused.durability, pb::Durability::Published as i32);
    (assignment, coordinator)
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn retries_abort_after_lost_original_release_and_process_restart() {
    for keep_paused in [false, true] {
        abort_after_lost_original_release(keep_paused).await;
    }
}

async fn abort_after_lost_original_release(keep_paused: bool) {
    use tokio::io::AsyncWriteExt;

    let mut rig = Rig::new(Duration::from_secs(2)).await;
    let original = rig.session.get("sleep").await.unwrap();
    let paused: pb::EnvironmentRecord = (&original).try_into().unwrap();
    if !keep_paused {
        let mut node = pb::node_service_client::NodeServiceClient::new(
            channel(&rig.source.report.node_address, Principal::ApiServer).await,
        );
        let deleted = node
            .delete_environment(pb::DeleteEnvironmentRequest {
                assignment: paused.assignment.clone(),
                caller: caller(),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(deleted.durability, pb::Durability::Published as i32);
    }
    let (address, server, _) = prepare_migration(&rig, paused).await;
    let abort = "tasks/planned-migration/abort";
    let freeze =
        json!({"task_id":"planned-migration","epoch":1,"operation_id":"freeze","stage":"freeze"});
    assert_eq!(
        post_upgrade(address, abort, freeze.clone()).await["phase"],
        "Aborting"
    );

    let previous_session = rig.source.report.session_id.clone();
    let mut restore = json!({"task_id":"planned-migration","epoch":1,"operation_id":"restore",
        "stage":"restore-original","node_id":"a","node_session_id":previous_session,
        "pod_uid":"source-pod"});
    let body = restore.to_string();
    let mut lost_reply = tokio::net::TcpStream::connect(address).await.unwrap();
    lost_reply
        .write_all(
            format!(
                "POST /v1/node-upgrades/{abort} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        loop {
            let task = rig.session.upgrade_task().await.unwrap().unwrap();
            let snapshot = rig.session.snapshot().await.unwrap();
            if task.nodes["a"].phase == "RestoredOriginal"
                && snapshot.nodes["a"].upgrade_pause.is_none()
                && rig.source.manager.accepting_allocations()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    drop(lost_reply);

    rig.source.task.abort();
    let _ = (&mut rig.source.task).await;
    let period = Duration::from_secs(60);
    let restarted = restart_source_report(&mut rig, period).await;
    if keep_paused {
        let error = restarted.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(error
            .message()
            .contains("node upgrade execution admission is closed"));
    } else {
        restarted.unwrap();
    }
    assert_ne!(rig.source.report.session_id, previous_session);
    assert!(!rig.source.manager.accepting_allocations());
    let finalize = json!({"task_id":"planned-migration","epoch":1,"operation_id":"finalize",
        "stage":"finalize","template_restored":true});
    let (status, body) = upgrade_response(address, "POST", abort, finalize.to_string()).await;
    assert_eq!(status, 409, "{body}");
    let task = rig.session.upgrade_task().await.unwrap().unwrap();
    assert_eq!(task.phase, "Aborting");
    assert_eq!(task.nodes["a"].node_session_id, previous_session);
    assert_eq!(task.nodes["a"].phase, "RestoredOriginal");
    post_upgrade(address, abort, freeze).await;

    restore["rollback"] = json!(true);
    let (status, body) = upgrade_response(address, "POST", abort, restore.to_string()).await;
    assert_eq!(status, 409, "{body}");
    restore["node_session_id"] = json!(rig.source.report.session_id);
    let mut wrong_pod = restore.clone();
    wrong_pod["pod_uid"] = json!("other-pod");
    let (status, body) = upgrade_response(address, "POST", abort, wrong_pod.to_string()).await;
    assert_eq!(status, 409, "{body}");
    assert!(!rig.source.manager.accepting_allocations());
    let snapshot = rig.session.snapshot().await.unwrap();
    assert!(snapshot.nodes["a"].upgrade_pause.is_some());
    let restored = post_upgrade(address, abort, restore.clone()).await;
    assert_eq!(restored["restored_original"], true);
    assert_eq!(restored["pod_uid"], "source-pod");
    assert_eq!(restored["node_session_id"], rig.source.report.session_id);
    assert_eq!(post_upgrade(address, abort, restore).await, restored);
    let snapshot = rig.session.snapshot().await.unwrap();
    assert!(snapshot.nodes["a"].upgrade_pause.is_none());
    assert!(rig.source.manager.accepting_allocations());
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);
    if keep_paused {
        let mut tick = interval_at(Instant::now() + period, period);
        tokio::select! {
            biased;
            _ = rig.source.manager.wait_for_reconciliation(&mut tick) => {
                panic!("unexpected queued reconciliation before Resume");
            }
            _ = std::future::ready(()) => {}
        }
        let mut client = pb::coordinator_service_client::CoordinatorServiceClient::new(
            channel(&rig.address, Principal::ApiServer).await,
        );
        let mut resume = Box::pin(client.ensure_running(pb::EnsureRunningRequest {
            expected_runtime_id: "sleep-1".into(),
            environment_id: "sleep".into(),
            caller: caller(),
            timeout_seconds: 5,
            ..Default::default()
        }));
        // Wait for Resume to encounter the missing Controller before
        // issuing the node report that adopts the original assignment.
        tokio::select! {
            biased;
            early = &mut resume => panic!("Resume completed before owner adoption: {early:?}"),
            wake = timeout(Duration::from_secs(2), rig.source.manager.wait_for_reconciliation(&mut tick)) => {
                wake.expect("Resume did not request owner reconciliation");
            }
        }
        let pending = rig.session.get("sleep").await.unwrap();
        assert_eq!(pending.assignment, original.assignment);
        assert!(pending.resume.is_some());
        assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);

        report(&mut rig.source, &rig.address, period).await.unwrap();
        let restored = resume.await.unwrap().into_inner().record.unwrap();
        let assignment: adx_core::Assignment = restored.assignment.unwrap().try_into().unwrap();
        assert_eq!(assignment, original.assignment);
        assert_eq!(restored.state, pb::EnvironmentState::Running as i32);
        assert!(rig.session.get("sleep").await.unwrap().resume.is_none());
        assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
    }
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    let aborted = post_upgrade(address, abort, finalize).await;
    assert_eq!(aborted["phase"], "Aborted");
    assert_eq!(
        aborted["nodes"]["a"]["node_session_id"],
        rig.source.report.session_id
    );
    assert_eq!(
        aborted["nodes"]["a"]["original_session_id"],
        previous_session
    );
    server.abort();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn retries_assigned_migration_after_source_departure_or_restart() {
    for (restarted, legacy) in [(false, false), (true, false), (false, true)] {
        let mut rig = Rig::new(Duration::from_secs(2)).await;
        let running = rig.resume(false).await.unwrap().record.unwrap();
        let (address, server, mut migrate) = prepare_migration(&rig, running).await;
        let body = migrate.to_string();
        let migration =
            tokio::spawn(
                async move { upgrade_response(address, "POST", MIGRATE_PATH, body).await },
            );

        let period = Duration::from_secs(60);
        let mut tick = interval_at(Instant::now() + period, period);
        timeout(
            Duration::from_secs(3),
            rig.target.manager.wait_for_reconciliation(&mut tick),
        )
        .await
        .unwrap();
        let assigned = rig.session.get("sleep").await.unwrap();
        assert_eq!(assigned.assignment.node_id, "b");
        assert_eq!(
            assigned.result.as_ref().unwrap().state,
            adx_core::EnvironmentState::Paused
        );
        assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);

        let original_session = rig.source.report.session_id.clone();
        pb::coordinator_service_client::CoordinatorServiceClient::new(
            channel(&rig.address, Principal::Node("a".into())).await,
        )
        .unregister_node(pb::UnregisterNodeRequest {
            node_id: "a".into(),
            session_id: original_session.clone(),
        })
        .await
        .unwrap();
        rig.source.task.abort();
        let _ = (&mut rig.source.task).await;
        let (status, error) = migration.await.unwrap();
        assert_eq!(status, 503, "{error}");

        if restarted {
            restart_source(&mut rig, period).await;
            assert_ne!(rig.source.report.session_id, original_session);
            let (status, body) = upgrade_response(
                address,
                "POST",
                "tasks/planned-migration/quiesce",
                quiesce_request(&rig, false).to_string(),
            )
            .await;
            assert_eq!(status, 409, "{body}");
        } else {
            // A frozen task can settle an already assigned operation without
            // reopening scheduling on an offline source.
            post_upgrade(
                address,
                "tasks/planned-migration/abort",
                json!({"task_id": "planned-migration", "epoch": 1, "operation_id": "freeze", "stage": "freeze"}),
            )
            .await;
            migrate["rollback"] = json!(true);
        }
        report(&mut rig.target, &rig.address, period).await.unwrap();
        if legacy {
            // Simulate a transfer from a Coordinator predating the durable stop proof.
            let mut record = serde_json::to_value(&assigned).unwrap();
            record["recovery"]
                .as_object_mut()
                .unwrap()
                .remove("stopped_source");
            let client = redis::Client::open(rig._redis.url.as_str()).unwrap();
            let mut conn = client.get_multiplexed_async_connection().await.unwrap();
            let _: usize = redis::cmd("HSET")
                .arg("adx:{test}:control:v1")
                .arg("environment:sleep")
                .arg(record.to_string())
                .query_async(&mut conn)
                .await
                .unwrap();
            let mut task = rig.session.upgrade_task().await.unwrap().unwrap();
            let revision = task.revision;
            task.operations
                .get_mut("migrate")
                .unwrap()
                .source_stopped_revision = None;
            rig.session
                .save_upgrade(task, Some(revision))
                .await
                .unwrap();
            let (status, error) =
                upgrade_response(address, "POST", MIGRATE_PATH, migrate.to_string()).await;
            assert_eq!(status, 503, "{error}");
            assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
            assert_eq!(
                rig.session
                    .get("sleep")
                    .await
                    .unwrap()
                    .result
                    .unwrap()
                    .state,
                adx_core::EnvironmentState::Paused
            );
            server.abort();
            continue;
        }
        let receipt = post_upgrade(address, MIGRATE_PATH, migrate.clone()).await;
        assert_eq!(receipt["node_id"], "b");
        assert_eq!(receipt["generation"], assigned.assignment.generation);
        assert_eq!(receipt["state"], "Running");
        assert_eq!(post_upgrade(address, MIGRATE_PATH, migrate).await, receipt);
        assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 1);
        if !restarted {
            restart_source(&mut rig, period).await;
        }
        let request = quiesce_request(&rig, !restarted);
        let permit =
            post_upgrade(address, "tasks/planned-migration/quiesce", request.clone()).await;
        assert_eq!(permit["strict_empty"], true);
        assert_eq!(permit["node_session_id"], rig.source.report.session_id);
        let task = rig.session.upgrade_task().await.unwrap().unwrap();
        assert_eq!(task.nodes["a"].original_session_id, original_session);
        if restarted {
            restart_after_delete_intent(&mut rig, address, request, permit, period).await;
        } else {
            let mut verify = request;
            verify["operation_id"] = json!("verify-restarted-source");
            verify["permit_id"] = permit["permit_id"].clone();
            post_upgrade(address, "tasks/planned-migration/verify-node", verify).await;
            let aborted = post_upgrade(
                address,
                "tasks/planned-migration/abort",
                json!({"task_id": "planned-migration", "epoch": 1, "operation_id": "finalize",
                    "stage": "finalize", "template_restored": true}),
            )
            .await;
            assert_eq!(aborted["phase"], "Aborted");
        }
        server.abort();
    }
}

async fn restart_after_delete_intent(
    rig: &mut Rig,
    address: std::net::SocketAddr,
    mut request: serde_json::Value,
    permit: serde_json::Value,
    period: Duration,
) {
    let quiesce = "tasks/planned-migration/quiesce";
    let revoke = "tasks/planned-migration/revoke-delete";
    let previous_session = rig.source.report.session_id.clone();
    let plain_quiesce = request.clone();
    request["delete_intent"] = json!(true);
    request["permit_id"] = permit["permit_id"].clone();
    request["pod_resource_version"] = json!("10");
    let confirmed = post_upgrade(address, quiesce, request.clone()).await;
    let revision = rig.session.upgrade_task().await.unwrap().unwrap().revision;
    let mut different_version = request.clone();
    different_version["pod_resource_version"] = json!("11");
    for stale in [plain_quiesce, different_version] {
        let (status, body) = upgrade_response(address, "POST", quiesce, stale.to_string()).await;
        assert_eq!(status, 409, "{body}");
    }
    assert_eq!(
        post_upgrade(address, quiesce, request.clone()).await,
        confirmed
    );
    assert_eq!(
        rig.session.upgrade_task().await.unwrap().unwrap().revision,
        revision
    );
    rig.source.task.abort();
    let _ = (&mut rig.source.task).await;
    restart_source(rig, period).await;

    let mut revocation = quiesce_request(rig, false);
    revocation["expected_delete_resource_version"] = json!("9");
    revocation["pod_resource_version"] = json!("11");
    let (status, body) = upgrade_response(address, "POST", revoke, revocation.to_string()).await;
    assert_eq!(status, 409, "{body}");
    revocation["expected_delete_resource_version"] = json!("10");
    post_upgrade(address, revoke, revocation.clone()).await;
    let task = rig.session.upgrade_task().await.unwrap().unwrap();
    assert_eq!(task.nodes["a"].node_session_id, previous_session);
    assert!(task.nodes["a"].delete_resource_version.is_none());
    assert!(task.nodes["a"].permit_id.is_none());

    let next = post_upgrade(address, quiesce, quiesce_request(rig, false)).await;
    assert_eq!(next["node_session_id"], rig.source.report.session_id);
    assert_ne!(next["permit_id"], permit["permit_id"]);
    post_upgrade(address, revoke, revocation).await;
    let task = rig.session.upgrade_task().await.unwrap().unwrap();
    assert_eq!(
        task.nodes["a"].permit_id.as_deref(),
        next["permit_id"].as_str()
    );
    request["node_session_id"] = json!(rig.source.report.session_id);
    let (status, body) = upgrade_response(address, "POST", quiesce, request.to_string()).await;
    assert_eq!(status, 409, "{body}");

    rollback_restarted_replacement(rig, address, next, period).await;
}

async fn rollback_restarted_replacement(
    rig: &mut Rig,
    address: std::net::SocketAddr,
    permit: serde_json::Value,
    period: Duration,
) {
    let quiesce = "tasks/planned-migration/quiesce";
    let revoke = "tasks/planned-migration/revoke-delete";
    let mut confirmed = quiesce_request(rig, false);
    confirmed["delete_intent"] = json!(true);
    confirmed["permit_id"] = permit["permit_id"].clone();
    confirmed["pod_resource_version"] = json!("12");
    post_upgrade(address, quiesce, confirmed).await;

    rig.source.task.abort();
    let _ = (&mut rig.source.task).await;
    restart_source(rig, period).await;
    let mut replacement = quiesce_request(rig, false);
    replacement["pod_uid"] = json!("replacement-pod");
    let bound = post_upgrade(address, quiesce, replacement).await;

    rig.source.task.abort();
    let _ = (&mut rig.source.task).await;
    restart_source(rig, period).await;
    let mut rollback = quiesce_request(rig, false);
    rollback["pod_uid"] = json!("replacement-pod");
    let (status, body) = upgrade_response(address, "POST", quiesce, rollback.to_string()).await;
    assert_eq!(status, 409, "{body}");
    post_upgrade(
        address,
        "tasks/planned-migration/abort",
        json!({"task_id":"planned-migration","epoch":1,"operation_id":"freeze-replacement","stage":"freeze"}),
    )
    .await;
    rollback["rollback"] = json!(true);
    let next = post_upgrade(address, quiesce, rollback.clone()).await;
    assert_ne!(next["permit_id"], bound["permit_id"]);
    assert_eq!(next["node_session_id"], rig.source.report.session_id);

    let mut confirmed = rollback.clone();
    confirmed["delete_intent"] = json!(true);
    confirmed["permit_id"] = next["permit_id"].clone();
    confirmed["pod_resource_version"] = json!("20");
    post_upgrade(address, quiesce, confirmed).await;
    let previous_session = rig.source.report.session_id.clone();
    rig.source.task.abort();
    let _ = (&mut rig.source.task).await;
    restart_source(rig, period).await;
    rollback["node_session_id"] = json!(rig.source.report.session_id);
    let (status, body) = upgrade_response(address, "POST", quiesce, rollback.to_string()).await;
    assert_eq!(status, 409, "{body}");

    let mut revocation = rollback.clone();
    revocation["expected_delete_resource_version"] = json!("20");
    revocation["pod_resource_version"] = json!("21");
    revocation["rollback"] = json!(false);
    let (status, body) = upgrade_response(address, "POST", revoke, revocation.to_string()).await;
    assert_eq!(status, 409, "{body}");
    revocation["rollback"] = json!(true);
    post_upgrade(address, revoke, revocation).await;
    let task = rig.session.upgrade_task().await.unwrap().unwrap();
    assert_eq!(task.nodes["a"].node_session_id, previous_session);
    assert!(task.nodes["a"].delete_resource_version.is_none());
    assert!(task.nodes["a"].permit_id.is_none());
    let rebound = post_upgrade(address, quiesce, rollback).await;
    assert_ne!(rebound["permit_id"], next["permit_id"]);
    assert_eq!(rebound["node_session_id"], rig.source.report.session_id);
    assert_eq!(rebound["pod_uid"], "replacement-pod");
}

async fn restart_source(rig: &mut Rig, period: Duration) {
    restart_source_report(rig, period).await.unwrap();
}

async fn restart_source_report(
    rig: &mut Rig,
    period: Duration,
) -> std::result::Result<(), tonic::Status> {
    // A same-Pod restart proves predecessor replacement at the same endpoint.
    let mut replacement = worker(
        "a",
        &rig.address,
        rig.remote.clone(),
        Some(&rig.source.report.node_address),
    )
    .await;
    let report = report(&mut replacement, &rig.address, period).await;
    rig.source = replacement;
    report
}

fn quiesce_request(rig: &Rig, rollback: bool) -> serde_json::Value {
    json!({"task_id": "planned-migration", "epoch": 1, "operation_id": "quiesce-restarted-source",
        "node_id": "a", "pod_uid": "source-pod", "node_session_id": rig.source.report.session_id,
        "rollback": rollback})
}
