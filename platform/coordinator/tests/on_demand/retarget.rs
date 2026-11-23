use super::*;
use adx_core::scheduling::LabelSelector;
use serde_json::json;
use tokio::{sync::Notify, time::timeout};

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn automatic_migration_reselects_target_when_capacity_is_taken_during_checkpoint() {
    migration_capacity_race(true).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn pinned_migration_does_not_change_target_when_capacity_is_taken_during_checkpoint() {
    migration_capacity_race(false).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn evacuation_defers_when_automatic_reselection_finds_no_target() {
    evacuation_capacity_loss(true).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn evacuation_defers_when_a_pinned_claim_cannot_dispatch() {
    evacuation_capacity_loss(false).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn migration_acceptance_rejects_source_version_changed_by_resume() {
    let rig = Rig::new(Duration::from_secs(4)).await;
    let paused = rig.session.get("sleep").await.unwrap();
    let running = rig.resume(false).await.unwrap().record.unwrap();
    let (_, server, _) = prepare_migration(&rig, running.clone()).await;
    let mut task = rig.session.upgrade_task().await.unwrap().unwrap();
    let revision = task.revision;
    let source_revision = paused.result.unwrap().revision;
    assert!(running.revision > source_revision);
    task.operations.insert(
        "stale-move".into(),
        adx_coordinator::storage::UpgradeOperation {
            environment_id: "sleep".into(),
            source_node_id: "a".into(),
            target_node_id: "b".into(),
            automatic_target: true,
            source_generation: paused.assignment.generation,
            source_revision,
            source_stopped_revision: None,
            source_session_id: rig.source.report.session_id.clone(),
            target_session_id: rig.target.report.session_id.clone(),
            was_running: false,
            phase: "Prepared".into(),
        },
    );
    assert_eq!(
        rig.session.save_upgrade(task, Some(revision)).await,
        Err(adx_core::Error::Conflict)
    );
    assert!(rig
        .session
        .upgrade_task()
        .await
        .unwrap()
        .unwrap()
        .operations
        .is_empty());
    server.abort();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn migration_acceptance_rejects_replaced_node_sessions() {
    let rig = Rig::new(Duration::from_secs(4)).await;
    let running = rig.resume(false).await.unwrap().record.unwrap();
    let generation = running.assignment.as_ref().unwrap().generation;
    let source_revision = running.revision;
    let (_, server, _) = prepare_migration(&rig, running).await;
    let task = rig.session.upgrade_task().await.unwrap().unwrap();
    for stale_source in [true, false] {
        let mut candidate = task.clone();
        candidate.operations.insert(
            "stale-session".into(),
            adx_coordinator::storage::UpgradeOperation {
                environment_id: "sleep".into(),
                source_node_id: "a".into(),
                target_node_id: "b".into(),
                automatic_target: true,
                source_generation: generation,
                source_revision,
                source_stopped_revision: None,
                source_session_id: if stale_source {
                    "replaced-source".into()
                } else {
                    rig.source.report.session_id.clone()
                },
                target_session_id: if stale_source {
                    rig.target.report.session_id.clone()
                } else {
                    "replaced-target".into()
                },
                was_running: true,
                phase: "Prepared".into(),
            },
        );
        assert_eq!(
            rig.session
                .save_upgrade(candidate, Some(task.revision))
                .await,
            Err(adx_core::Error::Conflict)
        );
    }
    assert!(rig
        .session
        .upgrade_task()
        .await
        .unwrap()
        .unwrap()
        .operations
        .is_empty());
    server.abort();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn delete_confirmation_rejects_replaced_node_session() {
    let rig = Rig::new(Duration::from_secs(4)).await;
    let running = rig.resume(false).await.unwrap().record.unwrap();
    let (_, server, _) = prepare_migration(&rig, running).await;
    let mut task = rig.session.upgrade_task().await.unwrap().unwrap();
    let node = task.nodes.get_mut("a").unwrap();
    node.phase = "quiescing".into();
    node.permit_id = Some("permit".into());
    let revision = task.revision;
    let task = rig
        .session
        .save_upgrade(task, Some(revision))
        .await
        .unwrap();
    let mut confirmed = task.clone();
    confirmed
        .nodes
        .get_mut("a")
        .unwrap()
        .delete_resource_version = Some("10".into());

    // Registering a successor advances the publication token independently of
    // the task, after the last successful Inspect and before confirming DELETE.
    let client = redis::Client::open(rig._redis.url.as_str()).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    let _: usize = redis::cmd("HSET")
        .arg("adx:{test}:control:v1")
        .arg("node-session:a")
        .arg("replacement-session")
        .query_async(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        rig.session
            .save_upgrade(confirmed, Some(task.revision))
            .await,
        Err(adx_core::Error::Conflict)
    );
    assert_eq!(rig.session.upgrade_task().await.unwrap().unwrap(), task);
    server.abort();
}

async fn migration_capacity_race(automatic: bool) {
    let mut rig = Rig::new(Duration::from_secs(4)).await;
    let running = rig.resume(false).await.unwrap().record.unwrap();
    let original: adx_core::Assignment = running.assignment.clone().unwrap().try_into().unwrap();
    // New sessions are isolated during an active task. Use a compatible spare
    // that was already available for scheduling when the task began.
    let mut spare = worker("c", &rig.address, rig.remote.clone(), None).await;
    register(&rig.address, &mut spare).await;
    let (address, server, mut migrate) = prepare_migration(&rig, running).await;
    if automatic {
        migrate.as_object_mut().unwrap().remove("target_node_id");
    }
    let gate = Arc::new(Notify::new());
    *rig.source.backend.checkpoint_gate.lock().unwrap() = Some(gate.clone());
    let body = migrate.to_string();
    let migration =
        tokio::spawn(async move { upgrade_response(address, "POST", MIGRATE_PATH, body).await });
    timeout(
        Duration::from_secs(3),
        rig.source.backend.checkpoint_entered.notified(),
    )
    .await
    .unwrap();
    let task = rig.session.upgrade_task().await.unwrap().unwrap();
    assert_eq!(task.operations["migrate"].target_node_id, "b");
    assert_eq!(task.operations["migrate"].phase, "Prepared");

    take_target_capacity(&rig).await;
    gate.notify_one();

    if !automatic {
        let (status, _) = timeout(Duration::from_secs(3), migration)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, 429);
        let stored = rig.session.get("sleep").await.unwrap();
        assert_eq!(stored.assignment, original);
        assert_eq!(
            stored.result.unwrap().state,
            adx_core::EnvironmentState::Paused
        );
        let task = rig.session.upgrade_task().await.unwrap().unwrap();
        assert_eq!(task.operations["migrate"].target_node_id, "b");
        assert_eq!(spare.backend.restored.load(Ordering::SeqCst), 0);
        migrate["target_node_id"] = serde_json::json!("c");
        let (status, _) =
            upgrade_response(address, "POST", MIGRATE_PATH, migrate.to_string()).await;
        assert_eq!(status, 409);
        // A different operation ID must not reinterpret the in-progress
        // migration's Paused state as an originally Paused Environment.
        migrate["operation_id"] = serde_json::json!("duplicate");
        migrate
            .as_object_mut()
            .unwrap()
            .remove("expected_source_revision");
        let (status, body) =
            upgrade_response(address, "POST", MIGRATE_PATH, migrate.to_string()).await;
        assert_eq!(status, 409, "{body}");
        let task = rig.session.upgrade_task().await.unwrap().unwrap();
        assert_eq!(task.operations.len(), 1);
        assert_eq!(rig.session.get("sleep").await.unwrap().assignment, original);
        server.abort();
        return;
    }

    // Withhold reports to exercise retry after the ownership CAS. A wake is
    // only advisory, so observe the HTTP result and durable assignment.
    let (status, body) = timeout(Duration::from_secs(6), migration)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, 503, "{body}");
    let assigned = rig.session.get("sleep").await.unwrap().assignment;
    assert_eq!(assigned.node_id, "c", "{body}");
    let task = rig.session.upgrade_task().await.unwrap().unwrap();
    assert_eq!(task.operations["migrate"].phase, "Assigned");

    // Make the old target eligible again. A stale caller hint cannot change C
    // or consume a new generation once the assignment CAS has succeeded.
    let period = Duration::from_secs(60);
    let capacity = Resources {
        cpu_millis: 200,
        memory_bytes: 256,
        disk_bytes: 0,
    };
    rig.target
        .manager
        .update_capacity(capacity, period)
        .unwrap();
    rig.target.report.capacity = Some(capacity.into());
    report(&mut rig.target, &rig.address, period).await.unwrap();
    report(&mut rig.source, &rig.address, period).await.unwrap();
    report(&mut spare, &rig.address, period).await.unwrap();
    migrate["target_node_id"] = serde_json::json!("b");
    let completed = post_upgrade(address, MIGRATE_PATH, migrate.clone()).await;
    assert_eq!(completed["node_id"], "c");
    assert_eq!(completed["state"], "Running");
    assert_eq!(rig.session.get("sleep").await.unwrap().assignment, assigned);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    assert_eq!(spare.backend.restored.load(Ordering::SeqCst), 1);

    assert_eq!(
        post_upgrade(address, MIGRATE_PATH, migrate).await,
        completed
    );
    server.abort();
}

async fn take_target_capacity(rig: &Rig) {
    let mut load = spec();
    load.id = "take-target-capacity".into();
    load.scheduling.required_node.push(LabelSelector {
        match_labels: BTreeMap::from([("NODE_ID".into(), "b".into())]),
        ..Default::default()
    });
    let load = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(&rig.address, Principal::ApiServer).await,
    )
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
    assert_eq!(load.assignment.unwrap().node_id, "b");
}

async fn evacuation_capacity_loss(automatic: bool) {
    let mut rig = Rig::new(Duration::from_secs(4)).await;
    let running = rig.resume(false).await.unwrap().record.unwrap();
    let original: adx_core::Assignment = running.assignment.clone().unwrap().try_into().unwrap();
    let stale_route = running.runtime.as_ref().unwrap().id.clone();
    let (address, server, mut migrate) = prepare_migration(&rig, running).await;
    migrate["defer_on_capacity"] = json!(true);
    if automatic {
        migrate.as_object_mut().unwrap().remove("target_node_id");
    }
    let gate = Arc::new(Notify::new());
    *rig.source.backend.checkpoint_gate.lock().unwrap() = Some(gate.clone());
    let body = migrate.to_string();
    let transfer =
        tokio::spawn(async move { upgrade_response(address, "POST", MIGRATE_PATH, body).await });
    timeout(
        Duration::from_secs(3),
        rig.source.backend.checkpoint_entered.notified(),
    )
    .await
    .unwrap();
    take_target_capacity(&rig).await;
    *rig.source.backend.checkpoint_gate.lock().unwrap() = None;
    gate.notify_one();
    let (status, deferred) = timeout(Duration::from_secs(6), transfer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, 200, "{deferred}");
    assert_eq!(deferred["outcome"], "deferred");
    assert_eq!(deferred["node_id"], "a");
    assert_eq!(deferred["generation"], original.generation);
    assert_eq!(
        post_upgrade(address, MIGRATE_PATH, migrate.clone()).await,
        deferred
    );
    let restored = rig.session.get("sleep").await.unwrap();
    assert_eq!(restored.assignment, original);
    let restored = restored.result.unwrap();
    assert_eq!(restored.state, adx_core::EnvironmentState::Running);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 2);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    let task = rig.session.upgrade_task().await.unwrap().unwrap();
    assert_eq!(task.phase, "Active");
    assert_eq!(task.nodes["a"].phase, "evacuating");
    assert_eq!(task.operations["migrate"].phase, "Deferred");
    assert!(!rig.source.manager.accepting_allocations());
    // A retry carrying the old runtime ID can resolve the restored execution
    // without trying to start a Paused Sandbox on the evacuating source.
    let replacement_route = pb::coordinator_service_client::CoordinatorServiceClient::new(
        channel(&rig.address, Principal::Ingress).await,
    )
    .ensure_running(pb::EnsureRunningRequest {
        expected_runtime_id: stale_route.clone(),
        environment_id: "sleep".into(),
        caller: caller(),
        timeout_seconds: 1,
        ..Default::default()
    })
    .await
    .unwrap()
    .into_inner()
    .record
    .unwrap();
    let route_owner: adx_core::Assignment =
        replacement_route.assignment.unwrap().try_into().unwrap();
    assert_eq!(route_owner, original);
    assert_ne!(replacement_route.runtime.unwrap().id, stale_route);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 2);

    let previous = migrate.clone();
    migrate["operation_id"] = json!("migrate-retry");
    migrate["expected_source_revision"] = json!(restored.revision);
    let (status, _) = upgrade_response(address, "POST", MIGRATE_PATH, migrate.to_string()).await;
    assert_eq!(status, 429);
    assert!(!rig
        .session
        .upgrade_task()
        .await
        .unwrap()
        .unwrap()
        .operations
        .contains_key("migrate-retry"));
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 2);

    retry_after_capacity(&mut rig, address, migrate, Duration::from_secs(60)).await;
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 2);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 1);
    assert_eq!(
        post_upgrade(address, MIGRATE_PATH, previous).await,
        deferred
    );
    server.abort();
}

async fn retry_after_capacity(
    rig: &mut Rig,
    address: std::net::SocketAddr,
    migrate: serde_json::Value,
    period: Duration,
) {
    let capacity = Resources {
        cpu_millis: 200,
        memory_bytes: 256,
        disk_bytes: 0,
    };
    rig.target
        .manager
        .update_capacity(capacity, period)
        .unwrap();
    rig.target.report.capacity = Some(capacity.into());
    report(&mut rig.target, &rig.address, period).await.unwrap();
    let body = migrate.to_string();
    let transfer =
        tokio::spawn(async move { upgrade_response(address, "POST", MIGRATE_PATH, body).await });
    // Wait for committed assignment before waking the two workers' reconciliation.
    timeout(Duration::from_secs(3), async {
        loop {
            if rig.session.get("sleep").await.unwrap().assignment.node_id == "b" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    report(&mut rig.source, &rig.address, period).await.unwrap();
    report(&mut rig.target, &rig.address, period).await.unwrap();
    let (status, completed) = timeout(Duration::from_secs(6), transfer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, 200, "{completed}");
    assert_eq!(completed["outcome"], "completed");
    assert_eq!(completed["node_id"], "b");
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn drain_defers_and_resumes_source_when_reselection_finds_no_target() {
    drain_capacity_loss(DrainCapacityLoss::LocalCapacity).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn drain_defers_and_resumes_source_when_a_pinned_claim_cannot_dispatch() {
    drain_capacity_loss(DrainCapacityLoss::Pinned).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn drain_deferral_retries_a_restore_input_with_unconfirmed_publication() {
    for commit_first in [false, true] {
        drain_capacity_loss(DrainCapacityLoss::InputAck { commit_first }).await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn drain_deferral_accepts_a_later_user_operation_after_published_restore() {
    drain_capacity_loss(DrainCapacityLoss::ResultAck).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn deferral_replays_after_source_session_restart() {
    for drain in [false, true] {
        deferral_with_source_restart(drain, false, RestartDeferralFault::InputAck, false).await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn deferral_replays_after_source_session_restart_during_abort() {
    for drain in [false, true] {
        deferral_with_source_restart(drain, true, RestartDeferralFault::InputAck, false).await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn deferral_replays_after_source_session_restart_with_a_surviving_backend() {
    for committed in [false, true] {
        deferral_with_source_restart(
            false,
            false,
            RestartDeferralFault::Running { committed },
            false,
        )
        .await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn settled_deferral_rebinds_after_another_source_session_restart() {
    for drain in [false, true] {
        deferral_with_source_restart(drain, false, RestartDeferralFault::InputAck, true).await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn settled_deferral_rebinds_after_another_source_session_restart_during_abort() {
    for drain in [false, true] {
        deferral_with_source_restart(drain, true, RestartDeferralFault::InputAck, true).await;
    }
}

#[derive(Clone, Copy)]
enum RestartDeferralFault {
    InputAck,
    Running { committed: bool },
}

async fn deferral_with_source_restart(
    drain: bool,
    abort: bool,
    fault: RestartDeferralFault,
    restart_again: bool,
) {
    let mut rig = Rig::new(Duration::from_secs(4)).await;
    let running = rig.resume(false).await.unwrap().record.unwrap();
    let original: adx_core::Assignment = running.assignment.clone().unwrap().try_into().unwrap();
    let previous_session = rig.source.report.session_id.clone();
    let (address, server, mut migrate) = if drain {
        let (address, server, _, _) = prepare_review_drain(&rig).await;
        let migrate = json!({
            "task_id":"planned-migration", "epoch":1, "operation_id":"migrate",
            "node_id":"a", "node_session_id":previous_session, "pod_uid":"source-pod",
            "environment_id":"sleep", "expected_source_generation":original.generation,
            "expected_source_revision":running.revision, "checkpoint_storage_alias":"obs"
        });
        (address, server, migrate)
    } else {
        let (address, server, mut migrate) = prepare_migration(&rig, running).await;
        migrate["defer_on_capacity"] = json!(true);
        (address, server, migrate)
    };
    let old_backend = rig.source.backend.clone();
    let checkpoint = Arc::new(Notify::new());
    *old_backend.checkpoint_gate.lock().unwrap() = Some(checkpoint.clone());
    let body = migrate.to_string();
    let migration =
        tokio::spawn(async move { upgrade_response(address, "POST", MIGRATE_PATH, body).await });
    timeout(
        Duration::from_secs(3),
        old_backend.checkpoint_entered.notified(),
    )
    .await
    .unwrap();
    take_target_capacity(&rig).await;
    match fault {
        RestartDeferralFault::InputAck => {
            *old_backend.restore_input_publication.lock().unwrap() = Some(true);
        }
        RestartDeferralFault::Running { committed: false } => {
            old_backend
                .lose_restore_result_publication
                .store(true, Ordering::SeqCst);
        }
        RestartDeferralFault::Running { committed: true } => {
            old_backend
                .lose_restore_result_ack
                .store(true, Ordering::SeqCst);
        }
    }
    checkpoint.notify_one();
    let (status, body) = migration.await.unwrap();
    assert_eq!(status, 503, "{body}");
    let task = rig.session.upgrade_task().await.unwrap().unwrap();
    assert_eq!(task.operations["migrate"].phase, "Deferring");
    let before = rig.session.get("sleep").await.unwrap();
    let record = before.result.as_ref().unwrap();
    let preserve_runtime = matches!(fault, RestartDeferralFault::Running { .. });
    let committed = matches!(fault, RestartDeferralFault::Running { committed: true });
    assert_eq!(before.assignment, original);
    if committed {
        assert_eq!(record.state, adx_core::EnvironmentState::Running);
    } else {
        assert_eq!(record.state, adx_core::EnvironmentState::Paused);
        assert_eq!(
            record.revision,
            task.operations["migrate"].source_stopped_revision.unwrap() + 1
        );
    }
    assert_eq!(
        old_backend.restored.load(Ordering::SeqCst),
        1 + usize::from(preserve_runtime)
    );
    let surviving_runtime = old_backend.running.lock().unwrap().keys().next().cloned();
    assert_eq!(surviving_runtime.is_some(), preserve_runtime);

    rig.source.task.abort();
    let _ = (&mut rig.source.task).await;
    let period = Duration::from_secs(60);
    let _ = super::source_session::restart_source_report_with_runtime(
        &mut rig,
        period,
        preserve_runtime,
    )
    .await;
    let mut current_session = rig.source.report.session_id.clone();
    assert_ne!(current_session, previous_session);
    assert!(!rig.source.manager.accepting_allocations());
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);

    let abort_path = "tasks/planned-migration/abort";
    let freeze = json!({"task_id":"planned-migration", "epoch":1,
        "operation_id":"freeze", "stage":"freeze"});
    if abort && !restart_again {
        let (status, body) =
            upgrade_response(address, "POST", abort_path, freeze.to_string()).await;
        assert_eq!(status, 409, "{body}");
        let task = rig.session.upgrade_task().await.unwrap().unwrap();
        assert_eq!(task.phase, "Aborting");
        migrate["rollback"] = json!(true);
    }
    let (status, body) = upgrade_response(address, "POST", MIGRATE_PATH, migrate.to_string()).await;
    assert_eq!(status, 409, "{body}");
    migrate["node_session_id"] = json!(current_session);
    let mut wrong_pod = migrate.clone();
    wrong_pod["pod_uid"] = json!("other-pod");
    let (status, body) =
        upgrade_response(address, "POST", MIGRATE_PATH, wrong_pod.to_string()).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(rig.session.get("sleep").await.unwrap(), before);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);

    let (status, reply) =
        upgrade_response(address, "POST", MIGRATE_PATH, migrate.to_string()).await;
    let receipt = if status == 503 {
        // The test worker has no periodic loop. Drive the real authoritative
        // adoption after the new session is authorized, then replay the same ID.
        let rebound = rig.session.upgrade_task().await.unwrap().unwrap();
        assert_eq!(rebound.nodes["a"].node_session_id, current_session);
        assert_eq!(rebound.operations["migrate"].phase, "Deferring");
        assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);
        report(&mut rig.source, &rig.address, period).await.unwrap();
        post_upgrade(address, MIGRATE_PATH, migrate.clone()).await
    } else {
        assert_eq!(status, 200, "{reply}");
        reply
    };
    assert_eq!(receipt["outcome"], "deferred");
    assert_eq!(receipt["node_id"], "a");
    assert_eq!(receipt["generation"], original.generation);
    assert_eq!(
        post_upgrade(address, MIGRATE_PATH, migrate.clone()).await,
        receipt
    );
    migrate["node_session_id"] = json!(previous_session);
    assert_eq!(
        post_upgrade(address, MIGRATE_PATH, migrate.clone()).await,
        receipt
    );
    let settled = rig.session.upgrade_task().await.unwrap().unwrap();
    assert_eq!(settled.nodes["a"].node_session_id, current_session);
    assert_eq!(settled.nodes["a"].original_session_id, previous_session);
    assert_eq!(
        settled.operations["migrate"].source_session_id,
        previous_session
    );
    assert_eq!(settled.operations["migrate"].phase, "Deferred");
    assert!(!rig.source.manager.accepting_allocations());
    let restored = rig.session.get("sleep").await.unwrap();
    assert_eq!(restored.assignment, original);
    let restored = restored.result.unwrap();
    assert_eq!(restored.state, adx_core::EnvironmentState::Running);
    assert_eq!(
        rig.source.backend.restored.load(Ordering::SeqCst),
        usize::from(!committed)
    );
    {
        let active = rig.source.backend.running.lock().unwrap();
        assert_eq!(active.len(), 1);
        assert!(active.contains_key(&restored.runtime.id));
        if let Some(survivor) = surviving_runtime {
            assert_eq!(active.contains_key(&survivor), committed);
        }
    }
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);

    if restart_again {
        rig.source.task.abort();
        let _ = (&mut rig.source.task).await;
        let _ =
            super::source_session::restart_source_report_with_runtime(&mut rig, period, true).await;
        let intermediate_session = current_session;
        current_session = rig.source.report.session_id.clone();
        assert_ne!(current_session, intermediate_session);
        assert!(!rig.source.manager.accepting_allocations());
        assert!(rig
            .source
            .backend
            .running
            .lock()
            .unwrap()
            .contains_key(&restored.runtime.id));
        if abort {
            let (status, body) =
                upgrade_response(address, "POST", abort_path, freeze.to_string()).await;
            assert_eq!(status, 409, "{body}");
            let task = rig.session.upgrade_task().await.unwrap().unwrap();
            assert_eq!(task.phase, "Aborting");
            migrate["rollback"] = json!(true);
        }
        // Historical receipt lookup is read-only, including on a stale session.
        assert_eq!(
            post_upgrade(address, MIGRATE_PATH, migrate.clone()).await,
            receipt
        );
        let mut rebind = migrate.clone();
        rebind["rebind_source"] = json!(true);
        rebind["rollback"] = json!(abort);
        rebind["node_session_id"] = json!(intermediate_session);
        let (stale_status, stale_reply) =
            upgrade_response(address, "POST", MIGRATE_PATH, rebind.to_string()).await;
        assert_eq!(
            rig.session.upgrade_task().await.unwrap().unwrap().nodes["a"].node_session_id,
            intermediate_session
        );
        rebind["node_session_id"] = json!(current_session);
        let mut wrong_pod = rebind.clone();
        wrong_pod["pod_uid"] = json!("other-pod");
        let (status, body) =
            upgrade_response(address, "POST", MIGRATE_PATH, wrong_pod.to_string()).await;
        assert_eq!(status, 409, "{body}");
        let rebound = post_upgrade(address, MIGRATE_PATH, rebind.clone()).await;
        assert_eq!(rebound, receipt);
        assert_eq!(stale_status, 409, "{stale_reply}");
        assert_eq!(post_upgrade(address, MIGRATE_PATH, rebind).await, receipt);
        report(&mut rig.source, &rig.address, period).await.unwrap();
        let rebound_task = rig.session.upgrade_task().await.unwrap().unwrap();
        assert_eq!(rebound_task.nodes["a"].node_session_id, current_session);
        assert_eq!(
            rebound_task.nodes["a"].original_session_id,
            previous_session
        );
        assert_eq!(
            rebound_task.operations["migrate"],
            settled.operations["migrate"]
        );
        let current = rig.session.get("sleep").await.unwrap();
        assert_eq!(current.assignment, original);
        let current = current.result.unwrap();
        assert_eq!(current.state, adx_core::EnvironmentState::Running);
        assert_eq!(current.runtime.id, restored.runtime.id);
        assert_eq!(rig.source.backend.running.lock().unwrap().len(), 1);
        assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);
        assert!(!rig.source.manager.accepting_allocations());
        if !abort {
            migrate["operation_id"] = json!("migrate-after-rebind");
            migrate["node_session_id"] = json!(current_session);
            migrate["expected_source_revision"] = json!(current.revision);
            retry_after_capacity(&mut rig, address, migrate, period).await;
            assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);
            assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 1);
        }
    }

    if abort {
        post_upgrade(address, abort_path, freeze).await;
        let release = post_upgrade(
            address,
            abort_path,
            json!({"task_id":"planned-migration",
            "epoch":1, "operation_id":"restore", "stage":"restore-original", "node_id":"a",
            "node_session_id":current_session, "pod_uid":"source-pod", "rollback":true}),
        )
        .await;
        assert_eq!(release["restored_original"], true);
        let finalized = post_upgrade(
            address,
            abort_path,
            json!({"task_id":"planned-migration",
            "epoch":1, "operation_id":"finalize", "stage":"finalize", "template_restored":true}),
        )
        .await;
        assert_eq!(finalized["phase"], "Aborted");
        assert!(rig.source.manager.accepting_allocations());
        assert_eq!(rig.session.get("sleep").await.unwrap().assignment, original);
        assert_eq!(
            rig.source.backend.restored.load(Ordering::SeqCst),
            usize::from(!restart_again)
        );
    }
    server.abort();
}

enum DrainCapacityLoss {
    Pinned,
    LocalCapacity,
    InputAck { commit_first: bool },
    ResultAck,
}

async fn drain_capacity_loss(fault: DrainCapacityLoss) {
    let rig = Rig::new(Duration::from_secs(4)).await;
    let running = rig.resume(false).await.unwrap().record.unwrap();
    let original = running.assignment.unwrap();
    let (address, server, source_session, target_session) = prepare_review_drain(&rig).await;
    let mut migrate = json!({
        "task_id":"planned-migration", "epoch":1, "operation_id":"migrate",
        "node_id":"a", "node_session_id":source_session, "pod_uid":"source-pod",
        "environment_id":"sleep", "expected_source_generation":original.generation,
        "expected_source_revision":running.revision, "checkpoint_storage_alias":"obs"
    });
    if matches!(fault, DrainCapacityLoss::Pinned) {
        migrate["target_node_id"] = json!("b");
    }
    let gate = Arc::new(Notify::new());
    *rig.source.backend.checkpoint_gate.lock().unwrap() = Some(gate.clone());
    let body = migrate.to_string();
    let migration =
        tokio::spawn(async move { upgrade_response(address, "POST", MIGRATE_PATH, body).await });
    timeout(
        Duration::from_secs(3),
        rig.source.backend.checkpoint_entered.notified(),
    )
    .await
    .unwrap();
    take_target_capacity(&rig).await;
    let period = Duration::from_secs(60);
    let reply = if matches!(fault, DrainCapacityLoss::Pinned) {
        gate.notify_one();
        let (status, reply) = timeout(Duration::from_secs(6), migration)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, 200, "{reply}");
        Some(reply)
    } else if let DrainCapacityLoss::InputAck { commit_first } = fault {
        // Fail the input-pin acknowledgement before the backend is invoked,
        // with and without the input becoming durable on the Coordinator.
        *rig.source.backend.restore_input_publication.lock().unwrap() = Some(commit_first);
        gate.notify_one();
        let (status, reply) = timeout(Duration::from_secs(6), migration)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, 503, "{reply}");
        let task = rig.session.upgrade_task().await.unwrap().unwrap();
        let op = &task.operations["migrate"];
        assert_eq!(op.phase, "Deferring");
        let paused = rig.session.get("sleep").await.unwrap();
        let record = paused.result.unwrap();
        assert_eq!(record.state, adx_core::EnvironmentState::Paused);
        assert_eq!(
            record.revision,
            op.source_stopped_revision.unwrap() + u64::from(commit_first)
        );
        assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
        None
    } else if matches!(fault, DrainCapacityLoss::ResultAck) {
        rig.source
            .backend
            .lose_restore_result_ack
            .store(true, Ordering::SeqCst);
        gate.notify_one();
        let (status, reply) = timeout(Duration::from_secs(6), migration)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, 503, "{reply}");
        let task = rig.session.upgrade_task().await.unwrap().unwrap();
        assert_eq!(task.operations["migrate"].phase, "Deferring");
        let restored = rig.session.get("sleep").await.unwrap().result.unwrap();
        assert_eq!(restored.state, adx_core::EnvironmentState::Running);
        assert_eq!(
            restored.last_operation.unwrap().kind,
            adx_core::LifecycleKind::Resume
        );
        let mut source = pb::node_service_client::NodeServiceClient::new(
            channel(&rig.source.report.node_address, Principal::ApiServer).await,
        );
        source
            .update_network_policy(pb::UpdateNetworkPolicyRequest {
                assignment: Some(original.clone()),
                caller: caller(),
                operation_id: "user-network-policy".into(),
                expected_revision: restored.revision,
                network: Some(adx_core::execution::NetworkPolicy::default().into()),
            })
            .await
            .unwrap();
        None
    } else {
        // Drop the client's connection while the Coordinator-owned operation
        // is at the checkpoint barrier. Simulate a temporary local capacity
        // change so the first restore cannot settle the durable deferral.
        rig.source
            .manager
            .update_capacity(Resources::default(), period)
            .unwrap();
        migration.abort();
        assert!(migration.await.unwrap_err().is_cancelled());
        gate.notify_one();
        let (status, reply) = timeout(
            Duration::from_secs(6),
            upgrade_response(address, "POST", MIGRATE_PATH, migrate.to_string()),
        )
        .await
        .unwrap();
        assert!(matches!(status, 429 | 503), "{status}: {reply}");
        let task = rig.session.upgrade_task().await.unwrap().unwrap();
        assert_eq!(task.operations["migrate"].phase, "Deferring");
        let paused = rig.session.get("sleep").await.unwrap();
        assert_eq!(paused.assignment.generation, original.generation);
        assert_eq!(
            paused.result.unwrap().state,
            adx_core::EnvironmentState::Paused
        );
        assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
        let target = &rig.session.snapshot().await.unwrap().nodes["b"];
        let delayed = adx_core::Assignment {
            node_id: "b".into(),
            shard_id: target.shard_id,
            generation: paused.assignment.generation + 1,
            ..paused.assignment.clone()
        };
        assert_eq!(
            rig.session
                .reserve_migration(
                    &paused.assignment,
                    delayed,
                    1,
                    true,
                    &source_session,
                    &target_session,
                )
                .await,
            Err(adx_core::Error::Conflict)
        );
        assert_eq!(
            rig.session.get("sleep").await.unwrap().assignment,
            paused.assignment
        );
        rig.source
            .manager
            .update_capacity(spec().resources, period)
            .unwrap();
        None
    };
    let replay = post_upgrade(address, MIGRATE_PATH, migrate).await;
    if let Some(reply) = reply {
        assert_eq!(replay, reply);
    }
    assert_eq!(replay["outcome"], "deferred");
    assert_eq!(replay["completed"], false);
    assert_eq!(replay["environment_id"], "sleep");
    assert_eq!(replay["node_id"], "a");
    assert_eq!(replay["generation"], original.generation);
    let task = rig.session.upgrade_task().await.unwrap().unwrap();
    assert_eq!(task.phase, "Active");
    assert_eq!(task.nodes["a"].phase, "draining");
    assert_eq!(task.operations["migrate"].phase, "Deferred");
    assert!(!rig.source.manager.accepting_allocations());
    let restored = rig.session.get("sleep").await.unwrap();
    assert_eq!(restored.assignment.generation, original.generation);
    let restored = restored.result.unwrap();
    assert_eq!(restored.state, adx_core::EnvironmentState::Running);
    if matches!(fault, DrainCapacityLoss::ResultAck) {
        assert_eq!(restored.last_operation.unwrap().id, "user-network-policy");
        assert!(restored.spec.execution.network.is_some());
    }
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 2);
    let resumed = rig.resume(true).await.unwrap().record.unwrap();
    assert_eq!(resumed.assignment.unwrap(), original);
    assert_eq!(resumed.state, pb::EnvironmentState::Running as i32);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 2);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn drain_defers_an_accepted_migration_without_resuming_a_user_paused_source() {
    deferral_preserves_user_pause(true).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn evacuation_defers_an_accepted_migration_without_resuming_a_user_paused_source() {
    deferral_preserves_user_pause(false).await;
}

async fn deferral_preserves_user_pause(drain: bool) {
    let rig = Rig::new(Duration::from_secs(4)).await;
    let before = rig.session.get("sleep").await.unwrap();
    let checkpoint = before.result.as_ref().unwrap();
    let (address, server) = if drain {
        let (address, server, _, _) = prepare_review_drain(&rig).await;
        (address, server)
    } else {
        let (address, server, _) =
            prepare_migration(&rig, checkpoint.clone().try_into().unwrap()).await;
        (address, server)
    };
    let source_session = &rig.source.report.session_id;
    let target_session = &rig.target.report.session_id;
    let mut task = rig.session.upgrade_task().await.unwrap().unwrap();
    let revision = task.revision;
    task.operations.insert(
        "migrate".into(),
        adx_coordinator::storage::UpgradeOperation {
            environment_id: "sleep".into(),
            source_node_id: "a".into(),
            target_node_id: "b".into(),
            automatic_target: true,
            source_generation: before.assignment.generation,
            source_revision: checkpoint.revision,
            source_stopped_revision: None,
            source_session_id: source_session.clone(),
            target_session_id: target_session.clone(),
            was_running: false,
            phase: "Prepared".into(),
        },
    );
    rig.session
        .save_upgrade(task, Some(revision))
        .await
        .unwrap();
    take_target_capacity(&rig).await;
    let receipt = post_upgrade(
        address,
        MIGRATE_PATH,
        json!({
            "task_id":"planned-migration", "epoch":1, "operation_id":"migrate",
            "node_id":"a", "node_session_id":source_session, "pod_uid":"source-pod",
            "environment_id":"sleep", "defer_on_capacity": !drain
        }),
    )
    .await;
    assert_eq!(receipt["outcome"], "deferred");
    assert_eq!(receipt["generation"], before.assignment.generation);
    assert_eq!(rig.session.get("sleep").await.unwrap(), before);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    if drain {
        assert_eq!(
            rig.resume(false).await.unwrap().record.unwrap().state,
            pb::EnvironmentState::Running as i32
        );
    }
    server.abort();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn abort_restores_prepared_drain_on_original_source_after_target_disconnects() {
    let mut rig = Rig::new(Duration::from_secs(4)).await;
    let running = rig.resume(false).await.unwrap().record.unwrap();
    let original: adx_core::Assignment = running.assignment.clone().unwrap().try_into().unwrap();
    let (address, server, source_session, _) = prepare_review_drain(&rig).await;
    let migrate = json!({
        "task_id":"planned-migration", "epoch":1, "operation_id":"migrate",
        "node_id":"a","node_session_id":source_session,"pod_uid":"source-pod",
        "environment_id":"sleep", "expected_source_generation":original.generation,
        "expected_source_revision":running.revision, "checkpoint_storage_alias":"obs"
    });
    let gate = Arc::new(Notify::new());
    *rig.source.backend.checkpoint_gate.lock().unwrap() = Some(gate.clone());
    let body = migrate.to_string();
    let migration =
        tokio::spawn(async move { upgrade_response(address, "POST", MIGRATE_PATH, body).await });
    timeout(
        Duration::from_secs(3),
        rig.source.backend.checkpoint_entered.notified(),
    )
    .await
    .unwrap();
    let accepted = rig.session.upgrade_task().await.unwrap().unwrap();
    assert_eq!(accepted.operations["migrate"].phase, "Prepared");
    assert_eq!(accepted.operations["migrate"].target_node_id, "b");

    // Unlike a definitive NoCapacity, a lost target inspection keeps the
    // operation pending for retry or an explicit Abort.
    rig.target.task.abort();
    assert!((&mut rig.target.task).await.unwrap_err().is_cancelled());
    gate.notify_one();
    let (denied, body) = timeout(Duration::from_secs(6), migration)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(denied, 503, "{body}");
    let paused = rig.session.get("sleep").await.unwrap();
    assert_eq!(paused.assignment, original);
    let paused_record = paused.result.unwrap();
    assert_eq!(paused_record.state, adx_core::EnvironmentState::Paused);
    assert!(paused_record.checkpoint.is_some());

    let freeze = json!({"task_id":"planned-migration", "epoch":1, "operation_id":"freeze", "stage":"freeze"});
    let (status, frozen) = upgrade_response(
        address,
        "POST",
        "tasks/planned-migration/abort",
        freeze.to_string(),
    )
    .await;
    assert_eq!(status, 200, "{frozen}");
    assert_eq!(frozen["phase"], "Aborting");
    assert_eq!(frozen["operations"]["migrate"]["phase"], "Cancelled");
    let again = post_upgrade(address, "tasks/planned-migration/abort", freeze).await;
    assert_eq!(again["operations"]["migrate"]["phase"], "Cancelled");
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 2);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    assert!(!rig.source.manager.accepting_allocations());

    let restore = json!({
        "task_id":"planned-migration", "epoch":1, "operation_id":"restore", "stage":"restore-original",
        "node_id":"a","node_session_id":source_session,"pod_uid":"source-pod"
    });
    let released = post_upgrade(address, "tasks/planned-migration/abort", restore).await;
    assert_eq!(released["restored_original"], true);
    let final_state = rig.session.get("sleep").await.unwrap();
    assert_eq!(final_state.assignment, original);
    assert!(final_state.resume.is_none());
    assert_eq!(
        final_state.result.unwrap().state,
        adx_core::EnvironmentState::Running
    );
    assert!(rig.source.manager.accepting_allocations());
    server.abort();
}

async fn prepare_review_drain(
    rig: &Rig,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<std::io::Result<()>>,
    String,
    String,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(adx_coordinator::autoscaling::serve(
        listener,
        rig.rpc.clone(),
    ));
    let source_session = rig.source.report.session_id.clone();
    let target_session = rig.target.report.session_id.clone();
    post_upgrade(address, "tasks", json!({
        "task_id":"planned-migration", "epoch":1, "namespace":"test", "daemonset_uid":"daemonset",
        "target_image":format!("image@sha256:{}", "d".repeat(64)), "nodes":[
            {"node_id":"a","node_session_id":source_session,"pod_uid":"source-pod"},
            {"node_id":"b","node_session_id":target_session,"pod_uid":"target-pod"}
        ]
    })).await;
    post_upgrade(
        address,
        "tasks/planned-migration/drain-node",
        json!({
            "task_id":"planned-migration", "epoch":1, "operation_id":"prepare",
            "node_id":"a","node_session_id":source_session,"pod_uid":"source-pod"
        }),
    )
    .await;
    (address, server, source_session, target_session)
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn abort_preserves_user_pause_after_migration_acceptance() {
    let rig = Rig::new(Duration::from_secs(4)).await;
    let running = rig.resume(false).await.unwrap().record.unwrap();
    let assignment = running.assignment.unwrap();
    let (address, server, source_session, target_session) = prepare_review_drain(&rig).await;
    let mut task = rig.session.upgrade_task().await.unwrap().unwrap();
    let revision = task.revision;
    task.operations.insert(
        "migrate".into(),
        adx_coordinator::storage::UpgradeOperation {
            environment_id: "sleep".into(),
            source_node_id: "a".into(),
            target_node_id: "b".into(),
            automatic_target: true,
            source_generation: assignment.generation,
            source_revision: running.revision,
            source_stopped_revision: None,
            source_session_id: source_session.clone(),
            target_session_id: target_session,
            was_running: true,
            phase: "Prepared".into(),
        },
    );
    rig.session
        .save_upgrade(task, Some(revision))
        .await
        .unwrap();
    let mut node = pb::node_service_client::NodeServiceClient::new(
        channel(&rig.source.report.node_address, Principal::ApiServer).await,
    );
    node.pause_environment(pb::PauseEnvironmentRequest {
        assignment: Some(assignment),
        caller: caller(),
        operation_id: "user-pause".into(),
        expected_revision: running.revision,
        timeout_seconds: 10,
        retention: Some(adx_core::lifecycle::RetentionPolicy::Ttl { ttl_seconds: 3600 }.into()),
        ..Default::default()
    })
    .await
    .unwrap();
    let (status, _) = upgrade_response(
        address,
        "POST",
        MIGRATE_PATH,
        json!({"task_id":"planned-migration", "epoch":1, "operation_id":"migrate",
               "environment_id":"sleep", "node_id":"a", "node_session_id":source_session,
               "pod_uid":"source-pod"})
        .to_string(),
    )
    .await;
    assert_eq!(status, 409);
    let frozen = post_upgrade(
        address,
        "tasks/planned-migration/abort",
        json!({"task_id":"planned-migration", "epoch":1, "operation_id":"freeze", "stage":"freeze"}),
    )
    .await;
    assert_eq!(frozen["operations"]["migrate"]["phase"], "Cancelled");
    let current = rig.session.get("sleep").await.unwrap();
    assert_eq!(current.assignment.node_id, "a");
    let record = current.result.unwrap();
    assert_eq!(record.state, adx_core::EnvironmentState::Paused);
    assert_eq!(record.checkpoint.unwrap().id, "user-pause");
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 1);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    server.abort();
}
