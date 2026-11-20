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

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn drain_defers_and_resumes_source_when_reselection_finds_no_target() {
    drain_capacity_loss(false, None).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn drain_defers_and_resumes_source_when_a_pinned_claim_cannot_dispatch() {
    drain_capacity_loss(true, None).await;
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn drain_deferral_retries_a_restore_input_with_unconfirmed_publication() {
    for commit_first in [false, true] {
        drain_capacity_loss(false, Some(commit_first)).await;
    }
}

async fn drain_capacity_loss(pinned: bool, lost_input_ack: Option<bool>) {
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
    if pinned {
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
    let reply = if pinned {
        gate.notify_one();
        let (status, reply) = timeout(Duration::from_secs(6), migration)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(status, 200, "{reply}");
        Some(reply)
    } else if let Some(commit_first) = lost_input_ack {
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
    assert_eq!(
        restored.result.unwrap().state,
        adx_core::EnvironmentState::Running
    );
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
    let rig = Rig::new(Duration::from_secs(4)).await;
    let before = rig.session.get("sleep").await.unwrap();
    let checkpoint = before.result.as_ref().unwrap();
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
            source_generation: before.assignment.generation,
            source_revision: checkpoint.revision,
            source_stopped_revision: None,
            source_session_id: source_session.clone(),
            target_session_id: target_session,
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
            "environment_id":"sleep"
        }),
    )
    .await;
    assert_eq!(receipt["outcome"], "deferred");
    assert_eq!(receipt["generation"], before.assignment.generation);
    assert_eq!(rig.session.get("sleep").await.unwrap(), before);
    assert_eq!(rig.source.backend.restored.load(Ordering::SeqCst), 0);
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    assert_eq!(
        rig.resume(false).await.unwrap().record.unwrap().state,
        pb::EnvironmentState::Running as i32
    );
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
