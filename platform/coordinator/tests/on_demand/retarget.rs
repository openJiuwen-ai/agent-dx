use super::*;
use adx_core::scheduling::LabelSelector;
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
