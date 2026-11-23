use super::*;
use tokio::time::{interval_at, timeout, Instant};

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn manual_pause_after_migration_prepared_stays_paused_on_target() {
    let mut rig = Rig::new(Duration::from_secs(4)).await;
    let running = rig.resume(false).await.unwrap().record.unwrap();
    let (address, server, migrate) = prepare_migration(&rig, running.clone()).await;
    rig.source
        .backend
        .migration_busy
        .store(true, Ordering::SeqCst);
    let (status, body) = upgrade_response(address, "POST", MIGRATE_PATH, migrate.to_string()).await;
    assert_eq!(status, 503, "{body}");
    let task = upgrade_request(address, "GET", "tasks/planned-migration", String::new()).await;
    assert_eq!(task["operations"]["migrate"]["phase"], "Prepared");
    assert_eq!(task["operations"]["migrate"]["was_running"], true);

    let mut source = pb::node_service_client::NodeServiceClient::new(
        channel(&rig.source.report.node_address, Principal::ApiServer).await,
    );
    let paused = source
        .pause_environment(pb::PauseEnvironmentRequest {
            assignment: running.assignment,
            caller: caller(),
            operation_id: "pause-during-migration".into(),
            expected_revision: running.revision,
            retention: Some(adx_core::lifecycle::RetentionPolicy::Ttl { ttl_seconds: 600 }.into()),
            timeout_seconds: 10,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .record
        .unwrap();
    let checkpoint = paused.checkpoint.unwrap();
    assert_eq!(checkpoint.id, "pause-during-migration");
    assert!(checkpoint.expires_at_unix_seconds.is_some());

    let operation = migrate.clone();
    let moving = tokio::spawn(async move { post_upgrade(address, MIGRATE_PATH, operation).await });
    let period = Duration::from_secs(60);
    let next_tick = Instant::now() + period;
    let mut source_tick = interval_at(next_tick, period);
    let mut target_tick = interval_at(next_tick, period);
    timeout(Duration::from_secs(3), async {
        tokio::join!(
            rig.source.manager.wait_for_reconciliation(&mut source_tick),
            rig.target.manager.wait_for_reconciliation(&mut target_tick),
        )
    })
    .await
    .unwrap();
    report(&mut rig.target, &rig.address, period).await.unwrap();
    report(&mut rig.source, &rig.address, period).await.unwrap();

    let moved = timeout(Duration::from_secs(3), moving)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(moved["node_id"], "b");
    assert_eq!(moved["state"], "Paused");
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 0);
    let record = rig.session.get("sleep").await.unwrap().result.unwrap();
    let transferred = record.checkpoint.unwrap();
    assert_eq!(transferred.id, checkpoint.id);
    assert_eq!(
        transferred.expires_at_unix_seconds,
        checkpoint.expires_at_unix_seconds
    );
    assert_eq!(post_upgrade(address, MIGRATE_PATH, migrate).await, moved);

    let resumed = rig.resume(false).await.unwrap().record.unwrap();
    assert_eq!(resumed.state, pb::EnvironmentState::Running as i32);
    assert_eq!(resumed.assignment.unwrap().node_id, "b");
    assert_eq!(rig.target.backend.restored.load(Ordering::SeqCst), 1);
    server.abort();
}
