use super::*;
use serde_json::json;
use tokio::time::{interval_at, timeout, Instant};

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
        if !restarted {
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

async fn restart_source(rig: &mut Rig, period: Duration) {
    // A same-Pod restart proves predecessor replacement at the same endpoint.
    let mut replacement = worker(
        "a",
        &rig.address,
        rig.remote.clone(),
        Some(&rig.source.report.node_address),
    )
    .await;
    report(&mut replacement, &rig.address, period)
        .await
        .unwrap();
    rig.source = replacement;
}

fn quiesce_request(rig: &Rig, rollback: bool) -> serde_json::Value {
    json!({"task_id": "planned-migration", "epoch": 1, "operation_id": "quiesce-restarted-source",
        "node_id": "a", "pod_uid": "source-pod", "node_session_id": rig.source.report.session_id,
        "rollback": rollback})
}
