use super::*;

pub(super) async fn business_header(
    rig: &common::Redis,
    edit: Option<fn(&mut serde_json::Value)>,
) -> serde_json::Value {
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let key = "adx:{test}:control:v1";
    let raw: String = redis::cmd("HGET")
        .arg(key)
        .arg("header")
        .query_async(&mut connection)
        .await
        .unwrap();
    let mut header = serde_json::from_str(&raw).unwrap();
    if let Some(edit) = edit {
        edit(&mut header);
        redis::cmd("HSET")
            .arg(key)
            .arg("header")
            .arg(header.to_string())
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
    }
    header
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn creation_observation_is_durable_and_not_recounted_by_replay() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    let assignment = Assignment {
        environment_id: "observed-create".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("observed-create"), assignment.clone())
        .await
        .unwrap();
    let result = running(spec("observed-create"), assignment);
    session.commit(result.clone()).await.unwrap();
    session.commit(result).await.unwrap();
    let saved = serde_json::to_value(session.get("observed-create").await.unwrap()).unwrap();
    assert_eq!(saved["creation"]["result"], "success");
    let restarted = store.begin(1).await.unwrap();
    let saved = serde_json::to_value(restarted.get("observed-create").await.unwrap()).unwrap();
    assert_eq!(saved["creation"]["result"], "success");
    assert_eq!(
        restarted.business_counters().await.unwrap().creations,
        [1, 0, 0]
    );
    assert_eq!(session.business_counters().await, Err(Error::Conflict));
    let missing_starts: [fn(&mut serde_json::Value); 3] = [
        |header| {
            header.as_object_mut().unwrap().remove("observability");
        },
        |header| {
            header["observability"]
                .as_object_mut()
                .unwrap()
                .remove("started_at_unix_millis");
        },
        |header| header["observability"]["started_at_unix_millis"] = 0.into(),
    ];
    for edit in missing_starts {
        business_header(&rig, Some(edit)).await;
        let earliest = adx_core::observation::now_millis().unwrap();
        let counters = restarted.business_counters().await.unwrap();
        assert!(counters.started_at_unix_millis >= earliest);
        assert!(counters.started_at_unix_millis <= adx_core::observation::now_millis().unwrap());
        let persisted = business_header(&rig, None).await;
        assert_eq!(
            persisted["observability"]["started_at_unix_millis"],
            counters.started_at_unix_millis
        );
        assert_eq!(
            persisted["observability"]["creations"],
            serde_json::json!([0, 0, 0])
        );
        assert_eq!(counters.creations, [1, 0, 0]);
        assert_eq!(restarted.business_counters().await.unwrap(), counters);
    }
}

fn interruption(
    record: &EnvironmentRecord,
    reason: adx_core::observation::InterruptionReason,
) -> adx_core::observation::InterruptionEvidence {
    adx_core::observation::InterruptionEvidence {
        event_id: format!(
            "execution:{}:{}",
            record.assignment.generation, record.runtime.id
        ),
        detected_at_unix_millis: adx_core::observation::now_millis().unwrap(),
        assignment_generation: record.assignment.generation,
        runtime_id: record.runtime.id.clone(),
        worker_id: record.assignment.node_id.clone(),
        reason,
        evidence_source: adx_core::observation::EvidenceSource::BackendExit,
        exit_code: Some(137),
        detail: "backend confirmed OOM".into(),
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn oom_recovery_keeps_evidence_and_counts_distinct_executions() {
    use adx_coordinator::storage::RecoveryState;
    use adx_core::observation::{ExecutionObservation, InterruptionReason};
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut spec = spec("oom");
    spec.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 3,
        initial_backoff_seconds: 1,
        max_backoff_seconds: 1,
    });
    let assignment = Assignment {
        environment_id: "oom".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec.clone(), assignment.clone())
        .await
        .unwrap();
    let mut record = running(spec, assignment);
    session.commit(record.clone()).await.unwrap();
    let original = interruption(&record, InterruptionReason::Oom);
    record.state = EnvironmentState::Failed;
    record.revision += 1;
    record.restart_pending = true;
    record.runtime.ip = None;
    record.observation = Some(ExecutionObservation {
        interruption: Some(original.clone()),
        ..Default::default()
    });
    session.commit(record.clone()).await.unwrap();
    session.commit(record.clone()).await.unwrap();
    assert_eq!(
        session.business_counters().await.unwrap().interruptions[2][0],
        1
    );
    let restarted = store.begin(1).await.unwrap();
    record.state = EnvironmentState::Running;
    record.revision += 2;
    record.restart_attempts = 1;
    record.restart_pending = false;
    record.runtime.id.push_str("-r1");
    record.runtime.ip = Some("10.0.0.2".parse().unwrap());
    record.failure = None;
    restarted.commit(record.clone()).await.unwrap();
    restarted.commit(record.clone()).await.unwrap();
    let saved = restarted.get("oom").await.unwrap();
    let summary = saved.last_interruption.unwrap();
    assert_eq!(summary.evidence, original);
    assert_eq!(summary.recovery_state, RecoveryState::Recovered);
    assert_eq!(
        restarted.business_counters().await.unwrap().recoveries,
        [1, 0, 0]
    );
    let next = interruption(&record, InterruptionReason::RuntimeExit);
    record.observation.as_mut().unwrap().interruption = Some(next.clone());
    record.state = EnvironmentState::Failed;
    record.revision += 1;
    record.restart_pending = true;
    record.runtime.ip = None;
    restarted.commit(record).await.unwrap();
    let counts = restarted.business_counters().await.unwrap();
    assert_eq!(counts.interruptions[2][0], 1);
    assert_eq!(counts.interruptions[2][1], 1);
    assert_eq!(counts.creations, [1, 0, 0]);
    assert_eq!(
        restarted
            .get("oom")
            .await
            .unwrap()
            .last_interruption
            .unwrap()
            .evidence,
        next
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn request_identity_is_settled_by_success_across_optional_claims() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    session
        .register_session(
            node("node"),
            "node:9000".into(),
            "node:9001".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let candidates = ["one", "two"]
        .into_iter()
        .map(|id| {
            (
                spec(id),
                adx_coordinator::storage::LocalClaim {
                    request_id: "same-request".into(),
                    node_id: "node".into(),
                    node_session_id: "boot".into(),
                    devices: vec![],
                },
            )
        })
        .collect();
    let results = session.claim_batch(candidates).await.unwrap();
    assert!(results.iter().all(Result::is_ok));
    assert!(session
        .published_request("tenant", "same-request")
        .await
        .unwrap()
        .is_none());
    let first = results[0].as_ref().unwrap().record().clone();
    let second = results[1].as_ref().unwrap().record().clone();
    let winner = session
        .publish_initial(
            running(first.spec, first.assignment),
            "same-request",
            "boot",
        )
        .await
        .unwrap();
    assert_eq!(
        session
            .publish_initial(
                running(second.spec, second.assignment),
                "same-request",
                "boot"
            )
            .await
            .unwrap(),
        winner
    );
    assert_eq!(
        session.business_counters().await.unwrap().creations,
        [1, 0, 0]
    );
    let mut deleted = winner;
    deleted.state = EnvironmentState::Deleted;
    deleted.resources_held = false;
    deleted.runtime.ip = None;
    deleted.revision += 1;
    session.commit(deleted).await.unwrap();
    // Successful request receipts survive deletion and prevent accidental resurrection.
    assert_eq!(
        session
            .published_request("tenant", "same-request")
            .await
            .unwrap()
            .unwrap()
            .state,
        EnvironmentState::Deleted
    );
    assert_eq!(
        session.business_counters().await.unwrap().creations,
        [1, 0, 0]
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn worker_incident_counts_only_running_executions_once() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    session
        .register_session(
            node("node"),
            "node:9000".into(),
            "node:9001".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    for index in 1..=22 {
        let id = format!("env-{index}");
        let assignment = Assignment {
            environment_id: id.clone(),
            node_id: "node".into(),
            shard_id: 0,
            generation: index,
            devices: vec![],
        };
        session
            .reserve(spec(&id), assignment.clone())
            .await
            .unwrap();
        if index <= 21 {
            let mut result = running(spec(&id), assignment);
            if index == 21 {
                result.state = EnvironmentState::Paused;
                result.resources_held = false;
                result.runtime.ip = None;
                result.checkpoint = Some(adx_core::RestorePoint {
                    origin: None,
                    id: "paused".into(),
                    source_runtime_id: result.runtime.id.clone(),
                    expires_at_unix_seconds: None,
                    artifact: adx_core::CheckpointArtifact {
                        scope: Default::default(),
                        storage: "shared".into(),
                        location: "checkpoint".into(),
                        size_bytes: 1,
                    },
                });
            }
            session.commit(result).await.unwrap();
        }
    }
    session.invalidate_node("node", "boot").await.unwrap();
    session.invalidate_node("node", "boot").await.unwrap();
    let c = session.business_counters().await.unwrap();
    assert_eq!(c.worker_incidents, 1);
    assert_eq!(c.interruptions[2][4], 20);
    assert!(session
        .get("env-21")
        .await
        .unwrap()
        .last_interruption
        .is_none());
    assert!(session
        .get("env-22")
        .await
        .unwrap()
        .last_interruption
        .is_none());
    assert_eq!(
        store
            .begin(1)
            .await
            .unwrap()
            .business_counters()
            .await
            .unwrap(),
        c
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn direct_recovery_running_carries_old_execution_without_failed_commit() {
    use adx_core::observation::{ExecutionObservation, InterruptionReason};
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let assignment = Assignment {
        environment_id: "direct".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("direct"), assignment.clone())
        .await
        .unwrap();
    let mut record = running(spec("direct"), assignment);
    session.commit(record.clone()).await.unwrap();
    let evidence = interruption(&record, InterruptionReason::Oom);
    record.runtime.id.push_str("-r4");
    record.revision += 3;
    record.observation = Some(ExecutionObservation {
        interruption: Some(evidence),
        ..Default::default()
    });
    session.commit(record.clone()).await.unwrap();
    session.commit(record).await.unwrap();
    let summary = session
        .get("direct")
        .await
        .unwrap()
        .last_interruption
        .unwrap();
    assert_eq!(
        summary.recovery_state,
        adx_coordinator::storage::RecoveryState::Recovered
    );
    let counts = session.business_counters().await.unwrap();
    assert_eq!(counts.interruptions[2][0], 1);
    assert_eq!(counts.recoveries, [1, 0, 0]);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn confirmed_creation_failure_and_timeout_are_not_runtime_interruptions() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    for (id, generation, reason) in [
        ("failed", 1, adx_core::RuntimeFailureReason::Exited),
        (
            "timed-out",
            2,
            adx_core::RuntimeFailureReason::DeadlineExceeded,
        ),
    ] {
        let assignment = Assignment {
            environment_id: id.into(),
            node_id: "node".into(),
            shard_id: 0,
            generation,
            devices: vec![],
        };
        session.reserve(spec(id), assignment.clone()).await.unwrap();
        let mut failed = running(spec(id), assignment);
        failed.state = EnvironmentState::Failed;
        failed.runtime.ip = None;
        failed.resources_held = false;
        failed.failure = Some(adx_core::RuntimeFailure {
            reason,
            exit_code: Some(1),
            message: "creation failed".into(),
            probe_failure: None,
        });
        session.commit(failed.clone()).await.unwrap();
        session.commit(failed).await.unwrap();
        assert!(session.get(id).await.unwrap().last_interruption.is_none());
    }
    let c = session.business_counters().await.unwrap();
    assert_eq!(c.creations, [0, 1, 1]);
    assert_eq!(c.interruptions, [[0; 7]; 4]);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn old_records_do_not_backfill_results_and_redis_reset_changes_coverage_start() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut connection = redis::Client::open(rig.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let assignment = Assignment {
        environment_id: "legacy".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("legacy"), assignment.clone())
        .await
        .unwrap();
    let key = "adx:{test}:control:v1";
    let mut old: serde_json::Value = serde_json::from_str(
        &redis::cmd("HGET")
            .arg(key)
            .arg("environment:legacy")
            .query_async::<String>(&mut connection)
            .await
            .unwrap(),
    )
    .unwrap();
    old.as_object_mut().unwrap().remove("creation");
    old.as_object_mut().unwrap().remove("last_interruption");
    redis::cmd("HSET")
        .arg(key)
        .arg("environment:legacy")
        .arg(old.to_string())
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    session
        .commit(running(spec("legacy"), assignment))
        .await
        .unwrap();
    let counters = session.business_counters().await.unwrap();
    assert_eq!(counters.creations, [0, 0, 0]);
    assert!(session.get("legacy").await.unwrap().creation.is_none());
    let started = counters.started_at_unix_millis;
    assert!(started > 0);
    assert_eq!(
        store
            .begin(1)
            .await
            .unwrap()
            .business_counters()
            .await
            .unwrap()
            .started_at_unix_millis,
        started
    );
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let reset = store
        .begin(1)
        .await
        .unwrap()
        .business_counters()
        .await
        .unwrap();
    assert!(reset.started_at_unix_millis > started);
    assert_eq!(reset.creations, [0, 0, 0]);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn unpublished_running_exit_does_not_block_terminal_commit() {
    use adx_core::observation::{ExecutionObservation, InterruptionReason};
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let assignment = Assignment {
        environment_id: "unpublished".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("unpublished"), assignment.clone())
        .await
        .unwrap();
    // The first Running publication was cancelled before journal persistence.
    let mut record = running(spec("unpublished"), assignment);
    let evidence = interruption(&record, InterruptionReason::RuntimeExit);
    record.state = EnvironmentState::Failed;
    record.revision += 1;
    record.resources_held = false;
    record.runtime.ip = None;
    record.observation = Some(ExecutionObservation {
        interruption: Some(evidence),
        ..Default::default()
    });
    let result = session.commit(record.clone()).await;
    assert!(
        result.is_ok(),
        "a real terminal result must settle the reservation, got {result:?}"
    );
    assert_eq!(
        session.business_counters().await.unwrap().creations,
        [0, 1, 0]
    );
    session.commit(record.clone()).await.unwrap();
    record.revision += 1;
    session.commit(record).await.unwrap();
    assert_eq!(
        session.business_counters().await.unwrap().interruptions,
        [[0; 7]; 4]
    );
    assert!(session
        .get("unpublished")
        .await
        .unwrap()
        .last_interruption
        .is_none());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn rejoined_same_process_counts_second_node_incident() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let mut incidents = std::collections::BTreeSet::new();
    for sequence in [1, 10] {
        session
            .register_session(
                node("node"),
                "node:9000".into(),
                "node:9001".into(),
                Some(adx_coordinator::storage::NodeSession {
                    id: "same-process".into(),
                    sequence,
                    routable: true,
                }),
            )
            .await
            .unwrap();
        let id = format!("round-{sequence}");
        let assignment = Assignment {
            environment_id: id.clone(),
            node_id: "node".into(),
            shard_id: 0,
            generation: sequence,
            devices: vec![],
        };
        session
            .reserve(spec(&id), assignment.clone())
            .await
            .unwrap();
        session
            .commit(running(spec(&id), assignment))
            .await
            .unwrap();
        session
            .invalidate_node("node", "same-process")
            .await
            .unwrap();
        let incident = session
            .get(&id)
            .await
            .unwrap()
            .last_interruption
            .unwrap()
            .incident_id
            .unwrap();
        assert!(
            incidents.insert(incident),
            "distinct outages require distinct incident IDs"
        );
        // A replay of the same loss is still idempotent.
        session
            .invalidate_node("node", "same-process")
            .await
            .unwrap();
        session
            .register_session(
                node("node"),
                "node:9000".into(),
                "node:9001".into(),
                Some(adx_coordinator::storage::NodeSession {
                    id: "same-process".into(),
                    sequence: sequence + 1,
                    routable: false,
                }),
            )
            .await
            .unwrap();
        session
            .invalidate_node("node", "same-process")
            .await
            .unwrap();
    }
    let counters = session.business_counters().await.unwrap();
    assert_eq!(counters.interruptions[2][4], 2);
    assert_eq!(
        counters.worker_incidents, 2,
        "a new loss after reconciliation is a second incident"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn committed_state_includes_atomic_summaries_and_delete_replay() {
    use adx_core::observation::{ExecutionObservation, InterruptionReason};
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let assignment = Assignment {
        environment_id: "commit-state".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("commit-state"), assignment.clone())
        .await
        .unwrap();
    let mut record = running(spec("commit-state"), assignment);
    let created = session.commit_with_state(record.clone()).await.unwrap();
    let stored = created.stored.unwrap();
    assert_eq!(
        stored.creation.as_ref().unwrap().result,
        adx_coordinator::storage::CreationResult::Success
    );
    assert_eq!(stored, session.get("commit-state").await.unwrap());
    assert_eq!(
        session
            .commit_with_state(record.clone())
            .await
            .unwrap()
            .stored,
        Some(stored)
    );
    let evidence = interruption(&record, InterruptionReason::Oom);
    record.state = EnvironmentState::Failed;
    record.resources_held = false;
    record.runtime.ip = None;
    record.revision += 1;
    record.observation = Some(ExecutionObservation {
        interruption: Some(evidence.clone()),
        ..Default::default()
    });
    let failed = session.commit_with_state(record.clone()).await.unwrap();
    let stored = failed.stored.unwrap();
    assert_eq!(
        stored.last_interruption.as_ref().unwrap().evidence,
        evidence
    );
    assert!(stored.failed_since_unix_seconds.is_some());
    assert_eq!(stored, session.get("commit-state").await.unwrap());
    assert_eq!(
        session
            .commit_with_state(record.clone())
            .await
            .unwrap()
            .stored,
        Some(stored)
    );
    record.state = EnvironmentState::Deleted;
    record.revision += 1;
    for _ in 0..2 {
        let deleted = session.commit_with_state(record.clone()).await.unwrap();
        assert_eq!(deleted.result, record);
        assert!(deleted.stored.is_none());
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn unpublished_reload_replacement_can_settle_failed() {
    use adx_core::observation::{ExecutionObservation, InterruptionReason, OperationIntent};
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let assignment = Assignment {
        environment_id: "reload-gap".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    let mut spec = spec("reload-gap");
    spec.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 1,
        initial_backoff_seconds: 1,
        max_backoff_seconds: 1,
    });
    session
        .reserve(spec.clone(), assignment.clone())
        .await
        .unwrap();
    let mut record = running(spec, assignment);
    session.commit(record.clone()).await.unwrap();
    record.revision += 1;
    record.observation = Some(ExecutionObservation {
        intent: Some(OperationIntent::User),
        ..Default::default()
    });
    session.commit(record.clone()).await.unwrap();
    // Reload has replaced A with B locally, but B's first Running publication timed out.
    record.runtime.id.push_str("-r4");
    record.revision += 4;
    let evidence = interruption(&record, InterruptionReason::RuntimeExit);
    record.state = EnvironmentState::Failed;
    record.restart_pending = true;
    record.runtime.ip = None;
    record.resources_held = false;
    record.observation = Some(ExecutionObservation {
        interruption: Some(evidence),
        ..Default::default()
    });
    // Evidence for an unrelated execution or owner must still be rejected.
    for field in ["runtime", "worker", "generation"] {
        let mut unrelated = record.clone();
        let evidence = unrelated
            .observation
            .as_mut()
            .unwrap()
            .interruption
            .as_mut()
            .unwrap();
        match field {
            "runtime" => evidence.runtime_id = "reload-gap-1-r99".into(),
            "worker" => evidence.worker_id = "other-node".into(),
            _ => evidence.assignment_generation += 1,
        }
        assert_eq!(session.commit(unrelated).await, Err(Error::Conflict));
    }
    for _ in 0..2 {
        session
            .commit(record.clone())
            .await
            .expect("valid replacement terminal result");
    }
    let saved = session.get("reload-gap").await.unwrap();
    assert_eq!(saved.result, Some(record.clone()));
    assert!(saved.last_interruption.is_none());
    let counters = session.business_counters().await.unwrap();
    assert_eq!(counters.interruptions, [[0; 7]; 4]);
    assert_eq!(counters.creations, [1, 0, 0]);
    // Persisted pre-availability evidence does not become an event on recovery.
    record.runtime.id = "reload-gap-1-r8".into();
    record.revision += 4;
    record.state = EnvironmentState::Running;
    record.restart_pending = false;
    record.restart_attempts = 1;
    record.resources_held = true;
    record.runtime.ip = Some("10.0.0.2".parse().unwrap());
    session.commit(record).await.unwrap();
    assert_eq!(session.business_counters().await.unwrap(), counters);
}
