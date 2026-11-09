use super::*;
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn concurrent_initial_publications_with_artifacts_preserve_every_result() {
    const COUNT: usize = 512;
    let rig = common::Redis::new().await;
    // Keep the fixture's 300ms default from masking CAS contention with Redis
    // transport timeouts during this debug-build burst. Production is unchanged.
    let store =
        adx_coordinator::storage::RedisStore::connect(&rig.url, "test", Duration::from_secs(5))
            .await
            .unwrap();
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    let barrier = Arc::new(tokio::sync::Barrier::new(COUNT));
    let mut tasks = Vec::with_capacity(COUNT);
    for index in 0..COUNT {
        let id = format!("artifact-publication-{index}");
        let request = format!("create-{index}");
        let specification = spec(&id);
        let assignment = Assignment {
            environment_id: id,
            node_id: "node".into(),
            shard_id: 0,
            generation: index as u64 + 1,
            devices: vec![],
        };
        session
            .reserve_with_request(specification.clone(), assignment.clone(), &request)
            .await
            .unwrap();
        let mut record = running(specification, assignment);
        record
            .execution_artifacts
            .push(adx_core::CheckpointArtifact {
                storage: "shared".into(),
                scope: String::new(),
                location: format!("checkpoint-input-{index}"),
                size_bytes: 64,
            });
        let session = session.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let result = session.publish_initial(record.clone(), &request, "").await;
            (record, request, result)
        }));
    }
    let mut published = Vec::with_capacity(COUNT);
    let mut failures = Vec::new();
    for task in tasks {
        let (record, request, result) = task.await.unwrap();
        match result {
            Ok(result) => {
                assert_eq!(result, record);
                published.push((record, request));
            }
            Err(error) => failures.push(error.to_string()),
        }
    }
    let mut failure_counts = std::collections::BTreeMap::new();
    for error in &failures {
        *failure_counts.entry(error).or_insert(0) += 1;
    }
    assert!(
        failures.is_empty(),
        "{} of {COUNT} initial artifact publications failed: {:?}",
        failures.len(),
        failure_counts
    );
    assert_eq!(published.len(), COUNT);
    for (record, request) in published {
        assert_eq!(
            session.get(&record.spec.id).await.unwrap().result,
            Some(record.clone())
        );
        assert_eq!(
            session.published_request("tenant", &request).await.unwrap(),
            Some(record)
        );
    }
    assert_eq!(session.shared_artifacts().await.unwrap().len(), COUNT);
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn first_publication_preserves_native_labels_and_optional_run_policy() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut spec = spec("metadata-publication");
    spec.run_policy = Some(adx_core::environment_control::RunPolicy {
        timeout_seconds: 60,
        action: adx_core::environment_control::RunExpiryAction::Delete,
    });
    let assignment = Assignment {
        environment_id: spec.id.clone(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    spec.scheduling
        .labels
        .insert("purpose".into(), "original".into());
    let labels = spec.scheduling.labels.clone();
    session
        .reserve_with_request(spec.clone(), assignment.clone(), "metadata-request")
        .await
        .unwrap();
    let record = running(spec, assignment);
    session
        .publish_initial(record.clone(), "metadata-request", "")
        .await
        .unwrap();
    let stored = session.get(&record.spec.id).await.unwrap();
    assert_eq!(stored.spec.scheduling.labels, labels);
    assert_eq!(stored.spec.run_policy, record.spec.run_policy);
    assert_eq!(
        session
            .published_request("tenant", "metadata-request")
            .await
            .unwrap(),
        Some(record)
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn initial_success_is_atomic_by_environment_or_request_and_survives_restart() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let make = |id: &str, generation| {
        running(
            spec(id),
            Assignment {
                environment_id: id.into(),
                node_id: "node".into(),
                shard_id: 0,
                generation,
                devices: vec![],
            },
        )
    };
    let first = make("first", 1);
    let other = make("other", 2);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let one = {
        let session = session.clone();
        let barrier = barrier.clone();
        let record = first.clone();
        async move {
            barrier.wait().await;
            session
                .publish_initial(record, "request", "")
                .await
                .unwrap()
        }
    };
    let two = {
        let session = session.clone();
        let record = other;
        async move {
            barrier.wait().await;
            session
                .publish_initial(record, "request", "")
                .await
                .unwrap()
        }
    };
    let (one, two) = tokio::join!(one, two);
    assert_eq!(one, two);
    assert_eq!(
        session.business_counters().await.unwrap().creations,
        [1, 0, 0]
    );
    assert_eq!(session.snapshot().await.unwrap().environments.len(), 1);
    let mut duplicate = one.clone();
    duplicate.assignment.generation += 10;
    duplicate.runtime.id = format!("{}-{}", duplicate.spec.id, duplicate.assignment.generation);
    assert_eq!(
        session
            .publish_initial(duplicate, "another-request", "")
            .await
            .unwrap(),
        one
    );
    rig.crash();
    rig.start().await;
    let recovered = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        recovered.business_counters().await.unwrap().creations,
        [1, 0, 0]
    );
    assert_eq!(
        recovered
            .published_request("tenant", "request")
            .await
            .unwrap(),
        Some(one.clone())
    );
    assert_eq!(
        recovered
            .published_request("tenant", "another-request")
            .await
            .unwrap(),
        Some(one)
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn independent_environment_commits_do_not_retry_a_global_version() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut records = vec![];
    for generation in 1..=16 {
        let id = format!("independent-{generation}");
        let record = running(
            spec(&id),
            Assignment {
                environment_id: id,
                node_id: "node".into(),
                shard_id: 0,
                generation,
                devices: vec![],
            },
        );
        session
            .publish_initial(record.clone(), "", "")
            .await
            .unwrap();
        records.push(record);
    }
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let calls = |info: &str| -> u64 {
        info.lines()
            .find_map(|line| {
                line.strip_prefix("cmdstat_eval:calls=")
                    .and_then(|s| s.split(',').next())
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(0)
    };
    let before: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(&mut connection)
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(records.len()));
    let mut tasks = vec![];
    for mut record in records {
        let session = session.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            record.revision += 1;
            record.state = EnvironmentState::Failed;
            record.runtime.ip = None;
            record.resources_held = false;
            record.observation = Some(adx_core::observation::ExecutionObservation {
                interruption: Some(adx_core::observation::InterruptionEvidence {
                    event_id: format!("exit:{}", record.spec.id),
                    detected_at_unix_millis: adx_core::observation::now_millis().unwrap(),
                    assignment_generation: record.assignment.generation,
                    runtime_id: record.runtime.id.clone(),
                    worker_id: record.assignment.node_id.clone(),
                    reason: adx_core::observation::InterruptionReason::Oom,
                    evidence_source: adx_core::observation::EvidenceSource::BackendExit,
                    exit_code: Some(137),
                    detail: "confirmed exit".into(),
                }),
                ..Default::default()
            });
            barrier.wait().await;
            session.commit(record).await.unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let after: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        calls(&after) - calls(&before),
        16,
        "unrelated records must each commit in one CAS attempt"
    );
    let counters = session.business_counters().await.unwrap();
    assert_eq!(counters.creations, [16, 0, 0]);
    assert_eq!(counters.interruptions[2][0], 16);
    assert_eq!(
        rig.store()
            .await
            .begin(1)
            .await
            .unwrap()
            .business_counters()
            .await
            .unwrap(),
        counters
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn new_epoch_rejects_delayed_initial_publication_without_a_pending_record() {
    let rig = common::Redis::new().await;
    let old = rig.store().await.begin(1).await.unwrap();
    register(&old, "node").await;
    let record = running(
        spec("lost"),
        Assignment {
            environment_id: "lost".into(),
            node_id: "node".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        },
    );
    let current = rig.store().await.begin(1).await.unwrap();
    assert!(old
        .publish_initial(record.clone(), "request", "")
        .await
        .is_err());
    assert!(current.snapshot().await.unwrap().environments.is_empty());
    current
        .publish_initial(record.clone(), "request", "")
        .await
        .unwrap();
    assert_eq!(
        current
            .published_request("tenant", "request")
            .await
            .unwrap(),
        Some(record)
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn same_environment_racing_requests_reuse_the_winner_after_crash() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut tasks = vec![];
    let barrier = Arc::new(tokio::sync::Barrier::new(12));
    for index in 0..12 {
        let session = session.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let mut candidate = spec("shared");
            candidate.image = format!("image-{index}");
            let record = running(
                candidate,
                Assignment {
                    environment_id: "shared".into(),
                    node_id: "node".into(),
                    shard_id: 0,
                    generation: index + 1,
                    devices: vec![],
                },
            );
            barrier.wait().await;
            session
                .publish_initial(record, &format!("request-{index}"), "")
                .await
                .unwrap()
        }));
    }
    let mut winners = vec![];
    for task in tasks {
        winners.push(task.await.unwrap());
    }
    assert!(winners.iter().all(|winner| winner == &winners[0]));
    rig.crash();
    rig.start().await;
    let recovered = rig.store().await.begin(1).await.unwrap();
    for index in 0..12 {
        assert_eq!(
            recovered
                .published_request("tenant", &format!("request-{index}"))
                .await
                .unwrap(),
            Some(winners[0].clone())
        );
    }
    let mut deleted = winners[0].clone();
    deleted.state = EnvironmentState::Deleted;
    deleted.resources_held = false;
    deleted.runtime.ip = None;
    deleted.revision += 2;
    recovered.commit(deleted).await.unwrap();
    let receipt = recovered
        .published_request("tenant", "request-0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.state, EnvironmentState::Deleted);
    assert!(!receipt.resources_held);
    assert_eq!(recovered.get("shared").await, Err(Error::NotFound));
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn node_expiry_cannot_miss_concurrent_initial_publications() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    session
        .register_session(
            node("node"),
            "127.0.0.1:1".into(),
            "127.0.0.1:2".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(17));
    let mut tasks = vec![];
    for index in 0..16 {
        let session = session.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let id = format!("racing-{index}");
            let record = running(
                spec(&id),
                Assignment {
                    environment_id: id.clone(),
                    node_id: "node".into(),
                    shard_id: 0,
                    generation: index + 1,
                    devices: vec![],
                },
            );
            barrier.wait().await;
            session.publish_initial(record, &id, "boot").await
        }));
    }
    barrier.wait().await;
    session.invalidate_node("node", "boot").await.unwrap();
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) | Err(Error::Conflict) => (),
            Err(error) => panic!("{error:?}"),
        }
    }
    let saved = session.snapshot().await.unwrap();
    assert!(!saved.nodes["node"].node.available);
    for environment in saved.environments.values() {
        assert!(
            environment.invalidated,
            "expiry omitted an initial publication"
        );
        assert_eq!(
            environment.result.as_ref().unwrap().state,
            EnvironmentState::Failed
        );
        assert!(!environment.resources_held());
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn returning_node_rearms_publication_after_reconciliation() {
    for initially_routable in [false, true] {
        let rig = common::Redis::new().await;
        let session = rig.store().await.begin(1).await.unwrap();
        let report = |sequence, routable| {
            Some(adx_coordinator::storage::NodeSession {
                id: "boot".into(),
                sequence,
                routable,
            })
        };
        let mut initial_node = node("node");
        initial_node.available = initially_routable;
        session
            .register_session(
                initial_node,
                "127.0.0.1:1".into(),
                "127.0.0.1:2".into(),
                report(1, initially_routable),
            )
            .await
            .unwrap();
        session
            .register_session(
                node("node"),
                "127.0.0.1:1".into(),
                "127.0.0.1:2".into(),
                report(2, true),
            )
            .await
            .unwrap();
        session.invalidate_node("node", "boot").await.unwrap();
        let record = running(
            spec("after-return"),
            Assignment {
                environment_id: "after-return".into(),
                node_id: "node".into(),
                shard_id: 0,
                generation: 1,
                devices: vec![],
            },
        );
        assert_eq!(
            session
                .publish_initial(record.clone(), "request", "boot")
                .await
                .unwrap_err(),
            Error::Conflict
        );
        session
            .register_session(
                node("node"),
                "127.0.0.1:1".into(),
                "127.0.0.1:2".into(),
                report(3, false),
            )
            .await
            .unwrap();
        assert_eq!(
            session
                .publish_initial(record.clone(), "request", "boot")
                .await
                .unwrap_err(),
            Error::Conflict
        );
        session
            .register_session(
                node("node"),
                "127.0.0.1:1".into(),
                "127.0.0.1:2".into(),
                report(4, true),
            )
            .await
            .unwrap();
        assert_eq!(
            session
                .publish_initial(record.clone(), "request", "boot")
                .await
                .unwrap(),
            record
        );
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn initial_publication_cannot_overwrite_another_pending_assignment() {
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    register(&s, "node").await;
    let assignment = Assignment {
        environment_id: "pending".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 2,
        devices: vec![],
    };
    s.reserve(spec("pending"), assignment.clone())
        .await
        .unwrap();
    for generation in [1, 3] {
        let late = running(
            spec("pending"),
            Assignment {
                generation,
                ..assignment.clone()
            },
        );
        assert!(s.publish_initial(late, "late", "").await.is_err());
    }
    assert_eq!(s.get("pending").await.unwrap().assignment, assignment);
    assert!(s.get("pending").await.unwrap().result.is_none());
}
