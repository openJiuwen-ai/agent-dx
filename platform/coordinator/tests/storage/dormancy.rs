use super::*;

async fn paused_fixture() -> (
    common::Redis,
    adx_coordinator::storage::Session,
    EnvironmentRecord,
) {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "source").await;
    register(&session, "target").await;
    let assignment = Assignment {
        environment_id: "sleep".into(),
        node_id: "source".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    let specification = spec("sleep");

    session
        .reserve(specification.clone(), assignment.clone())
        .await
        .unwrap();
    let mut record = running(specification, assignment);
    record.state = EnvironmentState::Paused;
    record.resources_held = false;
    record.runtime.ip = None;
    record.checkpoint = Some(adx_core::RestorePoint {
        id: "pause".into(),
        origin: None,
        expires_at_unix_seconds: None,
        source_runtime_id: "sleep-1".into(),
        artifact: adx_core::CheckpointArtifact {
            scope: String::new(),
            storage: "obs".into(),
            location: "artifact".into(),
            size_bytes: 8,
        },
    });
    session.commit(record.clone()).await.unwrap();
    (rig, session, record)
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn traffic_wakes_manually_paused_environment_without_opt_in() {
    let (_rig, session, _) = paused_fixture().await;
    assert!(session.request_resume("sleep", 1).await.is_ok());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn permanent_pause_survives_coordinator_epoch_and_can_transfer_after_node_loss() {
    let (rig, session, record) = paused_fixture().await;
    session
        .register_session(
            node("source"),
            "source:1".into(),
            "source:2".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    session.invalidate_node("source", "boot").await.unwrap();
    let replacement = rig.store().await.begin(1).await.unwrap();
    let dormant = replacement.get("sleep").await.unwrap();
    assert_eq!(
        dormant.result.as_ref().unwrap().state,
        EnvironmentState::Paused
    );
    assert!(dormant.resume.is_none());
    replacement.request_resume("sleep", u64::MAX).await.unwrap();
    let moved = replacement
        .reserve_recovery(
            &record.assignment,
            Assignment {
                node_id: "target".into(),
                generation: 2,
                ..record.assignment.clone()
            },
            u64::MAX,
        )
        .await
        .unwrap();
    assert_eq!(moved.assignment.node_id, "target");
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn concurrent_traffic_and_manual_resume_share_durable_intent() {
    let (_rig, session, _) = paused_fixture().await;
    let mut work = Vec::new();
    for _ in 0..16 {
        let s = session.clone();
        work.push(tokio::spawn(async move {
            s.request_resume("sleep", 1).await.unwrap()
        }));
    }
    let first = work.remove(0).await.unwrap();
    for work in work {
        assert_eq!(work.await.unwrap().resume, first.resume);
    }
    assert!(!first.resume.unwrap().local_rejected);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn shared_storage_alone_does_not_permit_transfer_before_local_rejection() {
    let (_rig, session, record) = paused_fixture().await;
    let demanded = session.request_resume("sleep", 1).await.unwrap();
    let replacement = Assignment {
        node_id: "target".into(),
        generation: 2,
        ..record.assignment.clone()
    };
    assert_eq!(
        session
            .reserve_recovery(&record.assignment, replacement.clone(), 1)
            .await,
        Err(Error::Conflict)
    );
    let operation = demanded.resume.unwrap().operation_id;
    let rejected = session
        .reject_local_resume(&record.assignment, &operation, 1)
        .await
        .unwrap();
    assert!(rejected.resume_point(1).is_some());
    let transfer = session
        .reserve_recovery(&record.assignment, replacement, 1)
        .await
        .unwrap();
    assert!(transfer.recovery.unwrap().pending);
    assert!(!transfer.resume.unwrap().local_rejected);
    let mut late = record;
    late.state = EnvironmentState::Running;
    late.resources_held = true;
    late.runtime.ip = Some("10.0.0.4".parse().unwrap());
    late.runtime.id = "sleep-1-r4".into();
    late.revision = 4;
    late.last_operation = Some(adx_core::CompletedOperation {
        id: operation,
        expected_revision: 2,
        kind: adx_core::LifecycleKind::Resume,
    });
    assert_eq!(session.commit(late).await, Err(Error::Conflict));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn deletion_and_expiry_fence_pending_resume() {
    let (_rig, session, mut record) = paused_fixture().await;
    session.request_resume("sleep", 1).await.unwrap();
    record.revision += 1;
    record.checkpoint.as_mut().unwrap().expires_at_unix_seconds = Some(2);
    session.commit(record.clone()).await.unwrap();
    assert!(session.request_resume("sleep", 2).await.is_err());
    let replacement = Assignment {
        node_id: "target".into(),
        generation: 2,
        ..record.assignment.clone()
    };
    session
        .reject_local_resume(
            &record.assignment,
            &session
                .get("sleep")
                .await
                .unwrap()
                .resume
                .unwrap()
                .operation_id,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        session
            .reserve_recovery(&record.assignment, replacement, 2)
            .await,
        Err(Error::Conflict)
    );
    record.state = EnvironmentState::Deleted;
    record.revision += 1;
    record.checkpoint = None;
    session.commit(record).await.unwrap();
    assert_eq!(
        session.request_resume("sleep", 1).await,
        Err(Error::NotFound)
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn node_loss_preserves_dormancy_and_does_not_enqueue_restore() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let session = db.begin(1).await.unwrap();
    session
        .register_session(
            node("source"),
            "source:1".into(),
            "source:2".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let assignment = Assignment {
        environment_id: "sleeper".into(),
        node_id: "source".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    session
        .reserve(spec("sleeper"), assignment.clone())
        .await
        .unwrap();
    let mut paused = running(spec("sleeper"), assignment);
    paused.state = EnvironmentState::Paused;
    paused.resources_held = false;
    paused.runtime.ip = None;
    paused.checkpoint = Some(adx_core::RestorePoint {
        id: "pause".into(),
        origin: None,
        expires_at_unix_seconds: None,
        source_runtime_id: "sleeper-1".into(),
        artifact: adx_core::CheckpointArtifact {
            scope: String::new(),
            storage: "obs".into(),
            location: "artifact".into(),
            size_bytes: 8,
        },
    });
    session.commit(paused.clone()).await.unwrap();
    session.invalidate_node("source", "boot").await.unwrap();
    let stored = session.get("sleeper").await.unwrap();
    assert!(stored.invalidated);
    assert_eq!(
        stored.result.as_ref().unwrap().state,
        EnvironmentState::Paused
    );
    assert_eq!(
        stored.result.as_ref().unwrap().checkpoint,
        paused.checkpoint
    );
    assert!(
        stored.recovery_point(1).is_none(),
        "sleeping is not a request to run"
    );
    assert!(!stored.failed_collectable(u64::MAX, 1));
    let restarted = db.begin(1).await.unwrap();
    assert_eq!(restarted.get("sleeper").await.unwrap(), stored);
    let scheduler =
        Coordinator::restore(&restarted.snapshot().await.unwrap(), Placement::Pack).unwrap();
    assert!(!scheduler.snapshot().environments().contains_key("sleeper"));
    let mut late = paused;
    late.state = EnvironmentState::Running;
    late.resources_held = true;
    late.runtime.ip = Some("10.0.0.2".parse().unwrap());
    late.runtime.id = "sleeper-1-r4".into();
    late.revision = 4;
    late.last_operation = Some(adx_core::CompletedOperation {
        id: "late-resume".into(),
        expected_revision: 2,
        kind: adx_core::LifecycleKind::Resume,
    });
    assert_eq!(restarted.commit(late).await, Err(Error::Conflict));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn capacity_retry_round_keeps_intent_identity_and_reconsiders_nodes() {
    let (_rig, session, record) = paused_fixture().await;
    let intent = session
        .request_resume("sleep", 1)
        .await
        .unwrap()
        .resume
        .unwrap();
    session
        .reject_local_resume(&record.assignment, &intent.operation_id, 1)
        .await
        .unwrap();
    let before = session
        .request_resume("sleep", 30)
        .await
        .unwrap()
        .resume
        .unwrap();
    assert!(before.local_rejected);
    assert!(before.rejected_nodes.contains("source"));
    let retry = session
        .request_resume("sleep", 31)
        .await
        .unwrap()
        .resume
        .unwrap();
    assert_eq!(retry.operation_id, intent.operation_id);
    assert!(!retry.local_rejected);
    assert!(retry.rejected_nodes.is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn failure_recovery_intermediate_pause_does_not_become_user_dormancy() {
    let (_rig, session, mut record) = paused_fixture().await;
    for id in ["source", "target"] {
        session
            .register_session(
                node(id),
                format!("{id}:1"),
                format!("{id}:2"),
                Some(adx_coordinator::storage::NodeSession {
                    id: "boot".into(),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
    }
    record.state = EnvironmentState::Failed;
    record.revision += 1;
    session.commit(record.clone()).await.unwrap();
    session.invalidate_node("source", "boot").await.unwrap();
    let replacement = Assignment {
        node_id: "target".into(),
        generation: 2,
        ..record.assignment.clone()
    };
    let recovering = session
        .reserve_recovery(&record.assignment, replacement, 1)
        .await
        .unwrap();
    assert!(recovering.recovery.unwrap().pending);
    assert!(recovering.resume.is_none());
    session.invalidate_node("target", "boot").await.unwrap();
    let interrupted = session.get("sleep").await.unwrap();
    assert_eq!(
        interrupted.result.as_ref().unwrap().state,
        EnvironmentState::Failed
    );
    assert!(interrupted.recovery_point(1).is_some());
}
