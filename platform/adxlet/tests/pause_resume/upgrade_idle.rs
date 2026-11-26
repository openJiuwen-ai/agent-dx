use super::*;
use adxlet::upgrade::{ExecutionGrant, NodeControl};

async fn planned_fixture() -> (
    tempfile::TempDir,
    Arc<Backend>,
    Adxlet,
    adxlet::EnvironmentHandle,
    PauseRequest,
) {
    let (temp, backend, node, spec, assignment) = fixture();
    let node = node
        .with_upgrade_control(temp.path().join("upgrade.json"), "session".into())
        .unwrap();
    let handle = node.environment(spec, assignment).unwrap();
    let running = handle.create().await.unwrap();
    let inspection = node
        .apply_upgrade_control(NodeControl {
            task_id: "task".into(),
            epoch: 1,
            revision: 1,
            node_session_id: "session".into(),
            phase: "evacuating".into(),
            grants: vec![ExecutionGrant {
                environment_id: "environment".into(),
                assignment_generation: 1,
                actions: vec!["pause".into(), "route".into()],
            }],
        })
        .await
        .unwrap();
    assert_eq!(inspection.protocol_version, 3);
    let mut request = pause(running.record.revision);
    request.operation_id = "task:task:migration".into();
    *backend.activity.lock().unwrap() = Some((1, 0));
    (temp, backend, node, handle, request)
}

#[tokio::test(start_paused = true)]
async fn upgrade_pause_waits_for_both_idle_observations_without_closing_admission() {
    let (_temp, backend, _node, handle, request) = planned_fixture().await;
    backend.proxy_activity.lock().unwrap().1 = 1;
    let pausing = tokio::spawn(async move { handle.pause(request).await });
    tokio::task::yield_now().await;
    {
        let mut proxy = backend.proxy_activity.lock().unwrap();
        assert!(!proxy.2, "long-lived stream must not close new admission");
        proxy.0 += 1;
        proxy.1 += 1; // A concurrent business request is still admitted.
    }
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(!pausing.is_finished());
    assert!(!backend.proxy_activity.lock().unwrap().2);
    assert!(backend.prepare_ids.lock().unwrap().is_empty());
    *backend.activity.lock().unwrap() = Some((2, 1));
    backend.proxy_activity.lock().unwrap().1 = 0;
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(
        !backend.proxy_activity.lock().unwrap().2,
        "direct runtime request is still busy"
    );
    *backend.activity.lock().unwrap() = Some((3, 0));
    assert_eq!(
        pausing.await.unwrap().unwrap().record.state,
        EnvironmentState::Paused
    );
    assert!(backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event == "checkpoint"));
}

#[tokio::test(start_paused = true)]
async fn upgrade_pause_busy_deadline_preserves_source_and_admission() {
    for proxy_busy in [false, true] {
        let (_temp, backend, _node, handle, mut request) = planned_fixture().await;
        request.timeout_seconds = 1;
        if proxy_busy {
            backend.proxy_activity.lock().unwrap().1 = 1;
        } else {
            *backend.activity.lock().unwrap() = Some((2, 1));
        }
        assert!(handle.pause(request).await.is_err());
        assert_eq!(
            handle.sync().await.unwrap().record.state,
            EnvironmentState::Running
        );
        assert!(!backend.proxy_activity.lock().unwrap().2);
        assert_eq!(backend.running.lock().unwrap().len(), 1);
        assert!(backend.prepare_ids.lock().unwrap().is_empty());
        assert!(!backend
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "retire" || event == "checkpoint"));
    }
}

#[tokio::test(start_paused = true)]
async fn upgrade_pause_captures_updated_policy_and_replays_the_original_request() {
    use adx_core::execution::{NetworkAction, NetworkPolicy, TrafficMode, TrafficPolicy};

    let (_temp, backend, _node, handle, request) = planned_fixture().await;
    let policy = NetworkPolicy {
        traffic: Some(TrafficPolicy {
            ingress_default_action: NetworkAction::Deny,
            egress_default_action: NetworkAction::Allow,
            rules: vec![],
            mode: TrafficMode::Stateful,
        }),
        dns: None,
    };
    let network = handle
        .update_network_policy(
            Some(policy.clone()),
            "network-a".into(),
            request.expected_revision,
        )
        .await
        .unwrap();
    assert!(network.record.revision > request.expected_revision);

    let public_request = pause(request.expected_revision);
    assert_eq!(
        handle.pause(public_request).await.unwrap_err(),
        Error::Conflict
    );
    let mut future_request = request.clone();
    future_request.expected_revision = network.record.revision + 1;
    assert_eq!(
        handle.pause(future_request).await.unwrap_err(),
        Error::Conflict
    );

    let paused = handle.pause(request.clone()).await.unwrap().record;
    assert_eq!(paused.state, EnvironmentState::Paused);
    assert_eq!(paused.spec.execution.network, Some(policy));
    let replay = handle.pause(request).await.unwrap().record;
    assert_eq!(replay.revision, paused.revision);
    assert_eq!(replay.checkpoint, paused.checkpoint);
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| *event == "checkpoint")
            .count(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn upgrade_pause_can_follow_a_snapshot_of_the_same_environment() {
    let (_temp, _backend, _node, handle, request) = planned_fixture().await;
    let saved = handle
        .snapshot(adxlet::checkpoint::SnapshotRequest {
            kind: adx_core::snapshots::SnapshotKind::Regular,
            operation_id: "snapshot-before-migration".into(),
            expected_revision: request.expected_revision,
            names: vec![],
            timeout_seconds: 60,
        })
        .await
        .unwrap();
    assert_eq!(saved.environment.record.state, EnvironmentState::Running);
    assert!(saved.environment.record.revision > request.expected_revision);
    assert_eq!(
        saved.snapshot.state,
        adx_core::snapshots::SnapshotState::Ready
    );
    let paused = handle.pause(request.clone()).await.unwrap().record;
    assert_eq!(paused.state, EnvironmentState::Paused);
    assert_eq!(
        handle.pause(request).await.unwrap().record.revision,
        paused.revision
    );
}

#[tokio::test(start_paused = true)]
async fn busy_upgrade_pause_yields_to_network_management() {
    let (_temp, backend, _node, handle, mut request) = planned_fixture().await;
    request.timeout_seconds = 300;
    backend.proxy_activity.lock().unwrap().1 = 1;
    let paused = {
        let handle = handle.clone();
        let request = request.clone();
        tokio::spawn(async move { handle.pause(request).await })
    };
    tokio::task::yield_now().await;
    let update = tokio::time::timeout(
        Duration::from_secs(2),
        handle.update_network_policy(
            None,
            "network-during-idle-wait".into(),
            request.expected_revision,
        ),
    )
    .await
    .expect("waiting for migration idle must not hold the controller until the checkpoint deadline")
    .unwrap();
    assert!(update.record.revision > request.expected_revision);
    assert!(matches!(paused.await.unwrap(), Err(Error::Unavailable(_))));
    assert!(!backend.proxy_activity.lock().unwrap().2);

    backend.proxy_activity.lock().unwrap().1 = 0;
    assert_eq!(
        handle.pause(request).await.unwrap().record.state,
        EnvironmentState::Paused
    );
}

#[tokio::test(start_paused = true)]
async fn upgrade_pause_idle_race_reopens_without_aborting_another_checkpoint() {
    let (_temp, backend, _node, handle, request) = planned_fixture().await;
    *backend.idle_race.lock().unwrap() = true;
    assert_eq!(handle.pause(request).await.unwrap_err(), Error::Conflict);
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert!(!backend.proxy_activity.lock().unwrap().2);
    assert!(backend.prepare_ids.lock().unwrap().is_empty());
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event == "abort" || event == "checkpoint"));
}
