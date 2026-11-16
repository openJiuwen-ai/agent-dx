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
