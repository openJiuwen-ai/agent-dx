use super::*;
use adx_protocol::{
    auth::Peers,
    control::{self as pb, node_service_server::NodeService},
};
use adxlet::checkpoint::{CheckpointCooperation, LocalCheckpointStore};
use std::sync::atomic::Ordering;

#[async_trait]
impl CheckpointCooperation for Backend {
    async fn prepare(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        Ok(())
    }
    async fn abort_unstarted(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        Ok(())
    }
    async fn resumed(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        Ok(())
    }
}

fn checkpoint_node(backend: &Arc<Backend>, root: &std::path::Path) -> Arc<Adxlet> {
    backend.allow_start.store(true, Ordering::SeqCst);
    let node = Adxlet::new(
        "n".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(
        Arc::new(LocalCheckpointStore::new(root.to_owned()).unwrap()),
        backend.clone(),
    )
    .unwrap();
    node.update_capacity(
        Resources {
            cpu_millis: 1000,
            memory_bytes: 1000,
            disk_bytes: 0,
        },
        Duration::from_secs(60),
    )
    .unwrap();
    Arc::new(node)
}

fn request<T>(value: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(value);
    request
        .metadata_mut()
        .insert("adx-component", "apiserver".parse().unwrap());
    request
}

async fn pause(node: &Arc<Adxlet>, record: EnvironmentRecord) -> EnvironmentRecord {
    let rpc = adxlet::rpc::NodeRpc::new(node.clone(), Peers::network(), "test-session".into());
    rpc.pause_environment(request(pb::PauseEnvironmentRequest {
        assignment: Some(record.assignment.try_into().unwrap()),
        caller: Some(pb::CallerContext {
            tenant_id: "t".into(),
            administrator: false,
        }),
        operation_id: format!("pause-{}", record.revision),
        expected_revision: record.revision,
        timeout_seconds: 5,
        retention: Some(adx_core::lifecycle::RetentionPolicy::Forever.into()),
    }))
    .await
    .unwrap()
    .into_inner()
    .record
    .unwrap()
    .try_into()
    .unwrap()
}

async fn resume(node: &Arc<Adxlet>, record: EnvironmentRecord) -> EnvironmentRecord {
    let rpc = adxlet::rpc::NodeRpc::new(node.clone(), Peers::network(), "test-session".into());
    rpc.resume_environment(request(pb::ResumeEnvironmentRequest {
        assignment: Some(record.assignment.try_into().unwrap()),
        caller: Some(pb::CallerContext {
            tenant_id: "t".into(),
            administrator: false,
        }),
        operation_id: format!("resume-{}", record.revision),
        expected_revision: record.revision,
    }))
    .await
    .unwrap()
    .into_inner()
    .record
    .unwrap()
    .try_into()
    .unwrap()
}

#[tokio::test]
async fn blocked_restore_allows_repeated_catalogs_new_creation_and_other_pause_resume() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() =
        vec![observed("busy"), observed("other"), observed("retired")];
    let root = tempfile::tempdir().unwrap();
    let node = checkpoint_node(&backend, root.path());
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = ["busy", "other", "retired"]
        .iter()
        .map(|id| record(id, EnvironmentState::Running).try_into().unwrap())
        .collect();
    authority.cycle(&node).await.unwrap();
    let paused = pause(&node, record("busy", EnvironmentState::Running)).await;
    authority.authority.records.lock().unwrap()[0] = paused.clone().try_into().unwrap();
    backend.block_restore.store(true, Ordering::SeqCst);
    let restoring = tokio::spawn({
        let node = node.clone();
        async move { resume(&node, paused).await }
    });
    backend.restore_entered.notified().await;
    authority.authority.records.lock().unwrap().pop();
    for _ in 0..4 {
        tokio::time::timeout(Duration::from_millis(250), authority.cycle(&node))
            .await
            .unwrap()
            .unwrap();
        assert!(node.accepting_allocations());
    }
    assert!(
        !backend
            .actual
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.environment_id == "retired"),
        "C cleanup must progress while A restore is pending"
    );
    let other = tokio::time::timeout(Duration::from_secs(1), async {
        let paused = pause(&node, record("other", EnvironmentState::Running)).await;
        resume(&node, paused).await
    })
    .await
    .expect("B pause/resume must not wait for A restore");
    assert_eq!(other.state, EnvironmentState::Running);
    let new = record("new", EnvironmentState::Pending);
    let new = tokio::time::timeout(
        Duration::from_secs(1),
        node.environment(new.spec, new.assignment).unwrap().create(),
    )
    .await
    .unwrap()
    .unwrap()
    .record;
    assert_eq!(new.state, EnvironmentState::Running);
    backend.restore_release.notify_one();
    let busy = restoring.await.unwrap();
    *authority.authority.records.lock().unwrap() = [busy.clone(), other.clone(), new]
        .into_iter()
        .map(|r| r.try_into().unwrap())
        .collect();
    // The in-flight old comparison observes the completed restore as newer;
    // the following fresh comparison must preserve it.
    let _ = authority.cycle(&node).await;
    authority.cycle(&node).await.unwrap();
    assert!(backend
        .actual
        .lock()
        .unwrap()
        .iter()
        .any(|r| r.runtime_id == busy.runtime.id));
    assert!(backend
        .actual
        .lock()
        .unwrap()
        .iter()
        .any(|r| r.runtime_id == other.runtime.id));
}

#[tokio::test]
async fn live_comparison_preserves_in_progress_checkpoint_directory() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("busy")];
    let root = tempfile::tempdir().unwrap();
    let node = checkpoint_node(&backend, root.path());
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("busy", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority.cycle(&node).await.unwrap();
    backend.block_checkpoint.store(true, Ordering::SeqCst);
    let capture = tokio::spawn({
        let node = node.clone();
        async move { pause(&node, record("busy", EnvironmentState::Running)).await }
    });
    backend.checkpoint_entered.notified().await;
    let path = backend.checkpoint_path.lock().unwrap().clone().unwrap();
    for _ in 0..3 {
        authority.cycle(&node).await.unwrap();
        assert_eq!(
            std::fs::read(path.join("state")).unwrap(),
            b"checkpoint data"
        );
    }
    backend.checkpoint_release.notify_one();
    let paused = capture.await.unwrap();
    assert_eq!(paused.state, EnvironmentState::Paused);
    assert_eq!(
        std::fs::read(path.join("state")).unwrap(),
        b"checkpoint data"
    );
}

#[tokio::test]
async fn recovery_cleanup_failure_keeps_admission_closed_until_retry() {
    let backend = Arc::new(Backend::default());
    let node = authority::manager(backend.clone());
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    backend.actual.lock().unwrap().push(observed("orphan"));
    backend.fail_remove.store(true, Ordering::SeqCst);
    authority.registration.reconciling = true;
    assert!(authority.cycle(&node).await.is_err());
    assert!(!node.accepting_allocations());
    let candidate = record("other", EnvironmentState::Pending);
    assert!(node
        .environment(candidate.spec.clone(), candidate.assignment.clone())
        .is_err());
    backend.fail_remove.store(false, Ordering::SeqCst);
    authority.cycle(&node).await.unwrap();
    assert!(backend.actual.lock().unwrap().is_empty());
    assert!(node.accepting_allocations());
    assert!(node
        .environment(candidate.spec, candidate.assignment)
        .is_ok());
}

#[tokio::test]
async fn recovery_cleans_all_orphan_executions_before_reopening() {
    let backend = Arc::new(Backend::default());
    let node = authority::manager(backend.clone());
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    let mut late = observed("orphan");
    late.runtime_id = "orphan-8".into();
    late.generation = 8;
    backend
        .actual
        .lock()
        .unwrap()
        .extend([observed("orphan"), late]);
    authority.cycle(&node).await.unwrap();
    assert_eq!(backend.actual.lock().unwrap().len(), 2);
    authority.registration.reconciling = true;
    authority.cycle(&node).await.unwrap();
    assert!(backend.actual.lock().unwrap().is_empty());
    let candidate = record("orphan", EnvironmentState::Pending);
    assert!(node
        .environment(candidate.spec, candidate.assignment)
        .is_ok());
}

#[tokio::test]
async fn periodic_journal_recovery_does_not_wait_for_another_environments_commit() {
    let backend = Arc::new(Backend::default());
    backend.allow_start.store(true, Ordering::SeqCst);
    *backend.actual.lock().unwrap() = vec![observed("busy")];
    let root = tempfile::tempdir().unwrap();
    let journal = Arc::new(adxlet::journal::JournalSink::new(
        root.path().join("journal.sqlite"),
        backend.clone(),
    ));
    let node = Arc::new(Adxlet::new(
        "n".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        journal.clone(),
    ));
    node.update_capacity(
        Resources {
            cpu_millis: 100,
            memory_bytes: 100,
            disk_bytes: 0,
        },
        Duration::from_secs(60),
    )
    .unwrap();
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("busy", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority
        .cycle_with_journal(&node, Some(&journal))
        .await
        .unwrap();
    let busy = record("busy", EnvironmentState::Running);
    let handle = node.environment(busy.spec, busy.assignment).unwrap();
    backend.block_commit.store(true, Ordering::SeqCst);
    let committing = tokio::spawn(async move { handle.sync().await });
    backend.commit_entered.notified().await;
    let result = tokio::time::timeout(Duration::from_millis(300), async {
        authority
            .cycle_with_journal(&node, Some(&journal))
            .await
            .unwrap();
        let new = record("new", EnvironmentState::Pending);
        node.environment(new.spec, new.assignment)
            .unwrap()
            .create()
            .await
            .unwrap();
    })
    .await;
    backend.block_commit.store(false, Ordering::SeqCst);
    backend.commit_release.notify_one();
    committing.await.unwrap().unwrap();
    result.expect("live journal replay must not enqueue a global writer behind A");
}

#[tokio::test]
async fn session_invalidation_still_drains_pending_environment_work_before_reopening() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("busy")];
    let node = Arc::new(authority::manager(backend.clone()));
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("busy", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority.cycle(&node).await.unwrap();
    backend.block_observation.store(true, Ordering::SeqCst);
    let monitor = tokio::spawn({
        let node = node.clone();
        async move { node.monitor_environments().await }
    });
    backend.observation_entered.notified().await;
    authority.cycle(&node).await.unwrap();
    authority
        .authority
        .requires_handshake
        .store(true, Ordering::SeqCst);
    let mut fencing = tokio::spawn({
        let node = node.clone();
        async move {
            let result = authority.cycle(&node).await;
            (authority, result)
        }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(40), &mut fencing)
            .await
            .is_err()
    );
    assert!(!node.accepting_allocations());
    let new = record("new", EnvironmentState::Pending);
    assert!(node.environment(new.spec, new.assignment).is_err());
    backend.block_observation.store(false, Ordering::SeqCst);
    backend.observation_release.notify_one();
    monitor.await.unwrap().unwrap();
    let (mut authority, result) = tokio::time::timeout(Duration::from_secs(1), fencing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
    authority.cycle(&node).await.unwrap();
    assert!(node.accepting_allocations());
    assert_eq!(*backend.actual.lock().unwrap(), vec![observed("busy")]);
}
