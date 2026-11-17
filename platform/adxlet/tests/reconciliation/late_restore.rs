//! Late Restore observations must converge even when their identity is current.
use super::*;

async fn failed_restore() -> (
    tempfile::TempDir,
    Arc<Backend>,
    Arc<Adxlet>,
    authority::Fixture,
    EnvironmentRecord,
) {
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
    let paused = pause(&node, record("busy", EnvironmentState::Running)).await;
    let handle = node
        .environment(paused.spec.clone(), paused.assignment.clone())
        .unwrap();
    backend.fail_start.store(true, Ordering::SeqCst);
    assert!(handle
        .resume(adxlet::checkpoint::ResumeRequest {
            run_timeout_seconds: None,
            operation_id: "unknown-restore".into(),
            expected_revision: paused.revision,
        })
        .await
        .is_err());
    backend.fail_start.store(false, Ordering::SeqCst);
    let rollback = handle.sync().await.unwrap();
    assert_eq!(rollback.durability, Durability::Published);
    assert_eq!(rollback.record.state, EnvironmentState::Paused);
    assert_eq!(rollback.record.checkpoint, paused.checkpoint);
    assert_ne!(rollback.record.runtime.id, paused.runtime.id);
    assert!(!rollback.record.resources_held);
    assert_eq!(node.used(), Resources::default());
    assert!(backend.actual.lock().unwrap().is_empty());
    *authority.authority.records.lock().unwrap() =
        vec![rollback.record.clone().try_into().unwrap()];
    (root, backend, node, authority, rollback.record)
}

fn inject_late(backend: &Backend, record: &EnvironmentRecord) {
    let mut late = observed("busy");
    late.runtime_id = record.runtime.id.clone();
    backend.actual.lock().unwrap().push(late);
}

fn contains(backend: &Backend, id: &str) -> bool {
    backend
        .actual
        .lock()
        .unwrap()
        .iter()
        .any(|r| r.runtime_id == id)
}

async fn assert_paused(node: &Adxlet, record: &EnvironmentRecord) {
    let current = node
        .environment(record.spec.clone(), record.assignment.clone())
        .unwrap()
        .sync()
        .await
        .unwrap();
    assert_eq!(current.record.state, EnvironmentState::Paused);
    assert_eq!(current.record.checkpoint, record.checkpoint);
    assert!(!current.record.resources_held);
    assert_eq!(node.used(), Resources::default());
    assert_eq!(
        std::fs::read(
            std::path::Path::new(&record.checkpoint.as_ref().unwrap().artifact.location)
                .join("state")
        )
        .unwrap(),
        b"checkpoint data"
    );
}

#[tokio::test]
async fn late_restore_after_paused_rollback_is_reaped() {
    let (_root, backend, node, mut authority, rollback) = failed_restore().await;
    inject_late(&backend, &rollback);
    for _ in 0..5 {
        authority.cycle(&node).await.unwrap();
        assert_paused(&node, &rollback).await;
    }
    assert!(
        !contains(&backend, &rollback.runtime.id),
        "late Restore survived periodic reconciliation"
    );
}

#[tokio::test]
async fn late_restore_after_restart_empty_inventory_is_reaped() {
    let (root, backend, node, _, rollback) = failed_restore().await;
    drop(node);
    let node = checkpoint_node(&backend, root.path());
    let mut authority = authority::Fixture::new().await;
    *authority.authority.records.lock().unwrap() = vec![rollback.clone().try_into().unwrap()];
    authority.cycle(&node).await.unwrap();
    assert!(backend.actual.lock().unwrap().is_empty());
    inject_late(&backend, &rollback);
    for _ in 0..5 {
        authority.cycle(&node).await.unwrap();
        assert_paused(&node, &rollback).await;
    }
    assert!(
        !contains(&backend, &rollback.runtime.id),
        "late Restore survived node restart and periodic scans"
    );
}

#[tokio::test]
async fn late_restore_delete_failure_fences_new_allocations_but_keeps_the_controller_usable() {
    let (_root, backend, node, mut authority, rollback) = failed_restore().await;
    inject_late(&backend, &rollback);
    backend.fail_remove.store(true, Ordering::SeqCst);
    for _ in 0..3 {
        assert!(authority.cycle(&node).await.is_err());
        assert_paused(&node, &rollback).await;
        assert!(!node.accepting_allocations());
        assert!(contains(&backend, &rollback.runtime.id));
    }
    backend.fail_remove.store(false, Ordering::SeqCst);
    for _ in 0..5 {
        let _ = authority.cycle(&node).await;
        assert_paused(&node, &rollback).await;
    }
    assert!(!contains(&backend, &rollback.runtime.id));
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn late_restore_cleanup_queues_behind_resume_and_preserves_successor() {
    let (_root, backend, node, mut authority, rollback) = failed_restore().await;
    inject_late(&backend, &rollback);
    backend.block_restore.store(true, Ordering::SeqCst);
    let restoring = tokio::spawn({
        let node = node.clone();
        let rollback = rollback.clone();
        async move { resume(&node, rollback).await }
    });
    tokio::time::timeout(Duration::from_secs(2), backend.restore_entered.notified())
        .await
        .unwrap();
    // Both the rejected execution and the new successor are visible while
    // the controller is still awaiting Restore's reply and the catalog is Paused.
    assert_eq!(backend.actual.lock().unwrap().len(), 2);
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_millis(250), authority.cycle(&node))
            .await
            .unwrap()
            .unwrap();
        // The rejected old execution has no resource reservation. Keep the
        // accepted Resume running, but do not allocate over that extra runtime.
        assert!(!node.accepting_allocations());
        assert!(!authority.registration.accepting_allocations);
    }
    assert_eq!(backend.actual.lock().unwrap().len(), 2);
    backend.restore_release.notify_one();
    let running = restoring.await.unwrap();
    *authority.authority.records.lock().unwrap() = vec![running.clone().try_into().unwrap()];
    for _ in 0..5 {
        let _ = authority.cycle(&node).await;
    }
    assert!(!contains(&backend, &rollback.runtime.id));
    assert!(contains(&backend, &running.runtime.id));
    assert_eq!(backend.actual.lock().unwrap().len(), 1);
    assert_eq!(node.used(), running.spec.resources);
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn terminal_catalog_does_not_protect_late_current_execution() {
    for state in [EnvironmentState::Failed, EnvironmentState::Deleted] {
        let backend = Arc::new(Backend::default());
        let root = tempfile::tempdir().unwrap();
        let node = checkpoint_node(&backend, root.path());
        let mut terminal = record("busy", state);
        terminal.resources_held = false;
        terminal.runtime.ip = None;
        let mut authority = authority::Fixture::new().await;
        *authority.authority.records.lock().unwrap() = vec![terminal.clone().try_into().unwrap()];
        authority.cycle(&node).await.unwrap();
        // Boot reconciliation can publish a newer Failed revision. Subsequent
        // live catalogs must reflect that publication, as the Coordinator does.
        terminal = node
            .environment(terminal.spec, terminal.assignment)
            .unwrap()
            .sync()
            .await
            .unwrap()
            .record;
        *authority.authority.records.lock().unwrap() = vec![terminal.clone().try_into().unwrap()];
        inject_late(&backend, &terminal);
        for _ in 0..5 {
            authority.cycle(&node).await.unwrap();
        }
        assert!(!contains(&backend, &terminal.runtime.id));
        let current = node
            .environment(terminal.spec, terminal.assignment)
            .unwrap()
            .sync()
            .await
            .unwrap();
        assert_eq!(current.record.state, state);
        assert_eq!(node.used(), Resources::default());
        assert!(node.accepting_allocations());
    }
}
