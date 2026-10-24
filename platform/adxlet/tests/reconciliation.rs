#[path = "common/authority.rs"]
mod authority;
#[path = "reconciliation/isolation.rs"]
mod isolation;
use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources, Result,
};
use adxlet::{Adxlet, Durability, Readiness, Routes, RuntimeDriver, RuntimeObservation, StateSink};
use async_trait::async_trait;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Backend {
    actual: Mutex<Vec<RuntimeObservation>>,
    readiness_calls: std::sync::atomic::AtomicUsize,
    binding_syncs: std::sync::atomic::AtomicUsize,
    observations: std::sync::atomic::AtomicUsize,
    block_observation: std::sync::atomic::AtomicBool,
    observation_entered: tokio::sync::Notify,
    observation_release: tokio::sync::Notify,
    fail_inventory: std::sync::atomic::AtomicBool,
    allow_start: std::sync::atomic::AtomicBool,
    fail_start: std::sync::atomic::AtomicBool,
    fail_remove: std::sync::atomic::AtomicBool,
    block_remove: std::sync::atomic::AtomicBool,
    remove_entered: tokio::sync::Notify,
    remove_release: tokio::sync::Notify,
    events: Mutex<Vec<String>>,
    block_restore: std::sync::atomic::AtomicBool,
    restore_entered: tokio::sync::Notify,
    restore_release: tokio::sync::Notify,
    block_checkpoint: std::sync::atomic::AtomicBool,
    checkpoint_entered: tokio::sync::Notify,
    checkpoint_release: tokio::sync::Notify,
    checkpoint_path: Mutex<Option<std::path::PathBuf>>,
    block_commit: std::sync::atomic::AtomicBool,
    commit_entered: tokio::sync::Notify,
    commit_release: tokio::sync::Notify,
    journal_only: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl RuntimeDriver for Backend {
    async fn set_network_policy(
        &self,
        _: &str,
        _: Option<&adx_core::execution::NetworkPolicy>,
        _: &[u16],
        _: Option<u16>,
    ) -> Result<()> {
        Ok(())
    }
    async fn checkpoint_supported(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn checkpoint(&self, id: &str, path: &std::path::Path, _: Duration) -> Result<()> {
        std::fs::write(path.join("state"), b"checkpoint data").unwrap();
        *self.checkpoint_path.lock().unwrap() = Some(path.to_owned());
        if id == "busy-7"
            && self
                .block_checkpoint
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.checkpoint_entered.notify_one();
            self.checkpoint_release.notified().await;
        }
        self.actual.lock().unwrap().retain(|r| r.runtime_id != id);
        Ok(())
    }
    async fn restore(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
        path: &std::path::Path,
    ) -> Result<std::net::IpAddr> {
        assert_eq!(
            std::fs::read(path.join("state")).unwrap(),
            b"checkpoint data"
        );
        if spec.id == "busy" && self.block_restore.load(std::sync::atomic::Ordering::SeqCst) {
            self.restore_entered.notify_one();
            self.restore_release.notified().await;
        }
        self.start(spec, id, generation, devices).await
    }
    async fn inventory(&self) -> Result<Vec<RuntimeObservation>> {
        if self
            .fail_inventory
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(adx_core::Error::Unavailable("inventory failed".into()));
        }
        Ok(self.actual.lock().unwrap().clone())
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        self.observations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if id == "busy-7"
            && self
                .block_observation
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.observation_entered.notify_one();
            self.observation_release.notified().await;
        }
        Ok(self
            .actual
            .lock()
            .unwrap()
            .iter()
            .any(|v| v.runtime_id == id && v.running))
    }
    async fn start(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        assert!(
            self.allow_start.load(std::sync::atomic::Ordering::SeqCst),
            "reconciliation must not recreate an image without restart policy"
        );
        if self.fail_start.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(adx_core::Error::Unavailable("start failed".into()));
        }
        self.events.lock().unwrap().push(format!("start:{id}"));
        self.actual.lock().unwrap().push(RuntimeObservation {
            environment_id: spec.id.clone(),
            runtime_id: id.into(),
            generation,
            tenant_id: spec.tenant_id.clone(),
            running: true,
        });
        Ok("10.0.0.3".parse().unwrap())
    }
    async fn remove(&self, id: &str) -> Result<()> {
        self.events.lock().unwrap().push(format!("remove:{id}"));
        if self.block_remove.load(std::sync::atomic::Ordering::SeqCst) {
            self.remove_entered.notify_waiters();
            self.remove_release.notified().await;
        }
        if self.fail_remove.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(adx_core::Error::Unavailable("delete failed".into()));
        }
        self.actual.lock().unwrap().retain(|v| v.runtime_id != id);
        Ok(())
    }
}
#[async_trait]
impl Readiness for Backend {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        self.readiness_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
#[async_trait]
impl Routes for Backend {
    async fn begin_reconcile(&self) -> Result<()> {
        self.binding_syncs
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    async fn activate(&self, r: &EnvironmentRecord) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("bind:{}", r.runtime.id));
        Ok(())
    }
    async fn retire(&self, record: &EnvironmentRecord) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("retire:{}", record.runtime.id));
        Ok(())
    }
    async fn retire_orphan(&self, r: &RuntimeObservation) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("retire:{}", r.runtime_id));
        Ok(())
    }
}
#[async_trait]
impl StateSink for Backend {
    async fn commit(&self, r: &EnvironmentRecord) -> Result<Durability> {
        if r.spec.id == "busy" && self.block_commit.load(std::sync::atomic::Ordering::SeqCst) {
            self.commit_entered.notify_one();
            self.commit_release.notified().await;
        }
        self.events
            .lock()
            .unwrap()
            .push(format!("commit:{:?}", r.state));
        Ok(
            if self.journal_only.load(std::sync::atomic::Ordering::SeqCst) {
                Durability::Journaled
            } else {
                Durability::Published
            },
        )
    }
}
fn record(id: &str, state: EnvironmentState) -> EnvironmentRecord {
    EnvironmentRecord {
        execution_artifacts: vec![],
        snapshot_source: None,
        failure: None,
        restart_attempts: 0,
        restart_pending: false,
        spec: EnvironmentSpec {
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            id: id.into(),
            tenant_id: "t".into(),
            image: "image".into(),
            runtime_class: "runc".into(),
            resources: Resources {
                cpu_millis: 2,
                memory_bytes: 2,
                disk_bytes: 0,
            },
            priority: 0,
            execution: Default::default(),
        },
        assignment: Assignment {
            environment_id: id.into(),
            node_id: "n".into(),
            shard_id: 0,
            generation: 7,
            devices: vec![],
        },
        state,
        revision: if state == EnvironmentState::Pending {
            0
        } else {
            2
        },
        runtime: adx_core::Runtime {
            id: format!("{id}-7"),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        checkpoint: None,
        last_operation: None,
        resources_held: true,
    }
}
fn observed(id: &str) -> RuntimeObservation {
    RuntimeObservation {
        environment_id: id.into(),
        runtime_id: format!("{id}-7"),
        generation: 7,
        tenant_id: "t".into(),
        running: true,
    }
}
fn manager(b: &Arc<Backend>) -> Adxlet {
    let n = Adxlet::new("n".into(), b.clone(), b.clone(), b.clone(), b.clone());
    n.update_capacity(
        Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        },
        Duration::from_secs(30),
    )
    .unwrap();
    n
}

fn reset_recovery_effects(backend: &Backend) {
    backend.events.lock().unwrap().clear();
    backend
        .readiness_calls
        .store(0, std::sync::atomic::Ordering::SeqCst);
    backend
        .binding_syncs
        .store(0, std::sync::atomic::Ordering::SeqCst);
    backend
        .observations
        .store(0, std::sync::atomic::Ordering::SeqCst);
}

fn assert_no_recovery_effects(backend: &Backend) {
    use std::sync::atomic::Ordering;
    assert_eq!(backend.readiness_calls.load(Ordering::SeqCst), 0);
    assert_eq!(backend.binding_syncs.load(Ordering::SeqCst), 0);
    assert_eq!(backend.observations.load(Ordering::SeqCst), 0);
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| { e.starts_with("bind:") || e.starts_with("commit:") }));
}

#[tokio::test]
async fn periodic_matching_execution_has_no_recovery_side_effects() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("kept", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority.cycle(&node).await.unwrap();
    assert!(
        backend
            .readiness_calls
            .load(std::sync::atomic::Ordering::SeqCst)
            > 0
    );
    reset_recovery_effects(&backend);
    for _ in 0..3 {
        authority.cycle(&node).await.unwrap();
    }
    assert_no_recovery_effects(&backend);
    assert!(backend.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn periodic_orphans_require_recovery_instead_of_runtime_scanning() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("kept", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority.cycle(&node).await.unwrap();
    reset_recovery_effects(&backend);
    backend
        .actual
        .lock()
        .unwrap()
        .extend([observed("orphan"), observed("late")]);
    for _ in 0..3 {
        authority.cycle(&node).await.unwrap();
    }
    assert_eq!(backend.actual.lock().unwrap().len(), 3);
    assert_no_recovery_effects(&backend);
    authority.registration.reconciling = true;
    authority.cycle(&node).await.unwrap();
    assert_eq!(*backend.actual.lock().unwrap(), vec![observed("kept")]);
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn periodic_comparison_does_not_infer_lifecycle_from_backend_state() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("lost")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("lost", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority.cycle(&node).await.unwrap();
    backend.actual.lock().unwrap().clear();
    reset_recovery_effects(&backend);
    authority.cycle(&node).await.unwrap();
    assert_eq!(
        node.used(),
        record("lost", EnvironmentState::Running).spec.resources
    );
    assert_no_recovery_effects(&backend);
    assert!(backend.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn failed_initial_handshake_retries_recovery_before_reopening() {
    use std::sync::atomic::Ordering;
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("kept", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority
        .authority
        .reject_ready
        .store(true, Ordering::SeqCst);
    assert!(authority.cycle(&node).await.is_err());
    assert!(!node.accepting_allocations());
    reset_recovery_effects(&backend);
    authority
        .authority
        .reject_ready
        .store(false, Ordering::SeqCst);
    authority.cycle(&node).await.unwrap();
    assert!(backend.binding_syncs.load(Ordering::SeqCst) > 0);
    reset_recovery_effects(&backend);
    authority.cycle(&node).await.unwrap();
    assert_no_recovery_effects(&backend);
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn periodic_stale_catalog_cannot_reactivate_a_published_deletion() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    let running = record("kept", EnvironmentState::Running);
    authority
        .authority
        .records
        .lock()
        .unwrap()
        .push(running.clone().try_into().unwrap());
    authority.cycle(&node).await.unwrap();
    let deleted = node
        .environment(running.spec, running.assignment)
        .unwrap()
        .delete()
        .await
        .unwrap()
        .record;
    reset_recovery_effects(&backend);
    assert!(authority.cycle(&node).await.is_err());
    assert_no_recovery_effects(&backend);
    assert_eq!(node.used(), Resources::default());
    *authority.authority.records.lock().unwrap() = vec![deleted.try_into().unwrap()];
    authority.cycle(&node).await.unwrap();
    reset_recovery_effects(&backend);
    backend.actual.lock().unwrap().push(observed("kept"));
    authority.cycle(&node).await.unwrap();
    authority.cycle(&node).await.unwrap();
    assert_eq!(backend.actual.lock().unwrap().len(), 1);
    authority.registration.reconciling = true;
    authority.cycle(&node).await.unwrap();
    assert!(backend.actual.lock().unwrap().is_empty());
    reset_recovery_effects(&backend);
    assert_no_recovery_effects(&backend);
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn periodic_stale_terminal_catalog_cannot_delete_newer_published_execution() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("kept", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority.cycle(&node).await.unwrap();
    let mut stale = record("kept", EnvironmentState::Failed);
    stale.resources_held = false;
    stale.revision = 1;
    *authority.authority.records.lock().unwrap() = vec![stale.try_into().unwrap()];
    reset_recovery_effects(&backend);
    assert!(authority.cycle(&node).await.is_err());
    assert_eq!(*backend.actual.lock().unwrap(), vec![observed("kept")]);
    assert_no_recovery_effects(&backend);
    assert!(backend.events.lock().unwrap().is_empty());
}

#[tokio::test]
async fn periodic_stale_network_policy_does_not_fence_delete() {
    use adx_core::execution::{NetworkAction, NetworkPolicy, TrafficMode, TrafficPolicy};
    for clear_policy in [false, true] {
        let backend = Arc::new(Backend::default());
        *backend.actual.lock().unwrap() = vec![observed("kept")];
        let node = manager(&backend);
        let mut authority = authority::Fixture::new().await;
        let initial = record("kept", EnvironmentState::Running);
        authority
            .authority
            .records
            .lock()
            .unwrap()
            .push(initial.clone().try_into().unwrap());
        authority.cycle(&node).await.unwrap();
        let handle = node.environment(initial.spec, initial.assignment).unwrap();
        let policy = NetworkPolicy {
            traffic: Some(TrafficPolicy {
                ingress_default_action: NetworkAction::Deny,
                egress_default_action: NetworkAction::Deny,
                rules: vec![],
                mode: TrafficMode::Stateful,
            }),
            dns: None,
        };
        let mut current = handle.sync().await.unwrap().record;
        if clear_policy {
            current = handle
                .update_network_policy(Some(policy.clone()), "block".into(), current.revision)
                .await
                .unwrap()
                .record;
        }
        // InspectNode has read this catalog before the serialized update commits.
        *authority.authority.records.lock().unwrap() = vec![current.clone().try_into().unwrap()];
        let updated = handle
            .update_network_policy(
                if clear_policy { None } else { Some(policy) },
                "update".into(),
                current.revision,
            )
            .await
            .unwrap();
        assert_eq!(updated.durability, Durability::Published);
        assert!(
            authority.cycle(&node).await.is_err(),
            "the old catalog requires a refresh"
        );
        assert_eq!(handle.sync().await.unwrap().record, updated.record);
        assert_eq!(*backend.actual.lock().unwrap(), vec![observed("kept")]);
        // A stale catalog must not block cleanup while waiting for the next cycle.
        assert_eq!(
            handle.delete().await.unwrap().record.state,
            EnvironmentState::Deleted
        );
        assert!(backend.actual.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn periodic_current_spec_conflict_remains_fenced_until_authority_matches() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    let initial = record("kept", EnvironmentState::Running);
    authority
        .authority
        .records
        .lock()
        .unwrap()
        .push(initial.clone().try_into().unwrap());
    authority.cycle(&node).await.unwrap();
    let handle = node.environment(initial.spec, initial.assignment).unwrap();
    let current = handle.sync().await.unwrap().record;
    let mut conflict = current.clone();
    conflict.spec.image = "conflicting-image".into();
    *authority.authority.records.lock().unwrap() = vec![conflict.try_into().unwrap()];
    assert!(authority.cycle(&node).await.is_err());
    assert!(matches!(
        handle.delete().await,
        Err(adx_core::Error::Unavailable(_))
    ));
    assert_eq!(*backend.actual.lock().unwrap(), vec![observed("kept")]);
    *authority.authority.records.lock().unwrap() = vec![current.try_into().unwrap()];
    authority.cycle(&node).await.unwrap();
    assert_eq!(
        handle.delete().await.unwrap().record.state,
        EnvironmentState::Deleted
    );
    assert!(backend.actual.lock().unwrap().is_empty());
}

#[tokio::test]
async fn periodic_network_update_must_publish_before_dismissing_stale_authority() {
    use std::sync::atomic::Ordering;
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    let initial = record("kept", EnvironmentState::Running);
    authority
        .authority
        .records
        .lock()
        .unwrap()
        .push(initial.clone().try_into().unwrap());
    authority.cycle(&node).await.unwrap();
    let handle = node.environment(initial.spec, initial.assignment).unwrap();
    let current = handle.sync().await.unwrap().record;
    *authority.authority.records.lock().unwrap() = vec![current.clone().try_into().unwrap()];
    backend.journal_only.store(true, Ordering::SeqCst);
    let updated = handle
        .update_network_policy(Some(Default::default()), "update".into(), current.revision)
        .await
        .unwrap();
    assert_eq!(updated.durability, Durability::Journaled);
    assert!(authority.cycle(&node).await.is_err());
    assert!(matches!(
        handle.delete().await,
        Err(adx_core::Error::Unavailable(_))
    ));
    assert_eq!(*backend.actual.lock().unwrap(), vec![observed("kept")]);
    backend.journal_only.store(false, Ordering::SeqCst);
    assert!(
        authority.cycle(&node).await.is_err(),
        "the old catalog still needs a refresh"
    );
    let published = handle.sync().await.unwrap();
    assert_eq!(published.durability, Durability::Published);
    assert_eq!(published.record, updated.record);
    assert_eq!(
        handle.delete().await.unwrap().record.state,
        EnvironmentState::Deleted
    );
    assert!(backend.actual.lock().unwrap().is_empty());
}

#[tokio::test]
async fn coordinator_restart_recovers_once_then_resumes_data_only_comparison() {
    use std::sync::atomic::Ordering;
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("kept", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority.cycle(&node).await.unwrap();
    reset_recovery_effects(&backend);
    authority
        .authority
        .requires_handshake
        .store(true, Ordering::SeqCst);
    assert!(authority.cycle(&node).await.is_err());
    authority.cycle(&node).await.unwrap();
    assert!(backend.binding_syncs.load(Ordering::SeqCst) > 0);
    reset_recovery_effects(&backend);
    authority.cycle(&node).await.unwrap();
    assert_no_recovery_effects(&backend);
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn periodic_catalog_fetch_preserves_known_lifecycle_operations() {
    use std::sync::atomic::Ordering;
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = Arc::new(manager(&backend));
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("kept", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority.cycle(&node).await.unwrap();
    reset_recovery_effects(&backend);
    let server = authority.authority.clone();
    server.block_inspect.store(true, Ordering::SeqCst);
    let task = tokio::spawn({
        let node = node.clone();
        async move { authority.cycle(&node).await }
    });
    tokio::time::timeout(Duration::from_secs(1), server.inspect_entered.notified())
        .await
        .unwrap();
    assert!(!node.accepting_allocations());
    tokio::time::timeout(Duration::from_secs(1), node.monitor_environments())
        .await
        .unwrap()
        .unwrap();
    let observed = backend.observations.load(Ordering::SeqCst);
    server.inspect_release.notify_one();
    task.await.unwrap().unwrap();
    assert!(
        observed > 0,
        "known lifecycle was disabled during catalog fetch"
    );
    assert_eq!(backend.readiness_calls.load(Ordering::SeqCst), 0);
    assert_eq!(backend.binding_syncs.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn periodic_busy_environment_does_not_close_other_environment_admission() {
    use std::sync::atomic::Ordering;
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("busy"), observed("other")];
    backend.allow_start.store(true, Ordering::SeqCst);
    let node = Arc::new(authority::manager(backend.clone()));
    let mut authority = authority::Fixture::new().await;
    for id in ["busy", "other"] {
        authority
            .authority
            .records
            .lock()
            .unwrap()
            .push(record(id, EnvironmentState::Running).try_into().unwrap());
    }
    authority.cycle(&node).await.unwrap();
    backend.block_observation.store(true, Ordering::SeqCst);
    let monitor = tokio::spawn({
        let node = node.clone();
        async move { node.monitor_environments().await }
    });
    backend.observation_entered.notified().await;
    let reports = authority.authority.reports.clone();
    reports.lock().unwrap().clear();
    let mut refresh = tokio::spawn({
        let node = node.clone();
        async move { authority.cycle(&node).await }
    });
    let admitted = tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            if reports
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.accepting_allocations)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let new = record("new", EnvironmentState::Pending);
        node.environment(new.spec, new.assignment)
            .unwrap()
            .create()
            .await
            .unwrap();
        node.observe_runtime_event(&adx_core::runtime::RuntimeIdentity {
            environment_id: "other".into(),
            runtime_id: "other-7".into(),
            ownership_generation: 7,
        })
        .await
        .unwrap();
    })
    .await;
    backend.block_observation.store(false, Ordering::SeqCst);
    backend.observation_release.notify_one();
    monitor.await.unwrap().unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), &mut refresh).await;
    admitted.expect("busy A must not close node admission or block B");
    result.unwrap().unwrap().unwrap();
    assert!(node.accepting_allocations());
    assert!(backend
        .actual
        .lock()
        .unwrap()
        .iter()
        .any(|r| r.runtime_id == "new-7"));
}

#[tokio::test]
async fn repeated_overlapping_monitor_rounds_do_not_starve_admission() {
    use std::sync::atomic::Ordering;
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("busy"), observed("other")];
    let node = Arc::new(manager(&backend));
    let mut authority = authority::Fixture::new().await;
    for id in ["busy", "other"] {
        authority
            .authority
            .records
            .lock()
            .unwrap()
            .push(record(id, EnvironmentState::Running).try_into().unwrap());
    }
    authority.cycle(&node).await.unwrap();
    for _ in 0..16 {
        backend.block_observation.store(true, Ordering::SeqCst);
        let monitor = tokio::spawn({
            let node = node.clone();
            async move { node.monitor_environments().await }
        });
        backend.observation_entered.notified().await;
        let reports = authority.authority.reports.clone();
        reports.lock().unwrap().clear();
        let refresh = tokio::spawn({
            let node = node.clone();
            async move {
                let result = authority.cycle(&node).await;
                (authority, result)
            }
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !reports
                .lock()
                .unwrap()
                .iter()
                .any(|r| r.accepting_allocations)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("each busy cycle must keep reporting allocation readiness");
        assert!(node.accepting_allocations());
        tokio::time::timeout(
            Duration::from_secs(1),
            node.observe_runtime_event(&adx_core::runtime::RuntimeIdentity {
                environment_id: "other".into(),
                runtime_id: "other-7".into(),
                ownership_generation: 7,
            }),
        )
        .await
        .unwrap()
        .unwrap();
        backend.block_observation.store(false, Ordering::SeqCst);
        backend.observation_release.notify_one();
        monitor.await.unwrap().unwrap();
        let (next, result) = refresh.await.unwrap();
        result.unwrap();
        authority = next;
        assert!(node.accepting_allocations());
    }
}

#[tokio::test]
async fn periodic_revoked_ownership_still_drains_accepted_work() {
    for terminal in [false, true] {
        use std::sync::atomic::Ordering;
        let backend = Arc::new(Backend::default());
        *backend.actual.lock().unwrap() = vec![observed("busy")];
        let node = Arc::new(authority::manager(backend.clone()));
        backend.allow_start.store(true, Ordering::SeqCst);
        let mut authority = authority::Fixture::new().await;
        authority.authority.records.lock().unwrap().push(
            record("busy", EnvironmentState::Running)
                .try_into()
                .unwrap(),
        );
        authority.cycle(&node).await.unwrap();
        backend.block_observation.store(true, Ordering::SeqCst);
        let busy = tokio::spawn({
            let node = node.clone();
            async move { node.monitor_environments().await }
        });
        tokio::time::timeout(
            Duration::from_secs(1),
            backend.observation_entered.notified(),
        )
        .await
        .unwrap();
        authority.authority.records.lock().unwrap().clear();
        if terminal {
            let mut expired = record("busy", EnvironmentState::Failed);
            expired.revision += 1;
            expired.resources_held = false;
            expired.runtime.ip = None;
            authority
                .authority
                .records
                .lock()
                .unwrap()
                .push(expired.try_into().unwrap());
        }
        // Reporting and unrelated admission continue, but this environment's
        // accepted operation must finish before its queued ownership cleanup.
        tokio::time::timeout(Duration::from_millis(200), authority.cycle(&node))
            .await
            .unwrap()
            .unwrap();
        assert!(backend
            .actual
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.runtime_id == "busy-7"));
        assert!(node.accepting_allocations());
        let old = record("busy", EnvironmentState::Running);
        let handle = node.environment(old.spec, old.assignment).unwrap();
        let delayed = tokio::spawn(async move { handle.ensure_running("late".into()).await });
        let new = record("new", EnvironmentState::Pending);
        let new = tokio::time::timeout(
            Duration::from_secs(1),
            node.environment(new.spec, new.assignment).unwrap().create(),
        )
        .await
        .unwrap()
        .unwrap()
        .record;
        authority
            .authority
            .records
            .lock()
            .unwrap()
            .push(new.try_into().unwrap());
        backend.block_observation.store(false, Ordering::SeqCst);
        backend.observation_release.notify_one();
        busy.await.unwrap().unwrap();
        assert!(
            delayed.await.unwrap().is_err(),
            "work queued after revocation must be fenced"
        );
        authority.cycle(&node).await.unwrap();
        assert!(!backend
            .actual
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.runtime_id == "busy-7"));
        assert!(node.accepting_allocations());
    }
}

#[tokio::test]
async fn periodic_stale_pending_catalog_preserves_lifecycle_without_restarting_handshake() {
    use std::sync::atomic::Ordering;
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    let running = record("kept", EnvironmentState::Running);
    authority
        .authority
        .records
        .lock()
        .unwrap()
        .push(running.clone().try_into().unwrap());
    authority.cycle(&node).await.unwrap();
    let mut stale = record("kept", EnvironmentState::Pending);
    stale.runtime.ip = None;
    *authority.authority.records.lock().unwrap() = vec![stale.try_into().unwrap()];
    reset_recovery_effects(&backend);
    assert_eq!(
        authority.cycle(&node).await.unwrap_err().code(),
        tonic::Code::Aborted
    );
    assert!(!authority.registration.reconciling);
    assert!(node.accepting_allocations());
    assert_no_recovery_effects(&backend);
    node.monitor_environments().await.unwrap();
    assert!(backend.observations.load(Ordering::SeqCst) > 0);
    *authority.authority.records.lock().unwrap() = vec![running.try_into().unwrap()];
    reset_recovery_effects(&backend);
    authority.cycle(&node).await.unwrap();
    assert_no_recovery_effects(&backend);
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn periodic_current_authority_retires_expired_execution_and_releases_resources() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("kept", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority.cycle(&node).await.unwrap();
    let mut expired = record("kept", EnvironmentState::Failed);
    expired.revision = 3;
    expired.resources_held = false;
    expired.runtime.ip = None;
    *authority.authority.records.lock().unwrap() = vec![expired.try_into().unwrap()];
    reset_recovery_effects(&backend);
    authority.cycle(&node).await.unwrap();
    assert!(backend.actual.lock().unwrap().is_empty());
    assert_eq!(node.used(), Resources::default());
    assert_no_recovery_effects(&backend);
    reset_recovery_effects(&backend);
    authority.cycle(&node).await.unwrap();
    assert!(backend.events.lock().unwrap().is_empty());
    let owner = record("kept", EnvironmentState::Running);
    assert_eq!(
        node.environment(owner.spec, owner.assignment)
            .unwrap()
            .delete()
            .await
            .unwrap()
            .record
            .state,
        EnvironmentState::Deleted
    );
}

#[tokio::test]
async fn periodic_stale_catalog_after_controller_collection_preserves_published_delete() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    let running = record("kept", EnvironmentState::Running);
    authority
        .authority
        .records
        .lock()
        .unwrap()
        .push(running.clone().try_into().unwrap());
    authority.cycle(&node).await.unwrap();
    node.environment(running.spec, running.assignment)
        .unwrap()
        .delete()
        .await
        .unwrap();
    node.monitor_environments().await.unwrap();
    reset_recovery_effects(&backend);
    assert_eq!(
        authority.cycle(&node).await.unwrap_err().code(),
        tonic::Code::Aborted
    );
    assert!(!authority.registration.reconciling);
    assert_no_recovery_effects(&backend);
    assert!(backend.actual.lock().unwrap().is_empty());
    assert_eq!(node.used(), Resources::default());
    authority.authority.records.lock().unwrap().clear();
    authority.cycle(&node).await.unwrap();
    assert_no_recovery_effects(&backend);
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn periodic_new_authoritative_execution_recovers_only_its_missing_controller() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let mut authority = authority::Fixture::new().await;
    authority.authority.records.lock().unwrap().push(
        record("kept", EnvironmentState::Running)
            .try_into()
            .unwrap(),
    );
    authority.cycle(&node).await.unwrap();
    reset_recovery_effects(&backend);
    backend.actual.lock().unwrap().push(observed("new"));
    authority
        .authority
        .records
        .lock()
        .unwrap()
        .push(record("new", EnvironmentState::Running).try_into().unwrap());
    authority.cycle(&node).await.unwrap();
    let events = backend.events.lock().unwrap().clone();
    assert!(events.contains(&"bind:new-7".into()));
    assert!(!events.contains(&"bind:kept-7".into()));
    assert_eq!(
        backend
            .readiness_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    reset_recovery_effects(&backend);
    authority.cycle(&node).await.unwrap();
    assert_no_recovery_effects(&backend);
}

#[tokio::test]
async fn restores_running_usage_without_start_and_cleans_uncommitted_orphans() {
    let b = Arc::new(Backend::default());
    *b.actual.lock().unwrap() = vec![observed("kept"), observed("orphan")];
    let n = manager(&b);
    n.reconcile(vec![record("kept", EnvironmentState::Running)])
        .await
        .unwrap();
    assert_eq!(n.used().cpu_millis, 2);
    assert_eq!(b.actual.lock().unwrap().len(), 1);
    let handle = n
        .environment(
            record("kept", EnvironmentState::Running).spec,
            record("kept", EnvironmentState::Running).assignment,
        )
        .unwrap();
    assert_eq!(
        handle.create().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert!(b.events.lock().unwrap().contains(&"remove:orphan-7".into()));
    handle.delete().await.unwrap();
    assert_eq!(n.used(), Resources::default());
}
#[tokio::test]
async fn missing_running_or_uncommitted_start_fails_without_recreation() {
    let b = Arc::new(Backend::default());
    *b.actual.lock().unwrap() = vec![observed("pending")];
    let n = manager(&b);
    n.reconcile(vec![
        record("missing", EnvironmentState::Running),
        record("pending", EnvironmentState::Pending),
    ])
    .await
    .unwrap();
    assert_eq!(n.used(), Resources::default());
    assert!(b.actual.lock().unwrap().is_empty());
    assert_eq!(
        b.events
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.as_str() == "commit:Failed")
            .count(),
        2
    );
    let events = b.events.lock().unwrap();
    assert!(events.contains(&"retire:missing-7".into()));
    assert!(events.contains(&"retire:pending-7".into()));
    assert!(!events.iter().any(|event| event.starts_with("bind:")));
}

#[tokio::test]
async fn invalid_catalog_does_not_clean_any_runtime() {
    let b = Arc::new(Backend::default());
    *b.actual.lock().unwrap() = vec![observed("orphan")];
    let n = manager(&b);
    let mut invalid = record("bad", EnvironmentState::Running);
    invalid.assignment.node_id = "other".into();
    assert!(n.reconcile(vec![invalid]).await.is_err());
    assert!(b.events.lock().unwrap().is_empty());
    assert_eq!(b.actual.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn repeated_reconciliation_preserves_usage_and_controller_identity() {
    let b = Arc::new(Backend::default());
    *b.actual.lock().unwrap() = vec![observed("kept")];
    let n = manager(&b);
    let r = record("kept", EnvironmentState::Running);
    n.reconcile(vec![r.clone()]).await.unwrap();
    n.reconcile(vec![r.clone()]).await.unwrap();
    assert_eq!(n.used().cpu_millis, 2);
    n.environment(r.spec, r.assignment)
        .unwrap()
        .delete()
        .await
        .unwrap();
    assert_eq!(n.used(), Resources::default());
}

#[tokio::test]
async fn unavailable_inventory_is_not_an_empty_catalog() {
    let b = Arc::new(Backend::default());
    *b.actual.lock().unwrap() = vec![observed("orphan")];
    b.fail_inventory
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let n = manager(&b);
    assert!(n.reconcile(vec![]).await.is_err());
    assert_eq!(b.actual.lock().unwrap().len(), 1);
    assert!(b.events.lock().unwrap().is_empty());
}
#[tokio::test]
async fn failed_cleanup_retains_claims_and_retry_releases_once() {
    let b = Arc::new(Backend::default());
    *b.actual.lock().unwrap() = vec![observed("pending")];
    b.fail_remove
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let n = manager(&b);
    let r = record("pending", EnvironmentState::Pending);
    assert!(n.reconcile(vec![r.clone()]).await.is_err());
    assert_eq!(n.used().cpu_millis, 2);
    b.fail_remove
        .store(false, std::sync::atomic::Ordering::SeqCst);
    n.reconcile(vec![r]).await.unwrap();
    assert_eq!(n.used(), Resources::default());
}

#[tokio::test]
async fn authority_removal_cleans_live_controller_and_fences_delayed_calls() {
    let b = Arc::new(Backend::default());
    *b.actual.lock().unwrap() = vec![observed("kept")];
    let n = manager(&b);
    let r = record("kept", EnvironmentState::Running);
    n.reconcile(vec![r.clone()]).await.unwrap();
    let old = n.environment(r.spec.clone(), r.assignment.clone()).unwrap();
    n.reconcile(vec![]).await.unwrap();
    assert!(b.actual.lock().unwrap().is_empty());
    assert_eq!(n.used(), Resources::default());
    assert!(old.create().await.is_err());
    assert!(n.environment(r.spec, r.assignment).is_err());
    assert_eq!(
        b.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.starts_with("commit:"))
            .count(),
        1
    );
}

#[tokio::test]
async fn reconnect_discards_live_controller_when_authority_invalidates_execution() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("lost")];
    let node = manager(&backend);
    let running = record("lost", EnvironmentState::Running);
    node.reconcile(vec![running.clone()]).await.unwrap();
    backend.events.lock().unwrap().clear();
    let mut failed = running;
    failed.state = EnvironmentState::Failed;
    failed.resources_held = false;
    failed.runtime.ip = None;
    failed.revision += 1;
    backend
        .fail_remove
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(node.reconcile(vec![failed.clone()]).await.is_err());
    assert!(!node.accepting_allocations());
    backend
        .fail_remove
        .store(false, std::sync::atomic::Ordering::SeqCst);
    node.reconcile(vec![failed]).await.unwrap();
    assert!(backend.actual.lock().unwrap().is_empty());
    assert_eq!(node.used(), Resources::default());
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e.starts_with("bind:")));
}

#[tokio::test]
async fn restart_after_interrupted_reconciliation_retries_physical_cleanup() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("stale")];
    backend
        .block_remove
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let entered = backend.remove_entered.notified();
    let first = Arc::new(manager(&backend));
    let reconciling = {
        let first = first.clone();
        tokio::spawn(async move { first.reconcile(vec![]).await })
    };
    tokio::time::timeout(Duration::from_secs(1), entered)
        .await
        .expect("first reconciliation did not enter physical cleanup");
    assert!(!first.accepting_allocations());
    assert_eq!(backend.actual.lock().unwrap().len(), 1);

    // Process loss cancels the in-flight cleanup. A fresh manager must inspect
    // sandboxd again and repeat the idempotent remove before opening admission.
    reconciling.abort();
    reconciling
        .await
        .expect_err("aborted task unexpectedly completed");
    let entered_again = backend.remove_entered.notified();
    let restarted = Arc::new(manager(&backend));
    let resumed = {
        let restarted = restarted.clone();
        tokio::spawn(async move { restarted.reconcile(vec![]).await })
    };
    tokio::time::timeout(Duration::from_secs(1), entered_again)
        .await
        .expect("restarted reconciliation did not retry physical cleanup");
    assert!(!restarted.accepting_allocations());
    backend.remove_release.notify_waiters();
    resumed.await.unwrap().unwrap();

    assert!(restarted.accepting_allocations());
    assert!(backend.actual.lock().unwrap().is_empty());
    assert_eq!(restarted.used(), Resources::default());
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.as_str() == "remove:stale-7")
            .count(),
        2
    );
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event.starts_with("bind:")));
}

#[tokio::test(start_paused = true)]
async fn missing_running_execution_reconciles_into_bounded_cold_restart() {
    let backend = Arc::new(Backend::default());
    backend
        .allow_start
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let node = manager(&backend);
    node.update_capacity(
        Resources {
            cpu_millis: 4,
            memory_bytes: 4,
            disk_bytes: 0,
        },
        Duration::from_secs(30),
    )
    .unwrap();
    let mut original = record("reboot", EnvironmentState::Running);
    original.spec.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 2,
        initial_backoff_seconds: 1,
        max_backoff_seconds: 1,
    });
    node.reconcile(vec![original.clone()]).await.unwrap();
    let handle = node
        .environment(original.spec.clone(), original.assignment.clone())
        .unwrap();
    let failed = handle.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(
        failed.restart_pending,
        "confirmed runtime loss during boot must retain configured restart intent"
    );
    assert_eq!(failed.restart_attempts, 0);
    assert!(!failed.resources_held);
    assert_eq!(node.used(), Resources::default());
    tokio::time::advance(Duration::from_secs(2)).await;
    handle.tick().await.unwrap();
    let recovered = handle.sync().await.unwrap().record;
    assert_eq!(recovered.state, EnvironmentState::Running);
    assert_eq!(recovered.assignment, original.assignment);
    assert_ne!(recovered.runtime.id, original.runtime.id);
    assert_eq!(recovered.restart_attempts, 1);
    assert!(!recovered.restart_pending);
    assert_eq!(backend.actual.lock().unwrap().len(), 1);
    let events = backend.events.lock().unwrap().clone();
    let removed = events
        .iter()
        .position(|event| event == "remove:reboot-7")
        .unwrap();
    let started = events
        .iter()
        .position(|event| event.starts_with("start:"))
        .unwrap();
    assert!(removed < started);
    handle.delete().await.unwrap();
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn boot_reconciliation_preserves_exhausted_restart_budget() {
    let backend = Arc::new(Backend::default());
    let node = manager(&backend);
    let mut original = record("exhausted", EnvironmentState::Running);
    original.spec.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 2,
        initial_backoff_seconds: 1,
        max_backoff_seconds: 1,
    });
    original.restart_attempts = 2;
    node.reconcile(vec![original.clone()]).await.unwrap();
    let handle = node
        .environment(original.spec, original.assignment)
        .unwrap();
    let failed = handle.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert_eq!(failed.restart_attempts, 2);
    assert!(!failed.restart_pending);
    assert!(!failed.resources_held);
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event.starts_with("start:")));
}

#[tokio::test]
async fn boot_restart_waits_for_confirmed_cleanup() {
    let backend = Arc::new(Backend::default());
    backend
        .allow_start
        .store(true, std::sync::atomic::Ordering::SeqCst);
    backend
        .fail_remove
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let node = manager(&backend);
    let mut original = record("blocked", EnvironmentState::Running);
    original.spec.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 2,
        initial_backoff_seconds: 1,
        max_backoff_seconds: 1,
    });
    assert!(node.reconcile(vec![original.clone()]).await.is_err());
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event.starts_with("start:")));
    backend
        .fail_remove
        .store(false, std::sync::atomic::Ordering::SeqCst);
    node.reconcile(vec![original.clone()]).await.unwrap();
    let handle = node
        .environment(original.spec, original.assignment)
        .unwrap();
    let failed = handle.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(failed.restart_pending);
    assert_eq!(failed.restart_attempts, 0);
    assert!(!failed.resources_held);
}

#[tokio::test]
async fn periodic_authority_outage_keeps_known_lifecycle_but_blocks_new_allocations() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = authority::manager(backend.clone());
    node.reconcile(vec![record("kept", EnvironmentState::Running)])
        .await
        .unwrap();
    let mut authority = authority::Fixture::new().await;
    authority
        .authority
        .unavailable
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(authority.cycle(&node).await.is_err());
    assert!(!node.accepting_allocations());
    let new = record("new", EnvironmentState::Pending);
    assert!(node.reserve_local(&new.spec).is_err());
    assert!(node.environment(new.spec, new.assignment).is_err());
    assert!(matches!(
        node.recover_environment(record("new", EnvironmentState::Running))
            .await,
        Err(adx_core::Error::Unavailable(_))
    ));
    // Existing failure/idle cleanup retains its established degradation path.
    backend.actual.lock().unwrap().clear();
    backend.events.lock().unwrap().clear();
    node.monitor_environments().await.unwrap();
    assert!(backend
        .events
        .lock()
        .unwrap()
        .contains(&"remove:kept-7".into()));
}

#[tokio::test]
async fn recovery_preserves_valid_execution_while_collecting_orphans() {
    let backend = Arc::new(Backend::default());
    *backend.actual.lock().unwrap() = vec![observed("kept")];
    let node = manager(&backend);
    let kept = record("kept", EnvironmentState::Running);
    node.reconcile(vec![kept.clone()]).await.unwrap();
    let mut authority = authority::Fixture::new().await;
    authority
        .authority
        .records
        .lock()
        .unwrap()
        .push(kept.clone().try_into().unwrap());
    authority.cycle(&node).await.unwrap();
    backend.actual.lock().unwrap().push(observed("orphan"));
    authority.registration.reconciling = true;
    authority.cycle(&node).await.unwrap();
    {
        let actual = backend.actual.lock().unwrap();
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].runtime_id, "kept-7");
    }
    assert_eq!(node.used(), kept.spec.resources);
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .contains(&"remove:kept-7".into()));
    assert_eq!(
        node.environment(kept.spec, kept.assignment)
            .unwrap()
            .create()
            .await
            .unwrap()
            .record
            .state,
        EnvironmentState::Running
    );
}

#[tokio::test]
async fn cached_log_retention_waits_for_recovery_and_cleanup_confirmation() {
    use std::sync::atomic::Ordering;
    let backend = Arc::new(Backend::default());
    let node = authority::manager(backend.clone());
    assert!(node.retained_runtime_ids().is_none());
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    assert!(node.retained_runtime_ids().unwrap().is_empty());
    backend.allow_start.store(true, Ordering::SeqCst);
    backend.fail_start.store(true, Ordering::SeqCst);
    backend.fail_remove.store(true, Ordering::SeqCst);
    let wanted = record("failed", EnvironmentState::Pending);
    let handle = node.environment(wanted.spec, wanted.assignment).unwrap();
    assert!(handle.create().await.is_err());
    assert_eq!(node.used(), Resources::default());
    assert!(node.retained_runtime_ids().unwrap().contains("failed-7"));
    backend.fail_remove.store(false, Ordering::SeqCst);
    handle.tick().await.unwrap();
    assert!(node.retained_runtime_ids().unwrap().is_empty());
}
