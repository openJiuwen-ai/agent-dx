use adx_core::{Assignment, InstanceRecord, InstanceSpec, InstanceState, Resources, Result};
use adx_node_manager::{
    Durability, NodeManager, Readiness, Routes, RuntimeBackend, RuntimeObservation, StateSink,
};
use async_trait::async_trait;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Backend {
    actual: Mutex<Vec<RuntimeObservation>>,
    fail_inventory: std::sync::atomic::AtomicBool,
    fail_remove: std::sync::atomic::AtomicBool,
    block_remove: std::sync::atomic::AtomicBool,
    remove_entered: tokio::sync::Notify,
    remove_release: tokio::sync::Notify,
    events: Mutex<Vec<String>>,
}
#[async_trait]
impl RuntimeBackend for Backend {
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
        Ok(self
            .actual
            .lock()
            .unwrap()
            .iter()
            .any(|v| v.runtime_id == id && v.running))
    }
    async fn start(
        &self,
        _: &InstanceSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        panic!("reconciliation must not recreate an image")
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
    async fn wait_ready(&self, _: &InstanceRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl Routes for Backend {
    async fn activate(&self, r: &InstanceRecord) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("bind:{}", r.runtime_id));
        Ok(())
    }
    async fn retire(&self, record: &InstanceRecord) -> Result<()> {
        self.events
            .lock()
            .unwrap()
            .push(format!("retire:{}", record.runtime_id));
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
    async fn commit(&self, r: &InstanceRecord) -> Result<Durability> {
        self.events
            .lock()
            .unwrap()
            .push(format!("commit:{:?}", r.state));
        Ok(Durability::Published)
    }
}
fn record(id: &str, state: InstanceState) -> InstanceRecord {
    InstanceRecord {
        restart_attempts: 0,
        restart_pending: false,
        spec: InstanceSpec {
            runtime_environment: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            id: id.into(),
            tenant_id: "t".into(),
            image: "image".into(),
            runtime: "runc".into(),
            resources: Resources {
                cpu_millis: 2,
                memory_bytes: 2,
                disk_bytes: 0,
            },
            priority: 0,
            sandbox: Default::default(),
        },
        assignment: Assignment {
            instance_id: id.into(),
            node_id: "n".into(),
            shard_id: 0,
            generation: 7,
            devices: vec![],
        },
        state,
        revision: if state == InstanceState::Pending {
            0
        } else {
            2
        },
        runtime_id: format!("{id}-7"),
        runtime_ip: Some("10.0.0.2".parse().unwrap()),
        checkpoint: None,
        last_operation: None,
        resources_held: true,
    }
}
fn observed(id: &str) -> RuntimeObservation {
    RuntimeObservation {
        instance_id: id.into(),
        runtime_id: format!("{id}-7"),
        generation: 7,
        tenant_id: "t".into(),
        running: true,
    }
}
fn manager(b: &Arc<Backend>) -> NodeManager {
    let n = NodeManager::new("n".into(), b.clone(), b.clone(), b.clone(), b.clone());
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
#[tokio::test]
async fn restores_running_usage_without_start_and_cleans_uncommitted_orphans() {
    let b = Arc::new(Backend::default());
    *b.actual.lock().unwrap() = vec![observed("kept"), observed("orphan")];
    let n = manager(&b);
    n.reconcile(vec![record("kept", InstanceState::Running)])
        .await
        .unwrap();
    assert_eq!(n.used().cpu_millis, 2);
    assert_eq!(b.actual.lock().unwrap().len(), 1);
    let handle = n
        .instance(
            record("kept", InstanceState::Running).spec,
            record("kept", InstanceState::Running).assignment,
        )
        .unwrap();
    assert_eq!(
        handle.create().await.unwrap().record.state,
        InstanceState::Running
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
        record("missing", InstanceState::Running),
        record("pending", InstanceState::Pending),
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
    let mut invalid = record("bad", InstanceState::Running);
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
    let r = record("kept", InstanceState::Running);
    n.reconcile(vec![r.clone()]).await.unwrap();
    n.reconcile(vec![r.clone()]).await.unwrap();
    assert_eq!(n.used().cpu_millis, 2);
    n.instance(r.spec, r.assignment)
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
    let r = record("pending", InstanceState::Pending);
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
    let r = record("kept", InstanceState::Running);
    n.reconcile(vec![r.clone()]).await.unwrap();
    let old = n.instance(r.spec.clone(), r.assignment.clone()).unwrap();
    n.reconcile(vec![]).await.unwrap();
    assert!(b.actual.lock().unwrap().is_empty());
    assert_eq!(n.used(), Resources::default());
    assert!(old.create().await.is_err());
    assert!(n.instance(r.spec, r.assignment).is_err());
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
    let running = record("lost", InstanceState::Running);
    node.reconcile(vec![running.clone()]).await.unwrap();
    backend.events.lock().unwrap().clear();
    let mut failed = running;
    failed.state = InstanceState::Failed;
    failed.resources_held = false;
    failed.runtime_ip = None;
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
