use adx_core::{Assignment, Error, InstanceRecord, InstanceSpec, InstanceState, Resources, Result};
use adx_node_manager::{Durability, NodeManager, Readiness, Routes, RuntimeBackend, StateSink};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

struct Dependencies {
    events: Mutex<Vec<String>>,
    fail_at: Mutex<Option<String>>,
    durability: Mutex<Durability>,
    start_entered: Semaphore,
    start_release: Semaphore,
    block_start: bool,
}

impl Dependencies {
    fn new(block_start: bool) -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::default(),
            fail_at: Mutex::default(),
            durability: Mutex::new(Durability::Published),
            start_entered: Semaphore::new(0),
            start_release: Semaphore::new(0),
            block_start,
        })
    }
    fn event(&self, name: &str) -> Result<()> {
        self.events.lock().unwrap().push(name.into());
        if self.fail_at.lock().unwrap().as_deref() == Some(name) {
            Err(Error::Unavailable(name.into()))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl RuntimeBackend for Dependencies {
    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(true)
    }
    async fn start(
        &self,
        _spec: &InstanceSpec,
        _runtime_id: &str,
        _generation: u64,
        _devices: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        self.event("start")?;
        self.start_entered.add_permits(1);
        if self.block_start {
            self.start_release.acquire().await.unwrap().forget();
        }
        Ok("10.0.0.2".parse().unwrap())
    }
    async fn remove(&self, _runtime_id: &str) -> Result<()> {
        self.event("remove")
    }
}
#[async_trait]
impl Readiness for Dependencies {
    async fn wait_ready(&self, _record: &InstanceRecord) -> Result<()> {
        self.event("ready")
    }
}
#[async_trait]
impl Routes for Dependencies {
    async fn activate(&self, _record: &InstanceRecord) -> Result<()> {
        self.event("activate")
    }
    async fn retire(&self, _record: &InstanceRecord) -> Result<()> {
        self.event("retire")
    }
}
#[async_trait]
impl StateSink for Dependencies {
    async fn commit(&self, record: &InstanceRecord) -> Result<Durability> {
        self.event(&format!("commit:{:?}", record.state))?;
        Ok(*self.durability.lock().unwrap())
    }
}

fn resources() -> Resources {
    Resources {
        cpu_millis: 1,
        memory_bytes: 1024,
        disk_bytes: 1024,
    }
}
fn spec(id: &str) -> InstanceSpec {
    InstanceSpec {
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        scheduling: Default::default(),
        id: id.into(),
        tenant_id: "t".into(),
        image: "image".into(),
        runtime: "runtime".into(),
        resources: resources(),
        priority: 0,
    }
}
fn assignment(id: &str) -> Assignment {
    Assignment {
        devices: vec![],
        instance_id: id.into(),
        node_id: "n1".into(),
        domain_id: 0,
        generation: 1,
    }
}
fn node(deps: &Arc<Dependencies>) -> NodeManager {
    let node = NodeManager::new(
        "n1".into(),
        deps.clone(),
        deps.clone(),
        deps.clone(),
        deps.clone(),
    );
    node.update_capacity(resources(), Duration::from_secs(30))
        .unwrap();
    node
}

#[tokio::test]
async fn create_publishes_only_after_runtime_readiness_and_route_binding() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let instance = node.instance(spec("a"), assignment("a")).unwrap();
    let created = instance.create().await.unwrap();
    assert_eq!(created.record.state, InstanceState::Running);
    assert_eq!(created.durability, Durability::Published);
    assert_eq!(
        *deps.events.lock().unwrap(),
        ["start", "ready", "activate", "commit:Running"]
    );
    instance.create().await.unwrap();
    assert_eq!(deps.events.lock().unwrap().len(), 4);
    assert_eq!(node.used(), resources());
    instance.delete().await.unwrap();
    assert_eq!(node.used(), Resources::default());
    assert_eq!(
        &deps.events.lock().unwrap()[4..],
        ["retire", "remove", "commit:Deleted"]
    );
    instance.delete().await.unwrap();
    assert_eq!(deps.events.lock().unwrap().len(), 7);
    assert_eq!(instance.create().await.unwrap_err(), Error::Conflict);
}

#[tokio::test]
async fn failed_readiness_is_cleaned_before_resources_are_reused() {
    let deps = Dependencies::new(false);
    *deps.fail_at.lock().unwrap() = Some("ready".into());
    let node = node(&deps);
    let instance = node.instance(spec("a"), assignment("a")).unwrap();
    assert!(instance.create().await.is_err());
    assert_eq!(
        *deps.events.lock().unwrap(),
        ["start", "ready", "retire", "remove", "commit:Failed"]
    );
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn failed_runtime_cleanup_keeps_reservation_and_can_be_retried() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let instance = node.instance(spec("a"), assignment("a")).unwrap();
    instance.create().await.unwrap();
    *deps.fail_at.lock().unwrap() = Some("remove".into());
    assert!(instance.delete().await.is_err());
    assert_eq!(node.used(), resources());
    assert!(instance.sync().await.unwrap().record.resources_held);
    *deps.fail_at.lock().unwrap() = None;
    instance.delete().await.unwrap();
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn node_without_any_valid_capacity_sample_rejects_new_instances() {
    let deps = Dependencies::new(false);
    let node = NodeManager::new(
        "n1".into(),
        deps.clone(),
        deps.clone(),
        deps.clone(),
        deps.clone(),
    );
    let instance = node.instance(spec("a"), assignment("a")).unwrap();
    assert_eq!(instance.create().await.unwrap_err(), Error::NoCapacity);
    assert!(deps.events.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn timed_out_start_requires_confirmed_cleanup_before_releasing_capacity() {
    let deps = Dependencies::new(true);
    let node = node(&deps)
        .with_operation_timeout(Duration::from_secs(2))
        .unwrap();
    let instance = node.instance(spec("a"), assignment("a")).unwrap();
    let task = tokio::spawn(async move { instance.create().await });
    deps.start_entered.acquire().await.unwrap().forget();
    tokio::time::advance(Duration::from_secs(3)).await;
    assert!(task.await.unwrap().is_err());
    assert_eq!(
        *deps.events.lock().unwrap(),
        ["start", "retire", "remove", "commit:Failed"]
    );
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn retired_route_failure_does_not_delete_runtime_or_release_resources() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let instance = node.instance(spec("a"), assignment("a")).unwrap();
    instance.create().await.unwrap();
    *deps.fail_at.lock().unwrap() = Some("retire".into());
    assert!(instance.delete().await.is_err());
    assert!(instance.sync().await.unwrap().record.resources_held);
    assert!(!deps.events.lock().unwrap().iter().any(|x| x == "remove"));
    assert_eq!(node.used(), resources());
}

#[tokio::test]
async fn deletion_commit_retry_does_not_repeat_cleanup() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let instance = node.instance(spec("a"), assignment("a")).unwrap();
    instance.create().await.unwrap();
    *deps.fail_at.lock().unwrap() = Some("commit:Deleted".into());
    assert!(instance.delete().await.is_err());
    assert_eq!(node.used(), Resources::default());
    *deps.fail_at.lock().unwrap() = None;
    let result = instance.delete().await.unwrap();
    assert_eq!(result.record.state, InstanceState::Deleted);
    assert!(!result.record.resources_held);
    assert_eq!(
        deps.events
            .lock()
            .unwrap()
            .iter()
            .filter(|x| *x == "remove")
            .count(),
        1
    );
}

#[tokio::test]
async fn journaled_result_is_explicitly_unpublished_and_can_be_synced() {
    let deps = Dependencies::new(false);
    *deps.durability.lock().unwrap() = Durability::Journaled;
    let node = node(&deps);
    let instance = node.instance(spec("a"), assignment("a")).unwrap();
    let created = instance.create().await.unwrap();
    assert_eq!(created.durability, Durability::Journaled);
    *deps.durability.lock().unwrap() = Durability::Published;
    let synced = instance.sync().await.unwrap();
    assert_eq!(synced.record, created.record);
    assert_eq!(synced.durability, Durability::Published);
    assert_eq!(
        deps.events
            .lock()
            .unwrap()
            .iter()
            .filter(|x| *x == "start")
            .count(),
        1
    );
}

#[tokio::test]
async fn failed_commit_does_not_claim_success_or_repeat_runtime_operation() {
    let deps = Dependencies::new(false);
    *deps.fail_at.lock().unwrap() = Some("commit:Running".into());
    let node = node(&deps);
    let instance = node.instance(spec("a"), assignment("a")).unwrap();
    assert!(instance.create().await.is_err());
    assert_eq!(node.used(), resources());
    *deps.fail_at.lock().unwrap() = None;
    assert_eq!(
        instance.create().await.unwrap().record.state,
        InstanceState::Running
    );
    assert_eq!(
        deps.events
            .lock()
            .unwrap()
            .iter()
            .filter(|x| *x == "start")
            .count(),
        1
    );
}

#[tokio::test]
async fn delete_waits_for_active_operation_even_if_original_caller_disconnects() {
    let deps = Dependencies::new(true);
    let node = node(&deps);
    let instance = node.instance(spec("a"), assignment("a")).unwrap();
    let creating = tokio::spawn({
        let instance = instance.clone();
        async move { instance.create().await }
    });
    deps.start_entered.acquire().await.unwrap().forget();
    creating.abort();
    let deleting = tokio::spawn({
        let instance = instance.clone();
        async move { instance.delete().await }
    });
    tokio::task::yield_now().await;
    assert_eq!(*deps.events.lock().unwrap(), ["start"]);
    deps.start_release.add_permits(1);
    assert_eq!(
        deleting.await.unwrap().unwrap().record.state,
        InstanceState::Deleted
    );
    assert_eq!(
        *deps.events.lock().unwrap(),
        [
            "start",
            "ready",
            "activate",
            "commit:Running",
            "retire",
            "remove",
            "commit:Deleted"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn capacity_expires_and_maintenance_only_blocks_new_admission() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    tokio::time::advance(Duration::from_secs(31)).await;
    let instance = node.instance(spec("a"), assignment("a")).unwrap();
    assert_eq!(instance.create().await.unwrap_err(), Error::NoCapacity);
    node.update_capacity(resources(), Duration::from_secs(30))
        .unwrap();
    node.set_maintenance(true);
    assert_eq!(instance.create().await.unwrap_err(), Error::NoCapacity);
    node.set_maintenance(false);
    instance.create().await.unwrap();
    node.set_maintenance(true);
    instance.delete().await.unwrap();
}

#[tokio::test]
async fn wrong_node_or_generation_cannot_acquire_existing_controller() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    node.instance(spec("a"), assignment("a")).unwrap();
    let mut other = assignment("a");
    other.generation = 2;
    assert!(node.instance(spec("a"), other).is_err());
    let mut other = assignment("b");
    other.node_id = "n2".into();
    assert!(node.instance(spec("b"), other).is_err());
}

#[tokio::test]
async fn local_device_admission_is_atomic_and_failed_cleanup_keeps_cards_reserved() {
    use adx_core::scheduling::*;
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let capacity = Resources {
        cpu_millis: 10,
        memory_bytes: 10240,
        disk_bytes: 10240,
    };
    node.update_capacity(capacity, Duration::from_secs(30))
        .unwrap();
    let cards = vec![Device {
        id: 0,
        kind: DeviceKind::Npu,
        model: "x".into(),
        healthy: true,
    }];
    node.update_devices(cards.clone(), Duration::from_secs(30))
        .unwrap();
    let mut a = spec("a");
    a.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Npu,
        model: Some("x".into()),
        count: 1,
    }];
    let mut assigned = assignment("a");
    assigned.devices = vec![DeviceAllocation {
        id: 0,
        kind: DeviceKind::Npu,
        model: "x".into(),
    }];
    let first = node.instance(a.clone(), assigned.clone()).unwrap();
    first.create().await.unwrap();
    a.id = "b".into();
    assigned.instance_id = "b".into();
    let second = node.instance(a, assigned).unwrap();
    assert!(matches!(second.create().await, Err(Error::NoCapacity)));
    assert_eq!(node.used(), resources());
    *deps.fail_at.lock().unwrap() = Some("remove".into());
    assert!(first.delete().await.is_err());
    node.update_devices(cards, Duration::from_secs(30)).unwrap();
    assert!(matches!(second.create().await, Err(Error::NoCapacity)));
    *deps.fail_at.lock().unwrap() = None;
    first.delete().await.unwrap();
    second.create().await.unwrap();
    assert_eq!(node.used(), resources());
}

#[tokio::test(start_paused = true)]
async fn stale_device_inventory_closes_accelerator_admission() {
    use adx_core::scheduling::*;
    let deps = Dependencies::new(false);
    let node = node(&deps);
    node.update_devices(
        vec![Device {
            id: 0,
            kind: DeviceKind::Gpu,
            model: "a".into(),
            healthy: true,
        }],
        Duration::from_secs(1),
    )
    .unwrap();
    let mut a = spec("a");
    a.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Gpu,
        model: None,
        count: 1,
    }];
    let mut assigned = assignment("a");
    assigned.devices = vec![DeviceAllocation {
        id: 0,
        kind: DeviceKind::Gpu,
        model: "a".into(),
    }];
    let handle = node.instance(a, assigned).unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(matches!(handle.create().await, Err(Error::NoCapacity)));
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn explicit_drain_requires_published_deletion_and_closes_admission() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    node.instance(spec("a"), assignment("a"))
        .unwrap()
        .create()
        .await
        .unwrap();
    *deps.fail_at.lock().unwrap() = Some("commit:Deleted".into());
    assert!(node.drain().await.is_err());
    assert!(node.is_draining());
    assert!(node.instance(spec("b"), assignment("b")).is_err());
    *deps.fail_at.lock().unwrap() = None;
    assert_eq!(node.drain().await.unwrap(), 1);
    assert_eq!(node.used(), Resources::default());
    assert_eq!(node.drain().await.unwrap(), 1);
    let events = deps.events.lock().unwrap();
    assert_eq!(events.iter().filter(|e| *e == "remove").count(), 1);
}

#[tokio::test]
async fn drain_before_authoritative_recovery_cannot_claim_empty_node() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    node.pause_lifecycle().await;
    assert!(node.drain().await.is_err());
    assert!(!node.is_draining());
}
