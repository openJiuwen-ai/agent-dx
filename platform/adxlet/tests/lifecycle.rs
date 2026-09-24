use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error, Resources, Result,
};
use adxlet::{Adxlet, Durability, Readiness, Routes, RuntimeDriver, StateSink};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

struct Dependencies {
    events: Mutex<Vec<String>>,
    traces: Mutex<Vec<(String, Option<String>)>>,
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
            traces: Mutex::default(),
            fail_at: Mutex::default(),
            durability: Mutex::new(Durability::Published),
            start_entered: Semaphore::new(0),
            start_release: Semaphore::new(0),
            block_start,
        })
    }
    fn event(&self, name: &str) -> Result<()> {
        self.events.lock().unwrap().push(name.into());
        self.traces
            .lock()
            .unwrap()
            .push((name.into(), adx_observability::trace::traceparent()));
        if self.fail_at.lock().unwrap().as_deref() == Some(name) {
            Err(Error::Unavailable(name.into()))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl RuntimeDriver for Dependencies {
    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(true)
    }
    async fn start(
        &self,
        _spec: &EnvironmentSpec,
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
    async fn set_network_policy(
        &self,
        _runtime_id: &str,
        policy: Option<&adx_core::sandbox::NetworkPolicy>,
        ports: &[u16],
    ) -> Result<()> {
        self.event(if policy.is_some() {
            "network:set"
        } else {
            "network:clear"
        })?;
        assert_eq!(ports, [8080]);
        Ok(())
    }
}
#[async_trait]
impl Readiness for Dependencies {
    async fn wait_ready(&self, _record: &EnvironmentRecord) -> Result<()> {
        self.event("ready")
    }
}
#[async_trait]
impl Routes for Dependencies {
    async fn activate(&self, _record: &EnvironmentRecord) -> Result<()> {
        self.event("activate")
    }
    async fn retire(&self, _record: &EnvironmentRecord) -> Result<()> {
        self.event("retire")
    }
}
#[async_trait]
impl StateSink for Dependencies {
    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability> {
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
fn spec(id: &str) -> EnvironmentSpec {
    EnvironmentSpec {
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        scheduling: Default::default(),
        id: id.into(),
        tenant_id: "t".into(),
        image: "image".into(),
        runtime_class: "runtime".into(),
        resources: resources(),
        priority: 0,
        sandbox: Default::default(),
    }
}
fn assignment(id: &str) -> Assignment {
    Assignment {
        devices: vec![],
        environment_id: id.into(),
        node_id: "n1".into(),
        shard_id: 0,
        generation: 1,
    }
}
fn node(deps: &Arc<Dependencies>) -> Adxlet {
    let node = Adxlet::new(
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
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    let created = environment.create().await.unwrap();
    assert_eq!(created.record.state, EnvironmentState::Running);
    assert_eq!(created.durability, Durability::Published);
    assert_eq!(
        *deps.events.lock().unwrap(),
        ["start", "ready", "activate", "commit:Running"]
    );
    environment.create().await.unwrap();
    assert_eq!(deps.events.lock().unwrap().len(), 4);
    assert_eq!(node.used(), resources());
    environment.delete().await.unwrap();
    assert_eq!(node.used(), Resources::default());
    assert_eq!(
        &deps.events.lock().unwrap()[4..],
        ["retire", "remove", "commit:Deleted"]
    );
    environment.delete().await.unwrap();
    assert_eq!(deps.events.lock().unwrap().len(), 7);
    assert_eq!(environment.create().await.unwrap_err(), Error::Conflict);
}

#[tokio::test]
async fn network_policy_replacement_is_serialized_and_published_idempotently() {
    use adx_core::sandbox::{NetworkAction, NetworkPolicy, TrafficMode, TrafficPolicy};
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let mut value = spec("network");
    value.sandbox.ports = vec![8080];
    let environment = node.environment(value, assignment("network")).unwrap();
    let created = environment.create().await.unwrap();
    let policy = NetworkPolicy {
        traffic: Some(TrafficPolicy {
            ingress_default_action: NetworkAction::Deny,
            egress_default_action: NetworkAction::Allow,
            rules: vec![],
            mode: TrafficMode::Stateful,
        }),
        dns: None,
    };
    let updated = environment
        .update_network_policy(
            Some(policy.clone()),
            "network-a".into(),
            created.record.revision,
        )
        .await
        .unwrap();
    assert_eq!(updated.record.spec.sandbox.network, Some(policy));
    assert_eq!(
        updated.record.last_operation.as_ref().unwrap().id,
        "network-a"
    );
    let replay = environment
        .update_network_policy(
            updated.record.spec.sandbox.network.clone(),
            "network-a".into(),
            created.record.revision,
        )
        .await
        .unwrap();
    assert_eq!(replay.record, updated.record);
    let cleared = environment
        .update_network_policy(None, "network-b".into(), updated.record.revision)
        .await
        .unwrap();
    assert!(cleared.record.spec.sandbox.network.is_none());
    let events = deps.events.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| *event == "network:set")
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| *event == "network:clear")
            .count(),
        1
    );
}

#[tokio::test]
async fn failed_readiness_is_cleaned_before_resources_are_reused() {
    let deps = Dependencies::new(false);
    *deps.fail_at.lock().unwrap() = Some("ready".into());
    let node = node(&deps);
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    assert!(environment.create().await.is_err());
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
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    environment.create().await.unwrap();
    *deps.fail_at.lock().unwrap() = Some("remove".into());
    assert!(environment.delete().await.is_err());
    assert_eq!(node.used(), resources());
    assert!(environment.sync().await.unwrap().record.resources_held);
    *deps.fail_at.lock().unwrap() = None;
    environment.delete().await.unwrap();
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn node_without_any_valid_capacity_sample_rejects_new_environments() {
    let deps = Dependencies::new(false);
    let node = Adxlet::new(
        "n1".into(),
        deps.clone(),
        deps.clone(),
        deps.clone(),
        deps.clone(),
    );
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    assert_eq!(environment.create().await.unwrap_err(), Error::NoCapacity);
    assert!(deps.events.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn timed_out_start_requires_confirmed_cleanup_before_releasing_capacity() {
    let deps = Dependencies::new(true);
    let node = node(&deps)
        .with_operation_timeout(Duration::from_secs(2))
        .unwrap();
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    let task = tokio::spawn(async move { environment.create().await });
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
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    environment.create().await.unwrap();
    *deps.fail_at.lock().unwrap() = Some("retire".into());
    assert!(environment.delete().await.is_err());
    assert!(environment.sync().await.unwrap().record.resources_held);
    assert!(!deps.events.lock().unwrap().iter().any(|x| x == "remove"));
    assert_eq!(node.used(), resources());
}

#[tokio::test]
async fn deletion_commit_retry_does_not_repeat_cleanup() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    environment.create().await.unwrap();
    *deps.fail_at.lock().unwrap() = Some("commit:Deleted".into());
    assert!(environment.delete().await.is_err());
    assert_eq!(node.used(), Resources::default());
    *deps.fail_at.lock().unwrap() = None;
    let result = environment.delete().await.unwrap();
    assert_eq!(result.record.state, EnvironmentState::Deleted);
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
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    let created = environment.create().await.unwrap();
    assert_eq!(created.durability, Durability::Journaled);
    *deps.durability.lock().unwrap() = Durability::Published;
    let synced = environment.sync().await.unwrap();
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
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    assert!(environment.create().await.is_err());
    assert_eq!(node.used(), resources());
    *deps.fail_at.lock().unwrap() = None;
    assert_eq!(
        environment.create().await.unwrap().record.state,
        EnvironmentState::Running
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
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    let creating = tokio::spawn({
        let environment = environment.clone();
        async move { environment.create().await }
    });
    deps.start_entered.acquire().await.unwrap().forget();
    creating.abort();
    let deleting = tokio::spawn({
        let environment = environment.clone();
        async move { environment.delete().await }
    });
    tokio::task::yield_now().await;
    assert_eq!(*deps.events.lock().unwrap(), ["start"]);
    deps.start_release.add_permits(1);
    assert_eq!(
        deleting.await.unwrap().unwrap().record.state,
        EnvironmentState::Deleted
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
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    assert_eq!(environment.create().await.unwrap_err(), Error::NoCapacity);
    node.update_capacity(resources(), Duration::from_secs(30))
        .unwrap();
    node.set_maintenance(true);
    assert_eq!(environment.create().await.unwrap_err(), Error::NoCapacity);
    node.set_maintenance(false);
    environment.create().await.unwrap();
    node.set_maintenance(true);
    environment.delete().await.unwrap();
}

#[tokio::test]
async fn wrong_node_or_generation_cannot_acquire_existing_controller() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    node.environment(spec("a"), assignment("a")).unwrap();
    let mut other = assignment("a");
    other.generation = 2;
    assert!(node.environment(spec("a"), other).is_err());
    let mut other = assignment("b");
    other.node_id = "n2".into();
    assert!(node.environment(spec("b"), other).is_err());
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
    let first = node.environment(a.clone(), assigned.clone()).unwrap();
    first.create().await.unwrap();
    a.id = "b".into();
    assigned.environment_id = "b".into();
    let second = node.environment(a, assigned).unwrap();
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
    let handle = node.environment(a, assigned).unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(matches!(handle.create().await, Err(Error::NoCapacity)));
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn explicit_drain_requires_published_deletion_and_closes_admission() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    node.environment(spec("a"), assignment("a"))
        .unwrap()
        .create()
        .await
        .unwrap();
    *deps.fail_at.lock().unwrap() = Some("commit:Deleted".into());
    assert!(node.drain().await.is_err());
    assert!(node.is_draining());
    assert!(node.environment(spec("b"), assignment("b")).is_err());
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

#[tokio::test]
async fn metrics_follow_reservations_and_capacity_shrink() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let environment = node
        .environment(spec("metrics"), assignment("metrics"))
        .unwrap();
    environment.create().await.unwrap();
    let text = node.metrics();
    assert!(text.contains("adx_node_capacity_cpu_millis 1\n"));
    assert!(text.contains("adx_node_available_memory_bytes 0\n"));
    node.update_capacity(
        Resources {
            cpu_millis: 1,
            memory_bytes: 512,
            disk_bytes: 1024,
        },
        Duration::from_secs(30),
    )
    .unwrap();
    let text = node.metrics();
    assert!(text.contains("adx_node_reserved_memory_bytes 1024\n"));
    assert!(text.contains("adx_node_overcommitted_memory_bytes 512\n"));
    environment.delete().await.unwrap();
    let text = node.metrics();
    assert!(text.contains("adx_node_reserved_memory_bytes 0\n"));
    assert!(text.contains("adx_node_available_memory_bytes 512\n"));
    node.set_maintenance(true);
    let text = node.metrics();
    assert!(text.contains("adx_node_available_memory_bytes 0\n"));
    assert!(text.contains("adx_node_capacity_memory_bytes 512\n"));
}

#[tokio::test]
async fn accepted_environment_operations_keep_request_context_after_caller_cancellation() {
    let _provider = adx_observability::trace::init("queue-test").unwrap();
    let deps = Dependencies::new(true);
    let node = node(&deps);
    let environment = node
        .environment(spec("trace"), assignment("trace"))
        .unwrap();
    let first = environment.clone();
    let create = tokio::spawn(
        adx_observability::trace::Trace::remote(
            "create",
            Some("00-11111111111111111111111111111111-1111111111111111-01"),
            None,
        )
        .run(async move { first.create().await }),
    );
    deps.start_entered.acquire().await.unwrap().forget();
    create.abort(); // Accepted operation keeps running in its controller.
    let delete = tokio::spawn(
        adx_observability::trace::Trace::remote(
            "delete",
            Some("00-22222222222222222222222222222222-2222222222222222-01"),
            None,
        )
        .run(async move { environment.delete().await }),
    );
    deps.start_release.add_permits(1);
    delete.await.unwrap().unwrap();
    for (event, parent) in deps.traces.lock().unwrap().iter() {
        let expected = if ["start", "ready", "activate", "commit:Running"].contains(&event.as_str())
        {
            "11111111111111111111111111111111"
        } else {
            "22222222222222222222222222222222"
        };
        assert_eq!(
            &parent.as_ref().expect("controller context missing")[3..35],
            expected,
            "event {event}"
        );
    }
}

#[tokio::test]
async fn local_reservations_share_one_hold_and_transfer_to_one_controller() {
    let deps = Dependencies::new(false);
    let manager = Arc::new(node(&deps));
    manager.update_runtime_classes(vec!["runc".into()]).unwrap();
    assert!(manager.reserve_local(&spec("a")).is_err());
    manager
        .update_runtime_classes(vec![spec("a").runtime_class])
        .unwrap();
    let a = manager.reserve_local(&spec("a")).unwrap();
    let b = manager.reserve_local(&spec("a")).unwrap();
    assert_eq!(a.token, b.token);
    assert_eq!(manager.used(), resources());
    assert_eq!(
        manager.reserve_local(&spec("b")).unwrap_err(),
        Error::NoCapacity
    );
    let x = manager.environment(spec("a"), assignment("a")).unwrap();
    let y = manager.environment(spec("a"), assignment("a")).unwrap();
    // A delayed losing/retry path must never release a transferred runtime hold.
    manager.release_local("a", &a).unwrap();
    let (x, y) = tokio::join!(x.create(), y.create());
    assert_eq!(x.unwrap(), y.unwrap());
    assert_eq!(manager.used(), resources());
    assert_eq!(
        deps.events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == "start")
            .count(),
        1
    );
    manager
        .environment(spec("a"), assignment("a"))
        .unwrap()
        .delete()
        .await
        .unwrap();
    assert_eq!(manager.used(), Resources::default());
}

#[tokio::test]
async fn losing_attempt_releases_only_its_token_and_partial_device_reservation_is_atomic() {
    use adx_core::scheduling::{DeviceKind, DeviceRequest};
    let deps = Dependencies::new(false);
    let manager = node(&deps);
    let mut bad = spec("a");
    bad.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Gpu,
        model: None,
        count: 1,
    }];
    assert!(manager.reserve_local(&bad).is_err());
    assert_eq!(manager.used(), Resources::default());
    let a = manager.reserve_local(&spec("a")).unwrap();
    manager.release_local("a", &a).unwrap();
    let b = manager.reserve_local(&spec("a")).unwrap();
    assert_ne!(a.token, b.token);
    manager.release_local("a", &a).unwrap();
    assert_eq!(manager.used(), resources());
    let mut other = spec("a");
    other.tenant_id = "other".into();
    assert_eq!(manager.reserve_local(&other).unwrap_err(), Error::Conflict);
    assert_eq!(manager.used(), resources());
    manager.release_local("a", &b).unwrap();
    assert_eq!(manager.used(), Resources::default());
}

#[tokio::test]
async fn local_gpu_npu_tokens_transfer_and_stale_release_cannot_free_cards() {
    use adx_core::scheduling::*;
    let deps = Dependencies::new(false);
    let manager = node(&deps);
    manager
        .update_capacity(
            Resources {
                cpu_millis: 100,
                memory_bytes: 10240,
                disk_bytes: 10240,
            },
            Duration::from_secs(30),
        )
        .unwrap();
    let cards = vec![
        Device {
            id: 0,
            kind: DeviceKind::Gpu,
            model: "gpu".into(),
            healthy: true,
        },
        Device {
            id: 1,
            kind: DeviceKind::Npu,
            model: "npu".into(),
            healthy: true,
        },
    ];
    manager
        .update_devices(cards, Duration::from_secs(30))
        .unwrap();
    let mut requested = spec("cards");
    requested.scheduling.devices = vec![
        DeviceRequest {
            kind: DeviceKind::Gpu,
            model: None,
            count: 1,
        },
        DeviceRequest {
            kind: DeviceKind::Npu,
            model: None,
            count: 1,
        },
    ];
    let token = manager.reserve_local(&requested).unwrap();
    assert_eq!(token.devices.len(), 2);
    let mut assigned = assignment("cards");
    assigned.devices = token.devices.clone();
    let environment = manager.environment(requested.clone(), assigned).unwrap();
    environment.create().await.unwrap();
    manager.release_local("cards", &token).unwrap();
    requested.id = "other".into();
    assert_eq!(
        manager.reserve_local(&requested).unwrap_err(),
        Error::NoCapacity
    );
    assert_eq!(manager.used(), resources());
    environment.delete().await.unwrap();
    let next = manager.reserve_local(&requested).unwrap();
    assert_eq!(next.devices, token.devices);
    manager.release_local("other", &next).unwrap();
    assert_eq!(manager.used(), Resources::default());
}
