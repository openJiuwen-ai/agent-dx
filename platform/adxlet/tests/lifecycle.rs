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
    probe_failure: Mutex<Option<adx_core::probes::ProbeFailure>>,
    records: Mutex<Vec<EnvironmentRecord>>,
    traces: Mutex<Vec<(String, Option<String>)>>,
    fail_at: Mutex<Option<String>>,
    durability: Mutex<Durability>,
    start_entered: Semaphore,
    start_release: Semaphore,
    block_start: bool,
    block_stage: Mutex<Option<String>>,
    actual: std::sync::atomic::AtomicBool,
    fail_cleanup: std::sync::atomic::AtomicBool,
    failure: Mutex<Option<adx_core::RuntimeFailure>>,
}

impl Dependencies {
    fn new(block_start: bool) -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::default(),
            probe_failure: Mutex::default(),
            records: Mutex::default(),
            traces: Mutex::default(),
            fail_at: Mutex::default(),
            durability: Mutex::new(Durability::Published),
            start_entered: Semaphore::new(0),
            start_release: Semaphore::new(0),
            block_start,
            block_stage: Mutex::default(),
            actual: std::sync::atomic::AtomicBool::new(false),
            fail_cleanup: std::sync::atomic::AtomicBool::new(false),
            failure: Mutex::default(),
        })
    }
    fn event(&self, name: &str) -> Result<()> {
        self.events.lock().unwrap().push(name.into());
        self.traces
            .lock()
            .unwrap()
            .push((name.into(), adx_observability::trace::traceparent()));
        if self.fail_at.lock().unwrap().as_deref() == Some(name)
            || (name == "remove" && self.fail_cleanup.load(std::sync::atomic::Ordering::SeqCst))
        {
            Err(Error::Unavailable(name.into()))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl RuntimeDriver for Dependencies {
    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(self.actual.load(std::sync::atomic::Ordering::SeqCst))
    }
    async fn observe(&self, id: &str) -> Result<adxlet::RuntimeStatus> {
        Ok(adxlet::RuntimeStatus {
            running: self.is_running(id).await?,
            failure: self.failure.lock().unwrap().clone(),
        })
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
        self.actual.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok("10.0.0.2".parse().unwrap())
    }
    async fn remove(&self, _runtime_id: &str) -> Result<()> {
        self.event("remove")?;
        self.actual
            .store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    async fn set_network_policy(
        &self,
        _runtime_id: &str,
        policy: Option<&adx_core::execution::NetworkPolicy>,
        ports: &[u16],
        _tunnel_http_port: Option<u16>,
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
    async fn probe_failure(
        &self,
        _: &EnvironmentRecord,
    ) -> Result<Option<adx_core::probes::ProbeFailure>> {
        Ok(self.probe_failure.lock().unwrap().clone())
    }
    async fn wait_ready(&self, _record: &EnvironmentRecord) -> Result<()> {
        self.event("ready")?;
        let block = self.block_stage.lock().unwrap().as_deref() == Some("ready");
        if block {
            std::future::pending::<()>().await;
        }
        if let Some(failure) = self
            .probe_failure
            .lock()
            .unwrap()
            .as_ref()
            .filter(|f| f.kind == adx_core::probes::ProbeFailureKind::ReadinessTimeout)
        {
            return Err(Error::ProbeFailed(failure.clone()));
        }
        Ok(())
    }
}
#[async_trait]
impl Routes for Dependencies {
    async fn activate(&self, _record: &EnvironmentRecord) -> Result<()> {
        let block = self.block_stage.lock().unwrap().as_deref() == Some("activate");
        if block {
            std::future::pending::<()>().await;
        }
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
        self.records.lock().unwrap().push(record.clone());
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
        execution: Default::default(),
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
async fn referenced_volume_cannot_start_without_coordinator_authorization() {
    use adx_core::{
        execution::{Mount, MountSource},
        volumes::VolumeRef,
    };
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let mut spec = spec("volume");
    spec.execution.mounts.push(Mount {
        kind: "bind".into(),
        target: "/workspace".into(),
        options: vec!["rw".into()],
        source: MountSource::Volume(VolumeRef {
            id: "workspace".into(),
            uid: Some("uid".into()),
        }),
        create_if_missing: false,
        create_options: None,
    });
    let environment = node.environment(spec, assignment("volume")).unwrap();
    assert!(environment.create().await.is_err());
    assert!(!deps
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event == "start"));
    assert!(!deps.actual.load(std::sync::atomic::Ordering::SeqCst));
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
        ["commit:Running", "retire", "remove", "commit:Deleted"]
    );
    environment.delete().await.unwrap();
    assert_eq!(deps.events.lock().unwrap().len(), 8);
    assert_eq!(environment.create().await.unwrap_err(), Error::Conflict);
}

#[tokio::test]
async fn failed_collection_is_fenced_and_keeps_metadata_until_cleanup_succeeds() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let environment = node.environment(spec("gc"), assignment("gc")).unwrap();
    let running = environment.create().await.unwrap();
    assert_eq!(
        environment
            .collect_failed(running.record.revision)
            .await
            .unwrap_err(),
        Error::Conflict
    );
    assert!(deps.actual.load(std::sync::atomic::Ordering::SeqCst));
    deps.actual
        .store(false, std::sync::atomic::Ordering::SeqCst);
    environment.tick().await.unwrap();
    let failed = environment.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert_eq!(
        environment
            .collect_failed(failed.revision - 1)
            .await
            .unwrap_err(),
        Error::Conflict
    );
    deps.fail_cleanup
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(environment.collect_failed(failed.revision).await.is_err());
    let retry = environment.sync().await.unwrap().record;
    assert_eq!(retry.state, EnvironmentState::Failed);
    deps.fail_cleanup
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let deleted = environment.collect_failed(retry.revision).await.unwrap();
    assert_eq!(deleted.record.state, EnvironmentState::Deleted);
    assert_eq!(deleted.durability, Durability::Published);
    assert!(!deleted.record.resources_held);
}

#[tokio::test]
async fn failed_collection_does_not_cancel_pending_restart() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let mut value = spec("restart-gc");
    value.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 2,
        initial_backoff_seconds: 10,
        max_backoff_seconds: 10,
    });
    let environment = node.environment(value, assignment("restart-gc")).unwrap();
    environment.create().await.unwrap();
    deps.actual
        .store(false, std::sync::atomic::Ordering::SeqCst);
    environment.tick().await.unwrap();
    let failed = environment.sync().await.unwrap().record;
    assert!(failed.restart_pending);
    assert_eq!(
        environment
            .collect_failed(failed.revision)
            .await
            .unwrap_err(),
        Error::Conflict
    );
    assert!(environment.sync().await.unwrap().record.restart_pending);
}

#[tokio::test]
async fn network_policy_replacement_is_serialized_and_published_idempotently() {
    use adx_core::execution::{NetworkAction, NetworkPolicy, TrafficMode, TrafficPolicy};
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let mut value = spec("network");
    value.execution.ports = vec![8080];
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
    assert_eq!(updated.record.spec.execution.network, Some(policy));
    assert_eq!(
        updated.record.last_operation.as_ref().unwrap().id,
        "network-a"
    );
    let replay = environment
        .update_network_policy(
            updated.record.spec.execution.network.clone(),
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
    assert!(cleared.record.spec.execution.network.is_none());
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
        ["start", "ready", "commit:Failed", "retire", "remove"]
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
async fn failed_create_cleans_late_runtime_after_reservation_was_released() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    *deps.fail_at.lock().unwrap() = Some("start".into());
    assert!(environment.create().await.is_err());
    assert_eq!(node.used(), Resources::default());
    assert_eq!(
        environment.sync().await.unwrap().record.state,
        EnvironmentState::Failed
    );
    *deps.fail_at.lock().unwrap() = None;
    deps.actual.store(true, std::sync::atomic::Ordering::SeqCst);
    deps.events.lock().unwrap().clear();
    environment.tick().await.unwrap();
    assert!(!deps.actual.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(*deps.events.lock().unwrap(), ["retire", "remove"]);
    assert_eq!(
        environment.sync().await.unwrap().record.state,
        EnvironmentState::Failed
    );
    assert!(environment.create().await.is_err());
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn failed_create_releases_reservation_even_when_cleanup_and_commit_fail() {
    let deps = Dependencies::new(true);
    let node = node(&deps)
        .with_operation_timeout(Duration::from_millis(10))
        .unwrap();
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    *deps.fail_at.lock().unwrap() = Some("commit:Failed".into());
    deps.fail_cleanup
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(environment.create().await.is_err());
    assert_eq!(node.used(), Resources::default());
    // Publishing the released reservation is retried even if backend cleanup
    // continues failing. Neither failure may reclaim another reservation.
    *deps.fail_at.lock().unwrap() = None;
    deps.events.lock().unwrap().clear();
    assert!(node.monitor_environments().await.is_err());
    assert!(deps
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e == "commit:Failed"));
    let failed = environment.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(!failed.resources_held);
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn failed_create_releases_scalar_and_devices_before_cleanup_succeeds() {
    use adx_core::scheduling::*;
    for kind in [DeviceKind::Gpu, DeviceKind::Npu] {
        let deps = Dependencies::new(false);
        let node = node(&deps);
        node.update_devices(
            vec![Device {
                id: 0,
                kind,
                model: "x".into(),
                healthy: true,
            }],
            Duration::from_secs(30),
        )
        .unwrap();
        let mut first_spec = spec("a");
        first_spec.scheduling.devices = vec![DeviceRequest {
            kind,
            model: None,
            count: 1,
        }];
        let mut assigned = assignment("a");
        assigned.devices = vec![DeviceAllocation {
            id: 0,
            kind,
            model: "x".into(),
        }];
        let first = node
            .environment(first_spec.clone(), assigned.clone())
            .unwrap();
        *deps.fail_at.lock().unwrap() = Some("ready".into());
        deps.fail_cleanup
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(first.create().await.is_err());
        let failed = first.sync().await.unwrap().record;
        assert_eq!(failed.state, EnvironmentState::Failed);
        assert!(!failed.resources_held);
        assert_eq!(node.used(), Resources::default());
        // The same scalar capacity and physical card are available to another
        // Environment; repeated cleanup of the failed one must not release them.
        *deps.fail_at.lock().unwrap() = None;
        first_spec.id = "b".into();
        assigned.environment_id = "b".into();
        let second = node.environment(first_spec, assigned).unwrap();
        second.create().await.unwrap();
        assert!(first.tick().await.is_err());
        assert_eq!(node.used(), resources());
        deps.fail_cleanup
            .store(false, std::sync::atomic::Ordering::SeqCst);
        first.tick().await.unwrap();
        assert_eq!(node.used(), resources());
        assert!(second.sync().await.unwrap().record.resources_held);
    }
}

#[tokio::test]
async fn failed_create_cleanup_retries_while_journal_publication_is_unavailable() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    *deps.durability.lock().unwrap() = Durability::Journaled;
    *deps.fail_at.lock().unwrap() = Some("ready".into());
    deps.fail_cleanup
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(environment.create().await.is_err());
    assert_eq!(node.used(), Resources::default());
    *deps.fail_at.lock().unwrap() = Some("commit:Failed".into());
    deps.events.lock().unwrap().clear();
    assert!(node.monitor_environments().await.is_err());
    assert!(deps
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event == "remove"));
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
async fn timed_out_start_releases_capacity_before_cleanup() {
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
        ["start", "commit:Failed", "retire", "remove"]
    );
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test(start_paused = true)]
async fn create_deadline_identifies_the_active_stage() {
    for (blocked, expected) in [
        ("start", "backend preparation/runtime start"),
        ("ready", "Execd readiness"),
        ("activate", "route activation"),
    ] {
        let deps = Dependencies::new(blocked == "start");
        *deps.block_stage.lock().unwrap() = Some(blocked.into());
        let node = node(&deps);
        let environment = node
            .environment(spec("timeout"), assignment("timeout"))
            .unwrap();
        let error = environment
            .create_with_deadline(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        let status = adx_protocol::status(error);
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded, "{status:?}");
        assert!(status.message().contains(expected), "{status:?}");
        let failed = environment.sync().await.unwrap().record;
        assert_eq!(failed.state, EnvironmentState::Failed);
        assert!(!failed.resources_held);
        assert!(failed.failure.unwrap().message.contains(expected));
        environment.tick().await.unwrap();
        assert!(!deps.actual.load(std::sync::atomic::Ordering::SeqCst));
    }
}

#[tokio::test(start_paused = true)]
async fn timed_out_create_with_unconfirmed_publication_retains_retry_identity() {
    let deps = Dependencies::new(true);
    *deps.fail_at.lock().unwrap() = Some("commit:Failed".into());
    let node = node(&deps);
    let environment = node
        .environment(spec("timeout"), assignment("timeout"))
        .unwrap();
    let error = environment
        .create_with_deadline(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap_err();
    let status = adx_protocol::status(error);
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    assert!(status
        .message()
        .contains("backend preparation/runtime start"));
    assert!(
        adx_protocol::deadline_failure(&status).is_none(),
        "{status:?}"
    );
    assert_eq!(node.used(), Resources::default());
    *deps.fail_at.lock().unwrap() = None;
    environment.tick().await.unwrap();
    let record = environment.sync().await.unwrap().record;
    assert_eq!(record.state, EnvironmentState::Failed);
    assert!(record
        .failure
        .unwrap()
        .message
        .contains("backend preparation/runtime start"));
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
            "commit:Running", // publish delete intent before cleanup
            "retire",
            "remove",
            "commit:Deleted"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn expired_capacity_remains_usable_and_maintenance_blocks_new_admission() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(node.accepting_allocations());
    assert!(node
        .metrics()
        .contains("adx_node_accepting_allocations 1\n"));
    assert!(node
        .metrics()
        .contains("adx_node_resource_observation_fresh 0\n"));
    let environment = node.environment(spec("a"), assignment("a")).unwrap();
    environment.create().await.unwrap();
    node.set_maintenance(true);
    environment.delete().await.unwrap();
    assert_eq!(node.used(), Resources::default());
    let next = node.environment(spec("b"), assignment("b")).unwrap();
    assert_eq!(next.create().await.unwrap_err(), Error::NoCapacity);
    node.set_maintenance(false);
    next.create().await.unwrap();
    node.set_maintenance(true);
    next.delete().await.unwrap();
    assert!(node
        .metrics()
        .contains("adx_node_accepting_allocations 0\n"));
    node.set_maintenance(false);
    node.set_pressure(true);
    assert!(node
        .metrics()
        .contains("adx_node_accepting_allocations 0\n"));
    node.set_pressure(false);
    node.update_runtime_classes(vec![]).unwrap();
    assert!(node
        .metrics()
        .contains("adx_node_accepting_allocations 0\n"));
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

#[tokio::test(start_paused = true)]
async fn zero_capacity_replaces_live_observations_without_releasing_running_work() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let first = node
        .environment(spec("running"), assignment("running"))
        .unwrap();
    first.create().await.unwrap();
    let events = deps.events.lock().unwrap().clone();
    tokio::time::advance(Duration::from_secs(20)).await;
    for (cpu_millis, memory_bytes) in [(0, 1024), (1, 0), (0, 0)] {
        node.update_capacity(
            Resources {
                cpu_millis,
                memory_bytes,
                disk_bytes: 1024,
            },
            Duration::from_secs(30),
        )
        .unwrap();
        // A valid zero observation refreshes freshness, not the reservation ledger.
        tokio::time::advance(Duration::from_secs(11)).await;
        assert!(node.accepting_allocations());
        assert_eq!(node.used(), resources());
        assert_eq!(
            node.reserve_local(&spec("new")).unwrap_err(),
            Error::NoCapacity
        );
        assert_eq!(
            first.create().await.unwrap().record.state,
            EnvironmentState::Running
        );
        let metrics = node.metrics();
        assert!(metrics.contains(&format!("adx_node_capacity_cpu_millis {cpu_millis}\n")));
        assert!(metrics.contains(&format!("adx_node_capacity_memory_bytes {memory_bytes}\n")));
        assert!(metrics.contains("adx_node_resource_observation_fresh 1\n"));
    }
    assert_eq!(*deps.events.lock().unwrap(), events);
    node.update_capacity(resources(), Duration::from_secs(30))
        .unwrap();
    assert_eq!(
        node.reserve_local(&spec("new")).unwrap_err(),
        Error::NoCapacity
    );
    first.delete().await.unwrap();
    assert_eq!(node.used(), Resources::default());
    let hold = node.reserve_local(&spec("new")).unwrap();
    node.release_local("new", &hold).unwrap();
}

#[tokio::test]
async fn capacity_below_used_recovers_by_growth_or_normal_release() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let capacity = Resources {
        cpu_millis: 3,
        memory_bytes: 3072,
        disk_bytes: 3072,
    };
    node.update_capacity(capacity, Duration::from_secs(30))
        .unwrap();
    let a = node.reserve_local(&spec("a")).unwrap();
    let b = node.reserve_local(&spec("b")).unwrap();
    node.update_capacity(resources(), Duration::from_secs(30))
        .unwrap();
    assert_eq!(
        node.used(),
        Resources {
            cpu_millis: 2,
            memory_bytes: 2048,
            disk_bytes: 2048
        }
    );
    assert_eq!(
        node.reserve_local(&spec("c")).unwrap_err(),
        Error::NoCapacity
    );
    assert!(node
        .metrics()
        .contains("adx_node_overcommitted_cpu_millis 1\n"));
    node.update_capacity(capacity, Duration::from_secs(30))
        .unwrap();
    let c = node.reserve_local(&spec("c")).unwrap();
    node.release_local("c", &c).unwrap();
    node.update_capacity(resources(), Duration::from_secs(30))
        .unwrap();
    node.release_local("a", &a).unwrap();
    assert_eq!(node.used(), resources());
    assert_eq!(
        node.reserve_local(&spec("c")).unwrap_err(),
        Error::NoCapacity
    );
    node.release_local("b", &b).unwrap();
    let c = node.reserve_local(&spec("c")).unwrap();
    node.release_local("c", &c).unwrap();
    assert_eq!(node.used(), Resources::default());
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
async fn stale_device_inventory_preserves_capacity_and_existing_card_reservations() {
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
    let handle = node.environment(a.clone(), assigned.clone()).unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    handle.create().await.unwrap();
    assert_eq!(node.used(), resources());
    let mut expanded = resources();
    expanded.cpu_millis *= 2;
    expanded.memory_bytes *= 2;
    expanded.disk_bytes *= 2;
    node.update_capacity(expanded, Duration::from_secs(30))
        .unwrap();
    a.id = "b".into();
    assigned.environment_id = "b".into();
    let next = node.environment(a, assigned).unwrap();
    assert_eq!(next.create().await.unwrap_err(), Error::NoCapacity);
    handle.delete().await.unwrap();
    next.create().await.unwrap();
    next.delete().await.unwrap();
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
    assert_eq!(node.drain().await.unwrap(), 0);
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
    for (index, (event, parent)) in deps.traces.lock().unwrap().iter().enumerate() {
        let expected = if index < 4 {
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
    let (x, y) = (x.unwrap(), y.unwrap());
    assert_eq!(u8::from(x.newly_created) + u8::from(y.newly_created), 1);
    assert_eq!(x.record, y.record);
    assert_eq!(x.durability, y.durability);
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

#[tokio::test]
async fn backend_oom_is_committed_before_cleanup_and_retained_on_retry() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let environment = node.environment(spec("oom"), assignment("oom")).unwrap();
    environment.create().await.unwrap();
    let failure = adx_core::RuntimeFailure {
        probe_failure: None,
        reason: adx_core::RuntimeFailureReason::OomKilled,
        exit_code: Some(137),
        message: "environment backend was OOM-killed".into(),
    };
    *deps.failure.lock().unwrap() = Some(failure.clone());
    deps.actual
        .store(false, std::sync::atomic::Ordering::SeqCst);
    deps.fail_cleanup
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(environment.tick().await.is_err());
    let record = environment.sync().await.unwrap().record;
    assert_eq!(record.state, EnvironmentState::Failed);
    assert_eq!(record.failure, Some(failure.clone()));
    // Cleanup remains unresolved; publishing the reason must not claim the
    // backend is absent or silently release a still-held reservation.
    assert!(record.resources_held);
    deps.fail_cleanup
        .store(false, std::sync::atomic::Ordering::SeqCst);
    environment.tick().await.unwrap();
    assert_eq!(node.used(), Resources::default());
    assert_eq!(
        environment.sync().await.unwrap().record.failure,
        Some(failure)
    );
    environment.delete().await.unwrap();
}

fn workload_spec(id: &str, healthcheck: bool) -> EnvironmentSpec {
    use adx_core::{execution::*, probes::*};
    let mut spec = spec(id);
    spec.execution.inherit_entrypoint = true;
    spec.execution.rootfs = Some(Rootfs {
        readonly: false,
        source: StorageSource::Image("image".into()),
    });
    let probe = ProbeSpec {
        action: ProbeAction::TcpSocket { port: 8080 },
        initial_delay_seconds: 0,
        period_seconds: 1,
        timeout_seconds: 1,
        failure_threshold: 1,
    };
    if healthcheck {
        spec.execution.healthcheck_probe = Some(probe);
    } else {
        spec.execution.readiness_probe = Some(probe);
    }
    spec
}
#[tokio::test]
async fn readiness_final_failure_commits_reason_without_activating_route() {
    let deps = Dependencies::new(false);
    let failure = adx_core::probes::ProbeFailure::timeout("HTTP 503");
    *deps.probe_failure.lock().unwrap() = Some(failure.clone());
    let manager = node(&deps);
    let environment = manager
        .environment(workload_spec("probe", false), assignment("probe"))
        .unwrap();
    assert_eq!(
        environment.create().await.unwrap_err(),
        Error::ProbeFailed(failure.clone())
    );
    let result = environment.sync().await.unwrap();
    assert_eq!(result.record.state, EnvironmentState::Failed);
    assert_eq!(result.record.failure, Some(failure.into()));
    assert!(!deps.events.lock().unwrap().contains(&"activate".into()));
    assert!(!deps.actual.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(manager.used(), Resources::default());
}
#[tokio::test]
async fn healthcheck_event_cleans_once_and_rejects_old_execution() {
    let deps = Dependencies::new(false);
    let manager = node(&deps);
    let environment = manager
        .environment(workload_spec("probe", true), assignment("probe"))
        .unwrap();
    let created = environment.create().await.unwrap();
    let identity = adxlet::runtime_control::RuntimeControlClient::identity(&created.record);
    let failure = adx_core::probes::ProbeFailure {
        kind: adx_core::probes::ProbeFailureKind::HealthcheckFailed,
        message: "unhealthy".into(),
        consecutive_failures: 3,
        exit_code: Some(7),
        http_status: None,
    };
    *deps.probe_failure.lock().unwrap() = Some(failure.clone());
    let mut stale = identity.clone();
    stale.runtime_id = "probe-1-r999".into();
    manager.observe_runtime_event(&stale).await.unwrap();
    assert!(deps.actual.load(std::sync::atomic::Ordering::SeqCst));
    manager.observe_runtime_event(&identity).await.unwrap();
    manager.observe_runtime_event(&identity).await.unwrap();
    let record = environment.sync().await.unwrap().record;
    assert_eq!(record.state, EnvironmentState::Failed);
    assert_eq!(record.failure, Some(failure.into()));
    assert_eq!(
        deps.events
            .lock()
            .unwrap()
            .iter()
            .filter(|s| *s == "remove")
            .count(),
        1
    );
    assert_eq!(manager.used(), Resources::default());
}
#[tokio::test]
async fn tick_recovers_healthcheck_failure_without_event() {
    let deps = Dependencies::new(false);
    let manager = node(&deps);
    let environment = manager
        .environment(workload_spec("probe", true), assignment("probe"))
        .unwrap();
    environment.create().await.unwrap();
    *deps.probe_failure.lock().unwrap() = Some(adx_core::probes::ProbeFailure {
        kind: adx_core::probes::ProbeFailureKind::HealthcheckFailed,
        message: "unhealthy".into(),
        consecutive_failures: 3,
        exit_code: None,
        http_status: None,
    });
    environment.tick().await.unwrap();
    assert_eq!(
        environment.sync().await.unwrap().record.state,
        EnvironmentState::Failed
    );
}

#[tokio::test]
async fn healthcheck_failure_uses_existing_cold_restart_policy() {
    let deps = Dependencies::new(false);
    let manager = node(&deps);
    let mut spec = workload_spec("probe-restart", true);
    spec.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 2,
        initial_backoff_seconds: 1,
        max_backoff_seconds: 1,
    });
    let environment = manager
        .environment(spec, assignment("probe-restart"))
        .unwrap();
    let created = environment.create().await.unwrap();
    *deps.probe_failure.lock().unwrap() = Some(adx_core::probes::ProbeFailure {
        kind: adx_core::probes::ProbeFailureKind::HealthcheckFailed,
        message: "unhealthy".into(),
        consecutive_failures: 1,
        exit_code: None,
        http_status: None,
    });
    manager
        .observe_runtime_event(&adxlet::runtime_control::RuntimeControlClient::identity(
            &created.record,
        ))
        .await
        .unwrap();
    let failed = environment.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(failed.restart_pending);
    assert_eq!(failed.restart_attempts, 0);
    assert!(failed.failure.is_some());
    *deps.probe_failure.lock().unwrap() = None;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    environment.tick().await.unwrap();
    let restarted = environment.sync().await.unwrap().record;
    assert_eq!(restarted.state, EnvironmentState::Running);
    assert_eq!(restarted.restart_attempts, 1);
    assert_ne!(restarted.runtime.id, created.record.runtime.id);
    assert!(restarted.failure.is_none());
}

#[tokio::test]
async fn tunnel_rejects_stateless_update_before_changing_backend_or_record() {
    use adx_core::execution::{NetworkAction, NetworkPolicy, TrafficMode, TrafficPolicy};
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let mut value = spec("tunnel-network");
    value.execution.ports = vec![8080];
    value.execution.tunnel_http_port = Some(8766);
    let environment = node
        .environment(value, assignment("tunnel-network"))
        .unwrap();
    let created = environment.create().await.unwrap();
    deps.events.lock().unwrap().clear();
    let policy = NetworkPolicy {
        traffic: Some(TrafficPolicy {
            ingress_default_action: NetworkAction::Deny,
            egress_default_action: NetworkAction::Deny,
            rules: vec![],
            mode: TrafficMode::Stateless,
        }),
        dns: None,
    };
    assert!(matches!(
        environment
            .update_network_policy(Some(policy), "p".repeat(128), created.record.revision)
            .await,
        Err(Error::Invalid(_))
    ));
    assert!(deps.events.lock().unwrap().is_empty());
    assert_eq!(environment.sync().await.unwrap().record, created.record);
}

#[tokio::test]
async fn delete_publishes_execution_intent_before_cleanup_and_aborts_if_unconfirmed() {
    let deps = Dependencies::new(false);
    let node = node(&deps);
    let environment = node
        .environment(spec("intent"), assignment("intent"))
        .unwrap();
    let running = environment.create().await.unwrap().record;
    *deps.fail_at.lock().unwrap() = Some("commit:Running".into());
    assert!(environment.delete().await.is_err());
    assert!(!deps.events.lock().unwrap().iter().any(|e| e == "remove"));
    assert!(!deps.events.lock().unwrap().iter().any(|e| e == "retire"));
    *deps.fail_at.lock().unwrap() = None;
    *deps.durability.lock().unwrap() = Durability::Journaled;
    assert!(environment.delete().await.is_err());
    assert!(!deps.events.lock().unwrap().iter().any(|e| e == "remove"));
    *deps.durability.lock().unwrap() = Durability::Published;
    let confirmed = environment.sync().await.unwrap().record;
    assert_eq!(confirmed.state, EnvironmentState::Running);
    assert_eq!(confirmed.runtime.id, running.runtime.id);
    assert_eq!(confirmed.observation.as_ref().and_then(|o| o.intent), None);
    environment.delete().await.unwrap();
    let records = deps.records.lock().unwrap();
    let intent = records
        .iter()
        .find(|r| r.observation.as_ref().and_then(|o| o.intent).is_some())
        .unwrap();
    assert_eq!(intent.state, EnvironmentState::Running);
    assert_eq!(intent.runtime.id, running.runtime.id);
    assert_eq!(intent.assignment, running.assignment);
    assert_eq!(
        intent.observation.as_ref().unwrap().intent,
        Some(adx_core::observation::OperationIntent::User)
    );
}
