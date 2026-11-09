use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error, Resources, Result,
    UnsupportedCapability,
};
use adxlet::{
    checkpoint::{CheckpointCooperation, LocalCheckpointStore, PauseRequest, ResumeRequest},
    Adxlet, Durability, Readiness, Routes, RuntimeDriver, StateSink,
};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

#[path = "pause_resume/publication_budget.rs"]
mod publication_budget;
#[path = "pause_resume/upgrade_idle.rs"]
mod upgrade_idle;

#[derive(Default)]
struct Backend {
    journal_after_capture: Mutex<bool>,
    snapshot_journal_failures: Mutex<usize>,
    snapshot_hold_error: Mutex<Option<Error>>,
    rejected_snapshot_records: Mutex<Vec<EnvironmentRecord>>,
    checkpoint_cleanups: Mutex<Vec<adx_core::CheckpointCleanup>>,
    execution_admission_error: Mutex<Option<Error>>,
    execution_admission_delay: Mutex<Duration>,
    sampled_runtimes: Mutex<BTreeSet<String>>,
    checkpoint_commit_failures: Mutex<usize>,
    workload_ack_failures: Mutex<usize>,
    prepare_ids: Mutex<Vec<String>>,
    prepare_gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
    prepare_entered: tokio::sync::Notify,
    checkpoint_gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
    checkpoint_entered: tokio::sync::Notify,
    running_checkpoint_delay: Mutex<Duration>,
    snapshots: Mutex<BTreeMap<String, adx_core::snapshots::Snapshot>>,
    committed_records: Mutex<BTreeMap<String, EnvironmentRecord>>,
    fail_snapshot_publish: Mutex<bool>,
    snapshot_publish_gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
    snapshot_publish_entered: tokio::sync::Notify,
    running: Mutex<BTreeSet<String>>,
    events: Mutex<Vec<String>>,
    unavailable_commit: Mutex<bool>,
    unavailable_state: Mutex<Option<EnvironmentState>>,
    fail_restore: Mutex<bool>,
    restore_delay: Mutex<Duration>,
    unsupported_checkpoint: Mutex<bool>,
    fail_start: Mutex<bool>,
    activity: Mutex<Option<(u64, u64)>>,
    proxy_activity: Mutex<(u64, u64, bool)>,
    idle_race: Mutex<bool>,
    recovery_profile: Mutex<Option<BTreeMap<String, String>>>,
    fail_prepare: Mutex<bool>,
    fail_abort: Mutex<bool>,
    fail_remove: Mutex<bool>,
    remove_then_timeout: Mutex<bool>,
    observed_failure: Mutex<Option<adx_core::RuntimeFailure>>,
    probe_failure: Mutex<Option<adx_core::probes::ProbeFailure>>,
    checkpoint_leaves_running: Mutex<bool>,
    workload_request: Mutex<Option<String>>,
    workload_deadline_unix_millis: Mutex<Option<u64>>,
    workload_checkpoint_timeouts: Mutex<Vec<Duration>>,
    fail_workload_checkpoint: Mutex<bool>,
    workload_prepared: Mutex<bool>,
    background_restore: Mutex<bool>,
}
impl Backend {
    fn event(&self, e: &str) {
        self.events.lock().unwrap().push(e.into());
    }
    async fn collect_deleted(&self, node: &Adxlet, artifact: &adx_core::CheckpointArtifact) {
        let tasks = std::mem::take(&mut *self.checkpoint_cleanups.lock().unwrap());
        assert!(tasks.iter().any(|task| task.artifacts.contains(artifact)));
        for task in tasks {
            node.collect_checkpoint_cleanup(&task, false).await.unwrap();
        }
    }
}
#[async_trait]
impl RuntimeDriver for Backend {
    fn recovery_profile(&self, _: &EnvironmentSpec) -> Result<BTreeMap<String, String>> {
        Ok(self
            .recovery_profile
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| BTreeMap::from([("runtime".into(), "test".into())])))
    }
    async fn stats(&self, id: &str) -> Result<adxlet::metrics::RuntimeUsage> {
        self.sampled_runtimes.lock().unwrap().insert(id.into());
        Ok(adxlet::metrics::RuntimeUsage {
            cpu_usage_ns: 1_000_000,
            memory_usage_bytes: 100,
            memory_limit_bytes: 1024,
        })
    }
    fn retains_checkpoint_files(&self, runtime_class: &str) -> bool {
        *self.background_restore.lock().unwrap() || runtime_class != "runsc"
    }
    fn retains_capture_files(&self, runtime_class: &str) -> bool {
        runtime_class != "runsc"
    }
    async fn inventory(&self) -> Result<Vec<adxlet::RuntimeObservation>> {
        Ok(self
            .running
            .lock()
            .unwrap()
            .iter()
            .map(|id| adxlet::RuntimeObservation {
                environment_id: "environment".into(),
                tenant_id: "tenant".into(),
                generation: 1,
                runtime_id: id.clone(),
                running: true,
            })
            .collect())
    }
    async fn start(
        &self,
        _: &EnvironmentSpec,
        id: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        self.event("start");
        if *self.fail_start.lock().unwrap() {
            return Err(Error::Unavailable("start rejected".into()));
        }
        self.running.lock().unwrap().insert(id.into());
        Ok("10.0.0.2".parse().unwrap())
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        Ok(self.running.lock().unwrap().contains(id))
    }
    async fn observe(&self, id: &str) -> Result<adxlet::RuntimeStatus> {
        Ok(adxlet::RuntimeStatus {
            running: self.running.lock().unwrap().contains(id),
            failure: self.observed_failure.lock().unwrap().clone(),
        })
    }
    async fn remove(&self, id: &str) -> Result<()> {
        self.event("remove");
        if *self.fail_remove.lock().unwrap() {
            return Err(Error::Unavailable("delete outcome unknown".into()));
        }
        self.running.lock().unwrap().remove(id);
        if *self.remove_then_timeout.lock().unwrap() {
            return Err(Error::Unavailable("remove succeeded but reply lost".into()));
        }
        Ok(())
    }
    async fn checkpoint_supported(&self, _: &str) -> Result<()> {
        if *self.unsupported_checkpoint.lock().unwrap() {
            return Err(Error::UnsupportedCapability(
                UnsupportedCapability::CheckpointRestore,
            ));
        }
        Ok(())
    }
    async fn checkpoint(&self, id: &str, path: &Path, _: Duration) -> Result<()> {
        self.event("checkpoint");
        self.checkpoint_entered.notify_one();
        let gate = self.checkpoint_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.notified().await;
        }
        std::fs::write(path.join("memory"), b"actual saved memory").unwrap();
        if !*self.checkpoint_leaves_running.lock().unwrap() {
            self.running.lock().unwrap().remove(id);
        }
        Ok(())
    }
    async fn checkpoint_running(&self, id: &str, path: &Path, timeout: Duration) -> Result<()> {
        self.event("checkpoint-running");
        let delay = *self.running_checkpoint_delay.lock().unwrap();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        self.workload_checkpoint_timeouts
            .lock()
            .unwrap()
            .push(timeout);
        assert!(self.running.lock().unwrap().contains(id));
        if *self.fail_workload_checkpoint.lock().unwrap() {
            return Err(Error::Unavailable("reply lost".into()));
        }
        std::fs::write(path.join("memory"), b"actual saved memory").unwrap();
        if *self.journal_after_capture.lock().unwrap() {
            *self.unavailable_commit.lock().unwrap() = true;
        }
        Ok(())
    }
    async fn restore_from(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[adx_core::scheduling::DeviceAllocation],
        path: &Path,
        origin: Option<&adx_core::runtime::RuntimeIdentity>,
    ) -> Result<std::net::IpAddr> {
        if let Some(origin) = origin {
            assert_eq!(origin.environment_id, "environment");
            if spec.id == origin.environment_id {
                assert!(generation > origin.ownership_generation);
            } else {
                assert_eq!(spec.id, "clone");
            }
        }
        self.restore(spec, id, generation, devices, path).await
    }
    async fn restore(
        &self,
        _: &EnvironmentSpec,
        id: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
        path: &Path,
    ) -> Result<std::net::IpAddr> {
        self.event("restore");
        let delay = *self.restore_delay.lock().unwrap();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if *self.fail_restore.lock().unwrap() {
            return Err(Error::Unavailable("restore failed".into()));
        }
        assert_eq!(
            std::fs::read(path.join("memory")).unwrap(),
            b"actual saved memory"
        );
        self.running.lock().unwrap().insert(id.into());
        Ok("10.0.0.3".parse().unwrap())
    }
}
#[async_trait]
impl CheckpointCooperation for Backend {
    async fn prepare_idle(
        &self,
        record: &EnvironmentRecord,
        id: &str,
        revision: u64,
    ) -> Result<()> {
        if *self.activity.lock().unwrap() != Some((revision, 0)) {
            return Err(Error::Conflict);
        }
        self.prepare(record, id).await
    }
    async fn workload_status(
        &self,
        record: &EnvironmentRecord,
    ) -> Result<Option<adx_core::runtime::RuntimeStatus>> {
        Ok(self.workload_request.lock().unwrap().clone().map(|id| {
            adx_core::runtime::RuntimeStatus {
                required_services: Default::default(),
                failure: None,
                identity: adxlet::runtime_control::RuntimeControlClient::identity(record),
                revision: 1,
                phase: adx_core::runtime::RuntimePhase::Running,
                checkpoint: self.workload_prepared.lock().unwrap().then(|| {
                    adx_core::runtime::CheckpointStatus {
                        operation_id: id.clone(),
                        phase: adx_core::runtime::CheckpointPhase::Prepared,
                        error: None,
                    }
                }),
                requested_checkpoint: Some(id),
                requested_checkpoint_deadline_unix_millis: *self
                    .workload_deadline_unix_millis
                    .lock()
                    .unwrap(),
                active_requests: 1,
                active_commands: 0,
                activity_revision: 1,
            }
        }))
    }
    async fn resumed(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        self.event("handoff");
        Ok(())
    }
    async fn finish_workload(
        &self,
        _: &EnvironmentRecord,
        _: &str,
        error: Option<String>,
    ) -> Result<()> {
        if error.is_none() {
            let mut failures = self.workload_ack_failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                self.event("checkpoint-ack-retry");
                return Err(Error::Unavailable("transient ACK failure".into()));
            }
        }
        self.event(if error.is_none() {
            "checkpoint-ack"
        } else {
            "checkpoint-error"
        });
        self.workload_request.lock().unwrap().take();
        Ok(())
    }
    async fn prepare(&self, _: &EnvironmentRecord, operation_id: &str) -> Result<()> {
        self.prepare_ids.lock().unwrap().push(operation_id.into());
        self.event("prepare");
        self.prepare_entered.notify_one();
        let gate = self.prepare_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.notified().await;
        }
        if *self.fail_prepare.lock().unwrap() {
            return Err(Error::Unavailable("prepare reply lost".into()));
        }
        Ok(())
    }
    async fn abort_unstarted(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        self.event("abort");
        if *self.fail_abort.lock().unwrap() {
            return Err(Error::Unavailable("abort unavailable".into()));
        }
        Ok(())
    }
}
#[async_trait]
impl Readiness for Backend {
    async fn probe_failure(
        &self,
        _: &EnvironmentRecord,
    ) -> Result<Option<adx_core::probes::ProbeFailure>> {
        Ok(self.probe_failure.lock().unwrap().clone())
    }
    async fn activity(&self, _: &EnvironmentRecord) -> Result<(u64, u64)> {
        self.activity
            .lock()
            .unwrap()
            .ok_or_else(|| Error::Unavailable("activity unknown".into()))
    }
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        self.event("ready");
        Ok(())
    }
}
#[async_trait]
impl Routes for Backend {
    async fn drain(&self, _: &EnvironmentRecord) -> Result<()> {
        self.proxy_activity.lock().unwrap().2 = true;
        loop {
            if self.proxy_activity.lock().unwrap().1 == 0 {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn activity(&self, _: &EnvironmentRecord) -> Result<(String, u64, u64)> {
        let activity = self.proxy_activity.lock().unwrap();
        Ok(("proxy".into(), activity.0 + 1, activity.1))
    }

    async fn quiesce(&self, _: &EnvironmentRecord, session: &str, revision: u64) -> Result<bool> {
        let mut activity = self.proxy_activity.lock().unwrap();
        if session != "proxy" || revision != activity.0 + 1 || activity.1 != 0 {
            return Ok(false);
        }
        activity.2 = true;
        if std::mem::take(&mut *self.idle_race.lock().unwrap()) {
            let mut runtime = self.activity.lock().unwrap();
            let revision = runtime.unwrap().0;
            *runtime = Some((revision + 1, 0));
        }
        Ok(true)
    }

    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        self.proxy_activity.lock().unwrap().2 = false;
        self.event("activate");
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        self.event("retire");
        Ok(())
    }
}
#[async_trait]
impl StateSink for Backend {
    async fn prepare_execution(
        &self,
        record: &EnvironmentRecord,
        _: &str,
    ) -> Result<EnvironmentSpec> {
        let delay = *self.execution_admission_delay.lock().unwrap();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if let Some(error) = self.execution_admission_error.lock().unwrap().clone() {
            return Err(error);
        }
        Ok(record.spec.clone())
    }
    async fn commit(&self, r: &EnvironmentRecord) -> Result<Durability> {
        self.event(&format!("commit:{:?}", r.state));
        if r.state == EnvironmentState::Running
            && r.last_operation.is_none()
            && r.execution_artifacts.iter().any(|a| a.size_bytes > 0)
        {
            if let Some(error) = self.snapshot_hold_error.lock().unwrap().clone() {
                self.rejected_snapshot_records
                    .lock()
                    .unwrap()
                    .push(r.clone());
                return Err(error);
            }
            let mut remaining = self.snapshot_journal_failures.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                self.rejected_snapshot_records
                    .lock()
                    .unwrap()
                    .push(r.clone());
                return Err(Error::Unavailable(
                    "concurrent environment commit; retry request".into(),
                ));
            }
        }
        if r.checkpoint.is_some() {
            let mut failures = self.checkpoint_commit_failures.lock().unwrap();
            if *failures > 0 {
                *failures -= 1;
                self.event("checkpoint-result-retry");
                return Err(Error::Unavailable("transient result commit failure".into()));
            }
        }
        if *self.unavailable_commit.lock().unwrap()
            && self
                .unavailable_state
                .lock()
                .unwrap()
                .is_none_or(|state| state == r.state)
        {
            Err(Error::Unavailable("Coordinator unavailable".into()))
        } else {
            self.committed_records
                .lock()
                .unwrap()
                .insert(r.spec.id.clone(), r.clone());
            Ok(Durability::Published)
        }
    }
    async fn commit_deleted(
        &self,
        record: &EnvironmentRecord,
        artifacts: &[adx_core::CheckpointArtifact],
        _: &[String],
    ) -> Result<Durability> {
        let durability = self.commit(record).await?;
        if durability == Durability::Published && !artifacts.is_empty() {
            let task = adx_core::CheckpointCleanup {
                assignment: record.assignment.clone(),
                revision: record.revision,
                artifacts: artifacts.to_vec(),
            };
            task.validate()?;
            self.checkpoint_cleanups.lock().unwrap().push(task);
        }
        Ok(durability)
    }
}
fn fixture() -> (
    tempfile::TempDir,
    Arc<Backend>,
    Adxlet,
    EnvironmentSpec,
    Assignment,
) {
    let temp = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let resources = Resources {
        cpu_millis: 100,
        memory_bytes: 1024,
        disk_bytes: 1024,
    };
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(
        Arc::new(LocalCheckpointStore::new(temp.path().into()).unwrap()),
        backend.clone(),
    )
    .unwrap()
    .with_snapshot_catalog(backend.clone())
    .unwrap();
    node.update_capacity(resources, Duration::from_secs(300))
        .unwrap();
    let spec = EnvironmentSpec {
        envd: None,
        run_policy: None,
        compatibility: None,
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        id: "environment".into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime_class: "firecracker".into(),
        resources,
        priority: 0,
        env: Default::default(),
        scheduling: Default::default(),
        execution: Default::default(),
    };
    let assignment = Assignment {
        environment_id: spec.id.clone(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    (temp, backend, node, spec, assignment)
}
fn pause(revision: u64) -> PauseRequest {
    PauseRequest {
        operation_id: "pause-a".into(),
        expected_revision: revision,
        retention: adx_core::lifecycle::RetentionPolicy::Ttl { ttl_seconds: 600 },
        timeout_seconds: 60,
    }
}
fn resume(revision: u64) -> ResumeRequest {
    ResumeRequest {
        run_timeout_seconds: None,
        operation_id: "resume-b".into(),
        expected_revision: revision,
    }
}

#[tokio::test]
async fn replacing_checkpoint_cleans_previous_only_after_retry_publishes() {
    let (temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    let first = paused
        .record
        .checkpoint
        .as_ref()
        .unwrap()
        .artifact
        .location
        .clone();
    let running = h.resume(resume(paused.record.revision)).await.unwrap();
    let mut request = pause(running.record.revision);
    request.operation_id = "pause-c".into();
    *backend.unavailable_state.lock().unwrap() = Some(EnvironmentState::Paused);
    *backend.unavailable_commit.lock().unwrap() = true;
    assert!(h.pause(request.clone()).await.is_err());
    assert!(Path::new(&first).exists());
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 2);
    *backend.unavailable_commit.lock().unwrap() = false;
    h.pause(request).await.unwrap();
    assert!(!Path::new(&first).exists());
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn restarted_node_prunes_only_after_authoritative_reconciliation() {
    let (temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    let retained = paused
        .record
        .checkpoint
        .as_ref()
        .unwrap()
        .artifact
        .location
        .clone();
    let orphan = temp.path().join(uuid::Uuid::new_v4().to_string());
    std::fs::create_dir(&orphan).unwrap();
    std::fs::write(orphan.join("memory"), b"unpublished").unwrap();
    let foreign = temp.path().join("operator-files");
    std::fs::create_dir(&foreign).unwrap();
    let restored = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(
        Arc::new(LocalCheckpointStore::new(temp.path().into()).unwrap()),
        backend.clone(),
    )
    .unwrap();
    *backend.unavailable_commit.lock().unwrap() = true;
    assert!(restored
        .reconcile(vec![paused.record.clone()])
        .await
        .is_err());
    assert!(orphan.exists());
    *backend.unavailable_commit.lock().unwrap() = false;
    restored.reconcile(vec![paused.record]).await.unwrap();
    assert!(!orphan.exists());
    assert!(Path::new(&retained).exists());
    assert!(foreign.exists());
}

#[tokio::test]
async fn pause_resume_preserves_environment_changes_execution_and_fences_delayed_retries() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec.clone(), assignment).unwrap();
    let created = h.create().await.unwrap();
    h.tick().await.unwrap();
    assert!(node
        .environment_metrics()
        .contains("adx_environment_memory_usage_bytes"));
    backend.events.lock().unwrap().clear();
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    assert_eq!(paused.record.state, EnvironmentState::Paused);
    assert!(!node
        .environment_metrics()
        .contains("adx_environment_memory_usage_bytes"));
    assert!(!paused.record.resources_held);
    assert_eq!(node.used(), Resources::default());
    assert!(
        paused
            .record
            .checkpoint
            .as_ref()
            .unwrap()
            .artifact
            .size_bytes
            > 0
    );
    assert_eq!(
        *backend.events.lock().unwrap(),
        [
            "prepare",
            "commit:Running",
            "retire",
            "checkpoint",
            "remove",
            "commit:Paused"
        ]
    );
    assert_eq!(
        h.pause(pause(created.record.revision)).await.unwrap(),
        paused
    );
    let running = h.resume(resume(paused.record.revision)).await.unwrap();
    assert_eq!(running.record.state, EnvironmentState::Running);
    assert_eq!(running.record.spec.id, created.record.spec.id);
    assert_ne!(running.record.runtime.id, created.record.runtime.id);
    assert_eq!(node.used(), spec.resources);
    assert_eq!(
        &backend.events.lock().unwrap()[6..],
        [
            "commit:Paused",
            "restore",
            "ready",
            "activate",
            "commit:Running"
        ]
    );
    assert_eq!(
        h.resume(resume(paused.record.revision)).await.unwrap(),
        running
    );
    assert_eq!(
        h.pause(pause(created.record.revision)).await.unwrap_err(),
        Error::Conflict
    );
    h.delete().await.unwrap();
    assert_eq!(
        h.resume(resume(paused.record.revision)).await.unwrap_err(),
        Error::Conflict
    );
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn unpublished_pause_is_retried_by_commit_without_repeating_checkpoint() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    *backend.unavailable_state.lock().unwrap() = Some(EnvironmentState::Paused);
    *backend.unavailable_commit.lock().unwrap() = true;
    assert!(h.pause(pause(created.record.revision)).await.is_err());
    assert!(!backend
        .running
        .lock()
        .unwrap()
        .contains(&created.record.runtime.id));
    *backend.unavailable_commit.lock().unwrap() = false;
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    assert_eq!(paused.record.state, EnvironmentState::Paused);
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == "checkpoint")
            .count(),
        1
    );
}

#[tokio::test]
async fn failed_restore_keeps_restore_point_and_releases_confirmed_absent_execution() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    *backend.fail_restore.lock().unwrap() = true;
    assert!(h.resume(resume(paused.record.revision)).await.is_err());
    let after = h.sync().await.unwrap();
    assert_eq!(after.record.state, EnvironmentState::Paused);
    assert_eq!(after.record.checkpoint, paused.record.checkpoint);
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn expiry_cleans_paused_environment_but_never_deletes_resumed_execution() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    let cp = paused.record.checkpoint.clone().unwrap();
    let running = h.resume(resume(paused.record.revision)).await.unwrap();
    h.expire_checkpoint(cp.expires_at_unix_seconds.unwrap())
        .await
        .unwrap();
    assert!(backend
        .running
        .lock()
        .unwrap()
        .contains(&running.record.runtime.id));
    assert!(Path::new(&cp.artifact.location).exists());
    let r = h.sync().await.unwrap();
    assert_eq!(r.record.state, EnvironmentState::Running);
    assert!(r.record.checkpoint.is_none());
    assert!(r.record.execution_artifacts.contains(&cp.artifact));
    let mut request = pause(r.record.revision);
    request.operation_id = "pause-c".into();
    let paused = h.pause(request).await.unwrap();
    h.expire_checkpoint(
        paused
            .record
            .checkpoint
            .unwrap()
            .expires_at_unix_seconds
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Deleted
    );
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn permanent_pause_survives_expiry_and_reconcile_then_resumes_and_deletes() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    let mut request = pause(created.record.revision);
    request.retention = adx_core::lifecycle::RetentionPolicy::Forever;
    let paused = h.pause(request).await.unwrap();
    let cp = paused.record.checkpoint.clone().unwrap();
    assert_eq!(cp.expires_at_unix_seconds, None);
    assert_eq!(h.expire_checkpoint(u64::MAX).await.unwrap(), None);
    node.reconcile(vec![paused.record]).await.unwrap();
    let current = h.sync().await.unwrap();
    assert_eq!(current.record.state, EnvironmentState::Paused);
    assert!(Path::new(&cp.artifact.location).exists());
    let running = h.resume(resume(current.record.revision)).await.unwrap();
    assert!(backend
        .running
        .lock()
        .unwrap()
        .contains(&running.record.runtime.id));
    h.delete().await.unwrap();
    assert!(Path::new(&cp.artifact.location).exists());
    backend.collect_deleted(&node, &cp.artifact).await;
    assert!(!Path::new(&cp.artifact.location).exists());
}

#[tokio::test]
async fn failed_prepare_and_abort_retire_route_and_keep_uncertain_capacity() {
    let (temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec.clone(), assignment).unwrap();
    let created = h.create().await.unwrap();
    *backend.fail_prepare.lock().unwrap() = true;
    *backend.fail_abort.lock().unwrap() = true;
    assert!(h.pause(pause(created.record.revision)).await.is_err());
    let state = h.sync().await.unwrap();
    assert_eq!(state.record.state, EnvironmentState::Failed);
    assert_eq!(node.used(), spec.resources);
    assert!(backend.events.lock().unwrap().iter().any(|s| s == "retire"));
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}
#[tokio::test]
async fn restore_cleanup_failure_keeps_resources_and_invalidates_previous_operation() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec.clone(), assignment).unwrap();
    let created = h.create().await.unwrap();
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    *backend.fail_restore.lock().unwrap() = true;
    *backend.fail_remove.lock().unwrap() = true;
    assert!(h.resume(resume(paused.record.revision)).await.is_err());
    let state = h.sync().await.unwrap();
    assert_eq!(state.record.state, EnvironmentState::Failed);
    assert_eq!(node.used(), spec.resources);
    assert!(state.record.last_operation.is_none());
    assert_eq!(
        h.pause(pause(created.record.revision)).await.unwrap_err(),
        Error::Conflict
    );
}

struct UnavailablePublication(LocalCheckpointStore);
#[async_trait]
impl adxlet::checkpoint::CheckpointAccess for UnavailablePublication {
    fn storage_alias(&self) -> &str {
        "obs"
    }
    async fn begin_capture(&self) -> Result<std::path::PathBuf> {
        self.0.begin_capture().await
    }
    async fn discard_staged(&self, p: &Path) -> Result<()> {
        self.0.discard_staged(p).await
    }
    async fn retain_staged(&self, p: &Path) -> Result<adx_core::CheckpointArtifact> {
        self.0.seal(p).await
    }
    async fn seal(&self, _: &Path) -> Result<adx_core::CheckpointArtifact> {
        Err(Error::Unavailable("storage unavailable".into()))
    }
    async fn prepare_restore(
        &self,
        a: &adx_core::CheckpointArtifact,
    ) -> Result<adxlet::checkpoint::RestoreAccess> {
        self.0.prepare_restore(a).await
    }
    async fn release(&self, a: &adx_core::CheckpointArtifact) -> Result<()> {
        self.0.release(a).await
    }
}

struct SharedPublication {
    local: LocalCheckpointStore,
    backend: Arc<Backend>,
}
#[async_trait]
impl adxlet::checkpoint::CheckpointAccess for SharedPublication {
    fn storage_alias(&self) -> &str {
        "shared"
    }
    async fn begin_capture(&self) -> Result<std::path::PathBuf> {
        self.local.begin_capture().await
    }
    async fn discard_staged(&self, path: &Path) -> Result<()> {
        self.local.discard_staged(path).await
    }
    async fn retain_staged(&self, path: &Path) -> Result<adx_core::CheckpointArtifact> {
        self.local.seal(path).await
    }
    async fn seal(&self, path: &Path) -> Result<adx_core::CheckpointArtifact> {
        self.backend.event("checkpoint-publish");
        let mut artifact = self.local.seal(path).await?;
        artifact.storage = "shared".into();
        Ok(artifact)
    }
    async fn prepare_restore(
        &self,
        artifact: &adx_core::CheckpointArtifact,
    ) -> Result<adxlet::checkpoint::RestoreAccess> {
        let mut local = artifact.clone();
        local.storage = "local".into();
        self.local.prepare_restore(&local).await
    }
    async fn release(&self, artifact: &adx_core::CheckpointArtifact) -> Result<()> {
        let mut local = artifact.clone();
        local.storage = "local".into();
        self.local.release(&local).await
    }
    async fn committed(&self, _: &adx_core::CheckpointArtifact) -> Result<()> {
        self.backend.event("checkpoint-store-committed");
        Ok(())
    }
}
#[tokio::test]
async fn failed_publication_restores_local_staging_and_reports_pause_failure() {
    let (temp, backend, _node, mut spec, assignment) = fixture();
    spec.lifecycle.pause = Some(adx_core::lifecycle::PausePolicy {
        storage: adx_core::lifecycle::CheckpointStorage::Shared,
        ..Default::default()
    });
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(
        Arc::new(UnavailablePublication(
            LocalCheckpointStore::new(temp.path().into()).unwrap(),
        )),
        backend.clone(),
    )
    .unwrap();
    node.update_capacity(spec.resources, Duration::from_secs(300))
        .unwrap();
    let h = node.environment(spec.clone(), assignment).unwrap();
    let created = h.create().await.unwrap();
    assert!(h.pause(pause(created.record.revision)).await.is_err());
    let after = h.sync().await.unwrap();
    assert_eq!(after.record.state, EnvironmentState::Running);
    assert_ne!(after.record.runtime.id, created.record.runtime.id);
    assert_eq!(node.used(), spec.resources);
    let local = after
        .record
        .checkpoint
        .as_ref()
        .expect("rollback must persist its local recovery files");
    assert_eq!(local.artifact.storage, "local");
    assert!(Path::new(&local.artifact.location).exists());
    let artifact = local.artifact.clone();
    h.delete().await.unwrap();
    assert!(Path::new(&artifact.location).exists());
    backend.collect_deleted(&node, &artifact).await;
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[tokio::test(start_paused = true)]
async fn idle_deletion_resets_on_bursts_and_unknown_observations() {
    let (_temp, b, node, mut spec, assignment) = fixture();
    spec.lifecycle.idle_timeout_seconds = 10;
    let h = node.environment(spec, assignment).unwrap();
    h.create().await.unwrap();
    *b.activity.lock().unwrap() = Some((1, 0));
    h.tick().await.unwrap();
    tokio::time::advance(Duration::from_secs(9)).await;
    *b.activity.lock().unwrap() = Some((3, 0)); // a complete request between polls
    h.tick().await.unwrap();
    tokio::time::advance(Duration::from_secs(9)).await;
    *b.activity.lock().unwrap() = None;
    h.tick().await.unwrap();
    tokio::time::advance(Duration::from_secs(20)).await;
    *b.activity.lock().unwrap() = Some((3, 0));
    h.tick().await.unwrap();
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    tokio::time::advance(Duration::from_secs(10)).await;
    node.monitor_environments().await.unwrap();
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Deleted
    );
    assert_eq!(node.used(), Resources::default());
    assert!(node.metrics().contains("adx_node_managed_environments 0\n"));
}

#[tokio::test(start_paused = true)]
async fn unexpected_exit_restarts_with_fresh_identity_and_bounded_backoff() {
    let (_temp, b, node, mut spec, assignment) = fixture();
    spec.lifecycle.restart = Some(adx_core::lifecycle::RestartPolicy {
        max_attempts: 2,
        initial_backoff_seconds: 2,
        max_backoff_seconds: 8,
    });
    let h = node.environment(spec, assignment).unwrap();
    let old = h.create().await.unwrap().record;
    b.running.lock().unwrap().clear();
    h.tick().await.unwrap();
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Failed
    );
    tokio::time::advance(Duration::from_secs(2)).await;
    h.tick().await.unwrap();
    let new = h.sync().await.unwrap().record;
    assert_eq!(new.state, EnvironmentState::Running);
    assert_ne!(new.runtime.id, old.runtime.id);
    assert_eq!(new.restart_attempts, 1);
    b.running.lock().unwrap().clear();
    *b.fail_start.lock().unwrap() = true;
    h.tick().await.unwrap();
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(h.tick().await.is_err());
    assert_eq!(h.sync().await.unwrap().record.restart_attempts, 2);
    let starts = b
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|v| *v == "start")
        .count();
    tokio::time::advance(Duration::from_secs(100)).await;
    h.tick().await.unwrap();
    assert_eq!(
        b.events
            .lock()
            .unwrap()
            .iter()
            .filter(|v| *v == "start")
            .count(),
        starts
    );
}

#[tokio::test]
async fn failover_restores_latest_checkpoint_without_cold_start() {
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.execution.failover = true;
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    let paused = handle.pause(pause(created.record.revision)).await.unwrap();
    let running = handle.resume(resume(paused.record.revision)).await.unwrap();
    let starts = backend
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| *event == "start")
        .count();
    let restores = backend
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| *event == "restore")
        .count();
    backend.running.lock().unwrap().clear();
    handle.tick().await.unwrap();
    let recovered = handle.sync().await.unwrap().record;
    assert_eq!(recovered.state, EnvironmentState::Running);
    assert_ne!(recovered.runtime.id, running.record.runtime.id);
    let evidence = recovered
        .observation
        .as_ref()
        .unwrap()
        .interruption
        .as_ref()
        .unwrap();
    assert_eq!(evidence.runtime_id, running.record.runtime.id);
    assert_eq!(
        evidence.assignment_generation,
        running.record.assignment.generation
    );
    assert_eq!(
        backend.committed_records.lock().unwrap()[&recovered.spec.id].observation,
        recovered.observation
    );
    assert_eq!(recovered.restart_attempts, running.record.restart_attempts);
    let events = backend.events.lock().unwrap();
    assert_eq!(
        events.iter().filter(|event| *event == "start").count(),
        starts
    );
    assert_eq!(
        events.iter().filter(|event| *event == "restore").count(),
        restores + 1
    );
}

#[tokio::test]
async fn failover_without_checkpoint_fails_without_cold_start() {
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.execution.failover = true;
    let handle = node.environment(spec, assignment).unwrap();
    handle.create().await.unwrap();
    backend.running.lock().unwrap().clear();
    assert!(handle.tick().await.is_err());
    let failed = handle.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(!failed.restart_pending);
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| *event == "start")
            .count(),
        1
    );
}

#[tokio::test]
async fn reload_replaces_runtime_from_checkpoint_and_replays_result() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    let paused = handle.pause(pause(created.record.revision)).await.unwrap();
    let running = handle.resume(resume(paused.record.revision)).await.unwrap();
    let restores = backend
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| *event == "restore")
        .count();
    *backend.unavailable_commit.lock().unwrap() = true;
    assert!(handle
        .reload("reload-a".into(), running.record.revision)
        .await
        .is_err());
    assert!(backend
        .running
        .lock()
        .unwrap()
        .contains(&running.record.runtime.id));
    *backend.unavailable_commit.lock().unwrap() = false;
    let reloaded = handle
        .reload("reload-a".into(), running.record.revision)
        .await
        .unwrap();
    assert_eq!(reloaded.record.state, EnvironmentState::Running);
    assert_ne!(reloaded.record.runtime.id, running.record.runtime.id);
    assert_eq!(
        reloaded.record.restart_attempts,
        running.record.restart_attempts
    );
    assert_eq!(
        reloaded.record.last_operation.as_ref().unwrap().kind,
        adx_core::LifecycleKind::Reload
    );
    let replay = handle
        .reload("reload-a".into(), running.record.revision)
        .await
        .unwrap();
    assert_eq!(replay.record, reloaded.record);
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| *event == "restore")
            .count(),
        restores + 1
    );
}

#[tokio::test]
async fn optional_runtime_health_respects_failure_tolerance() {
    let (_temp, b, node, spec, assignment) = fixture();
    let node = node.with_health_check(Some(2)).unwrap();
    let h = node.environment(spec, assignment).unwrap();
    h.create().await.unwrap();
    h.tick().await.unwrap();
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    *b.activity.lock().unwrap() = Some((1, 0));
    h.tick().await.unwrap(); // healthy resets consecutive failures
    *b.activity.lock().unwrap() = None;
    h.tick().await.unwrap();
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    h.tick().await.unwrap();
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Failed
    );
    assert!(b.running.lock().unwrap().is_empty());
}

#[tokio::test]
async fn pause_confirms_cleanup_when_checkpoint_reply_precedes_source_exit() {
    let (_temp, backend, node, spec, assignment) = fixture();
    *backend.checkpoint_leaves_running.lock().unwrap() = true;
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    assert_eq!(paused.record.state, EnvironmentState::Paused);
    assert!(!paused.record.resources_held);
    assert!(backend.running.lock().unwrap().is_empty());
    let events = backend.events.lock().unwrap();
    let saved = events.iter().position(|e| e == "checkpoint").unwrap();
    let removed = events.iter().position(|e| e == "remove").unwrap();
    assert!(saved < removed);
}

#[tokio::test]
async fn authoritative_paused_record_with_missing_local_artifact_becomes_failed() {
    let (temp, backend, node, spec, assignment) = fixture();
    let handle = node.environment(spec.clone(), assignment.clone()).unwrap();
    let running = handle.create().await.unwrap();
    let paused = handle.pause(pause(running.record.revision)).await.unwrap();
    std::fs::remove_dir_all(&paused.record.checkpoint.as_ref().unwrap().artifact.location).unwrap();
    let restarted = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(
        Arc::new(LocalCheckpointStore::new(temp.path().into()).unwrap()),
        backend.clone(),
    )
    .unwrap();
    restarted.reconcile(vec![paused.record]).await.unwrap();
    let result = restarted
        .environment(spec, assignment)
        .unwrap()
        .sync()
        .await
        .unwrap();
    assert_eq!(result.record.state, EnvironmentState::Failed);
    assert!(!result.record.resources_held);
    assert!(backend.running.lock().unwrap().is_empty());
}

#[tokio::test]
async fn reconciliation_preserves_ready_and_deleting_reusable_snapshots() {
    use adx_core::snapshots::{Reference, Snapshot};
    use adxlet::checkpoint::CheckpointAccess;
    let (temp, _, node, spec, _) = fixture();
    let store = LocalCheckpointStore::new(temp.path().into()).unwrap();
    let path = store.begin_capture().await.unwrap();
    std::fs::write(path.join("memory"), b"snapshot memory").unwrap();
    let artifact = store.seal(&path).await.unwrap();
    let mut snapshot = Snapshot::new(
        "saved".into(),
        vec![],
        spec,
        "node".into(),
        "environment-1".into(),
        artifact,
    )
    .unwrap();
    let orphan = store.begin_capture().await.unwrap();
    node.reconcile_catalog(vec![], vec![snapshot.clone()])
        .await
        .unwrap();
    assert!(path.exists());
    assert!(!orphan.exists());
    snapshot
        .acquire(
            "tenant",
            Reference::Restore {
                environment_id: "clone".into(),
            },
        )
        .unwrap();
    assert!(snapshot.delete("tenant").is_err());
    node.reconcile_catalog(vec![], vec![snapshot.clone()])
        .await
        .unwrap();
    assert!(
        path.exists(),
        "deleting snapshot remains while references exist"
    );
    snapshot.source_node_id = "foreign".into();
    assert!(node
        .reconcile_catalog(vec![], vec![snapshot])
        .await
        .is_err());
    assert!(path.exists(), "invalid catalog must not trigger cleanup");
    node.reconcile_catalog(vec![], vec![]).await.unwrap();
    assert!(
        !path.exists(),
        "authoritatively unreferenced artifact is cleaned"
    );
}

#[tokio::test]
async fn snapshot_collection_requires_closed_references_and_is_retryable() {
    use adx_core::snapshots::{Reference, Snapshot};
    use adxlet::checkpoint::CheckpointAccess;
    let (temp, _, node, spec, _) = fixture();
    let store = LocalCheckpointStore::new(temp.path().into()).unwrap();
    let path = store.begin_capture().await.unwrap();
    std::fs::write(path.join("memory"), b"saved memory").unwrap();
    let mut snapshot = Snapshot::new(
        "saved".into(),
        vec![],
        spec,
        "node".into(),
        "environment-1".into(),
        store.seal(&path).await.unwrap(),
    )
    .unwrap();
    assert!(node.collect_snapshot(&snapshot, false).await.is_err());
    let reference = Reference::Restore {
        environment_id: "clone".into(),
    };
    snapshot.acquire("tenant", reference.clone()).unwrap();
    assert!(snapshot.delete("tenant").is_err());
    assert!(node.collect_snapshot(&snapshot, false).await.is_err());
    assert!(path.exists());
    snapshot.release(&reference).unwrap();
    snapshot.delete("tenant").unwrap();
    let mut foreign = snapshot.clone();
    foreign.source_node_id = "other".into();
    assert!(node.collect_snapshot(&foreign, false).await.is_err());
    node.collect_snapshot(&snapshot, false).await.unwrap();
    assert!(!path.exists());
    node.collect_snapshot(&snapshot, false).await.unwrap();
}

#[async_trait]
impl adxlet::checkpoint::SnapshotCatalog for Backend {
    async fn get(&self, id: &str) -> Result<adx_core::snapshots::Snapshot> {
        self.snapshots
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or(Error::NotFound)
    }
    async fn publish(
        &self,
        snapshot: adx_core::snapshots::Snapshot,
        _timeout: Duration,
    ) -> Result<adx_core::snapshots::Snapshot> {
        let gate = self.snapshot_publish_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            self.snapshot_publish_entered.notify_one();
            gate.notified().await;
        }
        if *self.fail_snapshot_publish.lock().unwrap() {
            return Err(Error::Unavailable("catalog unavailable".into()));
        }
        // Match Coordinator's real publication precondition: a source must
        // commit its temporary artifact hold before the catalog accepts it.
        if !self
            .committed_records
            .lock()
            .unwrap()
            .get(&snapshot.template.id)
            .is_some_and(|record| {
                record.runtime.id == snapshot.source_runtime_id
                    && record.execution_artifacts.contains(&snapshot.artifact)
            })
        {
            return Err(Error::Conflict);
        }
        self.snapshots
            .lock()
            .unwrap()
            .insert(snapshot.id.clone(), snapshot.clone());
        Ok(snapshot)
    }
}

#[tokio::test]
async fn reusable_snapshot_survives_source_deletion_and_retries_without_checkpointing() {
    use adxlet::checkpoint::SnapshotRequest;
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    let request = SnapshotRequest {
        kind: adx_core::snapshots::SnapshotKind::Regular,
        operation_id: "snapshot-a".into(),
        expected_revision: created.record.revision,
        names: vec!["base".into()],
        timeout_seconds: 60,
    };
    let saved = h.snapshot(request.clone()).await.unwrap();
    assert_eq!(saved.environment.record.state, EnvironmentState::Running);
    assert_eq!(saved.environment.durability, Durability::Published);
    assert!(saved.environment.record.checkpoint.is_none());
    assert!(saved
        .environment
        .record
        .execution_artifacts
        .contains(&saved.snapshot.artifact));
    let replay = h.snapshot(request.clone()).await.unwrap();
    assert_eq!(saved.snapshot, replay.snapshot);
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.as_str() == "checkpoint-running")
            .count(),
        1
    );
    let mut changed = request.clone();
    changed.names = vec!["changed".into()];
    assert!(h.snapshot(changed).await.is_err());
    h.delete().await.unwrap();
    assert!(Path::new(&saved.snapshot.artifact.location).exists());
    assert!(
        h.snapshot(request).await.is_err(),
        "stale retry must not revive deleted source"
    );
}

fn snapshot_journal_node(
    temp: &tempfile::TempDir,
    backend: &Arc<Backend>,
    resources: Resources,
) -> (Adxlet, Arc<adxlet::journal::JournalSink>) {
    let journal = Arc::new(adxlet::journal::JournalSink::new(
        temp.path().join("pending.sqlite"),
        backend.clone(),
    ));
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        journal.clone(),
    )
    .with_checkpointing(
        Arc::new(LocalCheckpointStore::new(temp.path().into()).unwrap()),
        backend.clone(),
    )
    .unwrap()
    .with_snapshot_catalog(backend.clone())
    .unwrap();
    node.update_capacity(resources, Duration::from_secs(60))
        .unwrap();
    (node, journal)
}

#[tokio::test]
async fn reusable_snapshot_waits_for_journal_publication_without_recapturing() {
    use adxlet::checkpoint::SnapshotRequest;
    let (temp, backend, _, mut spec, assignment) = fixture();
    spec.runtime_class = "runsc".into();
    let (node, journal) = snapshot_journal_node(&temp, &backend, spec.resources);
    let handle = node.environment(spec, assignment).unwrap();
    let running = handle.create().await.unwrap();
    *backend.snapshot_journal_failures.lock().unwrap() = 3;
    let request = SnapshotRequest {
        kind: adx_core::snapshots::SnapshotKind::Regular,
        operation_id: "snapshot-journal-contention".into(),
        expected_revision: running.record.revision,
        names: vec![],
        timeout_seconds: 2,
    };
    let saved = handle.snapshot(request.clone()).await.unwrap();
    assert_eq!(saved.environment.durability, Durability::Published);
    assert_eq!(
        saved.snapshot.state,
        adx_core::snapshots::SnapshotState::Ready
    );
    assert_eq!(journal.pending().await.unwrap(), 0);
    let rejected = backend.rejected_snapshot_records.lock().unwrap().clone();
    assert_eq!(rejected.len(), 3);
    assert!(rejected.iter().all(|r| r == &rejected[0]));
    assert_eq!(
        handle.snapshot(request).await.unwrap().snapshot,
        saved.snapshot
    );
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == "checkpoint-running")
            .count(),
        1,
    );
    handle.delete().await.unwrap();
}

#[tokio::test]
async fn reusable_snapshot_rounds_remaining_capture_budget_to_seconds() {
    use adxlet::checkpoint::SnapshotRequest;
    let (temp, backend, _, mut spec, assignment) = fixture();
    spec.runtime_class = "runsc".into();
    let (node, _) = snapshot_journal_node(&temp, &backend, spec.resources);
    let handle = node.environment(spec, assignment).unwrap();
    let running = handle.create().await.unwrap();
    handle
        .snapshot(SnapshotRequest {
            kind: adx_core::snapshots::SnapshotKind::Regular,
            operation_id: "snapshot-subsecond-budget".into(),
            expected_revision: running.record.revision,
            names: vec![],
            timeout_seconds: 1,
        })
        .await
        .unwrap();
    assert_eq!(
        backend
            .workload_checkpoint_timeouts
            .lock()
            .unwrap()
            .as_slice(),
        &[Duration::from_secs(1)]
    );
    handle.delete().await.unwrap();
}

#[tokio::test]
async fn reusable_snapshot_publication_wait_keeps_the_original_deadline() {
    snapshot_publication_timeout_retry("runsc", false).await;
}

#[tokio::test]
async fn reusable_snapshot_retained_capture_retries_publication_without_recapture() {
    snapshot_publication_timeout_retry("firecracker", false).await;
}

#[tokio::test]
async fn pending_snapshot_publication_defers_workload_checkpoint() {
    snapshot_publication_timeout_retry("runsc", true).await;
}

#[tokio::test]
async fn pending_snapshot_publication_defers_workload_with_retained_capture() {
    snapshot_publication_timeout_retry("firecracker", true).await;
}

async fn snapshot_publication_timeout_retry(runtime_class: &str, workload_pending: bool) {
    use adxlet::checkpoint::SnapshotRequest;
    let (temp, backend, _, mut spec, assignment) = fixture();
    spec.runtime_class = runtime_class.into();
    let (node, journal) = snapshot_journal_node(&temp, &backend, spec.resources);
    let handle = node.environment(spec, assignment).unwrap();
    let running = handle.create().await.unwrap();
    if workload_pending {
        *backend.journal_after_capture.lock().unwrap() = true;
    } else {
        *backend.snapshot_journal_failures.lock().unwrap() = usize::MAX;
    }
    let request = SnapshotRequest {
        kind: adx_core::snapshots::SnapshotKind::Regular,
        operation_id: "snapshot-journal-deadline".into(),
        expected_revision: running.record.revision,
        names: vec![],
        timeout_seconds: 1,
    };
    let error = tokio::time::timeout(Duration::from_secs(3), handle.snapshot(request.clone()))
        .await
        .expect("publication must not reset its deadline")
        .unwrap_err();
    assert!(matches!(error, Error::Unavailable(reason) if reason.contains("timed out")));
    assert!(journal.pending().await.unwrap() > 0);
    assert!(backend.snapshots.lock().unwrap().is_empty());
    assert_eq!(backend.running.lock().unwrap().len(), 1);
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == "checkpoint-running")
            .count(),
        1,
    );
    *backend.snapshot_journal_failures.lock().unwrap() = 0;
    *backend.journal_after_capture.lock().unwrap() = false;
    *backend.unavailable_commit.lock().unwrap() = false;
    let flushed = handle.sync().await.unwrap();
    assert_eq!(flushed.durability, Durability::Published);
    if workload_pending {
        let held = flushed.record.execution_artifacts.clone();
        assert!(!held.is_empty());
        if runtime_class == "runsc" {
            assert!(held.iter().all(|artifact| artifact.size_bytes == 0));
        }
        let events = backend.events.lock().unwrap().clone();
        *backend.workload_request.lock().unwrap() = Some("deferred-workload".into());
        *backend.fail_prepare.lock().unwrap() = true;
        handle.tick().await.unwrap();
        node.observe_runtime_event(&adx_core::runtime::RuntimeIdentity {
            environment_id: flushed.record.spec.id.clone(),
            runtime_id: flushed.record.runtime.id.clone(),
            ownership_generation: flushed.record.assignment.generation,
        })
        .await
        .unwrap();
        assert_eq!(backend.events.lock().unwrap().as_slice(), events);
        let deferred = handle.sync().await.unwrap();
        assert_eq!(deferred.record.revision, flushed.record.revision);
        assert_eq!(deferred.record.execution_artifacts, held);
        assert_eq!(deferred.record.state, EnvironmentState::Running);
        assert_eq!(backend.running.lock().unwrap().len(), 1);
        assert_eq!(
            backend.workload_request.lock().unwrap().as_deref(),
            Some("deferred-workload"),
            "deferred workload must remain pending, without capture, abort or ACK"
        );
        *backend.fail_prepare.lock().unwrap() = false;
    }
    assert_eq!(
        handle
            .pause(PauseRequest {
                operation_id: "cannot-overtake-pending-snapshot".into(),
                expected_revision: flushed.record.revision,
                retention: adx_core::lifecycle::RetentionPolicy::Ttl { ttl_seconds: 60 },
                timeout_seconds: 1,
            })
            .await
            .unwrap_err(),
        Error::Conflict
    );
    let mut changed = request.clone();
    changed.names = vec!["different-capture".into()];
    assert_eq!(handle.snapshot(changed).await.unwrap_err(), Error::Conflict);
    let saved = handle.snapshot(request.clone()).await.unwrap();
    assert_eq!(saved.environment.durability, Durability::Published);
    assert_eq!(
        handle.snapshot(request).await.unwrap().snapshot,
        saved.snapshot
    );
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| *event == "checkpoint-running")
            .count(),
        1,
        "retry must publish the retained capture without recapturing"
    );
    if workload_pending {
        handle.tick().await.unwrap();
        let after = handle.sync().await.unwrap();
        assert_eq!(after.record.state, EnvironmentState::Running);
        assert!(after.record.checkpoint.is_some());
        assert!(backend.workload_request.lock().unwrap().is_none());
        assert_eq!(
            backend
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| *event == "checkpoint-running")
                .count(),
            2,
            "workload checkpoint may proceed only after snapshot publication completes"
        );
        assert_eq!(
            backend.snapshots.lock().unwrap()[&saved.snapshot.id],
            saved.snapshot
        );
    }
    handle.delete().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn reusable_snapshot_budget_expired_in_prepare_preserves_running_source() {
    use adxlet::checkpoint::SnapshotRequest;
    let (_temp, backend, node, spec, assignment) = fixture();
    let handle = node.environment(spec, assignment).unwrap();
    let running = handle.create().await.unwrap();
    let gate = Arc::new(tokio::sync::Notify::new());
    *backend.prepare_gate.lock().unwrap() = Some(gate.clone());
    let pending = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .snapshot(SnapshotRequest {
                    kind: adx_core::snapshots::SnapshotKind::Regular,
                    operation_id: "snapshot-preparation-deadline".into(),
                    expected_revision: running.record.revision,
                    names: vec![],
                    timeout_seconds: 1,
                })
                .await
        })
    };
    backend.prepare_entered.notified().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    gate.notify_one();
    assert!(matches!(pending.await.unwrap(), Err(Error::Unavailable(_))));
    let current = handle.sync().await.unwrap();
    assert_eq!(current.record.state, EnvironmentState::Running);
    assert!(current.record.execution_artifacts.is_empty());
    assert!(current.record.resources_held);
    assert_eq!(backend.running.lock().unwrap().len(), 1);
    let events = backend.events.lock().unwrap().clone();
    assert!(events.iter().any(|event| event == "abort"));
    assert!(!events
        .iter()
        .any(|event| event == "checkpoint-running" || event == "remove"));
    handle.delete().await.unwrap();
}

#[tokio::test]
async fn reusable_snapshot_does_not_retry_a_publication_conflict() {
    use adxlet::checkpoint::SnapshotRequest;
    let (temp, backend, _, mut spec, assignment) = fixture();
    spec.runtime_class = "runsc".into();
    let (node, journal) = snapshot_journal_node(&temp, &backend, spec.resources);
    let handle = node.environment(spec, assignment).unwrap();
    let running = handle.create().await.unwrap();
    *backend.snapshot_hold_error.lock().unwrap() = Some(Error::Conflict);
    let error = handle
        .snapshot(SnapshotRequest {
            kind: adx_core::snapshots::SnapshotKind::Regular,
            operation_id: "snapshot-journal-conflict".into(),
            expected_revision: running.record.revision,
            names: vec![],
            timeout_seconds: 2,
        })
        .await
        .unwrap_err();
    assert_eq!(error, Error::Conflict);
    assert_eq!(backend.rejected_snapshot_records.lock().unwrap().len(), 1);
    assert_eq!(journal.pending().await.unwrap(), 0);
    assert!(backend.snapshots.lock().unwrap().is_empty());
    *backend.snapshot_hold_error.lock().unwrap() = None;
    handle.delete().await.unwrap();
}

#[tokio::test]
async fn reusable_snapshot_returns_unconfirmed_publication_failure_without_retry() {
    use adxlet::checkpoint::SnapshotRequest;
    let (_temp, backend, node, spec, assignment) = fixture();
    let handle = node.environment(spec, assignment).unwrap();
    let running = handle.create().await.unwrap();
    let failure = Error::Unavailable("local durability not confirmed".into());
    *backend.snapshot_hold_error.lock().unwrap() = Some(failure.clone());
    let error = handle
        .snapshot(SnapshotRequest {
            kind: adx_core::snapshots::SnapshotKind::Regular,
            operation_id: "snapshot-no-durability".into(),
            expected_revision: running.record.revision,
            names: vec![],
            timeout_seconds: 2,
        })
        .await
        .unwrap_err();
    assert_eq!(error, failure);
    assert_eq!(backend.rejected_snapshot_records.lock().unwrap().len(), 1);
    assert!(backend.snapshots.lock().unwrap().is_empty());
    *backend.snapshot_hold_error.lock().unwrap() = None;
    handle.delete().await.unwrap();
}

#[tokio::test]
async fn golden_snapshot_capture_replays_metadata_and_rejects_kind_changes() {
    use adx_core::snapshots::SnapshotKind;
    use adxlet::checkpoint::SnapshotRequest;
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    let request = SnapshotRequest {
        operation_id: "golden".into(),
        expected_revision: created.record.revision,
        names: vec!["prepared".into()],
        kind: SnapshotKind::Golden,
        timeout_seconds: 60,
    };
    let saved = h.snapshot(request.clone()).await.unwrap();
    assert_eq!(saved.snapshot.kind, SnapshotKind::Golden);
    assert!(saved.snapshot.created_at_unix_ms > 0);
    assert_eq!(saved.environment.record.state, EnvironmentState::Running);
    assert_eq!(
        h.snapshot(request.clone()).await.unwrap().snapshot,
        saved.snapshot
    );
    let mut changed = request;
    changed.kind = SnapshotKind::Regular;
    assert!(h.snapshot(changed).await.is_err());
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.as_str() == "checkpoint-running")
            .count(),
        1
    );
}

#[tokio::test]
async fn snapshot_publication_failure_attempts_to_resume_source() {
    use adxlet::checkpoint::SnapshotRequest;
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    *backend.fail_snapshot_publish.lock().unwrap() = true;
    let request = SnapshotRequest {
        kind: adx_core::snapshots::SnapshotKind::Regular,
        operation_id: "snapshot-fail".into(),
        expected_revision: created.record.revision,
        names: vec![],
        timeout_seconds: 60,
    };
    assert!(h.snapshot(request.clone()).await.is_err());
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert_eq!(backend.running.lock().unwrap().len(), 1);
    *backend.fail_snapshot_publish.lock().unwrap() = false;
    let saved = h.snapshot(request).await.unwrap();
    assert_eq!(saved.environment.durability, Durability::Published);
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| *event == "checkpoint-running")
            .count(),
        1
    );
    h.delete().await.unwrap();
}

#[tokio::test]
async fn snapshot_checkpoint_commit_failure_still_attempts_source_resume() {
    use adxlet::checkpoint::SnapshotRequest;
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    *backend.unavailable_commit.lock().unwrap() = true;
    assert!(h
        .snapshot(SnapshotRequest {
            kind: adx_core::snapshots::SnapshotKind::Regular,
            operation_id: "snapshot-outage".into(),
            expected_revision: created.record.revision,
            names: vec![],
            timeout_seconds: 60
        })
        .await
        .is_err());
    assert_eq!(backend.running.lock().unwrap().len(), 1);
}

async fn assert_snapshot_clone_lifecycle(kind: adx_core::snapshots::SnapshotKind) {
    use adx_core::snapshots::Reference;
    let (_temp, backend, node, spec, assignment) = fixture();
    let source = node.environment(spec.clone(), assignment.clone()).unwrap();
    let running = source.create().await.unwrap();
    let saved = source
        .snapshot(adxlet::checkpoint::SnapshotRequest {
            kind,
            operation_id: "clone-source".into(),
            expected_revision: running.record.revision,
            names: vec![],
            timeout_seconds: 60,
        })
        .await
        .unwrap();
    source.delete().await.unwrap();
    let mut snapshot = saved.snapshot;
    snapshot
        .acquire(
            "tenant",
            Reference::Restore {
                environment_id: "clone".into(),
            },
        )
        .unwrap();
    let mut spec = spec;
    spec.id = "clone".into();
    spec.snapshot_id = Some(snapshot.id.clone());
    let mut assignment = assignment;
    assignment.environment_id = spec.id.clone();
    let clone = node.environment(spec, assignment).unwrap();
    // An assignment without its source must never silently boot a clean image.
    let starts = backend
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| *e == "start")
        .count();
    assert!(clone.create().await.is_err());
    let created = clone.create_from_snapshot(snapshot.clone()).await.unwrap();
    assert_eq!(created.record.spec.id, "clone");
    assert!(created.record.checkpoint.is_none());
    let cp = created.record.snapshot_source.as_ref().unwrap();
    assert_eq!(cp.artifact, snapshot.artifact);
    assert_eq!(cp.origin.as_ref().unwrap().environment_id, "environment");
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == "start")
            .count(),
        starts
    );
    assert!(matches!(
        snapshot.delete("tenant"),
        Err(Error::SnapshotInUse { .. })
    ));
    assert!(Path::new(&snapshot.artifact.location).exists());
    let paused = clone.pause(pause(created.record.revision)).await.unwrap();
    assert!(paused.record.checkpoint.as_ref().unwrap().origin.is_none());
    clone.resume(resume(paused.record.revision)).await.unwrap();
    clone.delete().await.unwrap();
    snapshot
        .release(&Reference::Restore {
            environment_id: "clone".into(),
        })
        .unwrap();
    snapshot.delete("tenant").unwrap();
    node.collect_snapshot(&snapshot, false).await.unwrap();
    assert!(!Path::new(&snapshot.artifact.location).exists());
    assert!(backend.running.lock().unwrap().is_empty());
}

#[tokio::test]
async fn clone_borrows_snapshot_and_blocks_deletion_until_it_releases_input() {
    assert_snapshot_clone_lifecycle(adx_core::snapshots::SnapshotKind::Regular).await;
}

#[tokio::test]
async fn golden_clone_uses_the_same_restore_references_and_collection() {
    assert_snapshot_clone_lifecycle(adx_core::snapshots::SnapshotKind::Golden).await;
}

#[tokio::test]
async fn failed_snapshot_copy_publishes_failure_without_booting_an_image() {
    use adx_core::snapshots::{Reference, Snapshot};
    let (temp, backend, node, mut spec, mut assignment) = fixture();
    let mut snapshot = Snapshot::new(
        "missing-files".into(),
        vec![],
        spec.clone(),
        "node".into(),
        "environment-1".into(),
        adx_core::CheckpointArtifact {
            scope: String::new(),
            storage: "local".into(),
            location: temp.path().join("missing").to_string_lossy().into(),
            size_bytes: 1,
        },
    )
    .unwrap();
    spec.id = "clone".into();
    spec.snapshot_id = Some(snapshot.id.clone());
    assignment.environment_id = spec.id.clone();
    snapshot
        .acquire(
            "tenant",
            Reference::Restore {
                environment_id: spec.id.clone(),
            },
        )
        .unwrap();
    let handle = node.environment(spec, assignment).unwrap();
    assert!(handle.create_from_snapshot(snapshot).await.is_err());
    let result = handle.sync().await.unwrap();
    assert_eq!(result.record.state, EnvironmentState::Failed);
    assert!(!result.record.resources_held);
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e == "start" || e == "restore"));
    handle.delete().await.unwrap();
}

#[tokio::test]
async fn transferred_checkpoint_rechecks_capture_profile_before_backend_restore() {
    for missing in [true, false] {
        let (_temp, backend, target, mut transferred, _) = shared_recovery_fixture().await;
        if missing {
            let checkpoint = transferred.checkpoint.as_mut().unwrap();
            checkpoint.recovery_profile.clear();
        } else {
            *backend.recovery_profile.lock().unwrap() = Some(BTreeMap::from([(
                "runtime".into(),
                "changed-after-assignment".into(),
            )]));
        }
        let rejected = target.recover_environment(transferred).await.unwrap();
        assert_eq!(rejected.record.state, EnvironmentState::Failed);
        assert!(!backend
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|event| event == "restore"));
        assert!(backend.running.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn recovery_with_unusable_checkpoint_settles_failed_without_image_start() {
    let (_temp, backend, node, spec, mut assignment) = fixture();
    assignment.generation = 2;
    let recovery_profile = backend.recovery_profile(&spec).unwrap();
    let record = EnvironmentRecord {
        observation: None,
        execution_artifacts: vec![],
        snapshot_source: None,
        failure: None,
        spec,
        assignment,
        runtime: adx_core::Runtime {
            id: "environment-2".into(),
            ip: None,
        },
        state: EnvironmentState::Paused,
        revision: 1,
        resources_held: false,
        checkpoint: Some(adx_core::RestorePoint {
            recovery_profile,
            id: "recovery".into(),
            source_runtime_id: "environment-1".into(),
            expires_at_unix_seconds: None,
            origin: Some(adx_core::runtime::RuntimeIdentity {
                environment_id: "environment".into(),
                runtime_id: "environment-1".into(),
                ownership_generation: 1,
            }),
            artifact: adx_core::CheckpointArtifact {
                scope: String::new(),
                storage: "missing-shared-backend".into(),
                location: "unavailable".into(),
                size_bytes: 8,
            },
        }),
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
    };
    let result = node
        .recover_environment(record.clone())
        .await
        .expect("recovery publishes a terminal failure");
    assert_eq!(result.record.state, EnvironmentState::Failed);
    assert!(!result.record.resources_held);
    assert_eq!(node.used(), Resources::default());
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e == "start" || e == "restore"));
    assert_eq!(
        node.recover_environment(record).await.unwrap().record,
        result.record
    );
}

async fn shared_recovery_fixture() -> (
    tempfile::TempDir,
    Arc<Backend>,
    Adxlet,
    EnvironmentRecord,
    Arc<object_store::memory::InMemory>,
) {
    use adxlet::checkpoint::{CheckpointAccess, ObjectCheckpointStore};
    let (temp, backend, _, spec, mut assignment) = fixture();
    let remote = Arc::new(object_store::memory::InMemory::new());
    let store = Arc::new(
        ObjectCheckpointStore::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
            temp.path().join("source"),
            1024,
        )
        .unwrap(),
    );
    let path = store.begin_capture().await.unwrap();
    std::fs::write(path.join("memory"), b"actual saved memory").unwrap();
    let artifact = store.seal(&path).await.unwrap();
    // Use a fresh target cache so missing remote objects cannot be hidden by
    // the source's completed local capture.
    let store = Arc::new(
        ObjectCheckpointStore::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
            temp.path().join("target"),
            1024,
        )
        .unwrap(),
    );
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(store, backend.clone())
    .unwrap();
    node.update_capacity(spec.resources, Duration::from_secs(60))
        .unwrap();
    assignment.generation = 2;
    let recovery_profile = backend.recovery_profile(&spec).unwrap();
    let seed = EnvironmentRecord {
        observation: None,
        execution_artifacts: vec![],
        snapshot_source: None,
        failure: None,
        spec,
        assignment,
        runtime: adx_core::Runtime {
            id: "environment-2".into(),
            ip: None,
        },
        state: EnvironmentState::Paused,
        revision: 1,
        resources_held: false,
        checkpoint: Some(adx_core::RestorePoint {
            id: "recovery".into(),
            source_runtime_id: "environment-1".into(),
            expires_at_unix_seconds: None,
            origin: Some(adx_core::runtime::RuntimeIdentity {
                environment_id: "environment".into(),
                runtime_id: "environment-1".into(),
                ownership_generation: 1,
            }),
            recovery_profile,
            artifact,
        }),
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
    };
    (temp, backend, node, seed, remote)
}

#[tokio::test]
async fn recovery_retries_transient_execution_admission_errors() {
    for (error, delay) in [
        (
            Some(Error::Unavailable(
                "execution authorized; scheduler reconciliation required".into(),
            )),
            Duration::ZERO,
        ),
        (
            Some(Error::VolumeBusy("workspace-uid".into())),
            Duration::ZERO,
        ),
        (Some(Error::NoCapacity), Duration::ZERO),
        // The same initialization deadline is retryable only before restore.
        (None, Duration::from_secs(10)),
    ] {
        let (_temp, backend, node, seed, _) = shared_recovery_fixture().await;
        let node = if delay.is_zero() {
            node
        } else {
            node.with_operation_timeout(Duration::from_millis(250))
                .unwrap()
        };
        *backend.execution_admission_error.lock().unwrap() = error.clone();
        *backend.execution_admission_delay.lock().unwrap() = delay;
        let expected =
            error.unwrap_or_else(|| Error::Unavailable("restore initialization timed out".into()));
        assert_eq!(node.recover_environment(seed.clone()).await, Err(expected));
        let waiting = node
            .environment(seed.spec.clone(), seed.assignment.clone())
            .unwrap()
            .sync()
            .await
            .unwrap()
            .record;
        assert_eq!(waiting.state, EnvironmentState::Paused);
        assert_eq!(waiting.checkpoint, seed.checkpoint);
        assert!(!waiting.resources_held);
        assert_eq!(node.used(), Resources::default());
        assert!(!backend
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e == "start" || e == "restore"));

        *backend.execution_admission_error.lock().unwrap() = None;
        *backend.execution_admission_delay.lock().unwrap() = Duration::ZERO;
        let restored = node.recover_environment(waiting.clone()).await.unwrap();
        assert_eq!(restored.record.state, EnvironmentState::Running);
        assert_eq!(restored.record.assignment, seed.assignment);
        assert_eq!(node.used(), seed.spec.resources);
        assert_eq!(
            node.recover_environment(waiting).await.unwrap().record,
            restored.record
        );
        assert_eq!(
            backend
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| *e == "restore")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn recovery_non_retryable_failures_settle_failed_without_repeating_restore() {
    use futures_util::TryStreamExt;
    use object_store::{ObjectStore, ObjectStoreExt};
    #[derive(Clone, Copy, Debug)]
    enum Failure {
        MissingManifest,
        MissingData,
        Backend,
        RestoreTimeout,
        AdmissionDenied,
    }
    for failure in [
        Failure::MissingManifest,
        Failure::MissingData,
        Failure::Backend,
        Failure::RestoreTimeout,
        Failure::AdmissionDenied,
    ] {
        let (_temp, backend, node, seed, remote) = shared_recovery_fixture().await;
        let node = if matches!(failure, Failure::RestoreTimeout) {
            node.with_operation_timeout(Duration::from_millis(250))
                .unwrap()
        } else {
            node
        };
        match failure {
            Failure::MissingManifest | Failure::MissingData => {
                let objects: Vec<_> = remote.list(None).try_collect().await.unwrap();
                let object = objects
                    .iter()
                    .find(|object| {
                        object.location.as_ref().ends_with("manifest.json")
                            == matches!(failure, Failure::MissingManifest)
                    })
                    .expect("published checkpoint object");
                remote.delete(&object.location).await.unwrap();
            }
            Failure::Backend => *backend.fail_restore.lock().unwrap() = true,
            Failure::RestoreTimeout => {
                *backend.restore_delay.lock().unwrap() = Duration::from_secs(10)
            }
            Failure::AdmissionDenied => {
                *backend.execution_admission_error.lock().unwrap() =
                    Some(adx_protocol::dependency_status(
                        tonic::Status::permission_denied("node credentials rejected"),
                    ))
            }
        }
        let result = node
            .recover_environment(seed.clone())
            .await
            .unwrap_or_else(|error| {
                panic!("{failure:?}: expected terminal recovery result, got {error}")
            });
        assert_eq!(result.record.state, EnvironmentState::Failed, "{failure:?}");
        assert!(!result.record.resources_held);
        assert_eq!(node.used(), Resources::default());
        assert_eq!(
            node.recover_environment(seed).await.unwrap().record,
            result.record
        );
        let events = backend.events.lock().unwrap();
        let expected_restores = usize::from(matches!(
            failure,
            Failure::Backend | Failure::RestoreTimeout
        ));
        assert_eq!(
            events.iter().filter(|event| *event == "restore").count(),
            expected_restores
        );
        assert!(!events.iter().any(|event| event == "start"));
        assert!(backend.running.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn recovery_retries_publication_without_repeating_backend_restore() {
    use adxlet::checkpoint::{CheckpointAccess, ObjectCheckpointStore};
    for failure_state in [EnvironmentState::Paused, EnvironmentState::Running] {
        let (temp, backend, _, spec, mut assignment) = fixture();
        let store = Arc::new(
            ObjectCheckpointStore::new(
                "shared".into(),
                Arc::new(object_store::memory::InMemory::new()),
                "test".into(),
                temp.path().into(),
                1024,
            )
            .unwrap(),
        );
        let path = store.begin_capture().await.unwrap();
        std::fs::write(path.join("memory"), b"actual saved memory").unwrap();
        let artifact = store.seal(&path).await.unwrap();
        let node = Adxlet::new(
            "node".into(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
        )
        .with_checkpointing(store, backend.clone())
        .unwrap();
        node.update_capacity(spec.resources, Duration::from_secs(60))
            .unwrap();
        assignment.generation = 2;
        let recovery_profile = backend.recovery_profile(&spec).unwrap();
        let seed = EnvironmentRecord {
            observation: None,
            execution_artifacts: vec![],
            snapshot_source: None,
            failure: None,
            spec,
            assignment,
            runtime: adx_core::Runtime {
                id: "environment-2".into(),
                ip: None,
            },
            state: EnvironmentState::Paused,
            revision: 1,
            resources_held: false,
            checkpoint: Some(adx_core::RestorePoint {
                recovery_profile,
                id: "recovery".into(),
                source_runtime_id: "environment-1".into(),
                expires_at_unix_seconds: None,
                origin: Some(adx_core::runtime::RuntimeIdentity {
                    environment_id: "environment".into(),
                    runtime_id: "environment-1".into(),
                    ownership_generation: 1,
                }),
                artifact,
            }),
            last_operation: None,
            restart_attempts: 0,
            restart_pending: false,
        };
        *backend.unavailable_state.lock().unwrap() = Some(failure_state);
        *backend.unavailable_commit.lock().unwrap() = true;
        assert!(node.recover_environment(seed.clone()).await.is_err());
        assert_eq!(
            backend.running.lock().unwrap().len(),
            usize::from(failure_state == EnvironmentState::Running)
        );
        *backend.unavailable_commit.lock().unwrap() = false;
        // Coordinator can restart after the input hold was published. Its retry
        // then carries that newer Paused record, not the original revision one.
        let mut progress = seed.clone();
        progress.revision += 1;
        progress.execution_artifacts = vec![seed.checkpoint.as_ref().unwrap().artifact.clone()];
        let handle = node
            .environment(seed.spec.clone(), seed.assignment.clone())
            .unwrap();
        assert_eq!(
            handle
                .resume(ResumeRequest {
                    run_timeout_seconds: None,
                    operation_id: "recover-2".into(),
                    expected_revision: progress.revision
                })
                .await
                .unwrap_err(),
            Error::Conflict
        );
        let result = node.recover_environment(progress.clone()).await.unwrap();
        assert_eq!(result.durability, Durability::Published);
        assert_eq!(result.record.state, EnvironmentState::Running);
        assert_eq!(
            result
                .record
                .last_operation
                .as_ref()
                .unwrap()
                .expected_revision,
            1
        );
        assert_eq!(
            node.recover_environment(progress).await.unwrap().record,
            result.record
        );
        assert_eq!(
            node.recover_environment(seed).await.unwrap().record,
            result.record
        );
        let events = backend.events.lock().unwrap();
        assert_eq!(events.iter().filter(|e| *e == "restore").count(), 1);
        assert!(!events.iter().any(|e| e == "start"));
    }
}

#[tokio::test]
async fn workload_checkpoint_keeps_execution_and_commits_before_acknowledging() {
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.execution.failover = true;
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    *backend.workload_request.lock().unwrap() = Some("workload-a".into());
    *backend.unavailable_commit.lock().unwrap() = true;
    assert!(handle.tick().await.is_err());
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e == "checkpoint-ack"));
    *backend.unavailable_commit.lock().unwrap() = false;
    handle.tick().await.unwrap();
    assert_eq!(
        backend
            .workload_checkpoint_timeouts
            .lock()
            .unwrap()
            .as_slice(),
        &[Duration::from_secs(600)]
    );
    let record = handle.sync().await.unwrap().record;
    assert_eq!(record.state, EnvironmentState::Running);
    assert_eq!(record.runtime, created.record.runtime);
    assert_eq!(node.used(), record.spec.resources);
    assert_eq!(record.checkpoint.as_ref().unwrap().id, "workload-a");
    assert_eq!(
        record.checkpoint.as_ref().unwrap().artifact.storage,
        "local"
    );
    let events = backend.events.lock().unwrap().clone();
    assert_eq!(
        events.iter().filter(|e| *e == "checkpoint-running").count(),
        1
    );
    assert!(!events
        .iter()
        .any(|e| e == "remove" || e == "restore" || e == "retire"));
    let ack = events.iter().position(|e| e == "checkpoint-ack").unwrap();
    assert_eq!(events[ack - 1], "commit:Running");
    // Existing failover consumes this anonymous recovery point.
    backend.running.lock().unwrap().clear();
    handle.tick().await.unwrap();
    assert_ne!(
        handle.sync().await.unwrap().record.runtime,
        created.record.runtime
    );
}

#[tokio::test]
async fn workload_checkpoint_passes_the_remaining_user_budget_to_the_backend() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let handle = node.environment(spec, assignment).unwrap();
    handle.create().await.unwrap();
    *backend.workload_request.lock().unwrap() = Some("bounded".into());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    *backend.workload_deadline_unix_millis.lock().unwrap() = Some(now + 2_000);
    handle.tick().await.unwrap();
    let timeouts = backend.workload_checkpoint_timeouts.lock().unwrap();
    assert_eq!(timeouts.len(), 1);
    assert!(timeouts[0] > Duration::ZERO);
    assert!(timeouts[0] <= Duration::from_secs(2));
}

#[tokio::test]
async fn expired_workload_checkpoint_is_rejected_without_starting_the_backend() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let handle = node.environment(spec, assignment).unwrap();
    handle.create().await.unwrap();
    *backend.workload_request.lock().unwrap() = Some("expired".into());
    *backend.workload_deadline_unix_millis.lock().unwrap() = Some(1);
    assert!(handle.tick().await.is_err());
    assert!(backend
        .workload_checkpoint_timeouts
        .lock()
        .unwrap()
        .is_empty());
    assert!(backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event == "checkpoint-error"));
}

#[tokio::test]
async fn workload_checkpoint_uses_configured_shared_store_before_acknowledging() {
    let (temp, backend, _node, mut spec, assignment) = fixture();
    spec.execution.failover = true;
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(
        Arc::new(SharedPublication {
            local: LocalCheckpointStore::new(temp.path().into()).unwrap(),
            backend: backend.clone(),
        }),
        backend.clone(),
    )
    .unwrap();
    node.update_capacity(spec.resources, Duration::from_secs(300))
        .unwrap();
    let handle = node.environment(spec, assignment).unwrap();
    let running = handle.create().await.unwrap();
    *backend.workload_request.lock().unwrap() = Some("shared-workload".into());
    handle.tick().await.unwrap();
    let record = handle.sync().await.unwrap().record;
    assert_eq!(record.runtime, running.record.runtime);
    assert_eq!(record.state, EnvironmentState::Running);
    assert_eq!(record.checkpoint.unwrap().artifact.storage, "shared");
    let events = backend.events.lock().unwrap();
    let published = events
        .iter()
        .position(|event| event == "checkpoint-publish")
        .unwrap();
    let committed = events
        .iter()
        .enumerate()
        .find(|(index, event)| *index > published && event.as_str() == "commit:Running")
        .map(|(index, _)| index)
        .unwrap();
    let cleaned = events
        .iter()
        .position(|event| event == "checkpoint-store-committed")
        .unwrap();
    let acknowledged = events
        .iter()
        .position(|event| event == "checkpoint-ack")
        .unwrap();
    assert!(published < committed && committed < cleaned && cleaned < acknowledged);
}

#[tokio::test]
async fn workload_checkpoint_rejects_unavailable_shared_publication() {
    let (temp, backend, _node, mut spec, assignment) = fixture();
    spec.execution.failover = true;
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(
        Arc::new(UnavailablePublication(
            LocalCheckpointStore::new(temp.path().into()).unwrap(),
        )),
        backend.clone(),
    )
    .unwrap();
    node.update_capacity(spec.resources, Duration::from_secs(300))
        .unwrap();
    let handle = node.environment(spec, assignment).unwrap();
    let running = handle.create().await.unwrap();
    *backend.workload_request.lock().unwrap() = Some("failed-upload".into());
    assert!(handle.tick().await.is_err());
    let record = handle.sync().await.unwrap().record;
    assert_eq!(record.state, EnvironmentState::Running);
    assert_eq!(record.runtime, running.record.runtime);
    assert!(record.checkpoint.is_none());
    let events = backend.events.lock().unwrap();
    assert!(events.iter().any(|event| event == "checkpoint-error"));
    assert!(!events.iter().any(|event| event == "checkpoint-ack"));
}

#[tokio::test]
async fn workload_checkpoint_unknown_backend_result_is_not_reexecuted() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let handle = node.environment(spec, assignment).unwrap();
    handle.create().await.unwrap();
    *backend.workload_request.lock().unwrap() = Some("workload-fail".into());
    *backend.fail_workload_checkpoint.lock().unwrap() = true;
    assert!(handle.tick().await.is_err());
    let failed = handle.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert!(failed.checkpoint.is_none());
    handle.tick().await.unwrap();
    let events = backend.events.lock().unwrap();
    assert_eq!(
        events.iter().filter(|e| *e == "checkpoint-running").count(),
        1
    );
    assert!(!events.iter().any(|e| e == "checkpoint-ack"));
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test]
async fn restart_retires_uncommitted_workload_checkpoint_without_recapture() {
    let (temp, backend, node, spec, assignment) = fixture();
    let created = node
        .environment(spec.clone(), assignment.clone())
        .unwrap()
        .create()
        .await
        .unwrap();
    *backend.workload_request.lock().unwrap() = Some("interrupted".into());
    *backend.workload_prepared.lock().unwrap() = true;
    let restarted = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(
        Arc::new(LocalCheckpointStore::new(temp.path().into()).unwrap()),
        backend.clone(),
    )
    .unwrap();
    restarted.reconcile(vec![created.record]).await.unwrap();
    let record = restarted
        .environment(spec, assignment)
        .unwrap()
        .sync()
        .await
        .unwrap()
        .record;
    assert_eq!(record.state, EnvironmentState::Failed);
    assert!(backend.running.lock().unwrap().is_empty());
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e == "checkpoint-running" || e == "checkpoint-ack"));
}

#[tokio::test]
async fn pause_retires_control_state_only_after_the_backend_stop_barrier() {
    use adxlet::{control_stream::RuntimeControlHub, runtime_control::RuntimeControlClient};
    let (_temp, backend, node, spec, assignment) = fixture();
    let hub = RuntimeControlHub::new(vec![1; 32], Duration::from_secs(2)).unwrap();
    let node = node
        .with_runtime_control(
            RuntimeControlClient::new(1, Duration::from_secs(2))
                .unwrap()
                .with_stream(hub.clone()),
        )
        .unwrap();
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    let identity = RuntimeControlClient::identity(&created.record);
    hub.expect(&identity).unwrap();
    let paused = handle.pause(pause(created.record.revision)).await.unwrap();
    assert_eq!(paused.record.state, EnvironmentState::Paused);
    assert!(
        hub.is_empty(),
        "stopped execution must not keep a control slot or reconnect authority"
    );
    assert!(hub.observed(&identity).is_err());

    let running = handle.resume(resume(paused.record.revision)).await.unwrap();
    let identity = RuntimeControlClient::identity(&running.record);
    hub.expect(&identity).unwrap();
    *backend.fail_remove.lock().unwrap() = true;
    let mut request = pause(running.record.revision);
    request.operation_id = "pause-stop-unknown".into();
    assert!(handle.pause(request).await.is_err());
    assert_eq!(
        hub.len(),
        1,
        "unknown backend stop must not discard control state"
    );
    *backend.fail_remove.lock().unwrap() = false;
    handle.delete().await.unwrap();
    assert!(hub.is_empty());
}

#[tokio::test(start_paused = true)]
async fn idle_pause_releases_resources_and_concurrent_demand_restores_once() {
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.lifecycle.idle_timeout_seconds = 10;
    spec.lifecycle.idle_action = adx_core::lifecycle::IdleAction::Pause;

    let h = node.environment(spec.clone(), assignment).unwrap();
    let initial = h.create().await.unwrap();
    *backend.activity.lock().unwrap() = Some((1, 0));
    h.tick().await.unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    h.tick().await.unwrap();
    let paused = h.sync().await.unwrap();
    assert_eq!(paused.record.state, EnvironmentState::Paused);
    assert_eq!(node.used(), Resources::default());
    assert!(backend.running.lock().unwrap().is_empty());
    let mut wakes = Vec::new();
    for _ in 0..16 {
        let h = h.clone();
        wakes.push(tokio::spawn(async move {
            h.ensure_running("shared-wake".into()).await.unwrap()
        }));
    }
    for wake in wakes {
        assert_eq!(wake.await.unwrap().record.state, EnvironmentState::Running);
    }
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.as_str() == "restore")
            .count(),
        1
    );
    assert_eq!(backend.running.lock().unwrap().len(), 1);
    assert_ne!(
        h.sync().await.unwrap().record.runtime.id,
        initial.record.runtime.id
    );
    assert_eq!(node.used(), spec.resources);
}

#[tokio::test(start_paused = true)]
async fn traffic_before_capture_cancels_idle_pause() {
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.lifecycle.idle_timeout_seconds = 10;
    spec.lifecycle.idle_action = adx_core::lifecycle::IdleAction::Pause;
    let h = node.environment(spec, assignment).unwrap();
    h.create().await.unwrap();
    *backend.activity.lock().unwrap() = Some((1, 0));
    h.tick().await.unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *backend.prepare_gate.lock().unwrap() = Some(gate.clone());
    let monitor = {
        let h = h.clone();
        tokio::spawn(async move { h.tick().await })
    };
    backend.prepare_entered.notified().await;
    let mut wake = Box::pin(h.ensure_running("arriving-demand".into()));
    assert!(futures_util::poll!(wake.as_mut()).is_pending());
    gate.notify_one();
    monitor.await.unwrap().unwrap();
    assert_eq!(wake.await.unwrap().record.state, EnvironmentState::Running);
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e == "checkpoint"));
    assert!(backend.events.lock().unwrap().iter().any(|e| e == "abort"));
}

#[tokio::test(start_paused = true)]
async fn traffic_during_capture_waits_then_resumes_without_replaying_capture() {
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.lifecycle.idle_timeout_seconds = 10;
    spec.lifecycle.idle_action = adx_core::lifecycle::IdleAction::Pause;
    let h = node.environment(spec, assignment).unwrap();
    h.create().await.unwrap();
    *backend.activity.lock().unwrap() = Some((1, 0));
    h.tick().await.unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *backend.checkpoint_gate.lock().unwrap() = Some(gate.clone());
    let monitor = {
        let h = h.clone();
        tokio::spawn(async move { h.tick().await })
    };
    backend.checkpoint_entered.notified().await;
    let mut wake = Box::pin(h.ensure_running("arriving-demand".into()));
    assert!(futures_util::poll!(wake.as_mut()).is_pending());
    gate.notify_one();
    monitor.await.unwrap().unwrap();
    assert_eq!(wake.await.unwrap().record.state, EnvironmentState::Running);
    let events = backend.events.lock().unwrap();
    assert_eq!(
        events.iter().filter(|e| e.as_str() == "checkpoint").count(),
        1
    );
    assert_eq!(events.iter().filter(|e| e.as_str() == "restore").count(), 1);
}

#[tokio::test]
async fn retired_controller_leaves_shared_checkpoint_for_coordinator() {
    retired_shared_checkpoint(Some(false)).await;
}
#[tokio::test]
async fn retired_controller_preserves_shared_checkpoint_used_by_another_node() {
    retired_shared_checkpoint(Some(true)).await;
}
#[tokio::test]
async fn retired_controller_preserves_shared_checkpoint_without_global_catalog() {
    retired_shared_checkpoint(None).await;
}
async fn retired_shared_checkpoint(retained: Option<bool>) {
    use adxlet::checkpoint::{CheckpointAccess, ObjectCheckpointStore};
    let (temp, backend, _, mut spec, assignment) = fixture();
    let store = Arc::new(
        ObjectCheckpointStore::new(
            "shared".into(),
            Arc::new(object_store::memory::InMemory::new()),
            "test".into(),
            temp.path().into(),
            1024,
        )
        .unwrap(),
    );
    spec.lifecycle.pause = Some(adx_core::lifecycle::PausePolicy {
        storage: adx_core::lifecycle::CheckpointStorage::Shared,
        ..Default::default()
    });
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(store.clone(), backend)
    .unwrap();
    node.update_capacity(spec.resources, Duration::from_secs(60))
        .unwrap();
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    let paused = handle.pause(pause(created.record.revision)).await.unwrap();
    let artifact = paused.record.checkpoint.unwrap().artifact;
    store.validate_artifact(&artifact).await.unwrap();
    if let Some(retained) = retained {
        node.reconcile_retained(
            vec![],
            vec![],
            if retained {
                vec![artifact.clone()]
            } else {
                vec![]
            },
        )
        .await
        .unwrap();
    } else {
        node.reconcile_catalog(vec![], vec![]).await.unwrap();
    }
    assert!(
        store.validate_artifact(&artifact).await.is_ok(),
        "node cleanup cannot delete shared objects"
    );
}

#[tokio::test]
async fn object_restore_over_quota_releases_runsc_files_and_protects_firecracker() {
    use adxlet::checkpoint::ObjectCheckpointStore;
    for (runtime_class, background) in [("runsc", false), ("runsc", true), ("firecracker", false)] {
        let (root, backend, _, mut spec, assignment) = fixture();
        *backend.background_restore.lock().unwrap() = background;
        spec.runtime_class = runtime_class.into();
        let store = Arc::new(
            ObjectCheckpointStore::new(
                "shared".into(),
                Arc::new(object_store::memory::InMemory::new()),
                "test".into(),
                root.path().into(),
                1,
            )
            .unwrap()
            .with_max_entries(1)
            .unwrap(),
        );
        let node = Adxlet::new(
            "node".into(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
            backend.clone(),
        )
        .with_checkpointing(store.clone(), backend)
        .unwrap();
        node.update_capacity(spec.resources, Duration::from_secs(300))
            .unwrap();
        let h = node.environment(spec, assignment).unwrap();
        let created = h.create().await.unwrap();
        let paused = h.pause(pause(created.record.revision)).await.unwrap();
        let artifact = &paused.record.checkpoint.as_ref().unwrap().artifact;
        let restored_path = root.path().join("cache").join(&artifact.location);
        let running = h.resume(resume(paused.record.revision)).await.unwrap();
        assert_eq!(running.record.state, EnvironmentState::Running);
        store.evict_unreferenced(0).await.unwrap();
        if runtime_class == "runsc" && !background {
            assert!(
                !restored_path.exists(),
                "synchronous runsc restore releases its inputs"
            );
        } else {
            assert!(
                restored_path.join("memory").exists(),
                "background restore input or Firecracker base remains pinned despite quota pressure"
            );
        }
        h.delete().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while restored_path.exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn object_pause_seeds_same_node_resume_without_remote_manifest() {
    use adxlet::checkpoint::ObjectCheckpointStore;
    use object_store::{ObjectStore, ObjectStoreExt};
    let (root, backend, _, spec, assignment) = fixture();
    let remote: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let store = Arc::new(
        ObjectCheckpointStore::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
            root.path().into(),
            1024 * 1024,
        )
        .unwrap(),
    );
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(store, backend)
    .unwrap();
    node.update_capacity(spec.resources, Duration::from_secs(300))
        .unwrap();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    let artifact = &paused.record.checkpoint.as_ref().unwrap().artifact;
    remote
        .delete(&object_store::path::Path::from(format!(
            "test/{}/manifest.json",
            artifact.location
        )))
        .await
        .unwrap();
    let running = h.resume(resume(paused.record.revision)).await.unwrap();
    assert_eq!(running.record.state, EnvironmentState::Running);
    h.delete().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn idle_pause_after_aborted_prepare_uses_a_new_operation() {
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.lifecycle.idle_timeout_seconds = 10;
    spec.lifecycle.idle_action = adx_core::lifecycle::IdleAction::Pause;
    let handle = node.environment(spec, assignment).unwrap();
    handle.create().await.unwrap();
    *backend.activity.lock().unwrap() = Some((1, 0));
    *backend.fail_prepare.lock().unwrap() = true;
    handle.tick().await.unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(handle.tick().await.is_err());
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert!(backend.events.lock().unwrap().iter().any(|e| e == "abort"));
    *backend.fail_prepare.lock().unwrap() = false;
    handle.tick().await.unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    handle.tick().await.unwrap();
    let ids = backend.prepare_ids.lock().unwrap().clone();
    assert_eq!(ids.len(), 2);
    assert_ne!(
        ids[0], ids[1],
        "an aborted operation cannot identify a new prepare attempt"
    );
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        EnvironmentState::Paused
    );
}

#[tokio::test]
async fn pause_retries_input_publication_without_capturing_twice() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    let request = pause(created.record.revision);
    *backend.unavailable_commit.lock().unwrap() = true;
    assert!(h.pause(request.clone()).await.is_err());
    assert!(backend
        .running
        .lock()
        .unwrap()
        .contains(&created.record.runtime.id));
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|e| e == "checkpoint"));
    *backend.unavailable_commit.lock().unwrap() = false;
    let paused = h.pause(request).await.unwrap();
    assert_eq!(paused.record.state, EnvironmentState::Paused);
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| *e == "checkpoint")
            .count(),
        1
    );
}

#[tokio::test]
async fn unsupported_snapshot_does_not_stop_source_or_allocate_capture() {
    let (temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    *backend.unsupported_checkpoint.lock().unwrap() = true;
    assert_eq!(
        h.snapshot(adxlet::checkpoint::SnapshotRequest {
            kind: adx_core::snapshots::SnapshotKind::Regular,
            operation_id: "snapshot-unsupported".into(),
            expected_revision: created.record.revision,
            names: vec![],
            timeout_seconds: 30,
        })
        .await
        .unwrap_err(),
        Error::UnsupportedCapability(UnsupportedCapability::CheckpointRestore),
    );
    assert!(backend
        .running
        .lock()
        .unwrap()
        .contains(&created.record.runtime.id));
    assert_eq!(h.sync().await.unwrap().record, created.record);
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn e2b_runtime_event_processes_guest_checkpoint_before_service_health() {
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.compatibility = Some(adx_core::environment_control::EnvironmentCompatibility {
        template_id: "base".into(),
        envd_access_token: "a".repeat(32),
        traffic_access_token: "b".repeat(32),
        allow_public_traffic: false,
    });
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    *backend.workload_request.lock().unwrap() = Some("guest-checkpoint".into());
    node.observe_runtime_event(&adx_core::runtime::RuntimeIdentity {
        environment_id: created.record.spec.id.clone(),
        runtime_id: created.record.runtime.id.clone(),
        ownership_generation: created.record.assignment.generation,
    })
    .await
    .unwrap();

    let record = handle.sync().await.unwrap().record;
    assert_eq!(record.state, EnvironmentState::Running);
    assert_eq!(record.runtime, created.record.runtime);
    assert_eq!(record.checkpoint.as_ref().unwrap().id, "guest-checkpoint");
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event == "retire" || event == "remove"));
}

#[tokio::test]
async fn unsupported_pause_does_not_stop_source_or_allocate_capture() {
    let (temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    *backend.unsupported_checkpoint.lock().unwrap() = true;
    assert_eq!(
        h.pause(pause(created.record.revision)).await.unwrap_err(),
        Error::UnsupportedCapability(UnsupportedCapability::CheckpointRestore),
    );
    assert!(backend
        .running
        .lock()
        .unwrap()
        .contains(&created.record.runtime.id));
    assert_eq!(h.sync().await.unwrap().record, created.record);
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn runsc_capture_preserves_background_restore_input_without_pinning_new_capture() {
    let (root, backend, _, mut spec, assignment) = fixture();
    let store = Arc::new(
        adxlet::checkpoint::ObjectCheckpointStore::new(
            "shared".into(),
            Arc::new(object_store::memory::InMemory::new()),
            "test".into(),
            root.path().into(),
            1,
        )
        .unwrap()
        .with_max_entries(1)
        .unwrap(),
    );
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(store.clone(), backend.clone())
    .unwrap()
    .with_snapshot_catalog(backend.clone())
    .unwrap();
    node.update_capacity(spec.resources, Duration::from_secs(300))
        .unwrap();
    spec.runtime_class = "runsc".into();
    *backend.background_restore.lock().unwrap() = true;
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    let input = paused.record.checkpoint.as_ref().unwrap().artifact.clone();
    let running = h.resume(resume(paused.record.revision)).await.unwrap();
    assert!(running.record.execution_artifacts.contains(&input));
    let snapshot = h
        .snapshot(adxlet::checkpoint::SnapshotRequest {
            kind: adx_core::snapshots::SnapshotKind::Regular,
            operation_id: "capture-after-background-restore".into(),
            expected_revision: running.record.revision,
            names: vec![],
            timeout_seconds: 60,
        })
        .await
        .unwrap();
    assert!(snapshot
        .environment
        .record
        .execution_artifacts
        .contains(&input));
    assert!(!snapshot
        .environment
        .record
        .execution_artifacts
        .contains(&snapshot.snapshot.artifact));
    store.evict_unreferenced(0).await.unwrap();
    let input_path = root.path().join("cache").join(&input.location);
    assert!(
        input_path.join("memory").exists(),
        "new capture must not unpin an ongoing background restore"
    );
    let deleted = h.delete().await.unwrap();
    assert!(deleted.record.execution_artifacts.is_empty());
    store.evict_unreferenced(0).await.unwrap();
    assert!(
        !input_path.exists(),
        "stopped execution releases the cache input"
    );
}

#[tokio::test]
async fn refused_resume_admission_preserves_previous_runtime_and_can_retry() {
    for error in [Error::VolumeBusy("workspace-uid".into()), Error::NoCapacity] {
        let (_temp, backend, node, spec, assignment) = fixture();
        let handle = node.environment(spec, assignment).unwrap();
        let created = handle.create().await.unwrap();
        let paused = handle.pause(pause(created.record.revision)).await.unwrap();
        *backend.execution_admission_error.lock().unwrap() = Some(error.clone());
        assert_eq!(
            handle.resume(resume(paused.record.revision)).await,
            Err(error)
        );
        let waiting = handle.sync().await.unwrap();
        assert_eq!(waiting.record.state, EnvironmentState::Paused);
        assert!(!waiting.record.resources_held);
        assert_eq!(waiting.record.runtime.id, paused.record.runtime.id);
        *backend.execution_admission_error.lock().unwrap() = None;
        let restored = handle
            .resume(resume(waiting.record.revision))
            .await
            .unwrap();
        assert_eq!(restored.record.state, EnvironmentState::Running);
        assert_ne!(restored.record.runtime.id, paused.record.runtime.id);
    }
}

#[tokio::test]
async fn ambiguous_resume_admission_retains_attempt_identity_for_cleanup() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    let paused = handle.pause(pause(created.record.revision)).await.unwrap();
    let error = Error::Unavailable("authorization response lost".into());
    *backend.execution_admission_error.lock().unwrap() = Some(error.clone());
    assert_eq!(
        handle.resume(resume(paused.record.revision)).await,
        Err(error)
    );
    let waiting = handle.sync().await.unwrap();
    assert_eq!(waiting.record.state, EnvironmentState::Paused);
    assert!(!waiting.record.resources_held);
    assert_ne!(waiting.record.runtime.id, paused.record.runtime.id);
}

#[derive(Clone, Copy)]
enum WorkloadRetryFailure {
    InputCommit,
    ResultCommit,
    Acknowledgement,
    ExpiredBudget,
}

async fn workload_retry_case(failure: WorkloadRetryFailure) {
    use adx_core::runtime::{RuntimePhase, RuntimeStatus};
    use adx_protocol::{runtime as pb, runtime_stream::execution_token};
    use adxlet::{control_stream::RuntimeControlHub, runtime_control::RuntimeControlClient};
    use tokio::sync::mpsc;
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.execution.failover = true;
    spec.runtime_class = "runsc".into();
    let secret = b"0123456789abcdef0123456789abcdef";
    let hub = RuntimeControlHub::new(secret.to_vec(), Duration::from_secs(1)).unwrap();
    let control = RuntimeControlClient::new(50090, Duration::from_secs(1))
        .unwrap()
        .with_stream(hub.clone());
    let node = node.with_runtime_control(control).unwrap();
    hub.expect(&adx_core::runtime::RuntimeIdentity {
        environment_id: spec.id.clone(),
        runtime_id: "environment-1".into(),
        ownership_generation: 1,
    })
    .unwrap();
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    let identity = RuntimeControlClient::identity(&created.record);
    hub.expect(&identity).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server_hub = hub.clone();
    let server = tokio::spawn(async move {
        server_hub.serve(listener).await.unwrap();
    });
    let mut client =
        pb::runtime_control_service_client::RuntimeControlServiceClient::connect(address)
            .await
            .unwrap();
    let (send, receive) = mpsc::channel(16);
    let mut status = RuntimeStatus {
        identity: identity.clone(),
        required_services: Default::default(),
        revision: 1,
        phase: RuntimePhase::Running,
        failure: None,
        checkpoint: None,
        requested_checkpoint: None,
        requested_checkpoint_deadline_unix_millis: None,
        active_requests: 1,
        active_commands: 0,
        activity_revision: 1,
    };
    *backend.workload_request.lock().unwrap() = Some("retry-workload".into());
    match failure {
        WorkloadRetryFailure::InputCommit | WorkloadRetryFailure::ExpiredBudget => {
            *backend.unavailable_commit.lock().unwrap() = true
        }
        WorkloadRetryFailure::ResultCommit => {
            *backend.checkpoint_commit_failures.lock().unwrap() = 1
        }
        WorkloadRetryFailure::Acknowledgement => *backend.workload_ack_failures.lock().unwrap() = 1,
    }
    status.requested_checkpoint = Some("retry-workload".into());
    if matches!(failure, WorkloadRetryFailure::ExpiredBudget) {
        let now: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap();
        status.requested_checkpoint_deadline_unix_millis = Some(now + 500);
        *backend.workload_deadline_unix_millis.lock().unwrap() = Some(now + 500);
    }
    send.send(pb::RuntimeEvent {
        event: Some(pb::runtime_event::Event::Hello(pb::RuntimeHello {
            token: execution_token(secret, &identity).unwrap(),
            status_json: serde_json::to_vec(&status).unwrap(),
        })),
    })
    .await
    .unwrap();
    let _commands = client
        .open_control(tokio_stream::wrappers::ReceiverStream::new(receive))
        .await
        .unwrap()
        .into_inner();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let attempts = backend
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.as_str() == "commit:Running")
                .count();
            let failed = match failure {
                WorkloadRetryFailure::InputCommit | WorkloadRetryFailure::ExpiredBudget => {
                    attempts >= 2
                }
                WorkloadRetryFailure::ResultCommit => backend
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| event == "checkpoint-result-retry"),
                WorkloadRetryFailure::Acknowledgement => backend
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| event == "checkpoint-ack-retry"),
            };
            if failed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    if matches!(failure, WorkloadRetryFailure::ExpiredBudget) {
        tokio::time::sleep(Duration::from_millis(800)).await;
        let now: u64 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap();
        status.requested_checkpoint_deadline_unix_millis = Some(now + 60_000);
        *backend.workload_deadline_unix_millis.lock().unwrap() = Some(now + 60_000);
        send.send(pb::RuntimeEvent {
            event: Some(pb::runtime_event::Event::StatusJson(
                serde_json::to_vec(&status).unwrap(),
            )),
        })
        .await
        .unwrap();
    }
    *backend.unavailable_commit.lock().unwrap() = false;
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let complete = backend.events.lock().unwrap().iter().any(|event| {
                event
                    == if matches!(failure, WorkloadRetryFailure::ExpiredBudget) {
                        "checkpoint-error"
                    } else {
                        "checkpoint-ack"
                    }
            });
            if complete {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("pending checkpoint must complete without another status notification");
    let events_before = backend.events.lock().unwrap().clone();
    let completed = events_before.iter().any(|e| e == "checkpoint-ack");
    server.abort();
    drop(send);
    if matches!(failure, WorkloadRetryFailure::ExpiredBudget) {
        assert!(!completed);
        assert!(!events_before
            .iter()
            .any(|event| event == "checkpoint-running"));
        return;
    }
    assert!(completed,"transient commit failure must retry pending workload checkpoint without a new status event: {events_before:?}");
    assert_eq!(
        events_before
            .iter()
            .filter(|event| event.as_str() == "checkpoint-running")
            .count(),
        1
    );
}

#[tokio::test]
async fn busy_controllers_do_not_starve_other_runtime_samples() {
    let (_temp, backend, node, mut spec, assignment) = fixture();
    let node = Arc::new(node);
    spec.resources = Resources {
        cpu_millis: 1,
        memory_bytes: 1,
        disk_bytes: 0,
    };
    let gate = Arc::new(tokio::sync::Notify::new());
    *backend.prepare_gate.lock().unwrap() = Some(gate.clone());
    let mut operations = Vec::new();
    for index in 0..33 {
        let mut spec = spec.clone();
        spec.id = format!("environment-{index:02}");
        let mut assignment = assignment.clone();
        assignment.environment_id = spec.id.clone();
        let handle = node.environment(spec, assignment).unwrap();
        let created = handle.create().await.unwrap();
        if index < 32 {
            operations.push(tokio::spawn(async move {
                handle
                    .snapshot(adxlet::checkpoint::SnapshotRequest {
                        kind: adx_core::snapshots::SnapshotKind::Regular,
                        operation_id: format!("review-{index}"),
                        expected_revision: created.record.revision,
                        names: vec![],
                        timeout_seconds: 60,
                    })
                    .await
            }));
        }
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let count = backend
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.as_str() == "prepare")
                .count();
            if count == 32 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let manager = node.clone();
    let sampling = tokio::spawn(async move { manager.sample_metrics().await });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let sampled_healthy = backend
        .sampled_runtimes
        .lock()
        .unwrap()
        .contains("environment-32-1");
    let sample_count = backend.sampled_runtimes.lock().unwrap().len();
    assert!(sample_count >= 32);
    gate.notify_waiters();
    for operation in operations {
        let _ = tokio::time::timeout(Duration::from_secs(5), operation)
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), sampling)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        sampled_healthy,
        "healthy runtime must be sampled even while other controllers execute checkpoints"
    );
}

#[tokio::test]
async fn workload_commit_failure_retries_without_another_notification() {
    workload_retry_case(WorkloadRetryFailure::InputCommit).await;
}
#[tokio::test]
async fn workload_result_commit_failure_does_not_repeat_capture() {
    workload_retry_case(WorkloadRetryFailure::ResultCommit).await;
}
#[tokio::test]
async fn workload_ack_failure_does_not_repeat_capture() {
    workload_retry_case(WorkloadRetryFailure::Acknowledgement).await;
}
#[tokio::test]
async fn workload_replayed_notification_cannot_extend_original_deadline() {
    workload_retry_case(WorkloadRetryFailure::ExpiredBudget).await;
}

#[tokio::test]
async fn reload_uncertain_cleanup_retains_intent() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    let paused = handle.pause(pause(created.record.revision)).await.unwrap();
    let running = handle.resume(resume(paused.record.revision)).await.unwrap();
    *backend.remove_then_timeout.lock().unwrap() = true;
    assert!(handle
        .reload("review-reload".into(), running.record.revision)
        .await
        .is_err());
    assert!(!backend
        .running
        .lock()
        .unwrap()
        .contains(&running.record.runtime.id));
    {
        let committed = backend.committed_records.lock().unwrap();
        let published = &committed[&running.record.spec.id];
        assert_eq!(published.state, EnvironmentState::Running);
        assert!(
            published
                .observation
                .as_ref()
                .and_then(|o| o.intent)
                .is_some(),
            "uncertain normal stop was published as Running without normal-operation intent"
        );
    }
    *backend.remove_then_timeout.lock().unwrap() = false;
    handle.tick().await.unwrap();
    let committed = backend.committed_records.lock().unwrap();
    let published = &committed[&running.record.spec.id];
    assert_eq!(published.state, EnvironmentState::Failed);
    assert!(published
        .observation
        .as_ref()
        .unwrap()
        .interruption
        .is_none());
}

#[tokio::test]
async fn reload_uncertain_cleanup_does_not_hide_independent_oom() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    let paused = handle.pause(pause(created.record.revision)).await.unwrap();
    let running = handle.resume(resume(paused.record.revision)).await.unwrap();
    *backend.fail_remove.lock().unwrap() = true;
    assert!(handle
        .reload("reload-oom".into(), running.record.revision)
        .await
        .is_err());
    backend.running.lock().unwrap().clear();
    *backend.observed_failure.lock().unwrap() = Some(adx_core::RuntimeFailure {
        reason: adx_core::RuntimeFailureReason::OomKilled,
        exit_code: Some(137),
        message: "backend confirmed OOM".into(),
        probe_failure: None,
    });
    *backend.fail_remove.lock().unwrap() = false;
    handle.tick().await.unwrap();
    let committed = backend.committed_records.lock().unwrap();
    let observation = committed[&running.record.spec.id]
        .observation
        .as_ref()
        .unwrap();
    assert!(observation.intent.is_none());
    assert_eq!(
        observation.interruption.as_ref().unwrap().reason,
        adx_core::observation::InterruptionReason::Oom
    );
}

#[tokio::test]
async fn intended_removal_with_late_exit_is_not_runtime_interruption() {
    let (_temp, backend, node, spec, assignment) = fixture();
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    let paused = handle.pause(pause(created.record.revision)).await.unwrap();
    let running = handle.resume(resume(paused.record.revision)).await.unwrap();
    *backend.remove_then_timeout.lock().unwrap() = true;
    assert!(handle
        .reload("normal-stop".into(), running.record.revision)
        .await
        .is_err());
    assert!(!backend
        .running
        .lock()
        .unwrap()
        .contains(&running.record.runtime.id));
    *backend.remove_then_timeout.lock().unwrap() = false;
    *backend.observed_failure.lock().unwrap() = Some(adx_core::RuntimeFailure {
        reason: adx_core::RuntimeFailureReason::Exited,
        exit_code: Some(137),
        message: "runtime stopped by requested removal".into(),
        probe_failure: None,
    });
    handle.tick().await.unwrap();
    let records = backend.committed_records.lock().unwrap();
    let settled = &records[&running.record.spec.id];
    assert_eq!(settled.state, EnvironmentState::Failed);
    let observation = settled.observation.as_ref().unwrap();
    assert!(
        observation.interruption.is_none(),
        "normal removal created interruption: {:?}",
        observation.interruption
    );
    assert_eq!(
        observation.intent,
        Some(adx_core::observation::OperationIntent::User)
    );
}

#[tokio::test]
async fn explicit_pause_stops_timer_and_next_run_timeout_is_consumed_once() {
    use adx_core::environment_control::{RunExpiryAction, RunPolicy};
    let (_temp, _backend, node, mut spec, assignment) = fixture();
    spec.run_policy = Some(RunPolicy {
        timeout_seconds: 60,
        action: RunExpiryAction::Pause,
    });
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    h.update_run_timeout(5, false).await.unwrap();
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    assert_eq!(h.run_control().await.unwrap().expires_at_unix_ms, 0);
    h.update_run_timeout(120, true).await.unwrap();
    let resumed = h.resume(resume(paused.record.revision)).await.unwrap();
    let current = h.run_control().await.unwrap();
    assert!(current.expires_at_unix_ms >= current.started_at_unix_ms + 119_000);
    let mut request = pause(resumed.record.revision);
    request.operation_id = "pause-timer-second".into();
    let paused = h.pause(request).await.unwrap();
    let mut request = resume(paused.record.revision);
    request.operation_id = "resume-timer-second".into();
    h.resume(request).await.unwrap();
    let current = h.run_control().await.unwrap();
    assert!(current.expires_at_unix_ms <= current.started_at_unix_ms + 60_000);
    h.delete().await.unwrap();
}

#[tokio::test]
async fn snapshot_elapsed_time_counts_against_original_source_deadline() {
    use adx_core::environment_control::{RunExpiryAction, RunPolicy};
    use adxlet::checkpoint::SnapshotRequest;
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.run_policy = Some(RunPolicy {
        timeout_seconds: 60,
        action: RunExpiryAction::Delete,
    });
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    h.update_run_timeout(20, false).await.unwrap();
    let before = h.run_control().await.unwrap();
    *backend.running_checkpoint_delay.lock().unwrap() = Duration::from_millis(100);
    let capture_started = tokio::time::Instant::now();
    h.snapshot(SnapshotRequest {
        kind: adx_core::snapshots::SnapshotKind::Regular,
        operation_id: "snapshot-run-timer".into(),
        expected_revision: created.record.revision,
        names: vec![],
        timeout_seconds: 60,
    })
    .await
    .unwrap();
    assert!(capture_started.elapsed() >= Duration::from_millis(100));
    let after = h.run_control().await.unwrap();
    assert_eq!(after.started_at_unix_ms, before.started_at_unix_ms);
    assert_eq!(after.revision, before.revision);
    // endAt is a wall-clock estimate, so allow rounding/scheduling headroom.
    // Freezing the timer during capture would extend it by at least 100 ms.
    assert!(after.expires_at_unix_ms <= before.expires_at_unix_ms + 50);
    h.delete().await.unwrap();
}

#[tokio::test]
async fn restarted_owner_uses_default_duration_instead_of_old_renewal() {
    use adx_core::environment_control::{RunExpiryAction, RunPolicy};
    let (temp, backend, owner, mut spec, assignment) = fixture();
    spec.run_policy = Some(RunPolicy {
        timeout_seconds: 60,
        action: RunExpiryAction::Delete,
    });
    let h = owner.environment(spec.clone(), assignment.clone()).unwrap();
    let created = h.create().await.unwrap();
    let short = h.update_run_timeout(2, false).await.unwrap();
    drop(h);
    drop(owner);
    tokio::task::yield_now().await;
    let replacement = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    )
    .with_checkpointing(
        Arc::new(LocalCheckpointStore::new(temp.path().into()).unwrap()),
        backend,
    )
    .unwrap();
    replacement.reconcile(vec![created.record]).await.unwrap();
    let h = replacement.environment(spec, assignment).unwrap();
    let restarted = h.run_control().await.unwrap();
    assert!(restarted.expires_at_unix_ms > short.expires_at_unix_ms + 50_000);
    h.delete().await.unwrap();
}

#[tokio::test]
async fn timer_that_expires_during_snapshot_runs_before_next_owner_command() {
    use adx_core::environment_control::{RunExpiryAction, RunPolicy};
    use adxlet::checkpoint::SnapshotRequest;
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.run_policy = Some(RunPolicy {
        timeout_seconds: 60,
        action: RunExpiryAction::Delete,
    });
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    h.update_run_timeout(1, false).await.unwrap();
    *backend.running_checkpoint_delay.lock().unwrap() = Duration::from_millis(1100);
    h.snapshot(SnapshotRequest {
        kind: adx_core::snapshots::SnapshotKind::Regular,
        operation_id: "snapshot-over-deadline".into(),
        expected_revision: created.record.revision,
        names: vec![],
        timeout_seconds: 60,
    })
    .await
    .unwrap();
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Deleted
    );
    assert!(backend.running.lock().unwrap().is_empty());
}

#[tokio::test]
async fn run_timeout_pause_retries_after_reservation_publication_recovers() {
    use adx_core::environment_control::{RunExpiryAction, RunPolicy};
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.run_policy = Some(RunPolicy {
        timeout_seconds: 60,
        action: RunExpiryAction::Pause,
    });
    let handle = node.environment(spec, assignment).unwrap();
    handle.create().await.unwrap();
    *backend.unavailable_state.lock().unwrap() = Some(EnvironmentState::Running);
    *backend.unavailable_commit.lock().unwrap() = true;
    handle.update_run_timeout(0, false).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if backend
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event == "abort")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    *backend.unavailable_commit.lock().unwrap() = false;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if handle
                .sync()
                .await
                .is_ok_and(|result| result.record.state == EnvironmentState::Paused)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let attempts = backend.prepare_ids.lock().unwrap();
    assert!(attempts.len() >= 2);
    // Execd rejects re-preparing a confirmed Aborted operation; retry must use
    // a fresh operation only after the first pre-capture abort was confirmed.
    assert_ne!(attempts[0], attempts[1]);
}

#[tokio::test]
async fn resume_applies_requested_duration_before_delayed_followup_and_consumes_once() {
    use adx_core::environment_control::{RunExpiryAction, RunPolicy};
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.run_policy = Some(RunPolicy {
        timeout_seconds: 1,
        action: RunExpiryAction::Delete,
    });
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    handle.update_run_timeout(60, false).await.unwrap();
    let paused = handle.pause(pause(created.record.revision)).await.unwrap();
    let mut request = resume(paused.record.revision);
    request.run_timeout_seconds = Some(30);
    let resumed = handle.resume(request.clone()).await.unwrap();
    // Simulate a delayed resume response/control RPC, beyond the old Spec default.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let control = handle
        .run_control()
        .await
        .expect("requested duration must protect the restored run");
    assert!(control.expires_at_unix_ms >= control.started_at_unix_ms + 29_000);
    assert_eq!(
        backend.committed_records.lock().unwrap()[&resumed.record.spec.id].state,
        EnvironmentState::Running
    );
    assert_eq!(
        resumed
            .record
            .spec
            .run_policy
            .as_ref()
            .unwrap()
            .timeout_seconds,
        1
    );
    let replay = handle.resume(request).await.unwrap();
    assert_eq!(replay.record.runtime.id, resumed.record.runtime.id);
    handle
        .ensure_running_with_timeout("coalesced-wake".into(), Some(5))
        .await
        .unwrap();
    let after_replay = handle.run_control().await.unwrap();
    assert!(
        after_replay
            .expires_at_unix_ms
            .abs_diff(control.expires_at_unix_ms)
            < 100
    );
    let mut pause_request = pause(replay.record.revision);
    pause_request.operation_id = "pause-duration-next".into();
    let paused = handle.pause(pause_request).await.unwrap();
    let mut next = resume(paused.record.revision);
    next.operation_id = "resume-duration-next".into();
    handle.resume(next).await.unwrap();
    let next_control = handle.run_control().await.unwrap();
    assert!(next_control.expires_at_unix_ms <= next_control.started_at_unix_ms + 1_000);
    handle.delete().await.unwrap();
}

#[tokio::test]
async fn snapshot_clone_keeps_probe_failure_handling_without_restarting_entrypoint() {
    use adx_core::{execution::*, probes::*, snapshots::*};
    let (_temp, backend, node, mut spec, mut assignment) = fixture();
    spec.execution.inherit_entrypoint = true;
    spec.execution.rootfs = Some(Rootfs {
        readonly: false,
        source: StorageSource::Image(spec.image.clone()),
    });
    spec.execution.healthcheck_probe = Some(ProbeSpec {
        action: ProbeAction::TcpSocket { port: 8080 },
        initial_delay_seconds: 0,
        period_seconds: 1,
        timeout_seconds: 1,
        failure_threshold: 3,
    });
    let source = node.environment(spec, assignment.clone()).unwrap();
    let running = source.create().await.unwrap();
    let mut snapshot = source
        .snapshot(adxlet::checkpoint::SnapshotRequest {
            kind: SnapshotKind::Regular,
            operation_id: "probed-source".into(),
            expected_revision: running.record.revision,
            names: vec![],
            timeout_seconds: 60,
        })
        .await
        .unwrap()
        .snapshot;
    source.delete().await.unwrap();
    let reference = Reference::Restore {
        environment_id: "clone".into(),
    };
    snapshot.acquire("tenant", reference.clone()).unwrap();
    // Coordinator inheritance is covered in rpc::cloning; feed its resulting
    // captured geometry into the node's real create-from-snapshot path.
    let mut restored_spec = snapshot.template.clone();
    restored_spec.id = "clone".into();
    restored_spec.snapshot_id = Some(snapshot.id.clone());
    assignment.environment_id = restored_spec.id.clone();
    let clone = node.environment(restored_spec, assignment).unwrap();
    backend.events.lock().unwrap().clear();
    let restored = clone.create_from_snapshot(snapshot.clone()).await.unwrap();
    assert_eq!(restored.record.state, EnvironmentState::Running);
    assert_eq!(
        restored.record.spec.execution.healthcheck_probe,
        snapshot.template.execution.healthcheck_probe
    );
    assert!(backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event == "restore"));
    assert!(!backend
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event == "start"));
    let failure = ProbeFailure {
        kind: ProbeFailureKind::HealthcheckFailed,
        message: "restored service unhealthy".into(),
        consecutive_failures: 3,
        exit_code: Some(7),
        http_status: None,
    };
    *backend.probe_failure.lock().unwrap() = Some(failure.clone());
    clone.tick().await.unwrap();
    let failed = clone.sync().await.unwrap().record;
    assert_eq!(failed.state, EnvironmentState::Failed);
    assert_eq!(failed.failure, Some(failure.into()));
    assert!(backend.running.lock().unwrap().is_empty());
    assert_eq!(node.used(), Resources::default());
    clone.delete().await.unwrap();
    snapshot.release(&reference).unwrap();
    snapshot.delete("tenant").unwrap();
    node.collect_snapshot(&snapshot, false).await.unwrap();
}

#[tokio::test]
async fn upgrade_quiescing_waits_for_snapshot_publication_and_rejects_new_capture() {
    use adxlet::checkpoint::SnapshotRequest;
    let (state, backend, node, spec, assignment) = fixture();
    let node = node
        .with_upgrade_control(state.path().join("upgrade.json"), "session".into())
        .unwrap();
    let mut control = adxlet::upgrade::NodeControl {
        task_id: "upgrade".into(),
        epoch: 1,
        node_session_id: "session".into(),
        revision: 1,
        phase: "verified".into(),
        ..Default::default()
    };
    node.apply_upgrade_control(control.clone()).await.unwrap();
    let handle = node.environment(spec, assignment).unwrap();
    let created = handle.create().await.unwrap();
    let request = SnapshotRequest {
        kind: adx_core::snapshots::SnapshotKind::Regular,
        operation_id: "accepted-capture".into(),
        expected_revision: created.record.revision,
        names: vec![],
        timeout_seconds: 60,
    };
    let gate = Arc::new(tokio::sync::Notify::new());
    *backend.snapshot_publish_gate.lock().unwrap() = Some(gate.clone());
    let snapshot = {
        let handle = handle.clone();
        let request = request.clone();
        tokio::spawn(async move { handle.snapshot(request).await })
    };
    backend.snapshot_publish_entered.notified().await;
    control.revision += 1;
    control.phase = "quiescing".into();
    let mut closing = Box::pin(node.apply_upgrade_control(control));
    assert!(futures_util::poll!(closing.as_mut()).is_pending());
    gate.notify_one();
    snapshot.await.unwrap().unwrap();
    let inspection = closing.await.unwrap();
    assert!(!inspection.strict_empty);
    assert_eq!(inspection.controllers, 1);
    *backend.snapshot_publish_gate.lock().unwrap() = None;
    let mut rejected = request;
    rejected.operation_id = "revoked-capture".into();
    rejected.expected_revision = handle.sync().await.unwrap().record.revision;
    let before = backend.events.lock().unwrap().clone();
    assert!(handle.snapshot(rejected).await.is_err());
    assert_eq!(*backend.events.lock().unwrap(), before);
}
