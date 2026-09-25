use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error, Resources, Result,
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

#[derive(Default)]
struct Backend {
    snapshots: Mutex<BTreeMap<String, adx_core::snapshots::Snapshot>>,
    fail_snapshot_publish: Mutex<bool>,
    running: Mutex<BTreeSet<String>>,
    events: Mutex<Vec<String>>,
    unavailable_commit: Mutex<bool>,
    fail_restore: Mutex<bool>,
    fail_start: Mutex<bool>,
    activity: Mutex<Option<(u64, u64)>>,
    fail_prepare: Mutex<bool>,
    fail_abort: Mutex<bool>,
    fail_remove: Mutex<bool>,
    checkpoint_leaves_running: Mutex<bool>,
    workload_request: Mutex<Option<String>>,
    fail_workload_checkpoint: Mutex<bool>,
    workload_prepared: Mutex<bool>,
}
impl Backend {
    fn event(&self, e: &str) {
        self.events.lock().unwrap().push(e.into());
    }
}
#[async_trait]
impl RuntimeDriver for Backend {
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
    async fn remove(&self, id: &str) -> Result<()> {
        self.event("remove");
        if *self.fail_remove.lock().unwrap() {
            return Err(Error::Unavailable("delete outcome unknown".into()));
        }
        self.running.lock().unwrap().remove(id);
        Ok(())
    }
    async fn checkpoint_supported(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn checkpoint(&self, id: &str, path: &Path, _: Duration) -> Result<()> {
        self.event("checkpoint");
        std::fs::write(path.join("memory"), b"actual saved memory").unwrap();
        if !*self.checkpoint_leaves_running.lock().unwrap() {
            self.running.lock().unwrap().remove(id);
        }
        Ok(())
    }
    async fn checkpoint_running(&self, id: &str, path: &Path, _: Duration) -> Result<()> {
        self.event("checkpoint-running");
        assert!(self.running.lock().unwrap().contains(id));
        if *self.fail_workload_checkpoint.lock().unwrap() {
            return Err(Error::Unavailable("reply lost".into()));
        }
        std::fs::write(path.join("memory"), b"actual saved memory").unwrap();
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
    async fn workload_status(
        &self,
        record: &EnvironmentRecord,
    ) -> Result<Option<adx_core::runtime::RuntimeStatus>> {
        Ok(self.workload_request.lock().unwrap().clone().map(|id| {
            adx_core::runtime::RuntimeStatus {
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
        self.event(if error.is_none() {
            "checkpoint-ack"
        } else {
            "checkpoint-error"
        });
        self.workload_request.lock().unwrap().take();
        Ok(())
    }
    async fn prepare(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        self.event("prepare");
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
    async fn activity(&self, _: &EnvironmentRecord) -> Result<(String, u64, u64)> {
        Ok(("proxy".into(), 1, 0))
    }

    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
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
    async fn commit(&self, r: &EnvironmentRecord) -> Result<Durability> {
        self.event(&format!("commit:{:?}", r.state));
        if *self.unavailable_commit.lock().unwrap() {
            Err(Error::Unavailable("Coordinator unavailable".into()))
        } else {
            Ok(Durability::Published)
        }
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
        sandbox: Default::default(),
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
        ttl_seconds: 600,
        timeout_seconds: 60,
    }
}
fn resume(revision: u64) -> ResumeRequest {
    ResumeRequest {
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
    backend.events.lock().unwrap().clear();
    let paused = h.pause(pause(created.record.revision)).await.unwrap();
    assert_eq!(paused.record.state, EnvironmentState::Paused);
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
        ["prepare", "retire", "checkpoint", "remove", "commit:Paused"]
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
        &backend.events.lock().unwrap()[5..],
        ["restore", "ready", "activate", "commit:Running"]
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
    h.expire_checkpoint(cp.expires_at_unix_seconds)
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
    assert_eq!(r.record.checkpoint.as_ref().unwrap().id, cp.id);
    let mut request = pause(r.record.revision);
    request.operation_id = "pause-c".into();
    let paused = h.pause(request).await.unwrap();
    h.expire_checkpoint(paused.record.checkpoint.unwrap().expires_at_unix_seconds)
        .await
        .unwrap();
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Deleted
    );
    assert_eq!(node.used(), Resources::default());
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
impl adxlet::checkpoint::CheckpointStore for UnavailablePublication {
    async fn allocate(&self) -> Result<std::path::PathBuf> {
        self.0.allocate().await
    }
    async fn discard_staged(&self, p: &Path) -> Result<()> {
        self.0.discard_staged(p).await
    }
    async fn retain_staged(&self, p: &Path) -> Result<adx_core::CheckpointArtifact> {
        self.0.publish(p).await
    }
    async fn publish(&self, _: &Path) -> Result<adx_core::CheckpointArtifact> {
        Err(Error::Unavailable("storage unavailable".into()))
    }
    async fn materialize(
        &self,
        a: &adx_core::CheckpointArtifact,
    ) -> Result<adxlet::checkpoint::MaterializedCheckpoint> {
        self.0.materialize(a).await
    }
    async fn remove(&self, a: &adx_core::CheckpointArtifact) -> Result<()> {
        self.0.remove(a).await
    }
}

struct SharedPublication {
    local: LocalCheckpointStore,
    backend: Arc<Backend>,
}
#[async_trait]
impl adxlet::checkpoint::CheckpointStore for SharedPublication {
    async fn allocate(&self) -> Result<std::path::PathBuf> {
        self.local.allocate().await
    }
    async fn discard_staged(&self, path: &Path) -> Result<()> {
        self.local.discard_staged(path).await
    }
    async fn retain_staged(&self, path: &Path) -> Result<adx_core::CheckpointArtifact> {
        self.local.publish(path).await
    }
    async fn publish(&self, path: &Path) -> Result<adx_core::CheckpointArtifact> {
        self.backend.event("checkpoint-publish");
        let mut artifact = self.local.publish(path).await?;
        artifact.storage = "shared".into();
        Ok(artifact)
    }
    async fn materialize(
        &self,
        artifact: &adx_core::CheckpointArtifact,
    ) -> Result<adxlet::checkpoint::MaterializedCheckpoint> {
        let mut local = artifact.clone();
        local.storage = "local".into();
        self.local.materialize(&local).await
    }
    async fn remove(&self, artifact: &adx_core::CheckpointArtifact) -> Result<()> {
        let mut local = artifact.clone();
        local.storage = "local".into();
        self.local.remove(&local).await
    }
    async fn committed(&self, _: &adx_core::CheckpointArtifact) -> Result<()> {
        self.backend.event("checkpoint-store-committed");
        Ok(())
    }
}
#[tokio::test]
async fn failed_publication_restores_local_staging_and_reports_pause_failure() {
    let (temp, backend, _node, spec, assignment) = fixture();
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
    h.delete().await.unwrap();
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
    h.tick().await.unwrap();
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Deleted
    );
    assert_eq!(node.used(), Resources::default());
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
    spec.sandbox.failover = true;
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
    spec.sandbox.failover = true;
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
    use adxlet::checkpoint::CheckpointStore;
    let (temp, _, node, spec, _) = fixture();
    let store = LocalCheckpointStore::new(temp.path().into()).unwrap();
    let path = store.allocate().await.unwrap();
    std::fs::write(path.join("memory"), b"snapshot memory").unwrap();
    let artifact = store.publish(&path).await.unwrap();
    let mut snapshot = Snapshot::new(
        "saved".into(),
        vec![],
        spec,
        "node".into(),
        "environment-1".into(),
        artifact,
    )
    .unwrap();
    let orphan = store.allocate().await.unwrap();
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
    snapshot.delete("tenant").unwrap();
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
    use adxlet::checkpoint::CheckpointStore;
    let (temp, _, node, spec, _) = fixture();
    let store = LocalCheckpointStore::new(temp.path().into()).unwrap();
    let path = store.allocate().await.unwrap();
    std::fs::write(path.join("memory"), b"saved memory").unwrap();
    let mut snapshot = Snapshot::new(
        "saved".into(),
        vec![],
        spec,
        "node".into(),
        "environment-1".into(),
        store.publish(&path).await.unwrap(),
    )
    .unwrap();
    assert!(node.collect_snapshot(&snapshot).await.is_err());
    let reference = Reference::Restore {
        environment_id: "clone".into(),
    };
    snapshot.acquire("tenant", reference.clone()).unwrap();
    snapshot.delete("tenant").unwrap();
    assert!(node.collect_snapshot(&snapshot).await.is_err());
    assert!(path.exists());
    snapshot.release(&reference).unwrap();
    let mut foreign = snapshot.clone();
    foreign.source_node_id = "other".into();
    assert!(node.collect_snapshot(&foreign).await.is_err());
    node.collect_snapshot(&snapshot).await.unwrap();
    assert!(!path.exists());
    node.collect_snapshot(&snapshot).await.unwrap();
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
    ) -> Result<adx_core::snapshots::Snapshot> {
        if *self.fail_snapshot_publish.lock().unwrap() {
            return Err(Error::Unavailable("catalog unavailable".into()));
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
        operation_id: "snapshot-a".into(),
        expected_revision: created.record.revision,
        names: vec!["base".into()],
        timeout_seconds: 60,
    };
    let saved = h.snapshot(request.clone()).await.unwrap();
    assert_eq!(saved.environment.record.state, EnvironmentState::Running);
    assert_eq!(saved.environment.durability, Durability::Published);
    assert_ne!(
        saved.snapshot.artifact,
        saved
            .environment
            .record
            .checkpoint
            .as_ref()
            .unwrap()
            .artifact
    );
    let replay = h.snapshot(request.clone()).await.unwrap();
    assert_eq!(saved.snapshot, replay.snapshot);
    assert_eq!(
        backend
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.as_str() == "checkpoint")
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

#[tokio::test]
async fn snapshot_publication_failure_attempts_to_resume_source() {
    use adxlet::checkpoint::SnapshotRequest;
    let (_temp, backend, node, spec, assignment) = fixture();
    let h = node.environment(spec, assignment).unwrap();
    let created = h.create().await.unwrap();
    *backend.fail_snapshot_publish.lock().unwrap() = true;
    assert!(h
        .snapshot(SnapshotRequest {
            operation_id: "snapshot-fail".into(),
            expected_revision: created.record.revision,
            names: vec![],
            timeout_seconds: 60
        })
        .await
        .is_err());
    assert_eq!(
        h.sync().await.unwrap().record.state,
        EnvironmentState::Running
    );
    assert_eq!(backend.running.lock().unwrap().len(), 1);
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
            operation_id: "snapshot-outage".into(),
            expected_revision: created.record.revision,
            names: vec![],
            timeout_seconds: 60
        })
        .await
        .is_err());
    assert_eq!(backend.running.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn clone_owns_its_checkpoint_after_source_and_snapshot_are_deleted() {
    use adx_core::snapshots::Reference;
    let (_temp, backend, node, spec, assignment) = fixture();
    let source = node.environment(spec.clone(), assignment.clone()).unwrap();
    let running = source.create().await.unwrap();
    let saved = source
        .snapshot(adxlet::checkpoint::SnapshotRequest {
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
    let cp = created.record.checkpoint.as_ref().unwrap();
    assert_ne!(cp.artifact, snapshot.artifact);
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
    snapshot
        .release(&Reference::Restore {
            environment_id: "clone".into(),
        })
        .unwrap();
    snapshot.delete("tenant").unwrap();
    node.collect_snapshot(&snapshot).await.unwrap();
    assert!(!Path::new(&snapshot.artifact.location).exists());
    assert!(Path::new(&cp.artifact.location).exists());
    assert_eq!(
        clone
            .create_from_snapshot(snapshot.clone())
            .await
            .unwrap_err(),
        Error::Conflict
    );
    let paused = clone.pause(pause(created.record.revision)).await.unwrap();
    assert!(paused.record.checkpoint.as_ref().unwrap().origin.is_none());
    clone.resume(resume(paused.record.revision)).await.unwrap();
    clone.delete().await.unwrap();
    assert!(!Path::new(&cp.artifact.location).exists());
    assert!(backend.running.lock().unwrap().is_empty());
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
async fn recovery_with_unusable_checkpoint_settles_failed_without_image_start() {
    let (_temp, backend, node, spec, mut assignment) = fixture();
    assignment.generation = 2;
    let record = EnvironmentRecord {
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
            expires_at_unix_seconds: u64::MAX,
            origin: Some(adx_core::runtime::RuntimeIdentity {
                environment_id: "environment".into(),
                runtime_id: "environment-1".into(),
                ownership_generation: 1,
            }),
            artifact: adx_core::CheckpointArtifact {
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

#[tokio::test]
async fn recovery_retries_publication_without_repeating_backend_restore() {
    use adxlet::checkpoint::{CheckpointStore, ObjectCheckpointStore};
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
    let path = store.allocate().await.unwrap();
    std::fs::write(path.join("memory"), b"actual saved memory").unwrap();
    let artifact = store.publish(&path).await.unwrap();
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
    let seed = EnvironmentRecord {
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
            expires_at_unix_seconds: u64::MAX,
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
    *backend.unavailable_commit.lock().unwrap() = true;
    assert!(node.recover_environment(seed.clone()).await.is_err());
    assert_eq!(backend.running.lock().unwrap().len(), 1);
    *backend.unavailable_commit.lock().unwrap() = false;
    let result = node.recover_environment(seed.clone()).await.unwrap();
    assert_eq!(result.durability, Durability::Published);
    assert_eq!(result.record.state, EnvironmentState::Running);
    assert_eq!(
        node.recover_environment(seed).await.unwrap().record,
        result.record
    );
    let events = backend.events.lock().unwrap();
    assert_eq!(events.iter().filter(|e| *e == "restore").count(), 1);
    assert!(!events.iter().any(|e| e == "start"));
}

#[tokio::test]
async fn workload_checkpoint_keeps_execution_and_commits_before_acknowledging() {
    let (_temp, backend, node, mut spec, assignment) = fixture();
    spec.sandbox.failover = true;
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
async fn workload_checkpoint_uses_configured_shared_store_before_acknowledging() {
    let (temp, backend, _node, mut spec, assignment) = fixture();
    spec.sandbox.failover = true;
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
    spec.sandbox.failover = true;
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
