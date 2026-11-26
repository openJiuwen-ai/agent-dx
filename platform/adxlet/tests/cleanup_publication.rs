#[allow(dead_code)]
#[path = "common/authority.rs"]
mod authority;

use adx_core::snapshots::{Reference, Snapshot};
use adx_core::{Assignment, EnvironmentRecord, EnvironmentSpec, Error, Resources, Result};
use adxlet::checkpoint::{CheckpointAccess, CheckpointCooperation, LocalCheckpointStore};
use adxlet::journal::JournalSink;
use adxlet::{Adxlet, Durability, Readiness, Routes, RuntimeDriver, RuntimeObservation, StateSink};
use async_trait::async_trait;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

#[derive(Default)]
struct Backend {
    actual: Mutex<Vec<RuntimeObservation>>,
    fail_readiness: AtomicBool,
    remove_fails: AtomicBool,
}

#[async_trait]
impl RuntimeDriver for Backend {
    async fn restore_from(
        &self,
        _: &EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
        _: &std::path::Path,
        _: Option<&adx_core::runtime::RuntimeIdentity>,
    ) -> Result<std::net::IpAddr> {
        Err(Error::Unavailable(
            "injected snapshot restore outcome unavailable".into(),
        ))
    }

    async fn start(
        &self,
        spec: &EnvironmentSpec,
        runtime_id: &str,
        generation: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        self.actual.lock().unwrap().push(RuntimeObservation {
            environment_id: spec.id.clone(),
            runtime_id: runtime_id.into(),
            generation,
            tenant_id: spec.tenant_id.clone(),
            running: true,
        });
        Ok("10.0.0.2".parse().unwrap())
    }

    async fn is_running(&self, runtime_id: &str) -> Result<bool> {
        Ok(self
            .actual
            .lock()
            .unwrap()
            .iter()
            .any(|runtime| runtime.runtime_id == runtime_id && runtime.running))
    }

    async fn inventory(&self) -> Result<Vec<RuntimeObservation>> {
        Ok(self.actual.lock().unwrap().clone())
    }

    async fn remove(&self, runtime_id: &str) -> Result<()> {
        if self.remove_fails.load(Ordering::SeqCst) {
            return Err(Error::Unavailable(
                "injected backend cleanup unavailable".into(),
            ));
        }
        self.actual
            .lock()
            .unwrap()
            .retain(|runtime| runtime.runtime_id != runtime_id);
        Ok(())
    }
}

#[async_trait]
impl Readiness for Backend {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        if self.fail_readiness.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("injected readiness failure".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl CheckpointCooperation for Backend {
    async fn prepare(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        Ok(())
    }

    async fn abort_unstarted(&self, _: &EnvironmentRecord, _: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Routes for Backend {
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }

    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct Upstream {
    offline: AtomicBool,
    records: Mutex<std::collections::BTreeMap<String, EnvironmentRecord>>,
}

#[async_trait]
impl StateSink for Upstream {
    async fn prepare_execution(
        &self,
        record: &EnvironmentRecord,
        _: &str,
    ) -> Result<EnvironmentSpec> {
        Ok(record.spec.clone())
    }

    async fn has_current_assignment(&self, _: &EnvironmentRecord) -> Result<bool> {
        Ok(true)
    }

    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability> {
        if self.offline.load(Ordering::SeqCst) {
            return Err(Error::Unavailable(
                "injected Coordinator publication outage".into(),
            ));
        }
        self.records
            .lock()
            .unwrap()
            .insert(record.spec.id.clone(), record.clone());
        Ok(Durability::Published)
    }
}

fn spec(id: &str) -> EnvironmentSpec {
    serde_json::from_value(serde_json::json!({
        "id": id,
        "tenant_id": "t",
        "image": "image",
        "runtime_class": "runc",
        "priority": 0,
        "resources": {"cpu_millis": 1, "memory_bytes": 1, "disk_bytes": 0}
    }))
    .unwrap()
}

fn assignment(id: &str) -> Assignment {
    Assignment {
        environment_id: id.into(),
        node_id: "n".into(),
        shard_id: 0,
        generation: 7,
        devices: vec![],
    }
}

#[tokio::test]
async fn failed_cleanup_republishes_changed_evidence_at_a_new_revision() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let upstream = Arc::new(Upstream::default());
    let journal = Arc::new(JournalSink::new(
        dir.path().join("journal.sqlite"),
        upstream.clone(),
    ));
    let storage = Arc::new(LocalCheckpointStore::new(dir.path().join("checkpoints")).unwrap());
    let node = Adxlet::new(
        "n".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        journal.clone(),
    )
    .with_checkpointing(storage.clone(), backend.clone())
    .unwrap();
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
    authority
        .cycle_with_journal(&node, Some(&journal))
        .await
        .unwrap();

    let healthy = node
        .environment(spec("healthy"), assignment("healthy"))
        .unwrap();
    healthy.create().await.unwrap();

    let capture = storage.begin_capture().await.unwrap();
    std::fs::write(capture.join("state"), b"snapshot fixture").unwrap();
    let artifact = storage.seal(&capture).await.unwrap();
    let mut snapshot = Snapshot::new(
        "snapshot".into(),
        vec![],
        spec("source"),
        "n".into(),
        "source-7".into(),
        artifact,
    )
    .unwrap();
    snapshot.references.insert(Reference::Restore {
        environment_id: "busy".into(),
    });
    let mut value = spec("busy");
    value.snapshot_id = Some(snapshot.id.clone());
    let pending: EnvironmentRecord = serde_json::from_value(serde_json::json!({
        "spec": value,
        "assignment": assignment("busy"),
        "state": "Pending",
        "revision": 0,
        "runtime": {"id": "busy-7", "ip": null},
        "resources_held": true
    }))
    .unwrap();
    let busy = node.environment(value, assignment("busy")).unwrap();

    upstream.offline.store(true, Ordering::SeqCst);
    backend.remove_fails.store(true, Ordering::SeqCst);
    assert!(busy.create_from_snapshot(snapshot).await.is_err());
    let before = busy.sync().await.unwrap();
    assert_eq!(before.durability, Durability::Journaled);
    assert!(!before.record.resources_held);
    assert!(!before.record.execution_artifacts.is_empty());

    backend.remove_fails.store(false, Ordering::SeqCst);
    busy.tick()
        .await
        .expect("cleanup must publish changed evidence, not conflict");

    upstream.offline.store(false, Ordering::SeqCst);
    upstream
        .records
        .lock()
        .unwrap()
        .insert("busy".into(), pending);
    authority.registration.reconciling = true;
    for _ in 0..3 {
        *authority.authority.records.lock().unwrap() = upstream
            .records
            .lock()
            .unwrap()
            .values()
            .cloned()
            .map(|record| record.try_into().unwrap())
            .collect();
        let outcome = authority.cycle_with_journal(&node, Some(&journal)).await;
        assert!(outcome
            .as_ref()
            .err()
            .is_none_or(|error| error.message() != "identity or version conflict"));
        assert_eq!(journal.pending().await.unwrap(), 0);
        assert!(node.accepting_allocations());
        assert!(authority.registration.accepting_allocations);
        healthy.sync().await.unwrap();
    }

    let fresh = node
        .environment(spec("fresh"), assignment("fresh"))
        .unwrap();
    fresh.create().await.unwrap();
    let published = busy.sync().await.unwrap();
    assert!(published.record.revision > before.record.revision);
    assert_eq!(
        published.record.revision,
        busy.sync().await.unwrap().record.revision,
        "idempotent cleanup must not advance revision repeatedly"
    );
}

#[tokio::test]
async fn start_failure_publishes_volume_stop_proof_at_a_new_revision() {
    let dir = tempfile::tempdir().unwrap();
    let backend = Arc::new(Backend::default());
    let upstream = Arc::new(Upstream::default());
    let journal = Arc::new(JournalSink::new(
        dir.path().join("journal.sqlite"),
        upstream.clone(),
    ));
    let node = Adxlet::new(
        "n".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        journal.clone(),
    );
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
    authority
        .cycle_with_journal(&node, Some(&journal))
        .await
        .unwrap();

    let mut value = spec("busy");
    value.execution.mounts.push(adx_core::execution::Mount {
        kind: "bind".into(),
        target: "/data".into(),
        options: vec![],
        source: adx_core::execution::MountSource::Volume(adx_core::volumes::VolumeRef {
            id: "data".into(),
            uid: Some("uid".into()),
        }),
        create_if_missing: false,
        create_options: None,
    });
    let busy = node.environment(value, assignment("busy")).unwrap();
    backend.fail_readiness.store(true, Ordering::SeqCst);

    let error = busy
        .create()
        .await
        .expect_err("readiness failure must fail create");
    assert!(
        !error.to_string().contains("commit"),
        "cleanup stop proof must publish without a Journal revision conflict: {error}"
    );
    assert_eq!(journal.pending().await.unwrap(), 0);
    let published = busy.sync().await.unwrap();
    assert_eq!(published.durability, Durability::Published);
    assert_eq!(node.used().cpu_millis, 0);
    let revision = published.record.revision;
    for _ in 0..3 {
        busy.tick()
            .await
            .expect("repeated cleanup ticks must stay published");
    }
    assert_eq!(
        busy.sync().await.unwrap().record.revision,
        revision,
        "a published stop proof must not advance revision on every tick"
    );
    assert_eq!(journal.pending().await.unwrap(), 0);
}
