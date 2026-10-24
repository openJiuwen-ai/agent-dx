#[path = "common/authority.rs"]
mod authority;

use adx_core::{EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error, Result};
use adxlet::{
    checkpoint::{CheckpointCooperation, LocalCheckpointStore, PauseRequest},
    RuntimeDriver, RuntimeObservation,
};
use async_trait::async_trait;
use opentelemetry::trace::Status;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

#[derive(Default)]
struct Backend {
    actual: Mutex<Vec<RuntimeObservation>>,
    fail_remove: AtomicBool,
}
#[async_trait]
impl RuntimeDriver for Backend {
    async fn start(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        self.actual.lock().unwrap().push(RuntimeObservation {
            environment_id: spec.id.clone(),
            tenant_id: spec.tenant_id.clone(),
            runtime_id: id.into(),
            generation,
            running: true,
        });
        Ok("10.0.0.2".parse().unwrap())
    }
    async fn inventory(&self) -> Result<Vec<RuntimeObservation>> {
        Ok(self.actual.lock().unwrap().clone())
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        Ok(self
            .actual
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.runtime_id == id))
    }
    async fn remove(&self, id: &str) -> Result<()> {
        if self.fail_remove.load(Ordering::Relaxed) {
            return Err(Error::Unavailable("test cleanup failure".into()));
        }
        self.actual.lock().unwrap().retain(|r| r.runtime_id != id);
        Ok(())
    }
    async fn checkpoint_supported(&self, _: &str) -> Result<()> {
        Ok(())
    }
    async fn checkpoint(
        &self,
        id: &str,
        path: &std::path::Path,
        _: std::time::Duration,
    ) -> Result<()> {
        std::fs::write(path.join("state"), b"checkpoint").unwrap();
        self.remove(id).await
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

#[tokio::test]
async fn authority_cycles_are_silent_until_cleanup_failure_or_state_change() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    let backend = Arc::new(Backend::default());
    let storage = tempfile::tempdir().unwrap();
    let node = authority::manager(backend.clone())
        .with_checkpointing(
            Arc::new(LocalCheckpointStore::new(storage.path().to_owned()).unwrap()),
            backend.clone(),
        )
        .unwrap();
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
        "id":"trace-authority", "tenant_id":"tenant", "image":"image", "runtime_class":"runsc", "priority":0,
        "resources":{"cpu_millis":1,"memory_bytes":1024,"disk_bytes":0}
    })).unwrap();
    let handle = node
        .environment(
            spec,
            adx_core::Assignment {
                environment_id: "trace-authority".into(),
                node_id: "n".into(),
                shard_id: 0,
                generation: 1,
                devices: vec![],
            },
        )
        .unwrap();
    let running = handle.create().await.unwrap().record;
    *authority.authority.records.lock().unwrap() = vec![running.clone().try_into().unwrap()];
    provider.force_flush().unwrap();
    let before = exporter.get_finished_spans().unwrap().len();
    for _ in 0..3 {
        authority.cycle(&node).await.unwrap();
        node.monitor_environments().await.unwrap();
        handle.expire_checkpoint(0).await.unwrap();
    }
    provider.force_flush().unwrap();
    assert_eq!(
        exporter.get_finished_spans().unwrap().len(),
        before,
        "healthy Running cycles must be silent"
    );

    let paused = handle
        .pause(PauseRequest {
            operation_id: "pause-trace".into(),
            expected_revision: running.revision,
            retention: adx_core::lifecycle::RetentionPolicy::Forever,
            timeout_seconds: 5,
        })
        .await
        .unwrap()
        .record;
    assert_eq!(paused.state, EnvironmentState::Paused);
    *authority.authority.records.lock().unwrap() = vec![paused.clone().try_into().unwrap()];
    provider.force_flush().unwrap();
    let after_pause = exporter.get_finished_spans().unwrap().len();
    assert!(
        after_pause > before,
        "explicit state changes retain lifecycle spans"
    );
    for _ in 0..3 {
        authority.cycle(&node).await.unwrap();
        node.monitor_environments().await.unwrap();
        handle.expire_checkpoint(0).await.unwrap();
    }
    provider.force_flush().unwrap();
    assert_eq!(
        exporter.get_finished_spans().unwrap().len(),
        after_pause,
        "healthy Paused cycles must be silent"
    );

    // A newly committed terminal record requires real cleanup, independent of a backend scan.
    let mut deleted = paused;
    deleted.state = EnvironmentState::Deleted;
    deleted.revision += 1;
    deleted.checkpoint = None;
    *authority.authority.records.lock().unwrap() = vec![deleted.try_into().unwrap()];
    backend.fail_remove.store(true, Ordering::Relaxed);
    assert!(authority.cycle(&node).await.is_err());
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let failed = spans[after_pause..]
        .iter()
        .find(|s| s.name == "environment.authority_reconcile")
        .expect("failed cleanup must be traced");
    assert!(spans[after_pause..].iter().any(|s| {
        s.span_context.trace_id() == failed.span_context.trace_id()
            && matches!(s.status, Status::Error { .. })
    }));
    assert!(failed
        .attributes
        .iter()
        .any(|kv| kv.key.as_str() == "environment.id" && kv.value.as_str() == "trace-authority"));
    let after_failure = spans.len();

    backend.fail_remove.store(false, Ordering::Relaxed);
    authority.cycle(&node).await.unwrap();
    assert!(backend.actual.lock().unwrap().is_empty());
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    assert!(spans[after_failure..]
        .iter()
        .any(|s| s.name == "environment.authority_reconcile"));
    let after_cleanup = spans.len();
    for _ in 0..3 {
        authority.cycle(&node).await.unwrap();
    }
    provider.force_flush().unwrap();
    assert_eq!(
        exporter.get_finished_spans().unwrap().len(),
        after_cleanup,
        "normal cycles must become silent again after recovery"
    );
    let after_deletion = after_cleanup;
    for _ in 0..3 {
        authority.cycle(&node).await.unwrap();
    }
    provider.force_flush().unwrap();
    assert_eq!(exporter.get_finished_spans().unwrap().len(), after_deletion);
    provider.shutdown().unwrap();
}
