use adx_core::{Assignment, EnvironmentRecord, EnvironmentSpec, Resources, Result};
use adxlet::{Adxlet, Durability, Readiness, Routes, RuntimeDriver, StateSink};
use async_trait::async_trait;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[derive(Default)]
struct Backend(AtomicBool, AtomicBool);
#[async_trait]
impl RuntimeDriver for Backend {
    async fn is_running(&self, _: &str) -> Result<bool> {
        if self.1.load(Ordering::Relaxed) {
            return Err(adx_core::Error::Unavailable(
                "observation unavailable".into(),
            ));
        }
        Ok(self.0.load(Ordering::Relaxed))
    }
    async fn start(
        &self,
        _: &EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        self.0.store(true, Ordering::Relaxed);
        Ok("10.0.0.2".parse().unwrap())
    }
    async fn remove(&self, _: &str) -> Result<()> {
        self.0.store(false, Ordering::Relaxed);
        Ok(())
    }
    async fn stats(&self, _: &str) -> Result<adxlet::metrics::RuntimeUsage> {
        Ok(adxlet::metrics::RuntimeUsage {
            cpu_usage_ns: 1_000_000,
            memory_usage_bytes: 100,
            memory_limit_bytes: 1024,
        })
    }
}
#[async_trait]
impl Readiness for Backend {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
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
#[async_trait]
impl StateSink for Backend {
    async fn commit(&self, _: &EnvironmentRecord) -> Result<Durability> {
        Ok(Durability::Published)
    }
}

#[tokio::test]
async fn passive_observation_is_silent_but_real_failure_keeps_lifecycle_spans() {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    let backend = Arc::new(Backend::default());
    let node = Adxlet::new(
        "node".into(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
        backend.clone(),
    );
    node.update_capacity(
        Resources {
            cpu_millis: 100,
            memory_bytes: 4096,
            disk_bytes: 0,
        },
        std::time::Duration::from_secs(30),
    )
    .unwrap();
    let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
        "id":"trace-test", "tenant_id":"tenant", "image":"image", "runtime_class":"runsc", "priority":0,
        "resources":{"cpu_millis":1,"memory_bytes":1024,"disk_bytes":0}
    })).unwrap();
    let handle = node
        .environment(
            spec,
            Assignment {
                environment_id: "trace-test".into(),
                node_id: "node".into(),
                shard_id: 0,
                generation: 1,
                devices: vec![],
            },
        )
        .unwrap();
    handle.create().await.unwrap();
    provider.force_flush().unwrap();
    let initial = exporter.get_finished_spans().unwrap();
    let execution = initial
        .iter()
        .find(|span| span.name == "environment.execute")
        .unwrap();
    let queue = initial
        .iter()
        .find(|span| span.span_context.span_id() == execution.parent_span_id)
        .unwrap();
    assert_eq!(queue.name, "environment.queue");
    assert!(
        queue.end_time <= execution.start_time,
        "queue duration must stop before execution begins"
    );
    assert!(initial.iter().any(|s| s.name == "environment.queue"));
    assert!(initial.iter().any(|s| s.name == "environment.execute"));
    for _ in 0..3 {
        node.monitor_environments().await.unwrap();
        handle.expire_checkpoint(0).await.unwrap();
    }
    provider.force_flush().unwrap();
    assert_eq!(exporter.get_finished_spans().unwrap().len(), initial.len());
    assert!(!node.metrics().contains("environment_id="));
    assert!(node
        .environment_metrics()
        .contains("environment_id=\"trace-test\""));
    backend.1.store(true, Ordering::Relaxed);
    assert!(node.monitor_environments().await.is_err());
    provider.force_flush().unwrap();
    let after_error = exporter.get_finished_spans().unwrap();
    let error = after_error[initial.len()..]
        .iter()
        .find(|s| s.name == "environment.observation_failed")
        .expect("failed observation must remain diagnosable");
    assert!(matches!(
        error.status,
        opentelemetry::trace::Status::Error { .. }
    ));
    backend.1.store(false, Ordering::Relaxed);
    node.monitor_environments().await.unwrap();
    provider.force_flush().unwrap();
    assert_eq!(
        exporter.get_finished_spans().unwrap().len(),
        after_error.len()
    );
    backend.0.store(false, Ordering::Relaxed);
    node.monitor_environments().await.unwrap();
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let changed = &spans[initial.len()..];
    assert!(changed
        .iter()
        .any(|s| s.name == "environment.unexpected_exit"));
    assert!(changed.iter().any(|s| s.name == "environment.state.commit"));
    assert!(!changed
        .iter()
        .any(|s| s.name == "environment.queue" || s.name == "environment.execute"));
    assert!(!node
        .environment_metrics()
        .contains("environment_id=\"trace-test\""));
    provider.shutdown().unwrap();
}
