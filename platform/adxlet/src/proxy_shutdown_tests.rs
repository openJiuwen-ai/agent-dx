use super::{serve_until_shutdown, EmbeddedProxy, ShutdownMode};
use crate::{Adxlet, Durability, Readiness, Routes, RuntimeDriver, RuntimeObservation, StateSink};
use adx_core::{EnvironmentRecord, EnvironmentSpec, Error, Resources, Result};
use async_trait::async_trait;
use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{oneshot, Notify};

#[derive(Default)]
struct Hooks(AtomicUsize);

#[async_trait]
impl RuntimeDriver for Hooks {
    async fn start(
        &self,
        _: &EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<IpAddr> {
        Ok("127.0.0.1".parse().unwrap())
    }
    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(true)
    }
    async fn remove(&self, _: &str) -> Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[async_trait]
impl Readiness for Hooks {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl Routes for Hooks {
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn retire_orphan(&self, _: &RuntimeObservation) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl StateSink for Hooks {
    async fn commit(&self, _: &EnvironmentRecord) -> Result<Durability> {
        Ok(Durability::Published)
    }
}

fn manager(hooks: Arc<Hooks>) -> Adxlet {
    let manager = Adxlet::new(
        "n".into(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
        hooks,
    );
    manager
        .update_capacity(
            Resources {
                cpu_millis: 10000,
                memory_bytes: 1 << 30,
                disk_bytes: 0,
            },
            Duration::from_secs(60),
        )
        .unwrap();
    manager
}

fn delayed_proxy(started: Arc<Notify>, delay: Duration) -> EmbeddedProxy {
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        stopped.await.unwrap();
        started.notify_one();
        tokio::time::sleep(delay).await;
        Ok(())
    });
    EmbeddedProxy {
        stop: Some(stop),
        task,
    }
}

#[tokio::test(start_paused = true)]
async fn relay_drain_keeps_heartbeats_and_control_services_alive_past_thirty_seconds() {
    let hooks = Arc::new(Hooks::default());
    let manager = manager(hooks.clone());
    *manager.lifecycle_ready.write().await = true;
    let reports = AtomicUsize::new(0);
    let controls = AtomicUsize::new(0);
    let proxy = delayed_proxy(Arc::new(Notify::new()), Duration::from_secs(35));
    let services = async {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        for _ in 0..100 {
            interval.tick().await;
            reports.fetch_add(1, Ordering::SeqCst);
            controls.fetch_add(1, Ordering::SeqCst);
        }
        Ok::<(), Error>(())
    };
    serve_until_shutdown(&manager, Some(proxy), services, async {
        ShutdownMode::Preserve
    })
    .await
    .unwrap();
    assert!(
        reports.load(Ordering::SeqCst) >= 34,
        "heartbeats stopped during Relay drain"
    );
    assert!(
        controls.load(Ordering::SeqCst) >= 34,
        "runtime controls stopped during Relay drain"
    );
    assert_eq!(hooks.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn background_service_exit_without_stop_intent_preserves_ownership() {
    let manager = manager(Arc::new(Hooks::default()));
    let mode = serve_until_shutdown(
        &manager,
        None,
        async { Ok::<(), Error>(()) },
        std::future::pending::<ShutdownMode>(),
    )
    .await
    .unwrap();
    assert_eq!(mode, ShutdownMode::Preserve);
}

#[tokio::test]
async fn shutdown_blocks_preexisting_controller_reservations_after_maintenance_reset() {
    let hooks = Arc::new(Hooks::default());
    let manager = manager(hooks.clone());
    let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
        "id": "late-start", "tenant_id": "t", "image": "image", "runtime_class": "runc", "priority": 0,
        "resources": {"cpu_millis": 1, "memory_bytes": 1, "disk_bytes": 0}
    })).unwrap();
    let handle = manager
        .environment(
            spec,
            adx_core::Assignment {
                environment_id: "late-start".into(),
                node_id: "n".into(),
                shard_id: 0,
                generation: 1,
                devices: vec![],
            },
        )
        .unwrap();
    manager.begin_shutdown().unwrap();
    manager.set_maintenance(false);
    assert!(
        handle.create().await.is_err(),
        "controller reopened resource admission during shutdown"
    );
    assert_eq!(manager.used(), Resources::default());
    assert_eq!(hooks.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn standalone_shutdown_closes_admission_without_waiting_for_services() {
    let hooks = Arc::new(Hooks::default());
    let manager = manager(hooks.clone());
    serve_until_shutdown(
        &manager,
        None,
        std::future::pending::<Result<()>>(),
        async { ShutdownMode::Preserve },
    )
    .await
    .unwrap();
    assert!(manager.is_stopping());
    assert!(!manager.accepting_allocations());
    assert_eq!(hooks.0.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn service_failure_during_relay_drain_is_returned() {
    let manager = manager(Arc::new(Hooks::default()));
    let started = Arc::new(Notify::new());
    let proxy = delayed_proxy(started.clone(), Duration::from_secs(35));
    let services = async {
        started.notified().await;
        Err(Error::Unavailable("runtime control failed".into()))
    };
    let error = serve_until_shutdown(&manager, Some(proxy), services, async {
        ShutdownMode::Preserve
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("runtime control failed"));
    assert!(manager.is_stopping());
}

#[tokio::test(start_paused = true)]
async fn preserve_closes_admission_before_relay_drains_and_retains_existing_environment() {
    let hooks = Arc::new(Hooks::default());
    let manager = manager(hooks.clone());
    *manager.lifecycle_ready.write().await = true;
    let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
        "id": "existing", "tenant_id": "t", "image": "image", "runtime_class": "runc", "priority": 0,
        "resources": {"cpu_millis": 1, "memory_bytes": 1, "disk_bytes": 0}
    })).unwrap();
    let handle = manager
        .environment(
            spec.clone(),
            adx_core::Assignment {
                environment_id: spec.id.clone(),
                node_id: "n".into(),
                shard_id: 0,
                generation: 1,
                devices: vec![],
            },
        )
        .unwrap();
    handle.create().await.unwrap();
    let used = manager.used();
    assert!(manager.accepting_allocations());
    let started = Arc::new(Notify::new());
    let proxy = delayed_proxy(started.clone(), Duration::from_secs(35));
    let run = serve_until_shutdown(
        &manager,
        Some(proxy),
        std::future::pending::<Result<()>>(),
        async { ShutdownMode::Preserve },
    );
    tokio::pin!(run);
    tokio::select! {
        _ = started.notified() => {},
        result = &mut run => panic!("node exited before Relay drain: {result:?}"),
    }
    assert!(
        !manager.accepting_allocations(),
        "preserve must close node admission first"
    );
    let mut fresh = spec.clone();
    fresh.id = "late".into();
    assert!(manager.reserve_local(&fresh).is_err());
    assert!(
        manager.reserve_local(&spec).is_err(),
        "late local retries cannot reopen admission"
    );
    assert!(manager
        .environment(
            fresh,
            adx_core::Assignment {
                environment_id: "late".into(),
                node_id: "n".into(),
                shard_id: 0,
                generation: 2,
                devices: vec![],
            }
        )
        .is_err());
    run.await.unwrap();
    assert_eq!(manager.used(), used);
    assert_eq!(handle.view().0.state, adx_core::EnvironmentState::Running);
    assert_eq!(hooks.0.load(Ordering::SeqCst), 0);
}
