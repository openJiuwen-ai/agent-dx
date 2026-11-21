#[path = "common/authority.rs"]
mod authority;
#[path = "../src/bin/adxlet/observations.rs"]
mod observations;

use adx_core::{volumes::StorageMount, EnvironmentSpec, Resources, Result};
use adxlet::{resources::Observation, RuntimeDriver, RuntimeObservation};
use async_trait::async_trait;
use observations::{observe, publish_capabilities, SandboxdHealth, Snapshot};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::watch;

#[derive(Default)]
struct Runtime {
    inventory_delay: Duration,
}
#[async_trait]
impl RuntimeDriver for Runtime {
    async fn start(
        &self,
        _: &adx_core::EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        Err(adx_core::Error::Unavailable("unused start".into()))
    }
    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(false)
    }
    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }

    async fn inventory(&self) -> Result<Vec<RuntimeObservation>> {
        tokio::time::sleep(self.inventory_delay).await;
        Ok(vec![])
    }
}

fn creating_spec(id: &str) -> EnvironmentSpec {
    serde_json::from_value(serde_json::json!({
        "id": id, "tenant_id": "t", "image": "image", "runtime_class": "runc", "priority": 0,
        "resources": {"cpu_millis": 1, "memory_bytes": 1, "disk_bytes": 0}
    }))
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn sandboxd_health_requires_failure_count_and_duration_and_resets_on_success() {
    let node = authority::manager(Arc::new(Runtime::default()));
    node.update_runtime_classes(vec!["runc".into()]).unwrap();
    let mut health = SandboxdHealth::new(3, Duration::from_secs(30)).unwrap();
    for _ in 0..3 {
        health.update(&node, false).unwrap();
    }
    assert!(
        node.accepting_allocations(),
        "three immediate failures are insufficient"
    );
    tokio::time::advance(Duration::from_secs(29)).await;
    health.update(&node, false).unwrap();
    assert!(node.accepting_allocations());
    node.reserve_local(&creating_spec("before-health-deadline"))
        .unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    health.update(&node, false).unwrap();
    assert!(!node.accepting_allocations());
    assert!(node
        .reserve_local(&creating_spec("after-health-deadline"))
        .is_err());

    health.update(&node, true).unwrap();
    assert!(node.accepting_allocations());
    node.reserve_local(&creating_spec("after-health-recovery"))
        .unwrap();
    health.update(&node, false).unwrap();
    tokio::time::advance(Duration::from_secs(30)).await;
    health.update(&node, false).unwrap();
    assert!(
        node.accepting_allocations(),
        "duration alone is insufficient after the count resets"
    );
    health.update(&node, false).unwrap();
    assert!(!node.accepting_allocations());

    health.update(&node, true).unwrap();
    health.update(&node, false).unwrap();
    tokio::time::advance(Duration::from_secs(29)).await;
    health.update(&node, true).unwrap();
    health.update(&node, false).unwrap();
    health.update(&node, false).unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    health.update(&node, false).unwrap();
    assert!(
        node.accepting_allocations(),
        "success also resets the failure duration"
    );
}

#[tokio::test]
async fn health_recovery_preserves_other_admission_conditions() {
    let node = authority::manager(Arc::new(Runtime::default()));
    let mut health = SandboxdHealth::new(3, Duration::from_secs(30)).unwrap();
    assert!(SandboxdHealth::new(0, Duration::from_secs(30)).is_err());
    assert!(SandboxdHealth::new(3, Duration::ZERO).is_err());
    node.set_maintenance(true);
    health.update(&node, true).unwrap();
    assert!(!node.accepting_allocations());
    node.set_maintenance(false);
    let mut pressure = adxlet::resources::PressureGate::new(Default::default()).unwrap();
    node.set_pressure_state(&pressure.update(adxlet::resources::Pressure {
        memory_percent: 95,
        disk_percent: 10,
    }));
    health.update(&node, true).unwrap();
    assert!(!node.accepting_allocations());
    node.set_pressure_state(&pressure.update(adxlet::resources::Pressure {
        memory_percent: 10,
        disk_percent: 10,
    }));
    assert!(node.accepting_allocations());
    node.begin_shutdown().unwrap();
    health.update(&node, true).unwrap();
    assert!(!node.accepting_allocations());
}

#[tokio::test]
async fn unhealthy_sandboxd_keeps_heartbeats_and_known_runtime_inventory() {
    let node = authority::manager(Arc::new(Runtime::default()));
    node.update_runtime_classes(vec!["runc".into()]).unwrap();
    let mut fixture = authority::Fixture::new().await;
    fixture.registration.runtime_classes = vec!["runc".into()];
    fixture.cycle(&node).await.unwrap();
    let before = fixture.registration.heartbeat_sequence;
    let mut health = SandboxdHealth::new(3, Duration::from_secs(30)).unwrap();
    tokio::time::pause();
    health.update(&node, false).unwrap();
    tokio::time::advance(Duration::from_secs(30)).await;
    health.update(&node, false).unwrap();
    health.update(&node, false).unwrap();
    tokio::time::resume();
    for _ in 0..3 {
        fixture.cycle(&node).await.unwrap();
        assert!(!fixture.registration.accepting_allocations);
        assert!(!fixture.registration.reconciling);
        assert_eq!(fixture.registration.runtime_classes, ["runc"]);
    }
    assert!(fixture.registration.heartbeat_sequence >= before + 3);
    health.update(&node, true).unwrap();
    fixture.cycle(&node).await.unwrap();
    assert!(fixture.registration.accepting_allocations);
}

#[tokio::test]
async fn capability_failure_preserves_runtime_admission_and_withdraws_storage() {
    let node = authority::manager(Arc::new(Runtime::default()));
    let mut fixture = authority::Fixture::new().await;
    let (updates, samples) = watch::channel(Snapshot::new(
        Observation {
            capacity: Resources {
                cpu_millis: 10000,
                memory_bytes: 1 << 30,
                disk_bytes: 0,
            },
            devices: vec![],
        },
        Duration::from_secs(30),
    ));
    let mounts = BTreeMap::from([(
        "shared".into(),
        StorageMount {
            backend_id: "shared-backend".into(),
            root: "/mnt/shared".into(),
        },
    )]);
    publish_capabilities(&node, &updates, mounts, Ok(vec!["runc".into()])).unwrap();
    samples.borrow().apply(&mut fixture.registration);
    fixture.cycle(&node).await.unwrap();
    assert!(node.accepting_allocations());
    assert!(fixture.registration.storage_mounts.contains_key("shared"));

    publish_capabilities(
        &node,
        &updates,
        BTreeMap::new(),
        Err(adx_core::Error::Unavailable(
            "runtime capability observation timed out".into(),
        )),
    )
    .unwrap();
    samples.borrow().apply(&mut fixture.registration);
    fixture.cycle(&node).await.unwrap();
    assert!(
        node.accepting_allocations(),
        "an observation failure cannot withdraw known runtime support"
    );
    assert_eq!(fixture.registration.runtime_classes, ["runc"]);
    assert!(fixture.registration.accepting_allocations);
    assert!(fixture.registration.storage_mounts.is_empty());
    node.reserve_local(&creating_spec("after-failed-observation"))
        .unwrap();

    publish_capabilities(&node, &updates, BTreeMap::new(), Ok(vec!["runsc".into()])).unwrap();
    samples.borrow().apply(&mut fixture.registration);
    fixture.cycle(&node).await.unwrap();
    assert_eq!(fixture.registration.runtime_classes, ["runsc"]);
    assert!(node
        .reserve_local(&creating_spec("unsupported-runtime"))
        .is_err());
}

#[tokio::test]
async fn stalled_sampler_keeps_reporting_expired_capacity_and_preserve_shutdown() {
    let node = authority::manager(Arc::new(Runtime::default()));
    let mut fixture = authority::Fixture::new().await;
    fixture.cycle(&node).await.unwrap();
    let capacity = Resources {
        cpu_millis: 10000,
        memory_bytes: 1 << 30,
        disk_bytes: 0,
    };
    node.update_capacity(capacity, Duration::from_millis(1))
        .unwrap();
    let (_updates, samples) = watch::channel(Snapshot::new(
        Observation {
            capacity,
            devices: vec![],
        },
        Duration::from_millis(1),
    ));
    let entered = tokio::sync::Notify::new();
    let ready_to_shutdown = tokio::sync::Notify::new();
    let sampling = observe(
        Duration::from_millis(1),
        || async {
            entered.notify_one();
            std::future::pending::<()>().await
        },
        |_| Ok(()),
    );
    let reporting = async {
        entered.notified().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(samples.borrow().valid_until < tokio::time::Instant::now());
        let first_sequence = fixture.registration.heartbeat_sequence;
        for _ in 0..3 {
            samples.borrow().apply(&mut fixture.registration);
            fixture.cycle(&node).await.unwrap();
            assert!(node.accepting_allocations());
            assert_eq!(
                fixture.registration.capacity.as_ref().unwrap().cpu_millis,
                10000
            );
        }
        assert!(fixture.registration.heartbeat_sequence >= first_sequence + 3);
        ready_to_shutdown.notify_one();
        std::future::pending::<Result<()>>().await
    };
    let services = observations::serve(reporting, sampling);
    let mode = tokio::time::timeout(
        Duration::from_secs(2),
        adxlet::proxy::serve_until_shutdown(&node, None, services, async {
            ready_to_shutdown.notified().await;
            adxlet::proxy::ShutdownMode::Preserve
        }),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(mode, adxlet::proxy::ShutdownMode::Preserve);
    assert!(!node.accepting_allocations());
    samples.borrow().apply(&mut fixture.registration);
    fixture.cycle(&node).await.unwrap();
    assert!(!fixture.registration.accepting_allocations);
    adxlet::node_reconciliation::finish_shutdown(
        mode,
        &mut fixture.client,
        &fixture.registration,
        Duration::from_millis(50),
    )
    .await
    .unwrap();
    assert!(fixture.authority.departures.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn sampling_has_one_round_in_flight_and_publishes_late_recovery() {
    let release = tokio::sync::Semaphore::new(0);
    let (published, mut results) = tokio::sync::mpsc::channel(1);
    let count = std::cell::Cell::new(0);
    let mut sampling = Box::pin(observe(
        Duration::from_secs(1),
        || async {
            count.set(count.get() + 1);
            release.acquire().await.unwrap().forget();
            7
        },
        |value| {
            published.try_send(value).unwrap();
            Ok(())
        },
    ));
    tokio::select! { result = &mut sampling => panic!("{result:?}"), _ = tokio::task::yield_now() => {} }
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::select! { result = &mut sampling => panic!("{result:?}"), _ = tokio::task::yield_now() => {} }
    assert_eq!(count.get(), 1);
    release.add_permits(1);
    tokio::select! { result = &mut sampling => panic!("{result:?}"), value = results.recv() => assert_eq!(value, Some(7)) }
}

#[tokio::test]
async fn stalled_capacity_does_not_hold_pressure_updates() {
    let node = authority::manager(Arc::new(Runtime::default()));
    let mut fixture = authority::Fixture::new().await;
    fixture.cycle(&node).await.unwrap();
    let pressure_updated = tokio::sync::Notify::new();
    let entered = tokio::sync::Notify::new();
    let capacity = observe(
        Duration::from_millis(1),
        || async {
            entered.notify_one();
            std::future::pending::<()>().await
        },
        |_| Ok(()),
    );
    let mut gate = adxlet::resources::PressureGate::new(Default::default()).unwrap();
    let pressure = observe(
        Duration::from_millis(1),
        || async {
            entered.notified().await;
            adxlet::resources::Pressure {
                memory_percent: 95,
                disk_percent: 10,
            }
        },
        |value| {
            let update = gate.update(value);
            node.set_pressure_state(&update);
            pressure_updated.notify_one();
            Ok(())
        },
    );
    let sampling = async {
        tokio::try_join!(capacity, pressure)?;
        Ok(())
    };
    let reporting = async {
        pressure_updated.notified().await;
        fixture.cycle(&node).await.unwrap();
        assert!(!fixture.registration.accepting_allocations);
        assert_eq!(
            fixture
                .registration
                .pressure
                .as_ref()
                .unwrap()
                .memory
                .as_ref()
                .unwrap()
                .percent,
            95
        );
        Ok::<(), adx_core::Error>(())
    };
    tokio::time::timeout(
        Duration::from_secs(2),
        observations::serve(reporting, sampling),
    )
    .await
    .unwrap()
    .unwrap();
}

#[tokio::test]
async fn recovery_heartbeats_refresh_pressure_after_the_cycle_started() {
    let node = authority::manager(Arc::new(Runtime {
        inventory_delay: Duration::from_millis(80),
    }));
    let mut fixture = authority::Fixture::new().await;
    let reports = fixture.authority.reports.clone();
    let sampling = async {
        while reports.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        let mut gate = adxlet::resources::PressureGate::new(Default::default()).unwrap();
        node.set_pressure_state(&gate.update(adxlet::resources::Pressure {
            memory_percent: 95,
            disk_percent: 10,
        }));
        std::future::pending::<Result<()>>().await
    };
    let reporting = async {
        fixture.cycle(&node).await.unwrap();
        let reports = reports.lock().unwrap();
        assert!(reports.first().unwrap().pressure.is_none());
        assert!(reports
            .iter()
            .any(|report| report.reconciling && report.pressure.is_some()));
        assert!(reports.iter().all(|report| !report.accepting_allocations));
        Ok::<(), adx_core::Error>(())
    };
    tokio::time::timeout(
        Duration::from_secs(2),
        observations::serve(reporting, sampling),
    )
    .await
    .unwrap()
    .unwrap();
}
