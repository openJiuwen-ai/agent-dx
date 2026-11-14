#[path = "common/authority.rs"]
mod authority;
use adx_core::{EnvironmentSpec, Error, Result};
use adxlet::{RuntimeDriver, RuntimeObservation};
use async_trait::async_trait;
fn creating_spec() -> EnvironmentSpec {
    serde_json::from_value(serde_json::json!({
        "id":"creating", "tenant_id":"t", "image":"image", "runtime_class":"runc", "priority":0,
        "resources":{"cpu_millis":1,"memory_bytes":1,"disk_bytes":0}
    }))
    .unwrap()
}

#[tokio::test]
async fn shutdown_reports_keep_advancing_without_reopening_admission_or_unregistering() {
    let node = authority::manager(Arc::new(Runtime::default()));
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    assert!(node.accepting_allocations());
    let before = authority.authority.reports.lock().unwrap().len();
    node.begin_shutdown().unwrap();
    // A periodic capacity/maintenance update must not reopen process shutdown.
    node.set_maintenance(false);
    for _ in 0..3 {
        authority.cycle(&node).await.unwrap();
        assert!(!node.accepting_allocations());
        assert!(node.reserve_local(&creating_spec()).is_err());
    }
    let reports = authority.authority.reports.lock().unwrap();
    assert!(reports.len() >= before + 3);
    assert!(reports[before..]
        .iter()
        .all(|report| !report.accepting_allocations));
    assert!(reports.windows(2).all(|pair| {
        pair[1].heartbeat_sequence > pair[0].heartbeat_sequence
            && pair[1].session_id == pair[0].session_id
    }));
    assert!(authority.authority.departures.lock().unwrap().is_empty());
}

#[tokio::test]
async fn node_records_the_coordinator_epoch_from_authority_responses() {
    let node = authority::manager(Arc::new(Runtime::default()));
    let mut fixture = authority::Fixture::new().await;
    fixture.authority.epoch.store(7, Ordering::SeqCst);
    assert_eq!(node.observed_coordinator_epoch(), 0);
    fixture.cycle(&node).await.unwrap();
    assert_eq!(
        node.observed_coordinator_epoch(),
        7,
        "RegisterNode and InspectNode responses must advance the node's epoch record"
    );
}

#[tokio::test]
async fn live_catalog_fetch_does_not_reject_or_wait_for_new_creation() {
    let node = authority::manager(Arc::new(Runtime::default()));
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    authority
        .authority
        .block_inspect
        .store(true, Ordering::SeqCst);
    let control = authority.authority.clone();
    let other = node.clone();
    let cycle = tokio::spawn(async move { authority.cycle(&other).await });
    control.inspect_entered.notified().await;
    assert!(
        node.accepting_allocations(),
        "a normal catalog fetch cannot close creation"
    );
    let spec = creating_spec();
    node.reserve_local(&spec).unwrap();
    node.environment(
        spec.clone(),
        adx_core::Assignment {
            environment_id: spec.id,
            node_id: "n".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        },
    )
    .unwrap();
    control.inspect_release.notify_one();
    cycle.await.unwrap().unwrap();
}

#[tokio::test]
async fn live_catalog_absence_does_not_cancel_an_in_flight_claim_reservation() {
    let node = authority::manager(Arc::new(Runtime::default()));
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    let spec = creating_spec();
    let before = node.reserve_local(&spec).unwrap();
    // Claim may still be in flight while this snapshot is read.
    authority.cycle(&node).await.unwrap();
    let after = node.reserve_local(&spec).unwrap();
    assert_eq!(
        before.token, after.token,
        "a catalog cannot prove an in-flight claim was rejected"
    );
}
use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct Runtime {
    actual: Mutex<Vec<RuntimeObservation>>,
    scans: AtomicUsize,
    fail_remove: AtomicBool,
    remove_delay_ms: AtomicU64,
}
#[async_trait]
impl RuntimeDriver for Runtime {
    async fn start(
        &self,
        _: &EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<IpAddr> {
        Err(Error::Unavailable(
            "reconciliation cannot create an orphan".into(),
        ))
    }
    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(false)
    }
    async fn inventory(&self) -> Result<Vec<RuntimeObservation>> {
        self.scans.fetch_add(1, Ordering::SeqCst);
        Ok(self.actual.lock().unwrap().clone())
    }
    async fn remove(&self, id: &str) -> Result<()> {
        if self.fail_remove.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("delete failed".into()));
        }
        tokio::time::sleep(Duration::from_millis(
            self.remove_delay_ms.load(Ordering::SeqCst),
        ))
        .await;
        self.actual.lock().unwrap().retain(|r| r.runtime_id != id);
        Ok(())
    }
}
fn orphan(id: &str) -> RuntimeObservation {
    RuntimeObservation {
        environment_id: id.into(),
        tenant_id: "t".into(),
        runtime_id: format!("{id}-1"),
        generation: 1,
        running: true,
    }
}

#[tokio::test]
async fn periodic_cycle_removes_orphan_without_any_local_controller() {
    let runtime = Arc::new(Runtime::default());
    let node = authority::manager(runtime.clone());
    node.reconcile(vec![]).await.unwrap();
    runtime.actual.lock().unwrap().push(orphan("unowned"));
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    assert!(runtime.actual.lock().unwrap().is_empty());
    assert!(node.accepting_allocations());
    let reports = authority.authority.reports.lock().unwrap();
    assert!(reports.first().unwrap().reconciling);
    assert!(!reports.last().unwrap().reconciling);
    assert!(reports.last().unwrap().accepting_allocations);
}

#[tokio::test]
async fn ordinary_cycles_scan_and_reclaim_late_runtime_state() {
    let runtime = Arc::new(Runtime::default());
    let node = authority::manager(runtime.clone());
    node.reconcile(vec![]).await.unwrap();
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    runtime.actual.lock().unwrap().push(orphan("late"));
    authority.cycle(&node).await.unwrap();
    assert!(runtime.actual.lock().unwrap().is_empty());
    assert_eq!(runtime.scans.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn unavailable_authority_does_not_become_an_empty_catalog() {
    let runtime = Arc::new(Runtime::default());
    let node = authority::manager(runtime.clone());
    node.reconcile(vec![]).await.unwrap();
    runtime.actual.lock().unwrap().push(orphan("unowned"));
    let mut authority = authority::Fixture::new().await;
    authority
        .authority
        .unavailable
        .store(true, Ordering::SeqCst);
    assert!(authority.cycle(&node).await.is_err());
    assert_eq!(runtime.actual.lock().unwrap().len(), 1);
    assert_eq!(runtime.scans.load(Ordering::SeqCst), 1);
    assert!(!node.accepting_allocations());
}

#[tokio::test]
async fn failed_orphan_delete_is_retried_by_the_next_authority_cycle() {
    let runtime = Arc::new(Runtime::default());
    let node = authority::manager(runtime.clone());
    node.reconcile(vec![]).await.unwrap();
    runtime.actual.lock().unwrap().push(orphan("unowned"));
    runtime.fail_remove.store(true, Ordering::SeqCst);
    let mut authority = authority::Fixture::new().await;
    assert!(authority.cycle(&node).await.is_err());
    assert!(!node.accepting_allocations());
    assert_eq!(runtime.actual.lock().unwrap().len(), 1);
    runtime.fail_remove.store(false, Ordering::SeqCst);
    authority.cycle(&node).await.unwrap();
    assert!(runtime.actual.lock().unwrap().is_empty());
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn slow_cleanup_keeps_reporting_and_publishes_ready_without_another_tick() {
    let runtime = Arc::new(Runtime::default());
    let node = authority::manager(runtime.clone());
    node.reconcile(vec![]).await.unwrap();
    runtime.actual.lock().unwrap().push(orphan("slow"));
    runtime.remove_delay_ms.store(80, Ordering::SeqCst);
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    let reports = authority.authority.reports.lock().unwrap();
    assert!(reports.iter().filter(|r| r.reconciling).count() >= 2);
    assert!(reports
        .windows(2)
        .all(|r| r[0].heartbeat_sequence < r[1].heartbeat_sequence));
    assert!(!reports.last().unwrap().reconciling);
    assert!(reports.last().unwrap().accepting_allocations);
}

#[tokio::test]
async fn failed_readiness_report_closes_local_admission() {
    let runtime = Arc::new(Runtime::default());
    let node = authority::manager(runtime);
    node.reconcile(vec![]).await.unwrap();
    let mut authority = authority::Fixture::new().await;
    authority
        .authority
        .reject_ready
        .store(true, Ordering::SeqCst);
    assert!(authority.cycle(&node).await.is_err());
    assert!(!node.accepting_allocations());
}

#[tokio::test]
async fn stalled_authority_report_is_bounded_and_keeps_admission_closed() {
    let runtime = Arc::new(Runtime::default());
    let node = authority::manager(runtime.clone());
    node.reconcile(vec![]).await.unwrap();
    let mut authority = authority::Fixture::new().await;
    authority
        .authority
        .stall_report
        .store(true, Ordering::SeqCst);
    let result = adxlet::node_reconciliation::reconcile_node(
        &mut authority.client,
        &mut authority.registration,
        &node,
        None,
        Duration::from_millis(20),
        Duration::from_millis(10),
        tokio::time::Instant::now() + Duration::from_secs(60),
    )
    .await;
    // Tonic's gRPC deadline may cancel first, before the outer wall-clock timer.
    assert!(matches!(
        result.unwrap_err().code(),
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled
    ));
    assert!(!node.accepting_allocations());
    assert_eq!(runtime.scans.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn periodic_authority_preserves_allocation_and_live_routes() {
    let runtime = Arc::new(Runtime::default());
    let node = authority::manager(runtime);
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    let before = authority.authority.reports.lock().unwrap().len();
    authority.cycle(&node).await.unwrap();
    {
        let reports = authority.authority.reports.lock().unwrap();
        assert!(reports[before].accepting_allocations);
        assert!(
            !reports[before].reconciling,
            "periodic catalog refresh must preserve routability"
        );
        assert!(reports.last().unwrap().accepting_allocations);
    }
    authority
        .authority
        .unavailable
        .store(true, Ordering::SeqCst);
    assert!(authority.cycle(&node).await.is_err());
    assert!(!authority.registration.reconciling);
    assert!(node.accepting_allocations());
    authority
        .authority
        .unavailable
        .store(false, Ordering::SeqCst);
    authority.cycle(&node).await.unwrap();
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn live_catalog_failure_preserves_creation_admission() {
    let node = authority::manager(Arc::new(Runtime::default()));
    let mut fixture = authority::Fixture::new().await;
    fixture.cycle(&node).await.unwrap();
    fixture.authority.unavailable.store(true, Ordering::SeqCst);
    assert!(fixture.cycle(&node).await.is_err());
    assert!(
        node.accepting_allocations(),
        "a failed periodic read is not node failure"
    );
    node.reserve_local(&creating_spec()).unwrap();
    fixture.authority.unavailable.store(false, Ordering::SeqCst);
    fixture.cycle(&node).await.unwrap();
    assert!(node.accepting_allocations());
}

#[tokio::test]
async fn expired_capacity_observation_keeps_last_capacity_for_creation() {
    let node = authority::manager(Arc::new(Runtime::default()));
    let mut fixture = authority::Fixture::new().await;
    fixture.cycle(&node).await.unwrap();
    node.update_capacity(
        adx_core::Resources {
            cpu_millis: 10,
            memory_bytes: 10,
            disk_bytes: 0,
        },
        Duration::from_millis(1),
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(
        node.accepting_allocations(),
        "sample age is diagnostic, not admission"
    );
    node.reserve_local(&creating_spec()).unwrap();
}

#[tokio::test]
async fn preserve_shutdown_retains_session_without_contacting_departure_rpc() {
    for previously_draining in [false, true] {
        let node = authority::manager(Arc::new(Runtime::default()));
        let mut fixture = authority::Fixture::new().await;
        fixture.cycle(&node).await.unwrap();
        if previously_draining {
            node.drain().await.unwrap();
        }
        node.begin_shutdown().unwrap();
        fixture
            .authority
            .stall_unregister
            .store(true, Ordering::SeqCst);
        adxlet::node_reconciliation::finish_shutdown(
            adxlet::proxy::ShutdownMode::Preserve,
            &mut fixture.client,
            &fixture.registration,
            Duration::from_millis(20),
        )
        .await
        .unwrap();
        assert!(
            fixture.authority.departures.lock().unwrap().is_empty(),
            "prior drain cannot override the current preserve intent"
        );
        assert!(!node.accepting_allocations());
    }
}

#[tokio::test]
async fn planned_departure_uses_exact_identity_and_bounds_unavailable_coordinator() {
    let node = authority::manager(Arc::new(Runtime::default()));
    let mut fixture = authority::Fixture::new().await;
    fixture.cycle(&node).await.unwrap();
    node.drain().await.unwrap();
    adxlet::node_reconciliation::finish_shutdown(
        adxlet::proxy::ShutdownMode::Drain,
        &mut fixture.client,
        &fixture.registration,
        Duration::from_millis(100),
    )
    .await
    .unwrap();
    let calls = fixture.authority.departures.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].node_id, fixture.registration.node_id);
    assert_eq!(calls[0].session_id, fixture.registration.session_id);
    fixture
        .authority
        .stall_unregister
        .store(true, Ordering::SeqCst);
    let started = std::time::Instant::now();
    let error = adxlet::node_reconciliation::finish_shutdown(
        adxlet::proxy::ShutdownMode::Drain,
        &mut fixture.client,
        &fixture.registration,
        Duration::from_millis(20),
    )
    .await
    .unwrap_err();
    assert!(matches!(
        error.code(),
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled
    ));
    assert!(started.elapsed() < Duration::from_secs(1));
}
