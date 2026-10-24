#[path = "common/authority.rs"]
mod authority;
use adx_core::{EnvironmentSpec, Error, Result};
use adxlet::{RuntimeDriver, RuntimeObservation};
use async_trait::async_trait;
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
async fn ordinary_cycles_compare_records_without_scanning_runtime_state() {
    let runtime = Arc::new(Runtime::default());
    let node = authority::manager(runtime.clone());
    node.reconcile(vec![]).await.unwrap();
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    runtime.actual.lock().unwrap().push(orphan("late"));
    authority.cycle(&node).await.unwrap();
    assert_eq!(runtime.actual.lock().unwrap().len(), 1);
    assert_eq!(runtime.scans.load(Ordering::SeqCst), 2);
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
async fn periodic_authority_closes_allocation_without_withdrawing_live_routes() {
    let runtime = Arc::new(Runtime::default());
    let node = authority::manager(runtime);
    let mut authority = authority::Fixture::new().await;
    authority.cycle(&node).await.unwrap();
    let before = authority.authority.reports.lock().unwrap().len();
    authority.cycle(&node).await.unwrap();
    {
        let reports = authority.authority.reports.lock().unwrap();
        assert!(!reports[before].accepting_allocations);
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
    assert!(!node.accepting_allocations());
    authority
        .authority
        .unavailable
        .store(false, Ordering::SeqCst);
    authority.cycle(&node).await.unwrap();
    assert!(node.accepting_allocations());
}
