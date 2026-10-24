//! Backend-independent node authority cycle, shared by startup and periodic reports.
use crate::{journal::JournalSink, reconciliation::ReconciliationOutcome, Adxlet};
use adx_core::EnvironmentRecord;
use adx_protocol::{control as pb, status};
use adx_transport::rpc::RpcChannel;
use std::{future::Future, sync::atomic::Ordering, time::Duration};
use tokio::time::{Instant, MissedTickBehavior};
use tonic::{Request, Status};

type Coordinator = pb::coordinator_service_client::CoordinatorServiceClient<RpcChannel>;

async fn bounded_rpc<T>(
    timeout: Duration,
    future: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| Status::deadline_exceeded("node authority RPC timed out"))?
}

async fn drain_reporting<D, H>(draining: D, reporting: H) -> Result<(), Status>
where
    D: Future<Output = ()>,
    H: Future<Output = Result<(), Status>>,
{
    tokio::select! {
        _ = draining => Ok(()),
        result = reporting => result,
    }
}

/// Close lifecycle admission while continuing bounded reports for accepted work.
pub async fn pause_lifecycle_reporting(
    manager: &Adxlet,
    coordinator: &mut Coordinator,
    registration: &mut pb::RegisterNodeRequest,
    timeout: Duration,
    report_interval: Duration,
    handshake_required: &mut bool,
) -> Result<(), Status> {
    if timeout.is_zero() || report_interval.is_zero() {
        return Err(Status::invalid_argument(
            "positive node reporting intervals required",
        ));
    }
    registration.accepting_allocations = false;
    let mut heartbeat = tokio::time::interval_at(Instant::now() + report_interval, report_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
    drain_reporting(manager.pause_lifecycle(), async {
        loop {
            heartbeat.tick().await;
            if let Err(error) = report(coordinator, registration, timeout, handshake_required).await
            {
                // A report failure cannot abandon the drain or reopen lifecycle.
                adx_observability::warn!(%error, "node report unavailable while lifecycle drains");
            }
        }
    })
    .await
}

async fn report(
    coordinator: &mut Coordinator,
    registration: &mut pb::RegisterNodeRequest,
    timeout: Duration,
    handshake_required: &mut bool,
) -> Result<(), Status> {
    registration.heartbeat_sequence = registration
        .heartbeat_sequence
        .checked_add(1)
        .ok_or_else(|| Status::internal("heartbeat sequence exhausted"))?;
    let mut request = Request::new(registration.clone());
    request.set_timeout(timeout);
    let result = bounded_rpc(timeout, coordinator.register_node(request))
        .await
        .map(|_| ());
    if result
        .as_ref()
        .is_err_and(|error| error.code() == tonic::Code::FailedPrecondition)
    {
        *handshake_required = true;
    }
    result
}

async fn inspect(
    coordinator: &mut Coordinator,
    registration: &pb::RegisterNodeRequest,
    timeout: Duration,
) -> Result<pb::InspectNodeResponse, Status> {
    let mut request = Request::new(pb::InspectNodeRequest {
        node_id: registration.node_id.clone(),
        session_id: registration.session_id.clone(),
    });
    request.set_timeout(timeout);
    Ok(bounded_rpc(timeout, coordinator.inspect_node(request))
        .await?
        .into_inner())
}

/// Obtain a complete current catalog with admission closed, reconcile every
/// configured runtime, and publish readiness before the next reporting interval.
/// Startup and session fencing drain the node. Live comparisons enqueue at most
/// one task per Environment and reopen admission after catalog validation, so a
/// slow or failed controller cannot close other Environments' admission. An
/// unavailable catalog is never empty and still closes new node allocation.
pub async fn reconcile_node(
    coordinator: &mut Coordinator,
    registration: &mut pb::RegisterNodeRequest,
    manager: &Adxlet,
    journal: Option<&JournalSink>,
    timeout: Duration,
    report_interval: Duration,
    capacity_valid_until: Instant,
) -> Result<(), Status> {
    let was_live = *manager.lifecycle_ready.read().await;
    // Admission can close after a failed cleanup/report. That does not make
    // already recovered controllers need startup recovery again.
    let periodic = manager.authority_initialized.load(Ordering::Acquire)
        && registration.heartbeat_sequence > 0
        && !registration.reconciling;
    manager.authority_reconciling.store(true, Ordering::Release);
    // Startup withdraws routability until recovery completes. A validated live
    // session only closes new allocation while refreshing its authority catalog.
    let handshake = !periodic || registration.reconciling;
    registration.reconciling = handshake;
    registration.accepting_allocations = false;
    if timeout.is_zero() || report_interval.is_zero() {
        return Err(Status::invalid_argument(
            "positive node reconciliation intervals required",
        ));
    }
    let mut catalog_received = false;
    let mut catalog_retry = false;
    let mut handshake_required = false;
    if !periodic {
        pause_lifecycle_reporting(
            manager,
            coordinator,
            registration,
            timeout,
            report_interval,
            &mut handshake_required,
        )
        .await?;
    }
    let result = async {
        report(coordinator, registration, timeout, &mut handshake_required).await?;
        let basis = periodic.then(|| manager.comparison_basis());
        let mut catalog = inspect(coordinator, registration, timeout).await?;
        catalog_received = true;
        if let Some(journal) = journal.filter(|_| !periodic) {
            let records = catalog
                .records
                .iter()
                .cloned()
                .map(TryInto::try_into)
                .collect::<adx_core::Result<Vec<EnvironmentRecord>>>()
                .map_err(status)?;
            journal.recover(&records).await.map_err(status)?;
            // Replay may change execution/checkpoint ownership. Recover both
            // from the same fresh complete Coordinator snapshot.
            catalog = inspect(coordinator, registration, timeout).await?;
        }
        let records = catalog
            .records
            .into_iter()
            .map(TryInto::try_into)
            .collect::<adx_core::Result<Vec<_>>>()
            .map_err(status)?;
        let snapshots = catalog
            .snapshots
            .into_iter()
            .map(TryInto::try_into)
            .collect::<adx_core::Result<Vec<_>>>()
            .map_err(status)?;
        let retained = catalog
            .retained_artifacts
            .into_iter()
            .map(adx_core::CheckpointArtifact::try_from)
            .collect::<adx_core::Result<Vec<_>>>()
            .map_err(status)?;
        let recovery = async {
            if let Some(basis) = basis {
                match manager
                    .compare_retained(basis, records, snapshots, retained, report_interval)
                    .await
                    .map_err(status)?
                {
                    ReconciliationOutcome::Complete | ReconciliationOutcome::Pending => (),
                    ReconciliationOutcome::StaleCatalog => {
                        catalog_retry = true;
                        return Err(Status::aborted("node authority catalog advanced"));
                    }
                    ReconciliationOutcome::Incomplete(error) => {
                        catalog_retry = true;
                        return Err(status(error));
                    }
                }
                Ok(())
            } else {
                manager
                    .reconcile_retained(records, snapshots, retained)
                    .await
                    .map_err(status)
            }
        };
        tokio::pin!(recovery);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + report_interval, report_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                result = &mut recovery => { result?; break; },
                _ = heartbeat.tick() => {
                    if periodic && !manager.authority_reconciling.load(Ordering::Acquire) {
                        registration.reconciling = false;
                        registration.accepting_allocations = manager.accepting_allocations()
                            && !manager.is_draining() && Instant::now() < capacity_valid_until;
                    }
                    report(coordinator, registration, timeout, &mut handshake_required).await?;
                },
            }
        }
        registration.reconciling = false;
        registration.accepting_allocations = manager.locally_accepting_allocations()
            && !manager.is_draining()
            && Instant::now() < capacity_valid_until;
        // Coordinator may dispatch while processing this report, before replying.
        // Authority is already validated, so accept those confirmed assignments.
        manager
            .authority_reconciling
            .store(false, Ordering::Release);
        report(coordinator, registration, timeout, &mut handshake_required).await
    }
    .await;
    if result.is_err() {
        if catalog_retry {
            // Even a fast local failure must publish restored node admission;
            // otherwise Coordinator only sees the earlier closed report.
            registration.reconciling = false;
            registration.accepting_allocations = manager.accepting_allocations()
                && !manager.is_draining()
                && Instant::now() < capacity_valid_until;
            if let Err(error) =
                report(coordinator, registration, timeout, &mut handshake_required).await
            {
                manager.authority_reconciling.store(true, Ordering::Release);
                pause_lifecycle_reporting(
                    manager,
                    coordinator,
                    registration,
                    timeout,
                    report_interval,
                    &mut handshake_required,
                )
                .await?;
                registration.reconciling = handshake_required;
                return Err(error);
            }
            return result;
        }
        manager.authority_reconciling.store(true, Ordering::Release);
        pause_lifecycle_reporting(
            manager,
            coordinator,
            registration,
            timeout,
            report_interval,
            &mut handshake_required,
        )
        .await?;
        // Coordinator restart/ownership invalidation can require a fresh
        // registration handshake, independently of local controller recovery.
        registration.reconciling = handshake || handshake_required;
        if was_live
            && !catalog_received
            && result.as_ref().is_err_and(|error| {
                matches!(
                    error.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Cancelled
                )
            })
        {
            // Do not invent a catalog or inspect/delete unknown resources. Existing
            // controllers may still perform their previously authorized operations.
            *manager.lifecycle_ready.write().await = true;
            registration.reconciling = handshake;
        }
        registration.accepting_allocations = false;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_rpc_releases_a_stalled_heartbeat() {
        let result = bounded_rpc(
            Duration::from_millis(10),
            std::future::pending::<Result<(), Status>>(),
        )
        .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    }

    #[tokio::test(start_paused = true)]
    async fn draining_a_restore_keeps_reporting_before_the_node_expiry_deadline() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let reports = AtomicU64::new(0);
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        drain_reporting(
            async {
                tokio::time::sleep(Duration::from_secs(24)).await;
                assert!(reports.load(Ordering::Relaxed) >= 10);
            },
            async {
                loop {
                    interval.tick().await;
                    reports.fetch_add(1, Ordering::Relaxed);
                }
            },
        )
        .await
        .unwrap();
    }
}
