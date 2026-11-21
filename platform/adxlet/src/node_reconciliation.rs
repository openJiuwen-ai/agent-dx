//! Backend-independent node authority cycle, shared by startup and periodic reports.
use crate::{journal::JournalSink, reconciliation::ReconciliationOutcome, Adxlet};
use adx_core::EnvironmentRecord;
use adx_protocol::{control as pb, status};
use adx_transport::rpc::RpcChannel;
use std::{future::Future, sync::atomic::Ordering, time::Duration};
use tokio::time::{Instant, MissedTickBehavior};
use tonic::{Request, Status};

type Coordinator = pb::coordinator_service_client::CoordinatorServiceClient<RpcChannel>;

impl Adxlet {
    /// Wait for the next report or an authority refresh request.
    pub async fn wait_for_reconciliation(&self, interval: &mut tokio::time::Interval) {
        tokio::select! {
            _ = interval.tick() => {},
            _ = self.reconciliation_requested.notified() => {},
        }
    }
}

fn require_handshake_on_failure(manager: &Adxlet, error: &Status, handshake_required: &mut bool) {
    if error.code() == tonic::Code::FailedPrecondition {
        manager
            .registration_acknowledged
            .store(false, Ordering::Release);
        *handshake_required = true;
    }
}

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
            if let Err(error) = report(
                manager,
                coordinator,
                registration,
                timeout,
                handshake_required,
            )
            .await
            {
                // A report failure cannot abandon the drain or reopen lifecycle.
                adx_observability::warn!(%error, "node report unavailable while lifecycle drains");
            }
        }
    })
    .await
}

/// Preserve the node session unless the exit signal selected drain/departure.
/// After drain, unregister the exact session with a bounded RPC; Coordinator
/// transport failures are returned. Preserve sends no RPC and relies on restart
/// within the existing heartbeat deadline for session takeover.
pub async fn finish_shutdown(
    mode: crate::proxy::ShutdownMode,
    coordinator: &mut Coordinator,
    registration: &pb::RegisterNodeRequest,
    timeout: Duration,
) -> Result<(), Status> {
    // Use this request's explicit intent, including after an earlier failed drain.
    if mode == crate::proxy::ShutdownMode::Preserve {
        return Ok(());
    }
    let mut request = Request::new(pb::UnregisterNodeRequest {
        node_id: registration.node_id.clone(),
        session_id: registration.session_id.clone(),
    });
    request.set_timeout(timeout);
    bounded_rpc(timeout, coordinator.unregister_node(request))
        .await
        .map(|_| ())
}

async fn report(
    manager: &Adxlet,
    coordinator: &mut Coordinator,
    registration: &mut pb::RegisterNodeRequest,
    timeout: Duration,
    handshake_required: &mut bool,
) -> Result<(), Status> {
    if registration.reconciling {
        manager
            .registration_acknowledged
            .store(false, Ordering::Release);
    }
    if manager.process_sealing() {
        registration.accepting_allocations = false;
    }
    registration.heartbeat_sequence = registration
        .heartbeat_sequence
        .checked_add(1)
        .ok_or_else(|| Status::internal("heartbeat sequence exhausted"))?;
    let mut request = Request::new(registration.clone());
    request.set_timeout(timeout);
    let response = bounded_rpc(timeout, coordinator.register_node(request))
        .await
        .map(tonic::Response::into_inner);
    registration_response(manager, registration, response, handshake_required).await
}

async fn registration_response(
    manager: &Adxlet,
    registration: &pb::RegisterNodeRequest,
    response: Result<pb::RegisterNodeResponse, Status>,
    handshake_required: &mut bool,
) -> Result<(), Status> {
    if registration.reconciling || registration.session_id != manager.services.upgrade.session() {
        manager
            .registration_acknowledged
            .store(false, Ordering::Release);
    }
    let result = match response {
        Ok(response) => {
            let applied = if let Some(control) = response.upgrade_control {
                manager
                    .registration_acknowledged
                    .store(false, Ordering::Release);
                manager
                    .apply_registration_upgrade_control(control)
                    .await
                    .map_err(status)
            } else {
                Ok(())
            };
            manager.registration_acknowledged.store(
                applied.is_ok()
                    && !registration.reconciling
                    && registration.session_id == manager.services.upgrade.session(),
                Ordering::Release,
            );
            applied
        }
        Err(error) => Err(error),
    };
    if let Err(error) = &result {
        require_handshake_on_failure(manager, error, handshake_required);
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

fn successor_status(stage: &'static str, error: Status) -> Status {
    Status::with_details_and_metadata(
        error.code(),
        format!("successor {stage}: {}", error.message()),
        bytes::Bytes::copy_from_slice(error.details()),
        error.metadata().clone(),
    )
}

/// A protected successor only compares/adopts exact execution identities. This
/// path is used on every report until explicit release, including rediscovery.
pub async fn reconcile_successor(
    coordinator: &mut Coordinator,
    registration: &mut pb::RegisterNodeRequest,
    manager: &Adxlet,
    routes: &crate::routes::UdsRoutes,
    identity: &adx_protocol::relay::AdxletProcessRequest,
    timeout: Duration,
) -> Result<(), Status> {
    // Healthy periodic observations retain their ordinary ACK through temporary
    // transport failures. Only a new handshake or confirmed invalidation clears it.
    async fn register(
        coordinator: &mut Coordinator,
        registration: &mut pb::RegisterNodeRequest,
        timeout: Duration,
    ) -> Result<pb::RegisterNodeResponse, Status> {
        registration.heartbeat_sequence = registration
            .heartbeat_sequence
            .checked_add(1)
            .ok_or_else(|| Status::internal("heartbeat sequence exhausted"))?;
        loop {
            let mut request = Request::new(registration.clone());
            request.set_timeout(timeout);
            match bounded_rpc(timeout, coordinator.register_node(request)).await {
                Ok(response) => return Ok(response.into_inner()),
                Err(error)
                    if matches!(
                        error.code(),
                        tonic::Code::Unavailable
                            | tonic::Code::DeadlineExceeded
                            | tonic::Code::Cancelled
                            | tonic::Code::Unknown
                    ) =>
                {
                    // Reuse the exact sequence/report after an ambiguous ACK.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
    let initial = !manager.authority_initialized.load(Ordering::Acquire);
    let mut registered_epoch = None;
    registration.accepting_allocations = false;
    // Do not reset the sequence when Coordinator restarts or a previous ACK was lost.
    if initial || registration.reconciling {
        manager
            .registration_acknowledged
            .store(false, Ordering::Release);
        registration.reconciling = true;
        registered_epoch = Some(
            register(coordinator, registration, timeout)
                .await
                .map_err(|error| successor_status("register-reconciling", error))?
                .coordinator_epoch,
        );
    }
    let catalog = match inspect(coordinator, registration, timeout).await {
        Ok(catalog) => catalog,
        Err(error) => {
            if error.code() == tonic::Code::FailedPrecondition {
                registration.reconciling = true;
                manager
                    .registration_acknowledged
                    .store(false, Ordering::Release);
            }
            return Err(successor_status("inspect", error));
        }
    };
    let epoch = catalog.coordinator_epoch;
    if registered_epoch.is_some_and(|registered| registered != epoch) {
        manager
            .registration_acknowledged
            .store(false, Ordering::Release);
        registration.reconciling = true;
        return Err(successor_status(
            "inspect-epoch",
            Status::failed_precondition("Coordinator changed before successor inspection"),
        ));
    }
    let records = catalog
        .records
        .into_iter()
        .map(TryInto::try_into)
        .collect::<adx_core::Result<Vec<EnvironmentRecord>>>()
        .map_err(|error| successor_status("inventory", status(error)))?;
    manager
        .validate_successor_inventory(&records)
        .await
        .map_err(|error| successor_status("inventory", status(error)))?;
    if initial {
        routes
            .takeover_bindings(&registration.node_id, &records)
            .await
            .map_err(|error| successor_status("relay-bindings", status(error)))?;
        manager
            .restore_successor_records(&records)
            .await
            .map_err(|error| successor_status("restore-runtime", status(error)))?;
    }
    registration.reconciling = false;
    let response = match register(coordinator, registration, timeout).await {
        Ok(response) => response,
        Err(error) => {
            registration.reconciling = true;
            manager
                .registration_acknowledged
                .store(false, Ordering::Release);
            return Err(successor_status("register-normal", error));
        }
    };
    if response.coordinator_epoch != epoch {
        manager
            .registration_acknowledged
            .store(false, Ordering::Release);
        registration.reconciling = true;
        return Err(successor_status(
            "register-normal-epoch",
            Status::failed_precondition("Coordinator changed during successor handshake"),
        ));
    }
    if let Some(control) = response.upgrade_control {
        manager
            .registration_acknowledged
            .store(false, Ordering::Release);
        if let Err(error) = manager.apply_registration_upgrade_control(control).await {
            registration.reconciling = true;
            return Err(successor_status("apply-control", status(error)));
        }
    }
    if initial {
        if let Err(error) = manager.publish_successor_ready(identity).await {
            manager
                .registration_acknowledged
                .store(false, Ordering::Release);
            registration.reconciling = true;
            return Err(successor_status("publish-ready", status(error)));
        }
    }
    if registration.session_id != manager.services.upgrade.session() {
        manager
            .registration_acknowledged
            .store(false, Ordering::Release);
        registration.reconciling = true;
        return Err(successor_status(
            "session",
            Status::failed_precondition("successor registration session changed"),
        ));
    }
    manager
        .registration_acknowledged
        .store(true, Ordering::Release);
    Ok(())
}

/// Compare a complete current catalog without closing healthy live admission.
/// Startup and session fencing drain accepted lifecycle work before recovery.
/// Live comparisons enqueue at most one task per Environment, so a slow or failed
/// controller cannot close other Environments' admission. An unavailable catalog
/// is never interpreted as empty; ordinary read failures preserve admission.
pub async fn reconcile_node(
    coordinator: &mut Coordinator,
    registration: &mut pb::RegisterNodeRequest,
    manager: &Adxlet,
    journal: Option<&JournalSink>,
    timeout: Duration,
    report_interval: Duration,
    _capacity_valid_until: Instant,
) -> Result<(), Status> {
    let was_live = *manager.lifecycle_ready.read().await;
    if manager.process_sealing() {
        registration.accepting_allocations = false;
        let mut handshake_required = false;
        return report(
            manager,
            coordinator,
            registration,
            timeout,
            &mut handshake_required,
        )
        .await;
    }
    // Admission can close after a failed cleanup/report. That does not make
    // already recovered controllers need startup recovery again.
    let periodic = manager.authority_initialized.load(Ordering::Acquire)
        && registration.heartbeat_sequence > 0
        && !registration.reconciling;
    // Startup withdraws routability until recovery completes. A validated live
    // session can compare a snapshot while accepting new allocations.
    let handshake = !periodic || registration.reconciling;
    registration.reconciling = handshake;
    if handshake {
        manager
            .registration_acknowledged
            .store(false, Ordering::Release);
    }
    registration.accepting_allocations =
        periodic && manager.accepting_allocations() && !manager.is_draining();
    if timeout.is_zero() || report_interval.is_zero() {
        return Err(Status::invalid_argument(
            "positive node reconciliation intervals required",
        ));
    }
    let mut catalog_received = false;
    let mut catalog_retry = false;
    let mut handshake_required = false;
    if !periodic {
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
    }
    let result = async {
        report(manager, coordinator, registration, timeout, &mut handshake_required).await?;
        let basis = if periodic {
            Some(manager.comparison_basis(report_interval).await)
        } else {
            None
        };
        let catalog = inspect(coordinator, registration, timeout).await?;
        catalog_received = true;
        let mut invalid_catalog = |error| {
            manager.authority_reconciling.store(true, Ordering::Release);
            catalog_retry = true;
            status(error)
        };
        let records = catalog
            .records
            .into_iter()
            .map(TryInto::try_into)
            .collect::<adx_core::Result<Vec<_>>>()
            .map_err(&mut invalid_catalog)?;
        let snapshots = catalog
            .snapshots
            .into_iter()
            .map(TryInto::try_into)
            .collect::<adx_core::Result<Vec<_>>>()
            .map_err(&mut invalid_catalog)?;
        let retained = catalog
            .retained_artifacts
            .into_iter()
            .map(adx_core::CheckpointArtifact::try_from)
            .collect::<adx_core::Result<Vec<_>>>()
            .map_err(&mut invalid_catalog)?;
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
                let result = manager
                    .reconcile_recovery(records, snapshots, retained, journal)
                    .await
                    .map_err(status);
                // Complete authority/inventory and the resource ledger are
                // known even when an individual controller remains fenced.
                catalog_retry = result.is_err() && *manager.lifecycle_ready.read().await;
                result
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
                    if manager.authority_initialized.load(Ordering::Acquire) && *manager.lifecycle_ready.read().await {
                        registration.reconciling = false;
                        registration.accepting_allocations = manager.accepting_allocations()
                            && !manager.is_draining();
                    }
                    report(manager, coordinator, registration, timeout, &mut handshake_required).await?;
                },
            }
        }
        registration.reconciling = false;
        registration.accepting_allocations =
            manager.accepting_allocations() && !manager.is_draining();
        // Coordinator may dispatch while processing this report, before replying.
        // Authority is already validated, so accept those confirmed assignments.
        report(manager, coordinator, registration, timeout, &mut handshake_required).await
    }
    .await;
    if let Err(error) = &result {
        // Includes both InspectNode calls, before the healthy-periodic early return.
        require_handshake_on_failure(manager, error, &mut handshake_required);
    }
    if result.is_err() && periodic && !handshake_required {
        // A failed live comparison leaves the last validated authority in force.
        // Never turn a transport failure into an empty catalog or node outage.
        registration.reconciling = false;
        registration.accepting_allocations =
            manager.accepting_allocations() && !manager.is_draining();
        if catalog_retry {
            let _ = report(
                manager,
                coordinator,
                registration,
                timeout,
                &mut handshake_required,
            )
            .await;
            if handshake_required {
                manager.authority_reconciling.store(true, Ordering::Release);
                registration.reconciling = true;
                registration.accepting_allocations = false;
                manager.reconciliation_requested.notify_one();
            }
        }
        return result;
    }
    if result.is_err() {
        if catalog_retry {
            // Even a fast local failure must publish restored node admission;
            // otherwise Coordinator only sees the earlier closed report.
            registration.reconciling = false;
            registration.accepting_allocations =
                manager.accepting_allocations() && !manager.is_draining();
            if let Err(error) = report(
                manager,
                coordinator,
                registration,
                timeout,
                &mut handshake_required,
            )
            .await
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
        if periodic && handshake_required {
            // Continue a newly required handshake without waiting for the next report tick.
            manager.reconciliation_requested.notify_one();
        }
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

    #[test]
    fn successor_stage_preserves_status_code_details_and_metadata() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.append("adx-diagnostic", "first".parse().unwrap());
        metadata.append("adx-diagnostic", "second".parse().unwrap());
        metadata.insert_bin(
            "adx-diagnostic-bin",
            tonic::metadata::MetadataValue::from_bytes(b"\0\xffcontext"),
        );
        let original = Status::with_details_and_metadata(
            tonic::Code::FailedPrecondition,
            "identity or version conflict",
            bytes::Bytes::from_static(b"\0\xffdetails"),
            metadata,
        );

        let contextual = successor_status("publish-ready", original.clone());

        assert_eq!(
            contextual.message(),
            "successor publish-ready: identity or version conflict"
        );
        assert_eq!(contextual.code(), original.code());
        assert_eq!(contextual.details(), original.details());
        assert_eq!(
            contextual.metadata().clone().into_headers(),
            original.metadata().clone().into_headers()
        );
    }

    #[tokio::test]
    async fn either_catalog_inspection_failure_requires_new_handshake_only_when_fenced() {
        let node = crate::admin::tests::manager();
        let mut handshake = false;
        node.registration_acknowledged
            .store(true, Ordering::Release);
        require_handshake_on_failure(
            &node,
            &Status::unavailable("temporary catalog failure"),
            &mut handshake,
        );
        assert!(node.registration_acknowledged.load(Ordering::Acquire));
        assert!(!handshake);
        require_handshake_on_failure(
            &node,
            &Status::failed_precondition("session fenced"),
            &mut handshake,
        );
        assert!(!node.registration_acknowledged.load(Ordering::Acquire));
        assert!(handshake);
    }

    #[tokio::test]
    async fn ordinary_ack_control_and_session_invalidation_contract() {
        let node = crate::admin::tests::manager();
        let mut registration = pb::RegisterNodeRequest {
            session_id: node.services.upgrade.session().into(),
            reconciling: false,
            ..Default::default()
        };
        let mut handshake = false;
        assert!(!node.registration_acknowledged.load(Ordering::Acquire));
        registration_response(&node, &registration, Ok(Default::default()), &mut handshake)
            .await
            .unwrap();
        assert!(node.registration_acknowledged.load(Ordering::Acquire));
        assert!(registration_response(
            &node,
            &registration,
            Err(Status::unavailable("temporary")),
            &mut handshake
        )
        .await
        .is_err());
        assert!(node.registration_acknowledged.load(Ordering::Acquire));
        assert!(!handshake);
        let invalid_control = pb::RegisterNodeResponse {
            upgrade_control: Some(pb::NodeUpgradeControl {
                phase: "invalid".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(
            registration_response(&node, &registration, Ok(invalid_control), &mut handshake)
                .await
                .is_err()
        );
        assert!(!node.registration_acknowledged.load(Ordering::Acquire));
        registration_response(&node, &registration, Ok(Default::default()), &mut handshake)
            .await
            .unwrap();
        registration.reconciling = true;
        registration_response(&node, &registration, Ok(Default::default()), &mut handshake)
            .await
            .unwrap();
        assert!(!node.registration_acknowledged.load(Ordering::Acquire));
        registration.reconciling = false;
        registration_response(&node, &registration, Ok(Default::default()), &mut handshake)
            .await
            .unwrap();
        assert!(registration_response(
            &node,
            &registration,
            Err(Status::failed_precondition("new handshake")),
            &mut handshake
        )
        .await
        .is_err());
        assert!(handshake);
        assert!(!node.registration_acknowledged.load(Ordering::Acquire));
        registration.session_id.push_str("changed");
        registration_response(&node, &registration, Ok(Default::default()), &mut handshake)
            .await
            .unwrap();
        assert!(!node.registration_acknowledged.load(Ordering::Acquire));
    }

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
