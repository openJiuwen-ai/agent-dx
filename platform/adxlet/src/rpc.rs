//! Node RPC adaptation; EnvironmentHandle remains the lifecycle owner.
use adx_transport::rpc::{authenticated_channel, RpcChannel};
mod local_create;
mod volumes;
use crate::{Adxlet, Durability, RetiredDeleteFence, StateSink};
use adx_core::{EnvironmentRecord, Error, Result};
use adx_protocol::{
    auth::{tenant, Peers, Principal},
    control as pb, dependency_status, status,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tonic::{transport::Channel, Request, Response, Status};

#[derive(Clone)]
pub struct NodeRpc {
    manager: Arc<Adxlet>,
    peers: Peers,
    session_id: String,
    coordinator: Option<CoordinatorStateSink>,
    local_retries:
        Arc<std::sync::Mutex<std::collections::BTreeMap<String, local_create::PendingLocalCreate>>>,
    entry_locks: Arc<
        std::sync::Mutex<
            std::collections::BTreeMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>,
        >,
    >,
}
impl NodeRpc {
    #[allow(clippy::result_large_err)] // tonic transport status is shared by all RPC adapters.
    fn owned_handle(
        &self,
        assignment: Option<pb::Assignment>,
        caller: Option<&pb::CallerContext>,
    ) -> std::result::Result<crate::EnvironmentHandle, Status> {
        let assignment: adx_core::Assignment = assignment
            .ok_or_else(|| Status::invalid_argument("assignment required"))?
            .try_into()
            .map_err(status)?;
        let (spec, owner, handle) = self
            .manager
            .environments
            .lock()
            .expect("shared state lock poisoned")
            .get(&assignment.environment_id)
            .cloned()
            .ok_or_else(|| Status::not_found("environment not managed on this node"))?;
        tenant(caller, &spec.tenant_id)?;
        if owner != assignment {
            return Err(Status::failed_precondition("assignment changed"));
        }
        Ok(handle)
    }
    pub fn with_local_creation(mut self, coordinator: CoordinatorStateSink) -> Self {
        self.coordinator = Some(coordinator);
        self
    }
    pub fn new(manager: Arc<Adxlet>, peers: Peers, session_id: String) -> Self {
        Self {
            manager,
            peers,
            session_id,
            coordinator: None,
            entry_locks: Arc::default(),
            local_retries: Arc::default(),
        }
    }
}
fn delete_fence(
    principal: Principal,
    failed_revision: Option<u64>,
    expected_revision: Option<u64>,
) -> adx_core::Result<Option<RetiredDeleteFence>> {
    match principal {
        Principal::ApiServer => {
            if failed_revision.is_some() || expected_revision.is_some() {
                return Err(Error::Invalid(
                    "Coordinator deletion fences are not accepted from API Server".into(),
                ));
            }
            Ok(None)
        }
        Principal::Coordinator => match (failed_revision, expected_revision) {
            (Some(failed), None) => Ok(Some(RetiredDeleteFence::Failed(failed))),
            (None, Some(revision)) => Ok(Some(RetiredDeleteFence::Revision(revision))),
            _ => Err(Error::Invalid(
                "Coordinator delete requires exactly one deletion revision".into(),
            )),
        },
        _ => Err(Error::BackendFailure {
            code: adx_core::BackendFailureCode::PermissionDenied,
            message: "Coordinator or API Server required".into(),
        }),
    }
}
fn creation_error(handle: &crate::EnvironmentHandle, error: Error) -> Status {
    let (record, durability) = handle.view();
    let stage = record
        .observation
        .as_ref()
        .and_then(|o| o.create_stage)
        .unwrap_or(adx_core::observation::CreateStage::Unknown);
    let reason = match &error {
        Error::NoCapacity => "capacity",
        Error::Conflict => "conflict",
        Error::DeadlineExceeded(_) => "timeout",
        Error::ProbeFailed(_) => "probe_failed",
        Error::BackendFailure { .. } => "runtime_error",
        Error::Unavailable(_) => "dependency",
        Error::ResponseTimeout(_) => "transport",
        _ => "unknown",
    };
    let mut status = adx_protocol::create_status(status(error), stage);
    status.metadata_mut().insert(
        "adx-create-reason",
        tonic::metadata::MetadataValue::from_static(reason),
    );
    if durability == Some(Durability::Published)
        && record.state == adx_core::EnvironmentState::Failed
        && !record.restart_pending
    {
        status.metadata_mut().insert(
            "adx-create-terminal",
            tonic::metadata::MetadataValue::from_static("true"),
        );
    }
    status
}
fn response(result: crate::OperationResult) -> Result<Response<pb::EnvironmentResult>> {
    adx_observability::info!(event="environment_operation_completed", traceparent=?adx_observability::trace::traceparent(), environment_id=%result.record.spec.id,
        generation=result.record.assignment.generation, revision=result.record.revision,
        state=?result.record.state, "environment operation completed");
    Ok(Response::new(pb::EnvironmentResult {
        newly_created: result.newly_created,
        record: Some(result.record.try_into()?),
        durability: match result.durability {
            Durability::Published => pb::Durability::Published,
            Durability::Journaled => pb::Durability::Journaled,
        } as i32,
        ..Default::default()
    }))
}
#[tonic::async_trait]
impl pb::node_service_server::NodeService for NodeRpc {
    async fn get_environment_control(
        &self,
        request: Request<pb::NodeGetEnvironmentControlRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentControlRecord>, Status> {
        if self.peers.authenticate(&request)? != Principal::Coordinator {
            return Err(Status::permission_denied("Coordinator required"));
        }
        let request = request.into_inner();
        let gate = self.manager.lifecycle_ready.read().await;
        if !*gate {
            return Err(Status::unavailable("node is reconciling"));
        }
        if self.session_id.is_empty() || request.node_session_id != self.session_id {
            return Err(Status::failed_precondition("node process session changed"));
        }
        let handle = self.owned_handle(request.assignment, request.caller.as_ref())?;
        Ok(Response::new(
            handle.run_control().await.map_err(status)?.into(),
        ))
    }
    async fn batch_get_environment_control(
        &self,
        request: Request<pb::NodeBatchGetEnvironmentControlRequest>,
    ) -> std::result::Result<Response<pb::BatchGetEnvironmentControlResponse>, Status> {
        if self.peers.authenticate(&request)? != Principal::Coordinator {
            return Err(Status::permission_denied("Coordinator required"));
        }
        let gate = self.manager.lifecycle_ready.read().await;
        if !*gate {
            return Err(Status::unavailable("node is reconciling"));
        }
        let requests = request.into_inner().requests;
        if requests.len() > 1000 {
            return Err(Status::invalid_argument(
                "at most 1000 controls per request",
            ));
        }
        let mut records = Vec::with_capacity(requests.len());
        for request in requests {
            if self.session_id.is_empty() || request.node_session_id != self.session_id {
                return Err(Status::failed_precondition("node process session changed"));
            }
            let handle = match self.owned_handle(request.assignment, request.caller.as_ref()) {
                Ok(handle) => handle,
                Err(error) if error.code() == tonic::Code::NotFound => continue,
                Err(error) => return Err(error),
            };
            match handle.run_control().await {
                Ok(control) => records.push(control.into()),
                Err(Error::NotFound) => continue,
                Err(error) => return Err(status(error)),
            }
        }
        Ok(Response::new(pb::BatchGetEnvironmentControlResponse {
            records,
        }))
    }
    async fn update_environment_control(
        &self,
        request: Request<pb::NodeUpdateEnvironmentControlRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentControlRecord>, Status> {
        if self.peers.authenticate(&request)? != Principal::Coordinator {
            return Err(Status::permission_denied("Coordinator required"));
        }
        let gate = self.manager.lifecycle_ready.read().await;
        if !*gate {
            return Err(Status::unavailable("node is reconciling"));
        }
        let request = request.into_inner();
        if self.session_id.is_empty() || request.node_session_id != self.session_id {
            return Err(Status::failed_precondition("node process session changed"));
        }
        let handle = self.owned_handle(request.assignment, request.caller.as_ref())?;
        Ok(Response::new(
            handle
                .update_run_timeout(request.timeout_seconds, request.extend_only)
                .await
                .map_err(status)?
                .into(),
        ))
    }

    async fn apply_node_upgrade_control(
        &self,
        request: Request<pb::NodeUpgradeControl>,
    ) -> std::result::Result<Response<pb::NodeUpgradeInspection>, Status> {
        if self.peers.authenticate(&request)? != Principal::Coordinator {
            return Err(Status::permission_denied("Coordinator identity required"));
        }
        let control = request.into_inner();
        if control.node_session_id != self.session_id || self.session_id.is_empty() {
            return Err(Status::failed_precondition("node process session changed"));
        }
        let result = self
            .manager
            .apply_upgrade_control(control)
            .await
            .map_err(status)?;
        Ok(Response::new(result))
    }

    async fn inspect_node_upgrade(
        &self,
        request: Request<pb::NodeUpgradeInspectRequest>,
    ) -> std::result::Result<Response<pb::NodeUpgradeInspection>, Status> {
        if self.peers.authenticate(&request)? != Principal::Coordinator {
            return Err(Status::permission_denied("Coordinator identity required"));
        }
        let request = request.into_inner();
        if request.node_session_id != self.session_id || self.session_id.is_empty() {
            return Err(Status::failed_precondition("node process session changed"));
        }
        let mut actual = self
            .manager
            .inspect_upgrade_environment(
                (!request.environment_id.is_empty()).then_some(request.environment_id.as_str()),
            )
            .await
            .map_err(status)?;
        let probe = request.task_id.is_empty() && request.epoch == 0;
        if !probe && (actual.task_id != request.task_id || actual.epoch != request.epoch) {
            return Err(Status::failed_precondition(
                "node upgrade authority changed",
            ));
        }
        if !request.environment_id.is_empty() {
            let lifecycle = self.manager.lifecycle_ready.read().await;
            actual.unknown |= !*lifecycle
                || self
                    .manager
                    .authority_reconciling
                    .load(std::sync::atomic::Ordering::Acquire);
            let view = self
                .manager
                .environments
                .lock()
                .map_err(|_| Status::internal("environment registry unavailable"))?
                .get(&request.environment_id)
                .map(|(_, _, handle)| handle.view());
            actual.environment = match view {
                Some((record, durability)) => Some(pb::EnvironmentResult {
                    record: Some(record.try_into().map_err(status)?),
                    durability: match durability {
                        Some(Durability::Published) => pb::Durability::Published,
                        Some(Durability::Journaled) => pb::Durability::Journaled,
                        None => pb::Durability::Unspecified,
                    } as i32,
                    ..Default::default()
                }),
                None => None,
            };
        }
        Ok(Response::new(actual))
    }

    async fn ensure_environment_running(
        &self,
        request: Request<pb::EnsureEnvironmentRunningRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        if self.peers.authenticate(&request)? != Principal::Coordinator {
            return Err(Status::permission_denied("Coordinator identity required"));
        }
        let r = request.into_inner();
        if r.node_session_id != self.session_id || self.session_id.is_empty() {
            return Err(Status::failed_precondition("node process session changed"));
        }
        let assignment: adx_core::Assignment = r
            .assignment
            .ok_or_else(|| Status::invalid_argument("assignment required"))?
            .try_into()
            .map_err(status)?;
        let (_, owner, handle) = self
            .manager
            .environments
            .lock()
            .map_err(|_| Status::internal("environment registry unavailable"))?
            .get(&assignment.environment_id)
            .cloned()
            .ok_or_else(|| Status::not_found("environment not managed"))?;
        if owner != assignment {
            return Err(Status::failed_precondition("assignment changed"));
        }
        let gate_started = Instant::now();
        let gate = self.manager.lifecycle_ready.read().await;
        let gate_wait_ms = gate_started.elapsed().as_secs_f64() * 1_000.0;
        if !*gate || self.manager.is_draining() {
            return Err(Status::unavailable("node is reconciling or draining"));
        }
        let controller_started = Instant::now();
        let result = handle
            .ensure_running_with_timeout(r.operation_id, r.run_timeout_seconds)
            .await;
        adx_observability::info!(
            event = "node_ensure_running_stages", environment_id = %assignment.environment_id,
            gate_wait_ms, controller_ms = controller_started.elapsed().as_secs_f64() * 1_000.0,
            success = result.is_ok(), "node ensure running stages completed"
        );
        response(result.map_err(status)?).map_err(status)
    }
    async fn create_local_environment(
        &self,
        request: Request<pb::LocalEnvironmentCreateRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.create_local_environment", &request);
        if let Some(environment_id) = request
            .get_ref()
            .create
            .as_ref()
            .and_then(|create| create.spec.as_ref())
            .map(|spec| spec.id.clone())
        {
            trace.attribute("environment.id", environment_id);
        }
        if self.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let service = self.clone();
        let mut request = request.into_inner();
        let timeout = request
            .create
            .as_ref()
            .map(|create| Duration::from_secs(create.create_timeout_seconds))
            .filter(|timeout| !timeout.is_zero())
            .or_else(|| service.coordinator.as_ref().map(|sink| sink.timeout))
            .unwrap_or(Duration::from_secs(90));
        let create = request
            .create
            .as_mut()
            .ok_or_else(|| Status::invalid_argument("create required"))?;
        let deadline = if create.create_deadline_unix_millis == 0 {
            adx_transport::deadline::Deadline::after(timeout)?
        } else {
            adx_transport::deadline::Deadline::from_unix_millis(create.create_deadline_unix_millis)?
        };
        create.create_deadline_unix_millis = deadline.unix_millis();
        tokio::spawn(trace.run_result(async move {
            let result = service.create_local(request, deadline.instant()).await;
            match &result {
                Ok(_) => adx_observability::trace::attribute("result", "success".into()),
                Err(error) => {
                    adx_observability::trace::attribute("result", "fail".into());
                    adx_observability::trace::attribute(
                        "error.code",
                        i32::from(error.code()).to_string(),
                    );
                    adx_observability::trace::attribute(
                        "error.message",
                        error.message().to_owned(),
                    );
                    adx_observability::trace::error_with(error.message().to_owned());
                }
            }
            result
        }))
        .await
        .map_err(|_| Status::internal("local creation task failed"))?
    }

    async fn recover_environment(
        &self,
        request: Request<pb::RecoverEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.recover_environment", &request);
        trace
            .run_result(async {
                if self.peers.authenticate(&request)? != Principal::Coordinator {
                    return Err(Status::permission_denied("Coordinator identity required"));
                }
                let r = request.into_inner();
                if r.node_session_id != self.session_id || self.session_id.is_empty() {
                    return Err(Status::failed_precondition("node process session changed"));
                }
                let record = r
                    .record
                    .ok_or_else(|| Status::invalid_argument("recovery record required"))?
                    .try_into()
                    .map_err(status)?;
                response(
                    self.manager
                        .recover_environment_with_timeout(record, r.run_timeout_seconds)
                        .await
                        .map_err(status)?,
                )
                .map_err(status)
            })
            .await
    }

    async fn get_session(
        &self,
        request: Request<pb::GetNodeSessionRequest>,
    ) -> std::result::Result<Response<pb::GetNodeSessionResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.get_session", &request);
        trace
            .run_result(async {
                if self.peers.authenticate(&request)? != Principal::Coordinator {
                    return Err(Status::permission_denied("Coordinator identity required"));
                }
                Ok(Response::new(pb::GetNodeSessionResponse {
                    node_id: self.manager.node_id.clone(),
                    session_id: self.session_id.clone(),
                }))
            })
            .await
    }

    async fn create_snapshot(
        &self,
        request: Request<pb::CreateSnapshotRequest>,
    ) -> std::result::Result<Response<pb::CreateSnapshotResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.create_snapshot", &request);
        trace
            .run_result(async {
                let principal = self.peers.authenticate(&request)?;
                if !matches!(principal, Principal::ApiServer | Principal::Coordinator) {
                    return Err(Status::permission_denied(
                        "validated Frontend caller required",
                    ));
                }
                let gate = self.manager.lifecycle_ready.read().await;
                if !*gate {
                    return Err(Status::unavailable("node is reconciling"));
                }
                let r = request.into_inner();
                if principal == Principal::Coordinator
                    && (self.session_id.is_empty() || r.node_session_id != self.session_id)
                {
                    return Err(Status::failed_precondition("node process session changed"));
                }
                let handle = self.owned_handle(r.assignment, r.caller.as_ref())?;
                let saved = handle
                    .snapshot(crate::checkpoint::SnapshotRequest {
                        kind: pb::SnapshotKind::try_from(r.kind)
                            .map_err(|_| Status::invalid_argument("invalid snapshot kind"))?
                            .into(),
                        operation_id: r.operation_id,
                        expected_revision: r.expected_revision,
                        names: r.names,
                        timeout_seconds: r.timeout_seconds,
                    })
                    .await
                    .map_err(status)?;
                Ok(Response::new(pb::CreateSnapshotResponse {
                    snapshot: Some(saved.snapshot.try_into().map_err(status)?),
                    environment: Some(response(saved.environment).map_err(status)?.into_inner()),
                }))
            })
            .await
    }
    async fn collect_snapshot(
        &self,
        request: Request<pb::CollectSnapshotRequest>,
    ) -> std::result::Result<Response<pb::CollectSnapshotResponse>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.collect_snapshot", &request);
        trace
            .run_result(async {
                if self.peers.authenticate(&request)? != Principal::Coordinator {
                    return Err(Status::permission_denied("Coordinator identity required"));
                }
                let gate = self.manager.lifecycle_ready.read().await;
                if !*gate {
                    return Err(Status::unavailable("node is reconciling"));
                }
                let r = request.into_inner();
                if self.session_id.is_empty() || r.node_session_id != self.session_id {
                    return Err(Status::failed_precondition("node process session changed"));
                }
                let snapshot = r
                    .snapshot
                    .ok_or_else(|| Status::invalid_argument("snapshot required"))?
                    .try_into()
                    .map_err(status)?;
                self.manager
                    .collect_snapshot(&snapshot, r.delegated)
                    .await
                    .map_err(status)?;
                Ok(Response::new(pb::CollectSnapshotResponse {
                    snapshot_id: snapshot.id,
                    revision: snapshot.revision,
                }))
            })
            .await
    }
    async fn collect_checkpoint_cleanup(
        &self,
        request: Request<pb::CollectCheckpointCleanupRequest>,
    ) -> std::result::Result<Response<pb::CollectCheckpointCleanupResponse>, Status> {
        let trace =
            adx_observability::trace::Trace::rpc("node.collect_checkpoint_cleanup", &request);
        trace
            .run_result(async {
                if self.peers.authenticate(&request)? != Principal::Coordinator {
                    return Err(Status::permission_denied("Coordinator identity required"));
                }
                let gate = self.manager.lifecycle_ready.read().await;
                if !*gate {
                    return Err(Status::unavailable("node is reconciling"));
                }
                let request = request.into_inner();
                if self.session_id.is_empty() || request.node_session_id != self.session_id {
                    return Err(Status::failed_precondition("node process session changed"));
                }
                let cleanup: adx_core::CheckpointCleanup = request
                    .cleanup
                    .ok_or_else(|| Status::invalid_argument("checkpoint cleanup required"))?
                    .try_into()
                    .map_err(status)?;
                self.manager
                    .collect_checkpoint_cleanup(&cleanup, request.delegated)
                    .await
                    .map_err(status)?;
                Ok(Response::new(pb::CollectCheckpointCleanupResponse {
                    environment_id: cleanup.assignment.environment_id,
                    generation: cleanup.assignment.generation,
                    revision: cleanup.revision,
                }))
            })
            .await
    }
    async fn create_environment(
        &self,
        request: Request<pb::StartAssignedEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.create_environment", &request);
        if let Some(environment_id) = request.get_ref().spec.as_ref().map(|spec| spec.id.clone()) {
            trace.attribute("environment.id", environment_id);
        }
        trace
            .run_result(async {
                if self.peers.authenticate(&request)? != Principal::Coordinator {
                    return Err(Status::permission_denied(
                        "only Coordinator may assign environments",
                    ));
                }
                let gate = self.manager.lifecycle_ready.read().await;
                if !*gate {
                    return Err(Status::unavailable("node is reconciling"));
                }
                let r = request.into_inner();
                if self.session_id.is_empty() || r.node_session_id != self.session_id {
                    return Err(Status::failed_precondition("node process session changed"));
                }
                let deadline = if r.create_deadline_unix_millis == 0 {
                    None
                } else {
                    Some(
                        adx_transport::deadline::Deadline::from_unix_millis(
                            r.create_deadline_unix_millis,
                        )?
                        .instant(),
                    )
                };
                let spec = r
                    .spec
                    .ok_or_else(|| Status::invalid_argument("spec required"))?
                    .try_into()
                    .map_err(status)?;
                let assignment = r
                    .assignment
                    .ok_or_else(|| Status::invalid_argument("assignment required"))?
                    .try_into()
                    .map_err(status)?;
                let handle = self.manager.environment(spec, assignment).map_err(status)?;
                let result = match r.snapshot {
                    Some(snapshot) => match deadline {
                        Some(deadline) => {
                            handle
                                .create_from_snapshot_with_deadline(
                                    snapshot.try_into().map_err(status)?,
                                    deadline,
                                )
                                .await
                        }
                        None => {
                            handle
                                .create_from_snapshot(snapshot.try_into().map_err(status)?)
                                .await
                        }
                    },
                    None => match deadline {
                        Some(deadline) => handle.create_with_deadline(deadline).await,
                        None => handle.create().await,
                    },
                };
                response(result.map_err(|error| creation_error(&handle, error))?).map_err(status)
            })
            .await
    }
    async fn pause_environment(
        &self,
        request: Request<pb::PauseEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.pause_environment", &request);
        trace
            .run_result(async {
                let principal = self.peers.authenticate(&request)?;
                if principal != Principal::ApiServer && principal != Principal::Coordinator {
                    return Err(Status::permission_denied(
                        "API Server or Coordinator required",
                    ));
                }
                let gate = self.manager.lifecycle_ready.read().await;
                if !*gate {
                    return Err(Status::unavailable("node is reconciling"));
                }
                let r = request.into_inner();
                if principal == Principal::Coordinator {
                    if !r.operation_id.starts_with("task:") {
                        return Err(Status::permission_denied(
                            "Coordinator pause requires an upgrade task",
                        ));
                    }
                    let assignment = r
                        .assignment
                        .as_ref()
                        .ok_or_else(|| Status::invalid_argument("assignment required"))?;
                    self.manager
                        .services
                        .upgrade
                        .management_grant(
                            &assignment.environment_id,
                            assignment.generation,
                            "pause",
                            &r.operation_id,
                        )
                        .map_err(status)?;
                }
                let handle = self.owned_handle(r.assignment, r.caller.as_ref())?;
                response(
                    handle
                        .pause(crate::checkpoint::PauseRequest {
                            operation_id: r.operation_id,
                            expected_revision: r.expected_revision,
                            retention: r
                                .retention
                                .ok_or_else(|| {
                                    Status::invalid_argument("pause retention required")
                                })?
                                .try_into()
                                .map_err(status)?,
                            timeout_seconds: r.timeout_seconds,
                        })
                        .await
                        .map_err(status)?,
                )
                .map_err(status)
            })
            .await
    }
    async fn resume_environment(
        &self,
        request: Request<pb::ResumeEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.resume_environment", &request);
        trace
            .run_result(async {
                if self.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied(
                        "validated Frontend caller required",
                    ));
                }
                let gate = self.manager.lifecycle_ready.read().await;
                if !*gate {
                    return Err(Status::unavailable("node is reconciling"));
                }
                let r = request.into_inner();
                let handle = self.owned_handle(r.assignment, r.caller.as_ref())?;
                response(
                    handle
                        .resume(crate::checkpoint::ResumeRequest {
                            run_timeout_seconds: None,
                            operation_id: r.operation_id,
                            expected_revision: r.expected_revision,
                        })
                        .await
                        .map_err(status)?,
                )
                .map_err(status)
            })
            .await
    }
    async fn update_network_policy(
        &self,
        request: Request<pb::UpdateNetworkPolicyRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.update_network_policy", &request);
        trace
            .run_result(async {
                if self.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied(
                        "validated API Server caller required",
                    ));
                }
                let gate = self.manager.lifecycle_ready.read().await;
                if !*gate {
                    return Err(Status::unavailable("node is reconciling"));
                }
                let r = request.into_inner();
                let assignment = r.assignment.clone();
                let handle = self.owned_handle(assignment, r.caller.as_ref())?;
                let policy = r
                    .network
                    .map(TryInto::try_into)
                    .transpose()
                    .map_err(status)?;
                let result = handle
                    .update_network_policy(policy, r.operation_id, r.expected_revision)
                    .await
                    .map_err(status)?;
                {
                    let mut environments = self
                        .manager
                        .environments
                        .lock()
                        .expect("shared state lock poisoned");
                    if let Some((spec, owner, _)) = environments.get_mut(&result.record.spec.id) {
                        if *owner != result.record.assignment {
                            return Err(Status::failed_precondition("assignment changed"));
                        }
                        *spec = result.record.spec.clone();
                    }
                }
                response(result).map_err(status)
            })
            .await
    }
    async fn reload_environment(
        &self,
        request: Request<pb::ReloadEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.reload_environment", &request);
        trace
            .run_result(async {
                if self.peers.authenticate(&request)? != Principal::ApiServer {
                    return Err(Status::permission_denied(
                        "validated API Server caller required",
                    ));
                }
                let gate = self.manager.lifecycle_ready.read().await;
                if !*gate {
                    return Err(Status::unavailable("node is reconciling"));
                }
                let r = request.into_inner();
                let handle = self.owned_handle(r.assignment, r.caller.as_ref())?;
                response(
                    handle
                        .reload(r.operation_id, r.expected_revision)
                        .await
                        .map_err(status)?,
                )
                .map_err(status)
            })
            .await
    }
    async fn delete_environment(
        &self,
        request: Request<pb::DeleteEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.delete_environment", &request);
        trace
            .run_result(async {
                let principal = self.peers.authenticate(&request)?;
                if !matches!(principal, Principal::ApiServer | Principal::Coordinator) {
                    return Err(Status::permission_denied(
                        "validated Frontend caller required",
                    ));
                }
                let lifecycle_ready = Arc::clone(&self.manager.lifecycle_ready);
                let gate = lifecycle_ready.read_owned().await;
                if !*gate {
                    return Err(Status::unavailable("node is reconciling"));
                }
                let r = request.into_inner();
                let upgrade_cleanup =
                    principal == Principal::Coordinator && !r.upgrade_task_id.is_empty();
                let fence = if upgrade_cleanup {
                    if r.failed_revision.is_some() || r.expected_revision.is_some() {
                        return Err(Status::invalid_argument(
                            "upgrade cleanup uses task authority instead of a deletion revision",
                        ));
                    }
                    None
                } else {
                    delete_fence(principal.clone(), r.failed_revision, r.expected_revision)
                        .map_err(status)?
                };
                if principal == Principal::Coordinator
                    && (self.session_id.is_empty() || r.node_session_id != self.session_id)
                {
                    return Err(Status::failed_precondition("node session changed"));
                }
                let assignment: adx_core::Assignment = r
                    .assignment
                    .ok_or_else(|| Status::invalid_argument("assignment required"))?
                    .try_into()
                    .map_err(status)?;

                if upgrade_cleanup {
                    let control = self
                        .manager
                        .services
                        .upgrade
                        .control()
                        .map_err(status)?
                        .ok_or_else(|| Status::failed_precondition("upgrade control required"))?;
                    if control.task_id != r.upgrade_task_id
                        || control.epoch != r.upgrade_epoch
                        || !assignment
                            .environment_id
                            .starts_with("default-upgrade-canary-")
                        || control
                            .grants
                            .iter()
                            .any(|grant| grant.environment_id == assignment.environment_id)
                    {
                        return Err(Status::failed_precondition(
                            "canary cleanup authorization changed",
                        ));
                    }
                }
                let managed = self
                    .manager
                    .environments
                    .lock()
                    .expect("shared state lock poisoned")
                    .get(&assignment.environment_id)
                    .cloned();
                let Some((spec, owner, handle)) = managed else {
                    let retired = self
                        .manager
                        .retired_environment(&assignment.environment_id)
                        .ok_or_else(|| Status::not_found("environment not managed on this node"))?;
                    let result = retired
                        .result
                        .ok_or_else(|| Status::not_found("environment not managed on this node"))?;
                    if principal == Principal::ApiServer {
                        tenant(r.caller.as_ref(), &result.record.spec.tenant_id)?;
                    }
                    if result.record.assignment != assignment {
                        return Err(Status::failed_precondition("assignment changed"));
                    }
                    if fence.is_some_and(|fence| retired.coordinator_delete_fence != Some(fence)) {
                        return Err(Status::failed_precondition(
                            "environment deletion fence changed",
                        ));
                    }
                    return response(result).map_err(status);
                };
                if principal == Principal::ApiServer {
                    tenant(r.caller.as_ref(), &spec.tenant_id)?;
                }
                if owner != assignment {
                    return Err(Status::failed_precondition("assignment changed"));
                }
                let result = if principal == Principal::Coordinator {
                    let manager = self.manager.clone();
                    tokio::spawn(adx_observability::trace::with_current_context(async move {
                        let _lifecycle_guard = gate;
                        let result = match fence {
                            Some(RetiredDeleteFence::Revision(revision)) => {
                                handle.delete_at_revision(revision).await
                            }
                            Some(RetiredDeleteFence::Failed(revision)) => {
                                handle.collect_failed(revision).await
                            }
                            None => handle.delete().await,
                        }?;
                        manager.release_published_controller_with_fence(&result, fence);
                        Ok::<_, Error>(result)
                    }))
                    .await
                    .map_err(|_| Status::internal("Coordinator deletion task failed"))?
                } else {
                    handle.delete().await
                }
                .map_err(status)?;
                if principal == Principal::ApiServer {
                    self.manager.release_published_controller(&result);
                }
                response(result).map_err(status)
            })
            .await
    }
}

#[derive(Clone)]
pub struct CoordinatorStateSink {
    volumes: Arc<
        std::sync::RwLock<
            pb::volume_execution_service_client::VolumeExecutionServiceClient<RpcChannel>,
        >,
    >,
    snapshots:
        Arc<std::sync::RwLock<pb::snapshot_service_client::SnapshotServiceClient<RpcChannel>>>,
    client: Arc<
        std::sync::RwLock<pb::coordinator_service_client::CoordinatorServiceClient<RpcChannel>>,
    >,
    session_id: String,
    timeout: Duration,
}
impl CoordinatorStateSink {
    /// Channel carries the node's deployment-provided client certificate.
    pub fn new(channel: Channel, timeout: Duration) -> Result<Self> {
        Self::with_rpc_channel(authenticated_channel(channel), timeout)
    }
    pub fn with_rpc_channel(channel: RpcChannel, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() {
            return Err(Error::Invalid("RPC timeout must be positive".into()));
        }
        Ok(Self {
            volumes: Arc::new(std::sync::RwLock::new(
                pb::volume_execution_service_client::VolumeExecutionServiceClient::new(
                    channel.clone(),
                ),
            )),
            snapshots: Arc::new(std::sync::RwLock::new(
                pb::snapshot_service_client::SnapshotServiceClient::new(channel.clone()),
            )),
            client: Arc::new(std::sync::RwLock::new(
                pb::coordinator_service_client::CoordinatorServiceClient::new(channel),
            )),
            session_id: String::new(),
            timeout,
        })
    }
    pub fn with_session(mut self, session_id: String) -> Self {
        self.session_id = session_id;
        self
    }
    pub fn reconnect(&self, channel: RpcChannel) -> Result<()> {
        *self
            .volumes
            .write()
            .map_err(|_| Error::Unavailable("Volume RPC client unavailable".into()))? =
            pb::volume_execution_service_client::VolumeExecutionServiceClient::new(channel.clone());
        *self
            .snapshots
            .write()
            .map_err(|_| Error::Unavailable("Snapshot RPC client unavailable".into()))? =
            pb::snapshot_service_client::SnapshotServiceClient::new(channel.clone());
        *self
            .client
            .write()
            .map_err(|_| Error::Unavailable("Coordinator RPC client unavailable".into()))? =
            pb::coordinator_service_client::CoordinatorServiceClient::new(channel);
        Ok(())
    }
}
#[async_trait::async_trait]
impl StateSink for CoordinatorStateSink {
    async fn has_current_assignment(&self, record: &EnvironmentRecord) -> Result<bool> {
        let mut client = self
            .client
            .read()
            .expect("shared state lock poisoned")
            .clone();
        let mut request = Request::new(pb::InspectNodeRequest {
            node_id: record.assignment.node_id.clone(),
            session_id: self.session_id.clone(),
        });
        adx_observability::trace::inject_metadata(request.metadata_mut());
        request.set_timeout(self.timeout);
        let catalog = tokio::time::timeout(self.timeout, client.inspect_node(request))
            .await
            .map_err(|_| Error::Unavailable("assignment lookup RPC timed out".into()))?
            .map_err(dependency_status)?
            .into_inner();
        for current in catalog.records {
            let current: EnvironmentRecord = current.try_into()?;
            if current.assignment == record.assignment {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn prepare_execution(
        &self,
        record: &EnvironmentRecord,
        previous_runtime_id: &str,
    ) -> Result<adx_core::EnvironmentSpec> {
        self.prepare_volume_execution(record, previous_runtime_id)
            .await
    }
    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability> {
        self.commit_releasing(record, &[]).await
    }

    async fn commit_deleted(
        &self,
        record: &EnvironmentRecord,
        cleanup_artifacts: &[adx_core::CheckpointArtifact],
        stopped_runtime_ids: &[String],
    ) -> Result<Durability> {
        self.commit_with_details(record, stopped_runtime_ids, cleanup_artifacts)
            .await
    }

    async fn commit_releasing(
        &self,
        record: &EnvironmentRecord,
        stopped_runtime_ids: &[String],
    ) -> Result<Durability> {
        self.commit_with_details(record, stopped_runtime_ids, &[])
            .await
    }
}

impl CoordinatorStateSink {
    async fn commit_with_details(
        &self,
        record: &EnvironmentRecord,
        stopped_runtime_ids: &[String],
        cleanup_artifacts: &[adx_core::CheckpointArtifact],
    ) -> Result<Durability> {
        let mut request = Request::new(pb::CommitEnvironmentRequest {
            stopped_runtime_ids: stopped_runtime_ids.to_vec(),
            record: Some(record.clone().try_into()?),
            node_session_id: self.session_id.clone(),
            cleanup_artifacts: cleanup_artifacts.iter().cloned().map(Into::into).collect(),
        });
        adx_observability::trace::inject_metadata(request.metadata_mut());
        request.set_timeout(self.timeout);
        let mut client = self
            .client
            .read()
            .expect("shared state lock poisoned")
            .clone();
        let response = tokio::time::timeout(self.timeout, client.commit_environment(request))
            .await
            .map_err(|_| Error::Unavailable("state commit RPC timed out".into()))?
            .map_err(|status| {
                adx_observability::warn!(
                    environment_id = %record.spec.id,
                    expected_revision = record.revision,
                    grpc_code = ?status.code(),
                    reason = status.message(),
                    "Coordinator state commit rejected"
                );
                match status.code() {
                    tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
                        Error::Invalid("Coordinator rejected node credentials".into())
                    }
                    _ => dependency_status(status),
                }
            })?
            .into_inner();
        let accepted: EnvironmentRecord = response
            .record
            .ok_or_else(|| Error::Unavailable("commit response missing record".into()))?
            .try_into()?;
        if accepted != *record {
            adx_observability::warn!(
                environment_id = %record.spec.id,
                expected_revision = record.revision,
                accepted_revision = accepted.revision,
                expected_artifacts = record.execution_artifacts.len(),
                accepted_artifacts = accepted.execution_artifacts.len(),
                "Coordinator commit response differs from submitted record"
            );
            return Err(Error::Conflict);
        }
        Ok(Durability::Published)
    }
}

#[async_trait::async_trait]
impl crate::checkpoint::SnapshotCatalog for CoordinatorStateSink {
    async fn get(&self, id: &str) -> Result<adx_core::snapshots::Snapshot> {
        let mut client = self
            .snapshots
            .read()
            .expect("shared state lock poisoned")
            .clone();
        let mut request = Request::new(pb::GetSnapshotRequest {
            id: id.into(),
            caller: None,
            node_session_id: self.session_id.clone(),
        });
        adx_observability::trace::inject_metadata(request.metadata_mut());
        request.set_timeout(self.timeout);
        tokio::time::timeout(self.timeout, client.get_snapshot(request))
            .await
            .map_err(|_| Error::Unavailable("snapshot lookup timed out".into()))?
            .map_err(dependency_status)?
            .into_inner()
            .try_into()
    }
    async fn publish(
        &self,
        snapshot: adx_core::snapshots::Snapshot,
        timeout: Duration,
    ) -> Result<adx_core::snapshots::Snapshot> {
        if timeout.is_zero() {
            return Err(Error::Unavailable("snapshot publication timed out".into()));
        }
        let snapshot_id = snapshot.id.clone();
        let source_environment_id = snapshot.template.id.clone();
        let mut client = self
            .snapshots
            .read()
            .expect("shared state lock poisoned")
            .clone();
        let mut request = Request::new(pb::PublishSnapshotRequest {
            snapshot: Some(snapshot.try_into()?),
            node_session_id: self.session_id.clone(),
        });
        adx_observability::trace::inject_metadata(request.metadata_mut());
        request.set_timeout(timeout);
        tokio::time::timeout(timeout, client.publish_snapshot(request))
            .await
            .map_err(|_| Error::Unavailable("snapshot publication timed out".into()))?
            .map_err(|status| {
                adx_observability::warn!(
                    snapshot_id = %snapshot_id,
                    environment_id = %source_environment_id,
                    grpc_code = ?status.code(),
                    reason = status.message(),
                    "Coordinator snapshot publication rejected"
                );
                dependency_status(status)
            })?
            .into_inner()
            .try_into()
    }
}

#[cfg(test)]
mod preparation_authorization_tests {
    use super::*;
    #[test]
    fn coordinator_cleanup_requires_one_revision_fence() {
        assert!(delete_fence(Principal::Coordinator, None, None).is_err());
        assert!(delete_fence(Principal::Coordinator, Some(3), Some(3)).is_err());
        assert!(delete_fence(Principal::ApiServer, None, Some(3)).is_err());
        assert_eq!(
            delete_fence(Principal::Coordinator, None, Some(3)).unwrap(),
            Some(RetiredDeleteFence::Revision(3))
        );
    }
}
