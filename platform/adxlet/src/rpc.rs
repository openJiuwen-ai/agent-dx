//! Node RPC adaptation; EnvironmentHandle remains the lifecycle owner.
use adx_transport::rpc::{authenticated_channel, RpcChannel};
mod local_create;
use crate::{Adxlet, Durability, StateSink};
use adx_core::{EnvironmentRecord, Error, Result};
use adx_protocol::{
    auth::{tenant, Peers, Principal},
    control as pb, dependency_status, status,
};
use std::{sync::Arc, time::Duration};
use tonic::{transport::Channel, Request, Response, Status};

#[derive(Clone)]
pub struct NodeRpc {
    manager: Arc<Adxlet>,
    peers: Peers,
    session_id: String,
    coordinator: Option<CoordinatorStateSink>,
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
        }
    }
}
fn response(result: crate::OperationResult) -> Result<Response<pb::EnvironmentResult>> {
    adx_observability::info!(event="environment_operation_completed", traceparent=?adx_observability::trace::traceparent(), environment_id=%result.record.spec.id,
        generation=result.record.assignment.generation, revision=result.record.revision,
        state=?result.record.state, "environment operation completed");
    Ok(Response::new(pb::EnvironmentResult {
        record: Some(result.record.try_into()?),
        durability: match result.durability {
            Durability::Published => pb::Durability::Published,
            Durability::Journaled => pb::Durability::Journaled,
        } as i32,
    }))
}
#[tonic::async_trait]
impl pb::node_service_server::NodeService for NodeRpc {
    async fn create_local_environment(
        &self,
        request: Request<pb::LocalEnvironmentCreateRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.create_local_environment", &request);
        if self.peers.authenticate(&request)? != Principal::ApiServer {
            return Err(Status::permission_denied("API Server required"));
        }
        let service = self.clone();
        let request = request.into_inner();
        let timeout = request
            .create
            .as_ref()
            .map(|create| Duration::from_secs(create.create_timeout_seconds))
            .filter(|timeout| !timeout.is_zero())
            .or_else(|| service.coordinator.as_ref().map(|sink| sink.timeout))
            .unwrap_or(Duration::from_secs(90));
        let deadline = tokio::time::Instant::now() + timeout;
        tokio::spawn(trace.run(async move { service.create_local(request, deadline).await }))
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
                        .recover_environment(record)
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
                let saved = handle
                    .snapshot(crate::checkpoint::SnapshotRequest {
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
                    .collect_snapshot(&snapshot)
                    .await
                    .map_err(status)?;
                Ok(Response::new(pb::CollectSnapshotResponse {
                    snapshot_id: snapshot.id,
                    revision: snapshot.revision,
                }))
            })
            .await
    }
    async fn create_environment(
        &self,
        request: Request<pb::StartAssignedEnvironmentRequest>,
    ) -> std::result::Result<Response<pb::EnvironmentResult>, Status> {
        let trace = adx_observability::trace::Trace::rpc("node.create_environment", &request);
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
                    Some(snapshot) => {
                        handle
                            .create_from_snapshot(snapshot.try_into().map_err(status)?)
                            .await
                    }
                    None => handle.create().await,
                };
                response(result.map_err(status)?).map_err(status)
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
                        .pause(crate::checkpoint::PauseRequest {
                            operation_id: r.operation_id,
                            expected_revision: r.expected_revision,
                            ttl_seconds: r.ttl_seconds,
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
                let assignment: adx_core::Assignment = r
                    .assignment
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
                tenant(r.caller.as_ref(), &spec.tenant_id)?;
                if owner != assignment {
                    return Err(Status::failed_precondition("assignment changed"));
                }
                response(handle.delete().await.map_err(status)?).map_err(status)
            })
            .await
    }
}

#[derive(Clone)]
pub struct CoordinatorStateSink {
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
    pub fn reconnect(&self, channel: RpcChannel) {
        *self.snapshots.write().expect("shared state lock poisoned") =
            pb::snapshot_service_client::SnapshotServiceClient::new(channel.clone());
        *self.client.write().expect("shared state lock poisoned") =
            pb::coordinator_service_client::CoordinatorServiceClient::new(channel);
    }
}
#[async_trait::async_trait]
impl StateSink for CoordinatorStateSink {
    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability> {
        let mut request = Request::new(pb::CommitEnvironmentRequest {
            record: Some(record.clone().try_into()?),
            node_session_id: self.session_id.clone(),
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
            .map_err(|status| match status.code() {
                tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => {
                    Error::Invalid("Coordinator rejected node credentials".into())
                }
                _ => dependency_status(status),
            })?
            .into_inner();
        let accepted: EnvironmentRecord = response
            .record
            .ok_or_else(|| Error::Unavailable("commit response missing record".into()))?
            .try_into()?;
        if accepted != *record {
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
    ) -> Result<adx_core::snapshots::Snapshot> {
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
        request.set_timeout(self.timeout);
        tokio::time::timeout(self.timeout, client.publish_snapshot(request))
            .await
            .map_err(|_| Error::Unavailable("snapshot publication timed out".into()))?
            .map_err(dependency_status)?
            .into_inner()
            .try_into()
    }
}
