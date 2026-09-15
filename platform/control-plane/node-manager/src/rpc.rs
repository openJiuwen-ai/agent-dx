//! Node RPC adaptation; InstanceHandle remains the lifecycle owner.
use crate::{Durability, NodeManager, StateSink};
use adx_core::{Error, InstanceRecord, Result};
use adx_protocol::{
    auth::{tenant, Peers, Principal},
    control as pb, dependency_status, status,
};
use std::{sync::Arc, time::Duration};
use tonic::{transport::Channel, Request, Response, Status};

#[derive(Clone)]
pub struct NodeRpc {
    manager: Arc<NodeManager>,
    peers: Peers,
    session_id: String,
}
impl NodeRpc {
    pub fn new(manager: Arc<NodeManager>, peers: Peers, session_id: String) -> Self {
        Self {
            manager,
            peers,
            session_id,
        }
    }
}
fn response(result: crate::OperationResult) -> Result<Response<pb::InstanceResult>> {
    Ok(Response::new(pb::InstanceResult {
        record: Some(result.record.try_into()?),
        durability: match result.durability {
            Durability::Published => pb::Durability::Published,
            Durability::Journaled => pb::Durability::Journaled,
        } as i32,
    }))
}
#[tonic::async_trait]
impl pb::node_service_server::NodeService for NodeRpc {
    async fn create_instance(
        &self,
        request: Request<pb::StartAssignedInstanceRequest>,
    ) -> std::result::Result<Response<pb::InstanceResult>, Status> {
        if self.peers.authenticate(&request)? != Principal::Master {
            return Err(Status::permission_denied(
                "only Master may assign instances",
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
        let handle = self.manager.instance(spec, assignment).map_err(status)?;
        response(handle.create().await.map_err(status)?).map_err(status)
    }
    async fn delete_instance(
        &self,
        request: Request<pb::DeleteInstanceRequest>,
    ) -> std::result::Result<Response<pb::InstanceResult>, Status> {
        if self.peers.authenticate(&request)? != Principal::Frontend {
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
            .instances
            .lock()
            .unwrap()
            .get(&assignment.instance_id)
            .cloned()
            .ok_or_else(|| Status::not_found("instance not managed on this node"))?;
        tenant(r.caller.as_ref(), &spec.tenant_id)?;
        if owner != assignment {
            return Err(Status::failed_precondition("assignment changed"));
        }
        response(handle.delete().await.map_err(status)?).map_err(status)
    }
}

#[derive(Clone)]
pub struct MasterStateSink {
    client: Arc<std::sync::RwLock<pb::master_service_client::MasterServiceClient<Channel>>>,
    session_id: String,
    timeout: Duration,
}
impl MasterStateSink {
    /// Channel carries the node's deployment-provided client certificate.
    pub fn new(channel: Channel, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() {
            return Err(Error::Invalid("RPC timeout must be positive".into()));
        }
        Ok(Self {
            client: Arc::new(std::sync::RwLock::new(
                pb::master_service_client::MasterServiceClient::new(channel),
            )),
            session_id: String::new(),
            timeout,
        })
    }
    pub fn with_session(mut self, session_id: String) -> Self {
        self.session_id = session_id;
        self
    }
    pub fn reconnect(&self, channel: Channel) {
        *self.client.write().unwrap() =
            pb::master_service_client::MasterServiceClient::new(channel);
    }
}
#[async_trait::async_trait]
impl StateSink for MasterStateSink {
    async fn commit(&self, record: &InstanceRecord) -> Result<Durability> {
        let mut request = Request::new(pb::CommitInstanceRequest {
            record: Some(record.clone().try_into()?),
            node_session_id: self.session_id.clone(),
        });
        request.set_timeout(self.timeout);
        let mut client = self.client.read().unwrap().clone();
        let response = tokio::time::timeout(self.timeout, client.commit_instance(request))
            .await
            .map_err(|_| Error::Unavailable("state commit RPC timed out".into()))?
            .map_err(dependency_status)?
            .into_inner();
        let accepted: InstanceRecord = response
            .record
            .ok_or_else(|| Error::Unavailable("commit response missing record".into()))?
            .try_into()?;
        if accepted != *record {
            return Err(Error::Conflict);
        }
        Ok(Durability::Published)
    }
}
