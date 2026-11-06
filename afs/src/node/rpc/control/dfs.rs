//! `DfsOwnerControl` service: the DFS owner handle lifecycle and advisory lock
//! proxy between Nodes. Registered only in `dfs` builds.

use super::BlockingLockWaits;
use crate::node::{
    rpc::data::{PeerAuthenticator, authenticate_peer},
    vfs::dfs::owner_api::{
        DfsOwnerLifecycleHandler, OwnerFuture, dfs_owner_lifecycle_unimplemented,
    },
};
use afs_protocol::node_control::{
    DfsOwnerAcknowledgeLockWaitReply, DfsOwnerAcknowledgeLockWaitRequest,
    DfsOwnerCancelLockWaitReply, DfsOwnerCancelLockWaitRequest, DfsOwnerGetAttrReply,
    DfsOwnerGetAttrRequest, DfsOwnerGetLockReply, DfsOwnerGetLockRequest, DfsOwnerMknodReply,
    DfsOwnerMknodRequest, DfsOwnerOpenReply, DfsOwnerOpenRequest, DfsOwnerReleaseLockSessionReply,
    DfsOwnerReleaseLockSessionRequest, DfsOwnerReleaseLocksReply, DfsOwnerReleaseLocksRequest,
    DfsOwnerReleaseReply, DfsOwnerReleaseRequest, DfsOwnerSetLockReply, DfsOwnerSetLockRequest,
    dfs_owner_control_server::{DfsOwnerControl, DfsOwnerControlServer},
};
use afs_transport::grpc::error_status::error_to_status;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct DfsOwnerControlService {
    dfs_owner: Option<Arc<dyn DfsOwnerLifecycleHandler>>,
    dfs_authenticator: Option<Arc<dyn PeerAuthenticator>>,
    lock_waits: BlockingLockWaits,
}

impl DfsOwnerControlService {
    /// A Node compiled with DFS but running without it: every call reports the
    /// lifecycle as unimplemented.
    #[must_use]
    pub fn new(lock_waits: BlockingLockWaits) -> Self {
        Self {
            dfs_owner: None,
            dfs_authenticator: None,
            lock_waits,
        }
    }

    #[must_use]
    pub fn with_dfs_owner(
        lock_waits: BlockingLockWaits,
        handler: Arc<dyn DfsOwnerLifecycleHandler>,
        authenticator: Arc<dyn PeerAuthenticator>,
    ) -> Self {
        Self {
            dfs_owner: Some(handler),
            dfs_authenticator: Some(authenticator),
            lock_waits,
        }
    }

    pub fn into_server(self) -> DfsOwnerControlServer<Self> {
        DfsOwnerControlServer::new(self)
    }

    async fn dispatch_dfs_owner<Req: Send + 'static, Reply: Send + 'static>(
        &self,
        request: Request<Req>,
        call: impl FnOnce(Arc<dyn DfsOwnerLifecycleHandler>, String, Req) -> afs_error::Result<Reply>
        + Send
        + 'static,
    ) -> Result<Response<Reply>, Status> {
        let handler = self
            .dfs_owner
            .clone()
            .ok_or_else(|| error_to_status(dfs_owner_lifecycle_unimplemented()))?;
        let authenticator = self
            .dfs_authenticator
            .as_ref()
            .ok_or_else(|| Status::permission_denied("DFS owner has no peer authenticator"))?;
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        crate::node::bridge::spawn_blocking(move || call(handler, peer, request))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map(Response::new)
            .map_err(error_to_status)
    }

    /// Lifecycle RPCs whose handler serves them on the async worker.
    async fn dispatch_dfs_owner_async<Req: Send + 'static, Reply: Send + 'static>(
        &self,
        request: Request<Req>,
        call: impl FnOnce(Arc<dyn DfsOwnerLifecycleHandler>, String, Req) -> OwnerFuture<Reply>,
    ) -> Result<Response<Reply>, Status> {
        let handler = self
            .dfs_owner
            .clone()
            .ok_or_else(|| error_to_status(dfs_owner_lifecycle_unimplemented()))?;
        let authenticator = self
            .dfs_authenticator
            .as_ref()
            .ok_or_else(|| Status::permission_denied("DFS owner has no peer authenticator"))?;
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        crate::node::bridge::run_detached(call(handler, peer, request))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map(Response::new)
            .map_err(error_to_status)
    }
    async fn dispatch_blocking_dfs_owner_lock<Req: Send + 'static, Reply: Send + 'static>(
        &self,
        request: Request<Req>,
        call: impl FnOnce(Arc<dyn DfsOwnerLifecycleHandler>, String, Req) -> afs_error::Result<Reply>
        + Send
        + 'static,
    ) -> Result<Response<Reply>, Status> {
        let handler = self
            .dfs_owner
            .clone()
            .ok_or_else(|| error_to_status(dfs_owner_lifecycle_unimplemented()))?;
        let authenticator = self
            .dfs_authenticator
            .as_ref()
            .ok_or_else(|| Status::permission_denied("DFS owner has no peer authenticator"))?;
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        self.lock_waits
            .dispatch(move || call(handler, peer, request))
            .await
    }
}

#[tonic::async_trait]
impl DfsOwnerControl for DfsOwnerControlService {
    async fn dfs_owner_open(
        &self,
        request: Request<DfsOwnerOpenRequest>,
    ) -> Result<Response<DfsOwnerOpenReply>, Status> {
        let body = request.get_ref();
        if body.namespace_id.is_empty()
            || body.inode_id.is_empty()
            || body.owner_node_id.is_empty()
            || body.owner_session_id.is_empty()
            || body.lease_epoch == 0
            || body.caller_session_id.is_empty()
            || body.open_seq == 0
        {
            return Err(Status::permission_denied(
                "DFS owner open identity is incomplete",
            ));
        }
        self.dispatch_dfs_owner_async(request, move |handler, peer, request| {
            handler.open_async(peer, request)
        })
        .await
    }
    async fn dfs_owner_get_attr(
        &self,
        request: Request<DfsOwnerGetAttrRequest>,
    ) -> Result<Response<DfsOwnerGetAttrReply>, Status> {
        validate_dfs_owner_control_handle(request.get_ref().handle.as_ref())?;
        self.dispatch_dfs_owner_async(request, |handler, peer, request| {
            handler.getattr_async(peer, request)
        })
        .await
    }
    async fn dfs_owner_mknod(
        &self,
        request: Request<DfsOwnerMknodRequest>,
    ) -> Result<Response<DfsOwnerMknodReply>, Status> {
        validate_dfs_owner_mknod_request(request.get_ref())?;
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.mknod(&peer, request)
        })
        .await
    }
    async fn dfs_owner_get_lock(
        &self,
        request: Request<DfsOwnerGetLockRequest>,
    ) -> Result<Response<DfsOwnerGetLockReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        validate_dfs_owner_lock_request(request.get_ref().lock.as_ref())?;
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.get_lock(&peer, request)
        })
        .await
    }
    async fn dfs_owner_set_lock(
        &self,
        request: Request<DfsOwnerSetLockRequest>,
    ) -> Result<Response<DfsOwnerSetLockReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        validate_dfs_owner_lock_request(request.get_ref().lock.as_ref())?;
        if let Some(waiter) = request.get_ref().waiter.as_ref() {
            validate_dfs_owner_lock_waiter(waiter)?;
        }
        if request.get_ref().waiter.is_some() {
            self.dispatch_blocking_dfs_owner_lock(request, |handler, peer, request| {
                handler.set_lock(&peer, request)
            })
            .await
        } else {
            self.dispatch_dfs_owner(request, |handler, peer, request| {
                handler.set_lock(&peer, request)
            })
            .await
        }
    }
    async fn dfs_owner_cancel_lock_wait(
        &self,
        request: Request<DfsOwnerCancelLockWaitRequest>,
    ) -> Result<Response<DfsOwnerCancelLockWaitReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        let waiter = request
            .get_ref()
            .waiter
            .as_ref()
            .ok_or_else(|| Status::permission_denied("DFS lock waiter is required"))?;
        validate_dfs_owner_lock_waiter(waiter)?;
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.cancel_lock_wait(&peer, request)
        })
        .await
    }
    async fn dfs_owner_acknowledge_lock_wait(
        &self,
        request: Request<DfsOwnerAcknowledgeLockWaitRequest>,
    ) -> Result<Response<DfsOwnerAcknowledgeLockWaitReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        let waiter = request
            .get_ref()
            .waiter
            .as_ref()
            .ok_or_else(|| Status::permission_denied("DFS lock waiter is required"))?;
        validate_dfs_owner_lock_waiter(waiter)?;
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.acknowledge_lock_wait(&peer, request)
        })
        .await
    }
    async fn dfs_owner_release_locks(
        &self,
        request: Request<DfsOwnerReleaseLocksRequest>,
    ) -> Result<Response<DfsOwnerReleaseLocksReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        validate_dfs_owner_lock_owner(request.get_ref().owner.as_ref())?;
        if request.get_ref().release_kind == 0 {
            return Err(Status::invalid_argument(
                "DFS lock release kind is required",
            ));
        }
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.release_locks(&peer, request)
        })
        .await
    }
    async fn dfs_owner_release_lock_session(
        &self,
        request: Request<DfsOwnerReleaseLockSessionRequest>,
    ) -> Result<Response<DfsOwnerReleaseLockSessionReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        if request.get_ref().ingress_session_id.is_empty() {
            return Err(Status::permission_denied(
                "DFS lock release session is incomplete",
            ));
        }
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.release_lock_session(&peer, request)
        })
        .await
    }
    async fn dfs_owner_release(
        &self,
        request: Request<DfsOwnerReleaseRequest>,
    ) -> Result<Response<DfsOwnerReleaseReply>, Status> {
        validate_dfs_owner_release_handle(request.get_ref().handle.as_ref())?;
        self.dispatch_dfs_owner_async(request, |handler, peer, request| {
            handler.release_async(peer, request)
        })
        .await
    }
}

fn validate_dfs_owner_lock_authority(
    authority: Option<&afs_protocol::node_control::DfsOwnerLockAuthority>,
) -> Result<(), Status> {
    let authority =
        authority.ok_or_else(|| Status::permission_denied("DFS lock authority is required"))?;
    if authority.namespace_id.is_empty()
        || authority.inode_id.is_empty()
        || authority.owner_node_id.is_empty()
        || authority.owner_session_id.is_empty()
        || authority.lease_epoch == 0
        || authority.lease_expires_at_unix_ms == 0
        || authority.caller_node_id.is_empty()
        || authority.caller_session_id.is_empty()
    {
        return Err(Status::permission_denied(
            "DFS lock authority identity is incomplete",
        ));
    }
    Ok(())
}
fn validate_dfs_owner_lock_owner(
    owner: Option<&afs_protocol::node_control::DfsOwnerLockOwner>,
) -> Result<(), Status> {
    let owner = owner.ok_or_else(|| Status::permission_denied("DFS lock owner is required"))?;
    if owner.ingress_session_id.is_empty() {
        return Err(Status::permission_denied(
            "DFS lock owner session is incomplete",
        ));
    }
    Ok(())
}
fn validate_dfs_owner_lock_waiter(
    waiter: &afs_protocol::node_control::DfsOwnerLockWaiter,
) -> Result<(), Status> {
    if waiter.ingress_session_id.is_empty() {
        return Err(Status::permission_denied(
            "DFS lock waiter session is incomplete",
        ));
    }
    Ok(())
}
fn validate_dfs_owner_lock_request(
    request: Option<&afs_protocol::node_control::DfsOwnerLockRequest>,
) -> Result<(), Status> {
    let request =
        request.ok_or_else(|| Status::permission_denied("DFS lock request is required"))?;
    if request.kind == 0 || request.lock_type == 0 {
        return Err(Status::invalid_argument("DFS lock kind/type is required"));
    }
    validate_dfs_owner_lock_owner(request.owner.as_ref())?;
    let range = request
        .range
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("DFS lock range is required"))?;
    if range.start > range.end {
        return Err(Status::invalid_argument("DFS lock range is invalid"));
    }
    Ok(())
}
fn validate_dfs_owner_mknod_request(request: &DfsOwnerMknodRequest) -> Result<(), Status> {
    if request.namespace_id.is_empty()
        || request.parent_inode_id.is_empty()
        || request.owner_node_id.is_empty()
        || request.owner_session_id.is_empty()
        || request.lease_epoch == 0
        || request.caller_session_id.is_empty()
        || request.name.is_empty()
    {
        return Err(Status::permission_denied(
            "DFS owner mknod identity is incomplete",
        ));
    }
    let special = request
        .special_node
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("DFS owner mknod missing special_node"))?;
    match afs_protocol::node_control::DfsOwnerSpecialNodeKind::try_from(special.kind) {
        Ok(afs_protocol::node_control::DfsOwnerSpecialNodeKind::Fifo)
        | Ok(afs_protocol::node_control::DfsOwnerSpecialNodeKind::Socket)
            if special.rdev == 0 =>
        {
            Ok(())
        }
        Ok(afs_protocol::node_control::DfsOwnerSpecialNodeKind::BlockDevice)
        | Ok(afs_protocol::node_control::DfsOwnerSpecialNodeKind::CharDevice) => Ok(()),
        _ => Err(Status::invalid_argument(
            "DFS owner mknod has invalid special kind/rdev",
        )),
    }
}
fn validate_dfs_owner_control_handle(
    handle: Option<&afs_protocol::node_control::DfsOwnerHandle>,
) -> Result<(), Status> {
    let handle = handle.ok_or_else(|| Status::permission_denied("DFS owner handle is required"))?;
    if handle.namespace_id.is_empty()
        || handle.inode_id.is_empty()
        || handle.owner_node_id.is_empty()
        || handle.owner_session_id.is_empty()
        || handle.lease_epoch == 0
        || handle.caller_node_id.is_empty()
        || handle.caller_session_id.is_empty()
        || handle.open_seq == 0
        || handle.opaque_handle.is_empty()
    {
        return Err(Status::permission_denied(
            "DFS owner handle identity is incomplete",
        ));
    }
    Ok(())
}
fn validate_dfs_owner_release_handle(
    handle: Option<&afs_protocol::node_control::DfsOwnerHandle>,
) -> Result<(), Status> {
    let handle = handle.ok_or_else(|| Status::permission_denied("DFS owner handle is required"))?;
    if handle.namespace_id.is_empty()
        || handle.inode_id.is_empty()
        || handle.owner_node_id.is_empty()
        || handle.owner_session_id.is_empty()
        || handle.lease_epoch == 0
        || handle.caller_node_id.is_empty()
        || handle.caller_session_id.is_empty()
        || handle.open_seq == 0
    {
        return Err(Status::permission_denied(
            "DFS owner release identity is incomplete",
        ));
    }
    Ok(())
}
