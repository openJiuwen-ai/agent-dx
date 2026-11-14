//! `OwnerControl` service: OwnerFs Home lock authority, root preparation and
//! the OwnerFiles RDMA session negotiation. Registered only in `ownerfs` builds.

use super::{
    BlockingLockWaits, PeerSessionIdentity, RDMA_HANDSHAKE_VERSION, RdmaSessionRegistry,
    negotiate_rdma, owner_rdma_identity_from_wire, rdma_unsupported_reply,
};
use crate::node::rpc::data::{PeerAuthenticator, authenticate_peer};
use afs_protocol::node_control::{
    CloseDataReply, NegotiateDataReply,
    owner_control_server::{OwnerControl, OwnerControlServer},
};
use afs_transport::grpc::error_status::{coded_status, error_to_status};
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct OwnerControlService {
    owner_rdma_registry: Option<RdmaSessionRegistry>,
    lock_waits: BlockingLockWaits,
    owner_locks: Option<crate::node::vfs::ownerfs::OwnerFsPeerExecutor>,
    owner_authenticator: Option<Arc<dyn PeerAuthenticator>>,
}

impl OwnerControlService {
    #[must_use]
    pub fn new(lock_waits: BlockingLockWaits) -> Self {
        Self {
            owner_rdma_registry: None,
            lock_waits,
            owner_locks: None,
            owner_authenticator: None,
        }
    }

    pub fn with_owner_locks(
        mut self,
        handler: crate::node::vfs::ownerfs::OwnerFsPeerExecutor,
        authenticator: Arc<dyn PeerAuthenticator>,
    ) -> Self {
        self.owner_locks = Some(handler);
        self.owner_authenticator = Some(authenticator);
        self
    }
    #[must_use]
    pub fn with_owner_rdma_registry(mut self, registry: RdmaSessionRegistry) -> Self {
        self.owner_rdma_registry = Some(registry);
        self
    }

    pub fn into_server(self) -> OwnerControlServer<Self> {
        OwnerControlServer::new(self)
    }

    async fn dispatch_owner_lock<Req: Send + 'static, Reply: Send + 'static>(
        &self,
        request: Request<Req>,
        blocking: bool,
        call: impl FnOnce(
            crate::node::vfs::ownerfs::OwnerFsPeerExecutor,
            String,
            Req,
        ) -> afs_error::Result<Reply>
        + Send
        + 'static,
    ) -> Result<Response<Reply>, Status> {
        let handler = self
            .owner_locks
            .clone()
            .ok_or_else(|| Status::unimplemented("Owner lock authority not wired"))?;
        let authenticator = self
            .owner_authenticator
            .as_ref()
            .ok_or_else(|| Status::permission_denied("Owner locks require authenticated peer"))?;
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        let body = request.into_inner();
        if blocking {
            return self
                .lock_waits
                .dispatch(move || call(handler, peer, body))
                .await;
        }
        crate::node::bridge::spawn_blocking(move || call(handler, peer, body))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map(Response::new)
            .map_err(error_to_status)
    }
}

#[tonic::async_trait]
impl OwnerControl for OwnerControlService {
    async fn delete_workspace(
        &self,
        request: Request<afs_protocol::node_control::DeleteWorkspaceRequest>,
    ) -> Result<Response<afs_protocol::node_control::DeleteWorkspaceReply>, Status> {
        let permit = self
            .owner_locks
            .as_ref()
            .ok_or_else(|| Status::unimplemented("OwnerFs disabled"))?
            .try_delete_permit()
            .map_err(error_to_status)?;
        self.dispatch_owner_lock(request, false, move |handler, peer, body| {
            handler.delete_workspace(
                &peer,
                &body.requester_session_id,
                &crate::node::vfs::ownerfs::root::RootId(body.root_id),
                body.expected_epoch,
                permit,
            )?;
            Ok(afs_protocol::node_control::DeleteWorkspaceReply {})
        })
        .await
    }

    async fn workspace_defaults(
        &self,
        request: Request<afs_protocol::node_control::WorkspaceDefaultsRequest>,
    ) -> Result<Response<afs_protocol::node_control::WorkspaceDefaultsReply>, Status> {
        let owner = self
            .owner_locks
            .as_ref()
            .ok_or_else(|| Status::unimplemented("OwnerFs is disabled"))?;
        let authenticator = self.owner_authenticator.as_ref().ok_or_else(|| {
            Status::permission_denied("workspace defaults require authenticated peer")
        })?;
        authenticate_peer(authenticator.as_ref(), &request)?;
        let (uid, gid) = owner.workspace_defaults();
        Ok(Response::new(
            afs_protocol::node_control::WorkspaceDefaultsReply { uid, gid },
        ))
    }

    async fn prepare_owner_root(
        &self,
        request: Request<afs_protocol::node_control::PrepareOwnerRootRequest>,
    ) -> Result<Response<afs_protocol::node_control::PrepareOwnerRootReply>, Status> {
        let owner = self.owner_locks.clone().ok_or_else(|| {
            coded_status(afs_error::NODE_VFS_UNIMPLEMENTED, "OwnerFs is disabled")
        })?;
        let authenticator = self.owner_authenticator.as_ref().ok_or_else(|| {
            coded_status(
                afs_error::IO_PERMISSION_DENIED,
                "workspace creation requires peer authentication",
            )
        })?;
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        let reservation = request.reservation.ok_or_else(|| {
            coded_status(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "workspace reservation is required",
            )
        })?;
        let reservation = crate::node::rpc::ownerfs::reservation_from_wire(reservation)
            .map_err(error_to_status)?;
        let (uid, gid) = owner.workspace_defaults();
        let params = crate::node::vfs::ownerfs::root::RootCreateParams {
            name: request.name,
            uid: request.uid.unwrap_or(uid),
            gid: request.gid.unwrap_or(gid),
            mode: request.mode,
        };
        crate::node::bridge::spawn_blocking(move || {
            owner.prepare_owner_root(&peer, &reservation, &params)
        })
        .await
        .map_err(|error| coded_status(afs_error::IO_OTHER, error.to_string()))?
        .map_err(error_to_status)?;
        Ok(Response::new(
            afs_protocol::node_control::PrepareOwnerRootReply {},
        ))
    }

    async fn owner_get_lock(
        &self,
        request: Request<afs_protocol::node_control::OwnerGetLockRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerGetLockReply>, Status> {
        self.dispatch_owner_lock(request, false, |handler, peer, body| {
            let (access, file) = owner_lock_handle_from_wire(body.handle)?;
            let lock = owner_lock_from_wire(body.lock)?;
            let conflict = handler.getlk(&peer, &access, &file, lock)?.map(|conflict| {
                owner_lock_to_wire(&crate::node::vfs::locks::LockRequest {
                    kind: conflict.kind,
                    owner: conflict.owner,
                    pid: conflict.pid,
                    range: conflict.range,
                    lock_type: conflict.lock_type,
                })
            });
            Ok(afs_protocol::node_control::OwnerGetLockReply { conflict })
        })
        .await
    }
    async fn owner_set_lock(
        &self,
        request: Request<afs_protocol::node_control::OwnerSetLockRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerSetLockReply>, Status> {
        let blocking = request.get_ref().waiter.is_some();
        let mut cancel_guard = if let Some(waiter) = request.get_ref().waiter.clone() {
            let handler = self
                .owner_locks
                .clone()
                .ok_or_else(|| Status::unimplemented("Owner lock authority not wired"))?;
            let authenticator = self.owner_authenticator.as_ref().ok_or_else(|| {
                Status::permission_denied("Owner locks require authenticated peer")
            })?;
            let peer = authenticate_peer(authenticator.as_ref(), &request)?;
            let (access, _) = owner_lock_handle_from_wire(request.get_ref().handle.clone())
                .map_err(error_to_status)?;
            let waiter = owner_lock_waiter_from_wire(waiter).map_err(error_to_status)?;
            Some(OwnerLockCancelOnDrop {
                handler,
                peer,
                access,
                waiter: Some(waiter),
            })
        } else {
            None
        };
        let result = self
            .dispatch_owner_lock(request, blocking, |handler, peer, body| {
                let (access, file) = owner_lock_handle_from_wire(body.handle)?;
                let lock = owner_lock_from_wire(body.lock)?;
                let waiter = body.waiter.map(owner_lock_waiter_from_wire).transpose()?;
                handler.setlk(&peer, &access, &file, lock, waiter)?;
                Ok(afs_protocol::node_control::OwnerSetLockReply {})
            })
            .await;
        if let Some(guard) = &mut cancel_guard {
            guard.waiter = None;
        }
        result
    }
    async fn owner_ack_lock_wait(
        &self,
        request: Request<afs_protocol::node_control::OwnerAckLockWaitRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerAckLockWaitReply>, Status> {
        self.dispatch_owner_lock(request, false, |handler, peer, body| {
            let access = crate::node::rpc::data::presented_access(body.access)?;
            let waiter =
                owner_lock_waiter_from_wire(body.waiter.ok_or_else(owner_lock_wire_invalid)?)?;
            handler.acknowledge_lock_wait(&peer, &access, waiter)?;
            Ok(afs_protocol::node_control::OwnerAckLockWaitReply {})
        })
        .await
    }
    async fn owner_cancel_lock_wait(
        &self,
        request: Request<afs_protocol::node_control::OwnerCancelLockWaitRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerCancelLockWaitReply>, Status> {
        self.dispatch_owner_lock(request, false, |handler, peer, body| {
            let access = crate::node::rpc::data::presented_access(body.access)?;
            let waiter =
                owner_lock_waiter_from_wire(body.waiter.ok_or_else(owner_lock_wire_invalid)?)?;
            let outcome = handler.cancel_lock_wait(&peer, &access, waiter)?;
            Ok(afs_protocol::node_control::OwnerCancelLockWaitReply {
                outcome: match outcome {
                    crate::node::vfs::locks::LockWaiterOutcome::Cancelled => 1,
                    crate::node::vfs::locks::LockWaiterOutcome::Granted => 2,
                    crate::node::vfs::locks::LockWaiterOutcome::Unknown => 3,
                },
            })
        })
        .await
    }
    async fn owner_release_locks(
        &self,
        request: Request<afs_protocol::node_control::OwnerReleaseLocksRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerReleaseLocksReply>, Status> {
        self.dispatch_owner_lock(request, false, |handler, peer, body| {
            let (access, file) = owner_lock_handle_from_wire(body.handle)?;
            if body.ingress_session_id.is_empty() || body.ingress_session_id.len() > 256 {
                return Err(owner_lock_wire_invalid());
            }
            let kind = match body.release_kind {
                1 => crate::node::vfs::types::ReleaseKind::PosixOwner,
                2 => crate::node::vfs::types::ReleaseKind::FlockOwner,
                _ => return Err(owner_lock_wire_invalid()),
            };
            handler.release_locks(
                &peer,
                &access,
                &file,
                crate::node::vfs::types::FileLockOwner {
                    ingress_session_id: body.ingress_session_id,
                    kernel_owner: body.kernel_owner,
                },
                kind,
            )?;
            Ok(afs_protocol::node_control::OwnerReleaseLocksReply {})
        })
        .await
    }
    async fn owner_release_lock_session(
        &self,
        request: Request<afs_protocol::node_control::OwnerReleaseLockSessionRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerReleaseLockSessionReply>, Status> {
        self.dispatch_owner_lock(request, false, |handler, peer, body| {
            let access = crate::node::rpc::data::presented_access(body.access)?;
            if body.ingress_session_id.is_empty() || body.ingress_session_id.len() > 256 {
                return Err(owner_lock_wire_invalid());
            }
            handler.release_lock_session(&peer, &access, &body.ingress_session_id)?;
            Ok(afs_protocol::node_control::OwnerReleaseLockSessionReply {})
        })
        .await
    }

    async fn owner_negotiate_data(
        &self,
        request: Request<afs_protocol::node_control::OwnerNegotiateDataRequest>,
    ) -> Result<Response<NegotiateDataReply>, Status> {
        let authenticator = self.owner_authenticator.as_ref().ok_or_else(|| {
            Status::permission_denied("OwnerFiles RDMA requires authenticated peer")
        })?;
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        let body = request.into_inner();
        let access = body
            .access
            .ok_or_else(|| Status::permission_denied("OwnerFiles RDMA access is required"))?;
        let presented_access = crate::node::rpc::data::presented_access(Some(access.clone()))
            .map_err(error_to_status)?;
        let handler = self
            .owner_locks
            .clone()
            .ok_or_else(|| Status::unimplemented("OwnerFs peer authority is not wired"))?;
        let auth_peer = peer.clone();
        let auth_access = presented_access.clone();
        crate::node::bridge::spawn_blocking(move || {
            handler.authorize_transport(&auth_peer, &auth_access)
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .map_err(error_to_status)?;
        let identity = PeerSessionIdentity::for_owner(peer, presented_access)?;
        let negotiation = body
            .negotiation
            .ok_or_else(|| Status::invalid_argument("OwnerFiles RDMA negotiation is required"))?;
        if negotiation.handshake_version != RDMA_HANDSHAKE_VERSION {
            return Err(coded_status(
                afs_error::NODE_RDMA_HANDSHAKE_VERSION,
                "unsupported RDMA handshake version",
            ));
        }
        let registry = self
            .owner_rdma_registry
            .as_ref()
            .ok_or_else(|| Status::unimplemented("OwnerFiles RDMA is not configured"))?;
        let Some(device) = registry.device.clone() else {
            return Ok(Response::new(rdma_unsupported_reply()));
        };
        negotiate_rdma(registry, device, negotiation, Some(identity)).await
    }

    async fn owner_close_data(
        &self,
        request: Request<afs_protocol::node_control::OwnerCloseDataRequest>,
    ) -> Result<Response<CloseDataReply>, Status> {
        let authenticator = self.owner_authenticator.as_ref().ok_or_else(|| {
            Status::permission_denied("OwnerFiles RDMA requires authenticated peer")
        })?;
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        let body = request.into_inner();
        let access = body
            .access
            .ok_or_else(|| Status::permission_denied("OwnerFiles RDMA access is required"))?;
        let identity = owner_rdma_identity_from_wire(peer, access)?;
        let registry = self
            .owner_rdma_registry
            .as_ref()
            .ok_or_else(|| Status::unimplemented("OwnerFiles RDMA is not configured"))?;
        registry.close_for(body.session_id, &identity).await?;
        Ok(Response::new(CloseDataReply {}))
    }
}

pub(in crate::node::rpc) fn owner_lock_to_wire(
    request: &crate::node::vfs::locks::LockRequest,
) -> afs_protocol::node_control::OwnerFileLock {
    use crate::node::vfs::types::{FileLockKind, FileLockType};
    afs_protocol::node_control::OwnerFileLock {
        kind: match request.kind {
            FileLockKind::Posix => 1,
            FileLockKind::Flock => 2,
        },
        ingress_session_id: request.owner.ingress_session_id.clone(),
        kernel_owner: request.owner.kernel_owner,
        pid: request.pid,
        start: request.range.start,
        end: request.range.end,
        lock_type: match request.lock_type {
            FileLockType::Read => 1,
            FileLockType::Write => 2,
            FileLockType::Unlock => 3,
        },
    }
}
pub(in crate::node::rpc) fn owner_lock_from_wire(
    wire: Option<afs_protocol::node_control::OwnerFileLock>,
) -> afs_error::Result<crate::node::vfs::locks::LockRequest> {
    use crate::node::vfs::{
        locks::LockRequest,
        types::{FileLockKind, FileLockOwner, FileLockRange, FileLockType},
    };
    let wire = wire.ok_or_else(owner_lock_wire_invalid)?;
    if wire.ingress_session_id.is_empty()
        || wire.ingress_session_id.len() > 1024
        || wire.start > wire.end
    {
        return Err(owner_lock_wire_invalid());
    }
    Ok(LockRequest {
        kind: match wire.kind {
            1 => FileLockKind::Posix,
            2 => FileLockKind::Flock,
            _ => return Err(owner_lock_wire_invalid()),
        },
        owner: FileLockOwner {
            ingress_session_id: wire.ingress_session_id,
            kernel_owner: wire.kernel_owner,
        },
        pid: wire.pid,
        range: FileLockRange {
            start: wire.start,
            end: wire.end,
        },
        lock_type: match wire.lock_type {
            1 => FileLockType::Read,
            2 => FileLockType::Write,
            3 => FileLockType::Unlock,
            _ => return Err(owner_lock_wire_invalid()),
        },
    })
}
fn owner_lock_handle_from_wire(
    handle: Option<afs_protocol::node_control::OwnerLockHandle>,
) -> afs_error::Result<(
    crate::node::vfs::ownerfs::root::PresentedRootAccess,
    crate::node::vfs::ownerfs::files::RemoteFile,
)> {
    let handle = handle.ok_or_else(owner_lock_wire_invalid)?;
    let access = crate::node::rpc::data::presented_access(handle.access)?;
    let bytes = handle.file.ok_or_else(owner_lock_wire_invalid)?.opaque;
    let identity = handle.identity.ok_or_else(owner_lock_wire_invalid)?.opaque;
    if bytes.len() != 8 || identity.is_empty() || identity.len() > 256 {
        return Err(owner_lock_wire_invalid());
    }
    let mut file = crate::node::rpc::data::remote_file_for_handle(&access, bytes);
    file.identity = crate::node::vfs::ownerfs::files::FileIdentity(identity);
    Ok((access, file))
}
fn owner_lock_waiter_from_wire(
    waiter: afs_protocol::node_control::OwnerLockWaiter,
) -> afs_error::Result<crate::node::vfs::locks::LockWaiterId> {
    if waiter.ingress_session_id.is_empty() || waiter.ingress_session_id.len() > 256 {
        return Err(owner_lock_wire_invalid());
    }
    Ok(crate::node::vfs::locks::LockWaiterId {
        ingress_session_id: waiter.ingress_session_id,
        request_id: waiter.request_id,
    })
}
fn owner_lock_wire_invalid() -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::CLIENT_ARGUMENT_INVALID,
        "Owner advisory lock authority/request is incomplete",
    )
}
struct OwnerLockCancelOnDrop {
    handler: crate::node::vfs::ownerfs::OwnerFsPeerExecutor,
    peer: String,
    access: crate::node::vfs::ownerfs::root::PresentedRootAccess,
    waiter: Option<crate::node::vfs::locks::LockWaiterId>,
}
impl Drop for OwnerLockCancelOnDrop {
    fn drop(&mut self) {
        if let Some(waiter) = self.waiter.take() {
            let handler = self.handler.clone();
            let peer = self.peer.clone();
            let access = self.access.clone();
            // Cancel the exact pending request when the HTTP/2 response future
            // is dropped. A raced Granted outcome is retained; no broad unlock.
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                crate::node::bridge::spawn_blocking_on(&runtime, move || {
                    let _ = handler.cancel_lock_wait(&peer, &access, waiter);
                });
            }
        }
    }
}
