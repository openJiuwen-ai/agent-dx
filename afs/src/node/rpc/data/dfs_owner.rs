//! Server side of forwarded DFS owner operations.

use super::*;

#[cfg(feature = "dfs")]
#[derive(Clone, Default)]
pub struct DfsOwnerFilesService {
    handler: Option<Arc<dyn DfsOwnerFilesHandler>>,
    pub(super) authenticator: Option<Arc<dyn PeerAuthenticator>>,
}

#[cfg(feature = "dfs")]
impl DfsOwnerFilesService {
    pub fn new(
        handler: Arc<dyn DfsOwnerFilesHandler>,
        authenticator: Arc<dyn PeerAuthenticator>,
    ) -> Self {
        Self {
            handler: Some(handler),
            authenticator: Some(authenticator),
        }
    }

    async fn dispatch<Req: Send + 'static, Reply: Send + 'static>(
        &self,
        request: Request<Req>,
        handle: &DfsOwnerHandle,
        operation_id: Option<&str>,
        call: impl FnOnce(Arc<dyn DfsOwnerFilesHandler>, String, Req) -> OwnerFuture<Reply>,
    ) -> Result<Response<Reply>, Status> {
        let handler = self
            .handler
            .clone()
            .ok_or_else(|| error_to_status(dfs_owner_unimplemented()))?;
        let authenticator = self
            .authenticator
            .as_ref()
            .ok_or_else(|| Status::permission_denied("DFS owner has no peer authenticator"))?;
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        if handle.namespace_id.is_empty()
            || handle.inode_id.is_empty()
            || handle.owner_node_id.is_empty()
            || handle.owner_session_id.is_empty()
            || handle.lease_epoch == 0
            || handle.caller_node_id != peer
            || handle.caller_session_id.is_empty()
            || handle.open_seq == 0
            || handle.opaque_handle.is_empty()
            || operation_id.is_some_and(str::is_empty)
        {
            return Err(Status::permission_denied(
                "DFS owner handle identity is incomplete or mismatched",
            ));
        }
        let request = request.into_inner();
        crate::node::bridge::run_detached(call(handler, peer, request))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map(Response::new)
            .map_err(error_to_status)
    }
}

#[cfg(feature = "dfs")]
pub fn make_dfs_owner_files_server(
    service: DfsOwnerFilesService,
) -> DfsOwnerFilesServer<DfsOwnerFilesService> {
    DfsOwnerFilesServer::new(service)
}

#[cfg(feature = "dfs")]
#[tonic::async_trait]
impl DfsOwnerFiles for DfsOwnerFilesService {
    async fn read(
        &self,
        request: Request<DfsOwnerReadRequest>,
    ) -> Result<Response<DfsOwnerReadReply>, Status> {
        let handle = request.get_ref().handle.clone();
        if request.get_ref().length > MAX_TRANSFER_BYTES as u64 {
            return Err(Status::invalid_argument("owner read exceeds inline budget"));
        }
        let handle =
            handle.ok_or_else(|| Status::permission_denied("DFS owner handle is required"))?;
        self.dispatch(request, &handle, None, |handler, peer, request| {
            handler.read_async(peer, request)
        })
        .await
    }
    async fn write(
        &self,
        request: Request<DfsOwnerWriteRequest>,
    ) -> Result<Response<DfsOwnerWriteReply>, Status> {
        let handle = request.get_ref().handle.clone();
        let operation = request.get_ref().operation_id.clone();
        if request.get_ref().data.len() > MAX_TRANSFER_BYTES {
            return Err(Status::invalid_argument(
                "owner write exceeds inline budget",
            ));
        }
        self.dispatch(
            request,
            &handle.ok_or_else(|| Status::permission_denied("DFS owner handle is required"))?,
            Some(&operation),
            |handler, peer, request| handler.write_async(peer, request),
        )
        .await
    }
    async fn resize(
        &self,
        request: Request<DfsOwnerResizeRequest>,
    ) -> Result<Response<DfsOwnerResizeReply>, Status> {
        let handle = request.get_ref().handle.clone();
        let operation = request.get_ref().operation_id.clone();
        self.dispatch(
            request,
            &handle.ok_or_else(|| Status::permission_denied("DFS owner handle is required"))?,
            Some(&operation),
            |handler, peer, request| handler.resize_async(peer, request),
        )
        .await
    }
    async fn sync(
        &self,
        request: Request<DfsOwnerSyncRequest>,
    ) -> Result<Response<DfsOwnerSyncReply>, Status> {
        let handle = request.get_ref().handle.clone();
        let operation = request.get_ref().operation_id.clone();
        self.dispatch(
            request,
            &handle.ok_or_else(|| Status::permission_denied("DFS owner handle is required"))?,
            Some(&operation),
            |handler, peer, request| handler.sync_async(peer, request),
        )
        .await
    }
}
