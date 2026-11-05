//! Forwarding DFS owner operations to the owner Node.

use super::*;

#[cfg(feature = "dfs")]
pub struct GrpcDfsOwnerFactory {
    pub(super) peers: Arc<PeerConnectionPool>,
    pub(super) timeout: Duration,
}

#[cfg(feature = "dfs")]
impl GrpcDfsOwnerFactory {
    #[must_use]
    pub fn new(peers: Arc<PeerConnectionPool>, timeout: Duration) -> Self {
        Self { peers, timeout }
    }
}

#[cfg(feature = "dfs")]
impl crate::node::vfs::dfs::DfsRemoteOwnerFactory for GrpcDfsOwnerFactory {
    fn connect(
        &self,
        location: crate::node::vfs::dfs::DfsNodeLocation,
    ) -> afs_error::Result<Arc<dyn RemoteDfsOwner>> {
        GrpcDfsOwnerClient::new(
            self.peers.clone(),
            location.node_id,
            location.node_epoch,
            location.data_endpoint,
            self.timeout,
        )
        .map(|client| Arc::new(client) as Arc<dyn RemoteDfsOwner>)
    }
}

#[cfg(feature = "dfs")]
pub struct GrpcDfsOwnerClient {
    pub(super) peers: Arc<PeerConnectionPool>,
    node_id: String,
    node_epoch: u64,
    pub(super) endpoint: String,
    pub(super) runtime: PeerRuntime,
    pub(super) timeout: Duration,
}

#[cfg(feature = "dfs")]
impl GrpcDfsOwnerClient {
    pub fn new(
        peers: Arc<PeerConnectionPool>,
        node_id: String,
        node_epoch: u64,
        endpoint: String,
        timeout: Duration,
    ) -> afs_error::Result<Self> {
        Ok(Self {
            peers,
            node_id,
            node_epoch,
            endpoint,
            timeout,
            runtime: PeerRuntime::current_or_new("DFS peer")?,
        })
    }

    async fn data_client(
        &self,
    ) -> afs_error::Result<
        afs_protocol::node_data::dfs_owner_files_client::DfsOwnerFilesClient<Channel>,
    > {
        let channel = self
            .peers
            .channel(&self.node_id, self.node_epoch, &self.endpoint)
            .await?;
        Ok(
            afs_protocol::node_data::dfs_owner_files_client::DfsOwnerFilesClient::new(channel)
                .max_encoding_message_size(self.peers.config.max_encoding_message_bytes)
                .max_decoding_message_size(self.peers.config.max_decoding_message_bytes),
        )
    }

    async fn control_client(
        &self,
    ) -> afs_error::Result<
        afs_protocol::node_control::dfs_owner_control_client::DfsOwnerControlClient<Channel>,
    > {
        self.control_client_with_long_wait(false).await
    }

    async fn control_client_with_long_wait(
        &self,
        long_wait: bool,
    ) -> afs_error::Result<
        afs_protocol::node_control::dfs_owner_control_client::DfsOwnerControlClient<Channel>,
    > {
        let channel = if long_wait {
            self.peers
                .long_wait_channel(&self.node_id, self.node_epoch, &self.endpoint)
                .await?
        } else {
            self.peers
                .channel(&self.node_id, self.node_epoch, &self.endpoint)
                .await?
        };
        Ok(
            afs_protocol::node_control::dfs_owner_control_client::DfsOwnerControlClient::new(
                channel,
            )
            .max_encoding_message_size(self.peers.config.max_encoding_message_bytes)
            .max_decoding_message_size(self.peers.config.max_decoding_message_bytes),
        )
    }

    fn dfs_owner_control_request<Req, Reply, Fut, Call>(
        &self,
        request: Req,
        call: Call,
    ) -> afs_error::Result<Reply>
    where
        Req: Send + 'static,
        Reply: Send + 'static,
        Fut: std::future::Future<Output = Result<tonic::Response<Reply>, tonic::Status>> + Send,
        Call: FnOnce(
                afs_protocol::node_control::dfs_owner_control_client::DfsOwnerControlClient<Channel>,
                tonic::Request<Req>,
            ) -> Fut
            + Send
            + 'static,
    {
        self.dfs_owner_control_request_for(request, Some(self.call_timeout()), call)
    }

    /// The configured timeout, shortened by the calling thread's scope. Read it
    /// before `block_on`: the scope is thread-local.
    fn call_timeout(&self) -> Duration {
        crate::node::call_deadline::bounded(self.timeout)
    }

    fn dfs_owner_control_request_for<Req, Reply, Fut, Call>(
        &self,
        request: Req,
        timeout: Option<Duration>,
        call: Call,
    ) -> afs_error::Result<Reply>
    where
        Req: Send + 'static,
        Reply: Send + 'static,
        Fut: std::future::Future<Output = Result<tonic::Response<Reply>, tonic::Status>> + Send,
        Call: FnOnce(
                afs_protocol::node_control::dfs_owner_control_client::DfsOwnerControlClient<Channel>,
                tonic::Request<Req>,
            ) -> Fut
            + Send
            + 'static,
    {
        self.runtime.block_on(async {
            let future = async {
                let client = self
                    .control_client_with_long_wait(timeout.is_none())
                    .await?;
                let mut request = request_with_current_context(request);
                if let Some(timeout) = timeout {
                    request.set_timeout(timeout);
                }
                call(client, request)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(afs_transport::grpc::error_status::status_to_error)
            };
            if let Some(timeout) = timeout {
                tokio::time::timeout(timeout, future).await.map_err(|_| {
                    afs_error::Error::coded(
                        afs_error::CLIENT_DEADLINE_EXCEEDED,
                        "DFS owner control request result may be unknown",
                    )
                })?
            } else {
                future.await
            }
        })
    }
}

#[cfg(feature = "dfs")]
macro_rules! dfs_owner_rpc {
    ($method:ident, $method_async:ident, $client:ident, $client_method:ident, $request:ty, $reply:ty) => {
        fn $method(&self, request: $request) -> afs_error::Result<$reply> {
            self.runtime.block_on(self.$method_async(request))
        }

        fn $method_async(
            &self,
            request: $request,
        ) -> crate::node::read::admission::ReadFuture<'_, $reply> {
            let timeout = self.call_timeout();
            Box::pin(async move {
                if request
                    .handle
                    .as_ref()
                    .is_none_or(|handle| handle.owner_node_id != self.node_id)
                {
                    return Err(dfs_protocol_error("DFS owner request targets another Node"));
                }
                tokio::time::timeout(timeout, async {
                    let mut client = self.$client().await?;
                    let mut request = request_with_current_context(request);
                    request.set_timeout(timeout);
                    client
                        .$client_method(request)
                        .await
                        .map(tonic::Response::into_inner)
                        .map_err(afs_transport::grpc::error_status::status_to_error)
                })
                .await
                .map_err(|_| {
                    afs_error::Error::coded(
                        afs_error::CLIENT_DEADLINE_EXCEEDED,
                        "DFS owner request result may be unknown; preserve its operation ID",
                    )
                })?
            })
        }
    };
}

#[cfg(feature = "dfs")]
impl RemoteDfsOwner for GrpcDfsOwnerClient {
    fn open(
        &self,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerOpenReply> {
        self.runtime.block_on(self.open_async(request))
    }

    fn open_async(
        &self,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> crate::node::read::admission::ReadFuture<'_, afs_protocol::node_control::DfsOwnerOpenReply>
    {
        let timeout = self.call_timeout();
        Box::pin(async move {
            if request.owner_node_id != self.node_id {
                return Err(dfs_protocol_error("DFS owner open targets another Node"));
            }
            tokio::time::timeout(timeout, async {
                let mut client = self.control_client().await?;
                let mut request = request_with_current_context(request);
                request.set_timeout(timeout);
                client
                    .dfs_owner_open(request)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(afs_transport::grpc::error_status::status_to_error)
            })
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "DFS owner open result may be unknown; retry with the same lease",
                )
            })?
        })
    }

    dfs_owner_rpc!(
        getattr,
        getattr_async,
        control_client,
        dfs_owner_get_attr,
        afs_protocol::node_control::DfsOwnerGetAttrRequest,
        afs_protocol::node_control::DfsOwnerGetAttrReply
    );
    dfs_owner_rpc!(
        release,
        release_async,
        control_client,
        dfs_owner_release,
        afs_protocol::node_control::DfsOwnerReleaseRequest,
        afs_protocol::node_control::DfsOwnerReleaseReply
    );
    fn get_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetLockRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerGetLockReply> {
        if request
            .authority
            .as_ref()
            .is_none_or(|authority| authority.owner_node_id != self.node_id)
        {
            return Err(dfs_protocol_error(
                "DFS owner control request targets another Node",
            ));
        }
        self.dfs_owner_control_request(request, |mut client, request| async move {
            client.dfs_owner_get_lock(request).await
        })
    }

    fn set_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerSetLockRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerSetLockReply> {
        if request
            .authority
            .as_ref()
            .is_none_or(|authority| authority.owner_node_id != self.node_id)
        {
            return Err(dfs_protocol_error(
                "DFS owner control request targets another Node",
            ));
        }
        let timeout = if request.waiter.is_some() {
            None
        } else {
            Some(self.call_timeout())
        };
        self.dfs_owner_control_request_for(request, timeout, |mut client, request| async move {
            client.dfs_owner_set_lock(request).await
        })
    }

    fn cancel_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply> {
        if request
            .authority
            .as_ref()
            .is_none_or(|authority| authority.owner_node_id != self.node_id)
        {
            return Err(dfs_protocol_error(
                "DFS owner control request targets another Node",
            ));
        }
        self.dfs_owner_control_request(request, |mut client, request| async move {
            client.dfs_owner_cancel_lock_wait(request).await
        })
    }

    fn acknowledge_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
        if request
            .authority
            .as_ref()
            .is_none_or(|authority| authority.owner_node_id != self.node_id)
        {
            return Err(dfs_protocol_error(
                "DFS owner control request targets another Node",
            ));
        }
        self.dfs_owner_control_request(request, |mut client, request| async move {
            client.dfs_owner_acknowledge_lock_wait(request).await
        })
    }

    fn release_locks(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply> {
        if request
            .authority
            .as_ref()
            .is_none_or(|authority| authority.owner_node_id != self.node_id)
        {
            return Err(dfs_protocol_error(
                "DFS owner control request targets another Node",
            ));
        }
        self.dfs_owner_control_request(request, |mut client, request| async move {
            client.dfs_owner_release_locks(request).await
        })
    }

    fn release_lock_session(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
        if request
            .authority
            .as_ref()
            .is_none_or(|authority| authority.owner_node_id != self.node_id)
        {
            return Err(dfs_protocol_error(
                "DFS owner control request targets another Node",
            ));
        }
        self.dfs_owner_control_request(request, |mut client, request| async move {
            client.dfs_owner_release_lock_session(request).await
        })
    }
    fn read(
        &self,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> afs_error::Result<afs_protocol::node_data::DfsOwnerReadReply> {
        self.runtime.block_on(self.read_async(request))
    }
    fn read_async(
        &self,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> crate::node::read::admission::ReadFuture<'_, afs_protocol::node_data::DfsOwnerReadReply>
    {
        Box::pin(async move {
            if request
                .handle
                .as_ref()
                .is_none_or(|h| h.owner_node_id != self.node_id)
            {
                return Err(dfs_protocol_error("DFS owner request targets another Node"));
            }
            let timeout = crate::node::read::admission::current_read_scope()
                .map(|s| {
                    crate::node::read::admission::remaining_read_budget(s.deadline, self.timeout)
                })
                .transpose()?
                .unwrap_or(self.timeout);
            tokio::time::timeout(timeout, async {
                let mut client = self.data_client().await?;
                let mut request = request_with_current_context(request);
                request.set_timeout(timeout);
                client
                    .read(request)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(afs_transport::grpc::error_status::status_to_error)
            })
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "DFS owner read expired",
                )
            })?
        })
    }
    dfs_owner_rpc!(
        write,
        write_async,
        data_client,
        write,
        afs_protocol::node_data::DfsOwnerWriteRequest,
        afs_protocol::node_data::DfsOwnerWriteReply
    );
    dfs_owner_rpc!(
        resize,
        resize_async,
        data_client,
        resize,
        afs_protocol::node_data::DfsOwnerResizeRequest,
        afs_protocol::node_data::DfsOwnerResizeReply
    );
    dfs_owner_rpc!(
        sync,
        sync_async,
        data_client,
        sync,
        afs_protocol::node_data::DfsOwnerSyncRequest,
        afs_protocol::node_data::DfsOwnerSyncReply
    );
}
