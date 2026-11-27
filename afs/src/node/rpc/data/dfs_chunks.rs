//! The DfsChunks peer service.

use super::*;

#[cfg(feature = "dfs")]
const REPLICA_RECEIVE_SLOTS: usize = 8;

#[cfg(feature = "dfs")]
struct ReplicaAdmission {
    _receive: tokio::sync::OwnedSemaphorePermit,
    _forward: Option<tokio::sync::OwnedSemaphorePermit>,
}
#[cfg(feature = "dfs")]
#[derive(Clone)]
pub struct DfsPayloadMetrics {
    pub(super) bytes: afs_metrics::IntCounterVec,
    pub(super) admissions: afs_metrics::IntCounterVec,
}

#[cfg(feature = "dfs")]
impl DfsPayloadMetrics {
    pub fn register(registry: &afs_metrics::Registry) -> Result<Self, afs_metrics::MetricsError> {
        registry.get_or_register(|registry| {
            let bytes = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_payload_bytes_total",
                    "Actual DFS file payload bytes transferred; descriptors are excluded.",
                ),
                &["transport", "direction", "operation"],
            )?;
            afs_metrics::register_collector(registry, &bytes)?;
            let admissions = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_peer_admissions_total",
                    "Peer replica receive slots: taken at once, taken after queueing, or rejected when the admission deadline expired.",
                ),
                &["operation", "outcome"],
            )?;
            afs_metrics::register_collector(registry, &admissions)?;
            for transport in ["grpc", "rdma"] {
                for (direction, operation) in [("recv", "replica"), ("send", "read")] {
                    bytes.with_label_values(&[transport, direction, operation]);
                }
            }
            for outcome in ["immediate", "waited", "rejected"] {
                admissions.with_label_values(&["replica", outcome]);
            }
            Ok(Self { bytes, admissions })
        })
    }
    fn admission(&self, operation: &'static str, outcome: &'static str) {
        self.admissions
            .with_label_values(&[operation, outcome])
            .inc();
    }
    pub(super) fn record(
        &self,
        transport: &'static str,
        direction: &'static str,
        operation: &'static str,
        bytes: u64,
    ) {
        self.bytes
            .with_label_values(&[transport, direction, operation])
            .inc_by(bytes);
    }
}

#[cfg(feature = "dfs")]
pub struct DfsChunkTransportResources {
    pub rdma_sessions: super::super::control::RdmaSessionRegistry,
    pub payload_metrics: Option<DfsPayloadMetrics>,
    /// H2 step 2: serve holder-form reads (`None`: refuse them).
    pub holders: Option<Arc<super::dfs_holder_serve::HolderServe>>,
}

// Existing constructors remain wrappers passing a disabled registry and no
// metrics. Only Node assembly uses the new focused constructor with shared resources.

#[cfg(feature = "dfs")]
#[derive(Clone)]
pub struct DfsChunksService {
    pub(super) local_chunks: Option<Arc<LocalChunkStore>>,
    pub(super) authenticator: Arc<dyn PeerAuthenticator>,
    pub(super) authorizer: Arc<dyn DfsReadAuthorizer>,
    pub(super) replica_authorizer: Arc<dyn DfsReplicaAuthorizer>,
    pub(super) replica_data_plane: Option<Arc<dyn crate::node::replication::ReplicaDataPlane>>,
    pub(super) replica_permits: Arc<tokio::sync::Semaphore>,
    pub(super) replica_forward_permits: Arc<tokio::sync::Semaphore>,
    pub(super) replica_authorization_permits: Arc<tokio::sync::Semaphore>,
    pub(super) read_permits: Arc<tokio::sync::Semaphore>,
    pub(super) replica_timeout: std::time::Duration,
    pub(super) rdma_sessions: super::super::control::RdmaSessionRegistry,
    pub(super) payload_metrics: Option<DfsPayloadMetrics>,
    pub(super) suspect_reviews: Arc<crate::node::fault::suspect::SuspectReviewGate>,
    /// H2 step 2: answers holder-form reads when this Node shares versions.
    pub(super) holders: Option<Arc<super::dfs_holder_serve::HolderServe>>,
}

#[cfg(all(feature = "dfs", test))]
pub fn make_dfs_chunks_server(
    local_chunks: Option<Arc<LocalChunkStore>>,
    authenticator: Arc<dyn PeerAuthenticator>,
    authorizer: Arc<dyn DfsReadAuthorizer>,
) -> DfsChunksServer<DfsChunksService> {
    make_dfs_chunks_server_with_replication(
        local_chunks,
        authenticator,
        authorizer,
        Arc::new(DenyDfsReplicaAuthorizer),
        None,
        std::time::Duration::from_secs(30),
    )
}

#[cfg(all(feature = "dfs", test))]
pub fn make_dfs_chunks_server_with_replication(
    local_chunks: Option<Arc<LocalChunkStore>>,
    authenticator: Arc<dyn PeerAuthenticator>,
    authorizer: Arc<dyn DfsReadAuthorizer>,
    replica_authorizer: Arc<dyn DfsReplicaAuthorizer>,
    replica_data_plane: Option<Arc<dyn crate::node::replication::ReplicaDataPlane>>,
    replica_timeout: std::time::Duration,
) -> DfsChunksServer<DfsChunksService> {
    make_dfs_chunks_server_with_transport(
        local_chunks,
        authenticator,
        authorizer,
        replica_authorizer,
        replica_data_plane,
        replica_timeout,
        DfsChunkTransportResources {
            rdma_sessions: super::super::control::RdmaSessionRegistry::new(None),
            payload_metrics: None,
            holders: None,
        },
    )
}

#[cfg(feature = "dfs")]
pub fn make_dfs_chunks_server_with_transport(
    local_chunks: Option<Arc<LocalChunkStore>>,
    authenticator: Arc<dyn PeerAuthenticator>,
    authorizer: Arc<dyn DfsReadAuthorizer>,
    replica_authorizer: Arc<dyn DfsReplicaAuthorizer>,
    replica_data_plane: Option<Arc<dyn crate::node::replication::ReplicaDataPlane>>,
    replica_timeout: std::time::Duration,
    resources: DfsChunkTransportResources,
) -> DfsChunksServer<DfsChunksService> {
    DfsChunksServer::new(DfsChunksService {
        local_chunks,
        authenticator,
        authorizer,
        replica_authorizer,
        replica_data_plane,
        replica_permits: Arc::new(tokio::sync::Semaphore::new(REPLICA_RECEIVE_SLOTS)),
        replica_forward_permits: Arc::new(tokio::sync::Semaphore::new(REPLICA_RECEIVE_SLOTS - 1)),
        replica_authorization_permits: Arc::new(tokio::sync::Semaphore::new(REPLICA_RECEIVE_SLOTS)),
        read_permits: Arc::new(tokio::sync::Semaphore::new(8)),
        replica_timeout,
        rdma_sessions: resources.rdma_sessions,
        payload_metrics: resources.payload_metrics,
        suspect_reviews: Default::default(),
        holders: resources.holders,
    })
}

#[cfg(feature = "dfs")]
impl DfsChunksService {
    /// Authorize the hop before deciding whether it can use the slot reserved
    /// for a terminal hop. Bound blocking authorization independently of the
    /// receive path, including detached work after the caller times out.
    async fn authorize_replica(
        &self,
        peer: String,
        header: afs_protocol::node_data::DfsPutReplicaHeader,
        deadline: tokio::time::Instant,
    ) -> Result<AuthorizedReplicaWrite, Status> {
        tokio::time::timeout_at(deadline, async {
            let slot = self
                .replica_authorization_permits
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Status::unavailable("DFS replica authorization is closed"))?;
            let authorizer = self.replica_authorizer.clone();
            crate::node::bridge::spawn_blocking(move || {
                let _slot = slot;
                authorizer.authorize(&peer, &header)
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(error_to_status)
        })
        .await
        .map_err(|_| Status::deadline_exceeded("DFS replica authorization deadline exceeded"))?
    }

    /// A forwarding hop can keep a receive slot while waiting for its peer.
    /// Leave one slot available to terminal hops and queue before taking a
    /// receive slot. All admission uses the original transfer deadline.
    async fn admit_replica(
        &self,
        deadline: tokio::time::Instant,
        grant: &AuthorizedReplicaWrite,
    ) -> Result<ReplicaAdmission, Status> {
        let admission = async {
            let (forward, forward_waited) = if grant.op.chain_tail.is_empty() {
                (None, false)
            } else {
                let (permit, waited) =
                    Self::take_replica_slot(&self.replica_forward_permits).await?;
                (Some(permit), waited)
            };
            let (receive, receive_waited) = Self::take_replica_slot(&self.replica_permits).await?;
            Ok((
                ReplicaAdmission {
                    _receive: receive,
                    _forward: forward,
                },
                forward_waited || receive_waited,
            ))
        };
        let count = |outcome| {
            if let Some(metrics) = &self.payload_metrics {
                metrics.admission("replica", outcome);
            }
        };
        match tokio::time::timeout_at(deadline, admission).await {
            Ok(Ok((admission, waited))) => {
                count(if waited { "waited" } else { "immediate" });
                Ok(admission)
            }
            Ok(Err(error)) => Err(error),
            Err(_) => {
                count("rejected");
                Err(Status::resource_exhausted(
                    "DFS replica admission deadline exceeded",
                ))
            }
        }
    }

    async fn take_replica_slot(
        slots: &Arc<tokio::sync::Semaphore>,
    ) -> Result<(tokio::sync::OwnedSemaphorePermit, bool), Status> {
        if let Ok(permit) = slots.clone().try_acquire_owned() {
            return Ok((permit, false));
        }
        slots
            .clone()
            .acquire_owned()
            .await
            .map(|permit| (permit, true))
            .map_err(|_| Status::unavailable("DFS replica admission is closed"))
    }
}

#[cfg(feature = "dfs")]
#[tonic::async_trait]
impl DfsChunks for DfsChunksService {
    type ReadRangesStream =
        Pin<Box<dyn Stream<Item = Result<DfsReadRangesFrame, Status>> + Send + 'static>>;

    async fn put_replica_stream(
        &self,
        request: Request<tonic::Streaming<DfsPutReplicaFrame>>,
    ) -> Result<Response<DfsPutReplicaReply>, Status> {
        use afs_protocol::node_data::dfs_put_replica_frame::Body;
        let peer = authenticate_peer(self.authenticator.as_ref(), &request)?;
        // One absolute deadline covers queueing and the transfer.
        let deadline = tokio::time::Instant::now() + self.replica_timeout;
        let local = self
            .local_chunks
            .clone()
            .ok_or_else(|| Status::failed_precondition("local chunk store is not configured"))?;
        let plane = self.replica_data_plane.clone();
        let metrics = self.payload_metrics.clone();
        let mut stream = request.into_inner();
        let header = tokio::time::timeout_at(deadline, async {
            let first = stream
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("replica stream requires a header"))?;
            match first.body {
                Some(Body::Header(header)) => Ok(header),
                _ => Err(Status::invalid_argument(
                    "first replica frame must be the header",
                )),
            }
        })
        .await
        .map_err(|_| Status::deadline_exceeded("DFS replica header deadline exceeded"))??;
        let digest = replica_header_digest(&header).map_err(error_to_status)?;
        let grant = self
            .authorize_replica(peer, header.clone(), deadline)
            .await?;
        let admission = self.admit_replica(deadline, &grant).await?;
        ensure_replica_grant_live(grant.expires_at_unix_ms).map_err(error_to_status)?;
        tokio::time::timeout_at(deadline, async move {
            let mut bytes = Vec::with_capacity(header.chunk_length as usize);
            while let Some(frame) = stream.message().await? {
                match frame.body {
                    Some(Body::Data(data))
                        if !data.is_empty()
                            && data.len() <= DFS_REPLICA_FRAME_BYTES
                            && bytes.len().saturating_add(data.len())
                                <= header.chunk_length as usize =>
                    {
                        if let Some(metrics) = &metrics {
                            metrics.record("grpc", "recv", "replica", data.len() as u64);
                        }
                        bytes.extend_from_slice(&data)
                    }
                    _ => {
                        return Err(Status::invalid_argument(
                            "replica stream has an invalid or oversized frame",
                        ));
                    }
                }
            }
            if bytes.len() != header.chunk_length as usize {
                return Err(Status::invalid_argument(
                    "replica stream length differs from header",
                ));
            }
            let expected = crate::dfs::ChunkObject {
                id: crate::dfs::ChunkId::new(header.chunk_id.clone()),
                length: header.chunk_length,
                content_digest: digest.clone(),
                encoding: crate::dfs::ChunkEncoding::Raw,
            };
            let staged = crate::node::chunk::StagedChunk::from_replica(
                crate::dfs::OperationId::new(header.operation_id),
                &expected,
                bytes,
            )
            .map_err(error_to_status)?;
            if staged.chunk.id.0 != header.chunk_id || staged.chunk.content_digest != digest {
                return Err(Status::invalid_argument(
                    "replica content differs from immutable identity",
                ));
            }
            let acks = crate::node::bridge::spawn_blocking(move || {
                let _admission = admission;
                persist_dfs_replica(&local, plane, grant, staged)
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(error_to_status)?;
            Ok(Response::new(DfsPutReplicaReply {
                durable_acks: acks.into_iter().map(wire_replica_ack).collect(),
            }))
        })
        .await
        .map_err(|_| Status::deadline_exceeded("DFS replica transfer total deadline exceeded"))?
    }

    async fn put_replica_rdma(
        &self,
        request: Request<DfsPutReplicaRdmaRequest>,
    ) -> Result<Response<DfsPutReplicaReply>, Status> {
        let peer = authenticate_peer(self.authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        #[cfg(not(feature = "rdma"))]
        {
            let _ = (peer, request);
            Err(Status::unimplemented("DFS RDMA is not compiled"))
        }
        #[cfg(feature = "rdma")]
        {
            let header = request
                .header
                .ok_or_else(|| Status::invalid_argument("RDMA replica header is absent"))?;
            let digest = replica_header_digest(&header).map_err(error_to_status)?;
            if request.region_offset != 0
                || request.chunk_offset != 0
                || request.staging_id != header.operation_id
                || request.length != header.chunk_length
            {
                return Err(Status::invalid_argument(
                    "RDMA replica requires one exact whole-chunk MR window",
                ));
            }
            let deadline = tokio::time::Instant::now() + self.replica_timeout;
            let local = self
                .local_chunks
                .clone()
                .ok_or_else(|| Status::failed_precondition("local ChunkStore is absent"))?;
            let grant = self
                .authorize_replica(peer, header.clone(), deadline)
                .await?;
            let admission = self.admit_replica(deadline, &grant).await?;
            let identity = self.rdma_sessions.dfs_session_identity(
                if grant.op.target_index == 0 {
                    grant.op.initiator_node_id.clone()
                } else {
                    grant.op.ordered_targets[grant.op.target_index - 1]
                        .node_id
                        .clone()
                },
                replica_sender_epoch(&grant.op).map_err(error_to_status)?,
                &request.server_generation,
            )?;
            let session = self
                .rdma_sessions
                .session_for(request.rdma_session_id, &identity)
                .await?;
            let poisoned_guard = DfsServerRdmaGuard::new(session.clone());
            let plane = self.replica_data_plane.clone();
            let metrics = self.payload_metrics.clone();
            let acks = tokio::time::timeout_at(
                deadline,
                crate::node::bridge::spawn_blocking(move || {
                    let _admission = admission;
                    ensure_replica_grant_live(grant.expires_at_unix_ms)?;
                    let bytes = {
                        let mut endpoint = session.endpoint.blocking_lock();
                        if session.poisoned.load(std::sync::atomic::Ordering::SeqCst) {
                            return Err(replica_denied("RDMA session is poisoned"));
                        }
                        endpoint
                            .transfer_read(request.length as usize)
                            .map_err(|error| {
                                session
                                    .poisoned
                                    .store(true, std::sync::atomic::Ordering::SeqCst);
                                afs_error::Error::coded(
                                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                                    error.to_string(),
                                )
                            })?;
                        if let Some(metrics) = &metrics {
                            metrics.record("rdma", "recv", "replica", request.length);
                        }
                        endpoint
                            .get_local(request.length as usize)
                            .map_err(|error| {
                                afs_error::Error::coded(
                                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                                    error.to_string(),
                                )
                            })?
                    };
                    let expected = crate::dfs::ChunkObject {
                        id: crate::dfs::ChunkId::new(header.chunk_id.clone()),
                        length: header.chunk_length,
                        content_digest: digest.clone(),
                        encoding: crate::dfs::ChunkEncoding::Raw,
                    };
                    let staged = crate::node::chunk::StagedChunk::from_replica(
                        crate::dfs::OperationId::new(header.operation_id),
                        &expected,
                        bytes,
                    )?;
                    if staged.chunk.id.0 != header.chunk_id || staged.chunk.content_digest != digest
                    {
                        return Err(afs_error::Error::coded(
                            afs_error::NODE_TRANSFER_CORRUPT_DATA,
                            "RDMA replica digest/immutable identity differs",
                        ));
                    }
                    persist_dfs_replica(&local, plane, grant, staged)
                }),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("DFS RDMA replica deadline exceeded"))?
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(error_to_status)?;
            poisoned_guard.disarm();
            Ok(Response::new(DfsPutReplicaReply {
                durable_acks: acks.into_iter().map(wire_replica_ack).collect(),
            }))
        }
    }

    async fn confirm_replica(
        &self,
        _request: Request<DfsConfirmReplicaRequest>,
    ) -> Result<Response<DfsReplicaAck>, Status> {
        Err(Status::unimplemented(
            "DFS replica confirmation is not implemented; no durable acknowledgement exists",
        ))
    }

    /// R59: trust only this disk. A bad copy is quarantined by `open_verified`
    /// and the repair worker self-reports it; a healthy one changes nothing.
    async fn review_suspect_chunk(
        &self,
        request: Request<afs_protocol::node_data::DfsReviewSuspectChunkRequest>,
    ) -> Result<Response<afs_protocol::node_data::DfsReviewSuspectChunkReply>, Status> {
        authenticate_peer(self.authenticator.as_ref(), &request)?;
        let chunk_id = crate::dfs::ChunkId::new(request.into_inner().chunk_id);
        let local = self
            .local_chunks
            .clone()
            .ok_or_else(|| Status::unavailable("DFS chunk store is not available"))?;
        let Some(permit) = self.suspect_reviews.admit(&chunk_id) else {
            return Ok(Response::new(
                afs_protocol::node_data::DfsReviewSuspectChunkReply {
                    reviewed: false,
                    copy_invalid: false,
                },
            ));
        };
        let copy_invalid = crate::node::bridge::spawn_blocking(move || {
            let _permit = permit;
            match local.open_verified(&chunk_id) {
                Ok(_) => Ok(false),
                Err(error) if crate::node::fault::classify::verified_copy_is_invalid(&error) => {
                    Ok(true)
                }
                Err(error) => Err(error),
            }
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .map_err(afs_transport::grpc::error_status::error_to_status)?;
        Ok(Response::new(
            afs_protocol::node_data::DfsReviewSuspectChunkReply {
                reviewed: true,
                copy_invalid,
            },
        ))
    }

    async fn negotiate_rdma(
        &self,
        request: Request<DfsNegotiateRdmaRequest>,
    ) -> Result<Response<DfsNegotiateRdmaReply>, Status> {
        let peer = authenticate_peer(self.authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        let permit = self
            .read_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("DFS RDMA authority slots are full"))?;
        let write_authorizer = self.replica_authorizer.clone();
        let read_authorizer = self.authorizer.clone();
        let authority = request
            .authority
            .ok_or_else(|| Status::permission_denied("DFS RDMA negotiation authority is absent"))?;
        let (peer, epoch, _permit) = crate::node::bridge::spawn_blocking(move || {
            use afs_protocol::node_data::dfs_negotiate_rdma_request::Authority;
            let epoch = match authority {
                Authority::ReplicaHeader(header) => {
                    let grant = write_authorizer.authorize(&peer, &header)?;
                    replica_sender_epoch(&grant.op)?
                }
                Authority::ReadRequest(read) => {
                    read_authorizer.authorize(&peer, &read)?;
                    dfs_read_caller_epoch(&peer, &read)
                        .map_err(afs_transport::grpc::error_status::status_to_error)?
                }
            };
            Ok::<_, afs_error::Error>((peer, epoch, permit))
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .map_err(error_to_status)?;
        let server_generation = if request.bind_server_generation {
            self.rdma_sessions.dfs_generation().to_vec()
        } else {
            Vec::new()
        };
        let identity = self
            .rdma_sessions
            .dfs_session_identity(peer, epoch, &server_generation)?;
        let negotiated = super::super::control::negotiate_for_peer(
            &self.rdma_sessions,
            identity,
            afs_protocol::node_control::NegotiateDataRequest {
                client_info: request.client_info,
                capacity: request.capacity,
                handshake_version: request.handshake_version,
            },
        )
        .await?
        .into_inner();
        if negotiated.rdma_supported && server_generation.is_empty() {
            afs_logging::warn!(
                "dfs.rdma_legacy_session_accepted";
                "session_id" => negotiated.session_id
            );
        }
        Ok(Response::new(DfsNegotiateRdmaReply {
            session_id: negotiated.session_id,
            server_info: negotiated.server_info,
            capacity: negotiated.capacity,
            rdma_supported: negotiated.rdma_supported,
            handshake_version: negotiated.handshake_version,
            server_generation: if negotiated.rdma_supported {
                server_generation
            } else {
                Vec::new()
            },
        }))
    }

    async fn close_rdma(
        &self,
        request: Request<DfsCloseRdmaRequest>,
    ) -> Result<Response<DfsCloseRdmaReply>, Status> {
        let peer = authenticate_peer(self.authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        let identity = self.rdma_sessions.dfs_session_identity(
            peer,
            request.peer_node_epoch,
            &request.server_generation,
        )?;
        self.rdma_sessions
            .close_for(request.session_id, &identity)
            .await?;
        Ok(Response::new(DfsCloseRdmaReply {}))
    }

    async fn read_ranges_rdma(
        &self,
        request: Request<DfsReadRangesRdmaRequest>,
    ) -> Result<Response<DfsReadRangesRdmaReply>, Status> {
        let peer = authenticate_peer(self.authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        #[cfg(not(feature = "rdma"))]
        {
            let _ = (peer, request);
            Err(Status::unimplemented("DFS RDMA is not compiled"))
        }
        #[cfg(feature = "rdma")]
        {
            let read = request
                .request
                .ok_or_else(|| Status::invalid_argument("DFS RDMA read request is absent"))?;
            validate_dfs_read_request(&read)?;
            let length = validate_packed_dfs_read(&read)?;
            let epoch = dfs_read_caller_epoch(&peer, &read)?;
            let permit = self
                .read_permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| Status::resource_exhausted("DFS peer read slots are full"))?;
            let authorizer = self.authorizer.clone();
            let (read, permit) = crate::node::bridge::spawn_blocking(move || {
                authorizer.authorize(&peer, &read).map(|()| (read, permit))
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(error_to_status)?;
            let caller = read.operations[0]
                .grant
                .as_ref()
                .ok_or_else(|| Status::permission_denied("DFS RDMA grant is absent"))?
                .caller_node_id
                .clone();
            let identity = self.rdma_sessions.dfs_session_identity(
                caller,
                epoch,
                &request.server_generation,
            )?;
            let session = self
                .rdma_sessions
                .session_for(request.rdma_session_id, &identity)
                .await?;
            let poisoned_guard = DfsServerRdmaGuard::new(session.clone());
            let local = self
                .local_chunks
                .clone()
                .ok_or_else(|| Status::failed_precondition("local ChunkStore is absent"))?;
            let metrics = self.payload_metrics.clone();
            let reply = tokio::time::timeout(
                self.replica_timeout,
                crate::node::bridge::spawn_blocking(move || {
                    let _permit = permit;
                    let packed = read_packed_dfs_ranges(&local, &read, length)?;
                    validate_dfs_read_request(&read)?;
                    let mut endpoint = session.endpoint.blocking_lock();
                    if session.poisoned.load(std::sync::atomic::Ordering::SeqCst) {
                        return Err(Status::permission_denied("RDMA session is poisoned"));
                    }
                    endpoint.put_local(&packed.bytes).map_err(rdma_status)?;
                    endpoint
                        .transfer_write(packed.bytes.len())
                        .map_err(|error| {
                            session
                                .poisoned
                                .store(true, std::sync::atomic::Ordering::SeqCst);
                            rdma_status(error)
                        })?;
                    if let Some(metrics) = metrics {
                        metrics.record("rdma", "send", "read", length as u64);
                    }
                    Ok::<_, Status>(DfsReadRangesRdmaReply {
                        read_id: read.read_id,
                        attempt_id: read.attempt_id,
                        completions: packed.completions,
                        transferred_bytes: length as u64,
                    })
                }),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("DFS RDMA read deadline exceeded"))?
            .map_err(|error| Status::internal(error.to_string()))??;
            poisoned_guard.disarm();
            Ok(Response::new(reply))
        }
    }

    async fn read_ranges(
        &self,
        request: Request<DfsReadRangesRequest>,
    ) -> Result<Response<Self::ReadRangesStream>, Status> {
        let admission_deadline = self
            .local_chunks
            .as_ref()
            .map_or_else(std::time::Instant::now, |local| local.peer_serve_deadline());
        let authenticated_peer = authenticate_peer(self.authenticator.as_ref(), &request)?;
        let crc_mode = requested_crc_read_mode(request.metadata())?;
        let request = request.into_inner();
        // H2 step 2: any holder grant selects the holder path, which refuses
        // a request that mixes it with per-copy grants (R4).
        if request
            .operations
            .iter()
            .any(|op| op.holder_grant.is_some())
        {
            return self
                .read_holder_ranges(authenticated_peer, crc_mode, request)
                .await;
        }
        if let Some(local) = &self.local_chunks {
            local.record_read_check(crate::node::metrics::work::ReadCheck::RequestValidate);
        }
        validate_dfs_read_request(&request)?;
        for operation in &request.operations {
            if operation
                .grant
                .as_ref()
                .is_none_or(|grant| grant.caller_node_id != authenticated_peer)
            {
                return Err(Status::permission_denied(
                    "DFS read grant caller does not match the authenticated peer",
                ));
            }
        }
        if let Some(local) = &self.local_chunks {
            local.ensure_peer_serve_open().map_err(error_to_status)?;
        }
        let permit = match self.read_permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                if admission_deadline <= std::time::Instant::now() {
                    return Err(Status::resource_exhausted("DFS peer read slots are full"));
                }
                // Keep one future and one absolute deadline. Queueing cannot
                // renew grants or start another 20ms payload/flight budget.
                let _wait = self
                    .local_chunks
                    .as_ref()
                    .and_then(|local| local.coarse_diagnostic("peer_slot_wait", 0));
                let admitted = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(admission_deadline),
                    self.read_permits.clone().acquire_owned(),
                )
                .await;
                if let Some(local) = &self.local_chunks {
                    local.ensure_peer_serve_open().map_err(error_to_status)?;
                }
                admitted
                    .map_err(|_| {
                        Status::resource_exhausted("DFS peer read admission deadline exceeded")
                    })?
                    .map_err(|_| Status::unavailable("DFS peer read admission is closed"))?
            }
        };
        if let Some(local) = &self.local_chunks {
            local.record_read_check(crate::node::metrics::work::ReadCheck::RequestValidate);
        }
        validate_dfs_read_request(&request)?;
        let authorizer = self.authorizer.clone();
        let local = self.local_chunks.clone();
        // Keep the response boundary after authorization while doing both
        // blocking stages in one task. Cancellation cannot stop an active
        // verifier, so its permit remains held until that task actually exits.
        let (authorized, ready) = tokio::sync::oneshot::channel();
        // Four bounded frame references; every reference retains its original
        // shared payload lease until consumed or dropped.
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let metrics = self.payload_metrics.clone();
        if let Some(local) = &local {
            local.record_peer_read_event("blocking_combined");
        }
        crate::node::bridge::spawn_blocking(move || {
            let _permit = permit;
            if authorized.is_closed() || sender.is_closed() {
                return;
            }
            if let Err(error) = authorizer.authorize(&authenticated_peer, &request) {
                let _ = authorized.send(Err(error_to_status(error)));
                return;
            }
            if authorized.is_closed() || sender.is_closed() {
                return;
            }
            let Some(local) = local else {
                let _ = authorized.send(Err(coded_status(
                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                    "DFS local ChunkStore is not available",
                )));
                return;
            };
            if authorized.send(Ok(())).is_err() || sender.is_closed() {
                return;
            }
            if let Err(error) = stream_dfs_ranges_until(
                &local,
                &request,
                &sender,
                metrics.as_ref(),
                admission_deadline,
                crc_mode,
            ) {
                let _ = sender.blocking_send(Err(error));
            }
        });
        ready
            .await
            .map_err(|_| Status::internal("DFS read authorization producer stopped"))??;
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
    }
}

#[cfg(feature = "dfs")]
#[cfg(feature = "dfs")]
impl DfsChunksService {
    /// H2 step 2: a read served as a version holder, in the order of
    /// `dfs_holder_serve` (plan/15 v3 §1.3). Steps 1–3 fail with one denial,
    /// whatever this Node holds.
    async fn read_holder_ranges(
        &self,
        peer: String,
        crc_mode: crate::config::DfsReadCrcMode,
        request: DfsReadRangesRequest,
    ) -> Result<Response<<Self as DfsChunks>::ReadRangesStream>, Status> {
        use super::dfs_holder_serve::{
            REVALIDATE_MARGIN, holder_denied, holder_not_ready, wait_budget,
        };
        let (Some(holders), Some(local)) = (self.holders.clone(), self.local_chunks.clone()) else {
            return Err(holder_denied());
        };
        let event = |name: &'static str| local.record_read_cache_event(name);
        let timing = local
            .read_work_metrics()
            .map(|metrics| metrics.share.clone());
        // Each stage is timed until it ends, also when it fails or the
        // request is cancelled (!318 P3-2).
        let timer =
            |start: &dyn Fn(
                &crate::node::metrics::work::ShareTimingMetrics,
            ) -> crate::node::metrics::work::ShareTimer| { timing.as_ref().map(start) };
        let denied = |status: Status| {
            if status.code() == tonic::Code::PermissionDenied {
                event("holder_denied");
            }
            status
        };
        local.ensure_peer_serve_open().map_err(error_to_status)?;
        // 1. one kind of grant, this Node, this peer, the request's version
        let grant = holders.check_request(&peer, &request).map_err(denied)?;
        // 2. Meta, outside the read slots
        let authorizing = timer(&|t| t.holder_request("authorize"));
        let mut authorized = holders
            .authorize(&peer, &grant, false)
            .await
            .map_err(denied)?;
        drop(authorizing);
        // 3. ranges inside the version's layout
        if let Err(status) = holders.check_ranges(&grant, &request.operations) {
            if status.code() == tonic::Code::PermissionDenied {
                return Err(denied(status));
            }
            event("holder_not_ready");
            return Err(status);
        }
        // 4. Probe readiness only after both checks and authorized ranges.
        // A partial lookup here does not retain the body across slot admission.
        let available = |op: &&afs_protocol::node_data::DfsChunkReadOp| {
            let chunk = crate::dfs::ChunkId::new(op.chunk_id.clone());
            local.has_servable_record(&chunk)
                || holders
                    .in_flight
                    .partials
                    .covering(&chunk, grant.holder_node_epoch, op.chunk_offset, op.length)
                    .is_some()
        };
        let mut missing: Vec<_> = request
            .operations
            .iter()
            .filter(|op| !available(op))
            .collect();
        if !missing.is_empty() {
            let budget = wait_budget(&request);
            let all_in_flight = |missing: &[&afs_protocol::node_data::DfsChunkReadOp]| {
                missing.iter().all(|op| {
                    holders.in_flight.overlaps(
                        &crate::dfs::ChunkId::new(op.chunk_id.clone()),
                        grant.holder_node_epoch,
                        op.chunk_offset,
                        op.length,
                    ) || available(op)
                })
            };
            if budget.is_zero() || !all_in_flight(&missing) {
                event("holder_not_ready");
                return Err(holder_not_ready());
            }
            let Some(_slot) = holders.wait_slot(&peer) else {
                event("holder_wait_rejected");
                return Err(holder_not_ready());
            };
            event("holder_waited");
            if let Some(timing) = &timing {
                timing.holder_waiters(holders.waiting());
            }
            let mut waiting = timer(&|t| t.holder_wait());
            let mut ended = |outcome: &'static str| {
                if let Some(waiting) = waiting.take() {
                    waiting.end(outcome);
                }
            };
            let deadline = tokio::time::Instant::now() + budget;
            loop {
                // Wakes on any in-flight change, and when a version stops
                // being shared (!318 P3-3): both armed before the checks.
                let changed = holders.in_flight.changed();
                let unshared = holders.registry.unshared();
                missing.retain(|op| !available(op));
                if missing.is_empty() {
                    break;
                }
                // The version may stop being shared while this waits (S3).
                if !holders.registry.is_shared(&grant.file_version_id) || !all_in_flight(&missing) {
                    event("holder_not_ready");
                    ended("not_ready");
                    return Err(holder_not_ready());
                }
                #[cfg(test)]
                holders.reach(super::dfs_holder_serve::TestPoint::Waiting);
                let woke = tokio::time::timeout_at(deadline, async {
                    tokio::select! {
                        () = changed => {}
                        () = unshared => {}
                    }
                })
                .await;
                if woke.is_err() {
                    missing.retain(|op| !available(op));
                    if missing.is_empty() {
                        break;
                    }
                    event("holder_wait_timeout");
                    ended("timeout");
                    return Err(holder_not_ready());
                }
            }
            ended("ready");
            // Ranges again after the wait: the version may have stopped being
            // shared (code review S3).
            if let Err(status) = holders.check_ranges(&grant, &request.operations) {
                if status.code() == tonic::Code::PermissionDenied {
                    return Err(denied(status));
                }
                event("holder_not_ready");
                return Err(status);
            }
        }
        // 5. re-validate before a slot when the authorization ran short (R5)
        if authorized.until <= std::time::Instant::now() + REVALIDATE_MARGIN {
            let authorizing = timer(&|t| t.holder_request("authorize"));
            authorized = holders
                .authorize(&peer, &grant, true)
                .await
                .map_err(denied)?;
            drop(authorizing);
        }
        // 6. a fresh admission deadline after any wait (R2), then a slot
        #[cfg(test)]
        holders.reach(super::dfs_holder_serve::TestPoint::SlotQueue);
        let slot_wait = timer(&|t| t.holder_request("slot"));
        let admission_deadline = local.peer_serve_deadline();
        let permit = match self.read_permits.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => tokio::time::timeout_at(
                tokio::time::Instant::from_std(admission_deadline),
                self.read_permits.clone().acquire_owned(),
            )
            .await
            .map_err(|_| Status::resource_exhausted("DFS peer read admission deadline exceeded"))?
            .map_err(|_| Status::unavailable("DFS peer read admission is closed"))?,
        };
        drop(slot_wait);
        local.ensure_peer_serve_open().map_err(error_to_status)?;
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let metrics = self.payload_metrics.clone();
        #[cfg(test)]
        let poll_hooks = holders.clone();
        let until = authorized.until;
        let registry = holders.registry.clone();
        let version = grant.file_version_id.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        // 7. Recheck expiry and sharing after slot admission, when the
        // producer actually starts. This step does not call Meta.
        let start = move |local: &LocalChunkStore| {
            if std::time::Instant::now() >= until {
                let _ = started.send(Err(holder_denied()));
                return false;
            }
            if !registry.is_shared(&version) {
                local.record_read_cache_event("holder_not_ready");
                let _ = started.send(Err(holder_not_ready()));
                return false;
            }
            if started.send(Ok(())).is_err() {
                return false;
            }
            local.record_read_cache_event("holder_served");
            true
        };
        let stream = Arc::new(super::dfs_holder_stream::HolderStream {
            local: local.clone(),
            request: Arc::new(request),
            sender,
            metrics,
            read_mode: crc_mode,
            admission_deadline,
            authorized_until: until,
        });
        tokio::spawn(async move {
            let _permit = permit;
            if !start(&local) || stream.sender.is_closed() {
                return;
            }
            #[cfg(test)]
            holders.reach(super::dfs_holder_serve::TestPoint::Serve);
            let _serving = timing.as_ref().map(|t| t.holder_request("serve"));
            if let Err(error) = stream
                .serve(&holders.in_flight, grant.holder_node_epoch)
                .await
            {
                let _ = tokio::time::timeout_at(until.into(), stream.sender.send(Err(error))).await;
            }
        });
        ready
            .await
            .map_err(|_| Status::internal("DFS holder read producer stopped"))??;
        let stream = tokio_stream::wrappers::ReceiverStream::new(receiver);
        #[cfg(test)]
        let stream = tokio_stream::StreamExt::map(stream, move |frame| {
            poll_hooks.reach(super::dfs_holder_serve::TestPoint::ResponsePoll);
            frame
        });
        Ok(Response::new(Box::pin(stream)))
    }
}

/// The per-frame re-check while serving: a holder-form request (every
/// operation carries a holder grant, none a per-copy grant) must still be in
/// shape and within its grant's deadline; any other request takes
/// [`validate_dfs_read_request`].
#[cfg(feature = "dfs")]
pub(super) fn validate_served_read(request: &DfsReadRangesRequest) -> Result<(), Status> {
    let holder_form = request
        .operations
        .iter()
        .all(|op| op.grant.is_none() && op.holder_grant.is_some());
    if !holder_form {
        return validate_dfs_read_request(request);
    }
    let now = crate::clock::now_unix_ms();
    if request.operations.is_empty()
        || request.operations.iter().any(|op| {
            op.holder_grant.as_ref().is_none_or(|grant| {
                grant.expires_at_unix_ms <= now
                    || grant.file_version_id != request.file_version_id
                    || grant.layout_root_id != request.layout_root_id
            })
        })
    {
        return Err(super::dfs_holder_serve::holder_denied());
    }
    Ok(())
}

pub(super) fn validate_dfs_read_request(request: &DfsReadRangesRequest) -> Result<(), Status> {
    if request.read_id.is_empty()
        || request.attempt_id.is_empty()
        || request.file_version_id.is_empty()
        || request.layout_root_id.is_empty()
        || request.operations.is_empty()
        || request.operations.len() > MAX_DFS_READ_OPS
    {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "DFS read batch identity or operation count is invalid",
        ));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let mut total = 0_u64;
    for (index, op) in request.operations.iter().enumerate() {
        let grant = op
            .grant
            .as_ref()
            .ok_or_else(|| Status::permission_denied("DFS read operation has no grant"))?;
        // A holder grant is served only by the gRPC holder path; per-copy
        // entries (gRPC and RDMA) refuse any operation carrying one (R4, T11).
        if op.holder_grant.is_some() {
            return Err(Status::permission_denied(
                "DFS read operation carries a holder grant",
            ));
        }
        if grant.namespace_id.is_empty()
            || grant.file_version_id != request.file_version_id
            || grant.layout_root_id != request.layout_root_id
            || grant.caller_node_id.is_empty()
            || grant.caller_node_epoch == 0
            || u128::from(grant.expires_at_unix_ms) <= now
            || grant.token.is_empty()
        {
            return Err(Status::permission_denied(
                "DFS read operation grant identity or expiry is invalid",
            ));
        }
        if op.chunk_id.is_empty()
            || op.source_copy_id.is_empty()
            || op.operation_index as usize != index
            || op.length == 0
            || op.chunk_offset.checked_add(op.length).is_none()
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "DFS read operation is invalid",
            ));
        }
        total = total
            .checked_add(op.length)
            .filter(|total| *total <= MAX_DFS_READ_BYTES)
            .ok_or_else(|| {
                coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "DFS read batch exceeds byte budget",
                )
            })?;
    }
    Ok(())
}

#[cfg(feature = "dfs")]
fn replica_sender_epoch(op: &crate::node::replication::ReplicaPeerOp) -> afs_error::Result<u64> {
    op.validate_shape()?;
    Ok(if op.target_index == 0 {
        op.initiator_node_epoch
    } else {
        op.ordered_targets[op.target_index - 1].node_epoch
    })
}

#[cfg(feature = "dfs")]
pub(super) fn dfs_read_caller_epoch(
    peer: &str,
    request: &DfsReadRangesRequest,
) -> Result<u64, Status> {
    validate_dfs_read_request(request)?;
    let epoch = request.operations[0]
        .grant
        .as_ref()
        .ok_or_else(|| Status::permission_denied("DFS grant is absent"))?
        .caller_node_epoch;
    if request.operations.iter().any(|op| {
        op.grant
            .as_ref()
            .is_none_or(|grant| grant.caller_node_id != peer || grant.caller_node_epoch != epoch)
    }) {
        return Err(Status::permission_denied(
            "RDMA batch has inconsistent authenticated peer epochs",
        ));
    }
    Ok(epoch)
}

#[cfg(all(feature = "dfs", any(feature = "rdma", test)))]
pub(super) fn validate_packed_dfs_read(request: &DfsReadRangesRequest) -> Result<usize, Status> {
    let mut length = 0_u64;
    for op in &request.operations {
        if op.destination_offset != length {
            return Err(Status::invalid_argument(
                "RDMA read windows must be tightly packed",
            ));
        }
        length = length
            .checked_add(op.length)
            .filter(|length| *length <= crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64)
            .ok_or_else(|| Status::invalid_argument("RDMA read exceeds registered capacity"))?;
    }
    Ok(length as usize)
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
struct DfsServerRdmaGuard {
    session: Arc<super::super::control::RdmaSession>,
    disarmed: bool,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
impl DfsServerRdmaGuard {
    fn new(session: Arc<super::super::control::RdmaSession>) -> Self {
        Self {
            session,
            disarmed: false,
        }
    }
    fn disarm(mut self) {
        self.disarmed = true;
    }
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
impl Drop for DfsServerRdmaGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            self.session
                .poisoned
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

#[cfg(feature = "dfs")]
fn persist_dfs_replica(
    local: &LocalChunkStore,
    plane: Option<Arc<dyn crate::node::replication::ReplicaDataPlane>>,
    grant: AuthorizedReplicaWrite,
    staged: crate::node::chunk::StagedChunk,
) -> afs_error::Result<Vec<crate::dfs::ReplicaAck>> {
    ensure_replica_grant_live(grant.expires_at_unix_ms)?;
    let op = grant.op;
    let ack = local.persist(
        &staged,
        &op.target,
        op.placement_revision,
        op.placement_epoch,
    )?;
    let mut acks = vec![ack];
    if let Some(next) = op.next_hop()? {
        let plane = plane.ok_or_else(|| replica_denied("replica forwarding is not configured"))?;
        let tail = plane.put_peer_replica(&next, &staged)?;
        crate::node::replication::validate_peer_acks(&next, &staged, &tail)?;
        acks.extend(tail);
    }
    crate::node::replication::validate_peer_acks(&op, &staged, &acks)?;
    Ok(acks)
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
#[derive(Debug)]
pub(super) struct PackedDfsRead {
    // Struct fields drop in declaration order: release the actual payload before
    // returning its shared reservation, including early errors during transfer.
    pub(super) bytes: Vec<u8>,
    pub(super) completions: Vec<DfsReadRangesCompletion>,
    _reservation: crate::node::chunk::PeerReadReservation,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) fn read_packed_dfs_ranges(
    local: &LocalChunkStore,
    request: &DfsReadRangesRequest,
    length: usize,
) -> Result<PackedDfsRead, Status> {
    // Packed RDMA bytes share the same pool as retained verified objects.
    // The owner keeps them charged through DMA completion and payload release.
    let mut reservation = local
        .reserve_peer_read_payload(length)
        .map_err(error_to_status)?;
    let mut bytes = vec![0; length];
    reservation
        .retain_capacity(bytes.capacity())
        .map_err(error_to_status)?;
    let mut completions = Vec::with_capacity(request.operations.len());
    for op in &request.operations {
        let start = op.destination_offset as usize;
        let end = start + op.length as usize;
        let range = &mut bytes[start..end];
        if local
            .read_range_at_uncommitted(
                &crate::dfs::ChunkId::new(op.chunk_id.clone()),
                op.chunk_offset,
                range,
            )
            .map_err(error_to_status)?
            != range.len()
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_CORRUPT_DATA,
                "immutable Chunk ended before RDMA range",
            ));
        }
        let table = local
            .peer_crc32c_table(&crate::dfs::ChunkId::new(op.chunk_id.clone()))
            .map_err(error_to_status)?;
        let (checksum, algorithm, descriptor) = if let Some(table) = table {
            (
                crate::node::chunk_crc32c::checksum(range)
                    .to_le_bytes()
                    .to_vec(),
                afs_protocol::node_data::DfsDigestAlgorithm::Crc32c as i32,
                serde_json::to_vec(&table).map_err(|error| Status::internal(error.to_string()))?,
            )
        } else {
            (
                blake3::hash(range).as_bytes().to_vec(),
                afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32,
                Vec::new(),
            )
        };
        completions.push(DfsReadRangesCompletion {
            crc32c_table: descriptor,
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            operation_index: op.operation_index,
            source_copy_id: op.source_copy_id.clone(),
            transferred_bytes: op.length,
            range_checksum: checksum,
            range_checksum_algorithm: algorithm,
        });
    }
    Ok(PackedDfsRead {
        bytes,
        completions,
        _reservation: reservation,
    })
}
