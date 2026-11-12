//! The DfsChunks peer service.

use super::*;

// Draft data.rs additions, keeping transport resources inside the existing RPC module.
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
    pub(super) read_permits: Arc<tokio::sync::Semaphore>,
    pub(super) replica_timeout: std::time::Duration,
    pub(super) rdma_sessions: super::super::control::RdmaSessionRegistry,
    pub(super) payload_metrics: Option<DfsPayloadMetrics>,
    pub(super) suspect_reviews: Arc<crate::node::fault::suspect::SuspectReviewGate>,
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
        replica_permits: Arc::new(tokio::sync::Semaphore::new(8)),
        read_permits: Arc::new(tokio::sync::Semaphore::new(8)),
        replica_timeout,
        rdma_sessions: resources.rdma_sessions,
        payload_metrics: resources.payload_metrics,
        suspect_reviews: Default::default(),
    })
}

#[cfg(feature = "dfs")]
impl DfsChunksService {
    /// Replica receive admission: take a slot at once, else queue until
    /// `deadline` (R71b). Two writers sharing a target can exceed its slots;
    /// that is backpressure, not an error. While queued, the sender's stream
    /// waits on HTTP/2 flow control. Repair transfers share the same FIFO
    /// semaphore, so neither side starves.
    async fn admit_replica(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        let count = |outcome| {
            if let Some(metrics) = &self.payload_metrics {
                metrics.admission("replica", outcome);
            }
        };
        if let Ok(permit) = self.replica_permits.clone().try_acquire_owned() {
            count("immediate");
            return Ok(permit);
        }
        match tokio::time::timeout_at(deadline, self.replica_permits.clone().acquire_owned()).await
        {
            Ok(Ok(permit)) => {
                count("waited");
                Ok(permit)
            }
            Ok(Err(_)) => Err(Status::unavailable("DFS replica admission is closed")),
            Err(_) => {
                count("rejected");
                Err(Status::resource_exhausted(
                    "DFS replica admission deadline exceeded",
                ))
            }
        }
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
        let permit = self.admit_replica(deadline).await?;
        let local = self
            .local_chunks
            .clone()
            .ok_or_else(|| Status::failed_precondition("local chunk store is not configured"))?;
        let authorizer = self.replica_authorizer.clone();
        let plane = self.replica_data_plane.clone();
        let metrics = self.payload_metrics.clone();
        let mut stream = request.into_inner();
        tokio::time::timeout_at(deadline, async move {
            let first = stream
                .message()
                .await?
                .ok_or_else(|| Status::invalid_argument("replica stream requires a header"))?;
            let header = match first.body {
                Some(Body::Header(header)) => header,
                _ => {
                    return Err(Status::invalid_argument(
                        "first replica frame must be the header",
                    ));
                }
            };
            let digest = replica_header_digest(&header).map_err(error_to_status)?;
            let auth_header = header.clone();
            // The permit follows blocking work: cancellation cannot release a
            // slot while its unabortable authority/persistence task is running.
            let (grant, permit) = crate::node::bridge::spawn_blocking(move || {
                authorizer
                    .authorize(&peer, &auth_header)
                    .map(|grant| (grant, permit))
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(error_to_status)?;
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
                let _permit = permit;
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
            let permit = self.admit_replica(deadline).await?;
            let local = self
                .local_chunks
                .clone()
                .ok_or_else(|| Status::failed_precondition("local ChunkStore is absent"))?;
            let authorizer = self.replica_authorizer.clone();
            let auth_header = header.clone();
            let (grant, permit) = crate::node::bridge::spawn_blocking(move || {
                authorizer
                    .authorize(&peer, &auth_header)
                    .map(|grant| (grant, permit))
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(error_to_status)?;
            let identity = super::super::control::PeerSessionIdentity::new(
                if grant.op.target_index == 0 {
                    grant.op.initiator_node_id.clone()
                } else {
                    grant.op.ordered_targets[grant.op.target_index - 1]
                        .node_id
                        .clone()
                },
                replica_sender_epoch(&grant.op).map_err(error_to_status)?,
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
                    let _permit = permit;
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
        let identity = super::super::control::PeerSessionIdentity::new(peer, epoch)?;
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
        Ok(Response::new(DfsNegotiateRdmaReply {
            session_id: negotiated.session_id,
            server_info: negotiated.server_info,
            capacity: negotiated.capacity,
            rdma_supported: negotiated.rdma_supported,
            handshake_version: negotiated.handshake_version,
        }))
    }

    async fn close_rdma(
        &self,
        request: Request<DfsCloseRdmaRequest>,
    ) -> Result<Response<DfsCloseRdmaReply>, Status> {
        let peer = authenticate_peer(self.authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        let identity =
            super::super::control::PeerSessionIdentity::new(peer, request.peer_node_epoch)?;
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
            let identity = super::super::control::PeerSessionIdentity::new(caller, epoch)?;
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
