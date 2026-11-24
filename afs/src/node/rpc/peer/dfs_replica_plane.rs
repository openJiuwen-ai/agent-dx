//! DFS replica write planes over gRPC and RDMA.

use super::*;

#[cfg(feature = "dfs")]
pub struct GrpcReplicaDataPlane {
    pub(super) peers: Arc<PeerConnectionPool>,
    pub(super) timeout: Duration,
    pub(super) runtime: PeerRuntime,
}

#[cfg(feature = "dfs")]
impl GrpcReplicaDataPlane {
    pub fn new(peers: Arc<PeerConnectionPool>, timeout: Duration) -> afs_error::Result<Self> {
        Ok(Self {
            peers,
            timeout,
            runtime: PeerRuntime::current_or_new("DFS replica data plane")?,
        })
    }
}

#[cfg(feature = "dfs")]
impl crate::node::replication::ReplicaDataPlane for GrpcReplicaDataPlane {
    fn mode(&self) -> crate::node::replication::ReplicaTransferMode {
        crate::node::replication::ReplicaTransferMode::GrpcStream
    }

    fn prepare_peer(&self, op: &crate::node::replication::ReplicaPeerOp) -> afs_error::Result<()> {
        op.validate_shape()?;
        // This obtains a reusable lazy channel; it does not claim a completed
        // TCP/TLS handshake or receiver authority. PutReplicaStream does both.
        self.runtime
            .block_on(self.peers.channel(
                &op.target.node_id,
                op.target.node_epoch,
                &op.target.data_endpoint,
            ))
            .map(|_| ())
    }

    fn put_peer_replica(
        &self,
        op: &crate::node::replication::ReplicaPeerOp,
        staged: &crate::node::chunk::StagedChunk,
    ) -> afs_error::Result<Vec<crate::dfs::ReplicaAck>> {
        op.validate_shape()?;
        if staged.chunk.id != op.chunk_id
            || staged.bytes().len() > super::super::data::MAX_DFS_REPLICA_BYTES
        {
            return Err(dfs_protocol_error(
                "replica Chunk identity or length is invalid",
            ));
        }
        let header = replica_header(op, staged)?;
        self.runtime.block_on(async {
            // One total deadline. A receiver whose slots stay full past its own
            // admission deadline answers ResourceExhausted; retry that with a
            // short backoff instead of failing the upload (R71b).
            let deadline = tokio::time::Instant::now() + self.timeout;
            let mut backoff = Duration::from_millis(5);
            loop {
                let attempt = tokio::time::timeout_at(
                    deadline,
                    self.put_peer_replica_once(op, header.clone(), staged.clone(), deadline),
                )
                .await
                .map_err(|_| {
                    afs_error::Error::coded(
                        afs_error::CLIENT_DEADLINE_EXCEEDED,
                        "DFS replica transfer total deadline exceeded",
                    )
                })?;
                match attempt {
                    Err(error)
                        if crate::node::fault::classify::retryable_backpressure(&error)
                            && tokio::time::Instant::now() + backoff < deadline =>
                    {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_millis(200));
                    }
                    result => return result,
                }
            }
        })
    }
}

#[cfg(feature = "dfs")]
impl GrpcReplicaDataPlane {
    async fn put_peer_replica_once(
        &self,
        op: &crate::node::replication::ReplicaPeerOp,
        header: afs_protocol::node_data::DfsPutReplicaHeader,
        staged: crate::node::chunk::StagedChunk,
        deadline: tokio::time::Instant,
    ) -> afs_error::Result<Vec<crate::dfs::ReplicaAck>> {
        let channel = self
            .peers
            .channel(
                &op.target.node_id,
                op.target.node_epoch,
                &op.target.data_endpoint,
            )
            .await?;
        let mut client = DfsChunksClient::new(channel)
            .max_encoding_message_size(self.peers.config.max_encoding_message_bytes)
            .max_decoding_message_size(self.peers.config.max_decoding_message_bytes);
        let validation_staged = staged.clone();
        let frames = tokio_stream::iter(std::iter::once(
            afs_protocol::node_data::DfsPutReplicaFrame {
                body: Some(afs_protocol::node_data::dfs_put_replica_frame::Body::Header(header)),
            },
        ))
        .chain(tokio_stream::iter(replica_data_frames(staged)));
        let mut request = request_with_current_context(frames);
        request.set_timeout(deadline.saturating_duration_since(tokio::time::Instant::now()));
        let reply = client
            .put_replica_stream(request)
            .await
            .map_err(afs_transport::grpc::error_status::status_to_error)?
            .into_inner();
        if reply.durable_acks.len() != op.sync_target_count - op.target_index {
            return Err(dfs_protocol_error(
                "replica reply does not include the complete synchronous tail",
            ));
        }
        let acks = reply
            .durable_acks
            .into_iter()
            .map(domain_replica_ack)
            .collect::<afs_error::Result<Vec<_>>>()?;
        crate::node::replication::validate_peer_acks(op, &validation_staged, &acks)?;
        Ok(acks)
    }
}

#[cfg(feature = "dfs")]
pub(super) fn replica_data_frames(
    staged: crate::node::chunk::StagedChunk,
) -> impl Iterator<Item = afs_protocol::node_data::DfsPutReplicaFrame> {
    // Retain the complete owner, including deferred-write/descriptor leases,
    // until the final frame slice is released. Sharing only SharedChunkBytes
    // would keep the allocation but omit the StagedChunk's separate leases.
    let bytes = prost::bytes::Bytes::from_owner(staged);
    (0..bytes.len())
        .step_by(super::super::data::DFS_REPLICA_FRAME_BYTES)
        .map(move |offset| {
            let end = (offset + super::super::data::DFS_REPLICA_FRAME_BYTES).min(bytes.len());
            afs_protocol::node_data::DfsPutReplicaFrame {
                body: Some(afs_protocol::node_data::dfs_put_replica_frame::Body::Data(
                    bytes.slice(offset..end),
                )),
            }
        })
}

#[cfg(feature = "dfs")]
pub(crate) fn replica_header(
    op: &crate::node::replication::ReplicaPeerOp,
    staged: &crate::node::chunk::StagedChunk,
) -> afs_error::Result<afs_protocol::node_data::DfsPutReplicaHeader> {
    op.validate_shape()?;
    Ok(afs_protocol::node_data::DfsPutReplicaHeader {
        operation_id: staged.operation_id.0.clone(),
        chunk_id: staged.chunk.id.0.clone(),
        chunk_length: staged.chunk.length,
        content_digest: staged.chunk.content_digest.bytes.to_vec(),
        content_digest_algorithm: wire_data_digest(staged.chunk.content_digest.algorithm),
        placement_revision: op.placement_revision,
        placement_epoch: op.placement_epoch,
        replica_group_id: op.replica_group_id.0.clone(),
        ordered_targets: op
            .ordered_targets
            .iter()
            .map(|target| afs_protocol::node_data::DfsReplicaTarget {
                node_id: target.node_id.clone(),
                node_epoch: target.node_epoch,
                data_endpoint: target.data_endpoint.clone(),
                device: Some(afs_protocol::node_data::DfsStorageDevice {
                    device_id: target.device.device_id.clone(),
                    device_epoch: target.device.device_epoch,
                    catalog_revision: target.device.catalog_revision,
                    failure_domain: target.device.failure_domain.clone(),
                }),
            })
            .collect(),
        target_index: op.target_index as u32,
        initiator_node_id: op.initiator_node_id.clone(),
        initiator_node_epoch: op.initiator_node_epoch,
        repair_claim: op
            .repair_claim
            .as_ref()
            .map(|claim| Box::new(super::super::meta::wire_replication_claim(claim))),
    })
}

#[cfg(feature = "dfs")]
pub(in crate::node::rpc) fn wire_data_digest(algorithm: crate::dfs::DigestAlgorithm) -> i32 {
    use afs_protocol::node_data::DfsDigestAlgorithm;
    match algorithm {
        crate::dfs::DigestAlgorithm::Blake3 => DfsDigestAlgorithm::Blake3 as i32,
        crate::dfs::DigestAlgorithm::Crc32cSegmentsV1 => {
            DfsDigestAlgorithm::Crc32cSegmentsV1 as i32
        }
    }
}

#[cfg(feature = "dfs")]
pub(in crate::node::rpc) fn domain_data_digest(
    algorithm: i32,
) -> afs_error::Result<crate::dfs::DigestAlgorithm> {
    use afs_protocol::node_data::DfsDigestAlgorithm;
    match DfsDigestAlgorithm::try_from(algorithm) {
        Ok(DfsDigestAlgorithm::Blake3) => Ok(crate::dfs::DigestAlgorithm::Blake3),
        Ok(DfsDigestAlgorithm::Crc32cSegmentsV1) => {
            Ok(crate::dfs::DigestAlgorithm::Crc32cSegmentsV1)
        }
        _ => Err(dfs_protocol_error("unsupported object digest algorithm")),
    }
}

#[cfg(feature = "dfs")]
fn domain_replica_ack(
    ack: afs_protocol::node_data::DfsReplicaAck,
) -> afs_error::Result<crate::dfs::ReplicaAck> {
    let algorithm = domain_data_digest(ack.verified_digest_algorithm)?;
    let bytes: [u8; 32] = ack
        .verified_digest
        .try_into()
        .map_err(|_| dfs_protocol_error("replica acknowledgement digest length is invalid"))?;
    Ok(crate::dfs::ReplicaAck {
        operation_id: crate::dfs::OperationId::new(ack.operation_id),
        chunk_id: crate::dfs::ChunkId::new(ack.chunk_id),
        placement_revision: ack.placement_revision,
        placement_epoch: ack.placement_epoch,
        node_id: ack.node_id,
        node_epoch: ack.node_epoch,
        device_id: ack.device_id,
        device_epoch: ack.device_epoch,
        catalog_revision: ack.catalog_revision,
        persisted_bytes: ack.persisted_bytes,
        verified_digest: crate::dfs::ContentDigest { algorithm, bytes },
    })
}

#[cfg(feature = "dfs")]
pub struct RdmaReplicaDataPlane {
    pub(super) pool: Arc<DfsRdmaPool>,
    pub(super) runtime: PeerRuntime,
    // Only the compiled RDMA transfer path reads this fallback.
    #[allow(dead_code)]
    fallback: Option<GrpcReplicaDataPlane>,
}

#[cfg(feature = "dfs")]
impl RdmaReplicaDataPlane {
    pub fn new(pool: Arc<DfsRdmaPool>) -> afs_error::Result<Self> {
        Ok(Self {
            pool,
            runtime: PeerRuntime::current_or_new("DFS peer")?,
            fallback: None,
        })
    }

    fn new_with_grpc_fallback(
        pool: Arc<DfsRdmaPool>,
        peers: Arc<PeerConnectionPool>,
        timeout: Duration,
    ) -> afs_error::Result<Self> {
        Ok(Self {
            pool,
            runtime: PeerRuntime::current_or_new("DFS peer")?,
            fallback: Some(GrpcReplicaDataPlane::new(peers, timeout)?),
        })
    }
}

#[cfg(feature = "dfs")]
impl crate::node::replication::ReplicaDataPlane for RdmaReplicaDataPlane {
    fn mode(&self) -> crate::node::replication::ReplicaTransferMode {
        crate::node::replication::ReplicaTransferMode::RdmaOneSided
    }
    fn prepare_peer(&self, op: &crate::node::replication::ReplicaPeerOp) -> afs_error::Result<()> {
        op.validate_shape()?;
        self.runtime
            .block_on(self.pool.peers.channel(
                &op.target.node_id,
                op.target.node_epoch,
                &op.target.data_endpoint,
            ))
            .map(|_| ())
    }
    fn put_peer_replica(
        &self,
        op: &crate::node::replication::ReplicaPeerOp,
        staged: &crate::node::chunk::StagedChunk,
    ) -> afs_error::Result<Vec<crate::dfs::ReplicaAck>> {
        #[cfg(not(feature = "rdma"))]
        {
            let _ = (op, staged, self.pool.timeout);
            Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                "DFS RDMA is not compiled",
            ))
        }
        #[cfg(feature = "rdma")]
        {
            if staged.chunk.id != op.chunk_id
                || staged.bytes().len() > crate::node::chunk::MAX_STAGED_CHUNK_BYTES
            {
                return Err(dfs_protocol_error(
                    "RDMA replica immutable identity/length differs",
                ));
            }
            let header = replica_header(op, staged)?;
            let epoch = if op.target_index == 0 {
                op.initiator_node_epoch
            } else {
                op.ordered_targets[op.target_index - 1].node_epoch
            };
            let started = std::time::Instant::now();
            let result = self.runtime.block_on(async {
                tokio::time::timeout(self.pool.timeout, async {
                    let lease = match self
                        .pool
                        .acquire(
                            &op.target.node_id,
                            op.target.node_epoch,
                            &op.target.data_endpoint,
                            afs_protocol::node_data::dfs_negotiate_rdma_request::Authority::ReplicaHeader(
                                header.clone(),
                            ),
                            epoch,
                        )
                        .await?
                    {
                        DfsRdmaAcquire::Lease(lease) => lease,
                        DfsRdmaAcquire::Unsupported => {
                            return Ok(RdmaReplicaResult::FallbackToGrpc);
                        }
                    };
                    let poisoned = lease.session.poisoned.clone();
                    let worker_op = op.clone();
                    let staged = staged.clone();
                    let task = tokio::spawn(async move {
                        let lease = lease;
                        let worker_guard = CancelPoisonGuard::new(lease.session.poisoned.clone());
                        validate_open(false, &lease.session.poisoned).map_err(|error| error.0)?;
                        let endpoint = lease.session.endpoint.clone();
                        let payload = staged.clone();
                        crate::node::bridge::spawn_blocking(move || {
                            endpoint.blocking_lock().put_local(payload.bytes())
                        })
                        .await
                        .map_err(dfs_rdma_join_error)?
                        .map_err(dfs_rdma_error)?;
                        let mut client = lease.session.client.clone();
                        let mut request = request_with_current_context(
                            afs_protocol::node_data::DfsPutReplicaRdmaRequest {
                                header: Some(header),
                                rdma_session_id: lease.session.session_id,
                                region_offset: 0,
                                length: staged.chunk.length,
                                chunk_offset: 0,
                                staging_id: staged.operation_id.0.clone(),
                                server_generation: lease.session.server_generation.clone(),
                            },
                        );
                        request.set_timeout(lease.session.timeout);
                        let reply = client
                            .put_replica_rdma(request)
                            .await
                            .map_err(afs_transport::grpc::error_status::status_to_error)?
                            .into_inner();
                        let acks = reply
                            .durable_acks
                            .into_iter()
                            .map(domain_replica_ack)
                            .collect::<afs_error::Result<Vec<_>>>()?;
                        crate::node::replication::validate_peer_acks(&worker_op, &staged, &acks)?;
                        validate_open(false, &lease.session.poisoned).map_err(|error| error.0)?;
                        worker_guard.disarm();
                        Ok::<_, afs_error::Error>((lease, acks))
                    });
                    let cancel_guard = CancelPoisonGuard::new(poisoned);
                    let (_lease, acks) = task.await.map_err(dfs_rdma_join_error)??;
                    cancel_guard.disarm();
                    Ok::<_, afs_error::Error>(RdmaReplicaResult::Acks(acks))
                })
                .await
                .map_err(|_| {
                    afs_error::Error::coded(
                        afs_error::CLIENT_DEADLINE_EXCEEDED,
                        "DFS RDMA replica total deadline exceeded",
                    )
                })?
            })?;
            match result {
                RdmaReplicaResult::Acks(acks) => Ok(acks),
                RdmaReplicaResult::FallbackToGrpc => {
                    let fallback = self.fallback.as_ref().ok_or_else(|| {
                        afs_error::Error::coded(
                            afs_error::NODE_TRANSFER_UNSUPPORTED,
                            "DFS RDMA is not supported by peer",
                        )
                    })?;
                    let timeout = remaining_timeout(started, self.pool.timeout)?;
                    afs_logging::warn!(
                        "dfs.rdma_replica_fallback";
                        "reason" => "peer reported RDMA unsupported before dispatch"
                    );
                    GrpcReplicaDataPlane::new(fallback.peers.clone(), timeout)?
                        .put_peer_replica(op, staged)
                }
            }
        }
    }
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) type DfsRdmaKey = (String, u64, String);

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) enum DfsRdmaAcquire {
    Lease(DfsRdmaLease),
    Unsupported,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
enum RdmaReplicaResult {
    Acks(Vec<crate::dfs::ReplicaAck>),
    FallbackToGrpc,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) fn dfs_rdma_canonical_unsupported(reply: &DfsNegotiateRdmaReply) -> bool {
    !reply.rdma_supported
        && reply.session_id == 0
        && reply.server_info.is_empty()
        && reply.server_generation.is_empty()
        && reply.capacity == 0
        && reply.handshake_version == RDMA_HANDSHAKE_VERSION
}

#[cfg(feature = "dfs")]
pub struct DfsRdmaPool {
    pub(super) peers: Arc<PeerConnectionPool>,
    #[cfg(feature = "rdma")]
    device: String,
    #[cfg(feature = "rdma")]
    require_server_generation: bool,
    pub(super) timeout: Duration,
    #[cfg(feature = "rdma")]
    slots: tokio::sync::Mutex<std::collections::HashMap<DfsRdmaKey, DfsRdmaSlot>>,
    #[cfg(feature = "rdma")]
    permits: Arc<tokio::sync::Semaphore>,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
static UNBOUND_SERVER_REPLIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) struct DfsRdmaSlot {
    connection: Arc<tokio::sync::Mutex<Option<Arc<DfsRdmaSession>>>>,
    touched: std::time::Instant,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) struct DfsRdmaSession {
    pub(super) endpoint: Arc<tokio::sync::Mutex<RdmaEndpointLease>>,
    pub(super) poisoned: Arc<AtomicBool>,
    pub(super) session_id: u64,
    pub(super) server_generation: Vec<u8>,
    caller_epoch: u64,
    pub(super) client: DfsChunksClient<Channel>,
    pub(super) timeout: Duration,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) struct DfsRdmaLease {
    pub(super) session: Arc<DfsRdmaSession>,
    // Held by the complete command task, including CQ and reply verification.
    _guard: tokio::sync::OwnedMutexGuard<Option<Arc<DfsRdmaSession>>>,
}

#[cfg(feature = "dfs")]
impl DfsRdmaPool {
    pub fn new(
        peers: Arc<PeerConnectionPool>,
        device: String,
        timeout: Duration,
    ) -> afs_error::Result<Self> {
        Self::new_with_generation_requirement(peers, device, timeout, false)
    }

    pub(crate) fn new_with_generation_requirement(
        peers: Arc<PeerConnectionPool>,
        device: String,
        timeout: Duration,
        require_server_generation: bool,
    ) -> afs_error::Result<Self> {
        if device.is_empty() || timeout.is_zero() {
            return Err(dfs_protocol_error("DFS RDMA device/timeout is required"));
        }
        #[cfg(not(feature = "rdma"))]
        {
            let _ = (peers, require_server_generation);
            Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                "DFS RDMA is not compiled",
            ))
        }
        #[cfg(feature = "rdma")]
        {
            // Startup preflight is bounded and cannot silently choose gRPC.
            let _ = RdmaEndpoint::open_with_capacity(
                &device,
                crate::node::chunk::MAX_STAGED_CHUNK_BYTES,
            )
            .map_err(dfs_rdma_error)?;
            Ok(Self {
                peers,
                device,
                require_server_generation,
                timeout,
                slots: tokio::sync::Mutex::new(std::collections::HashMap::new()),
                permits: Arc::new(tokio::sync::Semaphore::new(16)),
            })
        }
    }

    #[cfg(feature = "rdma")]
    pub(crate) fn unbound_server_replies() -> u64 {
        UNBOUND_SERVER_REPLIES.load(Ordering::Relaxed)
    }

    #[cfg(feature = "rdma")]
    pub(super) async fn acquire(
        &self,
        node_id: &str,
        node_epoch: u64,
        address: &str,
        authority: afs_protocol::node_data::dfs_negotiate_rdma_request::Authority,
        caller_epoch: u64,
    ) -> afs_error::Result<DfsRdmaAcquire> {
        let channel = self.peers.channel(node_id, node_epoch, address).await?;
        let key = (node_id.to_owned(), node_epoch, address.to_owned());
        let slot = {
            let mut slots = self.slots.lock().await;
            // An idle old epoch can be retired; active tasks retain their Arc.
            slots.retain(|(id, epoch, _), entry| {
                id != node_id || *epoch == node_epoch || Arc::strong_count(&entry.connection) > 1
            });
            if !slots.contains_key(&key) && slots.len() >= 16 {
                let old = slots
                    .iter()
                    .filter(|(_, entry)| Arc::strong_count(&entry.connection) == 1)
                    .min_by_key(|(_, entry)| entry.touched)
                    .map(|(key, _)| key.clone())
                    .ok_or_else(|| {
                        afs_error::Error::coded(
                            afs_error::NODE_RDMA_CAPACITY,
                            "all DFS RDMA sessions are busy",
                        )
                    })?;
                slots.remove(&old);
            }
            let entry = slots.entry(key).or_insert_with(|| DfsRdmaSlot {
                connection: Arc::new(tokio::sync::Mutex::new(None)),
                touched: std::time::Instant::now(),
            });
            entry.touched = std::time::Instant::now();
            entry.connection.clone()
        };
        let mut guard = slot.lock_owned().await;
        if guard.as_ref().is_some_and(|connection| {
            connection.poisoned.load(Ordering::SeqCst) || connection.caller_epoch != caller_epoch
        }) {
            *guard = None;
        }
        if guard.is_none() {
            // Retirement returns the permit only after native teardown. Read and
            // replica callers bound this wait with their overall deadline.
            let permit = self.permits.clone().acquire_owned().await.map_err(|_| {
                afs_error::Error::coded(afs_error::NODE_RDMA_CAPACITY, "DFS RDMA admission closed")
            })?;
            let device = self.device.clone();
            let (endpoint, info) = crate::node::bridge::spawn_blocking(move || {
                let mut endpoint = RdmaEndpointLease::open(
                    &device,
                    crate::node::chunk::MAX_STAGED_CHUNK_BYTES,
                    permit,
                )?;
                let info = endpoint.info()?;
                Ok::<_, afs_transport::rdma::RdmaError>((endpoint, info))
            })
            .await
            .map_err(dfs_rdma_join_error)?
            .map_err(dfs_rdma_error)?;
            let mut client = DfsChunksClient::new(channel);
            let mut request =
                request_with_current_context(afs_protocol::node_data::DfsNegotiateRdmaRequest {
                    client_info: info.to_vec(),
                    capacity: crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u32,
                    handshake_version: RDMA_HANDSHAKE_VERSION,
                    authority: Some(authority),
                    bind_server_generation: true,
                });
            request.set_timeout(self.timeout);
            let reply = client
                .negotiate_rdma(request)
                .await
                .map_err(afs_transport::grpc::error_status::status_to_error)?
                .into_inner();
            let mut remote_cleanup = DfsRdmaRemoteClose {
                client: client.clone(),
                session_id: reply.session_id,
                server_generation: reply.server_generation.clone(),
                caller_epoch,
                timeout: self.timeout,
                armed: true,
            };
            if !reply.rdma_supported {
                if dfs_rdma_canonical_unsupported(&reply) {
                    return Ok(DfsRdmaAcquire::Unsupported);
                }
                return Err(dfs_protocol_error(
                    "DFS RDMA negotiation unsupported reply is malformed",
                ));
            }
            if reply.session_id == 0
                || reply.capacity as usize != endpoint.capacity()
                || reply.handshake_version != RDMA_HANDSHAKE_VERSION
                || (!reply.server_generation.is_empty() && reply.server_generation.len() != 16)
            {
                return Err(dfs_protocol_error(
                    "DFS RDMA negotiation identity/capacity is invalid",
                ));
            }
            check_dfs_rdma_server_generation(
                &reply.server_generation,
                self.require_server_generation,
                node_id,
            )?;
            let server_info = reply.server_info;
            let endpoint = crate::node::bridge::spawn_blocking(move || {
                let mut endpoint = endpoint;
                endpoint.connect(&server_info)?;
                endpoint.send_probe(5000)?;
                Ok::<_, afs_transport::rdma::RdmaError>(endpoint)
            })
            .await
            .map_err(dfs_rdma_join_error)?
            .map_err(dfs_rdma_error)?;
            remote_cleanup.armed = false;
            *guard = Some(Arc::new(DfsRdmaSession {
                endpoint: Arc::new(tokio::sync::Mutex::new(endpoint)),
                poisoned: Arc::new(AtomicBool::new(false)),
                session_id: reply.session_id,
                server_generation: reply.server_generation,
                caller_epoch,
                client,
                timeout: self.timeout,
            }));
        }
        let session = guard
            .as_ref()
            .ok_or_else(|| dfs_protocol_error("DFS RDMA session publication failed"))?
            .clone();
        if session.server_generation.is_empty() {
            // Old peers can still serve one operation, but their session IDs
            // cannot fence a restart, so do not publish them for reuse.
            *guard = None;
        }
        Ok(DfsRdmaAcquire::Lease(DfsRdmaLease {
            session,
            _guard: guard,
        }))
    }
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
fn check_dfs_rdma_server_generation(
    generation: &[u8],
    required: bool,
    node_id: &str,
) -> afs_error::Result<()> {
    if generation.is_empty() {
        let total = UNBOUND_SERVER_REPLIES.fetch_add(1, Ordering::Relaxed) + 1;
        // A legacy peer can negotiate on each command. Record every reply
        // while keeping its warning rate bounded during a rolling upgrade.
        if total.is_power_of_two() {
            afs_logging::warn!(
                "dfs.rdma_server_generation_absent";
                "peer" => node_id,
                "rejected" => required,
                "total" => total
            );
        }
        if required {
            return Err(afs_error::Error::coded(
                afs_error::NODE_RDMA_HANDSHAKE_VERSION,
                "DFS RDMA peer did not return the required server generation",
            ));
        }
    }
    Ok(())
}

#[cfg(all(test, feature = "dfs", feature = "rdma"))]
#[test]
fn dfs_rdma_empty_server_generation_obeys_compatibility_policy() {
    let before = DfsRdmaPool::unbound_server_replies();
    check_dfs_rdma_server_generation(&[], false, "old-peer").unwrap();
    let status = check_dfs_rdma_server_generation(&[], true, "old-peer").unwrap_err();
    assert_eq!(status.code(), afs_error::NODE_RDMA_HANDSHAKE_VERSION);
    check_dfs_rdma_server_generation(&[1; 16], true, "new-peer").unwrap();
    assert!(DfsRdmaPool::unbound_server_replies() >= before + 2);
}

#[cfg(all(test, feature = "dfs", feature = "rdma"))]
#[test]
#[ignore = "requires AFS_TEST_RDMA_DEVICE with a working Linux RXE/RDMA device"]
fn dfs_rdma_full_pool_waits_for_evicted_endpoint() {
    use crate::node::rpc::{
        control::RdmaSessionRegistry,
        data::{
            DenyDfsReplicaAuthorizer, DfsChunkTransportResources, DfsReadAuthorizer,
            PeerAuthenticator, make_dfs_chunks_server_with_transport,
        },
    };
    use afs_protocol::node_data::{
        DfsChunkReadOp, DfsReadGrant, dfs_negotiate_rdma_request::Authority,
    };
    use std::{future::poll_fn, task::Poll};

    struct TestPeer;
    impl PeerAuthenticator for TestPeer {
        fn authenticate(
            &self,
            _: &tonic::metadata::MetadataMap,
            _: Option<std::net::SocketAddr>,
            _: Option<&[u8]>,
        ) -> afs_error::Result<String> {
            Ok("reader".into())
        }
    }
    impl DfsReadAuthorizer for TestPeer {
        fn authorize(&self, _: &str, _: &DfsReadRangesRequest) -> afs_error::Result<()> {
            Ok(())
        }
    }

    let device = std::env::var("AFS_TEST_RDMA_DEVICE").expect("set the Linux RXE device");
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
        .block_on(async {
            let timeout = Duration::from_secs(5);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(make_dfs_chunks_server_with_transport(
                        None,
                        Arc::new(TestPeer),
                        Arc::new(TestPeer),
                        Arc::new(DenyDfsReplicaAuthorizer),
                        None,
                        timeout,
                        DfsChunkTransportResources {
                            rdma_sessions: RdmaSessionRegistry::new(Some(device.clone())),
                            payload_metrics: None,
                            holders: None,
                        },
                    ))
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
            );
            let peers = Arc::new(
                PeerConnectionPool::new(
                    afs_transport::GrpcConfig::default(),
                    afs_transport::TlsConfig::Disabled,
                    32,
                )
                .unwrap(),
            );
            let pool = DfsRdmaPool::new(peers, device, timeout).unwrap();
            let authority = Authority::ReadRequest(DfsReadRangesRequest {
                read_id: "read".into(),
                attempt_id: "attempt".into(),
                file_version_id: "version".into(),
                layout_root_id: "layout".into(),
                operations: vec![DfsChunkReadOp {
                    chunk_id: "chunk".into(),
                    length: 4096,
                    source_copy_id: "copy".into(),
                    grant: Some(DfsReadGrant {
                        namespace_id: "namespace".into(),
                        file_version_id: "version".into(),
                        layout_root_id: "layout".into(),
                        caller_node_id: "reader".into(),
                        caller_node_epoch: 1,
                        expires_at_unix_ms: u64::MAX,
                        token: "fixture-grant".into(),
                        fence: 1,
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            });
            let capacity = pool.permits.available_permits();
            for index in 0..capacity {
                let acquired = pool
                    .acquire(&format!("peer-{index}"), 1, &address, authority.clone(), 1)
                    .await
                    .unwrap();
                assert!(matches!(acquired, DfsRdmaAcquire::Lease(_)));
            }
            assert_eq!(pool.slots.lock().await.len(), capacity);
            assert_eq!(pool.permits.available_permits(), 0);

            // Occupy the only blocking worker so the evicted endpoint still owns
            // its permit when the replacement reaches admission.
            let (started, busy) = tokio::sync::oneshot::channel();
            let (resume, blocked) = std::sync::mpsc::channel();
            let worker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                let _ = blocked.recv();
            });
            busy.await.unwrap();
            let replacement = pool.acquire("new-peer", 1, &address, authority, 1);
            tokio::pin!(replacement);
            let first_poll = poll_fn(|cx| Poll::Ready(replacement.as_mut().poll(cx))).await;
            assert!(
                pool.slots
                    .try_lock()
                    .expect("replacing a session must release the pool lock before waiting")
                    .contains_key(&("new-peer".into(), 1, address.clone()))
            );
            assert_eq!(pool.permits.available_permits(), 0);
            resume.send(()).unwrap();
            worker.await.unwrap();

            let result = match first_poll {
                Poll::Ready(result) => result,
                Poll::Pending => tokio::time::timeout(timeout, replacement).await.unwrap(),
            };
            let acquired = result.expect("replacement must reuse the permit after retirement");
            assert!(matches!(acquired, DfsRdmaAcquire::Lease(_)));
            assert_eq!(pool.slots.lock().await.len(), capacity);
            server.abort();
        });
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
impl Drop for DfsRdmaSession {
    fn drop(&mut self) {
        self.poisoned.store(true, Ordering::SeqCst);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let mut client = self.client.clone();
            let session_id = self.session_id;
            let epoch = self.caller_epoch;
            let server_generation = self.server_generation.clone();
            let timeout = self.timeout;
            handle.spawn(async move {
                let mut request =
                    request_with_current_context(afs_protocol::node_data::DfsCloseRdmaRequest {
                        session_id,
                        peer_node_epoch: epoch,
                        server_generation,
                    });
                request.set_timeout(timeout);
                let _ = client.close_rdma(request).await;
            });
        }
    }
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) fn dfs_rdma_error(error: afs_transport::rdma::RdmaError) -> afs_error::Error {
    afs_error::Error::coded(afs_error::NODE_TRANSFER_UNAVAILABLE, error.to_string())
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) fn dfs_rdma_join_error(error: tokio::task::JoinError) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string())
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
struct DfsRdmaRemoteClose {
    pub(super) client: DfsChunksClient<Channel>,
    pub(super) session_id: u64,
    server_generation: Vec<u8>,
    caller_epoch: u64,
    pub(super) timeout: Duration,
    armed: bool,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
impl Drop for DfsRdmaRemoteClose {
    fn drop(&mut self) {
        if !self.armed || self.session_id == 0 {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let mut client = self.client.clone();
            let session_id = self.session_id;
            let peer_node_epoch = self.caller_epoch;
            let server_generation = self.server_generation.clone();
            let timeout = self.timeout;
            handle.spawn(async move {
                let mut request =
                    request_with_current_context(afs_protocol::node_data::DfsCloseRdmaRequest {
                        session_id,
                        peer_node_epoch,
                        server_generation,
                    });
                request.set_timeout(timeout);
                let _ = client.close_rdma(request).await;
            });
        }
    }
}

#[cfg(feature = "dfs")]
pub fn make_replica_data_plane(
    mode: DataMode,
    peers: Arc<PeerConnectionPool>,
    timeout: Duration,
    rdma: Option<Arc<DfsRdmaPool>>,
) -> afs_error::Result<Arc<dyn crate::node::replication::ReplicaDataPlane>> {
    match mode {
        DataMode::Rdma => Ok(Arc::new(RdmaReplicaDataPlane::new(
            rdma.ok_or_else(|| dfs_protocol_error("DFS RDMA resources are absent"))?,
        )?)),
        DataMode::Auto => {
            if let Some(rdma) = rdma {
                Ok(Arc::new(RdmaReplicaDataPlane::new_with_grpc_fallback(
                    rdma, peers, timeout,
                )?))
            } else {
                Ok(Arc::new(GrpcReplicaDataPlane::new(peers, timeout)?))
            }
        }
        DataMode::Grpc => Ok(Arc::new(GrpcReplicaDataPlane::new(peers, timeout)?)),
    }
}
