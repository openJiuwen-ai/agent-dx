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
        let staged = staged.clone();
        self.runtime.block_on(async {
            tokio::time::timeout(self.timeout, async {
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
                        body: Some(
                            afs_protocol::node_data::dfs_put_replica_frame::Body::Header(header),
                        ),
                    },
                ))
                .chain(tokio_stream::iter(replica_data_frames(staged)));
                let mut request = request_with_current_context(frames);
                request.set_timeout(self.timeout);
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
            })
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "DFS replica transfer total deadline exceeded",
                )
            })?
        })
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
                    let cancel_guard = CancelPoisonGuard::new(lease.session.poisoned.clone());
                    let worker_op = op.clone();
                    let staged = staged.clone();
                    let task = tokio::spawn(async move {
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
                        Ok::<_, afs_error::Error>(RdmaReplicaResult::Acks(acks))
                    });
                    let result = task.await.map_err(dfs_rdma_join_error)?;
                    if result.is_ok() {
                        cancel_guard.disarm();
                    }
                    result
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
        && reply.capacity == 0
        && reply.handshake_version == RDMA_HANDSHAKE_VERSION
}

#[cfg(feature = "dfs")]
pub struct DfsRdmaPool {
    pub(super) peers: Arc<PeerConnectionPool>,
    #[cfg(feature = "rdma")]
    device: String,
    pub(super) timeout: Duration,
    #[cfg(feature = "rdma")]
    slots: tokio::sync::Mutex<std::collections::HashMap<DfsRdmaKey, DfsRdmaSlot>>,
    #[cfg(feature = "rdma")]
    permits: Arc<tokio::sync::Semaphore>,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) struct DfsRdmaSlot {
    connection: Arc<tokio::sync::Mutex<Option<Arc<DfsRdmaSession>>>>,
    touched: std::time::Instant,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) struct DfsRdmaSession {
    pub(super) endpoint: Arc<tokio::sync::Mutex<RdmaEndpoint>>,
    pub(super) poisoned: Arc<AtomicBool>,
    pub(super) session_id: u64,
    caller_epoch: u64,
    pub(super) client: DfsChunksClient<Channel>,
    pub(super) timeout: Duration,
    pub(super) _permit: tokio::sync::OwnedSemaphorePermit,
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
        if device.is_empty() || timeout.is_zero() {
            return Err(dfs_protocol_error("DFS RDMA device/timeout is required"));
        }
        #[cfg(not(feature = "rdma"))]
        {
            let _ = peers;
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
                timeout,
                slots: tokio::sync::Mutex::new(std::collections::HashMap::new()),
                permits: Arc::new(tokio::sync::Semaphore::new(16)),
            })
        }
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
            let permit = self.permits.clone().try_acquire_owned().map_err(|_| {
                afs_error::Error::coded(
                    afs_error::NODE_RDMA_CAPACITY,
                    "DFS RDMA registered memory budget is full",
                )
            })?;
            let device = self.device.clone();
            let (endpoint, info, permit) = crate::node::bridge::spawn_blocking(move || {
                let mut endpoint = RdmaEndpoint::open_with_capacity(
                    &device,
                    crate::node::chunk::MAX_STAGED_CHUNK_BYTES,
                )?;
                let info = endpoint.info()?;
                Ok::<_, afs_transport::rdma::RdmaError>((endpoint, info, permit))
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
            {
                return Err(dfs_protocol_error(
                    "DFS RDMA negotiation identity/capacity is invalid",
                ));
            }
            let server_info = reply.server_info;
            let (endpoint, permit) = crate::node::bridge::spawn_blocking(move || {
                let mut endpoint = endpoint;
                endpoint.connect(&server_info)?;
                endpoint.send_probe(5000)?;
                Ok::<_, afs_transport::rdma::RdmaError>((endpoint, permit))
            })
            .await
            .map_err(dfs_rdma_join_error)?
            .map_err(dfs_rdma_error)?;
            remote_cleanup.armed = false;
            *guard = Some(Arc::new(DfsRdmaSession {
                endpoint: Arc::new(tokio::sync::Mutex::new(endpoint)),
                poisoned: Arc::new(AtomicBool::new(false)),
                session_id: reply.session_id,
                caller_epoch,
                client,
                timeout: self.timeout,
                _permit: permit,
            }));
        }
        let session = guard
            .as_ref()
            .ok_or_else(|| dfs_protocol_error("DFS RDMA session publication failed"))?
            .clone();
        Ok(DfsRdmaAcquire::Lease(DfsRdmaLease {
            session,
            _guard: guard,
        }))
    }
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
impl Drop for DfsRdmaSession {
    fn drop(&mut self) {
        self.poisoned.store(true, Ordering::SeqCst);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let mut client = self.client.clone();
            let session_id = self.session_id;
            let epoch = self.caller_epoch;
            let timeout = self.timeout;
            handle.spawn(async move {
                let mut request =
                    request_with_current_context(afs_protocol::node_data::DfsCloseRdmaRequest {
                        session_id,
                        peer_node_epoch: epoch,
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
            let timeout = self.timeout;
            handle.spawn(async move {
                let mut request =
                    request_with_current_context(afs_protocol::node_data::DfsCloseRdmaRequest {
                        session_id,
                        peer_node_epoch,
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
