//! DFS peer reads over RDMA.

use super::*;

#[cfg(feature = "dfs")]
pub struct RdmaChunkTransfer {
    pub(super) pool: Arc<DfsRdmaPool>,
    pub(super) runtime: PeerRuntime,
    pub(super) next_read: std::sync::atomic::AtomicU64,
    // Only the compiled RDMA transfer path reads this fallback.
    #[allow(dead_code)]
    fallback: Option<GrpcChunkTransfer>,
}

#[cfg(feature = "dfs")]
impl RdmaChunkTransfer {
    pub fn new(pool: Arc<DfsRdmaPool>) -> afs_error::Result<Self> {
        Ok(Self {
            pool,
            runtime: PeerRuntime::current_or_new("DFS peer")?,
            next_read: std::sync::atomic::AtomicU64::new(1),
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
            next_read: std::sync::atomic::AtomicU64::new(1),
            fallback: Some(GrpcChunkTransfer::new(peers, timeout)?),
        })
    }
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
enum RdmaReadResult {
    Complete,
    FallbackToGrpc,
}

#[cfg(feature = "dfs")]
impl crate::node::read::engine::ChunkTransfer for RdmaChunkTransfer {
    fn block_on_read(
        &self,
        future: crate::node::read::admission::ReadFuture<'_, ()>,
    ) -> afs_error::Result<()> {
        self.runtime.block_on(future)
    }

    fn verification_metrics(&self) -> Option<&ReadWorkMetrics> {
        self.pool.peers.read_work_metrics.as_ref()
    }
    /// The review is one small control RPC; it never needs the RDMA data path.
    fn review_suspect_chunk(
        &self,
        suspect: &crate::node::fault::suspect::SuspectCopy,
    ) -> afs_error::Result<bool> {
        match &self.fallback {
            Some(grpc) => grpc.review_suspect_chunk(suspect),
            None => Ok(false),
        }
    }
    fn read_ranges(
        &self,
        batch: &crate::node::read::engine::PeerReadBatch,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        self.read_ranges_integrity(batch, out, None)
    }
    fn read_crc32c_range(
        &self,
        batch: &crate::node::read::engine::PeerReadBatch,
        chunk: &ChunkObject,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        self.read_ranges_integrity(batch, out, Some(chunk))
    }
}

#[cfg(feature = "dfs")]
impl RdmaChunkTransfer {
    fn read_ranges_integrity(
        &self,
        batch: &crate::node::read::engine::PeerReadBatch,
        out: &mut [u8],
        expected: Option<&ChunkObject>,
    ) -> afs_error::Result<()> {
        #[cfg(not(feature = "rdma"))]
        {
            let _ = (
                batch,
                out,
                expected,
                &self.pool,
                &self.runtime,
                &self.next_read,
            );
            Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                "DFS RDMA is not compiled",
            ))
        }
        #[cfg(feature = "rdma")]
        {
            batch.validate(out.len())?;
            let integrity = expected.cloned();
            let first = &batch.operations[0].source;
            let crate::dfs::CopyLocation::Node {
                node_id,
                node_epoch,
                ..
            } = &first.location
            else {
                return Err(dfs_protocol_error("RDMA source is not a Node"));
            };
            let endpoint = first
                .data_endpoint
                .as_deref()
                .ok_or_else(|| dfs_protocol_error("RDMA source endpoint is absent"))?;
            let sequence = self
                .next_read
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let windows = pack_dfs_rdma_read_windows(batch, sequence)?;
            let started = std::time::Instant::now();
            let result = self.runtime.block_on(async {
                tokio::time::timeout(self.pool.timeout, async {
                    for (index, (request, mappings, length)) in windows.into_iter().enumerate() {
                        let lease = match self
                            .pool
                            .acquire(
                                node_id,
                                *node_epoch,
                                endpoint,
                                afs_protocol::node_data::dfs_negotiate_rdma_request::Authority::ReadRequest(
                                    request.clone(),
                                ),
                                first.read_grant.caller_node_epoch,
                            )
                            .await?
                        {
                            DfsRdmaAcquire::Lease(lease) => lease,
                            DfsRdmaAcquire::Unsupported if index == 0 => {
                                return Ok(RdmaReadResult::FallbackToGrpc);
                            }
                            DfsRdmaAcquire::Unsupported => {
                                return Err(afs_error::Error::coded(
                                    afs_error::NODE_TRANSFER_UNSUPPORTED,
                                    "DFS RDMA became unsupported after read dispatch started",
                                ));
                            }
                        };
                        let cancel_guard = CancelPoisonGuard::new(lease.session.poisoned.clone());
                        let integrity = integrity.clone();
                        let task = tokio::spawn(async move {
                            validate_open(false, &lease.session.poisoned).map_err(|error| error.0)?;
                            let mut client = lease.session.client.clone();
                            let mut command = request_with_current_context(
                                afs_protocol::node_data::DfsReadRangesRdmaRequest {
                                    request: Some(request.clone()),
                                    rdma_session_id: lease.session.session_id,
                                },
                            );
                            command.set_timeout(lease.session.timeout);
                            let reply = client
                                .read_ranges_rdma(command)
                                .await
                                .map_err(afs_transport::grpc::error_status::status_to_error)?
                                .into_inner();
                            let endpoint = lease.session.endpoint.clone();
                            let bytes = crate::node::bridge::spawn_blocking(move || {
                                endpoint.blocking_lock().get_local(length)
                            })
                            .await
                            .map_err(dfs_rdma_join_error)?
                            .map_err(dfs_rdma_error)?;
                            validate_dfs_rdma_read_reply(&request, &reply, &bytes)?;
                            if let Some(chunk) = &integrity { crc32c::validate_rdma_crc(&request, &reply, &bytes, chunk)?; }
                            validate_open(false, &lease.session.poisoned).map_err(|error| error.0)?;
                            Ok::<_, afs_error::Error>(bytes)
                        });
                        let result = task.await.map_err(dfs_rdma_join_error)?;
                        let bytes = result?;
                        cancel_guard.disarm();
                        for (destination, range) in mappings {
                            out[destination..destination + range.len()].copy_from_slice(&bytes[range]);
                        }
                    }
                    Ok::<_, afs_error::Error>(RdmaReadResult::Complete)
                })
                .await
                .map_err(|_| {
                    afs_error::Error::coded(
                        afs_error::CLIENT_DEADLINE_EXCEEDED,
                        "DFS RDMA read total deadline exceeded",
                    )
                })?
            })?;
            match result {
                RdmaReadResult::Complete => Ok(()),
                RdmaReadResult::FallbackToGrpc => {
                    let fallback = self.fallback.as_ref().ok_or_else(|| {
                        afs_error::Error::coded(
                            afs_error::NODE_TRANSFER_UNSUPPORTED,
                            "DFS RDMA is not supported by peer",
                        )
                    })?;
                    let timeout = remaining_timeout(started, self.pool.timeout)?;
                    afs_logging::warn!(
                        "dfs.rdma_read_fallback";
                        "reason" => "peer reported RDMA unsupported before dispatch"
                    );
                    let grpc = GrpcChunkTransfer::new(fallback.peers.clone(), timeout)?;
                    if let Some(chunk) = expected {
                        grpc.read_crc32c_range(batch, chunk, out)
                    } else {
                        grpc.read_ranges(batch, out)
                    }
                }
            }
        }
    }
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) type DfsRdmaReadWindow = (
    DfsReadRangesRequest,
    Vec<(usize, std::ops::Range<usize>)>,
    usize,
);

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) fn pack_dfs_rdma_read_windows(
    batch: &crate::node::read::engine::PeerReadBatch,
    sequence: u64,
) -> afs_error::Result<Vec<DfsRdmaReadWindow>> {
    let first = &batch.operations[0].source.read_grant;
    let mut windows = Vec::new();
    let mut request = DfsReadRangesRequest {
        include_blake3_outboard: false,
        read_id: format!(
            "{}-{}-{sequence}",
            first.caller_node_id, first.caller_node_epoch
        ),
        attempt_id: format!("rdma-{sequence}-0"),
        file_version_id: batch.file_version_id.0.clone(),
        layout_root_id: batch.layout_root_id.0.clone(),
        operations: Vec::new(),
    };
    let mut mappings = Vec::new();
    let mut length = 0;
    for item in &batch.operations {
        let count = usize::try_from(item.op.length)
            .map_err(|_| dfs_protocol_error("RDMA range length overflow"))?;
        if count == 0 || count > crate::node::chunk::MAX_STAGED_CHUNK_BYTES {
            return Err(dfs_protocol_error(
                "RDMA range exceeds one immutable Chunk window",
            ));
        }
        if length + count > crate::node::chunk::MAX_STAGED_CHUNK_BYTES {
            let mut next = request.clone();
            next.operations.clear();
            next.attempt_id = format!("rdma-{sequence}-{}", windows.len() + 1);
            windows.push((request, mappings, length));
            request = next;
            mappings = Vec::new();
            length = 0;
        }
        request.operations.push(DfsChunkReadOp {
            operation_index: request.operations.len() as u32,
            chunk_id: item.op.chunk_id.0.clone(),
            chunk_offset: item.op.chunk_offset,
            length: item.op.length,
            destination_offset: length as u64,
            source_copy_id: item.source.copy_id.0.clone(),
            grant: Some(wire_dfs_read_grant(&item.source.read_grant)),
        });
        mappings.push((item.op.output_offset, length..length + count));
        length += count;
    }
    if !request.operations.is_empty() {
        windows.push((request, mappings, length));
    }
    Ok(windows)
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) fn validate_dfs_rdma_read_reply(
    request: &DfsReadRangesRequest,
    reply: &afs_protocol::node_data::DfsReadRangesRdmaReply,
    bytes: &[u8],
) -> afs_error::Result<()> {
    if reply.read_id != request.read_id
        || reply.attempt_id != request.attempt_id
        || reply.completions.len() != request.operations.len()
        || reply.transferred_bytes as usize != bytes.len()
    {
        return Err(dfs_protocol_error(
            "RDMA read completion batch identity differs",
        ));
    }
    for (completion, op) in reply.completions.iter().zip(&request.operations) {
        let start = op.destination_offset as usize;
        let end = start
            .checked_add(op.length as usize)
            .ok_or_else(|| dfs_protocol_error("RDMA read range overflow"))?;
        let range = bytes
            .get(start..end)
            .ok_or_else(|| dfs_protocol_error("RDMA read range exceeds MR"))?;
        if completion.read_id != request.read_id
            || completion.attempt_id != request.attempt_id
            || completion.operation_index != op.operation_index
            || completion.source_copy_id != op.source_copy_id
            || completion.transferred_bytes != op.length
        {
            return Err(dfs_protocol_error(
                "RDMA read completion identity/length differs",
            ));
        }
        let checksum = match afs_protocol::node_data::DfsDigestAlgorithm::try_from(
            completion.range_checksum_algorithm,
        ) {
            Ok(afs_protocol::node_data::DfsDigestAlgorithm::Blake3) => {
                blake3::hash(range).as_bytes().to_vec()
            }
            Ok(afs_protocol::node_data::DfsDigestAlgorithm::Crc32c) => {
                crate::node::chunk_crc32c::checksum(range)
                    .to_le_bytes()
                    .to_vec()
            }
            _ => return Err(dfs_protocol_error("unsupported RDMA range checksum")),
        };
        if completion.range_checksum != checksum {
            return Err(dfs_corrupt_error("RDMA read completion checksum differs"));
        }
    }
    Ok(())
}

#[cfg(feature = "dfs")]
pub fn make_chunk_transfer(
    mode: DataMode,
    peers: Arc<PeerConnectionPool>,
    timeout: Duration,
    rdma: Option<Arc<DfsRdmaPool>>,
) -> afs_error::Result<Arc<dyn crate::node::read::engine::ChunkTransfer>> {
    match mode {
        DataMode::Rdma => Ok(Arc::new(RdmaChunkTransfer::new(
            rdma.ok_or_else(|| dfs_protocol_error("DFS RDMA resources are absent"))?,
        )?)),
        DataMode::Auto => {
            if let Some(rdma) = rdma {
                Ok(Arc::new(RdmaChunkTransfer::new_with_grpc_fallback(
                    rdma, peers, timeout,
                )?))
            } else {
                Ok(Arc::new(GrpcChunkTransfer::new(peers, timeout)?))
            }
        }
        DataMode::Grpc => Ok(Arc::new(GrpcChunkTransfer::new(peers, timeout)?)),
    }
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
pub(super) fn remaining_timeout(
    started: std::time::Instant,
    timeout: Duration,
) -> afs_error::Result<Duration> {
    timeout.checked_sub(started.elapsed()).ok_or_else(|| {
        afs_error::Error::coded(
            afs_error::CLIENT_DEADLINE_EXCEEDED,
            "DFS Auto transport fallback deadline expired",
        )
    })
}
