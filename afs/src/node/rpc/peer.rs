//! 调用其他 Node 的诊断数据 API 与 adapter 边界。
//!
//! 上层诊断调用 DataPeerClient::read/write/close，不关心底层走 gRPC inline
//! 还是 RDMA one-sided。服务端对应 data.rs 的诊断 handler；OwnerFs 文件
//! 业务使用 node_data.proto 中独立的 OwnerFiles service 和 remote.rs 合同，
//! 尚未接入本客户端。
//!
//! 模式含义：
//! - Grpc：控制命令和文件内容都进 node_data proto；
//! - Rdma：control/data 命令仍走 gRPC proto，文件内容走 RDMA MR；
//! - Auto：先尝试 RDMA 建连，失败才在“尚未发出业务请求”前回退到 gRPC。
//!
//! 已经发出的写如果结果不明，绝不换通道重放；RDMA in-flight 被取消时会 poison
//! 当前 client/session，后续复用必须失败，避免重复写或顺序错乱。

#[cfg(feature = "dfs")]
mod crc32c;

#[cfg(all(feature = "dfs", feature = "rdma"))]
use crate::node::dfs_read::ChunkTransfer;

#[cfg(feature = "dfs")]
use crate::{
    dfs::ChunkObject,
    node::{
        chunk::outboard::{ChunkOutboard, OutboardBudget, OutboardBuilder, SharedOutboard},
        peer_cache::{CompletedPeerDiskRead, PeerStreamSink, VerifiedPeerChunk},
        read_work::{
            ReadDiagnosticConsumer, ReadDiagnosticConsumerStage, ReadDiagnosticIntervals,
            ReadDiagnosticProducer, ReadDiagnosticStage, ReadWorkMetrics,
        },
    },
};
#[cfg(any(feature = "ownerfs", feature = "dfs", feature = "rdma", test))]
use std::sync::Arc;
#[cfg(feature = "rdma")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
#[cfg(feature = "ownerfs")]
use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    os::unix::ffi::{OsStrExt, OsStringExt},
    sync::{
        Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
#[cfg(feature = "dfs")]
use tokio_stream::StreamExt;

#[cfg(feature = "rdma")]
use afs_protocol::node_control::{
    CloseDataRequest, NegotiateDataRequest, node_control_client::NodeControlClient,
};
#[cfg(all(feature = "ownerfs", feature = "rdma"))]
use afs_protocol::node_control::{OwnerCloseDataRequest, OwnerNegotiateDataRequest};
#[cfg(all(feature = "dfs", feature = "rdma"))]
use afs_protocol::node_data::DfsNegotiateRdmaReply;
#[cfg(feature = "ownerfs")]
use afs_protocol::node_data::{
    DataPlane, FileIdentity as PbFileIdentity, OwnerCreateRequest, OwnerDirectoryHandle,
    OwnerFileAttr, OwnerFileKind, OwnerFlushRequest, OwnerFsyncDirRequest, OwnerFsyncRequest,
    OwnerGetAttrRequest, OwnerGetXattrRequest, OwnerHandle, OwnerLinkRequest,
    OwnerListXattrRequest, OwnerLookupRequest, OwnerMkdirRequest, OwnerMknodRequest,
    OwnerOpenRequest, OwnerOpendirRequest, OwnerReadRequest, OwnerReaddirRequest,
    OwnerReadlinkRequest, OwnerReleaseDirRequest, OwnerReleaseRequest, OwnerRemoveXattrRequest,
    OwnerRenameRequest, OwnerRmdirRequest, OwnerSetAttr, OwnerSetAttrRequest, OwnerSetXattrRequest,
    OwnerStatFsRequest, OwnerSymlinkRequest, OwnerUnlinkRequest, OwnerWriteRequest, RootAccess,
    owner_files_client::OwnerFilesClient,
};
use afs_protocol::node_data::{
    DataReadRequest, DataTransfer, DataWriteRequest, node_data_client::NodeDataClient,
};
#[cfg(feature = "dfs")]
use afs_protocol::node_data::{
    DfsChunkReadOp, DfsReadGrant as PbDfsReadGrant, DfsReadRangesRequest,
    dfs_chunks_client::DfsChunksClient, dfs_read_ranges_frame,
};
#[cfg(feature = "rdma")]
use afs_tracing::Instrument;
use afs_tracing::request_with_current_context;
use tonic::transport::{Channel, Endpoint};

#[cfg(feature = "rdma")]
use tokio::sync::Mutex;

#[cfg(feature = "rdma")]
use super::control::RDMA_HANDSHAKE_VERSION;
#[cfg(feature = "rdma")]
use afs_transport::rdma::{CAPACITY, RdmaEndpoint};

#[cfg(feature = "ownerfs")]
use crate::node::vfs::{
    ownerfs::{
        files::{FileIdentity, OwnerEntry, RemoteDirectory, RemoteFile},
        remote::{RemoteCreatedFile, RemoteDirectoryEntry, RemoteFiles},
        root::RootGrant,
    },
    types::{
        AttributeChange, FileAttributes, FileKind, FilesystemCapacity, OpenOptions, RenameFlags,
        RequestContext, SetAttrOptions, SpecialFileKind, WriteOptions,
    },
};

const MAX_TRANSFER_BYTES: usize = crate::node::storage::MAX_TRANSFER_BYTES;
const DFS_PEER_RECEIVE_FRAME_BYTES: u32 = 128 * 1024;
#[cfg(feature = "ownerfs")]
const MAX_PENDING_RELEASES: usize = 64;
#[cfg(feature = "ownerfs")]
const RELEASE_MAX_ATTEMPTS: usize = 4;
#[cfg(feature = "ownerfs")]
const RELEASE_INITIAL_BACKOFF: Duration = Duration::from_millis(2);
#[cfg(feature = "ownerfs")]
const RELEASE_MAX_BACKOFF: Duration = Duration::from_millis(20);
#[cfg(feature = "ownerfs")]
pub const OWNER_RDMA_MAX_CLIENT_WINDOWS: usize = 64;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataMode {
    Grpc,
    Rdma,
    Auto,
}

#[cfg(feature = "dfs")]
#[derive(Clone)]
enum PeerRuntime {
    Existing(tokio::runtime::Handle),
    Owned(Arc<tokio::runtime::Runtime>),
}

#[cfg(feature = "dfs")]
impl PeerRuntime {
    fn current_or_new() -> PeerResult<Self> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return Ok(Self::Existing(handle));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                PeerError::coded(
                    afs_error::CLIENT_WORKER_FAILED,
                    format!("failed to build DFS peer read runtime: {error}"),
                )
            })?;
        Ok(Self::Owned(Arc::new(runtime)))
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        match self {
            Self::Existing(handle) => handle.block_on(future),
            Self::Owned(runtime) => runtime.block_on(future),
        }
    }
}

#[cfg(feature = "dfs")]
pub struct GrpcReplicaDataPlane {
    peers: Arc<PeerConnectionPool>,
    timeout: Duration,
    runtime: PeerRuntime,
}

#[cfg(feature = "dfs")]
impl GrpcReplicaDataPlane {
    pub fn new(peers: Arc<PeerConnectionPool>, timeout: Duration) -> afs_error::Result<Self> {
        Ok(Self {
            peers,
            timeout,
            runtime: PeerRuntime::current_or_new().map_err(|error| {
                afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string())
            })?,
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
            || staged.bytes().len() > super::data::MAX_DFS_REPLICA_BYTES
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
fn replica_data_frames(
    staged: crate::node::chunk::StagedChunk,
) -> impl Iterator<Item = afs_protocol::node_data::DfsPutReplicaFrame> {
    // Retain the complete owner, including deferred-write/descriptor leases,
    // until the final frame slice is released. Sharing only SharedChunkBytes
    // would keep the allocation but omit the StagedChunk's separate leases.
    let bytes = prost::bytes::Bytes::from_owner(staged);
    (0..bytes.len())
        .step_by(super::data::DFS_REPLICA_FRAME_BYTES)
        .map(move |offset| {
            let end = (offset + super::data::DFS_REPLICA_FRAME_BYTES).min(bytes.len());
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
            .map(|claim| Box::new(super::meta::wire_replication_claim(claim))),
    })
}

#[cfg(feature = "dfs")]
pub(super) fn wire_data_digest(algorithm: crate::dfs::DigestAlgorithm) -> i32 {
    use afs_protocol::node_data::DfsDigestAlgorithm;
    match algorithm {
        crate::dfs::DigestAlgorithm::Blake3 => DfsDigestAlgorithm::Blake3 as i32,
        crate::dfs::DigestAlgorithm::Crc32cSegmentsV1 => {
            DfsDigestAlgorithm::Crc32cSegmentsV1 as i32
        }
    }
}
#[cfg(feature = "dfs")]
pub(super) fn domain_data_digest(algorithm: i32) -> afs_error::Result<crate::dfs::DigestAlgorithm> {
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
    pool: Arc<DfsRdmaPool>,
    runtime: PeerRuntime,
    // Only the compiled RDMA transfer path reads this fallback.
    #[allow(dead_code)]
    fallback: Option<GrpcReplicaDataPlane>,
}
#[cfg(feature = "dfs")]
impl RdmaReplicaDataPlane {
    pub fn new(pool: Arc<DfsRdmaPool>) -> afs_error::Result<Self> {
        Ok(Self {
            pool,
            runtime: PeerRuntime::current_or_new().map_err(|error| error.0)?,
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
            runtime: PeerRuntime::current_or_new().map_err(|error| error.0)?,
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
                        tokio::task::spawn_blocking(move || {
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
type DfsRdmaKey = (String, u64, String);

#[cfg(all(feature = "dfs", feature = "rdma"))]
enum DfsRdmaAcquire {
    Lease(DfsRdmaLease),
    Unsupported,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
enum RdmaReplicaResult {
    Acks(Vec<crate::dfs::ReplicaAck>),
    FallbackToGrpc,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
fn dfs_rdma_canonical_unsupported(reply: &DfsNegotiateRdmaReply) -> bool {
    !reply.rdma_supported
        && reply.session_id == 0
        && reply.server_info.is_empty()
        && reply.capacity == 0
        && reply.handshake_version == RDMA_HANDSHAKE_VERSION
}

#[cfg(feature = "dfs")]
pub struct DfsRdmaPool {
    peers: Arc<PeerConnectionPool>,
    #[cfg(feature = "rdma")]
    device: String,
    timeout: Duration,
    #[cfg(feature = "rdma")]
    slots: tokio::sync::Mutex<std::collections::HashMap<DfsRdmaKey, DfsRdmaSlot>>,
    #[cfg(feature = "rdma")]
    permits: Arc<tokio::sync::Semaphore>,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
struct DfsRdmaSlot {
    connection: Arc<tokio::sync::Mutex<Option<Arc<DfsRdmaSession>>>>,
    touched: std::time::Instant,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
struct DfsRdmaSession {
    endpoint: Arc<tokio::sync::Mutex<RdmaEndpoint>>,
    poisoned: Arc<AtomicBool>,
    session_id: u64,
    caller_epoch: u64,
    client: DfsChunksClient<Channel>,
    timeout: Duration,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
struct DfsRdmaLease {
    session: Arc<DfsRdmaSession>,
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
    async fn acquire(
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
            let (endpoint, info, permit) = tokio::task::spawn_blocking(move || {
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
            let (endpoint, permit) = tokio::task::spawn_blocking(move || {
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
fn dfs_rdma_error(error: afs_transport::rdma::RdmaError) -> afs_error::Error {
    afs_error::Error::coded(afs_error::NODE_TRANSFER_UNAVAILABLE, error.to_string())
}
#[cfg(all(feature = "dfs", feature = "rdma"))]
fn dfs_rdma_join_error(error: tokio::task::JoinError) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string())
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
struct DfsRdmaRemoteClose {
    client: DfsChunksClient<Channel>,
    session_id: u64,
    caller_epoch: u64,
    timeout: Duration,
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

/// Node-owned connection resources shared by file reads, owner forwarding and
/// replica commands. Epoch changes invalidate cached channels; grant/handle
/// validity remains with the business operation, never with this pool.
type PeerChannelKey = (String, u64, String, bool);

pub struct PeerConnectionPool {
    #[cfg(feature = "dfs")]
    read_work_metrics: Option<crate::node::read_work::ReadWorkMetrics>,
    config: afs_transport::GrpcConfig,
    security: afs_transport::SecurityManager,
    capacity: usize,
    state: tokio::sync::Mutex<PeerPoolState>,
    #[cfg(feature = "dfs")]
    stream_work: Arc<PeerStreamWorkPool>,
}

#[derive(Default)]
struct PeerPoolState {
    channels: std::collections::HashMap<PeerChannelKey, CachedPeerChannel>,
    // A channel eviction does not erase the highest authority generation seen.
    high_water_epochs: std::collections::HashMap<String, u64>,
}

struct CachedPeerChannel {
    channel: Channel,
    touched: std::time::Instant,
}

impl PeerConnectionPool {
    pub fn new(
        config: afs_transport::GrpcConfig,
        tls: afs_transport::TlsConfig,
        capacity: usize,
    ) -> afs_error::Result<Self> {
        if capacity == 0 {
            return Err(afs_error::Error::coded(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "peer pool capacity must be positive",
            ));
        }
        let security = afs_transport::SecurityManager::new(tls).map_err(|error| {
            afs_error::Error::coded(afs_error::CLIENT_ARGUMENT_INVALID, error.to_string())
        })?;
        Ok(Self {
            config,
            security,
            capacity,
            state: tokio::sync::Mutex::new(PeerPoolState::default()),
            #[cfg(feature = "dfs")]
            stream_work: Arc::new(PeerStreamWorkPool::default()),
            #[cfg(feature = "dfs")]
            read_work_metrics: None,
        })
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn with_read_work_metrics(
        mut self,
        metrics: crate::node::read_work::ReadWorkMetrics,
    ) -> Self {
        self.read_work_metrics = Some(metrics);
        self
    }

    pub async fn channel(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
    ) -> afs_error::Result<Channel> {
        self.channel_with_timeout(node_id, node_epoch, endpoint, true, false)
            .await
    }

    /// OwnerFiles receive frames may carry large replies. Keep this profile
    /// separate from DFS channels while sharing epoch fencing and pool bounds.
    #[cfg(feature = "ownerfs")]
    pub async fn owner_files_channel(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
    ) -> afs_error::Result<Channel> {
        self.channel_with_timeout(node_id, node_epoch, endpoint, true, true)
            .await
    }

    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    pub async fn long_wait_channel(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
    ) -> afs_error::Result<Channel> {
        self.channel_with_timeout(node_id, node_epoch, endpoint, false, false)
            .await
    }

    async fn channel_with_timeout(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
        request_timeout: bool,
        owner_files: bool,
    ) -> afs_error::Result<Channel> {
        if node_id.is_empty() || node_epoch == 0 || endpoint.is_empty() {
            return Err(afs_error::Error::coded(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "peer identity or endpoint is incomplete",
            ));
        }
        let key = (
            node_id.to_owned(),
            node_epoch,
            endpoint.to_owned(),
            owner_files,
        );
        let mut state = self.state.lock().await;
        self.channel_locked(&mut state, &key, request_timeout)
    }

    // Both callers already hold the connection metadata lock. Stream admission
    // also holds its own short lock, so channel creation cannot race active work.
    fn channel_locked(
        &self,
        state: &mut PeerPoolState,
        key: &PeerChannelKey,
        request_timeout: bool,
    ) -> afs_error::Result<Channel> {
        let (node_id, node_epoch, endpoint, owner_files) = key;
        let node_epoch = *node_epoch;
        let owner_files = *owner_files;
        if state
            .high_water_epochs
            .get(node_id)
            .is_some_and(|epoch| node_epoch < *epoch)
        {
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "peer candidate has an older Node epoch than the connection pool",
            ));
        }
        if request_timeout && let Some(entry) = state.channels.get_mut(key) {
            entry.touched = std::time::Instant::now();
            return Ok(entry.channel.clone());
        }
        let endpoint = Endpoint::from_shared(endpoint.to_owned()).map_err(|error| {
            afs_error::Error::coded(afs_error::CLIENT_ARGUMENT_INVALID, error.to_string())
        })?;
        let endpoint = if request_timeout {
            self.config.configure_client(endpoint)
        } else {
            self.config.configure_long_wait_client(endpoint)
        };
        // This bounds each incoming frame, not the message/flow-control window.
        // Smaller frames remain valid; TLS, deadlines and message limits remain.
        // This only changes the client's receive cap. Frames still consume the
        // existing flow-control windows; message limits, budgets and deadlines
        // remain unchanged. Peers may continue to send smaller frames.
        let endpoint = endpoint.max_frame_size(if owner_files {
            256 * 1024
        } else {
            DFS_PEER_RECEIVE_FRAME_BYTES
        });
        let endpoint = self.security.configure_client(endpoint).map_err(|error| {
            afs_error::Error::coded(afs_error::CLIENT_ARGUMENT_INVALID, error.to_string())
        })?;
        let channel = endpoint.connect_lazy();
        if request_timeout {
            self.publish_channel(state, key.clone(), channel.clone());
        } else {
            self.publish_epoch(state, node_id, node_epoch);
        }
        Ok(channel)
    }

    #[cfg(feature = "dfs")]
    async fn acquire_stream_work(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
    ) -> afs_error::Result<Arc<PeerStreamActive>> {
        self.acquire_stream_work_mode(node_id, node_epoch, endpoint, false)
            .await?
            .ok_or_else(|| dfs_protocol_error("Peer foreground stream admission stopped"))
    }

    #[cfg(feature = "dfs")]
    async fn acquire_stream_work_mode(
        &self,
        node_id: &str,
        node_epoch: u64,
        endpoint: &str,
        background: bool,
    ) -> afs_error::Result<Option<Arc<PeerStreamActive>>> {
        let _waiting = if background {
            None
        } else {
            Some(PeerStreamWaiter::new(self.stream_work.clone())?)
        };
        if node_id.is_empty()
            || node_epoch == 0
            || endpoint.is_empty()
            || node_id.len().saturating_add(endpoint.len()) > STREAM_KEY_BYTES
        {
            return Err(dfs_protocol_error(
                "Peer stream connection identity exceeds its bound",
            ));
        }
        let key = (node_id.to_owned(), node_epoch, endpoint.to_owned(), false);
        if key.0.capacity().saturating_add(key.2.capacity()) > STREAM_KEY_BYTES {
            return Err(dfs_protocol_error(
                "Peer stream connection key allocation exceeds its bound",
            ));
        }
        loop {
            // Register before checking admission, so the last drop cannot be lost.
            let changed = self.stream_work.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            // Fence and select under the connection metadata lock. Never ask
            // the LRU for a replacement while another Channel is active.
            let mut channels = if background {
                match self.state.try_lock() {
                    Ok(channels) => channels,
                    Err(_) => return Ok(None),
                }
            } else {
                self.state.lock().await
            };
            if channels
                .high_water_epochs
                .get(node_id)
                .is_some_and(|epoch| node_epoch < *epoch)
            {
                return Err(afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                    "peer candidate has an older Node epoch than the connection pool",
                ));
            }
            {
                let mut state = if background {
                    match self.stream_work.state.try_lock() {
                        Ok(state) => state,
                        Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
                        Err(std::sync::TryLockError::Poisoned(_)) => {
                            return Err(dfs_protocol_error("Peer stream admission lock poisoned"));
                        }
                    }
                } else {
                    self.stream_work
                        .state
                        .lock()
                        .map_err(|_| dfs_protocol_error("Peer stream admission lock poisoned"))?
                };
                if background && state.foreground_waiters != 0 {
                    return Ok(None);
                }
                if state.active == 0 {
                    let candidate = self.channel_locked(&mut channels, &key, true)?;
                    let mut blocks = Vec::with_capacity(2);
                    for _ in 0..2 {
                        blocks.push(PeerStreamBlock::new(self.read_work_metrics.as_ref())?);
                    }
                    state.session = Some(Arc::new(PeerStreamSession {
                        key: key.clone(),
                        channel: candidate,
                        blocks: std::sync::Mutex::new(blocks),
                        available: tokio::sync::Notify::new(),
                        controls: Arc::new(tokio::sync::Semaphore::new(STREAM_FRAME_BACKING_BYTES)),
                        _work: std::sync::Mutex::new(
                            self.read_work_metrics
                                .as_ref()
                                .map(|metrics| metrics.peer_stream_pipeline(0)),
                        ),
                    }));
                }
                if let Some(session) = &state.session
                    && state.active < 2
                    && session.key == key
                {
                    let session = session.clone();
                    state.active += 1;
                    return Ok(Some(Arc::new(PeerStreamActive {
                        pool: self.stream_work.clone(),
                        session,
                    })));
                }
            }
            drop(channels);
            if background {
                return Ok(None);
            }
            changed.await;
        }
    }

    fn publish_epoch(&self, state: &mut PeerPoolState, node_id: &str, node_epoch: u64) {
        state
            .high_water_epochs
            .insert(node_id.to_owned(), node_epoch);
        state
            .channels
            .retain(|(id, epoch, _, _), _| id != node_id || *epoch == node_epoch);
    }

    fn publish_channel(&self, state: &mut PeerPoolState, key: PeerChannelKey, channel: Channel) {
        let (node_id, node_epoch, _, _) = &key;
        // No await occurs from the epoch check through publication. Lazy
        // connection completion cannot reinsert a stale channel after an epoch
        // advance; both the channel and its high-water mark publish under lock.
        self.publish_epoch(state, node_id, *node_epoch);
        let channels = &mut state.channels;
        if channels.len() >= self.capacity
            && let Some(oldest) = channels
                .iter()
                .min_by_key(|(_, value)| value.touched)
                .map(|(key, _)| key.clone())
        {
            channels.remove(&oldest);
        }
        channels.insert(
            key,
            CachedPeerChannel {
                channel,
                touched: std::time::Instant::now(),
            },
        );
    }
}

#[cfg(feature = "dfs")]
pub struct GrpcChunkTransfer {
    peers: Arc<PeerConnectionPool>,
    runtime: PeerRuntime,
    timeout: Duration,
    next_read: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "dfs")]
impl GrpcChunkTransfer {
    pub fn new(peers: Arc<PeerConnectionPool>, timeout: Duration) -> afs_error::Result<Self> {
        Ok(Self {
            peers,
            runtime: PeerRuntime::current_or_new().map_err(|error| error.0)?,
            timeout,
            next_read: std::sync::atomic::AtomicU64::new(1),
        })
    }
}

#[cfg(feature = "dfs")]
impl GrpcChunkTransfer {
    fn read_ranges_impl<'a>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: Option<&'a ChunkObject>,
        out: &'a mut [u8],
        proof_budget: Option<Arc<OutboardBudget>>,
    ) -> afs_error::Result<Option<VerifiedPeerChunk<'a>>> {
        match chunk {
            Some(chunk) => crate::node::dfs_read::validate_verified_peer_object_request(
                batch,
                chunk,
                out.len(),
            )?,
            None => batch.validate(out.len())?,
        }
        let first = &batch.operations[0].source;
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &first.location
        else {
            return Err(dfs_protocol_error("peer read candidate is not a Node copy"));
        };
        let sequence = self
            .next_read
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let request = DfsReadRangesRequest {
            include_blake3_outboard: chunk.is_some(),
            read_id: format!(
                "{}-{}-{sequence}",
                first.read_grant.caller_node_id, first.read_grant.caller_node_epoch
            ),
            attempt_id: format!("{node_id}-{node_epoch}-{sequence}"),
            file_version_id: batch.file_version_id.0.clone(),
            layout_root_id: batch.layout_root_id.0.clone(),
            operations: batch
                .operations
                .iter()
                .enumerate()
                .map(|(index, item)| DfsChunkReadOp {
                    operation_index: index as u32,
                    chunk_id: item.op.chunk_id.0.clone(),
                    chunk_offset: item.op.chunk_offset,
                    length: item.op.length,
                    destination_offset: item.op.output_offset as u64,
                    source_copy_id: item.source.copy_id.0.clone(),
                    grant: Some(wire_dfs_read_grant(&item.source.read_grant)),
                })
                .collect(),
        };
        self.runtime.block_on(async {
            // One deadline covers connection, response headers and every frame.
            tokio::time::timeout(self.timeout, async {
                let channel = self
                    .peers
                    .channel(
                        node_id,
                        *node_epoch,
                        first
                            .node_data_endpoint()
                            .ok_or_else(|| dfs_protocol_error("peer source has no endpoint"))?,
                    )
                    .await?;
                let mut client = DfsChunksClient::new(channel)
                    .max_encoding_message_size(self.peers.config.max_encoding_message_bytes)
                    .max_decoding_message_size(self.peers.config.max_decoding_message_bytes);
                let mut rpc_request = request_with_current_context(request.clone());
                rpc_request.set_timeout(self.timeout);
                if let Some(metrics) = &self.peers.read_work_metrics {
                    metrics.read_rpc("peer_read_ranges");
                }
                let response = client
                    .read_ranges(rpc_request)
                    .await
                    .map_err(afs_transport::grpc::error_status::status_to_error)?;
                consume_peer_read_stream(
                    &request,
                    chunk,
                    out,
                    response.into_inner(),
                    self.peers.read_work_metrics.as_ref(),
                    proof_budget,
                )
                .await
            })
            .await
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "DFS peer read total deadline exceeded",
                )
            })?
        })
    }
}

#[cfg(feature = "dfs")]
const STREAM_MESSAGE_BYTES: usize = 128 * 1024;
#[cfg(feature = "dfs")]
const STREAM_DATA_BYTES: usize = 64 * 1024;
#[cfg(feature = "dfs")]
const STREAM_FRAME_BACKING_BYTES: usize = 256 * 1024;
#[cfg(feature = "dfs")]
const STREAM_KEY_BYTES: usize = 8 * 1024;
#[cfg(feature = "dfs")]
const STREAM_REQUEST_METADATA_BYTES: usize = 32 * 1024;

#[cfg(feature = "dfs")]
struct PeerStreamBlock {
    // Drop the actual allocation before retiring its accounting lease.
    bytes: Vec<u8>,
    offset: usize,
    // Two small accounting owners are allocated once per shared session,
    // then move with the same blocks; handing off a block allocates nothing.
    _work: Option<Box<crate::node::read_work::PeerStreamFrameWork>>,
}

#[cfg(feature = "dfs")]
impl PeerStreamBlock {
    fn new(metrics: Option<&ReadWorkMetrics>) -> afs_error::Result<Self> {
        let bytes = Vec::with_capacity(STREAM_FRAME_BACKING_BYTES);
        if bytes.capacity() > STREAM_FRAME_BACKING_BYTES {
            return Err(dfs_protocol_error(
                "Peer stream block allocation exceeds budget",
            ));
        }
        let work = metrics.map(|metrics| {
            metrics.peer_read_event("stream_block_alloc");
            Box::new(metrics.peer_stream_frame(bytes.capacity()))
        });
        Ok(Self {
            bytes,
            offset: 0,
            _work: work,
        })
    }
}

// One Node owns the original two payload allocations and control allowance.
// Active operations pin one exact Channel, even if the connection LRU evicts it.
// Thus two streams share one <=2MiB connection window; a different connection
// waits. Two bounded decoders add <=256KiB: 2MiB + 512KiB + 256KiB + 256KiB
// plus <=256KiB bounded request/key/fixed metadata remains below the original
// 4MiB transport envelope (private output/proof separate). A key owns <=8KiB;
// each operation retains two <=32KiB request representations. The cache/pool
// guards admit only two operations and one shared session.
#[cfg(feature = "dfs")]
#[derive(Default)]
struct PeerStreamWorkPool {
    state: std::sync::Mutex<PeerStreamWorkState>,
    changed: tokio::sync::Notify,
}
#[cfg(feature = "dfs")]
#[derive(Default)]
struct PeerStreamWorkState {
    active: usize,
    foreground_waiters: usize,
    session: Option<Arc<PeerStreamSession>>,
}
#[cfg(feature = "dfs")]
struct PeerStreamWaiter {
    pool: Arc<PeerStreamWorkPool>,
}
#[cfg(feature = "dfs")]
impl PeerStreamWaiter {
    fn new(pool: Arc<PeerStreamWorkPool>) -> afs_error::Result<Self> {
        pool.state
            .lock()
            .map_err(|_| dfs_protocol_error("Peer stream wait lock poisoned"))?
            .foreground_waiters += 1;
        Ok(Self { pool })
    }
}
#[cfg(feature = "dfs")]
impl Drop for PeerStreamWaiter {
    fn drop(&mut self) {
        if let Ok(mut state) = self.pool.state.lock() {
            state.foreground_waiters = state.foreground_waiters.saturating_sub(1);
        }
        self.pool.changed.notify_waiters();
    }
}

#[cfg(feature = "dfs")]
struct PeerStreamSession {
    key: PeerChannelKey,
    channel: Channel,
    blocks: std::sync::Mutex<Vec<PeerStreamBlock>>,
    available: tokio::sync::Notify,
    controls: Arc<tokio::sync::Semaphore>,
    // Fields drop in order: all payload owners precede this shared observation.
    _work: std::sync::Mutex<Option<crate::node::read_work::PeerCacheStreamWork>>,
}
#[cfg(feature = "dfs")]
struct PeerStreamActive {
    pool: Arc<PeerStreamWorkPool>,
    session: Arc<PeerStreamSession>,
}
#[cfg(feature = "dfs")]
impl PeerStreamActive {
    async fn block(self: &Arc<Self>) -> afs_error::Result<PeerStreamBlockLease> {
        loop {
            let available = self.session.available.notified();
            tokio::pin!(available);
            available.as_mut().enable();
            if let Some(block) = self
                .session
                .blocks
                .lock()
                .map_err(|_| dfs_protocol_error("Peer stream block lock poisoned"))?
                .pop()
            {
                return Ok(PeerStreamBlockLease {
                    block,
                    active: self.clone(),
                });
            }
            available.await;
        }
    }
}
#[cfg(feature = "dfs")]
impl Drop for PeerStreamActive {
    fn drop(&mut self) {
        let retired = {
            let mut state = self
                .pool
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.active == 1 {
                // Retire the actual allocations and observation before publishing
                // zero. Waking another admission before our Arc fields drop could
                // otherwise overlap the old two blocks with two new allocations.
                self.session
                    .blocks
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clear();
                drop(
                    self.session
                        ._work
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .take(),
                );
            }
            state.active = state.active.saturating_sub(1);
            if state.active == 0 {
                state.session.take()
            } else {
                None
            }
        };
        // Last operation/producer/frame drops before its ticket. Idle blocks are
        // destroyed here, never kept as a permanent 512KiB Node allocation.
        drop(retired);
        self.pool.changed.notify_waiters();
    }
}
#[cfg(feature = "dfs")]
struct PeerStreamBlockLease {
    block: PeerStreamBlock,
    active: Arc<PeerStreamActive>,
}
#[cfg(feature = "dfs")]
impl std::ops::Deref for PeerStreamBlockLease {
    type Target = PeerStreamBlock;
    fn deref(&self) -> &Self::Target {
        &self.block
    }
}
#[cfg(feature = "dfs")]
impl std::ops::DerefMut for PeerStreamBlockLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.block
    }
}
#[cfg(feature = "dfs")]
impl Drop for PeerStreamBlockLease {
    fn drop(&mut self) {
        // Move the existing allocation/lease back without allocating a new body,
        // including on producer cancellation, failed send and consumer errors.
        let mut block = PeerStreamBlock {
            bytes: std::mem::take(&mut self.block.bytes),
            offset: 0,
            _work: self.block._work.take(),
        };
        block.bytes.clear();
        self.active
            .session
            .blocks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(block);
        self.active.session.available.notify_waiters();
    }
}

#[cfg(feature = "dfs")]
struct OwnedPeerStreamControl {
    // Typed, budgeted metadata only; never a decoded opaque Bytes backing.
    outboard_builder: Option<OutboardBuilder>,
    frame: afs_protocol::node_data::DfsReadRangesFrame,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

#[cfg(feature = "dfs")]
enum OwnedPeerStreamMessage {
    Control(Box<OwnedPeerStreamControl>),
    Block(PeerStreamBlockLease),
}

#[cfg(feature = "dfs")]
fn peer_stream_control_capacity(
    frame: &mut afs_protocol::node_data::DfsReadRangesFrame,
) -> afs_error::Result<usize> {
    let backing = match &mut frame.body {
        Some(dfs_read_ranges_frame::Body::Header(header)) => {
            if !header.blake3_outboard.is_empty() {
                return Err(dfs_protocol_error(
                    "decoded outboard cannot enter the control queue",
                ));
            }
            header.read_id.capacity()
                + header.attempt_id.capacity()
                + header.chunk_id.capacity()
                + header.source_copy_id.capacity()
        }
        Some(dfs_read_ranges_frame::Body::Completion(completion)) => {
            completion.range_checksum.capacity()
                + completion.read_id.capacity()
                + completion.attempt_id.capacity()
                + completion.source_copy_id.capacity()
        }
        Some(dfs_read_ranges_frame::Body::Data(data)) if data.is_empty() => {
            // Empty Data still has protocol order semantics. Retire any decoder
            // backing before it crosses the control queue.
            *data = prost::bytes::Bytes::new();
            0
        }
        Some(dfs_read_ranges_frame::Body::Data(_)) => {
            return Err(dfs_protocol_error(
                "Peer data cannot enter the control queue",
            ));
        }
        None => 0,
    };
    if backing > STREAM_FRAME_BACKING_BYTES {
        return Err(dfs_protocol_error(
            "Peer stream control exceeds backing budget",
        ));
    }
    Ok(backing)
}

// Metadata is copied once on B, after admission to the Node's shared proof
// ledger. Decoded Bytes belong to the existing codec reservation and are dropped
// before any control frame is queued; no decoded backing escapes as proof state.
#[cfg(feature = "dfs")]
fn peer_outboard_builder(
    expected: &ChunkObject,
    encoded: prost::bytes::Bytes,
    budget: Option<Arc<OutboardBudget>>,
    metrics: Option<&ReadWorkMetrics>,
) -> afs_error::Result<Option<OutboardBuilder>> {
    const MAX_ENCODED_OUTBOARD_BYTES: usize = 8192;
    if encoded.len() > MAX_ENCODED_OUTBOARD_BYTES {
        return Err(dfs_protocol_error("Peer outboard exceeds 8192-byte bound"));
    }
    let Some(budget) = budget else {
        return if encoded.is_empty() {
            Ok(None)
        } else {
            Err(dfs_protocol_error(
                "Peer outboard has no shared proof budget",
            ))
        };
    };
    if encoded.is_empty() {
        return Ok(Some(OutboardBuilder::new(expected.length, budget)));
    }
    // Shape/binding are checked even if the optional metadata pool is full.
    // Unused CVs are never advertised as verified; the original independent
    // complete-body root remains authoritative on this safe fallback path.
    ChunkOutboard::validate_encoded_shape(expected, &encoded)?;
    let charge = ChunkOutboard::resident_charge(encoded.len())?;
    let Some(mut lease) = budget.try_reserve(charge) else {
        return Ok(None);
    };
    let mut owned = Vec::new();
    if owned.try_reserve_exact(encoded.len()).is_err()
        || lease
            .retain_capacity(ChunkOutboard::resident_charge(owned.capacity())?)
            .is_err()
    {
        return Ok(None);
    }
    owned.extend_from_slice(&encoded);
    if let Some(metrics) = metrics {
        metrics.copy("peer_outboard_metadata", encoded.len() as u64);
    }
    drop(encoded);
    let proof = ChunkOutboard::from_encoded(expected, owned, lease)?;
    Ok(Some(OutboardBuilder::validating(
        expected.clone(),
        proof,
        budget,
    )))
}

/// The header fixes which local worker authenticates the complete object.
/// The producer always owns transport order/length/EOF; receipts never own bodies.
#[cfg(feature = "dfs")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeerRootMode {
    Producer,
    ConsumerTree,
}

#[cfg(feature = "dfs")]
impl PeerRootMode {
    /// Fix one complete-root owner before any body byte is accepted. A locally
    /// built charged tree is sufficient; optional peer CVs are not required.
    fn select(builder: &mut Option<OutboardBuilder>) -> Self {
        if builder.as_ref().is_some_and(OutboardBuilder::has_tree) {
            Self::ConsumerTree
        } else {
            // A root-only builder would hash the same body as the plain root
            // owner, but cannot install an outboard. Retire it before input.
            builder.take();
            Self::Producer
        }
    }
}

#[cfg(feature = "dfs")]
#[derive(Clone, Copy, Debug)]
enum PeerRootDigest {
    Producer([u8; 32]),
    ConsumerTree,
}
#[cfg(feature = "dfs")]
struct PeerRootProducer {
    chunk: Option<ChunkObject>,
    proof_budget: Option<Arc<OutboardBudget>>,
    outboard_builder: Option<OutboardBuilder>,
    outboard_header_seen: bool,
    root_mode: PeerRootMode,
    hasher: blake3::Hasher,
    length: u64,
    expected_length: u64,
    deadline: std::time::Instant,
    binding: crate::node::peer_cache::PeerStreamBinding,
    diagnostic: Option<ReadDiagnosticProducer>,
}

#[cfg(feature = "dfs")]
struct PeerRootReceipt {
    root_intervals: Option<ReadDiagnosticIntervals>,
    length: u64,
    digest: PeerRootDigest,
    binding: crate::node::peer_cache::PeerStreamBinding,
}

#[cfg(feature = "dfs")]
impl PeerRootProducer {
    fn new(
        expected_length: u64,
        binding: crate::node::peer_cache::PeerStreamBinding,
        diagnostic: Option<ReadDiagnosticProducer>,
        deadline: std::time::Instant,
    ) -> Self {
        Self {
            chunk: None,
            proof_budget: None,
            outboard_builder: None,
            outboard_header_seen: false,
            root_mode: PeerRootMode::Producer,
            hasher: blake3::Hasher::new(),
            length: 0,
            expected_length,
            binding,
            diagnostic,
            deadline,
        }
    }

    fn with_outboard_context(mut self, chunk: ChunkObject, budget: Arc<OutboardBudget>) -> Self {
        self.chunk = Some(chunk);
        self.proof_budget = Some(budget);
        self
    }

    fn accept_outboard_header(
        &mut self,
        header: &mut afs_protocol::node_data::DfsReadRangesHeader,
        metrics: Option<&ReadWorkMetrics>,
    ) -> afs_error::Result<()> {
        if self.outboard_header_seen || self.length != 0 {
            return Err(dfs_protocol_error("Peer outboard header order differs"));
        }
        self.outboard_header_seen = true;
        let encoded = std::mem::take(&mut header.blake3_outboard);
        if let Some(chunk) = &self.chunk {
            if header.chunk_id != chunk.id.0
                || header.chunk_offset != 0
                || header.length != chunk.length
            {
                return Err(dfs_protocol_error(
                    "Peer outboard header differs from trusted object",
                ));
            }
            self.outboard_builder =
                peer_outboard_builder(chunk, encoded, self.proof_budget.clone(), metrics)?;
            self.root_mode = PeerRootMode::select(&mut self.outboard_builder);
            if self.root_mode == PeerRootMode::Producer
                && let Some(metrics) = metrics
            {
                metrics.peer_read_event("root_fallback");
            }
        } else if !encoded.is_empty() {
            return Err(dfs_protocol_error("Peer outboard lacks a trusted object"));
        }
        Ok(())
    }

    fn update(
        &mut self,
        offset: usize,
        bytes: &[u8],
        metrics: Option<&ReadWorkMetrics>,
    ) -> afs_error::Result<()> {
        peer_stream_deadline(self.deadline)?;
        let end = self
            .length
            .checked_add(bytes.len() as u64)
            .filter(|end| *end <= self.expected_length)
            .ok_or_else(|| {
                dfs_protocol_error("Peer producer root exceeds trusted object length")
            })?;
        if offset as u64 != self.length {
            return Err(dfs_protocol_error("Peer producer root block order differs"));
        }
        if self.root_mode == PeerRootMode::Producer {
            match &self.diagnostic {
                Some(diagnostic) => diagnostic.measure(bytes.len() as u64, || {
                    self.hasher.update(bytes);
                }),
                None => {
                    self.hasher.update(bytes);
                }
            }
            if let Some(metrics) = metrics {
                metrics.hash("root_in_receive", bytes.len() as u64);
            }
        }
        self.length = end;
        // Synchronous hashing is not preempted by timeout_at. Count actual work
        // above even when it consumed the remaining original shared deadline.
        peer_stream_deadline(self.deadline)
    }

    // Called only after actual transport EOF and final block handoff. The
    // consumer-tree variant attests transport completion, never a computed root.
    // It must be joined and combined with the consumer's independently computed root.
    fn finish_after_eof(self) -> afs_error::Result<PeerRootReceipt> {
        peer_stream_deadline(self.deadline)?;
        if self.length != self.expected_length {
            return Err(dfs_protocol_error("Peer producer root is truncated"));
        }
        let digest = match self.root_mode {
            PeerRootMode::Producer => PeerRootDigest::Producer(match &self.diagnostic {
                Some(diagnostic) => diagnostic.measure(0, || *self.hasher.finalize().as_bytes()),
                None => *self.hasher.finalize().as_bytes(),
            }),
            PeerRootMode::ConsumerTree => PeerRootDigest::ConsumerTree,
        };
        if self.outboard_builder.is_some() {
            return Err(dfs_protocol_error(
                "Peer producer retained outboard work instead of handing it to the consumer",
            ));
        }
        peer_stream_deadline(self.deadline)?;
        Ok(PeerRootReceipt {
            root_intervals: self
                .diagnostic
                .as_ref()
                .map(ReadDiagnosticProducer::intervals),
            length: self.length,
            digest,
            binding: self.binding,
        })
    }
}

#[cfg(feature = "dfs")]
async fn send_peer_stream_block(
    current: &mut Option<PeerStreamBlockLease>,
    sender: &tokio::sync::mpsc::Sender<afs_error::Result<OwnedPeerStreamMessage>>,
    metrics: Option<&ReadWorkMetrics>,
    root: &mut PeerRootProducer,
) -> afs_error::Result<()> {
    if let Some(block) = current.take() {
        if block.bytes.is_empty() {
            return Err(dfs_protocol_error("Peer stream handed off an empty block"));
        }
        // Check transport order/length before handoff. Hash here only when the
        // header selected the legacy producer-root fallback.
        root.update(block.offset, &block.bytes, metrics)?;
        let handoff_wait = metrics.and_then(|metrics| {
            metrics.coarse_diagnostic("peer_block_handoff_wait", block.bytes.len() as u64)
        });
        sender
            .send(Ok(OwnedPeerStreamMessage::Block(block)))
            .await
            .map_err(|_| dfs_protocol_error("Peer stream consumer stopped"))?;
        drop(handoff_wait);
        if let Some(metrics) = metrics {
            metrics.peer_read_event("stream_block_handoff");
        }
    }
    Ok(())
}

#[cfg(feature = "dfs")]
async fn send_peer_stream_control(
    mut frame: afs_protocol::node_data::DfsReadRangesFrame,
    current: &mut Option<PeerStreamBlockLease>,
    sender: &tokio::sync::mpsc::Sender<afs_error::Result<OwnedPeerStreamMessage>>,
    controls: &Arc<tokio::sync::Semaphore>,
    metrics: Option<&ReadWorkMetrics>,
    root: &mut PeerRootProducer,
) -> afs_error::Result<()> {
    send_peer_stream_block(current, sender, metrics, root).await?;
    let backing = peer_stream_control_capacity(&mut frame)?;
    let permit = controls
        .clone()
        .acquire_many_owned(
            u32::try_from(backing)
                .map_err(|_| dfs_protocol_error("Peer control capacity conversion overflow"))?,
        )
        .await
        .map_err(|_| dfs_protocol_error("Peer stream control admission stopped"))?;
    let outboard_builder = if matches!(&frame.body, Some(dfs_read_ranges_frame::Body::Header(_))) {
        root.outboard_builder.take()
    } else {
        None
    };
    sender
        .send(Ok(OwnedPeerStreamMessage::Control(Box::new(
            OwnedPeerStreamControl {
                outboard_builder,
                frame,
                _permit: permit,
            },
        ))))
        .await
        .map_err(|_| dfs_protocol_error("Peer stream consumer stopped"))
}

#[cfg(feature = "dfs")]
fn peer_stream_deadline(deadline: std::time::Instant) -> afs_error::Result<()> {
    if std::time::Instant::now() >= deadline {
        return Err(afs_error::Error::coded(
            afs_error::CLIENT_DEADLINE_EXCEEDED,
            "Peer stream disk total deadline exceeded",
        ));
    }
    Ok(())
}

#[cfg(feature = "dfs")]
enum PeerSinkInput<'a> {
    Frame(afs_protocol::node_data::DfsReadRangesFrame),
    Block { offset: usize, bytes: &'a [u8] },
}

#[cfg(feature = "dfs")]
#[derive(Clone, Copy, Default)]
struct PeerConsumerObservation<'a> {
    metrics: Option<&'a ReadWorkMetrics>,
    diagnostic: Option<&'a ReadDiagnosticConsumer>,
}

#[cfg(feature = "dfs")]
impl PeerConsumerObservation<'_> {
    fn measure<T>(
        self,
        stage: ReadDiagnosticConsumerStage,
        bytes: u64,
        operation: impl FnOnce() -> T,
    ) -> T {
        match self.diagnostic {
            Some(diagnostic) => diagnostic.measure(stage, bytes, operation),
            None => operation(),
        }
    }
}

#[cfg(feature = "dfs")]
fn consume_peer_frame_into_sink(
    decoder: &mut ReadFrameDecoder<'_>,
    frame: afs_protocol::node_data::DfsReadRangesFrame,
    requested_offset: u64,
    out: &mut [u8],
    sink: &mut PeerStreamSink<'_>,
    deadline: std::time::Instant,
    observation: PeerConsumerObservation<'_>,
) -> afs_error::Result<()> {
    consume_peer_input_into_sink(
        decoder,
        PeerSinkInput::Frame(frame),
        requested_offset,
        out,
        sink,
        deadline,
        observation,
    )
}

#[cfg(feature = "dfs")]
fn consume_peer_input_into_sink(
    decoder: &mut ReadFrameDecoder<'_>,
    input: PeerSinkInput<'_>,
    requested_offset: u64,
    out: &mut [u8],
    sink: &mut PeerStreamSink<'_>,
    deadline: std::time::Instant,
    observation: PeerConsumerObservation<'_>,
) -> afs_error::Result<()> {
    let metrics = observation.metrics;
    peer_stream_deadline(deadline)?;
    let binding = decoder
        .stream_binding
        .as_ref()
        .ok_or_else(|| dfs_protocol_error("Peer stream output was not bound before decoding"))?;
    if !sink.accepts_binding(binding) || !binding.matches_output(requested_offset, out) {
        return Err(dfs_protocol_error(
            "Peer stream changed sink or private output during decoding",
        ));
    }
    match &input {
        PeerSinkInput::Frame(_) => {}
        PeerSinkInput::Block { offset, bytes } => {
            if bytes.is_empty()
                || bytes.len() > STREAM_FRAME_BACKING_BYTES
                || decoder
                    .active
                    .as_ref()
                    .is_none_or(|active| active.cursor != *offset)
            {
                return Err(dfs_protocol_error(
                    "Peer stream block offset/order/length mismatch",
                ));
            }
        }
    }
    let block_write = matches!(&input, PeerSinkInput::Block { .. });
    let full_length = decoder
        .verified_object
        .as_ref()
        .ok_or_else(|| dfs_protocol_error("Peer disk stream lacks full-object decoder"))?
        .expected_length;
    let write = |position: usize, bytes: &[u8]| {
        peer_stream_deadline(deadline)?;
        if block_write && let Some(metrics) = metrics {
            metrics.peer_read_event("stream_block_write");
        }
        observation.measure(
            ReadDiagnosticConsumerStage::SinkWrite,
            bytes.len() as u64,
            || sink.write(position as u64, bytes),
        )?;
        peer_stream_deadline(deadline)?;
        let from = (position as u64).max(requested_offset);
        let to = (position as u64 + bytes.len() as u64).min(requested_offset + out.len() as u64);
        if from < to {
            let length = (to - from) as usize;
            observation.measure(
                ReadDiagnosticConsumerStage::RangeExtract,
                length as u64,
                || {
                    out[(from - requested_offset) as usize..(to - requested_offset) as usize]
                        .copy_from_slice(
                            &bytes[(from - position as u64) as usize
                                ..(to - position as u64) as usize],
                        );
                },
            );
            if let Some(metrics) = metrics {
                metrics.copy("fill_to_output", length as u64);
            }
        }
        Ok(())
    };
    match input {
        PeerSinkInput::Frame(frame) => {
            decoder.accept_into(frame, full_length, write, observation)?
        }
        PeerSinkInput::Block { bytes, .. } => decoder.accept_data(bytes, write, observation)?,
    }
    peer_stream_deadline(deadline)
}

#[cfg(feature = "dfs")]
impl GrpcChunkTransfer {
    fn stream_sink_supported(&self) -> bool {
        let flavor = match &self.runtime {
            PeerRuntime::Existing(handle) => handle.runtime_flavor(),
            PeerRuntime::Owned(runtime) => runtime.handle().runtime_flavor(),
        };
        flavor == tokio::runtime::RuntimeFlavor::MultiThread
            && self.peers.config.initial_connection_window_size <= 2 * 1024 * 1024
            && self.peers.config.initial_stream_window_size <= 2 * 1024 * 1024
    }

    fn read_object_to_sink<'output>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &ChunkObject,
        requested_offset: u64,
        out: &'output mut [u8],
        sink: &mut PeerStreamSink<'_>,
        prefetch: Option<&crate::node::dfs_read::PrefetchControl>,
    ) -> afs_error::Result<Option<CompletedPeerDiskRead<'output>>> {
        if !self.stream_sink_supported()
            || out
                .len()
                .checked_add(4 * 1024 * 1024)
                .is_none_or(|bytes| bytes > crate::node::dfs_read::MAX_DFS_READ_BYTES as usize)
        {
            return Ok(None);
        }
        crate::node::dfs_read::validate_verified_peer_object_request(
            batch,
            chunk,
            chunk.length as usize,
        )?;
        if requested_offset
            .checked_add(out.len() as u64)
            .is_none_or(|end| end > chunk.length)
        {
            return Err(dfs_protocol_error(
                "Peer stream requested output exceeds object",
            ));
        }
        let source = &batch.operations[0].source;
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &source.location
        else {
            return Err(dfs_protocol_error("Peer stream source is not a Node"));
        };
        let endpoint = source
            .node_data_endpoint()
            .ok_or_else(|| dfs_protocol_error("Peer stream source endpoint is missing"))?;
        let grant = &source.read_grant;
        let metadata = [
            node_id.as_str(),
            endpoint,
            batch.file_version_id.0.as_str(),
            batch.layout_root_id.0.as_str(),
            chunk.id.0.as_str(),
            source.copy_id.0.as_str(),
            grant.namespace_id.0.as_str(),
            grant.file_version_id.0.as_str(),
            grant.layout_root_id.0.as_str(),
            grant.caller_node_id.as_str(),
            grant.caller_node_id.as_str(),
            grant.token.as_str(),
            node_id.as_str(),
        ]
        .iter()
        .fold(0usize, |total, value| total.saturating_add(value.len()));
        if metadata > STREAM_REQUEST_METADATA_BYTES / 2
            || node_id.len().saturating_add(endpoint.len()) > STREAM_KEY_BYTES
        {
            // Unsupported before cloning metadata or issuing an RPC; the caller
            // drains this sink and acquires the original exclusive legacy fill.
            return Ok(None);
        }
        let node_id = node_id.clone();
        let node_epoch = *node_epoch;
        let endpoint = endpoint.to_owned();
        let sequence = self
            .next_read
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let request = DfsReadRangesRequest {
            include_blake3_outboard: true,
            read_id: format!(
                "{}-{}-{sequence}",
                source.read_grant.caller_node_id, source.read_grant.caller_node_epoch
            ),
            attempt_id: format!("{node_id}-{node_epoch}-{sequence}"),
            file_version_id: batch.file_version_id.0.clone(),
            layout_root_id: batch.layout_root_id.0.clone(),
            operations: vec![DfsChunkReadOp {
                operation_index: 0,
                chunk_id: chunk.id.0.clone(),
                chunk_offset: 0,
                length: chunk.length,
                destination_offset: 0,
                source_copy_id: source.copy_id.0.clone(),
                grant: Some(wire_dfs_read_grant(&source.read_grant)),
            }],
        };
        let op = &request.operations[0];
        let request_capacity = request.read_id.capacity()
            + request.attempt_id.capacity()
            + request.file_version_id.capacity()
            + request.layout_root_id.capacity()
            + op.chunk_id.capacity()
            + op.source_copy_id.capacity()
            + op.grant.as_ref().map_or(0, |grant| {
                grant.namespace_id.capacity()
                    + grant.file_version_id.capacity()
                    + grant.layout_root_id.capacity()
                    + grant.caller_node_id.capacity()
                    + grant.token.capacity()
            });
        if request_capacity > STREAM_REQUEST_METADATA_BYTES {
            return Ok(None);
        }
        let handle = match &self.runtime {
            PeerRuntime::Existing(handle) => handle.clone(),
            PeerRuntime::Owned(runtime) => runtime.handle().clone(),
        };
        let peers = self.peers.clone();
        let timeout = self.timeout;
        let deadline = std::time::Instant::now() + timeout;
        let output_binding = sink.bind_output(requested_offset, out)?;
        // Capture tracing metadata on the caller before crossing the task boundary.
        let mut producer_request = request_with_current_context(request.clone());
        producer_request.set_timeout(timeout);
        // Acquire before spawning, and retain through producer join and all
        // queued/consumer blocks. Every block also pins this operation ticket.
        if let Some(control) = prefetch {
            control.check()?;
        }
        let active = self.runtime.block_on(async {
            let admit = async {
                tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
                    if prefetch.is_some() {
                        self.peers.acquire_stream_work_mode(&node_id, node_epoch, &endpoint, true).await
                    } else {
                        self.peers.acquire_stream_work(&node_id, node_epoch, &endpoint).await.map(Some)
                    }
                }).await.map_err(|_| afs_error::Error::coded(afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "Peer stream admission deadline exceeded"))?
            };
            if let Some(control) = prefetch {
                tokio::select! {
                    result = admit => result,
                    _ = control.cancelled() => Err(afs_error::Error::coded(afs_error::IO_UNAVAILABLE, "Peer prefetch cancelled before admission")),
                }
            } else { admit.await }
        })?;
        let Some(active) = active else {
            return Ok(None);
        };
        let controls = active.session.controls.clone();
        let producer_active = active.clone();
        if let Some(metrics) = &self.peers.read_work_metrics {
            metrics.peer_stream_private_output(out.len());
        }
        let (sender, mut receiver) =
            tokio::sync::mpsc::channel::<afs_error::Result<OwnedPeerStreamMessage>>(1);
        if let Some(metrics) = &self.peers.read_work_metrics {
            metrics.peer_read_event("stream_pipeline");
        }
        // Request and original payload owners are ready before this parent begins.
        // The journal stores metadata only and never extends a payload lifetime.
        let diagnostic = self
            .peers
            .read_work_metrics
            .as_ref()
            .and_then(|metrics| metrics.diagnostic(ReadDiagnosticStage::PeerObject));
        let consumer_diagnostic = diagnostic.as_ref().map(|span| span.token());
        let producer_diagnostic = consumer_diagnostic.clone();
        let expected_body_bytes = chunk.length;
        let producer_chunk = chunk.clone();
        let producer_proof_budget = sink.proof_budget();
        let producer_binding = output_binding.clone();
        let producer_control = prefetch.cloned();
        let producer = handle.spawn(async move {
            let receive = async {
                tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
                    if let Some(control) = &producer_control { control.check()?; }
                    let mut client = DfsChunksClient::new(producer_active.session.channel.clone())
                        .max_encoding_message_size(peers.config.max_encoding_message_bytes)
                        .max_decoding_message_size(
                            peers
                                .config
                                .max_decoding_message_bytes
                                .min(STREAM_MESSAGE_BYTES),
                        );
                    let request = producer_request;
                    if let Some(metrics) = &peers.read_work_metrics {
                        metrics.read_rpc("peer_read_ranges");
                    }
                    let mut stream = client
                        .read_ranges(request)
                        .await
                        .map_err(afs_transport::grpc::error_status::status_to_error)?
                        .into_inner();
                    let mut root = PeerRootProducer::new(
                        expected_body_bytes,
                        producer_binding,
                        producer_diagnostic.clone().map(ReadDiagnosticProducer::new),
                        deadline,
                    )
                    .with_outboard_context(producer_chunk, producer_proof_budget);
                    let mut current: Option<PeerStreamBlockLease> = None;
                    let mut received_bytes = 0usize;
                    while let Some(frame) = stream.next().await {
                        if let Some(control) = &producer_control { control.check()?; }
                        let frame =
                            frame.map_err(afs_transport::grpc::error_status::status_to_error)?;
                        if let Some(metrics) = &peers.read_work_metrics {
                            metrics.peer_read_event("stream_frame");
                        }
                        match frame.body {
                            Some(dfs_read_ranges_frame::Body::Header(mut header)) => {
                                root.accept_outboard_header(
                                    &mut header,
                                    peers.read_work_metrics.as_ref(),
                                )?;
                                send_peer_stream_control(
                                    afs_protocol::node_data::DfsReadRangesFrame {
                                        body: Some(dfs_read_ranges_frame::Body::Header(header)),
                                    },
                                    &mut current,
                                    &sender,
                                    &controls,
                                    peers.read_work_metrics.as_ref(),
                                    &mut root,
                                )
                                .await?;
                            }
                            Some(dfs_read_ranges_frame::Body::Data(data)) => {
                                if !root.outboard_header_seen {
                                    return Err(dfs_protocol_error(
                                        "Peer stream body precedes header",
                                    ));
                                }
                                if data.len() > STREAM_DATA_BYTES {
                                    return Err(dfs_protocol_error(
                                        "Peer streaming data frame exceeds 64KiB bound",
                                    ));
                                }
                                if data.is_empty() {
                                    send_peer_stream_control(
                                        afs_protocol::node_data::DfsReadRangesFrame {
                                            body: Some(dfs_read_ranges_frame::Body::Data(data)),
                                        },
                                        &mut current,
                                        &sender,
                                        &controls,
                                        peers.read_work_metrics.as_ref(),
                                        &mut root,
                                    )
                                    .await?;
                                    continue;
                                }
                                if let Some(diagnostic) = &producer_diagnostic {
                                    // First nonempty decoded body, before staging/copy/hash.
                                    diagnostic.first_data();
                                }
                                // The transport endpoint is decoded body completion,
                                // before frame-to-block copying, hashing or cache writes.
                                // EOF and trusted proof remain separate later events.
                                if let Some(diagnostic) = &producer_diagnostic
                                    && (received_bytes as u64).checked_add(data.len() as u64)
                                        == Some(expected_body_bytes)
                                {
                                    diagnostic.body_complete();
                                }
                                // Do not split a wire frame across owners. An irregular
                                // frame can flush a short block; its decoded backing is
                                // still charged to the decoder, never queued as payload.
                                if current.as_ref().is_some_and(|block| {
                                    block.bytes.len() + data.len() > STREAM_FRAME_BACKING_BYTES
                                }) {
                                    send_peer_stream_block(
                                        &mut current,
                                        &sender,
                                        peers.read_work_metrics.as_ref(),
                                        &mut root,
                                    )
                                    .await?;
                                }
                                if current.is_none() {
                                    let wait =
                                        peers.read_work_metrics.as_ref().and_then(|metrics| {
                                            metrics.coarse_diagnostic("peer_block_wait", 0)
                                        });
                                    let mut block = producer_active.block().await?;
                                    drop(wait);
                                    block.bytes.clear();
                                    block.offset = received_bytes;
                                    current = Some(block);
                                }
                                let block = current.as_mut().ok_or_else(|| {
                                    dfs_protocol_error("Peer stream block is missing")
                                })?;
                                {
                                    let _copy =
                                        peers.read_work_metrics.as_ref().and_then(|metrics| {
                                            metrics.coarse_diagnostic(
                                                "peer_frame_handoff_copy",
                                                data.len() as u64,
                                            )
                                        });
                                    block.bytes.extend_from_slice(&data);
                                }
                                received_bytes =
                                    received_bytes.checked_add(data.len()).ok_or_else(|| {
                                        dfs_protocol_error("Peer stream byte count overflow")
                                    })?;
                                if let Some(metrics) = &peers.read_work_metrics {
                                    metrics.copy("peer_frame_to_block", data.len() as u64);
                                }
                                // The original decoder Bytes never enters the handoff.
                                drop(data);
                                if block.bytes.len() == STREAM_FRAME_BACKING_BYTES {
                                    send_peer_stream_block(
                                        &mut current,
                                        &sender,
                                        peers.read_work_metrics.as_ref(),
                                        &mut root,
                                    )
                                    .await?;
                                }
                            }
                            body => {
                                send_peer_stream_control(
                                    afs_protocol::node_data::DfsReadRangesFrame { body },
                                    &mut current,
                                    &sender,
                                    &controls,
                                    peers.read_work_metrics.as_ref(),
                                    &mut root,
                                )
                                .await?;
                            }
                        }
                    }
                    if let Some(diagnostic) = &producer_diagnostic {
                        diagnostic.eof();
                    }
                    // Flush truncated tails too: they still cannot mint a proof.
                    send_peer_stream_block(
                        &mut current,
                        &sender,
                        peers.read_work_metrics.as_ref(),
                        &mut root,
                    )
                    .await?;
                    root.finish_after_eof()
                })
                .await
                .map_err(|_| {
                    afs_error::Error::coded(
                        afs_error::CLIENT_DEADLINE_EXCEEDED,
                        "Peer stream producer deadline exceeded",
                    )
                })?
            };
            let result: afs_error::Result<PeerRootReceipt> = if let Some(control) = &producer_control {
                tokio::select! {
                    result = receive => result,
                    _ = control.cancelled() => Err(afs_error::Error::coded(afs_error::IO_UNAVAILABLE, "Peer prefetch cancelled")),
                }
            } else { receive.await };
            if let Err(error) = &result {
                // The joined producer result carries the same failure. Never
                // wait again outside its deadline/cancellation boundary merely
                // to notify a consumer whose bounded queue may already be full.
                let _ = sender.try_send(Err(error.clone()));
            }
            result
        });
        let consumer_work = consumer_diagnostic.clone().map(ReadDiagnosticConsumer::new);
        let observation = PeerConsumerObservation {
            metrics: self.peers.read_work_metrics.as_ref(),
            diagnostic: consumer_work.as_ref(),
        };
        let received: afs_error::Result<ReadFrameDecoder<'_>> = (|| {
            let mut decoder = ReadFrameDecoder::for_producer_object(&request, chunk)?;
            decoder.stream_binding = Some(output_binding);
            while let Some(message) = receiver.blocking_recv() {
                if let Some(control) = prefetch {
                    control.check()?;
                }
                match message? {
                    OwnedPeerStreamMessage::Control(control) => {
                        let OwnedPeerStreamControl {
                            frame,
                            outboard_builder,
                            _permit,
                        } = *control;
                        if matches!(&frame.body, Some(dfs_read_ranges_frame::Body::Header(_))) {
                            decoder.install_producer_outboard(outboard_builder)?;
                        } else if outboard_builder.is_some() {
                            return Err(dfs_protocol_error(
                                "outboard metadata must accompany its header",
                            ));
                        }
                        let consume_start = consumer_diagnostic
                            .as_ref()
                            .map(|diagnostic| diagnostic.now());
                        let result = consume_peer_frame_into_sink(
                            &mut decoder,
                            frame,
                            requested_offset,
                            out,
                            sink,
                            deadline,
                            observation,
                        );
                        if let (Some(diagnostic), Some(start)) =
                            (&consumer_diagnostic, consume_start)
                        {
                            diagnostic.consumer(start);
                        }
                        result?;
                    }
                    OwnedPeerStreamMessage::Block(block) => {
                        let consume_start = consumer_diagnostic
                            .as_ref()
                            .map(|diagnostic| diagnostic.now());
                        let result = consume_peer_input_into_sink(
                            &mut decoder,
                            PeerSinkInput::Block {
                                offset: block.offset,
                                bytes: &block.bytes,
                            },
                            requested_offset,
                            out,
                            sink,
                            deadline,
                            observation,
                        );
                        if let (Some(diagnostic), Some(start)) =
                            (&consumer_diagnostic, consume_start)
                        {
                            diagnostic.consumer(start);
                        }
                        result?;
                        // RAII returns the shared block on success and every error.
                        drop(block);
                    }
                }
            }
            Ok(decoder)
        })();
        drop(receiver);
        if received.is_err() {
            producer.abort();
        }
        // Join/drain before output proof, sink cleanup or ownership retirement.
        let joined = self.runtime.block_on(producer);
        let decoder = received?;
        let root = joined.map_err(|error| {
            afs_error::Error::coded(
                afs_error::CLIENT_WORKER_FAILED,
                format!("Peer stream producer join failed: {error}"),
            )
        })??;
        peer_stream_deadline(deadline)?;
        let root_intervals = root.root_intervals;
        let finish_start = consumer_diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic.now());
        let proof = decoder.finish_stream_from_producer(
            chunk,
            root,
            sink,
            requested_offset,
            out,
            observation,
        );
        if let (Some(diagnostic), Some(start)) = (&consumer_diagnostic, finish_start) {
            diagnostic.consumer(start);
        }
        let segment_intervals = consumer_work
            .as_ref()
            .map(ReadDiagnosticConsumer::segment_intervals);
        drop(consumer_work);
        if let (Some(diagnostic), Some(root_intervals), Some(segment_intervals)) =
            (&consumer_diagnostic, &root_intervals, &segment_intervals)
        {
            diagnostic.root_segment_overlap(root_intervals, segment_intervals);
        }
        let proof = proof?;
        peer_stream_deadline(deadline)?;
        if let Some(control) = prefetch {
            control.check()?;
        }
        let result = sink.seal_completed(proof, requested_offset, out).map(Some);
        if let Some(diagnostic) = diagnostic {
            diagnostic.finish(result.is_ok());
        }
        result
    }
}

#[cfg(feature = "dfs")]
impl crate::node::dfs_read::ChunkTransfer for GrpcChunkTransfer {
    fn read_crc32c_range(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &ChunkObject,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        self.read_crc32c_impl(batch, chunk, out)
    }

    fn supports_verified_object_sink(&self) -> bool {
        self.stream_sink_supported()
    }

    fn read_verified_object_to_sink<'output>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &ChunkObject,
        requested_offset: u64,
        private_out: &'output mut [u8],
        sink: &mut PeerStreamSink<'_>,
    ) -> afs_error::Result<Option<CompletedPeerDiskRead<'output>>> {
        self.read_object_to_sink(batch, chunk, requested_offset, private_out, sink, None)
    }

    fn try_read_verified_object_to_sink<'output>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &ChunkObject,
        requested_offset: u64,
        private_out: &'output mut [u8],
        sink: &mut PeerStreamSink<'_>,
        control: &crate::node::dfs_read::PrefetchControl,
    ) -> afs_error::Result<Option<CompletedPeerDiskRead<'output>>> {
        self.read_object_to_sink(
            batch,
            chunk,
            requested_offset,
            private_out,
            sink,
            Some(control),
        )
    }

    fn verification_metrics(&self) -> Option<&ReadWorkMetrics> {
        self.peers.read_work_metrics.as_ref()
    }

    fn read_ranges(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        self.read_ranges_impl(batch, None, out, None).map(|_| ())
    }

    fn read_verified_object<'a>(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        chunk: &'a ChunkObject,
        out: &'a mut [u8],
        proof_budget: Arc<OutboardBudget>,
    ) -> afs_error::Result<VerifiedPeerChunk<'a>> {
        self.read_ranges_impl(batch, Some(chunk), out, Some(proof_budget))?
            .ok_or_else(|| dfs_protocol_error("verified Peer stream omitted its object proof"))
    }
}

/// Only EOF-validated decoding can construct this non-memory stream receipt.
#[cfg(feature = "dfs")]
pub(in crate::node) struct CompletedPeerStreamProof {
    chunk: ChunkObject,
    root_digest: [u8; 32],
    binding: crate::node::peer_cache::PeerStreamBinding,
    outboard: Option<SharedOutboard>,
}

#[cfg(feature = "dfs")]
impl CompletedPeerStreamProof {
    pub(in crate::node) fn into_outboard(self) -> Option<SharedOutboard> {
        self.outboard
    }

    pub(in crate::node) fn binding(&self) -> &crate::node::peer_cache::PeerStreamBinding {
        &self.binding
    }
    pub(in crate::node) fn chunk(&self) -> &ChunkObject {
        &self.chunk
    }
    pub(in crate::node) fn root_digest(&self) -> &[u8; 32] {
        &self.root_digest
    }
}

#[cfg(all(test, feature = "dfs"))]
fn peer_disk_test_frames(
    chunk: &ChunkObject,
    body: &[u8],
) -> (
    DfsReadRangesRequest,
    Vec<afs_protocol::node_data::DfsReadRangesFrame>,
) {
    use afs_protocol::node_data::{
        DfsReadRangesCompletion, DfsReadRangesFrame, DfsReadRangesHeader,
    };
    let request = DfsReadRangesRequest {
        include_blake3_outboard: true,
        read_id: "sink-test-read".into(),
        attempt_id: "sink-test-attempt".into(),
        file_version_id: "version".into(),
        layout_root_id: "layout".into(),
        operations: vec![DfsChunkReadOp {
            operation_index: 0,
            chunk_id: chunk.id.0.clone(),
            chunk_offset: 0,
            length: chunk.length,
            destination_offset: 0,
            source_copy_id: "sink-test-copy".into(),
            grant: None,
        }],
    };
    let mut frames = vec![DfsReadRangesFrame {
        body: Some(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
            crc32c_table: Default::default(),
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            operation_index: 0,
            chunk_id: chunk.id.0.clone(),
            chunk_offset: 0,
            length: chunk.length,
            source_copy_id: "sink-test-copy".into(),
            blake3_outboard: Default::default(),
        })),
    }];
    frames.extend(
        body.chunks(STREAM_DATA_BYTES)
            .map(|data| DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Data(data.to_vec().into())),
            }),
    );
    frames.push(DfsReadRangesFrame {
        body: Some(dfs_read_ranges_frame::Body::Completion(
            DfsReadRangesCompletion {
                crc32c_table: Default::default(),
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                operation_index: 0,
                source_copy_id: "sink-test-copy".into(),
                transferred_bytes: body.len() as u64,
                range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                    as i32,
                range_checksum: blake3::hash(body).as_bytes().to_vec(),
            },
        )),
    });
    (request, frames)
}

#[cfg(all(test, feature = "dfs"))]
pub(in crate::node) fn complete_peer_disk_read_for_test<'output>(
    chunk: &ChunkObject,
    body: &[u8],
    requested_offset: u64,
    out: &'output mut [u8],
    sink: &mut PeerStreamSink<'_>,
) -> afs_error::Result<CompletedPeerDiskRead<'output>> {
    let (request, frames) = peer_disk_test_frames(chunk, body);
    let mut decoder = ReadFrameDecoder::for_object(&request, chunk)?
        .with_outboard_budget(Some(sink.proof_budget()));
    decoder.stream_binding = Some(sink.bind_output(requested_offset, out)?);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    for frame in frames {
        consume_peer_frame_into_sink(
            &mut decoder,
            frame,
            requested_offset,
            out,
            sink,
            deadline,
            PeerConsumerObservation::default(),
        )?;
    }
    sink.seal_completed(decoder.finish_stream(chunk)?, requested_offset, out)
}

/// Only a complete, successful stream can mint this buffer-bound proof.
/// Borrowing the body prevents mutation or allocator reuse while it is usable.
#[cfg(feature = "dfs")]
#[derive(Debug)]
pub(in crate::node) struct CompletedPeerObjectProof<'a> {
    bytes: &'a [u8],
    root_digest: [u8; 32],
    outboard: Option<SharedOutboard>,
}

#[cfg(feature = "dfs")]
impl<'a> CompletedPeerObjectProof<'a> {
    pub(in crate::node) fn into_outboard(self) -> Option<SharedOutboard> {
        self.outboard
    }

    pub(in crate::node) fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    pub(in crate::node) fn root_digest(&self) -> &[u8; 32] {
        &self.root_digest
    }
}

#[cfg(feature = "dfs")]
async fn consume_peer_read_stream<'a, S>(
    request: &DfsReadRangesRequest,
    chunk: Option<&'a ChunkObject>,
    out: &'a mut [u8],
    mut stream: S,
    metrics: Option<&ReadWorkMetrics>,
    proof_budget: Option<Arc<OutboardBudget>>,
) -> afs_error::Result<Option<VerifiedPeerChunk<'a>>>
where
    S: tokio_stream::Stream<
            Item = Result<afs_protocol::node_data::DfsReadRangesFrame, tonic::Status>,
        > + Unpin,
{
    let mut decoder = match chunk {
        Some(chunk) => {
            ReadFrameDecoder::for_object(request, chunk)?.with_outboard_budget(proof_budget)
        }
        None => ReadFrameDecoder::new(request),
    };
    decoder.work_metrics = metrics.cloned();
    while let Some(frame) = stream.next().await {
        let frame = frame.map_err(afs_transport::grpc::error_status::status_to_error)?;
        let copied = match &frame.body {
            Some(dfs_read_ranges_frame::Body::Data(bytes)) => bytes.len() as u64,
            _ => 0,
        };
        decoder.accept(frame, out)?;
        if let Some(metrics) = metrics {
            metrics.copy("frame_to_attempt", copied);
        }
    }
    // EOF, including a possible late gRPC Status, precedes proof creation.
    match chunk {
        Some(chunk) => {
            let body: &'a [u8] = out;
            let proof = decoder.finish_object(body)?;
            VerifiedPeerChunk::from_completed_stream(chunk, body, proof).map(Some)
        }
        None => decoder.finish().map(|()| None),
    }
}

#[cfg(feature = "dfs")]
struct ReadFrameDecoder<'a> {
    request: &'a DfsReadRangesRequest,
    completed: Vec<bool>,
    active: Option<ActivePeerRead>,
    verified_object: Option<VerifiedObjectDecode>,
    stream_binding: Option<crate::node::peer_cache::PeerStreamBinding>,
    producer_root_pending: bool,
    producer_outboard_installed: bool,
    root_mode: PeerRootMode,
    work_metrics: Option<ReadWorkMetrics>,
}

#[cfg(feature = "dfs")]
struct VerifiedObjectDecode {
    chunk: ChunkObject,
    proof_budget: Option<Arc<OutboardBudget>>,
    outboard_builder: Option<OutboardBuilder>,
    outboard: Option<SharedOutboard>,
    expected_root: [u8; 32],
    expected_length: usize,
    actual_root: Option<[u8; 32]>,
    buffer: Option<(usize, usize)>,
}

#[cfg(feature = "dfs")]
impl VerifiedObjectDecode {
    fn finish_outboard(
        &mut self,
        observation: PeerConsumerObservation<'_>,
    ) -> afs_error::Result<Option<[u8; 32]>> {
        if let Some(builder) = self.outboard_builder.take() {
            // Legacy diagnostic name: this candidate's SegmentBlake3 lane is
            // standard-root/outboard work, not the removed v1 segment digest.
            let built =
                observation.measure(ReadDiagnosticConsumerStage::SegmentBlake3, 0, || {
                    builder.finish()
                })?;
            if built.digest.bytes != self.expected_root {
                return Err(dfs_corrupt_error(
                    "outboard body differs from trusted Meta root",
                ));
            }
            if let Some(metrics) = observation.metrics {
                metrics.hash("outboard_parent_hash", built.parent_hash_bytes);
            }
            self.outboard = built.outboard;
            return Ok(Some(built.digest.bytes));
        }
        Ok(None)
    }
}

#[cfg(feature = "dfs")]
struct ActivePeerRead {
    index: usize,
    start: usize,
    end: usize,
    cursor: usize,
    checksum: Option<blake3::Hasher>,
}

#[cfg(feature = "dfs")]
impl<'a> ReadFrameDecoder<'a> {
    fn new(request: &'a DfsReadRangesRequest) -> Self {
        Self {
            request,
            completed: vec![false; request.operations.len()],
            active: None,
            verified_object: None,
            stream_binding: None,
            producer_root_pending: false,
            producer_outboard_installed: false,
            root_mode: PeerRootMode::Producer,
            work_metrics: None,
        }
    }

    fn for_object(
        request: &'a DfsReadRangesRequest,
        chunk: &ChunkObject,
    ) -> afs_error::Result<Self> {
        if chunk.length == 0
            || chunk.length > crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
            || request.operations.len() != 1
            || request.operations[0].chunk_id != chunk.id.0
            || request.operations[0].chunk_offset != 0
            || request.operations[0].destination_offset != 0
            || request.operations[0].length != chunk.length
        {
            return Err(dfs_protocol_error(
                "verified Peer proof requires one complete trusted object range",
            ));
        }
        let mut decoder = Self::new(request);
        decoder.verified_object = Some(VerifiedObjectDecode {
            chunk: chunk.clone(),
            proof_budget: None,
            outboard_builder: None,
            outboard: None,
            expected_root: chunk.content_digest.bytes,
            expected_length: chunk.length as usize,
            actual_root: None,
            buffer: None,
        });
        Ok(decoder)
    }

    fn with_outboard_budget(mut self, budget: Option<Arc<OutboardBudget>>) -> Self {
        if let Some(object) = &mut self.verified_object {
            object.proof_budget = budget;
        }
        self
    }

    fn install_producer_outboard(
        &mut self,
        mut builder: Option<OutboardBuilder>,
    ) -> afs_error::Result<()> {
        if !self.producer_root_pending || self.active.is_some() || self.producer_outboard_installed
        {
            return Err(dfs_protocol_error(
                "outboard handoff is not before a producer header",
            ));
        }
        let object = self
            .verified_object
            .as_mut()
            .ok_or_else(|| dfs_protocol_error("outboard handoff lacks a trusted object"))?;
        if object.outboard_builder.is_some() {
            return Err(dfs_protocol_error("outboard work was already handed off"));
        }
        self.root_mode = PeerRootMode::select(&mut builder);
        object.outboard_builder = builder;
        self.producer_outboard_installed = true;
        Ok(())
    }

    fn for_producer_object(
        request: &'a DfsReadRangesRequest,
        chunk: &ChunkObject,
    ) -> afs_error::Result<Self> {
        let mut decoder = Self::for_object(request, chunk)?;
        decoder.producer_root_pending = true;
        Ok(decoder)
    }

    fn finish_stream_from_producer(
        mut self,
        chunk: &ChunkObject,
        receipt: PeerRootReceipt,
        sink: &PeerStreamSink<'_>,
        offset: u64,
        output: &[u8],
        observation: PeerConsumerObservation<'_>,
    ) -> afs_error::Result<CompletedPeerStreamProof> {
        self.check_frames_finished()?;
        let object = self
            .verified_object
            .as_ref()
            .ok_or_else(|| dfs_protocol_error("Peer producer receipt has no trusted object"))?;
        if !self.producer_root_pending
            || !self.producer_outboard_installed
            || receipt.length != object.expected_length as u64
            || !sink.accepts_binding(&receipt.binding)
            || !receipt.binding.matches_output(offset, output)
        {
            return Err(dfs_corrupt_error(
                "Peer joined transport receipt differs from trusted object or sink",
            ));
        }
        match (self.root_mode, receipt.digest) {
            (PeerRootMode::Producer, PeerRootDigest::Producer(digest))
                if digest == object.expected_root && Some(digest) == object.actual_root =>
            {
                if object.outboard_builder.is_some() {
                    return Err(dfs_protocol_error(
                        "producer root retained duplicate consumer root work",
                    ));
                }
            }
            (PeerRootMode::ConsumerTree, PeerRootDigest::ConsumerTree) => {
                let object = self
                    .verified_object
                    .as_mut()
                    .ok_or_else(|| dfs_protocol_error("consumer root lacks trusted object"))?;
                // actual_root still holds the wire Completion here. It is only
                // comparison evidence, never our independently computed root.
                let completion_root = object.actual_root;
                let computed_root = object
                    .finish_outboard(observation)?
                    .ok_or_else(|| dfs_protocol_error("consumer root work was not completed"))?;
                if computed_root != object.expected_root || Some(computed_root) != completion_root {
                    return Err(dfs_corrupt_error(
                        "Peer consumer root differs from Meta or wire completion",
                    ));
                }
                object.actual_root = Some(computed_root);
            }
            _ => return Err(dfs_corrupt_error("Peer root mode or joined digest differs")),
        }
        self.producer_root_pending = false;
        self.finish_stream(chunk)
    }

    fn accept(
        &mut self,
        frame: afs_protocol::node_data::DfsReadRangesFrame,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        if let Some(object) = &mut self.verified_object {
            let buffer = (out.as_ptr() as usize, out.len());
            if out.len() != object.expected_length
                || object.buffer.is_some_and(|previous| previous != buffer)
            {
                return Err(dfs_protocol_error(
                    "verified Peer stream changed its private object buffer",
                ));
            }
            object.buffer = Some(buffer);
        }
        let work_metrics = self.work_metrics.clone();
        self.accept_into(
            frame,
            out.len(),
            |start, data| {
                out[start..start + data.len()].copy_from_slice(data);
                Ok(())
            },
            PeerConsumerObservation {
                metrics: work_metrics.as_ref(),
                diagnostic: None,
            },
        )
    }

    fn accept_into(
        &mut self,
        frame: afs_protocol::node_data::DfsReadRangesFrame,
        output_length: usize,
        write: impl FnMut(usize, &[u8]) -> afs_error::Result<()>,
        observation: PeerConsumerObservation<'_>,
    ) -> afs_error::Result<()> {
        match frame.body {
            Some(dfs_read_ranges_frame::Body::Header(header)) => {
                let index = header.operation_index as usize;
                let op = self
                    .request
                    .operations
                    .get(index)
                    .ok_or_else(|| dfs_protocol_error("unknown peer read operation"))?;
                if self.active.is_some()
                    || self.completed[index]
                    || header.read_id != self.request.read_id
                    || header.attempt_id != self.request.attempt_id
                    || header.operation_index != op.operation_index
                    || header.chunk_id != op.chunk_id
                    || header.chunk_offset != op.chunk_offset
                    || header.length != op.length
                    || header.source_copy_id != op.source_copy_id
                {
                    return Err(dfs_protocol_error(
                        "peer read header identity/order mismatch",
                    ));
                }
                if !header.blake3_outboard.is_empty()
                    && (!self.request.include_blake3_outboard || self.verified_object.is_none())
                {
                    // A partial grant returns only its exact range and checksum;
                    // it can never manufacture a complete-object root capability.
                    return Err(dfs_protocol_error(
                        "outboard requires a trusted complete object",
                    ));
                }
                if !self.producer_root_pending {
                    if let Some(object) = &mut self.verified_object {
                        object.outboard_builder = peer_outboard_builder(
                            &object.chunk,
                            header.blake3_outboard,
                            object.proof_budget.clone(),
                            observation.metrics,
                        )?;
                        self.root_mode = PeerRootMode::select(&mut object.outboard_builder);
                        if self.root_mode == PeerRootMode::Producer
                            && let Some(metrics) = observation.metrics
                        {
                            metrics.peer_read_event("root_fallback");
                        }
                    }
                } else if !header.blake3_outboard.is_empty() {
                    return Err(dfs_protocol_error(
                        "producer did not retire decoded outboard backing",
                    ));
                }
                let start = usize::try_from(op.destination_offset)
                    .map_err(|_| dfs_protocol_error("read destination overflow"))?;
                let length = usize::try_from(op.length)
                    .map_err(|_| dfs_protocol_error("read length overflow"))?;
                let end = start
                    .checked_add(length)
                    .filter(|end| *end <= output_length)
                    .ok_or_else(|| dfs_protocol_error("read exceeds destination"))?;
                self.active = Some(ActivePeerRead {
                    index,
                    start,
                    end,
                    cursor: start,
                    checksum: (!self.producer_root_pending
                        && self.root_mode == PeerRootMode::Producer)
                        .then(blake3::Hasher::new),
                });
            }
            Some(dfs_read_ranges_frame::Body::Data(data)) => {
                self.accept_data(&data, write, observation)?
            }
            Some(dfs_read_ranges_frame::Body::Completion(completion)) => {
                let active = self
                    .active
                    .take()
                    .ok_or_else(|| dfs_protocol_error("completion without header"))?;
                let op = &self.request.operations[active.index];
                if completion.read_id != self.request.read_id
                    || completion.attempt_id != self.request.attempt_id
                    || completion.operation_index != op.operation_index
                    || completion.source_copy_id != op.source_copy_id
                    || active.cursor != active.end
                    || completion.transferred_bytes != (active.end - active.start) as u64
                    || completion.range_checksum_algorithm
                        != afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32
                {
                    return Err(dfs_protocol_error(
                        "peer read completion identity/length mismatch",
                    ));
                }
                let digest =
                    if self.producer_root_pending {
                        blake3::Hash::from(
                            <[u8; 32]>::try_from(completion.range_checksum.as_slice()).map_err(
                                |_| dfs_corrupt_error("peer read completion checksum mismatch"),
                            )?,
                        )
                    } else if self.root_mode == PeerRootMode::ConsumerTree {
                        let object = self.verified_object.as_mut().ok_or_else(|| {
                            dfs_protocol_error("consumer tree lacks trusted object")
                        })?;
                        let root = object.finish_outboard(observation)?.ok_or_else(|| {
                            dfs_protocol_error("consumer tree root was not completed")
                        })?;
                        blake3::Hash::from(root)
                    } else {
                        let checksum = active.checksum.as_ref().ok_or_else(|| {
                            dfs_protocol_error("Peer consumer root state is missing")
                        })?;
                        observation.measure(ReadDiagnosticConsumerStage::RootBlake3, 0, || {
                            checksum.finalize()
                        })
                    };
                if completion.range_checksum.as_slice() != digest.as_bytes() {
                    return Err(dfs_corrupt_error("peer read completion checksum mismatch"));
                }
                if let Some(object) = &mut self.verified_object {
                    if !self.producer_root_pending && digest.as_bytes() != &object.expected_root {
                        return Err(dfs_corrupt_error(
                            "peer read object differs from trusted Meta root",
                        ));
                    }
                    object.actual_root = Some(*digest.as_bytes());
                }
                self.completed[active.index] = true;
            }
            None => return Err(dfs_protocol_error("empty peer frame")),
        }
        Ok(())
    }

    fn accept_data(
        &mut self,
        data: &[u8],
        mut write: impl FnMut(usize, &[u8]) -> afs_error::Result<()>,
        observation: PeerConsumerObservation<'_>,
    ) -> afs_error::Result<()> {
        let active = self
            .active
            .as_mut()
            .ok_or_else(|| dfs_protocol_error("peer data without header"))?;
        let next = active
            .cursor
            .checked_add(data.len())
            .filter(|end| *end <= active.end)
            .ok_or_else(|| dfs_protocol_error("peer sent too much data"))?;
        write(active.cursor, data)?;
        if let Some(checksum) = &mut active.checksum {
            observation.measure(
                ReadDiagnosticConsumerStage::RootBlake3,
                data.len() as u64,
                || {
                    checksum.update(data);
                },
            );
            if let Some(metrics) = observation.metrics {
                metrics.hash(
                    if self.verified_object.is_some() {
                        "root_in_receive"
                    } else {
                        "peer_range_in_receive"
                    },
                    data.len() as u64,
                );
            }
        }
        if let Some(object) = &mut self.verified_object
            && let Some(builder) = &mut object.outboard_builder
        {
            observation.measure(
                ReadDiagnosticConsumerStage::SegmentBlake3,
                data.len() as u64,
                || builder.update(data),
            )?;
            if let Some(metrics) = observation.metrics {
                metrics.hash("outboard_body_hash", data.len() as u64);
            }
        }
        active.cursor = next;
        Ok(())
    }

    fn check_finished(&self) -> afs_error::Result<()> {
        if self.producer_root_pending {
            return Err(dfs_protocol_error(
                "Peer producer root receipt has not been joined and validated",
            ));
        }
        self.check_frames_finished()
    }

    fn check_frames_finished(&self) -> afs_error::Result<()> {
        if self.active.is_some()
            || self.completed.is_empty()
            || self.completed.iter().any(|done| !done)
        {
            return Err(dfs_protocol_error(
                "peer stream omitted a requested operation or completion",
            ));
        }
        Ok(())
    }

    fn finish(self) -> afs_error::Result<()> {
        self.check_finished()
    }

    fn finish_stream(self, chunk: &ChunkObject) -> afs_error::Result<CompletedPeerStreamProof> {
        self.check_finished()?;
        let object = self
            .verified_object
            .ok_or_else(|| dfs_protocol_error("ordinary ranges cannot mint stream proofs"))?;
        if self.request.operations.len() != 1
            || self.request.operations[0].chunk_id != chunk.id.0
            || object.expected_root != chunk.content_digest.bytes
            || object.expected_length as u64 != chunk.length
        {
            return Err(dfs_protocol_error("Peer stream proof object differs"));
        }
        Ok(CompletedPeerStreamProof {
            chunk: chunk.clone(),
            root_digest: object
                .actual_root
                .ok_or_else(|| dfs_protocol_error("Peer stream root was not completed"))?,
            outboard: object.outboard,
            binding: self
                .stream_binding
                .ok_or_else(|| dfs_protocol_error("Peer stream completion has no sink binding"))?,
        })
    }

    fn finish_object<'body>(
        self,
        body: &'body [u8],
    ) -> afs_error::Result<CompletedPeerObjectProof<'body>> {
        self.check_finished()?;
        let object = self
            .verified_object
            .ok_or_else(|| dfs_protocol_error("ordinary ranges cannot mint object proofs"))?;
        if object.buffer != Some((body.as_ptr() as usize, body.len())) {
            return Err(dfs_protocol_error("Peer object proof buffer differs"));
        }
        Ok(CompletedPeerObjectProof {
            bytes: body,
            root_digest: object
                .actual_root
                .ok_or_else(|| dfs_protocol_error("Peer object root was not completed"))?,
            outboard: object.outboard,
        })
    }
}

#[cfg(feature = "dfs")]
fn wire_dfs_read_grant(grant: &crate::dfs::DfsReadGrant) -> PbDfsReadGrant {
    PbDfsReadGrant {
        namespace_id: grant.namespace_id.0.clone(),
        file_version_id: grant.file_version_id.0.clone(),
        layout_root_id: grant.layout_root_id.0.clone(),
        caller_node_id: grant.caller_node_id.clone(),
        caller_node_epoch: grant.caller_node_epoch,
        expires_at_unix_ms: grant.expires_at_unix_ms,
        fence: grant.fence,
        token: grant.token.clone(),
    }
}

#[cfg(feature = "dfs")]
fn dfs_protocol_error(message: impl Into<String>) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_PROTOCOL_VIOLATION, message)
}

#[cfg(feature = "dfs")]
fn dfs_corrupt_error(message: impl Into<String>) -> afs_error::Error {
    afs_error::Error::coded(afs_error::NODE_TRANSFER_CORRUPT_DATA, message)
}

#[cfg(feature = "dfs")]
pub struct RdmaChunkTransfer {
    pool: Arc<DfsRdmaPool>,
    runtime: PeerRuntime,
    next_read: std::sync::atomic::AtomicU64,
    // Only the compiled RDMA transfer path reads this fallback.
    #[allow(dead_code)]
    fallback: Option<GrpcChunkTransfer>,
}
#[cfg(feature = "dfs")]
impl RdmaChunkTransfer {
    pub fn new(pool: Arc<DfsRdmaPool>) -> afs_error::Result<Self> {
        Ok(Self {
            pool,
            runtime: PeerRuntime::current_or_new().map_err(|error| error.0)?,
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
            runtime: PeerRuntime::current_or_new().map_err(|error| error.0)?,
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
impl crate::node::dfs_read::ChunkTransfer for RdmaChunkTransfer {
    fn verification_metrics(&self) -> Option<&ReadWorkMetrics> {
        self.pool.peers.read_work_metrics.as_ref()
    }
    fn read_ranges(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        self.read_ranges_integrity(batch, out, None)
    }
    fn read_crc32c_range(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
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
        batch: &crate::node::dfs_read::PeerReadBatch,
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
                            let bytes = tokio::task::spawn_blocking(move || {
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
type DfsRdmaReadWindow = (
    DfsReadRangesRequest,
    Vec<(usize, std::ops::Range<usize>)>,
    usize,
);

#[cfg(all(feature = "dfs", feature = "rdma"))]
fn pack_dfs_rdma_read_windows(
    batch: &crate::node::dfs_read::PeerReadBatch,
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
fn validate_dfs_rdma_read_reply(
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
) -> afs_error::Result<Arc<dyn crate::node::dfs_read::ChunkTransfer>> {
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
fn remaining_timeout(
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

/// Transport adapter for already-open DFS writer handles. There is deliberately
/// no automatic retry/fallback of mutating operations after an uncertain reply.
#[cfg(feature = "dfs")]
pub trait RemoteDfsOwner: Send + Sync {
    fn open(
        &self,
        request: afs_protocol::node_control::DfsOwnerOpenRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerOpenReply>;
    fn getattr(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerGetAttrReply>;
    fn release(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerReleaseReply>;
    fn release_with_timeout(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
        _timeout: Duration,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
        self.release(request)
    }
    fn get_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetLockRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerGetLockReply>;
    fn set_lock(
        &self,
        request: afs_protocol::node_control::DfsOwnerSetLockRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerSetLockReply>;
    fn cancel_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerCancelLockWaitReply>;
    fn acknowledge_lock_wait(
        &self,
        request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply>;
    fn acknowledge_lock_wait_with_timeout(
        &self,
        request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
        _timeout: Duration,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitReply> {
        self.acknowledge_lock_wait(request)
    }
    fn release_locks(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerReleaseLocksReply>;
    fn release_lock_session(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply>;
    fn release_lock_session_with_timeout(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
        _timeout: Duration,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerReleaseLockSessionReply> {
        self.release_lock_session(request)
    }
    fn read(
        &self,
        request: afs_protocol::node_data::DfsOwnerReadRequest,
    ) -> afs_error::Result<afs_protocol::node_data::DfsOwnerReadReply>;
    fn write(
        &self,
        request: afs_protocol::node_data::DfsOwnerWriteRequest,
    ) -> afs_error::Result<afs_protocol::node_data::DfsOwnerWriteReply>;
    fn resize(
        &self,
        request: afs_protocol::node_data::DfsOwnerResizeRequest,
    ) -> afs_error::Result<afs_protocol::node_data::DfsOwnerResizeReply>;
    fn sync(
        &self,
        request: afs_protocol::node_data::DfsOwnerSyncRequest,
    ) -> afs_error::Result<afs_protocol::node_data::DfsOwnerSyncReply>;
}

#[cfg(feature = "dfs")]
pub struct GrpcDfsOwnerFactory {
    peers: Arc<PeerConnectionPool>,
    timeout: Duration,
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
    peers: Arc<PeerConnectionPool>,
    node_id: String,
    node_epoch: u64,
    endpoint: String,
    runtime: PeerRuntime,
    timeout: Duration,
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
            runtime: PeerRuntime::current_or_new().map_err(|error| error.0)?,
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
        afs_protocol::node_control::node_control_client::NodeControlClient<Channel>,
    > {
        self.control_client_with_long_wait(false).await
    }

    async fn control_client_with_long_wait(
        &self,
        long_wait: bool,
    ) -> afs_error::Result<
        afs_protocol::node_control::node_control_client::NodeControlClient<Channel>,
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
            afs_protocol::node_control::node_control_client::NodeControlClient::new(channel)
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
                afs_protocol::node_control::node_control_client::NodeControlClient<Channel>,
                tonic::Request<Req>,
            ) -> Fut
            + Send
            + 'static,
    {
        self.dfs_owner_control_request_with_timeout(request, Some(self.timeout), call)
    }

    fn dfs_owner_control_request_with_timeout<Req, Reply, Fut, Call>(
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
                afs_protocol::node_control::node_control_client::NodeControlClient<Channel>,
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
    ($method:ident, $request:ty, $reply:ty) => {
        fn $method(&self, request: $request) -> afs_error::Result<$reply> {
            if request
                .handle
                .as_ref()
                .is_none_or(|handle| handle.owner_node_id != self.node_id)
            {
                return Err(dfs_protocol_error("DFS owner request targets another Node"));
            }
            self.runtime.block_on(async {
                tokio::time::timeout(self.timeout, async {
                    let mut client = self.data_client().await?;
                    let mut request = request_with_current_context(request);
                    request.set_timeout(self.timeout);
                    client
                        .$method(request)
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
        if request.owner_node_id != self.node_id {
            return Err(dfs_protocol_error("DFS owner open targets another Node"));
        }
        self.runtime.block_on(async {
            tokio::time::timeout(self.timeout, async {
                let mut client = self.control_client().await?;
                let mut request = request_with_current_context(request);
                request.set_timeout(self.timeout);
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

    fn getattr(
        &self,
        request: afs_protocol::node_control::DfsOwnerGetAttrRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerGetAttrReply> {
        if request
            .handle
            .as_ref()
            .is_none_or(|handle| handle.owner_node_id != self.node_id)
        {
            return Err(dfs_protocol_error("DFS owner request targets another Node"));
        }
        self.runtime.block_on(async {
            tokio::time::timeout(self.timeout, async {
                let mut client = self.control_client().await?;
                let mut request = request_with_current_context(request);
                request.set_timeout(self.timeout);
                client
                    .dfs_owner_get_attr(request)
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
    fn release(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
        self.release_with_timeout(request, self.timeout)
    }

    fn release_with_timeout(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseRequest,
        timeout: Duration,
    ) -> afs_error::Result<afs_protocol::node_control::DfsOwnerReleaseReply> {
        if request
            .handle
            .as_ref()
            .is_none_or(|handle| handle.owner_node_id != self.node_id)
        {
            return Err(dfs_protocol_error("DFS owner request targets another Node"));
        }
        let timeout = timeout.min(self.timeout);
        self.runtime.block_on(async {
            tokio::time::timeout(timeout, async {
                let mut client = self.control_client().await?;
                let mut request = request_with_current_context(request);
                request.set_timeout(timeout);
                client
                    .dfs_owner_release(request)
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
            Some(self.timeout)
        };
        self.dfs_owner_control_request_with_timeout(
            request,
            timeout,
            |mut client, request| async move { client.dfs_owner_set_lock(request).await },
        )
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
        self.acknowledge_lock_wait_with_timeout(request, self.timeout)
    }

    fn acknowledge_lock_wait_with_timeout(
        &self,
        request: afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
        timeout: Duration,
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
        self.dfs_owner_control_request_with_timeout(
            request,
            Some(timeout.min(self.timeout)),
            |mut client, request| async move {
                client.dfs_owner_acknowledge_lock_wait(request).await
            },
        )
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
        self.release_lock_session_with_timeout(request, self.timeout)
    }

    fn release_lock_session_with_timeout(
        &self,
        request: afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest,
        timeout: Duration,
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
        self.dfs_owner_control_request_with_timeout(
            request,
            Some(timeout.min(self.timeout)),
            |mut client, request| async move {
                client.dfs_owner_release_lock_session(request).await
            },
        )
    }
    dfs_owner_rpc!(
        read,
        afs_protocol::node_data::DfsOwnerReadRequest,
        afs_protocol::node_data::DfsOwnerReadReply
    );
    dfs_owner_rpc!(
        write,
        afs_protocol::node_data::DfsOwnerWriteRequest,
        afs_protocol::node_data::DfsOwnerWriteReply
    );
    dfs_owner_rpc!(
        resize,
        afs_protocol::node_data::DfsOwnerResizeRequest,
        afs_protocol::node_data::DfsOwnerResizeReply
    );
    dfs_owner_rpc!(
        sync,
        afs_protocol::node_data::DfsOwnerSyncRequest,
        afs_protocol::node_data::DfsOwnerSyncReply
    );
}

#[derive(Clone, Debug)]
pub struct DataClientOptions {
    pub endpoint: String,
    pub mode: DataMode,
    pub rdma_device: Option<String>,
    pub timeout: Duration,
    pub tls: afs_transport::TlsConfig,
}

pub type PeerResult<T> = Result<T, PeerError>;

/// 两个数据 adapter 共用错误合同；远端 code 保留，不再压成 String。
#[derive(Debug)]
pub struct PeerError(pub afs_error::Error);
impl PeerError {
    fn coded(code: afs_error::ErrorCode, message: impl Into<String>) -> Self {
        Self(afs_error::Error::coded(code, message))
    }
    pub fn error(&self) -> &afs_error::Error {
        &self.0
    }
    pub fn code(&self) -> afs_error::ErrorCode {
        self.0.code()
    }
    pub fn kind(&self) -> afs_error::ErrorKind {
        self.0.kind()
    }
}
impl std::fmt::Display for PeerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for PeerError {}
impl From<tonic::Status> for PeerError {
    fn from(value: tonic::Status) -> Self {
        Self(afs_transport::grpc::error_status::status_to_error(value))
    }
}
impl From<tonic::transport::Error> for PeerError {
    fn from(value: tonic::transport::Error) -> Self {
        Self::coded(afs_error::CLIENT_CONNECTION_UNAVAILABLE, value.to_string())
    }
}
impl From<tonic::codegen::http::uri::InvalidUri> for PeerError {
    fn from(value: tonic::codegen::http::uri::InvalidUri) -> Self {
        Self::coded(afs_error::CLIENT_ARGUMENT_INVALID, value.to_string())
    }
}

/// 远端 Node 数据面客户端统一入口。
///
/// 这是业务层看到的唯一类型：同一套 read/write API 后面可以接 gRPC inline 或 RDMA。
/// 它不是本地 SDK client；本地 SDK 只连接本机 node，这里用于 node-to-node。
pub struct DataPeerClient {
    inner: DataPeerClientInner,
}

enum DataPeerClientInner {
    Grpc(GrpcInlineClient),
    #[cfg(feature = "rdma")]
    Rdma(Box<RdmaDataClient>),
}

/// OwnerFs 的 node-to-node 文件客户端。
///
/// 它实现 `RemoteFiles`，供非 Home 节点把根内文件操作转发到 Home。read/write
/// 可以使用 gRPC inline 或 OwnerFiles 专用的 one-sided RDMA 数据窗口；不能直接
/// 复用 diagnostics `NodeData` 会话。
#[cfg(feature = "ownerfs")]
pub struct OwnerPeerClient {
    client: StdMutex<OwnerFilesClient<Channel>>,
    lock_control: afs_protocol::node_control::node_control_client::NodeControlClient<Channel>,
    long_lock_control:
        Option<afs_protocol::node_control::node_control_client::NodeControlClient<Channel>>,
    runtime: OwnerRuntime,
    // Per-open immutable read snapshot returned by OwnerFiles.Open. It is
    // removed at release and never reused for another open of the same path.
    prefetched_reads: StdMutex<HashMap<Vec<u8>, Vec<u8>>>,
    pending_releases: Arc<AtomicUsize>,
    rdma_admission: Arc<tokio::sync::Semaphore>,
    metrics: Option<super::OwnerRpcMetrics>,
    data_mode: DataMode,
    rdma_device: Option<String>,
    timeout: Duration,
}

/// Runtime used by synchronous OwnerFs/FUSE callbacks to drive async tonic RPCs.
///
/// `owner_files_client_from_channel` can be called from a Tokio task during tests
/// or from a plain FUSE worker thread in production slow paths. Capturing
/// `Handle::current()` unconditionally panics in the latter case, so we either
/// reuse the ambient runtime or keep a small private runtime alive with the
/// client.
#[cfg(feature = "ownerfs")]
#[derive(Clone)]
enum OwnerRuntime {
    Existing(tokio::runtime::Handle),
    Owned(Arc<tokio::runtime::Runtime>),
}

#[cfg(feature = "ownerfs")]
impl OwnerRuntime {
    fn current_or_new() -> PeerResult<Self> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            return Ok(Self::Existing(handle));
        }
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                PeerError::coded(
                    afs_error::CLIENT_WORKER_FAILED,
                    format!("failed to build OwnerFiles client runtime: {error}"),
                )
            })?;
        Ok(Self::Owned(Arc::new(runtime)))
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        match self {
            Self::Existing(handle) => handle.block_on(future),
            Self::Owned(runtime) => runtime.block_on(future),
        }
    }
}

#[cfg(feature = "ownerfs")]
pub async fn connect_owner_files_client(options: DataClientOptions) -> PeerResult<OwnerPeerClient> {
    let channel = connect_channel(&options.endpoint, options.timeout, options.tls.clone()).await?;
    Ok(
        owner_files_client_from_channel(channel).with_data_transport(
            options.mode,
            options.rdma_device,
            options.timeout,
        ),
    )
}

/// Build an OwnerFiles client from an already configured tonic channel.
///
/// Node wiring that already owns a configured channel can use this directly.
/// Callers that start from an endpoint, including diagnostics, must use
/// `connect_owner_files_client` so the supplied common TLS policy is applied.
#[cfg(feature = "ownerfs")]
pub fn owner_files_client_from_channel(channel: Channel) -> OwnerPeerClient {
    owner_files_client_from_channel_result(channel)
        .expect("OwnerFiles client runtime must be available or constructible")
}

/// Fallible variant used by code paths that want to surface runtime construction
/// errors instead of panicking. The public infallible helper is retained for the
/// existing Node/test wiring contract.
#[cfg(feature = "ownerfs")]
pub fn owner_files_client_from_channel_result(channel: Channel) -> PeerResult<OwnerPeerClient> {
    Ok(OwnerPeerClient {
        lock_control: afs_protocol::node_control::node_control_client::NodeControlClient::new(
            channel.clone(),
        ),
        long_lock_control: None,
        client: StdMutex::new(OwnerFilesClient::new(channel)),
        runtime: OwnerRuntime::current_or_new()?,
        prefetched_reads: StdMutex::new(HashMap::new()),
        pending_releases: Arc::new(AtomicUsize::new(0)),
        rdma_admission: Arc::new(tokio::sync::Semaphore::new(OWNER_RDMA_MAX_CLIENT_WINDOWS)),
        metrics: None,
        data_mode: DataMode::Grpc,
        rdma_device: None,
        timeout: Duration::from_secs(5),
    })
}

/// Production wiring passes the Node runtime so read-only CLOSE can be sent
/// without waiting for a response, like the prior HomeFs P2P path. The default
/// constructor above remains synchronous when it owns a current-thread runtime.
#[cfg(feature = "ownerfs")]
pub fn owner_files_client_from_channel_with_runtime(
    channel: Channel,
    runtime: tokio::runtime::Handle,
) -> OwnerPeerClient {
    owner_files_client_from_channel_with_runtime_and_metrics(channel, runtime, None)
}

#[cfg(feature = "ownerfs")]
pub fn owner_files_client_from_channel_with_runtime_and_metrics(
    channel: Channel,
    runtime: tokio::runtime::Handle,
    metrics: Option<super::OwnerRpcMetrics>,
) -> OwnerPeerClient {
    OwnerPeerClient {
        lock_control: afs_protocol::node_control::node_control_client::NodeControlClient::new(
            channel.clone(),
        ),
        long_lock_control: None,
        client: StdMutex::new(OwnerFilesClient::new(channel)),
        runtime: OwnerRuntime::Existing(runtime),
        prefetched_reads: StdMutex::new(HashMap::new()),
        pending_releases: Arc::new(AtomicUsize::new(0)),
        rdma_admission: Arc::new(tokio::sync::Semaphore::new(OWNER_RDMA_MAX_CLIENT_WINDOWS)),
        metrics,
        data_mode: DataMode::Grpc,
        rdma_device: None,
        timeout: Duration::from_secs(5),
    }
}

#[cfg(feature = "ownerfs")]
impl OwnerPeerClient {
    fn acknowledge_lock_wait(
        &self,
        grant: &RootGrant,
        id: &crate::node::vfs::locks::LockWaiterId,
    ) -> afs_error::Result<()> {
        let mut client = self.lock_control.clone();
        let body = afs_protocol::node_control::OwnerAckLockWaitRequest {
            access: Some(root_access(grant)),
            waiter: Some(afs_protocol::node_control::OwnerLockWaiter {
                ingress_session_id: id.ingress_session_id.clone(),
                request_id: id.request_id,
            }),
        };
        self.runtime.block_on(async move {
            tokio::time::timeout(
                Duration::from_secs(5),
                client.owner_ack_lock_wait(request_with_current_context(body)),
            )
            .await
            .map_err(|_| owner_lock_transport_unknown())?
            .map_err(|e| PeerError::from(e).0)
        })?;
        Ok(())
    }

    pub fn with_long_wait_channel(mut self, channel: Channel) -> Self {
        self.long_lock_control =
            Some(afs_protocol::node_control::node_control_client::NodeControlClient::new(channel));
        self
    }

    pub fn with_data_transport(
        mut self,
        mode: DataMode,
        device: Option<String>,
        timeout: Duration,
    ) -> Self {
        if mode != DataMode::Grpc
            && let Ok(mut prefetched) = self.prefetched_reads.lock()
        {
            prefetched.clear();
        }
        self.data_mode = mode;
        self.rdma_device = device;
        self.timeout = timeout;
        self
    }

    pub fn with_rdma_admission(mut self, admission: Arc<tokio::sync::Semaphore>) -> Self {
        self.rdma_admission = admission;
        self
    }

    fn cloned_client(&self) -> PeerResult<OwnerFilesClient<Channel>> {
        Ok(self
            .client
            .lock()
            .map_err(|_| {
                PeerError::coded(
                    afs_error::CLIENT_WORKER_FAILED,
                    "OwnerFiles client lock poisoned",
                )
            })?
            .clone())
    }

    #[cfg(feature = "rdma")]
    async fn negotiate_owner_rdma(&self, grant: &RootGrant) -> PeerResult<OwnerRdmaWindow> {
        let device = self.rdma_device.clone().ok_or_else(|| {
            PeerError::coded(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                "OwnerFiles RDMA requires rdma_device",
            )
        })?;
        let permit = self
            .rdma_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                PeerError::coded(
                    afs_error::NODE_RDMA_CAPACITY,
                    "OwnerFiles RDMA client admission is full",
                )
            })?;
        let (resource, info, capacity) = tokio::task::spawn_blocking(move || {
            let mut endpoint = RdmaEndpoint::open(&device)?;
            let info = endpoint.info()?.to_vec();
            let capacity = endpoint.capacity();
            let resource = OwnerRdmaEndpointResource {
                endpoint,
                _permit: permit,
            };
            Ok::<_, afs_transport::rdma::RdmaError>((resource, info, capacity))
        })
        .await
        .map_err(|error| PeerError::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string()))?
        .map_err(owner_rdma_error)?;
        let mut control = self.lock_control.clone();
        let mut request = request_with_current_context(OwnerNegotiateDataRequest {
            access: Some(root_access(grant)),
            negotiation: Some(NegotiateDataRequest {
                client_info: info,
                capacity: capacity as u32,
                handshake_version: RDMA_HANDSHAKE_VERSION,
            }),
        });
        request.set_timeout(self.timeout);
        let reply = control.owner_negotiate_data(request).await?.into_inner();
        if !reply.rdma_supported {
            if reply.session_id == 0
                && reply.server_info.is_empty()
                && reply.capacity == 0
                && reply.handshake_version == RDMA_HANDSHAKE_VERSION
            {
                return Err(PeerError::coded(
                    afs_error::NODE_TRANSFER_UNSUPPORTED,
                    "OwnerFiles RDMA is not supported by peer",
                ));
            }
            return Err(PeerError::coded(
                afs_error::CLIENT_PROTOCOL_VIOLATION,
                "OwnerFiles RDMA negotiation unsupported reply is malformed",
            ));
        }
        let mut cleanup = OwnerRdmaCloseGuard {
            control: control.clone(),
            access: root_access(grant),
            session_id: reply.session_id,
            timeout: self.timeout,
            armed: true,
        };
        if reply.session_id == 0
            || reply.handshake_version != RDMA_HANDSHAKE_VERSION
            || reply.capacity as usize != capacity
        {
            return Err(PeerError::coded(
                afs_error::CLIENT_PROTOCOL_VIOLATION,
                "OwnerFiles RDMA negotiation reply is invalid",
            ));
        }
        let server_info = reply.server_info;
        let resource = tokio::task::spawn_blocking(move || {
            let mut resource = resource;
            resource.endpoint.connect(&server_info)?;
            resource.endpoint.send_probe(5000)?;
            Ok::<_, afs_transport::rdma::RdmaError>(resource)
        })
        .await
        .map_err(|error| PeerError::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string()))?
        .map_err(owner_rdma_error)?;
        cleanup.armed = false;
        Ok(OwnerRdmaWindow {
            resource,
            session_id: reply.session_id,
            close: OwnerRdmaCloseGuard {
                control,
                access: root_access(grant),
                session_id: reply.session_id,
                timeout: self.timeout,
                armed: true,
            },
        })
    }

    #[cfg(not(feature = "rdma"))]
    async fn negotiate_owner_rdma(&self, _grant: &RootGrant) -> PeerResult<OwnerRdmaWindow> {
        Err(PeerError::coded(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "RDMA feature is not enabled",
        ))
    }

    #[cfg(feature = "rdma")]
    async fn owner_read_with_rdma_window(
        &self,
        window: OwnerRdmaWindow,
        grant: &RootGrant,
        file: &RemoteFile,
        offset: u64,
        out: &mut [u8],
    ) -> PeerResult<usize> {
        let OwnerRdmaWindow {
            resource,
            session_id,
            mut close,
        } = window;
        let mut client = self.cloned_client()?;
        let mut request = request_with_current_context(OwnerReadRequest {
            access: Some(root_access(grant)),
            handle: Some(file_handle(file)),
            offset,
            length: out.len() as u32,
            plane: Some(rdma_plane(session_id)),
        });
        request.set_timeout(self.timeout);
        let reply = client.read(request).await?.into_inner();
        if !reply.data.is_empty() || reply.read as usize > out.len() {
            return Err(PeerError::coded(
                afs_error::CLIENT_PROTOCOL_VIOLATION,
                "OwnerFiles RDMA read reply shape mismatch",
            ));
        }
        let len = reply.read as usize;
        let (resource_guard, bytes) = tokio::task::spawn_blocking(move || {
            let mut resource = resource;
            let bytes = resource.endpoint.get_local(len)?;
            Ok::<_, afs_transport::rdma::RdmaError>((resource, bytes))
        })
        .await
        .map_err(|error| PeerError::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string()))?
        .map_err(owner_rdma_error)?;
        if blake3::hash(&bytes).as_bytes().as_slice() != reply.data_checksum.as_slice() {
            return Err(PeerError::coded(
                afs_error::NODE_TRANSFER_CORRUPT_DATA,
                "OwnerFiles RDMA read checksum mismatch",
            ));
        }
        out[..len].copy_from_slice(&bytes);
        close.armed = false;
        let _ = owner_close_rdma(close).await;
        drop(resource_guard);
        Ok(len)
    }

    #[cfg(not(feature = "rdma"))]
    async fn owner_read_with_rdma_window(
        &self,
        _window: OwnerRdmaWindow,
        _grant: &RootGrant,
        _file: &RemoteFile,
        _offset: u64,
        _out: &mut [u8],
    ) -> PeerResult<usize> {
        Err(PeerError::coded(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "RDMA feature is not enabled",
        ))
    }

    #[cfg(feature = "rdma")]
    async fn owner_write_with_rdma_window(
        &self,
        window: OwnerRdmaWindow,
        grant: &RootGrant,
        file: &RemoteFile,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> PeerResult<usize> {
        let OwnerRdmaWindow {
            resource,
            session_id,
            mut close,
        } = window;
        let payload = data.to_vec();
        let checksum = blake3::hash(&payload).as_bytes().to_vec();
        let resource_guard = tokio::task::spawn_blocking({
            let payload = payload.clone();
            move || {
                let mut resource = resource;
                resource.endpoint.put_local(&payload)?;
                Ok::<_, afs_transport::rdma::RdmaError>(resource)
            }
        })
        .await
        .map_err(|error| PeerError::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string()))?
        .map_err(owner_rdma_error)?;
        let mut client = self.cloned_client()?;
        let mut request = request_with_current_context(OwnerWriteRequest {
            access: Some(root_access(grant)),
            handle: Some(file_handle(file)),
            offset,
            data: Vec::new(),
            length: payload.len() as u32,
            plane: Some(rdma_plane(session_id)),
            kill_suidgid: options.kill_suidgid,
            data_checksum: checksum,
        });
        request.set_timeout(self.timeout);
        let reply = client.write(request).await?.into_inner();
        if reply.written as usize > payload.len() {
            return Err(PeerError::coded(
                afs_error::CLIENT_PROTOCOL_VIOLATION,
                "OwnerFiles RDMA write count exceeds request length",
            ));
        }
        close.armed = false;
        let _ = owner_close_rdma(close).await;
        drop(resource_guard);
        Ok(reply.written as usize)
    }

    #[cfg(not(feature = "rdma"))]
    async fn owner_write_with_rdma_window(
        &self,
        _window: OwnerRdmaWindow,
        _grant: &RootGrant,
        _file: &RemoteFile,
        _offset: u64,
        _data: &[u8],
        _options: WriteOptions,
    ) -> PeerResult<usize> {
        Err(PeerError::coded(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "RDMA feature is not enabled",
        ))
    }
}

#[cfg(feature = "ownerfs")]
struct OwnerRdmaWindow {
    #[cfg(feature = "rdma")]
    resource: OwnerRdmaEndpointResource,
    #[cfg(feature = "rdma")]
    session_id: u64,
    #[cfg(feature = "rdma")]
    close: OwnerRdmaCloseGuard,
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
struct OwnerRdmaEndpointResource {
    endpoint: RdmaEndpoint,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
struct OwnerRdmaCloseGuard {
    control: afs_protocol::node_control::node_control_client::NodeControlClient<Channel>,
    access: RootAccess,
    session_id: u64,
    timeout: Duration,
    armed: bool,
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
async fn owner_close_rdma(mut close: OwnerRdmaCloseGuard) -> PeerResult<()> {
    let mut request = request_with_current_context(OwnerCloseDataRequest {
        access: Some(close.access.clone()),
        session_id: close.session_id,
    });
    request.set_timeout(close.timeout);
    close.control.owner_close_data(request).await?;
    close.armed = false;
    Ok(())
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
impl Drop for OwnerRdmaCloseGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let mut control = self.control.clone();
            let access = self.access.clone();
            let session_id = self.session_id;
            let timeout = self.timeout;
            handle.spawn(async move {
                let mut request = request_with_current_context(OwnerCloseDataRequest {
                    access: Some(access),
                    session_id,
                });
                request.set_timeout(timeout);
                let _ = control.owner_close_data(request).await;
            });
        }
    }
}

#[cfg(feature = "ownerfs")]
macro_rules! owner_rpc {
    ($owner:expr, $method:ident, $request:expr) => {{
        let mut client = $owner.cloned_client().map_err(|error| error.0)?;
        let started = Instant::now();
        let result = $owner
            .runtime
            .block_on(async move { client.$method(request_with_current_context($request)).await })
            .map_err(|error| PeerError::from(error).0);
        if let Some(metrics) = &$owner.metrics {
            metrics.observe("client", stringify!($method), started.elapsed());
        }
        result?.into_inner()
    }};
}

#[cfg(feature = "ownerfs")]
impl RemoteFiles for OwnerPeerClient {
    fn getlk(
        &self,
        grant: &RootGrant,
        file: &RemoteFile,
        request: crate::node::vfs::locks::LockRequest,
    ) -> afs_error::Result<Option<crate::node::vfs::types::FileLockConflict>> {
        let mut client = self.lock_control.clone();
        let body = afs_protocol::node_control::OwnerGetLockRequest {
            handle: Some(owner_lock_wire_handle(grant, file)),
            lock: Some(super::control::owner_lock_to_wire(&request)),
        };
        let reply = self
            .runtime
            .block_on(async move {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    client.owner_get_lock(request_with_current_context(body)),
                )
                .await
                .map_err(|_| owner_lock_transport_unknown())?
                .map_err(|e| PeerError::from(e).0)
            })?
            .into_inner();
        reply
            .conflict
            .map(|lock| {
                super::control::owner_lock_from_wire(Some(lock)).map(|request| {
                    crate::node::vfs::types::FileLockConflict {
                        kind: request.kind,
                        owner: request.owner,
                        pid: request.pid,
                        range: request.range,
                        lock_type: request.lock_type,
                    }
                })
            })
            .transpose()
    }
    fn setlk(
        &self,
        grant: &RootGrant,
        file: &RemoteFile,
        request: crate::node::vfs::locks::LockRequest,
        waiter: Option<crate::node::vfs::locks::LockWaiterId>,
    ) -> afs_error::Result<()> {
        let blocking = waiter.is_some();
        let mut client = if blocking {
            self.long_lock_control.clone().ok_or_else(|| {
                afs_error::Error::coded(
                    afs_error::NODE_VFS_UNIMPLEMENTED,
                    "Owner blocking locks require a channel without request deadline",
                )
            })?
        } else {
            self.lock_control.clone()
        };
        let body = afs_protocol::node_control::OwnerSetLockRequest {
            handle: Some(owner_lock_wire_handle(grant, file)),
            lock: Some(super::control::owner_lock_to_wire(&request)),
            waiter: waiter
                .as_ref()
                .map(|id| afs_protocol::node_control::OwnerLockWaiter {
                    ingress_session_id: id.ingress_session_id.clone(),
                    request_id: id.request_id,
                }),
        };
        let result = self.runtime.block_on(async move {
            let call = client.owner_set_lock(request_with_current_context(body));
            if blocking {
                call.await
            } else {
                tokio::time::timeout(Duration::from_secs(5), call)
                    .await
                    .map_err(|_| tonic::Status::deadline_exceeded("Owner lock control timed out"))?
            }
        });
        if let Err(error) = result {
            // A structured server error is a definitive business rejection;
            // preserve its errno (including EINTR/ENOLCK/permission errors).
            let transport_unknown = error.details().is_empty()
                && matches!(
                    error.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::Cancelled
                        | tonic::Code::Unknown
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Internal
                );
            if transport_unknown && let Some(waiter) = &waiter {
                // Only an authenticated terminal outcome resolves a lost reply.
                // A raced grant is never rolled back by broad owner cleanup.
                match self.cancel_lock_wait(grant, waiter.clone()) {
                    Ok(crate::node::vfs::locks::LockWaiterOutcome::Granted) => {
                        let _ = self.acknowledge_lock_wait(grant, waiter);
                        return Ok(());
                    }
                    Ok(crate::node::vfs::locks::LockWaiterOutcome::Cancelled) => {
                        let _ = self.acknowledge_lock_wait(grant, waiter);
                        return Err(afs_error::Error::from(std::io::Error::from_raw_os_error(
                            libc::EINTR,
                        )));
                    }
                    Ok(crate::node::vfs::locks::LockWaiterOutcome::Unknown) | Err(_) => {
                        return Err(afs_error::Error::coded(
                            afs_error::IO_UNAVAILABLE,
                            format!(
                                "Owner blocking lock response uncertain; original waiter retained: {error}"
                            ),
                        ));
                    }
                }
            }
            if let Some(waiter) = &waiter {
                // The server has delivered a definitive business rejection;
                // retire a cancelled terminal outcome without touching locks.
                let _ = self.acknowledge_lock_wait(grant, waiter);
            }
            return Err(PeerError::from(error).0);
        }
        if let Some(waiter) = waiter {
            let _ = self.acknowledge_lock_wait(grant, &waiter);
        }
        Ok(())
    }
    fn cancel_lock_wait(
        &self,
        grant: &RootGrant,
        waiter: crate::node::vfs::locks::LockWaiterId,
    ) -> afs_error::Result<crate::node::vfs::locks::LockWaiterOutcome> {
        let mut client = self.lock_control.clone();
        let body = afs_protocol::node_control::OwnerCancelLockWaitRequest {
            access: Some(root_access(grant)),
            waiter: Some(afs_protocol::node_control::OwnerLockWaiter {
                ingress_session_id: waiter.ingress_session_id,
                request_id: waiter.request_id,
            }),
        };
        let reply = self
            .runtime
            .block_on(async move {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    client.owner_cancel_lock_wait(request_with_current_context(body)),
                )
                .await
                .map_err(|_| owner_lock_transport_unknown())?
                .map_err(|e| PeerError::from(e).0)
            })?
            .into_inner();
        match reply.outcome {
            1 => Ok(crate::node::vfs::locks::LockWaiterOutcome::Cancelled),
            2 => Ok(crate::node::vfs::locks::LockWaiterOutcome::Granted),
            3 => Ok(crate::node::vfs::locks::LockWaiterOutcome::Unknown),
            _ => Err(afs_error::Error::coded(
                afs_error::IO_OTHER,
                "Owner lock cancellation outcome unknown",
            )),
        }
    }
    fn release_locks(
        &self,
        grant: &RootGrant,
        file: &RemoteFile,
        owner: crate::node::vfs::types::FileLockOwner,
        kind: crate::node::vfs::types::ReleaseKind,
    ) -> afs_error::Result<()> {
        let mut client = self.lock_control.clone();
        let body = afs_protocol::node_control::OwnerReleaseLocksRequest {
            handle: Some(owner_lock_wire_handle(grant, file)),
            ingress_session_id: owner.ingress_session_id,
            kernel_owner: owner.kernel_owner,
            release_kind: match kind {
                crate::node::vfs::types::ReleaseKind::PosixOwner => 1,
                crate::node::vfs::types::ReleaseKind::FlockOwner => 2,
            },
        };
        self.runtime.block_on(async move {
            tokio::time::timeout(
                Duration::from_secs(5),
                client.owner_release_locks(request_with_current_context(body)),
            )
            .await
            .map_err(|_| owner_lock_transport_unknown())?
            .map_err(|e| PeerError::from(e).0)
        })?;
        Ok(())
    }
    fn release_lock_session(&self, grant: &RootGrant, session: &str) -> afs_error::Result<()> {
        let mut client = self.lock_control.clone();
        let body = afs_protocol::node_control::OwnerReleaseLockSessionRequest {
            access: Some(root_access(grant)),
            ingress_session_id: session.into(),
        };
        self.runtime.block_on(async move {
            tokio::time::timeout(
                Duration::from_secs(5),
                client.owner_release_lock_session(request_with_current_context(body)),
            )
            .await
            .map_err(|_| owner_lock_transport_unknown())?
            .map_err(|e| PeerError::from(e).0)
        })?;
        Ok(())
    }

    fn lookup(
        &self,
        grant: &RootGrant,
        path: &OsStr,
        expected_parent: Option<&FileIdentity>,
    ) -> afs_error::Result<OwnerEntry> {
        let reply = owner_rpc!(
            self,
            lookup,
            OwnerLookupRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_parent_identity: expected_parent.map(file_identity),
            }
        );
        owner_entry(grant, reply.attr)
    }

    fn getattr(
        &self,
        grant: &RootGrant,
        path: &OsStr,
        expected_identity: Option<&FileIdentity>,
        file: Option<&RemoteFile>,
    ) -> afs_error::Result<OwnerEntry> {
        let reply = owner_rpc!(
            self,
            get_attr,
            OwnerGetAttrRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_file_identity: expected_identity.map(file_identity),
                handle: file.map(file_handle),
            }
        );
        owner_entry(grant, reply.attr)
    }

    fn statfs(
        &self,
        grant: &RootGrant,
        path: &OsStr,
        expected_identity: Option<&FileIdentity>,
    ) -> afs_error::Result<FilesystemCapacity> {
        let reply = owner_rpc!(
            self,
            stat_fs,
            OwnerStatFsRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_file_identity: expected_identity.map(file_identity),
            }
        );
        filesystem_capacity(reply.capacity)
    }

    fn setattr_with_options(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        path: &OsStr,
        expected_identity: Option<&FileIdentity>,
        file: Option<&RemoteFile>,
        change: &AttributeChange,
        options: SetAttrOptions,
    ) -> afs_error::Result<OwnerEntry> {
        let reply = owner_rpc!(
            self,
            set_attr,
            OwnerSetAttrRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_file_identity: expected_identity.map(file_identity),
                attr: Some(owner_set_attr(change, options)),
                handle: file.map(file_handle),
                caller: Some(owner_caller(ctx)),
            }
        );
        owner_entry(grant, reply.attr)
    }

    fn getxattr(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        path: &OsStr,
        expected_identity: &FileIdentity,
        name: &OsStr,
    ) -> afs_error::Result<Vec<u8>> {
        let reply = owner_rpc!(
            self,
            get_xattr,
            OwnerGetXattrRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_file_identity: Some(file_identity(expected_identity)),
                name: name.as_bytes().to_vec(),
                caller: Some(owner_caller(ctx)),
            }
        );
        Ok(reply.value)
    }

    fn listxattr(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        path: &OsStr,
        expected_identity: &FileIdentity,
    ) -> afs_error::Result<Vec<u8>> {
        let reply = owner_rpc!(
            self,
            list_xattr,
            OwnerListXattrRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_file_identity: Some(file_identity(expected_identity)),
                caller: Some(owner_caller(ctx)),
            }
        );
        Ok(reply.names)
    }

    fn setxattr(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        path: &OsStr,
        expected_identity: &FileIdentity,
        name: &OsStr,
        value: &[u8],
        flags: i32,
    ) -> afs_error::Result<()> {
        let _reply = owner_rpc!(
            self,
            set_xattr,
            OwnerSetXattrRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_file_identity: Some(file_identity(expected_identity)),
                name: name.as_bytes().to_vec(),
                value: value.to_vec(),
                flags,
                caller: Some(owner_caller(ctx)),
            }
        );
        Ok(())
    }

    fn removexattr(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        path: &OsStr,
        expected_identity: &FileIdentity,
        name: &OsStr,
    ) -> afs_error::Result<()> {
        let _reply = owner_rpc!(
            self,
            remove_xattr,
            OwnerRemoveXattrRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_file_identity: Some(file_identity(expected_identity)),
                name: name.as_bytes().to_vec(),
                caller: Some(owner_caller(ctx)),
            }
        );
        Ok(())
    }

    fn create_with_options(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        path: &OsStr,
        flags: i32,
        mode: u32,
        expected_parent: &FileIdentity,
        options: OpenOptions,
    ) -> afs_error::Result<RemoteCreatedFile> {
        let reply = owner_rpc!(
            self,
            create,
            OwnerCreateRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                flags: flags as u32,
                mode,
                expected_parent: Some(file_identity(expected_parent)),
                caller: Some(owner_caller(ctx)),
                kill_suidgid: options.kill_suidgid,
            }
        );
        let entry = owner_entry(grant, reply.attr)?;
        let handle = reply
            .handle
            .ok_or_else(|| protocol_error("OwnerCreateReply missing handle"))?;
        Ok(RemoteCreatedFile {
            file: remote_file(
                grant,
                entry.identity.clone(),
                handle.opaque,
                reply.owner_session_id,
            ),
            entry,
        })
    }

    fn mkdir(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        path: &OsStr,
        mode: u32,
        expected_parent: &FileIdentity,
    ) -> afs_error::Result<OwnerEntry> {
        let reply = owner_rpc!(
            self,
            mkdir,
            OwnerMkdirRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                mode,
                expected_parent: Some(file_identity(expected_parent)),
                caller: Some(owner_caller(ctx)),
            }
        );
        owner_entry(grant, reply.attr)
    }

    fn mknod(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        path: &OsStr,
        kind: SpecialFileKind,
        mode: u32,
        expected_parent: &FileIdentity,
    ) -> afs_error::Result<OwnerEntry> {
        let reply = owner_rpc!(
            self,
            mknod,
            OwnerMknodRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                special_node: Some(owner_special_node(kind)),
                mode,
                expected_parent: Some(file_identity(expected_parent)),
                caller: Some(owner_caller(ctx)),
            }
        );
        owner_entry(grant, reply.attr)
    }

    fn unlink(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        path: &OsStr,
        expected_identity: Option<&FileIdentity>,
        expected_parent: &FileIdentity,
    ) -> afs_error::Result<()> {
        let _reply = owner_rpc!(
            self,
            unlink,
            OwnerUnlinkRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_file_identity: expected_identity.map(file_identity),
                expected_parent: Some(file_identity(expected_parent)),
                caller: Some(owner_caller(ctx)),
            }
        );
        Ok(())
    }

    fn rmdir(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        path: &OsStr,
        expected_identity: Option<&FileIdentity>,
        expected_parent: &FileIdentity,
    ) -> afs_error::Result<()> {
        let _reply = owner_rpc!(
            self,
            rmdir,
            OwnerRmdirRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_file_identity: expected_identity.map(file_identity),
                expected_parent: Some(file_identity(expected_parent)),
                caller: Some(owner_caller(ctx)),
            }
        );
        Ok(())
    }

    fn rename(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        old_path: &OsStr,
        new_path: &OsStr,
        expected_old_identity: Option<&FileIdentity>,
        expected_new_identity: Option<&FileIdentity>,
        expected_old_parent: &FileIdentity,
        expected_new_parent: &FileIdentity,
        flags: RenameFlags,
    ) -> afs_error::Result<()> {
        let _reply = owner_rpc!(
            self,
            rename,
            OwnerRenameRequest {
                access: Some(root_access(grant)),
                old_path: old_path.as_bytes().to_vec(),
                new_path: new_path.as_bytes().to_vec(),
                expected_old_identity: expected_old_identity.map(file_identity),
                expected_new_identity: expected_new_identity.map(file_identity),
                flags: flags.0,
                expected_old_parent: Some(file_identity(expected_old_parent)),
                expected_new_parent: Some(file_identity(expected_new_parent)),
                caller: Some(owner_caller(ctx)),
            }
        );
        Ok(())
    }

    fn open_with_options(
        &self,
        grant: &RootGrant,
        path: &OsStr,
        flags: i32,
        expected_identity: Option<&FileIdentity>,
        options: OpenOptions,
    ) -> afs_error::Result<(RemoteFile, FileAttributes)> {
        let reply = owner_rpc!(
            self,
            open,
            OwnerOpenRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                flags: flags as u32,
                mode: 0,
                expected_file_identity: expected_identity.map(file_identity),
                kill_suidgid: options.kill_suidgid,
                disable_prefetch: self.data_mode != DataMode::Grpc,
            }
        );
        let identity = reply
            .file_identity
            .ok_or_else(|| protocol_error("OwnerOpenReply missing file_identity"))?;
        let handle = reply
            .handle
            .ok_or_else(|| protocol_error("OwnerOpenReply missing handle"))?;
        let attributes = file_attributes(
            reply
                .attr
                .ok_or_else(|| protocol_error("OwnerOpenReply missing attr"))?,
        )?;
        if self.data_mode == DataMode::Grpc
            && let Some(bytes) = reply.prefetched_data
        {
            self.prefetched_reads
                .lock()
                .map_err(|_| protocol_error("OwnerFiles prefetch cache lock poisoned"))?
                .insert(handle.opaque.clone(), bytes);
        }
        Ok((
            remote_file(
                grant,
                FileIdentity(identity.opaque),
                handle.opaque,
                reply.owner_session_id,
            ),
            attributes,
        ))
    }

    fn readlink(
        &self,
        grant: &RootGrant,
        path: &OsStr,
        expected_identity: Option<&FileIdentity>,
    ) -> afs_error::Result<Vec<u8>> {
        let reply = owner_rpc!(
            self,
            readlink,
            OwnerReadlinkRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_file_identity: expected_identity.map(file_identity),
            }
        );
        Ok(reply.target)
    }

    fn symlink(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        path: &OsStr,
        target: &OsStr,
        expected_parent: &FileIdentity,
    ) -> afs_error::Result<OwnerEntry> {
        let reply = owner_rpc!(
            self,
            symlink,
            OwnerSymlinkRequest {
                access: Some(root_access(grant)),
                link_path: path.as_bytes().to_vec(),
                target: target.as_bytes().to_vec(),
                expected_parent: Some(file_identity(expected_parent)),
                caller: Some(owner_caller(ctx)),
            }
        );
        owner_entry(grant, reply.attr)
    }

    fn link(
        &self,
        ctx: &RequestContext,
        grant: &RootGrant,
        old_path: &OsStr,
        new_path: &OsStr,
        expected_old_identity: &FileIdentity,
        expected_new_parent: &FileIdentity,
    ) -> afs_error::Result<OwnerEntry> {
        let reply = owner_rpc!(
            self,
            link,
            OwnerLinkRequest {
                access: Some(root_access(grant)),
                old_path: old_path.as_bytes().to_vec(),
                new_path: new_path.as_bytes().to_vec(),
                expected_old_identity: Some(file_identity(expected_old_identity)),
                expected_new_parent: Some(file_identity(expected_new_parent)),
                caller: Some(owner_caller(ctx)),
            }
        );
        owner_entry(grant, reply.attr)
    }

    fn read(
        &self,
        grant: &RootGrant,
        file: &RemoteFile,
        offset: u64,
        out: &mut [u8],
    ) -> afs_error::Result<usize> {
        validate_length(out.len()).map_err(|error| error.0)?;
        if self.data_mode == DataMode::Grpc
            && let Some(bytes) = self
                .prefetched_reads
                .lock()
                .map_err(|_| protocol_error("OwnerFiles prefetch cache lock poisoned"))?
                .get(&file.handle)
        {
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(bytes.len());
            let end = start.saturating_add(out.len()).min(bytes.len());
            out[..end - start].copy_from_slice(&bytes[start..end]);
            return Ok(end - start);
        }
        let started = std::time::Instant::now();
        let result = (|| -> PeerResult<usize> {
            if !out.is_empty() && matches!(self.data_mode, DataMode::Rdma | DataMode::Auto) {
                let negotiated = self.runtime.block_on(self.negotiate_owner_rdma(grant));
                match (self.data_mode, negotiated) {
                    (_, Ok(window)) => {
                        let read = self.runtime.block_on(
                            self.owner_read_with_rdma_window(window, grant, file, offset, out),
                        )?;
                        if let Some(metrics) = &self.metrics {
                            metrics.record_payload("client", "read", "rdma", read as u64);
                        }
                        return Ok(read);
                    }
                    (DataMode::Rdma, Err(error)) => return Err(error),
                    (DataMode::Auto, Err(error))
                        if error.code() == afs_error::NODE_TRANSFER_UNSUPPORTED =>
                    {
                        afs_logging::warn!(
                            "ownerfs.rdma_read_fallback";
                            "code" => error.code().to_string(),
                            "error" => error.to_string()
                        );
                    }
                    (DataMode::Auto, Err(error)) => return Err(error),
                    (DataMode::Grpc, _) => unreachable!("checked above"),
                }
            }
            let length = out.len();
            let mut client = self.cloned_client()?;
            let reply = self.runtime.block_on(async move {
                client
                    .read(request_with_current_context(OwnerReadRequest {
                        access: Some(root_access(grant)),
                        handle: Some(file_handle(file)),
                        offset,
                        length: length as u32,
                        plane: Some(grpc_plane()),
                    }))
                    .await
                    .map_err(PeerError::from)
                    .map(|reply| reply.into_inner())
            })?;
            if reply.read as usize != reply.data.len() || reply.data.len() > length {
                return Err(PeerError::coded(
                    afs_error::CLIENT_PROTOCOL_VIOLATION,
                    "OwnerReadReply shape mismatch",
                ));
            }
            if !reply.data_checksum.is_empty()
                && blake3::hash(&reply.data).as_bytes().as_slice() != reply.data_checksum.as_slice()
            {
                return Err(PeerError::coded(
                    afs_error::NODE_TRANSFER_CORRUPT_DATA,
                    "OwnerReadReply checksum mismatch",
                ));
            }
            out[..reply.data.len()].copy_from_slice(&reply.data);
            if let Some(metrics) = &self.metrics {
                metrics.record_payload("client", "read", "grpc", reply.data.len() as u64);
            }
            Ok(reply.data.len())
        })();
        if let Some(metrics) = &self.metrics {
            metrics.observe("client", "read", started.elapsed());
        }
        result.map_err(|error| error.0)
    }

    fn write_with_options(
        &self,
        grant: &RootGrant,
        file: &RemoteFile,
        offset: u64,
        data: &[u8],
        options: WriteOptions,
    ) -> afs_error::Result<usize> {
        validate_length(data.len()).map_err(|error| error.0)?;
        let started = std::time::Instant::now();
        let result = (|| -> PeerResult<usize> {
            if !data.is_empty() && matches!(self.data_mode, DataMode::Rdma | DataMode::Auto) {
                let negotiated = self.runtime.block_on(self.negotiate_owner_rdma(grant));
                match (self.data_mode, negotiated) {
                    (_, Ok(window)) => {
                        let written = self.runtime.block_on(self.owner_write_with_rdma_window(
                            window, grant, file, offset, data, options,
                        ))?;
                        if let Some(metrics) = &self.metrics {
                            metrics.record_payload("client", "write", "rdma", written as u64);
                        }
                        return Ok(written);
                    }
                    (DataMode::Rdma, Err(error)) => return Err(error),
                    (DataMode::Auto, Err(error))
                        if error.code() == afs_error::NODE_TRANSFER_UNSUPPORTED =>
                    {
                        afs_logging::warn!(
                            "ownerfs.rdma_write_fallback";
                            "code" => error.code().to_string(),
                            "error" => error.to_string()
                        );
                    }
                    (DataMode::Auto, Err(error)) => return Err(error),
                    (DataMode::Grpc, _) => unreachable!("checked above"),
                }
            }
            let len = data.len();
            let mut client = self.cloned_client()?;
            let reply = self.runtime.block_on(async move {
                client
                    .write(request_with_current_context(OwnerWriteRequest {
                        access: Some(root_access(grant)),
                        handle: Some(file_handle(file)),
                        offset,
                        data: data.to_vec(),
                        length: len as u32,
                        plane: Some(grpc_plane()),
                        kill_suidgid: options.kill_suidgid,
                        data_checksum: if data.is_empty() {
                            Vec::new()
                        } else {
                            blake3::hash(data).as_bytes().to_vec()
                        },
                    }))
                    .await
                    .map_err(PeerError::from)
                    .map(|reply| reply.into_inner())
            })?;
            if reply.written as usize > len {
                return Err(PeerError::coded(
                    afs_error::CLIENT_PROTOCOL_VIOLATION,
                    "OwnerWriteReply count exceeds request length",
                ));
            }
            if let Some(metrics) = &self.metrics {
                metrics.record_payload("client", "write", "grpc", reply.written as u64);
            }
            Ok(reply.written as usize)
        })();
        if let Some(metrics) = &self.metrics {
            metrics.observe("client", "write", started.elapsed());
        }
        result.map_err(|error| error.0)
    }

    fn flush(&self, grant: &RootGrant, file: &RemoteFile) -> afs_error::Result<()> {
        let _reply = owner_rpc!(
            self,
            flush,
            OwnerFlushRequest {
                access: Some(root_access(grant)),
                handle: Some(file_handle(file)),
            }
        );
        Ok(())
    }

    fn fsync(
        &self,
        grant: &RootGrant,
        file: &RemoteFile,
        data_only: bool,
    ) -> afs_error::Result<()> {
        let _reply = owner_rpc!(
            self,
            fsync,
            OwnerFsyncRequest {
                access: Some(root_access(grant)),
                handle: Some(file_handle(file)),
                datasync: data_only,
            }
        );
        Ok(())
    }

    fn release(&self, grant: &RootGrant, file: RemoteFile) -> afs_error::Result<()> {
        self.prefetched_reads
            .lock()
            .map_err(|_| protocol_error("OwnerFiles prefetch cache lock poisoned"))?
            .remove(&file.handle);
        let request = OwnerReleaseRequest {
            access: Some(root_access(grant)),
            handle: Some(OwnerHandle {
                opaque: file.handle,
            }),
        };
        // All writes and explicit sync/flush calls have already received their
        // own result before this cleanup request. A FUSE RELEASE is not a
        // durability barrier; waiting for its RPC only adds close latency. We
        // still keep the handle in a bounded background job so transient
        // transport failures do not leak Home-side opens after a single packet
        // loss or reconnect window.
        if let OwnerRuntime::Existing(runtime) = &self.runtime
            && reserve_release_slot(&self.pending_releases)
        {
            let mut client = match self.cloned_client() {
                Ok(client) => client,
                Err(error) => {
                    self.pending_releases.fetch_sub(1, AtomicOrdering::AcqRel);
                    return Err(error.0);
                }
            };
            let pending = self.pending_releases.clone();
            let metrics = self.metrics.clone();
            runtime.spawn(async move {
                if let Err(error) = release_with_retry(&mut client, request, metrics).await {
                    afs_logging::warn!(
                        "ownerfs.release_failed";
                        "error" => error.to_string(),
                        "code" => error.code().to_string()
                    );
                }
                pending.fetch_sub(1, AtomicOrdering::AcqRel);
            });
            return Ok(());
        }
        let mut client = self.cloned_client().map_err(|error| error.0)?;
        self.runtime
            .block_on(release_with_retry(
                &mut client,
                request,
                self.metrics.clone(),
            ))
            .map_err(Into::into)
    }

    fn opendir(
        &self,
        grant: &RootGrant,
        path: &OsStr,
        expected_identity: Option<&FileIdentity>,
    ) -> afs_error::Result<RemoteDirectory> {
        let reply = owner_rpc!(
            self,
            opendir,
            OwnerOpendirRequest {
                access: Some(root_access(grant)),
                path: path.as_bytes().to_vec(),
                expected_file_identity: expected_identity.map(file_identity),
            }
        );
        let identity = reply
            .file_identity
            .ok_or_else(|| protocol_error("OwnerOpendirReply missing file_identity"))?;
        let handle = reply
            .handle
            .ok_or_else(|| protocol_error("OwnerOpendirReply missing handle"))?;
        Ok(RemoteDirectory {
            root_id: grant.id.clone(),
            owner_node_id: grant.home_node_id.clone(),
            owner_session_id: reply.owner_session_id,
            identity: FileIdentity(identity.opaque),
            handle: handle.opaque,
        })
    }

    fn readdir(
        &self,
        grant: &RootGrant,
        directory: &RemoteDirectory,
        cookie: u64,
        max_entries: usize,
    ) -> afs_error::Result<Vec<RemoteDirectoryEntry>> {
        let reply = owner_rpc!(
            self,
            readdir,
            OwnerReaddirRequest {
                access: Some(root_access(grant)),
                handle: Some(directory_handle(directory)),
                offset: cookie,
                max_entries: max_entries as u32,
            }
        );
        reply
            .entries
            .into_iter()
            .map(|entry| {
                Ok(RemoteDirectoryEntry {
                    name: OsString::from_vec(entry.name),
                    entry: owner_entry(grant, entry.attr)?,
                    next_cookie: entry.next_offset,
                })
            })
            .collect()
    }

    fn fsyncdir(
        &self,
        grant: &RootGrant,
        directory: &RemoteDirectory,
        data_only: bool,
    ) -> afs_error::Result<()> {
        let _reply = owner_rpc!(
            self,
            fsync_dir,
            OwnerFsyncDirRequest {
                access: Some(root_access(grant)),
                handle: Some(directory_handle(directory)),
                datasync: data_only,
            }
        );
        Ok(())
    }

    fn releasedir(&self, grant: &RootGrant, directory: RemoteDirectory) -> afs_error::Result<()> {
        let _reply = owner_rpc!(
            self,
            release_dir,
            OwnerReleaseDirRequest {
                access: Some(root_access(grant)),
                handle: Some(OwnerDirectoryHandle {
                    opaque: directory.handle,
                }),
            }
        );
        Ok(())
    }
}

#[cfg(feature = "ownerfs")]
fn reserve_release_slot(pending: &AtomicUsize) -> bool {
    pending
        .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |current| {
            (current < MAX_PENDING_RELEASES).then(|| current + 1)
        })
        .is_ok()
}

#[cfg(feature = "ownerfs")]
async fn release_with_retry(
    client: &mut OwnerFilesClient<Channel>,
    request: OwnerReleaseRequest,
    metrics: Option<super::OwnerRpcMetrics>,
) -> PeerResult<()> {
    let mut backoff = RELEASE_INITIAL_BACKOFF;
    for attempt in 1..=RELEASE_MAX_ATTEMPTS {
        let started = Instant::now();
        let result = client
            .release(request_with_current_context(request.clone()))
            .await
            .map(|_| ())
            .map_err(PeerError::from);
        if let Some(metrics) = &metrics {
            metrics.observe("client", "release", started.elapsed());
        }
        match result {
            Ok(()) => return Ok(()),
            Err(error) if is_idempotent_release_success(&error) => return Ok(()),
            Err(error) if attempt < RELEASE_MAX_ATTEMPTS && is_transient_release_error(&error) => {
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(RELEASE_MAX_BACKOFF);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("release retry loop always returns from its final attempt")
}

#[cfg(feature = "ownerfs")]
fn is_idempotent_release_success(error: &PeerError) -> bool {
    error.code() == afs_error::NODE_OWNER_STALE_HANDLE
        || error.kind() == afs_error::ErrorKind::NotFound
}

#[cfg(feature = "ownerfs")]
fn is_transient_release_error(error: &PeerError) -> bool {
    matches!(
        error.kind(),
        afs_error::ErrorKind::Unavailable
            | afs_error::ErrorKind::DeadlineExceeded
            | afs_error::ErrorKind::ResourceExhausted
            | afs_error::ErrorKind::Aborted
    )
}

impl DataPeerClient {
    pub async fn read(&mut self, name: &str, offset: u64, length: u32) -> PeerResult<Vec<u8>> {
        match &mut self.inner {
            DataPeerClientInner::Grpc(client) => client.read(name, offset, length).await,
            #[cfg(feature = "rdma")]
            DataPeerClientInner::Rdma(client) => client.read(name, offset, length).await,
        }
    }

    pub async fn write(&mut self, name: &str, offset: u64, data: Vec<u8>) -> PeerResult<u32> {
        match &mut self.inner {
            DataPeerClientInner::Grpc(client) => client.write(name, offset, data).await,
            #[cfg(feature = "rdma")]
            DataPeerClientInner::Rdma(client) => client.write(name, offset, data).await,
        }
    }

    pub async fn close(&mut self) -> PeerResult<()> {
        match &mut self.inner {
            DataPeerClientInner::Grpc(_) => Ok(()),
            #[cfg(feature = "rdma")]
            DataPeerClientInner::Rdma(client) => client.close().await,
        }
    }

    #[must_use]
    pub fn mode(&self) -> &'static str {
        match &self.inner {
            DataPeerClientInner::Grpc(client) => client.reported_mode,
            #[cfg(feature = "rdma")]
            DataPeerClientInner::Rdma(client) => client.reported_mode,
        }
    }
}

/// 根据配置建立数据客户端。
///
/// Auto 只允许在 RDMA 建连阶段失败时回退；一旦业务 read/write 已发出，
/// 结果未知就不能自动改走 gRPC 重放。
pub async fn connect_data_client(options: DataClientOptions) -> PeerResult<DataPeerClient> {
    let channel = connect_channel(&options.endpoint, options.timeout, options.tls.clone()).await?;
    match options.mode {
        DataMode::Grpc => Ok(DataPeerClient {
            inner: DataPeerClientInner::Grpc(GrpcInlineClient::new(channel, "grpc")),
        }),
        DataMode::Rdma => connect_rdma(channel, options.rdma_device, "rdma").await,
        DataMode::Auto => match connect_rdma(channel.clone(), options.rdma_device, "rdma").await {
            Ok(client) => Ok(client),
            Err(_) => Ok(DataPeerClient {
                inner: DataPeerClientInner::Grpc(GrpcInlineClient::new(channel, "grpc")),
            }),
        },
    }
}

/// gRPC inline adapter：命令和内容都在 node_data proto 中传输。
struct GrpcInlineClient {
    data: NodeDataClient<Channel>,
    reported_mode: &'static str,
}

impl GrpcInlineClient {
    fn new(channel: Channel, reported_mode: &'static str) -> Self {
        Self {
            data: NodeDataClient::new(channel),
            reported_mode,
        }
    }

    /// gRPC 读文件：命令携带文件名和范围，响应的 data 字段直接携带内容。
    /// 不创建 RDMA endpoint，也不执行 NegotiateData 或 RDMA 就绪探测。
    async fn read(&mut self, name: &str, offset: u64, length: u32) -> PeerResult<Vec<u8>> {
        validate_length(length as usize)?;
        let reply = self
            .data
            .read(request_with_current_context(DataReadRequest {
                session_id: 0,
                transfer: DataTransfer::GrpcInline.into(),
                name: name.to_string(),
                offset,
                length,
            }))
            .await?
            .into_inner();
        if reply.length != length || reply.data.len() != length as usize {
            return Err(PeerError::coded(
                afs_error::CLIENT_PROTOCOL_VIOLATION,
                "gRPC read reply shape mismatch",
            ));
        }
        Ok(reply.data)
    }

    /// gRPC 写文件：命令的 data 字段携带全部内容，服务端写 Storage 后回复写入长度。
    /// Vec 在 Rust 中移入 Proto message；没有经过 RDMA 注册内存。
    async fn write(&mut self, name: &str, offset: u64, data: Vec<u8>) -> PeerResult<u32> {
        validate_length(data.len())?;
        let len = data.len();
        let reply = self
            .data
            .write(request_with_current_context(DataWriteRequest {
                session_id: 0,
                transfer: DataTransfer::GrpcInline.into(),
                name: name.to_string(),
                offset,
                length: data.len() as u32,
                data,
            }))
            .await?
            .into_inner();
        if reply.written as usize != len {
            return Err(PeerError::coded(
                afs_error::CLIENT_PROTOCOL_VIOLATION,
                "gRPC write reply count mismatch",
            ));
        }
        Ok(reply.written)
    }
}

#[cfg(feature = "rdma")]
/// RDMA adapter：命令走 gRPC，内容走已协商好的 RDMA endpoint。
///
/// `operation` 串行化同一 endpoint 上的读写，避免同一 MR 同时被两次 DMA 使用。
/// `poisoned` 是 fail-closed 开关：取消、CQ 错误、reply 形状错误都会让后续请求失败。
struct RdmaDataClient {
    data: NodeDataClient<Channel>,
    control: NodeControlClient<Channel>,
    endpoint: Arc<Mutex<RdmaEndpoint>>,
    operation: Arc<Mutex<()>>,
    poisoned: Arc<AtomicBool>,
    session_id: u64,
    reported_mode: &'static str,
    closed: bool,
}

#[cfg(feature = "rdma")]
impl RdmaDataClient {
    /// 建立 RDMA 数据客户端：
    /// 1. 本地 open endpoint + 导出 client_info；
    /// 2. gRPC NegotiateData 交给对端 control.rs；
    /// 3. 用返回的 server_info 连接 QP；
    /// 4. 通过 RDMA SEND_WITH_IMM 探测证明链路可用，发送完成后才发读写命令。
    ///
    /// 服务端首次数据请求消费接收完成；没有第二条 ReadyData gRPC，也没有每次重建连接。
    async fn connect(
        channel: Channel,
        device: String,
        reported_mode: &'static str,
    ) -> PeerResult<Self> {
        let mut endpoint = RdmaEndpoint::open(&device)
            .map_err(|error| PeerError::coded(afs_error::NODE_TRANSFER_UNAVAILABLE, error.0))?;
        let info = endpoint
            .info()
            .map_err(|error| PeerError::coded(afs_error::NODE_TRANSFER_UNAVAILABLE, error.0))?;
        let mut control = NodeControlClient::new(channel.clone());
        let negotiate = control
            .negotiate_data(request_with_current_context(NegotiateDataRequest {
                client_info: info.to_vec(),
                capacity: CAPACITY as u32,
                handshake_version: RDMA_HANDSHAKE_VERSION,
            }))
            .await?
            .into_inner();
        if !negotiate.rdma_supported {
            return Err(PeerError::coded(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                "peer does not support RDMA",
            ));
        }
        if negotiate.handshake_version != RDMA_HANDSHAKE_VERSION {
            close_session_best_effort(&mut control, negotiate.session_id).await;
            return Err(PeerError::coded(
                afs_error::NODE_RDMA_HANDSHAKE_VERSION,
                "peer uses unsupported RDMA handshake version",
            ));
        }
        let server_info = negotiate.server_info;
        // CQ 轮询属于阻塞 I/O；工作任务独占 endpoint，取消外层 future 不会释放在途资源。
        let connected = tokio::task::spawn_blocking(move || {
            endpoint.connect(&server_info)?;
            endpoint.send_probe(5000)?;
            Ok::<_, afs_transport::rdma::RdmaError>(endpoint)
        })
        .await
        .map_err(|error| PeerError::coded(afs_error::NODE_TRANSFER_UNAVAILABLE, error.to_string()))
        .and_then(|result| {
            result.map_err(|error| {
                PeerError::coded(afs_error::NODE_TRANSFER_UNAVAILABLE, error.to_string())
            })
        });
        let endpoint = match connected {
            Ok(endpoint) => endpoint,
            Err(error) => {
                close_session_best_effort(&mut control, negotiate.session_id).await;
                return Err(error);
            }
        };
        Ok(Self {
            data: NodeDataClient::new(channel),
            control,
            endpoint: Arc::new(Mutex::new(endpoint)),
            operation: Arc::new(Mutex::new(())),
            poisoned: Arc::new(AtomicBool::new(false)),
            session_id: negotiate.session_id,
            reported_mode,
            closed: false,
        })
    }

    /// RDMA 读文件客户端流程：
    /// 1. 发 node_data Read 命令，告诉服务端文件名、offset、len、session_id；
    /// 2. 服务端从 Storage 读文件，并 RDMA WRITE 到客户端 MR；
    /// 3. gRPC reply 返回后，客户端从本地 MR `get_local` 拷贝 bytes。
    async fn read(&mut self, name: &str, offset: u64, length: u32) -> PeerResult<Vec<u8>> {
        validate_open(self.closed, &self.poisoned)?;
        validate_length(length as usize)?;
        let cancel_guard = CancelPoisonGuard::new(self.poisoned.clone());
        let mut data_client = self.data.clone();
        let operation = self.operation.clone();
        let endpoint = self.endpoint.clone();
        let poisoned = self.poisoned.clone();
        let name = name.to_string();
        let session_id = self.session_id;
        let result = tokio::spawn(
            async move {
                let _operation = operation.lock_owned().await;
                validate_open(false, &poisoned)?;
                let reply = data_client
                    .read(request_with_current_context(DataReadRequest {
                        session_id,
                        transfer: DataTransfer::RdmaOneSided.into(),
                        name,
                        offset,
                        length,
                    }))
                    .await
                    .map_err(|error| {
                        poisoned.store(true, Ordering::SeqCst);
                        PeerError::from(error)
                    })?
                    .into_inner();
                if !reply.data.is_empty() || reply.length != length {
                    return Err(poison(
                        &poisoned,
                        afs_error::CLIENT_PROTOCOL_VIOLATION,
                        "RDMA read reply shape mismatch",
                    ));
                }
                let mut endpoint = endpoint.lock().await;
                endpoint.get_local(length as usize).map_err(|error| {
                    poison(&poisoned, afs_error::NODE_TRANSFER_UNAVAILABLE, error.0)
                })
            }
            .in_current_span(),
        )
        .await
        .map_err(|error| PeerError::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string()))?;
        if result.is_ok() {
            cancel_guard.disarm();
        }
        result
    }

    /// RDMA 写文件客户端流程：
    /// 1. 客户端先把待写 bytes `put_local` 到自己的 MR；
    /// 2. 发 node_data Write 命令，proto 带文件名、范围和会话，不带文件内容；
    /// 3. 服务端 RDMA READ 拉取客户端 MR 内容并写 Storage；
    /// 4. reply.written 必须等于 len，否则 poison。
    async fn write(&mut self, name: &str, offset: u64, data: Vec<u8>) -> PeerResult<u32> {
        validate_open(self.closed, &self.poisoned)?;
        validate_length(data.len())?;
        let cancel_guard = CancelPoisonGuard::new(self.poisoned.clone());
        let mut data_client = self.data.clone();
        let operation = self.operation.clone();
        let endpoint = self.endpoint.clone();
        let poisoned = self.poisoned.clone();
        let name = name.to_string();
        let len = data.len();
        let session_id = self.session_id;
        let result = tokio::spawn(
            async move {
                let _operation = operation.lock_owned().await;
                validate_open(false, &poisoned)?;
                {
                    let mut endpoint = endpoint.lock().await;
                    endpoint.put_local(&data).map_err(|error| {
                        poison(&poisoned, afs_error::NODE_TRANSFER_UNAVAILABLE, error.0)
                    })?;
                }
                let reply = data_client
                    .write(request_with_current_context(DataWriteRequest {
                        session_id,
                        transfer: DataTransfer::RdmaOneSided.into(),
                        name,
                        offset,
                        data: Vec::new(),
                        length: len as u32,
                    }))
                    .await
                    .map_err(|error| {
                        poisoned.store(true, Ordering::SeqCst);
                        PeerError::from(error)
                    })?
                    .into_inner();
                if reply.written as usize != len {
                    return Err(poison(
                        &poisoned,
                        afs_error::CLIENT_PROTOCOL_VIOLATION,
                        "RDMA write reply count mismatch",
                    ));
                }
                Ok(reply.written)
            }
            .in_current_span(),
        )
        .await
        .map_err(|error| PeerError::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string()))?;
        if result.is_ok() {
            cancel_guard.disarm();
        }
        result
    }

    async fn close(&mut self) -> PeerResult<()> {
        if self.closed {
            return Ok(());
        }
        let _operation = self.operation.clone().lock_owned().await;
        self.closed = true;
        self.poisoned.store(true, Ordering::SeqCst);
        self.control
            .close_data(request_with_current_context(CloseDataRequest {
                session_id: self.session_id,
            }))
            .await?;
        Ok(())
    }
}

#[cfg(feature = "rdma")]
impl Drop for RdmaDataClient {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        let mut control = self.control.clone();
        let session_id = self.session_id;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = control
                    .close_data(request_with_current_context(CloseDataRequest {
                        session_id,
                    }))
                    .await;
            });
        }
    }
}

#[cfg(feature = "rdma")]
async fn close_session_best_effort(control: &mut NodeControlClient<Channel>, session_id: u64) {
    let _ = control
        .close_data(request_with_current_context(CloseDataRequest {
            session_id,
        }))
        .await;
}

#[cfg(feature = "rdma")]
async fn connect_rdma(
    channel: Channel,
    device: Option<String>,
    reported_mode: &'static str,
) -> PeerResult<DataPeerClient> {
    let device = device.ok_or_else(|| {
        PeerError::coded(
            afs_error::CLIENT_ARGUMENT_INVALID,
            "RDMA mode requires a device",
        )
    })?;
    Ok(DataPeerClient {
        inner: DataPeerClientInner::Rdma(Box::new(
            RdmaDataClient::connect(channel, device, reported_mode).await?,
        )),
    })
}

#[cfg(not(feature = "rdma"))]
async fn connect_rdma(
    _channel: Channel,
    _device: Option<String>,
    _reported_mode: &'static str,
) -> PeerResult<DataPeerClient> {
    Err(PeerError::coded(
        afs_error::NODE_TRANSFER_UNSUPPORTED,
        "RDMA feature is not enabled",
    ))
}

#[cfg(feature = "ownerfs")]
fn owner_caller(ctx: &RequestContext) -> afs_protocol::node_data::OwnerCaller {
    afs_protocol::node_data::OwnerCaller {
        uid: ctx.uid,
        gid: ctx.gid,
        pid: ctx.pid,
        umask: ctx.umask,
        supplementary_gids: ctx.supplementary_gids.clone(),
    }
}

#[cfg(feature = "ownerfs")]
fn root_access(grant: &RootGrant) -> RootAccess {
    RootAccess {
        root_id: grant.id.0.clone(),
        root_epoch: grant.epoch,
        access_generation: grant.access_generation,
        holder_node_id: grant.holder_node_id.clone(),
        home_node_id: grant.home_node_id.clone(),
        session_id: grant.session_id.clone(),
        fencing_token: grant.fencing_token.clone(),
        home_session_id: grant.home_session_id.clone(),
    }
}

#[cfg(feature = "ownerfs")]
fn file_identity(identity: &FileIdentity) -> PbFileIdentity {
    PbFileIdentity {
        opaque: identity.0.clone(),
    }
}

#[cfg(feature = "ownerfs")]
fn file_handle(file: &RemoteFile) -> OwnerHandle {
    OwnerHandle {
        opaque: file.handle.clone(),
    }
}

#[cfg(feature = "ownerfs")]
fn directory_handle(directory: &RemoteDirectory) -> OwnerDirectoryHandle {
    OwnerDirectoryHandle {
        opaque: directory.handle.clone(),
    }
}

#[cfg(feature = "ownerfs")]
fn grpc_plane() -> DataPlane {
    DataPlane {
        transfer: DataTransfer::GrpcInline.into(),
        rdma_session_id: 0,
        buffer_offset: 0,
    }
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
fn rdma_plane(session_id: u64) -> DataPlane {
    DataPlane {
        transfer: DataTransfer::RdmaOneSided.into(),
        rdma_session_id: session_id,
        buffer_offset: 0,
    }
}

#[cfg(feature = "ownerfs")]
fn owner_special_node(kind: SpecialFileKind) -> afs_protocol::node_data::OwnerSpecialNode {
    let (kind, rdev) = match kind {
        SpecialFileKind::Fifo => (OwnerFileKind::Fifo, 0),
        SpecialFileKind::Socket => (OwnerFileKind::Socket, 0),
        SpecialFileKind::BlockDevice { rdev } => (OwnerFileKind::BlockDevice, rdev),
        SpecialFileKind::CharDevice { rdev } => (OwnerFileKind::CharDevice, rdev),
    };
    afs_protocol::node_data::OwnerSpecialNode {
        kind: kind.into(),
        rdev,
    }
}

#[cfg(feature = "ownerfs")]
fn remote_file(
    grant: &RootGrant,
    identity: FileIdentity,
    handle: Vec<u8>,
    owner_session_id: String,
) -> RemoteFile {
    RemoteFile {
        root_id: grant.id.clone(),
        owner_node_id: grant.home_node_id.clone(),
        owner_session_id,
        identity,
        handle,
    }
}

#[cfg(feature = "ownerfs")]
fn owner_entry(grant: &RootGrant, attr: Option<OwnerFileAttr>) -> afs_error::Result<OwnerEntry> {
    let attr = attr.ok_or_else(|| protocol_error("OwnerFileAttr missing"))?;
    let identity = attr
        .identity
        .clone()
        .ok_or_else(|| protocol_error("OwnerFileAttr missing identity"))?;
    Ok(OwnerEntry {
        root_id: grant.id.clone(),
        identity: FileIdentity(identity.opaque),
        attributes: file_attributes(attr)?,
    })
}

#[cfg(feature = "ownerfs")]
fn file_attributes(attr: OwnerFileAttr) -> afs_error::Result<FileAttributes> {
    let kind = match OwnerFileKind::try_from(attr.kind)
        .map_err(|_| protocol_error("unknown OwnerFileKind"))?
    {
        OwnerFileKind::Regular => FileKind::Regular,
        OwnerFileKind::Directory => FileKind::Directory,
        OwnerFileKind::Symlink => FileKind::Symlink,
        OwnerFileKind::Fifo
        | OwnerFileKind::Socket
        | OwnerFileKind::BlockDevice
        | OwnerFileKind::CharDevice => FileKind::Special(domain_owner_special_node(
            attr.kind,
            attr.special_node.as_ref(),
        )?),
        OwnerFileKind::Unspecified => {
            return Err(protocol_error("OwnerFileKind is unspecified"));
        }
    };
    Ok(FileAttributes {
        kind,
        size: attr.size,
        blocks: attr.blocks,
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        nlink: attr.nlink,
        atime: ns_to_time(attr.atime_ns),
        mtime: ns_to_time(attr.mtime_ns),
        ctime: ns_to_time(attr.ctime_ns),
    })
}

#[cfg(feature = "ownerfs")]
fn filesystem_capacity(
    capacity: Option<afs_protocol::node_data::OwnerFilesystemCapacity>,
) -> afs_error::Result<FilesystemCapacity> {
    let capacity = capacity.ok_or_else(|| protocol_error("OwnerStatFsReply missing capacity"))?;
    if capacity.bsize == 0 || capacity.frsize == 0 || capacity.namelen == 0 {
        return Err(protocol_error("OwnerStatFsReply capacity shape is invalid"));
    }
    if capacity.blocks < capacity.bfree || capacity.bfree < capacity.bavail {
        return Err(protocol_error("OwnerStatFsReply free blocks are invalid"));
    }
    if capacity.files < capacity.ffree {
        return Err(protocol_error("OwnerStatFsReply free files are invalid"));
    }
    Ok(FilesystemCapacity {
        blocks: capacity.blocks,
        bfree: capacity.bfree,
        bavail: capacity.bavail,
        files: capacity.files,
        ffree: capacity.ffree,
        bsize: capacity.bsize,
        namelen: capacity.namelen,
        frsize: capacity.frsize,
    })
}

#[cfg(feature = "ownerfs")]
fn domain_owner_special_node(
    attr_kind: i32,
    special: Option<&afs_protocol::node_data::OwnerSpecialNode>,
) -> afs_error::Result<SpecialFileKind> {
    let attr_kind =
        OwnerFileKind::try_from(attr_kind).map_err(|_| protocol_error("unknown OwnerFileKind"))?;
    let special = special.ok_or_else(|| protocol_error("OwnerFileAttr missing special_node"))?;
    let special_kind = OwnerFileKind::try_from(special.kind)
        .map_err(|_| protocol_error("unknown OwnerSpecialNode kind"))?;
    if special_kind != attr_kind {
        return Err(protocol_error(
            "OwnerFileAttr kind differs from special_node kind",
        ));
    }
    match special_kind {
        OwnerFileKind::Fifo if special.rdev == 0 => Ok(SpecialFileKind::Fifo),
        OwnerFileKind::Socket if special.rdev == 0 => Ok(SpecialFileKind::Socket),
        OwnerFileKind::BlockDevice => Ok(SpecialFileKind::BlockDevice { rdev: special.rdev }),
        OwnerFileKind::CharDevice => Ok(SpecialFileKind::CharDevice { rdev: special.rdev }),
        _ => Err(protocol_error(
            "OwnerSpecialNode has an invalid kind/rdev combination",
        )),
    }
}

#[cfg(feature = "ownerfs")]
fn owner_set_attr(change: &AttributeChange, options: SetAttrOptions) -> OwnerSetAttr {
    OwnerSetAttr {
        mode: change.mode,
        uid: change.uid,
        gid: change.gid,
        size: change.size,
        atime_ns: change.atime.map(time_to_ns),
        mtime_ns: change.mtime.map(time_to_ns),
        kill_suidgid: options.kill_suidgid,
        timestamps_now: options.timestamps_now,
    }
}

#[cfg(feature = "ownerfs")]
fn ns_to_time(ns: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_nanos(ns)
}

#[cfg(feature = "ownerfs")]
fn time_to_ns(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(feature = "ownerfs")]
fn protocol_error(message: &'static str) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_PROTOCOL_VIOLATION, message)
}

async fn connect_channel(
    endpoint: &str,
    timeout: Duration,
    tls: afs_transport::TlsConfig,
) -> PeerResult<Channel> {
    let endpoint = Endpoint::from_shared(endpoint.to_string())?
        .connect_timeout(timeout)
        .timeout(timeout);
    let endpoint = afs_transport::SecurityManager::new(tls)
        .map_err(|error| PeerError::coded(afs_error::CLIENT_ARGUMENT_INVALID, error.to_string()))?
        .configure_client(endpoint)
        .map_err(|error| PeerError::coded(afs_error::CLIENT_ARGUMENT_INVALID, error.to_string()))?;
    Ok(endpoint.connect().await?)
}

fn validate_length(length: usize) -> PeerResult<()> {
    if length > MAX_TRANSFER_BYTES {
        return Err(PeerError::coded(
            afs_error::CLIENT_ARGUMENT_INVALID,
            "transfer exceeds 1MiB",
        ));
    }
    Ok(())
}

#[cfg(feature = "rdma")]
fn validate_open(closed: bool, poisoned: &AtomicBool) -> PeerResult<()> {
    if closed {
        return Err(PeerError::coded(
            afs_error::NODE_RDMA_CLOSED,
            "RDMA session is closed",
        ));
    }
    if poisoned.load(Ordering::SeqCst) {
        return Err(PeerError::coded(
            afs_error::NODE_RDMA_SESSION_POISONED,
            "RDMA session is poisoned",
        ));
    }
    Ok(())
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
fn owner_rdma_error(error: afs_transport::rdma::RdmaError) -> PeerError {
    PeerError::coded(afs_error::NODE_TRANSFER_UNAVAILABLE, error.to_string())
}

#[cfg(feature = "rdma")]
fn poison(
    poisoned: &AtomicBool,
    code: afs_error::ErrorCode,
    message: impl Into<String>,
) -> PeerError {
    poisoned.store(true, Ordering::SeqCst);
    PeerError::coded(code, message)
}

#[cfg(feature = "rdma")]
/// 取消保护：调用者 drop read/write future 时，把 session 标成 poisoned。
///
/// 内部 spawned task 仍持有 endpoint Arc，保证 DMA/CQ 生命周期不会因为外层 future
/// 取消而提前释放；但业务结果已经对调用者未知，所以后续复用必须失败。
struct CancelPoisonGuard {
    poisoned: Arc<AtomicBool>,
    disarmed: bool,
}

#[cfg(feature = "rdma")]
impl CancelPoisonGuard {
    fn new(poisoned: Arc<AtomicBool>) -> Self {
        Self {
            poisoned,
            disarmed: false,
        }
    }

    fn disarm(mut self) {
        self.disarmed = true;
    }
}

#[cfg(feature = "rdma")]
impl Drop for CancelPoisonGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            self.poisoned.store(true, Ordering::SeqCst);
        }
    }
}

impl From<PeerError> for afs_error::Error {
    fn from(error: PeerError) -> Self {
        error.0
    }
}

#[cfg(feature = "ownerfs")]
fn owner_lock_wire_handle(
    grant: &RootGrant,
    file: &RemoteFile,
) -> afs_protocol::node_control::OwnerLockHandle {
    afs_protocol::node_control::OwnerLockHandle {
        access: Some(root_access(grant)),
        file: Some(OwnerHandle {
            opaque: file.handle.clone(),
        }),
        identity: Some(file_identity(&file.identity)),
    }
}
#[cfg(feature = "ownerfs")]
fn owner_lock_transport_unknown() -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::IO_OTHER,
        "Owner lock control transport response unknown",
    )
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "dfs")]
    #[test]
    fn replica_data_frames_share_the_staged_allocation_and_legacy_wire() {
        use afs_protocol::node_data::{
            DfsPutReplicaFrame, DfsPutReplicaHeader, dfs_put_replica_frame::Body,
        };
        use prost::Message;

        #[derive(Clone, PartialEq, prost::Message)]
        struct LegacyFrame {
            #[prost(oneof = "LegacyBody", tags = "1, 2")]
            body: Option<LegacyBody>,
        }

        #[derive(Clone, PartialEq, prost::Oneof)]
        enum LegacyBody {
            #[prost(message, tag = "1")]
            Header(DfsPutReplicaHeader),
            #[prost(bytes = "vec", tag = "2")]
            Data(Vec<u8>),
        }

        let payload: Vec<u8> = (0..150 * 1024).map(|index| (index % 251) as u8).collect();
        let staged = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("replica-shared-wire"),
            payload.clone(),
        );
        let start = staged.bytes().as_ptr() as usize;
        let mut offset = 0;
        let mut count = 0;
        for frame in replica_data_frames(staged.clone()) {
            let Some(Body::Data(data)) = &frame.body else {
                panic!("replica data iterator returned a control frame");
            };
            assert_eq!(data.as_ptr() as usize, start + offset);
            assert_eq!(
                data.len(),
                (payload.len() - offset).min(super::super::data::DFS_REPLICA_FRAME_BYTES)
            );
            assert_eq!(&data[..], &payload[offset..offset + data.len()]);
            let legacy = LegacyFrame {
                body: Some(LegacyBody::Data(data.to_vec())),
            };
            let old_wire = legacy.encode_to_vec();
            let new_wire = frame.encode_to_vec();
            assert_eq!(new_wire, old_wire);
            assert_eq!(
                DfsPutReplicaFrame::decode(old_wire.as_slice()).unwrap(),
                frame
            );
            assert_eq!(LegacyFrame::decode(new_wire.as_slice()).unwrap(), legacy);
            offset += data.len();
            count += 1;
        }
        assert_eq!(count, 3);
        assert_eq!(offset, payload.len());
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn cancelled_replica_frames_retain_allocation_and_all_write_leases_until_last_owner() {
        use crate::node::chunk::{
            ChunkBuilder, DeferredWriteBudget, StagedChunk, WriteBudgetReservation, WriteBufferPool,
        };
        use afs_protocol::node_data::dfs_put_replica_frame::Body;

        let capacity = 3 * super::super::data::DFS_REPLICA_FRAME_BYTES;
        for stop_pool in [false, true] {
            let pool = WriteBufferPool::new(capacity);
            let mut builder = ChunkBuilder::with_pool(pool.clone(), capacity).unwrap();
            let payload = vec![37; capacity - 17];
            builder.write_at(0, &payload).unwrap();
            let frozen = builder.freeze();
            let generation = frozen.allocation_id();
            let pointer = frozen.as_ptr();
            let mut staged = StagedChunk::from_shared(
                crate::dfs::OperationId::new("replica-frame-lease"),
                frozen,
            );
            let budget = Arc::new(DeferredWriteBudget::default());
            {
                let mut state = budget.state.lock().unwrap();
                state.normal_bytes = capacity as u64;
                state.publisher_bytes = 4096;
                state.descriptor_bytes = 128;
                state.descriptors = 1;
            }
            staged.retain_write_budget(&[WriteBudgetReservation::new(
                budget.clone(),
                capacity as u64,
                4096,
                128,
                1,
            )]);
            let mut frames = replica_data_frames(staged.clone());
            let first = frames.next().unwrap();
            let second = frames.next().unwrap();
            // Cancel the producer with an unsent tail and release the RPC's
            // original/ack owners. Queued or slow frame holders remain alive.
            drop(frames);
            drop(staged);
            let last_holder = second.clone();
            drop(second);
            assert_eq!(pool.snapshot().live_bytes, capacity);
            assert_eq!(pool.snapshot().idle_bytes, 0);
            let Some(Body::Data(data)) = &first.body else {
                panic!("missing data frame")
            };
            assert_eq!(data.as_ptr(), pointer);
            assert_eq!(&data[..], &payload[..data.len()]);
            {
                let state = budget.state.lock().unwrap();
                assert_eq!(
                    (
                        state.normal_bytes,
                        state.publisher_bytes,
                        state.descriptor_bytes,
                        state.descriptors
                    ),
                    (capacity as u64, 4096, 128, 1)
                );
            }
            drop(first);
            assert_eq!(pool.snapshot().live_bytes, capacity);
            assert_eq!(budget.state.lock().unwrap().normal_bytes, capacity as u64);
            if stop_pool {
                pool.stop();
            }
            drop(last_holder);
            assert_eq!(pool.snapshot().live_bytes, 0);
            {
                let state = budget.state.lock().unwrap();
                assert_eq!(
                    (
                        state.normal_bytes,
                        state.publisher_bytes,
                        state.descriptor_bytes,
                        state.descriptors
                    ),
                    (0, 0, 0, 0)
                );
            }
            if stop_pool {
                assert_eq!(pool.snapshot().idle_bytes, 0);
                assert_eq!(pool.snapshot().released_bytes, capacity as u64);
            } else {
                assert_eq!(pool.snapshot().idle_bytes, capacity);
                let reused = ChunkBuilder::with_pool(pool.clone(), capacity).unwrap();
                assert_eq!(reused.data_ptr(), pointer);
                assert_ne!(reused.allocation_id(), generation);
                assert_eq!(pool.snapshot().reuses, 1);
            }
        }
    }

    #[test]
    #[cfg(feature = "rdma")]
    fn poisoning_preserves_failure_category() {
        for code in [
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            afs_error::NODE_TRANSFER_UNAVAILABLE,
        ] {
            let flag = std::sync::atomic::AtomicBool::new(false);
            let error = super::poison(&flag, code, "diagnostic");
            assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
            assert_eq!(error.code(), code);
        }
    }
    use super::*;
    #[cfg(feature = "ownerfs")]
    use crate::node::rpc::data::{
        OwnerFilesHandler, PeerAuthenticator, make_owner_files_server_with_handler,
        make_owner_files_server_with_handler_and_metrics,
    };
    #[cfg(feature = "ownerfs")]
    use crate::node::vfs::ownerfs::{
        files::{FileIdentity, RemoteFile},
        remote::RemoteFiles,
        root::{RootGrant, RootId, RootRight},
    };
    use afs_protocol::node_data::{
        DataReadReply, DataWriteReply,
        node_data_server::{NodeData, NodeDataServer},
    };
    #[cfg(feature = "ownerfs")]
    use afs_protocol::node_data::{
        OwnerReadReply, OwnerReadRequest, OwnerReleaseReply, OwnerWriteReply, OwnerWriteRequest,
    };
    #[cfg(feature = "ownerfs")]
    use std::sync::atomic::{AtomicUsize as TestAtomicUsize, Ordering as TestOrdering};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Request, Response, Status, transport::Server};

    #[cfg(feature = "dfs")]
    type FetchFixtureObjects = std::collections::BTreeMap<String, (ChunkObject, Vec<u8>)>;

    #[cfg(feature = "dfs")]
    #[derive(Clone)]
    struct DiskStreamFixture {
        chunk: ChunkObject,
        body: Arc<Vec<u8>>,
        mode: u8,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        fetch_objects: Option<Arc<FetchFixtureObjects>>,
        fetch_gate: Option<(String, Arc<FetchRpcGate>)>,
    }

    #[cfg(feature = "dfs")]
    #[derive(Default)]
    struct FetchRpcGate {
        entered: std::sync::atomic::AtomicUsize,
        release: std::sync::atomic::AtomicBool,
        changed: tokio::sync::Notify,
    }

    #[cfg(feature = "dfs")]
    fn fetch_fixture_frames(
        request: &DfsReadRangesRequest,
        objects: &FetchFixtureObjects,
    ) -> Result<Vec<afs_protocol::node_data::DfsReadRangesFrame>, Status> {
        use afs_protocol::node_data::{
            DfsReadRangesCompletion, DfsReadRangesFrame, DfsReadRangesHeader,
        };
        let mut frames = Vec::new();
        for operation in &request.operations {
            let (chunk, body) = objects
                .get(&operation.chunk_id)
                .ok_or_else(|| Status::not_found("fixture object"))?;
            let start = operation.chunk_offset as usize;
            let end = start + operation.length as usize;
            let range = body
                .get(start..end)
                .ok_or_else(|| Status::invalid_argument("fixture range"))?;
            let full = start == 0 && operation.length == chunk.length;
            frames.push(DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
                    crc32c_table: Default::default(),
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: operation.operation_index,
                    chunk_id: operation.chunk_id.clone(),
                    chunk_offset: operation.chunk_offset,
                    length: operation.length,
                    source_copy_id: operation.source_copy_id.clone(),
                    blake3_outboard: if full && request.include_blake3_outboard {
                        wi6_encoded_outboard(body).into()
                    } else {
                        Default::default()
                    },
                })),
            });
            for data in range.chunks(STREAM_DATA_BYTES) {
                frames.push(DfsReadRangesFrame {
                    body: Some(dfs_read_ranges_frame::Body::Data(data.to_vec().into())),
                });
            }
            frames.push(DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Completion(
                    DfsReadRangesCompletion {
                        crc32c_table: Default::default(),
                        read_id: request.read_id.clone(),
                        attempt_id: request.attempt_id.clone(),
                        operation_index: operation.operation_index,
                        source_copy_id: operation.source_copy_id.clone(),
                        transferred_bytes: operation.length,
                        range_checksum: blake3::hash(range).as_bytes().to_vec(),
                        range_checksum_algorithm:
                            afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32,
                    },
                )),
            });
        }
        Ok(frames)
    }

    #[cfg(feature = "dfs")]
    #[tonic::async_trait]
    impl afs_protocol::node_data::dfs_chunks_server::DfsChunks for DiskStreamFixture {
        type ReadRangesStream = std::pin::Pin<
            Box<
                dyn tokio_stream::Stream<
                        Item = Result<afs_protocol::node_data::DfsReadRangesFrame, Status>,
                    > + Send,
            >,
        >;
        async fn put_replica_stream(
            &self,
            _request: Request<tonic::Streaming<afs_protocol::node_data::DfsPutReplicaFrame>>,
        ) -> Result<Response<afs_protocol::node_data::DfsPutReplicaReply>, Status> {
            Err(Status::unimplemented("unused fixture RPC"))
        }
        async fn put_replica_rdma(
            &self,
            _request: Request<afs_protocol::node_data::DfsPutReplicaRdmaRequest>,
        ) -> Result<Response<afs_protocol::node_data::DfsPutReplicaReply>, Status> {
            Err(Status::unimplemented("unused fixture RPC"))
        }
        async fn confirm_replica(
            &self,
            _request: Request<afs_protocol::node_data::DfsConfirmReplicaRequest>,
        ) -> Result<Response<afs_protocol::node_data::DfsReplicaAck>, Status> {
            Err(Status::unimplemented("unused fixture RPC"))
        }
        async fn negotiate_rdma(
            &self,
            _request: Request<afs_protocol::node_data::DfsNegotiateRdmaRequest>,
        ) -> Result<Response<afs_protocol::node_data::DfsNegotiateRdmaReply>, Status> {
            Err(Status::unimplemented("unused fixture RPC"))
        }
        async fn close_rdma(
            &self,
            _request: Request<afs_protocol::node_data::DfsCloseRdmaRequest>,
        ) -> Result<Response<afs_protocol::node_data::DfsCloseRdmaReply>, Status> {
            Err(Status::unimplemented("unused fixture RPC"))
        }
        async fn read_ranges_rdma(
            &self,
            _request: Request<afs_protocol::node_data::DfsReadRangesRdmaRequest>,
        ) -> Result<Response<afs_protocol::node_data::DfsReadRangesRdmaReply>, Status> {
            Err(Status::unimplemented("unused fixture RPC"))
        }
        async fn read_ranges(
            &self,
            request: Request<DfsReadRangesRequest>,
        ) -> Result<Response<Self::ReadRangesStream>, Status> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let request = request.into_inner();
            if let Some(objects) = &self.fetch_objects {
                if let Some((target, gate)) = &self.fetch_gate
                    && request.operations.iter().any(|operation| {
                        (target.is_empty() || &operation.chunk_id == target)
                            && objects.get(&operation.chunk_id).is_some_and(|(chunk, _)| {
                                operation.chunk_offset == 0 && operation.length == chunk.length
                            })
                    })
                {
                    gate.entered
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    gate.changed.notify_waiters();
                    loop {
                        let changed = gate.changed.notified();
                        tokio::pin!(changed);
                        changed.as_mut().enable();
                        if gate.release.load(std::sync::atomic::Ordering::Acquire) {
                            break;
                        }
                        changed.await;
                    }
                }
                let frames = fetch_fixture_frames(&request, objects)?;
                return Ok(Response::new(Box::pin(tokio_stream::iter(
                    frames.into_iter().map(Ok),
                ))));
            }

            let mut body = self.body.as_ref().clone();
            if matches!(self.mode, 3 | 11) {
                body[0] ^= 1;
            }
            let (_, mut frames) = peer_disk_test_frames(&self.chunk, &body);
            for frame in &mut frames {
                match &mut frame.body {
                    Some(dfs_read_ranges_frame::Body::Header(header)) => {
                        if self.mode >= 9 {
                            header.blake3_outboard = wi6_encoded_outboard(&self.body).into();
                        }
                        header.read_id.clone_from(&request.read_id);
                        header.attempt_id.clone_from(&request.attempt_id);
                        header
                            .source_copy_id
                            .clone_from(&request.operations[0].source_copy_id);
                    }
                    Some(dfs_read_ranges_frame::Body::Completion(completion)) => {
                        if self.mode == 14 {
                            let mut checksum = completion.range_checksum.to_vec();
                            checksum[0] ^= 1;
                            completion.range_checksum = checksum;
                        }
                        completion.read_id.clone_from(&request.read_id);
                        completion.attempt_id.clone_from(&request.attempt_id);
                        completion
                            .source_copy_id
                            .clone_from(&request.operations[0].source_copy_id);
                    }
                    _ => {}
                }
            }
            if self.mode == 6 {
                frames.push(afs_protocol::node_data::DfsReadRangesFrame {
                    body: Some(dfs_read_ranges_frame::Body::Data(prost::bytes::Bytes::new())),
                });
            } else if self.mode == 7 {
                frames.insert(1, frames[0].clone());
            } else if self.mode == 8 {
                frames[1].body = Some(dfs_read_ranges_frame::Body::Data(
                    vec![37; STREAM_DATA_BYTES + 1].into(),
                ));
            }
            if self.mode == 13 {
                frames.retain(|frame| {
                    !matches!(
                        &frame.body,
                        Some(dfs_read_ranges_frame::Body::Completion(_))
                    )
                });
            }
            let mut frames: Vec<_> = frames.into_iter().map(Ok).collect();
            if matches!(self.mode, 2 | 12 | 16) {
                frames.push(Err(Status::unavailable("late status after completion")));
            }
            Ok(Response::new(Box::pin(tokio_stream::iter(frames))))
        }
    }

    #[cfg(feature = "dfs")]
    fn disk_stream_batch(
        chunk: &ChunkObject,
        endpoint: String,
    ) -> crate::node::dfs_read::PeerReadBatch {
        use crate::dfs::*;
        crate::node::dfs_read::PeerReadBatch {
            file_version_id: FileVersionId::new("version"),
            layout_root_id: LayoutRootId::new("layout"),
            operations: vec![crate::node::dfs_read::ResolvedReadOp {
                op: crate::node::dfs_read::ChunkReadOp {
                    chunk_id: chunk.id.clone(),
                    chunk_offset: 0,
                    length: chunk.length,
                    output_offset: 0,
                },
                source: SourceCandidate {
                    chunk_id: chunk.id.clone(),
                    copy_id: CopyId::new("copy"),
                    role: CopyRole::DurableReplica,
                    state: CopyState::Ready,
                    location: CopyLocation::Node {
                        node_id: "server".into(),
                        node_epoch: 1,
                        device_id: "local-0".into(),
                        device_epoch: 1,
                        catalog_revision: 1,
                    },
                    data_endpoint: Some(endpoint),
                    load_hint: 0,
                    read_grant: DfsReadGrant {
                        namespace_id: NamespaceId::new("test"),
                        file_version_id: FileVersionId::new("version"),
                        layout_root_id: LayoutRootId::new("layout"),
                        caller_node_id: "reader".into(),
                        caller_node_epoch: 1,
                        expires_at_unix_ms: u64::MAX,
                        fence: 1,
                        token: "test-token".into(),
                    },
                },
            }],
        }
    }

    #[cfg(feature = "dfs")]
    struct FetchRpcCase {
        _temp: tempfile::TempDir,
        engine: Arc<crate::node::dfs_read::DfsReadEngine>,
        cache: Arc<crate::node::peer_cache::DiskPeerCache>,
        chunks: Vec<ChunkObject>,
        gate: Arc<FetchRpcGate>,
        calls: Arc<std::sync::atomic::AtomicUsize>,
        source_calls: Arc<std::sync::atomic::AtomicUsize>,
        registry: afs_metrics::Registry,
        server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    }

    #[cfg(feature = "dfs")]
    async fn fetch_rpc_case(readahead: u64, gate_all: bool) -> FetchRpcCase {
        use crate::node::dfs_read::{DfsReadConfig, DfsReadEngine, ReadSourceProvider};
        struct Sources {
            reply: crate::dfs::DfsChunkSourcesReply,
            calls: Arc<std::sync::atomic::AtomicUsize>,
        }
        impl ReadSourceProvider for Sources {
            fn sources_for(
                &self,
                request: crate::dfs::DfsChunkSourcesRequest,
            ) -> afs_error::Result<crate::dfs::DfsChunkSourcesReply> {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(crate::dfs::DfsChunkSourcesReply {
                    revision: 1,
                    chunks: self
                        .reply
                        .chunks
                        .iter()
                        .filter(|chunk| request.chunk_ids.contains(&chunk.chunk_id))
                        .cloned()
                        .collect(),
                })
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let objects: std::collections::BTreeMap<_, _> = [17, 29]
            .into_iter()
            .map(|byte| {
                let staged = crate::node::chunk::StagedChunk::new(
                    crate::dfs::OperationId::new(format!("r7-rpc-{byte}")),
                    vec![byte; 64 * 1024],
                );
                (
                    staged.chunk.id.0.clone(),
                    (staged.chunk.clone(), staged.bytes().to_vec()),
                )
            })
            .collect();
        let chunks: Vec<_> = [17, 29]
            .into_iter()
            .map(|byte| {
                objects
                    .values()
                    .find(|(_, body)| body[0] == byte)
                    .unwrap()
                    .0
                    .clone()
            })
            .collect();
        let objects = Arc::new(objects);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let gate = Arc::new(FetchRpcGate::default());
        let service = DiskStreamFixture {
            chunk: chunks[0].clone(),
            body: Arc::new(objects[&chunks[0].id.0].1.clone()),
            mode: 0,
            calls: calls.clone(),
            fetch_objects: Some(objects),
            fetch_gate: Some((
                if gate_all {
                    String::new()
                } else {
                    chunks[1].id.0.clone()
                },
                gate.clone(),
            )),
        };
        let server = tokio::spawn(
            Server::builder()
                .add_service(
                    afs_protocol::node_data::dfs_chunks_server::DfsChunksServer::new(service),
                )
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let pool = Arc::new(
            PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::Disabled,
                1,
            )
            .unwrap()
            .with_read_work_metrics(metrics),
        );
        let transfer = Arc::new(GrpcChunkTransfer::new(pool, Duration::from_secs(5)).unwrap());
        let source_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sources = Sources {
            calls: source_calls.clone(),
            reply: crate::dfs::DfsChunkSourcesReply {
                revision: 1,
                chunks: chunks
                    .iter()
                    .map(|chunk| crate::dfs::ChunkSources {
                        chunk_id: chunk.id.clone(),
                        chunk: Some(chunk.clone()),
                        allowed_ranges: vec![(0, chunk.length)],
                        sources: vec![
                            disk_stream_batch(chunk, endpoint.clone()).operations[0]
                                .source
                                .clone(),
                        ],
                    })
                    .collect(),
            },
        };
        let cache = Arc::new(
            crate::node::peer_cache::DiskPeerCache::open(
                crate::node::peer_cache::PeerCacheOptions {
                    directory: temp.path().join("cache"),
                    capacity_bytes: 1024 * 1024,
                    namespace_id: crate::dfs::NamespaceId::new("test"),
                },
            )
            .unwrap(),
        );
        let engine = Arc::new(
            DfsReadEngine::new(
                crate::dfs::NamespaceId::new("test"),
                "reader".into(),
                Arc::new(
                    crate::node::chunk::LocalChunkStore::open(temp.path().join("local"), "reader")
                        .unwrap(),
                ),
                Arc::new(sources),
                transfer,
                DfsReadConfig {
                    readahead_max_bytes: readahead,
                    ..DfsReadConfig::default()
                },
            )
            .with_read_authority(Arc::new(crate::node::dfs_read::LocalReadAuthority::new(
                1,
                std::time::Instant::now() + Duration::from_secs(30),
            )))
            .with_peer_cache(cache.clone()),
        );
        FetchRpcCase {
            _temp: temp,
            engine,
            cache,
            chunks,
            gate,
            calls,
            source_calls,
            registry,
            server,
        }
    }

    #[cfg(feature = "dfs")]
    fn fetch_case_batch(case: &FetchRpcCase, cross: bool) -> crate::node::dfs_read::ReadBatch {
        use crate::node::dfs_read::{ChunkReadOp, ReadBatch};
        ReadBatch {
            file_version_id: Some(crate::dfs::FileVersionId::new("version")),
            layout_root_id: crate::dfs::LayoutRootId::new("layout"),
            lookahead: if cross {
                Vec::new()
            } else {
                vec![case.chunks[1].id.clone()]
            },
            ops: if cross {
                case.chunks
                    .iter()
                    .enumerate()
                    .map(|(index, chunk)| ChunkReadOp {
                        chunk_id: chunk.id.clone(),
                        chunk_offset: if index == 0 { chunk.length - 8 } else { 0 },
                        length: 8,
                        output_offset: index * 8,
                    })
                    .collect()
            } else {
                vec![ChunkReadOp {
                    chunk_id: case.chunks[0].id.clone(),
                    chunk_offset: 0,
                    length: 8,
                    output_offset: 0,
                }]
            },
        }
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn r7_actual_rpc_cross_object_read_overlaps_and_keeps_output_atomic() {
        let mut case = fetch_rpc_case(0, true).await;
        let batch = fetch_case_batch(&case, true);
        let engine = case.engine.clone();
        let reading = tokio::task::spawn_blocking(move || {
            let mut out = [0; 16];
            engine
                .read_batch_native_into_private(&batch, &mut out)
                .map(|()| out)
        });
        let overlapped = tokio::time::timeout(Duration::from_secs(3), async {
            while case.gate.entered.load(std::sync::atomic::Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok();
        let while_active = afs_metrics::encode_text(&case.registry).unwrap();
        case.gate
            .release
            .store(true, std::sync::atomic::Ordering::Release);
        case.gate.changed.notify_waiters();
        let output = reading.await.unwrap().unwrap();
        assert!(
            overlapped,
            "both full-object RPCs must enter before either is released"
        );
        assert!(
            while_active.contains("afs_dfs_peer_disk_cache_stream_reserved_bytes 4194304\n"),
            "{while_active}"
        );
        assert!(
            while_active.contains("afs_dfs_peer_disk_cache_stream_frame_bytes 524288\n"),
            "{while_active}"
        );
        assert_eq!(&output[..8], &[17; 8]);
        assert_eq!(&output[8..], &[29; 8]);
        assert_eq!(case.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(case.cache.stats().unwrap().entries, 2);
        case.engine
            .object_fetcher()
            .unwrap()
            .shutdown()
            .await
            .unwrap();
        case.server.abort();
        let _ = (&mut case.server).await;
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn r7_actual_rpc_prefetch_is_sink_only_and_cancel_really_joins() {
        for mode in ["success", "global", "scope", "expiry"] {
            let mut case = fetch_rpc_case(8 * 1024 * 1024, false).await;
            let batch = fetch_case_batch(&case, false);
            let engine = case.engine.clone();
            let read_batch = batch.clone();
            assert_eq!(
                tokio::task::spawn_blocking(move || engine.read_batch_native_owned(&read_batch, 8))
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                [17; 8]
            );
            let engine = case.engine.clone();
            let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let scope_valid = valid.clone();
            let original_deadline = std::time::Instant::now() + Duration::from_millis(500);
            tokio::task::spawn_blocking(move || {
                engine.prefetch_cached(
                    &batch,
                    1,
                    Arc::new(move || {
                        if !scope_valid.load(std::sync::atomic::Ordering::Acquire)
                            || (mode == "expiry" && std::time::Instant::now() >= original_deadline)
                        {
                            return Err(afs_error::Error::coded(
                                afs_error::IO_UNAVAILABLE,
                                "test handle scope or original deadline retired",
                            ));
                        }
                        Ok(())
                    }),
                )
            })
            .await
            .unwrap();
            tokio::time::timeout(Duration::from_secs(3), async {
                while case.gate.entered.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            if mode == "global" {
                case.engine.object_fetcher().unwrap().close();
            } else if mode == "success" {
                case.gate
                    .release
                    .store(true, std::sync::atomic::Ordering::Release);
                case.gate.changed.notify_waiters();
                tokio::time::timeout(Duration::from_secs(3), async {
                    while case.cache.stats().unwrap().entries < 2 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap();
            }
            if mode == "scope" {
                valid.store(false, std::sync::atomic::Ordering::Release);
            }
            // Do not call global shutdown to make scope/expiry cancellation
            // pass. The quiet server remains gated until this job really ends.
            let retired_without_shutdown = if matches!(mode, "scope" | "expiry") {
                tokio::time::timeout(Duration::from_secs(2), async {
                    while case.cache.has_fill_flight_for_test() {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .is_ok()
            } else {
                true
            };
            if !retired_without_shutdown {
                case.gate
                    .release
                    .store(true, std::sync::atomic::Ordering::Release);
                case.gate.changed.notify_waiters();
            }
            tokio::time::timeout(
                Duration::from_secs(2),
                case.engine.object_fetcher().unwrap().shutdown(),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                case.cache.stats().unwrap().entries,
                if mode == "success" { 2 } else { 1 }
            );
            assert_eq!(case.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
            assert_eq!(
                case.source_calls.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "prefetch must use the first R-1 lookup only"
            );
            let text = afs_metrics::encode_text(&case.registry).unwrap();
            assert!(
                text.contains("afs_dfs_peer_disk_cache_stream_reserved_bytes 0\n"),
                "{text}"
            );
            assert!(
                text.contains("afs_dfs_peer_disk_cache_stream_frame_bytes 0\n"),
                "{text}"
            );
            assert!(
                text.contains("afs_dfs_fetch_resources{resource=\"jobs\"} 0\n"),
                "{text}"
            );
            case.gate
                .release
                .store(true, std::sync::atomic::Ordering::Release);
            case.gate.changed.notify_waiters();
            case.server.abort();
            let _ = (&mut case.server).await;
            assert!(
                retired_without_shutdown,
                "{mode}: quiet RPC must join before global shutdown"
            );
        }
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peer_stream_actual_rpc_seals_only_after_eof_and_drains_on_failure() {
        use crate::node::{
            dfs_read::ChunkTransfer,
            peer_cache::{DiskPeerCache, PeerCacheOptions},
        };
        // 9/10: peer proof, including disabled disk cache; 11: wrong body;
        // 12: late transport error; 13: missing Completion; 14: false wire root;
        // 15/16: exhausted proof budget, with success/late transport failure.
        for mode in 0..17 {
            let success = matches!(mode, 0 | 1 | 5 | 9 | 10 | 15);
            let consumer_root = mode < 15;
            let staged = crate::node::chunk::StagedChunk::new(
                crate::dfs::OperationId::new("actual-stream"),
                vec![
                    37;
                    if mode == 5 {
                        4 * 1024 * 1024 - 17
                    } else if mode == 4 {
                        1024 * 1024
                    } else {
                        512 * 1024
                    }
                ],
            );
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let service = DiskStreamFixture {
                chunk: staged.chunk.clone(),
                body: Arc::new(staged.bytes().to_vec()),
                mode,
                calls: calls.clone(),
                fetch_objects: None,
                fetch_gate: None,
            };
            let server = tokio::spawn(
                Server::builder()
                    .add_service(
                        afs_protocol::node_data::dfs_chunks_server::DfsChunksServer::new(service),
                    )
                    .serve_with_incoming(TcpListenerStream::new(listener)),
            );
            let registry = afs_metrics::Registry::new();
            let metrics = ReadWorkMetrics::register(&registry)
                .unwrap()
                .with_read_diagnostics(true);
            let pool = PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::Disabled,
                1,
            )
            .unwrap()
            .with_read_work_metrics(metrics.clone());
            let transfer = GrpcChunkTransfer::new(
                Arc::new(pool),
                if mode == 4 {
                    Duration::from_millis(500)
                } else {
                    Duration::from_secs(5)
                },
            )
            .unwrap();
            let batch = disk_stream_batch(&staged.chunk, endpoint);
            tokio::task::spawn_blocking(move || {
                assert!(transfer.supports_verified_object_sink());
                let directory = tempfile::tempdir().unwrap();
                let cache = DiskPeerCache::open_with_outboard_budget(
                    PeerCacheOptions {
                        directory: directory.path().join("cache"),
                        capacity_bytes: if matches!(mode, 1 | 10) { 4096 } else { 8 * 1024 * 1024 },
                        namespace_id: crate::dfs::NamespaceId::new("test"),
                    },
                    OutboardBudget::new(if consumer_root { 1024 * 1024 } else { 0 }),
                )
                .unwrap();
                let mut out = vec![0; 32 * 1024];
                {
                    let mut operation = cache.operation().unwrap();
                    let mut sink = operation.begin_stream(&staged.chunk).unwrap();
                    if mode == 4 {
                        sink.delay_writes_for_test(Duration::from_millis(700));
                    }
                    let result = transfer.read_verified_object_to_sink(
                        &batch,
                        &staged.chunk,
                        13,
                        &mut out,
                        &mut sink,
                    );
                    if success {
                        let proof = result.unwrap().unwrap();
                        assert!(proof.private_output().iter().all(|byte| *byte == 37));
                        // A zero metadata ledger cannot retain even the cache
                        // index/entry controls. The same RPC still completes
                        // full-root verification and private range delivery.
                        assert_eq!(
                            sink.commit(proof).unwrap(),
                            !matches!(mode, 1 | 10 | 15),
                            "optional cache installation, mode {mode}"
                        );
                    } else {
                        assert!(
                            result.is_err(),
                            "invalid/truncated/deadline stream must not seal"
                        );
                    }
                }
                assert_eq!(
                    cache.stats().unwrap().entries,
                    usize::from(matches!(mode, 0 | 5 | 9)),
                    "only budget-admitted healthy entries are retained, mode {mode}"
                );
                if mode >= 15 {
                    assert_eq!(cache.proof_budget().used(), 0);
                    assert_eq!(cache.proof_budget().resident_used(), 0);
                }
                assert_eq!(
                    calls.load(std::sync::atomic::Ordering::SeqCst),
                    1,
                    "cache rejection must not repeat RPC"
                );
                let diagnostics = metrics.diagnostics_snapshot().unwrap();
                assert_eq!(diagnostics.records.len(), 1);
                assert_eq!(diagnostics.overflow, 0);
                assert_eq!(diagnostics.clock_failures, 0);
                let record = &diagnostics.records[0];
                assert_eq!(record.stage, ReadDiagnosticStage::PeerObject);
                assert!(record.completed);
                assert_eq!(record.success, success, "mode {mode}");
                assert!(record.end_ns >= record.start_ns);
                assert!(record.consumer_before_body_ns <= record.consumer_ns);
                if record.success {
                    assert!(record.first_data_ns >= record.start_ns);
                    assert!(record.body_end_ns >= record.first_data_ns);
                    assert!(record.body_end_ns >= record.start_ns);
                    assert!(record.eof_ns >= record.body_end_ns);
                    assert!(record.end_ns >= record.eof_ns);
                    assert!(record.consumer_calls >= 3);
                    let work = record.consumer_work;
                    let blocks = staged.bytes().len().div_ceil(STREAM_FRAME_BACKING_BYTES) as u64;
                    assert_eq!(work.sink_write.calls, blocks);
                    assert_eq!(work.sink_write.bytes, staged.bytes().len() as u64);
                    assert_eq!(work.range_extract.bytes, out.len() as u64);
                    assert_eq!(
                        work.root_blake3.calls, 0,
                        "plain consumer hash is absent; the selected root owner authenticates the body"
                    );
                    assert_eq!(
                        work.root_blake3.bytes, 0,
                        "outboard work owns the consumer root without another plain hasher"
                    );
                    assert_eq!(record.producer_root_work.calls, if consumer_root { 0 } else { blocks + 1 });
                    assert_eq!(record.producer_root_work.bytes, if consumer_root { 0 } else { staged.bytes().len() as u64 });
                    assert_eq!(record.interval_overflow, 0);
                    assert!(
                        record.root_segment_overlap_ns.is_none(),
                        "one root owner leaves no duplicate producer/consumer root overlap"
                    );
                    assert_eq!(work.segment_blake3.bytes, if consumer_root { staged.bytes().len() as u64 } else { 0 });
                    assert_eq!(work.segment_blake3.calls, if consumer_root { blocks + 1 } else { 0 });
                    let measured = work.sink_write.wall_ns
                        + work.range_extract.wall_ns
                        + work.root_blake3.wall_ns
                        + work.segment_blake3.wall_ns;
                    assert!(
                        measured <= record.consumer_ns,
                        "exclusive children stay inside consumer parent"
                    );
                }
                let families = registry.gather();
                if success {
                    let events = families
                        .iter()
                        .find(|family| family.name() == "afs_dfs_peer_read_events_total")
                        .unwrap();
                    let event_count = |event: &str| {
                        events
                            .get_metric()
                            .iter()
                            .filter(|metric| {
                                metric
                                    .get_label()
                                    .iter()
                                    .any(|label| label.name() == "event" && label.value() == event)
                            })
                            .map(|metric| metric.get_counter().get_value())
                            .sum::<f64>()
                    };
                    assert_eq!(event_count("root_fallback"), if consumer_root { 0.0 } else { 1.0 });
                    assert_eq!(
                        event_count("stream_frame"),
                        (staged.bytes().len().div_ceil(STREAM_DATA_BYTES) + 2) as f64,
                        "wire frames stay unchanged"
                    );
                    let blocks = staged.bytes().len().div_ceil(STREAM_FRAME_BACKING_BYTES) as f64;
                    assert_eq!(
                        event_count("stream_block_handoff"),
                        blocks,
                        "wire data frames must use fixed-block handoffs"
                    );
                    assert_eq!(
                        event_count("stream_block_write"),
                        blocks,
                        "one sink entry per handed-off block, even when cache is disabled"
                    );
                    assert_eq!(
                        event_count("stream_block_alloc"),
                        2.0,
                        "only two reusable payload blocks per operation"
                    );
                    let copies = families
                        .iter()
                        .find(|family| family.name() == "afs_dfs_read_copy_bytes_total")
                        .unwrap();
                    let copied = copies
                        .get_metric()
                        .iter()
                        .filter(|metric| {
                            metric.get_label().iter().any(|label| {
                                label.name() == "stage" && label.value() == "peer_frame_to_block"
                            })
                        })
                        .map(|metric| metric.get_counter().get_value())
                        .sum::<f64>();
                    assert_eq!(
                        copied,
                        staged.bytes().len() as f64,
                        "exactly one wire-to-block payload copy"
                    );
                }
                let peak = families
                    .iter()
                    .find(|family| {
                        family.name() == "afs_dfs_peer_disk_cache_stream_frame_peak_bytes"
                    })
                    .unwrap();
                assert_eq!(
                    peak.get_metric()[0].get_gauge().get_value(),
                    (2 * STREAM_FRAME_BACKING_BYTES) as f64,
                    "both actual fixed allocations must be charged"
                );
                let current = families
                    .iter()
                    .find(|family| family.name() == "afs_dfs_peer_disk_cache_stream_frame_bytes")
                    .unwrap();
                assert_eq!(
                    current.get_metric()[0].get_gauge().get_value(),
                    0.0,
                    "producer and queued frame leases must drain before return"
                );
            })
            .await
            .unwrap();
            server.abort();
            let _ = server.await;
        }
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn peer_stream_joined_root_receipt_requires_completion_meta_and_sink_identity() {
        use crate::node::peer_cache::{DiskPeerCache, PeerCacheOptions};
        for consumer_tree in [false, true] {
            for mode in 0..6 {
                let staged = crate::node::chunk::StagedChunk::new(
                    crate::dfs::OperationId::new("joined-root"),
                    vec![37; 512 * 1024],
                );
                let directory = tempfile::tempdir().unwrap();
                let open_cache = |name| {
                    DiskPeerCache::open(PeerCacheOptions {
                        directory: directory.path().join(name),
                        capacity_bytes: 8 * 1024 * 1024,
                        namespace_id: crate::dfs::NamespaceId::new("test"),
                    })
                    .unwrap()
                };
                let cache = open_cache("cache");
                let other_cache = open_cache("other");
                let mut operation = cache.operation().unwrap();
                let mut other_operation = other_cache.operation().unwrap();
                let mut sink = operation.begin_stream(&staged.chunk).unwrap();
                let proof_budget = sink.proof_budget();
                let proof_baseline = proof_budget.used();
                let other_sink = other_operation.begin_stream(&staged.chunk).unwrap();
                let mut out = vec![0; 32 * 1024];
                let (request, mut frames) = peer_disk_test_frames(&staged.chunk, staged.bytes());
                if consumer_tree {
                    let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body
                    else {
                        panic!("header");
                    };
                    header.blake3_outboard = wi6_encoded_outboard(staged.bytes()).into();
                }
                if mode == 5 {
                    frames.pop();
                }
                let binding = sink.bind_output(13, &out).unwrap();
                let mut producer = PeerRootProducer::new(
                    staged.chunk.length,
                    binding.clone(),
                    None,
                    std::time::Instant::now() + Duration::from_secs(5),
                )
                .with_outboard_context(staged.chunk.clone(), sink.proof_budget());
                let mut decoder =
                    ReadFrameDecoder::for_producer_object(&request, &staged.chunk).unwrap();
                decoder.stream_binding = Some(binding);
                let mut offset = 0;
                for mut frame in frames {
                    if let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frame.body {
                        producer.accept_outboard_header(header, None).unwrap();
                        decoder
                            .install_producer_outboard(producer.outboard_builder.take())
                            .unwrap();
                    }
                    if let Some(dfs_read_ranges_frame::Body::Data(data)) = &frame.body {
                        producer.update(offset, data, None).unwrap();
                        offset += data.len();
                    }
                    consume_peer_frame_into_sink(
                        &mut decoder,
                        frame,
                        13,
                        &mut out,
                        &mut sink,
                        std::time::Instant::now() + Duration::from_secs(5),
                        PeerConsumerObservation::default(),
                    )
                    .unwrap();
                    if let Some(active) = &decoder.active {
                        assert!(
                            active.checksum.is_none(),
                            "consumer has no duplicate plain range hasher; its outboard tree is independently verified"
                        );
                    }
                }
                if mode == 1 {
                    assert!(
                        decoder.finish_stream(&staged.chunk).is_err(),
                        "completion alone cannot substitute for joined root/EOF"
                    );
                    assert_eq!(
                        proof_budget.used(),
                        proof_baseline,
                        "unjoined metadata must retire without a proof"
                    );
                    continue;
                }
                let mut receipt = producer.finish_after_eof().unwrap();
                if mode == 2 {
                    receipt.length -= 1;
                }
                if mode == 3 {
                    match &mut receipt.digest {
                        PeerRootDigest::Producer(digest) => digest[0] ^= 1,
                        PeerRootDigest::ConsumerTree => {
                            receipt.digest = PeerRootDigest::Producer([0; 32])
                        }
                    }
                }
                if mode == 4 {
                    receipt.binding = other_sink.bind_output(13, &out).unwrap();
                }
                let proof = decoder.finish_stream_from_producer(
                    &staged.chunk,
                    receipt,
                    &sink,
                    13,
                    &out,
                    PeerConsumerObservation::default(),
                );
                if mode == 0 {
                    let completed = sink.seal_completed(proof.unwrap(), 13, &out).unwrap();
                    assert!(completed.private_output().iter().all(|byte| *byte == 37));
                    assert!(sink.commit(completed).unwrap());
                } else {
                    assert!(
                        proof.is_err(),
                        "wrong/truncated/missing receipt evidence cannot mint output"
                    );
                    assert_eq!(
                        proof_budget.used(),
                        proof_baseline,
                        "failed join must retire consumer metadata"
                    );
                }
            }
        }
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn peer_root_producer_rejects_reordered_and_truncated_blocks() {
        use crate::node::peer_cache::{DiskPeerCache, PeerCacheOptions};
        let staged = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("producer-order"),
            vec![37; 512],
        );
        let directory = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(PeerCacheOptions {
            directory: directory.path().join("cache"),
            capacity_bytes: 8192,
            namespace_id: crate::dfs::NamespaceId::new("test"),
        })
        .unwrap();
        let mut operation = cache.operation().unwrap();
        let sink = operation.begin_stream(&staged.chunk).unwrap();
        let out = [0; 8];
        let mut producer = PeerRootProducer::new(
            staged.chunk.length,
            sink.bind_output(0, &out).unwrap(),
            None,
            std::time::Instant::now() + Duration::from_secs(5),
        );
        assert!(producer.update(1, &[37; 8], None).is_err());
        producer.update(0, &[37; 8], None).unwrap();
        assert!(producer.update(0, &[37; 8], None).is_err());
        assert!(
            producer.finish_after_eof().is_err(),
            "partial root cannot create a receipt"
        );
        let mut expired = PeerRootProducer::new(
            staged.chunk.length,
            sink.bind_output(0, &out).unwrap(),
            None,
            std::time::Instant::now(),
        );
        assert!(expired.update(0, staged.bytes(), None).is_err());
        assert_eq!(
            expired.length, 0,
            "expired deadline rejects before scanning"
        );
        let mut complete = PeerRootProducer::new(
            staged.chunk.length,
            sink.bind_output(0, &out).unwrap(),
            None,
            std::time::Instant::now() + Duration::from_secs(5),
        );
        complete.update(0, staged.bytes(), None).unwrap();
        complete.deadline = std::time::Instant::now();
        assert!(
            complete.finish_after_eof().is_err(),
            "expired deadline cannot mint a complete receipt"
        );
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn r7_background_admission_first_poll_never_waits_for_metadata_locks() {
        struct Wake;
        impl std::task::Wake for Wake {
            fn wake(self: Arc<Self>) {}
        }
        let waker = std::task::Waker::from(Arc::new(Wake));
        let mut context = std::task::Context::from_waker(&waker);
        let pool = PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            1,
        )
        .unwrap();
        let channels = pool.state.lock().await;
        let mut background =
            Box::pin(pool.acquire_stream_work_mode("peer", 1, "http://127.0.0.1:9", true));
        assert!(matches!(
            std::future::Future::poll(background.as_mut(), &mut context),
            std::task::Poll::Ready(Ok(None))
        ));
        drop(background);
        let mut foreground =
            Box::pin(pool.acquire_stream_work_mode("peer", 1, "http://127.0.0.1:9", false));
        assert!(std::future::Future::poll(foreground.as_mut(), &mut context).is_pending());
        drop(channels);
        let active = foreground.await.unwrap().unwrap();
        drop(active);
        let admission = pool.stream_work.state.lock().unwrap();
        let mut background =
            Box::pin(pool.acquire_stream_work_mode("peer", 1, "http://127.0.0.1:9", true));
        assert!(matches!(
            std::future::Future::poll(background.as_mut(), &mut context),
            std::task::Poll::Ready(Ok(None))
        ));
        drop(background);
        drop(admission);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn peer_stream_current_thread_is_unsupported_before_rpc() {
        use crate::node::{
            dfs_read::ChunkTransfer,
            peer_cache::{DiskPeerCache, PeerCacheOptions},
        };
        let pool = PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            1,
        )
        .unwrap();
        let transfer = GrpcChunkTransfer::new(Arc::new(pool), Duration::from_millis(10)).unwrap();
        assert!(!transfer.supports_verified_object_sink());
        let staged = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("unsupported-stream"),
            vec![37; 64 * 1024],
        );
        let batch = disk_stream_batch(&staged.chunk, "http://127.0.0.1:1".into());
        let directory = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(PeerCacheOptions {
            directory: directory.path().join("cache"),
            capacity_bytes: 1024 * 1024,
            namespace_id: crate::dfs::NamespaceId::new("test"),
        })
        .unwrap();
        let mut operation = cache.operation().unwrap();
        let mut sink = operation.begin_stream(&staged.chunk).unwrap();
        let mut out = [99; 8];
        assert!(
            transfer
                .read_verified_object_to_sink(&batch, &staged.chunk, 0, &mut out, &mut sink)
                .unwrap()
                .is_none()
        );
        assert_eq!(out, [99; 8]);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn peer_stream_large_windows_are_unsupported_before_rpc() {
        use crate::node::dfs_read::ChunkTransfer;
        for connection in [false, true] {
            let mut config = afs_transport::GrpcConfig::default();
            if connection {
                config.initial_connection_window_size += 1;
            } else {
                config.initial_stream_window_size += 1;
            }
            let pool =
                PeerConnectionPool::new(config, afs_transport::TlsConfig::Disabled, 1).unwrap();
            let transfer = GrpcChunkTransfer::new(Arc::new(pool), Duration::from_secs(1)).unwrap();
            assert!(!transfer.supports_verified_object_sink());
        }
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn peer_stream_blocks_retain_capacity_and_controls_reject_payload() {
        use afs_protocol::node_data::DfsReadRangesFrame;
        let mut block = PeerStreamBlock::new(None).unwrap();
        let address = block.bytes.as_ptr();
        for length in [STREAM_FRAME_BACKING_BYTES, 7, STREAM_DATA_BYTES] {
            block.bytes.clear();
            block.bytes.resize(length, 37);
            assert_eq!(block.bytes.as_ptr(), address);
            assert_eq!(block.bytes.capacity(), STREAM_FRAME_BACKING_BYTES);
        }
        let mut data = DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Data(
                vec![0; STREAM_DATA_BYTES].into(),
            )),
        };
        assert!(peer_stream_control_capacity(&mut data).is_err());
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn peer_stream_complete_proof_cannot_seal_another_same_object_sink() {
        use crate::node::peer_cache::{DiskPeerCache, PeerCacheOptions};
        let directory = tempfile::tempdir().unwrap();
        let make_cache = |name| {
            DiskPeerCache::open(PeerCacheOptions {
                directory: directory.path().join(name),
                capacity_bytes: 1024 * 1024,
                namespace_id: crate::dfs::NamespaceId::new("test"),
            })
            .unwrap()
        };
        let cache_a = make_cache("a");
        let cache_b = make_cache("b");
        let staged = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("different-sink"),
            vec![37; 64 * 1024],
        );
        let mut operation_a = cache_a.operation().unwrap();
        let mut operation_b = cache_b.operation().unwrap();
        let mut sink_a = operation_a.begin_stream(&staged.chunk).unwrap();
        let mut sink_b = operation_b.begin_stream(&staged.chunk).unwrap();
        let mut out = [0; 8];
        let (request, frames) = peer_disk_test_frames(&staged.chunk, staged.bytes());
        let mut decoder = ReadFrameDecoder::for_object(&request, &staged.chunk).unwrap();
        decoder.stream_binding = Some(sink_a.bind_output(0, &out).unwrap());
        for frame in frames {
            consume_peer_frame_into_sink(
                &mut decoder,
                frame,
                0,
                &mut out,
                &mut sink_a,
                std::time::Instant::now() + Duration::from_secs(5),
                PeerConsumerObservation::default(),
            )
            .unwrap();
        }
        let proof = decoder.finish_stream(&staged.chunk).unwrap();
        sink_b.write(0, staged.bytes()).unwrap();
        assert!(sink_b.seal_completed(proof, 0, &out).is_err());
    }

    #[tokio::test]
    async fn endpoint_client_applies_tls_policy_before_connecting() {
        let result = connect_data_client(DataClientOptions {
            endpoint: "https://127.0.0.1:1".into(),
            mode: DataMode::Grpc,
            rdma_device: None,
            timeout: Duration::from_millis(10),
            tls: afs_transport::TlsConfig::MutualTls {
                ca_certificate: "missing-ca.pem".into(),
                identity_certificate: "missing-client.pem".into(),
                identity_private_key: "missing-client-key.pem".into(),
                server_name: String::new(),
            },
        })
        .await;

        let error = match result {
            Ok(_) => panic!("invalid diagnostics TLS policy must fail before connecting"),
            Err(error) => error,
        };
        assert_eq!(error.code(), afs_error::CLIENT_ARGUMENT_INVALID);
        assert!(error.to_string().contains("server_name"));
    }

    #[derive(Clone, Copy)]
    enum BadReplyKind {
        ReadShape,
        WriteCount,
    }

    #[derive(Clone, Copy)]
    struct BadDataService {
        kind: BadReplyKind,
    }

    #[tonic::async_trait]
    impl NodeData for BadDataService {
        async fn read(
            &self,
            _request: Request<DataReadRequest>,
        ) -> Result<Response<DataReadReply>, Status> {
            match self.kind {
                BadReplyKind::ReadShape => Ok(Response::new(DataReadReply {
                    length: 8,
                    data: b"short".to_vec(),
                })),
                BadReplyKind::WriteCount => Err(Status::unimplemented("read not used")),
            }
        }

        async fn write(
            &self,
            request: Request<DataWriteRequest>,
        ) -> Result<Response<DataWriteReply>, Status> {
            match self.kind {
                BadReplyKind::ReadShape => Err(Status::unimplemented("write not used")),
                BadReplyKind::WriteCount => Ok(Response::new(DataWriteReply {
                    written: request.into_inner().data.len() as u32 + 1,
                })),
            }
        }
    }

    #[tokio::test]
    async fn grpc_client_rejects_read_reply_shape_mismatch() {
        let (endpoint, server) = spawn_bad_data_server(BadReplyKind::ReadShape).await;
        let mut client = connect_data_client(DataClientOptions {
            endpoint,
            mode: DataMode::Grpc,
            rdma_device: None,
            timeout: Duration::from_secs(5),
            tls: Default::default(),
        })
        .await
        .unwrap();

        let error = client.read("bad.bin", 0, 8).await.unwrap_err();

        assert!(error.to_string().contains("read reply shape mismatch"));
        server.abort();
    }

    #[tokio::test]
    async fn grpc_client_rejects_write_count_mismatch() {
        let (endpoint, server) = spawn_bad_data_server(BadReplyKind::WriteCount).await;
        let mut client = connect_data_client(DataClientOptions {
            endpoint,
            mode: DataMode::Grpc,
            rdma_device: None,
            timeout: Duration::from_secs(5),
            tls: Default::default(),
        })
        .await
        .unwrap();

        let error = client
            .write("bad.bin", 0, b"abcdefgh".to_vec())
            .await
            .unwrap_err();

        assert!(error.to_string().contains("write reply count mismatch"));
        server.abort();
    }

    #[tokio::test]
    #[cfg(feature = "ownerfs")]
    async fn owner_release_retries_transient_failures_without_losing_handle() {
        let handler = std::sync::Arc::new(ReleaseTestHandler::transient_failures(2));
        let (channel, server) = spawn_owner_release_server(handler.clone()).await;
        let client = owner_files_client_from_channel_with_runtime(
            channel,
            tokio::runtime::Handle::current(),
        );
        let grant = test_grant();
        let file = test_remote_file(b"handle-retry".to_vec());

        client.release(&grant, file).unwrap();

        wait_until(Duration::from_secs(2), || handler.attempts() >= 3).await;
        assert_eq!(handler.attempts(), 3);
        assert_eq!(handler.seen_handles(), vec![b"handle-retry".to_vec(); 3]);
        server.abort();
    }

    #[tokio::test]
    #[cfg(feature = "ownerfs")]
    async fn owner_release_treats_stale_handle_as_idempotent_success() {
        let handler = std::sync::Arc::new(ReleaseTestHandler::stale_handle());
        let (channel, server) = spawn_owner_release_server(handler.clone()).await;
        let client = owner_files_client_from_channel_with_runtime(
            channel,
            tokio::runtime::Handle::current(),
        );

        client
            .release(&test_grant(), test_remote_file(b"handle-stale".to_vec()))
            .unwrap();

        wait_until(Duration::from_secs(2), || handler.attempts() >= 1).await;
        tokio::time::sleep(RELEASE_INITIAL_BACKOFF * 3).await;
        assert_eq!(handler.attempts(), 1);
        assert_eq!(handler.seen_handles(), vec![b"handle-stale".to_vec()]);
        server.abort();
    }

    #[cfg(all(feature = "ownerfs", feature = "rdma"))]
    #[tokio::test]
    async fn owner_rdma_client_admission_rejects_before_endpoint_open() {
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let client = owner_files_client_from_channel(channel)
            .with_data_transport(
                DataMode::Rdma,
                Some("__afs_rdma_must_not_open__".to_owned()),
                Duration::from_millis(1),
            )
            .with_rdma_admission(Arc::new(tokio::sync::Semaphore::new(0)));
        let grant = RootGrant {
            id: RootId("job-admission".to_owned()),
            epoch: 1,
            home_node_id: "node-a".to_owned(),
            home_session_id: "home-session".to_owned(),
            holder_node_id: "node-b".to_owned(),
            session_id: "grant-session".to_owned(),
            access_generation: 1,
            rights: vec![RootRight::Read, RootRight::Write],
            fencing_token: "fence".to_owned(),
        };

        let error = match client.negotiate_owner_rdma(&grant).await {
            Ok(_) => panic!("zero client admission must fail before opening an endpoint"),
            Err(error) => error,
        };
        assert_eq!(error.code(), afs_error::NODE_RDMA_CAPACITY);
    }

    #[cfg(all(feature = "ownerfs", feature = "rdma"))]
    #[tokio::test]
    async fn owner_rdma_missing_device_errors_are_counted_by_client_timer() {
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let client = owner_files_client_from_channel_with_runtime_and_metrics(
            channel,
            tokio::runtime::Handle::current(),
            Some(metrics),
        )
        .with_data_transport(DataMode::Rdma, None, Duration::from_millis(1));
        let grant = test_grant();
        let file = test_remote_file(b"handle-rdma-missing".to_vec());

        tokio::task::spawn_blocking(move || {
            let mut one = [0_u8; 1];
            let read = client.read(&grant, &file, 0, &mut one).unwrap_err();
            assert_eq!(read.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
            let write = client.write(&grant, &file, 0, b"x").unwrap_err();
            assert_eq!(write.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
        })
        .await
        .expect("missing-device regression worker");

        assert_eq!(owner_rpc_histogram_count(&registry, "client", "read"), 1);
        assert_eq!(owner_rpc_histogram_count(&registry, "client", "write"), 1);
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn owner_grpc_payload_bytes_count_successful_logical_bytes() {
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
        let handler = std::sync::Arc::new(PayloadMetricsHandler::success());
        let (channel, server) = spawn_owner_payload_server(handler, metrics.clone()).await;
        let client = owner_files_client_from_channel_with_runtime_and_metrics(
            channel,
            tokio::runtime::Handle::current(),
            Some(metrics),
        );
        let grant = test_grant();
        let file = test_remote_file(b"handle-payload".to_vec());

        tokio::task::spawn_blocking(move || {
            let mut out = [0_u8; 8];
            assert_eq!(client.read(&grant, &file, 0, &mut out).unwrap(), 3);
            assert_eq!(&out[..3], b"abc");
            assert_eq!(client.write(&grant, &file, 0, b"hello").unwrap(), 2);
        })
        .await
        .expect("payload metrics worker");

        assert_eq!(owner_payload_bytes(&registry, "client", "read", "grpc"), 3);
        assert_eq!(owner_payload_bytes(&registry, "server", "read", "grpc"), 3);
        assert_eq!(owner_payload_bytes(&registry, "client", "write", "grpc"), 2);
        assert_eq!(owner_payload_bytes(&registry, "server", "write", "grpc"), 2);
        server.abort();
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn owner_grpc_write_preserves_request_contract() {
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
        let handler = std::sync::Arc::new(PayloadMetricsHandler::success());
        let (channel, server) = spawn_owner_payload_server(handler.clone(), metrics.clone()).await;
        let client = owner_files_client_from_channel_with_runtime_and_metrics(
            channel,
            tokio::runtime::Handle::current(),
            Some(metrics),
        );
        let grant = test_grant();
        let file = test_remote_file(b"handle-payload-contract".to_vec());
        let payload = b"write-contract-payload";
        let checksum = blake3::hash(payload).as_bytes().to_vec();

        tokio::task::spawn_blocking(move || {
            assert_eq!(
                client
                    .write_with_options(
                        &grant,
                        &file,
                        4096,
                        payload,
                        WriteOptions { kill_suidgid: true },
                    )
                    .unwrap(),
                2
            );
        })
        .await
        .expect("write contract worker");

        assert_eq!(
            handler.writes(),
            vec![PayloadWriteRecord {
                offset: 4096,
                data: payload.to_vec(),
                length: payload.len() as u32,
                kill_suidgid: true,
                data_checksum: checksum,
            }]
        );
        assert_eq!(owner_payload_bytes(&registry, "client", "write", "grpc"), 2);
        assert_eq!(owner_payload_bytes(&registry, "server", "write", "grpc"), 2);
        server.abort();
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn owner_grpc_write_large_payload_preserves_request_contract() {
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
        let handler = std::sync::Arc::new(PayloadMetricsHandler::success());
        let (channel, server) = spawn_owner_payload_server(handler.clone(), metrics.clone()).await;
        let client = owner_files_client_from_channel_with_runtime_and_metrics(
            channel,
            tokio::runtime::Handle::current(),
            Some(metrics),
        );
        let grant = test_grant();
        let file = test_remote_file(b"handle-large-payload-contract".to_vec());
        let expected_payload = deterministic_bytes(1024 * 1024 - 1);
        let payload = expected_payload.clone();
        let checksum = blake3::hash(&expected_payload).as_bytes().to_vec();

        tokio::task::spawn_blocking(move || {
            assert_eq!(
                client
                    .write_with_options(
                        &grant,
                        &file,
                        8192,
                        &payload,
                        WriteOptions { kill_suidgid: true },
                    )
                    .unwrap(),
                2
            );
        })
        .await
        .expect("large write contract worker");

        assert_eq!(
            handler.writes(),
            vec![PayloadWriteRecord {
                offset: 8192,
                data: expected_payload.clone(),
                length: expected_payload.len() as u32,
                kill_suidgid: true,
                data_checksum: checksum,
            }]
        );
        assert_eq!(owner_payload_bytes(&registry, "client", "write", "grpc"), 2);
        assert_eq!(owner_payload_bytes(&registry, "server", "write", "grpc"), 2);
        server.abort();
    }

    #[cfg(feature = "ownerfs")]
    fn deterministic_bytes(len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| {
                let mixed = index
                    .wrapping_mul(1_103_515_245)
                    .wrapping_add(12_345)
                    .rotate_left((index % 8) as u32);
                mixed as u8
            })
            .collect()
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn owner_grpc_write_rejects_bad_inline_checksum_before_handler() {
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
        let handler = std::sync::Arc::new(PayloadMetricsHandler::success());
        let (channel, server) = spawn_owner_payload_server(handler.clone(), metrics.clone()).await;
        let mut client = OwnerFilesClient::new(channel);
        let grant = test_grant();
        let file = test_remote_file(b"handle-inline-checksum".to_vec());
        let base = b"checksum-boundary".to_vec();
        let mut wrong_checksum = blake3::hash(&base).as_bytes().to_vec();
        wrong_checksum[0] ^= 0xff;

        async fn write_raw(
            client: &mut OwnerFilesClient<Channel>,
            grant: &RootGrant,
            file: &RemoteFile,
            data: Vec<u8>,
            length: u32,
            data_checksum: Vec<u8>,
        ) -> Result<OwnerWriteReply, tonic::Status> {
            client
                .write(request_with_current_context(OwnerWriteRequest {
                    access: Some(root_access(grant)),
                    handle: Some(file_handle(file)),
                    offset: 0,
                    data,
                    length,
                    plane: Some(grpc_plane()),
                    kill_suidgid: false,
                    data_checksum,
                }))
                .await
                .map(|reply| reply.into_inner())
        }

        let error = write_raw(
            &mut client,
            &grant,
            &file,
            base.clone(),
            base.len() as u32,
            wrong_checksum,
        )
        .await
        .expect_err("wrong 32-byte checksum must fail");
        assert_eq!(
            afs_transport::grpc::error_status::status_to_error(error).code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );

        let error = write_raw(
            &mut client,
            &grant,
            &file,
            base.clone(),
            base.len() as u32,
            b"short-checksum".to_vec(),
        )
        .await
        .expect_err("malformed checksum length must fail");
        assert_eq!(
            afs_transport::grpc::error_status::status_to_error(error).code(),
            afs_error::NODE_TRANSFER_INVALID
        );

        let error = write_raw(
            &mut client,
            &grant,
            &file,
            base.clone(),
            base.len() as u32 + 1,
            blake3::hash(&base).as_bytes().to_vec(),
        )
        .await
        .expect_err("length/data mismatch must fail");
        assert_eq!(
            afs_transport::grpc::error_status::status_to_error(error).code(),
            afs_error::NODE_TRANSFER_INVALID
        );

        assert!(handler.writes().is_empty());
        assert_eq!(owner_payload_bytes(&registry, "server", "write", "grpc"), 0);

        assert_eq!(
            write_raw(
                &mut client,
                &grant,
                &file,
                base.clone(),
                base.len() as u32,
                blake3::hash(&base).as_bytes().to_vec(),
            )
            .await
            .expect("valid checksum must succeed")
            .written,
            2
        );
        assert_eq!(
            write_raw(
                &mut client,
                &grant,
                &file,
                base.clone(),
                base.len() as u32,
                Vec::new(),
            )
            .await
            .expect("empty legacy checksum must succeed")
            .written,
            2
        );

        let writes = handler.writes();
        assert_eq!(writes.len(), 2);
        assert_eq!(
            writes[0].data_checksum,
            blake3::hash(&base).as_bytes().to_vec()
        );
        assert!(writes[1].data_checksum.is_empty());
        assert_eq!(owner_payload_bytes(&registry, "server", "write", "grpc"), 4);
        server.abort();
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn owner_grpc_malformed_replies_do_not_count_successful_payload_bytes() {
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
        let handler = std::sync::Arc::new(PayloadMetricsHandler::malformed());
        let (channel, server) = spawn_owner_payload_server(handler, metrics.clone()).await;
        let client = owner_files_client_from_channel_with_runtime_and_metrics(
            channel,
            tokio::runtime::Handle::current(),
            Some(metrics),
        );
        let grant = test_grant();
        let file = test_remote_file(b"handle-payload-bad".to_vec());

        tokio::task::spawn_blocking(move || {
            let mut out = [0_u8; 8];
            let read = client.read(&grant, &file, 0, &mut out).unwrap_err();
            assert_eq!(read.code(), afs_error::NODE_TRANSFER_INVALID);
            let write = client.write(&grant, &file, 0, b"abc").unwrap_err();
            assert_eq!(write.code(), afs_error::NODE_TRANSFER_INVALID);
        })
        .await
        .expect("malformed payload metrics worker");

        for side in ["client", "server"] {
            assert_eq!(owner_payload_bytes(&registry, side, "read", "grpc"), 0);
            assert_eq!(owner_payload_bytes(&registry, side, "write", "grpc"), 0);
        }
        server.abort();
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn owner_prefetch_cache_hits_do_not_count_as_client_rpc_or_payload() {
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::rpc::OwnerRpcMetrics::register(&registry).unwrap();
        let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        let client = owner_files_client_from_channel_with_runtime_and_metrics(
            channel,
            tokio::runtime::Handle::current(),
            Some(metrics),
        );
        let grant = test_grant();
        let file = test_remote_file(b"prefetched-handle".to_vec());
        client
            .prefetched_reads
            .lock()
            .unwrap()
            .insert(file.handle.clone(), b"cached".to_vec());

        let mut out = [0_u8; 6];
        assert_eq!(client.read(&grant, &file, 0, &mut out).unwrap(), 6);
        assert_eq!(&out, b"cached");
        assert_eq!(owner_rpc_histogram_count(&registry, "client", "read"), 0);
        assert_eq!(owner_payload_bytes(&registry, "client", "read", "grpc"), 0);
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn cancellation_guard_poisons_unfinished_session_and_disarm_preserves_it() {
        let poisoned = Arc::new(AtomicBool::new(false));
        {
            let _guard = CancelPoisonGuard::new(poisoned.clone());
        }
        assert!(poisoned.load(Ordering::SeqCst));

        let poisoned = Arc::new(AtomicBool::new(false));
        CancelPoisonGuard::new(poisoned.clone()).disarm();
        assert!(!poisoned.load(Ordering::SeqCst));
    }

    async fn spawn_bad_data_server(kind: BadReplyKind) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(NodeDataServer::new(BadDataService { kind }))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        (endpoint, server)
    }

    #[cfg(feature = "ownerfs")]
    struct AllowPeer;

    #[cfg(feature = "ownerfs")]
    impl PeerAuthenticator for AllowPeer {
        fn authenticate(
            &self,
            _metadata: &tonic::metadata::MetadataMap,
            _remote_addr: Option<std::net::SocketAddr>,
            _peer_cert_der: Option<&[u8]>,
        ) -> afs_error::Result<String> {
            Ok("node-b".to_owned())
        }
    }

    #[cfg(feature = "ownerfs")]
    enum ReleaseFailureMode {
        Transient { remaining: TestAtomicUsize },
        Stale,
    }

    #[cfg(feature = "ownerfs")]
    enum PayloadMetricsMode {
        Success,
        Malformed,
    }

    #[cfg(feature = "ownerfs")]
    struct PayloadMetricsHandler {
        mode: PayloadMetricsMode,
        writes: StdMutex<Vec<PayloadWriteRecord>>,
    }

    #[cfg(feature = "ownerfs")]
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct PayloadWriteRecord {
        offset: u64,
        data: Vec<u8>,
        length: u32,
        kill_suidgid: bool,
        data_checksum: Vec<u8>,
    }

    #[cfg(feature = "ownerfs")]
    impl PayloadMetricsHandler {
        fn success() -> Self {
            Self {
                mode: PayloadMetricsMode::Success,
                writes: StdMutex::new(Vec::new()),
            }
        }

        fn malformed() -> Self {
            Self {
                mode: PayloadMetricsMode::Malformed,
                writes: StdMutex::new(Vec::new()),
            }
        }

        fn writes(&self) -> Vec<PayloadWriteRecord> {
            self.writes.lock().unwrap().clone()
        }
    }

    #[cfg(feature = "ownerfs")]
    impl OwnerFilesHandler for PayloadMetricsHandler {
        fn read(
            &self,
            peer: &str,
            _request: OwnerReadRequest,
        ) -> afs_error::Result<OwnerReadReply> {
            assert_eq!(peer, "node-b");
            match self.mode {
                PayloadMetricsMode::Success => Ok(OwnerReadReply {
                    data: b"abc".to_vec().into(),
                    read: 3,
                    eof: true,
                    data_checksum: Vec::new(),
                }),
                PayloadMetricsMode::Malformed => Ok(OwnerReadReply {
                    data: b"abc".to_vec().into(),
                    read: 4,
                    eof: false,
                    data_checksum: Vec::new(),
                }),
            }
        }

        fn write(
            &self,
            peer: &str,
            request: OwnerWriteRequest,
        ) -> afs_error::Result<OwnerWriteReply> {
            assert_eq!(peer, "node-b");
            self.writes.lock().unwrap().push(PayloadWriteRecord {
                offset: request.offset,
                data: request.data.clone(),
                length: request.length,
                kill_suidgid: request.kill_suidgid,
                data_checksum: request.data_checksum.clone(),
            });
            match self.mode {
                PayloadMetricsMode::Success => Ok(OwnerWriteReply { written: 2 }),
                PayloadMetricsMode::Malformed => Ok(OwnerWriteReply {
                    written: request.length + 1,
                }),
            }
        }
    }

    #[cfg(feature = "ownerfs")]
    struct ReleaseTestHandler {
        mode: ReleaseFailureMode,
        attempts: TestAtomicUsize,
        seen_handles: StdMutex<Vec<Vec<u8>>>,
    }

    #[cfg(feature = "ownerfs")]
    impl ReleaseTestHandler {
        fn transient_failures(failures: usize) -> Self {
            Self {
                mode: ReleaseFailureMode::Transient {
                    remaining: TestAtomicUsize::new(failures),
                },
                attempts: TestAtomicUsize::new(0),
                seen_handles: StdMutex::new(Vec::new()),
            }
        }

        fn stale_handle() -> Self {
            Self {
                mode: ReleaseFailureMode::Stale,
                attempts: TestAtomicUsize::new(0),
                seen_handles: StdMutex::new(Vec::new()),
            }
        }

        fn attempts(&self) -> usize {
            self.attempts.load(TestOrdering::SeqCst)
        }

        fn seen_handles(&self) -> Vec<Vec<u8>> {
            self.seen_handles.lock().unwrap().clone()
        }
    }

    #[cfg(feature = "ownerfs")]
    impl OwnerFilesHandler for ReleaseTestHandler {
        fn release(
            &self,
            _authenticated_peer_node_id: &str,
            request: OwnerReleaseRequest,
        ) -> afs_error::Result<OwnerReleaseReply> {
            self.attempts.fetch_add(1, TestOrdering::SeqCst);
            let handle = request
                .handle
                .ok_or_else(|| {
                    afs_error::Error::coded(
                        afs_error::CLIENT_ARGUMENT_INVALID,
                        "missing test handle",
                    )
                })?
                .opaque;
            self.seen_handles.lock().unwrap().push(handle);
            match &self.mode {
                ReleaseFailureMode::Transient { remaining } => {
                    if remaining
                        .fetch_update(TestOrdering::SeqCst, TestOrdering::SeqCst, |current| {
                            (current > 0).then(|| current - 1)
                        })
                        .is_ok()
                    {
                        return Err(afs_error::Error::coded(
                            afs_error::CLIENT_CONNECTION_UNAVAILABLE,
                            "transient release failure",
                        ));
                    }
                    Ok(OwnerReleaseReply {})
                }
                ReleaseFailureMode::Stale => Err(afs_error::Error::coded(
                    afs_error::NODE_OWNER_STALE_HANDLE,
                    "stale release handle",
                )),
            }
        }
    }

    #[cfg(feature = "ownerfs")]
    async fn spawn_owner_release_server(
        handler: std::sync::Arc<ReleaseTestHandler>,
    ) -> (Channel, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(make_owner_files_server_with_handler(
                    handler,
                    std::sync::Arc::new(AllowPeer),
                ))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let channel = Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        (channel, server)
    }

    #[cfg(feature = "ownerfs")]
    async fn spawn_owner_payload_server(
        handler: std::sync::Arc<PayloadMetricsHandler>,
        metrics: crate::node::rpc::OwnerRpcMetrics,
    ) -> (Channel, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(make_owner_files_server_with_handler_and_metrics(
                    handler,
                    std::sync::Arc::new(AllowPeer),
                    metrics,
                ))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let channel = Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        (channel, server)
    }

    #[cfg(feature = "ownerfs")]
    fn owner_rpc_histogram_count(
        registry: &afs_metrics::Registry,
        side: &str,
        method: &str,
    ) -> u64 {
        afs_metrics::encode_text(registry)
            .unwrap()
            .lines()
            .find_map(|line| {
                if line.starts_with("afs_ownerfiles_rpc_duration_seconds_count{")
                    && line.contains(&format!("side=\"{side}\""))
                    && line.contains(&format!("method=\"{method}\""))
                {
                    line.rsplit_once(' ')
                        .and_then(|(_, value)| value.parse::<u64>().ok())
                } else {
                    None
                }
            })
            .unwrap_or(0)
    }

    #[cfg(feature = "ownerfs")]
    fn owner_payload_bytes(
        registry: &afs_metrics::Registry,
        side: &str,
        direction: &str,
        plane: &str,
    ) -> u64 {
        afs_metrics::encode_text(registry)
            .unwrap()
            .lines()
            .find_map(|line| {
                if line.starts_with("afs_ownerfiles_payload_bytes_total{")
                    && line.contains(&format!("side=\"{side}\""))
                    && line.contains(&format!("direction=\"{direction}\""))
                    && line.contains(&format!("plane=\"{plane}\""))
                {
                    line.rsplit_once(' ')
                        .and_then(|(_, value)| value.parse::<u64>().ok())
                } else {
                    None
                }
            })
            .unwrap_or(0)
    }

    #[cfg(feature = "ownerfs")]
    fn test_grant() -> RootGrant {
        RootGrant {
            id: RootId("root-a".to_owned()),
            epoch: 1,
            home_node_id: "node-a".to_owned(),
            home_session_id: "home-session-a".to_owned(),
            holder_node_id: "node-b".to_owned(),
            session_id: "grant-session-b".to_owned(),
            access_generation: 1,
            rights: vec![RootRight::Write],
            fencing_token: "fence-a".to_owned(),
        }
    }

    #[cfg(feature = "ownerfs")]
    fn test_remote_file(handle: Vec<u8>) -> RemoteFile {
        RemoteFile {
            root_id: RootId("root-a".to_owned()),
            owner_node_id: "node-a".to_owned(),
            owner_session_id: "home-session-a".to_owned(),
            identity: FileIdentity(b"identity-a".to_vec()),
            handle,
        }
    }

    #[cfg(feature = "ownerfs")]
    async fn wait_until(deadline: Duration, condition: impl Fn() -> bool) {
        let start = Instant::now();
        while start.elapsed() < deadline {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("condition did not become true before timeout");
    }
    #[cfg(feature = "dfs")]
    fn range_request() -> DfsReadRangesRequest {
        DfsReadRangesRequest {
            include_blake3_outboard: false,
            read_id: "read".into(),
            attempt_id: "attempt".into(),
            file_version_id: "version".into(),
            layout_root_id: "layout".into(),
            operations: (0..2)
                .map(|index| DfsChunkReadOp {
                    operation_index: index,
                    chunk_id: format!("chunk-{index}"),
                    chunk_offset: 0,
                    length: 1,
                    destination_offset: u64::from(index),
                    source_copy_id: format!("copy-{index}"),
                    grant: None,
                })
                .collect(),
        }
    }

    #[cfg(feature = "dfs")]
    fn range_frames(
        request: &DfsReadRangesRequest,
        index: usize,
    ) -> Vec<afs_protocol::node_data::DfsReadRangesFrame> {
        use afs_protocol::node_data::{
            DfsReadRangesCompletion, DfsReadRangesFrame, DfsReadRangesHeader,
        };
        let op = &request.operations[index];
        vec![
            DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
                    crc32c_table: Default::default(),
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: op.operation_index,
                    chunk_id: op.chunk_id.clone(),
                    chunk_offset: op.chunk_offset,
                    length: op.length,
                    source_copy_id: op.source_copy_id.clone(),
                    blake3_outboard: Default::default(),
                })),
            },
            DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Data(
                    vec![b'a' + index as u8].into(),
                )),
            },
            DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Completion(
                    DfsReadRangesCompletion {
                        crc32c_table: Default::default(),
                        read_id: request.read_id.clone(),
                        attempt_id: request.attempt_id.clone(),
                        operation_index: op.operation_index,
                        source_copy_id: op.source_copy_id.clone(),
                        transferred_bytes: op.length,
                        range_checksum: blake3::hash(&[b'a' + index as u8]).as_bytes().to_vec(),
                        range_checksum_algorithm:
                            afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32,
                    },
                )),
            },
        ]
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dfs_peer_receive_cap_reduces_actual_http2_data_frames() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut observations = Vec::new();
        for production in [false, true] {
            let payload = vec![37; 128 * 1024];
            let chunk = crate::node::chunk::StagedChunk::new(
                crate::dfs::OperationId::new("dfs-peer-receive-frames"),
                payload.clone(),
            )
            .chunk;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server_address = listener.local_addr().unwrap();
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let fixture = DiskStreamFixture {
                chunk: chunk.clone(),
                body: Arc::new(payload.clone()),
                mode: 0,
                calls: calls.clone(),
                fetch_objects: None,
                fetch_gate: None,
            };
            let (stop_server, server_done) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(
                Server::builder()
                    .add_service(
                        afs_protocol::node_data::dfs_chunks_server::DfsChunksServer::new(fixture),
                    )
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                        let _ = server_done.await;
                    }),
            );

            let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", proxy_listener.local_addr().unwrap());
            let data_frames = Arc::new(std::sync::Mutex::new(Vec::new()));
            let frames = data_frames.clone();
            let (stop_proxy, proxy_done) = tokio::sync::oneshot::channel();
            let proxy = tokio::spawn(async move {
                let (client, _) = proxy_listener.accept().await.unwrap();
                let upstream = tokio::net::TcpStream::connect(server_address)
                    .await
                    .unwrap();
                let (mut client_read, mut client_write) = client.into_split();
                let (mut upstream_read, mut upstream_write) = upstream.into_split();
                let responses = async {
                    loop {
                        let mut header = [0; 9];
                        if let Err(error) = upstream_read.read_exact(&mut header).await {
                            assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
                            break;
                        }
                        let length = (usize::from(header[0]) << 16)
                            | (usize::from(header[1]) << 8)
                            | usize::from(header[2]);
                        assert!(length <= 2 * 1024 * 1024);
                        let mut body = vec![0; length];
                        upstream_read.read_exact(&mut body).await.unwrap();
                        let stream_id =
                            u32::from_be_bytes([header[5] & 0x7f, header[6], header[7], header[8]]);
                        if header[3] == 0 && stream_id != 0 && length != 0 {
                            frames.lock().unwrap().push(length);
                        }
                        client_write.write_all(&header).await.unwrap();
                        client_write.write_all(&body).await.unwrap();
                    }
                };
                tokio::select! {
                    result = tokio::io::copy(&mut client_read, &mut upstream_write) => { result.unwrap(); }
                    () = responses => {}
                    _ = proxy_done => {}
                }
            });

            let config = afs_transport::GrpcConfig::default();
            assert_eq!(config.initial_connection_window_size, 2 * 1024 * 1024);
            assert_eq!(config.initial_stream_window_size, 2 * 1024 * 1024);
            let pool =
                PeerConnectionPool::new(config.clone(), afs_transport::TlsConfig::Disabled, 1)
                    .unwrap();
            let channel = if production {
                pool.channel("node-a", 1, &endpoint).await.unwrap()
            } else {
                config
                    .configure_client(Endpoint::from_shared(endpoint).unwrap())
                    .max_frame_size(16 * 1024)
                    .connect_lazy()
            };
            let mut client =
                afs_protocol::node_data::dfs_chunks_client::DfsChunksClient::new(channel);
            let mut request = range_request();
            request.operations.truncate(1);
            request.operations[0].chunk_id = chunk.id.0;
            request.operations[0].length = payload.len() as u64;
            let stream = client
                .read_ranges(request.clone())
                .await
                .unwrap()
                .into_inner();
            let mut output = vec![0; payload.len()];
            consume_peer_read_stream(&request, None, &mut output, stream, None, None)
                .await
                .unwrap();
            assert_eq!(output, payload);
            assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
            observations.push(data_frames.lock().unwrap().clone());

            drop(client);
            drop(pool);
            let _ = stop_proxy.send(());
            tokio::time::timeout(Duration::from_secs(3), proxy)
                .await
                .unwrap()
                .unwrap();
            let _ = stop_server.send(());
            tokio::time::timeout(Duration::from_secs(3), server)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }

        eprintln!(
            "actual HTTP2 DATA payload lengths: control={:?}, production={:?}",
            observations[0], observations[1]
        );
        assert!(observations[0].iter().all(|length| *length <= 16 * 1024));
        assert!(observations[1].len() < observations[0].len());
        assert!(observations[1].iter().any(|length| *length > 16 * 1024));
        assert_eq!(
            observations[0].iter().sum::<usize>(),
            observations[1].iter().sum::<usize>()
        );
    }

    #[cfg(feature = "dfs")]
    fn verified_object_frames(
        body: &[u8],
    ) -> (
        ChunkObject,
        DfsReadRangesRequest,
        Vec<afs_protocol::node_data::DfsReadRangesFrame>,
    ) {
        use afs_protocol::node_data::{DfsReadRangesCompletion, DfsReadRangesFrame};

        let chunk = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("verified-stream"),
            body.to_vec(),
        )
        .chunk;
        let mut request = range_request();
        request.include_blake3_outboard = true;
        request.operations.truncate(1);
        request.operations[0].chunk_id = chunk.id.0.clone();
        request.operations[0].length = chunk.length;
        let mut frames = vec![range_frames(&request, 0).remove(0)];
        for bytes in body.chunks(64 * 1024 - 1) {
            frames.push(DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Data(bytes.to_vec().into())),
            });
        }
        frames.push(DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Completion(
                DfsReadRangesCompletion {
                    crc32c_table: Default::default(),
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: 0,
                    source_copy_id: request.operations[0].source_copy_id.clone(),
                    transferred_bytes: chunk.length,
                    range_checksum: chunk.content_digest.bytes.to_vec(),
                    range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                        as i32,
                },
            )),
        });
        (chunk, request, frames)
    }

    #[cfg(feature = "dfs")]
    fn wi6_encoded_outboard(body: &[u8]) -> Vec<u8> {
        crate::node::chunk::outboard::build_borrowed(body, &OutboardBudget::new(1024 * 1024))
            .outboard
            .unwrap()
            .encoded()
            .to_vec()
    }

    #[cfg(feature = "dfs")]
    fn wi6_frames_with_outboard(
        body: &[u8],
        encoded: &[u8],
    ) -> (
        ChunkObject,
        DfsReadRangesRequest,
        Vec<afs_protocol::node_data::DfsReadRangesFrame>,
    ) {
        let (chunk, request, mut frames) = verified_object_frames(body);
        let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body else {
            panic!("header");
        };
        header.blake3_outboard = encoded.to_vec().into();
        (chunk, request, frames)
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn wi6_peer_outboard_binds_actual_body_and_moves_shared_budget_until_last_owner() {
        let body = vec![71; 96 * 1024 + 17];
        let encoded = wi6_encoded_outboard(&body);
        let (chunk, request, frames) = wi6_frames_with_outboard(&body, &encoded);
        let budget = OutboardBudget::new(1024 * 1024);
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let mut decoder = ReadFrameDecoder::for_object(&request, &chunk)
            .unwrap()
            .with_outboard_budget(Some(budget.clone()));
        decoder.work_metrics = Some(metrics);
        let mut output = vec![0; body.len()];
        for frame in frames {
            decoder.accept(frame, &mut output).unwrap();
        }
        let proof = decoder.finish_object(&output).unwrap();
        assert_eq!(proof.bytes(), body);
        assert_eq!(proof.root_digest(), &chunk.content_digest.bytes);
        let outboard = proof.into_outboard().unwrap();
        assert_eq!(outboard.encoded(), encoded);
        assert!(budget.used() >= ChunkOutboard::resident_charge(encoded.len()).unwrap());
        let retained = outboard.clone();
        drop(outboard);
        assert!(
            budget.used() > 0,
            "cache/slow holders keep the same metadata lease"
        );
        drop(retained);
        assert_eq!(budget.used(), 0);
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(
            text.contains(&format!(
                "afs_dfs_read_hash_bytes_total{{stage=\"outboard_body_hash\"}} {}\n",
                body.len()
            )),
            "{text}"
        );
        assert!(
            text.contains(&format!(
                "afs_dfs_read_copy_bytes_total{{stage=\"peer_outboard_metadata\"}} {}\n",
                encoded.len()
            )),
            "{text}"
        );
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn wi6_peer_outboard_rejects_shape_cv_replay_and_partial_capabilities() {
        let body = vec![71; 96 * 1024 + 17];
        let encoded = wi6_encoded_outboard(&body);
        for mode in 0..6 {
            let mut supplied = encoded.clone();
            match mode {
                0 => supplied[0] ^= 1, // unknown format
                1 => {
                    supplied.pop();
                } // truncated parent CVs
                2 => supplied.resize(8193, 0),
                3 => supplied[64] ^= 1, // valid shape, wrong body-bound parent
                _ => {}
            }
            let (mut chunk, request, frames) = wi6_frames_with_outboard(&body, &supplied);
            if mode == 4 {
                chunk.content_digest.bytes[0] ^= 1;
            }
            let budget = OutboardBudget::new(if mode == 5 { 0 } else { 1024 * 1024 });
            if mode == 5 {
                // Even a full pool cannot suppress validation of the fixed header.
                let mut malformed = frames;
                let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut malformed[0].body
                else {
                    panic!("header");
                };
                header.blake3_outboard = vec![0; 64].into();
                let mut decoder = ReadFrameDecoder::for_object(&request, &chunk)
                    .unwrap()
                    .with_outboard_budget(Some(budget.clone()));
                assert!(
                    decoder
                        .accept(malformed.remove(0), &mut vec![0; body.len()])
                        .is_err()
                );
                assert_eq!(budget.used(), 0);
                continue;
            }
            let mut output = vec![0; body.len()];
            let mut decoder = ReadFrameDecoder::for_object(&request, &chunk)
                .unwrap()
                .with_outboard_budget(Some(budget.clone()));
            let rejected = match frames
                .into_iter()
                .try_for_each(|frame| decoder.accept(frame, &mut output))
            {
                Err(_) => {
                    drop(decoder);
                    true
                }
                Ok(()) => decoder.finish_object(&output).is_err(),
            };
            assert!(rejected, "mode {mode}");
            assert_eq!(budget.used(), 0, "rejected proof lease must drain");
        }
        let mut request = range_request();
        request.operations.truncate(1);
        let mut frame = range_frames(&request, 0).remove(0);
        let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frame.body else {
            panic!("header");
        };
        header.blake3_outboard = encoded.into();
        let mut decoder = ReadFrameDecoder::new(&request);
        let mut output = [99];
        assert!(decoder.accept(frame, &mut output).is_err());
        assert_eq!(
            output,
            [99],
            "partial proof rejection must precede body writes"
        );
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn wi6_peer_producer_retires_wire_backing_and_keeps_standard_root() {
        use crate::node::peer_cache::{DiskPeerCache, PeerCacheOptions};
        let body = vec![47; 2 * STREAM_FRAME_BACKING_BYTES + 31];
        let encoded = wi6_encoded_outboard(&body);
        let (chunk, request, mut frames) = wi6_frames_with_outboard(&body, &encoded);
        let directory = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(PeerCacheOptions {
            directory: directory.path().join("cache"),
            capacity_bytes: 8 * 1024 * 1024,
            namespace_id: crate::dfs::NamespaceId::new("test"),
        })
        .unwrap();
        let mut operation = cache.operation().unwrap();
        let mut sink = operation.begin_stream(&chunk).unwrap();
        let mut output = [0; 31];
        let binding = sink.bind_output(0, &output).unwrap();
        let budget = sink.proof_budget();
        let baseline = budget.used();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let observation = PeerConsumerObservation {
            metrics: Some(&metrics),
            diagnostic: None,
        };
        let mut producer = PeerRootProducer::new(chunk.length, binding.clone(), None, deadline)
            .with_outboard_context(chunk.clone(), budget.clone());
        let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body else {
            panic!("header");
        };
        producer
            .accept_outboard_header(header, Some(&metrics))
            .unwrap();
        assert!(
            header.blake3_outboard.is_empty(),
            "opaque decoder owner must not reach control queue"
        );
        peer_stream_control_capacity(&mut frames[0]).unwrap();
        let mut decoder = ReadFrameDecoder::for_producer_object(&request, &chunk).unwrap();
        decoder.stream_binding = Some(binding);
        decoder
            .install_producer_outboard(producer.outboard_builder.take())
            .unwrap();
        assert!(
            producer.outboard_builder.is_none(),
            "producer hands metadata work to the consumer"
        );
        let mut offset = 0;
        for frame in frames {
            if let Some(dfs_read_ranges_frame::Body::Data(data)) = &frame.body {
                producer.update(offset, data, Some(&metrics)).unwrap();
                if offset == 0 {
                    let text = afs_metrics::encode_text(&registry).unwrap();
                    assert!(
                        text.contains(
                            "afs_dfs_read_hash_bytes_total{stage=\"outboard_body_hash\"} 0\n"
                        ),
                        "producer root must not execute consumer outboard work: {text}"
                    );
                }
                offset += data.len();
            }
            consume_peer_frame_into_sink(
                &mut decoder,
                frame,
                0,
                &mut output,
                &mut sink,
                deadline,
                observation,
            )
            .unwrap();
        }
        assert!(
            decoder.verified_object.as_ref().unwrap().outboard.is_none(),
            "Completion cannot mint a proof before real EOF/join"
        );
        let receipt = producer.finish_after_eof().unwrap();
        assert!(matches!(receipt.digest, PeerRootDigest::ConsumerTree));
        let proof = decoder
            .finish_stream_from_producer(&chunk, receipt, &sink, 0, &output, observation)
            .unwrap();
        let outboard = proof.into_outboard().unwrap();
        assert_eq!(outboard.encoded(), encoded);
        let text = afs_metrics::encode_text(&registry).unwrap();
        for (stage, expected) in [("root_in_receive", 0), ("outboard_body_hash", body.len())] {
            assert!(
                text.contains(&format!(
                    "afs_dfs_read_hash_bytes_total{{stage=\"{stage}\"}} {expected}\n"
                )),
                "{text}"
            );
        }
        let retained = outboard.clone();
        drop(outboard);
        assert!(
            budget.used() > baseline,
            "last shared proof holder must retain its lease"
        );
        drop(retained);
        assert_eq!(
            budget.used(),
            baseline,
            "proof retirement preserves the live cache baseline"
        );
        assert_eq!(output.as_slice(), &body[..output.len()]);
        drop(sink);
        drop(operation);
        cache.shutdown().unwrap();
        assert_eq!(
            budget.used(),
            0,
            "shutdown retires the cache's fixed index charge"
        );
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn wi6_r_producer_hash_is_reserved_for_header_fallbacks() {
        use crate::node::peer_cache::{DiskPeerCache, PeerCacheOptions};
        let body = vec![83; 96 * 1024 + 17];
        let encoded = wi6_encoded_outboard(&body);
        for mode in 0..4 {
            let budget = OutboardBudget::new(if mode == 2 { 0 } else { 1024 * 1024 });
            let directory = tempfile::tempdir().unwrap();
            let cache = DiskPeerCache::open_with_outboard_budget(
                PeerCacheOptions {
                    directory: directory.path().join("cache"),
                    capacity_bytes: 8 * 1024 * 1024,
                    namespace_id: crate::dfs::NamespaceId::new("test"),
                },
                budget.clone(),
            )
            .unwrap();
            let mut operation = cache.operation().unwrap();
            let (chunk, _, mut frames) = wi6_frames_with_outboard(&body, &encoded);
            let sink = operation.begin_stream(&chunk).unwrap();
            let output = [0; 31];
            let remaining = budget.limit() - budget.used();
            let holder = (mode == 3).then(|| budget.try_reserve(remaining).unwrap());
            let registry = afs_metrics::Registry::new();
            let metrics = ReadWorkMetrics::register(&registry).unwrap();
            let mut producer = PeerRootProducer::new(
                chunk.length,
                sink.bind_output(0, &output).unwrap(),
                None,
                std::time::Instant::now() + Duration::from_secs(5),
            )
            .with_outboard_context(chunk.clone(), budget);
            let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body else {
                panic!("header");
            };
            if mode == 1 {
                header.blake3_outboard = Default::default();
            }
            producer
                .accept_outboard_header(header, Some(&metrics))
                .unwrap();
            producer.update(0, &body, Some(&metrics)).unwrap();
            let expected = if mode <= 1 { 0 } else { body.len() };
            let text = afs_metrics::encode_text(&registry).unwrap();
            assert!(
                text.contains(&format!(
                    "afs_dfs_read_hash_bytes_total{{stage=\"root_in_receive\"}} {expected}\n"
                )),
                "mode {mode}: consumer tree must replace only eligible duplicate producer work: {text}"
            );
            drop(holder);
        }
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn wi6_peer_outboard_budget_exhaustion_keeps_full_root_and_eof_contract() {
        let body = vec![71; 96 * 1024 + 17];
        let encoded = wi6_encoded_outboard(&body);
        for occupied in [false, true] {
            let budget = OutboardBudget::new(if occupied { 8192 } else { 0 });
            let holder = occupied.then(|| budget.try_reserve(8192).unwrap());
            let (chunk, request, frames) = wi6_frames_with_outboard(&body, &encoded);
            let mut decoder = ReadFrameDecoder::for_object(&request, &chunk)
                .unwrap()
                .with_outboard_budget(Some(budget.clone()));
            let mut output = vec![0; body.len()];
            for frame in frames {
                decoder.accept(frame, &mut output).unwrap();
            }
            let proof = decoder.finish_object(&output).unwrap();
            assert_eq!(proof.bytes(), body);
            assert!(
                proof.into_outboard().is_none(),
                "unused CVs cannot mint a proof"
            );
            drop(holder);
            assert_eq!(budget.used(), 0);

            let (mut wrong, request, mut frames) = wi6_frames_with_outboard(&body, &encoded);
            wrong.content_digest.bytes[0] ^= 1;
            // A legacy peer omits optional proof; its self-consistent checksum
            // still cannot replace the trusted Meta root, even with budget zero.
            let Some(dfs_read_ranges_frame::Body::Header(header)) = &mut frames[0].body else {
                panic!("header");
            };
            header.blake3_outboard = Default::default();
            let mut decoder = ReadFrameDecoder::for_object(&request, &wrong)
                .unwrap()
                .with_outboard_budget(Some(OutboardBudget::new(0)));
            let mut output = vec![0; body.len()];
            assert!(
                frames
                    .into_iter()
                    .try_for_each(|frame| decoder.accept(frame, &mut output))
                    .is_err()
            );
        }
    }

    #[cfg(feature = "dfs")]
    async fn r3_assert_legacy_complete_stream_hashes_body_once(budget_limit: usize) {
        let body: Vec<u8> = (0..(2 * STREAM_FRAME_BACKING_BYTES + 17))
            .map(|index| (index % 251) as u8)
            .collect();
        let (chunk, request, frames) = verified_object_frames(&body);
        assert!(matches!(
            &frames[0].body,
            Some(dfs_read_ranges_frame::Body::Header(header))
                if header.blake3_outboard.is_empty()
        ));
        assert!(matches!(
            &frames.last().unwrap().body,
            Some(dfs_read_ranges_frame::Body::Completion(_))
        ));
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let budget = OutboardBudget::new(budget_limit);
        let mut out = vec![0xa5; body.len()];
        // Reuse the complete stream decoder, including Header/body/Completion
        // and real iterator EOF. This is not producer-only prefix accounting.
        let verified = consume_peer_read_stream(
            &request,
            Some(&chunk),
            &mut out,
            tokio_stream::iter(frames.into_iter().map(Ok)),
            Some(&metrics),
            Some(budget.clone()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(verified.bytes(), body);
        assert_eq!(verified.chunk(), &chunk);
        if budget_limit == 0 {
            assert_eq!(budget.used(), 0, "root-only fallback needs no proof lease");
        } else {
            assert!(budget.used() > 0, "legacy body built a charged cache tree");
        }
        let text = afs_metrics::encode_text(&registry).unwrap();
        let hash_bytes = |stage: &str| {
            let prefix = format!("afs_dfs_read_hash_bytes_total{{stage=\"{stage}\"}} ");
            text.lines()
                .find_map(|line| line.strip_prefix(&prefix))
                .unwrap()
                .parse::<u64>()
                .unwrap()
        };
        let plain = hash_bytes("root_in_receive");
        let builder = hash_bytes("outboard_body_hash");
        drop(verified);
        assert_eq!(budget.used(), 0, "last proof holder releases its charge");
        assert_eq!(
            plain + builder,
            body.len() as u64,
            "legacy complete stream scanned body twice: plain={plain}, builder={builder}, budget={budget_limit}"
        );
        assert_eq!(
            plain,
            if budget_limit == 0 {
                body.len() as u64
            } else {
                0
            }
        );
        assert_eq!(
            builder,
            if budget_limit == 0 {
                0
            } else {
                body.len() as u64
            }
        );
        let fallback_prefix = "afs_dfs_peer_read_events_total{event=\"root_fallback\"} ";
        let fallbacks: u64 = text
            .lines()
            .find_map(|line| line.strip_prefix(fallback_prefix))
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(fallbacks, u64::from(budget_limit == 0));
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn r3_legacy_complete_stream_with_charged_tree_hashes_body_once() {
        r3_assert_legacy_complete_stream_hashes_body_once(1024 * 1024).await;
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn r3_legacy_complete_stream_with_root_only_budget_hashes_body_once() {
        r3_assert_legacy_complete_stream_hashes_body_once(0).await;
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn r3_legacy_tree_and_plain_roots_cannot_publish_before_real_eof() {
        let body = vec![29; 2 * STREAM_FRAME_BACKING_BYTES + 17];
        for budget_limit in [0, 1024 * 1024] {
            let (chunk, request, frames) = verified_object_frames(&body);
            let mut stream: Vec<_> = frames.into_iter().map(Ok).collect();
            stream.push(Err(Status::unavailable(
                "late status after complete legacy body",
            )));
            let budget = OutboardBudget::new(budget_limit);
            let mut private = vec![0xa5; body.len()];
            assert!(
                consume_peer_read_stream(
                    &request,
                    Some(&chunk),
                    &mut private,
                    tokio_stream::iter(stream),
                    None,
                    Some(budget.clone()),
                )
                .await
                .is_err(),
                "Completion alone cannot return a verified object"
            );
            assert_eq!(
                budget.used(),
                0,
                "late failure retires all optional proof owners"
            );
        }
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn verified_peer_stream_hashes_once_and_binds_trusted_root() {
        let body: Vec<u8> = (0..(1024 * 1024) + 17)
            .map(|index| (index % 251) as u8)
            .collect();
        let (chunk, request, frames) = verified_object_frames(&body);
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let mut out = vec![0; body.len()];
        let verified = consume_peer_read_stream(
            &request,
            Some(&chunk),
            &mut out,
            tokio_stream::iter(frames.into_iter().map(Ok)),
            Some(&metrics),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(verified.bytes(), body);
        assert_eq!(verified.chunk(), &chunk);
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(
            text.contains(&format!(
                "afs_dfs_read_hash_bytes_total{{stage=\"root_in_receive\"}} {}\n",
                body.len()
            )),
            "{text}"
        );
        assert!(
            text.contains("afs_dfs_read_hash_bytes_total{stage=\"root_fallback\"} 0\n"),
            "{text}"
        );
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn verified_peer_stream_rejects_server_consistent_wrong_meta_root() {
        let (mut chunk, request, frames) = verified_object_frames(b"peer-consistent body");
        chunk.content_digest.bytes[0] ^= 1;
        let mut out = vec![0; chunk.length as usize];
        let error = consume_peer_read_stream(
            &request,
            Some(&chunk),
            &mut out,
            tokio_stream::iter(frames.into_iter().map(Ok)),
            None,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn verified_peer_stream_requires_eof_after_complete_identity_checked_frames() {
        let (chunk, request, frames) = verified_object_frames(b"complete body");
        let mut missing_completion: Vec<_> = frames.iter().cloned().map(Ok).collect();
        missing_completion.pop();
        let mut late_status: Vec<_> = frames.iter().cloned().map(Ok).collect();
        late_status.push(Err(tonic::Status::unavailable("late stream failure")));
        let mut extra_data: Vec<_> = frames.into_iter().map(Ok).collect();
        extra_data.push(Ok(afs_protocol::node_data::DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Data(vec![9].into())),
        }));
        for stream in [missing_completion, late_status, extra_data] {
            let mut out = vec![0; chunk.length as usize];
            assert!(
                consume_peer_read_stream(
                    &request,
                    Some(&chunk),
                    &mut out,
                    tokio_stream::iter(stream),
                    None,
                    None,
                )
                .await
                .is_err()
            );
        }
        let mut partial = request;
        partial.operations[0].length -= 1;
        assert!(ReadFrameDecoder::for_object(&partial, &chunk).is_err());
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn verified_peer_proof_binds_private_buffer_and_cross_frame_outboard() {
        let body = vec![7; (1024 * 1024) + 17];
        let (chunk, request, frames) = verified_object_frames(&body);
        let mut out = vec![0; body.len()];
        let budget = OutboardBudget::new(1024 * 1024);
        let mut decoder = ReadFrameDecoder::for_object(&request, &chunk)
            .unwrap()
            .with_outboard_budget(Some(budget.clone()));
        for frame in frames.clone() {
            decoder.accept(frame, &mut out).unwrap();
        }
        let proof = decoder.finish_object(&out).unwrap();
        assert_eq!(proof.root_digest(), &chunk.content_digest.bytes);
        assert_eq!(proof.bytes().as_ptr(), out.as_ptr());
        let outboard = proof.into_outboard().unwrap();
        assert_eq!(outboard.digest(), &chunk.content_digest);
        let official = crate::node::chunk::outboard::build_borrowed(&body, &budget);
        assert_eq!(outboard.encoded(), official.outboard.unwrap().encoded());

        let mut decoder = ReadFrameDecoder::for_object(&request, &chunk).unwrap();
        for frame in frames {
            decoder.accept(frame, &mut out).unwrap();
        }
        let different = out.clone();
        assert!(decoder.finish_object(&different).is_err());
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn range_frame_bytes_preserve_vec_wire_compatibility() {
        use afs_protocol::node_data::{
            DfsReadRangesCompletion, DfsReadRangesFrame, DfsReadRangesHeader,
        };
        use prost::Message;

        // Retain the former generated representation, including all oneof
        // tags, so both directions exercise the actual protobuf boundary.
        #[derive(Clone, PartialEq, prost::Message)]
        struct LegacyFrame {
            #[prost(oneof = "LegacyBody", tags = "1, 2, 3")]
            body: Option<LegacyBody>,
        }

        #[derive(Clone, PartialEq, prost::Oneof)]
        enum LegacyBody {
            #[prost(message, tag = "1")]
            Header(DfsReadRangesHeader),
            #[prost(bytes = "vec", tag = "2")]
            Data(Vec<u8>),
            #[prost(message, tag = "3")]
            Completion(DfsReadRangesCompletion),
        }

        let request = range_request();
        let mut frames = range_frames(&request, 0);
        frames.extend(range_frames(&request, 1));
        frames.push(DfsReadRangesFrame { body: None });
        for payload in [
            Vec::new(),
            vec![0xff],
            (0..64 * 1024).map(|index| (index % 251) as u8).collect(),
        ] {
            frames.push(DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Data(payload.into())),
            });
        }
        for frame in frames {
            let legacy = LegacyFrame {
                body: frame.body.as_ref().map(|body| match body {
                    dfs_read_ranges_frame::Body::Header(header) => {
                        LegacyBody::Header(header.clone())
                    }
                    dfs_read_ranges_frame::Body::Data(data) => {
                        let data: &prost::bytes::Bytes = data;
                        LegacyBody::Data(data.to_vec())
                    }
                    dfs_read_ranges_frame::Body::Completion(completion) => {
                        LegacyBody::Completion(completion.clone())
                    }
                }),
            };
            let old_wire = legacy.encode_to_vec();
            let new_wire = frame.encode_to_vec();
            assert_eq!(old_wire, new_wire);
            assert_eq!(
                DfsReadRangesFrame::decode(old_wire.as_slice()).unwrap(),
                frame
            );
            assert_eq!(LegacyFrame::decode(new_wire.as_slice()).unwrap(), legacy);
        }
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn range_decoder_requires_every_operation_and_completion() {
        let request = range_request();
        assert!(ReadFrameDecoder::new(&request).finish().is_err());
        let mut partial = ReadFrameDecoder::new(&request);
        let mut out = [0; 2];
        for frame in range_frames(&request, 0) {
            partial.accept(frame, &mut out).unwrap();
        }
        assert!(partial.finish().is_err());
        let mut complete = ReadFrameDecoder::new(&request);
        for index in 0..2 {
            for frame in range_frames(&request, index) {
                complete.accept(frame, &mut out).unwrap();
            }
        }
        complete.finish().unwrap();
        assert_eq!(&out, b"ab");
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn range_decoder_rejects_duplicate_stale_and_corrupt_frames() {
        let request = range_request();
        let mut out = [0; 2];
        let mut decoder = ReadFrameDecoder::new(&request);
        for frame in range_frames(&request, 0) {
            decoder.accept(frame, &mut out).unwrap();
        }
        assert!(
            decoder
                .accept(range_frames(&request, 0).remove(0), &mut out)
                .is_err()
        );
        for malformed in 0..4 {
            let mut frames = range_frames(&request, 0);
            if let Some(dfs_read_ranges_frame::Body::Completion(completion)) = &mut frames[2].body {
                match malformed {
                    0 => completion.attempt_id = "old-attempt".into(),
                    1 => completion.operation_index = 1,
                    2 => completion.range_checksum[0] ^= 1,
                    _ => completion.source_copy_id = "other-copy".into(),
                }
            }
            let mut decoder = ReadFrameDecoder::new(&request);
            decoder.accept(frames.remove(0), &mut out).unwrap();
            decoder.accept(frames.remove(0), &mut out).unwrap();
            let error = decoder.accept(frames.remove(0), &mut out).unwrap_err();
            if malformed == 2 {
                assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
            } else {
                assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
            }
        }
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn peer_stream_shared_work_pins_one_channel_and_drains_after_last_block() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let pool = PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            1,
        )
        .unwrap()
        .with_read_work_metrics(metrics.clone());
        let endpoint = "http://127.0.0.1:9";
        let first = pool.acquire_stream_work("a", 1, endpoint).await.unwrap();
        let second = pool.acquire_stream_work("a", 1, endpoint).await.unwrap();
        assert!(Arc::ptr_eq(&first.session, &second.session));
        assert_eq!(first.session.blocks.lock().unwrap().len(), 2);
        let gauge = |name: &str| {
            let families = registry.gather();
            families
                .iter()
                .find(|family| family.name() == name)
                .unwrap()
                .get_metric()[0]
                .get_gauge()
                .get_value()
        };
        assert_eq!(
            gauge("afs_dfs_peer_disk_cache_stream_reserved_bytes"),
            (4 * 1024 * 1024) as f64
        );
        assert_eq!(
            gauge("afs_dfs_peer_disk_cache_stream_frame_bytes"),
            (2 * STREAM_FRAME_BACKING_BYTES) as f64
        );
        assert_eq!(
            first.session.controls.available_permits(),
            STREAM_FRAME_BACKING_BYTES
        );
        metrics.peer_stream_private_output(32 * 1024);
        assert_eq!(
            gauge("afs_dfs_peer_disk_cache_stream_reserved_bytes"),
            (4 * 1024 * 1024) as f64
        );
        assert!(
            pool.acquire_stream_work(&"a".repeat(STREAM_KEY_BYTES + 1), 1, endpoint)
                .await
                .is_err()
        );
        let controls = first
            .session
            .controls
            .clone()
            .acquire_many_owned(u32::try_from(STREAM_FRAME_BACKING_BYTES).unwrap())
            .await
            .unwrap();
        assert!(second.session.controls.clone().try_acquire_owned().is_err());
        drop(controls);
        let wait = Duration::from_millis(30);
        assert!(
            tokio::time::timeout(wait, pool.acquire_stream_work("a", 1, endpoint))
                .await
                .is_err()
        );
        // Even LRU eviction must not let a same-key replacement Channel enter
        // while the old connection still has active streams.
        pool.channel("b", 1, endpoint).await.unwrap();
        assert!(
            tokio::time::timeout(wait, pool.acquire_stream_work("b", 1, endpoint))
                .await
                .is_err()
        );
        let first_block = first.block().await.unwrap();
        let second_block = second.block().await.unwrap();
        assert_eq!(first.session.blocks.lock().unwrap().len(), 0);
        let waiting = tokio::spawn({
            let first = first.clone();
            async move { first.block().await }
        });
        tokio::task::yield_now().await;
        waiting.abort();
        assert!(waiting.await.is_err());
        let retired_session = first.session.clone();
        drop(first);
        drop(second);
        assert_eq!(
            pool.stream_work.state.lock().unwrap().active,
            2,
            "consumer-held blocks retain both operation tickets"
        );
        drop(first_block);
        assert_eq!(pool.stream_work.state.lock().unwrap().active, 1);
        drop(second_block);
        {
            let state = pool.stream_work.state.lock().unwrap();
            assert_eq!(state.active, 0);
            assert!(
                state.session.is_none(),
                "idle body allocations must be destroyed"
            );
        }
        assert!(
            retired_session.blocks.lock().unwrap().is_empty(),
            "actual buffers retire before zero admission, even while the session Arc is retained"
        );
        assert!(retired_session._work.lock().unwrap().is_none());
        for name in [
            "afs_dfs_peer_disk_cache_stream_frame_bytes",
            "afs_dfs_peer_disk_cache_stream_reserved_bytes",
        ] {
            let families = registry.gather();
            let family = families
                .iter()
                .find(|family| family.name() == name)
                .unwrap();
            assert_eq!(
                family.get_metric()[0].get_gauge().get_value(),
                0.0,
                "{name}"
            );
        }
        let next = pool.acquire_stream_work("b", 1, endpoint).await.unwrap();
        assert_eq!(next.session.blocks.lock().unwrap().len(), 2);
        drop(next);
        assert!(pool.stream_work.state.lock().unwrap().session.is_none());
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn peer_pool_owner_profile_is_separate_but_shares_epoch_fencing() {
        let pool = PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            4,
        )
        .unwrap();
        let endpoint = "http://127.0.0.1:9";
        pool.channel("peer", 1, endpoint).await.unwrap();
        pool.owner_files_channel("peer", 1, endpoint).await.unwrap();
        pool.owner_files_channel("peer", 1, endpoint).await.unwrap();
        {
            let state = pool.state.lock().await;
            assert_eq!(state.channels.len(), 2);
            assert!(
                state
                    .channels
                    .contains_key(&("peer".into(), 1, endpoint.into(), false))
            );
            assert!(
                state
                    .channels
                    .contains_key(&("peer".into(), 1, endpoint.into(), true))
            );
        }
        pool.owner_files_channel("peer", 2, endpoint).await.unwrap();
        assert_eq!(pool.state.lock().await.channels.len(), 1);
        assert!(pool.channel("peer", 1, endpoint).await.is_err());
        assert!(pool.long_wait_channel("peer", 1, endpoint).await.is_err());
        pool.channel("peer", 3, endpoint).await.unwrap();
        assert_eq!(pool.state.lock().await.channels.len(), 1);
        assert!(pool.owner_files_channel("peer", 2, endpoint).await.is_err());
    }

    #[tokio::test]
    async fn peer_pool_never_regresses_epoch_even_after_channel_eviction() {
        let pool = Arc::new(
            PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::Disabled,
                1,
            )
            .unwrap(),
        );
        let endpoint = "http://127.0.0.1:9";
        pool.channel("peer", 1, endpoint).await.unwrap();
        let release_old = Arc::new(tokio::sync::Notify::new());
        let delayed_pool = pool.clone();
        let delayed_gate = release_old.clone();
        let old_lookup = tokio::spawn(async move {
            delayed_gate.notified().await;
            delayed_pool.channel("peer", 1, endpoint).await
        });
        pool.channel("peer", 2, endpoint).await.unwrap();
        release_old.notify_one();
        assert!(old_lookup.await.unwrap().is_err());
        {
            let state = pool.state.lock().await;
            assert_eq!(state.high_water_epochs.get("peer"), Some(&2));
            assert!(
                state
                    .channels
                    .keys()
                    .all(|(node, epoch, _, _)| node != "peer" || *epoch == 2)
            );
        }
        pool.channel("other", 1, endpoint).await.unwrap(); // evicts peer's channel, not its epoch
        assert!(pool.channel("peer", 1, endpoint).await.is_err());
        pool.channel("peer", 3, endpoint).await.unwrap();
        assert!(pool.channel("peer", 2, endpoint).await.is_err());
        assert_eq!(
            pool.state.lock().await.high_water_epochs.get("peer"),
            Some(&3)
        );
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn dfs_auto_factories_without_rdma_pool_choose_grpc_and_required_rdma_fails() {
        let peers = Arc::new(
            PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::Disabled,
                4,
            )
            .unwrap(),
        );

        let replica = make_replica_data_plane(
            DataMode::Auto,
            peers.clone(),
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        assert_eq!(
            replica.mode(),
            crate::node::replication::ReplicaTransferMode::GrpcStream
        );
        assert!(
            make_replica_data_plane(
                DataMode::Rdma,
                peers.clone(),
                Duration::from_millis(10),
                None
            )
            .is_err()
        );

        let _transfer =
            make_chunk_transfer(DataMode::Auto, peers, Duration::from_millis(10), None).unwrap();
        assert!(
            make_chunk_transfer(
                DataMode::Rdma,
                Arc::new(
                    PeerConnectionPool::new(
                        afs_transport::GrpcConfig::default(),
                        afs_transport::TlsConfig::Disabled,
                        4,
                    )
                    .unwrap()
                ),
                Duration::from_millis(10),
                None
            )
            .is_err()
        );
    }
    #[cfg(all(feature = "dfs", feature = "rdma"))]
    mod dfs_rdma_tests {
        use super::*;

        #[test]
        fn dfs_rdma_unsupported_reply_contract_accepts_only_canonical_false() {
            let canonical = DfsNegotiateRdmaReply {
                session_id: 0,
                server_info: Vec::new(),
                capacity: 0,
                rdma_supported: false,
                handshake_version: RDMA_HANDSHAKE_VERSION,
            };
            assert!(dfs_rdma_canonical_unsupported(&canonical));

            let malformed_session = DfsNegotiateRdmaReply {
                session_id: 9,
                ..canonical.clone()
            };
            assert!(!dfs_rdma_canonical_unsupported(&malformed_session));

            let malformed_info = DfsNegotiateRdmaReply {
                server_info: vec![1],
                ..canonical.clone()
            };
            assert!(!dfs_rdma_canonical_unsupported(&malformed_info));

            let malformed_capacity = DfsNegotiateRdmaReply {
                capacity: 1,
                ..canonical.clone()
            };
            assert!(!dfs_rdma_canonical_unsupported(&malformed_capacity));

            let malformed_version = DfsNegotiateRdmaReply {
                handshake_version: RDMA_HANDSHAKE_VERSION + 1,
                ..canonical.clone()
            };
            assert!(!dfs_rdma_canonical_unsupported(&malformed_version));

            let supported = DfsNegotiateRdmaReply {
                rdma_supported: true,
                ..canonical
            };
            assert!(!dfs_rdma_canonical_unsupported(&supported));
        }

        #[test]
        fn read_completion_binds_attempt_copy_range_and_payload_checksum() {
            let request = DfsReadRangesRequest {
                include_blake3_outboard: false,
                read_id: "read".into(),
                attempt_id: "attempt".into(),
                file_version_id: "version".into(),
                layout_root_id: "layout".into(),
                operations: vec![DfsChunkReadOp {
                    operation_index: 0,
                    chunk_id: "chunk".into(),
                    chunk_offset: 7,
                    length: 3,
                    destination_offset: 0,
                    source_copy_id: "copy".into(),
                    grant: None,
                }],
            };
            let reply = afs_protocol::node_data::DfsReadRangesRdmaReply {
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                transferred_bytes: 3,
                completions: vec![afs_protocol::node_data::DfsReadRangesCompletion {
                    crc32c_table: Default::default(),
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: 0,
                    source_copy_id: "copy".into(),
                    transferred_bytes: 3,
                    range_checksum: blake3::hash(b"abc").as_bytes().to_vec(),
                    range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                        as i32,
                }],
            };
            validate_dfs_rdma_read_reply(&request, &reply, b"abc").unwrap();
            assert_eq!(
                validate_dfs_rdma_read_reply(&request, &reply, b"abd")
                    .unwrap_err()
                    .code(),
                afs_error::NODE_TRANSFER_CORRUPT_DATA
            );
            let mut wrong = reply.clone();
            wrong.attempt_id = "another".into();
            assert_eq!(
                validate_dfs_rdma_read_reply(&request, &wrong, b"abc")
                    .unwrap_err()
                    .code(),
                afs_error::CLIENT_PROTOCOL_VIOLATION
            );
            let mut wrong = reply.clone();
            wrong.completions[0].source_copy_id = "another".into();
            assert_eq!(
                validate_dfs_rdma_read_reply(&request, &wrong, b"abc")
                    .unwrap_err()
                    .code(),
                afs_error::CLIENT_PROTOCOL_VIOLATION
            );
            let mut wrong = reply;
            wrong.completions[0].transferred_bytes = 2;
            assert_eq!(
                validate_dfs_rdma_read_reply(&request, &wrong, b"abc")
                    .unwrap_err()
                    .code(),
                afs_error::CLIENT_PROTOCOL_VIOLATION
            );
        }

        #[test]
        fn read_windows_split_at_registered_capacity_and_preserve_destinations() {
            use crate::dfs::*;
            use crate::node::dfs_read::{ChunkReadOp, PeerReadBatch, ResolvedReadOp};
            let size = 3 * 1024 * 1024;
            let batch = PeerReadBatch {
                file_version_id: FileVersionId::new("version"),
                layout_root_id: LayoutRootId::new("layout"),
                operations: (0..2)
                    .map(|i| ResolvedReadOp {
                        op: ChunkReadOp {
                            chunk_id: ChunkId::new(format!("chunk-{i}")),
                            chunk_offset: 0,
                            length: size as u64,
                            output_offset: i * size,
                        },
                        source: SourceCandidate {
                            copy_id: CopyId::new(format!("copy-{i}")),
                            chunk_id: ChunkId::new(format!("chunk-{i}")),
                            role: CopyRole::DurableReplica,
                            state: CopyState::Ready,
                            location: CopyLocation::Node {
                                node_id: "source".into(),
                                node_epoch: 1,
                                device_id: "disk".into(),
                                device_epoch: 1,
                                catalog_revision: 1,
                            },
                            data_endpoint: Some("http://127.0.0.1:1".into()),
                            load_hint: 0,
                            read_grant: DfsReadGrant {
                                namespace_id: NamespaceId::new("ns"),
                                file_version_id: FileVersionId::new("version"),
                                layout_root_id: LayoutRootId::new("layout"),
                                caller_node_id: "reader".into(),
                                caller_node_epoch: 1,
                                expires_at_unix_ms: u64::MAX,
                                fence: 1,
                                token: "fixture".into(),
                            },
                        },
                    })
                    .collect(),
            };
            let windows = pack_dfs_rdma_read_windows(&batch, 42).unwrap();
            assert_eq!(windows.len(), 2);
            assert_eq!(windows[0].2, size);
            assert_eq!(windows[1].2, size);
            assert_eq!(windows[0].1, vec![(0, 0..size)]);
            assert_eq!(windows[1].1, vec![(size, 0..size)]);
            assert_eq!(windows[1].0.operations[0].operation_index, 0);
            assert_eq!(windows[1].0.operations[0].destination_offset, 0);
            assert_ne!(windows[0].0.attempt_id, windows[1].0.attempt_id);
        }
    }
}
