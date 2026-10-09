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
                .chain(tokio_stream::iter(
                    (0..staged.bytes().len())
                        .step_by(super::data::DFS_REPLICA_FRAME_BYTES)
                        .map(move |offset| {
                            let end = (offset + super::data::DFS_REPLICA_FRAME_BYTES)
                                .min(staged.bytes().len());
                            afs_protocol::node_data::DfsPutReplicaFrame {
                                body: Some(
                                    afs_protocol::node_data::dfs_put_replica_frame::Body::Data(
                                        staged.bytes()[offset..end].to_vec(),
                                    ),
                                ),
                            }
                        }),
                ));
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
        content_digest_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32,
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
fn domain_replica_ack(
    ack: afs_protocol::node_data::DfsReplicaAck,
) -> afs_error::Result<crate::dfs::ReplicaAck> {
    if ack.verified_digest_algorithm != afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32 {
        return Err(dfs_protocol_error(
            "replica acknowledgement digest algorithm is unsupported",
        ));
    }
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
        verified_digest: crate::dfs::ContentDigest {
            algorithm: crate::dfs::DigestAlgorithm::Blake3,
            bytes,
        },
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
    config: afs_transport::GrpcConfig,
    security: afs_transport::SecurityManager,
    capacity: usize,
    state: tokio::sync::Mutex<PeerPoolState>,
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
        })
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
        if request_timeout && let Some(entry) = state.channels.get_mut(&key) {
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
        let endpoint = if owner_files {
            endpoint.max_frame_size(256 * 1024)
        } else {
            endpoint
        };
        let endpoint = self.security.configure_client(endpoint).map_err(|error| {
            afs_error::Error::coded(afs_error::CLIENT_ARGUMENT_INVALID, error.to_string())
        })?;
        let channel = endpoint.connect_lazy();
        if request_timeout {
            self.publish_channel(&mut state, key, channel.clone());
        } else {
            self.publish_epoch(&mut state, node_id, node_epoch);
        }
        Ok(channel)
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
impl crate::node::dfs_read::ChunkTransfer for GrpcChunkTransfer {
    fn read_ranges(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        batch.validate(out.len())?;
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
                let response = client
                    .read_ranges(rpc_request)
                    .await
                    .map_err(afs_transport::grpc::error_status::status_to_error)?;
                let mut stream = response.into_inner();
                let mut decoder = ReadFrameDecoder::new(&request);
                while let Some(frame) = stream
                    .message()
                    .await
                    .map_err(afs_transport::grpc::error_status::status_to_error)?
                {
                    decoder.accept(frame, out)?;
                }
                decoder.finish()
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
struct ReadFrameDecoder<'a> {
    request: &'a DfsReadRangesRequest,
    completed: Vec<bool>,
    active: Option<ActivePeerRead>,
}

#[cfg(feature = "dfs")]
struct ActivePeerRead {
    index: usize,
    start: usize,
    end: usize,
    cursor: usize,
    checksum: blake3::Hasher,
}

#[cfg(feature = "dfs")]
impl<'a> ReadFrameDecoder<'a> {
    fn new(request: &'a DfsReadRangesRequest) -> Self {
        Self {
            request,
            completed: vec![false; request.operations.len()],
            active: None,
        }
    }

    fn accept(
        &mut self,
        frame: afs_protocol::node_data::DfsReadRangesFrame,
        out: &mut [u8],
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
                let start = usize::try_from(op.destination_offset)
                    .map_err(|_| dfs_protocol_error("read destination overflow"))?;
                let length = usize::try_from(op.length)
                    .map_err(|_| dfs_protocol_error("read length overflow"))?;
                let end = start
                    .checked_add(length)
                    .filter(|end| *end <= out.len())
                    .ok_or_else(|| dfs_protocol_error("read exceeds destination"))?;
                self.active = Some(ActivePeerRead {
                    index,
                    start,
                    end,
                    cursor: start,
                    checksum: blake3::Hasher::new(),
                });
            }
            Some(dfs_read_ranges_frame::Body::Data(data)) => {
                let active = self
                    .active
                    .as_mut()
                    .ok_or_else(|| dfs_protocol_error("peer data without header"))?;
                let next = active
                    .cursor
                    .checked_add(data.len())
                    .filter(|end| *end <= active.end)
                    .ok_or_else(|| dfs_protocol_error("peer sent too much data"))?;
                out[active.cursor..next].copy_from_slice(&data);
                active.checksum.update(&data);
                active.cursor = next;
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
                if completion.range_checksum.as_slice() != active.checksum.finalize().as_bytes() {
                    return Err(dfs_corrupt_error("peer read completion checksum mismatch"));
                }
                self.completed[active.index] = true;
            }
            None => return Err(dfs_protocol_error("empty peer frame")),
        }
        Ok(())
    }

    fn finish(self) -> afs_error::Result<()> {
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
    fn read_ranges(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        out: &mut [u8],
    ) -> afs_error::Result<()> {
        #[cfg(not(feature = "rdma"))]
        {
            let _ = (batch, out, &self.pool, &self.runtime, &self.next_read);
            Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                "DFS RDMA is not compiled",
            ))
        }
        #[cfg(feature = "rdma")]
        {
            batch.validate(out.len())?;
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
                    GrpcChunkTransfer::new(fallback.peers.clone(), timeout)?.read_ranges(batch, out)
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
            || completion.range_checksum_algorithm
                != afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32
        {
            return Err(dfs_protocol_error(
                "RDMA read completion identity/length differs",
            ));
        }
        if completion.range_checksum.as_slice() != blake3::hash(range).as_bytes() {
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
    let channel = connect_channel(&options.endpoint, options.timeout).await?;
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
/// Node wiring should use this when peer traffic requires mTLS: construct the
/// `Channel` through the common SecurityManager/TLS config, then pass it here.
/// The legacy `connect_owner_files_client` remains for plaintext diagnostics and
/// tests until Node config owns secure endpoint construction.
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
    let channel = connect_channel(&options.endpoint, options.timeout).await?;
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

async fn connect_channel(endpoint: &str, timeout: Duration) -> PeerResult<Channel> {
    Ok(Endpoint::from_shared(endpoint.to_string())?
        .connect_timeout(timeout)
        .timeout(timeout)
        .connect()
        .await?)
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
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: op.operation_index,
                    chunk_id: op.chunk_id.clone(),
                    chunk_offset: op.chunk_offset,
                    length: op.length,
                    source_copy_id: op.source_copy_id.clone(),
                })),
            },
            DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Data(vec![b'a' + index as u8])),
            },
            DfsReadRangesFrame {
                body: Some(dfs_read_ranges_frame::Body::Completion(
                    DfsReadRangesCompletion {
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
