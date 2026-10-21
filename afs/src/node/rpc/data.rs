//! Node → Node 文件数据操作 Handler。
//!
//! 这个文件统一承载 Node 间入站数据服务：前半实现 node_data.proto 的诊断
//! handler，后半实现 OwnerFs 文件命令；二者不共用无授权的 diagnostics Storage 入口。
//! 对诊断请求而言，它是“共同服务端入口”：
//! 无论远端客户端选择 gRPC inline 还是 RDMA，最终都会进入这里，完成相同的
//! Storage read/write 诊断语义。区别只在文件内容怎么搬：
//! - gRPC inline：内容直接放在 DataReadReply/DataWriteRequest 的 proto bytes 里；
//! - RDMA one-sided：proto 只放命令、session_id、offset/len，不放文件内容。
//!
//! 单边方向要特别记住：
//! - 远端写文件：客户端先把待写 bytes 放入自己的 MR，服务端执行 RDMA READ 拉取，
//!   然后写入本地 Storage；
//! - 远端读文件：服务端先从 Storage 读出 bytes 放入自己的 MR，执行 RDMA WRITE 推到
//!   客户端 MR，客户端再从本地 MR 拷贝给调用者。
//!
//! CQ completion 只证明 DMA 完成，不证明文件落盘；文件成功标准仍由 Storage/业务层决定。

use afs_transport::grpc::error_status::{coded_status, error_to_status};
#[cfg(feature = "dfs")]
use std::pin::Pin;
use std::sync::Arc;
#[cfg(any(feature = "rdma", feature = "ownerfs"))]
use std::sync::atomic::Ordering;
use std::{collections::HashMap, net::SocketAddr};
use tonic::metadata::MetadataMap;

#[cfg(feature = "ownerfs")]
use afs_protocol::node_data::DataPlane;
use afs_protocol::node_data::{
    DataReadReply, DataReadRequest, DataTransfer, DataWriteReply, DataWriteRequest,
    node_data_server::{NodeData, NodeDataServer},
};
#[cfg(feature = "dfs")]
use afs_protocol::node_data::{
    DfsCloseRdmaReply, DfsCloseRdmaRequest, DfsConfirmReplicaRequest, DfsNegotiateRdmaReply,
    DfsNegotiateRdmaRequest, DfsPutReplicaFrame, DfsPutReplicaRdmaRequest, DfsPutReplicaReply,
    DfsReadRangeReply, DfsReadRangesCompletion, DfsReadRangesFrame, DfsReadRangesHeader,
    DfsReadRangesRdmaReply, DfsReadRangesRdmaRequest, DfsReadRangesRequest, DfsReadSessionEnd,
    DfsReadSessionFrame, DfsReadSessionRequest, DfsReplicaAck,
    dfs_chunks_server::{DfsChunks, DfsChunksServer},
    dfs_read_ranges_frame, dfs_read_session_frame,
    dfs_read_sessions_server::{DfsReadSessions, DfsReadSessionsServer},
};
use afs_tracing::Instrument;
#[cfg(feature = "dfs")]
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

#[cfg(feature = "dfs")]
use crate::node::{
    chunk::LocalChunkStore,
    dfs_read::{MAX_DFS_READ_BYTES, MAX_DFS_READ_OPS},
};
use crate::node::{
    rpc::control::RdmaSessionRegistry,
    storage::{MAX_TRANSFER_BYTES, Storage, StorageError},
};

#[cfg(feature = "dfs")]
use afs_protocol::node_data::{
    DfsOwnerHandle, DfsOwnerReadReply, DfsOwnerReadRequest, DfsOwnerResizeReply,
    DfsOwnerResizeRequest, DfsOwnerSyncReply, DfsOwnerSyncRequest, DfsOwnerWriteReply,
    DfsOwnerWriteRequest,
    dfs_owner_files_server::{DfsOwnerFiles, DfsOwnerFilesServer},
};

/// Framework boundary only: handlers own lease/handle validation, sequencing,
/// deduplication and result-unknown recovery. A transport ACK is never fsync.
#[cfg(feature = "dfs")]
pub trait DfsOwnerFilesHandler: Send + Sync + 'static {
    fn read(
        &self,
        _peer: &str,
        _request: DfsOwnerReadRequest,
    ) -> afs_error::Result<DfsOwnerReadReply> {
        Err(dfs_owner_unimplemented())
    }
    fn write(
        &self,
        _peer: &str,
        _request: DfsOwnerWriteRequest,
    ) -> afs_error::Result<DfsOwnerWriteReply> {
        Err(dfs_owner_unimplemented())
    }
    fn resize(
        &self,
        _peer: &str,
        _request: DfsOwnerResizeRequest,
    ) -> afs_error::Result<DfsOwnerResizeReply> {
        Err(dfs_owner_unimplemented())
    }
    fn sync(
        &self,
        _peer: &str,
        _request: DfsOwnerSyncRequest,
    ) -> afs_error::Result<DfsOwnerSyncReply> {
        Err(dfs_owner_unimplemented())
    }
}

#[cfg(feature = "dfs")]
fn dfs_owner_unimplemented() -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::NODE_VFS_UNIMPLEMENTED,
        "DFS remote inode owner execution is not wired",
    )
}

#[cfg(feature = "dfs")]
#[derive(Clone, Default)]
pub struct DfsOwnerFilesService {
    handler: Option<Arc<dyn DfsOwnerFilesHandler>>,
    authenticator: Option<Arc<dyn PeerAuthenticator>>,
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

    async fn dispatch<Req: InternalPeerRequest + Send + 'static, Reply: Send + 'static>(
        &self,
        request: Request<Req>,
        handle: &DfsOwnerHandle,
        operation_id: Option<&str>,
        call: impl FnOnce(Arc<dyn DfsOwnerFilesHandler>, String, Req) -> afs_error::Result<Reply>
        + Send
        + 'static,
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
        tokio::task::spawn_blocking(move || call(handler, peer, request))
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
            handler.read(&peer, request)
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
            |handler, peer, request| handler.write(&peer, request),
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
            |handler, peer, request| handler.resize(&peer, request),
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
            |handler, peer, request| handler.sync(&peer, request),
        )
        .await
    }
}

/// node_data 的服务端实现。
///
/// `storage` 是真实文件操作入口；`sessions` 只在 RDMA 模式下把 session_id 转为
/// 服务端 endpoint。这里不区分 OwnerFs/DFS，只实现第一版诊断用的 8-byte/文件数据路径。
#[derive(Clone)]
pub struct NodeDataService {
    storage: Arc<Storage>,
    sessions: RdmaSessionRegistry,
}

impl NodeDataService {
    #[must_use]
    pub fn new(storage: Arc<Storage>, sessions: RdmaSessionRegistry) -> Self {
        Self { storage, sessions }
    }
}

pub fn make_data_server(
    storage: Arc<Storage>,
    sessions: RdmaSessionRegistry,
) -> NodeDataServer<NodeDataService> {
    NodeDataServer::new(NodeDataService::new(storage, sessions))
}

/// Maximum staged immutable chunk; independent from individual gRPC frames.
#[cfg(feature = "dfs")]
pub(crate) const MAX_DFS_REPLICA_BYTES: usize = crate::node::chunk::MAX_STAGED_CHUNK_BYTES;
#[cfg(feature = "dfs")]
pub(crate) const DFS_REPLICA_FRAME_BYTES: usize = 64 * 1024;

#[cfg(feature = "dfs")]
pub struct AuthorizedReplicaWrite {
    pub op: crate::node::replication::ReplicaPeerOp,
    pub expires_at_unix_ms: u64,
}

#[cfg(feature = "dfs")]
pub trait DfsReplicaAuthorizer: Send + Sync + 'static {
    fn authorize(
        &self,
        peer: &str,
        header: &afs_protocol::node_data::DfsPutReplicaHeader,
    ) -> afs_error::Result<AuthorizedReplicaWrite>;
}

#[cfg(feature = "dfs")]
pub struct DenyDfsReplicaAuthorizer;
#[cfg(feature = "dfs")]
impl DfsReplicaAuthorizer for DenyDfsReplicaAuthorizer {
    fn authorize(
        &self,
        _: &str,
        _: &afs_protocol::node_data::DfsPutReplicaHeader,
    ) -> afs_error::Result<AuthorizedReplicaWrite> {
        Err(replica_denied("DFS replica authority is not configured"))
    }
}

/// First delivery lane validates each receiving hop with Meta. A reusable,
/// authenticated authority snapshot may later remove that RPC from the hot path.
#[cfg(feature = "dfs")]
pub struct MetaReplicaAuthorizer {
    pub meta: Arc<super::meta::GrpcDfsMeta>,
    pub node_id: String,
    pub node_epoch: u64,
}

#[cfg(feature = "dfs")]
impl DfsReplicaAuthorizer for MetaReplicaAuthorizer {
    fn authorize(
        &self,
        peer: &str,
        header: &afs_protocol::node_data::DfsPutReplicaHeader,
    ) -> afs_error::Result<AuthorizedReplicaWrite> {
        use crate::dfs::*;
        let digest = replica_header_digest(header)?;
        let targets = header
            .ordered_targets
            .iter()
            .map(|target| {
                let device = target
                    .device
                    .as_ref()
                    .ok_or_else(|| replica_denied("replica target device is missing"))?;
                Ok(ReplicaTarget {
                    node_id: target.node_id.clone(),
                    node_epoch: target.node_epoch,
                    data_endpoint: target.data_endpoint.clone(),
                    device: StorageDeviceDescriptor {
                        device_id: device.device_id.clone(),
                        device_epoch: device.device_epoch,
                        catalog_revision: device.catalog_revision,
                        failure_domain: device.failure_domain.clone(),
                    },
                })
            })
            .collect::<afs_error::Result<Vec<_>>>()?;
        let index = header.target_index as usize;
        let receiver = targets
            .get(index)
            .ok_or_else(|| replica_denied("replica receiver index is invalid"))?;
        let sender = if index == 0 {
            header.initiator_node_id.as_str()
        } else {
            targets[index - 1].node_id.as_str()
        };
        if peer != sender
            || receiver.node_id != self.node_id
            || receiver.node_epoch != self.node_epoch
        {
            return Err(replica_denied("replica sender or receiver session differs"));
        }
        let request = ValidateReplicaWriteRequest {
            requester_node_id: self.node_id.clone(),
            requester_node_epoch: self.node_epoch,
            initiator_node_id: header.initiator_node_id.clone(),
            initiator_node_epoch: header.initiator_node_epoch,
            operation_id: OperationId::new(header.operation_id.clone()),
            chunk_id: ChunkId::new(header.chunk_id.clone()),
            chunk_length: header.chunk_length,
            content_digest: digest,
            placement_revision: header.placement_revision,
            placement_epoch: header.placement_epoch,
            replica_group_id: ReplicaGroupId::new(header.replica_group_id.clone()),
            target_index: header.target_index,
            ordered_targets: targets,
            repair_claim: header
                .repair_claim
                .clone()
                .map(|claim| super::meta::domain_replication_claim(*claim))
                .transpose()?,
        };
        let grant = self.meta.validate_replica_write(request.clone())?;
        if grant.requester_node_id != request.requester_node_id
            || grant.requester_node_epoch != request.requester_node_epoch
            || grant.initiator_node_id != request.initiator_node_id
            || grant.initiator_node_epoch != request.initiator_node_epoch
            || grant.operation_id != request.operation_id
            || grant.chunk_id != request.chunk_id
            || grant.chunk_length != request.chunk_length
            || grant.content_digest != request.content_digest
            || grant.placement_revision != request.placement_revision
            || grant.placement_epoch != request.placement_epoch
            || grant.replica_group_id != request.replica_group_id
            || grant.target_index != request.target_index
            || grant.replica_group.targets != request.ordered_targets
        {
            return Err(replica_denied(
                "Meta replica grant does not bind the precise request",
            ));
        }
        ensure_replica_grant_live(grant.expires_at_unix_ms)?;
        let mut op = crate::node::replication::ReplicaPeerOp::from_grant(&grant)?;
        op.repair_claim = request.repair_claim;
        op.validate_shape()?;
        op.validate_sender(peer)?;
        Ok(AuthorizedReplicaWrite {
            op,
            expires_at_unix_ms: grant.expires_at_unix_ms,
        })
    }
}

#[cfg(feature = "dfs")]
fn replica_denied(message: &str) -> afs_error::Error {
    afs_error::Error::coded(afs_error::IO_PERMISSION_DENIED, message)
}

#[cfg(feature = "dfs")]
fn ensure_replica_grant_live(expires: u64) -> afs_error::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| replica_denied("system clock is before epoch"))?
        .as_millis();
    if expires as u128 <= now {
        return Err(replica_denied("replica grant expired before persistence"));
    }
    Ok(())
}

#[cfg(feature = "dfs")]
fn replica_header_digest(
    header: &afs_protocol::node_data::DfsPutReplicaHeader,
) -> afs_error::Result<crate::dfs::ContentDigest> {
    if header.operation_id.is_empty()
        || header.chunk_id.is_empty()
        || header.chunk_length == 0
        || header.chunk_length > MAX_DFS_REPLICA_BYTES as u64
        || header.initiator_node_id.is_empty()
        || header.initiator_node_epoch == 0
        || header.placement_revision == 0
        || header.placement_epoch == 0
        || header.content_digest_algorithm
            != afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32
    {
        return Err(replica_denied("replica header identity or size is invalid"));
    }
    let bytes: [u8; 32] = header
        .content_digest
        .as_slice()
        .try_into()
        .map_err(|_| replica_denied("replica digest length is invalid"))?;
    Ok(crate::dfs::ContentDigest {
        algorithm: crate::dfs::DigestAlgorithm::Blake3,
        bytes,
    })
}

#[cfg(feature = "dfs")]
fn wire_replica_ack(ack: crate::dfs::ReplicaAck) -> DfsReplicaAck {
    DfsReplicaAck {
        operation_id: ack.operation_id.0,
        chunk_id: ack.chunk_id.0,
        placement_revision: ack.placement_revision,
        placement_epoch: ack.placement_epoch,
        node_id: ack.node_id,
        node_epoch: ack.node_epoch,
        device_id: ack.device_id,
        device_epoch: ack.device_epoch,
        catalog_revision: ack.catalog_revision,
        persisted_bytes: ack.persisted_bytes,
        verified_digest: ack.verified_digest.bytes.to_vec(),
        verified_digest_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32,
    }
}

// Draft data.rs additions, keeping transport resources inside the existing RPC module.
#[cfg(feature = "dfs")]
#[derive(Clone)]
pub struct DfsPayloadMetrics {
    bytes: afs_metrics::IntCounterVec,
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
            for transport in ["grpc", "rdma"] {
                for (direction, operation) in [("recv", "replica"), ("send", "read")] {
                    bytes.with_label_values(&[transport, direction, operation]);
                }
            }
            Ok(Self { bytes })
        })
    }
    fn record(
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
    pub rdma_sessions: super::control::RdmaSessionRegistry,
    pub payload_metrics: Option<DfsPayloadMetrics>,
}

// Existing constructors remain wrappers passing a disabled registry and no
// metrics. Only Node assembly uses the new focused constructor with shared resources.

#[cfg(feature = "dfs")]
#[derive(Clone)]
pub struct DfsChunksService {
    local_chunks: Option<Arc<LocalChunkStore>>,
    authenticator: Arc<dyn PeerAuthenticator>,
    authorizer: Arc<dyn DfsReadAuthorizer>,
    replica_authorizer: Arc<dyn DfsReplicaAuthorizer>,
    replica_data_plane: Option<Arc<dyn crate::node::replication::ReplicaDataPlane>>,
    replica_permits: Arc<tokio::sync::Semaphore>,
    read_permits: Arc<tokio::sync::Semaphore>,
    read_sessions: Arc<tokio::sync::Semaphore>,
    replica_timeout: std::time::Duration,
    rdma_sessions: super::control::RdmaSessionRegistry,
    payload_metrics: Option<DfsPayloadMetrics>,
}

/// Verifies grant authenticity, caller epoch, namespace/version/layout membership
/// and the selected local copy/device epoch for every requested range. Structural
/// validation alone never authorizes access. The implementation owns any cached
/// authority snapshot and its expiry; it must not turn each range into a Meta RPC.
#[cfg(feature = "dfs")]
pub trait DfsReadAuthorizer: Send + Sync + 'static {
    fn authorize(
        &self,
        authenticated_peer: &str,
        request: &DfsReadRangesRequest,
    ) -> afs_error::Result<()>;
}

#[cfg(feature = "dfs")]
pub trait DfsReadGrantValidator: Send + Sync + 'static {
    fn validate(
        &self,
        request: crate::dfs::ValidateDfsReadGrants,
    ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>>;
}
#[cfg(feature = "dfs")]
impl DfsReadGrantValidator for super::meta::GrpcDfsMeta {
    fn validate(
        &self,
        request: crate::dfs::ValidateDfsReadGrants,
    ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
        self.validate_read_grants(request)
    }
}

/// Bounded capability-validation cache, never a data cache or an authority
/// fallback. Cache hits keep their original expiry and cannot renew a lease.
#[cfg(feature = "dfs")]
pub struct CachedDfsReadAuthorizer {
    validator: Arc<dyn DfsReadGrantValidator>,
    node_id: String,
    node_epoch: u64,
    cache: std::sync::Mutex<HashMap<String, CachedReadAuthority>>,
}
#[cfg(feature = "dfs")]
struct CachedReadAuthority {
    authorization: crate::dfs::DfsAuthorizedRead,
    deadline: std::time::Instant,
}
#[cfg(feature = "dfs")]
impl CachedDfsReadAuthorizer {
    pub fn new(
        validator: Arc<dyn DfsReadGrantValidator>,
        node_id: String,
        node_epoch: u64,
    ) -> Self {
        Self {
            validator,
            node_id,
            node_epoch,
            cache: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[cfg(feature = "dfs")]
fn read_authority_key(validation: &crate::dfs::DfsReadValidation) -> afs_error::Result<String> {
    let grant = &validation.grant;
    if grant.token.len() != 76
        || [
            &grant.namespace_id.0,
            &grant.file_version_id.0,
            &grant.layout_root_id.0,
            &grant.caller_node_id,
            &validation.chunk_id.0,
            &validation.copy_id.0,
        ]
        .iter()
        .any(|value| value.len() > 512)
    {
        return Err(replica_denied(
            "read authority identity exceeds its bounded budget",
        ));
    }
    serde_json::to_string(&(grant, &validation.chunk_id, &validation.copy_id))
        .map_err(|_| replica_denied("read authority key serialization failed"))
}

#[cfg(feature = "dfs")]
fn authorized_read_range(entry: &crate::dfs::DfsAuthorizedRead, offset: u64, length: u64) -> bool {
    length != 0
        && offset.checked_add(length).is_some_and(|end| {
            entry.allowed_ranges.iter().any(|(start, count)| {
                offset >= *start && start.checked_add(*count).is_some_and(|limit| end <= limit)
            })
        })
}

#[cfg(feature = "dfs")]
impl DfsReadAuthorizer for CachedDfsReadAuthorizer {
    fn authorize(&self, peer: &str, request: &DfsReadRangesRequest) -> afs_error::Result<()> {
        validate_dfs_read_request(request)
            .map_err(afs_transport::grpc::error_status::status_to_error)?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| replica_denied("read authority clock is before epoch"))?
            .as_millis() as u64;
        let mut checks = Vec::with_capacity(request.operations.len());
        for op in &request.operations {
            let grant = op
                .grant
                .as_ref()
                .ok_or_else(|| replica_denied("read operation grant is absent"))?;
            if grant.caller_node_id != peer {
                return Err(replica_denied("read capability belongs to another peer"));
            }
            let check = crate::dfs::DfsReadValidation {
                grant: crate::dfs::DfsReadGrant {
                    namespace_id: crate::dfs::NamespaceId::new(grant.namespace_id.clone()),
                    file_version_id: crate::dfs::FileVersionId::new(grant.file_version_id.clone()),
                    layout_root_id: crate::dfs::LayoutRootId::new(grant.layout_root_id.clone()),
                    caller_node_id: grant.caller_node_id.clone(),
                    caller_node_epoch: grant.caller_node_epoch,
                    expires_at_unix_ms: grant.expires_at_unix_ms,
                    fence: grant.fence,
                    token: grant.token.clone(),
                },
                chunk_id: crate::dfs::ChunkId::new(op.chunk_id.clone()),
                copy_id: crate::dfs::CopyId::new(op.source_copy_id.clone()),
                chunk_offset: op.chunk_offset,
                length: op.length,
            };
            checks.push((read_authority_key(&check)?, check));
        }
        let missing = {
            let mut cache = self
                .cache
                .lock()
                .map_err(|_| replica_denied("read authority cache is poisoned"))?;
            cache.retain(|_, entry| {
                entry.deadline > std::time::Instant::now()
                    && entry.authorization.expires_at_unix_ms > now
            });
            checks
                .iter()
                .filter(|(key, check)| {
                    cache.get(key).is_none_or(|entry| {
                        !authorized_read_range(
                            &entry.authorization,
                            check.chunk_offset,
                            check.length,
                        )
                    })
                })
                .map(|(_, check)| check.clone())
                .collect::<Vec<_>>()
        };
        if missing.is_empty() {
            return Ok(());
        }
        let authorized = self.validator.validate(crate::dfs::ValidateDfsReadGrants {
            receiver_node_id: self.node_id.clone(),
            receiver_node_epoch: self.node_epoch,
            peer_node_id: peer.into(),
            validations: missing.clone(),
        })?;
        if authorized.len() != missing.len() {
            return Err(replica_denied("read authority reply is incomplete"));
        }
        let completed_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| replica_denied("read authority clock is before epoch"))?
            .as_millis() as u64;
        for (entry, check) in authorized.iter().zip(&missing) {
            if entry.validation != *check
                || entry.allowed_ranges.is_empty()
                || entry.allowed_ranges.len() > 128
                || entry.expires_at_unix_ms <= completed_now
                || entry.expires_at_unix_ms > check.grant.expires_at_unix_ms
                || !authorized_read_range(entry, check.chunk_offset, check.length)
            {
                return Err(replica_denied(
                    "read authority reply is not the precise requested capability",
                ));
            }
        }
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| replica_denied("read authority cache is poisoned"))?;
        for entry in authorized {
            let key = read_authority_key(&entry.validation)?;
            if cache.len() >= 1024
                && !cache.contains_key(&key)
                && let Some(old) = cache.keys().next().cloned()
            {
                cache.remove(&old);
            }
            // Meta and this receiver have different wall clocks. Bound local
            // retention monotonically instead of rejecting a valid Meta reply
            // whose five-second expiry is slightly ahead of our clock.
            let remaining = entry
                .expires_at_unix_ms
                .saturating_sub(completed_now)
                .min(5_000);
            cache.insert(
                key,
                CachedReadAuthority {
                    authorization: entry,
                    deadline: std::time::Instant::now()
                        + std::time::Duration::from_millis(remaining),
                },
            );
        }
        Ok(())
    }
}

#[cfg(feature = "dfs")]
pub struct DenyDfsReadAuthorizer;

#[cfg(feature = "dfs")]
impl DfsReadAuthorizer for DenyDfsReadAuthorizer {
    fn authorize(&self, _peer: &str, _request: &DfsReadRangesRequest) -> afs_error::Result<()> {
        Err(afs_error::Error::coded(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "DFS read grant authority is not wired; peer reads are denied",
        ))
    }
}

#[cfg(feature = "dfs")]
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

#[cfg(feature = "dfs")]
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
            rdma_sessions: super::control::RdmaSessionRegistry::new(None),
            payload_metrics: None,
        },
    )
}

/// Bounds persistent session codecs inside the existing Peer payload pool.
/// The lease covers the encoded HTTP body, including idle/trailer ownership;
/// attaching it only to the producer would release it before tonic's buffers.
#[cfg(feature = "dfs")]
#[derive(Clone)]
pub struct DfsReadSessionService {
    inner: DfsReadSessionsServer<DfsChunksService>,
    local: Option<Arc<LocalChunkStore>>,
}

#[cfg(feature = "dfs")]
impl tonic::server::NamedService for DfsReadSessionService {
    const NAME: &'static str =
        <DfsReadSessionsServer<DfsChunksService> as tonic::server::NamedService>::NAME;
}

#[cfg(feature = "dfs")]
struct SessionCodecBody {
    // Drop codec owners before releasing their charge.
    inner: Pin<Box<tonic::body::Body>>,
    _codec: Arc<crate::node::chunk::PeerReadReservation>,
}

#[cfg(feature = "dfs")]
impl http_body::Body for SessionCodecBody {
    type Data = prost::bytes::Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        http_body::Body::poll_frame(self.inner.as_mut(), cx)
    }

    fn is_end_stream(&self) -> bool {
        http_body::Body::is_end_stream(self.inner.as_ref().get_ref())
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::Body::size_hint(self.inner.as_ref().get_ref())
    }
}

#[cfg(feature = "dfs")]
impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for DfsReadSessionService
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        <DfsReadSessionsServer<DfsChunksService> as tonic::codegen::Service<
            tonic::codegen::http::Request<B>,
        >>::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, mut request: tonic::codegen::http::Request<B>) -> Self::Future {
        let codec = self
            .local
            .as_ref()
            .ok_or_else(|| Status::unavailable("DFS local ChunkStore is not available"))
            .and_then(|local| {
                local
                    .reserve_peer_session_codec(512 * 1024)
                    .map_err(error_to_status)
            });
        let codec = match codec {
            Ok(codec) => Arc::new(codec),
            Err(status) => return Box::pin(async move { Ok(status.into_http()) }),
        };
        // The incoming decoder lives in the server task after the response
        // body may be cancelled. Both real codec owners share one charge.
        request.extensions_mut().insert(codec.clone());
        let response = tonic::codegen::Service::call(&mut self.inner, request);
        Box::pin(async move {
            Ok(response.await?.map(|inner| {
                tonic::body::Body::new(SessionCodecBody {
                    inner: Box::pin(inner),
                    _codec: codec,
                })
            }))
        })
    }
}

#[cfg(feature = "dfs")]
pub fn make_dfs_chunk_services_with_transport(
    local_chunks: Option<Arc<LocalChunkStore>>,
    authenticator: Arc<dyn PeerAuthenticator>,
    authorizer: Arc<dyn DfsReadAuthorizer>,
    replica_authorizer: Arc<dyn DfsReplicaAuthorizer>,
    replica_data_plane: Option<Arc<dyn crate::node::replication::ReplicaDataPlane>>,
    replica_timeout: std::time::Duration,
    resources: DfsChunkTransportResources,
) -> (DfsChunksServer<DfsChunksService>, DfsReadSessionService) {
    let service = DfsChunksService {
        local_chunks,
        authenticator,
        authorizer,
        replica_authorizer,
        replica_data_plane,
        replica_permits: Arc::new(tokio::sync::Semaphore::new(8)),
        read_permits: Arc::new(tokio::sync::Semaphore::new(8)),
        read_sessions: Arc::new(tokio::sync::Semaphore::new(8)),
        replica_timeout,
        rdma_sessions: resources.rdma_sessions,
        payload_metrics: resources.payload_metrics,
    };
    (
        DfsChunksServer::new(service.clone()),
        DfsReadSessionService {
            local: service.local_chunks.clone(),
            inner: DfsReadSessionsServer::new(service)
                .max_decoding_message_size(8192)
                .max_encoding_message_size(128 * 1024 + 128),
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
    make_dfs_chunk_services_with_transport(
        local_chunks,
        authenticator,
        authorizer,
        replica_authorizer,
        replica_data_plane,
        replica_timeout,
        resources,
    )
    .0
}

#[cfg(feature = "dfs")]
#[tonic::async_trait]
impl DfsReadSessions for DfsChunksService {
    type ReadSessionStream =
        Pin<Box<dyn Stream<Item = Result<DfsReadSessionFrame, Status>> + Send + 'static>>;

    async fn read_session(
        &self,
        request: Request<tonic::Streaming<DfsReadSessionRequest>>,
    ) -> Result<Response<Self::ReadSessionStream>, Status> {
        let codec = request
            .extensions()
            .get::<Arc<crate::node::chunk::PeerReadReservation>>()
            .cloned()
            .ok_or_else(|| Status::internal("DFS session codec admission is missing"))?;
        let peer = authenticate_transport_peer(self.authenticator.as_ref(), &request)?;
        let local = self
            .local_chunks
            .as_ref()
            .ok_or_else(|| Status::unavailable("DFS local ChunkStore is not available"))?;
        local.ensure_peer_serve_open().map_err(error_to_status)?;
        let slot = self
            .read_sessions
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("DFS read session slots are full"))?;
        // Charged to the existing 64KiB Peer control budget, not a new pool.
        // Idle sessions do not own a payload lease or a read execution slot.
        let peer_capacity = peer.as_ref().map_or(0, String::capacity);
        if peer_capacity > 4096 {
            return Err(Status::resource_exhausted(
                "DFS session adapter identity exceeds bound",
            ));
        }
        let control = local
            .reserve_peer_control_until(4096 + peer_capacity, std::time::Instant::now())
            .map_err(error_to_status)?;
        let service = self.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            // Retain admission until the actual incoming decoder/producer
            // exits, including cancellation before response-body destruction.
            let _codec = codec;
            let _slot = slot;
            let _control = control;
            if let Err(error) =
                serve_read_session(service, peer, request.into_inner(), &sender).await
            {
                // A stalled consumer cannot retain idle admission forever.
                // If full, dropping the sender yields EOF without End, which
                // the client rejects even after a valid Completion prefix.
                let _ = sender.try_send(Err(error));
            }
        });
        // No business message has been sent while the client awaits this
        // response: an old server's UNIMPLEMENTED is safe to fall back from.
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(receiver),
        )))
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
        let transport_peer = authenticate_transport_peer(self.authenticator.as_ref(), &request)?;
        let permit = self
            .replica_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("DFS replica staging slots are full"))?;
        let local = self
            .local_chunks
            .clone()
            .ok_or_else(|| Status::failed_precondition("local chunk store is not configured"))?;
        let authorizer = self.replica_authorizer.clone();
        let plane = self.replica_data_plane.clone();
        let metrics = self.payload_metrics.clone();
        let mut stream = request.into_inner();
        tokio::time::timeout(self.replica_timeout, async move {
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
            let peer = match transport_peer {
                Some(identity) => identity,
                None => resolve_internal_peer(header.caller_node_id())?,
            };
            let auth_header = header.clone();
            // The permit follows blocking work: cancellation cannot release a
            // slot while its unabortable authority/persistence task is running.
            let (grant, permit) = tokio::task::spawn_blocking(move || {
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
            let staged = crate::node::chunk::StagedChunk::new(
                crate::dfs::OperationId::new(header.operation_id),
                bytes,
            );
            if staged.chunk.id.0 != header.chunk_id || staged.chunk.content_digest != digest {
                return Err(Status::invalid_argument(
                    "replica content differs from immutable identity",
                ));
            }
            let acks = tokio::task::spawn_blocking(move || {
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
            let permit = self
                .replica_permits
                .clone()
                .try_acquire_owned()
                .map_err(|_| Status::resource_exhausted("DFS replica slots are full"))?;
            let local = self
                .local_chunks
                .clone()
                .ok_or_else(|| Status::failed_precondition("local ChunkStore is absent"))?;
            let authorizer = self.replica_authorizer.clone();
            let auth_header = header.clone();
            let (grant, permit) = tokio::task::spawn_blocking(move || {
                authorizer
                    .authorize(&peer, &auth_header)
                    .map(|grant| (grant, permit))
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(error_to_status)?;
            let identity = super::control::PeerSessionIdentity::new(
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
            let acks = tokio::time::timeout(
                self.replica_timeout,
                tokio::task::spawn_blocking(move || {
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
                    let staged = crate::node::chunk::StagedChunk::new(
                        crate::dfs::OperationId::new(header.operation_id),
                        bytes,
                    );
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
        let (peer, epoch, _permit) = tokio::task::spawn_blocking(move || {
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
        let identity = super::control::PeerSessionIdentity::new(peer, epoch)?;
        let negotiated = super::control::negotiate_for_peer(
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
        let identity = super::control::PeerSessionIdentity::new(peer, request.peer_node_epoch)?;
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
            let (read, permit) = tokio::task::spawn_blocking(move || {
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
            let identity = super::control::PeerSessionIdentity::new(caller, epoch)?;
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
                tokio::task::spawn_blocking(move || {
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
        let request = request.into_inner();
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
        tokio::task::spawn_blocking(move || {
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

#[tonic::async_trait]
impl NodeData for NodeDataService {
    /// 服务端处理“远端读文件”。
    ///
    /// gRPC inline：直接返回 `data`。
    /// RDMA：先校验 session，再读 Storage，然后服务端 RDMA WRITE 到客户端 MR，
    /// reply 只返回长度，`data` 为空。
    async fn read(
        &self,
        request: Request<DataReadRequest>,
    ) -> Result<Response<DataReadReply>, Status> {
        async move {
            let request = request.into_inner();
            validate_length(request.length)?;
            let transfer = transfer_mode(request.transfer)?;
            if transfer == DataTransfer::Unspecified {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "transfer is required",
                ));
            }
            validate_read_session(&self.sessions, request.session_id, transfer).await?;
            let data = self
                .storage
                .read(&request.name, request.offset, request.length)
                .await
                .map_err(storage_status)?;
            debug_assert_eq!(data.len(), request.length as usize);
            let actual_len = u32::try_from(data.len()).map_err(|_| {
                coded_status(afs_error::NODE_TRANSFER_INTERNAL, "read length exceeds u32")
            })?;
            match transfer {
                DataTransfer::GrpcInline => Ok(Response::new(DataReadReply {
                    length: actual_len,
                    data,
                })),
                DataTransfer::RdmaOneSided => {
                    rdma_write_to_client(&self.sessions, request.session_id, data).await?;
                    Ok(Response::new(DataReadReply {
                        length: actual_len,
                        data: Vec::new(),
                    }))
                }
                DataTransfer::Unspecified => unreachable!("checked above"),
            }
        }
        .instrument(afs_tracing::tracing::info_span!("node.data.read"))
        .await
    }

    /// 服务端处理“远端写文件”。
    ///
    /// gRPC inline：从 request.data 取内容。
    /// RDMA：request.data 必须为空；服务端 RDMA READ 从客户端 MR 拉取内容，
    /// 再写入 Storage。
    async fn write(
        &self,
        request: Request<DataWriteRequest>,
    ) -> Result<Response<DataWriteReply>, Status> {
        async move {
            let request = request.into_inner();
            let data = match transfer_mode(request.transfer)? {
                DataTransfer::GrpcInline => {
                    if !request.length.eq(&0) && request.length as usize != request.data.len() {
                        return Err(coded_status(
                            afs_error::NODE_TRANSFER_INVALID,
                            "inline length/data mismatch",
                        ));
                    }
                    validate_length(request.data.len() as u32)?;
                    request.data
                }
                DataTransfer::RdmaOneSided => {
                    validate_length(request.length)?;
                    if !request.data.is_empty() {
                        return Err(coded_status(
                            afs_error::NODE_TRANSFER_INVALID,
                            "RDMA write must not include inline data",
                        ));
                    }
                    rdma_read_from_client(
                        &self.sessions,
                        request.session_id,
                        request.length as usize,
                    )
                    .await?
                }
                DataTransfer::Unspecified => {
                    return Err(coded_status(
                        afs_error::NODE_TRANSFER_INVALID,
                        "transfer is required",
                    ));
                }
            };
            let written = self
                .storage
                .write(&request.name, request.offset, data)
                .await
                .map_err(storage_status)?;
            Ok(Response::new(DataWriteReply {
                written: written as u32,
            }))
        }
        .instrument(afs_tracing::tracing::info_span!("node.data.write"))
        .await
    }
}

fn validate_length(length: u32) -> Result<(), Status> {
    if length as usize > MAX_TRANSFER_BYTES {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "transfer exceeds 1MiB",
        ));
    }
    Ok(())
}

#[cfg(feature = "dfs")]
fn validate_dfs_read_request(request: &DfsReadRangesRequest) -> Result<(), Status> {
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

/// Owns metadata only; body frames keep their separate payload lease.
#[cfg(feature = "dfs")]
struct PeerOutboardOwner(crate::node::chunk::outboard::SharedOutboard);

#[cfg(feature = "dfs")]
impl AsRef<[u8]> for PeerOutboardOwner {
    fn as_ref(&self) -> &[u8] {
        self.0.encoded()
    }
}

#[cfg(all(feature = "dfs", test))]
fn stream_dfs_ranges(
    local: &LocalChunkStore,
    request: &DfsReadRangesRequest,
    sender: &tokio::sync::mpsc::Sender<Result<DfsReadRangesFrame, Status>>,
    metrics: Option<&DfsPayloadMetrics>,
) -> Result<(), Status> {
    stream_dfs_ranges_until(local, request, sender, metrics, local.peer_serve_deadline())
}

#[cfg(feature = "dfs")]
pub(crate) fn read_session_request_bytes(request: &DfsReadRangesRequest) -> Option<usize> {
    use prost::Message;
    if request.include_blake3_outboard
        || request.encoded_len() > 8000
        || request
            .read_id
            .len()
            .saturating_add(request.attempt_id.len())
            > 256
    {
        return None;
    }
    let mut size = std::mem::size_of::<DfsReadRangesRequest>().checked_add(
        request
            .operations
            .capacity()
            .checked_mul(std::mem::size_of::<afs_protocol::node_data::DfsChunkReadOp>())?,
    )?;
    for text in [
        &request.read_id,
        &request.attempt_id,
        &request.file_version_id,
        &request.layout_root_id,
    ] {
        size = size.checked_add(text.capacity())?;
    }
    for op in &request.operations {
        for text in [&op.chunk_id, &op.source_copy_id] {
            size = size.checked_add(text.capacity())?;
        }
        if let Some(grant) = &op.grant {
            for text in [
                &grant.namespace_id,
                &grant.file_version_id,
                &grant.layout_root_id,
                &grant.caller_node_id,
                &grant.token,
            ] {
                size = size.checked_add(text.capacity())?;
            }
        }
    }
    // Codec buffers are in the original transport envelope, not this lease.
    // Charge actual decoded allocation capacities, including fixed control.
    size.checked_add(2048).filter(|bytes| *bytes <= 32 * 1024)
}

#[cfg(feature = "dfs")]
struct ReadSessionCancel(Arc<std::sync::atomic::AtomicBool>);

#[cfg(feature = "dfs")]
impl Drop for ReadSessionCancel {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(feature = "dfs")]
async fn send_checked_session_frame(
    sender: &tokio::sync::mpsc::Sender<Result<DfsReadSessionFrame, Status>>,
    frame: Result<DfsReadSessionFrame, Status>,
    deadline: std::time::Instant,
    check: impl Fn() -> Result<(), Status>,
) -> Result<(), Status> {
    loop {
        check()?;
        // A capacity notification wakes immediately. This timer only bounds
        // stop/grant checks while genuinely stalled; it is not a fixed delay.
        let poll_end =
            (std::time::Instant::now() + std::time::Duration::from_millis(20)).min(deadline);
        match tokio::time::timeout_at(tokio::time::Instant::from_std(poll_end), sender.reserve())
            .await
        {
            Ok(Ok(permit)) => {
                check()?;
                permit.send(frame);
                return Ok(());
            }
            Ok(Err(_)) => return Err(Status::cancelled("DFS session consumer disconnected")),
            Err(_) => continue,
        }
    }
}

#[cfg(feature = "dfs")]
async fn serve_read_session(
    service: DfsChunksService,
    peer: Option<String>,
    mut incoming: tonic::Streaming<DfsReadSessionRequest>,
    sender: &tokio::sync::mpsc::Sender<Result<DfsReadSessionFrame, Status>>,
) -> Result<(), Status> {
    use prost::Message;
    let local = service
        .local_chunks
        .as_ref()
        .ok_or_else(|| Status::unavailable("DFS local ChunkStore is not available"))?;
    loop {
        // Polling here only bounds idle retirement/Node stop. It does not
        // renew the active request's absolute deadline or consume read slots.
        let idle_end = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let message = loop {
            local.ensure_peer_serve_open().map_err(error_to_status)?;
            if tokio::time::Instant::now() >= idle_end {
                return Ok(());
            }
            tokio::select! {
                _ = sender.closed() => return Ok(()),
                result = tokio::time::timeout(std::time::Duration::from_millis(20), incoming.message()) => {
                    match result {
                        Ok(message) => break message?,
                        Err(_) => continue,
                    }
                }
            }
        };
        let Some(message) = message else {
            return Ok(());
        };
        if message.encoded_len() > 8192 || message.timeout_millis == 0 {
            return Err(Status::invalid_argument(
                "DFS session request exceeds metadata/deadline bound",
            ));
        }
        let end = std::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(message.timeout_millis))
            .ok_or_else(|| Status::invalid_argument("DFS session deadline overflows"))?;
        let admission = local.peer_serve_deadline().min(end);
        let read = message
            .request
            .ok_or_else(|| Status::invalid_argument("DFS session has no read request"))?;
        let control_bytes = read_session_request_bytes(&read)
            .and_then(|bytes| bytes.checked_add(peer.as_ref().map_or(0, String::capacity)))
            .ok_or_else(|| {
                Status::resource_exhausted("DFS session decoded metadata exceeds bound")
            })?;
        validate_dfs_read_request(&read)?;
        if let Some(peer) = &peer {
            for op in &read.operations {
                if op
                    .grant
                    .as_ref()
                    .is_none_or(|grant| grant.caller_node_id != *peer)
                {
                    return Err(Status::permission_denied(
                        "DFS read caller differs from authenticated adapter",
                    ));
                }
            }
        }
        let permit = tokio::time::timeout_at(
            tokio::time::Instant::from_std(admission),
            service.read_permits.clone().acquire_owned(),
        )
        .await
        .map_err(|_| Status::resource_exhausted("DFS session read admission deadline exceeded"))?
        .map_err(|_| Status::unavailable("DFS peer read admission is closed"))?;
        local.ensure_peer_serve_open().map_err(error_to_status)?;
        let end_frame = DfsReadSessionEnd {
            read_id: read.read_id.clone(),
            attempt_id: read.attempt_id.clone(),
        };
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let _cancel = ReadSessionCancel(cancelled.clone());
        let producer_local = local.clone();
        let authorizer = service.authorizer.clone();
        let metrics = service.payload_metrics.clone();
        let peer = peer.clone();
        let producer_sender = sender.clone();
        // The original producer owns the read slot and all active metadata
        // until it actually exits, including when the async driver is dropped.
        let producer = tokio::task::spawn_blocking(move || {
            let local = producer_local;
            let _permit = permit;
            let _metadata = local
                .reserve_peer_control_until(control_bytes, admission)
                .map_err(error_to_status)?;
            let check = || {
                if cancelled.load(std::sync::atomic::Ordering::Acquire)
                    || producer_sender.is_closed()
                {
                    return Err(Status::cancelled("DFS session consumer disconnected"));
                }
                if std::time::Instant::now() >= end {
                    return Err(Status::deadline_exceeded(
                        "DFS session read total deadline exceeded",
                    ));
                }
                local.ensure_peer_serve_open().map_err(error_to_status)?;
                validate_dfs_read_request(&read)
            };
            check()?;
            let caller = peer
                .as_deref()
                .or_else(|| InternalPeerRequest::caller_node_id(&read))
                .ok_or_else(|| {
                    Status::invalid_argument("DFS session read has no caller context")
                })?;
            authorizer
                .authorize(caller, &read)
                .map_err(error_to_status)?;
            check()?;
            local.record_peer_read_event("blocking_combined");
            stream_dfs_ranges_with(
                &local,
                &read,
                metrics.as_ref(),
                admission,
                |body| {
                    let frame = Ok(DfsReadSessionFrame {
                        body: Some(dfs_read_session_frame::Body::Frame(DfsReadRangesFrame {
                            body: Some(body),
                        })),
                    });
                    // Backpressure retains the original frame/lease. It cannot
                    // outlive cancellation, grant expiry, Node stop or deadline.
                    check()?;
                    match producer_sender.try_send(frame) {
                        Ok(()) => Ok(()),
                        Err(tokio::sync::mpsc::error::TrySendError::Full(frame)) => {
                            // Re-enter the existing runtime only from this
                            // existing blocking producer, never a new worker.
                            let runtime = tokio::runtime::Handle::try_current()
                                .map_err(|error| Status::internal(error.to_string()))?;
                            runtime.block_on(send_checked_session_frame(
                                &producer_sender,
                                frame,
                                end,
                                check,
                            ))
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            Err(Status::cancelled("DFS session consumer disconnected"))
                        }
                    }
                },
                || {
                    cancelled.load(std::sync::atomic::Ordering::Acquire)
                        || producer_sender.is_closed()
                },
            )?;
            check()?;
            Ok::<_, Status>((read, _metadata))
        });
        let (read, _metadata) =
            tokio::time::timeout_at(tokio::time::Instant::from_std(end), producer)
                .await
                .map_err(|_| Status::deadline_exceeded("DFS session producer deadline exceeded"))?
                .map_err(|error| Status::internal(error.to_string()))??;
        local.ensure_peer_serve_open().map_err(error_to_status)?;
        validate_dfs_read_request(&read)?;
        // Never publish End before the real producer join, even after a valid
        // Completion frame. Errors between Completion and join remain fatal.
        let terminal = Ok(DfsReadSessionFrame {
            body: Some(dfs_read_session_frame::Body::End(end_frame)),
        });
        send_checked_session_frame(sender, terminal, end, || {
            local.ensure_peer_serve_open().map_err(error_to_status)?;
            validate_dfs_read_request(&read)?;
            if std::time::Instant::now() >= end {
                return Err(Status::deadline_exceeded(
                    "DFS session End deadline exceeded",
                ));
            }
            Ok(())
        })
        .await?;
    }
}

#[cfg(feature = "dfs")]
fn stream_dfs_ranges_until(
    local: &LocalChunkStore,
    request: &DfsReadRangesRequest,
    sender: &tokio::sync::mpsc::Sender<Result<DfsReadRangesFrame, Status>>,
    metrics: Option<&DfsPayloadMetrics>,
    admission_deadline: std::time::Instant,
) -> Result<(), Status> {
    let send = |body| {
        sender
            .blocking_send(Ok(DfsReadRangesFrame { body: Some(body) }))
            .map_err(|_| Status::cancelled("DFS range consumer disconnected"))
    };
    stream_dfs_ranges_with(local, request, metrics, admission_deadline, send, || {
        sender.is_closed()
    })
}

#[cfg(feature = "dfs")]
fn stream_dfs_ranges_with(
    local: &LocalChunkStore,
    request: &DfsReadRangesRequest,
    metrics: Option<&DfsPayloadMetrics>,
    admission_deadline: std::time::Instant,
    send: impl Fn(dfs_read_ranges_frame::Body) -> Result<(), Status>,
    closed: impl Fn() -> bool,
) -> Result<(), Status> {
    for op in &request.operations {
        if closed() {
            return Ok(());
        }
        let diagnostic =
            local.read_diagnostic(crate::node::read_work::ReadDiagnosticStage::PeerServe);
        let diagnostic_token = diagnostic.as_ref().map(|span| span.token());
        // Sidecars bind every streamed coverage group to the catalog's
        // standard root. There is no whole-object body allocation or pre-scan.
        // Legacy, partial and proof-capacity fallbacks retain the path below.
        if request.include_blake3_outboard
            && let Some(mut groups) = local
                .open_peer_group_stream_until(
                    &crate::dfs::ChunkId::new(op.chunk_id.clone()),
                    op.chunk_offset,
                    op.length as usize,
                    admission_deadline,
                )
                .map_err(error_to_status)?
        {
            let check_caller = || {
                if closed() {
                    return Err(afs_transport::grpc::error_status::status_to_error(
                        Status::cancelled("DFS range consumer disconnected"),
                    ));
                }
                validate_dfs_read_request(request)
                    .map_err(afs_transport::grpc::error_status::status_to_error)
            };
            check_caller().map_err(error_to_status)?;
            local.ensure_peer_serve_open().map_err(error_to_status)?;
            send(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                operation_index: op.operation_index,
                chunk_id: op.chunk_id.clone(),
                chunk_offset: op.chunk_offset,
                length: op.length,
                source_copy_id: op.source_copy_id.clone(),
                blake3_outboard: prost::bytes::Bytes::from_owner(PeerOutboardOwner(
                    groups.outboard(),
                )),
            }))?;
            let mut first = true;
            while let Some(data) = groups.next_group(check_caller).map_err(error_to_status)? {
                let length = data.len();
                let first_enqueue = first
                    .then(|| local.coarse_diagnostic("peer_first_frame_enqueue", length as u64));
                send(dfs_read_ranges_frame::Body::Data(data))?;
                drop(first_enqueue);
                if first && let Some(token) = &diagnostic_token {
                    token.first_data();
                }
                first = false;
                local.record_peer_read_event("data_frame");
                local.record_peer_read_event("shared_frame");
                if let Some(metrics) = metrics {
                    metrics.record("grpc", "send", "read", length as u64);
                }
            }
            // next_group(None) has checked the current path, catalog and grant
            // after the last group. B still verifies the complete object before
            // installing it; a partial prefix is never an object publication.
            send(dfs_read_ranges_frame::Body::Completion(
                DfsReadRangesCompletion {
                    read_id: request.read_id.clone(),
                    attempt_id: request.attempt_id.clone(),
                    operation_index: op.operation_index,
                    source_copy_id: op.source_copy_id.clone(),
                    transferred_bytes: op.length,
                    range_checksum: groups.digest().bytes.to_vec(),
                    range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                        as i32,
                },
            ))?;
            if let Some(diagnostic) = diagnostic {
                diagnostic.finish(true);
            }
            continue;
        }
        // Authorization has already succeeded for this request. Reuse only
        // verified immutable bytes, with the shared payload lease held by
        // every queued frame. No separate range or frame body is allocated.
        let verified = local
            .read_peer_range_until(
                &crate::dfs::ChunkId::new(op.chunk_id.clone()),
                op.chunk_offset,
                op.length as usize,
                admission_deadline,
            )
            .map_err(error_to_status)?;
        if verified.len() != op.length as usize {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_CORRUPT_DATA,
                "local Chunk ended before the requested DFS range",
            ));
        }
        // This capability exists only for the full allocation already verified
        // against the local catalog. Partial/fallback views keep hashing the
        // actual range; no request or self-reported header supplies the proof.
        let whole_digest = verified.verified_full_object_digest();
        // The Bytes owner retains the same budgeted proof allocation through
        // queueing and tonic encoding. Codec/network buffers retain their
        // existing transport budget; they are not charged to this proof lease.
        let blake3_outboard = if request.include_blake3_outboard {
            verified
                .outboard()
                .map(|proof| prost::bytes::Bytes::from_owner(PeerOutboardOwner(proof)))
                .unwrap_or_default()
        } else {
            Default::default()
        };
        let verified = verified.into_shared_bytes();
        // A fill follower can wait behind I/O. Content sharing never renews the
        // caller's grant or bypasses a known Node stop; no new Meta RPC here.
        validate_dfs_read_request(request)?;
        local.ensure_peer_serve_open().map_err(error_to_status)?;
        if !request.include_blake3_outboard
            && verified.len() <= afs_protocol::node_data::DFS_COMPACT_RANGE_MAX_BYTES
        {
            // This range is already fully verified and budgeted. Its Bytes
            // retains that same owner through queueing and encoding; neither
            // a second body nor repeated request identity is needed.
            let digest = whole_digest.map_or_else(
                || {
                    local.record_read_hash("stream_range_hash", verified.len() as u64);
                    blake3::hash(&verified).as_bytes().to_vec()
                },
                |digest| digest.bytes.to_vec(),
            );
            let length = verified.len();
            let first_enqueue = local.coarse_diagnostic("peer_first_frame_enqueue", length as u64);
            send(dfs_read_ranges_frame::Body::Compact(DfsReadRangeReply {
                operation_index: op.operation_index,
                data: verified,
                range_checksum: digest,
            }))?;
            drop(first_enqueue);
            if let Some(token) = &diagnostic_token {
                token.first_data();
            }
            local.record_peer_read_event("data_frame");
            local.record_peer_read_event("shared_frame");
            if let Some(metrics) = metrics {
                metrics.record("grpc", "send", "read", length as u64);
            }
            if let Some(diagnostic) = diagnostic {
                diagnostic.finish(true);
            }
            continue;
        }
        send(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            operation_index: op.operation_index,
            chunk_id: op.chunk_id.clone(),
            chunk_offset: op.chunk_offset,
            length: op.length,
            source_copy_id: op.source_copy_id.clone(),
            blake3_outboard,
        }))?;
        let mut offset = 0;
        let mut checksum = blake3::Hasher::new();
        while offset < op.length {
            if closed() {
                return Ok(());
            }
            let length = (op.length - offset).min(64 * 1024) as usize;
            let data = verified.slice(offset as usize..offset as usize + length);
            if whole_digest.is_none() {
                checksum.update(&data);
                local.record_read_hash("stream_range_hash", length as u64);
            }
            let first_enqueue = (offset == 0)
                .then(|| local.coarse_diagnostic("peer_first_frame_enqueue", length as u64));
            send(dfs_read_ranges_frame::Body::Data(data))?;
            drop(first_enqueue);
            if offset == 0
                && let Some(token) = &diagnostic_token
            {
                // Accepted by the existing channel, not an on-wire/peer receipt.
                token.first_data();
            }
            local.record_peer_read_event("data_frame");
            local.record_peer_read_event("shared_frame");
            if let Some(metrics) = metrics {
                metrics.record("grpc", "send", "read", length as u64);
            }
            offset += length as u64;
        }
        send(dfs_read_ranges_frame::Body::Completion(
            DfsReadRangesCompletion {
                read_id: request.read_id.clone(),
                attempt_id: request.attempt_id.clone(),
                operation_index: op.operation_index,
                source_copy_id: op.source_copy_id.clone(),
                transferred_bytes: op.length,
                range_checksum: whole_digest.map_or_else(
                    || checksum.finalize().as_bytes().to_vec(),
                    |digest| digest.bytes.to_vec(),
                ),
                range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                    as i32,
            },
        ))?;
        if let Some(diagnostic) = diagnostic {
            diagnostic.finish(true);
        }
    }
    Ok(())
}

/// Caller context carried by an internal request, never a credential.
pub trait InternalPeerRequest {
    fn caller_node_id(&self) -> Option<&str>;
}

#[cfg(any(feature = "ownerfs", feature = "dfs"))]
macro_rules! internal_peer_requests {
    ($($request_type:path),+ => |$request:ident| $body:expr) => {
        $(impl InternalPeerRequest for $request_type {
            fn caller_node_id(&self) -> Option<&str> {
                let $request = self;
                $body
            }
        })+
    };
}

#[cfg(feature = "ownerfs")]
internal_peer_requests!(
    OwnerLookupRequest, OwnerGetAttrRequest, OwnerStatFsRequest, OwnerSetAttrRequest,
    OwnerGetXattrRequest, OwnerListXattrRequest, OwnerSetXattrRequest,
    OwnerRemoveXattrRequest, OwnerCreateRequest, OwnerMkdirRequest, OwnerMknodRequest,
    OwnerUnlinkRequest, OwnerRmdirRequest, OwnerRenameRequest, OwnerOpenRequest,
    OwnerReadlinkRequest, OwnerReadRequest, OwnerWriteRequest, OwnerFlushRequest,
    OwnerFsyncRequest, OwnerReleaseRequest, OwnerOpendirRequest, OwnerReaddirRequest,
    OwnerFsyncDirRequest, OwnerReleaseDirRequest, OwnerSymlinkRequest, OwnerLinkRequest,
    afs_protocol::node_control::OwnerNegotiateDataRequest,
    afs_protocol::node_control::OwnerCloseDataRequest,
    afs_protocol::node_control::OwnerCancelLockWaitRequest,
    afs_protocol::node_control::OwnerReleaseLockSessionRequest,
    afs_protocol::node_control::OwnerAckLockWaitRequest
    => |request| request.access.as_ref().map(|access| access.holder_node_id.as_str())
);

#[cfg(feature = "ownerfs")]
internal_peer_requests!(
    afs_protocol::node_control::OwnerGetLockRequest,
    afs_protocol::node_control::OwnerSetLockRequest,
    afs_protocol::node_control::OwnerReleaseLocksRequest
    => |request| request.handle.as_ref().and_then(|handle| handle.access.as_ref())
        .map(|access| access.holder_node_id.as_str())
);

#[cfg(feature = "dfs")]
internal_peer_requests!(
    DfsOwnerReadRequest, DfsOwnerWriteRequest, DfsOwnerResizeRequest, DfsOwnerSyncRequest,
    afs_protocol::node_control::DfsOwnerGetAttrRequest,
    afs_protocol::node_control::DfsOwnerReleaseRequest
    => |request| request.handle.as_ref().map(|handle| handle.caller_node_id.as_str())
);

#[cfg(feature = "dfs")]
internal_peer_requests!(
    afs_protocol::node_control::DfsOwnerOpenRequest,
    afs_protocol::node_control::DfsOwnerMknodRequest,
    afs_protocol::node_data::DfsCloseRdmaRequest
    => |request| Some(request.caller_node_id.as_str())
);

#[cfg(feature = "dfs")]
internal_peer_requests!(
    afs_protocol::node_control::DfsOwnerGetLockRequest,
    afs_protocol::node_control::DfsOwnerSetLockRequest,
    afs_protocol::node_control::DfsOwnerCancelLockWaitRequest,
    afs_protocol::node_control::DfsOwnerAcknowledgeLockWaitRequest,
    afs_protocol::node_control::DfsOwnerReleaseLocksRequest,
    afs_protocol::node_control::DfsOwnerReleaseLockSessionRequest
    => |request| request.authority.as_ref().map(|authority| authority.caller_node_id.as_str())
);

#[cfg(feature = "dfs")]
internal_peer_requests!(DfsReadRangesRequest => |request|
    request.operations.first().and_then(|operation| operation.grant.as_ref())
        .map(|grant| grant.caller_node_id.as_str())
);

#[cfg(feature = "dfs")]
internal_peer_requests!(afs_protocol::node_data::DfsPutReplicaHeader => |request|
    if request.target_index == 0 {
        Some(request.initiator_node_id.as_str())
    } else {
        request.ordered_targets.get(usize::try_from(request.target_index - 1).ok()?)
            .map(|target| target.node_id.as_str())
    }
);

#[cfg(feature = "dfs")]
internal_peer_requests!(DfsPutReplicaRdmaRequest => |request|
    request.header.as_ref().and_then(InternalPeerRequest::caller_node_id)
);

#[cfg(feature = "dfs")]
internal_peer_requests!(DfsReadRangesRdmaRequest => |request|
    request.request.as_ref().and_then(InternalPeerRequest::caller_node_id)
);

#[cfg(feature = "dfs")]
internal_peer_requests!(afs_protocol::node_data::dfs_negotiate_rdma_request::Authority => |request|
    match request {
        afs_protocol::node_data::dfs_negotiate_rdma_request::Authority::ReplicaHeader(header) => header.caller_node_id(),
        afs_protocol::node_data::dfs_negotiate_rdma_request::Authority::ReadRequest(read) => read.caller_node_id(),
    }
);

#[cfg(feature = "dfs")]
internal_peer_requests!(DfsNegotiateRdmaRequest => |request|
    request.authority.as_ref().and_then(InternalPeerRequest::caller_node_id)
);

fn resolve_internal_peer(identity: Option<&str>) -> Result<String, Status> {
    identity
        .filter(|identity| !identity.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| {
            coded_status(
                afs_error::CLIENT_ARGUMENT_INVALID,
                "internal request has no caller node context",
            )
        })
}

fn authenticate_transport_peer<T>(
    authenticator: &dyn PeerAuthenticator,
    request: &Request<T>,
) -> Result<Option<String>, Status> {
    if authenticator.trusts_internal_requests() {
        return Ok(None);
    }
    let certificates = request.peer_certs();
    authenticator
        .authenticate(
            request.metadata(),
            request.remote_addr(),
            certificates
                .as_ref()
                .and_then(|certs| certs.first().map(|cert| cert.as_ref())),
        )
        .map(Some)
        .map_err(error_to_status)
}

/// Internal services use their request context; explicit authenticated adapters
/// retain their transport identity and the existing request/state comparisons.
pub fn authenticate_peer<T: InternalPeerRequest>(
    authenticator: &dyn PeerAuthenticator,
    request: &Request<T>,
) -> Result<String, Status> {
    match authenticate_transport_peer(authenticator, request)? {
        Some(identity) => Ok(identity),
        None => resolve_internal_peer(request.get_ref().caller_node_id()),
    }
}

fn transfer_mode(value: i32) -> Result<DataTransfer, Status> {
    DataTransfer::try_from(value)
        .map_err(|_| coded_status(afs_error::NODE_TRANSFER_INVALID, "unknown transfer mode"))
}

/// RDMA read 必须先校验 session，再碰 Storage。
///
/// 这样旧 session/poisoned session 不会因为文件不存在等业务错误掩盖掉传输授权错误。
async fn validate_read_session(
    sessions: &RdmaSessionRegistry,
    session_id: u64,
    transfer: DataTransfer,
) -> Result<(), Status> {
    if transfer == DataTransfer::RdmaOneSided {
        sessions.session(session_id).await?;
    }
    Ok(())
}

fn storage_status(error: StorageError) -> Status {
    error_to_status(error.into())
}

/// 写文件 RDMA 路径：服务端从客户端 MR 拉取 bytes。
///
/// verbs 方向是 server RDMA READ；函数名中的 from_client 表达业务视角。
#[cfg(feature = "rdma")]
async fn rdma_read_from_client(
    sessions: &RdmaSessionRegistry,
    session_id: u64,
    len: usize,
) -> Result<Vec<u8>, Status> {
    let session = sessions.session(session_id).await?;
    let endpoint = session.endpoint.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut endpoint = endpoint.blocking_lock();
        endpoint.transfer_read(len).map_err(rdma_status)?;
        endpoint.get_local(len).map_err(rdma_status)
    })
    .await
    .map_err(|error| coded_status(afs_error::NODE_TRANSFER_INTERNAL, error.to_string()))?;
    if result.is_err() {
        session.poisoned.store(true, Ordering::SeqCst);
    }
    result
}

/// 未编译 RDMA 时明确拒绝单边写文件请求，不在这里自动重放为 gRPC。
#[cfg(not(feature = "rdma"))]
async fn rdma_read_from_client(
    _sessions: &RdmaSessionRegistry,
    _session_id: u64,
    _len: usize,
) -> Result<Vec<u8>, Status> {
    Err(coded_status(
        afs_error::NODE_TRANSFER_UNSUPPORTED,
        "RDMA feature is not enabled",
    ))
}

/// 读文件 RDMA 路径：服务端把 bytes 推到客户端 MR。
///
/// verbs 方向是 server RDMA WRITE；函数名中的 to_client 表达业务视角。
#[cfg(feature = "rdma")]
async fn rdma_write_to_client(
    sessions: &RdmaSessionRegistry,
    session_id: u64,
    data: Vec<u8>,
) -> Result<(), Status> {
    let session = sessions.session(session_id).await?;
    let endpoint = session.endpoint.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut endpoint = endpoint.blocking_lock();
        endpoint.put_local(&data).map_err(rdma_status)?;
        endpoint.transfer_write(data.len()).map_err(rdma_status)
    })
    .await
    .map_err(|error| coded_status(afs_error::NODE_TRANSFER_INTERNAL, error.to_string()))?;
    if result.is_err() {
        session.poisoned.store(true, Ordering::SeqCst);
    }
    result
}

/// 未编译 RDMA 时明确拒绝单边读文件请求。
#[cfg(not(feature = "rdma"))]
async fn rdma_write_to_client(
    _sessions: &RdmaSessionRegistry,
    _session_id: u64,
    _data: Vec<u8>,
) -> Result<(), Status> {
    Err(coded_status(
        afs_error::NODE_TRANSFER_UNSUPPORTED,
        "RDMA feature is not enabled",
    ))
}

#[cfg(feature = "rdma")]
fn rdma_status(error: afs_transport::rdma::RdmaError) -> Status {
    coded_status(afs_error::NODE_TRANSFER_UNAVAILABLE, error.to_string())
}

// OwnerFs Peer 文件服务与诊断服务共处 data.rs；feature 属性只控制编译，
// 不形成独立文件或公开模块层级。
#[cfg(feature = "ownerfs")]
use std::{
    ffi::OsString,
    os::unix::ffi::OsStringExt,
    time::{Duration, UNIX_EPOCH},
};

#[cfg(feature = "ownerfs")]
use afs_protocol::node_data::{
    OwnerCreateReply, OwnerCreateRequest, OwnerDirEntry, OwnerDirectoryHandle, OwnerFlushReply,
    OwnerFlushRequest, OwnerFsyncDirReply, OwnerFsyncDirRequest, OwnerFsyncReply,
    OwnerFsyncRequest, OwnerGetAttrReply, OwnerGetAttrRequest, OwnerGetXattrReply,
    OwnerGetXattrRequest, OwnerLinkReply, OwnerLinkRequest, OwnerListXattrReply,
    OwnerListXattrRequest, OwnerLookupReply, OwnerLookupRequest, OwnerMkdirReply,
    OwnerMkdirRequest, OwnerMknodReply, OwnerMknodRequest, OwnerOpenReply, OwnerOpenRequest,
    OwnerOpendirReply, OwnerOpendirRequest, OwnerReadReply, OwnerReadRequest, OwnerReaddirReply,
    OwnerReaddirRequest, OwnerReadlinkReply, OwnerReadlinkRequest, OwnerReleaseDirReply,
    OwnerReleaseDirRequest, OwnerReleaseReply, OwnerReleaseRequest, OwnerRemoveXattrReply,
    OwnerRemoveXattrRequest, OwnerRenameReply, OwnerRenameRequest, OwnerRmdirReply,
    OwnerRmdirRequest, OwnerSetAttrReply, OwnerSetAttrRequest, OwnerSetXattrReply,
    OwnerSetXattrRequest, OwnerStatFsReply, OwnerStatFsRequest, OwnerSymlinkReply,
    OwnerSymlinkRequest, OwnerUnlinkReply, OwnerUnlinkRequest, OwnerWriteReply, OwnerWriteRequest,
    owner_files_server::{OwnerFiles, OwnerFilesServer},
};

#[cfg(feature = "ownerfs")]
use crate::node::vfs::{
    ownerfs::{
        OwnerFsPeerExecutor,
        files::{FileIdentity, RemoteDirectory, RemoteFile},
        root::{PresentedRootAccess, RootId},
    },
    types::{
        AttributeChange, FileAttributes, FileKind, FilesystemCapacity, OpenOptions, RenameFlags,
        RequestContext, SetAttrOptions, SpecialFileKind, WriteOptions,
    },
};

/// Home 侧真实文件操作接口。
///
/// 该 trait 保持同步形态，因为 OwnerFs/FUSE 后端目前是同步 VFS 合同，且
/// RootMeta adapter 可能在阻塞线程里等待异步 gRPC。生产多线程 runtime
/// 使用 `block_in_place` 处理短本机操作，使 Tokio 能调度其他任务，同时省去
/// 每次文件 RPC 投递阻塞线程池的开销；单线程测试 runtime 则使用 `spawn_blocking`。
/// The peer Node ID follows the explicitly selected policy: internal request
/// context or authenticated boundary identity. Handlers retain state/session fences.
#[cfg(feature = "ownerfs")]
pub trait OwnerFilesHandler: Send + Sync + 'static {
    fn lookup(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerLookupRequest,
    ) -> afs_error::Result<OwnerLookupReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Lookup"))
    }
    fn get_attr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerGetAttrRequest,
    ) -> afs_error::Result<OwnerGetAttrReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.GetAttr"))
    }
    fn statfs(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerStatFsRequest,
    ) -> afs_error::Result<OwnerStatFsReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.StatFs"))
    }
    fn set_attr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerSetAttrRequest,
    ) -> afs_error::Result<OwnerSetAttrReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.SetAttr"))
    }
    fn get_xattr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerGetXattrRequest,
    ) -> afs_error::Result<OwnerGetXattrReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.GetXattr"))
    }
    fn list_xattr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerListXattrRequest,
    ) -> afs_error::Result<OwnerListXattrReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.ListXattr"))
    }
    fn set_xattr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerSetXattrRequest,
    ) -> afs_error::Result<OwnerSetXattrReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.SetXattr"))
    }
    fn remove_xattr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerRemoveXattrRequest,
    ) -> afs_error::Result<OwnerRemoveXattrReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.RemoveXattr"))
    }
    fn create(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerCreateRequest,
    ) -> afs_error::Result<OwnerCreateReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Create"))
    }
    fn mkdir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerMkdirRequest,
    ) -> afs_error::Result<OwnerMkdirReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Mkdir"))
    }
    fn mknod(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerMknodRequest,
    ) -> afs_error::Result<OwnerMknodReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Mknod"))
    }
    fn unlink(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerUnlinkRequest,
    ) -> afs_error::Result<OwnerUnlinkReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Unlink"))
    }
    fn rmdir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerRmdirRequest,
    ) -> afs_error::Result<OwnerRmdirReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Rmdir"))
    }
    fn rename(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerRenameRequest,
    ) -> afs_error::Result<OwnerRenameReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Rename"))
    }
    fn open(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerOpenRequest,
    ) -> afs_error::Result<OwnerOpenReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Open"))
    }
    fn readlink(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerReadlinkRequest,
    ) -> afs_error::Result<OwnerReadlinkReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Readlink"))
    }
    fn read(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerReadRequest,
    ) -> afs_error::Result<OwnerReadReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Read"))
    }
    fn write(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerWriteRequest,
    ) -> afs_error::Result<OwnerWriteReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Write"))
    }
    fn authorize_data_write(
        &self,
        authenticated_peer_node_id: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
    ) -> afs_error::Result<()> {
        let _ = (authenticated_peer_node_id, access, file);
        Err(owner_handler_unimplemented("OwnerFiles.AuthorizeDataWrite"))
    }
    fn flush(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerFlushRequest,
    ) -> afs_error::Result<OwnerFlushReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Flush"))
    }
    fn fsync(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerFsyncRequest,
    ) -> afs_error::Result<OwnerFsyncReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Fsync"))
    }
    fn release(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerReleaseRequest,
    ) -> afs_error::Result<OwnerReleaseReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Release"))
    }
    fn opendir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerOpendirRequest,
    ) -> afs_error::Result<OwnerOpendirReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Opendir"))
    }
    fn readdir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerReaddirRequest,
    ) -> afs_error::Result<OwnerReaddirReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Readdir"))
    }
    fn fsync_dir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerFsyncDirRequest,
    ) -> afs_error::Result<OwnerFsyncDirReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.FsyncDir"))
    }
    fn release_dir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerReleaseDirRequest,
    ) -> afs_error::Result<OwnerReleaseDirReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.ReleaseDir"))
    }
    fn symlink(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerSymlinkRequest,
    ) -> afs_error::Result<OwnerSymlinkReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Symlink"))
    }
    fn link(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerLinkRequest,
    ) -> afs_error::Result<OwnerLinkReply> {
        let _ = (authenticated_peer_node_id, request);
        Err(owner_handler_unimplemented("OwnerFiles.Link"))
    }
}

/// Home-side OwnerFiles handler backed by the real OwnerFs peer executor.
///
/// This is the production adapter boundary: gRPC/RDMA command handlers stay in
/// `node/rpc`, while ordinary-file semantics, root-grant validation, stale
/// handle detection, and old-FD behavior stay in `OwnerFsPeerExecutor`.
#[derive(Clone)]
#[cfg(feature = "ownerfs")]
pub struct OwnerFsPeerHandler {
    executor: OwnerFsPeerExecutor,
}

#[cfg(feature = "ownerfs")]
impl OwnerFsPeerHandler {
    #[must_use]
    pub fn new(executor: OwnerFsPeerExecutor) -> Self {
        Self { executor }
    }
}

#[cfg(feature = "ownerfs")]
pub fn make_owner_files_handler(executor: OwnerFsPeerExecutor) -> Arc<dyn OwnerFilesHandler> {
    Arc::new(OwnerFsPeerHandler::new(executor))
}

#[cfg(feature = "ownerfs")]
impl OwnerFilesHandler for OwnerFsPeerHandler {
    fn lookup(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerLookupRequest,
    ) -> afs_error::Result<OwnerLookupReply> {
        let access = presented_access(request.access)?;
        let path_is_root = request.path.is_empty();
        let expected_parent = if path_is_root {
            request
                .expected_parent_identity
                .map(|identity| FileIdentity(identity.opaque))
        } else {
            Some(required_identity(
                request.expected_parent_identity,
                "OwnerLookupRequest missing expected_parent_identity",
            )?)
        };
        let entry = self.executor.lookup(
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            expected_parent.as_ref(),
        )?;
        Ok(OwnerLookupReply {
            attr: Some(owner_attr(entry.identity, entry.attributes)),
            owner_session_id: access.home_session_id,
        })
    }

    fn get_attr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerGetAttrRequest,
    ) -> afs_error::Result<OwnerGetAttrReply> {
        let access = presented_access(request.access)?;
        let expected = request
            .expected_file_identity
            .map(|identity| FileIdentity(identity.opaque));
        let remote = request
            .handle
            .map(|handle| remote_file_for_handle(&access, handle.opaque));
        let entry = self.executor.getattr(
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            expected.as_ref(),
            remote.as_ref(),
        )?;
        Ok(OwnerGetAttrReply {
            attr: Some(owner_attr(entry.identity, entry.attributes)),
            owner_session_id: access.home_session_id,
        })
    }

    fn statfs(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerStatFsRequest,
    ) -> afs_error::Result<OwnerStatFsReply> {
        let access = presented_access(request.access)?;
        let expected = request
            .expected_file_identity
            .map(|identity| FileIdentity(identity.opaque));
        let capacity = self.executor.statfs(
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            expected.as_ref(),
        )?;
        Ok(OwnerStatFsReply {
            capacity: Some(owner_capacity(capacity)),
        })
    }

    fn create(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerCreateRequest,
    ) -> afs_error::Result<OwnerCreateReply> {
        let access = presented_access(request.access)?;
        let expected_parent = required_identity(
            request.expected_parent,
            "OwnerCreateRequest missing expected_parent",
        )?;
        let ctx = caller_context(request.caller)?;
        let options = OpenOptions {
            kill_suidgid: request.kill_suidgid,
        };
        let created = self.executor.create_with_options(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            request.flags as i32,
            request.mode,
            &expected_parent,
            options,
        )?;
        Ok(OwnerCreateReply {
            handle: Some(afs_protocol::node_data::OwnerHandle {
                opaque: created.file.handle,
            }),
            attr: Some(owner_attr(created.entry.identity, created.entry.attributes)),
            owner_session_id: access.home_session_id,
        })
    }

    fn mkdir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerMkdirRequest,
    ) -> afs_error::Result<OwnerMkdirReply> {
        let access = presented_access(request.access)?;
        let expected_parent = required_identity(
            request.expected_parent,
            "OwnerMkdirRequest missing expected_parent",
        )?;
        let ctx = caller_context(request.caller)?;
        let entry = self.executor.mkdir(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            request.mode,
            &expected_parent,
        )?;
        Ok(OwnerMkdirReply {
            attr: Some(owner_attr(entry.identity, entry.attributes)),
            owner_session_id: access.home_session_id,
        })
    }

    fn mknod(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerMknodRequest,
    ) -> afs_error::Result<OwnerMknodReply> {
        let access = presented_access(request.access)?;
        let expected_parent = required_identity(
            request.expected_parent,
            "OwnerMknodRequest missing expected_parent",
        )?;
        let kind = owner_special_kind(request.special_node)?;
        let ctx = caller_context(request.caller)?;
        let entry = self.executor.mknod(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            kind,
            request.mode,
            &expected_parent,
        )?;
        Ok(OwnerMknodReply {
            attr: Some(owner_attr(entry.identity, entry.attributes)),
            owner_session_id: access.home_session_id,
        })
    }

    fn set_attr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerSetAttrRequest,
    ) -> afs_error::Result<OwnerSetAttrReply> {
        let access = presented_access(request.access)?;
        let expected = request
            .expected_file_identity
            .map(|identity| FileIdentity(identity.opaque));
        let remote = request
            .handle
            .map(|handle| remote_file_for_handle(&access, handle.opaque));
        let attr = request
            .attr
            .ok_or_else(|| protocol_error("OwnerSetAttrRequest missing attr"))?;
        let options = SetAttrOptions {
            kill_suidgid: attr.kill_suidgid,
            timestamps_now: attr.timestamps_now,
        };
        let change = attribute_change(attr);
        let ctx = caller_context(request.caller)?;
        let entry = self.executor.setattr_with_options(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            expected.as_ref(),
            remote.as_ref(),
            &change,
            options,
        )?;
        Ok(OwnerSetAttrReply {
            attr: Some(owner_attr(entry.identity, entry.attributes)),
            owner_session_id: access.home_session_id,
        })
    }

    fn get_xattr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerGetXattrRequest,
    ) -> afs_error::Result<OwnerGetXattrReply> {
        let access = presented_access(request.access)?;
        let expected = required_identity(
            request.expected_file_identity,
            "OwnerGetXattrRequest missing expected_file_identity",
        )?;
        let ctx = caller_context(request.caller)?;
        let value = self.executor.getxattr(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            &expected,
            &path_os(request.name),
        )?;
        Ok(OwnerGetXattrReply { value })
    }

    fn list_xattr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerListXattrRequest,
    ) -> afs_error::Result<OwnerListXattrReply> {
        let access = presented_access(request.access)?;
        let expected = required_identity(
            request.expected_file_identity,
            "OwnerListXattrRequest missing expected_file_identity",
        )?;
        let ctx = caller_context(request.caller)?;
        let names = self.executor.listxattr(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            &expected,
        )?;
        Ok(OwnerListXattrReply { names })
    }

    fn set_xattr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerSetXattrRequest,
    ) -> afs_error::Result<OwnerSetXattrReply> {
        let access = presented_access(request.access)?;
        let expected = required_identity(
            request.expected_file_identity,
            "OwnerSetXattrRequest missing expected_file_identity",
        )?;
        let ctx = caller_context(request.caller)?;
        self.executor.setxattr(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            &expected,
            &path_os(request.name),
            &request.value,
            request.flags,
        )?;
        Ok(OwnerSetXattrReply {})
    }

    fn remove_xattr(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerRemoveXattrRequest,
    ) -> afs_error::Result<OwnerRemoveXattrReply> {
        let access = presented_access(request.access)?;
        let expected = required_identity(
            request.expected_file_identity,
            "OwnerRemoveXattrRequest missing expected_file_identity",
        )?;
        let ctx = caller_context(request.caller)?;
        self.executor.removexattr(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            &expected,
            &path_os(request.name),
        )?;
        Ok(OwnerRemoveXattrReply {})
    }

    fn unlink(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerUnlinkRequest,
    ) -> afs_error::Result<OwnerUnlinkReply> {
        let access = presented_access(request.access)?;
        let expected = request
            .expected_file_identity
            .map(|identity| FileIdentity(identity.opaque));
        let expected_parent = required_identity(
            request.expected_parent,
            "OwnerUnlinkRequest missing expected_parent",
        )?;
        let ctx = caller_context(request.caller)?;
        self.executor.unlink(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            expected.as_ref(),
            &expected_parent,
        )?;
        Ok(OwnerUnlinkReply {})
    }

    fn rmdir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerRmdirRequest,
    ) -> afs_error::Result<OwnerRmdirReply> {
        let access = presented_access(request.access)?;
        let expected = request
            .expected_file_identity
            .map(|identity| FileIdentity(identity.opaque));
        let expected_parent = required_identity(
            request.expected_parent,
            "OwnerRmdirRequest missing expected_parent",
        )?;
        let ctx = caller_context(request.caller)?;
        self.executor.rmdir(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            expected.as_ref(),
            &expected_parent,
        )?;
        Ok(OwnerRmdirReply {})
    }

    fn rename(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerRenameRequest,
    ) -> afs_error::Result<OwnerRenameReply> {
        let access = presented_access(request.access)?;
        let expected_old = request
            .expected_old_identity
            .map(|identity| FileIdentity(identity.opaque));
        let expected_new = request
            .expected_new_identity
            .map(|identity| FileIdentity(identity.opaque));
        let expected_old_parent = required_identity(
            request.expected_old_parent,
            "OwnerRenameRequest missing expected_old_parent",
        )?;
        let expected_new_parent = required_identity(
            request.expected_new_parent,
            "OwnerRenameRequest missing expected_new_parent",
        )?;
        let ctx = caller_context(request.caller)?;
        self.executor.rename(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.old_path),
            &path_os(request.new_path),
            expected_old.as_ref(),
            expected_new.as_ref(),
            &expected_old_parent,
            &expected_new_parent,
            RenameFlags(request.flags),
        )?;
        Ok(OwnerRenameReply {})
    }

    fn open(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerOpenRequest,
    ) -> afs_error::Result<OwnerOpenReply> {
        let access = presented_access(request.access)?;
        let expected = request
            .expected_file_identity
            .map(|identity| FileIdentity(identity.opaque));
        let flags = request.flags as i32;
        let options = OpenOptions {
            kill_suidgid: request.kill_suidgid,
        };
        let disable_prefetch = request.disable_prefetch;
        let (file, attributes, mut prefetched_data) = self.executor.open_with_options(
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            flags,
            expected.as_ref(),
            options,
        )?;
        if disable_prefetch {
            prefetched_data = None;
        }
        Ok(OwnerOpenReply {
            handle: Some(afs_protocol::node_data::OwnerHandle {
                opaque: file.handle,
            }),
            file_identity: Some(afs_protocol::node_data::FileIdentity {
                opaque: file.identity.0.clone(),
            }),
            owner_session_id: file.owner_session_id,
            attr: Some(owner_attr(file.identity, attributes)),
            prefetched_data,
        })
    }

    fn read(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerReadRequest,
    ) -> afs_error::Result<OwnerReadReply> {
        let access = presented_access(request.access)?;
        let handle = required_handle(request.handle, "OwnerReadRequest missing handle")?;
        let mut out = vec![0_u8; request.length as usize];
        let read = self.executor.read(
            authenticated_peer_node_id,
            &access,
            &remote_file_for_handle(&access, handle.opaque),
            request.offset,
            &mut out,
        )?;
        out.truncate(read);
        Ok(OwnerReadReply {
            data: out.into(),
            read: read as u32,
            eof: read < request.length as usize,
            data_checksum: Vec::new(),
        })
    }

    fn write(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerWriteRequest,
    ) -> afs_error::Result<OwnerWriteReply> {
        let access = presented_access(request.access)?;
        let handle = required_handle(request.handle, "OwnerWriteRequest missing handle")?;
        let length = request.length as usize;
        if request.data.len() != length {
            return Err(protocol_error(
                "OwnerWriteRequest inline data length mismatch",
            ));
        }
        let options = WriteOptions {
            kill_suidgid: request.kill_suidgid,
        };
        let written = self.executor.write_with_options(
            authenticated_peer_node_id,
            &access,
            &remote_file_for_handle(&access, handle.opaque),
            request.offset,
            &request.data[..length],
            options,
        )?;
        Ok(OwnerWriteReply {
            written: written as u32,
        })
    }

    fn authorize_data_write(
        &self,
        authenticated_peer_node_id: &str,
        access: &PresentedRootAccess,
        file: &RemoteFile,
    ) -> afs_error::Result<()> {
        self.executor
            .authorize_data_write(authenticated_peer_node_id, access, file)
    }

    fn flush(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerFlushRequest,
    ) -> afs_error::Result<OwnerFlushReply> {
        let access = presented_access(request.access)?;
        let handle = required_handle(request.handle, "OwnerFlushRequest missing handle")?;
        self.executor.flush(
            authenticated_peer_node_id,
            &access,
            &remote_file_for_handle(&access, handle.opaque),
        )?;
        Ok(OwnerFlushReply {})
    }

    fn fsync(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerFsyncRequest,
    ) -> afs_error::Result<OwnerFsyncReply> {
        let access = presented_access(request.access)?;
        let handle = required_handle(request.handle, "OwnerFsyncRequest missing handle")?;
        self.executor.fsync(
            authenticated_peer_node_id,
            &access,
            &remote_file_for_handle(&access, handle.opaque),
            request.datasync,
        )?;
        Ok(OwnerFsyncReply {})
    }

    fn release(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerReleaseRequest,
    ) -> afs_error::Result<OwnerReleaseReply> {
        let access = presented_access(request.access)?;
        let handle = required_handle(request.handle, "OwnerReleaseRequest missing handle")?;
        self.executor.release(
            authenticated_peer_node_id,
            &access,
            remote_file_for_handle(&access, handle.opaque),
        )?;
        Ok(OwnerReleaseReply {})
    }

    fn readlink(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerReadlinkRequest,
    ) -> afs_error::Result<OwnerReadlinkReply> {
        let access = presented_access(request.access)?;
        let expected = request
            .expected_file_identity
            .map(|identity| FileIdentity(identity.opaque));
        let target = self.executor.readlink(
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            expected.as_ref(),
        )?;
        Ok(OwnerReadlinkReply { target })
    }

    fn opendir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerOpendirRequest,
    ) -> afs_error::Result<OwnerOpendirReply> {
        let access = presented_access(request.access)?;
        let expected = request
            .expected_file_identity
            .map(|identity| FileIdentity(identity.opaque));
        let directory = self.executor.opendir(
            authenticated_peer_node_id,
            &access,
            &path_os(request.path),
            expected.as_ref(),
        )?;
        Ok(OwnerOpendirReply {
            handle: Some(OwnerDirectoryHandle {
                opaque: directory.handle,
            }),
            file_identity: Some(afs_protocol::node_data::FileIdentity {
                opaque: directory.identity.0,
            }),
            owner_session_id: directory.owner_session_id,
        })
    }

    fn readdir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerReaddirRequest,
    ) -> afs_error::Result<OwnerReaddirReply> {
        let access = presented_access(request.access)?;
        let max_entries = request.max_entries as usize;
        let handle = required_directory_handle(
            request.handle,
            "OwnerReaddirRequest missing directory handle",
        )?;
        let entries = self.executor.readdir(
            authenticated_peer_node_id,
            &access,
            &remote_directory_for_handle(&access, handle.opaque),
            request.offset,
            max_entries,
        )?;
        let eof = entries.len() < max_entries;
        Ok(OwnerReaddirReply {
            entries: entries
                .into_iter()
                .map(|entry| OwnerDirEntry {
                    name: entry.name.into_vec(),
                    attr: Some(owner_attr(entry.entry.identity, entry.entry.attributes)),
                    next_offset: entry.next_cookie,
                })
                .collect(),
            eof,
        })
    }

    fn fsync_dir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerFsyncDirRequest,
    ) -> afs_error::Result<OwnerFsyncDirReply> {
        let access = presented_access(request.access)?;
        let handle = required_directory_handle(
            request.handle,
            "OwnerFsyncDirRequest missing directory handle",
        )?;
        self.executor.fsyncdir(
            authenticated_peer_node_id,
            &access,
            &remote_directory_for_handle(&access, handle.opaque),
            request.datasync,
        )?;
        Ok(OwnerFsyncDirReply {})
    }

    fn release_dir(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerReleaseDirRequest,
    ) -> afs_error::Result<OwnerReleaseDirReply> {
        let access = presented_access(request.access)?;
        let handle = required_directory_handle(
            request.handle,
            "OwnerReleaseDirRequest missing directory handle",
        )?;
        self.executor.releasedir(
            authenticated_peer_node_id,
            &access,
            remote_directory_for_handle(&access, handle.opaque),
        )?;
        Ok(OwnerReleaseDirReply {})
    }

    fn symlink(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerSymlinkRequest,
    ) -> afs_error::Result<OwnerSymlinkReply> {
        let access = presented_access(request.access)?;
        let expected_parent = required_identity(
            request.expected_parent,
            "OwnerSymlinkRequest missing expected_parent",
        )?;
        let ctx = caller_context(request.caller)?;
        let entry = self.executor.symlink(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.link_path),
            &path_os(request.target),
            &expected_parent,
        )?;
        Ok(OwnerSymlinkReply {
            attr: Some(owner_attr(entry.identity, entry.attributes)),
            owner_session_id: access.home_session_id,
        })
    }

    fn link(
        &self,
        authenticated_peer_node_id: &str,
        request: OwnerLinkRequest,
    ) -> afs_error::Result<OwnerLinkReply> {
        let access = presented_access(request.access)?;
        let expected_old = required_identity(
            request.expected_old_identity,
            "OwnerLinkRequest missing expected_old_identity",
        )?;
        let expected_new_parent = required_identity(
            request.expected_new_parent,
            "OwnerLinkRequest missing expected_new_parent",
        )?;
        let ctx = caller_context(request.caller)?;
        let entry = self.executor.link(
            &ctx,
            authenticated_peer_node_id,
            &access,
            &path_os(request.old_path),
            &path_os(request.new_path),
            &expected_old,
            &expected_new_parent,
        )?;
        Ok(OwnerLinkReply {
            attr: Some(owner_attr(entry.identity, entry.attributes)),
            owner_session_id: access.home_session_id,
        })
    }
}

#[cfg(feature = "ownerfs")]
pub(super) fn presented_access(
    access: Option<afs_protocol::node_data::RootAccess>,
) -> afs_error::Result<PresentedRootAccess> {
    let access = access.ok_or_else(|| protocol_error("OwnerFiles request missing RootAccess"))?;
    Ok(PresentedRootAccess {
        id: RootId(access.root_id),
        epoch: access.root_epoch,
        home_node_id: access.home_node_id,
        home_session_id: access.home_session_id,
        holder_node_id: access.holder_node_id,
        session_id: access.session_id,
        access_generation: access.access_generation,
        fencing_token: access.fencing_token,
    })
}

#[cfg(feature = "ownerfs")]
fn path_os(path: Vec<u8>) -> OsString {
    OsString::from_vec(path)
}

#[cfg(feature = "ownerfs")]
fn caller_context(
    caller: Option<afs_protocol::node_data::OwnerCaller>,
) -> afs_error::Result<RequestContext> {
    let caller = caller.ok_or_else(|| protocol_error("OwnerFiles request missing caller"))?;
    Ok(RequestContext {
        uid: caller.uid,
        gid: caller.gid,
        pid: caller.pid,
        umask: caller.umask,
        supplementary_gids: caller.supplementary_gids,
    })
}

#[cfg(feature = "ownerfs")]
fn required_handle(
    handle: Option<afs_protocol::node_data::OwnerHandle>,
    message: &'static str,
) -> afs_error::Result<afs_protocol::node_data::OwnerHandle> {
    handle.ok_or_else(|| protocol_error(message))
}

#[cfg(feature = "ownerfs")]
fn required_directory_handle(
    handle: Option<afs_protocol::node_data::OwnerDirectoryHandle>,
    message: &'static str,
) -> afs_error::Result<afs_protocol::node_data::OwnerDirectoryHandle> {
    handle.ok_or_else(|| protocol_error(message))
}

#[cfg(feature = "ownerfs")]
fn required_identity(
    identity: Option<afs_protocol::node_data::FileIdentity>,
    message: &'static str,
) -> afs_error::Result<FileIdentity> {
    identity
        .map(|identity| FileIdentity(identity.opaque))
        .ok_or_else(|| protocol_error(message))
}

#[cfg(feature = "ownerfs")]
fn attribute_change(attr: afs_protocol::node_data::OwnerSetAttr) -> AttributeChange {
    AttributeChange {
        size: attr.size,
        mode: attr.mode,
        uid: attr.uid,
        gid: attr.gid,
        atime: attr.atime_ns.map(ns_to_time),
        mtime: attr.mtime_ns.map(ns_to_time),
    }
}

#[cfg(feature = "ownerfs")]
pub(super) fn remote_file_for_handle(access: &PresentedRootAccess, handle: Vec<u8>) -> RemoteFile {
    RemoteFile {
        root_id: access.id.clone(),
        owner_node_id: access.home_node_id.clone(),
        owner_session_id: access.home_session_id.clone(),
        identity: FileIdentity(Vec::new()),
        handle,
    }
}

#[cfg(feature = "ownerfs")]
fn remote_directory_for_handle(access: &PresentedRootAccess, handle: Vec<u8>) -> RemoteDirectory {
    RemoteDirectory {
        root_id: access.id.clone(),
        owner_node_id: access.home_node_id.clone(),
        owner_session_id: access.home_session_id.clone(),
        identity: FileIdentity(Vec::new()),
        handle,
    }
}

#[cfg(feature = "ownerfs")]
fn ns_to_time(ns: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_nanos(ns)
}

#[cfg(feature = "ownerfs")]
fn owner_attr(
    identity: FileIdentity,
    attributes: FileAttributes,
) -> afs_protocol::node_data::OwnerFileAttr {
    let (kind, special_node) = owner_kind(attributes.kind);
    afs_protocol::node_data::OwnerFileAttr {
        identity: Some(afs_protocol::node_data::FileIdentity { opaque: identity.0 }),
        kind: kind.into(),
        mode: attributes.mode,
        uid: attributes.uid,
        gid: attributes.gid,
        size: attributes.size,
        blocks: attributes.blocks,
        atime_ns: time_ns(attributes.atime),
        mtime_ns: time_ns(attributes.mtime),
        ctime_ns: time_ns(attributes.ctime),
        nlink: attributes.nlink,
        blksize: 4096,
        special_node,
    }
}

#[cfg(feature = "ownerfs")]
fn owner_capacity(
    capacity: FilesystemCapacity,
) -> afs_protocol::node_data::OwnerFilesystemCapacity {
    afs_protocol::node_data::OwnerFilesystemCapacity {
        blocks: capacity.blocks,
        bfree: capacity.bfree,
        bavail: capacity.bavail,
        files: capacity.files,
        ffree: capacity.ffree,
        bsize: capacity.bsize,
        namelen: capacity.namelen,
        frsize: capacity.frsize,
    }
}

#[cfg(feature = "ownerfs")]
fn owner_kind(
    kind: FileKind,
) -> (
    afs_protocol::node_data::OwnerFileKind,
    Option<afs_protocol::node_data::OwnerSpecialNode>,
) {
    match kind {
        FileKind::Regular => (afs_protocol::node_data::OwnerFileKind::Regular, None),
        FileKind::Directory => (afs_protocol::node_data::OwnerFileKind::Directory, None),
        FileKind::Symlink => (afs_protocol::node_data::OwnerFileKind::Symlink, None),
        FileKind::Special(kind) => {
            let special = wire_owner_special_node(kind);
            (
                afs_protocol::node_data::OwnerFileKind::try_from(special.kind)
                    .unwrap_or(afs_protocol::node_data::OwnerFileKind::Unspecified),
                Some(special),
            )
        }
    }
}

#[cfg(feature = "ownerfs")]
fn wire_owner_special_node(kind: SpecialFileKind) -> afs_protocol::node_data::OwnerSpecialNode {
    let (kind, rdev) = match kind {
        SpecialFileKind::Fifo => (afs_protocol::node_data::OwnerFileKind::Fifo, 0),
        SpecialFileKind::Socket => (afs_protocol::node_data::OwnerFileKind::Socket, 0),
        SpecialFileKind::BlockDevice { rdev } => {
            (afs_protocol::node_data::OwnerFileKind::BlockDevice, rdev)
        }
        SpecialFileKind::CharDevice { rdev } => {
            (afs_protocol::node_data::OwnerFileKind::CharDevice, rdev)
        }
    };
    afs_protocol::node_data::OwnerSpecialNode {
        kind: kind.into(),
        rdev,
    }
}

#[cfg(feature = "ownerfs")]
fn owner_special_kind(
    special: Option<afs_protocol::node_data::OwnerSpecialNode>,
) -> afs_error::Result<SpecialFileKind> {
    let special = special.ok_or_else(|| {
        afs_error::Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "OwnerMknodRequest missing special_node",
        )
    })?;
    match afs_protocol::node_data::OwnerFileKind::try_from(special.kind) {
        Ok(afs_protocol::node_data::OwnerFileKind::Fifo) if special.rdev == 0 => {
            Ok(SpecialFileKind::Fifo)
        }
        Ok(afs_protocol::node_data::OwnerFileKind::Socket) if special.rdev == 0 => {
            Ok(SpecialFileKind::Socket)
        }
        Ok(afs_protocol::node_data::OwnerFileKind::BlockDevice) => {
            Ok(SpecialFileKind::BlockDevice { rdev: special.rdev })
        }
        Ok(afs_protocol::node_data::OwnerFileKind::CharDevice) => {
            Ok(SpecialFileKind::CharDevice { rdev: special.rdev })
        }
        _ => Err(afs_error::Error::coded(
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            "OwnerSpecialNode has an invalid kind/rdev combination",
        )),
    }
}

#[cfg(feature = "ownerfs")]
fn time_ns(time: std::time::SystemTime) -> u64 {
    time.duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(feature = "ownerfs")]
fn protocol_error(message: &'static str) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_PROTOCOL_VIOLATION, message)
}

/// Explicit peer policy. Internal services take caller context from the request;
/// authenticated boundary adapters can require a verified transport identity.
pub trait PeerAuthenticator: Send + Sync + 'static {
    fn trusts_internal_requests(&self) -> bool {
        false
    }

    fn authenticate(
        &self,
        metadata: &MetadataMap,
        remote_addr: Option<SocketAddr>,
        peer_cert_der: Option<&[u8]>,
    ) -> afs_error::Result<String>;
}

/// AFS service-to-service caller context is already inside the trust boundary.
/// This policy does not authenticate external clients or manufacture a Node ID.
#[derive(Clone, Copy, Debug)]
pub struct InternalPeerAuthenticator;

impl PeerAuthenticator for InternalPeerAuthenticator {
    fn trusts_internal_requests(&self) -> bool {
        true
    }

    fn authenticate(
        &self,
        _metadata: &MetadataMap,
        _remote_addr: Option<SocketAddr>,
        _peer_cert_der: Option<&[u8]>,
    ) -> afs_error::Result<String> {
        Err(afs_error::Error::coded(
            afs_error::CLIENT_ARGUMENT_INVALID,
            "internal caller context must be read from the request",
        ))
    }
}

/// Node-to-node mTLS identity checker backed by an exact certificate allow-list.
///
/// Explicit authenticated adapters avoid parsing X.509 names: their caller
/// provides the verified peer certificate DER for each node id, tonic
/// verifies the TLS client certificate chain, and this authenticator binds the
/// presented leaf certificate bytes to one configured node id. If no certificate
/// is present, or if the bytes are unknown, OwnerFiles fails closed before the
/// request body is inspected.
#[derive(Clone, Debug)]
pub struct MtlsPeerAuthenticator {
    node_id_by_cert_der: Arc<HashMap<Vec<u8>, String>>,
}

impl MtlsPeerAuthenticator {
    pub fn new<I>(trusted_peer_certs: I) -> afs_error::Result<Self>
    where
        I: IntoIterator<Item = (String, Vec<u8>)>,
    {
        let mut node_id_by_cert_der = HashMap::new();
        for (node_id, cert_der) in trusted_peer_certs {
            if node_id.is_empty() {
                return Err(afs_error::Error::coded(
                    afs_error::CLIENT_ARGUMENT_INVALID,
                    "trusted peer certificate node_id is empty",
                ));
            }
            if cert_der.is_empty() {
                return Err(afs_error::Error::coded(
                    afs_error::CLIENT_ARGUMENT_INVALID,
                    format!("trusted peer certificate for {node_id} is empty"),
                ));
            }
            if let Some(previous) = node_id_by_cert_der.insert(cert_der, node_id.clone()) {
                return Err(afs_error::Error::coded(
                    afs_error::CLIENT_ARGUMENT_INVALID,
                    format!(
                        "the same peer certificate is configured for both {previous} and {node_id}"
                    ),
                ));
            }
        }
        Ok(Self {
            node_id_by_cert_der: Arc::new(node_id_by_cert_der),
        })
    }
}

impl PeerAuthenticator for MtlsPeerAuthenticator {
    fn authenticate(
        &self,
        metadata: &MetadataMap,
        remote_addr: Option<SocketAddr>,
        peer_cert_der: Option<&[u8]>,
    ) -> afs_error::Result<String> {
        let _ = (metadata, remote_addr);
        let Some(peer_cert_der) = peer_cert_der else {
            return Err(afs_error::Error::coded(
                afs_error::NODE_OWNER_INVALID_GRANT,
                "OwnerFiles peer connection has no verified mTLS client certificate",
            ));
        };
        self.node_id_by_cert_der
            .get(peer_cert_der)
            .cloned()
            .ok_or_else(|| {
                afs_error::Error::coded(
                    afs_error::NODE_OWNER_INVALID_GRANT,
                    "OwnerFiles peer certificate is not trusted for any configured node id",
                )
            })
    }
}

/// OwnerFs 远端文件数据面入口。
///
/// 无 handler 的默认实例继续返回 UNIMPLEMENTED，避免把 diagnostics 的 Storage
/// 成功误当成 workspace 文件成功。有 handler 时也必须显式选择 peer policy；
/// 默认未接线的服务不接受请求。内部策略不为外部调用者提供鉴权。
#[derive(Clone, Default)]
#[cfg(feature = "ownerfs")]
pub struct OwnerFilesService {
    handler: Option<Arc<dyn OwnerFilesHandler>>,
    authenticator: Option<Arc<dyn PeerAuthenticator>>,
    metrics: Option<super::OwnerRpcMetrics>,
    rdma_sessions: Option<RdmaSessionRegistry>,
}

#[cfg(feature = "ownerfs")]
impl OwnerFilesService {
    #[must_use]
    pub fn new(
        handler: Arc<dyn OwnerFilesHandler>,
        authenticator: Arc<dyn PeerAuthenticator>,
    ) -> Self {
        Self {
            handler: Some(handler),
            authenticator: Some(authenticator),
            metrics: None,
            rdma_sessions: None,
        }
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: super::OwnerRpcMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    #[must_use]
    pub fn with_rdma_registry(mut self, sessions: RdmaSessionRegistry) -> Self {
        self.rdma_sessions = Some(sessions);
        self
    }

    #[must_use]
    pub fn with_rdma_sessions(self, sessions: RdmaSessionRegistry) -> Self {
        self.with_rdma_registry(sessions)
    }

    async fn dispatch<Req, Reply, F>(
        &self,
        request: Request<Req>,
        operation: &'static str,
        call: F,
    ) -> Result<Response<Reply>, Status>
    where
        Req: InternalPeerRequest + Send + 'static,
        Reply: Send + 'static,
        F: FnOnce(Arc<dyn OwnerFilesHandler>, String, Req) -> afs_error::Result<Reply>
            + Send
            + 'static,
    {
        let Some(handler) = self.handler.clone() else {
            return Err(owner_files_unimplemented(operation));
        };
        let Some(authenticator) = self.authenticator.clone() else {
            return Err(coded_status(
                afs_error::NODE_OWNER_INVALID_GRANT,
                format!("{operation} has no authenticated peer identity source"),
            ));
        };
        let started = std::time::Instant::now();
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        let work = move || call(handler, peer, request);
        // Production Node uses a multi-thread runtime. Run the short local
        // file action on this worker and let Tokio hand other tasks off while
        // it blocks, avoiding a blocking-pool scheduling hop per file RPC.
        // Current-thread test runtimes still need spawn_blocking.
        let result = if tokio::runtime::Handle::current().runtime_flavor()
            == tokio::runtime::RuntimeFlavor::MultiThread
        {
            Ok(tokio::task::block_in_place(work))
        } else {
            tokio::task::spawn_blocking(work).await
        };
        if let Some(metrics) = &self.metrics {
            metrics.observe("server", operation, started.elapsed());
        }
        let reply = result
            .map_err(|error| {
                coded_status(
                    afs_error::CLIENT_WORKER_FAILED,
                    format!("{operation} blocking worker failed: {error}"),
                )
            })?
            .map_err(error_to_status)?;
        Ok(Response::new(reply))
    }

    fn require_handler(
        &self,
        operation: &'static str,
    ) -> Result<Arc<dyn OwnerFilesHandler>, Status> {
        self.handler
            .clone()
            .ok_or_else(|| owner_files_unimplemented(operation))
    }

    fn require_authenticator(
        &self,
        operation: &'static str,
    ) -> Result<Arc<dyn PeerAuthenticator>, Status> {
        self.authenticator.clone().ok_or_else(|| {
            coded_status(
                afs_error::NODE_OWNER_INVALID_GRANT,
                format!("{operation} has no authenticated peer identity source"),
            )
        })
    }

    fn owner_peer<T: InternalPeerRequest>(
        &self,
        operation: &'static str,
        request: &Request<T>,
    ) -> Result<String, Status> {
        authenticate_peer(self.require_authenticator(operation)?.as_ref(), request)
    }

    fn rdma_sessions(&self, operation: &'static str) -> Result<RdmaSessionRegistry, Status> {
        self.rdma_sessions.clone().ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                format!("{operation} has no OwnerFs RDMA session registry"),
            )
        })
    }
}

#[cfg(feature = "ownerfs")]
fn owner_data_plane(plane: Option<DataPlane>) -> Result<DataPlane, Status> {
    let plane = plane.unwrap_or(DataPlane {
        transfer: DataTransfer::GrpcInline.into(),
        rdma_session_id: 0,
        buffer_offset: 0,
    });
    let transfer = transfer_mode(plane.transfer)?;
    if transfer == DataTransfer::Unspecified {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "OwnerFiles data plane transfer is required",
        ));
    }
    match transfer {
        DataTransfer::GrpcInline => {
            if plane.rdma_session_id != 0 || plane.buffer_offset != 0 {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "OwnerFiles inline plane must not carry an RDMA descriptor",
                ));
            }
        }
        DataTransfer::RdmaOneSided => {
            if plane.rdma_session_id == 0 || plane.buffer_offset != 0 {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "OwnerFiles RDMA requires a nonzero session and zero buffer offset",
                ));
            }
        }
        DataTransfer::Unspecified => unreachable!("checked above"),
    }
    Ok(plane)
}

#[cfg(feature = "ownerfs")]
fn checksum_blake3(data: &[u8]) -> Vec<u8> {
    blake3::hash(data).as_bytes().to_vec()
}

#[cfg(feature = "ownerfs")]
fn verify_owner_checksum(data: &[u8], expected: &[u8]) -> Result<(), Status> {
    if expected.len() != 32 {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "OwnerFiles RDMA write requires a 32-byte data checksum",
        ));
    }
    if checksum_blake3(data) != expected {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_CORRUPT_DATA,
            "OwnerFiles RDMA payload checksum mismatch",
        ));
    }
    Ok(())
}

#[cfg(feature = "ownerfs")]
struct OwnerServerRdmaGuard {
    session: Arc<super::control::RdmaSession>,
    disarmed: bool,
}

#[cfg(feature = "ownerfs")]
impl OwnerServerRdmaGuard {
    fn new(session: Arc<super::control::RdmaSession>) -> Self {
        Self {
            session,
            disarmed: false,
        }
    }

    fn disarm(mut self) {
        self.disarmed = true;
    }
}

#[cfg(feature = "ownerfs")]
impl Drop for OwnerServerRdmaGuard {
    fn drop(&mut self) {
        if !self.disarmed {
            self.session.poisoned.store(true, Ordering::SeqCst);
        }
    }
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
async fn owner_rdma_session(
    sessions: &RdmaSessionRegistry,
    peer: String,
    access: &afs_protocol::node_data::RootAccess,
    session_id: u64,
) -> Result<Arc<super::control::RdmaSession>, Status> {
    let identity = super::control::owner_rdma_identity_from_wire(peer, access.clone())?;
    sessions.session_for(session_id, &identity).await
}

#[cfg(all(feature = "ownerfs", not(feature = "rdma")))]
async fn owner_rdma_session(
    _sessions: &RdmaSessionRegistry,
    _peer: String,
    _access: &afs_protocol::node_data::RootAccess,
    _session_id: u64,
) -> Result<Arc<super::control::RdmaSession>, Status> {
    Err(coded_status(
        afs_error::NODE_TRANSFER_UNSUPPORTED,
        "RDMA feature is not enabled",
    ))
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
async fn owner_rdma_write_to_client(
    session: Arc<super::control::RdmaSession>,
    data: prost::bytes::Bytes,
) -> Result<(), Status> {
    let worker = session.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut endpoint = worker.endpoint.blocking_lock();
        let guard = OwnerServerRdmaGuard::new(worker.clone());
        if worker.poisoned.load(Ordering::SeqCst) {
            return Err(coded_status(
                afs_error::NODE_RDMA_SESSION_POISONED,
                "OwnerFiles RDMA session is poisoned",
            ));
        }
        if data.len() > endpoint.capacity() {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFiles RDMA read exceeds endpoint capacity",
            ));
        }
        if let Err(error) = endpoint
            .put_local(&data)
            .and_then(|_| endpoint.transfer_write(data.len()))
        {
            worker.poisoned.store(true, Ordering::SeqCst);
            return Err(rdma_status(error));
        }
        guard.disarm();
        Ok(())
    })
    .await
    .map_err(|error| coded_status(afs_error::NODE_TRANSFER_INTERNAL, error.to_string()))?;
    if result.is_err() {
        session.poisoned.store(true, Ordering::SeqCst);
    }
    result
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
async fn owner_rdma_capacity(session: Arc<super::control::RdmaSession>) -> Result<usize, Status> {
    let worker = session.clone();
    tokio::task::spawn_blocking(move || {
        let endpoint = worker.endpoint.blocking_lock();
        if worker.poisoned.load(Ordering::SeqCst) {
            return Err(coded_status(
                afs_error::NODE_RDMA_SESSION_POISONED,
                "OwnerFiles RDMA session is poisoned",
            ));
        }
        Ok(endpoint.capacity())
    })
    .await
    .map_err(|error| coded_status(afs_error::NODE_TRANSFER_INTERNAL, error.to_string()))?
}

#[cfg(all(feature = "ownerfs", not(feature = "rdma")))]
async fn owner_rdma_capacity(_session: Arc<super::control::RdmaSession>) -> Result<usize, Status> {
    Err(coded_status(
        afs_error::NODE_TRANSFER_UNSUPPORTED,
        "RDMA feature is not enabled",
    ))
}

#[cfg(all(feature = "ownerfs", not(feature = "rdma")))]
async fn owner_rdma_write_to_client(
    _session: Arc<super::control::RdmaSession>,
    _data: prost::bytes::Bytes,
) -> Result<(), Status> {
    Err(coded_status(
        afs_error::NODE_TRANSFER_UNSUPPORTED,
        "RDMA feature is not enabled",
    ))
}

#[cfg(all(feature = "ownerfs", feature = "rdma"))]
async fn owner_rdma_write_from_client(
    session: Arc<super::control::RdmaSession>,
    handler: Arc<dyn OwnerFilesHandler>,
    peer: String,
    mut request: OwnerWriteRequest,
) -> Result<OwnerWriteReply, Status> {
    let worker = session.clone();
    let result = tokio::task::spawn_blocking(move || {
        let len = request.length as usize;
        let mut endpoint = worker.endpoint.blocking_lock();
        let guard = OwnerServerRdmaGuard::new(worker.clone());
        if worker.poisoned.load(Ordering::SeqCst) {
            return Err(coded_status(
                afs_error::NODE_RDMA_SESSION_POISONED,
                "OwnerFiles RDMA session is poisoned",
            ));
        }
        if len > endpoint.capacity() {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFiles RDMA write exceeds endpoint capacity",
            ));
        }
        let data = match endpoint
            .transfer_read(len)
            .and_then(|_| endpoint.get_local(len))
        {
            Ok(data) => data,
            Err(error) => {
                worker.poisoned.store(true, Ordering::SeqCst);
                return Err(rdma_status(error));
            }
        };
        if let Err(error) = verify_owner_checksum(&data, &request.data_checksum) {
            worker.poisoned.store(true, Ordering::SeqCst);
            return Err(error);
        }
        let request_len = request.length;
        request.data = data;
        request.plane = Some(DataPlane {
            transfer: DataTransfer::GrpcInline.into(),
            rdma_session_id: 0,
            buffer_offset: 0,
        });
        let reply = match handler.write(&peer, request).map_err(error_to_status) {
            Ok(reply) => reply,
            Err(error) => {
                worker.poisoned.store(true, Ordering::SeqCst);
                return Err(error);
            }
        };
        if reply.written > request_len {
            worker.poisoned.store(true, Ordering::SeqCst);
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFiles RDMA write reply count exceeds request length",
            ));
        }
        guard.disarm();
        Ok(reply)
    })
    .await
    .map_err(|error| coded_status(afs_error::NODE_TRANSFER_INTERNAL, error.to_string()))?;
    if result.is_err() {
        session.poisoned.store(true, Ordering::SeqCst);
    }
    result
}

#[cfg(all(feature = "ownerfs", not(feature = "rdma")))]
async fn owner_rdma_write_from_client(
    _session: Arc<super::control::RdmaSession>,
    _handler: Arc<dyn OwnerFilesHandler>,
    _peer: String,
    _request: OwnerWriteRequest,
) -> Result<OwnerWriteReply, Status> {
    Err(coded_status(
        afs_error::NODE_TRANSFER_UNSUPPORTED,
        "RDMA feature is not enabled",
    ))
}

#[must_use]
#[cfg(feature = "ownerfs")]
pub fn make_owner_files_server() -> OwnerFilesServer<OwnerFilesService> {
    OwnerFilesServer::new(OwnerFilesService::default())
}

#[must_use]
#[cfg(feature = "ownerfs")]
pub fn make_owner_files_server_with_handler(
    handler: Arc<dyn OwnerFilesHandler>,
    authenticator: Arc<dyn PeerAuthenticator>,
) -> OwnerFilesServer<OwnerFilesService> {
    OwnerFilesServer::new(OwnerFilesService::new(handler, authenticator))
}

#[must_use]
#[cfg(feature = "ownerfs")]
pub fn make_owner_files_server_with_handler_and_metrics(
    handler: Arc<dyn OwnerFilesHandler>,
    authenticator: Arc<dyn PeerAuthenticator>,
    metrics: super::OwnerRpcMetrics,
) -> OwnerFilesServer<OwnerFilesService> {
    OwnerFilesServer::new(OwnerFilesService::new(handler, authenticator).with_metrics(metrics))
}

#[must_use]
#[cfg(feature = "ownerfs")]
pub fn make_owner_files_server_with_handler_and_transport(
    handler: Arc<dyn OwnerFilesHandler>,
    authenticator: Arc<dyn PeerAuthenticator>,
    rdma_sessions: RdmaSessionRegistry,
) -> OwnerFilesServer<OwnerFilesService> {
    OwnerFilesServer::new(
        OwnerFilesService::new(handler, authenticator).with_rdma_registry(rdma_sessions),
    )
}

#[must_use]
#[cfg(feature = "ownerfs")]
pub fn make_owner_files_server_with_handler_metrics_and_rdma(
    handler: Arc<dyn OwnerFilesHandler>,
    authenticator: Arc<dyn PeerAuthenticator>,
    metrics: super::OwnerRpcMetrics,
    rdma_sessions: RdmaSessionRegistry,
) -> OwnerFilesServer<OwnerFilesService> {
    OwnerFilesServer::new(
        OwnerFilesService::new(handler, authenticator)
            .with_metrics(metrics)
            .with_rdma_sessions(rdma_sessions),
    )
}

#[tonic::async_trait]
#[cfg(feature = "ownerfs")]
impl OwnerFiles for OwnerFilesService {
    async fn lookup(
        &self,
        request: Request<OwnerLookupRequest>,
    ) -> Result<Response<OwnerLookupReply>, Status> {
        self.dispatch(request, "OwnerFiles.Lookup", |handler, peer, request| {
            handler.lookup(&peer, request)
        })
        .await
    }

    async fn get_attr(
        &self,
        request: Request<OwnerGetAttrRequest>,
    ) -> Result<Response<OwnerGetAttrReply>, Status> {
        self.dispatch(request, "OwnerFiles.GetAttr", |handler, peer, request| {
            handler.get_attr(&peer, request)
        })
        .await
    }

    async fn stat_fs(
        &self,
        request: Request<OwnerStatFsRequest>,
    ) -> Result<Response<OwnerStatFsReply>, Status> {
        self.dispatch(request, "OwnerFiles.StatFs", |handler, peer, request| {
            handler.statfs(&peer, request)
        })
        .await
    }

    async fn set_attr(
        &self,
        request: Request<OwnerSetAttrRequest>,
    ) -> Result<Response<OwnerSetAttrReply>, Status> {
        self.dispatch(request, "OwnerFiles.SetAttr", |handler, peer, request| {
            handler.set_attr(&peer, request)
        })
        .await
    }

    async fn get_xattr(
        &self,
        request: Request<OwnerGetXattrRequest>,
    ) -> Result<Response<OwnerGetXattrReply>, Status> {
        self.dispatch(request, "OwnerFiles.GetXattr", |handler, peer, request| {
            handler.get_xattr(&peer, request)
        })
        .await
    }

    async fn list_xattr(
        &self,
        request: Request<OwnerListXattrRequest>,
    ) -> Result<Response<OwnerListXattrReply>, Status> {
        self.dispatch(request, "OwnerFiles.ListXattr", |handler, peer, request| {
            handler.list_xattr(&peer, request)
        })
        .await
    }

    async fn set_xattr(
        &self,
        request: Request<OwnerSetXattrRequest>,
    ) -> Result<Response<OwnerSetXattrReply>, Status> {
        self.dispatch(request, "OwnerFiles.SetXattr", |handler, peer, request| {
            handler.set_xattr(&peer, request)
        })
        .await
    }

    async fn remove_xattr(
        &self,
        request: Request<OwnerRemoveXattrRequest>,
    ) -> Result<Response<OwnerRemoveXattrReply>, Status> {
        self.dispatch(
            request,
            "OwnerFiles.RemoveXattr",
            |handler, peer, request| handler.remove_xattr(&peer, request),
        )
        .await
    }

    async fn create(
        &self,
        request: Request<OwnerCreateRequest>,
    ) -> Result<Response<OwnerCreateReply>, Status> {
        self.dispatch(request, "OwnerFiles.Create", |handler, peer, request| {
            handler.create(&peer, request)
        })
        .await
    }

    async fn mkdir(
        &self,
        request: Request<OwnerMkdirRequest>,
    ) -> Result<Response<OwnerMkdirReply>, Status> {
        self.dispatch(request, "OwnerFiles.Mkdir", |handler, peer, request| {
            handler.mkdir(&peer, request)
        })
        .await
    }

    async fn mknod(
        &self,
        request: Request<OwnerMknodRequest>,
    ) -> Result<Response<OwnerMknodReply>, Status> {
        self.dispatch(request, "OwnerFiles.Mknod", |handler, peer, request| {
            handler.mknod(&peer, request)
        })
        .await
    }

    async fn unlink(
        &self,
        request: Request<OwnerUnlinkRequest>,
    ) -> Result<Response<OwnerUnlinkReply>, Status> {
        self.dispatch(request, "OwnerFiles.Unlink", |handler, peer, request| {
            handler.unlink(&peer, request)
        })
        .await
    }

    async fn rmdir(
        &self,
        request: Request<OwnerRmdirRequest>,
    ) -> Result<Response<OwnerRmdirReply>, Status> {
        self.dispatch(request, "OwnerFiles.Rmdir", |handler, peer, request| {
            handler.rmdir(&peer, request)
        })
        .await
    }

    async fn rename(
        &self,
        request: Request<OwnerRenameRequest>,
    ) -> Result<Response<OwnerRenameReply>, Status> {
        self.dispatch(request, "OwnerFiles.Rename", |handler, peer, request| {
            handler.rename(&peer, request)
        })
        .await
    }

    async fn open(
        &self,
        request: Request<OwnerOpenRequest>,
    ) -> Result<Response<OwnerOpenReply>, Status> {
        self.dispatch(request, "OwnerFiles.Open", |handler, peer, request| {
            handler.open(&peer, request)
        })
        .await
    }

    async fn readlink(
        &self,
        request: Request<OwnerReadlinkRequest>,
    ) -> Result<Response<OwnerReadlinkReply>, Status> {
        self.dispatch(request, "OwnerFiles.Readlink", |handler, peer, request| {
            handler.readlink(&peer, request)
        })
        .await
    }

    async fn read(
        &self,
        request: Request<OwnerReadRequest>,
    ) -> Result<Response<OwnerReadReply>, Status> {
        let operation = "OwnerFiles.Read";
        let handler = self.require_handler(operation)?;
        let peer = self.owner_peer(operation, &request)?;
        let mut request = request.into_inner();
        validate_length(request.length)?;
        let plane = owner_data_plane(request.plane)?;
        let transfer = transfer_mode(plane.transfer)?;
        if transfer == DataTransfer::RdmaOneSided {
            let access = request
                .access
                .as_ref()
                .ok_or_else(|| Status::permission_denied("OwnerFiles RDMA requires RootAccess"))?
                .clone();
            let session = owner_rdma_session(
                &self.rdma_sessions(operation)?,
                peer.clone(),
                &access,
                plane.rdma_session_id,
            )
            .await?;
            let rpc_guard = OwnerServerRdmaGuard::new(session.clone());
            let capacity = owner_rdma_capacity(session.clone()).await?;
            if request.length as usize > capacity {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "OwnerFiles RDMA read exceeds endpoint capacity",
                ));
            }
            request.plane = Some(DataPlane {
                transfer: DataTransfer::GrpcInline.into(),
                rdma_session_id: 0,
                buffer_offset: 0,
            });
            let started = std::time::Instant::now();
            let reply = tokio::task::spawn_blocking(move || handler.read(&peer, request))
                .await
                .map_err(|error| {
                    coded_status(
                        afs_error::CLIENT_WORKER_FAILED,
                        format!("{operation} blocking worker failed: {error}"),
                    )
                })?
                .map_err(error_to_status)?;
            if reply.read as usize != reply.data.len() {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "OwnerFiles read reply length differs from payload",
                ));
            }
            let checksum = checksum_blake3(&reply.data);
            owner_rdma_write_to_client(session, reply.data).await?;
            if let Some(metrics) = &self.metrics {
                metrics.record_payload("server", "read", "rdma", reply.read as u64);
                metrics.observe("server", operation, started.elapsed());
            }
            rpc_guard.disarm();
            return Ok(Response::new(OwnerReadReply {
                data: prost::bytes::Bytes::new(),
                read: reply.read,
                eof: reply.eof,
                data_checksum: checksum,
            }));
        }
        let started = std::time::Instant::now();
        let reply = tokio::task::spawn_blocking(move || {
            let mut reply = handler.read(&peer, request)?;
            if reply.read as usize != reply.data.len() {
                return Err(afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_INVALID,
                    "OwnerFiles read reply length differs from payload",
                ));
            }
            reply.data_checksum = Vec::new();
            Ok(reply)
        })
        .await
        .map_err(|error| {
            coded_status(
                afs_error::CLIENT_WORKER_FAILED,
                format!("{operation} blocking worker failed: {error}"),
            )
        })?
        .map_err(error_to_status)?;
        if let Some(metrics) = &self.metrics {
            metrics.record_payload("server", "read", "grpc", reply.read as u64);
            metrics.observe("server", operation, started.elapsed());
        }
        Ok(Response::new(reply))
    }

    async fn write(
        &self,
        request: Request<OwnerWriteRequest>,
    ) -> Result<Response<OwnerWriteReply>, Status> {
        let operation = "OwnerFiles.Write";
        let handler = self.require_handler(operation)?;
        let peer = self.owner_peer(operation, &request)?;
        let request = request.into_inner();
        validate_length(request.length)?;
        let plane = owner_data_plane(request.plane)?;
        let transfer = transfer_mode(plane.transfer)?;
        if transfer == DataTransfer::RdmaOneSided {
            if !request.data.is_empty() {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "OwnerFiles RDMA write must not include inline data",
                ));
            }
            if request.data_checksum.len() != 32 {
                return Err(coded_status(
                    afs_error::NODE_TRANSFER_INVALID,
                    "OwnerFiles RDMA write requires a 32-byte data checksum",
                ));
            }
            let access = request
                .access
                .as_ref()
                .ok_or_else(|| Status::permission_denied("OwnerFiles RDMA requires RootAccess"))?
                .clone();
            let presented = presented_access(Some(access.clone())).map_err(error_to_status)?;
            let handle =
                required_handle(request.handle.clone(), "OwnerWriteRequest missing handle")
                    .map_err(error_to_status)?;
            let remote = remote_file_for_handle(&presented, handle.opaque);
            let auth_handler = handler.clone();
            let auth_peer = peer.clone();
            tokio::task::spawn_blocking(move || {
                auth_handler.authorize_data_write(&auth_peer, &presented, &remote)
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(error_to_status)?;
            let session = owner_rdma_session(
                &self.rdma_sessions(operation)?,
                peer.clone(),
                &access,
                plane.rdma_session_id,
            )
            .await?;
            let rpc_guard = OwnerServerRdmaGuard::new(session.clone());
            let started = std::time::Instant::now();
            let reply = owner_rdma_write_from_client(session, handler, peer, request).await?;
            if let Some(metrics) = &self.metrics {
                metrics.record_payload("server", "write", "rdma", reply.written as u64);
                metrics.observe("server", operation, started.elapsed());
            }
            rpc_guard.disarm();
            return Ok(Response::new(reply));
        } else if request.length as usize != request.data.len() {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFiles inline write length/data mismatch",
            ));
        } else if !request.data_checksum.is_empty() {
            verify_owner_checksum(&request.data, &request.data_checksum)?;
        }
        let request_length = request.length;
        let started = std::time::Instant::now();
        let reply = tokio::task::spawn_blocking(move || handler.write(&peer, request))
            .await
            .map_err(|error| {
                coded_status(
                    afs_error::CLIENT_WORKER_FAILED,
                    format!("{operation} blocking worker failed: {error}"),
                )
            })?
            .map_err(error_to_status)?;
        if reply.written > request_length {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_INVALID,
                "OwnerFiles inline write reply count exceeds request length",
            ));
        }
        if let Some(metrics) = &self.metrics {
            metrics.record_payload("server", "write", "grpc", reply.written as u64);
            metrics.observe("server", operation, started.elapsed());
        }
        Ok(Response::new(reply))
    }

    async fn flush(
        &self,
        request: Request<OwnerFlushRequest>,
    ) -> Result<Response<OwnerFlushReply>, Status> {
        self.dispatch(request, "OwnerFiles.Flush", |handler, peer, request| {
            handler.flush(&peer, request)
        })
        .await
    }

    async fn fsync(
        &self,
        request: Request<OwnerFsyncRequest>,
    ) -> Result<Response<OwnerFsyncReply>, Status> {
        self.dispatch(request, "OwnerFiles.Fsync", |handler, peer, request| {
            handler.fsync(&peer, request)
        })
        .await
    }

    async fn release(
        &self,
        request: Request<OwnerReleaseRequest>,
    ) -> Result<Response<OwnerReleaseReply>, Status> {
        self.dispatch(request, "OwnerFiles.Release", |handler, peer, request| {
            handler.release(&peer, request)
        })
        .await
    }

    async fn opendir(
        &self,
        request: Request<OwnerOpendirRequest>,
    ) -> Result<Response<OwnerOpendirReply>, Status> {
        self.dispatch(request, "OwnerFiles.Opendir", |handler, peer, request| {
            handler.opendir(&peer, request)
        })
        .await
    }

    async fn readdir(
        &self,
        request: Request<OwnerReaddirRequest>,
    ) -> Result<Response<OwnerReaddirReply>, Status> {
        self.dispatch(request, "OwnerFiles.Readdir", |handler, peer, request| {
            handler.readdir(&peer, request)
        })
        .await
    }

    async fn fsync_dir(
        &self,
        request: Request<OwnerFsyncDirRequest>,
    ) -> Result<Response<OwnerFsyncDirReply>, Status> {
        self.dispatch(request, "OwnerFiles.FsyncDir", |handler, peer, request| {
            handler.fsync_dir(&peer, request)
        })
        .await
    }

    async fn release_dir(
        &self,
        request: Request<OwnerReleaseDirRequest>,
    ) -> Result<Response<OwnerReleaseDirReply>, Status> {
        self.dispatch(
            request,
            "OwnerFiles.ReleaseDir",
            |handler, peer, request| handler.release_dir(&peer, request),
        )
        .await
    }

    async fn symlink(
        &self,
        request: Request<OwnerSymlinkRequest>,
    ) -> Result<Response<OwnerSymlinkReply>, Status> {
        self.dispatch(request, "OwnerFiles.Symlink", |handler, peer, request| {
            handler.symlink(&peer, request)
        })
        .await
    }

    async fn link(
        &self,
        request: Request<OwnerLinkRequest>,
    ) -> Result<Response<OwnerLinkReply>, Status> {
        self.dispatch(request, "OwnerFiles.Link", |handler, peer, request| {
            handler.link(&peer, request)
        })
        .await
    }
}

#[cfg(feature = "ownerfs")]
fn owner_files_unimplemented(operation: &'static str) -> Status {
    coded_status(
        afs_error::NODE_VFS_UNIMPLEMENTED,
        format!("{operation} is not wired to OwnerFs yet"),
    )
}

#[cfg(feature = "ownerfs")]
fn owner_handler_unimplemented(operation: &'static str) -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::NODE_VFS_UNIMPLEMENTED,
        format!("{operation} is not implemented by the injected OwnerFs handler"),
    )
}

#[cfg(all(test, feature = "ownerfs"))]
mod owner_tests {
    use super::*;

    #[test]
    fn p7_internal_peer_uses_request_context_without_transport_identity() {
        let mut request = Request::new(OwnerGetAttrRequest {
            access: Some(afs_protocol::node_data::RootAccess {
                holder_node_id: "actual-internal-caller".into(),
                ..Default::default()
            }),
            ..Default::default()
        });
        assert_eq!(
            authenticate_peer(&InternalPeerAuthenticator, &request).unwrap(),
            "actual-internal-caller"
        );
        let boundary = MtlsPeerAuthenticator::new([("node-b".into(), vec![1, 2, 3])]).unwrap();
        assert!(authenticate_peer(&boundary, &request).is_err());
        request
            .get_mut()
            .access
            .as_mut()
            .unwrap()
            .holder_node_id
            .clear();
        assert_eq!(
            authenticate_peer(&InternalPeerAuthenticator, &request)
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        request.get_mut().access = None;
        assert!(authenticate_peer(&InternalPeerAuthenticator, &request).is_err());
    }

    #[test]
    fn mtls_authenticator_binds_peer_identity_to_exact_leaf_der() {
        let authenticator = MtlsPeerAuthenticator::new(vec![("node-b".to_owned(), vec![1, 2, 3])])
            .expect("trusted cert config should be valid");
        let authenticated = authenticator
            .authenticate(&MetadataMap::new(), None, Some(&[1, 2, 3]))
            .expect("known cert should authenticate");
        assert_eq!(authenticated, "node-b");
    }

    #[test]
    fn mtls_authenticator_fails_closed_without_verified_cert() {
        let authenticator = MtlsPeerAuthenticator::new(vec![("node-b".to_owned(), vec![1, 2, 3])])
            .expect("trusted cert config should be valid");
        let error = authenticator
            .authenticate(&MetadataMap::new(), None, None)
            .expect_err("OwnerFiles must not accept unauthenticated peers");
        assert_eq!(error.code(), afs_error::NODE_OWNER_INVALID_GRANT);
    }

    #[test]
    fn mtls_authenticator_rejects_ambiguous_cert_config() {
        let error = MtlsPeerAuthenticator::new(vec![
            ("node-a".to_owned(), vec![9, 9]),
            ("node-b".to_owned(), vec![9, 9]),
        ])
        .expect_err("one certificate cannot identify two nodes");
        assert_eq!(error.code(), afs_error::CLIENT_ARGUMENT_INVALID);
    }
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
fn dfs_read_caller_epoch(peer: &str, request: &DfsReadRangesRequest) -> Result<u64, Status> {
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
fn validate_packed_dfs_read(request: &DfsReadRangesRequest) -> Result<usize, Status> {
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
    session: Arc<super::control::RdmaSession>,
    disarmed: bool,
}
#[cfg(all(feature = "dfs", feature = "rdma"))]
impl DfsServerRdmaGuard {
    fn new(session: Arc<super::control::RdmaSession>) -> Self {
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
struct PackedDfsRead {
    // Struct fields drop in declaration order: release the actual payload before
    // returning its shared reservation, including early errors during transfer.
    bytes: Vec<u8>,
    completions: Vec<DfsReadRangesCompletion>,
    _reservation: crate::node::chunk::PeerReadReservation,
}

#[cfg(all(feature = "dfs", feature = "rdma"))]
fn read_packed_dfs_ranges(
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
        completions.push(DfsReadRangesCompletion {
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            operation_index: op.operation_index,
            source_copy_id: op.source_copy_id.clone(),
            transferred_bytes: op.length,
            range_checksum: blake3::hash(range).as_bytes().to_vec(),
            range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32,
        });
    }
    Ok(PackedDfsRead {
        bytes,
        completions,
        _reservation: reservation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "dfs")]
    #[test]
    fn p7_internal_replica_context_tracks_the_previous_hop_not_the_initiator() {
        let mut header = afs_protocol::node_data::DfsPutReplicaHeader {
            initiator_node_id: "writer".into(),
            ordered_targets: vec![afs_protocol::node_data::DfsReplicaTarget {
                node_id: "first-copy".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(header.caller_node_id(), Some("writer"));
        header.target_index = 1;
        assert_eq!(header.caller_node_id(), Some("first-copy"));
        header.target_index = u32::MAX;
        assert!(resolve_internal_peer(header.caller_node_id()).is_err());
    }

    #[cfg(feature = "ownerfs")]
    use super::{make_owner_files_server, make_owner_files_server_with_handler};
    #[cfg(feature = "dfs")]
    use crate::node::chunk::ChunkStore;
    use crate::node::rpc::{
        control::make_control_server,
        peer::{DataClientOptions, DataMode, connect_data_client},
    };
    #[cfg(feature = "ownerfs")]
    use afs_protocol::node_data::{
        FileIdentity as PbFileIdentity, OwnerCaller, OwnerDirectoryHandle, OwnerFsyncRequest,
        OwnerGetAttrRequest, OwnerHandle, OwnerLinkRequest, OwnerOpenRequest, OwnerReadReply,
        OwnerReadRequest, OwnerReaddirRequest, OwnerReadlinkRequest, OwnerSymlinkRequest,
        RootAccess, owner_files_client::OwnerFilesClient,
    };
    #[cfg(feature = "ownerfs")]
    use afs_transport::grpc::error_status::status_to_error;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    #[cfg(feature = "dfs")]
    #[test]
    fn wi6_outboard_header_shares_owner_and_preserves_legacy_wire_fields() {
        use crate::node::chunk::outboard::{OutboardBudget, build_borrowed};
        use prost::Message;
        let body = vec![39; 96 * 1024 + 17];
        let budget = OutboardBudget::new(1024 * 1024);
        let proof = build_borrowed(&body, &budget).outboard.unwrap();
        let encoded = proof.encoded().to_vec();
        let address = proof.encoded().as_ptr();
        let bytes = prost::bytes::Bytes::from_owner(PeerOutboardOwner(proof.clone()));
        assert_eq!(
            bytes.as_ptr(),
            address,
            "A must not copy proof metadata into the header"
        );
        let header = DfsReadRangesHeader {
            read_id: "read".into(),
            attempt_id: "attempt".into(),
            operation_index: 3,
            chunk_id: "chunk".into(),
            chunk_offset: 0,
            length: body.len() as u64,
            source_copy_id: "copy".into(),
            blake3_outboard: bytes,
        };
        let queued = header.clone();
        drop(proof);
        drop(header);
        assert!(
            budget.used() > 0,
            "queued header retains the original proof lease"
        );
        // Older readers ignore the new optional tag. Existing identity, range
        // and Completion fields retain their numbers and semantics.
        #[derive(Clone, PartialEq, prost::Message)]
        struct LegacyHeader {
            #[prost(string, tag = "1")]
            read_id: String,
            #[prost(string, tag = "2")]
            attempt_id: String,
            #[prost(uint32, tag = "3")]
            operation_index: u32,
            #[prost(string, tag = "4")]
            chunk_id: String,
            #[prost(uint64, tag = "5")]
            chunk_offset: u64,
            #[prost(uint64, tag = "6")]
            length: u64,
            #[prost(string, tag = "7")]
            source_copy_id: String,
        }
        let wire = queued.encode_to_vec();
        let old = LegacyHeader::decode(wire.as_slice()).unwrap();
        assert_eq!(old.read_id, queued.read_id);
        assert_eq!(old.length, queued.length);
        let decoded = DfsReadRangesHeader::decode(wire.as_slice()).unwrap();
        assert_eq!(decoded.blake3_outboard.as_ref(), encoded);
        let from_old = DfsReadRangesHeader::decode(old.encode_to_vec().as_slice()).unwrap();
        assert!(from_old.blake3_outboard.is_empty());
        let capability = DfsReadRangesRequest {
            include_blake3_outboard: true,
            ..Default::default()
        };
        assert_eq!(
            capability.encode_to_vec(),
            [0x38, 0x01],
            "new capability is only tag 7"
        );
        assert!(
            !DfsReadRangesRequest::decode(&[][..])
                .unwrap()
                .include_blake3_outboard
        );
        drop(queued);
        assert_eq!(
            budget.used(),
            0,
            "codec output is separate transport ownership"
        );
    }

    #[tokio::test]
    async fn grpc_inline_write_then_read_uses_storage() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new(temp.path()).unwrap();
        let service = NodeDataService::new(Arc::new(storage), RdmaSessionRegistry::new(None));

        let written = service
            .write(Request::new(DataWriteRequest {
                session_id: 0,
                transfer: DataTransfer::GrpcInline.into(),
                name: "eight.bin".into(),
                offset: 0,
                data: b"12345678".to_vec(),
                length: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(written.written, 8);

        let read = service
            .read(Request::new(DataReadRequest {
                session_id: 0,
                transfer: DataTransfer::GrpcInline.into(),
                name: "eight.bin".into(),
                offset: 0,
                length: 8,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(read.length, 8);
        assert_eq!(read.data, b"12345678");
    }

    #[tokio::test]
    async fn rdma_read_validates_session_before_touching_storage() {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new(temp.path()).unwrap();
        let service = NodeDataService::new(Arc::new(storage), RdmaSessionRegistry::new(None));

        let error = service
            .read(Request::new(DataReadRequest {
                session_id: 99,
                transfer: DataTransfer::RdmaOneSided.into(),
                name: "missing.bin".into(),
                offset: 0,
                length: 8,
            }))
            .await
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn public_grpc_client_and_servers_move_eight_bytes() {
        let (_temp, endpoint, server) = spawn_grpc_server().await;

        let mut client = connect_data_client(DataClientOptions {
            endpoint: endpoint.clone(),
            mode: DataMode::Grpc,
            rdma_device: None,
            timeout: std::time::Duration::from_secs(5),
            tls: Default::default(),
        })
        .await
        .unwrap();
        assert_eq!(client.mode(), "grpc");
        assert_eq!(
            client
                .write("eight.bin", 0, b"abcdefgh".to_vec())
                .await
                .unwrap(),
            8
        );
        assert_eq!(client.read("eight.bin", 0, 8).await.unwrap(), b"abcdefgh");
        client.close().await.unwrap();
        server.abort();
    }

    #[tokio::test]
    async fn auto_without_rdma_device_falls_back_to_grpc() {
        let (_temp, endpoint, server) = spawn_grpc_server().await;

        let mut client = connect_data_client(DataClientOptions {
            endpoint,
            mode: DataMode::Auto,
            rdma_device: None,
            timeout: std::time::Duration::from_secs(5),
            tls: Default::default(),
        })
        .await
        .unwrap();

        assert_eq!(client.mode(), "grpc");
        assert_eq!(
            client
                .write("auto.bin", 0, b"abcdefgh".to_vec())
                .await
                .unwrap(),
            8
        );
        assert_eq!(client.read("auto.bin", 0, 8).await.unwrap(), b"abcdefgh");
        server.abort();
    }

    #[tokio::test]
    async fn forced_rdma_without_device_does_not_fallback_to_grpc() {
        let (_temp, endpoint, server) = spawn_grpc_server().await;

        let result = connect_data_client(DataClientOptions {
            endpoint,
            mode: DataMode::Rdma,
            rdma_device: None,
            timeout: std::time::Duration::from_secs(5),
            tls: Default::default(),
        })
        .await;
        let error = match result {
            Ok(_) => panic!("forced RDMA unexpectedly fell back to gRPC"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("RDMA"));
        server.abort();
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn owner_files_service_is_registered_but_not_faked() {
        let (_temp, endpoint, server) = spawn_grpc_server().await;
        let mut client = OwnerFilesClient::connect(endpoint).await.unwrap();

        let error = client
            .open(OwnerOpenRequest {
                access: Some(root_access()),
                path: b"notes.txt".to_vec(),
                flags: 0,
                mode: 0,
                expected_file_identity: None,
                kill_suidgid: false,
                disable_prefetch: false,
            })
            .await
            .unwrap_err();

        assert_owner_unimplemented(error);
        server.abort();
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn owner_files_attr_dir_and_fsync_rpcs_are_registered_but_not_faked() {
        let (_temp, endpoint, server) = spawn_grpc_server().await;
        let mut client = OwnerFilesClient::connect(endpoint).await.unwrap();

        assert_owner_unimplemented(
            client
                .get_attr(OwnerGetAttrRequest {
                    access: Some(root_access()),
                    path: b"notes.txt".to_vec(),
                    expected_file_identity: None,
                    handle: None,
                })
                .await
                .unwrap_err(),
        );
        assert_owner_unimplemented(
            client
                .readdir(OwnerReaddirRequest {
                    access: Some(root_access()),
                    handle: Some(OwnerDirectoryHandle {
                        opaque: b"dir-handle".to_vec(),
                    }),
                    offset: 0,
                    max_entries: 16,
                })
                .await
                .unwrap_err(),
        );
        assert_owner_unimplemented(
            client
                .fsync(OwnerFsyncRequest {
                    access: Some(root_access()),
                    handle: Some(OwnerHandle {
                        opaque: b"file-handle".to_vec(),
                    }),
                    datasync: true,
                })
                .await
                .unwrap_err(),
        );
        assert_owner_unimplemented(
            client
                .readlink(OwnerReadlinkRequest {
                    access: Some(root_access()),
                    path: b"link.txt".to_vec(),
                    expected_file_identity: None,
                })
                .await
                .unwrap_err(),
        );

        server.abort();
    }

    #[cfg(feature = "ownerfs")]
    struct AllowOwnerTestPeer;

    #[cfg(feature = "ownerfs")]
    impl PeerAuthenticator for AllowOwnerTestPeer {
        fn authenticate(
            &self,
            _: &MetadataMap,
            _: Option<SocketAddr>,
            _: Option<&[u8]>,
        ) -> afs_error::Result<String> {
            Ok("node-b".into())
        }
    }

    #[cfg(feature = "ownerfs")]
    #[derive(Default)]
    struct CaptureOwnerNamespaceHandler {
        seen_symlink: std::sync::Mutex<Option<OwnerSymlinkRequest>>,
        seen_link: std::sync::Mutex<Option<OwnerLinkRequest>>,
    }

    #[cfg(feature = "ownerfs")]
    impl OwnerFilesHandler for CaptureOwnerNamespaceHandler {
        fn symlink(
            &self,
            peer: &str,
            request: OwnerSymlinkRequest,
        ) -> afs_error::Result<OwnerSymlinkReply> {
            assert_eq!(peer, "node-b");
            *self.seen_symlink.lock().unwrap() = Some(request);
            Ok(OwnerSymlinkReply {
                attr: Some(test_owner_attr(
                    b"symlink-id",
                    afs_protocol::node_data::OwnerFileKind::Symlink,
                )),
                owner_session_id: "node-a-session-9".into(),
            })
        }

        fn link(&self, peer: &str, request: OwnerLinkRequest) -> afs_error::Result<OwnerLinkReply> {
            assert_eq!(peer, "node-b");
            *self.seen_link.lock().unwrap() = Some(request);
            Ok(OwnerLinkReply {
                attr: Some(test_owner_attr(
                    b"link-id",
                    afs_protocol::node_data::OwnerFileKind::Regular,
                )),
                owner_session_id: "node-a-session-9".into(),
            })
        }
    }

    #[cfg(feature = "ownerfs")]
    enum OwnerReadFixture {
        Reply(OwnerReadReply),
        Error(afs_error::Error),
        Panic,
    }

    #[cfg(feature = "ownerfs")]
    struct OwnerReadFixtureHandler {
        result: std::sync::Mutex<Option<OwnerReadFixture>>,
        seen_thread: std::sync::Mutex<Option<std::thread::ThreadId>>,
    }

    #[cfg(feature = "ownerfs")]
    impl OwnerReadFixtureHandler {
        fn reply(data: &[u8], read: u32, eof: bool) -> Arc<Self> {
            Arc::new(Self {
                result: std::sync::Mutex::new(Some(OwnerReadFixture::Reply(OwnerReadReply {
                    data: data.to_vec().into(),
                    read,
                    eof,
                    data_checksum: b"must-be-cleared".to_vec(),
                }))),
                seen_thread: std::sync::Mutex::new(None),
            })
        }

        fn error(code: afs_error::ErrorCode) -> Arc<Self> {
            Arc::new(Self {
                result: std::sync::Mutex::new(Some(OwnerReadFixture::Error(
                    afs_error::Error::coded(code, "fixture application error"),
                ))),
                seen_thread: std::sync::Mutex::new(None),
            })
        }

        fn panic() -> Arc<Self> {
            Arc::new(Self {
                result: std::sync::Mutex::new(Some(OwnerReadFixture::Panic)),
                seen_thread: std::sync::Mutex::new(None),
            })
        }

        fn seen_thread(&self) -> std::thread::ThreadId {
            self.seen_thread.lock().unwrap().unwrap()
        }
    }

    #[cfg(feature = "ownerfs")]
    impl OwnerFilesHandler for OwnerReadFixtureHandler {
        fn read(&self, peer: &str, request: OwnerReadRequest) -> afs_error::Result<OwnerReadReply> {
            assert_eq!(peer, "node-b");
            assert_eq!(request.access.unwrap().root_id, "workspace-1");
            assert_eq!(request.handle.unwrap().opaque, b"file-handle".to_vec());
            *self.seen_thread.lock().unwrap() = Some(std::thread::current().id());
            match self.result.lock().unwrap().take().unwrap() {
                OwnerReadFixture::Reply(reply) => Ok(reply),
                OwnerReadFixture::Error(error) => Err(error),
                OwnerReadFixture::Panic => panic!("fixture owner read panic"),
            }
        }
    }

    #[cfg(feature = "ownerfs")]
    fn owner_read_request(length: u32) -> OwnerReadRequest {
        OwnerReadRequest {
            access: Some(root_access()),
            handle: Some(OwnerHandle {
                opaque: b"file-handle".to_vec(),
            }),
            offset: 0,
            length,
            plane: Some(DataPlane {
                transfer: DataTransfer::GrpcInline.into(),
                rdma_session_id: 0,
                buffer_offset: 0,
            }),
        }
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owner_read_grpc_inline_keeps_short_read_and_eof_contract() {
        let handler = OwnerReadFixtureHandler::reply(b"abc", 3, true);
        let service = OwnerFilesService::new(handler.clone(), Arc::new(AllowOwnerTestPeer));

        let reply = service
            .read(Request::new(owner_read_request(8)))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(&reply.data[..], b"abc");
        assert_eq!(reply.read, 3);
        assert!(reply.eof);
        assert!(reply.data_checksum.is_empty());
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn owner_read_grpc_inline_current_thread_keeps_spawn_blocking_fallback() {
        let handler = OwnerReadFixtureHandler::reply(b"abc", 3, true);
        let caller_thread = std::thread::current().id();
        let service = OwnerFilesService::new(handler.clone(), Arc::new(AllowOwnerTestPeer));

        let reply = service
            .read(Request::new(owner_read_request(8)))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(&reply.data[..], b"abc");
        assert_ne!(handler.seen_thread(), caller_thread);
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owner_read_grpc_inline_rejects_malformed_reply_length() {
        let handler = OwnerReadFixtureHandler::reply(b"abc", 4, false);
        let service = OwnerFilesService::new(handler, Arc::new(AllowOwnerTestPeer));

        let error = service
            .read(Request::new(owner_read_request(8)))
            .await
            .unwrap_err();

        assert_eq!(
            status_to_error(error).code(),
            afs_error::NODE_TRANSFER_INVALID
        );
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owner_read_grpc_inline_preserves_application_errors() {
        let handler = OwnerReadFixtureHandler::error(afs_error::IO_PERMISSION_DENIED);
        let service = OwnerFilesService::new(handler, Arc::new(AllowOwnerTestPeer));

        let error = service
            .read(Request::new(owner_read_request(8)))
            .await
            .unwrap_err();

        assert_eq!(
            status_to_error(error).code(),
            afs_error::IO_PERMISSION_DENIED
        );
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn owner_read_grpc_inline_maps_handler_panic_to_worker_failure() {
        let handler = OwnerReadFixtureHandler::panic();
        let service = OwnerFilesService::new(handler, Arc::new(AllowOwnerTestPeer));

        let error = service
            .read(Request::new(owner_read_request(8)))
            .await
            .unwrap_err();

        assert_eq!(
            status_to_error(error).code(),
            afs_error::CLIENT_WORKER_FAILED
        );
    }

    #[cfg(feature = "ownerfs")]
    fn test_owner_attr(
        identity: &[u8],
        kind: afs_protocol::node_data::OwnerFileKind,
    ) -> afs_protocol::node_data::OwnerFileAttr {
        afs_protocol::node_data::OwnerFileAttr {
            identity: Some(PbFileIdentity {
                opaque: identity.to_vec(),
            }),
            kind: kind as i32,
            mode: 0o644,
            uid: 60001,
            gid: 60001,
            size: 0,
            blocks: 0,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
            nlink: 1,
            blksize: 4096,
            special_node: None,
        }
    }

    #[cfg(feature = "ownerfs")]
    #[tokio::test]
    async fn owner_files_namespace_rpcs_forward_caller_and_expected_identity() {
        let handler = Arc::new(CaptureOwnerNamespaceHandler::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn({
            let handler = handler.clone();
            async move {
                Server::builder()
                    .add_service(make_owner_files_server_with_handler(
                        handler,
                        Arc::new(AllowOwnerTestPeer),
                    ))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            }
        });
        let mut client = OwnerFilesClient::connect(endpoint).await.unwrap();

        client
            .symlink(OwnerSymlinkRequest {
                access: Some(root_access()),
                link_path: b"dir/link".to_vec(),
                target: b"target".to_vec(),
                expected_parent: Some(PbFileIdentity {
                    opaque: b"parent-id".to_vec(),
                }),
                caller: Some(OwnerCaller {
                    uid: 60001,
                    gid: 60002,
                    pid: 77,
                    umask: 0o027,
                    supplementary_gids: vec![60003, 60004],
                }),
            })
            .await
            .unwrap();
        client
            .link(OwnerLinkRequest {
                access: Some(root_access()),
                old_path: b"dir/source".to_vec(),
                new_path: b"dir/hard".to_vec(),
                expected_old_identity: Some(PbFileIdentity {
                    opaque: b"source-id".to_vec(),
                }),
                expected_new_parent: Some(PbFileIdentity {
                    opaque: b"parent-id".to_vec(),
                }),
                caller: Some(OwnerCaller {
                    uid: 60001,
                    gid: 60002,
                    pid: 78,
                    umask: 0o077,
                    supplementary_gids: vec![60003],
                }),
            })
            .await
            .unwrap();

        let symlink = handler.seen_symlink.lock().unwrap().take().unwrap();
        assert_eq!(symlink.link_path, b"dir/link");
        assert_eq!(symlink.target, b"target");
        assert_eq!(
            symlink.expected_parent.unwrap().opaque,
            b"parent-id".to_vec()
        );
        let symlink_caller = symlink.caller.unwrap();
        assert_eq!(symlink_caller.uid, 60001);
        assert_eq!(symlink_caller.gid, 60002);
        assert_eq!(symlink_caller.supplementary_gids, vec![60003, 60004]);

        let link = handler.seen_link.lock().unwrap().take().unwrap();
        assert_eq!(link.old_path, b"dir/source");
        assert_eq!(link.new_path, b"dir/hard");
        assert_eq!(
            link.expected_old_identity.unwrap().opaque,
            b"source-id".to_vec()
        );
        assert_eq!(
            link.expected_new_parent.unwrap().opaque,
            b"parent-id".to_vec()
        );
        assert_eq!(link.caller.unwrap().supplementary_gids, vec![60003]);
        server.abort();
    }

    #[cfg(feature = "ownerfs")]
    fn root_access() -> RootAccess {
        RootAccess {
            root_id: "workspace-1".into(),
            root_epoch: 7,
            access_generation: 3,
            holder_node_id: "node-b".into(),
            home_node_id: "node-a".into(),
            session_id: "node-b-session-11".into(),
            fencing_token: "grant-token-7-3".into(),
            home_session_id: "node-a-session-9".into(),
        }
    }

    #[cfg(feature = "ownerfs")]
    fn assert_owner_unimplemented(error: tonic::Status) {
        assert_eq!(error.code(), tonic::Code::Unimplemented);
        assert_eq!(
            status_to_error(error).code(),
            afs_error::NODE_VFS_UNIMPLEMENTED
        );
    }

    async fn spawn_grpc_server() -> (tempfile::TempDir, String, tokio::task::JoinHandle<()>) {
        let base = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::env::current_dir().unwrap().join("target"))
            .join("afs-test-tmp");
        std::fs::create_dir_all(&base).unwrap();
        let temp = tempfile::Builder::new().tempdir_in(base).unwrap();
        let storage = Arc::new(Storage::new(temp.path()).unwrap());
        let registry = RdmaSessionRegistry::new(None);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn({
            let registry = registry.clone();
            async move {
                let server = Server::builder()
                    .add_service(make_control_server(registry.clone()))
                    .add_service(make_data_server(storage, registry));
                #[cfg(feature = "ownerfs")]
                let server = server.add_service(make_owner_files_server());
                server
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            }
        });
        (temp, endpoint, server)
    }
    #[cfg(feature = "dfs")]
    struct AllowDfsTestPeer;
    #[cfg(feature = "dfs")]
    impl PeerAuthenticator for AllowDfsTestPeer {
        fn authenticate(
            &self,
            _: &MetadataMap,
            _: Option<SocketAddr>,
            _: Option<&[u8]>,
        ) -> afs_error::Result<String> {
            Ok("reader".into())
        }
    }
    #[cfg(feature = "dfs")]
    struct FixtureReplicaGrant(crate::node::replication::ReplicaPeerOp);
    #[cfg(feature = "dfs")]
    impl DfsReplicaAuthorizer for FixtureReplicaGrant {
        fn authorize(
            &self,
            peer: &str,
            header: &afs_protocol::node_data::DfsPutReplicaHeader,
        ) -> afs_error::Result<AuthorizedReplicaWrite> {
            self.0.validate_sender(peer)?;
            if header.chunk_id != self.0.chunk_id.0 {
                return Err(replica_denied("fixture immutable identity differs"));
            }
            Ok(AuthorizedReplicaWrite {
                op: self.0.clone(),
                expires_at_unix_ms: u64::MAX,
            })
        }
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dfs_replica_grpc_stream_durable_retry_and_malformed_body() {
        use crate::node::chunk::StagedChunk;
        use afs_protocol::node_data::{
            dfs_chunks_client::DfsChunksClient, dfs_put_replica_frame::Body,
        };
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "receiver").unwrap());
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("replica-wire"),
            vec![37; 150 * 1024],
        );
        let target = crate::dfs::ReplicaTarget {
            node_id: "receiver".into(),
            node_epoch: 1,
            data_endpoint: "http://127.0.0.1:1".into(),
            device: local.device_descriptor().unwrap(),
        };
        let op = crate::node::replication::ReplicaPeerOp {
            chunk_id: staged.chunk.id.clone(),
            placement_revision: 1,
            placement_epoch: 1,
            replica_group_id: crate::dfs::ReplicaGroupId::new("fixture"),
            initiator_node_id: "reader".into(),
            initiator_node_epoch: 1,
            ordered_targets: vec![target.clone()],
            sync_target_count: 1,
            target_index: 0,
            target,
            chain_tail: vec![],
            repair_claim: None,
        };
        let service = make_dfs_chunks_server_with_replication(
            Some(local.clone()),
            Arc::new(AllowDfsTestPeer),
            Arc::new(DenyDfsReadAuthorizer),
            Arc::new(FixtureReplicaGrant(op.clone())),
            None,
            std::time::Duration::from_secs(5),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let mut client = DfsChunksClient::connect(format!("http://{address}"))
            .await
            .unwrap();
        let header = super::super::peer::replica_header(&op, &staged).unwrap();
        let frames = || {
            let mut result = vec![DfsPutReplicaFrame {
                body: Some(Body::Header(header.clone())),
            }];
            result.extend(staged.bytes().chunks(DFS_REPLICA_FRAME_BYTES).map(|data| {
                DfsPutReplicaFrame {
                    body: Some(Body::Data(data.to_vec().into())),
                }
            }));
            result
        };
        let first = client
            .put_replica_stream(tokio_stream::iter(frames()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(first.durable_acks.len(), 1);
        assert_eq!(first.durable_acks[0].persisted_bytes, 150 * 1024);
        let retry = client
            .put_replica_stream(tokio_stream::iter(frames()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            retry.durable_acks[0].verified_digest,
            first.durable_acks[0].verified_digest
        );
        let mut bad = frames();
        if let Some(Body::Data(bytes)) = &mut bad[1].body {
            let mut corrupt = bytes.to_vec();
            corrupt[0] ^= 1;
            *bytes = corrupt.into();
        }
        assert_eq!(
            client
                .put_replica_stream(tokio_stream::iter(bad))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let duplicate = vec![
            DfsPutReplicaFrame {
                body: Some(Body::Header(header.clone())),
            },
            DfsPutReplicaFrame {
                body: Some(Body::Header(header)),
            },
        ];
        assert_eq!(
            client
                .put_replica_stream(tokio_stream::iter(duplicate))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let mut recovered = vec![0; staged.bytes().len()];
        assert_eq!(
            local.read_at(&staged.chunk.id, 0, &mut recovered).unwrap(),
            recovered.len()
        );
        assert_eq!(recovered, staged.bytes());
        server.abort();
        drop(local);
        let reopened = LocalChunkStore::open(temp.path(), "receiver").unwrap();
        let mut recovered = vec![0; staged.bytes().len()];
        assert_eq!(
            reopened
                .read_at(&staged.chunk.id, 0, &mut recovered)
                .unwrap(),
            recovered.len()
        );
        assert_eq!(recovered, staged.bytes());
    }

    #[cfg(feature = "dfs")]
    struct FixtureReplicaPeer(String);
    #[cfg(feature = "dfs")]
    impl PeerAuthenticator for FixtureReplicaPeer {
        fn authenticate(
            &self,
            _: &MetadataMap,
            _: Option<SocketAddr>,
            _: Option<&[u8]>,
        ) -> afs_error::Result<String> {
            Ok(self.0.clone())
        }
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dfs_replica_grpc_three_hops_retry_missing_tail() {
        replica_three_hops_retry_missing_tail(false).await;
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn p7_internal_replica_three_hops_retry_missing_tail() {
        replica_three_hops_retry_missing_tail(true).await;
    }

    #[cfg(feature = "dfs")]
    async fn replica_three_hops_retry_missing_tail(internal_trust: bool) {
        use crate::node::chunk::StagedChunk;
        use crate::node::replication::ReplicaDataPlane;
        let temp = tempfile::tempdir().unwrap();
        let mut stores = Vec::new();
        let mut listeners = Vec::new();
        let mut targets = Vec::new();
        for id in ["a", "b", "c"] {
            let store = Arc::new(LocalChunkStore::open(temp.path().join(id), id).unwrap());
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            targets.push(crate::dfs::ReplicaTarget {
                node_id: id.into(),
                node_epoch: 1,
                data_endpoint: format!("http://{}", listener.local_addr().unwrap()),
                device: store.device_descriptor().unwrap(),
            });
            stores.push(store);
            listeners.push(Some(listener));
        }
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("chain-wire"),
            vec![71; 180 * 1024],
        );
        let plane = Arc::new(
            super::super::peer::GrpcReplicaDataPlane::new(
                Arc::new(
                    super::super::peer::PeerConnectionPool::new(
                        afs_transport::GrpcConfig::default(),
                        afs_transport::TlsConfig::Disabled,
                        8,
                    )
                    .unwrap(),
                ),
                std::time::Duration::from_secs(2),
            )
            .unwrap(),
        );
        let op = crate::node::replication::ReplicaPeerOp {
            chunk_id: staged.chunk.id.clone(),
            placement_revision: 1,
            placement_epoch: 1,
            replica_group_id: crate::dfs::ReplicaGroupId::new("wire-chain"),
            initiator_node_id: "writer".into(),
            initiator_node_epoch: 1,
            ordered_targets: targets.clone(),
            sync_target_count: 3,
            target_index: 0,
            target: targets[0].clone(),
            chain_tail: targets[1..].to_vec(),
            repair_claim: None,
        };
        let mut ops = vec![op.clone()];
        ops.push(ops[0].next_hop().unwrap().unwrap());
        ops.push(ops[1].next_hop().unwrap().unwrap());
        let mut servers = Vec::new();
        for index in 0..2 {
            let service = make_dfs_chunks_server_with_replication(
                Some(stores[index].clone()),
                if internal_trust {
                    Arc::new(InternalPeerAuthenticator)
                } else {
                    Arc::new(FixtureReplicaPeer(
                        if index == 0 { "writer" } else { "a" }.into(),
                    ))
                },
                Arc::new(DenyDfsReadAuthorizer),
                Arc::new(FixtureReplicaGrant(ops[index].clone())),
                Some(plane.clone()),
                std::time::Duration::from_secs(5),
            );
            let listener = listeners[index].take().unwrap();
            servers.push(tokio::spawn(async move {
                Server::builder()
                    .add_service(service)
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            }));
        }
        let transfer = plane.clone();
        let first_op = op.clone();
        let first_staged = staged.clone();
        assert!(tokio::task::spawn_blocking(move || transfer.put_peer_replica(&first_op, &first_staged)).await.unwrap().is_err());
        for store in &stores[..2] {
            let mut bytes = vec![0; staged.bytes().len()];
            assert_eq!(
                store.read_at(&staged.chunk.id, 0, &mut bytes).unwrap(),
                bytes.len()
            );
            assert_eq!(bytes, staged.bytes());
        }
        let service = make_dfs_chunks_server_with_replication(
            Some(stores[2].clone()),
            if internal_trust {
                Arc::new(InternalPeerAuthenticator)
            } else {
                Arc::new(FixtureReplicaPeer("b".into()))
            },
            Arc::new(DenyDfsReadAuthorizer),
            Arc::new(FixtureReplicaGrant(ops[2].clone())),
            Some(plane.clone()),
            std::time::Duration::from_secs(5),
        );
        let listener = listeners[2].take().unwrap();
        servers.push(tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        }));
        let retry_staged = staged.clone();
        let acks = tokio::task::spawn_blocking(move || plane.put_peer_replica(&op, &retry_staged))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            acks.iter()
                .map(|ack| ack.node_id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        for server in servers {
            server.abort();
        }
        drop(stores);
        for id in ["a", "b", "c"] {
            let store = LocalChunkStore::open(temp.path().join(id), id).unwrap();
            let mut bytes = vec![0; staged.bytes().len()];
            assert_eq!(
                store.read_at(&staged.chunk.id, 0, &mut bytes).unwrap(),
                bytes.len()
            );
            assert_eq!(bytes, staged.bytes());
        }
    }

    #[cfg(feature = "dfs")]
    #[derive(Clone)]
    struct DelayFirstMetaReplyLayer {
        claim: Arc<std::sync::atomic::AtomicBool>,
        report: Arc<std::sync::atomic::AtomicBool>,
        claim_hits: Arc<std::sync::atomic::AtomicUsize>,
        report_hits: Arc<std::sync::atomic::AtomicUsize>,
        delay: std::time::Duration,
    }

    #[cfg(feature = "dfs")]
    impl DelayFirstMetaReplyLayer {
        fn new(delay: std::time::Duration) -> Self {
            Self {
                claim: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                report: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                claim_hits: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                report_hits: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                delay,
            }
        }

        fn claim_hits(&self) -> usize {
            self.claim_hits.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn report_hits(&self) -> usize {
            self.report_hits.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[cfg(feature = "dfs")]
    impl<S> tower::Layer<S> for DelayFirstMetaReplyLayer {
        type Service = DelayFirstMetaReplyService<S>;

        fn layer(&self, inner: S) -> Self::Service {
            DelayFirstMetaReplyService {
                inner,
                claim: self.claim.clone(),
                report: self.report.clone(),
                claim_hits: self.claim_hits.clone(),
                report_hits: self.report_hits.clone(),
                delay: self.delay,
            }
        }
    }

    #[cfg(feature = "dfs")]
    #[derive(Clone)]
    struct DelayFirstMetaReplyService<S> {
        inner: S,
        claim: Arc<std::sync::atomic::AtomicBool>,
        report: Arc<std::sync::atomic::AtomicBool>,
        claim_hits: Arc<std::sync::atomic::AtomicUsize>,
        report_hits: Arc<std::sync::atomic::AtomicUsize>,
        delay: std::time::Duration,
    }

    #[cfg(feature = "dfs")]
    impl<S, B> tower::Service<tonic::codegen::http::Request<B>> for DelayFirstMetaReplyService<S>
    where
        S: tower::Service<tonic::codegen::http::Request<B>> + Clone + Send + 'static,
        S::Future: Send + 'static,
        S::Response: Send + 'static,
        S::Error: Send + 'static,
        B: Send + 'static,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future =
            Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(
            &mut self,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
            let path = request.uri().path().to_owned();
            let future = self.inner.call(request);
            let claim = self.claim.clone();
            let report = self.report.clone();
            let claim_hits = self.claim_hits.clone();
            let report_hits = self.report_hits.clone();
            let delay = self.delay;
            Box::pin(async move {
                let response = future.await?;
                let should_delay = match path.as_str() {
                    "/afs.meta.v1.DfsMeta/ClaimReplicationTask"
                        if claim.swap(false, std::sync::atomic::Ordering::SeqCst) =>
                    {
                        claim_hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        true
                    }
                    "/afs.meta.v1.DfsMeta/ReportReplicationTask"
                        if report.swap(false, std::sync::atomic::Ordering::SeqCst) =>
                    {
                        report_hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        true
                    }
                    _ => false,
                };
                if should_delay {
                    tokio::time::sleep(delay).await;
                }
                Ok(response)
            })
        }
    }

    #[cfg(feature = "dfs")]
    async fn grpc_unknown_repair_fixture() -> (
        tempfile::TempDir,
        Arc<LocalChunkStore>,
        Arc<LocalChunkStore>,
        afs_metrics::Registry,
        crate::dfs::ChunkObject,
        Arc<dyn crate::meta::store::MetaStore>,
        String,
        DelayFirstMetaReplyLayer,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        use crate::dfs::*;
        use crate::meta::store::{
            MetaEntity, MetaStore, MetaTxn, NodeSessionLease, OperationResult, RequestKey,
            RequestOutcome, Store, StoreOperation, TxnCondition, TxnMutation, journal::MemoryLog,
        };
        use crate::node::chunk::StagedChunk;

        let temp = tempfile::tempdir().unwrap();
        let source_store =
            Arc::new(LocalChunkStore::open(temp.path().join("source"), "source").unwrap());
        let target_store =
            Arc::new(LocalChunkStore::open(temp.path().join("target"), "target").unwrap());
        let target_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_endpoint = format!("http://{}", target_listener.local_addr().unwrap());

        let staged = StagedChunk::new(OperationId::new("grpc-unknown-source"), vec![23; 64 * 1024]);
        let source_target = ReplicaTarget {
            node_id: "source".into(),
            node_epoch: 1,
            data_endpoint: "http://127.0.0.1:1".into(),
            device: source_store.device_descriptor().unwrap(),
        };
        let source_acks = source_store
            .persist_batch(std::slice::from_ref(&staged), &source_target, 1, 1)
            .unwrap();
        let source_ack = source_acks.into_iter().next().unwrap();
        let source_copy = CopyRecord {
            id: CopyId::new(format!(
                "source:1:{}:{}",
                source_ack.device_id, staged.chunk.id.0
            )),
            chunk_id: staged.chunk.id.clone(),
            role: CopyRole::DurableReplica,
            location: CopyLocation::Node {
                node_id: "source".into(),
                node_epoch: 1,
                device_id: source_ack.device_id.clone(),
                device_epoch: source_ack.device_epoch,
                catalog_revision: source_ack.catalog_revision,
            },
            state: CopyState::Ready,
            persisted_bytes: source_ack.persisted_bytes,
            verified_digest: source_ack.verified_digest,
        };

        let store: Arc<dyn MetaStore> = Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        );
        for (node_id, data_addr, device) in [
            (
                "source",
                "http://127.0.0.1:1".to_owned(),
                source_store.device_descriptor().unwrap(),
            ),
            (
                "target",
                target_endpoint.clone(),
                target_store.device_descriptor().unwrap(),
            ),
        ] {
            store
                .register_node_session(
                    RequestKey::new(node_id, "register"),
                    NodeSessionLease {
                        node_id: node_id.into(),
                        session_id: format!("{node_id}-session"),
                        grpc_addr: data_addr.clone(),
                        data_addr,
                        rest_addr: "http://127.0.0.1:1".into(),
                        storage_devices: vec![device],
                        lease_ttl: std::time::Duration::from_secs(30),
                    },
                )
                .await
                .unwrap();
        }
        let dfs = crate::meta::dfs::DfsService::with_replication_config(
            store.clone(),
            ReplicationConfig {
                desired_copies: 2,
                sync_required_copies: 2,
                min_distinct_nodes: 1,
                min_distinct_failure_domains: 1,
                local_copy: LocalCopyPolicy::Required,
            },
        );
        dfs.initialize_replication_config().await.unwrap();
        let key = RequestKey::new("fixture", "seed-underreplicated");
        let task_id = ReplicationTaskId::new(format!("repair:{}", staged.chunk.id.0));
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsChunk(staged.chunk.clone())),
            TxnMutation::Put(MetaEntity::DfsCopy(source_copy.clone())),
            TxnMutation::Put(MetaEntity::DfsPlacement(PlacementRecord {
                chunk_id: staged.chunk.id.clone(),
                replica_group_id: ReplicaGroupId::new("repair-grpc"),
                placement_epoch: 1,
                desired_copies: 2,
                copies: vec![source_copy.id.clone()],
                health: PlacementHealth::UnderReplicated,
            })),
            TxnMutation::Put(MetaEntity::DfsReplicationTask(ReplicationTask {
                id: task_id,
                chunk_id: staged.chunk.id.clone(),
                placement_epoch: 1,
                desired_copies: 2,
                existing_copies: vec![source_copy.id],
                state: ReplicationTaskState::Pending,
                attempt: 0,
                next_retry_unix_ms: 0,
                last_error: None,
                claim: None,
            })),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: StoreOperation::DfsCommitFileVersion,
                result: OperationResult::Empty,
            }),
        ]);
        store.compare_and_commit(txn).await.unwrap();

        let mut meta = crate::meta::Meta::with_store(
            "meta-grpc-unknown".into(),
            crate::runtime::Observability::new().unwrap(),
            store.clone(),
        );
        // Preserve the failure-injecting DFS authority rather than replacing
        // it with the constructor's ordinary service.
        meta.dfs = Some(dfs);
        let meta = Arc::new(meta);
        let meta_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let meta_endpoint = format!("http://{}", meta_listener.local_addr().unwrap());
        let delay_layer = DelayFirstMetaReplyLayer::new(std::time::Duration::from_secs(2));
        let meta_delay_probe = delay_layer.clone();
        let meta_server = tokio::spawn(async move {
            Server::builder()
                .layer(delay_layer)
                .add_service(afs_protocol::meta::dfs_meta_server::DfsMetaServer::new(
                    crate::meta::rpc::DfsMetaRpc(meta),
                ))
                .serve_with_incoming(TcpListenerStream::new(meta_listener))
                .await
                .unwrap();
        });

        let meta_for_target = Arc::new(
            super::super::meta::GrpcDfsMeta::new(
                &meta_endpoint,
                "target".into(),
                "target-session".into(),
                NamespaceId::new("default"),
                std::time::Duration::from_secs(2),
                afs_transport::TlsConfig::Disabled,
            )
            .unwrap(),
        );
        let registry = afs_metrics::Registry::new();
        let metrics = DfsPayloadMetrics::register(&registry).unwrap();
        let target_service = make_dfs_chunks_server_with_transport(
            Some(target_store.clone()),
            Arc::new(FixtureReplicaPeer("source".into())),
            Arc::new(DenyDfsReadAuthorizer),
            Arc::new(MetaReplicaAuthorizer {
                meta: meta_for_target,
                node_id: "target".into(),
                node_epoch: 1,
            }),
            None,
            std::time::Duration::from_secs(2),
            DfsChunkTransportResources {
                rdma_sessions: super::super::control::RdmaSessionRegistry::new(None),
                payload_metrics: Some(metrics),
            },
        );
        let target_server = tokio::spawn(async move {
            Server::builder()
                .add_service(target_service)
                .serve_with_incoming(TcpListenerStream::new(target_listener))
                .await
                .unwrap();
        });

        (
            temp,
            source_store,
            target_store.clone(),
            registry,
            staged.chunk,
            store,
            meta_endpoint,
            meta_delay_probe,
            meta_server,
            target_server,
        )
    }

    #[cfg(feature = "dfs")]
    async fn stored_replication_task(
        store: &dyn crate::meta::store::MetaStore,
        chunk: &crate::dfs::ChunkObject,
    ) -> crate::dfs::ReplicationTask {
        read_repair_meta_snapshot(store, chunk).await.task
    }

    #[cfg(feature = "dfs")]
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RepairMetaSnapshot {
        task_revision: crate::meta::store::StoreRevision,
        placement_revision: crate::meta::store::StoreRevision,
        claim_outcome_revision: crate::meta::store::StoreRevision,
        report_outcome_revision: Option<crate::meta::store::StoreRevision>,
        task: crate::dfs::ReplicationTask,
        placement: crate::dfs::PlacementRecord,
        copies: Vec<(
            crate::dfs::CopyId,
            crate::meta::store::StoreRevision,
            crate::dfs::CopyRecord,
        )>,
        claim_outcome: crate::meta::store::RequestOutcome,
        report_outcome: Option<crate::meta::store::RequestOutcome>,
    }

    #[cfg(feature = "dfs")]
    async fn read_repair_meta_snapshot(
        store: &dyn crate::meta::store::MetaStore,
        chunk: &crate::dfs::ChunkObject,
    ) -> RepairMetaSnapshot {
        use crate::meta::store::{MetaEntity, MetaRead, RequestKey};
        let task_snapshot = store
            .read(MetaRead::DfsReplicationTask(
                crate::dfs::ReplicationTaskId::new(format!("repair:{}", chunk.id.0)),
            ))
            .await
            .unwrap();
        let task = match task_snapshot.entity {
            Some(MetaEntity::DfsReplicationTask(task)) => task,
            _ => panic!("replication task missing"),
        };
        let placement_snapshot = store
            .read(MetaRead::DfsPlacement(chunk.id.clone()))
            .await
            .unwrap();
        let placement = match placement_snapshot.entity {
            Some(MetaEntity::DfsPlacement(placement)) => placement,
            _ => panic!("placement missing"),
        };
        let mut copies = Vec::new();
        let mut copy_ids = placement.copies.clone();
        copy_ids.sort();
        for copy_id in copy_ids {
            let copy_snapshot = store
                .read(MetaRead::DfsCopy(copy_id.clone()))
                .await
                .unwrap();
            let copy = match copy_snapshot.entity {
                Some(MetaEntity::DfsCopy(copy)) => copy,
                _ => panic!("copy missing"),
            };
            copies.push((copy_id, copy_snapshot.revision, copy));
        }
        let claim_outcome_snapshot = store
            .read(MetaRead::RequestOutcome(RequestKey::new(
                "source",
                "repair-claim:source-session:1",
            )))
            .await
            .unwrap();
        let claim_outcome = claim_outcome_snapshot
            .request_outcome
            .expect("claim request outcome missing");
        let report_outcome_snapshot = store
            .read(MetaRead::RequestOutcome(RequestKey::new(
                "source",
                "repair-report:source-session:2",
            )))
            .await
            .unwrap();
        RepairMetaSnapshot {
            task_revision: task_snapshot.revision,
            placement_revision: placement_snapshot.revision,
            claim_outcome_revision: claim_outcome_snapshot.revision,
            report_outcome_revision: report_outcome_snapshot
                .request_outcome
                .as_ref()
                .map(|_| report_outcome_snapshot.revision),
            task,
            placement,
            copies,
            claim_outcome,
            report_outcome: report_outcome_snapshot.request_outcome,
        }
    }

    #[cfg(feature = "dfs")]
    async fn run_repair_tick(
        worker: Arc<crate::node::replication::ReplicationWorker>,
    ) -> afs_error::Result<Option<crate::dfs::ReplicationTask>> {
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            tokio::task::spawn_blocking(move || worker.run_once()),
        )
        .await
        .expect("repair worker tick timed out")
        .unwrap()
    }

    #[cfg(feature = "dfs")]
    fn read_whole_chunk(store: &LocalChunkStore, chunk: &crate::dfs::ChunkObject) -> Vec<u8> {
        let mut bytes = vec![0; chunk.length as usize];
        assert_eq!(
            crate::node::chunk::ChunkStore::read_at(store, &chunk.id, 0, &mut bytes).unwrap(),
            bytes.len()
        );
        bytes
    }

    #[cfg(feature = "dfs")]
    fn grpc_replica_payload_bytes(registry: &afs_metrics::Registry) -> u64 {
        afs_metrics::encode_text(registry)
            .unwrap()
            .lines()
            .find_map(|line| {
                if line.starts_with("afs_dfs_payload_bytes_total{")
                    && line.contains("direction=\"recv\"")
                    && line.contains("operation=\"replica\"")
                    && line.contains("transport=\"grpc\"")
                {
                    line.rsplit_once(' ')
                        .and_then(|(_, value)| value.parse::<u64>().ok())
                } else {
                    None
                }
            })
            .unwrap_or(0)
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn repair_worker_replays_real_grpc_claim_and_report_after_lost_ack() {
        use crate::meta::store::{OperationResult, RequestKey, StoreOperation};
        use crate::node::replication::ReplicationWorker;
        let (
            _temp,
            source_store,
            target_store,
            registry,
            chunk,
            store,
            meta_endpoint,
            meta_delay_probe,
            meta_server,
            target_server,
        ) = grpc_unknown_repair_fixture().await;
        let authority = Arc::new(
            super::super::meta::GrpcDfsMeta::new(
                &meta_endpoint,
                "source".into(),
                "source-session".into(),
                crate::dfs::NamespaceId::new("default"),
                std::time::Duration::from_millis(200),
                afs_transport::TlsConfig::Disabled,
            )
            .unwrap(),
        );
        let data_plane = Arc::new(
            super::super::peer::GrpcReplicaDataPlane::new(
                Arc::new(
                    super::super::peer::PeerConnectionPool::new(
                        afs_transport::GrpcConfig::default(),
                        afs_transport::TlsConfig::Disabled,
                        4,
                    )
                    .unwrap(),
                ),
                std::time::Duration::from_secs(2),
            )
            .unwrap(),
        );
        let worker = Arc::new(ReplicationWorker::new(
            "source".into(),
            1,
            "source-session".into(),
            source_store,
            authority,
            data_plane,
        ));

        let first = run_repair_tick(worker.clone()).await;
        assert!(first.is_err());
        assert_eq!(meta_delay_probe.claim_hits(), 1);
        assert_eq!(meta_delay_probe.report_hits(), 0);
        assert_eq!(grpc_replica_payload_bytes(&registry), 0);
        let running = stored_replication_task(store.as_ref(), &chunk).await;
        assert_eq!(running.state, crate::dfs::ReplicationTaskState::Running);
        let running_claim = running
            .claim
            .as_deref()
            .expect("claim committed before timeout");
        assert_eq!(
            running_claim.operation_id.0,
            "repair-claim:source-session:1"
        );
        assert_eq!(running_claim.worker_node_id, "source");
        assert_eq!(running_claim.worker_session_id, "source-session");
        let after_claim_unknown = read_repair_meta_snapshot(store.as_ref(), &chunk).await;
        assert_eq!(
            after_claim_unknown.claim_outcome.request,
            RequestKey::new("source", "repair-claim:source-session:1")
        );
        assert_eq!(
            after_claim_unknown.claim_outcome.operation,
            StoreOperation::DfsClaimReplicationTask
        );
        assert_eq!(after_claim_unknown.report_outcome, None);
        assert_eq!(after_claim_unknown.report_outcome_revision, None);
        match &after_claim_unknown.claim_outcome.result {
            OperationResult::DfsReplicationClaim {
                claim: Some(claim), ..
            } => {
                assert_eq!(claim.operation_id.0, "repair-claim:source-session:1");
            }
            other => panic!("unexpected claim outcome: {other:?}"),
        }

        let second = run_repair_tick(worker.clone()).await;
        assert!(second.is_err());
        assert_eq!(meta_delay_probe.claim_hits(), 1);
        assert_eq!(meta_delay_probe.report_hits(), 1);
        assert_eq!(grpc_replica_payload_bytes(&registry), chunk.length);
        assert_eq!(
            read_whole_chunk(target_store.as_ref(), &chunk),
            vec![23; chunk.length as usize]
        );
        let after_report_unknown = read_repair_meta_snapshot(store.as_ref(), &chunk).await;
        assert_eq!(
            after_report_unknown.claim_outcome_revision,
            after_claim_unknown.claim_outcome_revision.next()
        );
        assert_eq!(
            after_report_unknown.task.state,
            crate::dfs::ReplicationTaskState::Completed
        );
        assert_eq!(after_report_unknown.task.claim, None);
        assert_eq!(after_report_unknown.task.attempt, 1);
        assert_eq!(after_report_unknown.task.existing_copies.len(), 2);
        assert_eq!(
            after_report_unknown.placement.health,
            crate::dfs::PlacementHealth::Satisfied
        );
        assert_eq!(after_report_unknown.placement.copies.len(), 2);
        for (_, _, copy) in &after_report_unknown.copies {
            assert_eq!(copy.state, crate::dfs::CopyState::Ready);
            assert_eq!(copy.persisted_bytes, chunk.length);
            assert_eq!(copy.verified_digest, chunk.content_digest);
        }
        let report_outcome = after_report_unknown.report_outcome.as_ref().unwrap();
        assert_eq!(
            report_outcome.request,
            RequestKey::new("source", "repair-report:source-session:2")
        );
        assert_eq!(
            report_outcome.operation,
            StoreOperation::DfsReportReplicationTask
        );
        assert!(after_report_unknown.report_outcome_revision.is_some());
        match &report_outcome.result {
            OperationResult::DfsNamespace { result, .. } => match result.as_ref() {
                OperationResult::DfsReplicationTask(task) => {
                    assert_eq!(task.state, crate::dfs::ReplicationTaskState::Completed);
                    assert_eq!(
                        task.existing_copies,
                        after_report_unknown.task.existing_copies
                    );
                }
                other => panic!("unexpected report outcome: {other:?}"),
            },
            other => panic!("unexpected report outcome wrapper: {other:?}"),
        }

        let third = run_repair_tick(worker.clone()).await.unwrap();
        assert_eq!(third.unwrap(), after_report_unknown.task);
        assert_eq!(meta_delay_probe.claim_hits(), 1);
        assert_eq!(meta_delay_probe.report_hits(), 1);
        assert_eq!(grpc_replica_payload_bytes(&registry), chunk.length);
        assert_eq!(
            read_whole_chunk(target_store.as_ref(), &chunk),
            vec![23; chunk.length as usize]
        );
        let after_report_replay = read_repair_meta_snapshot(store.as_ref(), &chunk).await;
        assert_eq!(after_report_replay, after_report_unknown);

        target_server.abort();
        meta_server.abort();
    }

    #[cfg(feature = "dfs")]
    struct MemoryReadGrantValidator {
        service: crate::meta::dfs::DfsService,
        runtime: tokio::runtime::Handle,
        calls: std::sync::atomic::AtomicUsize,
        unavailable: std::sync::atomic::AtomicBool,
    }

    #[cfg(feature = "dfs")]
    struct SkewedReadGrantValidator {
        change_identity: bool,
        exceed_grant_expiry: bool,
    }

    #[cfg(feature = "dfs")]
    impl DfsReadGrantValidator for SkewedReadGrantValidator {
        fn validate(
            &self,
            request: crate::dfs::ValidateDfsReadGrants,
        ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            Ok(request
                .validations
                .into_iter()
                .map(|mut validation| {
                    let expires_at_unix_ms = if self.exceed_grant_expiry {
                        validation.grant.expires_at_unix_ms + 1
                    } else {
                        assert!(validation.grant.expires_at_unix_ms > now + 5_600);
                        now + 5_500
                    };
                    let allowed_ranges = vec![(validation.chunk_offset, validation.length)];
                    if self.change_identity {
                        validation.copy_id = crate::dfs::CopyId::new("another-copy");
                    }
                    crate::dfs::DfsAuthorizedRead {
                        validation,
                        allowed_ranges,
                        expires_at_unix_ms,
                    }
                })
                .collect())
        }
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dfs_read_authority_tolerates_meta_clock_skew_without_extending_cache() {
        let (_meta, _local, _temp, request) = memory_read_authority_fixture().await;
        let authorizer = CachedDfsReadAuthorizer::new(
            Arc::new(SkewedReadGrantValidator {
                change_identity: false,
                exceed_grant_expiry: false,
            }),
            "receiver".into(),
            1,
        );
        authorizer.authorize("reader", &request).unwrap();
        let cache = authorizer.cache.lock().unwrap();
        assert!(!cache.is_empty());
        for entry in cache.values() {
            let remaining = entry
                .deadline
                .saturating_duration_since(std::time::Instant::now());
            assert!(remaining > std::time::Duration::ZERO);
            assert!(remaining <= std::time::Duration::from_secs(5));
        }
        drop(cache);

        for (change_identity, exceed_grant_expiry) in [(true, false), (false, true)] {
            let authorizer = CachedDfsReadAuthorizer::new(
                Arc::new(SkewedReadGrantValidator {
                    change_identity,
                    exceed_grant_expiry,
                }),
                "receiver".into(),
                1,
            );
            assert!(authorizer.authorize("reader", &request).is_err());
            assert!(authorizer.cache.lock().unwrap().is_empty());
        }
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "checkpoint authorization cost probe; release Linux only, explicit sample budget"]
    async fn checkpoint_read_authority_hit_cost() {
        let (_meta, _local, _temp, request) = memory_read_authority_fixture().await;
        let validator = Arc::new(CountingDeniedReadValidator(
            std::sync::atomic::AtomicUsize::new(0),
        ));
        for size in [1, 128, 1024] {
            let authorizer = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 1);
            let op = &request.operations[0];
            let wire = op.grant.as_ref().unwrap();
            let mut validation = crate::dfs::DfsReadValidation {
                grant: crate::dfs::DfsReadGrant {
                    namespace_id: crate::dfs::NamespaceId::new(wire.namespace_id.clone()),
                    file_version_id: crate::dfs::FileVersionId::new(wire.file_version_id.clone()),
                    layout_root_id: crate::dfs::LayoutRootId::new(wire.layout_root_id.clone()),
                    caller_node_id: wire.caller_node_id.clone(),
                    caller_node_epoch: wire.caller_node_epoch,
                    expires_at_unix_ms: wire.expires_at_unix_ms,
                    fence: wire.fence,
                    token: wire.token.clone(),
                },
                chunk_id: crate::dfs::ChunkId::new(op.chunk_id.clone()),
                copy_id: crate::dfs::CopyId::new(op.source_copy_id.clone()),
                chunk_offset: op.chunk_offset,
                length: op.length,
            };
            for index in 0..size {
                if index != 0 {
                    validation.copy_id = crate::dfs::CopyId::new(format!("copy-{index}"));
                }
                authorizer.cache.lock().unwrap().insert(
                    read_authority_key(&validation).unwrap(),
                    CachedReadAuthority {
                        authorization: crate::dfs::DfsAuthorizedRead {
                            validation: validation.clone(),
                            allowed_ranges: vec![(op.chunk_offset, op.length)],
                            expires_at_unix_ms: wire.expires_at_unix_ms,
                        },
                        deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
                    },
                );
            }
            for sample in 0..3 {
                let start = std::time::Instant::now();
                for _ in 0..4096 {
                    authorizer.authorize("reader", &request).unwrap();
                }
                eprintln!(
                    "checkpoint-authority-hit size={size} sample={sample} calls=4096 ns={}",
                    start.elapsed().as_nanos()
                );
            }
        }
        assert_eq!(validator.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[cfg(feature = "dfs")]
    struct CountingDeniedReadValidator(std::sync::atomic::AtomicUsize);

    #[cfg(feature = "dfs")]
    impl DfsReadGrantValidator for CountingDeniedReadValidator {
        fn validate(
            &self,
            _: crate::dfs::ValidateDfsReadGrants,
        ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(replica_denied("test validator denied cache miss"))
        }
    }
    #[cfg(feature = "dfs")]
    impl DfsReadGrantValidator for MemoryReadGrantValidator {
        fn validate(
            &self,
            request: crate::dfs::ValidateDfsReadGrants,
        ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.unavailable.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_UNAVAILABLE,
                    "test Meta unavailable",
                ));
            }
            self.runtime
                .block_on(self.service.validate_read_grants(request))
        }
    }

    #[cfg(feature = "dfs")]
    async fn memory_read_authority_fixture() -> (
        Arc<crate::meta::Meta>,
        Arc<LocalChunkStore>,
        tempfile::TempDir,
        DfsReadRangesRequest,
    ) {
        use crate::dfs::*;
        use crate::meta::store::{
            MetaEntity, MetaStore, MetaTxn, NodeSessionLease, OperationResult, RequestKey,
            RequestOutcome, Store, StoreOperation, TxnCondition, TxnMutation, journal::MemoryLog,
        };
        use crate::node::chunk::StagedChunk;
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "receiver").unwrap());
        let staged = StagedChunk::new(OperationId::new("read-fixture"), vec![9; 150 * 1024]);
        local.put(staged.clone()).unwrap();
        let device = local.device_descriptor().unwrap();
        let store = Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .unwrap(),
        );
        for id in ["reader", "receiver"] {
            store
                .register_node_session(
                    RequestKey::new(id, "register"),
                    NodeSessionLease {
                        node_id: id.into(),
                        session_id: format!("session-{id}"),
                        grpc_addr: "http://127.0.0.1:1".into(),
                        data_addr: "http://127.0.0.1:1".into(),
                        rest_addr: "http://127.0.0.1:1".into(),
                        storage_devices: vec![device.clone()],
                        lease_ttl: std::time::Duration::from_secs(30),
                    },
                )
                .await
                .unwrap();
        }
        let meta = Arc::new(crate::meta::Meta::with_store(
            "meta-test".into(),
            crate::runtime::Observability::new().unwrap(),
            store.clone(),
        ));
        let mut inode = crate::meta::dfs::DfsService::new(store.clone())
            .get_inode(InodeId::new("1"))
            .await
            .unwrap();
        inode.inode_id = InodeId::new("read-inode");
        inode.kind = InodeKind::Regular;
        inode.head_version = Some(FileVersionId::new("version"));
        let layout = LayoutRoot {
            id: LayoutRootId::new("layout"),
            file_length: 100_000,
            inline_extents: vec![Extent {
                file_offset: 0,
                length: 100_000,
                chunk_id: staged.chunk.id.clone(),
                chunk_offset: 1024,
            }],
        };
        let copy = CopyRecord {
            id: CopyId::new("copy"),
            chunk_id: staged.chunk.id.clone(),
            role: CopyRole::DurableReplica,
            state: CopyState::Ready,
            location: CopyLocation::Node {
                node_id: "receiver".into(),
                node_epoch: 1,
                device_id: device.device_id,
                device_epoch: device.device_epoch,
                catalog_revision: device.catalog_revision,
            },
            persisted_bytes: staged.chunk.length,
            verified_digest: staged.chunk.content_digest.clone(),
        };
        let key = RequestKey::new("fixture", "seed");
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        txn.mutations = vec![
            TxnMutation::Put(MetaEntity::DfsInode(inode)),
            TxnMutation::Put(MetaEntity::DfsFileVersion(FileVersion {
                id: FileVersionId::new("version"),
                inode_id: InodeId::new("read-inode"),
                parent_version: None,
                length: layout.file_length,
                layout_root: layout.id.clone(),
                created_at_unix_ms: 1,
            })),
            TxnMutation::Put(MetaEntity::DfsLayoutRoot(layout)),
            TxnMutation::Put(MetaEntity::DfsChunk(staged.chunk.clone())),
            TxnMutation::Put(MetaEntity::DfsCopy(copy)),
            TxnMutation::Put(MetaEntity::DfsPlacement(PlacementRecord {
                chunk_id: staged.chunk.id.clone(),
                replica_group_id: ReplicaGroupId::new("group"),
                placement_epoch: 1,
                desired_copies: 1,
                copies: vec![CopyId::new("copy")],
                health: PlacementHealth::Satisfied,
            })),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: StoreOperation::DfsCommitFileVersion,
                result: OperationResult::Empty,
            }),
        ];
        store.compare_and_commit(txn).await.unwrap();
        let sources = meta
            .dfs
            .as_ref()
            .unwrap()
            .chunk_sources(DfsChunkSourcesRequest {
                caller_id: "reader".into(),
                namespace_id: NamespaceId::new("default"),
                file_version_id: FileVersionId::new("version"),
                layout_root_id: LayoutRootId::new("layout"),
                chunk_ids: vec![staged.chunk.id.clone()],
            })
            .await
            .unwrap();
        let source = &sources.chunks[0].sources[0];
        let grant = &source.read_grant;
        let request = DfsReadRangesRequest {
            include_blake3_outboard: false,
            read_id: "read".into(),
            attempt_id: "attempt".into(),
            file_version_id: "version".into(),
            layout_root_id: "layout".into(),
            operations: vec![afs_protocol::node_data::DfsChunkReadOp {
                operation_index: 0,
                chunk_id: staged.chunk.id.0,
                chunk_offset: 1024,
                length: 75_000,
                destination_offset: 0,
                source_copy_id: "copy".into(),
                grant: Some(afs_protocol::node_data::DfsReadGrant {
                    namespace_id: grant.namespace_id.0.clone(),
                    file_version_id: grant.file_version_id.0.clone(),
                    layout_root_id: grant.layout_root_id.0.clone(),
                    caller_node_id: grant.caller_node_id.clone(),
                    caller_node_epoch: grant.caller_node_epoch,
                    expires_at_unix_ms: grant.expires_at_unix_ms,
                    fence: grant.fence,
                    token: grant.token.clone(),
                }),
            }],
        };
        (meta, local, temp, request)
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dfs_read_authority_batch_cache_and_adversarial_identity() {
        let (meta, _local, _temp, request) = memory_read_authority_fixture().await;
        let validator = Arc::new(MemoryReadGrantValidator {
            service: meta.dfs.as_ref().unwrap().clone(),
            runtime: tokio::runtime::Handle::current(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            unavailable: std::sync::atomic::AtomicBool::new(false),
        });
        let authorizer = Arc::new(CachedDfsReadAuthorizer::new(
            validator.clone(),
            "receiver".into(),
            1,
        ));
        let good = request.clone();
        let actor = authorizer.clone();
        tokio::task::spawn_blocking(move || {
            let mut batched = good.clone();
            let mut second = batched.operations[0].clone();
            second.operation_index = 1;
            second.chunk_offset = 80_000;
            second.length = 4_000;
            second.destination_offset = 75_000;
            batched.operations.push(second);
            actor.authorize("reader", &batched).unwrap();
            actor.authorize("reader", &good).unwrap();
        })
        .await
        .unwrap();
        assert_eq!(validator.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(authorizer.cache.lock().unwrap().len(), 1);
        let original_deadline = authorizer
            .cache
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .deadline;
        validator
            .unavailable
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let actor = authorizer.clone();
        let good = request.clone();
        tokio::task::spawn_blocking(move || actor.authorize("reader", &good))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            authorizer
                .cache
                .lock()
                .unwrap()
                .values()
                .next()
                .unwrap()
                .deadline,
            original_deadline
        );
        // A compact index must never accept a changed grant while the original
        // cached capability is live, even when Meta cannot answer the miss.
        let hot_mutations: Vec<fn(&mut DfsReadRangesRequest)> = vec![
            |r| r.operations[0].grant.as_mut().unwrap().namespace_id = "other".into(),
            |r| {
                r.file_version_id = "other".into();
                r.operations[0].grant.as_mut().unwrap().file_version_id = "other".into();
            },
            |r| {
                r.layout_root_id = "other".into();
                r.operations[0].grant.as_mut().unwrap().layout_root_id = "other".into();
            },
            |r| r.operations[0].grant.as_mut().unwrap().caller_node_id = "other".into(),
            |r| r.operations[0].grant.as_mut().unwrap().caller_node_epoch += 1,
            |r| r.operations[0].grant.as_mut().unwrap().expires_at_unix_ms += 1,
            |r| r.operations[0].grant.as_mut().unwrap().fence += 1,
            |r| {
                r.operations[0]
                    .grant
                    .as_mut()
                    .unwrap()
                    .token
                    .replace_range(75..76, "x")
            },
            |r| r.operations[0].chunk_id = "other".into(),
            |r| r.operations[0].source_copy_id = "other".into(),
            |r| r.operations[0].chunk_offset = 0,
            |r| r.operations[0].length = 150 * 1024,
        ];
        for mutate in hot_mutations {
            let mut bad = request.clone();
            mutate(&mut bad);
            assert!(authorizer.authorize("reader", &bad).is_err());
        }
        for entry in authorizer.cache.lock().unwrap().values_mut() {
            entry.deadline = std::time::Instant::now();
        }
        let actor = authorizer.clone();
        let good = request.clone();
        assert!(
            tokio::task::spawn_blocking(move || actor.authorize("reader", &good))
                .await
                .unwrap()
                .is_err()
        );
        validator
            .unavailable
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let mutations: Vec<fn(&mut DfsReadRangesRequest)> = vec![
            |request| {
                request.operations[0].grant.as_mut().unwrap().token =
                    format!("dfs-read-v1:{}", "0".repeat(64))
            },
            |request| {
                request.operations[0]
                    .grant
                    .as_mut()
                    .unwrap()
                    .caller_node_epoch += 1
            },
            |request| {
                request.operations[0]
                    .grant
                    .as_mut()
                    .unwrap()
                    .expires_at_unix_ms = 1
            },
            |request| request.operations[0].grant.as_mut().unwrap().namespace_id = "another".into(),
            |request| request.operations[0].source_copy_id = "other-copy".into(),
            |request| request.operations[0].chunk_id = "other-chunk".into(),
            |request| request.operations[0].chunk_offset = 0,
            |request| request.operations[0].length = 150 * 1024,
        ];
        for mutate in mutations {
            let mut bad = request.clone();
            mutate(&mut bad);
            let actor = authorizer.clone();
            assert!(
                tokio::task::spawn_blocking(move || actor.authorize("reader", &bad))
                    .await
                    .unwrap()
                    .is_err()
            );
        }
        let actor = authorizer.clone();
        let good = request.clone();
        assert!(
            tokio::task::spawn_blocking(move || actor.authorize("intruder", &good))
                .await
                .unwrap()
                .is_err()
        );
        let wrong_receiver = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 2);
        let good = request.clone();
        assert!(
            tokio::task::spawn_blocking(move || wrong_receiver.authorize("reader", &good))
                .await
                .unwrap()
                .is_err()
        );
        // Simulate capability-cache expiry; a Meta restart changes its MAC key,
        // so an old grant cannot recover through a permissive miss fallback.
        for entry in authorizer.cache.lock().unwrap().values_mut() {
            entry.deadline = std::time::Instant::now();
        }
        let restarted = crate::meta::dfs::DfsService::new(meta.store.as_ref().unwrap().clone());
        let replacement = MemoryReadGrantValidator {
            service: restarted,
            runtime: tokio::runtime::Handle::current(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            unavailable: std::sync::atomic::AtomicBool::new(false),
        };
        let restarted_authorizer =
            CachedDfsReadAuthorizer::new(Arc::new(replacement), "receiver".into(), 1);
        assert!(
            tokio::task::spawn_blocking(move || restarted_authorizer.authorize("reader", &request))
                .await
                .unwrap()
                .is_err()
        );
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dfs_read_authority_session_rotation_is_bounded_by_original_cache_lease() {
        use crate::meta::store::{NodeSessionLease, RequestKey};
        let (meta, local, _temp, request) = memory_read_authority_fixture().await;
        let validator = Arc::new(MemoryReadGrantValidator {
            service: meta.dfs.as_ref().unwrap().clone(),
            runtime: tokio::runtime::Handle::current(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            unavailable: std::sync::atomic::AtomicBool::new(false),
        });
        let actor = Arc::new(CachedDfsReadAuthorizer::new(
            validator.clone(),
            "receiver".into(),
            1,
        ));
        let authorized = actor.clone();
        let good = request.clone();
        tokio::task::spawn_blocking(move || authorized.authorize("reader", &good))
            .await
            .unwrap()
            .unwrap();
        meta.store
            .as_ref()
            .unwrap()
            .register_node_session(
                RequestKey::new("reader", "replacement-registration"),
                NodeSessionLease {
                    node_id: "reader".into(),
                    session_id: "new-reader-session".into(),
                    grpc_addr: "http://127.0.0.1:1".into(),
                    data_addr: "http://127.0.0.1:1".into(),
                    rest_addr: "http://127.0.0.1:1".into(),
                    storage_devices: vec![local.device_descriptor().unwrap()],
                    lease_ttl: std::time::Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        // Already validated capabilities retain only their original lease.
        let authorized = actor.clone();
        let good = request.clone();
        tokio::task::spawn_blocking(move || authorized.authorize("reader", &good))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(validator.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        for entry in actor.cache.lock().unwrap().values_mut() {
            entry.deadline = std::time::Instant::now();
        }
        assert!(
            tokio::task::spawn_blocking(move || actor.authorize("reader", &request))
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(validator.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dfs_read_grants_meta_and_peer_grpc_stream_real_authority() {
        use afs_protocol::node_data::dfs_chunks_client::DfsChunksClient;
        use tokio_stream::StreamExt;
        let (meta, local, _temp, request) = memory_read_authority_fixture().await;
        let meta_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let meta_endpoint = format!("http://{}", meta_listener.local_addr().unwrap());
        let meta_server = tokio::spawn(async move {
            Server::builder()
                .add_service(afs_protocol::meta::dfs_meta_server::DfsMetaServer::new(
                    crate::meta::rpc::DfsMetaRpc(meta),
                ))
                .serve_with_incoming(TcpListenerStream::new(meta_listener))
                .await
                .unwrap();
        });
        let adapter = Arc::new(
            super::super::meta::GrpcDfsMeta::new(
                &meta_endpoint,
                "receiver".into(),
                "session-receiver".into(),
                crate::dfs::NamespaceId::new("default"),
                std::time::Duration::from_secs(5),
                afs_transport::TlsConfig::Disabled,
            )
            .unwrap(),
        );
        let service = make_dfs_chunks_server(
            Some(local),
            Arc::new(AllowDfsTestPeer),
            Arc::new(CachedDfsReadAuthorizer::new(adapter, "receiver".into(), 1)),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_endpoint = format!("http://{}", listener.local_addr().unwrap());
        let peer_server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let mut client = DfsChunksClient::connect(peer_endpoint).await.unwrap();
        let mut stream = client
            .read_ranges(request.clone())
            .await
            .unwrap()
            .into_inner();
        let mut received = Vec::new();
        let mut completions = 0;
        while let Some(frame) = stream.next().await {
            match frame.unwrap().body.unwrap() {
                dfs_read_ranges_frame::Body::Data(data) => received.extend(data),
                dfs_read_ranges_frame::Body::Completion(_) => completions += 1,
                dfs_read_ranges_frame::Body::Compact(reply) => {
                    assert_eq!(reply.operation_index, 0);
                    assert_eq!(
                        reply.range_checksum.as_slice(),
                        blake3::hash(&reply.data).as_bytes()
                    );
                    received.extend(reply.data);
                    completions += 1;
                }
                _ => {}
            }
        }
        assert_eq!(received, vec![9; 75_000]);
        assert_eq!(completions, 1);
        let mut forged = request;
        forged.operations[0].grant.as_mut().unwrap().token =
            format!("dfs-read-v1:{}", "0".repeat(64));
        assert_eq!(
            client.read_ranges(forged).await.unwrap_err().code(),
            tonic::Code::PermissionDenied
        );
        peer_server.abort();
        meta_server.abort();
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn dfs_rdma_packed_ranges_reject_gaps_capacity_and_wrong_caller() {
        let mut request = dfs_read_request("chunk".into(), 75_000);
        assert_eq!(validate_packed_dfs_read(&request).unwrap(), 75_000);
        assert_eq!(dfs_read_caller_epoch("reader", &request).unwrap(), 1);
        assert!(dfs_read_caller_epoch("intruder", &request).is_err());
        request.operations[0].destination_offset = 1;
        assert!(validate_packed_dfs_read(&request).is_err());
        request.operations[0].destination_offset = 0;
        request.operations[0].length = crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64 + 1;
        assert!(validate_packed_dfs_read(&request).is_err());
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires explicit Linux RXE device; product payload, not diagnostic NodeData"]
    async fn dfs_product_rdma_replica_and_read_real_verbs() {
        dfs_product_rdma_replica_and_read_real_verbs_with_mode(
            super::super::peer::DataMode::Rdma,
            true,
        )
        .await;
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires explicit Linux RXE device; product payload, not diagnostic NodeData"]
    async fn dfs_auto_prefers_rdma_replica_and_read_real_verbs() {
        dfs_product_rdma_replica_and_read_real_verbs_with_mode(
            super::super::peer::DataMode::Auto,
            true,
        )
        .await;
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires Linux RXE client; receiver deliberately has no RDMA device"]
    async fn dfs_auto_unsupported_peer_uses_grpc_before_dispatch() {
        dfs_product_rdma_replica_and_read_real_verbs_with_mode(
            super::super::peer::DataMode::Auto,
            false,
        )
        .await;
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires Linux RXE client; receiver deliberately has no RDMA device"]
    async fn dfs_required_unsupported_peer_never_uses_grpc() {
        dfs_product_rdma_replica_and_read_real_verbs_with_mode(
            super::super::peer::DataMode::Rdma,
            false,
        )
        .await;
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "requires Linux RXE, GDB posted checkpoint, and explicit resume marker"]
    async fn dfs_replica_posted_rdma_deadline_has_unknown_outcome() {
        use crate::node::chunk::StagedChunk;
        use crate::node::rpc::peer::{DfsRdmaPool, PeerConnectionPool, make_replica_data_plane};
        let device = std::env::var("AFS_TEST_RDMA_DEVICE")
            .expect("set AFS_TEST_RDMA_DEVICE to the Linux RXE device");
        let checkpoint = std::path::PathBuf::from(
            std::env::var("AFS_TEST_RDMA_CHECKPOINT")
                .expect("set AFS_TEST_RDMA_CHECKPOINT to the GDB evidence directory"),
        );
        save_dfs_rdma_resources(&checkpoint, "baseline");

        let (meta, local, _temp, _read) = memory_read_authority_fixture().await;
        let validator = Arc::new(MemoryReadGrantValidator {
            service: meta.dfs.as_ref().unwrap().clone(),
            runtime: tokio::runtime::Handle::current(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            unavailable: std::sync::atomic::AtomicBool::new(false),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("rdma-deadline"),
            dfs_deadline_payload(4096),
        );
        let target = crate::dfs::ReplicaTarget {
            node_id: "receiver".into(),
            node_epoch: 1,
            data_endpoint: endpoint.clone(),
            device: local.device_descriptor().unwrap(),
        };
        let op = crate::node::replication::ReplicaPeerOp {
            chunk_id: staged.chunk.id.clone(),
            placement_revision: 1,
            placement_epoch: 1,
            replica_group_id: crate::dfs::ReplicaGroupId::new("deadline-fixture"),
            initiator_node_id: "reader".into(),
            initiator_node_epoch: 1,
            ordered_targets: vec![target.clone()],
            sync_target_count: 1,
            target_index: 0,
            target,
            chain_tail: vec![],
            repair_claim: None,
        };
        let registry = afs_metrics::Registry::new();
        let metrics = DfsPayloadMetrics::register(&registry).unwrap();
        let rdma_registry = super::super::control::RdmaSessionRegistry::new(Some(device.clone()));
        let service = make_dfs_chunks_server_with_transport(
            Some(local.clone()),
            Arc::new(AllowDfsTestPeer),
            Arc::new(CachedDfsReadAuthorizer::new(
                validator,
                "receiver".into(),
                1,
            )),
            Arc::new(FixtureReplicaGrant(op.clone())),
            None,
            std::time::Duration::from_secs(10),
            DfsChunkTransportResources {
                rdma_sessions: rdma_registry.clone(),
                payload_metrics: Some(metrics.clone()),
            },
        );
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let peers = Arc::new(
            PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::Disabled,
                16,
            )
            .unwrap(),
        );
        let pool = Arc::new(
            DfsRdmaPool::new(peers.clone(), device, std::time::Duration::from_secs(4)).unwrap(),
        );
        let writer = make_replica_data_plane(
            super::super::peer::DataMode::Rdma,
            peers,
            std::time::Duration::from_secs(4),
            Some(pool.clone()),
        )
        .unwrap();
        let saved = staged.clone();
        let saved_op = op.clone();
        let mut write_task = tokio::task::spawn_blocking(move || {
            writer
                .put_peer_replica(&saved_op, &saved)
                .map(|acks| acks.len())
        });
        wait_for_dfs_rdma_checkpoint(&checkpoint.join("posted")).await;
        save_dfs_rdma_resources(&checkpoint, "connected");

        let identity = super::super::control::PeerSessionIdentity::new("reader".into(), 1).unwrap();
        let session = rdma_registry.session_for(1, &identity).await.unwrap();
        let retained_endpoint = Arc::downgrade(&session.endpoint);
        drop(session);
        let error = tokio::time::timeout(std::time::Duration::from_secs(10), &mut write_task)
            .await
            .expect("posted DFS replica write must return the caller deadline")
            .unwrap()
            .expect_err("posted DFS replica write must not report success before resume");
        std::fs::write(
            checkpoint.join("caller-error.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "code": error.code().raw(),
                "kind": format!("{:?}", error.kind()),
                "message": error.message(),
            }))
            .unwrap(),
        )
        .expect("save the actual DFS deadline classification");
        assert_dfs_replica_deadline(&error);
        assert!(
            dfs_chunk_content(&local, &staged).is_none(),
            "replica must not be persisted while the native worker is paused"
        );
        assert_eq!(dfs_payload_bytes(&metrics, "grpc", "recv", "replica"), 0);
        assert_eq!(dfs_payload_bytes(&metrics, "rdma", "recv", "replica"), 0);
        close_dfs_rdma_session(&endpoint, "reader", 1, 1).await;
        wait_for_dfs_rdma_lookup_removed(&rdma_registry, 1, &identity).await;
        {
            let endpoint = retained_endpoint
                .upgrade()
                .expect("blocked worker retains server endpoint after lookup removal");
            assert!(
                endpoint.try_lock().is_err(),
                "native worker still owns the server endpoint while paused"
            );
        }
        save_dfs_rdma_resources(&checkpoint, "closed-paused");
        eprintln!(
            "AFS_DFS_REPLICA_DEADLINE caller=TIMEOUT lookup=STALE server=RETAINED content=PENDING replay=ABSENT client=RETAINED close=EXPLICIT"
        );
        std::fs::write(checkpoint.join("resume"), b"resume native worker\n")
            .expect("write GDB resume marker");

        wait_for_dfs_endpoint_release(retained_endpoint).await;
        let bytes = wait_for_dfs_exact_content(&local, &staged).await;
        assert_eq!(bytes, staged.bytes());
        assert_eq!(dfs_payload_bytes(&metrics, "rdma", "recv", "replica"), 4096);
        assert_eq!(dfs_payload_bytes(&metrics, "grpc", "recv", "replica"), 0);
        drop(pool);
        save_dfs_rdma_resources(&checkpoint, "drained");
        eprintln!(
            "AFS_DFS_REPLICA_DEADLINE drain=COMPLETE content=EXACT endpoint=RELEASED replay=ABSENT client=EXPLICITLY_RETIRED"
        );
        server.abort();
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    fn dfs_deadline_payload(len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| ((index.wrapping_mul(41).wrapping_add(index / 127)) & 0xff) as u8)
            .collect()
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    async fn wait_for_dfs_rdma_checkpoint(path: &std::path::Path) {
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            while !path.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("GDB must prove DFS replica data WQE posted before the caller deadline");
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    fn save_dfs_rdma_resources(directory: &std::path::Path, phase: &str) {
        let tids: Vec<u32> = std::fs::read_dir("/proc/self/task")
            .expect("Linux thread inventory")
            .map(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .parse()
                    .unwrap()
            })
            .collect();
        std::fs::write(
            directory.join(format!("{phase}-process.json")),
            serde_json::to_vec(&serde_json::json!({"pid": std::process::id(), "tids": tids}))
                .unwrap(),
        )
        .expect("save process identity");
        for kind in ["qp", "mr", "cq", "pd", "ctx"] {
            let output = std::process::Command::new("rdma")
                .args(["-j", "resource", "show", kind])
                .output()
                .expect("rdma resource inventory");
            assert!(output.status.success(), "RDMA {kind} inventory failed");
            std::fs::write(
                directory.join(format!("{phase}-{kind}.json")),
                output.stdout,
            )
            .expect("save exact RDMA resources");
        }
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    fn assert_dfs_replica_deadline(error: &afs_error::Error) {
        assert!(
            error.kind() == afs_error::ErrorKind::DeadlineExceeded
                || (error.code() == afs_error::CLIENT_REMOTE_STATUS
                    && error.kind() == afs_error::ErrorKind::Cancelled
                    && error.message() == "Timeout expired"),
            "posted DFS replica write must fail with the request deadline, got {error:?}"
        );
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    async fn close_dfs_rdma_session(
        endpoint: &str,
        caller_node_id: &str,
        session_id: u64,
        peer_node_epoch: u64,
    ) {
        let mut client = afs_protocol::node_data::dfs_chunks_client::DfsChunksClient::connect(
            endpoint.to_owned(),
        )
        .await
        .expect("connect DFS close client");
        let mut request = Request::new(DfsCloseRdmaRequest {
            session_id,
            peer_node_epoch,
            caller_node_id: caller_node_id.to_owned(),
        });
        request.set_timeout(std::time::Duration::from_secs(4));
        client
            .close_rdma(request)
            .await
            .expect("explicit fixture close removes registry lookup while native worker is paused");
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    async fn wait_for_dfs_rdma_lookup_removed(
        registry: &super::super::control::RdmaSessionRegistry,
        session_id: u64,
        identity: &super::super::control::PeerSessionIdentity,
    ) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                match registry.session_for(session_id, identity).await {
                    Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
                    Err(status) => {
                        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                        if status.message() == "RDMA session poisoned" {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            continue;
                        }
                        assert!(
                            status.message().contains("unknown RDMA session"),
                            "stale lookup must prove removal, not a live poisoned session: {status:?}"
                        );
                        break;
                    }
                }
            }
        })
        .await
        .expect("explicit DFS client teardown must remove the server registry lookup");
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    async fn wait_for_dfs_endpoint_release(
        endpoint: std::sync::Weak<super::super::control::RdmaServerEndpoint>,
    ) {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while endpoint.upgrade().is_some() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("server endpoint must release after the paused native transfer resolves");
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    async fn wait_for_dfs_exact_content(
        local: &LocalChunkStore,
        staged: &crate::node::chunk::StagedChunk,
    ) -> Vec<u8> {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(bytes) = dfs_chunk_content(local, staged) {
                    return bytes;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("resumed DFS RDMA READ should persist the exact staged Chunk")
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    fn dfs_chunk_content(
        local: &LocalChunkStore,
        staged: &crate::node::chunk::StagedChunk,
    ) -> Option<Vec<u8>> {
        let mut bytes = vec![0; staged.bytes().len()];
        match local.read_at(&staged.chunk.id, 0, &mut bytes) {
            Ok(read) => {
                assert_eq!(read, bytes.len());
                Some(bytes)
            }
            Err(error) => {
                assert!(
                    error.message().contains("not cataloged"),
                    "unexpected replica read error: {error:?}"
                );
                None
            }
        }
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    fn dfs_payload_bytes(
        metrics: &DfsPayloadMetrics,
        transport: &str,
        direction: &str,
        purpose: &str,
    ) -> u64 {
        metrics
            .bytes
            .with_label_values(&[transport, direction, purpose])
            .get()
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    async fn dfs_product_rdma_replica_and_read_real_verbs_with_mode(
        mode: super::super::peer::DataMode,
        server_rdma: bool,
    ) {
        use crate::node::chunk::StagedChunk;
        use crate::node::dfs_read::{ChunkReadOp, PeerReadBatch, ResolvedReadOp};
        use crate::node::rpc::peer::{
            DfsRdmaPool, PeerConnectionPool, make_chunk_transfer, make_replica_data_plane,
        };
        let device = std::env::var("AFS_TEST_RDMA_DEVICE")
            .expect("set AFS_TEST_RDMA_DEVICE to the Linux RXE device");
        let (meta, local, _temp, read) = memory_read_authority_fixture().await;
        let validator = Arc::new(MemoryReadGrantValidator {
            service: meta.dfs.as_ref().unwrap().clone(),
            runtime: tokio::runtime::Handle::current(),
            calls: std::sync::atomic::AtomicUsize::new(0),
            unavailable: std::sync::atomic::AtomicBool::new(false),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("rdma-product"),
            vec![37; crate::node::chunk::MAX_STAGED_CHUNK_BYTES],
        );
        let target = crate::dfs::ReplicaTarget {
            node_id: "receiver".into(),
            node_epoch: 1,
            data_endpoint: endpoint.clone(),
            device: local.device_descriptor().unwrap(),
        };
        let op = crate::node::replication::ReplicaPeerOp {
            chunk_id: staged.chunk.id.clone(),
            placement_revision: 1,
            placement_epoch: 1,
            replica_group_id: crate::dfs::ReplicaGroupId::new("fixture"),
            initiator_node_id: "reader".into(),
            initiator_node_epoch: 1,
            ordered_targets: vec![target.clone()],
            sync_target_count: 1,
            target_index: 0,
            target,
            chain_tail: vec![],
            repair_claim: None,
        };
        let registry = afs_metrics::Registry::new();
        let metrics = DfsPayloadMetrics::register(&registry).unwrap();
        let service = make_dfs_chunks_server_with_transport(
            Some(local.clone()),
            Arc::new(AllowDfsTestPeer),
            Arc::new(CachedDfsReadAuthorizer::new(
                validator,
                "receiver".into(),
                1,
            )),
            Arc::new(FixtureReplicaGrant(op.clone())),
            None,
            std::time::Duration::from_secs(10),
            DfsChunkTransportResources {
                rdma_sessions: super::super::control::RdmaSessionRegistry::new(
                    server_rdma.then(|| device.clone()),
                ),
                payload_metrics: Some(metrics.clone()),
            },
        );
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let peers = Arc::new(
            PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::Disabled,
                16,
            )
            .unwrap(),
        );
        let pool = Arc::new(
            DfsRdmaPool::new(peers.clone(), device, std::time::Duration::from_secs(10)).unwrap(),
        );
        let writer = make_replica_data_plane(
            mode,
            peers.clone(),
            std::time::Duration::from_secs(10),
            Some(pool.clone()),
        )
        .unwrap();
        let saved = staged.clone();
        let saved_op = op.clone();
        let acks = tokio::task::spawn_blocking(move || {
            if !server_rdma && mode == super::super::peer::DataMode::Rdma {
                let error = writer.put_peer_replica(&saved_op, &saved).unwrap_err();
                assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
                return None;
            }
            let acks = writer.put_peer_replica(&saved_op, &saved).unwrap();
            writer.put_peer_replica(&saved_op, &saved).unwrap();
            Some(acks)
        })
        .await
        .unwrap();
        if let Some(acks) = &acks {
            assert_eq!(
                acks[0].persisted_bytes,
                crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
            );
        }
        let plane = if server_rdma { "rdma" } else { "grpc" };
        let other_plane = if server_rdma { "grpc" } else { "rdma" };
        let wire = read.operations[0].grant.as_ref().unwrap();
        let item = &read.operations[0];
        let batch = PeerReadBatch {
            file_version_id: crate::dfs::FileVersionId::new(read.file_version_id.clone()),
            layout_root_id: crate::dfs::LayoutRootId::new(read.layout_root_id.clone()),
            operations: vec![ResolvedReadOp {
                op: ChunkReadOp {
                    chunk_id: crate::dfs::ChunkId::new(item.chunk_id.clone()),
                    chunk_offset: item.chunk_offset,
                    length: item.length,
                    output_offset: 0,
                },
                source: crate::dfs::SourceCandidate {
                    copy_id: crate::dfs::CopyId::new(item.source_copy_id.clone()),
                    chunk_id: crate::dfs::ChunkId::new(item.chunk_id.clone()),
                    role: crate::dfs::CopyRole::DurableReplica,
                    state: crate::dfs::CopyState::Ready,
                    location: crate::dfs::CopyLocation::Node {
                        node_id: "receiver".into(),
                        node_epoch: 1,
                        device_id: op.target.device.device_id.clone(),
                        device_epoch: op.target.device.device_epoch,
                        catalog_revision: op.target.device.catalog_revision,
                    },
                    data_endpoint: Some(endpoint.clone()),
                    load_hint: 0,
                    read_grant: crate::dfs::DfsReadGrant {
                        namespace_id: crate::dfs::NamespaceId::new(wire.namespace_id.clone()),
                        file_version_id: crate::dfs::FileVersionId::new(
                            wire.file_version_id.clone(),
                        ),
                        layout_root_id: crate::dfs::LayoutRootId::new(wire.layout_root_id.clone()),
                        caller_node_id: wire.caller_node_id.clone(),
                        caller_node_epoch: wire.caller_node_epoch,
                        expires_at_unix_ms: wire.expires_at_unix_ms,
                        fence: wire.fence,
                        token: wire.token.clone(),
                    },
                },
            }],
        };
        if acks.is_none() {
            let transfer =
                make_chunk_transfer(mode, peers, std::time::Duration::from_secs(10), Some(pool))
                    .unwrap();
            let bytes = tokio::task::spawn_blocking(move || {
                let mut bytes = vec![0; 75_000];
                let error = transfer.read_ranges(&batch, &mut bytes).unwrap_err();
                assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNSUPPORTED);
                bytes
            })
            .await
            .unwrap();
            assert_eq!(bytes, vec![0; 75_000]);
            for transport in ["rdma", "grpc"] {
                assert_eq!(
                    metrics
                        .bytes
                        .with_label_values(&[transport, "recv", "replica"])
                        .get(),
                    0
                );
                assert_eq!(
                    metrics
                        .bytes
                        .with_label_values(&[transport, "send", "read"])
                        .get(),
                    0
                );
            }
            println!(
                "DFS_REQUIRED_UNSUPPORTED write=REJECTED read=REJECTED error=NODE_TRANSFER_UNSUPPORTED grpc_payload_bytes=0 rdma_payload_bytes=0"
            );
            server.abort();
            return;
        }
        let transfer =
            make_chunk_transfer(mode, peers, std::time::Duration::from_secs(10), Some(pool))
                .unwrap();
        let bytes = tokio::task::spawn_blocking(move || {
            let mut bytes = vec![0; 75_000];
            transfer.read_ranges(&batch, &mut bytes).unwrap();
            bytes
        })
        .await
        .unwrap();
        assert_eq!(bytes, vec![9; 75_000]);
        assert_eq!(
            metrics
                .bytes
                .with_label_values(&[plane, "recv", "replica"])
                .get(),
            2 * crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64
        );
        assert_eq!(
            metrics
                .bytes
                .with_label_values(&[plane, "send", "read"])
                .get(),
            75_000
        );
        assert_eq!(
            metrics
                .bytes
                .with_label_values(&[other_plane, "recv", "replica"])
                .get(),
            0
        );
        assert_eq!(
            metrics
                .bytes
                .with_label_values(&[other_plane, "send", "read"])
                .get(),
            0
        );
        let mut client =
            afs_protocol::node_data::dfs_chunks_client::DfsChunksClient::connect(endpoint)
                .await
                .unwrap();
        let mut forged = read;
        forged.operations[0].grant.as_mut().unwrap().token = "forged".into();
        assert_eq!(
            client
                .negotiate_rdma(DfsNegotiateRdmaRequest {
                    client_info: vec![],
                    capacity: crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u32,
                    handshake_version: super::super::control::RDMA_HANDSHAKE_VERSION,
                    authority: Some(
                        afs_protocol::node_data::dfs_negotiate_rdma_request::Authority::ReadRequest(
                            forged
                        )
                    )
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            metrics
                .bytes
                .with_label_values(&[plane, "send", "read"])
                .get(),
            75_000
        );
        println!(
            "DFS_PRODUCT_TRANSPORT mode={mode:?} plane={plane} replica_bytes={} read_bytes=75000 other_payload_bytes=0 exact_retry=PASS forged_grant=DENIED",
            2 * crate::node::chunk::MAX_STAGED_CHUNK_BYTES
        );
        server.abort();
    }

    #[cfg(feature = "dfs")]
    struct AllowDfsTestGrant;
    #[cfg(feature = "dfs")]
    impl DfsReadAuthorizer for AllowDfsTestGrant {
        fn authorize(&self, peer: &str, request: &DfsReadRangesRequest) -> afs_error::Result<()> {
            if peer == "reader"
                && request.operations.iter().all(|op| {
                    op.grant
                        .as_ref()
                        .is_some_and(|grant| grant.token == "fixture-grant")
                })
            {
                Ok(())
            } else {
                Err(afs_error::Error::coded(
                    afs_error::NODE_TRANSFER_INVALID,
                    "fixture grant rejected",
                ))
            }
        }
    }

    #[cfg(feature = "dfs")]
    fn dfs_read_request(chunk_id: String, length: u64) -> DfsReadRangesRequest {
        DfsReadRangesRequest {
            include_blake3_outboard: false,
            read_id: "read".into(),
            attempt_id: "attempt".into(),
            file_version_id: "version".into(),
            layout_root_id: "layout".into(),
            operations: vec![afs_protocol::node_data::DfsChunkReadOp {
                operation_index: 0,
                chunk_id,
                chunk_offset: 0,
                length,
                destination_offset: 0,
                source_copy_id: "copy".into(),
                grant: Some(afs_protocol::node_data::DfsReadGrant {
                    namespace_id: "namespace".into(),
                    file_version_id: "version".into(),
                    layout_root_id: "layout".into(),
                    caller_node_id: "reader".into(),
                    caller_node_epoch: 1,
                    expires_at_unix_ms: u64::MAX,
                    fence: 1,
                    token: "fixture-grant".into(),
                }),
            }],
        }
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn dfs_shared_data_frame_preserves_protobuf_wire_encoding() {
        use prost::Message;
        let frame = DfsReadRangesFrame {
            body: Some(dfs_read_ranges_frame::Body::Data(
                prost::bytes::Bytes::from_static(b"abc"),
            )),
        };
        // The original bytes field is still field 2, length-delimited.
        let wire = [0x12, 0x03, b'a', b'b', b'c'];
        assert_eq!(frame.encode_to_vec(), wire);
        let decoded = DfsReadRangesFrame::decode(&wire[..]).unwrap();
        assert_eq!(decoded, frame);
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn dfs_corrupt_range_publishes_no_grpc_frames_or_rdma_completions() {
        use crate::node::chunk::StagedChunk;
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::read_work::ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let local = LocalChunkStore::open(temp.path(), "node")
            .unwrap()
            .with_read_work_metrics(metrics.clone());
        let staged = StagedChunk::new(crate::dfs::OperationId::new("corrupt"), vec![42; 150_000]);
        local.put(staged.clone()).unwrap();
        let path = temp.path().join("chunks").join(&staged.chunk.id.0);
        let mut damaged = staged.bytes().to_vec();
        damaged[149_999] ^= 1;
        std::fs::write(path, damaged).unwrap();
        let mut request = dfs_read_request(staged.chunk.id.0.clone(), 75_000);
        request.operations[0].chunk_offset = 1;
        validate_dfs_read_request(&request).unwrap();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        assert_eq!(
            stream_dfs_ranges(&local, &request, &sender, None)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
        assert!(receiver.try_recv().is_err());
        assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        let served = snapshot
            .records
            .iter()
            .find(|record| record.stage == crate::node::read_work::ReadDiagnosticStage::PeerServe)
            .unwrap();
        assert!(served.completed && !served.success);
        assert_eq!(
            served.first_data_ns, 0,
            "corrupt data never enters the channel"
        );
        #[cfg(feature = "rdma")]
        assert_eq!(
            read_packed_dfs_ranges(&local, &request, 75_000)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn p2_r4_verified_prefix_precedes_later_corruption_without_completion() {
        use crate::node::chunk::StagedChunk;
        let temp = tempfile::tempdir().unwrap();
        let local = LocalChunkStore::open(temp.path(), "node").unwrap();
        let body = vec![42; 3 * 32 * 1024];
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("r4-late-corrupt"),
            body.clone(),
        );
        local.put(staged.clone()).unwrap();
        // Persist a valid standard-root sidecar before corrupting only the last
        // group. Healthy earlier groups must not wait for a whole-object scan.
        drop(local.open_verified(&staged.chunk.id).unwrap());
        let mut damaged = body.clone();
        *damaged.last_mut().unwrap() ^= 1;
        std::fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), damaged).unwrap();
        let mut request = dfs_read_request(staged.chunk.id.0.clone(), body.len() as u64);
        request.include_blake3_outboard = true;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        let result = std::thread::scope(|scope| {
            let producer = scope.spawn(|| stream_dfs_ranges(&local, &request, &sender, None));
            let mut received = Vec::new();
            let mut completed = false;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while !producer.is_finished() || !receiver.is_empty() {
                if std::time::Instant::now() >= deadline {
                    receiver.close();
                    panic!("bounded verified-prefix producer did not finish");
                }
                match receiver.try_recv() {
                    Ok(Ok(frame)) => match frame.body.unwrap() {
                        dfs_read_ranges_frame::Body::Data(data) => {
                            received.extend_from_slice(&data)
                        }
                        dfs_read_ranges_frame::Body::Completion(_) => completed = true,
                        dfs_read_ranges_frame::Body::Header(header) => {
                            assert!(!header.blake3_outboard.is_empty())
                        }
                        dfs_read_ranges_frame::Body::Compact(_) => {
                            panic!("outboard requests must keep incremental proof delivery")
                        }
                    },
                    Ok(Err(error)) => panic!("direct producer must return its error: {error}"),
                    Err(_) => std::thread::yield_now(),
                }
            }
            (producer.join().unwrap(), received, completed)
        });
        assert_eq!(result.0.unwrap_err().code(), tonic::Code::DataLoss);
        assert_eq!(
            result.1.len(),
            2 * 32 * 1024,
            "only completely verified prefix groups may leave A"
        );
        assert!(result.1 == body[..result.1.len()]);
        assert!(!result.2, "a partial stream must never publish Completion");
        assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn p2_r4_corrupt_first_group_sends_no_unverified_data() {
        use crate::node::chunk::StagedChunk;
        let temp = tempfile::tempdir().unwrap();
        let local = LocalChunkStore::open(temp.path(), "node").unwrap();
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("r4-first-corrupt"),
            vec![42; 3 * 32 * 1024],
        );
        local.put(staged.clone()).unwrap();
        drop(local.open_verified(&staged.chunk.id).unwrap());
        let mut damaged = staged.bytes().to_vec();
        damaged[0] ^= 1;
        std::fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), damaged).unwrap();
        let mut request = dfs_read_request(staged.chunk.id.0.clone(), staged.chunk.length);
        request.include_blake3_outboard = true;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
        assert_eq!(
            stream_dfs_ranges(&local, &request, &sender, None)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
        while let Ok(frame) = receiver.try_recv() {
            assert!(matches!(
                frame.unwrap().body.unwrap(),
                dfs_read_ranges_frame::Body::Header(_)
            ));
        }
        assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn dfs_whole_digest_reuse_preserves_checksums_and_partial_hash_work() {
        use crate::node::chunk::StagedChunk;
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::read_work::ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let local = LocalChunkStore::open(temp.path(), "node")
            .unwrap()
            .with_read_work_metrics(metrics.clone());
        let bytes: Vec<_> = (0..150_001).map(|n| (n % 251) as u8).collect();
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("stream-whole-proof"),
            bytes.clone(),
        );
        local.put(staged.clone()).unwrap();
        let hash_bytes = || {
            afs_metrics::encode_text(&registry)
                .unwrap()
                .lines()
                .find_map(|line| {
                    line.strip_prefix("afs_dfs_read_hash_bytes_total{stage=\"stream_range_hash\"} ")
                })
                .unwrap()
                .parse::<u64>()
                .unwrap()
        };
        for (offset, length, want_proof) in [
            (0, bytes.len(), false),
            (0, bytes.len(), true),
            (1, 75_000, false),
            (1, 75_000, true),
        ] {
            let before = hash_bytes();
            let mut request = dfs_read_request(staged.chunk.id.0.clone(), length as u64);
            request.include_blake3_outboard = want_proof;
            request.operations[0].chunk_offset = offset as u64;
            let (sender, mut receiver) = tokio::sync::mpsc::channel(8);
            std::thread::scope(|scope| {
                let producer = scope.spawn(|| {
                    let result = stream_dfs_ranges(&local, &request, &sender, None);
                    drop(sender);
                    result
                });
                let mut received = Vec::new();
                let mut header_count = 0;
                let mut completions = 0;
                let mut compact = 0;
                while let Some(frame) = receiver.blocking_recv() {
                    match frame.unwrap().body.unwrap() {
                        dfs_read_ranges_frame::Body::Header(header) => {
                            header_count += 1;
                            assert_eq!(header.chunk_offset, offset as u64);
                            assert_eq!(header.length, length as u64);
                            if want_proof && offset == 0 {
                                assert!(!header.blake3_outboard.is_empty());
                                crate::node::chunk::outboard::ChunkOutboard::validate_encoded_shape(
                                &staged.chunk,
                                &header.blake3_outboard,
                            )
                            .unwrap();
                            } else {
                                assert!(
                                    header.blake3_outboard.is_empty(),
                                    "ordinary/partial reads keep their original wire contract"
                                );
                            }
                        }
                        dfs_read_ranges_frame::Body::Data(data) => received.extend(data),
                        dfs_read_ranges_frame::Body::Compact(reply) => {
                            assert!(!want_proof);
                            assert_eq!(reply.operation_index, 0);
                            assert_eq!(reply.data.len(), length);
                            assert_eq!(
                                reply.range_checksum.as_slice(),
                                blake3::hash(&reply.data).as_bytes()
                            );
                            received.extend(reply.data);
                            compact += 1;
                        }
                        dfs_read_ranges_frame::Body::Completion(completion) => {
                            completions += 1;
                            assert_eq!(completion.transferred_bytes, length as u64);
                            assert_eq!(
                                completion.range_checksum_algorithm,
                                afs_protocol::node_data::DfsDigestAlgorithm::Blake3 as i32
                            );
                            assert_eq!(
                                completion.range_checksum,
                                blake3::hash(&received).as_bytes().to_vec()
                            );
                        }
                    }
                }
                let expected_compact = usize::from(!want_proof && length <= 128 * 1024);
                assert_eq!(compact, expected_compact);
                assert_eq!(header_count, 1 - expected_compact);
                assert_eq!(completions, 1 - expected_compact);
                assert_eq!(received, bytes[offset..offset + length]);
                assert_eq!(
                    hash_bytes() - before,
                    if offset == 0 { 0 } else { length as u64 }
                );
                producer.join().unwrap().unwrap();
            });
        }
        assert_eq!(dfs_reuse_scan_bytes(&registry), bytes.len() as u64);
        let diagnostic = metrics.diagnostics_snapshot().unwrap();
        assert_eq!(diagnostic.overflow, 0);
        let served = diagnostic
            .records
            .iter()
            .filter(|record| record.stage == crate::node::read_work::ReadDiagnosticStage::PeerServe)
            .collect::<Vec<_>>();
        assert_eq!(served.len(), 4);
        for record in served {
            assert!(record.completed && record.success);
            assert!(record.first_data_ns >= record.start_ns);
            assert!(record.end_ns >= record.first_data_ns);
        }
        let enqueue = diagnostic
            .coarse_phases
            .iter()
            .find(|phase| phase.phase == "peer_first_frame_enqueue")
            .unwrap();
        assert_eq!(enqueue.calls, 4);
    }

    #[cfg(feature = "dfs")]
    fn dfs_reuse_test_service(
        local: Arc<LocalChunkStore>,
        authorizer: Arc<dyn DfsReadAuthorizer>,
    ) -> DfsChunksService {
        DfsChunksService {
            local_chunks: Some(local),
            authenticator: Arc::new(AllowDfsTestPeer),
            authorizer,
            replica_authorizer: Arc::new(DenyDfsReplicaAuthorizer),
            replica_data_plane: None,
            replica_permits: Arc::new(tokio::sync::Semaphore::new(8)),
            read_permits: Arc::new(tokio::sync::Semaphore::new(8)),
            read_sessions: Arc::new(tokio::sync::Semaphore::new(8)),
            replica_timeout: std::time::Duration::from_secs(30),
            rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
            payload_metrics: None,
        }
    }

    #[cfg(feature = "dfs")]
    fn dfs_reuse_scan_bytes(registry: &afs_metrics::Registry) -> u64 {
        afs_metrics::encode_text(registry)
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("afs_dfs_read_scan_bytes_total "))
            .unwrap()
            .parse()
            .unwrap()
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn p2_t1_native_read_waits_for_an_existing_rpc_slot() {
        use crate::node::chunk::StagedChunk;
        use std::{future::Future, task::Poll};
        use tokio_stream::StreamExt;

        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node").unwrap());
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("t1-rpc-admission"),
            vec![37; 65536],
        );
        local.put(staged.clone()).unwrap();
        let service = dfs_reuse_test_service(local, Arc::new(AllowDfsTestGrant));
        let held = service
            .read_permits
            .clone()
            .acquire_many_owned(8)
            .await
            .unwrap();
        let request = dfs_read_request(staged.chunk.id.0.clone(), staged.chunk.length);
        let mut pending = Box::pin(service.read_ranges(Request::new(request)));
        // Poll exactly once while all eight real slots are held. This tests
        // admission behavior without inferring it from scheduler/timing noise.
        let waiting =
            std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx).is_pending())).await;
        assert!(
            waiting,
            "temporary slot pressure must wait within the fixed admission deadline"
        );
        drop(held);
        let mut stream = pending.await.unwrap().into_inner();
        let mut received = Vec::new();
        let mut completions = 0;
        while let Some(frame) = stream.next().await {
            match frame.unwrap().body.unwrap() {
                dfs_read_ranges_frame::Body::Data(data) => received.extend(data),
                dfs_read_ranges_frame::Body::Completion(_) => completions += 1,
                dfs_read_ranges_frame::Body::Header(_) => {}
                dfs_read_ranges_frame::Body::Compact(reply) => {
                    received.extend(reply.data);
                    completions += 1;
                }
            }
        }
        assert_eq!(received, staged.bytes());
        assert_eq!(completions, 1);
        assert_eq!(service.read_permits.available_permits(), 8);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn p2_t1_zero_wait_rpc_slots_fail_without_queueing() {
        use std::{future::Future, task::Poll};
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node")
                .unwrap()
                .with_peer_serve_wait(std::time::Duration::ZERO)
                .unwrap(),
        );
        let service = dfs_reuse_test_service(local, Arc::new(AllowDfsTestGrant));
        let _held = service
            .read_permits
            .clone()
            .acquire_many_owned(8)
            .await
            .unwrap();
        let mut pending =
            Box::pin(service.read_ranges(Request::new(dfs_read_request("chunk".into(), 1))));
        let result = std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx))).await;
        match result {
            Poll::Ready(Err(error)) => assert_eq!(error.code(), tonic::Code::ResourceExhausted),
            _ => panic!("zero admission allowance must not queue behind occupied RPC slots"),
        }
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn p2_t1_rpc_slot_timeout_and_node_stop_return_no_stream() {
        use std::{future::Future, task::Poll};
        for stop in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let local = Arc::new(LocalChunkStore::open(temp.path(), "node").unwrap());
            let service = dfs_reuse_test_service(local.clone(), Arc::new(AllowDfsTestGrant));
            let _held = service
                .read_permits
                .clone()
                .acquire_many_owned(8)
                .await
                .unwrap();
            let mut pending =
                Box::pin(service.read_ranges(Request::new(dfs_read_request("chunk".into(), 1))));
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx).is_pending()))
                    .await
            );
            if stop {
                local.close_peer_serve_admission();
            }
            let error = pending.await.err().unwrap();
            assert_eq!(
                error.code(),
                if stop {
                    tonic::Code::Unavailable
                } else {
                    tonic::Code::ResourceExhausted
                }
            );
        }
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn p2_t1_grant_expiry_is_rechecked_after_rpc_queue() {
        use std::{future::Future, task::Poll};
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node")
                .unwrap()
                .with_peer_serve_wait(std::time::Duration::from_millis(50))
                .unwrap(),
        );
        let service = dfs_reuse_test_service(local, Arc::new(AllowDfsTestGrant));
        let held = service
            .read_permits
            .clone()
            .acquire_many_owned(8)
            .await
            .unwrap();
        let mut request = dfs_read_request("chunk".into(), 1);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        request.operations[0]
            .grant
            .as_mut()
            .unwrap()
            .expires_at_unix_ms = now + 10;
        let mut pending = Box::pin(service.read_ranges(Request::new(request)));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx).is_pending())).await
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
        drop(held);
        assert_eq!(
            pending.await.err().unwrap().code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(service.read_permits.available_permits(), 8);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn dfs_small_ranges_use_one_delivery_without_expanding_large_streams() {
        use crate::node::chunk::StagedChunk;
        use tokio_stream::StreamExt;

        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node").unwrap());
        let bytes: Vec<u8> = (0..1024 * 1024).map(|n| (n % 251) as u8).collect();
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("small-delivery"),
            bytes.clone(),
        );
        local.put(staged.clone()).unwrap();
        let service = dfs_reuse_test_service(local, Arc::new(AllowDfsTestGrant));
        let mut frame_counts = Vec::new();
        for length in [128 * 1024, 1, 64 * 1024, 128 * 1024 + 1, bytes.len()] {
            let request = dfs_read_request(staged.chunk.id.0.clone(), length as u64);
            let frames: Vec<_> = service
                .read_ranges(Request::new(request))
                .await
                .unwrap()
                .into_inner()
                .collect()
                .await;
            frame_counts.push(frames.len());
            let mut received = Vec::new();
            for frame in frames {
                match frame.unwrap().body.unwrap() {
                    dfs_read_ranges_frame::Body::Data(data) => received.extend(data),
                    dfs_read_ranges_frame::Body::Compact(reply) => {
                        assert_eq!(reply.operation_index, 0);
                        assert_eq!(
                            reply.range_checksum.as_slice(),
                            blake3::hash(&reply.data).as_bytes()
                        );
                        received.extend(reply.data);
                    }
                    _ => {}
                }
            }
            assert_eq!(received, bytes[..length]);
        }
        // Small complete replies need no independent header or completion.
        // Larger ranges keep the original bounded incremental delivery.
        assert_eq!(frame_counts, [1, 1, 1, 5, 18]);
        assert_eq!(service.read_permits.available_permits(), 8);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn cp04v2_compact_reply_crosses_grpc_with_bounded_encoded_size() {
        use crate::node::chunk::StagedChunk;
        use afs_protocol::node_data::dfs_chunks_client::DfsChunksClient;
        use tokio_stream::StreamExt;

        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node").unwrap());
        let body = vec![37; afs_protocol::node_data::DFS_COMPACT_RANGE_MAX_BYTES];
        let staged = StagedChunk::new(crate::dfs::OperationId::new("compact-grpc"), body.clone());
        local.put(staged.clone()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let service = make_dfs_chunks_server(
            Some(local),
            Arc::new(InternalPeerAuthenticator),
            Arc::new(AllowDfsTestGrant),
        );
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        // Include protobuf overhead. The disk-stream 128KiB *encoded* limit
        // deliberately cannot accept this ordinary 128KiB body reply.
        let mut client = DfsChunksClient::connect(endpoint)
            .await
            .unwrap()
            .max_decoding_message_size(body.len() + 64);
        let mut stream = client
            .read_ranges(dfs_read_request(staged.chunk.id.0, body.len() as u64))
            .await
            .unwrap()
            .into_inner();
        let frame = stream.next().await.unwrap().unwrap();
        let dfs_read_ranges_frame::Body::Compact(reply) = frame.body.unwrap() else {
            panic!("ordinary small range must use one compact reply");
        };
        assert_eq!(reply.operation_index, 0);
        assert_eq!(reply.data.as_ref(), body.as_slice());
        assert_eq!(
            reply.range_checksum.as_slice(),
            blake3::hash(&body).as_bytes()
        );
        assert!(stream.next().await.is_none());
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn cp04v2_compact_reply_pins_its_original_payload_after_cancel() {
        use crate::node::chunk::StagedChunk;
        use tokio_stream::StreamExt;

        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node")
                .unwrap()
                .with_peer_serve_config(1, 8 * 1024 * 1024)
                .unwrap(),
        );
        let first = StagedChunk::new(
            crate::dfs::OperationId::new("compact-held"),
            vec![7; 4 * 1024 * 1024],
        );
        let other = StagedChunk::new(
            crate::dfs::OperationId::new("compact-replace"),
            vec![9; 4 * 1024 * 1024],
        );
        local.put(first.clone()).unwrap();
        local.put(other.clone()).unwrap();
        let service = dfs_reuse_test_service(local.clone(), Arc::new(AllowDfsTestGrant));
        let mut stream = service
            .read_ranges(Request::new(dfs_read_request(first.chunk.id.0, 128 * 1024)))
            .await
            .unwrap()
            .into_inner();
        let frame = stream.next().await.unwrap().unwrap();
        let dfs_read_ranges_frame::Body::Compact(reply) = frame.body.unwrap() else {
            panic!("small range must retain its payload in the compact reply");
        };
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while service.read_permits.available_permits() != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(
            local
                .read_peer_range(&other.chunk.id, 0, 64 * 1024)
                .unwrap(),
        );
        assert_eq!(reply.data.as_ref(), &[7; 128 * 1024]);
        // A copied 128KiB body would wrongly release the original 4MiB lease.
        assert!(local.reserve_peer_read_payload(5 * 1024 * 1024).is_err());
        drop(reply);
        drop(local.reserve_peer_read_payload(8 * 1024 * 1024).unwrap());
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn dfs_peer_object_reuse_scans_once_and_still_checks_authority() {
        use crate::node::chunk::StagedChunk;
        use tokio_stream::StreamExt;
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node")
                .unwrap()
                .with_read_work_metrics(
                    crate::node::read_work::ReadWorkMetrics::register(&registry).unwrap(),
                ),
        );
        let bytes: Vec<u8> = (0..4 * 1024 * 1024).map(|n| (n % 251) as u8).collect();
        let staged = StagedChunk::new(crate::dfs::OperationId::new("reuse"), bytes.clone());
        local.put(staged.clone()).unwrap();
        let scans_before = dfs_reuse_scan_bytes(&registry);
        let allowed = dfs_reuse_test_service(local.clone(), Arc::new(AllowDfsTestGrant));
        for part in 0..4 {
            let start = part * 1024 * 1024;
            let mut request = dfs_read_request(staged.chunk.id.0.clone(), 1024 * 1024);
            request.operations[0].chunk_offset = start as u64;
            let mut stream = allowed
                .read_ranges(Request::new(request))
                .await
                .unwrap()
                .into_inner();
            let mut received = Vec::new();
            let mut completions = 0;
            while let Some(frame) = stream.next().await {
                match frame.unwrap().body.unwrap() {
                    dfs_read_ranges_frame::Body::Data(data) => {
                        assert!(data.len() <= 64 * 1024);
                        received.extend(data);
                    }
                    dfs_read_ranges_frame::Body::Completion(completion) => {
                        assert_eq!(
                            completion.range_checksum,
                            blake3::hash(&received).as_bytes().to_vec()
                        );
                        completions += 1;
                    }
                    dfs_read_ranges_frame::Body::Header(_) => {}
                    dfs_read_ranges_frame::Body::Compact(_) => {
                        panic!("large ranges must keep bounded incremental delivery")
                    }
                }
            }
            assert_eq!(received, bytes[start..start + 1024 * 1024]);
            assert_eq!(completions, 1);
        }
        assert_eq!(
            dfs_reuse_scan_bytes(&registry) - scans_before,
            bytes.len() as u64
        );
        let encoded = afs_metrics::encode_text(&registry).unwrap();
        let count = |prefix: &str| {
            encoded
                .lines()
                .find_map(|line| line.strip_prefix(prefix))
                .unwrap()
                .parse::<u64>()
                .unwrap()
        };
        assert_eq!(
            count("afs_dfs_peer_read_events_total{event=\"blocking_combined\"} "),
            4
        );
        assert_eq!(
            count("afs_dfs_peer_read_events_total{event=\"data_frame\"} "),
            64
        );
        assert_eq!(
            count("afs_dfs_peer_read_events_total{event=\"shared_frame\"} "),
            64
        );
        for event in [
            "blocking_authorize",
            "blocking_produce",
            "body_vec_allocation",
        ] {
            assert_eq!(
                count(&format!(
                    "afs_dfs_peer_read_events_total{{event=\"{event}\"}} "
                )),
                0
            );
        }
        assert_eq!(
            count("afs_dfs_read_copy_bytes_total{stage=\"range_to_frame\"} "),
            0
        );
        // A warmed immutable object is data reuse, never permission reuse.
        let denied = dfs_reuse_test_service(local.clone(), Arc::new(DenyDfsReadAuthorizer));
        assert!(
            denied
                .read_ranges(Request::new(dfs_read_request(staged.chunk.id.0, 1024)))
                .await
                .is_err()
        );
        assert_eq!(
            dfs_reuse_scan_bytes(&registry) - scans_before,
            bytes.len() as u64
        );
    }

    #[cfg(feature = "dfs")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dfs_peer_r6_followers_keep_independent_expiry_and_stop_checks() {
        use crate::node::chunk::StagedChunk;
        use tokio_stream::StreamExt;
        struct CountingAuthorizer(std::sync::atomic::AtomicUsize);
        impl DfsReadAuthorizer for CountingAuthorizer {
            fn authorize(
                &self,
                peer: &str,
                request: &DfsReadRangesRequest,
            ) -> afs_error::Result<()> {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                AllowDfsTestGrant.authorize(peer, request)
            }
        }
        for stop in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let local = Arc::new(LocalChunkStore::open(temp.path(), "node").unwrap());
            let staged =
                StagedChunk::new(crate::dfs::OperationId::new("r6-delivery"), vec![73; 4096]);
            local.put(staged.clone()).unwrap();
            let (entered, release) = local.pause_next_peer_fill_for_test(staged.chunk.id.clone());
            let authorizer = Arc::new(CountingAuthorizer(std::sync::atomic::AtomicUsize::new(0)));
            let service = dfs_reuse_test_service(local.clone(), authorizer.clone());
            let mut leader = service
                .read_ranges(Request::new(dfs_read_request(
                    staged.chunk.id.0.clone(),
                    4096,
                )))
                .await
                .unwrap()
                .into_inner();
            entered
                .recv_timeout(std::time::Duration::from_secs(3))
                .unwrap();
            let expires = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
                + 100;
            let mut request = dfs_read_request(staged.chunk.id.0.clone(), 4096);
            if !stop {
                request.operations[0]
                    .grant
                    .as_mut()
                    .unwrap()
                    .expires_at_unix_ms = expires;
            }
            let mut follower = service
                .read_ranges(Request::new(request))
                .await
                .unwrap()
                .into_inner();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
            while local.waiting_peer_fill_readers_for_test() == 0 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "RPC follower did not join flight"
                );
                tokio::task::yield_now().await;
            }
            assert_eq!(authorizer.0.load(std::sync::atomic::Ordering::SeqCst), 2);
            if stop {
                local.close_peer_serve_admission();
            } else {
                while std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
                    <= u128::from(expires)
                {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
            release.send(()).unwrap();
            let failure = follower.next().await.unwrap().unwrap_err();
            if !stop {
                assert_eq!(failure.code(), tonic::Code::PermissionDenied);
            } else {
                assert_eq!(failure.code(), tonic::Code::Unavailable);
            }
            assert!(
                follower.next().await.is_none(),
                "rejected follower must receive no Header or body"
            );
            let mut bytes = Vec::new();
            while let Some(frame) = leader.next().await {
                if stop {
                    assert!(frame.is_err());
                } else {
                    match frame.unwrap().body.unwrap() {
                        dfs_read_ranges_frame::Body::Data(data) => bytes.extend_from_slice(&data),
                        dfs_read_ranges_frame::Body::Compact(reply) => {
                            bytes.extend_from_slice(&reply.data)
                        }
                        _ => {}
                    }
                }
            }
            if !stop {
                assert_eq!(bytes, staged.bytes());
            }
            drop(leader);
            drop(follower);
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while service.read_permits.available_permits() != 8 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            local.shutdown_peer_serve().unwrap();
        }
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn dfs_peer_object_reuse_cancel_releases_slot_and_shared_budget() {
        use crate::node::chunk::StagedChunk;
        use tokio_stream::StreamExt;
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node")
                .unwrap()
                .with_peer_serve_config(1, 8 * 1024 * 1024)
                .unwrap(),
        );
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("cancel"),
            vec![7; 4 * 1024 * 1024],
        );
        local.put(staged.clone()).unwrap();
        let service = dfs_reuse_test_service(local.clone(), Arc::new(AllowDfsTestGrant));
        let mut stream = service
            .read_ranges(Request::new(dfs_read_request(
                staged.chunk.id.0,
                4 * 1024 * 1024,
            )))
            .await
            .unwrap()
            .into_inner();
        assert!(stream.next().await.unwrap().is_ok());
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while service.read_permits.available_permits() != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The retained slot can be evicted; no cancelled producer owns a hidden lease.
        let reservation = local.reserve_peer_read_payload(8 * 1024 * 1024).unwrap();
        drop(reservation);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn dfs_peer_cancel_during_authorization_keeps_permit_until_exit_and_skips_io() {
        use crate::node::chunk::StagedChunk;
        struct BlockedAuthorizer {
            started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
            release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl DfsReadAuthorizer for BlockedAuthorizer {
            fn authorize(
                &self,
                _peer: &str,
                _request: &DfsReadRangesRequest,
            ) -> afs_error::Result<()> {
                self.started
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap()
                    .send(())
                    .unwrap();
                self.release.lock().unwrap().recv().unwrap();
                Ok(())
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node")
                .unwrap()
                .with_read_work_metrics(
                    crate::node::read_work::ReadWorkMetrics::register(&registry).unwrap(),
                ),
        );
        let staged = StagedChunk::new(
            crate::dfs::OperationId::new("cancel-authorization"),
            vec![31; 4 * 1024 * 1024],
        );
        local.put(staged.clone()).unwrap();
        let before = dfs_reuse_scan_bytes(&registry);
        let (started, observed) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let service = Arc::new(dfs_reuse_test_service(
            local,
            Arc::new(BlockedAuthorizer {
                started: std::sync::Mutex::new(Some(started)),
                release: std::sync::Mutex::new(wait),
            }),
        ));
        let call = {
            let service = service.clone();
            tokio::spawn(async move {
                service
                    .read_ranges(Request::new(dfs_read_request(staged.chunk.id.0, 1024)))
                    .await
                    .map(|_| ())
            })
        };
        tokio::time::timeout(std::time::Duration::from_secs(2), observed)
            .await
            .unwrap()
            .unwrap();
        call.abort();
        assert!(call.await.unwrap_err().is_cancelled());
        assert_eq!(service.read_permits.available_permits(), 7);
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while service.read_permits.available_permits() != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(dfs_reuse_scan_bytes(&registry), before);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn dfs_peer_frame_keeps_shared_lease_after_cancel_and_object_replacement() {
        use crate::node::chunk::StagedChunk;
        use tokio_stream::StreamExt;
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node")
                .unwrap()
                .with_peer_serve_config(1, 8 * 1024 * 1024)
                .unwrap(),
        );
        let first = StagedChunk::new(
            crate::dfs::OperationId::new("held-frame"),
            vec![7; 4 * 1024 * 1024],
        );
        let replacement = StagedChunk::new(
            crate::dfs::OperationId::new("replace-held-frame"),
            vec![9; 4 * 1024 * 1024],
        );
        local.put(first.clone()).unwrap();
        local.put(replacement.clone()).unwrap();
        let service = dfs_reuse_test_service(local.clone(), Arc::new(AllowDfsTestGrant));
        let mut stream = service
            .read_ranges(Request::new(dfs_read_request(
                first.chunk.id.0,
                4 * 1024 * 1024,
            )))
            .await
            .unwrap()
            .into_inner();
        let held_frame = loop {
            let frame = stream.next().await.unwrap().unwrap();
            if matches!(&frame.body, Some(dfs_read_ranges_frame::Body::Data(_))) {
                break frame;
            }
        };
        drop(stream);
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while service.read_permits.available_permits() != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        // Replace the resident slot; the consumer still owns the old frame.
        let next = local
            .read_peer_range(&replacement.chunk.id, 0, 64 * 1024)
            .unwrap();
        assert_eq!(next.as_slice(), &[9; 64 * 1024]);
        drop(next);
        if let Some(dfs_read_ranges_frame::Body::Data(data)) = &held_frame.body {
            assert_eq!(&data[..], &[7; 64 * 1024]);
        } else {
            unreachable!();
        }
        // Even evicting the replacement cannot reclaim the held object's lease.
        assert!(local.reserve_peer_read_payload(5 * 1024 * 1024).is_err());
        drop(held_frame);
        let all = local.reserve_peer_read_payload(8 * 1024 * 1024).unwrap();
        drop(all);
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn dfs_peer_object_reuse_full_budget_returns_error_before_any_frame() {
        use crate::node::chunk::StagedChunk;
        use tokio_stream::StreamExt;
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node")
                .unwrap()
                .with_peer_serve_config(1, 8 * 1024 * 1024)
                .unwrap(),
        );
        let staged = StagedChunk::new(crate::dfs::OperationId::new("full-budget"), vec![3; 1024]);
        local.put(staged.clone()).unwrap();
        let reservation = local.reserve_peer_read_payload(8 * 1024 * 1024).unwrap();
        let service = dfs_reuse_test_service(local, Arc::new(AllowDfsTestGrant));
        let mut stream = service
            .read_ranges(Request::new(dfs_read_request(staged.chunk.id.0, 1024)))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        assert!(stream.next().await.is_none());
        drop(reservation);
    }

    #[cfg(all(feature = "dfs", feature = "rdma"))]
    #[test]
    fn dfs_peer_object_reuse_rdma_packed_shares_budget_until_payload_drop() {
        use crate::node::chunk::StagedChunk;
        let temp = tempfile::tempdir().unwrap();
        let local = LocalChunkStore::open(temp.path(), "node")
            .unwrap()
            .with_peer_serve_config(1, 8 * 1024 * 1024)
            .unwrap();
        let staged = StagedChunk::new(crate::dfs::OperationId::new("packed"), vec![9; 1024]);
        local.put(staged.clone()).unwrap();
        let request = dfs_read_request(staged.chunk.id.0, 1024);
        let all = local.reserve_peer_read_payload(8 * 1024 * 1024).unwrap();
        assert_eq!(
            read_packed_dfs_ranges(&local, &request, 1024)
                .err()
                .unwrap()
                .code(),
            tonic::Code::ResourceExhausted
        );
        drop(all);
        let packed = read_packed_dfs_ranges(&local, &request, 1024).unwrap();
        assert_eq!(packed.bytes, vec![9; 1024]);
        assert_eq!(
            packed.completions[0].range_checksum,
            blake3::hash(&packed.bytes).as_bytes().to_vec()
        );
        assert!(local.reserve_peer_read_payload(8 * 1024 * 1024).is_err());
        drop(packed);
        assert!(local.reserve_peer_read_payload(8 * 1024 * 1024).is_ok());
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn dfs_range_service_requires_authority_and_streams_bounded_frames() {
        use crate::node::chunk::StagedChunk;
        use tokio_stream::StreamExt;
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node").unwrap());
        let bytes = vec![42; 150 * 1024];
        let staged = StagedChunk::new(crate::dfs::OperationId::new("fixture-write"), bytes.clone());
        let request = dfs_read_request(staged.chunk.id.0.clone(), bytes.len() as u64);
        local.put(staged).unwrap();
        let denied = DfsChunksService {
            local_chunks: Some(local.clone()),
            authenticator: Arc::new(AllowDfsTestPeer),
            authorizer: Arc::new(DenyDfsReadAuthorizer),
            replica_authorizer: Arc::new(DenyDfsReplicaAuthorizer),
            replica_data_plane: None,
            replica_permits: Arc::new(tokio::sync::Semaphore::new(8)),
            read_permits: Arc::new(tokio::sync::Semaphore::new(8)),
            read_sessions: Arc::new(tokio::sync::Semaphore::new(8)),
            replica_timeout: std::time::Duration::from_secs(30),
            rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
            payload_metrics: None,
        };
        assert!(
            denied
                .read_ranges(Request::new(request.clone()))
                .await
                .is_err()
        );
        let allowed = DfsChunksService {
            local_chunks: Some(local),
            authenticator: Arc::new(AllowDfsTestPeer),
            authorizer: Arc::new(AllowDfsTestGrant),
            replica_authorizer: Arc::new(DenyDfsReplicaAuthorizer),
            replica_data_plane: None,
            replica_permits: Arc::new(tokio::sync::Semaphore::new(8)),
            read_permits: Arc::new(tokio::sync::Semaphore::new(8)),
            read_sessions: Arc::new(tokio::sync::Semaphore::new(8)),
            replica_timeout: std::time::Duration::from_secs(30),
            rdma_sessions: crate::node::rpc::control::RdmaSessionRegistry::new(None),
            payload_metrics: None,
        };
        let mut stream = allowed
            .read_ranges(Request::new(request))
            .await
            .unwrap()
            .into_inner();
        let mut received = Vec::new();
        let mut completions = 0;
        while let Some(frame) = stream.next().await {
            match frame.unwrap().body.unwrap() {
                dfs_read_ranges_frame::Body::Data(data) => {
                    assert!(data.len() <= 64 * 1024);
                    received.extend(data);
                }
                dfs_read_ranges_frame::Body::Completion(completion) => {
                    assert_eq!(
                        completion.range_checksum,
                        blake3::hash(&bytes).as_bytes().to_vec()
                    );
                    completions += 1;
                }
                dfs_read_ranges_frame::Body::Header(_) => {}
                dfs_read_ranges_frame::Body::Compact(_) => {
                    panic!("large ranges must keep bounded incremental delivery")
                }
            }
        }
        assert_eq!(received, bytes);
        assert_eq!(completions, 1);
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn dfs_range_request_rejects_expired_grants_and_unbounded_batches() {
        let mut request = dfs_read_request("chunk".into(), 1);
        request.operations[0]
            .grant
            .as_mut()
            .unwrap()
            .expires_at_unix_ms = 1;
        assert!(validate_dfs_read_request(&request).is_err());
        request.operations[0]
            .grant
            .as_mut()
            .unwrap()
            .expires_at_unix_ms = u64::MAX;
        request.operations[0].length = MAX_DFS_READ_BYTES + 1;
        assert!(validate_dfs_read_request(&request).is_err());
    }
}
