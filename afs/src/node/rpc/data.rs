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
    DfsReadRangesCompletion, DfsReadRangesFrame, DfsReadRangesHeader, DfsReadRangesRdmaReply,
    DfsReadRangesRdmaRequest, DfsReadRangesRequest, DfsReplicaAck,
    dfs_chunks_server::{DfsChunks, DfsChunksServer},
    dfs_read_ranges_frame,
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

    async fn dispatch<Req: Send + 'static, Reply: Send + 'static>(
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
    })
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
                    let (bytes, completions) = read_packed_dfs_ranges(&local, &read, length)?;
                    validate_dfs_read_request(&read)?;
                    let mut endpoint = session.endpoint.blocking_lock();
                    if session.poisoned.load(std::sync::atomic::Ordering::SeqCst) {
                        return Err(Status::permission_denied("RDMA session is poisoned"));
                    }
                    endpoint.put_local(&bytes).map_err(rdma_status)?;
                    endpoint.transfer_write(bytes.len()).map_err(|error| {
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
                        completions,
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
        let permit = self
            .read_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("DFS peer read slots are full"))?;
        let authorizer = self.authorizer.clone();
        let (request, permit) = tokio::task::spawn_blocking(move || {
            authorizer
                .authorize(&authenticated_peer, &request)
                .map(|()| (request, permit))
        })
        .await
        .map_err(|error| Status::internal(error.to_string()))?
        .map_err(error_to_status)?;
        let local = self.local_chunks.clone().ok_or_else(|| {
            coded_status(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "DFS local ChunkStore is not available",
            )
        })?;
        // At most four 64 KiB frames await the consumer. The blocking producer
        // owns each reader pin until its last range frame has been generated.
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let metrics = self.payload_metrics.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            if let Err(error) = stream_dfs_ranges(&local, &request, &sender, metrics.as_ref()) {
                let _ = sender.blocking_send(Err(error));
            }
        });
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

#[cfg(feature = "dfs")]
fn stream_dfs_ranges(
    local: &LocalChunkStore,
    request: &DfsReadRangesRequest,
    sender: &tokio::sync::mpsc::Sender<Result<DfsReadRangesFrame, Status>>,
    metrics: Option<&DfsPayloadMetrics>,
) -> Result<(), Status> {
    let send = |body| {
        sender
            .blocking_send(Ok(DfsReadRangesFrame { body: Some(body) }))
            .map_err(|_| Status::cancelled("DFS range consumer disconnected"))
    };
    for op in &request.operations {
        if sender.is_closed() {
            return Ok(());
        }
        // Verify one bounded range before framing it. Each read_at checks the
        // whole Chunk; issuing it per frame would amplify disk/hash work.
        let mut verified = vec![0; op.length as usize];
        if local
            .read_at(
                &crate::dfs::ChunkId::new(op.chunk_id.clone()),
                op.chunk_offset,
                &mut verified,
            )
            .map_err(error_to_status)?
            != verified.len()
        {
            return Err(coded_status(
                afs_error::NODE_TRANSFER_CORRUPT_DATA,
                "local Chunk ended before the requested DFS range",
            ));
        }
        send(dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
            read_id: request.read_id.clone(),
            attempt_id: request.attempt_id.clone(),
            operation_index: op.operation_index,
            chunk_id: op.chunk_id.clone(),
            chunk_offset: op.chunk_offset,
            length: op.length,
            source_copy_id: op.source_copy_id.clone(),
        }))?;
        let mut offset = 0;
        let mut checksum = blake3::Hasher::new();
        while offset < op.length {
            if sender.is_closed() {
                return Ok(());
            }
            let length = (op.length - offset).min(64 * 1024) as usize;
            let data = verified[offset as usize..offset as usize + length].to_vec();
            checksum.update(&data);
            send(dfs_read_ranges_frame::Body::Data(data))?;
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
                range_checksum: checksum.finalize().as_bytes().to_vec(),
                range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::Blake3
                    as i32,
            },
        ))?;
    }
    Ok(())
}

/// Extracts only transport-authenticated identity; request fields cannot supply it.
pub fn authenticate_peer<T>(
    authenticator: &dyn PeerAuthenticator,
    request: &Request<T>,
) -> Result<String, Status> {
    let certificates = request.peer_certs();
    authenticator
        .authenticate(
            request.metadata(),
            request.remote_addr(),
            certificates
                .as_ref()
                .and_then(|certs| certs.first().map(|cert| cert.as_ref())),
        )
        .map_err(error_to_status)
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
/// `authenticated_peer_node_id` 来自通道认证，不能从请求里的 holder_node_id 复制。
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

/// 从已经认证过的 node-to-node 通道提取对端 Node 身份。
///
/// 最终生产实现应绑定 mTLS/SPIFFE SAN、或 Meta 下发的每节点 token 与 TLS
/// channel binding。这里故意不提供“信任 holder_node_id 字段”的实现。
pub trait PeerAuthenticator: Send + Sync + 'static {
    fn authenticate(
        &self,
        metadata: &MetadataMap,
        remote_addr: Option<SocketAddr>,
        peer_cert_der: Option<&[u8]>,
    ) -> afs_error::Result<String>;
}

/// Node-to-node mTLS identity checker backed by an exact certificate allow-list.
///
/// The first production version intentionally avoids parsing X.509 names: Node
/// config provides the verified peer certificate DER for each node id, tonic
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
/// 成功误当成 workspace 文件成功。有 handler 时也必须提供 authenticator；
/// 否则服务 fail-closed，不允许依赖请求体里的 holder_node_id。
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
        Req: Send + 'static,
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
        let metadata = request.metadata().clone();
        let remote_addr = request.remote_addr();
        let peer_cert_der = request
            .peer_certs()
            .and_then(|certs| certs.iter().next().map(|cert| cert.as_ref().to_vec()));
        let request = request.into_inner();
        let work = move || {
            let authenticated_peer_node_id =
                authenticator.authenticate(&metadata, remote_addr, peer_cert_der.as_deref())?;
            call(handler, authenticated_peer_node_id, request)
        };
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

    fn owner_peer<T>(
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
fn read_packed_dfs_ranges(
    local: &LocalChunkStore,
    request: &DfsReadRangesRequest,
    length: usize,
) -> Result<(Vec<u8>, Vec<DfsReadRangesCompletion>), Status> {
    let mut bytes = vec![0; length];
    let mut completions = Vec::with_capacity(request.operations.len());
    for op in &request.operations {
        let start = op.destination_offset as usize;
        let end = start + op.length as usize;
        let range = &mut bytes[start..end];
        if local
            .read_at(
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
    Ok((bytes, completions))
}

#[cfg(test)]
mod tests {
    use super::*;
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
                    body: Some(Body::Data(data.to_vec())),
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
            bytes[0] ^= 1;
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
                Arc::new(FixtureReplicaPeer(
                    if index == 0 { "writer" } else { "a" }.into(),
                )),
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
            Arc::new(FixtureReplicaPeer("b".into())),
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
            RequestOutcome, Store, StoreOperation, TxnCondition, TxnMutation,
            memory::MemoryBackend,
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
            Store::open(Arc::new(MemoryBackend::default()))
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

        let meta = Arc::new(crate::meta::Meta {
            id: "meta-grpc-unknown".into(),
            observability: crate::runtime::Observability::new().unwrap(),
            store: Some(store.clone()),
            owner_roots: Arc::new(crate::meta::owner_roots::StoreOwnerRootAuthority::new(
                store.clone(),
            )),
            dfs: Some(dfs),
            trusted_nodes_by_der: Arc::new(std::collections::HashMap::new()),
            enforce_peer_identity: false,
        });
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
            RequestOutcome, Store, StoreOperation, TxnCondition, TxnMutation,
            memory::MemoryBackend,
        };
        use crate::node::chunk::StagedChunk;
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "receiver").unwrap());
        let staged = StagedChunk::new(OperationId::new("read-fixture"), vec![9; 150 * 1024]);
        local.put(staged.clone()).unwrap();
        let device = local.device_descriptor().unwrap();
        let store = Arc::new(
            Store::open(Arc::new(MemoryBackend::default()))
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
        close_dfs_rdma_session(&endpoint, 1, 1).await;
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
    async fn close_dfs_rdma_session(endpoint: &str, session_id: u64, peer_node_epoch: u64) {
        let mut client = afs_protocol::node_data::dfs_chunks_client::DfsChunksClient::connect(
            endpoint.to_owned(),
        )
        .await
        .expect("connect DFS close client");
        let mut request = Request::new(DfsCloseRdmaRequest {
            session_id,
            peer_node_epoch,
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
    fn dfs_corrupt_range_publishes_no_grpc_frames_or_rdma_completions() {
        use crate::node::chunk::StagedChunk;
        let temp = tempfile::tempdir().unwrap();
        let local = LocalChunkStore::open(temp.path(), "node").unwrap();
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
        #[cfg(feature = "rdma")]
        assert_eq!(
            read_packed_dfs_ranges(&local, &request, 75_000)
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
        );
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
