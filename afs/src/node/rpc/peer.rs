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
#[cfg(feature = "dfs")]
mod read_admission;

#[cfg(all(feature = "dfs", feature = "rdma"))]
use crate::node::read::engine::ChunkTransfer;

#[cfg(feature = "dfs")]
use crate::{
    dfs::ChunkObject,
    node::{
        chunk::outboard::{ChunkOutboard, OutboardBudget, OutboardBuilder, SharedOutboard},
        read::peer_cache::{
            CompletedPeerDiskRead, PeerObjectProof, PeerStreamProof, PeerStreamSink,
            VerifiedPeerChunk,
        },
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

#[cfg(feature = "dfs")]
mod dfs_grpc_transfer;
#[cfg(feature = "dfs")]
mod dfs_owner_client;
#[cfg(feature = "dfs")]
mod dfs_rdma_transfer;
#[cfg(feature = "dfs")]
mod dfs_read_decode;
#[cfg(feature = "dfs")]
mod dfs_replica_plane;
#[cfg(feature = "dfs")]
mod dfs_stream_work;
mod pool;
#[cfg(feature = "dfs")]
use crate::node::vfs::dfs::owner_api::RemoteDfsOwner;
#[cfg(feature = "dfs")]
pub use dfs_grpc_transfer::GrpcChunkTransfer;
#[cfg(feature = "dfs")]
pub use dfs_owner_client::{GrpcDfsOwnerClient, GrpcDfsOwnerFactory};
#[cfg(all(feature = "dfs", feature = "rdma"))]
use dfs_rdma_transfer::*;
#[cfg(feature = "dfs")]
pub use dfs_rdma_transfer::{RdmaChunkTransfer, make_chunk_transfer};
#[cfg(all(test, feature = "dfs"))]
pub(in crate::node) use dfs_read_decode::complete_peer_disk_read_for_test;
#[cfg(feature = "dfs")]
use dfs_read_decode::*;
#[cfg(all(test, feature = "dfs"))]
pub(crate) use dfs_replica_plane::replica_header;
#[cfg(all(feature = "dfs", any(feature = "rdma", test)))]
use dfs_replica_plane::*;
#[cfg(feature = "dfs")]
pub use dfs_replica_plane::{
    DfsRdmaPool, GrpcReplicaDataPlane, RdmaReplicaDataPlane, make_replica_data_plane,
};
#[cfg(feature = "dfs")]
pub(super) use dfs_replica_plane::{domain_data_digest, wire_data_digest};
#[cfg(feature = "dfs")]
use dfs_stream_work::*;
pub use pool::PeerConnectionPool;
#[cfg(feature = "dfs")]
use pool::*;

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
use crate::node::bridge::SyncRuntime as PeerRuntime;

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
impl From<afs_error::Error> for PeerError {
    fn from(value: afs_error::Error) -> Self {
        Self(value)
    }
}
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
/// or from a plain FUSE worker thread in production slow paths, so it reuses the
/// ambient runtime or keeps a small private one alive with the client.
#[cfg(feature = "ownerfs")]
use crate::node::bridge::SyncRuntime as OwnerRuntime;

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
        runtime: OwnerRuntime::current_or_new("OwnerFiles client")?,
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
        let (resource, info, capacity) = crate::node::bridge::spawn_blocking(move || {
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
        let resource = crate::node::bridge::spawn_blocking(move || {
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
        let (resource_guard, bytes) = crate::node::bridge::spawn_blocking(move || {
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
        let resource_guard = crate::node::bridge::spawn_blocking({
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
            .block_on(async move {
                client
                    .$method(request_with_current_context($request))
                    .await
                    .map_err(PeerError::from)
            })
            .map_err(|error| error.0);
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
            Ok::<_, afs_error::Error>(if blocking {
                call.await
            } else {
                match tokio::time::timeout(Duration::from_secs(5), call).await {
                    Ok(reply) => reply,
                    Err(_) => Err(tonic::Status::deadline_exceeded(
                        "Owner lock control timed out",
                    )),
                }
            })
        })?;
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
        let connected = crate::node::bridge::spawn_blocking(move || {
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
    let security = afs_transport::SecurityManager::new(tls)
        .map_err(|error| PeerError::coded(afs_error::CLIENT_ARGUMENT_INVALID, error.to_string()))?;
    let endpoint = Endpoint::from_shared(security.endpoint_uri(endpoint))?
        .connect_timeout(timeout)
        .timeout(timeout);
    security.connect(endpoint).await.map_err(|error| {
        PeerError::coded(afs_error::CLIENT_CONNECTION_UNAVAILABLE, error.to_string())
    })
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
mod tests;
