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
    read::engine::{MAX_DFS_READ_BYTES, MAX_DFS_READ_OPS},
};
use crate::node::{
    diagnostics::{DiagnosticStorage, DiagnosticStorageError},
    rpc::control::RdmaSessionRegistry,
    storage::MAX_TRANSFER_BYTES,
};

#[cfg(feature = "dfs")]
use afs_protocol::node_data::{
    DfsOwnerHandle, DfsOwnerReadReply, DfsOwnerReadRequest, DfsOwnerResizeReply,
    DfsOwnerResizeRequest, DfsOwnerSyncReply, DfsOwnerSyncRequest, DfsOwnerWriteReply,
    DfsOwnerWriteRequest,
    dfs_owner_files_server::{DfsOwnerFiles, DfsOwnerFilesServer},
};

#[cfg(feature = "dfs")]
use crate::node::vfs::dfs::owner_api::{
    DfsOwnerFilesHandler, OwnerFuture, dfs_owner_unimplemented,
};

#[cfg(feature = "dfs")]
mod dfs_chunks;
#[cfg(feature = "dfs")]
mod dfs_owner;
#[cfg(feature = "dfs")]
mod dfs_range_stream;
#[cfg(feature = "dfs")]
mod dfs_read_authz;
#[cfg(feature = "dfs")]
mod dfs_replica_authz;
#[cfg(feature = "dfs")]
use dfs_chunks::*;
#[cfg(feature = "dfs")]
pub use dfs_chunks::{
    DfsChunkTransportResources, DfsChunksService, DfsPayloadMetrics,
    make_dfs_chunks_server_with_transport,
};
#[cfg(all(feature = "dfs", test))]
pub use dfs_chunks::{make_dfs_chunks_server, make_dfs_chunks_server_with_replication};
#[cfg(feature = "dfs")]
pub use dfs_owner::{DfsOwnerFilesService, make_dfs_owner_files_server};
#[cfg(feature = "dfs")]
use dfs_range_stream::*;
#[cfg(feature = "dfs")]
pub use dfs_read_authz::{
    CachedDfsReadAuthorizer, DenyDfsReadAuthorizer, DfsReadAuthorizer, DfsReadGrantValidator,
};
#[cfg(feature = "dfs")]
use dfs_replica_authz::*;
#[cfg(feature = "dfs")]
pub use dfs_replica_authz::{
    AuthorizedReplicaWrite, DenyDfsReplicaAuthorizer, DfsReplicaAuthorizer, MetaReplicaAuthorizer,
};
#[cfg(feature = "dfs")]
pub(crate) use dfs_replica_authz::{DFS_REPLICA_FRAME_BYTES, MAX_DFS_REPLICA_BYTES};

/// node_data 的服务端实现。
///
/// `storage` 是真实文件操作入口；`sessions` 只在 RDMA 模式下把 session_id 转为
/// 服务端 endpoint。这里不区分 OwnerFs/DFS，只实现第一版诊断用的 8-byte/文件数据路径。
#[derive(Clone)]
pub struct NodeDataService {
    storage: Arc<DiagnosticStorage>,
    sessions: RdmaSessionRegistry,
}

impl NodeDataService {
    #[must_use]
    pub fn new(storage: Arc<DiagnosticStorage>, sessions: RdmaSessionRegistry) -> Self {
        Self { storage, sessions }
    }
}

pub fn make_data_server(
    storage: Arc<DiagnosticStorage>,
    sessions: RdmaSessionRegistry,
) -> NodeDataServer<NodeDataService> {
    NodeDataServer::new(NodeDataService::new(storage, sessions))
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

/// Extracts only transport-authenticated identity; request fields cannot supply it.
pub fn authenticate_peer<T>(
    authenticator: &dyn PeerAuthenticator,
    request: &Request<T>,
) -> Result<String, Status> {
    let certificates = afs_transport::grpc::peer_certificates(request);
    authenticator
        .authenticate(
            request.metadata(),
            afs_transport::grpc::peer_remote_addr(request),
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

fn storage_status(error: DiagnosticStorageError) -> Status {
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
    let result = crate::node::bridge::spawn_blocking(move || {
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
    let result = crate::node::bridge::spawn_blocking(move || {
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
        let remote_addr = afs_transport::grpc::peer_remote_addr(&request);
        let peer_cert_der = afs_transport::grpc::peer_certificates(&request)
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
        let result = crate::node::bridge::offload(work).await;
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
    let result = crate::node::bridge::spawn_blocking(move || {
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
    crate::node::bridge::spawn_blocking(move || {
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
    let result = crate::node::bridge::spawn_blocking(move || {
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
            let reply = crate::node::bridge::spawn_blocking(move || handler.read(&peer, request))
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
        let reply = crate::node::bridge::spawn_blocking(move || {
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
            crate::node::bridge::spawn_blocking(move || {
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
        let reply = crate::node::bridge::spawn_blocking(move || handler.write(&peer, request))
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

#[cfg(test)]
mod tests;

#[cfg(all(test, feature = "dfs"))]
mod crc_read_modes_metadata_tests;
