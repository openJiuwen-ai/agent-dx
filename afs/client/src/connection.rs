//! 本机 SDK 连接：只连同机 afs-node 的 UDS。
//!
//! diagnostics E2E 写入流程：调用方 `write(name, offset, data)` -> SDK 创建 memfd 并写入 data
//! -> 启动一次性 fd broker -> gRPC 只携带 SHM grant -> node 通过 broker 取 fd
//! -> node 从 fd 复制 bytes 到 diagnostics Storage。
//!
//! diagnostics E2E 读取流程：SDK 创建空 memfd -> gRPC 携带 target grant -> node 从 Storage 读
//! -> node 把 bytes 复制到 fd -> SDK 从本地 memfd 取回 Vec。
//!
//! DFS API 使用独立 service、typed handle / fixed FileVersion、显式 range 与 sync barrier；
//! buffer 被移入 worker，在 RPC 与 broker 完整收尾前保持所有权。
//!
//! 注意：这里当前是“fd 传递 + pread/pwrite copy”，还不是真正 zero-copy；但它已经
//! 避免把文件内容塞进 gRPC message，也没有 RDMA 和远端 SDK 连接。

use std::{fmt, path::PathBuf, sync::Arc, time::Duration};

use afs_protocol::local_api::{
    DfsCloseRequest, DfsFileHandle as WireDfsFileHandle, DfsFileRange as WireDfsFileRange,
    DfsFileVersionRef, DfsOpenAccess, DfsOpenRequest, DfsReadRequest,
    DfsReadTarget as WireDfsReadTarget, DfsSyncBarrier, DfsSyncRequest, DfsWriteRequest,
    LocalReadRequest, LocalWriteRequest, dfs_local_data_client::DfsLocalDataClient,
    dfs_read_target::Target as WireDfsReadTargetKind, local_data_client::LocalDataClient,
};
use afs_tracing::{Instrument, TracedChannel, request_with_current_context, traced_channel};
use afs_transport::grpc::GrpcConfig;
use hyper_util::rt::TokioIo;
use tokio::{sync::Semaphore, task::JoinHandle};
use tonic::transport::Endpoint;
use tower::service_fn;

use crate::buffer::{DfsReadBuffer, DfsWriteBuffer, OperationBuffer};

#[derive(Clone, Debug)]
pub struct LocalClientConfig {
    socket_path: PathBuf,
}

/// Canonical configuration name for the diagnostics-only Storage probe client.
pub type DiagnosticLocalClientConfig = LocalClientConfig;

/// Canonical configuration name for the DistributedFs local SDK client.
pub type DfsLocalClientConfig = LocalClientConfig;

impl LocalClientConfig {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }
}

#[derive(Clone)]
pub struct DiagnosticLocalClient {
    inner: LocalDataClient<TracedChannel>,
    request_timeout: Duration,
}

/// Compatibility name for the diagnostics-only Storage probe client.
/// New DFS integrations must use `DfsLocalClient`.
pub type LocalClient = DiagnosticLocalClient;

#[derive(Clone)]
pub struct DfsLocalClient {
    inner: DfsLocalDataClient<TracedChannel>,
    request_timeout: Duration,
}

/// Node 签发的 DFS 本机打开句柄。value 对调用方不透明，并且不能自行构造空句柄。
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DfsFileHandle(Vec<u8>);

impl DfsFileHandle {
    fn from_wire(handle: WireDfsFileHandle) -> Result<Self, LocalClientError> {
        if handle.value.is_empty() {
            return Err(protocol_violation(
                "DFS open reply contained an empty handle",
            ));
        }
        Ok(Self(handle.value))
    }

    fn to_wire(&self) -> WireDfsFileHandle {
        WireDfsFileHandle {
            value: self.0.clone(),
        }
    }
}

/// 固定的 immutable FileVersion 读取身份。
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DfsFileVersion {
    namespace_id: String,
    inode_id: String,
    file_version_id: String,
}

impl DfsFileVersion {
    pub fn new(
        namespace_id: impl Into<String>,
        inode_id: impl Into<String>,
        file_version_id: impl Into<String>,
    ) -> Result<Self, LocalClientError> {
        let value = Self {
            namespace_id: namespace_id.into(),
            inode_id: inode_id.into(),
            file_version_id: file_version_id.into(),
        };
        require_non_empty("namespace_id", &value.namespace_id)?;
        require_non_empty("inode_id", &value.inode_id)?;
        require_non_empty("file_version_id", &value.file_version_id)?;
        Ok(value)
    }

    fn to_wire(&self) -> DfsFileVersionRef {
        DfsFileVersionRef {
            namespace_id: self.namespace_id.clone(),
            inode_id: self.inode_id.clone(),
            file_version_id: self.file_version_id.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DfsReadTarget {
    Handle(DfsFileHandle),
    Version(DfsFileVersion),
}

impl DfsReadTarget {
    fn to_wire(&self) -> WireDfsReadTarget {
        let target = match self {
            Self::Handle(handle) => WireDfsReadTargetKind::Handle(handle.to_wire()),
            Self::Version(version) => WireDfsReadTargetKind::Version(version.to_wire()),
        };
        WireDfsReadTarget {
            target: Some(target),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DfsFileRange {
    pub offset: u64,
    pub length: u32,
}

impl DfsFileRange {
    pub const fn new(offset: u64, length: u32) -> Self {
        Self { offset, length }
    }

    fn to_wire(self) -> WireDfsFileRange {
        WireDfsFileRange {
            offset: self.offset,
            length: self.length,
        }
    }

    fn validate(self) -> Result<(), LocalClientError> {
        self.offset.checked_add(u64::from(self.length)).ok_or(
            LocalClientError::InvalidArgument {
                field: "range",
                reason: "file range overflows u64",
            },
        )?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DfsAccess {
    ReadOnly,
    ReadWrite,
}

impl DfsAccess {
    fn to_wire(self) -> i32 {
        match self {
            Self::ReadOnly => DfsOpenAccess::ReadOnly.into(),
            Self::ReadWrite => DfsOpenAccess::ReadWrite.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DfsSyncMode {
    Data,
    Full,
}

impl DfsSyncMode {
    fn to_wire(self) -> i32 {
        match self {
            Self::Data => DfsSyncBarrier::Data.into(),
            Self::Full => DfsSyncBarrier::Full.into(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct DfsOpenFile {
    pub handle: DfsFileHandle,
    pub file_version_id: String,
    pub length: u64,
}

pub struct DfsReadResult {
    pub buffer: DfsReadBuffer,
    pub bytes_read: u32,
    pub resolved_file_version_id: String,
}

impl DfsReadResult {
    pub fn into_bytes(self) -> Result<Vec<u8>, LocalClientError> {
        self.buffer.read_prefix(self.bytes_read)
    }
}

pub struct DfsWriteResult {
    pub buffer: DfsWriteBuffer,
    pub bytes_written: u32,
    pub accepted_write_seq: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsSyncResult {
    pub file_version_id: String,
    pub durable_write_seq: u64,
}

// 每个本地 SDK 操作都会临时占用一个 memfd 和一个 fd broker。
// 这里的 slot 上限是进程内背压，避免调用方用取消/并发把 fd、socket、线程资源打满。
// permit 使用 OwnedSemaphorePermit 交给 worker 持有：即使外层 future 被取消，worker
// 仍负责等待 broker 结束后再释放资源。
const SDK_SHM_SLOTS: usize = 64;
static BROKER_ADMISSION: std::sync::LazyLock<Arc<Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(Semaphore::new(SDK_SHM_SLOTS)));

pub fn max_parallel_shm_operations() -> usize {
    SDK_SHM_SLOTS
}

#[derive(Debug)]
pub enum LocalClientError {
    InvalidArgument {
        field: &'static str,
        reason: &'static str,
    },
    Transport(tonic::transport::Error),
    Status(afs_error::Error),
    Shm(afs_transport::shm::ShmError),
    BrokerThread,
    Worker,
    Timeout,
    Closed,
}

impl fmt::Display for LocalClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument { field, reason } => {
                write!(formatter, "invalid argument `{field}`: {reason}")
            }
            Self::Transport(error) => write!(formatter, "transport error: {error}"),
            Self::Status(status) => write!(formatter, "rpc status: {status}"),
            Self::Shm(error) => write!(formatter, "shared memory error: {error}"),
            Self::BrokerThread => formatter.write_str("broker thread failed"),
            Self::Worker => formatter.write_str("local SDK worker task failed"),
            Self::Timeout => formatter.write_str("local SDK RPC timed out"),
            Self::Closed => formatter.write_str("local SDK client is closed"),
        }
    }
}

impl LocalClientError {
    /// 返回统一错误。保留来源枚举便于本机诊断，但业务判断应使用 code/kind。
    pub fn error(&self) -> afs_error::Error {
        use afs_error::*;
        if let Self::Status(error) = self {
            return error.clone();
        }
        if let Self::Shm(afs_transport::shm::ShmError::Syscall(_, inner)) = self {
            return Error::from(std::io::Error::new(inner.kind(), inner.to_string()));
        }
        let code = match self {
            Self::InvalidArgument { .. } => CLIENT_ARGUMENT_INVALID,
            Self::Transport(_) => CLIENT_CONNECTION_UNAVAILABLE,
            Self::Timeout => CLIENT_DEADLINE_EXCEEDED,
            Self::Closed => CLIENT_CLOSED,
            Self::BrokerThread | Self::Worker => CLIENT_WORKER_FAILED,
            Self::Shm(afs_transport::shm::ShmError::InvalidArgument { .. }) => {
                CLIENT_ARGUMENT_INVALID
            }
            Self::Shm(afs_transport::shm::ShmError::InvalidToken) => CLIENT_PERMISSION_DENIED,
            Self::Shm(afs_transport::shm::ShmError::Protocol(_)) => CLIENT_PROTOCOL_VIOLATION,
            Self::Shm(_) => CLIENT_SHM_UNAVAILABLE,
            Self::Status(_) => unreachable!(),
        };
        Error::coded(code, self.to_string())
    }
    pub fn code(&self) -> afs_error::ErrorCode {
        self.error().code()
    }
    pub fn kind(&self) -> afs_error::ErrorKind {
        self.error().kind()
    }
}

impl std::error::Error for LocalClientError {}

impl From<tonic::transport::Error> for LocalClientError {
    fn from(value: tonic::transport::Error) -> Self {
        Self::Transport(value)
    }
}

impl From<tonic::Status> for LocalClientError {
    fn from(value: tonic::Status) -> Self {
        Self::Status(afs_transport::grpc::error_status::status_to_error(value))
    }
}

impl From<afs_transport::shm::ShmError> for LocalClientError {
    fn from(value: afs_transport::shm::ShmError) -> Self {
        Self::Shm(value)
    }
}

impl DiagnosticLocalClient {
    pub async fn connect(config: LocalClientConfig) -> Result<Self, LocalClientError> {
        // UDS 连接仍复用统一 GrpcConfig：超时、HTTP/2 window、message size 等配置
        // 和其它 gRPC 通道一致；差异只在 connector 把“网络地址”替换成本机 socket。
        let path = Arc::new(config.socket_path);
        let grpc = GrpcConfig::default();
        let endpoint = grpc.configure_client(
            Endpoint::try_from("http://[::]:50051").expect("static endpoint URI"),
        );
        let channel = endpoint
            .connect_with_connector(service_fn(move |_| {
                let path = Arc::clone(&path);
                async move {
                    let stream = tokio::net::UnixStream::connect(path.as_ref()).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }))
            .await?;
        Ok(Self {
            inner: LocalDataClient::new(traced_channel(channel))
                .max_encoding_message_size(grpc.max_encoding_message_bytes)
                .max_decoding_message_size(grpc.max_decoding_message_bytes),
            request_timeout: grpc.request_timeout,
        })
    }

    pub async fn write(
        &self,
        name: &str,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<usize, LocalClientError> {
        let length = u32::try_from(data.len()).map_err(|_| LocalClientError::InvalidArgument {
            field: "data",
            reason: "too large for local SDK request",
        })?;
        // 先拿 slot，再创建 memfd/broker。permit 会移动到 worker，防止调用方取消
        // `write()` future 时提前释放 slot，而 broker 线程还在等待 node 取 fd。
        let permit = BROKER_ADMISSION
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| LocalClientError::Closed)?;
        let worker = spawn_write_worker(
            permit,
            self.inner.clone(),
            self.request_timeout,
            name.to_owned(),
            offset,
            length,
            data,
        );
        worker.await.map_err(|_| LocalClientError::Worker)?
    }

    pub async fn read(
        &self,
        name: &str,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, LocalClientError> {
        let buffer_len =
            usize::try_from(length).map_err(|_| LocalClientError::InvalidArgument {
                field: "length",
                reason: "too large for local platform",
            })?;
        // read 同样先占 slot：target memfd 是 node 回填数据的唯一通道，
        // 不能在 RPC 返回前被调用方取消路径提前释放。
        let permit = BROKER_ADMISSION
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| LocalClientError::Closed)?;
        let worker = spawn_read_worker(
            permit,
            self.inner.clone(),
            self.request_timeout,
            name.to_owned(),
            offset,
            length,
            buffer_len,
        );
        worker.await.map_err(|_| LocalClientError::Worker)?
    }
}

impl DfsLocalClient {
    pub async fn connect(config: DfsLocalClientConfig) -> Result<Self, LocalClientError> {
        let path = Arc::new(config.socket_path);
        let grpc = GrpcConfig::default();
        let endpoint = grpc.configure_client(
            Endpoint::try_from("http://[::]:50051").expect("static endpoint URI"),
        );
        let channel = endpoint
            .connect_with_connector(service_fn(move |_| {
                let path = Arc::clone(&path);
                async move {
                    let stream = tokio::net::UnixStream::connect(path.as_ref()).await?;
                    Ok::<_, std::io::Error>(TokioIo::new(stream))
                }
            }))
            .await?;
        Ok(Self {
            inner: DfsLocalDataClient::new(traced_channel(channel))
                .max_encoding_message_size(grpc.max_encoding_message_bytes)
                .max_decoding_message_size(grpc.max_decoding_message_bytes),
            request_timeout: grpc.request_timeout,
        })
    }

    pub async fn open(
        &self,
        namespace_id: &str,
        inode_id: &str,
        access: DfsAccess,
        expected_file_version_id: Option<&str>,
    ) -> Result<DfsOpenFile, LocalClientError> {
        require_non_empty("namespace_id", namespace_id)?;
        require_non_empty("inode_id", inode_id)?;
        if let Some(version) = expected_file_version_id {
            require_non_empty("expected_file_version_id", version)?;
        }
        let request = request_with_current_context(DfsOpenRequest {
            namespace_id: namespace_id.to_owned(),
            inode_id: inode_id.to_owned(),
            access: access.to_wire(),
            expected_file_version_id: expected_file_version_id.map(str::to_owned),
        });
        let mut client = self.inner.clone();
        let reply = tokio::time::timeout(self.request_timeout, client.open(request))
            .await
            .map_err(|_| LocalClientError::Timeout)??
            .into_inner();
        let handle = reply
            .handle
            .ok_or_else(|| protocol_violation("DFS open reply omitted its handle"))?;
        Ok(DfsOpenFile {
            handle: DfsFileHandle::from_wire(handle)?,
            file_version_id: reply.file_version_id,
            length: reply.length,
        })
    }

    pub async fn read(
        &self,
        target: DfsReadTarget,
        range: DfsFileRange,
        buffer: DfsReadBuffer,
    ) -> Result<DfsReadResult, LocalClientError> {
        range.validate()?;
        if range.length > buffer.capacity() {
            return Err(LocalClientError::InvalidArgument {
                field: "range.length",
                reason: "exceeds DFS read buffer capacity",
            });
        }
        let permit = BROKER_ADMISSION
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| LocalClientError::Closed)?;
        spawn_dfs_read_worker(
            permit,
            self.inner.clone(),
            self.request_timeout,
            target,
            range,
            buffer,
        )
        .await
        .map_err(|_| LocalClientError::Worker)?
    }

    pub async fn write(
        &self,
        handle: &DfsFileHandle,
        range: DfsFileRange,
        buffer: DfsWriteBuffer,
    ) -> Result<DfsWriteResult, LocalClientError> {
        range.validate()?;
        if range.length != buffer.length() {
            return Err(LocalClientError::InvalidArgument {
                field: "range.length",
                reason: "must equal DFS write buffer length",
            });
        }
        let permit = BROKER_ADMISSION
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| LocalClientError::Closed)?;
        spawn_dfs_write_worker(
            permit,
            self.inner.clone(),
            self.request_timeout,
            handle.clone(),
            range,
            buffer,
        )
        .await
        .map_err(|_| LocalClientError::Worker)?
    }

    pub async fn sync(
        &self,
        handle: &DfsFileHandle,
        mode: DfsSyncMode,
    ) -> Result<DfsSyncResult, LocalClientError> {
        let request = request_with_current_context(DfsSyncRequest {
            handle: Some(handle.to_wire()),
            barrier: mode.to_wire(),
        });
        let mut client = self.inner.clone();
        let reply = tokio::time::timeout(self.request_timeout, client.sync(request))
            .await
            .map_err(|_| LocalClientError::Timeout)??
            .into_inner();
        Ok(DfsSyncResult {
            file_version_id: reply.file_version_id,
            durable_write_seq: reply.durable_write_seq,
        })
    }

    pub async fn close(&self, handle: DfsFileHandle) -> Result<(), LocalClientError> {
        let request = request_with_current_context(DfsCloseRequest {
            handle: Some(handle.to_wire()),
        });
        let mut client = self.inner.clone();
        tokio::time::timeout(self.request_timeout, client.close(request))
            .await
            .map_err(|_| LocalClientError::Timeout)??;
        Ok(())
    }
}

fn spawn_dfs_read_worker(
    permit: tokio::sync::OwnedSemaphorePermit,
    mut client: DfsLocalDataClient<TracedChannel>,
    timeout: Duration,
    target: DfsReadTarget,
    range: DfsFileRange,
    buffer: DfsReadBuffer,
) -> JoinHandle<Result<DfsReadResult, LocalClientError>> {
    tokio::spawn(
        async move {
            let _permit = permit;
            let broker = buffer.serve_one();
            let request = request_with_current_context(DfsReadRequest {
                target: Some(target.to_wire()),
                range: Some(range.to_wire()),
                target_buffer: Some(buffer.grant(range.length)),
            });
            let response = tokio::time::timeout(timeout, client.read(request)).await;
            let broker_result = join_broker(broker).await;
            let reply = response
                .map_err(|_| LocalClientError::Timeout)??
                .into_inner();
            broker_result?;
            if reply.length > range.length {
                return Err(protocol_violation(
                    "DFS read reply length exceeded the requested range",
                ));
            }
            Ok(DfsReadResult {
                buffer,
                bytes_read: reply.length,
                resolved_file_version_id: reply.resolved_file_version_id,
            })
        }
        .in_current_span(),
    )
}

fn spawn_dfs_write_worker(
    permit: tokio::sync::OwnedSemaphorePermit,
    mut client: DfsLocalDataClient<TracedChannel>,
    timeout: Duration,
    handle: DfsFileHandle,
    range: DfsFileRange,
    buffer: DfsWriteBuffer,
) -> JoinHandle<Result<DfsWriteResult, LocalClientError>> {
    tokio::spawn(
        async move {
            let _permit = permit;
            let broker = buffer.serve_one();
            let request = request_with_current_context(DfsWriteRequest {
                handle: Some(handle.to_wire()),
                range: Some(range.to_wire()),
                source_buffer: Some(buffer.grant()),
            });
            let response = tokio::time::timeout(timeout, client.write(request)).await;
            let broker_result = join_broker(broker).await;
            let reply = response
                .map_err(|_| LocalClientError::Timeout)??
                .into_inner();
            broker_result?;
            if reply.written > range.length {
                return Err(protocol_violation(
                    "DFS write reply length exceeded the requested range",
                ));
            }
            Ok(DfsWriteResult {
                buffer,
                bytes_written: reply.written,
                accepted_write_seq: reply.accepted_write_seq,
            })
        }
        .in_current_span(),
    )
}

// worker 是一次操作的资源所有者：permit、OperationBuffer、broker thread 都在这里闭环。
// 返回错误前仍会 join broker，避免“RPC 已失败但 fd broker 还活着”的资源泄漏。
fn spawn_write_worker(
    permit: tokio::sync::OwnedSemaphorePermit,
    mut client: LocalDataClient<TracedChannel>,
    timeout: Duration,
    name: String,
    offset: u64,
    length: u32,
    data: Vec<u8>,
) -> JoinHandle<Result<usize, LocalClientError>> {
    tokio::spawn(
        async move {
            let _permit = permit;
            let mut buffer = OperationBuffer::new(data.len())?;
            buffer.write_local(&data)?;
            let broker = buffer.serve_one();
            let request = request_with_current_context(LocalWriteRequest {
                name,
                file_offset: offset,
                length,
                source: Some(buffer.grant(length)),
            });
            // timeout 限制的是 gRPC 控制请求；broker join 仍必须执行，
            // 这样 node 未连接或连接失败时也能等到 broker 自己超时退出。
            let response = tokio::time::timeout(timeout, client.write(request)).await;
            let broker_result = join_broker(broker).await;
            let reply = response
                .map_err(|_| LocalClientError::Timeout)??
                .into_inner();
            broker_result?;
            if reply.written != length {
                return Err(LocalClientError::Status(afs_error::Error::coded(
                    afs_error::CLIENT_PROTOCOL_VIOLATION,
                    "reply length did not match request",
                )));
            }
            Ok(reply.written as usize)
        }
        .in_current_span(),
    )
}

// 读取 worker 与写入对称：node 只拿到 target fd，不拿到 SDK 内存引用。
// node 写完 fd 后 SDK 再从本地 memfd copy 出 Vec。
fn spawn_read_worker(
    permit: tokio::sync::OwnedSemaphorePermit,
    mut client: LocalDataClient<TracedChannel>,
    timeout: Duration,
    name: String,
    offset: u64,
    length: u32,
    buffer_len: usize,
) -> JoinHandle<Result<Vec<u8>, LocalClientError>> {
    tokio::spawn(
        async move {
            let _permit = permit;
            let buffer = OperationBuffer::new(buffer_len)?;
            let broker = buffer.serve_one();
            let request = request_with_current_context(LocalReadRequest {
                name,
                file_offset: offset,
                length,
                target: Some(buffer.grant(length)),
            });
            // 先等待 broker 收尾，再解释 RPC 结果；对外优先保留 RPC 原始错误，
            // 但资源生命周期不能因为错误路径而跳过。
            let response = tokio::time::timeout(timeout, client.read(request)).await;
            let broker_result = join_broker(broker).await;
            let reply = response
                .map_err(|_| LocalClientError::Timeout)??
                .into_inner();
            broker_result?;
            if reply.length > length {
                return Err(LocalClientError::Status(afs_error::Error::coded(
                    afs_error::CLIENT_PROTOCOL_VIOLATION,
                    "reply length exceeded request",
                )));
            }
            buffer.read_local(reply.length as usize).map_err(Into::into)
        }
        .in_current_span(),
    )
}

// std::thread::JoinHandle::join 是阻塞操作，必须放到 spawn_blocking，
// 否则 Tokio worker 可能被本地 broker 等待卡住。
async fn join_broker(
    broker: std::thread::JoinHandle<Result<(), afs_transport::shm::ShmError>>,
) -> Result<(), LocalClientError> {
    let broker_result = tokio::task::spawn_blocking(move || {
        broker.join().map_err(|_| LocalClientError::BrokerThread)
    })
    .await
    .map_err(|_| LocalClientError::BrokerThread)?
    .map_err(|_| LocalClientError::BrokerThread)?;
    broker_result.map_err(Into::into)
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), LocalClientError> {
    if value.is_empty() {
        return Err(LocalClientError::InvalidArgument {
            field,
            reason: "must not be empty",
        });
    }
    Ok(())
}

fn protocol_violation(message: &'static str) -> LocalClientError {
    LocalClientError::Status(afs_error::Error::coded(
        afs_error::CLIENT_PROTOCOL_VIOLATION,
        message,
    ))
}
