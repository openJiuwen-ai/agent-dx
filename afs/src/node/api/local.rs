//! afs-node 内的 Local SDK 服务端。
//!
//! 这层只处理同机 SDK 的高性能数据入口：gRPC over UDS 是控制面，memfd/FD pass
//! 是数据面。服务端不会从 gRPC payload 里收发文件内容，也不会在本层接 RDMA。
//!
//! `LocalData` 保留 diagnostics Storage 的 SHM 穿刺；`DfsLocalData` 是正式 DFS SDK
//! 的独立协议面。DFS backend 尚未接线时所有 DFS RPC 明确返回 unimplemented，绝不
//! 降级访问 diagnostics Storage。
//!
//! diagnostics 写入 E2E：SDK 给 source grant -> node 通过 fd broker 取 memfd fd -> `read_fd_at`
//! 从 fd copy bytes -> 写入 diagnostics Storage。
//!
//! 读取 E2E：node 从 Storage 读 bytes -> 通过 target grant 取 fd -> `write_fd_at`
//! copy 到 SDK memfd -> SDK 再本地读取。
//!
//! 当前安全边界是 trusted local host：UDS socket 权限 + 一次性 token/TTL/会话标识
//! 限制同机误用；它不是跨主机认证授权系统。

use afs_transport::grpc::error_status::{coded_status, error_to_status};
use std::{
    fs::{File, OpenOptions},
    io,
    os::{
        fd::OwnedFd,
        unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use afs_metrics::{IntCounterVec, Opts, Registry};
use afs_protocol::local_api::{
    DfsCloseReply, DfsCloseRequest, DfsOpenReply, DfsOpenRequest, DfsReadReply, DfsReadRequest,
    DfsSyncReply, DfsSyncRequest, DfsWriteReply, DfsWriteRequest, LocalReadReply, LocalReadRequest,
    LocalShmGrant, LocalWriteReply, LocalWriteRequest,
    dfs_local_data_server::{DfsLocalData, DfsLocalDataServer},
    local_data_server::{LocalData, LocalDataServer},
};
use afs_transport::shm::{
    BrokerToken, FdBrokerClient, FdRequest, ShmError, read_fd_at, write_fd_at,
};
use tokio::{net::UnixListener, sync::oneshot, task::JoinHandle};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status, transport::Server};

use crate::node::storage::{Storage, StorageError};

const STALE_SOCKET_CONNECT_TIMEOUT: Duration = Duration::from_millis(250);

pub async fn serve_local_api(
    storage: impl Into<Arc<Storage>>,
    socket_path: impl AsRef<Path>,
) -> io::Result<LocalApiServer> {
    serve_local_api_with_options(storage, socket_path, LocalApiOptions::default()).await
}

#[derive(Clone, Debug, Default)]
pub struct LocalApiOptions {
    pub metrics_registry: Option<Registry>,
}

/// 启动本机 Local API。
///
/// socket_path 必须是调用方独占的本机路径；如果底层文件系统不支持对 Unix socket
/// chmod，则要求父目录本身是 owner-only。启动失败会尽量删除自己创建的 socket。
pub async fn serve_local_api_with_options(
    storage: impl Into<Arc<Storage>>,
    socket_path: impl AsRef<Path>,
    options: LocalApiOptions,
) -> io::Result<LocalApiServer> {
    let socket_path = socket_path.as_ref().to_path_buf();
    if let Some(parent) = socket_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let socket_lock = acquire_local_api_socket_lock(&socket_path)?;
    reclaim_stale_local_api_socket(&socket_path).await?;
    let metrics = options
        .metrics_registry
        .as_ref()
        .map(LocalApiMetrics::register)
        .transpose()?;
    let listener = UnixListener::bind(&socket_path)?;
    let metadata = std::fs::symlink_metadata(&socket_path)?;
    let socket_identity = SocketIdentity::from_metadata(&metadata);
    if let Err(error) = secure_bound_socket(&socket_path).await {
        let _ = remove_owned_socket_sync(&socket_path, socket_identity);
        return Err(error);
    }
    let incoming = UnixListenerStream::new(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let diagnostic_service = LocalDataServer::new(DiagnosticLocalDataService {
        storage: storage.into(),
        metrics: metrics.clone(),
    });
    let dfs_service = DfsLocalDataServer::new(DfsLocalDataService { metrics });
    let task = tokio::spawn(async move {
        afs_transport::grpc::GrpcConfig::default()
            .configure_server(Server::builder())
            .layer(afs_tracing::GrpcServerTraceLayer::default())
            .add_service(diagnostic_service)
            .add_service(dfs_service)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|error| io::Error::other(error.to_string()))
    });
    Ok(LocalApiServer {
        shutdown_tx: Some(shutdown_tx),
        task: Some(task),
        socket_path,
        socket_identity,
        _socket_lock: socket_lock,
    })
}

/// Local API 运行句柄。
///
/// `shutdown()` 是正常路径；`Drop` 是兜底路径，只发关闭信号并清理自有 socket，
/// 不承诺等待所有运行中请求完成。root/node supervisor 可用 `abort_handle()` 观察或中止。
pub struct LocalApiServer {
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
    socket_path: PathBuf,
    socket_identity: SocketIdentity,
    _socket_lock: File,
}

impl LocalApiServer {
    /// 供 node supervisor 轮询服务 task 是否已经自然退出。
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// 返回 task abort handle，便于上层统一监督；LocalApiServer 本身仍保留 shutdown 语义。
    pub fn abort_handle(&self) -> Option<tokio::task::AbortHandle> {
        self.task.as_ref().map(JoinHandle::abort_handle)
    }

    /// 正常关闭：先通知 tonic server，再等待 task 退出，最后只删除自己创建的 socket。
    pub async fn shutdown(mut self) -> io::Result<()> {
        self.signal_shutdown();
        let task_result = if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| io::Error::other(error.to_string()))?
        } else {
            Ok(())
        };
        let cleanup_result = remove_owned_socket(&self.socket_path, self.socket_identity).await;
        task_result?;
        cleanup_result
    }

    fn signal_shutdown(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
    }
}

impl Drop for LocalApiServer {
    fn drop(&mut self) {
        self.signal_shutdown();
        let _ = remove_owned_socket_sync(&self.socket_path, self.socket_identity);
    }
}

#[derive(Clone)]
struct DiagnosticLocalDataService {
    storage: Arc<Storage>,
    metrics: Option<LocalApiMetrics>,
}

#[derive(Clone)]
struct DfsLocalDataService {
    metrics: Option<LocalApiMetrics>,
}

#[derive(Clone)]
struct LocalApiMetrics {
    requests: IntCounterVec,
}

#[derive(Clone, Copy)]
struct SocketIdentity {
    dev: u64,
    ino: u64,
}

impl SocketIdentity {
    fn from_metadata(metadata: &std::fs::Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }
}

impl LocalApiMetrics {
    fn register(registry: &Registry) -> io::Result<Self> {
        registry
            .get_or_register(|registry| {
                let requests = IntCounterVec::new(
                    Opts::new(
                        "afs_local_api_requests_total",
                        "Completed local SDK requests",
                    ),
                    &["operation", "result"],
                )?;
                registry.register(Box::new(requests.clone()))?;
                Ok(Self { requests })
            })
            .map_err(|error| io::Error::other(error.to_string()))
    }

    fn record(&self, operation: &'static str, ok: bool) {
        self.requests
            .with_label_values(&[operation, if ok { "ok" } else { "error" }])
            .inc();
    }
}

impl DiagnosticLocalDataService {
    fn record(&self, operation: &'static str, ok: bool) {
        if let Some(metrics) = &self.metrics {
            metrics.record(operation, ok);
        }
    }
}

impl DfsLocalDataService {
    fn reject<T>(&self, operation: &'static str) -> Result<Response<T>, Status> {
        if let Some(metrics) = &self.metrics {
            metrics.record(operation, false);
        }
        Err(coded_status(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            format!("DFS local SDK backend is not wired: {operation}"),
        ))
    }
}

#[tonic::async_trait]
impl LocalData for DiagnosticLocalDataService {
    async fn write(
        &self,
        request: Request<LocalWriteRequest>,
    ) -> Result<Response<LocalWriteReply>, Status> {
        let outcome = self.write_inner(request.into_inner()).await;
        self.record("diagnostic_write", outcome.is_ok());
        match &outcome {
            Ok(reply) => {
                afs_logging::info!("local_api.diagnostic_write";"result"=>"ok","written"=>reply.written);
            }
            Err(error) => {
                afs_logging::warn!("local_api.diagnostic_write";"result"=>"error","error"=>error.to_string());
            }
        }
        outcome.map(Response::new)
    }

    async fn read(
        &self,
        request: Request<LocalReadRequest>,
    ) -> Result<Response<LocalReadReply>, Status> {
        let outcome = self.read_inner(request.into_inner()).await;
        self.record("diagnostic_read", outcome.is_ok());
        match &outcome {
            Ok(reply) => {
                afs_logging::info!("local_api.diagnostic_read";"result"=>"ok","length"=>reply.length);
            }
            Err(error) => {
                afs_logging::warn!("local_api.diagnostic_read";"result"=>"error","error"=>error.to_string());
            }
        }
        outcome.map(Response::new)
    }
}

#[tonic::async_trait]
impl DfsLocalData for DfsLocalDataService {
    async fn open(&self, _: Request<DfsOpenRequest>) -> Result<Response<DfsOpenReply>, Status> {
        self.reject("dfs_open")
    }

    async fn read(&self, _: Request<DfsReadRequest>) -> Result<Response<DfsReadReply>, Status> {
        self.reject("dfs_read")
    }

    async fn write(&self, _: Request<DfsWriteRequest>) -> Result<Response<DfsWriteReply>, Status> {
        self.reject("dfs_write")
    }

    async fn sync(&self, _: Request<DfsSyncRequest>) -> Result<Response<DfsSyncReply>, Status> {
        self.reject("dfs_sync")
    }

    async fn close(&self, _: Request<DfsCloseRequest>) -> Result<Response<DfsCloseReply>, Status> {
        self.reject("dfs_close")
    }
}

impl DiagnosticLocalDataService {
    async fn write_inner(&self, request: LocalWriteRequest) -> Result<LocalWriteReply, Status> {
        // 控制面必须带 source grant；没有 grant 就直接失败，不能退化为 gRPC payload 写入。
        let grant = request
            .source
            .ok_or_else(|| coded_status(afs_error::NODE_SHM_INVALID, "missing SHM source grant"))?;
        if request.length != grant.length {
            return Err(coded_status(
                afs_error::NODE_SHM_INVALID,
                "request length does not match SHM grant",
            ));
        }
        let (fd, offset) = request_grant_fd(grant).await?;
        // 这里会从 memfd copy 出 Vec；第一版不是 zero-copy。seal 只校验 fd 大小稳定。
        let data = read_fd_at(fd, offset, request.length as usize).map_err(shm_status)?;
        let written = self
            .storage
            .write(&request.name, request.file_offset, data)
            .await
            .map_err(storage_status)?;
        Ok(LocalWriteReply {
            written: written as u32,
        })
    }

    async fn read_inner(&self, request: LocalReadRequest) -> Result<LocalReadReply, Status> {
        // 读取也必须带 target grant；node 把 Storage 读出的 bytes copy 进 SDK memfd。
        let grant = request
            .target
            .ok_or_else(|| coded_status(afs_error::NODE_SHM_INVALID, "missing SHM target grant"))?;
        if request.length != grant.length {
            return Err(coded_status(
                afs_error::NODE_SHM_INVALID,
                "request length does not match SHM grant",
            ));
        }
        let data = self
            .storage
            .read(&request.name, request.file_offset, request.length)
            .await
            .map_err(storage_status)?;
        let length = u32::try_from(data.len()).map_err(|_| {
            coded_status(
                afs_error::NODE_SHM_INTERNAL,
                "storage returned too many bytes",
            )
        })?;
        let (fd, offset) = request_grant_fd(grant).await?;
        write_fd_at(fd, offset, &data).map_err(shm_status)?;
        Ok(LocalReadReply { length })
    }
}

// 通过 SDK 提供的 broker socket 取一次性 fd。这个函数运行在同机信任边界内：
// token/session/region 防误用与重放，UDS 权限限制本机其它用户访问；不承担远端认证。
async fn request_grant_fd(grant: LocalShmGrant) -> Result<(OwnedFd, usize), Status> {
    let offset = usize::try_from(grant.region_offset)
        .map_err(|_| coded_status(afs_error::NODE_SHM_INVALID, "SHM offset is too large"))?;
    let _len = grant_len(&grant)?;
    let token = BrokerToken::new(grant.token).map_err(shm_status)?;
    let request = FdRequest::new(token, grant.session_id, grant.region_id);
    let path = PathBuf::from(grant.broker_socket_path);
    let fd = tokio::task::spawn_blocking(move || FdBrokerClient::request_fd(path, &request))
        .await
        .map_err(|error| coded_status(afs_error::NODE_SHM_INTERNAL, error.to_string()))?
        .map_err(shm_status)?;
    Ok((fd, offset))
}

fn grant_len(grant: &LocalShmGrant) -> Result<usize, Status> {
    let offset = usize::try_from(grant.region_offset)
        .map_err(|_| coded_status(afs_error::NODE_SHM_INVALID, "SHM offset is too large"))?;
    let length = usize::try_from(grant.length)
        .map_err(|_| coded_status(afs_error::NODE_SHM_INVALID, "SHM length is too large"))?;
    offset
        .checked_add(length.max(1))
        .ok_or_else(|| coded_status(afs_error::NODE_SHM_INVALID, "SHM range overflows"))
}

// Local API socket 的最小权限策略。优先 chmod socket 到 0600；如果挂载层不支持
// socket chmod，则只接受父目录已经 owner-only 的场景。
async fn secure_bound_socket(path: &Path) -> io::Result<()> {
    match tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
            let parent = path.parent().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "socket path has no parent")
            })?;
            let parent_mode = tokio::fs::metadata(parent).await?.permissions().mode() & 0o077;
            if parent_mode == 0 { Ok(()) } else { Err(error) }
        }
        Err(error) => Err(error),
    }
}

fn acquire_local_api_socket_lock(socket_path: &Path) -> io::Result<File> {
    let lock_path = local_api_socket_lock_path(socket_path)?;
    let file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&lock_path)
    {
        Ok(file) => file,
        Err(error) if error.raw_os_error() == Some(libc::ELOOP) => {
            return Err(local_socket_lock_exists_error(
                socket_path,
                &lock_path,
                "lock path is a symlink",
            ));
        }
        Err(error) => return Err(error),
    };
    file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    let file_metadata = file.metadata()?;
    let path_metadata = std::fs::symlink_metadata(&lock_path)?;
    if !path_metadata.file_type().is_file()
        || path_metadata.dev() != file_metadata.dev()
        || path_metadata.ino() != file_metadata.ino()
    {
        return Err(local_socket_lock_exists_error(
            socket_path,
            &lock_path,
            "lock path was replaced while opening",
        ));
    }
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(local_socket_lock_exists_error(
            socket_path,
            &lock_path,
            "another local api startup is active",
        )),
        Err(std::fs::TryLockError::Error(error)) => Err(error),
    }
}

fn local_api_socket_lock_path(socket_path: &Path) -> io::Result<PathBuf> {
    let parent = socket_path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "socket path has no parent"))?;
    let name = socket_path.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "socket path has no file name")
    })?;
    let mut lock_name = name.to_os_string();
    lock_name.push(".lock");
    Ok(parent.join(lock_name))
}

fn local_socket_lock_exists_error(socket_path: &Path, lock_path: &Path, detail: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "local api socket lock already exists: {} for {} ({})",
            lock_path.display(),
            socket_path.display(),
            detail
        ),
    )
}

async fn reclaim_stale_local_api_socket(path: &Path) -> io::Result<()> {
    let metadata = match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_socket() {
        return Err(local_socket_exists_error(
            path,
            "path exists and is not a Unix socket",
        ));
    }
    let identity = SocketIdentity::from_metadata(&metadata);
    match tokio::time::timeout(
        STALE_SOCKET_CONNECT_TIMEOUT,
        tokio::net::UnixStream::connect(path),
    )
    .await
    {
        Ok(Ok(_stream)) => Err(local_socket_exists_error(path, "active listener responded")),
        Ok(Err(error)) if error.kind() == io::ErrorKind::ConnectionRefused => {
            if remove_stale_socket_if_same(path, identity).await? {
                Ok(())
            } else {
                Err(local_socket_exists_error(
                    path,
                    "socket identity changed during stale-socket reclaim",
                ))
            }
        }
        Ok(Err(error)) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(Err(error)) => Err(local_socket_exists_error(
            path,
            format!("connect probe failed: {error}"),
        )),
        Err(_elapsed) => Err(local_socket_exists_error(path, "connect probe timed out")),
    }
}

fn local_socket_exists_error(path: &Path, detail: impl AsRef<str>) -> io::Error {
    io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!(
            "local api socket already exists: {} ({})",
            path.display(),
            detail.as_ref()
        ),
    )
}

async fn remove_stale_socket_if_same(path: &Path, identity: SocketIdentity) -> io::Result<bool> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata)
            if metadata.file_type().is_socket()
                && metadata.dev() == identity.dev
                && metadata.ino() == identity.ino =>
        {
            tokio::fs::remove_file(path).await?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Ok(_) | Err(_) => Ok(false),
    }
}

fn remove_owned_socket_sync(path: &Path, identity: SocketIdentity) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.file_type().is_socket()
                && metadata.dev() == identity.dev
                && metadata.ino() == identity.ino =>
        {
            std::fs::remove_file(path)
        }
        Ok(_) | Err(_) => Ok(()),
    }
}

async fn remove_owned_socket(path: &Path, identity: SocketIdentity) -> io::Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata)
            if metadata.file_type().is_socket()
                && metadata.dev() == identity.dev
                && metadata.ino() == identity.ino =>
        {
            tokio::fs::remove_file(path).await
        }
        Ok(_) | Err(_) => Ok(()),
    }
}

fn storage_status(error: StorageError) -> Status {
    error_to_status(error.into())
}

// SHM 错误映射成 gRPC status；这些都是控制面错误码，不表示内容曾经过 gRPC payload。
fn shm_status(error: ShmError) -> Status {
    match error {
        ShmError::InvalidArgument { .. } | ShmError::Protocol(_) => {
            coded_status(afs_error::NODE_SHM_INVALID, error.to_string())
        }
        ShmError::InvalidToken => {
            coded_status(afs_error::NODE_SHM_ACCESS_DENIED, error.to_string())
        }
        ShmError::Unsupported => coded_status(afs_error::NODE_SHM_UNSUPPORTED, error.to_string()),
        ShmError::Syscall(_, inner) => error_to_status(afs_error::Error::from(inner)),
        ShmError::Poisoned => coded_status(afs_error::NODE_SHM_INTERNAL, error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::{fs::symlink, net::UnixListener as StdUnixListener};

    #[tokio::test]
    async fn stale_local_api_socket_is_removed() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("node.sock");
        StdUnixListener::bind(&path).expect("bind stale socket");

        reclaim_stale_local_api_socket(&path)
            .await
            .expect("stale socket should be reclaimed");

        assert!(!path.exists());
    }

    #[tokio::test]
    async fn serve_local_api_reclaims_stale_socket_before_bind() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("node.sock");
        StdUnixListener::bind(&path).expect("bind stale socket");
        let storage = Storage::new(temp.path().join("data")).expect("storage");

        let server = serve_local_api(storage, &path)
            .await
            .expect("serve local api after stale socket");

        assert!(path.exists());
        server.shutdown().await.expect("shutdown local api");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn competing_local_api_startup_preserves_first_listener() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("node.sock");
        let first_storage = Storage::new(temp.path().join("first-data")).expect("first storage");
        let first = serve_local_api(first_storage, &path)
            .await
            .expect("first local api starts");

        let second_storage = Storage::new(temp.path().join("second-data")).expect("second storage");
        let error = match serve_local_api(second_storage, &path).await {
            Ok(server) => {
                drop(server);
                panic!("second cooperative startup must fail");
            }
            Err(error) => error,
        };

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        let stream = tokio::net::UnixStream::connect(&path)
            .await
            .expect("first listener remains reachable");
        drop(stream);
        first.shutdown().await.expect("shutdown first local api");
        assert!(!path.exists());
        assert!(local_api_socket_lock_path(&path).unwrap().exists());
    }

    #[tokio::test]
    async fn active_local_api_socket_is_preserved() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("node.sock");
        let listener = StdUnixListener::bind(&path).expect("bind active listener");

        let error = reclaim_stale_local_api_socket(&path)
            .await
            .expect_err("active listener must not be reclaimed");

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(path.exists());
        drop(listener);
    }

    #[tokio::test]
    async fn non_socket_local_api_path_is_preserved() {
        let temp = tempfile::tempdir().expect("tempdir");
        let file_path = temp.path().join("node.sock");
        std::fs::write(&file_path, b"not a socket").expect("write marker file");

        let error = reclaim_stale_local_api_socket(&file_path)
            .await
            .expect_err("ordinary file must not be reclaimed");

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read(&file_path).unwrap(), b"not a socket");
    }

    #[tokio::test]
    async fn symlink_local_api_path_is_preserved() {
        let temp = tempfile::tempdir().expect("tempdir");
        let link_path = temp.path().join("node.sock");
        symlink("target.sock", &link_path).expect("create symlink");

        let error = reclaim_stale_local_api_socket(&link_path)
            .await
            .expect_err("symlink must not be reclaimed");

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(
            std::fs::symlink_metadata(&link_path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn socket_lock_symlink_is_preserved() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("node.sock");
        let lock_path = local_api_socket_lock_path(&path).expect("lock path");
        symlink("target.lock", &lock_path).expect("create lock symlink");

        let error = acquire_local_api_socket_lock(&path).expect_err("lock symlink must fail");

        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert!(
            std::fs::symlink_metadata(&lock_path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[tokio::test]
    async fn stale_socket_reclaim_preserves_replacement_inode() {
        let temp = tempfile::tempdir().expect("tempdir");
        let path = temp.path().join("node.sock");
        let old_listener = StdUnixListener::bind(&path).expect("bind old listener");
        let old_metadata = std::fs::symlink_metadata(&path).expect("old metadata");
        let old_identity = SocketIdentity::from_metadata(&old_metadata);
        std::fs::remove_file(&path).expect("remove old socket");
        let replacement = StdUnixListener::bind(&path).expect("bind replacement listener");

        let removed = remove_stale_socket_if_same(&path, old_identity)
            .await
            .expect("identity checked remove");

        assert!(!removed);
        assert!(path.exists());
        drop(replacement);
        drop(old_listener);
    }
}
