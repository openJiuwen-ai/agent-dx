//! Node → Node 控制 Handler。
//!
//! 这个文件实现 node_control.proto 的公共 service `NodeControl`，负责“建立/关闭
//! 数据传输会话”和基础 ping，并持有各后端共用的 RDMA session 表。它不传文件内容，
//! 不代替 Meta 提交新的访问授权，也不决定文件读写是否成功。后端 service 在子模块：
//! `owner`（`OwnerControl`，`ownerfs`）、`dfs`（`DfsOwnerControl`，`dfs`）；未编译的
//! 后端不注册 service，对端调用得到 gRPC UNIMPLEMENTED。
//!
//! RDMA 建连顺序：
//! 1. 客户端本地打开 RdmaEndpoint，拿到 client_info；
//! 2. 客户端调用 NegotiateData，把 client_info 通过 gRPC 发给服务端；
//! 3. 服务端先 prepare_probe 投递接收，再连接 client_info，返回 server_info + session_id；
//! 4. 客户端连接 server_info，通过 RDMA SEND_WITH_IMM 发探测并等待发送完成；
//! 5. 首次 data 请求在 session() 中消费探测的接收完成，然后才能访问文件或单边搬运。
//!
//! 参考 3FS 的“实际 RDMA 通道证明就绪”；这里按首次请求消费 CQ，不为闲置连接起轮询线程。
//! 握手版本不匹配直接拒绝；就绪不是收到 gRPC 协商请求就能成立的。
//!
//! TTL/close 只管理 RDMA 资源生命周期：过期或 close 后旧 session 不能继续使用。

#[cfg(feature = "dfs")]
mod dfs;
#[cfg(feature = "ownerfs")]
mod owner;

#[cfg(feature = "dfs")]
pub use dfs::DfsOwnerControlService;
#[cfg(feature = "ownerfs")]
pub use owner::OwnerControlService;
#[cfg(feature = "ownerfs")]
pub(in crate::node::rpc) use owner::{owner_lock_from_wire, owner_lock_to_wire};

use afs_transport::grpc::error_status::coded_status;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use afs_protocol::node_control::{
    CloseDataReply, CloseDataRequest, NegotiateDataReply, NegotiateDataRequest, PingReply,
    PingRequest,
    node_control_server::{NodeControl, NodeControlServer},
};
use afs_tracing::Instrument;
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
use afs_transport::grpc::error_status::error_to_status;
use tokio::sync::Mutex;
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

#[cfg(feature = "rdma")]
use afs_transport::rdma::{INFO_BYTES, MAX_CAPACITY, RdmaEndpoint};
#[cfg(feature = "rdma")]
use std::ops::Deref;
#[cfg(feature = "rdma")]
use std::sync::atomic::AtomicU64;

#[cfg(feature = "rdma")]
const MAX_RDMA_SESSIONS: usize = 64;
/// RDMA SEND_WITH_IMM 探测握手版本；双方都确认后才创建/使用探测资源。
pub const RDMA_HANDSHAKE_VERSION: u32 = crate::format::version::RDMA_HANDSHAKE;
#[cfg(feature = "rdma")]
const PROBE_TIMEOUT_MS: u32 = 5000;
const SESSION_TTL: Duration = Duration::from_secs(600);

#[derive(Clone, Debug, Default)]
pub struct NodeControlConfig {
    pub rdma_device: Option<String>,
}

/// Identity established from an authenticated channel and a validated Node
/// registration epoch. File grants and inode leases remain separate authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerSessionIdentity {
    scope: PeerSessionScope,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code)]
enum PeerSessionScope {
    Dfs {
        node_id: String,
        node_epoch: u64,
    },
    #[cfg(feature = "ownerfs")]
    Owner {
        node_id: String,
        access: crate::node::vfs::ownerfs::root::PresentedRootAccess,
    },
}

impl PeerSessionIdentity {
    pub fn new(authenticated_node_id: String, verified_node_epoch: u64) -> Result<Self, Status> {
        if authenticated_node_id.is_empty() || verified_node_epoch == 0 {
            return Err(Status::permission_denied(
                "RDMA peer identity/epoch is incomplete",
            ));
        }
        Ok(Self {
            scope: PeerSessionScope::Dfs {
                node_id: authenticated_node_id,
                node_epoch: verified_node_epoch,
            },
        })
    }

    #[cfg(feature = "ownerfs")]
    pub fn for_owner(
        authenticated_node_id: String,
        access: crate::node::vfs::ownerfs::root::PresentedRootAccess,
    ) -> Result<Self, Status> {
        if authenticated_node_id.is_empty()
            || access.holder_node_id != authenticated_node_id
            || access.id.0.is_empty()
            || access.epoch == 0
            || access.home_node_id.is_empty()
            || access.home_session_id.is_empty()
            || access.session_id.is_empty()
            || access.access_generation == 0
            || access.fencing_token.is_empty()
        {
            return Err(Status::permission_denied(
                "Owner RDMA peer root scope is incomplete",
            ));
        }
        Ok(Self {
            scope: PeerSessionScope::Owner {
                node_id: authenticated_node_id,
                access,
            },
        })
    }
}

#[cfg(feature = "ownerfs")]
pub fn owner_rdma_identity_from_wire(
    authenticated_node_id: String,
    access: afs_protocol::node_data::RootAccess,
) -> Result<PeerSessionIdentity, Status> {
    let access = super::data::presented_access(Some(access)).map_err(error_to_status)?;
    PeerSessionIdentity::for_owner(authenticated_node_id, access)
}

#[cfg(feature = "ownerfs")]
pub fn owner_rdma_identity(
    authenticated_node_id: String,
    access: &afs_protocol::node_data::RootAccess,
) -> Result<PeerSessionIdentity, Status> {
    owner_rdma_identity_from_wire(authenticated_node_id, access.clone())
}

/// Authenticated business transports use this entry point after validating the
/// channel and Node epoch. The legacy diagnostic RPC creates unbound sessions.
pub async fn negotiate_for_peer(
    registry: &RdmaSessionRegistry,
    identity: PeerSessionIdentity,
    request: NegotiateDataRequest,
) -> Result<Response<NegotiateDataReply>, Status> {
    if request.handshake_version != RDMA_HANDSHAKE_VERSION {
        return Err(coded_status(
            afs_error::NODE_RDMA_HANDSHAKE_VERSION,
            "unsupported RDMA handshake version",
        ));
    }
    let Some(device) = registry.device.clone() else {
        return Ok(Response::new(rdma_unsupported_reply()));
    };
    negotiate_rdma(registry, device, request, Some(identity)).await
}

fn rdma_unsupported_reply() -> NegotiateDataReply {
    NegotiateDataReply {
        session_id: 0,
        server_info: Vec::new(),
        capacity: 0,
        rdma_supported: false,
        handshake_version: RDMA_HANDSHAKE_VERSION,
    }
}

/// 服务端 RDMA session 表。
///
/// 它把 control proto 里的 `session_id` 映射到服务端持有的 `RdmaEndpoint`。
/// data.rs 每次 RDMA read/write 都先通过这里校验：session 存在、已 Ready、未 poisoned。
/// poisoned 表示这条 RDMA 路径结果可能不明，后续请求必须 fail closed。
#[derive(Clone)]
pub struct RdmaSessionRegistry {
    inner: Arc<Mutex<HashMap<u64, Arc<RdmaSession>>>>,
    #[cfg(feature = "rdma")]
    next_id: Arc<AtomicU64>,
    #[cfg(feature = "rdma")]
    admission: Arc<tokio::sync::Semaphore>,
    device: Option<String>,
    ttl: Duration,
}

impl Default for RdmaSessionRegistry {
    fn default() -> Self {
        Self::new(None)
    }
}

impl RdmaSessionRegistry {
    #[must_use]
    pub fn new(device: Option<String>) -> Self {
        Self::with_ttl(device, SESSION_TTL)
    }

    #[must_use]
    pub fn with_ttl(device: Option<String>, ttl: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "rdma")]
            next_id: Arc::new(AtomicU64::new(1)),
            #[cfg(feature = "rdma")]
            admission: Arc::new(tokio::sync::Semaphore::new(MAX_RDMA_SESSIONS)),
            device,
            ttl,
        }
    }

    pub async fn session(&self, session_id: u64) -> Result<Arc<RdmaSession>, Status> {
        self.session_with_identity(session_id, None).await
    }

    pub async fn session_for(
        &self,
        session_id: u64,
        identity: &PeerSessionIdentity,
    ) -> Result<Arc<RdmaSession>, Status> {
        self.session_with_identity(session_id, Some(identity)).await
    }

    async fn session_with_identity(
        &self,
        session_id: u64,
        identity: Option<&PeerSessionIdentity>,
    ) -> Result<Arc<RdmaSession>, Status> {
        let session = self
            .inner
            .lock()
            .await
            .get(&session_id)
            .cloned()
            .ok_or_else(|| {
                coded_status(afs_error::NODE_RDMA_SESSION_UNKNOWN, "unknown RDMA session")
            })?;
        if session.peer_identity.as_ref() != identity {
            return Err(Status::permission_denied(
                "RDMA session belongs to another peer or transport scope",
            ));
        }
        if session.poisoned.load(Ordering::SeqCst) {
            return Err(coded_status(
                afs_error::NODE_RDMA_SESSION_POISONED,
                "RDMA session poisoned",
            ));
        }
        if !session.ready.load(Ordering::SeqCst) {
            #[cfg(feature = "rdma")]
            {
                // 工作任务持有 Arc 和 endpoint 锁直到 CQ 消费结束。即使 RPC future
                // 被取消，接收缓冲和队列也不会提前释放；并发首请求只消费一次探测。
                let pending = session.clone();
                crate::node::bridge::spawn_blocking(move || {
                    let mut endpoint = pending.endpoint.blocking_lock();
                    if pending.poisoned.load(Ordering::SeqCst) {
                        return Err(coded_status(
                            afs_error::NODE_RDMA_SESSION_POISONED,
                            "RDMA session poisoned",
                        ));
                    }
                    if !pending.ready.load(Ordering::SeqCst) {
                        if let Err(error) = endpoint.wait_probe(PROBE_TIMEOUT_MS) {
                            pending.poisoned.store(true, Ordering::SeqCst);
                            return Err(coded_status(
                                afs_error::NODE_TRANSFER_UNAVAILABLE,
                                error.to_string(),
                            ));
                        }
                        pending.ready.store(true, Ordering::SeqCst);
                    }
                    Ok(())
                })
                .await
                .map_err(|error| {
                    coded_status(afs_error::NODE_TRANSFER_INTERNAL, error.to_string())
                })??;
            }
            #[cfg(not(feature = "rdma"))]
            return Err(coded_status(
                afs_error::NODE_RDMA_NOT_READY,
                "RDMA session is not ready",
            ));
        }
        *session.last_used.lock().expect("last_used lock poisoned") = Instant::now();
        Ok(session)
    }

    /// 清理超过 TTL 或已经 poisoned 的 session。
    ///
    /// 这不是业务恢复机制，只是释放传输资源并阻止旧 session 继续访问 MR/QP。
    pub async fn cleanup_expired(&self) {
        let now = Instant::now();
        self.inner.lock().await.retain(|_, session| {
            !session.poisoned.load(Ordering::SeqCst)
                && now.duration_since(*session.last_used.lock().expect("last_used lock poisoned"))
                    < self.ttl
        });
    }

    #[cfg(feature = "rdma")]
    async fn reserve_admission(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        self.cleanup_expired().await;
        self.admission.clone().try_acquire_owned().map_err(|_| {
            coded_status(
                afs_error::NODE_RDMA_CAPACITY,
                "too many RDMA endpoints in this registry",
            )
        })
    }

    #[cfg(feature = "rdma")]
    async fn insert(&self, session: RdmaSession) -> Result<u64, Status> {
        self.cleanup_expired().await;
        let mut sessions = self.inner.lock().await;
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        sessions.insert(id, Arc::new(session));
        Ok(id)
    }

    // Close/TTL 关闭的是新请求入口，不是文件操作的取消或 drain 屏障。
    // 已取得 Arc 的请求可以完成；endpoint 最后一个持有者释放时才销毁 QP/MR。
    async fn remove(&self, session_id: u64) -> Result<(), Status> {
        self.close_with_identity(session_id, None).await
    }

    pub async fn close_for(
        &self,
        session_id: u64,
        identity: &PeerSessionIdentity,
    ) -> Result<(), Status> {
        self.close_with_identity(session_id, Some(identity)).await
    }

    async fn close_with_identity(
        &self,
        session_id: u64,
        identity: Option<&PeerSessionIdentity>,
    ) -> Result<(), Status> {
        let mut sessions = self.inner.lock().await;
        if let Some(session) = sessions.get(&session_id)
            && session.peer_identity.as_ref() != identity
        {
            return Err(Status::permission_denied(
                "RDMA close identity does not match session owner",
            ));
        }
        sessions.remove(&session_id);
        Ok(())
    }
}

/// 单个服务端 RDMA session 的状态。
///
/// `ready=false` 表示尚未消费真实 RDMA 探测接收完成；不是只缺一条 gRPC 确认。
/// `poisoned=true` 时表示某次 DMA/命令可能处于未知状态，必须拒绝复用。
pub struct RdmaSession {
    peer_identity: Option<PeerSessionIdentity>,
    #[cfg(feature = "rdma")]
    pub endpoint: Arc<RdmaServerEndpoint>,
    ready: AtomicBool,
    pub poisoned: AtomicBool,
    last_used: StdMutex<Instant>,
}

#[cfg(feature = "rdma")]
pub struct RdmaServerEndpoint {
    // Field order is intentional: the native endpoint must close before the
    // per-registry admission permit is released for reuse. The permit bounds
    // live AFS endpoint owners; a broken provider Drop can still leak native
    // resources outside what this admission counter can prove.
    endpoint: Mutex<RdmaEndpoint>,
    _admission: tokio::sync::OwnedSemaphorePermit,
}

#[cfg(feature = "rdma")]
impl RdmaServerEndpoint {
    fn new(endpoint: RdmaEndpoint, admission: tokio::sync::OwnedSemaphorePermit) -> Self {
        Self {
            endpoint: Mutex::new(endpoint),
            _admission: admission,
        }
    }
}

#[cfg(feature = "rdma")]
impl Deref for RdmaServerEndpoint {
    type Target = Mutex<RdmaEndpoint>;

    fn deref(&self) -> &Self::Target {
        &self.endpoint
    }
}

impl RdmaSession {
    #[cfg(feature = "rdma")]
    fn new(endpoint: RdmaServerEndpoint, peer_identity: Option<PeerSessionIdentity>) -> Self {
        Self {
            peer_identity,
            endpoint: Arc::new(endpoint),
            ready: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
            last_used: StdMutex::new(Instant::now()),
        }
    }
}

#[cfg(any(feature = "ownerfs", feature = "dfs"))]
const MAX_BLOCKING_LOCK_CONTROL_RPCS: usize = 128;

/// Bound on long advisory lock waits, shared by the OwnerFs and DFS control
/// services of one Node.
#[cfg(any(feature = "ownerfs", feature = "dfs"))]
#[derive(Clone)]
pub struct BlockingLockWaits(Arc<Semaphore>);

#[cfg(any(feature = "ownerfs", feature = "dfs"))]
impl Default for BlockingLockWaits {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(MAX_BLOCKING_LOCK_CONTROL_RPCS)))
    }
}

#[cfg(any(feature = "ownerfs", feature = "dfs"))]
impl BlockingLockWaits {
    async fn dispatch<Reply: Send + 'static>(
        &self,
        call: impl FnOnce() -> afs_error::Result<Reply> + Send + 'static,
    ) -> Result<Response<Reply>, Status> {
        let permit = self
            .0
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("too many blocking lock requests"))?;
        let (send, receive) = tokio::sync::oneshot::channel();
        // Long advisory waits never consume Tokio's ordinary blocking pool;
        // unlock, cancellation and lifecycle cleanup remain independently runnable.
        std::thread::Builder::new()
            .name("afs-lock-wait".into())
            .spawn(move || {
                let _permit = permit;
                let _ = send.send(call());
            })
            .map_err(|error| Status::resource_exhausted(error.to_string()))?;
        receive
            .await
            .map_err(|_| Status::internal("lock waiter worker exited"))?
            .map(Response::new)
            .map_err(error_to_status)
    }
}

/// Backend-neutral Node control: Ping and the diagnostic `NodeData` RDMA session.
#[derive(Clone)]
pub struct NodeControlService {
    registry: RdmaSessionRegistry,
}

impl NodeControlService {
    #[must_use]
    pub fn new(registry: RdmaSessionRegistry) -> Self {
        Self { registry }
    }

    pub fn into_server(self) -> NodeControlServer<Self> {
        NodeControlServer::new(self)
    }
}

pub fn make_control_server(registry: RdmaSessionRegistry) -> NodeControlServer<NodeControlService> {
    NodeControlServer::new(NodeControlService::new(registry))
}

#[tonic::async_trait]
impl NodeControl for NodeControlService {
    async fn negotiate_data(
        &self,
        request: Request<NegotiateDataRequest>,
    ) -> Result<Response<NegotiateDataReply>, Status> {
        let request = request.into_inner();
        if request.handshake_version != RDMA_HANDSHAKE_VERSION {
            return Err(coded_status(
                afs_error::NODE_RDMA_HANDSHAKE_VERSION,
                "unsupported RDMA handshake version",
            ));
        }
        let Some(device) = self.registry.device.clone() else {
            return Ok(Response::new(rdma_unsupported_reply()));
        };
        negotiate_rdma(&self.registry, device, request, None).await
    }

    async fn close_data(
        &self,
        request: Request<CloseDataRequest>,
    ) -> Result<Response<CloseDataReply>, Status> {
        let request = request.into_inner();
        self.registry.remove(request.session_id).await?;
        Ok(Response::new(CloseDataReply {}))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingReply>, Status> {
        async move {
            let payload = request.into_inner().payload;
            let payload = if payload == "ping" {
                "pong".into()
            } else {
                payload
            };
            Ok(Response::new(PingReply { payload }))
        }
        .instrument(afs_tracing::tracing::info_span!("node.control.ping"))
        .await
    }
}

#[cfg(feature = "rdma")]
/// 处理 NegotiateData：服务端创建自己的 endpoint，并把 server_info 返回给客户端。
///
/// 注意这里仍是控制面 gRPC；真正文件内容不会出现在这个 proto 里。
async fn negotiate_rdma(
    registry: &RdmaSessionRegistry,
    device: String,
    request: NegotiateDataRequest,
    identity: Option<PeerSessionIdentity>,
) -> Result<Response<NegotiateDataReply>, Status> {
    if request.client_info.len() != INFO_BYTES {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            "client_info must be 38 bytes",
        ));
    }
    let requested_capacity = request.capacity as usize;
    if requested_capacity == 0 || requested_capacity > MAX_CAPACITY {
        return Err(coded_status(
            afs_error::NODE_TRANSFER_INVALID,
            format!("client capacity must be within 1..={MAX_CAPACITY} bytes"),
        ));
    }
    let admission = registry.reserve_admission().await?;
    let client_info = request.client_info;
    let (endpoint, info) = crate::node::bridge::spawn_blocking(move || {
        let mut endpoint =
            RdmaEndpoint::open_with_capacity(&device, requested_capacity).map_err(native_status)?;
        let info = endpoint.info().map_err(native_status)?;
        // 必须先投递 RECV 再向客户端公开 endpoint，避免客户端探测到达时没有接收槽。
        endpoint.prepare_probe().map_err(native_status)?;
        endpoint.connect(&client_info).map_err(native_status)?;
        Ok::<_, Status>((RdmaServerEndpoint::new(endpoint, admission), info))
    })
    .await
    .map_err(|error| coded_status(afs_error::NODE_TRANSFER_INTERNAL, error.to_string()))??;
    let session_id = registry
        .insert(RdmaSession::new(endpoint, identity))
        .await?;
    Ok(Response::new(NegotiateDataReply {
        session_id,
        server_info: info.to_vec(),
        capacity: requested_capacity as u32,
        rdma_supported: true,
        handshake_version: RDMA_HANDSHAKE_VERSION,
    }))
}

#[cfg(not(feature = "rdma"))]
/// 未编译 RDMA 时仍保留控制 RPC，但仅返回“不支持”，不会创建 endpoint。
async fn negotiate_rdma(
    _registry: &RdmaSessionRegistry,
    _device: String,
    _request: NegotiateDataRequest,
    _identity: Option<PeerSessionIdentity>,
) -> Result<Response<NegotiateDataReply>, Status> {
    Ok(Response::new(NegotiateDataReply {
        session_id: 0,
        server_info: Vec::new(),
        capacity: 0,
        rdma_supported: false,
        handshake_version: RDMA_HANDSHAKE_VERSION,
    }))
}

#[cfg(feature = "rdma")]
fn native_status(error: afs_transport::rdma::RdmaError) -> Status {
    coded_status(afs_error::NODE_TRANSFER_UNAVAILABLE, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn legacy_handshake_is_rejected_before_allocating_rdma_resources() {
        let service = NodeControlService::new(RdmaSessionRegistry::new(None));
        let error = service
            .negotiate_data(Request::new(NegotiateDataRequest::default()))
            .await
            .expect_err("missing probe handshake version must not negotiate");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn ping_returns_pong_for_literal_ping() {
        let service = NodeControlService::new(RdmaSessionRegistry::new(None));

        let reply = service
            .ping(Request::new(PingRequest {
                payload: "ping".into(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.payload, "pong");
    }

    /// The other backend's service is simply not registered in a single-backend
    /// build: the peer sees gRPC UNIMPLEMENTED, classified as such on the client.
    #[tokio::test]
    async fn unregistered_backend_service_is_a_classified_unimplemented() {
        use afs_protocol::node_control::{
            DfsOwnerGetAttrRequest, OwnerGetLockRequest,
            dfs_owner_control_client::DfsOwnerControlClient,
            owner_control_client::OwnerControlClient,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(make_control_server(RdmaSessionRegistry::new(None)))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let channel = tonic::transport::Endpoint::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let owner = OwnerControlClient::new(channel.clone())
            .owner_get_lock(OwnerGetLockRequest::default())
            .await
            .unwrap_err();
        let dfs = DfsOwnerControlClient::new(channel)
            .dfs_owner_get_attr(DfsOwnerGetAttrRequest::default())
            .await
            .unwrap_err();
        server.abort();
        for status in [owner, dfs] {
            assert_eq!(status.code(), tonic::Code::Unimplemented);
            let error = afs_transport::grpc::error_status::status_to_error(status);
            assert_eq!(error.code(), afs_error::CLIENT_REMOTE_STATUS);
            assert_eq!(error.kind(), afs_error::ErrorKind::Unimplemented);
        }
    }

    #[tokio::test]
    async fn unknown_and_closed_sessions_are_rejected() {
        let registry = RdmaSessionRegistry::new(None);

        assert_eq!(
            session_error_code(&registry, 99).await,
            tonic::Code::FailedPrecondition
        );
        registry.remove(99).await.unwrap();
        assert_eq!(
            session_error_code(&registry, 99).await,
            tonic::Code::FailedPrecondition
        );
    }

    #[cfg(feature = "dfs")]
    #[tokio::test]
    async fn dfs_peer_negotiation_without_rdma_device_returns_canonical_unsupported() {
        let registry = RdmaSessionRegistry::new(None);
        let identity = PeerSessionIdentity::new("peer-a".into(), 7).unwrap();

        let reply = negotiate_for_peer(
            &registry,
            identity,
            NegotiateDataRequest {
                client_info: Vec::new(),
                capacity: 0,
                handshake_version: RDMA_HANDSHAKE_VERSION,
            },
        )
        .await
        .expect("missing device reports unsupported, not transport failure")
        .into_inner();

        assert!(!reply.rdma_supported);
        assert_eq!(reply.session_id, 0);
        assert!(reply.server_info.is_empty());
        assert_eq!(reply.capacity, 0);
        assert_eq!(reply.handshake_version, RDMA_HANDSHAKE_VERSION);
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn rdma_admission_is_per_registry_and_recovers_on_permit_drop() {
        let first = RdmaSessionRegistry::new(None);
        let second = RdmaSessionRegistry::new(None);
        let default = RdmaSessionRegistry::default();
        let mut retained = Vec::new();

        drop(
            default
                .reserve_admission()
                .await
                .expect("default registry has admission capacity"),
        );
        for _ in 0..MAX_RDMA_SESSIONS {
            retained.push(first.reserve_admission().await.expect("admission"));
        }

        let error = first
            .reserve_admission()
            .await
            .expect_err("first registry must be full");
        assert_eq!(
            afs_transport::grpc::error_status::status_to_error(error).code(),
            afs_error::NODE_RDMA_CAPACITY
        );

        let second_permit = second
            .reserve_admission()
            .await
            .expect("capacity is per registry");
        drop(second_permit);
        drop(retained.pop().expect("one retained permit"));
        let recovered = first
            .reserve_admission()
            .await
            .expect("admission recovers when a permit drops");
        drop(recovered);
        assert_eq!(first.admission.available_permits(), 1);
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn failed_rdma_native_allocation_releases_admission() {
        let registry =
            RdmaSessionRegistry::new(Some("afs-missing-rdma-device-for-admission-test".into()));
        assert_eq!(registry.admission.available_permits(), MAX_RDMA_SESSIONS);

        let error = negotiate_rdma(
            &registry,
            "afs-missing-rdma-device-for-admission-test".into(),
            NegotiateDataRequest {
                client_info: vec![0; INFO_BYTES],
                capacity: 1,
                handshake_version: RDMA_HANDSHAKE_VERSION,
            },
            None,
        )
        .await
        .expect_err("missing device should fail native allocation");
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(
            afs_transport::grpc::error_status::status_to_error(error).code(),
            afs_error::NODE_TRANSFER_UNAVAILABLE
        );
        assert_eq!(registry.admission.available_permits(), MAX_RDMA_SESSIONS);
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn concurrent_rdma_admission_is_bounded_per_registry() {
        let registry = Arc::new(RdmaSessionRegistry::new(None));
        let barrier = Arc::new(tokio::sync::Barrier::new(MAX_RDMA_SESSIONS + 1));
        let mut handles = Vec::new();

        for _ in 0..(MAX_RDMA_SESSIONS * 2) {
            let registry = registry.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                match registry.reserve_admission().await {
                    Ok(permit) => {
                        barrier.wait().await;
                        Some(permit)
                    }
                    Err(error) => {
                        assert_eq!(
                            afs_transport::grpc::error_status::status_to_error(error).code(),
                            afs_error::NODE_RDMA_CAPACITY
                        );
                        None
                    }
                }
            }));
        }

        tokio::time::timeout(Duration::from_secs(5), barrier.wait())
            .await
            .expect("64 concurrent reservations should reach the barrier");
        let mut retained = Vec::new();
        let mut rejected = 0usize;
        for handle in handles {
            match handle.await.expect("admission task joined") {
                Some(permit) => retained.push(permit),
                None => rejected += 1,
            }
        }
        assert_eq!(retained.len(), MAX_RDMA_SESSIONS);
        assert_eq!(rejected, MAX_RDMA_SESSIONS);
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    #[ignore = "requires a Linux RXE device from AFS_RDMA_DEVICE"]
    async fn closed_rdma_endpoints_retain_admission_until_last_owner_drops() {
        let device = std::env::var("AFS_RDMA_DEVICE")
            .or_else(|_| std::env::var("AFS_TEST_RDMA_DEVICE"))
            .expect("set AFS_RDMA_DEVICE or AFS_TEST_RDMA_DEVICE to a Linux RXE device");
        let registry = RdmaSessionRegistry::new(Some(device.clone()));
        let mut client = tokio::task::spawn_blocking({
            let device = device.clone();
            move || RdmaEndpoint::open_with_capacity(&device, 1)
        })
        .await
        .expect("client task joined")
        .expect("client endpoint");
        let client_info = client.info().expect("client info").to_vec();
        let mut retained = Vec::new();

        for _ in 0..MAX_RDMA_SESSIONS {
            let reply = negotiate_rdma(
                &registry,
                device.clone(),
                NegotiateDataRequest {
                    client_info: client_info.clone(),
                    capacity: 1,
                    handshake_version: RDMA_HANDSHAKE_VERSION,
                },
                None,
            )
            .await
            .expect("negotiate")
            .into_inner();
            let session = registry
                .inner
                .lock()
                .await
                .get(&reply.session_id)
                .cloned()
                .expect("session entry");
            retained.push(session.endpoint.clone());
            registry
                .remove(reply.session_id)
                .await
                .expect("close entry");
            drop(session);
        }

        let error = negotiate_rdma(
            &registry,
            device.clone(),
            NegotiateDataRequest {
                client_info: client_info.clone(),
                capacity: 1,
                handshake_version: RDMA_HANDSHAKE_VERSION,
            },
            None,
        )
        .await
        .expect_err("retained endpoints must keep admission at capacity");
        assert_eq!(
            afs_transport::grpc::error_status::status_to_error(error).code(),
            afs_error::NODE_RDMA_CAPACITY
        );

        drop(retained);
        let reply = negotiate_rdma(
            &registry,
            device,
            NegotiateDataRequest {
                client_info,
                capacity: 1,
                handshake_version: RDMA_HANDSHAKE_VERSION,
            },
            None,
        )
        .await
        .expect("admission recovers after endpoint owners drop")
        .into_inner();
        registry.remove(reply.session_id).await.expect("cleanup");
        drop(client);
    }

    #[cfg(not(feature = "rdma"))]
    #[tokio::test]
    async fn cleanup_expired_removes_stale_entries_without_long_waits() {
        let registry = RdmaSessionRegistry::with_ttl(None, Duration::from_millis(1));
        registry.inner.lock().await.insert(
            1,
            Arc::new(RdmaSession {
                peer_identity: None,
                ready: AtomicBool::new(true),
                poisoned: AtomicBool::new(false),
                last_used: StdMutex::new(Instant::now() - Duration::from_secs(1)),
            }),
        );

        registry.cleanup_expired().await;

        assert_eq!(
            session_error_code(&registry, 1).await,
            tonic::Code::FailedPrecondition
        );
    }

    async fn session_error_code(registry: &RdmaSessionRegistry, session_id: u64) -> tonic::Code {
        match registry.session(session_id).await {
            Ok(_) => panic!("session unexpectedly exists"),
            Err(status) => status.code(),
        }
    }
    #[cfg(not(feature = "rdma"))]
    #[tokio::test]
    async fn bound_session_rejects_diagnostic_and_other_peer_access() {
        let registry = RdmaSessionRegistry::new(None);
        let owner = PeerSessionIdentity::new("node-a".into(), 1).unwrap();
        let other = PeerSessionIdentity::new("node-b".into(), 1).unwrap();
        registry.inner.lock().await.insert(
            7,
            Arc::new(RdmaSession {
                peer_identity: Some(owner.clone()),
                ready: AtomicBool::new(true),
                poisoned: AtomicBool::new(false),
                last_used: StdMutex::new(Instant::now()),
            }),
        );
        assert!(registry.session(7).await.is_err());
        assert!(registry.remove(7).await.is_err());
        assert!(registry.session_for(7, &other).await.is_err());
        assert!(registry.close_for(7, &other).await.is_err());
        assert!(registry.session_for(7, &owner).await.is_ok());
        registry.close_for(7, &owner).await.unwrap();
        assert!(registry.session_for(7, &owner).await.is_err());
    }

    #[cfg(feature = "ownerfs")]
    fn owner_access() -> crate::node::vfs::ownerfs::root::PresentedRootAccess {
        crate::node::vfs::ownerfs::root::PresentedRootAccess {
            id: crate::node::vfs::ownerfs::root::RootId("root-a".into()),
            epoch: 11,
            home_node_id: "home-a".into(),
            home_session_id: "home-session-a".into(),
            holder_node_id: "node-b".into(),
            session_id: "holder-session-a".into(),
            access_generation: 17,
            fencing_token: "fence-a".into(),
        }
    }

    #[cfg(feature = "ownerfs")]
    fn owner_identity(
        peer: &str,
        access: crate::node::vfs::ownerfs::root::PresentedRootAccess,
    ) -> PeerSessionIdentity {
        PeerSessionIdentity::for_owner(peer.to_owned(), access).unwrap()
    }

    #[cfg(feature = "ownerfs")]
    #[test]
    fn owner_peer_session_identity_binds_every_root_access_field() {
        let base_access = owner_access();
        let base = owner_identity("node-b", base_access.clone());

        let mut changed = base_access.clone();
        changed.id = crate::node::vfs::ownerfs::root::RootId("root-b".into());
        assert_ne!(base, owner_identity("node-b", changed));

        let mut changed = base_access.clone();
        changed.epoch += 1;
        assert_ne!(base, owner_identity("node-b", changed));

        let mut changed = base_access.clone();
        changed.home_node_id = "home-b".into();
        assert_ne!(base, owner_identity("node-b", changed));

        let mut changed = base_access.clone();
        changed.home_session_id = "home-session-b".into();
        assert_ne!(base, owner_identity("node-b", changed));

        let mut changed = base_access.clone();
        changed.holder_node_id = "node-c".into();
        assert_ne!(base, owner_identity("node-c", changed));

        let mut changed = base_access.clone();
        changed.session_id = "holder-session-b".into();
        assert_ne!(base, owner_identity("node-b", changed));

        let mut changed = base_access.clone();
        changed.access_generation += 1;
        assert_ne!(base, owner_identity("node-b", changed));

        let mut changed = base_access.clone();
        changed.fencing_token = "fence-b".into();
        assert_ne!(base, owner_identity("node-b", changed));
    }

    #[cfg(feature = "ownerfs")]
    #[test]
    fn owner_peer_session_identity_is_structured_not_colon_encoded() {
        let mut left = owner_access();
        left.id = crate::node::vfs::ownerfs::root::RootId("root:with".into());
        left.home_node_id = "home".into();

        let mut right = owner_access();
        right.id = crate::node::vfs::ownerfs::root::RootId("root".into());
        right.home_node_id = "with:home".into();

        assert_ne!(
            owner_identity("node-b", left),
            owner_identity("node-b", right)
        );
    }

    #[cfg(feature = "ownerfs")]
    #[test]
    fn dfs_and_owner_peer_session_identities_do_not_alias() {
        let access = owner_access();
        assert_ne!(
            PeerSessionIdentity::new("node-b".into(), access.epoch).unwrap(),
            owner_identity("node-b", access)
        );
    }

    #[cfg(all(feature = "ownerfs", not(feature = "rdma")))]
    #[tokio::test]
    async fn owner_bound_session_rejects_forged_scope_lookup_and_close() {
        let registry = RdmaSessionRegistry::new(None);
        let owner = owner_identity("node-b", owner_access());
        registry.inner.lock().await.insert(
            8,
            Arc::new(RdmaSession {
                peer_identity: Some(owner.clone()),
                ready: AtomicBool::new(true),
                poisoned: AtomicBool::new(false),
                last_used: StdMutex::new(Instant::now()),
            }),
        );

        let mut forged_access = owner_access();
        forged_access.fencing_token = "forged-fence".into();
        let forged = owner_identity("node-b", forged_access);

        assert!(registry.session(8).await.is_err());
        assert!(registry.session_for(8, &forged).await.is_err());
        assert!(registry.close_for(8, &forged).await.is_err());
        assert!(registry.session_for(8, &owner).await.is_ok());
        registry.close_for(8, &owner).await.unwrap();
        assert!(registry.session_for(8, &owner).await.is_err());
    }
}
