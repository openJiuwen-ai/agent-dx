//! Node → Node 控制 Handler。
//!
//! 这个文件实现 node_control.proto 生成的 gRPC service，负责“建立/关闭数据
//! 传输会话”和基础 ping。它不传文件内容，不代替 Meta 提交新的访问授权，
//! 也不决定文件读写是否成功。
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

use afs_transport::grpc::error_status::coded_status;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(any(feature = "ownerfs", feature = "dfs"))]
use crate::node::rpc::data::{PeerAuthenticator, authenticate_peer};
#[cfg(feature = "dfs")]
use crate::node::vfs::types::OpenOptions;
use afs_protocol::node_control::{
    CloseDataReply, CloseDataRequest, DfsOwnerAcknowledgeLockWaitReply,
    DfsOwnerAcknowledgeLockWaitRequest, DfsOwnerCancelLockWaitReply, DfsOwnerCancelLockWaitRequest,
    DfsOwnerGetAttrReply, DfsOwnerGetAttrRequest, DfsOwnerGetLockReply, DfsOwnerGetLockRequest,
    DfsOwnerMknodReply, DfsOwnerMknodRequest, DfsOwnerOpenReply, DfsOwnerOpenRequest,
    DfsOwnerReleaseLockSessionReply, DfsOwnerReleaseLockSessionRequest, DfsOwnerReleaseLocksReply,
    DfsOwnerReleaseLocksRequest, DfsOwnerReleaseReply, DfsOwnerReleaseRequest,
    DfsOwnerSetLockReply, DfsOwnerSetLockRequest, NegotiateDataReply, NegotiateDataRequest,
    PingReply, PingRequest,
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
pub const RDMA_HANDSHAKE_VERSION: u32 = 1;
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
                tokio::task::spawn_blocking(move || {
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

#[cfg(feature = "dfs")]
pub trait DfsOwnerLifecycleHandler: Send + Sync + 'static {
    fn open(
        &self,
        _peer: &str,
        _request: DfsOwnerOpenRequest,
    ) -> afs_error::Result<DfsOwnerOpenReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }

    fn open_with_options(
        &self,
        peer: &str,
        request: DfsOwnerOpenRequest,
        options: OpenOptions,
    ) -> afs_error::Result<DfsOwnerOpenReply> {
        if options.kill_suidgid {
            return Err(dfs_owner_lifecycle_unimplemented());
        }
        self.open(peer, request)
    }
    fn getattr(
        &self,
        _peer: &str,
        _request: DfsOwnerGetAttrRequest,
    ) -> afs_error::Result<DfsOwnerGetAttrReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn mknod(
        &self,
        _peer: &str,
        _request: DfsOwnerMknodRequest,
    ) -> afs_error::Result<DfsOwnerMknodReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn get_lock(
        &self,
        _peer: &str,
        _request: DfsOwnerGetLockRequest,
    ) -> afs_error::Result<DfsOwnerGetLockReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn set_lock(
        &self,
        _peer: &str,
        _request: DfsOwnerSetLockRequest,
    ) -> afs_error::Result<DfsOwnerSetLockReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn cancel_lock_wait(
        &self,
        _peer: &str,
        _request: DfsOwnerCancelLockWaitRequest,
    ) -> afs_error::Result<DfsOwnerCancelLockWaitReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn acknowledge_lock_wait(
        &self,
        _peer: &str,
        _request: DfsOwnerAcknowledgeLockWaitRequest,
    ) -> afs_error::Result<DfsOwnerAcknowledgeLockWaitReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn release_locks(
        &self,
        _peer: &str,
        _request: DfsOwnerReleaseLocksRequest,
    ) -> afs_error::Result<DfsOwnerReleaseLocksReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn release_lock_session(
        &self,
        _peer: &str,
        _request: DfsOwnerReleaseLockSessionRequest,
    ) -> afs_error::Result<DfsOwnerReleaseLockSessionReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn release(
        &self,
        _peer: &str,
        _request: DfsOwnerReleaseRequest,
    ) -> afs_error::Result<DfsOwnerReleaseReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
}

#[cfg(feature = "dfs")]
fn dfs_owner_lifecycle_unimplemented() -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::NODE_VFS_UNIMPLEMENTED,
        "DFS remote inode owner lifecycle is not wired",
    )
}

#[cfg(any(feature = "ownerfs", feature = "dfs"))]
const MAX_BLOCKING_LOCK_CONTROL_RPCS: usize = 128;

#[derive(Clone)]
pub struct NodeControlService {
    registry: RdmaSessionRegistry,
    #[cfg(feature = "ownerfs")]
    owner_rdma_registry: Option<RdmaSessionRegistry>,
    #[cfg(feature = "dfs")]
    dfs_owner: Option<Arc<dyn DfsOwnerLifecycleHandler>>,
    #[cfg(feature = "dfs")]
    dfs_authenticator: Option<Arc<dyn PeerAuthenticator>>,
    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    lock_waits: Arc<Semaphore>,
    #[cfg(feature = "ownerfs")]
    owner_locks: Option<crate::node::vfs::ownerfs::OwnerFsPeerExecutor>,
    #[cfg(feature = "ownerfs")]
    owner_authenticator: Option<Arc<dyn PeerAuthenticator>>,
}

impl NodeControlService {
    #[cfg(feature = "ownerfs")]
    pub fn with_owner_locks(
        mut self,
        handler: crate::node::vfs::ownerfs::OwnerFsPeerExecutor,
        authenticator: Arc<dyn PeerAuthenticator>,
    ) -> Self {
        self.owner_locks = Some(handler);
        self.owner_authenticator = Some(authenticator);
        self
    }

    #[cfg(feature = "ownerfs")]
    #[must_use]
    pub fn with_owner_rdma_registry(mut self, registry: RdmaSessionRegistry) -> Self {
        self.owner_rdma_registry = Some(registry);
        self
    }

    pub fn into_server(self) -> NodeControlServer<Self> {
        NodeControlServer::new(self)
    }

    #[cfg(any(feature = "ownerfs", feature = "dfs"))]
    async fn dispatch_blocking_lock_call<Reply: Send + 'static>(
        &self,
        call: impl FnOnce() -> afs_error::Result<Reply> + Send + 'static,
    ) -> Result<Response<Reply>, Status> {
        let permit = self
            .lock_waits
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

    #[cfg(feature = "ownerfs")]
    async fn dispatch_owner_lock<Req: Send + 'static, Reply: Send + 'static>(
        &self,
        request: Request<Req>,
        blocking: bool,
        call: impl FnOnce(
            crate::node::vfs::ownerfs::OwnerFsPeerExecutor,
            String,
            Req,
        ) -> afs_error::Result<Reply>
        + Send
        + 'static,
    ) -> Result<Response<Reply>, Status> {
        let handler = self
            .owner_locks
            .clone()
            .ok_or_else(|| Status::unimplemented("Owner lock authority not wired"))?;
        let authenticator = self
            .owner_authenticator
            .as_ref()
            .ok_or_else(|| Status::permission_denied("Owner locks require authenticated peer"))?;
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        let body = request.into_inner();
        if blocking {
            return self
                .dispatch_blocking_lock_call(move || call(handler, peer, body))
                .await;
        }
        tokio::task::spawn_blocking(move || call(handler, peer, body))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map(Response::new)
            .map_err(error_to_status)
    }

    #[must_use]
    pub fn new(registry: RdmaSessionRegistry) -> Self {
        Self {
            registry,
            #[cfg(feature = "ownerfs")]
            owner_rdma_registry: None,
            #[cfg(feature = "dfs")]
            dfs_owner: None,
            #[cfg(feature = "dfs")]
            dfs_authenticator: None,
            #[cfg(any(feature = "ownerfs", feature = "dfs"))]
            lock_waits: Arc::new(Semaphore::new(MAX_BLOCKING_LOCK_CONTROL_RPCS)),
            #[cfg(feature = "ownerfs")]
            owner_locks: None,
            #[cfg(feature = "ownerfs")]
            owner_authenticator: None,
        }
    }

    #[cfg(feature = "dfs")]
    #[must_use]
    pub fn with_dfs_owner(
        registry: RdmaSessionRegistry,
        handler: Arc<dyn DfsOwnerLifecycleHandler>,
        authenticator: Arc<dyn PeerAuthenticator>,
    ) -> Self {
        Self {
            registry,
            #[cfg(feature = "ownerfs")]
            owner_rdma_registry: None,
            dfs_owner: Some(handler),
            dfs_authenticator: Some(authenticator),
            lock_waits: Arc::new(Semaphore::new(MAX_BLOCKING_LOCK_CONTROL_RPCS)),
            #[cfg(feature = "ownerfs")]
            owner_locks: None,
            #[cfg(feature = "ownerfs")]
            owner_authenticator: None,
        }
    }

    #[cfg(feature = "dfs")]
    async fn dispatch_dfs_owner<Req: Send + 'static, Reply: Send + 'static>(
        &self,
        request: Request<Req>,
        call: impl FnOnce(Arc<dyn DfsOwnerLifecycleHandler>, String, Req) -> afs_error::Result<Reply>
        + Send
        + 'static,
    ) -> Result<Response<Reply>, Status> {
        let handler = self
            .dfs_owner
            .clone()
            .ok_or_else(|| error_to_status(dfs_owner_lifecycle_unimplemented()))?;
        let authenticator = self
            .dfs_authenticator
            .as_ref()
            .ok_or_else(|| Status::permission_denied("DFS owner has no peer authenticator"))?;
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        tokio::task::spawn_blocking(move || call(handler, peer, request))
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map(Response::new)
            .map_err(error_to_status)
    }

    #[cfg(feature = "dfs")]
    async fn dispatch_blocking_dfs_owner_lock<Req: Send + 'static, Reply: Send + 'static>(
        &self,
        request: Request<Req>,
        call: impl FnOnce(Arc<dyn DfsOwnerLifecycleHandler>, String, Req) -> afs_error::Result<Reply>
        + Send
        + 'static,
    ) -> Result<Response<Reply>, Status> {
        let handler = self
            .dfs_owner
            .clone()
            .ok_or_else(|| error_to_status(dfs_owner_lifecycle_unimplemented()))?;
        let authenticator = self
            .dfs_authenticator
            .as_ref()
            .ok_or_else(|| Status::permission_denied("DFS owner has no peer authenticator"))?;
        let peer = authenticate_peer(authenticator.as_ref(), &request)?;
        let request = request.into_inner();
        self.dispatch_blocking_lock_call(move || call(handler, peer, request))
            .await
    }
}

pub fn make_control_server(registry: RdmaSessionRegistry) -> NodeControlServer<NodeControlService> {
    NodeControlServer::new(NodeControlService::new(registry))
}

#[cfg(feature = "dfs")]
pub fn make_control_server_with_dfs_owner(
    registry: RdmaSessionRegistry,
    handler: Arc<dyn DfsOwnerLifecycleHandler>,
    authenticator: Arc<dyn PeerAuthenticator>,
) -> NodeControlServer<NodeControlService> {
    NodeControlServer::new(NodeControlService::with_dfs_owner(
        registry,
        handler,
        authenticator,
    ))
}

#[tonic::async_trait]
impl NodeControl for NodeControlService {
    #[cfg(feature = "ownerfs")]
    async fn owner_get_lock(
        &self,
        request: Request<afs_protocol::node_control::OwnerGetLockRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerGetLockReply>, Status> {
        self.dispatch_owner_lock(request, false, |handler, peer, body| {
            let (access, file) = owner_lock_handle_from_wire(body.handle)?;
            let lock = owner_lock_from_wire(body.lock)?;
            let conflict = handler.getlk(&peer, &access, &file, lock)?.map(|conflict| {
                owner_lock_to_wire(&crate::node::vfs::locks::LockRequest {
                    kind: conflict.kind,
                    owner: conflict.owner,
                    pid: conflict.pid,
                    range: conflict.range,
                    lock_type: conflict.lock_type,
                })
            });
            Ok(afs_protocol::node_control::OwnerGetLockReply { conflict })
        })
        .await
    }
    #[cfg(feature = "ownerfs")]
    async fn owner_set_lock(
        &self,
        request: Request<afs_protocol::node_control::OwnerSetLockRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerSetLockReply>, Status> {
        let blocking = request.get_ref().waiter.is_some();
        let mut cancel_guard = if let Some(waiter) = request.get_ref().waiter.clone() {
            let handler = self
                .owner_locks
                .clone()
                .ok_or_else(|| Status::unimplemented("Owner lock authority not wired"))?;
            let authenticator = self.owner_authenticator.as_ref().ok_or_else(|| {
                Status::permission_denied("Owner locks require authenticated peer")
            })?;
            let peer = authenticate_peer(authenticator.as_ref(), &request)?;
            let (access, _) = owner_lock_handle_from_wire(request.get_ref().handle.clone())
                .map_err(error_to_status)?;
            let waiter = owner_lock_waiter_from_wire(waiter).map_err(error_to_status)?;
            Some(OwnerLockCancelOnDrop {
                handler,
                peer,
                access,
                waiter: Some(waiter),
            })
        } else {
            None
        };
        let result = self
            .dispatch_owner_lock(request, blocking, |handler, peer, body| {
                let (access, file) = owner_lock_handle_from_wire(body.handle)?;
                let lock = owner_lock_from_wire(body.lock)?;
                let waiter = body.waiter.map(owner_lock_waiter_from_wire).transpose()?;
                handler.setlk(&peer, &access, &file, lock, waiter)?;
                Ok(afs_protocol::node_control::OwnerSetLockReply {})
            })
            .await;
        if let Some(guard) = &mut cancel_guard {
            guard.waiter = None;
        }
        result
    }
    #[cfg(feature = "ownerfs")]
    async fn owner_ack_lock_wait(
        &self,
        request: Request<afs_protocol::node_control::OwnerAckLockWaitRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerAckLockWaitReply>, Status> {
        self.dispatch_owner_lock(request, false, |handler, peer, body| {
            let access = super::data::presented_access(body.access)?;
            let waiter =
                owner_lock_waiter_from_wire(body.waiter.ok_or_else(owner_lock_wire_invalid)?)?;
            handler.acknowledge_lock_wait(&peer, &access, waiter)?;
            Ok(afs_protocol::node_control::OwnerAckLockWaitReply {})
        })
        .await
    }
    #[cfg(not(feature = "ownerfs"))]
    async fn owner_ack_lock_wait(
        &self,
        _: Request<afs_protocol::node_control::OwnerAckLockWaitRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerAckLockWaitReply>, Status> {
        Err(Status::unimplemented("OwnerFs is not enabled"))
    }
    #[cfg(feature = "ownerfs")]
    async fn owner_cancel_lock_wait(
        &self,
        request: Request<afs_protocol::node_control::OwnerCancelLockWaitRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerCancelLockWaitReply>, Status> {
        self.dispatch_owner_lock(request, false, |handler, peer, body| {
            let access = super::data::presented_access(body.access)?;
            let waiter =
                owner_lock_waiter_from_wire(body.waiter.ok_or_else(owner_lock_wire_invalid)?)?;
            let outcome = handler.cancel_lock_wait(&peer, &access, waiter)?;
            Ok(afs_protocol::node_control::OwnerCancelLockWaitReply {
                outcome: match outcome {
                    crate::node::vfs::locks::LockWaiterOutcome::Cancelled => 1,
                    crate::node::vfs::locks::LockWaiterOutcome::Granted => 2,
                    crate::node::vfs::locks::LockWaiterOutcome::Unknown => 3,
                },
            })
        })
        .await
    }
    #[cfg(feature = "ownerfs")]
    async fn owner_release_locks(
        &self,
        request: Request<afs_protocol::node_control::OwnerReleaseLocksRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerReleaseLocksReply>, Status> {
        self.dispatch_owner_lock(request, false, |handler, peer, body| {
            let (access, file) = owner_lock_handle_from_wire(body.handle)?;
            if body.ingress_session_id.is_empty() || body.ingress_session_id.len() > 256 {
                return Err(owner_lock_wire_invalid());
            }
            let kind = match body.release_kind {
                1 => crate::node::vfs::types::ReleaseKind::PosixOwner,
                2 => crate::node::vfs::types::ReleaseKind::FlockOwner,
                _ => return Err(owner_lock_wire_invalid()),
            };
            handler.release_locks(
                &peer,
                &access,
                &file,
                crate::node::vfs::types::FileLockOwner {
                    ingress_session_id: body.ingress_session_id,
                    kernel_owner: body.kernel_owner,
                },
                kind,
            )?;
            Ok(afs_protocol::node_control::OwnerReleaseLocksReply {})
        })
        .await
    }
    #[cfg(feature = "ownerfs")]
    async fn owner_release_lock_session(
        &self,
        request: Request<afs_protocol::node_control::OwnerReleaseLockSessionRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerReleaseLockSessionReply>, Status> {
        self.dispatch_owner_lock(request, false, |handler, peer, body| {
            let access = super::data::presented_access(body.access)?;
            if body.ingress_session_id.is_empty() || body.ingress_session_id.len() > 256 {
                return Err(owner_lock_wire_invalid());
            }
            handler.release_lock_session(&peer, &access, &body.ingress_session_id)?;
            Ok(afs_protocol::node_control::OwnerReleaseLockSessionReply {})
        })
        .await
    }

    #[cfg(not(feature = "ownerfs"))]
    async fn owner_get_lock(
        &self,
        _: Request<afs_protocol::node_control::OwnerGetLockRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerGetLockReply>, Status> {
        Err(Status::unimplemented("OwnerFs is not enabled"))
    }
    #[cfg(not(feature = "ownerfs"))]
    async fn owner_set_lock(
        &self,
        _: Request<afs_protocol::node_control::OwnerSetLockRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerSetLockReply>, Status> {
        Err(Status::unimplemented("OwnerFs is not enabled"))
    }
    #[cfg(not(feature = "ownerfs"))]
    async fn owner_cancel_lock_wait(
        &self,
        _: Request<afs_protocol::node_control::OwnerCancelLockWaitRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerCancelLockWaitReply>, Status> {
        Err(Status::unimplemented("OwnerFs is not enabled"))
    }
    #[cfg(not(feature = "ownerfs"))]
    async fn owner_release_locks(
        &self,
        _: Request<afs_protocol::node_control::OwnerReleaseLocksRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerReleaseLocksReply>, Status> {
        Err(Status::unimplemented("OwnerFs is not enabled"))
    }
    #[cfg(not(feature = "ownerfs"))]
    async fn owner_release_lock_session(
        &self,
        _: Request<afs_protocol::node_control::OwnerReleaseLockSessionRequest>,
    ) -> Result<Response<afs_protocol::node_control::OwnerReleaseLockSessionReply>, Status> {
        Err(Status::unimplemented("OwnerFs is not enabled"))
    }
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

    async fn owner_negotiate_data(
        &self,
        request: Request<afs_protocol::node_control::OwnerNegotiateDataRequest>,
    ) -> Result<Response<NegotiateDataReply>, Status> {
        #[cfg(not(feature = "ownerfs"))]
        {
            let _ = request;
            Err(Status::permission_denied("OwnerFs is not enabled"))
        }
        #[cfg(feature = "ownerfs")]
        {
            let authenticator = self.owner_authenticator.as_ref().ok_or_else(|| {
                Status::permission_denied("OwnerFiles RDMA requires authenticated peer")
            })?;
            let peer = authenticate_peer(authenticator.as_ref(), &request)?;
            let body = request.into_inner();
            let access = body
                .access
                .ok_or_else(|| Status::permission_denied("OwnerFiles RDMA access is required"))?;
            let presented_access =
                super::data::presented_access(Some(access.clone())).map_err(error_to_status)?;
            let handler = self
                .owner_locks
                .clone()
                .ok_or_else(|| Status::unimplemented("OwnerFs peer authority is not wired"))?;
            let auth_peer = peer.clone();
            let auth_access = presented_access.clone();
            tokio::task::spawn_blocking(move || {
                handler.authorize_transport(&auth_peer, &auth_access)
            })
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .map_err(error_to_status)?;
            let identity = PeerSessionIdentity::for_owner(peer, presented_access)?;
            let negotiation = body.negotiation.ok_or_else(|| {
                Status::invalid_argument("OwnerFiles RDMA negotiation is required")
            })?;
            if negotiation.handshake_version != RDMA_HANDSHAKE_VERSION {
                return Err(coded_status(
                    afs_error::NODE_RDMA_HANDSHAKE_VERSION,
                    "unsupported RDMA handshake version",
                ));
            }
            let registry = self
                .owner_rdma_registry
                .as_ref()
                .ok_or_else(|| Status::unimplemented("OwnerFiles RDMA is not configured"))?;
            let Some(device) = registry.device.clone() else {
                return Ok(Response::new(rdma_unsupported_reply()));
            };
            negotiate_rdma(registry, device, negotiation, Some(identity)).await
        }
    }

    async fn owner_close_data(
        &self,
        request: Request<afs_protocol::node_control::OwnerCloseDataRequest>,
    ) -> Result<Response<CloseDataReply>, Status> {
        #[cfg(not(feature = "ownerfs"))]
        {
            let _ = request;
            Err(Status::permission_denied("OwnerFs is not enabled"))
        }
        #[cfg(feature = "ownerfs")]
        {
            let authenticator = self.owner_authenticator.as_ref().ok_or_else(|| {
                Status::permission_denied("OwnerFiles RDMA requires authenticated peer")
            })?;
            let peer = authenticate_peer(authenticator.as_ref(), &request)?;
            let body = request.into_inner();
            let access = body
                .access
                .ok_or_else(|| Status::permission_denied("OwnerFiles RDMA access is required"))?;
            let identity = owner_rdma_identity_from_wire(peer, access)?;
            let registry = self
                .owner_rdma_registry
                .as_ref()
                .ok_or_else(|| Status::unimplemented("OwnerFiles RDMA is not configured"))?;
            registry.close_for(body.session_id, &identity).await?;
            Ok(Response::new(CloseDataReply {}))
        }
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

    #[cfg(feature = "dfs")]
    async fn dfs_owner_open(
        &self,
        request: Request<DfsOwnerOpenRequest>,
    ) -> Result<Response<DfsOwnerOpenReply>, Status> {
        let body = request.get_ref();
        if body.namespace_id.is_empty()
            || body.inode_id.is_empty()
            || body.owner_node_id.is_empty()
            || body.owner_session_id.is_empty()
            || body.lease_epoch == 0
            || body.caller_session_id.is_empty()
            || body.open_seq == 0
        {
            return Err(Status::permission_denied(
                "DFS owner open identity is incomplete",
            ));
        }
        let options = OpenOptions {
            kill_suidgid: body.kill_suidgid,
        };
        self.dispatch_dfs_owner(request, move |handler, peer, request| {
            handler.open_with_options(&peer, request, options)
        })
        .await
    }

    #[cfg(feature = "dfs")]
    async fn dfs_owner_get_attr(
        &self,
        request: Request<DfsOwnerGetAttrRequest>,
    ) -> Result<Response<DfsOwnerGetAttrReply>, Status> {
        validate_dfs_owner_control_handle(request.get_ref().handle.as_ref())?;
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.getattr(&peer, request)
        })
        .await
    }

    #[cfg(feature = "dfs")]
    async fn dfs_owner_mknod(
        &self,
        request: Request<DfsOwnerMknodRequest>,
    ) -> Result<Response<DfsOwnerMknodReply>, Status> {
        validate_dfs_owner_mknod_request(request.get_ref())?;
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.mknod(&peer, request)
        })
        .await
    }

    #[cfg(feature = "dfs")]
    async fn dfs_owner_get_lock(
        &self,
        request: Request<DfsOwnerGetLockRequest>,
    ) -> Result<Response<DfsOwnerGetLockReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        validate_dfs_owner_lock_request(request.get_ref().lock.as_ref())?;
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.get_lock(&peer, request)
        })
        .await
    }

    #[cfg(feature = "dfs")]
    async fn dfs_owner_set_lock(
        &self,
        request: Request<DfsOwnerSetLockRequest>,
    ) -> Result<Response<DfsOwnerSetLockReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        validate_dfs_owner_lock_request(request.get_ref().lock.as_ref())?;
        if let Some(waiter) = request.get_ref().waiter.as_ref() {
            validate_dfs_owner_lock_waiter(waiter)?;
        }
        if request.get_ref().waiter.is_some() {
            self.dispatch_blocking_dfs_owner_lock(request, |handler, peer, request| {
                handler.set_lock(&peer, request)
            })
            .await
        } else {
            self.dispatch_dfs_owner(request, |handler, peer, request| {
                handler.set_lock(&peer, request)
            })
            .await
        }
    }

    #[cfg(feature = "dfs")]
    async fn dfs_owner_cancel_lock_wait(
        &self,
        request: Request<DfsOwnerCancelLockWaitRequest>,
    ) -> Result<Response<DfsOwnerCancelLockWaitReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        let waiter = request
            .get_ref()
            .waiter
            .as_ref()
            .ok_or_else(|| Status::permission_denied("DFS lock waiter is required"))?;
        validate_dfs_owner_lock_waiter(waiter)?;
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.cancel_lock_wait(&peer, request)
        })
        .await
    }

    #[cfg(feature = "dfs")]
    async fn dfs_owner_acknowledge_lock_wait(
        &self,
        request: Request<DfsOwnerAcknowledgeLockWaitRequest>,
    ) -> Result<Response<DfsOwnerAcknowledgeLockWaitReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        let waiter = request
            .get_ref()
            .waiter
            .as_ref()
            .ok_or_else(|| Status::permission_denied("DFS lock waiter is required"))?;
        validate_dfs_owner_lock_waiter(waiter)?;
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.acknowledge_lock_wait(&peer, request)
        })
        .await
    }

    #[cfg(feature = "dfs")]
    async fn dfs_owner_release_locks(
        &self,
        request: Request<DfsOwnerReleaseLocksRequest>,
    ) -> Result<Response<DfsOwnerReleaseLocksReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        validate_dfs_owner_lock_owner(request.get_ref().owner.as_ref())?;
        if request.get_ref().release_kind == 0 {
            return Err(Status::invalid_argument(
                "DFS lock release kind is required",
            ));
        }
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.release_locks(&peer, request)
        })
        .await
    }

    #[cfg(feature = "dfs")]
    async fn dfs_owner_release_lock_session(
        &self,
        request: Request<DfsOwnerReleaseLockSessionRequest>,
    ) -> Result<Response<DfsOwnerReleaseLockSessionReply>, Status> {
        validate_dfs_owner_lock_authority(request.get_ref().authority.as_ref())?;
        if request.get_ref().ingress_session_id.is_empty() {
            return Err(Status::permission_denied(
                "DFS lock release session is incomplete",
            ));
        }
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.release_lock_session(&peer, request)
        })
        .await
    }

    #[cfg(feature = "dfs")]
    async fn dfs_owner_release(
        &self,
        request: Request<DfsOwnerReleaseRequest>,
    ) -> Result<Response<DfsOwnerReleaseReply>, Status> {
        validate_dfs_owner_release_handle(request.get_ref().handle.as_ref())?;
        self.dispatch_dfs_owner(request, |handler, peer, request| {
            handler.release(&peer, request)
        })
        .await
    }

    #[cfg(not(feature = "dfs"))]
    async fn dfs_owner_open(
        &self,
        _request: Request<DfsOwnerOpenRequest>,
    ) -> Result<Response<DfsOwnerOpenReply>, Status> {
        Err(Status::unimplemented("DFS owner lifecycle is not enabled"))
    }

    #[cfg(not(feature = "dfs"))]
    async fn dfs_owner_get_attr(
        &self,
        _request: Request<DfsOwnerGetAttrRequest>,
    ) -> Result<Response<DfsOwnerGetAttrReply>, Status> {
        Err(Status::unimplemented("DFS owner lifecycle is not enabled"))
    }

    #[cfg(not(feature = "dfs"))]
    async fn dfs_owner_mknod(
        &self,
        _request: Request<DfsOwnerMknodRequest>,
    ) -> Result<Response<DfsOwnerMknodReply>, Status> {
        Err(Status::unimplemented("DFS owner lifecycle is not enabled"))
    }

    #[cfg(not(feature = "dfs"))]
    async fn dfs_owner_get_lock(
        &self,
        _request: Request<DfsOwnerGetLockRequest>,
    ) -> Result<Response<DfsOwnerGetLockReply>, Status> {
        Err(Status::unimplemented("DFS owner lifecycle is not enabled"))
    }

    #[cfg(not(feature = "dfs"))]
    async fn dfs_owner_set_lock(
        &self,
        _request: Request<DfsOwnerSetLockRequest>,
    ) -> Result<Response<DfsOwnerSetLockReply>, Status> {
        Err(Status::unimplemented("DFS owner lifecycle is not enabled"))
    }

    #[cfg(not(feature = "dfs"))]
    async fn dfs_owner_cancel_lock_wait(
        &self,
        _request: Request<DfsOwnerCancelLockWaitRequest>,
    ) -> Result<Response<DfsOwnerCancelLockWaitReply>, Status> {
        Err(Status::unimplemented("DFS owner lifecycle is not enabled"))
    }

    #[cfg(not(feature = "dfs"))]
    async fn dfs_owner_acknowledge_lock_wait(
        &self,
        _request: Request<DfsOwnerAcknowledgeLockWaitRequest>,
    ) -> Result<Response<DfsOwnerAcknowledgeLockWaitReply>, Status> {
        Err(Status::unimplemented("DFS owner lifecycle is not enabled"))
    }

    #[cfg(not(feature = "dfs"))]
    async fn dfs_owner_release_locks(
        &self,
        _request: Request<DfsOwnerReleaseLocksRequest>,
    ) -> Result<Response<DfsOwnerReleaseLocksReply>, Status> {
        Err(Status::unimplemented("DFS owner lifecycle is not enabled"))
    }

    #[cfg(not(feature = "dfs"))]
    async fn dfs_owner_release_lock_session(
        &self,
        _request: Request<DfsOwnerReleaseLockSessionRequest>,
    ) -> Result<Response<DfsOwnerReleaseLockSessionReply>, Status> {
        Err(Status::unimplemented("DFS owner lifecycle is not enabled"))
    }

    #[cfg(not(feature = "dfs"))]
    async fn dfs_owner_release(
        &self,
        _request: Request<DfsOwnerReleaseRequest>,
    ) -> Result<Response<DfsOwnerReleaseReply>, Status> {
        Err(Status::unimplemented("DFS owner lifecycle is not enabled"))
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
    let (endpoint, info) = tokio::task::spawn_blocking(move || {
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

#[cfg(feature = "dfs")]
fn validate_dfs_owner_lock_authority(
    authority: Option<&afs_protocol::node_control::DfsOwnerLockAuthority>,
) -> Result<(), Status> {
    let authority =
        authority.ok_or_else(|| Status::permission_denied("DFS lock authority is required"))?;
    if authority.namespace_id.is_empty()
        || authority.inode_id.is_empty()
        || authority.owner_node_id.is_empty()
        || authority.owner_session_id.is_empty()
        || authority.lease_epoch == 0
        || authority.lease_expires_at_unix_ms == 0
        || authority.caller_node_id.is_empty()
        || authority.caller_session_id.is_empty()
    {
        return Err(Status::permission_denied(
            "DFS lock authority identity is incomplete",
        ));
    }
    Ok(())
}

#[cfg(feature = "dfs")]
fn validate_dfs_owner_lock_owner(
    owner: Option<&afs_protocol::node_control::DfsOwnerLockOwner>,
) -> Result<(), Status> {
    let owner = owner.ok_or_else(|| Status::permission_denied("DFS lock owner is required"))?;
    if owner.ingress_session_id.is_empty() {
        return Err(Status::permission_denied(
            "DFS lock owner session is incomplete",
        ));
    }
    Ok(())
}

#[cfg(feature = "dfs")]
fn validate_dfs_owner_lock_waiter(
    waiter: &afs_protocol::node_control::DfsOwnerLockWaiter,
) -> Result<(), Status> {
    if waiter.ingress_session_id.is_empty() {
        return Err(Status::permission_denied(
            "DFS lock waiter session is incomplete",
        ));
    }
    Ok(())
}

#[cfg(feature = "dfs")]
fn validate_dfs_owner_lock_request(
    request: Option<&afs_protocol::node_control::DfsOwnerLockRequest>,
) -> Result<(), Status> {
    let request =
        request.ok_or_else(|| Status::permission_denied("DFS lock request is required"))?;
    if request.kind == 0 || request.lock_type == 0 {
        return Err(Status::invalid_argument("DFS lock kind/type is required"));
    }
    validate_dfs_owner_lock_owner(request.owner.as_ref())?;
    let range = request
        .range
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("DFS lock range is required"))?;
    if range.start > range.end {
        return Err(Status::invalid_argument("DFS lock range is invalid"));
    }
    Ok(())
}

#[cfg(feature = "dfs")]
fn validate_dfs_owner_mknod_request(request: &DfsOwnerMknodRequest) -> Result<(), Status> {
    if request.namespace_id.is_empty()
        || request.parent_inode_id.is_empty()
        || request.owner_node_id.is_empty()
        || request.owner_session_id.is_empty()
        || request.lease_epoch == 0
        || request.caller_session_id.is_empty()
        || request.name.is_empty()
    {
        return Err(Status::permission_denied(
            "DFS owner mknod identity is incomplete",
        ));
    }
    let special = request
        .special_node
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("DFS owner mknod missing special_node"))?;
    match afs_protocol::node_control::DfsOwnerSpecialNodeKind::try_from(special.kind) {
        Ok(afs_protocol::node_control::DfsOwnerSpecialNodeKind::Fifo)
        | Ok(afs_protocol::node_control::DfsOwnerSpecialNodeKind::Socket)
            if special.rdev == 0 =>
        {
            Ok(())
        }
        Ok(afs_protocol::node_control::DfsOwnerSpecialNodeKind::BlockDevice)
        | Ok(afs_protocol::node_control::DfsOwnerSpecialNodeKind::CharDevice) => Ok(()),
        _ => Err(Status::invalid_argument(
            "DFS owner mknod has invalid special kind/rdev",
        )),
    }
}

#[cfg(feature = "dfs")]
fn validate_dfs_owner_control_handle(
    handle: Option<&afs_protocol::node_control::DfsOwnerHandle>,
) -> Result<(), Status> {
    let handle = handle.ok_or_else(|| Status::permission_denied("DFS owner handle is required"))?;
    if handle.namespace_id.is_empty()
        || handle.inode_id.is_empty()
        || handle.owner_node_id.is_empty()
        || handle.owner_session_id.is_empty()
        || handle.lease_epoch == 0
        || handle.caller_node_id.is_empty()
        || handle.caller_session_id.is_empty()
        || handle.open_seq == 0
        || handle.opaque_handle.is_empty()
    {
        return Err(Status::permission_denied(
            "DFS owner handle identity is incomplete",
        ));
    }
    Ok(())
}

#[cfg(feature = "dfs")]
fn validate_dfs_owner_release_handle(
    handle: Option<&afs_protocol::node_control::DfsOwnerHandle>,
) -> Result<(), Status> {
    let handle = handle.ok_or_else(|| Status::permission_denied("DFS owner handle is required"))?;
    if handle.namespace_id.is_empty()
        || handle.inode_id.is_empty()
        || handle.owner_node_id.is_empty()
        || handle.owner_session_id.is_empty()
        || handle.lease_epoch == 0
        || handle.caller_node_id.is_empty()
        || handle.caller_session_id.is_empty()
        || handle.open_seq == 0
    {
        return Err(Status::permission_denied(
            "DFS owner release identity is incomplete",
        ));
    }
    Ok(())
}

#[cfg(feature = "ownerfs")]
pub(super) fn owner_lock_to_wire(
    request: &crate::node::vfs::locks::LockRequest,
) -> afs_protocol::node_control::OwnerFileLock {
    use crate::node::vfs::types::{FileLockKind, FileLockType};
    afs_protocol::node_control::OwnerFileLock {
        kind: match request.kind {
            FileLockKind::Posix => 1,
            FileLockKind::Flock => 2,
        },
        ingress_session_id: request.owner.ingress_session_id.clone(),
        kernel_owner: request.owner.kernel_owner,
        pid: request.pid,
        start: request.range.start,
        end: request.range.end,
        lock_type: match request.lock_type {
            FileLockType::Read => 1,
            FileLockType::Write => 2,
            FileLockType::Unlock => 3,
        },
    }
}

#[cfg(feature = "ownerfs")]
pub(super) fn owner_lock_from_wire(
    wire: Option<afs_protocol::node_control::OwnerFileLock>,
) -> afs_error::Result<crate::node::vfs::locks::LockRequest> {
    use crate::node::vfs::{
        locks::LockRequest,
        types::{FileLockKind, FileLockOwner, FileLockRange, FileLockType},
    };
    let wire = wire.ok_or_else(owner_lock_wire_invalid)?;
    if wire.ingress_session_id.is_empty()
        || wire.ingress_session_id.len() > 1024
        || wire.start > wire.end
    {
        return Err(owner_lock_wire_invalid());
    }
    Ok(LockRequest {
        kind: match wire.kind {
            1 => FileLockKind::Posix,
            2 => FileLockKind::Flock,
            _ => return Err(owner_lock_wire_invalid()),
        },
        owner: FileLockOwner {
            ingress_session_id: wire.ingress_session_id,
            kernel_owner: wire.kernel_owner,
        },
        pid: wire.pid,
        range: FileLockRange {
            start: wire.start,
            end: wire.end,
        },
        lock_type: match wire.lock_type {
            1 => FileLockType::Read,
            2 => FileLockType::Write,
            3 => FileLockType::Unlock,
            _ => return Err(owner_lock_wire_invalid()),
        },
    })
}

#[cfg(feature = "ownerfs")]
fn owner_lock_handle_from_wire(
    handle: Option<afs_protocol::node_control::OwnerLockHandle>,
) -> afs_error::Result<(
    crate::node::vfs::ownerfs::root::PresentedRootAccess,
    crate::node::vfs::ownerfs::files::RemoteFile,
)> {
    let handle = handle.ok_or_else(owner_lock_wire_invalid)?;
    let access = super::data::presented_access(handle.access)?;
    let bytes = handle.file.ok_or_else(owner_lock_wire_invalid)?.opaque;
    let identity = handle.identity.ok_or_else(owner_lock_wire_invalid)?.opaque;
    if bytes.len() != 8 || identity.is_empty() || identity.len() > 256 {
        return Err(owner_lock_wire_invalid());
    }
    let mut file = super::data::remote_file_for_handle(&access, bytes);
    file.identity = crate::node::vfs::ownerfs::files::FileIdentity(identity);
    Ok((access, file))
}

#[cfg(feature = "ownerfs")]
fn owner_lock_waiter_from_wire(
    waiter: afs_protocol::node_control::OwnerLockWaiter,
) -> afs_error::Result<crate::node::vfs::locks::LockWaiterId> {
    if waiter.ingress_session_id.is_empty() || waiter.ingress_session_id.len() > 256 {
        return Err(owner_lock_wire_invalid());
    }
    Ok(crate::node::vfs::locks::LockWaiterId {
        ingress_session_id: waiter.ingress_session_id,
        request_id: waiter.request_id,
    })
}
#[cfg(feature = "ownerfs")]
fn owner_lock_wire_invalid() -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::CLIENT_ARGUMENT_INVALID,
        "Owner advisory lock authority/request is incomplete",
    )
}

#[cfg(feature = "ownerfs")]
struct OwnerLockCancelOnDrop {
    handler: crate::node::vfs::ownerfs::OwnerFsPeerExecutor,
    peer: String,
    access: crate::node::vfs::ownerfs::root::PresentedRootAccess,
    waiter: Option<crate::node::vfs::locks::LockWaiterId>,
}
#[cfg(feature = "ownerfs")]
impl Drop for OwnerLockCancelOnDrop {
    fn drop(&mut self) {
        if let Some(waiter) = self.waiter.take() {
            let handler = self.handler.clone();
            let peer = self.peer.clone();
            let access = self.access.clone();
            // Cancel the exact pending request when the HTTP/2 response future
            // is dropped. A raced Granted outcome is retained; no broad unlock.
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn_blocking(move || {
                    let _ = handler.cancel_lock_wait(&peer, &access, waiter);
                });
            }
        }
    }
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
