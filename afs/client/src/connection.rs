//! 本机 SDK 连接：只连同机 afs-node 的 UDS。
//!
//! 控制面与数据面是两条独立通道：
//!
//! - 控制面：gRPC over UDS，只传身份与布局（open/close、iov_create/iov_destroy、
//!   register_file、ring_create/ring_destroy、sync barrier），从不携带文件内容。
//! - 数据面：usrbio 风格的「持久 iov + 共享环」。SDK 先把正文写进一块常驻共享内存，再通过
//!   共享提交队列（SQ）提交 `(reg_id, offset, iov_offset, len)`；node 的环 worker 在同一块
//!   映射上执行后端读写，并把结果写进共享完成队列（CQ）。一次 read/write 既没有 RPC，
//!   也没有逐次操作的文件描述符传递。
//!
//! 生命周期与所有权：
//!
//! - [`DfsIov`] 同时持有 mmap 视图（`MappedRegion`）、iov 视图（`SharedIov`）和区块分配器。
//!   `SharedIov` 只存裸指针，不与 `MappedRegion` 互相借用，因此二者放在同一个结构体里
//!   不需要 `Pin` 或自引用技巧。
//! - [`DfsRing`] 只有 SQ 生产者与 CQ 消费者，[`DfsIov`] 的分配器只经可变借用访问，所以
//!   `write_from`/`read_into` 要求调用方同时传 `&mut DfsIov`：并发约束由类型系统表达。
//! - `destroy()` 才是显式控制面调用；`Drop` 只做本地释放，不发 RPC。忘记调用 `destroy()`
//!   会在 node 侧留下 iov / 环注册项，直到连接关闭。
//!
//! diagnostics 穿刺（[`DiagnosticLocalClient`]）仍保留旧的「每次操作一个 memfd + 一次性
//! fd broker」路径：它的协议只有「一次操作一个 grant」这一种表达，不在本次 usrbio 数据面
//! 改造范围内。

// 共享内存视图只能由裸指针构造：`SharedIov::from_raw_parts` 与 `SharedRing::from_raw_parts`
// 是本模块唯一的 unsafe 边界，每一处都带 SAFETY 说明，其余逻辑保持 safe。
#![allow(unsafe_code)]

use std::{
    fmt,
    os::fd::OwnedFd,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use afs_protocol::local_api::{
    DfsCloseRequest, DfsFileHandle as WireDfsFileHandle, DfsIovCreateRequest, DfsIovDestroyRequest,
    DfsOpenAccess, DfsOpenRequest, DfsRegisterFileRequest, DfsRingCreateRequest,
    DfsRingDestroyRequest, DfsSyncBarrier, DfsSyncRequest, DfsUnregisterFileRequest,
    LocalReadRequest, LocalShmGrant, LocalWriteRequest, dfs_local_data_client::DfsLocalDataClient,
    local_data_client::LocalDataClient,
};
use afs_tracing::{Instrument, TracedChannel, request_with_current_context, traced_channel};
use afs_transport::{
    eventfd::{EventFd, EventFdError},
    grpc::GrpcConfig,
    iov::{IovAllocator, IovError, IovLayout, IovSlice, SharedIov},
    mapped::MappedRegion,
    ring::{Completion, OP_READ, OP_WRITE, RingError, RingLayout, SharedRing, Submission},
    shm::{BrokerToken, FdBrokerServer, FdGrant, FdRequest, SharedRegion, ShmError},
};
use hyper_util::rt::TokioIo;
use tokio::{sync::Semaphore, task::JoinHandle};
use tonic::transport::Endpoint;
use tower::service_fn;

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

/// 默认 namespace：本机 node 只服务一个挂载点，SDK 因此把它固定在连接上。
const DEFAULT_NAMESPACE: &str = "default";

#[derive(Clone)]
pub struct DfsLocalClient {
    inner: DfsLocalDataClient<TracedChannel>,
    request_timeout: Duration,
    /// 打开文件时使用的 namespace；默认 [`DEFAULT_NAMESPACE`]，可用 `with_namespace` 覆盖。
    namespace_id: String,
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
    /// 打开那一刻后端报告的版本。
    ///
    /// **空串表示后端没有暴露版本身份**（当前 `Backend` trait 没有版本访问器），此时本轮不做
    /// 固定版本校验；后端将来提供版本后这里自然会有值，调用方无需改动。
    pub file_version_id: String,
    pub length: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsSyncResult {
    pub file_version_id: String,
    pub durable_write_seq: u64,
}

/// 已注册到 node 的句柄：环内读写只携带 `reg_id`，不再逐次传 handle。
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DfsRegistration {
    pub reg_id: u32,
    /// 注册时 node 绑定的版本。
    ///
    /// **空串表示后端没有暴露版本身份**（当前 `Backend` trait 没有版本访问器），此时不做
    /// 固定版本校验；与 [`DfsOpenFile::file_version_id`] 都非空且不一致才说明注册期间版本
    /// 发生了变更。后端将来提供版本后这里自然会有值，调用方无需改动。
    pub file_version_id: String,
}

// diagnostics 穿刺的每次操作都会临时占用一个 memfd 和一个 fd broker。
// 这里的 slot 上限是进程内背压，避免调用方用取消/并发把 fd、socket、线程资源打满。
// permit 使用 OwnedSemaphorePermit 交给 worker 持有：即使外层 future 被取消，worker
// 仍负责等待 broker 结束后再释放资源。
const SDK_SHM_SLOTS: usize = 64;
static BROKER_ADMISSION: std::sync::LazyLock<Arc<Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(Semaphore::new(SDK_SHM_SLOTS)));

/// diagnostics 穿刺路径的并发上限；usrbio 数据面（iov/环）是常驻结构，不占这个名额。
pub fn max_parallel_shm_operations() -> usize {
    SDK_SHM_SLOTS
}

/// 环等待完成项的默认上界。`timeout_ms == 0` 表示 node 侧「有多少处理多少」，SDK 仍需要
/// 一个上界，避免 node 异常时调用方无限等待。
const DEFAULT_RING_TIMEOUT: Duration = Duration::from_secs(5);

/// 交给 node 的 fd 授权有效期：足够覆盖一次本机 create RPC，同时能清理未领取的悬挂授权。
const GRANT_TTL: Duration = Duration::from_secs(5);

static NEXT_SHM_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub enum LocalClientError {
    InvalidArgument {
        field: &'static str,
        reason: &'static str,
    },
    Transport(tonic::transport::Error),
    Status(afs_error::Error),
    Shm(afs_transport::shm::ShmError),
    Iov(afs_transport::iov::IovError),
    Ring(afs_transport::ring::RingError),
    BrokerThread,
    Worker,
    Timeout,
    /// 共享环等待完成项超时；提交项仍留在环里，node 仍可能完成它。
    RingTimeout,
    /// 门铃（eventfd）创建或接管失败。
    Doorbell(EventFdError),
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
            Self::Iov(error) => write!(formatter, "shared iov error: {error}"),
            Self::Ring(error) => write!(formatter, "shared ring error: {error}"),
            Self::BrokerThread => formatter.write_str("broker thread failed"),
            Self::Worker => formatter.write_str("local SDK worker task failed"),
            Self::Timeout => formatter.write_str("local SDK RPC timed out"),
            Self::RingTimeout => formatter.write_str("shared ring wait timed out"),
            Self::Doorbell(error) => write!(formatter, "shared ring doorbell error: {error}"),
            Self::Closed => formatter.write_str("local SDK client is closed"),
        }
    }
}

impl LocalClientError {
    /// 返回统一错误。保留来源枚举便于本机诊断，但业务判断应使用 code/kind。
    ///
    /// 数据面错误的编码约定：
    ///
    /// - 越界、跨区块、参数非法、区块编号非法 → `CLIENT_ARGUMENT_INVALID`（本机调用契约问题）；
    /// - iov 区块用尽、SQ/CQ 满 → `CLIENT_SHM_UNAVAILABLE`（背压，调用方应重试或等待）；
    /// - iov/环布局不一致、未初始化、基址未对齐 → `CLIENT_PROTOCOL_VIOLATION`（两端映射的
    ///   不是同一版本的结构）；
    /// - `RingTimeout` → `CLIENT_DEADLINE_EXCEEDED`，其结果不确定，不能当作「操作未发生」。
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
            Self::Timeout | Self::RingTimeout => CLIENT_DEADLINE_EXCEEDED,
            Self::Closed => CLIENT_CLOSED,
            Self::BrokerThread | Self::Worker => CLIENT_WORKER_FAILED,
            Self::Shm(afs_transport::shm::ShmError::InvalidArgument { .. }) => {
                CLIENT_ARGUMENT_INVALID
            }
            Self::Shm(afs_transport::shm::ShmError::InvalidToken) => CLIENT_PERMISSION_DENIED,
            Self::Shm(afs_transport::shm::ShmError::Protocol(_)) => CLIENT_PROTOCOL_VIOLATION,
            Self::Shm(_) => CLIENT_SHM_UNAVAILABLE,
            Self::Iov(
                afs_transport::iov::IovError::InvalidArgument { .. }
                | afs_transport::iov::IovError::OutOfRange
                | afs_transport::iov::IovError::SpansBlocks
                | afs_transport::iov::IovError::InvalidBlock(_),
            ) => CLIENT_ARGUMENT_INVALID,
            Self::Iov(afs_transport::iov::IovError::Exhausted) => CLIENT_SHM_UNAVAILABLE,
            Self::Iov(_) => CLIENT_PROTOCOL_VIOLATION,
            Self::Ring(
                afs_transport::ring::RingError::SubmissionFull
                | afs_transport::ring::RingError::CompletionFull,
            ) => CLIENT_SHM_UNAVAILABLE,
            Self::Ring(afs_transport::ring::RingError::InvalidArgument { .. }) => {
                CLIENT_ARGUMENT_INVALID
            }
            Self::Ring(_) => CLIENT_PROTOCOL_VIOLATION,
            Self::Doorbell(_) => CLIENT_SHM_UNAVAILABLE,
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

impl From<IovError> for LocalClientError {
    fn from(value: IovError) -> Self {
        Self::Iov(value)
    }
}

impl From<EventFdError> for LocalClientError {
    fn from(error: EventFdError) -> Self {
        Self::Doorbell(error)
    }
}

impl From<RingError> for LocalClientError {
    fn from(value: RingError) -> Self {
        Self::Ring(value)
    }
}

/// SDK 侧的持久 iov：一块常驻共享内存加上区块分配器。
///
/// 创建时 node 已经通过 fd broker 拿到同一块内存的映射，之后所有正文都在这里来回搬运，
/// 控制面不再参与。**销毁必须显式调用 [`DfsIov::destroy`]**：`Drop` 只撤销本地映射，
/// 不发任何 RPC，因此忘记销毁会在 node 侧留下这块 iov 的注册项，直到连接关闭。
pub struct DfsIov {
    client: DfsLocalDataClient<TracedChannel>,
    request_timeout: Duration,
    /// 映射本身。`view` 只保存它的基址裸指针，二者都活到 `DfsIov` 结束，不需要 `Pin`。
    region: MappedRegion,
    view: SharedIov,
    allocator: IovAllocator,
    iov_id: u64,
}

impl DfsIov {
    /// node 分配的 iov 编号。
    pub fn iov_id(&self) -> u64 {
        self.iov_id
    }

    /// iov 布局（区块大小与区块数）。
    pub fn layout(&self) -> IovLayout {
        self.view.layout()
    }

    /// 本地映射的字节数（头部 + 全部区块）。
    pub fn total_bytes(&self) -> usize {
        self.region.len()
    }

    /// 当前空闲区块数。
    pub fn free_blocks(&self) -> u32 {
        self.allocator.free_blocks()
    }

    /// 在途（已预约未释放）区块数。
    pub fn in_flight(&self) -> u32 {
        self.allocator.in_flight()
    }

    /// 销毁 node 侧的 iov 并释放本地映射。
    ///
    /// 调用方必须保证没有在途操作：已经提交给环的请求仍可能被 node 使用这块内存。
    pub async fn destroy(self) -> Result<(), LocalClientError> {
        let request = request_with_current_context(DfsIovDestroyRequest {
            iov_id: self.iov_id,
        });
        let mut client = self.client.clone();
        tokio::time::timeout(self.request_timeout, client.iov_destroy(request))
            .await
            .map_err(|_| LocalClientError::Timeout)??;
        Ok(())
    }
}

impl fmt::Debug for DfsIov {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DfsIov")
            .field("iov_id", &self.iov_id)
            .field("layout", &self.view.layout())
            .field("free_blocks", &self.allocator.free_blocks())
            .field("in_flight", &self.allocator.in_flight())
            .finish()
    }
}

/// SDK 侧的共享环：一条 SQ + 一条 CQ，绑定一块已创建好的 [`DfsIov`]。
///
/// 与 [`DfsIov`] 一样，销毁必须显式调用 [`DfsRing::destroy`]。`Drop` 只撤销本地映射，
/// 不发 RPC，所以忘记销毁会在 node 侧留下环 worker，直到连接关闭。
pub struct DfsRing {
    client: DfsLocalDataClient<TracedChannel>,
    request_timeout: Duration,
    /// 环映射。`ring` 只保存它的基址裸指针，二者都活到 `DfsRing` 结束。
    region: MappedRegion,
    ring: SharedRing,
    ring_id: u64,
    iov_id: u64,
    /// 等待完成项的上界（`ring_create` 的 `timeout_ms`）。
    wait_timeout: Duration,
    /// 单发接口使用的 userdata 序号，从 1 开始，避免与 node 默认填充的 0 混淆。
    next_userdata: u64,
    /// 跨进程门铃：提交后写它唤醒 node 的环 worker。node 持有同一个 eventfd 的副本。
    doorbell: EventFd,
    /// 完成通知：node 每投递一批完成项写一次。这是客户端唯一的阻塞等待通道。
    completion: EventFd,
    /// 同一个 eventfd 的另一个副本，注册到 reactor 用来 await 可读性。
    ///
    /// 客户端 crate 禁止 `unsafe`，所以不能用裸 `read` 清计数：等就绪走 `AsyncFd`，
    /// 清计数走 [`EventFd::drain`]，两者作用于同一个 eventfd 对象（dup 共享文件描述）。
    completion_ready: tokio::io::unix::AsyncFd<std::os::fd::OwnedFd>,
}

impl fmt::Debug for DfsRing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DfsRing")
            .field("ring_id", &self.ring_id)
            .field("iov_id", &self.iov_id)
            .field("entries", &self.ring.layout().entries())
            .field("pending", &self.pending())
            .finish()
    }
}

impl DfsRing {
    /// node 分配的环编号。
    pub fn ring_id(&self) -> u64 {
        self.ring_id
    }

    /// 环绑定的 iov 编号。
    pub fn iov_id(&self) -> u64 {
        self.iov_id
    }

    /// 环的项数（SQ 与 CQ 各这么多项）。
    pub fn entries(&self) -> u32 {
        self.ring.layout().entries()
    }

    /// 本地映射的字节数（头部 + SQ + CQ）。
    pub fn total_bytes(&self) -> usize {
        self.region.len()
    }

    /// 已经由 node 完成、但还没被 SDK 取走的完成项数量。
    ///
    /// 等待超时后可以用它判断「node 是否已经把那一批做完了」。[`DfsRing::wait_for`] 超时
    /// 只是本地放弃等待：提交项仍在环里，node 仍会完成它，因此对应区块不能立即复用。
    pub fn pending(&self) -> u32 {
        self.ring.completion_consumer().ready()
    }

    /// 提交一项，环满时在等待上界内退避重试。
    ///
    /// 提交成功后敲一次门铃（共享内存里的计数），node 侧环 worker 用它做廉价的变化判断；
    /// 真正的唤醒由 node 自己的轮询/事件负责。
    pub async fn submit(&mut self, submission: Submission) -> Result<(), LocalClientError> {
        let producer = self.ring.submission_producer();
        let deadline = Instant::now() + self.wait_timeout;
        loop {
            match producer.push(&submission) {
                Ok(_) => {
                    // 唯一的唤醒通道：node 的环 worker 阻塞在这个 eventfd 上。
                    self.doorbell.ring();
                    return Ok(());
                }
                Err(RingError::SubmissionFull) => {
                    // SQ 满说明 node 还没取走；等它的完成通知（取走会释放槽位并通知）。
                    self.wait_completion(deadline).await?;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// 一次推入多项，只在最后敲**一次**门铃。
    ///
    /// 返回真正入队的条数：SQ 剩余容量不足时会少推，调用方应对剩余部分再调一次。
    /// 这是批量提交与 [`DfsRing::submit`] 的唯一区别——后者每次提交都要敲一次门铃。
    pub async fn submit_batch(
        &mut self,
        submissions: &[Submission],
    ) -> Result<usize, LocalClientError> {
        if submissions.is_empty() {
            return Ok(0);
        }
        let producer = self.ring.submission_producer();
        let deadline = Instant::now() + self.wait_timeout;
        loop {
            let mut pushed = 0;
            for submission in submissions {
                match producer.push(submission) {
                    Ok(_) => pushed += 1,
                    Err(RingError::SubmissionFull) => break,
                    Err(error) => return Err(error.into()),
                }
            }
            if pushed > 0 {
                // 一批只敲一次门铃，这是它与 submit 的唯一区别。
                self.doorbell.ring();
                return Ok(pushed);
            }
            self.wait_completion(deadline).await?;
        }
    }

    /// 把一批提交全部入队（必要时分段），每段敲一次门铃。
    ///
    /// 与 [`DfsRing::submit_batch`] 相同，**不等待完成**；完成项由 [`DfsRing::wait_for`] 取。
    pub async fn submit_all(&mut self, submissions: &[Submission]) -> Result<(), LocalClientError> {
        let mut remaining = submissions;
        while !remaining.is_empty() {
            let pushed = self.submit_batch(remaining).await?;
            if pushed == 0 {
                return Err(LocalClientError::RingTimeout);
            }
            remaining = &remaining[pushed..];
        }
        Ok(())
    }

    /// 等到至少 `count` 个完成项就绪后一次性取走它们。
    ///
    /// 超时返回 [`LocalClientError::RingTimeout`]，但**不撤回提交项**：它们仍在环里，node 仍会
    /// 完成；超时只表示「结果未知」。此时调用方不得复用对应 iov 区块，可以继续用
    /// [`DfsRing::pending`] 观察后续完成项，或用 `sync` 判断文件状态。
    pub async fn wait_for(&mut self, count: usize) -> Result<Vec<Completion>, LocalClientError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let consumer = self.ring.completion_consumer();
        let deadline = Instant::now() + self.wait_timeout;
        loop {
            if consumer.ready() as usize >= count {
                // 单消费者：ready 之后只会有更多完成项，因此这里一定能取满 count 个。
                return Ok((0..count).filter_map(|_| consumer.pop()).collect());
            }
            self.wait_completion(deadline).await?;
        }
    }

    /// 阻塞到 node 投递了完成项，或到达截止时间。
    ///
    /// 这是取代轮询的关键：node 每投递一批完成项就写一次完成 eventfd，客户端用 `AsyncFd`
    /// 等它可读，不占 CPU、也不受 tokio 毫秒级定时器粒度影响。
    async fn wait_completion(&self, deadline: Instant) -> Result<(), LocalClientError> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(LocalClientError::RingTimeout);
        }
        match tokio::time::timeout(remaining, self.completion_ready.readable()).await {
            // 超时只是这一轮没等到，交给调用方按自己的循环语义处理。
            Err(_) => Err(LocalClientError::RingTimeout),
            Ok(Err(error)) => Err(LocalClientError::Doorbell(EventFdError::Create(error))),
            Ok(Ok(mut guard)) => {
                // 先把计数读空，再清 reactor 缓存的可读位；两步缺一不可，否则会立刻重入。
                self.completion.drain();
                guard.clear_ready();
                Ok(())
            }
        }
    }

    /// 单发写入：把 `data` 放进 iov 的一个区块并提交一项写请求，等到它的完成项。
    ///
    /// 一次操作不得跨区块，因此 `data.len()` 不能超过 `iov` 的 `block_size`；超过时返回
    /// [`IovError::SpansBlocks`] 对应的错误，调用方应自行拆分。返回实际写入字节数。
    ///
    /// 这是单发便利接口：环上同时只应有这一项在途，否则完成项的 userdata 可能对不上。
    pub async fn write_from(
        &mut self,
        iov: &mut DfsIov,
        reg: &DfsRegistration,
        offset: u64,
        data: &[u8],
    ) -> Result<usize, LocalClientError> {
        self.require_same_iov(iov)?;
        if data.is_empty() {
            return Ok(0);
        }
        let mut guard = ReservedBlock::reserve(&mut iov.allocator, data.len())?;
        let slice = guard.slice();
        iov.view.write_block(&slice, data)?;
        let submission = Submission {
            op: OP_WRITE,
            flags: 0,
            reg_id: reg.reg_id,
            len: slice.len,
            offset,
            iov_offset: slice.iov_offset,
            userdata: self.next_userdata(),
        };
        let completion = self.submit_and_wait(submission, &mut guard).await?;
        let written = completion_bytes(&completion)?;
        if written > slice.len as usize {
            return Err(protocol_violation(
                "DFS write completion exceeded the submitted length",
            ));
        }
        guard.release()?;
        Ok(written)
    }

    /// 单发读取：提交一项读请求，把完成项声明的字节数从 iov 复制到 `out`。
    ///
    /// 与 [`DfsRing::write_from`] 对称：`out.len()` 不能超过 `block_size`；返回实际读到的
    /// 字节数（可能小于 `out.len()`，表示读到了文件末尾）。
    pub async fn read_into(
        &mut self,
        iov: &mut DfsIov,
        reg: &DfsRegistration,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize, LocalClientError> {
        self.require_same_iov(iov)?;
        if out.is_empty() {
            return Ok(0);
        }
        let mut guard = ReservedBlock::reserve(&mut iov.allocator, out.len())?;
        let slice = guard.slice();
        let submission = Submission {
            op: OP_READ,
            flags: 0,
            reg_id: reg.reg_id,
            len: slice.len,
            offset,
            iov_offset: slice.iov_offset,
            userdata: self.next_userdata(),
        };
        let completion = self.submit_and_wait(submission, &mut guard).await?;
        let read = completion_bytes(&completion)?;
        if read > slice.len as usize {
            return Err(protocol_violation(
                "DFS read completion exceeded the requested length",
            ));
        }
        let source = iov.view.slice(slice.iov_offset, read as u64)?;
        let destination = out
            .get_mut(..read)
            .ok_or_else(|| protocol_violation("DFS read did not fit the destination buffer"))?;
        destination.copy_from_slice(source);
        guard.release()?;
        Ok(read)
    }

    /// 批量写：一次门铃提交一批，等在途全部完成后返回每项实际写入的字节数。
    ///
    /// 与逐项调用 [`DfsRing::write_from`] 相比只有"门铃次数"的差别，但那是 OS 系统调用的差别。
    /// 数据仍要拷进 iov（应用缓冲区不在共享内存里），所以拷贝次数与单发相同。
    ///
    /// 一批的实际大小受 SQ 剩余容量与 iov 空闲区块**双重**限制；装不下时会自动分波，
    /// 每波一次门铃，因此调用方不需要自己算容量。
    pub async fn write_batch(
        &mut self,
        iov: &mut DfsIov,
        reg: &DfsRegistration,
        items: &[(u64, &[u8])],
    ) -> Result<Vec<usize>, LocalClientError> {
        self.require_same_iov(iov)?;
        let mut results = vec![0_usize; items.len()];
        let mut cursor = 0;
        while cursor < items.len() {
            let wave = self.wave_size(iov, items.len() - cursor)?;
            let lengths: Vec<usize> = items[cursor..cursor + wave]
                .iter()
                .map(|(_, data)| data.len())
                .collect();
            let mut batch = ReservedBatch::reserve(&mut iov.allocator, &lengths)?;
            for (index, slice) in batch.slices().iter().enumerate() {
                iov.view.write_block(slice, items[cursor + index].1)?;
            }
            let submissions = self.batch_submissions(
                OP_WRITE,
                reg,
                batch.slices(),
                &items[cursor..cursor + wave],
            );
            let completed = match self.dispatch_batch(&submissions, &mut batch).await {
                Ok(completed) => completed,
                Err(error) => return Err(error),
            };
            for (index, bytes) in completed {
                results[cursor + index] = bytes;
            }
            batch.release_all()?;
            cursor += wave;
        }
        Ok(results)
    }

    /// 批量读：一次门铃提交一批，把结果写进调用方缓冲区。
    ///
    /// 与 [`DfsRing::write_batch`] 对称；区块预留、归还与 forfeit 规则完全一致。
    pub async fn read_batch(
        &mut self,
        iov: &mut DfsIov,
        reg: &DfsRegistration,
        items: &mut [(u64, &mut [u8])],
    ) -> Result<Vec<usize>, LocalClientError> {
        self.require_same_iov(iov)?;
        let mut results = vec![0_usize; items.len()];
        let mut cursor = 0;
        while cursor < items.len() {
            let wave = self.wave_size(iov, items.len() - cursor)?;
            let lengths: Vec<usize> = items[cursor..cursor + wave]
                .iter()
                .map(|(_, out)| out.len())
                .collect();
            let mut batch = ReservedBatch::reserve(&mut iov.allocator, &lengths)?;
            let offsets: Vec<u64> = items[cursor..cursor + wave]
                .iter()
                .map(|(offset, _)| *offset)
                .collect();
            let submissions =
                self.batch_submissions_with_offsets(OP_READ, reg, batch.slices(), &offsets);
            let completed = match self.dispatch_batch(&submissions, &mut batch).await {
                Ok(completed) => completed,
                Err(error) => return Err(error),
            };
            for (index, bytes) in completed {
                let source = iov
                    .view
                    .slice(batch.slices()[index].iov_offset, bytes as u64)?;
                let destination = items[cursor + index].1.get_mut(..bytes).ok_or_else(|| {
                    protocol_violation("DFS read did not fit the destination buffer")
                })?;
                destination.copy_from_slice(source);
                results[cursor + index] = bytes;
            }
            batch.release_all()?;
            cursor += wave;
        }
        Ok(results)
    }

    /// 一波能装多少：同时受 SQ 容量与 iov 空闲区块限制。
    fn wave_size(&self, iov: &DfsIov, remaining: usize) -> Result<usize, LocalClientError> {
        let entries = usize::try_from(self.ring.layout().entries()).unwrap_or(usize::MAX);
        let free = usize::try_from(iov.allocator.free_blocks()).unwrap_or(0);
        let wave = remaining.min(entries).min(free);
        if wave == 0 {
            // 空闲区块为零说明上一波还没结算（或已被 forfeit）：这是调用顺序/iov 状态问题，
            // 不是可以重试的瞬时背压，不能伪装成 RingTimeout。
            return Err(LocalClientError::InvalidArgument {
                field: "iov",
                reason: "no free blocks; settle the previous batch before submitting more",
            });
        }
        Ok(wave)
    }

    fn batch_submissions(
        &mut self,
        op: u32,
        reg: &DfsRegistration,
        slices: &[IovSlice],
        items: &[(u64, &[u8])],
    ) -> Vec<Submission> {
        let offsets: Vec<u64> = items.iter().map(|(offset, _)| *offset).collect();
        self.batch_submissions_with_offsets(op, reg, slices, &offsets)
    }

    fn batch_submissions_with_offsets(
        &mut self,
        op: u32,
        reg: &DfsRegistration,
        slices: &[IovSlice],
        offsets: &[u64],
    ) -> Vec<Submission> {
        let userdata: Vec<u64> = slices.iter().map(|_| self.next_userdata()).collect();
        slices
            .iter()
            .zip(offsets)
            .zip(userdata.iter().copied())
            .map(|((slice, offset), userdata)| Submission {
                op,
                flags: 0,
                reg_id: reg.reg_id,
                len: slice.len,
                offset: *offset,
                iov_offset: slice.iov_offset,
                userdata,
            })
            .collect()
    }

    /// 提交一批并等它们全部完成；返回 `(波内序号, 字节数)`。
    ///
    /// 失败路径的区块归属：结果明确（node 报错）→ 归还；结果未知（超时/对不上）→ forfeit。
    async fn dispatch_batch(
        &mut self,
        submissions: &[Submission],
        batch: &mut ReservedBatch<'_>,
    ) -> Result<Vec<(usize, usize)>, LocalClientError> {
        let pushed = self.submit_batch(submissions).await?;
        if pushed != submissions.len() {
            // 防御性分支：wave_size 已把 SQ 容量算进去，正常不会发生。此时哪些提交真的进了环
            // 无法确定，因此不归还任何区块。
            batch.forfeit_all();
            return Err(protocol_violation(
                "batched submissions were not fully accepted by the ring",
            ));
        }
        let completions = match self.wait_for(submissions.len()).await {
            Ok(completions) => completions,
            Err(error) => {
                batch.forfeit_all();
                return Err(error);
            }
        };
        let mut completed = Vec::with_capacity(completions.len());
        let mut failure = None;
        for completion in &completions {
            let index = match submissions
                .iter()
                .position(|submission| submission.userdata == completion.userdata)
            {
                Some(index) => index,
                None => {
                    // 完成项对不上号：这批结果未知，node 仍可能对区块做后端读写。
                    // forfeit_all 之后 Drop 不再归还，区块保持占用，不会被下一批复用。
                    batch.forfeit_all();
                    return Err(protocol_violation(
                        "batch completion did not match any submission",
                    ));
                }
            };
            let bytes = match completion_bytes(completion) {
                Ok(bytes) => bytes,
                Err(error) => {
                    // 先记下失败继续扫完整批：只有确认每一项都对得上号，失败才是「明确
                    // 失败」，走 Drop 归还区块才是安全的。
                    failure.get_or_insert(error);
                    0
                }
            };
            completed.push((index, bytes));
        }
        if let Some(error) = failure {
            // 整批都对上了号：node 报错也是明确结果，区块按约定归还（Drop 路径）。
            return Err(error);
        }
        Ok(completed)
    }

    /// 销毁 node 侧的环并释放本地映射。
    ///
    /// 调用方必须保证没有在途操作；环销毁不会顺带销毁 iov，[`DfsIov::destroy`] 仍需单独调用。
    pub async fn destroy(self) -> Result<(), LocalClientError> {
        let request = request_with_current_context(DfsRingDestroyRequest {
            ring_id: self.ring_id,
        });
        let mut client = self.client.clone();
        tokio::time::timeout(self.request_timeout, client.ring_destroy(request))
            .await
            .map_err(|_| LocalClientError::Timeout)??;
        Ok(())
    }

    fn require_same_iov(&self, iov: &DfsIov) -> Result<(), LocalClientError> {
        if iov.iov_id != self.iov_id {
            return Err(LocalClientError::InvalidArgument {
                field: "iov",
                reason: "does not belong to this ring",
            });
        }
        Ok(())
    }

    fn next_userdata(&mut self) -> u64 {
        let userdata = self.next_userdata;
        self.next_userdata = self.next_userdata.wrapping_add(1);
        userdata
    }

    // 提交并等待自己那一项完成。等待超时或完成项对不上时结果未知：区块**故意不释放**，
    // 因为 node 仍可能对它做后端读写，立即复用会让它读到被改写的数据。
    async fn submit_and_wait(
        &mut self,
        submission: Submission,
        guard: &mut ReservedBlock<'_>,
    ) -> Result<Completion, LocalClientError> {
        self.submit(submission).await?;
        match self.wait_for(1).await {
            Ok(mut completions) => match completions.pop() {
                Some(completion) if completion.userdata == submission.userdata => Ok(completion),
                _ => {
                    guard.forfeit();
                    Err(protocol_violation(
                        "DFS completion did not match the submitted userdata",
                    ))
                }
            },
            Err(error) => {
                guard.forfeit();
                Err(error)
            }
        }
    }
}

/// 已预约区块的 RAII 守卫。
///
/// 正常完成走 [`Self::release`]；结果未知走 [`Self::forfeit`]（区块保持占用）；其它错误与
/// future 取消路径由 `Drop` 归还区块，避免一次失败把环的容量永久吃掉。
struct ReservedBlock<'a> {
    allocator: &'a mut IovAllocator,
    slice: IovSlice,
    /// true 表示区块归属已经处理过（归还或故意保留），`Drop` 不再动它。
    settled: bool,
}

impl<'a> ReservedBlock<'a> {
    fn reserve(allocator: &'a mut IovAllocator, len: usize) -> Result<Self, LocalClientError> {
        let slice = allocator.reserve(len)?;
        Ok(Self {
            allocator,
            slice,
            settled: false,
        })
    }

    fn slice(&self) -> IovSlice {
        self.slice
    }

    /// 操作确有结果（成功或 node 明确报错）：区块可以安全复用。
    fn release(mut self) -> Result<(), LocalClientError> {
        self.settled = true;
        self.allocator.release(&self.slice)?;
        Ok(())
    }

    /// 结果未知（等待超时 / 完成项不匹配）：保留区块，宁可少一块也不能让 node 正在读写的
    /// 内存被复用。这块区块会一直算作在途，直到 iov 被销毁。
    fn forfeit(&mut self) {
        self.settled = true;
    }
}

impl Drop for ReservedBlock<'_> {
    fn drop(&mut self) {
        if !self.settled {
            // 预约之后、提交之前失败（或 future 被取消）：请求从未进入环，归还区块。
            let _ = self.allocator.release(&self.slice);
        }
    }
}

/// 一批已预留的区块。
///
/// 归还规则与单发的 [`ReservedBlock`] 逐条一致，只是数量从 1 变成 N：
/// - 结果明确（成功或 node 报错）→ 全部归还，可安全复用；
/// - 结果未知（等待超时 / 完成项对不上）→ 全部故意保留，宁可少几块也不让 node 正在读写的
///   内存被复用；
/// - 预约中途失败或被取消 → `Drop` 归还已拿到的部分，不留下悬挂区块。
struct ReservedBatch<'a> {
    allocator: &'a mut IovAllocator,
    slices: Vec<IovSlice>,
    /// true 表示归属已经处理过（归还或故意保留），`Drop` 不再动它们。
    settled: bool,
}

impl<'a> ReservedBatch<'a> {
    fn reserve(
        allocator: &'a mut IovAllocator,
        lengths: &[usize],
    ) -> Result<Self, LocalClientError> {
        let mut slices = Vec::with_capacity(lengths.len());
        for length in lengths {
            match allocator.reserve(*length) {
                Ok(slice) => slices.push(slice),
                Err(error) => {
                    // 部分成功：把已拿到的还回去，避免分配器状态被一次失败带歪。
                    for slice in &slices {
                        let _ = allocator.release(slice);
                    }
                    return Err(error.into());
                }
            }
        }
        Ok(Self {
            allocator,
            slices,
            settled: false,
        })
    }

    fn slices(&self) -> &[IovSlice] {
        &self.slices
    }

    fn release_all(mut self) -> Result<(), LocalClientError> {
        self.settled = true;
        let mut outcome = Ok(());
        for slice in &self.slices {
            if let Err(error) = self.allocator.release(slice) {
                outcome = outcome.and(Err(error.into()));
            }
        }
        outcome
    }

    /// 结果未知时保留全部区块；它们会一直算作在途，直到 iov 被销毁。
    fn forfeit_all(&mut self) {
        self.settled = true;
    }
}

impl Drop for ReservedBatch<'_> {
    fn drop(&mut self) {
        if !self.settled {
            for slice in &self.slices {
                let _ = self.allocator.release(slice);
            }
        }
    }
}

/// 把完成项解释成字节数。
///
/// node 在 `status` 里回传 AFS 编码错误号、在 `result` 里回传字节数；两者分开正是为了
/// 「部分成功」与「带错误码的失败」不互相覆盖。这里把非零 `status` 转成与 gRPC 错误同一条
/// 通道的 [`LocalClientError::Status`]，调用方按 code/kind 判断，不用解析文案。
fn completion_bytes(completion: &Completion) -> Result<usize, LocalClientError> {
    if completion.status != 0 {
        return Err(LocalClientError::Status(afs_error::Error::coded(
            afs_error::ErrorCode::from_raw(completion.status),
            "DFS ring operation reported a node-side error",
        )));
    }
    if completion.result < 0 {
        return Err(protocol_violation(
            "DFS ring completion returned a negative byte count",
        ));
    }
    usize::try_from(completion.result)
        .map_err(|_| protocol_violation("DFS ring completion returned an oversized byte count"))
}

/// 一次 fd broker 授权：把 SDK 创建的共享内存 fd 交给 node。
///
/// iov 与环都是常驻结构，所以 broker 只服务一次 accept（node 在 create RPC 里领走 fd）。
/// 因此 serve 线程必须在 RPC 期间运行，并在 RPC 返回后 join，确保 socket 与 fd 生命周期闭合。
struct ShmFdGrant {
    broker: FdBrokerServer,
    request: FdRequest,
}

impl ShmFdGrant {
    fn bind() -> Result<Self, LocalClientError> {
        let id = next_shm_id();
        let token = BrokerToken::new(unique_token("afs-local-usrbio", id).into_bytes())?;
        let request = FdRequest::new(token, std::process::id().into(), id);
        let broker = FdBrokerServer::bind(broker_socket_path("afs-local-usrbio", id))?;
        Ok(Self { broker, request })
    }

    fn grant(&self, fd: OwnedFd, length: usize) -> Result<LocalShmGrant, LocalClientError> {
        let length = u32::try_from(length).map_err(|_| LocalClientError::InvalidArgument {
            field: "region.length",
            reason: "exceeds the shared-memory grant length limit",
        })?;
        self.broker
            .register(FdGrant::with_ttl(self.request.clone(), fd, GRANT_TTL))?;
        Ok(LocalShmGrant {
            broker_socket_path: self.broker.path().to_string_lossy().into_owned(),
            token: self.request.token.as_bytes().to_vec(),
            session_id: self.request.session_id,
            region_id: self.request.region_id,
            region_offset: 0,
            length,
        })
    }

    /// 授权一个非区域对象（eventfd 门铃）：broker 不为它做区域校验。
    fn grant_object(&self, fd: OwnedFd) -> Result<LocalShmGrant, LocalClientError> {
        self.broker.register(FdGrant::object_with_ttl(
            self.request.clone(),
            fd,
            GRANT_TTL,
        ))?;
        Ok(LocalShmGrant {
            broker_socket_path: self.broker.path().to_string_lossy().into_owned(),
            token: self.request.token.as_bytes().to_vec(),
            session_id: self.request.session_id,
            region_id: self.request.region_id,
            region_offset: 0,
            // 长度对 eventfd 无意义；Node 侧只按 fd 使用它。
            length: 0,
        })
    }

    fn serve_one(&self) -> thread::JoinHandle<Result<(), ShmError>> {
        let broker = self.broker.clone();
        thread::spawn(move || broker.serve_one())
    }
}

/// diagnostics 的一次操作缓冲：一个 memfd + 一次性 broker + 服务线程。
///
/// 只服务旧的 per-op 协议（`LocalReadRequest`/`LocalWriteRequest` 各自带一个 grant）；
/// usrbio 数据面使用 [`ShmFdGrant`]，不经过这里。
struct OperationBuffer {
    region: SharedRegion,
    broker: FdBrokerServer,
    request: FdRequest,
}

impl OperationBuffer {
    fn new(len: usize) -> Result<Self, ShmError> {
        // len=0 时仍创建 1 byte memfd，因为 fd 传递需要一个实际对象；
        // grant.length 仍会记录调用方请求的真实长度。
        let id = NEXT_SHM_ID.fetch_add(1, Ordering::Relaxed);
        let region_len = len.max(1);
        let region = SharedRegion::create("afs-local-sdk", region_len)?;
        let token = BrokerToken::new(unique_token("afs-local-sdk", id).into_bytes())?;
        let request = FdRequest::new(token, std::process::id().into(), id);
        let broker = FdBrokerServer::bind(broker_socket_path("afs-local-sdk", id))?;
        broker.register(FdGrant::with_ttl(
            request.clone(),
            region.duplicate_fd_owned()?,
            GRANT_TTL,
        ))?;
        Ok(Self {
            region,
            broker,
            request,
        })
    }

    fn write_local(&mut self, data: &[u8]) -> Result<(), ShmError> {
        self.region.write_at(0, data)
    }

    fn read_local(&self, len: usize) -> Result<Vec<u8>, ShmError> {
        self.region.read_at(0, len)
    }

    fn grant(&self, len: u32) -> LocalShmGrant {
        // grant 是控制面唯一看到的“数据地址”：broker socket + token/session/region。
        // token 只在本机 broker 内校验，不是跨主机认证体系；当前信任边界是同机进程。
        LocalShmGrant {
            broker_socket_path: self.broker.path().to_string_lossy().into_owned(),
            token: self.request.token.as_bytes().to_vec(),
            session_id: self.request.session_id,
            region_id: self.request.region_id,
            region_offset: 0,
            length: len,
        }
    }

    fn serve_one(&self) -> thread::JoinHandle<Result<(), ShmError>> {
        // broker 只服务一次 fd 领取；worker 必须 join 它，确保 socket 和 fd 生命周期闭合。
        let broker = self.broker.clone();
        thread::spawn(move || broker.serve_one())
    }
}

fn next_shm_id() -> u64 {
    NEXT_SHM_ID.fetch_add(1, Ordering::Relaxed)
}

fn unique_token(prefix: &str, id: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}-{}-{id}-{now}", std::process::id())
}

fn broker_socket_path(prefix: &str, id: u64) -> PathBuf {
    std::env::temp_dir().join(format!("{prefix}-{}-{id}.sock", std::process::id()))
}

impl DiagnosticLocalClient {
    pub async fn connect(config: LocalClientConfig) -> Result<Self, LocalClientError> {
        // UDS 连接仍复用统一 GrpcConfig：超时、HTTP/2 window、message size 等配置
        // 和其它 gRPC 通道一致；差异只在 connector 把“网络地址”替换成本机 socket。
        let path = Arc::new(config.socket_path);
        let grpc = GrpcConfig::default();
        // 固定的本机占位 authority：真实目标是 connector 打开的 UDS，因此这里只需要一个
        // 合法 URI；用 from_shared 而不是 expect，保证错误走统一的 transport 通道。
        let endpoint =
            grpc.configure_client(Endpoint::from_shared("http://[::]:50051".to_owned())?);
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
        // 固定的本机占位 authority：真实目标是 connector 打开的 UDS，因此这里只需要一个
        // 合法 URI；用 from_shared 而不是 expect，保证错误走统一的 transport 通道。
        let endpoint =
            grpc.configure_client(Endpoint::from_shared("http://[::]:50051".to_owned())?);
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
            namespace_id: DEFAULT_NAMESPACE.to_owned(),
        })
    }

    /// 覆盖后续 `open` 使用的 namespace。
    ///
    /// 本机 node 只服务一个挂载点，所以 namespace 属于连接配置而不是逐次调用参数；默认值
    /// 是 [`DEFAULT_NAMESPACE`]。空值会在 `open` 时被拒绝。
    pub fn with_namespace(mut self, namespace_id: impl Into<String>) -> Self {
        self.namespace_id = namespace_id.into();
        self
    }

    /// 按挂载内路径打开文件。
    ///
    /// 路径由 node 从根 inode 逐段 lookup，与 FUSE 走同一条后端路径，因此权限与目录项语义
    /// 一致。返回的 [`DfsOpenFile`] 带着本机不透明 handle 与长度；`file_version_id` 为空时
    /// 表示后端没有暴露版本身份，本轮不做固定版本校验。
    pub async fn open(
        &self,
        path: impl Into<String>,
        access: DfsAccess,
    ) -> Result<DfsOpenFile, LocalClientError> {
        let path = path.into();
        require_non_empty("namespace_id", &self.namespace_id)?;
        require_non_empty("path", &path)?;
        if !path.starts_with('/') {
            return Err(LocalClientError::InvalidArgument {
                field: "path",
                reason: "must be an absolute path inside the mount",
            });
        }
        let request = request_with_current_context(DfsOpenRequest {
            namespace_id: self.namespace_id.clone(),
            path,
            access: access.to_wire(),
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

    /// 创建持久 iov：本地建共享内存并映射，经 fd broker 交给 node，再登记 iov 编号。
    ///
    /// 建好之后每次 read/write 都只操作这块常驻内存，不再有 memfd、fd 传递或 RPC。
    /// `block_size` 必须是 2 的幂且在 4 KiB..=64 MiB 之间（见 [`IovLayout::new`]），一次操作
    /// 不得跨区块。
    pub async fn create_iov(
        &self,
        block_size: usize,
        blocks: u32,
    ) -> Result<DfsIov, LocalClientError> {
        let layout = IovLayout::new(block_size, blocks)?;
        let total_bytes = layout.total_bytes();
        // iov 是常驻结构，必须按整块长度抬高 `SharedRegion` 的默认 1 MiB 上限。
        let region = SharedRegion::create_with_limit("afs-local-iov", total_bytes, total_bytes)?;
        let mapped = MappedRegion::map(region.duplicate_fd_owned()?, total_bytes)?;
        // SAFETY: mapped 是长度为 total_bytes、由 mmap 返回的页对齐映射（页对齐强于 iov 要求的
        // 8 字节对齐）；view 与 mapped 同属返回的 DfsIov，映射在 view 存续期间不会被移动、
        // 缩容或释放。
        let view = unsafe { SharedIov::from_raw_parts(mapped.as_ptr(), layout) }?;
        view.initialize()?;

        let broker = ShmFdGrant::bind()?;
        let grant = broker.grant(region.duplicate_fd_owned()?, total_bytes)?;
        let serving = broker.serve_one();
        let request = request_with_current_context(DfsIovCreateRequest {
            region: Some(grant),
            block_size: block_size as u64,
            blocks,
        });
        let mut client = self.inner.clone();
        // 先拿 RPC 结果，再收尾 broker：错误路径同样不能留下悬挂的 socket 与线程。
        let response = tokio::time::timeout(self.request_timeout, client.iov_create(request)).await;
        let broker_result = join_broker(serving).await;
        let reply = response
            .map_err(|_| LocalClientError::Timeout)??
            .into_inner();
        broker_result?;
        if reply.block_size != block_size as u64
            || reply.blocks != blocks
            || reply.total_bytes != total_bytes as u64
        {
            return Err(protocol_violation(
                "DFS iov_create reply did not match the requested layout",
            ));
        }
        Ok(DfsIov {
            client: self.inner.clone(),
            request_timeout: self.request_timeout,
            region: mapped,
            view,
            allocator: IovAllocator::new(layout),
            iov_id: reply.iov_id,
        })
    }

    /// 在已创建的 iov 上绑定一条共享环，node 会为该环启动唯一的消费 worker。
    ///
    /// `io_depth` 是 node 侧的凑批策略：`>0` 必须攒够这么多提交才处理，凑不满会**无限期
    /// 等待**，不受 `timeout_ms` 约束；`<0` 最多凑 `|io_depth|` 个，凑不满时最多等
    /// `timeout_ms`；`0` 有多少处理多少。`io_depth > 0` 且大于 `entries` 的组合在本地直接
    /// 拒绝：这样的环永远凑不满一批，只能永久停摆。
    ///
    /// 因此单发接口（[`DfsRing::write_from`]/[`DfsRing::read_into`]）不要用在 `io_depth > 0`
    /// 的环上：一次提交凑不满一批，node 不会处理它，SDK 先等到的只有「结果未知」超时。
    ///
    /// `timeout_ms` 同时是 SDK 等待完成项的上界，传 0 时使用 [`DEFAULT_RING_TIMEOUT`]。
    pub async fn create_ring(
        &self,
        iov: &DfsIov,
        entries: u32,
        io_depth: i32,
        timeout_ms: u32,
    ) -> Result<DfsRing, LocalClientError> {
        let layout = RingLayout::new(entries)?;
        layout.validate_io_depth(io_depth)?;
        let total_bytes = layout.total_bytes();
        let region = SharedRegion::create_with_limit("afs-local-ring", total_bytes, total_bytes)?;
        let mapped = MappedRegion::map(region.duplicate_fd_owned()?, total_bytes)?;
        // SAFETY: mapped 是长度为 total_bytes 的 mmap 映射，页对齐满足环要求的 64 字节对齐；
        // ring 与 mapped 同属返回的 DfsRing，映射在 ring 存续期间不会被移动、缩容或释放。
        let ring = unsafe { SharedRing::from_raw_parts(mapped.as_ptr(), layout) }?;
        ring.initialize()?;

        // 门铃是这张环唯一的唤醒通道：没有它 node 只能靠固定间隔轮询。建不出来就不必建环。
        let doorbell = EventFd::create()?;
        let broker = ShmFdGrant::bind()?;
        let grant = broker.grant(region.duplicate_fd_owned()?, total_bytes)?;
        // 一次授权只服务一次 accept，因此门铃单独走一个 broker。
        let doorbell_broker = ShmFdGrant::bind()?;
        let doorbell_fd = doorbell
            .try_clone_fd()
            .map_err(|error| LocalClientError::Doorbell(EventFdError::Create(error)))?;
        let doorbell_grant = doorbell_broker.grant_object(doorbell_fd)?;
        // 完成通知与门铃方向相反：node 写、客户端读。客户端自己也要阻塞等它，因此
        // 再拿一个副本设为非阻塞后交给 AsyncFd，让 tokio 的 reactor 负责唤醒。
        let completion = EventFd::create()?;
        let completion_ready_fd = completion
            .try_clone_fd()
            .map_err(|error| LocalClientError::Doorbell(EventFdError::Create(error)))?;
        completion
            .set_nonblocking()
            .map_err(LocalClientError::Doorbell)?;
        let completion_ready = tokio::io::unix::AsyncFd::new(completion_ready_fd)
            .map_err(|error| LocalClientError::Doorbell(EventFdError::Create(error)))?;
        let completion_broker = ShmFdGrant::bind()?;
        let completion_fd = completion
            .try_clone_fd()
            .map_err(|error| LocalClientError::Doorbell(EventFdError::Create(error)))?;
        let completion_grant = completion_broker.grant_object(completion_fd)?;
        let serving = broker.serve_one();
        let doorbell_serving = doorbell_broker.serve_one();
        let completion_serving = completion_broker.serve_one();
        let request = request_with_current_context(DfsRingCreateRequest {
            region: Some(grant),
            iov_id: iov.iov_id,
            entries,
            io_depth,
            timeout_ms,
            doorbell: Some(doorbell_grant),
            completion: Some(completion_grant),
        });
        let mut client = self.inner.clone();
        let response =
            tokio::time::timeout(self.request_timeout, client.ring_create(request)).await;
        let broker_result = join_broker(serving).await;
        let doorbell_result = join_broker(doorbell_serving).await;
        let completion_result = join_broker(completion_serving).await;
        let reply = response
            .map_err(|_| LocalClientError::Timeout)??
            .into_inner();
        broker_result?;
        doorbell_result?;
        completion_result?;
        if reply.entries != entries {
            return Err(protocol_violation(
                "DFS ring_create reply did not match the requested entry count",
            ));
        }
        Ok(DfsRing {
            client: self.inner.clone(),
            request_timeout: self.request_timeout,
            region: mapped,
            ring,
            ring_id: reply.ring_id,
            doorbell,
            completion,
            completion_ready,
            iov_id: iov.iov_id,
            wait_timeout: ring_wait_timeout(timeout_ms),
            next_userdata: 1,
        })
    }

    /// 把已打开的句柄注册到 node，之后环内读写只携带 `reg_id`。
    ///
    /// 注册会把权限与版本身份固定下来；注册项必须用 [`DfsLocalClient::unregister_file`] 显式
    /// 撤除（或等连接关闭）。
    ///
    /// 版本身份按「能比则比」处理：`Backend` 当前不暴露版本，node 的 open/register 回复里
    /// `file_version_id` 都是空串，表示“未固定”，此时跳过比对；只有打开与注册两侧都给出了
    /// 非空版本且不一致，才说明注册期间版本发生了变更，返回协议违规错误，调用方应重新打开。
    pub async fn register_file(
        &self,
        file: &DfsOpenFile,
    ) -> Result<DfsRegistration, LocalClientError> {
        let request = request_with_current_context(DfsRegisterFileRequest {
            handle: Some(file.handle.to_wire()),
        });
        let mut client = self.inner.clone();
        let reply = tokio::time::timeout(self.request_timeout, client.register_file(request))
            .await
            .map_err(|_| LocalClientError::Timeout)??
            .into_inner();
        if version_changed(&file.file_version_id, &reply.file_version_id) {
            return Err(protocol_violation(
                "DFS file version changed between open and register",
            ));
        }
        Ok(DfsRegistration {
            reg_id: reply.reg_id,
            file_version_id: reply.file_version_id,
        })
    }

    /// 撤除注册项。撤除后 `reg_id` 不再可用于环内请求。
    pub async fn unregister_file(&self, reg: DfsRegistration) -> Result<(), LocalClientError> {
        let request = request_with_current_context(DfsUnregisterFileRequest { reg_id: reg.reg_id });
        let mut client = self.inner.clone();
        tokio::time::timeout(self.request_timeout, client.unregister_file(request))
            .await
            .map_err(|_| LocalClientError::Timeout)??;
        Ok(())
    }
}

fn ring_wait_timeout(timeout_ms: u32) -> Duration {
    if timeout_ms == 0 {
        DEFAULT_RING_TIMEOUT
    } else {
        Duration::from_millis(u64::from(timeout_ms))
    }
}

/// 打开与注册两个版本是否冲突。
///
/// `Backend` 目前不暴露版本身份，node 的回复是空串，表示“未固定”；只有两边都给出了非空
/// 版本且不相等，才说明注册期间版本确实变了。
fn version_changed(opened: &str, registered: &str) -> bool {
    !opened.is_empty() && !registered.is_empty() && opened != registered
}

// diagnostics worker 是一次操作的资源所有者：permit、OperationBuffer、broker thread 都在这里闭环。
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_wait_timeout_falls_back_only_for_zero() {
        assert_eq!(ring_wait_timeout(0), DEFAULT_RING_TIMEOUT);
        assert_eq!(ring_wait_timeout(250), Duration::from_millis(250));
    }

    #[test]
    fn version_comparison_skips_unpinned_identities() {
        // 任一侧为空都表示“未固定”，跳过比对。
        assert!(!version_changed("", ""));
        assert!(!version_changed("v1", ""));
        assert!(!version_changed("", "v1"));
        // 两侧都有版本时才比对。
        assert!(!version_changed("v1", "v1"));
        assert!(version_changed("v1", "v2"));
    }

    #[test]
    fn completion_bytes_separates_node_errors_from_byte_counts() {
        assert_eq!(
            completion_bytes(&Completion {
                result: 7,
                ..Completion::default()
            })
            .expect("successful completion"),
            7
        );

        let node_error = completion_bytes(&Completion {
            result: -1,
            status: 0x0208_0001,
            ..Completion::default()
        })
        .expect_err("non-zero status must fail");
        assert_eq!(
            node_error.code(),
            afs_error::ErrorCode::from_raw(0x0208_0001)
        );

        let negative = completion_bytes(&Completion {
            result: -1,
            ..Completion::default()
        })
        .expect_err("negative result must fail");
        assert_eq!(negative.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
    }

    #[test]
    fn reserved_block_is_returned_only_when_the_outcome_is_known() {
        let layout = IovLayout::new(4096, 2).expect("iov layout");
        let mut allocator = IovAllocator::new(layout);

        // 预约后走到作用域末尾（提交前失败或 future 取消）：区块必须还回空闲池。
        let slice = {
            let guard = ReservedBlock::reserve(&mut allocator, 16).expect("reserve");
            guard.slice()
        };
        assert_eq!(slice.len, 16);
        assert_eq!(allocator.free_blocks(), 2);
        assert_eq!(allocator.in_flight(), 0);

        // 操作有明确结果：显式释放。
        {
            let guard = ReservedBlock::reserve(&mut allocator, 16).expect("reserve");
            guard.release().expect("release");
        }
        assert_eq!(allocator.free_blocks(), 2);
        assert_eq!(allocator.in_flight(), 0);

        // 结果未知：区块保持占用，避免 node 正在读写的内存被复用。
        {
            let mut guard = ReservedBlock::reserve(&mut allocator, 16).expect("reserve");
            guard.forfeit();
        }
        assert_eq!(allocator.free_blocks(), 1);
        assert_eq!(allocator.in_flight(), 1);
    }

    #[test]
    fn iov_and_ring_errors_carry_stable_codes() {
        assert_eq!(
            LocalClientError::from(IovError::Exhausted).code(),
            afs_error::CLIENT_SHM_UNAVAILABLE
        );
        assert_eq!(
            LocalClientError::from(IovError::SpansBlocks).code(),
            afs_error::CLIENT_ARGUMENT_INVALID
        );
        assert_eq!(
            LocalClientError::from(RingError::SubmissionFull).code(),
            afs_error::CLIENT_SHM_UNAVAILABLE
        );
        assert_eq!(
            LocalClientError::from(RingError::NotInitialized).code(),
            afs_error::CLIENT_PROTOCOL_VIOLATION
        );
    }

    #[test]
    fn ring_io_depth_beyond_entries_is_rejected_upfront() {
        // 「必须攒满」的目标大于环项数时永远凑不满一批：这样的组合必须在创建前被拒绝。
        let layout = RingLayout::new(8).expect("ring layout");
        assert!(layout.validate_io_depth(16).is_err());
        // 边界与另外两种策略不受影响。
        assert!(layout.validate_io_depth(8).is_ok());
        assert!(layout.validate_io_depth(-16).is_ok());
        assert!(layout.validate_io_depth(0).is_ok());
    }

    /// 构造一对本地 iov + 环，不经过控制面：门铃与完成 eventfd 都在本进程内，
    /// `client` 是从不连接的惰性 channel（dispatch 路径不碰它）。
    fn local_ring_pair(entries: u32) -> (DfsIov, DfsRing) {
        let iov_layout = IovLayout::new(4096, 4).expect("iov layout");
        let iov_region = SharedRegion::create_with_limit(
            "afs-client-test-iov",
            iov_layout.total_bytes(),
            iov_layout.total_bytes(),
        )
        .expect("iov region");
        let iov_mapped = MappedRegion::map(
            iov_region.duplicate_fd_owned().expect("dup iov fd"),
            iov_layout.total_bytes(),
        )
        .expect("map iov");
        // SAFETY: mapping 覆盖 iov_layout.total_bytes() 字节，基址来自 MappedRegion。
        let view = unsafe { SharedIov::from_raw_parts(iov_mapped.as_ptr(), iov_layout) }
            .expect("iov view");
        view.initialize().expect("iov init");

        let ring_layout = RingLayout::new(entries).expect("ring layout");
        let ring_region = SharedRegion::create_with_limit(
            "afs-client-test-ring",
            ring_layout.total_bytes(),
            ring_layout.total_bytes(),
        )
        .expect("ring region");
        let ring_mapped = MappedRegion::map(
            ring_region.duplicate_fd_owned().expect("dup ring fd"),
            ring_layout.total_bytes(),
        )
        .expect("map ring");
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节，基址来自 MappedRegion。
        let ring = unsafe { SharedRing::from_raw_parts(ring_mapped.as_ptr(), ring_layout) }
            .expect("ring view");
        ring.initialize().expect("ring init");

        let doorbell = EventFd::create().expect("doorbell");
        let completion = EventFd::create().expect("completion");
        let ready_fd = completion.try_clone_fd().expect("dup completion");
        completion
            .set_nonblocking()
            .expect("completion nonblocking");
        let completion_ready =
            tokio::io::unix::AsyncFd::new(ready_fd).expect("completion async fd");
        let client = DfsLocalDataClient::new(traced_channel(
            Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        ));
        let request_timeout = Duration::from_secs(1);
        (
            DfsIov {
                client: client.clone(),
                request_timeout,
                region: iov_mapped,
                view,
                allocator: IovAllocator::new(iov_layout),
                iov_id: 1,
            },
            DfsRing {
                client,
                request_timeout,
                region: ring_mapped,
                ring,
                ring_id: 1,
                iov_id: 1,
                wait_timeout: Duration::from_secs(1),
                next_userdata: 1,
                doorbell,
                completion,
                completion_ready,
            },
        )
    }

    #[tokio::test]
    async fn batched_completion_mismatch_forfeits_the_reserved_blocks() {
        // 预埋两个陈旧完成项（userdata 999/998），再提交两项新请求（userdata 1/2）。
        // wait_for 取回的是陈旧完成项：对不上号 = 结果未知，区块必须整体 forfeit，不能走
        // Drop 的归还路径——node 仍可能对它们做后端读写，归还会让内存被下一批复用。
        let (mut iov, mut ring) = local_ring_pair(8);

        // 预埋两个陈旧完成项（userdata 999/998），模拟「上一波超时留下的完成项」：
        // CQ 的完成数不能超过已消费的提交数，因此先把旧提交推入 SQ、由 node 侧消费者
        // 完成它们；客户端不取走，留给这一批的 wait_for 先撞上。
        let stale_submissions = [
            Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id: 1,
                len: 16,
                offset: 0,
                iov_offset: 0,
                userdata: 999,
            },
            Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id: 1,
                len: 16,
                offset: 0,
                iov_offset: 0,
                userdata: 998,
            },
        ];
        let producer = ring.ring.submission_producer();
        for submission in &stale_submissions {
            producer.push(submission).expect("push stale submission");
        }
        let stale_completions = [
            Completion {
                userdata: 999,
                ..Completion::default()
            },
            Completion {
                userdata: 998,
                ..Completion::default()
            },
        ];
        ring.ring
            .ring_consumer()
            .complete_batch(&stale_completions)
            .expect("preload stale completions");

        let lengths = [16_usize, 16];
        let mut batch = ReservedBatch::reserve(&mut iov.allocator, &lengths).expect("reserve");
        let submissions: Vec<Submission> = batch
            .slices()
            .iter()
            .enumerate()
            .map(|(index, slice)| Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id: 1,
                len: slice.len,
                offset: 0,
                iov_offset: slice.iov_offset,
                userdata: index as u64 + 1,
            })
            .collect();

        let error = ring
            .dispatch_batch(&submissions, &mut batch)
            .await
            .expect_err("mismatched completions must fail");
        // batch 仍持有分配器的可变借用；先显式结清再读计数，否则通不过借用检查。
        drop(batch);
        assert_eq!(error.code(), afs_error::CLIENT_PROTOCOL_VIOLATION);
        assert_eq!(
            iov.allocator.free_blocks(),
            2,
            "forfeited blocks stay occupied"
        );
        assert_eq!(
            iov.allocator.in_flight(),
            2,
            "forfeited blocks are in flight"
        );
    }
}
