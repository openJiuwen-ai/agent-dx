//! usrbio 服务端：持久 iov、已注册句柄和共享环的进程内注册表，以及每条环的消费线程。
//!
//! 数据面不经过 gRPC：SDK 把字节写进 iov，用共享环的提交队列描述「哪个区块、哪个注册
//! 句柄、文件的哪个偏移」，环 worker 从同一块内存取切片直接交给 DFS 后端，再把结果写进
//! 完成队列。控制面只做三件事：建 iov、注册句柄、绑环。
//!
//! 三条不变量：
//!
//! - 每条环恰好一个提交消费者（就是它的 worker 线程），SDK 侧恰好一个生产者；
//! - 提交项在写回完成项之前，对应 iov 区块不会被 SDK 复用（由 SDK 的区块分配器保证，
//!   但环 worker 也不能在完成项落盘前再读同一区块）；
//! - 后端句柄在仍被注册时不能被关闭，否则环里的请求会指向已释放的句柄。

mod fd;
mod owner;
pub use fd::FdBridge;
pub(crate) use fd::FdSession;
pub(crate) use owner::{Owned, Reclaimed};

use std::{
    collections::HashMap,
    ffi::OsStr,
    os::fd::OwnedFd,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use afs_error::Error;
use afs_metrics::IntCounterVec;
use afs_transport::{
    eventfd::EventFd,
    iov::{IovLayout, SharedIov},
    mapped::MappedRegion,
    ring::{Completion, OP_READ, OP_WRITE, RingLayout, SharedRing, Submission},
    shm,
};

use crate::node::vfs::{Backend, types};

/// 单条 iov 的字节上限。64 GiB 远大于任何合理批次，但能挡住明显的越界请求。
const MAX_IOV_BYTES: usize = 64 * 1024 * 1024 * 1024;
/// 单批处理上限，避免一条环长时间独占 worker 而让同进程其它环饿死。
/// 单个执行周期最多消费多少提交。数值与 [`afs_transport::ring::MAX_IO_DEPTH`] 同源：
/// 「必须攒满」目标的支持上限由「单周期能执行多少」决定，两端必须一起变。
const MAX_BATCH: u32 = afs_transport::ring::MAX_IO_DEPTH as u32;

/// Advisory residence estimate, isolated to one registration. It only selects
/// scheduling; backend permission/version/deadline checks remain authoritative.
#[derive(Default)]
struct ReadCost {
    cost_per_mib_ns: AtomicU64,
    samples: AtomicU64,
}
impl ReadCost {
    fn sample(&self, len: u32) -> Option<Instant> {
        (len >= 256 * 1024
            || self
                .samples
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(16))
        .then(Instant::now)
    }
    fn observe(&self, len: u32, elapsed: Duration) {
        let units = u64::from(len).div_ceil(1024 * 1024).max(1);
        let cost = u64::try_from(elapsed.as_nanos() / u128::from(units)).unwrap_or(u64::MAX);
        let _ = self
            .cost_per_mib_ns
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |old| {
                Some(if old == 0 {
                    cost
                } else {
                    old.saturating_mul(3).saturating_add(cost) / 4
                })
            });
    }
    fn is_slow(&self) -> bool {
        self.cost_per_mib_ns.load(Ordering::Relaxed) > 250_000
    }
}
/// A Node-local lazy bounded pool, shared by every ring.
#[derive(Default)]
struct ReadExecutor {
    pool: OnceLock<Option<rayon_core::ThreadPool>>,
}
impl ReadExecutor {
    fn pool(&self) -> Option<&rayon_core::ThreadPool> {
        self.pool.get_or_init(|| {
            let workers=thread::available_parallelism().map_or(1,usize::from).min(4);
            if workers<2 {return None;}
            match rayon_core::ThreadPoolBuilder::new().num_threads(workers)
                .thread_name(|i|format!("afs-usrbio-read-{i}")).build() {
                Ok(pool)=>Some(pool),
                Err(error)=>{afs_logging::warn!("usrbio.read_pool_unavailable";"error"=>error.to_string());None}
            }
        }).as_ref()
    }
}
// Residence threshold keeps cheap copies on the ring worker; only sampled
// slow callbacks pay for parallel dispatch. The budget scales with request size.

/// 环活动计数导出到 Prometheus registry 的形状。
///
/// 与 [`RingStats`] 的分工：`RingStats` 是每条环的原子快照（控制面按需读），这里是全进程
/// 聚合的 counter（registry 按拍取）。两者都只用于观察，不参与正确性判断。
#[derive(Clone)]
pub(crate) struct RingMetrics {
    io: IntCounterVec,
}

impl RingMetrics {
    pub(crate) const NAME: &'static str = "afs_usrbio_ring_io_total";

    /// 用已注册的 counter 构造导出句柄。
    ///
    /// 注册本身由调用方在它自己的 registry 闭包里完成——那里的错误类型是 `MetricsError`，
    /// 硬塞一个 `io::Result` 只会让两边互相将就。
    pub(crate) fn from_vec(io: IntCounterVec) -> Self {
        Self { io }
    }

    fn record(&self, event: &'static str, value: u64) {
        if value > 0 {
            self.io.with_label_values(&[event]).inc_by(value);
        }
    }

    /// Resources released because their client process exited (R18).
    fn record_reclaim(&self, done: Reclaimed) {
        let count = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        self.record("reclaimed_ring", count(done.rings));
        self.record("reclaimed_registration", count(done.registrations));
        self.record("reclaimed_handle", count(done.handles));
        self.record("reclaimed_iov", count(done.iovs));
    }
}

/// 一条环的活动计数。
///
/// 全部是原子量：worker 线程写、控制面读，不引入锁，也不参与环的正确性判断——计数永远
/// 落后于真实状态，只用于观察饱和度和退化。
#[derive(Debug, Default)]
struct RingStats {
    /// 从环里取出并执行过的提交数。
    submitted: AtomicU64,
    /// 成功完成数。
    completed: AtomicU64,
    /// 失败完成数（含未知注册编号、坏区间、后端错误）。
    failed: AtomicU64,
    /// 成功传输的字节数。
    bytes: AtomicU64,
    /// 门铃唤醒次数，包含因攒批上界到期而返回的情况。
    wakeups: AtomicU64,
}

/// [`RingStats`] 的只读快照。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RingStatsSnapshot {
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub bytes: u64,
    pub wakeups: u64,
}

impl RingStats {
    fn snapshot(&self) -> RingStatsSnapshot {
        RingStatsSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            wakeups: self.wakeups.load(Ordering::Relaxed),
        }
    }
}

/// 一次打开留下的句柄状态。
#[derive(Clone)]
struct OpenedFile {
    handle: types::FileHandle,
    ctx: types::RequestContext,
}

/// 一个注册过的句柄；环内只携带 `reg_id`。
#[derive(Clone)]
struct RegisteredFile {
    handle: types::FileHandle,
    ctx: types::RequestContext,
    /// 注册时使用的句柄值，用于拒绝「注册未注销就 close」。
    opened: Vec<u8>,
    read_cost: Arc<ReadCost>,
    imported: Option<Arc<fd::ImportedFile>>,
}

/// 一条 iov 及其映射。
struct IovEntry {
    /// 只用于维持映射生命周期，直到所有引用它的环都解绑。
    _region: MappedRegion,
    view: SharedIov,
}

/// 一条已绑定的环。
struct RingEntry {
    stop: Arc<AtomicBool>,
    /// 环活动计数；worker 与观察方共享。
    stats: Arc<RingStats>,
    /// 解绑环时用它把 worker 从阻塞等待里叫醒。
    doorbell: Arc<EventFd>,
    /// 解绑环时也要敲醒可能正阻塞在完成通知上的客户端。
    completion: Arc<EventFd>,
    worker: Option<JoinHandle<()>>,
    iov_id: u64,
}

/// 注册表本体；所有字段都在同一把锁下修改，环 worker 只在查表时短暂持有。
struct HubState {
    opened: HashMap<Vec<u8>, OpenedFile>,
    registrations: HashMap<u32, RegisteredFile>,
    iovs: HashMap<u64, Arc<IovEntry>>,
    rings: HashMap<u64, RingEntry>,
    /// Which client process created each resource (R18).
    owners: owner::Owners,
    next_handle: u64,
    next_registration: u32,
    next_iov: u64,
    next_ring: u64,
    handle_epoch: u64,
}

impl HubState {
    fn allocate_registration(&mut self) -> Result<u32, Error> {
        let id = self.next_registration;
        if id == 0 || self.registrations.contains_key(&id) {
            return Err(invalid("registration identity space exhausted"));
        }
        self.next_registration = id.checked_add(1).unwrap_or(0);
        Ok(id)
    }
    fn new() -> Self {
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or_default();
        Self {
            opened: HashMap::new(),
            registrations: HashMap::new(),
            iovs: HashMap::new(),
            rings: HashMap::new(),
            owners: owner::Owners::default(),
            next_handle: 1,
            next_registration: 1,
            next_iov: 1,
            next_ring: 1,
            handle_epoch: epoch,
        }
    }

    /// 生成不透明句柄值：8 字节进程内纪元 + 8 字节序号，SDK 不解析也不拼接。
    fn fresh_handle(&mut self) -> Vec<u8> {
        let serial = self.next_handle;
        self.next_handle = self.next_handle.wrapping_add(1);
        let mut value = Vec::with_capacity(16);
        value.extend_from_slice(&self.handle_epoch.to_be_bytes());
        value.extend_from_slice(&serial.to_be_bytes());
        value
    }
}

/// 投递到后端线程的一次调用。
type BackendJob = Box<dyn FnOnce() + Send>;

/// 专用于调用后端的工作线程。
///
/// `Backend` 是同步接口，而真实实现（`GrpcDfsMeta`）每次调用都做 `Handle::block_on`：它要求当前
/// 线程**不在** tokio runtime 上下文里。`spawn_blocking` 的线程仍然带 runtime 上下文，因此不
/// 适用；只有完全独立的 std 线程才满足。这里请求经 channel 过去、结果经 oneshot 回来。
///
/// 线程在 `jobs` 全部发送端被丢弃后自然退出，不需要额外 join。
pub(crate) struct BackendThread {
    jobs: Option<std::sync::Mutex<std::sync::mpsc::Sender<BackendJob>>>,
    /// 起线程失败的原因。此时控制面每次调用都返回同一个可诊断的错误，而不是 panic。
    failure: Option<String>,
}

impl BackendThread {
    fn enqueue(&self, work: BackendJob) -> Result<(), Error> {
        self.jobs
            .as_ref()
            .ok_or_else(|| invalid("backend thread unavailable"))?
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .send(work)
            .map_err(|_| invalid("backend thread is gone"))
    }
    fn spawn() -> Self {
        let (jobs, receiver) = std::sync::mpsc::channel::<BackendJob>();
        match std::thread::Builder::new()
            .name("afs-usrbio-backend".to_owned())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    job();
                }
            }) {
            Ok(_) => Self {
                jobs: Some(std::sync::Mutex::new(jobs)),
                failure: None,
            },
            Err(error) => Self {
                jobs: None,
                failure: Some(error.to_string()),
            },
        }
    }

    pub(crate) async fn run<T, F>(&self, work: F) -> Result<T, Error>
    where
        F: FnOnce() -> Result<T, Error> + Send + 'static,
        T: Send + 'static,
    {
        let Some(jobs) = self.jobs.as_ref() else {
            return Err(Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                format!(
                    "usrbio backend thread is unavailable: {}",
                    self.failure.as_deref().unwrap_or("unknown reason")
                ),
            ));
        };
        let (reply, answer) = tokio::sync::oneshot::channel();
        jobs.lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .send(Box::new(move || {
                // 接收端已消失说明调用方放弃了这次调用，结果无处可送，丢弃即可。
                let _ = reply.send(work());
            }))
            .map_err(|_| {
                Error::coded(
                    afs_error::NODE_VFS_UNAVAILABLE,
                    "usrbio backend thread is gone",
                )
            })?;
        answer.await.map_err(|_| {
            Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "usrbio backend thread dropped the reply",
            )
        })?
    }
}

/// Stops a ring removed from the table and joins its worker.
fn stop_ring(ring_id: u64, entry: RingEntry) {
    entry.stop.store(true, Ordering::Release);
    // worker 可能正阻塞在门铃上，必须先把它叫醒才会看到 stop 标志。
    entry.doorbell.ring();
    // 客户端可能正阻塞在完成通知上；环要销毁了，必须让它返回而不是永久等待。
    entry.completion.ring();
    if let Some(Err(error)) = entry.worker.map(|worker| worker.join()) {
        afs_logging::warn!("usrbio.ring_worker_join_failed";
            "ring_id" => ring_id, "error" => format!("{error:?}"));
    }
}

/// usrbio 注册表。控制面 RPC 与环 worker 共享同一份状态。
pub(crate) struct UsrbioHub {
    backend: Arc<dyn Backend>,
    reads: Arc<ReadExecutor>,
    fd_bridge: Arc<FdBridge>,
    /// 控制面调用后端的唯一通道；环 worker 有自己的 std 线程，不经过这里。
    executor: Arc<BackendThread>,
    metrics: Option<RingMetrics>,
    namespace: Option<String>,
    identity: types::RequestContext,
    /// 与环 worker 共享；worker 只在查表时短暂持有。
    state: Arc<Mutex<HubState>>,
    /// Started with the first owned resource; `None` if it could not start.
    owner_watch: OnceLock<Option<owner::ProcessWatch>>,
}

/// 建环需要的三个跨进程 fd。
///
/// 收成一个结构体而不是三个并列参数：三者总是一起出现，分开传容易在调用点错位，
/// 而且六参以上的签名已经触发 clippy 的 `too_many_arguments`。
pub(crate) struct RingFds {
    /// 环本体（头部 + SQ + CQ）。
    pub region: OwnedFd,
    /// 客户端 -> 节点：提交后敲门铃唤醒 worker。
    pub doorbell: OwnedFd,
    /// 节点 -> 客户端：每投递一批完成项写一次。
    pub completion: OwnedFd,
}

impl UsrbioHub {
    /// 绑定一个 DFS 后端与它服务的命名空间。
    pub(crate) fn new(backend: Arc<dyn Backend>, namespace: Option<String>) -> Self {
        let state = Arc::new(Mutex::new(HubState::new()));
        let executor = Arc::new(BackendThread::spawn());
        let fd_bridge = Arc::new(FdBridge::new(
            backend.clone(),
            Arc::downgrade(&state),
            executor.clone(),
            process_identity().uid,
        ));
        Self {
            backend,
            executor,
            fd_bridge,
            reads: Arc::new(ReadExecutor::default()),
            metrics: None,
            namespace,
            identity: process_identity(),
            state,
            owner_watch: OnceLock::new(),
        }
    }

    /// Records that client process `pid` created `resource`, so the resource
    /// is reclaimed when that process exits (R18). Without a pid (no peer
    /// credentials) nothing is recorded and only explicit calls release it.
    pub(crate) fn adopt(&self, pid: Option<i32>, resource: Owned) {
        let Some(pid) = pid else {
            return;
        };
        if !self.lock().owners.adopt(pid, resource) {
            return;
        }
        let watch = self.owner_watch.get_or_init(|| {
            let metrics = self.metrics.clone();
            owner::ProcessWatch::start(
                Arc::downgrade(&self.state),
                self.backend.clone(),
                Some(Box::new(move |done: Reclaimed| {
                    if let Some(metrics) = &metrics {
                        metrics.record_reclaim(done);
                    }
                })),
            )
            .inspect_err(|error| {
                afs_logging::warn!("usrbio.owner_watch_unavailable";
                    "error" => error.to_string());
            })
            .ok()
        });
        match watch.as_ref().map(|watch| watch.watch(pid)) {
            Some(Ok(())) | None => {}
            Some(Err(error)) if error.raw_os_error() == Some(libc::ESRCH) => {
                // The client exited before it could be watched. Reclaim off
                // this (possibly async) thread: closing handles may block.
                let (state, backend) = (self.state.clone(), self.backend.clone());
                let spawned = thread::Builder::new()
                    .name("afs-usrbio-reclaim".into())
                    .spawn(move || owner::reclaim(&state, backend.as_ref(), pid));
                if let Err(error) = spawned {
                    afs_logging::warn!("usrbio.owner_reclaim_unavailable"; "pid" => pid,
                        "error" => error.to_string());
                }
            }
            Some(Err(error)) => {
                afs_logging::warn!("usrbio.owner_watch_failed"; "pid" => pid,
                    "error" => error.to_string());
            }
        }
    }

    pub(crate) fn fd_bridge(&self) -> Arc<FdBridge> {
        self.fd_bridge.clone()
    }

    /// 控制面后端执行器。RPC 处理器必须经它调用后端，不能在 runtime 线程上直接调。
    pub(crate) fn executor(&self) -> &Arc<BackendThread> {
        &self.executor
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HubState> {
        // 注册表只在极短临界区内使用，被 poison 说明之前有 panic，直接取回内部状态继续，
        // 以免一次 panic 让整条本机 SDK 永久不可用。
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 装配指标导出；未调用时 [`RingStats`] 仍然可用，只是不导出到 registry。
    pub(crate) fn with_metrics(mut self, metrics: RingMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub(crate) fn check_namespace(&self, namespace: &str) -> Result<(), Error> {
        if self
            .namespace
            .as_deref()
            .is_some_and(|expected| expected != namespace)
        {
            return Err(invalid(format!(
                "namespace {namespace} is not served by this node"
            )));
        }
        Ok(())
    }

    /// 打开一个路径，返回不透明句柄值、文件长度。
    ///
    /// 路径从后端根 inode 逐段 `lookup` 解析，与 FUSE 走同一条后端路径，因此权限和目录项
    /// 语义完全一致；`..` 与空段被拒绝，避免 SDK 用路径穿越绕过挂载边界。
    pub(crate) fn open(
        &self,
        namespace: &str,
        path: &str,
        access: OpenAccess,
    ) -> Result<(Vec<u8>, u64), Error> {
        self.check_namespace(namespace)?;
        let ctx = self.identity.clone();
        let inode = self.resolve_path(&ctx, path)?;
        let flags = match access {
            OpenAccess::ReadOnly => libc::O_RDONLY,
            OpenAccess::ReadWrite => libc::O_RDWR,
        };
        let handle =
            self.backend
                .open_with_options(&ctx, inode, flags, types::OpenOptions::default())?;
        let attributes = match self.backend.getattr(&ctx, inode, Some(handle)) {
            Ok(attributes) => attributes,
            Err(error) => {
                // 打开成功但取属性失败：先释放句柄再上抛，避免泄漏后端句柄。
                if let Err(release_error) = self.backend.release(&ctx, handle) {
                    afs_logging::warn!("usrbio.open_release_failed";
                        "error" => release_error.to_string());
                }
                return Err(error);
            }
        };
        let mut state = self.lock();
        let value = state.fresh_handle();
        state
            .opened
            .insert(value.clone(), OpenedFile { handle, ctx });
        Ok((value, attributes.size))
    }

    /// 关闭句柄（决策 12，3FS 语义）：先做一次与 FUSE close 相同的 flush，再释放。
    /// flush 失败也释放（POSIX close 失败后 fd 同样已关闭），返回 flush 的错误。
    /// 仍被注册的句柄必须先用 `unregister_file` 注销。
    pub(crate) fn close(&self, handle: &[u8]) -> Result<(), Error> {
        let mut state = self.lock();
        if state
            .registrations
            .values()
            .any(|registered| registered.opened == handle)
        {
            return Err(invalid(
                "handle is still registered for a ring; unregister it first",
            ));
        }
        let Some(opened) = state.opened.remove(handle) else {
            return Err(invalid("unknown DFS file handle"));
        };
        state.owners.forget(&Owned::Handle(handle.to_vec()));
        drop(state);
        let flushed = self.backend.flush(&opened.ctx, opened.handle);
        let released = self.backend.release(&opened.ctx, opened.handle);
        flushed.and(released)
    }

    /// 应用显式屏障。
    pub(crate) fn sync(
        &self,
        handle: &[u8],
        mode: types::SyncMode,
    ) -> Result<types::SyncReceipt, Error> {
        let opened = {
            let state = self.lock();
            state
                .opened
                .get(handle)
                .cloned()
                .ok_or_else(|| invalid("unknown DFS file handle"))?
        };
        self.backend.fsync_receipt(&opened.ctx, opened.handle, mode)
    }

    /// 建立持久 iov 的 Node 侧视图。
    pub(crate) fn create_iov(&self, fd: OwnedFd, layout: IovLayout) -> Result<u64, Error> {
        let total = layout.total_bytes();
        if total > MAX_IOV_BYTES {
            return Err(invalid("iov is larger than the supported maximum"));
        }
        shm::validate_region_fd(std::os::fd::AsRawFd::as_raw_fd(&fd), total, MAX_IOV_BYTES)
            .map_err(|error| invalid(format!("iov region rejected: {error}")))?;
        let region = MappedRegion::map(fd, total)
            .map_err(|error| invalid(format!("iov mapping failed: {error}")))?;
        // SAFETY: map 保证 base 指向 layout.total_bytes() 字节的可读写映射，且映射生命周期
        // 由 `_region` 持有到 IovEntry 释放为止。
        let view = unsafe { SharedIov::from_raw_parts(region.as_ptr(), layout) }
            .map_err(|error| invalid(format!("iov layout rejected: {error}")))?;
        view.validate()
            .map_err(|error| invalid(format!("iov header rejected: {error}")))?;

        let mut state = self.lock();
        let iov_id = state.next_iov;
        state.next_iov += 1;
        state.iovs.insert(
            iov_id,
            Arc::new(IovEntry {
                _region: region,
                view,
            }),
        );
        Ok(iov_id)
    }

    /// 销毁 iov。仍被环引用的 iov 必须先解绑对应环。
    pub(crate) fn destroy_iov(&self, iov_id: u64) -> Result<(), Error> {
        let mut state = self.lock();
        if state.rings.values().any(|ring| ring.iov_id == iov_id) {
            return Err(invalid("iov is still bound to a ring"));
        }
        state.owners.forget(&Owned::Iov(iov_id));
        state
            .iovs
            .remove(&iov_id)
            .map(|_| ())
            .ok_or_else(|| invalid("unknown iov id"))
    }

    /// 注册打开句柄，返回注册编号。
    pub(crate) fn register_file(&self, handle: &[u8]) -> Result<u32, Error> {
        let mut state = self.lock();
        let Some(opened) = state.opened.get(handle).cloned() else {
            return Err(invalid("unknown DFS file handle"));
        };
        let registration = state.allocate_registration()?;
        state.registrations.insert(
            registration,
            RegisteredFile {
                handle: opened.handle,
                ctx: opened.ctx,
                opened: handle.to_vec(),
                read_cost: Arc::new(ReadCost::default()),
                imported: None,
            },
        );
        Ok(registration)
    }

    /// 注销注册编号。
    ///
    /// 调用方必须先确认该编号上没有在途提交：注销只把编号从注册表移除，已经进入 worker
    /// 的提交仍会用它已取到的句柄执行。`DfsRing::wait_for` 返回之后才注销是唯一安全的顺序。
    pub(crate) fn unregister_file(&self, registration: u32) -> Result<(), Error> {
        let mut state = self.lock();
        if state
            .registrations
            .get(&registration)
            .is_some_and(|r| r.imported.is_some())
        {
            return Err(invalid(
                "POSIX registrations require the fd handoff control",
            ));
        }
        state.owners.forget(&Owned::Registration(registration));
        state
            .registrations
            .remove(&registration)
            .map(|_| ())
            .ok_or_else(|| invalid("unknown registration id"))
    }

    /// 绑定一条共享环并启动它的 worker。
    #[cfg(test)]
    pub(crate) fn create_ring(
        &self,
        fds: RingFds,
        layout: RingLayout,
        iov_id: u64,
        io_depth: i32,
        timeout_ms: u32,
    ) -> Result<u64, Error> {
        self.create_ring_owned(fds, layout, iov_id, (io_depth, timeout_ms), None)
    }

    pub(crate) fn create_ring_owned(
        &self,
        fds: RingFds,
        layout: RingLayout,
        iov_id: u64,
        policy: (i32, u32),
        session: Option<Arc<FdSession>>,
    ) -> Result<u64, Error> {
        let (io_depth, timeout_ms) = policy;
        // 凑批策略在创建时校验：「必须攒满」的目标大于环项数时永远凑不满一批，环只能
        // 永久停摆；绝对值超过单周期执行批量（ring::MAX_IO_DEPTH）的目标同样拒绝，
        // 攒满后的剩余提交到不了下一轮门槛。两种情况都直接拒绝，不静默截断。
        layout
            .validate_io_depth(io_depth)
            .map_err(|error| invalid(format!("ring io_depth rejected: {error}")))?;
        let RingFds {
            region: fd,
            doorbell: doorbell_fd,
            completion: completion_fd,
        } = fds;
        let total = layout.total_bytes();
        shm::validate_region_fd(std::os::fd::AsRawFd::as_raw_fd(&fd), total, MAX_IOV_BYTES)
            .map_err(|error| invalid(format!("ring region rejected: {error}")))?;
        let region = MappedRegion::map(fd, total)
            .map_err(|error| invalid(format!("ring mapping failed: {error}")))?;

        let mut state = self.lock();
        let Some(iov) = state.iovs.get(&iov_id).cloned() else {
            return Err(invalid("unknown iov id for ring"));
        };
        let ring_id = state.next_ring;
        state.next_ring += 1;

        // SAFETY: 映射覆盖 layout.total_bytes() 字节，生命周期由 worker 内的 region 持有。
        let ring = unsafe { SharedRing::from_raw_parts(region.as_ptr(), layout) }
            .map_err(|error| invalid(format!("ring layout rejected: {error}")))?;
        ring.validate()
            .map_err(|error| invalid(format!("ring header rejected: {error}")))?;

        let stop = Arc::new(AtomicBool::new(false));
        let doorbell = Arc::new(EventFd::from_owned_fd(doorbell_fd));
        let completion = Arc::new(EventFd::from_owned_fd(completion_fd));
        let stats = Arc::new(RingStats::default());
        let worker = RingWorker {
            backend: self.backend.clone(),
            reads: self.reads.clone(),
            session,
            iov,
            _region: region,
            state: self.state_handle(),
            ring,
            stop: stop.clone(),
            doorbell: doorbell.clone(),
            completion: completion.clone(),
            stats: stats.clone(),
            metrics: self.metrics.clone(),
            io_depth,
            timeout: Duration::from_millis(u64::from(timeout_ms)),
        };
        let spawned = thread::Builder::new()
            .name(format!("afs-usrbio-ring-{ring_id}"))
            .spawn(move || worker.run());
        let handle = match spawned {
            Ok(handle) => handle,
            Err(error) => {
                return Err(Error::coded(
                    afs_error::NODE_VFS_UNAVAILABLE,
                    format!("failed to start ring worker: {error}"),
                ));
            }
        };
        state.rings.insert(
            ring_id,
            RingEntry {
                stop,
                doorbell,
                completion,
                stats,
                worker: Some(handle),
                iov_id,
            },
        );
        Ok(ring_id)
    }

    /// 读一条环的活动计数；环不存在时返回 `None`。
    ///
    /// 计数是观察性的，不参与正确性判断：它落后于真实状态，环解绑后即消失。
    pub(crate) fn ring_stats(&self, ring_id: u64) -> Option<RingStatsSnapshot> {
        let state = self.lock();
        state
            .rings
            .get(&ring_id)
            .map(|entry| entry.stats.snapshot())
    }

    /// 解绑环并等待 worker 退出。
    ///
    /// worker 退出前会把队列里剩余的提交以 `NODE_VFS_UNAVAILABLE` 失败完成，SDK 不会因为
    /// 解绑而永久等待。
    pub(crate) fn destroy_ring(&self, ring_id: u64) -> Result<(), Error> {
        let entry = {
            let mut state = self.lock();
            state.owners.forget(&Owned::Ring(ring_id));
            state
                .rings
                .remove(&ring_id)
                .ok_or_else(|| invalid("unknown ring id"))?
        };
        stop_ring(ring_id, entry);
        Ok(())
    }

    /// 停止所有环；服务关闭时调用。
    pub(crate) fn shutdown(&self) {
        let entries = {
            let mut state = self.lock();
            std::mem::take(&mut state.rings)
        };
        for (ring_id, entry) in entries {
            stop_ring(ring_id, entry);
        }
    }

    fn resolve_path(
        &self,
        ctx: &types::RequestContext,
        path: &str,
    ) -> Result<types::BackendInode, Error> {
        if !path.starts_with('/') {
            return Err(invalid("path must be absolute"));
        }
        let mut inode = self.backend.root_inode();
        let mut components = 0_usize;
        for component in path.split('/') {
            if component.is_empty() || component == "." {
                continue;
            }
            if component == ".." {
                return Err(invalid("path must not contain parent traversal"));
            }
            let entry = self.backend.lookup(ctx, inode, OsStr::new(component))?;
            inode = entry.inode;
            components += 1;
        }
        if components == 0 {
            return Err(invalid("path must name a file"));
        }
        Ok(inode)
    }

    /// 环 worker 需要同一份注册表，`&self` 不能跨线程，因此共享 Arc。
    fn state_handle(&self) -> Arc<Mutex<HubState>> {
        self.state.clone()
    }
}

impl Drop for UsrbioHub {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// 打开访问模式。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OpenAccess {
    ReadOnly,
    ReadWrite,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_VFS_INVALID, message)
}

/// 环的消费循环。
struct RingWorker {
    session: Option<Arc<FdSession>>,
    backend: Arc<dyn Backend>,
    reads: Arc<ReadExecutor>,
    iov: Arc<IovEntry>,
    /// 环的映射必须由 worker 持有：`SharedRing` 只存裸指针，映射一旦释放就是野指针。
    _region: MappedRegion,
    state: Arc<Mutex<HubState>>,
    ring: SharedRing,
    stop: Arc<AtomicBool>,
    doorbell: Arc<EventFd>,
    /// 完成通知：每投递一批完成项写一次，客户端因此可以真正阻塞等待。与门铃方向相反。
    completion: Arc<EventFd>,
    stats: Arc<RingStats>,
    metrics: Option<RingMetrics>,
    io_depth: i32,
    timeout: Duration,
}

// Eligibility has already range-checked both requests against one live iov.
fn ranges_overlap(a: &Submission, b: &Submission) -> bool {
    a.iov_offset < b.iov_offset.saturating_add(u64::from(b.len))
        && b.iov_offset < a.iov_offset.saturating_add(u64::from(a.len))
}

impl RingWorker {
    fn run(self) {
        let stop = self.stop.clone();
        // 攒批上界只在「等满但有时限」时才有值（`io_depth < 0`）；`io_depth > 0` 必须等满，
        // `0` 有多少处理多少。跨迭代保留它，否则每轮重新计时会让上界永远不到。
        let mut batch_deadline: Option<Instant> = None;
        while !stop.load(Ordering::Acquire) {
            let consumer = self.ring.ring_consumer();
            let (usable, target) = self.snapshot(&consumer);

            if self.io_depth < 0 && usable > 0 && usable < target && batch_deadline.is_none() {
                batch_deadline = Some(Instant::now() + self.timeout);
            }
            let expired = batch_deadline.is_some_and(|deadline| Instant::now() >= deadline);
            let ready = if self.io_depth == 0 {
                usable > 0
            } else {
                usable > 0 && (usable >= target || expired)
            };

            if !ready {
                // **唯一的等待点**。多设一个等待点会丢唤醒：解绑环只敲一次门铃，
                // 若它被内层等待消费掉，外层就会永久阻塞，join 再也回不来。
                let wait = batch_deadline
                    .map(|deadline| deadline.saturating_duration_since(Instant::now()));
                if self.doorbell.wait(wait) {
                    self.stats.wakeups.fetch_add(1, Ordering::Relaxed);
                    if let Some(metrics) = &self.metrics {
                        metrics.record("wakeups", 1);
                    }
                }
                continue;
            }

            let mut completions = Vec::with_capacity(usable as usize);
            let mut index = 0;
            while index < usable {
                let Some(submission) = consumer.peek_at(index) else {
                    break;
                };
                if index + 1 < usable && self.parallel_read_allowed(&submission) {
                    let mut group = vec![submission];
                    for next in index + 1..usable {
                        let Some(candidate) = consumer.peek_at(next) else {
                            break;
                        };
                        if !self.parallel_read_allowed(&candidate)
                            || group.iter().any(|s| ranges_overlap(s, &candidate))
                        {
                            break;
                        }
                        group.push(candidate);
                    }
                    if group.len() > 1 {
                        let start = completions.len();
                        completions.resize(start + group.len(), Completion::default());
                        if let Some(pool) = self.reads.pool() {
                            let worker = &self;
                            pool.scope(|scope| {
                                for (s, out) in group.iter().zip(&mut completions[start..]) {
                                    scope.spawn(move |_| {
                                        *out = worker.execute(s);
                                    });
                                }
                            });
                        } else {
                            for (s, out) in group.iter().zip(&mut completions[start..]) {
                                *out = self.execute(s);
                            }
                        }
                        index += group.len() as u32;
                        continue;
                    }
                }
                completions.push(self.execute(&submission));
                index += 1;
            }
            for completion in &completions {
                self.stats.submitted.fetch_add(1, Ordering::Relaxed);
                if let Some(metrics) = &self.metrics {
                    metrics.record("submitted", 1);
                }
                if completion.status == 0 {
                    self.stats.completed.fetch_add(1, Ordering::Relaxed);
                    if let Some(metrics) = &self.metrics {
                        metrics.record("completed", 1);
                    }
                    if completion.result > 0 {
                        let bytes = completion.result as u64;
                        self.stats.bytes.fetch_add(bytes, Ordering::Relaxed);
                        if let Some(metrics) = &self.metrics {
                            metrics.record("bytes", bytes);
                        }
                    }
                } else {
                    self.stats.failed.fetch_add(1, Ordering::Relaxed);
                    if let Some(metrics) = &self.metrics {
                        metrics.record("failed", 1);
                    }
                }
            }
            batch_deadline = None;
            if completions.is_empty() {
                continue;
            }
            if let Err(error) = consumer.complete_batch(&completions) {
                // snapshot 已按完成队列空间限制批量，走到这里是布局或头部被破坏。
                afs_logging::warn!("usrbio.ring_complete_failed";"error" => error.to_string());
                break;
            }
            // 一批只敲一次：让阻塞在完成通知上的客户端立刻看到这批完成项。
            self.completion.ring();
        }
        self.fail_pending();
    }

    /// 此刻可处理的提交数与攒批目标。
    ///
    /// `io_depth > 0` 必须等满；`< 0` 等满但有 `timeout` 上界；`0` 有多少处理多少。
    /// 凑批目标不做主动截断——创建环时已拒绝绝对值超过 [`MAX_BATCH`] 的目标，这里的
    /// `.min` 只是布局被破坏时的防御，正常路径上不会触发。
    fn snapshot(&self, consumer: &afs_transport::ring::RingConsumer<'_>) -> (u32, u32) {
        let target = match self.io_depth {
            depth if depth > 0 => (depth as u32).min(MAX_BATCH),
            depth if depth < 0 => depth.unsigned_abs().min(MAX_BATCH),
            _ => 1,
        };
        let ready = consumer.available();
        let space = consumer.completion_space();
        (ready.min(space).min(MAX_BATCH), target)
    }

    fn parallel_read_allowed(&self, submission: &Submission) -> bool {
        if submission.op != OP_READ
            || self
                .iov
                .view
                .layout()
                .validate_range(submission.iov_offset, u64::from(submission.len))
                .is_err()
        {
            return false;
        }
        let registered = self.state.lock().ok().and_then(|state| {
            state
                .registrations
                .get(&submission.reg_id)
                .filter(|r| r.read_cost.is_slow())
                .cloned()
        });
        registered.is_some_and(|r| {
            let _lease = match r
                .imported
                .as_ref()
                .map(|i| i.begin(OP_READ, self.session.as_ref()))
                .transpose()
            {
                Ok(lease) => lease,
                Err(_) => return false,
            };
            self.backend.supports_parallel_reads(r.handle)
        })
    }

    fn execute(&self, submission: &Submission) -> Completion {
        let mut completion = Completion {
            userdata: submission.userdata,
            result: -1,
            write_seq: 0,
            status: 0,
            op: submission.op,
        };
        let registered = {
            let Ok(state) = self.state.lock() else {
                completion.status = afs_error::NODE_VFS_UNAVAILABLE.raw();
                return completion;
            };
            state.registrations.get(&submission.reg_id).cloned()
        };
        let Some(registered) = registered else {
            completion.status = afs_error::NODE_VFS_INVALID.raw();
            return completion;
        };
        let _fd_use = match registered
            .imported
            .as_ref()
            .map(|i| i.begin(submission.op, self.session.as_ref()))
            .transpose()
        {
            Ok(lease) => lease,
            Err(error) => {
                completion.status = error.code().raw();
                return completion;
            }
        };
        let length = u64::from(submission.len);
        match submission.op {
            OP_READ => {
                let Ok(destination) = self.iov.view.write_ptr(submission.iov_offset, length) else {
                    completion.status = afs_error::NODE_VFS_INVALID.raw();
                    return completion;
                };
                // SAFETY: write_ptr 已按 iov 布局校验区间落在映射内且不跨区块；该区块在
                // 对应完成项被 SDK 取走前不会被复用，因此这里持有的是独占写入权。
                let buffer =
                    unsafe { std::slice::from_raw_parts_mut(destination, submission.len as usize) };
                let sampled = registered.read_cost.sample(submission.len);
                let outcome = self.backend.read(
                    &registered.ctx,
                    registered.handle,
                    submission.offset,
                    buffer,
                );
                if let Some(start) = sampled {
                    registered
                        .read_cost
                        .observe(submission.len, start.elapsed());
                }
                match outcome {
                    Ok(read) => completion.result = read as i64,
                    Err(error) => completion.status = error.code().raw(),
                }
            }
            OP_WRITE => {
                let Ok(source) = self.iov.view.slice(submission.iov_offset, length) else {
                    completion.status = afs_error::NODE_VFS_INVALID.raw();
                    return completion;
                };
                match self.backend.write(
                    &registered.ctx,
                    registered.handle,
                    submission.offset,
                    source,
                ) {
                    Ok(written) => completion.result = written as i64,
                    Err(error) => completion.status = error.code().raw(),
                }
            }
            _ => completion.status = afs_error::NODE_VFS_INVALID.raw(),
        }
        completion
    }

    /// 退出前把仍在队列里的提交全部以失败完成，避免 SDK 永久等待。
    fn fail_pending(self) {
        let consumer = self.ring.ring_consumer();
        let pending = consumer.available().min(self.ring.layout().entries());
        if pending == 0 {
            return;
        }
        let mut completions = Vec::with_capacity(pending as usize);
        for index in 0..pending {
            let Some(submission) = consumer.peek_at(index) else {
                break;
            };
            completions.push(Completion {
                userdata: submission.userdata,
                result: -1,
                write_seq: 0,
                status: afs_error::NODE_VFS_UNAVAILABLE.raw(),
                op: submission.op,
            });
        }
        if !completions.is_empty() {
            let _ = consumer.complete_batch(&completions);
            consumer.doorbell();
            // 完成通知 eventfd 是 SDK 唯一的唤醒通道。destroy_ring 在 join 前敲的那一下
            // 可能已经在这批 CQE 写入前被 SDK 读掉，这里必须再敲一次，否则 SDK 只能等到
            // 自己的上界，把已经失败完成的提交当成「结果未知」处理。
            self.completion.ring();
        }
    }
}

/// 本机 SDK 请求使用的身份。
///
/// UDS 是 owner-only 的，能连上来的进程与 node 同 uid，因此这里用 node 自己的实际身份
/// （含 supplementary groups）表达调用者，而不是伪造 root。这样后端照常执行自己的权限
/// 检查，越权请求仍然失败。
/// 测试用的完成通知 fd：节点侧只会在投递完成后写它，测试不需要读。
#[cfg(test)]
fn test_completion_fd() -> OwnedFd {
    EventFd::create()
        .expect("completion eventfd")
        .try_clone_fd()
        .expect("dup completion")
}

fn process_identity() -> types::RequestContext {
    // SAFETY: geteuid 是无参数的只读查询，不触碰内存。
    let uid = unsafe { libc::geteuid() };
    // SAFETY: getegid 是无参数的只读查询，不触碰内存。
    let gid = unsafe { libc::getegid() };
    // SAFETY: getpid 是无参数的只读查询，不触碰内存。
    let pid = unsafe { libc::getpid() } as u32;
    types::RequestContext {
        uid,
        gid,
        pid,
        umask: 0,
        supplementary_gids: process_groups(),
    }
}

fn process_groups() -> Vec<u32> {
    // SAFETY: 传入空指针且 size 为 0 时 getgroups 只返回组数，不写入任何内存。
    let count = unsafe { libc::getgroups(0, std::ptr::null_mut()) };
    if count <= 0 {
        return Vec::new();
    }
    let mut groups = vec![0 as libc::gid_t; count as usize];
    // SAFETY: 缓冲区按 getgroups 报告的数量分配，写入不超过该数量。
    let written = unsafe { libc::getgroups(count, groups.as_mut_ptr()) };
    if written <= 0 {
        return Vec::new();
    }
    groups.truncate(written as usize);
    // Linux 与 macOS 的 gid_t 都是 u32，因此这里不做可能失败的窄化转换。
    groups
}

#[cfg(test)]
pub(crate) mod tests;
