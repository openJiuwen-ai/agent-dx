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

use std::{
    collections::HashMap,
    ffi::OsStr,
    os::fd::OwnedFd,
    sync::{
        Arc, Mutex,
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
const MAX_BATCH: u32 = 256;

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
    next_handle: u64,
    next_registration: u32,
    next_iov: u64,
    next_ring: u64,
    handle_epoch: u64,
}

impl HubState {
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

/// usrbio 注册表。控制面 RPC 与环 worker 共享同一份状态。
pub(crate) struct UsrbioHub {
    backend: Arc<dyn Backend>,
    /// 控制面调用后端的唯一通道；环 worker 有自己的 std 线程，不经过这里。
    executor: Arc<BackendThread>,
    metrics: Option<RingMetrics>,
    namespace: Option<String>,
    identity: types::RequestContext,
    /// 与环 worker 共享；worker 只在查表时短暂持有。
    state: Arc<Mutex<HubState>>,
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
        Self {
            backend,
            executor: Arc::new(BackendThread::spawn()),
            metrics: None,
            namespace,
            identity: process_identity(),
            state: Arc::new(Mutex::new(HubState::new())),
        }
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
        if self
            .namespace
            .as_deref()
            .is_some_and(|expected| expected != namespace)
        {
            return Err(invalid(format!(
                "namespace {namespace} is not served by this node"
            )));
        }
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

    /// 释放句柄。仍被注册的句柄必须先用 `unregister_file` 注销。
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
        drop(state);
        self.backend.release(&opened.ctx, opened.handle)
    }

    /// 应用显式屏障。
    pub(crate) fn sync(&self, handle: &[u8], mode: types::SyncMode) -> Result<(), Error> {
        let opened = {
            let state = self.lock();
            state
                .opened
                .get(handle)
                .cloned()
                .ok_or_else(|| invalid("unknown DFS file handle"))?
        };
        self.backend.fsync(&opened.ctx, opened.handle, mode)
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
        let registration = state.next_registration;
        state.next_registration = state.next_registration.wrapping_add(1).max(1);
        state.registrations.insert(
            registration,
            RegisteredFile {
                handle: opened.handle,
                ctx: opened.ctx,
                opened: handle.to_vec(),
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
        state
            .registrations
            .remove(&registration)
            .map(|_| ())
            .ok_or_else(|| invalid("unknown registration id"))
    }

    /// 绑定一条共享环并启动它的 worker。
    pub(crate) fn create_ring(
        &self,
        fds: RingFds,
        layout: RingLayout,
        iov_id: u64,
        io_depth: i32,
        timeout_ms: u32,
    ) -> Result<u64, Error> {
        // 「必须攒满」的目标大于环项数时永远凑不满一批，环只能永久停摆，创建时直接拒绝。
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
            state
                .rings
                .remove(&ring_id)
                .ok_or_else(|| invalid("unknown ring id"))?
        };
        entry.stop.store(true, Ordering::Release);
        // worker 可能正阻塞在门铃上，必须先把它叫醒才会看到 stop 标志。
        entry.doorbell.ring();
        // 客户端可能正阻塞在完成通知上；环要销毁了，必须让它返回而不是永久等待。
        entry.completion.ring();
        if let Some(Err(error)) = entry.worker.map(|worker| worker.join()) {
            afs_logging::warn!("usrbio.ring_worker_join_failed";
                "ring_id" => ring_id, "error" => format!("{error:?}"));
        }
        Ok(())
    }

    /// 停止所有环；服务关闭时调用。
    pub(crate) fn shutdown(&self) {
        let entries = {
            let mut state = self.lock();
            std::mem::take(&mut state.rings)
        };
        for (ring_id, entry) in entries {
            entry.stop.store(true, Ordering::Release);
            entry.doorbell.ring();
            entry.completion.ring();
            if let Some(Err(error)) = entry.worker.map(|worker| worker.join()) {
                afs_logging::warn!("usrbio.ring_worker_join_failed";
                    "ring_id" => ring_id, "error" => format!("{error:?}"));
            }
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
    backend: Arc<dyn Backend>,
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
            for index in 0..usable {
                let Some(submission) = consumer.peek_at(index) else {
                    break;
                };
                completions.push(self.execute(&submission));
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
                match self.backend.read(
                    &registered.ctx,
                    registered.handle,
                    submission.offset,
                    buffer,
                ) {
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
pub(crate) mod tests {
    use super::*;
    use crate::node::vfs::types::{
        BackendInode, Entry, FileAttributes, FileHandle, FileKind, OpenOptions,
    };
    use afs_metrics::{IntCounterVec, Opts, Registry};
    use afs_transport::eventfd::EventFd;
    use afs_transport::iov::IovAllocator;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use std::time::{Duration, Instant};

    const FILE_INODE: u64 = 2;
    const FILE_NAME: &str = "data.bin";

    /// 只实现 usrbio 用到的最小内存后端；用于在没有 DFS meta/chunk 的情况下验证环数据面。
    pub(crate) struct MemoryBackend {
        files: StdMutex<HashMap<u64, Vec<u8>>>,
        handles: StdMutex<HashMap<u64, u64>>,
        next_handle: StdMutex<u64>,
        /// 记录后端实际收到的同步屏障，用来断言 SDK 没有把 DataOnly 悄悄降级成 Full 或反之。
        last_sync: StdMutex<Option<types::SyncMode>>,
    }

    impl MemoryBackend {
        pub(crate) fn new(size: usize) -> Self {
            let mut files = HashMap::new();
            files.insert(FILE_INODE, vec![0_u8; size]);
            Self {
                files: StdMutex::new(files),
                handles: StdMutex::new(HashMap::new()),
                next_handle: StdMutex::new(1),
                last_sync: StdMutex::new(None),
            }
        }

        fn attributes(&self, inode: u64) -> FileAttributes {
            let size = self
                .files
                .lock()
                .expect("files")
                .get(&inode)
                .map_or(0, |bytes| bytes.len() as u64);
            let now = SystemTime::now();
            FileAttributes {
                kind: FileKind::Regular,
                size,
                blocks: size.div_ceil(512),
                mode: 0o644,
                uid: 0,
                gid: 0,
                nlink: 1,
                atime: now,
                mtime: now,
                ctime: now,
            }
        }

        fn inode_of(&self, handle: FileHandle) -> Result<u64, Error> {
            self.handles
                .lock()
                .expect("handles")
                .get(&handle.0)
                .copied()
                .ok_or_else(|| invalid("unknown handle"))
        }
    }

    impl Backend for MemoryBackend {
        fn root_inode(&self) -> BackendInode {
            BackendInode { value: 1 }
        }

        fn lookup(
            &self,
            _ctx: &types::RequestContext,
            parent: BackendInode,
            name: &OsStr,
        ) -> Result<Entry, Error> {
            if parent.value != 1 || name != OsStr::new(FILE_NAME) {
                return Err(Error::coded(afs_error::NODE_VFS_NOT_FOUND, "no such entry"));
            }
            Ok(Entry {
                inode: BackendInode { value: FILE_INODE },
                attributes: self.attributes(FILE_INODE),
            })
        }

        fn open_with_options(
            &self,
            _ctx: &types::RequestContext,
            inode: BackendInode,
            _flags: i32,
            _options: OpenOptions,
        ) -> Result<FileHandle, Error> {
            if inode.value != FILE_INODE {
                return Err(Error::coded(afs_error::NODE_VFS_NOT_FOUND, "no such inode"));
            }
            let mut next = self.next_handle.lock().expect("next handle");
            let handle = FileHandle(*next);
            *next += 1;
            self.handles
                .lock()
                .expect("handles")
                .insert(handle.0, inode.value);
            Ok(handle)
        }

        fn getattr(
            &self,
            _ctx: &types::RequestContext,
            inode: BackendInode,
            _handle: Option<FileHandle>,
        ) -> Result<FileAttributes, Error> {
            Ok(self.attributes(inode.value))
        }

        fn read(
            &self,
            _ctx: &types::RequestContext,
            handle: FileHandle,
            offset: u64,
            out: &mut [u8],
        ) -> Result<usize, Error> {
            let inode = self.inode_of(handle)?;
            let files = self.files.lock().expect("files");
            let data = files.get(&inode).ok_or_else(|| invalid("no such file"))?;
            let start = usize::try_from(offset).map_err(|_| invalid("offset too large"))?;
            if start >= data.len() {
                return Ok(0);
            }
            let available = (data.len() - start).min(out.len());
            out[..available].copy_from_slice(&data[start..start + available]);
            Ok(available)
        }

        fn write(
            &self,
            _ctx: &types::RequestContext,
            handle: FileHandle,
            offset: u64,
            data: &[u8],
        ) -> Result<usize, Error> {
            let inode = self.inode_of(handle)?;
            let mut files = self.files.lock().expect("files");
            let target = files
                .get_mut(&inode)
                .ok_or_else(|| invalid("no such file"))?;
            let start = usize::try_from(offset).map_err(|_| invalid("offset too large"))?;
            let end = start
                .checked_add(data.len())
                .ok_or_else(|| invalid("range overflows"))?;
            if end > target.len() {
                target.resize(end, 0);
            }
            target[start..end].copy_from_slice(data);
            Ok(data.len())
        }

        fn fsync(
            &self,
            _ctx: &types::RequestContext,
            _handle: FileHandle,
            mode: types::SyncMode,
        ) -> Result<(), Error> {
            *self.last_sync.lock().expect("last sync") = Some(mode);
            Ok(())
        }

        fn release(&self, _ctx: &types::RequestContext, handle: FileHandle) -> Result<(), Error> {
            self.handles.lock().expect("handles").remove(&handle.0);
            Ok(())
        }
    }

    /// SDK 侧与 Node 侧各自映射同一块 memfd，模拟真实的两进程关系。
    struct TestRegion {
        _region: shm::SharedRegion,
        mapping: MappedRegion,
    }

    impl TestRegion {
        fn new(name: &str, len: usize) -> Self {
            let region = shm::SharedRegion::create_with_limit(name, len, len).expect("region");
            let mapping =
                MappedRegion::map(region.duplicate_fd_owned().expect("dup"), len).expect("map");
            Self {
                _region: region,
                mapping,
            }
        }

        fn handle_fd(&self) -> OwnedFd {
            self._region.duplicate_fd_owned().expect("dup")
        }
    }

    fn wait_for_completion(consumer: &afs_transport::ring::CompletionConsumer<'_>) -> Completion {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(completion) = consumer.pop() {
                return completion;
            }
            assert!(Instant::now() < deadline, "ring completion timed out");
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn ring_round_trip_writes_and_reads_through_shared_memory() {
        let backend = Arc::new(MemoryBackend::new(4096));
        let hub = UsrbioHub::new(backend.clone(), Some("test".to_owned()));

        // iov：4 个 4 KiB 区块
        let layout = IovLayout::new(4096, 4).expect("iov layout");
        let iov_region = TestRegion::new("usrbio-iov", layout.total_bytes());
        // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
        let iov_view =
            unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
        iov_view.initialize().expect("iov init");
        let iov_id = hub
            .create_iov(iov_region.handle_fd(), layout)
            .expect("create iov");

        // 环：8 项
        let ring_layout = RingLayout::new(8).expect("ring layout");
        let ring_region = TestRegion::new("usrbio-ring", ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
        ring.initialize().expect("ring init");
        // worker 阻塞在门铃上，测试必须持有同一个 eventfd 的副本才能唤醒它。
        let doorbell = EventFd::create().expect("eventfd");
        let ring_id = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                iov_id,
                0,
                200,
            )
            .expect("create ring");

        let (handle, length) = hub
            .open("test", "/data.bin", OpenAccess::ReadWrite)
            .expect("open");
        assert_eq!(length, 4096);
        let reg_id = hub.register_file(&handle).expect("register");

        let mut allocator = IovAllocator::new(layout);
        let producer = ring.submission_producer();
        let consumer = ring.completion_consumer();

        // 写：iov 第 0 区块 → 文件偏移 0
        let write_slice = allocator.reserve(5).expect("reserve");
        iov_view
            .write_block(&write_slice, b"hello")
            .expect("write block");
        producer
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id,
                len: 5,
                offset: 0,
                iov_offset: write_slice.iov_offset,
                userdata: 11,
            })
            .expect("push write");
        doorbell.ring();

        let completion = wait_for_completion(&consumer);
        assert_eq!(completion.userdata, 11);
        assert_eq!(completion.status, 0, "write status");
        assert_eq!(completion.result, 5);
        assert_eq!(
            backend.files.lock().expect("files")[&FILE_INODE][..5],
            *b"hello"
        );
        allocator.release(&write_slice).expect("release");

        // 读：文件偏移 0 → iov 第 1 区块
        let read_slice = allocator.reserve(5).expect("reserve");
        producer
            .push(&Submission {
                op: OP_READ,
                flags: 0,
                reg_id,
                len: 5,
                offset: 0,
                iov_offset: read_slice.iov_offset,
                userdata: 12,
            })
            .expect("push read");
        doorbell.ring();

        let completion = wait_for_completion(&consumer);
        assert_eq!(completion.userdata, 12);
        assert_eq!(completion.status, 0, "read status");
        assert_eq!(completion.result, 5);
        assert_eq!(
            iov_view
                .slice(read_slice.iov_offset, 5)
                .expect("read back slice"),
            b"hello"
        );

        hub.unregister_file(reg_id).expect("unregister");
        hub.close(&handle).expect("close");
        hub.destroy_ring(ring_id).expect("destroy ring");
        hub.destroy_iov(iov_id).expect("destroy iov");
    }

    #[test]
    fn create_ring_rejects_io_depth_beyond_the_entry_count() {
        // io_depth > 0 是「必须攒满」：目标大于环项数时这批永远凑不满，环会永久停摆，
        // 创建时就拒绝，而不是留一条永远不工作的环。
        let backend = Arc::new(MemoryBackend::new(64));
        let hub = UsrbioHub::new(backend, None);
        let layout = IovLayout::new(4096, 2).expect("iov layout");
        let iov_region = TestRegion::new("usrbio-iov-depth", layout.total_bytes());
        // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
        let view =
            unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
        view.initialize().expect("init");
        let iov_id = hub
            .create_iov(iov_region.handle_fd(), layout)
            .expect("create iov");

        let ring_layout = RingLayout::new(4).expect("ring layout");
        let ring_region = TestRegion::new("usrbio-ring-depth", ring_layout.total_bytes());
        let doorbell = EventFd::create().expect("doorbell");
        let error = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                iov_id,
                8,
                200,
            )
            .expect_err("io_depth beyond entries must be rejected");
        assert_eq!(error.code(), afs_error::NODE_VFS_INVALID);

        // 边界值：io_depth == entries 恰好能攒满一批，仍然合法。
        let ring_region = TestRegion::new("usrbio-ring-depth-ok", ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
        ring.initialize().expect("ring init");
        let ring_id = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                iov_id,
                4,
                200,
            )
            .expect("io_depth equal to entries is allowed");
        hub.destroy_ring(ring_id).expect("destroy ring");
        hub.destroy_iov(iov_id).expect("destroy iov");
    }

    #[test]
    fn destroy_ring_fails_pending_submissions_with_a_completion_doorbell() {
        // 提交不敲门铃，worker 睡在门铃上；io_depth = entries 让它即使扫到这项提交也不会
        // 处理（必须攒满 4 个，只有 1 个）。destroy_ring 必须把这项提交以失败完成，并且在
        // 写入 CQE 之后再敲一次完成 eventfd——join 前那一下可能已被 SDK 读掉。
        let backend = Arc::new(MemoryBackend::new(64));
        let hub = UsrbioHub::new(backend, None);
        let layout = IovLayout::new(4096, 2).expect("iov layout");
        let iov_region = TestRegion::new("usrbio-iov-failpending", layout.total_bytes());
        // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
        let view =
            unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
        view.initialize().expect("init");
        let iov_id = hub
            .create_iov(iov_region.handle_fd(), layout)
            .expect("create iov");

        let ring_layout = RingLayout::new(4).expect("ring layout");
        let ring_region = TestRegion::new("usrbio-ring-failpending", ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
        ring.initialize().expect("init");
        let doorbell = EventFd::create().expect("doorbell");
        let completion = EventFd::create().expect("completion");
        let ring_id = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: completion.try_clone_fd().expect("dup completion"),
                },
                ring_layout,
                iov_id,
                4,
                200,
            )
            .expect("create ring");

        ring.submission_producer()
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id: 7,
                len: 4,
                offset: 0,
                iov_offset: 0,
                userdata: 42,
            })
            .expect("push pending submission");

        hub.destroy_ring(ring_id).expect("destroy ring");

        // join 返回后计数必须是 2：destroy_ring join 前一次 + fail_pending 写完 CQE 后一次。
        // 只有 1 说明 fail_pending 没敲门，SDK 只能靠超时兜底才拿得到这批失败完成项。
        let mut counter = [0_u8; 8];
        // SAFETY: counter 是本栈帧内 8 字节数组，eventfd 的 read 恰好要求 8 字节缓冲。
        let read = unsafe { libc::read(completion.raw_fd(), counter.as_mut_ptr().cast(), 8) };
        assert_eq!(read, 8, "completion counter read");
        assert_eq!(
            u64::from_ne_bytes(counter),
            2,
            "fail_pending must ring the completion doorbell after writing CQEs"
        );

        let consumer = ring.completion_consumer();
        let failed = wait_for_completion(&consumer);
        assert_eq!(failed.userdata, 42);
        assert_eq!(failed.status, afs_error::NODE_VFS_UNAVAILABLE.raw());
        assert_eq!(failed.result, -1);

        hub.destroy_iov(iov_id).expect("destroy iov");
    }

    #[test]
    fn control_plane_enforces_handle_and_ring_lifetimes() {
        let backend = Arc::new(MemoryBackend::new(64));
        let hub = UsrbioHub::new(backend, None);
        let layout = IovLayout::new(4096, 2).expect("iov layout");
        let iov_region = TestRegion::new("usrbio-iov-lifecycle", layout.total_bytes());
        // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
        let view =
            unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
        view.initialize().expect("init");
        let iov_id = hub
            .create_iov(iov_region.handle_fd(), layout)
            .expect("create iov");

        let ring_layout = RingLayout::new(4).expect("ring layout");
        let ring_region = TestRegion::new("usrbio-ring-lifecycle", ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
        ring.initialize().expect("init");
        let doorbell = EventFd::create().expect("eventfd");
        let ring_id = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                iov_id,
                0,
                100,
            )
            .expect("create ring");

        // iov 仍被环引用时不能销毁
        assert!(hub.destroy_iov(iov_id).is_err());

        let (handle, _) = hub
            .open("default", "/data.bin", OpenAccess::ReadOnly)
            .expect("open");
        let reg_id = hub.register_file(&handle).expect("register");
        // 仍被注册时不能关闭
        assert!(hub.close(&handle).is_err());
        hub.unregister_file(reg_id).expect("unregister");
        hub.close(&handle).expect("close after unregister");
        // 句柄已关闭后注册应失败
        assert!(hub.register_file(&handle).is_err());

        hub.destroy_ring(ring_id).expect("destroy ring");
        hub.destroy_iov(iov_id).expect("destroy iov");
        assert!(hub.destroy_iov(iov_id).is_err());
    }

    #[test]
    fn open_rejects_relative_and_traversing_paths() {
        let backend = Arc::new(MemoryBackend::new(16));
        let hub = UsrbioHub::new(backend, Some("test".to_owned()));
        assert!(hub.open("test", "data.bin", OpenAccess::ReadOnly).is_err());
        assert!(
            hub.open("test", "/../data.bin", OpenAccess::ReadOnly)
                .is_err()
        );
        assert!(
            hub.open("test", "/missing.bin", OpenAccess::ReadOnly)
                .is_err()
        );
        assert!(
            hub.open("other", "/data.bin", OpenAccess::ReadOnly)
                .is_err()
        );
        assert!(hub.open("test", "/", OpenAccess::ReadOnly).is_err());
    }

    #[test]
    fn unknown_registration_produces_failed_completion() {
        let backend = Arc::new(MemoryBackend::new(16));
        let hub = UsrbioHub::new(backend, None);
        let layout = IovLayout::new(4096, 2).expect("iov layout");
        let iov_region = TestRegion::new("usrbio-iov-unknown", layout.total_bytes());
        // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
        let view =
            unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
        view.initialize().expect("init");
        let iov_id = hub
            .create_iov(iov_region.handle_fd(), layout)
            .expect("create iov");

        let ring_layout = RingLayout::new(4).expect("ring layout");
        let ring_region = TestRegion::new("usrbio-ring-unknown", ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
        ring.initialize().expect("init");
        let doorbell = EventFd::create().expect("eventfd");
        let ring_id = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                iov_id,
                0,
                50,
            )
            .expect("create ring");

        let producer = ring.submission_producer();
        let consumer = ring.completion_consumer();
        producer
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id: 999,
                len: 4,
                offset: 0,
                iov_offset: layout.block_offset(0) as u64,
                userdata: 3,
            })
            .expect("push");
        doorbell.ring();

        let completion = wait_for_completion(&consumer);
        assert_eq!(completion.userdata, 3);
        assert_eq!(completion.status, afs_error::NODE_VFS_INVALID.raw());
        assert_eq!(completion.result, -1);

        hub.destroy_ring(ring_id).expect("destroy ring");
        hub.destroy_iov(iov_id).expect("destroy iov");
    }

    /// 一次非法请求不能拖垮整条环：节点回失败完成项，后续合法请求照常工作。
    #[test]
    fn invalid_iov_span_fails_the_completion_but_keeps_the_ring_usable() {
        let backend = Arc::new(MemoryBackend::new(16));
        let hub = UsrbioHub::new(backend, None);
        let layout = IovLayout::new(4096, 2).expect("iov layout");
        let iov_region = TestRegion::new("usrbio-iov-span", layout.total_bytes());
        // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
        let view =
            unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
        view.initialize().expect("init");
        let iov_id = hub
            .create_iov(iov_region.handle_fd(), layout)
            .expect("create iov");
        let ring_layout = RingLayout::new(4).expect("ring layout");
        let ring_region = TestRegion::new("usrbio-ring-span", ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
        ring.initialize().expect("init");
        let doorbell = EventFd::create().expect("eventfd");
        let ring_id = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                iov_id,
                0,
                50,
            )
            .expect("create ring");
        let (handle, _) = hub
            .open("test", "/data.bin", OpenAccess::ReadWrite)
            .expect("open");
        let reg_id = hub.register_file(&handle).expect("register");

        let producer = ring.submission_producer();
        let consumer = ring.completion_consumer();
        // 跨度越过块尾：block 0 只有 4096 字节，这里要 4097。
        producer
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id,
                len: 4097,
                offset: 0,
                iov_offset: layout.block_offset(0) as u64,
                userdata: 11,
            })
            .expect("push");
        doorbell.ring();
        let completion = wait_for_completion(&consumer);
        assert_eq!(completion.userdata, 11, "失败完成项要带上原始 userdata");
        assert_ne!(completion.status, 0, "越界跨度必须报错");
        assert_eq!(completion.result, -1);

        // 环没被拖垮：紧接着一条合法写入必须成功。
        producer
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id,
                len: 4,
                offset: 0,
                iov_offset: layout.block_offset(1) as u64,
                userdata: 12,
            })
            .expect("push");
        doorbell.ring();
        let completion = wait_for_completion(&consumer);
        assert_eq!(completion.userdata, 12);
        assert_eq!(completion.status, 0, "坏请求之后环仍应可用");
        assert_eq!(completion.result, 4);

        hub.destroy_ring(ring_id).expect("destroy ring");
        hub.destroy_iov(iov_id).expect("destroy iov");
    }

    /// 带着未消费的提交销毁环：必须能收尾，不能挂死，也不能影响后续建环。
    #[test]
    fn destroying_a_ring_with_pending_submissions_completes() {
        let backend = Arc::new(MemoryBackend::new(16));
        let hub = UsrbioHub::new(backend, None);
        let layout = IovLayout::new(4096, 2).expect("iov layout");
        let iov_region = TestRegion::new("usrbio-iov-destroy", layout.total_bytes());
        // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
        let view =
            unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
        view.initialize().expect("init");
        let iov_id = hub
            .create_iov(iov_region.handle_fd(), layout)
            .expect("create iov");
        let ring_layout = RingLayout::new(8).expect("ring layout");
        let ring_region = TestRegion::new("usrbio-ring-destroy", ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
        ring.initialize().expect("init");
        let doorbell = EventFd::create().expect("eventfd");
        let ring_id = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                iov_id,
                0,
                50,
            )
            .expect("create ring");
        let (handle, _) = hub
            .open("test", "/data.bin", OpenAccess::ReadWrite)
            .expect("open");
        let reg_id = hub.register_file(&handle).expect("register");

        // 堆满提交但一条都不等，直接销毁。
        let producer = ring.submission_producer();
        for userdata in 0..8_u64 {
            producer
                .push(&Submission {
                    op: OP_WRITE,
                    flags: 0,
                    reg_id,
                    len: 4,
                    offset: 0,
                    iov_offset: layout.block_offset(0) as u64,
                    userdata,
                })
                .expect("push");
        }
        doorbell.ring();
        hub.destroy_ring(ring_id)
            .expect("destroy ring with pending work");

        // hub 还能继续工作：销毁不能留下坏状态。
        let second_layout = IovLayout::new(4096, 2).expect("iov layout");
        let second_region = TestRegion::new("usrbio-iov-after", second_layout.total_bytes());
        // SAFETY: mapping 覆盖 second_layout.total_bytes() 字节。
        let second_view =
            unsafe { SharedIov::from_raw_parts(second_region.mapping.as_ptr(), second_layout) }
                .expect("iov");
        second_view.initialize().expect("init");
        let second_iov = hub
            .create_iov(second_region.handle_fd(), second_layout)
            .expect("create iov");
        let second_ring_layout = RingLayout::new(4).expect("ring layout");
        let second_ring_region =
            TestRegion::new("usrbio-ring-after", second_ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 second_ring_layout.total_bytes() 字节。
        let second_ring = unsafe {
            SharedRing::from_raw_parts(second_ring_region.mapping.as_ptr(), second_ring_layout)
        }
        .expect("ring");
        second_ring.initialize().expect("init");
        let second_doorbell = EventFd::create().expect("eventfd");
        let second_id = hub
            .create_ring(
                RingFds {
                    region: second_ring_region.handle_fd(),
                    doorbell: second_doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                second_ring_layout,
                second_iov,
                0,
                50,
            )
            .expect("create second ring");

        let producer = second_ring.submission_producer();
        let consumer = second_ring.completion_consumer();
        producer
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id,
                len: 4,
                offset: 0,
                iov_offset: second_layout.block_offset(0) as u64,
                userdata: 21,
            })
            .expect("push");
        second_doorbell.ring();
        let completion = wait_for_completion(&consumer);
        assert_eq!(completion.userdata, 21);
        assert_eq!(completion.status, 0);

        hub.destroy_ring(second_id).expect("destroy second ring");
        hub.destroy_iov(iov_id).expect("destroy iov");
        hub.destroy_iov(second_iov).expect("destroy second iov");
    }

    /// 用真实 gRPC 服务 + 真实 SDK 客户端跑一遍 usrbio 往返，后端用内存实现。
    #[tokio::test]
    async fn local_sdk_round_trips_through_iov_and_ring() {
        use crate::node::api::local::{LocalApiOptions, serve_local_api_with_options};
        use crate::node::storage::Storage;
        use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig};

        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("afs-node.sock");
        let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
        let backend = Arc::new(MemoryBackend::new(4096));
        let server = serve_local_api_with_options(
            storage,
            &socket_path,
            LocalApiOptions {
                metrics_registry: None,
                dfs_backend: Some(backend.clone() as Arc<dyn Backend>),
                dfs_namespace: Some("default".to_owned()),
            },
        )
        .await
        .expect("serve local api");

        let client = DfsLocalClient::connect(DfsLocalClientConfig::new(socket_path))
            .await
            .expect("connect");
        let file = client
            .open("/data.bin", DfsAccess::ReadWrite)
            .await
            .expect("open");
        assert_eq!(file.length, 4096);

        let mut iov = client.create_iov(4096, 4).await.expect("create iov");
        let mut ring = client
            .create_ring(&iov, 8, 0, 500)
            .await
            .expect("create ring");
        let registration = client.register_file(&file).await.expect("register");

        let written = ring
            .write_from(&mut iov, &registration, 0, b"usrbio!")
            .await
            .expect("write through ring");
        assert_eq!(written, 7);
        assert_eq!(
            backend.files.lock().expect("files")[&FILE_INODE][..7],
            *b"usrbio!"
        );

        let mut out = vec![0_u8; 7];
        let read = ring
            .read_into(&mut iov, &registration, 0, &mut out)
            .await
            .expect("read through ring");
        assert_eq!(read, 7);
        assert_eq!(out, b"usrbio!");

        // 超过一个区块的请求必须被拒绝，不能由实现偷偷跨块拼接。
        let oversized = vec![0_u8; 8192];
        assert!(
            ring.write_from(&mut iov, &registration, 0, &oversized)
                .await
                .is_err(),
            "cross-block write must be rejected"
        );
        assert_eq!(ring.pending(), 0);
        assert_eq!(iov.in_flight(), 0);

        client
            .unregister_file(registration)
            .await
            .expect("unregister");
        client.close(file.handle).await.expect("close");
        ring.destroy().await.expect("destroy ring");
        iov.destroy().await.expect("destroy iov");
        server.shutdown().await.expect("shutdown");
    }

    /// 未装配 DFS 后端时必须报 UNIMPLEMENTED，绝不退回 diagnostics 目录。
    #[tokio::test]
    async fn local_sdk_without_dfs_backend_is_unimplemented() {
        use crate::node::api::local::serve_local_api;
        use crate::node::storage::Storage;
        use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig};

        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("afs-node.sock");
        let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
        let server = serve_local_api(storage, &socket_path)
            .await
            .expect("serve local api");
        let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
            .await
            .expect("connect");
        let error = client
            .open("/data.bin", DfsAccess::ReadOnly)
            .await
            .expect_err("open must fail without a DFS backend");
        assert_eq!(error.code(), afs_error::NODE_VFS_UNIMPLEMENTED);
        server.shutdown().await.expect("shutdown");
    }

    /// 两种同步屏障必须原样到达后端：不能把 DataOnly 悄悄升级或降级。
    #[tokio::test]
    async fn sync_modes_reach_the_backend_as_requested() {
        use crate::node::api::local::{LocalApiOptions, serve_local_api_with_options};
        use crate::node::storage::Storage;
        use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig, DfsSyncMode};

        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("afs-node.sock");
        let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
        let backend = Arc::new(MemoryBackend::new(4096));
        let server = serve_local_api_with_options(
            storage,
            &socket_path,
            LocalApiOptions {
                metrics_registry: None,
                dfs_backend: Some(backend.clone() as Arc<dyn Backend>),
                dfs_namespace: Some("default".to_owned()),
            },
        )
        .await
        .expect("serve local api");
        let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
            .await
            .expect("connect");

        let file = client
            .open("/data.bin", DfsAccess::ReadWrite)
            .await
            .expect("open");
        client
            .sync(&file.handle, DfsSyncMode::Data)
            .await
            .expect("sync data");
        assert!(
            matches!(
                *backend.last_sync.lock().expect("last sync"),
                Some(types::SyncMode::DataOnly)
            ),
            "DataOnly must not be widened to Full"
        );
        client
            .sync(&file.handle, DfsSyncMode::Full)
            .await
            .expect("sync full");
        assert!(
            matches!(
                *backend.last_sync.lock().expect("last sync"),
                Some(types::SyncMode::Full)
            ),
            "Full must reach the backend as-is"
        );

        client.close(file.handle.clone()).await.expect("close");
        // 已关闭的句柄不能再同步；必须显式报错，不能当作成功。
        assert!(
            client.sync(&file.handle, DfsSyncMode::Data).await.is_err(),
            "sync on a closed handle must fail"
        );
        server.shutdown().await.expect("shutdown");
    }

    /// 一次门铃必须能把环里已经排好的多项都处理掉；否则批量提交就没有意义。
    #[test]
    fn single_doorbell_completes_every_queued_submission() {
        let backend = Arc::new(MemoryBackend::new(4096));
        let hub = UsrbioHub::new(backend.clone(), None);
        let layout = IovLayout::new(4096, 8).expect("iov layout");
        let iov_region = TestRegion::new("usrbio-iov-batch", layout.total_bytes());
        // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
        let view =
            unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
        view.initialize().expect("init");
        let iov_id = hub
            .create_iov(iov_region.handle_fd(), layout)
            .expect("create iov");

        let ring_layout = RingLayout::new(8).expect("ring layout");
        let ring_region = TestRegion::new("usrbio-ring-batch", ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
        ring.initialize().expect("init");
        let doorbell = EventFd::create().expect("eventfd");
        let ring_id = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                iov_id,
                0,
                100,
            )
            .expect("create ring");

        let (handle, _) = hub
            .open("default", "/data.bin", OpenAccess::ReadWrite)
            .expect("open");
        let reg_id = hub.register_file(&handle).expect("register");

        let mut allocator = IovAllocator::new(layout);
        let producer = ring.submission_producer();
        let consumer = ring.completion_consumer();

        // 排 4 项，中间一次门铃都不敲。
        for index in 0..4_u64 {
            let slice = allocator.reserve(4).expect("reserve");
            view.write_block(&slice, b"abc\n").expect("write block");
            producer
                .push(&Submission {
                    op: OP_WRITE,
                    flags: 0,
                    reg_id,
                    len: 4,
                    offset: index * 4,
                    iov_offset: slice.iov_offset,
                    userdata: 100 + index,
                })
                .expect("push");
        }
        doorbell.ring();

        let mut seen = Vec::new();
        for _ in 0..4 {
            let completion = wait_for_completion(&consumer);
            assert_eq!(completion.status, 0, "batch write status");
            assert_eq!(completion.result, 4);
            seen.push(completion.userdata);
        }
        seen.sort_unstable();
        assert_eq!(seen, vec![100, 101, 102, 103]);
        assert_eq!(
            backend.files.lock().expect("files")[&FILE_INODE][..8],
            *b"abc\nabc\n"
        );

        let stats = hub.ring_stats(ring_id).expect("ring stats");
        assert_eq!(stats.submitted, 4, "四项都必须被执行过");
        assert_eq!(stats.completed, 4);
        assert_eq!(stats.failed, 0);
        assert_eq!(stats.bytes, 16, "4 × 4 字节");
        assert!(stats.wakeups >= 1, "至少被门铃唤醒过一次");

        hub.unregister_file(reg_id).expect("unregister");
        hub.close(&handle).expect("close");
        hub.destroy_ring(ring_id).expect("destroy ring");
        // 环解绑后计数随之消失，不再对外可见。
        assert!(hub.ring_stats(ring_id).is_none());
        hub.destroy_iov(iov_id).expect("destroy iov");
    }

    /// 计数必须真的进入 registry：口径与命名是运维实际会看到的接口，不能只在内部自洽。
    #[test]
    fn ring_counters_are_exported_to_the_registry() {
        let registry = Registry::new();
        let io = IntCounterVec::new(Opts::new(RingMetrics::NAME, "test"), &["event"])
            .expect("counter vec");
        registry.register(Box::new(io.clone())).expect("register");
        let metrics = RingMetrics::from_vec(io.clone());

        let backend = Arc::new(MemoryBackend::new(1024));
        let hub = UsrbioHub::new(backend, None).with_metrics(metrics);
        let layout = IovLayout::new(4096, 4).expect("iov layout");
        let iov_region = TestRegion::new("usrbio-iov-metrics", layout.total_bytes());
        // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
        let view =
            unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }.expect("iov");
        view.initialize().expect("init");
        let iov_id = hub
            .create_iov(iov_region.handle_fd(), layout)
            .expect("create iov");

        let ring_layout = RingLayout::new(4).expect("ring layout");
        let ring_region = TestRegion::new("usrbio-ring-metrics", ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let ring = unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
            .expect("ring");
        ring.initialize().expect("init");
        let doorbell = EventFd::create().expect("eventfd");
        let ring_id = hub
            .create_ring(
                RingFds {
                    region: ring_region.handle_fd(),
                    doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                iov_id,
                0,
                100,
            )
            .expect("create ring");

        let (handle, _) = hub
            .open("default", "/data.bin", OpenAccess::ReadWrite)
            .expect("open");
        let reg_id = hub.register_file(&handle).expect("register");

        let mut allocator = IovAllocator::new(layout);
        let producer = ring.submission_producer();
        let consumer = ring.completion_consumer();
        for index in 0..2_u64 {
            let slice = allocator.reserve(2).expect("reserve");
            view.write_block(&slice, b"hi").expect("write block");
            producer
                .push(&Submission {
                    op: OP_WRITE,
                    flags: 0,
                    reg_id,
                    len: 2,
                    offset: index * 2,
                    iov_offset: slice.iov_offset,
                    userdata: 200 + index,
                })
                .expect("push");
        }
        doorbell.ring();
        for _ in 0..2 {
            assert_eq!(wait_for_completion(&consumer).status, 0);
        }

        assert_eq!(io.with_label_values(&["submitted"]).get(), 2);
        assert_eq!(io.with_label_values(&["completed"]).get(), 2);
        assert_eq!(io.with_label_values(&["failed"]).get(), 0);
        assert_eq!(io.with_label_values(&["bytes"]).get(), 4);
        assert!(io.with_label_values(&["wakeups"]).get() >= 1);
        // 指标名就是运维看到的接口名，固定住它。
        assert_eq!(RingMetrics::NAME, "afs_usrbio_ring_io_total");

        hub.unregister_file(reg_id).expect("unregister");
        hub.close(&handle).expect("close");
        hub.destroy_ring(ring_id).expect("destroy ring");
        hub.destroy_iov(iov_id).expect("destroy iov");
    }

    /// 一条环 + 它的 iov 与门铃，供批量语义的用例复用。
    struct RingFixture {
        hub: UsrbioHub,
        backend: Arc<MemoryBackend>,
        layout: IovLayout,
        view: SharedIov,
        ring: SharedRing,
        doorbell: EventFd,
        iov_id: u64,
        ring_id: u64,
        _iov_region: TestRegion,
        _ring_region: TestRegion,
    }

    impl RingFixture {
        fn new(name: &str, io_depth: i32, timeout_ms: u32, blocks: u32) -> Self {
            let backend = Arc::new(MemoryBackend::new(16 * 1024));
            let hub = UsrbioHub::new(backend.clone(), None);
            let layout = IovLayout::new(4096, blocks).expect("iov layout");
            let iov_region = TestRegion::new(&format!("{name}-iov"), layout.total_bytes());
            // SAFETY: mapping 覆盖 layout.total_bytes() 字节。
            let view = unsafe { SharedIov::from_raw_parts(iov_region.mapping.as_ptr(), layout) }
                .expect("iov");
            view.initialize().expect("iov init");
            let iov_id = hub
                .create_iov(iov_region.handle_fd(), layout)
                .expect("create iov");

            let ring_layout = RingLayout::new(8).expect("ring layout");
            let ring_region = TestRegion::new(&format!("{name}-ring"), ring_layout.total_bytes());
            // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
            let ring =
                unsafe { SharedRing::from_raw_parts(ring_region.mapping.as_ptr(), ring_layout) }
                    .expect("ring");
            ring.initialize().expect("ring init");
            let doorbell = EventFd::create().expect("eventfd");
            let ring_id = hub
                .create_ring(
                    RingFds {
                        region: ring_region.handle_fd(),
                        doorbell: doorbell.try_clone_fd().expect("dup doorbell"),
                        completion: test_completion_fd(),
                    },
                    ring_layout,
                    iov_id,
                    io_depth,
                    timeout_ms,
                )
                .expect("create ring");
            Self {
                hub,
                backend,
                layout,
                view,
                ring,
                doorbell,
                iov_id,
                ring_id,
                _iov_region: iov_region,
                _ring_region: ring_region,
            }
        }

        /// 排队一项写；返回分到的 userdata。
        fn queue_write(
            &self,
            allocator: &mut IovAllocator,
            reg_id: u32,
            offset: u64,
            payload: &[u8],
            userdata: u64,
        ) {
            let slice = allocator.reserve(payload.len()).expect("reserve");
            self.view.write_block(&slice, payload).expect("write block");
            self.ring
                .submission_producer()
                .push(&Submission {
                    op: OP_WRITE,
                    flags: 0,
                    reg_id,
                    len: payload.len() as u32,
                    offset,
                    iov_offset: slice.iov_offset,
                    userdata,
                })
                .expect("push");
        }
    }

    /// `io_depth > 0`：必须等满才处理，不能拿半批就走。
    #[test]
    fn positive_io_depth_waits_for_the_full_batch() {
        let fixture = RingFixture::new("usrbio-depth-positive", 3, 0, 8);
        let (handle, _) = fixture
            .hub
            .open("default", "/data.bin", OpenAccess::ReadWrite)
            .expect("open");
        let reg_id = fixture.hub.register_file(&handle).expect("register");
        let mut allocator = IovAllocator::new(fixture.layout);
        let consumer = fixture.ring.completion_consumer();

        fixture.queue_write(&mut allocator, reg_id, 0, b"aaa", 1);
        fixture.queue_write(&mut allocator, reg_id, 3, b"bbb", 2);
        fixture.doorbell.ring();
        // 只有 2 个，未满 3 个：worker 必须继续等，不能提前执行。
        thread::sleep(Duration::from_millis(200));
        assert_eq!(consumer.ready(), 0, "未满批不得产生完成项");

        fixture.queue_write(&mut allocator, reg_id, 6, b"ccc", 3);
        fixture.doorbell.ring();
        let mut seen = Vec::new();
        for _ in 0..3 {
            let completion = wait_for_completion(&consumer);
            assert_eq!(completion.status, 0, "batch status");
            seen.push(completion.userdata);
        }
        seen.sort_unstable();
        assert_eq!(seen, vec![1, 2, 3]);
        assert_eq!(
            &fixture.backend.files.lock().expect("files")[&FILE_INODE][..9],
            b"aaabbbccc"
        );

        let stats = fixture.hub.ring_stats(fixture.ring_id).expect("stats");
        assert_eq!(stats.submitted, 3);
        assert_eq!(stats.completed, 3);

        fixture.hub.unregister_file(reg_id).expect("unregister");
        fixture.hub.close(&handle).expect("close");
    }

    /// `io_depth < 0`：等满但受 `timeout_ms` 约束，到点必须放行半批。
    #[test]
    fn negative_io_depth_releases_a_partial_batch_on_timeout() {
        let fixture = RingFixture::new("usrbio-depth-negative", -4, 100, 8);
        let (handle, _) = fixture
            .hub
            .open("default", "/data.bin", OpenAccess::ReadWrite)
            .expect("open");
        let reg_id = fixture.hub.register_file(&handle).expect("register");
        let mut allocator = IovAllocator::new(fixture.layout);
        let consumer = fixture.ring.completion_consumer();

        fixture.queue_write(&mut allocator, reg_id, 0, b"xy", 11);
        fixture.queue_write(&mut allocator, reg_id, 2, b"zw", 12);
        fixture.doorbell.ring();

        // 2 < 4，但上界会到期，因此必须拿到这 2 项而不是一直等。
        let mut seen = Vec::new();
        for _ in 0..2 {
            let completion = wait_for_completion(&consumer);
            assert_eq!(completion.status, 0, "timeout batch status");
            seen.push(completion.userdata);
        }
        seen.sort_unstable();
        assert_eq!(seen, vec![11, 12]);
        assert!(consumer.ready() == 0, "不应多出完成项");

        fixture.hub.unregister_file(reg_id).expect("unregister");
        fixture.hub.close(&handle).expect("close");
    }

    /// 多条环可以共享同一块 iov，各自的完成项与计数互不干扰。
    #[test]
    fn two_rings_share_one_iov_without_cross_talk() {
        let fixture = RingFixture::new("usrbio-multi-ring", 0, 100, 8);
        let (handle, _) = fixture
            .hub
            .open("default", "/data.bin", OpenAccess::ReadWrite)
            .expect("open");
        let reg_id = fixture.hub.register_file(&handle).expect("register");

        // 第二条环挂在同一个 iov 上。
        let ring_layout = RingLayout::new(4).expect("ring layout");
        let second_region = TestRegion::new("usrbio-multi-ring-2", ring_layout.total_bytes());
        // SAFETY: mapping 覆盖 ring_layout.total_bytes() 字节。
        let second_ring =
            unsafe { SharedRing::from_raw_parts(second_region.mapping.as_ptr(), ring_layout) }
                .expect("ring");
        second_ring.initialize().expect("init");
        let second_doorbell = EventFd::create().expect("eventfd");
        let second_id = fixture
            .hub
            .create_ring(
                RingFds {
                    region: second_region.handle_fd(),
                    doorbell: second_doorbell.try_clone_fd().expect("dup doorbell"),
                    completion: test_completion_fd(),
                },
                ring_layout,
                fixture.iov_id,
                0,
                100,
            )
            .expect("create second ring");

        let mut allocator = IovAllocator::new(fixture.layout);
        fixture.queue_write(&mut allocator, reg_id, 0, b"r1", 21);
        let second_producer = second_ring.submission_producer();
        let slice = allocator.reserve(2).expect("reserve");
        fixture
            .view
            .write_block(&slice, b"r2")
            .expect("write block");
        second_producer
            .push(&Submission {
                op: OP_WRITE,
                flags: 0,
                reg_id,
                len: 2,
                offset: 2,
                iov_offset: slice.iov_offset,
                userdata: 22,
            })
            .expect("push");

        fixture.doorbell.ring();
        second_doorbell.ring();

        let first = wait_for_completion(&fixture.ring.completion_consumer());
        assert_eq!(first.userdata, 21);
        let second = wait_for_completion(&second_ring.completion_consumer());
        assert_eq!(second.userdata, 22);
        assert_eq!(
            &fixture.backend.files.lock().expect("files")[&FILE_INODE][..4],
            b"r1r2"
        );

        // 计数按环隔离：每条环各自 1 项。
        assert_eq!(
            fixture
                .hub
                .ring_stats(fixture.ring_id)
                .expect("stats")
                .submitted,
            1
        );
        assert_eq!(
            fixture.hub.ring_stats(second_id).expect("stats").submitted,
            1
        );

        fixture.hub.unregister_file(reg_id).expect("unregister");
        fixture.hub.close(&handle).expect("close");
        fixture.hub.destroy_ring(fixture.ring_id).expect("destroy");
        fixture.hub.destroy_ring(second_id).expect("destroy");
    }

    /// 批量入口：一次门铃写多项、再批量读回，区块必须全部归还。
    #[tokio::test]
    async fn local_sdk_batches_writes_and_reads() {
        use crate::node::api::local::{LocalApiOptions, serve_local_api_with_options};
        use crate::node::storage::Storage;
        use afs_client::{DfsAccess, DfsLocalClient, DfsLocalClientConfig};

        let temp = tempfile::tempdir().expect("tempdir");
        let socket_path = temp.path().join("afs-node.sock");
        let storage = Storage::new(temp.path().join("diagnostics")).expect("storage");
        let backend = Arc::new(MemoryBackend::new(4096));
        let server = serve_local_api_with_options(
            storage,
            &socket_path,
            LocalApiOptions {
                metrics_registry: None,
                dfs_backend: Some(backend.clone() as Arc<dyn Backend>),
                dfs_namespace: Some("default".to_owned()),
            },
        )
        .await
        .expect("serve local api");

        let client = DfsLocalClient::connect(DfsLocalClientConfig::new(&socket_path))
            .await
            .expect("connect");
        let file = client
            .open("/data.bin", DfsAccess::ReadWrite)
            .await
            .expect("open");
        let mut iov = client.create_iov(4096, 4).await.expect("create iov");
        let mut ring = client
            .create_ring(&iov, 8, 0, 500)
            .await
            .expect("create ring");
        let registration = client.register_file(&file).await.expect("register");

        let items: [(u64, &[u8]); 3] = [(0, b"first"), (64, b"second"), (128, b"third")];
        let written = ring
            .write_batch(&mut iov, &registration, &items)
            .await
            .expect("write batch");
        assert_eq!(written, vec![5, 6, 5]);
        // 三项各写各的区间；中间的空隙必须没被动过，否则说明某项越界覆盖了邻居。
        {
            let files = backend.files.lock().expect("files");
            let content = &files[&FILE_INODE];
            assert_eq!(&content[..5], b"first");
            assert_eq!(&content[64..70], b"second");
            assert_eq!(&content[128..133], b"third");
            assert!(
                content[5..64].iter().all(|byte| *byte == 0),
                "填充区被越界写入"
            );
            assert!(
                content[70..128].iter().all(|byte| *byte == 0),
                "填充区被越界写入"
            );
        }

        let mut first = [0_u8; 5];
        let mut second = [0_u8; 6];
        let mut third = [0_u8; 5];
        let mut reads: [(u64, &mut [u8]); 3] =
            [(0, &mut first), (64, &mut second), (128, &mut third)];
        let read = ring
            .read_batch(&mut iov, &registration, &mut reads)
            .await
            .expect("read batch");
        assert_eq!(read, vec![5, 6, 5]);
        assert_eq!(&first, b"first");
        assert_eq!(&second, b"second");
        assert_eq!(&third, b"third");

        // 全部结算后不应有任何区块还挂在在途状态。
        assert_eq!(iov.in_flight(), 0, "批量完成后区块必须归还");
        assert_eq!(iov.free_blocks(), 4, "所有区块应重新可用");

        client
            .unregister_file(registration)
            .await
            .expect("unregister");
        client.close(file.handle).await.expect("close");
        ring.destroy().await.expect("destroy ring");
        iov.destroy().await.expect("destroy iov");
        server.shutdown().await.expect("shutdown");
    }
}
