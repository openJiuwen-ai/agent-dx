//! 统一 POSIX/FUSE 入口。
//!
//! 接收内核回调，转换到 VFS `Backend` 接口并映射 errno/回复；管理 FUSE 会话内的
//! inode、目录项和打开句柄。每个 mount session 绑定一个 Backend；FUSE inode 编号
//! 不能当作后端持久文件身份。OwnerFs 的独用 Home 根可短暂缓存；首次远端
//! 访问先通过 FUSE notifier 失效本机缓存，再由 Home 执行该请求。

mod state;

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet, VecDeque},
    ffi::OsStr,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use afs_error::{Error, Result};
use fuser::{
    AccessFlags, BackgroundSession, Config, Errno, FileAttr, FileHandle as FuseFileHandle,
    FileType, Filesystem, FopenFlags, Generation, INodeNo, InitFlags, KernelConfig, LockOwner,
    MountOption, OpenFlags, RenameFlags as FuseRenameFlags, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen, ReplyStatfs, ReplyWrite,
    ReplyXattr, Request, SessionACL, TimeOrNow, WriteFlags,
};

#[cfg(feature = "ownerfs")]
use crate::node::vfs::ownerfs::OwnerFs;
use crate::node::vfs::{
    Backend,
    types::{
        AttributeChange, BackendInode, DirectoryEntry, Entry, FileAttributes, FileKind,
        FileLockOwner, FilesystemCapacity, OpenOptions, ReleaseKind, RenameFlags, RequestContext,
        SetAttrOptions, SpecialFileKind, SyncMode, WriteOptions, is_legacy_privilege_clear,
    },
};

use self::state::{FuseNode, FuseState, ROOT_INO};

const TTL: Duration = Duration::ZERO;
const DIRECT_IO: FopenFlags = FopenFlags::FOPEN_DIRECT_IO;
const MAX_INLINE_LOCAL_READ_SCRATCH: usize = 1024 * 1024;

fn inline_local_read_data(
    scratch: &mut Vec<u8>,
    size: usize,
    read: impl FnOnce(&mut [u8]) -> std::result::Result<usize, i32>,
) -> std::result::Result<Cow<'_, [u8]>, i32> {
    if size > MAX_INLINE_LOCAL_READ_SCRATCH {
        let mut out = vec![0; size];
        let n = read(&mut out)?;
        out.truncate(n);
        return Ok(Cow::Owned(out));
    }
    if scratch.len() < size {
        // Avoid amortized Vec growth retaining more than the scratch limit.
        scratch.reserve_exact(size - scratch.len());
        scratch.resize(size, 0);
    }
    let n = read(&mut scratch[..size])?;
    Ok(Cow::Borrowed(&scratch[..n.min(size)]))
}

/// Implemented first-party FUSE callback entries, including failed requests.
/// This is neither kernel wire opcode accounting nor application syscall accounting.
#[derive(Clone)]
pub struct FuseRequestMetrics {
    requests: afs_metrics::IntCounterVec,
}

impl FuseRequestMetrics {
    pub fn register(
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        registry.get_or_register(|registry| {
            let requests = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_fuse_callbacks_total", "Implemented FUSE callback entries received by filesystem and operation, including failures."),
                &["filesystem", "operation"],
            )?;
            afs_metrics::register_collector(registry, &requests)?;
            for filesystem in ["ownerfs", "dfs"] {
                for operation in FUSE_CALLBACKS {
                    requests.with_label_values(&[filesystem, operation]);
                }
            }
            Ok(Self { requests })
        })
    }

    fn record(&self, filesystem: &'static str, operation: &'static str) {
        self.requests
            .with_label_values(&[filesystem, operation])
            .inc();
    }
}

const FUSE_CALLBACKS: &[&str] = &[
    "init",
    "destroy",
    "lookup",
    "forget",
    "getattr",
    "statfs",
    "setattr",
    "readlink",
    "mkdir",
    "unlink",
    "rmdir",
    "symlink",
    "rename",
    "link",
    "open",
    "read",
    "write",
    "flush",
    "release",
    "fsync",
    "opendir",
    "readdir",
    "releasedir",
    "fsyncdir",
    "create",
    "getlk",
    "setlk",
    "mknod",
    "access",
    "getxattr",
    "listxattr",
    "setxattr",
    "removexattr",
];

static NEXT_INGRESS_SESSION: AtomicU64 = AtomicU64::new(1);

/// Process-owned mount. A clean Node stop must use normal unmount and observe
/// callback cleanup through join; upstream Drop is not a closure proof.
#[derive(Debug)]
pub struct MountedFuse {
    session: BackgroundSession,
    mount_path: PathBuf,
    mount_id: u64,
    cleanup_error: Arc<Mutex<Option<Error>>>,
}

impl MountedFuse {
    fn new(
        session: BackgroundSession,
        path: &Path,
        cleanup_error: Arc<Mutex<Option<Error>>>,
    ) -> Result<Self> {
        match current_mount_id(path).and_then(|id| {
            id.ok_or_else(|| Error::from(io::Error::other("new FUSE mount identity is missing")))
        }) {
            Ok(mount_id) => Ok(Self {
                session,
                mount_path: path.to_path_buf(),
                mount_id,
                cleanup_error,
            }),
            Err(error) => {
                // Never let upstream Drop detach an unverified live mount.
                std::mem::forget(session);
                Err(error)
            }
        }
    }

    pub fn join(self) -> Result<()> {
        let unmounted = (|| {
            if let Some(id) = current_mount_id(&self.mount_path)? {
                if id != self.mount_id {
                    return Err(Error::from(io::Error::other(
                        "FUSE mount identity changed; refusing to unmount replacement",
                    )));
                }
                // The upstream EPERM fallback can use lazy unmount. Require
                // normal fusermount semantics for root and non-root alike.
                let output = std::process::Command::new("fusermount3")
                    .arg("-u")
                    .arg("--")
                    .arg(&self.mount_path)
                    .output()
                    .map_err(Error::from)?;
                if !output.status.success() {
                    return Err(Error::from(io::Error::other(format!(
                        "normal FUSE unmount failed: {}",
                        String::from_utf8_lossy(&output.stderr)
                    ))));
                }
                if current_mount_id(&self.mount_path)?.is_some() {
                    return Err(Error::from(io::Error::other(
                        "FUSE mount remains after normal unmount",
                    )));
                }
            }
            Ok(())
        })();
        if let Err(error) = unmounted {
            // Node must retain the backend and wait for its shutdown watchdog.
            // Dropping this session here could lazily detach the busy mount.
            std::mem::forget(self.session);
            return Err(error);
        }
        let joined =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || self.session.join()));
        let cleanup_error = self
            .cleanup_error
            .lock()
            .map_err(|_| Error::from(std::io::Error::other("FUSE cleanup state is poisoned")))?
            .take();
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        joined
            .map_err(|_| Error::from(std::io::Error::other("FUSE session cleanup thread failed")))?
            .map_err(Error::from)
    }
}

/// 建立真正的内核 FUSE 挂载。一个 session 只绑定一个业务 Backend。
pub fn mount_dfs(backend: Arc<dyn Backend>, path: &Path) -> Result<MountedFuse> {
    mount_dfs_inner(backend, path, None)
}

pub fn mount_dfs_with_metrics(
    backend: Arc<dyn Backend>,
    path: &Path,
    metrics: FuseRequestMetrics,
) -> Result<MountedFuse> {
    mount_dfs_inner(backend, path, Some(metrics))
}

fn mount_dfs_inner(
    backend: Arc<dyn Backend>,
    path: &Path,
    metrics: Option<FuseRequestMetrics>,
) -> Result<MountedFuse> {
    reject_existing_mount(path)?;
    let mut fs = AfsFuse::new(backend);
    fs.requests = metrics.map(|metrics| (metrics, "dfs"));
    // Cached write-through sends each syscall write to the inode owner and
    // keeps this mount's pages coherent. Do not enable writeback or KEEP_CACHE:
    // ordinary opens must refresh after another mount's close barrier.
    fs.cached_io = true;
    let cleanup_error = fs.cleanup_error.clone();
    let session =
        fuser::spawn_mount(fs, path, &mount_config("afs-dfs", true)).map_err(Error::from)?;
    MountedFuse::new(session, path, cleanup_error)
}

/// OwnerFs 使用相同 FUSE 实现，并额外接入其本地 Home 缓存策略与 notifier。
#[cfg(feature = "ownerfs")]
pub fn mount_ownerfs(ownerfs: Arc<OwnerFs>, path: &Path) -> Result<MountedFuse> {
    mount_ownerfs_inner(ownerfs, path, None)
}

#[cfg(feature = "ownerfs")]
pub fn mount_ownerfs_with_metrics(
    ownerfs: Arc<OwnerFs>,
    path: &Path,
    metrics: FuseRequestMetrics,
) -> Result<MountedFuse> {
    mount_ownerfs_inner(ownerfs, path, Some(metrics))
}

#[cfg(feature = "ownerfs")]
fn mount_ownerfs_inner(
    ownerfs: Arc<OwnerFs>,
    path: &Path,
    metrics: Option<FuseRequestMetrics>,
) -> Result<MountedFuse> {
    reject_existing_mount(path)?;
    let mut fs = AfsFuse::new(ownerfs.clone());
    fs.requests = metrics.map(|metrics| (metrics, "ownerfs"));
    fs.ownerfs = Some(ownerfs.clone());
    let cleanup_error = fs.cleanup_error.clone();
    let session =
        fuser::spawn_mount(fs, path, &mount_config("afs-ownerfs", true)).map_err(Error::from)?;
    ownerfs.register_fuse_notifier(session.notifier());
    MountedFuse::new(session, path, cleanup_error)
}

#[doc(hidden)]
pub fn mount_test_backend(backend: Arc<dyn Backend>, path: &Path) -> Result<MountedFuse> {
    reject_existing_mount(path)?;
    let fs = AfsFuse::new(backend);
    let cleanup_error = fs.cleanup_error.clone();
    let session =
        fuser::spawn_mount(fs, path, &mount_config("afs-test", false)).map_err(Error::from)?;
    MountedFuse::new(session, path, cleanup_error)
}

fn mount_config(fs_name: &str, allow_other: bool) -> Config {
    let mut config = Config::default();
    config.mount_options = vec![MountOption::FSName(fs_name.into()), MountOption::NoAtime];
    if allow_other {
        config.acl = SessionACL::All;
    }
    config.mount_options.push(MountOption::DefaultPermissions);
    config
}

fn reject_existing_mount(path: &Path) -> Result<()> {
    if mountpoint_is_current(path)? {
        return Err(afs_error::Error::coded(
            afs_error::NODE_MOUNT_CONFLICT,
            format!("mount point '{}' is already mounted", path.display()),
        ));
    }
    Ok(())
}

fn mountpoint_is_current(path: &Path) -> Result<bool> {
    let target = path.canonicalize().map_err(Error::from)?;
    Ok(current_mountpoints()?.into_iter().any(|mountpoint| {
        mountpoint
            .canonicalize()
            .is_ok_and(|mounted| mounted == target)
    }))
}

fn current_mount_id(path: &Path) -> Result<Option<u64>> {
    let target = path.canonicalize().map_err(Error::from)?;
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").map_err(Error::from)?;
    for line in mountinfo.lines().rev() {
        let fields: Vec<_> = line.split_whitespace().collect();
        if fields.len() < 6 {
            return Err(Error::from(io::Error::other("malformed mountinfo")));
        }
        if decode_mountinfo_path(fields[4])? == target {
            return fields[0]
                .parse()
                .map(Some)
                .map_err(|_| Error::from(io::Error::other("invalid mount ID")));
        }
    }
    Ok(None)
}

fn current_mountpoints() -> Result<Vec<PathBuf>> {
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").map_err(Error::from)?;
    mountinfo
        .lines()
        .map(|line| {
            line.split_whitespace()
                .nth(4)
                .ok_or_else(|| {
                    afs_error::Error::coded(
                        afs_error::RUNTIME_INTERNAL,
                        "malformed /proc/self/mountinfo line",
                    )
                })
                .and_then(decode_mountinfo_path)
        })
        .collect()
}

fn decode_mountinfo_path(encoded: &str) -> Result<PathBuf> {
    let bytes = encoded.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            if index + 3 >= bytes.len() {
                return Err(afs_error::Error::coded(
                    afs_error::RUNTIME_INTERNAL,
                    format!("malformed mountinfo escape in '{encoded}'"),
                ));
            }
            let value = parse_octal(&bytes[index + 1..index + 4]).ok_or_else(|| {
                afs_error::Error::coded(
                    afs_error::RUNTIME_INTERNAL,
                    format!("malformed mountinfo escape in '{encoded}'"),
                )
            })?;
            decoded.push(value);
            index += 4;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Ok(PathBuf::from(
        String::from_utf8_lossy(&decoded).into_owned(),
    ))
}

fn parse_octal(bytes: &[u8]) -> Option<u8> {
    bytes.iter().try_fold(0u8, |acc, byte| match byte {
        b'0'..=b'7' => acc.checked_mul(8)?.checked_add(byte - b'0'),
        _ => None,
    })
}

type FuseJob = Box<dyn FnOnce() + Send + 'static>;

// fuser dispatches callbacks on one receive thread. Different file handles may
// run concurrently, but requests for the same handle must remain FIFO so an
// fsync cannot pass its preceding write, or release pass fsync. The bounded
// queue backpressures the receive thread if peers stall.
struct FuseDispatch {
    shared: Arc<(Mutex<FuseQueue>, Condvar)>,
    capacity: usize,
}

struct FuseQueue {
    ready: VecDeque<(Option<u64>, FuseJob)>,
    waiting: HashMap<u64, VecDeque<FuseJob>>,
    active: HashSet<u64>,
    jobs: usize,
    closed: bool,
}

impl FuseDispatch {
    fn new(workers: usize) -> Self {
        let shared = Arc::new((
            Mutex::new(FuseQueue {
                ready: VecDeque::new(),
                waiting: HashMap::new(),
                active: HashSet::new(),
                jobs: 0,
                closed: false,
            }),
            Condvar::new(),
        ));
        for index in 0..workers {
            let shared = shared.clone();
            thread::Builder::new()
                .name(format!("afs-fuse-{index}"))
                .spawn(move || {
                    loop {
                        let (key, job) = {
                            let (queue, wake) = &*shared;
                            let mut queue = queue.lock().unwrap();
                            while queue.ready.is_empty() && !queue.closed {
                                queue = wake.wait(queue).unwrap();
                            }
                            let Some(job) = queue.ready.pop_front() else {
                                break;
                            };
                            job
                        };
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err() {
                            afs_logging::error!("FUSE worker callback panicked");
                        }
                        let (queue, wake) = &*shared;
                        let mut queue = queue.lock().unwrap();
                        queue.jobs -= 1;
                        if let Some(key) = key {
                            let next = queue.waiting.get_mut(&key).and_then(VecDeque::pop_front);
                            if queue.waiting.get(&key).is_some_and(VecDeque::is_empty) {
                                queue.waiting.remove(&key);
                            }
                            if let Some(next) = next {
                                queue.ready.push_back((Some(key), next));
                            } else {
                                queue.active.remove(&key);
                            }
                        }
                        wake.notify_all();
                    }
                })
                .expect("FUSE worker thread must start");
        }
        Self {
            shared,
            capacity: workers * 8,
        }
    }

    fn submit(&self, job: impl FnOnce() + Send + 'static) {
        let (queue, wake) = &*self.shared;
        let mut queue = queue.lock().unwrap();
        if queue.jobs >= self.capacity || queue.closed {
            drop(queue);
            job();
            return;
        }
        queue.jobs += 1;
        queue.ready.push_back((None, Box::new(job)));
        wake.notify_one();
    }

    fn submit_keyed(&self, key: u64, job: impl FnOnce() + Send + 'static) {
        let (queue, wake) = &*self.shared;
        let mut queue = queue.lock().unwrap();
        while queue.jobs >= self.capacity && !queue.closed {
            queue = wake.wait(queue).unwrap();
        }
        if queue.closed {
            drop(queue);
            job();
            return;
        }
        queue.jobs += 1;
        if queue.active.insert(key) {
            queue.ready.push_back((Some(key), Box::new(job)));
            wake.notify_one();
        } else {
            queue
                .waiting
                .entry(key)
                .or_default()
                .push_back(Box::new(job));
        }
    }

    fn close_and_drain(&self) {
        let (queue, wake) = &*self.shared;
        let mut state = queue.lock().unwrap();
        state.closed = true;
        wake.notify_all();
        // Workers retain ownership of accepted callbacks. Wait for their
        // completion; the Node process deadline covers a blocked callback.
        while state.jobs != 0 {
            state = wake.wait(state).unwrap();
        }
    }
}

impl Drop for FuseDispatch {
    fn drop(&mut self) {
        self.close_and_drain();
    }
}

pub struct AfsFuse {
    requests: Option<(FuseRequestMetrics, &'static str)>,
    backend: Arc<dyn Backend>,
    cached_io: bool,
    inline_local_read_scratch: Mutex<Vec<u8>>,
    state: Arc<Mutex<FuseState>>,
    file_handle_inodes: Arc<Mutex<HashMap<u64, u64>>>,
    dispatch: FuseDispatch,
    ingress_session_id: String,
    lock_session_cleaned: bool,
    callbacks_drained: bool,
    cleanup_error: Arc<Mutex<Option<Error>>>,
    #[cfg(feature = "ownerfs")]
    ownerfs: Option<Arc<OwnerFs>>,
}

impl AfsFuse {
    #[must_use]
    fn new(backend: Arc<dyn Backend>) -> Self {
        let state = Arc::new(Mutex::new(FuseState::new(backend.root_inode())));
        Self {
            requests: None,
            backend,
            cached_io: false,
            inline_local_read_scratch: Mutex::new(Vec::new()),
            state,
            file_handle_inodes: Arc::new(Mutex::new(HashMap::new())),
            dispatch: FuseDispatch::new(8),
            ingress_session_id: next_ingress_session_id(),
            lock_session_cleaned: false,
            callbacks_drained: false,
            cleanup_error: Arc::new(Mutex::new(None)),
            #[cfg(feature = "ownerfs")]
            ownerfs: None,
        }
    }

    fn record_request(&self, operation: &'static str) {
        if let Some((metrics, filesystem)) = &self.requests {
            metrics.record(filesystem, operation);
        }
    }

    fn remember_cached_inode(&self, _inode: BackendInode, ino: u64) {
        #[cfg(feature = "ownerfs")]
        if let Some(ownerfs) = &self.ownerfs {
            ownerfs.remember_fuse_inode(ino);
        }
        #[cfg(not(feature = "ownerfs"))]
        let _ = ino;
    }

    fn with_cache_policy<T>(
        &self,
        inode: BackendInode,
        reply: impl FnOnce(Duration, bool) -> T,
    ) -> T {
        #[cfg(feature = "ownerfs")]
        if let Some(ownerfs) = &self.ownerfs {
            return ownerfs.with_fuse_cache_policy(inode, reply);
        }
        #[cfg(not(feature = "ownerfs"))]
        let _ = inode;
        reply(TTL, false)
    }

    fn context(req: &Request, umask: u32) -> RequestContext {
        RequestContext {
            uid: req.uid(),
            gid: req.gid(),
            pid: req.pid(),
            umask,
            supplementary_gids: Vec::new(),
        }
    }

    fn metadata_context(req: &Request, umask: u32) -> RequestContext {
        let mut context = Self::context(req, umask);
        context.supplementary_gids = request_groups(&context);
        context
    }

    fn backend_inode(&self, ino: u64) -> std::result::Result<BackendInode, i32> {
        self.state
            .lock()
            .unwrap()
            .backend_inode(ino)
            .ok_or(libc::ESTALE)
    }

    fn file_lock_owner(&self, kernel_owner: LockOwner) -> FileLockOwner {
        FileLockOwner {
            ingress_session_id: self.ingress_session_id.clone(),
            kernel_owner: kernel_owner.0,
        }
    }

    fn cleanup_lock_session(&mut self) {
        if self.lock_session_cleaned {
            return;
        }
        self.lock_session_cleaned = true;
        self.release_lock_session();
    }

    fn release_lock_session(&self) {
        if let Err(error) = self.backend.release_lock_session(&self.ingress_session_id) {
            let errno = errno(error.clone());
            self.cleanup_error.lock().unwrap().get_or_insert(error);
            afs_logging::error!("fuse.lock_session_cleanup_failed"; "errno" => errno);
        }
    }

    fn finish_callbacks(&mut self) {
        if self.callbacks_drained {
            return;
        }
        // Fence the session before draining accepted callbacks. This official
        // fuser delivery does not forward advisory locks, but existing backend
        // lock state is still released for recoverable historical sessions.
        self.cleanup_lock_session();
        self.dispatch.close_and_drain();
        // Final idempotent sweep observes every accepted callback's terminal
        // outcome before a joined mount can report successful cleanup.
        self.release_lock_session();
        self.callbacks_drained = true;
    }
}

impl Drop for AfsFuse {
    fn drop(&mut self) {
        self.finish_callbacks();
    }
}

impl Filesystem for AfsFuse {
    fn init(&mut self, _: &Request, config: &mut KernelConfig) -> io::Result<()> {
        self.record_request("init");
        let _ = config.set_max_write(1024 * 1024);
        for capability in negotiated_init_flags(config.capabilities()) {
            let _ = config.add_capabilities(capability);
        }
        Ok(())
    }

    fn destroy(&mut self) {
        self.record_request("destroy");
        self.finish_callbacks();
    }

    fn lookup(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        self.record_request("lookup");
        let Ok(parent_inode) = self.backend_inode(parent.0) else {
            reply.error(fuse_errno(libc::ESTALE));
            return;
        };
        // A local Home directory lookup has no network wait. Keep it on the
        // FUSE receive thread; remote lookups still need worker concurrency.
        #[cfg(feature = "ownerfs")]
        let inline_local_lookup = self
            .ownerfs
            .as_ref()
            .is_some_and(|ownerfs| ownerfs.is_local_inode(parent_inode));
        #[cfg(not(feature = "ownerfs"))]
        let inline_local_lookup = false;
        let backend = self.backend.clone();
        let state = self.state.clone();
        let context = Self::context(req, 0);
        let name = name.to_os_string();
        #[cfg(feature = "ownerfs")]
        let ownerfs = self.ownerfs.clone();
        let run = move || {
            let result = backend.lookup(&context, parent_inode, &name).map_err(errno);
            match result {
                Ok(entry) => {
                    let ino = state.lock().unwrap().remember_lookup(&entry);
                    #[cfg(feature = "ownerfs")]
                    if let Some(ownerfs) = &ownerfs {
                        ownerfs.remember_fuse_inode(ino);
                        ownerfs.with_fuse_cache_policy(entry.inode, |ttl, _| {
                            // A native or remote writer may rename or unlink between
                            // opens. Apply the freshness policy to names as well as
                            // attributes so an old dentry cannot open a new alias.
                            reply.entry(&ttl, &file_attr(ino, &entry.attributes), Generation(0));
                        });
                        return;
                    }
                    reply.entry(&TTL, &file_attr(ino, &entry.attributes), Generation(0));
                }
                Err(error) => reply.error(fuse_errno(error)),
            }
        };
        if inline_local_lookup {
            run();
        } else {
            self.dispatch.submit(run);
        }
    }

    fn forget(&self, _: &Request, ino: INodeNo, nlookup: u64) {
        self.record_request("forget");
        self.state.lock().unwrap().forget(ino.0, nlookup);
    }

    fn getattr(&self, req: &Request, ino: INodeNo, fh: Option<FuseFileHandle>, reply: ReplyAttr) {
        self.record_request("getattr");
        let ino = ino.0;
        let fh = fh.map(|fh| fh.0);
        let inline_local_read = fh.is_some_and(|fh| {
            self.state
                .lock()
                .unwrap()
                .file_handle(fh)
                .is_some_and(|file| file.inline_local_read)
        });
        let backend = self.backend.clone();
        let state = self.state.clone();
        let context = Self::context(req, 0);
        #[cfg(feature = "ownerfs")]
        let ownerfs = self.ownerfs.clone();
        let run = move || {
            let node = state.lock().unwrap().node(ino);
            let result = match node {
                Some(FuseNode::Root(inode)) | Some(FuseNode::Backend(inode)) => {
                    let handle = fh.and_then(|fh| state.lock().unwrap().file_handle(fh));
                    handle
                        .map_or(Ok(()), |_| AfsFuse::validate_handle_inode_in(&state, ino))
                        .and_then(|()| {
                            backend
                                .getattr(&context, inode, handle.map(|handle| handle.handle))
                                .map(|attributes| file_attr(ino, &attributes))
                                .map_err(errno)
                        })
                }
                None => Err(libc::ESTALE),
            };
            let backend_inode = state.lock().unwrap().backend_inode(ino);
            match result {
                Ok(attr) => match backend_inode {
                    Some(inode) => {
                        #[cfg(feature = "ownerfs")]
                        if let Some(ownerfs) = &ownerfs {
                            ownerfs.with_fuse_cache_policy(inode, |ttl, _| reply.attr(&ttl, &attr));
                            return;
                        }
                        #[cfg(not(feature = "ownerfs"))]
                        let _ = inode;
                        reply.attr(&TTL, &attr);
                    }
                    None => reply.attr(&TTL, &attr),
                },
                Err(error) => reply.error(fuse_errno(error)),
            }
        };
        if let Some(fh) = fh.filter(|_| !inline_local_read) {
            self.dispatch.submit_keyed(fh, run);
        } else {
            run();
        }
    }

    fn statfs(&self, req: &Request, ino: INodeNo, reply: ReplyStatfs) {
        self.record_request("statfs");
        let Ok(inode) = self.backend_inode(ino.0) else {
            reply.error(fuse_errno(libc::ESTALE));
            return;
        };
        #[cfg(feature = "ownerfs")]
        let inline_local_statfs = self
            .ownerfs
            .as_ref()
            .is_some_and(|ownerfs| ownerfs.is_local_inode(inode));
        #[cfg(not(feature = "ownerfs"))]
        let inline_local_statfs = false;
        let backend = self.backend.clone();
        let context = Self::metadata_context(req, 0);
        let run = move || match backend.statfs(&context, inode) {
            Ok(capacity) => reply_statfs(reply, capacity),
            Err(error) => reply.error(fuse_errno(errno(error))),
        };
        if inline_local_statfs {
            run();
        } else {
            self.dispatch.submit(run);
        }
    }

    fn setattr(
        &self,
        req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FuseFileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        self.record_request("setattr");
        let ino = ino.0;
        let fh = fh.map(|fh| fh.0);
        let inline_local_read = fh.is_some_and(|fh| {
            self.state
                .lock()
                .unwrap()
                .file_handle(fh)
                .is_some_and(|file| file.inline_local_read)
        });
        let backend = self.backend.clone();
        let state = self.state.clone();
        let context = Self::metadata_context(req, 0);
        let timestamps_now = timestamps_now_only(atime.as_ref(), mtime.as_ref());
        let change = AttributeChange {
            size,
            mode,
            uid,
            gid,
            atime: atime.map(time_or_now),
            mtime: mtime.map(time_or_now),
        };
        let run = move || {
            let inode = state.lock().unwrap().backend_inode(ino);
            let result = inode.ok_or(libc::ESTALE).and_then(|inode| {
                let handle = fh.and_then(|fh| state.lock().unwrap().file_handle(fh));
                if handle.is_some() {
                    AfsFuse::validate_handle_inode_in(&state, ino)?;
                }
                let current = backend
                    .getattr(&context, inode, handle.map(|handle| handle.handle))
                    .map_err(errno)?;
                let options = SetAttrOptions {
                    kill_suidgid: is_legacy_privilege_clear(&current, &change),
                    timestamps_now,
                };
                Ok(backend.as_ref()).and_then(|backend| {
                    backend
                        .setattr_with_options(
                            &context,
                            inode,
                            handle.map(|handle| handle.handle),
                            &change,
                            options,
                        )
                        .map_err(errno)
                })
            });
            match result {
                Ok(attributes) => reply.attr(&TTL, &file_attr(ino, &attributes)),
                Err(error) => reply.error(fuse_errno(error)),
            }
        };
        if let Some(fh) = fh.filter(|_| !inline_local_read) {
            self.dispatch.submit_keyed(fh, run);
        } else {
            run();
        }
    }

    fn readlink(&self, req: &Request, ino: INodeNo, reply: ReplyData) {
        self.record_request("readlink");
        let result = self.backend_inode(ino.0).and_then(|inode| {
            Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .readlink(&Self::context(req, 0), inode)
                    .map(|target| os_str_bytes(target.as_os_str()).to_vec())
                    .map_err(errno)
            })
        });
        match result {
            Ok(data) => reply.data(&data),
            Err(error) => reply.error(fuse_errno(error)),
        }
    }

    fn mkdir(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        self.record_request("mkdir");
        let result = self.backend_inode(parent.0).and_then(|parent_inode| {
            Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .mkdir(&Self::context(req, umask), parent_inode, name, mode)
                    .map_err(errno)
            })
        });
        match result {
            Ok(entry) => {
                let ino = self.state.lock().unwrap().remember_lookup(&entry);
                reply.entry(&TTL, &file_attr(ino, &entry.attributes), Generation(0));
            }
            Err(error) => reply.error(fuse_errno(error)),
        }
    }

    fn unlink(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.record_request("unlink");
        let result = self.backend_inode(parent.0).and_then(|parent_inode| {
            Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .unlink(&Self::context(req, 0), parent_inode, name)
                    .map_err(errno)
            })
        });
        reply_empty(reply, result);
    }

    fn rmdir(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.record_request("rmdir");
        let result = self.backend_inode(parent.0).and_then(|parent_inode| {
            Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .rmdir(&Self::context(req, 0), parent_inode, name)
                    .map_err(errno)
            })
        });
        reply_empty(reply, result);
    }

    fn symlink(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        self.record_request("symlink");
        let result = self.backend_inode(parent.0).and_then(|parent_inode| {
            Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .symlink(
                        &Self::context(req, 0),
                        parent_inode,
                        name,
                        target.as_os_str(),
                    )
                    .map_err(errno)
            })
        });
        match result {
            Ok(entry) => {
                let ino = self.state.lock().unwrap().remember_lookup(&entry);
                reply.entry(&TTL, &file_attr(ino, &entry.attributes), Generation(0));
            }
            Err(error) => reply.error(fuse_errno(error)),
        }
    }

    fn rename(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: FuseRenameFlags,
        reply: ReplyEmpty,
    ) {
        self.record_request("rename");
        afs_logging::debug!(
            "fuse.rename";
            "parent" => parent.0,
            "name" => name.to_string_lossy().into_owned(),
            "newparent" => newparent.0,
            "newname" => newname.to_string_lossy().into_owned(),
            "flags" => flags.bits(),
        );
        let result = self.backend_inode(parent.0).and_then(|from_parent| {
            let to_parent = self.backend_inode(newparent.0)?;
            Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .rename(
                        &Self::context(req, 0),
                        from_parent,
                        name,
                        to_parent,
                        newname,
                        RenameFlags(flags.bits()),
                    )
                    .map_err(errno)
            })
        });
        reply_empty(reply, result);
    }

    fn link(
        &self,
        req: &Request,
        ino: INodeNo,
        newparent: INodeNo,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        self.record_request("link");
        let result = self.backend_inode(ino.0).and_then(|inode| {
            let parent = self.backend_inode(newparent.0)?;
            Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .link(&Self::context(req, 0), inode, parent, newname)
                    .map_err(errno)
            })
        });
        match result {
            Ok(entry) => {
                let ino = self.state.lock().unwrap().remember_lookup(&entry);
                reply.entry(&TTL, &file_attr(ino, &entry.attributes), Generation(0));
            }
            Err(error) => reply.error(fuse_errno(error)),
        }
    }

    fn open(&self, req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        self.record_request("open");
        let ino = ino.0;
        let flags = flags.0;
        let Ok(inode) = self.backend_inode(ino) else {
            reply.error(fuse_errno(libc::ESTALE));
            return;
        };
        let options = OpenOptions {
            kill_suidgid: false,
        };
        // The kernel cannot submit operations on this fh until open replies.
        // This lets independent writable opens overlap with read-only ones;
        // O_PATH remains on the original path because it has no I/O handle.
        if flags & libc::O_PATH != 0 {
            let result = Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .open_with_options(&Self::context(req, 0), inode, flags, options)
                    .map_err(errno)
            });
            match result {
                Ok(handle) => {
                    let fh = self.state.lock().unwrap().insert_file_handle(handle);
                    self.file_handle_inodes.lock().unwrap().insert(fh, ino);
                    self.with_cache_policy(inode, |_, private| {
                        reply.opened(FuseFileHandle(fh), open_reply_flags(!private));
                    });
                }
                Err(error) => reply.error(fuse_errno(error)),
            }
            return;
        }
        #[cfg(feature = "ownerfs")]
        let inline_local_read = flags & libc::O_ACCMODE == libc::O_RDONLY
            && self
                .ownerfs
                .as_ref()
                .is_some_and(|ownerfs| ownerfs.is_local_inode(inode));
        #[cfg(not(feature = "ownerfs"))]
        let inline_local_read = false;
        let backend = self.backend.clone();
        let state = self.state.clone();
        let file_handle_inodes = self.file_handle_inodes.clone();
        let context = Self::context(req, 0);
        let cached_io = self.cached_io;
        #[cfg(feature = "ownerfs")]
        let ownerfs = self.ownerfs.clone();
        let run = move || {
            let result = backend
                .open_with_options(&context, inode, flags, options)
                .map_err(errno);
            match result {
                Ok(handle) => {
                    let fh = state
                        .lock()
                        .unwrap()
                        .insert_file_handle_with_policy(handle, inline_local_read);
                    file_handle_inodes.lock().unwrap().insert(fh, ino);
                    #[cfg(feature = "ownerfs")]
                    if let Some(ownerfs) = &ownerfs {
                        ownerfs.with_fuse_cache_policy(inode, |_, private| {
                            reply.opened(FuseFileHandle(fh), open_reply_flags(!private));
                        });
                        return;
                    }
                    reply.opened(FuseFileHandle(fh), open_reply_flags(!cached_io));
                }
                Err(error) => reply.error(fuse_errno(error)),
            }
        };
        if inline_local_read {
            run();
        } else {
            self.dispatch.submit(run);
        }
    }

    fn read(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        self.record_request("read");
        let ino = ino.0;
        let fh = fh.0;
        let inline_local_read = self
            .state
            .lock()
            .unwrap()
            .file_handle(fh)
            .is_some_and(|file| file.inline_local_read);
        let backend = self.backend.clone();
        let state = self.state.clone();
        let context = Self::context(req, 0);
        if inline_local_read {
            let mut scratch = self.inline_local_read_scratch.lock().unwrap();
            let file = state.lock().unwrap().file_handle(fh);
            let file = file
                .ok_or(libc::ESTALE)
                .and_then(|file| AfsFuse::validate_handle_inode_in(&state, ino).map(|()| file));
            let result = match file {
                Ok(file) => inline_local_read_data(&mut scratch, size as usize, |out| {
                    backend
                        .read(&context, file.handle, offset, out)
                        .map_err(errno)
                }),
                Err(error) => Err(error),
            };
            match result {
                Ok(data) => reply.data(&data),
                Err(error) => reply.error(fuse_errno(error)),
            }
            return;
        }
        let run = move || {
            let result = (|| {
                let file = state.lock().unwrap().file_handle(fh).ok_or(libc::ESTALE)?;
                AfsFuse::validate_handle_inode_in(&state, ino)?;
                let mut out = vec![0; size as usize];
                Ok(backend.as_ref()).and_then(|backend| {
                    backend
                        .read(&context, file.handle, offset, &mut out)
                        .map(|n| {
                            out.truncate(n);
                            out
                        })
                        .map_err(errno)
                })
            })();
            match result {
                Ok(data) => reply.data(&data),
                Err(error) => reply.error(fuse_errno(error)),
            }
        };
        self.dispatch.submit_keyed(fh, run);
    }

    fn write(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        data: &[u8],
        write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        self.record_request("write");
        let ino = ino.0;
        let fh = fh.0;
        let backend = self.backend.clone();
        let state = self.state.clone();
        let context = Self::context(req, 0);
        let data = data.to_vec();
        let options = WriteOptions {
            kill_suidgid: write_kill_suidgid(write_flags),
        };
        self.dispatch.submit_keyed(fh, move || {
            let result = (|| {
                let file = state.lock().unwrap().file_handle(fh).ok_or(libc::ESTALE)?;
                AfsFuse::validate_handle_inode_in(&state, ino)?;
                Ok(backend.as_ref()).and_then(|backend| {
                    backend
                        .write_with_options(&context, file.handle, offset, &data, options)
                        .map_err(errno)
                })
            })();
            match result {
                Ok(written) => match u32::try_from(written) {
                    Ok(written) => reply.written(written),
                    Err(_) => reply.error(fuse_errno(libc::EIO)),
                },
                Err(error) => reply.error(fuse_errno(error)),
            }
        });
    }

    fn flush(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FuseFileHandle,
        lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        self.record_request("flush");
        let ino = ino.0;
        let fh = fh.0;
        let inline_local_read = self
            .state
            .lock()
            .unwrap()
            .file_handle(fh)
            .is_some_and(|file| file.inline_local_read);
        let backend = self.backend.clone();
        let state = self.state.clone();
        let context = Self::context(req, 0);
        let owner = self.file_lock_owner(lock_owner);
        let run = move || {
            let file = state.lock().unwrap().file_handle(fh);
            let result = file.ok_or(libc::ESTALE).and_then(|file| {
                AfsFuse::validate_handle_inode_in(&state, ino)?;
                let flushed = backend.flush(&context, file.handle).map_err(errno);
                let released = backend
                    .release_locks(
                        &context,
                        state
                            .lock()
                            .unwrap()
                            .backend_inode(ino)
                            .unwrap_or(BackendInode { value: ino }),
                        file.handle,
                        owner,
                        ReleaseKind::PosixOwner,
                    )
                    .map_err(errno);
                combine_empty_results(flushed, released)
            });
            reply_empty(reply, result);
        };
        if inline_local_read {
            run();
        } else {
            self.dispatch.submit_keyed(fh, run);
        }
    }

    fn release(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FuseFileHandle,
        _flags: OpenFlags,
        lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.record_request("release");
        let ino = ino.0;
        let fh = fh.0;
        let inline_local_read = self
            .state
            .lock()
            .unwrap()
            .file_handle(fh)
            .is_some_and(|file| file.inline_local_read);
        let backend = self.backend.clone();
        let state = self.state.clone();
        let file_handle_inodes = self.file_handle_inodes.clone();
        let context = Self::context(req, 0);
        let flock_owner = lock_owner.map(|owner| self.file_lock_owner(owner));
        let run = move || {
            let file = state.lock().unwrap().remove_file_handle(fh);
            file_handle_inodes.lock().unwrap().remove(&fh);
            let result = file.ok_or(libc::ESTALE).and_then(|file| {
                AfsFuse::validate_handle_inode_in(&state, ino)?;
                let inode = state
                    .lock()
                    .unwrap()
                    .backend_inode(ino)
                    .unwrap_or(BackendInode { value: ino });
                let released_flock = flock_owner.map_or(Ok(()), |owner| {
                    backend
                        .release_locks(&context, inode, file.handle, owner, ReleaseKind::FlockOwner)
                        .map_err(errno)
                });
                let released_handle = backend.release(&context, file.handle).map_err(errno);
                combine_empty_results(released_flock, released_handle)
            });
            reply_empty(reply, result);
        };
        if inline_local_read {
            run();
        } else {
            self.dispatch.submit_keyed(fh, run);
        }
    }

    fn fsync(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FuseFileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.record_request("fsync");
        let ino = ino.0;
        let fh = fh.0;
        let inline_local_read = self
            .state
            .lock()
            .unwrap()
            .file_handle(fh)
            .is_some_and(|file| file.inline_local_read);
        let backend = self.backend.clone();
        let state = self.state.clone();
        let context = Self::context(req, 0);
        let run = move || {
            let file = state.lock().unwrap().file_handle(fh);
            let result = file.ok_or(libc::ESTALE).and_then(|file| {
                AfsFuse::validate_handle_inode_in(&state, ino)?;
                Ok(backend.as_ref()).and_then(|backend| {
                    backend
                        .fsync(&context, file.handle, sync_mode(datasync))
                        .map_err(errno)
                })
            });
            reply_empty(reply, result);
        };
        if inline_local_read {
            run();
        } else {
            self.dispatch.submit_keyed(fh, run);
        }
    }

    fn opendir(&self, req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        self.record_request("opendir");
        let ino = ino.0;
        let node = self.state.lock().unwrap().node(ino);
        match node {
            Some(FuseNode::Root(_)) | Some(FuseNode::Backend(_)) => {
                let result = self.backend_inode(ino).and_then(|inode| {
                    self.backend
                        .opendir(&Self::context(req, 0), inode)
                        .map_err(errno)
                });
                match result {
                    Ok(handle) => {
                        let fh = self.state.lock().unwrap().insert_directory_handle(handle);
                        reply.opened(FuseFileHandle(fh), FopenFlags::empty());
                    }
                    Err(error) => reply.error(fuse_errno(error)),
                }
            }
            None => reply.error(fuse_errno(libc::ESTALE)),
        }
    }

    fn readdir(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        self.record_request("readdir");
        let ino = ino.0;
        let fh = fh.0;
        let node = self.state.lock().unwrap().node(ino);
        match node {
            Some(FuseNode::Root(_)) | Some(FuseNode::Backend(_)) => {
                let directory = self.state.lock().unwrap().directory_handle(fh);
                let result = directory.ok_or(libc::ESTALE).and_then(|directory| {
                    self.validate_handle_inode(ino)?;
                    let cookie = backend_cookie(offset);
                    Ok(self.backend.as_ref()).and_then(|backend| {
                        backend
                            .readdir(&Self::context(req, 0), directory.handle, cookie, 128)
                            .map_err(errno)
                    })
                });
                match result {
                    Ok(entries) => {
                        add_backend_entries(
                            &mut self.state.lock().unwrap(),
                            &mut reply,
                            ino,
                            offset,
                            &entries,
                        );
                        reply.ok();
                    }
                    Err(error) => reply.error(fuse_errno(error)),
                }
            }
            None => reply.error(fuse_errno(libc::ESTALE)),
        }
    }

    fn releasedir(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FuseFileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        self.record_request("releasedir");
        let ino = ino.0;
        let fh = fh.0;
        let directory = self.state.lock().unwrap().remove_directory_handle(fh);
        let result = directory.ok_or(libc::ESTALE).and_then(|directory| {
            self.validate_handle_inode(ino)?;
            Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .releasedir(&Self::context(req, 0), directory.handle)
                    .map_err(errno)
            })
        });
        reply_empty(reply, result);
    }

    fn fsyncdir(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FuseFileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.record_request("fsyncdir");
        let ino = ino.0;
        let fh = fh.0;
        let directory = self.state.lock().unwrap().directory_handle(fh);
        let result = directory.ok_or(libc::ESTALE).and_then(|directory| {
            self.validate_handle_inode(ino)?;
            Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .fsyncdir(
                        &Self::context(req, 0),
                        directory.handle,
                        sync_mode(datasync),
                    )
                    .map_err(errno)
            })
        });
        reply_empty(reply, result);
    }

    fn create(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        self.record_request("create");
        afs_logging::info!("fuse.create"; "parent" => parent.0, "name" => name.to_string_lossy().into_owned());
        let options = OpenOptions {
            kill_suidgid: false,
        };
        let result = self.backend_inode(parent.0).and_then(|parent_inode| {
            Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .create_with_options(
                        &Self::context(req, umask),
                        parent_inode,
                        name,
                        mode,
                        flags,
                        options,
                    )
                    .map_err(errno)
            })
        });
        match result {
            Ok(created) => {
                let ino = self.state.lock().unwrap().remember_lookup(&created.entry);
                self.remember_cached_inode(created.entry.inode, ino);
                let fh = self
                    .state
                    .lock()
                    .unwrap()
                    .insert_file_handle(created.handle);
                self.file_handle_inodes.lock().unwrap().insert(fh, ino);
                self.with_cache_policy(created.entry.inode, |ttl, private| {
                    reply.created(
                        &ttl,
                        &file_attr(ino, &created.entry.attributes),
                        Generation(0),
                        FuseFileHandle(fh),
                        open_reply_flags(!private && !self.cached_io),
                    )
                });
            }
            Err(error) => reply.error(fuse_errno(error)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn getlk(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FuseFileHandle,
        _lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        _pid: u32,
        reply: ReplyLock,
    ) {
        self.record_request("getlk");
        reply.error(fuse_errno(unsupported_lock_errno(start, end, typ)));
    }

    #[allow(clippy::too_many_arguments)]
    fn setlk(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FuseFileHandle,
        _lock_owner: LockOwner,
        start: u64,
        end: u64,
        typ: i32,
        _pid: u32,
        _sleep: bool,
        reply: ReplyEmpty,
    ) {
        self.record_request("setlk");
        reply.error(fuse_errno(unsupported_lock_errno(start, end, typ)));
    }

    fn mknod(
        &self,
        req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        self.record_request("mknod");
        let parent = parent.0;
        let raw_kind = mode & libc::S_IFMT;
        let context = Self::metadata_context(req, umask);
        let permissions = mode & !libc::S_IFMT;
        let result = self
            .backend_inode(parent)
            .and_then(|parent| match raw_kind {
                0 | libc::S_IFREG => {
                    let created = self
                        .backend
                        .create(
                            &context,
                            parent,
                            name,
                            permissions,
                            libc::O_CREAT | libc::O_EXCL | libc::O_WRONLY,
                        )
                        .map_err(errno)?;
                    // mknod has no application FD whose close could flush this handle.
                    // Finish the create boundary and release our temporary handle.
                    let flushed = self.backend.flush(&context, created.handle);
                    let released = self.backend.release(&context, created.handle);
                    flushed.map_err(errno)?;
                    released.map_err(errno)?;
                    Ok(created.entry)
                }
                libc::S_IFIFO => self
                    .backend
                    .mknod(&context, parent, name, SpecialFileKind::Fifo, permissions)
                    .map_err(errno),
                libc::S_IFSOCK => self
                    .backend
                    .mknod(&context, parent, name, SpecialFileKind::Socket, permissions)
                    .map_err(errno),
                libc::S_IFBLK => self
                    .backend
                    .mknod(
                        &context,
                        parent,
                        name,
                        SpecialFileKind::BlockDevice {
                            rdev: u64::from(rdev),
                        },
                        permissions,
                    )
                    .map_err(errno),
                libc::S_IFCHR => self
                    .backend
                    .mknod(
                        &context,
                        parent,
                        name,
                        SpecialFileKind::CharDevice {
                            rdev: u64::from(rdev),
                        },
                        permissions,
                    )
                    .map_err(errno),
                _ => Err(libc::EINVAL),
            });
        match result {
            Ok(entry) => {
                let ino = self.state.lock().unwrap().remember_lookup(&entry);
                self.remember_cached_inode(entry.inode, ino);
                reply.entry(&TTL, &file_attr(ino, &entry.attributes), Generation(0));
            }
            Err(error) => reply.error(fuse_errno(error)),
        }
    }

    fn access(&self, _: &Request, _: INodeNo, _: AccessFlags, reply: ReplyEmpty) {
        self.record_request("access");
        reply.error(fuse_errno(libc::ENOSYS));
    }

    fn getxattr(&self, req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        self.record_request("getxattr");
        let Ok(inode) = self.backend_inode(ino.0) else {
            reply.error(fuse_errno(libc::ESTALE));
            return;
        };
        let backend = self.backend.clone();
        let context = Self::metadata_context(req, 0);
        let name = name.to_os_string();
        self.dispatch.submit(move || {
            reply_xattr(reply, size, backend.getxattr(&context, inode, &name));
        });
    }

    fn listxattr(&self, req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        self.record_request("listxattr");
        let Ok(inode) = self.backend_inode(ino.0) else {
            reply.error(fuse_errno(libc::ESTALE));
            return;
        };
        let backend = self.backend.clone();
        let context = Self::metadata_context(req, 0);
        self.dispatch.submit(move || {
            reply_xattr(reply, size, backend.listxattr(&context, inode));
        });
    }

    fn setxattr(
        &self,
        req: &Request,
        ino: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: ReplyEmpty,
    ) {
        self.record_request("setxattr");
        if let Err(error) = validate_xattr_set(flags, position) {
            reply.error(fuse_errno(error));
            return;
        }
        let Ok(inode) = self.backend_inode(ino.0) else {
            reply.error(fuse_errno(libc::ESTALE));
            return;
        };
        let backend = self.backend.clone();
        let context = Self::metadata_context(req, 0);
        let name = name.to_os_string();
        let value = value.to_vec();
        self.dispatch.submit(move || {
            reply_empty(
                reply,
                backend
                    .setxattr(&context, inode, &name, &value, flags)
                    .map_err(errno),
            );
        });
    }

    fn removexattr(&self, req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.record_request("removexattr");
        let Ok(inode) = self.backend_inode(ino.0) else {
            reply.error(fuse_errno(libc::ESTALE));
            return;
        };
        let backend = self.backend.clone();
        let context = Self::metadata_context(req, 0);
        let name = name.to_os_string();
        self.dispatch.submit(move || {
            reply_empty(
                reply,
                backend.removexattr(&context, inode, &name).map_err(errno),
            );
        });
    }
}

impl AfsFuse {
    fn validate_handle_inode(&self, ino: u64) -> std::result::Result<(), i32> {
        Self::validate_handle_inode_in(&self.state, ino)
    }

    fn validate_handle_inode_in(
        state: &Mutex<FuseState>,
        ino: u64,
    ) -> std::result::Result<(), i32> {
        match state.lock().unwrap().node(ino) {
            Some(FuseNode::Root(_)) | Some(FuseNode::Backend(_)) => Ok(()),
            None => Ok(()),
        }
    }
}

fn add_backend_entries(
    state: &mut FuseState,
    reply: &mut ReplyDirectory,
    parent: u64,
    offset: u64,
    entries: &[DirectoryEntry],
) {
    if offset == 0 {
        let _ = reply.add(INodeNo(parent), 1, FileType::Directory, ".");
        let _ = reply.add(INodeNo(ROOT_INO), 2, FileType::Directory, "..");
    }
    for entry in entries {
        let fuse_entry = Entry {
            inode: entry.inode,
            attributes: FileAttributes {
                kind: entry.kind,
                size: 0,
                blocks: 0,
                mode: fallback_mode(entry.kind),
                uid: 0,
                gid: 0,
                nlink: 1,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
            },
        };
        let ino = state.remember_readdir_entry(&fuse_entry);
        let cookie = entry.next_cookie.max(3);
        if reply.add(INodeNo(ino), cookie, file_type(entry.kind), &entry.name) {
            break;
        }
    }
}

fn backend_cookie(offset: u64) -> u64 {
    if offset <= 2 { 0 } else { offset }
}

fn file_attr(ino: u64, attributes: &FileAttributes) -> FileAttr {
    FileAttr {
        ino: INodeNo(ino),
        size: attributes.size,
        blocks: attributes.blocks,
        atime: attributes.atime,
        mtime: attributes.mtime,
        ctime: attributes.ctime,
        crtime: UNIX_EPOCH,
        kind: file_type(attributes.kind),
        perm: attributes.mode as u16,
        nlink: attributes.nlink,
        uid: attributes.uid,
        gid: attributes.gid,
        rdev: file_rdev(attributes.kind),
        blksize: 4096,
        flags: 0,
    }
}

fn file_type(kind: FileKind) -> FileType {
    match kind {
        FileKind::Regular => FileType::RegularFile,
        FileKind::Directory => FileType::Directory,
        FileKind::Symlink => FileType::Symlink,
        FileKind::Special(SpecialFileKind::Fifo) => FileType::NamedPipe,
        FileKind::Special(SpecialFileKind::Socket) => FileType::Socket,
        FileKind::Special(SpecialFileKind::BlockDevice { .. }) => FileType::BlockDevice,
        FileKind::Special(SpecialFileKind::CharDevice { .. }) => FileType::CharDevice,
    }
}

fn fallback_mode(kind: FileKind) -> u32 {
    match kind {
        FileKind::Directory => 0o755,
        FileKind::Regular => 0o644,
        FileKind::Symlink => 0o777,
        FileKind::Special(SpecialFileKind::Fifo) => 0o644,
        FileKind::Special(SpecialFileKind::Socket) => 0o777,
        FileKind::Special(SpecialFileKind::BlockDevice { .. })
        | FileKind::Special(SpecialFileKind::CharDevice { .. }) => 0o600,
    }
}

fn file_rdev(kind: FileKind) -> u32 {
    match kind {
        FileKind::Special(SpecialFileKind::BlockDevice { rdev })
        | FileKind::Special(SpecialFileKind::CharDevice { rdev }) => {
            u32::try_from(rdev).unwrap_or(u32::MAX)
        }
        _ => 0,
    }
}

fn reply_empty(reply: ReplyEmpty, result: std::result::Result<(), i32>) {
    match result {
        Ok(()) => reply.ok(),
        Err(error) => reply.error(fuse_errno(error)),
    }
}

fn fuse_errno(error: i32) -> Errno {
    Errno::from_i32(error)
}

fn requested_init_flags() -> InitFlags {
    // Linux otherwise serializes LOOKUPs in one directory even when our FUSE
    // receive thread dispatches them to independent workers. Direct-IO mmap is
    // requested so mmap on direct_io handles remains capability-gated; ordinary
    // read/write still use FOPEN_DIRECT_IO for close-to-open freshness.
    InitFlags::FUSE_PARALLEL_DIROPS | InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP
}

fn negotiated_init_flags(available: InitFlags) -> impl Iterator<Item = InitFlags> {
    requested_init_flags()
        .iter()
        .filter(move |capability| available.contains(*capability))
}

fn write_kill_suidgid(write_flags: WriteFlags) -> bool {
    write_flags.contains(WriteFlags::FUSE_WRITE_KILL_SUIDGID)
}

fn open_reply_flags(direct_io: bool) -> FopenFlags {
    if direct_io {
        DIRECT_IO
    } else {
        FopenFlags::empty()
    }
}

fn unsupported_lock_errno(start: u64, end: u64, typ: i32) -> i32 {
    if start > end || !matches!(typ, libc::F_RDLCK | libc::F_WRLCK | libc::F_UNLCK) {
        libc::EINVAL
    } else {
        libc::EOPNOTSUPP
    }
}

fn validate_xattr_set(flags: i32, position: u32) -> std::result::Result<(), i32> {
    // Linux has no positional xattrs. Never forward a platform extension or an
    // invalid CREATE+REPLACE combination as a successful upsert.
    if position != 0 || !matches!(flags, 0 | libc::XATTR_CREATE | libc::XATTR_REPLACE) {
        return Err(libc::EINVAL);
    }
    Ok(())
}

fn xattr_response_size(actual: usize, requested: u32) -> std::result::Result<Option<u32>, i32> {
    let actual = u32::try_from(actual).map_err(|_| libc::E2BIG)?;
    if requested == 0 {
        Ok(Some(actual))
    } else if actual <= requested {
        Ok(None)
    } else {
        Err(libc::ERANGE)
    }
}

fn reply_xattr(reply: ReplyXattr, requested: u32, result: Result<Vec<u8>>) {
    match result {
        Ok(data) => match xattr_response_size(data.len(), requested) {
            Ok(Some(size)) => reply.size(size),
            Ok(None) => reply.data(&data),
            Err(error) => reply.error(fuse_errno(error)),
        },
        Err(error) => reply.error(fuse_errno(errno(error))),
    }
}

fn reply_statfs(reply: ReplyStatfs, capacity: FilesystemCapacity) {
    reply.statfs(
        capacity.blocks,
        capacity.bfree,
        capacity.bavail,
        capacity.files,
        capacity.ffree,
        capacity.bsize,
        capacity.namelen,
        capacity.frsize,
    );
}

fn sync_mode(datasync: bool) -> SyncMode {
    if datasync {
        SyncMode::DataOnly
    } else {
        SyncMode::Full
    }
}

fn next_ingress_session_id() -> String {
    let sequence = NEXT_INGRESS_SESSION.fetch_add(1, Ordering::Relaxed);
    format!("fuse-{}-{sequence}", std::process::id())
}

fn combine_empty_results(
    first: std::result::Result<(), i32>,
    second: std::result::Result<(), i32>,
) -> std::result::Result<(), i32> {
    match (first, second) {
        (Err(error), _) | (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn time_or_now(value: TimeOrNow) -> SystemTime {
    match value {
        TimeOrNow::SpecificTime(time) => time,
        TimeOrNow::Now => SystemTime::now(),
    }
}

fn timestamps_now_only(atime: Option<&TimeOrNow>, mtime: Option<&TimeOrNow>) -> bool {
    let mut saw_timestamp = false;
    for timestamp in [atime, mtime].into_iter().flatten() {
        saw_timestamp = true;
        if !matches!(timestamp, TimeOrNow::Now) {
            return false;
        }
    }
    saw_timestamp
}

fn os_str_bytes(value: &OsStr) -> &[u8] {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes()
}

fn errno(error: Error) -> i32 {
    afs_logging::error!("FUSE request failed"; "code"=>error.code().raw(), "kind"=>format!("{:?}", error.kind()), "message"=>error.message());
    crate::error::errno(&error)
}

// The kernel still enforces DefaultPermissions. FUSE headers supply uid/gid,
// but not supplementary groups. Resolve groups only for metadata operations,
// and fail closed if the caller disappeared or changed credentials. Data IO
// does not pay for /proc reads. No PID/group cache survives setgroups().
fn request_groups(context: &RequestContext) -> Vec<u32> {
    let path = PathBuf::from(format!("/proc/{}", context.pid));
    let observed = (|| -> Option<Vec<u32>> {
        let before = std::fs::read_to_string(path.join("stat")).ok()?;
        let status = std::fs::read_to_string(path.join("status")).ok()?;
        let second = std::fs::read_to_string(path.join("status")).ok()?;
        let after = std::fs::read_to_string(path.join("stat")).ok()?;
        if process_start(&before)? != process_start(&after)? {
            return None;
        }
        let groups = status_groups(&status, context.uid, context.gid)?;
        if groups != status_groups(&second, context.uid, context.gid)? {
            return None;
        }
        Some(groups)
    })();
    observed.unwrap_or_default()
}

fn process_start(stat: &str) -> Option<&str> {
    // comm may contain spaces and ')'; fields after the last ')' start at 3.
    stat.rsplit_once(')')?.1.split_whitespace().nth(19)
}

fn status_groups(status: &str, uid: u32, gid: u32) -> Option<Vec<u32>> {
    let fields = |name: &str| status.lines().find_map(|line| line.strip_prefix(name));
    let fs_id =
        |name: &str| -> Option<u32> { fields(name)?.split_whitespace().nth(3)?.parse().ok() };
    if fs_id("Uid:")? != uid || fs_id("Gid:")? != gid {
        return None;
    }
    let mut groups = fields("Groups:")?
        .split_whitespace()
        .map(str::parse::<u32>)
        .collect::<std::result::Result<Vec<_>, _>>()
        .ok()?;
    groups.sort_unstable();
    groups.dedup();
    Some(groups)
}

#[cfg(test)]
mod dispatch_tests {
    #[test]
    fn inline_local_read_data_observes_fresh_short_smaller_zero_and_eof_reads() {
        use std::os::unix::fs::FileExt;

        let file = tempfile::tempfile().unwrap();
        let mut scratch = Vec::new();
        let mut read = |size, offset| {
            super::inline_local_read_data(&mut scratch, size, |out| {
                file.read_at(out, offset)
                    .map_err(|error| error.raw_os_error().unwrap())
            })
            .unwrap()
            .into_owned()
        };
        file.write_at(b"abcdefgh", 0).unwrap();
        assert_eq!(read(8, 0), b"abcdefgh");
        file.set_len(0).unwrap();
        file.write_at(b"XY", 0).unwrap();
        assert_eq!(read(8, 0), b"XY");
        assert_eq!(read(1, 0), b"X");
        file.write_at(b"ZQ", 0).unwrap();
        assert_eq!(read(8, 0), b"ZQ");
        assert!(read(0, 0).is_empty());
        assert!(read(8, 2).is_empty());
        file.set_len(0).unwrap();
        assert!(read(8, 0).is_empty());
    }

    #[test]
    fn inline_local_read_data_error_after_success_never_returns_prior_bytes() {
        let mut scratch = Vec::new();
        let data = super::inline_local_read_data(&mut scratch, 8, |out| {
            out.copy_from_slice(b"abcdefgh");
            Ok(8)
        })
        .unwrap();
        assert_eq!(data.as_ref(), b"abcdefgh");
        let result = super::inline_local_read_data(&mut scratch, 8, |out| {
            out[0] = b'!';
            Err(libc::EACCES)
        });
        assert_eq!(result.unwrap_err(), libc::EACCES);
        let data = super::inline_local_read_data(&mut scratch, 8, |out| {
            out[0] = b'z';
            Ok(1)
        })
        .unwrap();
        assert_eq!(data.as_ref(), b"z");
    }

    #[test]
    fn inline_local_read_data_clamps_reported_length_to_requested_size() {
        let mut scratch = Vec::new();
        let data = super::inline_local_read_data(&mut scratch, 3, |out| {
            out.copy_from_slice(b"abc");
            Ok(usize::MAX)
        })
        .unwrap();
        assert_eq!(data.as_ref(), b"abc");
    }

    #[test]
    fn inline_local_read_data_reuses_initialized_storage_with_bounded_growth() {
        let mut scratch = Vec::new();
        let mut initialized = 0;
        for size in [
            17,
            64 * 1024,
            700 * 1024,
            super::MAX_INLINE_LOCAL_READ_SCRATCH,
        ] {
            let data = super::inline_local_read_data(&mut scratch, size, |out| {
                assert_eq!(out.len(), size);
                assert!(out[initialized..].iter().all(|byte| *byte == 0));
                out.fill(b'a');
                Ok(size)
            })
            .unwrap();
            assert!(matches!(data, std::borrow::Cow::Borrowed(_)));
            assert_eq!(data.len(), size);
            let pointer = data.as_ptr();
            let capacity = scratch.capacity();
            assert!(capacity >= size);
            assert!(capacity <= super::MAX_INLINE_LOCAL_READ_SCRATCH);
            let data = super::inline_local_read_data(&mut scratch, size / 2, |out| {
                out.fill(b'b');
                Ok(out.len())
            })
            .unwrap();
            assert_eq!(data.as_ptr(), pointer);
            assert!(data.iter().all(|byte| *byte == b'b'));
            assert_eq!(scratch.capacity(), capacity);
            initialized = size;
        }
    }

    #[test]
    fn inline_local_read_data_oversize_success_and_error_leave_scratch_unchanged() {
        let mut scratch = Vec::new();
        let oversized = super::MAX_INLINE_LOCAL_READ_SCRATCH + 1;
        for primed in [false, true] {
            if primed {
                super::inline_local_read_data(&mut scratch, 8, |out| {
                    out.copy_from_slice(b"retained");
                    Ok(8)
                })
                .unwrap();
            }
            let capacity = scratch.capacity();
            let pointer = scratch.as_ptr();
            let contents = scratch.clone();
            let data = super::inline_local_read_data(&mut scratch, oversized, |out| {
                assert_eq!(out.len(), oversized);
                assert!(out.iter().all(|byte| *byte == 0));
                out[..3].copy_from_slice(b"new");
                Ok(3)
            })
            .unwrap();
            assert!(matches!(data, std::borrow::Cow::Owned(_)));
            assert_eq!(data.as_ref(), b"new");
            assert_eq!(scratch.capacity(), capacity);
            assert_eq!(scratch.as_ptr(), pointer);
            assert_eq!(scratch, contents);
            let result = super::inline_local_read_data(&mut scratch, oversized, |out| {
                out[0] = b'!';
                Err(libc::EIO)
            });
            assert_eq!(result.unwrap_err(), libc::EIO);
            assert_eq!(scratch.capacity(), capacity);
            assert_eq!(scratch.as_ptr(), pointer);
            assert_eq!(scratch, contents);
        }
    }

    #[test]
    fn fuse_callback_metrics_share_registry_without_reset_or_cross_registry_leak() {
        let registry = afs_metrics::Registry::new();
        let first = super::FuseRequestMetrics::register(&registry).unwrap();
        let same = super::FuseRequestMetrics::register(&registry.clone()).unwrap();
        let other_registry = afs_metrics::Registry::new();
        let other = super::FuseRequestMetrics::register(&other_registry).unwrap();
        first.record("ownerfs", "read");
        same.record("ownerfs", "read");
        first.record("dfs", "read");
        assert_eq!(
            first.requests.with_label_values(&["ownerfs", "read"]).get(),
            2
        );
        assert_eq!(
            other.requests.with_label_values(&["ownerfs", "read"]).get(),
            0
        );
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(
            text.contains("afs_fuse_callbacks_total{filesystem=\"ownerfs\",operation=\"read\"} 2")
        );
        assert!(text.contains("afs_fuse_callbacks_total{filesystem=\"dfs\",operation=\"read\"} 1"));
        assert_eq!(text, afs_metrics::encode_text(&registry).unwrap());
        assert_eq!(
            first.requests.with_label_values(&["ownerfs", "read"]).get(),
            2
        );
    }

    #[test]
    fn fuse_callback_metrics_export_zeros_for_every_implemented_operation() {
        let registry = afs_metrics::Registry::new();
        super::FuseRequestMetrics::register(&registry).unwrap();
        let text = afs_metrics::encode_text(&registry).unwrap();
        for filesystem in ["ownerfs", "dfs"] {
            for operation in super::FUSE_CALLBACKS {
                assert!(text.contains(&format!("afs_fuse_callbacks_total{{filesystem=\"{filesystem}\",operation=\"{operation}\"}} 0")));
            }
        }
    }

    #[test]
    fn metadata_groups_require_matching_kernel_identity() {
        let status = "Uid:\t1000 1000 1000 1000\nGid:\t100 100 100 100\nGroups:\t200 100 200\n";
        assert_eq!(
            super::status_groups(status, 1000, 100),
            Some(vec![100, 200])
        );
        assert_eq!(super::status_groups(status, 1001, 100), None);
        assert_eq!(super::status_groups(status, 1000, 101), None);
        assert_eq!(super::status_groups("Uid: 1000\n", 1000, 100), None);
    }

    #[test]
    fn process_start_ignores_spaces_and_parentheses_in_command_name() {
        let stat = format!(
            "123 (a name ) b) {}",
            (3..=22)
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(" ")
        );
        assert_eq!(super::process_start(&stat), Some("22"));
        assert_eq!(super::process_start("123 (dead) Z"), None);
    }

    use super::FuseDispatch;
    use std::{
        sync::{Arc, Mutex, mpsc},
        time::Duration,
    };

    #[test]
    fn xattr_size_query_short_buffer_and_empty_value_are_distinct() {
        assert_eq!(super::xattr_response_size(4, 0), Ok(Some(4)));
        assert_eq!(super::xattr_response_size(4, 3), Err(libc::ERANGE));
        assert_eq!(super::xattr_response_size(4, 4), Ok(None));
        assert_eq!(super::xattr_response_size(4, 64), Ok(None));
        assert_eq!(super::xattr_response_size(0, 0), Ok(Some(0)));
        assert_eq!(super::xattr_response_size(0, 1), Ok(None));
        assert_eq!(super::xattr_response_size(usize::MAX, 0), Err(libc::E2BIG));
    }

    #[test]
    fn xattr_create_replace_and_invalid_flags_keep_linux_contract() {
        for flags in [0, libc::XATTR_CREATE, libc::XATTR_REPLACE] {
            assert_eq!(super::validate_xattr_set(flags, 0), Ok(()));
        }
        for flags in [libc::XATTR_CREATE | libc::XATTR_REPLACE, 4, -1] {
            assert_eq!(super::validate_xattr_set(flags, 0), Err(libc::EINVAL));
        }
        assert_eq!(super::validate_xattr_set(0, 1), Err(libc::EINVAL));
    }

    #[test]
    fn sparse_file_attributes_keep_allocated_blocks_separate_from_eof() {
        let attributes = super::FileAttributes {
            kind: super::FileKind::Regular,
            size: 8 * 1024 * 1024 * 1024,
            blocks: 8,
            mode: 0o600,
            uid: 1000,
            gid: 1000,
            nlink: 1,
            atime: super::UNIX_EPOCH,
            mtime: super::UNIX_EPOCH,
            ctime: super::UNIX_EPOCH,
        };
        let attr = super::file_attr(2, &attributes);
        assert_eq!(attr.size, attributes.size);
        assert_eq!(attr.blocks, 8);
    }

    #[test]
    fn dropping_fuse_dispatch_waits_for_admitted_jobs() {
        let dispatch = FuseDispatch::new(2);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (second_tx, second_rx) = mpsc::channel();
        dispatch.submit_keyed(7, move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        dispatch.submit_keyed(7, move || {
            second_tx.send(()).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let dropper = std::thread::spawn(move || {
            drop(dispatch);
            dropped_tx.send(()).unwrap();
        });
        let early = dropped_rx.recv_timeout(Duration::from_millis(100));
        release_tx.send(()).unwrap();
        second_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        dropper.join().unwrap();
        assert!(
            early.is_err(),
            "dispatch teardown returned while accepted write remained active"
        );
        dropped_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    }

    #[test]
    fn full_fuse_queue_releases_blocked_producer_without_ready_successor() {
        let dispatch = Arc::new(FuseDispatch::new(2));
        let (started_tx, started_rx) = mpsc::channel();
        let (first_release_tx, first_release_rx) = mpsc::channel();
        let (second_release_tx, second_release_rx) = mpsc::channel();
        let (finished_tx, finished_rx) = mpsc::channel();
        for (key, release_rx) in [(31, first_release_rx), (32, second_release_rx)] {
            let started_tx = started_tx.clone();
            let finished_tx = finished_tx.clone();
            dispatch.submit_keyed(key, move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                finished_tx.send(()).unwrap();
            });
        }
        for _ in 0..2 {
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        // Only key 31 has queued successors. Completing key 32 must wake the
        // capacity producer even though it makes no worker job ready.
        for _ in 2..dispatch.capacity {
            let finished_tx = finished_tx.clone();
            dispatch.submit_keyed(31, move || finished_tx.send(()).unwrap());
        }
        let (submitting_tx, submitting_rx) = mpsc::channel();
        let (admitted_tx, admitted_rx) = mpsc::channel();
        let producer_dispatch = dispatch.clone();
        let producer = std::thread::spawn(move || {
            submitting_tx.send(()).unwrap();
            producer_dispatch.submit_keyed(33, move || finished_tx.send(()).unwrap());
            admitted_tx.send(()).unwrap();
        });
        submitting_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let early_admission = admitted_rx.recv_timeout(Duration::from_millis(100));
        second_release_tx.send(()).unwrap();
        let capacity_admission = admitted_rx.recv_timeout(Duration::from_secs(1));
        // Release all callbacks before asserting, including on a missed wake.
        first_release_tx.send(()).unwrap();
        for _ in 0..=dispatch.capacity {
            finished_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        producer.join().unwrap();
        dispatch.close_and_drain();
        assert!(early_admission.is_err(), "full queue admitted another job");
        assert!(
            capacity_admission.is_ok(),
            "capacity-only completion lost a wake"
        );
        assert_eq!(dispatch.shared.0.lock().unwrap().jobs, 0);
    }

    #[test]
    fn independent_fuse_jobs_overlap_before_either_finishes() {
        let dispatch = FuseDispatch::new(2);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        for _ in 0..2 {
            let started_tx = started_tx.clone();
            let release_rx = release_rx.clone();
            dispatch.submit(move || {
                started_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            });
        }
        let first = started_rx.recv_timeout(Duration::from_secs(1));
        let second = started_rx.recv_timeout(Duration::from_secs(1));
        release_tx.send(()).unwrap();
        release_tx.send(()).unwrap();
        assert!(
            first.is_ok() && second.is_ok(),
            "jobs remained serial at FUSE ingress"
        );
    }

    #[test]
    fn same_handle_callbacks_keep_order_while_other_handles_progress() {
        let dispatch = FuseDispatch::new(2);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let first_tx = started_tx.clone();
        dispatch.submit_keyed(11, move || {
            first_tx.send("first").unwrap();
            release_rx.recv().unwrap();
        });
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "first"
        );
        let second_tx = started_tx.clone();
        dispatch.submit_keyed(11, move || {
            second_tx.send("second").unwrap();
        });
        dispatch.submit_keyed(12, move || {
            started_tx.send("other").unwrap();
        });
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "other"
        );
        release_tx.send(()).unwrap();
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "second"
        );
    }

    #[test]
    fn queued_same_handle_attribute_callbacks_do_not_block_unrelated_handles() {
        let dispatch = FuseDispatch::new(2);
        let (events_tx, events_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();

        let first_tx = events_tx.clone();
        dispatch.submit_keyed(21, move || {
            first_tx.send("write:start").unwrap();
            release_rx.recv().unwrap();
            first_tx.send("write:finish").unwrap();
        });
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "write:start"
        );

        let setattr_tx = events_tx.clone();
        let before_setattr_enqueue = std::time::Instant::now();
        dispatch.submit_keyed(21, move || {
            setattr_tx.send("setattr").unwrap();
        });
        assert!(
            before_setattr_enqueue.elapsed() < Duration::from_millis(100),
            "same-handle setattr enqueue blocked the FUSE receive path"
        );

        let getattr_tx = events_tx.clone();
        dispatch.submit_keyed(22, move || {
            getattr_tx.send("getattr:other-handle").unwrap();
        });
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "getattr:other-handle",
            "queued setattr for one fh blocked another fh"
        );

        release_tx.send(()).unwrap();
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "write:finish"
        );
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "setattr"
        );
    }

    #[test]
    fn official_fuser_delivery_does_not_request_lock_or_killpriv_capabilities() {
        let flags = super::requested_init_flags();
        assert!(flags.contains(fuser::InitFlags::FUSE_PARALLEL_DIROPS));
        assert!(flags.contains(fuser::InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP));
        assert!(!flags.contains(fuser::InitFlags::FUSE_POSIX_LOCKS));
        assert!(!flags.contains(fuser::InitFlags::FUSE_FLOCK_LOCKS));
        assert!(!flags.contains(fuser::InitFlags::FUSE_HANDLE_KILLPRIV));
        assert!(!flags.contains(fuser::InitFlags::FUSE_HANDLE_KILLPRIV_V2));
    }

    #[test]
    fn init_capabilities_are_requested_independently() {
        let available = fuser::InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP;
        let negotiated = super::negotiated_init_flags(available)
            .fold(fuser::InitFlags::empty(), |flags, capability| {
                flags | capability
            });
        assert!(negotiated.contains(fuser::InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP));
        assert!(!negotiated.contains(fuser::InitFlags::FUSE_PARALLEL_DIROPS));
    }

    #[test]
    fn fuser_lock_callbacks_reject_user_space_locking_explicitly() {
        assert_eq!(
            super::unsupported_lock_errno(0, 99, libc::F_RDLCK),
            libc::EOPNOTSUPP
        );
        assert_eq!(
            super::unsupported_lock_errno(10, 9, libc::F_WRLCK),
            libc::EINVAL
        );
        assert_eq!(super::unsupported_lock_errno(0, 9, -1), libc::EINVAL);
    }
}
