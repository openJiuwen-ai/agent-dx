//! 统一 POSIX/FUSE 入口。
//!
//! 接收内核回调，转换到 VFS `Backend` 接口并映射 errno/回复；管理 FUSE 会话内的
//! inode、目录项和打开句柄。每个 mount session 绑定一个 Backend；FUSE inode 编号
//! 不能当作后端持久文件身份。OwnerFs 的独用 Home 根可短暂缓存；首次远端
//! 访问先通过 FUSE notifier 失效本机缓存，再由 Home 执行该请求。

mod front_write;
#[cfg(feature = "dfs")]
mod read_pipeline;
mod state;
use crate::node::api::usrbio::FdBridge;
use afs_transport::usrbio_fd as fd_wire;

use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
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
use afs_tracing::tracing;
use fuser::{
    AccessFlags, BackgroundSession, Config, CopyFileRangeFlags, Errno, FileAttr,
    FileHandle as FuseFileHandle, FileType, Filesystem, FopenFlags, Generation, INodeNo, InitFlags,
    IoctlFlags, KernelConfig, LockOwner, MountOption, OpenFlags, RenameFlags as FuseRenameFlags,
    ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyIoctl,
    ReplyLock, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, SessionACL, TimeOrNow,
    WriteFlags,
};
use tokio::sync::RwLock;

#[cfg(feature = "ownerfs")]
use crate::node::vfs::ownerfs::OwnerFs;
use crate::node::vfs::{
    Backend, PageCacheIdentity, WriteAdmissionMode,
    types::{
        AttributeChange, BackendInode, DirectoryEntry, Entry, FileAttributes, FileHandle, FileKind,
        FileLockOwner, FilesystemCapacity, OpenOptions, ReleaseKind, RenameFlags, RequestContext,
        SetAttrOptions, SpecialFileKind, SyncMode, WriteOptions, is_legacy_privilege_clear,
    },
};

use self::state::{FuseNode, FuseState, ROOT_INO};

const TTL: Duration = Duration::ZERO;
const DIRECT_IO: FopenFlags = FopenFlags::FOPEN_DIRECT_IO;
const MAX_INLINE_LOCAL_READ_SCRATCH: usize = 1024 * 1024;
const PAGE_CACHE_IDENTITIES: usize = 65_536;
const PAGE_CACHE_VERSION_BYTES: usize = 1024 * 1024;
const PAGE_CACHE_INODE_GATES: usize = 64;
const MAX_OWNED_WRITE_INPUT_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrontOrdering {
    WriteAdmission,
    FixedLocalReply,
    WriteBarrier,
}

fn ordinary_queued_write(flags: OpenFlags, write_flags: WriteFlags) -> bool {
    flags.0 & (libc::O_APPEND | libc::O_SYNC | libc::O_DSYNC) == 0
        && !write_flags.contains(WriteFlags::FUSE_WRITE_CACHE)
}

fn xattr_front_ordering(backend: &dyn Backend, name: &OsStr) -> FrontOrdering {
    if backend.getxattr_is_fixed_local(name) {
        FrontOrdering::FixedLocalReply
    } else {
        FrontOrdering::WriteBarrier
    }
}

/// Explicit execution and DFS cache policy for one mounted FUSE session.
/// OwnerFs consumes execution settings only; its private cache policy remains
/// owned by OwnerFs. Nonzero DFS TTLs intentionally relax remote freshness.
#[derive(Clone, Copy, Debug)]
pub struct FuseMountOptions {
    pub fuse_threads: u16,
    pub fuse_clone_fd: bool,
    pub fuse_workers: u16,
    pub fuse_max_tasks: Option<usize>,
    pub dfs_read_pipeline: crate::node::read_pipeline::ReadPipelineLimits,
    pub fuse_max_readahead_bytes: u32,
    /// Front-owned normal ordinary-write payload; downstream buffers are separate.
    pub dfs_write_queue_bytes: u64,
}

impl Default for FuseMountOptions {
    fn default() -> Self {
        Self {
            fuse_threads: 4,
            fuse_clone_fd: true,
            fuse_workers: 32,
            fuse_max_tasks: None,
            dfs_read_pipeline: Default::default(),
            fuse_max_readahead_bytes: 16 * 1024 * 1024,
            dfs_write_queue_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

impl From<&crate::config::Config> for FuseMountOptions {
    fn from(config: &crate::config::Config) -> Self {
        Self {
            fuse_threads: config.fuse_threads,
            fuse_clone_fd: config.fuse_clone_fd,
            fuse_workers: config.fuse_workers,
            fuse_max_tasks: Some(config.fuse_max_tasks),
            dfs_read_pipeline: config.dfs_read_pipeline,
            fuse_max_readahead_bytes: config.fuse_max_readahead_bytes,
            dfs_write_queue_bytes: config.dfs_write_queue_bytes,
        }
    }
}

impl FuseMountOptions {
    fn task_capacity(self) -> usize {
        self.fuse_max_tasks
            .unwrap_or(usize::from(self.fuse_workers) * 8)
    }

    fn validate(self) -> Result<Self> {
        self.dfs_read_pipeline.validate()?;
        if !(1..=64).contains(&self.fuse_threads)
            || !(1..=1024).contains(&self.fuse_workers)
            || self
                .fuse_max_tasks
                .is_some_and(|n| !(1..=65_536).contains(&n))
            || self.fuse_max_readahead_bytes > 16 * 1024 * 1024
            || !(8 * 1024 * 1024..=1024 * 1024 * 1024 * 1024).contains(&self.dfs_write_queue_bytes)
            || usize::try_from(self.dfs_write_queue_bytes).is_err()
        {
            return Err(Error::coded(
                afs_error::CONFIG_INVALID,
                "FUSE mount options exceed supported bounds",
            ));
        }
        Ok(self)
    }
}

#[derive(Default)]
struct PageCacheIdentities {
    entries: HashMap<u64, (PageCacheIdentity, u64)>,
    version_bytes: usize,
    clock: u64,
}

impl PageCacheIdentities {
    fn remove(&mut self, ino: u64) {
        if let Some((identity, _)) = self.entries.remove(&ino) {
            self.version_bytes -= identity.version_id.as_ref().map_or(0, String::len);
        }
    }

    fn observe(&mut self, ino: u64, identity: Option<PageCacheIdentity>) -> bool {
        let Some(identity) = identity else {
            self.remove(ino);
            return false;
        };
        let bytes = identity.version_id.as_ref().map_or(0, String::len);
        if bytes > PAGE_CACHE_VERSION_BYTES
            || (identity.version_id.is_none() && identity.length != 0)
        {
            self.remove(ino);
            return false;
        }
        let keep = self
            .entries
            .get(&ino)
            .is_some_and(|(previous, _)| previous == &identity);
        self.remove(ino);
        while self.entries.len() >= PAGE_CACHE_IDENTITIES
            || self.version_bytes.saturating_add(bytes) > PAGE_CACHE_VERSION_BYTES
        {
            let need_entry = self.entries.len() >= PAGE_CACHE_IDENTITIES;
            let Some(oldest) = self
                .entries
                .iter()
                .filter(|(_, (entry, _))| {
                    need_entry || entry.version_id.as_ref().is_some_and(|id| !id.is_empty())
                })
                .min_by_key(|(_, (_, age))| *age)
                .map(|(ino, _)| *ino)
            else {
                break;
            };
            self.remove(oldest);
        }
        if self.clock == u64::MAX {
            // Forgetting an optimization is safe; wrapping an LRU age is not.
            self.entries.clear();
            self.version_bytes = 0;
            self.clock = 0;
        }
        self.clock += 1;
        self.version_bytes += bytes;
        self.entries.insert(ino, (identity, self.clock));
        keep
    }
}

/// One kernel inode has one data cache. Concurrent fixed-open views that
/// disagree must use direct IO instead of filling that cache with old bytes.
/// This is an admission table for live handles, not another payload cache.
#[derive(Default)]
struct ActiveCachedViews {
    inodes: HashMap<u64, (PageCacheIdentity, std::collections::HashSet<u64>)>,
    handles: usize,
    version_bytes: usize,
}

impl ActiveCachedViews {
    fn admit(&mut self, ino: u64, fh: u64, identity: &PageCacheIdentity) -> bool {
        if self.handles >= PAGE_CACHE_IDENTITIES {
            return false;
        }
        if let Some((previous, handles)) = self.inodes.get_mut(&ino) {
            if previous != identity {
                return false;
            }
            self.handles += usize::from(handles.insert(fh));
            return true;
        }
        let bytes = identity.version_id.as_ref().map_or(0, String::len);
        if (identity.version_id.is_none() && identity.length != 0)
            || bytes > PAGE_CACHE_VERSION_BYTES.saturating_sub(self.version_bytes)
        {
            return false;
        }
        self.inodes.insert(ino, (identity.clone(), [fh].into()));
        self.handles += 1;
        self.version_bytes += bytes;
        true
    }

    fn release(&mut self, ino: u64, fh: u64) {
        if let Some((_, handles)) = self.inodes.get_mut(&ino) {
            self.handles -= usize::from(handles.remove(&fh));
            if handles.is_empty()
                && let Some((identity, _)) = self.inodes.remove(&ino)
            {
                self.version_bytes -= identity.version_id.as_ref().map_or(0, String::len);
            }
        }
    }
}

// Called only after creating this DFS mount. Never tune a source disk or
// global BDI. INIT negotiation and Linux's effective BDI limit are independent.
#[cfg(target_os = "linux")]
fn configure_mount_readahead(path: &Path, requested: u32) {
    use std::os::unix::fs::MetadataExt;
    let outcome = (|| -> io::Result<(PathBuf, u64, u64)> {
        let dev = std::fs::metadata(path)?.dev();
        let setting = PathBuf::from(format!(
            "/sys/class/bdi/{}:{}/read_ahead_kb",
            libc::major(dev),
            libc::minor(dev)
        ));
        let (before, effective) = update_bdi_readahead(&setting, requested)?;
        Ok((setting, before, effective))
    })();
    match outcome {
        Ok((setting, before, effective)) => {
            afs_logging::info!("dfs.fuse_readahead"; "mount" => path.display().to_string(), "setting" => setting.display().to_string(), "requested_bytes" => requested, "initial_kib" => before, "effective_kib" => effective, "applied" => true);
        }
        Err(error) => {
            // Correct reads remain available with the kernel's existing limit.
            // Operators must not interpret a requested value as applied.
            afs_logging::warn!("dfs.fuse_readahead"; "mount" => path.display().to_string(), "requested_bytes" => requested, "applied" => false, "error" => error.to_string());
        }
    }
}

#[cfg(target_os = "linux")]
fn update_bdi_readahead(setting: &Path, requested: u32) -> io::Result<(u64, u64)> {
    let read = || -> io::Result<u64> {
        std::fs::read_to_string(setting)?
            .trim()
            .parse()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    };
    let before = read()?;
    let target = u64::from(requested) / 1024;
    std::fs::write(setting, format!("{target}\n"))?;
    let effective = read()?;
    if effective != target {
        return Err(io::Error::other(format!(
            "BDI read-ahead requested {target}KiB but effective {effective}KiB"
        )));
    }
    Ok((before, effective))
}

fn configure_readahead(
    requested: u32,
    mut set: impl FnMut(u32) -> std::result::Result<u32, u32>,
) -> io::Result<()> {
    if let Err(nearest) = set(requested) {
        // A kernel ceiling of zero already disables readahead. Otherwise the
        // public fuser API clamps to its supported limit (minimum one byte for
        // a zero request; Linux translates that to zero whole readahead pages).
        if nearest != 0 {
            match set(nearest) {
                Ok(_) | Err(0) => {}
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "kernel rejected supported FUSE readahead limit",
                    ));
                }
            }
        }
    }
    Ok(())
}

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

fn backend_read_data(
    backend: &dyn Backend,
    ctx: &RequestContext,
    handle: FileHandle,
    offset: u64,
    size: usize,
) -> Result<prost::bytes::Bytes> {
    backend.read_reply(ctx, handle, offset, size)
}

// Keep the diagnostic alive through the actual reply operation. Ending it at
// Filesystem::read return would lose queued worker time and the reply itself.
fn complete_read_reply(
    #[cfg(any(feature = "dfs", test))] diagnostic: Option<
        crate::node::read_work::ReadDiagnosticSpan,
    >,
    #[cfg(any(feature = "dfs", test))] success: bool,
    reply: impl FnOnce(),
) {
    reply();
    #[cfg(any(feature = "dfs", test))]
    if let Some(diagnostic) = diagnostic {
        diagnostic.finish(success);
    }
}

/// Implemented first-party FUSE callback entries, including failed requests.
/// This is neither kernel wire opcode accounting nor application syscall accounting.
/// Read/write size histograms observe callback input bytes, not completed payloads.
#[derive(Clone)]
pub struct FuseRequestMetrics {
    requests: afs_metrics::IntCounterVec,
    io_size_bytes: afs_metrics::HistogramVec,
    copy_range_duration: afs_metrics::HistogramVec,
    write_duration: afs_metrics::HistogramVec,
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
            let io_size_bytes = afs_metrics::HistogramVec::new(
                afs_metrics::HistogramOpts::new(
                    "afs_fuse_callback_size_bytes",
                    "Requested read and supplied write bytes at FUSE callback entry, including failures; not application syscall or completed payload sizes.",
                )
                .buckets(vec![4096.0, 65_536.0, 262_144.0, 1_048_576.0]),
                &["filesystem", "operation"],
            )?;
            afs_metrics::register_collector(registry, &requests)?;
            afs_metrics::register_collector(registry, &io_size_bytes)?;
            let copy_range_duration = afs_metrics::HistogramVec::new(
                afs_metrics::HistogramOpts::new(
                    "afs_fuse_copy_file_range_duration_seconds",
                    "Dispatched copy_file_range queue and execution wall time.",
                ),
                &["filesystem", "stage"],
            )?;
            afs_metrics::register_collector(registry, &copy_range_duration)?;
            let write_duration = afs_metrics::HistogramVec::new(
                afs_metrics::HistogramOpts::new(
                    "afs_fuse_write_duration_seconds",
                    "Dispatched write admission queue and backend execution wall time, including failures.",
                ).buckets(vec![0.0001, 0.001, 0.01, 0.1, 1.0, 5.0, 30.0, 60.0]),
                &["filesystem", "source", "stage"],
            )?;
            afs_metrics::register_collector(registry, &write_duration)?;
            for filesystem in ["ownerfs", "dfs"] {
                for source in ["page_cache", "write_call"] {
                    for stage in ["queue", "execute"] {
                        write_duration.with_label_values(&[filesystem, source, stage]);
                    }
                }
                for stage in ["queue", "execute"] {
                    copy_range_duration.with_label_values(&[filesystem, stage]);
                }
                for operation in FUSE_CALLBACKS {
                    requests.with_label_values(&[filesystem, operation]);
                }
                for operation in ["read", "write"] {
                    io_size_bytes.with_label_values(&[filesystem, operation]);
                }
            }
            Ok(Self { requests, io_size_bytes, copy_range_duration, write_duration })
        })
    }

    fn record(&self, filesystem: &'static str, operation: &'static str) {
        self.requests
            .with_label_values(&[filesystem, operation])
            .inc();
    }

    // Observe only the scalar callback size: no payload access, timing, or
    // per-request labels. Prometheus supplies the cumulative +Inf bucket.
    fn record_io(&self, filesystem: &'static str, operation: &'static str, bytes: usize) {
        self.record(filesystem, operation);
        self.io_size_bytes
            .with_label_values(&[filesystem, operation])
            .observe(bytes as f64);
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
    "copy_file_range",
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

fn canonical_mount_path(path: &Path) -> Result<PathBuf> {
    path.canonicalize().map_err(Error::from)
}

impl MountedFuse {
    fn new(
        session: BackgroundSession,
        path: &Path,
        cleanup_error: Arc<Mutex<Option<Error>>>,
    ) -> Result<Self> {
        let canonical_path = canonical_mount_path(path)?;
        match current_mount_id(path).and_then(|id| {
            id.ok_or_else(|| Error::from(io::Error::other("new FUSE mount identity is missing")))
        }) {
            Ok(mount_id) => Ok(Self {
                session,
                mount_path: canonical_path,
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

    /// Canonical path and kernel mount ID captured by the mount owner.
    /// Readiness must reuse these values rather than interpreting config again.
    #[must_use]
    pub fn identity(&self) -> (&Path, u64) {
        (&self.mount_path, self.mount_id)
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
    mount_dfs_with_options(backend, path, FuseMountOptions::default())
}

/// Mount DFS with explicit validated execution/cache settings.
pub fn mount_dfs_with_options(
    backend: Arc<dyn Backend>,
    path: &Path,
    options: FuseMountOptions,
) -> Result<MountedFuse> {
    mount_dfs_inner(
        backend,
        path,
        None,
        #[cfg(any(feature = "dfs", test))]
        None,
        options,
        None,
    )
}

pub fn mount_dfs_with_metrics_and_options(
    backend: Arc<dyn Backend>,
    path: &Path,
    metrics: FuseRequestMetrics,
    options: FuseMountOptions,
) -> Result<MountedFuse> {
    mount_dfs_inner(
        backend,
        path,
        Some(metrics),
        #[cfg(any(feature = "dfs", test))]
        None,
        options,
        None,
    )
}

/// Mount the same Backend served by a local SDK, with existing-fd controls.
pub fn mount_dfs_with_fd_bridge(
    backend: Arc<dyn Backend>,
    path: &Path,
    options: FuseMountOptions,
    bridge: Arc<FdBridge>,
) -> Result<MountedFuse> {
    mount_dfs_inner(
        backend,
        path,
        None,
        #[cfg(any(feature = "dfs", test))]
        None,
        options,
        Some(bridge),
    )
}

#[cfg(feature = "dfs")]
pub fn mount_dfs_with_diagnostics_and_fd_bridge(
    backend: Arc<dyn Backend>,
    path: &Path,
    metrics: FuseRequestMetrics,
    read_work: crate::node::read_work::ReadWorkMetrics,
    options: FuseMountOptions,
    bridge: Arc<FdBridge>,
) -> Result<MountedFuse> {
    mount_dfs_inner(
        backend,
        path,
        Some(metrics),
        Some(read_work),
        options,
        Some(bridge),
    )
}

fn mount_dfs_inner(
    backend: Arc<dyn Backend>,
    path: &Path,
    metrics: Option<FuseRequestMetrics>,
    #[cfg(any(feature = "dfs", test))] read_work: Option<crate::node::read_work::ReadWorkMetrics>,
    options: FuseMountOptions,
    bridge: Option<Arc<FdBridge>>,
) -> Result<MountedFuse> {
    let options = options.validate()?;
    let path = canonical_mount_path(path)?;
    reject_existing_mount(&path)?;
    if let Some(bridge) = &bridge {
        bridge.bind(&backend)?;
    }
    let mut fs = AfsFuse::with_mount_options(backend, options);
    fs.fd_bridge = bridge.clone();
    fs.enable_write_queue().map_err(Error::from)?;
    fs.requests = metrics.map(|metrics| (metrics, "dfs"));
    #[cfg(any(feature = "dfs", test))]
    {
        if let Some(metrics) = &read_work {
            let _ = fs.dispatch.shared.activity.set(metrics.activities.clone());
        }
        fs.read_work = read_work;
    }
    // Cached write-through sends each syscall write to the inode owner and
    // keeps this mount's pages coherent. KEEP_CACHE requires an exact fresh
    // committed identity; writeback remains disabled.
    fs.cached_io = true;
    #[cfg(feature = "dfs")]
    fs.enable_read_pipeline()?;
    let cleanup_error = fs.cleanup_error.clone();
    let session = fuser::spawn_mount(fs, &path, &configured_mount("afs-dfs", true, options))
        .map_err(Error::from)?;
    #[cfg(target_os = "linux")]
    configure_mount_readahead(&path, options.fuse_max_readahead_bytes);
    if let Some(bridge) = bridge {
        bridge.set_notifier(session.notifier());
    }
    MountedFuse::new(session, &path, cleanup_error)
}

/// OwnerFs 使用相同 FUSE 实现，并额外接入其本地 Home 缓存策略与 notifier。
#[cfg(feature = "ownerfs")]
pub fn mount_ownerfs(ownerfs: Arc<OwnerFs>, path: &Path) -> Result<MountedFuse> {
    mount_ownerfs_inner(ownerfs, path, None, FuseMountOptions::default())
}

#[cfg(feature = "ownerfs")]
pub fn mount_ownerfs_with_metrics(
    ownerfs: Arc<OwnerFs>,
    path: &Path,
    metrics: FuseRequestMetrics,
) -> Result<MountedFuse> {
    mount_ownerfs_with_metrics_and_options(ownerfs, path, metrics, FuseMountOptions::default())
}

#[cfg(feature = "ownerfs")]
pub fn mount_ownerfs_with_metrics_and_options(
    ownerfs: Arc<OwnerFs>,
    path: &Path,
    metrics: FuseRequestMetrics,
    options: FuseMountOptions,
) -> Result<MountedFuse> {
    mount_ownerfs_inner(ownerfs, path, Some(metrics), options)
}

#[cfg(feature = "ownerfs")]
fn mount_ownerfs_inner(
    ownerfs: Arc<OwnerFs>,
    path: &Path,
    metrics: Option<FuseRequestMetrics>,
    options: FuseMountOptions,
) -> Result<MountedFuse> {
    let options = options.validate()?;
    let path = canonical_mount_path(path)?;
    reject_existing_mount(&path)?;
    let mut fs = AfsFuse::with_mount_options(ownerfs.clone(), options);
    fs.requests = metrics.map(|metrics| (metrics, "ownerfs"));
    fs.ownerfs = Some(ownerfs.clone());
    let cleanup_error = fs.cleanup_error.clone();
    let session = fuser::spawn_mount(fs, &path, &configured_mount("afs-ownerfs", true, options))
        .map_err(Error::from)?;
    ownerfs.register_fuse_notifier(session.notifier());
    MountedFuse::new(session, &path, cleanup_error)
}

#[doc(hidden)]
pub fn mount_test_backend(backend: Arc<dyn Backend>, path: &Path) -> Result<MountedFuse> {
    let path = canonical_mount_path(path)?;
    reject_existing_mount(&path)?;
    let fs = AfsFuse::new(backend);
    let cleanup_error = fs.cleanup_error.clone();
    let session =
        fuser::spawn_mount(fs, &path, &mount_config("afs-test", false)).map_err(Error::from)?;
    MountedFuse::new(session, &path, cleanup_error)
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

fn configured_mount(fs_name: &str, allow_other: bool, options: FuseMountOptions) -> Config {
    let mut config = mount_config(fs_name, allow_other);
    config.n_threads = Some(usize::from(options.fuse_threads));
    config.clone_fd = options.fuse_threads > 1 && options.fuse_clone_fd;
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

#[cfg(test)]
mod mount_path_tests;

#[derive(Clone, Copy, PartialEq, Eq)]
enum FuseAccess {
    Exclusive,
    Read,
}

#[derive(Clone, Copy)]
struct FuseTurn {
    key: u64,
    access: FuseAccess,
}

enum FuseActive {
    Exclusive,
    Readers(usize),
}

enum FuseJob {
    Owned(Box<dyn FnOnce() + Send + 'static>),
    // Nonblocking starter only: hand ownership to a future, never run IO or wait here.
    Deferred(Box<dyn FnOnce(std::result::Result<FuseCompletion, i32>) + Send + 'static>),
    Borrowed(Arc<(Mutex<bool>, Condvar)>),
}

/// Owns the admitted task and its ordering turn after the worker is returned.
/// Moving this into a future or completion event does not retire the request.
struct FuseCompletion {
    inner: Arc<FuseCompletionInner>,
}
struct FuseCompletionInner {
    shared: Arc<FuseDispatchShared>,
    turn: Option<FuseTurn>,
}
impl FuseCompletion {
    fn hold(&self) -> crate::node::read_pipeline::ReadLifetime {
        self.inner.clone()
    }
}
impl Drop for FuseCompletionInner {
    fn drop(&mut self) {
        FuseDispatch::finish(&self.shared, self.turn.take());
    }
}

// Unlike an owned worker callback, a borrowed callback runs on the receiver.
// Keep its FIFO slot active until that callback finishes, including unwinding.
struct BorrowedAdmission<'a> {
    dispatch: &'a FuseDispatch,
    key: u64,
}

impl Drop for BorrowedAdmission<'_> {
    fn drop(&mut self) {
        FuseDispatch::finish(
            &self.dispatch.shared,
            Some(FuseTurn {
                key: self.key,
                access: FuseAccess::Exclusive,
            }),
        );
    }
}

// Independent handles and adjacent eligible reads may run concurrently. A
// queued exclusive callback fences later reads through fsync and release. Capacity waits backpressure only
// receive callbacks: pool jobs must not synchronously reenter this dispatcher or
// wait for the receive thread (in particular, notifier work must be separate).
struct FuseDispatch {
    shared: Arc<FuseDispatchShared>,
    capacity: usize,
}

/// A recovery continuation already owns its file/control admission. Enqueuing
/// it must not wait for the ordinary dispatcher quota from inside a completion.
struct FuseRecoverySender {
    shared: Arc<FuseDispatchShared>,
    normal_capacity: Option<usize>,
}
impl FuseRecoverySender {
    fn submit(&self, key: u64, job: impl FnOnce() + Send + 'static) {
        let mut queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while self
            .normal_capacity
            .is_some_and(|capacity| queue.jobs >= capacity)
            && !queue.closed
        {
            queue = self
                .shared
                .capacity
                .wait(queue)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if queue.closed {
            drop(queue);
            drop(job);
            return;
        }
        queue.jobs += 1;
        if let Some(metrics) = self.shared.activity.get() {
            metrics.observe_count(
                crate::node::read_activity::Activity::FuseTask,
                queue.jobs as u64,
            );
        }
        let job = FuseJob::Owned(Box::new(job));
        if let std::collections::hash_map::Entry::Vacant(entry) = queue.active.entry(key) {
            entry.insert(FuseActive::Exclusive);
            queue.ready.push_back((
                Some(FuseTurn {
                    key,
                    access: FuseAccess::Exclusive,
                }),
                job,
            ));
            self.shared.ready.notify_one();
        } else {
            queue
                .waiting
                .entry(key)
                .or_default()
                .push_back((FuseAccess::Exclusive, job));
        }
    }
}

struct FuseDispatchShared {
    activity: std::sync::OnceLock<crate::node::read_activity::Activities>,
    queue: Mutex<FuseQueue>,
    ready: Condvar,
    capacity: Condvar,
    drained: Condvar,
}

struct FuseWriteInput {
    data: Vec<u8>,
    shared: Arc<FuseDispatchShared>,
}

impl FuseWriteInput {
    fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

impl Drop for FuseWriteInput {
    fn drop(&mut self) {
        let bytes = self.data.len();
        drop(std::mem::take(&mut self.data));
        if let Ok(mut queue) = self.shared.queue.lock() {
            queue.write_input_bytes = queue.write_input_bytes.saturating_sub(bytes);
        }
        self.shared.capacity.notify_all();
    }
}

struct FuseQueue {
    ready: VecDeque<(Option<FuseTurn>, FuseJob)>,
    waiting: HashMap<u64, VecDeque<(FuseAccess, FuseJob)>>,
    active: HashMap<u64, FuseActive>,
    jobs: usize,
    write_input_bytes: usize,
    closed: bool,
}

impl FuseDispatch {
    #[cfg(test)]
    fn new(workers: usize) -> Self {
        Self::with_capacity(workers, workers * 8)
    }

    fn with_capacity(workers: usize, capacity: usize) -> Self {
        let shared = Arc::new(FuseDispatchShared {
            activity: std::sync::OnceLock::new(),
            queue: Mutex::new(FuseQueue {
                ready: VecDeque::new(),
                waiting: HashMap::new(),
                active: HashMap::new(),
                jobs: 0,
                write_input_bytes: 0,
                closed: false,
            }),
            ready: Condvar::new(),
            capacity: Condvar::new(),
            drained: Condvar::new(),
        });
        for index in 0..workers {
            let shared = shared.clone();
            thread::Builder::new()
                .name(format!("afs-fuse-{index}"))
                .spawn(move || {
                    loop {
                        let (key, job) = {
                            let mut queue = shared.queue.lock().unwrap();
                            while queue.ready.is_empty() {
                                if queue.closed && queue.jobs == 0 {
                                    return;
                                }
                                queue = shared.ready.wait(queue).unwrap();
                            }
                            let Some(job) = queue.ready.pop_front() else {
                                break;
                            };
                            job
                        };
                        match job {
                            FuseJob::Owned(job) => {
                                if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job))
                                    .is_err()
                                {
                                    afs_logging::error!("FUSE worker callback panicked");
                                }
                                Self::finish(&shared, key);
                            }
                            FuseJob::Deferred(job) => {
                                Self::start_deferred(&shared, key, job);
                            }

                            FuseJob::Borrowed(turn) => {
                                let (started, wake) = &*turn;
                                *started.lock().expect("borrowed FUSE turn mutex poisoned") = true;
                                wake.notify_one();
                                // The receiver retires this slot. Do not occupy a
                                // worker while it copies or executes a sync trigger.
                            }
                        }
                    }
                })
                .expect("FUSE worker thread must start");
        }
        Self { shared, capacity }
    }

    fn start_deferred(
        shared: &Arc<FuseDispatchShared>,
        turn: Option<FuseTurn>,
        starter: impl FnOnce(std::result::Result<FuseCompletion, i32>),
    ) {
        let completion = FuseCompletion {
            inner: Arc::new(FuseCompletionInner {
                shared: shared.clone(),
                turn,
            }),
        };
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| starter(Ok(completion))))
            .is_err()
        {
            afs_logging::error!("FUSE deferred callback panicked");
        }
    }

    fn submit(&self, job: impl FnOnce() + Send + 'static) {
        let mut queue = self.shared.queue.lock().unwrap();
        while queue.jobs >= self.capacity && !queue.closed {
            queue = self.shared.capacity.wait(queue).unwrap();
        }
        if queue.closed {
            drop(queue);
            // Not admitted: drop the captured fuser reply to send EIO. Running
            // backend work here would escape the completed shutdown drain.
            drop(job);
            return;
        }
        queue.jobs += 1;
        if let Some(metrics) = self.shared.activity.get() {
            metrics.observe_count(
                crate::node::read_activity::Activity::FuseTask,
                queue.jobs as u64,
            );
        }
        queue.ready.push_back((None, FuseJob::Owned(Box::new(job))));
        self.shared.ready.notify_one();
    }

    fn copy_write_input(&self, data: &[u8]) -> std::result::Result<FuseWriteInput, i32> {
        if data.len() > MAX_OWNED_WRITE_INPUT_BYTES {
            return Err(libc::E2BIG);
        }
        let mut queue = self.shared.queue.lock().map_err(|_| libc::EIO)?;
        while data.len() > MAX_OWNED_WRITE_INPUT_BYTES - queue.write_input_bytes && !queue.closed {
            queue = self.shared.capacity.wait(queue).map_err(|_| libc::EIO)?;
        }
        if queue.closed {
            return Err(libc::EIO);
        }
        queue.write_input_bytes += data.len();
        drop(queue);
        Ok(FuseWriteInput {
            data: data.to_vec(),
            shared: self.shared.clone(),
        })
    }

    fn submit_keyed(&self, key: u64, job: impl FnOnce() + Send + 'static) {
        self.submit_access(key, FuseAccess::Exclusive, job);
    }

    fn submit_read_keyed(&self, key: u64, job: impl FnOnce() + Send + 'static) {
        self.submit_access(key, FuseAccess::Read, job);
    }

    #[cfg(test)]
    fn submit_read_deferred(&self, key: u64, job: impl FnOnce(FuseCompletion) + Send + 'static) {
        self.submit_job(
            key,
            FuseAccess::Read,
            FuseJob::Deferred(Box::new(move |result| {
                if let Ok(completion) = result {
                    job(completion);
                }
            })),
        );
    }

    fn submit_access(&self, key: u64, access: FuseAccess, job: impl FnOnce() + Send + 'static) {
        self.submit_job(key, access, FuseJob::Owned(Box::new(job)));
    }

    fn submit_read_deferred_until(
        &self,
        key: u64,
        deadline: std::time::Instant,
        job: impl FnOnce(std::result::Result<FuseCompletion, i32>) + Send + 'static,
    ) {
        if let Err((error, job)) = self.submit_job_until(
            key,
            FuseAccess::Read,
            FuseJob::Deferred(Box::new(job)),
            Some(deadline),
        ) && let FuseJob::Deferred(job) = job
        {
            job(Err(error));
        }
    }

    fn submit_job(&self, key: u64, access: FuseAccess, job: FuseJob) {
        if let Err((_, job)) = self.submit_job_until(key, access, job, None) {
            drop(job);
        }
    }

    fn submit_job_until(
        &self,
        key: u64,
        access: FuseAccess,
        job: FuseJob,
        deadline: Option<std::time::Instant>,
    ) -> std::result::Result<(), (i32, FuseJob)> {
        let mut queue = match self.shared.queue.lock() {
            Ok(queue) => queue,
            Err(_) => return Err((libc::EIO, job)),
        };
        while queue.jobs >= self.capacity && !queue.closed {
            queue = if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return Err((libc::ETIMEDOUT, job));
                }
                match self.shared.capacity.wait_timeout(queue, remaining) {
                    Ok((queue, _)) => queue,
                    Err(_) => return Err((libc::EIO, job)),
                }
            } else {
                match self.shared.capacity.wait(queue) {
                    Ok(queue) => queue,
                    Err(_) => return Err((libc::EIO, job)),
                }
            };
        }
        if queue.closed {
            drop(queue);
            return Err((libc::EIO, job));
        }
        if deadline.is_some_and(|value| std::time::Instant::now() >= value) {
            return Err((libc::ETIMEDOUT, job));
        }
        queue.jobs += 1;
        if let Some(metrics) = self.shared.activity.get() {
            metrics.observe_count(
                crate::node::read_activity::Activity::FuseTask,
                queue.jobs as u64,
            );
        }
        // Readers can join only before an exclusive turn has queued. Counting
        // ready readers as active preserves the barrier before they execute.
        let join_reads = access == FuseAccess::Read
            && !queue.waiting.contains_key(&key)
            && matches!(queue.active.get(&key), Some(FuseActive::Readers(_)));
        if !queue.active.contains_key(&key) || join_reads {
            match queue.active.entry(key) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(match access {
                        FuseAccess::Exclusive => FuseActive::Exclusive,
                        FuseAccess::Read => FuseActive::Readers(1),
                    });
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let FuseActive::Readers(readers) = entry.get_mut() else {
                        unreachable!("only a read batch can be joined");
                    };
                    *readers += 1;
                }
            }
            let turn = Some(FuseTurn { key, access });
            match job {
                FuseJob::Deferred(starter) => {
                    // Already eligible at admission: starting the owned future
                    // needs no worker hop. Keep its counted task/order turn,
                    // and release the queue lock before a starter can finish.
                    drop(queue);
                    Self::start_deferred(&self.shared, turn, starter);
                    return Ok(());
                }
                job => queue.ready.push_back((turn, job)),
            }
            self.shared.ready.notify_one();
        } else {
            queue
                .waiting
                .entry(key)
                .or_default()
                .push_back((access, job));
        }
        Ok(())
    }

    fn finish(shared: &FuseDispatchShared, turn: Option<FuseTurn>) {
        let mut queue = shared
            .queue
            .lock()
            .expect("FUSE queue mutex poisoned at completion");
        queue.jobs -= 1;
        if let Some(metrics) = shared.activity.get() {
            metrics.observe_count(
                crate::node::read_activity::Activity::FuseTask,
                queue.jobs as u64,
            );
        }
        if let Some(FuseTurn { key, access }) = turn {
            let finished = match (access, queue.active.get_mut(&key)) {
                (FuseAccess::Read, Some(FuseActive::Readers(readers))) => {
                    *readers -= 1;
                    *readers == 0
                }
                (FuseAccess::Exclusive, Some(FuseActive::Exclusive)) => true,
                _ => unreachable!("completed FUSE turn must own the active access"),
            };
            if finished {
                queue.active.remove(&key);
                if let Some(mut waiting) = queue.waiting.remove(&key) {
                    let mut readers = 0;
                    while let Some((next_access, _)) = waiting.front() {
                        if readers != 0 && *next_access == FuseAccess::Exclusive {
                            break;
                        }
                        let (next_access, next) = waiting.pop_front().unwrap();
                        queue.ready.push_back((
                            Some(FuseTurn {
                                key,
                                access: next_access,
                            }),
                            next,
                        ));
                        shared.ready.notify_one();
                        if next_access == FuseAccess::Exclusive {
                            queue.active.insert(key, FuseActive::Exclusive);
                            break;
                        }
                        readers += 1;
                    }
                    if readers != 0 {
                        queue.active.insert(key, FuseActive::Readers(readers));
                    }
                    if !waiting.is_empty() {
                        queue.waiting.insert(key, waiting);
                    }
                }
            }
        }
        shared.capacity.notify_one();
        shared.drained.notify_all();
        if queue.jobs == 0 {
            shared.drained.notify_all();
            if queue.closed {
                shared.ready.notify_all();
            }
        }
    }

    /// Wait for this handle's FIFO turn, then execute without owning or copying
    /// its input. Only a bounded wakeup marker crosses to a worker. This blocks
    /// the receive callback, but holds no queue/state lock during backend I/O.
    fn run_borrowed_keyed(&self, key: u64, job: impl FnOnce()) {
        let mut queue = self
            .shared
            .queue
            .lock()
            .expect("FUSE queue mutex poisoned at admission");
        while queue.jobs >= self.capacity && !queue.closed {
            queue = self
                .shared
                .capacity
                .wait(queue)
                .expect("FUSE admission wait poisoned");
        }
        if queue.closed {
            drop(queue);
            drop(job);
            return;
        }
        queue.jobs += 1;
        if let Some(metrics) = self.shared.activity.get() {
            metrics.observe_count(
                crate::node::read_activity::Activity::FuseTask,
                queue.jobs as u64,
            );
        }
        if let std::collections::hash_map::Entry::Vacant(entry) = queue.active.entry(key) {
            entry.insert(FuseActive::Exclusive);
            drop(queue);
            let admission = BorrowedAdmission {
                dispatch: self,
                key,
            };
            job();
            drop(admission);
            return;
        }
        let turn = Arc::new((Mutex::new(false), Condvar::new()));
        queue
            .waiting
            .entry(key)
            .or_default()
            .push_back((FuseAccess::Exclusive, FuseJob::Borrowed(turn.clone())));
        drop(queue);
        let admission = BorrowedAdmission {
            dispatch: self,
            key,
        };
        let (started, wake) = &*turn;
        let mut started = started.lock().expect("borrowed FUSE turn mutex poisoned");
        while !*started {
            started = wake
                .wait(started)
                .expect("borrowed FUSE turn wait poisoned");
        }
        drop(started);
        job();
        drop(admission);
    }

    fn drain(&self) {
        let mut state = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.jobs != 0 {
            state = self
                .shared
                .drained
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn drain_key(&self, key: u64) {
        let mut state = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.active.contains_key(&key) || state.waiting.contains_key(&key) {
            state = self
                .shared
                .drained
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn close_and_drain(&self) {
        let mut state = self.shared.queue.lock().unwrap();
        state.closed = true;
        self.shared.capacity.notify_all();
        self.shared.ready.notify_all();
        // Already admitted workers and borrowed receiver turns retain callbacks.
        // Closing rejects capacity waiters; it never executes them out of order.
        // The Node process deadline covers a blocked accepted callback.
        while state.jobs != 0 {
            state = self.shared.drained.wait(state).unwrap();
        }
    }
}

impl Drop for FuseDispatch {
    fn drop(&mut self) {
        self.close_and_drain();
    }
}

/// Short shard-table locks only protect gate identity and reference retirement.
/// The per-inode lock, never the table lock, spans backend work and FUSE reply.
type GateShard = Arc<Mutex<HashMap<u64, Arc<RwLock<()>>>>>;

struct InodeCacheGates {
    shards: [GateShard; PAGE_CACHE_INODE_GATES],
}

impl Default for InodeCacheGates {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| Arc::new(Mutex::new(HashMap::new()))),
        }
    }
}

impl InodeCacheGates {
    fn acquire(&self, ino: u64) -> std::result::Result<InodeCacheGate, i32> {
        let shard = &self.shards[(ino % PAGE_CACHE_INODE_GATES as u64) as usize];
        let mut table = shard.lock().map_err(|_| libc::EIO)?;
        let gate = table
            .entry(ino)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone();
        Ok(InodeCacheGate {
            shard: shard.clone(),
            ino,
            gate: Some(gate),
        })
    }
}

/// Each lease owns one reference outside the table. It cannot be cloned:
/// acquisition and final retirement must both serialize through the table lock.
struct InodeCacheGate {
    shard: GateShard,
    ino: u64,
    gate: Option<Arc<RwLock<()>>>,
}

impl InodeCacheGate {
    fn read(&self) -> std::result::Result<tokio::sync::RwLockReadGuard<'_, ()>, i32> {
        let gate = self.gate.as_ref().ok_or(libc::EIO)?;
        Ok(match gate.try_read() {
            Ok(guard) => guard,
            Err(_) => gate.blocking_read(),
        })
    }

    fn write(&self) -> std::result::Result<tokio::sync::RwLockWriteGuard<'_, ()>, i32> {
        let gate = self.gate.as_ref().ok_or(libc::EIO)?;
        Ok(match gate.try_write() {
            Ok(guard) => guard,
            Err(_) => gate.blocking_write(),
        })
    }

    async fn read_owned_until(
        &self,
        deadline: std::time::Instant,
    ) -> std::result::Result<tokio::sync::OwnedRwLockReadGuard<()>, i32> {
        let gate = self.gate.as_ref().ok_or(libc::EIO)?.clone();
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), gate.read_owned())
            .await
            .map_err(|_| libc::ETIMEDOUT)
    }
}

impl Drop for InodeCacheGate {
    fn drop(&mut self) {
        let Some(gate) = self.gate.take() else {
            return;
        };
        // Even a poisoned optimization table must release leases. No backend
        // state is protected here, and acquire still fails closed on poison.
        let mut table = self
            .shard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let same_gate = table
            .get(&self.ino)
            .is_some_and(|current| Arc::ptr_eq(current, &gate));
        // Release this lease while holding the acquisition lock. Concurrent
        // last drops cannot both observe each other's still-live reference and
        // leave an idle entry forever. A waiting acquire cannot split identity.
        drop(gate);
        if same_gate
            && table
                .get(&self.ino)
                .is_some_and(|current| Arc::strong_count(current) == 1)
        {
            table.remove(&self.ino);
        }
    }
}

pub struct AfsFuse {
    fd_bridge: Option<Arc<FdBridge>>,
    #[cfg(any(feature = "dfs", test))]
    read_work: Option<crate::node::read_work::ReadWorkMetrics>,
    requests: Option<(FuseRequestMetrics, &'static str)>,
    backend: Arc<dyn Backend>,
    cached_io: bool,
    mount_options: FuseMountOptions,
    page_cache_ids: Arc<Mutex<PageCacheIdentities>>,
    active_cached_views: Arc<Mutex<ActiveCachedViews>>,
    inode_cache_gates: Arc<InodeCacheGates>,
    inline_local_read_scratch: Mutex<Vec<u8>>,
    state: Arc<Mutex<FuseState>>,
    file_handle_inodes: Arc<Mutex<HashMap<u64, u64>>>,
    dispatch: FuseDispatch,
    front_write: Option<Arc<front_write::FrontWriteQueue>>,
    #[cfg(feature = "dfs")]
    read_runtime: Option<tokio::runtime::Handle>,
    #[cfg(feature = "dfs")]
    read_completions: Option<read_pipeline::ReadCompletions>,
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
        Self::with_mount_options(
            backend,
            FuseMountOptions {
                fuse_workers: 8,
                fuse_max_tasks: Some(64),
                ..FuseMountOptions::default()
            },
        )
    }

    fn with_mount_options(backend: Arc<dyn Backend>, mount_options: FuseMountOptions) -> Self {
        let state = Arc::new(Mutex::new(FuseState::new(backend.root_inode())));
        Self {
            fd_bridge: None,
            #[cfg(any(feature = "dfs", test))]
            read_work: None,
            requests: None,
            backend,
            cached_io: false,
            mount_options,
            page_cache_ids: Arc::new(Mutex::new(PageCacheIdentities::default())),
            active_cached_views: Arc::new(Mutex::new(ActiveCachedViews::default())),
            inode_cache_gates: Arc::new(InodeCacheGates::default()),
            inline_local_read_scratch: Mutex::new(Vec::new()),
            state,
            file_handle_inodes: Arc::new(Mutex::new(HashMap::new())),
            dispatch: FuseDispatch::with_capacity(
                usize::from(mount_options.fuse_workers),
                mount_options.task_capacity(),
            ),
            #[cfg(feature = "dfs")]
            read_runtime: None,
            #[cfg(feature = "dfs")]
            read_completions: None,
            front_write: None,
            ingress_session_id: next_ingress_session_id(),
            lock_session_cleaned: false,
            callbacks_drained: false,
            cleanup_error: Arc::new(Mutex::new(None)),
            #[cfg(feature = "ownerfs")]
            ownerfs: None,
        }
    }

    fn enable_write_queue(&mut self) -> io::Result<()> {
        let bytes = usize::try_from(self.mount_options.dfs_write_queue_bytes)
            .map_err(|_| io::Error::from_raw_os_error(libc::EINVAL))?;
        self.front_write = Some(Arc::new(
            front_write::FrontWriteQueue::new(bytes).map_err(io::Error::from_raw_os_error)?,
        ));
        Ok(())
    }

    /// The gate orders callback registration/execution for the addressed files.
    /// Body quota waits retain only an owned entry, never this callback gate.
    fn front_callback(
        &self,
        ordering: FrontOrdering,
        inode: Option<u64>,
        retry: bool,
    ) -> (
        Option<front_write::CallbackGuard>,
        std::result::Result<(), i32>,
    ) {
        let files = inode.map(|inode| vec![inode]);
        self.front_callbacks(ordering, files.as_deref(), retry)
    }

    fn front_callbacks(
        &self,
        ordering: FrontOrdering,
        files: Option<&[u64]>,
        retry: bool,
    ) -> (
        Option<front_write::CallbackGuard>,
        std::result::Result<(), i32>,
    ) {
        let Some(queue) = &self.front_write else {
            return (None, Ok(()));
        };
        if ordering == FrontOrdering::FixedLocalReply {
            return (None, Ok(()));
        }
        let guard = match queue.enter(files, retry) {
            Ok(guard) => guard,
            Err(error) => return (None, Err(error)),
        };
        let mut result = Ok(());
        if ordering == FrontOrdering::WriteBarrier {
            if let Some(files) = files {
                for file in files {
                    if let Err(error) = queue.wait(Some(*file), retry) {
                        result = Err(error);
                        break;
                    }
                }
            } else {
                result = queue.wait(None, false);
            }
        }
        if let Some(files) = files {
            if ordering == FrontOrdering::WriteAdmission || retry {
                for file in files {
                    self.dispatch.drain_key(*file);
                }
            }
        } else {
            self.dispatch.drain();
        }
        (Some(guard), result)
    }

    fn file_dispatch_key(&self, inode: u64, handle: u64) -> u64 {
        if self.front_write.is_some() {
            inode
        } else {
            handle
        }
    }

    fn submit_file_metadata(&self, inode: u64, scoped: bool, job: impl FnOnce() + Send + 'static) {
        if scoped {
            self.dispatch.submit_keyed(inode, job);
        } else {
            self.dispatch.submit(job);
        }
    }

    fn front_recover(
        &self,
        inode: u64,
        closing: bool,
        ready: impl FnOnce(Option<front_write::CallbackGuard>, std::result::Result<(), i32>)
        + Send
        + 'static,
    ) {
        if let Some(queue) = &self.front_write {
            queue.recover(inode, closing, ready);
        } else {
            ready(None, Ok(()));
        }
    }

    #[cfg(test)]
    fn retire_front_handle(&self, ino: u64, fh: u64) {
        if let Some(queue) = &self.front_write {
            // File callbacks carry the same-inode gate through real completion.
            // Earlier releases removed their handle before this callback enters.
            let last_handle = self
                .file_handle_inodes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .values()
                .filter(|value| **value == ino)
                .count()
                <= 1;
            queue.release_handle(ino, fh, last_handle);
        }
    }

    /// Entry and attribute TTLs are zero: DFS has no cross-node kernel
    /// invalidation, so every path lookup and getattr goes to the Node
    /// (S1a-7, F10; positive TTLs need an invalidation scheme first).
    fn entry_ttls(&self) -> (Duration, Duration) {
        (TTL, TTL)
    }

    fn record_request(&self, operation: &'static str) {
        if let Some((metrics, filesystem)) = &self.requests {
            metrics.record(filesystem, operation);
        }
    }

    fn record_io_request(&self, operation: &'static str, bytes: usize) {
        if let Some((metrics, filesystem)) = &self.requests {
            metrics.record_io(filesystem, operation, bytes);
        }
    }

    fn filesystem_label(&self) -> &'static str {
        self.requests
            .as_ref()
            .map_or("unknown", |(_, filesystem)| *filesystem)
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
        if let Some(queue) = &self.front_write
            && let Err(error) = queue.close()
        {
            self.cleanup_error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert_with(|| Error::from(io::Error::from_raw_os_error(error)));
        }
        // Body completion can make parked recovery continuations ready. Keep
        // the dispatcher accepting until their file-gate chains have finished.
        self.dispatch.drain();
        self.cleanup_lock_session();
        self.dispatch.close_and_drain();
        if let Some(bridge) = &self.fd_bridge {
            bridge.unmount();
        }
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
        if self.cached_io {
            configure_readahead(self.mount_options.fuse_max_readahead_bytes, |limit| {
                config.set_max_readahead(limit)
            })?;
        }
        for capability in negotiated_init_flags(config.capabilities()) {
            let _ = config.add_capabilities(capability);
        }
        #[cfg(feature = "dfs")]
        if self.read_runtime.is_some() {
            // Fixed-version reads use direct_io. Without ASYNC_DIO the kernel
            // allows only one pending direct read per fh, regardless of workers.
            config
                .add_capabilities(InitFlags::FUSE_ASYNC_DIO)
                .map_err(|missing| {
                    io::Error::new(
                        io::ErrorKind::Unsupported,
                        format!("async read pipeline needs kernel ASYNC_DIO: {missing:?}"),
                    )
                })?;
        }
        if self.cached_io {
            afs_logging::info!("dfs.fuse_cache_init"; "requested_readahead_bytes" => self.mount_options.fuse_max_readahead_bytes, "kernel_config" => format!("{config:?}"));
        }
        Ok(())
    }

    fn destroy(&mut self) {
        self.record_request("destroy");
        self.finish_callbacks();
    }

    fn ioctl(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FuseFileHandle,
        flags: IoctlFlags,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: ReplyIoctl,
    ) {
        self.record_request("ioctl");
        if !flags.is_empty()
            || !matches!(
                cmd,
                fd_wire::PROBE | fd_wire::REGISTER | fd_wire::UNREGISTER
            )
        {
            reply.error(fuse_errno(libc::ENOTTY));
            return;
        }
        if out_size as usize != fd_wire::RECORD_BYTES {
            reply.error(fuse_errno(libc::EINVAL));
            return;
        }
        let Ok(value) = fd_wire::Control::decode(in_data) else {
            reply.error(fuse_errno(libc::EINVAL));
            return;
        };
        let Some(bridge) = self.fd_bridge.clone() else {
            reply.error(fuse_errno(libc::EOPNOTSUPP));
            return;
        };
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
        if let Err(error) = bridge.check_control(ino.0, fh.0) {
            reply.error(fuse_errno(error));
            return;
        }
        let uid = req.uid();
        self.dispatch
            .submit_keyed(self.file_dispatch_key(ino.0, fh.0), move || {
                match bridge.control(fh.0, uid, cmd, value) {
                    Ok((value, work)) => {
                        reply.ioctl(0, &value.encode());
                        if let Some(work) = work {
                            bridge.after_reply(work);
                        }
                    }
                    Err(error) => reply.error(fuse_errno(error)),
                }
            });
    }

    fn lookup(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, None, false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (entry_ttl, attr_ttl) = self.entry_ttls();
        let run = move || {
            let _front_guard = _front_guard;
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
                    reply.entry_with_ttls(
                        &attr_ttl,
                        &entry_ttl,
                        &file_attr(ino, &entry.attributes),
                        Generation(0),
                    );
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
        if let Ok(mut cache) = self.page_cache_ids.lock() {
            cache.remove(ino.0);
        }
    }

    fn getattr(&self, req: &Request, ino: INodeNo, fh: Option<FuseFileHandle>, reply: ReplyAttr) {
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_, attr_ttl) = self.entry_ttls();
        let run = move || {
            let _front_guard = _front_guard;
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
                        reply.attr(&attr_ttl, &attr);
                    }
                    None => reply.attr(&attr_ttl, &attr),
                },
                Err(error) => reply.error(fuse_errno(error)),
            }
        };
        if let Some(fh) = fh.filter(|_| !inline_local_read) {
            self.dispatch
                .submit_keyed(self.file_dispatch_key(ino, fh), run);
        } else {
            run();
        }
    }

    fn statfs(&self, req: &Request, ino: INodeNo, reply: ReplyStatfs) {
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let scoped = _front_guard.is_some();
        let run = move || {
            let _front_guard = _front_guard;
            match backend.statfs(&context, inode) {
                Ok(capacity) => reply_statfs(reply, capacity),
                Err(error) => reply.error(fuse_errno(errno(error))),
            }
        };
        if inline_local_statfs {
            run();
        } else {
            self.submit_file_metadata(ino.0, scoped, run);
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_, attr_ttl) = self.entry_ttls();
        let cache_ids = self.page_cache_ids.clone();
        let run = move || {
            let _front_guard = _front_guard;
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
                Ok(attributes) => {
                    if let Ok(mut cache) = cache_ids.lock() {
                        cache.remove(ino);
                    }
                    reply.attr(&attr_ttl, &file_attr(ino, &attributes));
                }
                Err(error) => reply.error(fuse_errno(error)),
            }
        };
        if let Some(fh) = fh.filter(|_| !inline_local_read) {
            self.dispatch
                .submit_keyed(self.file_dispatch_key(ino, fh), run);
        } else {
            run();
        }
    }

    fn readlink(&self, req: &Request, ino: INodeNo, reply: ReplyData) {
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, None, false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
                let (entry_ttl, attr_ttl) = self.entry_ttls();
                reply.entry_with_ttls(
                    &attr_ttl,
                    &entry_ttl,
                    &file_attr(ino, &entry.attributes),
                    Generation(0),
                );
            }
            Err(error) => reply.error(fuse_errno(error)),
        }
    }

    fn unlink(&self, req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, None, false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, None, false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, None, false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
                let (entry_ttl, attr_ttl) = self.entry_ttls();
                reply.entry_with_ttls(
                    &attr_ttl,
                    &entry_ttl,
                    &file_attr(ino, &entry.attributes),
                    Generation(0),
                );
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, None, false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
                let (entry_ttl, attr_ttl) = self.entry_ttls();
                reply.entry_with_ttls(
                    &attr_ttl,
                    &entry_ttl,
                    &file_attr(ino, &entry.attributes),
                    Generation(0),
                );
            }
            Err(error) => reply.error(fuse_errno(error)),
        }
    }

    fn open(&self, req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
                    if let Some(bridge) = &self.fd_bridge {
                        bridge.opened(
                            fh,
                            ino,
                            inode,
                            handle,
                            Self::context(req, 0),
                            flags,
                            self.cached_io,
                        );
                    }
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
        let fd_bridge = self.fd_bridge.clone();
        let backend = self.backend.clone();
        let state = self.state.clone();
        let file_handle_inodes = self.file_handle_inodes.clone();
        let context = Self::context(req, 0);
        let cached_io = self.cached_io;
        // Pages survive a reopen only for the exact same pinned version
        // (decision 4a), so keeping them is always safe (S1a-7, F8).
        let keep_cache = cached_io;
        let cache_ids = self.page_cache_ids.clone();
        let cache_gates = self.inode_cache_gates.clone();
        let active_cached_views = self.active_cached_views.clone();
        let enqueue_at = std::time::Instant::now();
        let open_span = tracing::info_span!(
            "node.fuse.open",
            ino,
            flags,
            queue_wait_us = tracing::field::Empty,
        );
        #[cfg(feature = "ownerfs")]
        let ownerfs = self.ownerfs.clone();
        let run = move || {
            // No table lock crosses Meta RPCs or reply I/O. This bounded local
            // fence drains old READ replies before a fresh OPEN changes the
            // inode mapping, and serializes identity decisions through reply.
            // Declare the lease first: its RwLock guard must drop before the
            // lease retires the final per-inode reference on every return path.
            let cache_gate = if cached_io {
                match cache_gates.acquire(ino) {
                    Ok(gate) => Some(gate),
                    Err(error) => {
                        reply.error(fuse_errno(error));
                        return;
                    }
                }
            } else {
                None
            };
            let _cache_open = if let Some(gate) = &cache_gate {
                match gate.write() {
                    Ok(guard) => Some(guard),
                    Err(_) => {
                        reply.error(fuse_errno(libc::EIO));
                        return;
                    }
                }
            } else {
                None
            };
            open_span.record("queue_wait_us", enqueue_at.elapsed().as_micros() as u64);
            let _entered = open_span.enter();
            let result = tracing::info_span!("node.fuse.open.backend").in_scope(|| {
                backend
                    .open_with_options(&context, inode, flags, options)
                    .map_err(errno)
            });
            match result {
                Ok(handle) => {
                    let fh = state
                        .lock()
                        .unwrap()
                        .insert_file_handle_with_policy(handle, inline_local_read);
                    file_handle_inodes.lock().unwrap().insert(fh, ino);
                    if let Some(bridge) = &fd_bridge {
                        bridge.opened(fh, ino, inode, handle, context.clone(), flags, cached_io);
                    }
                    #[cfg(feature = "ownerfs")]
                    if let Some(ownerfs) = &ownerfs {
                        ownerfs.with_fuse_cache_policy(inode, |_, private| {
                            reply.opened(FuseFileHandle(fh), open_reply_flags(!private));
                        });
                        return;
                    }
                    let versioned_read = backend.fixed_read_view(handle)
                        || (backend.supports_async_reads()
                            && flags & libc::O_ACCMODE == libc::O_RDONLY);
                    let identity = if cached_io
                        && flags & (libc::O_ACCMODE | libc::O_DIRECT) == libc::O_RDONLY
                    {
                        backend.page_cache_identity(handle)
                    } else {
                        None
                    };
                    let cache_admitted = identity.as_ref().is_some_and(|identity| {
                        active_cached_views
                            .lock()
                            .is_ok_and(|mut views| views.admit(ino, fh, identity))
                    });
                    // Poisoned optimization state opts out; it never permits
                    // reuse based on an identity that could not be checked.
                    let keep = cache_ids.lock().is_ok_and(|mut cache| {
                        cache.observe(
                            ino,
                            if keep_cache && cache_admitted {
                                identity
                            } else {
                                None
                            },
                        )
                    });
                    let mut reply_flags =
                        native_open_reply_flags(cached_io, versioned_read, flags, cache_admitted);
                    if keep {
                        reply_flags |= FopenFlags::FOPEN_KEEP_CACHE;
                    }
                    afs_logging::info!("dfs.fuse_cache_open"; "ino" => ino, "fh" => fh, "versioned_read" => versioned_read, "cached" => !reply_flags.contains(DIRECT_IO), "keep_cache" => keep);
                    reply.opened(FuseFileHandle(fh), reply_flags);
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
        #[cfg(test)]
        if let Some(bridge) = &self.fd_bridge {
            bridge.note_fuse_data(false);
        }

        #[cfg(feature = "dfs")]
        let reply = match self.try_start_async_read(req, ino, fh, offset, size, reply) {
            Some(reply) => reply,
            None => return,
        };
        #[cfg(any(feature = "dfs", test))]
        let diagnostic = self.read_work.as_ref().and_then(|metrics| {
            metrics.diagnostic(crate::node::read_work::ReadDiagnosticStage::FuseRead)
        });
        self.record_io_request("read", size as usize);
        let ino = ino.0;
        let fh = fh.0;
        let request_span = tracing::info_span!(
            "node.fuse.read.request",
            ino,
            fh,
            offset,
            requested_bytes = size as u64,
        );
        let file = self.state.lock().unwrap().file_handle(fh);
        let inline_local_read = file.as_ref().is_some_and(|file| file.inline_local_read);
        // The capability is local and excludes writable/remote-owner handles.
        // Clone the handle under the table lock; never hold it across backend work.
        let parallel_read = self.cached_io
            && file
                .as_ref()
                .is_some_and(|file| self.backend.supports_parallel_reads(file.handle));
        let backend = self.backend.clone();
        let state = self.state.clone();
        let context = Self::context(req, 0);
        let cached_io = self.cached_io;
        let cache_gates = self.inode_cache_gates.clone();
        let enqueue_at = std::time::Instant::now();
        let filesystem = self.filesystem_label();
        let fuse_span = tracing::info_span!(
            parent: &request_span,
            "node.fuse.read",
            filesystem,
            ino,
            fh,
            offset,
            requested_bytes = size as u64,
            queue_wait_us = tracing::field::Empty,
            returned_bytes = tracing::field::Empty,
        );
        if inline_local_read {
            let mut scratch = self.inline_local_read_scratch.lock().unwrap();
            let result = fuse_span.in_scope(|| {
                tracing::info_span!("node.fuse.read.execute").in_scope(|| {
                    fuse_span.record("queue_wait_us", 0_u64);
                    let file = state.lock().unwrap().file_handle(fh);
                    let file = file.ok_or(libc::ESTALE).and_then(|file| {
                        AfsFuse::validate_handle_inode_in(&state, ino).map(|()| file)
                    });
                    match file {
                        Ok(file) => inline_local_read_data(&mut scratch, size as usize, |out| {
                            backend
                                .read(&context, file.handle, offset, out)
                                .map_err(errno)
                        }),
                        Err(error) => Err(error),
                    }
                })
            });
            #[cfg(any(feature = "dfs", test))]
            let success = result.is_ok();
            complete_read_reply(
                #[cfg(any(feature = "dfs", test))]
                diagnostic,
                #[cfg(any(feature = "dfs", test))]
                success,
                || match result {
                    Ok(data) => {
                        fuse_span.record("returned_bytes", data.len() as u64);
                        tracing::info_span!(parent: &request_span, "node.fuse.read.reply", returned_bytes = data.len() as u64)
                        .in_scope(|| reply.data(&data))
                    }
                    Err(error) => reply.error(fuse_errno(error)),
                },
            );
            return;
        }
        let run = move || {
            let _front_guard = _front_guard;
            let cache_gate = if cached_io {
                match cache_gates.acquire(ino) {
                    Ok(gate) => Some(gate),
                    Err(error) => {
                        complete_read_reply(
                            #[cfg(any(feature = "dfs", test))]
                            diagnostic,
                            #[cfg(any(feature = "dfs", test))]
                            false,
                            || reply.error(fuse_errno(error)),
                        );
                        return;
                    }
                }
            } else {
                None
            };
            let _cache_read = if let Some(gate) = &cache_gate {
                match gate.read() {
                    Ok(guard) => Some(guard),
                    Err(_) => {
                        complete_read_reply(
                            #[cfg(any(feature = "dfs", test))]
                            diagnostic,
                            #[cfg(any(feature = "dfs", test))]
                            false,
                            || reply.error(fuse_errno(libc::EIO)),
                        );
                        return;
                    }
                }
            } else {
                None
            };
            let result = fuse_span.in_scope(|| {
                tracing::info_span!("node.fuse.read.execute").in_scope(|| {
                    fuse_span.record("queue_wait_us", enqueue_at.elapsed().as_micros() as u64);
                    let file = state.lock().unwrap().file_handle(fh).ok_or(libc::ESTALE)?;
                    AfsFuse::validate_handle_inode_in(&state, ino)?;
                    backend_read_data(
                        backend.as_ref(),
                        &context,
                        file.handle,
                        offset,
                        size as usize,
                    )
                    .map_err(errno)
                })
            });
            #[cfg(any(feature = "dfs", test))]
            let success = result.is_ok();
            complete_read_reply(
                #[cfg(any(feature = "dfs", test))]
                diagnostic,
                #[cfg(any(feature = "dfs", test))]
                success,
                || match result {
                    Ok(data) => {
                        fuse_span.record("returned_bytes", data.len() as u64);
                        tracing::info_span!(parent: &request_span, "node.fuse.read.reply", returned_bytes = data.len() as u64)
                        .in_scope(|| reply.data(&data))
                    }
                    Err(error) => reply.error(fuse_errno(error)),
                },
            );
        };
        if parallel_read {
            self.dispatch
                .submit_read_keyed(self.file_dispatch_key(ino, fh), run);
        } else {
            self.dispatch
                .submit_keyed(self.file_dispatch_key(ino, fh), run);
        }
    }

    fn write(
        &self,
        req: &Request,
        ino: INodeNo,
        fh: FuseFileHandle,
        offset: u64,
        data: &[u8],
        write_flags: WriteFlags,
        flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let queued = ordinary_queued_write(flags, write_flags)
            && self
                .state
                .lock()
                .ok()
                .and_then(|state| state.file_handle(fh.0))
                .is_some_and(|file| self.backend.accepts_queued_writes(file.handle));
        let ordering = if queued {
            FrontOrdering::WriteAdmission
        } else {
            FrontOrdering::WriteBarrier
        };
        let (_front_guard, front_result) = self.front_callback(ordering, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
        #[cfg(test)]
        if let Some(bridge) = &self.fd_bridge {
            bridge.note_fuse_data(true);
        }

        self.record_io_request("write", data.len());
        let ino = ino.0;
        let fh = fh.0;
        let backend = self.backend.clone();
        let state = self.state.clone();
        let context = Self::context(req, 0);
        let request_span = tracing::info_span!(
            "node.fuse.write.request",
            ino,
            fh,
            offset,
            requested_bytes = data.len() as u64,
        );
        let file = state
            .lock()
            .expect("FUSE handle state mutex poisoned")
            .file_handle(fh);
        if let Some(queue) = &self.front_write {
            let eligible = file.is_some_and(|file| backend.accepts_queued_writes(file.handle));
            if eligible && ordinary_queued_write(flags, write_flags) {
                let Some(file) = file else {
                    reply.error(fuse_errno(libc::ESTALE));
                    return;
                };
                if let Err(error) = Self::validate_handle_inode_in(&state, ino) {
                    reply.error(fuse_errno(error));
                    return;
                }
                if offset.checked_add(data.len() as u64).is_none()
                    || u32::try_from(data.len()).is_err()
                {
                    reply.error(fuse_errno(libc::EFBIG));
                    return;
                }
                let length = data.len();
                let cache_ids = self.page_cache_ids.clone();
                let options = WriteOptions {
                    kill_suidgid: write_kill_suidgid(write_flags),
                };
                let timing = self.requests.clone();
                let queued_at = std::time::Instant::now();
                queue.submit_deferred_with(
                    ino,
                    fh,
                    length,
                    || {
                        let mut owned = Vec::new();
                        owned.try_reserve_exact(length).map_err(|_| libc::ENOMEM)?;
                        owned.extend_from_slice(data);
                        let mut consumed = 0usize;
                        Ok(Box::new(move || {
                            let _timer = timing.as_ref().map(|(metrics, filesystem)| {
                                metrics
                                    .write_duration
                                    .with_label_values(&[filesystem, "write_call", "queue"])
                                    .observe(queued_at.elapsed().as_secs_f64());
                                metrics
                                    .write_duration
                                    .with_label_values(&[filesystem, "write_call", "execute"])
                                    .start_timer()
                            });
                            (|| {
                                while consumed < owned.len() {
                                    let written = backend
                                        .write_with_options(
                                            &context,
                                            file.handle,
                                            offset + consumed as u64,
                                            &owned[consumed..],
                                            options,
                                        )
                                        .map_err(errno)?;
                                    if written == 0 || written > owned.len() - consumed {
                                        return Err(libc::EIO);
                                    }
                                    consumed += written;
                                }
                                if let Ok(mut cache) = cache_ids.lock() {
                                    cache.remove(ino);
                                }
                                Ok(())
                            })()
                        }))
                    },
                    move |result| match result {
                        Ok(()) => reply.written(length as u32),
                        Err(error) => reply.error(fuse_errno(error)),
                    },
                );
                return;
            }
            // Sync, append, remote-owner and writeback requests retain the old
            // validation and completion point, after admitted ordinary writes.
            if let Err(error) = queue.wait(Some(ino), false) {
                reply.error(fuse_errno(error));
                return;
            }
        }
        let admission = fuse_write_admission(
            file.map_or(WriteAdmissionMode::Queued, |file| {
                backend.write_admission_mode(file.handle)
            }),
            write_flags,
        );
        afs_logging::debug!(
            "FUSE write admission";
            "event" => "fuse_write_admission",
            "ino" => ino,
            "fh" => fh,
            "offset" => offset,
            "requested_bytes" => data.len() as u64,
            "admission" => match admission {
                WriteAdmissionMode::Queued => "queued",
                WriteAdmissionMode::Borrowed => "borrowed",
            },
        );
        let copy_span_parent = request_span.clone();
        let options = WriteOptions {
            kill_suidgid: write_kill_suidgid(write_flags),
        };
        let enqueue_at = std::time::Instant::now();
        let requested_bytes = data.len() as u64;
        let filesystem = self.filesystem_label();
        let fuse_span = tracing::info_span!(
            parent: &request_span,
            "node.fuse.write",
            filesystem,
            ino,
            fh,
            offset,
            requested_bytes,
            queue_wait_us = tracing::field::Empty,
            returned_bytes = tracing::field::Empty,
        );
        let input = if admission == WriteAdmissionMode::Queued {
            let input = tracing::info_span!(
                parent: &copy_span_parent,
                "node.fuse.write.input_copy",
                copied_bytes = data.len() as u64,
            )
            .in_scope(|| self.dispatch.copy_write_input(data));
            match input {
                Ok(input) => Some(input),
                Err(error) => {
                    reply.error(fuse_errno(error));
                    return;
                }
            }
        } else {
            None
        };
        let cache_ids = self.page_cache_ids.clone();
        let timing = self.requests.clone();
        let source = if write_flags.contains(WriteFlags::FUSE_WRITE_CACHE) {
            "page_cache"
        } else {
            "write_call"
        };
        let run = move |data: &[u8]| {
            let _front_guard = _front_guard;
            if let Some((metrics, filesystem)) = &timing {
                metrics
                    .write_duration
                    .with_label_values(&[filesystem, source, "queue"])
                    .observe(enqueue_at.elapsed().as_secs_f64());
            }
            let executing = std::time::Instant::now();
            let result = fuse_span.in_scope(|| {
                tracing::info_span!("node.fuse.write.execute").in_scope(|| {
                    fuse_span.record("queue_wait_us", enqueue_at.elapsed().as_micros() as u64);
                    let file = state.lock().unwrap().file_handle(fh).ok_or(libc::ESTALE)?;
                    AfsFuse::validate_handle_inode_in(&state, ino)?;
                    Ok(backend.as_ref()).and_then(|backend| {
                        backend
                            .write_with_options(&context, file.handle, offset, data, options)
                            .map_err(errno)
                    })
                })
            });
            if let Some((metrics, filesystem)) = &timing {
                metrics
                    .write_duration
                    .with_label_values(&[filesystem, source, "execute"])
                    .observe(executing.elapsed().as_secs_f64());
            }
            match result {
                Ok(written) => match u32::try_from(written) {
                    Ok(written) => {
                        if let Ok(mut cache) = cache_ids.lock() {
                            cache.remove(ino);
                        }
                        fuse_span.record("returned_bytes", written as u64);
                        tracing::info_span!(parent: &request_span, "node.fuse.write.reply", returned_bytes = written as u64)
                            .in_scope(|| reply.written(written))
                    }
                    Err(_) => reply.error(fuse_errno(libc::EIO)),
                },
                Err(error) => reply.error(fuse_errno(error)),
            }
        };
        match input {
            None => {
                self.dispatch
                    .run_borrowed_keyed(self.file_dispatch_key(ino, fh), || run(data));
            }
            Some(data) => {
                self.dispatch
                    .submit_keyed(
                        self.file_dispatch_key(ino, fh),
                        move || run(data.as_slice()),
                    );
            }
        }
    }

    fn copy_file_range(
        &self,
        req: &Request,
        ino_in: INodeNo,
        fh_in: FuseFileHandle,
        offset_in: u64,
        ino_out: INodeNo,
        fh_out: FuseFileHandle,
        offset_out: u64,
        len: u64,
        flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        let ordering = if flags.is_empty() {
            FrontOrdering::WriteBarrier
        } else {
            FrontOrdering::FixedLocalReply
        };
        let files = [ino_in.0, ino_out.0];
        let (_front_guard, front_result) = self.front_callbacks(ordering, Some(&files), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
        self.record_request("copy_file_range");
        if !flags.is_empty() {
            reply.error(fuse_errno(libc::EINVAL));
            return;
        }
        let backend = self.backend.clone();
        let state = self.state.clone();
        let context = Self::context(req, 0);
        let queued = std::time::Instant::now();
        let metrics = self.requests.clone();
        self.dispatch
            .submit_keyed(self.file_dispatch_key(ino_out.0, fh_out.0), move || {
                let _front_guard = _front_guard;
                let _timer = metrics.as_ref().map(|(metrics, filesystem)| {
                    metrics
                        .copy_range_duration
                        .with_label_values(&[filesystem, "queue"])
                        .observe(queued.elapsed().as_secs_f64());
                    metrics
                        .copy_range_duration
                        .with_label_values(&[filesystem, "execute"])
                        .start_timer()
                });
                let result = (|| {
                    let (source, destination) = {
                        let state = state.lock().unwrap();
                        (
                            state.file_handle(fh_in.0).ok_or(libc::ESTALE)?,
                            state.file_handle(fh_out.0).ok_or(libc::ESTALE)?,
                        )
                    };
                    AfsFuse::validate_handle_inode_in(&state, ino_in.0)?;
                    AfsFuse::validate_handle_inode_in(&state, ino_out.0)?;
                    backend
                        .copy_file_range(
                            &context,
                            crate::node::vfs::types::CopyFileRange {
                                source: source.handle,
                                source_offset: offset_in,
                                destination: destination.handle,
                                destination_offset: offset_out,
                                length: len,
                            },
                        )
                        .map_err(errno)
                })();
                match result {
                    Ok(copied) => match u32::try_from(copied) {
                        Ok(copied) => reply.written(copied),
                        Err(_) => reply.error(fuse_errno(libc::EOVERFLOW)),
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
        let dispatch = FuseRecoverySender {
            shared: self.dispatch.shared.clone(),
            normal_capacity: self.front_write.is_none().then_some(self.dispatch.capacity),
        };
        let dispatch_key = self.file_dispatch_key(ino, fh);
        self.front_recover(ino, false, move |_front_guard, front_result| {
            if _front_guard.is_none()
                && let Err(error) = front_result
            {
                reply.error(fuse_errno(error));
                return;
            }
            let run = move || {
                let _front_guard = _front_guard;
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
                reply_empty(reply, combine_empty_results(front_result, result));
            };
            if inline_local_read {
                run();
            } else {
                dispatch.submit(dispatch_key, run);
            }
        });
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
        let fd_bridge = self.fd_bridge.clone();
        let backend = self.backend.clone();
        let state = self.state.clone();
        let file_handle_inodes = self.file_handle_inodes.clone();
        let context = Self::context(req, 0);
        let flock_owner = lock_owner.map(|owner| self.file_lock_owner(owner));
        let active_cached_views = self.active_cached_views.clone();
        let dispatch = FuseRecoverySender {
            shared: self.dispatch.shared.clone(),
            normal_capacity: self.front_write.is_none().then_some(self.dispatch.capacity),
        };
        let dispatch_key = self.file_dispatch_key(ino, fh);
        let front_queue = self.front_write.clone();
        self.front_recover(ino, true, move |_front_guard, front_result| {
            let run = move || {
                let _front_guard = _front_guard;
                if let Some(queue) = &front_queue {
                    let last = file_handle_inodes
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .values()
                        .filter(|value| **value == ino)
                        .count()
                        <= 1;
                    queue.release_handle(ino, fh, last);
                }
                let file = state.lock().unwrap().remove_file_handle(fh);
                if let Ok(mut views) = active_cached_views.lock() {
                    views.release(ino, fh);
                }
                if let Some(bridge) = &fd_bridge {
                    bridge.release_fuse(fh);
                }
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
                            .release_locks(
                                &context,
                                inode,
                                file.handle,
                                owner,
                                ReleaseKind::FlockOwner,
                            )
                            .map_err(errno)
                    });
                    let released_handle = backend.release(&context, file.handle).map_err(errno);
                    combine_empty_results(released_flock, released_handle)
                });
                reply_empty(reply, combine_empty_results(front_result, result));
            };
            if inline_local_read {
                run();
            } else {
                dispatch.submit(dispatch_key, run);
            }
        });
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
        let dispatch = FuseRecoverySender {
            shared: self.dispatch.shared.clone(),
            normal_capacity: self.front_write.is_none().then_some(self.dispatch.capacity),
        };
        let dispatch_key = self.file_dispatch_key(ino, fh);
        self.front_recover(ino, false, move |_front_guard, front_result| {
            if _front_guard.is_none()
                && let Err(error) = front_result
            {
                reply.error(fuse_errno(error));
                return;
            }
            let run = move || {
                let _front_guard = _front_guard;

                let file = state.lock().unwrap().file_handle(fh);
                let result = file.ok_or(libc::ESTALE).and_then(|file| {
                    AfsFuse::validate_handle_inode_in(&state, ino)?;
                    Ok(backend.as_ref()).and_then(|backend| {
                        backend
                            .fsync(&context, file.handle, sync_mode(datasync))
                            .map_err(errno)
                    })
                });

                reply_empty(reply, combine_empty_results(front_result, result));
            };
            if inline_local_read {
                run();
            } else {
                dispatch.submit(dispatch_key, run);
            }
        });
    }

    fn opendir(&self, req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, None, false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
        self.record_request("create");
        afs_logging::info!("fuse.create"; "parent" => parent.0, "name" => name.to_string_lossy().into_owned());
        let options = OpenOptions {
            kill_suidgid: false,
        };
        let context = Self::context(req, umask);
        let result = self.backend_inode(parent.0).and_then(|parent_inode| {
            Ok(self.backend.as_ref()).and_then(|backend| {
                backend
                    .create_with_options(&context, parent_inode, name, mode, flags, options)
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
                if let Some(bridge) = &self.fd_bridge {
                    bridge.opened(
                        fh,
                        ino,
                        created.entry.inode,
                        created.handle,
                        context.clone(),
                        flags,
                        self.cached_io,
                    );
                }
                if let Ok(mut cache) = self.page_cache_ids.lock() {
                    cache.remove(ino);
                }
                self.with_cache_policy(created.entry.inode, |ttl, private| {
                    // Official ReplyCreate has one TTL for both fields. Use
                    // the stricter DFS limit rather than relaxing either one.
                    let ttl = if self.cached_io {
                        let (entry_ttl, attr_ttl) = self.entry_ttls();
                        entry_ttl.min(attr_ttl)
                    } else {
                        ttl
                    };
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::FixedLocalReply, None, false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::FixedLocalReply, None, false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, None, false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
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
                let (entry_ttl, attr_ttl) = self.entry_ttls();
                reply.entry_with_ttls(
                    &attr_ttl,
                    &entry_ttl,
                    &file_attr(ino, &entry.attributes),
                    Generation(0),
                );
            }
            Err(error) => reply.error(fuse_errno(error)),
        }
    }

    fn access(&self, _: &Request, _: INodeNo, _: AccessFlags, reply: ReplyEmpty) {
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::FixedLocalReply, None, false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
        self.record_request("access");
        reply.error(fuse_errno(libc::ENOSYS));
    }

    fn getxattr(&self, req: &Request, ino: INodeNo, name: &OsStr, size: u32, reply: ReplyXattr) {
        let ordering = xattr_front_ordering(self.backend.as_ref(), name);
        let (_front_guard, front_result) = self.front_callback(ordering, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
        self.record_request("getxattr");
        let Ok(inode) = self.backend_inode(ino.0) else {
            reply.error(fuse_errno(libc::ESTALE));
            return;
        };
        let backend = self.backend.clone();
        let context = Self::metadata_context(req, 0);
        let name = name.to_os_string();
        let scoped = _front_guard.is_some();
        self.submit_file_metadata(ino.0, scoped, move || {
            let _front_guard = _front_guard;
            match backend.getxattr(&context, inode, &name) {
                Ok(data) => reply_xattr(reply, size, Ok(data)),
                Err(error) => reply.error(fuse_errno(errno_xattr(
                    error,
                    "getxattr",
                    inode.value,
                    &name,
                    &context,
                ))),
            }
        });
    }

    fn listxattr(&self, req: &Request, ino: INodeNo, size: u32, reply: ReplyXattr) {
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
        self.record_request("listxattr");
        let Ok(inode) = self.backend_inode(ino.0) else {
            reply.error(fuse_errno(libc::ESTALE));
            return;
        };
        let backend = self.backend.clone();
        let context = Self::metadata_context(req, 0);
        let scoped = _front_guard.is_some();
        self.submit_file_metadata(ino.0, scoped, move || {
            let _front_guard = _front_guard;
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
        let validation = validate_xattr_set(flags, position);
        let ordering = if validation.is_err() {
            FrontOrdering::FixedLocalReply
        } else {
            FrontOrdering::WriteBarrier
        };
        let (_front_guard, front_result) = self.front_callback(ordering, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
        self.record_request("setxattr");
        if let Err(error) = validation {
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
        let scoped = _front_guard.is_some();
        self.submit_file_metadata(ino.0, scoped, move || {
            let _front_guard = _front_guard;
            reply_empty(
                reply,
                backend
                    .setxattr(&context, inode, &name, &value, flags)
                    .map_err(|error| errno_xattr(error, "setxattr", inode.value, &name, &context)),
            );
        });
    }

    fn removexattr(&self, req: &Request, ino: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let (_front_guard, front_result) =
            self.front_callback(FrontOrdering::WriteBarrier, Some(ino.0), false);
        if let Err(error) = front_result {
            reply.error(fuse_errno(error));
            return;
        }
        self.record_request("removexattr");
        let Ok(inode) = self.backend_inode(ino.0) else {
            reply.error(fuse_errno(libc::ESTALE));
            return;
        };
        let backend = self.backend.clone();
        let context = Self::metadata_context(req, 0);
        let name = name.to_os_string();
        let scoped = _front_guard.is_some();
        self.submit_file_metadata(ino.0, scoped, move || {
            let _front_guard = _front_guard;
            reply_empty(
                reply,
                backend
                    .removexattr(&context, inode, &name)
                    .map_err(|error| {
                        errno_xattr(error, "removexattr", inode.value, &name, &context)
                    }),
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
    // requested so mmap on direct_io handles remains capability-gated. Native
    // DFS normally uses cached write-through and version-bound page reuse;
    // explicit direct IO and overlapping incompatible fixed-open views bypass
    // the shared inode cache. Cached handles follow close-to-open semantics.
    InitFlags::FUSE_PARALLEL_DIROPS | InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP
}

fn negotiated_init_flags(available: InitFlags) -> impl Iterator<Item = InitFlags> {
    requested_init_flags()
        .iter()
        .filter(move |capability| available.contains(*capability))
}

fn fuse_write_admission(mode: WriteAdmissionMode, flags: WriteFlags) -> WriteAdmissionMode {
    // Kernel page writeback can wait for uploads/publication. Keep the FUSE
    // receiver available for unrelated page faults; the owned bounded queue
    // retains the same handle FIFO and the original backend acknowledgement.
    if flags.contains(WriteFlags::FUSE_WRITE_CACHE) {
        WriteAdmissionMode::Queued
    } else {
        mode
    }
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

fn native_open_reply_flags(
    cached_io: bool,
    fixed_view: bool,
    flags: i32,
    cache_admitted: bool,
) -> FopenFlags {
    open_reply_flags(!cached_io || (fixed_view && !cache_admitted) || flags & libc::O_DIRECT != 0)
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

/// Enriched failure log for xattr requests. The generic `errno` log carries no
/// request identity, which makes repeated xattr rejections (for example the
/// OwnerFs user.*-only policy) impossible to attribute. xattr handlers add the
/// operation, inode, xattr name and request credentials so the log itself names
/// the caller without extra tracing.
fn errno_xattr(
    error: Error,
    op: &'static str,
    ino: u64,
    name: &OsStr,
    context: &RequestContext,
) -> i32 {
    afs_logging::error!(
        "FUSE request failed";
        "code" => error.code().raw(),
        "kind" => format!("{:?}", error.kind()),
        "message" => error.message(),
        "op" => op,
        "inode" => ino,
        "name" => name.to_string_lossy().into_owned(),
        "pid" => context.pid,
        "uid" => context.uid,
        "gid" => context.gid,
    );
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
mod dispatch_tests;

#[cfg(test)]
mod local_reply_tests;

#[cfg(test)]
mod queue_default_tests;
