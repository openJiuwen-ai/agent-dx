//! 统一 POSIX/FUSE 入口。
//!
//! 接收内核回调，转换到 VFS `Backend` 接口并映射 errno/回复；管理 FUSE 会话内的
//! inode、目录项和打开句柄。每个 mount session 绑定一个 Backend；FUSE inode 编号
//! 不能当作后端持久文件身份。OwnerFs 的独用 Home 根可短暂缓存；首次远端
//! 访问先通过 FUSE notifier 失效本机缓存，再由 Home 执行该请求。

mod state;

use std::{
    borrow::Cow,
    collections::{HashMap, VecDeque},
    ffi::OsStr,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, RwLock,
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
    KernelConfig, LockOwner, MountOption, OpenFlags, RenameFlags as FuseRenameFlags, ReplyAttr,
    ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyLock, ReplyOpen,
    ReplyStatfs, ReplyWrite, ReplyXattr, Request, SessionACL, TimeOrNow, WriteFlags,
};

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

/// Explicit execution and DFS cache policy for one mounted FUSE session.
/// OwnerFs consumes execution settings only; its private cache policy remains
/// owned by OwnerFs. Nonzero DFS TTLs intentionally relax remote freshness.
#[derive(Clone, Copy, Debug)]
pub struct FuseMountOptions {
    pub fuse_threads: u16,
    pub fuse_clone_fd: bool,
    pub fuse_workers: u16,
    pub dfs_fuse_keep_cache: bool,
    pub dfs_fuse_entry_ttl_ms: u32,
    pub dfs_fuse_attr_ttl_ms: u32,
    pub fuse_max_readahead_bytes: u32,
}

impl Default for FuseMountOptions {
    fn default() -> Self {
        Self {
            fuse_threads: 4,
            fuse_clone_fd: true,
            fuse_workers: 32,
            dfs_fuse_keep_cache: true,
            dfs_fuse_entry_ttl_ms: 0,
            dfs_fuse_attr_ttl_ms: 0,
            fuse_max_readahead_bytes: 1024 * 1024,
        }
    }
}

impl From<&crate::config::Config> for FuseMountOptions {
    fn from(config: &crate::config::Config) -> Self {
        Self {
            fuse_threads: config.fuse_threads,
            fuse_clone_fd: config.fuse_clone_fd,
            fuse_workers: config.fuse_workers,
            dfs_fuse_keep_cache: config.dfs_fuse_keep_cache,
            dfs_fuse_entry_ttl_ms: config.dfs_fuse_entry_ttl_ms,
            dfs_fuse_attr_ttl_ms: config.dfs_fuse_attr_ttl_ms,
            fuse_max_readahead_bytes: config.fuse_max_readahead_bytes,
        }
    }
}

impl FuseMountOptions {
    fn validate(self) -> Result<Self> {
        if !(1..=64).contains(&self.fuse_threads)
            || !(1..=1024).contains(&self.fuse_workers)
            || self.dfs_fuse_entry_ttl_ms > 60_000
            || self.dfs_fuse_attr_ttl_ms > 60_000
            || self.fuse_max_readahead_bytes > 16 * 1024 * 1024
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
) -> Result<Vec<u8>> {
    backend.read_owned(ctx, handle, offset, size)
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
    )
}

pub fn mount_dfs_with_metrics(
    backend: Arc<dyn Backend>,
    path: &Path,
    metrics: FuseRequestMetrics,
) -> Result<MountedFuse> {
    mount_dfs_with_metrics_and_options(backend, path, metrics, FuseMountOptions::default())
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
    )
}

/// The ordinary mount APIs keep detailed read diagnostics disabled. This
/// opt-in shares a bounded journal with the Node's Meta and Peer adapters.
#[cfg(feature = "dfs")]
pub fn mount_dfs_with_read_diagnostics(
    backend: Arc<dyn Backend>,
    path: &Path,
    metrics: FuseRequestMetrics,
    read_work: crate::node::read_work::ReadWorkMetrics,
) -> Result<MountedFuse> {
    mount_dfs_with_read_diagnostics_and_options(
        backend,
        path,
        metrics,
        read_work,
        FuseMountOptions::default(),
    )
}

#[cfg(feature = "dfs")]
pub fn mount_dfs_with_read_diagnostics_and_options(
    backend: Arc<dyn Backend>,
    path: &Path,
    metrics: FuseRequestMetrics,
    read_work: crate::node::read_work::ReadWorkMetrics,
    options: FuseMountOptions,
) -> Result<MountedFuse> {
    mount_dfs_inner(backend, path, Some(metrics), Some(read_work), options)
}

fn mount_dfs_inner(
    backend: Arc<dyn Backend>,
    path: &Path,
    metrics: Option<FuseRequestMetrics>,
    #[cfg(any(feature = "dfs", test))] read_work: Option<crate::node::read_work::ReadWorkMetrics>,
    options: FuseMountOptions,
) -> Result<MountedFuse> {
    let options = options.validate()?;
    let path = canonical_mount_path(path)?;
    reject_existing_mount(&path)?;
    let mut fs = AfsFuse::with_mount_options(backend, options);
    fs.requests = metrics.map(|metrics| (metrics, "dfs"));
    #[cfg(any(feature = "dfs", test))]
    {
        fs.read_work = read_work;
    }
    // Cached write-through sends each syscall write to the inode owner and
    // keeps this mount's pages coherent. KEEP_CACHE requires an exact fresh
    // committed identity; writeback remains disabled.
    fs.cached_io = true;
    let cleanup_error = fs.cleanup_error.clone();
    let session = fuser::spawn_mount(fs, &path, &configured_mount("afs-dfs", true, options))
        .map_err(Error::from)?;
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
mod mount_path_tests {
    use super::canonical_mount_path;
    use std::os::unix::fs::symlink;

    #[test]
    fn mount_owner_resolves_ancestor_symlinks_once() {
        let root = tempfile::tempdir().unwrap();
        let actual_parent = root.path().join("actual");
        let actual_mount = actual_parent.join("mount");
        std::fs::create_dir_all(&actual_mount).unwrap();
        let alias = root.path().join("alias");
        symlink(&actual_parent, &alias).unwrap();

        assert_eq!(
            canonical_mount_path(&alias.join("mount")).unwrap(),
            actual_mount
        );
    }
}

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
    Borrowed(Arc<(Mutex<bool>, Condvar)>),
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

struct FuseDispatchShared {
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
    fn new(workers: usize) -> Self {
        let shared = Arc::new(FuseDispatchShared {
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
        Self {
            shared,
            capacity: workers * 8,
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

    fn submit_access(&self, key: u64, access: FuseAccess, job: impl FnOnce() + Send + 'static) {
        let mut queue = self.shared.queue.lock().unwrap();
        while queue.jobs >= self.capacity && !queue.closed {
            queue = self.shared.capacity.wait(queue).unwrap();
        }
        if queue.closed {
            drop(queue);
            drop(job);
            return;
        }
        queue.jobs += 1;
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
            queue.ready.push_back((
                Some(FuseTurn { key, access }),
                FuseJob::Owned(Box::new(job)),
            ));
            self.shared.ready.notify_one();
        } else {
            queue
                .waiting
                .entry(key)
                .or_default()
                .push_back((access, FuseJob::Owned(Box::new(job))));
        }
    }

    fn finish(shared: &FuseDispatchShared, turn: Option<FuseTurn>) {
        let mut queue = shared
            .queue
            .lock()
            .expect("FUSE queue mutex poisoned at completion");
        queue.jobs -= 1;
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
struct InodeCacheGates {
    shards: [Mutex<HashMap<u64, Arc<RwLock<()>>>>; PAGE_CACHE_INODE_GATES],
}

impl Default for InodeCacheGates {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }
}

impl InodeCacheGates {
    fn acquire(&self, ino: u64) -> std::result::Result<InodeCacheGate<'_>, i32> {
        let shard = &self.shards[(ino % PAGE_CACHE_INODE_GATES as u64) as usize];
        let mut table = shard.lock().map_err(|_| libc::EIO)?;
        let gate = table
            .entry(ino)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone();
        Ok(InodeCacheGate {
            shard,
            ino,
            gate: Some(gate),
        })
    }
}

/// Each lease owns one reference outside the table. It cannot be cloned:
/// acquisition and final retirement must both serialize through the table lock.
struct InodeCacheGate<'a> {
    shard: &'a Mutex<HashMap<u64, Arc<RwLock<()>>>>,
    ino: u64,
    gate: Option<Arc<RwLock<()>>>,
}

impl InodeCacheGate<'_> {
    fn read(&self) -> std::result::Result<std::sync::RwLockReadGuard<'_, ()>, i32> {
        self.gate
            .as_ref()
            .ok_or(libc::EIO)?
            .read()
            .map_err(|_| libc::EIO)
    }

    fn write(&self) -> std::result::Result<std::sync::RwLockWriteGuard<'_, ()>, i32> {
        self.gate
            .as_ref()
            .ok_or(libc::EIO)?
            .write()
            .map_err(|_| libc::EIO)
    }
}

impl Drop for InodeCacheGate<'_> {
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
    #[cfg(any(feature = "dfs", test))]
    read_work: Option<crate::node::read_work::ReadWorkMetrics>,
    requests: Option<(FuseRequestMetrics, &'static str)>,
    backend: Arc<dyn Backend>,
    cached_io: bool,
    mount_options: FuseMountOptions,
    page_cache_ids: Arc<Mutex<PageCacheIdentities>>,
    inode_cache_gates: Arc<InodeCacheGates>,
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
        Self::with_mount_options(
            backend,
            FuseMountOptions {
                fuse_workers: 8,
                ..FuseMountOptions::default()
            },
        )
    }

    fn with_mount_options(backend: Arc<dyn Backend>, mount_options: FuseMountOptions) -> Self {
        let state = Arc::new(Mutex::new(FuseState::new(backend.root_inode())));
        Self {
            #[cfg(any(feature = "dfs", test))]
            read_work: None,
            requests: None,
            backend,
            cached_io: false,
            mount_options,
            page_cache_ids: Arc::new(Mutex::new(PageCacheIdentities::default())),
            inode_cache_gates: Arc::new(InodeCacheGates::default()),
            inline_local_read_scratch: Mutex::new(Vec::new()),
            state,
            file_handle_inodes: Arc::new(Mutex::new(HashMap::new())),
            dispatch: FuseDispatch::new(usize::from(mount_options.fuse_workers)),
            ingress_session_id: next_ingress_session_id(),
            lock_session_cleaned: false,
            callbacks_drained: false,
            cleanup_error: Arc::new(Mutex::new(None)),
            #[cfg(feature = "ownerfs")]
            ownerfs: None,
        }
    }

    fn entry_ttls(&self) -> (Duration, Duration) {
        if self.cached_io {
            (
                Duration::from_millis(u64::from(self.mount_options.dfs_fuse_entry_ttl_ms)),
                Duration::from_millis(u64::from(self.mount_options.dfs_fuse_attr_ttl_ms)),
            )
        } else {
            (TTL, TTL)
        }
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
        if self.cached_io {
            configure_readahead(self.mount_options.fuse_max_readahead_bytes, |limit| {
                config.set_max_readahead(limit)
            })?;
        }
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
        let (entry_ttl, attr_ttl) = self.entry_ttls();
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
        let (_, attr_ttl) = self.entry_ttls();
        let cache_ids = self.page_cache_ids.clone();
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
        let keep_cache = cached_io && self.mount_options.dfs_fuse_keep_cache;
        let cache_ids = self.page_cache_ids.clone();
        let cache_gates = self.inode_cache_gates.clone();
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
                    #[cfg(feature = "ownerfs")]
                    if let Some(ownerfs) = &ownerfs {
                        ownerfs.with_fuse_cache_policy(inode, |_, private| {
                            reply.opened(FuseFileHandle(fh), open_reply_flags(!private));
                        });
                        return;
                    }
                    let identity = if keep_cache && flags & libc::O_ACCMODE == libc::O_RDONLY {
                        backend.page_cache_identity(handle)
                    } else {
                        None
                    };
                    // Poisoned optimization state opts out; it never permits
                    // reuse based on an identity that could not be checked.
                    let keep = cache_ids
                        .lock()
                        .is_ok_and(|mut cache| cache.observe(ino, identity));
                    let mut reply_flags = open_reply_flags(!cached_io);
                    if keep {
                        reply_flags |= FopenFlags::FOPEN_KEEP_CACHE;
                    }
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
            self.dispatch.submit_read_keyed(fh, run);
        } else {
            self.dispatch.submit_keyed(fh, run);
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
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
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
                self.dispatch.run_borrowed_keyed(fh, || run(data));
            }
            Some(data) => {
                self.dispatch.submit_keyed(fh, move || run(data.as_slice()));
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
        self.dispatch.submit_keyed(fh_out.0, move || {
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
        self.dispatch
            .submit(move || match backend.getxattr(&context, inode, &name) {
                Ok(data) => reply_xattr(reply, size, Ok(data)),
                Err(error) => reply.error(fuse_errno(errno_xattr(
                    error,
                    "getxattr",
                    inode.value,
                    &name,
                    &context,
                ))),
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
                    .map_err(|error| errno_xattr(error, "setxattr", inode.value, &name, &context)),
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
    // DFS uses cached write-through and fresh-open version-bound page reuse.
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
mod dispatch_tests {
    #[test]
    fn page_cache_reuse_requires_exact_version_and_length() {
        let identity = |version: Option<&str>, length| super::PageCacheIdentity {
            version_id: version.map(str::to_owned),
            length,
        };
        let mut cache = super::PageCacheIdentities::default();
        assert!(!cache.observe(2, Some(identity(Some("same-prefix-old"), 4))));
        assert!(cache.observe(2, Some(identity(Some("same-prefix-old"), 4))));
        assert!(!cache.observe(2, Some(identity(Some("same-prefix-new"), 4))));
        assert!(!cache.observe(2, Some(identity(Some("same-prefix-new"), 8))));
        assert!(!cache.observe(2, None));
        assert!(!cache.observe(2, Some(identity(Some("same-prefix-new"), 8))));
        assert!(!cache.observe(3, Some(identity(None, 0))));
        assert!(cache.observe(3, Some(identity(None, 0))));
        assert!(!cache.observe(3, Some(identity(None, 1))));
        cache.remove(3);
        assert!(!cache.observe(3, Some(identity(None, 0))));
    }

    #[test]
    fn page_cache_identity_metadata_has_entry_and_version_byte_limits() {
        let mut cache = super::PageCacheIdentities::default();
        for ino in 0..=super::PAGE_CACHE_IDENTITIES {
            assert!(!cache.observe(
                ino as u64,
                Some(super::PageCacheIdentity {
                    version_id: None,
                    length: 0
                })
            ));
        }
        assert_eq!(cache.entries.len(), super::PAGE_CACHE_IDENTITIES);
        assert!(!cache.entries.contains_key(&0));
        let oversized = super::PageCacheIdentity {
            version_id: Some("x".repeat(super::PAGE_CACHE_VERSION_BYTES + 1)),
            length: 1,
        };
        assert!(!cache.observe(1, Some(oversized)));
        assert!(!cache.entries.contains_key(&1));
        let large = |value| super::PageCacheIdentity {
            version_id: Some(
                std::iter::repeat_n(value, super::PAGE_CACHE_VERSION_BYTES / 2).collect(),
            ),
            length: 1,
        };
        assert!(!cache.observe(1, Some(large('a'))));
        assert!(!cache.observe(2, Some(large('b'))));
        assert!(cache.observe(1, Some(large('a'))));
        assert!(!cache.observe(3, Some(large('c'))));
        assert!(!cache.entries.contains_key(&2));
        assert!(cache.entries.contains_key(&1));
        assert!(cache.version_bytes <= super::PAGE_CACHE_VERSION_BYTES);
    }

    #[test]
    fn fuse_mount_options_wire_threads_clone_and_dfs_ttls_only() {
        let options = super::FuseMountOptions {
            fuse_threads: 2,
            fuse_clone_fd: true,
            fuse_workers: 1,
            dfs_fuse_entry_ttl_ms: 23,
            dfs_fuse_attr_ttl_ms: 7,
            ..Default::default()
        };
        let config = super::configured_mount("afs-unit", false, options);
        assert_eq!(config.n_threads, Some(2));
        assert!(config.clone_fd);
        let single = super::configured_mount(
            "afs-unit",
            false,
            super::FuseMountOptions {
                fuse_threads: 1,
                ..options
            },
        );
        assert!(!single.clone_fd);
        let mut fs =
            super::AfsFuse::with_mount_options(Arc::new(OwnedReadBackend::default()), options);
        assert_eq!(fs.dispatch.capacity, 8);
        assert_eq!(fs.entry_ttls(), (Duration::ZERO, Duration::ZERO));
        fs.cached_io = true;
        assert_eq!(
            fs.entry_ttls(),
            (Duration::from_millis(23), Duration::from_millis(7))
        );
        assert!(
            super::FuseMountOptions {
                fuse_threads: 0,
                ..options
            }
            .validate()
            .is_err()
        );
        assert!(
            super::FuseMountOptions {
                fuse_workers: 0,
                ..options
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn fuse_readahead_uses_the_supported_public_api_limit() {
        for (requested, ceiling, expected) in [
            (1024 * 1024, 128 * 1024, 128 * 1024),
            (4096, 128 * 1024, 4096),
            (0, 128 * 1024, 1),
            (4096, 0, 0),
            (0, 0, 0),
        ] {
            let mut effective = ceiling;
            super::configure_readahead(requested, |value| {
                if value == 0 {
                    return Err(1);
                }
                if value > ceiling {
                    return Err(ceiling);
                }
                let old = effective;
                effective = value;
                Ok(old)
            })
            .unwrap();
            assert_eq!(effective, expected);
        }
    }

    #[test]
    fn inode_cache_gates_allow_distinct_inodes_in_one_shard() {
        // Exercise the current gate lookup with two distinct inode identities.
        // The held read is the barrier; a different inode's OPEN must acquire
        // its write side before this read is released, including shard peers.
        let fs = super::AfsFuse::new(Arc::new(OwnedReadBackend::default()));
        let gates = &fs.inode_cache_gates;
        let first_ino = 7;
        let other_ino = first_ino + super::PAGE_CACHE_INODE_GATES as u64;
        let first_gate = gates.acquire(first_ino).unwrap();
        let first_read = first_gate.read().unwrap();
        let other_gate = gates.acquire(other_ino).unwrap();
        let other_open = other_gate.gate.as_ref().unwrap().try_write();
        assert!(
            other_open.is_ok(),
            "another inode in the same shard was fenced by this READ"
        );
        drop(other_open);
        drop(first_read);
    }

    fn inode_cache_gate_entries(gates: &super::InodeCacheGates) -> usize {
        gates
            .shards
            .iter()
            .map(|shard| shard.lock().unwrap().len())
            .sum()
    }

    #[test]
    fn inode_cache_gate_concurrent_acquisition_keeps_live_identity() {
        let fs = super::AfsFuse::new(Arc::new(OwnedReadBackend::default()));
        let gates = &fs.inode_cache_gates;
        let anchor = gates.acquire(17).unwrap();
        let writer = anchor.write().unwrap();
        let expected = Arc::as_ptr(anchor.gate.as_ref().unwrap()) as usize;
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..128 {
                        let lease = gates.acquire(17).unwrap();
                        assert_eq!(Arc::as_ptr(lease.gate.as_ref().unwrap()) as usize, expected);
                        assert!(lease.gate.as_ref().unwrap().try_read().is_err());
                    }
                });
            }
        });
        assert_eq!(inode_cache_gate_entries(gates), 1);
        drop(writer);
        drop(anchor);
        assert_eq!(inode_cache_gate_entries(gates), 0);
    }

    #[test]
    fn inode_cache_gate_concurrent_last_release_does_not_accumulate() {
        let fs = super::AfsFuse::new(Arc::new(OwnedReadBackend::default()));
        let gates = &fs.inode_cache_gates;
        let barrier = std::sync::Barrier::new(9);
        let mut leaked_entries = 0;
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let barrier = &barrier;
                scope.spawn(move || {
                    for round in 0..64 {
                        // A different inode each round would accumulate leaked
                        // table-only entries if simultaneous last drops race.
                        let lease = gates.acquire(round * 64 + 3).unwrap();
                        let read = lease.read().unwrap();
                        barrier.wait();
                        drop(read);
                        drop(lease);
                        barrier.wait();
                        barrier.wait();
                    }
                });
            }
            for _ in 0..64 {
                barrier.wait();
                barrier.wait();
                leaked_entries += inode_cache_gate_entries(gates);
                barrier.wait();
            }
        });
        assert_eq!(leaked_entries, 0);
        assert_eq!(inode_cache_gate_entries(gates), 0);
    }

    #[test]
    fn inode_cache_gate_error_returns_retire_leases() {
        fn failed_read(gates: &super::InodeCacheGates, ino: u64) -> std::result::Result<(), i32> {
            let lease = gates.acquire(ino)?;
            let _read = lease.read()?;
            Err(libc::ESTALE)
        }
        fn failed_open(gates: &super::InodeCacheGates, ino: u64) -> std::result::Result<(), i32> {
            let lease = gates.acquire(ino)?;
            let _open = lease.write()?;
            Err(libc::EACCES)
        }
        let fs = super::AfsFuse::new(Arc::new(OwnedReadBackend::default()));
        for ino in 0..128 {
            assert_eq!(failed_read(&fs.inode_cache_gates, ino), Err(libc::ESTALE));
            assert_eq!(failed_open(&fs.inode_cache_gates, ino), Err(libc::EACCES));
            assert_eq!(inode_cache_gate_entries(&fs.inode_cache_gates), 0);
        }
    }

    #[test]
    fn inode_cache_open_fence_drains_with_small_and_production_worker_counts() {
        for workers in [
            1,
            2,
            usize::from(super::FuseMountOptions::default().fuse_workers),
        ] {
            let dispatch = Arc::new(super::FuseDispatch::new(workers));
            let fs = super::AfsFuse::new(Arc::new(OwnedReadBackend::default()));
            let gates = fs.inode_cache_gates.clone();
            let (started_tx, started_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let first_gate = gates.clone();
            dispatch.submit_keyed(61, move || {
                let lease = first_gate.acquire(61).unwrap();
                let _read = lease.read().unwrap();
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            let (opened_tx, opened_rx) = std::sync::mpsc::channel();
            let open_gate = gates.clone();
            dispatch.submit(move || {
                let lease = open_gate.acquire(61).unwrap();
                let _open = lease.write().unwrap();
                opened_tx.send(()).unwrap();
            });
            let (second_tx, second_rx) = std::sync::mpsc::channel();
            dispatch.submit_keyed(62, move || {
                let lease = gates.acquire(61).unwrap();
                let _read = lease.read().unwrap();
                second_tx.send(()).unwrap();
            });
            let (drained_tx, drained_rx) = std::sync::mpsc::channel();
            let drain_dispatch = dispatch.clone();
            let drainer = std::thread::spawn(move || {
                drain_dispatch.close_and_drain();
                drained_tx.send(()).unwrap();
            });
            let early_drain = drained_rx.recv_timeout(Duration::from_millis(50));
            release_tx.send(()).unwrap();
            opened_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            second_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            drained_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            drainer.join().unwrap();
            assert!(early_drain.is_err());
            assert_eq!(inode_cache_gate_entries(&fs.inode_cache_gates), 0);
        }
    }

    #[test]
    fn read_batch_overlaps_but_a_queued_exclusive_turn_fences_later_reads() {
        let dispatch = super::FuseDispatch::new(3);
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let mut releases = Vec::new();
        for _ in 0..2 {
            let started = started_tx.clone();
            let (release, wait) = std::sync::mpsc::channel();
            releases.push(release);
            dispatch.submit_read_keyed(41, move || {
                started.send(()).unwrap();
                wait.recv().unwrap();
            });
        }
        let first = started_rx.recv_timeout(Duration::from_secs(1));
        let second = started_rx.recv_timeout(Duration::from_secs(1));
        let (exclusive_tx, exclusive_rx) = std::sync::mpsc::channel();
        let (release_exclusive, wait_exclusive) = std::sync::mpsc::channel();
        dispatch.submit_keyed(41, move || {
            exclusive_tx.send(()).unwrap();
            wait_exclusive.recv().unwrap();
        });
        let (late_tx, late_rx) = std::sync::mpsc::channel();
        dispatch.submit_read_keyed(41, move || late_tx.send(()).unwrap());
        let early_exclusive = exclusive_rx.try_recv();
        let early_read = late_rx.try_recv();
        for release in releases {
            release.send(()).unwrap();
        }
        let exclusive = exclusive_rx.recv_timeout(Duration::from_secs(1));
        let read_crossed_barrier = late_rx.recv_timeout(Duration::from_millis(20));
        release_exclusive.send(()).unwrap();
        let late = late_rx.recv_timeout(Duration::from_secs(1));
        dispatch.close_and_drain();
        assert!(
            first.is_ok() && second.is_ok(),
            "adjacent readers must both run"
        );
        assert!(early_exclusive.is_err() && early_read.is_err());
        assert!(exclusive.is_ok() && read_crossed_barrier.is_err() && late.is_ok());
    }

    #[test]
    fn queued_read_batch_panic_retires_only_its_own_turn() {
        let dispatch = super::FuseDispatch::new(3);
        let (initial_tx, initial_rx) = std::sync::mpsc::channel();
        let (release_initial, wait_initial) = std::sync::mpsc::channel();
        dispatch.submit_keyed(42, move || {
            initial_tx.send(()).unwrap();
            wait_initial.recv().unwrap();
        });
        initial_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let panic_started = started_tx.clone();
        dispatch.submit_read_keyed(42, move || {
            panic_started.send(()).unwrap();
            panic!("injected read failure");
        });
        let (release_read, wait_read) = std::sync::mpsc::channel();
        dispatch.submit_read_keyed(42, move || {
            started_tx.send(()).unwrap();
            wait_read.recv().unwrap();
        });
        let (tail_tx, tail_rx) = std::sync::mpsc::channel();
        dispatch.submit_keyed(42, move || tail_tx.send(()).unwrap());
        release_initial.send(()).unwrap();
        let first = started_rx.recv_timeout(Duration::from_secs(1));
        let second = started_rx.recv_timeout(Duration::from_secs(1));
        let premature_tail = tail_rx.recv_timeout(Duration::from_millis(20));
        release_read.send(()).unwrap();
        let tail = tail_rx.recv_timeout(Duration::from_secs(1));
        dispatch.close_and_drain();
        assert!(first.is_ok() && second.is_ok());
        assert!(premature_tail.is_err() && tail.is_ok());
    }

    #[test]
    fn borrowed_write_and_release_follow_the_read_batch_with_production_workers() {
        for workers in [
            1,
            2,
            usize::from(super::FuseMountOptions::default().fuse_workers),
        ] {
            let dispatch = super::FuseDispatch::new(workers);
            let events = Arc::new(std::sync::Mutex::new(Vec::new()));
            for value in [1, 2] {
                let events = events.clone();
                dispatch.submit_read_keyed(43, move || events.lock().unwrap().push(value));
            }
            let input = [7_u8; 8];
            let address = input.as_ptr() as usize;
            dispatch.run_borrowed_keyed(43, || {
                assert_eq!(input.as_ptr() as usize, address);
                let mut seen = events.lock().unwrap();
                assert_eq!(seen.len(), 2);
                seen.push(3);
            });
            let tail = events.clone();
            dispatch.submit_read_keyed(43, move || tail.lock().unwrap().push(4));
            let released = events.clone();
            dispatch.submit_keyed(43, move || released.lock().unwrap().push(5));
            dispatch.close_and_drain();
            let seen = events.lock().unwrap();
            assert!(seen[..2].contains(&1) && seen[..2].contains(&2));
            assert_eq!(&seen[2..], &[3, 4, 5]);
        }
    }

    #[test]
    fn read_diagnostic_disabled_keeps_reply_behavior() {
        let registry = afs_metrics::Registry::new();
        let metrics = crate::node::read_work::ReadWorkMetrics::register(&registry).unwrap();
        let span = metrics.diagnostic(crate::node::read_work::ReadDiagnosticStage::FuseRead);
        assert!(span.is_none());
        let mut replied = false;
        super::complete_read_reply(span, false, || replied = true);
        assert!(replied);
        assert!(metrics.diagnostics_snapshot().is_none());
    }

    #[test]
    fn read_diagnostic_covers_async_queue_and_actual_reply_completion() {
        use crate::node::read_work::{ReadDiagnosticStage, ReadWorkMetrics};
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let dispatch = super::FuseDispatch::new(1);
        let (reply_entered_tx, reply_entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let span = metrics.diagnostic(ReadDiagnosticStage::FuseRead);
        dispatch.submit_keyed(1, move || {
            super::complete_read_reply(span, true, || {
                reply_entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
        });
        reply_entered_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let before_reply_return = metrics.diagnostics_snapshot().unwrap();
        // submit_keyed has returned, but the actual reply is still blocked.
        let prematurely_completed = before_reply_return
            .records
            .iter()
            .any(|record| record.completed);
        release_tx.send(()).unwrap();
        drop(dispatch);
        assert!(!prematurely_completed);
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        assert_eq!(snapshot.records.len(), 1);
        let record = &snapshot.records[0];
        assert_eq!(record.stage, ReadDiagnosticStage::FuseRead);
        assert!(record.completed && record.success);
        assert!(record.end_ns >= record.start_ns);
    }

    #[test]
    fn read_diagnostic_keeps_error_and_unwind_reply_outcomes() {
        use crate::node::read_work::{ReadDiagnosticStage, ReadWorkMetrics};
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry)
            .unwrap()
            .with_read_diagnostics(true);
        let mut reply_errno = 0;
        super::complete_read_reply(
            metrics.diagnostic(ReadDiagnosticStage::FuseRead),
            false,
            || {
                reply_errno = libc::ESTALE;
            },
        );
        assert_eq!(reply_errno, libc::ESTALE);
        let span = metrics.diagnostic(ReadDiagnosticStage::FuseRead);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            super::complete_read_reply(span, true, || panic!("test reply unwind"));
        }));
        assert!(unwound.is_err());
        let snapshot = metrics.diagnostics_snapshot().unwrap();
        assert_eq!(snapshot.records.len(), 2);
        assert!(
            snapshot
                .records
                .iter()
                .all(|record| record.completed && !record.success)
        );
    }

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

    #[derive(Default)]
    struct OwnedReadBackend {
        owned: Vec<u8>,
        buffered: &'static [u8],
        read_calls: std::sync::atomic::AtomicUsize,
        read_owned_calls: std::sync::atomic::AtomicUsize,
    }

    impl super::Backend for OwnedReadBackend {
        fn root_inode(&self) -> super::BackendInode {
            super::BackendInode { value: 1 }
        }

        fn read_owned(
            &self,
            _ctx: &super::RequestContext,
            handle: super::FileHandle,
            offset: u64,
            size: usize,
        ) -> super::Result<Vec<u8>> {
            self.read_owned_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(handle, super::FileHandle(9));
            assert_eq!(offset, 4);
            assert_eq!(size, 32);
            Ok(self.owned.clone())
        }

        fn read(
            &self,
            _ctx: &super::RequestContext,
            handle: super::FileHandle,
            offset: u64,
            out: &mut [u8],
        ) -> super::Result<usize> {
            self.read_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(handle, super::FileHandle(9));
            assert_eq!(offset, 4);
            out[..self.buffered.len()].copy_from_slice(self.buffered);
            Ok(self.buffered.len())
        }
    }

    fn test_context() -> super::RequestContext {
        super::RequestContext {
            uid: 1000,
            gid: 1000,
            pid: 1234,
            umask: 0,
            supplementary_gids: Vec::new(),
        }
    }

    #[test]
    fn backend_read_data_prefers_owned_response_without_buffered_read() {
        let backend = OwnedReadBackend {
            owned: b"private".to_vec(),
            buffered: b"fallback",
            read_calls: std::sync::atomic::AtomicUsize::new(0),
            read_owned_calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let data = super::backend_read_data(&backend, &test_context(), super::FileHandle(9), 4, 32)
            .unwrap();
        assert_eq!(data, b"private");
        assert_eq!(
            backend
                .read_owned_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            backend.read_calls.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[test]
    fn backend_read_data_uses_backend_default_owned_fallback() {
        struct BufferedOnlyBackend {
            read_calls: std::sync::atomic::AtomicUsize,
        }

        impl super::Backend for BufferedOnlyBackend {
            fn root_inode(&self) -> super::BackendInode {
                super::BackendInode { value: 1 }
            }

            fn read(
                &self,
                _ctx: &super::RequestContext,
                handle: super::FileHandle,
                offset: u64,
                out: &mut [u8],
            ) -> super::Result<usize> {
                self.read_calls
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                assert_eq!(handle, super::FileHandle(9));
                assert_eq!(offset, 4);
                out[..8].copy_from_slice(b"fallback");
                Ok(8)
            }
        }

        let backend = BufferedOnlyBackend {
            read_calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let data = super::backend_read_data(&backend, &test_context(), super::FileHandle(9), 4, 32)
            .unwrap();
        assert_eq!(data, b"fallback");
        assert_eq!(
            backend.read_calls.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
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
    fn fuse_io_size_metrics_count_callback_bytes_and_cumulative_boundaries() {
        let registry = afs_metrics::Registry::new();
        let metrics = super::FuseRequestMetrics::register(&registry).unwrap();
        let sizes = [
            4096, 4097, 65_536, 65_537, 262_144, 262_145, 1_048_576, 1_048_577,
        ];
        for size in sizes {
            metrics.record_io("dfs", "read", size);
        }
        metrics.record_io("ownerfs", "write", 0);
        metrics.record_io("ownerfs", "write", 131_072);
        let read = metrics.io_size_bytes.with_label_values(&["dfs", "read"]);
        assert_eq!(read.get_sample_count(), sizes.len() as u64);
        assert_eq!(read.get_sample_sum(), sizes.iter().sum::<usize>() as f64);
        assert_eq!(
            metrics.requests.with_label_values(&["dfs", "read"]).get(),
            8
        );
        let write = metrics
            .io_size_bytes
            .with_label_values(&["ownerfs", "write"]);
        assert_eq!(write.get_sample_count(), 2);
        assert_eq!(write.get_sample_sum(), 131_072.0);
        assert_eq!(
            metrics
                .requests
                .with_label_values(&["ownerfs", "write"])
                .get(),
            2
        );
        let text = afs_metrics::encode_text(&registry).unwrap();
        for (bound, count) in [
            ("4096", 1),
            ("65536", 3),
            ("262144", 5),
            ("1048576", 7),
            ("+Inf", 8),
        ] {
            assert!(
                text.lines().any(|line| {
                    line.starts_with("afs_fuse_callback_size_bytes_bucket{")
                        && line.contains("filesystem=\"dfs\"")
                        && line.contains("operation=\"read\"")
                        && line.contains(&format!("le=\"{bound}\""))
                        && line.ends_with(&format!(" {count}"))
                }),
                "missing cumulative bucket le={bound}, count={count}: {text}"
            );
        }
    }

    #[test]
    fn fuse_io_size_metrics_share_registry_and_export_only_bounded_io_labels() {
        let registry = afs_metrics::Registry::new();
        let first = super::FuseRequestMetrics::register(&registry).unwrap();
        let same = super::FuseRequestMetrics::register(&registry.clone()).unwrap();
        let other_registry = afs_metrics::Registry::new();
        let other = super::FuseRequestMetrics::register(&other_registry).unwrap();
        first.record_io("dfs", "write", 4096);
        same.record_io("dfs", "write", 65_536);
        assert_eq!(
            first
                .io_size_bytes
                .with_label_values(&["dfs", "write"])
                .get_sample_count(),
            2
        );
        assert_eq!(
            other
                .io_size_bytes
                .with_label_values(&["dfs", "write"])
                .get_sample_count(),
            0
        );
        let text = afs_metrics::encode_text(&registry).unwrap();
        for filesystem in ["ownerfs", "dfs"] {
            for operation in ["read", "write"] {
                assert!(text.lines().any(|line| {
                    line.starts_with("afs_fuse_callback_size_bytes_count{")
                        && line.contains(&format!("filesystem=\"{filesystem}\""))
                        && line.contains(&format!("operation=\"{operation}\""))
                }));
            }
        }
        assert_eq!(
            text.lines()
                .filter(|line| line.starts_with("afs_fuse_callback_size_bytes_count{"))
                .count(),
            4
        );
        assert_eq!(
            text.lines()
                .filter(|line| line.starts_with("afs_fuse_callback_size_bytes_bucket{"))
                .count(),
            20
        );
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
        assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
    }

    #[test]
    fn full_unkeyed_queue_never_executes_on_the_submitting_thread() {
        let dispatch = Arc::new(FuseDispatch::new(1));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        dispatch.submit(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        for _ in 1..dispatch.capacity {
            dispatch.submit(|| {});
        }
        let (submitting_tx, submitting_rx) = mpsc::channel();
        let (admitted_tx, admitted_rx) = mpsc::channel();
        let (executed_tx, executed_rx) = mpsc::channel();
        let producer_dispatch = dispatch.clone();
        let producer = std::thread::spawn(move || {
            let producer_id = std::thread::current().id();
            submitting_tx.send(producer_id).unwrap();
            producer_dispatch.submit(move || {
                executed_tx.send(std::thread::current().id()).unwrap();
            });
            admitted_tx.send(()).unwrap();
        });
        let producer_id = submitting_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let early_admission = admitted_rx.recv_timeout(Duration::from_millis(100));
        // Release every accepted callback before asserting, including on RED.
        release_tx.send(()).unwrap();
        let executed_id = executed_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        producer.join().unwrap();
        dispatch.close_and_drain();
        assert!(
            early_admission.is_err(),
            "full queue did not backpressure ingress"
        );
        assert_ne!(executed_id, producer_id, "full queue executed on ingress");
    }

    struct RejectedCallbackCapture(mpsc::Sender<()>);

    impl Drop for RejectedCallbackCapture {
        fn drop(&mut self) {
            // ReplyRaw is not publicly constructible in fuser. This probe locks
            // the closure ownership boundary: rejecting a callback must drop
            // its captured reply, whose official Drop sends the kernel EIO.
            let _ = self.0.send(());
        }
    }

    #[test]
    fn closing_dispatch_drops_a_blocked_callback_without_running_its_body() {
        for workers in [
            1,
            usize::from(super::FuseMountOptions::default().fuse_workers),
        ] {
            for access in [super::FuseAccess::Exclusive, super::FuseAccess::Read] {
                let dispatch = Arc::new(FuseDispatch::new(workers));
                let (started_tx, started_rx) = mpsc::channel();
                let (release_tx, release_rx) = mpsc::channel();
                // An exclusive predecessor keeps both queued access kinds behind
                // the same handle barrier even with production worker concurrency.
                dispatch.submit_access(51, super::FuseAccess::Exclusive, move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                });
                started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                for _ in 1..dispatch.capacity {
                    dispatch.submit_access(51, access, || {});
                }
                let (submitting_tx, submitting_rx) = mpsc::channel();
                let (returned_tx, returned_rx) = mpsc::channel();
                let (body_tx, body_rx) = mpsc::channel();
                let (dropped_tx, dropped_rx) = mpsc::channel();
                let producer_dispatch = dispatch.clone();
                let producer = std::thread::spawn(move || {
                    let captured_reply = RejectedCallbackCapture(dropped_tx);
                    submitting_tx.send(()).unwrap();
                    producer_dispatch.submit_access(52, access, move || {
                        let _captured_reply = captured_reply;
                        body_tx.send(()).unwrap();
                    });
                    returned_tx.send(()).unwrap();
                });
                submitting_rx.recv_timeout(Duration::from_secs(1)).unwrap();
                let early_admission = returned_rx.recv_timeout(Duration::from_millis(100));
                let drain_dispatch = dispatch.clone();
                let drainer = std::thread::spawn(move || drain_dispatch.close_and_drain());
                let rejected = returned_rx.recv_timeout(Duration::from_secs(1));
                let captured_reply_dropped = dropped_rx.recv_timeout(Duration::from_secs(1));
                release_tx.send(()).unwrap();
                producer.join().unwrap();
                drainer.join().unwrap();
                assert!(early_admission.is_err(), "full queue admitted a callback");
                assert!(rejected.is_ok(), "closing left an ingress waiter blocked");
                assert!(
                    captured_reply_dropped.is_ok(),
                    "rejected reply was retained"
                );
                assert!(
                    body_rx.try_recv().is_err(),
                    "closing executed an unaccepted callback"
                );
                assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
            }
        }
    }

    #[test]
    fn owned_callback_panic_retires_slot_and_runs_its_fifo_successor() {
        let dispatch = FuseDispatch::new(1);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        dispatch.submit_keyed(53, move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            panic!("injected owned callback panic");
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (successor_tx, successor_rx) = mpsc::channel();
        dispatch.submit_keyed(53, move || successor_tx.send(()).unwrap());
        release_tx.send(()).unwrap();
        successor_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        dispatch.close_and_drain();
        assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
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

    fn wait_for_admitted_jobs(dispatch: &FuseDispatch, count: usize) {
        // Test observation must not share a production worker/capacity wakeup.
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let jobs = dispatch.shared.queue.lock().unwrap().jobs;
            if jobs >= count {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "expected {count} admitted jobs, got {jobs}"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn borrowed_write_keeps_fifo_and_input_address_without_occupying_worker() {
        let dispatch = Arc::new(FuseDispatch::new(1));
        let (events_tx, events_rx) = mpsc::channel();
        let (first_release_tx, first_release_rx) = mpsc::channel();
        let (borrowed_release_tx, borrowed_release_rx) = mpsc::channel();
        let first_tx = events_tx.clone();
        dispatch.submit_keyed(41, move || {
            first_tx.send("write:start").unwrap();
            first_release_rx.recv().unwrap();
            first_tx.send("write:finish").unwrap();
        });
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            "write:start"
        );
        let setattr_tx = events_tx.clone();
        dispatch.submit_keyed(41, move || setattr_tx.send("resize").unwrap());
        let input = [7_u8; 32];
        let input_pointer = input.as_ptr() as usize;
        std::thread::scope(|scope| {
            let borrowed_dispatch = dispatch.clone();
            let borrowed_tx = events_tx.clone();
            let input = &input;
            scope.spawn(move || {
                borrowed_dispatch.run_borrowed_keyed(41, || {
                    assert_eq!(input.as_ptr() as usize, input_pointer);
                    assert_eq!(input, &[7_u8; 32]);
                    borrowed_tx.send("borrowed:start").unwrap();
                    borrowed_release_rx.recv().unwrap();
                    borrowed_tx.send("borrowed:finish").unwrap();
                });
            });
            wait_for_admitted_jobs(&dispatch, 3);
            let sync_tx = events_tx.clone();
            dispatch.submit_keyed(41, move || sync_tx.send("fsync").unwrap());
            let close_tx = events_tx.clone();
            dispatch.submit_keyed(41, move || close_tx.send("close").unwrap());
            first_release_tx.send(()).unwrap();
            for expected in ["write:finish", "resize", "borrowed:start"] {
                assert_eq!(
                    events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                    expected
                );
            }
            let other_tx = events_tx.clone();
            dispatch.submit_keyed(42, move || other_tx.send("other-handle").unwrap());
            assert_eq!(
                events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                "other-handle"
            );
            borrowed_release_tx.send(()).unwrap();
            for expected in ["borrowed:finish", "fsync", "close"] {
                assert_eq!(
                    events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
                    expected
                );
            }
        });
        dispatch.close_and_drain();
        assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
    }

    #[test]
    fn kernel_writeback_does_not_hold_the_fuse_receiver() {
        use crate::node::vfs::WriteAdmissionMode;
        let dispatch = Arc::new(FuseDispatch::new(2));
        let receiver = dispatch.clone();
        let (release, blocked) = std::sync::mpsc::channel();
        let (progress, observed) = std::sync::mpsc::channel();
        let (tail, tail_rx) = std::sync::mpsc::channel();
        let receive = std::thread::spawn(move || {
            let write = move || {
                blocked.recv().unwrap();
            };
            match super::fuse_write_admission(
                WriteAdmissionMode::Borrowed,
                fuser::WriteFlags::FUSE_WRITE_CACHE,
            ) {
                WriteAdmissionMode::Borrowed => receiver.run_borrowed_keyed(51, write),
                WriteAdmissionMode::Queued => receiver.submit_keyed(51, write),
            }
            receiver.submit_keyed(52, move || {
                progress.send(()).unwrap();
            });
            receiver.submit_keyed(51, move || {
                tail.send(()).unwrap();
            });
        });
        let independent = observed.recv_timeout(Duration::from_secs(1));
        let premature_sync = tail_rx.try_recv();
        release.send(()).unwrap();
        receive.join().unwrap();
        let sync = tail_rx.recv_timeout(Duration::from_secs(1));
        dispatch.close_and_drain();
        assert!(
            independent.is_ok(),
            "kernel page writeback must not stop admission of another inode's page fault"
        );
        assert!(
            premature_sync.is_err() && sync.is_ok(),
            "same-handle fsync must remain behind the accepted write"
        );
    }

    #[test]
    fn owned_write_input_waits_for_bytes_before_copying() {
        let dispatch = Arc::new(FuseDispatch::new(2));
        let held = dispatch
            .copy_write_input(&vec![7; super::MAX_OWNED_WRITE_INPUT_BYTES])
            .unwrap();
        let other = dispatch.clone();
        let (progress, observed) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let input = other.copy_write_input(&[9]).unwrap();
            progress.send(()).unwrap();
            assert_eq!(input.as_slice(), &[9]);
        });
        let early = observed.recv_timeout(Duration::from_millis(100));
        drop(held);
        if early.is_err() {
            observed.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        writer.join().unwrap();
        dispatch.close_and_drain();
        assert!(
            early.is_err(),
            "owned write inputs must not exceed the byte budget"
        );
        assert_eq!(dispatch.shared.queue.lock().unwrap().write_input_bytes, 0);
    }

    #[test]
    fn owned_write_input_shutdown_wakes_waiters_and_drops_unadmitted_body() {
        let dispatch = Arc::new(FuseDispatch::new(2));
        let held = dispatch
            .copy_write_input(&vec![7; super::MAX_OWNED_WRITE_INPUT_BYTES])
            .unwrap();
        let other = dispatch.clone();
        let (result, observed) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            result.send(other.copy_write_input(&[9]).err()).unwrap();
        });
        let early = observed.recv_timeout(Duration::from_millis(100));
        dispatch.close_and_drain();
        let error = match early {
            Ok(error) => error,
            Err(_) => observed.recv_timeout(Duration::from_secs(1)).unwrap(),
        };
        drop(held);
        writer.join().unwrap();
        assert_eq!(error, Some(libc::EIO));
        assert_eq!(dispatch.shared.queue.lock().unwrap().write_input_bytes, 0);
        let fresh = FuseDispatch::new(1);
        let input = fresh.copy_write_input(&[1; 8]).unwrap();
        fresh.close_and_drain();
        fresh.submit_keyed(1, move || {
            drop(input);
            panic!("closed dispatcher ran backend work");
        });
        assert_eq!(fresh.shared.queue.lock().unwrap().write_input_bytes, 0);
    }

    #[test]
    fn borrowed_callback_panic_retires_fifo_slot() {
        let dispatch = FuseDispatch::new(1);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            dispatch.run_borrowed_keyed(43, || panic!("injected callback panic"));
        }));
        assert!(result.is_err());
        let (finished_tx, finished_rx) = mpsc::channel();
        dispatch.submit_keyed(43, move || finished_tx.send(()).unwrap());
        finished_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        dispatch.close_and_drain();
        assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
    }

    #[test]
    fn drain_waits_for_borrowed_owner_and_queued_successor() {
        let dispatch = Arc::new(FuseDispatch::new(1));
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let borrowed_dispatch = dispatch.clone();
        let borrower = std::thread::spawn(move || {
            borrowed_dispatch.run_borrowed_keyed(44, || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            });
        });
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (successor_tx, successor_rx) = mpsc::channel();
        dispatch.submit_keyed(44, move || successor_tx.send(()).unwrap());
        let drain_dispatch = dispatch.clone();
        let (drained_tx, drained_rx) = mpsc::channel();
        let drainer = std::thread::spawn(move || {
            drain_dispatch.close_and_drain();
            drained_tx.send(()).unwrap();
        });
        let early_drain = drained_rx.recv_timeout(Duration::from_millis(50));
        release_tx.send(()).unwrap();
        successor_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drained_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        borrower.join().unwrap();
        drainer.join().unwrap();
        assert!(early_drain.is_err());
        assert_eq!(dispatch.shared.queue.lock().unwrap().jobs, 0);
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
