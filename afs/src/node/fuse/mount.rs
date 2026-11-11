//! FUSE mount options, session ownership, kernel negotiation and mountinfo plumbing.
//!
//! Test and tool entries, compiled in because integration tests link the product
//! library without `cfg(test)`: `mount_test_backend`, `mount_dfs` and
//! `mount_dfs_with_metrics_and_options` for `tests/fuse_contract.rs`, and
//! `mount_dfs_with_fd_bridge` for `tools/entrance-probe`. Node assembly uses the
//! other mount functions.

use super::AfsFuse;
use super::metrics::FuseRequestMetrics;
use super::policy::{DfsFusePolicy, FuseMountFeatures, FuseMountPolicy};
use crate::node::api::usrbio::FdBridge;
use crate::node::vfs::Backend;
use afs_error::{Error, Result};
use fuser::{BackgroundSession, Config, InitFlags, MountOption, SessionACL};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// FUSE mount settings as configured: execution settings shared by every
/// backend plus DFS-only settings. OwnerFs mounts consume and validate only
/// [`FuseExecOptions`]; their kernel cache policy is owned by OwnerFs.
#[derive(Clone, Copy, Debug, Default)]
pub struct FuseMountOptions {
    pub exec: FuseExecOptions,
    pub dfs: DfsMountOptions,
}

/// Execution settings shared by every FUSE mount.
#[derive(Clone, Copy, Debug)]
pub struct FuseExecOptions {
    pub fuse_threads: u16,
    pub fuse_clone_fd: bool,
    pub fuse_workers: u16,
    pub fuse_max_tasks: Option<usize>,
    pub fuse_max_readahead_bytes: u32,
}

/// Settings only DFS mounts use.
#[derive(Clone, Copy, Debug)]
pub struct DfsMountOptions {
    pub read_pipeline: crate::node::read::admission::ReadPipelineLimits,
    /// Front-owned normal ordinary-write payload; downstream buffers are separate.
    pub write_queue_bytes: u64,
}

impl Default for FuseExecOptions {
    fn default() -> Self {
        Self {
            fuse_threads: 4,
            fuse_clone_fd: true,
            fuse_workers: 32,
            fuse_max_tasks: None,
            fuse_max_readahead_bytes: 16 * 1024 * 1024,
        }
    }
}

impl Default for DfsMountOptions {
    fn default() -> Self {
        Self {
            read_pipeline: Default::default(),
            write_queue_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}

impl From<&crate::config::Config> for FuseMountOptions {
    fn from(config: &crate::config::Config) -> Self {
        Self {
            exec: FuseExecOptions {
                fuse_threads: config.fuse_threads,
                fuse_clone_fd: config.fuse_clone_fd,
                fuse_workers: config.fuse_workers,
                fuse_max_tasks: Some(config.fuse_max_tasks),
                fuse_max_readahead_bytes: config.fuse_max_readahead_bytes,
            },
            dfs: DfsMountOptions {
                read_pipeline: config.dfs_read_pipeline,
                write_queue_bytes: config.dfs_write_queue_bytes,
            },
        }
    }
}

pub(super) fn mount_bounds_error() -> Error {
    Error::coded(
        afs_error::CONFIG_INVALID,
        "FUSE mount options exceed supported bounds",
    )
}

impl FuseMountOptions {
    /// Validates both parts, as a DFS mount requires.
    pub(super) fn validate(self) -> Result<Self> {
        self.dfs.read_pipeline.validate()?;
        if !self.exec.in_bounds() || !self.dfs.write_queue_in_bounds() {
            return Err(mount_bounds_error());
        }
        Ok(self)
    }
}

impl FuseExecOptions {
    pub(super) fn task_capacity(self) -> usize {
        self.fuse_max_tasks
            .unwrap_or(usize::from(self.fuse_workers) * 8)
    }

    fn in_bounds(self) -> bool {
        (1..=64).contains(&self.fuse_threads)
            && (1..=1024).contains(&self.fuse_workers)
            && self
                .fuse_max_tasks
                .is_none_or(|n| (1..=65_536).contains(&n))
            && self.fuse_max_readahead_bytes <= 16 * 1024 * 1024
    }

    pub(crate) fn validate(self) -> Result<Self> {
        if self.in_bounds() {
            Ok(self)
        } else {
            Err(mount_bounds_error())
        }
    }
}

impl DfsMountOptions {
    fn write_queue_in_bounds(self) -> bool {
        (8 * 1024 * 1024..=1024 * 1024 * 1024 * 1024).contains(&self.write_queue_bytes)
            && usize::try_from(self.write_queue_bytes).is_ok()
    }

    pub(super) fn validate(self) -> Result<Self> {
        self.read_pipeline.validate()?;
        if self.write_queue_in_bounds() {
            Ok(self)
        } else {
            Err(mount_bounds_error())
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
pub(super) fn update_bdi_readahead(setting: &Path, requested: u32) -> io::Result<(u64, u64)> {
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

pub(super) fn configure_readahead(
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

/// Process-owned mount. A clean Node stop must use normal unmount and observe
/// callback cleanup through join; upstream Drop is not a closure proof.
#[derive(Debug)]
pub struct MountedFuse {
    session: BackgroundSession,
    mount_path: PathBuf,
    mount_id: u64,
    cleanup_error: Arc<Mutex<Option<Error>>>,
}

pub(super) fn canonical_mount_path(path: &Path) -> Result<PathBuf> {
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
    read_work: crate::node::metrics::work::ReadWorkMetrics,
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
    #[cfg(any(feature = "dfs", test))] read_work: Option<
        crate::node::metrics::work::ReadWorkMetrics,
    >,
    options: FuseMountOptions,
    bridge: Option<Arc<FdBridge>>,
) -> Result<MountedFuse> {
    let options = options.validate()?;
    let features = FuseMountFeatures::dfs(options.dfs, bridge)?;
    mount_with_policy(
        backend,
        path,
        Arc::new(DfsFusePolicy),
        features,
        options.exec,
        metrics,
        #[cfg(any(feature = "dfs", test))]
        read_work,
    )
}

/// Mount `backend` with an explicit per-mount policy and feature set. Only
/// the shared execution options are validated here; feature constructors
/// validate their own settings.
pub(crate) fn mount_with_policy(
    backend: Arc<dyn Backend>,
    path: &Path,
    policy: Arc<dyn FuseMountPolicy>,
    features: FuseMountFeatures,
    exec: FuseExecOptions,
    metrics: Option<FuseRequestMetrics>,
    #[cfg(any(feature = "dfs", test))] read_work: Option<
        crate::node::metrics::work::ReadWorkMetrics,
    >,
) -> Result<MountedFuse> {
    let exec = exec.validate()?;
    let path = canonical_mount_path(path)?;
    reject_existing_mount(&path)?;
    if let Some(bridge) = &features.fd_bridge {
        bridge.bind(&backend)?;
    }
    let fsync_runtime = if backend.fsync_timeout().is_some() {
        Some(tokio::runtime::Handle::try_current().map_err(|error| {
            Error::coded(
                afs_error::CONFIG_INVALID,
                format!("DFS fsync requires a runtime: {error}"),
            )
        })?)
    } else {
        None
    };
    let mut fs = AfsFuse::with_mount_options(backend, exec);
    fs.fsync_runtime = fsync_runtime;
    fs.requests = metrics.map(|metrics| (metrics, policy.label()));
    fs.policy = policy.clone();
    fs.fd_bridge = features.fd_bridge.clone();
    if let Some(bytes) = features.write_queue_bytes {
        fs.enable_write_queue(bytes).map_err(Error::from)?;
    }
    #[cfg(any(feature = "dfs", test))]
    {
        #[cfg(feature = "dfs")]
        if let Some(metrics) = &read_work {
            let _ = fs.dispatch.shared.activity.set(metrics.activities.clone());
        }
        fs.read_work = read_work;
    }
    fs.cached_io = features.kernel_cache;
    #[cfg(feature = "dfs")]
    if let Some(limits) = features.read_pipeline {
        fs.enable_read_pipeline(limits)?;
    }
    #[cfg(not(feature = "dfs"))]
    let _ = features.read_pipeline;
    let cleanup_error = fs.cleanup_error.clone();
    let fs_name = format!("afs-{}", policy.label());
    let session = fuser::spawn_mount(fs, &path, &configured_mount(&fs_name, true, exec))
        .map_err(Error::from)?;
    #[cfg(target_os = "linux")]
    if features.kernel_cache {
        configure_mount_readahead(&path, exec.fuse_max_readahead_bytes);
    }
    if let Some(bridge) = &features.fd_bridge {
        bridge.set_notifier(session.notifier());
    }
    policy.on_mounted(session.notifier());
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

pub(super) fn configured_mount(
    fs_name: &str,
    allow_other: bool,
    options: FuseExecOptions,
) -> Config {
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

pub(super) fn requested_init_flags() -> InitFlags {
    // Linux otherwise serializes LOOKUPs in one directory even when our FUSE
    // receive thread dispatches them to independent workers. Direct-IO mmap is
    // requested so mmap on direct_io handles remains capability-gated. Native
    // DFS normally uses cached write-through and version-bound page reuse;
    // explicit direct IO and overlapping incompatible fixed-open views bypass
    // the shared inode cache. Cached handles follow close-to-open semantics.
    InitFlags::FUSE_PARALLEL_DIROPS | InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP
}

pub(super) fn negotiated_init_flags(available: InitFlags) -> impl Iterator<Item = InitFlags> {
    requested_init_flags()
        .iter()
        .filter(move |capability| available.contains(*capability))
}
