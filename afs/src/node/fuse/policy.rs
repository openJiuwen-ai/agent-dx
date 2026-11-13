//! 每个 FUSE 挂载的后端策略与显式特性集。
//!
//! 公共 FUSE 层只通过 [`FuseMountPolicy`] 询问内核缓存 TTL、是否可在接收线程
//! 内联处理、open 回复标志与 notifier 接线；DFS 专属的写队列、读流水线、
//! usrbio fd 桥与内核页缓存由 [`FuseMountFeatures`] 在挂载时显式打开，
//! 不再由"哪个 mount 函数设置了哪个字段"隐式推断后端。

use std::{sync::Arc, time::Duration};

use fuser::{Notifier, ReplyOpen};

use crate::node::{
    api::usrbio::FdBridge, read::admission::ReadPipelineLimits, vfs::types::BackendInode,
};

/// Kernel entry/attribute TTL used when a policy has no cache of its own.
pub(crate) const UNCACHED_TTL: Duration = Duration::ZERO;

/// Backend-specific FUSE decisions for one mounted session.
///
/// Implementations must be cheap and must not block on the network: they run
/// on FUSE receive threads and workers around every lookup/getattr/open.
pub(crate) trait FuseMountPolicy: Send + Sync + 'static {
    /// Metrics `filesystem` label; the kernel fsname is `afs-<label>`.
    fn label(&self) -> &'static str;

    /// A kernel inode number was handed out for `_ino`.
    fn remember_inode(&self, _ino: u64) {}

    /// Whether a callback on `_inode` may run inline on the FUSE receive
    /// thread because it has no network wait.
    fn inline_local(&self, _inode: BackendInode) -> bool {
        false
    }

    /// Calls `reply(ttl, private)` exactly once. `private` means the kernel may
    /// cache file pages for the inode (no `FOPEN_DIRECT_IO`). Implementations
    /// may hold their own ordering lock while `reply` runs.
    fn with_cache_policy(&self, _inode: BackendInode, reply: &mut dyn FnMut(Duration, bool)) {
        reply(UNCACHED_TTL, false);
    }

    /// Replies to a regular open on the policy's own terms and returns `None`,
    /// or hands the reply back so the common page-cache identity rules decide.
    fn reply_opened(&self, _inode: BackendInode, _fh: u64, reply: ReplyOpen) -> Option<ReplyOpen> {
        Some(reply)
    }

    /// The kernel session exists; `_notifier` can invalidate its caches.
    fn on_mounted(&self, _notifier: Notifier) {}

    /// About to reply to an OPEN that admitted a new version of `_ino` to the
    /// kernel page cache (not a reopen of the version it already holds). A
    /// policy whose attribute replies follow the cached view must make the
    /// kernel discard attribute replies still in flight from before the
    /// admission. Runs before the OPEN reply; it must not block on the network.
    fn page_cache_version_changed(&self, _ino: u64) {}

    /// Attribute TTL for a reply about an inode of this kind, given the
    /// policy's entry/attribute `ttl`. The default caches every kind alike.
    fn attr_ttl(&self, _directory: bool, ttl: Duration) -> Duration {
        ttl
    }

    /// Entry and attribute TTL for replies that carry no per-inode policy
    /// decision (mkdir, mknod, symlink, link, setattr, and creates under the
    /// kernel cache feature).
    fn mount_entry_ttl(&self) -> Duration {
        UNCACHED_TTL
    }
}

/// DFS: one bounded kernel entry/attribute TTL (`dfs_kernel_cache_ttl_ms`,
/// 0 = no kernel caching). DFS has no cross-node kernel invalidation, so a
/// change made through another Node is seen here at most one TTL late;
/// changes through this mount update the kernel's own caches. Negative
/// lookups are never cached (an ENOENT reply carries no entry). Page caching
/// is governed by [`FuseMountFeatures::kernel_cache`] plus pinned versions.
pub(crate) struct DfsFusePolicy {
    kernel_ttl: Duration,
    notifier: std::sync::Mutex<Option<Notifier>>,
}

impl DfsFusePolicy {
    pub(crate) fn new(kernel_ttl: Duration) -> Self {
        Self {
            kernel_ttl,
            notifier: std::sync::Mutex::new(None),
        }
    }
}

impl FuseMountPolicy for DfsFusePolicy {
    fn label(&self) -> &'static str {
        "dfs"
    }

    fn with_cache_policy(&self, _inode: BackendInode, reply: &mut dyn FnMut(Duration, bool)) {
        reply(self.kernel_ttl, false);
    }

    fn mount_entry_ttl(&self) -> Duration {
        self.kernel_ttl
    }

    /// Only directories cache attributes. A regular file's attributes must
    /// stay uncached: a STAT without a file handle (statx passes none) can
    /// only be answered with the head version, while each open handle reads
    /// the version it pinned; the kernel keeps one `i_size` per inode, so a
    /// cached head size would make a pinned handle's READ come back short and
    /// the page cache zero-fill the gap. Uncached, the read path refreshes the
    /// size with the handle (today's semantics). Symlinks and special files
    /// follow regular files; names (entry TTL) stay cached for every kind.
    fn attr_ttl(&self, directory: bool, ttl: Duration) -> Duration {
        if directory { ttl } else { Duration::ZERO }
    }

    fn on_mounted(&self, notifier: Notifier) {
        let mut slot = self
            .notifier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = Some(notifier);
    }

    /// R97. `inval_inode(ino, -1, 0)` only invalidates attributes: it advances
    /// the inode's attr_version (v6.8 `fuse_reverse_inval_inode`), so the
    /// kernel drops any attribute reply requested before it
    /// (`fuse_change_attributes` discards replies older than attr_version).
    /// A negative offset touches no page, so it takes no page or inode lock
    /// and is safe before the OPEN reply. Requests after it reach the Node
    /// after the view was admitted and are capped to the view's length.
    fn page_cache_version_changed(&self, ino: u64) {
        let slot = self
            .notifier
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(notifier) = slot.as_ref()
            && let Err(error) = notifier.inval_inode(fuser::INodeNo(ino), -1, 0)
        {
            // The open stays valid; only the late-reply window stays open.
            afs_logging::warn!("dfs.fuse_attr_invalidation_failed";
                "ino" => ino,
                "error" => error.to_string()
            );
        }
    }
}

/// Test and contract backends: zero TTLs, direct I/O, nothing inline.
pub(crate) struct DefaultFusePolicy;

impl FuseMountPolicy for DefaultFusePolicy {
    fn label(&self) -> &'static str {
        "test"
    }
}

/// Runs `reply` through `policy`; a policy that never calls it drops the
/// captured FUSE reply, which fuser answers with an error.
pub(crate) fn with_cache_policy(
    policy: &dyn FuseMountPolicy,
    inode: BackendInode,
    reply: impl FnOnce(Duration, bool),
) {
    let mut reply = Some(reply);
    policy.with_cache_policy(inode, &mut |ttl, private| {
        if let Some(reply) = reply.take() {
            reply(ttl, private);
        }
    });
}

/// Mount-time feature set. Every field is opt-in; the empty set is what
/// OwnerFs and test backends run with.
#[derive(Clone, Default)]
pub(crate) struct FuseMountFeatures {
    /// Front-owned ordinary-write queue payload bytes.
    pub write_queue_bytes: Option<usize>,
    /// Async read pipeline limits; only used when the backend supports async
    /// reads and the `dfs` feature is compiled in.
    pub read_pipeline: Option<ReadPipelineLimits>,
    /// usrbio existing-fd controls served on this mount.
    pub fd_bridge: Option<Arc<FdBridge>>,
    /// Kernel page cache with pinned-version identities and configured
    /// read-ahead; without it every open replies `FOPEN_DIRECT_IO`.
    pub kernel_cache: bool,
}

impl FuseMountFeatures {
    /// DFS mounts: cached write-through sends each syscall write to the inode
    /// owner and keeps this mount's pages coherent. KEEP_CACHE requires an
    /// exact fresh committed identity; writeback remains disabled.
    pub(crate) fn dfs(
        options: super::DfsMountOptions,
        fd_bridge: Option<Arc<FdBridge>>,
    ) -> afs_error::Result<Self> {
        let options = options.validate()?;
        let write_queue_bytes =
            usize::try_from(options.write_queue_bytes).map_err(|_| super::mount_bounds_error())?;
        Ok(Self {
            write_queue_bytes: Some(write_queue_bytes),
            read_pipeline: Some(options.read_pipeline),
            fd_bridge,
            kernel_cache: true,
        })
    }
}
