//! 统一 POSIX/FUSE 入口。
//!
//! 接收内核回调，转换到 VFS `Backend` 接口并映射 errno/回复；管理 FUSE 会话内的
//! inode、目录项和打开句柄。每个 mount session 绑定一个 Backend；FUSE inode 编号
//! 不能当作后端持久文件身份。OwnerFs 的独用 Home 根可短暂缓存；首次远端
//! 访问先通过 FUSE notifier 失效本机缓存，再由 Home 执行该请求。

mod dispatch;
mod front_write;
mod metrics;
mod mount;
mod page_cache;
pub(crate) mod policy;
#[cfg(feature = "dfs")]
mod read_reply;
mod reply;
mod request;
mod state;
mod write_admission;

pub use self::metrics::FuseRequestMetrics;
#[cfg(feature = "dfs")]
pub use self::mount::mount_dfs_with_diagnostics_and_fd_bridge;
pub(crate) use self::mount::mount_with_policy;
pub use self::mount::{
    DfsMountOptions, FuseExecOptions, FuseMountOptions, MountedFuse, mount_dfs,
    mount_dfs_with_fd_bridge, mount_dfs_with_metrics_and_options, mount_dfs_with_options,
    mount_test_backend,
};
pub(crate) use self::reply::open_reply_flags;

use self::dispatch::{FuseDispatch, FuseRecoverySender};
use self::mount::{configure_readahead, mount_bounds_error, negotiated_init_flags};
use self::page_cache::{ActiveCachedViews, InodeCacheGates, PageCacheIdentities};
use self::reply::{
    FuseOp, add_backend_entries, backend_cookie, backend_read_data, complete_read_reply, errno_in,
    errno_xattr, file_attr, fuse_errno, inline_local_read_data, native_open_reply_flags,
    reply_empty, reply_statfs, reply_xattr,
};
use self::request::{
    combine_empty_results, next_ingress_session_id, os_str_bytes, request_groups, sync_mode,
    time_or_now, timestamps_now_only, unsupported_lock_errno, validate_xattr_set,
};
use self::write_admission::{
    DispatchedWrite, FrontOrdering, FrontQueuedWrite, fuse_write_admission, log_write_admission,
    ordinary_queued_write, write_kill_suidgid, xattr_front_ordering,
};

#[cfg(test)]
use self::dispatch::{FuseAccess, MAX_OWNED_WRITE_INPUT_BYTES};
#[cfg(test)]
use self::metrics::FUSE_CALLBACKS;
#[cfg(test)]
use self::mount::{
    canonical_mount_path, configured_mount, requested_init_flags, update_bdi_readahead,
};
#[cfg(test)]
use self::page_cache::{PAGE_CACHE_IDENTITIES, PAGE_CACHE_INODE_GATES, PAGE_CACHE_VERSION_BYTES};
#[cfg(test)]
use self::reply::{MAX_INLINE_LOCAL_READ_SCRATCH, xattr_response_size};
#[cfg(test)]
use self::request::{process_start, status_groups};

use self::policy::{DefaultFusePolicy, FuseMountPolicy};
use self::state::{FuseNode, FuseState};
use crate::node::api::usrbio::FdBridge;
use crate::node::vfs::types::{
    AttributeChange, BackendInode, FileLockOwner, OpenOptions, ReleaseKind, RenameFlags,
    RequestContext, SetAttrOptions, SpecialFileKind, WriteOptions, is_legacy_privilege_clear,
};
use crate::node::vfs::{Backend, WriteAdmissionMode};
use afs_error::Error;
use afs_tracing::tracing;
use afs_transport::usrbio_fd as fd_wire;
use fuser::FileHandle as FuseFileHandle;
#[cfg(any(feature = "dfs", test))]
use fuser::InitFlags;
use fuser::RenameFlags as FuseRenameFlags;
use fuser::{
    AccessFlags, CopyFileRangeFlags, Filesystem, FopenFlags, Generation, INodeNo, IoctlFlags,
    KernelConfig, LockOwner, OpenFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyIoctl, ReplyLock, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr,
    Request, TimeOrNow, WriteFlags,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

#[cfg(test)]
use self::policy::{DfsFusePolicy, FuseMountFeatures};
#[cfg(test)]
use crate::node::vfs::PageCacheIdentity;
#[cfg(test)]
use crate::node::vfs::types::{FileAttributes, FileHandle, FileKind};
#[cfg(test)]
use afs_error::Result;
#[cfg(test)]
use std::thread;
#[cfg(test)]
use std::time::UNIX_EPOCH;

const TTL: Duration = policy::UNCACHED_TTL;
const DIRECT_IO: FopenFlags = FopenFlags::FOPEN_DIRECT_IO;

/// OwnerFs 使用相同 FUSE 实现；其本地 Home 缓存策略与 notifier 接线由
/// `vfs::ownerfs` 的 FUSE 策略提供。
#[cfg(feature = "ownerfs")]
pub use crate::node::vfs::ownerfs::fuse_policy::{
    mount_ownerfs, mount_ownerfs_with_metrics, mount_ownerfs_with_metrics_and_options,
};

pub struct AfsFuse {
    fd_bridge: Option<Arc<FdBridge>>,
    #[cfg(any(feature = "dfs", test))]
    read_work: Option<crate::node::read_work::ReadWorkMetrics>,
    requests: Option<(FuseRequestMetrics, &'static str)>,
    backend: Arc<dyn Backend>,
    /// [`FuseMountFeatures::kernel_cache`].
    cached_io: bool,
    mount_options: FuseExecOptions,
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
    read_completions: Option<read_reply::ReadCompletions>,
    ingress_session_id: String,
    lock_session_cleaned: bool,
    callbacks_drained: bool,
    cleanup_error: Arc<Mutex<Option<Error>>>,
    policy: Arc<dyn FuseMountPolicy>,
}

impl AfsFuse {
    #[must_use]
    fn new(backend: Arc<dyn Backend>) -> Self {
        Self::with_mount_options(
            backend,
            FuseExecOptions {
                fuse_workers: 8,
                fuse_max_tasks: Some(64),
                ..FuseExecOptions::default()
            },
        )
    }

    fn with_mount_options(backend: Arc<dyn Backend>, mount_options: FuseExecOptions) -> Self {
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
            policy: Arc::new(DefaultFusePolicy),
        }
    }

    fn enable_write_queue(&mut self, bytes: usize) -> io::Result<()> {
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

    /// Entry and attribute TTLs for replies the mount policy does not own
    /// (mkdir/mknod/symlink/link): zero, see [`DfsFusePolicy`].
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

    fn remember_cached_inode(&self, ino: u64) {
        self.policy.remember_inode(ino);
    }

    fn with_cache_policy(&self, inode: BackendInode, reply: impl FnOnce(Duration, bool)) {
        policy::with_cache_policy(self.policy.as_ref(), inode, reply);
    }

    /// A read-only open of a policy-local inode replies on the receive thread.
    fn inline_local_open(&self, inode: BackendInode, flags: i32) -> bool {
        flags & libc::O_ACCMODE == libc::O_RDONLY && self.policy.inline_local(inode)
    }

    /// Create reply TTL and open flags. With the kernel cache feature the
    /// mount-wide entry TTL bounds both fields (official ReplyCreate has one
    /// TTL) and pages stay cacheable; otherwise the policy decides.
    fn create_reply_policy(&self, ttl: Duration, private: bool) -> (Duration, FopenFlags) {
        if self.cached_io {
            let (entry_ttl, attr_ttl) = self.entry_ttls();
            (entry_ttl.min(attr_ttl), open_reply_flags(false))
        } else {
            (ttl, open_reply_flags(!private))
        }
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
            let errno = crate::error::errno(&error);
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
        let inline_local_lookup = self.policy.inline_local(parent_inode);
        let backend = self.backend.clone();
        let state = self.state.clone();
        let context = Self::context(req, 0);
        let name = name.to_os_string();
        let mount_policy = self.policy.clone();
        let run = move || {
            let _front_guard = _front_guard;
            let result = backend
                .lookup(&context, parent_inode, &name)
                .map_err(errno_in(FuseOp::Lookup));
            match result {
                Ok(entry) => {
                    let ino = state.lock().unwrap().remember_lookup(&entry);
                    mount_policy.remember_inode(ino);
                    policy::with_cache_policy(mount_policy.as_ref(), entry.inode, |ttl, _| {
                        // A native or remote writer may rename or unlink between
                        // opens. Apply the freshness policy to names as well as
                        // attributes so an old dentry cannot open a new alias.
                        reply.entry(&ttl, &file_attr(ino, &entry.attributes), Generation(0));
                    });
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
        let mount_policy = self.policy.clone();
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
                                .map_err(errno_in(FuseOp::Getattr))
                        })
                }
                None => Err(libc::ESTALE),
            };
            let backend_inode = state.lock().unwrap().backend_inode(ino);
            match result {
                Ok(attr) => match backend_inode {
                    Some(inode) => {
                        policy::with_cache_policy(mount_policy.as_ref(), inode, |ttl, _| {
                            reply.attr(&ttl, &attr)
                        });
                    }
                    None => reply.attr(&TTL, &attr),
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
        let inline_local_statfs = self.policy.inline_local(inode);
        let backend = self.backend.clone();
        let context = Self::metadata_context(req, 0);
        let scoped = _front_guard.is_some();
        let run = move || {
            let _front_guard = _front_guard;
            match backend.statfs(&context, inode) {
                Ok(capacity) => reply_statfs(reply, capacity),
                Err(error) => reply.error(fuse_errno(errno_in(FuseOp::Statfs)(error))),
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
                    .map_err(errno_in(FuseOp::Setattr))?;
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
                        .map_err(errno_in(FuseOp::Setattr))
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
                    .map_err(errno_in(FuseOp::Readlink))
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
                    .map_err(errno_in(FuseOp::Mkdir))
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
                    .map_err(errno_in(FuseOp::Unlink))
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
                    .map_err(errno_in(FuseOp::Rmdir))
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
                    .map_err(errno_in(FuseOp::Symlink))
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
                    .map_err(errno_in(FuseOp::Rename))
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
                    .map_err(errno_in(FuseOp::Link))
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
                    .map_err(errno_in(FuseOp::Open))
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
        let inline_local_read = self.inline_local_open(inode, flags);
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
        let mount_policy = self.policy.clone();
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
                    .map_err(errno_in(FuseOp::Open))
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
                    let Some(reply) = mount_policy.reply_opened(inode, fh, reply) else {
                        return;
                    };
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
                                .map_err(errno_in(FuseOp::Read))
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
                    .map_err(errno_in(FuseOp::Read))
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
        let ordering = self.write_front_ordering(fh.0, flags, write_flags);
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
                self.submit_front_queued_write(
                    queue,
                    FrontQueuedWrite {
                        backend,
                        ino,
                        fh,
                        offset,
                        data,
                        write_flags,
                        context,
                        file,
                    },
                    reply,
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
        log_write_admission(ino, fh, offset, data.len() as u64, admission);
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
                parent: &request_span,
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
        let dispatched = DispatchedWrite {
            front_guard: _front_guard,
            backend,
            state,
            context,
            ino,
            fh,
            offset,
            options: WriteOptions {
                kill_suidgid: write_kill_suidgid(write_flags),
            },
            enqueue_at,
            source: if write_flags.contains(WriteFlags::FUSE_WRITE_CACHE) {
                "page_cache"
            } else {
                "write_call"
            },
            timing: self.requests.clone(),
            cache_ids: self.page_cache_ids.clone(),
            fuse_span,
            request_span,
            reply,
        };
        match input {
            None => {
                self.dispatch
                    .run_borrowed_keyed(self.file_dispatch_key(ino, fh), || dispatched.run(data));
            }
            Some(data) => {
                self.dispatch
                    .submit_keyed(self.file_dispatch_key(ino, fh), move || {
                        dispatched.run(data.as_slice())
                    });
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
                        .map_err(errno_in(FuseOp::CopyFileRange))
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
                    let flushed = backend
                        .flush(&context, file.handle)
                        .map_err(errno_in(FuseOp::Flush));
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
                        .map_err(errno_in(FuseOp::Flush));
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
                            .map_err(errno_in(FuseOp::Release))
                    });
                    let released_handle = backend
                        .release(&context, file.handle)
                        .map_err(errno_in(FuseOp::Release));
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
                            .map_err(errno_in(FuseOp::Fsync))
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
                        .map_err(errno_in(FuseOp::Opendir))
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
                            .map_err(errno_in(FuseOp::Readdir))
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
                    .map_err(errno_in(FuseOp::Releasedir))
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
                    .map_err(errno_in(FuseOp::Fsyncdir))
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
                    .map_err(errno_in(FuseOp::Create))
            })
        });
        match result {
            Ok(created) => {
                let ino = self.state.lock().unwrap().remember_lookup(&created.entry);
                self.remember_cached_inode(ino);
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
                    let (ttl, flags) = self.create_reply_policy(ttl, private);
                    reply.created(
                        &ttl,
                        &file_attr(ino, &created.entry.attributes),
                        Generation(0),
                        FuseFileHandle(fh),
                        flags,
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
                        .map_err(errno_in(FuseOp::Mknod))?;
                    // mknod has no application FD whose close could flush this handle.
                    // Finish the create boundary and release our temporary handle.
                    let flushed = self.backend.flush(&context, created.handle);
                    let released = self.backend.release(&context, created.handle);
                    flushed.map_err(errno_in(FuseOp::Mknod))?;
                    released.map_err(errno_in(FuseOp::Mknod))?;
                    Ok(created.entry)
                }
                libc::S_IFIFO => self
                    .backend
                    .mknod(&context, parent, name, SpecialFileKind::Fifo, permissions)
                    .map_err(errno_in(FuseOp::Mknod)),
                libc::S_IFSOCK => self
                    .backend
                    .mknod(&context, parent, name, SpecialFileKind::Socket, permissions)
                    .map_err(errno_in(FuseOp::Mknod)),
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
                    .map_err(errno_in(FuseOp::Mknod)),
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
                    .map_err(errno_in(FuseOp::Mknod)),
                _ => Err(libc::EINVAL),
            });
        match result {
            Ok(entry) => {
                let ino = self.state.lock().unwrap().remember_lookup(&entry);
                self.remember_cached_inode(ino);
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
                Ok(data) => reply_xattr(reply, FuseOp::Getxattr, size, Ok(data)),
                Err(error) => reply.error(fuse_errno(errno_xattr(
                    error,
                    FuseOp::Getxattr,
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
            reply_xattr(
                reply,
                FuseOp::Listxattr,
                size,
                backend.listxattr(&context, inode),
            );
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
                    .map_err(|error| {
                        errno_xattr(error, FuseOp::Setxattr, inode.value, &name, &context)
                    }),
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
                        errno_xattr(error, FuseOp::Removexattr, inode.value, &name, &context)
                    }),
            );
        });
    }
}

#[cfg(test)]
mod mount_path_tests;

#[cfg(test)]
mod dispatch_tests;

#[cfg(test)]
mod failure_grade_tests;

#[cfg(test)]
mod local_reply_tests;

#[cfg(test)]
mod policy_tests;

#[cfg(test)]
mod queue_default_tests;
