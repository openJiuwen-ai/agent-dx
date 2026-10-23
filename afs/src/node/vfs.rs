//! 文件后端共用的 POSIX 接入接口。
//!
//! 每个 FUSE session 在挂载时绑定一个 Backend。FUSE 节点号、持久文件身份与
//! 后端版本身份不能混为一谈；共用接口不意味着统一缓存、恢复或持久化模型。

#[cfg(feature = "dfs")]
pub mod dfs;
pub mod locks;
#[cfg(feature = "ownerfs")]
pub mod ownerfs;
pub mod types;

use std::ffi::OsStr;

use afs_error::{Error, Result};

/// FUSE write admission controls the lifetime of the borrowed request body.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteAdmissionMode {
    /// Preserve worker dispatch; the frontend owns a request-body copy.
    Queued,
    /// Complete admission before returning from the FUSE callback, joining the
    /// same handle FIFO as queued operations without retaining borrowed input.
    Borrowed,
}

/// Exact committed content identity used only for kernel page reuse at open.
/// It neither grants access nor pins a snapshot for the lifetime of a handle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PageCacheIdentity {
    pub version_id: Option<String>,
    pub length: u64,
}

/// 两种后端接收同一组文件操作；每项默认拒绝，未实现时不得报告成功。
///
/// 这里仅规定入口必须提供的参数与返回值。根授权、文件身份、缓存和持久化
/// 由具体后端决定。回调是同步的，以匹配当前 fuser 入口；后端可以用自己的
/// I/O 执行器，但不能在锁内无限等待远端或把未完成的操作假装完成。
pub trait Backend: Send + Sync {
    /// 当前 Backend 在本 mount 内的根 inode。它不是 FUSE inode，也不是跨进程身份。
    fn root_inode(&self) -> types::BackendInode;

    /// Select request-body admission for this open handle. Borrowed admission
    /// can block the receive thread through existing synchronous commit triggers;
    /// it does not change write completion or persistence semantics. Implementors
    /// must not retain the borrowed slice after `write_with_options` returns.
    fn write_admission_mode(&self, _handle: types::FileHandle) -> WriteAdmissionMode {
        WriteAdmissionMode::Queued
    }

    /// Local capability query after a successful fresh open. `None` requires
    /// discarding pages; implementations must not perform RPCs from this hook.
    fn page_cache_identity(&self, _handle: types::FileHandle) -> Option<PageCacheIdentity> {
        None
    }

    /// Permit concurrent reads only when the backend preserves per-operation
    /// version, authority and deadline checks. The frontend still orders reads
    /// against write/resize/sync/release barriers on the same handle.
    fn supports_parallel_reads(&self, _handle: types::FileHandle) -> bool {
        false
    }

    /// Linux FUSE killpriv v2 is advertised only when the selected backend can
    /// honor explicit kernel kill_suidgid causes on open/create, write and setattr.
    fn supports_killpriv_v2(&self) -> bool {
        false
    }

    /// Advisory locks are advertised to the kernel only when the selected
    /// backend can arbitrate POSIX byte-range locks and BSD flock locks across
    /// every mount/peer that can reach the same inode authority.
    fn supports_advisory_locks(&self) -> bool {
        false
    }

    fn getlk(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
        _handle: types::FileHandle,
        _request: locks::LockRequest,
    ) -> Result<Option<types::FileLockConflict>> {
        Err(unsupported("getlk"))
    }

    fn setlk(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
        _handle: types::FileHandle,
        _request: locks::LockRequest,
        _waiter: Option<locks::LockWaiterId>,
    ) -> Result<()> {
        Err(unsupported("setlk"))
    }

    fn cancel_lock_wait(&self, _waiter: locks::LockWaiterId) -> Result<()> {
        Err(unsupported("cancel_lock_wait"))
    }

    fn release_locks(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
        _handle: types::FileHandle,
        _owner: types::FileLockOwner,
        _kind: types::ReleaseKind,
    ) -> Result<()> {
        Ok(())
    }

    fn release_lock_session(&self, _ingress_session_id: &str) -> Result<()> {
        Ok(())
    }

    /// 仅在已选后端的父 inode 下查一个目录项。OwnerFs 应先由父 inode
    /// 找到 WorkspaceRoot，再校验其 RootGrant；不能按名字再查一次中心。
    fn lookup(
        &self,
        _ctx: &types::RequestContext,
        _parent: types::BackendInode,
        _name: &OsStr,
    ) -> Result<types::Entry> {
        Err(unsupported("lookup"))
    }

    /// 传入 handle 时查询打开的旧对象；同名文件被删除重建后不能转向新文件。
    fn getattr(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
        _handle: Option<types::FileHandle>,
    ) -> Result<types::FileAttributes> {
        Err(unsupported("getattr"))
    }

    /// Filesystem-wide capacity for a scoped backend inode. The default
    /// fails closed so backends never inherit fuser's zero-success statfs.
    fn statfs(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
    ) -> Result<types::FilesystemCapacity> {
        Err(unsupported("statfs"))
    }

    /// chmod/chown/truncate/时间更新共用一个可选字段结构；已打开 FD 的
    /// 属性更新优先按 handle 执行，不能依靠可能已变化的路径。
    fn setattr(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
        _handle: Option<types::FileHandle>,
        _change: &types::AttributeChange,
    ) -> Result<types::FileAttributes> {
        Err(unsupported("setattr"))
    }

    /// Options-aware setattr entry. The default implementation preserves the
    /// legacy API when no killpriv cause is present and fails closed otherwise.
    fn setattr_with_options(
        &self,
        ctx: &types::RequestContext,
        inode: types::BackendInode,
        handle: Option<types::FileHandle>,
        change: &types::AttributeChange,
        options: types::SetAttrOptions,
    ) -> Result<types::FileAttributes> {
        if options.kill_suidgid {
            return Err(unsupported("setattr kill_suidgid"));
        }
        self.setattr(ctx, inode, handle, change)
    }

    /// 一个 create 回调同时产生目录项与打开句柄；根首次创建涉及 Meta
    /// reserve/activate，是 OwnerFs 私有慢路径，不让 VFS 逐文件提交 Meta。
    fn create(
        &self,
        _ctx: &types::RequestContext,
        _parent: types::BackendInode,
        _name: &OsStr,
        _mode: u32,
        _flags: i32,
    ) -> Result<types::CreatedFile> {
        Err(unsupported("create"))
    }

    /// Options-aware create entry. FUSE_CREATE carries the same Linux open killpriv
    /// cause bit as FUSE_OPEN on kernels that support killpriv v2.
    fn create_with_options(
        &self,
        ctx: &types::RequestContext,
        parent: types::BackendInode,
        name: &OsStr,
        mode: u32,
        flags: i32,
        options: types::OpenOptions,
    ) -> Result<types::CreatedFile> {
        if options.kill_suidgid {
            return Err(unsupported("create kill_suidgid"));
        }
        self.create(ctx, parent, name, mode, flags)
    }

    /// Metadata-only special inode creation for FIFO/socket/block/char nodes.
    /// Regular files, directories and symlinks use create/mkdir/symlink because
    /// their POSIX entry points have different handle or payload contracts.
    fn mknod(
        &self,
        _ctx: &types::RequestContext,
        _parent: types::BackendInode,
        _name: &OsStr,
        _kind: types::SpecialFileKind,
        _mode: u32,
    ) -> Result<types::Entry> {
        Err(unsupported("mknod"))
    }

    /// 原始 Linux flags 由 FUSE 边缘验证后传入，后端仍必须执行权限/授权检查。
    /// O_TRUNC 必须在确认期望文件身份后生效。
    fn open(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
        _flags: i32,
    ) -> Result<types::FileHandle> {
        Err(unsupported("open"))
    }

    /// Options-aware open entry. The default implementation preserves the
    /// legacy API when no killpriv cause is present and fails closed otherwise.
    fn open_with_options(
        &self,
        ctx: &types::RequestContext,
        inode: types::BackendInode,
        flags: i32,
        options: types::OpenOptions,
    ) -> Result<types::FileHandle> {
        if options.kill_suidgid {
            return Err(unsupported("open kill_suidgid"));
        }
        self.open(ctx, inode, flags)
    }

    /// 返回写入调用者缓冲区的实际字节数；EOF 可返回 0，不补齐短读。
    fn read(
        &self,
        _ctx: &types::RequestContext,
        _handle: types::FileHandle,
        _offset: u64,
        _out: &mut [u8],
    ) -> Result<usize> {
        Err(unsupported("read"))
    }

    /// Optional owned read response for backends that can build a private final
    /// payload without first filling a caller-owned buffer. The default keeps
    /// legacy backends compatible by delegating to `read`.
    fn read_owned(
        &self,
        ctx: &types::RequestContext,
        handle: types::FileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Vec<u8>> {
        let mut out = vec![0; size];
        let n = self.read(ctx, handle, offset, &mut out)?;
        out.truncate(n);
        Ok(out)
    }

    /// 返回底层确认的实际字节数；不能把部分成功或未知结果报成整笔成功。
    /// O_APPEND 的末尾定位属于打开句柄语义，后端不能盲信传入 offset。
    fn write(
        &self,
        _ctx: &types::RequestContext,
        _handle: types::FileHandle,
        _offset: u64,
        _data: &[u8],
    ) -> Result<usize> {
        Err(unsupported("write"))
    }

    /// Options-aware write entry. The default implementation preserves the
    /// legacy API when no killpriv cause is present and fails closed otherwise.
    fn write_with_options(
        &self,
        ctx: &types::RequestContext,
        handle: types::FileHandle,
        offset: u64,
        data: &[u8],
        options: types::WriteOptions,
    ) -> Result<usize> {
        if options.kill_suidgid {
            return Err(unsupported("write kill_suidgid"));
        }
        self.write(ctx, handle, offset, data)
    }

    /// Copy file data, allowing a backend to share immutable chunks via COW.
    /// Unsupported ranges return EOPNOTSUPP for the caller's ordinary copy path.
    fn copy_file_range(
        &self,
        _ctx: &types::RequestContext,
        _range: types::CopyFileRange,
    ) -> Result<u64> {
        Err(unsupported("copy_file_range"))
    }

    /// FUSE close 的可返回错误屏障：提交该句柄先前的写入并报告错误。
    /// 只读句柄不能提交其它 writer；release 只清理资源，不承担提交。
    fn flush(&self, _ctx: &types::RequestContext, _handle: types::FileHandle) -> Result<()> {
        Err(unsupported("flush"))
    }

    /// 应用显式要求的屏障；DataOnly/Full 分别对应 fdatasync/fsync。
    fn fsync(
        &self,
        _ctx: &types::RequestContext,
        _handle: types::FileHandle,
        _mode: types::SyncMode,
    ) -> Result<()> {
        Err(unsupported("fsync"))
    }

    /// 释放本进程的句柄；不能以 path 重新寻找被 rename/unlink 的文件。
    fn release(&self, _ctx: &types::RequestContext, _handle: types::FileHandle) -> Result<()> {
        Err(unsupported("release"))
    }

    /// 返回独立目录句柄，支持 rename/unlink 后旧目录引用及 fsyncdir。
    fn opendir(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
    ) -> Result<types::DirectoryHandle> {
        Err(unsupported("opendir"))
    }

    /// `cookie` 是上一次 DirectoryEntry.next_cookie；0 表示从头开始。
    fn readdir(
        &self,
        _ctx: &types::RequestContext,
        _handle: types::DirectoryHandle,
        _cookie: u64,
        _max_entries: usize,
    ) -> Result<Vec<types::DirectoryEntry>> {
        Err(unsupported("readdir"))
    }

    /// 目录项持久化与文件内容持久化是两个边界；调用后端的目录同步。
    fn fsyncdir(
        &self,
        _ctx: &types::RequestContext,
        _handle: types::DirectoryHandle,
        _mode: types::SyncMode,
    ) -> Result<()> {
        Err(unsupported("fsyncdir"))
    }

    fn releasedir(
        &self,
        _ctx: &types::RequestContext,
        _handle: types::DirectoryHandle,
    ) -> Result<()> {
        Err(unsupported("releasedir"))
    }

    /// 子目录继承其 WorkspaceRoot 的归属，不在 Meta 创建另一个 root。
    fn mkdir(
        &self,
        _ctx: &types::RequestContext,
        _parent: types::BackendInode,
        _name: &OsStr,
        _mode: u32,
    ) -> Result<types::Entry> {
        Err(unsupported("mkdir"))
    }

    fn unlink(
        &self,
        _ctx: &types::RequestContext,
        _parent: types::BackendInode,
        _name: &OsStr,
    ) -> Result<()> {
        Err(unsupported("unlink"))
    }

    fn rmdir(
        &self,
        _ctx: &types::RequestContext,
        _parent: types::BackendInode,
        _name: &OsStr,
    ) -> Result<()> {
        Err(unsupported("rmdir"))
    }

    /// 同一后端内重命名；跨 workspace root 或跨 namespace 的策略由后端
    /// 明确拒绝，不能静默复制到另一个存储位置。
    fn rename(
        &self,
        _ctx: &types::RequestContext,
        _from_parent: types::BackendInode,
        _from_name: &OsStr,
        _to_parent: types::BackendInode,
        _to_name: &OsStr,
        _flags: types::RenameFlags,
    ) -> Result<()> {
        Err(unsupported("rename"))
    }

    fn symlink(
        &self,
        _ctx: &types::RequestContext,
        _parent: types::BackendInode,
        _name: &OsStr,
        _target: &OsStr,
    ) -> Result<types::Entry> {
        Err(unsupported("symlink"))
    }

    fn readlink(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
    ) -> Result<std::ffi::OsString> {
        Err(unsupported("readlink"))
    }

    fn link(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
        _new_parent: types::BackendInode,
        _name: &OsStr,
    ) -> Result<types::Entry> {
        Err(unsupported("link"))
    }

    /// Extended attributes use raw Linux names and values. Permission and
    /// namespace checks belong to the selected backend, including Peer calls.
    fn getxattr(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
        _name: &OsStr,
    ) -> Result<Vec<u8>> {
        Err(unsupported("getxattr"))
    }

    /// Return the Linux list encoding: each name followed by a NUL byte.
    fn listxattr(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
    ) -> Result<Vec<u8>> {
        Err(unsupported("listxattr"))
    }

    /// `flags` retains XATTR_CREATE/XATTR_REPLACE; zero means upsert. The
    /// frontend rejects invalid flag combinations before invoking the backend.
    fn setxattr(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
    ) -> Result<()> {
        Err(unsupported("setxattr"))
    }

    fn removexattr(
        &self,
        _ctx: &types::RequestContext,
        _inode: types::BackendInode,
        _name: &OsStr,
    ) -> Result<()> {
        Err(unsupported("removexattr"))
    }
}

fn unsupported(operation: &str) -> Error {
    Error::coded(
        afs_error::NODE_VFS_UNIMPLEMENTED,
        format!("VFS backend operation '{operation}' is not implemented"),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        Backend,
        types::{BackendInode, FileHandle, RequestContext},
    };
    use afs_error::Result;

    struct DefaultReadOwnedBackend;

    impl Backend for DefaultReadOwnedBackend {
        fn root_inode(&self) -> BackendInode {
            BackendInode { value: 1 }
        }

        fn read(
            &self,
            _ctx: &RequestContext,
            handle: FileHandle,
            offset: u64,
            out: &mut [u8],
        ) -> Result<usize> {
            assert_eq!(handle, FileHandle(7));
            assert_eq!(offset, 3);
            out[..5].copy_from_slice(b"owned");
            Ok(5)
        }
    }

    #[test]
    fn default_write_admission_retains_owned_worker_dispatch() {
        assert_eq!(
            DefaultReadOwnedBackend.write_admission_mode(FileHandle(7)),
            super::WriteAdmissionMode::Queued
        );
    }

    #[test]
    fn default_backend_never_opts_into_kernel_page_reuse_or_parallel_reads() {
        assert!(
            DefaultReadOwnedBackend
                .page_cache_identity(FileHandle(7))
                .is_none()
        );
        assert!(!DefaultReadOwnedBackend.supports_parallel_reads(FileHandle(7)));
    }

    #[test]
    fn default_read_owned_delegates_to_buffered_read() {
        let backend = DefaultReadOwnedBackend;
        let ctx = RequestContext {
            uid: 1000,
            gid: 1000,
            pid: 1234,
            umask: 0,
            supplementary_gids: Vec::new(),
        };
        let data = backend.read_owned(&ctx, FileHandle(7), 3, 16).unwrap();
        assert_eq!(data, b"owned");
    }
}
