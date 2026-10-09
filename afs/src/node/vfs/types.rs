//! FUSE 与业务后端之间的文件操作合同；这里不保存 OwnerFs 或 DFS 的权威状态。
//!
//! 入口的 FUSE inode 只在一个挂载会话内有效。后端 inode/句柄也是 Node 进程内的
//! 不透明编号；OwnerFs 的可恢复文件身份和 DFS 的版本身份由各自后端另行保存，
//! 不能把下面的编号写进 Meta 充当持久身份。

use std::{ffi::OsString, time::SystemTime};

/// 由入口认证后的调用者身份；FUSE 的 `Request` 不泄露给业务实现。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestContext {
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
    /// 仅创建操作使用；后端仍须执行自己的权限检查。
    pub umask: u32,
    /// Authenticated ingress groups for metadata authorization; never inferred
    /// from the daemon's own credentials.
    pub supplementary_gids: Vec<u32>,
}

/// 当前 Backend 内的会话 inode。`value` 不能当作磁盘 inode、持久文件身份、
/// Meta RootId 或 FileVersionId。
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BackendInode {
    pub value: u64,
}

/// 一次成功 open 后产生的进程内句柄，始终引用打开时的文件身份。
/// rename/unlink 后不能退化为按旧路径重新打开；daemon 重启后旧句柄失效。
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileHandle(pub u64);

/// Lock namespace: Linux keeps POSIX byte-range locks and BSD flock locks
/// independent for conflict checks.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FileLockKind {
    Posix,
    Flock,
}

/// Stable lock owner supplied by the ingress session and kernel lock owner.
///
/// `pid` is deliberately not part of ownership. It is only reported by
/// `F_GETLK`. POSIX owners are process owners and are not keyed by file handle;
/// flock owners represent open-file descriptions as supplied by the frontend.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FileLockOwner {
    pub ingress_session_id: String,
    pub kernel_owner: u64,
}

/// Inclusive byte range used by FUSE lock operations. `u64::MAX` represents EOF.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FileLockRange {
    pub start: u64,
    pub end: u64,
}

impl FileLockRange {
    pub fn is_valid(self) -> bool {
        self.start <= self.end
    }

    pub fn overlaps(self, other: &Self) -> bool {
        self.start <= other.end && other.start <= self.end
    }

    pub fn touches_or_overlaps(self, other: &Self) -> bool {
        self.overlaps(other)
            || self
                .end
                .checked_add(1)
                .is_some_and(|next| next == other.start)
            || other
                .end
                .checked_add(1)
                .is_some_and(|next| next == self.start)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FileLockType {
    Read,
    Write,
    Unlock,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileLockConflict {
    pub kind: FileLockKind,
    pub owner: FileLockOwner,
    pub pid: u32,
    pub range: FileLockRange,
    pub lock_type: FileLockType,
}

/// Close path that releases advisory locks for one kernel owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseKind {
    /// POSIX byte-range locks are process-owned and Linux drops them on any fd
    /// close for the inode by that process.
    PosixOwner,
    /// BSD flock locks are open-file-description owned and are dropped only on
    /// the final FUSE release that carries a lock owner.
    FlockOwner,
}

/// `opendir` 的游标身份，与普通文件句柄分开，避免误用 `fsyncdir`。
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DirectoryHandle(pub u64);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpecialFileKind {
    Fifo,
    Socket,
    BlockDevice { rdev: u64 },
    CharDevice { rdev: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileKind {
    Regular,
    Directory,
    Symlink,
    Special(SpecialFileKind),
}

/// 后端返回的 POSIX 可见属性；时间取自事实源，不从 FUSE 缓存推测。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileAttributes {
    pub kind: FileKind,
    pub size: u64,
    /// Allocated space in POSIX 512-byte units; holes do not consume blocks.
    /// This is independent of logical EOF and does not count replica copies.
    pub blocks: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
}

/// Filesystem-wide capacity snapshot returned to FUSE `statfs`.
///
/// Values are copied from a concrete capacity authority such as Linux
/// `statvfs`. They are observations, not reservations or quotas.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilesystemCapacity {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
    pub namelen: u32,
    pub frsize: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Entry {
    pub inode: BackendInode,
    pub attributes: FileAttributes,
}

/// `create` 必须同时返回新目录项和已打开句柄，供 FUSE 原子回复。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatedFile {
    pub entry: Entry,
    pub handle: FileHandle,
}

/// 目录 cookie 由后端产生；入口不以数组下标假装稳定的继续位置。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DirectoryEntry {
    /// Linux 文件名允许非 UTF-8 字节。
    pub name: OsString,
    pub inode: BackendInode,
    pub kind: FileKind,
    pub next_cookie: u64,
}

/// 只有显式出现的字段才修改；`handle` 优先于路径身份，保护旧 FD 语义。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AttributeChange {
    pub size: Option<u64>,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub atime: Option<SystemTime>,
    pub mtime: Option<SystemTime>,
}

/// Exact legacy kernel mode clearing, with no permission or ownership grant.
/// Valid only at an ingress that enforces kernel default_permissions; callers
/// must recheck authoritative attributes before applying the clearing.
pub(crate) fn is_legacy_privilege_clear(
    current: &FileAttributes,
    change: &AttributeChange,
) -> bool {
    let mut cleared = current.mode & !libc::S_ISUID;
    if current.mode & libc::S_IXGRP != 0 {
        cleared &= !libc::S_ISGID;
    }
    current.kind == FileKind::Regular
        && change.uid.is_none()
        && change.gid.is_none()
        && cleared != current.mode
        && change.mode.map(|mode| mode & 0o7777) == Some(cleared & 0o7777)
}

/// Linux FUSE killpriv v2 cause supplied by the kernel for open/create paths.
///
/// When set, the backend must clear suid and executable sgid privilege bits as
/// part of the same trusted owner/open lease operation. This is a kernel cause,
/// not a heuristic derived from uid, mode bits, or daemon credentials.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OpenOptions {
    pub kill_suidgid: bool,
}

/// Linux FUSE killpriv v2 cause supplied by the kernel for write paths.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WriteOptions {
    pub kill_suidgid: bool,
}

/// Kernel privilege clearing for setattr. The FUSE adapter may also mark an
/// exact legacy mode-clear request after default_permissions kernel checks;
/// Home must revalidate it against current attributes, never assign stale mode.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SetAttrOptions {
    pub kill_suidgid: bool,
    /// True when every supplied atime/mtime value used the kernel NOW marker
    /// and none supplied a specific timestamp. This preserves POSIX timestamp
    /// authorization intent for Meta/owner backends without exposing macOS
    /// setattr flags or re-deriving it from flattened SystemTime values.
    pub timestamps_now: bool,
}

/// `DataOnly` 对应 fdatasync；`Full` 对应 fsync。
/// 两者都不能被普通 write/flush/close 自动代替。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncMode {
    DataOnly,
    Full,
}

/// 除 Linux `RENAME_NOREPLACE`/`RENAME_EXCHANGE` 外的标志必须显式拒绝。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RenameFlags(pub u32);
