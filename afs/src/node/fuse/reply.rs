//! Reply construction and backend-to-FUSE attribute/errno conversion.

use super::DIRECT_IO;
use super::state::{FuseState, ROOT_INO};
use crate::error::FailureGrade;
use crate::node::vfs::Backend;
use crate::node::vfs::types::{
    DirectoryEntry, Entry, FileAttributes, FileHandle, FileKind, FilesystemCapacity,
    RequestContext, SpecialFileKind,
};
use afs_error::{Error, ErrorKind, Result};
use fuser::{
    Errno, FileAttr, FileType, FopenFlags, INodeNo, ReplyDirectory, ReplyEmpty, ReplyStatfs,
    ReplyXattr,
};
use std::borrow::Cow;
use std::ffi::OsStr;
use std::time::UNIX_EPOCH;

pub(super) const MAX_INLINE_LOCAL_READ_SCRATCH: usize = 1024 * 1024;

pub(super) fn inline_local_read_data(
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

pub(super) fn backend_read_data(
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
pub(super) fn complete_read_reply(
    #[cfg(any(feature = "dfs", test))] diagnostic: Option<
        crate::node::metrics::read_timing::ReadDiagnosticSpan,
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

pub(super) fn add_backend_entries(
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

pub(super) fn backend_cookie(offset: u64) -> u64 {
    if offset <= 2 { 0 } else { offset }
}

pub(super) fn file_attr(ino: u64, attributes: &FileAttributes) -> FileAttr {
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

pub(super) fn reply_empty(reply: ReplyEmpty, result: std::result::Result<(), i32>) {
    match result {
        Ok(()) => reply.ok(),
        Err(error) => reply.error(fuse_errno(error)),
    }
}

pub(super) fn fuse_errno(error: i32) -> Errno {
    Errno::from_i32(error)
}

pub(crate) fn open_reply_flags(direct_io: bool) -> FopenFlags {
    if direct_io {
        DIRECT_IO
    } else {
        FopenFlags::empty()
    }
}

pub(super) fn native_open_reply_flags(
    cached_io: bool,
    fixed_view: bool,
    flags: i32,
    cache_admitted: bool,
) -> FopenFlags {
    open_reply_flags(!cached_io || (fixed_view && !cache_admitted) || flags & libc::O_DIRECT != 0)
}

pub(super) fn xattr_response_size(
    actual: usize,
    requested: u32,
) -> std::result::Result<Option<u32>, i32> {
    let actual = u32::try_from(actual).map_err(|_| libc::E2BIG)?;
    if requested == 0 {
        Ok(Some(actual))
    } else if actual <= requested {
        Ok(None)
    } else {
        Err(libc::ERANGE)
    }
}

pub(super) fn reply_xattr(reply: ReplyXattr, op: FuseOp, requested: u32, result: Result<Vec<u8>>) {
    match result {
        Ok(data) => match xattr_response_size(data.len(), requested) {
            Ok(Some(size)) => reply.size(size),
            Ok(None) => reply.data(&data),
            Err(error) => reply.error(fuse_errno(error)),
        },
        Err(error) => reply.error(fuse_errno(errno_in(op)(error))),
    }
}

pub(super) fn reply_statfs(reply: ReplyStatfs, capacity: FilesystemCapacity) {
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

/// FUSE callback that turned a backend [`Error`] into an errno.
///
/// The operation decides whether a given errno is an expected answer (a lookup
/// of a missing name) or a fault (a read whose chunk vanished), so log grading
/// is keyed on `op × errno` rather than on the error kind alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FuseOp {
    Lookup,
    Getattr,
    Statfs,
    Setattr,
    Readlink,
    Mkdir,
    Unlink,
    Rmdir,
    Symlink,
    Rename,
    Link,
    Open,
    Read,
    Write,
    CopyFileRange,
    Flush,
    Release,
    Fsync,
    Opendir,
    Readdir,
    Releasedir,
    Fsyncdir,
    Create,
    Mknod,
    Getxattr,
    Listxattr,
    Setxattr,
    Removexattr,
}

impl FuseOp {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Lookup => "lookup",
            Self::Getattr => "getattr",
            Self::Statfs => "statfs",
            Self::Setattr => "setattr",
            Self::Readlink => "readlink",
            Self::Mkdir => "mkdir",
            Self::Unlink => "unlink",
            Self::Rmdir => "rmdir",
            Self::Symlink => "symlink",
            Self::Rename => "rename",
            Self::Link => "link",
            Self::Open => "open",
            Self::Read => "read",
            Self::Write => "write",
            Self::CopyFileRange => "copy_file_range",
            Self::Flush => "flush",
            Self::Release => "release",
            Self::Fsync => "fsync",
            Self::Opendir => "opendir",
            Self::Readdir => "readdir",
            Self::Releasedir => "releasedir",
            Self::Fsyncdir => "fsyncdir",
            Self::Create => "create",
            Self::Mknod => "mknod",
            Self::Getxattr => "getxattr",
            Self::Listxattr => "listxattr",
            Self::Setxattr => "setxattr",
            Self::Removexattr => "removexattr",
        }
    }

    /// Operations whose ENOENT is the POSIX answer about the namespace (the
    /// name or inode is absent) rather than lost data.
    fn enoent_is_answer(self) -> bool {
        matches!(
            self,
            Self::Lookup
                | Self::Getattr
                | Self::Setattr
                | Self::Readlink
                | Self::Unlink
                | Self::Rmdir
                | Self::Rename
                | Self::Link
                | Self::Opendir
                | Self::Open
                | Self::Mknod
        ) || self.is_xattr()
    }

    /// Operations that create a name, so EEXIST answers "already there".
    fn creates_name(self) -> bool {
        matches!(
            self,
            Self::Mkdir | Self::Create | Self::Mknod | Self::Symlink | Self::Link | Self::Rename
        )
    }

    /// Operations that resolve or change a path component, so ENOTDIR, EISDIR,
    /// ENAMETOOLONG, ELOOP and EXDEV describe the caller's path.
    fn is_namespace(self) -> bool {
        matches!(
            self,
            Self::Lookup
                | Self::Mkdir
                | Self::Unlink
                | Self::Rmdir
                | Self::Symlink
                | Self::Rename
                | Self::Link
                | Self::Create
                | Self::Mknod
                | Self::Open
                | Self::Opendir
        )
    }

    fn is_xattr(self) -> bool {
        matches!(
            self,
            Self::Getxattr | Self::Listxattr | Self::Setxattr | Self::Removexattr
        )
    }
}

/// Grades a failed FUSE request by `op × errno`. `errno` must be
/// `crate::error::errno(error)`; `error` disambiguates errnos shared by
/// unrelated kinds (EACCES from an authentication fault is not a POSIX
/// permission answer).
pub(super) fn fuse_failure_grade(op: FuseOp, errno: i32, error: &Error) -> FailureGrade {
    use FailureGrade::{Debug, Error as Fault, Warn};
    match errno {
        libc::EINTR => Debug,
        libc::ENOENT if op.enoent_is_answer() => Debug,
        libc::EEXIST if op.creates_name() => Debug,
        libc::ENOTEMPTY if matches!(op, FuseOp::Rmdir | FuseOp::Rename) => Debug,
        libc::ENOTDIR | libc::EISDIR | libc::ENAMETOOLONG | libc::ELOOP | libc::EXDEV
            if op.is_namespace() =>
        {
            Debug
        }
        libc::ENODATA if matches!(op, FuseOp::Getxattr | FuseOp::Removexattr) => Debug,
        // J6: the kernel probes `security.capability` on every write and the
        // DFS backend answers EOPNOTSUPP. Only the log level changes here.
        libc::EOPNOTSUPP if op.is_xattr() => Debug,
        libc::ERANGE if matches!(op, FuseOp::Getxattr | FuseOp::Listxattr) => Debug,
        // J5: POSIX permission checks. Unauthenticated also maps to EACCES but
        // is an AFS credential fault, not a permission answer.
        libc::EACCES if error.kind() == ErrorKind::Unauthenticated => Fault,
        libc::EACCES | libc::EPERM => Warn,
        libc::ESTALE => Warn,
        _ => Fault,
    }
}

/// Maps a backend error to the kernel errno for `op` and records it at the
/// grade chosen by [`fuse_failure_grade`]. The errno is exactly
/// `crate::error::errno(&error)`.
pub(super) fn errno_in(op: FuseOp) -> impl Fn(Error) -> i32 {
    move |error| {
        let errno = crate::error::errno(&error);
        macro_rules! record {
            ($level:ident) => {
                afs_logging::$level!(
                    "FUSE request failed";
                    "code" => error.code().raw(),
                    "kind" => format!("{:?}", error.kind()),
                    "message" => error.message(),
                    "op" => op.as_str(),
                    "errno" => errno,
                )
            };
        }
        match fuse_failure_grade(op, errno, &error) {
            FailureGrade::Debug => record!(debug),
            FailureGrade::Warn => record!(warn),
            FailureGrade::Error => record!(error),
        }
        errno
    }
}

/// Enriched failure log for xattr requests. The generic log carries no request
/// identity, which makes repeated xattr rejections (for example the OwnerFs
/// user.*-only policy) impossible to attribute. xattr handlers add the inode,
/// xattr name and request credentials so the log itself names the caller.
pub(super) fn errno_xattr(
    error: Error,
    op: FuseOp,
    ino: u64,
    name: &OsStr,
    context: &RequestContext,
) -> i32 {
    let errno = crate::error::errno(&error);
    macro_rules! record {
        ($level:ident) => {
            afs_logging::$level!(
                "FUSE request failed";
                "code" => error.code().raw(),
                "kind" => format!("{:?}", error.kind()),
                "message" => error.message(),
                "op" => op.as_str(),
                "errno" => errno,
                "inode" => ino,
                "name" => name.to_string_lossy().into_owned(),
                "pid" => context.pid,
                "uid" => context.uid,
                "gid" => context.gid,
            )
        };
    }
    match fuse_failure_grade(op, errno, &error) {
        FailureGrade::Debug => record!(debug),
        FailureGrade::Warn => record!(warn),
        FailureGrade::Error => record!(error),
    }
    errno
}
