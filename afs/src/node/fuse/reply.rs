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

thread_local! {
    /// Per-thread scratch buffer for inline local reads.
    ///
    /// A single global `Mutex<Vec<u8>>` used to guard this buffer, and because
    /// `inline_local_read_data` returns `Cow::Borrowed` over it the guard had to
    /// stay alive until `reply.data()` finished. That serialised handle lookup,
    /// the backend read and the `/dev/fuse` reply across *all* files on the
    /// mount. Taking the buffer out of the thread-local and returning it after
    /// the reply keeps the reuse without holding any lock across I/O.
    static INLINE_READ_SCRATCH: std::cell::RefCell<Vec<u8>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

/// Take this thread's scratch buffer, leaving an empty one behind.
pub(super) fn take_inline_read_scratch() -> Vec<u8> {
    INLINE_READ_SCRATCH.with(|slot| std::mem::take(&mut *slot.borrow_mut()))
}

/// Return the buffer, dropping it when it grew past the inline limit so a single
/// oversized read cannot pin a big allocation per thread.
pub(super) fn return_inline_read_scratch(buffer: Vec<u8>) {
    if buffer.capacity() > MAX_INLINE_LOCAL_READ_SCRATCH {
        return;
    }
    INLINE_READ_SCRATCH.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.capacity() < buffer.capacity() {
            *slot = buffer;
        }
    });
}

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
    // Cookies 1 and 2 are "." and ".."; a backend entry's cookie is its
    // backend cookie plus 2, so every entry has a distinct resume point.
    if offset == 0 && reply.add(INodeNo(parent), 1, FileType::Directory, ".") {
        return;
    }
    if offset <= 1 && reply.add(INodeNo(ROOT_INO), 2, FileType::Directory, "..") {
        return;
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
        let cookie = entry.next_cookie.saturating_add(DOT_ENTRIES);
        if reply.add(INodeNo(ino), cookie, file_type(entry.kind), &entry.name) {
            break;
        }
    }
}

/// "." and ".." precede the backend's entries in every listing.
const DOT_ENTRIES: u64 = 2;

/// The backend cookie to resume from after FUSE `offset` (see
/// `add_backend_entries`): 0 until both dot entries have been returned.
pub(super) fn backend_cookie(offset: u64) -> u64 {
    offset.saturating_sub(DOT_ENTRIES)
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
        // R89: XATTR_CREATE on an existing name answers the caller as well.
        libc::EEXIST if op.creates_name() || op == FuseOp::Setxattr => Debug,
        libc::ENOTEMPTY if matches!(op, FuseOp::Rmdir | FuseOp::Rename) => Debug,
        libc::ENOTDIR | libc::EISDIR | libc::ENAMETOOLONG | libc::ELOOP | libc::EXDEV
            if op.is_namespace() =>
        {
            Debug
        }
        // R89: Setxattr's ENODATA is XATTR_REPLACE on an absent name.
        libc::ENODATA
            if matches!(
                op,
                FuseOp::Getxattr | FuseOp::Removexattr | FuseOp::Setxattr
            ) =>
        {
            Debug
        }
        // R89: arguments the caller chose. Rename answers EINVAL for
        // unsupported flags (the backend's Unimplemented, see `reply_errno`)
        // and for a move into its own subtree; Setattr for times DFS cannot
        // store (before 1970).
        libc::EINVAL if matches!(op, FuseOp::Rename | FuseOp::Setattr) => Debug,
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
impl FuseOp {
    /// The kernel latches ENOSYS from these callbacks for the whole mount
    /// (`no_rename2` for RENAME2), although the operation itself is supported
    /// and only this request's arguments (flags) are not. Such a reply must be
    /// EINVAL, or one RENAME_EXCHANGE attempt disables RENAME_NOREPLACE until
    /// remount (R20). Other callbacks may answer ENOSYS only when the whole
    /// operation is never supported (OwnerFs remote xattr is recorded as such).
    pub(super) fn enosys_latches_supported_operation(self) -> bool {
        matches!(self, Self::Rename)
    }
}

/// The errno a callback replies with: the edge mapping, except that ENOSYS
/// for a supported operation becomes EINVAL (see
/// [`FuseOp::enosys_latches_supported_operation`]).
pub(super) fn reply_errno(op: FuseOp, error: &Error) -> i32 {
    let errno = crate::error::errno(error);
    if errno == libc::ENOSYS && op.enosys_latches_supported_operation() {
        libc::EINVAL
    } else {
        errno
    }
}

pub(super) fn errno_in(op: FuseOp) -> impl Fn(Error) -> i32 {
    move |error| {
        let errno = reply_errno(op, &error);
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

#[cfg(test)]
mod cookie_tests {
    use super::*;

    /// R72: backend entry `i` reports next_cookie `i + 1`; its FUSE cookie
    /// is distinct from "." (1), ".." (2) and every other entry, and resuming
    /// from it continues with entry `i + 1`. The first three entries used to
    /// share cookie 3.
    #[test]
    fn readdir_cookies_are_distinct_and_resume_after_each_entry() {
        let mut seen = std::collections::HashSet::from([1, 2]);
        for index in 0..10u64 {
            let cookie = (index + 1).saturating_add(DOT_ENTRIES);
            assert!(seen.insert(cookie), "entry {index} reuses cookie {cookie}");
            assert_eq!(backend_cookie(cookie), index + 1);
        }
        assert_eq!(backend_cookie(0), 0);
        assert_eq!(backend_cookie(1), 0);
        assert_eq!(backend_cookie(2), 0);
        assert_eq!(backend_cookie(u64::MAX), u64::MAX - 2);
    }
}
