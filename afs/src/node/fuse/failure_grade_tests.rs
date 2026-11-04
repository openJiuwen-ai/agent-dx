//! R52: FUSE failure grading. Pins both the log grade per `op × errno` and the
//! errno handed to the kernel, which must stay exactly as before grading.

use super::reply::{FuseOp, errno_in, fuse_failure_grade};
use crate::error::FailureGrade::{self, Debug, Error as Fault, Warn};
use afs_error::{
    Error, ErrorCode, ErrorKind, IO_ALREADY_EXISTS, IO_CAPACITY, IO_CROSS_DEVICE,
    IO_DIRECTORY_NOT_EMPTY, IO_INTERRUPTED, IO_IS_DIRECTORY, IO_NAME_TOO_LONG, IO_NO_DATA,
    IO_NOT_DIRECTORY, IO_NOT_FOUND, IO_NOT_SUPPORTED, IO_OPERATION_NOT_PERMITTED, IO_OTHER,
    IO_PERMISSION_DENIED, IO_TIMEOUT, IO_TOO_MANY_SYMLINKS, IO_WOULD_BLOCK,
    NODE_OWNER_STALE_HANDLE, NODE_TRANSFER_CORRUPT_DATA, NODE_VFS_NOT_FOUND,
};

const ALL_OPS: [FuseOp; 28] = [
    FuseOp::Lookup,
    FuseOp::Getattr,
    FuseOp::Statfs,
    FuseOp::Setattr,
    FuseOp::Readlink,
    FuseOp::Mkdir,
    FuseOp::Unlink,
    FuseOp::Rmdir,
    FuseOp::Symlink,
    FuseOp::Rename,
    FuseOp::Link,
    FuseOp::Open,
    FuseOp::Read,
    FuseOp::Write,
    FuseOp::CopyFileRange,
    FuseOp::Flush,
    FuseOp::Release,
    FuseOp::Fsync,
    FuseOp::Opendir,
    FuseOp::Readdir,
    FuseOp::Releasedir,
    FuseOp::Fsyncdir,
    FuseOp::Create,
    FuseOp::Mknod,
    FuseOp::Getxattr,
    FuseOp::Listxattr,
    FuseOp::Setxattr,
    FuseOp::Removexattr,
];

fn coded(code: ErrorCode) -> Error {
    Error::coded(code, "graded")
}

fn kind(kind: ErrorKind) -> Error {
    Error::new(IO_OTHER, kind, "graded")
}

/// `(op, error, errno before R52, grade)`. The errno column is the snapshot of
/// the pre-R52 `reply::errno` (log + `crate::error::errno`).
fn cases() -> Vec<(FuseOp, Error, i32, FailureGrade)> {
    use FuseOp::*;
    vec![
        // ENOENT answers a namespace question.
        (Lookup, coded(NODE_VFS_NOT_FOUND), libc::ENOENT, Debug),
        (Getattr, coded(IO_NOT_FOUND), libc::ENOENT, Debug),
        (Setattr, coded(IO_NOT_FOUND), libc::ENOENT, Debug),
        (Readlink, coded(IO_NOT_FOUND), libc::ENOENT, Debug),
        (Unlink, coded(IO_NOT_FOUND), libc::ENOENT, Debug),
        (Rmdir, coded(IO_NOT_FOUND), libc::ENOENT, Debug),
        (Rename, coded(IO_NOT_FOUND), libc::ENOENT, Debug),
        (Link, coded(IO_NOT_FOUND), libc::ENOENT, Debug),
        (Open, coded(IO_NOT_FOUND), libc::ENOENT, Debug),
        (Opendir, coded(IO_NOT_FOUND), libc::ENOENT, Debug),
        (Mknod, coded(IO_NOT_FOUND), libc::ENOENT, Debug),
        (Getxattr, coded(IO_NOT_FOUND), libc::ENOENT, Debug),
        // ENOENT on a data or handle operation is lost data, not an answer.
        (Read, coded(NODE_VFS_NOT_FOUND), libc::ENOENT, Fault),
        (Write, coded(IO_NOT_FOUND), libc::ENOENT, Fault),
        (CopyFileRange, coded(IO_NOT_FOUND), libc::ENOENT, Fault),
        (Flush, coded(IO_NOT_FOUND), libc::ENOENT, Fault),
        (Fsync, coded(IO_NOT_FOUND), libc::ENOENT, Fault),
        (Release, coded(IO_NOT_FOUND), libc::ENOENT, Fault),
        (Readdir, coded(IO_NOT_FOUND), libc::ENOENT, Fault),
        (Statfs, coded(IO_NOT_FOUND), libc::ENOENT, Fault),
        (Mkdir, coded(IO_NOT_FOUND), libc::ENOENT, Fault),
        (Create, coded(IO_NOT_FOUND), libc::ENOENT, Fault),
        // EEXIST on name-creating operations.
        (Mkdir, coded(IO_ALREADY_EXISTS), libc::EEXIST, Debug),
        (Create, coded(IO_ALREADY_EXISTS), libc::EEXIST, Debug),
        (Mknod, coded(IO_ALREADY_EXISTS), libc::EEXIST, Debug),
        (Symlink, coded(IO_ALREADY_EXISTS), libc::EEXIST, Debug),
        (Link, coded(IO_ALREADY_EXISTS), libc::EEXIST, Debug),
        (Rename, coded(IO_ALREADY_EXISTS), libc::EEXIST, Debug),
        (Unlink, coded(IO_ALREADY_EXISTS), libc::EEXIST, Fault),
        (Write, coded(IO_ALREADY_EXISTS), libc::EEXIST, Fault),
        // ENOTEMPTY.
        (Rmdir, coded(IO_DIRECTORY_NOT_EMPTY), libc::ENOTEMPTY, Debug),
        (
            Rename,
            coded(IO_DIRECTORY_NOT_EMPTY),
            libc::ENOTEMPTY,
            Debug,
        ),
        (
            Unlink,
            coded(IO_DIRECTORY_NOT_EMPTY),
            libc::ENOTEMPTY,
            Fault,
        ),
        // Path-shape answers on namespace operations.
        (Lookup, coded(IO_NOT_DIRECTORY), libc::ENOTDIR, Debug),
        (Rmdir, coded(IO_NOT_DIRECTORY), libc::ENOTDIR, Debug),
        (Unlink, coded(IO_IS_DIRECTORY), libc::EISDIR, Debug),
        (Open, coded(IO_IS_DIRECTORY), libc::EISDIR, Debug),
        (Create, coded(IO_NAME_TOO_LONG), libc::ENAMETOOLONG, Debug),
        (Lookup, coded(IO_TOO_MANY_SYMLINKS), libc::ELOOP, Debug),
        (Rename, coded(IO_CROSS_DEVICE), libc::EXDEV, Debug),
        (Read, coded(IO_IS_DIRECTORY), libc::EISDIR, Fault),
        (Write, coded(IO_CROSS_DEVICE), libc::EXDEV, Fault),
        // xattr answers (J6: EOPNOTSUPP only changes the log level).
        (Getxattr, coded(IO_NO_DATA), libc::ENODATA, Debug),
        (Removexattr, coded(IO_NO_DATA), libc::ENODATA, Debug),
        (Setxattr, coded(IO_NO_DATA), libc::ENODATA, Fault),
        (Getxattr, coded(IO_NOT_SUPPORTED), libc::EOPNOTSUPP, Debug),
        (Listxattr, coded(IO_NOT_SUPPORTED), libc::EOPNOTSUPP, Debug),
        (Setxattr, coded(IO_NOT_SUPPORTED), libc::EOPNOTSUPP, Debug),
        (
            Removexattr,
            coded(IO_NOT_SUPPORTED),
            libc::EOPNOTSUPP,
            Debug,
        ),
        (Read, coded(IO_NOT_SUPPORTED), libc::EOPNOTSUPP, Fault),
        (Getxattr, kind(ErrorKind::OutOfRange), libc::ERANGE, Debug),
        (Listxattr, kind(ErrorKind::OutOfRange), libc::ERANGE, Debug),
        (Write, kind(ErrorKind::OutOfRange), libc::ERANGE, Fault),
        // EINTR anywhere.
        (Read, coded(IO_INTERRUPTED), libc::EINTR, Debug),
        (Lookup, kind(ErrorKind::Cancelled), libc::EINTR, Debug),
        // J5: permission answers warn; an authentication fault stays ERROR.
        (Lookup, coded(IO_PERMISSION_DENIED), libc::EACCES, Warn),
        (Read, coded(IO_PERMISSION_DENIED), libc::EACCES, Warn),
        (
            Setattr,
            coded(IO_OPERATION_NOT_PERMITTED),
            libc::EPERM,
            Warn,
        ),
        (Unlink, coded(IO_OPERATION_NOT_PERMITTED), libc::EPERM, Warn),
        (
            Lookup,
            kind(ErrorKind::Unauthenticated),
            libc::EACCES,
            Fault,
        ),
        // Stale OwnerFs access warns.
        (Getattr, coded(NODE_OWNER_STALE_HANDLE), libc::ESTALE, Warn),
        // Faults stay ERROR.
        (Lookup, coded(IO_OTHER), libc::EIO, Fault),
        (Read, coded(NODE_TRANSFER_CORRUPT_DATA), libc::EIO, Fault),
        (Write, coded(IO_CAPACITY), libc::ENOSPC, Fault),
        (Fsync, coded(IO_TIMEOUT), libc::ETIMEDOUT, Fault),
        (Write, coded(IO_WOULD_BLOCK), libc::EAGAIN, Fault),
        (
            Getattr,
            kind(ErrorKind::Unavailable),
            libc::EHOSTUNREACH,
            Fault,
        ),
        (
            Lookup,
            kind(ErrorKind::InvalidArgument),
            libc::EINVAL,
            Fault,
        ),
        (
            Mkdir,
            kind(ErrorKind::FailedPrecondition),
            libc::EBUSY,
            Fault,
        ),
        (Open, kind(ErrorKind::Unimplemented), libc::ENOSYS, Fault),
    ]
}

#[test]
fn fuse_failure_grade_table_keeps_errno() {
    for (op, error, errno, grade) in cases() {
        assert_eq!(
            crate::error::errno(&error),
            errno,
            "errno snapshot {op:?} {error:?}"
        );
        assert_eq!(
            errno_in(op)(error.clone()),
            errno,
            "errno_in {op:?} {error:?}"
        );
        assert_eq!(
            fuse_failure_grade(op, errno, &error),
            grade,
            "grade {op:?} {error:?}"
        );
    }
}

#[test]
fn errno_in_matches_edge_mapping_for_every_op() {
    let errors: Vec<Error> = cases().into_iter().map(|(_, error, _, _)| error).collect();
    for op in ALL_OPS {
        for error in &errors {
            assert_eq!(
                errno_in(op)(error.clone()),
                crate::error::errno(error),
                "{op:?} {error:?}"
            );
        }
    }
}
