//! 进程边缘的错误映射。领域错误不依赖 Axum 或 libc；协议只在此转换。
//! POSIX 无法携带 AFS 数字码：返回 errno，同时在结构化日志保留 code/kind。
use afs_error::{
    Error, ErrorKind, IO_DIRECTORY_NOT_EMPTY, IO_FILE_TOO_LARGE, IO_IS_DIRECTORY, IO_NAME_TOO_LONG,
    IO_NO_DATA, IO_NOT_DIRECTORY, IO_NOT_SUPPORTED, IO_OPERATION_NOT_PERMITTED,
    IO_TOO_MANY_SYMLINKS,
};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
pub struct RestError(pub Error);
impl From<Error> for RestError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}
impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        let error = self.0;
        let status = http_status(error.kind());
        macro_rules! record {
            ($level:ident) => {
                afs_logging::$level!("request failed"; "status"=>status.as_u16(), "code"=>error.code().raw(), "kind"=>format!("{:?}", error.kind()), "message"=>error.message())
            };
        }
        match rest_failure_grade(status) {
            FailureGrade::Debug => record!(debug),
            FailureGrade::Warn => record!(warn),
            FailureGrade::Error => record!(error),
        }
        (status, Json(json!({"error": {"code": error.code().raw(), "kind": format!("{:?}", error.kind()), "message": error.message()}}))).into_response()
    }
}
/// Log level for a failed request at a process edge (FUSE or REST). Only the
/// log changes with the grade; the errno or HTTP status returned never does.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FailureGrade {
    /// Expected answer (ENOENT on lookup, EEXIST on mkdir, HTTP 404).
    Debug,
    /// Permission, conflict or staleness outcome: normal under probing or
    /// concurrency, but may reveal misconfiguration, so it stays visible at the
    /// default `warn` level.
    Warn,
    /// Everything else, as before.
    Error,
}
/// REST grading: 404 answers a lookup, 409 is a create/concurrency conflict
/// and 403 a permission answer (J5); every other status, including all 5xx,
/// stays at ERROR as before.
pub(crate) fn rest_failure_grade(status: StatusCode) -> FailureGrade {
    match status {
        StatusCode::NOT_FOUND => FailureGrade::Debug,
        StatusCode::CONFLICT | StatusCode::FORBIDDEN => FailureGrade::Warn,
        _ => FailureGrade::Error,
    }
}
pub fn http_status(kind: ErrorKind) -> StatusCode {
    use ErrorKind::*;
    match kind {
        InvalidArgument | OutOfRange => StatusCode::BAD_REQUEST,
        NotFound => StatusCode::NOT_FOUND,
        AlreadyExists | Aborted => StatusCode::CONFLICT,
        PermissionDenied => StatusCode::FORBIDDEN,
        Unauthenticated => StatusCode::UNAUTHORIZED,
        ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        Unimplemented => StatusCode::NOT_IMPLEMENTED,
        Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        Cancelled => StatusCode::REQUEST_TIMEOUT,
        Unknown | Internal | DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
pub fn errno(error: &Error) -> i32 {
    if error.code() == afs_error::IO_INTERRUPTED {
        return libc::EINTR;
    }
    if error.code() == afs_error::IO_WOULD_BLOCK {
        return libc::EAGAIN;
    }
    if error.code() == afs_error::IO_DEADLOCK {
        return libc::EDEADLK;
    }
    if error.code() == afs_error::IO_NO_LOCKS {
        return libc::ENOLCK;
    }
    if error.code() == afs_error::NODE_OWNER_STALE_ACCESS
        || error.code() == afs_error::NODE_OWNER_STALE_HANDLE
    {
        return libc::ESTALE;
    }
    if error.code() == IO_NOT_DIRECTORY {
        return libc::ENOTDIR;
    }
    if error.code() == IO_IS_DIRECTORY {
        return libc::EISDIR;
    }
    if error.code() == IO_DIRECTORY_NOT_EMPTY {
        return libc::ENOTEMPTY;
    }
    if error.code() == afs_error::IO_CROSS_DEVICE {
        return libc::EXDEV;
    }
    if error.code() == afs_error::IO_OUT_OF_MEMORY {
        return libc::ENOMEM;
    }
    if error.code() == afs_error::IO_BAD_FILE_DESCRIPTOR {
        return libc::EBADF;
    }
    if error.code() == IO_NAME_TOO_LONG {
        return libc::ENAMETOOLONG;
    }
    if error.code() == IO_TOO_MANY_SYMLINKS {
        return libc::ELOOP;
    }
    if error.code() == IO_FILE_TOO_LARGE {
        return libc::EFBIG;
    }
    if error.code() == IO_NO_DATA {
        return libc::ENODATA;
    }
    if error.code() == IO_NOT_SUPPORTED {
        return libc::EOPNOTSUPP;
    }
    if error.code() == IO_OPERATION_NOT_PERMITTED {
        return libc::EPERM;
    }
    use ErrorKind::*;
    match error.kind() {
        InvalidArgument => libc::EINVAL,
        OutOfRange => libc::ERANGE,
        NotFound => libc::ENOENT,
        AlreadyExists => libc::EEXIST,
        PermissionDenied | Unauthenticated => libc::EACCES,
        ResourceExhausted => {
            if error.code() == afs_error::IO_CAPACITY {
                libc::ENOSPC
            } else {
                libc::EAGAIN
            }
        }
        FailedPrecondition => libc::EBUSY,
        Aborted => libc::EAGAIN,
        Unimplemented => libc::ENOSYS,
        Unavailable => libc::EHOSTUNREACH,
        DeadlineExceeded => libc::ETIMEDOUT,
        Cancelled => libc::EINTR,
        Unknown | Internal | DataLoss => libc::EIO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rest_grade_lowers_only_not_found_conflict_and_forbidden() {
        use ErrorKind::*;
        let cases = [
            (NotFound, StatusCode::NOT_FOUND, FailureGrade::Debug),
            (AlreadyExists, StatusCode::CONFLICT, FailureGrade::Warn),
            (Aborted, StatusCode::CONFLICT, FailureGrade::Warn),
            (PermissionDenied, StatusCode::FORBIDDEN, FailureGrade::Warn),
            (
                InvalidArgument,
                StatusCode::BAD_REQUEST,
                FailureGrade::Error,
            ),
            (
                Unauthenticated,
                StatusCode::UNAUTHORIZED,
                FailureGrade::Error,
            ),
            (
                FailedPrecondition,
                StatusCode::PRECONDITION_FAILED,
                FailureGrade::Error,
            ),
            (
                Unavailable,
                StatusCode::SERVICE_UNAVAILABLE,
                FailureGrade::Error,
            ),
            (
                Internal,
                StatusCode::INTERNAL_SERVER_ERROR,
                FailureGrade::Error,
            ),
            (
                DataLoss,
                StatusCode::INTERNAL_SERVER_ERROR,
                FailureGrade::Error,
            ),
        ];
        for (kind, status, grade) in cases {
            assert_eq!(http_status(kind), status, "{kind:?}");
            assert_eq!(rest_failure_grade(status), grade, "{kind:?}");
        }
    }
}
