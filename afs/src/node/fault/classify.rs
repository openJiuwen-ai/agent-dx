//! Named error classifications on the Node DFS fault paths.
//!
//! Each predicate answers one question for one caller and keeps its own set on
//! purpose: the sets differ (a repair source also counts `NODE_STORAGE_INVALID`,
//! a corruption report keeps retrying `META_DFS_CONFLICT`, a claim also treats
//! `Aborted` as final). Do not merge them; the tests below pin every set.
//!
//! All predicates are pure functions of the error.

use afs_error::{Error, ErrorKind};

/// A verified local copy is missing or corrupt, so the caller may quarantine
/// it (after re-verifying the actual file). Set: {`DataLoss`, `NotFound`}.
/// Used by local reads, peer serving and verified opens in `node::chunk`.
pub(crate) fn verified_copy_is_invalid(error: &Error) -> bool {
    matches!(error.kind(), ErrorKind::DataLoss | ErrorKind::NotFound)
}

/// Opening the local repair source failed because the durable data itself is
/// missing or invalid, so the repair is reported `source_invalid`. Set:
/// code `NODE_STORAGE_INVALID` ∪ kinds {`NotFound`, `DataLoss`}. Permission,
/// transient I/O, ENOSPC and unavailable peers are not source corruption.
#[cfg(feature = "dfs")]
pub(crate) fn repair_source_is_invalid(error: &Error) -> bool {
    error.code() == afs_error::NODE_STORAGE_INVALID
        || error.kind() == ErrorKind::NotFound
        || error.kind() == ErrorKind::DataLoss
}

/// Meta definitively rejected a corruption report, so the worker stops
/// resending it. Set: kinds {`InvalidArgument`, `PermissionDenied`,
/// `Unauthenticated`, `FailedPrecondition`, `NotFound`} except code
/// `META_DFS_CONFLICT` (a retryable CAS conflict). `Aborted` and transport
/// failures keep the exact request pending.
#[cfg(feature = "dfs")]
pub(crate) fn corruption_report_definitively_rejected(error: &Error) -> bool {
    error.code() != afs_error::META_DFS_CONFLICT
        && matches!(
            error.kind(),
            ErrorKind::InvalidArgument
                | ErrorKind::PermissionDenied
                | ErrorKind::Unauthenticated
                | ErrorKind::FailedPrecondition
                | ErrorKind::NotFound
        )
}

/// Meta definitively rejected a repair claim, so the worker drops it. Set:
/// kinds {`InvalidArgument`, `PermissionDenied`, `Unauthenticated`,
/// `FailedPrecondition`, `Aborted`, `NotFound`}.
#[cfg(feature = "dfs")]
pub(crate) fn repair_request_definitively_rejected(error: &Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::InvalidArgument
            | ErrorKind::PermissionDenied
            | ErrorKind::Unauthenticated
            | ErrorKind::FailedPrecondition
            | ErrorKind::Aborted
            | ErrorKind::NotFound
    )
}

/// Meta definitively rejected a repair result report, so the pending report
/// is cleared instead of resent. Set: code `META_DFS_REPAIR_SUPERSEDED` ∪
/// kinds {`InvalidArgument`, `PermissionDenied`, `Unauthenticated`}.
#[cfg(feature = "dfs")]
pub(crate) fn repair_report_definitively_rejected(error: &Error) -> bool {
    error.code() == afs_error::META_DFS_REPAIR_SUPERSEDED
        || matches!(
            error.kind(),
            ErrorKind::InvalidArgument | ErrorKind::PermissionDenied | ErrorKind::Unauthenticated
        )
}

/// A read source refused this reader's authority, so the read fails (or the
/// cached authority is invalidated) instead of rotating to another copy.
/// Set: kinds {`PermissionDenied`, `Unauthenticated`}.
#[cfg(feature = "dfs")]
pub(crate) fn read_authority_rejected(error: &Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::PermissionDenied | ErrorKind::Unauthenticated
    )
}

/// A peer's bytes failed this reader's content verification, so that copy may
/// be bad on disk (or the bytes flipped in transit: only the holder can tell).
/// Set: code `NODE_TRANSFER_CORRUPT_DATA`. Frame protocol violations, transport
/// errors, timeouts and authorization are not content.
#[cfg(feature = "dfs")]
pub(crate) fn remote_copy_content_suspect(error: &Error) -> bool {
    error.code() == afs_error::NODE_TRANSFER_CORRUPT_DATA
}

/// A peer refused only because its slots or queue were full: the same request
/// may succeed after a short wait. Set: kind `ResourceExhausted` without code
/// `IO_CAPACITY` (a full disk is not retried). Used by the replica sender (R71b).
#[cfg(feature = "dfs")]
pub(crate) fn retryable_backpressure(error: &Error) -> bool {
    error.kind() == ErrorKind::ResourceExhausted && error.code() != afs_error::IO_CAPACITY
}

/// A peer-cache entry is corrupt or gone, so it is retired and counted as a
/// miss. Set: code `NODE_TRANSFER_CORRUPT_DATA` ∪ kind {`NotFound`}.
#[cfg(feature = "dfs")]
pub(crate) fn cached_copy_is_invalid(error: &Error) -> bool {
    error.code() == afs_error::NODE_TRANSFER_CORRUPT_DATA || error.kind() == ErrorKind::NotFound
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "dfs")]
    use afs_error::ErrorCode;

    const KINDS: [ErrorKind; 16] = [
        ErrorKind::Unknown,
        ErrorKind::Cancelled,
        ErrorKind::OutOfRange,
        ErrorKind::InvalidArgument,
        ErrorKind::NotFound,
        ErrorKind::AlreadyExists,
        ErrorKind::PermissionDenied,
        ErrorKind::ResourceExhausted,
        ErrorKind::FailedPrecondition,
        ErrorKind::Aborted,
        ErrorKind::Unimplemented,
        ErrorKind::Internal,
        ErrorKind::Unavailable,
        ErrorKind::DataLoss,
        ErrorKind::Unauthenticated,
        ErrorKind::DeadlineExceeded,
    ];

    /// Kinds accepted when the error carries a code with no special meaning.
    fn accepted_kinds(predicate: fn(&Error) -> bool) -> Vec<ErrorKind> {
        KINDS
            .into_iter()
            .filter(|kind| predicate(&Error::new(afs_error::IO_OTHER, *kind, "classify")))
            .collect()
    }

    #[cfg(feature = "dfs")]
    fn accepts_code(predicate: fn(&Error) -> bool, code: ErrorCode) -> bool {
        predicate(&Error::coded(code, "classify"))
    }

    #[test]
    fn verified_copy_set() {
        assert_eq!(
            accepted_kinds(verified_copy_is_invalid),
            [ErrorKind::NotFound, ErrorKind::DataLoss]
        );
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn retryable_backpressure_set() {
        assert_eq!(
            accepted_kinds(retryable_backpressure),
            [ErrorKind::ResourceExhausted]
        );
        assert!(!retryable_backpressure(&Error::new(
            afs_error::IO_CAPACITY,
            ErrorKind::ResourceExhausted,
            "disk full"
        )));
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn repair_source_set() {
        assert_eq!(
            accepted_kinds(repair_source_is_invalid),
            [ErrorKind::NotFound, ErrorKind::DataLoss]
        );
        assert!(accepts_code(
            repair_source_is_invalid,
            afs_error::NODE_STORAGE_INVALID
        ));
        assert!(!accepts_code(
            verified_copy_is_invalid,
            afs_error::NODE_STORAGE_INVALID
        ));
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn corruption_report_set() {
        assert_eq!(
            accepted_kinds(corruption_report_definitively_rejected),
            [
                ErrorKind::InvalidArgument,
                ErrorKind::NotFound,
                ErrorKind::PermissionDenied,
                ErrorKind::FailedPrecondition,
                ErrorKind::Unauthenticated,
            ]
        );
        assert!(!accepts_code(
            corruption_report_definitively_rejected,
            afs_error::META_DFS_CONFLICT
        ));
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn repair_request_set() {
        assert_eq!(
            accepted_kinds(repair_request_definitively_rejected),
            [
                ErrorKind::InvalidArgument,
                ErrorKind::NotFound,
                ErrorKind::PermissionDenied,
                ErrorKind::FailedPrecondition,
                ErrorKind::Aborted,
                ErrorKind::Unauthenticated,
            ]
        );
        assert!(accepts_code(
            repair_request_definitively_rejected,
            afs_error::META_DFS_CONFLICT
        ));
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn repair_report_set() {
        assert_eq!(
            accepted_kinds(repair_report_definitively_rejected),
            [
                ErrorKind::InvalidArgument,
                ErrorKind::PermissionDenied,
                ErrorKind::Unauthenticated,
            ]
        );
        assert!(accepts_code(
            repair_report_definitively_rejected,
            afs_error::META_DFS_REPAIR_SUPERSEDED
        ));
        assert!(!accepts_code(
            repair_report_definitively_rejected,
            afs_error::META_DFS_CONFLICT
        ));
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn read_authority_set() {
        assert_eq!(
            accepted_kinds(read_authority_rejected),
            [ErrorKind::PermissionDenied, ErrorKind::Unauthenticated]
        );
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn cached_copy_set() {
        assert_eq!(
            accepted_kinds(cached_copy_is_invalid),
            [ErrorKind::NotFound]
        );
        assert!(accepts_code(
            cached_copy_is_invalid,
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        ));
    }

    #[cfg(feature = "dfs")]
    #[test]
    fn remote_suspect_set() {
        assert!(remote_copy_content_suspect(&Error::coded(
            afs_error::NODE_TRANSFER_CORRUPT_DATA,
            "x"
        )));
        for code in [
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            afs_error::NODE_VFS_UNAVAILABLE,
            afs_error::IO_TIMEOUT,
        ] {
            assert!(!remote_copy_content_suspect(&Error::coded(code, "x")));
        }
    }
}
