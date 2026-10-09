//! Small, stable handling categories for AFS errors.
//!
//! The set follows gRPC canonical status semantics, but this crate remains
//! native and does not depend on `tonic` or protobuf generated code.

/// Broad category used for generic client behavior such as reconnect or retry.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ErrorKind {
    #[default]
    Unknown,
    Cancelled,
    OutOfRange,
    InvalidArgument,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    Unimplemented,
    Internal,
    Unavailable,
    DataLoss,
    Unauthenticated,
    DeadlineExceeded,
}
