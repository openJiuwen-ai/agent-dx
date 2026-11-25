//! Node data movement shared across filesystem adapters.

pub(crate) mod checksum;

#[cfg(feature = "ownerfs")]
pub(crate) mod hixl;

use crate::config::DataMode;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Backend {
    Grpc,
    Rdma,
    Hixl,
}

/// Auto considers the adapters assembled for the operation in priority order.
/// Explicit modes are passed to the adapter for a structured error if unavailable.
pub(crate) fn select(mode: DataMode, hixl: bool, rdma: bool) -> Backend {
    match mode {
        DataMode::Grpc => Backend::Grpc,
        DataMode::Rdma => Backend::Rdma,
        DataMode::Hixl => Backend::Hixl,
        DataMode::Auto if hixl => Backend::Hixl,
        DataMode::Auto if rdma => Backend::Rdma,
        DataMode::Auto => Backend::Grpc,
    }
}

/// Call only at negotiation boundaries where there is no in-flight transfer.
pub(crate) fn can_fallback(mode: DataMode, error: &afs_error::Error) -> bool {
    mode == DataMode::Auto
        && error.code() == afs_error::NODE_TRANSFER_UNSUPPORTED
        && error.kind() == afs_error::ErrorKind::Unimplemented
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_modes_and_auto_candidates_select_by_operation() {
        for (hixl, rdma, auto) in [
            (false, false, Backend::Grpc),
            (false, true, Backend::Rdma),
            (true, false, Backend::Hixl),
            (true, true, Backend::Hixl),
        ] {
            assert_eq!(select(DataMode::Grpc, hixl, rdma), Backend::Grpc);
            assert_eq!(select(DataMode::Rdma, hixl, rdma), Backend::Rdma);
            assert_eq!(select(DataMode::Hixl, hixl, rdma), Backend::Hixl);
            assert_eq!(select(DataMode::Auto, hixl, rdma), auto);
        }
    }

    #[test]
    fn only_auto_can_fall_back_on_explicit_unsupported() {
        let unsupported = afs_error::Error::coded(afs_error::NODE_TRANSFER_UNSUPPORTED, "peer");
        assert!(can_fallback(DataMode::Auto, &unsupported));
        assert!(!can_fallback(DataMode::Hixl, &unsupported));
        assert!(!can_fallback(DataMode::Rdma, &unsupported));
        for code in [
            afs_error::NODE_TRANSFER_UNAVAILABLE,
            afs_error::NODE_TRANSFER_RESOURCE_EXHAUSTED,
            afs_error::NODE_TRANSFER_CORRUPT_DATA,
            afs_error::CLIENT_PROTOCOL_VIOLATION,
            afs_error::IO_TIMEOUT,
        ] {
            assert!(!can_fallback(
                DataMode::Auto,
                &afs_error::Error::coded(code, "peer")
            ));
        }
    }
}
