//! Owner-side service contracts of the DFS write proxy.
//!
//! The RPC layer (`node::rpc::control` / `node::rpc::data`) decodes requests
//! and dispatches to these traits; the file layer implements them. Defining
//! them here keeps the dependency pointing from transport to file layer
//! (ledger R47 / S1b H4).
use afs_protocol::{
    node_control::{
        DfsOwnerAcknowledgeLockWaitReply, DfsOwnerAcknowledgeLockWaitRequest,
        DfsOwnerCancelLockWaitReply, DfsOwnerCancelLockWaitRequest, DfsOwnerGetAttrReply,
        DfsOwnerGetAttrRequest, DfsOwnerGetLockReply, DfsOwnerGetLockRequest, DfsOwnerMknodReply,
        DfsOwnerMknodRequest, DfsOwnerOpenReply, DfsOwnerOpenRequest,
        DfsOwnerReleaseLockSessionReply, DfsOwnerReleaseLockSessionRequest,
        DfsOwnerReleaseLocksReply, DfsOwnerReleaseLocksRequest, DfsOwnerReleaseReply,
        DfsOwnerReleaseRequest, DfsOwnerSetLockReply, DfsOwnerSetLockRequest,
    },
    node_data::{
        DfsOwnerReadReply, DfsOwnerReadRequest, DfsOwnerResizeReply, DfsOwnerResizeRequest,
        DfsOwnerSyncReply, DfsOwnerSyncRequest, DfsOwnerWriteReply, DfsOwnerWriteRequest,
    },
};

use crate::node::vfs::types::OpenOptions;

/// Lifecycle (open/getattr/release) and lock RPCs served by an inode owner.
pub trait DfsOwnerLifecycleHandler: Send + Sync + 'static {
    fn open(
        &self,
        _peer: &str,
        _request: DfsOwnerOpenRequest,
    ) -> afs_error::Result<DfsOwnerOpenReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }

    fn open_with_options(
        &self,
        peer: &str,
        request: DfsOwnerOpenRequest,
        options: OpenOptions,
    ) -> afs_error::Result<DfsOwnerOpenReply> {
        if options.kill_suidgid {
            return Err(dfs_owner_lifecycle_unimplemented());
        }
        self.open(peer, request)
    }
    fn getattr(
        &self,
        _peer: &str,
        _request: DfsOwnerGetAttrRequest,
    ) -> afs_error::Result<DfsOwnerGetAttrReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn mknod(
        &self,
        _peer: &str,
        _request: DfsOwnerMknodRequest,
    ) -> afs_error::Result<DfsOwnerMknodReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn get_lock(
        &self,
        _peer: &str,
        _request: DfsOwnerGetLockRequest,
    ) -> afs_error::Result<DfsOwnerGetLockReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn set_lock(
        &self,
        _peer: &str,
        _request: DfsOwnerSetLockRequest,
    ) -> afs_error::Result<DfsOwnerSetLockReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn cancel_lock_wait(
        &self,
        _peer: &str,
        _request: DfsOwnerCancelLockWaitRequest,
    ) -> afs_error::Result<DfsOwnerCancelLockWaitReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn acknowledge_lock_wait(
        &self,
        _peer: &str,
        _request: DfsOwnerAcknowledgeLockWaitRequest,
    ) -> afs_error::Result<DfsOwnerAcknowledgeLockWaitReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn release_locks(
        &self,
        _peer: &str,
        _request: DfsOwnerReleaseLocksRequest,
    ) -> afs_error::Result<DfsOwnerReleaseLocksReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn release_lock_session(
        &self,
        _peer: &str,
        _request: DfsOwnerReleaseLockSessionRequest,
    ) -> afs_error::Result<DfsOwnerReleaseLockSessionReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    fn release(
        &self,
        _peer: &str,
        _request: DfsOwnerReleaseRequest,
    ) -> afs_error::Result<DfsOwnerReleaseReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
}

pub(crate) fn dfs_owner_lifecycle_unimplemented() -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::NODE_VFS_UNIMPLEMENTED,
        "DFS remote inode owner lifecycle is not wired",
    )
}

/// Framework boundary only: handlers own lease/handle validation, sequencing,
/// deduplication and result-unknown recovery. A transport ACK is never fsync.
pub trait DfsOwnerFilesHandler: Send + Sync + 'static {
    fn read(
        &self,
        _peer: &str,
        _request: DfsOwnerReadRequest,
    ) -> afs_error::Result<DfsOwnerReadReply> {
        Err(dfs_owner_unimplemented())
    }
    fn write(
        &self,
        _peer: &str,
        _request: DfsOwnerWriteRequest,
    ) -> afs_error::Result<DfsOwnerWriteReply> {
        Err(dfs_owner_unimplemented())
    }
    fn resize(
        &self,
        _peer: &str,
        _request: DfsOwnerResizeRequest,
    ) -> afs_error::Result<DfsOwnerResizeReply> {
        Err(dfs_owner_unimplemented())
    }
    fn sync(
        &self,
        _peer: &str,
        _request: DfsOwnerSyncRequest,
    ) -> afs_error::Result<DfsOwnerSyncReply> {
        Err(dfs_owner_unimplemented())
    }
}

pub(crate) fn dfs_owner_unimplemented() -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::NODE_VFS_UNIMPLEMENTED,
        "DFS remote inode owner execution is not wired",
    )
}
