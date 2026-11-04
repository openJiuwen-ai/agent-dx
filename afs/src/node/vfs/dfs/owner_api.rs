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

/// A request being served on an async worker. It owns what it needs, so the RPC
/// layer can run it detached from the caller's cancellation.
pub type OwnerFuture<T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = afs_error::Result<T>> + Send + 'static>>;

/// Runs synchronous handler work on the blocking pool.
pub(crate) async fn offload_handler<T: Send + 'static>(
    work: impl FnOnce() -> afs_error::Result<T> + Send + 'static,
) -> afs_error::Result<T> {
    crate::node::bridge::spawn_blocking(work)
        .await
        .map_err(|error| {
            afs_error::Error::coded(
                afs_error::CLIENT_WORKER_FAILED,
                format!("DFS owner blocking worker failed: {error}"),
            )
        })?
}

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

    /// The RPC entry of `open_with_options`; runs on an async worker. The default
    /// runs the sync method on the blocking pool.
    fn open_async(
        self: std::sync::Arc<Self>,
        peer: String,
        request: DfsOwnerOpenRequest,
        options: OpenOptions,
    ) -> OwnerFuture<DfsOwnerOpenReply> {
        Box::pin(offload_handler(move || {
            self.open_with_options(&peer, request, options)
        }))
    }
    fn getattr(
        &self,
        _peer: &str,
        _request: DfsOwnerGetAttrRequest,
    ) -> afs_error::Result<DfsOwnerGetAttrReply> {
        Err(dfs_owner_lifecycle_unimplemented())
    }
    /// The RPC entry of `getattr`; see [`Self::open_async`].
    fn getattr_async(
        self: std::sync::Arc<Self>,
        peer: String,
        request: DfsOwnerGetAttrRequest,
    ) -> OwnerFuture<DfsOwnerGetAttrReply> {
        Box::pin(offload_handler(move || self.getattr(&peer, request)))
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
    /// The RPC entry of `release`; see [`Self::open_async`].
    fn release_async(
        self: std::sync::Arc<Self>,
        peer: String,
        request: DfsOwnerReleaseRequest,
    ) -> OwnerFuture<DfsOwnerReleaseReply> {
        Box::pin(offload_handler(move || self.release(&peer, request)))
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

    // The RPC entries; they run on an async worker. A default runs the sync
    // method on the blocking pool.
    fn read_async(
        self: std::sync::Arc<Self>,
        peer: String,
        request: DfsOwnerReadRequest,
    ) -> OwnerFuture<DfsOwnerReadReply> {
        Box::pin(offload_handler(move || self.read(&peer, request)))
    }
    fn write_async(
        self: std::sync::Arc<Self>,
        peer: String,
        request: DfsOwnerWriteRequest,
    ) -> OwnerFuture<DfsOwnerWriteReply> {
        Box::pin(offload_handler(move || self.write(&peer, request)))
    }
    fn resize_async(
        self: std::sync::Arc<Self>,
        peer: String,
        request: DfsOwnerResizeRequest,
    ) -> OwnerFuture<DfsOwnerResizeReply> {
        Box::pin(offload_handler(move || self.resize(&peer, request)))
    }
    fn sync_async(
        self: std::sync::Arc<Self>,
        peer: String,
        request: DfsOwnerSyncRequest,
    ) -> OwnerFuture<DfsOwnerSyncReply> {
        Box::pin(offload_handler(move || self.sync(&peer, request)))
    }
}

pub(crate) fn dfs_owner_unimplemented() -> afs_error::Error {
    afs_error::Error::coded(
        afs_error::NODE_VFS_UNIMPLEMENTED,
        "DFS remote inode owner execution is not wired",
    )
}
