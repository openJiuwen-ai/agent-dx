//! Authority storage boundary for afs-meta.
//!
//! `MetaStore` is the domain contract; `Store` is its single production
//! implementation. It serializes conditional transitions, persists each batch
//! through the canonical operation log, then publishes the acknowledged state.
//! Local-file is durable; the explicitly selected memory log is volatile.

use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use afs_error::{Error, Result};

use crate::clock::now_unix_ms;
use afs_tracing::{Instrument, tracing};
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use crate::dfs::{
    ChunkObject, CopyRecord, Dentry, DentryKey, DfsDurabilityPolicy, FileVersion, FileVersionId,
    InodeId, InodeRecord, LayoutRoot, LayoutRootId, NamespaceId, PlacementRecord, RenameOutcome,
    ReplicationClaim, ReplicationConfig, ReplicationTask, ReplicationTaskId,
    StorageDeviceDescriptor, WriteLease,
};

pub mod codec;
#[cfg(test)]
mod crash_tests;
mod history;
pub(crate) use history::Limits as HistoryLimits;
pub mod journal;
pub use journal::MetaLog;
pub mod local_log;
pub mod log;
#[cfg(test)]
mod log_contract_tests;
mod profile;
mod state;
use state::State;
#[cfg(test)]
use state::{State as MemoryState, StoreState, VersionedEntity};

mod admission;
mod conditions;
mod engine;
mod metrics;
mod model;
mod read_view;
mod txn;
use admission::*;
pub(super) use conditions::entity_key;
use conditions::*;
pub(crate) use engine::StoreOptions;
use engine::*;
pub use engine::{Store, unavailable_meta_store};
use metrics::*;
pub use model::{
    BackendPersistence, BackendReadiness, MetaEntity, MetaKey, MetaRead, MetaSnapshot, NodeSession,
    NodeSessionLease, OwnerRootScan, RecoveryCursor, RecoveryReason, RecoveryScan, RequestKey,
    RootAccessGrant, RootCommandAck, RootCommandRecord, RootCommandType, RootRecord,
    RootReservationRecord, RootRight, StoreRevision, WatchBatch, WatchChange, WatchEvent,
};
pub(crate) use model::{
    WATCH_RESPONSE_BASE_BYTES, WATCH_RESPONSE_MAX_BYTES, WATCH_RESPONSE_MAX_EVENTS,
};
pub use read_view::MetaReadView;
use read_view::*;
pub use txn::{
    GcPlanGuard, MetaTxn, OperationResult, RequestOutcome, StoreOperation, TxnCondition,
    TxnMutation, TxnOutcome,
};

/// Boxed only on the coarse Meta control path. Durable logs preserve results
/// across restart; memory is explicitly volatile.
pub type MetaFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub trait MetaStore: Send + Sync {
    /// Verifies that the durable backend can currently serve a real read.
    ///
    /// This must not be satisfied only from Store's in-memory acknowledged snapshot:
    /// REST readiness uses it to fail closed when the backend is disconnected.
    fn health(&self) -> MetaFuture<'_, ()>;

    /// Reports persistence capability only after probing the backend.
    fn backend_readiness(&self) -> MetaFuture<'_, BackendReadiness>;

    /// Pins one acknowledged revision for all reads belonging to a source resolution.
    fn read_view(&self) -> MetaFuture<'_, MetaReadView>;

    /// Returns a linearizable snapshot for a single entity or request outcome.
    fn read(&self, read: MetaRead) -> MetaFuture<'_, MetaSnapshot>;

    /// Applies a conditional transaction and records its idempotent outcome in
    /// the same store commit. Implementations must call `MetaTxn::validate`.
    /// `RootCommandAcked` is true only for a successful ACK from the recorded
    /// current Home session for that exact command, root epoch and generation.
    fn compare_and_commit(&self, txn: MetaTxn) -> MetaFuture<'_, TxnOutcome>;

    /// `compare_and_commit` for background work (GC batches): it queues
    /// within a sub-quota so it never takes the room foreground writes need.
    fn compare_and_commit_background(&self, txn: MetaTxn) -> MetaFuture<'_, TxnOutcome> {
        self.compare_and_commit(txn)
    }

    /// Returns ordered changes after `after_revision`, or a compaction cursor
    /// that forces callers to rescan before resuming.
    fn watch(&self, after_revision: StoreRevision, limit: usize) -> MetaFuture<'_, WatchBatch>;

    /// Registers or renews the current node process session.
    ///
    /// A successful new session for the same node fences the previous session:
    /// future grants must use the new session id, and P2P requests carrying the
    /// old Home session must be rejected by the Home node.
    fn register_node_session(
        &self,
        request: RequestKey,
        lease: NodeSessionLease,
    ) -> MetaFuture<'_, TxnOutcome>;

    /// Scans durable roots for a restarted Home node. The result is only an
    /// input to OwnerFs local-catalog reconciliation; it does not by itself
    /// grant file access.
    fn scan_roots_for_recovery<'a>(&'a self, home_node_id: &'a str)
    -> MetaFuture<'a, RecoveryScan>;

    /// Scans active roots and pending reserve-stage records for a restarted
    /// Home node in one linearizable snapshot.
    ///
    /// This is the OwnerFs reconciliation entry point. It includes active roots
    /// so startup can fail closed when Meta believes a root is active but the
    /// local catalog no longer has durable identity for it.
    fn scan_owner_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> MetaFuture<'a, OwnerRootScan>;
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod grant_lifecycle_tests;
