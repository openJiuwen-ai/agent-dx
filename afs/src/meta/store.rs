//! Authority storage boundary for afs-meta.
//!
//! `MetaStore` is the domain contract; `Store` is its single production
//! implementation. It serializes conditional transitions, persists each batch
//! through a selected backend, then publishes the acknowledged state. Memory,
//! local-file, etcd and Redis backends differ only in their persistence guarantees.

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use afs_error::{Error, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::{RwLock, mpsc, oneshot};

use crate::dfs::{
    ChunkObject, CopyRecord, Dentry, DentryKey, FileVersion, FileVersionId, InodeId, InodeRecord,
    LayoutRoot, LayoutRootId, NamespaceId, PlacementRecord, RenameOutcome, ReplicationClaim,
    ReplicationConfig, ReplicationTask, ReplicationTaskId, StorageDeviceDescriptor, WriteLease,
};

pub mod etcd;
pub mod local_file;
pub mod memory;
pub mod redis;

/// Observed persistence class for the configured Meta backend.
///
/// This is a readiness claim only. `Persistent` means the backend is designed
/// and configured for restart persistence, not that physical power-loss
/// behavior has been proven by the current health probe.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum BackendPersistence {
    Unknown,
    Volatile,
    Persistent,
}

impl BackendPersistence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Volatile => "volatile",
            Self::Persistent => "persistent",
        }
    }

    pub fn is_persistent(self) -> bool {
        matches!(self, Self::Persistent)
    }
}

/// Backend readiness after an actual backend health probe.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackendReadiness {
    pub persistence: BackendPersistence,
    pub healthy: bool,
    pub persistent_ready: bool,
    pub detail: String,
}

impl BackendReadiness {
    pub fn observed(
        persistence: BackendPersistence,
        healthy: bool,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            persistence,
            healthy,
            persistent_ready: healthy && persistence.is_persistent(),
            detail: detail.into(),
        }
    }
}

/// Monotonic revision assigned by the selected MetaStore.
///
/// Watch delivery, recovery scans, and read snapshots all use the same revision
/// space. A revision of `0` means "start from the current backend snapshot".
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct StoreRevision(pub u64);

impl StoreRevision {
    pub const ZERO: Self = Self(0);

    pub fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// Stable idempotency key provided by RPC callers.
///
/// A retry with the same request id must observe the same committed outcome
/// when the original transaction reached the store. A retry with the same id
/// but different semantic operation must be rejected by the store backend.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RequestKey {
    pub caller_id: String,
    pub request_id: String,
}

impl RequestKey {
    pub fn new(caller_id: impl Into<String>, request_id: impl Into<String>) -> Self {
        Self {
            caller_id: caller_id.into(),
            request_id: request_id.into(),
        }
    }

    fn is_valid(&self) -> bool {
        !self.caller_id.is_empty() && !self.request_id.is_empty()
    }
}

/// One afs-node process lifetime as registered in Meta.
///
/// `node_id` is stable across restarts; `session_id` changes on every process
/// start. Grants and P2P RootAccess must carry the session that was current
/// when the grant was issued, so Home restart fences stale remote access.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NodeSession {
    pub node_id: String,
    pub session_id: String,
    pub grpc_addr: String,
    pub data_addr: String,
    pub rest_addr: String,
    #[serde(default)]
    pub storage_devices: Vec<StorageDeviceDescriptor>,
    pub lease_epoch: u64,
    pub expires_at_unix_ms: u64,
}

impl NodeSession {
    /// Returns true only while the lease is strictly in the future.
    ///
    /// A session whose deadline is equal to the Meta clock is already expired:
    /// callers must renew before the boundary if they want to keep grants live.
    pub fn is_live_at_unix_ms(&self, now_unix_ms: u64) -> bool {
        self.expires_at_unix_ms > now_unix_ms
    }
}

/// Input used to create or renew a node session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeSessionLease {
    pub node_id: String,
    pub session_id: String,
    pub grpc_addr: String,
    pub data_addr: String,
    pub rest_addr: String,
    pub storage_devices: Vec<StorageDeviceDescriptor>,
    pub lease_ttl: Duration,
}

/// Workspace root durable location. It is not a grant.
///
/// Location tells callers where the Home is. Access still requires a
/// `RootAccessGrant`, because location queries are also used by schedulers and
/// management tools that should not receive fencing tokens.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RootRecord {
    pub root_id: String,
    pub root_epoch: u64,
    pub home_node_id: String,
    pub home_session_id: String,
    pub local_prepare_id: String,
    pub created_at_revision: StoreRevision,
    pub updated_at_revision: StoreRevision,
}

/// Durable reserve-stage record for a not-yet-active OwnerFs root.
///
/// Reserve exists so `mkdir /ownerfs/<root>` can first persist the chosen Home
/// and epoch, then prepare/fsync the local directory, then activate. A
/// reservation is not a grant and must never authorize data access.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RootReservationRecord {
    pub root_id: String,
    pub root_epoch: u64,
    pub home_node_id: String,
    pub home_session_id: String,
    pub create_intent_id: String,
    pub prepare_token: String,
    pub created_at_revision: StoreRevision,
}

/// Access grant persisted by Meta and echoed to nodes.
///
/// Multiple nodes may hold grants for the same root. B joining through P2P does
/// not revoke A's Home access; only explicit revoke/delete/recovery transitions
/// advance or invalidate grants.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RootAccessGrant {
    pub root_id: String,
    pub root_epoch: u64,
    pub home_node_id: String,
    pub home_session_id: String,
    pub holder_node_id: String,
    pub holder_session_id: String,
    pub access_generation: u64,
    pub rights: Vec<RootRight>,
    pub fencing_token: String,
    pub issued_at_revision: StoreRevision,
}

/// Coarse root rights. File permissions are still checked by the Home node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum RootRight {
    Lookup,
    Read,
    Write,
    Admin,
}

/// Durable root command type.
///
/// Existing serialized command records did not carry a type; they are
/// interpreted as revoke-access commands for compatibility with the original
/// watch stream and ACK path.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum RootCommandType {
    #[default]
    RevokeAccess,
    InvalidateCache,
}

/// Durable acknowledgement of a command that changes the validity of old grants.
///
/// The initial OwnerFs implementation should avoid a B-join revoke path, but
/// delete, explicit revocation, and Home recovery still need a durable command
/// and acknowledgement boundary. A later revoke/delete commit must condition on
/// this record; seeing a watch event is not proof that Home refused new work.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RootCommandAck {
    pub command_id: String,
    pub node_id: String,
    pub session_id: String,
    pub root_id: String,
    pub root_epoch: u64,
    pub access_generation: u64,
    pub success: bool,
    pub message: String,
}

/// Durable command that must precede a revoke/delete/recovery barrier.
///
/// The store records this pending command before watch delivery. A Home ACK
/// proves it installed refusal for the old generation; only then may a
/// conditional transaction commit the new generation or delete the root.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RootCommandRecord {
    pub command_id: String,
    pub home_node_id: String,
    pub home_session_id: String,
    pub root_id: String,
    pub root_epoch: u64,
    pub old_access_generation: u64,
    #[serde(default)]
    pub command_type: RootCommandType,
}

/// OwnerFs and DFS records use the same MetaStore mechanics but keep
/// their own business state. The DFS state machine extends this
/// enum instead of adding another authority path.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum MetaEntity {
    NodeSession(NodeSession),
    RootReservation(RootReservationRecord),
    Root(RootRecord),
    RootGrant(RootAccessGrant),
    RootCommand(RootCommandRecord),
    RootCommandAck(RootCommandAck),
    DfsDentry(Dentry),
    DfsInode(InodeRecord),
    DfsFileVersion(FileVersion),
    DfsLayoutRoot(LayoutRoot),
    DfsChunk(ChunkObject),
    DfsReplicationConfig(ReplicationConfig),
    DfsPlacement(PlacementRecord),
    DfsCopy(CopyRecord),
    DfsReplicationTask(ReplicationTask),
    DfsWriteLease(WriteLease),
}

impl MetaEntity {
    pub fn name(&self) -> &'static str {
        match self {
            Self::NodeSession(_) => "node_session",
            Self::RootReservation(_) => "root_reservation",
            Self::Root(_) => "root",
            Self::RootGrant(_) => "root_grant",
            Self::RootCommand(_) => "root_command",
            Self::RootCommandAck(_) => "root_command_ack",
            Self::DfsDentry(_) => "dfs_dentry",
            Self::DfsInode(_) => "dfs_inode",
            Self::DfsFileVersion(_) => "dfs_file_version",
            Self::DfsLayoutRoot(_) => "dfs_layout_root",
            Self::DfsChunk(_) => "dfs_chunk",
            Self::DfsReplicationConfig(_) => "dfs_replication_config",
            Self::DfsPlacement(_) => "dfs_placement",
            Self::DfsCopy(_) => "dfs_copy",
            Self::DfsReplicationTask(_) => "dfs_replication_task",
            Self::DfsWriteLease(_) => "dfs_write_lease",
        }
    }
}

/// Linearizable read selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaRead {
    NodeSession {
        node_id: String,
        session_id: String,
    },
    CurrentNodeSession {
        node_id: String,
    },
    CurrentNodeSessions,
    Root {
        root_id: String,
    },
    RootReservation {
        root_id: String,
    },
    RootGrantByHolder {
        root_id: String,
        holder_node_id: String,
        holder_session_id: String,
    },
    /// Fetches the current grant that matches presented P2P access facts.
    ///
    /// The backend must atomically compare the grant, current root generation,
    /// current Home session, current holder session, and absence of a pending
    /// revocation barrier in one linearizable read. Separate reads could race
    /// a session change or revoke and must not authorize P2P access.
    RootGrant {
        root_id: String,
        root_epoch: u64,
        home_session_id: String,
        holder_node_id: String,
        holder_session_id: String,
        access_generation: u64,
        fencing_token: String,
    },
    RequestOutcome(RequestKey),
    DfsDentry(DentryKey),
    DfsDirectory {
        namespace_id: NamespaceId,
        parent_inode_id: InodeId,
    },
    DfsInode(InodeId),
    DfsFileVersion(FileVersionId),
    DfsLayoutRoot(LayoutRootId),
    DfsChunk(crate::dfs::ChunkId),
    DfsPlacement(crate::dfs::ChunkId),
    DfsPlacements,
    DfsCopy(crate::dfs::CopyId),
    DfsReplicationConfig,
    DfsReplicationTask(ReplicationTaskId),
    DfsReplicationTasks,
    DfsWriteLease(InodeId),
}

/// A linearizable read response. Empty results are represented explicitly so
/// callers cannot confuse "not found" with "store unavailable".
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaSnapshot {
    pub revision: StoreRevision,
    pub entity: Option<MetaEntity>,
    pub request_outcome: Option<RequestOutcome>,
    pub entities: Vec<MetaEntity>,
}

/// Condition checked inside a single durable transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxnCondition {
    Missing(MetaKey),
    RevisionEquals {
        key: MetaKey,
        revision: StoreRevision,
    },
    /// Requires the current durable value at `entity_key(entity)` to exactly
    /// match `entity`.
    ///
    /// This is used for reservation transitions where revision numbers alone
    /// are not enough: a root can be reserved, aborted, and reserved again with
    /// the same logical epoch, so stale activate/abort calls must compare the
    /// reservation identity rather than just "some reservation exists".
    EntityEquals(MetaEntity),
    NodeSessionCurrent {
        node_id: String,
        session_id: String,
    },
    RootEpochEquals {
        root_id: String,
        root_epoch: u64,
    },
    RootCommandAcked {
        command_id: String,
        node_id: String,
        session_id: String,
    },
    RootCommandMatches {
        command_id: String,
        home_node_id: String,
        home_session_id: String,
        root_id: String,
        root_epoch: u64,
        old_access_generation: u64,
        command_type: RootCommandType,
    },
    RootCommandAckedExact {
        command_id: String,
        node_id: String,
        session_id: String,
        root_id: String,
        root_epoch: u64,
        access_generation: u64,
        command_type: RootCommandType,
    },
    RequestAbsent(RequestKey),
    DfsDirectoryEmpty {
        namespace_id: NamespaceId,
        parent_inode_id: InodeId,
    },
    DfsNotDescendant {
        namespace_id: NamespaceId,
        ancestor_inode_id: InodeId,
        child_inode_id: InodeId,
    },
}

/// Mutation written inside a single durable transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum TxnMutation {
    Put(MetaEntity),
    Delete(MetaKey),
    RecordRequestOutcome(RequestOutcome),
}

/// Key shapes used by transaction conditions and deletes.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum MetaKey {
    NodeSession {
        node_id: String,
        session_id: String,
    },
    CurrentNodeSession {
        node_id: String,
    },
    CurrentNodeSessions,
    Root {
        root_id: String,
    },
    RootReservation {
        root_id: String,
    },
    RootGrant {
        root_id: String,
        holder_node_id: String,
        holder_session_id: String,
    },
    RootCommand {
        command_id: String,
    },
    RootCommandAck {
        command_id: String,
        home_node_id: String,
        home_session_id: String,
    },
    RequestOutcome(RequestKey),
    DfsDentry(DentryKey),
    DfsInode(InodeId),
    DfsFileVersion(FileVersionId),
    DfsLayoutRoot(LayoutRootId),
    DfsChunk(crate::dfs::ChunkId),
    DfsReplicationConfig,
    DfsPlacement(crate::dfs::ChunkId),
    DfsCopy(crate::dfs::CopyId),
    DfsReplicationTask(ReplicationTaskId),
    DfsWriteLease(InodeId),
}

/// Idempotent result stored in the same transaction as the authority mutation.
///
/// The payload is intentionally domain-shaped rather than raw protobuf bytes so
/// future RPC adapters can evolve wire format without changing store semantics.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RequestOutcome {
    pub request: RequestKey,
    pub operation: StoreOperation,
    pub result: OperationResult,
}

/// Store operation identity used to reject accidental request-id reuse.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum StoreOperation {
    RegisterNode,
    ReserveRoot,
    AbortRoot,
    ActivateRoot,
    AcquireRoot,
    RecoverRoot,
    BeginRootRevocation,
    CommitRootRevocation,
    AckRootCommand,
    DfsCreate,
    DfsMkdir,
    DfsMknod,
    DfsInitializeNamespace,
    DfsUnlink,
    DfsRmdir,
    DfsRename,
    DfsLink,
    DfsSymlink,
    DfsSetInodeAttributes,
    DfsSetXattr,
    DfsRemoveXattr,
    DfsAcquireWriteLease,
    DfsRenewWriteLease,
    DfsSyncInodeMetadata,
    DfsCommitFileVersion,
    DfsInitializeReplicationConfig,
    DfsClaimReplicationTask,
    DfsReportReplicationTask,
    DfsReportChunkCorruption,
}

/// Operation result that can be replayed to idempotent callers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum OperationResult {
    /// Namespace retries must bind the complete original request, including
    /// caller credentials; a reused ID cannot authorize a different mutation.
    DfsNamespace {
        request_digest: [u8; 32],
        result: Box<OperationResult>,
    },
    NodeSession(NodeSession),
    RootReservation(RootReservationRecord),
    RootRecord(RootRecord),
    RootGrant(RootAccessGrant),
    RootCommand(RootCommandRecord),
    RootCommandAck(RootCommandAck),
    DfsInode(InodeRecord),
    DfsInodeWithLease {
        inode: InodeRecord,
        lease: WriteLease,
    },
    DfsWriteLease(WriteLease),
    DfsReplicationClaim {
        request_digest: [u8; 32],
        claim: Option<ReplicationClaim>,
    },
    DfsReplicationTask(ReplicationTask),
    DfsRename(RenameOutcome),
    Empty,
}

/// Conditional transaction plus the idempotent outcome that must be recorded
/// atomically with it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaTxn {
    pub request: RequestKey,
    pub operation: StoreOperation,
    pub conditions: Vec<TxnCondition>,
    pub mutations: Vec<TxnMutation>,
}

impl MetaTxn {
    pub fn new(request: RequestKey, operation: StoreOperation) -> Self {
        Self {
            request,
            operation,
            conditions: Vec::new(),
            mutations: Vec::new(),
        }
    }

    /// Rejects contract-breaking transactions before they reach a backend.
    ///
    /// A valid transaction must check idempotency and record the outcome in the
    /// same commit. Without that rule, a crash between authority mutation and
    /// reply recording could make retries advance epochs/generations twice.
    pub fn validate(&self) -> Result<()> {
        if !self.request.is_valid() {
            return Err(invalid_contract(
                "request key must include caller_id and request_id",
            ));
        }
        if self.conditions.is_empty() {
            return Err(invalid_contract("transaction must include conditions"));
        }
        if self.mutations.is_empty() {
            return Err(invalid_contract("transaction must include mutations"));
        }
        let checks_request = self.conditions.iter().any(|condition| {
            matches!(
                condition,
                TxnCondition::RequestAbsent(key) if key == &self.request
            )
        });
        if !checks_request {
            return Err(invalid_contract(
                "transaction must condition on absent request outcome",
            ));
        }
        let records_matching_outcome = self.mutations.iter().any(|mutation| {
            matches!(
                mutation,
                TxnMutation::RecordRequestOutcome(outcome)
                    if outcome.request == self.request && outcome.operation == self.operation
            )
        });
        if !records_matching_outcome {
            return Err(invalid_contract(
                "transaction must record matching request outcome",
            ));
        }
        Ok(())
    }
}

/// Result of a conditional transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxnOutcome {
    Committed {
        revision: StoreRevision,
        outcome: RequestOutcome,
    },
    ConditionFailed {
        revision: StoreRevision,
        existing_outcome: Option<RequestOutcome>,
    },
}

/// Ordered change stream item.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WatchEvent {
    pub revision: StoreRevision,
    /// Stable position among all keys changed by one atomic store revision.
    pub event_index: u32,
    pub key: MetaKey,
    pub change: WatchChange,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[allow(clippy::large_enum_variant)]
pub enum WatchChange {
    Put(MetaEntity),
    Delete,
}

/// Watch response. Backends may return a finite batch or report compaction.
/// A batch must never split the events of one transaction/revision; `limit` is
/// therefore a soft bound. Resume cursors advance only past complete revisions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WatchBatch {
    Events {
        start_revision: StoreRevision,
        next_revision: StoreRevision,
        events: Vec<WatchEvent>,
    },
    Compacted {
        requested_after: StoreRevision,
        compacted_to: StoreRevision,
        recovery: RecoveryCursor,
    },
}

impl WatchBatch {
    pub fn validate_order(&self) -> Result<()> {
        if let Self::Events {
            start_revision,
            next_revision,
            events,
        } = self
        {
            let mut previous: Option<(StoreRevision, u32)> = None;
            for event in events {
                if event.revision <= *start_revision
                    || previous.is_some_and(|(revision, index)| {
                        event.revision < revision
                            || (event.revision == revision && event.event_index <= index)
                    })
                {
                    return Err(invalid_contract("watch events must be strictly ordered"));
                }
                previous = Some((event.revision, event.event_index));
            }
            if previous.is_some_and(|(revision, _)| revision >= *next_revision) {
                return Err(invalid_contract(
                    "next_revision must be greater than the last event",
                ));
            }
        }
        Ok(())
    }
}

/// Cursor returned when a watcher missed compacted history.
///
/// The caller must rescan durable state from this cursor, rebuild local caches,
/// then resume watching from `resume_after`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryCursor {
    pub resume_after: StoreRevision,
    pub reason: RecoveryReason,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryReason {
    WatchCompacted,
    NodeSessionRestarted,
    BackendLeaderChanged,
}

/// Roots that a restarted Home node may reconcile with local catalog records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryScan {
    pub revision: StoreRevision,
    pub home_node_id: String,
    pub roots: Vec<RootRecord>,
}

/// OwnerFs roots and pending reservations that a restarted Home node may reconcile.
///
/// Pending reservations are not access grants. The combined snapshot lets
/// OwnerFs detect both sides of startup mismatch: active Meta roots without a
/// local catalog record, and local prepare records whose Meta reservation is
/// still pending.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerRootScan {
    pub revision: StoreRevision,
    pub home_node_id: String,
    pub roots: Vec<RootRecord>,
    pub reservations: Vec<RootReservationRecord>,
}

/// Authority operations required by Meta business services.
///
/// Operations are serialized by Store in the active Meta. Only durable
/// backends preserve facts across restart; memory is explicitly volatile.
/// Boxed only on the coarse Meta control path.
pub type MetaFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// A pinned, immutable authority view for compound reads. It exposes no mutations.
/// Production views share the acknowledged state; backends need no native read transaction.
pub struct MetaReadView {
    state: Arc<StoreState>,
}

impl MetaReadView {
    pub async fn read(&self, read: MetaRead) -> Result<MetaSnapshot> {
        self.state.read(read).await
    }
}

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

/// The backend persists opaque, complete authority snapshots. `version` is a
/// physical CAS token; it is deliberately separate from `StoreRevision`,
/// because one physical write may acknowledge several logical operations.
pub trait StoreBackend: Send + Sync {
    /// Probes backend availability with real backend IO.
    ///
    /// Backends may override this with a cheaper read-only command, but the
    /// default intentionally calls `load` so health cannot be satisfied from
    /// Store's in-memory snapshot.
    fn health(&self) -> MetaFuture<'_, ()> {
        Box::pin(async move { self.load().await.map(|_| ()) })
    }

    /// Static backend persistence classification. Unknown is fail-closed.
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Unknown
    }

    fn load(&self) -> MetaFuture<'_, Option<(u64, Vec<u8>)>>;
    fn commit(&self, expected_version: u64, bytes: Vec<u8>) -> MetaFuture<'_, u64>;
}

struct Visible {
    committed: RwLock<Arc<StoreState>>,
    /// Any failed durable write may mean another authority has taken over, or
    /// that the local disk is damaged. Refuse even reads until a fresh process
    /// reloads the backend; stale grants must never be served as current.
    poisoned: AtomicBool,
}

enum Mutation {
    Transaction(MetaTxn),
    RegisterNode(RequestKey, NodeSessionLease),
}

struct Pending {
    operation: Mutation,
    reply: oneshot::Sender<Result<TxnOutcome>>,
}

pub struct Store {
    visible: Arc<Visible>,
    writes: mpsc::Sender<Pending>,
    backend: Arc<dyn StoreBackend>,
}

impl Store {
    pub async fn open(backend: Arc<dyn StoreBackend>) -> Result<Self> {
        let (version, committed) = match backend.load().await? {
            Some((version, bytes)) => (version, StoreState::from_snapshot(&bytes)?),
            None => (0, StoreState::new()),
        };
        let visible = Arc::new(Visible {
            committed: RwLock::new(Arc::new(committed)),
            poisoned: AtomicBool::new(false),
        });
        let (writes, receiver) = mpsc::channel(1024);
        tokio::spawn(run_commit_loop(
            receiver,
            Arc::clone(&visible),
            backend.clone(),
            version,
        ));
        Ok(Self {
            visible,
            writes,
            backend,
        })
    }

    fn ensure_available(&self) -> Result<()> {
        if self.visible.poisoned.load(Ordering::Acquire) {
            Err(unavailable(
                "store backend commit failed; restart Meta to reload authority",
            ))
        } else {
            Ok(())
        }
    }

    async fn committed(&self) -> Result<Arc<StoreState>> {
        self.ensure_available()?;
        let state = Arc::clone(&*self.visible.committed.read().await);
        self.ensure_available()?;
        Ok(state)
    }

    async fn mutate(&self, operation: Mutation) -> Result<TxnOutcome> {
        self.ensure_available()?;
        let (reply, result) = oneshot::channel();
        self.writes
            .send(Pending { operation, reply })
            .await
            .map_err(|_| unavailable("store commit loop stopped"))?;
        result
            .await
            .map_err(|_| unavailable("store commit loop dropped reply"))?
    }
}

async fn run_commit_loop(
    mut receiver: mpsc::Receiver<Pending>,
    visible: Arc<Visible>,
    backend: Arc<dyn StoreBackend>,
    mut backend_version: u64,
) {
    while let Some(first) = receiver.recv().await {
        let mut batch = vec![first];
        // Meta is a coarse control path. A short gather window amortizes
        // backend fsync/CAS across concurrent callers without affecting data IO.
        tokio::time::sleep(Duration::from_millis(1)).await;
        while batch.len() < 64 {
            match receiver.try_recv() {
                Ok(item) => batch.push(item),
                Err(_) => break,
            }
        }
        let base = Arc::clone(&*visible.committed.read().await);
        let staged = match base.fork() {
            Ok(staged) => staged,
            Err(error) => {
                fail_batch(batch, error.to_string(), &visible);
                break;
            }
        };
        let mut replies = Vec::with_capacity(batch.len());
        let mut dirty = false;
        for item in batch {
            let result = match item.operation {
                Mutation::Transaction(txn) => staged.compare_and_commit(txn).await,
                Mutation::RegisterNode(request, lease) => {
                    staged.register_node_session(request, lease).await
                }
            };
            dirty |= matches!(result, Ok(TxnOutcome::Committed { .. }));
            replies.push((item.reply, result));
        }
        if dirty {
            let persist = match staged.snapshot_bytes() {
                Ok(bytes) => backend.commit(backend_version, bytes).await,
                Err(error) => Err(error),
            };
            match persist {
                Ok(version) => {
                    backend_version = version;
                    *visible.committed.write().await = Arc::new(staged);
                }
                Err(error) => {
                    visible.poisoned.store(true, Ordering::Release);
                    let message = format!("store backend commit failed: {error}");
                    for (reply, _) in replies {
                        let _ = reply.send(Err(unavailable(&message)));
                    }
                    break;
                }
            }
        }
        for (reply, result) in replies {
            let _ = reply.send(result);
        }
    }
    visible.poisoned.store(true, Ordering::Release);
}

fn fail_batch(batch: Vec<Pending>, message: String, visible: &Visible) {
    visible.poisoned.store(true, Ordering::Release);
    for item in batch {
        let _ = item.reply.send(Err(unavailable(&message)));
    }
}

fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}

impl MetaStore for Store {
    fn health(&self) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            self.ensure_available()?;
            self.backend.health().await?;
            self.ensure_available()
        })
    }

    fn backend_readiness(&self) -> MetaFuture<'_, BackendReadiness> {
        Box::pin(async move {
            let persistence = self.backend.persistence();
            if let Err(error) = self.ensure_available() {
                return Ok(BackendReadiness::observed(
                    persistence,
                    false,
                    format!("store unavailable before backend probe: {error}"),
                ));
            }
            match self.backend.health().await {
                Ok(()) => match self.ensure_available() {
                    Ok(()) => Ok(BackendReadiness::observed(
                        persistence,
                        true,
                        format!("{} backend health probe passed", persistence.as_str()),
                    )),
                    Err(error) => Ok(BackendReadiness::observed(
                        persistence,
                        false,
                        format!("store unavailable after backend probe: {error}"),
                    )),
                },
                Err(error) => Ok(BackendReadiness::observed(
                    persistence,
                    false,
                    format!("backend health probe failed: {error}"),
                )),
            }
        })
    }

    fn read_view(&self) -> MetaFuture<'_, MetaReadView> {
        Box::pin(async move {
            Ok(MetaReadView {
                state: self.committed().await?,
            })
        })
    }

    fn read(&self, read: MetaRead) -> MetaFuture<'_, MetaSnapshot> {
        Box::pin(async move { self.committed().await?.read(read).await })
    }

    fn compare_and_commit(&self, txn: MetaTxn) -> MetaFuture<'_, TxnOutcome> {
        Box::pin(async move { self.mutate(Mutation::Transaction(txn)).await })
    }

    fn watch(&self, after_revision: StoreRevision, limit: usize) -> MetaFuture<'_, WatchBatch> {
        Box::pin(async move { self.committed().await?.watch(after_revision, limit).await })
    }

    fn register_node_session(
        &self,
        request: RequestKey,
        lease: NodeSessionLease,
    ) -> MetaFuture<'_, TxnOutcome> {
        Box::pin(async move { self.mutate(Mutation::RegisterNode(request, lease)).await })
    }

    fn scan_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> MetaFuture<'a, RecoveryScan> {
        Box::pin(async move {
            self.committed()
                .await?
                .scan_roots_for_recovery(home_node_id)
                .await
        })
    }

    fn scan_owner_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> MetaFuture<'a, OwnerRootScan> {
        Box::pin(async move {
            self.committed()
                .await?
                .scan_owner_roots_for_recovery(home_node_id)
                .await
        })
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct VersionedEntity {
    revision: StoreRevision,
    entity: MetaEntity,
}

#[derive(Clone, Default)]
struct MemoryState {
    revision: StoreRevision,
    entities: HashMap<MetaKey, VersionedEntity>,
    requests: HashMap<RequestKey, RequestOutcome>,
    events: Vec<WatchEvent>,
    node_epochs: HashMap<String, u64>,
}

/// Private staging state for one Store commit batch.
///
/// It executes conditional transactions and keeps replayable outcomes. This is
/// not the configured memory backend: Store publishes it only after its
/// selected backend has acknowledged the complete snapshot.
#[derive(Default)]
struct StoreState {
    state: Mutex<MemoryState>,
}

impl StoreState {
    fn new() -> Self {
        Self::default()
    }

    /// Fork only committed authority for a private Store batch. Nothing in
    /// this fork is visible to RPC readers until the backend acknowledges it.
    fn fork(&self) -> Result<Self> {
        let state = self
            .state
            .lock()
            .map_err(|_| invalid_contract("meta store lock poisoned"))?
            .clone();
        Ok(Self {
            state: Mutex::new(state),
        })
    }

    fn snapshot_bytes(&self) -> Result<Vec<u8>> {
        let state = self
            .state
            .lock()
            .map_err(|_| invalid_contract("meta store lock poisoned"))?;
        let snapshot = PersistedMemoryState {
            schema_version: 1,
            revision: state.revision,
            entities: state
                .entities
                .iter()
                .map(|(key, entity)| (key.clone(), entity.clone()))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
            requests: state
                .requests
                .iter()
                .map(|(key, outcome)| (key.clone(), outcome.clone()))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
            events: state.events.clone(),
            node_epochs: state
                .node_epochs
                .iter()
                .map(|(node, epoch)| (node.clone(), *epoch))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        };
        serde_json::to_vec(&snapshot)
            .map_err(|error| invalid_contract(format!("encode store snapshot: {error}")))
    }

    fn from_snapshot(bytes: &[u8]) -> Result<Self> {
        let snapshot: PersistedMemoryState = serde_json::from_slice(bytes)
            .map_err(|error| invalid_contract(format!("decode store snapshot: {error}")))?;
        if snapshot.schema_version != 1 {
            return Err(invalid_contract("unsupported store snapshot schema"));
        }
        let mut state = MemoryState {
            revision: snapshot.revision,
            events: snapshot.events,
            ..MemoryState::default()
        };
        for (key, versioned) in snapshot.entities {
            if entity_key(&versioned.entity) != key
                && !matches!((&key, &versioned.entity),
                    (MetaKey::CurrentNodeSession { node_id }, MetaEntity::NodeSession(session))
                        if node_id == &session.node_id)
            {
                return Err(invalid_contract("store entity key does not match value"));
            }
            if versioned.revision > state.revision
                || state.entities.insert(key, versioned).is_some()
            {
                return Err(invalid_contract(
                    "invalid store entity revision or duplicate key",
                ));
            }
        }
        for (key, outcome) in snapshot.requests {
            if key != outcome.request || state.requests.insert(key, outcome).is_some() {
                return Err(invalid_contract("invalid store request outcome"));
            }
        }
        for (node, epoch) in snapshot.node_epochs {
            if state.node_epochs.insert(node, epoch).is_some() {
                return Err(invalid_contract("duplicate store node epoch"));
            }
        }
        if state
            .events
            .iter()
            .any(|event| event.revision > state.revision)
        {
            return Err(invalid_contract("store event exceeds snapshot revision"));
        }
        Ok(Self {
            state: Mutex::new(state),
        })
    }
}

/// JSON object keys cannot represent structured `MetaKey`, so a versioned
/// snapshot stores maps as pairs. This is a persistence format, not an RPC API.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedMemoryState {
    schema_version: u32,
    revision: StoreRevision,
    entities: Vec<(MetaKey, VersionedEntity)>,
    requests: Vec<(RequestKey, RequestOutcome)>,
    events: Vec<WatchEvent>,
    node_epochs: Vec<(String, u64)>,
}

impl MetaStore for StoreState {
    fn health(&self) -> MetaFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn backend_readiness(&self) -> MetaFuture<'_, BackendReadiness> {
        Box::pin(async {
            Ok(BackendReadiness::observed(
                BackendPersistence::Unknown,
                true,
                "in-memory state has no configured backend capability",
            ))
        })
    }

    fn read_view(&self) -> MetaFuture<'_, MetaReadView> {
        Box::pin(async move {
            Ok(MetaReadView {
                state: Arc::new(self.fork()?),
            })
        })
    }

    fn read(&self, read: MetaRead) -> MetaFuture<'_, MetaSnapshot> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            let entity = match &read {
                MetaRead::NodeSession {
                    node_id,
                    session_id,
                } => state.entities.get(&MetaKey::NodeSession {
                    node_id: node_id.clone(),
                    session_id: session_id.clone(),
                }),
                MetaRead::CurrentNodeSession { node_id } => {
                    state.entities.get(&MetaKey::CurrentNodeSession {
                        node_id: node_id.clone(),
                    })
                }
                MetaRead::CurrentNodeSessions => None,
                MetaRead::Root { root_id } => state.entities.get(&MetaKey::Root {
                    root_id: root_id.clone(),
                }),
                MetaRead::RootReservation { root_id } => {
                    state.entities.get(&MetaKey::RootReservation {
                        root_id: root_id.clone(),
                    })
                }
                MetaRead::RootGrantByHolder {
                    root_id,
                    holder_node_id,
                    holder_session_id,
                } => state.entities.get(&MetaKey::RootGrant {
                    root_id: root_id.clone(),
                    holder_node_id: holder_node_id.clone(),
                    holder_session_id: holder_session_id.clone(),
                }),
                MetaRead::RootGrant {
                    root_id,
                    root_epoch,
                    home_session_id,
                    holder_node_id,
                    holder_session_id,
                    access_generation,
                    fencing_token,
                } => {
                    let key = MetaKey::RootGrant {
                        root_id: root_id.clone(),
                        holder_node_id: holder_node_id.clone(),
                        holder_session_id: holder_session_id.clone(),
                    };
                    let candidate = state.entities.get(&key);
                    if let Some(versioned) = candidate
                        && let MetaEntity::RootGrant(grant) = &versioned.entity
                        && grant.root_epoch == *root_epoch
                        && grant.home_session_id == *home_session_id
                        && grant.access_generation == *access_generation
                        && grant.fencing_token == *fencing_token
                        && current_session_matches(&state, holder_node_id, holder_session_id)
                        && root_session_matches(&state, root_id, *root_epoch, home_session_id)
                        && !has_pending_root_command(
                            &state,
                            root_id,
                            *root_epoch,
                            *access_generation,
                        )
                    {
                        candidate
                    } else {
                        None
                    }
                }
                MetaRead::RequestOutcome(_) => None,
                MetaRead::DfsDentry(key) => state.entities.get(&MetaKey::DfsDentry(key.clone())),
                MetaRead::DfsDirectory { .. } => None,
                MetaRead::DfsInode(id) => state.entities.get(&MetaKey::DfsInode(id.clone())),
                MetaRead::DfsFileVersion(id) => {
                    state.entities.get(&MetaKey::DfsFileVersion(id.clone()))
                }
                MetaRead::DfsLayoutRoot(id) => {
                    state.entities.get(&MetaKey::DfsLayoutRoot(id.clone()))
                }
                MetaRead::DfsPlacement(id) => {
                    state.entities.get(&MetaKey::DfsPlacement(id.clone()))
                }
                MetaRead::DfsPlacements => None,
                MetaRead::DfsChunk(id) => state.entities.get(&MetaKey::DfsChunk(id.clone())),
                MetaRead::DfsCopy(id) => state.entities.get(&MetaKey::DfsCopy(id.clone())),
                MetaRead::DfsReplicationConfig => {
                    state.entities.get(&MetaKey::DfsReplicationConfig)
                }
                MetaRead::DfsReplicationTask(id) => {
                    state.entities.get(&MetaKey::DfsReplicationTask(id.clone()))
                }
                MetaRead::DfsReplicationTasks => None,
                MetaRead::DfsWriteLease(id) => {
                    state.entities.get(&MetaKey::DfsWriteLease(id.clone()))
                }
            };
            let request_outcome = match &read {
                MetaRead::RequestOutcome(key) => state.requests.get(key).cloned(),
                _ => None,
            };
            let (revision, entities) = match &read {
                MetaRead::DfsDirectory {
                    namespace_id,
                    parent_inode_id,
                } => {
                    let mut entries = state
                        .entities
                        .values()
                        .filter_map(|versioned| match &versioned.entity {
                            MetaEntity::DfsDentry(dentry)
                                if dentry.key.namespace_id == *namespace_id
                                    && dentry.key.parent_inode_id == *parent_inode_id =>
                            {
                                Some((dentry.key.name.clone(), versioned.clone()))
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    entries.sort_by(|left, right| left.0.cmp(&right.0));
                    let revision = entries
                        .iter()
                        .fold(state.revision, |max, (_, entity)| max.max(entity.revision));
                    (
                        revision,
                        entries
                            .into_iter()
                            .map(|(_, versioned)| versioned.entity)
                            .collect(),
                    )
                }
                MetaRead::CurrentNodeSessions => {
                    let mut sessions = state
                        .entities
                        .iter()
                        .filter_map(|(key, versioned)| match (key, &versioned.entity) {
                            (
                                MetaKey::CurrentNodeSession { .. },
                                MetaEntity::NodeSession(session),
                            ) => Some((
                                session.node_id.clone(),
                                session.session_id.clone(),
                                versioned.clone(),
                            )),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    sessions.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
                    let revision = sessions.iter().fold(state.revision, |max, (_, _, entity)| {
                        max.max(entity.revision)
                    });
                    (
                        revision,
                        sessions
                            .into_iter()
                            .map(|(_, _, versioned)| versioned.entity)
                            .collect(),
                    )
                }
                MetaRead::DfsPlacements => {
                    let mut placements = state
                        .entities
                        .iter()
                        .filter_map(|(key, versioned)| match (key, &versioned.entity) {
                            (MetaKey::DfsPlacement(chunk_id), MetaEntity::DfsPlacement(_)) => {
                                Some((chunk_id.clone(), versioned.clone()))
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    placements.sort_by(|left, right| left.0.cmp(&right.0));
                    let revision = placements
                        .iter()
                        .fold(state.revision, |max, (_, entity)| max.max(entity.revision));
                    (
                        revision,
                        placements
                            .into_iter()
                            .map(|(_, versioned)| versioned.entity)
                            .collect(),
                    )
                }
                MetaRead::DfsReplicationTasks => {
                    let mut tasks = state
                        .entities
                        .iter()
                        .filter_map(|(key, versioned)| match (key, &versioned.entity) {
                            (
                                MetaKey::DfsReplicationTask(id),
                                MetaEntity::DfsReplicationTask(_),
                            ) => Some((id.clone(), versioned.clone())),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    tasks.sort_by(|left, right| left.0.cmp(&right.0));
                    let revision = tasks
                        .iter()
                        .fold(state.revision, |max, (_, entity)| max.max(entity.revision));
                    (
                        revision,
                        tasks
                            .into_iter()
                            .map(|(_, versioned)| versioned.entity)
                            .collect(),
                    )
                }
                _ => (
                    // A conditional update compares the entity's mod revision,
                    // not the store-wide revision of unrelated later writes.
                    entity
                        .map(|versioned| versioned.revision)
                        .unwrap_or(state.revision),
                    Vec::new(),
                ),
            };
            Ok(MetaSnapshot {
                revision,
                entity: entity.map(|versioned| versioned.entity.clone()),
                request_outcome,
                entities,
            })
        })
    }

    fn compare_and_commit(&self, txn: MetaTxn) -> MetaFuture<'_, TxnOutcome> {
        Box::pin(async move {
            txn.validate()?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            if let Some(existing) = state.requests.get(&txn.request).cloned() {
                return Ok(TxnOutcome::ConditionFailed {
                    revision: state.revision,
                    existing_outcome: Some(existing),
                });
            }
            if !txn
                .conditions
                .iter()
                .all(|condition| condition_matches(&state, condition))
            {
                return Ok(TxnOutcome::ConditionFailed {
                    revision: state.revision,
                    existing_outcome: None,
                });
            }
            let outcome = txn
                .mutations
                .iter()
                .find_map(|mutation| match mutation {
                    TxnMutation::RecordRequestOutcome(outcome) => Some(outcome.clone()),
                    _ => None,
                })
                .ok_or_else(|| invalid_contract("missing request outcome"))?;
            let revision = state.revision.next();
            state.revision = revision;
            let mut event_index = 0;
            for mutation in txn.mutations {
                match mutation {
                    TxnMutation::Put(entity) => {
                        let key = entity_key(&entity);
                        state.entities.insert(
                            key.clone(),
                            VersionedEntity {
                                revision,
                                entity: entity.clone(),
                            },
                        );
                        state.events.push(WatchEvent {
                            revision,
                            event_index,
                            key,
                            change: WatchChange::Put(entity),
                        });
                        event_index += 1;
                    }
                    TxnMutation::Delete(key) => {
                        state.entities.remove(&key);
                        state.events.push(WatchEvent {
                            revision,
                            event_index,
                            key,
                            change: WatchChange::Delete,
                        });
                        event_index += 1;
                    }
                    TxnMutation::RecordRequestOutcome(outcome) => {
                        state.requests.insert(outcome.request.clone(), outcome);
                    }
                }
            }
            Ok(TxnOutcome::Committed { revision, outcome })
        })
    }

    fn watch(&self, after_revision: StoreRevision, limit: usize) -> MetaFuture<'_, WatchBatch> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            let mut events: Vec<_> = state
                .events
                .iter()
                .filter(|event| event.revision > after_revision)
                .cloned()
                .collect();
            if limit > 0 && events.len() > limit {
                let cutoff_revision = events[limit - 1].revision;
                events.retain(|event| event.revision <= cutoff_revision);
            }
            let next_revision = events
                .last()
                .map_or(after_revision.next(), |event| event.revision.next());
            let batch = WatchBatch::Events {
                start_revision: after_revision,
                next_revision,
                events,
            };
            batch.validate_order()?;
            Ok(batch)
        })
    }

    fn register_node_session(
        &self,
        request: RequestKey,
        lease: NodeSessionLease,
    ) -> MetaFuture<'_, TxnOutcome> {
        Box::pin(async move {
            if lease.node_id.is_empty() || lease.session_id.is_empty() || lease.lease_ttl.is_zero()
            {
                return Err(invalid_contract(
                    "node_id, session_id and positive lease_ttl are required",
                ));
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            if let Some(existing) = state.requests.get(&request).cloned() {
                return Ok(TxnOutcome::ConditionFailed {
                    revision: state.revision,
                    existing_outcome: Some(existing),
                });
            }
            if !request.is_valid() {
                return Err(invalid_contract(
                    "request key must include caller_id and request_id",
                ));
            }
            let now_ms = now_unix_ms();
            if let Some(versioned) = state.entities.get(&MetaKey::NodeSession {
                node_id: lease.node_id.clone(),
                session_id: lease.session_id.clone(),
            }) && let MetaEntity::NodeSession(session) = &versioned.entity
                && !session.is_live_at_unix_ms(now_ms)
            {
                return Err(invalid_contract(
                    "expired node session cannot be renewed; start a new session_id",
                ));
            }
            // Heartbeat renewal extends one process session; it must not fence
            // placement and grants issued to that same live process. Only a
            // new session_id advances the Node epoch.
            let lease_epoch = state
                .entities
                .get(&MetaKey::CurrentNodeSession {
                    node_id: lease.node_id.clone(),
                })
                .and_then(|versioned| match &versioned.entity {
                    MetaEntity::NodeSession(session)
                        if session.session_id == lease.session_id
                            && session.is_live_at_unix_ms(now_ms) =>
                    {
                        Some(session.lease_epoch)
                    }
                    _ => None,
                })
                .unwrap_or_else(|| {
                    let next = state
                        .node_epochs
                        .get(&lease.node_id)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(1);
                    state.node_epochs.insert(lease.node_id.clone(), next);
                    next
                });
            let ttl_ms = u64::try_from(lease.lease_ttl.as_millis()).unwrap_or(u64::MAX);
            let session = NodeSession {
                node_id: lease.node_id.clone(),
                session_id: lease.session_id.clone(),
                grpc_addr: lease.grpc_addr,
                data_addr: lease.data_addr,
                rest_addr: lease.rest_addr,
                storage_devices: lease.storage_devices,
                lease_epoch,
                expires_at_unix_ms: now_ms.saturating_add(ttl_ms),
            };
            let outcome = RequestOutcome {
                request,
                operation: StoreOperation::RegisterNode,
                result: OperationResult::NodeSession(session.clone()),
            };
            let revision = state.revision.next();
            state.revision = revision;
            for (key, entity) in [
                (
                    MetaKey::NodeSession {
                        node_id: session.node_id.clone(),
                        session_id: session.session_id.clone(),
                    },
                    MetaEntity::NodeSession(session.clone()),
                ),
                (
                    MetaKey::CurrentNodeSession {
                        node_id: session.node_id.clone(),
                    },
                    MetaEntity::NodeSession(session.clone()),
                ),
            ] {
                state.entities.insert(
                    key.clone(),
                    VersionedEntity {
                        revision,
                        entity: entity.clone(),
                    },
                );
                let event_index = state.events.len() as u32;
                state.events.push(WatchEvent {
                    revision,
                    event_index,
                    key,
                    change: WatchChange::Put(entity),
                });
            }
            state
                .requests
                .insert(outcome.request.clone(), outcome.clone());
            Ok(TxnOutcome::Committed { revision, outcome })
        })
    }

    fn scan_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> MetaFuture<'a, RecoveryScan> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            let roots = state
                .entities
                .values()
                .filter_map(|versioned| match &versioned.entity {
                    MetaEntity::Root(root) if root.home_node_id == home_node_id => {
                        Some(root.clone())
                    }
                    _ => None,
                })
                .collect();
            Ok(RecoveryScan {
                revision: state.revision,
                home_node_id: home_node_id.to_owned(),
                roots,
            })
        })
    }

    fn scan_owner_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> MetaFuture<'a, OwnerRootScan> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            let mut roots = Vec::new();
            let mut reservations = Vec::new();
            for versioned in state.entities.values() {
                match &versioned.entity {
                    MetaEntity::Root(root) if root.home_node_id == home_node_id => {
                        roots.push(root.clone());
                    }
                    MetaEntity::RootReservation(reservation)
                        if reservation.home_node_id == home_node_id =>
                    {
                        reservations.push(reservation.clone());
                    }
                    _ => {}
                }
            }
            Ok(OwnerRootScan {
                revision: state.revision,
                home_node_id: home_node_id.to_owned(),
                roots,
                reservations,
            })
        })
    }
}

/// Creates the explicit error used when no backend is configured.
pub fn unavailable_meta_store() -> Error {
    Error::coded(
        afs_error::META_STORE_UNIMPLEMENTED,
        "MetaStore backend is not configured",
    )
}

fn invalid_contract(message: impl Into<String>) -> Error {
    Error::coded(afs_error::META_CATALOG_INVALID_REQUEST, message)
}

pub(crate) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

fn entity_key(entity: &MetaEntity) -> MetaKey {
    match entity {
        MetaEntity::NodeSession(session) => MetaKey::NodeSession {
            node_id: session.node_id.clone(),
            session_id: session.session_id.clone(),
        },
        MetaEntity::RootReservation(reservation) => MetaKey::RootReservation {
            root_id: reservation.root_id.clone(),
        },
        MetaEntity::Root(root) => MetaKey::Root {
            root_id: root.root_id.clone(),
        },
        MetaEntity::RootGrant(grant) => MetaKey::RootGrant {
            root_id: grant.root_id.clone(),
            holder_node_id: grant.holder_node_id.clone(),
            holder_session_id: grant.holder_session_id.clone(),
        },
        MetaEntity::RootCommand(command) => MetaKey::RootCommand {
            command_id: command.command_id.clone(),
        },
        MetaEntity::RootCommandAck(ack) => MetaKey::RootCommandAck {
            command_id: ack.command_id.clone(),
            home_node_id: ack.node_id.clone(),
            home_session_id: ack.session_id.clone(),
        },
        MetaEntity::DfsDentry(dentry) => MetaKey::DfsDentry(dentry.key.clone()),
        MetaEntity::DfsInode(inode) => MetaKey::DfsInode(inode.inode_id.clone()),
        MetaEntity::DfsFileVersion(version) => MetaKey::DfsFileVersion(version.id.clone()),
        MetaEntity::DfsLayoutRoot(layout) => MetaKey::DfsLayoutRoot(layout.id.clone()),
        MetaEntity::DfsChunk(chunk) => MetaKey::DfsChunk(chunk.id.clone()),
        MetaEntity::DfsReplicationConfig(_) => MetaKey::DfsReplicationConfig,
        MetaEntity::DfsPlacement(placement) => MetaKey::DfsPlacement(placement.chunk_id.clone()),
        MetaEntity::DfsCopy(copy) => MetaKey::DfsCopy(copy.id.clone()),
        MetaEntity::DfsReplicationTask(task) => MetaKey::DfsReplicationTask(task.id.clone()),
        MetaEntity::DfsWriteLease(lease) => MetaKey::DfsWriteLease(lease.inode_id.clone()),
    }
}

fn condition_matches(state: &MemoryState, condition: &TxnCondition) -> bool {
    match condition {
        TxnCondition::Missing(key) => !state.entities.contains_key(key),
        TxnCondition::RevisionEquals { key, revision } => state
            .entities
            .get(key)
            .is_some_and(|entity| entity.revision == *revision),
        TxnCondition::EntityEquals(entity) => state
            .entities
            .get(&entity_key(entity))
            .is_some_and(|current| current.entity == *entity),
        TxnCondition::NodeSessionCurrent {
            node_id,
            session_id,
        } => current_session_matches(state, node_id, session_id),
        TxnCondition::RootEpochEquals {
            root_id,
            root_epoch,
        } => state
            .entities
            .get(&MetaKey::Root {
                root_id: root_id.clone(),
            })
            .is_some_and(|entity| {
                matches!(
                    &entity.entity,
                    MetaEntity::Root(root) if root.root_epoch == *root_epoch
                )
            }),
        TxnCondition::RootCommandAcked {
            command_id,
            node_id,
            session_id,
        } => state
            .entities
            .get(&MetaKey::RootCommandAck {
                command_id: command_id.clone(),
                home_node_id: node_id.clone(),
                home_session_id: session_id.clone(),
            })
            .is_some_and(|entity| {
                matches!(
                    &entity.entity,
                    MetaEntity::RootCommandAck(ack) if ack.success
                )
            }),
        TxnCondition::RootCommandMatches {
            command_id,
            home_node_id,
            home_session_id,
            root_id,
            root_epoch,
            old_access_generation,
            command_type,
        } => state
            .entities
            .get(&MetaKey::RootCommand {
                command_id: command_id.clone(),
            })
            .is_some_and(|entity| {
                matches!(
                    &entity.entity,
                    MetaEntity::RootCommand(command)
                        if command.home_node_id == *home_node_id
                            && command.home_session_id == *home_session_id
                            && command.root_id == *root_id
                            && command.root_epoch == *root_epoch
                            && command.old_access_generation == *old_access_generation
                            && command.command_type == *command_type
                )
            }),
        TxnCondition::RootCommandAckedExact {
            command_id,
            node_id,
            session_id,
            root_id,
            root_epoch,
            access_generation,
            command_type,
        } => {
            let acked = state
                .entities
                .get(&MetaKey::RootCommandAck {
                    command_id: command_id.clone(),
                    home_node_id: node_id.clone(),
                    home_session_id: session_id.clone(),
                })
                .is_some_and(|entity| {
                    matches!(
                        &entity.entity,
                        MetaEntity::RootCommandAck(ack)
                            if ack.success
                                && ack.root_id == *root_id
                                && ack.root_epoch == *root_epoch
                                && ack.access_generation == *access_generation
                    )
                });
            let command_matches = state
                .entities
                .get(&MetaKey::RootCommand {
                    command_id: command_id.clone(),
                })
                .is_some_and(|entity| {
                    matches!(
                        &entity.entity,
                        MetaEntity::RootCommand(command)
                            if command.home_node_id == *node_id
                                && command.home_session_id == *session_id
                                && command.root_id == *root_id
                                && command.root_epoch == *root_epoch
                                && command.old_access_generation == *access_generation
                                && command.command_type == *command_type
                    )
                });
            acked && command_matches
        }
        TxnCondition::RequestAbsent(key) => !state.requests.contains_key(key),
        TxnCondition::DfsDirectoryEmpty {
            namespace_id,
            parent_inode_id,
        } => dfs_directory_is_empty(state, namespace_id, parent_inode_id),
        TxnCondition::DfsNotDescendant {
            namespace_id,
            ancestor_inode_id,
            child_inode_id,
        } => !dfs_is_descendant_or_same(state, namespace_id, ancestor_inode_id, child_inode_id),
    }
}

fn dfs_directory_is_empty(
    state: &MemoryState,
    namespace_id: &NamespaceId,
    parent_inode_id: &InodeId,
) -> bool {
    !state.entities.values().any(|versioned| {
        matches!(
            &versioned.entity,
            MetaEntity::DfsDentry(dentry)
                if dentry.key.namespace_id == *namespace_id
                    && dentry.key.parent_inode_id == *parent_inode_id
        )
    })
}

fn dfs_is_descendant_or_same(
    state: &MemoryState,
    namespace_id: &NamespaceId,
    ancestor_inode_id: &InodeId,
    child_inode_id: &InodeId,
) -> bool {
    if ancestor_inode_id == child_inode_id {
        return true;
    }
    let mut current = child_inode_id.clone();
    let mut seen = std::collections::HashSet::new();
    while seen.insert(current.clone()) {
        let Some(parent) = dfs_parent_of(state, namespace_id, &current) else {
            return false;
        };
        if &parent == ancestor_inode_id {
            return true;
        }
        if parent.0 == "1" {
            return false;
        }
        current = parent;
    }
    true
}

fn dfs_parent_of(
    state: &MemoryState,
    namespace_id: &NamespaceId,
    inode_id: &InodeId,
) -> Option<InodeId> {
    state
        .entities
        .values()
        .find_map(|versioned| match &versioned.entity {
            MetaEntity::DfsDentry(dentry)
                if dentry.key.namespace_id == *namespace_id && dentry.inode_id == *inode_id =>
            {
                Some(dentry.key.parent_inode_id.clone())
            }
            _ => None,
        })
}

fn current_session_matches(state: &MemoryState, node_id: &str, session_id: &str) -> bool {
    let now_ms = now_unix_ms();
    state
        .entities
        .get(&MetaKey::CurrentNodeSession {
            node_id: node_id.to_owned(),
        })
        .is_some_and(|entity| {
            matches!(
                &entity.entity,
                MetaEntity::NodeSession(session)
                    if session.session_id == session_id
                        && session.is_live_at_unix_ms(now_ms)
            )
        })
}

fn root_session_matches(
    state: &MemoryState,
    root_id: &str,
    root_epoch: u64,
    home_session_id: &str,
) -> bool {
    state
        .entities
        .get(&MetaKey::Root {
            root_id: root_id.to_owned(),
        })
        .is_some_and(|entity| {
            if let MetaEntity::Root(root) = &entity.entity {
                root.root_epoch == root_epoch
                    && root.home_session_id == home_session_id
                    && current_session_matches(state, &root.home_node_id, home_session_id)
            } else {
                false
            }
        })
}

fn has_pending_root_command(
    state: &MemoryState,
    root_id: &str,
    root_epoch: u64,
    access_generation: u64,
) -> bool {
    state.entities.iter().any(|(key, versioned)| {
        matches!(key, MetaKey::RootCommand { .. })
            && matches!(
                &versioned.entity,
                MetaEntity::RootCommand(command)
                    if command.root_id == root_id
                        && command.root_epoch == root_epoch
                        && command.old_access_generation == access_generation
            )
    })
}

impl fmt::Display for StoreRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Notify;

    fn request() -> RequestKey {
        RequestKey::new("node-a/session-1", "req-1")
    }

    fn grant() -> RootAccessGrant {
        RootAccessGrant {
            root_id: "workspace-1".into(),
            root_epoch: 7,
            home_node_id: "node-a".into(),
            home_session_id: "home-session-2".into(),
            holder_node_id: "node-b".into(),
            holder_session_id: "holder-session-9".into(),
            access_generation: 3,
            rights: vec![RootRight::Lookup, RootRight::Read],
            fencing_token: "fence-3".into(),
            issued_at_revision: StoreRevision(11),
        }
    }

    fn session(node_id: &str, session_id: &str, expires_at_unix_ms: u64) -> NodeSession {
        NodeSession {
            node_id: node_id.into(),
            session_id: session_id.into(),
            grpc_addr: format!("http://{node_id}:7400"),
            data_addr: format!("http://{node_id}:7500"),
            rest_addr: format!("http://{node_id}:7600"),
            storage_devices: Vec::new(),
            lease_epoch: 1,
            expires_at_unix_ms,
        }
    }

    #[test]
    fn node_session_liveness_uses_strict_expiry_boundary() {
        let session = session("node-a", "session-1", 100);
        assert!(!session.is_live_at_unix_ms(100));
        assert!(!session.is_live_at_unix_ms(101));
        assert!(session.is_live_at_unix_ms(99));
    }

    #[test]
    fn snapshot_bytes_are_independent_of_hash_map_insertion_order() {
        fn state_in_order(nodes: &[&str]) -> StoreState {
            let mut state = MemoryState {
                revision: StoreRevision(7),
                ..MemoryState::default()
            };
            for node in nodes {
                let value = session(node, "session-1", 100);
                state.entities.insert(
                    MetaKey::NodeSession {
                        node_id: (*node).to_owned(),
                        session_id: value.session_id.clone(),
                    },
                    VersionedEntity {
                        revision: StoreRevision(7),
                        entity: MetaEntity::NodeSession(value.clone()),
                    },
                );
                let request = RequestKey::new(*node, "register-1");
                state.requests.insert(
                    request.clone(),
                    RequestOutcome {
                        request,
                        operation: StoreOperation::RegisterNode,
                        result: OperationResult::NodeSession(value),
                    },
                );
                state.node_epochs.insert((*node).to_owned(), 1);
            }
            StoreState {
                state: Mutex::new(state),
            }
        }

        let bytes = state_in_order(&["node-a", "node-b", "node-c"])
            .snapshot_bytes()
            .unwrap();
        assert_eq!(
            bytes,
            state_in_order(&["node-c", "node-a", "node-b"])
                .snapshot_bytes()
                .unwrap()
        );
        assert_eq!(
            bytes,
            StoreState::from_snapshot(&bytes)
                .unwrap()
                .snapshot_bytes()
                .unwrap()
        );
    }

    #[test]
    fn current_session_match_rejects_expired_session() {
        let mut state = MemoryState::default();
        let expired = session("node-a", "session-1", 0);
        state.entities.insert(
            MetaKey::CurrentNodeSession {
                node_id: "node-a".into(),
            },
            VersionedEntity {
                revision: StoreRevision(1),
                entity: MetaEntity::NodeSession(expired),
            },
        );
        assert!(!current_session_matches(&state, "node-a", "session-1"));

        let live = session("node-a", "session-1", now_unix_ms().saturating_add(60_000));
        state.entities.insert(
            MetaKey::CurrentNodeSession {
                node_id: "node-a".into(),
            },
            VersionedEntity {
                revision: StoreRevision(2),
                entity: MetaEntity::NodeSession(live),
            },
        );
        assert!(current_session_matches(&state, "node-a", "session-1"));
    }

    #[tokio::test]
    async fn root_grant_read_rejects_expired_holder_session() {
        let mut state = MemoryState {
            revision: StoreRevision(3),
            ..MemoryState::default()
        };
        let root = RootRecord {
            root_id: "workspace-1".into(),
            root_epoch: 7,
            home_node_id: "node-a".into(),
            home_session_id: "home-session-2".into(),
            local_prepare_id: "prepare-1".into(),
            created_at_revision: StoreRevision(1),
            updated_at_revision: StoreRevision(1),
        };
        let grant = grant();
        for (key, entity) in [
            (
                MetaKey::Root {
                    root_id: root.root_id.clone(),
                },
                MetaEntity::Root(root),
            ),
            (
                MetaKey::CurrentNodeSession {
                    node_id: grant.home_node_id.clone(),
                },
                MetaEntity::NodeSession(session(
                    &grant.home_node_id,
                    &grant.home_session_id,
                    now_unix_ms().saturating_add(60_000),
                )),
            ),
            (
                MetaKey::CurrentNodeSession {
                    node_id: grant.holder_node_id.clone(),
                },
                MetaEntity::NodeSession(session(
                    &grant.holder_node_id,
                    &grant.holder_session_id,
                    0,
                )),
            ),
            (
                MetaKey::RootGrant {
                    root_id: grant.root_id.clone(),
                    holder_node_id: grant.holder_node_id.clone(),
                    holder_session_id: grant.holder_session_id.clone(),
                },
                MetaEntity::RootGrant(grant.clone()),
            ),
        ] {
            state.entities.insert(
                key,
                VersionedEntity {
                    revision: StoreRevision(3),
                    entity,
                },
            );
        }
        let store = StoreState {
            state: Mutex::new(state),
        };
        let snapshot = store
            .read(MetaRead::RootGrant {
                root_id: grant.root_id,
                root_epoch: grant.root_epoch,
                home_session_id: grant.home_session_id,
                holder_node_id: grant.holder_node_id,
                holder_session_id: grant.holder_session_id,
                access_generation: grant.access_generation,
                fencing_token: grant.fencing_token,
            })
            .await
            .unwrap();
        assert!(snapshot.entity.is_none());
    }

    #[tokio::test]
    async fn root_grant_read_rejects_expired_home_session() {
        let mut state = MemoryState {
            revision: StoreRevision(3),
            ..MemoryState::default()
        };
        let root = RootRecord {
            root_id: "workspace-1".into(),
            root_epoch: 7,
            home_node_id: "node-a".into(),
            home_session_id: "home-session-2".into(),
            local_prepare_id: "prepare-1".into(),
            created_at_revision: StoreRevision(1),
            updated_at_revision: StoreRevision(1),
        };
        let grant = grant();
        for (key, entity) in [
            (
                MetaKey::Root {
                    root_id: root.root_id.clone(),
                },
                MetaEntity::Root(root),
            ),
            (
                MetaKey::CurrentNodeSession {
                    node_id: grant.home_node_id.clone(),
                },
                MetaEntity::NodeSession(session(&grant.home_node_id, &grant.home_session_id, 0)),
            ),
            (
                MetaKey::CurrentNodeSession {
                    node_id: grant.holder_node_id.clone(),
                },
                MetaEntity::NodeSession(session(
                    &grant.holder_node_id,
                    &grant.holder_session_id,
                    now_unix_ms().saturating_add(60_000),
                )),
            ),
            (
                MetaKey::RootGrant {
                    root_id: grant.root_id.clone(),
                    holder_node_id: grant.holder_node_id.clone(),
                    holder_session_id: grant.holder_session_id.clone(),
                },
                MetaEntity::RootGrant(grant.clone()),
            ),
        ] {
            state.entities.insert(
                key,
                VersionedEntity {
                    revision: StoreRevision(3),
                    entity,
                },
            );
        }
        let store = StoreState {
            state: Mutex::new(state),
        };
        let snapshot = store
            .read(MetaRead::RootGrant {
                root_id: grant.root_id,
                root_epoch: grant.root_epoch,
                home_session_id: grant.home_session_id,
                holder_node_id: grant.holder_node_id,
                holder_session_id: grant.holder_session_id,
                access_generation: grant.access_generation,
                fencing_token: grant.fencing_token,
            })
            .await
            .unwrap();
        assert!(snapshot.entity.is_none());
    }

    #[test]
    fn transaction_requires_same_commit_idempotent_outcome() {
        let req = request();
        let mut txn = MetaTxn::new(req.clone(), StoreOperation::AcquireRoot);
        txn.conditions
            .push(TxnCondition::RequestAbsent(req.clone()));
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::RootGrant(grant())));
        assert!(txn.validate().is_err());

        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: req,
                operation: StoreOperation::AcquireRoot,
                result: OperationResult::RootGrant(grant()),
            }));
        txn.validate().unwrap();
    }

    #[test]
    fn transaction_rejects_reused_request_id_for_other_operation() {
        let req = request();
        let mut txn = MetaTxn::new(req.clone(), StoreOperation::RecoverRoot);
        txn.conditions
            .push(TxnCondition::RequestAbsent(req.clone()));
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::RootGrant(grant())));
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: req,
                operation: StoreOperation::AcquireRoot,
                result: OperationResult::RootGrant(grant()),
            }));
        assert!(txn.validate().is_err());
    }

    fn n2b1_command(
        command_id: &str,
        root_id: &str,
        old_access_generation: u64,
        command_type: RootCommandType,
    ) -> RootCommandRecord {
        RootCommandRecord {
            command_id: command_id.into(),
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            root_id: root_id.into(),
            root_epoch: 7,
            old_access_generation,
            command_type,
        }
    }

    fn n2b1_ack(command_id: &str, root_id: &str, access_generation: u64) -> RootCommandAck {
        RootCommandAck {
            command_id: command_id.into(),
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            root_id: root_id.into(),
            root_epoch: 7,
            access_generation,
            success: true,
            message: "installed".into(),
        }
    }

    #[test]
    fn n2b1_old_root_command_records_default_to_revoke_access() {
        let raw = r#"{
            "command_id":"cmd-old",
            "home_node_id":"node-a",
            "home_session_id":"session-a",
            "root_id":"workspace-a",
            "root_epoch":7,
            "old_access_generation":3
        }"#;
        let command: RootCommandRecord = serde_json::from_str(raw).unwrap();
        assert_eq!(command.command_type, RootCommandType::RevokeAccess);
    }

    #[test]
    fn n2b1_root_command_exact_conditions_match_stored_command_and_ack() {
        let command = n2b1_command("cmd-exact", "workspace-a", 3, RootCommandType::RevokeAccess);
        let ack = n2b1_ack("cmd-exact", "workspace-a", 3);
        let mut state = MemoryState::default();
        state.entities.insert(
            MetaKey::RootCommand {
                command_id: command.command_id.clone(),
            },
            VersionedEntity {
                revision: StoreRevision(1),
                entity: MetaEntity::RootCommand(command),
            },
        );
        state.entities.insert(
            MetaKey::RootCommandAck {
                command_id: ack.command_id.clone(),
                home_node_id: ack.node_id.clone(),
                home_session_id: ack.session_id.clone(),
            },
            VersionedEntity {
                revision: StoreRevision(2),
                entity: MetaEntity::RootCommandAck(ack),
            },
        );

        assert!(condition_matches(
            &state,
            &TxnCondition::RootCommandMatches {
                command_id: "cmd-exact".into(),
                home_node_id: "node-a".into(),
                home_session_id: "session-a".into(),
                root_id: "workspace-a".into(),
                root_epoch: 7,
                old_access_generation: 3,
                command_type: RootCommandType::RevokeAccess,
            }
        ));
        assert!(condition_matches(
            &state,
            &TxnCondition::RootCommandAckedExact {
                command_id: "cmd-exact".into(),
                node_id: "node-a".into(),
                session_id: "session-a".into(),
                root_id: "workspace-a".into(),
                root_epoch: 7,
                access_generation: 3,
                command_type: RootCommandType::RevokeAccess,
            }
        ));
        assert!(!condition_matches(
            &state,
            &TxnCondition::RootCommandMatches {
                command_id: "cmd-exact".into(),
                home_node_id: "node-a".into(),
                home_session_id: "session-a".into(),
                root_id: "workspace-a".into(),
                root_epoch: 7,
                old_access_generation: 4,
                command_type: RootCommandType::RevokeAccess,
            }
        ));
        assert!(!condition_matches(
            &state,
            &TxnCondition::RootCommandAckedExact {
                command_id: "cmd-exact".into(),
                node_id: "node-a".into(),
                session_id: "session-a".into(),
                root_id: "workspace-a".into(),
                root_epoch: 7,
                access_generation: 3,
                command_type: RootCommandType::InvalidateCache,
            }
        ));
    }

    #[test]
    fn n2b1_watch_batch_rejects_max_revision_event_without_advance_cursor() {
        let batch = WatchBatch::Events {
            start_revision: StoreRevision(u64::MAX - 2),
            next_revision: StoreRevision(u64::MAX),
            events: vec![WatchEvent {
                revision: StoreRevision(u64::MAX),
                event_index: 0,
                key: MetaKey::Root {
                    root_id: "workspace-max".into(),
                },
                change: WatchChange::Delete,
            }],
        };
        assert!(batch.validate_order().is_err());
    }

    #[tokio::test]
    async fn n2b1_watch_limit_preserves_revision_boundary_at_1024() {
        let mut state = MemoryState {
            revision: StoreRevision(1024),
            ..MemoryState::default()
        };
        for revision in 1..=1023 {
            state.events.push(WatchEvent {
                revision: StoreRevision(revision),
                event_index: 0,
                key: MetaKey::Root {
                    root_id: format!("workspace-{revision}"),
                },
                change: WatchChange::Delete,
            });
        }
        state
            .events
            .extend([0, 1].into_iter().map(|event_index| WatchEvent {
                revision: StoreRevision(1024),
                event_index,
                key: MetaKey::Root {
                    root_id: format!("workspace-boundary-{event_index}"),
                },
                change: WatchChange::Delete,
            }));
        let store = StoreState {
            state: Mutex::new(state),
        };
        let WatchBatch::Events {
            start_revision,
            next_revision,
            events,
        } = store.watch(StoreRevision::ZERO, 1024).await.unwrap()
        else {
            panic!("memory store should return events");
        };
        assert_eq!(start_revision, StoreRevision::ZERO);
        assert_eq!(next_revision, StoreRevision(1025));
        assert_eq!(events.len(), 1025);
        assert_eq!(events[1023].revision, StoreRevision(1024));
        assert_eq!(events[1024].revision, StoreRevision(1024));
    }

    #[test]
    fn watch_batch_rejects_duplicate_positions_within_a_revision() {
        let batch = WatchBatch::Events {
            start_revision: StoreRevision(10),
            next_revision: StoreRevision(12),
            events: vec![
                WatchEvent {
                    revision: StoreRevision(11),
                    event_index: 0,
                    key: MetaKey::Root {
                        root_id: "workspace-1".into(),
                    },
                    change: WatchChange::Delete,
                },
                WatchEvent {
                    revision: StoreRevision(11),
                    event_index: 0,
                    key: MetaKey::Root {
                        root_id: "workspace-2".into(),
                    },
                    change: WatchChange::Delete,
                },
            ],
        };
        assert!(batch.validate_order().is_err());
    }

    #[test]
    fn watch_allows_multiple_ordered_keys_in_one_revision() {
        let batch = WatchBatch::Events {
            start_revision: StoreRevision(10),
            next_revision: StoreRevision(12),
            events: [0, 1]
                .into_iter()
                .map(|event_index| WatchEvent {
                    revision: StoreRevision(11),
                    event_index,
                    key: MetaKey::Root {
                        root_id: format!("workspace-{event_index}"),
                    },
                    change: WatchChange::Delete,
                })
                .collect(),
        };
        batch.validate_order().unwrap();
    }

    #[test]
    fn unavailable_store_is_explicit_not_an_in_memory_fallback() {
        let error = unavailable_meta_store();
        assert_eq!(error.kind(), afs_error::ErrorKind::Unimplemented);
        assert!(error.message().contains("not configured"));
    }

    #[test]
    fn entity_names_are_stable_for_diagnostics() {
        assert_eq!(MetaEntity::RootGrant(grant()).name(), "root_grant");
    }

    #[test]
    fn meta_store_is_replaceable_behind_a_trait_object() {
        fn accepts_trait_object(_: Option<&dyn MetaStore>) {}
        accepts_trait_object(None);
    }
    fn lease(session_id: &str) -> NodeSessionLease {
        NodeSessionLease {
            node_id: "node-a".into(),
            session_id: session_id.into(),
            grpc_addr: "http://node-a:7400".into(),
            data_addr: "http://node-a:7500".into(),
            rest_addr: "http://node-a:7600".into(),
            storage_devices: Vec::new(),
            lease_ttl: Duration::from_secs(30),
        }
    }

    #[tokio::test]
    async fn replay_and_session_fencing_survive_store_reopen() {
        let backend = Arc::new(memory::MemoryBackend::default());
        let store = Store::open(backend.clone()).await.unwrap();
        let request = RequestKey::new("node-a", "register-1");
        let first = store
            .register_node_session(request.clone(), lease("session-1"))
            .await
            .unwrap();
        assert!(matches!(first, TxnOutcome::Committed { .. }));
        drop(store);

        let reopened = Store::open(backend).await.unwrap();
        let retry = reopened
            .register_node_session(request, lease("session-1"))
            .await
            .unwrap();
        assert!(matches!(
            retry,
            TxnOutcome::ConditionFailed {
                existing_outcome: Some(_),
                ..
            }
        ));
        let current = reopened
            .read(MetaRead::CurrentNodeSession {
                node_id: "node-a".into(),
            })
            .await
            .unwrap();
        assert!(
            matches!(current.entity, Some(MetaEntity::NodeSession(session)) if session.session_id == "session-1" && session.lease_epoch == 1)
        );
        reopened
            .register_node_session(RequestKey::new("node-a", "register-2"), lease("session-2"))
            .await
            .unwrap();
        let current = reopened
            .read(MetaRead::CurrentNodeSession {
                node_id: "node-a".into(),
            })
            .await
            .unwrap();
        assert!(
            matches!(current.entity, Some(MetaEntity::NodeSession(session)) if session.session_id == "session-2" && session.lease_epoch == 2)
        );
    }

    #[tokio::test]
    async fn expired_session_id_cannot_be_registered_again() {
        let store = Store::open(Arc::new(memory::MemoryBackend::default()))
            .await
            .unwrap();
        store
            .register_node_session(
                RequestKey::new("node-a", "register-1"),
                NodeSessionLease {
                    lease_ttl: Duration::from_nanos(1),
                    ..lease("session-1")
                },
            )
            .await
            .unwrap();

        let error = store
            .register_node_session(RequestKey::new("node-a", "register-2"), lease("session-1"))
            .await
            .unwrap_err();
        assert!(error.message().contains("expired node session"));
    }

    #[tokio::test]
    async fn concurrent_commands_keep_distinct_ordered_logical_revisions() {
        let store = Store::open(Arc::new(memory::MemoryBackend::default()))
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            store.register_node_session(RequestKey::new("node-a", "r1"), lease("session-1")),
            store.register_node_session(RequestKey::new("node-a", "r2"), lease("session-2")),
        );
        let (
            TxnOutcome::Committed { revision: one, .. },
            TxnOutcome::Committed { revision: two, .. },
        ) = (first.unwrap(), second.unwrap())
        else {
            panic!("both distinct registration requests must commit");
        };
        assert_ne!(one, two);
        let batch = store.watch(StoreRevision::ZERO, 1).await.unwrap();
        batch.validate_order().unwrap();
        let WatchBatch::Events { events, .. } = batch else {
            panic!("memory store must retain these events");
        };
        let earlier = one.min(two);
        let later = one.max(two);
        assert!(events.iter().any(|event| event.revision == earlier));
        assert!(!events.iter().any(|event| event.revision == later));
    }

    struct GatedBackend {
        inner: memory::MemoryBackend,
        started: Notify,
        release: Notify,
        fail: bool,
    }

    impl StoreBackend for GatedBackend {
        fn load(&self) -> MetaFuture<'_, Option<(u64, Vec<u8>)>> {
            self.inner.load()
        }

        fn commit(&self, expected_version: u64, bytes: Vec<u8>) -> MetaFuture<'_, u64> {
            Box::pin(async move {
                self.started.notify_one();
                self.release.notified().await;
                if self.fail {
                    Err(unavailable("injected disk error"))
                } else {
                    self.inner.commit(expected_version, bytes).await
                }
            })
        }
    }

    #[tokio::test]
    async fn pinned_read_view_keeps_one_revision_across_later_commits() {
        let store = Store::open(Arc::new(memory::MemoryBackend::default()))
            .await
            .unwrap();
        let view = store.read_view().await.unwrap();
        let key = MetaRead::CurrentNodeSession {
            node_id: "node-a".into(),
        };
        let before = view.read(key.clone()).await.unwrap();
        store
            .register_node_session(
                RequestKey::new("node-a", "view-register"),
                lease("session-1"),
            )
            .await
            .unwrap();
        let pinned = view.read(key.clone()).await.unwrap();
        let current = store.read(key).await.unwrap();
        assert_eq!(before.revision, pinned.revision);
        assert!(pinned.entity.is_none());
        assert!(current.entity.is_some());
        assert!(current.revision > pinned.revision);
    }

    #[tokio::test]
    async fn pending_write_is_invisible_until_backend_ack() {
        let backend = Arc::new(GatedBackend {
            inner: memory::MemoryBackend::default(),
            started: Notify::new(),
            release: Notify::new(),
            fail: false,
        });
        let store = Arc::new(Store::open(backend.clone()).await.unwrap());
        let writer = {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                store
                    .register_node_session(RequestKey::new("node-a", "r1"), lease("session-1"))
                    .await
            })
        };
        backend.started.notified().await;
        assert!(
            store
                .read(MetaRead::CurrentNodeSession {
                    node_id: "node-a".into()
                })
                .await
                .unwrap()
                .entity
                .is_none()
        );
        backend.release.notify_one();
        writer.await.unwrap().unwrap();
        assert!(
            store
                .read(MetaRead::CurrentNodeSession {
                    node_id: "node-a".into()
                })
                .await
                .unwrap()
                .entity
                .is_some()
        );
    }

    #[tokio::test]
    async fn failed_commit_poison_store_instead_of_exposing_stale_authority() {
        let backend = Arc::new(GatedBackend {
            inner: memory::MemoryBackend::default(),
            started: Notify::new(),
            release: Notify::new(),
            fail: true,
        });
        let store = Arc::new(Store::open(backend.clone()).await.unwrap());
        let writer = {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                store
                    .register_node_session(RequestKey::new("node-a", "r1"), lease("session-1"))
                    .await
            })
        };
        backend.started.notified().await;
        backend.release.notify_one();
        assert!(writer.await.unwrap().is_err());
        assert!(
            store
                .read(MetaRead::CurrentNodeSession {
                    node_id: "node-a".into()
                })
                .await
                .is_err()
        );
    }

    /// Run only against a fresh dedicated etcd instance. This writes the
    /// fixed production-format key and intentionally leaves it for replay.
    #[tokio::test]
    async fn store_replays_business_state_from_dedicated_etcd() {
        let Ok(endpoint) = std::env::var("AFS_TEST_ETCD_STORE_BUSINESS_ENDPOINT") else {
            return;
        };
        let backend = Arc::new(etcd::EtcdBackend::connect(endpoint.clone()).await.unwrap());
        let store = Store::open(backend).await.unwrap();
        store
            .register_node_session(RequestKey::new("node-a", "etcd-r1"), lease("etcd-session"))
            .await
            .unwrap();
        drop(store);

        let reopened = Store::open(Arc::new(
            etcd::EtcdBackend::connect(endpoint).await.unwrap(),
        ))
        .await
        .unwrap();
        let current = reopened
            .read(MetaRead::CurrentNodeSession {
                node_id: "node-a".into(),
            })
            .await
            .unwrap();
        assert!(
            matches!(current.entity, Some(MetaEntity::NodeSession(session)) if session.session_id == "etcd-session")
        );
        let replay = reopened
            .register_node_session(RequestKey::new("node-a", "etcd-r1"), lease("etcd-session"))
            .await
            .unwrap();
        assert!(matches!(
            replay,
            TxnOutcome::ConditionFailed {
                existing_outcome: Some(_),
                ..
            }
        ));
    }
}
