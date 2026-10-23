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
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use afs_error::{Error, Result};
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
mod profile;
mod state;
use state::State;
#[cfg(test)]
use state::{State as MemoryState, StoreState, VersionedEntity};

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
    DfsDurabilityPolicy(DfsDurabilityPolicy),
    DfsPlacement(PlacementRecord),
    DfsCopy(CopyRecord),
    DfsReplicationTask(ReplicationTask),
    DfsWriteLease(WriteLease),
    /// Latest issued OwnerFs incarnation; never removed with a Root.
    RootEpochWatermark(u64),
    DfsReference(crate::dfs::GcReference),
    DfsGc(crate::dfs::GcState),
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
            Self::DfsDurabilityPolicy(_) => "dfs_durability_policy",
            Self::DfsPlacement(_) => "dfs_placement",
            Self::DfsCopy(_) => "dfs_copy",
            Self::DfsReplicationTask(_) => "dfs_replication_task",
            Self::DfsWriteLease(_) => "dfs_write_lease",
            Self::RootEpochWatermark(_) => "root_epoch_watermark",
            Self::DfsReference(_) => "dfs_reference",
            Self::DfsGc(_) => "dfs_gc",
        }
    }
}

/// Linearizable read selector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaRead {
    /// Logical allocation floor, including legacy authority before the first allocation.
    RootEpochWatermark,
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
    /// Administrative scan of all stored grants, including invalid historical ones.
    /// Logical deactivation and bounded maintenance do not use this full scan.
    RootGrants {
        root_id: String,
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
    DfsDurabilityPolicy,
    DfsReplicationTask(ReplicationTaskId),
    DfsReplicationTasks,
    DfsWriteLease(InodeId),
    DfsGc,
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
    /// Compare the allocation floor in the same writer turn as reserving an epoch.
    RootEpochWatermarkEquals(u64),
    StoreRevisionEquals(StoreRevision),
    Missing(MetaKey),
    RevisionEquals {
        key: MetaKey,
        revision: StoreRevision,
    },
    /// Requires the current durable value at `entity_key(entity)` to exactly
    /// match `entity`.
    ///
    /// This is used for reservation transitions where revision numbers alone
    /// are not enough: a root can be reserved, aborted, and reserved again under
    /// RootId, so stale activate/abort calls must compare the complete
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
    DfsDurabilityPolicy,
    DfsPlacement(crate::dfs::ChunkId),
    DfsCopy(crate::dfs::CopyId),
    DfsReplicationTask(ReplicationTaskId),
    DfsWriteLease(InodeId),
    RootEpochWatermark,
    DfsReference {
        node_id: String,
        session_id: String,
        reference_id: String,
    },
    DfsGc,
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
    DeactivateRoot,
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
    DfsInitializeDurabilityPolicy,
    DfsClaimReplicationTask,
    DfsReportReplicationTask,
    DfsReportChunkCorruption,
    CleanupRootGrants,
    CleanupRootCommands,
    DfsUpdateReference,
    DfsCollectGarbage,
}

impl StoreOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::CleanupRootGrants => "cleanup_root_grants",
            Self::CleanupRootCommands => "cleanup_root_commands",
            Self::RegisterNode => "register_node",
            Self::ReserveRoot => "reserve_root",
            Self::AbortRoot => "abort_root",
            Self::DeactivateRoot => "deactivate_root",
            Self::ActivateRoot => "activate_root",
            Self::AcquireRoot => "acquire_root",
            Self::RecoverRoot => "recover_root",
            Self::BeginRootRevocation => "begin_root_revocation",
            Self::CommitRootRevocation => "commit_root_revocation",
            Self::AckRootCommand => "ack_root_command",
            Self::DfsCreate => "dfs_create",
            Self::DfsMkdir => "dfs_mkdir",
            Self::DfsMknod => "dfs_mknod",
            Self::DfsInitializeNamespace => "dfs_initialize_namespace",
            Self::DfsUnlink => "dfs_unlink",
            Self::DfsRmdir => "dfs_rmdir",
            Self::DfsRename => "dfs_rename",
            Self::DfsLink => "dfs_link",
            Self::DfsSymlink => "dfs_symlink",
            Self::DfsSetInodeAttributes => "dfs_set_inode_attributes",
            Self::DfsSetXattr => "dfs_set_xattr",
            Self::DfsRemoveXattr => "dfs_remove_xattr",
            Self::DfsAcquireWriteLease => "dfs_acquire_write_lease",
            Self::DfsRenewWriteLease => "dfs_renew_write_lease",
            Self::DfsSyncInodeMetadata => "dfs_sync_inode_metadata",
            Self::DfsCommitFileVersion => "dfs_commit_file_version",
            Self::DfsUpdateReference => "dfs_update_reference",
            Self::DfsCollectGarbage => "dfs_collect_garbage",
            Self::DfsInitializeReplicationConfig => "dfs_initialize_replication_config",
            Self::DfsInitializeDurabilityPolicy => "dfs_initialize_durability_policy",
            Self::DfsClaimReplicationTask => "dfs_claim_replication_task",
            Self::DfsReportReplicationTask => "dfs_report_replication_task",
            Self::DfsReportChunkCorruption => "dfs_report_chunk_corruption",
        }
    }
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
        for mutation in &self.mutations {
            match mutation {
                TxnMutation::Delete(MetaKey::RootEpochWatermark) => {
                    return Err(invalid_contract("root epoch watermark cannot be deleted"));
                }
                TxnMutation::Put(MetaEntity::RootEpochWatermark(value)) => {
                    let allocated = self.conditions.iter().any(|condition| {
                        matches!(condition,
                        TxnCondition::RootEpochWatermarkEquals(previous) if value > previous)
                    });
                    let reserved = self.mutations.iter().any(|mutation| matches!(mutation,
                        TxnMutation::Put(MetaEntity::RootReservation(root)) if root.root_epoch == *value));
                    let bootstrap = self.operation == StoreOperation::CleanupRootGrants
                        && self.conditions.iter().any(|condition| {
                            matches!(condition, TxnCondition::Missing(MetaKey::RootEpochWatermark))
                        })
                        && self.conditions.iter().any(|condition| {
                            matches!(condition, TxnCondition::RootEpochWatermarkEquals(previous) if value == previous)
                        });
                    if !bootstrap
                        && (self.operation != StoreOperation::ReserveRoot
                            || !allocated
                            || !reserved)
                    {
                        return Err(invalid_contract(
                            "root epoch watermark requires an atomic reservation or initial capture",
                        ));
                    }
                }
                _ => {}
            }
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

// Response limits are independent of retained-history limits. Zero requested
// limit never disables these hard bounds; one revision is never split.
pub(crate) const WATCH_RESPONSE_MAX_EVENTS: usize = 16_384;
pub(crate) const WATCH_RESPONSE_MAX_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const WATCH_RESPONSE_BASE_BYTES: u64 = 64 * 1024;

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
/// Constructs the stable identity reused by filesystem mutation retries.
pub fn timed_request_id(id: impl std::fmt::Display) -> String {
    format!("afs1-{}-{id}", now_unix_ms())
}

/// Boxed only on the coarse Meta control path. Durable logs preserve results
/// across restart; memory is explicitly volatile.
pub type MetaFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Clone, Copy)]
struct ReadLimits {
    views: usize,
    held_bytes: u64,
    lifetime: Duration,
    reap: Duration,
}
impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            views: 128,
            held_bytes: 8 * 1024 * 1024 * 1024,
            lifetime: Duration::from_secs(30),
            reap: Duration::from_millis(100),
        }
    }
}
#[cfg(test)]
struct ReadBudgetStats {
    views: usize,
    bytes: u64,
}

const VIEW_CHARGE_UNIT: u64 = 4096;
impl ReadLimits {
    fn validate(self) -> Result<Self> {
        let units = self.held_bytes / VIEW_CHARGE_UNIT;
        if !(1..=4096).contains(&self.views)
            || units == 0
            || units > u64::from(u32::MAX)
            || units > Semaphore::MAX_PERMITS as u64
            || self.lifetime.is_zero()
            || self.lifetime > Duration::from_secs(60)
            || self.reap.is_zero()
            || self.reap > self.lifetime
        {
            return Err(invalid_contract("invalid finite Meta read-view limits"));
        }
        Ok(self)
    }
}
struct ReadAllocation {
    state: Arc<State>,
    _bytes: OwnedSemaphorePermit,
}
struct ReadHold {
    allocation: Arc<ReadAllocation>,
    _slot: OwnedSemaphorePermit,
    // Keeps the single reaper alive while an idle or executing view owns data.
    owner: Arc<ReadBudget>,
}
struct ReadPin {
    held: Mutex<Option<Arc<ReadHold>>>,
    deadline: tokio::time::Instant,
    visible: Weak<Visible>,
}
impl ReadPin {
    fn check(&self) -> Result<()> {
        if tokio::time::Instant::now() >= self.deadline {
            return Err(unavailable("Meta read view expired; acquire a fresh view"));
        }
        let visible = self
            .visible
            .upgrade()
            .ok_or_else(|| unavailable("Meta read authority retired"))?;
        if visible.poisoned.load(Ordering::Acquire) {
            return Err(unavailable("Meta read authority failed; restart to reload"));
        }
        Ok(())
    }
    fn capture(&self) -> Result<Arc<ReadHold>> {
        self.check()?;
        let held = self
            .held
            .lock()
            .map_err(|_| unavailable("Meta read pin lock poisoned"))?
            .as_ref()
            .cloned()
            .ok_or_else(|| unavailable("Meta read view was released"))?;
        if held.owner.bytes.is_closed() {
            return Err(unavailable("Meta read budget failed"));
        }
        self.check()?;
        Ok(held)
    }
    fn expire(&self, now: tokio::time::Instant) -> Result<()> {
        if now < self.deadline {
            return Ok(());
        }
        // Destruction may release a large old tree. It happens outside both
        // the admission registry and pin locks; an executing read still owns
        // its Arc<ReadHold> and therefore retains both permits until it ends.
        let retired = {
            self.held
                .lock()
                .map_err(|_| unavailable("Meta read pin lock poisoned"))?
                .take()
        };
        drop(retired);
        Ok(())
    }
}
#[derive(Default)]
struct ReadRegistry {
    views: Vec<Weak<ReadPin>>,
    allocations: std::collections::BTreeMap<usize, Weak<ReadAllocation>>,
}
struct ReadBudget {
    limits: ReadLimits,
    bytes: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    registry: Mutex<ReadRegistry>,
}
impl ReadBudget {
    fn new(limits: ReadLimits) -> Result<Arc<Self>> {
        let limits = limits.validate()?;
        let units = usize::try_from(limits.held_bytes / VIEW_CHARGE_UNIT)
            .map_err(|_| invalid_contract("Meta read byte units overflow"))?;
        let budget = Arc::new(Self {
            limits,
            bytes: Arc::new(Semaphore::new(units)),
            slots: Arc::new(Semaphore::new(limits.views)),
            registry: Mutex::new(ReadRegistry::default()),
        });
        let weak = Arc::downgrade(&budget);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(limits.reap).await;
                let Some(budget) = weak.upgrade() else {
                    break;
                };
                if let Err(error) = budget.reap() {
                    budget.bytes.close();
                    budget.slots.close();
                    tracing::error!(%error, "Meta read budget reaper failed closed");
                    break;
                }
            }
        });
        Ok(budget)
    }
    fn reap(&self) -> Result<()> {
        let pins = {
            let registry = self
                .registry
                .lock()
                .map_err(|_| unavailable("Meta read registry lock poisoned"))?;
            registry
                .views
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        let now = tokio::time::Instant::now();
        for pin in pins {
            pin.expire(now)?;
        }
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| unavailable("Meta read registry lock poisoned"))?;
        // Expired handles may remain owned by a caller; they no longer retain
        // a State and must not accumulate weak entries in this registry.
        registry
            .views
            .retain(|weak| weak.upgrade().is_some_and(|pin| pin.deadline > now));
        registry
            .allocations
            .retain(|_, allocation| allocation.strong_count() > 0);
        Ok(())
    }
    fn pin(self: &Arc<Self>, state: Arc<State>, visible: &Arc<Visible>) -> Result<MetaReadView> {
        self.reap()?;
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| journal::full("Meta read view count exhausted"))?;
        let charge = state.pin_charge()?;
        let units = u32::try_from(charge.div_ceil(VIEW_CHARGE_UNIT))
            .map_err(|_| journal::full("Meta pinned state charge overflow"))?;
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| unavailable("Meta read registry lock poisoned"))?;
        // Another thread can finish its reap before a just-created view is
        // registered/dropped. Recheck weak-entry bounds under this same lock;
        // slot permits alone must not allow dead registry entries to grow.
        registry.views.retain(|pin| pin.strong_count() > 0);
        registry
            .allocations
            .retain(|_, allocation| allocation.strong_count() > 0);
        if registry.views.len() >= self.limits.views {
            return Err(journal::full("Meta read registry count exhausted"));
        }
        let key = Arc::as_ptr(&state) as usize;
        let allocation =
            if let Some(allocation) = registry.allocations.get(&key).and_then(Weak::upgrade) {
                if !Arc::ptr_eq(&allocation.state, &state) {
                    return Err(invalid_contract("Meta pinned state identity changed"));
                }
                allocation
            } else {
                if registry.allocations.len() >= self.limits.views {
                    return Err(journal::full("Meta read allocation registry exhausted"));
                }
                let permit = self
                    .bytes
                    .clone()
                    .try_acquire_many_owned(units)
                    .map_err(|_| journal::full("Meta pinned state byte capacity exhausted"))?;
                let allocation = Arc::new(ReadAllocation {
                    state,
                    _bytes: permit,
                });
                registry
                    .allocations
                    .insert(key, Arc::downgrade(&allocation));
                allocation
            };
        let deadline = tokio::time::Instant::now()
            .checked_add(self.limits.lifetime)
            .ok_or_else(|| invalid_contract("Meta read deadline overflow"))?;
        let pin = Arc::new(ReadPin {
            held: Mutex::new(Some(Arc::new(ReadHold {
                allocation,
                _slot: slot,
                owner: self.clone(),
            }))),
            deadline,
            visible: Arc::downgrade(visible),
        });
        registry.views.push(Arc::downgrade(&pin));
        Ok(MetaReadView {
            state: ReadState::Pinned(pin),
        })
    }
    #[cfg(test)]
    fn stats(&self) -> ReadBudgetStats {
        ReadBudgetStats {
            views: self.limits.views - self.slots.available_permits(),
            bytes: (self.limits.held_bytes / VIEW_CHARGE_UNIT
                - self.bytes.available_permits() as u64)
                * VIEW_CHARGE_UNIT,
        }
    }
}
enum ReadState {
    Pinned(Arc<ReadPin>),
    // Test-only reference state machine; never an unbounded production view.
    #[cfg(test)]
    Unmanaged(Arc<State>),
}
/// Immutable compound read of one acknowledged authority revision.
/// Canonical views have finite admission and lifetime; callers must acquire a
/// fresh view after expiry or a known authority failure. They cannot mutate it.
pub struct MetaReadView {
    state: ReadState,
}
impl MetaReadView {
    #[cfg(test)]
    fn unmanaged(state: Arc<State>) -> Self {
        Self {
            state: ReadState::Unmanaged(state),
        }
    }
    #[cfg(test)]
    fn state_for_test(&self) -> Arc<State> {
        match &self.state {
            ReadState::Unmanaged(state) => state.clone(),
            ReadState::Pinned(pin) => pin
                .held
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .allocation
                .state
                .clone(),
        }
    }
    fn with_state<T>(&self, read: impl FnOnce(&State) -> Result<T>) -> Result<T> {
        match &self.state {
            #[cfg(test)]
            ReadState::Unmanaged(state) => read(state),
            ReadState::Pinned(pin) => {
                let held = pin.capture()?;
                let result = read(&held.allocation.state);
                pin.check()?;
                if held.owner.bytes.is_closed() {
                    return Err(unavailable("Meta read budget failed"));
                }
                result
            }
        }
    }

    /// Reads one key from this pinned revision for related-key CAS.
    pub(crate) fn lookup(&self, key: &MetaKey) -> Result<MetaSnapshot> {
        self.with_state(|state| {
            let value = state.entities.get(key);
            Ok(MetaSnapshot {
                revision: value.map_or(state.revision, |entry| entry.revision),
                entity: value.map(|entry| entry.entity.clone()),
                request_outcome: None,
                entities: Vec::new(),
            })
        })
    }

    pub(crate) fn snapshot(&self) -> Result<(StoreRevision, Vec<MetaEntity>)> {
        self.with_state(|state| {
            Ok((
                state.revision,
                state
                    .entities
                    .values()
                    .map(|entry| entry.entity.clone())
                    .collect(),
            ))
        })
    }

    pub(crate) fn statistics(&self) -> Result<serde_json::Value> {
        self.with_state(|state| {
            let mut kinds = std::collections::BTreeMap::new();
            for entry in state.entities.values() {
                *kinds.entry(entry.entity.name()).or_insert(0_u64) += 1;
            }
            Ok(
                serde_json::json!({"revision": state.revision.0, "entities": kinds,
                "requests": state.requests.len(), "events": state.events.len(),
                "watch_floor": state.compacted_to.0}),
            )
        })
    }

    pub async fn read(&self, read: MetaRead) -> Result<MetaSnapshot> {
        self.with_state(|state| state.read(read))
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

/// Process-local, fixed-cardinality observation only. Collectors retain atomic
/// handles, never a State/Store/backend, and do no I/O or authority scans.
struct StoreMetrics {
    requests: afs_metrics::IntGauge,
    request_bytes: afs_metrics::IntGauge,
    events: afs_metrics::IntGauge,
    event_bytes: afs_metrics::IntGauge,
    history_index_bytes: afs_metrics::IntGauge,
    revision: afs_metrics::IntGauge,
    log_sequence: afs_metrics::IntGauge,
    compacted_revision: afs_metrics::IntGauge,
    checkpoint_inflight: afs_metrics::IntGauge,
    checkpoint_bytes: afs_metrics::IntGauge,
    checkpoint_sequence: afs_metrics::IntGauge,
    request_evictions: afs_metrics::IntCounter,
    event_evictions: afs_metrics::IntCounter,
    wal_batches: afs_metrics::IntCounter,
    wal_records: afs_metrics::IntCounter,
    wal_framed_bytes: afs_metrics::IntCounter,
    wal_failures: afs_metrics::IntCounter,
    wal_capacity_rejections: afs_metrics::IntCounter,
    checkpoints: afs_metrics::IntCounter,
    checkpoint_failures: afs_metrics::IntCounter,
    checkpoint_duration: afs_metrics::Histogram,
}
impl StoreMetrics {
    fn new() -> std::result::Result<Self, afs_metrics::MetricsError> {
        Ok(Self {
            requests: afs_metrics::IntGauge::new(
                "afs_meta_requests_retained",
                "Current retained deduplication outcomes",
            )?,
            request_bytes: afs_metrics::IntGauge::new(
                "afs_meta_request_bytes",
                "Logical retained outcome capacities; not RSS",
            )?,
            events: afs_metrics::IntGauge::new(
                "afs_meta_events_retained",
                "Current retained watch events",
            )?,
            event_bytes: afs_metrics::IntGauge::new(
                "afs_meta_event_bytes",
                "Logical retained watch event capacities; not RSS",
            )?,
            history_index_bytes: afs_metrics::IntGauge::new(
                "afs_meta_history_index_bytes",
                "Fixed logical history index reserve; not RSS",
            )?,
            revision: afs_metrics::IntGauge::new(
                "afs_meta_revision",
                "Latest published business revision",
            )?,
            log_sequence: afs_metrics::IntGauge::new(
                "afs_meta_log_sequence",
                "Latest published canonical record sequence",
            )?,
            compacted_revision: afs_metrics::IntGauge::new(
                "afs_meta_compacted_revision",
                "Published watch compaction cursor",
            )?,
            checkpoint_inflight: afs_metrics::IntGauge::new(
                "afs_meta_checkpoint_inflight",
                "One while the owned checkpoint task is active",
            )?,
            checkpoint_bytes: afs_metrics::IntGauge::new(
                "afs_meta_checkpoint_bytes",
                "Latest recovered or successfully installed checkpoint payload bytes",
            )?,
            checkpoint_sequence: afs_metrics::IntGauge::new(
                "afs_meta_checkpoint_sequence",
                "Latest recovered or successfully installed checkpoint record sequence",
            )?,
            request_evictions: afs_metrics::IntCounter::new(
                "afs_meta_request_evictions_total",
                "Exact outcome removals acknowledged in this process lifetime",
            )?,
            event_evictions: afs_metrics::IntCounter::new(
                "afs_meta_watch_events_evicted_total",
                "Exact watch removals acknowledged in this process lifetime",
            )?,
            wal_batches: afs_metrics::IntCounter::new(
                "afs_meta_wal_batches_total",
                "WAL batches with a successful durable ACK in this process lifetime",
            )?,
            wal_records: afs_metrics::IntCounter::new(
                "afs_meta_wal_records_total",
                "Records in acknowledged WAL batches in this process lifetime",
            )?,
            wal_framed_bytes: afs_metrics::IntCounter::new(
                "afs_meta_wal_framed_bytes_total",
                "Framed bytes in acknowledged appends; not retained disk usage",
            )?,
            wal_failures: afs_metrics::IntCounter::new(
                "afs_meta_wal_failures_total",
                "Append ACK failures; persistence outcome may be unknown",
            )?,
            wal_capacity_rejections: afs_metrics::IntCounter::new(
                "afs_meta_wal_capacity_rejections_total",
                "Capacity waits that expired before any append",
            )?,
            checkpoints: afs_metrics::IntCounter::new(
                "afs_meta_checkpoints_total",
                "Owned checkpoint installations completed successfully",
            )?,
            checkpoint_failures: afs_metrics::IntCounter::new(
                "afs_meta_checkpoint_failures_total",
                "Owned checkpoint tasks ended unsuccessfully; observer timeout is not failure",
            )?,
            checkpoint_duration: afs_metrics::Histogram::with_opts(
                afs_metrics::HistogramOpts::new(
                    "afs_meta_checkpoint_duration_seconds",
                    "Actual owned task duration, including capture and install",
                ),
            )?,
        })
    }
    fn register(
        &self,
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<(), afs_metrics::MetricsError> {
        afs_metrics::register_collector(registry, &self.requests)?;
        afs_metrics::register_collector(registry, &self.request_bytes)?;
        afs_metrics::register_collector(registry, &self.events)?;
        afs_metrics::register_collector(registry, &self.event_bytes)?;
        afs_metrics::register_collector(registry, &self.history_index_bytes)?;
        afs_metrics::register_collector(registry, &self.revision)?;
        afs_metrics::register_collector(registry, &self.log_sequence)?;
        afs_metrics::register_collector(registry, &self.compacted_revision)?;
        afs_metrics::register_collector(registry, &self.checkpoint_inflight)?;
        afs_metrics::register_collector(registry, &self.checkpoint_bytes)?;
        afs_metrics::register_collector(registry, &self.checkpoint_sequence)?;
        afs_metrics::register_collector(registry, &self.request_evictions)?;
        afs_metrics::register_collector(registry, &self.event_evictions)?;
        afs_metrics::register_collector(registry, &self.wal_batches)?;
        afs_metrics::register_collector(registry, &self.wal_records)?;
        afs_metrics::register_collector(registry, &self.wal_framed_bytes)?;
        afs_metrics::register_collector(registry, &self.wal_failures)?;
        afs_metrics::register_collector(registry, &self.wal_capacity_rejections)?;
        afs_metrics::register_collector(registry, &self.checkpoints)?;
        afs_metrics::register_collector(registry, &self.checkpoint_failures)?;
        afs_metrics::register_collector(registry, &self.checkpoint_duration)?;
        Ok(())
    }
    fn set(gauge: &afs_metrics::IntGauge, value: u64) {
        // Diagnostic-only saturation never changes an already durable outcome.
        gauge.set(i64::try_from(value).unwrap_or(i64::MAX));
    }
    fn publish(&self, state: &State) {
        let retained = state.history_stats();
        Self::set(&self.requests, retained.requests as u64);
        Self::set(&self.request_bytes, retained.request_bytes);
        Self::set(&self.events, retained.events as u64);
        Self::set(&self.event_bytes, retained.event_bytes);
        Self::set(&self.history_index_bytes, retained.index_base_bytes);
        Self::set(&self.revision, state.revision.0);
        Self::set(&self.log_sequence, state.log_tip.sequence);
        Self::set(&self.compacted_revision, state.compacted_to.0);
    }
}

struct Visible {
    metrics: StoreMetrics,
    committed: ArcSwap<State>,
    checkpoint_active: AtomicBool,
    checkpoint_wakeup: Arc<Notify>,
    checkpoint_finished: Arc<Notify>,
    /// Any failed durable write may mean another authority has taken over, or
    /// that the local disk is damaged. Refuse even reads until a fresh process
    /// reloads the backend; stale grants must never be served as current.
    poisoned: AtomicBool,
}

struct CheckpointGuard {
    visible: Arc<Visible>,
    started: Instant,
    completed: bool,
}
impl Drop for CheckpointGuard {
    fn drop(&mut self) {
        let metrics = &self.visible.metrics;
        metrics
            .checkpoint_duration
            .observe(self.started.elapsed().as_secs_f64());
        if !self.completed {
            metrics.checkpoint_failures.inc();
        }
        metrics.checkpoint_inflight.set(0);
        self.visible
            .checkpoint_active
            .store(false, Ordering::Release);
        self.visible.checkpoint_finished.notify_waiters();
    }
}

enum Mutation {
    Transaction(MetaTxn),
    RegisterNode(RequestKey, NodeSessionLease),
}

const PENDING_INPUT_BYTES: usize = 128 * 1024 * 1024;
const PROTECTED_KEY_BYTES: u64 = 4 * 1024 * 1024;
impl Mutation {
    fn capacity_charge(&self) -> Result<u64> {
        match self {
            Self::Transaction(txn) => history::transaction_charge(txn),
            Self::RegisterNode(key, lease) => history::registration_charge(key, lease),
        }
    }
    fn protect(
        &self,
        keys: &mut std::collections::BTreeSet<RequestKey>,
        bytes: &mut u64,
        limit: usize,
    ) -> Result<()> {
        let mut add = |key: &RequestKey| -> Result<()> {
            if keys.contains(key) {
                return Ok(());
            }
            let next = bytes
                .checked_add(history::protection_charge(key)?)
                .ok_or_else(|| unavailable("protected request bytes overflow"))?;
            if keys.len() == limit || next > PROTECTED_KEY_BYTES {
                return Err(unavailable(
                    "prepared request protection capacity exhausted",
                ));
            }
            keys.insert(key.clone());
            *bytes = next;
            Ok(())
        };
        match self {
            Self::RegisterNode(key, _) => add(key),
            Self::Transaction(txn) => {
                add(&txn.request)?;
                for mutation in &txn.mutations {
                    if let TxnMutation::RecordRequestOutcome(outcome) = mutation {
                        add(&outcome.request)?;
                    }
                }
                Ok(())
            }
        }
    }
    fn operation_name(&self) -> &'static str {
        match self {
            Self::Transaction(txn) => txn.operation.as_str(),
            Self::RegisterNode(_, _) => StoreOperation::RegisterNode.as_str(),
        }
    }
}

/// Admission identities survive caller cancellation until the writer settles.
/// The same short mutex serializes admission and retirement selection; it is
/// never held across backend I/O. All copies use the existing protection budget.
#[derive(Default)]
struct InFlightState {
    active: std::collections::BTreeMap<Arc<RequestKey>, usize>,
    retiring: std::collections::BTreeSet<Arc<RequestKey>>,
    bytes: u64,
    failed: bool,
}
struct InFlight {
    state: Mutex<InFlightState>,
    limit: usize,
}
struct InFlightGuard {
    owner: Arc<InFlight>,
    keys: Vec<Arc<RequestKey>>,
    retirement: bool,
    holder_bytes: u64,
}
impl InFlight {
    fn charge(key: &RequestKey) -> Result<u64> {
        // Registry, queued holder, selection snapshot and retirement holder.
        history::protection_charge(key)?
            .checked_mul(4)
            .ok_or_else(|| unavailable("in-flight identity charge overflow"))
    }
    fn admit(self: &Arc<Self>, operation: &Mutation) -> Result<InFlightGuard> {
        let mut keys = std::collections::BTreeSet::new();
        operation.protect(&mut keys, &mut 0, self.limit)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("in-flight identity lock poisoned"))?;
        let holder_bytes = 128u64 + keys.len() as u64 * 32;
        if state.failed {
            return Err(unavailable("in-flight identity invariant failed"));
        }
        let mut extra = holder_bytes;
        let mut count = 0;
        for key in &keys {
            if state.retiring.contains(key) {
                return Err(unavailable(
                    "request outcome retirement in progress; retry admission",
                ));
            }
            if !state.active.contains_key(key) {
                count += 1;
                extra += Self::charge(key)?;
            }
        }
        if state.active.len() + state.retiring.len() + count > self.limit
            || state
                .bytes
                .checked_add(extra)
                .is_none_or(|n| n > PROTECTED_KEY_BYTES)
        {
            return Err(unavailable(
                "in-flight request protection capacity exhausted",
            ));
        }
        state.bytes += extra;
        let held = keys
            .into_iter()
            .map(|key| {
                let key = state
                    .active
                    .get_key_value(&key)
                    .map(|(key, _)| Arc::clone(key))
                    .unwrap_or_else(|| Arc::new(key));
                *state.active.entry(Arc::clone(&key)).or_default() += 1;
                key
            })
            .collect();
        Ok(InFlightGuard {
            owner: Arc::clone(self),
            keys: held,
            retirement: false,
            holder_bytes,
        })
    }
    fn prepare_history(
        self: &Arc<Self>,
        current: &State,
        now: u64,
        mut limits: history::Limits,
    ) -> Result<Option<(State, history::Delta, InFlightGuard)>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("in-flight identity lock poisoned"))?;
        if state.failed {
            return Err(unavailable("in-flight identity invariant failed"));
        }
        let protected = state
            .active
            .keys()
            .chain(state.retiring.iter())
            .map(|key| key.as_ref().clone())
            .collect();
        let slots = self
            .limit
            .saturating_sub(state.active.len() + state.retiring.len());
        if slots == 0 {
            return Ok(None);
        }
        limits.cleanup_items = limits.cleanup_items.min(slots);
        loop {
            let Some((next, delta)) = current.prepare_history(now, limits, &protected)? else {
                return Ok(None);
            };
            let extra = delta.requests.iter().try_fold(0u64, |sum, entry| {
                sum.checked_add(Self::charge(&entry.key)?)
                    .ok_or_else(|| unavailable("retirement identity charge overflow"))
            })?;
            if state
                .bytes
                .checked_add(extra)
                .is_some_and(|n| n <= PROTECTED_KEY_BYTES)
            {
                let keys: Vec<_> = delta
                    .requests
                    .iter()
                    .map(|entry| Arc::new(entry.key.clone()))
                    .collect();
                state.retiring.extend(keys.iter().cloned());
                state.bytes += extra;
                return Ok(Some((
                    next,
                    delta,
                    InFlightGuard {
                        owner: Arc::clone(self),
                        keys,
                        retirement: true,
                        holder_bytes: 0,
                    },
                )));
            }
            if limits.cleanup_items <= 1 {
                return Ok(None);
            }
            limits.cleanup_items /= 2;
        }
    }
}
impl Drop for InFlightGuard {
    fn drop(&mut self) {
        // Poisoning is fail-closed at the next admission/cleanup; never panic
        // while dropping an already-settled caller's protection.
        if let Ok(mut state) = self.owner.state.lock() {
            if let Some(bytes) = state.bytes.checked_sub(self.holder_bytes) {
                state.bytes = bytes;
            } else {
                state.failed = true;
            }
            for key in &self.keys {
                let removed = if self.retirement {
                    state.retiring.remove(key)
                } else if let Some(count) = state.active.get_mut(key) {
                    *count -= 1;
                    if *count == 0 {
                        state.active.remove(key);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                if removed {
                    match InFlight::charge(key)
                        .ok()
                        .and_then(|charge| state.bytes.checked_sub(charge))
                    {
                        Some(bytes) => state.bytes = bytes,
                        None => state.failed = true,
                    }
                }
            }
        }
    }
}

struct Pending {
    identity: InFlightGuard,
    capacity: Option<OwnedSemaphorePermit>,
    operation: Mutation,
    reply: oneshot::Sender<Result<TxnOutcome>>,
}

pub struct Store {
    in_flight: Arc<InFlight>,
    read_budget: Arc<ReadBudget>,
    pending_bytes: Arc<Semaphore>,
    visible: Arc<Visible>,
    writes: mpsc::Sender<Pending>,
    backend: Arc<dyn journal::MetaLog>,
    next_request_id: AtomicU64,
}

impl Store {
    #[cfg(test)]
    async fn open_log_with_read_limits(
        log: Arc<dyn journal::MetaLog>,
        limits: ReadLimits,
    ) -> Result<Self> {
        Self::open_log_with_policies(log, history::Limits::default(), limits).await
    }
    #[cfg(test)]
    fn read_budget_stats(&self) -> ReadBudgetStats {
        self.read_budget.stats()
    }
    /// Replays already-decided records before serving. Does not re-execute
    /// client requests, conditions, clocks, Node IO, or external storage IO.
    pub async fn open_log(log: Arc<dyn journal::MetaLog>) -> Result<Self> {
        Self::open_log_with_history(log, history::Limits::default()).await
    }
    pub(crate) async fn open_log_with_history(
        log: Arc<dyn journal::MetaLog>,
        history: history::Limits,
    ) -> Result<Self> {
        Self::open_log_with_policies(log, history, ReadLimits::default()).await
    }
    async fn open_log_with_policies(
        log: Arc<dyn journal::MetaLog>,
        history: history::Limits,
        reads: ReadLimits,
    ) -> Result<Self> {
        let history = history.validate()?;
        let reads = reads.validate()?;
        log.limits().validate()?;
        let maintenance = log
            .maintenance_policy()
            .map(journal::MaintenancePolicy::validate)
            .transpose()?;
        let recovered = log.recover().await?;
        log.checkpoint_limits().validate()?;
        let metrics = StoreMetrics::new()
            .map_err(|error| unavailable(format!("Meta metrics initialization: {error}")))?;
        if let Some(image) = &recovered.checkpoint {
            StoreMetrics::set(&metrics.checkpoint_bytes, image.bytes.len() as u64);
            StoreMetrics::set(&metrics.checkpoint_sequence, image.covered.sequence);
        }
        let mut committed = match recovered.checkpoint {
            Some(image) => {
                if image.covered.sequence > recovered.tip.sequence {
                    return Err(journal::invalid_log("checkpoint exceeds recovered log tip"));
                }
                log::StateImage::decode(&image.bytes, log.checkpoint_limits().image_max_bytes)?
            }
            None => State::default(),
        };
        for record in recovered.records {
            committed = committed.replay_record(&record)?;
        }
        committed.ensure_history_index()?;
        let mut recovered_tip = recovered.tip;
        // A lowered history budget cannot expose an over-budget authority.
        // Persist exact startup removals before service admission; failure does
        // not serve a partially prepared state or erase the original chain.
        while !committed.history_fits(history) {
            let Some((next, delta)) = committed.prepare_history(
                now_unix_ms(),
                history,
                &std::collections::BTreeSet::new(),
            )?
            else {
                return Err(journal::full(
                    "recovered history cannot fit configured limits",
                ));
            };
            let removed_requests = delta.requests.len() as u64;
            let removed_events = next.events_removed.saturating_sub(committed.events_removed);
            let record = log::Record::History(delta);
            let size = journal::frame_size(recovered_tip, &record)?;
            if size > log.limits().record_max_bytes || !log.can_append(recovered_tip, size).await? {
                return Err(journal::full(
                    "startup retention has no bounded WAL capacity",
                ));
            }
            recovered_tip = log.append_and_sync(recovered_tip, vec![record]).await?;
            metrics.wal_batches.inc();
            metrics.wal_records.inc();
            metrics.wal_framed_bytes.inc_by(size as u64);
            metrics.request_evictions.inc_by(removed_requests);
            metrics.event_evictions.inc_by(removed_events);
            committed = next;
        }
        committed.log_tip = recovered_tip;
        metrics.publish(&committed);
        let visible = Arc::new(Visible {
            metrics,
            committed: ArcSwap::from_pointee(committed),
            checkpoint_active: AtomicBool::new(false),
            checkpoint_wakeup: Arc::new(Notify::new()),
            checkpoint_finished: Arc::new(Notify::new()),
            poisoned: AtomicBool::new(false),
        });
        if let Some(policy) = maintenance {
            tokio::spawn(run_checkpoint_loop(
                Arc::downgrade(&visible),
                Arc::downgrade(&log),
                Arc::clone(&visible.checkpoint_wakeup),
                policy,
            ));
        }
        let in_flight = Arc::new(InFlight {
            state: Mutex::new(InFlightState::default()),
            limit: history.protected_requests,
        });
        let (writes, receiver) = mpsc::channel(1024);
        tokio::spawn(run_log_commit_loop(
            receiver,
            Arc::clone(&visible),
            Arc::clone(&log),
            recovered_tip,
            history,
            Arc::clone(&in_flight),
        ));
        Ok(Self {
            in_flight,
            read_budget: ReadBudget::new(reads)?,
            pending_bytes: Arc::new(Semaphore::new(PENDING_INPUT_BYTES)),
            visible,
            writes,
            backend: log,
            next_request_id: AtomicU64::new(1),
        })
    }

    /// Register this authority's fixed collectors with its process Registry.
    /// Call once before listener admission. Duplicate registration returns an
    /// error (possibly after partial registration) and never alters authority.
    pub fn register_metrics(
        &self,
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<(), afs_metrics::MetricsError> {
        self.visible.metrics.register(registry)
    }

    /// Persist one already-published state without creating a business revision.
    /// Returns an error when the selected backend cannot install checkpoints.
    pub async fn checkpoint(&self) -> Result<journal::LogTip> {
        self.ensure_available()?;
        checkpoint_visible(Arc::clone(&self.visible), Arc::clone(&self.backend)).await
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

    async fn committed(&self) -> Result<Arc<State>> {
        self.ensure_available()?;
        let state = self.visible.committed.load_full();
        self.ensure_available()?;
        Ok(state)
    }

    async fn mutate(&self, operation: Mutation) -> Result<TxnOutcome> {
        let store_request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let operation_name = operation.operation_name();
        let span = tracing::info_span!(
            "meta.store.mutate",
            store.request_id = store_request_id,
            store.operation = operation_name,
            store.batch_id = tracing::field::Empty,
            result = tracing::field::Empty,
        );
        async move {
            self.ensure_available()?;
            let bytes = u32::try_from(operation.capacity_charge()?)
                .map_err(|_| journal::full("queued Meta input charge overflow"))?;
            let capacity = Some(
                Arc::clone(&self.pending_bytes)
                    .try_acquire_many_owned(bytes)
                    .map_err(|_| journal::full("queued Meta input byte capacity exhausted"))?,
            );
            let identity = self.in_flight.admit(&operation)?;
            let (reply, result) = oneshot::channel();
            self.writes
                .send(Pending {
                    identity,
                    capacity,
                    operation,
                    reply,
                })
                .instrument(tracing::info_span!(
                    "meta.store.enqueue",
                    store.request_id = store_request_id,
                    store.operation = operation_name,
                    result = tracing::field::Empty,
                ))
                .await
                .map_err(|_| unavailable("store commit loop stopped"))?;
            let outcome = result
                .instrument(tracing::info_span!(
                    "meta.store.commit_wait",
                    store.request_id = store_request_id,
                    store.operation = operation_name,
                    result = tracing::field::Empty,
                ))
                .await
                .map_err(|_| unavailable("store commit loop dropped reply"))?;
            tracing::Span::current().record("result", tracing_result(outcome.as_ref()));
            outcome
        }
        .instrument(span)
        .await
    }
}

async fn checkpoint_visible(
    visible: Arc<Visible>,
    log: Arc<dyn journal::MetaLog>,
) -> Result<journal::LogTip> {
    let limits = log.checkpoint_limits().validate()?;
    if visible
        .checkpoint_active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Err(unavailable("checkpoint already in progress"));
    }
    visible.metrics.checkpoint_inflight.set(1);
    let guard = CheckpointGuard {
        visible: Arc::clone(&visible),
        started: Instant::now(),
        completed: false,
    };
    // Dropping the caller/observer never aborts this task or releases its gate.
    tokio::spawn(async move {
        let mut guard = guard;
        if visible.poisoned.load(Ordering::Acquire) {
            return Err(unavailable("store unavailable before checkpoint capture"));
        }
        let state = visible.committed.load_full();
        let covered = state.log_tip;
        let bytes = tokio::task::spawn_blocking(move || {
            if log::StateImage::capture_charge(&state)? > limits.capture_charge_max_bytes {
                return Err(journal::full("checkpoint capture logical charge exceeded"));
            }
            log::StateImage::encode(&state, limits.image_max_bytes)
        })
        .await
        .map_err(|error| unavailable(format!("checkpoint encoder task: {error}")))??;
        let payload_bytes = bytes.len() as u64;
        log.install_checkpoint(covered, bytes).await?;
        StoreMetrics::set(&visible.metrics.checkpoint_bytes, payload_bytes);
        StoreMetrics::set(&visible.metrics.checkpoint_sequence, covered.sequence);
        visible.metrics.checkpoints.inc();
        guard.completed = true;
        Ok(covered)
    })
    .await
    .map_err(|error| unavailable(format!("checkpoint task: {error}")))?
}
async fn run_checkpoint_loop(
    visible: std::sync::Weak<Visible>,
    log: std::sync::Weak<dyn journal::MetaLog>,
    wakeup: Arc<Notify>,
    policy: journal::MaintenancePolicy,
) {
    let mut last_success = tokio::time::Instant::now();
    let mut last_attempt = None::<tokio::time::Instant>;
    loop {
        let forced =
            tokio::select! {_=wakeup.notified()=>true,_=tokio::time::sleep(policy.poll)=>false};
        let (Some(visible), Some(log)) = (visible.upgrade(), log.upgrade()) else {
            break;
        };
        if visible.poisoned.load(Ordering::Acquire) {
            break;
        }
        if visible.checkpoint_active.load(Ordering::Acquire)
            || last_attempt.is_some_and(|last| last.elapsed() < policy.poll)
        {
            continue;
        }
        let status = match log.status().await {
            Ok(status) => status,
            Err(error) => {
                last_attempt = Some(tokio::time::Instant::now());
                tracing::warn!(error=%error,"Meta checkpoint status failed");
                continue;
            }
        };
        let Some(records) = status.tip.sequence.checked_sub(status.checkpoint.sequence) else {
            tracing::warn!("Meta checkpoint position exceeds WAL tip");
            continue;
        };
        if !forced
            && (records == 0
                || (records < policy.wal_records
                    && status.pending_bytes_upper_bound < policy.wal_bytes
                    && last_success.elapsed() < policy.interval))
        {
            continue;
        }
        last_attempt = Some(tokio::time::Instant::now());
        match tokio::time::timeout(policy.checkpoint_timeout, checkpoint_visible(visible, log))
            .await
        {
            Ok(Ok(_)) => last_success = tokio::time::Instant::now(),
            Ok(Err(error)) => {
                tracing::warn!(error=%error,"Meta checkpoint failed; old recovery chain retained")
            }
            Err(_) => tracing::warn!(
                "Meta checkpoint observation deadline exceeded; actual task retains its gate and charge"
            ),
        }
        // Only weak ownership crosses the next sleep, so an idle maintenance
        // loop cannot retain a retired Store or its backend root lock.
    }
}
async fn wait_log_capacity(
    visible: &Arc<Visible>,
    log: &Arc<dyn journal::MetaLog>,
    tip: journal::LogTip,
    bytes: usize,
) -> Result<bool> {
    if log.can_append(tip, bytes).await? {
        return Ok(true);
    }
    let Some(policy) = log.maintenance_policy() else {
        return Ok(false);
    };
    let deadline = tokio::time::Instant::now()
        .checked_add(policy.capacity_wait)
        .ok_or_else(|| journal::invalid_log("capacity wait deadline overflow"))?;
    loop {
        let finished = visible.checkpoint_finished.notified();
        if visible.poisoned.load(Ordering::Acquire) {
            return Err(unavailable(
                "store unavailable while waiting for WAL capacity",
            ));
        }
        if log.can_append(tip, bytes).await? {
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        visible.checkpoint_wakeup.notify_one();
        tokio::select! {_=finished=>{},_=tokio::time::sleep_until(deadline)=>return Ok(false),_=tokio::time::sleep(policy.poll)=>{}}
    }
}
enum LogGroup {
    Committed(journal::LogTip),
    NotAppended,
}
type LogReply = (oneshot::Sender<Result<TxnOutcome>>, Result<TxnOutcome>);

async fn persist_log_group(
    visible: &Arc<Visible>,
    log: &Arc<dyn journal::MetaLog>,
    staged: &State,
    tip: journal::LogTip,
    records: &mut Vec<log::Record>,
    replies: &mut Vec<LogReply>,
    framed_bytes: usize,
) -> Result<LogGroup> {
    let next = if records.is_empty() {
        tip
    } else {
        match wait_log_capacity(visible, log, tip, framed_bytes).await {
            Ok(true) => {}
            Ok(false) => {
                visible.metrics.wal_capacity_rejections.inc();
                records.clear();
                for (reply, _) in replies.drain(..) {
                    let _ = reply.send(Err(unavailable(
                        "WAL capacity wait expired before append; group not written",
                    )));
                }
                return Ok(LogGroup::NotAppended);
            }
            Err(error) => {
                visible.poisoned.store(true, Ordering::Release);
                for (reply, _) in replies.drain(..) {
                    let _ = reply.send(Err(unavailable(format!("WAL admission failed: {error}"))));
                }
                return Err(error);
            }
        }
        let record_count = records.len() as u64;
        let request_evictions = records
            .iter()
            .map(|record| match record {
                log::Record::Decision {
                    history: Some(delta),
                    ..
                }
                | log::Record::History(delta) => delta.requests.len() as u64,
                _ => 0,
            })
            .sum::<u64>();
        let append_timer = profile::Timer::start();
        match log.append_and_sync(tip, std::mem::take(records)).await {
            Ok(next) => {
                let append_sync_ms = append_timer.ms();
                let publish_timer = profile::Timer::start();
                #[cfg(test)]
                crash_tests::pause(
                    "after_sync_before_publish",
                    &format!(
                        "visible={} staged={}",
                        visible.committed.load().revision.0,
                        staged.revision.0
                    ),
                );
                let mut published = staged.clone();
                published.log_tip = next;
                let retained = published.history_stats();
                tracing::debug!(
                    history.requests = retained.requests,
                    history.request_bytes = retained.request_bytes,
                    history.events = retained.events,
                    history.event_bytes = retained.event_bytes,
                    history.index_base_bytes = retained.index_base_bytes,
                    "published bounded Meta history"
                );
                let published = Arc::new(published);
                let event_evictions = published
                    .events_removed
                    .saturating_sub(visible.committed.load().events_removed);
                visible.committed.store(Arc::clone(&published));
                visible.metrics.publish(&published);
                visible.metrics.wal_batches.inc();
                visible.metrics.wal_records.inc_by(record_count);
                visible.metrics.wal_framed_bytes.inc_by(framed_bytes as u64);
                visible.metrics.request_evictions.inc_by(request_evictions);
                visible.metrics.event_evictions.inc_by(event_evictions);
                if profile::enabled() {
                    profile::record(serde_json::json!({
                        "event": "afs_meta_store_profile", "backend": "canonical_log",
                        "bytes": framed_bytes, "records": record_count,
                        "revision": staged.revision.0,
                        "append_sync_ms": append_sync_ms, "publish_ms": publish_timer.ms()
                    }));
                }
                next
            }
            Err(error) => {
                visible.metrics.wal_failures.inc();
                visible.poisoned.store(true, Ordering::Release);
                for (reply, _) in replies.drain(..) {
                    let _ = reply.send(Err(unavailable(format!(
                        "operation log commit failed: {error}"
                    ))));
                }
                return Err(error);
            }
        }
    };
    for (reply, result) in replies.drain(..) {
        let _ = reply.send(result);
    }
    Ok(LogGroup::Committed(next))
}

/// Resource deferral must not suppress request/event retention on the same tick.
enum AuthorityCleanup<'a> {
    Grants(&'a mut Option<MetaKey>),
    Commands(&'a mut Option<MetaKey>),
}

async fn persist_authority_cleanup(
    visible: &Arc<Visible>,
    log: &Arc<dyn journal::MetaLog>,
    tip: journal::LogTip,
    cleanup: AuthorityCleanup<'_>,
    history: history::Limits,
    in_flight: &Arc<InFlight>,
) -> Result<journal::LogTip> {
    let limits = log.limits();
    let current = visible.committed.load_full();
    let max_deletes = history.event_count.min(history.cleanup_items);
    let prepared = match cleanup {
        AuthorityCleanup::Commands(cursor) => current.prepare_root_command_cleanup(
            cursor,
            now_unix_ms(),
            limits.record_max_bytes,
            max_deletes,
        ),
        AuthorityCleanup::Grants(cursor) => current.prepare_root_grant_cleanup(
            cursor,
            now_unix_ms(),
            limits.record_max_bytes,
            max_deletes,
        ),
    };
    let (mut next, decision) = match prepared {
        Ok(Some(prepared)) => prepared,
        Ok(None) => return Ok(tip),
        Err(error) => {
            afs_logging::warn!("meta.authority_cleanup_deferred"; "error" => error.to_string());
            return Ok(tip);
        }
    };
    let retention = match in_flight.prepare_history(&next, now_unix_ms(), history) {
        Ok(retention) => retention,
        Err(error) => {
            afs_logging::warn!("meta.authority_cleanup_deferred"; "error" => error.to_string());
            return Ok(tip);
        }
    };
    let (delta, _retirement) = match retention {
        Some((cleaned, delta, guard)) => {
            next = cleaned;
            (Some(delta), Some(guard))
        }
        None => (None, None),
    };
    if !next.history_fits(history) {
        return Ok(tip);
    }
    let record = log::Record::Decision {
        decision,
        history: delta,
    };
    let size = match journal::frame_size(tip, &record) {
        Ok(size) if size <= limits.record_max_bytes => size,
        _ => {
            afs_logging::warn!("meta.authority_cleanup_deferred"; "reason" => "framed record exceeds configured limit");
            return Ok(tip);
        }
    };
    let mut records = vec![record];
    let mut replies = Vec::new();
    match persist_log_group(visible, log, &next, tip, &mut records, &mut replies, size).await? {
        LogGroup::Committed(next) => Ok(next),
        LogGroup::NotAppended => Ok(tip),
    }
}

async fn run_log_commit_loop(
    mut receiver: mpsc::Receiver<Pending>,
    visible: Arc<Visible>,
    log: Arc<dyn journal::MetaLog>,
    mut tip: journal::LogTip,
    history: history::Limits,
    in_flight: Arc<InFlight>,
) {
    let limits = log.limits();
    // The deadline must survive ready requests, including retries that produce
    // no new business record. Otherwise an active caller can starve expiry.
    let period = history.poll();
    let mut cleanup_tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    cleanup_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut grant_cleanup_cursor = None;
    let mut command_cleanup_cursor = None;
    'receive: loop {
        let first = tokio::select! {
            biased;
            _=cleanup_tick.tick()=>{
                match persist_authority_cleanup(&visible, &log, tip, AuthorityCleanup::Grants(&mut grant_cleanup_cursor), history, &in_flight).await {
                    Ok(next) => tip = next, Err(_) => break,
                }
                match persist_authority_cleanup(&visible, &log, tip, AuthorityCleanup::Commands(&mut command_cleanup_cursor), history, &in_flight).await {
                    Ok(next) => tip = next, Err(_) => break,
                }
                let current=visible.committed.load_full();
                match in_flight.prepare_history(&current,now_unix_ms(),history) {
                    Ok(Some((next,delta,_retirement)))=>{
                        let record=log::Record::History(delta);
                        let size=match journal::frame_size(tip,&record) {Ok(size) if size<=limits.record_max_bytes=>size,_=>continue};
                        let mut records=vec![record];let mut replies=Vec::new();
                        match persist_log_group(&visible,&log,&next,tip,&mut records,&mut replies,size).await {
                            Ok(LogGroup::Committed(next))=>tip=next,Ok(LogGroup::NotAppended)=>{},Err(_)=>break,
                        }
                    },
                    Ok(None)=>{},Err(_)=>break,
                }
                continue;
            },
            item=receiver.recv()=>match item {Some(item)=>item,None=>break},
        };
        // Gather only ready work: no fixed 1ms delay on an otherwise idle Meta.
        let mut batch = VecDeque::from([first]);
        tokio::task::yield_now().await;
        while batch.len() < 64 {
            match receiver.try_recv() {
                Ok(item) => batch.push_back(item),
                Err(_) => break,
            }
        }
        let mut retirements = Vec::new();
        let mut input_holds = Vec::new();
        let mut staged = visible.committed.load_full().as_ref().clone();
        let mut records = Vec::new();
        let mut replies = Vec::new();
        let mut bytes = 0usize;
        while let Some(item) = batch.pop_front() {
            // A cancelled caller cannot release queued input capacity or its
            // prepared identity before this writer has settled the group.
            input_holds.push((item.capacity, item.identity));
            let prepared = match item.operation {
                Mutation::Transaction(txn) => {
                    staged.prepare_txn_record(txn, now_unix_ms(), limits.record_max_bytes)
                }
                Mutation::RegisterNode(request, lease) => staged.prepare_node_record(
                    request,
                    lease,
                    now_unix_ms(),
                    limits.record_max_bytes,
                ),
            };
            let (mut next, outcome, record) = match prepared {
                Ok(prepared) => prepared,
                Err(error) => {
                    replies.push((item.reply, Err(error)));
                    continue;
                }
            };
            let record = match record {
                Some(decision) => {
                    let cleanup = match in_flight.prepare_history(&next, now_unix_ms(), history) {
                        Ok(cleanup) => cleanup,
                        Err(error) => {
                            replies.push((item.reply, Err(error)));
                            continue;
                        }
                    };
                    let delta = cleanup.map(|(cleaned, delta, guard)| {
                        retirements.push(guard);
                        next = cleaned;
                        delta
                    });
                    if !next.history_fits(history) {
                        replies.push((
                            item.reply,
                            Err(journal::full(
                                "protected prepared history exceeds count/byte limits",
                            )),
                        ));
                        continue;
                    }
                    Some(log::Record::Decision {
                        decision,
                        history: delta,
                    })
                }
                None => None,
            };
            let size = match record
                .as_ref()
                .map(|record| {
                    let sequence = tip
                        .sequence
                        .checked_add(records.len() as u64)
                        .ok_or_else(|| journal::invalid_log("group sequence exhausted"))?;
                    journal::frame_size(
                        journal::LogTip {
                            sequence,
                            record_hash: tip.record_hash,
                        },
                        record,
                    )
                })
                .transpose()
            {
                Ok(size) => size.unwrap_or(0),
                Err(error) => {
                    replies.push((item.reply, Err(error)));
                    continue;
                }
            };
            if size > limits.record_max_bytes {
                replies.push((
                    item.reply,
                    Err(journal::full("record exceeds framed byte limit")),
                ));
                continue;
            }
            if bytes
                .checked_add(size)
                .is_none_or(|total| total > limits.batch_max_bytes)
            {
                match persist_log_group(
                    &visible,
                    &log,
                    &staged,
                    tip,
                    &mut records,
                    &mut replies,
                    bytes,
                )
                .await
                {
                    Ok(LogGroup::Committed(next_tip)) => {
                        tip = next_tip;
                        bytes = 0;
                    }
                    Ok(LogGroup::NotAppended) => {
                        let _ = item
                            .reply
                            .send(Err(unavailable("earlier prepared group was not appended")));
                        for pending in batch.drain(..) {
                            let _ = pending.reply.send(Err(unavailable(
                                "ready batch rejected before preparation due to WAL capacity",
                            )));
                        }
                        continue 'receive;
                    }
                    Err(_) => break 'receive,
                }
            }
            bytes += size;
            staged = next;
            if let Some(record) = record {
                records.push(record);
            }
            replies.push((item.reply, Ok(outcome)));
        }
        match persist_log_group(
            &visible,
            &log,
            &staged,
            tip,
            &mut records,
            &mut replies,
            bytes,
        )
        .await
        {
            Ok(LogGroup::Committed(next)) => tip = next,
            Ok(LogGroup::NotAppended) => continue,
            Err(_) => break,
        }
    }
    visible.poisoned.store(true, Ordering::Release);
}

fn tracing_result(result: std::result::Result<&TxnOutcome, &Error>) -> &'static str {
    match result {
        Ok(TxnOutcome::Committed { .. }) => "committed",
        Ok(TxnOutcome::ConditionFailed { .. }) => "condition_failed",
        Err(_) => "error",
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
            let state = self.committed().await?;
            self.read_budget.pin(state, &self.visible)
        })
    }

    fn read(&self, read: MetaRead) -> MetaFuture<'_, MetaSnapshot> {
        Box::pin(async move { self.committed().await?.read(read) })
    }

    fn compare_and_commit(&self, txn: MetaTxn) -> MetaFuture<'_, TxnOutcome> {
        Box::pin(async move { self.mutate(Mutation::Transaction(txn)).await })
    }

    fn watch(&self, after_revision: StoreRevision, limit: usize) -> MetaFuture<'_, WatchBatch> {
        Box::pin(async move { self.committed().await?.watch(after_revision, limit) })
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

pub(super) fn entity_key(entity: &MetaEntity) -> MetaKey {
    match entity {
        MetaEntity::RootEpochWatermark(_) => MetaKey::RootEpochWatermark,
        MetaEntity::DfsReference(reference) => MetaKey::DfsReference {
            node_id: reference.node_id.clone(),
            session_id: reference.session_id.clone(),
            reference_id: reference.reference_id.clone(),
        },
        MetaEntity::DfsGc(_) => MetaKey::DfsGc,
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
        MetaEntity::DfsDurabilityPolicy(_) => MetaKey::DfsDurabilityPolicy,
        MetaEntity::DfsPlacement(placement) => MetaKey::DfsPlacement(placement.chunk_id.clone()),
        MetaEntity::DfsCopy(copy) => MetaKey::DfsCopy(copy.id.clone()),
        MetaEntity::DfsReplicationTask(task) => MetaKey::DfsReplicationTask(task.id.clone()),
        MetaEntity::DfsWriteLease(lease) => MetaKey::DfsWriteLease(lease.inode_id.clone()),
    }
}

fn condition_matches(state: &State, condition: &TxnCondition) -> bool {
    match condition {
        TxnCondition::StoreRevisionEquals(revision) => state.revision == *revision,
        TxnCondition::RootEpochWatermarkEquals(expected) => {
            state.root_epoch_watermark() == *expected
        }
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
    state: &State,
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
    state: &State,
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

fn dfs_parent_of(state: &State, namespace_id: &NamespaceId, inode_id: &InodeId) -> Option<InodeId> {
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

fn current_session_matches(state: &State, node_id: &str, session_id: &str) -> bool {
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
    state: &State,
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
    state: &State,
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
    use super::journal::MetaLog;
    use super::*;
    use std::sync::Mutex;

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
    fn p3_typed_prefix_reads_are_scoped_sorted_and_rebuilt_from_facts() {
        let mut state = State {
            revision: StoreRevision(42),
            ..State::default()
        };
        fn put(state: &mut State, key: MetaKey, entity: MetaEntity) {
            state.entities.insert(
                key,
                VersionedEntity {
                    revision: StoreRevision(7),
                    entity,
                },
            );
        }
        for (ns, parent, name) in [
            ("n", "p", vec![255]),
            ("n", "p", b"a".to_vec()),
            ("n", "p", vec![0]),
            ("nn", "p", b"other".to_vec()),
            ("n", "pp", b"other".to_vec()),
            ("m", "p", b"before".to_vec()),
        ] {
            let key = DentryKey {
                namespace_id: NamespaceId::new(ns),
                parent_inode_id: InodeId::new(parent),
                name,
            };
            put(
                &mut state,
                MetaKey::DfsDentry(key.clone()),
                MetaEntity::DfsDentry(Dentry {
                    key,
                    inode_id: InodeId::new("inode"),
                }),
            );
        }
        for id in ["z", "a"] {
            let session = session(id, "session", u64::MAX);
            put(
                &mut state,
                MetaKey::CurrentNodeSession { node_id: id.into() },
                MetaEntity::NodeSession(session.clone()),
            );
            put(
                &mut state,
                MetaKey::NodeSession {
                    node_id: id.into(),
                    session_id: "session".into(),
                },
                MetaEntity::NodeSession(session),
            );
            let placement = PlacementRecord {
                chunk_id: crate::dfs::ChunkId::new(id),
                replica_group_id: crate::dfs::ReplicaGroupId::new("group"),
                placement_epoch: 1,
                desired_copies: 1,
                copies: Vec::new(),
                health: crate::dfs::PlacementHealth::Satisfied,
            };
            put(
                &mut state,
                MetaKey::DfsPlacement(placement.chunk_id.clone()),
                MetaEntity::DfsPlacement(placement),
            );
            let task = ReplicationTask {
                id: ReplicationTaskId::new(id),
                chunk_id: crate::dfs::ChunkId::new(id),
                placement_epoch: 1,
                desired_copies: 1,
                existing_copies: Vec::new(),
                state: crate::dfs::ReplicationTaskState::Pending,
                attempt: 0,
                next_retry_unix_ms: 0,
                last_error: None,
                claim: None,
            };
            put(
                &mut state,
                MetaKey::DfsReplicationTask(task.id.clone()),
                MetaEntity::DfsReplicationTask(task),
            );
        }
        for root in ["r", "rr", "q"] {
            for holder in ["z", "a"] {
                let mut grant = grant();
                grant.root_id = root.into();
                grant.holder_node_id = holder.into();
                put(
                    &mut state,
                    entity_key(&MetaEntity::RootGrant(grant.clone())),
                    MetaEntity::RootGrant(grant),
                );
            }
        }
        let directory = MetaRead::DfsDirectory {
            namespace_id: NamespaceId::new("n"),
            parent_inode_id: InodeId::new("p"),
        };
        let snapshot = state.read(directory.clone()).unwrap();
        let names: Vec<_> = snapshot
            .entities
            .iter()
            .map(|entity| match entity {
                MetaEntity::DfsDentry(value) => value.key.name.clone(),
                _ => panic!("wrong entity family"),
            })
            .collect();
        assert_eq!(names, vec![vec![0], b"a".to_vec(), vec![255]]);
        let before = state.snapshot_bytes_with_batch(0).unwrap();
        let recovered = State::from_snapshot(&before).unwrap();
        for (query, expected) in [
            (directory, 3),
            (MetaRead::CurrentNodeSessions, 2),
            (
                MetaRead::RootGrants {
                    root_id: "r".into(),
                },
                2,
            ),
            (MetaRead::DfsPlacements, 2),
            (MetaRead::DfsReplicationTasks, 2),
            (
                MetaRead::DfsDirectory {
                    namespace_id: NamespaceId::new("none"),
                    parent_inode_id: InodeId::new("p"),
                },
                0,
            ),
        ] {
            let original = state.read(query.clone()).unwrap();
            assert_eq!(original.entities.len(), expected);
            assert_eq!(original.revision, StoreRevision(42));
            assert_eq!(recovered.read(query).unwrap(), original);
        }
        assert_eq!(recovered.snapshot_bytes_with_batch(0).unwrap(), before);
    }

    #[test]
    fn p3_fork_shares_payloads_but_keeps_updates_private() {
        let original = StoreState::new();
        let key = MetaKey::NodeSession {
            node_id: "node-a".into(),
            session_id: "session-1".into(),
        };
        let entity = MetaEntity::NodeSession(session("node-a", "session-1", 100));
        {
            let mut state = original.state.lock().unwrap();
            state.revision = StoreRevision(1);
            state.entities.insert(
                key.clone(),
                VersionedEntity {
                    revision: StoreRevision(1),
                    entity: entity.clone(),
                },
            );
            state.events.push(WatchEvent {
                revision: StoreRevision(1),
                event_index: 0,
                key: key.clone(),
                change: WatchChange::Put(entity),
            });
        }
        let fork = original.fork().unwrap();
        let original_payload = {
            let state = original.state.lock().unwrap();
            state.entities.get(&key).unwrap() as *const VersionedEntity
        };
        let fork_payload = {
            let state = fork.state.lock().unwrap();
            state.entities.get(&key).unwrap() as *const VersionedEntity
        };
        assert_eq!(
            original_payload, fork_payload,
            "a private fork must share unchanged entity payloads"
        );
        let original_event = {
            let state = original.state.lock().unwrap();
            state.events.iter().next().unwrap() as *const WatchEvent
        };
        let fork_event = {
            let state = fork.state.lock().unwrap();
            state.events.iter().next().unwrap() as *const WatchEvent
        };
        assert_eq!(
            original_event, fork_event,
            "history payloads must not be cloned by fork"
        );
        {
            let mut state = fork.state.lock().unwrap();
            state.revision = StoreRevision(2);
            state.entities.insert(
                key.clone(),
                VersionedEntity {
                    revision: StoreRevision(2),
                    entity: MetaEntity::NodeSession(session("node-a", "session-1", 200)),
                },
            );
            state.events.push(WatchEvent {
                revision: StoreRevision(2),
                event_index: 0,
                key: key.clone(),
                change: WatchChange::Delete,
            });
        }
        assert_eq!(
            original
                .state
                .lock()
                .unwrap()
                .entities
                .get(&key)
                .unwrap()
                .revision,
            StoreRevision(1)
        );
        assert_eq!(
            fork.state
                .lock()
                .unwrap()
                .entities
                .get(&key)
                .unwrap()
                .revision,
            StoreRevision(2)
        );
        assert_eq!(original.state.lock().unwrap().events.len(), 1);
        assert_eq!(fork.state.lock().unwrap().events.len(), 2);
        let original_bytes = original.snapshot_bytes().unwrap();
        let restored = StoreState::from_snapshot(&original_bytes).unwrap();
        assert_eq!(restored.snapshot_bytes().unwrap(), original_bytes);
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

    fn p3_watch_test_event(revision: u64, event_index: u32, name: String) -> WatchEvent {
        WatchEvent {
            revision: StoreRevision(revision),
            event_index,
            key: MetaKey::Root { root_id: name },
            change: WatchChange::Delete,
        }
    }
    #[test]
    fn p3_watch_response_zero_limit_still_bounds_count_at_whole_revision() {
        let mut state = MemoryState::default();
        for n in 1..=16_385 {
            state
                .events
                .push(p3_watch_test_event(n, 0, format!("root-{n}")));
        }
        let WatchBatch::Events {
            events,
            next_revision,
            ..
        } = state.watch(StoreRevision::ZERO, 0).unwrap()
        else {
            panic!("retained history must not be fabricated as compacted");
        };
        assert_eq!(
            events.len(),
            16_384,
            "limit=0 must not disable the hard response bound"
        );
        assert_eq!(next_revision, StoreRevision(16_385));
    }
    #[test]
    fn p3_watch_response_oversize_first_revision_refuses_without_partial_success() {
        let mut state = MemoryState::default();
        let mut name = String::with_capacity(17 * 1024 * 1024);
        name.push('x');
        state.events.push(p3_watch_test_event(1, 0, name));
        assert!(
            state.watch(StoreRevision::ZERO, 1).is_err(),
            "owned spare capacity must participate in response admission before clone"
        );
        assert_eq!(state.events.len(), 1);
        assert_eq!(state.compacted_to, StoreRevision::ZERO);
    }
    #[test]
    fn p3_watch_response_byte_limit_keeps_only_complete_prior_revisions() {
        let mut state = MemoryState::default();
        state.events.push(p3_watch_test_event(1, 0, "small".into()));
        state
            .events
            .push(p3_watch_test_event(2, 0, "first-of-large-revision".into()));
        state
            .events
            .push(p3_watch_test_event(2, 1, "x".repeat(17 * 1024 * 1024)));
        let WatchBatch::Events {
            events,
            next_revision,
            ..
        } = state.watch(StoreRevision::ZERO, 0).unwrap()
        else {
            panic!("response capacity is not history compaction");
        };
        assert_eq!(
            events.len(),
            1,
            "a response must roll back the entire over-budget revision"
        );
        assert_eq!(next_revision, StoreRevision(2));
        assert!(state.watch(StoreRevision(1), 0).is_err());
        assert_eq!(state.compacted_to, StoreRevision::ZERO);
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
        let backend = Arc::new(journal::MemoryLog::new(Default::default()).unwrap());
        let store = Store::open_log(backend.clone()).await.unwrap();
        let request = RequestKey::new("node-a", "register-1");
        let first = store
            .register_node_session(request.clone(), lease("session-1"))
            .await
            .unwrap();
        assert!(matches!(first, TxnOutcome::Committed { .. }));
        drop(store);

        let reopened = Store::open_log(backend).await.unwrap();
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
        let store = Store::open_log(Arc::new(
            journal::MemoryLog::new(Default::default()).unwrap(),
        ))
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
        let store = Store::open_log(Arc::new(
            journal::MemoryLog::new(Default::default()).unwrap(),
        ))
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
        inner: journal::MemoryLog,
        started: Notify,
        release: Notify,
        fail: bool,
    }

    impl MetaLog for GatedBackend {
        fn persistence(&self) -> BackendPersistence {
            self.inner.persistence()
        }
        fn limits(&self) -> journal::LogLimits {
            self.inner.limits()
        }
        fn health(&self) -> MetaFuture<'_, ()> {
            self.inner.health()
        }
        fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
            self.inner.recover()
        }
        fn append_and_sync(
            &self,
            expected: journal::LogTip,
            records: Vec<log::Record>,
        ) -> MetaFuture<'_, journal::LogTip> {
            Box::pin(async move {
                self.started.notify_one();
                self.release.notified().await;
                if self.fail {
                    Err(unavailable("injected disk error"))
                } else {
                    self.inner.append_and_sync(expected, records).await
                }
            })
        }
    }

    struct CountingBackend {
        inner: journal::MemoryLog,
        commits: Arc<std::sync::atomic::AtomicUsize>,
        last_record_bytes: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl MetaLog for CountingBackend {
        fn persistence(&self) -> BackendPersistence {
            self.inner.persistence()
        }
        fn limits(&self) -> journal::LogLimits {
            self.inner.limits()
        }
        fn health(&self) -> MetaFuture<'_, ()> {
            self.inner.health()
        }
        fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
            self.inner.recover()
        }
        fn append_and_sync(
            &self,
            expected: journal::LogTip,
            records: Vec<log::Record>,
        ) -> MetaFuture<'_, journal::LogTip> {
            self.commits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let bytes = journal::encode_batch(
                journal::StoreIdentity {
                    store_id: [1; 16],
                    cluster_id: [2; 16],
                    incarnation: [3; 16],
                },
                expected,
                records.clone(),
                self.limits(),
            )
            .map(|(_, _, bytes)| bytes);
            Box::pin(async move {
                self.last_record_bytes
                    .store(bytes?, std::sync::atomic::Ordering::SeqCst);
                self.inner.append_and_sync(expected, records).await
            })
        }
    }

    fn view_limits() -> ReadLimits {
        ReadLimits {
            views: 2,
            held_bytes: 1024 * 1024,
            lifetime: Duration::from_millis(80),
            reap: Duration::from_millis(5),
        }
    }
    #[test]
    fn p3_views_charge_includes_authority_owned_capacity() {
        let mut state = State {
            revision: StoreRevision(1),
            ..State::default()
        };
        let mut text = String::with_capacity(128 * 1024);
        text.push('x');
        let devices = Vec::<StorageDeviceDescriptor>::with_capacity(32);
        let minimum =
            text.capacity() + devices.capacity() * std::mem::size_of::<StorageDeviceDescriptor>();
        state.entities.insert(
            MetaKey::CurrentNodeSession {
                node_id: "n".into(),
            },
            state::VersionedEntity {
                revision: StoreRevision(1),
                entity: MetaEntity::NodeSession(NodeSession {
                    node_id: "n".into(),
                    session_id: "s".into(),
                    grpc_addr: String::new(),
                    data_addr: String::new(),
                    rest_addr: text,
                    storage_devices: devices,
                    lease_epoch: 1,
                    expires_at_unix_ms: u64::MAX,
                }),
            },
        );
        state.ensure_history_index().unwrap();
        assert!(
            state.pin_charge().unwrap() >= minimum as u64,
            "a view pins authority payloads, not just history or pointers"
        );
    }
    #[test]
    fn p3_views_request_completion_does_not_hide_input_spare_capacity() {
        let mut request_id = String::with_capacity(128 * 1024);
        request_id.push_str("id");
        let (state, _, _) = State::default()
            .prepare_node_record(
                RequestKey::new("view", request_id),
                lease("s"),
                now_unix_ms(),
                1024 * 1024,
            )
            .unwrap();
        let retained = state
            .request_completion
            .iter()
            .map(|(key, _)| key.caller_id.capacity() + key.request_id.capacity())
            .sum::<usize>();
        assert!(
            state.history_stats().request_bytes >= retained as u64,
            "completion keys cannot retain large uncharged admission capacity"
        );
    }
    #[test]
    fn p3_views_incremental_authority_charge_matches_full_rebuild_and_image() {
        let mut state = State::default();
        for n in 0..4 {
            state = state
                .prepare_node_record(
                    RequestKey::new("view-charge", format!("r{n}")),
                    lease(&format!("s{n}")),
                    now_unix_ms(),
                    1024 * 1024,
                )
                .unwrap()
                .0;
            let expected = state.pin_charge().unwrap();
            let mut rebuilt = state.clone();
            rebuilt.authority_indexed = false;
            rebuilt.history_indexed = false;
            rebuilt.ensure_history_index().unwrap();
            assert_eq!(
                rebuilt.pin_charge().unwrap(),
                expected,
                "incremental owner charges must agree with a full reconstruction"
            );
            let bytes = log::StateImage::encode(&state, 1024 * 1024).unwrap();
            let restored = log::StateImage::decode(&bytes, 1024 * 1024).unwrap();
            assert_eq!(
                restored.pin_charge().unwrap(),
                expected,
                "normal compact capacities remain equal after durable image restore"
            );
        }
    }
    #[tokio::test]
    async fn p3_views_same_state_shares_bytes_but_counts_each_view() {
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let mut limits = view_limits();
        // This case tests quota/sharing, not deadline scheduling.
        limits.lifetime = Duration::from_secs(5);
        let store = Store::open_log_with_read_limits(log, limits).await.unwrap();
        let first = store.read_view().await.unwrap();
        let before = store.read_budget_stats();
        assert_eq!(before.views, 1);
        assert!(before.bytes > 0);
        let second = store.read_view().await.unwrap();
        assert_eq!(store.read_budget_stats().views, 2);
        assert_eq!(store.read_budget_stats().bytes, before.bytes);
        assert!(
            store.read_view().await.is_err(),
            "view count must be finite"
        );
        store
            .register_node_session(RequestKey::new("view", "write"), lease("new"))
            .await
            .unwrap();
        assert_eq!(
            first
                .read(MetaRead::CurrentNodeSessions)
                .await
                .unwrap()
                .revision,
            StoreRevision::ZERO
        );
        drop(second);
        let fresh = store.read_view().await.unwrap();
        assert_eq!(
            fresh
                .read(MetaRead::CurrentNodeSessions)
                .await
                .unwrap()
                .revision,
            StoreRevision(1)
        );
        drop(first);
        drop(fresh);
        assert_eq!(store.read_budget_stats().views, 0);
        assert_eq!(store.read_budget_stats().bytes, 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn p3_views_registry_stays_bounded_during_concurrent_fast_drop() {
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let mut limits = view_limits();
        limits.lifetime = Duration::from_secs(5);
        let store = Arc::new(Store::open_log_with_read_limits(log, limits).await.unwrap());
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let store = store.clone();
            tasks.spawn(async move {
                for n in 0..64 {
                    if let Ok(view) = store.read_view().await {
                        if n % 3 == 0 {
                            tokio::task::yield_now().await;
                        }
                        drop(view);
                    }
                    let registry = store.read_budget.registry.lock().unwrap();
                    assert!(
                        registry.views.len() <= limits.views,
                        "weak view registry exceeded its independent bound"
                    );
                    assert!(
                        registry.allocations.len() <= limits.views,
                        "weak state registry exceeded its independent bound"
                    );
                }
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        assert_eq!(store.read_budget_stats().views, 0);
        assert_eq!(store.read_budget_stats().bytes, 0);
        store
            .register_node_session(RequestKey::new("view", "after-fast-drop"), lease("new"))
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn p3_views_byte_refusal_is_before_pin_and_does_not_block_writer() {
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let mut limits = view_limits();
        limits.held_bytes = 4096;
        let store = Store::open_log_with_read_limits(log, limits).await.unwrap();
        assert!(
            store.read_view().await.is_err(),
            "an oversized state cannot bypass pin bytes"
        );
        assert_eq!(store.read_budget_stats().views, 0);
        assert_eq!(store.read_budget_stats().bytes, 0);
        store
            .register_node_session(RequestKey::new("view", "write"), lease("new"))
            .await
            .unwrap();
        store.health().await.unwrap();
    }
    #[tokio::test]
    async fn p3_views_expiry_reclaims_state_without_caller_polling() {
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let store = Store::open_log_with_read_limits(log, view_limits())
            .await
            .unwrap();
        let view = store.read_view().await.unwrap();
        let old = Arc::downgrade(&view.state_for_test());
        store
            .register_node_session(RequestKey::new("view", "next"), lease("new"))
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while old.upgrade().is_some() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            old.upgrade().is_none(),
            "retaining an idle handle must not pin its old state forever"
        );
        assert!(view.read(MetaRead::CurrentNodeSessions).await.is_err());
        assert!(view.lookup(&MetaKey::CurrentNodeSessions).is_err());
        assert!(view.snapshot().is_err());
        assert!(view.statistics().is_err());
        assert_eq!(store.read_budget_stats().views, 0);
        assert_eq!(store.read_budget_stats().bytes, 0);
    }
    #[tokio::test]
    async fn p3_views_known_failed_authority_refuses_existing_view() {
        let log = Arc::new(GatedLog {
            inner: Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap()),
            started: Notify::new(),
            release: Notify::new(),
            fail: true,
            persist_before_error: true,
        });
        let store = Arc::new(
            Store::open_log_with_read_limits(log.clone(), ReadLimits::default())
                .await
                .unwrap(),
        );
        let view = store.read_view().await.unwrap();
        let writer = store.clone();
        let task = tokio::spawn(async move {
            writer
                .register_node_session(RequestKey::new("view", "unknown"), lease("new"))
                .await
        });
        log.started.notified().await;
        log.release.notify_one();
        assert!(task.await.unwrap().is_err());
        assert!(
            view.read(MetaRead::CurrentNodeSessions).await.is_err(),
            "a known failed authority cannot serve cached authorization facts"
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn p3_views_expired_active_read_retains_charge_until_release() {
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let store = Store::open_log_with_read_limits(log, view_limits())
            .await
            .unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let pause = Arc::new(state::ReadPause::new(entered_tx));
        install_read_pause(&store, pause.clone()).await;
        let view = Arc::new(store.read_view().await.unwrap());
        let charged = store.read_budget_stats();
        assert!(charged.bytes > 0);
        let reader = view.clone();
        let runtime = tokio::runtime::Handle::current();
        let task = std::thread::spawn(move || {
            runtime.block_on(reader.read(MetaRead::CurrentNodeSessions))
        });
        let entered = entered_rx.recv_timeout(Duration::from_secs(1));
        if entered.is_err() {
            pause.release();
            let _ = task.join();
            panic!("view reader missed gate");
        }
        tokio::time::sleep(Duration::from_millis(160)).await;
        let during = store.read_budget_stats();
        pause.release();
        let result = task.join().unwrap();
        assert_eq!(during.views, 1);
        assert_eq!(
            during.bytes, charged.bytes,
            "active reader still owns allocation and slot"
        );
        assert!(
            result.is_err(),
            "expired work cannot deliver a success after the pause"
        );
        assert_eq!(store.read_budget_stats().views, 0);
        assert_eq!(store.read_budget_stats().bytes, 0);
    }

    fn retention_limits() -> history::Limits {
        history::Limits {
            request_count: 2,
            event_count: 4,
            request_bytes: 1024 * 1024,
            event_bytes: 1024 * 1024,
            cleanup_items: 4,
            ..history::Limits::default()
        }
    }
    async fn stop_local_retention_store(store: Store, log: Arc<local_log::LocalLog>) {
        drop(store);
        tokio::time::timeout(Duration::from_secs(2), async {
            while Arc::strong_count(&log) != 1 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("closed writer and checkpoint observer must release the local log");
        drop(log);
    }
    #[tokio::test]
    async fn p3_retention_local_runtime_expiry_checkpoint_reopen_keeps_removal() {
        let dir = tempfile::tempdir().unwrap();
        let local =
            Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
        let identity = local.identity();
        let mut limits = retention_limits();
        limits.request_retention = Duration::from_millis(100);
        limits.event_retention = Duration::from_millis(100);
        let store = Store::open_log_with_history(local.clone(), limits)
            .await
            .unwrap();
        let key = RequestKey::new("local-retention", "expired");
        store
            .register_node_session(key.clone(), lease("session"))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let state = store.committed().await.unwrap();
                if !state.requests.contains_key(&key) && state.compacted_to == StoreRevision(1) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actual local WAL must durably expire the idle history");
        let tip = store.checkpoint().await.unwrap();
        assert!(tip.sequence >= 2);
        stop_local_retention_store(store, local).await;

        let local =
            Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
        assert_eq!(local.identity(), identity);
        // Raising the retention window must not resurrect an explicit removal.
        let store = Store::open_log_with_history(local.clone(), retention_limits())
            .await
            .unwrap();
        let state = store.committed().await.unwrap();
        assert_eq!(state.revision, StoreRevision(1));
        assert_eq!(state.log_tip, tip);
        assert!(!state.requests.contains_key(&key));
        assert_eq!(state.compacted_to, StoreRevision(1));
        assert!(matches!(
            store.watch(StoreRevision::ZERO, 1).await.unwrap(),
            WatchBatch::Compacted { .. }
        ));
        assert!(matches!(
            store
                .register_node_session(key, lease("session"))
                .await
                .unwrap(),
            TxnOutcome::Committed {
                revision: StoreRevision(2),
                ..
            }
        ));
        stop_local_retention_store(store, local).await;
    }
    #[tokio::test]
    async fn p3_retention_local_runtime_lowered_limits_persist_before_admission() {
        let dir = tempfile::tempdir().unwrap();
        let local =
            Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
        let identity = local.identity();
        let wide = history::Limits {
            request_count: 8,
            event_count: 16,
            ..retention_limits()
        };
        let store = Store::open_log_with_history(local.clone(), wide)
            .await
            .unwrap();
        for n in 0..4 {
            store
                .register_node_session(
                    RequestKey::new("local-retention", format!("r{n}")),
                    lease(&format!("session-{n}")),
                )
                .await
                .unwrap();
        }
        let before = store.committed().await.unwrap();
        assert_eq!(before.requests.len(), 4);
        let before_tip = before.log_tip;
        stop_local_retention_store(store, local).await;

        let local =
            Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
        assert_eq!(local.identity(), identity);
        let store = Store::open_log_with_history(local.clone(), retention_limits())
            .await
            .unwrap();
        let state = store.committed().await.unwrap();
        assert_eq!(state.revision, StoreRevision(4));
        assert_eq!(state.history_stats().requests, 2);
        assert_eq!(state.history_stats().events, 4);
        assert_eq!(state.compacted_to, StoreRevision(2));
        assert!(state.log_tip.sequence > before_tip.sequence);
        let cleaned_tip = state.log_tip;
        for n in 0..4 {
            assert_eq!(
                state
                    .requests
                    .contains_key(&RequestKey::new("local-retention", format!("r{n}"))),
                n >= 2
            );
        }
        stop_local_retention_store(store, local).await;

        let local =
            Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
        assert_eq!(local.identity(), identity);
        let store = Store::open_log_with_history(local.clone(), wide)
            .await
            .unwrap();
        let recovered = store.committed().await.unwrap();
        assert_eq!(recovered.log_tip, cleaned_tip);
        assert_eq!(recovered.revision, StoreRevision(4));
        assert_eq!(recovered.requests.len(), 2);
        assert_eq!(recovered.compacted_to, StoreRevision(2));
        assert_eq!(
            recovered.history_stats().request_bytes,
            state.history_stats().request_bytes
        );
        assert_eq!(
            recovered.history_stats().event_bytes,
            state.history_stats().event_bytes
        );
        stop_local_retention_store(store, local).await;
    }
    #[tokio::test]
    async fn p3_retention_runtime_capacity_and_rebuild_follow_actual_outcomes() {
        use journal::MetaLog;
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let store = Store::open_log_with_history(log.clone(), retention_limits())
            .await
            .unwrap();
        for n in 0..4 {
            store
                .register_node_session(
                    RequestKey::new("retained", format!("r{n}")),
                    lease(&format!("session-{n}")),
                )
                .await
                .unwrap();
        }
        let view = store.read_view().await.unwrap();
        let stats = view.state_for_test().history_stats();
        assert_eq!(
            stats.requests, 2,
            "running writer must apply count cleanup, not only expose a selector"
        );
        assert_eq!(stats.events, 4);
        assert!(stats.request_bytes > 0 && stats.event_bytes > 0);
        assert!(
            store
                .read(MetaRead::RequestOutcome(RequestKey::new("retained", "r0")))
                .await
                .unwrap()
                .request_outcome
                .is_none()
        );
        assert!(matches!(
            store.watch(StoreRevision::ZERO, 1).await.unwrap(),
            WatchBatch::Compacted {
                compacted_to: StoreRevision(2),
                ..
            }
        ));
        let recovered = log.recover().await.unwrap();
        let mut replayed = State::default();
        for record in recovered.records {
            replayed = replayed.replay_record(&record).unwrap();
        }
        assert_eq!(replayed.history_stats().requests, 2);
        assert_eq!(replayed.history_stats().request_bytes, stats.request_bytes);
        let bytes = log::StateImage::encode(&replayed, 1024 * 1024).unwrap();
        let restored = log::StateImage::decode(&bytes, 1024 * 1024).unwrap();
        assert_eq!(restored.history_stats().request_bytes, stats.request_bytes);
        assert_eq!(restored.history_stats().event_bytes, stats.event_bytes);
    }
    #[test]
    fn p3_retention_in_flight_capacity_and_retirement_admission_bounds() {
        let registry = Arc::new(InFlight {
            state: Mutex::new(InFlightState::default()),
            limit: 1,
        });
        let key = RequestKey::new("admission", "original");
        let operation = Mutation::RegisterNode(key.clone(), lease("one"));
        let first = registry.admit(&operation).unwrap();
        let duplicate = registry.admit(&operation).unwrap();
        assert!(
            registry
                .admit(&Mutation::RegisterNode(
                    RequestKey::new("admission", "other"),
                    lease("two")
                ))
                .is_err()
        );
        drop(first);
        assert_eq!(registry.state.lock().unwrap().active.get(&key), Some(&1));
        drop(duplicate);
        assert_eq!(registry.state.lock().unwrap().bytes, 0);
        let (state, _, _) = State::default()
            .prepare_node_record(key.clone(), lease("one"), 1, 65536)
            .unwrap();
        let limits = history::Limits {
            request_retention: Duration::from_millis(1),
            event_retention: Duration::from_millis(1),
            ..Default::default()
        };
        let (_, delta, retiring) = registry
            .prepare_history(&state, 100, limits)
            .unwrap()
            .unwrap();
        assert_eq!(delta.requests.len(), 1);
        assert!(
            registry.admit(&operation).is_err(),
            "admission cannot race a selected durable retirement"
        );
        drop(retiring);
        let admitted = registry.admit(&operation).unwrap();
        drop(admitted);
        assert_eq!(registry.state.lock().unwrap().bytes, 0);
        let large = Mutation::RegisterNode(
            RequestKey::new("admission", "x".repeat(PROTECTED_KEY_BYTES as usize / 4)),
            lease("large"),
        );
        assert!(
            registry.admit(&large).is_err(),
            "all identity copies must fit the existing4MiB protection budget"
        );
        let state = registry.state.lock().unwrap();
        assert_eq!(state.bytes, 0);
        assert!(state.active.is_empty() && state.retiring.is_empty() && !state.failed);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn p3_retention_queued_retry_survives_cleanup_deadline() {
        let log = Arc::new(GatedLog {
            inner: Arc::new(journal::MemoryLog::new(Default::default()).unwrap()),
            started: Notify::new(),
            release: Notify::new(),
            fail: false,
            persist_before_error: false,
        });
        let mut policy = retention_limits();
        policy.request_retention = Duration::from_millis(100);
        policy.event_retention = Duration::from_millis(100);
        let store = Arc::new(
            Store::open_log_with_history(log.clone(), policy)
                .await
                .unwrap(),
        );
        let key = RequestKey::new("queued-retained", "same-operation");
        log.release.notify_one();
        let first = store
            .register_node_session(key.clone(), lease("original"))
            .await
            .unwrap();
        log.started.notified().await;
        let TxnOutcome::Committed {
            outcome: expected, ..
        } = first
        else {
            panic!("seed was not committed");
        };
        let writer = store.clone();
        let blocker = tokio::spawn(async move {
            let mut other = lease("blocked");
            other.node_id = "node-blocked".into();
            writer
                .register_node_session(RequestKey::new("queued-retained", "blocker"), other)
                .await
        });
        log.started.notified().await;
        let mut retry = store.register_node_session(key.clone(), lease("must-not-execute"));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(retry.as_mut().poll(&mut context).is_pending());
        assert_eq!(
            store.writes.capacity(),
            1023,
            "retry must actually be queued while the prior ACK is blocked"
        );
        // Unlike blocking the executor, this lets the timer driver mark expiry
        // ready while the writer is still inside the preceding durable ACK.
        tokio::time::sleep(Duration::from_millis(220)).await;
        let pump_log = log.clone();
        let pump = tokio::spawn(async move {
            loop {
                pump_log.started.notified().await;
                pump_log.release.notify_one();
            }
        });
        log.release.notify_one();
        let actual = retry.await.unwrap();
        blocker.await.unwrap().unwrap();
        assert!(
            matches!(actual, TxnOutcome::ConditionFailed { existing_outcome: Some(outcome), .. } if outcome == expected),
            "queued retry lost its admitted outcome and executed again"
        );
        assert_eq!(
            store.visible.committed.load().node_epochs.get("node-a"),
            Some(&1)
        );
        tokio::time::sleep(Duration::from_millis(220)).await;
        assert!(
            store
                .read(MetaRead::RequestOutcome(key))
                .await
                .unwrap()
                .request_outcome
                .is_none(),
            "settled admission protection must not become permanent retention"
        );
        drop(store);
        tokio::time::timeout(Duration::from_secs(2), async {
            while Arc::strong_count(&log) > 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        pump.abort();
        assert!(pump.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn p3_lifecycle_completed_commands_reclaimed_pending_and_failed_preserved() {
        use journal::MetaLog;
        let backend = Arc::new(journal::MemoryLog::new(Default::default()).unwrap());
        let policy = history::Limits {
            request_count: 128,
            event_count: 512,
            cleanup_items: 32,
            request_retention: Duration::from_millis(50),
            event_retention: Duration::from_millis(50),
            ..Default::default()
        };
        let store = Store::open_log_with_history(backend.clone(), policy)
            .await
            .unwrap();
        let mut old_lease = lease("expired-id");
        old_lease.lease_ttl = Duration::from_millis(1);
        store
            .register_node_session(RequestKey::new("lifecycle", "old-session"), old_lease)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
        store
            .register_node_session(
                RequestKey::new("lifecycle", "current-session"),
                lease("current-id"),
            )
            .await
            .unwrap();
        let make_command = |id: &str| RootCommandRecord {
            command_id: id.into(),
            home_node_id: "old-home".into(),
            home_session_id: "old-session".into(),
            root_id: "removed-root".into(),
            root_epoch: 7,
            old_access_generation: 3,
            command_type: RootCommandType::RevokeAccess,
        };
        let make_ack = |id: &str, success| RootCommandAck {
            command_id: id.into(),
            node_id: "old-home".into(),
            session_id: "old-session".into(),
            root_id: "removed-root".into(),
            root_epoch: 7,
            access_generation: 3,
            success,
            message: String::new(),
        };
        let mut active_command = make_command("active");
        active_command.root_id = "active-root".into();
        let mut active_ack = make_ack("active", true);
        active_ack.root_id = "active-root".into();
        let mut mismatched_ack = make_ack("mismatch", true);
        mismatched_ack.root_id = "wrong-root".into();
        let key = RequestKey::new("lifecycle", "seed");
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::AckRootCommand);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::RootCommand(make_command("done"))),
            TxnMutation::Put(MetaEntity::RootCommandAck(make_ack("done", true))),
            TxnMutation::Put(MetaEntity::RootCommand(make_command("failed"))),
            TxnMutation::Put(MetaEntity::RootCommandAck(make_ack("failed", false))),
            TxnMutation::Put(MetaEntity::RootCommand(make_command("pending"))),
            TxnMutation::Put(MetaEntity::RootCommand(active_command)),
            TxnMutation::Put(MetaEntity::RootCommandAck(active_ack)),
            TxnMutation::Put(MetaEntity::Root(RootRecord {
                root_id: "active-root".into(),
                root_epoch: 7,
                home_node_id: "old-home".into(),
                home_session_id: "old-session".into(),
                local_prepare_id: "prepare".into(),
                created_at_revision: StoreRevision(1),
                updated_at_revision: StoreRevision(1),
            })),
            TxnMutation::Put(MetaEntity::RootGrant(RootAccessGrant {
                root_id: "active-root".into(),
                root_epoch: 7,
                home_node_id: "old-home".into(),
                home_session_id: "old-session".into(),
                holder_node_id: "old-home".into(),
                holder_session_id: "old-session".into(),
                access_generation: 3,
                rights: vec![RootRight::Read],
                fencing_token: "current-barrier".into(),
                issued_at_revision: StoreRevision(1),
            })),
            TxnMutation::Put(MetaEntity::RootCommand(make_command("mismatch"))),
            TxnMutation::Put(MetaEntity::RootCommandAck(mismatched_ack)),
            TxnMutation::Put(MetaEntity::RootCommandAck(make_ack("orphan", true))),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key.clone(),
                operation: StoreOperation::AckRootCommand,
                result: OperationResult::RootCommandAck(make_ack("done", true)),
            }),
        ]);
        assert!(matches!(
            store.compare_and_commit(txn).await.unwrap(),
            TxnOutcome::Committed { .. }
        ));
        let done = MetaKey::RootCommand {
            command_id: "done".into(),
        };
        assert!(
            store.visible.committed.load().entities.contains_key(&done),
            "retained outcome/watch still protect completed command"
        );
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while store.visible.committed.load().entities.contains_key(&done)
            && tokio::time::Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let state = store.visible.committed.load_full();
        assert!(
            !state.entities.contains_key(&done),
            "completed command remained after outcome and watch retention expired"
        );
        assert!(!state.entities.contains_key(&MetaKey::RootCommandAck {
            command_id: "done".into(),
            home_node_id: "old-home".into(),
            home_session_id: "old-session".into()
        }));
        for id in ["failed", "pending", "active", "mismatch"] {
            assert!(state.entities.contains_key(&MetaKey::RootCommand {
                command_id: id.into()
            }));
        }
        assert!(state.entities.contains_key(&MetaKey::RootCommandAck {
            command_id: "failed".into(),
            home_node_id: "old-home".into(),
            home_session_id: "old-session".into()
        }));
        assert!(state.entities.contains_key(&MetaKey::RootCommandAck {
            command_id: "orphan".into(),
            home_node_id: "old-home".into(),
            home_session_id: "old-session".into()
        }));
        assert_eq!(state.root_epoch_watermark(), 7);
        assert!(
            state.entities.contains_key(&MetaKey::NodeSession {
                node_id: "node-a".into(),
                session_id: "expired-id".into()
            }),
            "expired-ID rejection record must remain"
        );
        assert!(
            store
                .register_node_session(
                    RequestKey::new("lifecycle", "reuse-expired"),
                    lease("expired-id")
                )
                .await
                .is_err()
        );
        assert_eq!(
            store.visible.committed.load().node_epochs.get("node-a"),
            Some(&2)
        );
        let recovered = backend.recover().await.unwrap();
        let mut replay = State::default();
        for record in recovered.records {
            replay = replay.replay_record(&record).unwrap();
        }
        assert!(!replay.entities.contains_key(&done));
        assert_eq!(replay.root_epoch_watermark(), 7);
        let image = log::StateImage::encode(&replay, 1024 * 1024).unwrap();
        let restored = log::StateImage::decode(&image, 1024 * 1024).unwrap();
        assert_eq!(restored.root_epoch_watermark(), 7);
        assert_eq!(restored.pin_charge().unwrap(), replay.pin_charge().unwrap());
    }

    #[tokio::test]
    async fn p3_retention_runtime_idle_expiry_is_a_durable_management_change() {
        use journal::MetaLog;
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let mut policy = retention_limits();
        policy.request_retention = Duration::from_millis(100);
        policy.event_retention = Duration::from_millis(100);
        let store = Store::open_log_with_history(log.clone(), policy)
            .await
            .unwrap();
        let key = RequestKey::new("retained", "expires");
        store
            .register_node_session(key.clone(), lease("session"))
            .await
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if store
                .read(MetaRead::RequestOutcome(key.clone()))
                .await
                .unwrap()
                .request_outcome
                .is_none()
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "idle expiry was not persisted and published"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            store.read_view().await.unwrap().state_for_test().revision,
            StoreRevision(1)
        );
        let recovered = log.recover().await.unwrap();
        assert!(recovered.tip.sequence >= 2);
        let mut replayed = State::default();
        for record in recovered.records {
            replayed = replayed.replay_record(&record).unwrap();
        }
        assert!(!replayed.requests.contains_key(&key));
        assert_eq!(replayed.compacted_to, StoreRevision(1));
        let retry = store
            .register_node_session(key, lease("session"))
            .await
            .unwrap();
        assert!(
            matches!(
                retry,
                TxnOutcome::Committed {
                    revision: StoreRevision(2),
                    ..
                }
            ),
            "outside retention is not permanent exactly-once"
        );
    }
    #[tokio::test]
    async fn p3_retention_runtime_busy_requests_do_not_reset_cleanup_deadline() {
        use journal::MetaLog;
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let mut policy = retention_limits();
        policy.request_retention = Duration::from_millis(200);
        policy.event_retention = Duration::from_millis(200);
        let store = Store::open_log_with_history(log.clone(), policy)
            .await
            .unwrap();
        let key = RequestKey::new("retained", "busy-retry");
        store
            .register_node_session(key.clone(), lease("session"))
            .await
            .unwrap();
        // Keep the writer active without generating new decisions. A timer
        // created anew by every recv iteration would never reach its deadline.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut attempts = 0usize;
        let mut recommitted = false;
        while tokio::time::Instant::now() < deadline {
            attempts += 1;
            let outcome = store
                .register_node_session(key.clone(), lease("session"))
                .await
                .unwrap();
            if matches!(outcome, TxnOutcome::Committed { .. }) {
                recommitted = true;
                break;
            }
        }
        println!("P3 busy expiry attempts={attempts} recommitted={recommitted}");
        assert!(
            attempts >= 32,
            "test did not exercise a sustained ready writer"
        );
        assert!(
            recommitted,
            "ready retries starved the durable expiry record"
        );
        let recovered = log.recover().await.unwrap();
        assert!(
            recovered
                .records
                .iter()
                .any(|record| matches!(record, log::Record::History(_)))
        );
        let mut replayed = State::default();
        for record in recovered.records {
            replayed = replayed.replay_record(&record).unwrap();
        }
        assert_eq!(replayed.revision, StoreRevision(2));
        assert_eq!(replayed.compacted_to, StoreRevision(1));
    }
    #[tokio::test]
    async fn p3_retention_runtime_protected_new_outcome_cannot_bypass_byte_cap() {
        use journal::MetaLog;
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let mut policy = retention_limits();
        policy.request_bytes = 1;
        let store = Store::open_log_with_history(log.clone(), policy)
            .await
            .unwrap();
        assert!(
            store
                .register_node_session(RequestKey::new("retained", "too-large"), lease("session"))
                .await
                .is_err(),
            "protected result does not exempt its bytes"
        );
        assert_eq!(
            store.read_view().await.unwrap().state_for_test().revision,
            StoreRevision::ZERO
        );
        assert_eq!(log.recover().await.unwrap().tip.sequence, 0);
        store.health().await.unwrap();
    }
    #[tokio::test]
    async fn p3_retention_runtime_cleanup_stays_private_until_ack() {
        use journal::MetaLog;
        let inner = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let mut state = State::default();
        let mut records = Vec::new();
        for n in 0..2 {
            let (next, _, record) = state
                .prepare_node_record(
                    RequestKey::new("log-test", format!("request-{n}")),
                    lease(&format!("old-{n}")),
                    now_unix_ms(),
                    1024 * 1024,
                )
                .unwrap();
            state = next;
            records.push(log::Record::from(record.unwrap()));
        }
        inner
            .append_and_sync(journal::LogTip::EMPTY, records)
            .await
            .unwrap();
        let log = Arc::new(GatedLog {
            inner,
            started: Notify::new(),
            release: Notify::new(),
            fail: false,
            persist_before_error: false,
        });
        let store = Arc::new(
            Store::open_log_with_history(log.clone(), retention_limits())
                .await
                .unwrap(),
        );
        let writer = store.clone();
        let task = tokio::spawn(async move {
            writer
                .register_node_session(RequestKey::new("retained", "new"), lease("new"))
                .await
        });
        log.started.notified().await;
        let key = RequestKey::new("log-test", "request-0");
        assert!(
            store
                .read(MetaRead::RequestOutcome(key.clone()))
                .await
                .unwrap()
                .request_outcome
                .is_some()
        );
        assert!(matches!(
            store.watch(StoreRevision::ZERO, 1).await.unwrap(),
            WatchBatch::Events { .. }
        ));
        assert!(!task.is_finished());
        log.release.notify_one();
        task.await.unwrap().unwrap();
        assert!(
            store
                .read(MetaRead::RequestOutcome(key))
                .await
                .unwrap()
                .request_outcome
                .is_none(),
            "cleanup must publish with the acknowledged new decision"
        );
        assert!(matches!(
            store.watch(StoreRevision::ZERO, 1).await.unwrap(),
            WatchBatch::Compacted { .. }
        ));
    }
    #[tokio::test]
    async fn p3_retention_runtime_input_capacity_rejects_before_enqueue() {
        use journal::MetaLog;
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let mut store = Store::open_log(log.clone()).await.unwrap();
        store.pending_bytes = Arc::new(Semaphore::new(1));
        assert!(
            store
                .register_node_session(RequestKey::new("retained", "input-cap"), lease("new"))
                .await
                .is_err()
        );
        assert_eq!(log.recover().await.unwrap().tip.sequence, 0);
        assert_eq!(
            store.read_view().await.unwrap().state_for_test().revision,
            StoreRevision::ZERO
        );
        store.health().await.unwrap();
    }
    #[tokio::test]
    async fn p3_retention_runtime_cancelled_caller_keeps_input_charge_until_ack() {
        let log = Arc::new(GatedLog {
            inner: Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap()),
            started: Notify::new(),
            release: Notify::new(),
            fail: false,
            persist_before_error: false,
        });
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        let budget = store.pending_bytes.clone();
        let full = budget.available_permits();
        let writer = store.clone();
        let key = RequestKey::new("retained", "cancel");
        let caller_key = key.clone();
        let task =
            tokio::spawn(
                async move { writer.register_node_session(caller_key, lease("new")).await },
            );
        log.started.notified().await;
        let charged = budget.available_permits();
        assert!(charged < full);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            budget.available_permits(),
            charged,
            "caller cancellation cannot return writer-owned input quota"
        );
        assert!(
            store
                .in_flight
                .state
                .lock()
                .unwrap()
                .active
                .contains_key(&key),
            "cancelled queued caller must retain its request identity"
        );
        log.release.notify_one();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while budget.available_permits() != full {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            store
                .read(MetaRead::RequestOutcome(key))
                .await
                .unwrap()
                .request_outcome
                .is_some()
        );
    }
    #[tokio::test]
    async fn p3_retention_runtime_unknown_ack_stops_all_following_cleanup() {
        use journal::MetaLog;
        let inner = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let mut state = State::default();
        let mut records = Vec::new();
        for n in 0..2 {
            let (next, _, record) = state
                .prepare_node_record(
                    RequestKey::new("old", format!("r{n}")),
                    lease(&format!("old-{n}")),
                    now_unix_ms(),
                    1024 * 1024,
                )
                .unwrap();
            state = next;
            records.push(log::Record::from(record.unwrap()));
        }
        inner
            .append_and_sync(journal::LogTip::EMPTY, records)
            .await
            .unwrap();
        let log = Arc::new(GatedLog {
            inner: inner.clone(),
            started: Notify::new(),
            release: Notify::new(),
            fail: true,
            persist_before_error: true,
        });
        let store = Arc::new(
            Store::open_log_with_history(log.clone(), retention_limits())
                .await
                .unwrap(),
        );
        let old = store.read_view().await.unwrap();
        let writer = store.clone();
        let key = RequestKey::new("retained", "unknown");
        let sent = key.clone();
        let task =
            tokio::spawn(async move { writer.register_node_session(sent, lease("new")).await });
        log.started.notified().await;
        log.release.notify_one();
        assert!(task.await.unwrap().is_err());
        assert!(store.health().await.is_err());
        assert!(
            store
                .read(MetaRead::RequestOutcome(key.clone()))
                .await
                .is_err()
        );
        assert_eq!(old.state_for_test().revision, StoreRevision(2));
        assert!(
            store
                .register_node_session(RequestKey::new("retained", "later"), lease("later"))
                .await
                .is_err()
        );
        let recovered = inner.recover().await.unwrap();
        assert_eq!(
            recovered.tip.sequence, 3,
            "no cleanup may follow an unknown append result"
        );
        let mut restored = State::default();
        for record in recovered.records {
            restored = restored.replay_record(&record).unwrap();
        }
        assert!(restored.requests.contains_key(&key));
        assert_eq!(restored.history_stats().requests, 2);
        let reopened = Store::open_log_with_history(inner, retention_limits())
            .await
            .unwrap();
        assert!(matches!(
            reopened
                .register_node_session(key, lease("new"))
                .await
                .unwrap(),
            TxnOutcome::ConditionFailed {
                existing_outcome: Some(_),
                ..
            }
        ));
    }
    struct CountingLog {
        inner: local_log::LocalLog,
        commits: std::sync::atomic::AtomicUsize,
        bytes: std::sync::atomic::AtomicUsize,
    }
    impl journal::MetaLog for CountingLog {
        fn persistence(&self) -> BackendPersistence {
            self.inner.persistence()
        }
        fn limits(&self) -> journal::LogLimits {
            self.inner.limits()
        }
        fn health(&self) -> MetaFuture<'_, ()> {
            self.inner.health()
        }
        fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
            self.inner.recover()
        }
        fn append_and_sync(
            &self,
            expected: journal::LogTip,
            records: Vec<log::Record>,
        ) -> MetaFuture<'_, journal::LogTip> {
            Box::pin(async move {
                let mut tip = expected;
                let mut size = 0;
                for record in &records {
                    size += journal::frame_size(tip, record)?;
                    tip.sequence += 1;
                }
                let result = self.inner.append_and_sync(expected, records).await?;
                self.commits.fetch_add(1, Ordering::SeqCst);
                self.bytes.fetch_add(size, Ordering::SeqCst);
                Ok(result)
            })
        }
    }

    #[tokio::test]
    async fn p3_incremental_write_does_not_reencode_untouched_authority() {
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(CountingLog {
            inner: local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap(),
            commits: 0.into(),
            bytes: 0.into(),
        });
        let store = Store::open_log(log.clone()).await.unwrap();
        let request = RequestKey::new("seed", "2000-roots");
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::ActivateRoot);
        txn.conditions
            .push(TxnCondition::RequestAbsent(request.clone()));
        for number in 0..2000 {
            txn.mutations
                .push(TxnMutation::Put(MetaEntity::Root(RootRecord {
                    root_id: format!("unchanged-root-{number:04}"),
                    root_epoch: 1,
                    home_node_id: "old-home".into(),
                    home_session_id: "old-session".into(),
                    local_prepare_id: "old-prepare".into(),
                    created_at_revision: StoreRevision(1),
                    updated_at_revision: StoreRevision(1),
                })));
        }
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: request.clone(),
                operation: StoreOperation::ActivateRoot,
                result: OperationResult::Empty,
            }));
        store.compare_and_commit(txn).await.unwrap();
        log.commits.store(0, Ordering::SeqCst);
        log.bytes.store(0, Ordering::SeqCst);
        let before = std::fs::metadata(dir.path().join("wal-00000000000000000001.log"))
            .unwrap()
            .len();
        let changed = RequestKey::new("changed", "small-update");
        let outcome = store
            .register_node_session(changed.clone(), lease("new-session"))
            .await
            .unwrap();
        assert!(matches!(outcome, TxnOutcome::Committed { .. }));
        let after = std::fs::metadata(dir.path().join("wal-00000000000000000001.log"))
            .unwrap()
            .len();
        assert_eq!(log.commits.load(Ordering::SeqCst), 1);
        assert_eq!(after - before, log.bytes.load(Ordering::SeqCst) as u64);
        assert!(
            after - before <= 16 * 1024,
            "small decided change must not rewrite 2000 roots: {} bytes",
            after - before
        );
        eprintln!(
            "P3_INCREMENTAL roots=2000 commits=1 appended_bytes={}",
            after - before
        );
        let tip = log.recover().await.unwrap().tip;
        assert_eq!(tip.sequence, 2);
        drop(store);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&log) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(log);
        let reopened = Store::open_log(Arc::new(
            local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap(),
        ))
        .await
        .unwrap();
        for number in 0..2000 {
            assert!(
                reopened
                    .read(MetaRead::Root {
                        root_id: format!("unchanged-root-{number:04}")
                    })
                    .await
                    .unwrap()
                    .entity
                    .is_some()
            );
        }
        for key in [request, changed.clone()] {
            assert!(
                reopened
                    .read(MetaRead::RequestOutcome(key))
                    .await
                    .unwrap()
                    .request_outcome
                    .is_some()
            );
        }
        assert!(matches!(
            reopened
                .register_node_session(changed, lease("new-session"))
                .await
                .unwrap(),
            TxnOutcome::ConditionFailed {
                existing_outcome: Some(_),
                ..
            }
        ));
        assert_eq!(
            reopened
                .read(MetaRead::CurrentNodeSession {
                    node_id: "node-a".into()
                })
                .await
                .unwrap()
                .revision,
            StoreRevision(2)
        );
    }

    struct GatedLog {
        inner: Arc<journal::MemoryLog>,
        started: Notify,
        release: Notify,
        fail: bool,
        persist_before_error: bool,
    }
    impl journal::MetaLog for GatedLog {
        fn persistence(&self) -> BackendPersistence {
            BackendPersistence::Volatile
        }
        fn limits(&self) -> journal::LogLimits {
            self.inner.limits()
        }
        fn health(&self) -> MetaFuture<'_, ()> {
            self.inner.health()
        }
        fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
            self.inner.recover()
        }
        fn append_and_sync(
            &self,
            expected: journal::LogTip,
            records: Vec<log::Record>,
        ) -> MetaFuture<'_, journal::LogTip> {
            Box::pin(async move {
                self.started.notify_one();
                self.release.notified().await;
                if self.fail {
                    if self.persist_before_error {
                        self.inner.append_and_sync(expected, records).await?;
                    }
                    Err(unavailable("injected log sync failure"))
                } else {
                    self.inner.append_and_sync(expected, records).await
                }
            })
        }
    }
    #[tokio::test]
    async fn p3_log_holds_visibility_and_reply_until_durable_ack() {
        let log = Arc::new(GatedLog {
            inner: Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap()),
            started: Notify::new(),
            release: Notify::new(),
            fail: false,
            persist_before_error: false,
        });
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        let writer = Arc::clone(&store);
        let task = tokio::spawn(async move {
            writer
                .register_node_session(RequestKey::new("a", "gated"), lease("new"))
                .await
        });
        log.started.notified().await;
        assert!(!task.is_finished());
        assert_eq!(
            store.read_view().await.unwrap().state_for_test().revision,
            StoreRevision::ZERO
        );
        log.release.notify_one();
        assert!(task.await.unwrap().is_ok());
        assert_eq!(
            store.read_view().await.unwrap().state_for_test().revision,
            StoreRevision(1)
        );
        assert_eq!(log.recover().await.unwrap().tip.sequence, 1);
    }
    #[tokio::test]
    async fn p3_log_failed_ack_poisons_store_and_does_not_publish() {
        let log = Arc::new(GatedLog {
            inner: Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap()),
            started: Notify::new(),
            release: Notify::new(),
            fail: true,
            persist_before_error: false,
        });
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        let old = store.read_view().await.unwrap();
        let writer = Arc::clone(&store);
        let task = tokio::spawn(async move {
            writer
                .register_node_session(RequestKey::new("a", "failed"), lease("new"))
                .await
        });
        log.started.notified().await;
        log.release.notify_one();
        assert!(task.await.unwrap().is_err());
        assert_eq!(old.state_for_test().revision, StoreRevision::ZERO);
        assert_eq!(store.visible.committed.load().revision, StoreRevision::ZERO);
        assert!(store.read_view().await.is_err());
        assert!(store.health().await.is_err());
        assert!(
            store
                .register_node_session(RequestKey::new("a", "later"), lease("later"))
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn p3_log_groups_ready_requests_preserving_all_outcomes_and_events() {
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let store = Store::open_log(log.clone()).await.unwrap();
        let a = RequestKey::new("a", "one");
        let b = RequestKey::new("a", "two");
        let (first, second) = tokio::join!(
            store.register_node_session(a.clone(), lease("one")),
            store.register_node_session(b.clone(), lease("two"))
        );
        assert!(matches!(
            first.unwrap(),
            TxnOutcome::Committed {
                revision: StoreRevision(1),
                ..
            }
        ));
        assert!(matches!(
            second.unwrap(),
            TxnOutcome::Committed {
                revision: StoreRevision(2),
                ..
            }
        ));
        let original = store
            .committed()
            .await
            .unwrap()
            .snapshot_bytes_with_batch(0)
            .unwrap();
        assert_eq!(log.recover().await.unwrap().records.len(), 2);
        assert!(matches!(
            store.register_node_session(a, lease("one")).await.unwrap(),
            TxnOutcome::ConditionFailed { .. }
        ));
        assert_eq!(log.recover().await.unwrap().records.len(), 2);
        let replayed = Store::open_log(log).await.unwrap();
        assert_eq!(
            replayed
                .committed()
                .await
                .unwrap()
                .snapshot_bytes_with_batch(0)
                .unwrap(),
            original
        );
    }

    #[tokio::test]
    async fn p3_log_unknown_ack_stops_old_store_and_reopens_exact_outcome() {
        let log = Arc::new(GatedLog {
            inner: Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap()),
            started: Notify::new(),
            release: Notify::new(),
            fail: true,
            persist_before_error: true,
        });
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        let writer = Arc::clone(&store);
        let request = RequestKey::new("unknown", "once");
        let original = request.clone();
        let task =
            tokio::spawn(async move { writer.register_node_session(original, lease("one")).await });
        log.started.notified().await;
        log.release.notify_one();
        assert!(task.await.unwrap().is_err());
        assert_eq!(store.visible.committed.load().revision, StoreRevision::ZERO);
        assert!(
            store
                .register_node_session(RequestKey::new("later", "must-not-pass"), lease("two"))
                .await
                .is_err()
        );
        assert_eq!(log.inner.recover().await.unwrap().tip.sequence, 1);
        let reopened = Store::open_log(log.inner.clone()).await.unwrap();
        let before = reopened
            .read(MetaRead::RequestOutcome(request.clone()))
            .await
            .unwrap()
            .request_outcome
            .unwrap();
        assert!(
            matches!(reopened.register_node_session(request,lease("one")).await.unwrap(),TxnOutcome::ConditionFailed {existing_outcome:Some(ref old),..} if old==&before)
        );
        assert_eq!(
            reopened
                .committed()
                .await
                .unwrap()
                .node_epochs
                .get("node-a"),
            Some(&1)
        );
        assert_eq!(
            reopened.committed().await.unwrap().revision,
            StoreRevision(1)
        );
        assert_eq!(log.inner.recover().await.unwrap().tip.sequence, 1);
    }
    #[tokio::test]
    async fn p3_all_condition_variants_keep_truth_idempotence_and_wal_admission() {
        let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        for (node, session) in [("a", "s"), ("b", "t")] {
            store
                .register_node_session(
                    RequestKey::new(node, "register"),
                    NodeSessionLease {
                        node_id: node.into(),
                        session_id: session.into(),
                        grpc_addr: "g".into(),
                        data_addr: "d".into(),
                        rest_addr: "r".into(),
                        storage_devices: Vec::new(),
                        lease_ttl: Duration::from_secs(300),
                    },
                )
                .await
                .unwrap();
        }
        let seed_request = RequestKey::new("seed", "entities");
        let mut bootstrap = MetaTxn::new(seed_request.clone(), StoreOperation::DfsCreate);
        bootstrap
            .conditions
            .push(TxnCondition::RequestAbsent(seed_request.clone()));
        let mut entities = codec::tests::entity_fixtures();
        entities.retain(|entity| !matches!(entity, MetaEntity::NodeSession(_)));
        for entity in &mut entities {
            if let MetaEntity::RootCommandAck(ack) = entity {
                ack.success = true;
            }
        }
        bootstrap
            .mutations
            .extend(entities.iter().cloned().map(TxnMutation::Put));
        bootstrap
            .mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: seed_request.clone(),
                operation: bootstrap.operation,
                result: OperationResult::Empty,
            }));
        assert!(matches!(
            store.compare_and_commit(bootstrap).await.unwrap(),
            TxnOutcome::Committed {
                revision: StoreRevision(3),
                ..
            }
        ));
        let root = entities
            .iter()
            .find_map(|entity| match entity {
                MetaEntity::Root(root) => Some(root.clone()),
                _ => None,
            })
            .unwrap();
        let inode = entities
            .iter()
            .find_map(|entity| match entity {
                MetaEntity::DfsInode(inode) => Some(inode.clone()),
                _ => None,
            })
            .unwrap();
        let root_key = MetaKey::Root {
            root_id: "r".into(),
        };
        let absent_key = MetaKey::Root {
            root_id: "absent".into(),
        };
        let mut changed_root = root.clone();
        changed_root.root_epoch += 1;
        // Expected truth is fixed by this fixture, not derived from the new
        // predicate. The unchanged P2 predicate is independently hash-compared.
        let cases = vec![
            (true, TxnCondition::Missing(absent_key.clone())),
            (false, TxnCondition::Missing(root_key.clone())),
            (
                true,
                TxnCondition::RevisionEquals {
                    key: root_key.clone(),
                    revision: StoreRevision(3),
                },
            ),
            (
                false,
                TxnCondition::RevisionEquals {
                    key: root_key.clone(),
                    revision: StoreRevision(4),
                },
            ),
            (
                false,
                TxnCondition::RevisionEquals {
                    key: absent_key,
                    revision: StoreRevision(3),
                },
            ),
            (true, TxnCondition::EntityEquals(MetaEntity::Root(root))),
            (
                false,
                TxnCondition::EntityEquals(MetaEntity::Root(changed_root)),
            ),
            (
                true,
                TxnCondition::NodeSessionCurrent {
                    node_id: "a".into(),
                    session_id: "s".into(),
                },
            ),
            (
                false,
                TxnCondition::NodeSessionCurrent {
                    node_id: "a".into(),
                    session_id: "stale".into(),
                },
            ),
            (
                true,
                TxnCondition::RootEpochEquals {
                    root_id: "r".into(),
                    root_epoch: 1,
                },
            ),
            (
                false,
                TxnCondition::RootEpochEquals {
                    root_id: "r".into(),
                    root_epoch: 2,
                },
            ),
            (
                true,
                TxnCondition::RootCommandAcked {
                    command_id: "c".into(),
                    node_id: "a".into(),
                    session_id: "s".into(),
                },
            ),
            (
                false,
                TxnCondition::RootCommandAcked {
                    command_id: "c".into(),
                    node_id: "a".into(),
                    session_id: "stale".into(),
                },
            ),
            (
                true,
                TxnCondition::RootCommandMatches {
                    command_id: "c".into(),
                    home_node_id: "a".into(),
                    home_session_id: "s".into(),
                    root_id: "r".into(),
                    root_epoch: 1,
                    old_access_generation: 2,
                    command_type: RootCommandType::InvalidateCache,
                },
            ),
            (
                false,
                TxnCondition::RootCommandMatches {
                    command_id: "c".into(),
                    home_node_id: "a".into(),
                    home_session_id: "s".into(),
                    root_id: "r".into(),
                    root_epoch: 1,
                    old_access_generation: 3,
                    command_type: RootCommandType::InvalidateCache,
                },
            ),
            (
                true,
                TxnCondition::RootCommandAckedExact {
                    command_id: "c".into(),
                    node_id: "a".into(),
                    session_id: "s".into(),
                    root_id: "r".into(),
                    root_epoch: 1,
                    access_generation: 2,
                    command_type: RootCommandType::InvalidateCache,
                },
            ),
            (
                false,
                TxnCondition::RootCommandAckedExact {
                    command_id: "c".into(),
                    node_id: "a".into(),
                    session_id: "s".into(),
                    root_id: "r".into(),
                    root_epoch: 2,
                    access_generation: 2,
                    command_type: RootCommandType::InvalidateCache,
                },
            ),
            (
                false,
                TxnCondition::RootCommandAckedExact {
                    command_id: "c".into(),
                    node_id: "a".into(),
                    session_id: "s".into(),
                    root_id: "r".into(),
                    root_epoch: 1,
                    access_generation: 2,
                    command_type: RootCommandType::RevokeAccess,
                },
            ),
            (
                true,
                TxnCondition::RequestAbsent(RequestKey::new("never", "used")),
            ),
            (false, TxnCondition::RequestAbsent(seed_request)),
            (
                true,
                TxnCondition::DfsDirectoryEmpty {
                    namespace_id: NamespaceId("n".into()),
                    parent_inode_id: InodeId("empty".into()),
                },
            ),
            (
                false,
                TxnCondition::DfsDirectoryEmpty {
                    namespace_id: NamespaceId("n".into()),
                    parent_inode_id: InodeId("p".into()),
                },
            ),
            (
                true,
                TxnCondition::DfsNotDescendant {
                    namespace_id: NamespaceId("other".into()),
                    ancestor_inode_id: InodeId("p".into()),
                    child_inode_id: InodeId("i".into()),
                },
            ),
            (
                false,
                TxnCondition::DfsNotDescendant {
                    namespace_id: NamespaceId("n".into()),
                    ancestor_inode_id: InodeId("p".into()),
                    child_inode_id: InodeId("i".into()),
                },
            ),
            (
                false,
                TxnCondition::DfsNotDescendant {
                    namespace_id: NamespaceId("n".into()),
                    ancestor_inode_id: InodeId("i".into()),
                    child_inode_id: InodeId("i".into()),
                },
            ),
        ];
        assert_eq!(cases.len(), 25);
        let meta = crate::meta::Meta::with_store(
            "condition-test".into(),
            crate::runtime::Observability::new().unwrap(),
            store.clone(),
        );
        for (index, (expected, condition)) in cases.into_iter().enumerate() {
            let before = store.committed().await.unwrap();
            assert_eq!(
                condition_matches(&before, &condition),
                expected,
                "{condition:?}"
            );
            let before_bytes = before.snapshot_bytes_with_batch(0).unwrap();
            let tip = log.recover().await.unwrap().tip;
            let request = RequestKey::new("conditions", format!("case-{index}"));
            let original = RequestOutcome {
                request: request.clone(),
                operation: StoreOperation::DfsCreate,
                result: OperationResult::DfsInode(inode.clone()),
            };
            let mut txn = MetaTxn::new(request.clone(), original.operation);
            txn.conditions
                .extend([TxnCondition::RequestAbsent(request.clone()), condition]);
            txn.mutations
                .push(TxnMutation::RecordRequestOutcome(original.clone()));
            let result = store.compare_and_commit(txn).await.unwrap();
            if !expected {
                assert!(matches!(
                    result,
                    TxnOutcome::ConditionFailed {
                        existing_outcome: None,
                        ..
                    }
                ));
                assert_eq!(
                    store
                        .committed()
                        .await
                        .unwrap()
                        .snapshot_bytes_with_batch(0)
                        .unwrap(),
                    before_bytes
                );
                assert_eq!(log.recover().await.unwrap().tip, tip);
                assert!(
                    store
                        .read(MetaRead::RequestOutcome(request))
                        .await
                        .unwrap()
                        .request_outcome
                        .is_none()
                );
                continue;
            }
            assert_eq!(
                result,
                TxnOutcome::Committed {
                    revision: StoreRevision(before.revision.0 + 1),
                    outcome: original.clone()
                }
            );
            let committed = store
                .committed()
                .await
                .unwrap()
                .snapshot_bytes_with_batch(0)
                .unwrap();
            let committed_tip = log.recover().await.unwrap().tip;
            assert_eq!(committed_tip.sequence, tip.sequence + 1);
            let mut retry = MetaTxn::new(request.clone(), original.operation);
            retry
                .conditions
                .push(TxnCondition::RequestAbsent(request.clone()));
            retry
                .mutations
                .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                    result: OperationResult::Empty,
                    ..original.clone()
                }));
            assert!(
                matches!(store.compare_and_commit(retry).await.unwrap(), TxnOutcome::ConditionFailed { existing_outcome: Some(saved), .. } if saved == original)
            );
            let operation = StoreOperation::DfsUnlink;
            let mut wrong = MetaTxn::new(request.clone(), operation);
            wrong
                .conditions
                .push(TxnCondition::RequestAbsent(request.clone()));
            wrong
                .mutations
                .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request: request.clone(),
                    operation,
                    result: OperationResult::Empty,
                }));
            // Store preserves P2's typed rejection and original outcome;
            // domain callers must not treat a different operation as replay.
            assert!(matches!(store.compare_and_commit(wrong).await.unwrap(),
                TxnOutcome::ConditionFailed { existing_outcome: Some(saved), .. } if saved == original));
            let error = meta
                .register_node(
                    request,
                    NodeSessionLease {
                        node_id: "must-not-register".into(),
                        session_id: "must-not-appear".into(),
                        grpc_addr: "g".into(),
                        data_addr: "d".into(),
                        rest_addr: "r".into(),
                        storage_devices: Vec::new(),
                        lease_ttl: Duration::from_secs(300),
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), afs_error::META_CATALOG_INVALID_REQUEST);
            assert_eq!(log.recover().await.unwrap().tip, committed_tip);
            assert_eq!(
                store
                    .committed()
                    .await
                    .unwrap()
                    .snapshot_bytes_with_batch(0)
                    .unwrap(),
                committed
            );
            store.health().await.unwrap();
        }
    }

    #[tokio::test]
    async fn p3_root_deactivation_accepts_accumulated_grants_below_byte_budget() {
        use crate::meta::owner_roots::{
            DeactivateRootInput, OwnerRootAuthority, StoreOwnerRootAuthority,
        };

        const OLD_GRANTS: usize = 65_536;
        const SEED_BATCH: usize = 4096;
        let log = Arc::new(journal::MemoryLog::new(Default::default()).unwrap());
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        store
            .register_node_session(
                RequestKey::new("a", "register"),
                NodeSessionLease {
                    node_id: "a".into(),
                    session_id: "s".into(),
                    grpc_addr: "g".into(),
                    data_addr: "d".into(),
                    rest_addr: "r".into(),
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(300),
                },
            )
            .await
            .unwrap();
        let mut template = grant();
        template.root_id = "r".into();
        template.root_epoch = 1;
        template.home_node_id = "a".into();
        template.home_session_id = "s".into();
        template.holder_node_id = "b".into();
        let mut home = template.clone();
        home.holder_node_id = "a".into();
        home.holder_session_id = "s".into();
        for batch in 0..OLD_GRANTS / SEED_BATCH {
            let request = RequestKey::new("fixture", format!("grant-batch-{batch}"));
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::AcquireRoot);
            txn.conditions
                .push(TxnCondition::RequestAbsent(request.clone()));
            if batch == 0 {
                txn.mutations.extend([
                    TxnMutation::Put(MetaEntity::Root(RootRecord {
                        root_id: "r".into(),
                        root_epoch: 1,
                        home_node_id: "a".into(),
                        home_session_id: "s".into(),
                        local_prepare_id: "p".into(),
                        created_at_revision: StoreRevision(2),
                        updated_at_revision: StoreRevision(2),
                    })),
                    TxnMutation::Put(MetaEntity::RootGrant(home.clone())),
                ]);
            }
            // Past holder sessions can legitimately leave distinct durable
            // grants. Deactivation fences them atomically; physical deletion is deferred.
            for index in batch * SEED_BATCH..(batch + 1) * SEED_BATCH {
                let mut value = template.clone();
                value.holder_session_id = format!("old-{index:05}");
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::RootGrant(value)));
            }
            txn.mutations
                .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request,
                    operation: txn.operation,
                    result: OperationResult::Empty,
                }));
            assert!(matches!(
                store.compare_and_commit(txn).await.unwrap(),
                TxnOutcome::Committed { .. }
            ));
        }
        let before = store.committed().await.unwrap();
        let grants = before
            .read(MetaRead::RootGrants {
                root_id: "r".into(),
            })
            .unwrap();
        assert_eq!(grants.entities.len(), OLD_GRANTS + 1);
        let keys_bytes: usize = grants
            .entities
            .iter()
            .map(|entity| codec::encoded_size(&entity_key(entity)).unwrap())
            .sum();
        // This is a lower-level byte account, not a claimed framed record size.
        assert!(keys_bytes < log.limits().record_max_bytes / 4);
        let tip = log.recover().await.unwrap().tip;
        let authority = StoreOwnerRootAuthority::new(store.clone());
        let result = authority
            .deactivate_root(DeactivateRootInput {
                request_id: "deactivate".into(),
                root_id: "r".into(),
                root_epoch: 1,
                home_node_id: "a".into(),
                home_session_id: "s".into(),
                reason: "finished".into(),
                authenticated_home_node_id: Some("a".into()),
            })
            .await;
        if let Err(error) = &result {
            let after = store.committed().await.unwrap();
            assert_eq!(after.revision, before.revision);
            assert!(Arc::ptr_eq(&after, &before));
            assert_eq!(log.recover().await.unwrap().tip, tip);
            store.health().await.unwrap();
            eprintln!(
                "P3_GRANT_BOUND grants={} delete_key_bytes={keys_bytes} record_limit={} rejected_without_mutation=true error={error}",
                grants.entities.len(),
                log.limits().record_max_bytes
            );
        }
        result.expect(
            "supported atomic root deactivation must not acquire an undocumented count limit",
        );
        let after = store.committed().await.unwrap();
        assert!(after.revision.0 > before.revision.0);
        assert!(
            after
                .read(MetaRead::Root {
                    root_id: "r".into()
                })
                .unwrap()
                .entity
                .is_none()
        );
        assert!(
            !after
                .read(MetaRead::RootGrants {
                    root_id: "r".into()
                })
                .unwrap()
                .entities
                .is_empty()
        );
        let recovered = log.recover().await.unwrap();
        let record = recovered
            .records
            .iter()
            .find(|record| {
                matches!(record, log::Record::Decision { .. })
                    && record.revision_before() == before.revision
                    && record.revision_after() == before.revision.next()
            })
            .unwrap();
        let log::Record::Decision { decision, .. } = record else {
            unreachable!()
        };
        assert_eq!(
            decision.facts().unwrap().operation,
            StoreOperation::DeactivateRoot
        );
        let framed = journal::frame_size(tip, record).unwrap();
        assert!(
            framed < 4096,
            "logical deactivation must not scale with stored grants"
        );
        drop(authority);
        drop(store);
        tokio::time::timeout(Duration::from_secs(2), async {
            while Arc::strong_count(&log) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let reopened = Store::open_log(log.clone()).await.unwrap();
        let reopened = reopened.committed().await.unwrap();
        assert!(reopened.revision >= after.revision);
        assert!(
            !reopened
                .read(MetaRead::RootGrants {
                    root_id: "r".into()
                })
                .unwrap()
                .entities
                .is_empty()
        );
        let request = RequestKey::new("a/s", "deactivate");
        assert_eq!(
            reopened.requests.get(&request),
            after.requests.get(&request)
        );
        eprintln!(
            "P3_GRANT_BOUND_GREEN grants={} delete_key_bytes={keys_bytes} framed_bytes={framed} record_limit={} one_business_revision=true replayed=true",
            grants.entities.len(),
            log.limits().record_max_bytes
        );
    }

    #[tokio::test]
    async fn p3_a27_typed_byte_limits_reject_atomically_and_reopen_controls() {
        use crate::dfs::{
            ChunkEncoding, ChunkId, ContentDigest, CopyId, CopyLocation, CopyRole, CopyState,
            DigestAlgorithm, Extent,
        };
        use std::collections::BTreeMap;

        const REJECT_BYTES: usize = 4096;
        const CONTROL_BYTES: usize = 65536;
        fn limits(record_max_bytes: usize) -> journal::LogLimits {
            journal::LogLimits {
                record_max_bytes,
                batch_max_bytes: 131072,
                wal_max_bytes: 1048576,
                ..Default::default()
            }
        }
        fn files(path: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
            std::fs::read_dir(path)
                .unwrap()
                .map(|entry| {
                    let entry = entry.unwrap();
                    (
                        entry.file_name().into_string().unwrap(),
                        std::fs::read(entry.path()).unwrap(),
                    )
                })
                .collect()
        }
        let digest = ContentDigest {
            algorithm: DigestAlgorithm::Blake3,
            bytes: [3; 32],
        };
        let chunk = MetaEntity::DfsChunk(ChunkObject {
            id: ChunkId::new("chunk"),
            length: 4096,
            content_digest: digest.clone(),
            encoding: ChunkEncoding::Raw,
        });
        let layout = vec![
            chunk.clone(),
            MetaEntity::DfsLayoutRoot(LayoutRoot {
                id: LayoutRootId::new("layout"),
                file_length: 512 * 4096,
                inline_extents: (0..512)
                    .map(|number| Extent {
                        file_offset: number * 4096,
                        length: 4096,
                        chunk_id: ChunkId::new("chunk"),
                        chunk_offset: 0,
                    })
                    .collect(),
            }),
            MetaEntity::DfsFileVersion(FileVersion {
                id: FileVersionId::new("version"),
                inode_id: InodeId::new("inode"),
                parent_version: None,
                length: 512 * 4096,
                layout_root: LayoutRootId::new("layout"),
                created_at_unix_ms: 1,
            }),
        ];
        let grants = (0..64)
            .map(|number| {
                let mut value = grant();
                value.holder_session_id = format!("holder-{number}");
                value.issued_at_revision = StoreRevision(1);
                MetaEntity::RootGrant(value)
            })
            .collect::<Vec<_>>();
        let mut copies = vec![chunk];
        copies.extend((0..128).map(|number| {
            MetaEntity::DfsCopy(CopyRecord {
                id: CopyId::new(format!("copy-{number}")),
                chunk_id: ChunkId::new("chunk"),
                role: CopyRole::DurableReplica,
                location: CopyLocation::Node {
                    node_id: format!("node-{number}"),
                    node_epoch: 1,
                    device_id: "device".into(),
                    device_epoch: 1,
                    catalog_revision: 1,
                },
                state: CopyState::Ready,
                persisted_bytes: 4096,
                verified_digest: digest.clone(),
            })
        }));
        // Typed Store fixtures test persistence, not placement/receipt policy.
        for (name, entities) in [("layout", layout), ("grants", grants), ("copies", copies)] {
            let request = RequestKey::new("a27", name);
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsCommitFileVersion);
            txn.conditions
                .push(TxnCondition::RequestAbsent(request.clone()));
            txn.mutations
                .extend(entities.into_iter().map(TxnMutation::Put));
            txn.mutations
                .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request: request.clone(),
                    operation: txn.operation,
                    result: OperationResult::Empty,
                }));
            let (_, _, record) = State::default()
                .prepare_txn_record(txn.clone(), 1, CONTROL_BYTES)
                .unwrap();
            let framed =
                journal::frame_size(journal::LogTip::EMPTY, &log::Record::from(record.unwrap()))
                    .unwrap();
            assert!(
                framed > REJECT_BYTES && framed < CONTROL_BYTES,
                "{name}: {framed}"
            );
            let rejected_dir = tempfile::tempdir().unwrap();
            let rejected_log = Arc::new(
                local_log::LocalLog::open(rejected_dir.path(), limits(REJECT_BYTES)).unwrap(),
            );
            let rejected = Store::open_log(rejected_log.clone()).await.unwrap();
            let before = rejected.committed().await.unwrap();
            let before_files = files(rejected_dir.path());
            for _ in 0..2 {
                let error = rejected.compare_and_commit(txn.clone()).await.unwrap_err();
                assert!(error.to_string().contains("byte"), "{name}: {error}");
                assert!(Arc::ptr_eq(&before, &rejected.committed().await.unwrap()));
                assert_eq!(
                    rejected_log.recover().await.unwrap().tip,
                    journal::LogTip::EMPTY
                );
                assert_eq!(files(rejected_dir.path()), before_files);
                assert!(
                    !rejected
                        .committed()
                        .await
                        .unwrap()
                        .requests
                        .contains_key(&request)
                );
                rejected.health().await.unwrap();
            }
            rejected
                .register_node_session(RequestKey::new("a27", "after-reject"), lease("small"))
                .await
                .unwrap();
            assert_eq!(
                rejected.committed().await.unwrap().revision,
                StoreRevision(1)
            );
            drop(rejected);
            tokio::time::timeout(Duration::from_secs(5), async {
                while Arc::strong_count(&rejected_log) > 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            drop(rejected_log);

            let control_dir = tempfile::tempdir().unwrap();
            let control_log = Arc::new(
                local_log::LocalLog::open(control_dir.path(), limits(CONTROL_BYTES)).unwrap(),
            );
            let control = Store::open_log(control_log.clone()).await.unwrap();
            assert!(matches!(
                control.compare_and_commit(txn.clone()).await.unwrap(),
                TxnOutcome::Committed {
                    revision: StoreRevision(1),
                    ..
                }
            ));
            let original = control
                .committed()
                .await
                .unwrap()
                .snapshot_bytes_with_batch(0)
                .unwrap();
            let tip = control_log.recover().await.unwrap().tip;
            assert!(matches!(
                control.compare_and_commit(txn).await.unwrap(),
                TxnOutcome::ConditionFailed {
                    existing_outcome: Some(_),
                    ..
                }
            ));
            assert_eq!(control_log.recover().await.unwrap().tip, tip);
            drop(control);
            tokio::time::timeout(Duration::from_secs(5), async {
                while Arc::strong_count(&control_log) > 1 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            drop(control_log);
            let reopened_log = Arc::new(
                local_log::LocalLog::open(control_dir.path(), limits(CONTROL_BYTES)).unwrap(),
            );
            let reopened = Store::open_log(reopened_log).await.unwrap();
            assert_eq!(
                reopened
                    .committed()
                    .await
                    .unwrap()
                    .snapshot_bytes_with_batch(0)
                    .unwrap(),
                original
            );
            eprintln!(
                "P3_A27_TYPED name={name} framed_bytes={framed} reject_limit={REJECT_BYTES} control_limit={CONTROL_BYTES} no_append_or_publish=true actual_LocalLog_reopen=true"
            );
        }
    }

    #[tokio::test]
    async fn p3_a27_ready_group_keeps_failed_requests_out_of_durable_authority() {
        let directory = tempfile::tempdir().unwrap();
        let backend = Arc::new(CountingLog {
            inner: local_log::LocalLog::open(
                directory.path(),
                journal::LogLimits {
                    record_max_bytes: 4096,
                    batch_max_bytes: 131072,
                    wal_max_bytes: 1048576,
                    ..Default::default()
                },
            )
            .unwrap(),
            commits: 0.into(),
            bytes: 0.into(),
        });
        let store = Store::open_log(backend.clone()).await.unwrap();
        let root = MetaEntity::Root(RootRecord {
            root_id: "rejected-root".into(),
            root_epoch: 1,
            home_node_id: "a".into(),
            home_session_id: "s".into(),
            local_prepare_id: "p".into(),
            created_at_revision: StoreRevision(1),
            updated_at_revision: StoreRevision(1),
        });
        let make_txn = |name: &str| {
            let request = RequestKey::new("a27-group", name);
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::ActivateRoot);
            txn.conditions
                .push(TxnCondition::RequestAbsent(request.clone()));
            txn.mutations.push(TxnMutation::Put(root.clone()));
            txn.mutations
                .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request,
                    operation: txn.operation,
                    result: OperationResult::Empty,
                }));
            txn
        };
        let mut invalid_apply = make_txn("apply-invalid");
        invalid_apply.mutations.pop(); // apply_txn must reject a missing result.
        let mut invalid_encoding = make_txn("encode-invalid");
        let mut nested = OperationResult::Empty;
        for _ in 0..129 {
            nested = OperationResult::DfsNamespace {
                request_digest: [1; 32],
                result: Box::new(nested),
            };
        }
        if let Some(TxnMutation::RecordRequestOutcome(outcome)) =
            invalid_encoding.mutations.last_mut()
        {
            outcome.result = nested;
        } else {
            panic!("fixture must end with its outcome");
        }
        let mut false_condition = make_txn("condition-false");
        false_condition
            .conditions
            .push(TxnCondition::Missing(MetaKey::CurrentNodeSession {
                node_id: "node-a".into(),
            }));
        let mut large = lease("too-large");
        large.grpc_addr = "x".repeat(8192);
        let mut last = lease("last");
        last.node_id = "node-b".into();
        // Current-thread runtime plus biased polling queues the six operations
        // before the writer runs. Assert the actual physical batch, not timing.
        let (first, apply, encoding, oversize, condition, last) = tokio::join!(biased;
            store.register_node_session(RequestKey::new("a27-group", "first"), lease("first")),
            store.compare_and_commit(invalid_apply),
            store.compare_and_commit(invalid_encoding),
            store.register_node_session(RequestKey::new("a27-group", "too-large"), large),
            store.compare_and_commit(false_condition),
            store.register_node_session(RequestKey::new("a27-group", "last"), last),
        );
        assert!(matches!(
            first.unwrap(),
            TxnOutcome::Committed {
                revision: StoreRevision(1),
                ..
            }
        ));
        assert!(
            apply
                .unwrap_err()
                .to_string()
                .contains("matching request outcome")
        );
        assert!(encoding.unwrap_err().to_string().contains("canonical"));
        assert!(oversize.unwrap_err().to_string().contains("byte"));
        assert!(matches!(
            condition.unwrap(),
            TxnOutcome::ConditionFailed {
                revision: StoreRevision(1),
                existing_outcome: None
            }
        ));
        assert!(matches!(
            last.unwrap(),
            TxnOutcome::Committed {
                revision: StoreRevision(2),
                ..
            }
        ));
        store.health().await.unwrap();
        let state = store.committed().await.unwrap();
        assert_eq!(state.revision, StoreRevision(2));
        assert!(!state.entities.contains_key(&MetaKey::Root {
            root_id: "rejected-root".into()
        }));
        for name in [
            "apply-invalid",
            "encode-invalid",
            "too-large",
            "condition-false",
        ] {
            assert!(
                !state
                    .requests
                    .contains_key(&RequestKey::new("a27-group", name))
            );
        }
        assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
        let recovered = backend.recover().await.unwrap();
        assert_eq!(recovered.records.len(), 2);
        assert_eq!(recovered.tip.sequence, 2);
        let original = state.snapshot_bytes_with_batch(0).unwrap();
        drop(state);
        drop(store);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&backend) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(backend);
        let reopened = Store::open_log(Arc::new(
            local_log::LocalLog::open(
                directory.path(),
                journal::LogLimits {
                    record_max_bytes: 4096,
                    batch_max_bytes: 131072,
                    wal_max_bytes: 1048576,
                    ..Default::default()
                },
            )
            .unwrap(),
        ))
        .await
        .unwrap();
        assert_eq!(
            reopened
                .committed()
                .await
                .unwrap()
                .snapshot_bytes_with_batch(0)
                .unwrap(),
            original
        );
        eprintln!(
            "P3_A27_GROUP ready=6 decided=2 physical_appends=1 apply_rejected=1 canonical_rejected=1 oversized=1 false_condition=1 no_failed_authority=true actual_LocalLog_reopen=true"
        );
    }

    #[tokio::test]
    async fn p3_log_failed_condition_and_oversize_do_not_append_or_publish() {
        let limits = journal::LogLimits {
            record_max_bytes: 1024,
            batch_max_bytes: 1024,
            wal_max_bytes: 32 * 1024,
            ..Default::default()
        };
        let log = Arc::new(journal::MemoryLog::new(limits).unwrap());
        let store = Store::open_log(log.clone()).await.unwrap();
        store
            .register_node_session(RequestKey::new("seed", "one"), lease("one"))
            .await
            .unwrap();
        let tip = log.recover().await.unwrap().tip;
        let request = RequestKey::new("condition", "false");
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::ActivateRoot);
        txn.conditions
            .push(TxnCondition::RequestAbsent(request.clone()));
        txn.conditions
            .push(TxnCondition::Missing(MetaKey::CurrentNodeSession {
                node_id: "node-a".into(),
            }));
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation: StoreOperation::ActivateRoot,
                result: OperationResult::Empty,
            }));
        assert!(matches!(
            store.compare_and_commit(txn).await.unwrap(),
            TxnOutcome::ConditionFailed {
                existing_outcome: None,
                ..
            }
        ));
        let mut large = lease("oversize");
        large.grpc_addr = "x".repeat(8192);
        assert!(
            store
                .register_node_session(RequestKey::new("large", "rejected"), large)
                .await
                .is_err()
        );
        assert_eq!(log.recover().await.unwrap().tip, tip);
        assert_eq!(store.committed().await.unwrap().revision, StoreRevision(1));
        store.health().await.unwrap();
        assert!(matches!(
            store
                .register_node_session(RequestKey::new("small", "after-rejection"), lease("two"))
                .await
                .unwrap(),
            TxnOutcome::Committed {
                revision: StoreRevision(2),
                ..
            }
        ));
    }
    #[tokio::test]
    async fn p3_log_byte_limit_splits_ready_group_without_losing_order() {
        let dir = tempfile::tempdir().unwrap();
        let limits = journal::LogLimits {
            record_max_bytes: 1024,
            batch_max_bytes: 1024,
            wal_max_bytes: 32 * 1024,
            ..Default::default()
        };
        let log = Arc::new(CountingLog {
            inner: local_log::LocalLog::open(dir.path(), limits).unwrap(),
            commits: 0.into(),
            bytes: 0.into(),
        });
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        let mut tasks = tokio::task::JoinSet::new();
        for number in 0..8 {
            let store = Arc::clone(&store);
            tasks.spawn(async move {
                store
                    .register_node_session(
                        RequestKey::new("split", format!("{number}")),
                        lease(&format!("session-{number}")),
                    )
                    .await
            });
        }
        while let Some(result) = tasks.join_next().await {
            assert!(matches!(
                result.unwrap().unwrap(),
                TxnOutcome::Committed { .. }
            ));
        }
        assert!(log.commits.load(Ordering::SeqCst) > 1);
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.records.len(), 8);
        assert_eq!(recovered.tip.sequence, 8);
        let replayed = Store::open_log(log).await.unwrap();
        assert_eq!(
            store
                .committed()
                .await
                .unwrap()
                .snapshot_bytes_with_batch(0)
                .unwrap(),
            replayed
                .committed()
                .await
                .unwrap()
                .snapshot_bytes_with_batch(0)
                .unwrap()
        );
    }

    #[tokio::test]
    async fn batch_observability_preserves_group_commit_replay() {
        let commits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let last_record_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let backend = Arc::new(CountingBackend {
            inner: journal::MemoryLog::new(Default::default()).unwrap(),
            commits: Arc::clone(&commits),
            last_record_bytes: Arc::clone(&last_record_bytes),
        });
        let store = Store::open_log(backend.clone()).await.unwrap();
        let first_request = RequestKey::new("node-a", "batch-observe-1");
        let second_request = RequestKey::new("node-a", "batch-observe-2");

        let (first, second) = tokio::join!(
            store.register_node_session(first_request.clone(), lease("session-1")),
            store.register_node_session(second_request.clone(), lease("session-2")),
        );
        assert!(matches!(first.unwrap(), TxnOutcome::Committed { .. }));
        assert!(matches!(second.unwrap(), TxnOutcome::Committed { .. }));
        assert_eq!(
            commits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "concurrent mutations should stay one physical batch after instrumentation"
        );
        assert!(
            last_record_bytes.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "instrumented canonical batch must still produce framed bytes"
        );

        drop(store);
        let reopened = Store::open_log(backend).await.unwrap();
        for request in [first_request, second_request] {
            let snapshot = reopened
                .read(MetaRead::RequestOutcome(request))
                .await
                .unwrap();
            assert!(snapshot.request_outcome.is_some());
        }
    }

    // Installs only a test scheduler gate; it changes no authority fact.
    async fn install_read_pause(store: &Store, pause: Arc<state::ReadPause>) {
        let mut state = store.committed().await.unwrap().as_ref().clone();
        state.read_pause = Some(pause);
        store.visible.committed.store(Arc::new(state));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn p3_two_readers_and_writer_progress_while_one_reader_is_paused() {
        let store = Arc::new(
            Store::open_log(Arc::new(
                journal::MemoryLog::new(Default::default()).unwrap(),
            ))
            .await
            .unwrap(),
        );
        store
            .register_node_session(RequestKey::new("a", "initial"), lease("session-1"))
            .await
            .unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let pause = Arc::new(state::ReadPause::new(entered_tx));
        install_read_pause(&store, Arc::clone(&pause)).await;
        let runtime = tokio::runtime::Handle::current();
        let first_store = Arc::clone(&store);
        let first_runtime = runtime.clone();
        let first = std::thread::spawn(move || {
            let id = std::thread::current().id();
            (
                id,
                first_runtime
                    .block_on(first_store.read(MetaRead::CurrentNodeSessions))
                    .unwrap(),
            )
        });
        let entered = entered_rx.recv_timeout(Duration::from_secs(3));
        if entered.is_err() {
            pause.release();
            first.join().unwrap();
            panic!("reader did not reach the scheduler gate: {entered:?}");
        }
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let second_store = Arc::clone(&store);
        let second_runtime = runtime.clone();
        let second_tx = done_tx.clone();
        let second = std::thread::spawn(move || {
            let snapshot = second_runtime
                .block_on(second_store.read(MetaRead::CurrentNodeSessions))
                .unwrap();
            second_tx.send("reader").unwrap();
            (std::thread::current().id(), snapshot)
        });
        let writer_store = Arc::clone(&store);
        let writer = std::thread::spawn(move || {
            let mut next = lease("session-b");
            next.node_id = "node-b".into();
            let outcome = runtime
                .block_on(writer_store.register_node_session(RequestKey::new("b", "next"), next))
                .unwrap();
            done_tx.send("writer").unwrap();
            (std::thread::current().id(), outcome)
        });
        let mut observed = Vec::new();
        for _ in 0..2 {
            match done_rx.recv_timeout(Duration::from_secs(3)) {
                Ok(done) => observed.push(done),
                Err(_) => break,
            }
        }
        // Always release before joining/asserting, including the old-lock RED.
        pause.release();
        let (first_id, first_snapshot) = first.join().unwrap();
        let (second_id, second_snapshot) = second.join().unwrap();
        let (writer_id, outcome) = writer.join().unwrap();
        assert_ne!(first_id, second_id);
        assert_ne!(first_id, writer_id);
        assert_ne!(second_id, writer_id);
        assert_eq!(
            observed.len(),
            2,
            "a paused read must not serialize another reader or the writer: {observed:?}"
        );
        assert!(matches!(
            outcome,
            TxnOutcome::Committed {
                revision: StoreRevision(2),
                ..
            }
        ));
        assert_eq!(first_snapshot.revision, StoreRevision(1));
        assert_eq!(first_snapshot.entities.len(), 1);
        match second_snapshot.revision {
            StoreRevision(1) => assert_eq!(second_snapshot.entities.len(), 1),
            StoreRevision(2) => assert_eq!(second_snapshot.entities.len(), 2),
            other => panic!("unexpected/torn revision {other}"),
        }
        let current = store.read(MetaRead::CurrentNodeSessions).await.unwrap();
        assert_eq!(current.revision, StoreRevision(2));
        assert_eq!(current.entities.len(), 2);
    }

    #[tokio::test]
    async fn pinned_read_view_keeps_one_revision_across_later_commits() {
        let store = Store::open_log(Arc::new(
            journal::MemoryLog::new(Default::default()).unwrap(),
        ))
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
        let absent_key = MetaKey::CurrentNodeSession {
            node_id: "node-a".into(),
        };
        assert!(view.lookup(&absent_key).unwrap().entity.is_none());
        let first = store.read_view().await.unwrap();
        store
            .register_node_session(
                RequestKey::new("node-a", "view-register-2"),
                lease("session-2"),
            )
            .await
            .unwrap();
        let latest = store.read_view().await.unwrap();
        let old_key = MetaKey::NodeSession {
            node_id: "node-a".into(),
            session_id: "session-1".into(),
        };
        let old = first.lookup(&old_key).unwrap();
        assert!(old.entity.is_some());
        assert_eq!(old, latest.lookup(&old_key).unwrap());
    }

    #[tokio::test]
    async fn pending_write_is_invisible_until_backend_ack() {
        let backend = Arc::new(GatedBackend {
            inner: journal::MemoryLog::new(Default::default()).unwrap(),
            started: Notify::new(),
            release: Notify::new(),
            fail: false,
        });
        let store = Arc::new(Store::open_log(backend.clone()).await.unwrap());
        let writer = {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                store
                    .register_node_session(RequestKey::new("node-a", "r1"), lease("session-1"))
                    .await
            })
        };
        backend.started.notified().await;
        assert_eq!(store.visible.metrics.wal_batches.get(), 0);
        assert_eq!(store.visible.metrics.revision.get(), 0);
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
        assert_eq!(store.visible.metrics.wal_batches.get(), 1);
        assert_eq!(store.visible.metrics.wal_records.get(), 1);
        assert!(store.visible.metrics.wal_framed_bytes.get() > 0);
        assert_eq!(store.visible.metrics.revision.get(), 1);
        assert_eq!(store.visible.metrics.wal_failures.get(), 0);
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
            inner: journal::MemoryLog::new(Default::default()).unwrap(),
            started: Notify::new(),
            release: Notify::new(),
            fail: true,
        });
        let store = Arc::new(Store::open_log(backend.clone()).await.unwrap());
        let writer = {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                store
                    .register_node_session(RequestKey::new("node-a", "r1"), lease("session-1"))
                    .await
            })
        };
        backend.started.notified().await;
        assert_eq!(store.visible.metrics.wal_batches.get(), 0);
        assert_eq!(store.visible.metrics.revision.get(), 0);
        backend.release.notify_one();
        assert!(writer.await.unwrap().is_err());
        assert_eq!(store.visible.metrics.wal_failures.get(), 1);
        assert_eq!(store.visible.metrics.wal_batches.get(), 0);
        assert_eq!(store.visible.metrics.revision.get(), 0);
        assert!(
            store
                .read(MetaRead::CurrentNodeSession {
                    node_id: "node-a".into()
                })
                .await
                .is_err()
        );
    }

    struct MaintenanceLog {
        inner: journal::MemoryLog,
        policy: journal::MaintenancePolicy,
        admitted: AtomicBool,
        uncertain_failure: AtomicBool,
        checked: Notify,
        installed: Notify,
        appends: AtomicU64,
        installs: AtomicU64,
    }
    impl MaintenanceLog {
        fn new() -> Self {
            Self {
                inner: journal::MemoryLog::new(journal::LogLimits::default()).unwrap(),
                policy: journal::MaintenancePolicy {
                    wal_records: 1,
                    poll: Duration::from_millis(5),
                    interval: Duration::from_millis(30),
                    checkpoint_timeout: Duration::from_secs(1),
                    capacity_wait: Duration::from_secs(1),
                    ..Default::default()
                },
                admitted: AtomicBool::new(true),
                uncertain_failure: AtomicBool::new(false),
                checked: Notify::new(),
                installed: Notify::new(),
                appends: 0.into(),
                installs: 0.into(),
            }
        }
    }
    impl journal::MetaLog for MaintenanceLog {
        fn persistence(&self) -> BackendPersistence {
            BackendPersistence::Volatile
        }
        fn limits(&self) -> journal::LogLimits {
            self.inner.limits()
        }
        fn health(&self) -> MetaFuture<'_, ()> {
            self.inner.health()
        }
        fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
            self.inner.recover()
        }
        fn maintenance_policy(&self) -> Option<journal::MaintenancePolicy> {
            Some(self.policy)
        }
        fn status(&self) -> MetaFuture<'_, journal::LogStatus> {
            self.inner.status()
        }
        fn can_append(&self, _expected: journal::LogTip, _bytes: usize) -> MetaFuture<'_, bool> {
            Box::pin(async move {
                self.checked.notify_one();
                Ok(self.admitted.load(Ordering::Acquire))
            })
        }
        fn append_and_sync(
            &self,
            tip: journal::LogTip,
            records: Vec<log::Record>,
        ) -> MetaFuture<'_, journal::LogTip> {
            Box::pin(async move {
                self.appends.fetch_add(1, Ordering::Relaxed);
                let next = self.inner.append_and_sync(tip, records).await?;
                if self.uncertain_failure.load(Ordering::Acquire) {
                    return Err(std::io::Error::from(std::io::ErrorKind::StorageFull).into());
                }
                Ok(next)
            })
        }
        fn install_checkpoint(&self, tip: journal::LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()> {
            Box::pin(async move {
                self.inner.install_checkpoint(tip, bytes).await?;
                self.installs.fetch_add(1, Ordering::Relaxed);
                self.installed.notify_one();
                Ok(())
            })
        }
    }
    #[tokio::test]
    async fn p3_maintenance_background_installs_without_manual_call() {
        let log = Arc::new(MaintenanceLog::new());
        let store = Store::open_log(log.clone()).await.unwrap();
        store
            .register_node_session(RequestKey::new("bg", "one"), lease("one"))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), log.installed.notified())
            .await
            .expect("background checkpoint must run without a manual call");
        assert!(log.installs.load(Ordering::Acquire) > 0);
        assert_eq!(
            log.recover()
                .await
                .unwrap()
                .checkpoint
                .unwrap()
                .covered
                .sequence,
            1
        );
    }
    #[tokio::test]
    async fn p3_maintenance_capacity_wait_never_appends_or_exposes_private_state() {
        let log = Arc::new(MaintenanceLog::new());
        log.admitted.store(false, Ordering::Release);
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        let write = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .register_node_session(RequestKey::new("blocked", "one"), lease("one"))
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), log.checked.notified())
            .await
            .expect("read-only capacity admission must precede append");
        assert!(!write.is_finished());
        assert_eq!(log.appends.load(Ordering::Acquire), 0);
        assert_eq!(
            store.committed().await.unwrap().revision,
            StoreRevision::ZERO
        );
        log.admitted.store(true, Ordering::Release);
        store.visible.checkpoint_finished.notify_waiters();
        assert!(matches!(
            write.await.unwrap().unwrap(),
            TxnOutcome::Committed {
                revision: StoreRevision(1),
                ..
            }
        ));
        assert_eq!(log.appends.load(Ordering::Acquire), 1);
    }
    #[tokio::test]
    async fn p3_maintenance_capacity_timeout_is_not_an_uncertain_enospc_retry() {
        let mut fixture = MaintenanceLog::new();
        fixture.policy.capacity_wait = Duration::from_millis(25);
        let log = Arc::new(fixture);
        log.admitted.store(false, Ordering::Release);
        let store = Store::open_log(log.clone()).await.unwrap();
        let result = store
            .register_node_session(RequestKey::new("blocked", "no-append"), lease("no-append"))
            .await;
        assert!(
            result.is_err(),
            "known capacity exhaustion must not attempt append"
        );
        assert_eq!(log.appends.load(Ordering::Acquire), 0);
        store.health().await.unwrap();
        assert_eq!(
            store.committed().await.unwrap().revision,
            StoreRevision::ZERO
        );
        log.admitted.store(true, Ordering::Release);
        log.uncertain_failure.store(true, Ordering::Release);
        assert!(
            store
                .register_node_session(RequestKey::new("written", "unknown"), lease("unknown"))
                .await
                .is_err()
        );
        assert_eq!(
            log.appends.load(Ordering::Acquire),
            1,
            "a failed append must never be automatically retried"
        );
        assert!(store.committed().await.is_err());
        assert_eq!(
            log.recover().await.unwrap().tip.sequence,
            1,
            "unknown append may already be present"
        );
        log.uncertain_failure.store(false, Ordering::Release);
        let reopened = Store::open_log(log.clone()).await.unwrap();
        assert!(matches!(
            reopened
                .register_node_session(RequestKey::new("written", "unknown"), lease("unknown"))
                .await
                .unwrap(),
            TxnOutcome::ConditionFailed {
                existing_outcome: Some(_),
                ..
            }
        ));
        assert_eq!(
            log.appends.load(Ordering::Acquire),
            1,
            "exact retained request retry must not append twice"
        );
    }

    #[tokio::test]
    async fn p3_metrics_publish_confirmed_history_and_do_not_pin_authority() {
        let store = Store::open_log_with_history(
            Arc::new(journal::MemoryLog::new(Default::default()).unwrap()),
            history::Limits {
                request_count: 1,
                event_count: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let registry = afs_metrics::Registry::new();
        store.register_metrics(&registry).unwrap();
        assert!(store.register_metrics(&registry).is_err());
        for request in ["first", "second"] {
            store
                .register_node_session(RequestKey::new("metrics", request), lease(request))
                .await
                .unwrap();
        }
        let state = store.committed().await.unwrap();
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(text.contains("afs_meta_requests_retained 1\n"), "{text}");
        assert!(
            text.contains("afs_meta_request_evictions_total 1\n"),
            "{text}"
        );
        assert!(text.contains("afs_meta_log_sequence 2\n"), "{text}");
        assert!(
            state.events_removed > 0,
            "fixture must actually evict watch events"
        );
        assert_eq!(
            store.visible.metrics.event_evictions.get(),
            state.events_removed
        );
        assert_eq!(
            store.visible.metrics.compacted_revision.get(),
            i64::try_from(state.compacted_to.0).unwrap()
        );
        assert_eq!(
            store.visible.metrics.request_bytes.get(),
            i64::try_from(state.history_stats().request_bytes).unwrap()
        );
        let other = Store::open_log(Arc::new(
            journal::MemoryLog::new(Default::default()).unwrap(),
        ))
        .await
        .unwrap();
        let other_registry = afs_metrics::Registry::new();
        other.register_metrics(&other_registry).unwrap();
        assert!(
            afs_metrics::encode_text(&other_registry)
                .unwrap()
                .contains("afs_meta_log_sequence 0\n")
        );
        let weak = Arc::downgrade(&state);
        drop(state);
        drop(store);
        tokio::time::timeout(Duration::from_secs(2), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !registry.gather().is_empty(),
            "collector handles survive without pinning State"
        );
    }

    struct GatedCheckpointLog {
        inner: journal::MemoryLog,
        entered: Notify,
        release: Notify,
        policy: Option<journal::MaintenancePolicy>,
        checkpoint_limits: journal::CheckpointLimits,
        installs: AtomicU64,
    }
    impl journal::MetaLog for GatedCheckpointLog {
        fn persistence(&self) -> BackendPersistence {
            BackendPersistence::Volatile
        }
        fn limits(&self) -> journal::LogLimits {
            self.inner.limits()
        }
        fn health(&self) -> MetaFuture<'_, ()> {
            self.inner.health()
        }
        fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
            self.inner.recover()
        }
        fn maintenance_policy(&self) -> Option<journal::MaintenancePolicy> {
            self.policy
        }
        fn checkpoint_limits(&self) -> journal::CheckpointLimits {
            self.checkpoint_limits
        }
        fn status(&self) -> MetaFuture<'_, journal::LogStatus> {
            self.inner.status()
        }
        fn append_and_sync(
            &self,
            tip: journal::LogTip,
            records: Vec<log::Record>,
        ) -> MetaFuture<'_, journal::LogTip> {
            self.inner.append_and_sync(tip, records)
        }
        fn install_checkpoint(
            &self,
            covered: journal::LogTip,
            bytes: Vec<u8>,
        ) -> MetaFuture<'_, ()> {
            Box::pin(async move {
                self.installs.fetch_add(1, Ordering::Relaxed);
                self.entered.notify_one();
                self.release.notified().await;
                self.inner.install_checkpoint(covered, bytes).await
            })
        }
    }

    #[tokio::test]
    async fn p3_checkpoint_cancelled_caller_retains_singleflight_and_state_anchor() {
        let log = Arc::new(GatedCheckpointLog {
            inner: journal::MemoryLog::new(journal::LogLimits::default()).unwrap(),
            entered: Notify::new(),
            release: Notify::new(),
            policy: None,
            checkpoint_limits: journal::CheckpointLimits::default(),
            installs: AtomicU64::new(0),
        });
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        store
            .register_node_session(RequestKey::new("writer", "before"), lease("one"))
            .await
            .unwrap();
        let capture = {
            let store = store.clone();
            tokio::spawn(async move { store.checkpoint().await })
        };
        tokio::time::timeout(Duration::from_secs(5), log.entered.notified())
            .await
            .unwrap();
        assert_eq!(store.visible.metrics.checkpoint_inflight.get(), 1);
        capture.abort();
        assert!(capture.await.unwrap_err().is_cancelled());
        assert!(
            store
                .checkpoint()
                .await
                .unwrap_err()
                .to_string()
                .contains("already in progress")
        );
        store
            .register_node_session(RequestKey::new("writer", "after"), lease("two"))
            .await
            .unwrap();
        log.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.visible.checkpoint_active.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(store.visible.metrics.checkpoint_inflight.get(), 0);
        assert_eq!(store.visible.metrics.checkpoints.get(), 1);
        assert_eq!(store.visible.metrics.checkpoint_failures.get(), 0);
        assert!(store.visible.metrics.checkpoint_bytes.get() > 0);
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.checkpoint.unwrap().covered.sequence, 1);
        assert_eq!(recovered.records.len(), 1);
        assert_eq!(store.committed().await.unwrap().log_tip.sequence, 2);
        let restored = Store::open_log(log).await.unwrap();
        assert_eq!(restored.committed().await.unwrap().log_tip.sequence, 2);
        assert_eq!(
            restored.committed().await.unwrap().node_epochs,
            store.committed().await.unwrap().node_epochs
        );
    }

    #[tokio::test]
    async fn p3_operational_observer_timeout_keeps_task_and_capture_owned() {
        let log = Arc::new(GatedCheckpointLog {
            inner: journal::MemoryLog::new(journal::LogLimits::default()).unwrap(),
            entered: Notify::new(),
            release: Notify::new(),
            policy: Some(journal::MaintenancePolicy {
                wal_records: 2,
                poll: Duration::from_millis(5),
                checkpoint_timeout: Duration::from_millis(10),
                ..Default::default()
            }),
            checkpoint_limits: journal::CheckpointLimits::default(),
            installs: 0.into(),
        });
        let store = Store::open_log(log.clone()).await.unwrap();
        for request in ["one", "two"] {
            store
                .register_node_session(RequestKey::new("timeout", request), lease(request))
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), log.entered.notified())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(store.visible.checkpoint_active.load(Ordering::Acquire));
        assert_eq!(log.installs.load(Ordering::Acquire), 1);
        assert_eq!(store.visible.metrics.checkpoint_inflight.get(), 1);
        assert_eq!(store.visible.metrics.checkpoint_failures.get(), 0);
        assert_eq!(store.visible.metrics.checkpoints.get(), 0);
        assert!(log.recover().await.unwrap().checkpoint.is_none());
        assert!(
            store
                .checkpoint()
                .await
                .unwrap_err()
                .to_string()
                .contains("already in progress")
        );
        store
            .register_node_session(RequestKey::new("timeout", "three"), lease("three"))
            .await
            .unwrap();
        log.release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            while store.visible.checkpoint_active.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(store.visible.metrics.checkpoint_inflight.get(), 0);
        assert_eq!(store.visible.metrics.checkpoints.get(), 1);
        assert_eq!(store.visible.metrics.checkpoint_failures.get(), 0);
        assert!(store.visible.metrics.checkpoint_bytes.get() > 0);
        let recovered = log.recover().await.unwrap();
        assert_eq!(recovered.checkpoint.unwrap().covered.sequence, 2);
        assert_eq!(recovered.tip.sequence, 3);
        assert_eq!(recovered.records.len(), 1);
        assert_eq!(log.installs.load(Ordering::Acquire), 1);
        assert_eq!(store.committed().await.unwrap().revision, StoreRevision(3));
    }

    #[tokio::test]
    async fn p3_operational_capture_limit_rejects_before_install_without_poison() {
        let log = Arc::new(GatedCheckpointLog {
            inner: journal::MemoryLog::new(journal::LogLimits::default()).unwrap(),
            entered: Notify::new(),
            release: Notify::new(),
            policy: None,
            checkpoint_limits: journal::CheckpointLimits {
                image_max_bytes: 256,
                capture_charge_max_bytes: 256,
                disk_max_bytes: 256 * 1024,
                files_max: 5,
            },
            installs: 0.into(),
        });
        let store = Store::open_log(log.clone()).await.unwrap();
        store
            .register_node_session(RequestKey::new("capture", "one"), lease("one"))
            .await
            .unwrap();
        assert!(log::StateImage::capture_charge(&store.committed().await.unwrap()).unwrap() > 256);
        assert!(
            store
                .checkpoint()
                .await
                .unwrap_err()
                .to_string()
                .contains("capture logical charge exceeded")
        );
        assert_eq!(log.installs.load(Ordering::Acquire), 0);
        assert_eq!(store.visible.metrics.checkpoint_inflight.get(), 0);
        assert_eq!(store.visible.metrics.checkpoint_failures.get(), 1);
        assert_eq!(store.visible.metrics.checkpoints.get(), 0);
        assert!(!store.visible.checkpoint_active.load(Ordering::Acquire));
        assert!(log.recover().await.unwrap().checkpoint.is_none());
        store.health().await.unwrap();
        store
            .register_node_session(RequestKey::new("capture", "two"), lease("two"))
            .await
            .unwrap();
        assert_eq!(store.committed().await.unwrap().revision, StoreRevision(2));
    }

    struct PressureLocalLog {
        inner: local_log::LocalLog,
        installs: AtomicU64,
    }
    impl journal::MetaLog for PressureLocalLog {
        fn persistence(&self) -> BackendPersistence {
            BackendPersistence::Persistent
        }
        fn limits(&self) -> journal::LogLimits {
            self.inner.limits()
        }
        fn checkpoint_limits(&self) -> journal::CheckpointLimits {
            self.inner.checkpoint_limits()
        }
        fn health(&self) -> MetaFuture<'_, ()> {
            self.inner.health()
        }
        fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
            self.inner.recover()
        }
        fn status(&self) -> MetaFuture<'_, journal::LogStatus> {
            self.inner.status()
        }
        fn can_append(&self, tip: journal::LogTip, bytes: usize) -> MetaFuture<'_, bool> {
            self.inner.can_append(tip, bytes)
        }
        fn maintenance_policy(&self) -> Option<journal::MaintenancePolicy> {
            Some(journal::MaintenancePolicy {
                poll: Duration::from_millis(5),
                capacity_wait: Duration::from_secs(2),
                ..Default::default()
            })
        }
        fn append_and_sync(
            &self,
            tip: journal::LogTip,
            records: Vec<log::Record>,
        ) -> MetaFuture<'_, journal::LogTip> {
            self.inner.append_and_sync(tip, records)
        }
        fn install_checkpoint(&self, tip: journal::LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()> {
            Box::pin(async move {
                self.inner.install_checkpoint(tip, bytes).await?;
                self.installs.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }
    }

    #[tokio::test]
    async fn p3_operational_real_wal_pressure_reclaims_and_continues() {
        let directory = tempfile::tempdir().unwrap().keep();
        eprintln!("P3 WAL pressure fixture={}", directory.display());
        let limits = journal::LogLimits {
            record_max_bytes: 1024,
            batch_max_bytes: 4096,
            wal_max_bytes: 128 * 1024,
            segment_bytes: 4608,
            segments_max: 4,
        };
        let log = Arc::new(PressureLocalLog {
            inner: local_log::LocalLog::open(&directory, limits).unwrap(),
            installs: 0.into(),
        });
        let store = Store::open_log(log.clone()).await.unwrap();
        let mut max_files = 0;
        let mut max_bytes = 0;
        for number in 0..24 {
            let outcome = store
                .register_node_session(
                    RequestKey::new("pressure", format!("request-{number}")),
                    lease(&format!("session-{number}")),
                )
                .await
                .unwrap();
            assert!(matches!(outcome,TxnOutcome::Committed{revision,..} if revision.0==number+1));
            let mut files = 0;
            let mut bytes = 0;
            for entry in std::fs::read_dir(&directory).unwrap() {
                let path = entry.unwrap().path();
                let name = path.file_name().unwrap().to_string_lossy();
                if name.starts_with("wal-") && name.ends_with(".log") {
                    files += 1;
                    bytes += std::fs::metadata(path).unwrap().len();
                }
            }
            assert!(files <= limits.segments_max && bytes <= limits.wal_max_bytes);
            max_files = max_files.max(files);
            max_bytes = max_bytes.max(bytes);
        }
        assert!(
            log.installs.load(Ordering::Acquire) > 1,
            "actual pressure must install and reclaim repeatedly"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.visible.checkpoint_active.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let recovered = log.recover().await.unwrap();
        let image = recovered.checkpoint.unwrap();
        assert!(image.covered.sequence > 4);
        assert_eq!(recovered.tip.sequence, 24);
        let mut restored =
            log::StateImage::decode(&image.bytes, log.checkpoint_limits().image_max_bytes).unwrap();
        for record in recovered.records {
            restored = restored.replay_record(&record).unwrap();
        }
        assert_eq!(
            restored.snapshot_bytes_with_batch(0).unwrap(),
            store
                .committed()
                .await
                .unwrap()
                .snapshot_bytes_with_batch(0)
                .unwrap()
        );
        eprintln!(
            "P3 WAL pressure PASS ACK=24 checkpoint={} installs={} max_sampled_files={max_files} max_sampled_bytes={max_bytes} files_cap=4 WAL_cap=131072",
            image.covered.sequence,
            log.installs.load(Ordering::Acquire)
        );
        drop(store);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&log) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(log);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn p3_checkpoint_local_reopen_restores_image_and_only_replays_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let local =
            Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
        let identity = local.identity();
        let store = Store::open_log(local.clone()).await.unwrap();
        let request = RequestKey::new("writer", "checkpoint-first");
        let first = store
            .register_node_session(request.clone(), lease("one"))
            .await
            .unwrap();
        let first_state = store.committed().await.unwrap();
        let covered = store
            .checkpoint()
            .await
            .expect("published state must install a durable canonical checkpoint");
        assert_eq!(covered.sequence, 1);
        assert!(dir.path().join("CURRENT").is_file());
        store
            .register_node_session(RequestKey::new("writer", "suffix"), lease("two"))
            .await
            .unwrap();
        let expected = store
            .committed()
            .await
            .unwrap()
            .snapshot_bytes_with_batch(0)
            .unwrap();
        drop(store);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&local) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(local);
        let local =
            Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
        assert_eq!(local.identity(), identity);
        assert_eq!(
            local.recover().await.unwrap().records.len(),
            1,
            "covered record must not be replayed twice"
        );
        let restored = Store::open_log(local.clone()).await.unwrap();
        let actual = restored.committed().await.unwrap();
        assert_eq!(actual.snapshot_bytes_with_batch(0).unwrap(), expected);
        assert_eq!(
            actual.request_completion.get(&request),
            first_state.request_completion.get(&request)
        );
        let retry = restored
            .register_node_session(request, lease("one"))
            .await
            .unwrap();
        assert!(
            matches!((first, retry), (TxnOutcome::Committed { outcome: original, .. }, TxnOutcome::ConditionFailed { existing_outcome: Some(retried), .. }) if original == retried)
        );
        assert_eq!(local.recover().await.unwrap().tip.sequence, 2);
    }
    // A-P3-77 bounded resource report (test-only).
    #[tokio::test]
    async fn p3_a77_owned_memory_and_real_wal_resource_report() {
        use crate::dfs::{
            ChunkEncoding, ChunkId, ContentDigest, DigestAlgorithm, InodeAttributes, InodeKind,
        };
        use serde_json::json;
        use std::{collections::BTreeMap, mem::size_of, path::Path};

        fn inode(number: usize) -> InodeRecord {
            InodeRecord {
                namespace_id: NamespaceId::new("meta-bench"),
                inode_id: InodeId::new(format!("bench-inode-{number:016x}")),
                kind: InodeKind::Regular,
                attributes: InodeAttributes {
                    mode: 0o600,
                    uid: 0,
                    gid: 0,
                    nlink: 1,
                    atime_unix_ms: 1,
                    mtime_unix_ms: 1,
                    ctime_unix_ms: 1,
                },
                head_version: None,
                symlink_target: None,
                xattrs: BTreeMap::new(),
                revision: 1,
            }
        }
        fn txn(id: &str, operation: StoreOperation, values: Vec<MetaEntity>) -> MetaTxn {
            let request = RequestKey::new("a77-report", id);
            let mut txn = MetaTxn::new(request.clone(), operation);
            txn.conditions
                .push(TxnCondition::RequestAbsent(request.clone()));
            txn.mutations
                .extend(values.into_iter().map(TxnMutation::Put));
            txn.mutations
                .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request,
                    operation,
                    result: OperationResult::Empty,
                }));
            txn
        }
        fn disk_bytes(path: &Path, prefix: &str) -> u64 {
            std::fs::read_dir(path)
                .unwrap()
                .map(|entry| entry.unwrap())
                .filter(|entry| entry.file_name().to_string_lossy().starts_with(prefix))
                .map(|entry| entry.metadata().unwrap().len())
                .sum()
        }
        fn memory(key: &MetaKey, value: &VersionedEntity) -> serde_json::Value {
            let inline = size_of::<MetaKey>() + size_of::<VersionedEntity>();
            let charge = history::authority_charge(key, &value.entity).unwrap();
            // This is the existing production accounting model, not a malloc
            // or RSS observation. Fixed tree/Arc slack is not payload copying.
            let structural = 1024;
            assert!(charge >= inline as u64 + structural);
            json!({
                "inline_key_bytes":size_of::<MetaKey>(),
                "inline_versioned_entity_bytes":size_of::<VersionedEntity>(),
                "owned_heap_capacity_bytes":charge-inline as u64-structural,
                "im_arc_logical_structural_reserve_bytes":structural,
                "total_logical_authority_charge_bytes":charge,
                "scope":"one typed entity; owned capacities and existing reserve, not physical allocation or RSS"
            })
        }
        let limits = journal::LogLimits::default();
        let checkpoint = journal::CheckpointLimits::default();
        let history = history::Limits::default();
        let mut observations = Vec::new();
        for files in [64, 128] {
            let directory = tempfile::tempdir().unwrap();
            let local = local_log::LocalLog::open(directory.path(), limits).unwrap();
            let mut values = Vec::new();
            for number in 0..files {
                values.push(MetaEntity::DfsInode(inode(number)));
                values.push(MetaEntity::DfsChunk(ChunkObject {
                    id: ChunkId::new(format!("bench-chunk-{number:016x}")),
                    length: 4 * 1024 * 1024,
                    content_digest: ContentDigest {
                        algorithm: DigestAlgorithm::Blake3,
                        bytes: [7; 32],
                    },
                    encoding: ChunkEncoding::Raw,
                }));
            }
            let (mut state, outcome, decision) = State::default()
                .prepare_txn_record(
                    txn("seed-0000000000000001", StoreOperation::DfsCreate, values),
                    1,
                    limits.record_max_bytes,
                )
                .unwrap();
            assert!(matches!(outcome, TxnOutcome::Committed { .. }));
            let seed_record = log::Record::from(decision.unwrap());
            let seed_payload = codec::encoded_size(&seed_record).unwrap();
            let seed_frame = journal::frame_size(journal::LogTip::EMPTY, &seed_record).unwrap();
            let initial_wal = disk_bytes(directory.path(), "wal-");
            state.log_tip = local
                .append_and_sync(journal::LogTip::EMPTY, vec![seed_record])
                .await
                .unwrap();
            let after_seed_wal = disk_bytes(directory.path(), "wal-");
            assert_eq!(after_seed_wal - initial_wal, seed_frame as u64);
            let image = log::StateImage::encode(&state, checkpoint.image_max_bytes).unwrap();
            let image_payload = image.len();
            local
                .install_checkpoint(state.log_tip, image)
                .await
                .unwrap();
            let installed_checkpoint = disk_bytes(directory.path(), "checkpoint-");
            assert!(installed_checkpoint >= image_payload as u64);

            let inode_key = MetaKey::DfsInode(inode(0).inode_id);
            let chunk_key = MetaKey::DfsChunk(ChunkId::new("bench-chunk-0000000000000000"));
            let (stored_inode_key, stored_inode) = state
                .entities
                .iter()
                .find(|(key, _)| *key == &inode_key)
                .unwrap();
            let (stored_chunk_key, stored_chunk) = state
                .entities
                .iter()
                .find(|(key, _)| *key == &chunk_key)
                .unwrap();
            let inode_memory = memory(stored_inode_key, stored_inode);
            let chunk_memory = memory(stored_chunk_key, stored_chunk);
            let scanned_authority_bytes = state
                .entities
                .iter()
                .map(|(key, value)| history::authority_charge(key, &value.entity).unwrap())
                .sum::<u64>();
            assert_eq!(scanned_authority_bytes, state.authority_bytes);
            assert_eq!(
                state.authority_bytes,
                files as u64
                    * (inode_memory["total_logical_authority_charge_bytes"]
                        .as_u64()
                        .unwrap()
                        + chunk_memory["total_logical_authority_charge_bytes"]
                            .as_u64()
                            .unwrap())
            );
            let old_inode = state.entities.get(&inode_key).unwrap();
            let MetaEntity::DfsInode(mut updated_inode) = old_inode.entity.clone() else {
                panic!("fixture inode missing")
            };
            updated_inode.attributes.mode = 0o640;
            updated_inode.revision = 2;
            let (mut updated, outcome, decision) = state
                .prepare_txn_record(
                    txn(
                        "edit-0000000000000001",
                        StoreOperation::DfsSetInodeAttributes,
                        vec![MetaEntity::DfsInode(updated_inode)],
                    ),
                    2,
                    limits.record_max_bytes,
                )
                .unwrap();
            assert!(matches!(outcome, TxnOutcome::Committed { .. }));
            let record = log::Record::from(decision.unwrap());
            let update_payload = codec::encoded_size(&record).unwrap();
            let update_frame = journal::frame_size(state.log_tip, &record).unwrap();
            let before_update = disk_bytes(directory.path(), "wal-");
            updated.log_tip = local
                .append_and_sync(state.log_tip, vec![record])
                .await
                .unwrap();
            let after_update = disk_bytes(directory.path(), "wal-");
            assert_eq!(after_update - before_update, update_frame as u64);
            assert!(update_frame > update_payload);
            assert_eq!(updated.entities.len(), 2 * files);
            assert!(
                matches!(&old_inode.entity, MetaEntity::DfsInode(inode) if inode.attributes.mode==0o600)
            );
            let restored = local.recover().await.unwrap();
            let mut replayed = log::StateImage::decode(
                &restored.checkpoint.unwrap().bytes,
                checkpoint.image_max_bytes,
            )
            .unwrap();
            for record in restored.records {
                replayed = replayed.replay_record(&record).unwrap();
            }
            assert_eq!(
                replayed.snapshot_bytes_with_batch(0).unwrap(),
                updated.snapshot_bytes_with_batch(0).unwrap()
            );
            observations.push(json!({
                "fixture_pairs":files,"entities":state.entities.len(),
                "inode_memory":inode_memory,"chunk_memory":chunk_memory,
                "initial_wal_file_bytes":initial_wal,
                "seed_decision_bytes":seed_payload,"seed_frame_and_wal_growth_bytes":seed_frame,
                "checkpoint_image_payload_bytes":image_payload,"checkpoint_installed_file_bytes":installed_checkpoint,
                "edit_decision_bytes":update_payload,"edit_frame_bytes":update_frame,
                "edit_actual_wal_file_growth_bytes":after_update-before_update,
                "edit_framing_ratio":update_frame as f64/update_payload as f64,
                "total_wal_bytes_after_edit":after_update,
                "whole_state_authority_logical_charge_bytes":state.authority_bytes,
                "scanned_actual_key_authority_logical_charge_bytes":scanned_authority_bytes,
                "history_fixed_root_logical_reserve_bytes":state.history_stats().index_base_bytes,
                "history_stats_requests":state.history_stats().requests,
                "history_stats_events":state.history_stats().events,
                "checkpoint_plus_suffix_restores_same_fields":true
            }));
        }
        assert_eq!(
            observations[0]["edit_actual_wal_file_growth_bytes"],
            observations[1]["edit_actual_wal_file_growth_bytes"]
        );
        assert_eq!(
            observations[0]["inode_memory"],
            observations[1]["inode_memory"]
        );
        assert_eq!(
            observations[0]["chunk_memory"],
            observations[1]["chunk_memory"]
        );
        assert!(
            observations[1]["checkpoint_installed_file_bytes"]
                .as_u64()
                .unwrap()
                > observations[0]["checkpoint_installed_file_bytes"]
                    .as_u64()
                    .unwrap()
        );
        println!(
            "P3_A77_RESOURCE_REPORT={}",
            json!({
                "schema":1,"kind":"bounded diagnostic, not formal performance",
                "architecture":std::env::consts::ARCH,"pointer_bytes":size_of::<usize>(),
                "im_version":"15.1.0","im_map_header_bytes":size_of::<im::OrdMap<MetaKey,Arc<VersionedEntity>>>(),
                "scope":"logical estimates use existing owned capacity visitor and reserve; no allocator/RSS claim; synthetic inode+chunk pairs are not S/M datasets",
                "wal_ratio_boundary":"actual WAL file length increment / canonical Record encoding bytes; full frame includes envelope/CRC/hash; excludes ACK slot overwrites, fsync, device amplification, CURRENT/checkpoint writes",
                "limits":{
                    "record_max_bytes":limits.record_max_bytes,"batch_max_bytes":limits.batch_max_bytes,"wal_max_bytes":limits.wal_max_bytes,
                    "segment_bytes":limits.segment_bytes,"segments_max":limits.segments_max,
                    "checkpoint_image_max_bytes":checkpoint.image_max_bytes,"checkpoint_capture_max_bytes":checkpoint.capture_charge_max_bytes,
                    "checkpoint_disk_max_bytes":checkpoint.disk_max_bytes,"checkpoint_files_max":checkpoint.files_max,
                    "requests_max":history.request_count,"request_bytes_max":history.request_bytes,"request_retention_ms":history.request_retention.as_millis(),
                    "events_max":history.event_count,"event_bytes_max":history.event_bytes,"event_retention_ms":history.event_retention.as_millis(),
                    "cleanup_items":history.cleanup_items,"protected_requests":history.protected_requests
                },
                "observations":observations
            })
        );
    }
}

#[cfg(test)]
mod grant_lifecycle_tests {
    use super::*;
    use crate::meta::owner_roots::*;
    use crate::meta::store::{
        RequestOutcome, Store,
        journal::{MemoryLog, MetaLog, frame_size},
    };
    use std::time::Duration;

    async fn fixture() -> (Arc<Store>, Arc<MemoryLog>, StoreOwnerRootAuthority) {
        let log = Arc::new(MemoryLog::new(Default::default()).unwrap());
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        store
            .register_node_session(
                RequestKey::new("home", "register"),
                super::super::store::NodeSessionLease {
                    node_id: "home".into(),
                    session_id: "s".into(),
                    grpc_addr: "g".into(),
                    data_addr: "d".into(),
                    rest_addr: "r".into(),
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(300),
                },
            )
            .await
            .unwrap();
        let authority = StoreOwnerRootAuthority::new(store.clone());
        (store, log, authority)
    }
    async fn reserve(authority: &StoreOwnerRootAuthority, intent: &str) -> RootReservationRecord {
        authority
            .reserve_root(ReserveRootInput {
                request_id: format!("reserve-{intent}"),
                root_id: "workspace".into(),
                preferred_home_node_id: "home".into(),
                session_id: "s".into(),
                rights: vec![RootRight::Read],
                expected_root_epoch: 0,
                create_intent_id: intent.into(),
            })
            .await
            .unwrap()
            .reservation
    }
    async fn activate(
        authority: &StoreOwnerRootAuthority,
        r: &RootReservationRecord,
    ) -> RootAccessGrant {
        authority
            .activate_root(ActivateRootInput {
                request_id: format!("activate-{}", r.create_intent_id),
                local_prepare_id: "prepared".into(),
                reservation: WireRootReservation {
                    root_id: r.root_id.clone(),
                    root_epoch: r.root_epoch,
                    home_node_id: r.home_node_id.clone(),
                    session_id: r.home_session_id.clone(),
                    create_intent_id: r.create_intent_id.clone(),
                    prepare_token: r.prepare_token.clone(),
                },
            })
            .await
            .unwrap()
    }
    fn deactivate(epoch: u64) -> DeactivateRootInput {
        DeactivateRootInput {
            request_id: "deactivate".into(),
            root_id: "workspace".into(),
            root_epoch: epoch,
            home_node_id: "home".into(),
            home_session_id: "s".into(),
            reason: "finished".into(),
            authenticated_home_node_id: Some("home".into()),
        }
    }
    #[tokio::test]
    async fn root_epoch_is_not_reused_after_abort_and_reopen() {
        let (_store, log, authority) = fixture().await;
        let first = reserve(&authority, "first").await;
        authority
            .abort_root(AbortRootInput {
                request_id: "abort".into(),
                root_id: first.root_id.clone(),
                root_epoch: first.root_epoch,
                session_id: first.home_session_id.clone(),
                create_intent_id: first.create_intent_id.clone(),
                prepare_token: first.prepare_token.clone(),
                authenticated_home_node_id: Some("home".into()),
            })
            .await
            .unwrap();
        let reopened = Arc::new(Store::open_log(log).await.unwrap());
        let authority = StoreOwnerRootAuthority::new(reopened);
        let second = reserve(&authority, "second").await;
        assert!(
            second.root_epoch > first.root_epoch,
            "aborted/replayed reservations must never reuse epoch"
        );
        assert_eq!(
            reserve(&authority, "second").await,
            second,
            "same intent must retain allocated epoch"
        );
    }
    #[tokio::test]
    async fn deactivation_record_does_not_scale_with_historical_grants() {
        let (store, log, authority) = fixture().await;
        let reservation = reserve(&authority, "first").await;
        let home = activate(&authority, &reservation).await;
        let key = RequestKey::new("fixture", "seed-grants");
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::AcquireRoot);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        for i in 0..140 {
            let mut grant = home.clone();
            grant.holder_node_id = "peer".into();
            grant.holder_session_id = format!("old-{i}");
            txn.mutations
                .push(TxnMutation::Put(MetaEntity::RootGrant(grant)));
        }
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: txn.operation,
                result: OperationResult::Empty,
            }));
        assert!(matches!(
            store.compare_and_commit(txn).await.unwrap(),
            TxnOutcome::Committed { .. }
        ));
        let tip = log.recover().await.unwrap().tip;
        authority
            .deactivate_root(deactivate(home.root_epoch))
            .await
            .unwrap();
        let recovery = log.recover().await.unwrap();
        assert_eq!(recovery.tip.sequence, tip.sequence + 1);
        let bytes = frame_size(tip, recovery.records.last().unwrap()).unwrap();
        assert!(
            bytes <= 1536,
            "deactivate must remain a small record, got {bytes} bytes"
        );
        assert_eq!(
            store
                .read(MetaRead::RootGrants {
                    root_id: "workspace".into()
                })
                .await
                .unwrap()
                .entities
                .len(),
            141,
            "logical invalidation must not require eager deletion"
        );
        assert!(
            authority
                .validate_root_access(ValidateRootAccessInput {
                    root_id: home.root_id.clone(),
                    root_epoch: home.root_epoch,
                    home_session_id: home.home_session_id.clone(),
                    holder_node_id: home.holder_node_id.clone(),
                    holder_session_id: home.holder_session_id.clone(),
                    access_generation: home.access_generation,
                    fencing_token: home.fencing_token.clone(),
                })
                .await
                .is_err()
        );
        let next = reserve(&authority, "second").await;
        assert!(
            next.root_epoch > home.root_epoch,
            "same-name recreation must fence the old incarnation"
        );
        let new_home = activate(&authority, &next).await;
        assert_ne!(new_home.root_epoch, home.root_epoch);
    }
    #[tokio::test]
    async fn grant_lifecycle_recovery_selects_current_home_and_gc_preserves_it() {
        let (store, _, authority) = fixture().await;
        let reservation = reserve(&authority, "first").await;
        let old = activate(&authority, &reservation).await;
        store
            .register_node_session(
                RequestKey::new("home", "register-new"),
                NodeSessionLease {
                    node_id: "home".into(),
                    session_id: "zz-new".into(),
                    grpc_addr: "g".into(),
                    data_addr: "d".into(),
                    rest_addr: "r".into(),
                    storage_devices: vec![],
                    lease_ttl: Duration::from_secs(300),
                },
            )
            .await
            .unwrap();
        let home = authority
            .recover_root(RecoverRootInput {
                request_id: "recover".into(),
                root_id: old.root_id.clone(),
                expected_root_epoch: old.root_epoch,
                home_node_id: "home".into(),
                home_session_id: "zz-new".into(),
                local_prepare_id: "prepared".into(),
            })
            .await
            .unwrap();
        assert_eq!(home.access_generation, old.access_generation + 1);
        let current = store.committed().await.unwrap();
        let (cleaned, _) = current
            .prepare_root_grant_cleanup(&mut None, now_unix_ms(), 1024 * 1024, 32)
            .unwrap()
            .unwrap();
        assert!(
            cleaned
                .read(MetaRead::RootGrantByHolder {
                    root_id: home.root_id.clone(),
                    holder_node_id: "home".into(),
                    holder_session_id: "s".into()
                })
                .unwrap()
                .entity
                .is_none()
        );
        assert_eq!(
            cleaned
                .read(MetaRead::RootGrantByHolder {
                    root_id: home.root_id.clone(),
                    holder_node_id: "home".into(),
                    holder_session_id: "zz-new".into()
                })
                .unwrap()
                .entity,
            Some(MetaEntity::RootGrant(home.clone()))
        );
        let mut input = deactivate(home.root_epoch);
        input.home_session_id = "zz-new".into();
        authority.deactivate_root(input).await.unwrap();
        let view = store.committed().await.unwrap();
        let key = MetaKey::RootCommand {
            command_id: format!("deactivate:workspace:{}:deactivate", home.root_epoch),
        };
        assert!(
            matches!(&view.entities.get(&key).unwrap().entity,MetaEntity::RootCommand(command)
            if command.home_session_id=="zz-new" && command.old_access_generation==home.access_generation)
        );
    }
    #[tokio::test]
    async fn grant_lifecycle_concurrent_reservations_get_distinct_epochs() {
        let (store, _, _) = fixture().await;
        let mut jobs = tokio::task::JoinSet::new();
        for i in 0..16 {
            let store = store.clone();
            jobs.spawn(async move {
                StoreOwnerRootAuthority::new(store)
                    .reserve_root(ReserveRootInput {
                        request_id: format!("r-{i}"),
                        root_id: format!("root-{i}"),
                        preferred_home_node_id: "home".into(),
                        session_id: "s".into(),
                        rights: vec![RootRight::Read],
                        expected_root_epoch: 0,
                        create_intent_id: format!("i-{i}"),
                    })
                    .await
                    .unwrap()
                    .reservation
                    .root_epoch
            });
        }
        let mut epochs = std::collections::BTreeSet::new();
        while let Some(v) = jobs.join_next().await {
            assert!(epochs.insert(v.unwrap()));
        }
        assert_eq!(epochs.len(), 16);
    }
    #[tokio::test]
    async fn grant_lifecycle_expected_epoch_cannot_exhaust_authority() {
        let (store, log, authority) = fixture().await;
        let tip = log.recover().await.unwrap().tip;
        let before = store.committed().await.unwrap();
        let rejected = authority
            .reserve_root(ReserveRootInput {
                request_id: "max".into(),
                root_id: "other".into(),
                preferred_home_node_id: "home".into(),
                session_id: "s".into(),
                rights: vec![],
                expected_root_epoch: u64::MAX,
                create_intent_id: "max".into(),
            })
            .await;
        assert!(
            rejected.is_err(),
            "caller must not raise the allocation watermark"
        );
        assert_eq!(log.recover().await.unwrap().tip, tip);
        assert!(Arc::ptr_eq(&before, &store.committed().await.unwrap()));
        let allocated = reserve(&authority, "after-invalid").await;
        assert_eq!(allocated.root_epoch, 1);
        assert_eq!(reserve(&authority, "after-invalid").await, allocated);
    }
    #[tokio::test]
    async fn grant_lifecycle_background_cleanup_local_wal_and_checkpoint_recovery() {
        use crate::meta::store::{local_log::LocalLog, log::StateImage};
        let directory = tempfile::tempdir().unwrap();
        let log = Arc::new(LocalLog::open(directory.path(), Default::default()).unwrap());
        let identity = log.identity();
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        store
            .register_node_session(
                RequestKey::new("home", "register"),
                NodeSessionLease {
                    node_id: "home".into(),
                    session_id: "s".into(),
                    grpc_addr: "g".into(),
                    data_addr: "d".into(),
                    rest_addr: "r".into(),
                    storage_devices: vec![],
                    lease_ttl: Duration::from_secs(300),
                },
            )
            .await
            .unwrap();
        let authority = StoreOwnerRootAuthority::new(store.clone());
        let first = reserve(&authority, "first").await;
        let old = activate(&authority, &first).await;
        authority
            .deactivate_root(deactivate(old.root_epoch))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if store
                    .read(MetaRead::RootGrants {
                        root_id: "workspace".into(),
                    })
                    .await
                    .unwrap()
                    .entities
                    .is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("bounded maintenance must eventually collect inactive Root grants");
        let tip = log.recover().await.unwrap().tip;
        assert!(log.recover().await.unwrap().records.iter().any(|record| {
            serde_json::to_value(record).unwrap()["Decision"]["decision"]["operation"]
                == "CleanupRootGrants"
        }));
        let current = store.committed().await.unwrap();
        let image = StateImage::encode(&current, 1024 * 1024).unwrap();
        log.install_checkpoint(tip, image).await.unwrap();
        drop(authority);
        drop(store);
        tokio::time::timeout(Duration::from_secs(2), async {
            while Arc::strong_count(&log) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(log);
        let log = Arc::new(LocalLog::open(directory.path(), Default::default()).unwrap());
        assert_eq!(log.identity(), identity);
        let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
        let authority = StoreOwnerRootAuthority::new(store.clone());
        assert!(
            store
                .read(MetaRead::RootGrants {
                    root_id: "workspace".into()
                })
                .await
                .unwrap()
                .entities
                .is_empty()
        );
        let second = reserve(&authority, "second").await;
        assert!(second.root_epoch > first.root_epoch);
        activate(&authority, &second).await;
        assert!(
            authority
                .validate_root_access(ValidateRootAccessInput {
                    root_id: old.root_id,
                    root_epoch: old.root_epoch,
                    home_session_id: old.home_session_id,
                    holder_node_id: old.holder_node_id,
                    holder_session_id: old.holder_session_id,
                    access_generation: old.access_generation,
                    fencing_token: old.fencing_token
                })
                .await
                .is_err()
        );
    }
}
