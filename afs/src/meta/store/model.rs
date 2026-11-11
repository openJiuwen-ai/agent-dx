//! Stored entities, keys, reads, watch and recovery types.

use super::*;

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

    pub(super) fn is_valid(&self) -> bool {
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
    /// Address peers dial for this node's NFS export (outlet imports). Empty
    /// when the node runs no outlet. Kept separate from `data_addr`, which is
    /// the DFS data-plane endpoint peers dial with gRPC.
    #[serde(default)]
    pub nfs_addr: String,
    pub ownerfs_enabled: bool,
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
    pub nfs_addr: String,
    pub ownerfs_enabled: bool,
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
    /// Address the home node advertises for its NFS export.
    ///
    /// Carried alongside the record for the outlet snapshot only: it belongs to
    /// the home's *session*, not to the root, so persisting it here would let a
    /// stale address outlive the session that produced it. The snapshot fills it
    /// from `CurrentNodeSessions` on every read, and it is skipped on serialize
    /// so the durable format is unchanged.
    #[serde(skip)]
    pub nfs_addr: String,
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
    pub requester_node_id: String,
    pub requester_session_id: String,
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

/// Durable root command type. Its variant index is part of the Meta storage
/// format (pinned by the G1 table).
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
    // Share the immutable layout across authority, watch events and snapshots.
    DfsLayoutRoot(Arc<LayoutRoot>),
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
    /// Appended last: earlier variant indexes are on disk.
    DfsReplicaFailureConfig(crate::dfs::ReplicaFailureConfig),
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
            Self::DfsReplicaFailureConfig(_) => "dfs_replica_failure_config",
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
    /// 全局 Active root 全量扫描（outlet 镜像面唯一输入）。
    ///
    /// "Active" 在持久层等价于"Root 实体存在"：`DeactivateRoot` 直接删除
    /// Root 键，不存在软删除状态位。因此本扫描不需要额外状态过滤。
    ActiveRoots,
    Root {
        root_id: String,
    },
    RootReservation {
        root_id: String,
    },
    RootState {
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
    /// At most `limit` dentries of a directory in name order, strictly after
    /// `after` (empty starts at the beginning).
    DfsDirectoryPage {
        namespace_id: NamespaceId,
        parent_inode_id: InodeId,
        after: Vec<u8>,
        limit: usize,
    },
    /// The dentry naming a directory inode, whose key holds its parent.
    DfsDirectoryParent {
        namespace_id: NamespaceId,
        child_inode_id: InodeId,
    },
    DfsInode(InodeId),
    DfsFileVersion(FileVersionId),
    DfsLayoutRoot(LayoutRootId),
    DfsChunk(crate::dfs::ChunkId),
    DfsPlacement(crate::dfs::ChunkId),
    DfsPlacements,
    /// At most `limit` placements in chunk order, after `after`.
    DfsPlacementPage {
        after: Option<crate::dfs::ChunkId>,
        limit: usize,
    },
    /// Placements whose recorded health is not Satisfied.
    DfsUnhealthyPlacements,
    /// Replication tasks that are not Completed.
    DfsOpenReplicationTasks,
    DfsCopy(crate::dfs::CopyId),
    DfsReplicationConfig,
    DfsDurabilityPolicy,
    DfsReplicationTask(ReplicationTaskId),
    DfsReplicationTasks,
    DfsWriteLease(InodeId),
    DfsGc,
    DfsReplicaFailureConfig,
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
    /// Appended last: earlier variant indexes are on disk.
    DfsReplicaFailureConfig,
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

impl fmt::Display for StoreRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
