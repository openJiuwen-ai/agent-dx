//! Transaction conditions, mutations, operations and outcomes.

use super::*;

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
    /// Requires that the supplied process session is no longer current and
    /// live. This lets a new reserve replace a stale caller atomically.
    NodeSessionExpired {
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
    pub(super) fn as_str(self) -> &'static str {
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
