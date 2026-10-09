//! DistributedFs namespace and FileVersion authority on the shared MetaStore.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use afs_error::{Error, Result};

use crate::dfs::{
    CallerContext, ChunkObject, ChunkSources, ClaimReplicationTask, CommitFileVersion,
    CommitMetadataMode, CopyId, CopyLocation, CopyRecord, CopyRole, CopyState, Dentry, DentryKey,
    DentryRecord, DfsChunkSourcesReply, DfsChunkSourcesRequest, DfsReadGrant, FileVersion,
    FileVersionId, GetXattrRequest, InodeAttributes, InodeId, InodeKind, InodeRecord, LayoutRoot,
    LinkRequest, ListXattrRequest, LocalCopyPolicy, MkdirRequest, MknodRequest, NamespaceId,
    OperationId, PlacementHealth, PlacementRecord, PlacementSnapshot, ReadLinkRequest,
    RemoveXattrRequest, RenameMode, RenameOutcome, RenameRequest, ReplicaAck, ReplicaGroup,
    ReplicaGroupId, ReplicaTarget, ReplicaWriteGrant, ReplicationClaim, ReplicationConfig,
    ReplicationTask, ReplicationTaskId, ReplicationTaskState, ReportChunkCorruption,
    ReportReplicationTask, RmdirRequest, SetInodeAttrRequest, SetXattrRequest, SourceCandidate,
    SpecialNodeKind, SymlinkRequest, SyncInodeMetadata, UnlinkRequest, ValidateReplicaWriteRequest,
    WriteLease, XattrSetMode,
};

use super::store::{
    MetaEntity, MetaKey, MetaRead, MetaReadView, MetaStore, MetaTxn, NodeSession, OperationResult,
    RequestKey, RequestOutcome, StoreOperation, TxnCondition, TxnMutation, TxnOutcome,
};

#[derive(Clone)]
pub struct DfsService {
    store: Arc<dyn MetaStore>,
    replication: ReplicationConfig,
    read_grant_key: Arc<Option<[u8; 32]>>,
}

// Store batches at most 64 submitted transactions in one durable commit pass.
// A create can therefore observe up to one full batch of same-parent namespace
// timestamp churn before the next fair turn should either succeed or report
// contention instead of spinning without a boundary.
const CREATE_PARENT_DRIFT_RETRY_LIMIT: usize = 64;

#[derive(Clone, Debug, serde::Serialize)]
pub struct CreateFileRequest {
    pub caller_id: String,
    pub owner_session_id: String,
    pub operation_id: OperationId,
    pub namespace_id: NamespaceId,
    pub parent_inode_id: InodeId,
    pub name: Vec<u8>,
    pub attributes: InodeAttributes,
    pub lease_seconds: u64,
}

impl DfsService {
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self::with_replication_config(store, ReplicationConfig::local_single_copy())
    }

    pub fn with_replication_config(
        store: Arc<dyn MetaStore>,
        replication: ReplicationConfig,
    ) -> Self {
        use std::io::Read;
        let mut key = [0; 32];
        let read_grant_key = std::fs::File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(&mut key))
            .ok()
            .map(|()| key);
        Self {
            store,
            replication,
            read_grant_key: Arc::new(read_grant_key),
        }
    }

    /// Creates the single filesystem replication contract or verifies the
    /// exact value persisted by an earlier start. A different value is a
    /// reformat boundary, not a live configuration update.
    pub async fn initialize_replication_config(&self) -> Result<()> {
        if self.read_grant_key.is_none() {
            return Err(permission_denied(
                "Meta cannot initialize read authority without OS entropy",
            ));
        }
        if !self.replication.is_valid() {
            return Err(invalid("DFS replication config is invalid"));
        }
        let existing = self.store.read(MetaRead::DfsReplicationConfig).await?;
        if let Some(MetaEntity::DfsReplicationConfig(config)) = existing.entity {
            return if config == self.replication {
                Ok(())
            } else {
                Err(conflict(
                    "DFS replication config is immutable; reinitialize the filesystem to change it",
                ))
            };
        }

        let request = RequestKey::new("afs-meta", "dfs-replication-config-v1");
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsInitializeReplicationConfig,
            result: OperationResult::Empty,
        };
        let mut txn = MetaTxn::new(
            request.clone(),
            StoreOperation::DfsInitializeReplicationConfig,
        );
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::Missing(MetaKey::DfsReplicationConfig),
        ]);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsReplicationConfig(self.replication.clone())),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        let _ = self.store.compare_and_commit(txn).await?;
        let persisted = self.store.read(MetaRead::DfsReplicationConfig).await?;
        match persisted.entity {
            Some(MetaEntity::DfsReplicationConfig(config)) if config == self.replication => Ok(()),
            Some(MetaEntity::DfsReplicationConfig(_)) => Err(conflict(
                "DFS replication config changed during initialization",
            )),
            _ => Err(invalid("DFS replication config was not persisted")),
        }
    }

    pub async fn placement_snapshot(&self, caller_id: String) -> Result<PlacementSnapshot> {
        require_id(&caller_id, "caller_id")?;
        let config_snapshot = self.store.read(MetaRead::DfsReplicationConfig).await?;
        let replication = match config_snapshot.entity {
            Some(MetaEntity::DfsReplicationConfig(config)) => config,
            _ => return Err(invalid("DFS replication config is not initialized")),
        };
        if !replication.is_valid() {
            return Err(invalid("DFS replication config is invalid"));
        }

        let sessions_snapshot = self.store.read(MetaRead::CurrentNodeSessions).await?;
        let now = now_unix_ms();
        let mut sessions = sessions_snapshot
            .entities
            .into_iter()
            .filter_map(|entity| match entity {
                MetaEntity::NodeSession(session) if session.is_live_at_unix_ms(now) => {
                    Some(session)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        sessions.sort_by(|left, right| left.node_id.cmp(&right.node_id));

        let caller_session = sessions.iter().find(|session| session.node_id == caller_id);
        if caller_session.is_none() {
            return Err(conflict(
                "DFS placement requires a live caller Node session",
            ));
        }
        let storage_sessions = sessions
            .iter()
            .filter(|session| !session.storage_devices.is_empty())
            .cloned()
            .collect::<Vec<_>>();

        let replica_groups = build_replica_groups(&caller_id, &replication, &storage_sessions)?;
        Ok(PlacementSnapshot {
            revision: config_snapshot.revision.0.max(sessions_snapshot.revision.0),
            replication,
            replica_groups,
        })
    }

    /// Authorizes one receiver to durably accept one chunk replica for a
    /// current placement group. The requester is the authenticated receiver,
    /// while the initiator is the node that selected the chain. Meta rebuilds
    /// the initiator-scoped placement and never trusts a receiver-local
    /// caller-specific snapshot.
    pub async fn validate_replica_write(
        &self,
        request: ValidateReplicaWriteRequest,
    ) -> Result<ReplicaWriteGrant> {
        require_id(&request.requester_node_id, "requester_node_id")?;
        require_id(&request.initiator_node_id, "initiator_node_id")?;
        require_id(&request.operation_id.0, "operation_id")?;
        require_id(&request.chunk_id.0, "chunk_id")?;
        require_id(&request.replica_group_id.0, "replica_group_id")?;
        if request.requester_node_epoch == 0 || request.initiator_node_epoch == 0 {
            return Err(invalid("DFS replica write node epochs are required"));
        }
        if request.chunk_length == 0 {
            return Err(invalid(
                "DFS replica write chunk_length must be greater than zero",
            ));
        }
        if request.content_digest.bytes == [0; 32] {
            return Err(invalid("DFS replica write content digest is required"));
        }
        if request.placement_revision == 0 || request.placement_epoch == 0 {
            return Err(invalid("DFS replica write placement identity is required"));
        }
        if request.ordered_targets.is_empty() {
            return Err(invalid("DFS replica write ordered_targets are required"));
        }
        if request.repair_claim.is_some() {
            return self.validate_repair_replica_write(request).await;
        }

        let initiator_snapshot = self
            .store
            .read(MetaRead::CurrentNodeSession {
                node_id: request.initiator_node_id.clone(),
            })
            .await?;
        match initiator_snapshot.entity {
            Some(MetaEntity::NodeSession(session))
                if session.lease_epoch == request.initiator_node_epoch
                    && session.is_live_at_unix_ms(now_unix_ms()) => {}
            _ => return Err(conflict("DFS replica write initiator epoch is not live")),
        }

        let placement = self
            .placement_snapshot(request.initiator_node_id.clone())
            .await?;
        if request.placement_revision > placement.revision {
            return Err(conflict(
                "DFS replica write references a future placement revision",
            ));
        }
        let group = placement
            .replica_groups
            .iter()
            .find(|group| group.id == request.replica_group_id)
            .filter(|group| group.placement_epoch == request.placement_epoch)
            .ok_or_else(|| conflict("DFS replica write references a stale ReplicaGroup"))?;
        if !replica_targets_match_requested_floor(&request.ordered_targets, &group.targets) {
            return Err(conflict(
                "DFS replica write target order does not match current placement",
            ));
        }
        let target_index = usize::try_from(request.target_index)
            .map_err(|_| invalid("DFS replica write target_index is invalid"))?;
        let target = group
            .targets
            .get(target_index)
            .ok_or_else(|| invalid("DFS replica write target_index is out of range"))?;
        if target.node_id != request.requester_node_id
            || target.node_epoch != request.requester_node_epoch
        {
            return Err(permission_denied(
                "DFS replica write requester is not the assigned placement target",
            ));
        }

        let expires_at_unix_ms = now_unix_ms().saturating_add(30_000);
        let fence = placement.revision;
        let token = replica_write_token(&request, fence, expires_at_unix_ms);
        Ok(ReplicaWriteGrant {
            requester_node_id: request.requester_node_id,
            requester_node_epoch: request.requester_node_epoch,
            initiator_node_id: request.initiator_node_id,
            initiator_node_epoch: request.initiator_node_epoch,
            operation_id: request.operation_id,
            chunk_id: request.chunk_id,
            chunk_length: request.chunk_length,
            content_digest: request.content_digest,
            placement_revision: request.placement_revision,
            placement_epoch: request.placement_epoch,
            replica_group_id: request.replica_group_id,
            target_index: request.target_index,
            replication: placement.replication.clone(),
            // Authority has checked the frozen order and device identities
            // against current placement. Keep the caller's validated catalog
            // floors so granting a retry cannot rewrite its transfer identity.
            replica_group: ReplicaGroup {
                id: group.id.clone(),
                placement_epoch: group.placement_epoch,
                targets: request.ordered_targets,
            },
            expires_at_unix_ms,
            fence,
            token,
        })
    }

    pub async fn claim_replication_task(
        &self,
        request: ClaimReplicationTask,
    ) -> Result<Option<ReplicationClaim>> {
        let request_digest = namespace_request_digest(&request)?;
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.caller_session_id, "caller_session_id")?;
        require_id(&request.operation_id.0, "operation_id")?;
        if request.caller_node_epoch == 0 {
            return Err(invalid("caller_node_epoch is required"));
        }
        if let Some(claim) = self
            .replayed_replication_claim(
                &request.caller_id,
                &request.operation_id,
                StoreOperation::DfsClaimReplicationTask,
                request_digest,
            )
            .await?
        {
            return Ok(claim);
        }
        let lease_seconds = request.lease_seconds.clamp(1, 120);
        let now = now_unix_ms();
        let caller_session = self
            .current_session(
                &request.caller_id,
                request.caller_node_epoch,
                Some(&request.caller_session_id),
                now,
            )
            .await?;
        if caller_session.storage_devices.is_empty() {
            return Err(conflict(
                "replication worker has no registered storage device",
            ));
        }
        let replication = self.current_replication_config().await?;
        let placements = self
            .store
            .read(MetaRead::DfsPlacements)
            .await?
            .entities
            .into_iter()
            .filter_map(|entity| match entity {
                MetaEntity::DfsPlacement(placement) => Some(placement),
                _ => None,
            })
            .collect::<Vec<_>>();
        let tasks = self
            .store
            .read(MetaRead::DfsReplicationTasks)
            .await?
            .entities
            .into_iter()
            .filter_map(|entity| match entity {
                MetaEntity::DfsReplicationTask(task) => Some(task),
                _ => None,
            })
            .collect::<Vec<_>>();

        let mut candidates = tasks
            .iter()
            .filter(|task| task.state != ReplicationTaskState::Completed)
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.id.cmp(&right.id));
        for placement in placements {
            if placement.desired_copies <= placement.copies.len() as u16
                && placement.health == PlacementHealth::Satisfied
                && self.live_ready_copy_count(&placement, now).await?
                    >= usize::from(placement.desired_copies)
            {
                continue;
            }
            let task_id = repair_task_id(&placement.chunk_id);
            if candidates
                .iter()
                .any(|task| task.chunk_id == placement.chunk_id)
            {
                continue;
            }
            if let Some(completed) = tasks.iter().find(|task| task.id == task_id) {
                let mut reactivated = completed.clone();
                reactivated.chunk_id = placement.chunk_id.clone();
                reactivated.placement_epoch = placement.placement_epoch;
                reactivated.desired_copies = placement.desired_copies;
                reactivated.existing_copies = placement.copies.clone();
                reactivated.state = ReplicationTaskState::Pending;
                reactivated.next_retry_unix_ms = now;
                reactivated.last_error = None;
                reactivated.claim = None;
                candidates.push(reactivated);
            } else {
                candidates.push(ReplicationTask {
                    id: task_id,
                    chunk_id: placement.chunk_id.clone(),
                    placement_epoch: placement.placement_epoch,
                    desired_copies: placement.desired_copies,
                    existing_copies: placement.copies.clone(),
                    state: ReplicationTaskState::Pending,
                    attempt: 0,
                    next_retry_unix_ms: now,
                    last_error: None,
                    claim: None,
                });
            }
        }
        candidates.sort_by(|left, right| left.id.cmp(&right.id));

        for task in candidates {
            if !task_is_claimable(&task, now) {
                continue;
            }
            // A task lease outlives the worker's Node lease. Reassignment is
            // allowed once that exact process is retired, without waiting for
            // the longer task lease. Read failures are not proof of retirement.
            let mut retired_worker_conditions = Vec::new();
            if let Some(claim) = task.claim.as_ref()
                && claim.expires_at_unix_ms > now
            {
                let worker_snapshot = self
                    .store
                    .read(MetaRead::CurrentNodeSession {
                        node_id: claim.worker_node_id.clone(),
                    })
                    .await?;
                match worker_snapshot.entity {
                    Some(MetaEntity::NodeSession(session)) => {
                        if session.session_id == claim.worker_session_id
                            && session.lease_epoch == claim.worker_node_epoch
                            && session.is_live_at_unix_ms(now)
                        {
                            continue;
                        }
                        // Freeze both the current pointer and lease contents:
                        // a heartbeat/re-registration racing this decision must
                        // cause CAS rejection, not two active repair workers.
                        retired_worker_conditions.extend([
                            TxnCondition::RevisionEquals {
                                key: MetaKey::CurrentNodeSession {
                                    node_id: session.node_id.clone(),
                                },
                                revision: worker_snapshot.revision,
                            },
                            TxnCondition::EntityEquals(MetaEntity::NodeSession(session)),
                        ]);
                    }
                    None => retired_worker_conditions.push(TxnCondition::Missing(
                        MetaKey::CurrentNodeSession {
                            node_id: claim.worker_node_id.clone(),
                        },
                    )),
                    Some(_) => return Err(invalid("repair worker session record has wrong type")),
                }
            }
            let placement = match self.placement(&task.chunk_id).await {
                Ok(placement) => placement,
                Err(_) => continue,
            };
            if self.live_ready_copy_count(&placement, now).await? == 0 {
                if task.state != ReplicationTaskState::BlockedNoSource
                    || placement.health != PlacementHealth::BlockedNoSource
                {
                    let mut blocked_task = task.clone();
                    blocked_task.state = ReplicationTaskState::BlockedNoSource;
                    blocked_task.claim = None;
                    blocked_task.next_retry_unix_ms = now.saturating_add(5_000);
                    blocked_task.last_error = Some("no live source for replication repair".into());
                    let mut blocked_placement = placement.clone();
                    blocked_placement.health = PlacementHealth::BlockedNoSource;
                    let request_key =
                        RequestKey::new(request.caller_id.clone(), request.operation_id.0.clone());
                    let outcome = RequestOutcome {
                        request: request_key.clone(),
                        operation: StoreOperation::DfsClaimReplicationTask,
                        result: OperationResult::DfsReplicationClaim {
                            request_digest,
                            claim: None,
                        },
                    };
                    let mut txn =
                        MetaTxn::new(request_key.clone(), StoreOperation::DfsClaimReplicationTask);
                    txn.conditions.extend(retired_worker_conditions.clone());
                    txn.conditions.extend([
                        TxnCondition::RequestAbsent(request_key),
                        TxnCondition::EntityEquals(MetaEntity::DfsPlacement(placement)),
                        TxnCondition::NodeSessionCurrent {
                            node_id: request.caller_id.clone(),
                            session_id: request.caller_session_id.clone(),
                        },
                    ]);
                    match self
                        .store
                        .read(MetaRead::DfsReplicationTask(task.id.clone()))
                        .await?
                        .entity
                    {
                        Some(MetaEntity::DfsReplicationTask(current))
                            if current == task || task_reactivates_completed(&task, &current) =>
                        {
                            txn.conditions.push(TxnCondition::EntityEquals(
                                MetaEntity::DfsReplicationTask(current),
                            ))
                        }
                        None => {
                            txn.conditions
                                .push(TxnCondition::Missing(MetaKey::DfsReplicationTask(
                                    task.id.clone(),
                                )))
                        }
                        _ => continue,
                    }
                    txn.mutations.extend([
                        TxnMutation::Put(MetaEntity::DfsPlacement(blocked_placement)),
                        TxnMutation::Put(MetaEntity::DfsReplicationTask(blocked_task)),
                        TxnMutation::RecordRequestOutcome(outcome),
                    ]);
                    return replication_claim_outcome(
                        self.store.compare_and_commit(txn).await?,
                        request_digest,
                    );
                }
                continue;
            }
            let Some(selection) = self
                .build_replication_claim(
                    &request,
                    &caller_session,
                    &task,
                    &replication,
                    lease_seconds,
                    now,
                )
                .await?
            else {
                // Repair debt exists independently of eligible replacement
                // capacity. A completed (or absent) task must not hide a lost
                // replica while there are too few nodes to issue a claim.
                if task.state != ReplicationTaskState::Pending {
                    continue;
                }
                let task_condition = match self
                    .store
                    .read(MetaRead::DfsReplicationTask(task.id.clone()))
                    .await?
                    .entity
                {
                    Some(MetaEntity::DfsReplicationTask(current))
                        if task_reactivates_completed(&task, &current)
                            || (current == task
                                && placement.health != PlacementHealth::UnderReplicated) =>
                    {
                        TxnCondition::EntityEquals(MetaEntity::DfsReplicationTask(current))
                    }
                    None => TxnCondition::Missing(MetaKey::DfsReplicationTask(task.id.clone())),
                    _ => continue,
                };
                let mut pending_placement = placement.clone();
                pending_placement.health = PlacementHealth::UnderReplicated;
                let request_key =
                    RequestKey::new(request.caller_id.clone(), request.operation_id.0.clone());
                let mut txn =
                    MetaTxn::new(request_key.clone(), StoreOperation::DfsClaimReplicationTask);
                txn.conditions.extend(retired_worker_conditions);
                txn.conditions.extend([
                    TxnCondition::RequestAbsent(request_key.clone()),
                    task_condition,
                    TxnCondition::EntityEquals(MetaEntity::DfsPlacement(placement)),
                    TxnCondition::EntityEquals(MetaEntity::DfsReplicationConfig(
                        replication.clone(),
                    )),
                    TxnCondition::NodeSessionCurrent {
                        node_id: request.caller_id.clone(),
                        session_id: request.caller_session_id.clone(),
                    },
                ]);
                txn.mutations.extend([
                    TxnMutation::Put(MetaEntity::DfsPlacement(pending_placement)),
                    TxnMutation::Put(MetaEntity::DfsReplicationTask(task)),
                    TxnMutation::RecordRequestOutcome(RequestOutcome {
                        request: request_key,
                        operation: StoreOperation::DfsClaimReplicationTask,
                        result: OperationResult::DfsReplicationClaim {
                            request_digest,
                            claim: None,
                        },
                    }),
                ]);
                return replication_claim_outcome(
                    self.store.compare_and_commit(txn).await?,
                    request_digest,
                );
            };
            let (claim, updated_task, conditions) = selection;
            let request_key =
                RequestKey::new(request.caller_id.clone(), request.operation_id.0.clone());
            let outcome = RequestOutcome {
                request: request_key.clone(),
                operation: StoreOperation::DfsClaimReplicationTask,
                result: OperationResult::DfsReplicationClaim {
                    request_digest,
                    claim: Some(claim.clone()),
                },
            };
            let mut txn =
                MetaTxn::new(request_key.clone(), StoreOperation::DfsClaimReplicationTask);
            txn.conditions
                .push(TxnCondition::RequestAbsent(request_key));
            txn.conditions.extend(retired_worker_conditions);
            txn.conditions.extend(conditions);
            txn.conditions.push(TxnCondition::EntityEquals(
                MetaEntity::DfsReplicationConfig(replication.clone()),
            ));
            txn.conditions.push(TxnCondition::NodeSessionCurrent {
                node_id: request.caller_id.clone(),
                session_id: request.caller_session_id.clone(),
            });
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::DfsReplicationTask(updated_task)),
                TxnMutation::RecordRequestOutcome(outcome),
            ]);
            return replication_claim_outcome(
                self.store.compare_and_commit(txn).await?,
                request_digest,
            );
        }

        Ok(None)
    }

    pub async fn report_replication_task(
        &self,
        request: ReportReplicationTask,
    ) -> Result<ReplicationTask> {
        let request_digest = namespace_request_digest(&request)?;
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.caller_session_id, "caller_session_id")?;
        require_id(&request.operation_id.0, "operation_id")?;
        if request.caller_node_epoch == 0 {
            return Err(invalid("caller_node_epoch is required"));
        }
        if let Some(task) = self
            .replayed_replication_task(
                &request.caller_id,
                &request.operation_id,
                StoreOperation::DfsReportReplicationTask,
                request_digest,
            )
            .await?
        {
            return Ok(task);
        }
        let now = now_unix_ms();
        let caller_session = self
            .current_session(
                &request.caller_id,
                request.caller_node_epoch,
                Some(&request.caller_session_id),
                now,
            )
            .await?;
        let task = self.replication_task(&request.claim.task_id).await?;
        if task.state != ReplicationTaskState::Running
            || task.claim.as_deref() != Some(&request.claim)
        {
            return Err(Error::coded(
                afs_error::META_DFS_REPAIR_SUPERSEDED,
                "replication report does not match the current claim",
            ));
        }
        if request.claim.worker_node_id != request.caller_id
            || request.claim.worker_node_epoch != request.caller_node_epoch
            || request.claim.worker_session_id != request.caller_session_id
        {
            return Err(permission_denied(
                "replication report caller does not own the claim",
            ));
        }
        let placement = self.placement(&request.claim.chunk.id).await?;
        let chunk = self.chunk(&request.claim.chunk.id).await?;
        if chunk != request.claim.chunk {
            return Err(conflict("replication report references a changed chunk"));
        }
        let source_copy = self.copy(&request.claim.source_copy_id).await?;
        let replication = self.current_replication_config().await?;
        let mut updated_task = task.clone();
        let mut mutations = Vec::new();
        let mut conditions = vec![
            TxnCondition::EntityEquals(MetaEntity::DfsReplicationTask(task)),
            TxnCondition::EntityEquals(MetaEntity::DfsPlacement(placement.clone())),
            TxnCondition::EntityEquals(MetaEntity::DfsChunk(chunk)),
            TxnCondition::EntityEquals(MetaEntity::DfsCopy(source_copy.clone())),
            TxnCondition::EntityEquals(MetaEntity::DfsReplicationConfig(replication)),
            TxnCondition::NodeSessionCurrent {
                node_id: request.caller_id.clone(),
                session_id: request.caller_session_id.clone(),
            },
        ];
        if request.source_invalid {
            if !copy_is_on_node(&source_copy, &request.caller_id)
                || !copy_matches_chunk(&source_copy, &request.claim.chunk)
                || serving_read_copy(&source_copy, &caller_session, now).is_none()
            {
                return Err(permission_denied(
                    "source_invalid can only mark the worker's bound source copy",
                ));
            }
            let mut corrupt = source_copy.clone();
            corrupt.state = CopyState::Corrupt;
            mutations.push(TxnMutation::Put(MetaEntity::DfsCopy(corrupt)));
            let remaining_live_sources = self
                .live_ready_copy_count_excluding(&placement, now, Some(&source_copy.id))
                .await?;
            let mut updated_placement = placement.clone();
            if remaining_live_sources == 0 {
                updated_task.state = ReplicationTaskState::BlockedNoSource;
                updated_placement.health = PlacementHealth::BlockedNoSource;
            } else {
                updated_task.state = ReplicationTaskState::RetryWaiting;
                updated_placement.health = PlacementHealth::UnderReplicated;
            }
            mutations.push(TxnMutation::Put(MetaEntity::DfsPlacement(
                updated_placement,
            )));
            updated_task.claim = None;
            updated_task.last_error = Some("source copy failed local verification".into());
            updated_task.next_retry_unix_ms =
                now.saturating_add(retry_backoff_ms(updated_task.attempt));
        } else if let Some(error) = request.error.clone() {
            updated_task.state = ReplicationTaskState::RetryWaiting;
            updated_task.claim = None;
            updated_task.last_error = Some(error);
            updated_task.next_retry_unix_ms =
                now.saturating_add(retry_backoff_ms(updated_task.attempt));
        } else {
            let accepted = self
                .validate_repair_acks(&request.claim, &request.durable_acks, now, &mut conditions)
                .await?;
            let mut copy_ids = placement.copies.clone();
            for copy in accepted {
                if !copy_ids.contains(&copy.id) {
                    copy_ids.push(copy.id.clone());
                }
                mutations.push(TxnMutation::Put(MetaEntity::DfsCopy(copy)));
            }
            if copy_ids.len() < usize::from(request.claim.replication.desired_copies) {
                return Err(invalid(
                    "replication report does not include the full repaired target set",
                ));
            }
            mutations.push(TxnMutation::Put(MetaEntity::DfsPlacement(
                PlacementRecord {
                    chunk_id: placement.chunk_id.clone(),
                    replica_group_id: request.claim.replica_group.id.clone(),
                    placement_epoch: request.claim.replica_group.placement_epoch,
                    desired_copies: request.claim.replication.desired_copies,
                    copies: copy_ids.clone(),
                    health: PlacementHealth::Satisfied,
                },
            )));
            updated_task.state = ReplicationTaskState::Completed;
            updated_task.placement_epoch = request.claim.replica_group.placement_epoch;
            updated_task.desired_copies = request.claim.replication.desired_copies;
            updated_task.existing_copies = copy_ids;
            updated_task.claim = None;
            updated_task.last_error = None;
            updated_task.next_retry_unix_ms = 0;
        }

        let request_key =
            RequestKey::new(request.caller_id.clone(), request.operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request_key.clone(),
            operation: StoreOperation::DfsReportReplicationTask,
            result: namespace_result(
                request_digest,
                OperationResult::DfsReplicationTask(updated_task.clone()),
            ),
        };
        let mut txn = MetaTxn::new(
            request_key.clone(),
            StoreOperation::DfsReportReplicationTask,
        );
        txn.conditions
            .push(TxnCondition::RequestAbsent(request_key));
        txn.conditions.extend(conditions);
        mutations.push(TxnMutation::Put(MetaEntity::DfsReplicationTask(
            updated_task,
        )));
        mutations.push(TxnMutation::RecordRequestOutcome(outcome));
        txn.mutations = mutations;
        replication_task_outcome(
            self.store.compare_and_commit(txn).await?,
            StoreOperation::DfsReportReplicationTask,
            request_digest,
        )
    }

    pub async fn report_chunk_corruption(&self, request: ReportChunkCorruption) -> Result<()> {
        let request_digest = namespace_request_digest(&request)?;
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.caller_session_id, "caller_session_id")?;
        require_id(&request.operation_id.0, "operation_id")?;
        require_id(&request.chunk_id.0, "chunk_id")?;
        require_id(&request.device_id, "device_id")?;
        if request.caller_node_epoch == 0 {
            return Err(invalid("caller_node_epoch is required"));
        }
        if request.device_epoch == 0 {
            return Err(invalid("device_epoch is required"));
        }
        if request.catalog_revision == 0 {
            return Err(invalid("catalog_revision is required"));
        }
        if self
            .replayed_namespace_result(
                &request.caller_id,
                &request.operation_id,
                StoreOperation::DfsReportChunkCorruption,
                request_digest,
            )
            .await?
            .is_some()
        {
            return Ok(());
        }

        let now = now_unix_ms();
        let caller_session = self
            .current_session(
                &request.caller_id,
                request.caller_node_epoch,
                Some(&request.caller_session_id),
                now,
            )
            .await?;
        if !caller_session.storage_devices.iter().any(|device| {
            device.device_id == request.device_id && device.device_epoch == request.device_epoch
        }) {
            return Err(permission_denied(
                "corruption report device is not owned by the live caller session",
            ));
        }
        let chunk = self.chunk(&request.chunk_id).await?;
        let placement = self.placement(&request.chunk_id).await?;
        let replication = self.current_replication_config().await?;
        let task_id = repair_task_id(&request.chunk_id);
        let task_snapshot = self
            .store
            .read(MetaRead::DfsReplicationTask(task_id.clone()))
            .await?;
        let observed_task = match task_snapshot.entity {
            Some(MetaEntity::DfsReplicationTask(task)) => Some(task),
            None => None,
            Some(_) => return Err(invalid("DFS repair task record has wrong type")),
        };

        let mut observed_copies = Vec::with_capacity(placement.copies.len());
        let mut same_device_seen = false;
        let mut has_superseding_same_device = false;
        let mut changed_copies = Vec::new();
        let mut changed_copy_ids = HashSet::new();
        for copy_id in &placement.copies {
            let copy = self.copy(copy_id).await?;
            if copy_matches_chunk(&copy, &chunk)
                && copy.role == CopyRole::DurableReplica
                && let CopyLocation::Node {
                    node_id,
                    device_id,
                    device_epoch,
                    catalog_revision,
                    ..
                } = &copy.location
                && node_id == &request.caller_id
                && device_id == &request.device_id
                && *device_epoch == request.device_epoch
            {
                same_device_seen = true;
                if *catalog_revision >= request.catalog_revision || copy.state == CopyState::Corrupt
                {
                    has_superseding_same_device = true;
                }
                if copy.state == CopyState::Ready && *catalog_revision < request.catalog_revision {
                    let mut corrupt = copy.clone();
                    corrupt.state = CopyState::Corrupt;
                    changed_copy_ids.insert(copy.id.clone());
                    changed_copies.push(corrupt);
                }
            }
            observed_copies.push(copy);
        }
        if !same_device_seen {
            return Err(permission_denied(
                "corruption report found no caller-owned durable copy on the reported device",
            ));
        }

        let remaining_live_sources = live_ready_copy_count_from_observed_excluding(
            &observed_copies,
            &changed_copy_ids,
            self,
            now,
        )
        .await?;
        let repair_state = if remaining_live_sources == 0 {
            ReplicationTaskState::BlockedNoSource
        } else {
            ReplicationTaskState::Pending
        };
        let repair_health = if remaining_live_sources == 0 {
            PlacementHealth::BlockedNoSource
        } else {
            PlacementHealth::UnderReplicated
        };

        let mut updated_placement = placement.clone();
        if !changed_copies.is_empty() || updated_placement.health != repair_health {
            updated_placement.health = repair_health;
        }
        let updated_task = ReplicationTask {
            id: task_id.clone(),
            chunk_id: request.chunk_id.clone(),
            placement_epoch: placement.placement_epoch,
            desired_copies: placement.desired_copies,
            existing_copies: placement.copies.clone(),
            state: repair_state,
            attempt: observed_task.as_ref().map_or(0, |task| task.attempt),
            next_retry_unix_ms: if repair_state == ReplicationTaskState::BlockedNoSource {
                now.saturating_add(5_000)
            } else {
                now
            },
            last_error: Some(format!(
                "corruption reported by {} on device {} at catalog revision {}",
                request.caller_id, request.device_id, request.catalog_revision
            )),
            claim: None,
        };

        if changed_copies.is_empty() {
            if !has_superseding_same_device {
                return Err(invalid(
                    "corruption report found no Ready same-device copy below the reported catalog revision",
                ));
            }
            let mut ack_conditions = vec![
                TxnCondition::EntityEquals(MetaEntity::NodeSession(caller_session.clone())),
                TxnCondition::NodeSessionCurrent {
                    node_id: request.caller_id.clone(),
                    session_id: request.caller_session_id.clone(),
                },
                TxnCondition::EntityEquals(MetaEntity::DfsChunk(chunk.clone())),
                TxnCondition::EntityEquals(MetaEntity::DfsPlacement(placement.clone())),
                TxnCondition::EntityEquals(MetaEntity::DfsReplicationConfig(replication.clone())),
            ];
            match &observed_task {
                Some(task) => ack_conditions.push(TxnCondition::EntityEquals(
                    MetaEntity::DfsReplicationTask(task.clone()),
                )),
                None => ack_conditions.push(TxnCondition::Missing(MetaKey::DfsReplicationTask(
                    task_id.clone(),
                ))),
            }
            for copy in &observed_copies {
                ack_conditions.push(TxnCondition::EntityEquals(MetaEntity::DfsCopy(
                    copy.clone(),
                )));
            }
            return self
                .record_empty_namespace_outcome(
                    &request.caller_id,
                    &request.operation_id,
                    StoreOperation::DfsReportChunkCorruption,
                    request_digest,
                    ack_conditions,
                )
                .await;
        }

        let request_key =
            RequestKey::new(request.caller_id.clone(), request.operation_id.0.clone());
        let mut txn = MetaTxn::new(
            request_key.clone(),
            StoreOperation::DfsReportChunkCorruption,
        );
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request_key.clone()),
            TxnCondition::EntityEquals(MetaEntity::NodeSession(caller_session)),
            TxnCondition::NodeSessionCurrent {
                node_id: request.caller_id.clone(),
                session_id: request.caller_session_id.clone(),
            },
            TxnCondition::EntityEquals(MetaEntity::DfsChunk(chunk)),
            TxnCondition::EntityEquals(MetaEntity::DfsPlacement(placement)),
            TxnCondition::EntityEquals(MetaEntity::DfsReplicationConfig(replication)),
        ]);
        match observed_task {
            Some(task) => {
                txn.conditions
                    .push(TxnCondition::EntityEquals(MetaEntity::DfsReplicationTask(
                        task,
                    )))
            }
            None => txn
                .conditions
                .push(TxnCondition::Missing(MetaKey::DfsReplicationTask(task_id))),
        }
        for copy in observed_copies {
            txn.conditions
                .push(TxnCondition::EntityEquals(MetaEntity::DfsCopy(copy)));
        }
        for copy in changed_copies {
            txn.mutations
                .push(TxnMutation::Put(MetaEntity::DfsCopy(copy)));
        }
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsPlacement(updated_placement)),
            TxnMutation::Put(MetaEntity::DfsReplicationTask(updated_task)),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: request_key,
                operation: StoreOperation::DfsReportChunkCorruption,
                result: namespace_result(request_digest, OperationResult::Empty),
            }),
        ]);
        empty_namespace_outcome(
            self.store.compare_and_commit(txn).await?,
            StoreOperation::DfsReportChunkCorruption,
            request_digest,
        )
    }

    pub async fn lookup(
        &self,
        namespace_id: NamespaceId,
        parent_inode_id: InodeId,
        name: Vec<u8>,
    ) -> Result<Option<InodeRecord>> {
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        validate_name(&name)?;
        let key = DentryKey {
            namespace_id,
            parent_inode_id,
            name,
        };
        let snapshot = self.store.read(MetaRead::DfsDentry(key)).await?;
        let Some(MetaEntity::DfsDentry(dentry)) = snapshot.entity else {
            return Ok(None);
        };
        self.get_inode(dentry.inode_id).await.map(Some)
    }

    pub async fn create(&self, request: CreateFileRequest) -> Result<(InodeRecord, WriteLease)> {
        let request_digest = namespace_request_digest(&request)?;
        let CreateFileRequest {
            caller_id,
            owner_session_id,
            operation_id,
            namespace_id,
            parent_inode_id,
            name,
            attributes,
            lease_seconds,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&owner_session_id, "owner_session_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        if let Some((inode, lease)) = self
            .replayed_namespace_inode_and_lease(
                &caller_id,
                &operation_id,
                StoreOperation::DfsCreate,
                request_digest,
            )
            .await?
        {
            return Ok((inode, lease));
        }
        validate_name(&name)?;
        let inode = InodeRecord {
            namespace_id: namespace_id.clone(),
            inode_id: namespace_inode_id(&caller_id, &operation_id),
            kind: InodeKind::Regular,
            attributes,
            head_version: None,
            symlink_target: None,
            xattrs: Default::default(),
            revision: 1,
        };
        let dentry_key = DentryKey {
            namespace_id: namespace_id.clone(),
            parent_inode_id: parent_inode_id.clone(),
            name,
        };
        let lease = WriteLease {
            inode_id: inode.inode_id.clone(),
            owner_node_id: caller_id.clone(),
            owner_session_id,
            lease_epoch: 1,
            expires_at_unix_ms: lease_expiry(lease_seconds)?,
        };
        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let mut parent = self
            .directory_for_mutation(&namespace_id, &parent_inode_id)
            .await?;
        let max_attempts = CREATE_PARENT_DRIFT_RETRY_LIMIT;
        for attempt in 0..max_attempts {
            let outcome = RequestOutcome {
                request: request.clone(),
                operation: StoreOperation::DfsCreate,
                result: namespace_result(
                    request_digest,
                    OperationResult::DfsInodeWithLease {
                        inode: inode.clone(),
                        lease: lease.clone(),
                    },
                ),
            };
            let dentry = Dentry {
                key: dentry_key.clone(),
                inode_id: inode.inode_id.clone(),
            };
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsCreate);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(request.clone()),
                TxnCondition::Missing(MetaKey::DfsDentry(dentry.key.clone())),
                TxnCondition::Missing(MetaKey::DfsInode(inode.inode_id.clone())),
                TxnCondition::Missing(MetaKey::DfsWriteLease(inode.inode_id.clone())),
            ]);
            push_parent_namespace_change(&mut txn, parent.clone(), 0);
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::DfsDentry(dentry)),
                TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
                TxnMutation::Put(MetaEntity::DfsWriteLease(lease.clone())),
                TxnMutation::RecordRequestOutcome(outcome),
            ]);
            let outcome = validate_namespace_outcome(
                self.store.compare_and_commit(txn).await?,
                request_digest,
            )?;
            match inode_and_lease_outcome(outcome, StoreOperation::DfsCreate) {
                Ok(created) => return Ok(created),
                Err(error) if error.code() == afs_error::META_DFS_CONFLICT => {
                    if let Some(created) = self
                        .replayed_namespace_inode_and_lease(
                            &request.caller_id,
                            &OperationId::new(request.request_id.clone()),
                            StoreOperation::DfsCreate,
                            request_digest,
                        )
                        .await?
                    {
                        return Ok(created);
                    }
                    let current_parent = self
                        .retryable_create_parent_drift(
                            &namespace_id,
                            &parent_inode_id,
                            &dentry_key,
                            &inode.inode_id,
                            parent.as_ref(),
                        )
                        .await?;
                    let Some(current_parent) = current_parent else {
                        return Err(error);
                    };
                    if attempt + 1 >= max_attempts {
                        return Err(conflict(
                            "DFS create parent changed repeatedly during bounded retry",
                        ));
                    }
                    parent = Some(current_parent);
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("bounded DFS create retry loop returns on every outcome")
    }

    async fn retryable_create_parent_drift(
        &self,
        namespace_id: &NamespaceId,
        parent_inode_id: &InodeId,
        dentry_key: &DentryKey,
        inode_id: &InodeId,
        previous_parent: Option<&InodeRecord>,
    ) -> Result<Option<InodeRecord>> {
        if self
            .store
            .read(MetaRead::DfsDentry(dentry_key.clone()))
            .await?
            .entity
            .is_some()
            || self
                .store
                .read(MetaRead::DfsInode(inode_id.clone()))
                .await?
                .entity
                .is_some()
            || self
                .store
                .read(MetaRead::DfsWriteLease(inode_id.clone()))
                .await?
                .entity
                .is_some()
        {
            return Ok(None);
        }
        let Some(previous_parent) = previous_parent else {
            return Ok(None);
        };
        let Some(current_parent) = self
            .directory_for_mutation(namespace_id, parent_inode_id)
            .await?
        else {
            return Ok(None);
        };
        if create_parent_retry_drift_only(previous_parent, &current_parent) {
            Ok(Some(current_parent))
        } else {
            Ok(None)
        }
    }

    pub async fn mkdir(&self, request: MkdirRequest) -> Result<InodeRecord> {
        let request_digest = namespace_request_digest(&request)?;
        let MkdirRequest {
            caller_id,
            operation_id,
            namespace_id,
            parent_inode_id,
            name,
            mut attributes,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        if let Some(inode) = self
            .replayed_namespace_inode(
                &caller_id,
                &operation_id,
                StoreOperation::DfsMkdir,
                request_digest,
            )
            .await?
        {
            return Ok(inode);
        }
        validate_name(&name)?;
        let parent = self
            .directory_for_mutation(&namespace_id, &parent_inode_id)
            .await?;
        if let Some(parent) = &parent {
            ensure_directory_create_access(&caller, parent)?;
        }
        attributes.uid = caller.uid;
        attributes.gid = inherited_child_gid(&caller, parent.as_ref());
        if parent
            .as_ref()
            .is_some_and(|parent| parent.attributes.mode & 0o2000 != 0)
        {
            attributes.mode |= 0o2000;
        }
        attributes.nlink = 2;
        let inode = InodeRecord {
            namespace_id: namespace_id.clone(),
            inode_id: namespace_inode_id(&caller_id, &operation_id),
            kind: InodeKind::Directory,
            attributes,
            head_version: None,
            symlink_target: None,
            xattrs: Default::default(),
            revision: 1,
        };
        let dentry = Dentry {
            key: DentryKey {
                namespace_id,
                parent_inode_id,
                name,
            },
            inode_id: inode.inode_id.clone(),
        };
        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsMkdir,
            result: namespace_result(request_digest, OperationResult::DfsInode(inode.clone())),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsMkdir);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::Missing(MetaKey::DfsDentry(dentry.key.clone())),
            TxnCondition::Missing(MetaKey::DfsInode(inode.inode_id.clone())),
        ]);
        push_parent_namespace_change(&mut txn, parent, 1);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsDentry(dentry)),
            TxnMutation::Put(MetaEntity::DfsInode(inode)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        inode_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsMkdir,
        )
    }

    /// Special inode bytes are interpreted by the Linux VFS, not ChunkStore.
    /// Meta atomically owns the name, mode, type and device number.
    pub async fn mknod(&self, request: MknodRequest) -> Result<InodeRecord> {
        let request_digest = namespace_request_digest(&request)?;
        let MknodRequest {
            caller_id,
            operation_id,
            namespace_id,
            parent_inode_id,
            name,
            kind,
            mut attributes,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        if let Some(inode) = self
            .replayed_namespace_inode(
                &caller_id,
                &operation_id,
                StoreOperation::DfsMknod,
                request_digest,
            )
            .await?
        {
            return Ok(inode);
        }
        validate_name(&name)?;
        let parent = self
            .directory_for_mutation(&namespace_id, &parent_inode_id)
            .await?;
        if let Some(parent) = &parent {
            ensure_directory_create_access(&caller, parent)?;
        }
        if matches!(
            kind,
            SpecialNodeKind::BlockDevice { .. } | SpecialNodeKind::CharDevice { .. }
        ) && caller.uid != 0
        {
            return Err(operation_not_permitted(
                "DFS device inode creation requires a privileged caller",
            ));
        }
        if let SpecialNodeKind::BlockDevice { rdev } | SpecialNodeKind::CharDevice { rdev } = kind
            && u32::try_from(rdev).is_err()
        {
            return Err(invalid(
                "DFS device number exceeds the FUSE representable range",
            ));
        }
        attributes.uid = caller.uid;
        attributes.gid = if let Some(parent) = &parent
            && parent.attributes.mode & 0o2000 != 0
        {
            parent.attributes.gid
        } else {
            caller.gid
        };
        attributes.nlink = 1;
        let now = now_unix_ms();
        attributes.atime_unix_ms = now;
        attributes.mtime_unix_ms = now;
        attributes.ctime_unix_ms = now;
        let inode = InodeRecord {
            namespace_id: namespace_id.clone(),
            inode_id: namespace_inode_id(&caller_id, &operation_id),
            kind: InodeKind::Special(kind),
            attributes,
            head_version: None,
            symlink_target: None,
            xattrs: Default::default(),
            revision: 1,
        };
        let dentry = Dentry {
            key: DentryKey {
                namespace_id,
                parent_inode_id,
                name,
            },
            inode_id: inode.inode_id.clone(),
        };
        let request = RequestKey::new(caller_id, operation_id.0);
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsMknod);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request.clone()),
            TxnCondition::Missing(MetaKey::DfsDentry(dentry.key.clone())),
            TxnCondition::Missing(MetaKey::DfsInode(inode.inode_id.clone())),
        ]);
        push_parent_namespace_change(&mut txn, parent, 0);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsDentry(dentry)),
            TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation: StoreOperation::DfsMknod,
                result: namespace_result(request_digest, OperationResult::DfsInode(inode)),
            }),
        ]);
        inode_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsMknod,
        )
    }

    pub async fn read_dir(
        &self,
        namespace_id: NamespaceId,
        parent_inode_id: InodeId,
    ) -> Result<Vec<DentryRecord>> {
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        let view = self.store.read_view().await?;
        let parent = inode_from_view(&view, &namespace_id, parent_inode_id.clone()).await?;
        if parent.kind != InodeKind::Directory {
            return Err(not_directory("DFS readdir target is not a directory"));
        }
        let snapshot = view
            .read(MetaRead::DfsDirectory {
                namespace_id: namespace_id.clone(),
                parent_inode_id,
            })
            .await?;
        let mut records = Vec::with_capacity(snapshot.entities.len());
        for entity in snapshot.entities {
            let MetaEntity::DfsDentry(dentry) = entity else {
                continue;
            };
            let inode = inode_from_view(&view, &namespace_id, dentry.inode_id).await?;
            records.push(DentryRecord {
                name: dentry.key.name,
                inode,
            });
        }
        Ok(records)
    }

    pub async fn unlink(&self, request: UnlinkRequest) -> Result<InodeRecord> {
        self.remove_namespace_entry(request, false).await
    }

    pub async fn rmdir(&self, request: RmdirRequest) -> Result<InodeRecord> {
        self.remove_namespace_entry(
            UnlinkRequest {
                caller_id: request.caller_id,
                operation_id: request.operation_id,
                namespace_id: request.namespace_id,
                parent_inode_id: request.parent_inode_id,
                name: request.name,
                caller: request.caller,
            },
            true,
        )
        .await
    }

    pub async fn rename(&self, request: RenameRequest) -> Result<RenameOutcome> {
        let request_digest = namespace_request_digest(&request)?;
        let RenameRequest {
            caller_id,
            operation_id,
            namespace_id,
            old_parent_inode_id,
            old_name,
            new_parent_inode_id,
            new_name,
            mode,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&old_parent_inode_id.0, "old_parent_inode_id")?;
        require_id(&new_parent_inode_id.0, "new_parent_inode_id")?;
        if let Some(outcome) = self
            .replayed_namespace_rename(
                &caller_id,
                &operation_id,
                StoreOperation::DfsRename,
                request_digest,
            )
            .await?
        {
            return Ok(outcome);
        }
        validate_name(&old_name)?;
        validate_name(&new_name)?;
        let old_parent = self
            .directory_for_mutation(&namespace_id, &old_parent_inode_id)
            .await?;
        let new_parent = self
            .directory_for_mutation(&namespace_id, &new_parent_inode_id)
            .await?;
        if let Some(parent) = &old_parent {
            ensure_directory_create_access(&caller, parent)?;
        }
        if old_parent_inode_id != new_parent_inode_id
            && let Some(parent) = &new_parent
        {
            ensure_directory_create_access(&caller, parent)?;
        }
        let old_key = DentryKey {
            namespace_id: namespace_id.clone(),
            parent_inode_id: old_parent_inode_id.clone(),
            name: old_name,
        };
        let new_key = DentryKey {
            namespace_id: namespace_id.clone(),
            parent_inode_id: new_parent_inode_id.clone(),
            name: new_name,
        };
        let view = self.store.read_view().await?;
        let source_dentry = dentry_from_view(&view, old_key.clone())
            .await?
            .ok_or_else(|| not_found("DFS rename source was not found"))?;
        let source_inode =
            inode_from_view(&view, &namespace_id, source_dentry.inode_id.clone()).await?;
        ensure_sticky_parent_allows(&caller, old_parent.as_ref(), &source_inode)?;
        let target_dentry = dentry_from_view(&view, new_key.clone()).await?;
        if old_key == new_key {
            return self
                .record_rename_noop(
                    caller_id,
                    operation_id,
                    source_dentry,
                    source_inode,
                    request_digest,
                )
                .await;
        }
        if source_inode.kind == InodeKind::Directory
            && directory_is_descendant_or_same(
                &view,
                &namespace_id,
                &source_inode.inode_id,
                &new_parent_inode_id,
            )
            .await?
        {
            return Err(invalid("DFS cannot rename a directory below itself"));
        }
        if target_dentry.is_some() && mode == RenameMode::NoReplace {
            return Err(already_exists("DFS rename target already exists"));
        }
        let mut replaced_inode = None;
        let mut target_directory_empty_condition = None;
        if let Some(target) = &target_dentry {
            let target_inode =
                inode_from_view(&view, &namespace_id, target.inode_id.clone()).await?;
            ensure_sticky_parent_allows(&caller, new_parent.as_ref(), &target_inode)?;
            if target_inode.inode_id == source_inode.inode_id {
                return self
                    .record_rename_noop(
                        caller_id,
                        operation_id,
                        source_dentry,
                        source_inode,
                        request_digest,
                    )
                    .await;
            }
            if source_inode.kind == InodeKind::Directory
                || target_inode.kind == InodeKind::Directory
            {
                if source_inode.kind == InodeKind::Directory
                    && target_inode.kind != InodeKind::Directory
                {
                    return Err(not_directory(
                        "DFS rename cannot replace a directory with a non-directory",
                    ));
                }
                if source_inode.kind != InodeKind::Directory
                    && target_inode.kind == InodeKind::Directory
                {
                    return Err(is_directory(
                        "DFS rename cannot replace a non-directory with a directory",
                    ));
                }
                let target_empty =
                    directory_entries_from_view(&view, &namespace_id, &target_inode.inode_id)
                        .await?;
                if !target_empty.is_empty() {
                    return Err(directory_not_empty(
                        "DFS rename cannot replace a non-empty directory",
                    ));
                }
                target_directory_empty_condition = Some(target_inode.inode_id.clone());
            }
            replaced_inode = Some(target_inode);
        }

        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let mut renamed_inode = source_inode.clone();
        renamed_inode.attributes.ctime_unix_ms = now_unix_ms();
        renamed_inode.revision = renamed_inode.revision.saturating_add(1);
        if let Some(inode) = replaced_inode.as_mut() {
            inode.attributes.nlink = if inode.kind == InodeKind::Directory {
                0
            } else {
                inode.attributes.nlink.saturating_sub(1)
            };
            inode.attributes.ctime_unix_ms = now_unix_ms();
            inode.revision = inode.revision.saturating_add(1);
        }
        let outcome_value = RenameOutcome {
            inode: renamed_inode.clone(),
            replaced_inode: replaced_inode.clone(),
        };
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsRename,
            result: namespace_result(request_digest, OperationResult::DfsRename(outcome_value)),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsRename);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::EntityEquals(MetaEntity::DfsDentry(source_dentry.clone())),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(source_inode.clone())),
            TxnCondition::Missing(MetaKey::DfsDentry(new_key.clone())),
        ]);
        if let Some(target) = &target_dentry {
            txn.conditions.pop();
            txn.conditions
                .push(TxnCondition::EntityEquals(MetaEntity::DfsDentry(
                    target.clone(),
                )));
        }
        if source_inode.kind == InodeKind::Directory {
            txn.conditions.push(TxnCondition::DfsNotDescendant {
                namespace_id: namespace_id.clone(),
                ancestor_inode_id: source_inode.inode_id.clone(),
                child_inode_id: new_parent_inode_id.clone(),
            });
        }
        if let Some(target_inode_id) = target_directory_empty_condition {
            txn.conditions.push(TxnCondition::DfsDirectoryEmpty {
                namespace_id: namespace_id.clone(),
                parent_inode_id: target_inode_id,
            });
        }
        txn.mutations
            .push(TxnMutation::Delete(MetaKey::DfsDentry(old_key)));
        if let Some(target) = &target_dentry {
            txn.mutations
                .push(TxnMutation::Delete(MetaKey::DfsDentry(target.key.clone())));
            if let Some(inode) = replaced_inode.clone() {
                let original =
                    inode_from_view(&view, &namespace_id, inode.inode_id.clone()).await?;
                txn.conditions
                    .push(TxnCondition::EntityEquals(MetaEntity::DfsInode(original)));
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsInode(inode)));
            }
        }
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::DfsDentry(Dentry {
                key: new_key,
                inode_id: source_inode.inode_id.clone(),
            })));
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::DfsInode(renamed_inode)));
        let moving_directory = source_inode.kind == InodeKind::Directory;
        let replacing_directory = replaced_inode
            .as_ref()
            .is_some_and(|inode| inode.kind == InodeKind::Directory);
        if old_parent_inode_id == new_parent_inode_id {
            push_parent_namespace_change(
                &mut txn,
                old_parent,
                if replacing_directory { -1 } else { 0 },
            );
        } else {
            push_parent_namespace_change(
                &mut txn,
                old_parent,
                if moving_directory { -1 } else { 0 },
            );
            push_parent_namespace_change(
                &mut txn,
                new_parent,
                i32::from(moving_directory) - i32::from(replacing_directory),
            );
        }
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(outcome));
        rename_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsRename,
        )
    }

    async fn remove_namespace_entry(
        &self,
        request: UnlinkRequest,
        directory: bool,
    ) -> Result<InodeRecord> {
        let request_digest = namespace_request_digest(&request)?;
        let UnlinkRequest {
            caller_id,
            operation_id,
            namespace_id,
            parent_inode_id,
            name,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        let operation = if directory {
            StoreOperation::DfsRmdir
        } else {
            StoreOperation::DfsUnlink
        };
        if let Some(inode) = self
            .replayed_namespace_inode(&caller_id, &operation_id, operation, request_digest)
            .await?
        {
            return Ok(inode);
        }
        validate_name(&name)?;
        let parent = self
            .directory_for_mutation(&namespace_id, &parent_inode_id)
            .await?;
        if let Some(parent) = &parent {
            ensure_directory_create_access(&caller, parent)?;
        }
        let key = DentryKey {
            namespace_id: namespace_id.clone(),
            parent_inode_id,
            name,
        };
        let view = self.store.read_view().await?;
        let dentry = dentry_from_view(&view, key.clone())
            .await?
            .ok_or_else(|| not_found("DFS namespace entry was not found"))?;
        let mut inode = inode_from_view(&view, &namespace_id, dentry.inode_id.clone()).await?;
        if directory {
            if inode.kind != InodeKind::Directory {
                return Err(not_directory("DFS rmdir target is not a directory"));
            }
            if !directory_entries_from_view(&view, &namespace_id, &inode.inode_id)
                .await?
                .is_empty()
            {
                return Err(directory_not_empty("DFS rmdir target is not empty"));
            }
        } else if inode.kind == InodeKind::Directory {
            return Err(is_directory("DFS unlink target is a directory"));
        }
        ensure_sticky_parent_allows(&caller, parent.as_ref(), &inode)?;
        let original_inode = inode.clone();
        if directory {
            inode.attributes.nlink = 0;
        } else {
            inode.attributes.nlink = inode.attributes.nlink.saturating_sub(1);
        }
        inode.attributes.ctime_unix_ms = now_unix_ms();
        inode.revision = inode.revision.saturating_add(1);
        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation,
            result: namespace_result(request_digest, OperationResult::DfsInode(inode.clone())),
        };
        let mut txn = MetaTxn::new(request.clone(), operation);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::EntityEquals(MetaEntity::DfsDentry(dentry)),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(original_inode)),
        ]);
        if directory {
            txn.conditions.push(TxnCondition::DfsDirectoryEmpty {
                namespace_id: namespace_id.clone(),
                parent_inode_id: inode.inode_id.clone(),
            });
        }
        txn.mutations
            .push(TxnMutation::Delete(MetaKey::DfsDentry(key)));
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::DfsInode(inode)));
        push_parent_namespace_change(&mut txn, parent, if directory { -1 } else { 0 });
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(outcome));
        inode_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            operation,
        )
    }

    pub async fn link(&self, request: LinkRequest) -> Result<InodeRecord> {
        let request_digest = namespace_request_digest(&request)?;
        let LinkRequest {
            caller_id,
            operation_id,
            namespace_id,
            existing_inode_id,
            expected_inode_revision,
            parent_inode_id,
            name,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&existing_inode_id.0, "existing_inode_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        if let Some(inode) = self
            .replayed_namespace_inode(
                &caller_id,
                &operation_id,
                StoreOperation::DfsLink,
                request_digest,
            )
            .await?
        {
            return Ok(inode);
        }
        validate_name(&name)?;
        let parent = self
            .directory_for_mutation(&namespace_id, &parent_inode_id)
            .await?;
        if let Some(parent) = &parent {
            ensure_directory_create_access(&caller, parent)?;
        }
        let view = self.store.read_view().await?;
        let mut inode = inode_from_view(&view, &namespace_id, existing_inode_id.clone()).await?;
        if inode.kind == InodeKind::Directory {
            return Err(operation_not_permitted(
                "DFS hardlink target cannot be a directory",
            ));
        }
        if inode.attributes.nlink == 0 {
            return Err(not_found("DFS hardlink target is no longer linked"));
        }
        if inode.revision != expected_inode_revision {
            return Err(conflict("DFS hardlink target inode revision changed"));
        }
        let dentry = Dentry {
            key: DentryKey {
                namespace_id: namespace_id.clone(),
                parent_inode_id,
                name,
            },
            inode_id: inode.inode_id.clone(),
        };
        let original_inode = inode.clone();
        inode.attributes.nlink = inode.attributes.nlink.saturating_add(1);
        inode.attributes.ctime_unix_ms = now_unix_ms();
        inode.revision = inode.revision.saturating_add(1);
        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsLink,
            result: namespace_result(request_digest, OperationResult::DfsInode(inode.clone())),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsLink);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::Missing(MetaKey::DfsDentry(dentry.key.clone())),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(original_inode)),
        ]);
        push_parent_namespace_change(&mut txn, parent, 0);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsDentry(dentry)),
            TxnMutation::Put(MetaEntity::DfsInode(inode)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        inode_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsLink,
        )
    }

    pub async fn symlink(&self, request: SymlinkRequest) -> Result<InodeRecord> {
        let request_digest = namespace_request_digest(&request)?;
        let SymlinkRequest {
            caller_id,
            operation_id,
            namespace_id,
            parent_inode_id,
            name,
            target,
            mut attributes,
            caller,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&namespace_id.0, "namespace_id")?;
        require_id(&parent_inode_id.0, "parent_inode_id")?;
        if let Some(inode) = self
            .replayed_namespace_inode(
                &caller_id,
                &operation_id,
                StoreOperation::DfsSymlink,
                request_digest,
            )
            .await?
        {
            return Ok(inode);
        }
        validate_name(&name)?;
        validate_symlink_target(&target)?;
        let parent = self
            .directory_for_mutation(&namespace_id, &parent_inode_id)
            .await?;
        if let Some(parent) = &parent {
            ensure_directory_create_access(&caller, parent)?;
        }
        attributes.uid = caller.uid;
        attributes.gid = inherited_child_gid(&caller, parent.as_ref());
        attributes.nlink = 1;
        let inode = InodeRecord {
            namespace_id: namespace_id.clone(),
            inode_id: namespace_inode_id(&caller_id, &operation_id),
            kind: InodeKind::Symlink,
            attributes,
            head_version: None,
            symlink_target: Some(target),
            xattrs: Default::default(),
            revision: 1,
        };
        let dentry = Dentry {
            key: DentryKey {
                namespace_id,
                parent_inode_id,
                name,
            },
            inode_id: inode.inode_id.clone(),
        };
        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsSymlink,
            result: namespace_result(request_digest, OperationResult::DfsInode(inode.clone())),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsSymlink);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::Missing(MetaKey::DfsDentry(dentry.key.clone())),
            TxnCondition::Missing(MetaKey::DfsInode(inode.inode_id.clone())),
        ]);
        push_parent_namespace_change(&mut txn, parent, 0);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsDentry(dentry)),
            TxnMutation::Put(MetaEntity::DfsInode(inode)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        inode_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsSymlink,
        )
    }

    pub async fn read_link(&self, request: ReadLinkRequest) -> Result<Vec<u8>> {
        require_id(&request.namespace_id.0, "namespace_id")?;
        require_id(&request.inode_id.0, "inode_id")?;
        let view = self.store.read_view().await?;
        let inode = inode_from_view(&view, &request.namespace_id, request.inode_id).await?;
        if inode.kind != InodeKind::Symlink {
            return Err(invalid("DFS readlink target is not a symlink"));
        }
        inode
            .symlink_target
            .ok_or_else(|| invalid("DFS symlink inode is missing target metadata"))
    }

    pub async fn set_inode_attributes(&self, request: SetInodeAttrRequest) -> Result<InodeRecord> {
        let SetInodeAttrRequest {
            caller_id,
            operation_id,
            caller,
            inode_id,
            expected_inode_revision,
            update,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&inode_id.0, "inode_id")?;
        if let Some(inode) = self
            .replayed_inode(
                &caller_id,
                &operation_id,
                StoreOperation::DfsSetInodeAttributes,
            )
            .await?
        {
            return Ok(inode);
        }
        let current = self.get_inode(inode_id.clone()).await?;
        if current.revision != expected_inode_revision {
            return Err(conflict("DFS inode revision changed before setattr"));
        }
        validate_attr_update_permission(&caller, &current, &update)?;
        let mut updated = current.clone();
        let ownership_changed = update.uid.is_some() || update.gid.is_some();
        if let Some(mode) = update.mode {
            updated.attributes.mode = mode;
        }
        if let Some(uid) = update.uid {
            updated.attributes.uid = uid;
        }
        if let Some(gid) = update.gid {
            updated.attributes.gid = gid;
        }
        if ownership_changed && current.kind != InodeKind::Directory {
            updated.attributes.mode &= !0o6000;
        }
        let attr_now = now_unix_ms();
        if let Some(atime) = update.atime_unix_ms {
            updated.attributes.atime_unix_ms = if update.timestamps_now {
                attr_now
            } else {
                atime
            };
        }
        if let Some(mtime) = update.mtime_unix_ms {
            updated.attributes.mtime_unix_ms = if update.timestamps_now {
                attr_now
            } else {
                mtime
            };
        }
        updated.attributes.ctime_unix_ms = attr_now;
        updated.revision = updated.revision.saturating_add(1);
        self.commit_inode_metadata_mutation(
            caller_id,
            operation_id,
            StoreOperation::DfsSetInodeAttributes,
            current,
            updated,
        )
        .await
    }

    pub async fn get_xattr(&self, request: GetXattrRequest) -> Result<Vec<u8>> {
        let name = xattr_key(&request.name)?;
        let inode = self.get_inode(request.inode_id).await?;
        ensure_xattr_permission(&request.caller, &inode, &name, XattrAccess::Read)?;
        inode
            .xattrs
            .get(&name)
            .cloned()
            .ok_or_else(|| no_data("DFS xattr does not exist"))
    }

    pub async fn list_xattr(&self, request: ListXattrRequest) -> Result<Vec<Vec<u8>>> {
        let inode = self.get_inode(request.inode_id).await?;
        ensure_basic_access(&request.caller, &inode, AccessMode::Read)?;
        Ok(inode.xattrs.keys().cloned().collect())
    }

    pub async fn set_xattr(&self, request: SetXattrRequest) -> Result<InodeRecord> {
        let SetXattrRequest {
            caller_id,
            operation_id,
            caller,
            inode_id,
            expected_inode_revision,
            name,
            value,
            mode,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&inode_id.0, "inode_id")?;
        let name = xattr_key(&name)?;
        if let Some(inode) = self
            .replayed_inode(&caller_id, &operation_id, StoreOperation::DfsSetXattr)
            .await?
        {
            return Ok(inode);
        }
        let current = self.get_inode(inode_id).await?;
        if current.revision != expected_inode_revision {
            return Err(conflict("DFS inode revision changed before setxattr"));
        }
        ensure_xattr_permission(&caller, &current, &name, XattrAccess::Write)?;
        let exists = current.xattrs.contains_key(&name);
        match mode {
            XattrSetMode::Create if exists => {
                return Err(already_exists("DFS xattr already exists"));
            }
            XattrSetMode::Replace if !exists => {
                return Err(no_data("DFS xattr does not exist"));
            }
            _ => {}
        }
        let mut updated = current.clone();
        updated.xattrs.insert(name, value);
        updated.revision = updated.revision.saturating_add(1);
        self.commit_inode_metadata_mutation(
            caller_id,
            operation_id,
            StoreOperation::DfsSetXattr,
            current,
            updated,
        )
        .await
    }

    pub async fn remove_xattr(&self, request: RemoveXattrRequest) -> Result<InodeRecord> {
        let RemoveXattrRequest {
            caller_id,
            operation_id,
            caller,
            inode_id,
            expected_inode_revision,
            name,
        } = request;
        require_id(&caller_id, "caller_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&inode_id.0, "inode_id")?;
        let name = xattr_key(&name)?;
        if let Some(inode) = self
            .replayed_inode(&caller_id, &operation_id, StoreOperation::DfsRemoveXattr)
            .await?
        {
            return Ok(inode);
        }
        let current = self.get_inode(inode_id).await?;
        if current.revision != expected_inode_revision {
            return Err(conflict("DFS inode revision changed before removexattr"));
        }
        ensure_xattr_permission(&caller, &current, &name, XattrAccess::Write)?;
        if !current.xattrs.contains_key(&name) {
            return Err(no_data("DFS xattr does not exist"));
        }
        let mut updated = current.clone();
        updated.xattrs.remove(&name);
        updated.revision = updated.revision.saturating_add(1);
        self.commit_inode_metadata_mutation(
            caller_id,
            operation_id,
            StoreOperation::DfsRemoveXattr,
            current,
            updated,
        )
        .await
    }

    async fn commit_inode_metadata_mutation(
        &self,
        caller_id: String,
        operation_id: OperationId,
        operation: StoreOperation,
        current: InodeRecord,
        updated: InodeRecord,
    ) -> Result<InodeRecord> {
        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation,
            result: OperationResult::DfsInode(updated.clone()),
        };
        let mut txn = MetaTxn::new(request.clone(), operation);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(current)),
        ]);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsInode(updated)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        inode_outcome(self.store.compare_and_commit(txn).await?, operation)
    }

    async fn acquire_write_lease(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        inode_id: InodeId,
        lease_seconds: u64,
    ) -> Result<WriteLease> {
        require_id(&caller_id, "caller_id")?;
        require_id(&owner_session_id, "owner_session_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&inode_id.0, "inode_id")?;
        if let Some(lease) = self
            .replayed_lease(
                &caller_id,
                &operation_id,
                StoreOperation::DfsAcquireWriteLease,
            )
            .await?
        {
            return Ok(lease);
        }
        let inode = self.get_inode(inode_id.clone()).await?;
        if inode.kind != InodeKind::Regular {
            return Err(invalid("DFS write lease requires a regular file"));
        }
        let snapshot = self
            .store
            .read(MetaRead::DfsWriteLease(inode_id.clone()))
            .await?;
        let existing = match snapshot.entity {
            Some(MetaEntity::DfsWriteLease(lease)) => Some(lease),
            _ => None,
        };
        let now = now_unix_ms();
        let mut lease = existing.clone().unwrap_or_else(|| WriteLease {
            inode_id: inode_id.clone(),
            owner_node_id: caller_id.clone(),
            owner_session_id: owner_session_id.clone(),
            lease_epoch: 0,
            expires_at_unix_ms: 0,
        });
        let same_owner =
            lease.owner_node_id == caller_id && lease.owner_session_id == owner_session_id;
        let live = lease.expires_at_unix_ms > now;
        if live && !same_owner {
            return Err(conflict("DFS write lease is held by another live owner"));
        }
        lease.owner_node_id = caller_id.clone();
        lease.owner_session_id = owner_session_id;
        if !same_owner || !live {
            lease.lease_epoch = lease.lease_epoch.saturating_add(1);
        }
        let requested_expiry = lease_expiry(lease_seconds)?;
        lease.expires_at_unix_ms = if same_owner && live {
            lease.expires_at_unix_ms.max(requested_expiry)
        } else {
            requested_expiry
        };

        let request = RequestKey::new(caller_id, operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsAcquireWriteLease,
            result: OperationResult::DfsWriteLease(lease.clone()),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsAcquireWriteLease);
        txn.conditions.push(TxnCondition::RequestAbsent(request));
        match existing {
            Some(current) => {
                txn.conditions
                    .push(TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(
                        current,
                    )))
            }
            None => txn
                .conditions
                .push(TxnCondition::Missing(MetaKey::DfsWriteLease(inode_id))),
        }
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsWriteLease(lease)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        lease_outcome(
            self.store.compare_and_commit(txn).await?,
            StoreOperation::DfsAcquireWriteLease,
        )
    }

    pub async fn open_write(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        inode_id: InodeId,
        lease_seconds: u64,
    ) -> Result<(InodeRecord, WriteLease)> {
        let inode = self.get_inode(inode_id.clone()).await?;
        let snapshot = self
            .store
            .read(MetaRead::DfsWriteLease(inode_id.clone()))
            .await?;
        if let Some(MetaEntity::DfsWriteLease(lease)) = snapshot.entity
            && lease.expires_at_unix_ms > now_unix_ms()
            && (lease.owner_node_id != caller_id || lease.owner_session_id != owner_session_id)
        {
            return Ok((inode, lease));
        }
        let lease = self
            .acquire_write_lease(
                caller_id,
                owner_session_id,
                operation_id,
                inode_id,
                lease_seconds,
            )
            .await?;
        Ok((inode, lease))
    }

    async fn resolve_fresh_write_authority(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        inode_id: InodeId,
        lease_seconds: u64,
        context: &'static str,
    ) -> Result<(InodeRecord, WriteLease)> {
        require_id(&caller_id, "caller_id")?;
        require_id(&owner_session_id, "owner_session_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&inode_id.0, "inode_id")?;
        let view = self.store.read_view().await?;
        let recorded = view
            .read(MetaRead::RequestOutcome(RequestKey::new(
                caller_id.clone(),
                operation_id.0.clone(),
            )))
            .await?
            .request_outcome;
        let inode = match view
            .read(MetaRead::DfsInode(inode_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsInode(inode)) if inode.inode_id == inode_id => inode,
            _ => {
                return Err(Error::coded(
                    afs_error::NODE_VFS_NOT_FOUND,
                    "DFS inode was not found",
                ));
            }
        };
        if inode.kind != InodeKind::Regular {
            return Err(invalid(format!(
                "DFS {context} authority requires a regular file"
            )));
        }
        // Recover a lost acquire reply before reading a newer live authority.
        // Read-only resolutions have no recorded result and remain fresh reads.
        if let Some(recorded) = recorded {
            if recorded.operation != StoreOperation::DfsAcquireWriteLease {
                return Err(invalid(
                    "DFS operation_id was already used for another operation",
                ));
            }
            return match recorded.result {
                OperationResult::DfsWriteLease(lease)
                    if lease.inode_id == inode_id
                        && lease.owner_node_id == caller_id
                        && lease.owner_session_id == owner_session_id
                        && lease.lease_epoch != 0 =>
                {
                    Ok((inode, lease))
                }
                _ => Err(invalid(format!(
                    "DFS {context} acquire replay does not match caller and inode"
                ))),
            };
        }
        if let Some(MetaEntity::DfsWriteLease(lease)) = view
            .read(MetaRead::DfsWriteLease(inode_id.clone()))
            .await?
            .entity
        {
            if lease.inode_id != inode_id
                || lease.lease_epoch == 0
                || lease.owner_node_id.is_empty()
                || lease.owner_session_id.is_empty()
            {
                return Err(invalid(format!(
                    "DFS stored {context} authority is incomplete"
                )));
            }
            if lease.expires_at_unix_ms > now_unix_ms() {
                return Ok((inode, lease));
            }
        }
        let lease = self
            .acquire_write_lease(
                caller_id.clone(),
                owner_session_id.clone(),
                operation_id,
                inode_id,
                lease_seconds,
            )
            .await?;
        if lease.inode_id != inode.inode_id
            || lease.owner_node_id != caller_id
            || lease.owner_session_id != owner_session_id
            || lease.lease_epoch == 0
        {
            return Err(invalid(format!(
                "DFS {context} acquire result does not match caller and inode"
            )));
        }
        Ok((inode, lease))
    }

    pub async fn resolve_lock_authority(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        inode_id: InodeId,
        lease_seconds: u64,
    ) -> Result<(InodeRecord, WriteLease)> {
        self.resolve_fresh_write_authority(
            caller_id,
            owner_session_id,
            operation_id,
            inode_id,
            lease_seconds,
            "lock",
        )
        .await
    }

    pub async fn resolve_write_authority(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        inode_id: InodeId,
        lease_seconds: u64,
    ) -> Result<(InodeRecord, WriteLease)> {
        self.resolve_fresh_write_authority(
            caller_id,
            owner_session_id,
            operation_id,
            inode_id,
            lease_seconds,
            "write",
        )
        .await
    }

    pub async fn renew_write_lease(
        &self,
        caller_id: String,
        owner_session_id: String,
        operation_id: OperationId,
        current: WriteLease,
        lease_seconds: u64,
    ) -> Result<WriteLease> {
        require_id(&caller_id, "caller_id")?;
        require_id(&owner_session_id, "owner_session_id")?;
        require_id(&operation_id.0, "operation_id")?;
        require_id(&current.inode_id.0, "current.inode_id")?;
        if current.owner_node_id != caller_id || current.owner_session_id != owner_session_id {
            return Err(conflict(
                "DFS write lease owner does not match renew caller",
            ));
        }
        if let Some(lease) = self
            .replayed_lease(
                &caller_id,
                &operation_id,
                StoreOperation::DfsRenewWriteLease,
            )
            .await?
        {
            return Ok(lease);
        }
        // Expiry is a mutable renewal hint, not the fencing identity. A file
        // handle and its lock authority can hold different expiry snapshots
        // of the same live owner/epoch after another open or renewal.
        for _ in 0..16 {
            let stored = self
                .validate_write_lease(&caller_id, &current.inode_id, &current)
                .await?;
            let mut renewed = stored.clone();
            renewed.expires_at_unix_ms =
                stored.expires_at_unix_ms.max(lease_expiry(lease_seconds)?);
            let request = RequestKey::new(caller_id.clone(), operation_id.0.clone());
            let outcome = RequestOutcome {
                request: request.clone(),
                operation: StoreOperation::DfsRenewWriteLease,
                result: OperationResult::DfsWriteLease(renewed.clone()),
            };
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsRenewWriteLease);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(request),
                TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(stored)),
            ]);
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::DfsWriteLease(renewed)),
                TxnMutation::RecordRequestOutcome(outcome),
            ]);
            match self.store.compare_and_commit(txn).await? {
                TxnOutcome::ConditionFailed {
                    existing_outcome: None,
                    ..
                } => continue,
                outcome => {
                    return lease_outcome(outcome, StoreOperation::DfsRenewWriteLease);
                }
            }
        }
        Err(Error::coded(
            afs_error::META_DFS_LEASE_RETRY,
            "DFS lease renewal remained contended after retries",
        ))
    }

    pub async fn get_inode(&self, inode_id: InodeId) -> Result<InodeRecord> {
        require_id(&inode_id.0, "inode_id")?;
        let root = inode_id.0 == "1";
        let snapshot = self.store.read(MetaRead::DfsInode(inode_id)).await?;
        match snapshot.entity {
            Some(MetaEntity::DfsInode(inode)) => Ok(inode),
            None if root => Ok(root_inode(NamespaceId::new("default"))),
            _ => Err(Error::coded(
                afs_error::NODE_VFS_NOT_FOUND,
                "DFS inode was not found",
            )),
        }
    }

    pub async fn get_file_version(
        &self,
        version_id: FileVersionId,
    ) -> Result<(FileVersion, LayoutRoot)> {
        require_id(&version_id.0, "version_id")?;
        let version = match self
            .store
            .read(MetaRead::DfsFileVersion(version_id))
            .await?
            .entity
        {
            Some(MetaEntity::DfsFileVersion(version)) => version,
            _ => {
                return Err(Error::coded(
                    afs_error::NODE_VFS_NOT_FOUND,
                    "DFS FileVersion was not found",
                ));
            }
        };
        let layout = match self
            .store
            .read(MetaRead::DfsLayoutRoot(version.layout_root.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsLayoutRoot(layout)) => layout,
            _ => {
                return Err(Error::coded(
                    afs_error::NODE_VFS_NOT_FOUND,
                    "DFS LayoutRoot was not found",
                ));
            }
        };
        Ok((version, layout))
    }

    pub async fn chunk_sources(
        &self,
        request: DfsChunkSourcesRequest,
    ) -> Result<DfsChunkSourcesReply> {
        require_id(&request.caller_id, "caller_id")?;
        require_id(&request.namespace_id.0, "namespace_id")?;
        require_id(&request.file_version_id.0, "file_version_id")?;
        require_id(&request.layout_root_id.0, "layout_root_id")?;
        if request.chunk_ids.is_empty() {
            return Ok(DfsChunkSourcesReply {
                revision: 0,
                chunks: Vec::new(),
            });
        }
        let view = self.store.read_view().await?;
        let Some(MetaEntity::DfsFileVersion(version)) = view
            .read(MetaRead::DfsFileVersion(request.file_version_id.clone()))
            .await?
            .entity
        else {
            return Err(invalid("DFS read source FileVersion does not exist"));
        };
        let Some(MetaEntity::DfsLayoutRoot(layout)) = view
            .read(MetaRead::DfsLayoutRoot(request.layout_root_id.clone()))
            .await?
            .entity
        else {
            return Err(invalid("DFS read source LayoutRoot does not exist"));
        };
        let Some(MetaEntity::DfsInode(inode)) = view
            .read(MetaRead::DfsInode(version.inode_id.clone()))
            .await?
            .entity
        else {
            return Err(invalid("DFS read source inode does not exist"));
        };
        if inode.namespace_id != request.namespace_id {
            return Err(invalid("DFS read source namespace does not match the file"));
        }
        if version.layout_root != request.layout_root_id || layout.id != request.layout_root_id {
            return Err(conflict(
                "DFS read source request references a stale LayoutRoot",
            ));
        }
        let requested = request.chunk_ids.iter().cloned().collect::<HashSet<_>>();
        for chunk_id in &requested {
            if !layout
                .inline_extents
                .iter()
                .any(|extent| extent.chunk_id == *chunk_id)
            {
                return Err(invalid(
                    "DFS read source request includes a Chunk outside the FileVersion layout",
                ));
            }
        }

        let caller_session = view
            .read(MetaRead::CurrentNodeSession {
                node_id: request.caller_id.clone(),
            })
            .await?;
        let caller_epoch = match caller_session.entity {
            Some(MetaEntity::NodeSession(session)) if session.is_live_at_unix_ms(now_unix_ms()) => {
                session.lease_epoch
            }
            _ => {
                return Err(conflict(
                    "DFS read source request requires a live caller session",
                ));
            }
        };

        let mut revision = 0;
        let mut chunks = Vec::with_capacity(request.chunk_ids.len());
        for chunk_id in request.chunk_ids {
            let placement_snapshot = view.read(MetaRead::DfsPlacement(chunk_id.clone())).await?;
            revision = revision.max(placement_snapshot.revision.0);
            let Some(MetaEntity::DfsPlacement(placement)) = placement_snapshot.entity else {
                chunks.push(ChunkSources {
                    chunk_id,
                    sources: Vec::new(),
                });
                continue;
            };
            let mut sources = Vec::new();
            for copy_id in placement.copies {
                let copy_snapshot = view.read(MetaRead::DfsCopy(copy_id)).await?;
                revision = revision.max(copy_snapshot.revision.0);
                let Some(MetaEntity::DfsCopy(copy)) = copy_snapshot.entity else {
                    continue;
                };
                if copy.chunk_id != chunk_id || !copy.is_ready_durable() {
                    continue;
                }
                let CopyLocation::Node { node_id, .. } = &copy.location else {
                    continue;
                }; // ExternalCommitted never masquerades as a Peer replica.
                let session_snapshot = view
                    .read(MetaRead::CurrentNodeSession {
                        node_id: node_id.clone(),
                    })
                    .await?;
                revision = revision.max(session_snapshot.revision.0);
                let Some(MetaEntity::NodeSession(session)) = session_snapshot.entity else {
                    continue;
                };
                let Some(serving_copy) = serving_read_copy(&copy, &session, now_unix_ms()) else {
                    continue;
                };
                let mut read_grant = DfsReadGrant {
                    namespace_id: request.namespace_id.clone(),
                    file_version_id: request.file_version_id.clone(),
                    layout_root_id: request.layout_root_id.clone(),
                    caller_node_id: request.caller_id.clone(),
                    caller_node_epoch: caller_epoch,
                    expires_at_unix_ms: now_unix_ms().saturating_add(30_000),
                    fence: revision,
                    token: String::new(),
                };
                let chunk = match view
                    .read(MetaRead::DfsChunk(chunk_id.clone()))
                    .await?
                    .entity
                {
                    Some(MetaEntity::DfsChunk(chunk)) => chunk,
                    _ => return Err(invalid("DFS source chunk identity is missing")),
                };
                let ranges = read_chunk_intervals(&layout, &chunk)?;
                read_grant.token =
                    self.read_grant_token(&read_grant, &serving_copy, &chunk, &ranges)?;
                sources.push(SourceCandidate {
                    copy_id: copy.id.clone(),
                    chunk_id: chunk_id.clone(),
                    role: copy.role,
                    state: copy.state,
                    location: serving_copy.location.clone(),
                    data_endpoint: Some(session.data_addr),
                    load_hint: 0,
                    read_grant,
                });
            }
            chunks.push(ChunkSources { chunk_id, sources });
        }
        Ok(DfsChunkSourcesReply { revision, chunks })
    }

    fn read_grant_mac(
        &self,
        grant: &DfsReadGrant,
        copy: &CopyRecord,
        chunk: &crate::dfs::ChunkObject,
        ranges: &[(u64, u64)],
    ) -> Result<blake3::Hash> {
        let key = self
            .read_grant_key
            .as_ref()
            .as_ref()
            .ok_or_else(|| permission_denied("Meta read grant entropy is unavailable"))?;
        let mut unsigned = grant.clone();
        unsigned.token.clear();
        let canonical = serde_json::to_vec(&("afs-dfs-read-v1", unsigned, copy, chunk, ranges))
            .map_err(|_| invalid("read grant canonical serialization failed"))?;
        Ok(blake3::keyed_hash(key, &canonical))
    }

    fn read_grant_token(
        &self,
        grant: &DfsReadGrant,
        copy: &CopyRecord,
        chunk: &crate::dfs::ChunkObject,
        ranges: &[(u64, u64)],
    ) -> Result<String> {
        Ok(format!(
            "dfs-read-v1:{}",
            self.read_grant_mac(grant, copy, chunk, ranges)?.to_hex()
        ))
    }

    pub async fn validate_read_grants(
        &self,
        request: crate::dfs::ValidateDfsReadGrants,
    ) -> Result<Vec<crate::dfs::DfsAuthorizedRead>> {
        if request.validations.is_empty() || request.validations.len() > 128 {
            return Err(invalid(
                "read grant validation batch requires 1..128 entries",
            ));
        }
        let now = now_unix_ms();
        let view = self.store.read_view().await?;
        let receiver = live_read_session(
            &view,
            &request.receiver_node_id,
            request.receiver_node_epoch,
            now,
        )
        .await?;
        let mut result = Vec::with_capacity(request.validations.len());
        for validation in request.validations {
            let grant = &validation.grant;
            if grant.caller_node_id != request.peer_node_id
                || grant.expires_at_unix_ms <= now
                || grant.token.len() != 76
            {
                return Err(permission_denied("read grant peer or expiry differs"));
            }
            let caller =
                live_read_session(&view, &request.peer_node_id, grant.caller_node_epoch, now)
                    .await?;
            let version = match view
                .read(MetaRead::DfsFileVersion(grant.file_version_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsFileVersion(value))
                    if value.layout_root == grant.layout_root_id =>
                {
                    value
                }
                _ => return Err(permission_denied("read grant version or layout differs")),
            };
            match view
                .read(MetaRead::DfsInode(version.inode_id))
                .await?
                .entity
            {
                Some(MetaEntity::DfsInode(inode)) if inode.namespace_id == grant.namespace_id => {}
                _ => return Err(permission_denied("read grant namespace differs")),
            }
            let layout = match view
                .read(MetaRead::DfsLayoutRoot(grant.layout_root_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsLayoutRoot(value)) => value,
                _ => return Err(permission_denied("read grant layout is missing")),
            };
            let chunk = match view
                .read(MetaRead::DfsChunk(validation.chunk_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsChunk(value)) => value,
                _ => return Err(permission_denied("read grant chunk is missing")),
            };
            let copy = match view
                .read(MetaRead::DfsCopy(validation.copy_id.clone()))
                .await?
                .entity
            {
                Some(MetaEntity::DfsCopy(value))
                    if value.chunk_id == validation.chunk_id && value.is_ready_durable() =>
                {
                    value
                }
                _ => return Err(permission_denied("read grant copy is not Ready")),
            };
            let serving_copy = serving_read_copy(&copy, &receiver, now).ok_or_else(|| {
                permission_denied("read grant does not select a recovered receiver device")
            })?;
            let intervals = read_chunk_intervals(&layout, &chunk)?;
            let supplied = grant
                .token
                .strip_prefix("dfs-read-v1:")
                .and_then(|hex| blake3::Hash::from_hex(hex).ok())
                .ok_or_else(|| permission_denied("read grant MAC format is invalid"))?;
            if supplied != self.read_grant_mac(grant, &serving_copy, &chunk, &intervals)? {
                return Err(permission_denied("read grant MAC authentication failed"));
            }
            if !read_range_covered(&intervals, validation.chunk_offset, validation.length) {
                return Err(permission_denied(
                    "read range is outside the committed layout",
                ));
            }
            // Highly fragmented layouts retain functionality without unbounded
            // authority replies/cache entries: grant this already checked range.
            let allowed_ranges = if intervals.len() <= 128 {
                intervals
            } else {
                vec![(validation.chunk_offset, validation.length)]
            };
            let expires_at_unix_ms = now
                .saturating_add(5_000)
                .min(grant.expires_at_unix_ms)
                .min(caller.expires_at_unix_ms)
                .min(receiver.expires_at_unix_ms);
            result.push(crate::dfs::DfsAuthorizedRead {
                validation,
                allowed_ranges,
                expires_at_unix_ms,
            });
        }
        Ok(result)
    }

    pub async fn sync_inode_metadata(
        &self,
        caller_id: String,
        sync: SyncInodeMetadata,
    ) -> Result<InodeRecord> {
        require_id(&caller_id, "caller_id")?;
        require_id(&sync.operation_id.0, "operation_id")?;
        require_id(&sync.inode_id.0, "inode_id")?;
        if let Some(inode) = self
            .replayed_inode(
                &caller_id,
                &sync.operation_id,
                StoreOperation::DfsSyncInodeMetadata,
            )
            .await?
        {
            return Ok(inode);
        }
        if sync.metadata_delta.mode != CommitMetadataMode::Full {
            return Err(invalid("DFS inode metadata sync requires full metadata"));
        }
        let current = self.get_inode(sync.inode_id.clone()).await?;
        let current_lease = self
            .validate_write_lease(&caller_id, &sync.inode_id, &sync.write_lease)
            .await?;
        if current.revision < sync.expected_inode_revision
            || current.head_version != sync.expected_head_version
        {
            return Err(conflict("DFS inode changed before metadata sync"));
        }
        let mut updated = current.clone();
        updated.revision = updated.revision.saturating_add(1);
        updated.attributes.mtime_unix_ms = sync
            .metadata_delta
            .mtime_unix_ms
            .ok_or_else(|| invalid("DFS full metadata sync requires mtime"))?;
        updated.attributes.ctime_unix_ms = sync
            .metadata_delta
            .ctime_unix_ms
            .ok_or_else(|| invalid("DFS full metadata sync requires ctime"))?;
        if sync.metadata_delta.kill_suidgid {
            updated.attributes.mode = cleared_write_privileges(updated.attributes.mode);
        }

        let request = RequestKey::new(caller_id, sync.operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsSyncInodeMetadata,
            result: OperationResult::DfsInode(updated.clone()),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsSyncInodeMetadata);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(current)),
            TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(current_lease)),
        ]);
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsInode(updated)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        inode_outcome(
            self.store.compare_and_commit(txn).await?,
            StoreOperation::DfsSyncInodeMetadata,
        )
    }

    pub async fn commit_file_version(
        &self,
        caller_id: String,
        commit: CommitFileVersion,
    ) -> Result<InodeRecord> {
        let request_digest = namespace_request_digest(&commit)?;
        require_id(&caller_id, "caller_id")?;
        require_id(&commit.operation_id.0, "operation_id")?;
        require_id(&commit.inode_id.0, "inode_id")?;
        require_id(
            &commit.write_lease.owner_node_id,
            "write_lease.owner_node_id",
        )?;
        require_id(
            &commit.write_lease.owner_session_id,
            "write_lease.owner_session_id",
        )?;
        require_id(&commit.file_version.id.0, "file_version.id")?;
        require_id(&commit.layout_root.id.0, "layout_root.id")?;
        if let Some(inode) = self
            .replayed_namespace_inode(
                &caller_id,
                &commit.operation_id,
                StoreOperation::DfsCommitFileVersion,
                request_digest,
            )
            .await?
        {
            return Ok(inode);
        }
        let current = self.get_inode(commit.inode_id.clone()).await?;
        if commit.write_lease.inode_id != commit.inode_id
            || commit.write_lease.owner_node_id != caller_id
        {
            return Err(conflict(
                "DFS commit lease does not belong to caller and inode",
            ));
        }
        let current_lease = self
            .validate_write_lease(&caller_id, &commit.inode_id, &commit.write_lease)
            .await?;
        if current.revision < commit.expected_inode_revision
            || current.head_version != commit.expected_head_version
        {
            return Err(conflict("DFS inode head changed before commit"));
        }
        if commit.file_version.inode_id != commit.inode_id
            || commit.file_version.layout_root != commit.layout_root.id
            || commit.file_version.length != commit.layout_root.file_length
            || commit.file_version.parent_version != commit.expected_head_version
        {
            return Err(invalid(
                "FileVersion and LayoutRoot do not describe one file",
            ));
        }
        let replication = self.current_replication_config().await?;
        let expected_base_layout = match commit.expected_head_version.as_ref() {
            Some(version_id) => Some(self.get_file_version(version_id.clone()).await?.1),
            None => None,
        };
        let mut accepted_copies = Vec::with_capacity(commit.chunk_receipts.len());
        let mut receipt_chunks = HashSet::new();
        for receipt in &commit.chunk_receipts {
            if receipt.operation_id != commit.operation_id {
                return Err(invalid("ChunkReceipt operation does not match commit"));
            }
            if !receipt_chunks.insert(receipt.chunk.id.clone()) {
                return Err(invalid("commit contains duplicate ChunkReceipts"));
            }
            accepted_copies.push(
                self.validate_chunk_receipt(&caller_id, receipt, &replication)
                    .await?,
            );
        }
        let mut previous_end = 0;
        for extent in &commit.layout_root.inline_extents {
            let file_end = extent
                .file_offset
                .checked_add(extent.length)
                .ok_or_else(|| invalid("DFS extent file range overflows"))?;
            if extent.length == 0
                || extent.file_offset < previous_end
                || file_end > commit.file_version.length
            {
                return Err(invalid(
                    "LayoutRoot extents must be non-empty, ordered and non-overlapping",
                ));
            }
            let chunk_end = extent
                .chunk_offset
                .checked_add(extent.length)
                .ok_or_else(|| invalid("DFS extent chunk range overflows"))?;
            if let Some(receipt) = commit
                .chunk_receipts
                .iter()
                .find(|receipt| receipt.chunk.id == extent.chunk_id)
            {
                if chunk_end > receipt.chunk.length {
                    return Err(invalid("DFS extent exceeds its referenced Chunk"));
                }
            } else if !expected_base_layout
                .as_ref()
                .is_some_and(|layout| extent_is_inherited(extent, layout))
            {
                return Err(invalid(
                    "LayoutRoot references neither a newly durable Chunk nor a legal expected-base range",
                ));
            }
            previous_end = file_end;
        }
        let mut updated = current.clone();
        updated.head_version = Some(commit.file_version.id.clone());
        updated.revision = updated.revision.saturating_add(1);
        match commit.metadata_delta.mode {
            CommitMetadataMode::DataOnly => {}
            CommitMetadataMode::Full => {
                updated.attributes.mtime_unix_ms = commit
                    .metadata_delta
                    .mtime_unix_ms
                    .ok_or_else(|| invalid("DFS full commit requires mtime"))?;
                updated.attributes.ctime_unix_ms = commit
                    .metadata_delta
                    .ctime_unix_ms
                    .ok_or_else(|| invalid("DFS full commit requires ctime"))?;
            }
        }
        if commit.metadata_delta.kill_suidgid {
            let ctime = commit
                .metadata_delta
                .ctime_unix_ms
                .ok_or_else(|| invalid("DFS killpriv commit requires ctime"))?;
            let cleared = cleared_write_privileges(updated.attributes.mode);
            if cleared != updated.attributes.mode {
                updated.attributes.mode = cleared;
                updated.attributes.ctime_unix_ms = ctime;
            }
        }

        let request = RequestKey::new(caller_id, commit.operation_id.0.clone());
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsCommitFileVersion,
            result: namespace_result(request_digest, OperationResult::DfsInode(updated.clone())),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsCommitFileVersion);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(current)),
            TxnCondition::EntityEquals(MetaEntity::DfsWriteLease(current_lease)),
            TxnCondition::EntityEquals(MetaEntity::DfsReplicationConfig(replication.clone())),
            TxnCondition::Missing(MetaKey::DfsFileVersion(commit.file_version.id.clone())),
            TxnCondition::Missing(MetaKey::DfsLayoutRoot(commit.layout_root.id.clone())),
        ]);
        for (receipt, copies) in commit.chunk_receipts.iter().zip(accepted_copies) {
            txn.mutations.push(TxnMutation::Put(MetaEntity::DfsChunk(
                receipt.chunk.clone(),
            )));
            for copy in &copies {
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsCopy(copy.clone())));
            }
            let copy_ids = copies
                .iter()
                .map(|copy| copy.id.clone())
                .collect::<Vec<_>>();
            let health = if copies.len() >= usize::from(replication.desired_copies) {
                PlacementHealth::Satisfied
            } else {
                PlacementHealth::UnderReplicated
            };
            txn.mutations
                .push(TxnMutation::Put(MetaEntity::DfsPlacement(
                    PlacementRecord {
                        chunk_id: receipt.chunk.id.clone(),
                        replica_group_id: receipt.replica_group_id.clone(),
                        placement_epoch: receipt.placement_epoch,
                        desired_copies: replication.desired_copies,
                        copies: copy_ids.clone(),
                        health,
                    },
                )));
            if copies.len() < usize::from(replication.desired_copies) {
                txn.mutations
                    .push(TxnMutation::Put(MetaEntity::DfsReplicationTask(
                        ReplicationTask {
                            id: repair_task_id(&receipt.chunk.id),
                            chunk_id: receipt.chunk.id.clone(),
                            placement_epoch: receipt.placement_epoch,
                            desired_copies: replication.desired_copies,
                            existing_copies: copy_ids,
                            state: ReplicationTaskState::Pending,
                            attempt: 0,
                            next_retry_unix_ms: now_unix_ms(),
                            last_error: None,
                            claim: None,
                        },
                    )));
            }
        }
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsLayoutRoot(commit.layout_root)),
            TxnMutation::Put(MetaEntity::DfsFileVersion(commit.file_version)),
            TxnMutation::Put(MetaEntity::DfsInode(updated)),
            TxnMutation::RecordRequestOutcome(outcome),
        ]);
        inode_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsCommitFileVersion,
        )
    }

    async fn current_replication_config(&self) -> Result<ReplicationConfig> {
        let snapshot = self.store.read(MetaRead::DfsReplicationConfig).await?;
        match snapshot.entity {
            Some(MetaEntity::DfsReplicationConfig(config)) if config == self.replication => {
                Ok(config)
            }
            Some(MetaEntity::DfsReplicationConfig(_)) => Err(conflict(
                "DFS replication config differs from the initialized filesystem",
            )),
            _ => {
                self.initialize_replication_config().await?;
                Ok(self.replication.clone())
            }
        }
    }

    async fn validate_chunk_receipt(
        &self,
        caller_id: &str,
        receipt: &crate::dfs::ChunkReceipt,
        replication: &ReplicationConfig,
    ) -> Result<Vec<CopyRecord>> {
        if receipt.placement_revision == 0
            || receipt.placement_epoch == 0
            || receipt.durable_acks.len() < usize::from(replication.sync_required_copies)
        {
            return Err(invalid(
                "ChunkReceipt does not satisfy the initialized replication config",
            ));
        }

        let mut nodes = HashSet::new();
        let mut failure_domains = HashSet::new();
        let mut copies = Vec::with_capacity(receipt.durable_acks.len());
        let placement = self.placement_snapshot(caller_id.to_owned()).await?;
        if placement.replication != *replication || receipt.placement_revision > placement.revision
        {
            return Err(conflict(
                "ChunkReceipt does not match the current replication authority",
            ));
        }
        let group = placement
            .replica_groups
            .iter()
            .find(|group| group.id == receipt.replica_group_id)
            .filter(|group| group.placement_epoch == receipt.placement_epoch)
            .ok_or_else(|| conflict("ChunkReceipt references a stale ReplicaGroup"))?;
        for ack in &receipt.durable_acks {
            if ack.operation_id != receipt.operation_id
                || ack.chunk_id != receipt.chunk.id
                || ack.placement_revision != receipt.placement_revision
                || ack.placement_epoch != receipt.placement_epoch
                || ack.persisted_bytes != receipt.chunk.length
                || ack.verified_digest != receipt.chunk.content_digest
            {
                return Err(invalid("ReplicaAck does not prove its ChunkReceipt"));
            }
            if !nodes.insert(ack.node_id.clone()) {
                return Err(invalid(
                    "ChunkReceipt contains duplicate Node acknowledgements",
                ));
            }
            group
                .targets
                .iter()
                .find(|target| {
                    target.node_id == ack.node_id
                        && target.node_epoch == ack.node_epoch
                        && target.device.device_id == ack.device_id
                        && target.device.device_epoch == ack.device_epoch
                })
                .ok_or_else(|| {
                    conflict("ReplicaAck target is not assigned by the current ReplicaGroup")
                })?;
            // This snapshot may include unrelated catalog advances after the
            // write grant. The receiver enforces the frozen grant's floor;
            // comparing that ACK against today's floor would reject valid data.
            let session_snapshot = self
                .store
                .read(MetaRead::CurrentNodeSession {
                    node_id: ack.node_id.clone(),
                })
                .await?;
            let session = match session_snapshot.entity {
                Some(MetaEntity::NodeSession(session))
                    if session.lease_epoch == ack.node_epoch
                        && session.is_live_at_unix_ms(now_unix_ms()) =>
                {
                    session
                }
                _ => return Err(conflict("ReplicaAck references a stale Node epoch")),
            };
            let device = session
                .storage_devices
                .iter()
                .find(|device| {
                    device.device_id == ack.device_id && device.device_epoch == ack.device_epoch
                })
                .ok_or_else(|| conflict("ReplicaAck references a stale storage device"))?;
            failure_domains.insert(device.failure_domain.clone());
            copies.push(CopyRecord {
                id: CopyId::new(format!(
                    "{}:{}:{}:{}",
                    ack.node_id, ack.node_epoch, ack.device_id, receipt.chunk.id.0
                )),
                chunk_id: receipt.chunk.id.clone(),
                role: CopyRole::DurableReplica,
                location: CopyLocation::Node {
                    node_id: ack.node_id.clone(),
                    node_epoch: ack.node_epoch,
                    device_id: ack.device_id.clone(),
                    device_epoch: ack.device_epoch,
                    catalog_revision: ack.catalog_revision,
                },
                state: CopyState::Ready,
                persisted_bytes: ack.persisted_bytes,
                verified_digest: ack.verified_digest.clone(),
            });
        }

        if nodes.len() < usize::from(replication.min_distinct_nodes)
            || failure_domains.len() < usize::from(replication.min_distinct_failure_domains)
            || (replication.local_copy == LocalCopyPolicy::Required && !nodes.contains(caller_id))
        {
            return Err(invalid(
                "ChunkReceipt does not satisfy Node, failure-domain or local-copy constraints",
            ));
        }
        Ok(copies)
    }

    async fn validate_repair_replica_write(
        &self,
        request: ValidateReplicaWriteRequest,
    ) -> Result<ReplicaWriteGrant> {
        let claim = request
            .repair_claim
            .clone()
            .ok_or_else(|| invalid("repair claim is required"))?;
        if claim.operation_id != request.operation_id
            || claim.worker_node_id != request.initiator_node_id
            || claim.worker_node_epoch != request.initiator_node_epoch
            || claim.chunk.id != request.chunk_id
            || claim.chunk.length != request.chunk_length
            || claim.chunk.content_digest != request.content_digest
            || claim.placement_revision != request.placement_revision
            || claim.replica_group.id != request.replica_group_id
            || claim.replica_group.placement_epoch != request.placement_epoch
            || claim.replica_group.targets != request.ordered_targets
        {
            return Err(conflict("repair replica write does not match its claim"));
        }
        let now = now_unix_ms();
        if claim.expires_at_unix_ms <= now {
            return Err(conflict("repair claim expired"));
        }
        let task = self.replication_task(&claim.task_id).await?;
        if task.state != ReplicationTaskState::Running || task.claim.as_deref() != Some(&claim) {
            return Err(conflict("repair claim is not the current running claim"));
        }
        self.current_session(
            &claim.worker_node_id,
            claim.worker_node_epoch,
            Some(&claim.worker_session_id),
            now,
        )
        .await?;
        let target_index = usize::try_from(request.target_index)
            .map_err(|_| invalid("DFS replica write target_index is invalid"))?;
        let target = claim
            .replica_group
            .targets
            .get(target_index)
            .ok_or_else(|| invalid("DFS replica write target_index is out of range"))?;
        if target.node_id != request.requester_node_id
            || target.node_epoch != request.requester_node_epoch
        {
            return Err(permission_denied(
                "repair replica write requester is not the assigned target",
            ));
        }
        self.current_session(&target.node_id, target.node_epoch, None, now)
            .await?;
        if target_index > 0 {
            let sender = &claim.replica_group.targets[target_index - 1];
            if sender.node_id != request.initiator_node_id
                && request.initiator_node_id != claim.worker_node_id
            {
                return Err(permission_denied(
                    "repair replica write sender does not match claim chain",
                ));
            }
        }
        let fence = task.attempt.into();
        let token = replica_write_token(&request, fence, claim.expires_at_unix_ms);
        Ok(ReplicaWriteGrant {
            requester_node_id: request.requester_node_id,
            requester_node_epoch: request.requester_node_epoch,
            initiator_node_id: request.initiator_node_id,
            initiator_node_epoch: request.initiator_node_epoch,
            operation_id: request.operation_id,
            chunk_id: request.chunk_id,
            chunk_length: request.chunk_length,
            content_digest: request.content_digest,
            placement_revision: request.placement_revision,
            placement_epoch: request.placement_epoch,
            replica_group_id: request.replica_group_id,
            target_index: request.target_index,
            replication: claim.replication,
            replica_group: claim.replica_group,
            expires_at_unix_ms: claim.expires_at_unix_ms,
            fence,
            token,
        })
    }

    async fn current_session(
        &self,
        node_id: &str,
        epoch: u64,
        session_id: Option<&str>,
        now: u64,
    ) -> Result<NodeSession> {
        let session = self.current_live_session(node_id, now).await?;
        if session.lease_epoch == epoch && session_id.is_none_or(|id| session.session_id == id) {
            Ok(session)
        } else {
            Err(conflict("Node session is not current"))
        }
    }

    async fn current_live_session(&self, node_id: &str, now: u64) -> Result<NodeSession> {
        let snapshot = self
            .store
            .read(MetaRead::CurrentNodeSession {
                node_id: node_id.to_owned(),
            })
            .await?;
        match snapshot.entity {
            Some(MetaEntity::NodeSession(session)) if session.is_live_at_unix_ms(now) => {
                Ok(session)
            }
            _ => Err(conflict("Node session is not current")),
        }
    }

    async fn chunk(&self, chunk_id: &crate::dfs::ChunkId) -> Result<ChunkObject> {
        match self
            .store
            .read(MetaRead::DfsChunk(chunk_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsChunk(chunk)) => Ok(chunk),
            _ => Err(not_found("DFS chunk is missing")),
        }
    }

    async fn copy(&self, copy_id: &CopyId) -> Result<CopyRecord> {
        match self
            .store
            .read(MetaRead::DfsCopy(copy_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsCopy(copy)) => Ok(copy),
            _ => Err(not_found("DFS copy is missing")),
        }
    }

    async fn placement(&self, chunk_id: &crate::dfs::ChunkId) -> Result<PlacementRecord> {
        match self
            .store
            .read(MetaRead::DfsPlacement(chunk_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsPlacement(placement)) => Ok(placement),
            _ => Err(not_found("DFS placement is missing")),
        }
    }

    async fn replication_task(&self, task_id: &ReplicationTaskId) -> Result<ReplicationTask> {
        match self
            .store
            .read(MetaRead::DfsReplicationTask(task_id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsReplicationTask(task)) => Ok(task),
            _ => Err(not_found("DFS replication task is missing")),
        }
    }

    async fn live_ready_copy_count(&self, placement: &PlacementRecord, now: u64) -> Result<usize> {
        self.live_ready_copy_count_excluding(placement, now, None)
            .await
    }

    async fn live_ready_copy_count_excluding(
        &self,
        placement: &PlacementRecord,
        now: u64,
        excluded_copy_id: Option<&CopyId>,
    ) -> Result<usize> {
        let chunk = self.chunk(&placement.chunk_id).await?;
        let mut nodes = HashSet::new();
        for copy_id in &placement.copies {
            if excluded_copy_id.is_some_and(|excluded| excluded == copy_id) {
                continue;
            }
            let copy = self.copy(copy_id).await?;
            if !copy_matches_chunk(&copy, &chunk) {
                continue;
            }
            if let CopyLocation::Node { node_id, .. } = &copy.location
                && let Ok(session) = self.current_live_session(node_id, now).await
                && let Some(serving_copy) = serving_read_copy(&copy, &session, now)
                && let CopyLocation::Node { node_id, .. } = serving_copy.location
            {
                nodes.insert(node_id);
            }
        }
        Ok(nodes.len())
    }

    async fn build_replication_claim(
        &self,
        request: &ClaimReplicationTask,
        caller_session: &NodeSession,
        task: &ReplicationTask,
        replication: &ReplicationConfig,
        lease_seconds: u64,
        now: u64,
    ) -> Result<Option<(ReplicationClaim, ReplicationTask, Vec<TxnCondition>)>> {
        let chunk = self.chunk(&task.chunk_id).await?;
        let placement = match self.placement(&task.chunk_id).await {
            Ok(placement) => placement,
            Err(_) => return Ok(None),
        };
        if placement.placement_epoch != task.placement_epoch {
            return Ok(None);
        }
        let mut source = None;
        for copy_id in &placement.copies {
            let copy = self.copy(copy_id).await?;
            if !copy_matches_chunk(&copy, &chunk) {
                continue;
            }
            if let CopyLocation::Node { node_id, .. } = &copy.location
                && node_id == &request.caller_id
                && serving_read_copy(&copy, caller_session, now).is_some()
            {
                source = Some(copy);
                break;
            }
        }
        let Some(source) = source else {
            return Ok(None);
        };
        let mut repair_replication = replication.clone();
        repair_replication.sync_required_copies = repair_replication.desired_copies;
        repair_replication.local_copy = LocalCopyPolicy::Required;
        let sessions_snapshot = self.store.read(MetaRead::CurrentNodeSessions).await?;
        let mut storage_sessions = sessions_snapshot
            .entities
            .into_iter()
            .filter_map(|entity| match entity {
                MetaEntity::NodeSession(session)
                    if session.is_live_at_unix_ms(now) && !session.storage_devices.is_empty() =>
                {
                    Some(session)
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        storage_sessions.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        // Insufficient live capacity is a normal repair wait state. Backend
        // failures above still propagate; this pure placement computation
        // only checks whether the initialized policy can be satisfied now.
        let Ok(groups) =
            build_replica_groups(&request.caller_id, &repair_replication, &storage_sessions)
        else {
            return Ok(None);
        };
        let group = groups
            .into_iter()
            .find(|group| {
                group.targets.first().is_some_and(|target| {
                    target.node_id == request.caller_id
                        && target.node_epoch == request.caller_node_epoch
                })
            })
            .ok_or_else(|| conflict("no repair replica group starts with the source worker"))?;
        if group.targets.len() < usize::from(repair_replication.desired_copies) {
            return Err(conflict("repair placement cannot satisfy desired replicas"));
        }
        let claim = ReplicationClaim {
            task_id: task.id.clone(),
            operation_id: request.operation_id.clone(),
            worker_node_id: request.caller_id.clone(),
            worker_node_epoch: request.caller_node_epoch,
            worker_session_id: request.caller_session_id.clone(),
            expires_at_unix_ms: now.saturating_add(lease_seconds.saturating_mul(1000)),
            fence: task.attempt.saturating_add(1).into(),
            chunk,
            source_copy_id: source.id.clone(),
            placement_revision: sessions_snapshot.revision.0,
            replica_group: group,
            replication: repair_replication,
        };
        let mut updated = task.clone();
        updated.state = ReplicationTaskState::Running;
        updated.attempt = updated.attempt.saturating_add(1);
        updated.claim = Some(Box::new(claim.clone()));
        updated.last_error = None;
        updated.next_retry_unix_ms = claim.expires_at_unix_ms;
        let task_condition = match self
            .store
            .read(MetaRead::DfsReplicationTask(task.id.clone()))
            .await?
            .entity
        {
            Some(MetaEntity::DfsReplicationTask(current))
                if current == *task || task_reactivates_completed(task, &current) =>
            {
                TxnCondition::EntityEquals(MetaEntity::DfsReplicationTask(current))
            }
            None => TxnCondition::Missing(MetaKey::DfsReplicationTask(task.id.clone())),
            _ => return Ok(None),
        };
        let conditions = vec![
            task_condition,
            TxnCondition::EntityEquals(MetaEntity::DfsPlacement(placement)),
            TxnCondition::EntityEquals(MetaEntity::DfsChunk(claim.chunk.clone())),
            TxnCondition::EntityEquals(MetaEntity::DfsCopy(source)),
        ];
        Ok(Some((claim, updated, conditions)))
    }

    async fn validate_repair_acks(
        &self,
        claim: &ReplicationClaim,
        durable_acks: &[ReplicaAck],
        now: u64,
        conditions: &mut Vec<TxnCondition>,
    ) -> Result<Vec<CopyRecord>> {
        if durable_acks.len() < usize::from(claim.replication.desired_copies) {
            return Err(invalid(
                "repair report has fewer ACKs than desired replicas",
            ));
        }
        let mut nodes = HashSet::new();
        let mut domains = HashSet::new();
        let mut copies = Vec::with_capacity(durable_acks.len());
        for ack in durable_acks {
            if ack.operation_id != claim.operation_id
                || ack.chunk_id != claim.chunk.id
                || ack.placement_revision != claim.placement_revision
                || ack.placement_epoch != claim.replica_group.placement_epoch
                || ack.persisted_bytes != claim.chunk.length
                || ack.verified_digest != claim.chunk.content_digest
            {
                return Err(invalid("repair ReplicaAck does not prove the claim chunk"));
            }
            if !nodes.insert(ack.node_id.clone()) {
                return Err(invalid("repair report contains duplicate Node ACKs"));
            }
            let target = claim
                .replica_group
                .targets
                .iter()
                .find(|target| {
                    target.node_id == ack.node_id
                        && target.node_epoch == ack.node_epoch
                        && target.device.device_id == ack.device_id
                        && target.device.device_epoch == ack.device_epoch
                })
                .ok_or_else(|| conflict("repair ACK target is not in the claim group"))?;
            let session = self
                .current_session(&ack.node_id, ack.node_epoch, None, now)
                .await?;
            conditions.push(TxnCondition::NodeSessionCurrent {
                node_id: session.node_id.clone(),
                session_id: session.session_id.clone(),
            });
            let device = session
                .storage_devices
                .iter()
                .find(|device| {
                    device.device_id == ack.device_id && device.device_epoch == ack.device_epoch
                })
                .ok_or_else(|| conflict("repair ACK references a stale storage device"))?;
            if ack.catalog_revision < target.device.catalog_revision {
                return Err(conflict("repair ACK catalog is below claim floor"));
            }
            domains.insert(device.failure_domain.clone());
            copies.push(replica_ack_copy_record(ack, &claim.chunk));
        }
        if nodes.len() < usize::from(claim.replication.min_distinct_nodes)
            || domains.len() < usize::from(claim.replication.min_distinct_failure_domains)
            || !nodes.contains(&claim.worker_node_id)
        {
            return Err(invalid("repair ACKs do not satisfy placement constraints"));
        }
        Ok(copies)
    }

    async fn validate_write_lease(
        &self,
        caller_id: &str,
        inode_id: &InodeId,
        presented: &WriteLease,
    ) -> Result<WriteLease> {
        if presented.inode_id != *inode_id || presented.owner_node_id != caller_id {
            return Err(conflict(
                "DFS write lease does not belong to caller and inode",
            ));
        }
        let lease_snapshot = self
            .store
            .read(MetaRead::DfsWriteLease(inode_id.clone()))
            .await?;
        let current = match lease_snapshot.entity {
            Some(MetaEntity::DfsWriteLease(lease)) => lease,
            _ => return Err(conflict("DFS operation requires a current write lease")),
        };
        if current.inode_id != presented.inode_id
            || current.owner_node_id != presented.owner_node_id
            || current.owner_session_id != presented.owner_session_id
            || current.lease_epoch != presented.lease_epoch
            || current.expires_at_unix_ms <= now_unix_ms()
        {
            return Err(conflict("DFS write lease changed or expired"));
        }
        Ok(current)
    }

    async fn directory_for_mutation(
        &self,
        namespace_id: &NamespaceId,
        parent: &InodeId,
    ) -> Result<Option<InodeRecord>> {
        let inode = if parent.0 == "1" {
            self.ensure_namespace_root(namespace_id).await?
        } else {
            self.get_inode(parent.clone()).await?
        };
        if inode.kind != InodeKind::Directory || inode.namespace_id != *namespace_id {
            return Err(not_directory(
                "DFS parent is not a directory in this namespace",
            ));
        }
        if inode.attributes.nlink == 0 {
            return Err(not_found("DFS namespace parent has been removed"));
        }
        Ok(Some(inode))
    }

    // The current GetInode API and mount root use one global root ID. Persist
    // that configured namespace root once; never alias a different namespace.
    async fn ensure_namespace_root(&self, namespace_id: &NamespaceId) -> Result<InodeRecord> {
        let id = InodeId::new("1");
        if self
            .store
            .read(MetaRead::DfsInode(id.clone()))
            .await?
            .entity
            .is_none()
        {
            let inode = root_inode(namespace_id.clone());
            let request =
                RequestKey::new("afs-meta", format!("dfs-namespace-root:{}", namespace_id.0));
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsInitializeNamespace);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(request.clone()),
                TxnCondition::Missing(MetaKey::DfsInode(id.clone())),
            ]);
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
                TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request,
                    operation: StoreOperation::DfsInitializeNamespace,
                    result: OperationResult::DfsInode(inode),
                }),
            ]);
            // A concurrent initializer can win; the authoritative read below
            // checks the resulting namespace before any namespace mutation.
            self.store.compare_and_commit(txn).await?;
        }
        let inode = self.get_inode(id).await?;
        if inode.namespace_id != *namespace_id {
            return Err(invalid(
                "DFS root already belongs to another configured namespace",
            ));
        }
        Ok(inode)
    }

    async fn record_rename_noop(
        &self,
        caller_id: String,
        operation_id: OperationId,
        source_dentry: Dentry,
        source_inode: InodeRecord,
        request_digest: [u8; 32],
    ) -> Result<RenameOutcome> {
        let request = RequestKey::new(caller_id, operation_id.0);
        let value = RenameOutcome {
            inode: source_inode.clone(),
            replaced_inode: None,
        };
        let outcome = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsRename,
            result: namespace_result(request_digest, OperationResult::DfsRename(value)),
        };
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsRename);
        txn.conditions.extend([
            TxnCondition::RequestAbsent(request),
            TxnCondition::EntityEquals(MetaEntity::DfsDentry(source_dentry)),
            TxnCondition::EntityEquals(MetaEntity::DfsInode(source_inode)),
        ]);
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(outcome));
        rename_outcome(
            validate_namespace_outcome(self.store.compare_and_commit(txn).await?, request_digest)?,
            StoreOperation::DfsRename,
        )
    }

    async fn replayed_namespace_result(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<OperationResult>> {
        let key = RequestKey::new(caller_id, operation_id.0.clone());
        let snapshot = self.store.read(MetaRead::RequestOutcome(key)).await?;
        let Some(outcome) = snapshot.request_outcome else {
            return Ok(None);
        };
        if outcome.operation != operation {
            return Err(invalid(
                "DFS operation_id was already used for another operation",
            ));
        }
        namespace_result_inner(outcome.result, request_digest).map(Some)
    }

    async fn replayed_namespace_inode(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<InodeRecord>> {
        match self
            .replayed_namespace_result(caller_id, operation_id, operation, request_digest)
            .await?
        {
            Some(OperationResult::DfsInode(inode)) => Ok(Some(inode)),
            None => Ok(None),
            _ => Err(invalid("DFS namespace replay returned the wrong result")),
        }
    }

    async fn replayed_namespace_inode_and_lease(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<(InodeRecord, WriteLease)>> {
        match self
            .replayed_namespace_result(caller_id, operation_id, operation, request_digest)
            .await?
        {
            Some(OperationResult::DfsInodeWithLease { inode, lease }) => Ok(Some((inode, lease))),
            None => Ok(None),
            _ => Err(invalid("DFS namespace replay returned the wrong result")),
        }
    }

    async fn replayed_namespace_rename(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<RenameOutcome>> {
        match self
            .replayed_namespace_result(caller_id, operation_id, operation, request_digest)
            .await?
        {
            Some(OperationResult::DfsRename(value)) => Ok(Some(value)),
            None => Ok(None),
            _ => Err(invalid("DFS namespace replay returned the wrong result")),
        }
    }

    async fn replayed_inode(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
    ) -> Result<Option<InodeRecord>> {
        let key = RequestKey::new(caller_id, operation_id.0.clone());
        let snapshot = self.store.read(MetaRead::RequestOutcome(key)).await?;
        let Some(outcome) = snapshot.request_outcome else {
            return Ok(None);
        };
        if outcome.operation != operation {
            return Err(invalid(
                "DFS operation_id was already used for another operation",
            ));
        }
        match outcome.result {
            OperationResult::DfsInode(inode) => Ok(Some(inode)),
            _ => Err(invalid("DFS operation replay returned the wrong result")),
        }
    }

    async fn replayed_lease(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
    ) -> Result<Option<WriteLease>> {
        let key = RequestKey::new(caller_id, operation_id.0.clone());
        let snapshot = self.store.read(MetaRead::RequestOutcome(key)).await?;
        let Some(outcome) = snapshot.request_outcome else {
            return Ok(None);
        };
        if outcome.operation != operation {
            return Err(invalid(
                "DFS operation_id was already used for another operation",
            ));
        }
        match outcome.result {
            OperationResult::DfsWriteLease(lease) => Ok(Some(lease)),
            _ => Err(invalid("DFS operation replay returned the wrong result")),
        }
    }

    async fn replayed_replication_claim(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<Option<ReplicationClaim>>> {
        let key = RequestKey::new(caller_id, operation_id.0.clone());
        let snapshot = self.store.read(MetaRead::RequestOutcome(key)).await?;
        let Some(outcome) = snapshot.request_outcome else {
            return Ok(None);
        };
        if outcome.operation != operation {
            return Err(invalid(
                "DFS operation_id was already used for another operation",
            ));
        }
        match outcome.result {
            OperationResult::DfsReplicationClaim {
                request_digest: found,
                claim,
            } if found == request_digest => Ok(Some(claim)),
            OperationResult::DfsReplicationClaim { .. } => Err(invalid(
                "DFS replication claim replay request does not match original",
            )),
            _ => Err(invalid(
                "DFS replication claim replay returned the wrong result",
            )),
        }
    }

    async fn replayed_replication_task(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
    ) -> Result<Option<ReplicationTask>> {
        let key = RequestKey::new(caller_id, operation_id.0.clone());
        let snapshot = self.store.read(MetaRead::RequestOutcome(key)).await?;
        let Some(outcome) = snapshot.request_outcome else {
            return Ok(None);
        };
        if outcome.operation != operation {
            return Err(invalid(
                "DFS operation_id was already used for another operation",
            ));
        }
        match outcome.result {
            OperationResult::DfsNamespace {
                request_digest: found,
                result,
            } if found == request_digest => match *result {
                OperationResult::DfsReplicationTask(task) => Ok(Some(task)),
                _ => Err(invalid(
                    "DFS replication task replay returned the wrong result",
                )),
            },
            OperationResult::DfsNamespace { .. } => Err(invalid(
                "DFS replication report replay request does not match original",
            )),
            OperationResult::DfsReplicationTask(task) => Ok(Some(task)),
            _ => Err(invalid(
                "DFS replication task replay returned the wrong result",
            )),
        }
    }

    async fn record_empty_namespace_outcome(
        &self,
        caller_id: &str,
        operation_id: &OperationId,
        operation: StoreOperation,
        request_digest: [u8; 32],
        mut conditions: Vec<TxnCondition>,
    ) -> Result<()> {
        let request_key = RequestKey::new(caller_id, operation_id.0.clone());
        let mut txn = MetaTxn::new(request_key.clone(), operation);
        txn.conditions
            .push(TxnCondition::RequestAbsent(request_key.clone()));
        txn.conditions.append(&mut conditions);
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: request_key,
                operation,
                result: namespace_result(request_digest, OperationResult::Empty),
            }));
        empty_namespace_outcome(
            self.store.compare_and_commit(txn).await?,
            operation,
            request_digest,
        )
    }
}

fn namespace_request_digest(request: &impl serde::Serialize) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(request)
        .map_err(|_| invalid("DFS namespace request cannot be encoded"))?;
    Ok(*blake3::hash(&bytes).as_bytes())
}

fn namespace_inode_id(caller_id: &str, operation_id: &OperationId) -> InodeId {
    let mut bytes = (caller_id.len() as u64).to_be_bytes().to_vec();
    bytes.extend_from_slice(caller_id.as_bytes());
    bytes.extend_from_slice(operation_id.0.as_bytes());
    InodeId::new(format!("inode:{}", blake3::hash(&bytes).to_hex()))
}

fn namespace_result(request_digest: [u8; 32], result: OperationResult) -> OperationResult {
    OperationResult::DfsNamespace {
        request_digest,
        result: Box::new(result),
    }
}

fn namespace_result_inner(result: OperationResult, expected: [u8; 32]) -> Result<OperationResult> {
    match result {
        OperationResult::DfsNamespace {
            request_digest,
            result,
        } if request_digest == expected => Ok(*result),
        _ => Err(invalid(
            "DFS namespace retry differs from the recorded request or lacks its identity proof",
        )),
    }
}

fn validate_namespace_outcome(mut outcome: TxnOutcome, expected: [u8; 32]) -> Result<TxnOutcome> {
    let stored = match &mut outcome {
        TxnOutcome::Committed { outcome, .. } => Some(outcome),
        TxnOutcome::ConditionFailed {
            existing_outcome, ..
        } => existing_outcome.as_mut(),
    };
    if let Some(stored) = stored {
        stored.result = namespace_result_inner(stored.result.clone(), expected)?;
    }
    Ok(outcome)
}

fn extent_is_inherited(extent: &crate::dfs::Extent, base: &LayoutRoot) -> bool {
    let Some(extent_file_end) = extent.file_offset.checked_add(extent.length) else {
        return false;
    };
    base.inline_extents.iter().any(|candidate| {
        let Some(candidate_file_end) = candidate.file_offset.checked_add(candidate.length) else {
            return false;
        };
        if candidate.chunk_id != extent.chunk_id
            || extent.file_offset < candidate.file_offset
            || extent_file_end > candidate_file_end
        {
            return false;
        }
        candidate
            .chunk_offset
            .checked_add(extent.file_offset - candidate.file_offset)
            == Some(extent.chunk_offset)
    })
}

fn root_inode(namespace_id: NamespaceId) -> InodeRecord {
    InodeRecord {
        namespace_id,
        inode_id: InodeId::new("1"),
        kind: InodeKind::Directory,
        attributes: InodeAttributes {
            mode: 0o755,
            uid: 0,
            gid: 0,
            nlink: 2,
            atime_unix_ms: 0,
            mtime_unix_ms: 0,
            ctime_unix_ms: 0,
        },
        head_version: None,
        symlink_target: None,
        xattrs: Default::default(),
        revision: 1,
    }
}

fn inode_outcome(outcome: TxnOutcome, operation: StoreOperation) -> Result<InodeRecord> {
    let stored = match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } if outcome.operation == operation => outcome,
        TxnOutcome::ConditionFailed { .. } => {
            return Err(conflict("DFS metadata condition changed during commit"));
        }
        _ => {
            return Err(invalid(
                "DFS metadata operation returned an invalid outcome",
            ));
        }
    };
    match stored.result {
        OperationResult::DfsInode(inode) => Ok(inode),
        _ => Err(invalid("DFS metadata operation replayed the wrong result")),
    }
}

fn lease_outcome(outcome: TxnOutcome, operation: StoreOperation) -> Result<WriteLease> {
    let stored = match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } if outcome.operation == operation => outcome,
        TxnOutcome::ConditionFailed { .. } => {
            return Err(conflict(
                "DFS metadata condition changed during lease update",
            ));
        }
        _ => {
            return Err(invalid(
                "DFS metadata operation returned an invalid outcome",
            ));
        }
    };
    match stored.result {
        OperationResult::DfsWriteLease(lease) => Ok(lease),
        _ => Err(invalid("DFS metadata operation replayed the wrong result")),
    }
}

fn replication_claim_outcome(
    outcome: TxnOutcome,
    request_digest: [u8; 32],
) -> Result<Option<ReplicationClaim>> {
    match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } => {
            if outcome.operation != StoreOperation::DfsClaimReplicationTask {
                return Err(invalid(
                    "DFS replication claim replay used another operation",
                ));
            }
            match outcome.result {
                OperationResult::DfsReplicationClaim {
                    request_digest: found,
                    claim,
                } if found == request_digest => Ok(claim),
                OperationResult::DfsReplicationClaim { .. } => Err(invalid(
                    "DFS replication claim replay request does not match original",
                )),
                _ => Err(invalid("DFS replication claim outcome has wrong result")),
            }
        }
        TxnOutcome::ConditionFailed { .. } => {
            Err(conflict("DFS replication claim changed during commit"))
        }
    }
}

fn replication_task_outcome(
    outcome: TxnOutcome,
    operation: StoreOperation,
    request_digest: [u8; 32],
) -> Result<ReplicationTask> {
    match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } => {
            if outcome.operation != operation {
                return Err(invalid(
                    "DFS replication task replay used another operation",
                ));
            }
            match namespace_result_inner(outcome.result, request_digest)? {
                OperationResult::DfsReplicationTask(task) => Ok(task),
                _ => Err(invalid("DFS replication task outcome has wrong result")),
            }
        }
        TxnOutcome::ConditionFailed { .. } => {
            Err(conflict("DFS replication task changed during commit"))
        }
    }
}

fn empty_namespace_outcome(
    outcome: TxnOutcome,
    operation: StoreOperation,
    request_digest: [u8; 32],
) -> Result<()> {
    match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } => {
            if outcome.operation != operation {
                return Err(invalid("DFS empty replay used another operation"));
            }
            match namespace_result_inner(outcome.result, request_digest)? {
                OperationResult::Empty => Ok(()),
                _ => Err(invalid("DFS empty outcome has wrong result")),
            }
        }
        TxnOutcome::ConditionFailed { .. } => {
            Err(conflict("DFS empty operation changed during commit"))
        }
    }
}

fn inode_and_lease_outcome(
    outcome: TxnOutcome,
    operation: StoreOperation,
) -> Result<(InodeRecord, WriteLease)> {
    let stored = match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } if outcome.operation == operation => outcome,
        TxnOutcome::ConditionFailed { .. } => {
            return Err(conflict("DFS metadata condition changed during create"));
        }
        _ => {
            return Err(invalid(
                "DFS metadata operation returned an invalid outcome",
            ));
        }
    };
    match stored.result {
        OperationResult::DfsInodeWithLease { inode, lease } => Ok((inode, lease)),
        _ => Err(invalid("DFS metadata operation replayed the wrong result")),
    }
}

fn rename_outcome(outcome: TxnOutcome, operation: StoreOperation) -> Result<RenameOutcome> {
    let stored = match outcome {
        TxnOutcome::Committed { outcome, .. }
        | TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } if outcome.operation == operation => outcome,
        TxnOutcome::ConditionFailed { .. } => {
            return Err(conflict("DFS metadata condition changed during rename"));
        }
        _ => {
            return Err(invalid(
                "DFS metadata operation returned an invalid outcome",
            ));
        }
    };
    match stored.result {
        OperationResult::DfsRename(outcome) => Ok(outcome),
        _ => Err(invalid("DFS metadata operation replayed the wrong result")),
    }
}

async fn inode_from_view(
    view: &MetaReadView,
    namespace_id: &NamespaceId,
    inode_id: InodeId,
) -> Result<InodeRecord> {
    let root = inode_id.0 == "1";
    match view.read(MetaRead::DfsInode(inode_id)).await?.entity {
        Some(MetaEntity::DfsInode(inode)) if inode.namespace_id == *namespace_id => Ok(inode),
        None if root => Ok(root_inode(namespace_id.clone())),
        _ => Err(Error::coded(
            afs_error::NODE_VFS_NOT_FOUND,
            "DFS inode was not found",
        )),
    }
}

async fn dentry_from_view(view: &MetaReadView, key: DentryKey) -> Result<Option<Dentry>> {
    match view.read(MetaRead::DfsDentry(key)).await?.entity {
        Some(MetaEntity::DfsDentry(dentry)) => Ok(Some(dentry)),
        _ => Ok(None),
    }
}

async fn directory_entries_from_view(
    view: &MetaReadView,
    namespace_id: &NamespaceId,
    parent_inode_id: &InodeId,
) -> Result<Vec<Dentry>> {
    let snapshot = view
        .read(MetaRead::DfsDirectory {
            namespace_id: namespace_id.clone(),
            parent_inode_id: parent_inode_id.clone(),
        })
        .await?;
    Ok(snapshot
        .entities
        .into_iter()
        .filter_map(|entity| match entity {
            MetaEntity::DfsDentry(dentry) => Some(dentry),
            _ => None,
        })
        .collect())
}

async fn directory_is_descendant_or_same(
    view: &MetaReadView,
    namespace_id: &NamespaceId,
    ancestor_inode_id: &InodeId,
    child_inode_id: &InodeId,
) -> Result<bool> {
    if ancestor_inode_id == child_inode_id {
        return Ok(true);
    }
    let mut current = child_inode_id.clone();
    let mut seen = HashSet::new();
    while seen.insert(current.clone()) {
        let Some(parent) = parent_inode_from_view(view, namespace_id, &current).await? else {
            return Ok(false);
        };
        if &parent == ancestor_inode_id {
            return Ok(true);
        }
        if parent.0 == "1" {
            return Ok(false);
        }
        current = parent;
    }
    Ok(true)
}

async fn parent_inode_from_view(
    view: &MetaReadView,
    namespace_id: &NamespaceId,
    inode_id: &InodeId,
) -> Result<Option<InodeId>> {
    if inode_id.0 == "1" {
        return Ok(None);
    }
    let cursor = InodeId::new("1");
    let mut seen = HashSet::new();
    while seen.insert(cursor.clone()) {
        for dentry in directory_entries_from_view(view, namespace_id, &cursor).await? {
            if dentry.inode_id == *inode_id {
                return Ok(Some(cursor));
            }
            let Ok(inode) = inode_from_view(view, namespace_id, dentry.inode_id.clone()).await
            else {
                continue;
            };
            if inode.kind == InodeKind::Directory {
                // Depth-first scan is acceptable for the current fullsnapshot Meta slice.
                if let Some(parent) = parent_inode_from_subtree(
                    view,
                    namespace_id,
                    &inode.inode_id,
                    inode_id,
                    &mut HashSet::new(),
                )
                .await?
                {
                    return Ok(Some(parent));
                }
            }
        }
    }
    Ok(None)
}

fn parent_inode_from_subtree<'a>(
    view: &'a MetaReadView,
    namespace_id: &'a NamespaceId,
    directory_inode_id: &'a InodeId,
    inode_id: &'a InodeId,
    seen: &'a mut HashSet<InodeId>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Option<InodeId>>> + Send + 'a>> {
    Box::pin(async move {
        if !seen.insert(directory_inode_id.clone()) {
            return Ok(None);
        }
        for dentry in directory_entries_from_view(view, namespace_id, directory_inode_id).await? {
            if dentry.inode_id == *inode_id {
                return Ok(Some(directory_inode_id.clone()));
            }
            let Ok(inode) = inode_from_view(view, namespace_id, dentry.inode_id.clone()).await
            else {
                continue;
            };
            if inode.kind == InodeKind::Directory
                && let Some(parent) =
                    parent_inode_from_subtree(view, namespace_id, &inode.inode_id, inode_id, seen)
                        .await?
            {
                return Ok(Some(parent));
            }
        }
        Ok(None)
    })
}

fn push_parent_namespace_change(txn: &mut MetaTxn, parent: Option<InodeRecord>, delta: i32) {
    let Some(mut parent) = parent else {
        return;
    };
    txn.conditions
        .push(TxnCondition::EntityEquals(MetaEntity::DfsInode(
            parent.clone(),
        )));
    let now = now_unix_ms();
    parent.attributes.mtime_unix_ms = now;
    parent.attributes.ctime_unix_ms = now;
    if delta >= 0 {
        parent.attributes.nlink = parent.attributes.nlink.saturating_add(delta as u32);
    } else {
        parent.attributes.nlink = parent.attributes.nlink.saturating_sub(delta.unsigned_abs());
    }
    parent.revision = parent.revision.saturating_add(1);
    txn.mutations
        .push(TxnMutation::Put(MetaEntity::DfsInode(parent)));
}

fn create_parent_retry_drift_only(previous: &InodeRecord, current: &InodeRecord) -> bool {
    if previous == current
        || previous.namespace_id != current.namespace_id
        || previous.inode_id != current.inode_id
        || previous.kind != InodeKind::Directory
        || current.kind != InodeKind::Directory
        || previous.head_version != current.head_version
        || previous.symlink_target != current.symlink_target
        || previous.xattrs != current.xattrs
        || previous.attributes.mode != current.attributes.mode
        || previous.attributes.uid != current.attributes.uid
        || previous.attributes.gid != current.attributes.gid
        || previous.attributes.nlink != current.attributes.nlink
        || previous.attributes.atime_unix_ms != current.attributes.atime_unix_ms
    {
        return false;
    }
    current.revision > previous.revision
        && current.attributes.mtime_unix_ms >= previous.attributes.mtime_unix_ms
        && current.attributes.ctime_unix_ms >= previous.attributes.ctime_unix_ms
}

fn validate_name(name: &[u8]) -> Result<()> {
    if name.len() > 255 {
        return Err(name_too_long(
            "DFS name exceeds the supported component length",
        ));
    }
    if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') {
        return Err(invalid_name_error());
    }
    Ok(())
}

fn validate_symlink_target(target: &[u8]) -> Result<()> {
    if target.is_empty() {
        return Err(invalid("DFS symlink target must not be empty"));
    }
    if target.len() > 4096 {
        return Err(name_too_long("DFS symlink target exceeds supported length"));
    }
    Ok(())
}

fn xattr_key(name: &[u8]) -> Result<Vec<u8>> {
    if name.is_empty() || name.len() > 255 {
        return Err(name_too_long("DFS xattr name length is unsupported"));
    }
    if !name.starts_with(b"user.") {
        return Err(not_supported("DFS currently supports only user.* xattrs"));
    }
    Ok(name.to_vec())
}

#[derive(Clone, Copy)]
enum AccessMode {
    Read,
    Write,
}

#[derive(Clone, Copy)]
enum XattrAccess {
    Read,
    Write,
}

fn validate_attr_update_permission(
    caller: &CallerContext,
    inode: &InodeRecord,
    update: &crate::dfs::InodeAttrUpdate,
) -> Result<()> {
    if let Some(uid) = update.uid
        && caller.uid != 0
        && !(caller.uid == inode.attributes.uid && uid == inode.attributes.uid)
    {
        return Err(operation_not_permitted(
            "DFS chown requires privileged caller context",
        ));
    }
    if let Some(gid) = update.gid
        && caller.uid != 0
        && !(caller.uid == inode.attributes.uid
            && (gid == inode.attributes.gid || caller_in_group(caller, gid)))
    {
        return Err(operation_not_permitted(
            "DFS chgrp requires owner with target group membership or privileged caller context",
        ));
    }
    if update.mode.is_some() && caller.uid != 0 && caller.uid != inode.attributes.uid {
        return Err(operation_not_permitted(
            "DFS chmod requires owner or privileged caller context",
        ));
    }
    if (update.atime_unix_ms.is_some() || update.mtime_unix_ms.is_some())
        && caller.uid != 0
        && caller.uid != inode.attributes.uid
    {
        if !update.timestamps_now {
            return Err(operation_not_permitted(
                "DFS explicit timestamps require owner or privileged caller context",
            ));
        }
        ensure_basic_access(caller, inode, AccessMode::Write)?;
    }
    Ok(())
}

fn cleared_write_privileges(mode: u32) -> u32 {
    let clear = 0o4000 | if mode & 0o0010 != 0 { 0o2000 } else { 0 };
    mode & !clear
}

fn ensure_xattr_permission(
    caller: &CallerContext,
    inode: &InodeRecord,
    name: &[u8],
    access: XattrAccess,
) -> Result<()> {
    if !matches!(inode.kind, InodeKind::Regular | InodeKind::Directory) {
        return match access {
            XattrAccess::Read => Err(no_data(
                "DFS user xattrs are not available on this inode kind",
            )),
            XattrAccess::Write => Err(operation_not_permitted(
                "DFS user xattrs can be written only on regular files and directories",
            )),
        };
    }
    if matches!(inode.kind, InodeKind::Directory)
        && inode.attributes.mode & 0o1000 != 0
        && matches!(access, XattrAccess::Write)
        && caller.uid != 0
        && caller.uid != inode.attributes.uid
    {
        return Err(operation_not_permitted(
            "DFS sticky directory xattr writes require owner or privileged caller context",
        ));
    }
    let mode = match access {
        XattrAccess::Read => AccessMode::Read,
        XattrAccess::Write => AccessMode::Write,
    };
    let _ = name;
    ensure_basic_access(caller, inode, mode)
}

fn inherited_child_gid(caller: &CallerContext, parent: Option<&InodeRecord>) -> u32 {
    if let Some(parent) = parent
        && parent.attributes.mode & 0o2000 != 0
    {
        parent.attributes.gid
    } else {
        caller.gid
    }
}

fn ensure_sticky_parent_allows(
    caller: &CallerContext,
    parent: Option<&InodeRecord>,
    victim: &InodeRecord,
) -> Result<()> {
    let Some(parent) = parent else {
        return Ok(());
    };
    if parent.attributes.mode & 0o1000 == 0
        || caller.uid == 0
        || caller.uid == parent.attributes.uid
        || caller.uid == victim.attributes.uid
    {
        Ok(())
    } else {
        Err(permission_denied(
            "DFS sticky directory mutation requires directory owner, victim owner, or privileged caller context",
        ))
    }
}

fn ensure_directory_create_access(caller: &CallerContext, inode: &InodeRecord) -> Result<()> {
    if caller.uid == 0 {
        return Ok(());
    }
    let shift = if caller.uid == inode.attributes.uid {
        6
    } else if caller_in_group(caller, inode.attributes.gid) {
        3
    } else {
        0
    };
    if ((inode.attributes.mode >> shift) & 0o3) == 0o3 {
        Ok(())
    } else {
        Err(permission_denied(
            "DFS namespace mutation requires parent write and search permission",
        ))
    }
}

fn ensure_basic_access(
    caller: &CallerContext,
    inode: &InodeRecord,
    access: AccessMode,
) -> Result<()> {
    if caller.uid == 0 {
        return Ok(());
    }
    let mask = match access {
        AccessMode::Read => 0o4,
        AccessMode::Write => 0o2,
    };
    let shift = if caller.uid == inode.attributes.uid {
        6
    } else if caller_in_group(caller, inode.attributes.gid) {
        3
    } else {
        0
    };
    if ((inode.attributes.mode >> shift) & mask) != 0 {
        Ok(())
    } else {
        Err(permission_denied(
            "DFS caller lacks inode permission bits for metadata operation",
        ))
    }
}

fn caller_in_group(caller: &CallerContext, gid: u32) -> bool {
    caller.gid == gid || caller.supplementary_gids.contains(&gid)
}

fn task_is_claimable(task: &ReplicationTask, now: u64) -> bool {
    match task.state {
        ReplicationTaskState::Pending => true,
        ReplicationTaskState::RetryWaiting => task.next_retry_unix_ms <= now,
        // The caller checks precise worker liveness and freezes that evidence
        // in the claim transaction; time alone cannot decide this state.
        ReplicationTaskState::Running => true,
        ReplicationTaskState::Completed => false,
        ReplicationTaskState::BlockedNoSource => task.next_retry_unix_ms <= now,
    }
}

fn repair_task_id(chunk_id: &crate::dfs::ChunkId) -> ReplicationTaskId {
    ReplicationTaskId::new(format!("repair:{}", chunk_id.0))
}

fn task_reactivates_completed(task: &ReplicationTask, current: &ReplicationTask) -> bool {
    current.id == task.id
        && current.chunk_id == task.chunk_id
        && current.state == ReplicationTaskState::Completed
        && task.state == ReplicationTaskState::Pending
}

fn retry_backoff_ms(attempt: u32) -> u64 {
    let shift = attempt.min(6);
    1_000u64.saturating_mul(1u64 << shift)
}

fn copy_is_on_node(copy: &CopyRecord, node_id: &str) -> bool {
    matches!(
        &copy.location,
        CopyLocation::Node {
            node_id: copy_node,
            ..
        } if copy_node == node_id
    )
}

fn copy_matches_chunk(copy: &CopyRecord, chunk: &ChunkObject) -> bool {
    copy.chunk_id == chunk.id
        && copy.persisted_bytes == chunk.length
        && copy.verified_digest == chunk.content_digest
}

fn replica_ack_copy_record(ack: &ReplicaAck, chunk: &ChunkObject) -> CopyRecord {
    CopyRecord {
        id: CopyId::new(format!(
            "{}:{}:{}:{}",
            ack.node_id, ack.node_epoch, ack.device_id, chunk.id.0
        )),
        chunk_id: chunk.id.clone(),
        role: CopyRole::DurableReplica,
        location: CopyLocation::Node {
            node_id: ack.node_id.clone(),
            node_epoch: ack.node_epoch,
            device_id: ack.device_id.clone(),
            device_epoch: ack.device_epoch,
            catalog_revision: ack.catalog_revision,
        },
        state: CopyState::Ready,
        persisted_bytes: ack.persisted_bytes,
        verified_digest: ack.verified_digest.clone(),
    }
}

fn build_replica_groups(
    caller_id: &str,
    replication: &ReplicationConfig,
    sessions: &[NodeSession],
) -> Result<Vec<ReplicaGroup>> {
    let mut targets = sessions
        .iter()
        .filter_map(preferred_replica_target)
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then(left.node_epoch.cmp(&right.node_epoch))
            .then(left.device.device_id.cmp(&right.device.device_id))
            .then(left.device.device_epoch.cmp(&right.device.device_epoch))
    });
    targets.dedup_by(|left, right| left.node_id == right.node_id);

    let caller_target = targets
        .iter()
        .find(|target| target.node_id == caller_id)
        .cloned();
    if replication.local_copy == LocalCopyPolicy::Required && caller_target.is_none() {
        return Err(conflict(
            "DFS placement requires a caller-local storage target for local-copy=required",
        ));
    }

    let required_targets = usize::from(replication.sync_required_copies)
        .max(usize::from(replication.min_distinct_nodes))
        .max(usize::from(replication.min_distinct_failure_domains));
    if targets.len() < required_targets {
        return Err(conflict(
            "DFS placement has too few live storage nodes for the configured policy",
        ));
    }
    let target_count = usize::from(replication.desired_copies).min(targets.len());
    if target_count < required_targets {
        return Err(conflict(
            "DFS placement cannot satisfy sync, node or failure-domain minima",
        ));
    }

    let mut groups = Vec::new();
    match (replication.local_copy, caller_target) {
        (LocalCopyPolicy::Required | LocalCopyPolicy::Preferred, Some(head)) => {
            let tail = targets
                .iter()
                .filter(|target| target.node_id != head.node_id)
                .cloned()
                .collect::<Vec<_>>();
            let rotations = tail.len().max(1);
            for seed in 0..rotations {
                if let Some(group) =
                    build_replica_group(replication, Some(head.clone()), &tail, target_count, seed)
                {
                    groups.push(group);
                }
            }
        }
        _ => {
            for seed in 0..targets.len() {
                if let Some(group) =
                    build_replica_group(replication, None, &targets, target_count, seed)
                {
                    groups.push(group);
                }
            }
        }
    }

    groups.sort_by(|left, right| left.id.0.cmp(&right.id.0));
    groups.dedup_by(|left, right| left.id == right.id);
    if groups.is_empty() {
        return Err(conflict(
            "DFS placement could not build a replica group satisfying the configured policy",
        ));
    }
    Ok(groups)
}

fn preferred_replica_target(session: &NodeSession) -> Option<ReplicaTarget> {
    let mut devices = session.storage_devices.clone();
    devices.sort_by(|left, right| {
        left.device_id
            .cmp(&right.device_id)
            .then(left.device_epoch.cmp(&right.device_epoch))
            .then(left.failure_domain.cmp(&right.failure_domain))
    });
    devices.into_iter().next().map(|device| ReplicaTarget {
        node_id: session.node_id.clone(),
        node_epoch: session.lease_epoch,
        data_endpoint: session.data_addr.clone(),
        device,
    })
}

fn build_replica_group(
    replication: &ReplicationConfig,
    head: Option<ReplicaTarget>,
    candidates: &[ReplicaTarget],
    target_count: usize,
    seed: usize,
) -> Option<ReplicaGroup> {
    let mut selected = Vec::with_capacity(target_count);
    let mut nodes = HashSet::new();
    let mut domains = HashSet::new();
    if let Some(head) = head {
        nodes.insert(head.node_id.clone());
        domains.insert(head.device.failure_domain.clone());
        selected.push(head);
    }

    let rotated = (0..candidates.len())
        .map(|offset| candidates[(seed + offset) % candidates.len()].clone())
        .collect::<Vec<_>>();
    for prefer_new_domain in [true, false] {
        for target in &rotated {
            if selected.len() >= target_count {
                break;
            }
            if nodes.contains(&target.node_id) {
                continue;
            }
            let new_domain = !domains.contains(&target.device.failure_domain);
            if prefer_new_domain && !new_domain {
                continue;
            }
            nodes.insert(target.node_id.clone());
            domains.insert(target.device.failure_domain.clone());
            selected.push(target.clone());
        }
    }

    if selected.len() < usize::from(replication.sync_required_copies)
        || nodes.len() < usize::from(replication.min_distinct_nodes)
        || domains.len() < usize::from(replication.min_distinct_failure_domains)
    {
        return None;
    }
    let placement_epoch = placement_hash(replication, &selected);
    Some(ReplicaGroup {
        id: ReplicaGroupId::new(format!("group:{placement_epoch:016x}")),
        placement_epoch,
        targets: selected,
    })
}

fn replica_targets_match_requested_floor(
    requested: &[ReplicaTarget],
    current: &[ReplicaTarget],
) -> bool {
    requested.len() == current.len()
        && requested.iter().zip(current).all(|(requested, current)| {
            requested.node_id == current.node_id
                && requested.node_epoch == current.node_epoch
                && requested.data_endpoint == current.data_endpoint
                && requested.device.device_id == current.device.device_id
                && requested.device.device_epoch == current.device.device_epoch
                && requested.device.failure_domain == current.device.failure_domain
                && requested.device.catalog_revision <= current.device.catalog_revision
        })
}

fn replica_write_token(
    request: &ValidateReplicaWriteRequest,
    fence: u64,
    expires_at_unix_ms: u64,
) -> String {
    let mut hash = StableHash::new();
    hash.write_str(&request.requester_node_id);
    hash.write_u64(request.requester_node_epoch);
    hash.write_str(&request.initiator_node_id);
    hash.write_u64(request.initiator_node_epoch);
    hash.write_str(&request.operation_id.0);
    hash.write_str(&request.chunk_id.0);
    hash.write_u64(request.chunk_length);
    hash.write_bytes(&request.content_digest.bytes);
    hash.write_u64(request.placement_revision);
    hash.write_u64(request.placement_epoch);
    hash.write_str(&request.replica_group_id.0);
    hash.write_u64(u64::from(request.target_index));
    for target in &request.ordered_targets {
        hash.write_str(&target.node_id);
        hash.write_u64(target.node_epoch);
        hash.write_str(&target.device.device_id);
        hash.write_u64(target.device.device_epoch);
        hash.write_u64(target.device.catalog_revision);
    }
    hash.write_u64(fence);
    hash.write_u64(expires_at_unix_ms);
    format!("dfs-replica-write:{:016x}", hash.finish_nonzero())
}

fn placement_hash(replication: &ReplicationConfig, targets: &[ReplicaTarget]) -> u64 {
    let mut hash = StableHash::new();
    hash.write_u64(u64::from(replication.desired_copies));
    hash.write_u64(u64::from(replication.sync_required_copies));
    hash.write_u64(u64::from(replication.min_distinct_nodes));
    hash.write_u64(u64::from(replication.min_distinct_failure_domains));
    hash.write_u64(match replication.local_copy {
        LocalCopyPolicy::Required => 1,
        LocalCopyPolicy::Preferred => 2,
        LocalCopyPolicy::NotRequired => 3,
    });
    for target in targets {
        hash.write_str(&target.node_id);
        hash.write_u64(target.node_epoch);
        hash.write_str(&target.data_endpoint);
        hash.write_str(&target.device.device_id);
        hash.write_u64(target.device.device_epoch);
        hash.write_str(&target.device.failure_domain);
    }
    hash.finish_nonzero()
}

struct StableHash(u64);

impl StableHash {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    fn write_str(&mut self, value: &str) {
        self.write_bytes(value.as_bytes());
        self.write_bytes(&[0xff]);
    }

    fn write_u64(&mut self, value: u64) {
        self.write_bytes(&value.to_le_bytes());
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.0 ^= u64::from(*byte);
            self.0 = self.0.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn finish_nonzero(self) -> u64 {
        if self.0 == 0 { 1 } else { self.0 }
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn lease_expiry(lease_seconds: u64) -> Result<u64> {
    if lease_seconds == 0 {
        return Err(invalid("lease_seconds must be greater than zero"));
    }
    Ok(now_unix_ms().saturating_add(lease_seconds.saturating_mul(1000)))
}

fn not_found(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_NOT_FOUND, message)
}

fn not_directory(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_NOT_DIRECTORY, message)
}

fn is_directory(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_IS_DIRECTORY, message)
}

fn directory_not_empty(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_DIRECTORY_NOT_EMPTY, message)
}

fn already_exists(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_ALREADY_EXISTS, message)
}

fn name_too_long(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_NAME_TOO_LONG, message)
}

fn no_data(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_NO_DATA, message)
}

fn not_supported(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_NOT_SUPPORTED, message)
}

fn operation_not_permitted(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_OPERATION_NOT_PERMITTED, message)
}

fn permission_denied(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_PERMISSION_DENIED, message)
}

fn invalid_name_error() -> Error {
    invalid("DFS name must be one non-empty path component")
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::META_CATALOG_INVALID_REQUEST, message)
}

fn require_id(value: &str, field: &str) -> Result<()> {
    if value.is_empty() {
        Err(invalid(format!("{field} is required")))
    } else {
        Ok(())
    }
}

fn conflict(message: impl Into<String>) -> Error {
    Error::coded(afs_error::META_DFS_CONFLICT, message)
}

/// A committed copy records the process that produced its durable receipt;
/// reads use the live process serving that same persistent device. After a
/// restart, registration follows LocalChunkStore recovery and advertises the
/// recovered catalog revision. Its floor must cover the original receipt, so
/// an older catalog/device cannot acquire read authority for that copy.
/// Same-process writes may advance beyond the startup descriptor, so that
/// additional floor is required only across process epochs. This projection
/// is read-only: neither persisted copy evidence nor write/replica fencing is
/// changed. Signing the projected epoch rejects old grants on a new process.
pub(super) fn serving_read_copy(
    copy: &CopyRecord,
    session: &NodeSession,
    now: u64,
) -> Option<CopyRecord> {
    let CopyLocation::Node {
        node_id,
        node_epoch,
        device_id,
        device_epoch,
        catalog_revision,
    } = &copy.location
    else {
        return None;
    };
    if !copy.is_ready_durable()
        || node_id != &session.node_id
        || *node_epoch == 0
        || session.lease_epoch < *node_epoch
        || !session.is_live_at_unix_ms(now)
        || !session.storage_devices.iter().any(|device| {
            device.device_id == *device_id
                && device.device_epoch == *device_epoch
                && (session.lease_epoch == *node_epoch
                    || device.catalog_revision >= *catalog_revision)
        })
    {
        return None;
    }
    let mut serving_copy = copy.clone();
    if let CopyLocation::Node { node_epoch, .. } = &mut serving_copy.location {
        *node_epoch = session.lease_epoch;
    }
    Some(serving_copy)
}

async fn live_read_session(
    view: &MetaReadView,
    node_id: &str,
    epoch: u64,
    now: u64,
) -> Result<NodeSession> {
    match view
        .read(MetaRead::CurrentNodeSession {
            node_id: node_id.into(),
        })
        .await?
        .entity
    {
        Some(MetaEntity::NodeSession(session))
            if epoch != 0 && session.lease_epoch == epoch && session.is_live_at_unix_ms(now) =>
        {
            Ok(session)
        }
        _ => Err(permission_denied("read grant Node session is not live")),
    }
}

async fn live_ready_copy_count_from_observed_excluding(
    copies: &[CopyRecord],
    excluded: &HashSet<CopyId>,
    service: &DfsService,
    now: u64,
) -> Result<usize> {
    let mut nodes = HashSet::new();
    for copy in copies {
        if excluded.contains(&copy.id) {
            continue;
        }
        if let CopyLocation::Node { node_id, .. } = &copy.location {
            let session = match service.current_live_session(node_id, now).await {
                Ok(session) => session,
                Err(error) if error.code() == afs_error::META_DFS_CONFLICT => continue,
                Err(error) => return Err(error),
            };
            if let Some(serving_copy) = serving_read_copy(copy, &session, now)
                && let CopyLocation::Node { node_id, .. } = serving_copy.location
            {
                nodes.insert(node_id);
            }
        }
    }
    Ok(nodes.len())
}

fn read_chunk_intervals(
    layout: &LayoutRoot,
    chunk: &crate::dfs::ChunkObject,
) -> Result<Vec<(u64, u64)>> {
    let mut intervals = Vec::new();
    for extent in layout
        .inline_extents
        .iter()
        .filter(|extent| extent.chunk_id == chunk.id)
    {
        let length = extent
            .length
            .min(layout.file_length.saturating_sub(extent.file_offset));
        let end = extent
            .chunk_offset
            .checked_add(length)
            .filter(|end| *end <= chunk.length)
            .ok_or_else(|| invalid("read layout extent exceeds immutable chunk"))?;
        if length != 0 {
            intervals.push((extent.chunk_offset, end));
        }
    }
    intervals.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::new();
    for (start, end) in intervals {
        if let Some(last) = merged.last_mut().filter(|last| start <= last.1) {
            last.1 = last.1.max(end);
        } else {
            merged.push((start, end));
        }
    }
    Ok(merged
        .into_iter()
        .map(|(start, end)| (start, end - start))
        .collect())
}

fn read_range_covered(intervals: &[(u64, u64)], offset: u64, length: u64) -> bool {
    length != 0
        && offset.checked_add(length).is_some_and(|end| {
            intervals.iter().any(|(start, len)| {
                offset >= *start && start.checked_add(*len).is_some_and(|limit| end <= limit)
            })
        })
}

#[cfg(all(test, feature = "dfs"))]
mod read_recovery_tests {
    use super::*;
    use crate::dfs::{
        DfsReadValidation, Extent, LayoutRootId, StorageDeviceDescriptor, ValidateDfsReadGrants,
    };
    use crate::meta::store::{NodeSessionLease, Store, memory::MemoryBackend};
    use crate::node::chunk::{ChunkStore, LocalChunkStore, StagedChunk};

    struct Fixture {
        service: DfsService,
        store: Arc<Store>,
        temp: tempfile::TempDir,
        local: LocalChunkStore,
        request: DfsChunkSourcesRequest,
        copy: CopyRecord,
    }

    impl Fixture {
        async fn sources(&self) -> Vec<SourceCandidate> {
            self.service
                .chunk_sources(self.request.clone())
                .await
                .unwrap()
                .chunks
                .remove(0)
                .sources
        }

        async fn validate(&self, source: &SourceCandidate, epoch: u64) -> Result<()> {
            self.service
                .validate_read_grants(ValidateDfsReadGrants {
                    receiver_node_id: "receiver".into(),
                    receiver_node_epoch: epoch,
                    peer_node_id: "reader".into(),
                    validations: vec![DfsReadValidation {
                        grant: source.read_grant.clone(),
                        chunk_id: source.chunk_id.clone(),
                        copy_id: source.copy_id.clone(),
                        chunk_offset: 0,
                        length: 4,
                    }],
                })
                .await
                .map(|_| ())
        }

        async fn restart(&self, device: StorageDeviceDescriptor) -> NodeSession {
            register(&self.store, "receiver", "restarted", vec![device]).await;
            match self
                .store
                .read(MetaRead::CurrentNodeSession {
                    node_id: "receiver".into(),
                })
                .await
                .unwrap()
                .entity
            {
                Some(MetaEntity::NodeSession(session)) => session,
                _ => panic!("replacement session is missing"),
            }
        }
    }

    async fn register(
        store: &Store,
        id: &str,
        session: &str,
        devices: Vec<StorageDeviceDescriptor>,
    ) {
        store
            .register_node_session(
                RequestKey::new(id, format!("register-{session}")),
                NodeSessionLease {
                    node_id: id.into(),
                    session_id: session.into(),
                    grpc_addr: "http://127.0.0.1:1".into(),
                    data_addr: "http://127.0.0.1:2".into(),
                    rest_addr: "http://127.0.0.1:3".into(),
                    storage_devices: devices,
                    lease_ttl: std::time::Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
    }

    async fn legacy_direct_commit_fixture() -> (
        DfsService,
        Arc<Store>,
        InodeRecord,
        FileVersion,
        LayoutRoot,
        RequestKey,
    ) {
        let store = Arc::new(
            Store::open(Arc::new(MemoryBackend::default()))
                .await
                .unwrap(),
        );
        let service = DfsService::new(store.clone());
        let mut inode = root_inode(NamespaceId::new("default"));
        inode.inode_id = InodeId::new("legacy-file");
        inode.kind = InodeKind::Regular;
        inode.head_version = Some(FileVersionId::new("legacy-version"));
        inode.revision = 2;
        let version = FileVersion {
            id: FileVersionId::new("legacy-version"),
            inode_id: inode.inode_id.clone(),
            parent_version: None,
            length: 0,
            layout_root: LayoutRootId::new("legacy-layout"),
            created_at_unix_ms: 1,
        };
        let layout = LayoutRoot {
            id: LayoutRootId::new("legacy-layout"),
            file_length: 0,
            inline_extents: Vec::new(),
        };
        let key = RequestKey::new("legacy-node", "legacy-commit");
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
            TxnMutation::Put(MetaEntity::DfsFileVersion(version.clone())),
            TxnMutation::Put(MetaEntity::DfsLayoutRoot(layout.clone())),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key.clone(),
                operation: StoreOperation::DfsCommitFileVersion,
                result: OperationResult::DfsInode(inode.clone()),
            }),
        ]);
        store.compare_and_commit(txn).await.unwrap();
        (service, store, inode, version, layout, key)
    }

    fn commit_for_stored_version(
        operation_id: OperationId,
        inode: &InodeRecord,
        version: FileVersion,
        layout: LayoutRoot,
    ) -> CommitFileVersion {
        CommitFileVersion {
            operation_id,
            inode_id: inode.inode_id.clone(),
            write_lease: WriteLease {
                inode_id: inode.inode_id.clone(),
                owner_node_id: "legacy-node".into(),
                owner_session_id: "legacy-session".into(),
                lease_epoch: 1,
                expires_at_unix_ms: 1,
            },
            expected_inode_revision: inode.revision,
            expected_head_version: None,
            file_version: version,
            layout_root: layout,
            chunk_receipts: Vec::new(),
            metadata_delta: crate::dfs::CommitMetadataDelta {
                mode: CommitMetadataMode::DataOnly,
                mtime_unix_ms: None,
                ctime_unix_ms: None,
                kill_suidgid: false,
            },
        }
    }

    async fn fixture() -> Fixture {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalChunkStore::open(temp.path(), "receiver").unwrap();
        let store = Arc::new(
            Store::open(Arc::new(MemoryBackend::default()))
                .await
                .unwrap(),
        );
        // Registration precedes writes: the live descriptor retains floor 0.
        register(&store, "reader", "reader-original", Vec::new()).await;
        register(
            &store,
            "receiver",
            "receiver-original",
            vec![local.device_descriptor().unwrap()],
        )
        .await;
        let staged = StagedChunk::new(OperationId::new("durable-r1"), b"data".to_vec());
        let receipt = local.put(staged.clone()).unwrap();
        let ack = &receipt.durable_acks[0];
        let copy = CopyRecord {
            id: CopyId::new("copy"),
            chunk_id: staged.chunk.id.clone(),
            role: CopyRole::DurableReplica,
            state: CopyState::Ready,
            location: CopyLocation::Node {
                node_id: ack.node_id.clone(),
                node_epoch: ack.node_epoch,
                device_id: ack.device_id.clone(),
                device_epoch: ack.device_epoch,
                catalog_revision: ack.catalog_revision,
            },
            persisted_bytes: ack.persisted_bytes,
            verified_digest: ack.verified_digest.clone(),
        };
        let service = DfsService::new(store.clone());
        let mut inode = root_inode(NamespaceId::new("default"));
        inode.inode_id = InodeId::new("file");
        inode.kind = InodeKind::Regular;
        inode.head_version = Some(FileVersionId::new("version"));
        let layout = LayoutRoot {
            id: LayoutRootId::new("layout"),
            file_length: 4,
            inline_extents: vec![Extent {
                file_offset: 0,
                length: 4,
                chunk_id: staged.chunk.id.clone(),
                chunk_offset: 0,
            }],
        };
        let key = RequestKey::new("fixture", "commit");
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        txn.mutations = vec![
            TxnMutation::Put(MetaEntity::DfsInode(inode)),
            TxnMutation::Put(MetaEntity::DfsFileVersion(FileVersion {
                id: FileVersionId::new("version"),
                inode_id: InodeId::new("file"),
                parent_version: None,
                length: 4,
                layout_root: layout.id.clone(),
                created_at_unix_ms: 1,
            })),
            TxnMutation::Put(MetaEntity::DfsLayoutRoot(layout)),
            TxnMutation::Put(MetaEntity::DfsChunk(staged.chunk.clone())),
            TxnMutation::Put(MetaEntity::DfsCopy(copy.clone())),
            TxnMutation::Put(MetaEntity::DfsPlacement(PlacementRecord {
                chunk_id: staged.chunk.id.clone(),
                replica_group_id: ReplicaGroupId::new("r1"),
                placement_epoch: 1,
                desired_copies: 1,
                copies: vec![copy.id.clone()],
                health: PlacementHealth::Satisfied,
            })),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: StoreOperation::DfsCommitFileVersion,
                result: OperationResult::Empty,
            }),
        ];
        store.compare_and_commit(txn).await.unwrap();
        Fixture {
            service,
            store,
            temp,
            local,
            request: DfsChunkSourcesRequest {
                caller_id: "reader".into(),
                namespace_id: NamespaceId::new("default"),
                file_version_id: FileVersionId::new("version"),
                layout_root_id: LayoutRootId::new("layout"),
                chunk_ids: vec![staged.chunk.id],
            },
            copy,
        }
    }

    #[tokio::test]
    async fn legacy_direct_commit_outcome_rejects_replay_but_keeps_state_readable() {
        let (service, _store, inode, version, layout, key) = legacy_direct_commit_fixture().await;

        assert_eq!(
            service.get_inode(inode.inode_id.clone()).await.unwrap(),
            inode
        );
        assert_eq!(
            service.get_file_version(version.id.clone()).await.unwrap(),
            (version.clone(), layout.clone())
        );

        let err = service
            .commit_file_version(
                key.caller_id.clone(),
                commit_for_stored_version(
                    OperationId::new(key.request_id.clone()),
                    &inode,
                    version,
                    layout,
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), afs_error::META_CATALOG_INVALID_REQUEST);
        assert!(err.message().contains("lacks its identity proof"));
    }

    #[tokio::test]
    async fn namespace_outcome_validates_condition_failed_existing_digest() {
        let (_service, store, inode, _version, _layout, _key) =
            legacy_direct_commit_fixture().await;
        let key = RequestKey::new("digest-node", "digest-commit");
        let digest = [7; 32];
        let outcome = RequestOutcome {
            request: key.clone(),
            operation: StoreOperation::DfsCommitFileVersion,
            result: namespace_result(digest, OperationResult::DfsInode(inode.clone())),
        };
        let mut seed = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
        seed.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        seed.mutations
            .push(TxnMutation::RecordRequestOutcome(outcome.clone()));
        store.compare_and_commit(seed).await.unwrap();

        let mut replay = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
        replay.conditions.push(TxnCondition::RequestAbsent(key));
        replay
            .mutations
            .push(TxnMutation::RecordRequestOutcome(outcome));
        let raced = store.compare_and_commit(replay).await.unwrap();
        assert!(matches!(
            raced,
            TxnOutcome::ConditionFailed {
                existing_outcome: Some(_),
                ..
            }
        ));
        let validated = validate_namespace_outcome(raced.clone(), digest).unwrap();
        assert_eq!(
            inode_outcome(validated, StoreOperation::DfsCommitFileVersion).unwrap(),
            inode
        );

        let mismatched = validate_namespace_outcome(raced, [8; 32]).unwrap_err();
        assert_eq!(mismatched.code(), afs_error::META_CATALOG_INVALID_REQUEST);
        assert!(
            mismatched
                .message()
                .contains("differs from the recorded request")
        );
    }

    #[tokio::test]
    async fn recovered_r1_copy_serves_new_epoch_without_mutating_durable_evidence() {
        let fixture = fixture().await;
        let old = fixture.sources().await.remove(0);
        fixture.validate(&old, 1).await.unwrap();
        let placement = fixture
            .store
            .read(MetaRead::DfsPlacement(old.chunk_id.clone()))
            .await
            .unwrap();
        let recovered = LocalChunkStore::open(fixture.temp.path(), "receiver").unwrap();
        let session = fixture
            .restart(recovered.device_descriptor().unwrap())
            .await;
        assert_eq!(session.lease_epoch, 2);
        let fresh = fixture.sources().await.remove(0);
        assert!(matches!(
            fresh.location,
            CopyLocation::Node { node_epoch: 2, .. }
        ));
        fixture.validate(&fresh, 2).await.unwrap();
        // The old token is unexpired and caller is unchanged: its receiver MAC
        // must nevertheless fail after serving authority changes.
        assert!(fixture.validate(&old, 2).await.is_err());
        assert!(fixture.validate(&fresh, 1).await.is_err());
        let mut bytes = [0; 4];
        assert_eq!(
            recovered.read_at(&fresh.chunk_id, 0, &mut bytes).unwrap(),
            4
        );
        assert_eq!(&bytes, b"data");
        assert!(matches!(
            fixture.store.read(MetaRead::DfsCopy(fixture.copy.id.clone())).await.unwrap().entity,
            Some(MetaEntity::DfsCopy(copy)) if copy == fixture.copy
        ));
        let after = fixture
            .store
            .read(MetaRead::DfsPlacement(old.chunk_id))
            .await
            .unwrap();
        assert_eq!(after.revision, placement.revision);
        assert!(
            matches!(after.entity, Some(MetaEntity::DfsPlacement(value)) if value.copies == vec![fixture.copy.id])
        );
    }

    #[tokio::test]
    async fn recovered_read_copy_rejects_changed_device_and_insufficient_catalog() {
        for case in 0..3 {
            let fixture = fixture().await;
            let old = fixture.sources().await.remove(0);
            let mut device = fixture.local.device_descriptor().unwrap();
            match case {
                0 => device.device_id = "replacement-device".into(),
                1 => device.device_epoch = device.device_epoch.wrapping_add(1),
                _ => device.catalog_revision = 0,
            }
            fixture.restart(device).await;
            assert!(fixture.sources().await.is_empty());
            assert!(fixture.validate(&old, 2).await.is_err());
        }
    }

    #[tokio::test]
    async fn live_read_copy_allows_new_writes_above_startup_catalog_floor() {
        let fixture = fixture().await;
        let device = fixture.local.device_descriptor().unwrap();
        assert!(device.catalog_revision > 0);
        let source = fixture.sources().await.remove(0);
        fixture.validate(&source, 1).await.unwrap();
        let session = match fixture
            .store
            .read(MetaRead::CurrentNodeSession {
                node_id: "receiver".into(),
            })
            .await
            .unwrap()
            .entity
        {
            Some(MetaEntity::NodeSession(session)) => session,
            _ => panic!("original session is missing"),
        };
        assert_eq!(session.storage_devices[0].catalog_revision, 0);
        assert!(serving_read_copy(&fixture.copy, &session, now_unix_ms()).is_some());
    }

    fn repair_replication() -> ReplicationConfig {
        ReplicationConfig {
            desired_copies: 2,
            sync_required_copies: 2,
            min_distinct_nodes: 1,
            min_distinct_failure_domains: 1,
            local_copy: LocalCopyPolicy::Required,
        }
    }

    async fn underreplicated_fixture() -> Fixture {
        let fixture = fixture().await;
        register(
            &fixture.store,
            "target",
            "target-original",
            vec![StorageDeviceDescriptor {
                device_id: "target-device".into(),
                device_epoch: 1,
                catalog_revision: 0,
                failure_domain: "target-fd".into(),
            }],
        )
        .await;
        let service =
            DfsService::with_replication_config(fixture.store.clone(), repair_replication());
        service.initialize_replication_config().await.unwrap();
        let key = RequestKey::new("fixture", "mark-underreplicated");
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsPlacement(PlacementRecord {
                chunk_id: fixture.copy.chunk_id.clone(),
                replica_group_id: ReplicaGroupId::new("repair-r2"),
                placement_epoch: 2,
                desired_copies: 2,
                copies: vec![fixture.copy.id.clone()],
                health: PlacementHealth::UnderReplicated,
            })),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: StoreOperation::DfsCommitFileVersion,
                result: OperationResult::Empty,
            }),
        ]);
        fixture.store.compare_and_commit(txn).await.unwrap();
        Fixture { service, ..fixture }
    }

    fn claim_request(operation_id: &str) -> ClaimReplicationTask {
        ClaimReplicationTask {
            caller_id: "receiver".into(),
            caller_session_id: "receiver-original".into(),
            caller_node_epoch: 1,
            operation_id: OperationId::new(operation_id),
            lease_seconds: 30,
        }
    }

    fn repair_acks(claim: &ReplicationClaim) -> Vec<ReplicaAck> {
        claim
            .replica_group
            .targets
            .iter()
            .take(usize::from(claim.replication.desired_copies))
            .map(|target| ReplicaAck {
                operation_id: claim.operation_id.clone(),
                chunk_id: claim.chunk.id.clone(),
                placement_revision: claim.placement_revision,
                placement_epoch: claim.replica_group.placement_epoch,
                node_id: target.node_id.clone(),
                node_epoch: target.node_epoch,
                device_id: target.device.device_id.clone(),
                device_epoch: target.device.device_epoch,
                catalog_revision: target.device.catalog_revision,
                persisted_bytes: claim.chunk.length,
                verified_digest: claim.chunk.content_digest.clone(),
            })
            .collect()
    }

    async fn overwrite_task(store: &Store, task: ReplicationTask, operation_id: &str) {
        let key = RequestKey::new("fixture", operation_id);
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsReplicationTask(task)),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: StoreOperation::DfsCommitFileVersion,
                result: OperationResult::Empty,
            }),
        ]);
        store.compare_and_commit(txn).await.unwrap();
    }

    async fn overwrite_copy(store: &Store, copy: CopyRecord, operation_id: &str) {
        let key = RequestKey::new("fixture", operation_id);
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
        txn.conditions
            .push(TxnCondition::RequestAbsent(key.clone()));
        txn.mutations.extend([
            TxnMutation::Put(MetaEntity::DfsCopy(copy)),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: StoreOperation::DfsCommitFileVersion,
                result: OperationResult::Empty,
            }),
        ]);
        store.compare_and_commit(txn).await.unwrap();
    }

    async fn stored_copy(fixture: &Fixture, copy_id: &CopyId) -> CopyRecord {
        match fixture
            .store
            .read(MetaRead::DfsCopy(copy_id.clone()))
            .await
            .unwrap()
            .entity
        {
            Some(MetaEntity::DfsCopy(copy)) => copy,
            _ => panic!("copy missing"),
        }
    }

    async fn stored_placement(fixture: &Fixture) -> PlacementRecord {
        match fixture
            .store
            .read(MetaRead::DfsPlacement(fixture.copy.chunk_id.clone()))
            .await
            .unwrap()
            .entity
        {
            Some(MetaEntity::DfsPlacement(placement)) => placement,
            _ => panic!("placement missing"),
        }
    }

    async fn stored_repair_task(fixture: &Fixture) -> ReplicationTask {
        match fixture
            .store
            .read(MetaRead::DfsReplicationTask(repair_task_id(
                &fixture.copy.chunk_id,
            )))
            .await
            .unwrap()
            .entity
        {
            Some(MetaEntity::DfsReplicationTask(task)) => task,
            _ => panic!("repair task missing"),
        }
    }

    fn node_catalog_revision(copy: &CopyRecord) -> u64 {
        match &copy.location {
            CopyLocation::Node {
                catalog_revision, ..
            } => *catalog_revision,
            CopyLocation::External { .. } => panic!("expected node copy"),
        }
    }

    fn corruption_report(
        operation_id: &str,
        copy: &CopyRecord,
        catalog_revision: u64,
    ) -> ReportChunkCorruption {
        let CopyLocation::Node {
            node_id,
            node_epoch,
            device_id,
            device_epoch,
            ..
        } = &copy.location
        else {
            panic!("expected node copy");
        };
        ReportChunkCorruption {
            caller_id: node_id.clone(),
            caller_session_id: format!("{node_id}-original"),
            caller_node_epoch: *node_epoch,
            operation_id: OperationId::new(operation_id),
            chunk_id: copy.chunk_id.clone(),
            device_id: device_id.clone(),
            device_epoch: *device_epoch,
            catalog_revision,
        }
    }

    async fn task_record(fixture: &Fixture, claim: &ReplicationClaim) -> ReplicationTask {
        match fixture
            .store
            .read(MetaRead::DfsReplicationTask(claim.task_id.clone()))
            .await
            .unwrap()
            .entity
        {
            Some(MetaEntity::DfsReplicationTask(task)) => task,
            _ => panic!("replication task missing"),
        }
    }

    #[tokio::test]
    async fn replication_claim_is_idempotent_and_digest_bound() {
        let fixture = underreplicated_fixture().await;
        let request = claim_request("claim-repair");
        let claim = fixture
            .service
            .claim_replication_task(request.clone())
            .await
            .unwrap()
            .expect("underreplicated chunk should be claimable by its source");
        assert_eq!(claim.task_id, repair_task_id(&fixture.copy.chunk_id));
        assert_eq!(claim.source_copy_id, fixture.copy.id);
        assert_eq!(claim.replication.desired_copies, 2);
        assert_eq!(claim.replication.sync_required_copies, 2);
        assert_eq!(claim.replication.local_copy, LocalCopyPolicy::Required);
        assert_eq!(
            fixture
                .service
                .claim_replication_task(request.clone())
                .await
                .unwrap(),
            Some(claim.clone())
        );
        let mut changed = request;
        changed.lease_seconds = 31;
        assert!(
            fixture
                .service
                .claim_replication_task(changed)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn restored_source_device_can_claim_repair_with_new_epoch() {
        let fixture = underreplicated_fixture().await;
        let session = fixture
            .restart(fixture.local.device_descriptor().unwrap())
            .await;
        let claim = fixture
            .service
            .claim_replication_task(ClaimReplicationTask {
                caller_id: "receiver".into(),
                caller_session_id: session.session_id.clone(),
                caller_node_epoch: session.lease_epoch,
                operation_id: OperationId::new("claim-after-restart"),
                lease_seconds: 30,
            })
            .await
            .unwrap()
            .expect("recovered source device should remain repair-readable");
        assert_eq!(claim.source_copy_id, fixture.copy.id);
        assert_eq!(claim.worker_node_epoch, session.lease_epoch);
    }

    #[tokio::test]
    async fn restarted_worker_reclaims_unexpired_repair_without_waiting_for_task_lease() {
        let fixture = underreplicated_fixture().await;
        let first = fixture
            .service
            .claim_replication_task(claim_request("claim-before-worker-restart"))
            .await
            .unwrap()
            .unwrap();
        assert!(first.expires_at_unix_ms > now_unix_ms());
        // A live matching worker keeps ownership even when another poll arrives.
        assert!(
            fixture
                .service
                .claim_replication_task(claim_request("claim-live-worker-again"))
                .await
                .unwrap()
                .is_none()
        );
        let session = fixture
            .restart(fixture.local.device_descriptor().unwrap())
            .await;
        let replacement = fixture
            .service
            .claim_replication_task(ClaimReplicationTask {
                caller_id: "receiver".into(),
                caller_session_id: session.session_id.clone(),
                caller_node_epoch: session.lease_epoch,
                operation_id: OperationId::new("claim-restarted-unexpired"),
                lease_seconds: 30,
            })
            .await
            .unwrap()
            .expect("a retired process cannot retain a repair lease on its recovered device");
        assert_ne!(replacement.operation_id, first.operation_id);
        assert_eq!(replacement.worker_node_epoch, session.lease_epoch);
        assert_eq!(replacement.source_copy_id, fixture.copy.id);
        assert!(first.expires_at_unix_ms > now_unix_ms());
        let stale = fixture
            .service
            .report_replication_task(ReportReplicationTask {
                caller_id: first.worker_node_id.clone(),
                caller_session_id: first.worker_session_id.clone(),
                caller_node_epoch: first.worker_node_epoch,
                operation_id: OperationId::new("report-retired-worker"),
                durable_acks: repair_acks(&first),
                error: None,
                source_invalid: false,
                claim: first,
            })
            .await;
        assert!(
            stale.is_err(),
            "the retired process must not promote its old report"
        );
    }

    #[tokio::test]
    async fn expired_worker_blocks_without_source_before_repair_lease_expires() {
        let fixture = underreplicated_fixture().await;
        let claim = fixture
            .service
            .claim_replication_task(claim_request("claim-before-worker-expiry"))
            .await
            .unwrap()
            .unwrap();
        // Expire the worker through the real registration/lease path. Keep the
        // CurrentNodeSession pointer present, as happens after process death.
        fixture
            .store
            .register_node_session(
                RequestKey::new("receiver", "shorten-worker-lease"),
                NodeSessionLease {
                    node_id: "receiver".into(),
                    session_id: "receiver-original".into(),
                    grpc_addr: "http://127.0.0.1:1".into(),
                    data_addr: "http://127.0.0.1:2".into(),
                    rest_addr: "http://127.0.0.1:3".into(),
                    storage_devices: vec![fixture.local.device_descriptor().unwrap()],
                    lease_ttl: std::time::Duration::from_millis(50),
                },
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(75)).await;
        assert!(claim.expires_at_unix_ms > now_unix_ms());
        let result = fixture
            .service
            .claim_replication_task(ClaimReplicationTask {
                caller_id: "target".into(),
                caller_session_id: "target-original".into(),
                caller_node_epoch: 1,
                operation_id: OperationId::new("observe-expired-worker"),
                lease_seconds: 30,
            })
            .await
            .unwrap();
        assert!(result.is_none());
        let task = task_record(&fixture, &claim).await;
        assert_eq!(task.state, ReplicationTaskState::BlockedNoSource);
        assert!(task.claim.is_none());
    }

    #[tokio::test]
    async fn lost_target_persists_repair_debt_before_replacement_is_available() {
        for missing_task in [false, true] {
            let fixture = underreplicated_fixture().await;
            let claim = fixture
                .service
                .claim_replication_task(claim_request("claim-full-copy-set"))
                .await
                .unwrap()
                .unwrap();
            fixture
                .service
                .report_replication_task(ReportReplicationTask {
                    caller_id: claim.worker_node_id.clone(),
                    caller_session_id: claim.worker_session_id.clone(),
                    caller_node_epoch: claim.worker_node_epoch,
                    operation_id: OperationId::new("report-full-copy-set"),
                    durable_acks: repair_acks(&claim),
                    error: None,
                    source_invalid: false,
                    claim: claim.clone(),
                })
                .await
                .unwrap();
            if missing_task {
                let key = RequestKey::new("fixture", "remove-completed-task");
                let mut txn = MetaTxn::new(key.clone(), StoreOperation::DfsCommitFileVersion);
                txn.conditions
                    .push(TxnCondition::RequestAbsent(key.clone()));
                txn.mutations.extend([
                    TxnMutation::Delete(MetaKey::DfsReplicationTask(claim.task_id.clone())),
                    TxnMutation::RecordRequestOutcome(RequestOutcome {
                        request: key,
                        operation: StoreOperation::DfsCommitFileVersion,
                        result: OperationResult::Empty,
                    }),
                ]);
                fixture.store.compare_and_commit(txn).await.unwrap();
            }
            fixture
                .store
                .register_node_session(
                    RequestKey::new("target", "expire-target"),
                    NodeSessionLease {
                        node_id: "target".into(),
                        session_id: "target-original".into(),
                        grpc_addr: "http://127.0.0.1:1".into(),
                        data_addr: "http://127.0.0.1:2".into(),
                        rest_addr: "http://127.0.0.1:3".into(),
                        storage_devices: vec![StorageDeviceDescriptor {
                            device_id: "target-device".into(),
                            device_epoch: 1,
                            catalog_revision: 0,
                            failure_domain: "target-fd".into(),
                        }],
                        lease_ttl: std::time::Duration::from_millis(50),
                    },
                )
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(75)).await;
            let request = claim_request("observe-lost-target");
            assert!(
                fixture
                    .service
                    .claim_replication_task(request.clone())
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                fixture
                    .service
                    .claim_replication_task(request)
                    .await
                    .unwrap()
                    .is_none()
            );
            let task = task_record(&fixture, &claim).await;
            assert_eq!(task.state, ReplicationTaskState::Pending);
            assert!(task.claim.is_none());
            assert!(matches!(
                fixture
                    .store
                    .read(MetaRead::DfsPlacement(fixture.copy.chunk_id.clone()))
                    .await
                    .unwrap()
                    .entity,
                Some(MetaEntity::DfsPlacement(PlacementRecord {
                    health: PlacementHealth::UnderReplicated,
                    ..
                }))
            ));
            register(
                &fixture.store,
                "target",
                "target-restored",
                vec![StorageDeviceDescriptor {
                    device_id: "target-device".into(),
                    device_epoch: 1,
                    catalog_revision: 0,
                    failure_domain: "target-fd".into(),
                }],
            )
            .await;
            assert!(
                fixture
                    .service
                    .claim_replication_task(claim_request("claim-after-target-restored"))
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[tokio::test]
    async fn repair_report_requires_full_desired_receipts() {
        let fixture = underreplicated_fixture().await;
        let claim = fixture
            .service
            .claim_replication_task(claim_request("claim-short-report"))
            .await
            .unwrap()
            .unwrap();
        let err = fixture
            .service
            .report_replication_task(ReportReplicationTask {
                caller_id: "receiver".into(),
                caller_session_id: "receiver-original".into(),
                caller_node_epoch: 1,
                operation_id: OperationId::new("report-short"),
                claim,
                durable_acks: Vec::new(),
                error: None,
                source_invalid: false,
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("fewer ACKs"));
    }

    #[tokio::test]
    async fn source_invalid_marks_only_proven_source_and_blocks_without_live_source() {
        let fixture = underreplicated_fixture().await;
        let claim = fixture
            .service
            .claim_replication_task(claim_request("claim-bad-source"))
            .await
            .unwrap()
            .unwrap();
        let task = fixture
            .service
            .report_replication_task(ReportReplicationTask {
                caller_id: "receiver".into(),
                caller_session_id: "receiver-original".into(),
                caller_node_epoch: 1,
                operation_id: OperationId::new("report-bad-source"),
                claim: claim.clone(),
                durable_acks: Vec::new(),
                error: None,
                source_invalid: true,
            })
            .await
            .unwrap();
        assert_eq!(task.state, ReplicationTaskState::BlockedNoSource);
        assert!(task.claim.is_none());
        assert!(matches!(
            fixture
                .store
                .read(MetaRead::DfsCopy(claim.source_copy_id.clone()))
                .await
                .unwrap()
                .entity,
            Some(MetaEntity::DfsCopy(CopyRecord {
                state: CopyState::Corrupt,
                ..
            }))
        ));
        assert!(matches!(
            fixture
                .store
                .read(MetaRead::DfsPlacement(fixture.copy.chunk_id.clone()))
                .await
                .unwrap()
                .entity,
            Some(MetaEntity::DfsPlacement(PlacementRecord {
                health: PlacementHealth::BlockedNoSource,
                ..
            }))
        ));
    }

    #[tokio::test]
    async fn corruption_report_excludes_bad_own_copy_and_persists_repair_debt() {
        let fixture = underreplicated_fixture().await;
        let claim = fixture
            .service
            .claim_replication_task(claim_request("claim-before-corruption-report"))
            .await
            .unwrap()
            .unwrap();
        fixture
            .service
            .report_replication_task(ReportReplicationTask {
                caller_id: claim.worker_node_id.clone(),
                caller_session_id: claim.worker_session_id.clone(),
                caller_node_epoch: claim.worker_node_epoch,
                operation_id: OperationId::new("complete-before-corruption-report"),
                durable_acks: repair_acks(&claim),
                error: None,
                source_invalid: false,
                claim,
            })
            .await
            .unwrap();
        let pre_report_placement = stored_placement(&fixture).await;
        let report = corruption_report(
            "report-own-corruption",
            &fixture.copy,
            node_catalog_revision(&fixture.copy).saturating_add(1),
        );
        fixture
            .service
            .report_chunk_corruption(report.clone())
            .await
            .unwrap();

        let copy = stored_copy(&fixture, &fixture.copy.id).await;
        assert_eq!(copy.state, CopyState::Corrupt);
        let placement = stored_placement(&fixture).await;
        assert_eq!(placement.health, PlacementHealth::UnderReplicated);
        assert_eq!(placement.copies, pre_report_placement.copies);
        let mut observed_copies = Vec::new();
        for copy_id in &placement.copies {
            observed_copies.push(stored_copy(&fixture, copy_id).await);
        }
        let mut excluded = std::collections::HashSet::new();
        excluded.insert(fixture.copy.id.clone());
        assert_eq!(
            live_ready_copy_count_from_observed_excluding(
                &observed_copies,
                &excluded,
                &fixture.service,
                now_unix_ms(),
            )
            .await
            .unwrap(),
            1
        );
        let task = stored_repair_task(&fixture).await;
        assert_eq!(task.state, ReplicationTaskState::Pending);
        assert!(task.claim.is_none());
        assert_eq!(task.existing_copies, placement.copies);
        assert_eq!(
            fixture
                .service
                .report_chunk_corruption(report)
                .await
                .unwrap(),
            ()
        );
    }

    #[tokio::test]
    async fn corruption_report_rejects_forged_identity_device_revision_and_session() {
        let fixture = underreplicated_fixture().await;
        let mut other_node = corruption_report(
            "report-forged-node",
            &fixture.copy,
            node_catalog_revision(&fixture.copy).saturating_add(1),
        );
        other_node.caller_id = "target".into();
        other_node.caller_session_id = "target-original".into();
        assert!(
            fixture
                .service
                .report_chunk_corruption(other_node)
                .await
                .is_err()
        );

        let mut other_device = corruption_report(
            "report-forged-device",
            &fixture.copy,
            node_catalog_revision(&fixture.copy).saturating_add(1),
        );
        other_device.device_id = "other-device".into();
        assert!(
            fixture
                .service
                .report_chunk_corruption(other_device)
                .await
                .is_err()
        );

        let mut zero_revision = corruption_report("report-zero-revision", &fixture.copy, 0);
        assert!(
            fixture
                .service
                .report_chunk_corruption(zero_revision.clone())
                .await
                .is_err()
        );

        zero_revision.operation_id = OperationId::new("report-forged-session");
        zero_revision.catalog_revision = node_catalog_revision(&fixture.copy).saturating_add(1);
        zero_revision.caller_session_id = "stale-session".into();
        assert!(
            fixture
                .service
                .report_chunk_corruption(zero_revision)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn corruption_report_exact_replay_does_not_remark_newer_repaired_copy() {
        let fixture = fixture().await;
        fixture
            .service
            .initialize_replication_config()
            .await
            .unwrap();
        let report_revision = node_catalog_revision(&fixture.copy).saturating_add(1);
        let report = corruption_report("report-replay-corruption", &fixture.copy, report_revision);
        fixture
            .service
            .report_chunk_corruption(report.clone())
            .await
            .unwrap();

        let mut repaired = fixture.copy.clone();
        repaired.state = CopyState::Ready;
        if let CopyLocation::Node {
            catalog_revision, ..
        } = &mut repaired.location
        {
            *catalog_revision = report_revision.saturating_add(1);
        }
        overwrite_copy(
            &fixture.store,
            repaired.clone(),
            "overwrite-newer-repaired-copy",
        )
        .await;
        fixture
            .service
            .report_chunk_corruption(report)
            .await
            .unwrap();
        assert_eq!(stored_copy(&fixture, &fixture.copy.id).await, repaired);
    }

    #[tokio::test]
    async fn corruption_report_fresh_stale_quarantine_ack_preserves_newer_repair() {
        let fixture = fixture().await;
        fixture
            .service
            .initialize_replication_config()
            .await
            .unwrap();
        let report_revision = node_catalog_revision(&fixture.copy).saturating_add(1);
        let report = corruption_report("report-before-fresh-stale", &fixture.copy, report_revision);
        fixture
            .service
            .report_chunk_corruption(report)
            .await
            .unwrap();

        let mut repaired = fixture.copy.clone();
        repaired.state = CopyState::Ready;
        if let CopyLocation::Node {
            catalog_revision, ..
        } = &mut repaired.location
        {
            *catalog_revision = report_revision.saturating_add(1);
        }
        overwrite_copy(
            &fixture.store,
            repaired.clone(),
            "overwrite-newer-repaired-copy-before-fresh-stale",
        )
        .await;

        let mut stale = corruption_report(
            "report-fresh-stale-quarantine",
            &fixture.copy,
            report_revision,
        );
        fixture
            .service
            .report_chunk_corruption(stale.clone())
            .await
            .unwrap();
        assert_eq!(stored_copy(&fixture, &fixture.copy.id).await, repaired);

        fixture
            .service
            .report_chunk_corruption(stale.clone())
            .await
            .unwrap();
        stale.catalog_revision = node_catalog_revision(&repaired).saturating_add(1);
        assert!(
            fixture
                .service
                .report_chunk_corruption(stale)
                .await
                .is_err()
        );
        assert_eq!(stored_copy(&fixture, &fixture.copy.id).await, repaired);
    }

    #[tokio::test]
    async fn corruption_report_rejects_modified_request_reusing_same_operation() {
        let fixture = fixture().await;
        fixture
            .service
            .initialize_replication_config()
            .await
            .unwrap();
        let mut report = corruption_report(
            "report-digest-bound",
            &fixture.copy,
            node_catalog_revision(&fixture.copy).saturating_add(1),
        );
        fixture
            .service
            .report_chunk_corruption(report.clone())
            .await
            .unwrap();
        report.catalog_revision = report.catalog_revision.saturating_add(1);
        assert!(
            fixture
                .service
                .report_chunk_corruption(report)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn corruption_report_blocks_when_all_sources_are_bad() {
        let fixture = fixture().await;
        fixture
            .service
            .initialize_replication_config()
            .await
            .unwrap();
        let report = corruption_report(
            "report-only-source-corruption",
            &fixture.copy,
            node_catalog_revision(&fixture.copy).saturating_add(1),
        );
        fixture
            .service
            .report_chunk_corruption(report)
            .await
            .unwrap();
        assert_eq!(
            stored_copy(&fixture, &fixture.copy.id).await.state,
            CopyState::Corrupt
        );
        assert_eq!(
            stored_placement(&fixture).await.health,
            PlacementHealth::BlockedNoSource
        );
        assert_eq!(
            stored_repair_task(&fixture).await.state,
            ReplicationTaskState::BlockedNoSource
        );
    }

    #[tokio::test]
    async fn late_exact_report_with_full_receipts_completes_and_replays() {
        let fixture = underreplicated_fixture().await;
        let mut claim = fixture
            .service
            .claim_replication_task(claim_request("claim-late-report"))
            .await
            .unwrap()
            .unwrap();
        let mut task = task_record(&fixture, &claim).await;
        claim.expires_at_unix_ms = 1;
        task.claim = Some(Box::new(claim.clone()));
        task.next_retry_unix_ms = 1;
        overwrite_task(&fixture.store, task, "expire-running-claim").await;

        let report = ReportReplicationTask {
            caller_id: "receiver".into(),
            caller_session_id: "receiver-original".into(),
            caller_node_epoch: 1,
            operation_id: OperationId::new("report-late-success"),
            durable_acks: repair_acks(&claim),
            error: None,
            source_invalid: false,
            claim: claim.clone(),
        };
        let task = fixture
            .service
            .report_replication_task(report.clone())
            .await
            .unwrap();
        assert_eq!(task.state, ReplicationTaskState::Completed);
        assert_eq!(task.placement_epoch, claim.replica_group.placement_epoch);
        assert_eq!(task.desired_copies, claim.replication.desired_copies);
        assert_eq!(
            fixture
                .service
                .report_replication_task(report)
                .await
                .unwrap(),
            task
        );
    }

    #[tokio::test]
    async fn stale_reassigned_report_returns_superseded_code() {
        let fixture = underreplicated_fixture().await;
        let mut old_claim = fixture
            .service
            .claim_replication_task(claim_request("claim-old"))
            .await
            .unwrap()
            .unwrap();
        let mut task = task_record(&fixture, &old_claim).await;
        old_claim.expires_at_unix_ms = 1;
        task.claim = Some(Box::new(old_claim.clone()));
        task.next_retry_unix_ms = 1;
        overwrite_task(&fixture.store, task, "expire-old-claim").await;
        let new_claim = fixture
            .service
            .claim_replication_task(claim_request("claim-new"))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(new_claim.operation_id, old_claim.operation_id);

        let err = fixture
            .service
            .report_replication_task(ReportReplicationTask {
                caller_id: "receiver".into(),
                caller_session_id: "receiver-original".into(),
                caller_node_epoch: 1,
                operation_id: OperationId::new("report-stale"),
                durable_acks: repair_acks(&old_claim),
                error: None,
                source_invalid: false,
                claim: old_claim,
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), afs_error::META_DFS_REPAIR_SUPERSEDED);
    }

    #[tokio::test]
    async fn mismatched_source_copy_is_not_claimable() {
        let fixture = underreplicated_fixture().await;
        let mut bad = fixture.copy.clone();
        bad.persisted_bytes = bad.persisted_bytes.saturating_add(1);
        overwrite_copy(&fixture.store, bad, "mismatch-source-copy").await;
        let claim = fixture
            .service
            .claim_replication_task(claim_request("claim-mismatch-source"))
            .await
            .unwrap();
        assert!(claim.is_none());
    }

    #[tokio::test]
    async fn recovered_read_copy_rejects_regressed_missing_expired_or_wrong_authority() {
        let fixture = fixture().await;
        let session = fixture
            .restart(fixture.local.device_descriptor().unwrap())
            .await;
        let now = now_unix_ms();
        let mut future_copy = fixture.copy.clone();
        if let CopyLocation::Node { node_epoch, .. } = &mut future_copy.location {
            *node_epoch = session.lease_epoch + 1;
        }
        assert!(serving_read_copy(&future_copy, &session, now).is_none());
        let mut bad = session.clone();
        bad.storage_devices.clear();
        assert!(serving_read_copy(&fixture.copy, &bad, now).is_none());
        bad = session.clone();
        bad.node_id = "other-node".into();
        assert!(serving_read_copy(&fixture.copy, &bad, now).is_none());
        bad = session.clone();
        bad.expires_at_unix_ms = now;
        assert!(serving_read_copy(&fixture.copy, &bad, now).is_none());
        let mut corrupt = fixture.copy.clone();
        corrupt.state = CopyState::Corrupt;
        assert!(serving_read_copy(&corrupt, &session, now).is_none());
        let mut zero_epoch = fixture.copy.clone();
        if let CopyLocation::Node { node_epoch, .. } = &mut zero_epoch.location {
            *node_epoch = 0;
        }
        assert!(serving_read_copy(&zero_epoch, &session, now).is_none());
    }
}
