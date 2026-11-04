//! Replication tasks: claim, result report and corruption report, with repair-write checks.

use super::*;

impl DfsService {
    pub async fn claim_replication_task(
        &self,
        request: ClaimReplicationTask,
    ) -> Result<Option<ReplicationClaim>> {
        if self.is_external_backend() {
            self.current_durability_policy().await?;
            return Ok(None);
        }
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
        if self.is_external_backend() {
            return Err(invalid(
                "external durability does not accept Node repair reports",
            ));
        }
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

    pub(super) async fn validate_repair_replica_write(
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
        // The repair source must head its chain. This is a chain constraint, not
        // the user's local-copy policy, so it never follows an effective value.
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
        let Ok(groups) = build_replica_groups(
            &request.caller_id,
            &repair_replication,
            EffectiveLocalCopy::declared(&repair_replication),
            &storage_sessions,
        ) else {
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

pub(super) fn repair_task_id(chunk_id: &crate::dfs::ChunkId) -> ReplicationTaskId {
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

pub(in crate::meta) fn external_copy_matches(
    policy: &DfsDurabilityPolicy,
    copy: &CopyRecord,
    chunk: &ChunkObject,
) -> Result<bool> {
    let DfsDurabilityPolicy::ExternalBackend {
        store_id,
        generation,
    } = policy
    else {
        return Ok(false);
    };
    if copy.role != CopyRole::ExternalCommitted
        || !copy.state.is_readable()
        || !copy_matches_chunk(copy, chunk)
    {
        return Ok(false);
    }
    let expected_key = crate::dfs::external_object_key(store_id, generation, &chunk.id)?;
    Ok(matches!(&copy.location,
        CopyLocation::External { store_id: actual_store, object_key, object_revision }
            if actual_store == store_id && object_key == &expected_key && object_revision == generation))
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

pub(super) async fn live_ready_copy_count_from_observed_excluding(
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
