//! OwnerFs root authority state machine.
//!
//! This module owns the durable OwnerRoots business transitions. RPC adapters
//! convert protobuf/auth context into these typed inputs and map returned domain
//! records back to wire replies.

use std::sync::Arc;

use afs_error::{Error, Result};

use super::store::{
    MetaEntity, MetaFuture, MetaKey, MetaRead, MetaStore, MetaTxn, NodeSession, OperationResult,
    RecoveryCursor, RequestKey, RootAccessGrant, RootCommandAck, RootCommandRecord,
    RootCommandType, RootRecord, RootReservationRecord, RootRight, StoreOperation, StoreRevision,
    TxnCondition, TxnMutation, TxnOutcome, WatchBatch, WatchChange, unavailable_meta_store,
};

pub trait OwnerRootAuthority: Send + Sync {
    fn reserve_root(&self, input: ReserveRootInput) -> MetaFuture<'_, ReserveRootOutput>;
    fn activate_root(&self, input: ActivateRootInput) -> MetaFuture<'_, RootAccessGrant>;
    fn abort_root(&self, input: AbortRootInput) -> MetaFuture<'_, ()>;
    fn deactivate_root(&self, input: DeactivateRootInput) -> MetaFuture<'_, ()>;
    fn lookup_root(&self, input: LookupRootInput) -> MetaFuture<'_, RootLookupState>;
    fn list_owner_roots(&self, input: ListOwnerRootsInput) -> MetaFuture<'_, ListOwnerRootsOutput>;
    fn acquire_root(&self, input: AcquireRootInput) -> MetaFuture<'_, RootAccessGrant>;
    fn validate_root_access(
        &self,
        input: ValidateRootAccessInput,
    ) -> MetaFuture<'_, ValidateRootAccessOutput>;
    fn watch_root_commands(
        &self,
        input: WatchRootCommandsInput,
    ) -> MetaFuture<'_, Vec<WatchedRootCommand>>;
    fn poll_root_command_batch(
        &self,
        input: WatchRootCommandsInput,
    ) -> MetaFuture<'_, RootCommandBatch>;
    fn ack_revocation(&self, input: AckRevocationInput) -> MetaFuture<'_, u64>;
    fn recover_root(&self, input: RecoverRootInput) -> MetaFuture<'_, RootAccessGrant>;
}

pub struct MissingOwnerRootAuthority;

impl OwnerRootAuthority for MissingOwnerRootAuthority {
    fn reserve_root(&self, _input: ReserveRootInput) -> MetaFuture<'_, ReserveRootOutput> {
        Box::pin(async { Err(unavailable_meta_store()) })
    }

    fn activate_root(&self, _input: ActivateRootInput) -> MetaFuture<'_, RootAccessGrant> {
        Box::pin(async { Err(unavailable_meta_store()) })
    }

    fn abort_root(&self, _input: AbortRootInput) -> MetaFuture<'_, ()> {
        Box::pin(async { Err(unavailable_meta_store()) })
    }

    fn deactivate_root(&self, _input: DeactivateRootInput) -> MetaFuture<'_, ()> {
        Box::pin(async { Err(unavailable_meta_store()) })
    }

    fn lookup_root(&self, _input: LookupRootInput) -> MetaFuture<'_, RootLookupState> {
        Box::pin(async { Err(unavailable_meta_store()) })
    }

    fn list_owner_roots(
        &self,
        _input: ListOwnerRootsInput,
    ) -> MetaFuture<'_, ListOwnerRootsOutput> {
        Box::pin(async { Err(unavailable_meta_store()) })
    }

    fn acquire_root(&self, _input: AcquireRootInput) -> MetaFuture<'_, RootAccessGrant> {
        Box::pin(async { Err(unavailable_meta_store()) })
    }

    fn validate_root_access(
        &self,
        _input: ValidateRootAccessInput,
    ) -> MetaFuture<'_, ValidateRootAccessOutput> {
        Box::pin(async { Err(unavailable_meta_store()) })
    }

    fn watch_root_commands(
        &self,
        _input: WatchRootCommandsInput,
    ) -> MetaFuture<'_, Vec<WatchedRootCommand>> {
        Box::pin(async { Err(unavailable_meta_store()) })
    }

    fn poll_root_command_batch(
        &self,
        _input: WatchRootCommandsInput,
    ) -> MetaFuture<'_, RootCommandBatch> {
        Box::pin(async {
            Ok(RootCommandBatch::Unsupported {
                message: "owner root command batch polling is unavailable".into(),
            })
        })
    }

    fn ack_revocation(&self, _input: AckRevocationInput) -> MetaFuture<'_, u64> {
        Box::pin(async { Err(unavailable_meta_store()) })
    }

    fn recover_root(&self, _input: RecoverRootInput) -> MetaFuture<'_, RootAccessGrant> {
        Box::pin(async { Err(unavailable_meta_store()) })
    }
}

pub struct StoreOwnerRootAuthority {
    store: Arc<dyn MetaStore>,
    load_balancing: bool,
}

impl StoreOwnerRootAuthority {
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self {
            store,
            load_balancing: false,
        }
    }

    pub fn with_load_balancing(store: Arc<dyn MetaStore>, load_balancing: bool) -> Self {
        Self {
            store,
            load_balancing,
        }
    }

    // Commands and the resume cursor come from the same pinned authority view.
    // A revocation that fell out of Watch history must still be delivered.
    async fn resynchronize_commands(
        &self,
        input: &WatchRootCommandsInput,
    ) -> Result<(StoreRevision, Vec<WatchedRootCommand>)> {
        let view = self.store.read_view().await?;
        let session = view
            .read(MetaRead::CurrentNodeSession {
                node_id: input.node_id.clone(),
            })
            .await?;
        if !matches!(session.entity,Some(MetaEntity::NodeSession(ref session)) if session.session_id==input.session_id && session.is_live_at_unix_ms(now_unix_ms()))
        {
            return Err(invalid("node session is not current"));
        }
        let (revision, entities) = view.snapshot()?;
        let commands=entities.iter().filter_map(|entity| {
            let MetaEntity::RootCommand(command)=entity else {return None};
            if command.home_node_id!=input.node_id || command.home_session_id!=input.session_id {return None;}
            let acknowledged=entities.iter().any(|entity| matches!(entity,MetaEntity::RootCommandAck(ack) if ack.command_id==command.command_id && ack.node_id==command.home_node_id && ack.session_id==command.home_session_id && ack.root_id==command.root_id && ack.root_epoch==command.root_epoch && ack.access_generation==command.old_access_generation && ack.success));
            (!acknowledged).then(||WatchedRootCommand{command:command.clone(),revision})
        }).collect();
        Ok((revision, commands))
    }
}

impl OwnerRootAuthority for StoreOwnerRootAuthority {
    fn reserve_root(&self, input: ReserveRootInput) -> MetaFuture<'_, ReserveRootOutput> {
        Box::pin(async move {
            if input.root_id.is_empty()
                || input.preferred_home_node_id.is_empty()
                || input.session_id.is_empty()
                || input.create_intent_id.is_empty()
            {
                return Err(invalid("root and requester identity are required"));
            }
            let requester_node_id = input.preferred_home_node_id.clone();
            let requester_session_id = input.session_id.clone();
            let key = request_key(
                format!("{requester_node_id}/{requester_session_id}"),
                input.request_id.clone(),
            );
            if let Some(outcome) = self
                .store
                .read(MetaRead::RequestOutcome(key.clone()))
                .await?
                .request_outcome
            {
                require_current_live_session(
                    self.store.as_ref(),
                    &requester_node_id,
                    &requester_session_id,
                )
                .await?;
                if outcome.operation != StoreOperation::ReserveRoot {
                    return Err(invalid(
                        "request_id was already used for a different Meta operation",
                    ));
                }
                let reservation = take_reserve_result(outcome.result, &input)?;
                return Ok(ReserveRootOutput {
                    reservation,
                    already_reserved_by_same_intent: true,
                });
            }
            // Allocation shares the existing writer. A different Root reserving
            // concurrently may advance its floor; retry only that CAS conflict.
            for _ in 0..32 {
                let root = self
                    .store
                    .read(MetaRead::Root {
                        root_id: input.root_id.clone(),
                    })
                    .await?;
                if root.entity.is_some() {
                    return Err(root_exists("root already exists"));
                }
                let snapshot = self
                    .store
                    .read(MetaRead::RootReservation {
                        root_id: input.root_id.clone(),
                    })
                    .await?;
                let existing = match snapshot.entity {
                    Some(MetaEntity::RootReservation(reservation)) => Some(reservation),
                    Some(_) => {
                        return Err(invalid("root reservation lookup returned wrong entity"));
                    }
                    None => None,
                };
                if let Some(existing) = &existing {
                    let same_intent = existing.create_intent_id == input.create_intent_id
                        && existing.requester_node_id == requester_node_id
                        && existing.requester_session_id == requester_session_id;
                    if same_intent {
                        let mut txn = MetaTxn::new(key.clone(), StoreOperation::ReserveRoot);
                        txn.conditions.extend([
                            TxnCondition::RequestAbsent(key.clone()),
                            TxnCondition::NodeSessionCurrent {
                                node_id: requester_node_id.clone(),
                                session_id: requester_session_id.clone(),
                            },
                            TxnCondition::Missing(MetaKey::Root {
                                root_id: input.root_id.clone(),
                            }),
                            TxnCondition::EntityEquals(MetaEntity::RootReservation(
                                existing.clone(),
                            )),
                        ]);
                        txn.mutations.push(TxnMutation::RecordRequestOutcome(
                            super::store::RequestOutcome {
                                request: txn.request.clone(),
                                operation: txn.operation,
                                result: OperationResult::RootReservation(existing.clone()),
                            },
                        ));
                        let outcome = self.store.compare_and_commit(txn).await?;
                        let reservation = take_reserve_result(
                            classify_reserve_outcome(
                                self.store.as_ref(),
                                outcome,
                                &input.root_id,
                                Some(existing),
                            )
                            .await?,
                            &input,
                        )?;
                        return Ok(ReserveRootOutput {
                            reservation,
                            already_reserved_by_same_intent: true,
                        });
                    }

                    if reservation_requester_is_live(self.store.as_ref(), existing).await? {
                        return Err(root_busy("root is reserved by another live request"));
                    }
                }
                let home = select_home(
                    self.store.as_ref(),
                    self.load_balancing,
                    &input.root_id,
                    &requester_node_id,
                    &requester_session_id,
                )
                .await?;
                let floor = self.store.read(MetaRead::RootEpochWatermark).await?;
                let Some(MetaEntity::RootEpochWatermark(previous)) = floor.entity else {
                    return Err(invalid("root epoch allocation floor is unavailable"));
                };
                let root_epoch = previous
                    .checked_add(1)
                    .ok_or_else(|| invalid("root epoch exhausted"))?;
                if input.expected_root_epoch > root_epoch {
                    return Err(invalid("expected root epoch exceeds next allocated epoch"));
                }
                let reservation = reservation_record(&input, home, root_epoch);
                let mut txn = MetaTxn::new(key.clone(), StoreOperation::ReserveRoot);
                txn.conditions.extend([
                    TxnCondition::RequestAbsent(key.clone()),
                    TxnCondition::RootEpochWatermarkEquals(previous),
                    TxnCondition::NodeSessionCurrent {
                        node_id: requester_node_id.clone(),
                        session_id: requester_session_id.clone(),
                    },
                    TxnCondition::Missing(MetaKey::Root {
                        root_id: input.root_id.clone(),
                    }),
                    TxnCondition::NodeSessionCurrent {
                        node_id: reservation.home_node_id.clone(),
                        session_id: reservation.home_session_id.clone(),
                    },
                ]);
                match &existing {
                    Some(existing) => txn.conditions.extend([
                        TxnCondition::EntityEquals(MetaEntity::RootReservation(existing.clone())),
                        TxnCondition::NodeSessionExpired {
                            node_id: existing.requester_node_id.clone(),
                            session_id: existing.requester_session_id.clone(),
                        },
                    ]),
                    None => txn
                        .conditions
                        .push(TxnCondition::Missing(MetaKey::RootReservation {
                            root_id: input.root_id.clone(),
                        })),
                }
                txn.mutations.extend([
                    TxnMutation::Put(MetaEntity::RootEpochWatermark(root_epoch)),
                    TxnMutation::Put(MetaEntity::RootReservation(reservation.clone())),
                    TxnMutation::RecordRequestOutcome(super::store::RequestOutcome {
                        request: txn.request.clone(),
                        operation: txn.operation,
                        result: OperationResult::RootReservation(reservation),
                    }),
                ]);
                let outcome = self.store.compare_and_commit(txn).await?;
                if matches!(
                    &outcome,
                    TxnOutcome::ConditionFailed {
                        existing_outcome: None,
                        ..
                    }
                ) {
                    let refreshed = self.store.read(MetaRead::RootEpochWatermark).await?;
                    if refreshed.entity != Some(MetaEntity::RootEpochWatermark(previous)) {
                        continue;
                    }
                }
                let reservation = take_reserve_result(
                    classify_reserve_outcome(
                        self.store.as_ref(),
                        outcome,
                        &input.root_id,
                        existing.as_ref(),
                    )
                    .await?,
                    &input,
                )?;
                return Ok(ReserveRootOutput {
                    reservation,
                    already_reserved_by_same_intent: false,
                });
            }
            Err(home_unavailable(
                "root epoch allocation contention; retry the same request",
            ))
        })
    }

    fn activate_root(&self, input: ActivateRootInput) -> MetaFuture<'_, RootAccessGrant> {
        Box::pin(async move {
            if input.local_prepare_id.is_empty() {
                return Err(invalid("local_prepare_id is required"));
            }
            let key = request_key(
                format!(
                    "{}/{}",
                    input.reservation.home_node_id, input.reservation.session_id
                ),
                input.request_id,
            );
            if let Some(outcome) = self
                .store
                .read(MetaRead::RequestOutcome(key.clone()))
                .await?
                .request_outcome
            {
                require_current_live_session(
                    self.store.as_ref(),
                    &input.reservation.home_node_id,
                    &input.reservation.session_id,
                )
                .await?;
                if outcome.operation != StoreOperation::ActivateRoot {
                    return Err(invalid(
                        "request_id was already used for a different Meta operation",
                    ));
                }
                let OperationResult::RootGrant(grant) = outcome.result else {
                    return Err(invalid("activate replay returned wrong result type"));
                };
                if grant.root_id != input.reservation.root_id
                    || grant.root_epoch != input.reservation.root_epoch
                    || grant.home_node_id != input.reservation.home_node_id
                    || grant.home_session_id != input.reservation.session_id
                {
                    return Err(invalid("activate replay parameters do not match"));
                }
                let root = self
                    .store
                    .read(MetaRead::Root {
                        root_id: input.reservation.root_id.clone(),
                    })
                    .await?;
                if !matches!(
                    root.entity,
                    Some(MetaEntity::Root(root))
                        if root.local_prepare_id == input.local_prepare_id
                ) {
                    return Err(invalid("activate replay parameters do not match"));
                }
                return Ok(grant);
            }
            let snapshot = self
                .store
                .read(MetaRead::RootReservation {
                    root_id: input.reservation.root_id.clone(),
                })
                .await?;
            let Some(MetaEntity::RootReservation(record)) = snapshot.entity else {
                return Err(invalid("root reservation was not found"));
            };
            if record.root_epoch != input.reservation.root_epoch
                || record.home_node_id != input.reservation.home_node_id
                || record.home_session_id != input.reservation.session_id
                || record.create_intent_id != input.reservation.create_intent_id
                || record.prepare_token != input.reservation.prepare_token
                || record.requester_node_id != input.reservation.requester_node_id
                || record.requester_session_id != input.reservation.requester_session_id
            {
                return Err(invalid("reservation does not match durable reserve record"));
            }
            let root = RootRecord {
                root_id: record.root_id.clone(),
                root_epoch: record.root_epoch,
                home_node_id: record.home_node_id.clone(),
                home_session_id: record.home_session_id.clone(),
                local_prepare_id: input.local_prepare_id.clone(),
                created_at_revision: StoreRevision::ZERO,
                updated_at_revision: StoreRevision::ZERO,
            };
            let grant = home_grant(&root, 1);
            let mut txn = MetaTxn::new(key.clone(), StoreOperation::ActivateRoot);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(key),
                TxnCondition::NodeSessionCurrent {
                    node_id: root.home_node_id.clone(),
                    session_id: root.home_session_id.clone(),
                },
                TxnCondition::Missing(MetaKey::Root {
                    root_id: root.root_id.clone(),
                }),
                TxnCondition::EntityEquals(MetaEntity::RootReservation(record.clone())),
            ]);
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::Root(root)),
                TxnMutation::Put(MetaEntity::RootGrant(grant.clone())),
                TxnMutation::Delete(MetaKey::RootReservation {
                    root_id: grant.root_id.clone(),
                }),
                TxnMutation::RecordRequestOutcome(super::store::RequestOutcome {
                    request: txn.request.clone(),
                    operation: txn.operation,
                    result: OperationResult::RootGrant(grant.clone()),
                }),
            ]);
            let OperationResult::RootGrant(grant) =
                commit_or_replay(self.store.as_ref(), txn).await?
            else {
                return Err(invalid("activate replay returned wrong result type"));
            };
            Ok(grant)
        })
    }

    fn abort_root(&self, input: AbortRootInput) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            let snapshot = self
                .store
                .read(MetaRead::RootReservation {
                    root_id: input.root_id.clone(),
                })
                .await?;
            let Some(MetaEntity::RootReservation(reservation)) = snapshot.entity else {
                return Err(invalid("root reservation was not found"));
            };
            if reservation.home_session_id != input.session_id {
                return Err(invalid("abort session does not match durable reservation"));
            }
            if reservation.root_epoch != input.root_epoch
                || reservation.create_intent_id != input.create_intent_id
                || reservation.prepare_token != input.prepare_token
            {
                return Err(invalid("abort request does not match durable reservation"));
            }
            if let Some(authenticated) = &input.authenticated_home_node_id
                && authenticated != &reservation.home_node_id
            {
                return Err(permission_denied(format!(
                    "authenticated node {authenticated} does not match reservation home {}",
                    reservation.home_node_id
                )));
            }
            let key = request_key(
                format!("{}/{}", reservation.home_node_id, input.session_id),
                input.request_id,
            );
            let mut txn = MetaTxn::new(key.clone(), StoreOperation::AbortRoot);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(key),
                TxnCondition::EntityEquals(MetaEntity::RootReservation(reservation)),
            ]);
            txn.mutations.extend([
                TxnMutation::Delete(MetaKey::RootReservation {
                    root_id: input.root_id,
                }),
                TxnMutation::RecordRequestOutcome(super::store::RequestOutcome {
                    request: txn.request.clone(),
                    operation: txn.operation,
                    result: OperationResult::Empty,
                }),
            ]);
            let _ = commit_or_replay(self.store.as_ref(), txn).await?;
            Ok(())
        })
    }

    fn deactivate_root(&self, input: DeactivateRootInput) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            let snapshot = self
                .store
                .read(MetaRead::Root {
                    root_id: input.root_id.clone(),
                })
                .await?;
            let Some(MetaEntity::Root(root)) = snapshot.entity else {
                // 幂等：root 已不在 Meta active 列表。
                return Ok(());
            };
            if root.root_epoch != input.root_epoch {
                return Err(invalid("deactivate root epoch does not match durable root"));
            }
            if root.home_node_id != input.home_node_id
                || root.home_session_id != input.home_session_id
            {
                return Err(invalid("deactivate caller does not match durable root"));
            }
            if let Some(authenticated) = &input.authenticated_home_node_id
                && authenticated != &root.home_node_id
            {
                return Err(permission_denied(format!(
                    "authenticated node {authenticated} does not match root home {}",
                    root.home_node_id
                )));
            }
            let home = self
                .store
                .read(MetaRead::RootGrantByHolder {
                    root_id: root.root_id.clone(),
                    holder_node_id: root.home_node_id.clone(),
                    holder_session_id: root.home_session_id.clone(),
                })
                .await?;
            let Some(MetaEntity::RootGrant(home_grant)) = home.entity else {
                return Err(invalid("current Home grant is missing during deactivation"));
            };
            if home_grant.root_epoch != root.root_epoch
                || home_grant.home_node_id != root.home_node_id
                || home_grant.home_session_id != root.home_session_id
                || home_grant.access_generation == 0
            {
                return Err(invalid(
                    "current Home grant does not match deactivated Root",
                ));
            }
            let access_generation = home_grant.access_generation;
            let key = request_key(
                format!("{}/{}", root.home_node_id, root.home_session_id),
                input.request_id.clone(),
            );
            let mut txn = MetaTxn::new(key.clone(), StoreOperation::DeactivateRoot);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(key),
                TxnCondition::NodeSessionCurrent {
                    node_id: root.home_node_id.clone(),
                    session_id: root.home_session_id.clone(),
                },
                TxnCondition::EntityEquals(MetaEntity::Root(root.clone())),
                TxnCondition::EntityEquals(MetaEntity::RootGrant(home_grant)),
            ]);
            txn.mutations.push(TxnMutation::Delete(MetaKey::Root {
                root_id: root.root_id.clone(),
            }));
            // Root absence fences every old grant in the authoritative view.
            // Physical grant removal is bounded background work, not part of this ACK.
            txn.mutations.push(TxnMutation::Put(MetaEntity::RootCommand(
                RootCommandRecord {
                    command_id: format!(
                        "deactivate:{}:{}:{}",
                        root.root_id, root.root_epoch, input.request_id
                    ),
                    home_node_id: root.home_node_id.clone(),
                    home_session_id: root.home_session_id.clone(),
                    root_id: root.root_id.clone(),
                    root_epoch: root.root_epoch,
                    old_access_generation: access_generation,
                    command_type: RootCommandType::RevokeAccess,
                },
            )));
            txn.mutations.push(TxnMutation::RecordRequestOutcome(
                super::store::RequestOutcome {
                    request: txn.request.clone(),
                    operation: txn.operation,
                    result: OperationResult::Empty,
                },
            ));
            let _ = commit_or_replay(self.store.as_ref(), txn).await?;
            Ok(())
        })
    }

    fn lookup_root(&self, input: LookupRootInput) -> MetaFuture<'_, RootLookupState> {
        Box::pin(async move {
            let snapshot = self
                .store
                .read(MetaRead::RootState {
                    root_id: input.root_id,
                })
                .await?;
            let mut state = RootLookupState {
                root: None,
                reservation: None,
            };
            for entity in snapshot.entities {
                match entity {
                    MetaEntity::Root(root) => state.root = Some(root),
                    MetaEntity::RootReservation(reservation) => {
                        state.reservation = Some(reservation)
                    }
                    _ => return Err(invalid("root state read returned wrong entity type")),
                }
            }
            Ok(state)
        })
    }

    fn list_owner_roots(&self, input: ListOwnerRootsInput) -> MetaFuture<'_, ListOwnerRootsOutput> {
        Box::pin(async move {
            let scan = self
                .store
                .scan_owner_roots_for_recovery(&input.home_node_id)
                .await?;
            Ok(ListOwnerRootsOutput {
                authority_revision: scan.revision,
                active_roots: scan.roots,
                pending_reservations: scan.reservations,
            })
        })
    }

    fn acquire_root(&self, input: AcquireRootInput) -> MetaFuture<'_, RootAccessGrant> {
        Box::pin(async move {
            let root = self
                .store
                .read(MetaRead::Root {
                    root_id: input.root_id.clone(),
                })
                .await?;
            let Some(MetaEntity::Root(root)) = root.entity else {
                return Err(invalid("root was not found"));
            };
            if input.expected_root_epoch != 0 && input.expected_root_epoch != root.root_epoch {
                return Err(invalid("root epoch changed"));
            }
            let home_grant = self
                .store
                .read(MetaRead::RootGrantByHolder {
                    root_id: root.root_id.clone(),
                    holder_node_id: root.home_node_id.clone(),
                    holder_session_id: root.home_session_id.clone(),
                })
                .await?;
            let Some(MetaEntity::RootGrant(home_grant)) = home_grant.entity else {
                return Err(invalid("home grant was not found"));
            };
            if input.expected_access_generation != 0
                && input.expected_access_generation != home_grant.access_generation
            {
                return Err(invalid("root access generation changed"));
            }
            let existing_holder_grant = self
                .store
                .read(MetaRead::RootGrantByHolder {
                    root_id: root.root_id.clone(),
                    holder_node_id: input.requester_node_id.clone(),
                    holder_session_id: input.session_id.clone(),
                })
                .await?;
            let (existing_holder_revision, existing_rights) = match existing_holder_grant.entity {
                Some(MetaEntity::RootGrant(grant)) => {
                    let rights = if grant.root_epoch == root.root_epoch
                        && grant.home_session_id == root.home_session_id
                        && grant.access_generation == home_grant.access_generation
                    {
                        grant.rights
                    } else {
                        Vec::new()
                    };
                    (Some(existing_holder_grant.revision), rights)
                }
                None => (None, Vec::new()),
                Some(_) => return Err(invalid("holder grant lookup returned wrong entity type")),
            };
            let holder_node_id = input.requester_node_id.clone();
            let holder_session_id = input.session_id.clone();
            let rights = merge_rights(existing_rights, input.rights);
            let grant = RootAccessGrant {
                root_id: root.root_id.clone(),
                root_epoch: root.root_epoch,
                home_node_id: root.home_node_id,
                home_session_id: root.home_session_id,
                holder_node_id: holder_node_id.clone(),
                holder_session_id: holder_session_id.clone(),
                access_generation: home_grant.access_generation,
                rights,
                fencing_token: format!(
                    "fence:{}:{}:{}",
                    root.root_id, holder_session_id, home_grant.access_generation
                ),
                issued_at_revision: StoreRevision::ZERO,
            };
            let key = request_key(
                format!("{}/{}", holder_node_id, holder_session_id),
                input.request_id,
            );
            let mut txn = MetaTxn::new(key.clone(), StoreOperation::AcquireRoot);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(key),
                TxnCondition::NodeSessionCurrent {
                    node_id: holder_node_id.clone(),
                    session_id: holder_session_id.clone(),
                },
                TxnCondition::RootEpochEquals {
                    root_id: grant.root_id.clone(),
                    root_epoch: grant.root_epoch,
                },
            ]);
            if let Some(revision) = existing_holder_revision {
                txn.conditions.push(TxnCondition::RevisionEquals {
                    key: MetaKey::RootGrant {
                        root_id: grant.root_id.clone(),
                        holder_node_id,
                        holder_session_id,
                    },
                    revision,
                });
            } else {
                txn.conditions
                    .push(TxnCondition::Missing(MetaKey::RootGrant {
                        root_id: grant.root_id.clone(),
                        holder_node_id,
                        holder_session_id,
                    }));
            }
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::RootGrant(grant.clone())),
                TxnMutation::RecordRequestOutcome(super::store::RequestOutcome {
                    request: txn.request.clone(),
                    operation: txn.operation,
                    result: OperationResult::RootGrant(grant.clone()),
                }),
            ]);
            let OperationResult::RootGrant(grant) =
                commit_or_replay(self.store.as_ref(), txn).await?
            else {
                return Err(invalid("acquire replay returned wrong result type"));
            };
            Ok(grant)
        })
    }

    fn validate_root_access(
        &self,
        input: ValidateRootAccessInput,
    ) -> MetaFuture<'_, ValidateRootAccessOutput> {
        Box::pin(async move {
            let snapshot = self
                .store
                .read(MetaRead::RootGrant {
                    root_id: input.root_id,
                    root_epoch: input.root_epoch,
                    home_session_id: input.home_session_id,
                    holder_node_id: input.holder_node_id,
                    holder_session_id: input.holder_session_id,
                    access_generation: input.access_generation,
                    fencing_token: input.fencing_token,
                })
                .await?;
            let Some(MetaEntity::RootGrant(grant)) = snapshot.entity else {
                return Err(invalid("presented root access is not current"));
            };
            Ok(ValidateRootAccessOutput {
                grant,
                authority_revision: snapshot.revision,
            })
        })
    }

    fn watch_root_commands(
        &self,
        input: WatchRootCommandsInput,
    ) -> MetaFuture<'_, Vec<WatchedRootCommand>> {
        Box::pin(async move {
            let batch = self
                .store
                .watch(StoreRevision(input.after_revision), 1024)
                .await?;
            match batch {
                WatchBatch::Events { events, .. } => Ok(events
                    .into_iter()
                    .filter_map(|event| match event.change {
                        WatchChange::Put(MetaEntity::RootCommand(command))
                            if command.home_node_id == input.node_id
                                && command.home_session_id == input.session_id =>
                        {
                            Some(WatchedRootCommand {
                                command,
                                revision: event.revision,
                            })
                        }
                        _ => None,
                    })
                    .collect()),
                WatchBatch::Compacted { .. } => self
                    .resynchronize_commands(&input)
                    .await
                    .map(|(_, commands)| commands),
            }
        })
    }

    fn poll_root_command_batch(
        &self,
        input: WatchRootCommandsInput,
    ) -> MetaFuture<'_, RootCommandBatch> {
        Box::pin(async move {
            require_current_live_session(self.store.as_ref(), &input.node_id, &input.session_id)
                .await?;
            let batch = self
                .store
                .watch(StoreRevision(input.after_revision), 1024)
                .await?;
            match batch {
                WatchBatch::Events {
                    start_revision,
                    next_revision,
                    events,
                } => {
                    let max_event_revision = events.iter().map(|event| event.revision).max();
                    if next_revision <= start_revision
                        || next_revision <= StoreRevision(input.after_revision)
                        || max_event_revision.is_some_and(|revision| next_revision <= revision)
                    {
                        return Err(invalid("root command batch cursor did not advance"));
                    }
                    let commands = events
                        .into_iter()
                        .filter_map(|event| match event.change {
                            WatchChange::Put(MetaEntity::RootCommand(command))
                                if command.home_node_id == input.node_id
                                    && command.home_session_id == input.session_id =>
                            {
                                Some(WatchedRootCommand {
                                    command,
                                    revision: event.revision,
                                })
                            }
                            _ => None,
                        })
                        .collect();
                    Ok(RootCommandBatch::Events {
                        start_revision,
                        next_revision,
                        commands,
                    })
                }
                WatchBatch::Compacted { .. } => {
                    let (revision, commands) = self.resynchronize_commands(&input).await?;
                    Ok(RootCommandBatch::Events {
                        start_revision: StoreRevision(input.after_revision),
                        next_revision: revision.next(),
                        commands,
                    })
                }
            }
        })
    }

    fn ack_revocation(&self, input: AckRevocationInput) -> MetaFuture<'_, u64> {
        Box::pin(async move {
            let ack = RootCommandAck {
                command_id: input.command_id,
                node_id: input.node_id.clone(),
                session_id: input.session_id.clone(),
                root_id: input.root_id,
                root_epoch: input.root_epoch,
                access_generation: input.access_generation,
                success: input.success,
                message: input.message,
            };
            let key = request_key(
                format!("{}/{}", input.node_id, input.session_id),
                input.request_id,
            );
            let mut txn = MetaTxn::new(key.clone(), StoreOperation::AckRootCommand);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(key),
                TxnCondition::NodeSessionCurrent {
                    node_id: ack.node_id.clone(),
                    session_id: ack.session_id.clone(),
                },
                TxnCondition::RootCommandMatches {
                    command_id: ack.command_id.clone(),
                    home_node_id: ack.node_id.clone(),
                    home_session_id: ack.session_id.clone(),
                    root_id: ack.root_id.clone(),
                    root_epoch: ack.root_epoch,
                    old_access_generation: ack.access_generation,
                    command_type: RootCommandType::RevokeAccess,
                },
                TxnCondition::Missing(MetaKey::RootCommandAck {
                    command_id: ack.command_id.clone(),
                    home_node_id: ack.node_id.clone(),
                    home_session_id: ack.session_id.clone(),
                }),
            ]);
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::RootCommandAck(ack.clone())),
                TxnMutation::RecordRequestOutcome(super::store::RequestOutcome {
                    request: txn.request.clone(),
                    operation: txn.operation,
                    result: OperationResult::RootCommandAck(ack.clone()),
                }),
            ]);
            match commit_or_replay(self.store.as_ref(), txn).await? {
                OperationResult::RootCommandAck(recorded) if recorded == ack => {}
                OperationResult::RootCommandAck(_) => {
                    return Err(invalid(
                        "request_id was already used for a different root command ACK payload",
                    ));
                }
                _ => return Err(invalid("unexpected root command ACK outcome")),
            }
            Ok(now_unix_ms())
        })
    }

    fn recover_root(&self, input: RecoverRootInput) -> MetaFuture<'_, RootAccessGrant> {
        Box::pin(async move {
            let root = self
                .store
                .read(MetaRead::Root {
                    root_id: input.root_id.clone(),
                })
                .await?;
            let Some(MetaEntity::Root(mut root)) = root.entity else {
                return Err(invalid("root was not found"));
            };
            if root.home_node_id != input.home_node_id
                || root.root_epoch != input.expected_root_epoch
                || root.local_prepare_id != input.local_prepare_id
            {
                return Err(invalid("recover request does not match durable root"));
            }
            let previous = self
                .store
                .read(MetaRead::RootGrantByHolder {
                    root_id: root.root_id.clone(),
                    holder_node_id: root.home_node_id.clone(),
                    holder_session_id: root.home_session_id.clone(),
                })
                .await?;
            let next_generation = match previous.entity {
                Some(MetaEntity::RootGrant(grant)) => grant.access_generation.saturating_add(1),
                _ => 1,
            };
            root.home_session_id = input.home_session_id.clone();
            let grant = home_grant(&root, next_generation);
            let key = request_key(
                format!("{}/{}", root.home_node_id, root.home_session_id),
                input.request_id,
            );
            let mut txn = MetaTxn::new(key.clone(), StoreOperation::RecoverRoot);
            txn.conditions.extend([
                TxnCondition::RequestAbsent(key),
                TxnCondition::NodeSessionCurrent {
                    node_id: root.home_node_id.clone(),
                    session_id: root.home_session_id.clone(),
                },
                TxnCondition::RootEpochEquals {
                    root_id: root.root_id.clone(),
                    root_epoch: root.root_epoch,
                },
            ]);
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::Root(root)),
                TxnMutation::Put(MetaEntity::RootGrant(grant.clone())),
                TxnMutation::RecordRequestOutcome(super::store::RequestOutcome {
                    request: txn.request.clone(),
                    operation: txn.operation,
                    result: OperationResult::RootGrant(grant.clone()),
                }),
            ]);
            let OperationResult::RootGrant(grant) =
                commit_or_replay(self.store.as_ref(), txn).await?
            else {
                return Err(invalid("recover replay returned wrong result type"));
            };
            Ok(grant)
        })
    }
}

fn reservation_record(
    input: &ReserveRootInput,
    home: NodeSession,
    root_epoch: u64,
) -> RootReservationRecord {
    RootReservationRecord {
        root_id: input.root_id.clone(),
        root_epoch,
        home_node_id: home.node_id,
        home_session_id: home.session_id,
        create_intent_id: input.create_intent_id.clone(),
        prepare_token: format!(
            "prepare:{}:{}:{}:{}",
            input.root_id, input.session_id, input.create_intent_id, input.request_id
        ),
        requester_node_id: input.preferred_home_node_id.clone(),
        requester_session_id: input.session_id.clone(),
        created_at_revision: StoreRevision::ZERO,
    }
}

async fn reservation_requester_is_live(
    store: &dyn MetaStore,
    reservation: &RootReservationRecord,
) -> Result<bool> {
    let snapshot = store
        .read(MetaRead::CurrentNodeSession {
            node_id: reservation.requester_node_id.clone(),
        })
        .await?;
    Ok(matches!(
        snapshot.entity,
        Some(MetaEntity::NodeSession(session))
            if session.session_id == reservation.requester_session_id
                && session.is_live_at_unix_ms(now_unix_ms())
    ))
}

async fn select_home(
    store: &dyn MetaStore,
    load_balancing: bool,
    root_id: &str,
    requester_node_id: &str,
    requester_session_id: &str,
) -> Result<NodeSession> {
    if !load_balancing {
        return require_current_live_session(store, requester_node_id, requester_session_id).await;
    }
    let snapshot = store.read(MetaRead::CurrentNodeSessions).await?;
    let sessions = snapshot
        .entities
        .into_iter()
        .filter_map(|entity| match entity {
            MetaEntity::NodeSession(session)
                if session.ownerfs_enabled && session.is_live_at_unix_ms(now_unix_ms()) =>
            {
                Some(session)
            }
            _ => None,
        });
    let mut selected: Option<(blake3::Hash, NodeSession)> = None;
    for session in sessions {
        let score = placement_score(root_id, &session.node_id);
        let replace = selected
            .as_ref()
            .is_none_or(|(selected_score, selected_session)| {
                placement_is_preferred(
                    &score,
                    &session.node_id,
                    selected_score,
                    &selected_session.node_id,
                )
            });
        if replace {
            selected = Some((score, session));
        }
    }
    selected
        .map(|(_, session)| session)
        .ok_or_else(|| home_unavailable("no live node is available for workspace creation"))
}

fn placement_is_preferred(
    score: &blake3::Hash,
    node_id: &str,
    selected_score: &blake3::Hash,
    selected_node_id: &str,
) -> bool {
    score.as_bytes().cmp(selected_score.as_bytes()).is_gt()
        || (score == selected_score && node_id < selected_node_id)
}

fn placement_score(root_id: &str, node_id: &str) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"afs-ownerfs-placement-v1");
    hasher.update(
        &u64::try_from(root_id.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(root_id.as_bytes());
    hasher.update(
        &u64::try_from(node_id.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(node_id.as_bytes());
    hasher.finalize()
}

pub struct ReserveRootInput {
    pub request_id: String,
    pub root_id: String,
    /// Entry node identity; Meta selects the actual Home.
    pub preferred_home_node_id: String,
    pub session_id: String,
    pub rights: Vec<RootRight>,
    /// Zero selects automatic allocation; a nonzero floor must not exceed the
    /// authority's next epoch. The caller cannot advance the global watermark.
    pub expected_root_epoch: u64,
    pub create_intent_id: String,
}

pub struct ReserveRootOutput {
    pub reservation: RootReservationRecord,
    pub already_reserved_by_same_intent: bool,
}

pub struct ActivateRootInput {
    pub request_id: String,
    pub reservation: WireRootReservation,
    pub local_prepare_id: String,
}

pub struct WireRootReservation {
    pub root_id: String,
    pub root_epoch: u64,
    pub home_node_id: String,
    pub session_id: String,
    pub create_intent_id: String,
    pub prepare_token: String,
    pub requester_node_id: String,
    pub requester_session_id: String,
}

pub struct AbortRootInput {
    pub request_id: String,
    pub root_id: String,
    pub root_epoch: u64,
    pub session_id: String,
    pub create_intent_id: String,
    pub prepare_token: String,
    pub authenticated_home_node_id: Option<String>,
}

pub struct DeactivateRootInput {
    pub request_id: String,
    pub root_id: String,
    pub root_epoch: u64,
    pub home_node_id: String,
    pub home_session_id: String,
    pub reason: String,
    pub authenticated_home_node_id: Option<String>,
}

pub struct LookupRootInput {
    pub root_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootLookupState {
    pub root: Option<RootRecord>,
    pub reservation: Option<RootReservationRecord>,
}

pub struct ListOwnerRootsInput {
    pub home_node_id: String,
}

pub struct ListOwnerRootsOutput {
    pub authority_revision: StoreRevision,
    pub active_roots: Vec<RootRecord>,
    pub pending_reservations: Vec<RootReservationRecord>,
}

pub struct AcquireRootInput {
    pub request_id: String,
    pub root_id: String,
    pub requester_node_id: String,
    pub session_id: String,
    pub rights: Vec<RootRight>,
    pub expected_root_epoch: u64,
    pub expected_access_generation: u64,
}

pub struct ValidateRootAccessInput {
    pub root_id: String,
    pub root_epoch: u64,
    pub home_session_id: String,
    pub holder_node_id: String,
    pub holder_session_id: String,
    pub access_generation: u64,
    pub fencing_token: String,
}

pub struct ValidateRootAccessOutput {
    pub grant: RootAccessGrant,
    pub authority_revision: StoreRevision,
}

pub struct WatchRootCommandsInput {
    pub node_id: String,
    pub session_id: String,
    pub after_revision: u64,
}

pub struct WatchedRootCommand {
    pub command: RootCommandRecord,
    pub revision: StoreRevision,
}

pub enum RootCommandBatch {
    Events {
        start_revision: StoreRevision,
        next_revision: StoreRevision,
        commands: Vec<WatchedRootCommand>,
    },
    Compacted {
        requested_after: StoreRevision,
        compacted_to: StoreRevision,
        recovery: RecoveryCursor,
    },
    Unsupported {
        message: String,
    },
}

pub struct AckRevocationInput {
    pub request_id: String,
    pub command_id: String,
    pub node_id: String,
    pub session_id: String,
    pub root_id: String,
    pub root_epoch: u64,
    pub access_generation: u64,
    pub success: bool,
    pub message: String,
}

pub struct RecoverRootInput {
    pub request_id: String,
    pub root_id: String,
    pub expected_root_epoch: u64,
    pub home_node_id: String,
    pub home_session_id: String,
    pub local_prepare_id: String,
}

async fn commit_or_replay(store: &dyn MetaStore, txn: MetaTxn) -> Result<OperationResult> {
    let operation = txn.operation;
    match store.compare_and_commit(txn).await? {
        TxnOutcome::Committed { outcome, .. } => Ok(outcome.result),
        TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } if outcome.operation == operation => Ok(outcome.result),
        TxnOutcome::ConditionFailed {
            existing_outcome: Some(_),
            ..
        } => Err(invalid(
            "request_id was already used for a different Meta operation",
        )),
        TxnOutcome::ConditionFailed { .. } => Err(invalid(
            "Meta transaction conditions failed; caller must refresh authority state",
        )),
    }
}

fn take_reserve_result(
    result: OperationResult,
    input: &ReserveRootInput,
) -> Result<RootReservationRecord> {
    let OperationResult::RootReservation(reservation) = result else {
        return Err(invalid("reserve replay returned wrong result type"));
    };
    if reservation.root_id != input.root_id
        || reservation.root_epoch == 0
        || reservation.root_epoch < input.expected_root_epoch
        || reservation.create_intent_id != input.create_intent_id
        || reservation.requester_node_id != input.preferred_home_node_id
        || reservation.requester_session_id != input.session_id
    {
        return Err(invalid("reserve replay parameters do not match"));
    }
    Ok(reservation)
}

async fn classify_reserve_outcome(
    store: &dyn MetaStore,
    outcome: TxnOutcome,
    root_id: &str,
    previous_reservation: Option<&RootReservationRecord>,
) -> Result<OperationResult> {
    match outcome {
        TxnOutcome::Committed { outcome, .. } => Ok(outcome.result),
        TxnOutcome::ConditionFailed {
            existing_outcome: Some(outcome),
            ..
        } if outcome.operation == StoreOperation::ReserveRoot => Ok(outcome.result),
        TxnOutcome::ConditionFailed {
            existing_outcome: Some(_),
            ..
        } => Err(invalid(
            "request_id was already used for a different Meta operation",
        )),
        TxnOutcome::ConditionFailed {
            existing_outcome: None,
            ..
        } => {
            let state = store
                .read(MetaRead::RootState {
                    root_id: root_id.to_owned(),
                })
                .await?;
            if state
                .entities
                .iter()
                .any(|entity| matches!(entity, MetaEntity::Root(_)))
            {
                return Err(root_exists("root was activated while reserving"));
            }
            if let Some(reservation) = state.entities.iter().find_map(|entity| match entity {
                MetaEntity::RootReservation(reservation) => Some(reservation),
                _ => None,
            }) && Some(reservation) != previous_reservation
            {
                return Err(root_busy("root is reserved by another request"));
            }
            // The old reservation can remain when requester/Home authority
            // changed before commit. Its unchanged identity is not evidence
            // that another request won the name.
            Err(home_unavailable(
                "node authority changed while reserving; caller must refresh authority state",
            ))
        }
    }
}

async fn require_current_live_session(
    store: &dyn MetaStore,
    node_id: &str,
    session_id: &str,
) -> Result<NodeSession> {
    let snapshot = store
        .read(MetaRead::CurrentNodeSession {
            node_id: node_id.to_owned(),
        })
        .await?;
    match snapshot.entity {
        Some(MetaEntity::NodeSession(session))
            if session.session_id == session_id && session.is_live_at_unix_ms(now_unix_ms()) =>
        {
            Ok(session)
        }
        _ => Err(invalid("node session is not current")),
    }
}

fn request_key(caller_id: impl Into<String>, request_id: impl Into<String>) -> RequestKey {
    RequestKey::new(caller_id, request_id)
}

fn home_grant(root: &RootRecord, access_generation: u64) -> RootAccessGrant {
    RootAccessGrant {
        root_id: root.root_id.clone(),
        root_epoch: root.root_epoch,
        home_node_id: root.home_node_id.clone(),
        home_session_id: root.home_session_id.clone(),
        holder_node_id: root.home_node_id.clone(),
        holder_session_id: root.home_session_id.clone(),
        access_generation,
        rights: vec![
            RootRight::Lookup,
            RootRight::Read,
            RootRight::Write,
            RootRight::Admin,
        ],
        fencing_token: format!(
            "fence:{}:{}:{}",
            root.root_id, root.home_session_id, access_generation
        ),
        issued_at_revision: StoreRevision::ZERO,
    }
}

fn merge_rights(
    existing: impl IntoIterator<Item = RootRight>,
    requested: impl IntoIterator<Item = RootRight>,
) -> Vec<RootRight> {
    let mut has_lookup = false;
    let mut has_read = false;
    let mut has_write = false;
    let mut has_admin = false;
    for right in existing.into_iter().chain(requested) {
        match right {
            RootRight::Lookup => has_lookup = true,
            RootRight::Read => has_read = true,
            RootRight::Write => has_write = true,
            RootRight::Admin => has_admin = true,
        }
    }
    let mut out = Vec::new();
    if has_lookup {
        out.push(RootRight::Lookup);
    }
    if has_read {
        out.push(RootRight::Read);
    }
    if has_write {
        out.push(RootRight::Write);
    }
    if has_admin {
        out.push(RootRight::Admin);
    }
    out
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::META_CATALOG_INVALID_REQUEST, message)
}

fn root_exists(message: impl Into<String>) -> Error {
    Error::coded(afs_error::META_OWNER_ROOT_EXISTS, message)
}

fn root_busy(message: impl Into<String>) -> Error {
    Error::coded(afs_error::META_OWNER_ROOT_BUSY, message)
}

fn home_unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::META_OWNER_HOME_UNAVAILABLE, message)
}

fn permission_denied(message: impl Into<String>) -> Error {
    Error::coded(afs_error::CLIENT_PERMISSION_DENIED, message)
}

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod retention_tests {
    use super::*;
    use crate::meta::store::{HistoryLimits, NodeSessionLease, Store, journal::MemoryLog};
    #[tokio::test]
    async fn compacted_watch_resynchronizes_pending_commands_without_losing_new_events() {
        let store = Arc::new(
            Store::open_log_with_history(
                Arc::new(MemoryLog::new(Default::default()).unwrap()),
                HistoryLimits {
                    event_count: 8,
                    ..Default::default()
                },
            )
            .await
            .unwrap(),
        );
        let key = || RequestKey::new("node", crate::clock::timed_request_id(uuid_id()));
        store
            .register_node_session(
                key(),
                NodeSessionLease {
                    node_id: "n".into(),
                    session_id: "s".into(),
                    grpc_addr: "http://n:1".into(),
                    data_addr: "http://n:2".into(),
                    rest_addr: "http://n:3".into(),
                    ownerfs_enabled: true,
                    storage_devices: vec![],
                    lease_ttl: std::time::Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
        async fn command(store: &Store, key: RequestKey, id: &str) {
            let mut txn = MetaTxn::new(key.clone(), StoreOperation::BeginRootRevocation);
            txn.conditions
                .push(TxnCondition::RequestAbsent(key.clone()));
            txn.mutations.push(TxnMutation::Put(MetaEntity::RootCommand(
                RootCommandRecord {
                    command_id: id.into(),
                    home_node_id: "n".into(),
                    home_session_id: "s".into(),
                    root_id: "root".into(),
                    root_epoch: 1,
                    old_access_generation: 1,
                    command_type: RootCommandType::RevokeAccess,
                },
            )));
            txn.mutations.push(TxnMutation::RecordRequestOutcome(
                super::super::store::RequestOutcome {
                    request: key,
                    operation: StoreOperation::BeginRootRevocation,
                    result: OperationResult::Empty,
                },
            ));
            assert!(matches!(
                store.compare_and_commit(txn).await.unwrap(),
                TxnOutcome::Committed { .. }
            ));
        }
        command(&store, key(), "old-pending").await;
        // Manually seeded commands need the same durable epoch floor that
        // normal root admission establishes before history can be retired.
        let bootstrap_key = key();
        let mut bootstrap = MetaTxn::new(bootstrap_key.clone(), StoreOperation::CleanupRootGrants);
        bootstrap.conditions.extend([
            TxnCondition::RequestAbsent(bootstrap_key.clone()),
            TxnCondition::Missing(MetaKey::RootEpochWatermark),
            TxnCondition::RootEpochWatermarkEquals(1),
        ]);
        bootstrap
            .mutations
            .push(TxnMutation::Put(MetaEntity::RootEpochWatermark(1)));
        bootstrap.mutations.push(TxnMutation::RecordRequestOutcome(
            super::super::store::RequestOutcome {
                request: bootstrap_key,
                operation: StoreOperation::CleanupRootGrants,
                result: OperationResult::Empty,
            },
        ));
        assert!(matches!(
            store.compare_and_commit(bootstrap).await.unwrap(),
            TxnOutcome::Committed { .. }
        ));
        for index in 0..8 {
            store
                .register_node_session(
                    key(),
                    NodeSessionLease {
                        node_id: "unrelated".into(),
                        session_id: format!("session-{index}"),
                        grpc_addr: "http://other:1".into(),
                        data_addr: "http://other:2".into(),
                        rest_addr: "http://other:3".into(),
                        ownerfs_enabled: true,
                        storage_devices: vec![],
                        lease_ttl: std::time::Duration::from_secs(30),
                    },
                )
                .await
                .unwrap();
        }
        assert!(matches!(
            store.watch(StoreRevision(0), 16).await.unwrap(),
            WatchBatch::Compacted { .. }
        ));
        let authority = StoreOwnerRootAuthority::new(store.clone());
        let RootCommandBatch::Events {
            commands,
            next_revision,
            ..
        } = authority
            .poll_root_command_batch(WatchRootCommandsInput {
                node_id: "n".into(),
                session_id: "s".into(),
                after_revision: 0,
            })
            .await
            .unwrap()
        else {
            panic!("compaction must return a full resynchronization")
        };
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command.command_id, "old-pending");
        command(&store, key(), "new-pending").await;
        let RootCommandBatch::Events { commands, .. } = authority
            .poll_root_command_batch(WatchRootCommandsInput {
                node_id: "n".into(),
                session_id: "s".into(),
                after_revision: next_revision.0 - 1,
            })
            .await
            .unwrap()
        else {
            panic!("incremental follow-up")
        };
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command.command_id, "new-pending");
        assert!(
            authority
                .poll_root_command_batch(WatchRootCommandsInput {
                    node_id: "n".into(),
                    session_id: "old".into(),
                    after_revision: 0
                })
                .await
                .is_err()
        );
    }
    fn uuid_id() -> u64 {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::store::journal::MemoryLog;
    use crate::meta::store::{NodeSessionLease, Store};
    use std::time::Duration;

    fn wire_reservation(reservation: &RootReservationRecord) -> WireRootReservation {
        WireRootReservation {
            root_id: reservation.root_id.clone(),
            root_epoch: reservation.root_epoch,
            home_node_id: reservation.home_node_id.clone(),
            session_id: reservation.home_session_id.clone(),
            create_intent_id: reservation.create_intent_id.clone(),
            prepare_token: reservation.prepare_token.clone(),
            requester_node_id: reservation.requester_node_id.clone(),
            requester_session_id: reservation.requester_session_id.clone(),
        }
    }

    async fn register_node(store: &Store, node_id: &str, session_id: &str) {
        register_node_with_ttl(store, node_id, session_id, Duration::from_secs(60)).await;
    }

    async fn register_node_with_ttl(
        store: &Store,
        node_id: &str,
        session_id: &str,
        lease_ttl: Duration,
    ) {
        store
            .register_node_session(
                RequestKey::new(
                    node_id,
                    format!("register-{session_id}-{}", lease_ttl.as_nanos()),
                ),
                NodeSessionLease {
                    node_id: node_id.into(),
                    session_id: session_id.into(),
                    grpc_addr: format!("http://{node_id}:7400"),
                    data_addr: format!("http://{node_id}:7500"),
                    rest_addr: format!("http://{node_id}:7600"),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl,
                },
            )
            .await
            .expect("register test node");
    }

    async fn open_store() -> Arc<Store> {
        Arc::new(
            Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
                .await
                .expect("open memory store"),
        )
    }

    fn reserve_input(
        requester_node_id: &str,
        requester_session_id: &str,
        request_id: &str,
        create_intent_id: &str,
    ) -> ReserveRootInput {
        ReserveRootInput {
            request_id: request_id.into(),
            root_id: "root-6167656e742d61".into(),
            rights: vec![RootRight::Lookup],
            expected_root_epoch: 0,
            create_intent_id: create_intent_id.into(),
            preferred_home_node_id: requester_node_id.into(),
            session_id: requester_session_id.into(),
        }
    }

    #[test]
    fn equal_placement_scores_prefer_the_smallest_node_id() {
        let tied = blake3::Hash::from_bytes([0x80; 32]);
        assert!(placement_is_preferred(&tied, "node-a", &tied, "node-b"));
        assert!(!placement_is_preferred(&tied, "node-b", &tied, "node-a"));
        assert!(!placement_is_preferred(&tied, "node-a", &tied, "node-a"));
        let smaller = blake3::Hash::from_bytes([0x7f; 32]);
        assert!(placement_is_preferred(&tied, "node-z", &smaller, "node-a"));
        assert!(!placement_is_preferred(&smaller, "node-a", &tied, "node-z"));
    }

    #[test]
    fn placement_scores_match_independent_blake3_vectors() {
        // Computed independently with Python blake3 from the documented byte
        // layout; these constants pin domain, lengths, order and digest bytes.
        for (root_id, node_id, expected) in [
            (
                "root-6167656e742d61",
                "node-a",
                "ed8e0b31e0c45cf6ed45eb3391531a6e78039ee7b62850de0e0fa398c5d3b350",
            ),
            (
                "root-6167656e742d61",
                "node-b",
                "ef11aee7c1e2f56af8279c5df511cae7fda66d2606e82cec1e1406b7e208243f",
            ),
            (
                "root-ff80",
                "node-a",
                "3269e57524da2f1ede6a434ff7c4150a6e505a027365b7b2e9fa3977c3b4f19d",
            ),
        ] {
            assert_eq!(
                placement_score(root_id, node_id).to_hex().as_str(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn load_balancing_selects_current_live_home_and_persists_requester() {
        let store = open_store().await;
        register_node(&store, "node-a", "session-a").await;
        register_node(&store, "node-b", "old-session-b").await;
        register_node(&store, "node-b", "session-b").await;
        let authority = StoreOwnerRootAuthority::with_load_balancing(store.clone(), true);
        let input = reserve_input("node-a", "session-a", "reserve-1", "intent-1");
        let output = authority.reserve_root(input).await.expect("reserve root");
        assert_eq!(output.reservation.home_node_id, "node-b");
        assert_eq!(output.reservation.home_session_id, "session-b");
        assert_eq!(output.reservation.requester_node_id, "node-a");
        assert_eq!(output.reservation.requester_session_id, "session-a");

        register_node_with_ttl(&store, "node-b", "session-b", Duration::from_millis(5)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let selected = select_home(
            store.as_ref(),
            true,
            "root-6167656e742d61",
            "node-a",
            "session-a",
        )
        .await
        .expect("expired higher-scoring candidate must be excluded");
        assert_eq!(selected.node_id, "node-a");
    }

    #[tokio::test]
    async fn activate_replay_rejects_a_different_prepare_identity() {
        let store = open_store().await;
        register_node(store.as_ref(), "node-a", "session-a").await;
        let authority = StoreOwnerRootAuthority::new(store);
        let reservation = authority
            .reserve_root(ReserveRootInput {
                request_id: "reserve-1".into(),
                root_id: "root-6167656e742d61".into(),
                rights: vec![RootRight::Lookup],
                expected_root_epoch: 0,
                create_intent_id: "node-a:session-a:agent-a".into(),
                preferred_home_node_id: "node-a".into(),
                session_id: "session-a".into(),
            })
            .await
            .expect("reserve root")
            .reservation;

        authority
            .activate_root(ActivateRootInput {
                request_id: "activate-1".into(),
                reservation: wire_reservation(&reservation),
                local_prepare_id: "local-prepare-1".into(),
            })
            .await
            .expect("activate root");

        let replay = authority
            .activate_root(ActivateRootInput {
                request_id: "activate-1".into(),
                reservation: wire_reservation(&reservation),
                local_prepare_id: "local-prepare-2".into(),
            })
            .await
            .expect_err("changed prepare identity must not replay the grant");
        assert_eq!(replay.code(), afs_error::META_CATALOG_INVALID_REQUEST);
    }

    #[tokio::test]
    async fn disabled_load_balancing_keeps_requester_as_home() {
        let store = open_store().await;
        register_node(store.as_ref(), "node-a", "session-a").await;
        register_node(store.as_ref(), "node-b", "session-b").await;
        let output = StoreOwnerRootAuthority::new(store)
            .reserve_root(reserve_input(
                "node-a",
                "session-a",
                "reserve-1",
                "node-a:session-a:agent-a",
            ))
            .await
            .expect("reserve root with local fallback");
        assert_eq!(output.reservation.home_node_id, "node-a");
    }

    #[tokio::test]
    async fn dfs_only_node_is_not_a_workspace_home() {
        let store = open_store().await;
        store
            .register_node_session(
                RequestKey::new("node-b", "register-dfs"),
                NodeSessionLease {
                    node_id: "node-b".into(),
                    session_id: "session-b".into(),
                    grpc_addr: "http://node-b:7400".into(),
                    data_addr: "http://node-b:7500".into(),
                    rest_addr: "http://node-b:7600".into(),
                    ownerfs_enabled: false,
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(60),
                },
            )
            .await
            .unwrap();
        let error = select_home(
            store.as_ref(),
            true,
            "root-6167656e742d61",
            "node-b",
            "session-b",
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), afs_error::META_OWNER_HOME_UNAVAILABLE);
        register_node(&store, "node-a", "session-a").await;
        // node-b has the higher HRW score for this name, but cannot create OwnerFs roots.
        let home = select_home(
            store.as_ref(),
            true,
            "root-6167656e742d61",
            "node-a",
            "session-a",
        )
        .await
        .unwrap();
        assert_eq!(home.node_id, "node-a");
    }

    #[tokio::test]
    async fn no_live_candidate_returns_structured_unavailable() {
        let store = open_store().await;
        let error = match StoreOwnerRootAuthority::with_load_balancing(store, true)
            .reserve_root(reserve_input(
                "node-a",
                "session-a",
                "reserve-1",
                "node-a:session-a:agent-a",
            ))
            .await
        {
            Ok(_) => panic!("no live candidate must fail"),
            Err(error) => error,
        };
        assert_eq!(error.code(), afs_error::META_OWNER_HOME_UNAVAILABLE);
    }
}
