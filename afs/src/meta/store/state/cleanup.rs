//! Root grant and root command cleanup preparation.

use super::*;

impl State {
    /// Runs only on the single writer, without an await between selection and
    /// preparation. Queued acquire/recover/recreate operations cannot interleave.
    /// The cursor is disposable: restart rescans; only decided deletes are durable.
    pub(in crate::meta::store) fn prepare_root_grant_cleanup(
        &self,
        cursor: &mut Option<MetaKey>,
        completed_at: u64,
        byte_limit: usize,
        max_deletes: usize,
    ) -> Result<Option<(Self, super::super::log::RecordDecision)>> {
        const SCAN: usize = 128;
        let delete_limit = 32.min(max_deletes);
        if delete_limit == 0 {
            return Ok(None);
        }
        // Soft target: one existing large key can exceed it, but the complete
        // selected record is still checked against the unchanged hard frame limit.
        const KEY_BYTES: usize = 64 * 1024;
        let start = cursor.clone().unwrap_or(MetaKey::RootGrant {
            root_id: String::new(),
            holder_node_id: String::new(),
            holder_session_id: String::new(),
        });
        let request = RequestKey::new(
            "meta-maintenance/root-grants",
            format!("after-{}", self.revision.0),
        );
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::CleanupRootGrants);
        txn.conditions
            .push(super::super::TxnCondition::RequestAbsent(request.clone()));
        let bootstrap = (!self.entities.contains_key(&MetaKey::RootEpochWatermark))
            .then(|| self.root_epoch_watermark())
            .filter(|value| *value != 0);
        if let Some(value) = bootstrap {
            txn.conditions.extend([
                super::super::TxnCondition::Missing(MetaKey::RootEpochWatermark),
                super::super::TxnCondition::RootEpochWatermarkEquals(value),
            ]);
        }
        let base_conditions = txn.conditions.len();
        let mut scanned = 0;
        let mut deleted = 0;
        let mut bytes: usize = 0;
        let previous = cursor.clone();
        let mut exhausted = true;
        for (key, versioned) in self.entities.starting_at(start) {
            if !matches!(key, MetaKey::RootGrant { .. }) {
                break;
            }
            if previous.as_ref().is_some_and(|old| key <= old) {
                continue;
            }
            if scanned == SCAN || deleted == delete_limit || bytes >= KEY_BYTES {
                exhausted = false;
                break;
            }
            scanned += 1;
            *cursor = Some(key.clone());
            let MetaEntity::RootGrant(grant) = &versioned.entity else {
                return Err(invalid_contract("root grant key has wrong entity type"));
            };
            if !self.obsolete_root_grant(grant, completed_at) {
                continue;
            }
            let key_bytes = super::super::codec::encoded_size(key)?;
            bytes = bytes
                .checked_add(key_bytes)
                .ok_or_else(|| invalid_contract("grant cleanup size overflow"))?;
            txn.conditions
                .push(super::super::TxnCondition::RevisionEquals {
                    key: key.clone(),
                    revision: versioned.revision,
                });
            txn.mutations
                .push(super::super::TxnMutation::Delete(key.clone()));
            deleted += 1;
        }
        if exhausted {
            *cursor = None;
        }
        if deleted == 0 && bootstrap.is_none() {
            return Ok(None);
        }
        let watermark = bootstrap
            .map(|value| super::super::TxnMutation::Put(MetaEntity::RootEpochWatermark(value)));
        if let Some(mutation) = &watermark {
            txn.mutations.push(mutation.clone());
        }
        let result = super::super::TxnMutation::RecordRequestOutcome(RequestOutcome {
            request,
            operation: txn.operation,
            result: OperationResult::Empty,
        });
        txn.mutations.push(result.clone());
        // A lower configured record limit must not make every 32-delete batch
        // permanently fail. Shrink only this selected batch, at most five times;
        // never rescan or change the logical deactivation transaction.
        loop {
            let prepared = self
                .prepare_txn_record(txn.clone(), completed_at, byte_limit)
                .and_then(|(next, _, record)| {
                    let Some(record) = record else {
                        return Ok(None);
                    };
                    let frame = super::super::log::Record::from(record.clone());
                    if super::super::journal::frame_size(self.log_cursor, &frame)? > byte_limit {
                        return Err(super::super::journal::full(
                            "grant cleanup exceeds framed byte limit",
                        ));
                    }
                    Ok(Some((next, record)))
                });
            match prepared {
                Ok(result) => return Ok(result),
                Err(error) if deleted <= 1 => return Err(error),
                Err(_) => {}
            }
            deleted /= 2;
            txn.conditions.truncate(deleted + base_conditions);
            txn.mutations.truncate(deleted);
            if let Some(super::super::TxnMutation::Delete(key)) = txn.mutations.last() {
                *cursor = Some(key.clone());
            }
            if let Some(mutation) = &watermark {
                txn.mutations.push(mutation.clone());
            }
            txn.mutations.push(result.clone());
        }
    }

    /// At most128 command candidates/16 atomic pairs per pass. Expired sessions
    /// remain durable rejection tombstones; pending/failed/orphan ACKs remain.
    pub(in crate::meta::store) fn prepare_root_command_cleanup(
        &self,
        cursor: &mut Option<MetaKey>,
        now: u64,
        byte_limit: usize,
        max_deletes: usize,
    ) -> Result<Option<(Self, super::super::log::RecordDecision)>> {
        if max_deletes < 2
            || !self.history_indexed
            || !self.entities.contains_key(&MetaKey::RootEpochWatermark)
        {
            return Ok(None);
        }
        let start = cursor.clone().unwrap_or(MetaKey::RootCommand {
            command_id: String::new(),
        });
        let previous = cursor.clone();
        let mut pairs = Vec::new();
        let mut key_bytes = 0usize;
        let mut scanned = 0;
        let mut exhausted = true;
        for (key, entry) in self.entities.starting_at(start) {
            let MetaKey::RootCommand { command_id } = key else {
                break;
            };
            if previous.as_ref().is_some_and(|old| key <= old) {
                continue;
            }
            if scanned == 128 || pairs.len() == 16.min(max_deletes / 2) || key_bytes >= 64 * 1024 {
                exhausted = false;
                break;
            }
            scanned += 1;
            *cursor = Some(key.clone());
            let MetaEntity::RootCommand(command) = &entry.entity else {
                return Err(invalid_contract("command key has wrong entity type"));
            };
            if !matches!(self.entities.get(&MetaKey::RootEpochWatermark).map(|v| &v.entity), Some(MetaEntity::RootEpochWatermark(value)) if *value >= command.root_epoch)
            {
                continue;
            }
            if entry.revision > self.compacted_to
                || self.command_history.contains_key(command_id)
                || self.entities.contains_key(&MetaKey::RootReservation {
                    root_id: command.root_id.clone(),
                })
            {
                continue;
            }
            let ack_key = MetaKey::RootCommandAck {
                command_id: command_id.clone(),
                home_node_id: command.home_node_id.clone(),
                home_session_id: command.home_session_id.clone(),
            };
            let Some(ack_entry) = self.entities.get(&ack_key) else {
                continue;
            };
            let MetaEntity::RootCommandAck(ack) = &ack_entry.entity else {
                return Err(invalid_contract("ack key has wrong entity type"));
            };
            if ack_entry.revision > self.compacted_to
                || !ack.success
                || ack.command_id != command.command_id
                || ack.node_id != command.home_node_id
                || ack.session_id != command.home_session_id
                || ack.root_id != command.root_id
                || ack.root_epoch != command.root_epoch
                || ack.access_generation != command.old_access_generation
            {
                continue;
            }
            let safe = match self
                .entities
                .get(&MetaKey::Root {
                    root_id: command.root_id.clone(),
                })
                .map(|v| &v.entity)
            {
                None => true,
                Some(MetaEntity::Root(root))
                    if root.root_epoch != command.root_epoch
                        || root.home_node_id != command.home_node_id
                        || root.home_session_id != command.home_session_id =>
                {
                    true
                }
                Some(MetaEntity::Root(root)) => {
                    matches!(self.entities.get(&MetaKey::RootGrant { root_id: root.root_id.clone(), holder_node_id: root.home_node_id.clone(), holder_session_id: root.home_session_id.clone() }).map(|v| &v.entity), Some(MetaEntity::RootGrant(grant)) if grant.root_epoch == root.root_epoch && grant.home_session_id == root.home_session_id && grant.access_generation > command.old_access_generation)
                }
                _ => false,
            };
            if !safe {
                continue;
            }
            key_bytes = key_bytes
                .checked_add(super::super::codec::encoded_size(key)?)
                .and_then(|n| n.checked_add(super::super::codec::encoded_size(&ack_key).ok()?))
                .ok_or_else(|| invalid_contract("command cleanup key charge overflow"))?;
            pairs.push(((key.clone(), entry.revision), (ack_key, ack_entry.revision)));
        }
        if exhausted {
            *cursor = None;
        }
        while !pairs.is_empty() {
            let request = RequestKey::new(
                "meta-maintenance/root-commands",
                format!("after-{}", self.revision.0),
            );
            let mut txn = MetaTxn::new(request.clone(), StoreOperation::CleanupRootCommands);
            txn.conditions
                .push(super::super::TxnCondition::RequestAbsent(request.clone()));
            for (command, ack) in &pairs {
                for (key, revision) in [command, ack] {
                    txn.conditions
                        .push(super::super::TxnCondition::RevisionEquals {
                            key: key.clone(),
                            revision: *revision,
                        });
                    txn.mutations.push(TxnMutation::Delete(key.clone()));
                }
            }
            txn.mutations
                .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request,
                    operation: txn.operation,
                    result: OperationResult::Empty,
                }));
            let result =
                self.prepare_txn_record(txn, now, byte_limit)
                    .and_then(|(next, _, record)| {
                        let record = record.ok_or_else(|| {
                            invalid_contract("new command cleanup unexpectedly replayed")
                        })?;
                        if super::super::journal::frame_size(
                            self.log_cursor,
                            &super::super::log::Record::from(record.clone()),
                        )? > byte_limit
                        {
                            return Err(super::super::journal::full(
                                "command cleanup exceeds framed byte limit",
                            ));
                        }
                        Ok(Some((next, record)))
                    });
            match result {
                Ok(done) => return Ok(done),
                Err(error) if pairs.len() == 1 => return Err(error),
                Err(_) => {
                    pairs.truncate(pairs.len() / 2);
                    *cursor = pairs.last().map(|(command, _)| command.0.clone());
                }
            }
        }
        Ok(None)
    }

    fn obsolete_root_grant(&self, grant: &super::super::RootAccessGrant, now: u64) -> bool {
        let root = self.entities.get(&MetaKey::Root {
            root_id: grant.root_id.clone(),
        });
        let Some(MetaEntity::Root(root)) = root.map(|value| &value.entity) else {
            return true;
        };
        if root.root_epoch != grant.root_epoch
            || root.home_node_id != grant.home_node_id
            || root.home_session_id != grant.home_session_id
        {
            return true;
        }
        // Even an expired Home needs this exact record to recover its generation.
        if root.home_node_id == grant.holder_node_id
            && root.home_session_id == grant.holder_session_id
        {
            return false;
        }
        let current = self.entities.get(&MetaKey::CurrentNodeSession {
            node_id: grant.holder_node_id.clone(),
        });
        if let Some(MetaEntity::NodeSession(current)) = current.map(|value| &value.entity)
            && (current.session_id != grant.holder_session_id || !current.is_live_at_unix_ms(now))
        {
            return true;
        }
        // register_validated rejects expired session IDs; ordinary expired
        // holders cannot renew back into this grant. Current Home was protected above.
        let home = self.entities.get(&MetaKey::RootGrant {
            root_id: root.root_id.clone(),
            holder_node_id: root.home_node_id.clone(),
            holder_session_id: root.home_session_id.clone(),
        });
        matches!(home.map(|value| &value.entity), Some(MetaEntity::RootGrant(home))
            if home.root_epoch == root.root_epoch && home.home_session_id == root.home_session_id
                && home.access_generation != grant.access_generation)
    }
}
