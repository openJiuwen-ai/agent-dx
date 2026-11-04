//! Reads, watch and recovery scans over the published State.

use super::*;

impl State {
    /// First allocation bootstraps old authority once. Subsequent allocations
    /// use the durable singleton; deleted roots/aborted reservations cannot reset it.
    pub(in crate::meta::store) fn root_epoch_watermark(&self) -> u64 {
        if let Some(record) = self.entities.get(&MetaKey::RootEpochWatermark)
            && let MetaEntity::RootEpochWatermark(value) = &record.entity
        {
            return *value;
        }
        let entities = self.entities.values().map(|v| match &v.entity {
            MetaEntity::Root(v) => v.root_epoch,
            MetaEntity::RootReservation(v) => v.root_epoch,
            MetaEntity::RootGrant(v) => v.root_epoch,
            MetaEntity::RootCommand(v) => v.root_epoch,
            MetaEntity::RootCommandAck(v) => v.root_epoch,
            _ => 0,
        });
        let outcomes = self.requests.values().map(|v| match &v.result {
            OperationResult::RootRecord(v) => v.root_epoch,
            OperationResult::RootReservation(v) => v.root_epoch,
            OperationResult::RootGrant(v) => v.root_epoch,
            OperationResult::RootCommand(v) => v.root_epoch,
            OperationResult::RootCommandAck(v) => v.root_epoch,
            _ => 0,
        });
        entities.chain(outcomes).max().unwrap_or(0)
    }

    pub(in crate::meta::store) fn read(&self, read: MetaRead) -> Result<MetaSnapshot> {
        #[cfg(test)]
        if let Some(pause) = &self.read_pause {
            pause.block_once();
        }
        if read == MetaRead::RootEpochWatermark {
            return Ok(MetaSnapshot {
                revision: self.revision,
                entity: Some(MetaEntity::RootEpochWatermark(self.root_epoch_watermark())),
                request_outcome: None,
                entities: Vec::new(),
            });
        }
        let entity = self.point_entity(&read);
        let request_outcome = match &read {
            MetaRead::RequestOutcome(key) => self.requests.get(key).cloned(),
            _ => None,
        };
        let (revision, entities) = match &read {
            MetaRead::RootState { root_id } => self.read_root_state(root_id),
            MetaRead::DfsDirectory {
                namespace_id,
                parent_inode_id,
            } => self.read_dfs_directory(namespace_id, parent_inode_id),
            MetaRead::CurrentNodeSessions => self.read_current_node_sessions(),
            MetaRead::DfsPlacements => self.read_dfs_placements(),
            MetaRead::DfsReplicationTasks => self.read_dfs_replication_tasks(),
            MetaRead::RootGrants { root_id } => self.read_root_grants(root_id),
            _ => (
                // A conditional update compares the entity's mod revision,
                // not the store-wide revision of unrelated later writes.
                entity
                    .map(|versioned| versioned.revision)
                    .unwrap_or(self.revision),
                Vec::new(),
            ),
        };
        Ok(MetaSnapshot {
            revision,
            entity: entity.map(|versioned| versioned.entity.clone()),
            request_outcome,
            entities,
        })
    }

    /// The single entity a point read names; `None` for range reads.
    fn point_entity(&self, read: &MetaRead) -> Option<&VersionedEntity> {
        let state = self;
        match read {
            MetaRead::RootEpochWatermark => None,
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
            MetaRead::CurrentNodeSessions | MetaRead::RootState { .. } => None,
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
            MetaRead::RootGrants { .. } => None,
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
                    && current_session_matches(state, holder_node_id, holder_session_id)
                    && root_session_matches(state, root_id, *root_epoch, home_session_id)
                    && !has_pending_root_command(state, root_id, *root_epoch, *access_generation)
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
            MetaRead::DfsLayoutRoot(id) => state.entities.get(&MetaKey::DfsLayoutRoot(id.clone())),
            MetaRead::DfsPlacement(id) => state.entities.get(&MetaKey::DfsPlacement(id.clone())),
            MetaRead::DfsPlacements => None,
            MetaRead::DfsChunk(id) => state.entities.get(&MetaKey::DfsChunk(id.clone())),
            MetaRead::DfsCopy(id) => state.entities.get(&MetaKey::DfsCopy(id.clone())),
            MetaRead::DfsDurabilityPolicy => state.entities.get(&MetaKey::DfsDurabilityPolicy),
            MetaRead::DfsReplicationConfig => state.entities.get(&MetaKey::DfsReplicationConfig),
            MetaRead::DfsReplicationTask(id) => {
                state.entities.get(&MetaKey::DfsReplicationTask(id.clone()))
            }
            MetaRead::DfsReplicationTasks => None,
            MetaRead::DfsWriteLease(id) => state.entities.get(&MetaKey::DfsWriteLease(id.clone())),
            MetaRead::DfsGc => state.entities.get(&MetaKey::DfsGc),
        }
    }

    fn read_root_state(&self, root_id: &str) -> (StoreRevision, Vec<MetaEntity>) {
        let state = self;
        let entities = [
            MetaKey::Root {
                root_id: root_id.to_owned(),
            },
            MetaKey::RootReservation {
                root_id: root_id.to_owned(),
            },
        ]
        .iter()
        .filter_map(|key| state.entities.get(key).map(|v| v.entity.clone()))
        .collect();
        (state.revision, entities)
    }

    fn read_dfs_directory(
        &self,
        namespace_id: &crate::dfs::NamespaceId,
        parent_inode_id: &crate::dfs::InodeId,
    ) -> (StoreRevision, Vec<MetaEntity>) {
        let state = self;
        let mut entries = state
            .entities
            .starting_at(MetaKey::DfsDentry(crate::dfs::DentryKey { namespace_id:namespace_id.clone(), parent_inode_id:parent_inode_id.clone(), name:Vec::new() }))
            .take_while(|(key, _)| matches!(key, MetaKey::DfsDentry(key) if key.namespace_id == *namespace_id && key.parent_inode_id == *parent_inode_id))
            .filter_map(|(_, versioned)| match &versioned.entity {
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

    fn read_current_node_sessions(&self) -> (StoreRevision, Vec<MetaEntity>) {
        let state = self;
        let mut sessions = state
            .entities
            .starting_at(MetaKey::CurrentNodeSession {
                node_id: String::new(),
            })
            .take_while(|(key, _)| matches!(key, MetaKey::CurrentNodeSession { .. }))
            .filter_map(|(key, versioned)| match (key, &versioned.entity) {
                (MetaKey::CurrentNodeSession { .. }, MetaEntity::NodeSession(session)) => Some((
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

    fn read_dfs_placements(&self) -> (StoreRevision, Vec<MetaEntity>) {
        let state = self;
        let mut placements = state
            .entities
            .starting_at(MetaKey::DfsPlacement(crate::dfs::ChunkId(String::new())))
            .take_while(|(key, _)| matches!(key, MetaKey::DfsPlacement(_)))
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

    fn read_dfs_replication_tasks(&self) -> (StoreRevision, Vec<MetaEntity>) {
        let state = self;
        let mut tasks = state
            .entities
            .starting_at(MetaKey::DfsReplicationTask(crate::dfs::ReplicationTaskId(
                String::new(),
            )))
            .take_while(|(key, _)| matches!(key, MetaKey::DfsReplicationTask(_)))
            .filter_map(|(key, versioned)| match (key, &versioned.entity) {
                (MetaKey::DfsReplicationTask(id), MetaEntity::DfsReplicationTask(_)) => {
                    Some((id.clone(), versioned.clone()))
                }
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

    fn read_root_grants(&self, root_id: &str) -> (StoreRevision, Vec<MetaEntity>) {
        let state = self;
        let mut grants = state
            .entities
            .starting_at(MetaKey::RootGrant { root_id:root_id.to_owned(),holder_node_id:String::new(),holder_session_id:String::new() })
            .take_while(|(key, _)| matches!(key, MetaKey::RootGrant { root_id:key_root, .. } if key_root == root_id))
            .filter_map(|(key, versioned)| match (key, &versioned.entity) {
                (
                    MetaKey::RootGrant {
                        root_id: key_root, ..
                    },
                    MetaEntity::RootGrant(grant),
                ) if key_root == root_id => Some((grant.holder_node_id.clone(), versioned.clone())),
                _ => None,
            })
            .collect::<Vec<_>>();
        grants.sort_by(|left, right| left.0.cmp(&right.0));
        let revision = grants
            .iter()
            .fold(state.revision, |max, (_, entity)| max.max(entity.revision));
        (
            revision,
            grants
                .into_iter()
                .map(|(_, versioned)| versioned.entity)
                .collect(),
        )
    }

    pub(in crate::meta::store) fn watch(
        &self,
        after_revision: StoreRevision,
        limit: usize,
    ) -> Result<WatchBatch> {
        let state = self;
        if after_revision < state.compacted_to {
            return Ok(WatchBatch::Compacted {
                requested_after: after_revision,
                compacted_to: state.compacted_to,
                recovery: super::super::RecoveryCursor {
                    resume_after: state.revision,
                    reason: super::super::RecoveryReason::WatchCompacted,
                },
            });
        }
        // Select against owned bytes before cloning. Binary-search the
        // retained COW vector instead of visiting its old prefix on every poll.
        let start = state.events.after(after_revision);
        let mut end = start;
        let mut bytes = super::super::WATCH_RESPONSE_BASE_BYTES;
        let mut complete_end = start;
        let mut complete_bytes = bytes;
        let mut last_revision = None;
        for index in start..state.events.len() {
            let event = state.events.get(index);
            if last_revision != Some(event.revision) {
                complete_end = index;
                complete_bytes = bytes;
                if limit > 0 && index - start >= limit {
                    break;
                }
            }
            let next_bytes = bytes
                .checked_add(super::super::history::event_charge(event)?)
                .ok_or_else(|| super::super::journal::full("watch response byte overflow"))?;
            if index - start >= super::super::WATCH_RESPONSE_MAX_EVENTS
                || next_bytes > super::super::WATCH_RESPONSE_MAX_BYTES
            {
                if complete_end == start {
                    return Err(super::super::journal::full(
                        "one watch revision exceeds response limits; recover by scanning authority",
                    ));
                }
                // None of the over-budget revision may leak into the reply.
                end = complete_end;
                bytes = complete_bytes;
                break;
            }
            bytes = next_bytes;
            end = index + 1;
            last_revision = Some(event.revision);
        }
        let count = end - start;
        let mut events = Vec::new();
        events.try_reserve_exact(count).map_err(|_| {
            afs_error::Error::coded(afs_error::IO_OUT_OF_MEMORY, "watch response allocation")
        })?;
        let spare = events
            .capacity()
            .saturating_sub(count)
            .checked_mul(std::mem::size_of::<WatchEvent>())
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| super::super::journal::full("watch response capacity overflow"))?;
        if bytes
            .checked_add(spare)
            .is_none_or(|n| n > super::super::WATCH_RESPONSE_MAX_BYTES)
        {
            return Err(super::super::journal::full(
                "watch response allocation exceeds byte bound",
            ));
        }
        for index in start..end {
            events.push(state.events.get(index).clone());
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
    }

    pub(in crate::meta::store) fn scan_roots_for_recovery(
        &self,
        home_node_id: &str,
    ) -> Result<RecoveryScan> {
        let state = self;
        let roots = state
            .entities
            .values()
            .filter_map(|versioned| match &versioned.entity {
                MetaEntity::Root(root) if root.home_node_id == home_node_id => Some(root.clone()),
                _ => None,
            })
            .collect();
        Ok(RecoveryScan {
            revision: state.revision,
            home_node_id: home_node_id.to_owned(),
            roots,
        })
    }

    pub(in crate::meta::store) fn scan_owner_roots_for_recovery(
        &self,
        home_node_id: &str,
    ) -> Result<OwnerRootScan> {
        let state = self;
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
    }
}
