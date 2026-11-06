//! Validated transaction and node registration application, and authority index maintenance.

use super::*;
use crate::dfs::{Dentry, DentryKey, InodeId, NamespaceId};

impl State {
    pub(in crate::meta::store) fn apply_txn(&mut self, txn: MetaTxn) -> Result<TxnOutcome> {
        txn.validate()?;
        self.apply_validated(txn)
    }

    pub(in crate::meta::store) fn register_node_session(
        &mut self,
        request: RequestKey,
        lease: NodeSessionLease,
    ) -> Result<TxnOutcome> {
        if lease.node_id.is_empty() || lease.session_id.is_empty() || lease.lease_ttl.is_zero() {
            return Err(invalid_contract(
                "node_id, session_id and positive lease_ttl are required",
            ));
        }
        self.register_validated(request, lease)
    }

    pub(super) fn apply_validated(&mut self, txn: MetaTxn) -> Result<TxnOutcome> {
        let state = self;
        if let Some(existing) = state.requests.get(&txn.request).cloned() {
            return Ok(TxnOutcome::ConditionFailed {
                revision: state.revision,
                existing_outcome: Some(existing),
            });
        }
        if !txn
            .conditions
            .iter()
            .all(|condition| condition_matches(state, condition))
        {
            super::super::profile::namespace_conflict(state, &txn);
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
        state.history_indexed = false;
        state.revision = revision;
        let mut event_index = 0;
        super::super::profile::namespace_applied(&txn, revision);
        for mutation in txn.mutations {
            match mutation {
                TxnMutation::Put(entity) => {
                    let key = entity_key(&entity);
                    state.put_authority(
                        key.clone(),
                        VersionedEntity {
                            revision,
                            entity: entity.clone(),
                        },
                    )?;
                    state.events.push(WatchEvent {
                        revision,
                        event_index,
                        key,
                        change: WatchChange::Put(entity),
                    });
                    event_index += 1;
                }
                TxnMutation::Delete(key) => {
                    state.delete_authority(&key)?;
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
    }

    pub(super) fn register_validated(
        &mut self,
        request: RequestKey,
        lease: NodeSessionLease,
    ) -> Result<TxnOutcome> {
        let state = self;
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
        state
            .next_node_event_index()?
            .checked_add(1)
            .ok_or_else(|| invalid_contract("node event index overflow"))?;
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
            });
        let lease_epoch = match lease_epoch {
            Some(epoch) => epoch,
            None => {
                let next = state
                    .node_epochs
                    .get(&lease.node_id)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(1);
                state.set_epoch(lease.node_id.clone(), next)?;
                next
            }
        };
        let ttl_ms = u64::try_from(lease.lease_ttl.as_millis()).unwrap_or(u64::MAX);
        let session = NodeSession {
            node_id: lease.node_id.clone(),
            session_id: lease.session_id.clone(),
            grpc_addr: lease.grpc_addr,
            data_addr: lease.data_addr,
            rest_addr: lease.rest_addr,
            ownerfs_enabled: lease.ownerfs_enabled,
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
        state.history_indexed = false;
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
            state.put_authority(
                key.clone(),
                VersionedEntity {
                    revision,
                    entity: entity.clone(),
                },
            )?;
            let event_index = state.next_node_event_index()?;
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
    }
}

impl State {
    pub(in crate::meta::store) fn command_index_charge(&self) -> Result<u64> {
        self.command_history_bytes
            .checked_add(if self.command_history.is_empty() {
                0
            } else {
                4096
            })
            .ok_or_else(|| invalid_contract("command index root charge overflow"))
    }

    pub(in crate::meta::store) fn pin_charge(&self) -> Result<u64> {
        if !self.authority_indexed || !self.history_indexed {
            return Err(invalid_contract("Meta read capacity index is not ready"));
        }
        let command_bytes = self.command_index_charge()?;
        self.history
            .stats()
            .total_bytes()
            .and_then(|bytes| bytes.checked_add(self.authority_bytes))
            .and_then(|bytes| bytes.checked_add(command_bytes))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() as u64))
            .ok_or_else(|| invalid_contract("Meta pinned state byte charge overflow"))
    }

    pub(super) fn ensure_authority_index(&mut self) -> Result<()> {
        if self.authority_indexed {
            return Ok(());
        }
        let mut bytes = 0u64;
        for (key, value) in self.entities.iter() {
            bytes = bytes
                .checked_add(super::super::history::authority_charge(key, &value.entity)?)
                .ok_or_else(|| invalid_contract("Meta authority byte charge overflow"))?;
        }
        for (node, _) in &self.node_epochs {
            bytes = bytes
                .checked_add(super::super::history::epoch_charge(node)?)
                .ok_or_else(|| invalid_contract("Meta epoch byte charge overflow"))?;
        }
        self.authority_bytes = bytes;
        self.authority_indexed = true;
        Ok(())
    }

    pub(super) fn put_authority(&mut self, key: MetaKey, value: VersionedEntity) -> Result<()> {
        if self.authority_indexed {
            let old = self
                .entities
                .get(&key)
                .map(|old| super::super::history::authority_charge(&key, &old.entity))
                .transpose()?
                .unwrap_or(0);
            let new = super::super::history::authority_charge(&key, &value.entity)?;
            self.authority_bytes = self
                .authority_bytes
                .checked_sub(old)
                .and_then(|bytes| bytes.checked_add(new))
                .ok_or_else(|| invalid_contract("Meta authority replacement charge overflow"))?;
        }
        if let Some(old) = self.entities.get(&key) {
            let old = old.entity.clone();
            self.unindex_dentry(&old);
        }
        self.index_dentry(&value.entity);
        self.entities.insert(key, value);
        Ok(())
    }

    fn index_dentry(&mut self, entity: &MetaEntity) {
        if let MetaEntity::DfsDentry(dentry) = entity {
            self.dentry_parents.insert(dentry_parent_entry(dentry));
        }
    }

    fn unindex_dentry(&mut self, entity: &MetaEntity) {
        if let MetaEntity::DfsDentry(dentry) = entity {
            self.dentry_parents.remove(&dentry_parent_entry(dentry));
        }
    }

    /// Rebuilds the derived dentry index after loading entities from an image.
    pub(in crate::meta::store) fn rebuild_dentry_parents(&mut self) {
        self.dentry_parents = self
            .entities
            .values()
            .filter_map(|versioned| match &versioned.entity {
                MetaEntity::DfsDentry(dentry) => Some(dentry_parent_entry(dentry)),
                _ => None,
            })
            .collect();
    }

    /// The dentry naming `child`: its only one for a directory.
    pub(in crate::meta::store) fn dentry_of_child(
        &self,
        namespace_id: &NamespaceId,
        child: &InodeId,
    ) -> Option<&VersionedEntity> {
        let first = (
            namespace_id.clone(),
            child.clone(),
            InodeId(String::new()),
            Vec::new(),
        );
        let (namespace, found_child, parent, name) = self.dentry_parents.range(first..).next()?;
        if namespace != namespace_id || found_child != child {
            return None;
        }
        self.entities.get(&MetaKey::DfsDentry(DentryKey {
            namespace_id: namespace.clone(),
            parent_inode_id: parent.clone(),
            name: name.clone(),
        }))
    }

    pub(super) fn delete_authority(&mut self, key: &MetaKey) -> Result<()> {
        if self.authority_indexed
            && let Some(old) = self.entities.get(key)
        {
            self.authority_bytes = self
                .authority_bytes
                .checked_sub(super::super::history::authority_charge(key, &old.entity)?)
                .ok_or_else(|| invalid_contract("Meta authority deletion charge underflow"))?;
        }
        if let Some(old) = self.entities.get(key) {
            let old = old.entity.clone();
            self.unindex_dentry(&old);
        }
        self.entities.remove(key);
        Ok(())
    }

    pub(super) fn set_epoch(&mut self, node: String, epoch: u64) -> Result<()> {
        if self.authority_indexed && !self.node_epochs.contains_key(&node) {
            self.authority_bytes = self
                .authority_bytes
                .checked_add(super::super::history::epoch_charge(&node)?)
                .ok_or_else(|| invalid_contract("Meta epoch replacement charge overflow"))?;
        }
        self.node_epochs.insert(node, epoch);
        Ok(())
    }
}

fn dentry_parent_entry(dentry: &Dentry) -> (NamespaceId, InodeId, InodeId, Vec<u8>) {
    (
        dentry.key.namespace_id.clone(),
        dentry.inode_id.clone(),
        dentry.key.parent_inode_id.clone(),
        dentry.key.name.clone(),
    )
}

#[cfg(test)]
mod dentry_parent_tests {
    use super::*;

    fn dentry(parent: &str, name: &str, child: &str) -> MetaEntity {
        MetaEntity::DfsDentry(Dentry {
            key: DentryKey {
                namespace_id: NamespaceId::new("n"),
                parent_inode_id: InodeId::new(parent),
                name: name.as_bytes().to_vec(),
            },
            inode_id: InodeId::new(child),
        })
    }

    fn parent_of(state: &State, child: &str) -> Option<String> {
        match state.read(MetaRead::DfsDirectoryParent {
            namespace_id: NamespaceId::new("n"),
            child_inode_id: InodeId::new(child),
        }) {
            Ok(MetaSnapshot {
                entity: Some(MetaEntity::DfsDentry(dentry)),
                ..
            }) => Some(dentry.key.parent_inode_id.0),
            _ => None,
        }
    }

    fn put(state: &mut State, entity: MetaEntity) {
        state
            .put_authority(
                entity_key(&entity),
                VersionedEntity {
                    revision: StoreRevision(1),
                    entity,
                },
            )
            .unwrap();
    }

    #[test]
    fn the_dentry_index_follows_changes_and_is_rebuilt_on_recovery() {
        let mut state = State {
            revision: StoreRevision(1),
            ..State::default()
        };
        assert_eq!(parent_of(&state, "dir"), None);
        put(&mut state, dentry("root", "dir", "dir"));
        assert_eq!(parent_of(&state, "dir").as_deref(), Some("root"));

        // Same key, new child: the old child leaves the index.
        put(&mut state, dentry("root", "dir", "dir2"));
        assert_eq!(parent_of(&state, "dir"), None);
        assert_eq!(parent_of(&state, "dir2").as_deref(), Some("root"));

        // Rename moves a directory: put at the new key, delete the old one.
        put(&mut state, dentry("other", "moved", "dir2"));
        state
            .delete_authority(&entity_key(&dentry("root", "dir", "dir2")))
            .unwrap();
        assert_eq!(parent_of(&state, "dir2").as_deref(), Some("other"));

        // Hard links: removing one link keeps the other.
        put(&mut state, dentry("a", "l1", "file"));
        put(&mut state, dentry("b", "l2", "file"));
        state
            .delete_authority(&entity_key(&dentry("a", "l1", "file")))
            .unwrap();
        assert_eq!(parent_of(&state, "file").as_deref(), Some("b"));

        let bytes = state.snapshot_bytes_with_batch(0).unwrap();
        let recovered = State::from_snapshot(&bytes).unwrap();
        assert_eq!(recovered.dentry_parents, state.dentry_parents);
        let mut rebuilt = state.clone();
        rebuilt.dentry_parents = im::OrdSet::new();
        rebuilt.rebuild_dentry_parents();
        assert_eq!(rebuilt.dentry_parents, state.dentry_parents);
        let image = crate::meta::store::log::StateImage::encode(&state, 1024 * 1024).unwrap();
        let restored = crate::meta::store::log::StateImage::decode(&image, 1024 * 1024).unwrap();
        assert_eq!(restored.dentry_parents, state.dentry_parents);
    }
}
