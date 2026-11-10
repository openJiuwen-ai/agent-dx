//! Canonical record preparation and replay decisions.

use super::super::log::{
    DecidedFacts, LayoutAppendBase, Record, RecordDecision, valid_layout_suffix,
};
use super::*;
use crate::dfs::{FileVersion, InodeRecord, LayoutRoot};

/// Identify the single layout, its new version, and the corresponding head update.
fn append_entities(events: &[WatchEvent]) -> Option<(&LayoutRoot, &FileVersion, &InodeRecord)> {
    let mut layout = None;
    let mut version = None;
    let mut inode = None;
    for event in events {
        match &event.change {
            WatchChange::Put(MetaEntity::DfsLayoutRoot(value)) if layout.is_none() => {
                layout = Some(value.as_ref());
            }
            WatchChange::Put(MetaEntity::DfsFileVersion(value)) if version.is_none() => {
                version = Some(value);
            }
            WatchChange::Put(MetaEntity::DfsInode(value)) if inode.is_none() => {
                inode = Some(value);
            }
            WatchChange::Put(
                MetaEntity::DfsLayoutRoot(_)
                | MetaEntity::DfsFileVersion(_)
                | MetaEntity::DfsInode(_),
            )
            | WatchChange::Delete => return None,
            _ => {}
        }
    }
    let (layout, version, inode) = (layout?, version?, inode?);
    (version.layout_root == layout.id
        && version.length == layout.file_length
        && version.inode_id == inode.inode_id
        && inode.head_version.as_ref() == Some(&version.id))
    .then_some((layout, version, inode))
}

// Prepare privately; only Store publishes after its selected log acknowledges.
impl State {
    pub(in crate::meta::store) fn replay_record(
        &self,
        record: &super::super::log::Record,
    ) -> Result<Self> {
        record.validate()?;
        match record {
            super::super::log::Record::Decision { decision, history } => {
                let next = self.replay_decision(decision)?;
                match history {
                    Some(history) => next.apply_history(history),
                    None => Ok(next),
                }
            }
            Record::AppendLayout {
                decision,
                history,
                base,
            } => {
                let mut facts = decision.facts()?;
                let layout = self.expand_layout_append(&facts.events, base)?;
                for event in &mut facts.events {
                    if matches!(event.change, WatchChange::Put(MetaEntity::DfsLayoutRoot(_))) {
                        event.change = WatchChange::Put(MetaEntity::DfsLayoutRoot(layout.clone()));
                    }
                }
                let next = self.replay_facts(facts)?;
                match history {
                    Some(history) => next.apply_history(history),
                    None => Ok(next),
                }
            }
            Record::History(history) => self.apply_history(history),
        }
    }

    fn predecessor_layout(&self, version: &FileVersion) -> Option<&LayoutRoot> {
        let parent = version.parent_version.as_ref()?;
        let MetaEntity::DfsInode(inode) = &self
            .entities
            .get(&MetaKey::DfsInode(version.inode_id.clone()))?
            .entity
        else {
            return None;
        };
        if inode.head_version.as_ref() != Some(parent)
            || self
                .entities
                .get(&MetaKey::DfsFileVersion(version.id.clone()))
                .is_some()
            || self
                .entities
                .get(&MetaKey::DfsLayoutRoot(version.layout_root.clone()))
                .is_some()
        {
            return None;
        }
        let MetaEntity::DfsFileVersion(previous) = &self
            .entities
            .get(&MetaKey::DfsFileVersion(parent.clone()))?
            .entity
        else {
            return None;
        };
        let MetaEntity::DfsLayoutRoot(layout) = &self
            .entities
            .get(&MetaKey::DfsLayoutRoot(previous.layout_root.clone()))?
            .entity
        else {
            return None;
        };
        (previous.inode_id == version.inode_id
            && previous.length == layout.file_length
            && previous.layout_root == layout.id)
            .then_some(layout)
    }

    fn layout_append_base(&self, events: &[WatchEvent]) -> Option<LayoutAppendBase> {
        let (layout, version, _) = append_entities(events)?;
        let base = self.predecessor_layout(version)?;
        let suffix = layout
            .inline_extents
            .strip_prefix(base.inline_extents.as_slice())?;
        valid_layout_suffix(base.file_length, layout, suffix).then(|| LayoutAppendBase {
            layout: base.id.clone(),
            length: base.file_length,
        })
    }

    fn expand_layout_append(
        &self,
        events: &[WatchEvent],
        base: &LayoutAppendBase,
    ) -> Result<Arc<LayoutRoot>> {
        let (suffix, version, _) = append_entities(events).ok_or_else(|| {
            invalid_contract("append replay requires one layout, version and inode")
        })?;
        let previous = self
            .predecessor_layout(version)
            .filter(|layout| layout.id == base.layout && layout.file_length == base.length)
            .ok_or_else(|| invalid_contract("append replay predecessor mismatch"))?;
        let mut extents = Vec::new();
        extents
            .try_reserve_exact(
                previous
                    .inline_extents
                    .len()
                    .saturating_add(suffix.inline_extents.len()),
            )
            .map_err(|_| {
                afs_error::Error::coded(afs_error::IO_OUT_OF_MEMORY, "allocate replay layout")
            })?;
        extents.extend_from_slice(&previous.inline_extents);
        extents.extend_from_slice(&suffix.inline_extents);
        Ok(Arc::new(LayoutRoot {
            id: suffix.id.clone(),
            file_length: suffix.file_length,
            inline_extents: extents,
        }))
    }

    pub(in crate::meta::store) fn prepare_txn_record(
        &self,
        txn: MetaTxn,
        completed_at: u64,
        byte_limit: usize,
    ) -> Result<(Self, TxnOutcome, Option<super::super::log::RecordDecision>)> {
        let (state, outcome, record, _) = self.prepare_txn(txn, completed_at, byte_limit, false)?;
        Ok((state, outcome, record))
    }

    pub(in crate::meta::store) fn prepare_txn(
        &self,
        txn: MetaTxn,
        completed_at: u64,
        byte_limit: usize,
        append_enabled: bool,
    ) -> Result<(
        Self,
        TxnOutcome,
        Option<RecordDecision>,
        Option<LayoutAppendBase>,
    )> {
        use super::super::log::DecisionInput;
        let mut base = None;
        let request = txn.request.clone();
        let operation = txn.operation;
        let outcomes = txn
            .mutations
            .iter()
            .filter_map(|mutation| match mutation {
                TxnMutation::RecordRequestOutcome(outcome) => Some(outcome.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut prepared = self.clone();
        prepared.ensure_history_index()?;
        let outcome = prepared.apply_txn(txn)?;
        let record = if let TxnOutcome::Committed {
            revision,
            outcome: reply,
        } = &outcome
        {
            let events = (self.events.len()..prepared.events.len())
                .map(|index| prepared.events.0[index].as_ref().clone())
                .collect::<Vec<_>>();
            if append_enabled && operation == StoreOperation::DfsCommitFileVersion {
                base = self.layout_append_base(&events);
            }
            let record = RecordDecision::new_with_append(
                DecisionInput {
                    request,
                    operation,
                    revision_before: self.revision,
                    revision_after: *revision,
                    completed_at_unix_ms: completed_at,
                    events: &events,
                    outcomes: &outcomes,
                    reply,
                    node_epochs: Vec::new(),
                },
                byte_limit,
                base.as_ref(),
            )?;
            for outcome in &outcomes {
                prepared.request_completion.insert(
                    outcome.request.clone(),
                    RequestCompletion {
                        completed_at_unix_ms: completed_at,
                        revision: *revision,
                    },
                );
            }
            if !events.is_empty() {
                prepared.event_completion.insert(*revision, completed_at);
            }
            prepared.account_history_changes(
                self,
                outcomes.iter().map(|outcome| outcome.request.clone()),
                self.events.len(),
                completed_at,
            )?;
            Some(record)
        } else {
            None
        };
        Ok((prepared, outcome, record, base))
    }

    pub(in crate::meta::store) fn prepare_node_record(
        &self,
        request: RequestKey,
        lease: NodeSessionLease,
        completed_at: u64,
        byte_limit: usize,
    ) -> Result<(Self, TxnOutcome, Option<super::super::log::RecordDecision>)> {
        use super::super::log::{DecisionInput, RecordDecision};
        let node_id = lease.node_id.clone();
        let mut prepared = self.clone();
        prepared.ensure_history_index()?;
        let outcome = prepared.register_node_session(request.clone(), lease)?;
        let record = if let TxnOutcome::Committed {
            revision,
            outcome: reply,
        } = &outcome
        {
            let events = (self.events.len()..prepared.events.len())
                .map(|index| prepared.events.0[index].as_ref().clone())
                .collect::<Vec<_>>();
            let epoch = prepared.node_epochs.get(&node_id).copied();
            let epochs = if epoch != self.node_epochs.get(&node_id).copied() {
                vec![(
                    node_id,
                    epoch.ok_or_else(|| invalid_contract("missing decided node epoch"))?,
                )]
            } else {
                Vec::new()
            };
            let record = RecordDecision::new(
                DecisionInput {
                    request: request.clone(),
                    operation: StoreOperation::RegisterNode,
                    revision_before: self.revision,
                    revision_after: *revision,
                    completed_at_unix_ms: completed_at,
                    events: &events,
                    outcomes: std::slice::from_ref(reply),
                    reply,
                    node_epochs: epochs,
                },
                byte_limit,
            )?;
            // Do not retain spare capacity from an admission-only input key.
            // The stored key copies are charged from the decided outcome.
            prepared.request_completion.insert(
                reply.request.clone(),
                RequestCompletion {
                    completed_at_unix_ms: completed_at,
                    revision: *revision,
                },
            );
            if !events.is_empty() {
                prepared.event_completion.insert(*revision, completed_at);
            }
            prepared.account_history_changes(
                self,
                std::iter::once(reply.request.clone()),
                self.events.len(),
                completed_at,
            )?;
            Some(record)
        } else {
            None
        };
        Ok((prepared, outcome, record))
    }

    pub(in crate::meta::store) fn replay_decision(
        &self,
        record: &super::super::log::RecordDecision,
    ) -> Result<Self> {
        self.replay_facts(record.facts()?)
    }

    fn replay_facts(&self, facts: DecidedFacts) -> Result<Self> {
        if facts.revision_before != self.revision {
            return Err(invalid_contract(
                "canonical replay revision does not follow current state",
            ));
        }
        // Validate before changing even the private recovery state. No original
        // business condition, wall clock or external request is re-executed.
        if let Some(first) = facts.events.first() {
            // RegisterNode is also a legal generic transaction label (for
            // example, deleting a departed writer's current-session pointer).
            // Generic transactions start each revision at zero; the dedicated
            // registration path retains its historical cumulative index.
            // The decided record already stores the exact event base. Do not
            // infer the original entry point from the shared business label.
            let valid = first.event_index == 0
                || (facts.operation == StoreOperation::RegisterNode
                    && first.event_index == self.next_node_event_index()?);
            if !valid {
                return Err(invalid_contract("canonical replay event base mismatch"));
            }
        }
        if self.requests.contains_key(&facts.request) {
            return Err(invalid_contract(
                "canonical replay replaces a retained request",
            ));
        }
        for (node, epoch) in &facts.node_epochs {
            if self.node_epochs.get(node).is_some_and(|old| epoch < old) {
                return Err(invalid_contract("canonical replay node epoch regresses"));
            }
        }
        let mut prepared = self.clone();
        prepared.ensure_history_index()?;
        let history_keys = facts
            .outcomes
            .iter()
            .map(|outcome| outcome.request.clone())
            .collect::<Vec<_>>();
        if !facts.events.is_empty() {
            prepared
                .event_completion
                .insert(facts.revision_after, facts.completed_at_unix_ms);
        }
        for event in facts.events {
            match &event.change {
                WatchChange::Put(entity) => {
                    prepared.put_authority(
                        event.key.clone(),
                        VersionedEntity {
                            revision: event.revision,
                            entity: entity.clone(),
                        },
                    )?;
                }
                WatchChange::Delete => {
                    prepared.delete_authority(&event.key)?;
                }
            }
            prepared.events.push(event);
        }
        for outcome in facts.outcomes {
            prepared.request_completion.insert(
                outcome.request.clone(),
                RequestCompletion {
                    completed_at_unix_ms: facts.completed_at_unix_ms,
                    revision: facts.revision_after,
                },
            );
            prepared.record_request_outcome(outcome);
        }
        for (node, epoch) in facts.node_epochs {
            prepared.set_epoch(node, epoch)?;
        }
        prepared.revision = facts.revision_after;
        prepared.account_history_changes(
            self,
            history_keys.into_iter(),
            self.events.len(),
            facts.completed_at_unix_ms,
        )?;
        Ok(prepared)
    }
}
