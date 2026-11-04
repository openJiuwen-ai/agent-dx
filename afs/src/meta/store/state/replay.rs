//! Canonical record preparation and replay decisions.

use super::*;

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
            super::super::log::Record::History(history) => self.apply_history(history),
        }
    }

    pub(in crate::meta::store) fn prepare_txn_record(
        &self,
        txn: MetaTxn,
        completed_at: u64,
        byte_limit: usize,
    ) -> Result<(Self, TxnOutcome, Option<super::super::log::RecordDecision>)> {
        use super::super::log::{DecisionInput, RecordDecision};
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
            let record = RecordDecision::new(
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
        Ok((prepared, outcome, record))
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
        let facts = record.facts()?;
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
            prepared.requests.insert(outcome.request.clone(), outcome);
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
