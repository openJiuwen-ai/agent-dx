//! Checkpoint snapshot encoding and decoding of State.

use super::*;

impl State {
    #[cfg(test)]
    pub(in crate::meta::store) fn snapshot_bytes_with_batch(
        &self,
        batch_id: u64,
    ) -> Result<Vec<u8>> {
        let span = tracing::info_span!(
            "meta.store.snapshot_encode",
            store.batch_id = batch_id,
            store.revision = tracing::field::Empty,
            store.entities = tracing::field::Empty,
            store.requests = tracing::field::Empty,
            store.events = tracing::field::Empty,
            store.node_epochs = tracing::field::Empty,
            store.snapshot_bytes = tracing::field::Empty,
            result = tracing::field::Empty,
        );
        let _entered = span.enter();
        let state = self;
        span.record("store.revision", state.revision.0);
        span.record("store.entities", state.entities.len() as u64);
        span.record("store.requests", state.requests.len() as u64);
        span.record("store.events", state.events.len() as u64);
        span.record("store.node_epochs", state.node_epochs.len() as u64);
        let clone_span = tracing::info_span!(
            "meta.store.snapshot_clone_sort",
            store.batch_id = batch_id,
            store.entities = state.entities.len() as u64,
            store.requests = state.requests.len() as u64,
            store.events = state.events.len() as u64,
            store.node_epochs = state.node_epochs.len() as u64,
        );
        let (entities, requests, events, node_epochs) = {
            let _entered = clone_span.enter();
            (
                state
                    .entities
                    .iter()
                    .map(|(key, entity)| (key.clone(), entity.clone()))
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .collect(),
                state
                    .requests
                    .iter()
                    .map(|(key, outcome)| (key.clone(), outcome.clone()))
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .collect(),
                state.events.iter().cloned().collect(),
                state
                    .node_epochs
                    .iter()
                    .map(|(node, epoch)| (node.clone(), *epoch))
                    .collect::<BTreeMap<_, _>>()
                    .into_iter()
                    .collect(),
            )
        };
        let snapshot = PersistedState {
            schema_version: 1,
            revision: state.revision,
            entities,
            requests,
            request_completion: state
                .request_completion
                .iter()
                .map(|(key, completion)| (key.clone(), completion.clone()))
                .collect(),
            compacted_to: state.compacted_to,
            events_removed: state.events_removed,
            event_completion: state
                .event_completion
                .iter()
                .map(|(revision, time)| (*revision, *time))
                .collect(),
            events,
            node_epochs,
        };
        let bytes = tracing::info_span!("meta.store.snapshot_json", store.batch_id = batch_id)
            .in_scope(|| serde_json::to_vec(&snapshot))
            .map_err(|error| invalid_contract(format!("encode store snapshot: {error}")))?;
        span.record("store.snapshot_bytes", bytes.len() as u64);
        span.record("result", "ok");
        Ok(bytes)
    }

    #[cfg(test)]
    pub(in crate::meta::store) fn from_snapshot(bytes: &[u8]) -> Result<Self> {
        let snapshot: PersistedState = serde_json::from_slice(bytes)
            .map_err(|error| invalid_contract(format!("decode store snapshot: {error}")))?;
        if snapshot.schema_version != 1 {
            return Err(invalid_contract("unsupported store snapshot schema"));
        }
        let mut state = State {
            revision: snapshot.revision,
            compacted_to: snapshot.compacted_to,
            events_removed: snapshot.events_removed,
            events: snapshot.events.into_iter().collect(),
            ..State::default()
        };
        for (key, versioned) in snapshot.entities {
            if entity_key(&versioned.entity) != key
                && !matches!((&key, &versioned.entity),
                    (MetaKey::CurrentNodeSession { node_id }, MetaEntity::NodeSession(session))
                        if node_id == &session.node_id)
            {
                return Err(invalid_contract("store entity key does not match value"));
            }
            if versioned.revision > state.revision
                || state.entities.insert(key, versioned).is_some()
            {
                return Err(invalid_contract(
                    "invalid store entity revision or duplicate key",
                ));
            }
        }
        for (key, outcome) in snapshot.requests {
            if key != outcome.request || state.requests.insert(key, outcome).is_some() {
                return Err(invalid_contract("invalid store request outcome"));
            }
        }
        if state.compacted_to > state.revision {
            return Err(invalid_contract("compaction cursor exceeds state revision"));
        }
        for (key, completion) in snapshot.request_completion {
            if !state.requests.contains_key(&key)
                || completion.revision > state.revision
                || state.request_completion.insert(key, completion).is_some()
            {
                return Err(invalid_contract("invalid request completion metadata"));
            }
        }
        for (revision, time) in snapshot.event_completion {
            if revision <= state.compacted_to
                || revision > state.revision
                || !state.events.iter().any(|event| event.revision == revision)
                || state.event_completion.insert(revision, time).is_some()
            {
                return Err(invalid_contract(
                    "invalid duplicate/orphan event completion metadata",
                ));
            }
        }
        if state
            .events_removed
            .checked_add(state.events.len() as u64)
            .is_none()
            || (state.compacted_to == StoreRevision::ZERO && state.events_removed != 0)
        {
            return Err(invalid_contract("invalid persisted event ordinal base"));
        }
        for (node, epoch) in snapshot.node_epochs {
            if state.node_epochs.insert(node, epoch).is_some() {
                return Err(invalid_contract("duplicate store node epoch"));
            }
        }
        if state
            .events
            .iter()
            .any(|event| event.revision > state.revision)
        {
            return Err(invalid_contract("store event exceeds snapshot revision"));
        }
        state.rebuild_dentry_parents();
        Ok(state)
    }
}
