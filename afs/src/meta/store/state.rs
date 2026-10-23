//! Immutable published authority and privately mutable batch preparation.
//! Typed MetaKey remains an in-memory business key; it does not become one
//! backend KV per entity. Canonical logs prepare and replay this same state;
//! old JSON encoding is a test-only equivalence reference, never a backend.
#[cfg(test)]
use super::{BackendPersistence, BackendReadiness, MetaFuture, MetaReadView, MetaStore};
use super::{
    MetaEntity, MetaKey, MetaRead, MetaSnapshot, MetaTxn, NodeSession, NodeSessionLease,
    OperationResult, OwnerRootScan, RecoveryScan, RequestKey, RequestOutcome, StoreOperation,
    StoreRevision, TxnMutation, TxnOutcome, WatchBatch, WatchChange, WatchEvent, condition_matches,
    current_session_matches, entity_key, has_pending_root_command, invalid_contract, now_unix_ms,
    root_session_matches,
};
use afs_error::Result;
#[cfg(test)]
use afs_tracing::tracing;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::collections::BTreeMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

/// Persistent tree nodes and immutable payloads are shared independently.
/// Lookup retains the old borrowed-value interface, so callers cannot mutate
/// values belonging to a published or pinned state through an alias.
pub(super) struct SharedMap<K: Ord + Clone, V>(im::OrdMap<K, Arc<V>>);
impl<K: Ord + Clone, V> Default for SharedMap<K, V> {
    fn default() -> Self {
        Self(im::OrdMap::new())
    }
}
impl<K: Ord + Clone, V> Clone for SharedMap<K, V> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<K: Ord + Clone, V> SharedMap<K, V> {
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }
    pub(super) fn get(&self, key: &K) -> Option<&V> {
        self.0.get(key).map(Arc::as_ref)
    }
    pub(super) fn contains_key(&self, key: &K) -> bool {
        self.0.contains_key(key)
    }
    pub(super) fn insert(&mut self, key: K, value: V) -> Option<Arc<V>> {
        self.0.insert(key, Arc::new(value))
    }
    pub(super) fn remove(&mut self, key: &K) -> Option<Arc<V>> {
        self.0.remove(key)
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.0.iter().map(|(k, v)| (k, v.as_ref()))
    }
    pub(super) fn starting_at(&self, key: K) -> impl Iterator<Item = (&K, &V)> {
        self.0
            .range(key..)
            .map(|(key, value)| (key, value.as_ref()))
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &V> {
        self.0.values().map(Arc::as_ref)
    }
}

#[derive(Clone, Default)]
pub(super) struct SharedEvents(im::Vector<Arc<WatchEvent>>);
impl SharedEvents {
    pub(super) fn push(&mut self, event: WatchEvent) {
        self.0.push_back(Arc::new(event));
    }
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }
    fn get(&self, index: usize) -> &WatchEvent {
        self.0[index].as_ref()
    }
    fn after(&self, revision: StoreRevision) -> usize {
        let (mut low, mut high) = (0, self.len());
        while low < high {
            let middle = low + (high - low) / 2;
            if self.get(middle).revision <= revision {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }
    pub(super) fn iter(&self) -> impl Iterator<Item = &WatchEvent> {
        self.0.iter().map(Arc::as_ref)
    }
}
impl Extend<WatchEvent> for SharedEvents {
    fn extend<T: IntoIterator<Item = WatchEvent>>(&mut self, events: T) {
        for event in events {
            self.push(event);
        }
    }
}
impl FromIterator<WatchEvent> for SharedEvents {
    fn from_iter<T: IntoIterator<Item = WatchEvent>>(events: T) -> Self {
        Self(events.into_iter().map(Arc::new).collect())
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct VersionedEntity {
    pub(super) revision: StoreRevision,
    pub(super) entity: MetaEntity,
}

/// Decided completion metadata belongs to the retained outcome, not restart time.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct RequestCompletion {
    pub(super) completed_at_unix_ms: u64,
    pub(super) revision: StoreRevision,
}

/// Only retained outcomes reference command IDs after watch compaction. This is
/// a derived index, rebuilt from canonical outcomes, never another authority.
fn command_result_id(mut result: &OperationResult) -> Option<&str> {
    loop {
        match result {
            OperationResult::DfsNamespace { result: inner, .. } => result = inner,
            OperationResult::RootCommand(command) => return Some(&command.command_id),
            OperationResult::RootCommandAck(ack) => return Some(&ack.command_id),
            _ => return None,
        }
    }
}
fn account_command_result(
    index: &mut im::OrdMap<String, u64>,
    bytes: &mut u64,
    result: &OperationResult,
    add: bool,
) -> Result<()> {
    let Some(id) = command_result_id(result) else {
        return Ok(());
    };
    let count = index.get(id).copied().unwrap_or(0);
    let charge = (id.len() as u64)
        .checked_add(256)
        .ok_or_else(|| invalid_contract("command outcome index charge overflow"))?;
    if add {
        let next = count
            .checked_add(1)
            .ok_or_else(|| invalid_contract("command outcome reference overflow"))?;
        if count == 0 {
            *bytes = bytes
                .checked_add(charge)
                .ok_or_else(|| invalid_contract("command outcome index charge overflow"))?;
        }
        index.insert(id.to_owned(), next);
    } else if count == 1 {
        index.remove(id);
        *bytes = bytes
            .checked_sub(charge)
            .ok_or_else(|| invalid_contract("command outcome index charge underflow"))?;
    } else if count > 1 {
        index.insert(id.to_owned(), count - 1);
    } else {
        return Err(invalid_contract("missing command outcome reference"));
    }
    Ok(())
}

#[derive(Clone, Default)]
pub(super) struct State {
    /// Runtime recovery anchor paired with this immutable published state.
    /// It is framed by the backend, not duplicated in the business image.
    pub(super) log_tip: super::journal::LogTip,
    pub(super) revision: StoreRevision,
    pub(super) entities: SharedMap<MetaKey, VersionedEntity>,
    pub(super) requests: SharedMap<RequestKey, RequestOutcome>,
    pub(super) request_completion: SharedMap<RequestKey, RequestCompletion>,
    pub(super) compacted_to: StoreRevision,
    /// Number of removed events, not a new per-revision index.
    pub(super) events_removed: u64,
    pub(super) event_completion: im::OrdMap<StoreRevision, u64>,
    /// Derived capacity-aware retention index; never serialized as authority.
    pub(super) history: super::history::Index,
    pub(super) history_indexed: bool,
    pub(super) command_history: im::OrdMap<String, u64>,
    pub(super) command_history_bytes: u64,
    /// Derived full authority capacity, incremented only for changed keys.
    pub(super) authority_bytes: u64,
    pub(super) authority_indexed: bool,
    pub(super) events: SharedEvents,
    pub(super) node_epochs: im::OrdMap<String, u64>,
    #[cfg(test)]
    pub(super) read_pause: Option<Arc<ReadPause>>,
}

/// Test-only scheduler gate. No hook, lock or branch is compiled into products.
#[cfg(test)]
pub(super) struct ReadPause {
    once: std::sync::atomic::AtomicBool,
    entered: std::sync::mpsc::SyncSender<()>,
    released: Mutex<bool>,
    wake: std::sync::Condvar,
}
#[cfg(test)]
impl ReadPause {
    pub(super) fn new(entered: std::sync::mpsc::SyncSender<()>) -> Self {
        Self {
            once: std::sync::atomic::AtomicBool::new(true),
            entered,
            released: Mutex::new(false),
            wake: std::sync::Condvar::new(),
        }
    }
    fn block_once(&self) {
        if self.once.swap(false, std::sync::atomic::Ordering::AcqRel) {
            self.entered.send(()).unwrap();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.wake.wait(released).unwrap();
            }
        }
    }
    pub(super) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

/// Private staging state for one Store commit batch.
///
/// It executes conditional transactions and keeps replayable outcomes. This is
/// not the configured memory backend: Store publishes it only after its
/// selected backend has acknowledged the complete snapshot.
#[cfg(test)]
#[derive(Default)]
pub(super) struct StoreState {
    pub(super) state: Mutex<State>,
}

#[cfg(test)]
impl StoreState {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Fork only committed authority for a private Store batch. Nothing in
    /// this fork is visible to RPC readers until the backend acknowledges it.
    pub(super) fn fork(&self) -> Result<Self> {
        let state = self
            .state
            .lock()
            .map_err(|_| invalid_contract("meta store lock poisoned"))?
            .clone();
        Ok(Self {
            state: Mutex::new(state),
        })
    }

    #[cfg(test)]
    pub(super) fn snapshot_bytes(&self) -> Result<Vec<u8>> {
        self.snapshot_bytes_with_batch(0)
    }

    #[cfg(test)]
    pub(super) fn snapshot_bytes_with_batch(&self, batch_id: u64) -> Result<Vec<u8>> {
        self.state
            .lock()
            .map_err(|_| invalid_contract("meta store lock poisoned"))?
            .snapshot_bytes_with_batch(batch_id)
    }
    #[cfg(test)]
    pub(super) fn from_snapshot(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            state: Mutex::new(State::from_snapshot(bytes)?),
        })
    }
}

/// JSON object keys cannot represent structured `MetaKey`, so a versioned
/// snapshot stores maps as pairs. This is a persistence format, not an RPC API.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg(test)]
struct PersistedState {
    schema_version: u32,
    revision: StoreRevision,
    entities: Vec<(MetaKey, VersionedEntity)>,
    requests: Vec<(RequestKey, RequestOutcome)>,
    #[serde(default)]
    request_completion: Vec<(RequestKey, RequestCompletion)>,
    #[serde(default)]
    compacted_to: StoreRevision,
    #[serde(default)]
    events_removed: u64,
    #[serde(default)]
    event_completion: Vec<(StoreRevision, u64)>,
    events: Vec<WatchEvent>,
    node_epochs: Vec<(String, u64)>,
}

#[cfg(test)]
impl MetaStore for StoreState {
    fn health(&self) -> MetaFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn backend_readiness(&self) -> MetaFuture<'_, BackendReadiness> {
        Box::pin(async {
            Ok(BackendReadiness::observed(
                BackendPersistence::Unknown,
                true,
                "in-memory state has no configured backend capability",
            ))
        })
    }

    fn read_view(&self) -> MetaFuture<'_, MetaReadView> {
        Box::pin(async move {
            Ok(MetaReadView::unmanaged(Arc::new(
                self.state
                    .lock()
                    .map_err(|_| invalid_contract("meta store lock poisoned"))?
                    .clone(),
            )))
        })
    }

    fn read(&self, read: MetaRead) -> MetaFuture<'_, MetaSnapshot> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            state.read(read)
        })
    }

    fn compare_and_commit(&self, txn: MetaTxn) -> MetaFuture<'_, TxnOutcome> {
        Box::pin(async move {
            txn.validate()?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            state.apply_validated(txn)
        })
    }

    fn watch(&self, after_revision: StoreRevision, limit: usize) -> MetaFuture<'_, WatchBatch> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            state.watch(after_revision, limit)
        })
    }

    fn register_node_session(
        &self,
        request: RequestKey,
        lease: NodeSessionLease,
    ) -> MetaFuture<'_, TxnOutcome> {
        Box::pin(async move {
            if lease.node_id.is_empty() || lease.session_id.is_empty() || lease.lease_ttl.is_zero()
            {
                return Err(invalid_contract(
                    "node_id, session_id and positive lease_ttl are required",
                ));
            }
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            state.register_validated(request, lease)
        })
    }

    fn scan_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> MetaFuture<'a, RecoveryScan> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            state.scan_roots_for_recovery(home_node_id)
        })
    }

    fn scan_owner_roots_for_recovery<'a>(
        &'a self,
        home_node_id: &'a str,
    ) -> MetaFuture<'a, OwnerRootScan> {
        Box::pin(async move {
            let state = self
                .state
                .lock()
                .map_err(|_| invalid_contract("meta store lock poisoned"))?;
            state.scan_owner_roots_for_recovery(home_node_id)
        })
    }
}

impl State {
    /// First allocation bootstraps old authority once. Subsequent allocations
    /// use the durable singleton; deleted roots/aborted reservations cannot reset it.
    pub(super) fn root_epoch_watermark(&self) -> u64 {
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
    pub(super) fn apply_txn(&mut self, txn: MetaTxn) -> Result<TxnOutcome> {
        txn.validate()?;
        self.apply_validated(txn)
    }
    pub(super) fn register_node_session(
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
    #[cfg(test)]
    pub(super) fn snapshot_bytes_with_batch(&self, batch_id: u64) -> Result<Vec<u8>> {
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
    pub(super) fn from_snapshot(bytes: &[u8]) -> Result<Self> {
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
        Ok(state)
    }
    pub(super) fn read(&self, read: MetaRead) -> Result<MetaSnapshot> {
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
        let state = self;
        let entity = match &read {
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
            MetaRead::CurrentNodeSessions => None,
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
        };
        let request_outcome = match &read {
            MetaRead::RequestOutcome(key) => state.requests.get(key).cloned(),
            _ => None,
        };
        let (revision, entities) = match &read {
            MetaRead::DfsDirectory {
                namespace_id,
                parent_inode_id,
            } => {
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
            MetaRead::CurrentNodeSessions => {
                let mut sessions = state
                    .entities
                    .starting_at(MetaKey::CurrentNodeSession {
                        node_id: String::new(),
                    })
                    .take_while(|(key, _)| matches!(key, MetaKey::CurrentNodeSession { .. }))
                    .filter_map(|(key, versioned)| match (key, &versioned.entity) {
                        (MetaKey::CurrentNodeSession { .. }, MetaEntity::NodeSession(session)) => {
                            Some((
                                session.node_id.clone(),
                                session.session_id.clone(),
                                versioned.clone(),
                            ))
                        }
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
            MetaRead::DfsPlacements => {
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
            MetaRead::DfsReplicationTasks => {
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
            MetaRead::RootGrants { root_id } => {
                let mut grants = state
                    .entities
                    .starting_at(MetaKey::RootGrant { root_id:root_id.clone(),holder_node_id:String::new(),holder_session_id:String::new() })
                    .take_while(|(key, _)| matches!(key, MetaKey::RootGrant { root_id:key_root, .. } if key_root == root_id))
                    .filter_map(|(key, versioned)| match (key, &versioned.entity) {
                        (
                            MetaKey::RootGrant {
                                root_id: key_root, ..
                            },
                            MetaEntity::RootGrant(grant),
                        ) if key_root == root_id => {
                            Some((grant.holder_node_id.clone(), versioned.clone()))
                        }
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
            _ => (
                // A conditional update compares the entity's mod revision,
                // not the store-wide revision of unrelated later writes.
                entity
                    .map(|versioned| versioned.revision)
                    .unwrap_or(state.revision),
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

    fn apply_validated(&mut self, txn: MetaTxn) -> Result<TxnOutcome> {
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
            super::profile::namespace_conflict(state, &txn);
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
        super::profile::namespace_applied(&txn, revision);
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

    pub(super) fn watch(&self, after_revision: StoreRevision, limit: usize) -> Result<WatchBatch> {
        let state = self;
        if after_revision < state.compacted_to {
            return Ok(WatchBatch::Compacted {
                requested_after: after_revision,
                compacted_to: state.compacted_to,
                recovery: super::RecoveryCursor {
                    resume_after: state.revision,
                    reason: super::RecoveryReason::WatchCompacted,
                },
            });
        }
        // Select against owned bytes before cloning. Binary-search the
        // retained COW vector instead of visiting its old prefix on every poll.
        let start = state.events.after(after_revision);
        let mut end = start;
        let mut bytes = super::WATCH_RESPONSE_BASE_BYTES;
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
                .checked_add(super::history::event_charge(event)?)
                .ok_or_else(|| super::journal::full("watch response byte overflow"))?;
            if index - start >= super::WATCH_RESPONSE_MAX_EVENTS
                || next_bytes > super::WATCH_RESPONSE_MAX_BYTES
            {
                if complete_end == start {
                    return Err(super::journal::full(
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
            .ok_or_else(|| super::journal::full("watch response capacity overflow"))?;
        if bytes
            .checked_add(spare)
            .is_none_or(|n| n > super::WATCH_RESPONSE_MAX_BYTES)
        {
            return Err(super::journal::full(
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

    fn register_validated(
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

    pub(super) fn scan_roots_for_recovery(&self, home_node_id: &str) -> Result<RecoveryScan> {
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

    pub(super) fn scan_owner_roots_for_recovery(
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

// Prepare privately; only Store publishes after its selected log acknowledges.
impl State {
    pub(super) fn command_index_charge(&self) -> Result<u64> {
        self.command_history_bytes
            .checked_add(if self.command_history.is_empty() {
                0
            } else {
                4096
            })
            .ok_or_else(|| invalid_contract("command index root charge overflow"))
    }
    pub(super) fn pin_charge(&self) -> Result<u64> {
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
    fn ensure_authority_index(&mut self) -> Result<()> {
        if self.authority_indexed {
            return Ok(());
        }
        let mut bytes = 0u64;
        for (key, value) in self.entities.iter() {
            bytes = bytes
                .checked_add(super::history::authority_charge(key, &value.entity)?)
                .ok_or_else(|| invalid_contract("Meta authority byte charge overflow"))?;
        }
        for (node, _) in &self.node_epochs {
            bytes = bytes
                .checked_add(super::history::epoch_charge(node)?)
                .ok_or_else(|| invalid_contract("Meta epoch byte charge overflow"))?;
        }
        self.authority_bytes = bytes;
        self.authority_indexed = true;
        Ok(())
    }
    fn put_authority(&mut self, key: MetaKey, value: VersionedEntity) -> Result<()> {
        if self.authority_indexed {
            let old = self
                .entities
                .get(&key)
                .map(|old| super::history::authority_charge(&key, &old.entity))
                .transpose()?
                .unwrap_or(0);
            let new = super::history::authority_charge(&key, &value.entity)?;
            self.authority_bytes = self
                .authority_bytes
                .checked_sub(old)
                .and_then(|bytes| bytes.checked_add(new))
                .ok_or_else(|| invalid_contract("Meta authority replacement charge overflow"))?;
        }
        self.entities.insert(key, value);
        Ok(())
    }
    fn delete_authority(&mut self, key: &MetaKey) -> Result<()> {
        if self.authority_indexed
            && let Some(old) = self.entities.get(key)
        {
            self.authority_bytes = self
                .authority_bytes
                .checked_sub(super::history::authority_charge(key, &old.entity)?)
                .ok_or_else(|| invalid_contract("Meta authority deletion charge underflow"))?;
        }
        self.entities.remove(key);
        Ok(())
    }
    fn set_epoch(&mut self, node: String, epoch: u64) -> Result<()> {
        if self.authority_indexed && !self.node_epochs.contains_key(&node) {
            self.authority_bytes = self
                .authority_bytes
                .checked_add(super::history::epoch_charge(&node)?)
                .ok_or_else(|| invalid_contract("Meta epoch replacement charge overflow"))?;
        }
        self.node_epochs.insert(node, epoch);
        Ok(())
    }
    pub(super) fn history_stats(&self) -> super::history::Retained {
        self.history.stats()
    }
    pub(super) fn ensure_history_index(&mut self) -> Result<()> {
        if !self.history_indexed {
            self.rebuild_history_index()?;
        }
        Ok(())
    }
    pub(super) fn rebuild_history_index(&mut self) -> Result<()> {
        use super::history;
        self.ensure_authority_index()?;
        if self.request_completion.len() != self.requests.len() {
            return Err(invalid_contract(
                "history outcome completion coverage mismatch",
            ));
        }
        let mut command_history = im::OrdMap::new();
        let mut command_history_bytes = 0;
        let mut index = history::Index::default();
        for (key, outcome) in self.requests.iter() {
            let completion = self
                .request_completion
                .get(key)
                .ok_or_else(|| invalid_contract("history request completion missing"))?;
            if key != &outcome.request
                || completion.revision == StoreRevision::ZERO
                || completion.revision > self.revision
            {
                return Err(invalid_contract(
                    "history original request identity/revision invalid",
                ));
            }
            account_command_result(
                &mut command_history,
                &mut command_history_bytes,
                &outcome.result,
                true,
            )?;
            index.track_request(
                key.clone(),
                completion.clone(),
                history::request_charge(outcome)?,
            )?;
        }
        let mut events = self.events.iter().peekable();
        let mut previous = self.compacted_to;
        while let Some(event) = events.peek() {
            let revision = event.revision;
            if revision <= previous || revision > self.revision {
                return Err(invalid_contract("history event revision order invalid"));
            }
            let group = std::iter::from_fn(|| events.next_if(|event| event.revision == revision));
            let (count, bytes) = history::events_charge(group)?;
            index.track_events(
                revision,
                self.event_completion.get(&revision).copied(),
                count,
                bytes,
            )?;
            previous = revision;
        }
        self.history = index;
        self.command_history = command_history;
        self.command_history_bytes = command_history_bytes;
        self.history_indexed = true;
        Ok(())
    }
    fn account_history_changes(
        &mut self,
        previous: &Self,
        keys: impl Iterator<Item = RequestKey>,
        events_from: usize,
        completed_at: u64,
    ) -> Result<()> {
        use super::history;
        for key in keys {
            let outcome = self
                .requests
                .get(&key)
                .ok_or_else(|| invalid_contract("history new outcome missing"))?;
            let completion = self
                .request_completion
                .get(&key)
                .ok_or_else(|| invalid_contract("history new completion missing"))?;
            if let Some(old) = previous.requests.get(&key) {
                account_command_result(
                    &mut self.command_history,
                    &mut self.command_history_bytes,
                    &old.result,
                    false,
                )?;
            }
            account_command_result(
                &mut self.command_history,
                &mut self.command_history_bytes,
                &outcome.result,
                true,
            )?;
            self.history.track_request(
                key,
                completion.clone(),
                history::request_charge(outcome)?,
            )?;
        }
        if events_from > self.events.len() {
            return Err(invalid_contract("history new event range is invalid"));
        }
        // im::Vector's iterator has no indexed nth; skip would walk the entire
        // old history on every small commit. Visit only this decision's tail.
        let (count, bytes) = history::events_charge(
            (events_from..self.events.len()).map(|index| self.events.0[index].as_ref()),
        )?;
        if count > 0 {
            self.history
                .track_events(self.revision, Some(completed_at), count, bytes)?;
        }
        self.history_indexed = true;
        Ok(())
    }
    pub(super) fn prepare_history(
        &self,
        now: u64,
        limits: super::history::Limits,
        protected: &std::collections::BTreeSet<RequestKey>,
    ) -> Result<Option<(Self, super::history::Delta)>> {
        // Before the first maintenance tick, legacy outcomes may be the last
        // surviving evidence of a used epoch. Capture that floor durably before
        // retention can retire it. Normal resource admission remains unchanged.
        if !self.entities.contains_key(&MetaKey::RootEpochWatermark)
            && self.root_epoch_watermark() != 0
        {
            return Ok(None);
        }
        let mut prepared = self.clone();
        prepared.ensure_history_index()?;
        let Some(delta) = prepared.history.select(
            prepared.revision,
            prepared.compacted_to,
            now,
            limits,
            protected,
        )?
        else {
            return Ok(None);
        };
        Ok(Some((prepared.apply_history(&delta)?, delta)))
    }
    pub(super) fn history_fits(&self, limits: super::history::Limits) -> bool {
        self.history_indexed && self.history.fits(limits)
    }

    fn next_node_event_index(&self) -> Result<u32> {
        self.events_removed
            .checked_add(self.events.len() as u64)
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| invalid_contract("node event index overflow"))
    }
    pub(super) fn replay_record(&self, record: &super::log::Record) -> Result<Self> {
        record.validate()?;
        match record {
            super::log::Record::Decision { decision, history } => {
                let next = self.replay_decision(decision)?;
                match history {
                    Some(history) => next.apply_history(history),
                    None => Ok(next),
                }
            }
            super::log::Record::History(history) => self.apply_history(history),
        }
    }
    pub(super) fn apply_history(&self, history: &super::history::Delta) -> Result<Self> {
        history.validate()?;
        if history.revision != self.revision || history.compacted_from != self.compacted_to {
            return Err(invalid_contract(
                "history cleanup does not follow published revision/watermark",
            ));
        }
        for request in &history.requests {
            if !self.requests.contains_key(&request.key)
                || self.request_completion.get(&request.key) != Some(&request.completion)
            {
                return Err(invalid_contract(
                    "history cleanup request generation mismatch",
                ));
            }
        }
        // Validate the exact complete prefix before changing this private view.
        // The persisted watermark, not this process's clock, chooses removals.
        let mut removed = 0usize;
        let mut last = self.compacted_to;
        for event in self.events.iter() {
            if event.revision <= self.compacted_to {
                return Err(invalid_contract(
                    "retained event is below compaction watermark",
                ));
            }
            if event.revision > history.compacted_to {
                break;
            }
            removed = removed
                .checked_add(1)
                .ok_or_else(|| invalid_contract("history event removal count overflow"))?;
            last = event.revision;
        }
        if last != history.compacted_to {
            return Err(invalid_contract(
                "history cleanup watermark is not a retained revision boundary",
            ));
        }
        let events_removed = self
            .events_removed
            .checked_add(removed as u64)
            .ok_or_else(|| invalid_contract("history event ordinal overflow"))?;
        let mut next = self.clone();
        next.ensure_history_index()?;
        next.history = next
            .history
            .apply(history, self.revision, self.compacted_to)?;
        for request in &history.requests {
            let outcome = next
                .requests
                .get(&request.key)
                .ok_or_else(|| invalid_contract("retiring command outcome missing"))?;
            account_command_result(
                &mut next.command_history,
                &mut next.command_history_bytes,
                &outcome.result,
                false,
            )?;
            next.requests.remove(&request.key);
            next.request_completion.remove(&request.key);
        }
        for _ in 0..removed {
            next.events.0.pop_front();
        }
        while let Some(revision) = next
            .event_completion
            .iter()
            .next()
            .map(|(revision, _)| *revision)
        {
            if revision > history.compacted_to {
                break;
            }
            next.event_completion.remove(&revision);
        }
        next.events_removed = events_removed;
        next.compacted_to = history.compacted_to;
        Ok(next)
    }
    /// Runs only on the single writer, without an await between selection and
    /// preparation. Queued acquire/recover/recreate operations cannot interleave.
    /// The cursor is disposable: restart rescans; only decided deletes are durable.
    pub(super) fn prepare_root_grant_cleanup(
        &self,
        cursor: &mut Option<MetaKey>,
        completed_at: u64,
        byte_limit: usize,
        max_deletes: usize,
    ) -> Result<Option<(Self, super::log::RecordDecision)>> {
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
            .push(super::TxnCondition::RequestAbsent(request.clone()));
        let bootstrap = (!self.entities.contains_key(&MetaKey::RootEpochWatermark))
            .then(|| self.root_epoch_watermark())
            .filter(|value| *value != 0);
        if let Some(value) = bootstrap {
            txn.conditions.extend([
                super::TxnCondition::Missing(MetaKey::RootEpochWatermark),
                super::TxnCondition::RootEpochWatermarkEquals(value),
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
            let key_bytes = super::codec::encoded_size(key)?;
            bytes = bytes
                .checked_add(key_bytes)
                .ok_or_else(|| invalid_contract("grant cleanup size overflow"))?;
            txn.conditions.push(super::TxnCondition::RevisionEquals {
                key: key.clone(),
                revision: versioned.revision,
            });
            txn.mutations.push(super::TxnMutation::Delete(key.clone()));
            deleted += 1;
        }
        if exhausted {
            *cursor = None;
        }
        if deleted == 0 && bootstrap.is_none() {
            return Ok(None);
        }
        let watermark =
            bootstrap.map(|value| super::TxnMutation::Put(MetaEntity::RootEpochWatermark(value)));
        if let Some(mutation) = &watermark {
            txn.mutations.push(mutation.clone());
        }
        let result = super::TxnMutation::RecordRequestOutcome(RequestOutcome {
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
                    let frame = super::log::Record::from(record.clone());
                    if super::journal::frame_size(self.log_tip, &frame)? > byte_limit {
                        return Err(super::journal::full(
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
            if let Some(super::TxnMutation::Delete(key)) = txn.mutations.last() {
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
    pub(super) fn prepare_root_command_cleanup(
        &self,
        cursor: &mut Option<MetaKey>,
        now: u64,
        byte_limit: usize,
        max_deletes: usize,
    ) -> Result<Option<(Self, super::log::RecordDecision)>> {
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
                .checked_add(super::codec::encoded_size(key)?)
                .and_then(|n| n.checked_add(super::codec::encoded_size(&ack_key).ok()?))
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
                .push(super::TxnCondition::RequestAbsent(request.clone()));
            for (command, ack) in &pairs {
                for (key, revision) in [command, ack] {
                    txn.conditions.push(super::TxnCondition::RevisionEquals {
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
                        if super::journal::frame_size(
                            self.log_tip,
                            &super::log::Record::from(record.clone()),
                        )? > byte_limit
                        {
                            return Err(super::journal::full(
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

    fn obsolete_root_grant(&self, grant: &super::RootAccessGrant, now: u64) -> bool {
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

    pub(super) fn prepare_txn_record(
        &self,
        txn: MetaTxn,
        completed_at: u64,
        byte_limit: usize,
    ) -> Result<(Self, TxnOutcome, Option<super::log::RecordDecision>)> {
        use super::log::{DecisionInput, RecordDecision};
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

    pub(super) fn prepare_node_record(
        &self,
        request: RequestKey,
        lease: NodeSessionLease,
        completed_at: u64,
        byte_limit: usize,
    ) -> Result<(Self, TxnOutcome, Option<super::log::RecordDecision>)> {
        use super::log::{DecisionInput, RecordDecision};
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

    pub(super) fn replay_decision(&self, record: &super::log::RecordDecision) -> Result<Self> {
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

#[cfg(test)]
mod completion_tests {
    use super::*;
    fn lease(session: &str) -> NodeSessionLease {
        NodeSessionLease {
            node_id: "node".into(),
            session_id: session.into(),
            grpc_addr: "grpc".into(),
            data_addr: "data".into(),
            rest_addr: "rest".into(),
            storage_devices: Vec::new(),
            lease_ttl: std::time::Duration::from_secs(60),
        }
    }
    #[test]
    fn p3_lifecycle_command_reference_index_rebuilds_and_retires_exactly() {
        let command = super::super::RootCommandRecord {
            command_id: "shared-command".into(),
            home_node_id: "home".into(),
            home_session_id: "session".into(),
            root_id: "removed".into(),
            root_epoch: 7,
            old_access_generation: 1,
            command_type: super::super::RootCommandType::RevokeAccess,
        };
        let key = RequestKey::new("index", "first");
        let mut txn = MetaTxn::new(key.clone(), StoreOperation::BeginRootRevocation);
        txn.conditions
            .push(super::super::TxnCondition::RequestAbsent(key.clone()));
        for request in [key.clone(), RequestKey::new("index", "second")] {
            txn.mutations
                .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                    request,
                    operation: txn.operation,
                    result: OperationResult::RootCommand(command.clone()),
                }));
        }
        let (state, _, first) = State::default().prepare_txn_record(txn, 1, 65536).unwrap();
        assert_eq!(state.command_history.get("shared-command"), Some(&2));
        let (state, bootstrap) = state
            .prepare_root_grant_cleanup(&mut None, 2, 65536, 32)
            .unwrap()
            .unwrap();
        let limits = super::super::history::Limits {
            cleanup_items: 1,
            request_retention: std::time::Duration::from_millis(1),
            event_retention: std::time::Duration::from_millis(1),
            ..Default::default()
        };
        let (once, delta1) = state
            .prepare_history(100, limits, &Default::default())
            .unwrap()
            .unwrap();
        assert_eq!(once.command_history.get("shared-command"), Some(&1));
        assert!(once.command_index_charge().unwrap() >= 4096 + 256);
        let bytes = super::super::log::StateImage::encode(&once, 65536).unwrap();
        let restored = super::super::log::StateImage::decode(&bytes, 65536).unwrap();
        assert_eq!(restored.command_history, once.command_history);
        assert_eq!(
            restored.command_index_charge().unwrap(),
            once.command_index_charge().unwrap()
        );
        let (twice, delta2) = once
            .prepare_history(100, limits, &Default::default())
            .unwrap()
            .unwrap();
        assert!(twice.command_history.is_empty());
        assert_eq!(twice.command_index_charge().unwrap(), 0);
        let mut replay = State::default();
        for record in [
            super::super::log::Record::from(first.unwrap()),
            super::super::log::Record::from(bootstrap),
            super::super::log::Record::History(delta1),
            super::super::log::Record::History(delta2),
        ] {
            replay = replay.replay_record(&record).unwrap();
        }
        assert!(replay.command_history.is_empty());
        assert_eq!(replay.root_epoch_watermark(), 7);
    }

    #[test]
    fn p3_replay_register_label_preserves_generic_and_dedicated_event_bases() {
        let empty = State::default();
        let (first, _, record1) = empty
            .prepare_node_record(RequestKey::new("node", "first"), lease("first"), 100, 65536)
            .unwrap();
        let (second, _, record2) = first
            .prepare_node_record(
                RequestKey::new("node", "second"),
                lease("second"),
                200,
                65536,
            )
            .unwrap();
        let key = RequestKey::new("node", "departed");
        let mut request = MetaTxn::new(key.clone(), StoreOperation::RegisterNode);
        request
            .conditions
            .push(super::super::TxnCondition::RequestAbsent(key.clone()));
        request.mutations.extend([
            TxnMutation::Delete(MetaKey::CurrentNodeSession {
                node_id: "node".into(),
            }),
            TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: key,
                operation: StoreOperation::RegisterNode,
                result: OperationResult::Empty,
            }),
        ]);
        let (expected, _, record3) = second.prepare_txn_record(request, 300, 65536).unwrap();
        let record3 = record3.unwrap();
        assert_eq!(
            record2.as_ref().unwrap().facts().unwrap().events[0].event_index,
            2
        );
        assert_eq!(record3.facts().unwrap().events[0].event_index, 0);
        let replayed = empty
            .replay_decision(&record1.unwrap())
            .unwrap()
            .replay_decision(&record2.unwrap())
            .unwrap()
            .replay_decision(&record3)
            .unwrap();
        assert_eq!(
            replayed.snapshot_bytes_with_batch(0).unwrap(),
            expected.snapshot_bytes_with_batch(0).unwrap()
        );
        // A nonzero base must still be the exact dedicated-registration base.
        let facts = record3.facts().unwrap();
        let mut wrong_events = facts.events.clone();
        wrong_events[0].event_index = 1;
        let bad = super::super::log::RecordDecision::new(
            super::super::log::DecisionInput {
                request: facts.request,
                operation: facts.operation,
                revision_before: facts.revision_before,
                revision_after: facts.revision_after,
                completed_at_unix_ms: facts.completed_at_unix_ms,
                events: &wrong_events,
                outcomes: &facts.outcomes,
                reply: &facts.reply,
                node_epochs: facts.node_epochs,
            },
            65536,
        )
        .unwrap();
        let error = match second.replay_decision(&bad) {
            Err(error) => error,
            Ok(_) => panic!("invalid event base accepted"),
        };
        assert!(error.to_string().contains("event base mismatch"));
    }

    #[test]
    fn p3_completion_metadata_survives_prepare_replay_and_retry() {
        let state = State::default();
        let key = RequestKey::new("node", "first");
        let (first, _, record) = state
            .prepare_node_record(key.clone(), lease("first"), 12345, 65536)
            .unwrap();
        let expected = RequestCompletion {
            completed_at_unix_ms: 12345,
            revision: StoreRevision(1),
        };
        assert_eq!(
            first.request_completion.get(&key),
            Some(&expected),
            "private prepare must retain original decided completion"
        );
        let restored = state.replay_decision(&record.unwrap()).unwrap();
        assert_eq!(restored.request_completion.get(&key), Some(&expected));
        let (retried, result, duplicate) = restored
            .prepare_node_record(key.clone(), lease("first"), 99999, 65536)
            .unwrap();
        assert!(matches!(
            result,
            TxnOutcome::ConditionFailed {
                existing_outcome: Some(_),
                ..
            }
        ));
        assert!(duplicate.is_none());
        assert_eq!(
            retried.request_completion.get(&key),
            Some(&expected),
            "retry or restart must not reset retention clock"
        );
        let bytes = first.snapshot_bytes_with_batch(0).unwrap();
        let snapshot = State::from_snapshot(&bytes).unwrap();
        assert_eq!(snapshot.request_completion.get(&key), Some(&expected));
        assert_eq!(snapshot.requests.get(&key), first.requests.get(&key));
    }
    fn txn(request: RequestKey) -> MetaTxn {
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::ActivateRoot);
        txn.conditions
            .push(super::super::TxnCondition::RequestAbsent(request.clone()));
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation: StoreOperation::ActivateRoot,
                result: OperationResult::Empty,
            }));
        txn
    }

    #[test]
    fn p3_completion_metadata_covers_auxiliary_outcomes_without_retry_refresh() {
        let primary = RequestKey::new("owner", "primary");
        let auxiliary = RequestKey::new("owner", "auxiliary");
        let mut request = txn(primary.clone());
        request
            .mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: auxiliary.clone(),
                operation: StoreOperation::ActivateRoot,
                result: OperationResult::Empty,
            }));
        let empty = State::default();
        let (prepared, _, record) = empty
            .prepare_txn_record(request.clone(), 200, 65536)
            .unwrap();
        assert!(
            empty.request_completion.iter().next().is_none(),
            "private prepare cannot mutate published state"
        );
        let replayed = empty.replay_decision(&record.unwrap()).unwrap();
        for key in [&primary, &auxiliary] {
            let expected = RequestCompletion {
                completed_at_unix_ms: 200,
                revision: StoreRevision(1),
            };
            assert_eq!(prepared.request_completion.get(key), Some(&expected));
            assert_eq!(replayed.request_completion.get(key), Some(&expected));
        }
        let (retried, result, duplicate) =
            replayed.prepare_txn_record(request, 9999, 65536).unwrap();
        assert!(matches!(
            result,
            TxnOutcome::ConditionFailed {
                existing_outcome: Some(_),
                ..
            }
        ));
        assert!(duplicate.is_none());
        assert_eq!(
            retried.request_completion.get(&primary),
            prepared.request_completion.get(&primary)
        );
        assert_eq!(
            retried.request_completion.get(&auxiliary),
            prepared.request_completion.get(&auxiliary)
        );
    }

    #[test]
    fn p3_completion_metadata_rejects_orphans_duplicates_and_future_revisions() {
        let key = RequestKey::new("owner", "single");
        let (state, _, _) = State::default()
            .prepare_txn_record(txn(key.clone()), 300, 65536)
            .unwrap();
        let snapshot = state.snapshot_bytes_with_batch(0).unwrap();
        for case in 0..4 {
            let mut persisted: PersistedState = serde_json::from_slice(&snapshot).unwrap();
            match case {
                0 => persisted.request_completion[0].0 = RequestKey::new("missing", "outcome"),
                1 => persisted
                    .request_completion
                    .push(persisted.request_completion[0].clone()),
                2 => persisted.request_completion[0].1.revision = StoreRevision(2),
                3 => persisted.compacted_to = StoreRevision(2),
                _ => unreachable!(),
            }
            let corrupt = serde_json::to_vec(&persisted).unwrap();
            assert!(
                State::from_snapshot(&corrupt).is_err(),
                "corrupt metadata case {case}"
            );
        }
        let mut old: serde_json::Value = serde_json::from_slice(&snapshot).unwrap();
        old.as_object_mut().unwrap().remove("request_completion");
        old.as_object_mut().unwrap().remove("compacted_to");
        let restored = State::from_snapshot(&serde_json::to_vec(&old).unwrap()).unwrap();
        assert_eq!(restored.requests.get(&key), state.requests.get(&key));
        assert!(
            restored.request_completion.iter().next().is_none(),
            "old JSON must not invent a restart completion time"
        );
    }

    #[test]
    fn p3_completion_metadata_rejected_prepare_leaves_authority_unchanged() {
        let state = State::default();
        let key = RequestKey::new("owner", "too-large");
        assert!(state.prepare_txn_record(txn(key.clone()), 100, 1).is_err());
        assert_eq!(state.revision, StoreRevision(0));
        assert!(state.requests.get(&key).is_none());
        assert!(state.request_completion.get(&key).is_none());
    }
}

#[cfg(test)]
mod grant_lifecycle_gc_tests {
    use super::super::{RootAccessGrant, RootRecord, VersionedEntity};
    use super::*;

    fn put(state: &mut State, entity: MetaEntity) {
        state
            .put_authority(
                entity_key(&entity),
                VersionedEntity {
                    revision: state.revision,
                    entity,
                },
            )
            .unwrap();
    }
    fn fixture() -> (State, RootAccessGrant) {
        let mut state = State {
            revision: StoreRevision(1),
            ..Default::default()
        };
        let root = RootRecord {
            root_id: "workspace".into(),
            root_epoch: 7,
            home_node_id: "home".into(),
            home_session_id: "s".into(),
            local_prepare_id: "p".into(),
            created_at_revision: StoreRevision(1),
            updated_at_revision: StoreRevision(1),
        };
        let grant = RootAccessGrant {
            root_id: root.root_id.clone(),
            root_epoch: 7,
            home_node_id: "home".into(),
            home_session_id: "s".into(),
            holder_node_id: "home".into(),
            holder_session_id: "s".into(),
            access_generation: 9,
            rights: vec![super::super::RootRight::Read],
            fencing_token: "f".into(),
            issued_at_revision: StoreRevision(1),
        };
        put(&mut state, MetaEntity::Root(root));
        put(&mut state, MetaEntity::RootGrant(grant.clone()));
        put(&mut state, MetaEntity::RootEpochWatermark(7));
        // Expiry is intentionally not enough to collect the current Home grant.
        let session = NodeSession {
            node_id: "home".into(),
            session_id: "s".into(),
            grpc_addr: "g".into(),
            data_addr: "d".into(),
            rest_addr: "r".into(),
            storage_devices: vec![],
            lease_epoch: 1,
            expires_at_unix_ms: 0,
        };
        state
            .put_authority(
                MetaKey::CurrentNodeSession {
                    node_id: "home".into(),
                },
                VersionedEntity {
                    revision: state.revision,
                    entity: MetaEntity::NodeSession(session.clone()),
                },
            )
            .unwrap();
        put(&mut state, MetaEntity::NodeSession(session));
        state.set_epoch("home".into(), 1).unwrap();
        (state, grant)
    }
    #[test]
    fn grant_lifecycle_gc_bounds_scan_deletes_and_retains_expired_current_home() {
        let (mut state, home) = fixture();
        for i in 0..300 {
            let mut v = home.clone();
            v.holder_node_id = format!("keep-{i:04}");
            put(&mut state, MetaEntity::RootGrant(v));
        }
        for i in 0..75 {
            let mut v = home.clone();
            v.holder_node_id = format!("stale-{i:04}");
            v.home_session_id = "old-home".into();
            put(&mut state, MetaEntity::RootGrant(v));
        }
        let original = state.clone();
        let mut cursor = None;
        assert!(
            state
                .prepare_root_grant_cleanup(&mut cursor, 1, 1024 * 1024, 32)
                .unwrap()
                .is_none()
        );
        assert!(
            matches!(&cursor,Some(MetaKey::RootGrant{holder_node_id,..}) if holder_node_id=="keep-0126"),
            "only128entries may be scanned"
        );
        let mut removed = 0;
        for _ in 0..12 {
            if let Some((next, record)) = state
                .prepare_root_grant_cleanup(&mut cursor, 1, 1024 * 1024, 32)
                .unwrap()
            {
                let count = state.entities.len() - next.entities.len();
                assert!((1..=32).contains(&count));
                removed += count;
                let replayed = state
                    .replay_record(&super::super::log::Record::from(record))
                    .unwrap();
                assert_eq!(replayed.entities.len(), next.entities.len());
                state = replayed;
            }
        }
        assert_eq!(removed, 75);
        assert_eq!(original.entities.len() - state.entities.len(), 75);
        assert!(
            state
                .entities
                .contains_key(&entity_key(&MetaEntity::RootGrant(home)))
        );
    }
    #[test]
    fn grant_lifecycle_gc_cannot_delete_reacquired_same_key_in_either_writer_order() {
        let (mut state, home) = fixture();
        let mut old = home.clone();
        old.holder_node_id = "peer".into();
        old.root_epoch = 6;
        put(&mut state, MetaEntity::RootGrant(old.clone()));
        let mut fresh = old;
        fresh.root_epoch = 7;
        let fresh_key = entity_key(&MetaEntity::RootGrant(fresh.clone()));
        let mut acquired_first = state.clone();
        put(&mut acquired_first, MetaEntity::RootGrant(fresh.clone()));
        assert!(
            acquired_first
                .prepare_root_grant_cleanup(&mut None, 1, 1024 * 1024, 32)
                .unwrap()
                .is_none()
        );
        let (mut cleaned, record) = state
            .prepare_root_grant_cleanup(&mut None, 1, 1024 * 1024, 32)
            .unwrap()
            .unwrap();
        assert!(!cleaned.entities.contains_key(&fresh_key));
        let decoded = state
            .replay_record(&super::super::log::Record::from(record))
            .unwrap();
        assert!(!decoded.entities.contains_key(&fresh_key));
        put(&mut cleaned, MetaEntity::RootGrant(fresh.clone()));
        assert!(
            cleaned
                .prepare_root_grant_cleanup(&mut None, 1, 1024 * 1024, 32)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            cleaned.entities.get(&fresh_key).unwrap().entity,
            MetaEntity::RootGrant(fresh)
        );
    }
    #[test]
    fn grant_lifecycle_gc_collects_expired_or_replaced_peers_but_retains_home() {
        let (mut state, home) = fixture();
        let mut peer = home.clone();
        peer.holder_node_id = "peer".into();
        put(&mut state, MetaEntity::RootGrant(peer.clone()));
        let mut session = NodeSession {
            node_id: "peer".into(),
            session_id: "s".into(),
            grpc_addr: "g".into(),
            data_addr: "d".into(),
            rest_addr: "r".into(),
            storage_devices: vec![],
            lease_epoch: 1,
            expires_at_unix_ms: 100,
        };
        let key = MetaKey::CurrentNodeSession {
            node_id: "peer".into(),
        };
        state
            .put_authority(
                key.clone(),
                VersionedEntity {
                    revision: state.revision,
                    entity: MetaEntity::NodeSession(session.clone()),
                },
            )
            .unwrap();
        assert!(
            state
                .prepare_root_grant_cleanup(&mut None, 1, 1024 * 1024, 32)
                .unwrap()
                .is_none()
        );
        session.expires_at_unix_ms = 0;
        state
            .put_authority(
                key.clone(),
                VersionedEntity {
                    revision: state.revision,
                    entity: MetaEntity::NodeSession(session.clone()),
                },
            )
            .unwrap();
        let (expired, _) = state
            .prepare_root_grant_cleanup(&mut None, 1, 1024 * 1024, 32)
            .unwrap()
            .unwrap();
        assert!(
            !expired
                .entities
                .contains_key(&entity_key(&MetaEntity::RootGrant(peer.clone())))
        );
        assert!(
            expired
                .entities
                .contains_key(&entity_key(&MetaEntity::RootGrant(home.clone())))
        );
        session.expires_at_unix_ms = 100;
        session.session_id = "new-session".into();
        state
            .put_authority(
                key,
                VersionedEntity {
                    revision: state.revision,
                    entity: MetaEntity::NodeSession(session),
                },
            )
            .unwrap();
        let (cleaned, _) = state
            .prepare_root_grant_cleanup(&mut None, 1, 1024 * 1024, 32)
            .unwrap()
            .unwrap();
        assert!(
            !cleaned
                .entities
                .contains_key(&entity_key(&MetaEntity::RootGrant(peer)))
        );
        assert!(
            cleaned
                .entities
                .contains_key(&entity_key(&MetaEntity::RootGrant(home)))
        );
    }
    #[test]
    fn grant_lifecycle_epoch_watermark_bootstraps_legacy_and_survives_entity_removal() {
        let (mut state, home) = fixture();
        state
            .delete_authority(&MetaKey::RootEpochWatermark)
            .unwrap();
        assert_eq!(state.root_epoch_watermark(), 7);
        let request = RequestKey::new("legacy", "aborted-reservation");
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::ReserveRoot);
        txn.conditions
            .push(super::super::TxnCondition::RequestAbsent(request.clone()));
        txn.mutations
            .push(super::super::TxnMutation::RecordRequestOutcome(
                RequestOutcome {
                    request,
                    operation: StoreOperation::ReserveRoot,
                    result: OperationResult::RootReservation(super::super::RootReservationRecord {
                        root_id: "retired".into(),
                        root_epoch: 40,
                        home_node_id: "home".into(),
                        home_session_id: "s".into(),
                        create_intent_id: "legacy".into(),
                        prepare_token: "p".into(),
                        created_at_revision: StoreRevision(1),
                    }),
                },
            ));
        state = state.prepare_txn_record(txn, 1, 1024 * 1024).unwrap().0;
        assert_eq!(state.root_epoch_watermark(), 40);
        assert!(
            state
                .prepare_history(
                    u64::MAX - 1,
                    super::super::history::Limits::default(),
                    &std::collections::BTreeSet::new()
                )
                .unwrap()
                .is_none(),
            "outcome-only epoch evidence must survive until the floor is durable"
        );
        let (bootstrapped, decision) = state
            .prepare_root_grant_cleanup(&mut None, 1, 1024 * 1024, 32)
            .unwrap()
            .expect("persist the legacy floor before grant/history retirement");
        state = state
            .replay_record(&super::super::log::Record::from(decision))
            .unwrap();
        assert_eq!(state.revision, bootstrapped.revision);
        assert!(state.entities.contains_key(&MetaKey::RootEpochWatermark));
        state
            .delete_authority(&entity_key(&MetaEntity::RootGrant(home)))
            .unwrap();
        state
            .delete_authority(&MetaKey::Root {
                root_id: "workspace".into(),
            })
            .unwrap();
        let (retired, _) = state
            .prepare_history(
                u64::MAX - 1,
                super::super::history::Limits::default(),
                &std::collections::BTreeSet::new(),
            )
            .unwrap()
            .expect("history can retire after capture");
        state = retired;
        assert_eq!(state.root_epoch_watermark(), 40);
        let image = super::super::log::StateImage::encode(&state, 1024 * 1024).unwrap();
        let recovered = super::super::log::StateImage::decode(&image, 1024 * 1024).unwrap();
        assert_eq!(recovered.root_epoch_watermark(), 40);
    }
    #[test]
    fn grant_lifecycle_gc_shrinks_to_record_limit_and_failed_prepare_keeps_authority() {
        let (mut state, home) = fixture();
        for i in 0..40 {
            let mut old = home.clone();
            old.holder_node_id = format!("old-{i:02}-{}", "x".repeat(32));
            old.home_session_id = "old".into();
            put(&mut state, MetaEntity::RootGrant(old));
        }
        let before = state.entities.len();
        assert!(
            state
                .prepare_root_grant_cleanup(&mut None, 1, 1, 32)
                .is_err()
        );
        assert_eq!(state.entities.len(), before);
        let (small, record) = state
            .prepare_root_grant_cleanup(&mut None, 1, 1024, 32)
            .unwrap()
            .unwrap();
        let removed = before - small.entities.len();
        assert!(
            removed > 0 && removed < 32,
            "the byte bound must shrink this real batch"
        );
        assert!(
            super::super::journal::frame_size(
                state.log_tip,
                &super::super::log::Record::from(record)
            )
            .unwrap()
                <= 1024
        );
        let mut cursor = None;
        let mut current = state;
        for _ in 0..40 {
            if let Some((next, _)) = current
                .prepare_root_grant_cleanup(&mut cursor, 1, 1024, 32)
                .unwrap()
            {
                current = next;
            }
        }
        assert_eq!(
            before - current.entities.len(),
            40,
            "bounded smaller batches must eventually finish"
        );
    }
}
