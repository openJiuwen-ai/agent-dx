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

mod apply;
mod cleanup;
mod history_index;
mod read;
mod replay;
mod snapshot;

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
    /// Runtime recovery anchor (tip and writer term) paired with this
    /// immutable published state. It is framed by the backend, not duplicated
    /// in the business image.
    pub(super) log_cursor: super::journal::LogCursor,
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
    /// Derived from `DfsDentry` keys, never serialized: (namespace, child, parent,
    /// name), so the parent of a directory is one ordered lookup. A directory has
    /// exactly one dentry; a hard-linked file has one entry per link.
    pub(super) dentry_parents: im::OrdSet<(
        crate::dfs::NamespaceId,
        crate::dfs::InodeId,
        crate::dfs::InodeId,
        Vec<u8>,
    )>,
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

#[cfg(test)]
mod completion_tests;

#[cfg(test)]
mod grant_lifecycle_gc_tests;
