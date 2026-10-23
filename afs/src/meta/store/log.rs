//! Canonical, already-decided authority changes. No RPC is re-executed on replay.
//!
//! Each distinct entity payload occurs once in the value pool. Changes and
//! original outcomes refer to that pool; watch events are derived from changes.
//! This module is independent of WAL framing, recovery identity and disk I/O.

use std::collections::BTreeMap;

use afs_error::Result;
use serde::{Deserialize, Serialize};

use super::{
    MetaEntity, MetaKey, OperationResult, RequestKey, RequestOutcome, StoreOperation,
    StoreRevision, WatchChange, WatchEvent, codec, entity_key, invalid_contract,
};
use crate::dfs::{
    CopyId, OperationId, RenameOutcome, ReplicaGroup, ReplicationClaim, ReplicationTask,
    ReplicationTaskId,
};

const SCHEMA: u16 = 1;
const IMAGE_SCHEMA: u16 = 2;
const MAX_NESTING: usize = 128;
const MAX_ITEMS: usize = 65_536;

// Reuse the former four independent vector allowances. Neither their combined
// slot ceiling nor their combined inline allocation ceiling is increased.
// A delete-only operation can use slots not occupied by pooled entity values.
const RECORD_ITEMS: usize = 4 * MAX_ITEMS;
const RECORD_VECTOR_BYTES: usize = MAX_ITEMS
    * (std::mem::size_of::<PooledValue>()
        + std::mem::size_of::<Change>()
        + std::mem::size_of::<SavedOutcome>()
        + std::mem::size_of::<(String, u64)>());

struct RecordBudget {
    items: usize,
    bytes: usize,
}
impl RecordBudget {
    fn capacity(count: usize) -> Result<usize> {
        if count == 0 {
            Ok(0)
        } else {
            count
                .checked_next_power_of_two()
                .ok_or_else(|| invalid_contract("canonical capacity overflow"))
        }
    }
    fn new() -> Self {
        Self {
            items: RECORD_ITEMS,
            bytes: RECORD_VECTOR_BYTES,
        }
    }
    fn charge<T>(&mut self, count: usize) -> Result<()> {
        let bytes = count
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| invalid_contract("canonical vector charge overflow"))?;
        let items = self
            .items
            .checked_sub(count)
            .ok_or_else(|| invalid_contract("canonical combined item budget exceeded"))?;
        let bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or_else(|| invalid_contract("canonical vector byte budget exceeded"))?;
        self.items = items;
        self.bytes = bytes;
        Ok(())
    }
    fn reserve<T>(&mut self, values: &mut Vec<T>, capacity: usize) -> Result<()> {
        // Use the same geometric capacity policy with or without a count hint.
        // Thus a writer cannot accept an exact-fit vector which the reader
        // would reject after geometric growth on postcard's unhinted sequence.
        let capacity = Self::capacity(capacity)?;
        if capacity <= values.capacity() {
            return Ok(());
        }
        self.charge::<T>(capacity - values.capacity())?;
        values
            .try_reserve_exact(capacity - values.len())
            .map_err(|_| {
                afs_error::Error::coded(afs_error::IO_OUT_OF_MEMORY, "grow canonical vector")
            })?;
        // Account reported capacity, rather than assuming allocator rounding.
        self.charge::<T>(values.capacity() - capacity)
    }
    fn reserve_slot<T>(&mut self, values: &mut Vec<T>) -> Result<()> {
        if values.len() < values.capacity() {
            return Ok(());
        }
        let width = std::mem::size_of::<T>();
        if width == 0 {
            return Err(invalid_contract("zero-width canonical vector item"));
        }
        let capacity = values.capacity();
        let target = capacity
            .checked_mul(2)
            .ok_or_else(|| invalid_contract("canonical capacity overflow"))?
            .max(1);
        self.reserve(values, target)
    }
}

struct RecordItems<'a, T> {
    budget: &'a mut RecordBudget,
    marker: std::marker::PhantomData<T>,
}
struct RecordItem<'a, T> {
    budget: &'a mut RecordBudget,
    values: &'a mut Vec<T>,
}
impl<'de, T: Deserialize<'de>> serde::de::DeserializeSeed<'de> for RecordItem<'_, T> {
    type Value = T;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<T, D::Error> {
        self.budget
            .reserve_slot(self.values)
            .map_err(serde::de::Error::custom)?;
        T::deserialize(deserializer)
    }
}
impl<'de, T: Deserialize<'de>> serde::de::DeserializeSeed<'de> for RecordItems<'_, T> {
    type Value = Vec<T>;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Vec<T>, D::Error> {
        struct Items<'a, T>(RecordItems<'a, T>);
        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Items<'_, T> {
            type Value = Vec<T>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("canonical items within the shared vector budget")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut input: A,
            ) -> std::result::Result<Vec<T>, A::Error> {
                let mut values = Vec::new();
                if let Some(count) = input.size_hint() {
                    // Reject an untrusted count before allocating its vector.
                    self.0
                        .budget
                        .reserve(&mut values, count)
                        .map_err(serde::de::Error::custom)?;
                }
                while let Some(value) = input.next_element_seed(RecordItem {
                    budget: &mut *self.0.budget,
                    values: &mut values,
                })? {
                    values.push(value);
                }
                Ok(values)
            }
        }
        deserializer.deserialize_seq(Items(self))
    }
}

pub(super) fn bounded_items<'de, D, T>(deserializer: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Bounded<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Bounded<T> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "at most {MAX_ITEMS} canonical items")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let expected = sequence.size_hint().unwrap_or(0);
            if expected > MAX_ITEMS {
                return Err(serde::de::Error::custom(
                    "canonical item count exceeds limit",
                ));
            }
            let mut values = Vec::new();
            values
                .try_reserve_exact(expected)
                .map_err(|_| serde::de::Error::custom("allocate canonical items"))?;
            while let Some(value) = sequence.next_element()? {
                if values.len() == MAX_ITEMS {
                    return Err(serde::de::Error::custom(
                        "canonical item count exceeds limit",
                    ));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Bounded(std::marker::PhantomData))
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ValueId(u32);
impl ValueId {
    fn at(index: usize) -> Result<Self> {
        u32::try_from(index)
            .map(Self)
            .map_err(|_| invalid_contract("canonical value index overflow"))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum Change {
    Put { key: MetaKey, value: ValueId },
    Delete { key: MetaKey },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SavedOutcome {
    request: RequestKey,
    operation: StoreOperation,
    /// Flatten the recursive public wrapper before persistence, so corrupt
    /// nesting cannot overflow the deserializer's stack.
    #[serde(deserialize_with = "bounded_items")]
    namespace_digests: Vec<[u8; 32]>,
    result: SavedResult,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum SavedResult {
    NodeSession(ValueId),
    RootReservation(ValueId),
    RootRecord(ValueId),
    RootGrant(ValueId),
    RootCommand(ValueId),
    RootCommandAck(ValueId),
    DfsInode(ValueId),
    DfsInodeWithLease {
        inode: ValueId,
        lease: ValueId,
    },
    DfsWriteLease(ValueId),
    DfsReplicationClaim {
        request_digest: [u8; 32],
        claim: Option<ValueId>,
    },
    DfsReplicationTask(ValueId),
    DfsRename {
        inode: ValueId,
        replaced_inode: Option<ValueId>,
    },
    Empty,
}

/// Only the claim's independently meaningful result fields are inline. Its
/// chunk/config entities share the same pool as authority changes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SavedClaim {
    task_id: ReplicationTaskId,
    operation_id: OperationId,
    worker_node_id: String,
    worker_node_epoch: u64,
    worker_session_id: String,
    expires_at_unix_ms: u64,
    fence: u64,
    chunk: ValueId,
    source_copy_id: CopyId,
    placement_revision: u64,
    replica_group: ReplicaGroup,
    replication: ValueId,
}

/// A deterministic operation delta, prior to adding the log's identity/chain
/// envelope. `new` consumes the decisions made by the domain state machine;
/// `decode` checks structure before a recovery caller can apply the delta.
#[derive(Clone, Debug, Serialize)]
pub struct RecordDecision {
    schema: u16,
    request: RequestKey,
    operation: StoreOperation,
    revision_before: StoreRevision,
    revision_after: StoreRevision,
    completed_at_unix_ms: u64,
    event_index_start: u32,
    values: Vec<PooledValue>,
    changes: Vec<Change>,
    outcomes: Vec<SavedOutcome>,
    reply_outcome: u32,
    node_epochs: Vec<(String, u64)>,
}

// The serialized field order and schema remain unchanged. All four vectors
// share one budget during decoding, including when JSON is used by tests.
impl<'de> Deserialize<'de> for RecordDecision {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        const FIELDS: &[&str] = &[
            "schema",
            "request",
            "operation",
            "revision_before",
            "revision_after",
            "completed_at_unix_ms",
            "event_index_start",
            "values",
            "changes",
            "outcomes",
            "reply_outcome",
            "node_epochs",
        ];
        struct Decision;
        impl<'de> serde::de::Visitor<'de> for Decision {
            type Value = RecordDecision;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("canonical decision")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut input: A,
            ) -> std::result::Result<RecordDecision, A::Error> {
                let mut budget = RecordBudget::new();
                macro_rules! next {
                    ($field:ident) => {
                        input
                            .next_element()?
                            .ok_or_else(|| serde::de::Error::missing_field(stringify!($field)))?
                    };
                    ($field:ident, $ty:ty) => {
                        input
                            .next_element_seed(RecordItems::<$ty> {
                                budget: &mut budget,
                                marker: std::marker::PhantomData,
                            })?
                            .ok_or_else(|| serde::de::Error::missing_field(stringify!($field)))?
                    };
                }
                Ok(RecordDecision {
                    schema: next!(schema),
                    request: next!(request),
                    operation: next!(operation),
                    revision_before: next!(revision_before),
                    revision_after: next!(revision_after),
                    completed_at_unix_ms: next!(completed_at_unix_ms),
                    event_index_start: next!(event_index_start),
                    values: next!(values, PooledValue),
                    changes: next!(changes, Change),
                    outcomes: next!(outcomes, SavedOutcome),
                    reply_outcome: next!(reply_outcome),
                    node_epochs: next!(node_epochs, (String, u64)),
                })
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut input: A,
            ) -> std::result::Result<RecordDecision, A::Error> {
                let mut budget = RecordBudget::new();
                let (
                    mut schema,
                    mut request,
                    mut operation,
                    mut revision_before,
                    mut revision_after,
                    mut completed_at_unix_ms,
                    mut event_index_start,
                ) = (None, None, None, None, None, None, None);
                let (mut values, mut changes, mut outcomes, mut reply_outcome, mut node_epochs) =
                    (None, None, None, None, None);
                macro_rules! read {
                    ($field:ident) => {{
                        if $field.is_some() {
                            return Err(serde::de::Error::duplicate_field(stringify!($field)));
                        }
                        $field = Some(input.next_value()?);
                    }};
                    ($field:ident, $ty:ty) => {{
                        if $field.is_some() {
                            return Err(serde::de::Error::duplicate_field(stringify!($field)));
                        }
                        $field = Some(input.next_value_seed(RecordItems::<$ty> {
                            budget: &mut budget,
                            marker: std::marker::PhantomData,
                        })?);
                    }};
                }
                while let Some(field) = input.next_key::<String>()? {
                    match field.as_str() {
                        "schema" => read!(schema),
                        "request" => read!(request),
                        "operation" => read!(operation),
                        "revision_before" => read!(revision_before),
                        "revision_after" => read!(revision_after),
                        "completed_at_unix_ms" => read!(completed_at_unix_ms),
                        "event_index_start" => read!(event_index_start),
                        "values" => read!(values, PooledValue),
                        "changes" => read!(changes, Change),
                        "outcomes" => read!(outcomes, SavedOutcome),
                        "reply_outcome" => read!(reply_outcome),
                        "node_epochs" => read!(node_epochs, (String, u64)),
                        _ => {
                            let _: serde::de::IgnoredAny = input.next_value()?;
                        }
                    }
                }
                macro_rules! required {
                    ($field:ident) => {
                        $field.ok_or_else(|| serde::de::Error::missing_field(stringify!($field)))?
                    };
                }
                Ok(RecordDecision {
                    schema: required!(schema),
                    request: required!(request),
                    operation: required!(operation),
                    revision_before: required!(revision_before),
                    revision_after: required!(revision_after),
                    completed_at_unix_ms: required!(completed_at_unix_ms),
                    event_index_start: required!(event_index_start),
                    values: required!(values),
                    changes: required!(changes),
                    outcomes: required!(outcomes),
                    reply_outcome: required!(reply_outcome),
                    node_epochs: required!(node_epochs),
                })
            }
        }
        deserializer.deserialize_struct("RecordDecision", FIELDS, Decision)
    }
}

/// All fields here are already decided by the live authority state machine.
/// Event/result slices cover this operation only, not the full state/history.
pub struct DecisionInput<'a> {
    pub request: RequestKey,
    pub operation: StoreOperation,
    pub revision_before: StoreRevision,
    pub revision_after: StoreRevision,
    pub completed_at_unix_ms: u64,
    pub events: &'a [WatchEvent],
    pub outcomes: &'a [RequestOutcome],
    pub reply: &'a RequestOutcome,
    pub node_epochs: Vec<(String, u64)>,
}

/// Materialized original facts for applying an already validated record.
/// Event entities are reconstructed from exactly the referenced pool values.
pub struct DecidedFacts {
    pub request: RequestKey,
    pub operation: StoreOperation,
    pub revision_before: StoreRevision,
    pub revision_after: StoreRevision,
    pub completed_at_unix_ms: u64,
    pub events: Vec<WatchEvent>,
    pub outcomes: Vec<RequestOutcome>,
    pub reply: RequestOutcome,
    pub node_epochs: Vec<(String, u64)>,
}

struct Pool {
    values: Vec<PooledValue>,
    by_key: BTreeMap<(MetaKey, [u8; 32]), Vec<ValueId>>,
    by_claim: BTreeMap<(ReplicationTaskId, OperationId, [u8; 32]), Vec<ValueId>>,
    bytes: usize,
    limit: usize,
    item_limit: usize,
    record_budget: Option<RecordBudget>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
enum PooledValue {
    Entity(MetaEntity),
    ClaimedTask {
        task: ReplicationTask,
        claim: ValueId,
    },
    Claim(SavedClaim),
}
impl PooledValue {
    fn references(&self) -> Vec<ValueId> {
        match self {
            Self::Entity(_) => Vec::new(),
            Self::ClaimedTask { claim, .. } => vec![*claim],
            Self::Claim(claim) => vec![claim.chunk, claim.replication],
        }
    }
}
fn restore_claim(values: &[PooledValue], id: ValueId) -> Result<ReplicationClaim> {
    let Some(PooledValue::Claim(v)) = values.get(id.0 as usize) else {
        return Err(invalid_contract(
            "canonical claim reference has wrong type/range",
        ));
    };
    let Some(PooledValue::Entity(MetaEntity::DfsChunk(chunk))) = values.get(v.chunk.0 as usize)
    else {
        return Err(invalid_contract(
            "canonical claim chunk reference has wrong type/range",
        ));
    };
    let Some(PooledValue::Entity(MetaEntity::DfsReplicationConfig(replication))) =
        values.get(v.replication.0 as usize)
    else {
        return Err(invalid_contract(
            "canonical claim config reference has wrong type/range",
        ));
    };
    Ok(ReplicationClaim {
        task_id: v.task_id.clone(),
        operation_id: v.operation_id.clone(),
        worker_node_id: v.worker_node_id.clone(),
        worker_node_epoch: v.worker_node_epoch,
        worker_session_id: v.worker_session_id.clone(),
        expires_at_unix_ms: v.expires_at_unix_ms,
        fence: v.fence,
        chunk: chunk.clone(),
        source_copy_id: v.source_copy_id.clone(),
        placement_revision: v.placement_revision,
        replica_group: v.replica_group.clone(),
        replication: replication.clone(),
    })
}
fn restore_entity(values: &[PooledValue], id: ValueId) -> Result<MetaEntity> {
    match values.get(id.0 as usize) {
        Some(PooledValue::Entity(MetaEntity::DfsReplicationTask(task))) if task.claim.is_some() => {
            Err(invalid_contract("canonical task hides an inline claim"))
        }
        Some(PooledValue::Entity(entity)) => Ok(entity.clone()),
        Some(PooledValue::ClaimedTask { task, claim }) => {
            if task.claim.is_some() {
                return Err(invalid_contract("canonical task duplicates claim content"));
            }
            let mut task = task.clone();
            task.claim = Some(Box::new(restore_claim(values, *claim)?));
            Ok(MetaEntity::DfsReplicationTask(task))
        }
        _ => Err(invalid_contract(
            "canonical entity reference has wrong type/range",
        )),
    }
}
impl Pool {
    fn push(&mut self, value: PooledValue) -> Result<ValueId> {
        let length = codec::encoded_size(&value)?;
        self.bytes = self
            .bytes
            .checked_add(length)
            .filter(|&size| size <= self.limit)
            .ok_or_else(|| invalid_contract("canonical pool exceeds byte limit"))?;
        if self.values.len() >= self.item_limit {
            return Err(invalid_contract("canonical pool exceeds item limit"));
        }
        let id = ValueId(
            u32::try_from(self.values.len())
                .map_err(|_| invalid_contract("canonical reference overflow"))?,
        );
        if let Some(budget) = &mut self.record_budget {
            budget.reserve_slot(&mut self.values)?;
        } else {
            self.values.try_reserve(1).map_err(|_| {
                afs_error::Error::coded(afs_error::IO_OUT_OF_MEMORY, "grow canonical pool")
            })?;
        }
        self.values.push(value);
        Ok(id)
    }
    fn insert(&mut self, entity: &MetaEntity) -> Result<ValueId> {
        let key = (entity_key(entity), codec::encoded_fingerprint(entity)?);
        if let Some(ids) = self.by_key.get(&key) {
            for &id in ids {
                if restore_entity(&self.values, id)? == *entity {
                    return Ok(id);
                }
            }
        }
        if codec::encoded_size(entity)? > self.limit {
            return Err(invalid_contract("canonical entity exceeds byte limit"));
        }
        let value = match entity {
            MetaEntity::DfsReplicationTask(task) if task.claim.is_some() => {
                let mut task = task.clone();
                let claim = task
                    .claim
                    .take()
                    .ok_or_else(|| invalid_contract("missing task claim"))?;
                PooledValue::ClaimedTask {
                    task,
                    claim: self.insert_claim(&claim)?,
                }
            }
            _ => PooledValue::Entity(entity.clone()),
        };
        let id = self.push(value)?;
        self.by_key.entry(key).or_default().push(id);
        Ok(id)
    }
    fn insert_claim(&mut self, claim: &ReplicationClaim) -> Result<ValueId> {
        let key = (
            claim.task_id.clone(),
            claim.operation_id.clone(),
            codec::encoded_fingerprint(claim)?,
        );
        if let Some(ids) = self.by_claim.get(&key) {
            for &id in ids {
                if restore_claim(&self.values, id)? == *claim {
                    return Ok(id);
                }
            }
        }
        let saved = SavedClaim {
            task_id: claim.task_id.clone(),
            operation_id: claim.operation_id.clone(),
            worker_node_id: claim.worker_node_id.clone(),
            worker_node_epoch: claim.worker_node_epoch,
            worker_session_id: claim.worker_session_id.clone(),
            expires_at_unix_ms: claim.expires_at_unix_ms,
            fence: claim.fence,
            chunk: self.insert(&MetaEntity::DfsChunk(claim.chunk.clone()))?,
            source_copy_id: claim.source_copy_id.clone(),
            placement_revision: claim.placement_revision,
            replica_group: claim.replica_group.clone(),
            replication: self
                .insert(&MetaEntity::DfsReplicationConfig(claim.replication.clone()))?,
        };
        let id = self.push(PooledValue::Claim(saved))?;
        self.by_claim.entry(key).or_default().push(id);
        Ok(id)
    }
}

impl SavedOutcome {
    fn references(&self) -> Vec<ValueId> {
        match &self.result {
            SavedResult::NodeSession(id)
            | SavedResult::RootReservation(id)
            | SavedResult::RootRecord(id)
            | SavedResult::RootGrant(id)
            | SavedResult::RootCommand(id)
            | SavedResult::RootCommandAck(id)
            | SavedResult::DfsInode(id)
            | SavedResult::DfsWriteLease(id)
            | SavedResult::DfsReplicationTask(id) => vec![*id],
            SavedResult::DfsInodeWithLease { inode, lease } => vec![*inode, *lease],
            SavedResult::DfsRename {
                inode,
                replaced_inode,
            } => std::iter::once(*inode).chain(*replaced_inode).collect(),
            SavedResult::DfsReplicationClaim { claim, .. } => {
                claim.as_ref().map_or_else(Vec::new, |claim| vec![*claim])
            }
            SavedResult::Empty => Vec::new(),
        }
    }

    fn save(outcome: &RequestOutcome, pool: &mut Pool) -> Result<Self> {
        let mut result = &outcome.result;
        let mut namespace_digests = Vec::new();
        while let OperationResult::DfsNamespace {
            request_digest,
            result: inner,
        } = result
        {
            if namespace_digests.len() == MAX_NESTING {
                return Err(invalid_contract("canonical outcome nesting exceeds limit"));
            }
            namespace_digests.push(*request_digest);
            result = inner;
        }
        let result = match result {
            OperationResult::DfsNamespace { .. } => {
                return Err(invalid_contract("unflattened outcome"));
            }
            OperationResult::NodeSession(v) => {
                SavedResult::NodeSession(pool.insert(&MetaEntity::NodeSession(v.clone()))?)
            }
            OperationResult::RootReservation(v) => {
                SavedResult::RootReservation(pool.insert(&MetaEntity::RootReservation(v.clone()))?)
            }
            OperationResult::RootRecord(v) => {
                SavedResult::RootRecord(pool.insert(&MetaEntity::Root(v.clone()))?)
            }
            OperationResult::RootGrant(v) => {
                SavedResult::RootGrant(pool.insert(&MetaEntity::RootGrant(v.clone()))?)
            }
            OperationResult::RootCommand(v) => {
                SavedResult::RootCommand(pool.insert(&MetaEntity::RootCommand(v.clone()))?)
            }
            OperationResult::RootCommandAck(v) => {
                SavedResult::RootCommandAck(pool.insert(&MetaEntity::RootCommandAck(v.clone()))?)
            }
            OperationResult::DfsInode(v) => {
                SavedResult::DfsInode(pool.insert(&MetaEntity::DfsInode(v.clone()))?)
            }
            OperationResult::DfsInodeWithLease { inode, lease } => SavedResult::DfsInodeWithLease {
                inode: pool.insert(&MetaEntity::DfsInode(inode.clone()))?,
                lease: pool.insert(&MetaEntity::DfsWriteLease(lease.clone()))?,
            },
            OperationResult::DfsWriteLease(v) => {
                SavedResult::DfsWriteLease(pool.insert(&MetaEntity::DfsWriteLease(v.clone()))?)
            }
            OperationResult::DfsReplicationClaim {
                request_digest,
                claim,
            } => SavedResult::DfsReplicationClaim {
                request_digest: *request_digest,
                claim: claim
                    .as_ref()
                    .map(|value| pool.insert_claim(value))
                    .transpose()?,
            },
            OperationResult::DfsReplicationTask(v) => SavedResult::DfsReplicationTask(
                pool.insert(&MetaEntity::DfsReplicationTask(v.clone()))?,
            ),
            OperationResult::DfsRename(v) => SavedResult::DfsRename {
                inode: pool.insert(&MetaEntity::DfsInode(v.inode.clone()))?,
                replaced_inode: v
                    .replaced_inode
                    .as_ref()
                    .map(|v| pool.insert(&MetaEntity::DfsInode(v.clone())))
                    .transpose()?,
            },
            OperationResult::Empty => SavedResult::Empty,
        };
        Ok(Self {
            request: outcome.request.clone(),
            operation: outcome.operation,
            namespace_digests,
            result,
        })
    }

    fn restore(&self, values: &[PooledValue]) -> Result<RequestOutcome> {
        macro_rules! value {
            ($id:expr, $variant:ident) => {
                match restore_entity(values, *$id)? {
                    MetaEntity::$variant(value) => value,
                    _ => {
                        return Err(invalid_contract(
                            "canonical outcome reference has wrong entity type",
                        ))
                    }
                }
            };
        }
        let mut result = match &self.result {
            SavedResult::NodeSession(id) => OperationResult::NodeSession(value!(id, NodeSession)),
            SavedResult::RootReservation(id) => {
                OperationResult::RootReservation(value!(id, RootReservation))
            }
            SavedResult::RootRecord(id) => OperationResult::RootRecord(value!(id, Root)),
            SavedResult::RootGrant(id) => OperationResult::RootGrant(value!(id, RootGrant)),
            SavedResult::RootCommand(id) => OperationResult::RootCommand(value!(id, RootCommand)),
            SavedResult::RootCommandAck(id) => {
                OperationResult::RootCommandAck(value!(id, RootCommandAck))
            }
            SavedResult::DfsInode(id) => OperationResult::DfsInode(value!(id, DfsInode)),
            SavedResult::DfsInodeWithLease { inode, lease } => OperationResult::DfsInodeWithLease {
                inode: value!(inode, DfsInode),
                lease: value!(lease, DfsWriteLease),
            },
            SavedResult::DfsWriteLease(id) => {
                OperationResult::DfsWriteLease(value!(id, DfsWriteLease))
            }
            SavedResult::DfsReplicationTask(id) => {
                OperationResult::DfsReplicationTask(value!(id, DfsReplicationTask))
            }
            SavedResult::DfsRename {
                inode,
                replaced_inode,
            } => OperationResult::DfsRename(RenameOutcome {
                inode: value!(inode, DfsInode),
                replaced_inode: replaced_inode
                    .as_ref()
                    .map(|id| -> Result<_> { Ok(value!(id, DfsInode)) })
                    .transpose()?,
            }),
            SavedResult::DfsReplicationClaim {
                request_digest,
                claim,
            } => OperationResult::DfsReplicationClaim {
                request_digest: *request_digest,
                claim: claim
                    .as_ref()
                    .map(|id| restore_claim(values, *id))
                    .transpose()?,
            },
            SavedResult::Empty => OperationResult::Empty,
        };
        if self.namespace_digests.len() > MAX_NESTING {
            return Err(invalid_contract(
                "canonical namespace digest depth exceeds limit",
            ));
        }
        for &request_digest in self.namespace_digests.iter().rev() {
            result = OperationResult::DfsNamespace {
                request_digest,
                result: Box::new(result),
            };
        }
        Ok(RequestOutcome {
            request: self.request.clone(),
            operation: self.operation,
            result,
        })
    }
}

/// One physical record. Management changes do not invent a user request or a
/// business revision. Associated retention is atomic with its business record.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Record {
    Decision {
        decision: RecordDecision,
        history: Option<super::history::Delta>,
    },
    History(super::history::Delta),
}
impl From<RecordDecision> for Record {
    fn from(decision: RecordDecision) -> Self {
        Self::Decision {
            decision,
            history: None,
        }
    }
}
impl Record {
    pub(super) fn validate(&self) -> Result<()> {
        match self {
            Self::Decision { decision, history } => {
                decision.validate()?;
                if let Some(history) = history {
                    history.validate()?;
                    if history.revision != decision.revision_after() {
                        return Err(invalid_contract("associated retention revision mismatch"));
                    }
                }
                Ok(())
            }
            Self::History(history) => history.validate(),
        }
    }
    pub(super) fn revision_before(&self) -> StoreRevision {
        match self {
            Self::Decision { decision, .. } => decision.revision_before(),
            Self::History(history) => history.revision,
        }
    }
    pub(super) fn revision_after(&self) -> StoreRevision {
        match self {
            Self::Decision { decision, .. } => decision.revision_after(),
            Self::History(history) => history.revision,
        }
    }
}

impl RecordDecision {
    /// Captures only the newly decided events/results. It never traverses the
    /// complete state. `node_epochs` contains only explicit high-water changes.
    /// Oversize or inconsistent input is rejected before any durable append.
    pub fn new(input: DecisionInput<'_>, byte_limit: usize) -> Result<Self> {
        let DecisionInput {
            request,
            operation,
            revision_before,
            revision_after,
            completed_at_unix_ms,
            events,
            outcomes,
            reply,
            node_epochs,
        } = input;
        let mut budget = RecordBudget::new();
        budget.charge::<(String, u64)>(RecordBudget::capacity(node_epochs.capacity())?)?;
        let mut changes = Vec::new();
        budget.reserve(&mut changes, events.len())?;
        let mut saved = Vec::new();
        budget.reserve(&mut saved, outcomes.len())?;
        let mut pool = Pool {
            values: Vec::new(),
            by_key: BTreeMap::new(),
            by_claim: BTreeMap::new(),
            bytes: 0,
            limit: byte_limit,
            item_limit: RECORD_ITEMS,
            record_budget: Some(budget),
        };
        let event_index_start = events.first().map_or(0, |event| event.event_index);
        for (offset, event) in events.iter().enumerate() {
            let index = u32::try_from(offset)
                .ok()
                .and_then(|offset| event_index_start.checked_add(offset));
            if event.revision != revision_after || index != Some(event.event_index) {
                return Err(invalid_contract("canonical event revision/index mismatch"));
            }
            changes.push(match &event.change {
                WatchChange::Put(entity) => {
                    if !matches_key(&event.key, entity) {
                        return Err(invalid_contract("canonical put key/value mismatch"));
                    }
                    Change::Put {
                        key: event.key.clone(),
                        value: pool.insert(entity)?,
                    }
                }
                WatchChange::Delete => Change::Delete {
                    key: event.key.clone(),
                },
            });
        }
        let reply_outcome = outcomes
            .iter()
            .position(|outcome| outcome == reply)
            .and_then(|index| u32::try_from(index).ok())
            .ok_or_else(|| {
                invalid_contract("canonical reply must reference an original saved outcome")
            })?;
        for outcome in outcomes {
            saved.push(SavedOutcome::save(outcome, &mut pool)?);
        }
        let record = Self {
            schema: SCHEMA,
            request,
            operation,
            revision_before,
            revision_after,
            completed_at_unix_ms,
            event_index_start,
            values: pool.values,
            changes,
            outcomes: saved,
            reply_outcome,
            node_epochs,
        };
        record.validate()?;
        // Count all envelope/descriptor overhead, not only entity payloads.
        let length = codec::encoded_size(&record)?;
        if length > byte_limit {
            return Err(invalid_contract("canonical decision exceeds byte limit"));
        }
        Ok(record)
    }

    pub fn encode(&self, byte_limit: usize) -> Result<Vec<u8>> {
        self.validate()?;
        codec::encode_bounded(self, byte_limit)
    }

    pub fn decode(bytes: &[u8], byte_limit: usize) -> Result<Self> {
        if bytes.len() > byte_limit {
            return Err(invalid_contract("canonical decision exceeds byte limit"));
        }
        let (record, tail): (Self, _) = postcard::take_from_bytes(bytes)
            .map_err(|e| invalid_contract(format!("decode canonical decision: {e}")))?;
        if !tail.is_empty() {
            return Err(invalid_contract("trailing canonical decision bytes"));
        }
        record.validate()?;
        Ok(record)
    }

    fn entity(&self, id: ValueId) -> Result<MetaEntity> {
        restore_entity(&self.values, id)
    }

    pub(super) fn revision_before(&self) -> StoreRevision {
        self.revision_before
    }
    pub(super) fn revision_after(&self) -> StoreRevision {
        self.revision_after
    }

    pub(super) fn validate(&self) -> Result<()> {
        let mut budget = RecordBudget::new();
        budget.charge::<PooledValue>(RecordBudget::capacity(self.values.capacity())?)?;
        budget.charge::<Change>(RecordBudget::capacity(self.changes.capacity())?)?;
        budget.charge::<SavedOutcome>(RecordBudget::capacity(self.outcomes.capacity())?)?;
        budget.charge::<(String, u64)>(RecordBudget::capacity(self.node_epochs.capacity())?)?;
        if self.schema != SCHEMA
            || !self.request.is_valid()
            || self.revision_before.0.checked_add(1) != Some(self.revision_after.0)
            || self.reply_outcome as usize >= self.outcomes.len()
        {
            return Err(invalid_contract(
                "invalid canonical schema/identity/revision/length",
            ));
        }
        for (index, value) in self.values.iter().enumerate() {
            if value.references().iter().any(|id| id.0 as usize >= index) {
                return Err(invalid_contract(
                    "canonical value graph has a forward/cyclic reference",
                ));
            }
            match value {
                PooledValue::Claim(_) => {
                    restore_claim(&self.values, ValueId::at(index)?)?;
                }
                _ => {
                    self.entity(ValueId::at(index)?)?;
                }
            }
        }
        let mut epochs = BTreeMap::new();
        for (node, epoch) in &self.node_epochs {
            if node.is_empty() || *epoch == 0 || epochs.insert(node, epoch).is_some() {
                return Err(invalid_contract("invalid canonical epoch changes"));
            }
        }
        for change in &self.changes {
            if let Change::Put { key, value } = change
                && !matches_key(key, &self.entity(*value)?)
            {
                return Err(invalid_contract(
                    "canonical put reference key/value mismatch",
                ));
            }
        }
        if !self
            .outcomes
            .iter()
            .any(|outcome| outcome.request == self.request && outcome.operation == self.operation)
        {
            return Err(invalid_contract(
                "canonical decision lacks matching operation outcome",
            ));
        }
        for outcome in &self.outcomes {
            if !outcome.request.is_valid() {
                return Err(invalid_contract("invalid canonical outcome identity"));
            }
            outcome.restore(&self.values)?;
        }
        let mut referenced = vec![false; self.values.len()];
        for value in self
            .changes
            .iter()
            .filter_map(|change| match change {
                Change::Put { value, .. } => Some(*value),
                Change::Delete { .. } => None,
            })
            .chain(self.outcomes.iter().flat_map(SavedOutcome::references))
            .chain(self.values.iter().flat_map(PooledValue::references))
        {
            let slot = referenced
                .get_mut(value.0 as usize)
                .ok_or_else(|| invalid_contract("canonical reference out of range"))?;
            *slot = true;
        }
        if referenced.iter().any(|used| !used) {
            return Err(invalid_contract(
                "canonical value pool contains an unreferenced entity",
            ));
        }
        let mut unique: BTreeMap<MetaKey, Vec<MetaEntity>> = BTreeMap::new();
        let mut claims: BTreeMap<(&ReplicationTaskId, &OperationId), Vec<&SavedClaim>> =
            BTreeMap::new();
        for (index, value) in self.values.iter().enumerate() {
            if let PooledValue::Claim(claim) = value {
                let previous = claims
                    .entry((&claim.task_id, &claim.operation_id))
                    .or_default();
                if previous.contains(&claim) {
                    return Err(invalid_contract("canonical pool duplicates claim content"));
                }
                previous.push(claim);
            } else {
                let entity = self.entity(ValueId::at(index)?)?;
                let previous = unique.entry(entity_key(&entity)).or_default();
                if previous.contains(&entity) {
                    return Err(invalid_contract("canonical pool duplicates entity content"));
                }
                previous.push(entity);
            }
        }
        if self.changes.is_empty() && self.event_index_start != 0 {
            return Err(invalid_contract(
                "canonical empty changes have nonzero event index",
            ));
        }
        let last_offset = self.changes.len().saturating_sub(1);
        if u32::try_from(last_offset)
            .ok()
            .and_then(|offset| self.event_index_start.checked_add(offset))
            .is_none()
        {
            return Err(invalid_contract("canonical event index overflow"));
        }
        Ok(())
    }

    /// Derives events and original responses; it does not consult current
    /// entities, the wall clock, Node, S3 or any other external service.
    pub fn facts(&self) -> Result<DecidedFacts> {
        self.validate()?;
        let events = self
            .changes
            .iter()
            .enumerate()
            .map(|(offset, change)| {
                let (key, change) = match change {
                    Change::Put { key, value } => {
                        (key.clone(), WatchChange::Put(self.entity(*value)?))
                    }
                    Change::Delete { key } => (key.clone(), WatchChange::Delete),
                };
                let offset =
                    u32::try_from(offset).map_err(|_| invalid_contract("event offset overflow"))?;
                let event_index = self
                    .event_index_start
                    .checked_add(offset)
                    .ok_or_else(|| invalid_contract("event index overflow"))?;
                Ok(WatchEvent {
                    revision: self.revision_after,
                    event_index,
                    key,
                    change,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let outcomes = self
            .outcomes
            .iter()
            .map(|outcome| outcome.restore(&self.values))
            .collect::<Result<Vec<_>>>()?;
        Ok(DecidedFacts {
            request: self.request.clone(),
            operation: self.operation,
            revision_before: self.revision_before,
            revision_after: self.revision_after,
            completed_at_unix_ms: self.completed_at_unix_ms,
            events,
            reply: outcomes[self.reply_outcome as usize].clone(),
            outcomes,
            node_epochs: self.node_epochs.clone(),
        })
    }
}

fn matches_key(key: &MetaKey, entity: &MetaEntity) -> bool {
    *key == entity_key(entity)
        || matches!((key, entity),
        (MetaKey::CurrentNodeSession { node_id }, MetaEntity::NodeSession(session)) if node_id == &session.node_id)
}

// This is a whole-state bound, separate from the operation's shared budget.
// Encoded bytes are independently bounded by the checkpoint caller. No count
// hint can cause a full-size allocation before actual items are decoded.
const MAX_IMAGE_ITEMS: usize = 256 * 1024 * 1024;
fn image_items<'de, D, T>(deserializer: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Items<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Items<T> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "bounded checkpoint items")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut input: A,
        ) -> std::result::Result<Vec<T>, A::Error> {
            if input.size_hint().is_some_and(|size| size > MAX_IMAGE_ITEMS) {
                return Err(serde::de::Error::custom(
                    "checkpoint item count exceeds limit",
                ));
            }
            let mut values = Vec::new();
            while let Some(value) = input.next_element()? {
                if values.len() == MAX_IMAGE_ITEMS {
                    return Err(serde::de::Error::custom(
                        "checkpoint item count exceeds limit",
                    ));
                }
                values
                    .try_reserve(1)
                    .map_err(|_| serde::de::Error::custom("allocate checkpoint items"))?;
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Items(std::marker::PhantomData))
}
#[derive(Deserialize, Serialize)]
struct ImageEntity {
    key: MetaKey,
    revision: StoreRevision,
    value: ValueId,
}
#[derive(Deserialize, Serialize)]
struct ImageEvent {
    revision: StoreRevision,
    event_index: u32,
    change: Change,
}
#[derive(Deserialize, Serialize)]
struct ImageRequest {
    outcome: SavedOutcome,
    completion: super::state::RequestCompletion,
}
/// A self-contained canonical state image, independent of retained WAL records.
/// Current values, historical original results and events share one value pool.
#[derive(Deserialize, Serialize)]
pub(super) struct StateImage {
    magic: [u8; 4],
    schema: u16,
    revision: StoreRevision,
    compacted_to: StoreRevision,
    events_removed: u64,
    #[serde(deserialize_with = "image_items")]
    event_completion: Vec<(StoreRevision, u64)>,
    #[serde(deserialize_with = "image_items")]
    values: Vec<PooledValue>,
    #[serde(deserialize_with = "image_items")]
    entities: Vec<ImageEntity>,
    #[serde(deserialize_with = "image_items")]
    requests: Vec<ImageRequest>,
    #[serde(deserialize_with = "image_items")]
    events: Vec<ImageEvent>,
    #[serde(deserialize_with = "image_items")]
    node_epochs: Vec<(String, u64)>,
}
impl StateImage {
    pub(super) fn capture_charge(state: &super::state::State) -> Result<usize> {
        let mut bytes = 0usize;
        let mut add = |size: usize| -> Result<()> {
            bytes = bytes
                .checked_add(size)
                .ok_or_else(|| invalid_contract("checkpoint capture charge overflow"))?;
            Ok(())
        };
        for entity in state.entities.values() {
            add(codec::encoded_size(&entity.entity)?)?;
        }
        for outcome in state.requests.values() {
            add(codec::encoded_size(outcome)?)?;
        }
        for event in state.events.iter() {
            add(codec::encoded_size(event)?)?;
        }
        let items = [
            state.entities.len(),
            state.requests.len(),
            state.events.len(),
            state.event_completion.len(),
            state.node_epochs.len(),
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .ok_or_else(|| invalid_contract("checkpoint item charge overflow"))?;
        let command_index = usize::try_from(state.command_index_charge()?)
            .map_err(|_| invalid_contract("checkpoint command index charge overflow"))?;
        bytes
            .checked_mul(4)
            .and_then(|size| size.checked_add(command_index))
            .and_then(|size| {
                items
                    .checked_mul(512)
                    .and_then(|items| size.checked_add(items))
            })
            .ok_or_else(|| invalid_contract("checkpoint capture charge overflow"))
    }
    pub(super) fn validate_encoded(bytes: &[u8], limit: usize) -> Result<StoreRevision> {
        let image = Self::parse(bytes, limit)?;
        image.validate()?;
        Ok(image.revision)
    }
    fn parse(bytes: &[u8], limit: usize) -> Result<Self> {
        if bytes.len() > limit {
            return Err(invalid_contract("checkpoint exceeds encoded byte limit"));
        }
        let (image, tail): (Self, _) = postcard::take_from_bytes(bytes)
            .map_err(|e| invalid_contract(format!("decode checkpoint: {e}")))?;
        if !tail.is_empty() {
            return Err(invalid_contract("checkpoint trailing bytes"));
        }
        Ok(image)
    }
    fn capture(state: &super::state::State, limit: usize) -> Result<Self> {
        let counts = [
            state.entities.len(),
            state.requests.len(),
            state.events.len(),
            state.event_completion.len(),
            state.node_epochs.len(),
        ];
        if counts.into_iter().any(|n| n > MAX_IMAGE_ITEMS)
            || counts
                .into_iter()
                .try_fold(0usize, usize::checked_add)
                .is_none_or(|minimum_bytes| minimum_bytes > limit)
            || state.request_completion.len() != state.requests.len()
        {
            return Err(invalid_contract(
                "checkpoint counts or completion coverage invalid",
            ));
        }
        let mut pool = Pool {
            values: Vec::new(),
            by_key: BTreeMap::new(),
            by_claim: BTreeMap::new(),
            bytes: 0,
            limit,
            item_limit: MAX_IMAGE_ITEMS,
            record_budget: None,
        };
        let mut entities = Vec::new();
        for (key, entity) in state.entities.iter() {
            entities.push(ImageEntity {
                key: key.clone(),
                revision: entity.revision,
                value: pool.insert(&entity.entity)?,
            });
        }
        let mut requests = Vec::new();
        for (key, outcome) in state.requests.iter() {
            if key != &outcome.request {
                return Err(invalid_contract("checkpoint request key mismatch"));
            }
            let completion = state
                .request_completion
                .get(key)
                .cloned()
                .ok_or_else(|| invalid_contract("checkpoint completion missing"))?;
            requests.push(ImageRequest {
                outcome: SavedOutcome::save(outcome, &mut pool)?,
                completion,
            });
        }
        let mut events = Vec::new();
        for event in state.events.iter() {
            let change = match &event.change {
                WatchChange::Put(entity) => Change::Put {
                    key: event.key.clone(),
                    value: pool.insert(entity)?,
                },
                WatchChange::Delete => Change::Delete {
                    key: event.key.clone(),
                },
            };
            events.push(ImageEvent {
                revision: event.revision,
                event_index: event.event_index,
                change,
            });
        }
        let image = Self {
            magic: *b"AFSC",
            schema: IMAGE_SCHEMA,
            revision: state.revision,
            compacted_to: state.compacted_to,
            events_removed: state.events_removed,
            event_completion: state
                .event_completion
                .iter()
                .map(|(revision, time)| (*revision, *time))
                .collect(),
            values: pool.values,
            entities,
            requests,
            events,
            node_epochs: state
                .node_epochs
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
        };
        if codec::encoded_size(&image)? > limit {
            return Err(invalid_contract("checkpoint exceeds encoded byte limit"));
        }
        Ok(image)
    }
    pub(super) fn encode(state: &super::state::State, limit: usize) -> Result<Vec<u8>> {
        let image = Self::capture(state, limit)?;
        image.validate()?;
        codec::encode_bounded(&image, limit)
    }
    pub(super) fn decode(bytes: &[u8], limit: usize) -> Result<super::state::State> {
        Self::parse(bytes, limit)?.restore()
    }
    fn validate(&self) -> Result<()> {
        if self.magic != *b"AFSC"
            || self.schema != IMAGE_SCHEMA
            || self.compacted_to > self.revision
            || [
                self.values.len(),
                self.entities.len(),
                self.requests.len(),
                self.events.len(),
                self.node_epochs.len(),
            ]
            .into_iter()
            .any(|n| n > MAX_IMAGE_ITEMS)
        {
            return Err(invalid_contract("checkpoint schema/count/revision invalid"));
        }
        if self.event_completion.len() > MAX_IMAGE_ITEMS
            || self
                .events_removed
                .checked_add(self.events.len() as u64)
                .is_none()
            || (self.compacted_to == StoreRevision::ZERO && self.events_removed != 0)
        {
            return Err(invalid_contract("checkpoint event ordinal bound invalid"));
        }
        let mut previous_group = None;
        let mut group_count = 0usize;
        for event in &self.events {
            if previous_group != Some(event.revision) {
                group_count += 1;
                previous_group = Some(event.revision);
            }
        }
        if group_count != self.event_completion.len() {
            return Err(invalid_contract(
                "checkpoint event original completion coverage missing",
            ));
        }
        let mut previous_completion = StoreRevision::ZERO;
        for (revision, _) in &self.event_completion {
            if *revision <= previous_completion
                || *revision <= self.compacted_to
                || *revision > self.revision
                || self
                    .events
                    .binary_search_by_key(revision, |event| event.revision)
                    .is_err()
            {
                return Err(invalid_contract(
                    "checkpoint event completion coverage invalid",
                ));
            }
            previous_completion = *revision;
        }
        let mut unique: BTreeMap<[u8; 32], Vec<usize>> = BTreeMap::new();
        for (index, value) in self.values.iter().enumerate() {
            if value.references().iter().any(|id| id.0 as usize >= index) {
                return Err(invalid_contract("checkpoint forward/cyclic pool reference"));
            }
            match value {
                PooledValue::Claim(_) => {
                    restore_claim(&self.values, ValueId::at(index)?)?;
                }
                _ => {
                    restore_entity(&self.values, ValueId::at(index)?)?;
                }
            }
            let digest = codec::encoded_fingerprint(value)?;
            let previous = unique.entry(digest).or_default();
            if previous.iter().any(|&id| self.values[id] == *value) {
                return Err(invalid_contract("checkpoint duplicate content in pool"));
            }
            previous.push(index);
        }
        let mut referenced = vec![false; self.values.len()];
        let mut mark = |id: ValueId| -> Result<()> {
            *referenced
                .get_mut(id.0 as usize)
                .ok_or_else(|| invalid_contract("checkpoint reference out of range"))? = true;
            Ok(())
        };
        let mut previous_key = None;
        for entry in &self.entities {
            if previous_key.is_some_and(|previous: &MetaKey| previous >= &entry.key)
                || entry.revision == StoreRevision::ZERO
                || entry.revision > self.revision
            {
                return Err(invalid_contract("checkpoint entity order/revision invalid"));
            }
            if !self.matches_key(&entry.key, entry.value) {
                return Err(invalid_contract("checkpoint entity key/value mismatch"));
            }
            mark(entry.value)?;
            previous_key = Some(&entry.key);
        }
        let mut previous_request = None;
        for entry in &self.requests {
            let key = &entry.outcome.request;
            if !key.is_valid()
                || previous_request.is_some_and(|previous: &RequestKey| previous >= key)
                || entry.completion.revision == StoreRevision::ZERO
                || entry.completion.revision > self.revision
            {
                return Err(invalid_contract(
                    "checkpoint outcome order/completion invalid",
                ));
            }
            entry.outcome.restore(&self.values)?;
            for id in entry.outcome.references() {
                mark(id)?;
            }
            previous_request = Some(key);
        }
        let mut previous_event = None;
        let mut latest: BTreeMap<MetaKey, &ImageEvent> = BTreeMap::new();
        for event in &self.events {
            if event.revision <= self.compacted_to
                || event.revision > self.revision
                || previous_event.is_some_and(|(revision, index): (StoreRevision, u32)| {
                    event.revision < revision
                        || (event.revision == revision
                            && index.checked_add(1) != Some(event.event_index))
                })
            {
                return Err(invalid_contract("checkpoint event order/revision invalid"));
            }
            let key = match &event.change {
                Change::Put { key, value } => {
                    if !self.matches_key(key, *value) {
                        return Err(invalid_contract("checkpoint event key/value mismatch"));
                    }
                    mark(*value)?;
                    key
                }
                Change::Delete { key } => key,
            };
            latest.insert(key.clone(), event);
            previous_event = Some((event.revision, event.event_index));
        }
        for (key, event) in latest {
            match &event.change {
                Change::Put { value, .. } => {
                    let position = self
                        .entities
                        .binary_search_by(|entry| entry.key.cmp(&key))
                        .map_err(|_| {
                            invalid_contract("checkpoint latest put missing from authority")
                        })?;
                    let actual = &self.entities[position];
                    if actual.revision != event.revision || actual.value != *value {
                        return Err(invalid_contract(
                            "checkpoint latest event disagrees with authority",
                        ));
                    }
                }
                Change::Delete { .. }
                    if self
                        .entities
                        .binary_search_by(|entry| entry.key.cmp(&key))
                        .is_ok() =>
                {
                    return Err(invalid_contract(
                        "checkpoint latest delete disagrees with authority",
                    ));
                }
                Change::Delete { .. } => {}
            }
        }
        let mut previous_node = None;
        for (node, epoch) in &self.node_epochs {
            if node.is_empty()
                || *epoch == 0
                || previous_node.is_some_and(|previous: &String| previous >= node)
            {
                return Err(invalid_contract(
                    "checkpoint node epoch order/value invalid",
                ));
            }
            previous_node = Some(node);
        }
        for entry in &self.entities {
            if let Some(PooledValue::Entity(MetaEntity::NodeSession(session))) =
                self.values.get(entry.value.0 as usize)
            {
                let epoch = self
                    .node_epochs
                    .binary_search_by(|(node, _)| node.cmp(&session.node_id))
                    .ok()
                    .map(|position| self.node_epochs[position].1);
                if session.lease_epoch == 0 || epoch.is_none_or(|epoch| epoch < session.lease_epoch)
                {
                    return Err(invalid_contract("checkpoint session exceeds node epoch"));
                }
                if matches!(entry.key, MetaKey::CurrentNodeSession { .. }) {
                    let session_key = MetaKey::NodeSession {
                        node_id: session.node_id.clone(),
                        session_id: session.session_id.clone(),
                    };
                    let same_session = self
                        .entities
                        .binary_search_by(|candidate| candidate.key.cmp(&session_key))
                        .ok()
                        .is_some_and(|position| self.entities[position].value == entry.value);
                    if !same_session || epoch != Some(session.lease_epoch) {
                        return Err(invalid_contract(
                            "checkpoint current session alias/epoch mismatch",
                        ));
                    }
                }
            }
        }
        for value in &self.values {
            for id in value.references() {
                mark(id)?;
            }
        }
        if referenced.iter().any(|used| !used) {
            return Err(invalid_contract(
                "checkpoint contains unreachable pooled content",
            ));
        }
        Ok(())
    }
    fn matches_key(&self, key: &MetaKey, value: ValueId) -> bool {
        match self.values.get(value.0 as usize) {
            Some(PooledValue::Entity(entity)) => matches_key(key, entity),
            Some(PooledValue::ClaimedTask { task, .. }) => {
                *key == MetaKey::DfsReplicationTask(task.id.clone())
            }
            _ => false,
        }
    }
    fn restore(&self) -> Result<super::state::State> {
        use super::state::{State, VersionedEntity};
        self.validate()?;
        let mut state = State {
            revision: self.revision,
            compacted_to: self.compacted_to,
            events_removed: self.events_removed,
            ..State::default()
        };
        for entry in &self.entities {
            state.entities.insert(
                entry.key.clone(),
                VersionedEntity {
                    revision: entry.revision,
                    entity: restore_entity(&self.values, entry.value)?,
                },
            );
        }
        for entry in &self.requests {
            state.requests.insert(
                entry.outcome.request.clone(),
                entry.outcome.restore(&self.values)?,
            );
            state
                .request_completion
                .insert(entry.outcome.request.clone(), entry.completion.clone());
        }
        for event in &self.events {
            let (key, change) = match &event.change {
                Change::Put { key, value } => (
                    key.clone(),
                    WatchChange::Put(restore_entity(&self.values, *value)?),
                ),
                Change::Delete { key } => (key.clone(), WatchChange::Delete),
            };
            state.events.push(WatchEvent {
                revision: event.revision,
                event_index: event.event_index,
                key,
                change,
            });
        }
        state
            .event_completion
            .extend(self.event_completion.iter().copied());
        state.node_epochs.extend(self.node_epochs.iter().cloned());
        state.rebuild_history_index()?;
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::{Change, DecisionInput, MAX_ITEMS, RecordDecision, SavedResult, ValueId};
    use crate::dfs::{InodeId, RenameOutcome, ReplicaGroup, ReplicaGroupId, ReplicationClaim};
    use crate::meta::store::{
        MetaEntity, MetaKey, MetaRead, MetaTxn, NodeSessionLease, OperationResult, RequestKey,
        RequestOutcome, StoreOperation, StoreRevision, TxnCondition, TxnMutation, TxnOutcome,
        WatchChange, WatchEvent, codec, entity_key, state::State,
    };
    use std::time::Duration;

    const LIMIT: usize = 8 * 1024 * 1024;
    #[test]
    fn p3_record_vector_budget_preserves_both_original_combined_ceilings() {
        let mut budget = super::RecordBudget::new();
        budget.charge::<super::PooledValue>(MAX_ITEMS).unwrap();
        budget.charge::<Change>(MAX_ITEMS).unwrap();
        budget.charge::<super::SavedOutcome>(MAX_ITEMS).unwrap();
        budget.charge::<(String, u64)>(MAX_ITEMS).unwrap();
        assert_eq!((budget.items, budget.bytes), (0, 0));
        assert!(budget.charge::<Change>(1).is_err());
        // A vector can borrow unused slots without increasing either total.
        let mut budget = super::RecordBudget::new();
        budget.charge::<Change>(MAX_ITEMS + 3).unwrap();
        budget.charge::<super::PooledValue>(1).unwrap();
        budget.charge::<super::SavedOutcome>(1).unwrap();
        assert!(budget.items < super::RECORD_ITEMS);
        assert!(budget.bytes < super::RECORD_VECTOR_BYTES);
        eprintln!(
            "P3_RECORD_BUDGET slots={} vector_bytes={} change_width={} pooled_width={} outcome_width={} epoch_width={}",
            super::RECORD_ITEMS,
            super::RECORD_VECTOR_BYTES,
            std::mem::size_of::<Change>(),
            std::mem::size_of::<super::PooledValue>(),
            std::mem::size_of::<super::SavedOutcome>(),
            std::mem::size_of::<(String, u64)>()
        );
    }

    #[test]
    fn p3_record_decode_rejects_slot_and_byte_hints_before_allocating_items() {
        use serde::de::DeserializeSeed;
        struct CountOnly(usize);
        impl<'de> serde::de::SeqAccess<'de> for CountOnly {
            type Error = serde::de::value::Error;
            fn next_element_seed<T: DeserializeSeed<'de>>(
                &mut self,
                _seed: T,
            ) -> std::result::Result<Option<T::Value>, Self::Error> {
                panic!("over-budget count must be rejected before reading an item")
            }
            fn size_hint(&self) -> Option<usize> {
                Some(self.0)
            }
        }
        impl<'de> serde::Deserializer<'de> for CountOnly {
            type Error = serde::de::value::Error;
            fn deserialize_any<V: serde::de::Visitor<'de>>(
                self,
                visitor: V,
            ) -> std::result::Result<V::Value, Self::Error> {
                visitor.visit_seq(self)
            }
            serde::forward_to_deserialize_any! { bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf option unit unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier ignored_any }
        }
        for (count, expected) in [
            (super::RECORD_ITEMS + 1, "combined item budget"),
            (
                super::RECORD_VECTOR_BYTES / std::mem::size_of::<super::PooledValue>() + 1,
                "vector byte budget",
            ),
        ] {
            let mut budget = super::RecordBudget::new();
            let error = super::RecordItems::<super::PooledValue> {
                budget: &mut budget,
                marker: std::marker::PhantomData,
            }
            .deserialize(CountOnly(count))
            .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
            // Postcard does not advertise the decoded length via size_hint.
            // A truncated claimed sequence is still rejected before apply;
            // its count cannot trigger a count-sized upfront allocation.
            let mut bytes = postcard::to_allocvec(&(
                super::SCHEMA,
                RequestKey::new("a", "r"),
                StoreOperation::DeactivateRoot,
                StoreRevision(1),
                StoreRevision(2),
                1u64,
                0u32,
            ))
            .unwrap();
            bytes.extend(postcard::to_allocvec(&count).unwrap());
            assert!(RecordDecision::decode(&bytes, LIMIT).is_err());
        }
    }

    #[test]
    fn p3_record_writer_and_unhinted_reader_agree_near_the_shared_capacity_edge() {
        let reply = outcome(OperationResult::Empty);
        for count in [2 * MAX_ITEMS, 2 * MAX_ITEMS + 1] {
            let events = (0..count)
                .map(|index| WatchEvent {
                    revision: StoreRevision(2),
                    event_index: u32::try_from(index).unwrap(),
                    key: MetaKey::Root {
                        root_id: format!("root-{index}"),
                    },
                    change: WatchChange::Delete,
                })
                .collect::<Vec<_>>();
            let record = RecordDecision::new(
                DecisionInput {
                    request: reply.request.clone(),
                    operation: reply.operation,
                    revision_before: StoreRevision(1),
                    revision_after: StoreRevision(2),
                    completed_at_unix_ms: 1,
                    events: &events,
                    outcomes: std::slice::from_ref(&reply),
                    reply: &reply,
                    node_epochs: Vec::new(),
                },
                LIMIT,
            );
            if count == 2 * MAX_ITEMS {
                let record = record.unwrap();
                let bytes = record.encode(LIMIT).unwrap();
                assert_eq!(
                    RecordDecision::decode(&bytes, LIMIT)
                        .unwrap()
                        .encode(LIMIT)
                        .unwrap(),
                    bytes
                );
            } else {
                assert!(
                    record
                        .unwrap_err()
                        .to_string()
                        .contains("combined item budget")
                );
            }
        }
    }

    #[test]
    fn p3_record_wire_is_still_the_original_twelve_field_sequence() {
        let entity = root_entity();
        let record = decision(
            &[event(entity.clone(), 0)],
            &[outcome(root_result(&entity))],
        );
        let fields = (
            &record.schema,
            &record.request,
            &record.operation,
            &record.revision_before,
            &record.revision_after,
            &record.completed_at_unix_ms,
            &record.event_index_start,
            &record.values,
            &record.changes,
            &record.outcomes,
            &record.reply_outcome,
            &record.node_epochs,
        );
        let bytes = postcard::to_allocvec(&record).unwrap();
        assert_eq!(bytes, postcard::to_allocvec(&fields).unwrap());
        assert_eq!(
            RecordDecision::decode(&bytes, LIMIT)
                .unwrap()
                .encode(LIMIT)
                .unwrap(),
            bytes
        );
        let json = serde_json::to_value(&record).unwrap();
        assert_eq!(
            serde_json::from_value::<RecordDecision>(json)
                .unwrap()
                .encode(LIMIT)
                .unwrap(),
            bytes
        );
    }

    fn fixtures() -> Vec<MetaEntity> {
        codec::tests::entity_fixtures()
    }
    fn root_entity() -> MetaEntity {
        fixtures()
            .into_iter()
            .find(|entity| matches!(entity, MetaEntity::Root(_)))
            .unwrap()
    }
    fn outcome(result: OperationResult) -> RequestOutcome {
        RequestOutcome {
            request: RequestKey::new("caller", "original"),
            operation: StoreOperation::DfsCreate,
            result,
        }
    }
    fn event(entity: MetaEntity, index: u32) -> WatchEvent {
        WatchEvent {
            revision: StoreRevision(2),
            event_index: index,
            key: entity_key(&entity),
            change: WatchChange::Put(entity),
        }
    }
    fn decision(events: &[WatchEvent], outcomes: &[RequestOutcome]) -> RecordDecision {
        RecordDecision::new(
            DecisionInput {
                request: outcomes[0].request.clone(),
                operation: outcomes[0].operation,
                revision_before: StoreRevision(1),
                revision_after: StoreRevision(2),
                completed_at_unix_ms: 123456,
                events,
                outcomes,
                reply: &outcomes[0],
                node_epochs: Vec::new(),
            },
            LIMIT,
        )
        .unwrap()
    }
    fn root_result(entity: &MetaEntity) -> OperationResult {
        let MetaEntity::Root(root) = entity else {
            panic!("root fixture")
        };
        OperationResult::RootRecord(root.clone())
    }
    #[test]
    fn canonical_entity_content_is_encoded_once_and_events_are_derived() {
        let MetaEntity::Root(mut root) = root_entity() else {
            unreachable!()
        };
        root.local_prepare_id = "single-authority-content-do-not-copy-487159".into();
        let entity = MetaEntity::Root(root);
        let events = vec![event(entity.clone(), 0), event(entity.clone(), 1)];
        let outcomes = vec![outcome(root_result(&entity))];
        let record = decision(&events, &outcomes);
        assert_eq!(record.values.len(), 1);
        let encoded = record.encode(LIMIT).unwrap();
        let marker = b"single-authority-content-do-not-copy-487159";
        assert_eq!(
            encoded
                .windows(marker.len())
                .filter(|bytes| *bytes == marker)
                .count(),
            1
        );
        let decoded = RecordDecision::decode(&encoded, encoded.len()).unwrap();
        let facts = decoded.facts().unwrap();
        assert_eq!(facts.events, events);
        assert_eq!(facts.outcomes, outcomes);
        assert_eq!(facts.reply, outcomes[0]);
        assert_eq!(facts.completed_at_unix_ms, 123456);
        // There is no separately editable serialized watch payload in a record.
        let changes_only = decoded.encode(LIMIT).unwrap();
        assert_eq!(
            RecordDecision::decode(&changes_only, LIMIT)
                .unwrap()
                .facts()
                .unwrap()
                .events,
            events
        );
    }

    #[test]
    fn all_entity_and_result_shapes_keep_original_values() {
        let entities = fixtures();
        let events = entities
            .iter()
            .enumerate()
            .map(|(i, entity)| event(entity.clone(), i as u32))
            .collect::<Vec<_>>();
        let mut results = Vec::new();
        let mut inode = None;
        let mut lease = None;
        let mut chunk = None;
        let mut replication = None;
        for entity in &entities {
            results.push(match entity {
                MetaEntity::NodeSession(v) => OperationResult::NodeSession(v.clone()),
                MetaEntity::RootReservation(v) => OperationResult::RootReservation(v.clone()),
                MetaEntity::Root(v) => OperationResult::RootRecord(v.clone()),
                MetaEntity::RootGrant(v) => OperationResult::RootGrant(v.clone()),
                MetaEntity::RootCommand(v) => OperationResult::RootCommand(v.clone()),
                MetaEntity::RootCommandAck(v) => OperationResult::RootCommandAck(v.clone()),
                MetaEntity::DfsInode(v) => {
                    inode = Some(v.clone());
                    OperationResult::DfsInode(v.clone())
                }
                MetaEntity::DfsWriteLease(v) => {
                    lease = Some(v.clone());
                    OperationResult::DfsWriteLease(v.clone())
                }
                MetaEntity::DfsReplicationTask(v) => OperationResult::DfsReplicationTask(v.clone()),
                MetaEntity::DfsChunk(v) => {
                    chunk = Some(v.clone());
                    OperationResult::Empty
                }
                MetaEntity::DfsReplicationConfig(v) => {
                    replication = Some(v.clone());
                    OperationResult::Empty
                }
                _ => OperationResult::Empty,
            });
        }
        let inode = inode.unwrap();
        results.extend([
            OperationResult::DfsInodeWithLease {
                inode: inode.clone(),
                lease: lease.unwrap(),
            },
            OperationResult::DfsRename(RenameOutcome {
                inode: inode.clone(),
                replaced_inode: Some(inode.clone()),
            }),
            OperationResult::DfsRename(RenameOutcome {
                inode,
                replaced_inode: None,
            }),
            OperationResult::DfsReplicationClaim {
                request_digest: [7; 32],
                claim: Some(ReplicationClaim {
                    task_id: crate::dfs::ReplicationTaskId("task".into()),
                    operation_id: crate::dfs::OperationId("operation".into()),
                    worker_node_id: "worker".into(),
                    worker_node_epoch: 7,
                    worker_session_id: "session".into(),
                    expires_at_unix_ms: 42,
                    fence: 9,
                    chunk: chunk.unwrap(),
                    source_copy_id: crate::dfs::CopyId("copy".into()),
                    placement_revision: 6,
                    replica_group: ReplicaGroup {
                        id: ReplicaGroupId("g".into()),
                        placement_epoch: 2,
                        targets: Vec::new(),
                    },
                    replication: replication.unwrap(),
                }),
            },
            OperationResult::DfsReplicationClaim {
                request_digest: [8; 32],
                claim: None,
            },
            OperationResult::DfsNamespace {
                request_digest: [9; 32],
                result: Box::new(OperationResult::DfsNamespace {
                    request_digest: [10; 32],
                    result: Box::new(OperationResult::Empty),
                }),
            },
        ]);
        let outcomes = results
            .into_iter()
            .enumerate()
            .map(|(i, result)| {
                let mut value = outcome(result);
                value.request.request_id = format!("original-{i}");
                value
            })
            .collect::<Vec<_>>();
        let record = decision(&events, &outcomes);
        assert_eq!(
            record.values.len(),
            18,
            "17 unique entities plus one separately shared claim"
        );
        let restored = RecordDecision::decode(&record.encode(LIMIT).unwrap(), LIMIT)
            .unwrap()
            .facts()
            .unwrap();
        assert_eq!(restored.events, events);
        assert_eq!(restored.outcomes, outcomes);
    }

    #[test]
    fn embedded_task_claim_and_outcome_share_chunk_config_and_claim_facts() {
        let all = fixtures();
        let chunk = all
            .iter()
            .find_map(|entity| {
                if let MetaEntity::DfsChunk(v) = entity {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .unwrap();
        let config = all
            .iter()
            .find_map(|entity| {
                if let MetaEntity::DfsReplicationConfig(v) = entity {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .unwrap();
        let mut task = all
            .iter()
            .find_map(|entity| {
                if let MetaEntity::DfsReplicationTask(v) = entity {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .unwrap();
        let claim:ReplicationClaim=serde_json::from_value(serde_json::json!({
            "task_id":"task","operation_id":"repair-op","worker_node_id":"unique-worker-claim-content-486721",
            "worker_node_epoch":3,"worker_session_id":"s","expires_at_unix_ms":42,"fence":7,
            "chunk":chunk,"source_copy_id":"copy","placement_revision":1,
            "replica_group":{"id":"g","placement_epoch":1,"targets":[]},"replication":config
        })).unwrap();
        task.claim = Some(Box::new(claim.clone()));
        let events = vec![
            event(MetaEntity::DfsChunk(claim.chunk.clone()), 0),
            event(
                MetaEntity::DfsReplicationConfig(claim.replication.clone()),
                1,
            ),
            event(MetaEntity::DfsReplicationTask(task.clone()), 2),
        ];
        let mut claimed = outcome(OperationResult::DfsReplicationClaim {
            request_digest: [1; 32],
            claim: Some(claim),
        });
        claimed.request.request_id = "second-result".into();
        let outcomes = vec![outcome(OperationResult::DfsReplicationTask(task)), claimed];
        let record = decision(&events, &outcomes);
        assert_eq!(
            record.values.len(),
            4,
            "chunk/config/task/claim must each occur once"
        );
        let bytes = record.encode(LIMIT).unwrap();
        let marker = b"unique-worker-claim-content-486721";
        assert_eq!(
            bytes
                .windows(marker.len())
                .filter(|part| *part == marker)
                .count(),
            1
        );
        let decoded = RecordDecision::decode(&bytes, LIMIT).unwrap();
        assert_eq!(decoded.facts().unwrap().events, events);
        assert_eq!(decoded.facts().unwrap().outcomes, outcomes);
        let mut cycle = record.clone();
        let task_id = cycle
            .values
            .iter()
            .position(|value| matches!(value, super::PooledValue::ClaimedTask { .. }))
            .unwrap();
        let claim = cycle
            .values
            .iter_mut()
            .find_map(|value| {
                if let super::PooledValue::Claim(v) = value {
                    Some(v)
                } else {
                    None
                }
            })
            .unwrap();
        claim.chunk = ValueId(task_id as u32);
        assert!(cycle.encode(LIMIT).is_err());
        let mut hidden = record;
        if let super::PooledValue::ClaimedTask { task, .. } = &mut hidden.values[task_id] {
            task.claim=Some(Box::new(serde_json::from_value(serde_json::to_value(&outcomes[1].result).unwrap()["DfsReplicationClaim"]["claim"].clone()).unwrap()));
        }
        assert!(
            hidden.encode(LIMIT).is_err(),
            "inline claim cannot hide a second authority copy"
        );
    }

    #[test]
    fn original_outcome_can_refer_to_a_historical_value_not_current_head() {
        let old = root_entity();
        let MetaEntity::Root(mut root) = old.clone() else {
            unreachable!()
        };
        root.root_epoch += 1;
        let new = MetaEntity::Root(root);
        let outcomes = vec![outcome(root_result(&old))];
        let events = vec![event(new.clone(), 0)];
        let record = decision(&events, &outcomes);
        assert_eq!(record.values.len(), 2);
        let facts = RecordDecision::decode(&record.encode(LIMIT).unwrap(), LIMIT)
            .unwrap()
            .facts()
            .unwrap();
        assert_eq!(facts.reply, outcomes[0]);
        assert_eq!(facts.events[0].change, WatchChange::Put(new));
    }

    #[test]
    fn malformed_references_keys_revisions_and_duplicate_authority_are_rejected() {
        let entity = root_entity();
        let record = decision(
            &[event(entity.clone(), 0)],
            &[outcome(root_result(&entity))],
        );
        let mut bad = record.clone();
        bad.changes[0] = Change::Put {
            key: entity_key(&entity),
            value: ValueId(99),
        };
        assert!(bad.encode(LIMIT).is_err());
        let mut bad = record.clone();
        bad.changes[0] = Change::Put {
            key: MetaKey::Root {
                root_id: "other".into(),
            },
            value: ValueId(0),
        };
        assert!(bad.encode(LIMIT).is_err());
        let mut bad = record.clone();
        bad.outcomes[0].result = SavedResult::DfsInode(ValueId(0));
        assert!(bad.encode(LIMIT).is_err());
        let mut bad = record.clone();
        bad.schema += 1;
        assert!(bad.encode(LIMIT).is_err());
        let mut bad = record.clone();
        bad.revision_after = StoreRevision(9);
        assert!(bad.encode(LIMIT).is_err());
        let mut bad = record.clone();
        bad.values.push(super::PooledValue::Entity(entity));
        assert!(bad.encode(LIMIT).is_err());
        let mut bad = record.clone();
        bad.values.push(bad.values[0].clone());
        bad.changes.push(Change::Put {
            key: entity_key(&bad.entity(ValueId(1)).unwrap()),
            value: ValueId(1),
        });
        assert!(bad.encode(LIMIT).is_err());
        let mut bad = record;
        bad.outcomes[0].namespace_digests = vec![[1; 32]; MAX_ITEMS + 1];
        let bytes = postcard::to_allocvec(&bad).unwrap();
        assert!(RecordDecision::decode(&bytes, LIMIT).is_err());
    }

    #[test]
    fn exact_record_budget_truncation_and_bad_inputs_are_bounded() {
        let entity = root_entity();
        let record = decision(
            &[event(entity.clone(), 0)],
            &[outcome(root_result(&entity))],
        );
        let bytes = record.encode(LIMIT).unwrap();
        assert_eq!(record.encode(bytes.len()).unwrap(), bytes);
        assert!(record.encode(bytes.len() - 1).is_err());
        assert!(RecordDecision::decode(&bytes, bytes.len() - 1).is_err());
        for end in 0..bytes.len() {
            assert!(RecordDecision::decode(&bytes[..end], LIMIT).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(RecordDecision::decode(&trailing, LIMIT).is_err());
        for length in 0..128 {
            for seed in 0..16 {
                let input = (0..length)
                    .map(|i| (i as u8).wrapping_mul(37).wrapping_add(seed))
                    .collect::<Vec<_>>();
                let _ = RecordDecision::decode(&input, 256);
            }
        }
    }

    fn create_txn(id: &str, entity: MetaEntity) -> MetaTxn {
        let mut reply = outcome(root_result(&entity));
        reply.request = RequestKey::new("caller", id);
        let mut txn = MetaTxn::new(reply.request.clone(), reply.operation);
        txn.conditions
            .push(TxnCondition::RequestAbsent(reply.request.clone()));
        txn.mutations.extend([
            TxnMutation::Put(entity),
            TxnMutation::RecordRequestOutcome(reply),
        ]);
        txn
    }
    #[test]
    fn actual_state_prepare_and_deterministic_replay_are_identical() {
        let initial = State::default();
        let entity = root_entity();
        let txn = create_txn("create", entity);
        let (prepared, reply, record) =
            initial.prepare_txn_record(txn.clone(), 100, LIMIT).unwrap();
        assert!(matches!(reply, TxnOutcome::Committed { .. }));
        let record = record.unwrap();
        let restored = initial
            .replay_decision(
                &RecordDecision::decode(&record.encode(LIMIT).unwrap(), LIMIT).unwrap(),
            )
            .unwrap();
        assert_eq!(
            prepared.snapshot_bytes_with_batch(0).unwrap(),
            restored.snapshot_bytes_with_batch(0).unwrap()
        );
        assert!(
            restored.replay_decision(&record).is_err(),
            "same durable delta cannot apply twice"
        );
        let (unchanged, retry, no_record) = restored.prepare_txn_record(txn, 101, LIMIT).unwrap();
        assert!(matches!(
            retry,
            TxnOutcome::ConditionFailed {
                existing_outcome: Some(_),
                ..
            }
        ));
        assert!(no_record.is_none());
        assert_eq!(
            unchanged.snapshot_bytes_with_batch(0).unwrap(),
            restored.snapshot_bytes_with_batch(0).unwrap()
        );
        assert_eq!(initial.revision, StoreRevision::ZERO);
        assert!(
            initial
                .prepare_txn_record(create_txn("oversize", root_entity()), 102, 8)
                .is_err()
        );
        assert_eq!(
            initial.revision,
            StoreRevision::ZERO,
            "rejected preparation cannot modify published facts"
        );
    }

    #[test]
    fn auxiliary_outcome_replacement_keeps_existing_transaction_semantics() {
        let initial = State::default();
        let (base, _, _) = initial
            .prepare_txn_record(create_txn("aux", root_entity()), 100, LIMIT)
            .unwrap();
        let mut txn = create_txn("primary", root_entity());
        let aux = RequestOutcome {
            request: RequestKey::new("caller", "aux"),
            operation: txn.operation,
            result: OperationResult::Empty,
        };
        txn.mutations
            .insert(1, TxnMutation::RecordRequestOutcome(aux.clone()));
        let mut old_semantics = base.clone();
        let old_reply = old_semantics.apply_txn(txn.clone()).unwrap();
        // Legacy apply has no decided-time argument. Extend this reference
        // with the explicit new bookkeeping contract, then still compare every
        // business field, outcome and event byte-for-byte as before.
        assert_eq!(
            old_semantics
                .request_completion
                .get(&aux.request)
                .unwrap()
                .completed_at_unix_ms,
            100,
        );
        for request in [&txn.request, &aux.request] {
            old_semantics.request_completion.insert(
                request.clone(),
                crate::meta::store::state::RequestCompletion {
                    completed_at_unix_ms: 101,
                    revision: StoreRevision(2),
                },
            );
        }
        old_semantics.event_completion.insert(StoreRevision(2), 101);
        let (prepared, reply, record) = base.prepare_txn_record(txn, 101, LIMIT).unwrap();
        let restored = base.replay_decision(&record.unwrap()).unwrap();
        assert_eq!(reply, old_reply);
        assert_eq!(
            prepared.snapshot_bytes_with_batch(0).unwrap(),
            old_semantics.snapshot_bytes_with_batch(0).unwrap()
        );
        assert_eq!(
            restored.snapshot_bytes_with_batch(0).unwrap(),
            old_semantics.snapshot_bytes_with_batch(0).unwrap()
        );
        assert_eq!(
            restored
                .read(MetaRead::RequestOutcome(aux.request.clone()))
                .unwrap()
                .request_outcome,
            Some(aux)
        );
    }

    #[test]
    fn namespace_result_depth_is_bounded_without_recursive_wire_data() {
        for depth in [128, 129] {
            let mut result = root_result(&root_entity());
            for index in 0..depth {
                result = OperationResult::DfsNamespace {
                    request_digest: [index as u8; 32],
                    result: Box::new(result),
                };
            }
            let reply = outcome(result);
            let outcomes = vec![reply.clone()];
            let record = RecordDecision::new(
                DecisionInput {
                    request: reply.request.clone(),
                    operation: reply.operation,
                    revision_before: StoreRevision(1),
                    revision_after: StoreRevision(2),
                    completed_at_unix_ms: 100,
                    events: &[],
                    outcomes: &outcomes,
                    reply: &reply,
                    node_epochs: Vec::new(),
                },
                LIMIT,
            );
            if depth == 128 {
                let bytes = record.unwrap().encode(LIMIT).unwrap();
                assert_eq!(
                    RecordDecision::decode(&bytes, LIMIT)
                        .unwrap()
                        .facts()
                        .unwrap()
                        .reply,
                    reply
                );
            } else {
                assert!(record.is_err());
            }
        }
    }

    #[test]
    fn node_epochs_aliases_absolute_expiry_and_original_results_survive_replay() {
        let mut actual = State::default();
        let mut replayed = State::default();
        for (number, session) in [(0, "s1"), (1, "s1"), (2, "s2")] {
            let request = RequestKey::new("a", format!("session-{number}"));
            let lease = NodeSessionLease {
                node_id: "a".into(),
                session_id: session.into(),
                grpc_addr: "g".into(),
                data_addr: "d".into(),
                rest_addr: "r".into(),
                storage_devices: Vec::new(),
                lease_ttl: Duration::from_secs(300),
            };
            let (next, _, record) = actual
                .prepare_node_record(request, lease, number + 100, LIMIT)
                .unwrap();
            let record = record.unwrap();
            assert_eq!(
                record.values.len(),
                1,
                "both session aliases/outcome share one entity"
            );
            replayed = replayed
                .replay_decision(
                    &RecordDecision::decode(&record.encode(LIMIT).unwrap(), LIMIT).unwrap(),
                )
                .unwrap();
            assert_eq!(
                next.snapshot_bytes_with_batch(0).unwrap(),
                replayed.snapshot_bytes_with_batch(0).unwrap()
            );
            actual = next;
        }
        assert_eq!(replayed.node_epochs.get("a"), Some(&2));
        assert!(
            matches!(replayed.read(MetaRead::CurrentNodeSession { node_id:"a".into() }).unwrap().entity,Some(MetaEntity::NodeSession(session)) if session.session_id=="s2" && session.lease_epoch==2)
        );
        assert!(
            replayed
                .read(MetaRead::DfsInode(InodeId("absent".into())))
                .unwrap()
                .entity
                .is_none()
        );
    }

    #[test]
    fn p3_namespace_history_every_checkpoint_cut_replays_all_facts_and_queries() {
        use super::{Record, StateImage};
        use crate::dfs::{ChunkId, CopyId, FileVersionId, LayoutRootId, NamespaceId};
        use crate::meta::store::history::{Delta, RemovedRequest};

        fn node(states: &mut Vec<State>, records: &mut Vec<Record>, id: &str, session: &str) {
            let previous = states.last().unwrap();
            let (next, reply, record) = previous
                .prepare_node_record(
                    RequestKey::new("node", format!("session-{}", records.len())),
                    NodeSessionLease {
                        node_id: id.into(),
                        session_id: session.into(),
                        grpc_addr: "g".into(),
                        data_addr: "d".into(),
                        rest_addr: "r".into(),
                        storage_devices: Vec::new(),
                        lease_ttl: Duration::from_secs(300),
                    },
                    100 + records.len() as u64,
                    LIMIT,
                )
                .unwrap();
            assert!(matches!(reply, TxnOutcome::Committed { .. }));
            records.push(record.unwrap().into());
            states.push(next);
        }
        fn transaction(
            states: &mut Vec<State>,
            records: &mut Vec<Record>,
            id: &str,
            operation: StoreOperation,
            mut mutations: Vec<TxnMutation>,
            result: OperationResult,
        ) {
            let request = RequestKey::new("writer", id);
            let mut txn = MetaTxn::new(request.clone(), operation);
            txn.conditions
                .push(TxnCondition::RequestAbsent(request.clone()));
            mutations.push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation,
                result,
            }));
            txn.mutations = mutations;
            let (next, reply, record) = states
                .last()
                .unwrap()
                .prepare_txn_record(txn, 100 + records.len() as u64, LIMIT)
                .unwrap();
            assert!(matches!(reply, TxnOutcome::Committed { .. }));
            records.push(record.unwrap().into());
            states.push(next);
        }
        fn equivalent(expected: &State, restored: &State, queries: &[MetaRead]) {
            // The old full-state test codec includes every entity revision,
            // original outcome/completion, event ordinal/watermark and epoch.
            let before = expected.snapshot_bytes_with_batch(0).unwrap();
            assert_eq!(restored.snapshot_bytes_with_batch(0).unwrap(), before);
            for query in queries {
                assert_eq!(
                    restored.read(query.clone()).unwrap(),
                    expected.read(query.clone()).unwrap(),
                    "{query:?}"
                );
            }
            for after in [
                StoreRevision::ZERO,
                StoreRevision(1),
                StoreRevision(3),
                expected.revision,
            ] {
                for limit in [0, 1, 8] {
                    assert_eq!(
                        restored.watch(after, limit).unwrap(),
                        expected.watch(after, limit).unwrap()
                    );
                }
            }
            // Capacity indexes are reconstructed from authority, not persisted
            // as another fact. Include them in the equivalence check as well.
            let mut indexed = expected.clone();
            indexed.ensure_history_index().unwrap();
            assert_eq!(
                restored.pin_charge().unwrap(),
                indexed.pin_charge().unwrap()
            );
            assert_eq!(restored.snapshot_bytes_with_batch(0).unwrap(), before);
        }

        let mut states = vec![State::default()];
        let mut records = Vec::new();
        node(&mut states, &mut records, "a", "old");
        node(&mut states, &mut records, "b", "t");
        node(&mut states, &mut records, "a", "s");
        let entities = fixtures();
        let mut inode = entities
            .iter()
            .find_map(|entity| match entity {
                MetaEntity::DfsInode(value) => Some(value.clone()),
                _ => None,
            })
            .unwrap();
        let dentry = entities
            .iter()
            .find_map(|entity| match entity {
                MetaEntity::DfsDentry(value) => Some(value.clone()),
                _ => None,
            })
            .unwrap();
        inode.head_version = None;
        inode.revision = 4;
        let mut create = entities
            .iter()
            .filter(|entity| {
                matches!(
                    entity,
                    MetaEntity::Root(_)
                        | MetaEntity::RootGrant(_)
                        | MetaEntity::DfsReplicationConfig(_)
                        | MetaEntity::DfsDurabilityPolicy(_)
                )
            })
            .cloned()
            .map(TxnMutation::Put)
            .collect::<Vec<_>>();
        create.extend([
            TxnMutation::Put(MetaEntity::DfsDentry(dentry.clone())),
            TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
        ]);
        transaction(
            &mut states,
            &mut records,
            "create",
            StoreOperation::DfsCreate,
            create,
            OperationResult::DfsInode(inode.clone()),
        );

        inode.head_version = Some(FileVersionId("v".into()));
        inode.revision = 5;
        let mut publish = entities
            .iter()
            .filter(|entity| {
                matches!(
                    entity,
                    MetaEntity::DfsFileVersion(_)
                        | MetaEntity::DfsLayoutRoot(_)
                        | MetaEntity::DfsChunk(_)
                        | MetaEntity::DfsPlacement(_)
                        | MetaEntity::DfsCopy(_)
                        | MetaEntity::DfsReplicationTask(_)
                        | MetaEntity::DfsWriteLease(_)
                )
            })
            .cloned()
            .map(TxnMutation::Put)
            .collect::<Vec<_>>();
        publish.push(TxnMutation::Put(MetaEntity::DfsInode(inode.clone())));
        transaction(
            &mut states,
            &mut records,
            "publish",
            StoreOperation::DfsCommitFileVersion,
            publish,
            OperationResult::DfsInode(inode.clone()),
        );
        let mut version = entities
            .iter()
            .find_map(|entity| match entity {
                MetaEntity::DfsFileVersion(value) => Some(value.clone()),
                _ => None,
            })
            .unwrap();
        let mut layout = entities
            .iter()
            .find_map(|entity| match entity {
                MetaEntity::DfsLayoutRoot(value) => Some(value.clone()),
                _ => None,
            })
            .unwrap();
        version.parent_version = Some(version.id.clone());
        version.id = FileVersionId("v2".into());
        version.layout_root = LayoutRootId("l2".into());
        version.created_at_unix_ms += 1;
        layout.id = version.layout_root.clone();
        inode.head_version = Some(version.id.clone());
        inode.revision = 6;
        inode.attributes.mtime_unix_ms += 10;
        inode.xattrs.insert(b"new".to_vec(), vec![0, 255, 1]);
        transaction(
            &mut states,
            &mut records,
            "publish-2",
            StoreOperation::DfsCommitFileVersion,
            vec![
                TxnMutation::Put(MetaEntity::DfsFileVersion(version)),
                TxnMutation::Put(MetaEntity::DfsLayoutRoot(layout)),
                TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
            ],
            OperationResult::DfsInode(inode.clone()),
        );

        let mut renamed = dentry.clone();
        renamed.key.parent_inode_id = InodeId("other-parent".into());
        renamed.key.name = b"renamed".to_vec();
        inode.revision = 7;
        transaction(
            &mut states,
            &mut records,
            "rename",
            StoreOperation::DfsRename,
            vec![
                TxnMutation::Delete(entity_key(&MetaEntity::DfsDentry(dentry.clone()))),
                TxnMutation::Put(MetaEntity::DfsDentry(renamed.clone())),
                TxnMutation::Put(MetaEntity::DfsInode(inode.clone())),
            ],
            OperationResult::DfsRename(RenameOutcome {
                inode: inode.clone(),
                replaced_inode: None,
            }),
        );
        transaction(
            &mut states,
            &mut records,
            "unlink",
            StoreOperation::DfsUnlink,
            vec![
                TxnMutation::Delete(entity_key(&MetaEntity::DfsDentry(renamed.clone()))),
                TxnMutation::Delete(MetaKey::DfsInode(inode.inode_id.clone())),
            ],
            OperationResult::Empty,
        );
        node(&mut states, &mut records, "a", "latest");
        let previous = states.last().unwrap();
        let removed = RequestKey::new("node", "session-0");
        let cleanup = Delta {
            revision: previous.revision,
            selected_at_unix_ms: 999,
            compacted_from: StoreRevision::ZERO,
            compacted_to: StoreRevision(3),
            requests: vec![RemovedRequest {
                key: removed.clone(),
                completion: previous.request_completion.get(&removed).unwrap().clone(),
            }],
        };
        let next = previous.apply_history(&cleanup).unwrap();
        records.push(Record::History(cleanup));
        states.push(next);
        assert_eq!(records.len(), 10);
        assert_eq!(states.last().unwrap().node_epochs.get("a"), Some(&3));
        assert!(states.last().unwrap().requests.get(&removed).is_none());
        assert_eq!(states.last().unwrap().compacted_to, StoreRevision(3));

        let mut queries = vec![
            MetaRead::CurrentNodeSessions,
            MetaRead::CurrentNodeSession {
                node_id: "a".into(),
            },
            MetaRead::NodeSession {
                node_id: "a".into(),
                session_id: "old".into(),
            },
            MetaRead::Root {
                root_id: "r".into(),
            },
            MetaRead::RootGrants {
                root_id: "r".into(),
            },
            MetaRead::RootGrantByHolder {
                root_id: "r".into(),
                holder_node_id: "b".into(),
                holder_session_id: "t".into(),
            },
            MetaRead::DfsDirectory {
                namespace_id: NamespaceId("n".into()),
                parent_inode_id: InodeId("p".into()),
            },
            MetaRead::DfsDirectory {
                namespace_id: NamespaceId("n".into()),
                parent_inode_id: InodeId("other-parent".into()),
            },
            MetaRead::DfsDentry(dentry.key),
            MetaRead::DfsDentry(renamed.key),
            MetaRead::DfsInode(inode.inode_id.clone()),
            MetaRead::DfsInode(InodeId("absent".into())),
            MetaRead::DfsFileVersion(FileVersionId("v".into())),
            MetaRead::DfsFileVersion(FileVersionId("v2".into())),
            MetaRead::DfsLayoutRoot(LayoutRootId("l".into())),
            MetaRead::DfsLayoutRoot(LayoutRootId("l2".into())),
            MetaRead::DfsChunk(ChunkId("c".into())),
            MetaRead::DfsPlacement(ChunkId("c".into())),
            MetaRead::DfsPlacements,
            MetaRead::DfsCopy(CopyId("copy".into())),
            MetaRead::DfsWriteLease(inode.inode_id),
            MetaRead::DfsReplicationConfig,
            MetaRead::DfsDurabilityPolicy,
            MetaRead::DfsReplicationTasks,
        ];
        for state in &states {
            queries.extend(
                state
                    .requests
                    .iter()
                    .map(|(key, _)| MetaRead::RequestOutcome(key.clone())),
            );
        }
        for (cut, expected) in states.iter().enumerate() {
            let image = StateImage::encode(expected, LIMIT).unwrap();
            let mut restored = StateImage::decode(&image, LIMIT).unwrap();
            equivalent(expected, &restored, &queries);
            for (index, record) in records.iter().enumerate().skip(cut) {
                let encoded = codec::encode_bounded(record, LIMIT).unwrap();
                let (decoded, tail): (Record, _) = postcard::take_from_bytes(&encoded).unwrap();
                assert!(tail.is_empty());
                restored = restored.replay_record(&decoded).unwrap();
                equivalent(&states[index + 1], &restored, &queries);
            }
            equivalent(states.last().unwrap(), &restored, &queries);
            assert_eq!(
                StateImage::encode(expected, LIMIT).unwrap(),
                image,
                "pinned state changed at cut {cut}"
            );
        }
    }

    #[test]
    fn p3_checkpoint_image_is_self_contained_and_pools_history() {
        let old_marker = "old-canonical-content-only-once-0123456789";
        let new_marker = "new-canonical-content-only-once-9876543210";
        let mut old = root_entity();
        if let MetaEntity::Root(ref mut root) = old {
            root.local_prepare_id = old_marker.into();
        }
        let initial = State::default();
        let (first, _, _) = initial
            .prepare_txn_record(create_txn("old", old), 111, LIMIT)
            .unwrap();
        let pinned = first.snapshot_bytes_with_batch(0).unwrap();
        let mut new = root_entity();
        if let MetaEntity::Root(ref mut root) = new {
            root.local_prepare_id = new_marker.into();
        }
        let (second, _, _) = first
            .prepare_txn_record(create_txn("new", new), 222, LIMIT)
            .unwrap();
        let bytes = super::StateImage::encode(&second, LIMIT)
            .expect("self-contained canonical image must encode without keeping records");
        let restored = super::StateImage::decode(&bytes, LIMIT).unwrap();
        assert_eq!(
            restored.snapshot_bytes_with_batch(0).unwrap(),
            second.snapshot_bytes_with_batch(0).unwrap()
        );
        for marker in [old_marker, new_marker] {
            assert_eq!(
                bytes
                    .windows(marker.len())
                    .filter(|value| *value == marker.as_bytes())
                    .count(),
                1,
                "current authority, original reply and historical events must share one exact entity content"
            );
        }
        assert_eq!(
            first.snapshot_bytes_with_batch(0).unwrap(),
            pinned,
            "encoding cannot mutate a pinned published view"
        );
        assert!(super::StateImage::encode(&second, 16).is_err());
        assert!(super::StateImage::decode(&bytes, 16).is_err());
    }

    #[test]
    fn p3_checkpoint_image_preserves_alias_epochs_completion_and_watermark() {
        let mut state = State::default();
        for (index, session) in ["old", "new"].into_iter().enumerate() {
            state = state
                .prepare_node_record(
                    RequestKey::new("node", session),
                    NodeSessionLease {
                        node_id: "node".into(),
                        session_id: session.into(),
                        grpc_addr: "g".into(),
                        data_addr: "d".into(),
                        rest_addr: "r".into(),
                        storage_devices: Vec::new(),
                        lease_ttl: Duration::from_secs(60),
                    },
                    100 + index as u64,
                    LIMIT,
                )
                .unwrap()
                .0;
        }
        state.compacted_to = StoreRevision(1);
        state.events_removed = 2;
        state.event_completion.remove(&StoreRevision(1));
        state.events = state
            .events
            .iter()
            .filter(|event| event.revision > StoreRevision(1))
            .cloned()
            .collect();
        let bytes = super::StateImage::encode(&state, LIMIT).unwrap();
        let restored = super::StateImage::decode(&bytes, LIMIT).unwrap();
        assert_eq!(
            restored.snapshot_bytes_with_batch(0).unwrap(),
            state.snapshot_bytes_with_batch(0).unwrap()
        );
        assert_eq!(restored.node_epochs.get("node"), Some(&2));
        assert_eq!(
            restored
                .request_completion
                .get(&RequestKey::new("node", "old"))
                .unwrap()
                .completed_at_unix_ms,
            100
        );
        assert!(
            matches!(restored.read(MetaRead::CurrentNodeSession { node_id: "node".into() }).unwrap().entity,
            Some(MetaEntity::NodeSession(session)) if session.session_id == "new" && session.lease_epoch == 2)
        );
    }

    #[test]
    fn p3_checkpoint_image_rejects_stale_current_session_alias() {
        let mut state = State::default();
        for session in ["old", "new"] {
            state = state
                .prepare_node_record(
                    RequestKey::new("n", session),
                    NodeSessionLease {
                        node_id: "n".into(),
                        session_id: session.into(),
                        grpc_addr: "g".into(),
                        data_addr: "d".into(),
                        rest_addr: "r".into(),
                        storage_devices: Vec::new(),
                        lease_ttl: Duration::from_secs(60),
                    },
                    10,
                    LIMIT,
                )
                .unwrap()
                .0;
        }
        let bytes = super::StateImage::encode(&state, LIMIT).unwrap();
        let (mut image, _): (super::StateImage, _) = postcard::take_from_bytes(&bytes).unwrap();
        let old = ValueId::at(image.values.iter().position(|value| matches!(value, super::PooledValue::Entity(MetaEntity::NodeSession(session)) if session.lease_epoch == 1)).unwrap()).unwrap();
        let alias = MetaKey::CurrentNodeSession {
            node_id: "n".into(),
        };
        image
            .entities
            .iter_mut()
            .find(|entry| entry.key == alias)
            .unwrap()
            .value = old;
        for event in image
            .events
            .iter_mut()
            .filter(|event| event.revision == StoreRevision(2))
        {
            if let super::Change::Put { key, value } = &mut event.change
                && *key == alias
            {
                *value = old;
            }
        }
        let corrupt = codec::encode_bounded(&image, LIMIT).unwrap();
        let error = match super::StateImage::decode(&corrupt, LIMIT) {
            Ok(_) => panic!("stale current alias accepted"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("current session alias/epoch mismatch"),
            "{error}"
        );
    }

    #[test]
    fn p3_checkpoint_image_rejects_corrupt_reference_order_and_authority() {
        let state = State::default()
            .prepare_txn_record(create_txn("one", root_entity()), 50, LIMIT)
            .unwrap()
            .0;
        let bytes = super::StateImage::encode(&state, LIMIT).unwrap();
        for case in 0..12 {
            let (mut image, tail): (super::StateImage, _) =
                postcard::take_from_bytes(&bytes).unwrap();
            assert!(tail.is_empty());
            match case {
                0 => image.magic[0] = b'X',
                1 => image.schema += 1,
                2 => image.compacted_to = StoreRevision(2),
                3 => image.entities[0].value = ValueId(u32::MAX),
                4 => {
                    image.entities[0].key = MetaKey::Root {
                        root_id: "wrong".into(),
                    }
                }
                5 => image.entities[0].revision = StoreRevision(2),
                6 => image.requests[0].completion.revision = StoreRevision(0),
                7 => image.requests[0].completion.revision = StoreRevision(2),
                8 => image.values.push(image.values[0].clone()),
                9 => image.events[0].revision = StoreRevision(0),
                10 => {
                    image.events[0].change = super::Change::Delete {
                        key: image.entities[0].key.clone(),
                    }
                }
                11 => {
                    let mut unused = root_entity();
                    if let MetaEntity::Root(root) = &mut unused {
                        root.root_id = "unreachable".into();
                    }
                    image.values.push(super::PooledValue::Entity(unused));
                }
                _ => unreachable!(),
            }
            let corrupt = codec::encode_bounded(&image, LIMIT).unwrap();
            assert!(
                super::StateImage::decode(&corrupt, LIMIT).is_err(),
                "corruption case {case}"
            );
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(super::StateImage::decode(&trailing, LIMIT).is_err());
        for size in 0..bytes.len() {
            assert!(super::StateImage::decode(&bytes[..size], LIMIT).is_err());
        }
    }

    #[test]
    fn p3_checkpoint_image_is_not_limited_to_one_operation_item_count() {
        use crate::meta::store::state::VersionedEntity;
        let mut state = State {
            revision: StoreRevision(1),
            compacted_to: StoreRevision(1),
            ..State::default()
        };
        let template = root_entity();
        for index in 0..=MAX_ITEMS {
            let mut entity = template.clone();
            if let MetaEntity::Root(root) = &mut entity {
                root.root_id = format!("root-{index:08}");
            }
            state.entities.insert(
                entity_key(&entity),
                VersionedEntity {
                    revision: StoreRevision(1),
                    entity,
                },
            );
        }
        let limit = 64 * 1024 * 1024;
        let bytes = super::StateImage::encode(&state, limit).unwrap();
        let restored = super::StateImage::decode(&bytes, limit).unwrap();
        assert_eq!(restored.entities.len(), MAX_ITEMS + 1);
        assert_eq!(restored.revision, StoreRevision(1));
        for index in [0, MAX_ITEMS / 2, MAX_ITEMS] {
            let key = MetaKey::Root {
                root_id: format!("root-{index:08}"),
            };
            assert_eq!(
                restored.entities.get(&key).unwrap().entity,
                state.entities.get(&key).unwrap().entity
            );
        }
        eprintln!(
            "P3_IMAGE_BOUND entities={} encoded_bytes={}",
            restored.entities.len(),
            bytes.len()
        );
    }

    #[test]
    fn p3_checkpoint_image_rejects_untrusted_huge_count_before_items() {
        struct Count;
        impl serde::Serialize for Count {
            fn serialize<S: serde::Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                use serde::ser::SerializeSeq;
                serializer
                    .serialize_seq(Some(super::MAX_IMAGE_ITEMS + 1))?
                    .end()
            }
        }
        let announced = codec::encode_bounded(
            &(
                *b"AFSC",
                super::IMAGE_SCHEMA,
                StoreRevision(0),
                StoreRevision(0),
                0u64,
                Count,
            ),
            128,
        )
        .unwrap();
        assert!(super::StateImage::decode(&announced, 128).is_err());
        for size in 0..128 {
            let bytes: Vec<u8> = (0..size).map(|x| (x as u8).wrapping_mul(37)).collect();
            assert!(super::StateImage::decode(&bytes, 128).is_err());
        }
    }
}
