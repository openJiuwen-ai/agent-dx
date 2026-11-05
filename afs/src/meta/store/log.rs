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

use crate::format::{self, FormatKind};

/// `RecordDecision` schema: inner layer of a WAL record (`MetaRecordDecision`).
const SCHEMA: u16 = format::version::META_RECORD_DECISION;
/// `StateImage` schema: inner layer of a checkpoint file (`MetaStateImage`).
const IMAGE_SCHEMA: u16 = format::version::META_STATE_IMAGE;
const IMAGE_MAGIC: [u8; 4] = format::META_STATE_IMAGE_MAGIC;
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
    /// Version-first probe of an encoded record (inner layer): a `Decision`
    /// (variant 0, pinned by the G1 table) starts with its `RecordDecision`
    /// schema. `History` deltas carry no own version. An undecodable prefix
    /// is left to the full decode.
    pub(super) fn check_schema(encoded: &[u8]) -> Result<()> {
        match postcard::take_from_bytes::<u32>(encoded) {
            Ok((0, decision)) => {
                format::check_postcard_u16(FormatKind::MetaRecordDecision, "record", decision)
            }
            _ => Ok(()),
        }
    }
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

    /// Standalone decision codec. The product encodes decisions inside their
    /// `Record`; tests use this to pin the decision encoding on its own.
    #[cfg(test)]
    pub fn encode(&self, byte_limit: usize) -> Result<Vec<u8>> {
        self.validate()?;
        codec::encode_bounded(self, byte_limit)
    }

    #[cfg(test)]
    pub fn decode(bytes: &[u8], byte_limit: usize) -> Result<Self> {
        if bytes.len() > byte_limit {
            return Err(invalid_contract("canonical decision exceeds byte limit"));
        }
        format::check_postcard_u16(FormatKind::MetaRecordDecision, "decision", bytes)?;
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
        // Version first: magic and schema lead the payload; read them before
        // the full decode and validation.
        if let Ok(((magic, schema), _)) = postcard::take_from_bytes::<([u8; 4], u16)>(bytes)
            && magic == IMAGE_MAGIC
        {
            format::check(FormatKind::MetaStateImage, "payload", u64::from(schema))?;
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
            magic: IMAGE_MAGIC,
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
        self.validate_bounds()?;
        self.validate_event_completion()?;
        self.validate_pool()?;
        let mut referenced = PoolMarks(vec![false; self.values.len()]);
        self.validate_entities(&mut referenced)?;
        self.validate_requests(&mut referenced)?;
        self.validate_events(&mut referenced)?;
        self.validate_node_epochs()?;
        for value in &self.values {
            for id in value.references() {
                referenced.mark(id)?;
            }
        }
        if referenced.0.iter().any(|used| !used) {
            return Err(invalid_contract(
                "checkpoint contains unreachable pooled content",
            ));
        }
        Ok(())
    }
    /// Magic, schema, revision cursor, item counts and the event ordinal base.
    fn validate_bounds(&self) -> Result<()> {
        if self.magic != IMAGE_MAGIC
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
        Ok(())
    }
    /// One original completion time per retained event revision group.
    fn validate_event_completion(&self) -> Result<()> {
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
        Ok(())
    }
    /// Pool values only reference earlier values, decode, and are unique.
    fn validate_pool(&self) -> Result<()> {
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
        Ok(())
    }
    fn validate_entities(&self, referenced: &mut PoolMarks) -> Result<()> {
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
            referenced.mark(entry.value)?;
            previous_key = Some(&entry.key);
        }
        Ok(())
    }
    fn validate_requests(&self, referenced: &mut PoolMarks) -> Result<()> {
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
                referenced.mark(id)?;
            }
            previous_request = Some(key);
        }
        Ok(())
    }
    /// Event order, key/value agreement, and that each key's latest event
    /// agrees with the authoritative entity set.
    fn validate_events(&self, referenced: &mut PoolMarks) -> Result<()> {
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
                    referenced.mark(*value)?;
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
        Ok(())
    }
    /// Node epochs are ordered and positive, and every stored session (and
    /// current-session alias) stays within its node's epoch.
    fn validate_node_epochs(&self) -> Result<()> {
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

/// Which pooled values a checkpoint image reaches; every value must be.
struct PoolMarks(Vec<bool>);

impl PoolMarks {
    fn mark(&mut self, id: ValueId) -> Result<()> {
        *self
            .0
            .get_mut(id.0 as usize)
            .ok_or_else(|| invalid_contract("checkpoint reference out of range"))? = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) mod format_golden_tests;
