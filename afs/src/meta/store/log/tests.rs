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
        task.claim = Some(Box::new(
            serde_json::from_value(
                serde_json::to_value(&outcomes[1].result).unwrap()["DfsReplicationClaim"]["claim"]
                    .clone(),
            )
            .unwrap(),
        ));
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
    let (prepared, reply, record) = initial.prepare_txn_record(txn.clone(), 100, LIMIT).unwrap();
    assert!(matches!(reply, TxnOutcome::Committed { .. }));
    let record = record.unwrap();
    let restored = initial
        .replay_decision(&RecordDecision::decode(&record.encode(LIMIT).unwrap(), LIMIT).unwrap())
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
            ownerfs_enabled: true,
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
                    ownerfs_enabled: true,
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
                    ownerfs_enabled: true,
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
                    ownerfs_enabled: true,
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
        let (mut image, tail): (super::StateImage, _) = postcard::take_from_bytes(&bytes).unwrap();
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

fn assert_incompatible(error: afs_error::Error, name: &str, found: &str, supported: &str) {
    assert_eq!(
        error.code(),
        afs_error::STORAGE_FORMAT_INCOMPATIBLE,
        "{error}"
    );
    let message = error.to_string();
    for part in [name, found, supported] {
        assert!(message.contains(part), "missing {part:?}: {message}");
    }
}

/// Inner payload versions are read from the leading bytes; the rest of
/// the payload is cut off so a decode-first reader would fail differently.
#[test]
fn image_and_decision_versions_are_identified_before_the_full_decode() {
    let state = State::default()
        .prepare_txn_record(create_txn("one", root_entity()), 50, LIMIT)
        .unwrap()
        .0;
    let mut image = super::StateImage::encode(&state, LIMIT).unwrap();
    // postcard: four magic bytes, then the schema varint.
    assert_eq!(image[..4], super::IMAGE_MAGIC);
    assert_eq!(u16::from(image[4]), super::IMAGE_SCHEMA);
    image[4] = 3;
    image.truncate(5);
    assert_incompatible(
        super::StateImage::validate_encoded(&image, LIMIT).unwrap_err(),
        "Meta checkpoint state image",
        "version 3",
        "supports version 2",
    );

    let entity = root_entity();
    let outcomes = vec![outcome(root_result(&entity))];
    let record = decision(&[event(entity, 0)], &outcomes);
    let mut bytes = record.encode(LIMIT).unwrap();
    assert_eq!(u16::from(bytes[0]), super::SCHEMA);
    bytes[0] = 2;
    bytes.truncate(1);
    assert_incompatible(
        RecordDecision::decode(&bytes, LIMIT).unwrap_err(),
        "Meta WAL record decision",
        "version 2",
        "supports version 1",
    );
    let mut framed = postcard::to_allocvec(&super::Record::from(record)).unwrap();
    assert_eq!((framed[0], u16::from(framed[1])), (0, super::SCHEMA));
    framed[1] = 2;
    assert_incompatible(
        super::Record::check_schema(&framed).unwrap_err(),
        "Meta WAL record decision",
        "version 2",
        "supports version 1",
    );
    super::Record::check_schema(&postcard::to_allocvec(&1u32).unwrap()).unwrap();
}
