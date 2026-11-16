use super::*;
fn lease(session: &str) -> NodeSessionLease {
    NodeSessionLease {
        node_id: "node".into(),
        session_id: session.into(),
        grpc_addr: "grpc".into(),
        data_addr: "data".into(),
        rest_addr: "rest".into(),
        nfs_addr: String::new(),
        ownerfs_enabled: true,
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
    let (retried, result, duplicate) = replayed.prepare_txn_record(request, 9999, 65536).unwrap();
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
