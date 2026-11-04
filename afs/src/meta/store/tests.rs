use super::journal::MetaLog;
use super::*;
use std::sync::Mutex;

fn request() -> RequestKey {
    RequestKey::new("node-a/session-1", "req-1")
}

fn grant() -> RootAccessGrant {
    RootAccessGrant {
        root_id: "workspace-1".into(),
        root_epoch: 7,
        home_node_id: "node-a".into(),
        home_session_id: "home-session-2".into(),
        holder_node_id: "node-b".into(),
        holder_session_id: "holder-session-9".into(),
        access_generation: 3,
        rights: vec![RootRight::Lookup, RootRight::Read],
        fencing_token: "fence-3".into(),
        issued_at_revision: StoreRevision(11),
    }
}

fn session(node_id: &str, session_id: &str, expires_at_unix_ms: u64) -> NodeSession {
    NodeSession {
        node_id: node_id.into(),
        session_id: session_id.into(),
        grpc_addr: format!("http://{node_id}:7400"),
        data_addr: format!("http://{node_id}:7500"),
        rest_addr: format!("http://{node_id}:7600"),
        ownerfs_enabled: true,
        storage_devices: Vec::new(),
        lease_epoch: 1,
        expires_at_unix_ms,
    }
}

#[test]
fn p3_typed_prefix_reads_are_scoped_sorted_and_rebuilt_from_facts() {
    let mut state = State {
        revision: StoreRevision(42),
        ..State::default()
    };
    fn put(state: &mut State, key: MetaKey, entity: MetaEntity) {
        state.entities.insert(
            key,
            VersionedEntity {
                revision: StoreRevision(7),
                entity,
            },
        );
    }
    for (ns, parent, name) in [
        ("n", "p", vec![255]),
        ("n", "p", b"a".to_vec()),
        ("n", "p", vec![0]),
        ("nn", "p", b"other".to_vec()),
        ("n", "pp", b"other".to_vec()),
        ("m", "p", b"before".to_vec()),
    ] {
        let key = DentryKey {
            namespace_id: NamespaceId::new(ns),
            parent_inode_id: InodeId::new(parent),
            name,
        };
        put(
            &mut state,
            MetaKey::DfsDentry(key.clone()),
            MetaEntity::DfsDentry(Dentry {
                key,
                inode_id: InodeId::new("inode"),
            }),
        );
    }
    for id in ["z", "a"] {
        let session = session(id, "session", u64::MAX);
        put(
            &mut state,
            MetaKey::CurrentNodeSession { node_id: id.into() },
            MetaEntity::NodeSession(session.clone()),
        );
        put(
            &mut state,
            MetaKey::NodeSession {
                node_id: id.into(),
                session_id: "session".into(),
            },
            MetaEntity::NodeSession(session),
        );
        let placement = PlacementRecord {
            chunk_id: crate::dfs::ChunkId::new(id),
            replica_group_id: crate::dfs::ReplicaGroupId::new("group"),
            placement_epoch: 1,
            desired_copies: 1,
            copies: Vec::new(),
            health: crate::dfs::PlacementHealth::Satisfied,
        };
        put(
            &mut state,
            MetaKey::DfsPlacement(placement.chunk_id.clone()),
            MetaEntity::DfsPlacement(placement),
        );
        let task = ReplicationTask {
            id: ReplicationTaskId::new(id),
            chunk_id: crate::dfs::ChunkId::new(id),
            placement_epoch: 1,
            desired_copies: 1,
            existing_copies: Vec::new(),
            state: crate::dfs::ReplicationTaskState::Pending,
            attempt: 0,
            next_retry_unix_ms: 0,
            last_error: None,
            claim: None,
        };
        put(
            &mut state,
            MetaKey::DfsReplicationTask(task.id.clone()),
            MetaEntity::DfsReplicationTask(task),
        );
    }
    for root in ["r", "rr", "q"] {
        for holder in ["z", "a"] {
            let mut grant = grant();
            grant.root_id = root.into();
            grant.holder_node_id = holder.into();
            put(
                &mut state,
                entity_key(&MetaEntity::RootGrant(grant.clone())),
                MetaEntity::RootGrant(grant),
            );
        }
    }
    let directory = MetaRead::DfsDirectory {
        namespace_id: NamespaceId::new("n"),
        parent_inode_id: InodeId::new("p"),
    };
    let snapshot = state.read(directory.clone()).unwrap();
    let names: Vec<_> = snapshot
        .entities
        .iter()
        .map(|entity| match entity {
            MetaEntity::DfsDentry(value) => value.key.name.clone(),
            _ => panic!("wrong entity family"),
        })
        .collect();
    assert_eq!(names, vec![vec![0], b"a".to_vec(), vec![255]]);
    let before = state.snapshot_bytes_with_batch(0).unwrap();
    let recovered = State::from_snapshot(&before).unwrap();
    for (query, expected) in [
        (directory, 3),
        (MetaRead::CurrentNodeSessions, 2),
        (
            MetaRead::RootGrants {
                root_id: "r".into(),
            },
            2,
        ),
        (MetaRead::DfsPlacements, 2),
        (MetaRead::DfsReplicationTasks, 2),
        (
            MetaRead::DfsDirectory {
                namespace_id: NamespaceId::new("none"),
                parent_inode_id: InodeId::new("p"),
            },
            0,
        ),
    ] {
        let original = state.read(query.clone()).unwrap();
        assert_eq!(original.entities.len(), expected);
        assert_eq!(original.revision, StoreRevision(42));
        assert_eq!(recovered.read(query).unwrap(), original);
    }
    assert_eq!(recovered.snapshot_bytes_with_batch(0).unwrap(), before);
}

#[test]
fn p3_fork_shares_payloads_but_keeps_updates_private() {
    let original = StoreState::new();
    let key = MetaKey::NodeSession {
        node_id: "node-a".into(),
        session_id: "session-1".into(),
    };
    let entity = MetaEntity::NodeSession(session("node-a", "session-1", 100));
    {
        let mut state = original.state.lock().unwrap();
        state.revision = StoreRevision(1);
        state.entities.insert(
            key.clone(),
            VersionedEntity {
                revision: StoreRevision(1),
                entity: entity.clone(),
            },
        );
        state.events.push(WatchEvent {
            revision: StoreRevision(1),
            event_index: 0,
            key: key.clone(),
            change: WatchChange::Put(entity),
        });
    }
    let fork = original.fork().unwrap();
    let original_payload = {
        let state = original.state.lock().unwrap();
        state.entities.get(&key).unwrap() as *const VersionedEntity
    };
    let fork_payload = {
        let state = fork.state.lock().unwrap();
        state.entities.get(&key).unwrap() as *const VersionedEntity
    };
    assert_eq!(
        original_payload, fork_payload,
        "a private fork must share unchanged entity payloads"
    );
    let original_event = {
        let state = original.state.lock().unwrap();
        state.events.iter().next().unwrap() as *const WatchEvent
    };
    let fork_event = {
        let state = fork.state.lock().unwrap();
        state.events.iter().next().unwrap() as *const WatchEvent
    };
    assert_eq!(
        original_event, fork_event,
        "history payloads must not be cloned by fork"
    );
    {
        let mut state = fork.state.lock().unwrap();
        state.revision = StoreRevision(2);
        state.entities.insert(
            key.clone(),
            VersionedEntity {
                revision: StoreRevision(2),
                entity: MetaEntity::NodeSession(session("node-a", "session-1", 200)),
            },
        );
        state.events.push(WatchEvent {
            revision: StoreRevision(2),
            event_index: 0,
            key: key.clone(),
            change: WatchChange::Delete,
        });
    }
    assert_eq!(
        original
            .state
            .lock()
            .unwrap()
            .entities
            .get(&key)
            .unwrap()
            .revision,
        StoreRevision(1)
    );
    assert_eq!(
        fork.state
            .lock()
            .unwrap()
            .entities
            .get(&key)
            .unwrap()
            .revision,
        StoreRevision(2)
    );
    assert_eq!(original.state.lock().unwrap().events.len(), 1);
    assert_eq!(fork.state.lock().unwrap().events.len(), 2);
    let original_bytes = original.snapshot_bytes().unwrap();
    let restored = StoreState::from_snapshot(&original_bytes).unwrap();
    assert_eq!(restored.snapshot_bytes().unwrap(), original_bytes);
}

#[test]
fn node_session_liveness_uses_strict_expiry_boundary() {
    let session = session("node-a", "session-1", 100);
    assert!(!session.is_live_at_unix_ms(100));
    assert!(!session.is_live_at_unix_ms(101));
    assert!(session.is_live_at_unix_ms(99));
}

#[test]
fn snapshot_bytes_are_independent_of_hash_map_insertion_order() {
    fn state_in_order(nodes: &[&str]) -> StoreState {
        let mut state = MemoryState {
            revision: StoreRevision(7),
            ..MemoryState::default()
        };
        for node in nodes {
            let value = session(node, "session-1", 100);
            state.entities.insert(
                MetaKey::NodeSession {
                    node_id: (*node).to_owned(),
                    session_id: value.session_id.clone(),
                },
                VersionedEntity {
                    revision: StoreRevision(7),
                    entity: MetaEntity::NodeSession(value.clone()),
                },
            );
            let request = RequestKey::new(*node, "register-1");
            state.requests.insert(
                request.clone(),
                RequestOutcome {
                    request,
                    operation: StoreOperation::RegisterNode,
                    result: OperationResult::NodeSession(value),
                },
            );
            state.node_epochs.insert((*node).to_owned(), 1);
        }
        StoreState {
            state: Mutex::new(state),
        }
    }

    let bytes = state_in_order(&["node-a", "node-b", "node-c"])
        .snapshot_bytes()
        .unwrap();
    assert_eq!(
        bytes,
        state_in_order(&["node-c", "node-a", "node-b"])
            .snapshot_bytes()
            .unwrap()
    );
    assert_eq!(
        bytes,
        StoreState::from_snapshot(&bytes)
            .unwrap()
            .snapshot_bytes()
            .unwrap()
    );
}

#[test]
fn current_session_match_rejects_expired_session() {
    let mut state = MemoryState::default();
    let expired = session("node-a", "session-1", 0);
    state.entities.insert(
        MetaKey::CurrentNodeSession {
            node_id: "node-a".into(),
        },
        VersionedEntity {
            revision: StoreRevision(1),
            entity: MetaEntity::NodeSession(expired),
        },
    );
    assert!(!current_session_matches(&state, "node-a", "session-1"));

    let live = session("node-a", "session-1", now_unix_ms().saturating_add(60_000));
    state.entities.insert(
        MetaKey::CurrentNodeSession {
            node_id: "node-a".into(),
        },
        VersionedEntity {
            revision: StoreRevision(2),
            entity: MetaEntity::NodeSession(live),
        },
    );
    assert!(current_session_matches(&state, "node-a", "session-1"));
}

#[tokio::test]
async fn root_grant_read_rejects_expired_holder_session() {
    let mut state = MemoryState {
        revision: StoreRevision(3),
        ..MemoryState::default()
    };
    let root = RootRecord {
        root_id: "workspace-1".into(),
        root_epoch: 7,
        home_node_id: "node-a".into(),
        home_session_id: "home-session-2".into(),
        local_prepare_id: "prepare-1".into(),
        created_at_revision: StoreRevision(1),
        updated_at_revision: StoreRevision(1),
    };
    let grant = grant();
    for (key, entity) in [
        (
            MetaKey::Root {
                root_id: root.root_id.clone(),
            },
            MetaEntity::Root(root),
        ),
        (
            MetaKey::CurrentNodeSession {
                node_id: grant.home_node_id.clone(),
            },
            MetaEntity::NodeSession(session(
                &grant.home_node_id,
                &grant.home_session_id,
                now_unix_ms().saturating_add(60_000),
            )),
        ),
        (
            MetaKey::CurrentNodeSession {
                node_id: grant.holder_node_id.clone(),
            },
            MetaEntity::NodeSession(session(&grant.holder_node_id, &grant.holder_session_id, 0)),
        ),
        (
            MetaKey::RootGrant {
                root_id: grant.root_id.clone(),
                holder_node_id: grant.holder_node_id.clone(),
                holder_session_id: grant.holder_session_id.clone(),
            },
            MetaEntity::RootGrant(grant.clone()),
        ),
    ] {
        state.entities.insert(
            key,
            VersionedEntity {
                revision: StoreRevision(3),
                entity,
            },
        );
    }
    let store = StoreState {
        state: Mutex::new(state),
    };
    let snapshot = store
        .read(MetaRead::RootGrant {
            root_id: grant.root_id,
            root_epoch: grant.root_epoch,
            home_session_id: grant.home_session_id,
            holder_node_id: grant.holder_node_id,
            holder_session_id: grant.holder_session_id,
            access_generation: grant.access_generation,
            fencing_token: grant.fencing_token,
        })
        .await
        .unwrap();
    assert!(snapshot.entity.is_none());
}

#[tokio::test]
async fn root_grant_read_rejects_expired_home_session() {
    let mut state = MemoryState {
        revision: StoreRevision(3),
        ..MemoryState::default()
    };
    let root = RootRecord {
        root_id: "workspace-1".into(),
        root_epoch: 7,
        home_node_id: "node-a".into(),
        home_session_id: "home-session-2".into(),
        local_prepare_id: "prepare-1".into(),
        created_at_revision: StoreRevision(1),
        updated_at_revision: StoreRevision(1),
    };
    let grant = grant();
    for (key, entity) in [
        (
            MetaKey::Root {
                root_id: root.root_id.clone(),
            },
            MetaEntity::Root(root),
        ),
        (
            MetaKey::CurrentNodeSession {
                node_id: grant.home_node_id.clone(),
            },
            MetaEntity::NodeSession(session(&grant.home_node_id, &grant.home_session_id, 0)),
        ),
        (
            MetaKey::CurrentNodeSession {
                node_id: grant.holder_node_id.clone(),
            },
            MetaEntity::NodeSession(session(
                &grant.holder_node_id,
                &grant.holder_session_id,
                now_unix_ms().saturating_add(60_000),
            )),
        ),
        (
            MetaKey::RootGrant {
                root_id: grant.root_id.clone(),
                holder_node_id: grant.holder_node_id.clone(),
                holder_session_id: grant.holder_session_id.clone(),
            },
            MetaEntity::RootGrant(grant.clone()),
        ),
    ] {
        state.entities.insert(
            key,
            VersionedEntity {
                revision: StoreRevision(3),
                entity,
            },
        );
    }
    let store = StoreState {
        state: Mutex::new(state),
    };
    let snapshot = store
        .read(MetaRead::RootGrant {
            root_id: grant.root_id,
            root_epoch: grant.root_epoch,
            home_session_id: grant.home_session_id,
            holder_node_id: grant.holder_node_id,
            holder_session_id: grant.holder_session_id,
            access_generation: grant.access_generation,
            fencing_token: grant.fencing_token,
        })
        .await
        .unwrap();
    assert!(snapshot.entity.is_none());
}

#[test]
fn transaction_requires_same_commit_idempotent_outcome() {
    let req = request();
    let mut txn = MetaTxn::new(req.clone(), StoreOperation::AcquireRoot);
    txn.conditions
        .push(TxnCondition::RequestAbsent(req.clone()));
    txn.mutations
        .push(TxnMutation::Put(MetaEntity::RootGrant(grant())));
    assert!(txn.validate().is_err());

    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: req,
            operation: StoreOperation::AcquireRoot,
            result: OperationResult::RootGrant(grant()),
        }));
    txn.validate().unwrap();
}

#[test]
fn transaction_rejects_reused_request_id_for_other_operation() {
    let req = request();
    let mut txn = MetaTxn::new(req.clone(), StoreOperation::RecoverRoot);
    txn.conditions
        .push(TxnCondition::RequestAbsent(req.clone()));
    txn.mutations
        .push(TxnMutation::Put(MetaEntity::RootGrant(grant())));
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: req,
            operation: StoreOperation::AcquireRoot,
            result: OperationResult::RootGrant(grant()),
        }));
    assert!(txn.validate().is_err());
}

fn n2b1_command(
    command_id: &str,
    root_id: &str,
    old_access_generation: u64,
    command_type: RootCommandType,
) -> RootCommandRecord {
    RootCommandRecord {
        command_id: command_id.into(),
        home_node_id: "node-a".into(),
        home_session_id: "session-a".into(),
        root_id: root_id.into(),
        root_epoch: 7,
        old_access_generation,
        command_type,
    }
}

fn n2b1_ack(command_id: &str, root_id: &str, access_generation: u64) -> RootCommandAck {
    RootCommandAck {
        command_id: command_id.into(),
        node_id: "node-a".into(),
        session_id: "session-a".into(),
        root_id: root_id.into(),
        root_epoch: 7,
        access_generation,
        success: true,
        message: "installed".into(),
    }
}

#[test]
fn n2b1_old_root_command_records_default_to_revoke_access() {
    let raw = r#"{
            "command_id":"cmd-old",
            "home_node_id":"node-a",
            "home_session_id":"session-a",
            "root_id":"workspace-a",
            "root_epoch":7,
            "old_access_generation":3
        }"#;
    let command: RootCommandRecord = serde_json::from_str(raw).unwrap();
    assert_eq!(command.command_type, RootCommandType::RevokeAccess);
}

#[test]
fn n2b1_root_command_exact_conditions_match_stored_command_and_ack() {
    let command = n2b1_command("cmd-exact", "workspace-a", 3, RootCommandType::RevokeAccess);
    let ack = n2b1_ack("cmd-exact", "workspace-a", 3);
    let mut state = MemoryState::default();
    state.entities.insert(
        MetaKey::RootCommand {
            command_id: command.command_id.clone(),
        },
        VersionedEntity {
            revision: StoreRevision(1),
            entity: MetaEntity::RootCommand(command),
        },
    );
    state.entities.insert(
        MetaKey::RootCommandAck {
            command_id: ack.command_id.clone(),
            home_node_id: ack.node_id.clone(),
            home_session_id: ack.session_id.clone(),
        },
        VersionedEntity {
            revision: StoreRevision(2),
            entity: MetaEntity::RootCommandAck(ack),
        },
    );

    assert!(condition_matches(
        &state,
        &TxnCondition::RootCommandMatches {
            command_id: "cmd-exact".into(),
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            root_id: "workspace-a".into(),
            root_epoch: 7,
            old_access_generation: 3,
            command_type: RootCommandType::RevokeAccess,
        }
    ));
    assert!(condition_matches(
        &state,
        &TxnCondition::RootCommandAckedExact {
            command_id: "cmd-exact".into(),
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            root_id: "workspace-a".into(),
            root_epoch: 7,
            access_generation: 3,
            command_type: RootCommandType::RevokeAccess,
        }
    ));
    assert!(!condition_matches(
        &state,
        &TxnCondition::RootCommandMatches {
            command_id: "cmd-exact".into(),
            home_node_id: "node-a".into(),
            home_session_id: "session-a".into(),
            root_id: "workspace-a".into(),
            root_epoch: 7,
            old_access_generation: 4,
            command_type: RootCommandType::RevokeAccess,
        }
    ));
    assert!(!condition_matches(
        &state,
        &TxnCondition::RootCommandAckedExact {
            command_id: "cmd-exact".into(),
            node_id: "node-a".into(),
            session_id: "session-a".into(),
            root_id: "workspace-a".into(),
            root_epoch: 7,
            access_generation: 3,
            command_type: RootCommandType::InvalidateCache,
        }
    ));
}

#[test]
fn n2b1_watch_batch_rejects_max_revision_event_without_advance_cursor() {
    let batch = WatchBatch::Events {
        start_revision: StoreRevision(u64::MAX - 2),
        next_revision: StoreRevision(u64::MAX),
        events: vec![WatchEvent {
            revision: StoreRevision(u64::MAX),
            event_index: 0,
            key: MetaKey::Root {
                root_id: "workspace-max".into(),
            },
            change: WatchChange::Delete,
        }],
    };
    assert!(batch.validate_order().is_err());
}

#[tokio::test]
async fn n2b1_watch_limit_preserves_revision_boundary_at_1024() {
    let mut state = MemoryState {
        revision: StoreRevision(1024),
        ..MemoryState::default()
    };
    for revision in 1..=1023 {
        state.events.push(WatchEvent {
            revision: StoreRevision(revision),
            event_index: 0,
            key: MetaKey::Root {
                root_id: format!("workspace-{revision}"),
            },
            change: WatchChange::Delete,
        });
    }
    state
        .events
        .extend([0, 1].into_iter().map(|event_index| WatchEvent {
            revision: StoreRevision(1024),
            event_index,
            key: MetaKey::Root {
                root_id: format!("workspace-boundary-{event_index}"),
            },
            change: WatchChange::Delete,
        }));
    let store = StoreState {
        state: Mutex::new(state),
    };
    let WatchBatch::Events {
        start_revision,
        next_revision,
        events,
    } = store.watch(StoreRevision::ZERO, 1024).await.unwrap()
    else {
        panic!("memory store should return events");
    };
    assert_eq!(start_revision, StoreRevision::ZERO);
    assert_eq!(next_revision, StoreRevision(1025));
    assert_eq!(events.len(), 1025);
    assert_eq!(events[1023].revision, StoreRevision(1024));
    assert_eq!(events[1024].revision, StoreRevision(1024));
}

fn p3_watch_test_event(revision: u64, event_index: u32, name: String) -> WatchEvent {
    WatchEvent {
        revision: StoreRevision(revision),
        event_index,
        key: MetaKey::Root { root_id: name },
        change: WatchChange::Delete,
    }
}
#[test]
fn p3_watch_response_zero_limit_still_bounds_count_at_whole_revision() {
    let mut state = MemoryState::default();
    for n in 1..=16_385 {
        state
            .events
            .push(p3_watch_test_event(n, 0, format!("root-{n}")));
    }
    let WatchBatch::Events {
        events,
        next_revision,
        ..
    } = state.watch(StoreRevision::ZERO, 0).unwrap()
    else {
        panic!("retained history must not be fabricated as compacted");
    };
    assert_eq!(
        events.len(),
        16_384,
        "limit=0 must not disable the hard response bound"
    );
    assert_eq!(next_revision, StoreRevision(16_385));
}
#[test]
fn p3_watch_response_oversize_first_revision_refuses_without_partial_success() {
    let mut state = MemoryState::default();
    let mut name = String::with_capacity(17 * 1024 * 1024);
    name.push('x');
    state.events.push(p3_watch_test_event(1, 0, name));
    assert!(
        state.watch(StoreRevision::ZERO, 1).is_err(),
        "owned spare capacity must participate in response admission before clone"
    );
    assert_eq!(state.events.len(), 1);
    assert_eq!(state.compacted_to, StoreRevision::ZERO);
}
#[test]
fn p3_watch_response_byte_limit_keeps_only_complete_prior_revisions() {
    let mut state = MemoryState::default();
    state.events.push(p3_watch_test_event(1, 0, "small".into()));
    state
        .events
        .push(p3_watch_test_event(2, 0, "first-of-large-revision".into()));
    state
        .events
        .push(p3_watch_test_event(2, 1, "x".repeat(17 * 1024 * 1024)));
    let WatchBatch::Events {
        events,
        next_revision,
        ..
    } = state.watch(StoreRevision::ZERO, 0).unwrap()
    else {
        panic!("response capacity is not history compaction");
    };
    assert_eq!(
        events.len(),
        1,
        "a response must roll back the entire over-budget revision"
    );
    assert_eq!(next_revision, StoreRevision(2));
    assert!(state.watch(StoreRevision(1), 0).is_err());
    assert_eq!(state.compacted_to, StoreRevision::ZERO);
}
#[test]
fn watch_batch_rejects_duplicate_positions_within_a_revision() {
    let batch = WatchBatch::Events {
        start_revision: StoreRevision(10),
        next_revision: StoreRevision(12),
        events: vec![
            WatchEvent {
                revision: StoreRevision(11),
                event_index: 0,
                key: MetaKey::Root {
                    root_id: "workspace-1".into(),
                },
                change: WatchChange::Delete,
            },
            WatchEvent {
                revision: StoreRevision(11),
                event_index: 0,
                key: MetaKey::Root {
                    root_id: "workspace-2".into(),
                },
                change: WatchChange::Delete,
            },
        ],
    };
    assert!(batch.validate_order().is_err());
}

#[test]
fn watch_allows_multiple_ordered_keys_in_one_revision() {
    let batch = WatchBatch::Events {
        start_revision: StoreRevision(10),
        next_revision: StoreRevision(12),
        events: [0, 1]
            .into_iter()
            .map(|event_index| WatchEvent {
                revision: StoreRevision(11),
                event_index,
                key: MetaKey::Root {
                    root_id: format!("workspace-{event_index}"),
                },
                change: WatchChange::Delete,
            })
            .collect(),
    };
    batch.validate_order().unwrap();
}

#[test]
fn unavailable_store_is_explicit_not_an_in_memory_fallback() {
    let error = unavailable_meta_store();
    assert_eq!(error.kind(), afs_error::ErrorKind::Unimplemented);
    assert!(error.message().contains("not configured"));
}

#[test]
fn entity_names_are_stable_for_diagnostics() {
    assert_eq!(MetaEntity::RootGrant(grant()).name(), "root_grant");
}

#[test]
fn meta_store_is_replaceable_behind_a_trait_object() {
    fn accepts_trait_object(_: Option<&dyn MetaStore>) {}
    accepts_trait_object(None);
}
fn lease(session_id: &str) -> NodeSessionLease {
    NodeSessionLease {
        node_id: "node-a".into(),
        session_id: session_id.into(),
        grpc_addr: "http://node-a:7400".into(),
        data_addr: "http://node-a:7500".into(),
        rest_addr: "http://node-a:7600".into(),
        ownerfs_enabled: true,
        storage_devices: Vec::new(),
        lease_ttl: Duration::from_secs(30),
    }
}

#[tokio::test]
async fn replay_and_session_fencing_survive_store_reopen() {
    let backend = Arc::new(journal::MemoryLog::new(Default::default()).unwrap());
    let store = Store::open_log(backend.clone()).await.unwrap();
    let request = RequestKey::new("node-a", "register-1");
    let first = store
        .register_node_session(request.clone(), lease("session-1"))
        .await
        .unwrap();
    assert!(matches!(first, TxnOutcome::Committed { .. }));
    drop(store);

    let reopened = Store::open_log(backend).await.unwrap();
    let retry = reopened
        .register_node_session(request, lease("session-1"))
        .await
        .unwrap();
    assert!(matches!(
        retry,
        TxnOutcome::ConditionFailed {
            existing_outcome: Some(_),
            ..
        }
    ));
    let current = reopened
        .read(MetaRead::CurrentNodeSession {
            node_id: "node-a".into(),
        })
        .await
        .unwrap();
    assert!(
        matches!(current.entity, Some(MetaEntity::NodeSession(session)) if session.session_id == "session-1" && session.lease_epoch == 1)
    );
    reopened
        .register_node_session(RequestKey::new("node-a", "register-2"), lease("session-2"))
        .await
        .unwrap();
    let current = reopened
        .read(MetaRead::CurrentNodeSession {
            node_id: "node-a".into(),
        })
        .await
        .unwrap();
    assert!(
        matches!(current.entity, Some(MetaEntity::NodeSession(session)) if session.session_id == "session-2" && session.lease_epoch == 2)
    );
}

#[tokio::test]
async fn expired_session_id_cannot_be_registered_again() {
    let store = Store::open_log(Arc::new(
        journal::MemoryLog::new(Default::default()).unwrap(),
    ))
    .await
    .unwrap();
    store
        .register_node_session(
            RequestKey::new("node-a", "register-1"),
            NodeSessionLease {
                lease_ttl: Duration::from_nanos(1),
                ..lease("session-1")
            },
        )
        .await
        .unwrap();

    let error = store
        .register_node_session(RequestKey::new("node-a", "register-2"), lease("session-1"))
        .await
        .unwrap_err();
    assert!(error.message().contains("expired node session"));
}

#[tokio::test]
async fn concurrent_commands_keep_distinct_ordered_logical_revisions() {
    let store = Store::open_log(Arc::new(
        journal::MemoryLog::new(Default::default()).unwrap(),
    ))
    .await
    .unwrap();
    let (first, second) = tokio::join!(
        store.register_node_session(RequestKey::new("node-a", "r1"), lease("session-1")),
        store.register_node_session(RequestKey::new("node-a", "r2"), lease("session-2")),
    );
    let (TxnOutcome::Committed { revision: one, .. }, TxnOutcome::Committed { revision: two, .. }) =
        (first.unwrap(), second.unwrap())
    else {
        panic!("both distinct registration requests must commit");
    };
    assert_ne!(one, two);
    let batch = store.watch(StoreRevision::ZERO, 1).await.unwrap();
    batch.validate_order().unwrap();
    let WatchBatch::Events { events, .. } = batch else {
        panic!("memory store must retain these events");
    };
    let earlier = one.min(two);
    let later = one.max(two);
    assert!(events.iter().any(|event| event.revision == earlier));
    assert!(!events.iter().any(|event| event.revision == later));
}

struct GatedBackend {
    inner: journal::MemoryLog,
    started: Notify,
    release: Notify,
    fail: bool,
}

impl MetaLog for GatedBackend {
    fn persistence(&self) -> BackendPersistence {
        self.inner.persistence()
    }
    fn limits(&self) -> journal::LogLimits {
        self.inner.limits()
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        self.inner.health()
    }
    fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
        self.inner.recover()
    }
    fn append_and_sync(
        &self,
        expected: journal::LogTip,
        records: Vec<log::Record>,
    ) -> MetaFuture<'_, journal::LogTip> {
        Box::pin(async move {
            self.started.notify_one();
            self.release.notified().await;
            if self.fail {
                Err(unavailable("injected disk error"))
            } else {
                self.inner.append_and_sync(expected, records).await
            }
        })
    }
}

struct CountingBackend {
    inner: journal::MemoryLog,
    commits: Arc<std::sync::atomic::AtomicUsize>,
    last_record_bytes: Arc<std::sync::atomic::AtomicUsize>,
}

impl MetaLog for CountingBackend {
    fn persistence(&self) -> BackendPersistence {
        self.inner.persistence()
    }
    fn limits(&self) -> journal::LogLimits {
        self.inner.limits()
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        self.inner.health()
    }
    fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
        self.inner.recover()
    }
    fn append_and_sync(
        &self,
        expected: journal::LogTip,
        records: Vec<log::Record>,
    ) -> MetaFuture<'_, journal::LogTip> {
        self.commits
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let bytes = journal::encode_batch(
            journal::StoreIdentity {
                store_id: [1; 16],
                cluster_id: [2; 16],
                incarnation: [3; 16],
            },
            expected,
            records.clone(),
            self.limits(),
        )
        .map(|(_, _, bytes)| bytes);
        Box::pin(async move {
            self.last_record_bytes
                .store(bytes?, std::sync::atomic::Ordering::SeqCst);
            self.inner.append_and_sync(expected, records).await
        })
    }
}

fn view_limits() -> ReadLimits {
    ReadLimits {
        views: 2,
        held_bytes: 1024 * 1024,
        lifetime: Duration::from_millis(80),
        reap: Duration::from_millis(5),
    }
}
#[test]
fn p3_views_charge_includes_authority_owned_capacity() {
    let mut state = State {
        revision: StoreRevision(1),
        ..State::default()
    };
    let mut text = String::with_capacity(128 * 1024);
    text.push('x');
    let devices = Vec::<StorageDeviceDescriptor>::with_capacity(32);
    let minimum =
        text.capacity() + devices.capacity() * std::mem::size_of::<StorageDeviceDescriptor>();
    state.entities.insert(
        MetaKey::CurrentNodeSession {
            node_id: "n".into(),
        },
        state::VersionedEntity {
            revision: StoreRevision(1),
            entity: MetaEntity::NodeSession(NodeSession {
                node_id: "n".into(),
                session_id: "s".into(),
                grpc_addr: String::new(),
                data_addr: String::new(),
                rest_addr: text,
                ownerfs_enabled: true,
                storage_devices: devices,
                lease_epoch: 1,
                expires_at_unix_ms: u64::MAX,
            }),
        },
    );
    state.ensure_history_index().unwrap();
    assert!(
        state.pin_charge().unwrap() >= minimum as u64,
        "a view pins authority payloads, not just history or pointers"
    );
}
#[test]
fn p3_views_request_completion_does_not_hide_input_spare_capacity() {
    let mut request_id = String::with_capacity(128 * 1024);
    request_id.push_str("id");
    let (state, _, _) = State::default()
        .prepare_node_record(
            RequestKey::new("view", request_id),
            lease("s"),
            now_unix_ms(),
            1024 * 1024,
        )
        .unwrap();
    let retained = state
        .request_completion
        .iter()
        .map(|(key, _)| key.caller_id.capacity() + key.request_id.capacity())
        .sum::<usize>();
    assert!(
        state.history_stats().request_bytes >= retained as u64,
        "completion keys cannot retain large uncharged admission capacity"
    );
}
#[test]
fn p3_views_incremental_authority_charge_matches_full_rebuild_and_image() {
    let mut state = State::default();
    for n in 0..4 {
        state = state
            .prepare_node_record(
                RequestKey::new("view-charge", format!("r{n}")),
                lease(&format!("s{n}")),
                now_unix_ms(),
                1024 * 1024,
            )
            .unwrap()
            .0;
        let expected = state.pin_charge().unwrap();
        let mut rebuilt = state.clone();
        rebuilt.authority_indexed = false;
        rebuilt.history_indexed = false;
        rebuilt.ensure_history_index().unwrap();
        assert_eq!(
            rebuilt.pin_charge().unwrap(),
            expected,
            "incremental owner charges must agree with a full reconstruction"
        );
        let bytes = log::StateImage::encode(&state, 1024 * 1024).unwrap();
        let restored = log::StateImage::decode(&bytes, 1024 * 1024).unwrap();
        assert_eq!(
            restored.pin_charge().unwrap(),
            expected,
            "normal compact capacities remain equal after durable image restore"
        );
    }
}
#[tokio::test]
async fn p3_views_same_state_shares_bytes_but_counts_each_view() {
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let mut limits = view_limits();
    // This case tests quota/sharing, not deadline scheduling.
    limits.lifetime = Duration::from_secs(5);
    let store = Store::open_log_with_read_limits(log, limits).await.unwrap();
    let first = store.read_view().await.unwrap();
    let before = store.read_budget_stats();
    assert_eq!(before.views, 1);
    assert!(before.bytes > 0);
    let second = store.read_view().await.unwrap();
    assert_eq!(store.read_budget_stats().views, 2);
    assert_eq!(store.read_budget_stats().bytes, before.bytes);
    assert!(
        store.read_view().await.is_err(),
        "view count must be finite"
    );
    store
        .register_node_session(RequestKey::new("view", "write"), lease("new"))
        .await
        .unwrap();
    assert_eq!(
        first
            .read(MetaRead::CurrentNodeSessions)
            .await
            .unwrap()
            .revision,
        StoreRevision::ZERO
    );
    drop(second);
    let fresh = store.read_view().await.unwrap();
    assert_eq!(
        fresh
            .read(MetaRead::CurrentNodeSessions)
            .await
            .unwrap()
            .revision,
        StoreRevision(1)
    );
    drop(first);
    drop(fresh);
    assert_eq!(store.read_budget_stats().views, 0);
    assert_eq!(store.read_budget_stats().bytes, 0);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn p3_views_registry_stays_bounded_during_concurrent_fast_drop() {
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let mut limits = view_limits();
    limits.lifetime = Duration::from_secs(5);
    let store = Arc::new(Store::open_log_with_read_limits(log, limits).await.unwrap());
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let store = store.clone();
        tasks.spawn(async move {
            for n in 0..64 {
                if let Ok(view) = store.read_view().await {
                    if n % 3 == 0 {
                        tokio::task::yield_now().await;
                    }
                    drop(view);
                }
                let registry = store.read_budget.registry.lock().unwrap();
                assert!(
                    registry.views.len() <= limits.views,
                    "weak view registry exceeded its independent bound"
                );
                assert!(
                    registry.allocations.len() <= limits.views,
                    "weak state registry exceeded its independent bound"
                );
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    assert_eq!(store.read_budget_stats().views, 0);
    assert_eq!(store.read_budget_stats().bytes, 0);
    store
        .register_node_session(RequestKey::new("view", "after-fast-drop"), lease("new"))
        .await
        .unwrap();
}
#[tokio::test]
async fn p3_views_byte_refusal_is_before_pin_and_does_not_block_writer() {
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let mut limits = view_limits();
    limits.held_bytes = 4096;
    let store = Store::open_log_with_read_limits(log, limits).await.unwrap();
    assert!(
        store.read_view().await.is_err(),
        "an oversized state cannot bypass pin bytes"
    );
    assert_eq!(store.read_budget_stats().views, 0);
    assert_eq!(store.read_budget_stats().bytes, 0);
    store
        .register_node_session(RequestKey::new("view", "write"), lease("new"))
        .await
        .unwrap();
    store.health().await.unwrap();
}
#[tokio::test]
async fn p3_views_expiry_reclaims_state_without_caller_polling() {
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let store = Store::open_log_with_read_limits(log, view_limits())
        .await
        .unwrap();
    let view = store.read_view().await.unwrap();
    let old = Arc::downgrade(&view.state_for_test());
    store
        .register_node_session(RequestKey::new("view", "next"), lease("new"))
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while old.upgrade().is_some() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        old.upgrade().is_none(),
        "retaining an idle handle must not pin its old state forever"
    );
    assert!(view.read(MetaRead::CurrentNodeSessions).await.is_err());
    assert!(view.lookup(&MetaKey::CurrentNodeSessions).is_err());
    assert!(view.snapshot().is_err());
    assert!(view.statistics().is_err());
    assert_eq!(store.read_budget_stats().views, 0);
    assert_eq!(store.read_budget_stats().bytes, 0);
}
#[tokio::test]
async fn p3_views_known_failed_authority_refuses_existing_view() {
    let log = Arc::new(GatedLog {
        inner: Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap()),
        started: Notify::new(),
        release: Notify::new(),
        fail: true,
        persist_before_error: true,
    });
    let store = Arc::new(
        Store::open_log_with_read_limits(log.clone(), ReadLimits::default())
            .await
            .unwrap(),
    );
    let view = store.read_view().await.unwrap();
    let writer = store.clone();
    let task = tokio::spawn(async move {
        writer
            .register_node_session(RequestKey::new("view", "unknown"), lease("new"))
            .await
    });
    log.started.notified().await;
    log.release.notify_one();
    assert!(task.await.unwrap().is_err());
    assert!(
        view.read(MetaRead::CurrentNodeSessions).await.is_err(),
        "a known failed authority cannot serve cached authorization facts"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p3_views_expired_active_read_retains_charge_until_release() {
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let store = Store::open_log_with_read_limits(log, view_limits())
        .await
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let pause = Arc::new(state::ReadPause::new(entered_tx));
    install_read_pause(&store, pause.clone()).await;
    let view = Arc::new(store.read_view().await.unwrap());
    let charged = store.read_budget_stats();
    assert!(charged.bytes > 0);
    let reader = view.clone();
    let runtime = tokio::runtime::Handle::current();
    let task =
        std::thread::spawn(move || runtime.block_on(reader.read(MetaRead::CurrentNodeSessions)));
    let entered = entered_rx.recv_timeout(Duration::from_secs(1));
    if entered.is_err() {
        pause.release();
        let _ = task.join();
        panic!("view reader missed gate");
    }
    tokio::time::sleep(Duration::from_millis(160)).await;
    let during = store.read_budget_stats();
    pause.release();
    let result = task.join().unwrap();
    assert_eq!(during.views, 1);
    assert_eq!(
        during.bytes, charged.bytes,
        "active reader still owns allocation and slot"
    );
    assert!(
        result.is_err(),
        "expired work cannot deliver a success after the pause"
    );
    assert_eq!(store.read_budget_stats().views, 0);
    assert_eq!(store.read_budget_stats().bytes, 0);
}

fn retention_limits() -> history::Limits {
    history::Limits {
        request_count: 2,
        event_count: 4,
        request_bytes: 1024 * 1024,
        event_bytes: 1024 * 1024,
        cleanup_items: 4,
        ..history::Limits::default()
    }
}
async fn stop_local_retention_store(store: Store, log: Arc<local_log::LocalLog>) {
    drop(store);
    tokio::time::timeout(Duration::from_secs(2), async {
        while Arc::strong_count(&log) != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("closed writer and checkpoint observer must release the local log");
    drop(log);
}
#[tokio::test]
async fn p3_retention_local_runtime_expiry_checkpoint_reopen_keeps_removal() {
    let dir = tempfile::tempdir().unwrap();
    let local =
        Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
    let identity = local.identity();
    let mut limits = retention_limits();
    limits.request_retention = Duration::from_millis(100);
    limits.event_retention = Duration::from_millis(100);
    let store = Store::open_log_with_history(local.clone(), limits)
        .await
        .unwrap();
    let key = RequestKey::new("local-retention", "expired");
    store
        .register_node_session(key.clone(), lease("session"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let state = store.committed().await.unwrap();
            if !state.requests.contains_key(&key) && state.compacted_to == StoreRevision(1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual local WAL must durably expire the idle history");
    let tip = store.checkpoint().await.unwrap();
    assert!(tip.sequence >= 2);
    stop_local_retention_store(store, local).await;

    let local =
        Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
    assert_eq!(local.identity(), identity);
    // Raising the retention window must not resurrect an explicit removal.
    let store = Store::open_log_with_history(local.clone(), retention_limits())
        .await
        .unwrap();
    let state = store.committed().await.unwrap();
    assert_eq!(state.revision, StoreRevision(1));
    assert_eq!(state.log_tip, tip);
    assert!(!state.requests.contains_key(&key));
    assert_eq!(state.compacted_to, StoreRevision(1));
    assert!(matches!(
        store.watch(StoreRevision::ZERO, 1).await.unwrap(),
        WatchBatch::Compacted { .. }
    ));
    assert!(matches!(
        store
            .register_node_session(key, lease("session"))
            .await
            .unwrap(),
        TxnOutcome::Committed {
            revision: StoreRevision(2),
            ..
        }
    ));
    stop_local_retention_store(store, local).await;
}
#[tokio::test]
async fn p3_retention_local_runtime_lowered_limits_persist_before_admission() {
    let dir = tempfile::tempdir().unwrap();
    let local =
        Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
    let identity = local.identity();
    let wide = history::Limits {
        request_count: 8,
        event_count: 16,
        ..retention_limits()
    };
    let store = Store::open_log_with_history(local.clone(), wide)
        .await
        .unwrap();
    for n in 0..4 {
        store
            .register_node_session(
                RequestKey::new("local-retention", format!("r{n}")),
                lease(&format!("session-{n}")),
            )
            .await
            .unwrap();
    }
    let before = store.committed().await.unwrap();
    assert_eq!(before.requests.len(), 4);
    let before_tip = before.log_tip;
    stop_local_retention_store(store, local).await;

    let local =
        Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
    assert_eq!(local.identity(), identity);
    let store = Store::open_log_with_history(local.clone(), retention_limits())
        .await
        .unwrap();
    let state = store.committed().await.unwrap();
    assert_eq!(state.revision, StoreRevision(4));
    assert_eq!(state.history_stats().requests, 2);
    assert_eq!(state.history_stats().events, 4);
    assert_eq!(state.compacted_to, StoreRevision(2));
    assert!(state.log_tip.sequence > before_tip.sequence);
    let cleaned_tip = state.log_tip;
    for n in 0..4 {
        assert_eq!(
            state
                .requests
                .contains_key(&RequestKey::new("local-retention", format!("r{n}"))),
            n >= 2
        );
    }
    stop_local_retention_store(store, local).await;

    let local =
        Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
    assert_eq!(local.identity(), identity);
    let store = Store::open_log_with_history(local.clone(), wide)
        .await
        .unwrap();
    let recovered = store.committed().await.unwrap();
    assert_eq!(recovered.log_tip, cleaned_tip);
    assert_eq!(recovered.revision, StoreRevision(4));
    assert_eq!(recovered.requests.len(), 2);
    assert_eq!(recovered.compacted_to, StoreRevision(2));
    assert_eq!(
        recovered.history_stats().request_bytes,
        state.history_stats().request_bytes
    );
    assert_eq!(
        recovered.history_stats().event_bytes,
        state.history_stats().event_bytes
    );
    stop_local_retention_store(store, local).await;
}
#[tokio::test]
async fn p3_retention_runtime_capacity_and_rebuild_follow_actual_outcomes() {
    use journal::MetaLog;
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let store = Store::open_log_with_history(log.clone(), retention_limits())
        .await
        .unwrap();
    for n in 0..4 {
        store
            .register_node_session(
                RequestKey::new("retained", format!("r{n}")),
                lease(&format!("session-{n}")),
            )
            .await
            .unwrap();
    }
    let view = store.read_view().await.unwrap();
    let stats = view.state_for_test().history_stats();
    assert_eq!(
        stats.requests, 2,
        "running writer must apply count cleanup, not only expose a selector"
    );
    assert_eq!(stats.events, 4);
    assert!(stats.request_bytes > 0 && stats.event_bytes > 0);
    assert!(
        store
            .read(MetaRead::RequestOutcome(RequestKey::new("retained", "r0")))
            .await
            .unwrap()
            .request_outcome
            .is_none()
    );
    assert!(matches!(
        store.watch(StoreRevision::ZERO, 1).await.unwrap(),
        WatchBatch::Compacted {
            compacted_to: StoreRevision(2),
            ..
        }
    ));
    let recovered = log.recover().await.unwrap();
    let mut replayed = State::default();
    for record in recovered.records {
        replayed = replayed.replay_record(&record).unwrap();
    }
    assert_eq!(replayed.history_stats().requests, 2);
    assert_eq!(replayed.history_stats().request_bytes, stats.request_bytes);
    let bytes = log::StateImage::encode(&replayed, 1024 * 1024).unwrap();
    let restored = log::StateImage::decode(&bytes, 1024 * 1024).unwrap();
    assert_eq!(restored.history_stats().request_bytes, stats.request_bytes);
    assert_eq!(restored.history_stats().event_bytes, stats.event_bytes);
}
#[test]
fn p3_retention_in_flight_capacity_and_retirement_admission_bounds() {
    let registry = Arc::new(InFlight {
        state: Mutex::new(InFlightState::default()),
        limit: 1,
    });
    let key = RequestKey::new("admission", "original");
    let operation = Mutation::RegisterNode(key.clone(), lease("one"));
    let first = registry.admit(&operation).unwrap();
    let duplicate = registry.admit(&operation).unwrap();
    assert!(
        registry
            .admit(&Mutation::RegisterNode(
                RequestKey::new("admission", "other"),
                lease("two")
            ))
            .is_err()
    );
    drop(first);
    assert_eq!(registry.state.lock().unwrap().active.get(&key), Some(&1));
    drop(duplicate);
    assert_eq!(registry.state.lock().unwrap().bytes, 0);
    let (state, _, _) = State::default()
        .prepare_node_record(key.clone(), lease("one"), 1, 65536)
        .unwrap();
    let limits = history::Limits {
        request_retention: Duration::from_millis(1),
        event_retention: Duration::from_millis(1),
        ..Default::default()
    };
    let (_, delta, retiring) = registry
        .prepare_history(&state, 100, limits)
        .unwrap()
        .unwrap();
    assert_eq!(delta.requests.len(), 1);
    assert!(
        registry.admit(&operation).is_err(),
        "admission cannot race a selected durable retirement"
    );
    drop(retiring);
    let admitted = registry.admit(&operation).unwrap();
    drop(admitted);
    assert_eq!(registry.state.lock().unwrap().bytes, 0);
    let large = Mutation::RegisterNode(
        RequestKey::new("admission", "x".repeat(PROTECTED_KEY_BYTES as usize / 4)),
        lease("large"),
    );
    assert!(
        registry.admit(&large).is_err(),
        "all identity copies must fit the existing4MiB protection budget"
    );
    let state = registry.state.lock().unwrap();
    assert_eq!(state.bytes, 0);
    assert!(state.active.is_empty() && state.retiring.is_empty() && !state.failed);
}

#[tokio::test(flavor = "current_thread")]
async fn p3_retention_queued_retry_survives_cleanup_deadline() {
    let log = Arc::new(GatedLog {
        inner: Arc::new(journal::MemoryLog::new(Default::default()).unwrap()),
        started: Notify::new(),
        release: Notify::new(),
        fail: false,
        persist_before_error: false,
    });
    let mut policy = retention_limits();
    policy.request_retention = Duration::from_millis(100);
    policy.event_retention = Duration::from_millis(100);
    let store = Arc::new(
        Store::open_log_with_history(log.clone(), policy)
            .await
            .unwrap(),
    );
    let key = RequestKey::new("queued-retained", "same-operation");
    log.release.notify_one();
    let first = store
        .register_node_session(key.clone(), lease("original"))
        .await
        .unwrap();
    log.started.notified().await;
    let TxnOutcome::Committed {
        outcome: expected, ..
    } = first
    else {
        panic!("seed was not committed");
    };
    let writer = store.clone();
    let blocker = tokio::spawn(async move {
        let mut other = lease("blocked");
        other.node_id = "node-blocked".into();
        writer
            .register_node_session(RequestKey::new("queued-retained", "blocker"), other)
            .await
    });
    log.started.notified().await;
    let mut retry = store.register_node_session(key.clone(), lease("must-not-execute"));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(retry.as_mut().poll(&mut context).is_pending());
    assert_eq!(
        store.writes.capacity(),
        1023,
        "retry must actually be queued while the prior ACK is blocked"
    );
    // Unlike blocking the executor, this lets the timer driver mark expiry
    // ready while the writer is still inside the preceding durable ACK.
    tokio::time::sleep(Duration::from_millis(220)).await;
    let pump_log = log.clone();
    let pump = tokio::spawn(async move {
        loop {
            pump_log.started.notified().await;
            pump_log.release.notify_one();
        }
    });
    log.release.notify_one();
    let actual = retry.await.unwrap();
    blocker.await.unwrap().unwrap();
    assert!(
        matches!(actual, TxnOutcome::ConditionFailed { existing_outcome: Some(outcome), .. } if outcome == expected),
        "queued retry lost its admitted outcome and executed again"
    );
    assert_eq!(
        store.visible.committed.load().node_epochs.get("node-a"),
        Some(&1)
    );
    tokio::time::sleep(Duration::from_millis(220)).await;
    assert!(
        store
            .read(MetaRead::RequestOutcome(key))
            .await
            .unwrap()
            .request_outcome
            .is_none(),
        "settled admission protection must not become permanent retention"
    );
    drop(store);
    tokio::time::timeout(Duration::from_secs(2), async {
        while Arc::strong_count(&log) > 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    pump.abort();
    assert!(pump.await.unwrap_err().is_cancelled());
}

#[tokio::test]
async fn p3_lifecycle_completed_commands_reclaimed_pending_and_failed_preserved() {
    use journal::MetaLog;
    let backend = Arc::new(journal::MemoryLog::new(Default::default()).unwrap());
    let policy = history::Limits {
        request_count: 128,
        event_count: 512,
        cleanup_items: 32,
        request_retention: Duration::from_millis(50),
        event_retention: Duration::from_millis(50),
        ..Default::default()
    };
    let store = Store::open_log_with_history(backend.clone(), policy)
        .await
        .unwrap();
    let mut old_lease = lease("expired-id");
    old_lease.lease_ttl = Duration::from_millis(1);
    store
        .register_node_session(RequestKey::new("lifecycle", "old-session"), old_lease)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    store
        .register_node_session(
            RequestKey::new("lifecycle", "current-session"),
            lease("current-id"),
        )
        .await
        .unwrap();
    let make_command = |id: &str| RootCommandRecord {
        command_id: id.into(),
        home_node_id: "old-home".into(),
        home_session_id: "old-session".into(),
        root_id: "removed-root".into(),
        root_epoch: 7,
        old_access_generation: 3,
        command_type: RootCommandType::RevokeAccess,
    };
    let make_ack = |id: &str, success| RootCommandAck {
        command_id: id.into(),
        node_id: "old-home".into(),
        session_id: "old-session".into(),
        root_id: "removed-root".into(),
        root_epoch: 7,
        access_generation: 3,
        success,
        message: String::new(),
    };
    let mut active_command = make_command("active");
    active_command.root_id = "active-root".into();
    let mut active_ack = make_ack("active", true);
    active_ack.root_id = "active-root".into();
    let mut mismatched_ack = make_ack("mismatch", true);
    mismatched_ack.root_id = "wrong-root".into();
    let key = RequestKey::new("lifecycle", "seed");
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::AckRootCommand);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    txn.mutations.extend([
        TxnMutation::Put(MetaEntity::RootCommand(make_command("done"))),
        TxnMutation::Put(MetaEntity::RootCommandAck(make_ack("done", true))),
        TxnMutation::Put(MetaEntity::RootCommand(make_command("failed"))),
        TxnMutation::Put(MetaEntity::RootCommandAck(make_ack("failed", false))),
        TxnMutation::Put(MetaEntity::RootCommand(make_command("pending"))),
        TxnMutation::Put(MetaEntity::RootCommand(active_command)),
        TxnMutation::Put(MetaEntity::RootCommandAck(active_ack)),
        TxnMutation::Put(MetaEntity::Root(RootRecord {
            root_id: "active-root".into(),
            root_epoch: 7,
            home_node_id: "old-home".into(),
            home_session_id: "old-session".into(),
            local_prepare_id: "prepare".into(),
            created_at_revision: StoreRevision(1),
            updated_at_revision: StoreRevision(1),
        })),
        TxnMutation::Put(MetaEntity::RootGrant(RootAccessGrant {
            root_id: "active-root".into(),
            root_epoch: 7,
            home_node_id: "old-home".into(),
            home_session_id: "old-session".into(),
            holder_node_id: "old-home".into(),
            holder_session_id: "old-session".into(),
            access_generation: 3,
            rights: vec![RootRight::Read],
            fencing_token: "current-barrier".into(),
            issued_at_revision: StoreRevision(1),
        })),
        TxnMutation::Put(MetaEntity::RootCommand(make_command("mismatch"))),
        TxnMutation::Put(MetaEntity::RootCommandAck(mismatched_ack)),
        TxnMutation::Put(MetaEntity::RootCommandAck(make_ack("orphan", true))),
        TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key.clone(),
            operation: StoreOperation::AckRootCommand,
            result: OperationResult::RootCommandAck(make_ack("done", true)),
        }),
    ]);
    assert!(matches!(
        store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::Committed { .. }
    ));
    let done = MetaKey::RootCommand {
        command_id: "done".into(),
    };
    assert!(
        store.visible.committed.load().entities.contains_key(&done),
        "retained outcome/watch still protect completed command"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while store.visible.committed.load().entities.contains_key(&done)
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let state = store.visible.committed.load_full();
    assert!(
        !state.entities.contains_key(&done),
        "completed command remained after outcome and watch retention expired"
    );
    assert!(!state.entities.contains_key(&MetaKey::RootCommandAck {
        command_id: "done".into(),
        home_node_id: "old-home".into(),
        home_session_id: "old-session".into()
    }));
    for id in ["failed", "pending", "active", "mismatch"] {
        assert!(state.entities.contains_key(&MetaKey::RootCommand {
            command_id: id.into()
        }));
    }
    assert!(state.entities.contains_key(&MetaKey::RootCommandAck {
        command_id: "failed".into(),
        home_node_id: "old-home".into(),
        home_session_id: "old-session".into()
    }));
    assert!(state.entities.contains_key(&MetaKey::RootCommandAck {
        command_id: "orphan".into(),
        home_node_id: "old-home".into(),
        home_session_id: "old-session".into()
    }));
    assert_eq!(state.root_epoch_watermark(), 7);
    assert!(
        state.entities.contains_key(&MetaKey::NodeSession {
            node_id: "node-a".into(),
            session_id: "expired-id".into()
        }),
        "expired-ID rejection record must remain"
    );
    assert!(
        store
            .register_node_session(
                RequestKey::new("lifecycle", "reuse-expired"),
                lease("expired-id")
            )
            .await
            .is_err()
    );
    assert_eq!(
        store.visible.committed.load().node_epochs.get("node-a"),
        Some(&2)
    );
    let recovered = backend.recover().await.unwrap();
    let mut replay = State::default();
    for record in recovered.records {
        replay = replay.replay_record(&record).unwrap();
    }
    assert!(!replay.entities.contains_key(&done));
    assert_eq!(replay.root_epoch_watermark(), 7);
    let image = log::StateImage::encode(&replay, 1024 * 1024).unwrap();
    let restored = log::StateImage::decode(&image, 1024 * 1024).unwrap();
    assert_eq!(restored.root_epoch_watermark(), 7);
    assert_eq!(restored.pin_charge().unwrap(), replay.pin_charge().unwrap());
}

#[tokio::test]
async fn p3_retention_runtime_idle_expiry_is_a_durable_management_change() {
    use journal::MetaLog;
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let mut policy = retention_limits();
    policy.request_retention = Duration::from_millis(100);
    policy.event_retention = Duration::from_millis(100);
    let store = Store::open_log_with_history(log.clone(), policy)
        .await
        .unwrap();
    let key = RequestKey::new("retained", "expires");
    store
        .register_node_session(key.clone(), lease("session"))
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if store
            .read(MetaRead::RequestOutcome(key.clone()))
            .await
            .unwrap()
            .request_outcome
            .is_none()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "idle expiry was not persisted and published"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        store.read_view().await.unwrap().state_for_test().revision,
        StoreRevision(1)
    );
    let recovered = log.recover().await.unwrap();
    assert!(recovered.tip.sequence >= 2);
    let mut replayed = State::default();
    for record in recovered.records {
        replayed = replayed.replay_record(&record).unwrap();
    }
    assert!(!replayed.requests.contains_key(&key));
    assert_eq!(replayed.compacted_to, StoreRevision(1));
    let retry = store
        .register_node_session(key, lease("session"))
        .await
        .unwrap();
    assert!(
        matches!(
            retry,
            TxnOutcome::Committed {
                revision: StoreRevision(2),
                ..
            }
        ),
        "outside retention is not permanent exactly-once"
    );
}
#[tokio::test]
async fn p3_retention_runtime_busy_requests_do_not_reset_cleanup_deadline() {
    use journal::MetaLog;
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let mut policy = retention_limits();
    policy.request_retention = Duration::from_millis(200);
    policy.event_retention = Duration::from_millis(200);
    let store = Store::open_log_with_history(log.clone(), policy)
        .await
        .unwrap();
    let key = RequestKey::new("retained", "busy-retry");
    store
        .register_node_session(key.clone(), lease("session"))
        .await
        .unwrap();
    // Keep the writer active without generating new decisions. A timer
    // created anew by every recv iteration would never reach its deadline.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut attempts = 0usize;
    let mut recommitted = false;
    while tokio::time::Instant::now() < deadline {
        attempts += 1;
        let outcome = store
            .register_node_session(key.clone(), lease("session"))
            .await
            .unwrap();
        if matches!(outcome, TxnOutcome::Committed { .. }) {
            recommitted = true;
            break;
        }
    }
    println!("P3 busy expiry attempts={attempts} recommitted={recommitted}");
    assert!(
        attempts >= 32,
        "test did not exercise a sustained ready writer"
    );
    assert!(
        recommitted,
        "ready retries starved the durable expiry record"
    );
    let recovered = log.recover().await.unwrap();
    assert!(
        recovered
            .records
            .iter()
            .any(|record| matches!(record, log::Record::History(_)))
    );
    let mut replayed = State::default();
    for record in recovered.records {
        replayed = replayed.replay_record(&record).unwrap();
    }
    assert_eq!(replayed.revision, StoreRevision(2));
    assert_eq!(replayed.compacted_to, StoreRevision(1));
}
#[tokio::test]
async fn p3_retention_runtime_protected_new_outcome_cannot_bypass_byte_cap() {
    use journal::MetaLog;
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let mut policy = retention_limits();
    policy.request_bytes = 1;
    let store = Store::open_log_with_history(log.clone(), policy)
        .await
        .unwrap();
    assert!(
        store
            .register_node_session(RequestKey::new("retained", "too-large"), lease("session"))
            .await
            .is_err(),
        "protected result does not exempt its bytes"
    );
    assert_eq!(
        store.read_view().await.unwrap().state_for_test().revision,
        StoreRevision::ZERO
    );
    assert_eq!(log.recover().await.unwrap().tip.sequence, 0);
    store.health().await.unwrap();
}
#[tokio::test]
async fn p3_retention_runtime_cleanup_stays_private_until_ack() {
    use journal::MetaLog;
    let inner = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let mut state = State::default();
    let mut records = Vec::new();
    for n in 0..2 {
        let (next, _, record) = state
            .prepare_node_record(
                RequestKey::new("log-test", format!("request-{n}")),
                lease(&format!("old-{n}")),
                now_unix_ms(),
                1024 * 1024,
            )
            .unwrap();
        state = next;
        records.push(log::Record::from(record.unwrap()));
    }
    inner
        .append_and_sync(journal::LogTip::EMPTY, records)
        .await
        .unwrap();
    let log = Arc::new(GatedLog {
        inner,
        started: Notify::new(),
        release: Notify::new(),
        fail: false,
        persist_before_error: false,
    });
    let store = Arc::new(
        Store::open_log_with_history(log.clone(), retention_limits())
            .await
            .unwrap(),
    );
    let writer = store.clone();
    let task = tokio::spawn(async move {
        writer
            .register_node_session(RequestKey::new("retained", "new"), lease("new"))
            .await
    });
    log.started.notified().await;
    let key = RequestKey::new("log-test", "request-0");
    assert!(
        store
            .read(MetaRead::RequestOutcome(key.clone()))
            .await
            .unwrap()
            .request_outcome
            .is_some()
    );
    assert!(matches!(
        store.watch(StoreRevision::ZERO, 1).await.unwrap(),
        WatchBatch::Events { .. }
    ));
    assert!(!task.is_finished());
    log.release.notify_one();
    task.await.unwrap().unwrap();
    assert!(
        store
            .read(MetaRead::RequestOutcome(key))
            .await
            .unwrap()
            .request_outcome
            .is_none(),
        "cleanup must publish with the acknowledged new decision"
    );
    assert!(matches!(
        store.watch(StoreRevision::ZERO, 1).await.unwrap(),
        WatchBatch::Compacted { .. }
    ));
}
#[tokio::test]
async fn p3_retention_runtime_input_capacity_rejects_before_enqueue() {
    use journal::MetaLog;
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let mut store = Store::open_log(log.clone()).await.unwrap();
    store.pending_bytes = Arc::new(Semaphore::new(1));
    assert!(
        store
            .register_node_session(RequestKey::new("retained", "input-cap"), lease("new"))
            .await
            .is_err()
    );
    assert_eq!(log.recover().await.unwrap().tip.sequence, 0);
    assert_eq!(
        store.read_view().await.unwrap().state_for_test().revision,
        StoreRevision::ZERO
    );
    store.health().await.unwrap();
}
#[tokio::test]
async fn p3_retention_runtime_cancelled_caller_keeps_input_charge_until_ack() {
    let log = Arc::new(GatedLog {
        inner: Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap()),
        started: Notify::new(),
        release: Notify::new(),
        fail: false,
        persist_before_error: false,
    });
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    let budget = store.pending_bytes.clone();
    let full = budget.available_permits();
    let writer = store.clone();
    let key = RequestKey::new("retained", "cancel");
    let caller_key = key.clone();
    let task =
        tokio::spawn(async move { writer.register_node_session(caller_key, lease("new")).await });
    log.started.notified().await;
    let charged = budget.available_permits();
    assert!(charged < full);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        budget.available_permits(),
        charged,
        "caller cancellation cannot return writer-owned input quota"
    );
    assert!(
        store
            .in_flight
            .state
            .lock()
            .unwrap()
            .active
            .contains_key(&key),
        "cancelled queued caller must retain its request identity"
    );
    log.release.notify_one();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while budget.available_permits() != full {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        store
            .read(MetaRead::RequestOutcome(key))
            .await
            .unwrap()
            .request_outcome
            .is_some()
    );
}
#[tokio::test]
async fn p3_retention_runtime_unknown_ack_stops_all_following_cleanup() {
    use journal::MetaLog;
    let inner = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let mut state = State::default();
    let mut records = Vec::new();
    for n in 0..2 {
        let (next, _, record) = state
            .prepare_node_record(
                RequestKey::new("old", format!("r{n}")),
                lease(&format!("old-{n}")),
                now_unix_ms(),
                1024 * 1024,
            )
            .unwrap();
        state = next;
        records.push(log::Record::from(record.unwrap()));
    }
    inner
        .append_and_sync(journal::LogTip::EMPTY, records)
        .await
        .unwrap();
    let log = Arc::new(GatedLog {
        inner: inner.clone(),
        started: Notify::new(),
        release: Notify::new(),
        fail: true,
        persist_before_error: true,
    });
    let store = Arc::new(
        Store::open_log_with_history(log.clone(), retention_limits())
            .await
            .unwrap(),
    );
    let old = store.read_view().await.unwrap();
    let writer = store.clone();
    let key = RequestKey::new("retained", "unknown");
    let sent = key.clone();
    let task = tokio::spawn(async move { writer.register_node_session(sent, lease("new")).await });
    log.started.notified().await;
    log.release.notify_one();
    assert!(task.await.unwrap().is_err());
    assert!(store.health().await.is_err());
    assert!(
        store
            .read(MetaRead::RequestOutcome(key.clone()))
            .await
            .is_err()
    );
    assert_eq!(old.state_for_test().revision, StoreRevision(2));
    assert!(
        store
            .register_node_session(RequestKey::new("retained", "later"), lease("later"))
            .await
            .is_err()
    );
    let recovered = inner.recover().await.unwrap();
    assert_eq!(
        recovered.tip.sequence, 3,
        "no cleanup may follow an unknown append result"
    );
    let mut restored = State::default();
    for record in recovered.records {
        restored = restored.replay_record(&record).unwrap();
    }
    assert!(restored.requests.contains_key(&key));
    assert_eq!(restored.history_stats().requests, 2);
    let reopened = Store::open_log_with_history(inner, retention_limits())
        .await
        .unwrap();
    assert!(matches!(
        reopened
            .register_node_session(key, lease("new"))
            .await
            .unwrap(),
        TxnOutcome::ConditionFailed {
            existing_outcome: Some(_),
            ..
        }
    ));
}
struct CountingLog {
    inner: local_log::LocalLog,
    commits: std::sync::atomic::AtomicUsize,
    bytes: std::sync::atomic::AtomicUsize,
}
impl journal::MetaLog for CountingLog {
    fn persistence(&self) -> BackendPersistence {
        self.inner.persistence()
    }
    fn limits(&self) -> journal::LogLimits {
        self.inner.limits()
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        self.inner.health()
    }
    fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
        self.inner.recover()
    }
    fn append_and_sync(
        &self,
        expected: journal::LogTip,
        records: Vec<log::Record>,
    ) -> MetaFuture<'_, journal::LogTip> {
        Box::pin(async move {
            let mut tip = expected;
            let mut size = 0;
            for record in &records {
                size += journal::frame_size(tip, record)?;
                tip.sequence += 1;
            }
            let result = self.inner.append_and_sync(expected, records).await?;
            self.commits.fetch_add(1, Ordering::SeqCst);
            self.bytes.fetch_add(size, Ordering::SeqCst);
            Ok(result)
        })
    }
}

#[tokio::test]
async fn p3_incremental_write_does_not_reencode_untouched_authority() {
    let dir = tempfile::tempdir().unwrap();
    let log = Arc::new(CountingLog {
        inner: local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap(),
        commits: 0.into(),
        bytes: 0.into(),
    });
    let store = Store::open_log(log.clone()).await.unwrap();
    let request = RequestKey::new("seed", "2000-roots");
    let mut txn = MetaTxn::new(request.clone(), StoreOperation::ActivateRoot);
    txn.conditions
        .push(TxnCondition::RequestAbsent(request.clone()));
    for number in 0..2000 {
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::Root(RootRecord {
                root_id: format!("unchanged-root-{number:04}"),
                root_epoch: 1,
                home_node_id: "old-home".into(),
                home_session_id: "old-session".into(),
                local_prepare_id: "old-prepare".into(),
                created_at_revision: StoreRevision(1),
                updated_at_revision: StoreRevision(1),
            })));
    }
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::ActivateRoot,
            result: OperationResult::Empty,
        }));
    store.compare_and_commit(txn).await.unwrap();
    log.commits.store(0, Ordering::SeqCst);
    log.bytes.store(0, Ordering::SeqCst);
    let before = std::fs::metadata(dir.path().join("wal-00000000000000000001.log"))
        .unwrap()
        .len();
    let changed = RequestKey::new("changed", "small-update");
    let outcome = store
        .register_node_session(changed.clone(), lease("new-session"))
        .await
        .unwrap();
    assert!(matches!(outcome, TxnOutcome::Committed { .. }));
    let after = std::fs::metadata(dir.path().join("wal-00000000000000000001.log"))
        .unwrap()
        .len();
    assert_eq!(log.commits.load(Ordering::SeqCst), 1);
    assert_eq!(after - before, log.bytes.load(Ordering::SeqCst) as u64);
    assert!(
        after - before <= 16 * 1024,
        "small decided change must not rewrite 2000 roots: {} bytes",
        after - before
    );
    eprintln!(
        "P3_INCREMENTAL roots=2000 commits=1 appended_bytes={}",
        after - before
    );
    let tip = log.recover().await.unwrap().tip;
    assert_eq!(tip.sequence, 2);
    drop(store);
    tokio::time::timeout(Duration::from_secs(5), async {
        while Arc::strong_count(&log) > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(log);
    let reopened = Store::open_log(Arc::new(
        local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap(),
    ))
    .await
    .unwrap();
    for number in 0..2000 {
        assert!(
            reopened
                .read(MetaRead::Root {
                    root_id: format!("unchanged-root-{number:04}")
                })
                .await
                .unwrap()
                .entity
                .is_some()
        );
    }
    for key in [request, changed.clone()] {
        assert!(
            reopened
                .read(MetaRead::RequestOutcome(key))
                .await
                .unwrap()
                .request_outcome
                .is_some()
        );
    }
    assert!(matches!(
        reopened
            .register_node_session(changed, lease("new-session"))
            .await
            .unwrap(),
        TxnOutcome::ConditionFailed {
            existing_outcome: Some(_),
            ..
        }
    ));
    assert_eq!(
        reopened
            .read(MetaRead::CurrentNodeSession {
                node_id: "node-a".into()
            })
            .await
            .unwrap()
            .revision,
        StoreRevision(2)
    );
}

struct GatedLog {
    inner: Arc<journal::MemoryLog>,
    started: Notify,
    release: Notify,
    fail: bool,
    persist_before_error: bool,
}
impl journal::MetaLog for GatedLog {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Volatile
    }
    fn limits(&self) -> journal::LogLimits {
        self.inner.limits()
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        self.inner.health()
    }
    fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
        self.inner.recover()
    }
    fn append_and_sync(
        &self,
        expected: journal::LogTip,
        records: Vec<log::Record>,
    ) -> MetaFuture<'_, journal::LogTip> {
        Box::pin(async move {
            self.started.notify_one();
            self.release.notified().await;
            if self.fail {
                if self.persist_before_error {
                    self.inner.append_and_sync(expected, records).await?;
                }
                Err(unavailable("injected log sync failure"))
            } else {
                self.inner.append_and_sync(expected, records).await
            }
        })
    }
}
#[tokio::test]
async fn p3_log_holds_visibility_and_reply_until_durable_ack() {
    let log = Arc::new(GatedLog {
        inner: Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap()),
        started: Notify::new(),
        release: Notify::new(),
        fail: false,
        persist_before_error: false,
    });
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    let writer = Arc::clone(&store);
    let task = tokio::spawn(async move {
        writer
            .register_node_session(RequestKey::new("a", "gated"), lease("new"))
            .await
    });
    log.started.notified().await;
    assert!(!task.is_finished());
    assert_eq!(
        store.read_view().await.unwrap().state_for_test().revision,
        StoreRevision::ZERO
    );
    log.release.notify_one();
    assert!(task.await.unwrap().is_ok());
    assert_eq!(
        store.read_view().await.unwrap().state_for_test().revision,
        StoreRevision(1)
    );
    assert_eq!(log.recover().await.unwrap().tip.sequence, 1);
}
#[tokio::test]
async fn p3_log_failed_ack_poisons_store_and_does_not_publish() {
    let log = Arc::new(GatedLog {
        inner: Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap()),
        started: Notify::new(),
        release: Notify::new(),
        fail: true,
        persist_before_error: false,
    });
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    let old = store.read_view().await.unwrap();
    let writer = Arc::clone(&store);
    let task = tokio::spawn(async move {
        writer
            .register_node_session(RequestKey::new("a", "failed"), lease("new"))
            .await
    });
    log.started.notified().await;
    log.release.notify_one();
    assert!(task.await.unwrap().is_err());
    assert_eq!(old.state_for_test().revision, StoreRevision::ZERO);
    assert_eq!(store.visible.committed.load().revision, StoreRevision::ZERO);
    assert!(store.read_view().await.is_err());
    assert!(store.health().await.is_err());
    assert!(
        store
            .register_node_session(RequestKey::new("a", "later"), lease("later"))
            .await
            .is_err()
    );
}
#[tokio::test]
async fn p3_log_groups_ready_requests_preserving_all_outcomes_and_events() {
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let store = Store::open_log(log.clone()).await.unwrap();
    let a = RequestKey::new("a", "one");
    let b = RequestKey::new("a", "two");
    let (first, second) = tokio::join!(
        store.register_node_session(a.clone(), lease("one")),
        store.register_node_session(b.clone(), lease("two"))
    );
    assert!(matches!(
        first.unwrap(),
        TxnOutcome::Committed {
            revision: StoreRevision(1),
            ..
        }
    ));
    assert!(matches!(
        second.unwrap(),
        TxnOutcome::Committed {
            revision: StoreRevision(2),
            ..
        }
    ));
    let original = store
        .committed()
        .await
        .unwrap()
        .snapshot_bytes_with_batch(0)
        .unwrap();
    assert_eq!(log.recover().await.unwrap().records.len(), 2);
    assert!(matches!(
        store.register_node_session(a, lease("one")).await.unwrap(),
        TxnOutcome::ConditionFailed { .. }
    ));
    assert_eq!(log.recover().await.unwrap().records.len(), 2);
    let replayed = Store::open_log(log).await.unwrap();
    assert_eq!(
        replayed
            .committed()
            .await
            .unwrap()
            .snapshot_bytes_with_batch(0)
            .unwrap(),
        original
    );
}

#[tokio::test]
async fn p3_log_unknown_ack_stops_old_store_and_reopens_exact_outcome() {
    let log = Arc::new(GatedLog {
        inner: Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap()),
        started: Notify::new(),
        release: Notify::new(),
        fail: true,
        persist_before_error: true,
    });
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    let writer = Arc::clone(&store);
    let request = RequestKey::new("unknown", "once");
    let original = request.clone();
    let task =
        tokio::spawn(async move { writer.register_node_session(original, lease("one")).await });
    log.started.notified().await;
    log.release.notify_one();
    assert!(task.await.unwrap().is_err());
    assert_eq!(store.visible.committed.load().revision, StoreRevision::ZERO);
    assert!(
        store
            .register_node_session(RequestKey::new("later", "must-not-pass"), lease("two"))
            .await
            .is_err()
    );
    assert_eq!(log.inner.recover().await.unwrap().tip.sequence, 1);
    let reopened = Store::open_log(log.inner.clone()).await.unwrap();
    let before = reopened
        .read(MetaRead::RequestOutcome(request.clone()))
        .await
        .unwrap()
        .request_outcome
        .unwrap();
    assert!(
        matches!(reopened.register_node_session(request,lease("one")).await.unwrap(),TxnOutcome::ConditionFailed {existing_outcome:Some(ref old),..} if old==&before)
    );
    assert_eq!(
        reopened
            .committed()
            .await
            .unwrap()
            .node_epochs
            .get("node-a"),
        Some(&1)
    );
    assert_eq!(
        reopened.committed().await.unwrap().revision,
        StoreRevision(1)
    );
    assert_eq!(log.inner.recover().await.unwrap().tip.sequence, 1);
}
#[tokio::test]
async fn p3_all_condition_variants_keep_truth_idempotence_and_wal_admission() {
    let log = Arc::new(journal::MemoryLog::new(journal::LogLimits::default()).unwrap());
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    for (node, session) in [("a", "s"), ("b", "t")] {
        store
            .register_node_session(
                RequestKey::new(node, "register"),
                NodeSessionLease {
                    node_id: node.into(),
                    session_id: session.into(),
                    grpc_addr: "g".into(),
                    data_addr: "d".into(),
                    rest_addr: "r".into(),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(300),
                },
            )
            .await
            .unwrap();
    }
    let seed_request = RequestKey::new("seed", "entities");
    let mut bootstrap = MetaTxn::new(seed_request.clone(), StoreOperation::DfsCreate);
    bootstrap
        .conditions
        .push(TxnCondition::RequestAbsent(seed_request.clone()));
    let mut entities = codec::tests::entity_fixtures();
    entities.retain(|entity| !matches!(entity, MetaEntity::NodeSession(_)));
    for entity in &mut entities {
        if let MetaEntity::RootCommandAck(ack) = entity {
            ack.success = true;
        }
    }
    bootstrap
        .mutations
        .extend(entities.iter().cloned().map(TxnMutation::Put));
    bootstrap
        .mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: seed_request.clone(),
            operation: bootstrap.operation,
            result: OperationResult::Empty,
        }));
    assert!(matches!(
        store.compare_and_commit(bootstrap).await.unwrap(),
        TxnOutcome::Committed {
            revision: StoreRevision(3),
            ..
        }
    ));
    let root = entities
        .iter()
        .find_map(|entity| match entity {
            MetaEntity::Root(root) => Some(root.clone()),
            _ => None,
        })
        .unwrap();
    let inode = entities
        .iter()
        .find_map(|entity| match entity {
            MetaEntity::DfsInode(inode) => Some(inode.clone()),
            _ => None,
        })
        .unwrap();
    let root_key = MetaKey::Root {
        root_id: "r".into(),
    };
    let absent_key = MetaKey::Root {
        root_id: "absent".into(),
    };
    let mut changed_root = root.clone();
    changed_root.root_epoch += 1;
    // Expected truth is fixed by this fixture, not derived from the new
    // predicate. The unchanged P2 predicate is independently hash-compared.
    let cases = vec![
        (true, TxnCondition::Missing(absent_key.clone())),
        (false, TxnCondition::Missing(root_key.clone())),
        (
            true,
            TxnCondition::RevisionEquals {
                key: root_key.clone(),
                revision: StoreRevision(3),
            },
        ),
        (
            false,
            TxnCondition::RevisionEquals {
                key: root_key.clone(),
                revision: StoreRevision(4),
            },
        ),
        (
            false,
            TxnCondition::RevisionEquals {
                key: absent_key,
                revision: StoreRevision(3),
            },
        ),
        (true, TxnCondition::EntityEquals(MetaEntity::Root(root))),
        (
            false,
            TxnCondition::EntityEquals(MetaEntity::Root(changed_root)),
        ),
        (
            true,
            TxnCondition::NodeSessionCurrent {
                node_id: "a".into(),
                session_id: "s".into(),
            },
        ),
        (
            false,
            TxnCondition::NodeSessionCurrent {
                node_id: "a".into(),
                session_id: "stale".into(),
            },
        ),
        (
            true,
            TxnCondition::RootEpochEquals {
                root_id: "r".into(),
                root_epoch: 1,
            },
        ),
        (
            false,
            TxnCondition::RootEpochEquals {
                root_id: "r".into(),
                root_epoch: 2,
            },
        ),
        (
            true,
            TxnCondition::RootCommandAcked {
                command_id: "c".into(),
                node_id: "a".into(),
                session_id: "s".into(),
            },
        ),
        (
            false,
            TxnCondition::RootCommandAcked {
                command_id: "c".into(),
                node_id: "a".into(),
                session_id: "stale".into(),
            },
        ),
        (
            true,
            TxnCondition::RootCommandMatches {
                command_id: "c".into(),
                home_node_id: "a".into(),
                home_session_id: "s".into(),
                root_id: "r".into(),
                root_epoch: 1,
                old_access_generation: 2,
                command_type: RootCommandType::InvalidateCache,
            },
        ),
        (
            false,
            TxnCondition::RootCommandMatches {
                command_id: "c".into(),
                home_node_id: "a".into(),
                home_session_id: "s".into(),
                root_id: "r".into(),
                root_epoch: 1,
                old_access_generation: 3,
                command_type: RootCommandType::InvalidateCache,
            },
        ),
        (
            true,
            TxnCondition::RootCommandAckedExact {
                command_id: "c".into(),
                node_id: "a".into(),
                session_id: "s".into(),
                root_id: "r".into(),
                root_epoch: 1,
                access_generation: 2,
                command_type: RootCommandType::InvalidateCache,
            },
        ),
        (
            false,
            TxnCondition::RootCommandAckedExact {
                command_id: "c".into(),
                node_id: "a".into(),
                session_id: "s".into(),
                root_id: "r".into(),
                root_epoch: 2,
                access_generation: 2,
                command_type: RootCommandType::InvalidateCache,
            },
        ),
        (
            false,
            TxnCondition::RootCommandAckedExact {
                command_id: "c".into(),
                node_id: "a".into(),
                session_id: "s".into(),
                root_id: "r".into(),
                root_epoch: 1,
                access_generation: 2,
                command_type: RootCommandType::RevokeAccess,
            },
        ),
        (
            true,
            TxnCondition::RequestAbsent(RequestKey::new("never", "used")),
        ),
        (false, TxnCondition::RequestAbsent(seed_request)),
        (
            true,
            TxnCondition::DfsDirectoryEmpty {
                namespace_id: NamespaceId("n".into()),
                parent_inode_id: InodeId("empty".into()),
            },
        ),
        (
            false,
            TxnCondition::DfsDirectoryEmpty {
                namespace_id: NamespaceId("n".into()),
                parent_inode_id: InodeId("p".into()),
            },
        ),
        (
            true,
            TxnCondition::DfsNotDescendant {
                namespace_id: NamespaceId("other".into()),
                ancestor_inode_id: InodeId("p".into()),
                child_inode_id: InodeId("i".into()),
            },
        ),
        (
            false,
            TxnCondition::DfsNotDescendant {
                namespace_id: NamespaceId("n".into()),
                ancestor_inode_id: InodeId("p".into()),
                child_inode_id: InodeId("i".into()),
            },
        ),
        (
            false,
            TxnCondition::DfsNotDescendant {
                namespace_id: NamespaceId("n".into()),
                ancestor_inode_id: InodeId("i".into()),
                child_inode_id: InodeId("i".into()),
            },
        ),
    ];
    assert_eq!(cases.len(), 25);
    let meta = crate::meta::Meta::with_store(
        "condition-test".into(),
        crate::runtime::Observability::new().unwrap(),
        store.clone(),
    );
    for (index, (expected, condition)) in cases.into_iter().enumerate() {
        let before = store.committed().await.unwrap();
        assert_eq!(
            condition_matches(&before, &condition),
            expected,
            "{condition:?}"
        );
        let before_bytes = before.snapshot_bytes_with_batch(0).unwrap();
        let tip = log.recover().await.unwrap().tip;
        let request = RequestKey::new("conditions", format!("case-{index}"));
        let original = RequestOutcome {
            request: request.clone(),
            operation: StoreOperation::DfsCreate,
            result: OperationResult::DfsInode(inode.clone()),
        };
        let mut txn = MetaTxn::new(request.clone(), original.operation);
        txn.conditions
            .extend([TxnCondition::RequestAbsent(request.clone()), condition]);
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(original.clone()));
        let result = store.compare_and_commit(txn).await.unwrap();
        if !expected {
            assert!(matches!(
                result,
                TxnOutcome::ConditionFailed {
                    existing_outcome: None,
                    ..
                }
            ));
            assert_eq!(
                store
                    .committed()
                    .await
                    .unwrap()
                    .snapshot_bytes_with_batch(0)
                    .unwrap(),
                before_bytes
            );
            assert_eq!(log.recover().await.unwrap().tip, tip);
            assert!(
                store
                    .read(MetaRead::RequestOutcome(request))
                    .await
                    .unwrap()
                    .request_outcome
                    .is_none()
            );
            continue;
        }
        assert_eq!(
            result,
            TxnOutcome::Committed {
                revision: StoreRevision(before.revision.0 + 1),
                outcome: original.clone()
            }
        );
        let committed = store
            .committed()
            .await
            .unwrap()
            .snapshot_bytes_with_batch(0)
            .unwrap();
        let committed_tip = log.recover().await.unwrap().tip;
        assert_eq!(committed_tip.sequence, tip.sequence + 1);
        let mut retry = MetaTxn::new(request.clone(), original.operation);
        retry
            .conditions
            .push(TxnCondition::RequestAbsent(request.clone()));
        retry
            .mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                result: OperationResult::Empty,
                ..original.clone()
            }));
        assert!(
            matches!(store.compare_and_commit(retry).await.unwrap(), TxnOutcome::ConditionFailed { existing_outcome: Some(saved), .. } if saved == original)
        );
        let operation = StoreOperation::DfsUnlink;
        let mut wrong = MetaTxn::new(request.clone(), operation);
        wrong
            .conditions
            .push(TxnCondition::RequestAbsent(request.clone()));
        wrong
            .mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: request.clone(),
                operation,
                result: OperationResult::Empty,
            }));
        // Store preserves P2's typed rejection and original outcome;
        // domain callers must not treat a different operation as replay.
        assert!(matches!(store.compare_and_commit(wrong).await.unwrap(),
            TxnOutcome::ConditionFailed { existing_outcome: Some(saved), .. } if saved == original));
        let error = meta
            .register_node(
                request,
                NodeSessionLease {
                    node_id: "must-not-register".into(),
                    session_id: "must-not-appear".into(),
                    grpc_addr: "g".into(),
                    data_addr: "d".into(),
                    rest_addr: "r".into(),
                    ownerfs_enabled: true,
                    storage_devices: Vec::new(),
                    lease_ttl: Duration::from_secs(300),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code(), afs_error::META_CATALOG_INVALID_REQUEST);
        assert_eq!(log.recover().await.unwrap().tip, committed_tip);
        assert_eq!(
            store
                .committed()
                .await
                .unwrap()
                .snapshot_bytes_with_batch(0)
                .unwrap(),
            committed
        );
        store.health().await.unwrap();
    }
}

#[tokio::test]
async fn p3_root_deactivation_accepts_accumulated_grants_below_byte_budget() {
    use crate::meta::owner_roots::{
        DeactivateRootInput, OwnerRootAuthority, StoreOwnerRootAuthority,
    };

    const OLD_GRANTS: usize = 65_536;
    const SEED_BATCH: usize = 4096;
    let log = Arc::new(journal::MemoryLog::new(Default::default()).unwrap());
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    store
        .register_node_session(
            RequestKey::new("a", "register"),
            NodeSessionLease {
                node_id: "a".into(),
                session_id: "s".into(),
                grpc_addr: "g".into(),
                data_addr: "d".into(),
                rest_addr: "r".into(),
                ownerfs_enabled: true,
                storage_devices: Vec::new(),
                lease_ttl: Duration::from_secs(300),
            },
        )
        .await
        .unwrap();
    let mut template = grant();
    template.root_id = "r".into();
    template.root_epoch = 1;
    template.home_node_id = "a".into();
    template.home_session_id = "s".into();
    template.holder_node_id = "b".into();
    let mut home = template.clone();
    home.holder_node_id = "a".into();
    home.holder_session_id = "s".into();
    for batch in 0..OLD_GRANTS / SEED_BATCH {
        let request = RequestKey::new("fixture", format!("grant-batch-{batch}"));
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::AcquireRoot);
        txn.conditions
            .push(TxnCondition::RequestAbsent(request.clone()));
        if batch == 0 {
            txn.mutations.extend([
                TxnMutation::Put(MetaEntity::Root(RootRecord {
                    root_id: "r".into(),
                    root_epoch: 1,
                    home_node_id: "a".into(),
                    home_session_id: "s".into(),
                    local_prepare_id: "p".into(),
                    created_at_revision: StoreRevision(2),
                    updated_at_revision: StoreRevision(2),
                })),
                TxnMutation::Put(MetaEntity::RootGrant(home.clone())),
            ]);
        }
        // Past holder sessions can legitimately leave distinct durable
        // grants. Deactivation fences them atomically; physical deletion is deferred.
        for index in batch * SEED_BATCH..(batch + 1) * SEED_BATCH {
            let mut value = template.clone();
            value.holder_session_id = format!("old-{index:05}");
            txn.mutations
                .push(TxnMutation::Put(MetaEntity::RootGrant(value)));
        }
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation: txn.operation,
                result: OperationResult::Empty,
            }));
        assert!(matches!(
            store.compare_and_commit(txn).await.unwrap(),
            TxnOutcome::Committed { .. }
        ));
    }
    let before = store.committed().await.unwrap();
    let grants = before
        .read(MetaRead::RootGrants {
            root_id: "r".into(),
        })
        .unwrap();
    assert_eq!(grants.entities.len(), OLD_GRANTS + 1);
    let keys_bytes: usize = grants
        .entities
        .iter()
        .map(|entity| codec::encoded_size(&entity_key(entity)).unwrap())
        .sum();
    // This is a lower-level byte account, not a claimed framed record size.
    assert!(keys_bytes < log.limits().record_max_bytes / 4);
    let tip = log.recover().await.unwrap().tip;
    let authority = StoreOwnerRootAuthority::new(store.clone());
    let result = authority
        .deactivate_root(DeactivateRootInput {
            request_id: "deactivate".into(),
            root_id: "r".into(),
            root_epoch: 1,
            home_node_id: "a".into(),
            home_session_id: "s".into(),
            reason: "finished".into(),
            authenticated_home_node_id: Some("a".into()),
        })
        .await;
    if let Err(error) = &result {
        let after = store.committed().await.unwrap();
        assert_eq!(after.revision, before.revision);
        assert!(Arc::ptr_eq(&after, &before));
        assert_eq!(log.recover().await.unwrap().tip, tip);
        store.health().await.unwrap();
        eprintln!(
            "P3_GRANT_BOUND grants={} delete_key_bytes={keys_bytes} record_limit={} rejected_without_mutation=true error={error}",
            grants.entities.len(),
            log.limits().record_max_bytes
        );
    }
    result
        .expect("supported atomic root deactivation must not acquire an undocumented count limit");
    let after = store.committed().await.unwrap();
    assert!(after.revision.0 > before.revision.0);
    assert!(
        after
            .read(MetaRead::Root {
                root_id: "r".into()
            })
            .unwrap()
            .entity
            .is_none()
    );
    assert!(
        !after
            .read(MetaRead::RootGrants {
                root_id: "r".into()
            })
            .unwrap()
            .entities
            .is_empty()
    );
    let recovered = log.recover().await.unwrap();
    let record = recovered
        .records
        .iter()
        .find(|record| {
            matches!(record, log::Record::Decision { .. })
                && record.revision_before() == before.revision
                && record.revision_after() == before.revision.next()
        })
        .unwrap();
    let log::Record::Decision { decision, .. } = record else {
        unreachable!()
    };
    assert_eq!(
        decision.facts().unwrap().operation,
        StoreOperation::DeactivateRoot
    );
    let framed = journal::frame_size(tip, record).unwrap();
    assert!(
        framed < 4096,
        "logical deactivation must not scale with stored grants"
    );
    drop(authority);
    drop(store);
    tokio::time::timeout(Duration::from_secs(2), async {
        while Arc::strong_count(&log) > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let reopened = Store::open_log(log.clone()).await.unwrap();
    let reopened = reopened.committed().await.unwrap();
    assert!(reopened.revision >= after.revision);
    assert!(
        !reopened
            .read(MetaRead::RootGrants {
                root_id: "r".into()
            })
            .unwrap()
            .entities
            .is_empty()
    );
    let request = RequestKey::new("a/s", "deactivate");
    assert_eq!(
        reopened.requests.get(&request),
        after.requests.get(&request)
    );
    eprintln!(
        "P3_GRANT_BOUND_GREEN grants={} delete_key_bytes={keys_bytes} framed_bytes={framed} record_limit={} one_business_revision=true replayed=true",
        grants.entities.len(),
        log.limits().record_max_bytes
    );
}

#[tokio::test]
async fn p3_a27_typed_byte_limits_reject_atomically_and_reopen_controls() {
    use crate::dfs::{
        ChunkEncoding, ChunkId, ContentDigest, CopyId, CopyLocation, CopyRole, CopyState,
        DigestAlgorithm, Extent,
    };
    use std::collections::BTreeMap;

    const REJECT_BYTES: usize = 4096;
    const CONTROL_BYTES: usize = 65536;
    fn limits(record_max_bytes: usize) -> journal::LogLimits {
        journal::LogLimits {
            record_max_bytes,
            batch_max_bytes: 131072,
            wal_max_bytes: 1048576,
            ..Default::default()
        }
    }
    fn files(path: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (
                    entry.file_name().into_string().unwrap(),
                    std::fs::read(entry.path()).unwrap(),
                )
            })
            .collect()
    }
    let digest = ContentDigest {
        algorithm: DigestAlgorithm::Blake3,
        bytes: [3; 32],
    };
    let chunk = MetaEntity::DfsChunk(ChunkObject {
        id: ChunkId::new("chunk"),
        length: 4096,
        content_digest: digest.clone(),
        encoding: ChunkEncoding::Raw,
    });
    let layout = vec![
        chunk.clone(),
        MetaEntity::DfsLayoutRoot(LayoutRoot {
            id: LayoutRootId::new("layout"),
            file_length: 512 * 4096,
            inline_extents: (0..512)
                .map(|number| Extent {
                    file_offset: number * 4096,
                    length: 4096,
                    chunk_id: ChunkId::new("chunk"),
                    chunk_offset: 0,
                })
                .collect(),
        }),
        MetaEntity::DfsFileVersion(FileVersion {
            id: FileVersionId::new("version"),
            inode_id: InodeId::new("inode"),
            parent_version: None,
            length: 512 * 4096,
            layout_root: LayoutRootId::new("layout"),
            created_at_unix_ms: 1,
        }),
    ];
    let grants = (0..64)
        .map(|number| {
            let mut value = grant();
            value.holder_session_id = format!("holder-{number}");
            value.issued_at_revision = StoreRevision(1);
            MetaEntity::RootGrant(value)
        })
        .collect::<Vec<_>>();
    let mut copies = vec![chunk];
    copies.extend((0..128).map(|number| {
        MetaEntity::DfsCopy(CopyRecord {
            id: CopyId::new(format!("copy-{number}")),
            chunk_id: ChunkId::new("chunk"),
            role: CopyRole::DurableReplica,
            location: CopyLocation::Node {
                node_id: format!("node-{number}"),
                node_epoch: 1,
                device_id: "device".into(),
                device_epoch: 1,
                catalog_revision: 1,
            },
            state: CopyState::Ready,
            persisted_bytes: 4096,
            verified_digest: digest.clone(),
        })
    }));
    // Typed Store fixtures test persistence, not placement/receipt policy.
    for (name, entities) in [("layout", layout), ("grants", grants), ("copies", copies)] {
        let request = RequestKey::new("a27", name);
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::DfsCommitFileVersion);
        txn.conditions
            .push(TxnCondition::RequestAbsent(request.clone()));
        txn.mutations
            .extend(entities.into_iter().map(TxnMutation::Put));
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request: request.clone(),
                operation: txn.operation,
                result: OperationResult::Empty,
            }));
        let (_, _, record) = State::default()
            .prepare_txn_record(txn.clone(), 1, CONTROL_BYTES)
            .unwrap();
        let framed =
            journal::frame_size(journal::LogTip::EMPTY, &log::Record::from(record.unwrap()))
                .unwrap();
        assert!(
            framed > REJECT_BYTES && framed < CONTROL_BYTES,
            "{name}: {framed}"
        );
        let rejected_dir = tempfile::tempdir().unwrap();
        let rejected_log =
            Arc::new(local_log::LocalLog::open(rejected_dir.path(), limits(REJECT_BYTES)).unwrap());
        let rejected = Store::open_log(rejected_log.clone()).await.unwrap();
        let before = rejected.committed().await.unwrap();
        let before_files = files(rejected_dir.path());
        for _ in 0..2 {
            let error = rejected.compare_and_commit(txn.clone()).await.unwrap_err();
            assert!(error.to_string().contains("byte"), "{name}: {error}");
            assert!(Arc::ptr_eq(&before, &rejected.committed().await.unwrap()));
            assert_eq!(
                rejected_log.recover().await.unwrap().tip,
                journal::LogTip::EMPTY
            );
            assert_eq!(files(rejected_dir.path()), before_files);
            assert!(
                !rejected
                    .committed()
                    .await
                    .unwrap()
                    .requests
                    .contains_key(&request)
            );
            rejected.health().await.unwrap();
        }
        rejected
            .register_node_session(RequestKey::new("a27", "after-reject"), lease("small"))
            .await
            .unwrap();
        assert_eq!(
            rejected.committed().await.unwrap().revision,
            StoreRevision(1)
        );
        drop(rejected);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&rejected_log) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(rejected_log);

        let control_dir = tempfile::tempdir().unwrap();
        let control_log =
            Arc::new(local_log::LocalLog::open(control_dir.path(), limits(CONTROL_BYTES)).unwrap());
        let control = Store::open_log(control_log.clone()).await.unwrap();
        assert!(matches!(
            control.compare_and_commit(txn.clone()).await.unwrap(),
            TxnOutcome::Committed {
                revision: StoreRevision(1),
                ..
            }
        ));
        let original = control
            .committed()
            .await
            .unwrap()
            .snapshot_bytes_with_batch(0)
            .unwrap();
        let tip = control_log.recover().await.unwrap().tip;
        assert!(matches!(
            control.compare_and_commit(txn).await.unwrap(),
            TxnOutcome::ConditionFailed {
                existing_outcome: Some(_),
                ..
            }
        ));
        assert_eq!(control_log.recover().await.unwrap().tip, tip);
        drop(control);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&control_log) > 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(control_log);
        let reopened_log =
            Arc::new(local_log::LocalLog::open(control_dir.path(), limits(CONTROL_BYTES)).unwrap());
        let reopened = Store::open_log(reopened_log).await.unwrap();
        assert_eq!(
            reopened
                .committed()
                .await
                .unwrap()
                .snapshot_bytes_with_batch(0)
                .unwrap(),
            original
        );
        eprintln!(
            "P3_A27_TYPED name={name} framed_bytes={framed} reject_limit={REJECT_BYTES} control_limit={CONTROL_BYTES} no_append_or_publish=true actual_LocalLog_reopen=true"
        );
    }
}

#[tokio::test]
async fn p3_a27_ready_group_keeps_failed_requests_out_of_durable_authority() {
    let directory = tempfile::tempdir().unwrap();
    let backend = Arc::new(CountingLog {
        inner: local_log::LocalLog::open(
            directory.path(),
            journal::LogLimits {
                record_max_bytes: 4096,
                batch_max_bytes: 131072,
                wal_max_bytes: 1048576,
                ..Default::default()
            },
        )
        .unwrap(),
        commits: 0.into(),
        bytes: 0.into(),
    });
    let store = Store::open_log(backend.clone()).await.unwrap();
    let root = MetaEntity::Root(RootRecord {
        root_id: "rejected-root".into(),
        root_epoch: 1,
        home_node_id: "a".into(),
        home_session_id: "s".into(),
        local_prepare_id: "p".into(),
        created_at_revision: StoreRevision(1),
        updated_at_revision: StoreRevision(1),
    });
    let make_txn = |name: &str| {
        let request = RequestKey::new("a27-group", name);
        let mut txn = MetaTxn::new(request.clone(), StoreOperation::ActivateRoot);
        txn.conditions
            .push(TxnCondition::RequestAbsent(request.clone()));
        txn.mutations.push(TxnMutation::Put(root.clone()));
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation: txn.operation,
                result: OperationResult::Empty,
            }));
        txn
    };
    let mut invalid_apply = make_txn("apply-invalid");
    invalid_apply.mutations.pop(); // apply_txn must reject a missing result.
    let mut invalid_encoding = make_txn("encode-invalid");
    let mut nested = OperationResult::Empty;
    for _ in 0..129 {
        nested = OperationResult::DfsNamespace {
            request_digest: [1; 32],
            result: Box::new(nested),
        };
    }
    if let Some(TxnMutation::RecordRequestOutcome(outcome)) = invalid_encoding.mutations.last_mut()
    {
        outcome.result = nested;
    } else {
        panic!("fixture must end with its outcome");
    }
    let mut false_condition = make_txn("condition-false");
    false_condition
        .conditions
        .push(TxnCondition::Missing(MetaKey::CurrentNodeSession {
            node_id: "node-a".into(),
        }));
    let mut large = lease("too-large");
    large.grpc_addr = "x".repeat(8192);
    let mut last = lease("last");
    last.node_id = "node-b".into();
    // Current-thread runtime plus biased polling queues the six operations
    // before the writer runs. Assert the actual physical batch, not timing.
    let (first, apply, encoding, oversize, condition, last) = tokio::join!(biased;
        store.register_node_session(RequestKey::new("a27-group", "first"), lease("first")),
        store.compare_and_commit(invalid_apply),
        store.compare_and_commit(invalid_encoding),
        store.register_node_session(RequestKey::new("a27-group", "too-large"), large),
        store.compare_and_commit(false_condition),
        store.register_node_session(RequestKey::new("a27-group", "last"), last),
    );
    assert!(matches!(
        first.unwrap(),
        TxnOutcome::Committed {
            revision: StoreRevision(1),
            ..
        }
    ));
    assert!(
        apply
            .unwrap_err()
            .to_string()
            .contains("matching request outcome")
    );
    assert!(encoding.unwrap_err().to_string().contains("canonical"));
    assert!(oversize.unwrap_err().to_string().contains("byte"));
    assert!(matches!(
        condition.unwrap(),
        TxnOutcome::ConditionFailed {
            revision: StoreRevision(1),
            existing_outcome: None
        }
    ));
    assert!(matches!(
        last.unwrap(),
        TxnOutcome::Committed {
            revision: StoreRevision(2),
            ..
        }
    ));
    store.health().await.unwrap();
    let state = store.committed().await.unwrap();
    assert_eq!(state.revision, StoreRevision(2));
    assert!(!state.entities.contains_key(&MetaKey::Root {
        root_id: "rejected-root".into()
    }));
    for name in [
        "apply-invalid",
        "encode-invalid",
        "too-large",
        "condition-false",
    ] {
        assert!(
            !state
                .requests
                .contains_key(&RequestKey::new("a27-group", name))
        );
    }
    assert_eq!(backend.commits.load(Ordering::SeqCst), 1);
    let recovered = backend.recover().await.unwrap();
    assert_eq!(recovered.records.len(), 2);
    assert_eq!(recovered.tip.sequence, 2);
    let original = state.snapshot_bytes_with_batch(0).unwrap();
    drop(state);
    drop(store);
    tokio::time::timeout(Duration::from_secs(5), async {
        while Arc::strong_count(&backend) > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(backend);
    let reopened = Store::open_log(Arc::new(
        local_log::LocalLog::open(
            directory.path(),
            journal::LogLimits {
                record_max_bytes: 4096,
                batch_max_bytes: 131072,
                wal_max_bytes: 1048576,
                ..Default::default()
            },
        )
        .unwrap(),
    ))
    .await
    .unwrap();
    assert_eq!(
        reopened
            .committed()
            .await
            .unwrap()
            .snapshot_bytes_with_batch(0)
            .unwrap(),
        original
    );
    eprintln!(
        "P3_A27_GROUP ready=6 decided=2 physical_appends=1 apply_rejected=1 canonical_rejected=1 oversized=1 false_condition=1 no_failed_authority=true actual_LocalLog_reopen=true"
    );
}

#[tokio::test]
async fn p3_log_failed_condition_and_oversize_do_not_append_or_publish() {
    let limits = journal::LogLimits {
        record_max_bytes: 1024,
        batch_max_bytes: 1024,
        wal_max_bytes: 32 * 1024,
        ..Default::default()
    };
    let log = Arc::new(journal::MemoryLog::new(limits).unwrap());
    let store = Store::open_log(log.clone()).await.unwrap();
    store
        .register_node_session(RequestKey::new("seed", "one"), lease("one"))
        .await
        .unwrap();
    let tip = log.recover().await.unwrap().tip;
    let request = RequestKey::new("condition", "false");
    let mut txn = MetaTxn::new(request.clone(), StoreOperation::ActivateRoot);
    txn.conditions
        .push(TxnCondition::RequestAbsent(request.clone()));
    txn.conditions
        .push(TxnCondition::Missing(MetaKey::CurrentNodeSession {
            node_id: "node-a".into(),
        }));
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request,
            operation: StoreOperation::ActivateRoot,
            result: OperationResult::Empty,
        }));
    assert!(matches!(
        store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::ConditionFailed {
            existing_outcome: None,
            ..
        }
    ));
    let mut large = lease("oversize");
    large.grpc_addr = "x".repeat(8192);
    assert!(
        store
            .register_node_session(RequestKey::new("large", "rejected"), large)
            .await
            .is_err()
    );
    assert_eq!(log.recover().await.unwrap().tip, tip);
    assert_eq!(store.committed().await.unwrap().revision, StoreRevision(1));
    store.health().await.unwrap();
    assert!(matches!(
        store
            .register_node_session(RequestKey::new("small", "after-rejection"), lease("two"))
            .await
            .unwrap(),
        TxnOutcome::Committed {
            revision: StoreRevision(2),
            ..
        }
    ));
}
#[tokio::test]
async fn p3_log_byte_limit_splits_ready_group_without_losing_order() {
    let dir = tempfile::tempdir().unwrap();
    let limits = journal::LogLimits {
        record_max_bytes: 1024,
        batch_max_bytes: 1024,
        wal_max_bytes: 32 * 1024,
        ..Default::default()
    };
    let log = Arc::new(CountingLog {
        inner: local_log::LocalLog::open(dir.path(), limits).unwrap(),
        commits: 0.into(),
        bytes: 0.into(),
    });
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    let mut tasks = tokio::task::JoinSet::new();
    for number in 0..8 {
        let store = Arc::clone(&store);
        tasks.spawn(async move {
            store
                .register_node_session(
                    RequestKey::new("split", format!("{number}")),
                    lease(&format!("session-{number}")),
                )
                .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        assert!(matches!(
            result.unwrap().unwrap(),
            TxnOutcome::Committed { .. }
        ));
    }
    assert!(log.commits.load(Ordering::SeqCst) > 1);
    let recovered = log.recover().await.unwrap();
    assert_eq!(recovered.records.len(), 8);
    assert_eq!(recovered.tip.sequence, 8);
    let replayed = Store::open_log(log).await.unwrap();
    assert_eq!(
        store
            .committed()
            .await
            .unwrap()
            .snapshot_bytes_with_batch(0)
            .unwrap(),
        replayed
            .committed()
            .await
            .unwrap()
            .snapshot_bytes_with_batch(0)
            .unwrap()
    );
}

#[tokio::test]
async fn batch_observability_preserves_group_commit_replay() {
    let commits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let last_record_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let backend = Arc::new(CountingBackend {
        inner: journal::MemoryLog::new(Default::default()).unwrap(),
        commits: Arc::clone(&commits),
        last_record_bytes: Arc::clone(&last_record_bytes),
    });
    let store = Store::open_log(backend.clone()).await.unwrap();
    let first_request = RequestKey::new("node-a", "batch-observe-1");
    let second_request = RequestKey::new("node-a", "batch-observe-2");

    let (first, second) = tokio::join!(
        store.register_node_session(first_request.clone(), lease("session-1")),
        store.register_node_session(second_request.clone(), lease("session-2")),
    );
    assert!(matches!(first.unwrap(), TxnOutcome::Committed { .. }));
    assert!(matches!(second.unwrap(), TxnOutcome::Committed { .. }));
    assert_eq!(
        commits.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "concurrent mutations should stay one physical batch after instrumentation"
    );
    assert!(
        last_record_bytes.load(std::sync::atomic::Ordering::SeqCst) > 0,
        "instrumented canonical batch must still produce framed bytes"
    );

    drop(store);
    let reopened = Store::open_log(backend).await.unwrap();
    for request in [first_request, second_request] {
        let snapshot = reopened
            .read(MetaRead::RequestOutcome(request))
            .await
            .unwrap();
        assert!(snapshot.request_outcome.is_some());
    }
}

// Installs only a test scheduler gate; it changes no authority fact.
async fn install_read_pause(store: &Store, pause: Arc<state::ReadPause>) {
    let mut state = store.committed().await.unwrap().as_ref().clone();
    state.read_pause = Some(pause);
    store.visible.committed.store(Arc::new(state));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p3_two_readers_and_writer_progress_while_one_reader_is_paused() {
    let store = Arc::new(
        Store::open_log(Arc::new(
            journal::MemoryLog::new(Default::default()).unwrap(),
        ))
        .await
        .unwrap(),
    );
    store
        .register_node_session(RequestKey::new("a", "initial"), lease("session-1"))
        .await
        .unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let pause = Arc::new(state::ReadPause::new(entered_tx));
    install_read_pause(&store, Arc::clone(&pause)).await;
    let runtime = tokio::runtime::Handle::current();
    let first_store = Arc::clone(&store);
    let first_runtime = runtime.clone();
    let first = std::thread::spawn(move || {
        let id = std::thread::current().id();
        (
            id,
            first_runtime
                .block_on(first_store.read(MetaRead::CurrentNodeSessions))
                .unwrap(),
        )
    });
    let entered = entered_rx.recv_timeout(Duration::from_secs(3));
    if entered.is_err() {
        pause.release();
        first.join().unwrap();
        panic!("reader did not reach the scheduler gate: {entered:?}");
    }
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let second_store = Arc::clone(&store);
    let second_runtime = runtime.clone();
    let second_tx = done_tx.clone();
    let second = std::thread::spawn(move || {
        let snapshot = second_runtime
            .block_on(second_store.read(MetaRead::CurrentNodeSessions))
            .unwrap();
        second_tx.send("reader").unwrap();
        (std::thread::current().id(), snapshot)
    });
    let writer_store = Arc::clone(&store);
    let writer = std::thread::spawn(move || {
        let mut next = lease("session-b");
        next.node_id = "node-b".into();
        let outcome = runtime
            .block_on(writer_store.register_node_session(RequestKey::new("b", "next"), next))
            .unwrap();
        done_tx.send("writer").unwrap();
        (std::thread::current().id(), outcome)
    });
    let mut observed = Vec::new();
    for _ in 0..2 {
        match done_rx.recv_timeout(Duration::from_secs(3)) {
            Ok(done) => observed.push(done),
            Err(_) => break,
        }
    }
    // Always release before joining/asserting, including the old-lock RED.
    pause.release();
    let (first_id, first_snapshot) = first.join().unwrap();
    let (second_id, second_snapshot) = second.join().unwrap();
    let (writer_id, outcome) = writer.join().unwrap();
    assert_ne!(first_id, second_id);
    assert_ne!(first_id, writer_id);
    assert_ne!(second_id, writer_id);
    assert_eq!(
        observed.len(),
        2,
        "a paused read must not serialize another reader or the writer: {observed:?}"
    );
    assert!(matches!(
        outcome,
        TxnOutcome::Committed {
            revision: StoreRevision(2),
            ..
        }
    ));
    assert_eq!(first_snapshot.revision, StoreRevision(1));
    assert_eq!(first_snapshot.entities.len(), 1);
    match second_snapshot.revision {
        StoreRevision(1) => assert_eq!(second_snapshot.entities.len(), 1),
        StoreRevision(2) => assert_eq!(second_snapshot.entities.len(), 2),
        other => panic!("unexpected/torn revision {other}"),
    }
    let current = store.read(MetaRead::CurrentNodeSessions).await.unwrap();
    assert_eq!(current.revision, StoreRevision(2));
    assert_eq!(current.entities.len(), 2);
}

#[tokio::test]
async fn pinned_read_view_keeps_one_revision_across_later_commits() {
    let store = Store::open_log(Arc::new(
        journal::MemoryLog::new(Default::default()).unwrap(),
    ))
    .await
    .unwrap();
    let view = store.read_view().await.unwrap();
    let key = MetaRead::CurrentNodeSession {
        node_id: "node-a".into(),
    };
    let before = view.read(key.clone()).await.unwrap();
    store
        .register_node_session(
            RequestKey::new("node-a", "view-register"),
            lease("session-1"),
        )
        .await
        .unwrap();
    let pinned = view.read(key.clone()).await.unwrap();
    let current = store.read(key).await.unwrap();
    assert_eq!(before.revision, pinned.revision);
    assert!(pinned.entity.is_none());
    assert!(current.entity.is_some());
    assert!(current.revision > pinned.revision);
    let absent_key = MetaKey::CurrentNodeSession {
        node_id: "node-a".into(),
    };
    assert!(view.lookup(&absent_key).unwrap().entity.is_none());
    let first = store.read_view().await.unwrap();
    store
        .register_node_session(
            RequestKey::new("node-a", "view-register-2"),
            lease("session-2"),
        )
        .await
        .unwrap();
    let latest = store.read_view().await.unwrap();
    let old_key = MetaKey::NodeSession {
        node_id: "node-a".into(),
        session_id: "session-1".into(),
    };
    let old = first.lookup(&old_key).unwrap();
    assert!(old.entity.is_some());
    assert_eq!(old, latest.lookup(&old_key).unwrap());
}

#[tokio::test]
async fn pending_write_is_invisible_until_backend_ack() {
    let backend = Arc::new(GatedBackend {
        inner: journal::MemoryLog::new(Default::default()).unwrap(),
        started: Notify::new(),
        release: Notify::new(),
        fail: false,
    });
    let store = Arc::new(Store::open_log(backend.clone()).await.unwrap());
    let writer = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            store
                .register_node_session(RequestKey::new("node-a", "r1"), lease("session-1"))
                .await
        })
    };
    backend.started.notified().await;
    assert_eq!(store.visible.metrics.wal_batches.get(), 0);
    assert_eq!(store.visible.metrics.revision.get(), 0);
    assert!(
        store
            .read(MetaRead::CurrentNodeSession {
                node_id: "node-a".into()
            })
            .await
            .unwrap()
            .entity
            .is_none()
    );
    backend.release.notify_one();
    writer.await.unwrap().unwrap();
    assert_eq!(store.visible.metrics.wal_batches.get(), 1);
    assert_eq!(store.visible.metrics.wal_records.get(), 1);
    assert!(store.visible.metrics.wal_framed_bytes.get() > 0);
    assert_eq!(store.visible.metrics.revision.get(), 1);
    assert_eq!(store.visible.metrics.wal_failures.get(), 0);
    assert!(
        store
            .read(MetaRead::CurrentNodeSession {
                node_id: "node-a".into()
            })
            .await
            .unwrap()
            .entity
            .is_some()
    );
}

#[tokio::test]
async fn failed_commit_poison_store_instead_of_exposing_stale_authority() {
    let backend = Arc::new(GatedBackend {
        inner: journal::MemoryLog::new(Default::default()).unwrap(),
        started: Notify::new(),
        release: Notify::new(),
        fail: true,
    });
    let store = Arc::new(Store::open_log(backend.clone()).await.unwrap());
    let writer = {
        let store = Arc::clone(&store);
        tokio::spawn(async move {
            store
                .register_node_session(RequestKey::new("node-a", "r1"), lease("session-1"))
                .await
        })
    };
    backend.started.notified().await;
    assert_eq!(store.visible.metrics.wal_batches.get(), 0);
    assert_eq!(store.visible.metrics.revision.get(), 0);
    backend.release.notify_one();
    assert!(writer.await.unwrap().is_err());
    assert_eq!(store.visible.metrics.wal_failures.get(), 1);
    assert_eq!(store.visible.metrics.wal_batches.get(), 0);
    assert_eq!(store.visible.metrics.revision.get(), 0);
    assert!(
        store
            .read(MetaRead::CurrentNodeSession {
                node_id: "node-a".into()
            })
            .await
            .is_err()
    );
}

struct MaintenanceLog {
    inner: journal::MemoryLog,
    policy: journal::MaintenancePolicy,
    admitted: AtomicBool,
    uncertain_failure: AtomicBool,
    checked: Notify,
    installed: Notify,
    appends: AtomicU64,
    installs: AtomicU64,
}
impl MaintenanceLog {
    fn new() -> Self {
        Self {
            inner: journal::MemoryLog::new(journal::LogLimits::default()).unwrap(),
            policy: journal::MaintenancePolicy {
                wal_records: 1,
                poll: Duration::from_millis(5),
                interval: Duration::from_millis(30),
                checkpoint_timeout: Duration::from_secs(1),
                capacity_wait: Duration::from_secs(1),
                ..Default::default()
            },
            admitted: AtomicBool::new(true),
            uncertain_failure: AtomicBool::new(false),
            checked: Notify::new(),
            installed: Notify::new(),
            appends: 0.into(),
            installs: 0.into(),
        }
    }
}
impl journal::MetaLog for MaintenanceLog {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Volatile
    }
    fn limits(&self) -> journal::LogLimits {
        self.inner.limits()
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        self.inner.health()
    }
    fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
        self.inner.recover()
    }
    fn maintenance_policy(&self) -> Option<journal::MaintenancePolicy> {
        Some(self.policy)
    }
    fn status(&self) -> MetaFuture<'_, journal::LogStatus> {
        self.inner.status()
    }
    fn can_append(&self, _expected: journal::LogTip, _bytes: usize) -> MetaFuture<'_, bool> {
        Box::pin(async move {
            self.checked.notify_one();
            Ok(self.admitted.load(Ordering::Acquire))
        })
    }
    fn append_and_sync(
        &self,
        tip: journal::LogTip,
        records: Vec<log::Record>,
    ) -> MetaFuture<'_, journal::LogTip> {
        Box::pin(async move {
            self.appends.fetch_add(1, Ordering::Relaxed);
            let next = self.inner.append_and_sync(tip, records).await?;
            if self.uncertain_failure.load(Ordering::Acquire) {
                return Err(std::io::Error::from(std::io::ErrorKind::StorageFull).into());
            }
            Ok(next)
        })
    }
    fn install_checkpoint(&self, tip: journal::LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            self.inner.install_checkpoint(tip, bytes).await?;
            self.installs.fetch_add(1, Ordering::Relaxed);
            self.installed.notify_one();
            Ok(())
        })
    }
}
#[tokio::test]
async fn p3_maintenance_background_installs_without_manual_call() {
    let log = Arc::new(MaintenanceLog::new());
    let store = Store::open_log(log.clone()).await.unwrap();
    store
        .register_node_session(RequestKey::new("bg", "one"), lease("one"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), log.installed.notified())
        .await
        .expect("background checkpoint must run without a manual call");
    assert!(log.installs.load(Ordering::Acquire) > 0);
    assert_eq!(
        log.recover()
            .await
            .unwrap()
            .checkpoint
            .unwrap()
            .covered
            .sequence,
        1
    );
}
#[tokio::test]
async fn p3_maintenance_capacity_wait_never_appends_or_exposes_private_state() {
    let log = Arc::new(MaintenanceLog::new());
    log.admitted.store(false, Ordering::Release);
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    let write = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .register_node_session(RequestKey::new("blocked", "one"), lease("one"))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), log.checked.notified())
        .await
        .expect("read-only capacity admission must precede append");
    assert!(!write.is_finished());
    assert_eq!(log.appends.load(Ordering::Acquire), 0);
    assert_eq!(
        store.committed().await.unwrap().revision,
        StoreRevision::ZERO
    );
    log.admitted.store(true, Ordering::Release);
    store.visible.checkpoint_finished.notify_waiters();
    assert!(matches!(
        write.await.unwrap().unwrap(),
        TxnOutcome::Committed {
            revision: StoreRevision(1),
            ..
        }
    ));
    assert_eq!(log.appends.load(Ordering::Acquire), 1);
}
#[tokio::test]
async fn p3_maintenance_capacity_timeout_is_not_an_uncertain_enospc_retry() {
    let mut fixture = MaintenanceLog::new();
    fixture.policy.capacity_wait = Duration::from_millis(25);
    let log = Arc::new(fixture);
    log.admitted.store(false, Ordering::Release);
    let store = Store::open_log(log.clone()).await.unwrap();
    let result = store
        .register_node_session(RequestKey::new("blocked", "no-append"), lease("no-append"))
        .await;
    assert!(
        result.is_err(),
        "known capacity exhaustion must not attempt append"
    );
    assert_eq!(log.appends.load(Ordering::Acquire), 0);
    store.health().await.unwrap();
    assert_eq!(
        store.committed().await.unwrap().revision,
        StoreRevision::ZERO
    );
    log.admitted.store(true, Ordering::Release);
    log.uncertain_failure.store(true, Ordering::Release);
    assert!(
        store
            .register_node_session(RequestKey::new("written", "unknown"), lease("unknown"))
            .await
            .is_err()
    );
    assert_eq!(
        log.appends.load(Ordering::Acquire),
        1,
        "a failed append must never be automatically retried"
    );
    assert!(store.committed().await.is_err());
    assert_eq!(
        log.recover().await.unwrap().tip.sequence,
        1,
        "unknown append may already be present"
    );
    log.uncertain_failure.store(false, Ordering::Release);
    let reopened = Store::open_log(log.clone()).await.unwrap();
    assert!(matches!(
        reopened
            .register_node_session(RequestKey::new("written", "unknown"), lease("unknown"))
            .await
            .unwrap(),
        TxnOutcome::ConditionFailed {
            existing_outcome: Some(_),
            ..
        }
    ));
    assert_eq!(
        log.appends.load(Ordering::Acquire),
        1,
        "exact retained request retry must not append twice"
    );
}

#[tokio::test]
async fn p3_metrics_publish_confirmed_history_and_do_not_pin_authority() {
    let store = Store::open_log_with_history(
        Arc::new(journal::MemoryLog::new(Default::default()).unwrap()),
        history::Limits {
            request_count: 1,
            event_count: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let registry = afs_metrics::Registry::new();
    store.register_metrics(&registry).unwrap();
    assert!(store.register_metrics(&registry).is_err());
    for request in ["first", "second"] {
        store
            .register_node_session(RequestKey::new("metrics", request), lease(request))
            .await
            .unwrap();
    }
    let state = store.committed().await.unwrap();
    let text = afs_metrics::encode_text(&registry).unwrap();
    assert!(text.contains("afs_meta_requests_retained 1\n"), "{text}");
    assert!(
        text.contains("afs_meta_request_evictions_total 1\n"),
        "{text}"
    );
    assert!(text.contains("afs_meta_log_sequence 2\n"), "{text}");
    assert!(
        state.events_removed > 0,
        "fixture must actually evict watch events"
    );
    assert_eq!(
        store.visible.metrics.event_evictions.get(),
        state.events_removed
    );
    assert_eq!(
        store.visible.metrics.compacted_revision.get(),
        i64::try_from(state.compacted_to.0).unwrap()
    );
    assert_eq!(
        store.visible.metrics.request_bytes.get(),
        i64::try_from(state.history_stats().request_bytes).unwrap()
    );
    let other = Store::open_log(Arc::new(
        journal::MemoryLog::new(Default::default()).unwrap(),
    ))
    .await
    .unwrap();
    let other_registry = afs_metrics::Registry::new();
    other.register_metrics(&other_registry).unwrap();
    assert!(
        afs_metrics::encode_text(&other_registry)
            .unwrap()
            .contains("afs_meta_log_sequence 0\n")
    );
    let weak = Arc::downgrade(&state);
    drop(state);
    drop(store);
    tokio::time::timeout(Duration::from_secs(2), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !registry.gather().is_empty(),
        "collector handles survive without pinning State"
    );
}

struct GatedCheckpointLog {
    inner: journal::MemoryLog,
    entered: Notify,
    release: Notify,
    policy: Option<journal::MaintenancePolicy>,
    checkpoint_limits: journal::CheckpointLimits,
    installs: AtomicU64,
}
impl journal::MetaLog for GatedCheckpointLog {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Volatile
    }
    fn limits(&self) -> journal::LogLimits {
        self.inner.limits()
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        self.inner.health()
    }
    fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
        self.inner.recover()
    }
    fn maintenance_policy(&self) -> Option<journal::MaintenancePolicy> {
        self.policy
    }
    fn checkpoint_limits(&self) -> journal::CheckpointLimits {
        self.checkpoint_limits
    }
    fn status(&self) -> MetaFuture<'_, journal::LogStatus> {
        self.inner.status()
    }
    fn append_and_sync(
        &self,
        tip: journal::LogTip,
        records: Vec<log::Record>,
    ) -> MetaFuture<'_, journal::LogTip> {
        self.inner.append_and_sync(tip, records)
    }
    fn install_checkpoint(&self, covered: journal::LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            self.installs.fetch_add(1, Ordering::Relaxed);
            self.entered.notify_one();
            self.release.notified().await;
            self.inner.install_checkpoint(covered, bytes).await
        })
    }
}

#[tokio::test]
async fn p3_checkpoint_cancelled_caller_retains_singleflight_and_state_anchor() {
    let log = Arc::new(GatedCheckpointLog {
        inner: journal::MemoryLog::new(journal::LogLimits::default()).unwrap(),
        entered: Notify::new(),
        release: Notify::new(),
        policy: None,
        checkpoint_limits: journal::CheckpointLimits::default(),
        installs: AtomicU64::new(0),
    });
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    store
        .register_node_session(RequestKey::new("writer", "before"), lease("one"))
        .await
        .unwrap();
    let capture = {
        let store = store.clone();
        tokio::spawn(async move { store.checkpoint().await })
    };
    tokio::time::timeout(Duration::from_secs(5), log.entered.notified())
        .await
        .unwrap();
    assert_eq!(store.visible.metrics.checkpoint_inflight.get(), 1);
    capture.abort();
    assert!(capture.await.unwrap_err().is_cancelled());
    assert!(
        store
            .checkpoint()
            .await
            .unwrap_err()
            .to_string()
            .contains("already in progress")
    );
    store
        .register_node_session(RequestKey::new("writer", "after"), lease("two"))
        .await
        .unwrap();
    log.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.visible.checkpoint_active.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.visible.metrics.checkpoint_inflight.get(), 0);
    assert_eq!(store.visible.metrics.checkpoints.get(), 1);
    assert_eq!(store.visible.metrics.checkpoint_failures.get(), 0);
    assert!(store.visible.metrics.checkpoint_bytes.get() > 0);
    let recovered = log.recover().await.unwrap();
    assert_eq!(recovered.checkpoint.unwrap().covered.sequence, 1);
    assert_eq!(recovered.records.len(), 1);
    assert_eq!(store.committed().await.unwrap().log_tip.sequence, 2);
    let restored = Store::open_log(log).await.unwrap();
    assert_eq!(restored.committed().await.unwrap().log_tip.sequence, 2);
    assert_eq!(
        restored.committed().await.unwrap().node_epochs,
        store.committed().await.unwrap().node_epochs
    );
}

#[tokio::test]
async fn p3_operational_observer_timeout_keeps_task_and_capture_owned() {
    let log = Arc::new(GatedCheckpointLog {
        inner: journal::MemoryLog::new(journal::LogLimits::default()).unwrap(),
        entered: Notify::new(),
        release: Notify::new(),
        policy: Some(journal::MaintenancePolicy {
            wal_records: 2,
            poll: Duration::from_millis(5),
            checkpoint_timeout: Duration::from_millis(10),
            ..Default::default()
        }),
        checkpoint_limits: journal::CheckpointLimits::default(),
        installs: 0.into(),
    });
    let store = Store::open_log(log.clone()).await.unwrap();
    for request in ["one", "two"] {
        store
            .register_node_session(RequestKey::new("timeout", request), lease(request))
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), log.entered.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(store.visible.checkpoint_active.load(Ordering::Acquire));
    assert_eq!(log.installs.load(Ordering::Acquire), 1);
    assert_eq!(store.visible.metrics.checkpoint_inflight.get(), 1);
    assert_eq!(store.visible.metrics.checkpoint_failures.get(), 0);
    assert_eq!(store.visible.metrics.checkpoints.get(), 0);
    assert!(log.recover().await.unwrap().checkpoint.is_none());
    assert!(
        store
            .checkpoint()
            .await
            .unwrap_err()
            .to_string()
            .contains("already in progress")
    );
    store
        .register_node_session(RequestKey::new("timeout", "three"), lease("three"))
        .await
        .unwrap();
    log.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.visible.checkpoint_active.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.visible.metrics.checkpoint_inflight.get(), 0);
    assert_eq!(store.visible.metrics.checkpoints.get(), 1);
    assert_eq!(store.visible.metrics.checkpoint_failures.get(), 0);
    assert!(store.visible.metrics.checkpoint_bytes.get() > 0);
    let recovered = log.recover().await.unwrap();
    assert_eq!(recovered.checkpoint.unwrap().covered.sequence, 2);
    assert_eq!(recovered.tip.sequence, 3);
    assert_eq!(recovered.records.len(), 1);
    assert_eq!(log.installs.load(Ordering::Acquire), 1);
    assert_eq!(store.committed().await.unwrap().revision, StoreRevision(3));
}

#[tokio::test]
async fn p3_operational_capture_limit_rejects_before_install_without_poison() {
    let log = Arc::new(GatedCheckpointLog {
        inner: journal::MemoryLog::new(journal::LogLimits::default()).unwrap(),
        entered: Notify::new(),
        release: Notify::new(),
        policy: None,
        checkpoint_limits: journal::CheckpointLimits {
            image_max_bytes: 256,
            capture_charge_max_bytes: 256,
            disk_max_bytes: 256 * 1024,
            files_max: 5,
        },
        installs: 0.into(),
    });
    let store = Store::open_log(log.clone()).await.unwrap();
    store
        .register_node_session(RequestKey::new("capture", "one"), lease("one"))
        .await
        .unwrap();
    assert!(log::StateImage::capture_charge(&store.committed().await.unwrap()).unwrap() > 256);
    assert!(
        store
            .checkpoint()
            .await
            .unwrap_err()
            .to_string()
            .contains("capture logical charge exceeded")
    );
    assert_eq!(log.installs.load(Ordering::Acquire), 0);
    assert_eq!(store.visible.metrics.checkpoint_inflight.get(), 0);
    assert_eq!(store.visible.metrics.checkpoint_failures.get(), 1);
    assert_eq!(store.visible.metrics.checkpoints.get(), 0);
    assert!(!store.visible.checkpoint_active.load(Ordering::Acquire));
    assert!(log.recover().await.unwrap().checkpoint.is_none());
    store.health().await.unwrap();
    store
        .register_node_session(RequestKey::new("capture", "two"), lease("two"))
        .await
        .unwrap();
    assert_eq!(store.committed().await.unwrap().revision, StoreRevision(2));
}

struct PressureLocalLog {
    inner: local_log::LocalLog,
    installs: AtomicU64,
}
impl journal::MetaLog for PressureLocalLog {
    fn persistence(&self) -> BackendPersistence {
        BackendPersistence::Persistent
    }
    fn limits(&self) -> journal::LogLimits {
        self.inner.limits()
    }
    fn checkpoint_limits(&self) -> journal::CheckpointLimits {
        self.inner.checkpoint_limits()
    }
    fn health(&self) -> MetaFuture<'_, ()> {
        self.inner.health()
    }
    fn recover(&self) -> MetaFuture<'_, journal::RecoveredLog> {
        self.inner.recover()
    }
    fn status(&self) -> MetaFuture<'_, journal::LogStatus> {
        self.inner.status()
    }
    fn can_append(&self, tip: journal::LogTip, bytes: usize) -> MetaFuture<'_, bool> {
        self.inner.can_append(tip, bytes)
    }
    fn maintenance_policy(&self) -> Option<journal::MaintenancePolicy> {
        Some(journal::MaintenancePolicy {
            poll: Duration::from_millis(5),
            capacity_wait: Duration::from_secs(2),
            ..Default::default()
        })
    }
    fn append_and_sync(
        &self,
        tip: journal::LogTip,
        records: Vec<log::Record>,
    ) -> MetaFuture<'_, journal::LogTip> {
        self.inner.append_and_sync(tip, records)
    }
    fn install_checkpoint(&self, tip: journal::LogTip, bytes: Vec<u8>) -> MetaFuture<'_, ()> {
        Box::pin(async move {
            self.inner.install_checkpoint(tip, bytes).await?;
            self.installs.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
    }
}

#[tokio::test]
async fn p3_operational_real_wal_pressure_reclaims_and_continues() {
    let directory = tempfile::tempdir().unwrap().keep();
    eprintln!("P3 WAL pressure fixture={}", directory.display());
    let limits = journal::LogLimits {
        record_max_bytes: 1024,
        batch_max_bytes: 4096,
        wal_max_bytes: 128 * 1024,
        segment_bytes: 4608,
        segments_max: 4,
    };
    let log = Arc::new(PressureLocalLog {
        inner: local_log::LocalLog::open(&directory, limits).unwrap(),
        installs: 0.into(),
    });
    let store = Store::open_log(log.clone()).await.unwrap();
    let mut max_files = 0;
    let mut max_bytes = 0;
    for number in 0..24 {
        let outcome = store
            .register_node_session(
                RequestKey::new("pressure", format!("request-{number}")),
                lease(&format!("session-{number}")),
            )
            .await
            .unwrap();
        assert!(matches!(outcome,TxnOutcome::Committed{revision,..} if revision.0==number+1));
        let mut files = 0;
        let mut bytes = 0;
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy();
            if name.starts_with("wal-") && name.ends_with(".log") {
                files += 1;
                bytes += std::fs::metadata(path).unwrap().len();
            }
        }
        assert!(files <= limits.segments_max && bytes <= limits.wal_max_bytes);
        max_files = max_files.max(files);
        max_bytes = max_bytes.max(bytes);
    }
    assert!(
        log.installs.load(Ordering::Acquire) > 1,
        "actual pressure must install and reclaim repeatedly"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.visible.checkpoint_active.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let recovered = log.recover().await.unwrap();
    let image = recovered.checkpoint.unwrap();
    assert!(image.covered.sequence > 4);
    assert_eq!(recovered.tip.sequence, 24);
    let mut restored =
        log::StateImage::decode(&image.bytes, log.checkpoint_limits().image_max_bytes).unwrap();
    for record in recovered.records {
        restored = restored.replay_record(&record).unwrap();
    }
    assert_eq!(
        restored.snapshot_bytes_with_batch(0).unwrap(),
        store
            .committed()
            .await
            .unwrap()
            .snapshot_bytes_with_batch(0)
            .unwrap()
    );
    eprintln!(
        "P3 WAL pressure PASS ACK=24 checkpoint={} installs={} max_sampled_files={max_files} max_sampled_bytes={max_bytes} files_cap=4 WAL_cap=131072",
        image.covered.sequence,
        log.installs.load(Ordering::Acquire)
    );
    drop(store);
    tokio::time::timeout(Duration::from_secs(5), async {
        while Arc::strong_count(&log) > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(log);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn p3_checkpoint_local_reopen_restores_image_and_only_replays_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let local =
        Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
    let identity = local.identity();
    let store = Store::open_log(local.clone()).await.unwrap();
    let request = RequestKey::new("writer", "checkpoint-first");
    let first = store
        .register_node_session(request.clone(), lease("one"))
        .await
        .unwrap();
    let first_state = store.committed().await.unwrap();
    let covered = store
        .checkpoint()
        .await
        .expect("published state must install a durable canonical checkpoint");
    assert_eq!(covered.sequence, 1);
    assert!(dir.path().join("CURRENT").is_file());
    store
        .register_node_session(RequestKey::new("writer", "suffix"), lease("two"))
        .await
        .unwrap();
    let expected = store
        .committed()
        .await
        .unwrap()
        .snapshot_bytes_with_batch(0)
        .unwrap();
    drop(store);
    tokio::time::timeout(Duration::from_secs(5), async {
        while Arc::strong_count(&local) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(local);
    let local =
        Arc::new(local_log::LocalLog::open(dir.path(), journal::LogLimits::default()).unwrap());
    assert_eq!(local.identity(), identity);
    assert_eq!(
        local.recover().await.unwrap().records.len(),
        1,
        "covered record must not be replayed twice"
    );
    let restored = Store::open_log(local.clone()).await.unwrap();
    let actual = restored.committed().await.unwrap();
    assert_eq!(actual.snapshot_bytes_with_batch(0).unwrap(), expected);
    assert_eq!(
        actual.request_completion.get(&request),
        first_state.request_completion.get(&request)
    );
    let retry = restored
        .register_node_session(request, lease("one"))
        .await
        .unwrap();
    assert!(
        matches!((first, retry), (TxnOutcome::Committed { outcome: original, .. }, TxnOutcome::ConditionFailed { existing_outcome: Some(retried), .. }) if original == retried)
    );
    assert_eq!(local.recover().await.unwrap().tip.sequence, 2);
}
// A-P3-77 bounded resource report (test-only).
#[tokio::test]
async fn p3_a77_owned_memory_and_real_wal_resource_report() {
    use crate::dfs::{
        ChunkEncoding, ChunkId, ContentDigest, DigestAlgorithm, InodeAttributes, InodeKind,
    };
    use serde_json::json;
    use std::{collections::BTreeMap, mem::size_of, path::Path};

    fn inode(number: usize) -> InodeRecord {
        InodeRecord {
            namespace_id: NamespaceId::new("meta-bench"),
            inode_id: InodeId::new(format!("bench-inode-{number:016x}")),
            kind: InodeKind::Regular,
            attributes: InodeAttributes {
                mode: 0o600,
                uid: 0,
                gid: 0,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            },
            head_version: None,
            symlink_target: None,
            xattrs: BTreeMap::new(),
            revision: 1,
        }
    }
    fn txn(id: &str, operation: StoreOperation, values: Vec<MetaEntity>) -> MetaTxn {
        let request = RequestKey::new("a77-report", id);
        let mut txn = MetaTxn::new(request.clone(), operation);
        txn.conditions
            .push(TxnCondition::RequestAbsent(request.clone()));
        txn.mutations
            .extend(values.into_iter().map(TxnMutation::Put));
        txn.mutations
            .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
                request,
                operation,
                result: OperationResult::Empty,
            }));
        txn
    }
    fn disk_bytes(path: &Path, prefix: &str) -> u64 {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with(prefix))
            .map(|entry| entry.metadata().unwrap().len())
            .sum()
    }
    fn memory(key: &MetaKey, value: &VersionedEntity) -> serde_json::Value {
        let inline = size_of::<MetaKey>() + size_of::<VersionedEntity>();
        let charge = history::authority_charge(key, &value.entity).unwrap();
        // This is the existing production accounting model, not a malloc
        // or RSS observation. Fixed tree/Arc slack is not payload copying.
        let structural = 1024;
        assert!(charge >= inline as u64 + structural);
        json!({
            "inline_key_bytes":size_of::<MetaKey>(),
            "inline_versioned_entity_bytes":size_of::<VersionedEntity>(),
            "owned_heap_capacity_bytes":charge-inline as u64-structural,
            "im_arc_logical_structural_reserve_bytes":structural,
            "total_logical_authority_charge_bytes":charge,
            "scope":"one typed entity; owned capacities and existing reserve, not physical allocation or RSS"
        })
    }
    let limits = journal::LogLimits::default();
    let checkpoint = journal::CheckpointLimits::default();
    let history = history::Limits::default();
    let mut observations = Vec::new();
    for files in [64, 128] {
        let directory = tempfile::tempdir().unwrap();
        let local = local_log::LocalLog::open(directory.path(), limits).unwrap();
        let mut values = Vec::new();
        for number in 0..files {
            values.push(MetaEntity::DfsInode(inode(number)));
            values.push(MetaEntity::DfsChunk(ChunkObject {
                id: ChunkId::new(format!("bench-chunk-{number:016x}")),
                length: 4 * 1024 * 1024,
                content_digest: ContentDigest {
                    algorithm: DigestAlgorithm::Blake3,
                    bytes: [7; 32],
                },
                encoding: ChunkEncoding::Raw,
            }));
        }
        let (mut state, outcome, decision) = State::default()
            .prepare_txn_record(
                txn("seed-0000000000000001", StoreOperation::DfsCreate, values),
                1,
                limits.record_max_bytes,
            )
            .unwrap();
        assert!(matches!(outcome, TxnOutcome::Committed { .. }));
        let seed_record = log::Record::from(decision.unwrap());
        let seed_payload = codec::encoded_size(&seed_record).unwrap();
        let seed_frame = journal::frame_size(journal::LogTip::EMPTY, &seed_record).unwrap();
        let initial_wal = disk_bytes(directory.path(), "wal-");
        state.log_tip = local
            .append_and_sync(journal::LogTip::EMPTY, vec![seed_record])
            .await
            .unwrap();
        let after_seed_wal = disk_bytes(directory.path(), "wal-");
        assert_eq!(after_seed_wal - initial_wal, seed_frame as u64);
        let image = log::StateImage::encode(&state, checkpoint.image_max_bytes).unwrap();
        let image_payload = image.len();
        local
            .install_checkpoint(state.log_tip, image)
            .await
            .unwrap();
        let installed_checkpoint = disk_bytes(directory.path(), "checkpoint-");
        assert!(installed_checkpoint >= image_payload as u64);

        let inode_key = MetaKey::DfsInode(inode(0).inode_id);
        let chunk_key = MetaKey::DfsChunk(ChunkId::new("bench-chunk-0000000000000000"));
        let (stored_inode_key, stored_inode) = state
            .entities
            .iter()
            .find(|(key, _)| *key == &inode_key)
            .unwrap();
        let (stored_chunk_key, stored_chunk) = state
            .entities
            .iter()
            .find(|(key, _)| *key == &chunk_key)
            .unwrap();
        let inode_memory = memory(stored_inode_key, stored_inode);
        let chunk_memory = memory(stored_chunk_key, stored_chunk);
        let scanned_authority_bytes = state
            .entities
            .iter()
            .map(|(key, value)| history::authority_charge(key, &value.entity).unwrap())
            .sum::<u64>();
        assert_eq!(scanned_authority_bytes, state.authority_bytes);
        assert_eq!(
            state.authority_bytes,
            files as u64
                * (inode_memory["total_logical_authority_charge_bytes"]
                    .as_u64()
                    .unwrap()
                    + chunk_memory["total_logical_authority_charge_bytes"]
                        .as_u64()
                        .unwrap())
        );
        let old_inode = state.entities.get(&inode_key).unwrap();
        let MetaEntity::DfsInode(mut updated_inode) = old_inode.entity.clone() else {
            panic!("fixture inode missing")
        };
        updated_inode.attributes.mode = 0o640;
        updated_inode.revision = 2;
        let (mut updated, outcome, decision) = state
            .prepare_txn_record(
                txn(
                    "edit-0000000000000001",
                    StoreOperation::DfsSetInodeAttributes,
                    vec![MetaEntity::DfsInode(updated_inode)],
                ),
                2,
                limits.record_max_bytes,
            )
            .unwrap();
        assert!(matches!(outcome, TxnOutcome::Committed { .. }));
        let record = log::Record::from(decision.unwrap());
        let update_payload = codec::encoded_size(&record).unwrap();
        let update_frame = journal::frame_size(state.log_tip, &record).unwrap();
        let before_update = disk_bytes(directory.path(), "wal-");
        updated.log_tip = local
            .append_and_sync(state.log_tip, vec![record])
            .await
            .unwrap();
        let after_update = disk_bytes(directory.path(), "wal-");
        assert_eq!(after_update - before_update, update_frame as u64);
        assert!(update_frame > update_payload);
        assert_eq!(updated.entities.len(), 2 * files);
        assert!(
            matches!(&old_inode.entity, MetaEntity::DfsInode(inode) if inode.attributes.mode==0o600)
        );
        let restored = local.recover().await.unwrap();
        let mut replayed = log::StateImage::decode(
            &restored.checkpoint.unwrap().bytes,
            checkpoint.image_max_bytes,
        )
        .unwrap();
        for record in restored.records {
            replayed = replayed.replay_record(&record).unwrap();
        }
        assert_eq!(
            replayed.snapshot_bytes_with_batch(0).unwrap(),
            updated.snapshot_bytes_with_batch(0).unwrap()
        );
        observations.push(json!({
            "fixture_pairs":files,"entities":state.entities.len(),
            "inode_memory":inode_memory,"chunk_memory":chunk_memory,
            "initial_wal_file_bytes":initial_wal,
            "seed_decision_bytes":seed_payload,"seed_frame_and_wal_growth_bytes":seed_frame,
            "checkpoint_image_payload_bytes":image_payload,"checkpoint_installed_file_bytes":installed_checkpoint,
            "edit_decision_bytes":update_payload,"edit_frame_bytes":update_frame,
            "edit_actual_wal_file_growth_bytes":after_update-before_update,
            "edit_framing_ratio":update_frame as f64/update_payload as f64,
            "total_wal_bytes_after_edit":after_update,
            "whole_state_authority_logical_charge_bytes":state.authority_bytes,
            "scanned_actual_key_authority_logical_charge_bytes":scanned_authority_bytes,
            "history_fixed_root_logical_reserve_bytes":state.history_stats().index_base_bytes,
            "history_stats_requests":state.history_stats().requests,
            "history_stats_events":state.history_stats().events,
            "checkpoint_plus_suffix_restores_same_fields":true
        }));
    }
    assert_eq!(
        observations[0]["edit_actual_wal_file_growth_bytes"],
        observations[1]["edit_actual_wal_file_growth_bytes"]
    );
    assert_eq!(
        observations[0]["inode_memory"],
        observations[1]["inode_memory"]
    );
    assert_eq!(
        observations[0]["chunk_memory"],
        observations[1]["chunk_memory"]
    );
    assert!(
        observations[1]["checkpoint_installed_file_bytes"]
            .as_u64()
            .unwrap()
            > observations[0]["checkpoint_installed_file_bytes"]
                .as_u64()
                .unwrap()
    );
    println!(
        "P3_A77_RESOURCE_REPORT={}",
        json!({
            "schema":1,"kind":"bounded diagnostic, not formal performance",
            "architecture":std::env::consts::ARCH,"pointer_bytes":size_of::<usize>(),
            "im_version":"15.1.0","im_map_header_bytes":size_of::<im::OrdMap<MetaKey,Arc<VersionedEntity>>>(),
            "scope":"logical estimates use existing owned capacity visitor and reserve; no allocator/RSS claim; synthetic inode+chunk pairs are not S/M datasets",
            "wal_ratio_boundary":"actual WAL file length increment / canonical Record encoding bytes; full frame includes envelope/CRC/hash; excludes ACK slot overwrites, fsync, device amplification, CURRENT/checkpoint writes",
            "limits":{
                "record_max_bytes":limits.record_max_bytes,"batch_max_bytes":limits.batch_max_bytes,"wal_max_bytes":limits.wal_max_bytes,
                "segment_bytes":limits.segment_bytes,"segments_max":limits.segments_max,
                "checkpoint_image_max_bytes":checkpoint.image_max_bytes,"checkpoint_capture_max_bytes":checkpoint.capture_charge_max_bytes,
                "checkpoint_disk_max_bytes":checkpoint.disk_max_bytes,"checkpoint_files_max":checkpoint.files_max,
                "requests_max":history.request_count,"request_bytes_max":history.request_bytes,"request_retention_ms":history.request_retention.as_millis(),
                "events_max":history.event_count,"event_bytes_max":history.event_bytes,"event_retention_ms":history.event_retention.as_millis(),
                "cleanup_items":history.cleanup_items,"protected_requests":history.protected_requests
            },
            "observations":observations
        })
    );
}
