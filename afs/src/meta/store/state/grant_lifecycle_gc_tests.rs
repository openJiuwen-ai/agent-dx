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
        ownerfs_enabled: true,
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
        ownerfs_enabled: true,
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
                    requester_node_id: String::new(),
                    requester_session_id: String::new(),
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
            state.log_cursor,
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
