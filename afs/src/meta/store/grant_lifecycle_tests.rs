use super::*;
use crate::meta::owner_roots::*;
use crate::meta::store::{
    RequestOutcome, Store,
    journal::{MemoryLog, MetaLog, frame_size},
};
use std::time::Duration;

async fn fixture() -> (Arc<Store>, Arc<MemoryLog>, StoreOwnerRootAuthority) {
    let log = Arc::new(MemoryLog::new(Default::default()).unwrap());
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    store
        .register_node_session(
            RequestKey::new("home", "register"),
            super::super::store::NodeSessionLease {
                node_id: "home".into(),
                session_id: "s".into(),
                grpc_addr: "g".into(),
                data_addr: "d".into(),
                rest_addr: "r".into(),
                nfs_addr: String::new(),
                ownerfs_enabled: true,
                storage_devices: Vec::new(),
                lease_ttl: Duration::from_secs(300),
            },
        )
        .await
        .unwrap();
    let authority = StoreOwnerRootAuthority::new(store.clone());
    (store, log, authority)
}
async fn reserve(authority: &StoreOwnerRootAuthority, intent: &str) -> RootReservationRecord {
    authority
        .reserve_root(ReserveRootInput {
            request_id: format!("reserve-{intent}"),
            root_id: "workspace".into(),
            preferred_home_node_id: "home".into(),
            session_id: "s".into(),
            rights: vec![RootRight::Read],
            expected_root_epoch: 0,
            create_intent_id: intent.into(),
        })
        .await
        .unwrap()
        .reservation
}
async fn activate(
    authority: &StoreOwnerRootAuthority,
    r: &RootReservationRecord,
) -> RootAccessGrant {
    authority
        .activate_root(ActivateRootInput {
            request_id: format!("activate-{}", r.create_intent_id),
            local_prepare_id: "prepared".into(),
            reservation: WireRootReservation {
                root_id: r.root_id.clone(),
                root_epoch: r.root_epoch,
                home_node_id: r.home_node_id.clone(),
                session_id: r.home_session_id.clone(),
                create_intent_id: r.create_intent_id.clone(),
                prepare_token: r.prepare_token.clone(),
                requester_node_id: r.requester_node_id.clone(),
                requester_session_id: r.requester_session_id.clone(),
            },
        })
        .await
        .unwrap()
}
fn deactivate(epoch: u64) -> DeactivateRootInput {
    DeactivateRootInput {
        request_id: "deactivate".into(),
        root_id: "workspace".into(),
        root_epoch: epoch,
        home_node_id: "home".into(),
        home_session_id: "s".into(),
        reason: "finished".into(),
        authenticated_home_node_id: Some("home".into()),
    }
}
#[tokio::test]
async fn root_epoch_is_not_reused_after_abort_and_reopen() {
    let (_store, log, authority) = fixture().await;
    let first = reserve(&authority, "first").await;
    authority
        .abort_root(AbortRootInput {
            request_id: "abort".into(),
            root_id: first.root_id.clone(),
            root_epoch: first.root_epoch,
            session_id: first.home_session_id.clone(),
            create_intent_id: first.create_intent_id.clone(),
            prepare_token: first.prepare_token.clone(),
            authenticated_home_node_id: Some("home".into()),
        })
        .await
        .unwrap();
    let reopened = Arc::new(Store::open_log(log).await.unwrap());
    let authority = StoreOwnerRootAuthority::new(reopened);
    let second = reserve(&authority, "second").await;
    assert!(
        second.root_epoch > first.root_epoch,
        "aborted/replayed reservations must never reuse epoch"
    );
    assert_eq!(
        reserve(&authority, "second").await,
        second,
        "same intent must retain allocated epoch"
    );
}
#[tokio::test]
async fn deactivation_record_does_not_scale_with_historical_grants() {
    let (store, log, authority) = fixture().await;
    let reservation = reserve(&authority, "first").await;
    let home = activate(&authority, &reservation).await;
    let key = RequestKey::new("fixture", "seed-grants");
    let mut txn = MetaTxn::new(key.clone(), StoreOperation::AcquireRoot);
    txn.conditions
        .push(TxnCondition::RequestAbsent(key.clone()));
    for i in 0..140 {
        let mut grant = home.clone();
        grant.holder_node_id = "peer".into();
        grant.holder_session_id = format!("old-{i}");
        txn.mutations
            .push(TxnMutation::Put(MetaEntity::RootGrant(grant)));
    }
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: key,
            operation: txn.operation,
            result: OperationResult::Empty,
        }));
    assert!(matches!(
        store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::Committed { .. }
    ));
    let tip = log.recover().await.unwrap().tip;
    authority
        .deactivate_root(deactivate(home.root_epoch))
        .await
        .unwrap();
    let recovery = log.recover().await.unwrap();
    assert_eq!(recovery.tip.sequence, tip.sequence + 1);
    let bytes = frame_size(tip.at(recovery.term), recovery.records.last().unwrap()).unwrap();
    assert!(
        bytes <= 1536,
        "deactivate must remain a small record, got {bytes} bytes"
    );
    assert_eq!(
        store
            .read(MetaRead::RootGrants {
                root_id: "workspace".into()
            })
            .await
            .unwrap()
            .entities
            .len(),
        141,
        "logical invalidation must not require eager deletion"
    );
    assert!(
        authority
            .validate_root_access(ValidateRootAccessInput {
                root_id: home.root_id.clone(),
                root_epoch: home.root_epoch,
                home_session_id: home.home_session_id.clone(),
                holder_node_id: home.holder_node_id.clone(),
                holder_session_id: home.holder_session_id.clone(),
                access_generation: home.access_generation,
                fencing_token: home.fencing_token.clone(),
            })
            .await
            .is_err()
    );
    let next = reserve(&authority, "second").await;
    assert!(
        next.root_epoch > home.root_epoch,
        "same-name recreation must fence the old incarnation"
    );
    let new_home = activate(&authority, &next).await;
    assert_ne!(new_home.root_epoch, home.root_epoch);
}
#[tokio::test]
async fn grant_lifecycle_recovery_selects_current_home_and_gc_preserves_it() {
    let (store, _, authority) = fixture().await;
    let reservation = reserve(&authority, "first").await;
    let old = activate(&authority, &reservation).await;
    store
        .register_node_session(
            RequestKey::new("home", "register-new"),
            NodeSessionLease {
                node_id: "home".into(),
                session_id: "zz-new".into(),
                grpc_addr: "g".into(),
                data_addr: "d".into(),
                rest_addr: "r".into(),
                nfs_addr: String::new(),
                ownerfs_enabled: true,
                storage_devices: vec![],
                lease_ttl: Duration::from_secs(300),
            },
        )
        .await
        .unwrap();
    let home = authority
        .recover_root(RecoverRootInput {
            request_id: "recover".into(),
            root_id: old.root_id.clone(),
            expected_root_epoch: old.root_epoch,
            home_node_id: "home".into(),
            home_session_id: "zz-new".into(),
            local_prepare_id: "prepared".into(),
        })
        .await
        .unwrap();
    assert_eq!(home.access_generation, old.access_generation + 1);
    let current = store.committed().await.unwrap();
    let (cleaned, _) = current
        .prepare_root_grant_cleanup(&mut None, now_unix_ms(), 1024 * 1024, 32)
        .unwrap()
        .unwrap();
    assert!(
        cleaned
            .read(MetaRead::RootGrantByHolder {
                root_id: home.root_id.clone(),
                holder_node_id: "home".into(),
                holder_session_id: "s".into()
            })
            .unwrap()
            .entity
            .is_none()
    );
    assert_eq!(
        cleaned
            .read(MetaRead::RootGrantByHolder {
                root_id: home.root_id.clone(),
                holder_node_id: "home".into(),
                holder_session_id: "zz-new".into()
            })
            .unwrap()
            .entity,
        Some(MetaEntity::RootGrant(home.clone()))
    );
    let mut input = deactivate(home.root_epoch);
    input.home_session_id = "zz-new".into();
    authority.deactivate_root(input).await.unwrap();
    let view = store.committed().await.unwrap();
    let key = MetaKey::RootCommand {
        command_id: format!("deactivate:workspace:{}:deactivate", home.root_epoch),
    };
    assert!(
        matches!(&view.entities.get(&key).unwrap().entity,MetaEntity::RootCommand(command)
        if command.home_session_id=="zz-new" && command.old_access_generation==home.access_generation)
    );
}
#[tokio::test]
async fn grant_lifecycle_concurrent_reservations_get_distinct_epochs() {
    let (store, _, _) = fixture().await;
    let mut jobs = tokio::task::JoinSet::new();
    for i in 0..16 {
        let store = store.clone();
        jobs.spawn(async move {
            StoreOwnerRootAuthority::new(store)
                .reserve_root(ReserveRootInput {
                    request_id: format!("r-{i}"),
                    root_id: format!("root-{i}"),
                    preferred_home_node_id: "home".into(),
                    session_id: "s".into(),
                    rights: vec![RootRight::Read],
                    expected_root_epoch: 0,
                    create_intent_id: format!("i-{i}"),
                })
                .await
                .unwrap()
                .reservation
                .root_epoch
        });
    }
    let mut epochs = std::collections::BTreeSet::new();
    while let Some(v) = jobs.join_next().await {
        assert!(epochs.insert(v.unwrap()));
    }
    assert_eq!(epochs.len(), 16);
}
#[tokio::test]
async fn grant_lifecycle_expected_epoch_cannot_exhaust_authority() {
    let (store, log, authority) = fixture().await;
    let tip = log.recover().await.unwrap().tip;
    let before = store.committed().await.unwrap();
    let rejected = authority
        .reserve_root(ReserveRootInput {
            request_id: "max".into(),
            root_id: "other".into(),
            preferred_home_node_id: "home".into(),
            session_id: "s".into(),
            rights: vec![],
            expected_root_epoch: u64::MAX,
            create_intent_id: "max".into(),
        })
        .await;
    assert!(
        rejected.is_err(),
        "caller must not raise the allocation watermark"
    );
    assert_eq!(log.recover().await.unwrap().tip, tip);
    assert!(Arc::ptr_eq(&before, &store.committed().await.unwrap()));
    let allocated = reserve(&authority, "after-invalid").await;
    assert_eq!(allocated.root_epoch, 1);
    assert_eq!(reserve(&authority, "after-invalid").await, allocated);
}
#[tokio::test]
async fn grant_lifecycle_background_cleanup_local_wal_and_checkpoint_recovery() {
    use crate::meta::store::{local_log::LocalLog, log::StateImage};
    let directory = tempfile::tempdir().unwrap();
    let log = Arc::new(LocalLog::open(directory.path(), Default::default()).unwrap());
    let identity = log.identity();
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    store
        .register_node_session(
            RequestKey::new("home", "register"),
            NodeSessionLease {
                node_id: "home".into(),
                session_id: "s".into(),
                grpc_addr: "g".into(),
                data_addr: "d".into(),
                rest_addr: "r".into(),
                nfs_addr: String::new(),
                ownerfs_enabled: true,
                storage_devices: vec![],
                lease_ttl: Duration::from_secs(300),
            },
        )
        .await
        .unwrap();
    let authority = StoreOwnerRootAuthority::new(store.clone());
    let first = reserve(&authority, "first").await;
    let old = activate(&authority, &first).await;
    authority
        .deactivate_root(deactivate(old.root_epoch))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if store
                .read(MetaRead::RootGrants {
                    root_id: "workspace".into(),
                })
                .await
                .unwrap()
                .entities
                .is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("bounded maintenance must eventually collect inactive Root grants");
    let tip = log.recover().await.unwrap().tip;
    assert!(log.recover().await.unwrap().records.iter().any(|record| {
        serde_json::to_value(record).unwrap()["Decision"]["decision"]["operation"]
            == "CleanupRootGrants"
    }));
    let current = store.committed().await.unwrap();
    let image = StateImage::encode(&current, 1024 * 1024).unwrap();
    log.install_checkpoint(tip, image).await.unwrap();
    drop(authority);
    drop(store);
    tokio::time::timeout(Duration::from_secs(2), async {
        while Arc::strong_count(&log) > 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(log);
    let log = Arc::new(LocalLog::open(directory.path(), Default::default()).unwrap());
    assert_eq!(log.identity(), identity);
    let store = Arc::new(Store::open_log(log.clone()).await.unwrap());
    let authority = StoreOwnerRootAuthority::new(store.clone());
    assert!(
        store
            .read(MetaRead::RootGrants {
                root_id: "workspace".into()
            })
            .await
            .unwrap()
            .entities
            .is_empty()
    );
    let second = reserve(&authority, "second").await;
    assert!(second.root_epoch > first.root_epoch);
    activate(&authority, &second).await;
    assert!(
        authority
            .validate_root_access(ValidateRootAccessInput {
                root_id: old.root_id,
                root_epoch: old.root_epoch,
                home_session_id: old.home_session_id,
                holder_node_id: old.holder_node_id,
                holder_session_id: old.holder_session_id,
                access_generation: old.access_generation,
                fencing_token: old.fencing_token
            })
            .await
            .is_err()
    );
}
