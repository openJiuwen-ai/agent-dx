use super::*;
use adx_master::storage::{ClaimOutcome, LocalClaim, NodeSession};

async fn claimant(s: &adx_master::storage::Session, id: &str) -> LocalClaim {
    s.register_session(
        node(id),
        "127.0.0.1:9000".into(),
        "127.0.0.1:9001".into(),
        Some(NodeSession {
            id: format!("boot-{id}"),
            sequence: 1,
            routable: true,
        }),
    )
    .await
    .unwrap();
    LocalClaim {
        node_id: id.into(),
        node_session_id: format!("boot-{id}"),
        devices: vec![],
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn competing_local_claims_return_one_owner_and_replay_without_mutation() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(2).await.unwrap();
    let a = claimant(&s, "a").await;
    let b = claimant(&s, "b").await;
    let (x, y) = tokio::join!(s.claim(spec("i"), &a), s.claim(spec("i"), &b));
    let x = x.unwrap();
    let y = y.unwrap();
    assert_ne!(
        matches!(x, ClaimOutcome::Owned(_)),
        matches!(y, ClaimOutcome::Owned(_))
    );
    assert_eq!(x.record(), y.record());
    let stored = s.get("i").await.unwrap();
    assert_eq!(x.record(), &stored);
    let before = s.snapshot().await.unwrap();
    assert_eq!(s.claim(spec("i"), &a).await.unwrap(), x);
    assert_eq!(s.claim(spec("i"), &b).await.unwrap(), y);
    assert_eq!(s.snapshot().await.unwrap(), before);
    assert!(before.routes().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn same_node_concurrent_claims_share_assignment_but_changed_specs_conflict() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    let a = claimant(&s, "a").await;
    let (x, y) = tokio::join!(s.claim(spec("i"), &a), s.claim(spec("i"), &a));
    let x = x.unwrap();
    assert!(matches!(x, ClaimOutcome::Owned(_)));
    assert_eq!(x, y.unwrap());
    let before = s.snapshot().await.unwrap();
    for change_tenant in [false, true] {
        let mut changed = spec("i");
        if change_tenant {
            changed.tenant_id = "other".into();
        } else {
            changed.image = "other".into();
        }
        assert_eq!(s.claim(changed, &a).await, Err(Error::Conflict));
    }
    assert_eq!(s.snapshot().await.unwrap(), before);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn claims_fence_old_master_node_sessions_and_closed_admission() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    let a = claimant(&s, "a").await;
    s.claim(spec("i"), &a).await.unwrap();
    let mut stale = a.clone();
    stale.node_session_id = "stale".into();
    assert_eq!(s.claim(spec("i"), &stale).await, Err(Error::Conflict));
    assert_eq!(s.claim(spec("new"), &stale).await, Err(Error::Conflict));
    s.register_session(
        node("a"),
        "127.0.0.1:9000".into(),
        "127.0.0.1:9001".into(),
        Some(NodeSession {
            id: a.node_session_id.clone(),
            sequence: 2,
            routable: false,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(
        s.claim(spec("new"), &a).await,
        Err(Error::Unavailable(_))
    ));
    assert!(matches!(
        s.claim(spec("i"), &a).await,
        Err(Error::Unavailable(_))
    ));
    let next = db.begin(1).await.unwrap();
    assert_eq!(s.claim(spec("i"), &a).await, Err(Error::Conflict));
    assert_eq!(next.get("i").await.unwrap().assignment.node_id, "a");
    assert_eq!(next.get("new").await, Err(Error::NotFound));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn completed_or_invalidated_claims_only_return_existing_results() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    let a = claimant(&s, "a").await;
    let b = claimant(&s, "b").await;
    let first = s.claim(spec("i"), &a).await.unwrap();
    let r = running(spec("i"), first.record().assignment.clone());
    s.commit(r.clone()).await.unwrap();
    for c in [&a, &b] {
        let answer = s.claim(spec("i"), c).await.unwrap();
        assert!(matches!(answer, ClaimOutcome::Existing(_)));
        assert_eq!(answer.record().result, Some(r.clone()));
    }
    s.invalidate_node("a", &a.node_session_id).await.unwrap();
    let before = s.snapshot().await.unwrap();
    let answer = s.claim(spec("i"), &b).await.unwrap();
    assert!(matches!(answer, ClaimOutcome::Existing(_)));
    assert!(answer.record().invalidated);
    assert_eq!(s.snapshot().await.unwrap(), before);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn local_claim_and_scheduler_reservation_cannot_create_two_owners() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    let a = claimant(&s, "a").await;
    claimant(&s, "b").await;
    let candidate = Assignment {
        instance_id: "i".into(),
        node_id: "b".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    let (local, central) = tokio::join!(s.claim(spec("i"), &a), s.reserve(spec("i"), candidate));
    let local = local.unwrap();
    let stored = s.get("i").await.unwrap();
    assert_eq!(local.record(), &stored);
    match central {
        Ok(central) => {
            assert_eq!(central, stored);
            assert!(matches!(local, ClaimOutcome::Existing(_)));
        }
        Err(error) => {
            assert_eq!(error, Error::Conflict);
            assert!(matches!(local, ClaimOutcome::Owned(_)));
        }
    }
    assert_eq!(s.snapshot().await.unwrap().instances.len(), 1);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn lost_claim_reply_can_be_queried_and_retried_after_redis_restart() {
    let mut rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    let a = claimant(&s, "a").await;
    // Discard the acknowledgement as a caller would after losing its response.
    s.claim(spec("i"), &a).await.unwrap();
    let before = s.snapshot().await.unwrap();
    rig.crash();
    assert!(matches!(
        s.claim(spec("i"), &a).await,
        Err(Error::Unavailable(_))
    ));
    rig.start().await;
    let answer = s.claim(spec("i"), &a).await.unwrap();
    assert!(matches!(answer, ClaimOutcome::Owned(_)));
    assert_eq!(answer.record(), &s.get("i").await.unwrap());
    assert_eq!(s.snapshot().await.unwrap(), before);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn concurrent_claims_allocate_exact_generations_above_lua_integer_precision() {
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    let a = claimant(&s, "a").await;
    let high = (1_u64 << 53) + 7;
    s.reserve(
        spec("seed"),
        Assignment {
            instance_id: "seed".into(),
            node_id: "a".into(),
            shard_id: 0,
            generation: high,
            devices: vec![],
        },
    )
    .await
    .unwrap();
    let (x, y) = tokio::join!(s.claim(spec("x"), &a), s.claim(spec("y"), &a));
    let mut generations = [
        x.unwrap().record().assignment.generation,
        y.unwrap().record().assignment.generation,
    ];
    generations.sort();
    assert_eq!(generations, [high + 1, high + 2]);
    assert_eq!(s.snapshot().await.unwrap().generation, high + 2);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn device_choice_is_part_of_owned_replay_and_bad_claims_do_not_consume_generation() {
    use adx_core::scheduling::{DeviceAllocation, DeviceKind, DeviceRequest};
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    let mut a = claimant(&s, "a").await;
    let b = claimant(&s, "b").await;
    let mut request = spec("gpu");
    request.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Gpu,
        model: Some("test".into()),
        count: 1,
    }];
    let before = s.snapshot().await.unwrap();
    assert!(s.claim(request.clone(), &a).await.is_err());
    assert_eq!(s.snapshot().await.unwrap(), before);
    a.devices = vec![DeviceAllocation {
        kind: DeviceKind::Gpu,
        model: "test".into(),
        id: 0,
    }];
    let first = s.claim(request.clone(), &a).await.unwrap();
    a.devices[0].id = 1;
    assert_eq!(s.claim(request.clone(), &a).await, Err(Error::Conflict));
    let mut b = b;
    b.devices = a.devices;
    let other = s.claim(request, &b).await.unwrap();
    assert!(matches!(other, ClaimOutcome::Existing(_)));
    assert_eq!(other.record(), first.record());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn snapshot_claim_requires_existing_restore_reference() {
    use adx_master::snapshots::{Reference, Snapshot};
    let rig = common::Redis::new().await;
    let db = rig.store().await;
    let s = db.begin(1).await.unwrap();
    let a = claimant(&s, "a").await;
    let snapshot = Snapshot::new(
        "snap".into(),
        vec![],
        spec("source"),
        "a".into(),
        "source-1".into(),
        adx_core::CheckpointArtifact {
            storage: "shared".into(),
            location: "artifact".into(),
            size_bytes: 1,
        },
    )
    .unwrap();
    s.publish_snapshot(snapshot).await.unwrap();
    let mut request = spec("clone");
    request.snapshot_id = Some("snap".into());
    let before = s.snapshot().await.unwrap();
    assert_eq!(s.claim(request.clone(), &a).await, Err(Error::Conflict));
    assert_eq!(s.snapshot().await.unwrap(), before);
    s.acquire_snapshot(
        "snap",
        "tenant",
        Reference::Restore {
            instance_id: "clone".into(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        s.claim(request, &a).await.unwrap(),
        ClaimOutcome::Owned(_)
    ));
}
