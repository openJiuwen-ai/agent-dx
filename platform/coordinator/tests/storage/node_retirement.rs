use super::*;
use adx_coordinator::storage::NodeSession;

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn expired_empty_node_is_retired_and_does_not_return_after_restart() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    session
        .register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: "boot-one".into(),
                sequence: 7,
                routable: true,
            }),
        )
        .await
        .unwrap();
    session.invalidate_node("old", "boot-one").await.unwrap();
    assert!(!session
        .retire_expired_node("old", "boot-one", 0, 1800)
        .await
        .unwrap());
    assert!(session
        .retire_expired_node("old", "boot-one", u64::MAX, 1800)
        .await
        .unwrap());
    assert!(!session.snapshot().await.unwrap().nodes.contains_key("old"));
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        1
    );
    let restarted = store.begin(1).await.unwrap();
    assert!(!restarted
        .snapshot()
        .await
        .unwrap()
        .nodes
        .contains_key("old"));
    assert_eq!(
        restarted
            .business_counters()
            .await
            .unwrap()
            .worker_incidents,
        1
    );
}

async fn registered(session: &adx_coordinator::storage::Session, boot: &str) {
    session
        .register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: boot.into(),
                sequence: 7,
                routable: true,
            }),
        )
        .await
        .unwrap();
}

async fn register_host(
    session: &adx_coordinator::storage::Session,
    process: &str,
    host: Option<&str>,
) -> StoredNode {
    let mut report = node("reused-name");
    if let Some(host) = host {
        report
            .labels
            .insert("ADX_NODE_HOST_UUID".into(), host.into());
    }
    session
        .register_session(
            report,
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: process.into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_scheduling_pause_does_not_follow_a_replacement_host() {
    const OLD_HOST: &str = "11111111-1111-4111-8111-111111111111";
    const NEW_HOST: &str = "22222222-2222-4222-8222-222222222222";
    for retire in [false, true] {
        let rig = common::Redis::new().await;
        let store = rig.store().await;
        let session = store.begin(1).await.unwrap();
        register_host(&session, "first-process", Some(OLD_HOST)).await;
        session
            .set_node_scheduling("reused-name", true, false)
            .await
            .unwrap();
        let restarted = register_host(&session, "second-process", Some(OLD_HOST)).await;
        assert!(restarted.scheduling_paused && !restarted.node.available);
        session
            .invalidate_node("reused-name", "second-process")
            .await
            .unwrap();
        if retire {
            assert!(session
                .retire_expired_node("reused-name", "second-process", u64::MAX, 1800)
                .await
                .unwrap());
        }
        let recovered = store.begin(1).await.unwrap();
        let replacement = register_host(&recovered, "replacement-process", Some(NEW_HOST)).await;
        assert!(!replacement.scheduling_paused && replacement.node.available);
        assert_eq!(replacement.shard_id, restarted.shard_id);
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn replacement_clears_old_administrative_pause_when_upgrade_releases_its_pause() {
    use adx_coordinator::storage::{UpgradeNode, UpgradeTask};

    const OLD_HOST: &str = "11111111-1111-4111-8111-111111111111";
    const NEW_HOST: &str = "22222222-2222-4222-8222-222222222222";
    for retired in [false, true] {
        for (next_host, expected_admin_pause) in [
            (Some(OLD_HOST), true),
            (None, true),
            (Some(NEW_HOST), false),
        ] {
            let rig = common::Redis::new().await;
            let session = rig.store().await.begin(1).await.unwrap();
            register_host(&session, "previous", Some(OLD_HOST)).await;
            session
                .set_node_scheduling("reused-name", true, false)
                .await
                .unwrap();
            session
                .invalidate_node("reused-name", "previous")
                .await
                .unwrap();
            if retired {
                assert!(session
                    .retire_expired_node("reused-name", "previous", u64::MAX, 1800)
                    .await
                    .unwrap());
            }
            session
                .save_upgrade(
                    UpgradeTask {
                        protocol_version: 3,
                        task_id: "upgrade".into(),
                        epoch: 1,
                        namespace: "test".into(),
                        daemonset_uid: "daemonset".into(),
                        target_image: format!("node@sha256:{}", "a".repeat(64)),
                        phase: "Active".into(),
                        nodes: [(
                            "reused-name".into(),
                            UpgradeNode {
                                node_id: "reused-name".into(),
                                pod_uid: "previous-pod".into(),
                                node_session_id: "previous".into(),
                                phase: "evacuating".into(),
                                ..Default::default()
                            },
                        )]
                        .into(),
                        ..Default::default()
                    },
                    None,
                )
                .await
                .unwrap();
            if !retired {
                session
                    .set_upgrade_node_scheduling("upgrade", 1, "reused-name", true)
                    .await
                    .unwrap();
            }
            let next = register_host(&session, "next", next_host).await;
            assert!(next.scheduling_paused && !next.node.available);
            assert_eq!(
                next.upgrade_pause.unwrap().original_paused,
                expected_admin_pause,
                "retired={retired}, next_host={next_host:?}",
            );
            let released = session
                .set_upgrade_node_scheduling("upgrade", 1, "reused-name", false)
                .await
                .unwrap();
            assert!(released.upgrade_pause.is_none());
            assert_eq!(released.scheduling_paused, expected_admin_pause);
            assert_eq!(released.node.available, !expected_admin_pause);
        }
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_scheduling_pause_preserves_unknown_host_identity() {
    const HOST: &str = "22222222-2222-4222-8222-222222222222";
    for (previous_uuid, reported_uuid) in [
        (None, Some(HOST)),
        (Some(HOST), None),
        (Some(HOST), Some("invalid")),
    ] {
        let rig = common::Redis::new().await;
        let session = rig.store().await.begin(1).await.unwrap();
        register_host(&session, "previous", previous_uuid).await;
        session
            .set_node_scheduling("reused-name", true, false)
            .await
            .unwrap();
        let next = register_host(&session, "next", reported_uuid).await;
        assert!(next.scheduling_paused && !next.node.available);
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_retirement_preserves_administrative_policy_and_heartbeat_fencing() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(3).await.unwrap();
    registered(&session, "boot-one").await;
    session
        .set_node_scheduling("old", true, false)
        .await
        .unwrap();
    let shard = session.snapshot().await.unwrap().nodes["old"].shard_id;
    session.invalidate_node("old", "boot-one").await.unwrap();
    assert!(session
        .retire_expired_node("old", "boot-one", u64::MAX, 1800)
        .await
        .unwrap());
    let restarted = store.begin(3).await.unwrap();
    let stale = restarted
        .register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: "boot-one".into(),
                sequence: 7,
                routable: false,
            }),
        )
        .await;
    assert!(matches!(stale, Err(Error::Conflict)));
    let restored = restarted
        .register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: "boot-one".into(),
                sequence: 8,
                routable: false,
            }),
        )
        .await
        .unwrap();
    assert_eq!(restored.shard_id, shard);
    assert!(restored.scheduling_paused && !restored.node.available);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_retirement_waits_for_environment_reference_cleanup() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    registered(&session, "boot-one").await;
    let assignment = Assignment {
        environment_id: "held".into(),
        node_id: "old".into(),
        shard_id: 0,
        generation: session.epoch() << 32 | 1,
        devices: vec![],
    };
    session
        .reserve(spec("held"), assignment.clone())
        .await
        .unwrap();
    session
        .commit(running(spec("held"), assignment))
        .await
        .unwrap();
    session.invalidate_node("old", "boot-one").await.unwrap();
    assert!(!session
        .retire_expired_node("old", "boot-one", u64::MAX, 1800)
        .await
        .unwrap());
    let failed = session.get("held").await.unwrap();
    session
        .retire_invalidated_failed(&failed, u64::MAX)
        .await
        .unwrap();
    assert!(session
        .retire_expired_node("old", "boot-one", u64::MAX, 1800)
        .await
        .unwrap());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_retirement_racing_a_new_registration_preserves_the_replacement() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    registered(&session, "boot-one").await;
    session.invalidate_node("old", "boot-one").await.unwrap();
    let (retired, new) = tokio::join!(
        session.retire_expired_node("old", "boot-one", u64::MAX, 1800),
        session.register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: "boot-two".into(),
                sequence: 1,
                routable: false
            })
        )
    );
    retired.unwrap();
    new.unwrap();
    let saved = session.snapshot().await.unwrap();
    assert_eq!(saved.nodes["old"].session.as_ref().unwrap().id, "boot-two");
    assert!(!session
        .retire_expired_node("old", "boot-one", u64::MAX, 1800)
        .await
        .unwrap());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn planned_node_departure_is_not_an_incident_and_old_boot_cannot_return() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    registered(&session, "boot-one").await;
    session
        .invalidate_node_with_departure("old", "boot-one", true)
        .await
        .unwrap();
    assert!(session
        .retire_expired_node("old", "boot-one", 0, 1800)
        .await
        .unwrap());
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        0
    );
    let restarted = store.begin(1).await.unwrap();
    assert!(restarted
        .is_unregistered_node("old", "boot-one")
        .await
        .unwrap());
    let late = restarted
        .register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: "boot-one".into(),
                sequence: 100,
                routable: false,
            }),
        )
        .await;
    assert!(matches!(late, Err(Error::Conflict)));
    registered(&restarted, "boot-two").await;
    assert!(!restarted
        .is_unregistered_node("old", "boot-one")
        .await
        .unwrap());
    assert!(matches!(
        session
            .retire_expired_node("old", "boot-two", u64::MAX, 1800)
            .await,
        Err(Error::Conflict)
    ));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_retirement_new_boot_readiness_closes_the_previous_departure_durably() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    registered(&session, "boot-one").await;
    session
        .invalidate_node_with_departure("old", "boot-one", true)
        .await
        .unwrap();
    for (sequence, routable) in [(1, false), (2, true)] {
        session
            .register_session(
                node("old"),
                "127.0.0.1:9000".into(),
                "127.0.0.1:9001".into(),
                Some(NodeSession {
                    id: "boot-two".into(),
                    sequence,
                    routable,
                }),
            )
            .await
            .unwrap();
    }
    session.invalidate_node("old", "boot-two").await.unwrap();
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        1
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn retired_node_rejoins_with_current_checkpoint_scopes_and_reconciliation_fence() {
    use std::collections::BTreeSet;

    for initially_routable in [false, true] {
        let rig = common::Redis::new().await;
        let store = rig.store().await;
        let session = store.begin(1).await.unwrap();
        session
            .register_session_with_scopes(
                node("old"),
                "127.0.0.1:9000".into(),
                "127.0.0.1:9001".into(),
                Some(NodeSession {
                    id: "boot-one".into(),
                    sequence: 7,
                    routable: true,
                }),
                BTreeSet::from(["old-storage".into()]),
            )
            .await
            .unwrap();
        let invalidated = session.invalidate_node("old", "boot-one").await.unwrap();
        assert!(invalidated.nodes["old"].checkpoint_gc_fenced);
        assert!(session
            .retire_expired_node("old", "boot-one", u64::MAX, 1800)
            .await
            .unwrap());

        let active = store.begin(1).await.unwrap();
        let advertised = BTreeSet::from(["current-storage".into(), "second-storage".into()]);
        let restored = active
            .register_session_with_scopes(
                node("old"),
                "127.0.0.1:9000".into(),
                "127.0.0.1:9001".into(),
                Some(NodeSession {
                    id: "boot-two".into(),
                    sequence: 1,
                    routable: initially_routable,
                }),
                advertised.clone(),
            )
            .await
            .unwrap();
        assert_eq!(restored.checkpoint_scopes, advertised);
        assert_eq!(restored.checkpoint_gc_fenced, !initially_routable);
        let persisted = store.begin(1).await.unwrap();
        let saved = persisted.snapshot().await.unwrap();
        assert_eq!(saved.nodes["old"].checkpoint_scopes, advertised);
        assert_eq!(saved.nodes["old"].checkpoint_gc_fenced, !initially_routable);

        let ready = persisted
            .register_session_with_scopes(
                node("old"),
                "127.0.0.1:9000".into(),
                "127.0.0.1:9001".into(),
                Some(NodeSession {
                    id: "boot-two".into(),
                    sequence: 2,
                    routable: true,
                }),
                advertised.clone(),
            )
            .await
            .unwrap();
        assert_eq!(ready.checkpoint_scopes, advertised);
        assert!(!ready.checkpoint_gc_fenced);
    }
}
