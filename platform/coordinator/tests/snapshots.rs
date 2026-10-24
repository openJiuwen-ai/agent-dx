mod common;
use adx_coordinator::snapshots::{Reference, Snapshot, SnapshotState};
use adx_core::{CheckpointArtifact, EnvironmentSpec, Resources};

fn snapshot() -> Snapshot {
    Snapshot::new(
        "snapshot-1".into(),
        vec!["base".into()],
        EnvironmentSpec {
            runtime_profile: None,
            snapshot_id: None,
            id: "source".into(),
            tenant_id: "tenant".into(),
            image: "image".into(),
            runtime_class: "firecracker".into(),
            resources: Resources {
                cpu_millis: 100,
                memory_bytes: 128,
                disk_bytes: 0,
            },
            priority: 0,
            scheduling: Default::default(),
            env: Default::default(),
            lifecycle: Default::default(),
            execution: Default::default(),
        },
        "node1".into(),
        "source-1".into(),
        CheckpointArtifact {
            storage: "shared".into(),
            location: "artifact-1".into(),
            size_bytes: 100,
        },
    )
    .unwrap()
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn snapshot_busy_delete_is_rejected_and_survives_coordinator_restart() {
    let mut rig = common::Redis::new().await;
    let db = rig.store().await;
    let first = db.begin(1).await.unwrap();
    let snapshot = snapshot();
    first.publish_snapshot(snapshot.clone()).await.unwrap();
    let reference = Reference::Template {
        node_id: "node1".into(),
        template_id: "base".into(),
    };
    first
        .acquire_snapshot(
            &snapshot.id,
            &snapshot.template.tenant_id,
            reference.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(
        first.delete_snapshot(&snapshot.id, "tenant").await,
        Err(adx_core::Error::SnapshotInUse { .. })
    ));
    assert_eq!(
        first.get_snapshot(&snapshot.id).await.unwrap().state,
        SnapshotState::Ready
    );
    rig.crash();
    rig.start().await;
    let second = rig.store().await.begin(1).await.unwrap();
    assert!(first
        .release_snapshot(&snapshot.id, reference.clone())
        .await
        .is_err());
    let ready = second
        .release_snapshot(&snapshot.id, reference.clone())
        .await
        .unwrap();
    assert_eq!(ready.state, SnapshotState::Ready);
    assert_eq!(
        second
            .release_snapshot(&snapshot.id, reference)
            .await
            .unwrap(),
        ready
    );
    let deleting = second
        .delete_snapshot(&snapshot.id, "tenant")
        .await
        .unwrap();
    second
        .finish_snapshot_deletion(&snapshot.id, deleting.revision)
        .await
        .unwrap();
    assert_eq!(
        second.get_snapshot(&snapshot.id).await.unwrap().state,
        SnapshotState::Deleted
    );
    assert!(second
        .acquire_snapshot(
            &snapshot.id,
            "tenant",
            Reference::Restore {
                environment_id: "late".into()
            }
        )
        .await
        .is_err());
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn snapshot_identity_is_immutable_and_tenant_checked() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let first = snapshot();
    session.publish_snapshot(first.clone()).await.unwrap();
    session.publish_snapshot(first.clone()).await.unwrap();
    let mut conflict = first.clone();
    conflict.artifact.location = "different".into();
    assert!(session.publish_snapshot(conflict).await.is_err());
    assert!(session
        .acquire_snapshot(
            &first.id,
            "another",
            Reference::Restore {
                environment_id: "a".into()
            }
        )
        .await
        .is_err());
    assert!(session.delete_snapshot(&first.id, "another").await.is_err());
    let records = session.list_snapshots("tenant").await.unwrap();
    assert_eq!(records, vec![first]);
    assert!(session.list_snapshots("another").await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn node_snapshot_catalog_retains_deleting_until_physical_deletion() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let first = snapshot();
    session.publish_snapshot(first.clone()).await.unwrap();
    assert_eq!(
        session.node_snapshots("node1").await.unwrap(),
        vec![first.clone()]
    );
    assert!(session.node_snapshots("other").await.unwrap().is_empty());
    let deleting = session.delete_snapshot(&first.id, "tenant").await.unwrap();
    assert!(session.list_snapshots("tenant").await.unwrap().is_empty());
    assert_eq!(
        session.node_snapshots("node1").await.unwrap(),
        vec![deleting.clone()]
    );
    session
        .finish_snapshot_deletion(&first.id, deleting.revision)
        .await
        .unwrap();
    assert!(session.node_snapshots("node1").await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn acquire_and_delete_race_has_one_winner_and_deletion_fences_republication() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    for index in 0..24 {
        let mut s = snapshot();
        s.id = format!("snapshot-{index}");
        s.artifact.location = uuid::Uuid::new_v4().to_string();
        session.publish_snapshot(s.clone()).await.unwrap();
        let reference = Reference::Restore {
            environment_id: format!("sandbox-{index}"),
        };
        let (acquired, deleted) = tokio::join!(
            session.acquire_snapshot(&s.id, "tenant", reference.clone()),
            session.delete_snapshot(&s.id, "tenant")
        );
        match (acquired, deleted) {
            (Ok(_), Err(adx_core::Error::SnapshotInUse { sandbox_ids, .. })) => {
                assert_eq!(sandbox_ids, vec![format!("sandbox-{index}")]);
                assert_eq!(
                    session.get_snapshot(&s.id).await.unwrap().state,
                    SnapshotState::Ready
                );
                session.release_snapshot(&s.id, reference).await.unwrap();
                session.delete_snapshot(&s.id, "tenant").await.unwrap();
            }
            (Err(_), Ok(deleted)) => assert_eq!(deleted.state, SnapshotState::Deleting),
            pair => panic!("unexpected race result: {pair:?}"),
        }
        let _deletion = session
            .claim_artifact_deletion(&s.artifact, Some(&s.id))
            .await
            .unwrap();
        let mut alias = s.clone();
        alias.id = format!("alias-{index}");
        assert!(
            session.publish_snapshot(alias).await.is_err(),
            "a reclaiming identity cannot gain a new owner"
        );
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn committed_snapshot_changes_update_inspection_cache() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    session.refresh_inspection_inventory().await.unwrap();
    let record = snapshot();
    session.publish_snapshot(record.clone()).await.unwrap();
    let reference = Reference::Template {
        node_id: "node1".into(),
        template_id: "cache".into(),
    };
    let updated = session
        .acquire_snapshot(&record.id, "tenant", reference)
        .await
        .unwrap();
    rig.crash();
    let (snapshots, holds) = session.inspection_inventory().await.unwrap();
    assert_eq!(snapshots, vec![updated]);
    assert!(holds.is_empty());
}
