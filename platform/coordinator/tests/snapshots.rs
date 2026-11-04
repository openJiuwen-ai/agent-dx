mod common;
use adx_coordinator::snapshots::{Reference, Snapshot, SnapshotState};
use adx_core::{CheckpointArtifact, EnvironmentSpec, Resources};

fn snapshot() -> Snapshot {
    Snapshot::new(
        "snapshot-1".into(),
        vec!["base".into()],
        EnvironmentSpec {
            envd: None,
            run_policy: None,
            compatibility: None,
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
            scope: String::new(),
            location: "artifact-1".into(),
            size_bytes: 100,
        },
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires dedicated real Redis"]
async fn independent_snapshot_references_survive_concurrent_header_updates() {
    const COUNT: usize = 512;
    let rig = common::Redis::new().await;
    let session = std::sync::Arc::new(rig.store().await.begin(1).await.unwrap());
    let mut ids = vec![];
    for index in 0..COUNT {
        let mut value = snapshot();
        value.id = format!("independent-{index}");
        value.names = vec![value.id.clone()];
        value.artifact.location = uuid::Uuid::new_v4().to_string();
        session.publish_snapshot(value.clone()).await.unwrap();
        ids.push(value.id);
    }
    let gate = std::sync::Arc::new(tokio::sync::Barrier::new(COUNT));
    let mut operations = tokio::task::JoinSet::new();
    for (index, id) in ids.iter().enumerate() {
        let session = session.clone();
        let gate = gate.clone();
        let id = id.clone();
        operations.spawn(async move {
            gate.wait().await;
            session
                .acquire_snapshot(
                    &id,
                    "tenant",
                    Reference::Restore {
                        environment_id: format!("restore-{index}"),
                    },
                )
                .await
        });
    }
    let mut errors = vec![];
    while let Some(result) = operations.join_next().await {
        if let Err(error) = result.unwrap() {
            errors.push(error);
        }
    }
    assert!(
        errors.is_empty(),
        "Independent snapshot acquisitions failed: {errors:?}"
    );
    for (index, id) in ids.iter().enumerate() {
        let value = session.get_snapshot(id).await.unwrap();
        assert_eq!(value.state, SnapshotState::Ready);
        assert_eq!(value.references.len(), 1);
        assert!(value.references.contains(&Reference::Restore {
            environment_id: format!("restore-{index}"),
        }));
    }
    let gate = std::sync::Arc::new(tokio::sync::Barrier::new(COUNT));
    let mut operations = tokio::task::JoinSet::new();
    for (index, id) in ids.into_iter().enumerate() {
        let session = session.clone();
        let gate = gate.clone();
        operations.spawn(async move {
            gate.wait().await;
            session
                .release_snapshot(
                    &id,
                    Reference::Restore {
                        environment_id: format!("restore-{index}"),
                    },
                )
                .await
        });
    }
    while let Some(result) = operations.join_next().await {
        let value = result.unwrap().unwrap();
        assert!(value.references.is_empty());
        assert_eq!(value.state, SnapshotState::Ready);
    }
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn snapshot_busy_delete_is_rejected_and_survives_coordinator_restart() {
    let mut rig = common::Redis::new().await;
    let db = rig.store().await;
    let first = db.begin(1).await.unwrap();
    let mut snapshot = snapshot();
    snapshot.kind = adx_core::snapshots::SnapshotKind::Golden;
    let created_at = snapshot.created_at_unix_ms;
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
    let restored = second.get_snapshot(&snapshot.id).await.unwrap();
    assert_eq!(restored.kind, adx_core::snapshots::SnapshotKind::Golden);
    assert_eq!(restored.created_at_unix_ms, created_at);
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
    conflict.kind = adx_core::snapshots::SnapshotKind::Golden;
    assert!(session.publish_snapshot(conflict).await.is_err());
    let mut conflict = first.clone();
    conflict.created_at_unix_ms += 1;
    assert!(session.publish_snapshot(conflict).await.is_err());
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
