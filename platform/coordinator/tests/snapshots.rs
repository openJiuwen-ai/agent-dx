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
            sandbox: Default::default(),
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
async fn snapshot_deletion_waits_for_references_and_survives_coordinator_restart() {
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
    let deleting = first.delete_snapshot(&snapshot.id, "tenant").await.unwrap();
    assert_eq!(deleting.state, SnapshotState::Deleting);
    assert!(!deleting.collectable());
    assert!(first
        .acquire_snapshot(
            &snapshot.id,
            "tenant",
            Reference::Restore {
                environment_id: "another".into()
            }
        )
        .await
        .is_err());
    assert!(first
        .finish_snapshot_deletion(&snapshot.id, deleting.revision)
        .await
        .is_err());
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
    assert!(ready.collectable());
    assert_eq!(
        second
            .release_snapshot(&snapshot.id, reference)
            .await
            .unwrap(),
        ready
    );
    second
        .finish_snapshot_deletion(&snapshot.id, ready.revision)
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
