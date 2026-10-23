use adx_core::snapshots::{Reference, Snapshot, SnapshotState};
use adx_core::{CheckpointArtifact, EnvironmentSpec, Resources};
fn snapshot() -> Snapshot {
    Snapshot::new(
        "snapshot-1".into(),
        vec!["base".into()],
        EnvironmentSpec {
            run_policy: None,
            template_identity: None,
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
            scope: String::new(),

            storage: "shared".into(),
            location: "artifact-1".into(),
            size_bytes: 100,
        },
    )
    .unwrap()
}

#[test]
fn busy_delete_reports_users_and_keeps_ready_revision() {
    let mut snapshot = snapshot();
    for id in ["sandbox-b", "sandbox-a"] {
        snapshot
            .acquire(
                "tenant",
                Reference::Restore {
                    environment_id: id.into(),
                },
            )
            .unwrap();
    }
    let before = snapshot.clone();
    let error = snapshot.delete("tenant");
    assert_eq!(
        error,
        Err(adx_core::Error::SnapshotInUse {
            sandbox_ids: vec!["sandbox-a".into(), "sandbox-b".into()],
            template_ids: vec![]
        })
    );
    assert_eq!(
        snapshot, before,
        "a rejected delete must not close new users"
    );
}

#[test]
fn unreferenced_delete_closes_new_users() {
    let mut snapshot = snapshot();
    snapshot.delete("tenant").unwrap();
    assert_eq!(snapshot.state, SnapshotState::Deleting);
    assert!(snapshot
        .acquire(
            "tenant",
            Reference::Restore {
                environment_id: "late".into()
            }
        )
        .is_err());
}
