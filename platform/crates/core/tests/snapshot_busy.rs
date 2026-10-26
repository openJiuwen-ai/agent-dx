use adx_core::snapshots::{Reference, Snapshot, SnapshotState};
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

#[test]
fn snapshot_source_replay_accepts_omitted_uid_but_rejects_different_mount_identity() {
    use adx_core::{
        execution::{Mount, MountSource},
        volumes::VolumeRef,
    };
    let mut saved = snapshot();
    saved.template.execution.mounts.push(Mount {
        kind: "bind".into(),
        target: "/workspace".into(),
        options: vec!["rw".into()],
        source: MountSource::Volume(VolumeRef {
            id: "workspace".into(),
            uid: Some("fixed-uid".into()),
        }),
        create_if_missing: false,
        create_options: None,
    });
    let mut request = saved.clone();
    let MountSource::Volume(reference) = &mut request.template.execution.mounts[0].source else {
        unreachable!()
    };
    reference.uid = None;
    assert!(saved.same_content(&request));
    assert!(saved.matches_template(&request.template));
    let MountSource::Volume(reference) = &mut request.template.execution.mounts[0].source else {
        unreachable!()
    };
    reference.uid = Some("recreated-uid".into());
    assert!(!saved.same_content(&request));
    request.template.execution.mounts.clear();
    assert!(!saved.same_content(&request));
    assert!(!saved.matches_template(&request.template));
}
