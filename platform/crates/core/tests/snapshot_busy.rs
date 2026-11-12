use adx_core::snapshots::{Reference, Snapshot, SnapshotState};
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
            scope: String::new(),

            storage: "shared".into(),
            location: "artifact-1".into(),
            size_bytes: 100,
        },
    )
    .unwrap()
}

#[test]
fn golden_capture_drops_source_route_credentials_without_changing_regular_captures() {
    use adx_core::{environment_control::EnvironmentCompatibility, snapshots::SnapshotKind};
    let mut source = snapshot();
    source.template.compatibility = Some(EnvironmentCompatibility {
        template_id: "family".into(),
        envd_access_token: "a".repeat(32),
        traffic_access_token: "b".repeat(32),
        allow_public_traffic: false,
    });
    for kind in [SnapshotKind::Regular, SnapshotKind::Golden] {
        let capture = Snapshot::new_with_kind(
            kind,
            source.id.clone(),
            source.names.clone(),
            source.template.clone(),
            source.source_node_id.clone(),
            source.source_runtime_id.clone(),
            source.artifact.clone(),
        )
        .unwrap();
        assert_eq!(
            capture.template.compatibility.is_none(),
            kind == SnapshotKind::Golden
        );
        assert!(capture.matches_template(&source.template));
        if kind == SnapshotKind::Golden {
            let mut legacy = capture.clone();
            legacy.template.compatibility = source.template.compatibility.clone();
            assert!(!capture.same_content(&legacy));
            assert!(!legacy.same_content(&capture));
        }
        let mut changed = source.template.clone();
        changed.image = "another-image".into();
        assert!(!capture.matches_template(&changed));
    }
    // Reading an older Golden retains its fixed source identity and strict comparison.
    source.kind = SnapshotKind::Golden;
    let mut changed = source.template.clone();
    changed.compatibility.as_mut().unwrap().traffic_access_token = "c".repeat(32);
    assert!(!source.matches_template(&changed));
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
    assert!(request.template.matches_fixed_spec(&saved.template));
    let unresolved = request.clone();
    unresolved.validate().unwrap();
    assert!(!unresolved.same_content(&unresolved));
    assert!(!unresolved.matches_template(&unresolved.template));
    assert!(!unresolved.template.matches_fixed_spec(&unresolved.template));
    let MountSource::Volume(reference) = &mut request.template.execution.mounts[0].source else {
        unreachable!()
    };
    reference.uid = Some("recreated-uid".into());
    assert!(!saved.same_content(&request));
    assert!(!request.template.matches_fixed_spec(&saved.template));
    request.template.execution.mounts.clear();
    assert!(!saved.same_content(&request));
    assert!(!saved.matches_template(&request.template));
}

#[test]
fn capture_identity_preserves_existing_algorithm_and_scopes_each_input() {
    use adx_core::snapshots::snapshot_id;
    let id = snapshot_id("tenant", "environment", "operation").unwrap();
    assert_eq!(
        id,
        "snapshot-77ef1426cfeabb67ace325518d616e49ec845e50aa98938d6971b473f689c308"
    );
    assert_ne!(
        id,
        snapshot_id("other", "environment", "operation").unwrap()
    );
    assert_ne!(id, snapshot_id("tenant", "other", "operation").unwrap());
    assert_ne!(id, snapshot_id("tenant", "environment", "other").unwrap());
}

#[test]
fn snapshot_kind_and_capture_time_are_immutable_content() {
    use adx_core::snapshots::SnapshotKind;
    let saved = snapshot();
    assert_eq!(saved.kind, SnapshotKind::Regular);
    assert!(saved.created_at_unix_ms > 0);
    let mut changed = saved.clone();
    changed.kind = SnapshotKind::Golden;
    assert!(!saved.same_content(&changed));
    changed.kind = saved.kind;
    changed.created_at_unix_ms += 1;
    assert!(!saved.same_content(&changed));
    let mut golden = saved.clone();
    golden.kind = SnapshotKind::Golden;
    let created_at = golden.created_at_unix_ms;
    golden
        .acquire(
            "tenant",
            Reference::Restore {
                environment_id: "clone".into(),
            },
        )
        .unwrap();
    golden
        .release(&Reference::Restore {
            environment_id: "clone".into(),
        })
        .unwrap();
    golden.delete("tenant").unwrap();
    assert_eq!(golden.kind, SnapshotKind::Golden);
    assert_eq!(golden.created_at_unix_ms, created_at);
}

#[test]
fn legacy_snapshot_metadata_defaults_to_regular_with_unknown_capture_time() {
    use adx_core::snapshots::SnapshotKind;
    let mut raw = serde_json::to_value(snapshot()).unwrap();
    raw.as_object_mut().unwrap().remove("kind");
    raw.as_object_mut().unwrap().remove("created_at_unix_ms");
    let legacy: Snapshot = serde_json::from_value(raw.clone()).unwrap();
    assert_eq!(legacy.kind, SnapshotKind::Regular);
    assert_eq!(legacy.created_at_unix_ms, 0);
    legacy.validate().unwrap();
    raw["kind"] = serde_json::json!("unknown");
    assert!(serde_json::from_value::<Snapshot>(raw).is_err());
}
