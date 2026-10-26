use adx_core::{
    execution::{Mount, MountSource},
    volumes::{AccessMode, VolumeRef, VolumeSpec},
};
use adx_protocol::control;

#[test]
fn volume_definition_roundtrip_preserves_defaults_and_creation_rules() {
    let spec: VolumeSpec = serde_json::from_value(serde_json::json!({
        "tenant_id": "tenant", "id": "workspace",
        "source": {"type": "directory", "storage_domain": "sfs", "path": "workspaces/user"}
    }))
    .unwrap();
    let wire: control::VolumeSpec = spec.clone().into();
    assert_eq!(VolumeSpec::try_from(wire).unwrap(), spec);
    assert_eq!(spec.access_mode, AccessMode::Shared);
}

#[test]
fn referenced_mount_roundtrip_keeps_fixed_uid() {
    let mount = Mount {
        kind: "bind".into(),
        target: "/home/jiuwen".into(),
        options: vec!["rw".into()],
        source: MountSource::Volume(VolumeRef {
            id: "workspace".into(),
            uid: Some("uid-1".into()),
        }),
        create_if_missing: false,
        create_options: None,
    };
    let wire: control::MountSpec = mount.clone().into();
    assert_eq!(Mount::try_from(wire).unwrap(), mount);
}

#[test]
fn unrecognized_access_modes_and_missing_mount_sources_are_rejected() {
    let wire = control::VolumeSpec {
        tenant_id: "tenant".into(),
        id: "workspace".into(),
        access_mode: 99,
        source: Some(control::volume_spec::Source::Directory(
            control::DirectoryVolumeSource {
                storage_domain: "sfs".into(),
                path: "workspaces/user".into(),
            },
        )),
        ..Default::default()
    };
    assert!(VolumeSpec::try_from(wire).is_err());
    assert!(Mount::try_from(control::MountSpec::default()).is_err());
}

#[test]
fn occupancy_roundtrip_preserves_the_specific_execution_owner() {
    use adx_core::{
        volumes::{Occupancy, OccupancyPhase},
        Assignment,
    };
    let occupancy = Occupancy {
        volume_uid: "uid-1".into(),
        assignment: Assignment {
            environment_id: "env".into(),
            node_id: "node".into(),
            shard_id: 0,
            generation: 7,
            devices: vec![],
        },
        runtime_id: "env-7-r4".into(),
        node_session_id: "session".into(),
        phase: OccupancyPhase::Reserved,
    };
    let wire: control::VolumeOccupancy = occupancy.clone().try_into().unwrap();
    assert_eq!(Occupancy::try_from(wire).unwrap(), occupancy);
}

#[test]
fn volume_busy_remains_distinct_from_compute_capacity_across_rpc() {
    let expected = adx_core::Error::VolumeBusy("workspace-uid".into());
    assert_eq!(
        adx_protocol::dependency_status(adx_protocol::status(expected.clone())),
        expected
    );
}

#[test]
fn storage_mount_reports_validate_roots_before_use() {
    let mount = adx_core::volumes::StorageMount {
        backend_id: "server:/export".into(),
        root: "/mnt/sfs".into(),
    };
    let wire: adx_protocol::control::StorageMount = mount.clone().into();
    assert_eq!(
        adx_core::volumes::StorageMount::try_from(wire).unwrap(),
        mount
    );
    for root in ["relative", "/mnt/../sfs", "/mnt/./sfs", "/mnt/\0sfs"] {
        let wire = adx_protocol::control::StorageMount {
            backend_id: "server:/export".into(),
            root: root.into(),
        };
        assert!(adx_core::volumes::StorageMount::try_from(wire).is_err());
    }
}
