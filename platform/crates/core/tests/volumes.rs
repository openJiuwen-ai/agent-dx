use adx_core::{
    execution::{CreateOptions, Mount, MountSource, StorageSource},
    volumes::{AccessMode, Occupancy, OccupancyPhase, VolumeRef, VolumeSpec},
    Assignment,
};
use serde_json::json;

fn definition() -> serde_json::Value {
    json!({
        "tenant_id": "tenant-a",
        "id": "workspace",
        "source": {
            "type": "directory",
            "storage_domain": "sfs-main",
            "path": "workspaces/user-42"
        }
    })
}

#[test]
fn omitted_access_mode_is_shared_and_definition_is_normalized() {
    let implicit: VolumeSpec = serde_json::from_value(definition()).unwrap();
    implicit.validate().unwrap();
    assert_eq!(implicit.access_mode, AccessMode::Shared);
    let explicit: VolumeSpec = serde_json::from_value({
        let mut value = definition();
        value["access_mode"] = json!("shared");
        value
    })
    .unwrap();
    assert_eq!(implicit, explicit);
}

#[test]
fn volume_directory_is_relative_and_creation_attributes_are_explicit() {
    for path in [
        "",
        "/workspaces/user",
        "../user",
        "a/../user",
        "a//user",
        "a/./user",
        "a\\user",
    ] {
        let mut value = definition();
        value["source"]["path"] = json!(path);
        let spec: VolumeSpec = serde_json::from_value(value).unwrap();
        assert!(spec.validate().is_err(), "accepted {path:?}");
    }
    let mut spec: VolumeSpec = serde_json::from_value(definition()).unwrap();
    spec.create_options = Some(CreateOptions {
        uid: 10001,
        gid: 10001,
        mode: "0700".into(),
    });
    assert!(spec.validate().is_err());
    spec.create_if_missing = true;
    spec.validate().unwrap();
}

#[test]
fn mount_reference_excludes_inline_source_and_initialization() {
    let mut mount = Mount {
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
    mount.validate().unwrap();
    mount.create_if_missing = true;
    assert!(mount.validate().is_err());
    let mut value = serde_json::to_value(&mount).unwrap();
    value["source"]["Local"] = json!("/mnt/sfs/workspaces/user");
    assert!(serde_json::from_value::<Mount>(value).is_err());
}

#[test]
fn inline_sources_and_rootfs_still_use_existing_storage_types() {
    for source in [
        StorageSource::Image("registry/models:v1".into()),
        StorageSource::Local("/data".into()),
    ] {
        let mount = Mount {
            kind: "bind".into(),
            target: "/data".into(),
            options: vec!["ro".into()],
            source: MountSource::Inline(source),
            create_if_missing: false,
            create_options: None,
        };
        mount.validate().unwrap();
        assert_eq!(
            serde_json::from_value::<Mount>(serde_json::to_value(&mount).unwrap()).unwrap(),
            mount
        );
    }
}

#[test]
fn occupancy_identifies_a_specific_runtime_within_an_assignment() {
    let mut occupancy = Occupancy {
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
    occupancy.validate().unwrap();
    occupancy.runtime_id = "env-6".into();
    assert!(occupancy.validate().is_err());
    occupancy.assignment.generation = 0;
    occupancy.runtime_id = "env-0".into();
    assert!(occupancy.validate().is_err());
}

#[test]
fn volume_ids_use_the_same_url_safe_ascii_contract_for_definitions_and_references() {
    for id in ["workspace.A_0-z".to_owned(), "a".repeat(256)] {
        let mut spec: VolumeSpec = serde_json::from_value(definition()).unwrap();
        spec.id = id.clone();
        spec.validate().unwrap();
        VolumeRef { id, uid: None }.validate().unwrap();
    }
    for id in [
        "",
        "a/b",
        "a\\b",
        "$id",
        "{id}",
        "a b",
        "a%2Fb",
        "中文",
        "a?b",
        "a#b",
        ".",
        "..",
        &"a".repeat(257),
    ] {
        let mut spec: VolumeSpec = serde_json::from_value(definition()).unwrap();
        spec.id = id.into();
        assert!(spec.validate().is_err(), "accepted definition ID {id:?}");
        assert!(
            VolumeRef {
                id: id.into(),
                uid: None
            }
            .validate()
            .is_err(),
            "accepted reference ID {id:?}"
        );
    }
}

#[test]
fn dynamic_storage_domain_can_exist_before_any_node_registers() {
    let value = json!({
        "backend_id": "server:/export",
        "dynamic_nodes": true,
        "node_roots": {},
        "tenant_paths": {"tenant": "workspaces/tenant"}
    });
    let domain: adx_core::volumes::StorageDomain = serde_json::from_value(value).unwrap();
    domain.validate().unwrap();
    let fixed: adx_core::volumes::StorageDomain = serde_json::from_value(json!({
        "backend_id": "server:/export", "node_roots": {}, "tenant_paths": {}
    }))
    .unwrap();
    assert!(fixed.validate().is_err());
}
