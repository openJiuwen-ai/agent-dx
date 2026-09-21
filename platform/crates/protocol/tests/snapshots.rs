use adx_core::{
    snapshots::{Reference, Snapshot},
    CheckpointArtifact, InstanceSpec, Resources,
};
#[test]
fn snapshot_wire_preserves_template_and_durable_references() {
    let spec = InstanceSpec {
        runtime_environment: None,
        snapshot_id: None,
        id: "source".into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime: "firecracker".into(),
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
    };
    let mut record = Snapshot::new(
        "snapshot-1".into(),
        vec!["base".into()],
        spec,
        "node1".into(),
        "source-1".into(),
        CheckpointArtifact {
            storage: "shared".into(),
            location: "artifact".into(),
            size_bytes: 512,
        },
    )
    .unwrap();
    record
        .acquire(
            "tenant",
            Reference::Template {
                node_id: "node1".into(),
                template_id: "base".into(),
            },
        )
        .unwrap();
    record.delete("tenant").unwrap();
    let wire: adx_protocol::control::ReusableSnapshot = record.clone().try_into().unwrap();
    assert_eq!(Snapshot::try_from(wire).unwrap(), record);
}
