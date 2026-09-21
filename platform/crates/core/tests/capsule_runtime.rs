use adx_core::{CapsuleRecord, CapsuleSpec, CapsuleState, Runtime};

#[test]
fn capsule_identity_survives_runtime_replacement() {
    fn replace_runtime(record: &mut CapsuleRecord, runtime: Runtime) {
        record.runtime = runtime;
    }

    let mut record = CapsuleRecord {
        spec: CapsuleSpec {
            id: "capsule-a".into(),
            tenant_id: "tenant-a".into(),
            image: "image-a".into(),
            runtime_class: "firecracker".into(),
            environment: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            resources: adx_core::Resources {
                cpu_millis: 100,
                memory_bytes: 1024,
                disk_bytes: 0,
            },
            priority: 0,
            scheduling: Default::default(),
            sandbox: Default::default(),
        },
        assignment: adx_core::Assignment {
            capsule_id: "capsule-a".into(),
            node_id: "node-a".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        },
        state: CapsuleState::Running,
        revision: 1,
        runtime: Runtime {
            id: "capsule-a-1".into(),
            ip: Some("10.0.0.1".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: None,
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
    };

    replace_runtime(
        &mut record,
        Runtime {
            id: "capsule-a-2".into(),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
    );

    assert_eq!(record.spec.id, "capsule-a");
    assert_eq!(record.runtime.id, "capsule-a-2");
    assert_eq!(record.runtime.ip.unwrap().to_string(), "10.0.0.2");
}
