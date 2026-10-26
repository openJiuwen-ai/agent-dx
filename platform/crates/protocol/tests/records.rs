use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Resources, Runtime,
};
use adx_protocol::control;

#[test]
fn environment_options_roundtrip_preserves_backend_contract() {
    use adx_core::execution as model;

    let options = model::ExecutionOptions {
        user: Some("1001".into()),
        group: Some("1002".into()),
        cwd: Some("/workspace/task".into()),
        tunnel_http_port: Some(18766),
        rootfs: Some(model::Rootfs {
            readonly: true,
            source: model::StorageSource::S3(model::S3Source {
                endpoint: "https://s3.example".into(),
                bucket: "rootfs".into(),
                object: "application.erofs".into(),
                access_key_id: "key".into(),
                access_key_secret: "secret".into(),
            }),
        }),
        mounts: vec![model::Mount {
            create_if_missing: false,
            create_options: None,
            kind: "bind".into(),
            target: "/workspace".into(),
            options: vec!["ro".into()],
            source: model::StorageSource::Image("registry.example/tools:v1".into()).into(),
        }],
        network: Some(model::NetworkPolicy {
            traffic: Some(model::TrafficPolicy {
                ingress_default_action: model::NetworkAction::Deny,
                egress_default_action: model::NetworkAction::Allow,
                rules: vec![model::NetworkRule {
                    action: model::NetworkAction::Allow,
                    direction: model::NetworkDirection::Ingress,
                    protocol: model::NetworkProtocol::Tcp,
                    peer: Default::default(),
                    environment_port: 8080,
                    environment_port_range: None,
                    priority: 100,
                }],
                mode: model::TrafficMode::Stateful,
            }),
            dns: Some(model::DnsPolicy {
                default_action: model::NetworkAction::Allow,
                rules: vec![model::DnsRule {
                    action: model::NetworkAction::Deny,
                    pattern: "blocked.example".into(),
                }],
            }),
        }),
        data_plane: model::DataPlanePolicy {
            tunnel: model::DataPlaneSecurityMode::Tls,
            port_forward: model::DataPlaneSecurityMode::TlsToken,
        },
        ports: vec![8080],
        failover: true,
        inherit_entrypoint: false,
        process: None,
        readiness_probe: None,
        healthcheck_probe: None,
        init_timeout_seconds: 0,
        limits: model::ResourceLimits {
            cpu_millis: 1500,
            memory_bytes: 2 << 30,
            disk_bytes: 4 << 30,
        },
        extra_config: r#"{"networkStack":"netstack"}"#.into(),
    };
    let wire: control::ExecutionOptions = options.clone().into();
    assert_eq!(model::ExecutionOptions::try_from(wire).unwrap(), options);
}

#[test]
fn environment_roundtrip_preserves_oci_source() {
    let runtime_profile: adx_core::runtime_profile::RuntimeProfile =
        serde_json::from_value(serde_json::json!({
            "rootfs": {"runtime_class": "runc", "type": "image", "image": "registry/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            "bootstrap": {"type": "image", "image": "registry/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "target": "/__adx", "entrypoint": ["/__adx/usr/local/bin/adx-execd"]}
        }))
        .unwrap();
    runtime_profile.validate().unwrap();
    let wire: control::RuntimeProfile = runtime_profile.clone().into();
    assert_eq!(
        adx_core::runtime_profile::RuntimeProfile::try_from(wire).unwrap(),
        runtime_profile
    );
}

#[test]
fn record_roundtrip_preserves_identity_state_and_ownership() {
    let spec = EnvironmentSpec {
        runtime_profile: Some(serde_json::from_value(serde_json::json!({
            "rootfs": {"runtime_class": "runc", "type": "local", "path": "/opt/adx/runtime/rootfs.img", "readonly": false},
            "bootstrap": {"type": "erofs", "root": "/opt/adx/runtime/rootfs.img", "target": "/__adx", "entrypoint": ["/__adx/usr/local/bin/adx-execd"]},
            "env": {"PATH": "/bin:/usr/bin"}
        })).unwrap()),
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: "i".into(),
        tenant_id: "t".into(),
        image: String::new(),
        runtime_class: "runc".into(),
        resources: Resources {
            cpu_millis: 10,
            memory_bytes: 100,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: Default::default(),
    execution: Default::default(),
    };
    let r = EnvironmentRecord {
        observation: None,
        execution_artifacts: vec![adx_core::CheckpointArtifact {
            storage: "afs".into(),
            location: "pending-capture".into(),
            size_bytes: 0,
        }],
        snapshot_source: Some(adx_core::RestorePoint {
            id: "saved".into(),
            artifact: adx_core::CheckpointArtifact {
                storage: "afs".into(),
                location: "snapshot-input".into(),
                size_bytes: 8,
            },
            origin: Some(adx_core::runtime::RuntimeIdentity {
                environment_id: "source".into(),
                ownership_generation: 1,
                runtime_id: "source-1".into(),
            }),
            source_runtime_id: "source-1".into(),
            expires_at_unix_seconds: None,
        }),
        restart_attempts: 0,
        restart_pending: false,
        spec,
        assignment: Assignment {
            environment_id: "i".into(),
            node_id: "n".into(),
            shard_id: 0,
            generation: 9,
            devices: vec![],
        },
        state: EnvironmentState::Running,
        revision: 2,
        runtime: Runtime {
            id: "i-9".into(),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: None,
        failure: Some(adx_core::RuntimeFailure {
            probe_failure: None,
            reason: adx_core::RuntimeFailureReason::OomKilled,
            exit_code: Some(137),
            message: "environment backend was OOM-killed".into(),
        }),
        last_operation: None,
    };
    let persisted = serde_json::to_vec(&r).unwrap();
    assert_eq!(
        serde_json::from_slice::<EnvironmentRecord>(&persisted).unwrap(),
        r
    );
    let wire = control::EnvironmentRecord::try_from(r.clone()).unwrap();
    assert_eq!(EnvironmentRecord::try_from(wire.clone()).unwrap(), r);
    let mut invalid = wire.clone();
    invalid.state = 0;
    assert!(EnvironmentRecord::try_from(invalid).is_err());
    let mut invalid = wire;
    invalid.assignment.as_mut().unwrap().environment_id = "other".into();
    assert!(EnvironmentRecord::try_from(invalid).is_err());
}

#[test]
fn transport_metadata_cannot_impersonate_a_component() {
    let mut request = tonic::Request::new(());
    request
        .metadata_mut()
        .insert("x-adx-role", "coordinator".parse().unwrap());
    assert_eq!(
        adx_protocol::auth::Peers::default()
            .authenticate(&request)
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}

#[test]
fn local_directory_creation_survives_mount_protocol_roundtrip() {
    use adx_core::execution as model;
    let mount = model::Mount {
        kind: "bind".into(),
        target: "/data".into(),
        options: vec!["rw".into()],
        source: model::StorageSource::Local("/mnt/sfs/workspaces/user".into()).into(),
        create_if_missing: true,
        create_options: Some(model::CreateOptions {
            uid: 10001,
            gid: 10001,
            mode: "0700".into(),
        }),
    };
    let wire: adx_protocol::control::MountSpec = mount.clone().into();
    let restored: model::Mount = wire.try_into().unwrap();
    assert_eq!(restored, mount);
}

#[test]
fn capture_reservation_is_not_a_restorable_checkpoint() {
    let artifact = control::CheckpointArtifact {
        storage: "afs".into(),
        location: "capture".into(),
        size_bytes: 0,
    };
    assert!(adx_core::CheckpointArtifact::try_from(artifact.clone()).is_ok());
    let point = control::RestorePoint {
        id: "capture".into(),
        artifact: Some(artifact),
        source_runtime_id: "environment-1".into(),
        origin: None,
        expires_at_unix_seconds: None,
    };
    assert!(adx_core::RestorePoint::try_from(point).is_err());
}
