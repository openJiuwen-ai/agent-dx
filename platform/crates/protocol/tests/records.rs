use adx_core::{Assignment, InstanceRecord, InstanceSpec, InstanceState, Resources};
use adx_protocol::control;

#[test]
fn sandbox_options_roundtrip_preserves_backend_contract() {
    use adx_core::sandbox as model;

    let options = model::SandboxOptions {
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
            kind: "bind".into(),
            target: "/workspace".into(),
            options: vec!["ro".into()],
            source: model::StorageSource::Image("registry.example/tools:v1".into()),
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
                    sandbox_port: 8080,
                    sandbox_port_range: None,
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
        limits: model::ResourceLimits {
            cpu_millis: 1500,
            memory_bytes: 2 << 30,
            disk_bytes: 4 << 30,
        },
        extra_config: r#"{"networkStack":"netstack"}"#.into(),
    };
    let wire: control::SandboxOptions = options.clone().into();
    assert_eq!(model::SandboxOptions::try_from(wire).unwrap(), options);
}

#[test]
fn runtime_environment_roundtrip_preserves_oci_source() {
    let environment: adx_core::environment::RuntimeEnvironment =
        serde_json::from_value(serde_json::json!({
            "rootfs": {"runtime": "runc", "type": "image", "image": "registry/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
            "bootstrap": {"type": "image", "image": "registry/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "target": "/__adx", "entrypoint": ["/__adx/usr/local/bin/rrt-runtime"]}
        }))
        .unwrap();
    environment.validate().unwrap();
    let wire: control::RuntimeEnvironment = environment.clone().into();
    assert_eq!(
        adx_core::environment::RuntimeEnvironment::try_from(wire).unwrap(),
        environment
    );
}

#[test]
fn record_roundtrip_preserves_identity_state_and_ownership() {
    let spec = InstanceSpec {
        runtime_environment: Some(serde_json::from_value(serde_json::json!({
            "rootfs": {"runtime": "runc", "type": "local", "path": "/opt/adx/runtime/rootfs.img", "readonly": false},
            "bootstrap": {"type": "erofs", "root": "/opt/adx/runtime/rootfs.img", "target": "/__adx", "entrypoint": ["/__adx/usr/local/bin/rrt-runtime"]},
            "env": {"PATH": "/bin:/usr/bin"}
        })).unwrap()),
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: "i".into(),
        tenant_id: "t".into(),
        image: String::new(),
        runtime: "runc".into(),
        resources: Resources {
            cpu_millis: 10,
            memory_bytes: 100,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: Default::default(),
    sandbox: Default::default(),
    };
    let r = InstanceRecord {
        restart_attempts: 0,
        restart_pending: false,
        spec,
        assignment: Assignment {
            instance_id: "i".into(),
            node_id: "n".into(),
            shard_id: 0,
            generation: 9,
            devices: vec![],
        },
        state: InstanceState::Running,
        revision: 2,
        runtime_id: "i-9".into(),
        resources_held: true,
        runtime_ip: Some("10.0.0.2".parse().unwrap()),
        checkpoint: None,
        last_operation: None,
    };
    let persisted = serde_json::to_vec(&r).unwrap();
    assert_eq!(
        serde_json::from_slice::<InstanceRecord>(&persisted).unwrap(),
        r
    );
    let wire = control::InstanceRecord::try_from(r.clone()).unwrap();
    assert_eq!(InstanceRecord::try_from(wire.clone()).unwrap(), r);
    let mut invalid = wire.clone();
    invalid.state = 0;
    assert!(InstanceRecord::try_from(invalid).is_err());
    let mut invalid = wire;
    invalid.assignment.as_mut().unwrap().instance_id = "other".into();
    assert!(InstanceRecord::try_from(invalid).is_err());
}

#[test]
fn transport_metadata_cannot_impersonate_a_component() {
    let mut request = tonic::Request::new(());
    request
        .metadata_mut()
        .insert("x-adx-role", "master".parse().unwrap());
    assert_eq!(
        adx_protocol::auth::Peers::default()
            .authenticate(&request)
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}
