use adx_core::{Assignment, InstanceRecord, InstanceSpec, InstanceState, Resources};
use adx_protocol::control;
#[test]
fn record_roundtrip_preserves_identity_state_and_ownership() {
    let spec = InstanceSpec {
        env: Default::default(),
        id: "i".into(),
        tenant_id: "t".into(),
        image: "image".into(),
        runtime: "runc".into(),
        resources: Resources {
            cpu_millis: 10,
            memory_bytes: 100,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: Default::default(),
    };
    let r = InstanceRecord {
        spec,
        assignment: Assignment {
            instance_id: "i".into(),
            node_id: "n".into(),
            domain_id: 0,
            generation: 9,
            devices: vec![],
        },
        state: InstanceState::Running,
        revision: 2,
        runtime_id: "i-9".into(),
        resources_held: true,
        runtime_ip: Some("10.0.0.2".parse().unwrap()),
    };
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
