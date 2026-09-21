use adx_core::{scheduling::*, Assignment, InstanceSpec, Resources};
use adx_protocol::control as wire;
use prost::Message;
fn selector() -> LabelSelector {
    LabelSelector {
        match_labels: [("app".into(), "a".into())].into(),
        expressions: vec![LabelRequirement {
            key: "rank".into(),
            op: SelectorOp::Gt,
            values: vec!["3".into()],
        }],
    }
}
#[test]
fn protobuf_round_trip_preserves_every_placement_constraint_and_card_identity() {
    let peer = PeerTerm {
        selector: selector(),
        topology_key: "zone".into(),
        tenants: vec!["t".into()],
    };
    let spec = InstanceSpec {
        runtime_environment: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: "i".into(),
        tenant_id: "t".into(),
        image: "image".into(),
        runtime: "runsc".into(),
        priority: 5,
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        },
        scheduling: SchedulingPolicy {
            placement_groups: vec![PlacementGroup {
                target: PlacementTarget::Instance,
                terms: vec![WeightedSelector {
                    selector: selector(),
                    weight: 7,
                }],
                required: true,
                anti: false,
                ordered: true,
            }],
            labels: [("app".into(), "a".into())].into(),
            devices: vec![DeviceRequest {
                kind: DeviceKind::Gpu,
                model: Some("a".into()),
                count: 1,
            }],
            required_node: vec![selector()],
            preferred_node: vec![WeightedSelector {
                selector: selector(),
                weight: 3,
            }],
            required_affinity: vec![peer.clone()],
            required_anti_affinity: vec![peer.clone()],
            preferred_affinity: vec![WeightedPeer {
                term: peer.clone(),
                weight: 4,
            }],
            preferred_anti_affinity: vec![WeightedPeer {
                term: peer,
                weight: 5,
            }],
            topology_spread: vec![TopologySpread {
                selector: selector(),
                topology_key: "zone".into(),
                max_skew: 2,
                min_domains: 3,
                when_unsatisfiable: SpreadMode::ScheduleAnyway,
            }],
        },
        sandbox: Default::default(),
    };
    let bytes = wire::InstanceSpec::from(spec.clone()).encode_to_vec();
    let decoded = wire::InstanceSpec::decode(bytes.as_slice()).unwrap();
    assert_eq!(InstanceSpec::try_from(decoded).unwrap(), spec);
    let assignment = Assignment {
        instance_id: "i".into(),
        node_id: "n".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![DeviceAllocation {
            id: 9,
            kind: DeviceKind::Gpu,
            model: "a".into(),
        }],
    };
    let bytes = wire::Assignment::try_from(assignment.clone())
        .unwrap()
        .encode_to_vec();
    assert_eq!(
        Assignment::try_from(wire::Assignment::decode(bytes.as_slice()).unwrap()).unwrap(),
        assignment
    );
}
#[test]
fn unknown_enums_missing_selectors_and_duplicate_cards_are_rejected() {
    let unknown = wire::SchedulingPolicy {
        devices: vec![wire::DeviceRequest {
            kind: 99,
            model: None,
            count: 1,
        }],
        ..Default::default()
    };
    assert!(SchedulingPolicy::try_from(unknown).is_err());
    let missing = wire::SchedulingPolicy {
        required_affinity: vec![wire::PeerTerm {
            topology_key: "zone".into(),
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(SchedulingPolicy::try_from(missing).is_err());
    let cards = vec![
        wire::DeviceAllocation {
            id: 0,
            kind: 1,
            model: "a".into()
        };
        2
    ];
    assert!(Assignment::try_from(wire::Assignment {
        instance_id: "i".into(),
        node_id: "n".into(),
        generation: 1,
        devices: cards,
        ..Default::default()
    })
    .is_err());
}
#[test]
fn registration_preserves_inventory_and_labels() {
    let node = Node::try_from(wire::RegisterNodeRequest {
        session_id: String::new(),
        heartbeat_sequence: 0,
        reconciling: false,
        node_id: "n".into(),
        node_address: "localhost:1234".into(),
        proxy_address: "localhost:1235".into(),
        accepting_allocations: true,
        capacity: Some(wire::Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        }),
        labels: [("zone".into(), "z".into())].into(),
        devices: vec![wire::Device {
            id: 2,
            kind: 2,
            model: "x".into(),
            healthy: true,
        }],
    })
    .unwrap();
    assert_eq!(node.labels["zone"], "z");
    assert_eq!(node.devices[0].kind, DeviceKind::Npu);
}
