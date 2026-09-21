use adx_core::{
    scheduling::{Device, DeviceAllocation, DeviceKind, DeviceRequest},
    Assignment, CapsuleSpec, Error, Resources,
};
use adx_master::{
    storage::{StoredCapsule, StoredNode, StoredSnapshot},
    Master, Node, Placement,
};
fn saved() -> StoredSnapshot {
    let request = CapsuleSpec {
        environment: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: "i".into(),
        tenant_id: "t".into(),
        image: "i".into(),
        runtime_class: "r".into(),
        resources: Resources {
            cpu_millis: 100,
            memory_bytes: 100,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: Default::default(),
        sandbox: Default::default(),
    };
    let node = Node {
        id: "n".into(),
        capacity: Resources {
            cpu_millis: 50,
            memory_bytes: 50,
            disk_bytes: 0,
        },
        available: true,
        labels: Default::default(),
        devices: vec![],
    };
    let mut request = request;
    request.scheduling.devices.push(DeviceRequest {
        kind: DeviceKind::Gpu,
        model: Some("card".into()),
        count: 1,
    });
    let assignment = Assignment {
        capsule_id: "i".into(),
        node_id: "n".into(),
        shard_id: 0,
        generation: 42,
        devices: vec![DeviceAllocation {
            id: 0,
            kind: DeviceKind::Gpu,
            model: "card".into(),
        }],
    };
    StoredSnapshot {
        shard_count: 1,
        generation: 42,
        revision: 5,
        nodes: [(
            "n".into(),
            StoredNode {
                node,
                shard_id: 0,
                address: "127.0.0.1:9000".into(),
                proxy_address: "127.0.0.1:9001".into(),
                session: None,
            },
        )]
        .into(),
        capsules: [(
            "i".into(),
            StoredCapsule {
                recovery: None,
                invalidated: false,
                spec: request,
                assignment,
                result: None,
            },
        )]
        .into(),
    }
}
#[test]
fn restore_keeps_over_capacity_usage_and_temporarily_missing_cards() {
    let saved = saved();
    let mut m = Master::restore(&saved, Placement::Pack).unwrap();
    assert!(!m.snapshot().node("n").unwrap().available);
    let mut request = saved.capsules["i"].spec.clone();
    request.id = "waiting".into();
    m.submit(request).unwrap();
    let mut node = saved.nodes["n"].node.clone();
    node.capacity.cpu_millis = 200;
    node.capacity.memory_bytes = 200;
    node.devices = vec![Device {
        id: 0,
        kind: DeviceKind::Gpu,
        model: "card".into(),
        healthy: true,
    }];
    m.register(node).unwrap();
    assert!(
        m.schedule(0).unwrap().is_none(),
        "reappearing card is still occupied"
    );
    m.release(&saved.capsules["i"].assignment).unwrap();
    let a = m.schedule(0).unwrap().unwrap();
    assert_eq!(a.capsule_id, "waiting");
    assert_eq!(a.generation, 43);
}
#[test]
fn corrupt_ownership_or_double_card_assignment_fails_recovery() {
    let mut saved = saved();
    let mut duplicate = saved.capsules["i"].clone();
    duplicate.spec.id = "other".into();
    duplicate.assignment.capsule_id = "other".into();
    saved.capsules.insert("other".into(), duplicate);
    assert!(matches!(
        Master::restore(&saved, Placement::Pack),
        Err(Error::Conflict)
    ));
    saved.capsules.remove("other");
    saved.capsules.get_mut("i").unwrap().assignment.shard_id = 1;
    assert!(Master::restore(&saved, Placement::Pack).is_err());
}
#[test]
fn exhausted_generation_never_wraps_to_an_old_identity() {
    let mut saved = saved();
    saved.generation = u64::MAX;
    let mut m = Master::restore(&saved, Placement::Pack).unwrap();
    m.release(&saved.capsules["i"].assignment).unwrap();
    let mut request = saved.capsules["i"].spec.clone();
    request.id = "new".into();
    request.scheduling.devices.clear();
    request.resources.cpu_millis = 1;
    request.resources.memory_bytes = 1;
    m.register(saved.nodes["n"].node.clone()).unwrap();
    m.submit(request).unwrap();
    assert_eq!(m.schedule(0), Err(Error::Conflict));
    assert!(m.snapshot().capsules().is_empty());
}

#[test]
fn capacity_reduction_does_not_erase_persisted_scalar_usage() {
    let saved = saved();
    let mut m = Master::restore(&saved, Placement::Pack).unwrap();
    m.register(saved.nodes["n"].node.clone()).unwrap();
    let mut request = saved.capsules["i"].spec.clone();
    request.id = "cpu-only".into();
    request.scheduling.devices.clear();
    request.resources.cpu_millis = 1;
    request.resources.memory_bytes = 1;
    m.submit(request).unwrap();
    assert!(m.schedule(0).unwrap().is_none());
    m.release(&saved.capsules["i"].assignment).unwrap();
    assert!(m.schedule(0).unwrap().is_some());
}

#[test]
fn metrics_preserve_missing_device_and_overcapacity_reservations_after_restart() {
    let saved = saved();
    let mut master = Master::restore(&saved, Placement::Pack).unwrap();
    let text = master.metrics();
    assert!(
        text.contains("adx_master_node_reserved_cpu_millis{shard_id=\"0\",node_id=\"n\"} 100\n")
    );
    assert!(text
        .contains("adx_master_node_overcommitted_cpu_millis{shard_id=\"0\",node_id=\"n\"} 50\n"));
    assert!(text.contains("adx_master_node_available_cpu_millis{shard_id=\"0\",node_id=\"n\"} 0\n"));
    assert!(text.contains("kind=\"gpu\",model=\"card\",state=\"reserved\"} 1\n"));
    let mut pending = saved.capsules["i"].spec.clone();
    pending.id = "pending".into();
    master.submit(pending).unwrap();
    assert!(master
        .metrics()
        .contains("adx_master_queued_requests{shard_id=\"0\"} 1\n"));
    master.release(&saved.capsules["i"].assignment).unwrap();
    assert!(master
        .metrics()
        .contains("adx_master_node_reserved_cpu_millis{shard_id=\"0\",node_id=\"n\"} 0\n"));
}
