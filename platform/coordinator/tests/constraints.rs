use adx_coordinator::{Coordinator, Node, Placement};
use adx_core::{scheduling::*, EnvironmentSpec, Resources};
fn spec(id: &str) -> EnvironmentSpec {
    EnvironmentSpec {
        envd: None,
        run_policy: None,
        compatibility: None,
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: id.into(),
        tenant_id: "t".into(),
        image: "image".into(),
        runtime_class: "runsc".into(),
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: SchedulingPolicy::default(),
        execution: Default::default(),
    }
}
fn node(id: &str, zone: &str) -> Node {
    Node {
        runtime_classes: vec![
            "runsc".into(),
            "runc".into(),
            "firecracker".into(),
            "r".into(),
            "test-runtime".into(),
        ],
        id: id.into(),
        capacity: Resources {
            cpu_millis: 100,
            memory_bytes: 100,
            disk_bytes: 100,
        },
        available: true,
        labels: [("zone".into(), zone.into())].into(),
        devices: vec![],
    }
}
fn selector(key: &str, value: &str) -> LabelSelector {
    LabelSelector {
        match_labels: [(key.into(), value.into())].into(),
        ..Default::default()
    }
}
fn card(id: u32, kind: DeviceKind, model: &str) -> Device {
    Device {
        id,
        kind,
        model: model.into(),
        healthy: true,
    }
}

#[test]
fn queue_deadline_can_cancel_only_an_unassigned_request() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.submit(spec("waiting")).unwrap();
    assert_eq!(coordinator.pending(0).unwrap(), 1);
    assert!(coordinator.cancel_pending("waiting"));
    assert_eq!(coordinator.pending(0).unwrap(), 0);
    assert!(!coordinator.cancel_pending("waiting"));

    coordinator.register(node("n", "z")).unwrap();
    assert!(
        coordinator.schedule(0).unwrap().is_none(),
        "cancelled work must never start later"
    );
    assert_eq!(
        coordinator
            .scheduling_failure(&spec("probe"))
            .unwrap()
            .max_available
            .cpu_millis,
        100
    );
    coordinator.submit(spec("assigned")).unwrap();
    let assignment = coordinator.schedule(0).unwrap().unwrap();
    assert!(!coordinator.cancel_pending("assigned"));
    coordinator.release(&assignment).unwrap();
}

#[test]
fn scheduling_diagnosis_distinguishes_total_capacity_and_held_reservations() {
    let mut coordinator = Coordinator::new(2, Placement::Pack).unwrap();
    coordinator.register(node("a", "z")).unwrap();
    coordinator.register(node("b", "z")).unwrap();
    let mut occupied = spec("occupied");
    occupied.resources.cpu_millis = 100;
    coordinator.submit(occupied).unwrap();
    let assignment = coordinator.schedule(0).unwrap().unwrap();
    let mut request = spec("oversized");
    request.resources.cpu_millis = 101;
    request.resources.memory_bytes = 101;
    let failure = coordinator.scheduling_failure(&request).unwrap();
    assert_eq!(failure.node_count, 2);
    assert_eq!(failure.eligible_nodes, 0);
    assert_eq!(failure.requested.cpu_millis, 101);
    assert_eq!(failure.max_capacity.cpu_millis, 100);
    assert_eq!(failure.max_available.cpu_millis, 100);
    assert_eq!(failure.rejections["insufficient_cpu"], 2);
    assert_eq!(failure.rejections["insufficient_memory"], 2);
    // A smaller request fits the idle worker while the held reservation remains.
    request.resources.cpu_millis = 1;
    request.resources.memory_bytes = 1;
    let busy = coordinator.scheduling_failure(&request).unwrap();
    assert_eq!(busy.eligible_nodes, 1);
    assert_eq!(busy.rejections["insufficient_cpu"], 1);
    coordinator.release(&assignment).unwrap();
    assert_eq!(
        coordinator
            .scheduling_failure(&request)
            .unwrap()
            .eligible_nodes,
        2
    );
}

#[test]
fn scheduling_diagnosis_keeps_unavailable_runtime_affinity_and_device_reasons() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    assert_eq!(
        coordinator
            .scheduling_failure(&spec("none"))
            .unwrap()
            .rejections["no_workers"],
        1
    );
    let mut n = node("n", "zone-a");
    n.available = false;
    n.runtime_classes = vec!["runc".into()];
    coordinator.register(n).unwrap();
    let mut request = spec("constrained");
    request.scheduling.required_node = vec![selector("zone", "zone-b")];
    request.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Gpu,
        model: None,
        count: 1,
    }];
    let failure = coordinator.scheduling_failure(&request).unwrap();
    for reason in [
        "node_available",
        "runtime_fit",
        "node_affinity",
        "device_fit",
    ] {
        assert_eq!(failure.rejections[reason], 1, "{reason}");
    }
    assert!(!failure.rejections.contains_key("insufficient_cpu"));
}

#[test]
fn central_scheduler_places_only_on_reported_runtime_class() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    let mut runsc = node("runsc-node", "z");
    runsc.runtime_classes = vec!["runsc".into()];
    let mut runc = node("runc-node", "z");
    runc.runtime_classes = vec!["runc".into()];
    coordinator.register(runsc).unwrap();
    coordinator.register(runc).unwrap();
    coordinator.submit(spec("runtime-choice")).unwrap();
    assert_eq!(
        coordinator.schedule(0).unwrap().unwrap().node_id,
        "runsc-node"
    );
}

fn require_checkpoint_restore(request: &mut EnvironmentSpec) {
    request.scheduling.required_node = vec![selector(
        "adx.internal/node-capability/runsc/checkpoint-restore.v1",
        "true",
    )];
}

#[test]
fn capability_constrained_creates_use_eligible_shards_and_local_candidates() {
    let mut coordinator = Coordinator::new(2, Placement::Pack).unwrap();
    let ordinary = node("ordinary", "z");
    assert_eq!(coordinator.register(ordinary).unwrap(), 0);
    let mut capable = node("capable", "z");
    let mut constrained = spec("constrained");
    require_checkpoint_restore(&mut constrained);
    capable
        .labels
        .extend(constrained.scheduling.required_node[0].match_labels.clone());
    assert_eq!(coordinator.register(capable).unwrap(), 1);
    assert!(!coordinator
        .local_candidate(&constrained, "ordinary", &[])
        .unwrap());
    assert!(coordinator
        .local_candidate(&constrained, "capable", &[])
        .unwrap());
    assert_eq!(coordinator.submit(constrained).unwrap(), 1);
    assert_eq!(coordinator.schedule(1).unwrap().unwrap().node_id, "capable");

    assert_eq!(coordinator.submit(spec("ordinary-request")).unwrap(), 0);
    assert_eq!(
        coordinator.schedule(0).unwrap().unwrap().node_id,
        "ordinary"
    );
}

#[test]
fn queued_capability_request_waits_for_compatible_capacity_on_another_shard() {
    let mut coordinator = Coordinator::new(2, Placement::Pack).unwrap();
    coordinator.register(node("ordinary", "z")).unwrap();
    let mut constrained = spec("constrained");
    require_checkpoint_restore(&mut constrained);
    assert_eq!(coordinator.submit(constrained.clone()).unwrap(), 0);
    assert!(coordinator.schedule(0).unwrap().is_none());
    assert_eq!(coordinator.pending(0).unwrap(), 1);

    let mut replacement = node("replacement", "z");
    replacement.runtime_classes = vec!["runc".into()];
    replacement.labels.insert(
        "adx.internal/node-capability/runc/checkpoint-restore.v1".into(),
        "true".into(),
    );
    assert_eq!(coordinator.register(replacement.clone()).unwrap(), 1);
    assert!(coordinator.schedule(0).unwrap().is_none());
    let failure = coordinator.scheduling_failure(&constrained).unwrap();
    assert_eq!(failure.eligible_nodes, 0);
    assert_eq!(failure.rejections["node_capability"], 2);

    replacement.runtime_classes.push("runsc".into());
    replacement
        .labels
        .extend(constrained.scheduling.required_node[0].match_labels.clone());
    coordinator.register(replacement).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
    assert_eq!(coordinator.pending(0).unwrap(), 0);
    assert_eq!(
        coordinator.schedule(1).unwrap().unwrap().node_id,
        "replacement"
    );
}

#[test]
fn whole_cards_are_model_selected_reserved_and_released() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    let mut n = node("n", "z");
    n.devices = vec![
        card(0, DeviceKind::Gpu, "a"),
        card(1, DeviceKind::Gpu, "b"),
        card(0, DeviceKind::Npu, "x"),
    ];
    coordinator.register(n).unwrap();
    let mut a = spec("a");
    a.scheduling.devices = vec![
        DeviceRequest {
            kind: DeviceKind::Gpu,
            model: None,
            count: 1,
        },
        DeviceRequest {
            kind: DeviceKind::Gpu,
            model: Some("a".into()),
            count: 1,
        },
        DeviceRequest {
            kind: DeviceKind::Npu,
            model: Some("x".into()),
            count: 1,
        },
    ];
    coordinator.submit(a).unwrap();
    let assigned = coordinator.schedule(0).unwrap().unwrap();
    assert_eq!(assigned.devices.len(), 3);
    let mut b = spec("b");
    b.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Gpu,
        model: Some("a".into()),
        count: 1,
    }];
    coordinator.submit(b).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
    coordinator.release(&assigned).unwrap();
    assert_eq!(coordinator.schedule(0).unwrap().unwrap().devices[0].id, 0);
}
#[test]
fn required_and_preferred_node_affinity_drive_selection() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    for (n, z) in [("a", "x"), ("b", "y"), ("c", "z")] {
        coordinator.register(node(n, z)).unwrap();
    }
    let mut a = spec("a");
    a.scheduling.required_node = vec![selector("zone", "x"), selector("zone", "y")];
    a.scheduling.preferred_node = vec![WeightedSelector {
        selector: selector("zone", "y"),
        weight: 10,
    }];
    coordinator.submit(a).unwrap();
    assert_eq!(coordinator.schedule(0).unwrap().unwrap().node_id, "b");
}
#[test]
fn environment_affinity_and_reverse_anti_affinity_use_pending_reservations() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.register(node("a", "x")).unwrap();
    coordinator.register(node("b", "y")).unwrap();
    let mut first = spec("first");
    first.scheduling.labels.insert("app".into(), "a".into());
    first.scheduling.required_anti_affinity = vec![PeerTerm {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    coordinator.submit(first).unwrap();
    assert_eq!(coordinator.schedule(0).unwrap().unwrap().node_id, "a");
    let mut second = spec("second");
    second.scheduling.labels.insert("app".into(), "a".into());
    coordinator.submit(second).unwrap();
    assert_eq!(coordinator.schedule(0).unwrap().unwrap().node_id, "b");
    let mut third = spec("third");
    third.scheduling.required_affinity = vec![PeerTerm {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    third.scheduling.required_node = vec![selector("zone", "y")];
    coordinator.submit(third).unwrap();
    assert_eq!(coordinator.schedule(0).unwrap().unwrap().node_id, "b");
}
#[test]
fn topology_spread_counts_reservations_in_all_embedded_shards() {
    let mut coordinator = Coordinator::new(2, Placement::Pack).unwrap();
    coordinator.register(node("a", "x")).unwrap();
    coordinator.register(node("b", "y")).unwrap();
    for id in ["first", "second", "third", "fourth"] {
        let mut s = spec(id);
        s.scheduling.labels.insert("app".into(), "a".into());
        s.scheduling.topology_spread = vec![TopologySpread {
            selector: selector("app", "a"),
            topology_key: "zone".into(),
            max_skew: 1,
            min_domains: 2,
            when_unsatisfiable: SpreadMode::DoNotSchedule,
        }];
        let d = coordinator.submit(s).unwrap();
        assert!(coordinator.schedule(d).unwrap().is_some());
    }
}

#[test]
fn unavailable_and_unhealthy_cards_are_excluded_and_inventory_refresh_keeps_leases() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    let mut n = node("n", "z");
    n.devices = vec![card(0, DeviceKind::Gpu, "a"), card(1, DeviceKind::Gpu, "a")];
    n.devices[1].healthy = false;
    coordinator.register(n.clone()).unwrap();
    let mut a = spec("a");
    a.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Gpu,
        model: Some("a".into()),
        count: 1,
    }];
    coordinator.submit(a.clone()).unwrap();
    let first = coordinator.schedule(0).unwrap().unwrap();
    n.devices.clear();
    coordinator.register(n.clone()).unwrap();
    n.devices = vec![card(0, DeviceKind::Gpu, "a")];
    coordinator.register(n).unwrap();
    a.id = "b".into();
    coordinator.submit(a).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
    coordinator.release(&first).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_some());
}
#[test]
fn self_affinity_bootstraps_but_other_groups_and_missing_labels_do_not_match() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    let mut missing = node("a", "z");
    missing.labels.clear();
    coordinator.register(missing).unwrap();
    coordinator.register(node("b", "z")).unwrap();
    let mut a = spec("a");
    a.scheduling.labels.insert("app".into(), "a".into());
    a.scheduling.required_affinity = vec![PeerTerm {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    coordinator.submit(a).unwrap();
    assert_eq!(coordinator.schedule(0).unwrap().unwrap().node_id, "b");
    let mut b = spec("b");
    b.scheduling.required_affinity = vec![PeerTerm {
        selector: selector("app", "missing"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    coordinator.submit(b).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
}
#[test]
fn affinity_defaults_to_own_tenant_and_can_target_explicit_tenants() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.register(node("a", "z")).unwrap();
    let mut existing = spec("existing");
    existing.tenant_id = "other".into();
    existing.scheduling.labels.insert("app".into(), "a".into());
    coordinator.submit(existing).unwrap();
    coordinator.schedule(0).unwrap().unwrap();
    let mut private = spec("private");
    private.scheduling.required_affinity = vec![PeerTerm {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    coordinator.submit(private.clone()).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
    private.id = "explicit".into();
    private.scheduling.required_affinity[0].tenants = vec!["other".into()];
    coordinator.submit(private).unwrap();
    assert_eq!(
        coordinator.schedule(0).unwrap().unwrap().environment_id,
        "explicit"
    );
}
#[test]
fn required_anti_affinity_is_enforced_across_embedded_shards() {
    let mut coordinator = Coordinator::new(2, Placement::Pack).unwrap();
    coordinator.register(node("a", "same")).unwrap();
    coordinator.register(node("b", "same")).unwrap();
    let mut a = spec("a");
    a.scheduling.labels.insert("app".into(), "a".into());
    coordinator.submit(a.clone()).unwrap();
    let first = coordinator.schedule(0).unwrap().unwrap();
    a.id = "b".into();
    a.scheduling.required_anti_affinity = vec![PeerTerm {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    coordinator.submit(a).unwrap();
    assert!(coordinator.schedule(1).unwrap().is_none());
    coordinator.release(&first).unwrap();
    assert!(coordinator.schedule(1).unwrap().is_some());
}
#[test]
fn soft_peer_preferences_choose_colocation_or_separation() {
    for anti in [false, true] {
        let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
        coordinator.register(node("a", "x")).unwrap();
        coordinator.register(node("b", "y")).unwrap();
        let mut first = spec("first");
        first.scheduling.labels.insert("app".into(), "a".into());
        first.scheduling.required_node = vec![selector("zone", "x")];
        coordinator.submit(first).unwrap();
        coordinator.schedule(0).unwrap().unwrap();
        let mut second = spec("second");
        let term = WeightedPeer {
            term: PeerTerm {
                selector: selector("app", "a"),
                topology_key: "zone".into(),
                tenants: vec![],
            },
            weight: 5,
        };
        if anti {
            second.scheduling.preferred_anti_affinity.push(term);
        } else {
            second.scheduling.preferred_affinity.push(term);
        }
        coordinator.submit(second).unwrap();
        assert_eq!(
            coordinator.schedule(0).unwrap().unwrap().node_id,
            if anti { "b" } else { "a" }
        );
    }
}
#[test]
fn hard_spread_blocks_when_min_domains_is_unmet_and_soft_spread_stays_schedulable() {
    for mode in [SpreadMode::DoNotSchedule, SpreadMode::ScheduleAnyway] {
        let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
        coordinator.register(node("a", "only")).unwrap();
        let mut a = spec("a");
        a.scheduling.labels.insert("app".into(), "a".into());
        a.scheduling.topology_spread = vec![TopologySpread {
            selector: selector("app", "a"),
            topology_key: "zone".into(),
            max_skew: 1,
            min_domains: 2,
            when_unsatisfiable: mode,
        }];
        coordinator.submit(a.clone()).unwrap();
        coordinator.schedule(0).unwrap().unwrap();
        a.id = "b".into();
        coordinator.submit(a).unwrap();
        assert_eq!(
            coordinator.schedule(0).unwrap().is_some(),
            mode == SpreadMode::ScheduleAnyway
        );
    }
}
#[test]
fn soft_topology_spread_prefers_the_less_populated_zone() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.register(node("a", "x")).unwrap();
    coordinator.register(node("b", "y")).unwrap();
    let mut a = spec("a");
    a.scheduling.labels.insert("app".into(), "a".into());
    a.scheduling.required_node = vec![selector("zone", "x")];
    coordinator.submit(a.clone()).unwrap();
    coordinator.schedule(0).unwrap().unwrap();
    a.id = "b".into();
    a.scheduling.required_node.clear();
    a.scheduling.topology_spread = vec![TopologySpread {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        max_skew: 1,
        min_domains: 1,
        when_unsatisfiable: SpreadMode::ScheduleAnyway,
    }];
    coordinator.submit(a).unwrap();
    assert_eq!(coordinator.schedule(0).unwrap().unwrap().node_id, "b");
}
#[test]
fn invalid_constraints_are_rejected_before_entering_the_queue() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    let mut a = spec("a");
    a.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Npu,
        model: None,
        count: 0,
    }];
    assert!(coordinator.submit(a.clone()).is_err());
    a.scheduling.devices.clear();
    a.scheduling.required_node = vec![LabelSelector {
        match_labels: Default::default(),
        expressions: vec![LabelRequirement {
            key: "generation".into(),
            op: SelectorOp::Gt,
            values: vec!["nan".into()],
        }],
    }];
    assert!(coordinator.submit(a).is_err());
}
