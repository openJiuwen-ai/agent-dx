use adx_core::{scheduling::*, InstanceSpec, Resources};
use adx_master::{Master, Node, Placement};
fn spec(id: &str) -> InstanceSpec {
    InstanceSpec {
        runtime_environment: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: id.into(),
        tenant_id: "t".into(),
        image: "image".into(),
        runtime: "runsc".into(),
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: SchedulingPolicy::default(),
    }
}
fn node(id: &str, zone: &str) -> Node {
    Node {
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
fn whole_cards_are_model_selected_reserved_and_released() {
    let mut master = Master::new(1, Placement::Pack).unwrap();
    let mut n = node("n", "z");
    n.devices = vec![
        card(0, DeviceKind::Gpu, "a"),
        card(1, DeviceKind::Gpu, "b"),
        card(0, DeviceKind::Npu, "x"),
    ];
    master.register(n).unwrap();
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
    master.submit(a).unwrap();
    let assigned = master.schedule(0).unwrap().unwrap();
    assert_eq!(assigned.devices.len(), 3);
    let mut b = spec("b");
    b.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Gpu,
        model: Some("a".into()),
        count: 1,
    }];
    master.submit(b).unwrap();
    assert!(master.schedule(0).unwrap().is_none());
    master.release(&assigned).unwrap();
    assert_eq!(master.schedule(0).unwrap().unwrap().devices[0].id, 0);
}
#[test]
fn required_and_preferred_node_affinity_drive_selection() {
    let mut master = Master::new(1, Placement::Pack).unwrap();
    for (n, z) in [("a", "x"), ("b", "y"), ("c", "z")] {
        master.register(node(n, z)).unwrap();
    }
    let mut a = spec("a");
    a.scheduling.required_node = vec![selector("zone", "x"), selector("zone", "y")];
    a.scheduling.preferred_node = vec![WeightedSelector {
        selector: selector("zone", "y"),
        weight: 10,
    }];
    master.submit(a).unwrap();
    assert_eq!(master.schedule(0).unwrap().unwrap().node_id, "b");
}
#[test]
fn instance_affinity_and_reverse_anti_affinity_use_pending_reservations() {
    let mut master = Master::new(1, Placement::Pack).unwrap();
    master.register(node("a", "x")).unwrap();
    master.register(node("b", "y")).unwrap();
    let mut first = spec("first");
    first.scheduling.labels.insert("app".into(), "a".into());
    first.scheduling.required_anti_affinity = vec![PeerTerm {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    master.submit(first).unwrap();
    assert_eq!(master.schedule(0).unwrap().unwrap().node_id, "a");
    let mut second = spec("second");
    second.scheduling.labels.insert("app".into(), "a".into());
    master.submit(second).unwrap();
    assert_eq!(master.schedule(0).unwrap().unwrap().node_id, "b");
    let mut third = spec("third");
    third.scheduling.required_affinity = vec![PeerTerm {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    third.scheduling.required_node = vec![selector("zone", "y")];
    master.submit(third).unwrap();
    assert_eq!(master.schedule(0).unwrap().unwrap().node_id, "b");
}
#[test]
fn topology_spread_counts_reservations_in_all_embedded_shards() {
    let mut master = Master::new(2, Placement::Pack).unwrap();
    master.register(node("a", "x")).unwrap();
    master.register(node("b", "y")).unwrap();
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
        let d = master.submit(s).unwrap();
        assert!(master.schedule(d).unwrap().is_some());
    }
}

#[test]
fn unavailable_and_unhealthy_cards_are_excluded_and_inventory_refresh_keeps_leases() {
    let mut master = Master::new(1, Placement::Pack).unwrap();
    let mut n = node("n", "z");
    n.devices = vec![card(0, DeviceKind::Gpu, "a"), card(1, DeviceKind::Gpu, "a")];
    n.devices[1].healthy = false;
    master.register(n.clone()).unwrap();
    let mut a = spec("a");
    a.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Gpu,
        model: Some("a".into()),
        count: 1,
    }];
    master.submit(a.clone()).unwrap();
    let first = master.schedule(0).unwrap().unwrap();
    n.devices.clear();
    master.register(n.clone()).unwrap();
    n.devices = vec![card(0, DeviceKind::Gpu, "a")];
    master.register(n).unwrap();
    a.id = "b".into();
    master.submit(a).unwrap();
    assert!(master.schedule(0).unwrap().is_none());
    master.release(&first).unwrap();
    assert!(master.schedule(0).unwrap().is_some());
}
#[test]
fn self_affinity_bootstraps_but_other_groups_and_missing_labels_do_not_match() {
    let mut master = Master::new(1, Placement::Pack).unwrap();
    let mut missing = node("a", "z");
    missing.labels.clear();
    master.register(missing).unwrap();
    master.register(node("b", "z")).unwrap();
    let mut a = spec("a");
    a.scheduling.labels.insert("app".into(), "a".into());
    a.scheduling.required_affinity = vec![PeerTerm {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    master.submit(a).unwrap();
    assert_eq!(master.schedule(0).unwrap().unwrap().node_id, "b");
    let mut b = spec("b");
    b.scheduling.required_affinity = vec![PeerTerm {
        selector: selector("app", "missing"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    master.submit(b).unwrap();
    assert!(master.schedule(0).unwrap().is_none());
}
#[test]
fn affinity_defaults_to_own_tenant_and_can_target_explicit_tenants() {
    let mut master = Master::new(1, Placement::Pack).unwrap();
    master.register(node("a", "z")).unwrap();
    let mut existing = spec("existing");
    existing.tenant_id = "other".into();
    existing.scheduling.labels.insert("app".into(), "a".into());
    master.submit(existing).unwrap();
    master.schedule(0).unwrap().unwrap();
    let mut private = spec("private");
    private.scheduling.required_affinity = vec![PeerTerm {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    master.submit(private.clone()).unwrap();
    assert!(master.schedule(0).unwrap().is_none());
    private.id = "explicit".into();
    private.scheduling.required_affinity[0].tenants = vec!["other".into()];
    master.submit(private).unwrap();
    assert_eq!(master.schedule(0).unwrap().unwrap().instance_id, "explicit");
}
#[test]
fn required_anti_affinity_is_enforced_across_embedded_shards() {
    let mut master = Master::new(2, Placement::Pack).unwrap();
    master.register(node("a", "same")).unwrap();
    master.register(node("b", "same")).unwrap();
    let mut a = spec("a");
    a.scheduling.labels.insert("app".into(), "a".into());
    master.submit(a.clone()).unwrap();
    let first = master.schedule(0).unwrap().unwrap();
    a.id = "b".into();
    a.scheduling.required_anti_affinity = vec![PeerTerm {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        tenants: vec![],
    }];
    master.submit(a).unwrap();
    assert!(master.schedule(1).unwrap().is_none());
    master.release(&first).unwrap();
    assert!(master.schedule(1).unwrap().is_some());
}
#[test]
fn soft_peer_preferences_choose_colocation_or_separation() {
    for anti in [false, true] {
        let mut master = Master::new(1, Placement::Pack).unwrap();
        master.register(node("a", "x")).unwrap();
        master.register(node("b", "y")).unwrap();
        let mut first = spec("first");
        first.scheduling.labels.insert("app".into(), "a".into());
        first.scheduling.required_node = vec![selector("zone", "x")];
        master.submit(first).unwrap();
        master.schedule(0).unwrap().unwrap();
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
        master.submit(second).unwrap();
        assert_eq!(
            master.schedule(0).unwrap().unwrap().node_id,
            if anti { "b" } else { "a" }
        );
    }
}
#[test]
fn hard_spread_blocks_when_min_domains_is_unmet_and_soft_spread_stays_schedulable() {
    for mode in [SpreadMode::DoNotSchedule, SpreadMode::ScheduleAnyway] {
        let mut master = Master::new(1, Placement::Pack).unwrap();
        master.register(node("a", "only")).unwrap();
        let mut a = spec("a");
        a.scheduling.labels.insert("app".into(), "a".into());
        a.scheduling.topology_spread = vec![TopologySpread {
            selector: selector("app", "a"),
            topology_key: "zone".into(),
            max_skew: 1,
            min_domains: 2,
            when_unsatisfiable: mode,
        }];
        master.submit(a.clone()).unwrap();
        master.schedule(0).unwrap().unwrap();
        a.id = "b".into();
        master.submit(a).unwrap();
        assert_eq!(
            master.schedule(0).unwrap().is_some(),
            mode == SpreadMode::ScheduleAnyway
        );
    }
}
#[test]
fn soft_topology_spread_prefers_the_less_populated_zone() {
    let mut master = Master::new(1, Placement::Pack).unwrap();
    master.register(node("a", "x")).unwrap();
    master.register(node("b", "y")).unwrap();
    let mut a = spec("a");
    a.scheduling.labels.insert("app".into(), "a".into());
    a.scheduling.required_node = vec![selector("zone", "x")];
    master.submit(a.clone()).unwrap();
    master.schedule(0).unwrap().unwrap();
    a.id = "b".into();
    a.scheduling.required_node.clear();
    a.scheduling.topology_spread = vec![TopologySpread {
        selector: selector("app", "a"),
        topology_key: "zone".into(),
        max_skew: 1,
        min_domains: 1,
        when_unsatisfiable: SpreadMode::ScheduleAnyway,
    }];
    master.submit(a).unwrap();
    assert_eq!(master.schedule(0).unwrap().unwrap().node_id, "b");
}
#[test]
fn invalid_constraints_are_rejected_before_entering_the_queue() {
    let mut master = Master::new(1, Placement::Pack).unwrap();
    let mut a = spec("a");
    a.scheduling.devices = vec![DeviceRequest {
        kind: DeviceKind::Npu,
        model: None,
        count: 0,
    }];
    assert!(master.submit(a.clone()).is_err());
    a.scheduling.devices.clear();
    a.scheduling.required_node = vec![LabelSelector {
        match_labels: Default::default(),
        expressions: vec![LabelRequirement {
            key: "generation".into(),
            op: SelectorOp::Gt,
            values: vec!["nan".into()],
        }],
    }];
    assert!(master.submit(a).is_err());
}
