use adx_core::{scheduling::*, EnvironmentSpec, Resources};
use adx_scheduling::{Candidate, Framework, Node, PlacedEnvironment, Placement, Snapshot};
use std::collections::BTreeMap;
fn request(id: &str) -> EnvironmentSpec {
    EnvironmentSpec {
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
        scheduling: Default::default(),
        sandbox: Default::default(),
    }
}
fn term(key: &str, value: &str, weight: u32) -> WeightedSelector {
    WeightedSelector {
        selector: LabelSelector {
            match_labels: BTreeMap::from([(key.into(), value.into())]),
            ..Default::default()
        },
        weight,
    }
}
fn selected(r: &EnvironmentSpec, s: &Snapshot) -> Option<String> {
    Framework::builtin(Placement::Spread)
        .select(
            r,
            s.nodes().values().map(|n| Candidate {
                node: n,
                available: n.capacity,
                devices: &[],
                snapshot: s,
                prepared: None,
            }),
        )
        .unwrap()
        .map(str::to_owned)
}
#[test]
fn peer_groups_preserve_or_tenants_pending_placements_and_reverse_exclusion() {
    let mut s = Snapshot::default();
    for id in ["a", "b"] {
        s.update_node(Node {
            runtime_classes: vec![
                "runsc".into(),
                "runc".into(),
                "firecracker".into(),
                "r".into(),
                "test-runtime".into(),
            ],
            id: id.into(),
            devices: vec![],
            capacity: Resources {
                cpu_millis: 10,
                memory_bytes: 10,
                disk_bytes: 0,
            },
            labels: BTreeMap::new(),
            available: true,
        });
    }
    let mut peer = request("peer");
    peer.scheduling.labels.insert("app".into(), "db".into());
    s.place(PlacedEnvironment {
        spec: peer,
        node_id: "b".into(),
    });
    let mut r = request("client");
    r.scheduling.placement_groups.push(PlacementGroup {
        target: PlacementTarget::Environment,
        terms: vec![term("app", "missing", 1), term("app", "db", 1)],
        required: true,
        anti: false,
        ordered: false,
    });
    assert_eq!(selected(&r, &s).as_deref(), Some("b"));
    r.tenant_id = "other".into();
    assert_eq!(selected(&r, &s), None);
    r.tenant_id = "t".into();
    r.scheduling.placement_groups[0].anti = true;
    assert_eq!(selected(&r, &s).as_deref(), Some("a"));
    s.place(PlacedEnvironment {
        spec: r,
        node_id: "a".into(),
    });
    let mut db = request("another-db");
    db.scheduling.labels.insert("app".into(), "db".into());
    assert_eq!(selected(&db, &s).as_deref(), Some("b"));
}
#[test]
fn ordered_group_uses_first_match_and_weights_apply_only_in_weighted_mode() {
    let mut s = Snapshot::default();
    for (id, labels) in [
        ("a", BTreeMap::from([("tier".into(), "first".into())])),
        (
            "b",
            BTreeMap::from([
                ("tier".into(), "second".into()),
                ("disk".into(), "fast".into()),
            ]),
        ),
    ] {
        s.update_node(Node {
            runtime_classes: vec![
                "runsc".into(),
                "runc".into(),
                "firecracker".into(),
                "r".into(),
                "test-runtime".into(),
            ],
            id: id.into(),
            devices: vec![],
            capacity: Resources {
                cpu_millis: 10,
                memory_bytes: 10,
                disk_bytes: 0,
            },
            labels,
            available: true,
        });
    }
    let mut r = request("client");
    r.scheduling.placement_groups.push(PlacementGroup {
        target: PlacementTarget::Node,
        terms: vec![
            term("tier", "first", 1),
            term("tier", "second", 100),
            term("disk", "fast", 100),
        ],
        required: false,
        anti: false,
        ordered: true,
    });
    assert_eq!(selected(&r, &s).as_deref(), Some("a"));
    r.scheduling.placement_groups[0].ordered = false;
    assert_eq!(selected(&r, &s).as_deref(), Some("b"));
}
