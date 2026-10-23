use adx_coordinator::{Coordinator, Node, Placement};
use adx_core::scheduling::{LabelSelector, WeightedSelector};
use adx_core::{Assignment, EnvironmentSpec, Resources};

fn node(id: &str, capacity: u64) -> Node {
    Node {
        id: id.into(),
        capacity: Resources {
            cpu_millis: capacity,
            memory_bytes: capacity,
            disk_bytes: capacity,
        },
        available: true,
        runtime_classes: vec!["runsc".into()],
        devices: vec![],
        labels: [
            ("NODE_ID".into(), id.into()),
            ("ADX_CHECKPOINT_STORAGE".into(), "juicefs".into()),
        ]
        .into(),
    }
}

fn selector(key: &str, value: &str) -> LabelSelector {
    LabelSelector {
        match_labels: [(key.into(), value.into())].into(),
        ..Default::default()
    }
}

fn request(id: &str) -> EnvironmentSpec {
    let mut spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
        "id": id, "tenant_id": "tenant", "image": "image", "runtime_class": "runsc", "snapshot_id": "snapshot",
        "priority": 0, "resources": {"cpu_millis": 1, "memory_bytes": 1, "disk_bytes": 1}
    })).unwrap();
    spec.scheduling
        .required_node
        .push(selector("ADX_CHECKPOINT_STORAGE", "juicefs"));
    spec.scheduling.preferred_node.push(WeightedSelector {
        selector: selector("NODE_ID", "source"),
        weight: 1,
    });
    spec
}

fn drain(coordinator: &mut Coordinator) -> Vec<Assignment> {
    let mut result = Vec::new();
    let mut rounds = 0;
    while let Some(shard) = coordinator.take_ready_shard() {
        rounds += 1;
        assert!(
            rounds <= 20,
            "pending snapshots must sleep when no node fits"
        );
        let round = coordinator.schedule_round(shard).unwrap();
        assert!(round.error.is_none());
        result.extend(round.assignments);
    }
    result
}

#[test]
fn snapshot_source_preference_selects_its_shard_before_node_scoring() {
    for placement in [Placement::Pack, Placement::Spread] {
        let mut coordinator = Coordinator::new(2, placement).unwrap();
        coordinator.register(node("other", 100)).unwrap();
        coordinator.register(node("source", 1)).unwrap();
        assert_eq!(coordinator.submit(request("clone")).unwrap(), 1);
        assert_eq!(drain(&mut coordinator)[0].node_id, "source");
    }
}

#[test]
fn snapshot_source_preference_preserves_rotation_for_interleaved_creates() {
    for placement in [Placement::Pack, Placement::Spread] {
        for nodes in [
            &["other-a", "source"][..],
            &["other-a", "source", "other-b"][..],
        ] {
            let mut coordinator = Coordinator::new(nodes.len(), placement).unwrap();
            for id in nodes {
                coordinator.register(node(id, 100)).unwrap();
            }
            let mut ordinary_nodes = std::collections::BTreeMap::new();
            for index in 0..nodes.len() * 2 {
                let clone = request(&format!("clone-{index}"));
                let mut ordinary = request(&format!("ordinary-{index}"));
                ordinary.snapshot_id = None;
                ordinary.scheduling = Default::default();
                coordinator.submit(clone.clone()).unwrap();
                coordinator.submit(ordinary.clone()).unwrap();
                let assignments = drain(&mut coordinator);
                assert_eq!(assignments.len(), 2);
                for assignment in assignments {
                    if assignment.environment_id == clone.id {
                        assert_eq!(assignment.node_id, "source");
                    } else {
                        assert_eq!(assignment.environment_id, ordinary.id);
                        *ordinary_nodes.entry(assignment.node_id).or_insert(0) += 1;
                    }
                }
            }
            assert_eq!(
                ordinary_nodes,
                nodes.iter().map(|id| ((*id).into(), 2)).collect(),
                "snapshot affinity must not concentrate ordinary creates on one shard"
            );
        }
    }
}

#[test]
fn snapshot_explicit_preference_and_local_only_pin_select_the_right_shard() {
    for required in [false, true] {
        let mut coordinator = Coordinator::new(2, Placement::Pack).unwrap();
        coordinator.register(node("source", 1)).unwrap();
        coordinator.register(node("explicit-target", 1)).unwrap();
        let mut spec = request("clone");
        spec.scheduling.preferred_node.clear();
        if required {
            // Local-only normalization produces a required source selector.
            spec.scheduling.required_node = vec![selector("NODE_ID", "explicit-target")];
        } else {
            spec.scheduling.preferred_node.push(WeightedSelector {
                selector: selector("NODE_ID", "explicit-target"),
                weight: 1,
            });
        }
        assert_eq!(coordinator.submit(spec).unwrap(), 1);
        assert_eq!(drain(&mut coordinator)[0].node_id, "explicit-target");
    }
}

#[test]
fn snapshot_source_ineligible_uses_another_shard_without_waiting() {
    for reason in ["offline", "capacity", "runtime", "storage"] {
        let mut coordinator = Coordinator::new(2, Placement::Spread).unwrap();
        let mut source = node("source", 1);
        match reason {
            "offline" => source.available = false,
            "capacity" => source.capacity.cpu_millis = 0,
            "runtime" => source.runtime_classes = vec!["firecracker".into()],
            "storage" => {
                source
                    .labels
                    .insert("ADX_CHECKPOINT_STORAGE".into(), "other-fs".into());
            }
            _ => unreachable!(),
        }
        coordinator.register(source).unwrap();
        coordinator.register(node("other", 1)).unwrap();
        assert_eq!(coordinator.submit(request("clone")).unwrap(), 1, "{reason}");
        assert_eq!(drain(&mut coordinator)[0].node_id, "other", "{reason}");
    }
}

#[test]
fn concurrent_snapshot_requests_fall_back_after_source_capacity_is_reserved() {
    let mut coordinator = Coordinator::new(2, Placement::Spread).unwrap();
    coordinator.register(node("source", 1)).unwrap();
    coordinator.register(node("other", 1)).unwrap();
    assert_eq!(coordinator.submit(request("first")).unwrap(), 0);
    assert_eq!(coordinator.submit(request("second")).unwrap(), 0);
    let assignments = drain(&mut coordinator);
    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0].environment_id, "first");
    assert_eq!(assignments[0].node_id, "source");
    assert_eq!(assignments[1].environment_id, "second");
    assert_eq!(assignments[1].node_id, "other");
    assert!(assignments[1].generation > assignments[0].generation);
    assert_eq!(coordinator.submit(request("second")).unwrap(), 1);
    assert!(drain(&mut coordinator).is_empty());
    assert_eq!(
        coordinator.node_resources("source").unwrap().1.cpu_millis,
        0
    );
    assert_eq!(coordinator.node_resources("other").unwrap().1.cpu_millis, 0);
}

#[test]
fn source_loss_while_queued_preserves_identity_and_enqueue_time_across_shards() {
    let mut coordinator = Coordinator::new(2, Placement::Pack).unwrap();
    coordinator.register(node("source", 1)).unwrap();
    coordinator.register(node("other", 1)).unwrap();
    coordinator.submit(request("clone")).unwrap();
    let original = coordinator.pending_requests()[0].clone();
    let mut source = node("source", 1);
    source.available = false;
    coordinator.register(source).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
    let pending = coordinator.pending_requests();
    assert_eq!(pending, vec![original]);
    assert_eq!(coordinator.submit(request("clone")).unwrap(), 1);
    assert_eq!(drain(&mut coordinator)[0].node_id, "other");
}

#[test]
fn snapshot_hard_node_constraint_never_falls_back_outside_the_constraint() {
    let mut coordinator = Coordinator::new(2, Placement::Pack).unwrap();
    coordinator.register(node("source", 0)).unwrap();
    coordinator.register(node("other", 2)).unwrap();
    let mut spec = request("clone");
    spec.scheduling.required_node[0]
        .match_labels
        .insert("NODE_ID".into(), "source".into());
    coordinator.submit(spec).unwrap();
    assert!(drain(&mut coordinator).is_empty());
    assert_eq!(coordinator.pending_requests().len(), 1);
    coordinator.register(node("source", 1)).unwrap();
    assert_eq!(drain(&mut coordinator)[0].node_id, "source");
}

#[test]
fn snapshot_waits_only_when_all_eligible_nodes_are_full() {
    let mut coordinator = Coordinator::new(2, Placement::Pack).unwrap();
    coordinator.register(node("source", 0)).unwrap();
    coordinator.register(node("other", 0)).unwrap();
    coordinator.submit(request("clone")).unwrap();
    assert!(drain(&mut coordinator).is_empty());
    coordinator.register(node("other", 1)).unwrap();
    assert_eq!(drain(&mut coordinator)[0].node_id, "other");
}

#[test]
fn snapshot_retry_keeps_rejected_nodes_excluded_across_shard_transfers() {
    let mut coordinator = Coordinator::new(3, Placement::Pack).unwrap();
    for id in ["source", "other-a", "other-b"] {
        coordinator.register(node(id, 1)).unwrap();
    }
    coordinator.submit(request("clone")).unwrap();
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..3 {
        let assignments = drain(&mut coordinator);
        assert_eq!(assignments.len(), 1);
        assert!(seen.insert(assignments[0].node_id.clone()));
        coordinator.retry(&assignments[0]).unwrap();
    }
    assert!(drain(&mut coordinator).is_empty());
    assert_eq!(coordinator.pending_requests().len(), 1);
    assert!(coordinator.cancel_pending("clone"));
    assert!(coordinator.pending_requests().is_empty());
}
