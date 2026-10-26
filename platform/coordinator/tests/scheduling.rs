use adx_coordinator::{Coordinator, Node, Placement, TenantQueue};
use adx_core::{EnvironmentSpec, Error, Resources};

fn resources(cpu: u64) -> Resources {
    Resources {
        cpu_millis: cpu,
        memory_bytes: 1024,
        disk_bytes: 1024,
    }
}

fn spec(id: &str, tenant: &str, priority: i32) -> EnvironmentSpec {
    EnvironmentSpec {
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: id.into(),
        tenant_id: tenant.into(),
        image: "test-image".into(),
        runtime_class: "test-runtime".into(),
        resources: resources(1),
        priority,
        scheduling: Default::default(),
        execution: Default::default(),
    }
}

fn node(id: &str, capacity: u64) -> Node {
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
            cpu_millis: capacity,
            memory_bytes: 10000,
            disk_bytes: 10000,
        },
        available: true,
        labels: [("NODE_ID".into(), id.into())].into(),
        devices: vec![],
    }
}

#[test]
fn volume_waiting_skips_a_high_priority_request_and_release_wakes_it() {
    use adx_coordinator::VolumePlacement;
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.register(node("n1", 4)).unwrap();
    coordinator.submit(spec("blocked", "t", 10)).unwrap();
    coordinator.set_volume_placement(
        "blocked",
        VolumePlacement {
            storage_domains: Default::default(),
            nodes: ["n1".into()].into(),
            waiting_for: Some("uid".into()),
        },
    );
    coordinator.submit(spec("other", "t", 0)).unwrap();
    let round = coordinator.schedule_round(0).unwrap();
    assert_eq!(round.assignments.len(), 1);
    assert_eq!(round.assignments[0].environment_id, "other");
    assert_eq!(coordinator.pending(0).unwrap(), 1);
    assert_eq!(
        coordinator
            .scheduling_failure(&spec("blocked", "t", 10))
            .unwrap()
            .rejections
            .get("waiting_for_volume"),
        Some(&1)
    );
    coordinator.set_volume_placement(
        "blocked",
        VolumePlacement {
            storage_domains: Default::default(),
            nodes: ["n1".into()].into(),
            waiting_for: None,
        },
    );
    let shard = coordinator.take_ready_shard().unwrap();
    assert_eq!(
        coordinator.schedule(shard).unwrap().unwrap().environment_id,
        "blocked"
    );
}

#[test]
fn volume_topology_filters_local_and_cached_central_candidates_per_request() {
    use adx_coordinator::VolumePlacement;
    let mut coordinator = Coordinator::new(1, Placement::Spread).unwrap();
    coordinator.register(node("n1", 4)).unwrap();
    coordinator.register(node("n2", 4)).unwrap();
    let restricted = spec("restricted", "t", 10);
    coordinator.set_volume_placement(
        &restricted.id,
        VolumePlacement {
            storage_domains: Default::default(),
            nodes: ["n2".into()].into(),
            waiting_for: None,
        },
    );
    assert!(!coordinator.local_candidate(&restricted, "n1", &[]).unwrap());
    assert!(coordinator.local_candidate(&restricted, "n2", &[]).unwrap());
    coordinator.submit(restricted).unwrap();
    coordinator.submit(spec("other", "t", 0)).unwrap();
    let round = coordinator.schedule_round(0).unwrap();
    assert_eq!(round.assignments.len(), 2);
    assert_eq!(round.assignments[0].node_id, "n2");
    assert_eq!(round.assignments[1].node_id, "n1");
}

#[test]
fn global_round_robin_shards_and_balanced_node_registration() {
    let mut coordinator = Coordinator::new(2, Placement::Spread).unwrap();
    assert_eq!(coordinator.register(node("n1", 2)).unwrap(), 0);
    assert_eq!(coordinator.register(node("n2", 2)).unwrap(), 1);
    assert_eq!(coordinator.register(node("n3", 2)).unwrap(), 0);
    assert_eq!(coordinator.register(node("n4", 2)).unwrap(), 1);
    assert_eq!(coordinator.register(node("n1", 2)).unwrap(), 0);
    let a = coordinator.submit(spec("a", "t", 0)).unwrap();
    let b = coordinator.submit(spec("b", "t", 0)).unwrap();
    assert_eq!((a, b), (0, 1));
    assert_eq!(coordinator.schedule(0).unwrap().unwrap().node_id, "n1");
    assert_eq!(coordinator.schedule(1).unwrap().unwrap().node_id, "n2");
}

#[test]
fn reservations_prevent_overcommit_and_release_wakes_waiting_work() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.register(node("n1", 1)).unwrap();
    coordinator.submit(spec("a", "t", 0)).unwrap();
    coordinator.submit(spec("b", "t", 0)).unwrap();
    let a = coordinator.schedule(0).unwrap().unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
    coordinator.release(&a).unwrap();
    assert_eq!(
        coordinator.schedule(0).unwrap().unwrap().environment_id,
        "b"
    );
    assert_eq!(coordinator.release(&a), Err(Error::Conflict));
}

#[test]
fn maintenance_preserves_existing_reservations_and_blocks_new_ones() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.register(node("n1", 2)).unwrap();
    coordinator.submit(spec("a", "t", 0)).unwrap();
    let a = coordinator.schedule(0).unwrap().unwrap();
    let mut update = node("n1", 2);
    update.available = false;
    coordinator.register(update).unwrap();
    coordinator.submit(spec("b", "t", 0)).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
    coordinator.release(&a).unwrap();
}

#[test]
fn tenant_round_robin_with_priority_and_fifo_inside_tenant() {
    let mut queue = TenantQueue::default();
    for request in [
        spec("a-low", "a", 0),
        spec("a-high-1", "a", 5),
        spec("a-high-2", "a", 5),
        spec("b", "b", 0),
    ] {
        queue.push(request);
    }
    let ids: Vec<_> = std::iter::from_fn(|| queue.pop()).map(|x| x.id).collect();
    assert_eq!(ids, ["a-high-1", "b", "a-high-2", "a-low"]);
}

#[test]
fn pending_requests_expose_identity_and_enqueue_time_without_dequeueing() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.submit(spec("waiting", "tenant", 3)).unwrap();
    let pending = coordinator.pending_requests();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].spec.id, "waiting");
    assert!(pending[0].enqueue_time_millis > 0);
    assert_eq!(coordinator.pending(0).unwrap(), 1);
}

#[test]
fn duplicate_submission_is_idempotent_but_changed_spec_conflicts() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.register(node("n1", 2)).unwrap();
    let request = spec("a", "t", 0);
    assert_eq!(coordinator.submit(request.clone()).unwrap(), 0);
    assert_eq!(coordinator.submit(request).unwrap(), 0);
    assert_eq!(
        coordinator.submit(spec("a", "another-tenant", 0)),
        Err(Error::Conflict)
    );
    assert!(coordinator.schedule(0).unwrap().is_some());
    assert!(coordinator.schedule(0).unwrap().is_none());
}

#[test]
fn zero_shards_and_zero_resource_requests_are_rejected() {
    assert!(Coordinator::new(0, Placement::Pack).is_err());
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    for (cpu_millis, memory_bytes) in [(0, 1024), (1, 0), (0, 0)] {
        let mut request = spec("a", "t", 0);
        request.resources.cpu_millis = cpu_millis;
        request.resources.memory_bytes = memory_bytes;
        assert!(coordinator.submit(request).is_err());
    }
}

#[test]
fn zero_capacity_reports_preserve_reservations_until_capacity_recovers() {
    for (cpu_millis, memory_bytes) in [(0, 10000), (2, 0), (0, 0)] {
        let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
        coordinator.register(node("n", 2)).unwrap();
        coordinator.submit(spec("running", "t", 0)).unwrap();
        let running = coordinator.schedule(0).unwrap().unwrap();
        let mut zero = node("n", 2);
        zero.capacity.cpu_millis = cpu_millis;
        zero.capacity.memory_bytes = memory_bytes;
        coordinator.register(zero.clone()).unwrap();
        assert_eq!(
            coordinator.node_resources("n").unwrap(),
            (zero.capacity, zero.capacity.saturating_sub(resources(1)))
        );
        assert_eq!(coordinator.snapshot().nodes()["n"].capacity, zero.capacity);
        coordinator.submit(spec("waiting", "t", 0)).unwrap();
        assert!(coordinator.schedule(0).unwrap().is_none());
        // Recovery to the occupied capacity must not erase the running reservation.
        coordinator.register(node("n", 1)).unwrap();
        assert!(coordinator.schedule(0).unwrap().is_none());
        coordinator.register(node("n", 2)).unwrap();
        let waiting = coordinator.schedule(0).unwrap().unwrap();
        assert_eq!(waiting.environment_id, "waiting");
        coordinator.release(&running).unwrap();
        coordinator.release(&waiting).unwrap();
        assert_eq!(
            coordinator.node_resources("n").unwrap(),
            (node("n", 2).capacity, node("n", 2).capacity)
        );
    }
}

#[test]
fn capacity_below_used_blocks_placement_until_normal_release() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.register(node("n", 3)).unwrap();
    coordinator.submit(spec("a", "t", 0)).unwrap();
    coordinator.submit(spec("b", "t", 0)).unwrap();
    let a = coordinator.schedule(0).unwrap().unwrap();
    let b = coordinator.schedule(0).unwrap().unwrap();
    coordinator.register(node("n", 1)).unwrap();
    let (capacity, available) = coordinator.node_resources("n").unwrap();
    assert_eq!(capacity.cpu_millis, 1);
    assert_eq!(available.cpu_millis, 0);
    coordinator.submit(spec("c", "t", 0)).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
    coordinator.release(&a).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
    coordinator.release(&b).unwrap();
    assert_eq!(
        coordinator.schedule(0).unwrap().unwrap().environment_id,
        "c"
    );
}

#[test]
fn pack_and_spread_choose_different_resource_fits() {
    for (policy, expected) in [(Placement::Pack, "small"), (Placement::Spread, "large")] {
        let mut coordinator = Coordinator::new(1, policy).unwrap();
        coordinator.register(node("small", 2)).unwrap();
        coordinator.register(node("large", 4)).unwrap();
        coordinator.submit(spec("a", "t", 0)).unwrap();
        assert_eq!(coordinator.schedule(0).unwrap().unwrap().node_id, expected);
    }
}

#[test]
fn stale_release_cannot_remove_a_new_assignment_for_the_same_id() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.register(node("n", 1)).unwrap();
    coordinator.submit(spec("a", "t", 0)).unwrap();
    let first = coordinator.schedule(0).unwrap().unwrap();
    coordinator.release(&first).unwrap();
    coordinator.submit(spec("a", "t", 0)).unwrap();
    let second = coordinator.schedule(0).unwrap().unwrap();
    assert!(second.generation > first.generation);
    assert_eq!(coordinator.release(&first), Err(Error::Conflict));
    coordinator.submit(spec("b", "t", 0)).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_none());
    coordinator.release(&second).unwrap();
    assert!(coordinator.schedule(0).unwrap().is_some());
}

#[test]
fn unschedulable_tenant_does_not_block_a_fitting_other_tenant() {
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.register(node("n", 1)).unwrap();
    let mut large = spec("a", "tenant-a", 10);
    large.resources.cpu_millis = 10;
    coordinator.submit(large).unwrap();
    coordinator.submit(spec("b", "tenant-b", 0)).unwrap();
    assert_eq!(
        coordinator.schedule(0).unwrap().unwrap().environment_id,
        "b"
    );
    assert!(coordinator.schedule(0).unwrap().is_none());
}

#[test]
fn locally_claimed_owner_updates_capacity_queue_and_generation_exactly_once() {
    use adx_core::Assignment;
    let mut coordinator = Coordinator::new(1, Placement::Spread).unwrap();
    coordinator.register(node("n1", 1)).unwrap();
    coordinator.register(node("n2", 1)).unwrap();
    let local = spec("local", "t", 0);
    coordinator.submit(local.clone()).unwrap(); // A concurrent center request is already queued.
    let assignment = Assignment {
        environment_id: "local".into(),
        node_id: "n1".into(),
        shard_id: 0,
        generation: 77,
        devices: vec![],
    };
    coordinator.accept_claim(&local, &assignment).unwrap();
    coordinator.accept_claim(&local, &assignment).unwrap();
    assert_eq!(coordinator.pending(0).unwrap(), 0);
    coordinator.submit(spec("next", "t", 0)).unwrap();
    let next = coordinator.schedule(0).unwrap().unwrap();
    assert_eq!(next.node_id, "n2");
    assert_eq!(next.generation, 78);
    coordinator.release(&assignment).unwrap();
    assert_eq!(coordinator.snapshot().environments().len(), 1);
}

#[test]
fn local_candidate_obeys_hard_constraints_and_occupancy_without_global_score() {
    use adx_core::scheduling::LabelSelector;
    let mut coordinator = Coordinator::new(1, Placement::Pack).unwrap();
    coordinator.register(node("n1", 1)).unwrap();
    coordinator.register(node("n2", 2)).unwrap();
    let mut request = spec("local", "t", 0);
    request.scheduling.required_node = vec![LabelSelector {
        match_labels: [("NODE_ID".into(), "n2".into())].into(),
        ..Default::default()
    }];
    assert!(!coordinator.local_candidate(&request, "n1", &[]).unwrap());
    assert!(coordinator.local_candidate(&request, "n2", &[]).unwrap());
    let assignment = adx_core::Assignment {
        environment_id: request.id.clone(),
        node_id: "n2".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    coordinator.accept_claim(&request, &assignment).unwrap();
    let mut too_large = spec("other", "t", 0);
    too_large.resources.cpu_millis = 2;
    assert!(!coordinator.local_candidate(&too_large, "n2", &[]).unwrap());
}

#[test]
fn queued_volume_request_uses_a_newly_accessible_node_on_another_shard() {
    use adx_coordinator::VolumePlacement;
    let mut coordinator = Coordinator::new(2, Placement::Pack).unwrap();
    assert_eq!(coordinator.register(node("a", 0)).unwrap(), 0);
    assert_eq!(coordinator.register(node("b", 0)).unwrap(), 1);
    // Rotate the second request into b's shard before c exists.
    assert_eq!(coordinator.submit(spec("unrelated", "t", 0)).unwrap(), 0);
    assert_eq!(coordinator.submit(spec("waiting", "t", 0)).unwrap(), 1);
    coordinator.set_volume_placement(
        "waiting",
        VolumePlacement {
            storage_domains: ["sfs".into()].into(),
            nodes: ["a".into(), "b".into()].into(),
            waiting_for: None,
        },
    );
    while let Some(shard) = coordinator.take_ready_shard() {
        assert!(coordinator
            .schedule_round(shard)
            .unwrap()
            .assignments
            .is_empty());
    }
    assert_eq!(coordinator.register(node("c", 4)).unwrap(), 0);
    coordinator.set_volume_placement(
        "waiting",
        VolumePlacement {
            storage_domains: ["sfs".into()].into(),
            nodes: ["c".into()].into(),
            waiting_for: None,
        },
    );
    let mut assignments = Vec::new();
    while let Some(shard) = coordinator.take_ready_shard() {
        assignments.extend(coordinator.schedule_round(shard).unwrap().assignments);
    }
    assert_eq!(assignments.len(), 2);
    let assigned = assignments
        .iter()
        .find(|a| a.environment_id == "waiting")
        .unwrap();
    assert_eq!(assigned.node_id, "c");
    assert_eq!(assigned.shard_id, 0);
    assert!(coordinator.pending_requests().is_empty());
}
