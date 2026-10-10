use adx_coordinator::{Coordinator, Node, Placement, SchedulerConfig};
use adx_core::{EnvironmentSpec, Resources};
use std::{sync::Arc, time::Duration};
fn node(id: &str, cpu: u64) -> Node {
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
            cpu_millis: cpu,
            memory_bytes: cpu,
            disk_bytes: 0,
        },
        available: true,
        labels: Default::default(),
        devices: vec![],
    }
}
fn spec(id: &str) -> EnvironmentSpec {
    EnvironmentSpec {
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: id.into(),
        tenant_id: "t".into(),
        image: "i".into(),
        runtime_class: "r".into(),
        priority: 0,
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        },
        scheduling: Default::default(),
        sandbox: Default::default(),
    }
}
#[test]
fn snapshots_share_unchanged_nodes_and_keep_old_placements_immutable() {
    let mut m = Coordinator::new(1, Placement::Spread).unwrap();
    m.register(node("a", 10)).unwrap();
    m.register(node("b", 10)).unwrap();
    let before = m.snapshot();
    m.submit(spec("i")).unwrap();
    let a = m.schedule(0).unwrap().unwrap();
    let after = m.snapshot();
    assert!(Arc::ptr_eq(
        before.nodes().get("b").unwrap(),
        after.nodes().get("b").unwrap()
    ));
    assert!(before.environments().is_empty());
    assert_eq!(after.environments().len(), 1);
    m.release(&a).unwrap();
    assert_eq!(after.environments().len(), 1);
    assert!(m.snapshot().environments().is_empty());
}
#[test]
fn bounded_rounds_progress_past_an_unfittable_prefix_without_draining_the_queue() {
    let config = SchedulerConfig {
        max_attempts: 3,
        max_duration: Duration::from_secs(1),
        ..Default::default()
    };
    let mut m = Coordinator::with_config(1, Placement::Pack, config).unwrap();
    m.register(node("n", 1)).unwrap();
    for i in 0..8 {
        let mut r = spec(&format!("big-{i}"));
        r.resources.cpu_millis = 2;
        m.submit(r).unwrap();
    }
    m.submit(spec("fit")).unwrap();
    for _ in 0..2 {
        let round = m.schedule_round(0).unwrap();
        assert_eq!(round.attempted, 3);
        assert!(round.assignments.is_empty());
        assert!(round.yielded);
    }
    let round = m.schedule_round(0).unwrap();
    assert_eq!(round.assignments[0].environment_id, "fit");
    assert_eq!(m.pending(0).unwrap(), 8);
}
#[test]
fn cached_candidates_preserve_spread_and_reconcile_release_and_status_updates() {
    let mut m = Coordinator::new(1, Placement::Spread).unwrap();
    m.register(node("a", 4)).unwrap();
    m.register(node("b", 4)).unwrap();
    for i in 0..4 {
        m.submit(spec(&format!("i-{i}"))).unwrap();
    }
    let round = m.schedule_round(0).unwrap();
    assert_eq!(
        round
            .assignments
            .iter()
            .map(|a| a.node_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b", "a", "b"]
    );
    let stats = m.stats(0).unwrap();
    assert!(stats.cache_hits >= 3);
    assert!(stats.candidate_evaluations < 8);
    m.release(&round.assignments[0]).unwrap();
    let mut b = node("b", 4);
    b.available = false;
    m.register(b).unwrap();
    m.submit(spec("next")).unwrap();
    assert_eq!(m.schedule(0).unwrap().unwrap().node_id, "a");
}
#[test]
fn publication_precedes_wakeup_and_stalled_shards_do_not_spin() {
    let mut m = Coordinator::new(1, Placement::Pack).unwrap();
    m.register(node("n", 1)).unwrap();
    let mut n = node("n", 1);
    n.available = false;
    m.register(n).unwrap();
    m.submit(spec("i")).unwrap();
    assert_eq!(m.take_ready_shard(), Some(0));
    assert!(m.schedule_round(0).unwrap().assignments.is_empty());
    assert_eq!(m.take_ready_shard(), None);
    let old = m.snapshot();
    m.register(node("n", 1)).unwrap();
    assert_eq!(m.take_ready_shard(), Some(0));
    assert!(m.snapshot().revision > old.revision);
    assert!(m.snapshot().node("n").unwrap().available);
    assert_eq!(m.schedule_round(0).unwrap().assignments.len(), 1);
}
#[test]
fn journal_overflow_rebuilds_candidates_instead_of_using_stale_capacity() {
    let config = SchedulerConfig {
        mutation_history: 2,
        ..Default::default()
    };
    let mut m = Coordinator::with_config(1, Placement::Spread, config).unwrap();
    m.register(node("n", 10)).unwrap();
    m.submit(spec("first")).unwrap();
    let a = m.schedule(0).unwrap().unwrap();
    for cpu in [9, 8, 7, 6] {
        m.register(node("n", cpu)).unwrap();
    }
    m.release(&a).unwrap();
    m.submit(spec("next")).unwrap();
    assert!(m.schedule(0).unwrap().is_some());
    assert!(m.stats(0).unwrap().journal_rebuilds > 0);
}
#[test]
fn optimization_matches_uncached_selection_under_mixed_mutations() {
    for policy in [Placement::Pack, Placement::Spread] {
        let mut fast = Coordinator::new(2, policy).unwrap();
        let mut slow = Coordinator::with_config(
            2,
            policy,
            SchedulerConfig {
                candidate_cache_entries: 0,
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..12 {
            fast.register(node(&format!("n{i:02}"), 8 + i)).unwrap();
            slow.register(node(&format!("n{i:02}"), 8 + i)).unwrap();
        }
        let mut held = vec![];
        for i in 0..160 {
            if i % 7 == 0 && !held.is_empty() {
                let a = held.remove(0);
                fast.release(&a).unwrap();
                slow.release(&a).unwrap();
            }
            let mut r = spec(&format!("i{i}"));
            r.resources.cpu_millis = 1 + i % 3;
            r.tenant_id = format!("t{}", i % 3);
            let d = fast.submit(r.clone()).unwrap();
            assert_eq!(slow.submit(r).unwrap(), d);
            let a = fast.schedule(d).unwrap();
            assert_eq!(a, slow.schedule(d).unwrap());
            if let Some(a) = a {
                held.push(a);
            }
        }
    }
}

#[test]
fn stalled_fifo_is_preserved_when_capacity_returns_before_a_new_submission() {
    let mut m = Coordinator::new(1, Placement::Pack).unwrap();
    let mut unavailable = node("n", 1);
    unavailable.available = false;
    m.register(unavailable).unwrap();
    m.submit(spec("older")).unwrap();
    assert!(m.schedule(0).unwrap().is_none());
    m.register(node("n", 1)).unwrap();
    m.submit(spec("newer")).unwrap();
    assert_eq!(m.schedule(0).unwrap().unwrap().environment_id, "older");
}
