use adx_core::{scheduling::*, Error, InstanceSpec, Resources, Result};
use adx_master::{Master, Placement, SchedulerConfig};
use adx_scheduling::{Candidate, Framework, Score, WeightedScore};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
fn spec(id: &str) -> InstanceSpec {
    InstanceSpec {
        runtime_environment: None,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: id.into(),
        tenant_id: "t".into(),
        image: "i".into(),
        runtime: "r".into(),
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
fn node(id: &str) -> Node {
    Node {
        id: id.into(),
        capacity: Resources {
            cpu_millis: 100,
            memory_bytes: 100,
            disk_bytes: 100,
        },
        available: true,
        labels: [("host".into(), id.into()), ("zone".into(), id.into())].into(),
        devices: vec![],
    }
}
fn term() -> PeerTerm {
    PeerTerm {
        selector: Default::default(),
        topology_key: "host".into(),
        tenants: vec![],
    }
}
#[test]
fn unsafe_policies_and_custom_profiles_never_enter_semantic_cache() {
    for variant in 0..4 {
        let mut m = Master::new(1, Placement::Spread).unwrap();
        m.register(node("a")).unwrap();
        m.register(node("b")).unwrap();
        for i in 0..3 {
            let mut r = spec(&format!("{i}"));
            match variant {
                0 => r.resources.disk_bytes = 1,
                1 => {
                    r.scheduling.labels.insert("app".into(), "x".into());
                }
                2 => r.scheduling.required_node.push(LabelSelector::default()),
                3 => r.scheduling.devices.push(DeviceRequest {
                    kind: DeviceKind::Gpu,
                    model: None,
                    count: 1,
                }),
                _ => unreachable!(),
            }
            m.submit(r).unwrap();
        }
        m.schedule_round(0).unwrap();
        assert_eq!(m.stats(0).unwrap().cache_hits, 0);
        assert_eq!(m.stats(0).unwrap().cache_builds, 0);
    }
    // A name resembling a builtin must not opt an unknown plugin into reuse.
    struct Spoof;
    impl Score for Spoof {
        fn name(&self) -> &'static str {
            "resource-balance"
        }
        fn score(&self, _: &InstanceSpec, _: &Candidate<'_>) -> Result<u32> {
            Ok(0)
        }
    }
    let f = Framework::new(
        vec![],
        vec![WeightedScore::new(Arc::new(Spoof), 1).unwrap()],
    )
    .unwrap();
    let mut m = Master::with_framework(1, f).unwrap();
    m.register(node("a")).unwrap();
    for i in 0..3 {
        m.submit(spec(&i.to_string())).unwrap();
    }
    m.schedule_round(0).unwrap();
    assert_eq!(m.stats(0).unwrap().cache_builds, 0);
}
#[test]
fn cached_scalar_request_respects_new_cross_shard_reverse_anti_affinity() {
    let mut m = Master::new(2, Placement::Pack).unwrap();
    let mut a = node("a");
    a.labels.insert("host".into(), "shared".into());
    let mut b = node("b");
    b.labels.insert("host".into(), "shared".into());
    m.register(a).unwrap();
    m.register(b).unwrap();
    m.submit(spec("warm")).unwrap();
    let first = m.schedule(0).unwrap().unwrap();
    m.release(&first).unwrap();
    let mut owner = spec("owner");
    owner.scheduling.required_anti_affinity.push(term());
    m.submit(owner).unwrap();
    let owned = m.schedule(1).unwrap().unwrap();
    m.submit(spec("next")).unwrap();
    assert!(m.schedule(0).unwrap().is_none());
    m.release(&owned).unwrap();
    assert!(m.schedule(0).unwrap().is_some());
}
#[test]
fn bounded_round_applies_peer_reservations_between_requests() {
    let mut m = Master::new(1, Placement::Pack).unwrap();
    m.register(node("a")).unwrap();
    m.register(node("b")).unwrap();
    for i in 0..3 {
        let mut r = spec(&i.to_string());
        r.scheduling.required_anti_affinity.push(term());
        m.submit(r).unwrap();
    }
    let r = m.schedule_round(0).unwrap();
    assert_eq!(r.assignments.len(), 2);
    assert_eq!(r.assignments[0].node_id, "a");
    assert_eq!(r.assignments[1].node_id, "b");
    assert_eq!(m.pending(0).unwrap(), 1);
}
#[test]
fn aggregation_keeps_tenant_rotation_priority_and_fifo() {
    let mut m = Master::new(1, Placement::Spread).unwrap();
    m.register(node("a")).unwrap();
    for (id, tenant, priority) in [
        ("a-low", "a", 0),
        ("a-high-1", "a", 5),
        ("a-high-2", "a", 5),
        ("b1", "b", 0),
        ("b2", "b", 0),
    ] {
        let mut r = spec(id);
        r.tenant_id = tenant.into();
        r.priority = priority;
        m.submit(r).unwrap();
    }
    let r = m.schedule_round(0).unwrap();
    assert_eq!(m.stats(0).unwrap().cache_builds, 1);
    assert_eq!(m.stats(0).unwrap().cache_hits, 4);
    assert_eq!(
        r.assignments
            .iter()
            .map(|a| a.instance_id.as_str())
            .collect::<Vec<_>>(),
        ["a-high-1", "b1", "a-high-2", "b2", "a-low"]
    );
}
struct FailSecond(Arc<AtomicUsize>);
impl Score for FailSecond {
    fn name(&self) -> &'static str {
        "fail-second"
    }
    fn score(&self, _: &InstanceSpec, _: &Candidate<'_>) -> Result<u32> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 1 {
            Err(Error::Unavailable("fixture".into()))
        } else {
            Ok(0)
        }
    }
}
#[test]
fn partial_round_failure_returns_committed_assignments_and_keeps_failed_request() {
    let calls = Arc::new(AtomicUsize::new(0));
    let f = Framework::new(
        vec![],
        vec![WeightedScore::new(Arc::new(FailSecond(calls)), 1).unwrap()],
    )
    .unwrap();
    let mut m = Master::with_framework(1, f).unwrap();
    m.register(node("a")).unwrap();
    for i in 0..3 {
        m.submit(spec(&i.to_string())).unwrap();
    }
    let r = m.schedule_round(0).unwrap();
    assert_eq!(r.assignments.len(), 1);
    assert!(r.error.is_some());
    assert_eq!(m.snapshot().instances().len(), 1);
    assert_eq!(m.pending(0).unwrap(), 2);
    let next = m.schedule_round(0).unwrap();
    assert_eq!(
        next.assignments
            .iter()
            .map(|a| a.instance_id.as_str())
            .collect::<Vec<_>>(),
        ["1", "2"]
    );
    m.release(&r.assignments[0]).unwrap();
    assert_eq!(m.snapshot().instances().len(), 2);
}
#[test]
fn time_budget_yields_between_requests_without_losing_work() {
    struct Slow;
    impl Score for Slow {
        fn name(&self) -> &'static str {
            "slow"
        }
        fn score(&self, _: &InstanceSpec, _: &Candidate<'_>) -> Result<u32> {
            std::thread::sleep(Duration::from_millis(5));
            Ok(0)
        }
    }
    let f = Framework::new(vec![], vec![WeightedScore::new(Arc::new(Slow), 1).unwrap()]).unwrap();
    let config = SchedulerConfig {
        max_duration: Duration::from_millis(2),
        ..Default::default()
    };
    let mut m = Master::with_framework_config(1, f, config).unwrap();
    m.register(node("a")).unwrap();
    for i in 0..3 {
        m.submit(spec(&i.to_string())).unwrap();
    }
    let r = m.schedule_round(0).unwrap();
    assert_eq!(r.assignments.len(), 1);
    assert!(r.yielded);
    assert_eq!(m.pending(0).unwrap(), 2);
}
#[test]
fn different_signatures_do_not_share_candidates_and_cache_capacity_is_bounded() {
    let config = SchedulerConfig {
        candidate_cache_entries: 1,
        ..Default::default()
    };
    let mut m = Master::with_config(1, Placement::Pack, config).unwrap();
    m.register(node("a")).unwrap();
    for (i, runtime) in ["r", "other", "r"].iter().enumerate() {
        let mut r = spec(&i.to_string());
        r.runtime = runtime.to_string();
        m.submit(r).unwrap();
        m.schedule(0).unwrap();
    }
    let s = m.stats(0).unwrap();
    assert_eq!(s.cache_builds, 3);
    assert_eq!(s.cache_hits, 0);
}
#[test]
fn invalid_limits_are_rejected() {
    for c in [
        SchedulerConfig {
            max_attempts: 0,
            ..Default::default()
        },
        SchedulerConfig {
            max_duration: Duration::ZERO,
            ..Default::default()
        },
        SchedulerConfig {
            mutation_history: 0,
            ..Default::default()
        },
    ] {
        assert!(Master::with_config(1, Placement::Pack, c).is_err());
    }
}
