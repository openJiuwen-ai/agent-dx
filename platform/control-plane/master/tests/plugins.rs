use adx_core::{Error, InstanceSpec, Resources, Result};
use adx_master::{Master, Node};
use adx_scheduling::{Candidate, Filter, Framework, Score, WeightedScore};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

fn spec(id: &str) -> InstanceSpec {
    InstanceSpec {
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        scheduling: Default::default(),
        id: id.into(),
        tenant_id: "t".into(),
        image: "image".into(),
        runtime: "runsc".into(),
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 1,
        },
        priority: 0,
    }
}
fn node(id: &str, available: bool) -> Node {
    Node {
        labels: Default::default(),
        devices: vec![],
        id: id.into(),
        capacity: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 1,
        },
        available,
    }
}
struct ExcludeA;
impl Filter for ExcludeA {
    fn name(&self) -> &'static str {
        "exclude-a"
    }
    fn filter(&self, _: &InstanceSpec, node: &Candidate<'_>) -> Result<bool> {
        Ok(node.node.id != "a")
    }
}
struct Prefer {
    name: &'static str,
    node: &'static str,
    points: u32,
}
impl Score for Prefer {
    fn name(&self) -> &'static str {
        self.name
    }
    fn score(&self, _: &InstanceSpec, candidate: &Candidate<'_>) -> Result<u32> {
        assert!(
            candidate.node.available,
            "unavailable nodes must not be scored"
        );
        Ok(if candidate.node.id == self.node {
            self.points
        } else {
            0
        })
    }
}
fn weighted(name: &'static str, node: &'static str, points: u32, weight: u32) -> WeightedScore {
    WeightedScore::new(Arc::new(Prefer { name, node, points }), weight).unwrap()
}
#[test]
fn domain_uses_filters_before_weighted_scores_and_keeps_admission_guards() {
    let framework = Framework::new(
        vec![Arc::new(ExcludeA)],
        vec![
            weighted("prefer-a", "a", 100, 1),
            weighted("prefer-b", "b", 10, 1),
        ],
    )
    .unwrap();
    let mut master = Master::with_framework(1, framework).unwrap();
    for n in [node("a", true), node("b", true), node("c", false)] {
        master.register(n).unwrap();
    }
    master.submit(spec("first")).unwrap();
    let first = master.schedule(0).unwrap().unwrap();
    assert_eq!(first.node_id, "b");
    master.submit(spec("waiting")).unwrap();
    assert!(
        master.schedule(0).unwrap().is_none(),
        "custom profile must preserve capacity checks"
    );
    master.release(&first).unwrap();
    assert_eq!(master.schedule(0).unwrap().unwrap().instance_id, "waiting");
}
#[test]
fn weights_change_selection_and_ties_are_deterministic() {
    for (weight, expected) in [(1, "a"), (3, "b")] {
        let framework = Framework::new(
            vec![],
            vec![
                weighted("prefer-a", "a", 20, 1),
                weighted("prefer-b", "b", 10, weight),
            ],
        )
        .unwrap();
        let mut master = Master::with_framework(1, framework).unwrap();
        for id in ["b", "a"] {
            master.register(node(id, true)).unwrap();
        }
        master.submit(spec("i")).unwrap();
        assert_eq!(master.schedule(0).unwrap().unwrap().node_id, expected);
    }
    let framework = Framework::new(vec![], vec![]).unwrap();
    let mut master = Master::with_framework(1, framework).unwrap();
    for id in ["b", "a"] {
        master.register(node(id, true)).unwrap();
    }
    master.submit(spec("i")).unwrap();
    assert_eq!(master.schedule(0).unwrap().unwrap().node_id, "a");
}
struct FallibleScore(Arc<AtomicBool>);
impl Score for FallibleScore {
    fn name(&self) -> &'static str {
        "fallible"
    }
    fn score(&self, _: &InstanceSpec, _: &Candidate<'_>) -> Result<u32> {
        if self.0.load(Ordering::SeqCst) {
            Err(Error::Unavailable("scoring dependency".into()))
        } else {
            Ok(0)
        }
    }
}
#[test]
fn plugin_error_does_not_drop_queued_requests_or_reserve_capacity() {
    let failing = Arc::new(AtomicBool::new(true));
    let framework = Framework::new(
        vec![],
        vec![WeightedScore::new(Arc::new(FallibleScore(failing.clone())), 1).unwrap()],
    )
    .unwrap();
    let mut master = Master::with_framework(1, framework).unwrap();
    master.register(node("a", true)).unwrap();
    master.submit(spec("first")).unwrap();
    master.submit(spec("second")).unwrap();
    assert!(matches!(master.schedule(0), Err(Error::Unavailable(_))));
    failing.store(false, Ordering::SeqCst);
    let first = master.schedule(0).unwrap().unwrap();
    assert_eq!(first.instance_id, "first");
    master.release(&first).unwrap();
    assert_eq!(master.schedule(0).unwrap().unwrap().instance_id, "second");
}
