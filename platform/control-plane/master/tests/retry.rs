use adx_core::{Error, InstanceSpec, Resources};
use adx_master::{Master, Node, Placement};
fn spec() -> InstanceSpec {
    InstanceSpec {
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: "r".into(),
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
    }
}
#[test]
fn rejected_allocation_retries_another_node_without_leaking_or_overwriting_a_new_generation() {
    let mut m = Master::new(1, Placement::Pack).unwrap();
    for id in ["a", "b"] {
        m.register(Node {
            id: id.into(),
            capacity: spec().resources,
            available: true,
            labels: Default::default(),
            devices: vec![],
        })
        .unwrap();
    }
    m.submit(spec()).unwrap();
    let first = m.schedule(0).unwrap().unwrap();
    m.retry(&first).unwrap();
    assert_eq!(m.retry(&first), Err(Error::Conflict));
    let second = m.schedule(0).unwrap().unwrap();
    assert_ne!(first.node_id, second.node_id);
    assert!(second.generation > first.generation);
    assert_eq!(m.snapshot().instances().len(), 1);
    assert!(m.stats(0).unwrap().cache_hits > 0);
    assert_eq!(m.release(&first), Err(Error::Conflict));
    m.retry(&second).unwrap();
    assert!(m.schedule(0).unwrap().is_none());
    assert!(m.snapshot().instances().is_empty());
    let mut other = spec();
    other.id = "other".into();
    m.submit(other).unwrap();
    assert!(m.schedule(0).unwrap().is_some());
}
