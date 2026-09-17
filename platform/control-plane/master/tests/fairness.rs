use adx_core::{InstanceSpec, Resources};
use adx_master::{Master, Node, Placement, SchedulerConfig};
use std::time::Duration;

fn request(id: &str, tenant: &str) -> InstanceSpec {
    InstanceSpec {
        id: id.into(),
        tenant_id: tenant.into(),
        image: "image".into(),
        runtime: "runc".into(),
        resources: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        },
        priority: 0,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        scheduling: Default::default(),
    }
}
fn node(available: bool) -> Node {
    Node {
        id: "node".into(),
        capacity: Resources {
            cpu_millis: 1,
            memory_bytes: 1,
            disk_bytes: 0,
        },
        available,
        labels: Default::default(),
        devices: vec![],
    }
}

#[test]
fn recovered_capacity_revisits_old_tenant_under_continuous_arrivals() {
    for cache in [0, 32] {
        let config = SchedulerConfig {
            max_attempts: 1,
            max_duration: Duration::from_secs(1),
            candidate_cache_entries: cache,
            ..Default::default()
        };
        let mut master = Master::with_config(1, Placement::Pack, config).unwrap();
        master.register(node(false)).unwrap();
        master.submit(request("old", "old-tenant")).unwrap();
        for n in 0..2 {
            master
                .submit(request(&format!("initial-{n}"), "busy"))
                .unwrap();
        }
        assert!(master.schedule_round(0).unwrap().assignments.is_empty());
        master.register(node(true)).unwrap();
        let mut served = false;
        // The finite original sweep has only two remaining tickets. New arrivals
        // must not extend it indefinitely and keep the recovered old tenant asleep.
        for round in 0..3 {
            for n in 0..2 {
                master
                    .submit(request(&format!("new-{round}-{n}"), "busy"))
                    .unwrap();
            }
            let outcome = master.schedule_round(0).unwrap();
            assert!(outcome.attempted <= 1);
            for allocation in outcome.assignments {
                served |= allocation.instance_id == "old";
                master.release(&allocation).unwrap();
            }
        }
        assert!(
            served,
            "recovered old tenant starved with candidate_cache_entries={cache}"
        );
    }
}

#[test]
fn fresh_work_deferred_behind_blocked_sweep_remains_awake_then_sleeps() {
    let mut master = Master::with_config(
        1,
        Placement::Pack,
        SchedulerConfig {
            max_attempts: 1,
            max_duration: Duration::from_secs(1),
            ..Default::default()
        },
    )
    .unwrap();
    master.register(node(true)).unwrap();
    for n in 0..2 {
        let mut blocked = request(&format!("blocked-{n}"), "old");
        blocked.resources.cpu_millis = 2;
        master.submit(blocked).unwrap();
    }
    assert!(master.schedule_round(0).unwrap().assignments.is_empty());
    master.submit(request("fits", "new")).unwrap();
    assert_eq!(master.take_ready_shard(), Some(0));
    assert!(master.schedule_round(0).unwrap().assignments.is_empty());
    assert_eq!(
        master.take_ready_shard(),
        Some(0),
        "fresh deferred work lost its wakeup"
    );
    let mut assigned = None;
    for _ in 0..3 {
        assigned = master.schedule_round(0).unwrap().assignments.pop();
        if assigned.is_some() {
            break;
        }
        assert_eq!(master.take_ready_shard(), Some(0));
    }
    let assignment =
        assigned.expect("new fitting request must run without another resource report");
    assert_eq!(assignment.instance_id, "fits");
    master.release(&assignment).unwrap();
    for _ in 0..5 {
        let Some(shard) = master.take_ready_shard() else {
            break;
        };
        assert!(master.schedule_round(shard).unwrap().assignments.is_empty());
    }
    assert_eq!(master.pending(0).unwrap(), 2);
    assert_eq!(
        master.take_ready_shard(),
        None,
        "permanently blocked requests must sleep"
    );
}
