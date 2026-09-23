use adx_coordinator::{Coordinator, Node, Placement, SchedulerConfig};
use adx_core::{EnvironmentSpec, Resources};
use std::time::Duration;

fn request(id: &str, tenant: &str) -> EnvironmentSpec {
    EnvironmentSpec {
        runtime_profile: None,
        id: id.into(),
        tenant_id: tenant.into(),
        image: "image".into(),
        runtime_class: "runc".into(),
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
        sandbox: Default::default(),
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
        let mut coordinator = Coordinator::with_config(1, Placement::Pack, config).unwrap();
        coordinator.register(node(false)).unwrap();
        coordinator.submit(request("old", "old-tenant")).unwrap();
        for n in 0..2 {
            coordinator
                .submit(request(&format!("initial-{n}"), "busy"))
                .unwrap();
        }
        assert!(coordinator
            .schedule_round(0)
            .unwrap()
            .assignments
            .is_empty());
        coordinator.register(node(true)).unwrap();
        let mut served = false;
        // The finite original sweep has only two remaining tickets. New arrivals
        // must not extend it indefinitely and keep the recovered old tenant asleep.
        for round in 0..3 {
            for n in 0..2 {
                coordinator
                    .submit(request(&format!("new-{round}-{n}"), "busy"))
                    .unwrap();
            }
            let outcome = coordinator.schedule_round(0).unwrap();
            assert!(outcome.attempted <= 1);
            for allocation in outcome.assignments {
                served |= allocation.environment_id == "old";
                coordinator.release(&allocation).unwrap();
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
    let mut coordinator = Coordinator::with_config(
        1,
        Placement::Pack,
        SchedulerConfig {
            max_attempts: 1,
            max_duration: Duration::from_secs(1),
            ..Default::default()
        },
    )
    .unwrap();
    coordinator.register(node(true)).unwrap();
    for n in 0..2 {
        let mut blocked = request(&format!("blocked-{n}"), "old");
        blocked.resources.cpu_millis = 2;
        coordinator.submit(blocked).unwrap();
    }
    assert!(coordinator
        .schedule_round(0)
        .unwrap()
        .assignments
        .is_empty());
    coordinator.submit(request("fits", "new")).unwrap();
    assert_eq!(coordinator.take_ready_shard(), Some(0));
    assert!(coordinator
        .schedule_round(0)
        .unwrap()
        .assignments
        .is_empty());
    assert_eq!(
        coordinator.take_ready_shard(),
        Some(0),
        "fresh deferred work lost its wakeup"
    );
    let mut assigned = None;
    for _ in 0..3 {
        assigned = coordinator.schedule_round(0).unwrap().assignments.pop();
        if assigned.is_some() {
            break;
        }
        assert_eq!(coordinator.take_ready_shard(), Some(0));
    }
    let assignment =
        assigned.expect("new fitting request must run without another resource report");
    assert_eq!(assignment.environment_id, "fits");
    coordinator.release(&assignment).unwrap();
    for _ in 0..5 {
        let Some(shard) = coordinator.take_ready_shard() else {
            break;
        };
        assert!(coordinator
            .schedule_round(shard)
            .unwrap()
            .assignments
            .is_empty());
    }
    assert_eq!(coordinator.pending(0).unwrap(), 2);
    assert_eq!(
        coordinator.take_ready_shard(),
        None,
        "permanently blocked requests must sleep"
    );
}
