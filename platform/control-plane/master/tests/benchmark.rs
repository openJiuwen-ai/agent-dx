//! Run with --release --ignored --nocapture; timing is evidence, not a flaky CI threshold.
use adx_core::{InstanceSpec, Resources};
use adx_master::{Master, Node, Placement, SchedulerConfig, SchedulingStats};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};
fn run(cache: usize, heterogeneous: bool) -> (Duration, SchedulingStats, Vec<String>) {
    let config = SchedulerConfig {
        candidate_cache_entries: cache,
        max_duration: Duration::from_secs(60),
        ..Default::default()
    };
    let mut m = Master::with_config(1, Placement::Spread, config).unwrap();
    for i in 0..128 {
        m.register(Node {
            id: format!("n{i:03}"),
            capacity: Resources {
                cpu_millis: 1000,
                memory_bytes: 1000,
                disk_bytes: 0,
            },
            available: true,
            labels: Default::default(),
            devices: vec![],
        })
        .unwrap();
    }
    let mut inflight = VecDeque::new();
    let mut results = vec![];
    let started = Instant::now();
    for i in 0..4096 {
        if inflight.len() == 256 {
            m.release(&inflight.pop_front().unwrap()).unwrap();
        }
        if heterogeneous && i % 16 == 0 {
            m.register(Node {
                id: format!("n{:03}", (i / 16) % 128),
                capacity: Resources {
                    cpu_millis: 800 + i % 201,
                    memory_bytes: 1000,
                    disk_bytes: 0,
                },
                available: true,
                labels: Default::default(),
                devices: vec![],
            })
            .unwrap();
        }
        let r = InstanceSpec {
            runtime_environment: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            id: format!("i{i}"),
            tenant_id: if heterogeneous {
                format!("t{}", i % 4)
            } else {
                "t".into()
            },
            image: "i".into(),
            runtime: "r".into(),
            priority: 0,
            resources: Resources {
                cpu_millis: if heterogeneous { 1 + i % 3 } else { 1 },
                memory_bytes: 1,
                disk_bytes: 0,
            },
            scheduling: Default::default(),
        };
        m.submit(r).unwrap();
        let a = m.schedule(0).unwrap().unwrap();
        results.push(a.node_id.clone());
        inflight.push_back(a);
    }
    (started.elapsed(), m.stats(0).unwrap(), results)
}
#[test]
#[ignore = "local release performance evidence"]
fn scheduler_fixed_inflight_benchmark() {
    for mixed in [false, true] {
        for cache in [0, 32] {
            run(cache, mixed);
        }
        let mut reference = vec![];
        let mut optimized = vec![];
        for round in 0..7 {
            let (slow, fast) = if round % 2 == 0 {
                (run(0, mixed), run(32, mixed))
            } else {
                let fast = run(32, mixed);
                (run(0, mixed), fast)
            };
            assert_eq!(slow.2, fast.2, "placement parity");
            assert!(
                fast.1.candidate_evaluations < slow.1.candidate_evaluations / 4,
                "candidate work regression"
            );
            println!("mixed={mixed} round={round} uncached_us={} cached_us={} uncached_candidates={} cached_candidates={} cache_hits={} rebuilds={}",slow.0.as_micros(),fast.0.as_micros(),slow.1.candidate_evaluations,fast.1.candidate_evaluations,fast.1.cache_hits,fast.1.journal_rebuilds);
            reference.push(slow.0.as_micros());
            optimized.push(fast.0.as_micros());
        }
        reference.sort();
        optimized.sort();
        println!(
            "MEDIAN mixed={mixed} uncached_us={} cached_us={} speedup={:.2}",
            reference[3],
            optimized[3],
            reference[3] as f64 / optimized[3] as f64
        );
    }
}
