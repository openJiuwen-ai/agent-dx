//! Deterministic sustained scheduling workload. No runtime, RPC or hardware emulation.
use adx_core::{Assignment, InstanceSpec, Resources};
use adx_master::{Master, Node, Placement, SchedulerConfig};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

const DOMAINS: usize = 4;
const TENANTS: usize = 8;
fn capacity() -> Resources {
    Resources {
        cpu_millis: 8,
        memory_bytes: 8,
        disk_bytes: 8,
    }
}
fn node(index: usize, available: bool) -> Node {
    Node {
        id: format!("n{index}"),
        capacity: capacity(),
        available,
        labels: Default::default(),
        devices: vec![],
    }
}
fn submit(
    master: &mut Master,
    serial: &mut usize,
    tenant: usize,
    priority: i32,
    tick: usize,
    pending: &mut BTreeMap<String, (InstanceSpec, usize, usize)>,
) {
    let shape = [(3, 1, 0), (1, 3, 0), (1, 1, 3), (2, 2, 0)][(*serial / DOMAINS + *serial) % 4];
    let request = InstanceSpec {
        runtime_environment: None,
        id: format!("r{serial}"),
        tenant_id: format!("t{tenant}"),
        image: "test-image".into(),
        runtime: "runc".into(),
        resources: Resources {
            cpu_millis: shape.0,
            memory_bytes: shape.1,
            disk_bytes: shape.2,
        },
        priority,
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        scheduling: Default::default(),
        sandbox: Default::default(),
    };
    let shard = master.submit(request.clone()).unwrap();
    assert_eq!(shard, *serial % DOMAINS, "Global must rotate Domains");
    pending.insert(request.id.clone(), (request, shard, tick));
    *serial += 1;
}
fn run(waves: usize, placement: Placement, cache: usize) -> Value {
    let mut master = Master::with_config(
        DOMAINS,
        placement,
        SchedulerConfig {
            max_attempts: 5,
            max_duration: Duration::from_secs(1),
            candidate_cache_entries: cache,
            ..Default::default()
        },
    )
    .unwrap();
    for n in 0..DOMAINS * 2 {
        assert_eq!(master.register(node(n, true)).unwrap(), n % DOMAINS);
    }
    let mut pending = BTreeMap::new();
    let mut live: BTreeMap<String, (Assignment, Resources, usize)> = BTreeMap::new();
    let mut usage = [Resources::default(); DOMAINS * 2];
    let mut serial = 0;
    let mut completed = [0usize; TENANTS];
    let mut waits = Vec::new();
    let mut max_wait = [0usize; TENANTS];
    let mut digest = Sha256::new();
    let mut rounds = 0;
    let started = Instant::now();
    for wave in 0..waves {
        let unavailable = wave % (DOMAINS * 2);
        master.register(node(unavailable, false)).unwrap();
        for tenant in 0..TENANTS {
            for priority in [-1, 1, 0] {
                for _ in 0..DOMAINS {
                    submit(&mut master, &mut serial, tenant, priority, 0, &mut pending);
                }
            }
        }
        for tick in 0..256 {
            if tick == 3 {
                master.register(node(unavailable, true)).unwrap();
            }
            // Keep arrivals overlapping the existing queue while resources are
            // occupied, including after the maintained node becomes available.
            if tick < 8 && tick % 3 == 1 {
                for tenant in 0..3 {
                    for _ in 0..DOMAINS {
                        submit(
                            &mut master,
                            &mut serial,
                            (wave + tenant) % TENANTS,
                            0,
                            tick,
                            &mut pending,
                        );
                    }
                }
            }
            let released: Vec<_> = live
                .iter()
                .filter(|(_, (_, _, due))| *due <= tick)
                .map(|(id, _)| id.clone())
                .collect();
            for id in released {
                let (assignment, resources, _) = live.remove(&id).unwrap();
                master.release(&assignment).unwrap();
                let n: usize = assignment.node_id[1..].parse().unwrap();
                usage[n].cpu_millis -= resources.cpu_millis;
                usage[n].memory_bytes -= resources.memory_bytes;
                usage[n].disk_bytes -= resources.disk_bytes;
            }
            // Consume the real coalesced wake queue, bounded to four rounds per tick.
            for _ in 0..DOMAINS {
                let Some(shard) = master.take_ready_shard() else {
                    break;
                };
                let outcome = master.schedule_round(shard).unwrap();
                assert!(outcome.error.is_none());
                assert!(outcome.attempted <= 5);
                rounds += 1;
                for assignment in outcome.assignments {
                    let (request, expected_shard, queued_at) =
                        pending.remove(&assignment.instance_id).unwrap();
                    assert_eq!(assignment.shard_id, expected_shard);
                    let n: usize = assignment.node_id[1..].parse().unwrap();
                    assert!(
                        n != unavailable || tick >= 3,
                        "maintenance admitted new work"
                    );
                    let tenant: usize = request.tenant_id[1..].parse().unwrap();
                    let wait = tick - queued_at;
                    waits.push(wait);
                    max_wait[tenant] = max_wait[tenant].max(wait);
                    completed[tenant] += 1;
                    let r = request.resources;
                    usage[n].cpu_millis += r.cpu_millis;
                    usage[n].memory_bytes += r.memory_bytes;
                    usage[n].disk_bytes += r.disk_bytes;
                    assert!(
                        usage[n].cpu_millis <= 8
                            && usage[n].memory_bytes <= 8
                            && usage[n].disk_bytes <= 8,
                        "overcommit"
                    );
                    digest.update(format!(
                        "{}:{}:{}\n",
                        assignment.instance_id, assignment.node_id, tick
                    ));
                    let duration = 1 + request.id[1..].parse::<usize>().unwrap() % 7;
                    live.insert(request.id, (assignment, r, tick + duration));
                }
            }
            if tick >= 8 && pending.is_empty() && live.is_empty() {
                break;
            }
            assert!(
                tick < 255,
                "bounded workload failed to drain: wave={wave}, pending={}",
                pending.len()
            );
        }
        assert!(master.snapshot().instances().is_empty());
        assert!(usage.iter().all(|u| *u == Resources::default()));
        for shard in 0..DOMAINS {
            assert_eq!(master.pending(shard).unwrap(), 0);
        }
        if (wave + 1) % 250 == 0 {
            eprintln!(
                "mixed workload cache={cache} waves={}/{} completed={}",
                wave + 1,
                waves,
                serial
            );
        }
    }
    waits.sort_unstable();
    assert_eq!(completed.iter().sum::<usize>(), serial);
    let p99 = waits[(waits.len() - 1) * 99 / 100];
    let stats: Vec<_> = (0..DOMAINS).map(|d| master.stats(d).unwrap()).collect();
    json!({"waves":waves,"shards":DOMAINS,"tenants":TENANTS,"requests":serial,"rounds":rounds,
        "cache_entries":cache,"seconds":started.elapsed().as_secs_f64(),"completed_per_tenant":completed,
        "max_wait_ticks_per_tenant":max_wait,"p99_wait_ticks":p99,"decision_sha256":format!("{:x}",digest.finalize()),
        "cache_hits":stats.iter().map(|s|s.cache_hits).sum::<u64>(),"candidate_evaluations":stats.iter().map(|s|s.candidate_evaluations).sum::<u64>(),
        "overcommit":false,"pending":0,"live":0,"status":"passed"})
}
fn main() {
    let waves = std::env::args()
        .nth(1)
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(1000);
    assert!(waves > 0);
    let mut profiles = Vec::new();
    for (name, placement) in [("pack", Placement::Pack), ("spread", Placement::Spread)] {
        let off = run(waves, placement, 0);
        let on = run(waves, placement, 32);
        assert_eq!(
            off["decision_sha256"], on["decision_sha256"],
            "candidate reuse changed mixed-workload decisions"
        );
        profiles.push(json!({"placement":name,"without_cache":off,"with_cache":on}));
    }
    println!(
        "{}",
        json!({"scope":"in-process scheduler; scalar resources; no runtime/RPC/device hardware", "profiles":profiles,"status":"passed"})
    );
}
