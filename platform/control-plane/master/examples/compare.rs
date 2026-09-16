//! Local mailbox benchmark. Reports are in-process acknowledgments, not RPC or persistence.
use adx_core::{Assignment, InstanceSpec, Resources};
use adx_master::{Master, Node, Placement, SchedulerConfig};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};
const NODES: usize = 1000;
fn node(i: usize, capacity: u64, revision: usize) -> Node {
    Node {
        id: format!("unit-{i}"),
        capacity: Resources {
            cpu_millis: 300 * capacity,
            memory_bytes: 128 * capacity,
            disk_bytes: 0,
        },
        available: true,
        labels: [("report_seq".into(), revision.to_string())].into(),
        devices: vec![],
    }
}
fn spec(i: usize) -> InstanceSpec {
    InstanceSpec {
        snapshot_id: None,
        lifecycle: Default::default(),
        env: Default::default(),
        id: format!("r{i}"),
        tenant_id: "t".into(),
        image: "i".into(),
        runtime: "r".into(),
        priority: 0,
        resources: Resources {
            cpu_millis: 300,
            memory_bytes: 128,
            disk_bytes: 0,
        },
        scheduling: Default::default(),
    }
}
fn master(cache: usize, capacity: u64) -> Master {
    let mut m = Master::with_config(
        1,
        Placement::Pack,
        SchedulerConfig {
            candidate_cache_entries: cache,
            ..Default::default()
        },
    )
    .unwrap();
    for i in 0..NODES {
        m.register(node(i, capacity, 0)).unwrap();
    }
    m
}
fn wait<T>(rx: Receiver<T>) -> T {
    rx.recv_timeout(Duration::from_secs(120))
        .expect("mailbox timeout")
}
// Keep complete command payloads in the benchmark mailbox.
#[allow(clippy::large_enum_variant)]
enum Command {
    Schedule(InstanceSpec, Sender<Assignment>),
    Retry(Assignment, Sender<Assignment>),
    Add(Assignment, Sender<()>),
    Delete(Assignment, Sender<()>),
    Update(Node, Sender<()>),
    Stop(Sender<usize>),
}
struct Actor {
    tx: Sender<Command>,
    join: Option<thread::JoinHandle<()>>,
}
fn handle(
    cmd: Command,
    m: &mut Master,
    pending: &mut BTreeMap<String, Sender<Assignment>>,
    confirmed: &mut BTreeSet<String>,
) -> bool {
    match cmd {
        Command::Schedule(r, tx) => {
            pending.insert(r.id.clone(), tx);
            m.submit(r).unwrap();
        }
        Command::Retry(a, tx) => {
            pending.insert(a.instance_id.clone(), tx);
            m.retry(&a).unwrap();
        }
        Command::Add(a, tx) => {
            let snapshot = m.snapshot();
            let p = &snapshot.instances()[&a.instance_id];
            assert_eq!(p.node_id, a.node_id);
            assert!(confirmed.insert(a.instance_id));
            tx.send(()).unwrap();
        }
        Command::Delete(a, tx) => {
            assert!(confirmed.remove(&a.instance_id));
            m.release(&a).unwrap();
            tx.send(()).unwrap();
        }
        Command::Update(n, tx) => {
            m.register(n).unwrap();
            tx.send(()).unwrap();
        }
        Command::Stop(tx) => {
            assert!(pending.is_empty());
            tx.send(m.snapshot().instances().len()).unwrap();
            return false;
        }
    }
    true
}
impl Actor {
    fn new(cache: usize, capacity: u64) -> Self {
        let mut m = master(cache, capacity);
        let (tx, rx) = mpsc::channel();
        let join = thread::spawn(move || {
            let mut pending = BTreeMap::new();
            let mut confirmed = BTreeSet::new();
            let mut ready = None;
            loop {
                if ready.is_none()
                    && !handle(rx.recv().unwrap(), &mut m, &mut pending, &mut confirmed)
                {
                    break;
                }
                for _ in 0..255 {
                    match rx.try_recv() {
                        Ok(cmd) => {
                            if !handle(cmd, &mut m, &mut pending, &mut confirmed) {
                                return;
                            }
                        }
                        Err(_) => break,
                    }
                }
                if let Some(d) = ready.or_else(|| m.take_ready_domain()) {
                    let r = m.schedule_round(d).unwrap();
                    assert!(r.error.is_none(), "{:?}", r.error);
                    for a in r.assignments {
                        pending.remove(&a.instance_id).unwrap().send(a).unwrap();
                    }
                }
                ready = m.take_ready_domain();
            }
        });
        Self {
            tx,
            join: Some(join),
        }
    }
    fn schedule(&self, i: usize) -> Receiver<Assignment> {
        let (tx, rx) = mpsc::channel();
        self.tx.send(Command::Schedule(spec(i), tx)).unwrap();
        rx
    }
    fn retry(&self, a: Assignment) -> Receiver<Assignment> {
        let (tx, rx) = mpsc::channel();
        self.tx.send(Command::Retry(a, tx)).unwrap();
        rx
    }
    fn report(&self, a: Assignment, add: bool) {
        let (tx, rx) = mpsc::channel();
        self.tx
            .send(if add {
                Command::Add(a, tx)
            } else {
                Command::Delete(a, tx)
            })
            .unwrap();
        wait(rx);
    }
    fn stop(mut self) -> usize {
        let (tx, rx) = mpsc::channel();
        self.tx.send(Command::Stop(tx)).unwrap();
        let count = wait(rx);
        self.join.take().unwrap().join().unwrap();
        count
    }
}
fn percentile(v: &[f64], p: usize) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let mut v = v.to_vec();
    v.sort_by(f64::total_cmp);
    v[((v.len() - 1) * p) / 100]
}
#[allow(clippy::too_many_arguments)]
fn print(
    case: &str,
    cache: usize,
    count: usize,
    elapsed: Duration,
    latency: &[f64],
    report: &[f64],
    final_count: usize,
    updates: usize,
) {
    println!("ADX_COMPARE {{\"case\":\"{case}\",\"cache\":{cache},\"nodes\":{NODES},\"requests\":{count},\"qps\":{},\"p50_us\":{},\"p99_us\":{},\"report_p50_us\":{},\"report_p99_us\":{},\"final_instances\":{final_count},\"updates\":{updates},\"success\":{count},\"invalid_placement\":0}}",count as f64/elapsed.as_secs_f64(),percentile(latency,50),percentile(latency,99),percentile(report,50),percentile(report,99));
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let case = &args[1];
    let cache: usize = args[2].parse().unwrap();
    let count: usize = std::env::var("DOMAIN_BENCH_REQUEST_COUNT")
        .unwrap_or("1000".into())
        .parse()
        .unwrap();
    if case == "direct" {
        let mut m = master(cache, (count + 30) as u64);
        for i in 0..30 {
            m.submit(spec(i)).unwrap();
            m.schedule(0).unwrap().unwrap();
        }
        let mut l = vec![];
        let t = Instant::now();
        for i in 30..count + 30 {
            let a = Instant::now();
            m.submit(spec(i)).unwrap();
            m.schedule(0).unwrap().unwrap();
            l.push(a.elapsed().as_secs_f64() * 1e6);
        }
        print(
            case,
            cache,
            count,
            t.elapsed(),
            &l,
            &[],
            m.snapshot().instances().len(),
            0,
        );
        return;
    }
    let inflight: usize = std::env::var("DOMAIN_BENCH_INFLIGHT")
        .unwrap_or("5000".into())
        .parse()
        .unwrap();
    let capacity = match case.as_str() {
        "sustained" => (8.max(inflight / NODES * 4)) as u64,
        "retry" => 4,
        "update" => 30,
        _ => (count + 30) as u64,
    };
    let actor = Actor::new(cache, capacity);
    if case == "update" {
        let mut l = vec![];
        let t = Instant::now();
        for i in 0..1000 {
            let start = Instant::now();
            let (tx, rx) = mpsc::channel();
            actor
                .tx
                .send(Command::Update(
                    node(i % NODES, capacity, 1 + i / NODES),
                    tx,
                ))
                .unwrap();
            wait(rx);
            l.push(start.elapsed().as_secs_f64() * 1e6);
        }
        let elapsed = t.elapsed();
        let left = actor.stop();
        assert_eq!(left, 0);
        print(case, cache, 1000, elapsed, &l, &l, left, 1000);
        return;
    }
    if case == "sustained" {
        let mut slots: Vec<_> = (0..inflight)
            .map(|i| (Instant::now(), actor.schedule(i)))
            .collect();
        let mut latency = vec![];
        let mut reports = vec![];
        let mut measured = Instant::now();
        for i in 0..inflight + count {
            let (submitted, rx) = std::mem::replace(
                &mut slots[i % inflight],
                (Instant::now(), mpsc::channel().1),
            );
            let a = wait(rx);
            let reporting = Instant::now();
            actor.report(a.clone(), true);
            actor.report(a, false);
            if i >= inflight {
                latency.push(submitted.elapsed().as_secs_f64() * 1e6);
                reports.push(reporting.elapsed().as_secs_f64() * 1e6);
            }
            slots[i % inflight] = (Instant::now(), actor.schedule(inflight + i));
            if i + 1 == inflight {
                measured = Instant::now();
            }
        }
        let elapsed = measured.elapsed();
        for (_, rx) in slots {
            let a = wait(rx);
            actor.report(a.clone(), true);
            actor.report(a, false);
        }
        let left = actor.stop();
        assert_eq!(left, 0);
        print(case, cache, count, elapsed, &latency, &reports, left, 0);
        return;
    }
    let warmup = if case == "retry" { 0 } else { 30 };
    for i in 0..warmup {
        wait(actor.schedule(i));
    }
    let mut latencies = vec![];
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let updater = if case == "closed500" {
        let tx = actor.tx.clone();
        let stop = stop.clone();
        Some(thread::spawn(move || {
            let mut n = 0;
            let mut next = Instant::now();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let (a, b) = mpsc::channel();
                tx.send(Command::Update(node(n % NODES, capacity, 1 + n / NODES), a))
                    .unwrap();
                wait(b);
                n += 1;
                next += Duration::from_millis(2);
                thread::sleep(next.saturating_duration_since(Instant::now()));
            }
            n
        }))
    } else {
        None
    };
    let t = Instant::now();
    match case.as_str() {
        "open" => {
            let futures: Vec<_> = (30..count + 30)
                .map(|i| (Instant::now(), actor.schedule(i)))
                .collect();
            for (start, rx) in futures {
                let a = wait(rx);
                assert!(a.node_id.starts_with("unit-"));
                latencies.push(start.elapsed().as_secs_f64() * 1e6);
            }
        }
        "closed" | "closed500" => {
            for i in 30..count + 30 {
                let start = Instant::now();
                wait(actor.schedule(i));
                latencies.push(start.elapsed().as_secs_f64() * 1e6);
            }
        }
        "retry" => {
            for i in 30..count + 30 {
                let a = wait(actor.schedule(i));
                let start = Instant::now();
                let b = wait(actor.retry(a.clone()));
                assert_ne!(a.node_id, b.node_id);
                assert!(b.generation > a.generation);
                latencies.push(start.elapsed().as_secs_f64() * 1e6);
            }
        }
        _ => panic!("unknown case"),
    }
    let elapsed = t.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let updates = updater.map(|h| h.join().unwrap()).unwrap_or(0);
    let left = actor.stop();
    assert_eq!(left, count + warmup);
    print(case, cache, count, elapsed, &latencies, &[], left, updates);
}
