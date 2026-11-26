//! Bounded diagnostics for transfers. The watchdog never reads or buffers payloads.
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferPolicy {
    pub slow_after: Duration,
    pub min_bytes_per_second: u64,
    pub logs_per_minute: usize,
}
impl Default for TransferPolicy {
    fn default() -> Self {
        Self {
            slow_after: Duration::from_secs(10),
            min_bytes_per_second: 64 * 1024,
            logs_per_minute: 60,
        }
    }
}

#[derive(Default)]
pub(crate) struct Wait {
    since: AtomicU64,
    total: AtomicU64,
    longest: AtomicU64,
}
impl Wait {
    pub fn pending(&self, now: u64) {
        let _ = self.since.compare_exchange(
            0,
            now.saturating_add(1),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }
    pub fn ready(&self, now: u64) {
        let since = self.since.swap(0, Ordering::Relaxed);
        if since != 0 {
            let elapsed = now.saturating_sub(since - 1);
            self.total.fetch_add(elapsed, Ordering::Relaxed);
            self.longest.fetch_max(elapsed, Ordering::Relaxed);
        }
    }
    pub fn snapshot(&self, now: u64) -> (u64, u64, u64) {
        let since = self.since.load(Ordering::Relaxed);
        let pending = if since == 0 {
            0
        } else {
            now.saturating_sub(since - 1)
        };
        (
            self.total.load(Ordering::Relaxed).saturating_add(pending),
            self.longest.load(Ordering::Relaxed).max(pending),
            pending,
        )
    }
}

pub(crate) struct ConnectionProgress {
    pub id: u64,
    pub written: AtomicU64,
    pub write_wait: Wait,
    started: Instant,
}
impl ConnectionProgress {
    pub fn new() -> Arc<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Arc::new(Self {
            id: NEXT.fetch_add(1, Ordering::Relaxed),
            written: AtomicU64::new(0),
            write_wait: Wait::default(),
            started: Instant::now(),
        })
    }
    pub fn now_ms(&self) -> u64 {
        milliseconds(self.started.elapsed())
    }
}

pub(crate) struct TransferIdentity {
    pub component: &'static str,
    pub direction: &'static str,
    pub request_id: String,
    pub trace_id: String,
    pub span_id: String,
}

pub(crate) struct TransferProgress {
    pub identity: TransferIdentity,
    pub request_bytes: AtomicU64,
    pub response_bytes: AtomicU64,
    pub request_wait: Wait,
    pub response_wait: Wait,
    pub h2_wait: Wait,
    pub frontend: Option<Arc<ConnectionProgress>>,
    pub headers_ms: AtomicU64,
    pub h2_enqueued_bytes: AtomicU64,
    pub slow: AtomicBool,
    started: Instant,
    last_progress: AtomicU64,
    finished: AtomicBool,
    sample: Mutex<(Instant, u64)>,
    events: Mutex<()>,
}
impl TransferProgress {
    pub fn now_ms(&self) -> u64 {
        milliseconds(self.started.elapsed())
    }
    pub fn progress(&self, request: bool, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let counter = if request {
            &self.request_bytes
        } else {
            &self.response_bytes
        };
        counter.fetch_add(bytes as u64, Ordering::Relaxed);
        self.last_progress.store(self.now_ms(), Ordering::Relaxed);
    }
    pub fn bytes(&self) -> u64 {
        self.request_bytes
            .load(Ordering::Relaxed)
            .saturating_add(self.response_bytes.load(Ordering::Relaxed))
    }
    fn slow_reason(&self, policy: &TransferPolicy) -> Option<&'static str> {
        if self.finished.load(Ordering::Acquire) || self.slow.load(Ordering::Relaxed) {
            return None;
        }
        let now = Instant::now();
        if now.duration_since(self.started) < policy.slow_after {
            return None;
        }
        if self.identity.component == "relay" {
            let now = self.now_ms();
            if self.h2_wait.snapshot(now).2 >= milliseconds(policy.slow_after) {
                return Some("h2_capacity_wait");
            }
            if self.request_wait.snapshot(now).2 >= milliseconds(policy.slow_after) {
                return Some("runtime_write_wait");
            }
            return None;
        }
        let age = self
            .now_ms()
            .saturating_sub(self.last_progress.load(Ordering::Relaxed));
        if age >= milliseconds(policy.slow_after) {
            return Some("no_progress");
        }
        let mut sample = self
            .sample
            .lock()
            .expect("transfer sampling never panics while locked");
        let elapsed = now.duration_since(sample.0);
        if elapsed < policy.slow_after {
            return None;
        }
        let bytes = self.bytes();
        let rate = bytes.saturating_sub(sample.1) as f64 / elapsed.as_secs_f64();
        *sample = (now, bytes);
        (rate < policy.min_bytes_per_second as f64).then_some("low_throughput")
    }
}

// Each live transfer has one cancellable deadline. Completing a normal request
// removes its entry immediately; no completed-request tombstones are retained.
#[derive(Default)]
struct Schedule {
    deadlines: BTreeMap<(Instant, u64), Weak<TransferProgress>>,
    by_id: HashMap<u64, Instant>,
    armed: Option<Instant>,
}
impl Schedule {
    fn insert(&mut self, id: u64, at: Instant, progress: Weak<TransferProgress>) -> bool {
        if let Some(previous) = self.by_id.insert(id, at) {
            self.deadlines.remove(&(previous, id));
        }
        self.deadlines.insert((at, id), progress);
        self.armed.is_none_or(|armed| at < armed)
    }
    fn cancel(&mut self, id: u64) {
        if let Some(at) = self.by_id.remove(&id) {
            self.deadlines.remove(&(at, id));
        }
    }
    fn due(&mut self) -> Option<(u64, Weak<TransferProgress>)> {
        if self
            .deadlines
            .first_key_value()
            .is_none_or(|((at, _), _)| *at > Instant::now())
        {
            return None;
        }
        let ((_, id), progress) = self.deadlines.pop_first()?;
        self.by_id.remove(&id);
        Some((id, progress))
    }
    fn next(&mut self) -> Option<Instant> {
        self.armed = self.deadlines.first_key_value().map(|((at, _), _)| *at);
        self.armed
    }
}

#[derive(Clone)]
pub(crate) struct TransferMonitor(Arc<Monitor>);
struct Monitor {
    policy: TransferPolicy,
    schedule: Mutex<Schedule>,
    wake: Arc<Notify>,
    next: AtomicU64,
    started: AtomicBool,
    active: AtomicU64,
    checks: AtomicU64,
    slow: AtomicU64,
    suppressed: AtomicU64,
    budget: Mutex<(Instant, usize)>,
}
impl Default for TransferMonitor {
    fn default() -> Self {
        Self::new(TransferPolicy::default())
    }
}
impl TransferMonitor {
    pub fn new(policy: TransferPolicy) -> Self {
        Self(Arc::new(Monitor {
            policy,
            schedule: Mutex::new(Schedule::default()),
            wake: Arc::new(Notify::new()),
            next: AtomicU64::new(1),
            started: AtomicBool::new(false),
            active: AtomicU64::new(0),
            checks: AtomicU64::new(0),
            slow: AtomicU64::new(0),
            suppressed: AtomicU64::new(0),
            budget: Mutex::new((Instant::now(), 0)),
        }))
    }
    pub fn start(
        &self,
        identity: TransferIdentity,
        frontend: Option<Arc<ConnectionProgress>>,
    ) -> TransferRegistration {
        let started = Instant::now();
        let progress = Arc::new(TransferProgress {
            identity,
            frontend,
            started,
            request_bytes: AtomicU64::new(0),
            response_bytes: AtomicU64::new(0),
            request_wait: Wait::default(),
            response_wait: Wait::default(),
            h2_wait: Wait::default(),
            headers_ms: AtomicU64::new(0),
            h2_enqueued_bytes: AtomicU64::new(0),
            last_progress: AtomicU64::new(0),
            slow: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            sample: Mutex::new((started, 0)),
            events: Mutex::new(()),
        });
        let id = self.0.next.fetch_add(1, Ordering::Relaxed);
        let earlier = self
            .0
            .schedule
            .lock()
            .expect("transfer scheduling never panics while locked")
            .insert(
                id,
                started + self.0.policy.slow_after,
                Arc::downgrade(&progress),
            );
        self.0.active.fetch_add(1, Ordering::Relaxed);
        if earlier {
            self.0.wake.notify_one();
        }
        if !self.0.started.swap(true, Ordering::Relaxed) {
            let monitor = Arc::downgrade(&self.0);
            tokio::spawn(async move {
                loop {
                    let Some(current) = monitor.upgrade() else {
                        break;
                    };
                    current.check();
                    let wake = current.wake.clone();
                    let next = current
                        .schedule
                        .lock()
                        .expect("transfer scheduling never panics while locked")
                        .next();
                    drop(current);
                    match next {
                        Some(at) => {
                            tokio::select! { _ = tokio::time::sleep_until(at) => {}, _ = wake.notified() => {} }
                        }
                        None => wake.notified().await,
                    }
                }
            });
        }
        TransferRegistration {
            monitor: self.clone(),
            id,
            progress,
            finished: false,
        }
    }
    pub fn prometheus(&self, component: &str) -> String {
        let active = self.0.active.load(Ordering::Relaxed);
        format!("data_plane_{component}_transfers_in_flight {active}\ndata_plane_{component}_slow_transfers_total {}\ndata_plane_{component}_transfer_diagnostics_suppressed_total {}\ndata_plane_{component}_transfer_slow_checks_total {}\n", self.0.slow.load(Ordering::Relaxed), self.0.suppressed.load(Ordering::Relaxed), self.0.checks.load(Ordering::Relaxed))
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.wake.notify_one();
    }
}
impl Monitor {
    fn check(&self) {
        loop {
            let due = self
                .schedule
                .lock()
                .expect("transfer scheduling never panics while locked")
                .due();
            let Some((id, progress)) = due else {
                break;
            };
            let Some(state) = progress.upgrade() else {
                continue;
            };
            let _events = state
                .events
                .lock()
                .expect("diagnostic lifecycle never panics while locked");
            if state.finished.load(Ordering::Acquire) {
                continue;
            }
            self.checks.fetch_add(1, Ordering::Relaxed);
            self.consider(&state);
            if !state.slow.load(Ordering::Relaxed) {
                let next = Instant::now() + self.policy.slow_after;
                self.schedule
                    .lock()
                    .expect("transfer scheduling never panics while locked")
                    .insert(id, next, Arc::downgrade(&state));
            }
        }
    }
    fn consider(&self, state: &TransferProgress) {
        if let Some(reason) = state.slow_reason(&self.policy) {
            if !state.slow.swap(true, Ordering::AcqRel) {
                self.slow.fetch_add(1, Ordering::Relaxed);
                self.emit(state, "slow_transfer_started", reason);
            }
        }
    }
    fn emit(&self, state: &TransferProgress, event: &'static str, outcome: &'static str) {
        let mut budget = self
            .budget
            .lock()
            .expect("diagnostic budget never panics while locked");
        if budget.0.elapsed() >= Duration::from_secs(60) {
            *budget = (Instant::now(), 0);
        }
        if budget.1 >= self.policy.logs_per_minute {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return;
        }
        budget.1 += 1;
        drop(budget);
        let now = state.now_ms();
        let (request_wait_ms, _, _) = state.request_wait.snapshot(now);
        let (response_wait_ms, _, _) = state.response_wait.snapshot(now);
        let (h2_capacity_wait_ms, _, _) = state.h2_wait.snapshot(now);
        let (
            frontend_connection_id,
            frontend_connection_written_bytes,
            frontend_write_wait_ms,
            frontend_write_pending_ms,
        ) = state.frontend.as_ref().map_or((0, 0, 0, 0), |connection| {
            let (wait, _, pending) = connection.write_wait.snapshot(connection.now_ms());
            (
                connection.id,
                connection.written.load(Ordering::Relaxed),
                wait,
                pending,
            )
        });
        tracing::warn!(target: "adx_transfer", event, outcome,
            component = state.identity.component, direction = state.identity.direction,
            request_id = %state.identity.request_id, trace_id = %state.identity.trace_id, span_id = %state.identity.span_id,
            elapsed_ms = now, headers_ms = state.headers_ms.load(Ordering::Relaxed),
            request_bytes = state.request_bytes.load(Ordering::Relaxed), response_bytes = state.response_bytes.load(Ordering::Relaxed),
            last_progress_age_ms = now.saturating_sub(state.last_progress.load(Ordering::Relaxed)),
            request_wait_ms, response_wait_ms, h2_capacity_wait_ms,
            h2_enqueued_bytes = state.h2_enqueued_bytes.load(Ordering::Relaxed),
            frontend_connection_id, frontend_connection_written_bytes, frontend_write_wait_ms, frontend_write_pending_ms,
            "Slow transfer diagnostic");
    }
}

pub(crate) struct TransferRegistration {
    monitor: TransferMonitor,
    id: u64,
    pub progress: Arc<TransferProgress>,
    finished: bool,
}
impl TransferRegistration {
    pub fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        // A delayed executor may not have polled an already-due timer before completion.
        let _events = self
            .progress
            .events
            .lock()
            .expect("diagnostic lifecycle never panics while locked");
        self.monitor.0.consider(&self.progress);
        self.progress.finished.store(true, Ordering::Release);
        self.monitor
            .0
            .schedule
            .lock()
            .expect("transfer scheduling never panics while locked")
            .cancel(self.id);
        self.monitor.0.active.fetch_sub(1, Ordering::Relaxed);
        if self.progress.slow.load(Ordering::Acquire) {
            self.monitor
                .0
                .emit(&self.progress, "slow_transfer_finished", outcome);
        }
    }
}
impl Drop for TransferRegistration {
    fn drop(&mut self) {
        self.finish("dropped");
    }
}
fn milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity(component: &'static str) -> TransferIdentity {
        TransferIdentity {
            component,
            direction: "download",
            request_id: "data-00000000-0000-0000-0000-000000000000".into(),
            trace_id: "11111111111111111111111111111111".into(),
            span_id: "2222222222222222".into(),
        }
    }
    #[tokio::test(start_paused = true)]
    async fn watchdog_detects_a_stalled_transfer_without_another_body_poll() {
        let monitor = TransferMonitor::default();
        let mut transfer = monitor.start(identity("ingress"), None);
        transfer.progress.response_wait.pending(0);
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(transfer.progress.slow.load(Ordering::Relaxed));
        assert_eq!(monitor.0.slow.load(Ordering::Relaxed), 1);
        transfer.finish("response_body_error");
        assert!(monitor
            .prometheus("ingress")
            .contains("transfers_in_flight 0"));
        assert_eq!(monitor.0.budget.lock().unwrap().1, 2);
    }
    #[tokio::test(start_paused = true)]
    async fn steady_low_throughput_is_slow_even_with_frequent_progress() {
        let monitor = TransferMonitor::default();
        let mut transfer = monitor.start(identity("ingress"), None);
        for _ in 0..11 {
            transfer.progress.progress(false, 12 * 1024);
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        monitor.0.check();
        assert!(transfer.progress.slow.load(Ordering::Relaxed));
        transfer.finish("complete");
        monitor.0.check();
        assert_eq!(monitor.0.budget.lock().unwrap().1, 2);
    }
    #[tokio::test(start_paused = true)]
    async fn healthy_large_transfers_and_idle_connects_do_not_emit_diagnostics() {
        let monitor = TransferMonitor::default();
        let mut transfer = monitor.start(identity("ingress"), None);
        let mut idle = monitor.start(identity("relay"), None);
        for _ in 0..30 {
            transfer.progress.progress(false, 1024 * 1024);
            tokio::time::advance(Duration::from_secs(1)).await;
            monitor.0.check();
        }
        transfer.finish("complete");
        idle.finish("complete");
        assert_eq!(monitor.0.slow.load(Ordering::Relaxed), 0);
        assert_eq!(monitor.0.budget.lock().unwrap().1, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn relay_capacity_wait_is_detected_and_counted_without_body_progress() {
        let monitor = TransferMonitor::default();
        let mut transfer = monitor.start(identity("relay"), None);
        transfer.progress.h2_wait.pending(0);
        tokio::time::advance(Duration::from_secs(12)).await;
        monitor.0.check();
        assert!(transfer.progress.slow.load(Ordering::Relaxed));
        assert_eq!(
            transfer.progress.h2_wait.snapshot(12000),
            (12000, 12000, 12000)
        );
        transfer.progress.h2_wait.ready(12000);
        assert_eq!(transfer.progress.h2_wait.snapshot(15000), (12000, 12000, 0));
        transfer.finish("complete");
    }
    #[tokio::test(start_paused = true)]
    async fn concurrent_slow_transfers_share_a_bounded_output_budget() {
        let monitor = TransferMonitor::new(TransferPolicy {
            logs_per_minute: 2,
            ..Default::default()
        });
        let mut active: Vec<_> = (0..100)
            .map(|_| monitor.start(identity("ingress"), None))
            .collect();
        tokio::time::advance(Duration::from_secs(11)).await;
        monitor.0.check();
        for transfer in &mut active {
            transfer.finish("dropped");
        }
        assert_eq!(monitor.0.slow.load(Ordering::Relaxed), 100);
        assert_eq!(monitor.0.budget.lock().unwrap().1, 2);
        assert_eq!(monitor.0.suppressed.load(Ordering::Relaxed), 198);
        assert!(monitor
            .prometheus("ingress")
            .contains("transfers_in_flight 0"));
    }
    #[tokio::test(start_paused = true)]
    async fn completed_fast_requests_are_removed_before_their_check_deadlines() {
        let monitor = TransferMonitor::default();
        for _ in 0..10000 {
            let mut transfer = monitor.start(identity("ingress"), None);
            transfer.progress.progress(false, 4096);
            transfer.finish("complete");
        }
        {
            let schedule = monitor.0.schedule.lock().unwrap();
            assert!(schedule.deadlines.is_empty());
            assert!(schedule.by_id.is_empty());
        }
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(monitor.0.checks.load(Ordering::Relaxed), 0);
        assert_eq!(monitor.0.budget.lock().unwrap().1, 0);
    }

    #[tokio::test]
    #[ignore = "manual observer CPU and memory measurement"]
    async fn observer_load_probe() {
        fn rss_kib() -> u64 {
            let output = std::process::Command::new("/bin/ps")
                .args(["-o", "rss=", "-p", &std::process::id().to_string()])
                .output()
                .unwrap();
            String::from_utf8(output.stdout)
                .unwrap()
                .trim()
                .parse()
                .unwrap()
        }
        let count: usize = std::env::var("ADX_OBSERVER_LOAD_COUNT")
            .unwrap_or_else(|_| "10000".into())
            .parse()
            .unwrap();
        let monitor = TransferMonitor::default();
        let before = rss_kib();
        let started = std::time::Instant::now();
        let mut active: Vec<_> = (0..count)
            .map(|_| monitor.start(identity("ingress"), None))
            .collect();
        let registration_ns = started.elapsed().as_nanos() / count as u128;
        let active_rss = rss_kib();
        let started = std::time::Instant::now();
        for transfer in &active {
            for _ in 0..8 {
                transfer.progress.progress(false, 65536);
            }
        }
        let progress_ns = started.elapsed().as_nanos() / (count * 8) as u128;
        let started = std::time::Instant::now();
        for transfer in &mut active {
            transfer.finish("complete");
        }
        let completion_ns = started.elapsed().as_nanos() / count as u128;
        assert_eq!(monitor.0.active.load(Ordering::Relaxed), 0);
        assert!(monitor.0.schedule.lock().unwrap().deadlines.is_empty());
        assert_eq!(monitor.0.checks.load(Ordering::Relaxed), 0);
        println!(
            "{}",
            serde_json::json!({"concurrency":count,"register_ns_per_transfer":registration_ns as u64,"progress_ns_per_chunk":progress_ns as u64,"finish_ns_per_transfer":completion_ns as u64,"rss_before_kib":before,"rss_with_active_transfers_kib":active_rss,"rss_delta_kib":active_rss.saturating_sub(before),"progress_struct_bytes":std::mem::size_of::<TransferProgress>(),"slow_checks":monitor.0.checks.load(Ordering::Relaxed),"diagnostic_events":monitor.0.budget.lock().unwrap().1})
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "manual shared-scheduler contention measurement"]
    async fn observer_contention_probe() {
        let count: usize = std::env::var("ADX_OBSERVER_LOAD_COUNT")
            .unwrap_or_else(|_| "10000".into())
            .parse()
            .unwrap();
        let monitor = TransferMonitor::new(TransferPolicy {
            slow_after: Duration::from_secs(3600),
            ..Default::default()
        });
        let active: Vec<_> = (0..count)
            .map(|_| monitor.start(identity("ingress"), None))
            .collect();
        let started = std::time::Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let monitor = monitor.clone();
            tasks.push(tokio::spawn(async move {
                for i in 0..25000 {
                    let mut transfer = monitor.start(identity("ingress"), None);
                    transfer.progress.progress(false, 65536);
                    transfer.finish("complete");
                    if i % 100 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let elapsed = started.elapsed();
        assert_eq!(monitor.0.active.load(Ordering::Relaxed), count as u64);
        assert_eq!(monitor.0.checks.load(Ordering::Relaxed), 0);
        assert_eq!(monitor.0.budget.lock().unwrap().1, 0);
        println!(
            "{}",
            serde_json::json!({"background_active_transfers":count,"worker_threads":4,"completed_transfers":100000,"elapsed_ms":elapsed.as_secs_f64()*1000.0,"observer_lifecycles_per_second":100000.0/elapsed.as_secs_f64(),"slow_checks":0,"diagnostic_events":0})
        );
        drop(active);
        assert_eq!(monitor.0.active.load(Ordering::Relaxed), 0);
        assert!(monitor.0.schedule.lock().unwrap().deadlines.is_empty());
    }
}
