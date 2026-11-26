//! Bounded diagnostics for transfers. The watchdog never reads or buffers payloads.
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;
#[path = "transfer/diagnostic.rs"]
mod diagnostic;
use diagnostic::Diagnostic;
const CHECK_BATCH: usize = 128;

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
}
impl Wait {
    /// Polls of each I/O direction have one writer. Snapshots may run concurrently.
    pub fn observe(&self, pending: bool, now: impl FnOnce() -> u64) -> bool {
        match (pending, self.since.load(Ordering::Relaxed) != 0) {
            (true, false) => self.pending(now()),
            (false, true) => {
                self.ready(now());
                false
            }
            _ => false,
        }
    }
    pub fn pending(&self, now: u64) -> bool {
        self.since
            .compare_exchange(
                0,
                now.saturating_add(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    }
    pub fn ready(&self, now: u64) {
        let since = self.since.swap(0, Ordering::Relaxed);
        if since != 0 {
            self.total
                .fetch_add(now.saturating_sub(since - 1), Ordering::Relaxed);
        }
    }
    pub fn snapshot(&self, now: u64) -> (u64, u64) {
        let since = self.since.load(Ordering::Relaxed);
        let pending = if since == 0 {
            0
        } else {
            now.saturating_sub(since - 1)
        };
        (
            self.total.load(Ordering::Relaxed).saturating_add(pending),
            pending,
        )
    }
}
#[derive(Default)]
pub(crate) struct H2Wait {
    wait: Wait,
    count: AtomicU64,
}
impl H2Wait {
    pub fn observe(&self, pending: bool, now: impl FnOnce() -> u64) {
        if self.wait.observe(pending, now) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }
    #[cfg(test)]
    fn pending(&self, now: u64) {
        self.observe(true, || now);
    }
    #[cfg(test)]
    fn ready(&self, now: u64) {
        self.observe(false, || now);
    }
    pub fn snapshot(&self, now: u64) -> (u64, u64, u64) {
        let (total, pending) = self.wait.snapshot(now);
        (total, self.count.load(Ordering::Relaxed), pending)
    }
}

/// Counters belong to one CONNECT transport, including when HTTP reuses it.
pub(crate) struct H2Observation {
    pub wait: H2Wait,
    pub enqueued: AtomicU64,
    pub stream_id: AtomicU32,
    pub tunnel_id: String,
    last_progress: AtomicU64,
    started: Instant,
}
impl H2Observation {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            wait: H2Wait::default(),
            enqueued: AtomicU64::new(0),
            stream_id: AtomicU32::new(0),
            tunnel_id: uuid::Uuid::new_v4().to_string(),
            last_progress: AtomicU64::new(0),
            started: Instant::now(),
        })
    }
    pub fn now_ms(&self) -> u64 {
        milliseconds(self.started.elapsed())
    }
    pub fn poll_capacity(&self, pending: bool) {
        self.wait.observe(pending, || self.now_ms());
    }
    pub fn sent(&self, bytes: usize) {
        self.enqueued.fetch_add(bytes as u64, Ordering::Relaxed);
        self.last_progress.store(self.now_ms(), Ordering::Relaxed);
    }
    fn snapshot(&self) -> H2Snapshot {
        let (wait_ms, wait_count, pending_ms) = self.wait.snapshot(self.now_ms());
        H2Snapshot {
            wait_ms,
            pending_ms,
            wait_count,
            enqueued_bytes: self.enqueued.load(Ordering::Relaxed),
        }
    }
}
#[derive(Clone, Copy, Default, Debug)]
pub(crate) struct H2Snapshot {
    pub wait_ms: u64,
    pub wait_count: u64,
    pub pending_ms: u64,
    pub enqueued_bytes: u64,
}
struct H2Transport {
    observation: Arc<H2Observation>,
    baseline: H2Snapshot,
    completed: Option<H2Snapshot>,
}
#[derive(Clone, Copy, Default, Debug)]
pub(crate) struct TransferWindow {
    pub duration_ms: u64,
    pub end_ms: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub h2_wait_ms: u64,
    pub h2_wait_count: u64,
    pub h2_enqueued_bytes: u64,
}
impl TransferWindow {
    pub fn h2_wait_ratio(&self) -> f64 {
        ratio(self.h2_wait_ms, self.duration_ms)
    }
    fn rate(&self, bytes: u64) -> f64 {
        if self.duration_ms == 0 {
            0.0
        } else {
            bytes as f64 * 1000.0 / self.duration_ms as f64
        }
    }
}
struct TransferSample {
    at: Instant,
    request_bytes: u64,
    response_bytes: u64,
    h2: H2Snapshot,
    window: TransferWindow,
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

#[derive(Clone)]
pub(crate) struct TransferIdentity {
    pub component: &'static str,
    pub direction: &'static str,
    pub request_id: String,
    pub trace_id: String,
    pub span_id: String,
    pub tunnel_id: String,
    pub h2_stream_id: u32,
}

pub(crate) struct TransferProgress {
    pub identity: TransferIdentity,
    pub request_bytes: AtomicU64,
    pub response_bytes: AtomicU64,
    pub request_wait: Wait,
    pub response_wait: Wait,
    pub h2_wait: H2Wait,
    pub frontend: Option<Arc<ConnectionProgress>>,
    pub headers_ms: AtomicU64,
    pub h2_enqueued_bytes: AtomicU64,
    pub slow: AtomicBool,
    reason: OnceLock<&'static str>,
    started: Instant,
    last_progress: AtomicU64,
    finished: AtomicBool,
    sample: Mutex<TransferSample>,
    h2_transport: Mutex<Option<H2Transport>>,
    events: Mutex<()>,
}
impl TransferProgress {
    pub fn attach_h2(&self, observation: Arc<H2Observation>) {
        let baseline = observation.snapshot();
        *self
            .h2_transport
            .lock()
            .expect("transport attachment never panics while locked") = Some(H2Transport {
            observation,
            baseline,
            completed: None,
        });
    }
    pub fn finish_h2(&self) {
        let mut transport = self
            .h2_transport
            .lock()
            .expect("transport attachment never panics while locked");
        if let Some(transport) = transport.as_mut() {
            if transport.completed.is_none() {
                transport.completed = Some(transport.observation.snapshot());
            }
        }
    }
    pub fn h2_snapshot(&self) -> H2Snapshot {
        let transport = self
            .h2_transport
            .lock()
            .expect("transport attachment never panics while locked");
        if let Some(transport) = transport.as_ref() {
            let current = transport
                .completed
                .unwrap_or_else(|| transport.observation.snapshot());
            return H2Snapshot {
                wait_ms: current.wait_ms.saturating_sub(transport.baseline.wait_ms),
                wait_count: current
                    .wait_count
                    .saturating_sub(transport.baseline.wait_count),
                enqueued_bytes: current
                    .enqueued_bytes
                    .saturating_sub(transport.baseline.enqueued_bytes),
                pending_ms: current.pending_ms.min(self.now_ms()),
            };
        }
        let (wait_ms, wait_count, pending_ms) = self.h2_wait.snapshot(self.now_ms());
        H2Snapshot {
            wait_ms,
            pending_ms,
            wait_count,
            enqueued_bytes: self.h2_enqueued_bytes.load(Ordering::Relaxed),
        }
    }
    pub fn h2_identity(&self) -> (String, u32) {
        let transport = self
            .h2_transport
            .lock()
            .expect("transport attachment never panics while locked");
        transport.as_ref().map_or_else(
            || (self.identity.tunnel_id.clone(), self.identity.h2_stream_id),
            |transport| {
                (
                    transport.observation.tunnel_id.clone(),
                    transport.observation.stream_id.load(Ordering::Relaxed),
                )
            },
        )
    }
    pub fn window(&self) -> TransferWindow {
        self.sample
            .lock()
            .expect("transfer sampling never panics while locked")
            .window
    }
    pub fn reason(&self) -> &'static str {
        self.reason.get().copied().unwrap_or_default()
    }
    fn progress_age_ms(&self) -> u64 {
        let age = self
            .now_ms()
            .saturating_sub(self.last_progress.load(Ordering::Relaxed));
        let transport = self
            .h2_transport
            .lock()
            .expect("transport attachment never panics while locked");
        match transport.as_ref() {
            Some(transport)
                if transport.observation.enqueued.load(Ordering::Relaxed)
                    > transport.baseline.enqueued_bytes =>
            {
                age.min(
                    transport.observation.now_ms().saturating_sub(
                        transport.observation.last_progress.load(Ordering::Relaxed),
                    ),
                )
            }
            _ => age,
        }
    }
    fn sample_window(&self, period: Duration, partial: bool) -> TransferWindow {
        let now = Instant::now();
        let h2 = self.h2_snapshot();
        let mut sample = self
            .sample
            .lock()
            .expect("transfer sampling never panics while locked");
        let elapsed = now.duration_since(sample.at);
        if elapsed < period && !partial || milliseconds(elapsed) == 0 {
            return sample.window;
        }
        let request = self.request_bytes.load(Ordering::Relaxed);
        let response = self.response_bytes.load(Ordering::Relaxed);
        let window = TransferWindow {
            duration_ms: milliseconds(elapsed),
            end_ms: self.now_ms(),
            request_bytes: request.saturating_sub(sample.request_bytes),
            response_bytes: response.saturating_sub(sample.response_bytes),
            h2_wait_ms: h2.wait_ms.saturating_sub(sample.h2.wait_ms),
            h2_wait_count: h2.wait_count.saturating_sub(sample.h2.wait_count),
            h2_enqueued_bytes: h2.enqueued_bytes.saturating_sub(sample.h2.enqueued_bytes),
        };
        *sample = TransferSample {
            at: now,
            request_bytes: request,
            response_bytes: response,
            h2,
            window,
        };
        window
    }
    pub fn now_ms(&self) -> u64 {
        milliseconds(self.started.elapsed())
    }
    pub fn progress(&self, request: bool, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.progress_at(request, bytes, self.now_ms());
    }
    pub fn progress_at(&self, request: bool, bytes: usize, now: u64) {
        if bytes == 0 {
            return;
        }
        let counter = if request {
            &self.request_bytes
        } else {
            &self.response_bytes
        };
        counter.fetch_add(bytes as u64, Ordering::Relaxed);
        self.last_progress.store(now, Ordering::Relaxed);
    }
    fn slow_reason(&self, policy: &TransferPolicy) -> Option<&'static str> {
        if self.finished.load(Ordering::Acquire) {
            return None;
        }
        let now = Instant::now();
        if now.duration_since(self.started) < policy.slow_after {
            return None;
        }
        let window = self.sample_window(policy.slow_after, false);
        if self.slow.load(Ordering::Relaxed) {
            return None;
        }
        if self.identity.component == "relay" {
            let now = self.now_ms();
            if self.h2_wait.snapshot(now).2 >= milliseconds(policy.slow_after) {
                return Some("h2_capacity_wait");
            }
            if self.request_wait.snapshot(now).1 >= milliseconds(policy.slow_after) {
                return Some("runtime_write_wait");
            }
            // A low-rate CONNECT is diagnostic only when flow-control waits
            // dominate its observation window; idle reused transports stay quiet.
            if window.h2_wait_ratio() >= 0.5
                && window.rate(window.h2_enqueued_bytes) < policy.min_bytes_per_second as f64
            {
                return Some("h2_low_throughput");
            }
            return None;
        }
        let age = self.progress_age_ms();
        if age >= milliseconds(policy.slow_after) {
            return Some("no_progress");
        }
        let rate = if self.identity.direction == "upload" && window.h2_wait_ratio() >= 0.5 {
            window.rate(window.h2_enqueued_bytes)
        } else {
            window.rate(window.request_bytes.saturating_add(window.response_bytes))
        };
        (rate < policy.min_bytes_per_second as f64).then_some("low_throughput")
    }
}

// Each live transfer has one cancellable deadline. Completing a normal request
// removes its entry immediately; no completed-request tombstones are retained.
#[derive(Default)]
struct Schedule {
    deadlines: BTreeMap<(Instant, usize), Weak<TransferProgress>>,
    by_id: HashMap<usize, Instant>,
    armed: Option<Instant>,
}
impl Schedule {
    fn insert(&mut self, id: usize, at: Instant, progress: Weak<TransferProgress>) -> bool {
        if let Some(previous) = self.by_id.insert(id, at) {
            self.deadlines.remove(&(previous, id));
        }
        self.deadlines.insert((at, id), progress);
        self.armed.is_none_or(|armed| at < armed)
    }
    fn cancel(&mut self, id: usize) {
        if let Some(at) = self.by_id.remove(&id) {
            self.deadlines.remove(&(at, id));
        }
    }
    fn due(&mut self) -> Option<(usize, Weak<TransferProgress>)> {
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
    started: AtomicBool,
    active: AtomicU64,
    checks: AtomicU64,
    slow: AtomicU64,
    suppressed: AtomicU64,
    budget: Mutex<(Instant, usize)>,
    diagnostics: OnceLock<Option<std::sync::mpsc::SyncSender<Diagnostic>>>,
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
            started: AtomicBool::new(false),
            active: AtomicU64::new(0),
            checks: AtomicU64::new(0),
            slow: AtomicU64::new(0),
            suppressed: AtomicU64::new(0),
            budget: Mutex::new((Instant::now(), 0)),
            diagnostics: OnceLock::new(),
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
            h2_wait: H2Wait::default(),
            headers_ms: AtomicU64::new(0),
            h2_enqueued_bytes: AtomicU64::new(0),
            last_progress: AtomicU64::new(0),
            slow: AtomicBool::new(false),
            reason: OnceLock::new(),
            finished: AtomicBool::new(false),
            sample: Mutex::new(TransferSample {
                at: started,
                request_bytes: 0,
                response_bytes: 0,
                h2: H2Snapshot::default(),
                window: TransferWindow::default(),
            }),
            h2_transport: Mutex::new(None),
            events: Mutex::new(()),
        });
        // The registration and queued Weak keep this allocation alive until
        // cancellation; its private address cannot be reused while scheduled.
        let id = Arc::as_ptr(&progress) as usize;
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
        if !self.0.started.load(Ordering::Relaxed)
            && self
                .0
                .started
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
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
                    if next.is_some_and(|at| at <= Instant::now()) {
                        tokio::task::yield_now().await;
                        continue;
                    }
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
        for _ in 0..CHECK_BATCH {
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
            let events = state
                .events
                .lock()
                .expect("diagnostic lifecycle never panics while locked");
            if state.finished.load(Ordering::Acquire) {
                continue;
            }
            self.checks.fetch_add(1, Ordering::Relaxed);
            let diagnostic = self.consider(&state);
            self.schedule
                .lock()
                .expect("transfer scheduling never panics while locked")
                .insert(
                    id,
                    Instant::now() + self.policy.slow_after,
                    Arc::downgrade(&state),
                );
            drop(events);
            if let Some(diagnostic) = diagnostic {
                self.enqueue(diagnostic);
            }
        }
    }
    fn consider(&self, state: &TransferProgress) -> Option<Diagnostic> {
        let reason = state.slow_reason(&self.policy)?;
        if state.slow.swap(true, Ordering::AcqRel) {
            return None;
        }
        let _ = state.reason.set(reason);
        self.slow.fetch_add(1, Ordering::Relaxed);
        self.prepare(state, "slow_transfer_started", reason)
    }
    fn prepare(
        &self,
        state: &TransferProgress,
        event: &'static str,
        outcome: &'static str,
    ) -> Option<Diagnostic> {
        let mut budget = self
            .budget
            .lock()
            .expect("diagnostic budget never panics while locked");
        if budget.0.elapsed() >= Duration::from_secs(60) {
            *budget = (Instant::now(), 0);
        }
        if budget.1 >= self.policy.logs_per_minute {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        budget.1 += 1;
        drop(budget);
        Some(Diagnostic::capture(state, event, outcome))
    }
    fn enqueue(&self, diagnostic: Diagnostic) {
        let sender = self.diagnostics.get_or_init(diagnostic::start_worker);
        if sender
            .as_ref()
            .is_none_or(|sender| sender.try_send(diagnostic).is_err())
        {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) struct TransferRegistration {
    monitor: TransferMonitor,
    id: usize,
    pub progress: Arc<TransferProgress>,
    finished: bool,
}
impl TransferRegistration {
    pub fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        // Finish also checks a due transfer if its timer has not run yet.
        let (started, finished) = {
            let _events = self
                .progress
                .events
                .lock()
                .expect("diagnostic lifecycle never panics while locked");
            let started = self.monitor.0.consider(&self.progress);
            self.progress.finished.store(true, Ordering::Release);
            self.monitor
                .0
                .schedule
                .lock()
                .expect("transfer scheduling never panics while locked")
                .cancel(self.id);
            self.monitor.0.active.fetch_sub(1, Ordering::Relaxed);
            let finished = if self.progress.slow.load(Ordering::Acquire) {
                self.progress
                    .sample_window(self.monitor.0.policy.slow_after, true);
                self.monitor
                    .0
                    .prepare(&self.progress, "slow_transfer_finished", outcome)
            } else {
                None
            };
            (started, finished)
        };
        for diagnostic in [started, finished].into_iter().flatten() {
            self.monitor.0.enqueue(diagnostic);
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
/// Preserve the character bound while allocating once for the common short IDs.
pub(crate) fn bounded_id(value: &str) -> String {
    let end = if value.len() <= 128 {
        value.len()
    } else {
        value
            .char_indices()
            .nth(128)
            .map_or(value.len(), |(index, _)| index)
    };
    value[..end].to_owned()
}
pub(crate) fn ratio(wait_ms: u64, elapsed_ms: u64) -> f64 {
    if elapsed_ms == 0 {
        0.0
    } else {
        (wait_ms as f64 / elapsed_ms as f64).min(1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_ids_preserve_short_and_multibyte_values() {
        assert_eq!(bounded_id("data-short"), "data-short");
        assert_eq!(bounded_id(&"x".repeat(129)), "x".repeat(128));
        assert_eq!(bounded_id(&"é".repeat(100)), "é".repeat(100));
        assert_eq!(bounded_id(&"é".repeat(129)), "é".repeat(128));
    }
    #[test]
    fn overhead_wait_clock_runs_only_on_state_changes() {
        let wait = Wait::default();
        let reads = std::cell::Cell::new(0);
        let clock = || {
            reads.set(reads.get() + 1);
            100
        };
        for _ in 0..1000 {
            wait.observe(false, clock);
        }
        assert_eq!(reads.get(), 0, "ready writes must not read the clock");
        for _ in 0..1000 {
            wait.observe(true, clock);
        }
        assert_eq!(reads.get(), 1, "repeated pending polls share one start");
        for _ in 0..1000 {
            wait.observe(false, clock);
        }
        assert_eq!(reads.get(), 2);
    }
    #[tokio::test(start_paused = true)]
    async fn overhead_watchdog_bounds_one_due_batch() {
        let monitor = TransferMonitor::default();
        monitor.0.started.store(true, Ordering::Relaxed);
        let active: Vec<_> = (0..1024)
            .map(|_| monitor.start(identity("relay"), None))
            .collect();
        tokio::time::advance(Duration::from_secs(11)).await;
        monitor.0.check();
        let checked = monitor.0.checks.load(Ordering::Relaxed);
        assert!(
            checked > 0 && checked <= 128,
            "one batch processed {checked} transfers"
        );
        assert_eq!(monitor.0.active.load(Ordering::Relaxed), 1024);
        while monitor.0.checks.load(Ordering::Relaxed) < 1024 {
            monitor.0.check();
        }
        drop(active);
        assert_eq!(monitor.0.active.load(Ordering::Relaxed), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn overhead_blocked_diagnostics_do_not_block_transfer_completion() {
        // Other observer tests install no-op dispatchers on their log workers.
        // Isolate this blocking subscriber so it always exercises the sink.
        const CHILD: &str = "ADX_OVERHEAD_BACKPRESSURE_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            assert!(std::process::Command::new(std::env::current_exe().unwrap())
                .args(["common::transfer::tests::overhead_blocked_diagnostics_do_not_block_transfer_completion", "--exact", "--nocapture"])
                .env(CHILD,"1").status().unwrap().success());
            return;
        }
        use std::sync::{mpsc, Condvar};
        struct Blocked {
            entered: mpsc::SyncSender<()>,
            release: Arc<(Mutex<bool>, Condvar)>,
        }
        impl tracing::Subscriber for Blocked {
            fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
                true
            }
            fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                tracing::span::Id::from_u64(1)
            }
            fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
            fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
            fn enter(&self, _: &tracing::span::Id) {}
            fn exit(&self, _: &tracing::span::Id) {}
            fn event(&self, event: &tracing::Event<'_>) {
                if event.metadata().target() != "adx_transfer" {
                    return;
                }
                let _ = self.entered.try_send(());
                let (lock, wake) = &*self.release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
            }
        }
        let (entered, receiver) = mpsc::sync_channel(1);
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let dispatch = tracing::Dispatch::new(Blocked {
            entered,
            release: release.clone(),
        });
        let monitor = TransferMonitor::new(TransferPolicy {
            logs_per_minute: 512,
            ..Default::default()
        });
        monitor.0.started.store(true, Ordering::Relaxed);
        let mut transfer = monitor.start(identity("ingress"), None);
        let mut others: Vec<_> = (0..100)
            .map(|_| monitor.start(identity("ingress"), None))
            .collect();
        tokio::time::advance(Duration::from_secs(11)).await;
        let runtime = tokio::runtime::Handle::current();
        let checker = monitor.clone();
        let runtime_check = runtime.clone();
        let worker = std::thread::spawn(move || {
            let _runtime = runtime_check.enter();
            tracing::dispatcher::with_default(&dispatch, || checker.0.check());
        });
        let blocked = receiver.recv_timeout(Duration::from_secs(2));
        let (done, completed) = mpsc::channel();
        let finish = std::thread::spawn(move || {
            let _runtime = runtime.enter();
            transfer.finish("complete");
            done.send(()).unwrap();
        });
        let result = completed.recv_timeout(Duration::from_millis(250));
        for other in &mut others {
            other.finish("complete");
        }
        let suppressed = monitor.0.suppressed.load(Ordering::Relaxed);
        *release.0.lock().unwrap() = true;
        release.1.notify_all();
        worker.join().unwrap();
        finish.join().unwrap();
        assert!(
            blocked.is_ok(),
            "diagnostic subscriber was not exercised: checks={}, slow={}, budget={}, suppressed={}",
            monitor.0.checks.load(Ordering::Relaxed),
            monitor.0.slow.load(Ordering::Relaxed),
            monitor.0.budget.lock().unwrap().1,
            monitor.0.suppressed.load(Ordering::Relaxed)
        );
        assert!(
            result.is_ok(),
            "blocked diagnostic output delayed completion"
        );
        assert_eq!(monitor.0.active.load(Ordering::Relaxed), 0);
        assert!(monitor.0.budget.lock().unwrap().1 < 512);
        assert!(
            suppressed > 0,
            "a full bounded queue must suppress without blocking"
        );
    }
    fn identity(component: &'static str) -> TransferIdentity {
        TransferIdentity {
            component,
            direction: "download",
            request_id: "data-00000000-0000-0000-0000-000000000000".into(),
            trace_id: "11111111111111111111111111111111".into(),
            span_id: "2222222222222222".into(),
            tunnel_id: "test-connect".into(),
            h2_stream_id: 1,
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
        assert_eq!(transfer.progress.h2_wait.snapshot(12000), (12000, 1, 12000));
        transfer.progress.h2_wait.ready(12000);
        assert_eq!(transfer.progress.h2_wait.snapshot(15000), (12000, 1, 0));
        transfer.finish("complete");
    }
    #[tokio::test(start_paused = true)]
    async fn repeated_short_h2_waits_detect_sustained_low_throughput() {
        let monitor = TransferMonitor::default();
        let mut transfer = monitor.start(identity("relay"), None);
        for _ in 0..10 {
            transfer
                .progress
                .h2_wait
                .pending(transfer.progress.now_ms());
            tokio::time::advance(Duration::from_millis(900)).await;
            transfer.progress.h2_wait.ready(transfer.progress.now_ms());
            transfer.progress.progress(false, 12 * 1024);
            transfer
                .progress
                .h2_enqueued_bytes
                .fetch_add(12 * 1024, Ordering::Relaxed);
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        monitor.0.check();
        assert!(
            transfer.progress.slow.load(Ordering::Relaxed),
            "12KiB/s with repeated subsecond H2 waits must be diagnosed"
        );
        let h2 = transfer.progress.h2_snapshot();
        assert_eq!((h2.wait_ms, h2.wait_count), (9000, 10));
        assert_eq!(transfer.progress.reason(), "h2_low_throughput");
        assert!((transfer.progress.window().h2_wait_ratio() - 0.9).abs() < 0.01);
        transfer.finish("complete");
        assert_eq!(monitor.0.budget.lock().unwrap().1, 2);
    }
    #[test]
    fn wait_counts_blocking_episodes_instead_of_repeated_polls() {
        let wait = H2Wait::default();
        wait.pending(0);
        wait.pending(10);
        wait.pending(20);
        wait.ready(100);
        wait.pending(200);
        wait.ready(450);
        assert_eq!(wait.count.load(Ordering::Relaxed), 2);
        assert_eq!(wait.snapshot(500), (350, 2, 0));
    }
    #[tokio::test(start_paused = true)]
    async fn healthy_h2_rate_and_idle_reused_tunnels_stay_quiet() {
        let monitor = TransferMonitor::default();
        let mut transfer = monitor.start(identity("relay"), None);
        for _ in 0..10 {
            transfer
                .progress
                .h2_wait
                .pending(transfer.progress.now_ms());
            tokio::time::advance(Duration::from_millis(900)).await;
            transfer.progress.h2_wait.ready(transfer.progress.now_ms());
            transfer
                .progress
                .h2_enqueued_bytes
                .fetch_add(1024 * 1024, Ordering::Relaxed);
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        monitor.0.check();
        assert!(!transfer.progress.slow.load(Ordering::Relaxed));
        tokio::time::advance(Duration::from_secs(11)).await;
        monitor.0.check();
        assert!(!transfer.progress.slow.load(Ordering::Relaxed));
        assert_eq!(transfer.progress.window().h2_wait_ms, 0);
        transfer.finish("complete");
        assert_eq!(monitor.0.budget.lock().unwrap().1, 0);
    }
    #[tokio::test(start_paused = true)]
    async fn reused_h2_counters_start_at_each_request_and_upload_keeps_progressing() {
        let observation = H2Observation::new();
        observation.poll_capacity(true);
        tokio::time::advance(Duration::from_secs(1)).await;
        observation.poll_capacity(false);
        observation.sent(65536);
        let monitor = TransferMonitor::default();
        let mut id = identity("ingress");
        id.direction = "upload";
        let mut first = monitor.start(id, None);
        first.progress.attach_h2(observation.clone());
        assert_eq!(first.progress.h2_snapshot().wait_count, 0);
        first.progress.progress(true, 1024 * 1024);
        for _ in 0..10 {
            observation.poll_capacity(true);
            tokio::time::advance(Duration::from_millis(900)).await;
            observation.poll_capacity(false);
            observation.sent(12 * 1024);
            tokio::time::advance(Duration::from_millis(100)).await;
        }
        monitor.0.check();
        assert!(first.progress.slow.load(Ordering::Relaxed));
        assert_eq!(first.progress.reason(), "low_throughput");
        assert_eq!(first.progress.h2_snapshot().wait_count, 10);
        first.progress.finish_h2();
        first.finish("complete");
        let mut second = monitor.start(identity("ingress"), None);
        second.progress.attach_h2(observation.clone());
        assert_eq!(second.progress.h2_snapshot().wait_count, 0);
        assert_eq!(second.progress.h2_snapshot().wait_ms, 0);
        assert_eq!(second.progress.h2_snapshot().enqueued_bytes, 0);
        observation.poll_capacity(true);
        tokio::time::advance(Duration::from_millis(100)).await;
        observation.poll_capacity(false);
        observation.sent(1024);
        assert_eq!(second.progress.h2_snapshot().wait_count, 1);
        assert_eq!(first.progress.h2_snapshot().wait_count, 10);
        assert_eq!(first.progress.h2_snapshot().wait_ms, 9000);
        second.finish("complete");
    }

    #[tokio::test]
    async fn real_h2_window_replenishment_reports_repeated_short_waits() {
        use tokio::io::AsyncWriteExt;
        use tokio::net::{TcpListener, TcpStream};
        let monitor = TransferMonitor::new(TransferPolicy {
            slow_after: Duration::from_millis(500),
            ..Default::default()
        });
        let mut transfer = monitor.start(identity("relay"), None);
        let progress = transfer.progress.clone();
        let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let backend_address = backend.local_addr().unwrap();
        let writer = tokio::spawn(async move {
            let (mut socket, _) = backend.accept().await.unwrap();
            let _ = socket.write_all(&vec![0x5a; 256 * 1024]).await;
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let relay = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(socket).await.unwrap();
            let (request, mut response) = connection.accept().await.unwrap().unwrap();
            let send = response
                .send_response(http::Response::new(()), false)
                .unwrap();
            let tcp = TcpStream::connect(backend_address).await.unwrap();
            tokio::select! {
                _=crate::node::relay::relay_h2_tcp_observed(request.into_body(),send,tcp,
                    crate::node::relay::RelayStats::default(),Some(progress))=>{},
                _=async {while connection.accept().await.is_some(){}}=>{},
            }
        });
        let socket = TcpStream::connect(address).await.unwrap();
        let (mut sender, connection) = h2::client::Builder::new()
            .initial_window_size(1024)
            .handshake::<_, bytes::Bytes>(socket)
            .await
            .unwrap();
        let driver = tokio::spawn(connection);
        let (response, send) = sender
            .send_request(
                http::Request::builder()
                    .method("CONNECT")
                    .uri("http://localhost/")
                    .body(())
                    .unwrap(),
                false,
            )
            .unwrap();
        let mut recv = response.await.unwrap().into_body();
        let mut received = 0;
        tokio::time::timeout(Duration::from_secs(4), async {
            for _ in 0..40 {
                let bytes = recv.data().await.unwrap().unwrap();
                received += bytes.len();
                tokio::time::sleep(Duration::from_millis(20)).await;
                recv.flow_control().release_capacity(bytes.len()).unwrap();
            }
        })
        .await
        .unwrap();
        assert!(received > 0);
        assert!(transfer.progress.slow.load(Ordering::Relaxed));
        assert_eq!(transfer.progress.reason(), "h2_low_throughput");
        assert!(transfer.progress.h2_snapshot().wait_count > 10);
        assert!(transfer.progress.window().h2_wait_ratio() > 0.5);
        assert!(
            transfer
                .progress
                .h2_wait
                .snapshot(transfer.progress.now_ms())
                .2
                < 500,
            "credit updates must keep the current wait below the stall threshold"
        );
        transfer.finish("cancelled");
        drop(send);
        drop(recv);
        drop(sender);
        relay.abort();
        driver.abort();
        writer.abort();
        let _ = relay.await;
        let _ = driver.await;
        let _ = writer.await;
        assert_eq!(monitor.0.budget.lock().unwrap().1, 2);
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
