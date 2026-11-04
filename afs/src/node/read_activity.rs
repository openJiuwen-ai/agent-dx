//! Measurement-only owned scopes. These counters never schedule or admit work.
//! Durations sum overlapping scopes and must not be added to E2E wall time.
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

#[derive(Clone, Copy)]
#[repr(usize)]
pub(crate) enum Activity {
    RangeRpc,
    ReplyBytes,
    FuseTask,
    CompletionWait,
    ReplyWait,
}
const NAMES: [&str; 5] = [
    "range_rpc",
    "reply_bytes",
    "fuse_task",
    "completion_wait",
    "reply_wait",
];

#[derive(Debug)]
struct Counters {
    active: AtomicU64,
    peak: AtomicU64,
    bytes: AtomicU64,
    byte_peak: AtomicU64,
    active_metric: afs_metrics::IntGauge,
    peak_metric: afs_metrics::IntCounter,
    byte_metric: afs_metrics::IntGauge,
    byte_peak_metric: afs_metrics::IntCounter,
    calls: afs_metrics::IntCounter,
    nanos: afs_metrics::IntCounter,
}
#[derive(Clone, Debug)]
pub(crate) struct Activities([Arc<Counters>; 5]);

impl Activities {
    pub(crate) fn register(
        registry: &afs_metrics::Registry,
    ) -> Result<Self, afs_metrics::MetricsError> {
        let gauge = |name: &str, help: &str| -> Result<_, afs_metrics::MetricsError> {
            let m = afs_metrics::IntGaugeVec::new(afs_metrics::Opts::new(name, help), &["stage"])?;
            afs_metrics::register_collector(registry, &m)?;
            Ok(m)
        };
        let counter = |name: &str, help: &str| -> Result<_, afs_metrics::MetricsError> {
            let m =
                afs_metrics::IntCounterVec::new(afs_metrics::Opts::new(name, help), &["stage"])?;
            afs_metrics::register_collector(registry, &m)?;
            Ok(m)
        };
        let active = gauge(
            "afs_dfs_read_pipeline_active",
            "Currently owned scopes, by stage; source_io is file calls, not device queue depth.",
        )?;
        let peak = counter(
            "afs_dfs_read_pipeline_peak",
            "Process-lifetime peak simultaneously owned scopes.",
        )?;
        let bytes = gauge(
            "afs_dfs_read_pipeline_bytes",
            "Currently owned reply buffer capacity; excludes idle pools, allocator and transport.",
        )?;
        let byte_peak = counter(
            "afs_dfs_read_pipeline_peak_bytes",
            "Process-lifetime peak accounted byte capacity.",
        )?;
        let calls = counter(
            "afs_dfs_read_pipeline_calls_total",
            "Scope starts including failures and retries.",
        )?;
        let nanos = counter(
            "afs_dfs_read_pipeline_nanoseconds_total",
            "Sum of scope wall durations; overlaps are not additive E2E time.",
        )?;
        Ok(Self(std::array::from_fn(|i| {
            Arc::new(Counters {
                active: AtomicU64::new(0),
                peak: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
                byte_peak: AtomicU64::new(0),
                active_metric: active.with_label_values(&[NAMES[i]]),
                peak_metric: peak.with_label_values(&[NAMES[i]]),
                byte_metric: bytes.with_label_values(&[NAMES[i]]),
                byte_peak_metric: byte_peak.with_label_values(&[NAMES[i]]),
                calls: calls.with_label_values(&[NAMES[i]]),
                nanos: nanos.with_label_values(&[NAMES[i]]),
            })
        })))
    }
    /// Caller serializes observations; this stage measures state, not scope time.
    pub(crate) fn observe_count(&self, activity: Activity, current: u64) {
        let counters = &self.0[activity as usize];
        let previous = counters.active.swap(current, Ordering::AcqRel);
        let peak = counters.peak.fetch_max(current, Ordering::AcqRel);
        counters.active_metric.set(current as i64);
        counters.peak_metric.inc_by(current.saturating_sub(peak));
        counters.calls.inc_by(current.saturating_sub(previous));
    }
    pub(crate) fn enter(&self, activity: Activity, bytes: u64) -> ActivityGuard {
        let counters = self.0[activity as usize].clone();
        let n = counters.active.fetch_add(1, Ordering::AcqRel) + 1;
        let old = counters.peak.fetch_max(n, Ordering::AcqRel);
        let b = counters.bytes.fetch_add(bytes, Ordering::AcqRel) + bytes;
        let old_bytes = counters.byte_peak.fetch_max(b, Ordering::AcqRel);
        counters.active_metric.inc();
        counters.peak_metric.inc_by(n.saturating_sub(old));
        counters.byte_metric.add(bytes as i64);
        counters
            .byte_peak_metric
            .inc_by(b.saturating_sub(old_bytes));
        counters.calls.inc();
        ActivityGuard {
            counters,
            bytes,
            started: Instant::now(),
        }
    }
}
#[derive(Debug)]
pub(crate) struct ActivityGuard {
    counters: Arc<Counters>,
    bytes: u64,
    started: Instant,
}
impl Drop for ActivityGuard {
    fn drop(&mut self) {
        self.counters.active.fetch_sub(1, Ordering::AcqRel);
        self.counters.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.counters.active_metric.dec();
        self.counters.byte_metric.sub(self.bytes as i64);
        self.counters
            .nanos
            .inc_by(u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX));
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn read_concurrency_activity_is_owned_and_not_a_scheduler() {
        let registry = afs_metrics::Registry::default();
        let activities = Activities::register(&registry).unwrap();
        let a = activities.enter(Activity::ReplyBytes, 4096);
        let b = activities.enter(Activity::ReplyBytes, 8192);
        let state = &activities.0[Activity::ReplyBytes as usize];
        assert_eq!(state.peak.load(Ordering::Acquire), 2);
        assert_eq!(state.byte_peak.load(Ordering::Acquire), 12288);
        drop(a);
        assert_eq!(state.bytes.load(Ordering::Acquire), 8192);
        drop(b);
        assert_eq!(state.active.load(Ordering::Acquire), 0);
        assert_eq!(state.bytes.load(Ordering::Acquire), 0);
    }
}
