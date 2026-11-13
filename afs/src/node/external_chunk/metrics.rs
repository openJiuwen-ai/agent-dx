//! S3 store metrics and slow-GET diagnostics.

use super::*;

#[derive(Clone)]
pub struct S3ChunkStoreMetrics {
    get_stage_seconds: afs_metrics::HistogramVec,
    verify_seconds: afs_metrics::HistogramVec,
    verify_bytes_total: afs_metrics::IntCounterVec,
    request_reply_seconds: afs_metrics::HistogramVec,
    cache_admit_seconds: afs_metrics::HistogramVec,
    cache_activity_total: afs_metrics::IntCounterVec,
    get_bytes_total: afs_metrics::IntCounterVec,
    get_failures_total: afs_metrics::IntCounterVec,
    slow_gets_total: afs_metrics::IntCounterVec,
    read_branch_total: afs_metrics::IntCounterVec,
    read_branch_bytes_total: afs_metrics::IntCounterVec,
    sidecar_events_total: afs_metrics::IntCounterVec,
}

impl std::fmt::Debug for S3ChunkStoreMetrics {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3ChunkStoreMetrics")
            .finish_non_exhaustive()
    }
}

impl S3ChunkStoreMetrics {
    pub fn register(
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        registry
            .get_or_register(|registry| {
                let get_stage_seconds = afs_metrics::HistogramVec::new(
                    afs_metrics::HistogramOpts::new(
                        "afs_dfs_s3_get_stage_seconds",
                        "Bounded S3 GET phase durations observed by AFS; SDK transport internals remain aggregated.",
                    )
                    .buckets(afs_metrics::latency_buckets()),
                    &["phase"],
                )?;
                let verify_seconds = afs_metrics::HistogramVec::new(
                    afs_metrics::HistogramOpts::new(
                        "afs_dfs_external_verify_seconds",
                        "Full immutable external chunk verification duration by source.",
                    )
                    .buckets(afs_metrics::latency_buckets()),
                    &["source"],
                )?;
                let request_reply_seconds = afs_metrics::HistogramVec::new(
                    afs_metrics::HistogramOpts::new(
                        "afs_dfs_s3_request_reply_seconds",
                        "Caller wait for the owned S3 worker reply by operation.",
                    )
                    .buckets(afs_metrics::latency_buckets()),
                    &["operation"],
                )?;
                let verify_bytes_total = afs_metrics::IntCounterVec::new(
                    afs_metrics::Opts::new(
                        "afs_dfs_external_verify_bytes_total",
                        "Bytes successfully covered by full external chunk verification by source.",
                    ),
                    &["source"],
                )?;
                let cache_admit_seconds = afs_metrics::HistogramVec::new(
                    afs_metrics::HistogramOpts::new(
                        "afs_dfs_s3_cache_admit_seconds",
                        "Synchronous time spent handing a verified S3 GET result to the cache admission path.",
                    )
                    .buckets(afs_metrics::latency_buckets()),
                    &["result"],
                )?;
                let cache_activity_total = afs_metrics::IntCounterVec::new(
                    afs_metrics::Opts::new(
                        "afs_dfs_s3_cache_activity_total",
                        "S3 cache admission and eviction activity observed after cache operations return.",
                    ),
                    &["event"],
                )?;
                let get_bytes_total = afs_metrics::IntCounterVec::new(
                    afs_metrics::Opts::new(
                        "afs_dfs_s3_get_body_bytes_total",
                        "Successful S3 GET body bytes copied into verified immutable chunks.",
                    ),
                    &["result"],
                )?;
                let slow_gets_total = afs_metrics::IntCounterVec::new(
                    afs_metrics::Opts::new(
                        "afs_dfs_s3_slow_gets_total",
                        "S3 GET requests that crossed the bounded slow diagnostic threshold.",
                    ),
                    &["result"],
                )?;
                let get_failures_total = afs_metrics::IntCounterVec::new(
                    afs_metrics::Opts::new(
                        "afs_dfs_s3_get_failures_total",
                        "S3 GET failures by bounded AFS-observed stage and sanitized result.",
                    ),
                    &["stage", "result"],
                )?;
                let read_branch_total = afs_metrics::IntCounterVec::new(
                    afs_metrics::Opts::new(
                        "afs_dfs_s3_read_branch_total",
                        "S3 chunk reads that missed the cache, by path: verified ranges or the whole object and why.",
                    ),
                    &["branch"],
                )?;
                let read_branch_bytes_total = afs_metrics::IntCounterVec::new(
                    afs_metrics::Opts::new(
                        "afs_dfs_s3_read_branch_bytes_total",
                        "Object bytes those S3 chunk reads fetched and verified, by path.",
                    ),
                    &["branch"],
                )?;
                let sidecar_events_total = afs_metrics::IntCounterVec::new(
                    afs_metrics::Opts::new(
                        "afs_dfs_s3_sidecar_events_total",
                        "BLAKE3 outboard sidecar object PUT and GET outcomes.",
                    ),
                    &["event"],
                )?;
                afs_metrics::register_collector(registry, &read_branch_total)?;
                afs_metrics::register_collector(registry, &read_branch_bytes_total)?;
                afs_metrics::register_collector(registry, &sidecar_events_total)?;
                for branch in [
                    "range",
                    "whole",
                    "whole_no_sidecar",
                    "whole_after_failed_proof",
                ] {
                    read_branch_total.with_label_values(&[branch]);
                    read_branch_bytes_total.with_label_values(&[branch]);
                }
                for event in [
                    "put_ok",
                    "put_failed",
                    "get_ok",
                    "get_missing",
                    "get_invalid",
                    "proof_failed",
                ] {
                    sidecar_events_total.with_label_values(&[event]);
                }
                afs_metrics::register_collector(registry, &get_stage_seconds)?;
                afs_metrics::register_collector(registry, &verify_seconds)?;
                afs_metrics::register_collector(registry, &verify_bytes_total)?;
                afs_metrics::register_collector(registry, &request_reply_seconds)?;
                afs_metrics::register_collector(registry, &cache_admit_seconds)?;
                afs_metrics::register_collector(registry, &cache_activity_total)?;
                afs_metrics::register_collector(registry, &get_bytes_total)?;
                afs_metrics::register_collector(registry, &slow_gets_total)?;
                afs_metrics::register_collector(registry, &get_failures_total)?;
                for phase in [
                    "queue",
                    "headers_sdk_aggregate",
                    "body_first_poll_wait",
                    "body_poll_wait_total",
                    "body_poll_wait_max",
                    "body_copy",
                    "body_final_eof_wait",
                ] {
                    get_stage_seconds.with_label_values(&[phase]);
                }
                for source in ["get", "ready"] {
                    verify_seconds.with_label_values(&[source]);
                    verify_bytes_total.with_label_values(&[source]);
                }
                for operation in ["put", "read", "read_ranges"] {
                    request_reply_seconds.with_label_values(&[operation]);
                }
                for result in ["attempted", "no_cache"] {
                    cache_admit_seconds.with_label_values(&[result]);
                }
                for event in [
                    "pending_budget_rejects",
                    "capacity_rejects",
                    "task_rejects",
                    "admissions",
                    "evictions",
                ] {
                    cache_activity_total.with_label_values(&[event]);
                }
                get_bytes_total.with_label_values(&["ok"]);
                slow_gets_total.with_label_values(&["ok"]);
                for stage in ["timeout", "headers", "body", "verify", "metadata"] {
                    for result in ["error", "timeout", "corrupt"] {
                        get_failures_total.with_label_values(&[stage, result]);
                    }
                }
                Ok(Self {
                    get_stage_seconds,
                    verify_seconds,
                    verify_bytes_total,
                    request_reply_seconds,
                    cache_admit_seconds,
                    cache_activity_total,
                    get_bytes_total,
                    get_failures_total,
                    slow_gets_total,
                    read_branch_total,
                    read_branch_bytes_total,
                    sidecar_events_total,
                })
            })
    }

    pub(super) fn observe_get(&self, timing: &S3ReadTiming) {
        self.get_stage_seconds
            .with_label_values(&["queue"])
            .observe(seconds_from_micros(timing.queue_wait_us));
        self.get_stage_seconds
            .with_label_values(&["headers_sdk_aggregate"])
            .observe(seconds_from_micros(timing.headers_us));
        self.get_stage_seconds
            .with_label_values(&["body_first_poll_wait"])
            .observe(seconds_from_micros(timing.body.first_poll_wait_us));
        self.get_stage_seconds
            .with_label_values(&["body_poll_wait_total"])
            .observe(seconds_from_micros(timing.body.poll_wait_total_us));
        self.get_stage_seconds
            .with_label_values(&["body_poll_wait_max"])
            .observe(seconds_from_micros(timing.body.poll_wait_max_us));
        self.get_stage_seconds
            .with_label_values(&["body_copy"])
            .observe(seconds_from_micros(timing.body.copy_us));
        self.get_stage_seconds
            .with_label_values(&["body_final_eof_wait"])
            .observe(seconds_from_micros(timing.body.final_eof_wait_us));
        self.verify_seconds
            .with_label_values(&["get"])
            .observe(seconds_from_micros(timing.verify_us));
        self.verify_bytes_total
            .with_label_values(&["get"])
            .inc_by(timing.body.copied_bytes);
        self.get_bytes_total
            .with_label_values(&["ok"])
            .inc_by(timing.body.copied_bytes);
    }

    pub(super) fn observe_ready_verify(&self, bytes: u64, elapsed: Duration) {
        self.verify_seconds
            .with_label_values(&["ready"])
            .observe(elapsed.as_secs_f64());
        self.verify_bytes_total
            .with_label_values(&["ready"])
            .inc_by(bytes);
    }

    pub(super) fn observe_reply_wait(&self, operation: &'static str, elapsed: Duration) {
        self.request_reply_seconds
            .with_label_values(&[operation])
            .observe(elapsed.as_secs_f64());
    }

    pub(super) fn observe_cache_admit(&self, result: &'static str, elapsed: Duration) {
        self.cache_admit_seconds
            .with_label_values(&[result])
            .observe(elapsed.as_secs_f64());
    }

    fn observe_cache_activity_delta(&self, previous: CacheActivity, current: CacheActivity) {
        for (event, delta) in [
            (
                "pending_budget_rejects",
                current
                    .pending_budget_rejects
                    .saturating_sub(previous.pending_budget_rejects),
            ),
            (
                "task_rejects",
                current.task_rejects.saturating_sub(previous.task_rejects),
            ),
            (
                "capacity_rejects",
                current
                    .capacity_rejects
                    .saturating_sub(previous.capacity_rejects),
            ),
            (
                "admissions",
                current.admissions.saturating_sub(previous.admissions),
            ),
            (
                "evictions",
                current.evictions.saturating_sub(previous.evictions),
            ),
        ] {
            if delta > 0 {
                self.cache_activity_total
                    .with_label_values(&[event])
                    .inc_by(delta);
            }
        }
    }

    fn observe_slow_get(&self) {
        self.slow_gets_total.with_label_values(&["ok"]).inc();
    }

    pub(super) fn observe_get_failure(&self, stage: &'static str, result: &'static str) {
        self.get_failures_total
            .with_label_values(&[stage, result])
            .inc();
    }
}

#[derive(Clone)]
pub(super) struct S3GetObserver {
    metrics: Option<S3ChunkStoreMetrics>,
    slow: Arc<S3SlowGetLimiter>,
    last_cache_activity: Arc<Mutex<CacheActivity>>,
}

impl S3GetObserver {
    pub(super) fn new(metrics: Option<S3ChunkStoreMetrics>) -> Self {
        Self {
            metrics,
            slow: Arc::new(S3SlowGetLimiter::default()),
            last_cache_activity: Arc::new(Mutex::new(CacheActivity::default())),
        }
    }

    pub(super) fn observe_get(&self, chunk: &ChunkObject, timing: &S3ReadTiming) {
        if let Some(metrics) = &self.metrics {
            metrics.observe_get(timing);
        }
        if timing.total_without_reply_us() >= SLOW_GET_THRESHOLD_US
            && let Some(metrics) = &self.metrics
        {
            metrics.observe_slow_get();
        }
        if timing.total_without_reply_us() >= SLOW_GET_THRESHOLD_US && self.slow.allow() {
            afs_logging::warn!(
                "dfs.s3.slow_get";
                "chunk_id" => chunk.id.0.as_str(),
                "chunk_bytes" => chunk.length,
                "queue_wait_us" => timing.queue_wait_us,
                "headers_sdk_aggregate_us" => timing.headers_us,
                "body_first_poll_wait_us" => timing.body.first_poll_wait_us,
                "body_poll_wait_total_us" => timing.body.poll_wait_total_us,
                "body_poll_wait_max_us" => timing.body.poll_wait_max_us,
                "body_copy_us" => timing.body.copy_us,
                "body_final_eof_wait_us" => timing.body.final_eof_wait_us,
                "verify_us" => timing.verify_us,
                "sdk_transport_subphases" => "unknown"
            );
        }
    }

    pub(super) fn observe_reply_wait(&self, operation: &'static str, elapsed: Duration) {
        if let Some(metrics) = &self.metrics {
            metrics.observe_reply_wait(operation, elapsed);
        }
    }

    pub(super) fn observe_cache_admit(&self, result: &'static str, elapsed: Duration) {
        if let Some(metrics) = &self.metrics {
            metrics.observe_cache_admit(result, elapsed);
        }
    }

    pub(super) fn observe_cache_activity(&self, current: CacheActivity) {
        let Some(metrics) = &self.metrics else {
            return;
        };
        let Ok(mut previous) = self.last_cache_activity.lock() else {
            return;
        };
        metrics.observe_cache_activity_delta(*previous, current);
        *previous = previous.max_fields(current);
    }

    pub(super) fn observe_get_failure(&self, stage: &'static str, result: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics.observe_get_failure(stage, result);
        }
    }

    pub(super) fn observe_read_branch(&self, branch: &'static str, bytes: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.read_branch_total.with_label_values(&[branch]).inc();
            metrics
                .read_branch_bytes_total
                .with_label_values(&[branch])
                .inc_by(bytes);
        }
    }

    pub(super) fn observe_sidecar(&self, event: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics
                .sidecar_events_total
                .with_label_values(&[event])
                .inc();
        }
    }
}

const SLOW_GET_THRESHOLD_US: u64 = 50_000;

pub(super) const SLOW_GET_LIMIT_PER_WINDOW: u32 = 16;

pub(super) const SLOW_GET_WINDOW: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct S3SlowGetLimiter {
    window: Mutex<Option<(std::time::Instant, u32)>>,
}

impl S3SlowGetLimiter {
    pub(super) fn allow(&self) -> bool {
        let Ok(mut window) = self.window.lock() else {
            return false;
        };
        let now = std::time::Instant::now();
        match *window {
            Some((started, count)) if now.saturating_duration_since(started) < SLOW_GET_WINDOW => {
                if count >= SLOW_GET_LIMIT_PER_WINDOW {
                    false
                } else {
                    *window = Some((started, count + 1));
                    true
                }
            }
            _ => {
                *window = Some((now, 1));
                true
            }
        }
    }

    #[cfg(test)]
    pub(super) fn set_window_for_test(&self, started: std::time::Instant, count: u32) {
        *self.window.lock().unwrap() = Some((started, count));
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct S3BodyTiming {
    pub(super) first_poll_wait_us: u64,
    pub(super) poll_wait_total_us: u64,
    pub(super) poll_wait_max_us: u64,
    pub(super) copy_us: u64,
    pub(super) final_eof_wait_us: u64,
    pub(super) copied_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct S3ReadTiming {
    pub(super) queue_wait_us: u64,
    pub(super) headers_us: u64,
    pub(super) body: S3BodyTiming,
    pub(super) verify_us: u64,
}

impl S3ReadTiming {
    fn total_without_reply_us(&self) -> u64 {
        self.queue_wait_us
            .saturating_add(self.headers_us)
            .saturating_add(self.body.poll_wait_total_us)
            .saturating_add(self.body.copy_us)
            .saturating_add(self.body.final_eof_wait_us)
            .saturating_add(self.verify_us)
    }
}

fn seconds_from_micros(value: u64) -> f64 {
    Duration::from_micros(value).as_secs_f64()
}

pub(super) fn micros_u64(value: Duration) -> u64 {
    value.as_micros().min(u128::from(u64::MAX)) as u64
}
